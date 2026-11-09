// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Index write-ahead log and sequence-range coverage (ADR-0001 §6.2, §6.3).
//!
//! The index WAL is Arrow-free: a frame carries the reserved item sequence
//! range and the raw mergeset items, so replaying it is exactly
//! `Table::add_items_with_seq_range`. A frame is:
//!
//! ```text
//! magic(4) | len(4) | frame_type(1) | index_partition_id(8)
//!          | min_seq(8) | max_seq(8) | max_series_id(8) | checksum(4)
//! payload: varuint(count) then per item varuint(len) + bytes
//! ```
//!
//! All integers are big-endian; the checksum is CRC-32C over the frame header
//! (excluding the checksum field) plus the payload.
//!
//! WAL segments (`*.iwal`) are reclaimed with a **per-frame** coverage check
//! against the sequence ranges persisted in part metadata: a segment may be
//! deleted only when every frame's `[min_seq, max_seq]` is covered by the
//! union of the file parts. A global low watermark is not sufficient — a part
//! can carry a high `max_seq` while an older mid-range part is still pending.

use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use crate::tsf::checksum::crc32c_multi;

/// Magic bytes `"IWAL"` that begin every index WAL frame.
pub const WAL_MAGIC: [u8; 4] = *b"IWAL";
/// Size of the fixed frame header, excluding the checksum field.
const FRAME_HEADER_LEN: usize = 41;
/// Frame type used for mergeset index items.
pub const FRAME_TYPE_ITEMS: u8 = 1;

/// A decoded index WAL frame (ADR-0001 §6.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalFrame {
    /// Index partition (month) this frame belongs to.
    pub partition_id: u64,
    /// Reserved item sequence range `[min_seq, max_seq]`.
    pub min_seq: u64,
    pub max_seq: u64,
    /// Highest series id allocated at append time.
    pub max_series_id: u64,
    /// The raw mergeset items carried by the frame.
    pub items: Vec<Vec<u8>>,
}

impl WalFrame {
    /// Builds a frame from the reserved sequence range and the items to store.
    pub fn new(
        partition_id: u64,
        min_seq: u64,
        max_seq: u64,
        max_series_id: u64,
        items: Vec<Vec<u8>>,
    ) -> Self {
        Self {
            partition_id,
            min_seq,
            max_seq,
            max_series_id,
            items,
        }
    }
}

/// Encodes one item as `varuint(len) + bytes`.
pub fn encode_item(dst: &mut Vec<u8>, item: &[u8]) {
    marshal_var_uint64(dst, item.len() as u64);
    dst.extend_from_slice(item);
}

/// Decodes one item from `src` at `pos`, advancing past the varint length.
pub fn decode_item(src: &[u8], pos: &mut usize) -> Result<Vec<u8>, String> {
    let len = unmarshal_var_uint64(src, pos)? as usize;
    if *pos + len > src.len() {
        return Err("truncated item payload".to_string());
    }
    let item = src[*pos..*pos + len].to_vec();
    *pos += len;
    Ok(item)
}

fn marshal_var_uint64(dst: &mut Vec<u8>, mut v: u64) {
    loop {
        let mut b = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            b |= 0x80;
        }
        dst.push(b);
        if v == 0 {
            break;
        }
    }
}

fn unmarshal_var_uint64(src: &[u8], pos: &mut usize) -> Result<u64, String> {
    let mut result = 0u64;
    let mut shift = 0u32;
    loop {
        if *pos >= src.len() {
            return Err("truncated varuint".to_string());
        }
        let byte = src[*pos];
        *pos += 1;
        result |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift > 63 {
            return Err("varuint overflow".to_string());
        }
    }
    Ok(result)
}

/// Serializes a frame to `dst`: fixed header then the payload.
fn encode_frame(dst: &mut Vec<u8>, frame: &WalFrame) {
    let mut payload = Vec::new();
    marshal_var_uint64(&mut payload, frame.items.len() as u64);
    for item in &frame.items {
        encode_item(&mut payload, item);
    }

    dst.extend_from_slice(&WAL_MAGIC);
    dst.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    dst.push(FRAME_TYPE_ITEMS);
    dst.extend_from_slice(&frame.partition_id.to_be_bytes());
    dst.extend_from_slice(&frame.min_seq.to_be_bytes());
    dst.extend_from_slice(&frame.max_seq.to_be_bytes());
    dst.extend_from_slice(&frame.max_series_id.to_be_bytes());
    let checksum = crc32c_multi(&[&dst[4..FRAME_HEADER_LEN], &payload]);
    dst.extend_from_slice(&checksum.to_be_bytes());
    dst.extend_from_slice(&payload);
}

/// Decodes a frame from `src` at `pos`, returning `None` when `src` ends
/// cleanly at a frame boundary (the tail was never written).
fn decode_frame(src: &[u8], pos: &mut usize) -> Result<Option<WalFrame>, String> {
    if src.len() - *pos < FRAME_HEADER_LEN {
        // A partial header is a torn write at the tail; ignore it.
        return Ok(None);
    }
    let base = *pos;
    if src[base..base + 4] != WAL_MAGIC {
        return Ok(None);
    }
    let payload_len = u32::from_be_bytes(src[base + 4..base + 8].try_into().unwrap()) as usize;
    let frame_type = src[base + 8];
    if frame_type != FRAME_TYPE_ITEMS {
        return Err(format!("unknown frame type {frame_type}"));
    }
    // Fixed header (41 bytes) + checksum (4 bytes) precede the payload.
    let payload_start = base + FRAME_HEADER_LEN + 4;
    let end = payload_start
        .checked_add(payload_len)
        .ok_or("frame length overflow")?;
    if end > src.len() {
        // A partial payload is a torn write at the tail; ignore it.
        return Ok(None);
    }
    let partition_id = u64::from_be_bytes(src[base + 9..base + 17].try_into().unwrap());
    let min_seq = u64::from_be_bytes(src[base + 17..base + 25].try_into().unwrap());
    let max_seq = u64::from_be_bytes(src[base + 25..base + 33].try_into().unwrap());
    let max_series_id = u64::from_be_bytes(src[base + 33..base + 41].try_into().unwrap());
    let stored_checksum = u32::from_be_bytes(src[base + 41..base + 45].try_into().unwrap());
    let calc = crc32c_multi(&[
        &src[base + 4..base + FRAME_HEADER_LEN],
        &src[payload_start..end],
    ]);
    if stored_checksum != calc {
        return Err("frame checksum mismatch".to_string());
    }
    let mut p = base + 45;
    let count = unmarshal_var_uint64(src, &mut p)? as usize;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        items.push(decode_item(src, &mut p)?);
    }
    *pos = end;
    Ok(Some(WalFrame {
        partition_id,
        min_seq,
        max_seq,
        max_series_id,
        items,
    }))
}

/// Appends index WAL frames to `dir`, one segment file per writer instance.
///
/// A segment is named `wal-{seq:016X}.iwal`. Frames are appended in order and
/// the file is fsynced after each `append` when `sync_on_write` is set,
/// matching the durability ordering of ADR-0001 §6.1 (the index WAL is fsynced
/// before the data batch is acknowledged).
pub struct WalWriter {
    file: File,
    path: PathBuf,
    /// Index partition this segment belongs to (stamped in frame headers).
    #[allow(dead_code)] // kept for segment ownership accounting
    partition_id: u64,
    sync_on_write: bool,
    /// Frames appended to the current segment; used for the coverage check.
    frames: Vec<WalFrame>,
    /// Total bytes written to the current segment (for size-based rotation).
    bytes_written: u64,
}

impl WalWriter {
    /// Opens (creating if needed) the next WAL segment in `dir`.
    pub fn open(
        dir: impl AsRef<Path>,
        partition_id: u64,
        sync_on_write: bool,
    ) -> Result<Self, String> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir).map_err(|e| format!("cannot create {dir:?}: {e}"))?;
        let seq = next_segment_number(dir)?;
        let name = format!("wal-{seq:016X}.iwal");
        let path = dir.join(name);
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .map_err(|e| format!("cannot open {path:?}: {e}"))?;
        Ok(Self {
            file,
            path,
            partition_id,
            sync_on_write,
            frames: Vec::new(),
            bytes_written: 0,
        })
    }

    /// The path of the segment being written.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Total bytes written to this segment so far.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Appends one frame and returns the byte length written.
    pub fn append(&mut self, frame: WalFrame) -> Result<u64, String> {
        let mut bytes = Vec::new();
        encode_frame(&mut bytes, &frame);
        let n = bytes.len() as u64;
        self.file
            .write_all(&bytes)
            .and_then(|_| {
                if self.sync_on_write {
                    self.file.sync_all()
                } else {
                    Ok(())
                }
            })
            .map_err(|e| format!("cannot write WAL frame to {:?}: {e}", self.path))?;
        self.bytes_written += n;
        self.frames.push(frame);
        Ok(n)
    }

    /// The frames appended in this segment so far.
    pub fn frames(&self) -> &[WalFrame] {
        &self.frames
    }

    /// Finishes the segment: fsyncs the directory so the file is durable.
    pub fn finish(self) -> Result<(), String> {
        self.file
            .sync_all()
            .map_err(|e| format!("cannot fsync {:?}: {e}", self.path))?;
        sync_dir(self.path.parent().unwrap_or_else(|| Path::new(".")))
    }
}

/// Chooses the next segment number from the existing `*.iwal` file names.
fn next_segment_number(dir: &Path) -> Result<u64, String> {
    let mut max = 0u64;
    for entry in fs::read_dir(dir).map_err(|e| format!("cannot read {dir:?}: {e}"))? {
        let entry = entry.map_err(|e| format!("cannot read dir entry in {dir:?}: {e}"))?;
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(hex) = name
            .strip_prefix("wal-")
            .and_then(|n| n.strip_suffix(".iwal"))
        {
            if let Ok(seq) = u64::from_str_radix(hex, 16) {
                max = max.max(seq + 1);
            }
        }
    }
    Ok(max)
}

fn sync_dir(dir: &Path) -> Result<(), String> {
    File::open(dir)
        .and_then(|f| f.sync_all())
        .map_err(|e| format!("cannot fsync {dir:?}: {e}"))
}

/// A decoded WAL segment: its path plus the frames it holds.
#[derive(Debug, Clone)]
pub struct WalSegment {
    pub path: PathBuf,
    pub frames: Vec<WalFrame>,
}

/// Reads every `*.iwal` segment in `dir` in name order, returning the frames
/// and whether any segment had a torn tail frame (which is dropped and, when
/// `truncate` is set, removed from the file).
///
/// A frame whose payload or checksum is invalid is treated as a torn tail: the
/// frame is dropped and the file is truncated to the last good frame so a
/// subsequent append does not extend garbage.
pub fn read_segments(dir: impl AsRef<Path>, truncate: bool) -> Result<Vec<WalSegment>, String> {
    let dir = dir.as_ref();
    let mut paths: Vec<PathBuf> = match fs::read_dir(dir) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("iwal"))
            .collect(),
        Err(_) => Vec::new(),
    };
    paths.sort();
    let mut segments = Vec::with_capacity(paths.len());
    for path in paths {
        // An empty segment is a freshly rotated writer that has not appended
        // yet; it carries no frames and must not show up in the segment list.
        if fs::metadata(&path).map(|m| m.len()).unwrap_or(0) == 0 {
            continue;
        }
        let data = fs::read(&path).map_err(|e| format!("cannot read {path:?}: {e}"))?;
        let mut pos = 0usize;
        let mut good = 0usize;
        loop {
            match decode_frame(&data, &mut pos) {
                Ok(Some(_)) => {
                    good = pos;
                }
                Ok(None) => break,
                Err(err) => {
                    // A mid-file corruption: stop, drop the tail, truncate.
                    if truncate {
                        fs::write(&path, &data[..good])
                            .map_err(|e| format!("cannot truncate {path:?}: {e}"))?;
                    }
                    return Err(format!("corrupt frame in {path:?}: {err}"));
                }
            }
        }
        let mut frames = Vec::new();
        let mut p = 0usize;
        while let Ok(Some(frame)) = decode_frame(&data, &mut p) {
            frames.push(frame);
        }
        if truncate && good != data.len() {
            fs::write(&path, &data[..good])
                .map_err(|e| format!("cannot truncate {path:?}: {e}"))?;
        }
        segments.push(WalSegment { path, frames });
    }
    Ok(segments)
}

/// A union of closed sequence intervals, kept sorted and non-overlapping. Used
/// for the WAL reclamation coverage check (ADR-0001 §6.3).
#[derive(Debug, Default, Clone)]
pub struct SeqIntervalSet {
    ranges: Vec<(u64, u64)>,
}

impl SeqIntervalSet {
    /// Adds `[min, max]` to the set, merging overlapping or adjacent ranges.
    pub fn add(&mut self, min: u64, max: u64) {
        if min > max {
            return;
        }
        self.ranges.push((min, max));
        self.ranges.sort_unstable();
        let mut merged: Vec<(u64, u64)> = Vec::with_capacity(self.ranges.len());
        for (lo, hi) in self.ranges.drain(..) {
            match merged.last_mut() {
                Some(last) if lo <= last.1.saturating_add(1) => {
                    if hi > last.1 {
                        last.1 = hi;
                    }
                }
                _ => merged.push((lo, hi)),
            }
        }
        self.ranges = merged;
    }

    /// Returns `true` when every point of `[min, max]` is covered by the set.
    pub fn covers(&self, min: u64, max: u64) -> bool {
        self.ranges.iter().any(|&(lo, hi)| lo <= min && max <= hi)
    }

    /// Builds the set from a sequence range `[min, max]`.
    pub fn from_range(min: u64, max: u64) -> Self {
        let mut set = Self::default();
        set.add(min, max);
        set
    }

    /// Number of disjoint ranges in the set.
    pub fn range_count(&self) -> usize {
        self.ranges.len()
    }
}

/// Replays index WAL frames whose sequence range is **not** yet covered by a
/// persisted part, in segment then frame order, invoking `replay` for each.
///
/// Returns the number of frames replayed and the number skipped as already
/// covered. Replaying an already-covered frame is safe (index items are
/// idempotent), so the coverage check is an optimization, not a correctness
/// requirement.
pub fn replay_uncovered<F>(
    dir: impl AsRef<Path>,
    parts: &[super::part::Part],
    mut replay: F,
) -> Result<(usize, usize), String>
where
    F: FnMut(&WalFrame),
{
    let covered = parts_covered_set(parts);
    let segments = read_segments(dir, true)?;
    let mut replayed = 0usize;
    let mut skipped = 0usize;
    for segment in segments {
        for frame in &segment.frames {
            if covered.covers(frame.min_seq, frame.max_seq) {
                skipped += 1;
                continue;
            }
            replay(frame);
            replayed += 1;
        }
    }
    Ok((replayed, skipped))
}

/// Builds the coverage set from persisted parts' sequence ranges. A part with
/// `min_seq == max_seq == 0` (written before the sequence range was tracked)
/// contributes no range.
pub fn parts_covered_set(parts: &[super::part::Part]) -> SeqIntervalSet {
    let mut set = SeqIntervalSet::default();
    for part in parts {
        let (min_seq, max_seq) = (part.header().min_seq, part.header().max_seq);
        if max_seq > 0 {
            set.add(min_seq, max_seq);
        }
    }
    set
}

/// Deletes every WAL segment in `dir` whose frames are all covered by the
/// persisted parts, after fsyncing each unlink. Returns the segments removed.
///
/// `active` is the segment the writer is currently appending to. It is never
/// removed even when its frames are covered: the writer still holds the file
/// descriptor, so an unlink would make every later append invisible.
pub fn reclaim_covered(
    dir: impl AsRef<Path>,
    parts: &[super::part::Part],
    active: Option<&Path>,
) -> Result<Vec<PathBuf>, String> {
    let dir = dir.as_ref();
    let covered = parts_covered_set(parts);
    let segments = read_segments(dir, false)?;
    let mut removed = Vec::new();
    for segment in segments {
        if active.is_some_and(|a| a == segment.path.as_path()) {
            continue;
        }
        if segment
            .frames
            .iter()
            .all(|f| covered.covers(f.min_seq, f.max_seq))
        {
            fs::remove_file(&segment.path)
                .map_err(|e| format!("cannot remove {:?}: {e}", segment.path))?;
            removed.push(segment.path.clone());
        }
    }
    if !removed.is_empty() {
        sync_dir(dir)?;
    }
    Ok(removed)
}

/// Returns `true` when a freshly created segment should be written. Exposed so
/// tests can assert segments are only created when frames exist.
pub fn wal_dir(dir: impl AsRef<Path>) -> PathBuf {
    dir.as_ref().to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("mergeset-wal-{}-{n}", std::process::id()));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn items(n: usize, prefix: &str) -> Vec<Vec<u8>> {
        (0..n)
            .map(|i| format!("{prefix}{i:08}").into_bytes())
            .collect()
    }

    #[test]
    fn test_frame_round_trips() {
        let frame = WalFrame::new(1, 10, 20, 7, items(3, "x"));
        let mut bytes = Vec::new();
        encode_frame(&mut bytes, &frame);
        let mut pos = 0usize;
        let decoded = decode_frame(&bytes, &mut pos).unwrap().unwrap();
        assert_eq!(decoded, frame);
        assert_eq!(pos, bytes.len());
    }

    #[test]
    fn test_frame_checksum_rejects_tampering() {
        let frame = WalFrame::new(1, 1, 1, 1, items(1, "y"));
        let mut bytes = Vec::new();
        encode_frame(&mut bytes, &frame);
        bytes[20] ^= 0xff;
        let mut pos = 0usize;
        assert!(decode_frame(&bytes, &mut pos).is_err());
    }

    #[test]
    fn test_torn_tail_is_ignored() {
        let frame = WalFrame::new(1, 1, 1, 1, items(2, "z"));
        let mut bytes = Vec::new();
        encode_frame(&mut bytes, &frame);
        bytes.extend_from_slice(&WAL_MAGIC);
        bytes.extend_from_slice(&[0, 0, 0]);
        let mut pos = 0usize;
        let decoded = decode_frame(&bytes, &mut pos).unwrap().unwrap();
        assert_eq!(decoded.items.len(), 2);
        assert!(pos < bytes.len());
    }

    #[test]
    fn test_wal_writer_appends_and_reads_back() {
        let dir = TempDir::new();
        let mut writer = WalWriter::open(&dir.0, 1, true).unwrap();
        writer
            .append(WalFrame::new(1, 1, 5, 1, items(2, "a")))
            .unwrap();
        writer
            .append(WalFrame::new(1, 6, 10, 1, items(2, "b")))
            .unwrap();
        writer.finish().unwrap();
        let segments = read_segments(&dir.0, false).unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].frames.len(), 2);
        assert_eq!(segments[0].frames[0].min_seq, 1);
        assert_eq!(segments[0].frames[1].max_seq, 10);
    }

    #[test]
    fn test_seq_interval_set_merges() {
        let mut set = SeqIntervalSet::default();
        set.add(10, 20);
        set.add(15, 25);
        set.add(30, 35);
        assert_eq!(set.range_count(), 2);
        assert!(set.covers(12, 18));
        assert!(!set.covers(9, 9));
    }

    #[test]
    fn test_replay_uncovered_replays_all_when_no_coverage() {
        let dir = TempDir::new();
        let mut writer = WalWriter::open(&dir.0, 1, true).unwrap();
        writer
            .append(WalFrame::new(1, 1, 5, 1, items(2, "a")))
            .unwrap();
        writer
            .append(WalFrame::new(1, 6, 10, 1, items(2, "b")))
            .unwrap();
        writer.finish().unwrap();
        let empty: Vec<crate::mergeset::part::Part> = Vec::new();
        let (replayed, skipped) = replay_uncovered(&dir.0, &empty, |_| {}).unwrap();
        assert_eq!(replayed, 2);
        assert_eq!(skipped, 0);
    }
}
