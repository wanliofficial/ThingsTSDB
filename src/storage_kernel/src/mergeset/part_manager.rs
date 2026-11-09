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
// specific language governing permissions and
// limitations under the License.

//! Background part manager (ADR-0001 §3.6).
//!
//! Drives the two steady-state loops of an indexDB:
//!
//! - **Flush loop** (`DATA_FLUSH_INTERVAL`): pending items become a searchable
//!   in-memory part.
//! - **Merge loop**: the [`merge_policy`] selects a window of file parts and
//!   merges them into one, removing the sources on success.
//!
//! Both loops are bounded by a stop channel; an interrupted merge leaves its
//! sources untouched. The intervals are injected so the acceptance tests
//! (crash/restart, write amplification) can drive the loops deterministically
//! without sleeping for the real 1s/5s.

use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
};

use crate::mergeset::{
    merge_policy::{select_parts_to_merge, MergeCandidate, Selection},
    part::{Part, PartWriter},
    table::Table,
    table_search::TableSearch,
    wal::{read_segments, reclaim_covered, WalFrame, WalWriter},
};

/// Tunables for the part manager. The ADR fixes the defaults (§3.6).
#[derive(Debug, Clone)]
pub struct ManagerConfig {
    /// `DATA_FLUSH_INTERVAL`: how often pending items become a file part.
    pub flush_interval: std::time::Duration,
    /// How often the merge loop considers a merge.
    pub merge_interval: std::time::Duration,
    /// zstd level for merged output when the source item count is small.
    pub small_part_compress_level: i32,
    /// The item count above which a part bypasses the page cache.
    pub cached_part_threshold: u64,
    /// Number of file-merger threads (`max(cpus, 4)` by default, ADR §3.6).
    pub merge_threads: usize,
    /// Fsync each index WAL frame after appending (ADR §6.1). Disable only when
    /// the deployment accepts potential index/data divergence on crash.
    pub wal_sync_on_write: bool,
    /// Soft cap per WAL segment; a new segment is opened past this size.
    pub max_wal_segment_bytes: u64,
}

/// Soft cap per index WAL segment before rotation.
const MAX_WAL_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

impl Default for ManagerConfig {
    fn default() -> Self {
        Self {
            flush_interval: std::time::Duration::from_secs(5),
            merge_interval: std::time::Duration::from_secs(1),
            small_part_compress_level: -5,
            cached_part_threshold: 1_000_000,
            merge_threads: default_merge_threads(),
            wal_sync_on_write: true,
            max_wal_segment_bytes: MAX_WAL_SEGMENT_BYTES,
        }
    }
}

/// `max(cpus, 4)` merger threads as fixed by ADR-0001 §3.6.
fn default_merge_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .max(4)
}

/// Outcome of one flush cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushOutcome {
    /// Number of items flushed into the new part.
    pub items_flushed: usize,
    /// Number of file parts after the flush.
    pub file_parts: usize,
    /// Whether a part was actually written.
    pub flushed: bool,
}

/// Outcome of one merge cycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeOutcome {
    /// The part ids that were merged, when a merge ran.
    pub merged_ids: Vec<usize>,
    /// Bytes read from the merged sources.
    pub input_bytes: u64,
    /// Bytes written to the output part.
    pub output_bytes: u64,
    /// Whether a merge ran.
    pub merged: bool,
}

impl MergeOutcome {
    /// The outcome of a cycle that found nothing to merge.
    fn skipped() -> Self {
        Self {
            merged_ids: Vec::new(),
            input_bytes: 0,
            output_bytes: 0,
            merged: false,
        }
    }
}

/// Counters for the flush and merge loops, used to measure write
/// amplification (ADR-0001 §3.6). All fields are monotonic.
#[derive(Debug, Default)]
pub struct MergeStats {
    flushes: AtomicU64,
    flush_bytes: AtomicU64,
    merges: AtomicU64,
    merge_input_bytes: AtomicU64,
    merge_output_bytes: AtomicU64,
    wal_reclaimed: AtomicU64,
}

impl MergeStats {
    fn new() -> Self {
        Self::default()
    }

    /// Records one flush that wrote `bytes` to a file part.
    fn record_flush(&self, bytes: u64) {
        self.flushes.fetch_add(1, Ordering::Relaxed);
        self.flush_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Records one merge that read `input` bytes and wrote `output` bytes.
    fn record_merge(&self, input: u64, output: u64) {
        self.merges.fetch_add(1, Ordering::Relaxed);
        self.merge_input_bytes.fetch_add(input, Ordering::Relaxed);
        self.merge_output_bytes.fetch_add(output, Ordering::Relaxed);
    }

    /// Records the number of WAL segments reclaimed after a part swap.
    fn record_wal_reclaimed(&self, count: u64) {
        self.wal_reclaimed.fetch_add(count, Ordering::Relaxed);
    }

    /// Number of flushes that wrote a part.
    pub fn flushes(&self) -> u64 {
        self.flushes.load(Ordering::Relaxed)
    }

    /// Bytes written by flushes (the baseline for write amplification).
    pub fn flush_bytes(&self) -> u64 {
        self.flush_bytes.load(Ordering::Relaxed)
    }

    /// Number of completed merges.
    pub fn merges(&self) -> u64 {
        self.merges.load(Ordering::Relaxed)
    }

    /// Bytes read from merge sources.
    pub fn merge_input_bytes(&self) -> u64 {
        self.merge_input_bytes.load(Ordering::Relaxed)
    }

    /// Bytes written by merges.
    pub fn merge_output_bytes(&self) -> u64 {
        self.merge_output_bytes.load(Ordering::Relaxed)
    }

    /// Number of WAL segments reclaimed after part swaps.
    pub fn wal_reclaimed(&self) -> u64 {
        self.wal_reclaimed.load(Ordering::Relaxed)
    }

    /// Write amplification: total bytes written (flush + merge output) divided
    /// by the bytes written by flushes alone. A steady-state LSM target is
    /// below 5x; the value is `0.0` before the first flush.
    pub fn write_amplification(&self) -> f64 {
        let base = self.flush_bytes() as f64;
        if base == 0.0 {
            return 0.0;
        }
        (self.flush_bytes() + self.merge_output_bytes()) as f64 / base
    }
}

/// Drives the flush and merge loops for one indexDB.
pub struct PartManager {
    table: Arc<Mutex<Table>>,
    dir: PathBuf,
    config: ManagerConfig,
    next_part_id: Arc<Mutex<u64>>,
    stop: Arc<AtomicBool>,
    file_parts: Arc<Mutex<Vec<Part>>>,
    stats: Arc<MergeStats>,
    /// Index WAL writer, when durable registration is enabled (ADR-0001 §6.2).
    wal: Option<Arc<Mutex<WalWriter>>>,
    /// Directory holding the WAL segments.
    wal_dir: Option<PathBuf>,
    /// Month partition id stamped into WAL frame headers.
    partition_id: u64,
}

impl PartManager {
    pub fn new(table: Arc<Mutex<Table>>, dir: impl AsRef<Path>, config: ManagerConfig) -> Self {
        Self {
            table,
            dir: dir.as_ref().to_path_buf(),
            config,
            next_part_id: Arc::new(Mutex::new(0)),
            stop: Arc::new(AtomicBool::new(false)),
            file_parts: Arc::new(Mutex::new(Vec::new())),
            stats: Arc::new(MergeStats::new()),
            wal: None,
            wal_dir: None,
            partition_id: 0,
        }
    }

    /// Returns the write-amplification counters (shared with the loops).
    pub fn stats(&self) -> Arc<MergeStats> {
        Arc::clone(&self.stats)
    }

    /// Attaches an index WAL so writes are durable before they are searchable
    /// and can be replayed after a crash (ADR-0001 §6.1/§6.2). `partition_id`
    /// is the month partition stamped into every frame header.
    pub fn with_wal(mut self, wal: WalWriter, wal_dir: PathBuf, partition_id: u64) -> Self {
        self.wal = Some(Arc::new(Mutex::new(wal)));
        self.wal_dir = Some(wal_dir);
        self.partition_id = partition_id;
        self
    }

    /// Appends `items` to the index WAL (fsynced) and then to the pending
    /// buffer, following the durability ordering of ADR-0001 §6.1: a durable
    /// data row may never reference an unrecoverable series id, so the index
    /// WAL is written and fsynced **before** the items become searchable.
    ///
    /// Returns the reserved sequence range the frame was stamped with.
    pub fn write_index_items(&self, items: Vec<Vec<u8>>) -> Result<(u64, u64), String> {
        if items.is_empty() {
            return Ok((0, 0));
        }
        let (min_seq, max_seq) = {
            let mut table = self.table.lock().unwrap_or_else(|e| e.into_inner());
            table.reserve_seq_range(items.len() as u64)
        };
        if let Some(wal) = &self.wal {
            let frame = WalFrame::new(self.partition_id, min_seq, max_seq, 0, items.clone());
            let mut wal = wal.lock().unwrap_or_else(|e| e.into_inner());
            // Rotate the segment before appending when it is already large, so
            // a single segment is bounded and reclaimable in one piece.
            if wal.bytes_written() > 0 && wal.bytes_written() >= MAX_WAL_SEGMENT_BYTES {
                let old = std::mem::replace(
                    &mut *wal,
                    WalWriter::open(
                        self.wal_dir.as_ref().expect("wal dir set with wal"),
                        self.partition_id,
                        self.config.wal_sync_on_write,
                    )?,
                );
                old.finish()?;
                wal.append(frame)?;
            } else {
                wal.append(frame)?;
            }
        }
        let mut table = self.table.lock().unwrap_or_else(|e| e.into_inner());
        table.add_items_with_seq_range(items, min_seq, max_seq);
        Ok((min_seq, max_seq))
    }

    /// Reclaims WAL segments fully covered by the current file parts, then
    /// re-evaluates after every part swap (ADR-0001 §6.3). No-op without a WAL.
    ///
    /// The segment the writer is appending to is never unlinked: that would
    /// make later appends invisible. When the active segment is already fully
    /// covered by the persisted parts, it is closed and rotated first, so the
    /// covered segment can be reclaimed while the new one stays writable.
    fn reclaim_wal(&self) -> Result<(), String> {
        let (Some(wal_dir), Some(wal)) = (&self.wal_dir, &self.wal) else {
            return Ok(());
        };
        let parts = self.file_parts();
        let covered = crate::mergeset::wal::parts_covered_set(&parts);
        {
            let mut writer = wal.lock().unwrap_or_else(|e| e.into_inner());
            let all_covered = !writer.frames().is_empty()
                && writer
                    .frames()
                    .iter()
                    .all(|f| covered.covers(f.min_seq, f.max_seq));
            if all_covered {
                let old = std::mem::replace(
                    &mut *writer,
                    WalWriter::open(wal_dir, self.partition_id, self.config.wal_sync_on_write)?,
                );
                old.finish()?;
            }
        }
        let active = wal
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .path()
            .to_path_buf();
        let removed = reclaim_covered(wal_dir, &parts, Some(&active))?;
        self.stats.record_wal_reclaimed(removed.len() as u64);
        Ok(())
    }

    /// Allocates the next zero-padded part directory name.
    fn alloc_part_path(&self, tag: &str) -> PathBuf {
        let mut id = self.next_part_id.lock().unwrap_or_else(|e| e.into_inner());
        *id += 1;
        self.dir.join(format!("{tag}-{:016X}", *id))
    }

    /// Registers a freshly written file part with the manager.
    pub fn register_part(&self, part: Part) {
        self.file_parts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(part);
        self.publish_parts();
    }

    /// Pushes the manager's file-part list into the table, which is what the
    /// search path reads. Called after every change to the part set.
    fn publish_parts(&self) {
        let parts = self
            .file_parts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        self.table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .set_parts(parts);
    }

    /// Rebuilds the file-part set from `parts.json` after a restart.
    ///
    /// Every part listed is reopened and validated; a missing or corrupt part
    /// directory is skipped (its `metadata.json` makes an unfinished part
    /// unreadable, so the merge that was interrupted left no orphan rows). The
    /// parts are then re-registered in first-item order.
    pub fn recover_from_disk(&self) -> Result<usize, String> {
        let names = crate::mergeset::part::read_parts_json(&self.dir)?;
        let mut recovered = 0;
        for name in names {
            let path = self.dir.join(&name);
            match Part::open(&path) {
                Ok(part) => {
                    self.register_part(part);
                    recovered += 1;
                }
                Err(_) => {
                    // An unfinished or corrupt part from an interrupted merge
                    // is not loaded; it is not referenced by any data row.
                }
            }
        }
        Ok(recovered)
    }

    /// Replays index WAL frames that are not yet covered by a persisted part
    /// (ADR-0001 §6.4 recovery ordering: parts first, then the remaining WAL).
    /// Replayed items are appended to the pending buffer; a subsequent flush
    /// makes them searchable. Replaying an already-covered frame is idempotent,
    /// so the coverage check is an optimization.
    ///
    /// Returns the number of frames replayed. No-op without a WAL.
    pub fn replay_wal(&self) -> Result<usize, String> {
        let Some(wal_dir) = &self.wal_dir else {
            return Ok(0);
        };
        let parts = self.file_parts();
        let segments = read_segments(wal_dir, true)?;
        let covered = crate::mergeset::wal::parts_covered_set(&parts);
        let mut replayed = 0usize;
        let mut table = self.table.lock().unwrap_or_else(|e| e.into_inner());
        for segment in &segments {
            for frame in &segment.frames {
                if covered.covers(frame.min_seq, frame.max_seq) {
                    continue;
                }
                table.add_items_with_seq_range(frame.items.clone(), frame.min_seq, frame.max_seq);
                replayed += 1;
            }
        }
        Ok(replayed)
    }

    /// Returns the current file parts, sorted by first item.
    pub fn file_parts(&self) -> Vec<Part> {
        let mut parts = self
            .file_parts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        parts.sort_by(|a, b| a.first_item().cmp(b.first_item()));
        parts
    }

    /// Runs one flush cycle: writes pending items to a file part.
    pub fn flush_once(&self) -> Result<FlushOutcome, String> {
        let (mut pending, min_seq, max_seq) = self
            .table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take_pending_with_seq()
            .unwrap_or((Vec::new(), 0, 0));
        if pending.is_empty() {
            return Ok(FlushOutcome {
                items_flushed: 0,
                file_parts: self
                    .file_parts
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .len(),
                flushed: false,
            });
        }
        // The pending buffer collects rows in write order, which is not sorted
        // (one series writes ns0, ns2, ns3, ns1, ...). A part is a sorted item
        // list, so sort and dedup before handing the items to the writer.
        pending.sort_unstable();
        pending.dedup();
        let level = if (pending.len() as u64) < self.config.cached_part_threshold {
            self.config.small_part_compress_level
        } else {
            3
        };
        let out = self.alloc_part_path("part");
        let part = {
            let mut writer = PartWriter::new(level);
            writer.set_seq_range(min_seq, max_seq);
            for item in &pending {
                writer.add(item)?;
            }
            writer.finish(&out)?
        };
        let count = pending.len();
        let part_bytes = part.size;
        self.register_part(part);
        self.stats.record_flush(part_bytes);
        // Persist the part list so recovery finds the flushed part. Written
        // atomically (temp + rename + dir fsync) so a crash never leaves a torn
        // parts.json (ADR-0001 §6.5).
        let paths: Vec<PathBuf> = self
            .file_parts()
            .iter()
            .map(|p| p.path().to_path_buf())
            .collect();
        let refs: Vec<&Path> = paths.iter().map(|p| p.as_path()).collect();
        crate::mergeset::part::write_parts_json(&self.dir, &refs)?;
        // The flushed part now covers its sequence range, so WAL segments whose
        // frames are all covered can be reclaimed (ADR-0001 §6.3).
        let _ = self.reclaim_wal();
        Ok(FlushOutcome {
            items_flushed: count,
            file_parts: self
                .file_parts
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .len(),
            flushed: true,
        })
    }

    /// Runs one merge cycle: claims a window of file parts, merges them, and
    /// publishes the output.
    ///
    /// The window is selected **and removed** from the file-part set under one
    /// lock, so several merger threads can run concurrently without ever
    /// selecting the same part twice. The merge itself runs outside the lock.
    /// If it fails, the claimed sources are put back unchanged.
    pub fn merge_once(&self) -> Result<MergeOutcome, String> {
        let claimed = {
            let mut file_parts = self.file_parts.lock().unwrap_or_else(|e| e.into_inner());
            let claimed = claim_merge_window(&mut file_parts);
            if claimed.len() < 2 {
                // Nothing claimed: put back anything we removed defensively.
                file_parts.extend(claimed);
                return Ok(MergeOutcome::skipped());
            }
            claimed
        };
        let out = self.alloc_part_path("merged");
        let compressed_level = compress_level_for(&claimed, &self.config);
        let merged = match merge_parts(&claimed, &out, compressed_level) {
            Ok(part) => part,
            Err(err) => {
                // Roll back: the sources were never deleted, only claimed.
                self.file_parts
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .extend(claimed);
                return Err(err);
            }
        };
        let input_bytes: u64 = claimed.iter().map(|p| p.size).sum();
        let output_bytes = merged.size;
        let merged_ids: Vec<usize> = claimed.iter().map(part_identity).collect();
        // Publish the output and drop the sources from the manager's set. The
        // source directories are only deleted once the output is registered.
        {
            let mut file_parts = self.file_parts.lock().unwrap_or_else(|e| e.into_inner());
            file_parts.push(merged);
        }
        for part in &claimed {
            let path = part.path();
            // Files are still open in `part`; drop the handle first on Unix it
            // does not matter, but be explicit and remove after the merge is
            // fully published.
            let _ = std::fs::remove_dir_all(path);
        }
        self.publish_parts();
        self.stats.record_merge(input_bytes, output_bytes);
        // Persist the updated part list (the merged part replaces its sources),
        // then reclaim WAL segments the merged part now covers.
        let paths: Vec<PathBuf> = self
            .file_parts()
            .iter()
            .map(|p| p.path().to_path_buf())
            .collect();
        let refs: Vec<&Path> = paths.iter().map(|p| p.as_path()).collect();
        crate::mergeset::part::write_parts_json(&self.dir, &refs)?;
        let _ = self.reclaim_wal();
        Ok(MergeOutcome {
            merged_ids,
            input_bytes,
            output_bytes,
            merged: true,
        })
    }

    /// Signals the loops to stop after their current cycle.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    pub fn is_stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }

    /// Spawns the flush loop and `merge_threads` merger loops on background
    /// threads. Returns the join handles; joining a loop ends when `stop()` is
    /// called.
    ///
    /// Takes `&Arc<Self>` so the loop closures hold a real shared handle to the
    /// manager rather than a stack reconstruction.
    pub fn spawn(self: &Arc<Self>) -> Vec<JoinHandle<()>> {
        let mut handles = Vec::with_capacity(1 + self.config.merge_threads);
        handles.push(spawn_loop(
            "indexdb-flush".to_string(),
            self.config.flush_interval,
            self.stop.clone(),
            {
                let this = Arc::clone(self);
                move || this.flush_once().map(|_| ())
            },
        ));
        for n in 0..self.config.merge_threads {
            handles.push(spawn_loop(
                format!("indexdb-merge-{n}"),
                self.config.merge_interval,
                self.stop.clone(),
                {
                    let this = Arc::clone(self);
                    move || this.merge_once().map(|_| ())
                },
            ));
        }
        handles
    }
}

/// A cheap stable identity for a part: its on-disk path string hash.
fn part_identity(part: &Part) -> usize {
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
    };
    let mut hasher = DefaultHasher::new();
    part.path().hash(&mut hasher);
    hasher.finish() as usize
}

/// Selects a merge window and removes it from `file_parts` (the claim).
///
/// Returns the claimed parts, or an empty vector when the policy found no
/// window worth merging. The caller merges the claimed parts outside the lock
/// and either publishes the output or puts the claim back on failure.
fn claim_merge_window(file_parts: &mut Vec<Part>) -> Vec<Part> {
    if file_parts.len() < 8 {
        return Vec::new();
    }
    let candidates: Vec<MergeCandidate> = file_parts
        .iter()
        .map(|p| MergeCandidate {
            id: part_identity(p),
            size: p.size,
        })
        .collect();
    // Candidates must be sorted by ascending size for the policy.
    let mut sorted: Vec<MergeCandidate> = candidates;
    sorted.sort_by_key(|c| c.size);
    let Selection::Merge(window) = select_parts_to_merge(&sorted) else {
        return Vec::new();
    };
    let wanted: Vec<usize> = window.iter().map(|c| c.id).collect();
    // Partition: keep the unselected parts, return the selected ones.
    let mut claimed = Vec::with_capacity(wanted.len());
    let mut keep = Vec::with_capacity(file_parts.len() - wanted.len());
    for part in file_parts.drain(..) {
        if wanted.contains(&part_identity(&part)) {
            claimed.push(part);
        } else {
            keep.push(part);
        }
    }
    *file_parts = keep;
    claimed
}

/// Picks the zstd level for merged output from the source item count.
fn compress_level_for(sources: &[Part], config: &ManagerConfig) -> i32 {
    let items: u64 = sources.iter().map(|p| p.item_count).sum();
    if items < config.cached_part_threshold {
        config.small_part_compress_level
    } else {
        3
    }
}

/// Merges the given parts into one, streaming items through a k-way search.
fn merge_parts(sources: &[Part], out: &Path, compress_level: i32) -> Result<Part, String> {
    let mut writer = PartWriter::new(compress_level);
    let mut search = TableSearch::new(sources, None)?;
    while let Some(item) = search.next_item()? {
        writer.add(&item)?;
    }
    writer.finish(out)
}

/// Runs `work` every `interval` until `stop` is set. `work` returns an error
/// string on failure, which is dropped here (the loops are best-effort; a
/// failed cycle does not abort the index).
fn spawn_loop<F>(
    name: String,
    interval: std::time::Duration,
    stop: Arc<AtomicBool>,
    work: F,
) -> JoinHandle<()>
where
    F: Fn() -> Result<(), String> + Send + 'static,
{
    thread::Builder::new()
        .name(name)
        .spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                if let Err(err) = work() {
                    eprintln!("part-manager loop error: {err}");
                }
                // Sleep in small slices so stop is responsive.
                let mut slept = std::time::Duration::ZERO;
                while slept < interval && !stop.load(Ordering::Relaxed) {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    slept += std::time::Duration::from_millis(10);
                }
            }
        })
        .expect("spawn part-manager loop")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    use super::*;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
            let path = std::env::temp_dir().join(format!("mergeset-pm-{}-{n}", std::process::id()));
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

    fn make_items(n: usize) -> Vec<Vec<u8>> {
        (0..n)
            .map(|i| format!("metric{{id={i:08}}}").into_bytes())
            .collect()
    }

    #[test]
    fn test_flush_moves_pending_to_file_part() {
        let dir = TempDir::new();
        let table = Arc::new(Mutex::new(Table::new()));
        {
            let mut t = table.lock().unwrap();
            for item in make_items(1_000) {
                t.add(&item);
            }
        }
        let pm = PartManager::new(table.clone(), &dir.0, ManagerConfig::default());
        let outcome = pm.flush_once().unwrap();
        assert!(outcome.flushed);
        assert_eq!(outcome.items_flushed, 1_000);
        assert_eq!(outcome.file_parts, 1);
        // The table no longer holds the pending items.
        assert_eq!(table.lock().unwrap().pending_len(), 0);
        // A reopen sees the same items.
        let parts = pm.file_parts();
        let mut search = TableSearch::new(&parts, None).unwrap();
        let mut count = 0;
        while search.next_item().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 1_000);
    }

    #[test]
    fn test_flush_noop_when_empty() {
        let dir = TempDir::new();
        let table = Arc::new(Mutex::new(Table::new()));
        let pm = PartManager::new(table, &dir.0, ManagerConfig::default());
        let outcome = pm.flush_once().unwrap();
        assert!(!outcome.flushed);
    }

    #[test]
    fn test_merge_combines_parts_and_removes_sources() {
        let dir = TempDir::new();
        let table = Arc::new(Mutex::new(Table::new()));
        // Seed nine file parts directly so the merge policy has a window.
        let pm = PartManager::new(table.clone(), &dir.0, ManagerConfig::default());
        let total = 9 * 500;
        for part_no in 0..9u32 {
            let mut writer = PartWriter::new(1);
            for i in 0..500u32 {
                let id = part_no * 500 + i;
                writer
                    .add(format!("metric{{id={id:08}}}").as_bytes())
                    .unwrap();
            }
            let path = dir.0.join(format!("seed-{part_no}"));
            let part = writer.finish(path).unwrap();
            pm.register_part(part);
        }
        assert_eq!(pm.file_parts().len(), 9);
        let outcome = pm.merge_once().unwrap();
        assert!(outcome.merged, "merge policy should select a window");
        // Sources are removed, one output added.
        assert_eq!(pm.file_parts().len(), 9 - outcome.merged_ids.len() + 1);
        // All items survive the merge.
        let parts = pm.file_parts();
        let mut search = TableSearch::new(&parts, None).unwrap();
        let mut count = 0;
        while search.next_item().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, total);
    }

    #[test]
    fn test_merge_needs_enough_parts() {
        let dir = TempDir::new();
        let table = Arc::new(Mutex::new(Table::new()));
        let pm = PartManager::new(table, &dir.0, ManagerConfig::default());
        // Only a few parts: below the merge window minimum.
        let outcome = pm.merge_once().unwrap();
        assert!(!outcome.merged);
    }

    /// Writes `n` parts of `items_per_part` items each, in disjoint id ranges,
    /// and returns the item total.
    fn seed_parts(pm: &PartManager, dir: &Path, n: u32, items_per_part: u32) -> usize {
        for part_no in 0..n {
            let mut writer = PartWriter::new(1);
            for i in 0..items_per_part {
                let id = part_no * items_per_part + i;
                writer
                    .add(format!("metric{{id={id:08}}}").as_bytes())
                    .unwrap();
            }
            let path = dir.join(format!("seed-{part_no:04}"));
            pm.register_part(writer.finish(path).unwrap());
        }
        (n * items_per_part) as usize
    }

    fn count_items(parts: &[Part]) -> usize {
        let mut search = TableSearch::new(parts, None).unwrap();
        let mut count = 0;
        while search.next_item().unwrap().is_some() {
            count += 1;
        }
        count
    }

    #[test]
    fn test_stats_record_flush_and_merge() {
        let dir = TempDir::new();
        let table = Arc::new(Mutex::new(Table::new()));
        let pm = PartManager::new(table.clone(), &dir.0, ManagerConfig::default());
        let stats = pm.stats();

        // A flush records its bytes.
        for item in make_items(500) {
            table.lock().unwrap().add(&item);
        }
        let flush = pm.flush_once().unwrap();
        assert!(flush.flushed);
        assert_eq!(stats.flushes(), 1);
        assert!(stats.flush_bytes() > 0);
        assert_eq!(stats.merges(), 0);
        // Before any merge the amplification is exactly 1 (flush bytes /
        // flush).
        assert!((stats.write_amplification() - 1.0).abs() < 1e-9);

        // A merge records input and output bytes.
        seed_parts(&pm, &dir.0, 8, 200);
        let merge = pm.merge_once().unwrap();
        assert!(merge.merged);
        assert_eq!(stats.merges(), 1);
        assert_eq!(stats.merge_input_bytes(), merge.input_bytes);
        assert_eq!(stats.merge_output_bytes(), merge.output_bytes);
        assert!(stats.write_amplification() > 1.0);
    }

    #[test]
    fn test_write_amplification_stays_below_target() {
        let dir = TempDir::new();
        let table = Arc::new(Mutex::new(Table::new()));
        let pm = PartManager::new(table.clone(), &dir.0, ManagerConfig::default());
        let stats = pm.stats();

        // Drive several rounds of "8 flushes then merge", the steady state the
        // merge policy is designed for. ADR §3.6 targets write amplification
        // below 5x for index ingestion.
        let rounds = 5;
        let per_flush = 400;
        for round in 0..rounds {
            for _ in 0..8 {
                let base = round * 8 * per_flush;
                for i in 0..per_flush {
                    let id = base + i;
                    table
                        .lock()
                        .unwrap()
                        .add(format!("metric{{id={id:08}}}").as_bytes());
                }
                assert!(pm.flush_once().unwrap().flushed);
            }
            // At least one merge should run once 8 similar parts exist.
            let outcome = pm.merge_once().unwrap();
            assert!(outcome.merged, "round {round} should trigger a merge");
        }
        let wa = stats.write_amplification();
        assert!(
            wa < 5.0,
            "write amplification {wa:.2} exceeds the 5x target"
        );
        // The manager still answers every flushed item exactly once.
        assert_eq!(stats.flushes(), (rounds * 8) as u64);
        assert_eq!(
            count_items(&pm.file_parts()),
            (rounds * 8 * per_flush) as usize
        );
    }

    #[test]
    fn test_concurrent_mergers_do_not_duplicate_work() {
        let dir = TempDir::new();
        let table = Arc::new(Mutex::new(Table::new()));
        let config = ManagerConfig {
            merge_threads: 4,
            ..ManagerConfig::default()
        };
        let pm = Arc::new(PartManager::new(table, &dir.0, config));
        // 16 equal parts: the policy can pick two disjoint windows of 8.
        let total = seed_parts(&pm, &dir.0, 16, 300);

        let threads: Vec<_> = (0..4)
            .map(|_| {
                let pm = Arc::clone(&pm);
                std::thread::spawn(move || pm.merge_once().unwrap())
            })
            .collect();
        let outcomes: Vec<MergeOutcome> = threads.into_iter().map(|t| t.join().unwrap()).collect();

        // Every part claimed by exactly one merge: the merged-id sets are
        // disjoint and together cover the claimed parts.
        let mut all_ids = Vec::new();
        for o in &outcomes {
            all_ids.extend_from_slice(&o.merged_ids);
        }
        let mut unique = all_ids.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(all_ids.len(), unique.len(), "a part was claimed twice");

        // No item is lost or duplicated.
        assert_eq!(count_items(&pm.file_parts()), total);
    }

    #[test]
    fn test_default_merge_threads_is_max_cpus_or_four() {
        let config = ManagerConfig::default();
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        assert_eq!(config.merge_threads, cpus.max(4));
    }

    #[test]
    fn test_spawn_runs_flush_and_configured_mergers() {
        let dir = TempDir::new();
        let table = Arc::new(Mutex::new(Table::new()));
        let config = ManagerConfig {
            flush_interval: std::time::Duration::from_millis(20),
            merge_interval: std::time::Duration::from_millis(20),
            merge_threads: 3,
            ..ManagerConfig::default()
        };
        let pm = Arc::new(PartManager::new(table.clone(), &dir.0, config));
        for item in make_items(200) {
            table.lock().unwrap().add(&item);
        }
        let handles = pm.spawn();
        // One flush loop plus `merge_threads` merger loops.
        assert_eq!(handles.len(), 1 + 3);
        // Wait for the flush to land, then stop and join.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while pm.file_parts().is_empty() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        pm.stop();
        for h in handles {
            h.join().unwrap();
        }
        assert!(pm.stats().flushes() >= 1);
        assert_eq!(count_items(&pm.file_parts()), 200);
    }

    #[test]
    fn test_crash_restart_recovers_written_parts() {
        let dir = TempDir::new();
        // First "process": write two parts and persist parts.json.
        {
            let table = Arc::new(Mutex::new(Table::new()));
            let pm = PartManager::new(table.clone(), &dir.0, ManagerConfig::default());
            for item in make_items(500) {
                table.lock().unwrap().add(&item);
            }
            pm.flush_once().unwrap();
            for item in make_items(500).into_iter().map(|mut v| {
                v.extend_from_slice(b"-2");
                v
            }) {
                table.lock().unwrap().add(&item);
            }
            pm.flush_once().unwrap();
            // Persist the part list as the shutdown path would.
            let paths: Vec<PathBuf> = pm
                .file_parts()
                .iter()
                .map(|p| p.path().to_path_buf())
                .collect();
            let refs: Vec<&Path> = paths.iter().map(|p| p.as_path()).collect();
            crate::mergeset::part::write_parts_json(&dir.0, &refs).unwrap();
            assert_eq!(pm.file_parts().len(), 2);
        }
        // Second "process": a fresh manager recovers from parts.json.
        let table = Arc::new(Mutex::new(Table::new()));
        let pm = PartManager::new(table, &dir.0, ManagerConfig::default());
        let recovered = pm.recover_from_disk().unwrap();
        assert_eq!(recovered, 2);
        let parts = pm.file_parts();
        assert_eq!(parts.len(), 2);
        // The recovered parts answer the full item set.
        let mut search = TableSearch::new(&parts, None).unwrap();
        let mut count = 0;
        while search.next_item().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 1_000);
    }

    #[test]
    fn test_recovery_skips_unfinished_part() {
        let dir = TempDir::new();
        // Write a part, then truncate its metadata so it cannot be opened.
        let table = Arc::new(Mutex::new(Table::new()));
        let pm = PartManager::new(table.clone(), &dir.0, ManagerConfig::default());
        for item in make_items(100) {
            table.lock().unwrap().add(&item);
        }
        pm.flush_once().unwrap();
        let parts = pm.file_parts();
        let bad = parts[0].path().to_path_buf();
        let _ = std::fs::remove_file(bad.join("metadata.json"));
        let refs: Vec<&Path> = vec![parts[0].path()];
        crate::mergeset::part::write_parts_json(&dir.0, &refs).unwrap();

        let pm2 = PartManager::new(
            Arc::new(Mutex::new(Table::new())),
            &dir.0,
            ManagerConfig::default(),
        );
        // The corrupt part is skipped, not loaded.
        assert_eq!(pm2.recover_from_disk().unwrap(), 0);
        assert!(pm2.file_parts().is_empty());
    }

    fn wal_pm(table: Arc<Mutex<Table>>, dir: &TempDir, partition_id: u64) -> PartManager {
        let wal_dir = dir.0.join("wal");
        let wal = WalWriter::open(&wal_dir, partition_id, true).unwrap();
        PartManager::new(table, &dir.0, ManagerConfig::default()).with_wal(
            wal,
            wal_dir,
            partition_id,
        )
    }

    #[test]
    fn test_write_index_items_durable_before_searchable() {
        let dir = TempDir::new();
        let table = Arc::new(Mutex::new(Table::new()));
        let pm = wal_pm(table.clone(), &dir, 202610);
        let stats = pm.stats();

        // Write items: they land in the pending buffer (not yet a searchable
        // file part), but the WAL frame is already durable and fsynced.
        let (min_seq, max_seq) = pm
            .write_index_items(vec![b"metric{id=0}".to_vec(), b"metric{id=1}".to_vec()])
            .unwrap();
        assert!(min_seq >= 1 && max_seq >= min_seq);
        assert_eq!(table.lock().unwrap().pending_len(), 2);
        assert!(pm.file_parts().is_empty());
        // The WAL segment exists on disk.
        assert!(dir.0.join("wal").read_dir().unwrap().count() >= 1);
        assert_eq!(stats.merges(), 0);

        // After flush the item is searchable and the part records its seq
        // range.
        pm.flush_once().unwrap();
        let parts = pm.file_parts();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].header().min_seq, min_seq);
        assert_eq!(parts[0].header().max_seq, max_seq);
    }

    #[test]
    fn test_wal_reclaimed_after_flush_covers_range() {
        let dir = TempDir::new();
        let table = Arc::new(Mutex::new(Table::new()));
        let pm = wal_pm(table, &dir, 202610);

        pm.write_index_items(vec![b"metric{id=0}".to_vec()])
            .unwrap();
        // The WAL segment exists before the flush covers it.
        let segments_before = read_segments(dir.0.join("wal"), false).unwrap();
        assert_eq!(segments_before.len(), 1);
        assert_eq!(segments_before[0].frames.len(), 1);

        pm.flush_once().unwrap();

        // The flushed part covers the frame's range, so the segment is
        // reclaimed.
        assert_eq!(pm.stats().wal_reclaimed(), 1);
        let segments_after = read_segments(dir.0.join("wal"), false).unwrap();
        assert!(
            segments_after.is_empty(),
            "covered segment should be deleted"
        );
    }

    #[test]
    fn test_replay_wal_restores_unflushed_items() {
        let dir = TempDir::new();
        // "First process": write items to the WAL but never flush.
        {
            let table = Arc::new(Mutex::new(Table::new()));
            let pm = wal_pm(table, &dir, 202610);
            for i in 0..50 {
                pm.write_index_items(vec![format!("metric{{id={i:08}}}").into_bytes()])
                    .unwrap();
            }
            // No flush: every frame must be replayed on recovery.
        }
        // "Second process": recover parts (none) then replay the WAL.
        let table = Arc::new(Mutex::new(Table::new()));
        let pm = wal_pm(table.clone(), &dir, 202610);
        assert_eq!(pm.recover_from_disk().unwrap(), 0);
        let replayed = pm.replay_wal().unwrap();
        assert_eq!(replayed, 50);
        assert_eq!(table.lock().unwrap().pending_len(), 50);
        // Flush makes them searchable.
        pm.flush_once().unwrap();
        assert_eq!(count_items(&pm.file_parts()), 50);
    }

    #[test]
    fn test_replay_skips_frames_already_covered_by_part() {
        let dir = TempDir::new();
        // Write + flush so a part covers the range. flush_once reclaims the
        // WAL, so the frame is gone — covered by the part, nothing to replay.
        // This models a crash after the flush: recovery finds the part and no
        // WAL.
        let table = Arc::new(Mutex::new(Table::new()));
        let pm = wal_pm(table, &dir, 202610);
        pm.write_index_items(vec![b"metric{id=0}".to_vec()])
            .unwrap();
        pm.flush_once().unwrap();
        let segments = read_segments(dir.0.join("wal"), false).unwrap();
        assert!(segments.is_empty(), "flush reclaimed the covered segment");

        // A fresh manager finds the part and replays nothing.
        let table2 = Arc::new(Mutex::new(Table::new()));
        let pm2 = wal_pm(table2, &dir, 202610);
        assert_eq!(pm2.recover_from_disk().unwrap(), 1);
        assert_eq!(pm2.replay_wal().unwrap(), 0, "covered frame not replayed");
    }
}
