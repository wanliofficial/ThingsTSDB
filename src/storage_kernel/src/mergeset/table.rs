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

//! Minimal mergeset table: pending items, in-memory parts and file parts.
//!
//! The prototype keeps the search and merge paths of ADR-0001 but defers
//! background workers, caches, per-part locks and the merge-selection policy.

use std::path::Path;

use crate::mergeset::{
    part::{Part, PartWriter},
    table_search::TableSearch,
};

/// A mergeset table over sorted byte items.
pub struct Table {
    parts: Vec<Part>,
    pending: Vec<Vec<u8>>,
    /// Rows a flush has taken out of `pending` but not yet published as a
    /// part.
    ///
    /// Without this the flush window would be a visibility hole: the rows are
    /// acknowledged before the flush starts, and the new part is only
    /// searchable once it is registered, so a query landing between the two
    /// would see neither. The flush keeps its own copy of the batch to sort and
    /// write; the search path unions `pending` with this buffer until the part
    /// is published.
    flushing: Vec<Vec<u8>>,
    /// Sequence range of the batch in `flushing`, restored when a flush aborts
    /// so the retry's part header still covers the same WAL frames.
    flushing_seq: (u64, u64),
    /// Highest item sequence handed out by [`Table::reserve_seq_range`].
    next_seq: u64,
    /// Union of the reserved sequence ranges of the pending buffer. `seq_min`
    /// is 0 until the first item with a sequence range is added.
    seq_min: u64,
    seq_max: u64,
    /// Invoked after a flush makes the pending buffer searchable (the flush
    /// callback invalidates the tag-filter cache, ADR-0001 §4.2/§6.1).
    flush_callback: Option<FlushCallback>,
}

/// A callback fired when items become searchable after a flush. The callback
/// is shared so it can be moved into the background flush loop.
pub type FlushCallback = std::sync::Arc<dyn Fn() + Send + Sync>;

impl Default for Table {
    fn default() -> Self {
        Self::new()
    }
}

impl Table {
    pub fn new() -> Self {
        Self {
            parts: Vec::new(),
            pending: Vec::new(),
            flushing: Vec::new(),
            flushing_seq: (0, 0),
            next_seq: 1,
            seq_min: 0,
            seq_max: 0,
            flush_callback: None,
        }
    }

    pub fn from_parts(parts: Vec<Part>) -> Result<Self, String> {
        let mut parts = parts;
        parts.sort_by(|a, b| a.first_item().cmp(b.first_item()));
        Ok(Self {
            parts,
            pending: Vec::new(),
            flushing: Vec::new(),
            flushing_seq: (0, 0),
            next_seq: 1,
            seq_min: 0,
            seq_max: 0,
            flush_callback: None,
        })
    }

    /// Installs a flush callback that runs after every flush makes the pending
    /// buffer searchable (used to invalidate the tag-filter cache).
    pub fn set_flush_callback(&mut self, cb: FlushCallback) {
        self.flush_callback = Some(cb);
    }

    /// Reserves `n` item sequence numbers and returns the inclusive range
    /// `[min_seq, max_seq]` the caller must attach to the items before
    /// appending them to the index WAL (ADR-0001 §6.3). The range is reserved
    /// up front so the WAL frame header and the items agree.
    pub fn reserve_seq_range(&mut self, n: u64) -> (u64, u64) {
        let min = self.next_seq;
        let max = self.next_seq + n.saturating_sub(1);
        self.next_seq = max + 1;
        (min, max)
    }

    /// Adds `items` to the pending buffer, recording the reserved sequence
    /// range so a later flush can stamp it into the part header for WAL
    /// coverage.
    pub fn add_items_with_seq_range(
        &mut self,
        items: impl IntoIterator<Item = Vec<u8>>,
        min_seq: u64,
        max_seq: u64,
    ) {
        if min_seq > 0 {
            self.seq_min = if self.seq_min == 0 {
                min_seq
            } else {
                self.seq_min.min(min_seq)
            };
            self.seq_max = self.seq_max.max(max_seq);
        }
        self.pending.extend(items);
    }

    /// Takes the pending items together with the union of their sequence
    /// ranges. Returns `None` when the pending buffer is empty.
    pub fn take_pending_with_seq(&mut self) -> Option<(Vec<Vec<u8>>, u64, u64)> {
        if self.pending.is_empty() {
            return None;
        }
        let items = std::mem::take(&mut self.pending);
        // The caller sorts, merges and writes its copy; the search path keeps
        // seeing the rows through `flushing` until the part is published.
        self.flushing = items.clone();
        let (min_seq, max_seq) = (self.seq_min, self.seq_max);
        self.flushing_seq = (min_seq, max_seq);
        self.seq_min = 0;
        self.seq_max = 0;
        Some((items, min_seq, max_seq))
    }

    pub fn parts(&self) -> &[Part] {
        &self.parts
    }

    /// Replaces the file-part set with `parts`, which the part manager owns.
    ///
    /// The manager keeps the authoritative list (flush, merge, recovery) and
    /// pushes it here so the search path sees the same parts as the WAL
    /// coverage check. Parts are cheap to clone (an open of the same directory)
    /// and the list is bounded by the merge policy.
    pub fn set_parts(&mut self, mut parts: Vec<Part>) {
        parts.sort_by(|a, b| a.first_item().cmp(b.first_item()));
        self.parts = parts;
    }

    pub fn pending_len(&self) -> usize {
        self.pending.len() + self.flushing.len()
    }

    /// Drops the rows of the last taken batch.
    ///
    /// Called once the part that holds them is registered, so the search path
    /// never loses them.
    pub fn finish_flush(&mut self) {
        self.flushing.clear();
        self.flushing_seq = (0, 0);
    }

    /// Puts the rows of a taken batch back into the pending buffer.
    ///
    /// Called when the flush could not publish its part, so the rows stay
    /// visible and the next attempt picks them up again.
    pub fn abort_flush(&mut self) {
        if self.flushing.is_empty() {
            return;
        }
        let flushing = std::mem::take(&mut self.flushing);
        self.pending.extend(flushing);
        let (min_seq, max_seq) = std::mem::take(&mut self.flushing_seq);
        if min_seq > 0 {
            self.seq_min = if self.seq_min == 0 {
                min_seq
            } else {
                self.seq_min.min(min_seq)
            };
            self.seq_max = self.seq_max.max(max_seq);
        }
    }

    pub fn add(&mut self, item: &[u8]) {
        self.pending.push(item.to_vec());
    }

    pub fn add_owned(&mut self, item: Vec<u8>) {
        self.pending.push(item);
    }

    /// Takes the pending items out of the table, leaving it empty. Used by the
    /// part manager flush loop, which writes the pending buffer to a file part.
    pub fn take_pending(&mut self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.pending)
    }

    /// Ensures the pending buffer is empty (drops any items).
    pub fn clear_pending(&mut self) {
        self.pending.clear();
        self.flushing.clear();
        self.flushing_seq = (0, 0);
        self.seq_min = 0;
        self.seq_max = 0;
    }

    /// Sorts pending items and writes them as a new file part. The part header
    /// records the union of the pending sequence range so the WAL coverage
    /// check can reclaim the corresponding segments.
    pub fn flush(&mut self, path: impl AsRef<Path>, compress_level: i32) -> Result<(), String> {
        let Some((pending, min_seq, max_seq)) = self.take_pending_with_seq() else {
            return Ok(());
        };
        self.flush_sorted(path, &pending, min_seq, max_seq, compress_level)?;
        self.finish_flush();
        Ok(())
    }

    /// Writes an already-taken set of items as a part with a known sequence
    /// range, then fires the flush callback. Shared by `flush` and the part
    /// manager's flush loop.
    fn flush_sorted(
        &mut self,
        path: impl AsRef<Path>,
        items: &[Vec<u8>],
        min_seq: u64,
        max_seq: u64,
        compress_level: i32,
    ) -> Result<(), String> {
        let mut sorted = items.to_vec();
        sorted.sort_unstable();
        let mut writer = PartWriter::new(compress_level);
        writer.set_seq_range(min_seq, max_seq);
        for item in &sorted {
            writer.add(item)?;
        }
        self.parts.push(writer.finish(path)?);
        if let Some(cb) = &self.flush_callback {
            cb();
        }
        Ok(())
    }

    /// Returns the union of all items with `prefix`, sorted and deduplicated.
    ///
    /// The pending buffer is included: a write must be searchable as soon as it
    /// is acknowledged, and the buffer is bounded by the flush interval.
    pub fn search_prefix(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, String> {
        let mut dst = self.search_prefix_parts(prefix)?;
        self.merge_pending(&mut dst, |item| item.starts_with(prefix));
        Ok(dst)
    }

    /// Like [`Table::search_prefix`] but without the pending buffer.
    pub fn search_prefix_parts(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, String> {
        let mut dst = Vec::new();
        let mut search = TableSearch::new(&self.parts, Some(prefix))?;
        while let Some(item) = search.next_item()? {
            if !item.starts_with(prefix) {
                break;
            }
            dst.push(item);
        }
        dst.dedup();
        Ok(dst)
    }

    /// Returns the union of all items in `[start, end)`, sorted and
    /// deduplicated, including the pending buffer.
    pub fn search_range(&self, start: &[u8], end: &[u8]) -> Result<Vec<Vec<u8>>, String> {
        let mut dst = Vec::new();
        let mut search = TableSearch::new(&self.parts, Some(start))?;
        while let Some(item) = search.next_item()? {
            if item.as_slice() >= end {
                break;
            }
            dst.push(item);
        }
        dst.dedup();
        self.merge_pending(&mut dst, |item| item >= start && item < end);
        Ok(dst)
    }

    /// Merges the matching pending items into a sorted, deduplicated item list.
    fn merge_pending(&self, dst: &mut Vec<Vec<u8>>, keep: impl Fn(&[u8]) -> bool) {
        if self.pending.is_empty() && self.flushing.is_empty() {
            return;
        }
        let mut pending: Vec<&Vec<u8>> = self
            .pending
            .iter()
            .chain(self.flushing.iter())
            .filter(|it| keep(it))
            .collect();
        if pending.is_empty() {
            return;
        }
        if dst.is_empty() {
            pending.sort_unstable();
            pending.dedup();
            dst.extend(pending.into_iter().cloned());
            return;
        }
        pending.sort_unstable();
        pending.dedup();
        let mut merged = Vec::with_capacity(dst.len() + pending.len());
        let (mut i, mut j) = (0usize, 0usize);
        while i < dst.len() && j < pending.len() {
            match dst[i].as_slice().cmp(pending[j].as_slice()) {
                std::cmp::Ordering::Less => {
                    merged.push(std::mem::take(&mut dst[i]));
                    i += 1;
                }
                std::cmp::Ordering::Greater => {
                    merged.push(pending[j].clone());
                    j += 1;
                }
                std::cmp::Ordering::Equal => {
                    merged.push(std::mem::take(&mut dst[i]));
                    i += 1;
                    j += 1;
                }
            }
        }
        merged.extend(dst.drain(i..));
        merged.extend(pending[j..].iter().map(|it| (*it).clone()));
        *dst = merged;
    }

    /// Merges every part into a single part at `out_path`, deduplicating
    /// identical items. The input part files are removed on success.
    pub fn merge_all(
        &mut self,
        out_path: impl AsRef<Path>,
        compress_level: i32,
    ) -> Result<(), String> {
        if self.parts.len() <= 1 {
            return Ok(());
        }
        let mut cursors = Vec::with_capacity(self.parts.len());
        for part in &self.parts {
            cursors.push(part.cursor()?);
        }

        let mut writer = PartWriter::new(compress_level);
        // A merged part's sequence range is the union of its sources' ranges,
        // so merging never creates a coverage hole (ADR-0001 §6.3).
        let (min_seq, max_seq) = self.parts.iter().fold((0u64, 0u64), |(mn, mx), p| {
            let (lo, hi) = (p.header().min_seq, p.header().max_seq);
            (
                if lo > 0 {
                    if mn == 0 {
                        lo
                    } else {
                        mn.min(lo)
                    }
                } else {
                    mn
                },
                mx.max(hi),
            )
        });
        writer.set_seq_range(min_seq, max_seq);
        loop {
            let mut best: Option<(usize, Vec<u8>)> = None;
            for (idx, cursor) in cursors.iter().enumerate() {
                let Some(item) = cursor.current() else {
                    continue;
                };
                match &best {
                    Some((_, current)) if current.as_slice() <= item => {}
                    _ => best = Some((idx, item.to_vec())),
                }
            }
            let Some((_, item)) = best else {
                break;
            };
            writer.add(&item)?;
            for cursor in cursors.iter_mut() {
                while cursor.current().is_some_and(|x| x == item.as_slice()) {
                    cursor.advance()?;
                }
            }
        }

        let new_part = writer.finish(out_path)?;
        let old_parts = std::mem::replace(&mut self.parts, vec![new_part]);
        for part in old_parts {
            let _ = std::fs::remove_dir_all(part.path());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    use super::*;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "storage-kernel-mergeset-{}-{n}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn join(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn make_items(n: usize) -> Vec<Vec<u8>> {
        (0..n)
            .map(|i| format!("metric{:03}\x02series{:06}", i % 100, i).into_bytes())
            .collect()
    }

    #[test]
    fn test_flush_search_and_reopen() {
        let dir = TempDir::new();
        let mut table = Table::new();
        let mut items = make_items(20_000);
        items.sort();
        for item in &items {
            table.add(item);
        }
        table.flush(dir.join("part-1"), 1).unwrap();
        assert_eq!(table.parts().len(), 1);
        assert_eq!(table.parts()[0].item_count, items.len() as u64);

        let all = table.search_range(b"metric000", b"metric999").unwrap();
        assert_eq!(all, items);

        let prefix = table.search_prefix(b"metric007\x02series").unwrap();
        let expected: Vec<_> = items
            .iter()
            .filter(|x| x.starts_with(b"metric007\x02series"))
            .cloned()
            .collect();
        assert_eq!(prefix, expected);

        let reopened = Part::open(dir.join("part-1")).unwrap();
        assert_eq!(reopened.item_count, items.len() as u64);
        let mut table2 = Table::from_parts(vec![reopened]).unwrap();
        assert_eq!(
            table2.search_range(b"metric000", b"metric999").unwrap(),
            items
        );

        // A fresh part still sees the same items after a flush.
        table2.add(b"metric000\x02series999999");
        table2.flush(dir.join("part-2"), 1).unwrap();
        assert_eq!(table2.parts().len(), 2);
    }

    #[test]
    fn test_merge_dedups_and_removes_inputs() {
        let dir = TempDir::new();
        let mut table = Table::new();

        let mut a = make_items(5_000);
        a.sort();
        for item in &a {
            table.add(item);
        }
        table.flush(dir.join("part-a"), 1).unwrap();

        // The second part overlaps the first one and adds new items.
        let mut b = a[4_000..].to_vec();
        b.extend(make_items(1_000));
        b.sort();
        for item in &b {
            table.add(item);
        }
        table.flush(dir.join("part-b"), 1).unwrap();

        let mut expected: Vec<_> = a.iter().chain(b.iter()).cloned().collect();
        expected.sort();
        expected.dedup();

        table.merge_all(dir.join("part-merged"), 1).unwrap();
        assert_eq!(table.parts().len(), 1);
        assert!(!dir.join("part-a").exists());
        assert!(!dir.join("part-b").exists());
        assert_eq!(table.parts()[0].path(), dir.join("part-merged"));
        assert_eq!(table.search_range(b"", b"\xff\xff").unwrap(), expected);
    }

    #[test]
    fn test_flush_stamps_sequence_range_into_header() {
        let dir = TempDir::new();
        let mut table = Table::new();
        table.add_items_with_seq_range(vec![b"metric000\x02series1".to_vec()], 11, 15);
        table.flush(dir.join("part-seq"), 1).unwrap();
        let header = table.parts()[0].header();
        assert_eq!(header.min_seq, 11);
        assert_eq!(header.max_seq, 15);
    }

    #[test]
    fn test_flush_callback_fires_after_flush() {
        let dir = TempDir::new();
        let mut table = Table::new();
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let cb = {
            let calls = std::sync::Arc::clone(&calls);
            std::sync::Arc::new(move || {
                calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            })
        };
        table.set_flush_callback(cb);
        table.add(b"metric000\x02series1");
        table.flush(dir.join("part-cb"), 1).unwrap();
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    /// A flush takes the pending rows out before their part exists. Those rows
    /// are already acknowledged, so the search path must keep seeing them for
    /// the whole window; otherwise a query that lands mid-flush misses data
    /// that a query before and after it finds.
    #[test]
    fn test_taken_batch_stays_searchable_until_its_part_is_published() {
        let mut table = Table::new();
        table.add(b"metric000\x02series1");
        table.add(b"metric000\x02series2");

        // The flush takes the batch but has not published a part yet.
        let (items, _, _) = table.take_pending_with_seq().unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(
            table.search_prefix(b"metric000").unwrap().len(),
            2,
            "the taken rows must stay searchable while the part is written"
        );
        // Rows written during the flush are visible too.
        table.add(b"metric000\x02series3");
        assert_eq!(table.search_prefix(b"metric000").unwrap().len(), 3);

        // Publishing the part drops the batch; the written rows remain.
        table.finish_flush();
        assert_eq!(table.pending_len(), 1);
        assert_eq!(table.search_prefix(b"metric000").unwrap().len(), 1);
    }

    /// A failed flush must not swallow the rows it took.
    #[test]
    fn test_aborted_batch_returns_to_pending() {
        let mut table = Table::new();
        let (min_seq, max_seq) = table.reserve_seq_range(1);
        table.add_items_with_seq_range([b"metric000\x02series1".to_vec()], min_seq, max_seq);
        let (items, taken_min, taken_max) = table.take_pending_with_seq().unwrap();
        assert_eq!((taken_min, taken_max), (min_seq, max_seq));
        assert_eq!(table.pending_len(), 1, "still visible while flushing");

        table.abort_flush();
        assert_eq!(table.pending_len(), 1);
        let (again, again_min, again_max) = table.take_pending_with_seq().unwrap();
        assert_eq!(again, items);
        assert_eq!(
            (again_min, again_max),
            (min_seq, max_seq),
            "the retry must still cover the same WAL frames"
        );
    }

    #[test]
    fn test_reserve_seq_range_is_monotonic_and_non_overlapping() {
        let mut table = Table::new();
        assert_eq!(table.reserve_seq_range(3), (1, 3));
        assert_eq!(table.reserve_seq_range(1), (4, 4));
        assert_eq!(table.reserve_seq_range(10), (5, 14));
    }

    #[test]
    fn test_merge_all_union_sequence_range() {
        let dir = TempDir::new();
        let mut table = Table::new();
        table.add_items_with_seq_range(make_items(50), 100, 149);
        table.flush(dir.join("part-lo"), 1).unwrap();
        table.add_items_with_seq_range(make_items(50), 200, 249);
        table.flush(dir.join("part-hi"), 1).unwrap();
        table.merge_all(dir.join("part-union"), 1).unwrap();
        let header = table.parts()[0].header();
        assert_eq!(header.min_seq, 100);
        assert_eq!(header.max_seq, 249);
    }
}
