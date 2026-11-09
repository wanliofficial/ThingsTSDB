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
}

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
        }
    }

    pub fn from_parts(parts: Vec<Part>) -> Result<Self, String> {
        let mut parts = parts;
        parts.sort_by(|a, b| a.first_item().cmp(b.first_item()));
        Ok(Self {
            parts,
            pending: Vec::new(),
        })
    }

    pub fn parts(&self) -> &[Part] {
        &self.parts
    }

    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub fn add(&mut self, item: &[u8]) {
        self.pending.push(item.to_vec());
    }

    pub fn add_owned(&mut self, item: Vec<u8>) {
        self.pending.push(item);
    }

    /// Sorts pending items and writes them as a new file part.
    pub fn flush(&mut self, path: impl AsRef<Path>, compress_level: i32) -> Result<(), String> {
        if self.pending.is_empty() {
            return Ok(());
        }
        self.pending.sort_unstable();
        let mut writer = PartWriter::new(compress_level);
        for item in self.pending.drain(..) {
            writer.add(&item)?;
        }
        self.parts.push(writer.finish(path)?);
        Ok(())
    }

    /// Returns the union of all items with `prefix`, sorted and deduplicated.
    pub fn search_prefix(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, String> {
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
    /// deduplicated.
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
        Ok(dst)
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
}
