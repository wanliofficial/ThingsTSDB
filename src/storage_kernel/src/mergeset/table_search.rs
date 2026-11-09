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

//! k-way merge cursor over the parts of one mergeset table (ADR-0001 §3.5).
//!
//! Duplicates across parts are **not** removed here: index rows are written
//! idempotently, and the indexDB layer deduplicates through its posting sets.
//! The cursor yields items in non-decreasing order.

use std::{cmp::Reverse, collections::BinaryHeap};

use crate::mergeset::part::{BlockCursor, Part};

/// A seekable, sorted stream over every item of a set of parts.
pub struct TableSearch<'a> {
    cursors: Vec<BlockCursor<'a>>,
    heap: BinaryHeap<Reverse<(Vec<u8>, usize)>>,
}

impl<'a> TableSearch<'a> {
    /// Starts a search over `parts`. `start` is inclusive; `None` starts at the
    /// first item.
    pub fn new(parts: &'a [Part], start: Option<&[u8]>) -> Result<Self, String> {
        let mut cursors = Vec::with_capacity(parts.len());
        for part in parts {
            let cursor = match start {
                Some(key) => part.seek_cursor(key)?,
                None => part.cursor()?,
            };
            cursors.push(cursor);
        }
        let mut heap = BinaryHeap::with_capacity(cursors.len());
        for (idx, cursor) in cursors.iter().enumerate() {
            if let Some(item) = cursor.current() {
                heap.push(Reverse((item.to_vec(), idx)));
            }
        }
        Ok(Self { cursors, heap })
    }

    /// Returns the next item, or `None` when every cursor is exhausted.
    ///
    /// Deliberately not named `next`: this is not an [`Iterator`] (it can fail
    /// and borrows the parts for the cursor's lifetime), so it should not look
    /// like one at the call site.
    pub fn next_item(&mut self) -> Result<Option<Vec<u8>>, String> {
        let Some(Reverse((item, idx))) = self.heap.pop() else {
            return Ok(None);
        };
        self.cursors[idx].advance()?;
        if let Some(next) = self.cursors[idx].current() {
            self.heap.push(Reverse((next.to_vec(), idx)));
        }
        Ok(Some(item))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::mergeset::part::PartWriter;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "mergeset-search-{tag}-{}-{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            let _ = std::fs::remove_dir_all(&path);
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

    fn make_part(dir: &TempDir, name: &str, items: &[&[u8]]) -> Part {
        let mut writer = PartWriter::new(1);
        for item in items {
            writer.add(item).unwrap();
        }
        writer.finish(dir.join(name)).unwrap()
    }

    #[test]
    fn test_table_search_merges_parts_in_order_with_duplicates() {
        let dir = TempDir::new("merge");
        let p0 = make_part(&dir, "p0", &[b"a", b"c", b"e", b"g"]);
        let p1 = make_part(&dir, "p1", &[b"b", b"c", b"f", b"g"]);
        let p2 = make_part(&dir, "p2", &[b"a", b"d", b"g", b"g", b"h"]);
        let parts = vec![p0, p1, p2];
        let mut search = TableSearch::new(&parts, None).unwrap();
        let mut got = Vec::new();
        while let Some(item) = search.next_item().unwrap() {
            got.push(String::from_utf8(item).unwrap());
        }
        assert_eq!(
            got,
            ["a", "a", "b", "c", "c", "d", "e", "f", "g", "g", "g", "g", "h"]
        );
    }

    #[test]
    fn test_table_search_seek_is_inclusive() {
        let dir = TempDir::new("seek");
        let p0 = make_part(&dir, "p0", &[b"a", b"d", b"g"]);
        let p1 = make_part(&dir, "p1", &[b"b", b"d", b"h"]);
        let parts = vec![p0, p1];
        let mut search = TableSearch::new(&parts, Some(b"d")).unwrap();
        let mut got = Vec::new();
        while let Some(item) = search.next_item().unwrap() {
            got.push(String::from_utf8(item).unwrap());
        }
        assert_eq!(got, ["d", "d", "g", "h"]);
    }

    #[test]
    fn test_table_search_spans_many_blocks() {
        // Wide fixed-size items force many blocks per part.
        let dir = TempDir::new("blocks");
        let key = |i: u32| format!("{i:040}").into_bytes();
        let items: Vec<Vec<u8>> = (0..20_000u32).map(key).collect();
        let refs: Vec<&[u8]> = items.iter().map(Vec::as_slice).collect();
        let p0 = make_part(&dir, "p0", &refs[..10_000]);
        let p1 = make_part(&dir, "p1", &refs[10_000..]);
        assert!(p0.header().blocks_count > 1, "test must span blocks");
        let parts = vec![p0, p1];
        let mut search = TableSearch::new(&parts, None).unwrap();
        let mut count = 0;
        let mut last: Option<Vec<u8>> = None;
        while let Some(item) = search.next_item().unwrap() {
            if let Some(prev) = &last {
                assert!(prev <= &item, "output must be sorted");
            }
            last = Some(item);
            count += 1;
        }
        assert_eq!(count, 20_000);
        let seek_key = key(5_000);
        let mut seek = TableSearch::new(&parts, Some(&seek_key)).unwrap();
        let first = seek.next_item().unwrap().unwrap();
        assert_eq!(first, seek_key);
    }
}
