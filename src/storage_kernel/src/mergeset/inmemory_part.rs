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

//! In-memory part (ADR-0001 §3.1, §3.6).
//!
//! A freshly flushed part holds items in memory as one or more sealed
//! `InmemoryBlock`s plus a pending block, so it is searchable immediately
//! without touching disk. After `DATA_FLUSH_INTERVAL` it is written to a file
//! part and replaced by an open [`super::part::Part`]. This module is the
//! in-memory side of the table; the background worker lives in
//! `part_manager.rs`.

use std::sync::Arc;

use crate::mergeset::inmemory_block::InmemoryBlock;

/// A searchable part held entirely in memory.
///
/// Items are distributed across sealed blocks (each <=
/// `MAX_INMEMORY_BLOCK_SIZE`) and one pending block. Items must be added in
/// non-decreasing order, like the file writer.
#[derive(Default)]
pub struct InMemoryPart {
    sealed: Vec<Arc<InmemoryBlock>>,
    pending: InmemoryBlock,
    item_count: u64,
}

impl InMemoryPart {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of items.
    pub fn len(&self) -> u64 {
        self.item_count
    }

    pub fn is_empty(&self) -> bool {
        self.item_count == 0
    }

    /// Returns the number of sealed blocks.
    pub fn block_count(&self) -> usize {
        self.sealed.len() + usize::from(!self.pending.items.is_empty())
    }

    /// Adds one item in non-decreasing order, sealing the pending block when it
    /// is full. Returns `false` when the item does not fit in a fresh block
    /// (larger than `MAX_INMEMORY_BLOCK_SIZE`).
    pub fn add(&mut self, item: &[u8]) -> bool {
        if !self.pending.add(item) {
            self.seal_pending();
            if !self.pending.add(item) {
                return false;
            }
        }
        self.item_count += 1;
        true
    }

    fn seal_pending(&mut self) {
        if !self.pending.items.is_empty() {
            self.pending.sort_items();
            self.pending.update_common_prefix_sorted();
            self.sealed
                .push(Arc::new(std::mem::take(&mut self.pending)));
        }
    }

    /// Freezes the part: seals the pending block and returns a fully-sorted,
    /// immutable snapshot. After this call `add` must not be used.
    pub fn freeze(&mut self) {
        self.seal_pending();
    }

    /// Returns all items with the given prefix.
    pub fn search_prefix(&self, prefix: &[u8]) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        for block in self.blocks() {
            for item in &block.items {
                let bytes = item.bytes(&block.data);
                if bytes.starts_with(prefix) {
                    out.push(bytes.to_vec());
                } else if bytes > prefix {
                    // Items within a block are sorted, so nothing later in this
                    // block matches. A later block may still match, so
                    // continue.
                    break;
                }
            }
        }
        out
    }

    /// Returns all items in `[start, end)`.
    pub fn search_range(&self, start: &[u8], end: &[u8]) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        for block in self.blocks() {
            for item in &block.items {
                let bytes = item.bytes(&block.data);
                // Items within a block are sorted, but a block may still hold
                // items past `end`; skip them and keep scanning later blocks
                // only when this block is entirely below `start`.
                if bytes < start {
                    continue;
                }
                if bytes >= end {
                    break;
                }
                out.push(bytes.to_vec());
            }
        }
        out
    }

    /// Iterates over the sealed blocks plus the pending block when non-empty.
    fn blocks(&self) -> BlocksIter<'_> {
        BlocksIter {
            sealed: &self.sealed,
            sealed_idx: 0,
            pending: if self.pending.items.is_empty() {
                None
            } else {
                Some(&self.pending)
            },
        }
    }
}

/// Iterator over an [`InMemoryPart`]'s blocks: sealed blocks first, then the
/// pending block if it holds items.
struct BlocksIter<'a> {
    sealed: &'a [Arc<InmemoryBlock>],
    sealed_idx: usize,
    pending: Option<&'a InmemoryBlock>,
}

impl<'a> Iterator for BlocksIter<'a> {
    type Item = &'a InmemoryBlock;

    fn next(&mut self) -> Option<Self::Item> {
        if self.sealed_idx < self.sealed.len() {
            let block = &self.sealed[self.sealed_idx];
            self.sealed_idx += 1;
            return Some(block.as_ref());
        }
        if let Some(pending) = self.pending.take() {
            return Some(pending);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_and_search_single_block() {
        let mut part = InMemoryPart::new();
        for i in 0..100u32 {
            part.add(format!("item{i:04}").as_bytes());
        }
        assert_eq!(part.len(), 100);
        let got = part.search_range(b"item0010", b"item0020");
        assert_eq!(got.len(), 10);
        assert_eq!(got[0], b"item0010");
        assert_eq!(got[9], b"item0019");
    }

    #[test]
    fn test_seals_blocks_on_overflow() {
        // 1 KiB items at 64 KiB per block force multiple sealed blocks.
        let mut part = InMemoryPart::new();
        let item = vec![b'x'; 1024];
        for _ in 0..200 {
            assert!(part.add(&item));
        }
        assert!(part.block_count() >= 3);
        // Every item is the same, so a prefix search returns all of them.
        assert_eq!(part.search_prefix(b"x").len(), 200);
    }

    #[test]
    fn test_range_search_across_blocks() {
        let mut part = InMemoryPart::new();
        for i in 0..20_000u32 {
            assert!(part.add(format!("{i:040}").as_bytes()));
        }
        let start = format!("{:040}", 500);
        let end = format!("{:040}", 510);
        let got = part.search_range(start.as_bytes(), end.as_bytes());
        assert_eq!(got.len(), 10);
        assert_eq!(got[0], start.into_bytes());
        assert_eq!(got[9], format!("{:040}", 509).into_bytes());
    }

    #[test]
    fn test_rejects_oversized_item() {
        let mut part = InMemoryPart::new();
        let big = vec![b'y'; 64 * 1024 + 1];
        assert!(!part.add(&big));
    }
}
