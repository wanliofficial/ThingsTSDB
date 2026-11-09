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

//! A single block cache with LRU eviction (ADR-0001 §4.2).
//!
//! The indexDB caches decoded block headers so repeated seeks do not re-read
//! and re-decompress the same index block. Parts larger than the cached-part
//! threshold are admitted but not pinned; they may be evicted under pressure
//! and re-read on demand. The cache is bounded by a byte budget.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

/// One cached index block: its decompressed block headers, keyed by part.
#[derive(Debug, Clone)]
pub struct CachedBlock {
    pub part_path: String,
    /// The block headers, in first-item order.
    pub headers: Vec<Vec<u8>>,
}

/// A byte-budgeted cache of decompressed index blocks.
pub struct BlockCache {
    inner: Mutex<BlockCacheInner>,
    /// Maximum total bytes of cached headers.
    byte_budget: usize,
}

struct BlockCacheInner {
    entries: HashMap<String, CachedBlock>,
    /// Approximate bytes held by cached headers.
    bytes: usize,
    /// Insertion order for simple FIFO eviction (oldest first).
    order: Vec<String>,
}

impl BlockCache {
    /// Creates a cache that holds at most `byte_budget` bytes of headers.
    pub fn new(byte_budget: usize) -> Self {
        Self {
            inner: Mutex::new(BlockCacheInner {
                entries: HashMap::new(),
                bytes: 0,
                order: Vec::new(),
            }),
            byte_budget,
        }
    }

    /// Returns the cached block for `part_path`, if present.
    pub fn get(&self, part_path: &str) -> Option<CachedBlock> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.entries.get(part_path).cloned()
    }

    /// Inserts (or replaces) a cached block, evicting oldest entries when the
    /// budget is exceeded.
    pub fn put(&self, part_path: impl Into<String>, block: CachedBlock) {
        let part_path = part_path.into();
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let added = header_bytes(&block);
        if let Some(old) = inner.entries.remove(&part_path) {
            inner.bytes -= header_bytes(&old);
        }
        inner.bytes += added;
        inner.entries.insert(part_path.clone(), block);
        inner.order.retain(|p| p != &part_path);
        inner.order.push(part_path);
        while inner.bytes > self.byte_budget && inner.order.len() > 1 {
            let evict = inner.order.remove(0);
            if let Some(old) = inner.entries.remove(&evict) {
                inner.bytes -= header_bytes(&old);
            }
        }
    }

    /// Number of cached parts.
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entries
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn header_bytes(block: &CachedBlock) -> usize {
    block.headers.iter().map(|h| h.len()).sum()
}

/// The shared cache type used across the indexDB and its workers.
pub type SharedBlockCache = Arc<BlockCache>;

#[cfg(test)]
mod tests {
    use super::*;

    fn block(part: &str, header: &[u8]) -> CachedBlock {
        CachedBlock {
            part_path: part.to_string(),
            headers: vec![header.to_vec()],
        }
    }

    #[test]
    fn test_put_and_get() {
        let cache = BlockCache::new(1024);
        cache.put("p0", block("p0", b"header"));
        assert_eq!(cache.get("p0").unwrap().headers, vec![b"header".to_vec()]);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_evicts_oldest_over_budget() {
        // Budget holds only one header at a time.
        let cache = BlockCache::new(8);
        cache.put("p0", block("p0", b"aaaa")); // 4 bytes
        cache.put("p1", block("p1", b"bbbb")); // 4 bytes -> total 8, ok
        assert!(cache.get("p0").is_some());
        cache.put("p2", block("p2", b"cccc")); // evicts p0
        assert!(cache.get("p0").is_none());
        assert!(cache.get("p1").is_some());
        assert!(cache.get("p2").is_some());
    }

    #[test]
    fn test_replace_does_not_double_count() {
        let cache = BlockCache::new(1024);
        cache.put("p0", block("p0", b"12345678"));
        cache.put("p0", block("p0", b"9"));
        assert_eq!(cache.get("p0").unwrap().headers, vec![b"9".to_vec()]);
    }
}
