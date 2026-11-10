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

//! Per-series latest value cache (roadmap Phase 3).
//!
//! Dashboard "current value" queries dominate metric workloads, and answering
//! them by scanning the newest data partitions is wasteful. The write path
//! mirrors every sample into this map, so an instant query is an index lookup
//! plus a hash probe instead of a data scan.
//!
//! The cache is a pure optimization: on a miss (restart, eviction) the query
//! falls back to scanning the requested range and promotes the result back into
//! the map. A full cache stops accepting new series rather than evicting, so a
//! high-cardinality burst cannot push out the active dashboards.

use std::{collections::HashMap, sync::Mutex};

use crate::types::Sample;

/// Entries kept by default. One entry is 16 B, so the default cap is ~16 MiB;
/// a 10M-series deployment can raise it to ~160 MiB (the design note estimates
/// ~1 GB at 10M with label copies, which this cache does not hold).
pub const DEFAULT_LAST_CACHE_ENTRIES: usize = 1_000_000;

/// The newest sample of one series.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LastValue {
    pub timestamp: i64,
    pub value: f64,
}

pub struct LastCache {
    entries: Mutex<HashMap<u64, LastValue>>,
    capacity: usize,
}

impl LastCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            capacity,
        }
    }

    pub fn with_default_capacity() -> Self {
        Self::new(DEFAULT_LAST_CACHE_ENTRIES)
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        self.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Mirrors written samples into the cache. Samples must carry their series
    /// id, which the index manager populates before the data write.
    pub fn update(&self, samples: &[Sample]) {
        for sample in samples {
            let Some(id) = sample.series_id else {
                continue;
            };
            self.observe(id.0, sample.timestamp, sample.value);
        }
    }

    /// Records one sample, keeping the newest timestamp. An equal timestamp
    /// overwrites, matching last-write-wins semantics.
    pub fn observe(&self, series_id: u64, timestamp: i64, value: f64) {
        let mut entries = self.lock();
        match entries.get_mut(&series_id) {
            Some(current) if current.timestamp > timestamp => {}
            Some(current) if current.timestamp == timestamp => current.value = value,
            Some(current) => {
                *current = LastValue { timestamp, value };
            }
            None => {
                if entries.len() < self.capacity {
                    entries.insert(series_id, LastValue { timestamp, value });
                }
            }
        }
    }

    pub fn get(&self, series_id: u64) -> Option<LastValue> {
        self.lock().get(&series_id).copied()
    }

    /// Drops every entry; the next query falls back to a data scan.
    pub fn clear(&self) {
        self.lock().clear();
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<u64, LastValue>> {
        self.lock_inner()
    }

    fn lock_inner(&self) -> std::sync::MutexGuard<'_, HashMap<u64, LastValue>> {
        self.entries.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_observe_keeps_newest_and_capacity() {
        let cache = LastCache::new(2);
        cache.observe(1, 10, 1.0);
        cache.observe(1, 20, 2.0);
        assert_eq!(
            cache.get(1),
            Some(LastValue {
                timestamp: 20,
                value: 2.0
            })
        );
        // Older samples never replace a newer one.
        cache.observe(1, 5, 9.0);
        assert_eq!(cache.get(1).unwrap().value, 2.0);
        // Equal timestamps use the last write.
        cache.observe(1, 20, 3.0);
        assert_eq!(cache.get(1).unwrap().value, 3.0);

        cache.observe(2, 10, 4.0);
        assert_eq!(cache.len(), 2);
        // Full: a new series is not admitted, existing ones still update.
        cache.observe(3, 10, 5.0);
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get(3), None);
        cache.observe(2, 11, 6.0);
        assert_eq!(cache.get(2).unwrap().value, 6.0);

        cache.clear();
        assert!(cache.is_empty());
    }
}
