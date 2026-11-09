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

//! A small growable set of sorted series IDs (ADR-0001 §3.5, §4).
//!
//! The indexDB intersects and unions posting sets. A `Vec<u64>` keeps them in
//! the same memory order as the merge cursor, keeps the `uint64set` fast path
//! for intersection, and stores the fixed-width 8-byte IDs the row format uses.

use std::ops::RangeInclusive;

/// A sorted, duplicate-free set of series IDs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SeriesIdSet {
    ids: Vec<u64>,
}

impl SeriesIdSet {
    pub fn new() -> Self {
        Self { ids: Vec::new() }
    }

    pub fn from_ids(ids: impl IntoIterator<Item = u64>) -> Self {
        let mut set = Self::new();
        for id in ids {
            set.insert(id);
        }
        set
    }

    /// Returns the number of IDs.
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    /// Returns the sorted IDs.
    pub fn ids(&self) -> &[u64] {
        &self.ids
    }

    /// Inserts one ID, keeping the set sorted and deduplicated.
    pub fn insert(&mut self, id: u64) -> bool {
        let pos = self.ids.partition_point(|x| *x < id);
        if self.ids.get(pos) == Some(&id) {
            return false;
        }
        self.ids.insert(pos, id);
        true
    }

    /// Inserts every ID from `other`.
    pub fn extend_from(&mut self, other: &SeriesIdSet) {
        for &id in other.ids() {
            self.insert(id);
        }
    }

    /// Removes `id` if present. Returns whether it was present.
    pub fn remove(&mut self, id: u64) -> bool {
        let pos = self.ids.partition_point(|x| *x < id);
        if self.ids.get(pos) == Some(&id) {
            self.ids.remove(pos);
            true
        } else {
            false
        }
    }

    /// Removes every ID in `other`.
    pub fn subtract(&mut self, other: &SeriesIdSet) {
        for &id in other.ids() {
            self.remove(id);
        }
    }

    /// Returns `self ∩ other`. Both inputs must be sorted and deduplicated.
    pub fn intersect(a: &SeriesIdSet, b: &SeriesIdSet) -> SeriesIdSet {
        let mut out = Vec::with_capacity(a.len().min(b.len()));
        let (mut i, mut j) = (0, 0);
        let (a, b) = (a.ids(), b.ids());
        while i < a.len() && j < b.len() {
            match a[i].cmp(&b[j]) {
                std::cmp::Ordering::Equal => {
                    out.push(a[i]);
                    i += 1;
                    j += 1;
                }
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
            }
        }
        SeriesIdSet { ids: out }
    }

    /// Returns `self ∪ other`.
    pub fn union(a: &SeriesIdSet, b: &SeriesIdSet) -> SeriesIdSet {
        let mut out = a.clone();
        out.extend_from(b);
        out
    }

    /// Returns `true` if `id` is a member.
    pub fn contains(&self, id: u64) -> bool {
        let pos = self.ids.partition_point(|x| *x < id);
        self.ids.get(pos) == Some(&id)
    }

    /// Returns the ID range as inclusive bounds (empty set -> `None`).
    pub fn range(&self) -> Option<RangeInclusive<u64>> {
        let first = *self.ids.first()?;
        let last = *self.ids.last()?;
        Some(first..=last)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_keeps_sorted_and_dedup() {
        let mut set = SeriesIdSet::new();
        assert!(set.insert(5));
        assert!(set.insert(1));
        assert!(!set.insert(5));
        assert_eq!(set.ids(), &[1, 5]);
    }

    #[test]
    fn test_extend_and_subtract() {
        let a = SeriesIdSet::from_ids([1, 3, 5, 7]);
        let b = SeriesIdSet::from_ids([3, 4, 5]);
        let mut u = a.clone();
        u.extend_from(&b);
        assert_eq!(u.ids(), &[1, 3, 4, 5, 7]);
        let mut s = a.clone();
        s.subtract(&b);
        assert_eq!(s.ids(), &[1, 7]);
    }

    #[test]
    fn test_intersect_and_union() {
        let a = SeriesIdSet::from_ids([1, 2, 3, 4]);
        let b = SeriesIdSet::from_ids([3, 4, 5, 6]);
        assert_eq!(SeriesIdSet::intersect(&a, &b).ids(), &[3, 4]);
        assert_eq!(SeriesIdSet::union(&a, &b).ids(), &[1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_range_and_contains() {
        let set = SeriesIdSet::from_ids([10, 20, 30]);
        assert_eq!(set.range(), Some(10..=30));
        assert!(set.contains(20));
        assert!(!set.contains(25));
        assert!(SeriesIdSet::new().range().is_none());
    }
}
