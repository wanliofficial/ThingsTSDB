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

/// Bitset stored once a sorted set grows to this many IDs (8 MiB).
pub const BITMAP_MIN_IDS: usize = 1 << 20;

/// A sorted, duplicate-free set of series IDs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SeriesIdSet {
    ids: Vec<u64>,
}

impl SeriesIdSet {
    pub fn new() -> Self {
        Self { ids: Vec::new() }
    }

    /// Builds a set from an arbitrary iterator.
    ///
    /// Bulk creation sorts and deduplicates once instead of doing a sorted
    /// insert per ID, which would be quadratic on a 10M-ID posting list.
    ///
    /// Posting rows are written sorted (the collapse pass preserves order and a
    /// single value's row is a sorted run), so the decoded IDs of one day or
    /// group posting usually arrive already sorted. A single linear scan skips
    /// the `O(n log n)` sort in that case, which is most of the cost of a cold
    /// query that materializes a multi-million-ID set.
    pub fn from_ids(ids: impl IntoIterator<Item = u64>) -> Self {
        let mut ids: Vec<u64> = ids.into_iter().collect();
        // Cheap sortedness probe: bail at the first inversion. A sorted, unique
        // vector only needs the dedup pass.
        let mut sorted = true;
        for pair in ids.windows(2) {
            if pair[0] > pair[1] {
                sorted = false;
                break;
            }
        }
        if sorted {
            ids.dedup();
            return Self { ids };
        }
        ids.sort_unstable();
        ids.dedup();
        Self { ids }
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
    ///
    /// Merges the two sorted vectors in one pass; the previous per-ID sorted
    /// insert was quadratic on large posting lists.
    pub fn extend_from(&mut self, other: &SeriesIdSet) {
        if other.is_empty() {
            return;
        }
        if self.is_empty() {
            self.ids = other.ids.clone();
            return;
        }
        let mut out = Vec::with_capacity(self.ids.len() + other.ids.len());
        let (mut i, mut j) = (0, 0);
        while i < self.ids.len() && j < other.ids.len() {
            match self.ids[i].cmp(&other.ids[j]) {
                std::cmp::Ordering::Less => {
                    out.push(self.ids[i]);
                    i += 1;
                }
                std::cmp::Ordering::Greater => {
                    out.push(other.ids[j]);
                    j += 1;
                }
                std::cmp::Ordering::Equal => {
                    out.push(self.ids[i]);
                    i += 1;
                    j += 1;
                }
            }
        }
        out.extend_from_slice(&self.ids[i..]);
        out.extend_from_slice(&other.ids[j..]);
        self.ids = out;
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

    /// Removes every ID in `other` in one merge pass.
    pub fn subtract(&mut self, other: &SeriesIdSet) {
        if other.is_empty() || self.is_empty() {
            return;
        }
        let mut out = Vec::with_capacity(self.ids.len());
        let (mut i, mut j) = (0, 0);
        while i < self.ids.len() && j < other.ids.len() {
            match self.ids[i].cmp(&other.ids[j]) {
                std::cmp::Ordering::Less => {
                    out.push(self.ids[i]);
                    i += 1;
                }
                std::cmp::Ordering::Greater => j += 1,
                std::cmp::Ordering::Equal => {
                    i += 1;
                    j += 1;
                }
            }
        }
        out.extend_from_slice(&self.ids[i..]);
        self.ids = out;
    }

    /// Returns `self ∩ other`. Both inputs must be sorted and deduplicated.
    ///
    /// The merge walk costs `O(|a| + |b|)`, but a point filter against a large
    /// day set only has a handful of IDs. When one side is at least 16x
    /// smaller, probe it against the other with a binary search per ID
    /// (`O(|small| log |big|)`), which is what makes an exact-tag query cheap
    /// even when the per-day set holds millions of series. Two large, dense
    /// sets go through the bitset path instead (see
    /// [`SeriesIdSet::intersect_bitmap`]).
    pub fn intersect(a: &SeriesIdSet, b: &SeriesIdSet) -> SeriesIdSet {
        let (small, big) = if a.len() <= b.len() { (a, b) } else { (b, a) };
        if small.len().saturating_mul(16) < big.len() {
            let ids = small
                .ids()
                .iter()
                .copied()
                .filter(|id| big.contains(*id))
                .collect();
            return SeriesIdSet { ids };
        }
        if small.len() >= BITMAP_MIN_IDS {
            if let Some(hit) = Self::intersect_bitmap(a, b) {
                return hit;
            }
        }
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

    /// Estimated heap use, used to decide whether a result is worth caching.
    pub fn heap_bytes(&self) -> usize {
        self.ids.capacity() * std::mem::size_of::<u64>()
    }

    /// Returns `self ∩ other` through a 64-bit bitset over `other`'s ID range.
    ///
    /// A point or narrow filter against a multi-million-ID day set hits the
    /// binary-search path, but a *wide* filter (`job=api` matching 80% of the
    /// metric) intersects two sets of similar size, and the merge walk then
    /// reads 2 x 8 B per ID. Clearing a bitset costs `range/64` words and the
    /// membership test is one load, so the same intersection runs 10-20x
    /// faster when the smaller side still covers a large ID range.
    ///
    /// Returns `None` when the set is too sparse for a bitset to pay off (the
    /// `width/64` clear-and-scan steps would exceed a merge walk over both
    /// sides) or too wide to allocate.
    pub fn intersect_bitmap(a: &SeriesIdSet, b: &SeriesIdSet) -> Option<SeriesIdSet> {
        // The bitset is built from `b` and every ID of `a` is probed.
        let (small, big) = (a, b);
        if small.is_empty() || big.is_empty() {
            return Some(SeriesIdSet::new());
        }
        let (low, high) = (big.ids[0], big.ids[big.len() - 1]);
        // Width in bits; a set covering most of the u64 space maps to
        // `u64::MAX` rather than overflowing.
        let span = high.wrapping_sub(low).wrapping_add(1);
        // Density gate on the denser side (the one filling the bitset).
        let words = span.div_ceil(64);
        // Sparse: the bitset/scan (`width/64` steps) would exceed a merge walk
        // over both sides (`|a| + |b|`), or the width is unrepresentable.
        if words > a.len() as u64 + b.len() as u64 {
            return None;
        }
        // The bitset itself is the cost driver: cap it at 64 MiB (512M bits).
        if span > (1u64 << 29) * 64 {
            return None;
        }
        let mut bitset = Bitmap::new(span);
        for &id in big.ids() {
            bitset.set(id.wrapping_sub(low));
        }
        debug_assert!(bitset.contains(high.wrapping_sub(low)));
        let mut out = Vec::with_capacity(small.len().min(big.len()));
        for &id in small.ids() {
            if bitset.contains(id.wrapping_sub(low)) {
                out.push(id);
            }
        }
        // Return the buffer to the pool instead of dropping 1+ MiB per query.
        bitset.recycle();
        Some(SeriesIdSet { ids: out })
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
    fn test_intersect_probes_the_much_smaller_side() {
        let small = SeriesIdSet::from_ids([3, 7, 1_000]);
        let big = SeriesIdSet::from_ids(0..10_000u64);
        assert_eq!(SeriesIdSet::intersect(&small, &big).ids(), &[3, 7, 1_000]);
        // The probe is symmetric.
        assert_eq!(SeriesIdSet::intersect(&big, &small).ids(), &[3, 7, 1_000]);
        let missing = SeriesIdSet::from_ids([10_000, 20_000]);
        assert!(SeriesIdSet::intersect(&big, &missing).is_empty());
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

/// A zero-initialised bitset over an ID span, borrowed from a per-thread pool
/// when a buffer of the right size class is idle.
///
/// Building a 10M-bit set per query allocates 1.25 MiB and zero-fills it, which
/// is a large share of the cost of a cold wide intersection. Pooled buffers of
/// an exact size class are already zero when handed out, because
/// [`Bitmap::recycle`] clears them.
pub struct Bitmap {
    words: Vec<u64>,
    /// Words of `words` that hold this bitmap's bits.
    len: usize,
}

/// Buffer classes for the pool, as log2 of the word count (512 B .. 128 MiB).
const BITMAP_POOL_SHIFTS: [usize; 19] = [
    6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
];

thread_local! {
    /// Class -> idle zeroed bitset buffer, at most one per class.
    static BITMAP_POOL: std::cell::RefCell<Vec<(usize, Vec<u64>)>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

impl Bitmap {
    /// Allocates a zeroed bitset of `span` bits (word-aligned up).
    pub fn new(span: u64) -> Self {
        // Power of two so every span up to 16 GiB bits lands in a pool class.
        let words = span.div_ceil(64).max(1).next_power_of_two() as usize;
        let buf = Self::take_pooled(words).unwrap_or_else(|| vec![0u64; words]);
        Self {
            words: buf,
            len: words,
        }
    }

    fn take_pooled(words: usize) -> Option<Vec<u64>> {
        if !words.is_power_of_two() || words < (1 << BITMAP_POOL_SHIFTS[0]) {
            return None;
        }
        let shift = words.trailing_zeros() as usize;
        BITMAP_POOL.with(|pool| {
            let mut pool = pool.borrow_mut();
            let pos = pool.iter().position(|(cls, _)| *cls == shift)?;
            Some(pool.swap_remove(pos).1)
        })
    }

    /// Returns the buffer to the thread pool, cleared for the next query.
    pub fn recycle(mut self) {
        let words = self.words.len();
        if !words.is_power_of_two() || words < (1 << BITMAP_POOL_SHIFTS[0]) {
            return;
        }
        let shift = words.trailing_zeros() as usize;
        if shift > *BITMAP_POOL_SHIFTS.last().expect("non-empty") {
            return;
        }
        self.words.fill(0);
        BITMAP_POOL.with(|pool| {
            let mut pool = pool.borrow_mut();
            if pool.iter().all(|(cls, _)| *cls != shift) {
                pool.push((shift, std::mem::take(&mut self.words)));
            }
        });
        // Keep the most recently used buffer instead of growing the pool.

        let _ = &self.len;
    }

    pub fn set(&mut self, bit: u64) {
        self.words[(bit / 64) as usize] |= 1u64 << (bit % 64);
    }

    pub fn contains(&self, bit: u64) -> bool {
        let word = (bit / 64) as usize;
        word < self.len && self.words[word] & (1u64 << (bit % 64)) != 0
    }
}

#[cfg(test)]
mod bitset_tests {
    use super::*;

    fn dense(n: u64) -> SeriesIdSet {
        SeriesIdSet::from_ids((0..n).map(|i| i * 3))
    }

    #[test]
    fn test_intersect_bitmap_matches_merge() {
        let a = dense(100_000);
        let b = SeriesIdSet::from_ids((0..100_000u64).filter(|i| i % 7 == 0).map(|i| i * 3));
        let via_bits = SeriesIdSet::intersect_bitmap(&a, &b).expect("dense enough for a bitset");
        assert_eq!(via_bits.len(), 14_286);
        assert_eq!(via_bits, SeriesIdSet::intersect(&a, &b));

        // Wide filters intersect through the bitset path once large enough.
        let wide = SeriesIdSet::from_ids((0..1_200_000u64).map(|i| i * 5));
        let day = SeriesIdSet::from_ids((0..1_500_000u64).map(|i| i * 4));
        let expected = SeriesIdSet::intersect(&wide, &day);
        let bits = SeriesIdSet::intersect_bitmap(&wide, &day).unwrap();
        assert_eq!(bits, expected);

        // Empty inputs.
        assert!(SeriesIdSet::intersect_bitmap(&a, &SeriesIdSet::new())
            .unwrap()
            .is_empty());
    }

    #[test]
    fn test_intersect_bitmap_rejects_sparse_ranges() {
        // Two IDs spread over a huge range: a bitset would be pure waste.
        let sparse = SeriesIdSet::from_ids([1u64, u64::MAX]);
        let far = SeriesIdSet::from_ids([1 << 40, u64::MAX]);
        assert!(SeriesIdSet::intersect_bitmap(&sparse, &far).is_none());
        let other = SeriesIdSet::from_ids([1u64, u64::MAX - 1]);
        assert!(SeriesIdSet::intersect_bitmap(&sparse, &other).is_none());
        // A wide-but-sparse set rejects its own width (2 IDs over 1<<40).
        let wide = SeriesIdSet::from_ids([0u64, 1 << 40]);
        assert!(SeriesIdSet::intersect_bitmap(&wide, &wide).is_none());
        // A dense set against a sparse one still uses the small side, so the
        // answer stays correct whichever side fills the bitset.
        let dense = SeriesIdSet::from_ids(0..2000u64);
        let nearby = SeriesIdSet::from_ids([2u64, 5]);
        let sparse_hit = SeriesIdSet::intersect_bitmap(&dense, &nearby).unwrap();
        assert_eq!(sparse_hit, SeriesIdSet::intersect(&dense, &nearby));
    }

    #[test]
    fn test_bitmap_set_and_contains() {
        let mut b = Bitmap::new(200);
        b.set(0);
        b.set(63);
        b.set(64);
        b.set(199);
        assert!(b.contains(0) && b.contains(63) && b.contains(64) && b.contains(199));
        assert!(!b.contains(1));
        assert!(!b.contains(200));
    }

    #[test]
    fn test_bitmap_pool_reuses_classified_buffers() {
        let span = 100_000u64;
        let mut first = Bitmap::new(span);
        let ptr = first.words.as_ptr();
        let words = first.words.len();
        first.set(99_999);
        first.recycle();
        let second = Bitmap::new(span);
        assert_eq!(second.words.as_ptr(), ptr, "pooled buffer is reused");
        assert_eq!(second.words.len(), words);
        assert!(!second.contains(99_999), "handed out cleared");
    }
}
