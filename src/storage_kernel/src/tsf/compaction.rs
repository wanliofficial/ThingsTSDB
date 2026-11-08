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

//! Pure compaction policy and LWW deduplication (ADR-0002 §6.1, §6.2).
//!
//! The file writer and the future outer-compaction job share these helpers:
//! point-wise LWW resolution, sequence-span union (compaction never narrows
//! sequence coverage) and the small/big-part merge trigger.

use super::Row;

/// Merge when a partition has more than this many small parts.
pub const SMALL_PART_COUNT_THRESHOLD: usize = 8;
/// Merge when small-part bytes exceed this fraction of the partition.
pub const SMALL_PART_BYTES_RATIO: f64 = 0.25;

/// Sorts `rows` by `(metric_id, series_id, timestamp, sequence)` and reduces
/// duplicate `(metric_id, series_id, timestamp)` keys to the row with the
/// highest `__seq__` (point-wise LWW, ADR-0002 §6.2).
///
/// `__reserved__` is left untouched: expiration/tombstone masking runs before
/// compaction drops a row.
pub fn dedup_lww(rows: &mut Vec<Row>) {
    rows.sort_unstable_by_key(|r| (r.metric_id, r.series_id, r.timestamp, r.sequence));
    let mut write = 0usize;
    for read in 0..rows.len() {
        if write > 0 {
            let previous = rows[write - 1];
            let current = rows[read];
            if previous.metric_id == current.metric_id
                && previous.series_id == current.series_id
                && previous.timestamp == current.timestamp
            {
                // Sorted by sequence, so the later row always wins.
                rows[write - 1] = current;
                continue;
            }
        }
        rows[write] = rows[read];
        write += 1;
    }
    rows.truncate(write);
}

/// Returns the union of the `(min_sequence, max_sequence)` spans of the source
/// parts, or `None` when there are no sources.
///
/// Outer compaction must never narrow sequence coverage, so the result is the
/// point-wise minimum and maximum of the inputs.
pub fn merge_sequence_span<I>(spans: I) -> Option<(u64, u64)>
where
    I: IntoIterator<Item = (u64, u64)>,
{
    spans
        .into_iter()
        .reduce(|(lo, hi), (lo2, hi2)| (lo.min(lo2), hi.max(hi2)))
}

/// True when `small_part_bytes` is over [`SMALL_PART_BYTES_RATIO`] of the
/// partition's `total_bytes`.
pub fn small_part_bytes_exceed_ratio(small_part_bytes: u64, total_bytes: u64) -> bool {
    if total_bytes == 0 {
        return false;
    }
    small_part_bytes as f64 > total_bytes as f64 * SMALL_PART_BYTES_RATIO
}

/// Applies the ADR-0002 §6.1 count and byte triggers: merge when a partition
/// has more than [`SMALL_PART_COUNT_THRESHOLD`] small parts, or when small-part
/// bytes exceed [`SMALL_PART_BYTES_RATIO`] of the partition. The
/// partition-close trigger is time-based and belongs to the caller (it needs a
/// clock).
pub fn should_compact(part_count: usize, small_part_bytes: u64, total_bytes: u64) -> bool {
    part_count > SMALL_PART_COUNT_THRESHOLD
        || small_part_bytes_exceed_ratio(small_part_bytes, total_bytes)
}

/// The ADR-0002 §6.1 byte trigger over a partition's part sizes.
///
/// A part only counts as the *big* part when it holds more than half the
/// partition; without one the partition is still a pile of flush-sized files,
/// and compacting it as soon as the bytes pass a quarter would rewrite
/// everything after every flush. Those partitions are left to the count trigger
/// ([`should_compact`]), so this returns `false` when no part dominates.
pub fn small_parts_exceed_ratio<I>(sizes: I) -> bool
where
    I: IntoIterator<Item = u64>,
{
    let (total, largest) = sizes
        .into_iter()
        .fold((0u64, 0u64), |(total, largest), size| {
            (total + size, largest.max(size))
        });
    if largest * 2 <= total {
        return false;
    }
    small_part_bytes_exceed_ratio(total - largest, total)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(series: u64, ts: i64, sequence: u64) -> Row {
        Row {
            metric_id: 1,
            series_id: series,
            timestamp: ts,
            value: sequence as f64,
            sequence,
            reserved: 0,
        }
    }

    #[test]
    fn test_dedup_lww_keeps_highest_sequence() {
        let mut rows = vec![
            row(2, 10, 1),
            row(1, 20, 5),
            row(1, 10, 2),
            row(1, 10, 9),
            row(1, 10, 4),
            row(2, 10, 3),
        ];
        dedup_lww(&mut rows);
        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows.iter()
                .map(|r| (r.series_id, r.timestamp, r.sequence))
                .collect::<Vec<_>>(),
            vec![(1, 10, 9), (1, 20, 5), (2, 10, 3)]
        );
    }

    #[test]
    fn test_merge_sequence_span_never_narrows() {
        assert_eq!(merge_sequence_span([]), None);
        assert_eq!(merge_sequence_span([(5, 10)]), Some((5, 10)));
        assert_eq!(
            merge_sequence_span([(5, 10), (1, 3), (7, 12)]),
            Some((1, 12))
        );
    }

    #[test]
    fn test_should_compact_thresholds() {
        assert!(!should_compact(8, 0, 100));
        assert!(should_compact(9, 0, 100));
        assert!(should_compact(2, 26, 100));
        assert!(!should_compact(2, 25, 100));
        assert!(!should_compact(0, 0, 0));
    }

    /// The byte trigger needs a dominant big part: a pile of equal-sized
    /// flushes is left to the count rule, otherwise every flush would rewrite
    /// the partition.
    #[test]
    fn test_small_parts_exceed_ratio_needs_a_big_part() {
        assert!(!small_parts_exceed_ratio([]));
        assert!(!small_parts_exceed_ratio([100]));
        // One part is exactly half the partition: not dominant.
        assert!(!small_parts_exceed_ratio([100, 100]));
        assert!(!small_parts_exceed_ratio([100, 100, 100, 100]));

        // A 100 B big part next to 20 B of flushes: 20 is under a quarter.
        assert!(!small_parts_exceed_ratio([100, 10, 10]));
        // 40 B of flushes is over a quarter of 140 B.
        assert!(small_parts_exceed_ratio([100, 20, 20]));
    }
}
