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

//! UTC-day partitioning and TTL drop planning (ADR-0002 §2.4, §8.1, §8.3).
//!
//! The format allows a configurable `partition_duration`; Phase 2 exercises
//! one UTC day and that is the only value these helpers accept implicitly.
//! They are pure functions so the manifest/TTL layer can unit-test its policy
//! without touching the filesystem.

/// One UTC day in milliseconds.
pub const DAY_MS: i64 = 86_400_000;

/// Returns the UTC-day index containing `timestamp_ms`.
///
/// The index is `timestamp_ms.div_euclid(DAY_MS)`, i.e. it is contiguous
/// across the Unix epoch and negative timestamps do not round towards zero.
pub fn partition_index(timestamp_ms: i64) -> i64 {
    timestamp_ms.div_euclid(DAY_MS)
}

/// Start of the UTC day containing `timestamp_ms`, in milliseconds.
pub fn partition_start(timestamp_ms: i64) -> i64 {
    partition_index(timestamp_ms) * DAY_MS
}

/// Exclusive end of the UTC day containing `timestamp_ms`, in milliseconds.
pub fn partition_end(timestamp_ms: i64) -> i64 {
    partition_start(timestamp_ms) + DAY_MS
}

/// `YYYYMMDD` label for the partition containing `timestamp_ms`.
///
/// This matches the `{root}/data/{YYYYMMDD}/` directory layout in ADR-0002
/// §8.1. It is implemented directly from the proleptic Gregorian calendar so
/// the kernel does not need a date-time dependency.
pub fn partition_label(timestamp_ms: i64) -> String {
    let (year, month, day) = civil_from_days(partition_index(timestamp_ms));
    format!("{year:04}{month:02}{day:02}")
}

/// Returns the partition indexes whose exclusive end is at or before
/// `now_ms - ttl_ms`, in ascending order.
///
/// ADR-0002 §8.3 defines `expire_time = now - ttl` and drops a partition when
/// `partition_end <= expire_time`. A partition whose end equals the expire
/// time is fully outside the retention window and is therefore dropped. The
/// caller removes the manifest entries first, then deletes the files.
pub fn plan_ttl_drop<I>(partitions: I, now_ms: i64, ttl_ms: i64) -> Vec<i64>
where
    I: IntoIterator<Item = i64>,
{
    let expire_time = now_ms.saturating_sub(ttl_ms);
    let mut expired: Vec<i64> = partitions
        .into_iter()
        .filter(|&idx| (idx + 1).saturating_mul(DAY_MS) <= expire_time)
        .collect();
    expired.sort_unstable();
    expired.dedup();
    expired
}

/// Days since 1970-01-01 to a proleptic Gregorian `(year, month, day)`.
///
/// Howard Hinnant's `civil_from_days` algorithm; valid for the full `i64`
/// range used by the format (well beyond any realistic timestamp).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_index_and_bounds() {
        assert_eq!(partition_index(0), 0);
        assert_eq!(partition_index(DAY_MS - 1), 0);
        assert_eq!(partition_index(DAY_MS), 1);
        assert_eq!(partition_index(-1), -1);
        assert_eq!(partition_start(1_700_000_000_000), 1_699_920_000_000);
        assert_eq!(
            partition_start(1_700_000_000_000),
            partition_end(1_700_000_000_000) - DAY_MS
        );
    }

    #[test]
    fn test_partition_label_matches_utc_calendar() {
        // 2023-11-14T22:13:20Z
        assert_eq!(partition_label(1_700_000_000_000), "20231114");
        assert_eq!(partition_label(0), "19700101");
        assert_eq!(partition_label(-1), "19691231");
        // Leap day.
        assert_eq!(partition_label(1_709_164_800_000), "20240229");
    }

    #[test]
    fn test_plan_ttl_drop_is_exclusive_and_deduplicated() {
        let now = 10 * DAY_MS;
        let ttl = 3 * DAY_MS;
        // expire_time = 7 days; partitions ending at or before day 7 go.
        let plan = plan_ttl_drop([9, 6, 5, 7, 6, 0], now, ttl);
        assert_eq!(plan, vec![0, 5, 6]);
        // A zero TTL drops every partition that ended at or before `now`.
        assert_eq!(plan_ttl_drop([9, 10], now, 0), vec![9]);
        // A TTL longer than the data drops nothing.
        assert!(plan_ttl_drop([0, 1], now, 100 * DAY_MS).is_empty());
    }

    #[test]
    fn test_partition_label_round_trips_day_boundaries() {
        for idx in [-10, -1, 0, 1, 19_000] {
            let ts = idx * DAY_MS;
            assert_eq!(partition_index(ts), idx);
            assert!(!partition_label(ts).is_empty());
        }
    }
}
