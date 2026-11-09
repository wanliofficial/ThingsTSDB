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

//! UTC day and month arithmetic for indexDB partitioning (ADR-0001 §2.3, §4).
//!
//! Index rows are keyed by a UTC day number (`unix_seconds / 86400`) and one
//! indexDB exists per calendar month. Both are derived here without pulling in
//! a date-time crate, keeping the index kernel dependency-free.

/// Milliseconds in one UTC day.
pub const DAY_MS: i64 = 86_400_000;

/// The UTC day number of a millisecond timestamp, floored (negative-safe).
pub fn day_of_millis(ms: i64) -> u64 {
    ms.div_euclid(DAY_MS) as u64
}

/// The inclusive `[start, end)` days covered by `[start_ms, end_ms)`.
///
/// An empty or inverted range yields no days, so a query outside the data
/// range never touches the per-day index.
pub fn days_in_range(start_ms: i64, end_ms: i64) -> Vec<u64> {
    if end_ms <= start_ms {
        return Vec::new();
    }
    let first = day_of_millis(start_ms);
    // `end_ms` is exclusive, so the last day is the day of `end_ms - 1`.
    let last = day_of_millis(end_ms - 1);
    (first..=last).collect()
}

/// `(year, month)` of a UTC day number, month in `1..=12`.
pub fn civil_from_days(days: u64) -> (i32, u32, u32) {
    // Howard Hinnant's `civil_from_days`, shifted to the 1970-01-01 epoch.
    let z = days as i64 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    ((if m <= 2 { y + 1 } else { y }) as i32, m as u32, d as u32)
}

/// Days since the Unix epoch for a civil date.
pub fn days_from_civil(year: i32, month: u32, day: u32) -> u64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let m = month as i32;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + day as i32 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146_097 + doe - 719_468) as u64
}

/// The monthly partition index of a UTC day: months since 1970-01.
///
/// Fits the 16-bit partition field of an encoded series ID (ADR-0001 §2.3).
pub fn month_of_day(day: u64) -> u16 {
    let (year, month, _) = civil_from_days(day);
    let months = (year - 1970) * 12 + month as i32 - 1;
    debug_assert!((0..=u16::MAX as i32).contains(&months));
    months as u16
}

/// The monthly partition index of a millisecond timestamp.
pub fn month_of_millis(ms: i64) -> u16 {
    month_of_day(day_of_millis(ms))
}

/// The first day of a monthly partition.
pub fn month_first_day(month: u16) -> u64 {
    let months = month as i32;
    let year = 1970 + months / 12;
    let month_of_year = (months % 12 + 1) as u32;
    days_from_civil(year, month_of_year, 1)
}

/// The `[first_day, day_after_last_day)` range covered by a partition.
pub fn month_day_bounds(month: u16) -> (u64, u64) {
    let first = month_first_day(month);
    let last = month_first_day(month + 1);
    (first, last)
}

/// The partition label used for the on-disk directory, `YYYY_MM`.
pub fn month_label(month: u16) -> String {
    let first = month_first_day(month);
    let (year, month_of_year, _) = civil_from_days(first);
    format!("{year:04}_{month_of_year:02}")
}

/// Days of `month` that intersect `[start_ms, end_ms)`, clamped to the month.
pub fn month_days_in_range(month: u16, start_ms: i64, end_ms: i64) -> Vec<u64> {
    let (first_day, day_after) = month_day_bounds(month);
    let from = day_of_millis(start_ms).max(first_day);
    let to = day_of_millis(end_ms.saturating_sub(1).max(start_ms)).min(day_after - 1);
    if to < from {
        return Vec::new();
    }
    (from..=to).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_day_and_month_of_a_known_timestamp() {
        // 2023-11-14T22:13:20Z.
        let ms = 1_700_000_000_000i64;
        let day = day_of_millis(ms);
        assert_eq!(day, 19_675);
        assert_eq!(civil_from_days(day), (2023, 11, 14));
        assert_eq!(month_of_day(day), ((2023 - 1970) * 12 + 10) as u16);
        assert_eq!(month_label(month_of_day(day)), "2023_11");
        assert_eq!(
            month_first_day(month_of_day(day)),
            day_of_millis(1_698_796_800_000)
        );
    }

    #[test]
    fn test_civil_days_round_trip() {
        for day in [0u64, 1, 59, 60, 365, 19_675, 20_000, 30_000] {
            let (y, m, d) = civil_from_days(day);
            assert_eq!(days_from_civil(y, m, d), day, "day {day}");
        }
        assert_eq!(civil_from_days(0), (1970, 1, 1));
    }

    #[test]
    fn test_month_bounds_and_range_clamp() {
        let month = month_of_day(19_675);
        let (first, after) = month_day_bounds(month);
        assert_eq!(first, 19_662, "2023-11-01");
        assert_eq!(after - first, 30, "November has 30 days");

        // A range inside the month yields exactly its days.
        let start = first as i64 * DAY_MS + 1000;
        let end = (first as i64 + 2) * DAY_MS;
        assert_eq!(
            month_days_in_range(month, start, end),
            vec![first, first + 1]
        );
        // A whole-month range is clamped to the month.
        let all = month_days_in_range(month, 0, i64::MAX);
        assert_eq!(all.len(), 30);
        assert_eq!(all[0], first);
        // A range outside the month is empty.
        assert!(month_days_in_range(month, 0, first as i64 * DAY_MS).is_empty());
    }

    #[test]
    fn test_days_in_range_ignores_empty_ranges() {
        assert!(days_in_range(10, 10).is_empty());
        assert!(days_in_range(10, 5).is_empty());
        assert_eq!(days_in_range(0, DAY_MS), vec![0]);
        assert_eq!(days_in_range(0, DAY_MS + 1), vec![0, 1]);
    }
}
