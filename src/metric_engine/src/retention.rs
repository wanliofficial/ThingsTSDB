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
// KIND, either express or implied.

//! Per-metric retention (roadmap Phase 4).
//!
//! The global TTL (`StorageConfig::scheduler.ttl`) expires whole segments once
//! they age past a single bound, which is too coarse when one metric must be
//! kept longer than another. `RetentionPolicy` adds a per-metric read-time
//! ceiling: a query for a metric with a configured retention only sees samples
//! inside `[now - retention, ∞)`, so older samples become invisible even while
//! they still share a segment with a longer-lived metric.
//!
//! Enforcement is at read time (the query range is clamped). Physical
//! reclamation stays with the global segment TTL, which drops a whole segment
//! past a single bound; per-metric retention bounds *visibility*, not disk
//! usage, and keeps eviction O(1) per segment instead of rewriting a shared
//! segment on every query.

use std::{collections::HashMap, sync::Arc};

/// Per-metric retention windows, in milliseconds since the Unix epoch.
///
/// Cloning is cheap (it shares one map) and reads are lock-free, so the policy
/// can be held by every query without coordination. A metric absent from the
/// map has no per-metric retention and is governed only by the global TTL.
#[derive(Debug, Default, Clone)]
pub struct RetentionPolicy {
    per_metric: Arc<HashMap<Vec<u8>, i64>>,
}

impl RetentionPolicy {
    /// An empty policy: no per-metric retention.
    pub fn new() -> Self {
        Self::default()
    }

    /// Builds a policy from `metric name -> retention milliseconds` pairs.
    pub fn from_ms(entries: impl IntoIterator<Item = (Vec<u8>, i64)>) -> Self {
        Self {
            per_metric: Arc::new(entries.into_iter().collect()),
        }
    }

    /// Returns a policy that additionally retains `name` for `retention_ms`.
    ///
    /// A copy-on-write update: the shared map is cloned only when the metric is
    /// new or its retention changes, so an unrelated reader keeps the old map
    /// without a lock.
    pub fn with_metric(self, name: &[u8], retention_ms: i64) -> Self {
        if self.per_metric.get(name).copied() == Some(retention_ms) {
            return self;
        }
        let mut next = (*self.per_metric).clone();
        next.insert(name.to_vec(), retention_ms);
        Self {
            per_metric: Arc::new(next),
        }
    }

    /// The configured retention window of `name`, if any.
    pub fn retention_ms(&self, name: &[u8]) -> Option<i64> {
        self.per_metric.get(name).copied()
    }

    /// The number of metrics with an explicit retention window.
    pub fn len(&self) -> usize {
        self.per_metric.len()
    }

    /// Whether no metric has a retention window.
    pub fn is_empty(&self) -> bool {
        self.per_metric.is_empty()
    }

    /// Clamps `[start_ms, end_ms)` to the retention window of `name` at
    /// `now_ms`.
    ///
    /// A metric with retention `R` only exposes samples at or after
    /// `now_ms - R`, so the lower bound moves up to that floor. The upper bound
    /// is untouched. If the floor passes `end_ms` the range collapses (every
    /// sample has aged out) and the caller returns no rows. A non-positive
    /// retention or an unconfigured metric leaves the range unchanged.
    pub fn clamp(&self, name: &[u8], start_ms: i64, end_ms: i64, now_ms: i64) -> (i64, i64) {
        match self.retention_ms(name) {
            Some(retention) if retention > 0 => {
                let floor = now_ms.saturating_sub(retention);
                (start_ms.max(floor), end_ms)
            }
            _ => (start_ms, end_ms),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_policy_is_a_no_op() {
        let policy = RetentionPolicy::new();
        assert!(policy.is_empty());
        assert_eq!(policy.retention_ms(b"m"), None);
        assert_eq!(policy.clamp(b"m", 0, 100, 1_000), (0, 100));
    }

    #[test]
    fn clamp_raises_the_lower_bound_to_the_retention_floor() {
        let policy = RetentionPolicy::from_ms([(b"m".to_vec(), 1_000)]);
        // now=5000, retention=1000 -> floor=4000; a range starting at 0 is
        // pulled up to 4000, the end is untouched.
        assert_eq!(policy.clamp(b"m", 0, 6_000, 5_000), (4_000, 6_000));
        // A range already inside the window is unchanged.
        assert_eq!(policy.clamp(b"m", 4_500, 6_000, 5_000), (4_500, 6_000));
    }

    #[test]
    fn clamp_collapses_a_fully_expired_range() {
        let policy = RetentionPolicy::from_ms([(b"m".to_vec(), 1_000)]);
        // now=5000, retention=1000 -> floor=4000; an end of 3000 is below it,
        // so the range is empty and nothing is served.
        let (start, end) = policy.clamp(b"m", 0, 3_000, 5_000);
        assert!(start >= end, "expired range collapses: {start}..{end}");
    }

    #[test]
    fn unconfigured_metric_keeps_its_full_range() {
        let policy = RetentionPolicy::from_ms([(b"a".to_vec(), 1_000)]);
        assert_eq!(policy.clamp(b"b", 0, 100, 5_000), (0, 100));
    }

    #[test]
    fn non_positive_retention_is_ignored() {
        let policy = RetentionPolicy::from_ms([(b"m".to_vec(), 0)]);
        assert_eq!(policy.clamp(b"m", 0, 100, 5_000), (0, 100));
    }

    #[test]
    fn with_metric_is_copy_on_write_for_readers() {
        let base = RetentionPolicy::new();
        let updated = base.clone().with_metric(b"m", 2_000);
        // The base policy is unaffected by the derived one.
        assert!(base.is_empty());
        assert_eq!(updated.retention_ms(b"m"), Some(2_000));
        assert_eq!(updated.len(), 1);
        // Re-adding the same value does not grow the map.
        assert_eq!(updated.clone().with_metric(b"m", 2_000).len(), 1);
    }

    #[test]
    fn subtraction_does_not_underflow_on_clock_skew() {
        let policy = RetentionPolicy::from_ms([(b"m".to_vec(), 1_000)]);
        // A `now` below the retention window would underflow a naive
        // `now - retention`; saturating flooring keeps the lower bound at the
        // requested start instead of wrapping to a huge positive value.
        assert_eq!(policy.clamp(b"m", 0, 100, 500), (0, 100));
    }
}
