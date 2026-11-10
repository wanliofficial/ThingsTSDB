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

//! Lightweight request observability for the query endpoints.
//!
//! [`ServerMetrics`] is an atomics-only counter/histogram set shared by every
//! handler through `AppState`. Each query records its wall-clock latency into a
//! fixed log-linear bucket histogram (so p50/p99 can be read back without a
//! per-request allocation), the number of series and samples it returned, and
//! — when it exceeds the configured slow threshold — a structured `warn!` line
//! (the slow-query log, roadmap Phase 4).
//!
//! The histogram mirrors Prometheus' cumulative-bucket model: observing a value
//! increments every bucket whose upper bound is `>=` the value, plus `count`
//! and `sum`. A quantile is the first bucket whose cumulative count reaches the
//! desired rank.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

/// Upper bounds of the latency buckets, in microseconds, ascending. The last
/// bucket is `+Inf` (everything above 10 s lands there). Chosen so a dashboard
/// query (target p99 < 50 ms) and an ad-hoc query (seconds) both land in a
/// bucket with a few neighbours instead of one huge span.
const BUCKET_UPPER_US: [u64; 12] = [
    1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 2_500_000,
    10_000_000,
];

/// Number of finite buckets plus the `+Inf` bucket.
const BUCKET_COUNT: usize = BUCKET_UPPER_US.len() + 1;

/// Default slow-query threshold: a query slower than this is logged.
pub const DEFAULT_SLOW_THRESHOLD: Duration = Duration::from_millis(50);

/// Shared, lock-free query metrics. The only state is atomics, so the handle is
/// cheap to clone (it shares the same atomics) and can be copied into every
/// handler without a lock.
#[derive(Debug, Clone)]
pub struct ServerMetrics {
    inner: Arc<MetricsInner>,
}

/// The atomics behind [`ServerMetrics`].
#[derive(Debug)]
struct MetricsInner {
    slow_threshold_us: u64,
    queries: [AtomicU64; QueryKind::COUNT],
    /// Sum of observed latencies, by kind (microseconds).
    latency_sum_us: [AtomicU64; QueryKind::COUNT],
    /// Cumulative histogram bucket counts, by kind. Index `BUCKET_COUNT - 1` is
    /// `+Inf` (equals the query count for that kind).
    buckets: [[AtomicU64; BUCKET_COUNT]; QueryKind::COUNT],
    /// Total series returned across queries, by kind.
    series_returned: [AtomicU64; QueryKind::COUNT],
    /// Total samples (data points) returned across queries, by kind.
    samples_returned: [AtomicU64; QueryKind::COUNT],
    /// Queries rejected with a 4xx (bad request), by kind.
    rejected: [AtomicU64; QueryKind::COUNT],
    /// Queries that failed with a 5xx, by kind.
    failed: [AtomicU64; QueryKind::COUNT],
}

/// The instrumented query kinds. `COUNT` is the array length.
pub struct QueryKind;

impl QueryKind {
    const ALL: [&'static str; Self::COUNT] = ["range", "last", "range_step"];
    pub const COUNT: usize = 3;
    pub const LAST: usize = 1;
    pub const RANGE: usize = 0;
    pub const RANGE_STEP: usize = 2;

    /// The stable name used in logs and the stats endpoint.
    pub fn name(kind: usize) -> &'static str {
        Self::ALL[kind]
    }
}

impl Default for ServerMetrics {
    fn default() -> Self {
        Self::new(DEFAULT_SLOW_THRESHOLD)
    }
}

impl ServerMetrics {
    pub fn new(slow_threshold: Duration) -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                slow_threshold_us: slow_threshold.as_micros() as u64,
                queries: std::array::from_fn(|_| AtomicU64::new(0)),
                latency_sum_us: std::array::from_fn(|_| AtomicU64::new(0)),
                buckets: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
                series_returned: std::array::from_fn(|_| AtomicU64::new(0)),
                samples_returned: std::array::from_fn(|_| AtomicU64::new(0)),
                rejected: std::array::from_fn(|_| AtomicU64::new(0)),
                failed: std::array::from_fn(|_| AtomicU64::new(0)),
            }),
        }
    }

    /// Records one successful query of `kind` that took `latency` and returned
    /// `series` series carrying `samples` data points. Logs a slow-query line
    /// when the latency exceeds the threshold.
    pub fn observe(&self, kind: usize, latency: Duration, series: u64, samples: u64, detail: &str) {
        let us = latency.as_micros() as u64;
        self.inner.queries[kind].fetch_add(1, Ordering::Relaxed);
        self.inner.latency_sum_us[kind].fetch_add(us, Ordering::Relaxed);
        self.inner.series_returned[kind].fetch_add(series, Ordering::Relaxed);
        self.inner.samples_returned[kind].fetch_add(samples, Ordering::Relaxed);
        for (i, upper) in BUCKET_UPPER_US.iter().enumerate() {
            if us <= *upper {
                self.inner.buckets[kind][i].fetch_add(1, Ordering::Relaxed);
            }
        }
        // Every observation lands in the +Inf bucket.
        self.inner.buckets[kind][BUCKET_COUNT - 1].fetch_add(1, Ordering::Relaxed);

        if us >= self.inner.slow_threshold_us {
            tracing::warn!(
                kind = QueryKind::name(kind),
                latency_us = us,
                series,
                samples,
                slow_threshold_us = self.inner.slow_threshold_us,
                "{detail}"
            );
        }
    }

    /// Records a client-side rejection (bad request) for `kind`.
    pub fn record_rejected(&self, kind: usize) {
        self.inner.rejected[kind].fetch_add(1, Ordering::Relaxed);
    }

    /// Records a server-side failure for `kind`.
    pub fn record_failed(&self, kind: usize) {
        self.inner.failed[kind].fetch_add(1, Ordering::Relaxed);
    }

    /// The observed p50 and p99 latency of `kind`, and the sample count behind
    /// them. Returns `None` for a quantile when no query of that kind has run.
    pub fn latency_quantiles(&self, kind: usize) -> Option<LatencyQuantiles> {
        let count = self.inner.queries[kind].load(Ordering::Relaxed);
        if count == 0 {
            return None;
        }
        let sum_us = self.inner.latency_sum_us[kind].load(Ordering::Relaxed);
        Some(LatencyQuantiles {
            count,
            avg_us: sum_us / count,
            p50_us: self.quantile(kind, 0.50, count),
            p99_us: self.quantile(kind, 0.99, count),
            max_us: self.max_us(kind),
        })
    }

    /// First bucket upper bound whose cumulative count reaches `rank`.
    ///
    /// The buckets are stored cumulative (each holds the count of observations
    /// `<=` its upper bound, mirroring Prometheus), so no re-summing is needed.
    fn quantile(&self, kind: usize, q: f64, count: u64) -> u64 {
        let rank = ((q * count as f64).ceil() as u64).max(1);
        for (i, upper) in BUCKET_UPPER_US.iter().enumerate() {
            if self.inner.buckets[kind][i].load(Ordering::Relaxed) >= rank {
                return *upper;
            }
        }
        // Above every finite bucket: report the +Inf sentinel.
        u64::MAX
    }

    /// The tightest finite bucket bound that contains the slowest observation.
    ///
    /// With cumulative buckets, the slowest observation is the smallest upper
    /// bound whose count has reached the total, so scan ascending for the first
    /// bucket that holds every observation.
    fn max_us(&self, kind: usize) -> u64 {
        let total = self.inner.buckets[kind][BUCKET_COUNT - 1].load(Ordering::Relaxed);
        for (i, upper) in BUCKET_UPPER_US.iter().enumerate() {
            if self.inner.buckets[kind][i].load(Ordering::Relaxed) >= total {
                return *upper;
            }
        }
        0
    }

    /// Total queries of `kind` (successful + rejected + failed).
    pub fn total(&self, kind: usize) -> u64 {
        self.inner.queries[kind].load(Ordering::Relaxed)
            + self.inner.rejected[kind].load(Ordering::Relaxed)
            + self.inner.failed[kind].load(Ordering::Relaxed)
    }

    /// Renders the current metrics as a Prometheus text exposition.
    pub fn render_prometheus(&self) -> String {
        let mut out = String::new();
        out.push_str("# HELP horae_query_total Total queries by endpoint.\n");
        out.push_str("# TYPE horae_query_total counter\n");
        for kind in 0..QueryKind::COUNT {
            out.push_str(&format!(
                "horae_query_total{{kind=\"{}\"}} {}\n",
                QueryKind::name(kind),
                self.total(kind)
            ));
        }
        out.push_str("# HELP horae_query_latency_seconds Query latency histogram.\n");
        out.push_str("# TYPE horae_query_latency_seconds histogram\n");
        for kind in 0..QueryKind::COUNT {
            let name = QueryKind::name(kind);
            // Buckets are stored cumulative (count of observations `<=` each
            // bound), exactly what Prometheus' `_bucket` series expects.
            for (i, upper) in BUCKET_UPPER_US.iter().enumerate() {
                out.push_str(&format!(
                    "horae_query_latency_seconds_bucket{{kind=\"{}\",le=\"{}\"}} {}\n",
                    name,
                    format_bucket_bound(*upper),
                    self.inner.buckets[kind][i].load(Ordering::Relaxed)
                ));
            }
            let inf = self.inner.buckets[kind][BUCKET_COUNT - 1].load(Ordering::Relaxed);
            out.push_str(&format!(
                "horae_query_latency_seconds_bucket{{kind=\"{}\",le=\"+Inf\"}} {}\n",
                name, inf
            ));
            out.push_str(&format!(
                "horae_query_latency_seconds_sum{{kind=\"{}\"}} {}\n",
                name,
                self.inner.latency_sum_us[kind].load(Ordering::Relaxed) as f64 / 1_000_000.0
            ));
            out.push_str(&format!(
                "horae_query_latency_seconds_count{{kind=\"{}\"}} {}\n",
                name,
                self.inner.queries[kind].load(Ordering::Relaxed)
            ));
        }
        out.push_str("# HELP horae_query_series_total Series returned by queries.\n");
        out.push_str("# TYPE horae_query_series_total counter\n");
        for kind in 0..QueryKind::COUNT {
            out.push_str(&format!(
                "horae_query_series_total{{kind=\"{}\"}} {}\n",
                QueryKind::name(kind),
                self.inner.series_returned[kind].load(Ordering::Relaxed)
            ));
        }
        out.push_str("# HELP horae_query_samples_total Samples returned by queries.\n");
        out.push_str("# TYPE horae_query_samples_total counter\n");
        for kind in 0..QueryKind::COUNT {
            out.push_str(&format!(
                "horae_query_samples_total{{kind=\"{}\"}} {}\n",
                QueryKind::name(kind),
                self.inner.samples_returned[kind].load(Ordering::Relaxed)
            ));
        }
        out.push_str("# HELP horae_query_rejected_total Requests rejected (4xx).\n");
        out.push_str("# TYPE horae_query_rejected_total counter\n");
        for kind in 0..QueryKind::COUNT {
            out.push_str(&format!(
                "horae_query_rejected_total{{kind=\"{}\"}} {}\n",
                QueryKind::name(kind),
                self.inner.rejected[kind].load(Ordering::Relaxed)
            ));
        }
        out.push_str("# HELP horae_query_failed_total Requests that failed (5xx).\n");
        out.push_str("# TYPE horae_query_failed_total counter\n");
        for kind in 0..QueryKind::COUNT {
            out.push_str(&format!(
                "horae_query_failed_total{{kind=\"{}\"}} {}\n",
                QueryKind::name(kind),
                self.inner.failed[kind].load(Ordering::Relaxed)
            ));
        }
        out
    }
}

/// Latency summary of one query kind.
#[derive(Debug, Clone, Copy)]
pub struct LatencyQuantiles {
    pub count: u64,
    pub avg_us: u64,
    pub p50_us: u64,
    pub p99_us: u64,
    /// Upper bound of the highest non-empty finite bucket, or 0.
    pub max_us: u64,
}

/// Formats a bucket bound in seconds, trimming trailing zeros.
fn format_bucket_bound(us: u64) -> String {
    let seconds = us as f64 / 1_000_000.0;
    let text = format!("{seconds:.6}");
    let text = text.trim_end_matches('0').trim_end_matches('.');
    text.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quantiles_land_in_expected_buckets() {
        let metrics = ServerMetrics::new(Duration::from_millis(50));
        // 100 queries: 90 fast (< 5 ms), 9 slow (60 ms), 1 very slow (2 s).
        for _ in 0..90 {
            metrics.observe(QueryKind::RANGE, Duration::from_millis(1), 1, 1, "fast");
        }
        for _ in 0..9 {
            metrics.observe(QueryKind::RANGE, Duration::from_millis(60), 1, 1, "slow");
        }
        metrics.observe(QueryKind::RANGE, Duration::from_secs(2), 1, 1, "very slow");

        let q = metrics.latency_quantiles(QueryKind::RANGE).unwrap();
        assert_eq!(q.count, 100);
        // p50 is inside the <=5 ms bucket.
        assert!(q.p50_us <= 5_000);
        // p99 is the 99th observation; the 91st..99th are 60 ms, so p99 lands
        // in the <=100 ms bucket.
        assert_eq!(q.p99_us, 100_000);
        assert_eq!(q.avg_us, (90 * 1_000 + 9 * 60_000 + 2_000_000) / 100);

        // The 2 s observation is above every finite bucket except the last few;
        // max_us reports the highest finite bucket that is non-empty (<=2.5 s).
        assert_eq!(q.max_us, 2_500_000);
    }

    #[test]
    fn test_empty_kind_has_no_quantiles() {
        let metrics = ServerMetrics::default();
        assert!(metrics.latency_quantiles(QueryKind::LAST).is_none());
        assert_eq!(metrics.total(QueryKind::LAST), 0);
    }

    #[test]
    fn test_rejected_and_failed_are_counted_separately() {
        let metrics = ServerMetrics::default();
        metrics.observe(QueryKind::RANGE, Duration::from_millis(1), 0, 0, "ok");
        metrics.record_rejected(QueryKind::RANGE);
        metrics.record_failed(QueryKind::RANGE);
        // Only the successful one counts as a query.
        assert_eq!(metrics.total(QueryKind::RANGE), 3);
        let text = metrics.render_prometheus();
        assert!(text.contains("horae_query_rejected_total{kind=\"range\"} 1"));
        assert!(text.contains("horae_query_failed_total{kind=\"range\"} 1"));
    }

    #[test]
    fn test_prometheus_histogram_is_cumulative() {
        let metrics = ServerMetrics::default();
        metrics.observe(QueryKind::RANGE, Duration::from_millis(3), 0, 0, "");
        metrics.observe(QueryKind::RANGE, Duration::from_millis(40), 0, 0, "");
        let text = metrics.render_prometheus();
        // Both observations are <= 50 ms, so the cumulative count at le=0.05 is
        // 2.
        assert!(text.contains("le=\"0.05\"} 2"));
        // Only the 40 ms one is <= 0.025? No: 3 ms <= 25 ms and 40 ms > 25 ms,
        // so le=0.025 is 1.
        assert!(text.contains("le=\"0.025\"} 1"));
    }

    #[test]
    fn test_slow_query_logs_over_threshold() {
        // Uses a 0 threshold so every observation is "slow"; the observe call
        // must not panic and must still count.
        let metrics = ServerMetrics::new(Duration::from_micros(0));
        metrics.observe(QueryKind::LAST, Duration::from_micros(1), 5, 10, "instant");
        assert_eq!(metrics.total(QueryKind::LAST), 1);
    }
}
