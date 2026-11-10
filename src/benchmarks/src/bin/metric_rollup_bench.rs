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

//! Dashboard latency through the write-time rollups (Phase 3 gate: "dashboard
//! queries p99 < 50 ms").
//!
//! Loads a day of ten-second samples per series, then times the same dashboard
//! query twice: once as `search_downsampled` (which reads the 1m/1h rollup
//! tables) and once as `search` + `downsample` (every raw sample). The ratio is
//! the read amplification the rollups remove.
//!
//! Usage: `cargo run --release -p benchmarks --bin metric_rollup_bench -- \
//! [series] [queries]`

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use columnar_storage::{config::StorageConfig, storage::StorageRuntimes, types::ObjectStoreRef};
use metric_engine::{
    downsample, Aggregation, Label, LabelMatcher, MatcherType, MetricEngine, Sample, SearchRequest,
};
use object_store::local::LocalFileSystem;

/// 2023-11-14T22:13:20Z, hour-aligned so every rollup bucket is aligned.
const BASE: i64 = 1_699_999_200_000;
const DAY_MS: i64 = 24 * 3_600_000;
/// One sample every ten seconds.
const PERIOD_MS: i64 = 10_000;

fn main() {
    let mut args = std::env::args().skip(1);
    let series: usize = args.next().and_then(|v| v.parse().ok()).unwrap_or(200);
    let queries: usize = args.next().and_then(|v| v.parse().ok()).unwrap_or(50);

    let root = std::env::temp_dir().join(format!(
        "metric-rollup-bench-{}-{}",
        std::process::id(),
        Instant::now().elapsed().as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create bench dir");

    let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());
    let runtimes = StorageRuntimes::new(Arc::clone(&rt), Arc::clone(&rt));
    let store: ObjectStoreRef = Arc::new(LocalFileSystem::new());
    let engine = rt
        .block_on(MetricEngine::try_open(
            &root.to_string_lossy(),
            Duration::from_hours(2),
            store,
            StorageConfig::default(),
            runtimes,
        ))
        .expect("open metric engine");

    let per_series = DAY_MS / PERIOD_MS;
    let samples = make_samples(series, per_series);
    let total = samples.len();
    let started = Instant::now();
    rt.block_on(engine.write(samples)).expect("write");
    let write_s = started.elapsed().as_secs_f64();
    let started = Instant::now();
    rt.block_on(engine.flush_all()).expect("flush");
    let flush_s = started.elapsed().as_secs_f64();
    println!(
        "rollup bench: series={series} samples={total} ({:.1} M) write={write_s:.2}s \
         flush={flush_s:.2}s",
        total as f64 / 1e6
    );
    let minute = engine.rollups().minute();
    let hour = engine.rollups().hour();
    println!(
        "  published coverage: 1m={} buckets worth, 1h={} buckets worth",
        (minute.coverage() - BASE) / 60_000,
        (hour.coverage() - BASE) / 3_600_000
    );

    // A dashboard panel: one job selector over the whole day, one point per
    // minute (a 6x reduction from the ten-second raw samples).
    let req = SearchRequest {
        metric_name: b"http_requests".to_vec(),
        matchers: vec![LabelMatcher {
            matcher_type: MatcherType::Eq,
            name: b"job".to_vec(),
            value: b"api".to_vec(),
        }],
        start_ms: BASE,
        end_ms: BASE + DAY_MS,
        limit: 0,
    };

    // The shapes a dashboard actually sends: a wide panel (every series of the
    // job, one point per minute) and the same panel limited to a few series, as
    // a Grafana panel with `maxSeries` does.
    let shapes: Vec<(&str, i64, Aggregation, usize)> = vec![
        ("1m avg wide", 60_000, Aggregation::Avg, 0),
        ("5m avg wide", 300_000, Aggregation::Avg, 0),
        ("1h max wide", 3_600_000, Aggregation::Max, 0),
        ("1m avg top5", 60_000, Aggregation::Avg, 5),
        ("1h max top5", 3_600_000, Aggregation::Max, 5),
    ];
    for (label, step, agg, limit) in shapes {
        let req = SearchRequest {
            limit,
            ..req.clone()
        };
        let (rollup, rollup_points) = measure(queries, || {
            let found = rt
                .block_on(engine.search_downsampled(&req, step, agg))
                .expect("rollup query");
            found.iter().map(|s| s.samples.len()).sum::<usize>()
        });
        let (raw, raw_points) = measure(queries, || {
            let mut found = rt.block_on(engine.search(&req)).expect("raw query");
            let mut points = 0;
            for series in &mut found {
                series.samples = downsample(&series.samples, step, agg);
                points += series.samples.len();
            }
            points
        });
        println!(
            "query {label:12} step={step:>7}ms limit={limit:<5} \
             rollup p50={:>6}us p99={:>6}us ({rollup_points} pts)  \
             raw p50={:>7}us p99={:>7}us ({raw_points} pts)  speedup={:.1}x",
            rollup.p50,
            rollup.p99,
            raw.p50,
            raw.p99,
            raw.p50 as f64 / rollup.p50.max(1) as f64
        );
        assert_eq!(
            rollup_points, raw_points,
            "{label}: the rollup path must return the same points as raw"
        );
    }

    let _ = std::fs::remove_dir_all(&root);
}

struct Latency {
    p50: u64,
    p99: u64,
}

/// Runs `f` `queries` times and reports the latency percentiles plus the point
/// count of the last answer (noise guard: the two paths must return the same
/// amount of data).
fn measure(queries: usize, mut f: impl FnMut() -> usize) -> (Latency, usize) {
    let mut latencies = Vec::with_capacity(queries);
    let mut points = 0;
    for _ in 0..queries {
        let started = Instant::now();
        points = f();
        latencies.push(started.elapsed().as_micros() as u64);
    }
    latencies.sort_unstable();
    let p = |q: f64| latencies[((latencies.len() as f64 * q) as usize).min(latencies.len() - 1)];
    (
        Latency {
            p50: p(0.50),
            p99: p(0.99),
        },
        points,
    )
}

fn make_samples(series: usize, per_series: i64) -> Vec<Sample> {
    let mut samples = Vec::with_capacity(series * per_series as usize);
    for s in 0..series {
        let job: &[u8] = if s % 10 == 0 { b"db" } else { b"api" };
        let code: &[u8] = if s % 7 == 0 { b"404" } else { b"200" };
        for i in 0..per_series {
            samples.push(Sample {
                name: b"http_requests".to_vec(),
                labels: vec![
                    Label {
                        name: b"job".to_vec(),
                        value: job.to_vec(),
                    },
                    Label {
                        name: b"instance".to_vec(),
                        value: format!("host-{s}").into_bytes(),
                    },
                    Label {
                        name: b"code".to_vec(),
                        value: code.to_vec(),
                    },
                ],
                timestamp: BASE + i * PERIOD_MS,
                value: (i as f64 * 0.01 + s as f64).sin() * 100.0 + 200.0,
                name_id: None,
                series_id: None,
            });
        }
    }
    samples
}
