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

//! End-to-end query latency through `MetricEngine::search` (Phase 3 gate).
//!
//! Writes one sample per series, then measures the deployed `DualRead` query
//! path: index resolution, label reconstruction and the data scan. The ADR
//! target for dashboard queries is a p99 under 50 ms.
//!
//! Usage: `cargo run --release -p benchmarks --bin metric_query_bench -- \
//! [series] [queries]`

use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use columnar_storage::{config::StorageConfig, storage::StorageRuntimes, types::ObjectStoreRef};
use metric_engine::{
    IndexMode, Label, LabelMatcher, MatcherType, MetricEngine, Sample, SearchRequest,
};
use object_store::local::LocalFileSystem;

fn main() {
    let mut args = std::env::args().skip(1);
    let series: usize = args.next().and_then(|v| v.parse().ok()).unwrap_or(200_000);
    let queries: usize = args.next().and_then(|v| v.parse().ok()).unwrap_or(200);
    let per_shape = (queries / 6).max(1);

    let root = std::env::temp_dir().join(format!(
        "metric-query-bench-{}-{}",
        std::process::id(),
        Instant::now().elapsed().as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create bench dir");

    let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());
    let runtimes = StorageRuntimes::new(Arc::clone(&rt), Arc::clone(&rt));
    let store: ObjectStoreRef = Arc::new(LocalFileSystem::new());
    let engine = rt
        .block_on(MetricEngine::try_open_with_index_mode(
            &root.to_string_lossy(),
            Duration::from_hours(2),
            store,
            StorageConfig::default(),
            runtimes,
            IndexMode::DualRead,
        ))
        .expect("open metric engine");

    // Samples are second-aligned: `window = ts / 1000 * 1000` is evaluated in
    // integer milliseconds, so a non-aligned `ts` would truncate to the start
    // of its second but the window only covers `[window, window + 1000)`.
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let samples = make_samples(series, ts);
    let started = Instant::now();
    rt.block_on(engine.write(samples)).expect("write");
    println!(
        "metric query bench: series={series} queries={queries} \
         (write {:.2} s)",
        started.elapsed().as_secs_f64()
    );

    let eq = |name: &[u8], value: &[u8]| LabelMatcher {
        matcher_type: MatcherType::Eq,
        name: name.to_vec(),
        value: value.to_vec(),
    };
    let re = |name: &[u8], value: &[u8]| LabelMatcher {
        matcher_type: MatcherType::Re,
        name: name.to_vec(),
        value: value.to_vec(),
    };
    let shapes: Vec<(&str, Vec<LabelMatcher>, usize)> = vec![
        ("name only", vec![], 0),
        ("eq job", vec![eq(b"job", b"api")], 0),
        ("eq job limit=1000", vec![eq(b"job", b"api")], 1_000),
        (
            "eq job+code",
            vec![eq(b"job", b"api"), eq(b"code", b"200")],
            0,
        ),
        ("eq instance", vec![eq(b"instance", b"host-12345")], 0),
        ("re instance", vec![re(b"instance", b"host-123.*")], 0),
    ];

    // One-second window around the sample timestamp.
    let window = ts;
    for (label, matchers, limit) in &shapes {
        let req = SearchRequest {
            metric_name: b"http_requests".to_vec(),
            matchers: matchers.clone(),
            start_ms: window,
            end_ms: window + 5_000,
            limit: *limit,
        };
        let mut latencies = Vec::with_capacity(per_shape);
        let mut matched = 0usize;
        let index_before = engine.index_stats().map_or(0, |s| s.index_micros());
        for _ in 0..per_shape {
            let started = Instant::now();
            let found = rt.block_on(engine.search(&req)).expect("search");
            latencies.push(started.elapsed().as_micros() as u64);
            matched += found.len();
        }
        latencies.sort_unstable();
        let p =
            |q: f64| latencies[((latencies.len() as f64 * q) as usize).min(latencies.len() - 1)];
        let index_micros = engine.index_stats().map_or(0, |s| s.index_micros()) - index_before;
        println!(
            "query {label:18} p50={:>7} us  p99={:>7} us  max={:>7} us  avg_matched={:<8} index_avg={} us",
            p(0.50),
            p(0.99),
            latencies[latencies.len() - 1],
            matched / per_shape,
            index_micros / per_shape as u64
        );
    }

    // Last-value (instant) query: served from the per-series cache the write
    // path maintains, so it should stay far below the range-query latencies.
    let last_req = SearchRequest {
        metric_name: b"http_requests".to_vec(),
        matchers: vec![eq(b"instance", b"host-12345")],
        start_ms: 0,
        end_ms: ts + 1,
        limit: 0,
    };
    let mut latencies = Vec::with_capacity(per_shape);
    let mut matched = 0usize;
    for _ in 0..per_shape {
        let started = Instant::now();
        let found = rt
            .block_on(engine.last_values(&last_req))
            .expect("last values");
        latencies.push(started.elapsed().as_micros() as u64);
        matched += found.len();
    }
    latencies.sort_unstable();
    let p = |q: f64| latencies[((latencies.len() as f64 * q) as usize).min(latencies.len() - 1)];
    println!(
        "query {:<14} p50={:>7} us  p99={:>7} us  max={:>7} us  avg_matched={}",
        "last eq instance",
        p(0.50),
        p(0.99),
        latencies[latencies.len() - 1],
        matched / per_shape
    );

    let _ = std::fs::remove_dir_all(&root);
}

fn make_samples(series: usize, ts: i64) -> Vec<Sample> {
    (0..series)
        .map(|i| {
            let job: &[u8] = if i % 10 == 0 { b"db" } else { b"api" };
            let code: &[u8] = if i % 7 == 0 { b"404" } else { b"200" };
            Sample {
                name: b"http_requests".to_vec(),
                labels: vec![
                    Label {
                        name: b"job".to_vec(),
                        value: job.to_vec(),
                    },
                    Label {
                        name: b"instance".to_vec(),
                        value: format!("host-{i}").into_bytes(),
                    },
                    Label {
                        name: b"code".to_vec(),
                        value: code.to_vec(),
                    },
                ],
                timestamp: ts,
                value: i as f64,
                name_id: None,
                series_id: None,
            }
        })
        .collect()
}
