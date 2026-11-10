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

//! TSBS IoT-shaped benchmark (roadmap Phase 4: "TSBS IoT 场景基准").
//!
//! TSBS ("Time Series Benchmark Suite") models an IoT fleet: N devices report
//! several readings each on a fixed interval, and the query mix is dominated by
//! (a) a dashboard that reads one measurement of a handful of devices over a
//! window, (b) the latest reading of every device, and (c) a fleet-wide
//! aggregate. This benchmark reproduces that shape without the external TSBS
//! binary, so it runs offline and in CI:
//!
//! - devices × readings per report, one report every 10 s over a configurable
//!   duration, written round-robin per timestamp the way a collector batches;
//! - a configurable share of out-of-order samples (a device that reconnects and
//!   backfills), so the LWW path and the rollup dirty-bucket rebuild are
//!   exercised, not just the append path;
//! - queries: per-device window, latest-per-device, fleet window at a coarse
//!   rollup step, and a wide selector, each reported as p50/p99.
//!
//! Usage:
//! `cargo run --release -p benchmarks --bin tsbs_iot_bench -- \
//! [devices] [readings] [minutes] [late_percent] [queries]`
//!
//! Defaults: 1 000 devices, 8 readings, 60 minutes, 5 % late, 50 queries.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use columnar_storage::{config::StorageConfig, storage::StorageRuntimes, types::ObjectStoreRef};
use metric_engine::{
    Aggregation, Label, LabelMatcher, MatcherType, MetricEngine, Sample, SearchRequest,
};
use object_store::local::LocalFileSystem;

/// Hour-aligned so every rollup bucket in the fixture is aligned.
const BASE: i64 = 1_699_999_200_000;
const REPORT_INTERVAL_MS: i64 = 10_000;

/// The measurements a device reports, as TSBS' IoT scenario does (temperature,
/// humidity, pressure, ...). One metric per reading keeps the index shape
/// small.
const READINGS: [&str; 8] = [
    "temperature",
    "humidity",
    "pressure",
    "wind_speed",
    "wind_direction",
    "visibility",
    "precipitation",
    "uv_index",
];

/// One device of the fleet.
struct Device {
    /// `region_<n>` and `service_<n>`: the low-cardinality tags a dashboard
    /// filters on. The device hostname is the high-cardinality tag.
    region: String,
    service: String,
    host: String,
}

fn devices(count: usize) -> Vec<Device> {
    // TSBS derives tags from a deterministic generator too, so the benchmark is
    // reproducible across runs and machines.
    (0..count)
        .map(|i| Device {
            region: format!("region_{}", i % 4),
            service: format!("service_{}", i % 8),
            host: format!("host_{i:06}"),
        })
        .collect()
}

fn reading_value(reading: usize, device: usize, tick: i64) -> f64 {
    // A slow daily trend plus a per-device phase offset, so an aggregate over
    // the fleet is not a constant and a rounding bug cannot hide behind zeros.
    let phase = (device % 97) as f64 * 0.13;
    let trend = (tick as f64 / 360.0) * std::f64::consts::TAU;
    (trend + phase + reading as f64).sin() * 50.0 + 100.0 + reading as f64
}

/// Builds the sample stream of the whole fleet, `ticks` reports in.
///
/// Every sample is emitted exactly once. A share of the ticks is delivered out
/// of order, the way a device that lost its uplink behaves: the collector keeps
/// reporting newer samples, and the buffered window arrives after them.
///
/// - ticks `0..on_time` arrive in order;
/// - the newer half of the outage window arrives on schedule;
/// - the older half of the window arrives last, i.e. older than data already
///   written, which is the out-of-order path (LWW plus rollup dirty rebuild).
fn sample_stream(
    fleet: &[Device],
    ticks: i64,
    late_percent: u64,
    reading_count: usize,
) -> Vec<Sample> {
    let mut samples = Vec::with_capacity(fleet.len() * reading_count * ticks as usize);
    let late_ticks = (ticks * late_percent as i64 / 100).clamp(0, ticks);
    let on_time = ticks - late_ticks;
    let backfill_mid = on_time + late_ticks / 2;
    // On-time reports first, then the backfilled ones: a reconnect that flushes
    // a device's buffer arrives after newer data.
    let emit = |tick: i64, fleet: &[Device], out: &mut Vec<Sample>| {
        let timestamp = BASE + tick * REPORT_INTERVAL_MS;
        for (d, device) in fleet.iter().enumerate() {
            for (idx, reading) in READINGS[..reading_count].iter().enumerate() {
                out.push(Sample {
                    name: reading.as_bytes().to_vec(),
                    labels: vec![
                        Label {
                            name: b"host".to_vec(),
                            value: device.host.as_bytes().to_vec(),
                        },
                        Label {
                            name: b"region".to_vec(),
                            value: device.region.as_bytes().to_vec(),
                        },
                        Label {
                            name: b"service".to_vec(),
                            value: device.service.as_bytes().to_vec(),
                        },
                    ],
                    timestamp,
                    value: reading_value(idx, d, tick),
                    name_id: None,
                    series_id: None,
                });
            }
        }
    };
    for tick in 0..on_time {
        emit(tick, fleet, &mut samples);
    }
    for tick in backfill_mid..ticks {
        emit(tick, fleet, &mut samples);
    }
    for tick in on_time..backfill_mid {
        emit(tick, fleet, &mut samples);
    }
    samples
}

fn matcher(name: &str, value: &str) -> LabelMatcher {
    LabelMatcher {
        matcher_type: MatcherType::Eq,
        name: name.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
    }
}

struct Latency {
    p50: u64,
    p99: u64,
    count: usize,
}

fn measure(queries: usize, mut f: impl FnMut() -> usize) -> Latency {
    // Warm up once so the first query's cold cache does not sit in p99.
    let _ = f();
    let mut latencies = Vec::with_capacity(queries);
    let mut count = 0;
    for _ in 0..queries {
        let started = Instant::now();
        count = f();
        latencies.push(started.elapsed().as_micros() as u64);
    }
    latencies.sort_unstable();
    let p = |q: f64| latencies[((latencies.len() as f64 * q) as usize).min(latencies.len() - 1)];
    Latency {
        p50: p(0.50),
        p99: p(0.99),
        count,
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let device_count: usize = args.next().and_then(|v| v.parse().ok()).unwrap_or(1_000);
    let reading_count: usize = args.next().and_then(|v| v.parse().ok()).unwrap_or(8);
    let minutes: i64 = args.next().and_then(|v| v.parse().ok()).unwrap_or(60);
    let late_percent: u64 = args.next().and_then(|v| v.parse().ok()).unwrap_or(5);
    let queries: usize = args.next().and_then(|v| v.parse().ok()).unwrap_or(50);
    assert!(
        reading_count <= READINGS.len(),
        "at most {} readings are modelled",
        READINGS.len()
    );

    let root = std::env::temp_dir().join(format!(
        "tsbs-iot-bench-{}-{}",
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

    let fleet = devices(device_count);
    let ticks = minutes * 60_000 / REPORT_INTERVAL_MS;
    let samples = sample_stream(&fleet, ticks, late_percent, reading_count);
    let total = samples.len();
    let series = device_count * reading_count;
    println!(
        "TSBS IoT bench: devices={device_count} readings={reading_count} series={series} \
         minutes={minutes} late={late_percent}% samples={} ({:.2} M)",
        total,
        total as f64 / 1e6
    );

    // Write in collector-sized batches (a 1 000-device fleet reporting every
    // 10 s is one batch per tick), so the benchmark measures the ingest path a
    // deployment actually uses instead of one giant call.
    let batch = device_count * reading_count;
    let started = Instant::now();
    let mut written = 0usize;
    for chunk in samples.chunks(batch.max(1)) {
        rt.block_on(engine.write(chunk.to_vec())).expect("write");
        written += chunk.len();
    }
    let write_s = started.elapsed().as_secs_f64();
    let started = Instant::now();
    rt.block_on(engine.flush_all()).expect("flush");
    let flush_s = started.elapsed().as_secs_f64();
    println!(
        "ingest: {written} samples in {write_s:.2}s = {:.0} samples/s \
         (flush {flush_s:.2}s)",
        written as f64 / write_s
    );

    let end = BASE + ticks * REPORT_INTERVAL_MS;

    // (a) Dashboard: one reading, a few devices, the whole window.
    let req = SearchRequest {
        metric_name: b"temperature".to_vec(),
        matchers: vec![matcher("region", "region_1")],
        start_ms: BASE,
        end_ms: end,
        limit: 0,
    };
    let lat = measure(queries, || {
        rt.block_on(engine.search(&req)).expect("range").len()
    });
    println!(
        "query {:24} p50={:>7}us p99={:>7}us  series={}",
        "range region", lat.p50, lat.p99, lat.count
    );

    let req = SearchRequest {
        metric_name: b"temperature".to_vec(),
        matchers: vec![matcher("host", "host_000123")],
        start_ms: BASE,
        end_ms: end,
        limit: 0,
    };
    let lat = measure(queries, || {
        rt.block_on(engine.search(&req)).expect("single").len()
    });
    println!(
        "query {:24} p50={:>7}us p99={:>7}us  series={}",
        "range single device", lat.p50, lat.p99, lat.count
    );

    // (b) Latest reading of every device (the "is the fleet healthy" panel).
    let req = SearchRequest {
        metric_name: b"temperature".to_vec(),
        matchers: vec![],
        start_ms: 0,
        end_ms: end,
        limit: 0,
    };
    let lat = measure(queries, || {
        rt.block_on(engine.last_values(&req)).expect("last").len()
    });
    println!(
        "query {:24} p50={:>7}us p99={:>7}us  series={}",
        "latest fleet", lat.p50, lat.p99, lat.count
    );

    // (c) Fleet-wide aggregate over the window, read from the rollup tables.
    for (label, step) in [("rollup 1m", 60_000i64), ("rollup 1h", 3_600_000)] {
        let req = SearchRequest {
            metric_name: b"temperature".to_vec(),
            matchers: vec![matcher("service", "service_3")],
            start_ms: BASE,
            end_ms: end,
            limit: 0,
        };
        let lat = measure(queries, || {
            let found = rt
                .block_on(engine.search_downsampled(&req, step, Aggregation::Avg))
                .expect("rollup");
            found.iter().map(|s| s.samples.len()).sum()
        });
        println!(
            "query {label:24} p50={:>7}us p99={:>7}us  points={}",
            lat.p50, lat.p99, lat.count
        );
    }

    // (d) Wide selector: every series of a low-cardinality tag.
    let req = SearchRequest {
        metric_name: b"temperature".to_vec(),
        matchers: vec![matcher("service", "service_0")],
        start_ms: BASE,
        end_ms: end,
        limit: 0,
    };
    let lat = measure(queries, || {
        rt.block_on(engine.search(&req)).expect("wide").len()
    });
    println!(
        "query {:24} p50={:>7}us p99={:>7}us  series={}",
        "range wide service", lat.p50, lat.p99, lat.count
    );

    let _ = std::fs::remove_dir_all(&root);
}
