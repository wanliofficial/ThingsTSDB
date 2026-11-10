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

//! Legacy-vs-mergeset write cost on the same samples (ADR-0001 §12).
//!
//! Writes `samples_per_series` rounds of one sample per series through
//! `MetricEngine::write`, which is the full legacy pipeline (metric ids,
//! series/index rows, Arrow buffers, data buffer) plus the mergeset index
//! registration (dual write, the deployed configuration). Only the first round
//! pays the per-series index cost; later rounds hit the registered-series
//! cache. It also replays the same series into a fresh `IndexDbManager` alone,
//! with the durable defaults (one WAL fsync per frame), and reports both rates
//! and the index share of the total write.
//!
//! The legacy path is buffered by design, so this measures ingestion CPU and
//! WAL cost rather than Parquet flush throughput.
//!
//! Usage: `cargo run --release -p benchmarks --bin metric_write_bench -- \
//! [series] [chunk] [samples_per_series]`

use std::{
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use columnar_storage::{config::StorageConfig, storage::StorageRuntimes, types::ObjectStoreRef};
use metric_engine::{IndexDbManager, IndexMode, Label, MetricEngine, Sample};
use object_store::local::LocalFileSystem;
use storage_kernel::mergeset::indexdb_table::IndexDbConfig;

fn main() {
    let mut args = std::env::args().skip(1);
    let series: usize = args.next().and_then(|v| v.parse().ok()).unwrap_or(200_000);
    let chunk: usize = args.next().and_then(|v| v.parse().ok()).unwrap_or(4_096);
    let rounds: usize = args.next().and_then(|v| v.parse().ok()).unwrap_or(10);

    let base = std::env::temp_dir().join(format!(
        "metric-write-bench-{}-{}",
        std::process::id(),
        Instant::now().elapsed().as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).expect("create bench dir");
    let legacy_root = base.join("engine");
    let index_root = base.join("index-only");
    std::fs::create_dir_all(&legacy_root).unwrap();
    std::fs::create_dir_all(&index_root).unwrap();

    let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());
    let runtimes = StorageRuntimes::new(Arc::clone(&rt), Arc::clone(&rt));
    let store: ObjectStoreRef = Arc::new(LocalFileSystem::new());
    let engine = rt
        .block_on(MetricEngine::try_open_with_index_mode(
            &legacy_root.to_string_lossy(),
            Duration::from_hours(2),
            store,
            StorageConfig::default(),
            runtimes,
            IndexMode::DualRead,
        ))
        .expect("open metric engine");

    let base_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let samples = make_samples(series, base_ts);
    let started = Instant::now();
    for round in 0..rounds {
        let batch = if round == 0 {
            samples.clone()
        } else {
            make_samples(series, base_ts + round as i64 * 1_000)
        };
        rt.block_on(engine.write(batch)).expect("write");
    }
    let total = started.elapsed();
    let total_samples = series * rounds;

    // The same series through the mergeset index alone, with the durable
    // defaults (WAL fsync per frame).
    let index = IndexDbManager::open(&index_root, IndexDbConfig::default()).expect("indexdb");
    let started = Instant::now();
    for batch in samples.chunks(chunk) {
        index.register_samples(batch).expect("register");
    }
    let index_elapsed = started.elapsed();
    index.flush().expect("flush");

    let rows = index.stats().writes().checked_mul(0).unwrap_or_default();
    let _ = rows;
    println!(
        "metric write bench: series={series} rounds={rounds} chunk={chunk} \
         (dual-write engine)"
    );
    println!(
        "engine.write ({total_samples} samples):      {:>8.2} ms  {:>9.0} samples/s",
        total.as_secs_f64() * 1e3,
        total_samples as f64 / total.as_secs_f64()
    );
    println!(
        "indexdb registration (one-time, fsync on): {:>8.2} ms  {:>9.0} series/s",
        index_elapsed.as_secs_f64() * 1e3,
        series as f64 / index_elapsed.as_secs_f64()
    );
    println!(
        "index share of the write:     {:>8.1}%",
        100.0 * index_elapsed.as_secs_f64() / total.as_secs_f64()
    );
    println!(
        "index disk: {:.1} MiB ({:.1} B/series)",
        dir_bytes(&index_root) as f64 / (1024.0 * 1024.0),
        dir_bytes(&index_root) as f64 / series as f64
    );

    let _ = std::fs::remove_dir_all(&base);
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

/// Bytes of every file below `dir`.
fn dir_bytes(dir: &Path) -> u64 {
    let mut total = 0;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        if meta.is_dir() {
            total += dir_bytes(&entry.path());
        } else {
            total += meta.len();
        }
    }
    total
}
