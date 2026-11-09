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

//! IndexDB write/query benchmark (ADR-0001 §12 acceptance targets, M5).
//!
//! Builds one month partition with `series` distinct series spread over a
//! handful of flushed parts, then measures:
//!
//! - index write time and rows/s (`write_series` + `flush`),
//! - on-disk bytes per series (a proxy for the "< 200 B/series" budget),
//! - query p50/p99 for an exact tag pair, a broad two-tag intersection, a
//!   literal-prefix regex and a general regex fallback.
//!
//! The ADR target is a p99 under 5 ms for Eq/Regex resolution. The WAL fsync
//! is disabled here because this measures index CPU/IO, not durability (the
//! kill -9 integration test covers the durable ordering).
//!
//! Usage: `cargo run --release -p benchmarks --bin index_query_bench -- \
//! [series] [queries]`
//!
//! Defaults: 200,000 series, 2,000 queries (500 per matcher shape).

use std::{
    path::Path,
    time::{Duration, Instant},
};

use storage_kernel::mergeset::{
    indexdb_table::{IndexDb, IndexDbConfig},
    part_manager::ManagerConfig,
    tag_filter::{QueryPlan, TagMatcher},
    timeutil,
};

fn main() {
    let mut args = std::env::args().skip(1);
    let series: usize = args.next().and_then(|v| v.parse().ok()).unwrap_or(200_000);
    let queries: usize = args.next().and_then(|v| v.parse().ok()).unwrap_or(2_000);
    let per_shape = (queries / 4).max(1);

    let dir = std::env::temp_dir().join(format!(
        "index-query-bench-{}-{}",
        std::process::id(),
        Instant::now().elapsed().as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create bench dir");

    let day = timeutil::days_from_civil(2026, 11, 9);
    let partition = timeutil::month_of_day(day);

    let config = IndexDbConfig {
        spawn_workers: false,
        manager: ManagerConfig {
            wal_sync_on_write: false,
            flush_interval: Duration::from_secs(3_600),
            ..ManagerConfig::default()
        },
    };
    let db = IndexDb::open(&dir, partition, config).expect("open indexdb");

    println!(
        "indexdb bench: series={series} queries={queries} partition={} day={day}",
        db.partition()
    );

    // Write phase: flush every 20k series so the query path sees several parts.
    let batch = 20_000;
    let started = Instant::now();
    let mut rows = 0usize;
    let mut written = 0usize;
    while written < series {
        let end = (written + batch).min(series);
        for i in written..end {
            let instance = format!("host-{i}");
            let job = if i % 10 == 0 { "db" } else { "api" };
            let code = if i % 7 == 0 { "404" } else { "200" };
            let tags: [(&[u8], &[u8]); 3] = [
                (b"job", job.as_bytes()),
                (b"instance", instance.as_bytes()),
                (b"code", code.as_bytes()),
            ];
            rows += db
                .write_series(i as u64, b"http_requests", &tags, &[day])
                .expect("write series");
        }
        db.flush().expect("flush");
        written = end;
    }
    let write_elapsed = started.elapsed();
    let parts = db.stats().flushes();
    println!(
        "write: {series} series, {rows} rows, {parts} flushes in {:.2} s ({:.0} series/s, {:.0} rows/s, {:.0} ns/series)",
        write_elapsed.as_secs_f64(),
        series as f64 / write_elapsed.as_secs_f64(),
        rows as f64 / write_elapsed.as_secs_f64(),
        write_elapsed.as_nanos() as f64 / series as f64
    );
    // Merge the flushed parts so the query path runs against the steady state
    // the background merger reaches, not the worst case of ten fresh parts.
    let mut merges = 0;
    while db.merge_once().expect("merge") {
        merges += 1;
    }
    println!(
        "merged: {merges} merge cycles, write amplification {:.2}x",
        db.stats().write_amplification()
    );
    let disk = dir_bytes(&dir);
    println!(
        "disk: {:.1} MiB, {:.1} B/series",
        disk as f64 / (1024.0 * 1024.0),
        disk as f64 / series as f64
    );

    let name = TagMatcher::Equal {
        key: b"__name__".to_vec(),
        value: b"http_requests".to_vec(),
    };
    let eq = |key: &[u8], value: &[u8]| TagMatcher::Equal {
        key: key.to_vec(),
        value: value.to_vec(),
    };
    let re = |key: &[u8], regex: &[u8]| TagMatcher::Re {
        key: key.to_vec(),
        regex: regex.to_vec(),
    };

    let shapes: Vec<(&str, QueryPlan)> = vec![
        (
            "eq instance",
            QueryPlan::from_matchers(&[name.clone(), eq(b"instance", b"host-12345")]),
        ),
        (
            "eq job+code",
            QueryPlan::from_matchers(&[name.clone(), eq(b"job", b"api"), eq(b"code", b"200")]),
        ),
        (
            "prefix instance",
            QueryPlan::from_matchers(&[name.clone(), re(b"instance", b"host-123.*")]),
        ),
        (
            "regex code",
            QueryPlan::from_matchers(&[name.clone(), re(b"code", b"4.4")]),
        ),
    ];

    for (label, plan) in shapes {
        let mut latencies = Vec::with_capacity(per_shape);
        let mut matched = 0usize;
        for _ in 0..per_shape {
            let started = Instant::now();
            let ids = db.search_series_ids(&plan, &[day]).expect("search");
            latencies.push(started.elapsed().as_micros() as u64);
            matched += ids.len();
        }
        latencies.sort_unstable();
        let p =
            |q: f64| latencies[((latencies.len() as f64 * q) as usize).min(latencies.len() - 1)];
        println!(
            "query {label:16} p50={:>7} us  p99={:>7} us  max={:>7} us  avg_matched={}",
            p(0.50),
            p(0.99),
            latencies[latencies.len() - 1],
            matched / per_shape
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
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
