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

//! Phase 2 vectorized-decode benchmark.
//!
//! Exercises the column scan path (`read_metric_columns`,
//! `scan_metric_parallel`) rather than the scalar `Vec<Row>` oracle, and
//! reports throughput, cache statistics and decode working-set bytes.
//!
//! Usage:
//! `cargo run --release -p benchmarks --bin phase2_bench -- [series] [samples]
//! [shape]`

use std::time::{Duration, Instant};

use storage_kernel::tsf::{
    file::{
        encode_file, scan_metric_parallel, DecodedColumns, ReadContext, ScanOptions, TimeRange,
        TsfReader, WriterOptions,
    },
    ColumnMask, Row,
};

const BASE_TS: i64 = 1_700_000_000_000;
const STEP_MS: i64 = 15_000;
const METRIC_ID: u64 = 1;

fn main() -> Result<(), String> {
    let args: Vec<String> = std::env::args().collect();
    let series: u64 = args.get(1).map(|s| s.parse().unwrap()).unwrap_or(20_000);
    let samples: i64 = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(200);
    let shape = args.get(3).map(String::as_str).unwrap_or("gauge");
    if shape != "gauge" && shape != "counter" {
        return Err(format!(
            "unknown data shape {shape:?}; use gauge or counter"
        ));
    }
    let rows = series * samples as u64;

    println!("# Phase 2 vectorized decode benchmark");
    println!();
    println!(
        "Dataset: {series} series x {samples} samples = {rows} rows, one metric, \
         {STEP_MS} ms scrape interval, {shape} values."
    );
    println!();

    let data = generate(series, samples, shape);
    let (tsf_bytes, stats) = encode_file(
        data,
        (BASE_TS, BASE_TS + samples * STEP_MS),
        &WriterOptions::default(),
    )?;
    println!(
        "TSF encode: {} bytes, {:.2} bytes/row, {} series, {} groups",
        tsf_bytes.len(),
        tsf_bytes.len() as f64 / rows as f64,
        stats.series,
        stats.groups
    );
    let reader = TsfReader::from_bytes(tsf_bytes)?;
    println!("File identity: 0x{:016x}", reader.file_identity());
    println!();

    benchmark_scalar(&reader, rows)?;
    benchmark_columns(&reader, rows)?;
    benchmark_parallel(&reader, rows)?;
    benchmark_arrow_scan(&reader, rows)?;
    benchmark_selective(&reader, series, samples)?;
    Ok(())
}

fn generate(series: u64, samples: i64, shape: &str) -> Vec<Row> {
    let mut rows = Vec::with_capacity(series as usize * samples as usize);
    for s in 0..series {
        for i in 0..samples {
            let value = if shape == "counter" {
                1_000.0 + (s as f64) + (i as f64) * 10.0
            } else {
                (s as f64) * 0.001 + ((i as f64) * 0.01).sin()
            };
            rows.push(Row {
                metric_id: METRIC_ID,
                series_id: s,
                timestamp: BASE_TS + i * STEP_MS,
                value,
                sequence: i as u64,
                reserved: 0,
            });
        }
    }
    rows
}

fn time_best<F, T>(iters: usize, mut f: F) -> Result<(Duration, T), String>
where
    F: FnMut() -> Result<T, String>,
{
    let mut best = Duration::MAX;
    let mut out = None;
    for _ in 0..iters {
        let start = Instant::now();
        out = Some(f()?);
        best = best.min(start.elapsed());
    }
    Ok((best, out.unwrap()))
}

fn report(label: &str, elapsed: Duration, rows: usize, callback: impl FnOnce()) {
    println!(
        "{label}: {:.3} s ({:.1} M rows/s), {:.2} ns/row",
        elapsed.as_secs_f64(),
        rows as f64 / elapsed.as_secs_f64() / 1e6,
        elapsed.as_secs_f64() * 1e9 / rows as f64
    );
    callback();
}

fn total_rows(columns: &[DecodedColumns]) -> usize {
    columns.iter().map(DecodedColumns::len).sum()
}

fn benchmark_scalar(reader: &TsfReader, rows: u64) -> Result<(), String> {
    let (elapsed, out) = time_best(3, || reader.read_all().map(|v| v.len()))?;
    report("Scalar read_all (Vec<Row>)", elapsed, out, || {});
    debug_assert_eq!(out, rows as usize);
    Ok(())
}

fn benchmark_columns(reader: &TsfReader, rows: u64) -> Result<(), String> {
    let range = TimeRange::new(i64::MIN, i64::MAX);
    let mut ctx = ReadContext::new();
    let (elapsed, out) = time_best(3, || {
        let columns = reader.read_metric_columns(METRIC_ID, range, ColumnMask::SAMPLE, &mut ctx)?;
        Ok(total_rows(&columns))
    })?;
    let stats = ctx.cache_stats();
    report("Column scan (single worker, SAMPLE)", elapsed, out, || {
        println!(
            "  cache: hits={} misses={} evictions={} rejects={} hit_ratio={:.2}%, bytes={}",
            stats.hits,
            stats.misses,
            stats.evictions,
            stats.rejects,
            stats.hit_ratio() * 100.0,
            ctx.cache_bytes()
        );
        println!(
            "  decode working set: {} bytes",
            ctx.buffers.capacity_bytes()
        );
    });
    debug_assert_eq!(out, rows as usize);
    Ok(())
}

fn benchmark_parallel(reader: &TsfReader, rows: u64) -> Result<(), String> {
    let range = TimeRange::new(i64::MIN, i64::MAX);
    for workers in [0usize, 2, 4, 8] {
        let opts = ScanOptions {
            workers,
            ..ScanOptions::default()
        };
        let (elapsed, result) = time_best(3, || {
            scan_metric_parallel(reader, METRIC_ID, range, ColumnMask::SAMPLE, &opts)
        })?;
        let decoded = total_rows(&result.columns);
        debug_assert_eq!(decoded, rows as usize);
        let label = if workers == 0 {
            "Parallel column scan (auto workers)".to_string()
        } else {
            format!("Parallel column scan ({workers} workers)")
        };
        report(&label, elapsed, decoded, || {
            println!(
                "  workers={} cache hit_ratio={:.2}% cache_bytes={} decoded_bytes={}",
                result.workers,
                result.stats.hit_ratio() * 100.0,
                result.stats.bytes,
                result.bytes
            );
        });
    }
    Ok(())
}

fn benchmark_arrow_scan(reader: &TsfReader, rows: u64) -> Result<(), String> {
    // The Arrow conversion lives in `columnar_storage`; the benchmark crate
    // depends on it, so include it to measure the T2 boundary end to end.
    let request =
        columnar_storage::tsf::TsfScanRequest::new(METRIC_ID, TimeRange::new(i64::MIN, i64::MAX));
    let (elapsed, batches) = time_best(3, || columnar_storage::tsf::scan(reader, &request))?;
    let emitted: usize = batches.iter().map(|b| b.num_rows()).sum();
    report("Arrow scan (T2 boundary)", elapsed, emitted, || {
        println!(
            "  batches={} avg_rows={:.0}",
            batches.len(),
            emitted as f64 / batches.len().max(1) as f64
        );
    });
    debug_assert_eq!(emitted, rows as usize);
    Ok(())
}

fn benchmark_selective(reader: &TsfReader, series: u64, samples: i64) -> Result<(), String> {
    let targets: Vec<u64> = (0..100).map(|i| (i * 7919 + 13) % series).collect();
    let window_start = BASE_TS + samples * STEP_MS * 45 / 100;
    let window_end = BASE_TS + samples * STEP_MS * 55 / 100;
    let range = TimeRange::new(window_start, window_end);
    let expected = 100 * ((window_end - window_start) / STEP_MS) as usize;

    let (elapsed, out) = time_best(3, || {
        let mut ctx = ReadContext::new();
        let mut n = 0;
        for &s in &targets {
            n += reader
                .read_series_columns(METRIC_ID, s, range, ColumnMask::SAMPLE, &mut ctx)?
                .len();
        }
        Ok(n)
    })?;
    report(
        "Selective query (100 series, 10% window)",
        elapsed,
        out,
        || {
            println!("  expected rows: {expected}");
        },
    );
    debug_assert_eq!(out, expected);
    Ok(())
}
