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

//! Phase 2 end-to-end benchmark (ADR-0002 §12 T5).
//!
//! Unlike `phase2_bench`, which measures the kernel decode path on an
//! in-memory file, this runs the whole storage stack for both formats: WAL +
//! buffer + flush + manifest commit for writes, and `ColumnarStorage::scan` for
//! reads. It reports the T5 matrix:
//!
//! - size (bytes per sample) and file count from the write counters;
//! - durable ingest throughput (write + flush) and the flush share of it;
//! - full-scan throughput and p50/p99 for a Grafana-style one-day query and a
//!   30-day single-series query;
//! - page pruning as the bytes read for a one-hour window over the bytes read
//!   for the same series across the whole dataset;
//! - restart time (manifest load + first full scan).
//!
//! Usage:
//! `cargo run --release -p benchmarks --bin phase2_e2e_bench -- [series] [days]
//! [gauge|counter]`
//!
//! Defaults: 50 series, 30 days, gauge values, 15 s scrape interval.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::{
    array::{Float64Array, Int64Array, RecordBatch, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use columnar_storage::{
    config::{BufferConfig, DataFileFormat, SchedulerConfig, StorageConfig},
    metrics::FormatStats,
    sst::FileFormat,
    storage::{ColumnarStorage, ObjectBasedStorage, ScanRequest, StorageRuntimes, WriteRequest},
    types::TimeRange,
};
use common::{ReadableDuration, ReadableSize};
use datafusion::logical_expr::{col, lit, Expr};
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use storage_kernel::tsf::{codec::MarshalType, partition::DAY_MS};
use tokio::runtime::Runtime;

const STEP_MS: i64 = 15_000;
const SAMPLES_PER_DAY: i64 = DAY_MS / STEP_MS; // 5,760
const HOUR_MS: i64 = 3_600_000;
const METRIC_ID: u64 = 1;
/// Query iterations per shape. p99 over twenty samples is the second worst,
/// which is enough to catch a one-off stall without making the benchmark slow.
const QUERY_ITERS: usize = 20;

fn main() -> Result<(), String> {
    let args: Vec<String> = std::env::args().collect();
    let series: u64 = args.get(1).map(|s| s.parse().unwrap()).unwrap_or(50);
    let days: i64 = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(30);
    let shape = args
        .get(3)
        .map(String::as_str)
        .unwrap_or("gauge")
        .to_string();
    if shape != "gauge" && shape != "counter" {
        return Err(format!(
            "unknown data shape {shape:?}; use gauge or counter"
        ));
    }
    if series == 0 || days <= 0 {
        return Err("series and days must be positive".to_string());
    }

    // Align the dataset to whole UTC days so each write stays inside one
    // segment and each TSF file inside one day partition.
    let now = common::now();
    let base_ts = (now.div_euclid(DAY_MS) - days) * DAY_MS;
    let ds = Dataset {
        series,
        days,
        base_ts,
    };

    println!("# Phase 2 end-to-end storage benchmark");
    println!();
    println!(
        "Dataset: {} series x {} days x {} samples/day = {} rows, one metric, \
         {} ms scrape interval, {shape} values.",
        ds.series,
        ds.days,
        SAMPLES_PER_DAY,
        ds.rows(),
        STEP_MS
    );
    println!();

    let rt = Arc::new(Runtime::new().map_err(|e| e.to_string())?);
    let mut reports = Vec::new();
    for format in [DataFileFormat::Parquet, DataFileFormat::Tsf] {
        reports.push(
            rt.block_on(run_format(&rt, &ds, format, &shape))
                .map_err(|e| e.to_string())?,
        );
    }
    print_summary(&reports, days);

    // Dropping the runtime closes the flush loops of the two storages.
    drop(rt);
    Ok(())
}

#[derive(Clone, Copy)]
struct Dataset {
    series: u64,
    days: i64,
    base_ts: i64,
}

impl Dataset {
    fn samples_per_series(&self) -> i64 {
        self.days * SAMPLES_PER_DAY
    }

    fn rows(&self) -> u64 {
        self.series * self.samples_per_series() as u64
    }

    fn day_start(&self, day: i64) -> i64 {
        self.base_ts + day * DAY_MS
    }

    /// One-day range at the end of the dataset, the Grafana default window.
    fn last_day(&self) -> TimeRange {
        (self.day_start(self.days - 1)..self.day_start(self.days)).into()
    }

    /// One hour in the middle of the last day.
    fn last_day_hour(&self) -> TimeRange {
        let start = self.day_start(self.days - 1) + 12 * HOUR_MS;
        (start..start + HOUR_MS).into()
    }

    fn whole(&self) -> TimeRange {
        (self.base_ts..self.day_start(self.days)).into()
    }
}

struct FormatReport {
    format: &'static str,
    rows: u64,
    files_written: u64,
    bytes: u64,
    ingest_secs: f64,
    flush_secs: f64,
    full_scan_rows_per_sec: f64,
    full_scan_p50_ms: f64,
    full_scan_p99_ms: f64,
    day_p50_ms: f64,
    day_p99_ms: f64,
    month_p50_ms: f64,
    month_p99_ms: f64,
    hour_bytes: u64,
    month_bytes: u64,
    restart_ms: f64,
}

async fn run_format(
    rt: &Arc<Runtime>,
    ds: &Dataset,
    format: DataFileFormat,
    shape: &str,
) -> Result<FormatReport, String> {
    let name = match format {
        DataFileFormat::Parquet => "parquet",
        DataFileFormat::Tsf => "tsf",
    };
    let file_format: FileFormat = format.clone().into();
    let root =
        std::env::temp_dir().join(format!("thingsdb-phase2-e2e-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).map_err(|e| format!("create {root:?}: {e}"))?;
    let root = root.to_string_lossy().to_string();
    let schema = sample_schema();

    let config = StorageConfig {
        format,
        // Hold the whole dataset in the buffer so the benchmark measures one
        // deterministic flush instead of the background flush loop. The write
        // path is still the real one: WAL append + fsync, sort, encode,
        // manifest commit.
        buffer: BufferConfig {
            max_rows: ds.rows() as usize,
            max_bytes: ReadableSize::gb(4),
            flush_interval: ReadableDuration::hours(1),
        },
        // Keep compaction out of the query measurements; it has its own
        // counters.
        scheduler: SchedulerConfig {
            schedule_interval: ReadableDuration::hours(1),
            ..Default::default()
        },
        ..Default::default()
    };

    println!("## {name}");
    println!();

    let storage = open(rt, &root, &schema, config.clone()).await?;

    // ---- Write path ----
    // Batches are generated up front of each write and not timed: the target is
    // the storage path (WAL + buffer + flush + commit), not value generation.
    let ingest_start = Instant::now();
    for day in 0..ds.days {
        for series_id in 0..ds.series {
            let batch = series_day_batch(&schema, ds, series_id, day, shape)?;
            storage
                .write(WriteRequest {
                    batch,
                    time_range: (ds.day_start(day)..ds.day_start(day + 1)).into(),
                    enable_check: true,
                })
                .await
                .map_err(|e| format!("{name} write: {e}"))?;
        }
    }
    let write_elapsed = ingest_start.elapsed();
    let flush_start = Instant::now();
    storage
        .flush()
        .await
        .map_err(|e| format!("{name} flush: {e}"))?;
    let flush_elapsed = flush_start.elapsed();
    let ingest_secs = (write_elapsed + flush_elapsed).as_secs_f64();

    let stats: FormatStats = storage.stats().for_format(file_format);
    if stats.rows_written != ds.rows() {
        return Err(format!(
            "{name}: counters report {} rows written, expected {}",
            stats.rows_written,
            ds.rows()
        ));
    }
    println!(
        "Ingest (WAL + buffer): {:.2} s, {:.1} M rows/s; flush + commit: {:.2} s",
        write_elapsed.as_secs_f64(),
        ds.rows() as f64 / write_elapsed.as_secs_f64() / 1e6,
        flush_elapsed.as_secs_f64(),
    );
    println!(
        "Ingest to durable (write + flush): {:.1} M rows/s; {} files, {} bytes, {:.2} B/sample",
        ds.rows() as f64 / ingest_secs / 1e6,
        stats.files_written,
        stats.bytes_written,
        stats.bytes_written as f64 / ds.rows() as f64,
    );
    // The flush `write` stage is split into encode (CPU) and upload (object
    // PUT) for TSF; the streaming Parquet writer cannot separate them.
    let flush = storage.stats().flush;
    if file_format == FileFormat::Tsf {
        match flush.write_split() {
            Some((encode, upload)) => println!(
                "Flush write split: encode {:.0}% / upload {:.0}% ({} / {} ms of {} ms)",
                encode * 100.0,
                upload * 100.0,
                flush.encode_millis,
                flush.upload_millis,
                flush.write_millis,
            ),
            None => println!(
                "Flush write split: encode/upload too short to time (write {} ms)",
                flush.write_millis
            ),
        }
    } else {
        println!("Flush write split: not reported (streaming writer)");
    }
    if file_format == FileFormat::Tsf {
        // The size target is dominated by the value payload, so report what
        // each codec actually produced (ADR-0002 §4.4, §13).
        let kernel = storage.stats().tsf_kernel;
        let raw: u64 = kernel.codec_raw_bytes.iter().sum();
        if raw > 0 {
            println!(
                "Kernel codec profile: {} pages ({:.1}% without ZSTD), payload {:.2} B/sample",
                kernel.pages,
                kernel.uncompressed_ratio() * 100.0,
                kernel.payload_bytes() as f64 * 8.0 / raw as f64,
            );
            for id in 1..=MarshalType::MAX_ID {
                let Some(codec) = MarshalType::from_u8(id) else {
                    continue;
                };
                let count = kernel.codec_count(codec);
                if count == 0 {
                    continue;
                }
                let raw_codec = kernel.codec_raw_bytes(codec);
                let per_sample = if raw_codec == 0 {
                    0.0
                } else {
                    kernel.codec_bytes(codec) as f64 * 8.0 / raw_codec as f64
                };
                println!(
                    "  {codec:?}: {count} payloads, {per_sample:.2} B/sample, {:.2}x",
                    kernel.codec_compression_ratio(codec).unwrap_or(0.0)
                );
            }
        }
    }
    println!();

    // ---- Full scan ----
    let (full_p50, full_p99, full_samples) = measure(
        &storage,
        ds.whole(),
        Vec::new(),
        ds.rows() as usize,
        QUERY_ITERS,
    )
    .await?;
    let full_best = full_samples
        .iter()
        .min()
        .copied()
        .unwrap_or_else(|| Duration::from_secs(1));
    println!(
        "Full scan: p50 {:.1} ms, p99 {:.1} ms, best {:.1} M rows/s",
        full_p50,
        full_p99,
        ds.rows() as f64 / full_best.as_secs_f64() / 1e6,
    );

    // ---- Query latency ----
    let target_series = ds.series / 2;
    let predicate = |range: &TimeRange| {
        vec![
            col("tsid").eq(lit(target_series)),
            col("timestamp").gt_eq(lit(range.start.0)),
            col("timestamp").lt(lit(range.end.0)),
        ]
    };

    let day = ds.last_day();
    let (day_p50, day_p99, _) = measure(
        &storage,
        day.clone(),
        predicate(&day),
        SAMPLES_PER_DAY as usize,
        QUERY_ITERS,
    )
    .await?;
    println!(
        "1-day single-series query: p50 {:.2} ms, p99 {:.2} ms",
        day_p50, day_p99
    );

    let month = ds.whole();
    let (month_p50, month_p99, _) = measure(
        &storage,
        month.clone(),
        predicate(&month),
        ds.samples_per_series() as usize,
        QUERY_ITERS,
    )
    .await?;
    println!(
        "{}-day single-series query: p50 {:.2} ms, p99 {:.2} ms",
        ds.days, month_p50, month_p99
    );

    // ---- Page pruning: bytes read for a one-hour window vs the whole dataset
    // ----
    let hour = ds.last_day_hour();
    let hour_bytes = bytes_for_query(&storage, file_format, hour.clone(), predicate(&hour)).await?;
    let month_bytes =
        bytes_for_query(&storage, file_format, month.clone(), predicate(&month)).await?;
    println!(
        "Pruning (1 h / {} d read volume): {} / {} bytes = {:.2}%",
        ds.days,
        hour_bytes,
        month_bytes,
        hour_bytes as f64 * 100.0 / month_bytes.max(1) as f64,
    );
    println!();

    // ---- Restart: manifest load + first full scan ----
    drop(storage);
    let restart_start = Instant::now();
    let reopened = open(rt, &root, &schema, config).await?;
    let recovered = scan_rows(&reopened, ds.whole(), Vec::new(), ds.rows() as usize).await?;
    let restart = restart_start.elapsed();
    debug_assert_eq!(recovered, ds.rows() as usize);
    println!(
        "Restart (manifest load + first full scan): {:.1} ms",
        restart.as_secs_f64() * 1e3
    );
    println!();
    drop(reopened);
    let _ = std::fs::remove_dir_all(&root);

    Ok(FormatReport {
        format: name,
        rows: ds.rows(),
        files_written: stats.files_written,
        bytes: stats.bytes_written,
        ingest_secs,
        flush_secs: flush_elapsed.as_secs_f64(),
        full_scan_rows_per_sec: ds.rows() as f64 / full_best.as_secs_f64(),
        full_scan_p50_ms: full_p50,
        full_scan_p99_ms: full_p99,
        day_p50_ms: day_p50,
        day_p99_ms: day_p99,
        month_p50_ms: month_p50,
        month_p99_ms: month_p99,
        hour_bytes,
        month_bytes,
        restart_ms: restart.as_secs_f64() * 1e3,
    })
}

async fn open(
    rt: &Arc<Runtime>,
    root: &str,
    schema: &SchemaRef,
    config: StorageConfig,
) -> Result<ObjectBasedStorage, String> {
    ObjectBasedStorage::try_new(
        root.to_string(),
        Duration::from_secs(24 * 3600),
        Arc::new(LocalFileSystem::new()),
        schema.clone(),
        3, // metric_id, tsid, timestamp
        config,
        StorageRuntimes::new(rt.clone(), rt.clone()),
    )
    .await
    .map_err(|e| format!("open {root}: {e}"))
}

fn sample_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("metric_id", DataType::UInt64, true),
        Field::new("tsid", DataType::UInt64, true),
        Field::new("timestamp", DataType::Int64, true),
        Field::new("value", DataType::Float64, true),
    ]))
}

/// The rows of one series for one UTC day, already in primary-key order.
fn series_day_batch(
    schema: &SchemaRef,
    ds: &Dataset,
    series_id: u64,
    day: i64,
    shape: &str,
) -> Result<RecordBatch, String> {
    let n = SAMPLES_PER_DAY as usize;
    let mut metric = Vec::with_capacity(n);
    let mut tsid = Vec::with_capacity(n);
    let mut timestamps = Vec::with_capacity(n);
    let mut values = Vec::with_capacity(n);
    let day_start = ds.day_start(day);
    for i in 0..SAMPLES_PER_DAY {
        let index = day * SAMPLES_PER_DAY + i;
        let value = if shape == "counter" {
            1_000.0 + series_id as f64 + index as f64 * 10.0
        } else {
            series_id as f64 * 0.001 + (index as f64 * 0.01).sin()
        };
        metric.push(METRIC_ID);
        tsid.push(series_id);
        timestamps.push(day_start + i * STEP_MS);
        values.push(value);
    }
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(metric)),
            Arc::new(UInt64Array::from(tsid)),
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(Float64Array::from(values)),
        ],
    )
    .map_err(|e| format!("build batch: {e}"))
}

async fn scan_rows(
    storage: &ObjectBasedStorage,
    range: TimeRange,
    predicate: Vec<Expr>,
    expected: usize,
) -> Result<usize, String> {
    let mut stream = storage
        .scan(ScanRequest {
            range,
            predicate,
            projections: None,
        })
        .await
        .map_err(|e| format!("scan: {e}"))?;
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        rows += batch.map_err(|e| format!("scan batch: {e}"))?.num_rows();
    }
    if rows != expected {
        return Err(format!("scan returned {rows} rows, expected {expected}"));
    }
    Ok(rows)
}

/// Runs `iters` scans and returns (p50 ms, p99 ms, all samples).
async fn measure(
    storage: &ObjectBasedStorage,
    range: TimeRange,
    predicate: Vec<Expr>,
    expected: usize,
    iters: usize,
) -> Result<(f64, f64, Vec<Duration>), String> {
    // One warm-up scan: the first query of a shape pays page-cache and plan
    // construction costs that a Grafana dashboard amortises across refreshes.
    // Restart cost is measured separately, cold, below.
    scan_rows(storage, range.clone(), predicate.clone(), expected).await?;

    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let start = Instant::now();
        scan_rows(storage, range.clone(), predicate.clone(), expected).await?;
        samples.push(start.elapsed());
    }
    Ok((
        percentile_ms(&mut samples, 0.5),
        percentile_ms(&mut samples, 0.99),
        samples,
    ))
}

async fn bytes_for_query(
    storage: &ObjectBasedStorage,
    format: FileFormat,
    range: TimeRange,
    predicate: Vec<Expr>,
) -> Result<u64, String> {
    let before = storage.stats().for_format(format).bytes_read;
    let mut stream = storage
        .scan(ScanRequest {
            range,
            predicate,
            projections: None,
        })
        .await
        .map_err(|e| format!("scan: {e}"))?;
    while let Some(batch) = stream.next().await {
        batch.map_err(|e| format!("scan batch: {e}"))?;
    }
    let after = storage.stats().for_format(format).bytes_read;
    Ok(after - before)
}

fn percentile_ms(samples: &mut [Duration], p: f64) -> f64 {
    samples.sort_unstable();
    let idx = ((samples.len() as f64 - 1.0) * p).ceil() as usize;
    samples[idx.min(samples.len() - 1)].as_secs_f64() * 1e3
}

fn print_summary(reports: &[FormatReport], days: i64) {
    println!("## Summary");
    println!();
    println!(
        "| metric | {} |",
        reports
            .iter()
            .map(|r| r.format)
            .collect::<Vec<_>>()
            .join(" | ")
    );
    println!("|---|{}", "---|".repeat(reports.len()));
    let row = |label: &str, f: &dyn Fn(&FormatReport) -> String| {
        println!(
            "| {label} | {} |",
            reports.iter().map(f).collect::<Vec<_>>().join(" | ")
        );
    };
    row("rows", &|r| r.rows.to_string());
    row("files written", &|r| r.files_written.to_string());
    row("bytes", &|r| r.bytes.to_string());
    row("bytes/sample", &|r| {
        format!("{:.2}", r.bytes as f64 / r.rows as f64)
    });
    row("ingest (write + flush) M rows/s", &|r| {
        format!("{:.1}", r.rows as f64 / r.ingest_secs / 1e6)
    });
    row("flush share", &|r| {
        format!("{:.0}%", r.flush_secs * 100.0 / r.ingest_secs)
    });
    row("full scan M rows/s", &|r| {
        format!("{:.1}", r.full_scan_rows_per_sec / 1e6)
    });
    row("full scan p50/p99 ms", &|r| {
        format!("{:.1} / {:.1}", r.full_scan_p50_ms, r.full_scan_p99_ms)
    });
    row("1-day query p50/p99 ms", &|r| {
        format!("{:.2} / {:.2}", r.day_p50_ms, r.day_p99_ms)
    });
    row(&format!("{days}-day query p50/p99 ms"), &|r| {
        format!("{:.2} / {:.2}", r.month_p50_ms, r.month_p99_ms)
    });
    row(&format!("1 h read / {days} d read"), &|r| {
        format!(
            "{:.2}% ({} / {} B)",
            r.hour_bytes as f64 * 100.0 / r.month_bytes.max(1) as f64,
            r.hour_bytes,
            r.month_bytes
        )
    });
    row("restart ms", &|r| format!("{:.1}", r.restart_ms));
    println!();
}
