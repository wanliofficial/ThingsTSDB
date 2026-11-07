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

//! Phase 0 benchmark from `docs/roadmap.md`:
//!
//! 1. TSF v1 prototype vs Parquet: write time, file size, full scan, and a
//!    multi-series range query on the same sorted rows.
//! 2. Mergeset: ingest + flush, point range search, and full merge.
//!
//! Usage: `cargo run --release -p benchmarks --bin phase0_bench -- [series]
//! [samples]`
//!
//! Parquet is written with ZSTD level 1 and column statistics enabled, sorted
//! by `(metric, series, timestamp)`. The query filters decoded rows because
//! Parquet has no per-series page index; TSF prunes at group, series and page
//! level.

use std::{
    collections::HashSet,
    fs::File,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::{
    array::{Array, Float64Array, Int64Array, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use storage_kernel::{
    mergeset::table::Table,
    tsf::{
        file::{encode_file, TimeRange, TsfReader, WriterOptions},
        Row,
    },
};

const BASE_TS: i64 = 1_700_000_000_000;
const STEP_MS: i64 = 15_000;
const METRIC_ID: u64 = 1;
const BATCH_ROWS: usize = 8192;
const MERGESET_ITEMS: usize = 500_000;

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
    println!("# Phase 0 benchmark");
    println!();
    println!(
        "Dataset: {series} series x {samples} samples = {rows} rows, one metric, \
         {STEP_MS} ms scrape interval, {shape} values."
    );
    println!();

    let mut data = generate(series, samples, shape);
    let work = TempDir::new();

    let mut tsf_bytes = Vec::new();
    let tsf_stats = {
        let mut best = Duration::MAX;
        let mut stats = None;
        for _ in 0..3 {
            let start = Instant::now();
            let (bytes, s) = encode_file(
                data.clone(),
                (BASE_TS, BASE_TS + samples * STEP_MS),
                &WriterOptions::default(),
            )?;
            best = best.min(start.elapsed());
            tsf_bytes = bytes;
            stats = Some(s);
        }
        println!(
            "TSF encode: {:.3} s ({:.0} rows/s), {} bytes, {:.2} bytes/row",
            best.as_secs_f64(),
            rows as f64 / best.as_secs_f64(),
            tsf_bytes.len(),
            tsf_bytes.len() as f64 / rows as f64
        );
        stats.unwrap()
    };
    debug_assert_eq!(tsf_stats.rows, rows);

    let parquet_path = work.join("data.parquet");
    {
        let start = Instant::now();
        write_parquet(&parquet_path, &mut data)?;
        let elapsed = start.elapsed();
        let size = std::fs::metadata(&parquet_path).unwrap().len();
        println!(
            "Parquet write: {:.3} s ({:.0} rows/s), {} bytes, {:.2} bytes/row",
            elapsed.as_secs_f64(),
            rows as f64 / elapsed.as_secs_f64(),
            size,
            size as f64 / rows as f64
        );
    }
    println!();

    let reader = TsfReader::from_bytes(tsf_bytes)?;

    let tsf_scan = time_best(3, || reader.read_all().map(|v| v.len()))?;
    println!(
        "TSF full scan (scalar rows): {:.3} s ({:.0} rows/s), {:.2} ns/row",
        tsf_scan.0.as_secs_f64(),
        rows as f64 / tsf_scan.0.as_secs_f64(),
        tsf_scan.0.as_secs_f64() * 1e9 / rows as f64
    );
    let tsf_arrow_scan = time_best(3, || {
        let request = columnar_storage::tsf::TsfScanRequest::new(
            METRIC_ID,
            TimeRange::new(i64::MIN, i64::MAX),
        );
        let batches = columnar_storage::tsf::scan(&reader, &request)?;
        Ok(batches.iter().map(|b| b.num_rows()).sum::<usize>())
    })?;
    println!(
        "TSF full scan (Arrow emission): {:.3} s ({:.0} rows/s), {:.2} ns/row",
        tsf_arrow_scan.0.as_secs_f64(),
        rows as f64 / tsf_arrow_scan.0.as_secs_f64(),
        tsf_arrow_scan.0.as_secs_f64() * 1e9 / rows as f64
    );
    let parquet_scan = time_best(3, || read_parquet_all(&parquet_path))?;
    println!(
        "Parquet full scan (Arrow): {:.3} s ({:.0} rows/s), {:.2} ns/row",
        parquet_scan.0.as_secs_f64(),
        rows as f64 / parquet_scan.0.as_secs_f64(),
        parquet_scan.0.as_secs_f64() * 1e9 / rows as f64
    );
    assert_eq!(tsf_scan.1, parquet_scan.1);
    assert_eq!(tsf_arrow_scan.1, parquet_scan.1);
    println!();

    // 100 random series, 10% of the time range: representative dashboard query.
    let targets: Vec<u64> = (0..100).map(|i| (i * 7919 + 13) % series).collect();
    let window_start = BASE_TS + samples * STEP_MS * 45 / 100;
    let window_end = BASE_TS + samples * STEP_MS * 55 / 100;
    let range = TimeRange::new(window_start, window_end);
    let expected = 100 * ((window_end - window_start) / STEP_MS);

    let tsf_query = time_best(3, || {
        let mut n = 0;
        for &s in &targets {
            n += reader.read_series(METRIC_ID, s, range, None)?.len();
        }
        Ok(n)
    })?;
    println!(
        "TSF range query: 100 series, 10% window, {expected} rows: {:.3} ms ({:.0} rows/s)",
        tsf_query.0.as_secs_f64() * 1e3,
        tsf_query.1 as f64 / tsf_query.0.as_secs_f64()
    );
    let target_set: HashSet<u64> = targets.iter().copied().collect();
    let parquet_query = time_best(3, || {
        read_parquet_query(&parquet_path, &target_set, window_start, window_end)
    })?;
    println!(
        "Parquet range query (scan + filter): {:.3} ms ({:.0} rows/s)",
        parquet_query.0.as_secs_f64() * 1e3,
        parquet_query.1 as f64 / parquet_query.0.as_secs_f64()
    );
    assert_eq!(tsf_query.1, expected as usize);
    assert_eq!(parquet_query.1, expected as usize);
    println!();

    benchmark_mergeset(&work)?;
    Ok(())
}

fn generate(series: u64, samples: i64, shape: &str) -> Vec<Row> {
    let mut rows = Vec::with_capacity(series as usize * samples as usize);
    for s in 0..series {
        for i in 0..samples {
            let value = if shape == "counter" {
                // Integer-valued cumulative counter: the common IoT case where
                // nearest-delta on the f64 bit pattern pays off.
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

fn parquet_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("metric_id", DataType::UInt64, false),
        Field::new("series_id", DataType::UInt64, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("sequence", DataType::UInt64, false),
    ]))
}

fn write_parquet(path: &PathBuf, rows: &mut [Row]) -> Result<(), String> {
    rows.sort_unstable_by_key(|r| (r.metric_id, r.series_id, r.timestamp));
    let schema = parquet_schema();
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(1).unwrap()))
        .set_max_row_group_size(1_000_000)
        .build();
    let file = File::create(path).map_err(|e| format!("cannot create {path:?}: {e}"))?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
        .map_err(|e| format!("cannot create parquet writer: {e}"))?;
    for chunk in rows.chunks(BATCH_ROWS) {
        let batch = batch_from_rows(&schema, chunk)?;
        writer
            .write(&batch)
            .map_err(|e| format!("cannot write parquet batch: {e}"))?;
    }
    let file = writer
        .into_inner()
        .map_err(|e| format!("cannot finish parquet: {e}"))?;
    file.sync_all()
        .map_err(|e| format!("cannot fsync parquet: {e}"))?;
    Ok(())
}

fn batch_from_rows(schema: &Arc<Schema>, rows: &[Row]) -> Result<RecordBatch, String> {
    let metric = UInt64Array::from_iter_values(rows.iter().map(|r| r.metric_id));
    let series = UInt64Array::from_iter_values(rows.iter().map(|r| r.series_id));
    let ts = Int64Array::from_iter_values(rows.iter().map(|r| r.timestamp));
    let value = Float64Array::from_iter_values(rows.iter().map(|r| r.value));
    let seq = UInt64Array::from_iter_values(rows.iter().map(|r| r.sequence));
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(metric),
            Arc::new(series),
            Arc::new(ts),
            Arc::new(value),
            Arc::new(seq),
        ],
    )
    .map_err(|e| format!("cannot build record batch: {e}"))
}

fn parquet_reader(
    path: &PathBuf,
) -> Result<parquet::arrow::arrow_reader::ParquetRecordBatchReader, String> {
    let file = File::open(path).map_err(|e| format!("cannot open {path:?}: {e}"))?;
    ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| format!("cannot read parquet metadata: {e}"))?
        .build()
        .map_err(|e| format!("cannot build parquet reader: {e}"))
}

fn read_parquet_all(path: &PathBuf) -> Result<usize, String> {
    let reader = parquet_reader(path)?;
    let mut n = 0;
    for batch in reader {
        n += batch
            .map_err(|e| format!("cannot read parquet batch: {e}"))?
            .num_rows();
    }
    Ok(n)
}

fn read_parquet_query(
    path: &PathBuf,
    targets: &HashSet<u64>,
    start: i64,
    end: i64,
) -> Result<usize, String> {
    let reader = parquet_reader(path)?;
    let mut n = 0;
    for batch in reader {
        let batch = batch.map_err(|e| format!("cannot read parquet batch: {e}"))?;
        let series = downcast::<UInt64Array>(&batch, 1)?;
        let ts = downcast::<Int64Array>(&batch, 2)?;
        for i in 0..batch.num_rows() {
            if targets.contains(&series.value(i)) && ts.value(i) >= start && ts.value(i) < end {
                n += 1;
            }
        }
    }
    Ok(n)
}

fn downcast<T: Array + 'static>(batch: &RecordBatch, column: usize) -> Result<&T, String> {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| format!("column {column} has an unexpected type"))
}

fn time_best<F>(iters: usize, mut f: F) -> Result<(Duration, usize), String>
where
    F: FnMut() -> Result<usize, String>,
{
    let mut best = Duration::MAX;
    let mut out = 0;
    for _ in 0..iters {
        let start = Instant::now();
        out = f()?;
        best = best.min(start.elapsed());
    }
    Ok((best, out))
}

fn mergeset_key(series: u64, ts: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(16);
    key.extend_from_slice(&METRIC_ID.to_be_bytes());
    key.extend_from_slice(&series.to_be_bytes());
    key.extend_from_slice(&ts.to_be_bytes());
    key
}

fn benchmark_mergeset(work: &TempDir) -> Result<(), String> {
    println!("## Mergeset (LSM index)");
    println!();
    let mut table = Table::new();
    let mut total = 0usize;

    let start = Instant::now();
    for round in 0..3u64 {
        let mut keys: Vec<Vec<u8>> = (0..MERGESET_ITEMS as u64)
            .map(|i| mergeset_key(i % 20_000, BASE_TS + (i as i64) * STEP_MS + round as i64))
            .collect();
        keys.sort_unstable();
        for key in keys {
            table.add_owned(key);
        }
        table
            .flush(work.join(&format!("part-{round}")), 1)
            .map_err(|e| format!("mergeset flush failed: {e}"))?;
        total += MERGESET_ITEMS;
    }
    let ingest = start.elapsed();
    println!(
        "Ingest + flush: 3 x {} items in {:.3} s ({:.0} items/s), {} parts, {} bytes on disk",
        MERGESET_ITEMS,
        ingest.as_secs_f64(),
        total as f64 / ingest.as_secs_f64(),
        table.parts().len(),
        table.parts().iter().map(|p| p.size).sum::<u64>()
    );

    let queries: Vec<(Vec<u8>, Vec<u8>)> = (0..100u64)
        .map(|i| {
            let series = (i * 7919 + 3) % 20_000;
            let lo = mergeset_key(series, BASE_TS);
            let hi = mergeset_key(series + 1, BASE_TS);
            (lo, hi)
        })
        .collect();
    let start = Instant::now();
    let mut found = 0usize;
    for (lo, hi) in &queries {
        found += table.search_range(lo, hi).map_err(|e| e.to_string())?.len();
    }
    let search = start.elapsed();
    println!(
        "Range search: 100 queries, {found} items in {:.3} ms ({:.0} queries/s)",
        search.as_secs_f64() * 1e3,
        100.0 / search.as_secs_f64()
    );

    let start = Instant::now();
    table
        .merge_all(work.join("part-merged"), 1)
        .map_err(|e| format!("mergeset merge failed: {e}"))?;
    let merge = start.elapsed();
    let merged = table
        .search_range(&[0u8; 16], &[0xffu8; 16])
        .map_err(|e| e.to_string())?;
    assert_eq!(merged.len(), total, "merge lost or duplicated items");
    println!(
        "Full merge: {:.3} s ({:.0} items/s), {} bytes, {total} items verified",
        merge.as_secs_f64(),
        total as f64 / merge.as_secs_f64(),
        table.parts()[0].size
    );
    Ok(())
}

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("phase0-bench-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
