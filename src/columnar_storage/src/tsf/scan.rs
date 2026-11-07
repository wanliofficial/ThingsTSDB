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

//! `ScanRequest` -> TSF page reads -> Arrow batches.
//!
//! The decoded column vectors are handed to Arrow's `ScalarBuffer` without a
//! copy. Small series may be concatenated to fill a batch; the large-series
//! path keeps zero-copy buffers and slices them with `RecordBatch::slice`.

use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, UInt64Array},
    buffer::ScalarBuffer,
    compute::filter_record_batch,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use storage_kernel::tsf::{
    file::{scan_metric_parallel, DecodedColumns, ReadContext, ScanOptions, TimeRange, TsfReader},
    page::MAX_PAGE_ROWS,
    ColumnMask,
};

use crate::types::{TimeRange as StorageTimeRange, Timestamp};

/// The Arrow schema shared with the Parquet path (ADR-0002 §7.3).
///
/// Cached because scans build one batch per series and schema construction is
/// otherwise repeated thousands of times per scan.
pub fn tsf_schema() -> SchemaRef {
    static SCHEMA: std::sync::OnceLock<SchemaRef> = std::sync::OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("metric_id", DataType::UInt64, false),
                Field::new("tsid", DataType::UInt64, false),
                Field::new("timestamp", DataType::Int64, false),
                Field::new("value", DataType::Float64, false),
                Field::new("__seq__", DataType::UInt64, true),
                Field::new("__reserved__", DataType::UInt64, true),
            ]))
        })
        .clone()
}

/// Options for a TSF scan.
#[derive(Debug, Clone, Copy)]
pub struct TsfScanOptions {
    pub columns: ColumnMask,
    pub batch_rows: usize,
    pub workers: usize,
    pub cache_bytes_per_worker: usize,
}

impl Default for TsfScanOptions {
    fn default() -> Self {
        Self {
            columns: ColumnMask::ALL,
            batch_rows: MAX_PAGE_ROWS,
            workers: 0,
            cache_bytes_per_worker: storage_kernel::tsf::file::DEFAULT_CACHE_BYTES,
        }
    }
}

impl TsfScanOptions {
    fn scan_options(&self) -> ScanOptions {
        ScanOptions {
            workers: self.workers,
            cache_bytes_per_worker: self.cache_bytes_per_worker,
            batch_rows: self.batch_rows,
        }
    }
}

/// A typed TSF scan request.
///
/// This mirrors the `ScanRequest` extension in ADR-0002 §7.1 without pulling
/// DataFusion expressions into the storage kernel.
#[derive(Debug, Clone)]
pub struct TsfScanRequest {
    pub metric_id: u64,
    pub range: TimeRange,
    /// Sorted, deduplicated series IDs from the index. `None` scans all.
    pub series_ids: Option<Vec<u64>>,
    /// Optional `[start, end)` sequence predicate, applied with an Arrow
    /// filter because `__seq__` is not monotonic.
    pub sequence_range: Option<(u64, u64)>,
    pub options: TsfScanOptions,
}

impl TsfScanRequest {
    pub fn new(metric_id: u64, range: TimeRange) -> Self {
        Self {
            metric_id,
            range,
            series_ids: None,
            sequence_range: None,
            options: TsfScanOptions::default(),
        }
    }

    pub fn with_series(mut self, series_ids: Vec<u64>) -> Self {
        self.series_ids = Some(series_ids);
        self
    }

    pub fn with_sequence_range(mut self, range: (u64, u64)) -> Self {
        self.sequence_range = Some(range);
        self
    }

    pub fn with_options(mut self, options: TsfScanOptions) -> Self {
        self.options = options;
        self
    }
}

/// Converts one decoded series into a `RecordBatch` without copying the
/// timestamp and value buffers.
///
/// `timestamp` and `value` must be present. Missing `__seq__` /
/// `__reserved__` columns become all-null arrays; callers that need LWW must
/// request them.
pub fn decoded_columns_to_batch(columns: DecodedColumns) -> Result<RecordBatch, String> {
    if !columns.mask.contains(ColumnMask::TIMESTAMP) || !columns.mask.contains(ColumnMask::VALUE) {
        return Err("TSF Arrow emission requires timestamp and value columns".to_string());
    }
    let rows = columns.timestamps.len();
    if columns.values.len() != rows {
        return Err(format!(
            "column length mismatch: timestamp={rows}, value={}",
            columns.values.len()
        ));
    }
    if columns.mask.contains(ColumnMask::SEQUENCE) && columns.sequences.len() != rows {
        return Err("sequence column length mismatch".to_string());
    }
    if columns.mask.contains(ColumnMask::RESERVED) && columns.reserved.len() != rows {
        return Err("reserved column length mismatch".to_string());
    }

    let metric = Arc::new(UInt64Array::new(
        ScalarBuffer::from(vec![columns.metric_id; rows]),
        None,
    )) as ArrayRef;
    let series = Arc::new(UInt64Array::new(
        ScalarBuffer::from(vec![columns.series_id; rows]),
        None,
    )) as ArrayRef;
    let timestamps = Arc::new(Int64Array::new(
        ScalarBuffer::from(columns.timestamps),
        None,
    )) as ArrayRef;
    let values = Arc::new(Float64Array::new(ScalarBuffer::from(columns.values), None)) as ArrayRef;
    let sequences = if columns.mask.contains(ColumnMask::SEQUENCE) {
        Arc::new(UInt64Array::new(
            ScalarBuffer::from(columns.sequences),
            None,
        )) as ArrayRef
    } else {
        Arc::new(UInt64Array::new_null(rows)) as ArrayRef
    };
    let reserved = if columns.mask.contains(ColumnMask::RESERVED) {
        Arc::new(UInt64Array::new(ScalarBuffer::from(columns.reserved), None)) as ArrayRef
    } else {
        Arc::new(UInt64Array::new_null(rows)) as ArrayRef
    };

    RecordBatch::try_new(
        tsf_schema(),
        vec![metric, series, timestamps, values, sequences, reserved],
    )
    .map_err(|e| format!("cannot build TSF record batch: {e}"))
}

/// Applies a sequence range with Arrow's `filter` kernel.
pub fn filter_by_sequence(
    batch: &RecordBatch,
    range: Option<(u64, u64)>,
) -> Result<RecordBatch, String> {
    let Some((start, end)) = range else {
        return Ok(batch.clone());
    };
    let seq = batch
        .column(4)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| "TSF batch has no __seq__ column".to_string())?;
    if seq.null_count() != 0 {
        return Err("cannot apply a sequence predicate to null __seq__ values".to_string());
    }
    let mask = BooleanArray::from_iter((0..seq.len()).map(|i| {
        let value = seq.value(i);
        Some(value >= start && value < end)
    }));
    filter_record_batch(batch, &mask).map_err(|e| format!("cannot apply sequence filter: {e}"))
}

/// Accumulates decoded columns into batches of at most `batch_rows` rows.
pub struct BatchBuilder {
    batch_rows: usize,
    pending: Option<PendingColumns>,
    out: Vec<RecordBatch>,
}

/// Column-major accumulation buffer for one output batch.
///
/// Appending decoded series directly avoids the per-series `RecordBatch` plus
/// Arrow `concat_batches` round trip, which dominated full-scan emission.
struct PendingColumns {
    mask: ColumnMask,
    metric_ids: Vec<u64>,
    series_ids: Vec<u64>,
    timestamps: Vec<i64>,
    values: Vec<f64>,
    sequences: Vec<u64>,
    reserved: Vec<u64>,
}

impl PendingColumns {
    fn from_columns(columns: DecodedColumns) -> Self {
        let rows = columns.len();
        let mask = columns.mask;
        Self {
            mask,
            metric_ids: vec![columns.metric_id; rows],
            series_ids: vec![columns.series_id; rows],
            timestamps: columns.timestamps,
            values: columns.values,
            sequences: columns.sequences,
            reserved: columns.reserved,
        }
    }

    fn extend(&mut self, columns: DecodedColumns) {
        let rows = columns.len();
        self.metric_ids
            .extend(std::iter::repeat_n(columns.metric_id, rows));
        self.series_ids
            .extend(std::iter::repeat_n(columns.series_id, rows));
        self.timestamps.extend_from_slice(&columns.timestamps);
        self.values.extend_from_slice(&columns.values);
        if self.mask.contains(ColumnMask::SEQUENCE) {
            self.sequences.extend_from_slice(&columns.sequences);
        }
        if self.mask.contains(ColumnMask::RESERVED) {
            self.reserved.extend_from_slice(&columns.reserved);
        }
    }

    fn rows(&self) -> usize {
        self.timestamps.len()
    }

    fn into_batch(self) -> Result<RecordBatch, String> {
        let rows = self.timestamps.len();
        let metric =
            Arc::new(UInt64Array::new(ScalarBuffer::from(self.metric_ids), None)) as ArrayRef;
        let series =
            Arc::new(UInt64Array::new(ScalarBuffer::from(self.series_ids), None)) as ArrayRef;
        let timestamps =
            Arc::new(Int64Array::new(ScalarBuffer::from(self.timestamps), None)) as ArrayRef;
        let values = Arc::new(Float64Array::new(ScalarBuffer::from(self.values), None)) as ArrayRef;
        let sequences = if self.mask.contains(ColumnMask::SEQUENCE) {
            Arc::new(UInt64Array::new(ScalarBuffer::from(self.sequences), None)) as ArrayRef
        } else {
            Arc::new(UInt64Array::new_null(rows)) as ArrayRef
        };
        let reserved = if self.mask.contains(ColumnMask::RESERVED) {
            Arc::new(UInt64Array::new(ScalarBuffer::from(self.reserved), None)) as ArrayRef
        } else {
            Arc::new(UInt64Array::new_null(rows)) as ArrayRef
        };
        RecordBatch::try_new(
            tsf_schema(),
            vec![metric, series, timestamps, values, sequences, reserved],
        )
        .map_err(|e| format!("cannot build TSF record batch: {e}"))
    }
}

impl BatchBuilder {
    pub fn new(batch_rows: usize) -> Self {
        Self {
            batch_rows: batch_rows.max(1),
            pending: None,
            out: Vec::new(),
        }
    }

    pub fn push(&mut self, columns: DecodedColumns) -> Result<(), String> {
        let rows = columns.len();
        if rows == 0 {
            return Ok(());
        }
        if rows >= self.batch_rows {
            // Large series: build one zero-copy batch and slice it.
            self.flush_pending()?;
            let batch = decoded_columns_to_batch(columns)?;
            return self.emit_sliced(&batch);
        }
        let same_shape = self
            .pending
            .as_ref()
            .is_some_and(|pending| pending.mask == columns.mask);
        if !same_shape {
            self.flush_pending()?;
            self.pending = Some(PendingColumns::from_columns(columns));
            return Ok(());
        }
        // Flush before overflowing so a full batch is emitted intact instead of
        // being sliced into a full batch plus a tiny remainder.
        let pending_rows = self.pending.as_ref().map_or(0, PendingColumns::rows);
        if pending_rows + rows > self.batch_rows {
            self.flush_pending()?;
            self.pending = Some(PendingColumns::from_columns(columns));
            return Ok(());
        }
        if let Some(pending) = self.pending.as_mut() {
            pending.extend(columns);
        }
        Ok(())
    }

    /// Appends a pre-built batch, flushing any pending decoded columns first.
    pub fn push_batch(&mut self, batch: RecordBatch) -> Result<(), String> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.flush_pending()?;
        self.emit_sliced(&batch)
    }

    fn flush_pending(&mut self) -> Result<(), String> {
        let Some(pending) = self.pending.take() else {
            return Ok(());
        };
        if pending.rows() == 0 {
            return Ok(());
        }
        let batch = pending.into_batch()?;
        self.emit_sliced(&batch)
    }

    /// Emits zero-copy slices of at most `batch_rows` rows.
    fn emit_sliced(&mut self, batch: &RecordBatch) -> Result<(), String> {
        let mut offset = 0;
        while offset < batch.num_rows() {
            let len = self.batch_rows.min(batch.num_rows() - offset);
            self.out.push(batch.slice(offset, len));
            offset += len;
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<Vec<RecordBatch>, String> {
        self.flush_pending()?;
        Ok(self.out)
    }

    pub fn rows(&self) -> usize {
        self.out.iter().map(RecordBatch::num_rows).sum::<usize>()
            + self.pending.as_ref().map_or(0, PendingColumns::rows)
    }
}

/// Reads every series of one metric and emits Arrow batches.
pub fn scan_metric(
    reader: &TsfReader,
    metric_id: u64,
    range: TimeRange,
    options: &TsfScanOptions,
) -> Result<Vec<RecordBatch>, String> {
    let result = scan_metric_parallel(
        reader,
        metric_id,
        range,
        options.columns,
        &options.scan_options(),
    )?;
    let mut builder = BatchBuilder::new(options.batch_rows);
    for columns in result.columns {
        builder.push(columns)?;
    }
    builder.finish()
}

/// Reads an explicit series set, preserving `series_ids` order and skipping
/// IDs that are absent from the file.
pub fn scan_series(
    reader: &TsfReader,
    metric_id: u64,
    series_ids: &[u64],
    range: TimeRange,
    options: &TsfScanOptions,
) -> Result<Vec<RecordBatch>, String> {
    let mut ids = series_ids.to_vec();
    ids.sort_unstable();
    ids.dedup();
    let mut builder = BatchBuilder::new(options.batch_rows);
    if options.workers <= 1 {
        let mut ctx = ReadContext::with_cache_bytes(options.cache_bytes_per_worker);
        for id in ids {
            let columns =
                reader.read_series_columns(metric_id, id, range, options.columns, &mut ctx)?;
            if !columns.is_empty() {
                builder.push(columns)?;
            }
        }
    } else {
        // Split the requested IDs across workers; each worker owns a context.
        let workers = options.workers.min(ids.len()).max(1);
        let chunk_size = ids.len().div_ceil(workers);
        let mut per_worker = Vec::new();
        std::thread::scope(|scope| -> Result<(), String> {
            let handles: Vec<_> = ids
                .chunks(chunk_size)
                .map(|chunk| {
                    let columns = options.columns;
                    let cache_bytes = options.cache_bytes_per_worker;
                    scope.spawn(move || -> Result<Vec<DecodedColumns>, String> {
                        let mut ctx = ReadContext::with_cache_bytes(cache_bytes);
                        let mut out = Vec::new();
                        for &id in chunk {
                            let decoded = reader
                                .read_series_columns(metric_id, id, range, columns, &mut ctx)?;
                            if !decoded.is_empty() {
                                out.push(decoded);
                            }
                        }
                        Ok(out)
                    })
                })
                .collect();
            for handle in handles {
                per_worker.push(
                    handle
                        .join()
                        .map_err(|_| "TSF scan worker panicked".to_string())??,
                );
            }
            Ok(())
        })?;
        for mut columns in per_worker {
            for decoded in columns.drain(..) {
                builder.push(decoded)?;
            }
        }
    }
    builder.finish()
}

/// Executes a [`TsfScanRequest`] and applies its sequence predicate with
/// Arrow's filter kernel.
pub fn scan(reader: &TsfReader, request: &TsfScanRequest) -> Result<Vec<RecordBatch>, String> {
    let batches = match &request.series_ids {
        Some(ids) => scan_series(
            reader,
            request.metric_id,
            ids,
            request.range,
            &request.options,
        )?,
        None => scan_metric(reader, request.metric_id, request.range, &request.options)?,
    };
    if request.sequence_range.is_none() {
        return Ok(batches);
    }
    let mut out = Vec::with_capacity(batches.len());
    for batch in batches {
        let filtered = filter_by_sequence(&batch, request.sequence_range)?;
        if filtered.num_rows() > 0 {
            out.push(filtered);
        }
    }
    Ok(out)
}

/// Converts the storage layer's time range into the kernel range.
pub fn kernel_time_range(range: &StorageTimeRange) -> TimeRange {
    TimeRange::new(range.start.0, range.end.0)
}

impl From<StorageTimeRange> for TimeRange {
    fn from(value: StorageTimeRange) -> Self {
        Self::new(value.start.0, value.end.0)
    }
}

impl From<Timestamp> for TimeRange {
    fn from(value: Timestamp) -> Self {
        Self::new(value.0, value.0)
    }
}

#[cfg(test)]
mod tests {
    use storage_kernel::tsf::{
        file::{encode_file, WriterOptions},
        Row,
    };

    use super::*;

    const METRIC: u64 = 7;

    fn row(series_id: u64, timestamp: i64, value: f64, sequence: u64) -> Row {
        Row {
            metric_id: METRIC,
            series_id,
            timestamp,
            value,
            sequence,
            reserved: 0,
        }
    }

    fn test_reader() -> TsfReader {
        let mut rows = Vec::new();
        for series in 0..6u64 {
            for i in 0..200i64 {
                let value = if i == 17 && series == 2 {
                    f64::NAN
                } else {
                    series as f64 * 1000.0 + i as f64
                };
                let timestamp = 1_700_000_000_000 + i * 10;
                rows.push(row(series, timestamp, value, i as u64));
                // Duplicate timestamp; the encoder keeps the highest seq.
                if i == 50 {
                    rows.push(row(series, timestamp, value + 0.5, 1000 + i as u64));
                }
            }
        }
        let (bytes, _) = encode_file(
            rows,
            (0, 2_000_000_000_000),
            &WriterOptions { page_rows: 64 },
        )
        .unwrap();
        TsfReader::from_bytes(bytes).unwrap()
    }

    #[test]
    fn test_arrow_emission_is_zero_copy_and_preserves_bits() {
        let reader = test_reader();
        let range = TimeRange::new(1_700_000_000_000, 1_700_000_002_000);
        let mut ctx = ReadContext::new();
        let columns = reader
            .read_series_columns(METRIC, 2, range, ColumnMask::ALL, &mut ctx)
            .unwrap();
        let expected_timestamps = columns.timestamps.clone();
        let expected_values: Vec<u64> = columns.values.iter().map(|v| v.to_bits()).collect();
        let ts_ptr = columns.timestamps.as_ptr();

        let batch = decoded_columns_to_batch(columns).unwrap();
        assert_eq!(batch.num_rows(), expected_timestamps.len());
        let ts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts.values().as_ptr(), ts_ptr, "timestamp buffer was copied");
        assert_eq!(
            (0..batch.num_rows())
                .map(|i| ts.value(i))
                .collect::<Vec<_>>(),
            expected_timestamps
        );
        let values = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(
            (0..batch.num_rows())
                .map(|i| values.value(i).to_bits())
                .collect::<Vec<_>>(),
            expected_values
        );
        assert!(values.iter().any(|v| v.is_some_and(f64::is_nan)));
        let metric = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert!(metric.iter().all(|v| v == Some(METRIC)));
    }

    #[test]
    fn test_batch_builder_respects_batch_rows_and_row_count() {
        let reader = test_reader();
        let range = TimeRange::new(1_700_000_000_000, 1_700_000_001_000);
        let mut ctx = ReadContext::new();
        let columns = reader
            .read_metric_columns(METRIC, range, ColumnMask::ALL, &mut ctx)
            .unwrap();
        let expected: usize = columns.iter().map(DecodedColumns::len).sum();
        let mut builder = BatchBuilder::new(128);
        for columns in columns {
            builder.push(columns).unwrap();
        }
        let batches = builder.finish().unwrap();
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            expected
        );
        assert!(batches.iter().all(|b| b.num_rows() <= 128));
        assert!(batches.len() > 1);
    }

    #[test]
    fn test_scan_metric_matches_scalar_rows() {
        let reader = test_reader();
        let range = TimeRange::new(1_700_000_000_500, 1_700_000_001_500);
        let batches = scan_metric(&reader, METRIC, range, &TsfScanOptions::default()).unwrap();
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        let scalar: usize = reader.read_metric(METRIC, range, None).unwrap().len();
        assert_eq!(rows, scalar);

        // Every emitted row must match the scalar path, including duplicates
        // and NaN values.
        let mut emitted = Vec::new();
        for batch in &batches {
            let ts = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let value = batch
                .column(3)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let seq = batch
                .column(4)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                emitted.push((ts.value(i), value.value(i).to_bits(), seq.value(i)));
            }
        }
        let mut expected: Vec<_> = reader
            .read_metric(METRIC, range, None)
            .unwrap()
            .into_iter()
            .map(|r| (r.timestamp, r.value.to_bits(), r.sequence))
            .collect();
        emitted.sort_unstable();
        expected.sort_unstable();
        assert_eq!(emitted, expected);
    }

    #[test]
    fn test_scan_series_and_sequence_filter() {
        let reader = test_reader();
        let range = TimeRange::new(1_700_000_000_000, 1_700_000_002_000);
        let options = TsfScanOptions {
            columns: ColumnMask::ALL,
            batch_rows: 64,
            workers: 3,
            cache_bytes_per_worker: 1 << 20,
        };
        let batches = scan_series(&reader, METRIC, &[4, 1, 3], range, &options).unwrap();
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 3 * 200);

        let request = TsfScanRequest::new(METRIC, range)
            .with_series(vec![1, 2])
            .with_sequence_range((40, 60))
            .with_options(options);
        let batches = scan(&reader, &request).unwrap();
        let mut count = 0;
        for batch in &batches {
            let seq = batch
                .column(4)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                let v = seq.value(i);
                assert!((40..60).contains(&v));
                count += 1;
            }
        }
        // Derive the expectation from the scalar oracle rather than assuming a
        // dense sequence range; the encoder dedups by (metric, series, ts).
        let expected: usize = [1u64, 2u64]
            .iter()
            .map(|&series| {
                reader
                    .read_series(METRIC, series, range, Some((40, 60)))
                    .unwrap()
                    .len()
            })
            .sum();
        assert_eq!(count, expected);
        assert!(expected > 0);
    }

    #[test]
    fn test_sequence_filter_rejects_null_sequence() {
        let reader = test_reader();
        let range = TimeRange::new(1_700_000_000_000, 1_700_000_000_100);
        let columns = reader
            .read_series_columns(
                METRIC,
                0,
                range,
                ColumnMask::TIMESTAMP | ColumnMask::VALUE,
                &mut ReadContext::new(),
            )
            .unwrap();
        let batch = decoded_columns_to_batch(columns).unwrap();
        assert!(filter_by_sequence(&batch, Some((0, 10))).is_err());
    }
}
