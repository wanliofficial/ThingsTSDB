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

//! Per-format I/O counters (ADR-0002 §13).
//!
//! While both formats can serve a query, a single "bytes read" number hides the
//! thing operators need to see during migration: how much traffic still goes to
//! Parquet, and how a format switch changes the write and read volume. Every
//! counter here is split by [`FileFormat`].
//!
//! Counting points:
//!
//! - writes are recorded where a file is committed, from the `FileMeta` the
//!   manifest receives, so the byte and row counts are the real file content
//!   rather than an estimate;
//! - reads for TSF are recorded by the execution plan, which already knows
//!   exactly which byte ranges it fetched
//!   (`test_ranged_read_fetches_only_touched_pages` relies on the same
//!   accounting);
//! - reads for Parquet go through [`CountingStore`], a thin `ObjectStore`
//!   decorator, because `ParquetExec` owns the reader and reports nothing
//!   upward.
//!
//! `files_read` counts *file selections*: one per file per scan, for both
//! formats. Both readers open each selected file at most once, so this is the
//! file count of a scan without hiding the pruning decisions behind an
//! object-store request count.
//!
//! Two more groups answer the operational questions the per-format counters
//! cannot:
//!
//! - [`FlushStats`] reports how long a flush takes and how many rows it moves,
//!   which is where write latency comes from;
//! - [`CompactionStats`] reports compaction input and output volume, so write
//!   amplification (output bytes / input bytes) is visible.
//!
//! The codec, bloom and index-cache counters are process-wide and live in the
//! kernel ([`storage_kernel::tsf::codec_stats`]); [`StorageMetrics::snapshot`]
//! folds them into the same report.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult,
};

use crate::{sst::FileFormat, types::ObjectStoreRef};

/// Counters for one data-file format.
///
/// Shared by reference behind an `Arc<StorageMetrics>`, so every increment is
/// relaxed: these are advisory counters, not a synchronization mechanism.
#[derive(Debug, Default)]
pub struct FormatCounters {
    files_written: AtomicU64,
    bytes_written: AtomicU64,
    rows_written: AtomicU64,
    files_read: AtomicU64,
    bytes_read: AtomicU64,
}

impl FormatCounters {
    /// Records `files` committed files totalling `bytes` and holding `rows`.
    pub fn record_write(&self, files: u64, bytes: u64, rows: u64) {
        self.files_written.fetch_add(files, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
        self.rows_written.fetch_add(rows, Ordering::Relaxed);
    }

    /// Records that one more file was selected by a scan.
    pub fn record_file_read(&self) {
        self.record_file_read_many(1);
    }

    /// Records `files` file selections at once.
    pub fn record_file_read_many(&self, files: u64) {
        self.files_read.fetch_add(files, Ordering::Relaxed);
    }

    /// Records bytes fetched from the object store by a scan.
    pub fn record_bytes_read(&self, bytes: u64) {
        self.bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> FormatStats {
        FormatStats {
            files_written: self.files_written.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            rows_written: self.rows_written.load(Ordering::Relaxed),
            files_read: self.files_read.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
        }
    }
}

/// A point-in-time copy of one format's counters.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FormatStats {
    pub files_written: u64,
    pub bytes_written: u64,
    pub rows_written: u64,
    pub files_read: u64,
    pub bytes_read: u64,
}

impl FormatStats {
    pub fn merge(&mut self, other: &FormatStats) {
        self.files_written += other.files_written;
        self.bytes_written += other.bytes_written;
        self.rows_written += other.rows_written;
        self.files_read += other.files_read;
        self.bytes_read += other.bytes_read;
    }
}

/// Flush latency and volume counters.
#[derive(Debug, Default)]
pub struct FlushCounters {
    flushes: AtomicU64,
    rows: AtomicU64,
    total_millis: AtomicU64,
    max_millis: AtomicU64,
}

impl FlushCounters {
    /// Records one successful flush of `rows` buffered rows.
    pub fn record(&self, rows: u64, elapsed: Duration) {
        let millis = elapsed.as_millis() as u64;
        self.flushes.fetch_add(1, Ordering::Relaxed);
        self.rows.fetch_add(rows, Ordering::Relaxed);
        self.total_millis.fetch_add(millis, Ordering::Relaxed);
        self.max_millis.fetch_max(millis, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> FlushStats {
        FlushStats {
            flushes: self.flushes.load(Ordering::Relaxed),
            rows: self.rows.load(Ordering::Relaxed),
            total_millis: self.total_millis.load(Ordering::Relaxed),
            max_millis: self.max_millis.load(Ordering::Relaxed),
        }
    }
}

/// A point-in-time copy of the flush counters.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FlushStats {
    /// Successful flushes (a flush with no buffered rows is not counted).
    pub flushes: u64,
    /// Rows those flushes persisted.
    pub rows: u64,
    /// Wall time spent inside flushes, in milliseconds.
    pub total_millis: u64,
    /// Slowest single flush, in milliseconds.
    pub max_millis: u64,
}

impl FlushStats {
    pub fn average_millis(&self) -> f64 {
        if self.flushes == 0 {
            0.0
        } else {
            self.total_millis as f64 / self.flushes as f64
        }
    }
}

/// Compaction volume counters.
#[derive(Debug, Default)]
pub struct CompactionCounters {
    tasks: AtomicU64,
    input_files: AtomicU64,
    input_bytes: AtomicU64,
    output_files: AtomicU64,
    output_bytes: AtomicU64,
    expired_files: AtomicU64,
    expired_bytes: AtomicU64,
}

impl CompactionCounters {
    /// Records one completed compaction: the files and bytes it merged and the
    /// files and bytes it wrote in their place.
    pub fn record(&self, inputs: u64, input_bytes: u64, outputs: u64, output_bytes: u64) {
        self.tasks.fetch_add(1, Ordering::Relaxed);
        self.input_files.fetch_add(inputs, Ordering::Relaxed);
        self.input_bytes.fetch_add(input_bytes, Ordering::Relaxed);
        self.output_files.fetch_add(outputs, Ordering::Relaxed);
        self.output_bytes.fetch_add(output_bytes, Ordering::Relaxed);
    }

    /// Records files removed for TTL rather than merged.
    pub fn record_expired(&self, files: u64, bytes: u64) {
        self.expired_files.fetch_add(files, Ordering::Relaxed);
        self.expired_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> CompactionStats {
        CompactionStats {
            tasks: self.tasks.load(Ordering::Relaxed),
            input_files: self.input_files.load(Ordering::Relaxed),
            input_bytes: self.input_bytes.load(Ordering::Relaxed),
            output_files: self.output_files.load(Ordering::Relaxed),
            output_bytes: self.output_bytes.load(Ordering::Relaxed),
            expired_files: self.expired_files.load(Ordering::Relaxed),
            expired_bytes: self.expired_bytes.load(Ordering::Relaxed),
        }
    }
}

/// A point-in-time copy of the compaction counters.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CompactionStats {
    /// Compaction tasks that committed their output.
    pub tasks: u64,
    pub input_files: u64,
    pub input_bytes: u64,
    pub output_files: u64,
    pub output_bytes: u64,
    /// Files dropped by TTL instead of being merged.
    pub expired_files: u64,
    pub expired_bytes: u64,
}

impl CompactionStats {
    /// Output bytes per input byte. Above 1.0 means compaction is writing more
    /// than it read, which is the write-amplification cost of the policy.
    pub fn write_amplification(&self) -> f64 {
        if self.input_bytes == 0 {
            0.0
        } else {
            self.output_bytes as f64 / self.input_bytes as f64
        }
    }
}

/// Both formats' counters, as reported to the operator.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StorageStats {
    pub parquet: FormatStats,
    pub tsf: FormatStats,
    pub flush: FlushStats,
    pub compaction: CompactionStats,
    /// Process-wide codec, bloom and index-cache counters from the kernel.
    /// They cover every TSF file this process has read or written, not just
    /// this storage instance.
    pub tsf_kernel: storage_kernel::tsf::CodecStats,
}

impl StorageStats {
    /// Sum across formats, for a "total I/O" dashboard.
    pub fn total(&self) -> FormatStats {
        let mut total = self.parquet;
        total.merge(&self.tsf);
        total
    }

    pub fn for_format(&self, format: FileFormat) -> FormatStats {
        match format {
            FileFormat::Parquet => self.parquet,
            FileFormat::Tsf => self.tsf,
        }
    }
}

/// The live counters shared by the flush path, both readers and compaction.
#[derive(Debug, Default)]
pub struct StorageMetrics {
    parquet: FormatCounters,
    tsf: FormatCounters,
    flush: FlushCounters,
    compaction: CompactionCounters,
}

impl StorageMetrics {
    /// Records one successful flush.
    pub fn record_flush(&self, rows: u64, elapsed: Duration) {
        self.flush.record(rows, elapsed);
    }

    /// Records one committed compaction task.
    pub fn record_compaction(
        &self,
        inputs: u64,
        input_bytes: u64,
        outputs: u64,
        output_bytes: u64,
    ) {
        self.compaction
            .record(inputs, input_bytes, outputs, output_bytes);
    }

    /// Records files removed by TTL.
    pub fn record_expired(&self, files: u64, bytes: u64) {
        self.compaction.record_expired(files, bytes);
    }

    /// Counters for `format`; use it to record a write or a file read.
    pub fn for_format(&self, format: FileFormat) -> &FormatCounters {
        match format {
            FileFormat::Parquet => &self.parquet,
            FileFormat::Tsf => &self.tsf,
        }
    }

    pub fn snapshot(&self) -> StorageStats {
        StorageStats {
            parquet: self.parquet.snapshot(),
            tsf: self.tsf.snapshot(),
            flush: self.flush.snapshot(),
            compaction: self.compaction.snapshot(),
            tsf_kernel: storage_kernel::tsf::codec_stats(),
        }
    }
}

/// An `ObjectStore` that adds the bytes it serves to one format's read counter.
///
/// Only the Parquet reader is wrapped: `ParquetExec` owns the reader and drops
/// its own metrics, so the store is the only place that sees the traffic. The
/// TSF plan counts its own fetches instead, which keeps the coalescing and
/// range arithmetic in one place.
#[derive(Debug)]
pub struct CountingStore {
    inner: ObjectStoreRef,
    metrics: Arc<StorageMetrics>,
    format: FileFormat,
}

impl CountingStore {
    pub fn new(inner: ObjectStoreRef, metrics: Arc<StorageMetrics>, format: FileFormat) -> Self {
        Self {
            inner,
            metrics,
            format,
        }
    }

    fn record(&self, bytes: u64) {
        if bytes > 0 {
            self.metrics
                .for_format(self.format)
                .record_bytes_read(bytes);
        }
    }
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore({}, {:?})", self.inner, self.format)
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        // `head` requests carry no bytes; the Parquet reader issues one per
        // file to learn the size, and counting it would inflate the read
        // volume.
        let head = options.head;
        let result = self.inner.get_opts(location, options).await?;
        if !head {
            self.record(result.range.len() as u64);
        }
        Ok(result)
    }

    async fn get_range(
        &self,
        location: &Path,
        range: std::ops::Range<usize>,
    ) -> object_store::Result<bytes::Bytes> {
        // `get_ranges` defaults to a loop over `get_range`, so overriding this
        // one covers both paths.
        let requested = range.len() as u64;
        let bytes = self.inner.get_range(location, range).await?;
        self.record(requested.min(bytes.len() as u64));
        Ok(bytes)
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'_, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;
    use object_store::{local::LocalFileSystem, ObjectStore};

    use super::*;

    fn metrics() -> Arc<StorageMetrics> {
        Arc::new(StorageMetrics::default())
    }

    #[test]
    fn test_counters_are_split_by_format() {
        let stats = metrics();
        stats.for_format(FileFormat::Tsf).record_write(2, 100, 10);
        stats.for_format(FileFormat::Parquet).record_write(1, 50, 5);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.tsf.files_written, 2);
        assert_eq!(snapshot.tsf.bytes_written, 100);
        assert_eq!(snapshot.tsf.rows_written, 10);
        assert_eq!(snapshot.parquet.files_written, 1);
        assert_eq!(snapshot.parquet.bytes_written, 50);
        assert_eq!(snapshot.total().files_written, 3);
        assert_eq!(snapshot.for_format(FileFormat::Tsf).rows_written, 10);
        assert_eq!(snapshot.for_format(FileFormat::Parquet).rows_written, 5);
    }

    /// The decorator must count served bytes, skip `head` and leave every other
    /// operation untouched.
    #[test]
    fn test_counting_store_counts_reads_but_not_heads() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let root = temp_dir::TempDir::new().unwrap();
            let inner: ObjectStoreRef = Arc::new(LocalFileSystem::new());
            let store = CountingStore::new(inner.clone(), metrics(), FileFormat::Parquet);
            let path = Path::from(format!("{}/data/1.sst", root.path().to_string_lossy()));
            inner.put(&path, vec![7u8; 100].into()).await.unwrap();

            // A head must not add bytes.
            let meta = store.head(&path).await.unwrap();
            assert_eq!(meta.size, 100);
            assert_eq!(store.metrics.snapshot().parquet.bytes_read, 0);

            store.get_range(&path, 10..20).await.unwrap();
            assert_eq!(store.metrics.snapshot().parquet.bytes_read, 10);

            // `get_ranges` must be accounted too, through the `get_range`
            // default implementation. That default coalesces nearby ranges
            // (up to 1 MiB), so the counter follows the bytes actually fetched
            // from the inner store rather than the bytes the caller asked for:
            // here 0..5 and 20..25 become one 0..25 request.
            let ranges = store.get_ranges(&path, &[0..5, 20..25]).await.unwrap();
            assert_eq!(ranges.iter().map(|b| b.len()).sum::<usize>(), 10);
            assert_eq!(store.metrics.snapshot().parquet.bytes_read, 10 + 25);

            // Reads land in this store's format only.
            assert_eq!(store.metrics.snapshot().tsf.bytes_read, 0);

            // Listing still works through the decorator.
            let listed = store
                .list(Some(&Path::from(format!(
                    "{}/data",
                    root.path().to_string_lossy()
                ))))
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(listed.len(), 1);
        });
    }

    /// Flush and compaction counters are independent of the per-format ones:
    /// a flush moves rows, a compaction rewrites bytes.
    #[test]
    fn test_flush_and_compaction_counters() {
        let stats = metrics();
        stats.record_flush(1_000, Duration::from_millis(30));
        stats.record_flush(2_000, Duration::from_millis(90));

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.flush.flushes, 2);
        assert_eq!(snapshot.flush.rows, 3_000);
        assert_eq!(snapshot.flush.total_millis, 120);
        assert_eq!(snapshot.flush.max_millis, 90);
        assert_eq!(snapshot.flush.average_millis(), 60.0);
        // Flushes say nothing about file I/O; that is still the manifest's job.
        assert_eq!(snapshot.total().files_written, 0);

        stats.record_compaction(4, 1_000, 1, 600);
        stats.record_compaction(2, 400, 1, 500);
        stats.record_expired(3, 300);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.compaction.tasks, 2);
        assert_eq!(snapshot.compaction.input_files, 6);
        assert_eq!(snapshot.compaction.input_bytes, 1_400);
        assert_eq!(snapshot.compaction.output_files, 2);
        assert_eq!(snapshot.compaction.output_bytes, 1_100);
        assert_eq!(snapshot.compaction.expired_files, 3);
        assert_eq!(snapshot.compaction.expired_bytes, 300);
        assert!((snapshot.compaction.write_amplification() - 1_100.0 / 1_400.0).abs() < 1e-9);
        assert_eq!(CompactionStats::default().write_amplification(), 0.0);
        assert_eq!(FlushStats::default().average_millis(), 0.0);
    }

    /// The snapshot also carries the kernel's process-wide counters, so an
    /// operator sees codec choices next to the storage's own numbers.
    #[test]
    fn test_snapshot_includes_kernel_counters() {
        let before = storage_kernel::tsf::codec_stats();
        let page = storage_kernel::tsf::page::encode_page(&[storage_kernel::tsf::Row {
            metric_id: 1,
            series_id: 1,
            timestamp: 1,
            value: 1.5,
            sequence: 1,
            reserved: 0,
        }])
        .unwrap();
        assert!(!page.0.is_empty());
        let snapshot = metrics().snapshot();
        assert!(snapshot.tsf_kernel.pages > before.pages);
        assert_eq!(
            snapshot.tsf_kernel.pages - before.pages,
            1,
            "one page encoded"
        );
    }
}
