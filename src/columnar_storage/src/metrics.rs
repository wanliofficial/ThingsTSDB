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
//! - [`FlushStats`] reports how long a flush takes, its sort/write/commit/WAL
//!   stage split, how long acknowledged writes waited in the WAL, and how many
//!   rows it moves, which is where write latency comes from;
//! - [`CompactionStats`] reports compaction input and output volume and rows,
//!   so write amplification (output bytes / input bytes) and deduplicated rows
//!   are visible;
//! - [`ManifestStats`] reports delta-commit bytes and latency, the shared cost
//!   of every flush, compaction and rewrite.
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
    sort_millis: AtomicU64,
    write_millis: AtomicU64,
    encode_millis: AtomicU64,
    upload_millis: AtomicU64,
    commit_millis: AtomicU64,
    wal_millis: AtomicU64,
    wal_lag_total_millis: AtomicU64,
    wal_lag_max_millis: AtomicU64,
}

/// Where the time inside one flush went. The stages are disjoint and `write`
/// no longer includes the manifest commit of the produced files.
///
/// `encode` and `upload` are a breakdown of `write`, not extra stages: they are
/// always within it and `write - encode - upload` is Arrow conversion and other
/// per-segment bookkeeping. The Parquet writer interleaves page encoding and
/// the object PUT in its streaming writer, so it reports both as zero.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FlushStages {
    /// Merging and sorting the buffered batches of every segment.
    pub sort: Duration,
    /// Encoding and uploading the segment files.
    pub write: Duration,
    /// CPU part of `write`: Arrow conversion, page encoding, header read-back.
    pub encode: Duration,
    /// Object-store PUT part of `write`.
    pub upload: Duration,
    /// Committing the produced files to the manifest.
    pub commit: Duration,
    /// Removing the WAL files that the flush made durable.
    pub wal: Duration,
}

impl FlushStages {
    /// Total time covered by the stages.
    pub fn total(&self) -> Duration {
        self.sort + self.write + self.commit + self.wal
    }
}

impl FlushCounters {
    /// Records one successful flush of `rows` buffered rows. `wal_lag` is the
    /// age of the oldest buffered frame when its WAL file is removed.
    pub fn record(&self, rows: u64, elapsed: Duration, stages: FlushStages, wal_lag: Duration) {
        let millis = elapsed.as_millis() as u64;
        let lag_millis = wal_lag.as_millis() as u64;
        self.flushes.fetch_add(1, Ordering::Relaxed);
        self.rows.fetch_add(rows, Ordering::Relaxed);
        self.total_millis.fetch_add(millis, Ordering::Relaxed);
        self.max_millis.fetch_max(millis, Ordering::Relaxed);
        self.sort_millis
            .fetch_add(stages.sort.as_millis() as u64, Ordering::Relaxed);
        self.write_millis
            .fetch_add(stages.write.as_millis() as u64, Ordering::Relaxed);
        self.encode_millis
            .fetch_add(stages.encode.as_millis() as u64, Ordering::Relaxed);
        self.upload_millis
            .fetch_add(stages.upload.as_millis() as u64, Ordering::Relaxed);
        self.commit_millis
            .fetch_add(stages.commit.as_millis() as u64, Ordering::Relaxed);
        self.wal_millis
            .fetch_add(stages.wal.as_millis() as u64, Ordering::Relaxed);
        self.wal_lag_total_millis
            .fetch_add(lag_millis, Ordering::Relaxed);
        self.wal_lag_max_millis
            .fetch_max(lag_millis, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> FlushStats {
        FlushStats {
            flushes: self.flushes.load(Ordering::Relaxed),
            rows: self.rows.load(Ordering::Relaxed),
            total_millis: self.total_millis.load(Ordering::Relaxed),
            max_millis: self.max_millis.load(Ordering::Relaxed),
            sort_millis: self.sort_millis.load(Ordering::Relaxed),
            write_millis: self.write_millis.load(Ordering::Relaxed),
            encode_millis: self.encode_millis.load(Ordering::Relaxed),
            upload_millis: self.upload_millis.load(Ordering::Relaxed),
            commit_millis: self.commit_millis.load(Ordering::Relaxed),
            wal_millis: self.wal_millis.load(Ordering::Relaxed),
            wal_lag_total_millis: self.wal_lag_total_millis.load(Ordering::Relaxed),
            wal_lag_max_millis: self.wal_lag_max_millis.load(Ordering::Relaxed),
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
    /// Time spent sorting buffered batches, summed over all flushes.
    pub sort_millis: u64,
    /// Time spent encoding and uploading files.
    pub write_millis: u64,
    /// CPU part of `write_millis`: Arrow conversion, page encoding and header
    /// read-back. Always zero for the Parquet writer (see [`FlushStages`]).
    pub encode_millis: u64,
    /// Object-store PUT part of `write_millis`. Always zero for Parquet.
    pub upload_millis: u64,
    /// Time spent committing files to the manifest.
    pub commit_millis: u64,
    /// Time spent removing the flushed WAL files.
    pub wal_millis: u64,
    /// Age of the oldest buffered frame when its WAL file was removed, summed
    /// over all flushes.
    pub wal_lag_total_millis: u64,
    /// Oldest single-frame WAL lag seen, in milliseconds.
    pub wal_lag_max_millis: u64,
}

impl FlushStats {
    pub fn average_millis(&self) -> f64 {
        if self.flushes == 0 {
            0.0
        } else {
            self.total_millis as f64 / self.flushes as f64
        }
    }

    /// Mean age of the oldest buffered frame at WAL truncation: how long a
    /// write waited between being acknowledged and leaving the WAL.
    pub fn average_wal_lag_millis(&self) -> f64 {
        if self.flushes == 0 {
            0.0
        } else {
            self.wal_lag_total_millis as f64 / self.flushes as f64
        }
    }

    /// Fraction of the stage time spent in each stage (`sort`, `write`,
    /// `commit`, `wal`). The stages sum to approximately `total_millis`; the
    /// difference is the time between them (lock acquisition, draining the
    /// buffer).
    pub fn stage_ratio(&self) -> Option<(f64, f64, f64, f64)> {
        let total = self.sort_millis + self.write_millis + self.commit_millis + self.wal_millis;
        if total == 0 {
            None
        } else {
            let total = total as f64;
            Some((
                self.sort_millis as f64 / total,
                self.write_millis as f64 / total,
                self.commit_millis as f64 / total,
                self.wal_millis as f64 / total,
            ))
        }
    }

    /// Splits `write_millis` into its encode and upload parts. Returns `None`
    /// when the format reports no split (Parquet) or nothing was written.
    pub fn write_split(&self) -> Option<(f64, f64)> {
        if self.write_millis == 0 || self.encode_millis + self.upload_millis == 0 {
            return None;
        }
        let total = self.write_millis as f64;
        Some((
            self.encode_millis as f64 / total,
            self.upload_millis as f64 / total,
        ))
    }
}

/// One completed compaction, as reported to the counters.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CompactionVolume {
    pub input_files: u64,
    pub input_bytes: u64,
    pub input_rows: u64,
    pub output_files: u64,
    pub output_bytes: u64,
    pub output_rows: u64,
}

/// Compaction volume counters.
#[derive(Debug, Default)]
pub struct CompactionCounters {
    tasks: AtomicU64,
    input_files: AtomicU64,
    input_bytes: AtomicU64,
    input_rows: AtomicU64,
    output_files: AtomicU64,
    output_bytes: AtomicU64,
    output_rows: AtomicU64,
    expired_files: AtomicU64,
    expired_bytes: AtomicU64,
}

impl CompactionCounters {
    /// Records one completed compaction: the files, bytes and rows it merged
    /// and the files, bytes and rows it wrote in their place.
    pub fn record(&self, volume: CompactionVolume) {
        self.tasks.fetch_add(1, Ordering::Relaxed);
        self.input_files
            .fetch_add(volume.input_files, Ordering::Relaxed);
        self.input_bytes
            .fetch_add(volume.input_bytes, Ordering::Relaxed);
        self.input_rows
            .fetch_add(volume.input_rows, Ordering::Relaxed);
        self.output_files
            .fetch_add(volume.output_files, Ordering::Relaxed);
        self.output_bytes
            .fetch_add(volume.output_bytes, Ordering::Relaxed);
        self.output_rows
            .fetch_add(volume.output_rows, Ordering::Relaxed);
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
            input_rows: self.input_rows.load(Ordering::Relaxed),
            output_files: self.output_files.load(Ordering::Relaxed),
            output_bytes: self.output_bytes.load(Ordering::Relaxed),
            output_rows: self.output_rows.load(Ordering::Relaxed),
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
    /// Rows read from the merged input files.
    pub input_rows: u64,
    pub output_files: u64,
    pub output_bytes: u64,
    /// Rows written after deduplication.
    pub output_rows: u64,
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

    /// Rows removed because a merge found newer versions of the same series
    /// and timestamp.
    pub fn deduplicated_rows(&self) -> u64 {
        self.input_rows.saturating_sub(self.output_rows)
    }
}

/// Manifest commit counters.
#[derive(Debug, Default)]
pub struct ManifestCounters {
    commits: AtomicU64,
    bytes_written: AtomicU64,
    total_millis: AtomicU64,
    max_millis: AtomicU64,
}

impl ManifestCounters {
    /// Records one durable delta commit of `bytes`.
    pub fn record(&self, bytes: u64, elapsed: Duration) {
        let millis = elapsed.as_millis() as u64;
        self.commits.fetch_add(1, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
        self.total_millis.fetch_add(millis, Ordering::Relaxed);
        self.max_millis.fetch_max(millis, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> ManifestStats {
        ManifestStats {
            commits: self.commits.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            total_millis: self.total_millis.load(Ordering::Relaxed),
            max_millis: self.max_millis.load(Ordering::Relaxed),
        }
    }
}

/// A point-in-time copy of the manifest commit counters.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ManifestStats {
    /// Delta objects committed successfully.
    pub commits: u64,
    /// Encoded delta bytes uploaded.
    pub bytes_written: u64,
    /// Wall time of those uploads, in milliseconds.
    pub total_millis: u64,
    /// Slowest single commit, in milliseconds.
    pub max_millis: u64,
}

impl ManifestStats {
    pub fn average_millis(&self) -> f64 {
        if self.commits == 0 {
            0.0
        } else {
            self.total_millis as f64 / self.commits as f64
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
    /// Manifest delta commits, shared by flush, compaction, rewrite and TTL.
    pub manifest: ManifestStats,
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
    manifest: ManifestCounters,
}

impl StorageMetrics {
    /// Records one successful flush.
    pub fn record_flush(
        &self,
        rows: u64,
        elapsed: Duration,
        stages: FlushStages,
        wal_lag: Duration,
    ) {
        self.flush.record(rows, elapsed, stages, wal_lag);
    }

    /// Records one committed compaction task.
    pub fn record_compaction(&self, volume: CompactionVolume) {
        self.compaction.record(volume);
    }

    /// Records one manifest delta upload.
    pub fn record_manifest_commit(&self, bytes: u64, elapsed: Duration) {
        self.manifest.record(bytes, elapsed);
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
            manifest: self.manifest.snapshot(),
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
    /// a flush moves rows, a compaction rewrites bytes and drops duplicates.
    #[test]
    fn test_flush_and_compaction_counters() {
        let stats = metrics();
        stats.record_flush(
            1_000,
            Duration::from_millis(30),
            FlushStages {
                sort: Duration::from_millis(5),
                write: Duration::from_millis(20),
                encode: Duration::from_millis(12),
                upload: Duration::from_millis(6),
                commit: Duration::from_millis(2),
                wal: Duration::from_millis(1),
            },
            Duration::from_millis(900),
        );
        // A Parquet flush reports no encode/upload split.
        stats.record_flush(
            2_000,
            Duration::from_millis(90),
            FlushStages {
                sort: Duration::from_millis(10),
                write: Duration::from_millis(70),
                commit: Duration::from_millis(3),
                wal: Duration::from_millis(4),
                ..Default::default()
            },
            Duration::from_millis(1_500),
        );

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.flush.flushes, 2);
        assert_eq!(snapshot.flush.rows, 3_000);
        assert_eq!(snapshot.flush.total_millis, 120);
        assert_eq!(snapshot.flush.max_millis, 90);
        assert_eq!(snapshot.flush.average_millis(), 60.0);
        assert_eq!(snapshot.flush.sort_millis, 15);
        assert_eq!(snapshot.flush.write_millis, 90);
        assert_eq!(snapshot.flush.encode_millis, 12);
        assert_eq!(snapshot.flush.upload_millis, 6);
        assert_eq!(snapshot.flush.commit_millis, 5);
        assert_eq!(snapshot.flush.wal_millis, 5);
        assert_eq!(snapshot.flush.wal_lag_total_millis, 2_400);
        assert_eq!(snapshot.flush.wal_lag_max_millis, 1_500);
        assert_eq!(snapshot.flush.average_wal_lag_millis(), 1_200.0);
        let (encode, upload) = snapshot.flush.write_split().unwrap();
        assert!((encode - 12.0 / 90.0).abs() < 1e-9);
        assert!((upload - 6.0 / 90.0).abs() < 1e-9);
        let (sort, write, commit, wal) = snapshot.flush.stage_ratio().unwrap();
        assert!((sort - 15.0 / 115.0).abs() < 1e-9);
        assert!((write - 90.0 / 115.0).abs() < 1e-9);
        assert!((commit - 5.0 / 115.0).abs() < 1e-9);
        assert!((wal - 5.0 / 115.0).abs() < 1e-9);
        // Flushes say nothing about file I/O; that is still the manifest's job.
        assert_eq!(snapshot.total().files_written, 0);

        stats.record_compaction(CompactionVolume {
            input_files: 4,
            input_bytes: 1_000,
            input_rows: 800,
            output_files: 1,
            output_bytes: 600,
            output_rows: 750,
        });
        stats.record_compaction(CompactionVolume {
            input_files: 2,
            input_bytes: 400,
            input_rows: 300,
            output_files: 1,
            output_bytes: 500,
            output_rows: 300,
        });
        stats.record_expired(3, 300);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.compaction.tasks, 2);
        assert_eq!(snapshot.compaction.input_files, 6);
        assert_eq!(snapshot.compaction.input_bytes, 1_400);
        assert_eq!(snapshot.compaction.input_rows, 1_100);
        assert_eq!(snapshot.compaction.output_files, 2);
        assert_eq!(snapshot.compaction.output_bytes, 1_100);
        assert_eq!(snapshot.compaction.output_rows, 1_050);
        assert_eq!(snapshot.compaction.deduplicated_rows(), 50);
        assert_eq!(snapshot.compaction.expired_files, 3);
        assert_eq!(snapshot.compaction.expired_bytes, 300);
        assert!((snapshot.compaction.write_amplification() - 1_100.0 / 1_400.0).abs() < 1e-9);
        assert_eq!(CompactionStats::default().write_amplification(), 0.0);
        assert_eq!(CompactionStats::default().deduplicated_rows(), 0);
        assert_eq!(FlushStats::default().average_millis(), 0.0);
        assert_eq!(FlushStats::default().stage_ratio(), None);
        assert_eq!(FlushStats::default().write_split(), None);
    }

    #[test]
    fn test_manifest_commit_counters() {
        let stats = metrics();
        stats.record_manifest_commit(120, Duration::from_millis(8));
        stats.record_manifest_commit(30, Duration::from_millis(2));

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.manifest.commits, 2);
        assert_eq!(snapshot.manifest.bytes_written, 150);
        assert_eq!(snapshot.manifest.total_millis, 10);
        assert_eq!(snapshot.manifest.max_millis, 8);
        assert_eq!(snapshot.manifest.average_millis(), 5.0);
        assert_eq!(ManifestStats::default().average_millis(), 0.0);
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
