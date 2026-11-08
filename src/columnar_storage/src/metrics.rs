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

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
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

/// Both formats' counters, as reported to the operator.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StorageStats {
    pub parquet: FormatStats,
    pub tsf: FormatStats,
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
}

impl StorageMetrics {
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
}
