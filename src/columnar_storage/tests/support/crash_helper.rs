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

//! Crash-injection helper for `tests/crash_recovery.rs` (ADR-0002 §12 T3).
//!
//! `kill -9` cannot be simulated in-process, so the recovery test drives a real
//! `ObjectBasedStorage` in this child process and sends `SIGKILL` at a chosen
//! point. The helper prints `READY <detail>` on stdout when it reaches that
//! point and then blocks; the parent kills it, reopens the same root and checks
//! that the manifest, the WAL and the object store agree on what is durable.
//!
//! Usage: `crash_helper <root> <phase> [rows]`.
//!
//! Phases:
//!
//! | phase | state at the kill |
//! |---|---|
//! | `wal` | all rows in the WAL, nothing flushed |
//! | `flushed` | rows committed to the manifest, WAL truncated |
//! | `orphan` | committed rows plus one object the manifest does not reference |
//! | `crash-flush` | data object PUT done, manifest commit never ran, WAL intact |
//! | `crash-commit` | manifest delta PUT done, in-memory update and WAL truncation never ran |
//! | `crash-compact` | compaction output PUT done, manifest update never ran |

use std::{
    io::Write,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use arrow::{
    array::{Float64Array, Int64Array, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use columnar_storage::{
    config::{DataFileFormat, StorageConfig},
    storage::{ColumnarStorage, CompactRequest, ObjectBasedStorage, StorageRuntimes, WriteRequest},
    types::ObjectStoreRef,
};
use object_store::{
    local::LocalFileSystem, path::Path, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
};

/// Scrape interval of the synthetic series.
const STEP_MS: i64 = 1_000;
/// Series per metric; rows cycle through them.
const SERIES: u64 = 8;
/// Rows per `write` call, so one flush sees several WAL frames.
const CHUNK: usize = 16;
const DAY_MS: i64 = 86_400_000;

fn main() {
    let mut args = std::env::args().skip(1);
    let (Some(root), Some(phase)) = (args.next(), args.next()) else {
        eprintln!("usage: crash_helper <root> <phase> [rows]");
        std::process::exit(2);
    };
    let rows: usize = args.next().map(|v| v.parse().expect("rows")).unwrap_or(160);

    // The storage runs on its own runtime; the driver only owns the future we
    // block in, which never completes for the phases that wait to be killed.
    let worker = Arc::new(tokio::runtime::Runtime::new().expect("worker runtime"));
    let runtimes = StorageRuntimes::new(worker.clone(), worker.clone());
    let driver = tokio::runtime::Runtime::new().expect("driver runtime");
    driver.block_on(run(root, phase, rows, runtimes));
}

async fn run(root: String, phase: String, rows: usize, runtimes: StorageRuntimes) {
    let base = 1_700_000_000_000i64.div_euclid(DAY_MS) * DAY_MS;
    let inner: ObjectStoreRef = Arc::new(LocalFileSystem::new());
    // A crash between the data PUT and the manifest commit is only
    // distinguishable from one between the commit and the WAL truncation by
    // where the PUT lands, so stall on one kind of path at a time.
    let stall_prefix = if phase == "crash-commit" {
        Path::from(format!("{root}/manifest")).to_string()
    } else {
        Path::from(format!("{root}/data")).to_string()
    };
    let stalling = Arc::new(StallStore::new(inner.clone(), stall_prefix));
    let store: ObjectStoreRef = stalling.clone();

    let storage = ObjectBasedStorage::try_new(
        root.clone(),
        Duration::from_hours(24),
        store,
        sample_schema(),
        3,
        helper_config(),
        runtimes,
    )
    .await
    .expect("open storage");

    match phase.as_str() {
        // Everything is in the WAL; the buffer is never persisted.
        "wal" => {
            write_all(&storage, base, rows).await;
            ready("rows appended to the WAL");
        }
        // A clean, committed flush with the WAL already truncated.
        "flushed" => {
            write_all(&storage, base, rows).await;
            storage.flush().await.expect("flush");
            ready("rows committed");
        }
        // A committed flush plus an object that no manifest entry mentions,
        // the way an interrupted write path would leave one.
        "orphan" => {
            write_all(&storage, base, rows).await;
            storage.flush().await.expect("flush");
            inner
                .put(
                    &Path::from(format!("{root}/data/9999999999.tsf")),
                    vec![0u8; 128].into(),
                )
                .await
                .expect("put orphan");
            ready("orphan object written");
        }
        // The flush writes its data object, then stalls before the manifest
        // commit. The WAL still holds every row.
        "crash-flush" => {
            write_all(&storage, base, rows).await;
            stalling.arm();
            storage.flush().await.expect("flush stalls");
        }
        // The manifest delta is durable, but the in-memory update and the WAL
        // truncation have not run yet.
        "crash-commit" => {
            write_all(&storage, base, rows).await;
            stalling.arm();
            storage.flush().await.expect("flush stalls");
        }
        // One committed file per chunk, so the picker has a compaction
        // candidate, then a compaction that stalls after writing its output
        // object. Its inputs are still live.
        "crash-compact" => {
            let mut written = 0;
            while written < rows {
                let count = CHUNK.min(rows - written);
                write_chunk(&storage, base, written, count).await;
                storage.flush().await.expect("flush");
                written += count;
            }
            stalling.arm();
            storage.compact(CompactRequest {}).await.expect("compact");
        }
        other => {
            eprintln!("crash_helper: unknown phase {other}");
            std::process::exit(2);
        }
    }

    // Stay alive until the parent sends SIGKILL; nothing below this point is
    // allowed to clean anything up.
    std::future::pending::<()>().await
}

fn helper_config() -> StorageConfig {
    let mut config = StorageConfig {
        format: DataFileFormat::Tsf,
        ..StorageConfig::default()
    };
    // No background flush, no periodic compaction: only the phase itself may
    // move data, otherwise the crash point would move with it.
    config.buffer.flush_interval = common::ReadableDuration::hours(1);
    config.buffer.max_rows = 1 << 30;
    config.buffer.max_bytes = common::ReadableSize::gb(4);
    config.scheduler.schedule_interval = common::ReadableDuration::hours(1);
    // Two files in one segment are enough for the compaction phase.
    config.scheduler.input_sst_min_num = 2;
    config.scheduler.input_sst_max_num = 30;
    config
}

fn sample_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("metric_id", DataType::UInt64, false),
        Field::new("tsid", DataType::UInt64, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]))
}

async fn write_all(storage: &ObjectBasedStorage, base: i64, rows: usize) {
    let mut written = 0;
    while written < rows {
        let count = CHUNK.min(rows - written);
        write_chunk(storage, base, written, count).await;
        written += count;
    }
}

async fn write_chunk(storage: &ObjectBasedStorage, base: i64, from: usize, count: usize) {
    let mut metric_id = Vec::with_capacity(count);
    let mut tsid = Vec::with_capacity(count);
    let mut timestamp = Vec::with_capacity(count);
    let mut value = Vec::with_capacity(count);
    for i in from..from + count {
        metric_id.push(1u64);
        tsid.push(i as u64 % SERIES);
        timestamp.push(base + i as i64 * STEP_MS);
        value.push(i as f64 * 0.5);
    }
    let batch = RecordBatch::try_new(
        sample_schema(),
        vec![
            Arc::new(UInt64Array::from(metric_id)),
            Arc::new(UInt64Array::from(tsid)),
            Arc::new(Int64Array::from(timestamp)),
            Arc::new(Float64Array::from(value)),
        ],
    )
    .expect("build batch");
    let start = base + from as i64 * STEP_MS;
    let end = start + count as i64 * STEP_MS;
    storage
        .write(WriteRequest {
            batch,
            time_range: (start..end).into(),
            enable_check: true,
        })
        .await
        .expect("write");
}

fn ready(detail: &str) {
    println!("READY {detail}");
    let _ = std::io::stdout().flush();
}

/// An object store that writes one matching object and then never returns from
/// the call, so the caller can never finish its manifest commit or WAL
/// truncation. The parent kills the process while it hangs here.
#[derive(Debug)]
struct StallStore {
    inner: ObjectStoreRef,
    /// Only paths under this prefix stall; the other paths are untouched.
    prefix: String,
    armed: AtomicBool,
}

impl StallStore {
    fn new(inner: ObjectStoreRef, prefix: String) -> Self {
        Self {
            inner,
            prefix,
            armed: AtomicBool::new(false),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
}

impl std::fmt::Display for StallStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StallStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for StallStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let result = self.inner.put_opts(location, payload, opts).await?;
        if location.as_ref().starts_with(self.prefix.as_str())
            && self.armed.swap(false, Ordering::SeqCst)
        {
            ready(&format!("put {location}"));
            std::future::pending::<()>().await;
        }
        Ok(result)
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
        self.inner.get_opts(location, options).await
    }

    async fn get_range(
        &self,
        location: &Path,
        range: std::ops::Range<usize>,
    ) -> object_store::Result<bytes::Bytes> {
        self.inner.get_range(location, range).await
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
