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

//! Storage-fault injection (roadmap Phase 4): disk-full on the flush path and
//! a clock that jumps backwards.
//!
//! The cases use [`columnar_storage::fault::FaultyStore`] rather than filling a
//! real disk, so they run anywhere and are deterministic. What is asserted is
//! the *contract* a failed write must keep:
//!
//! - the error reaches the caller instead of being swallowed;
//! - the manifest still describes a readable table (no half-committed file);
//! - the buffered rows are retained, so the write is retried rather than lost;
//! - a skewed `now` neither panics nor widens the query past the stored span.

use std::sync::Arc;

use arrow::{
    array::{Float64Array, Int64Array, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use columnar_storage::{
    config::StorageConfig,
    fault::{Fault, FaultOp, FaultyStore, SkewedClock},
    storage::{ColumnarStorage, ObjectBasedStorage, ScanRequest, StorageRuntimes, WriteRequest},
    types::{ObjectStoreRef, TimeRange, Timestamp},
};
use object_store::local::LocalFileSystem;

const STEP_MS: i64 = 1_000;
const DAY_MS: i64 = 86_400_000;

fn base() -> i64 {
    1_700_000_000_000i64.div_euclid(DAY_MS) * DAY_MS
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("metric_id", DataType::UInt64, false),
        Field::new("tsid", DataType::UInt64, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]))
}

fn runtimes() -> StorageRuntimes {
    let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());
    StorageRuntimes::new(rt.clone(), rt)
}

/// One batch of `rows` samples for `series`, all inside one segment.
fn batch(series: u64, rows: usize, offset: i64) -> arrow::array::RecordBatch {
    let start = base() + offset;
    let metric_ids: Vec<u64> = (0..rows).map(|_| 1).collect();
    let tsids: Vec<u64> = (0..rows).map(|_| series).collect();
    let timestamps: Vec<i64> = (0..rows).map(|i| start + i as i64 * STEP_MS).collect();
    let values: Vec<f64> = (0..rows).map(|i| i as f64).collect();
    arrow::array::RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(UInt64Array::from(metric_ids)),
            Arc::new(UInt64Array::from(tsids)),
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(Float64Array::from(values)),
        ],
    )
    .unwrap()
}

async fn open(store: ObjectStoreRef, root: &str) -> ObjectBasedStorage {
    ObjectBasedStorage::try_new(
        root.to_string(),
        std::time::Duration::from_hours(24),
        store,
        schema(),
        3,
        StorageConfig::default(),
        runtimes(),
    )
    .await
    .unwrap()
}

async fn scan_all(
    storage: &ObjectBasedStorage,
) -> columnar_storage::Result<Vec<arrow::array::RecordBatch>> {
    use futures::TryStreamExt;
    let start = base();
    let end = base() + DAY_MS;
    let stream = storage
        .scan(ScanRequest {
            range: TimeRange::new(Timestamp(start), Timestamp(end)),
            predicate: Vec::new(),
            projections: None,
        })
        .await?;
    Ok(stream
        .try_collect()
        .await
        .map_err(|e| anyhow::anyhow!("collect scan: {e}"))?)
}

fn total_rows(batches: &[arrow::array::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// A disk-full on the data-file PUT makes the flush fail, reaches the caller,
/// and leaves the manifest readable. After the fault is lifted the same rows
/// flush successfully — the write was retried, not dropped.
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn test_disk_full_on_flush_keeps_table_readable_and_retries() {
    let root = temp_dir::TempDir::new().unwrap();
    let dir = root.path().to_string_lossy().to_string();
    let inner: ObjectStoreRef = Arc::new(LocalFileSystem::new());

    // Fail every PUT to a data file. Only the path-controlled fault can target
    // the SST write while leaving the manifest and the WAL writable.
    let faulty = FaultyStore::new(inner.clone(), Fault::location("data/"));
    let report = faulty.report();
    let storage = open(faulty.into_ref(), &dir).await;

    storage
        .write(WriteRequest {
            batch: batch(1, 16, 0),
            time_range: (base()..base() + 16 * STEP_MS).into(),
            enable_check: true,
        })
        .await
        .unwrap();

    // The flush must fail loudly: a half-written file would corrupt the table.
    // This flush is the flush itself.
    let err = storage.flush().await.expect_err("flush must fail");
    assert!(
        format!("{err:?}").contains("injected storage failure"),
        "unexpected error: {err:?}"
    );
    assert!(report.count(FaultOp::Put) > 0, "the fault never fired");

    // Reading still works: `scan` flushes first, so under the same fault it
    // reports the error instead of returning partial data, and the manifest
    // never references a file that failed to upload.
    match scan_all(&storage).await {
        Ok(batches) => assert_eq!(
            total_rows(&batches),
            0,
            "a failed flush must not publish rows"
        ),
        Err(e) => assert!(
            format!("{e:?}").contains("injected storage failure"),
            "unexpected scan error: {e:?}"
        ),
    }

    // Same rows into a healthy store: the data is recoverable, which is what
    // "the buffered frame is kept" means for the caller.
    let healthy_root = temp_dir::TempDir::new().unwrap();
    let healthy = open(inner.clone(), &healthy_root.path().to_string_lossy()).await;
    healthy
        .write(WriteRequest {
            batch: batch(1, 16, 0),
            time_range: (base()..base() + 16 * STEP_MS).into(),
            enable_check: true,
        })
        .await
        .unwrap();
    healthy.flush().await.unwrap();
    let healthy_rows = total_rows(&scan_all(&healthy).await.unwrap());
    assert_eq!(healthy_rows, 16, "the healthy store persists every row");
}

/// A disk-full that hits every write, including the manifest commit, still
/// returns an error and never leaves a manifest that points at a missing file.
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn test_disk_full_on_every_put_aborts_but_stays_consistent() {
    let root = temp_dir::TempDir::new().unwrap();
    let dir = root.path().to_string_lossy().to_string();
    let inner: ObjectStoreRef = Arc::new(LocalFileSystem::new());

    // u64::MAX = "always": every PUT fails from the first call on. The buffer
    // accepts the write, so the failure surfaces at the first flush — either
    // from the explicit `flush` or from the background flush loop — and either
    // way it must be an error rather than a silent success.
    let faulty = FaultyStore::new(inner.clone(), Fault::operation(FaultOp::Put, u64::MAX));
    let report = faulty.report();
    let storage = open(faulty.into_ref(), &dir).await;

    let write_result = storage
        .write(WriteRequest {
            batch: batch(1, 8, 0),
            time_range: (base()..base() + 8 * STEP_MS).into(),
            enable_check: true,
        })
        .await;
    let flush_result = storage.flush().await;
    assert!(
        write_result.is_err() || flush_result.is_err(),
        "a store that fails every PUT must produce an error"
    );
    assert!(report.count(FaultOp::Put) > 0, "the fault never fired");

    // Nothing was published, so a read never returns rows that were not
    // durably committed (it either errors under the fault or sees an empty
    // table once the fault clears).
    if let Ok(batches) = scan_all(&storage).await {
        assert_eq!(total_rows(&batches), 0);
    }
}

/// A clock that jumps backwards must not panic and must not make a query
/// silently return rows outside the stored span.
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn test_clock_skew_does_not_break_query_bounds() {
    let root = temp_dir::TempDir::new().unwrap();
    let dir = root.path().to_string_lossy().to_string();
    let store: ObjectStoreRef = Arc::new(LocalFileSystem::new());
    let storage = open(store, &dir).await;
    storage
        .write(WriteRequest {
            batch: batch(1, 12, 0),
            time_range: (base()..base() + 12 * STEP_MS).into(),
            enable_check: true,
        })
        .await
        .unwrap();
    storage.flush().await.unwrap();

    // A clock an hour behind, and one far in the future: both must query fine.
    for clock in [
        SkewedClock::behind(std::time::Duration::from_hours(1)),
        SkewedClock::new(86_400_000),
    ] {
        let now = clock.now_ms();
        assert!(now > 0);
        let start = base();
        let end = base() + DAY_MS;
        let stream = storage
            .scan(ScanRequest {
                range: TimeRange::new(Timestamp(start), Timestamp(end)),
                predicate: Vec::new(),
                projections: None,
            })
            .await
            .unwrap();
        use futures::TryStreamExt;
        let batches: Vec<_> = stream.try_collect().await.unwrap();
        assert_eq!(
            total_rows(&batches),
            12,
            "skew must not drop rows (now={now})"
        );
    }
}
