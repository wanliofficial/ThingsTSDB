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

//! Deterministic storage-fault injection for tests and drills (roadmap Phase
//! 4).
//!
//! [`FaultyStore`] wraps an [`ObjectStore`] and turns selected operations into
//! errors, which is how a disk-full (`ENOSPC`) or a transient upload failure is
//! reproduced without filling a real disk or unplugging a network. The store is
//! transparent when no fault is armed, so it can sit in front of the production
//! store for a drill.
//!
//! Two knobs matter for "the write failed" semantics:
//!
//! - [`FaultKind::Operation`] fails the *first* `n` calls of one operation;
//! - [`FaultKind::Location`] fails any operation whose path contains a
//!   substring, so a test can make exactly one table (say the manifest) fail.
//!
//! A fault that fires is counted in [`FaultReport`], so a test can assert the
//! fault really was hit rather than silently skipped.
//!
//! Clock skew is the third case and needs no wrapper: callers pass the
//! timestamp, and [`skewed_now`] plus the query-range clamp
//! (`MetricEngine::search` clamps to the on-disk span) already make an
//! out-of-order `now` a non-event. The metric-engine tests cover that path.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use tokio::time::Instant;

use crate::types::ObjectStoreRef;

/// Which storage operation a fault applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultOp {
    Put,
    Get,
    Delete,
    List,
    Copy,
}

impl FaultOp {
    fn matches(self, op: FaultOp) -> bool {
        self == op
    }
}

/// The shape of the injected failure.
#[derive(Debug, Clone)]
pub enum FaultKind {
    /// Fail the next `remaining` calls of `op`. `u64::MAX` means "always".
    Operation {
        op: FaultOp,
        remaining: Arc<AtomicU64>,
    },
    /// Fail any operation whose path contains `needle`.
    Location { needle: String },
}

/// An armed fault. Cheap to clone so several wrappers can share one plan.
#[derive(Debug, Clone)]
pub struct Fault {
    kind: FaultKind,
    /// How the failure is reported. Disk-full is `Storage`, a transient upload
    /// error is `Generic`.
    display: &'static str,
    retryable: bool,
}

impl Fault {
    /// A failure on `op` for the next `remaining` calls, reported as a
    /// disk-full style error (non-retryable).
    pub fn operation(op: FaultOp, remaining: u64) -> Self {
        Self {
            kind: FaultKind::Operation {
                op,
                remaining: Arc::new(AtomicU64::new(remaining)),
            },
            display: "No space left on device",
            retryable: false,
        }
    }

    /// A failure on any operation touching a path containing `needle`.
    pub fn location(needle: impl Into<String>) -> Self {
        Self {
            kind: FaultKind::Location {
                needle: needle.into(),
            },
            display: "injected storage failure",
            retryable: false,
        }
    }

    /// Marks the failure as transient (a 5xx-style error a retry may fix).
    pub fn retryable(mut self) -> Self {
        self.retryable = true;
        self
    }

    fn error(&self) -> object_store::Error {
        object_store::Error::Generic {
            store: "FaultyStore",
            source: Box::new(std::io::Error::other(self.display)),
        }
    }

    fn is_retryable(&self) -> bool {
        self.retryable
    }
}

/// Counts of how often the injected fault actually fired, by operation.
#[derive(Debug, Default)]
pub struct FaultReport {
    put: AtomicU64,
    get: AtomicU64,
    delete: AtomicU64,
    list: AtomicU64,
    copy: AtomicU64,
}

impl FaultReport {
    /// Faults fired for `op`.
    pub fn count(&self, op: FaultOp) -> u64 {
        match op {
            FaultOp::Put => self.put.load(Ordering::Relaxed),
            FaultOp::Get => self.get.load(Ordering::Relaxed),
            FaultOp::Delete => self.delete.load(Ordering::Relaxed),
            FaultOp::List => self.list.load(Ordering::Relaxed),
            FaultOp::Copy => self.copy.load(Ordering::Relaxed),
        }
    }

    /// Total faults fired across all operations.
    pub fn total(&self) -> u64 {
        [
            FaultOp::Put,
            FaultOp::Get,
            FaultOp::Delete,
            FaultOp::List,
            FaultOp::Copy,
        ]
        .into_iter()
        .map(|op| self.count(op))
        .sum()
    }

    fn record(&self, op: FaultOp) {
        let counter = match op {
            FaultOp::Put => &self.put,
            FaultOp::Get => &self.get,
            FaultOp::Delete => &self.delete,
            FaultOp::List => &self.list,
            FaultOp::Copy => &self.copy,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

/// An [`ObjectStore`] that fails selected operations.
///
/// Sharing one `FaultReport` between several `FaultyStore` instances lets a
/// test count every injected failure while each table is wrapped separately.
pub struct FaultyStore {
    inner: ObjectStoreRef,
    fault: Fault,
    report: Arc<FaultReport>,
}

impl FaultyStore {
    /// Wraps `inner`, failing according to `fault` and counting in a fresh
    /// report.
    pub fn new(inner: ObjectStoreRef, fault: Fault) -> Self {
        Self {
            inner,
            fault,
            report: Arc::new(FaultReport::default()),
        }
    }

    /// Wraps `inner`, sharing `report` with other wrappers.
    pub fn with_report(inner: ObjectStoreRef, fault: Fault, report: Arc<FaultReport>) -> Self {
        Self {
            inner,
            fault,
            report,
        }
    }

    /// The shared report so a test can assert the fault fired.
    pub fn report(&self) -> Arc<FaultReport> {
        Arc::clone(&self.report)
    }

    /// The wrapper as a store reference, which is what `try_new` takes.
    pub fn into_ref(self) -> ObjectStoreRef {
        Arc::new(self)
    }

    /// Decides whether `op` on `location` fails, and records it when it does.
    fn check(&self, op: FaultOp, location: &Path) -> OsResult<()> {
        let hit = match &self.fault.kind {
            FaultKind::Operation {
                op: failing,
                remaining,
            } => {
                if !failing.matches(op) {
                    false
                } else {
                    // Decrement-and-test: the counter is shared through the
                    // `Arc`, so a fault armed for N calls is consumed across
                    // every wrapper that holds the same plan.
                    remaining
                        .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
                        .is_ok()
                }
            }
            FaultKind::Location { needle } => location.to_string().contains(needle),
        };
        if !hit {
            return Ok(());
        }
        self.report.record(op);
        Err(self.fault.error())
    }

    /// Retryability of the armed fault, used by callers that classify errors.
    pub fn is_retryable(&self) -> bool {
        self.fault.is_retryable()
    }
}

impl std::fmt::Debug for FaultyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FaultyStore")
    }
}

impl std::fmt::Display for FaultyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FaultyStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for FaultyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.check(FaultOp::Put, location)?;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.check(FaultOp::Put, location)?;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        self.check(FaultOp::Get, location)?;
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: std::ops::Range<usize>) -> OsResult<Bytes> {
        self.check(FaultOp::Get, location)?;
        self.inner.get_range(location, range).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.check(FaultOp::Delete, location)?;
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        // `list` is not async, so a fault is surfaced as a one-item error
        // stream, which is how a real backend reports a failed listing too.
        let prefix_path = prefix.cloned().unwrap_or_else(|| Path::from(""));
        if self.check(FaultOp::List, &prefix_path).is_err() {
            let err = self.fault.error();
            return Box::pin(futures::stream::once(async move { Err(err) }));
        }
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        let prefix_path = prefix.cloned().unwrap_or_else(|| Path::from(""));
        self.check(FaultOp::List, &prefix_path)?;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.check(FaultOp::Copy, from)?;
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.check(FaultOp::Copy, from)?;
        self.inner.copy_if_not_exists(from, to).await
    }
}

/// A wall-clock reader that can be skewed for a test, so "clock went
/// backwards" is reproducible without changing the host clock.
///
/// Production code reads `common::now()`; a drill passes `skewed_now` the same
/// way a query would and checks that the range clamp keeps results sane.
#[derive(Debug, Clone, Copy, Default)]
pub struct SkewedClock {
    offset_ms: i64,
}

impl SkewedClock {
    /// A clock that reports the real time plus `offset_ms`.
    pub fn new(offset_ms: i64) -> Self {
        Self { offset_ms }
    }

    /// A clock that has jumped `behind` into the past.
    pub fn behind(behind: Duration) -> Self {
        Self {
            offset_ms: -(behind.as_millis() as i64),
        }
    }

    /// The skewed "now" in milliseconds.
    pub fn now_ms(&self) -> i64 {
        common::now().saturating_add(self.offset_ms)
    }
}

/// A deliberately slow store that adds `delay` to every `put`, used to widen a
/// timing window in concurrency drills.
pub struct SlowPutStore {
    inner: ObjectStoreRef,
    delay: Duration,
}

impl SlowPutStore {
    pub fn new(inner: ObjectStoreRef, delay: Duration) -> Self {
        Self { inner, delay }
    }
}

impl std::fmt::Debug for SlowPutStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SlowPutStore")
    }
}

impl std::fmt::Display for SlowPutStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlowPutStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for SlowPutStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        tokio::time::sleep_until(Instant::now() + self.delay).await;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}
