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

use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, LazyLock,
    },
    time::SystemTime,
};

use crate::{
    ensure,
    types::{TimeRange, Timestamp},
    Error,
};

const PREFIX_PATH: &str = "data";

// Used for sst file id allocation.
// This number mustn't go backwards on restarts, otherwise file id
// collisions are possible. So don't change time on the server
// between server restarts.
static NEXT_ID: LazyLock<AtomicU64> = LazyLock::new(|| {
    AtomicU64::new(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
    )
});

pub type FileId = u64;

#[derive(Clone)]
pub struct SstFile {
    inner: Arc<Inner>,
}

impl fmt::Debug for SstFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SstFile")
            .field("id", &self.id())
            .field("meta", &self.meta())
            .field("in_compaction", &self.is_compaction())
            .finish()
    }
}

#[derive(Debug)]
struct Inner {
    id: FileId,
    meta: FileMeta,

    in_compaction: AtomicBool,
}

impl Inner {
    pub fn new(id: FileId, meta: FileMeta) -> Self {
        Self {
            id,
            meta,
            in_compaction: AtomicBool::new(false),
        }
    }
}

impl SstFile {
    pub fn new(id: FileId, meta: FileMeta) -> Self {
        let inner = Arc::new(Inner::new(id, meta));
        Self { inner }
    }

    pub fn id(&self) -> FileId {
        self.inner.id
    }

    pub fn meta(&self) -> &FileMeta {
        &self.inner.meta
    }

    pub fn mark_compaction(&self) {
        self.inner.in_compaction.store(true, Ordering::Relaxed);
    }

    pub fn unmark_compaction(&self) {
        self.inner.in_compaction.store(false, Ordering::Relaxed);
    }

    pub fn is_compaction(&self) -> bool {
        self.inner.in_compaction.load(Ordering::Relaxed)
    }

    pub fn is_expired(&self, expire_time: Option<Timestamp>) -> bool {
        match expire_time {
            Some(expire_time) => self.meta().time_range.end < expire_time,
            None => false,
        }
    }

    pub fn size(&self) -> u32 {
        self.meta().size
    }

    pub fn allocate_id() -> FileId {
        Self::allocate_ids(1)
    }

    /// Allocate `num` consecutive ids and return the first one. Row sequences
    /// and file ids are allocated from the same counter, so a later write
    /// always wins over an earlier one during merge.
    pub fn allocate_ids(num: u64) -> FileId {
        NEXT_ID.fetch_add(num, Ordering::SeqCst)
    }
}

impl TryFrom<pb_types::SstFile> for SstFile {
    type Error = Error;

    fn try_from(value: pb_types::SstFile) -> Result<Self, Self::Error> {
        ensure!(value.meta.is_some(), "file meta is missing");
        let meta = value.meta.unwrap();
        let meta = meta.try_into()?;

        Ok(Self::new(value.id, meta))
    }
}

impl From<SstFile> for pb_types::SstFile {
    fn from(value: SstFile) -> Self {
        pb_types::SstFile {
            id: value.id(),
            meta: Some(value.meta().clone().into()),
        }
    }
}

impl PartialEq for SstFile {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id() && self.meta() == self.meta()
    }
}

impl Eq for SstFile {}

/// Data format of a manifest entry. Mirrors `pb_types::FileFormat`; the
/// `Parquet` default keeps old manifests and deltas describing Parquet SSTs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FileFormat {
    #[default]
    Parquet,
    Tsf,
}

impl From<pb_types::FileFormat> for FileFormat {
    fn from(value: pb_types::FileFormat) -> Self {
        match value {
            pb_types::FileFormat::ParquetV1 => FileFormat::Parquet,
            pb_types::FileFormat::TsfV1 => FileFormat::Tsf,
        }
    }
}

impl From<FileFormat> for pb_types::FileFormat {
    fn from(value: FileFormat) -> Self {
        match value {
            FileFormat::Parquet => pb_types::FileFormat::ParquetV1,
            FileFormat::Tsf => pb_types::FileFormat::TsfV1,
        }
    }
}

/// File metadata used by the manifest, the TTL drop and the format-aware
/// reader.
///
/// `min_sequence` defaults to 0 for legacy Parquet entries and is populated by
/// the flush and compaction writers; compaction never narrows the sequence
/// span of its inputs (ADR-0002 §6.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMeta {
    pub max_sequence: u64,
    pub min_sequence: u64,
    pub num_rows: u32,
    pub size: u32,
    pub time_range: TimeRange,
    pub format: FileFormat,
}

impl FileMeta {
    /// Builds metadata for a file written from the given `rows`.
    ///
    /// The rows are expected to be sorted by `(metric_id, series_id,
    /// timestamp)`.
    pub fn from_rows<I>(rows: I, size: u32, format: FileFormat) -> Self
    where
        I: IntoIterator<Item = (u64, i64, u64)>,
    {
        let mut max_sequence = 0;
        let mut min_sequence = u64::MAX;
        let mut num_rows = 0;
        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;
        for (_, ts, seq) in rows {
            max_sequence = max_sequence.max(seq);
            min_sequence = min_sequence.min(seq);
            min_ts = min_ts.min(ts);
            max_ts = max_ts.max(ts);
            num_rows += 1;
        }
        if num_rows == 0 {
            return Self {
                max_sequence: 0,
                min_sequence: 0,
                num_rows: 0,
                size,
                time_range: TimeRange::new(Timestamp::MIN, Timestamp::MIN),
                format,
            };
        }
        Self {
            max_sequence,
            min_sequence,
            num_rows,
            size,
            time_range: (min_ts..max_ts).into(),
            format,
        }
    }

    /// Whether a query must route this file to the TSF reader.
    pub fn is_tsf(&self) -> bool {
        self.format == FileFormat::Tsf
    }
}

impl TryFrom<pb_types::SstMeta> for FileMeta {
    type Error = Error;

    fn try_from(value: pb_types::SstMeta) -> Result<Self, Self::Error> {
        ensure!(value.time_range.is_some(), "time range is missing");
        let time_range = value.time_range.unwrap();

        Ok(Self {
            max_sequence: value.max_sequence,
            min_sequence: value.min_sequence,
            num_rows: value.num_rows,
            size: value.size,
            time_range: TimeRange::new(time_range.start.into(), time_range.end.into()),
            format: value.format().into(),
        })
    }
}

impl From<FileMeta> for pb_types::SstMeta {
    fn from(value: FileMeta) -> Self {
        pb_types::SstMeta {
            max_sequence: value.max_sequence,
            min_sequence: value.min_sequence,
            num_rows: value.num_rows,
            size: value.size,
            format: pb_types::FileFormat::from(value.format) as i32,
            time_range: Some(pb_types::TimeRange {
                start: *value.time_range.start,
                end: *value.time_range.end,
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SstPathGenerator {
    prefix: String,
}

impl SstPathGenerator {
    pub fn new(prefix: String) -> Self {
        Self { prefix }
    }

    /// Legacy Parquet path: `{root}/data/{file_id}.sst`.
    pub fn generate(&self, id: FileId) -> String {
        format!("{}/{}/{}.sst", self.prefix, PREFIX_PATH, id)
    }

    /// TSF path: `{root}/data/{YYYYMMDD}/{file_id}.tsf` (ADR-0002 §8.1).
    ///
    /// The partition directory comes from the file's time range start, which
    /// is always the earliest timestamp in the file. Files that span a day
    /// boundary are rejected by the flush path before reaching here.
    pub fn generate_tsf(&self, id: FileId, partition_start_ms: i64) -> String {
        use storage_kernel::tsf::partition::partition_label;
        format!(
            "{}/{}/{}/{}.tsf",
            self.prefix,
            PREFIX_PATH,
            partition_label(partition_start_ms),
            id
        )
    }

    /// Resolves the object path described by a manifest entry.
    pub fn generate_for(&self, id: FileId, meta: &FileMeta) -> String {
        if meta.is_tsf() {
            self.generate_tsf(id, *meta.time_range.start)
        } else {
            self.generate(id)
        }
    }
}
