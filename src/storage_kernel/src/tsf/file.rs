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

//! TSF v1 file writer and reader.
//!
//! The prototype writes the exact header/footer/index structures described in
//! ADR-0002. A reader can be built either from the whole file or from a set of
//! fetched byte ranges ([`TsfReader::from_segments`]), which is what the object
//! store path uses so a narrow query does not download the whole file.

use std::{collections::HashMap, hash::Hash, path::Path, sync::Arc};

use super::Row;
use crate::tsf::{
    bloom,
    checksum::crc32c,
    codec::{compress_zstd_level, decompress_zstd},
    page::{
        contiguous_time_slice, decode_page, decode_page_into, encode_page, ColumnMask, PageBuffers,
        PageMeta, MAX_PAGE_ROWS,
    },
};

pub const FILE_HEADER_SIZE: usize = 128;
pub const FOOTER_SIZE: usize = 64;
pub const CHUNK_GROUP_HEADER_SIZE: usize = 48;
pub const SERIES_CHUNK_HEADER_SIZE: usize = 96;
pub const SERIES_INDEX_ROW_SIZE: usize = 40;
pub const SERIES_ENTRY_SIZE: usize = 56;
pub const PAGE_ENTRY_SIZE: usize = 80;
pub const CHUNK_GROUP_ENTRY_SIZE: usize = 40;
pub const FORMAT_VERSION: u16 = 1;
pub const SCHEMA_ID: u32 = 1;
const MAGIC: &[u8; 4] = b"TSF1";
const SERIES_INDEX_BLOCK_ENTRIES: usize = 64;

/// Options for the prototype writer.
#[derive(Clone, Copy, Debug)]
pub struct WriterOptions {
    pub page_rows: usize,
}

impl Default for WriterOptions {
    fn default() -> Self {
        Self {
            page_rows: MAX_PAGE_ROWS,
        }
    }
}

/// Summary of a written file.
#[derive(Debug, Clone, Copy, Default)]
pub struct FileStats {
    pub bytes: u64,
    pub rows: u64,
    pub series: u64,
    pub groups: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct TimeRange {
    /// Inclusive.
    pub start: i64,
    /// Exclusive.
    pub end: i64,
}

impl TimeRange {
    pub fn new(start: i64, end: i64) -> Self {
        Self { start, end }
    }

    fn contains(&self, ts: i64) -> bool {
        ts >= self.start && ts < self.end
    }
}

/// `[start, end)` over `__seq__`.
pub type SequenceRange = Option<(u64, u64)>;

/// Decoded, projected columns for one series and one time range.
///
/// Unrequested columns are empty. Rows preserve the on-disk order
/// `(timestamp, __seq__)`; duplicate timestamps and NaN values are kept.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DecodedColumns {
    pub metric_id: u64,
    pub series_id: u64,
    pub timestamps: Vec<i64>,
    pub values: Vec<f64>,
    pub sequences: Vec<u64>,
    pub reserved: Vec<u64>,
    pub mask: ColumnMask,
}

impl DecodedColumns {
    pub fn new(metric_id: u64, series_id: u64, mask: ColumnMask) -> Self {
        Self {
            metric_id,
            series_id,
            timestamps: Vec::new(),
            values: Vec::new(),
            sequences: Vec::new(),
            reserved: Vec::new(),
            mask,
        }
    }

    pub fn len(&self) -> usize {
        if self.mask.contains(ColumnMask::TIMESTAMP) {
            self.timestamps.len()
        } else {
            self.values
                .len()
                .max(self.sequences.len())
                .max(self.reserved.len())
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Approximate heap bytes held by this batch.
    pub fn capacity_bytes(&self) -> usize {
        self.timestamps.capacity() * std::mem::size_of::<i64>()
            + self.values.capacity() * std::mem::size_of::<f64>()
            + self.sequences.capacity() * std::mem::size_of::<u64>()
            + self.reserved.capacity() * std::mem::size_of::<u64>()
    }
}

/// A sorted, deduplicated set of series IDs that a scan may decode.
///
/// A query usually knows its series before it reads: the metric engine resolves
/// label matchers to `tsid`s and pushes them down as `tsid IN (...)`. Passing
/// that set to the planner turns "read every series of the metric" into "read
/// these series chunks only", which is the difference between a full-metric
/// decode and a single-series one.
///
/// An empty filter matches no series. It is distinct from "no filter", which
/// [`TsfReader::next_scan_ranges`] expresses as `None`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SeriesFilter {
    ids: Vec<u64>,
}

impl SeriesFilter {
    /// Builds a filter from any ID order; duplicates are removed.
    pub fn new(ids: impl IntoIterator<Item = u64>) -> Self {
        let mut ids = ids.into_iter().collect::<Vec<_>>();
        ids.sort_unstable();
        ids.dedup();
        Self { ids }
    }

    pub fn contains(&self, series_id: u64) -> bool {
        self.ids.binary_search(&series_id).is_ok()
    }

    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    pub fn len(&self) -> usize {
        self.ids.len()
    }

    /// The IDs, ascending and deduplicated.
    pub fn ids(&self) -> &[u64] {
        &self.ids
    }

    /// True when the closed interval `[first, last]` holds a filtered ID. Used
    /// to skip whole series-index blocks, which cover a contiguous ID range.
    fn intersects_block(&self, first: u64, last: u64) -> bool {
        let idx = self.ids.partition_point(|&id| id < first);
        matches!(self.ids.get(idx), Some(&id) if id <= last)
    }
}

/// Counters exposed for ADR-0002 §13 observability.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub rejects: u64,
    pub entries: usize,
    pub bytes: usize,
}

impl CacheStats {
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }

    pub fn merge(&mut self, other: &CacheStats) {
        self.hits += other.hits;
        self.misses += other.misses;
        self.evictions += other.evictions;
        self.rejects += other.rejects;
        self.entries += other.entries;
        self.bytes += other.bytes;
    }
}

struct CacheSlot<V> {
    value: Arc<V>,
    bytes: usize,
    tick: u64,
}

/// A small byte-bounded LRU cache.
///
/// Entries are immutable for the lifetime of a TSF file, so there is no
/// invalidation path: a new file gets a new identity and therefore new keys.
struct LruCache<K, V> {
    map: HashMap<K, CacheSlot<V>>,
    bytes: usize,
    max_bytes: usize,
    tick: u64,
    hits: u64,
    misses: u64,
    evictions: u64,
    rejects: u64,
}

impl<K: Eq + Hash + Clone, V> LruCache<K, V> {
    fn new(max_bytes: usize) -> Self {
        Self {
            map: HashMap::new(),
            bytes: 0,
            max_bytes,
            tick: 0,
            hits: 0,
            misses: 0,
            evictions: 0,
            rejects: 0,
        }
    }

    fn get(&mut self, key: &K) -> Option<Arc<V>> {
        self.tick += 1;
        match self.map.get_mut(key) {
            Some(slot) => {
                slot.tick = self.tick;
                self.hits += 1;
                Some(slot.value.clone())
            }
            None => {
                self.misses += 1;
                None
            }
        }
    }

    fn insert(&mut self, key: K, value: V, bytes: usize) -> Arc<V> {
        self.tick += 1;
        let value = Arc::new(value);
        if bytes > self.max_bytes {
            // A single entry larger than the whole budget is returned
            // uncached, so queries still make progress.
            self.rejects += 1;
            return value;
        }
        if let Some(old) = self.map.remove(&key) {
            self.bytes = self.bytes.saturating_sub(old.bytes);
        }
        self.map.insert(
            key,
            CacheSlot {
                value: value.clone(),
                bytes,
                tick: self.tick,
            },
        );
        self.bytes += bytes;
        while self.bytes > self.max_bytes {
            let victim = self
                .map
                .iter()
                .min_by_key(|(_, slot)| slot.tick)
                .map(|(key, _)| key.clone());
            let Some(victim) = victim else {
                break;
            };
            if let Some(slot) = self.map.remove(&victim) {
                self.bytes = self.bytes.saturating_sub(slot.bytes);
                self.evictions += 1;
            }
        }
        value
    }

    fn clear(&mut self) {
        self.map.clear();
        self.bytes = 0;
    }

    fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits,
            misses: self.misses,
            evictions: self.evictions,
            rejects: self.rejects,
            entries: self.map.len(),
            bytes: self.bytes,
        }
    }
}

type SeriesIndexKey = (u64, u64);
type SeriesBlockKey = (u64, u64, u64);
type PageIndexKey = (u64, u64, u64);

/// Default per-worker index-cache budget from the decode plan (16 MiB).
pub const DEFAULT_CACHE_BYTES: usize = 16 * 1024 * 1024;

/// Caller-owned decode state: page buffers plus decompressed index caches.
///
/// The reader itself stays immutable and `Sync`; one context belongs to one
/// decode worker so no locks are needed.
pub struct ReadContext {
    pub buffers: PageBuffers,
    series_index: LruCache<SeriesIndexKey, Vec<SeriesIndexRow>>,
    series_blocks: LruCache<SeriesBlockKey, Vec<SeriesEntry>>,
    page_index: LruCache<PageIndexKey, Vec<PageEntry>>,
}

impl Default for ReadContext {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadContext {
    pub fn new() -> Self {
        Self::with_cache_bytes(DEFAULT_CACHE_BYTES)
    }

    /// Creates a context whose three index caches share `max_bytes`.
    pub fn with_cache_bytes(max_bytes: usize) -> Self {
        let per_cache = max_bytes / 3;
        let mut buffers = PageBuffers::new();
        buffers.reserve_page();
        Self {
            buffers,
            series_index: LruCache::new(per_cache),
            series_blocks: LruCache::new(per_cache),
            page_index: LruCache::new(per_cache),
        }
    }

    pub fn cache_stats(&self) -> CacheStats {
        let mut stats = self.series_index.stats();
        stats.merge(&self.series_blocks.stats());
        stats.merge(&self.page_index.stats());
        stats
    }

    pub fn cache_bytes(&self) -> usize {
        self.series_index.bytes + self.series_blocks.bytes + self.page_index.bytes
    }

    pub fn clear_caches(&mut self) {
        self.series_index.clear();
        self.series_blocks.clear();
        self.page_index.clear();
    }
}

/// Options for a parallel metric scan.
#[derive(Debug, Clone, Copy)]
pub struct ScanOptions {
    /// Number of decode workers. Zero means "available parallelism".
    pub workers: usize,
    /// Index-cache budget per worker.
    pub cache_bytes_per_worker: usize,
    /// Target rows per emitted Arrow batch. Kept here so the kernel reports
    /// the same batching contract the Arrow layer uses.
    pub batch_rows: usize,
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self {
            workers: 0,
            cache_bytes_per_worker: DEFAULT_CACHE_BYTES,
            batch_rows: MAX_PAGE_ROWS,
        }
    }
}

/// Result of a parallel metric scan: decoded series in `series_id` order.
#[derive(Debug)]
pub struct ScanResult {
    pub columns: Vec<DecodedColumns>,
    pub stats: CacheStats,
    pub workers: usize,
    pub bytes: usize,
}

/// Decodes one metric range, splitting series chunks across dedicated
/// worker threads when more than one worker is requested.
///
/// Ordering is deterministic: the returned `columns` follow the file's
/// `series_id` order regardless of thread completion order. Each worker owns
/// its own [`ReadContext`], so the reader is never locked.
pub fn scan_metric_parallel(
    reader: &TsfReader,
    metric_id: u64,
    range: TimeRange,
    columns: ColumnMask,
    opts: &ScanOptions,
) -> Result<ScanResult, String> {
    let Some(group) = reader.group_for(metric_id)? else {
        return Ok(ScanResult {
            columns: Vec::new(),
            stats: CacheStats::default(),
            workers: 0,
            bytes: 0,
        });
    };
    let header = reader.chunk_group_header(&group)?;
    if !header.overlaps(range) {
        return Ok(ScanResult {
            columns: Vec::new(),
            stats: CacheStats::default(),
            workers: 0,
            bytes: 0,
        });
    }
    let entries = reader.series_entries(&header)?;
    if entries.is_empty() {
        return Ok(ScanResult {
            columns: Vec::new(),
            stats: CacheStats::default(),
            workers: 0,
            bytes: 0,
        });
    }

    let available = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let requested = if opts.workers == 0 {
        available
    } else {
        opts.workers
    };
    let workers = requested.clamp(1, entries.len());

    let decode_chunk =
        |chunk: &[SeriesEntry]| -> Result<(Vec<DecodedColumns>, CacheStats), String> {
            let mut ctx = ReadContext::with_cache_bytes(opts.cache_bytes_per_worker);
            let mut out = Vec::new();
            for entry in chunk {
                if entry.max_timestamp < range.start || entry.min_timestamp >= range.end {
                    continue;
                }
                let mut columns_out = DecodedColumns::new(metric_id, entry.series_id, columns);
                reader.read_entry_columns(
                    metric_id,
                    entry,
                    range,
                    columns,
                    &mut ctx,
                    &mut columns_out,
                )?;
                if !columns_out.is_empty() {
                    out.push(columns_out);
                }
            }
            Ok((out, ctx.cache_stats()))
        };

    if workers == 1 {
        let (columns, stats) = decode_chunk(&entries)?;
        let bytes = columns.iter().map(|c| c.capacity_bytes()).sum();
        return Ok(ScanResult {
            columns,
            stats,
            workers: 1,
            bytes,
        });
    }

    let chunk_size = entries.len().div_ceil(workers);
    let mut all = Vec::with_capacity(entries.len());
    let mut stats = CacheStats::default();
    std::thread::scope(|scope| -> Result<(), String> {
        let handles: Vec<_> = entries
            .chunks(chunk_size)
            .map(|chunk| scope.spawn(move || decode_chunk(chunk)))
            .collect();
        for handle in handles {
            let (mut columns, worker_stats) = handle
                .join()
                .map_err(|_| "decode worker panicked".to_string())??;
            stats.merge(&worker_stats);
            all.append(&mut columns);
        }
        Ok(())
    })?;
    // `entries` is sorted by series_id and chunks preserve order, so `all` is
    // already deterministic. Sort defensively to keep the contract explicit.
    all.sort_by_key(|c| c.series_id);
    let bytes = all.iter().map(|c| c.capacity_bytes()).sum();
    Ok(ScanResult {
        columns: all,
        stats,
        workers,
        bytes,
    })
}

#[derive(Debug, Clone, Default)]
pub struct FileHeader {
    pub flags: u16,
    pub partition_start_ms: i64,
    pub partition_end_ms: i64,
    pub chunk_group_count: u32,
    pub series_count: u64,
    pub row_count: u64,
    pub min_sequence: u64,
    pub max_sequence: u64,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, Copy)]
struct Footer {
    file_index_offset: u64,
    file_index_size: u32,
    bloom_offset: u64,
    bloom_size: u32,
    bloom_crc32c: u32,
    chunk_group_count: u32,
    row_count: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct ChunkGroupEntry {
    pub metric_id: u64,
    group_offset: u64,
    group_size: u32,
    min_timestamp: i64,
    max_timestamp: i64,
    series_count: u32,
}

#[derive(Debug, Clone, Copy)]
struct ChunkGroupHeader {
    metric_id: u64,
    series_count: u32,
    series_index_crc32c: u32,
    min_timestamp: i64,
    max_timestamp: i64,
    series_index_offset: u64,
    series_index_size: u32,
}

impl ChunkGroupHeader {
    fn overlaps(&self, range: TimeRange) -> bool {
        self.max_timestamp >= range.start && self.min_timestamp < range.end
    }
}

#[derive(Debug, Clone, Copy)]
struct SeriesIndexRow {
    first_series_id: u64,
    last_series_id: u64,
    entry_count: u32,
    block_offset: u64,
    block_size: u32,
    crc32c: u32,
}

#[derive(Debug, Clone, Copy)]
struct SeriesEntry {
    series_id: u64,
    chunk_offset: u64,
    chunk_size: u32,
    min_timestamp: i64,
    max_timestamp: i64,
    min_sequence: u64,
    max_sequence: u64,
    flags: u16,
}

#[allow(dead_code)] // value statistics are consumed by the Phase 2 pruning layer
#[derive(Debug, Clone, Copy)]
struct PageEntry {
    min_timestamp: i64,
    max_timestamp: i64,
    first_value_bits: u64,
    last_value_bits: u64,
    min_value_bits: u64,
    max_value_bits: u64,
    min_sequence: u64,
    max_sequence: u64,
    page_offset: u64,
    page_size: u32,
    row_count: u32,
}

#[derive(Debug, Clone, Copy)]
struct SeriesChunkHeader {
    series_id: u64,
    sample_count: u64,
    page_count: u32,
    min_timestamp: i64,
    max_timestamp: i64,
    page_index_offset: u64,
    page_index_size: u32,
    page_index_crc32c: u32,
}

/// Sorts and deduplicates `rows` by `(metric_id, series_id, timestamp)`,
/// keeping the row with the highest `__seq__`, then encodes a TSF file.
pub fn encode_file(
    mut rows: Vec<Row>,
    partition: (i64, i64),
    opts: &WriterOptions,
) -> Result<(Vec<u8>, FileStats), String> {
    if rows.is_empty() {
        return Err("cannot write a TSF file without rows".to_string());
    }
    if opts.page_rows == 0 || opts.page_rows > MAX_PAGE_ROWS {
        return Err(format!(
            "page_rows must be in 1..={MAX_PAGE_ROWS}; got {}",
            opts.page_rows
        ));
    }
    super::compaction::dedup_lww(&mut rows);
    let deduped = rows;

    let min_sequence = deduped.iter().map(|r| r.sequence).min().unwrap();
    let max_sequence = deduped.iter().map(|r| r.sequence).max().unwrap();
    let row_count = deduped.len() as u64;

    let mut buf = vec![0u8; FILE_HEADER_SIZE];
    let mut groups = Vec::new();
    let mut series_ids = Vec::new();
    let mut i = 0;
    while i < deduped.len() {
        let metric_id = deduped[i].metric_id;
        let group_start = buf.len();
        buf.resize(group_start + CHUNK_GROUP_HEADER_SIZE, 0);
        let mut entries = Vec::new();
        let mut group_min_ts = i64::MAX;
        let mut group_max_ts = i64::MIN;
        while i < deduped.len() && deduped[i].metric_id == metric_id {
            let series_id = deduped[i].series_id;
            let start = i;
            while i < deduped.len()
                && deduped[i].metric_id == metric_id
                && deduped[i].series_id == series_id
            {
                i += 1;
            }
            let entry = encode_series_chunk(&mut buf, &deduped[start..i], opts)?;
            group_min_ts = group_min_ts.min(entry.min_timestamp);
            group_max_ts = group_max_ts.max(entry.max_timestamp);
            series_ids.push(series_id);
            entries.push(entry);
        }
        let (series_index_offset, series_index_size, series_index_crc) =
            write_series_index(&mut buf, &entries)?;
        write_chunk_group_header(
            &mut buf[group_start..group_start + CHUNK_GROUP_HEADER_SIZE],
            metric_id,
            entries.len() as u32,
            group_min_ts,
            group_max_ts,
            series_index_offset,
            series_index_size,
            series_index_crc,
        );
        let group_size = buf.len() - group_start;
        groups.push(ChunkGroupEntry {
            metric_id,
            group_offset: group_start as u64,
            group_size: group_size as u32,
            min_timestamp: group_min_ts,
            max_timestamp: group_max_ts,
            series_count: entries.len() as u32,
        });
    }

    let filter = bloom::build(&series_ids);
    let bloom_offset = buf.len() as u64;
    let (bloom_size, bloom_crc) = if let Some(filter) = &filter {
        buf.extend_from_slice(filter);
        (filter.len() as u32, crc32c(filter))
    } else {
        (0, 0)
    };

    let file_index = encode_chunk_group_entries(&groups);
    let file_index_offset = buf.len() as u64;
    let file_index_size = file_index.len() as u32;
    let file_index_crc = crc32c(&file_index);
    buf.extend_from_slice(&file_index);

    let mut footer = [0u8; FOOTER_SIZE];
    footer[0..4].copy_from_slice(MAGIC);
    put_u16(&mut footer, 4, FORMAT_VERSION);
    footer[6..8].copy_from_slice(&0u16.to_le_bytes());
    put_u64(&mut footer, 8, file_index_offset);
    put_u32(&mut footer, 16, file_index_size);
    put_u64(&mut footer, 20, bloom_offset);
    put_u32(&mut footer, 28, bloom_size);
    put_u32(&mut footer, 32, groups.len() as u32);
    put_u64(&mut footer, 36, row_count);
    put_u32(&mut footer, 44, file_index_crc);
    put_u32(&mut footer, 48, bloom_crc);
    let footer_crc = crc32c(&footer[..52]);
    put_u32(&mut footer, 52, footer_crc);
    buf.extend_from_slice(&footer);

    let mut flags = 1u16 << 1; // compressed file index
    if filter.is_some() {
        flags |= 1;
    }
    write_file_header(
        &mut buf[..FILE_HEADER_SIZE],
        flags,
        partition,
        groups.len() as u32,
        series_ids.len() as u64,
        row_count,
        min_sequence,
        max_sequence,
    );

    let stats = FileStats {
        bytes: buf.len() as u64,
        rows: row_count,
        series: series_ids.len() as u64,
        groups: groups.len() as u64,
    };
    Ok((buf, stats))
}

/// Encodes a file and writes it to `path`, fsyncing before returning.
pub fn write_file(
    path: impl AsRef<Path>,
    rows: Vec<Row>,
    partition: (i64, i64),
    opts: &WriterOptions,
) -> Result<FileStats, String> {
    let (bytes, stats) = encode_file(rows, partition, opts)?;
    std::fs::write(path.as_ref(), &bytes)
        .map_err(|e| format!("cannot write {:?}: {e}", path.as_ref()))?;
    // `std::fs::write` does not fsync; do it explicitly so the benchmark
    // measures a durable write.
    let file = std::fs::OpenOptions::new()
        .read(true)
        .open(path.as_ref())
        .map_err(|e| format!("cannot reopen {:?}: {e}", path.as_ref()))?;
    file.sync_all()
        .map_err(|e| format!("cannot fsync {:?}: {e}", path.as_ref()))?;
    Ok(stats)
}

/// A TSF file opened for reads.
pub struct TsfReader {
    /// Byte ranges of the file that were fetched, in ascending `offset` order.
    /// A whole-file reader has exactly one segment covering the entire file.
    segments: Vec<Segment>,
    /// Total file length, needed to validate offsets and to derive identity.
    len: u64,
    header: FileHeader,
    groups: Vec<ChunkGroupEntry>,
    bloom: Option<Vec<u8>>,
    /// Identity used as the index-cache key prefix. Files are immutable, so a
    /// rewrite produces a different header CRC and/or size.
    identity: u64,
}

/// Layout of a TSF file's index section, parsed from its footer.
///
/// A ranged reader fetches this first (the last [`FOOTER_SIZE`] bytes of the
/// file), then the header and the index ranges it names, and only afterwards
/// the chunk groups a query actually touches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FooterLayout {
    pub file_index_offset: u64,
    pub file_index_size: u32,
    pub bloom_offset: u64,
    pub bloom_size: u32,
    pub chunk_group_count: u32,
    pub row_count: u64,
}

impl FooterLayout {
    /// Byte ranges to fetch after the footer so that
    /// [`TsfReader::from_segments`] can rebuild the reader: the file header,
    /// the bloom filter (when the file has one) and the file index, plus the
    /// footer itself. Adjacent ranges are merged.
    ///
    /// Fetching these and nothing else gives a reader that can answer
    /// [`TsfReader::missing_ranges_for_metrics`], i.e. that can tell a caller
    /// exactly which chunk groups a query needs.
    pub fn index_ranges(&self, file_len: u64) -> Vec<(u64, u64)> {
        let mut ranges = vec![(0u64, FILE_HEADER_SIZE as u64)];
        if self.bloom_size > 0 {
            ranges.push((
                self.bloom_offset,
                self.bloom_offset + self.bloom_size as u64,
            ));
        }
        ranges.push((
            self.file_index_offset,
            self.file_index_offset + self.file_index_size as u64,
        ));
        let footer_start = file_len.saturating_sub(FOOTER_SIZE as u64);
        ranges.push((footer_start, file_len));
        ranges.sort_unstable();
        let mut merged: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
        for (start, end) in ranges {
            if start >= end {
                continue;
            }
            match merged.last_mut() {
                Some(last) if start <= last.1 => last.1 = last.1.max(end),
                _ => merged.push((start, end)),
            }
        }
        merged
    }
}

/// One contiguous run of bytes that a reader holds.
#[derive(Clone)]
struct Segment {
    offset: u64,
    data: Vec<u8>,
}

impl Segment {
    fn end(&self) -> u64 {
        self.offset + self.data.len() as u64
    }
}

impl TsfReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        let data = std::fs::read(path.as_ref())
            .map_err(|e| format!("cannot read {:?}: {e}", path.as_ref()))?;
        Self::from_bytes(data)
    }

    pub fn from_bytes(data: Vec<u8>) -> Result<Self, String> {
        Self::from_segments(vec![(0, data)])
    }

    /// Builds a reader from arbitrary byte ranges of a file of length `len`.
    ///
    /// Ranges may be given in any order and must not overlap; gaps are allowed.
    /// Every access that touches a gap fails with an error naming the missing
    /// range, so a caller that under-fetches gets a clear failure rather than
    /// wrong data. This is what the object-store query path uses: fetch the
    /// tail (footer + file index), then only the chunks the predicate needs.
    pub fn from_segments<I>(segments: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = (u64, Vec<u8>)>,
    {
        let mut segments = segments
            .into_iter()
            .map(|(offset, data)| Segment { offset, data })
            .collect::<Vec<_>>();
        segments.sort_unstable_by_key(|s| s.offset);
        for pair in segments.windows(2) {
            if pair[0].end() > pair[1].offset {
                return Err("TSF reader segments overlap".to_string());
            }
        }
        let len = segments.last().map(|s| s.end()).unwrap_or(0);

        let header_bytes = read_segments(&segments, 0, FILE_HEADER_SIZE as u64)?;
        let header = parse_file_header(&header_bytes)?;

        let footer_offset = len
            .checked_sub(FOOTER_SIZE as u64)
            .ok_or_else(|| format!("TSF file is too small: {len} bytes"))?;
        let footer_bytes = read_segments(&segments, footer_offset, FOOTER_SIZE as u64)?;
        let footer = parse_footer(&footer_bytes, len)?;

        if footer.chunk_group_count != header.chunk_group_count {
            return Err(format!(
                "chunk group count mismatch: header={} footer={}",
                header.chunk_group_count, footer.chunk_group_count
            ));
        }
        let file_index = decompress_at(
            &segments,
            footer.file_index_offset,
            footer.file_index_size,
            0,
            "file index",
        )?;
        let groups = decode_chunk_group_entries(&file_index)?;
        if groups.len() != header.chunk_group_count as usize {
            return Err("file index entry count mismatch".to_string());
        }
        if footer.row_count != header.row_count {
            return Err("row count mismatch between header and footer".to_string());
        }
        let bloom = if header.flags & 1 != 0 {
            if footer.bloom_size == 0 {
                return Err("bloom flag set but bloom size is zero".to_string());
            }
            let filter = read_segments(&segments, footer.bloom_offset, footer.bloom_size as u64)?;
            if crc32c(&filter) != footer.bloom_crc32c {
                return Err("bloom filter CRC32C mismatch".to_string());
            }
            Some(filter.into_owned())
        } else {
            None
        };
        // The header CRC changes whenever the file contents change, and the
        // length disambiguates CRC collisions; together they form a stable
        // identity for the immutable file.
        let header_crc = u32::from_le_bytes(header_bytes[124..128].try_into().unwrap());
        let identity = ((header_crc as u64) << 32) | (len as u32 as u64);
        Ok(Self {
            segments,
            len,
            header,
            groups,
            bloom,
            identity,
        })
    }

    /// Total length of the file this reader was built from.
    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Byte ranges that the reader already holds, merged and in ascending
    /// order. Callers use this to see what they would still need to fetch.
    pub fn loaded_ranges(&self) -> Vec<(u64, u64)> {
        let mut out: Vec<(u64, u64)> = Vec::new();
        for segment in &self.segments {
            match out.last_mut() {
                Some(last) if last.1 == segment.offset => last.1 = segment.end(),
                _ => out.push((segment.offset, segment.end())),
            }
        }
        out
    }

    /// Parses the footer that lives in the last [`FOOTER_SIZE`] bytes of a file
    /// of `file_len` bytes.
    ///
    /// This is the entry point for ranged reads: it tells a caller which byte
    /// ranges hold the header, the file index and the bloom filter, so it can
    /// fetch those instead of the whole file.
    pub fn footer_layout(footer: &[u8], file_len: u64) -> Result<FooterLayout, String> {
        let footer = parse_footer(footer, file_len)?;
        Ok(FooterLayout {
            file_index_offset: footer.file_index_offset,
            file_index_size: footer.file_index_size,
            bloom_offset: footer.bloom_offset,
            bloom_size: footer.bloom_size,
            chunk_group_count: footer.chunk_group_count,
            row_count: footer.row_count,
        })
    }

    /// True when `[offset, offset + size)` is already held.
    pub fn has_range(&self, offset: u64, size: u64) -> bool {
        self.subtract_loaded(vec![(offset, offset + size)])
            .is_empty()
    }

    /// The next byte ranges a caller must fetch to plan a metric scan, or
    /// `None` when every page range is known.
    ///
    /// Walks the file metadata one level at a time, so a ranged reader can plan
    /// a scan with a bounded number of small fetches instead of downloading the
    /// file:
    ///
    /// 1. chunk group header (48 B) — gives the series index location;
    /// 2. series index (40 B per block row) and series index blocks (56 B per
    ///    series) — give each series' time and sequence bounds;
    /// 3. series chunk headers (96 B) — give each page index location;
    /// 4. page indexes (80 B per page) — give each page's time and sequence
    ///    bounds plus its byte range.
    ///
    /// After each call the caller fetches the ranges, rebuilds the reader with
    /// them, and calls again. Every level only requests ranges that overlap the
    /// query, so a narrow scan touches few series and, in the last level, few
    /// pages (see [`TsfReader::page_ranges_for_metrics`]).
    ///
    /// `series` restricts the plan to a set of series IDs: series-index blocks
    /// that cover no filtered ID and series chunks for other series are never
    /// requested. `None` plans for every series of the metric.
    pub fn next_scan_ranges(
        &self,
        metrics: &[(u64, TimeRange)],
        series: Option<&SeriesFilter>,
    ) -> Option<Vec<(u64, u64)>> {
        let mut wanted: Vec<(u64, u64)> = Vec::new();
        let note = |wanted: &mut Vec<(u64, u64)>, offset: u64, size: u64| {
            let end = offset + size;
            wanted.extend(self.subtract_loaded(vec![(offset, end)]));
        };
        for &(metric_id, range) in metrics {
            let Ok(Some(group)) = self.group_for(metric_id) else {
                continue;
            };
            if group.min_timestamp > range.end || group.max_timestamp < range.start {
                continue;
            }
            let group_header_size = CHUNK_GROUP_HEADER_SIZE as u64;
            if !self.has_range(group.group_offset, group_header_size) {
                note(&mut wanted, group.group_offset, group_header_size);
                continue;
            }
            let Ok(header) = self.chunk_group_header(&group) else {
                continue;
            };
            if !header.overlaps(range) {
                continue;
            }
            if !self.has_range(header.series_index_offset, header.series_index_size as u64) {
                note(
                    &mut wanted,
                    header.series_index_offset,
                    header.series_index_size as u64,
                );
                continue;
            }
            let Ok(rows) = self.series_index_rows(&header) else {
                continue;
            };
            for row in &rows {
                if let Some(filter) = series {
                    if !filter.intersects_block(row.first_series_id, row.last_series_id) {
                        continue;
                    }
                }
                if !self.has_range(row.block_offset, row.block_size as u64) {
                    note(&mut wanted, row.block_offset, row.block_size as u64);
                }
            }
            if !wanted.is_empty() {
                continue;
            }
            for row in &rows {
                let Ok(entries) = self.series_block(row) else {
                    continue;
                };
                for entry in entries {
                    if entry.max_timestamp < range.start || entry.min_timestamp >= range.end {
                        continue;
                    }
                    if let Some(filter) = series {
                        if !filter.contains(entry.series_id) {
                            continue;
                        }
                    }
                    let chunk_header_size = SERIES_CHUNK_HEADER_SIZE as u64;
                    if !self.has_range(entry.chunk_offset, chunk_header_size) {
                        note(&mut wanted, entry.chunk_offset, chunk_header_size);
                        continue;
                    }
                    let Ok(chunk) = self.series_chunk_header(&entry) else {
                        continue;
                    };
                    if !self.has_range(chunk.page_index_offset, chunk.page_index_size as u64) {
                        note(
                            &mut wanted,
                            chunk.page_index_offset,
                            chunk.page_index_size as u64,
                        );
                    }
                }
            }
        }
        if wanted.is_empty() {
            return None;
        }
        wanted.sort_unstable();
        let mut merged: Vec<(u64, u64)> = Vec::with_capacity(wanted.len());
        for (start, end) in wanted {
            if start >= end {
                continue;
            }
            match merged.last_mut() {
                Some(last) if start <= last.1 => last.1 = last.1.max(end),
                _ => merged.push((start, end)),
            }
        }
        Some(merged)
    }

    /// Byte ranges of the pages that a metric scan must decode.
    ///
    /// Requires the metadata that [`TsfReader::next_scan_ranges`] asks for to
    /// be loaded. Pages whose time bounds miss `range` are skipped, so this is
    /// the finest pruning the format allows. `series` skips the chunks of every
    /// series the query did not ask for.
    pub fn page_ranges_for_metrics(
        &self,
        metrics: &[(u64, TimeRange)],
        series: Option<&SeriesFilter>,
    ) -> Vec<(u64, u64)> {
        let mut wanted: Vec<(u64, u64)> = Vec::new();
        for &(metric_id, range) in metrics {
            let Ok(Some(group)) = self.group_for(metric_id) else {
                continue;
            };
            let Ok(header) = self.chunk_group_header(&group) else {
                continue;
            };
            if !header.overlaps(range) {
                continue;
            }
            let Ok(entries) = self.series_entries(&header) else {
                continue;
            };
            for entry in &entries {
                if entry.max_timestamp < range.start || entry.min_timestamp >= range.end {
                    continue;
                }
                if let Some(filter) = series {
                    if !filter.contains(entry.series_id) {
                        continue;
                    }
                }
                let Ok(chunk) = self.series_chunk_header(entry) else {
                    continue;
                };
                let Ok(raw) = decompress_at(
                    &self.segments,
                    chunk.page_index_offset,
                    chunk.page_index_size,
                    chunk.page_index_crc32c,
                    "page index",
                ) else {
                    continue;
                };
                for bytes in raw.as_chunks::<PAGE_ENTRY_SIZE>().0 {
                    let min_timestamp = get_i64(bytes, 0);
                    let max_timestamp = get_i64(bytes, 8);
                    if max_timestamp < range.start || min_timestamp >= range.end {
                        continue;
                    }
                    let offset = get_u64(bytes, 64);
                    let size = get_u32(bytes, 72) as u64;
                    wanted.push((offset, offset + size));
                }
            }
        }
        wanted.sort_unstable();
        let mut merged: Vec<(u64, u64)> = Vec::with_capacity(wanted.len());
        for (start, end) in wanted {
            if start >= end {
                continue;
            }
            match merged.last_mut() {
                Some(last) if start <= last.1 => last.1 = last.1.max(end),
                _ => merged.push((start, end)),
            }
        }
        merged
    }

    /// Subtracts ranges the reader already holds from `wanted`.
    fn subtract_loaded(&self, wanted: Vec<(u64, u64)>) -> Vec<(u64, u64)> {
        let loaded = self.loaded_ranges();
        let mut out = Vec::new();
        for (mut start, end) in wanted {
            for &(loaded_start, loaded_end) in &loaded {
                if loaded_end <= start {
                    continue;
                }
                if loaded_start >= end {
                    break;
                }
                if loaded_start > start {
                    out.push((start, loaded_start));
                }
                start = start.max(loaded_end);
                if start >= end {
                    break;
                }
            }
            if start < end {
                out.push((start, end));
            }
        }
        out
    }

    pub fn file_identity(&self) -> u64 {
        self.identity
    }

    pub fn header(&self) -> &FileHeader {
        &self.header
    }

    pub fn metric_ids(&self) -> Vec<u64> {
        self.groups.iter().map(|g| g.metric_id).collect()
    }

    fn group_for(&self, metric_id: u64) -> Result<Option<ChunkGroupEntry>, String> {
        Ok(self
            .groups
            .binary_search_by_key(&metric_id, |g| g.metric_id)
            .ok()
            .map(|idx| self.groups[idx]))
    }

    fn chunk_group_header(&self, group: &ChunkGroupEntry) -> Result<ChunkGroupHeader, String> {
        let h = read_segments(
            &self.segments,
            group.group_offset,
            CHUNK_GROUP_HEADER_SIZE as u64,
        )?;
        let h = h.as_ref();
        if crc32c(&h[..44]) != u32::from_le_bytes(h[44..48].try_into().unwrap()) {
            return Err(format!(
                "chunk group header CRC mismatch for metric {}",
                group.metric_id
            ));
        }
        let header = ChunkGroupHeader {
            metric_id: get_u64(h, 0),
            series_count: get_u32(h, 8),
            series_index_crc32c: get_u32(h, 12),
            min_timestamp: get_i64(h, 16),
            max_timestamp: get_i64(h, 24),
            series_index_offset: get_u64(h, 32),
            series_index_size: get_u32(h, 40),
        };
        if header.metric_id != group.metric_id {
            return Err("chunk group header metric ID mismatch".to_string());
        }
        if header.series_count != group.series_count {
            return Err("chunk group header series count mismatch".to_string());
        }
        Ok(header)
    }

    fn series_index_rows(&self, group: &ChunkGroupHeader) -> Result<Vec<SeriesIndexRow>, String> {
        let raw = decompress_at(
            &self.segments,
            group.series_index_offset,
            group.series_index_size,
            group.series_index_crc32c,
            "series index",
        )?;
        if !raw.len().is_multiple_of(SERIES_INDEX_ROW_SIZE) {
            return Err("series index size is not a multiple of 40".to_string());
        }
        let mut rows = Vec::with_capacity(raw.len() / SERIES_INDEX_ROW_SIZE);
        for chunk in raw.as_chunks::<SERIES_INDEX_ROW_SIZE>().0 {
            rows.push(SeriesIndexRow {
                first_series_id: get_u64(chunk, 0),
                last_series_id: get_u64(chunk, 8),
                entry_count: get_u32(chunk, 16),
                block_offset: get_u64(chunk, 24),
                block_size: get_u32(chunk, 32),
                crc32c: get_u32(chunk, 36),
            });
        }
        Ok(rows)
    }

    fn series_block(&self, row: &SeriesIndexRow) -> Result<Vec<SeriesEntry>, String> {
        let raw = decompress_at(
            &self.segments,
            row.block_offset,
            row.block_size,
            row.crc32c,
            "series index block",
        )?;
        if !raw.len().is_multiple_of(SERIES_ENTRY_SIZE) {
            return Err("series index block size is not a multiple of 56".to_string());
        }
        let mut entries = Vec::with_capacity(raw.len() / SERIES_ENTRY_SIZE);
        for chunk in raw.as_chunks::<SERIES_ENTRY_SIZE>().0 {
            entries.push(SeriesEntry {
                series_id: get_u64(chunk, 0),
                chunk_offset: get_u64(chunk, 8),
                chunk_size: get_u32(chunk, 16),
                min_timestamp: get_i64(chunk, 20),
                max_timestamp: get_i64(chunk, 28),
                min_sequence: get_u64(chunk, 36),
                max_sequence: get_u64(chunk, 44),
                flags: get_u16(chunk, 52),
            });
        }
        if entries.len() != row.entry_count as usize {
            return Err("series index block entry count mismatch".to_string());
        }
        Ok(entries)
    }

    fn series_entries(&self, group: &ChunkGroupHeader) -> Result<Vec<SeriesEntry>, String> {
        let rows = self.series_index_rows(group)?;
        let mut entries = Vec::with_capacity(group.series_count as usize);
        for row in &rows {
            entries.extend(self.series_block(row)?);
        }
        Ok(entries)
    }

    fn series_index_rows_cached(
        &self,
        group: &ChunkGroupHeader,
        ctx: &mut ReadContext,
    ) -> Result<Arc<Vec<SeriesIndexRow>>, String> {
        let key = (self.identity, group.metric_id);
        if let Some(rows) = ctx.series_index.get(&key) {
            return Ok(rows);
        }
        let rows = self.series_index_rows(group)?;
        let bytes = rows.len() * SERIES_INDEX_ROW_SIZE;
        Ok(ctx.series_index.insert(key, rows, bytes))
    }

    fn series_block_cached(
        &self,
        group: &ChunkGroupHeader,
        row: &SeriesIndexRow,
        ctx: &mut ReadContext,
    ) -> Result<Arc<Vec<SeriesEntry>>, String> {
        let key = (self.identity, group.metric_id, row.first_series_id);
        if let Some(entries) = ctx.series_blocks.get(&key) {
            return Ok(entries);
        }
        let entries = self.series_block(row)?;
        let bytes = entries.len() * SERIES_ENTRY_SIZE;
        Ok(ctx.series_blocks.insert(key, entries, bytes))
    }

    fn page_index_cached(
        &self,
        metric_id: u64,
        series_id: u64,
        header: &SeriesChunkHeader,
        ctx: &mut ReadContext,
    ) -> Result<Arc<Vec<PageEntry>>, String> {
        let key = (self.identity, metric_id, series_id);
        if let Some(pages) = ctx.page_index.get(&key) {
            return Ok(pages);
        }
        let raw = decompress_at(
            &self.segments,
            header.page_index_offset,
            header.page_index_size,
            header.page_index_crc32c,
            "page index",
        )?;
        if !raw.len().is_multiple_of(PAGE_ENTRY_SIZE) {
            return Err("page index size is not a multiple of 80".to_string());
        }
        let mut pages = Vec::with_capacity(raw.len() / PAGE_ENTRY_SIZE);
        for chunk in raw.as_chunks::<PAGE_ENTRY_SIZE>().0 {
            pages.push(PageEntry {
                min_timestamp: get_i64(chunk, 0),
                max_timestamp: get_i64(chunk, 8),
                first_value_bits: get_u64(chunk, 16),
                last_value_bits: get_u64(chunk, 24),
                min_value_bits: get_u64(chunk, 32),
                max_value_bits: get_u64(chunk, 40),
                min_sequence: get_u64(chunk, 48),
                max_sequence: get_u64(chunk, 56),
                page_offset: get_u64(chunk, 64),
                page_size: get_u32(chunk, 72),
                row_count: get_u32(chunk, 76),
            });
        }
        if pages.len() != header.page_count as usize {
            return Err("page index entry count mismatch".to_string());
        }
        let indexed_rows: u64 = pages.iter().map(|p| p.row_count as u64).sum();
        if indexed_rows != header.sample_count {
            return Err("series chunk sample count mismatch".to_string());
        }
        let bytes = pages.len() * PAGE_ENTRY_SIZE;
        Ok(ctx.page_index.insert(key, pages, bytes))
    }

    fn series_entry_cached(
        &self,
        group: &ChunkGroupHeader,
        series_id: u64,
        ctx: &mut ReadContext,
    ) -> Result<Option<SeriesEntry>, String> {
        let rows = self.series_index_rows_cached(group, ctx)?;
        let pos = rows.partition_point(|r| r.first_series_id <= series_id);
        if pos == 0 {
            return Ok(None);
        }
        let row = &rows[pos - 1];
        if series_id > row.last_series_id {
            return Ok(None);
        }
        let entries = self.series_block_cached(group, row, ctx)?;
        Ok(entries
            .binary_search_by_key(&series_id, |e| e.series_id)
            .ok()
            .map(|idx| entries[idx]))
    }

    fn series_entries_cached(
        &self,
        group: &ChunkGroupHeader,
        ctx: &mut ReadContext,
    ) -> Result<Vec<SeriesEntry>, String> {
        let rows = self.series_index_rows_cached(group, ctx)?;
        let mut entries = Vec::with_capacity(group.series_count as usize);
        for row in rows.iter() {
            entries.extend(self.series_block_cached(group, row, ctx)?.iter().copied());
        }
        Ok(entries)
    }

    /// Parses and validates one series chunk header.
    fn series_chunk_header(&self, entry: &SeriesEntry) -> Result<SeriesChunkHeader, String> {
        let h = read_segments(
            &self.segments,
            entry.chunk_offset,
            SERIES_CHUNK_HEADER_SIZE as u64,
        )?;
        let h = h.as_ref();
        if crc32c(&h[..92]) != u32::from_le_bytes(h[92..96].try_into().unwrap()) {
            return Err(format!(
                "series chunk header CRC mismatch for series {}",
                entry.series_id
            ));
        }
        let header = SeriesChunkHeader {
            series_id: get_u64(h, 0),
            sample_count: get_u64(h, 8),
            page_count: get_u32(h, 16),
            min_timestamp: get_i64(h, 24),
            max_timestamp: get_i64(h, 32),
            page_index_offset: get_u64(h, 56),
            page_index_size: get_u32(h, 64),
            page_index_crc32c: get_u32(h, 84),
        };
        if header.series_id != entry.series_id {
            return Err("series chunk header ID mismatch".to_string());
        }
        if header.min_timestamp != entry.min_timestamp
            || header.max_timestamp != entry.max_timestamp
        {
            return Err("series chunk header timestamp range mismatch".to_string());
        }
        Ok(header)
    }

    fn series_entry(
        &self,
        group: &ChunkGroupHeader,
        series_id: u64,
    ) -> Result<Option<SeriesEntry>, String> {
        let rows = self.series_index_rows(group)?;
        let pos = rows.partition_point(|r| r.first_series_id <= series_id);
        if pos == 0 {
            return Ok(None);
        }
        let row = &rows[pos - 1];
        if series_id > row.last_series_id {
            return Ok(None);
        }
        let entries = self.series_block(row)?;
        Ok(entries
            .binary_search_by_key(&series_id, |e| e.series_id)
            .ok()
            .map(|idx| entries[idx]))
    }

    /// Returns the series IDs of one metric.
    pub fn series_ids(&self, metric_id: u64) -> Result<Vec<u64>, String> {
        let Some(group) = self.group_for(metric_id)? else {
            return Ok(Vec::new());
        };
        let header = self.chunk_group_header(&group)?;
        Ok(self
            .series_entries(&header)?
            .into_iter()
            .map(|e| e.series_id)
            .collect())
    }

    /// Reads one series in `[range.start, range.end)`.
    pub fn read_series(
        &self,
        metric_id: u64,
        series_id: u64,
        range: TimeRange,
        sequence_range: SequenceRange,
    ) -> Result<Vec<Row>, String> {
        if let Some(filter) = &self.bloom {
            if !bloom::might_contain(filter, series_id) {
                return Ok(Vec::new());
            }
        }
        let Some(group) = self.group_for(metric_id)? else {
            return Ok(Vec::new());
        };
        let header = self.chunk_group_header(&group)?;
        if !header.overlaps(range) {
            return Ok(Vec::new());
        }
        let Some(entry) = self.series_entry(&header, series_id)? else {
            return Ok(Vec::new());
        };
        self.read_series_entry(metric_id, &entry, range, sequence_range)
    }

    /// Reads projected columns for one series, reusing `ctx` buffers and
    /// index caches.
    ///
    /// Timestamps are decoded internally even when not requested, because a
    /// boundary page is sliced by `partition_point`; only requested columns
    /// are returned. Sequence predicates are not applied here (they are not
    /// monotonic); compaction keeps the [`Row`] path.
    pub fn read_series_columns(
        &self,
        metric_id: u64,
        series_id: u64,
        range: TimeRange,
        columns: ColumnMask,
        ctx: &mut ReadContext,
    ) -> Result<DecodedColumns, String> {
        let mut out = DecodedColumns::new(metric_id, series_id, columns);
        if let Some(filter) = &self.bloom {
            if !bloom::might_contain(filter, series_id) {
                return Ok(out);
            }
        }
        let Some(group) = self.group_for(metric_id)? else {
            return Ok(out);
        };
        let header = self.chunk_group_header(&group)?;
        if !header.overlaps(range) {
            return Ok(out);
        }
        let Some(entry) = self.series_entry_cached(&header, series_id, ctx)? else {
            return Ok(out);
        };
        self.read_entry_columns(metric_id, &entry, range, columns, ctx, &mut out)?;
        Ok(out)
    }

    /// Reads projected columns for every series of one metric, in
    /// `series_id` order.
    pub fn read_metric_columns(
        &self,
        metric_id: u64,
        range: TimeRange,
        columns: ColumnMask,
        ctx: &mut ReadContext,
    ) -> Result<Vec<DecodedColumns>, String> {
        let Some(group) = self.group_for(metric_id)? else {
            return Ok(Vec::new());
        };
        let header = self.chunk_group_header(&group)?;
        if !header.overlaps(range) {
            return Ok(Vec::new());
        }
        let entries = self.series_entries_cached(&header, ctx)?;
        let mut out = Vec::new();
        for entry in &entries {
            if entry.max_timestamp < range.start || entry.min_timestamp >= range.end {
                continue;
            }
            let mut columns_out = DecodedColumns::new(metric_id, entry.series_id, columns);
            self.read_entry_columns(metric_id, entry, range, columns, ctx, &mut columns_out)?;
            if !columns_out.is_empty() {
                out.push(columns_out);
            }
        }
        Ok(out)
    }

    /// Reads projected columns for every row in the file.
    pub fn read_all_columns(
        &self,
        columns: ColumnMask,
        ctx: &mut ReadContext,
    ) -> Result<Vec<DecodedColumns>, String> {
        let mut out = Vec::new();
        for group in &self.groups {
            out.extend(self.read_metric_columns(
                group.metric_id,
                TimeRange::new(i64::MIN, i64::MAX),
                columns,
                ctx,
            )?);
        }
        Ok(out)
    }

    fn read_entry_columns(
        &self,
        metric_id: u64,
        entry: &SeriesEntry,
        range: TimeRange,
        columns: ColumnMask,
        ctx: &mut ReadContext,
        out: &mut DecodedColumns,
    ) -> Result<(), String> {
        if entry.max_timestamp < range.start || entry.min_timestamp >= range.end {
            return Ok(());
        }
        let header = self.series_chunk_header(entry)?;
        let pages = self.page_index_cached(metric_id, entry.series_id, &header, ctx)?;
        let decode_mask = columns | ColumnMask::TIMESTAMP;
        for page in pages.iter() {
            if page.max_timestamp < range.start || page.min_timestamp >= range.end {
                continue;
            }
            let bytes = read_segments(&self.segments, page.page_offset, page.page_size as u64)?;
            decode_page_into(&bytes, decode_mask, &mut ctx.buffers)?;
            if ctx.buffers.row_count != page.row_count as usize {
                return Err("decoded page row count mismatch".to_string());
            }
            let selected = contiguous_time_slice(&ctx.buffers.timestamps, range.start, range.end);
            if selected.is_empty() {
                continue;
            }
            if columns.contains(ColumnMask::TIMESTAMP) {
                out.timestamps
                    .extend_from_slice(&ctx.buffers.timestamps[selected.clone()]);
            }
            if columns.contains(ColumnMask::VALUE) {
                out.values
                    .extend_from_slice(&ctx.buffers.values[selected.clone()]);
            }
            if columns.contains(ColumnMask::SEQUENCE) {
                out.sequences
                    .extend_from_slice(&ctx.buffers.sequences[selected.clone()]);
            }
            if columns.contains(ColumnMask::RESERVED) {
                out.reserved
                    .extend_from_slice(&ctx.buffers.reserved[selected.clone()]);
            }
        }
        Ok(())
    }

    /// Reads every series of one metric in `[range.start, range.end)`.
    pub fn read_metric(
        &self,
        metric_id: u64,
        range: TimeRange,
        sequence_range: SequenceRange,
    ) -> Result<Vec<Row>, String> {
        let Some(group) = self.group_for(metric_id)? else {
            return Ok(Vec::new());
        };
        let header = self.chunk_group_header(&group)?;
        if !header.overlaps(range) {
            return Ok(Vec::new());
        }
        let mut rows = Vec::new();
        for entry in self.series_entries(&header)? {
            rows.extend(self.read_series_entry(metric_id, &entry, range, sequence_range)?);
        }
        Ok(rows)
    }

    /// Reads every row in the file.
    pub fn read_all(&self) -> Result<Vec<Row>, String> {
        let mut rows = Vec::new();
        for group in &self.groups {
            rows.extend(self.read_metric(
                group.metric_id,
                TimeRange::new(i64::MIN, i64::MAX),
                None,
            )?);
        }
        Ok(rows)
    }

    fn read_series_entry(
        &self,
        metric_id: u64,
        entry: &SeriesEntry,
        range: TimeRange,
        sequence_range: SequenceRange,
    ) -> Result<Vec<Row>, String> {
        if entry.max_timestamp < range.start || entry.min_timestamp >= range.end {
            return Ok(Vec::new());
        }
        if let Some((seq_start, seq_end)) = sequence_range {
            if entry.max_sequence < seq_start || entry.min_sequence >= seq_end {
                return Ok(Vec::new());
            }
        }
        let header = self.series_chunk_header(entry)?;
        let raw = decompress_at(
            &self.segments,
            header.page_index_offset,
            header.page_index_size,
            header.page_index_crc32c,
            "page index",
        )?;
        if !raw.len().is_multiple_of(PAGE_ENTRY_SIZE) {
            return Err("page index size is not a multiple of 80".to_string());
        }
        let mut pages = Vec::with_capacity(raw.len() / PAGE_ENTRY_SIZE);
        for chunk in raw.as_chunks::<PAGE_ENTRY_SIZE>().0 {
            pages.push(PageEntry {
                min_timestamp: get_i64(chunk, 0),
                max_timestamp: get_i64(chunk, 8),
                first_value_bits: get_u64(chunk, 16),
                last_value_bits: get_u64(chunk, 24),
                min_value_bits: get_u64(chunk, 32),
                max_value_bits: get_u64(chunk, 40),
                min_sequence: get_u64(chunk, 48),
                max_sequence: get_u64(chunk, 56),
                page_offset: get_u64(chunk, 64),
                page_size: get_u32(chunk, 72),
                row_count: get_u32(chunk, 76),
            });
        }
        if pages.len() != header.page_count as usize {
            return Err("page index entry count mismatch".to_string());
        }
        let indexed_rows: u64 = pages.iter().map(|p| p.row_count as u64).sum();
        if indexed_rows != header.sample_count {
            return Err("series chunk sample count mismatch".to_string());
        }

        let mut rows = Vec::new();
        for page in &pages {
            if page.max_timestamp < range.start || page.min_timestamp >= range.end {
                continue;
            }
            if let Some((seq_start, seq_end)) = sequence_range {
                if page.max_sequence < seq_start || page.min_sequence >= seq_end {
                    continue;
                }
            }
            let bytes = read_segments(&self.segments, page.page_offset, page.page_size as u64)?;
            let decoded = decode_page(&bytes)?;
            if decoded.len() != page.row_count as usize {
                return Err("decoded page row count mismatch".to_string());
            }
            for i in 0..decoded.len() {
                let ts = decoded.timestamps[i];
                if !range.contains(ts) {
                    continue;
                }
                let seq = decoded.sequences[i];
                if let Some((seq_start, seq_end)) = sequence_range {
                    if seq < seq_start || seq >= seq_end {
                        continue;
                    }
                }
                rows.push(Row {
                    metric_id,
                    series_id: entry.series_id,
                    timestamp: ts,
                    value: decoded.values[i],
                    sequence: seq,
                    reserved: decoded.reserved[i],
                });
            }
        }
        Ok(rows)
    }
}

fn encode_series_chunk(
    buf: &mut Vec<u8>,
    rows: &[Row],
    opts: &WriterOptions,
) -> Result<SeriesEntry, String> {
    let chunk_start = buf.len();
    buf.resize(chunk_start + SERIES_CHUNK_HEADER_SIZE, 0);
    let data_offset = buf.len();
    let mut pages = Vec::new();
    let mut has_nan = false;
    let mut has_duplicates = false;
    for chunk in rows.chunks(opts.page_rows) {
        let (bytes, mut meta) = encode_page(chunk)?;
        meta.page_offset = buf.len() as u64;
        has_nan |= meta.has_nan;
        has_duplicates |= meta.has_duplicates;
        buf.extend_from_slice(&bytes);
        pages.push(meta);
    }
    let data_size = buf.len() - data_offset;
    let page_index_offset = buf.len();
    let page_index = encode_page_index(&pages);
    let page_index_crc = crc32c(&page_index);
    buf.extend_from_slice(&page_index);
    let chunk_size = (buf.len() - chunk_start) as u32;

    let mut h = [0u8; SERIES_CHUNK_HEADER_SIZE];
    put_u64(&mut h, 0, rows[0].series_id);
    put_u64(&mut h, 8, rows.len() as u64);
    put_u32(&mut h, 16, pages.len() as u32);
    let mut flags = 0u16;
    if has_nan {
        flags |= 1;
    }
    if has_duplicates {
        flags |= 2;
    }
    put_u16(&mut h, 20, flags);
    put_i64(&mut h, 24, rows.first().unwrap().timestamp);
    put_i64(&mut h, 32, rows.last().unwrap().timestamp);
    put_u64(&mut h, 40, rows.iter().map(|r| r.sequence).min().unwrap());
    put_u64(&mut h, 48, rows.iter().map(|r| r.sequence).max().unwrap());
    put_u64(&mut h, 56, page_index_offset as u64);
    put_u32(&mut h, 64, page_index.len() as u32);
    put_u64(&mut h, 68, data_offset as u64);
    put_u64(&mut h, 76, data_size as u64);
    put_u32(&mut h, 84, page_index_crc);
    let crc = crc32c(&h[..92]);
    put_u32(&mut h, 92, crc);
    buf[chunk_start..chunk_start + SERIES_CHUNK_HEADER_SIZE].copy_from_slice(&h);

    Ok(SeriesEntry {
        series_id: rows[0].series_id,
        chunk_offset: chunk_start as u64,
        chunk_size,
        min_timestamp: rows.first().unwrap().timestamp,
        max_timestamp: rows.last().unwrap().timestamp,
        min_sequence: rows.iter().map(|r| r.sequence).min().unwrap(),
        max_sequence: rows.iter().map(|r| r.sequence).max().unwrap(),
        flags,
    })
}

fn write_series_index(
    buf: &mut Vec<u8>,
    entries: &[SeriesEntry],
) -> Result<(u64, u32, u32), String> {
    let mut rows = Vec::new();
    for block in entries.chunks(SERIES_INDEX_BLOCK_ENTRIES) {
        let mut raw = Vec::with_capacity(block.len() * SERIES_ENTRY_SIZE);
        for entry in block {
            encode_series_entry(&mut raw, entry);
        }
        let mut compressed = Vec::new();
        compress_zstd_level(&mut compressed, &raw, 3);
        let block_offset = buf.len() as u64;
        let block_size = compressed.len() as u32;
        let crc = crc32c(&compressed);
        buf.extend_from_slice(&compressed);
        rows.push(SeriesIndexRow {
            first_series_id: block.first().unwrap().series_id,
            last_series_id: block.last().unwrap().series_id,
            entry_count: block.len() as u32,
            block_offset,
            block_size,
            crc32c: crc,
        });
    }
    let mut raw = Vec::with_capacity(rows.len() * SERIES_INDEX_ROW_SIZE);
    for row in &rows {
        encode_series_index_row(&mut raw, row);
    }
    let mut compressed = Vec::new();
    compress_zstd_level(&mut compressed, &raw, 3);
    let offset = buf.len() as u64;
    let size = compressed.len() as u32;
    let crc = crc32c(&compressed);
    buf.extend_from_slice(&compressed);
    Ok((offset, size, crc))
}

#[allow(clippy::too_many_arguments)]
fn write_chunk_group_header(
    dst: &mut [u8],
    metric_id: u64,
    series_count: u32,
    min_timestamp: i64,
    max_timestamp: i64,
    series_index_offset: u64,
    series_index_size: u32,
    series_index_crc: u32,
) {
    put_u64(dst, 0, metric_id);
    put_u32(dst, 8, series_count);
    put_u32(dst, 12, series_index_crc);
    put_i64(dst, 16, min_timestamp);
    put_i64(dst, 24, max_timestamp);
    put_u64(dst, 32, series_index_offset);
    put_u32(dst, 40, series_index_size);
    let crc = crc32c(&dst[..44]);
    put_u32(dst, 44, crc);
}

#[allow(clippy::too_many_arguments)]
fn write_file_header(
    dst: &mut [u8],
    flags: u16,
    partition: (i64, i64),
    chunk_group_count: u32,
    series_count: u64,
    row_count: u64,
    min_sequence: u64,
    max_sequence: u64,
) {
    dst[0..4].copy_from_slice(MAGIC);
    put_u16(dst, 4, FORMAT_VERSION);
    put_u16(dst, 6, flags);
    put_u32(dst, 8, FILE_HEADER_SIZE as u32);
    put_u32(dst, 12, SCHEMA_ID);
    put_i64(dst, 16, partition.0);
    put_i64(dst, 24, partition.1);
    put_u32(dst, 32, chunk_group_count);
    put_u32(dst, 36, 0);
    put_u64(dst, 40, series_count);
    put_u64(dst, 48, row_count);
    put_u64(dst, 56, min_sequence);
    put_u64(dst, 64, max_sequence);
    put_i64(dst, 72, now_ms());
    put_u32(dst, 80, 1);
    let crc = crc32c(&dst[..124]);
    put_u32(dst, 124, crc);
}

fn encode_page_index(pages: &[PageMeta]) -> Vec<u8> {
    let mut raw = Vec::with_capacity(pages.len() * PAGE_ENTRY_SIZE);
    for page in pages {
        let mut e = [0u8; PAGE_ENTRY_SIZE];
        put_i64(&mut e, 0, page.min_timestamp);
        put_i64(&mut e, 8, page.max_timestamp);
        put_u64(&mut e, 16, page.first_value_bits);
        put_u64(&mut e, 24, page.last_value_bits);
        put_u64(&mut e, 32, page.min_value_bits);
        put_u64(&mut e, 40, page.max_value_bits);
        put_u64(&mut e, 48, page.min_sequence);
        put_u64(&mut e, 56, page.max_sequence);
        put_u64(&mut e, 64, page.page_offset);
        put_u32(&mut e, 72, page.page_size);
        put_u32(&mut e, 76, page.row_count);
        raw.extend_from_slice(&e);
    }
    let mut compressed = Vec::new();
    compress_zstd_level(&mut compressed, &raw, 3);
    compressed
}

fn encode_chunk_group_entries(groups: &[ChunkGroupEntry]) -> Vec<u8> {
    let mut raw = Vec::with_capacity(groups.len() * CHUNK_GROUP_ENTRY_SIZE);
    for group in groups {
        let mut e = [0u8; CHUNK_GROUP_ENTRY_SIZE];
        put_u64(&mut e, 0, group.metric_id);
        put_u64(&mut e, 8, group.group_offset);
        put_u32(&mut e, 16, group.group_size);
        put_i64(&mut e, 20, group.min_timestamp);
        put_i64(&mut e, 28, group.max_timestamp);
        put_u32(&mut e, 36, group.series_count);
        raw.extend_from_slice(&e);
    }
    let mut compressed = Vec::new();
    compress_zstd_level(&mut compressed, &raw, 3);
    compressed
}

fn encode_series_entry(dst: &mut Vec<u8>, entry: &SeriesEntry) {
    let mut e = [0u8; SERIES_ENTRY_SIZE];
    put_u64(&mut e, 0, entry.series_id);
    put_u64(&mut e, 8, entry.chunk_offset);
    put_u32(&mut e, 16, entry.chunk_size);
    put_i64(&mut e, 20, entry.min_timestamp);
    put_i64(&mut e, 28, entry.max_timestamp);
    put_u64(&mut e, 36, entry.min_sequence);
    put_u64(&mut e, 44, entry.max_sequence);
    put_u16(&mut e, 52, entry.flags);
    dst.extend_from_slice(&e);
}

fn encode_series_index_row(dst: &mut Vec<u8>, row: &SeriesIndexRow) {
    let mut e = [0u8; SERIES_INDEX_ROW_SIZE];
    put_u64(&mut e, 0, row.first_series_id);
    put_u64(&mut e, 8, row.last_series_id);
    put_u32(&mut e, 16, row.entry_count);
    put_u64(&mut e, 24, row.block_offset);
    put_u32(&mut e, 32, row.block_size);
    put_u32(&mut e, 36, row.crc32c);
    dst.extend_from_slice(&e);
}

fn decode_chunk_group_entries(raw: &[u8]) -> Result<Vec<ChunkGroupEntry>, String> {
    if !raw.len().is_multiple_of(CHUNK_GROUP_ENTRY_SIZE) {
        return Err("file index size is not a multiple of 40".to_string());
    }
    let mut groups = Vec::with_capacity(raw.len() / CHUNK_GROUP_ENTRY_SIZE);
    for chunk in raw.as_chunks::<CHUNK_GROUP_ENTRY_SIZE>().0 {
        groups.push(ChunkGroupEntry {
            metric_id: get_u64(chunk, 0),
            group_offset: get_u64(chunk, 8),
            group_size: get_u32(chunk, 16),
            min_timestamp: get_i64(chunk, 20),
            max_timestamp: get_i64(chunk, 28),
            series_count: get_u32(chunk, 36),
        });
    }
    if !groups.windows(2).all(|w| w[0].metric_id < w[1].metric_id) {
        return Err("file index is not sorted by metric_id".to_string());
    }
    Ok(groups)
}

fn parse_file_header(h: &[u8]) -> Result<FileHeader, String> {
    if h.len() < FILE_HEADER_SIZE {
        return Err(format!("TSF header is {} bytes", h.len()));
    }
    let h = &h[..FILE_HEADER_SIZE];
    if &h[0..4] != MAGIC {
        return Err("unexpected TSF magic".to_string());
    }
    if get_u16(h, 4) != FORMAT_VERSION {
        return Err(format!("unsupported TSF version {}", get_u16(h, 4)));
    }
    if crc32c(&h[..124]) != get_u32(h, 124) {
        return Err("file header CRC32C mismatch".to_string());
    }
    Ok(FileHeader {
        flags: get_u16(h, 6),
        partition_start_ms: get_i64(h, 16),
        partition_end_ms: get_i64(h, 24),
        chunk_group_count: get_u32(h, 32),
        series_count: get_u64(h, 40),
        row_count: get_u64(h, 48),
        min_sequence: get_u64(h, 56),
        max_sequence: get_u64(h, 64),
        created_at_ms: get_i64(h, 72),
    })
}

fn parse_footer(f: &[u8], file_len: u64) -> Result<Footer, String> {
    if f.len() != FOOTER_SIZE {
        return Err(format!(
            "TSF footer must be {FOOTER_SIZE} bytes, got {}",
            f.len()
        ));
    }
    if &f[0..4] != MAGIC {
        return Err("unexpected TSF footer magic".to_string());
    }
    if get_u16(f, 4) != FORMAT_VERSION {
        return Err(format!("unsupported TSF footer version {}", get_u16(f, 4)));
    }
    if crc32c(&f[..52]) != get_u32(f, 52) {
        return Err("footer CRC32C mismatch".to_string());
    }
    let footer = Footer {
        file_index_offset: get_u64(f, 8),
        file_index_size: get_u32(f, 16),
        bloom_offset: get_u64(f, 20),
        bloom_size: get_u32(f, 28),
        bloom_crc32c: get_u32(f, 48),
        chunk_group_count: get_u32(f, 32),
        row_count: get_u64(f, 36),
    };
    if footer
        .file_index_offset
        .checked_add(footer.file_index_size as u64)
        .is_none_or(|end| end > file_len - FOOTER_SIZE as u64)
    {
        return Err("file index points outside the file".to_string());
    }
    Ok(footer)
}

fn decompress_at(
    segments: &[Segment],
    offset: u64,
    size: u32,
    expected_crc: u32,
    what: &str,
) -> Result<Vec<u8>, String> {
    let bytes = read_segments(segments, offset, size as u64)?;
    if expected_crc != 0 && crc32c(&bytes) != expected_crc {
        return Err(format!("{what} CRC32C mismatch"));
    }
    let mut raw = Vec::new();
    decompress_zstd(&mut raw, &bytes).map_err(|e| format!("cannot decompress {what}: {e}"))?;
    Ok(raw)
}

/// Returns the bytes at `[offset, offset + size)`.
///
/// Serves straight out of a single segment when one covers the range (the
/// common case: a whole-file reader, or a page read after the chunk group was
/// fetched) and copies only when the range straddles segment boundaries.
fn read_segments<'a>(
    segments: &'a [Segment],
    offset: u64,
    size: u64,
) -> Result<std::borrow::Cow<'a, [u8]>, String> {
    let end = offset
        .checked_add(size)
        .ok_or_else(|| "offset + size overflows".to_string())?;
    let idx = match segments.binary_search_by(|s| s.offset.cmp(&offset)) {
        Ok(idx) => idx,
        Err(0) => {
            return Err(format!(
                "byte range {offset}..{end} is not loaded (file starts at {})",
                segments.first().map(|s| s.offset).unwrap_or(0)
            ))
        }
        Err(idx) => idx - 1,
    };
    let first = &segments[idx];
    if offset < first.offset || end > first.end() {
        // Either the range starts before what we hold, or it spans a gap or
        // several segments. Walk forward and stitch; report the gap if a byte
        // inside the range is missing.
        let mut out = Vec::with_capacity(size as usize);
        let mut cursor = offset;
        for segment in &segments[idx..] {
            if cursor >= end {
                break;
            }
            if segment.end() <= cursor {
                continue;
            }
            if segment.offset > cursor {
                return Err(format!(
                    "byte range {cursor}..{end} is not loaded (gap before {})",
                    segment.offset
                ));
            }
            let skip = (cursor - segment.offset) as usize;
            let take = ((end - cursor) as usize).min(segment.data.len() - skip);
            out.extend_from_slice(&segment.data[skip..skip + take]);
            cursor += take as u64;
        }
        if cursor < end {
            return Err(format!(
                "byte range {cursor}..{end} is not loaded (file ends at {cursor})"
            ));
        }
        return Ok(std::borrow::Cow::Owned(out));
    }
    let skip = (offset - first.offset) as usize;
    Ok(std::borrow::Cow::Borrowed(
        &first.data[skip..skip + size as usize],
    ))
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

macro_rules! get_le {
    ($name:ident, $ty:ty, $size:expr) => {
        fn $name(src: &[u8], offset: usize) -> $ty {
            <$ty>::from_le_bytes(src[offset..offset + $size].try_into().unwrap())
        }
    };
}

get_le!(get_u16, u16, 2);
get_le!(get_u32, u32, 4);
get_le!(get_u64, u64, 8);
get_le!(get_i64, i64, 8);

fn put_u16(dst: &mut [u8], offset: usize, v: u16) {
    dst[offset..offset + 2].copy_from_slice(&v.to_le_bytes());
}

fn put_u32(dst: &mut [u8], offset: usize, v: u32) {
    dst[offset..offset + 4].copy_from_slice(&v.to_le_bytes());
}

fn put_u64(dst: &mut [u8], offset: usize, v: u64) {
    dst[offset..offset + 8].copy_from_slice(&v.to_le_bytes());
}

fn put_i64(dst: &mut [u8], offset: usize, v: i64) {
    dst[offset..offset + 8].copy_from_slice(&v.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(metric_id: u64, series_id: u64, timestamp: i64, value: f64, sequence: u64) -> Row {
        Row {
            metric_id,
            series_id,
            timestamp,
            value,
            sequence,
            reserved: 0,
        }
    }

    fn bits(rows: &[Row]) -> Vec<(u64, u64, i64, u64, u64, u64)> {
        rows.iter()
            .map(|r| {
                (
                    r.metric_id,
                    r.series_id,
                    r.timestamp,
                    r.value.to_bits(),
                    r.sequence,
                    r.reserved,
                )
            })
            .collect()
    }

    #[test]
    fn test_roundtrip_multiple_metrics_and_pages() {
        let mut rows = Vec::new();
        for metric in 1..=2u64 {
            for series in 0..3u64 {
                for i in 0..250i64 {
                    let value = if i == 7 && series == 1 {
                        f64::NAN
                    } else {
                        (series as f64) * 1000.0 + i as f64
                    };
                    rows.push(row(
                        metric,
                        series,
                        1_700_000_000_000 + i * 10,
                        value,
                        100 + i as u64,
                    ));
                }
            }
        }
        let original = {
            let mut sorted = rows.clone();
            sorted.sort_by_key(|r| (r.metric_id, r.series_id, r.timestamp, r.sequence));
            sorted
        };
        let opts = WriterOptions { page_rows: 64 };
        let (bytes, stats) =
            encode_file(rows, (1_700_000_000_000, 1_700_086_400_000), &opts).unwrap();
        assert_eq!(stats.rows, original.len() as u64);
        assert_eq!(stats.series, 6);
        assert_eq!(stats.groups, 2);

        let reader = TsfReader::from_bytes(bytes).unwrap();
        assert_eq!(reader.header().row_count, original.len() as u64);
        assert_eq!(reader.header().min_sequence, 100);
        assert_eq!(reader.header().max_sequence, 349);
        assert_eq!(reader.metric_ids(), vec![1, 2]);
        assert_eq!(reader.series_ids(1).unwrap(), vec![0, 1, 2]);

        let decoded = reader.read_all().unwrap();
        assert_eq!(bits(&decoded), bits(&original));

        // Pruning at every level: group -> series entry -> page index.
        let narrow = reader
            .read_series(
                2,
                1,
                TimeRange::new(1_700_000_000_500, 1_700_000_001_000),
                None,
            )
            .unwrap();
        assert!(!narrow.is_empty());
        assert!(narrow.iter().all(|r| r.metric_id == 2 && r.series_id == 1));
        assert!(narrow
            .iter()
            .all(|r| r.timestamp >= 1_700_000_000_500 && r.timestamp < 1_700_000_001_000));

        let out_of_range = reader
            .read_metric(1, TimeRange::new(0, 1_000), None)
            .unwrap();
        assert!(out_of_range.is_empty());

        let missing = reader
            .read_series(1, 999, TimeRange::new(i64::MIN, i64::MAX), None)
            .unwrap();
        assert!(missing.is_empty());
    }

    #[test]
    fn test_sequence_range_filter() {
        let rows = (0..100i64)
            .map(|i| row(7, 1, 1_000 + i, i as f64, i as u64))
            .collect::<Vec<_>>();
        let (bytes, _) = encode_file(rows, (0, 10_000), &WriterOptions::default()).unwrap();
        let reader = TsfReader::from_bytes(bytes).unwrap();
        let filtered = reader
            .read_series(7, 1, TimeRange::new(i64::MIN, i64::MAX), Some((40, 60)))
            .unwrap();
        assert_eq!(filtered.len(), 20);
        assert!(filtered.iter().all(|r| (40..60).contains(&r.sequence)));
    }

    #[test]
    fn test_dedup_keeps_highest_sequence() {
        let rows = vec![
            row(1, 1, 100, 1.0, 5),
            row(1, 1, 100, 2.0, 9),
            row(1, 1, 100, 3.0, 7),
            row(1, 1, 200, 4.0, 1),
        ];
        let (bytes, stats) = encode_file(rows, (0, 1000), &WriterOptions::default()).unwrap();
        assert_eq!(stats.rows, 2);
        let reader = TsfReader::from_bytes(bytes).unwrap();
        let decoded = reader.read_all().unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].value, 2.0);
        assert_eq!(decoded[0].sequence, 9);
    }

    #[test]
    fn test_sparse_series_index_multi_block() {
        let mut rows = Vec::new();
        for series in 0..200u64 {
            for i in 0..5i64 {
                rows.push(row(3, series, 1_000 + i, series as f64 + i as f64, 1));
            }
        }
        let (bytes, stats) = encode_file(rows, (0, 10_000), &WriterOptions::default()).unwrap();
        assert_eq!(stats.series, 200);
        let reader = TsfReader::from_bytes(bytes).unwrap();
        let ids = reader.series_ids(3).unwrap();
        assert_eq!(ids, (0..200).collect::<Vec<_>>());
        // A series in a later index block.
        let rows = reader
            .read_series(3, 173, TimeRange::new(i64::MIN, i64::MAX), None)
            .unwrap();
        assert_eq!(rows.len(), 5);
        // A gap between blocks.
        let missing = reader
            .read_series(3, 500, TimeRange::new(i64::MIN, i64::MAX), None)
            .unwrap();
        assert!(missing.is_empty());
    }

    #[test]
    fn test_page_corruption_is_detected() {
        let rows = vec![row(1, 1, 100, 1.0, 1), row(1, 1, 110, 2.0, 2)];
        let (mut bytes, _) = encode_file(rows, (0, 1000), &WriterOptions::default()).unwrap();
        // 128 B file header + 48 B chunk group + 96 B series chunk = 272 B;
        // flipping any page byte must trip the page CRC.
        bytes[272 + 100] ^= 0xFF;
        let reader = TsfReader::from_bytes(bytes).unwrap();
        let err = reader.read_all().unwrap_err();
        assert!(err.contains("CRC"), "unexpected error: {err}");
    }

    #[test]
    fn test_header_corruption_is_detected() {
        let rows = vec![row(1, 1, 100, 1.0, 1)];
        let (mut bytes, _) = encode_file(rows, (0, 1000), &WriterOptions::default()).unwrap();
        bytes[40] ^= 0xFF;
        assert!(TsfReader::from_bytes(bytes).is_err());
    }

    #[test]
    fn test_write_file_roundtrip() {
        let dir = std::env::temp_dir().join(format!("tsf-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("file.tsf");
        let rows = vec![row(1, 1, 100, 1.5, 1), row(1, 2, 100, 2.5, 1)];
        let stats = write_file(&path, rows, (0, 1000), &WriterOptions::default()).unwrap();
        assert_eq!(stats.bytes, std::fs::metadata(&path).unwrap().len());
        let reader = TsfReader::open(&path).unwrap();
        assert_eq!(reader.read_all().unwrap().len(), 2);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    fn assert_columns_match_rows(columns: &DecodedColumns, rows: &[Row]) {
        assert_eq!(columns.len(), rows.len());
        if columns.mask.contains(ColumnMask::TIMESTAMP) {
            for (i, row) in rows.iter().enumerate() {
                assert_eq!(columns.timestamps[i], row.timestamp);
            }
        }
        if columns.mask.contains(ColumnMask::VALUE) {
            for (i, row) in rows.iter().enumerate() {
                assert_eq!(columns.values[i].to_bits(), row.value.to_bits());
            }
        }
        if columns.mask.contains(ColumnMask::SEQUENCE) {
            for (i, row) in rows.iter().enumerate() {
                assert_eq!(columns.sequences[i], row.sequence);
            }
        }
        if columns.mask.contains(ColumnMask::RESERVED) {
            for (i, row) in rows.iter().enumerate() {
                assert_eq!(columns.reserved[i], row.reserved);
            }
        }
    }

    fn mixed_rows() -> Vec<Row> {
        let mut rows = Vec::new();
        for metric in 1..=2u64 {
            for series in 0..5u64 {
                for i in 0..300i64 {
                    let mut timestamp = 1_700_000_000_000 + i * 10;
                    // Duplicate timestamps are legal.
                    if i > 0 && i % 97 == 0 {
                        timestamp -= 10;
                    }
                    let value = if i == 31 && series == 2 {
                        f64::NAN
                    } else {
                        series as f64 * 100.0 + i as f64
                    };
                    rows.push(row(metric, series, timestamp, value, (i as u64) % 4));
                }
            }
        }
        rows
    }

    #[test]
    fn test_columns_differential_against_scalar_rows() {
        let rows = mixed_rows();
        let (bytes, _) = encode_file(
            rows,
            (0, 2_000_000_000_000),
            &WriterOptions { page_rows: 64 },
        )
        .unwrap();
        let reader = TsfReader::from_bytes(bytes).unwrap();
        let mut ctx = ReadContext::new();
        let ranges = [
            (i64::MIN, i64::MAX),
            (1_700_000_000_000, 1_700_000_001_000),
            (1_700_000_000_500, 1_700_000_000_900),
            (1_700_000_001_505, 1_700_000_001_515),
            (0, 1),
        ];
        for metric in [1u64, 2] {
            for series in 0..5u64 {
                for (start, end) in ranges {
                    let range = TimeRange::new(start, end);
                    let scalar = reader.read_series(metric, series, range, None).unwrap();
                    for mask in [
                        ColumnMask::ALL,
                        ColumnMask::SAMPLE,
                        ColumnMask::TIMESTAMP,
                        ColumnMask::VALUE | ColumnMask::SEQUENCE,
                        ColumnMask::RESERVED,
                    ] {
                        let columns = reader
                            .read_series_columns(metric, series, range, mask, &mut ctx)
                            .unwrap();
                        assert_eq!(columns.mask, mask);
                        assert_columns_match_rows(&columns, &scalar);
                    }
                }
            }
        }
    }

    #[test]
    fn test_metric_columns_match_rows_and_keep_series_order() {
        let rows = mixed_rows();
        let (bytes, _) = encode_file(
            rows,
            (0, 2_000_000_000_000),
            &WriterOptions { page_rows: 128 },
        )
        .unwrap();
        let reader = TsfReader::from_bytes(bytes).unwrap();
        let mut ctx = ReadContext::new();
        let range = TimeRange::new(1_700_000_000_000, 1_700_000_001_000);
        let columns = reader
            .read_metric_columns(1, range, ColumnMask::SAMPLE, &mut ctx)
            .unwrap();
        assert_eq!(
            columns.iter().map(|c| c.series_id).collect::<Vec<_>>(),
            (0..5).collect::<Vec<_>>()
        );
        for columns in &columns {
            let rows = reader
                .read_series(1, columns.series_id, range, None)
                .unwrap();
            assert_columns_match_rows(columns, &rows);
        }

        let all = reader
            .read_all_columns(ColumnMask::ALL, &mut ReadContext::new())
            .unwrap();
        let total: usize = all.iter().map(|c| c.len()).sum();
        assert_eq!(total, reader.read_all().unwrap().len());
    }

    #[test]
    fn test_index_cache_hits_and_evictions() {
        let mut rows = Vec::new();
        for series in 0..200u64 {
            for i in 0..10i64 {
                rows.push(row(3, series, 1_000 + i, series as f64, 1));
            }
        }
        let (bytes, _) = encode_file(rows, (0, 10_000), &WriterOptions::default()).unwrap();
        let reader = TsfReader::from_bytes(bytes).unwrap();
        let range = TimeRange::new(i64::MIN, i64::MAX);
        let mut ctx = ReadContext::with_cache_bytes(1 << 20);
        reader
            .read_series_columns(3, 73, range, ColumnMask::SAMPLE, &mut ctx)
            .unwrap();
        let first = ctx.cache_stats();
        assert!(first.misses > 0);
        assert!(first.entries > 0);
        reader
            .read_series_columns(3, 73, range, ColumnMask::SAMPLE, &mut ctx)
            .unwrap();
        let second = ctx.cache_stats();
        assert!(second.hits > first.hits, "expected a cache hit");
        assert!(second.hit_ratio() > 0.0);

        // A zero-byte budget must reject cache inserts but still answer.
        let mut tiny = ReadContext::with_cache_bytes(0);
        let out = reader
            .read_series_columns(3, 73, range, ColumnMask::SAMPLE, &mut tiny)
            .unwrap();
        assert!(!out.is_empty());
        assert!(tiny.cache_stats().rejects > 0);
        assert_eq!(tiny.cache_bytes(), 0);
    }

    #[test]
    fn test_parallel_scan_matches_single_worker_and_preserves_order() {
        let mut rows = Vec::new();
        for series in 0..64u64 {
            for i in 0..80i64 {
                rows.push(row(
                    9,
                    series,
                    2_000 + i * 5,
                    series as f64 + (i as f64) * 0.5,
                    i as u64,
                ));
            }
        }
        let (bytes, _) = encode_file(rows, (0, 10_000), &WriterOptions { page_rows: 32 }).unwrap();
        let reader = TsfReader::from_bytes(bytes).unwrap();
        let range = TimeRange::new(2_100, 2_250);
        let mut ctx = ReadContext::new();
        let single = reader
            .read_metric_columns(9, range, ColumnMask::SAMPLE, &mut ctx)
            .unwrap();
        let parallel = scan_metric_parallel(
            &reader,
            9,
            range,
            ColumnMask::SAMPLE,
            &ScanOptions {
                workers: 4,
                cache_bytes_per_worker: DEFAULT_CACHE_BYTES,
                batch_rows: MAX_PAGE_ROWS,
            },
        )
        .unwrap();
        assert_eq!(parallel.workers, 4);
        assert_eq!(single.len(), parallel.columns.len());
        for (a, b) in single.iter().zip(&parallel.columns) {
            assert_eq!(a.series_id, b.series_id);
            assert_eq!(a.timestamps, b.timestamps);
            assert_eq!(
                a.values.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
                b.values.iter().map(|v| v.to_bits()).collect::<Vec<_>>()
            );
        }
        assert!(parallel.stats.hits + parallel.stats.misses > 0);
    }

    /// T1 acceptance: a 1M-row file must round-trip every column exactly.
    ///
    /// Kept in the normal suite: it runs in a few seconds and guards the page
    /// boundaries, the index and the NaN/reserved columns at file scale.
    #[test]
    fn test_one_million_row_roundtrip() {
        const SERIES: u64 = 1000;
        const SAMPLES: i64 = 1000;
        let interval = 15_000i64;
        let base = 1_700_000_000_000i64;

        let mut rows = Vec::with_capacity((SERIES * SAMPLES as u64) as usize);
        for series in 0..SERIES {
            for i in 0..SAMPLES {
                let value = if i == 17 && series == 33 {
                    f64::NAN
                } else {
                    series as f64 * 0.5 + (i as f64 * 0.01).sin()
                };
                rows.push(Row {
                    metric_id: 1,
                    series_id: series,
                    timestamp: base + i * interval,
                    value,
                    sequence: 1000 + i as u64,
                    reserved: (series + i as u64) % 3,
                });
            }
        }
        let expected_rows = rows.len() as u64;

        let (bytes, stats) = encode_file(
            rows,
            (base, base + SAMPLES * interval),
            &WriterOptions::default(),
        )
        .unwrap();
        assert_eq!(stats.rows, expected_rows);
        assert_eq!(stats.series, SERIES);
        assert!(
            bytes.len() < 16 * 1024 * 1024,
            "unexpected file size {}",
            bytes.len()
        );

        let reader = TsfReader::from_bytes(bytes).unwrap();
        assert_eq!(reader.header().row_count, expected_rows);
        assert_eq!(reader.header().series_count, SERIES);
        assert_eq!(reader.metric_ids(), vec![1]);

        // Full column scan must reproduce every row, including the NaN bits and
        // the reserved column.
        let mut ctx = ReadContext::new();
        let columns = reader
            .read_metric_columns(
                1,
                TimeRange::new(i64::MIN, i64::MAX),
                ColumnMask::ALL,
                &mut ctx,
            )
            .unwrap();
        let scanned: u64 = columns.iter().map(|c| c.len() as u64).sum();
        assert_eq!(scanned, expected_rows);
        let mut checked = 0u64;
        for column in &columns {
            let series = column.series_id;
            assert_eq!(column.len() as i64, SAMPLES);
            for (i, timestamp) in column.timestamps.iter().enumerate() {
                assert_eq!(*timestamp, base + i as i64 * interval);
            }
            for (i, value) in column.values.iter().enumerate() {
                let expected = if i == 17 && series == 33 {
                    f64::NAN
                } else {
                    series as f64 * 0.5 + (i as f64 * 0.01).sin()
                };
                assert_eq!(value.to_bits(), expected.to_bits());
            }
            for (i, sequence) in column.sequences.iter().enumerate() {
                assert_eq!(*sequence, 1000 + i as u64);
            }
            for (i, reserved) in column.reserved.iter().enumerate() {
                assert_eq!(*reserved, (series + i as u64) % 3);
            }
            checked += column.len() as u64;
        }
        assert_eq!(checked, expected_rows);

        // A single-series read still works on a ~630-page file.
        let one = reader
            .read_series(1, 500, TimeRange::new(base, base + 5 * interval), None)
            .unwrap();
        assert_eq!(one.len(), 5);
        assert_eq!(one[0].series_id, 500);
        assert_eq!(one.last().unwrap().timestamp, base + 4 * interval);
    }

    /// Fetches `ranges` out of `data`, the way an object store would, and
    /// rebuilds a reader from just those bytes.
    fn ranged_reader(data: &[u8], ranges: &[(u64, u64)]) -> TsfReader {
        TsfReader::from_segments(
            ranges
                .iter()
                .map(|&(start, end)| (start, data[start as usize..end as usize].to_vec()))
                .collect::<Vec<_>>(),
        )
        .unwrap()
    }

    #[test]
    fn test_ranged_reader_matches_whole_file() {
        let rows = mixed_rows();
        let (bytes, stats) = encode_file(
            rows.clone(),
            (0, 2_000_000_000_000),
            &WriterOptions { page_rows: 64 },
        )
        .unwrap();
        let len = bytes.len() as u64;

        // Round 1: footer only, to learn the index layout.
        let footer = TsfReader::footer_layout(&bytes[bytes.len() - FOOTER_SIZE..], len).unwrap();
        assert_eq!(footer.chunk_group_count, 2);
        assert_eq!(footer.row_count, stats.rows);

        // Round 2: header + bloom + file index + footer.
        let index_ranges = footer.index_ranges(len);
        assert!(index_ranges.iter().all(|&(_, end)| end <= len));
        let loaded = index_ranges.iter().map(|&(a, b)| b - a).sum::<u64>();
        assert!(loaded < len / 4, "index bytes {loaded} vs file {len}");
        let index_only = ranged_reader(&bytes, &index_ranges);

        // Reading a chunk group that was never fetched must fail loudly, not
        // silently return nothing.
        let mut ctx = ReadContext::new();
        let err = index_only
            .read_metric_columns(
                1,
                TimeRange::new(i64::MIN, i64::MAX),
                ColumnMask::ALL,
                &mut ctx,
            )
            .unwrap_err();
        assert!(err.contains("not loaded"), "unexpected error: {err}");

        // Round 3: the staged planner, until it has everything it needs.
        let metrics = [(1u64, TimeRange::new(i64::MIN, i64::MAX))];
        let (ranged, _, _) = plan_ranged_scan(&bytes, &metrics, None);

        let whole = TsfReader::from_bytes(bytes.clone()).unwrap();
        let mut ctx = ReadContext::new();
        let expected = whole
            .read_metric_columns(
                1,
                TimeRange::new(i64::MIN, i64::MAX),
                ColumnMask::ALL,
                &mut ctx,
            )
            .unwrap();
        let mut ctx = ReadContext::new();
        let actual = ranged
            .read_metric_columns(
                1,
                TimeRange::new(i64::MIN, i64::MAX),
                ColumnMask::ALL,
                &mut ctx,
            )
            .unwrap();
        assert_eq!(actual.len(), expected.len());
        for (a, b) in actual.iter().zip(&expected) {
            assert_eq!(a.series_id, b.series_id);
            assert_eq!(a.timestamps, b.timestamps);
            assert_eq!(
                a.values.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
                b.values.iter().map(|v| v.to_bits()).collect::<Vec<_>>()
            );
            assert_eq!(a.sequences, b.sequences);
            assert_eq!(a.reserved, b.reserved);
        }
    }

    #[test]
    fn test_ranged_reader_prunes_by_metric_and_time() {
        let (bytes, _) = encode_file(
            mixed_rows(),
            (0, 2_000_000_000_000),
            &WriterOptions { page_rows: 64 },
        )
        .unwrap();
        let len = bytes.len() as u64;
        let footer = TsfReader::footer_layout(&bytes[bytes.len() - FOOTER_SIZE..], len).unwrap();
        let reader = ranged_reader(&bytes, &footer.index_ranges(len));

        // Metric absent from the file index: the planner asks for nothing.
        assert!(reader
            .next_scan_ranges(&[(99, TimeRange::new(i64::MIN, i64::MAX))], None)
            .is_none());

        // A range that misses the metric's chunk group asks for nothing too.
        assert!(reader
            .next_scan_ranges(&[(1, TimeRange::new(1, 2))], None)
            .is_none());

        // A metric that is present needs its chunk group.
        let wanted = reader
            .next_scan_ranges(&[(1, TimeRange::new(i64::MIN, i64::MAX))], None)
            .unwrap();
        assert!(!wanted.is_empty());
        for pair in wanted.windows(2) {
            assert!(pair[0].1 < pair[1].0, "ranges not merged: {wanted:?}");
        }
        for &(start, end) in &wanted {
            assert!(start < end && end <= len);
        }
    }

    /// Fetches whatever `next_scan_ranges` asks for until it stops asking,
    /// the way the object-store reader does.
    fn plan_ranged_scan(
        data: &[u8],
        metrics: &[(u64, TimeRange)],
        series: Option<&SeriesFilter>,
    ) -> (TsfReader, u64, usize) {
        let len = data.len() as u64;
        let footer = TsfReader::footer_layout(&data[data.len() - FOOTER_SIZE..], len).unwrap();
        let mut ranges = footer.index_ranges(len);
        let mut rounds = 0;
        loop {
            let reader = ranged_reader(data, &ranges);
            let Some(more) = reader.next_scan_ranges(metrics, series) else {
                break;
            };
            rounds += 1;
            assert!(rounds < 16, "planner did not converge: {more:?}");
            ranges.extend(more);
        }
        ranges.extend(ranged_reader(data, &ranges).page_ranges_for_metrics(metrics, series));
        ranges.sort_unstable();
        let mut merged: Vec<(u64, u64)> = Vec::new();
        for (start, end) in ranges {
            match merged.last_mut() {
                Some(last) if start <= last.1 => last.1 = last.1.max(end),
                _ => merged.push((start, end)),
            }
        }
        let fetched = merged.iter().map(|(a, b)| b - a).sum::<u64>();
        (ranged_reader(data, &merged), fetched, rounds)
    }

    #[test]
    fn test_staged_scan_planner_reads_only_touched_pages() {
        // One metric, many series, many pages per series, a full day wide.
        let base = 1_700_000_000_000;
        let mut rows = Vec::new();
        for series in 0..50u64 {
            for i in 0..400i64 {
                rows.push(row(
                    1,
                    series,
                    base + i * 1_000,
                    series as f64 + i as f64,
                    0,
                ));
            }
        }
        let (bytes, stats) = encode_file(
            rows.clone(),
            (base, base + 86_400_000),
            &WriterOptions { page_rows: 64 },
        )
        .unwrap();
        let len = bytes.len() as u64;

        // A one-sample window. Fifty series still means fifty pages (one page
        // per series), so this measures page-level pruning while the file
        // metadata is a fixed floor: the check is that the bytes fetched track
        // the window, not the file.
        let narrow = TimeRange::new(base + 100 * 1_000, base + 100 * 1_000 + 1);
        let (reader, fetched, rounds) = plan_ranged_scan(&bytes, &[(1, narrow)], None);
        assert!(rounds >= 1, "expected several planning rounds");
        assert!(
            fetched * 5 < len * 2,
            "narrow scan fetched {fetched} of {len} bytes"
        );
        // Exactly one page per series is needed.
        let (planner, _, _) = plan_ranged_scan(&bytes, &[(1, narrow)], None);
        assert_eq!(
            planner.page_ranges_for_metrics(&[(1, narrow)], None).len(),
            50
        );

        let mut ctx = ReadContext::new();
        let decoded = reader
            .read_metric_columns(1, narrow, ColumnMask::ALL, &mut ctx)
            .unwrap();
        let rows_read: usize = decoded.iter().map(|c| c.len()).sum();
        assert_eq!(rows_read, 50, "one sample per series");
        for column in &decoded {
            assert_eq!(column.len(), 1);
            assert_eq!(column.timestamps[0], base + 100 * 1_000);
            assert_eq!(column.values[0], column.series_id as f64 + 100.0);
        }

        // A whole-file window must still be correct, and it fetches
        // everything.
        let wide = TimeRange::new(i64::MIN, i64::MAX);
        let (reader, fetched, _) = plan_ranged_scan(&bytes, &[(1, wide)], None);
        assert_eq!(fetched, len);
        let mut ctx = ReadContext::new();
        let decoded = reader
            .read_metric_columns(1, wide, ColumnMask::ALL, &mut ctx)
            .unwrap();
        let rows_read: usize = decoded.iter().map(|c| c.len()).sum();
        assert_eq!(rows_read, stats.rows as usize);
    }

    /// A series filter must plan the requested series only, and decode only
    /// them: this is the difference between a single-series query and a
    /// whole-metric scan.
    #[test]
    fn test_series_filter_plans_only_requested_series() {
        let base = 1_700_000_000_000;
        let mut rows = Vec::new();
        for series in 0..50u64 {
            for i in 0..400i64 {
                rows.push(row(
                    1,
                    series,
                    base + i * 1_000,
                    series as f64 + i as f64,
                    0,
                ));
            }
        }
        let (bytes, _) = encode_file(
            rows,
            (base, base + 86_400_000),
            &WriterOptions { page_rows: 64 },
        )
        .unwrap();

        // One series, one page: the whole dataset is 50 series x 7 pages.
        let narrow = TimeRange::new(base + 100 * 1_000, base + 100 * 1_000 + 1);
        let filter = SeriesFilter::new([7]);
        let (planner, fetched, _) = plan_ranged_scan(&bytes, &[(1, narrow)], Some(&filter));
        assert_eq!(
            planner
                .page_ranges_for_metrics(&[(1, narrow)], Some(&filter))
                .len(),
            1
        );
        // A three-series filter plans three pages, not fifty.
        let three = SeriesFilter::new([0, 25, 49]);
        let (planner, _, _) = plan_ranged_scan(&bytes, &[(1, narrow)], Some(&three));
        assert_eq!(
            planner
                .page_ranges_for_metrics(&[(1, narrow)], Some(&three))
                .len(),
            3
        );

        // The unfiltered plan of the same window needs all fifty pages, so the
        // filtered read must fetch dramatically fewer bytes.
        let (_, unfiltered, _) = plan_ranged_scan(&bytes, &[(1, narrow)], None);
        assert!(
            fetched * 10 < unfiltered,
            "filtered read fetched {fetched} vs {unfiltered} bytes"
        );

        // The planned reader holds the filtered metadata only, and decoding
        // those series works.
        let mut ctx = ReadContext::new();
        let mut decoded = Vec::new();
        for id in three.ids() {
            decoded.push(
                planner
                    .read_series_columns(1, *id, narrow, ColumnMask::ALL, &mut ctx)
                    .unwrap(),
            );
        }
        assert_eq!(decoded.len(), 3);
        for column in &decoded {
            assert_eq!(column.len(), 1);
            assert_eq!(column.timestamps[0], base + 100 * 1_000);
            assert_eq!(column.values[0], column.series_id as f64 + 100.0);
        }

        // An empty filter matches no series, so nothing is planned.
        let empty = SeriesFilter::new([]);
        let (planner, _, _) = plan_ranged_scan(&bytes, &[(1, narrow)], Some(&empty));
        assert!(planner
            .page_ranges_for_metrics(&[(1, narrow)], Some(&empty))
            .is_empty());
    }

    #[test]
    fn test_staged_scan_planner_skips_absent_metrics() {
        let (bytes, _) = encode_file(
            mixed_rows(),
            (0, 2_000_000_000_000),
            &WriterOptions { page_rows: 64 },
        )
        .unwrap();
        let len = bytes.len() as u64;
        let footer = TsfReader::footer_layout(&bytes[bytes.len() - FOOTER_SIZE..], len).unwrap();
        let index_only = ranged_reader(&bytes, &footer.index_ranges(len));
        assert!(index_only
            .next_scan_ranges(&[(42, TimeRange::new(i64::MIN, i64::MAX))], None)
            .is_none());
        assert!(index_only
            .next_scan_ranges(&[(1, TimeRange::new(1, 2))], None)
            .is_none());
        assert!(index_only
            .page_ranges_for_metrics(&[(42, TimeRange::new(i64::MIN, i64::MAX))], None)
            .is_empty());
    }
}
