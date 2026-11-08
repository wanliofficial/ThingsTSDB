# ADR-0002: Partitioned TSF Data Files and the Single-Value Series Model

- Status: Implemented for T1-T4 on the sample schema (T5 benchmark matrix pending)
- Date: 2026-10-28
- Implemented: 2026-11-07 (see `docs/rfcs/20261029-vectorized-decode-plan.md`)
- Related: `docs/rfcs/20261028-mergeset-index-adr.md` (ADR-0001), `docs/roadmap.md`,
  `docs/3high-design.md`
- Reference implementations: `esmetrics` (Rust port of VictoriaMetrics v1.146.0,
  Apache-2.0), Apache IoTDB TsFile design (format ideas only, no byte compatibility)

# Summary

Phase 1 replaces only the index. This ADR decides the Phase 2 data path:

1. Keep the **single-value series model** (`metric name + labels` identifies one
   `f64` series). A device wide-table model is deferred, not adopted.
2. Replace Parquet SSTs with **TSF v1**, a TsFile-inspired, self-contained file per
   part: one header, metric-scoped chunk groups, series chunks, pages with
   min/max timestamp, min/max/first/last value and `__seq__` statistics, sparse
   indexes and a footer.
3. Partition storage by **UTC day**. Retention becomes a whole-partition drop whose
   cost does not grow with the row count.
4. Use VM-style integer/float block codecs (delta-of-delta, nearest-delta, constant
   fast paths) with ZSTD, and page-level statistics for time and `__seq__` pruning.
5. Keep the existing manifest, WAL, `__seq__` last-write-wins rule and DataFusion
   merge layer; add `format` to file metadata and support dual-read during migration.

The file format is a **single object**, unlike VictoriaMetrics' multi-file parts. This
keeps object-store uploads atomic and lets readers fetch only the footer, index blocks
and pages they need.

# Motivation

The current data path stores `(metric_id, tsid, timestamp, value)` rows in Parquet SSTs
and relies on Parquet row-group statistics plus DataFusion for filtering. It works, but
it has structural limits for the target workload:

- **Row-group granularity.** Parquet row groups are typically 128-512 MiB before
  compression. A time-range query still decodes whole row groups for the selected
  series, even when only a few pages overlap.
- **Encoding is generic.** PLAIN + Snappy does not exploit monotonic timestamps or
  slowly changing gauge values. IoT scrape workloads compress far better with
  delta-of-delta timestamps and XOR/nearest-delta float values.
- **No series-aware file layout.** Parquet does not know that rows are keyed by
  `(metric_id, tsid, timestamp)`, so a query for a small set of series cannot seek a
  per-series chunk; it filters after decoding.
- **TTL is file-level.** Expiring old data requires deleting individual SSTs and
  rewriting manifests; whole-day partition drops are simpler and cheaper.
- **Index and data formats evolve independently.** The Phase 1 index makes series IDs
  first-class; the data file should use the same ID and expose page statistics that the
  query planner can consume directly.

The roadmap also asks for a decision between single-value series and an IoTDB-style
device wide table. This ADR settles it for Phase 2 and records what would be required
to revisit it.

# Details

## 1. Decision

We will:

1. Keep the logical model **one metric name + one label set = one series**, with one
   `f64` value per timestamp. `SearchRequest` remains metric-scoped.
2. Introduce **TSF v1**, a single-file, self-contained part format:
   `header -> chunk groups (metric) -> series chunks (series) -> pages`, followed by a
   bloom filter, a file index and a footer.
3. Partition data by **UTC day** (`partition_duration = 1d` by default). A file never
   spans partitions. Retention drops whole partition directories.
4. Use page size **8,192 rows or 1 MiB uncompressed**, whichever comes first, with
   per-page min/max timestamp, min/max/first/last value and `__seq__` statistics.
5. Keep `__seq__` and `__reserved__` as first-class stored columns. `__seq__` is the
   last-write-wins arbiter; `__reserved__` carries tombstone/expiration flags.
6. Extend `FileMeta` with `format` and `min_sequence`, keep `max_sequence` and
   `time_range`, and support Parquet/TSF dual-read during migration.
7. Keep the data WAL and flush pipeline. Flushing writes a TSF file, fsyncs it, then
   commits a manifest delta; WAL truncation follows the manifest commit.

Non-goals for Phase 2:

- Byte compatibility with Apache TsFile.
- Lossy value compression enabled by default (`precision_bits = 64`).
- Storing labels in the data file; the index remains the label source.
- Device wide-table / aligned-timestamp queries.
- A native object-store layout without a local cache (Phase 4).

## 2. Data Model

### 2.1 Chosen model: single-value series

A series is identified by a metric name and a label set:

```
series = (metric_name, {label_key -> label_value}*)
row    = (metric_id, series_id, timestamp, value, __seq__, __reserved__)
```

- `metric_id = seahash(metric_name)`, unchanged from the current data schema.
- `series_id` is the trailing 8 bytes of the 24-byte TSID from ADR-0001; it is globally
  unique across months and nodes. Before the index migration, the data table's `tsid`
  is the legacy `seahash(series_key)`; a TSF file written in that window stores the
  legacy value in the same header field, and the query's `__seq__` generation selects
  the matching label source.
- `value` is `f64` in v1. Integer, boolean and string values are a future format
  extension, not a Phase 2 feature.
- A series chunk stores samples sorted by `(timestamp, __seq__)` ascending. Duplicate
  timestamps are legal; the query layer keeps the row with the highest `__seq__`.

This model matches Prometheus remote write, the existing `SearchRequest`, and the
Phase 1 index. It also keeps chunk groups metric-scoped, so a query for one metric never
reads another metric's chunk group.

### 2.2 Why not a device wide table

IoTDB's device model stores one row per `(device, timestamp)` with one column per
measurement. It is attractive when many sensors of one device are sampled together:

- aligned timestamps are stored once,
- a device query reads one chunk group,
- schema templates amortize metadata across identical devices.

It is not adopted in Phase 2 because:

1. The public query shape is metric-scoped (`SearchRequest.metric_name`), and Grafana /
   Prometheus queries are per-metric. A wide-table storage model would not be exercised
   by the current API.
2. It requires a schema registry (schema templates), null semantics for missing
   measurements, and a new scan shape that returns multiple metric columns per device.
3. It couples data layout to device identity, while the index and TSID are series-first.
4. The physical benefit (shared timestamps) is modest for scrape workloads where
   different metrics are often scraped independently.

The TSID's `job_id` / `instance_id` grouping fields still give locality hints. A future
`tsf-v2` layout can group series chunks by `(metric_id, job_id, instance_id)` if the
writer stores those grouping fields in the series chunk header, without changing the
logical model or the query API; see Open Questions.

### 2.3 Row schema

The stored schema is fixed in v1:

| Column | Type | Role |
|---|---|---|
| `metric_id` | `u64` | chunk-group key; not repeated per row |
| `series_id` | `u64` | series-chunk key; not repeated per row |
| `timestamp` | `i64` | milliseconds since Unix epoch, non-decreasing in a series chunk |
| `value` | `f64` | sample value, stored as its IEEE-754 bit pattern |
| `__seq__` | `u64` | storage sequence; higher wins for duplicate timestamps |
| `__reserved__` | `u64` | tombstone/expiration bit flags; usually constant 0 |

`metric_id` and `series_id` live in chunk headers, not in every page. `__seq__` and
`__reserved__` are stored per row but compress well: a flush usually has a narrow
sequence range and a constant reserved value.

### 2.4 Partitioning

- Data is partitioned by **UTC day**:
  `partition_start = timestamp.div_euclid(86_400_000) * 86_400_000`.
- A part (file) belongs to exactly one partition. A write that crosses midnight is
  split at flush time.
- The partition key is also the per-day granularity used by the Phase 1 index (ns5/ns6/
  ns7), so index pruning and data pruning agree.
- `partition_duration` is configurable; one day is the default and the only value
  exercised in Phase 2 tests.

Late writes inside the retention window are accepted and eventually compacted. A write
whose partition has already been dropped by TTL is rejected with `OutOfRetention`; old
data is never resurrected by a late sample.

## 3. File Format (TSF v1)

### 3.1 Overall layout

A TSF file is one object:

```
+-----------------------------+
| FileHeader (128 B)          |
+-----------------------------+
| ChunkGroup[0]               |
|   ChunkGroupHeader (48 B)   |
|   SeriesChunk[0]            |
|     SeriesChunkHeader (96 B)|
|     Page[0] ... Page[n]     |
|     PageIndex (zstd)        |
|   SeriesChunk[1] ...        |
|   SparseSeriesIndex (zstd)  |
| ChunkGroup[1] ...           |
+-----------------------------+
| SeriesBloom (optional)      |
+-----------------------------+
| FileIndex (zstd)            |
+-----------------------------+
| Footer (64 B)               |
+-----------------------------+
```

- Chunk groups are sorted by `metric_id`.
- Series chunks inside a group are sorted by `series_id`.
- Pages inside a series chunk are sorted by `min_timestamp`.
- File-level offsets (chunk group, series chunk, page index, page offset) are absolute
  byte offsets from the start of the file. Page payload offsets are relative to the
  page start.
- The file is immutable once committed. Compaction writes a new file and swaps it in
  the manifest.

### 3.2 File header

Fixed 128 bytes, little-endian:

| Offset | Size | Field | Notes |
|---|---|---|---|
| 0 | 4 | `magic` | `"TSF1"` |
| 4 | 2 | `version` | `1` |
| 6 | 2 | `flags` | bit 0: bloom present; bit 1: file index compressed |
| 8 | 4 | `header_size` | 128 |
| 12 | 4 | `schema_id` | 1 = `(i64 ts, f64 value, u64 seq, u64 reserved)` |
| 16 | 8 | `partition_start_ms` | inclusive |
| 24 | 8 | `partition_end_ms` | exclusive end of the partition |
| 32 | 4 | `chunk_group_count` | |
| 36 | 4 | `flags2` | reserved, zero |
| 40 | 8 | `series_count` | |
| 48 | 8 | `row_count` | |
| 56 | 8 | `min_sequence` | minimum `__seq__` in this file |
| 64 | 8 | `max_sequence` | maximum `__seq__` in this file |
| 72 | 8 | `created_at_ms` | writer clock, for diagnostics |
| 80 | 4 | `writer_version` | build version |
| 84 | 40 | `reserved` | zero |
| 124 | 4 | `crc32c` | over bytes `[0, 124)` |

`min_sequence` / `max_sequence` are used by compaction, migration and the data WAL
truncation accounting. They are derived from the rows, not from the file ID.

### 3.3 Chunk group

One chunk group per `metric_id`. Fixed 48-byte header:

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | `metric_id` |
| 8 | 4 | `series_count` |
| 12 | 4 | `series_index_crc32c` |
| 16 | 8 | `min_timestamp` |
| 24 | 8 | `max_timestamp` |
| 32 | 8 | `series_index_offset` |
| 40 | 4 | `series_index_size` |
| 44 | 4 | `crc32c` over bytes `[0, 44)` |

The group body is the concatenation of its series chunks, followed by the sparse series
index. The sparse index maps a range of series IDs to a series-index block:

```
SeriesIndexRow (fixed 40 B):
  first_series_id u64
  last_series_id  u64
  entry_count     u32
  reserved        u32
  block_offset    u64
  block_size      u32
  crc32c          u32
```

Each series-index block contains up to **64** fixed-size entries:

```
SeriesEntry (fixed 56 B):
  series_id        u64
  chunk_offset     u64
  chunk_size       u32
  min_timestamp    i64
  max_timestamp    i64
  min_sequence     u64
  max_sequence     u64
  flags            u16
  reserved         u16
```

The sparse index is zstd-compressed. A lookup binary-searches `SeriesIndexRow`s by
`first_series_id`, reads one block, then binary-searches the entries. This bounds index
memory: a file with one million series of one metric has ~15,625 index rows, not one
million entries in memory at once. The series index is loaded on demand for the
requested metric.

### 3.4 Series chunk

Each series chunk belongs to one `series_id`. Fixed 96-byte header:

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | `series_id` |
| 8 | 8 | `sample_count` |
| 16 | 4 | `page_count` |
| 20 | 4 | `flags` | bit 0: has NaN; bit 1: has duplicates |
| 24 | 8 | `min_timestamp` |
| 32 | 8 | `max_timestamp` |
| 40 | 8 | `min_sequence` |
| 48 | 8 | `max_sequence` |
| 56 | 8 | `page_index_offset` |
| 64 | 4 | `page_index_size` |
| 68 | 8 | `data_offset` |
| 76 | 8 | `data_size` |
| 84 | 4 | `page_index_crc32c` |
| 88 | 4 | `reserved` |
| 92 | 4 | `crc32c` over bytes `[0, 92)` |

Pages follow the header at `data_offset`. The page index follows the last page and is
zstd-compressed. `sample_count` includes duplicates; `page_count` is at least 1.

### 3.5 Page

A page is the unit of compression, statistics and pruning. Fixed 128-byte header:

| Offset | Size | Field | Notes |
|---|---|---|---|
| 0 | 4 | `row_count` | 1..=8,192 |
| 4 | 2 | `flags` | bit 0: has NaN; bit 1: all NaN; bit 2: duplicate timestamps; bit 3: uncompressed |
| 6 | 1 | `ts_marshal_type` | codec id |
| 7 | 1 | `value_marshal_type` | codec id |
| 8 | 1 | `seq_marshal_type` | codec id |
| 9 | 1 | `reserved_marshal_type` | codec id |
| 10 | 1 | `precision_bits` | 64 default; lower values mean lossy |
| 11 | 1 | `reserved0` | zero |
| 12 | 8 | `min_timestamp` | |
| 20 | 8 | `max_timestamp` | |
| 28 | 8 | `first_timestamp` | first row's timestamp; needed by delta codecs |
| 36 | 8 | `first_value_bits` | first row's f64 bits |
| 44 | 8 | `min_value_bits` | over non-NaN values |
| 52 | 8 | `max_value_bits` | over non-NaN values |
| 60 | 8 | `min_sequence` | |
| 68 | 8 | `max_sequence` | |
| 76 | 8 | `first_sequence` | first row's `__seq__` |
| 84 | 8 | `first_reserved` | first row's `__reserved__` |
| 92 | 4 | `ts_offset` | relative to page start |
| 96 | 4 | `ts_size` | |
| 100 | 4 | `value_offset` | |
| 104 | 4 | `value_size` | |
| 108 | 4 | `seq_offset` | |
| 112 | 4 | `seq_size` | |
| 116 | 4 | `reserved_offset` | |
| 120 | 4 | `reserved_size` | |
| 124 | 4 | `crc32c` | over header `[0, 124)` plus all payloads |

Each of the four payloads is compressed independently, unless the page is marked
uncompressed. `first_timestamp`, `first_value_bits`, `first_sequence` and
`first_reserved` are stored in the header and omitted from the encoded payload, as in
VictoriaMetrics block headers. `__reserved__` is usually a constant zero; its payload is
empty for the `Const` codec.

`min_value_bits` / `max_value_bits` are computed numerically over non-NaN values and
stored as raw IEEE-754 bits. NaN values are excluded from the statistics and flagged;
a page with only NaN values sets the all-NaN flag and stores zero statistics.

### 3.6 Page index

The page index is a zstd-compressed array of fixed 80-byte entries:

```
PageEntry (fixed 80 B):
  min_timestamp    i64
  max_timestamp    i64
  first_value_bits u64
  last_value_bits  u64
  min_value_bits   u64
  max_value_bits   u64
  min_sequence     u64
  max_sequence     u64
  page_offset      u64
  page_size        u32
  row_count        u32
```

A query reads the page index for a requested series chunk, drops pages outside the time
range or `__seq__` range, and only then issues page reads. The index is not loaded for
series that the query does not touch.

### 3.7 File index and bloom filter

The file index is a zstd-compressed array of chunk-group entries:

```
ChunkGroupEntry (fixed 40 B):
  metric_id        u64
  group_offset     u64
  group_size       u32
  min_timestamp    i64
  max_timestamp    i64
  series_count     u32
```

It is loaded lazily when a query first needs the file, after the bloom filter (if
present) accepts it. A query selects the group whose `metric_id` matches; metric IDs are
unique within a file.

The optional bloom filter covers `series_id` values present in the file:

- `xxhash64(series_id)` with double hashing, 10 bits per key, target 1% false positive;
- size capped at 4 MiB; if the file has too many series, the writer omits the filter and
  clears the header flag;
- a query with a large `series_id` set can reject the file without reading the file
  index;
- the bloom filter is an optimization only. Correctness never depends on it.

### 3.8 Footer and checksums

Fixed 64 bytes at the end of the file:

| Offset | Size | Field |
|---|---|---|
| 0 | 4 | `magic` `"TSF1"` |
| 4 | 2 | `version` |
| 6 | 2 | `flags` |
| 8 | 8 | `file_index_offset` |
| 16 | 4 | `file_index_size` |
| 20 | 8 | `bloom_offset` |
| 28 | 4 | `bloom_size` |
| 32 | 4 | `chunk_group_count` |
| 36 | 8 | `row_count` |
| 44 | 4 | `file_index_crc32c` |
| 48 | 4 | `bloom_crc32c` |
| 52 | 4 | `footer_crc32c` over bytes `[0, 52)` |
| 56 | 8 | `reserved` |

Checksums use CRC32C:

- file header CRC covers the fixed header;
- chunk-group, series-chunk and page headers carry their own CRC;
- page CRC covers the header (excluding the CRC field) and the four payloads;
- each chunk group stores the CRC of its sparse series index;
- each series chunk stores the CRC of its page index;
- each sparse-index row stores the CRC of its series-index block;
- the footer stores the CRCs of the file index and bloom filter;
- a full-file checksum is **not** computed on open; scrubbing is an offline tool.

When the bloom filter is absent, `bloom_offset`, `bloom_size` and `bloom_crc32c` are
zero. The file index is always present; a file with zero series is invalid.

A CRC mismatch is fatal for that file: the reader returns `CorruptFile` with the file
ID and offset, and the query fails. Silent skipping of a page would turn corruption into
data loss.

## 4. Encoding

Codec ids are stored per payload in the page header:

| id | Codec | Used for |
|---|---|---|
| 0 | `PlainVarint` | exact fallback for timestamps, `__seq__`, `__reserved__` |
| 1 | `ZstdNearestDelta2` | counter-like integer arrays |
| 2 | `DeltaConst` | constant delta |
| 3 | `Const` | constant value |
| 4 | `ZstdNearestDelta` | gauge-like integer arrays |
| 5 | `NearestDelta2` | uncompressed counter fallback |
| 6 | `NearestDelta` | uncompressed gauge fallback |

Ids 1-6 match the `esm-encoding` discriminants so the codec modules can be ported
without translation. `f64` values are converted to their bit patterns and encoded as
`i64` arrays; `precision_bits` applies only to the nearest-delta codecs.

### 4.1 Timestamps

Timestamps in a page are non-decreasing. The encoder:

1. stores the first timestamp in the page header;
2. tries `Const` if all timestamps are equal;
3. tries `DeltaConst` if all deltas are equal (the common fixed-scrape-interval case);
4. otherwise uses the nearest-delta / delta-of-delta codec (the `esm-encoding`
   `NearestDelta` / `ZstdNearestDelta` family, the same idea as IoTDB `TS_2DIFF`);
5. falls back to plain varint if ZSTD does not save at least 10%.

Out-of-order input is sorted at flush before paging; a page never contains timestamps
that decrease. Duplicate timestamps are legal and set the duplicate flag.

### 4.2 Values

`f64` values are stored as their 64-bit IEEE-754 pattern in an `i64` lane:

1. `Const` if all values are bit-identical;
2. `DeltaConst` if all bit-pattern deltas are equal (rare, but cheap to detect);
3. nearest-delta / delta-of-delta with 64 precision bits for gauges and counters;
4. plain varint or raw bits + ZSTD fallback.

`precision_bits = 64` is the default and is lossless. Lower precision is an opt-in
compression mode for future versions; it must be recorded in the page header so readers
can report lossy data. NaN and infinities round-trip exactly.

IoTDB's Gorilla/CHIMP XOR encoding is a valid alternative for gauges; it is listed as a
benchmark alternative rather than the v1 default because the nearest-delta codec is
already implemented and tested in the reference implementation.

### 4.3 `__seq__` and `__reserved__`

- `__seq__` is usually constant or a narrow delta per flush; `Const` / `DeltaConst`
  cover most pages, otherwise delta + ZSTD.
- `__reserved__` is usually constant zero; `Const` has an empty payload. If expiration
  or tombstone bits are used, RLE/ZSTD is used.
- Both columns are stored even if the query does not project them, because `__seq__` is
  required for LWW and `__reserved__` for deletion/expiration.

### 4.4 Compression

- Page payloads: ZSTD level 1 by default; level -5 for pages smaller than 4 KiB; level 3
  for cold compaction output.
- Indexes: ZSTD level 3.
- No LZ4/Snappy in v1 to keep the codec surface small. A codec id byte is reserved per
  page so a later version can add LZ4 without a format break.
- A page is written uncompressed (`flags` bit 3) when compression does not save at least
  10%; the reader checks the flag before decompressing.

## 5. Write Path

### 5.1 Flush

The existing write buffer and WAL are retained. A flush:

1. selects frames whose partition is complete or whose size/time threshold is reached;
2. splits any frame that crosses a partition boundary;
3. sorts rows by `(metric_id, series_id, timestamp, __seq__)`;
4. groups by `metric_id` into chunk groups and by `series_id` into series chunks;
5. builds pages of up to 8,192 rows / 1 MiB uncompressed;
6. builds page indexes, sparse series indexes, the file index and the bloom filter;
7. writes the file to a temp path, fsyncs it, renames it into the partition directory,
   fsyncs the directory;
8. commits a manifest delta with `FileMeta { format: TsfV1, ... }`;
9. truncates WAL frames that are fully covered by the committed file.

For object storage, step 7 becomes a single PUT (multipart for large files) and step 8
is the manifest delta. A crash after PUT but before the manifest add leaves an orphan
object; startup GC removes objects not referenced by the manifest after a grace period.

### 5.2 File size and rollover

- Target file size: 512 MiB–2 GiB after compaction.
- Hard cap: 4 GiB, because the manifest's `FileMeta.size` is `u32` in the current
  schema. A larger partition is split into multiple big parts by metric-ID and
  series-ID ranges.
- A flush that would exceed the cap is split at a series-chunk boundary. A single series
  chunk is bounded by its page count and sample count; at a 1-second scrape interval one
  day is 86,400 samples and remains far below the cap. The writer rejects a series chunk
  larger than 1 GiB with `SeriesTooLarge`; this is outside the supported workload and
  avoids continuation chunks.

### 5.3 Atomic commit and recovery

The data WAL remains the source of truth until the manifest commit:

```
data WAL append -> buffer -> TSF write -> fsync/upload -> manifest add -> WAL truncate
```

- If the process dies before the manifest add, the WAL replays the batch and a new file
  is written. The orphaned TSF file is deleted by GC.
- If the process dies after the manifest add but before WAL truncation, replay writes a
  duplicate file. The duplicate is harmless: both files contain the same rows and the
  query deduplicates by `__seq__`. GC can merge/remove it later.
- A manifest delta is atomic (temp + rename for local FS; a single object PUT for object
  storage). Readers see either the old or the new file list.

### 5.4 Late and out-of-order writes

- A late sample is accepted if its partition is still within retention.
- The flush writes it into a new small part for that partition; compaction later merges
  it with the big part.
- If the partition has already been dropped by TTL, the write is rejected with
  `OutOfRetention`. The check runs before the data WAL append, so a rejected write does
  not leave a WAL frame to replay. The alternative (recreating the partition) would
  resurrect expired data and break the retention contract.
- Within a series chunk, rows are sorted by timestamp. Duplicate timestamps are kept;
  the query picks the highest `__seq__`.

## 6. Compaction and Deduplication

### 6.1 Inner and outer compaction

- **Inner compaction** happens while building a series chunk: pages are sorted, statistics
  are computed, and duplicate timestamps inside the new file are reduced to the row with
  the highest `__seq__` (optional; see 6.2).
- **Outer compaction** merges small parts within one partition, optionally together with
  an existing big part, into a new big part:
  - a k-way merge over series chunks sorted by `series_id`;
  - for equal series IDs, a merge of page streams by `timestamp, __seq__`;
  - pages are re-encoded with the cold-compaction ZSTD level;
  - the output file replaces the sources in one manifest delta;
  - source files are deleted after the delta commits;
  - the output file's `min_sequence` / `max_sequence` are the union of its sources'
    ranges; compaction never narrows sequence coverage.
- Compaction never crosses a partition boundary. This keeps TTL drops independent and
  avoids rewriting old days while new data arrives.
- Trigger policy: merge when a partition has more than 8 small parts, or when small-part
  bytes exceed 25% of the partition, or at partition close. The exact thresholds are
  configuration, not format.

### 6.2 Deduplication semantics

Storage keeps a point-wise LWW rule, not a time-interval dedup rule:

- for the same `(series_id, timestamp)`, the row with the highest `__seq__` wins;
- inner/outer compaction may drop lower-`__seq__` duplicates because they can never win
  again;
- `__reserved__` expiration/tombstone bits are applied before dropping a row;
- interval dedup ("keep one sample per 1m") is not a storage feature. It belongs to
  rollup/downsampling in Phase 3.

This is stricter and simpler than VictoriaMetrics' `dedup_interval`, and it matches the
current `__seq__`-based merge plan. A future interval-dedup mode can be added as an
opt-in compaction policy without changing the file format.

### 6.3 Deletion and expiration

- Series deletion is an index operation: ns4 tombstones hide the series from search.
  Data files are not rewritten immediately.
- Partition expiration is a whole-partition drop (section 8.3).
- A Phase 3 series GC can rewrite a partition to physically remove tombstoned series;
  until then, tombstones are logical and the index is the authority.
- `__reserved__` can mark individual rows expired; the reader filters them. A future
  compaction can drop them.

## 7. Read Path and Query Integration

### 7.1 Scan request

The existing `ScanRequest` has `range`, `predicate: Vec<Expr>` and `projections`. TSF
adds two optional fields used by the metric engine's index-driven path:

```rust
pub struct ScanRequest {
    pub range: TimeRange,
    pub predicate: Vec<Expr>,
    /// `None` means all columns; indices refer to the storage schema.
    pub projections: Option<Vec<usize>>,
    /// Sorted, deduplicated series IDs from the index. `None` means no ID filter.
    pub series_ids: Option<Arc<[u64]>>,
    /// Optional `[start, end)` sequence range, used by the migration split.
    pub sequence_range: Option<(u64, u64)>,
}
```

The legacy Parquet path translates `series_ids` and `sequence_range` into `IN` and
comparison predicates, or ignores them if the caller already supplied equivalent
`Expr`s. The TSF path consumes the typed fields directly, avoiding a large `IN` list in
the DataFusion expression tree.

### 7.2 File and page pruning

For each query:

1. Select files whose `FileMeta.time_range` overlaps the requested range and whose
   partition is not dropped.
2. Select files whose `format` has an enabled reader. During migration both Parquet and
   TSF files are selected.
3. For TSF files, use the bloom filter to reject files when the series set is large.
4. For TSF files, select the chunk group by `metric_id`; if the file has no such group,
   skip it.
5. Resolve each requested `series_id` through the sparse series index. If the ID set is
   larger than 1,024, merge-scan the sorted series index and the sorted IDs instead of
   doing a binary search per ID.
6. Read the page index and drop pages outside the time range or `sequence_range`.
7. Read only the payloads needed by the projection. `timestamp` and `value` are
   required for samples; `__seq__` is required internally for LWW; `__reserved__` is
   read only when it can be non-zero.
8. Decode pages into Arrow builders and emit batches of up to 8,192 rows in
   `(series_id, timestamp)` order.

### 7.3 Arrow emission and LWW

TSF pages decode to the same Arrow schema as Parquet:

```
metric_id: UInt64 | tsid: UInt64 | timestamp: Int64 | value: Float64
__seq__: UInt64 | __reserved__: UInt64
```

The reader emits rows in primary-key order within a series chunk. DataFusion's existing
`SortPreservingMergeExec` + `MergeExec(seq LWW)` layer handles duplicates across files
and across Parquet/TSF generations. The TSF reader does not need to deduplicate across
files.

`metric_id` and `tsid` are reconstructed from the chunk headers and prepended to each
decoded batch. `__seq__` and `__reserved__` are appended as built-in columns, matching
the current storage contract.

### 7.4 Caches

| Cache | Budget | Contents |
|---|---|---|
| file index / sparse series index | mem / 20 | decoded indexes, keyed by `FileId` |
| page index | mem / 40 | decoded page indexes for requested series |
| decoded page | mem / 4 | Arrow-independent decoded columns |
| compressed page | mem / 20 | optional, for object-store ranged reads |

- Local FS readers may rely on the OS page cache for compressed pages; the compressed
  page cache is mainly for object storage.
- A decoded page is cached only after its second miss, mirroring the mergeset block
  cache policy.
- Closing a file removes its index entries; eviction is LRU with a memory-size budget.

## 8. Partitioning, TTL and Manifest

### 8.1 Directory layout

```
{root}/data/{YYYYMMDD}/{file_id}.tsf   # TSF parts, grouped by UTC day
{root}/data/{file_id}.sst              # existing Parquet SSTs, unchanged path
```

`YYYYMMDD` is the UTC partition. Existing Parquet files keep their current path; a
format-aware path generator maps `(partition, file_id, format)` to the path, and the
manifest's `format` decides which reader is used. New Parquet files are written only
while `format = "parquet"`. The manifest remains the source of truth.

The partition directory comes from the file's first row, so every file must hold rows
from exactly one UTC day. Neither the flush nor the compaction write path can rely on
its input being that narrow: both group work by time **segment**, and a segment aligns
with a day only when the segment duration divides 24 hours. Both therefore split their
output by UTC day (`tsf::write::write_batches_by_day`) and may produce several files
from one segment. The manifest `time_range` of each output stays the real row coverage,
not the day bounds, so pruning and whole-file TTL remain accurate.

### 8.2 Manifest changes

`FileMeta` gains two fields:

```rust
pub struct FileMeta {
    pub max_sequence: u64,
    pub min_sequence: u64,      // new
    pub num_rows: u32,
    pub size: u32,
    pub time_range: TimeRange,
    pub format: FileFormat,     // new: ParquetV1 | TsfV1
}
```

`format` is encoded as a new protobuf enum field with default `ParquetV1` for old
snapshots/deltas. `min_sequence` is derived from the file's rows and is used by
compaction and migration accounting; a missing field defaults to `0`, which is safe for
old Parquet files. The snapshot and delta formats remain backward compatible; an old
reader sees the file as Parquet, but old binaries cannot read TSF files anyway, so a
downgrade requires dual-read or a rewrite.

### 8.3 TTL and partition drop

Retention is evaluated per partition:

```
expire_time = now - ttl
drop partitions whose end <= expire_time
```

Drop order:

1. commit a manifest delta that removes every file in the partition;
2. delete the partition directory/objects (best effort). A crash after step 1 leaves
   orphan files for GC.

Deleting the directory before the manifest delta would make queries race with missing
files, so the manifest is always updated first. The index's per-day rows are dropped with
the monthly indexDB partition, not here (ADR-0001).

A `partition_duration` other than one day is allowed by the format, but the index and
the TTL tests are defined for daily partitions in Phase 2.

Implementation notes (`columnar_storage::partition::PartitionManager`):

- Partition discovery lists `data/` once with `list_with_delimiter` and accepts only
  `YYYYMMDD` segments; Parquet objects sitting directly in `data/` are not partitions.
  The manifest remains the only source of truth for queries, so listing happens on the
  maintenance path only.
- A directory with zero objects is not reported as a partition: deleting objects does
  not remove the directory on a local filesystem, and treating the leftover as a live
  partition would make a completed drop look pending.
- Both sides of every path comparison go through `object_store::path::Path`, which
  strips the leading `/` that a local-filesystem root adds to the configured prefix.
- Orphan GC deletes objects under `data/` that no manifest entry names. It cannot tell
  an orphan from a flushed/compacted file that has not been committed yet, so it must
  run while writes are quiet; the storage-trait doc comment says so.
- `drop_expired` flushes buffered writes first, so a segment that is still partly in
  the buffer cannot be dropped (or kept) arbitrarily.

## 9. Object Storage

TSF is designed to be one object per part:

- **Atomic publish.** The file is fully written before the manifest commit, so a reader
  never sees a partially uploaded part.
- **Ranged reads.** Opening a file reads the footer (last 64 bytes), then the bloom
  filter and, if it accepts the series set, the file index. Requested series chunks read
  their sparse index block and page index, then pages. Each read is a `get_range`;
  adjacent pages are coalesced.
- **Metadata cache.** Footer, file index and sparse series index are cached by
  `(FileId, offset)`. Object-store latency is paid once per file, not per query.
- **No data-file listing.** The manifest is the data-file list; `list` is never used on
  the query path. Manifest recovery may list delta objects, as it does today.
- **Upload.** The existing `object_store` abstraction is used. A multipart upload is
  allowed; the object becomes visible only after completion. Local FS uses temp +
  fsync + rename.
- **Phase 2 boundary.** Native object-store parts (no local staging), cold/hot tiering and
  remote page cache are Phase 4. Phase 2 targets local FS and compatible object stores
  with a local cache.

## 10. Migration and Rollout

Phase 2 ships behind:

```toml
[storage.time_merge_storage]
format = "tsf"                 # "parquet" | "tsf"; defaults to "parquet"

# Not implemented yet: page sizing is fixed at the kernel default
# (8,192 rows) and compression is chosen per page by the encoder.
# partition_duration = "1d"
# page_rows = 8192
# page_size = "1MiB"
# compression = "zstd"
```

`format` is the switch implemented in Phase 2 (`StorageConfig::format`). It applies
to the sample data table only; metadata tables stay on Parquet because TSF v1 has a
single fixed schema. Page rows and compression are structural constants of the
current writer and are not exposed as config yet.

Migration procedure:

1. Keep writing Parquet until the switch is enabled; the data WAL is unchanged.
2. Flush all pending writes, then set `format = "tsf"`. New flushes write TSF files;
   existing Parquet SSTs stay in the manifest with `format = ParquetV1`.
3. Queries read both formats and merge through the existing DataFusion LWW plan. The
   index migration split from ADR-0001 is orthogonal: the index path is chosen by
   `__seq__`, while the data reader is chosen by `FileMeta.format`. A query may
   therefore combine a legacy index with a TSF file or a new index with a Parquet file.
4. `tsf::rewrite_to_tsf` (exposed as `ColumnarStorage::rewrite_legacy_files`) reads
   Parquet files, writes TSF files, and swaps them in the manifest. It is not a
   background job yet: the caller decides when to run it, and repeated passes
   converge because TSF files are skipped. Files that span more than one UTC day are
   skipped and counted, since splitting them needs page-level work. Once no Parquet
   file remains, the Parquet reader can be removed.
5. Rollback to Parquet is safe only while dual-read is enabled and new TSF files can
   still be read. After TSF-only data exists, a rollback needs a rewrite, not a flag
   flip. This mirrors the index migration caveat.

The format version is recorded per file, so a future `tsf-v2` can coexist with `tsf-v1`
during its own migration.

## 11. Module Layout and Licensing

```
src/storage_kernel/src/tsf/
  mod.rs
  file.rs            # header/footer/index layouts, reader/writer
  page.rs            # page build/decode, statistics
  partition.rs       # pure UTC-day partition + TTL planning helpers
  compaction.rs      # pure LWW dedup and compaction policy helpers
  codec/             # const/delta/nearest-delta/varint/ZSTD wrappers
    mod.rs
    int.rs
    float.rs
    nearest_delta.rs
    nearest_delta2.rs
    compress.rs
    encoding.rs
    simd.rs
  bloom.rs           # series bloom filter
  checksum.rs        # crc32c helpers
src/columnar_storage/src/tsf/
  mod.rs
  scan.rs            # ScanRequest -> page reads -> Arrow batches
  write.rs           # sorted RecordBatch -> TSF rows and file bytes
  exec.rs            # TsfExec: DataFusion plan for TSF files (dual-read)
```

The flush path lives in `columnar_storage::buffer` (it owns the WAL and manifest
ordering); TSF compaction output lives in `columnar_storage::compaction::executor`.
`FileMeta.format` and the partition TTL drop live in `columnar_storage::sst` and
`columnar_storage::manifest`.

- `storage_kernel::tsf` has no Arrow or DataFusion dependency. It operates on byte
  slices, typed columns and plain Rust vectors.
- `columnar_storage::tsf` owns Arrow conversion, DataFusion integration, WAL integration
  and manifest updates.
- The codec module is a selective port of `esmetrics/crates/esm-encoding` (Apache-2.0).
  Each ported file keeps its upstream copyright header; `NOTICE` and
  `src/storage_kernel/PORTING.md` record the source path and commit.
- The format design borrows ideas from Apache IoTDB TsFile (chunk groups, column chunks,
  page statistics) but shares no bytes or code.

Dependencies added by Phase 2: `zstd`, `crc32c`, `xxhash-rust`, `bytes`. If the
`crc32c` crate cannot be vendored, the reader dispatches to the SSE4.2
`_mm_crc32_u64` intrinsic at runtime and keeps the software implementation as a
portable fallback. No new DataFusion/Arrow dependency in `storage_kernel`.

## 12. Work Breakdown and Acceptance

| Milestone | Scope | Exit criteria |
|---|---|---|
| T1 | `storage_kernel::tsf` file writer/reader, page codecs, checksums, projected decode buffers, golden byte layouts | round-trip and differential/fuzz tests; corrupted files rejected; 1M-row round trip; uncompressed-page flag honoured |
| T2 | `columnar_storage::tsf` flush path, `ScanRequest` extensions, zero-copy Arrow emission | parity with Parquet for the existing metric-engine test suite; Phase 0 benchmark rerun Arrow-to-Arrow through the real IO path |
| T3 | partition manager, TTL drop, small/big compaction, dedup, crash recovery, orphan GC | kill -9 tests; TTL drop leaves no query-visible files; compaction reduces part count |
| T4 | manifest format field, dual-read, rewrite job, config switch, metrics | mixed Parquet/TSF queries return identical results; rewrite removes Parquet files |
| T5 | benchmarks vs Parquet and TsFile-style pages | size, write throughput, query p99, page-skip ratio, recovery time |

Decode-side detail (buffer reuse, column projection, index cache, zero-copy Arrow
emission, hardware CRC, SIMD kernels, parallel page decoding) is in
`docs/rfcs/20261029-vectorized-decode-plan.md`. T1 additionally requires differential
parity against the scalar decoder on all codecs; T2 requires at least 2x the prototype
scan throughput with selective-query latency unchanged or better.

### 12.1 Implementation status (2026-11-08)

Done:

- **T1** in `storage_kernel::tsf`: file/page codecs, CRC32C with hardware dispatch,
  projected decode buffers, reusable page buffers, a golden page-header layout test,
  a 1M-row round-trip test and the §4.8 uncompressed-page flag. The randomized
  differential suite runs 128 pages per commit and 100k pages as an ignored nightly
  test; `cargo-fuzz` targets are not wired yet.
- **T2 boundary** in `columnar_storage::tsf`: `DecodedColumns`, zero-copy Arrow
  emission, Arrow sequence filtering and the 2x-plus full-scan result measured in
  the decode plan.
- **T2 flush path**: `FlushState` writes either Parquet or TSF per
  `StorageConfig::format`; `columnar_storage::tsf::write::{batch_to_rows,
  encode_batch}` converts sorted batches and rejects schemas other than the sample
  `(metric_id, tsid, timestamp, value)`. TSF output is written to
  `{root}/data/{YYYYMMDD}/{file_id}.tsf`. Because flush groups frames by segment and
  a segment only aligns with a day when the duration divides 24h,
  `write_batches_by_day` splits a straddling segment into one file per UTC day and
  commits them in a single manifest update. Covered by
  `test_storage_tsf_flush_and_scan` and `test_storage_tsf_flush_splits_on_day_boundary`.
- **T3** in `storage_kernel::tsf`: pure UTC-day partition/TTL helpers and pure
  compaction policy/LWW-dedup helpers; `Manifest::plan_ttl_drop` and
  `drop_expired_partitions` drive a manifest-first, object-delete-after TTL drop
  (`test_plan_and_drop_ttl_partitions`). Compaction output honors the configured
  format, splits TSF output by UTC day for the same segment-straddling reason, and
  never narrows or widens the `(min_sequence, max_sequence)` span of its inputs
  (`test_merge_inputs_splits_tsf_output_by_day`).
- **T4** in `pb_types`/`columnar_storage`: `SstMeta` gained `min_sequence` (5) and
  `format` (6) with the new `FileFormat` enum, defaulting to `PARQUET_V1` so old
  manifests and deltas still parse. Snapshot records are versioned (v1 = 32 bytes,
  v2 = 44 bytes) with v1 dual-read that upgrades on the next rewrite. The query path
  partitions SSTs by format in `build_df_plan` and unions `ParquetExec` with the new
  `TsfExec`, so a root written by both formats scans as one merge stream
  (`test_storage_dual_read_parquet_and_tsf`). `tsf::rewrite_to_tsf` migrates the
  Parquet SSTs left behind by a format switch, exposed as
  `ColumnarStorage::rewrite_legacy_files`, and converges when repeated
  (`test_rewrite_parquet_files_to_tsf`, `test_storage_rewrite_legacy_files`).
- **T2 range reads:** `TsfReader` is built from byte segments (`from_segments`), so a
  reader can hold any subset of a file. `FooterLayout` parses the footer alone and
  names the header/bloom/index ranges; `next_scan_ranges` then walks chunk-group
  header -> series index -> series index blocks -> series chunk header -> page index
  one level per round, and `page_ranges_for_metrics` returns the exact pages a window
  needs. `TsfExec` drives those rounds against the object store, coalescing only
  across small gaps (the store's own `get_ranges` merges up to 1 MiB, which would
  defeat the plan), and decodes only the intersection of the scan range and the file
  range (`test_ranged_read_fetches_only_touched_pages`,
  `test_staged_scan_planner_reads_only_touched_pages`). Files at or below 512 KiB are
  still read whole, where the round trips cost more than the bytes they save.
- **T3 partition manager / GC:** `columnar_storage::partition::PartitionManager`
  discovers day partitions under `data/` with `list_with_delimiter` +
  `parse_partition_label`, drives TTL drops through `Manifest::drop_expired_partitions`
  and then deletes the objects, and sweeps objects that no manifest entry references.
  Exposed on the storage trait as `partitions`, `drop_expired` and `gc_orphans`
  (`test_list_partitions_groups_by_day`,
  `test_drop_expired_removes_manifest_first_then_objects`,
  `test_gc_orphans_keeps_live_files_and_bytes`,
  `test_storage_drop_expired_and_gc_orphans`).

Follow-up (not yet implemented):

- **T3 compaction policies:** small/big compaction-job selection beyond the current
  time-window strategy, and kill -9 crash-recovery tests (WAL replay and orphan
  cleanup are implemented, the fault-injection suite is not).
- **T4 per-format metrics:** write/read counters are not split by format yet. The
  rewrite job currently skips files that span more than one UTC day, since splitting
  them needs page-level work.
- **T5:** the full size/write-throughput/query-p99/recovery benchmark matrix.

Acceptance targets (from the roadmap, made measurable):

- **Size:** at most 2.5 bytes/sample compressed for a 15-second scrape workload
  (timestamp + value + `__seq__`), with a stretch target of 1.5 bytes/sample when
  `__seq__` is constant per flush.
- **Write throughput:** at least 90% of the current Parquet path on the same hardware,
  measured after WAL and manifest commit.
- **Full scan:** at least 15 M rows/s on the Phase 0 wide gauge workload by the end
  of the decode plan, keeping TSF within roughly 2x of Arrow/Parquet.
- **Query latency:** a 30-day, single-series query p99 under 50 ms; a 1-day Grafana-style
  query p99 under 50 ms.
- **Page pruning:** on a 30-day dataset, a 1-hour query reads at most 5% of the pages of
  the matching series chunks.
- **Recovery:** `kill -9` recovery under 30 seconds with no manifest/file inconsistency
  and no lost acknowledged write.
- **TTL:** dropping a partition is one manifest delta plus a directory delete; the cost
  does not grow with the row count. The drop is observable in metrics and no query
  returns dropped-partition data.

Phase 0 prototype results (`docs/rfcs/20261028-phase0-benchmark.md`): the counter
workload meets the size target at 1.57 B/sample, and selective queries are 28-41x
faster than Parquet scan-and-filter. Full-scan decode is 4-7x slower than Arrow, so
vectorized page decoding and direct Arrow emission are mandatory before T2 lands.
Write throughput is 70-99% of Parquet, below the 90% target on the noisiest gauge
shape; buffer reuse and cheaper codec selection are the first optimizations. The
decode follow-up is specified in `docs/rfcs/20261029-vectorized-decode-plan.md`.

## 13. Observability

Expose from T1/T2:

- files written/read, bytes written/read, bytes per sample, rows per page, pages per
  series chunk;
- compression ratio by codec, codec selection counts, fallback-to-plain counts;
- page-skip ratio, index-cache and page-cache hit ratios, bloom rejections;
- compaction input/output bytes, files merged, deduplicated rows, write amplification;
- flush duration, fsync/upload duration, manifest commit latency, WAL truncation lag;
- partition drop count and duration, orphan files GCed;
- corrupt files, CRC failures, late writes outside retention.

# Drawbacks

- **Custom format.** No external tool can read TSF; debugging needs in-repo tooling and
  golden tests. Parquet's ecosystem (DuckDB, Spark, `parquet-tools`) is lost.
- **Compaction rewrites whole series chunks.** A late sample in a large file forces a
  rewrite of the affected series chunks (or a new small part); the write amplification is
  higher than an LSM with per-series memtables.
- **Single-object parts.** Compaction reads and rewrites entire objects; object-store
  multipart upload is required for 2 GiB parts.
- **Index dependency.** Labels are not stored in the data file, so losing the index loses
  label reconstruction unless the open question below is resolved.
- **Page-size tuning.** 8,192 rows is a compromise: smaller pages improve pruning but
  increase index overhead; larger pages compress better but waste decode work.
- **Two data formats during migration.** Dual-read, format-aware compaction and a rewrite
  job add operational complexity.
- **ZSTD CPU cost.** Compression/decompression competes with ingest and query threads;
  the codec level is a tuning knob, not a free win.
- **`u32` file size.** The current manifest limits a file to 4 GiB; larger partitions need
  multiple files or a manifest change.

# Alternatives

1. **Keep Parquet and tune row groups/encodings.**
   Lowest risk, preserves the ecosystem. But Parquet cannot seek per series, its
   statistics are row-group scoped, and its encodings do not specialize for monotonic
   timestamps or gauge floats. Rejected for Phase 2; retained as the migration format.

2. **Adopt VictoriaMetrics parts verbatim.**
   Proven format and codecs, and it matches the index's block model. It uses multiple
   files per part (`timestamps.bin`, `values.bin`, `index.bin`, `metaindex.bin`,
   `parts.json`), which is less convenient for atomic object-store publication and makes
   a part a directory rather than an object. The codec ideas are reused; the multi-file
   layout is rejected.

3. **Use Apache TsFile byte-for-byte.**
   Mature chunk-group/page design, but no ecosystem requirement in this project, a large
   Java-oriented spec surface, and features (aligned series, schema templates) that the
   single-value model does not need yet. Rejected; the design is borrowed selectively.

4. **One file per series.**
   Simplest reader and best pruning for a single series, but millions of tiny objects,
   expensive metadata, and poor object-store behavior. Rejected.

5. **An LSM for data (per-series memtables + sorted runs).**
   Natural for out-of-order writes and dedup, but time-range queries would scan many runs,
   and partition drops would have to delete keys from every run. The current
   time-partitioned files + compaction model fits the retention requirement better.
   Rejected for Phase 2.

6. **Device wide table with aligned timestamps.**
   Excellent for multi-sensor devices and reduces timestamp storage. Rejected for Phase 2
   because the query API is metric-scoped and the schema/null/semantics work is large; see
   Open Questions.

7. **Per-table (per-metric) format override, selectable at creation.**
   Keep both readers and writers and let each metric pick `ParquetV1` or `TsfV1`, with a
   global default in config — the `CREATE TABLE ... ENGINE=` shape familiar from SQL
   systems. The query path already supports it, because readers are chosen per file by
   `FileMeta.format` and results merge through the same LWW plan. The costs are that the
   Parquet path can never be removed, the test matrix doubles, and flush must route
   samples into format-specific partition buckets since one day partition mixes metrics.
   Deferred rather than rejected: revisit after T4 if reading the data with external
   Parquet tooling is a requirement. The choice should be a `file_format` property on the
   metric record, not a new "storage engine" abstraction, because both formats return the
   same Arrow schema and differ only in physical layout, size and latency.

# Open Questions

- **Device alignment.** Should `tsf-v2` group series chunks by `(metric_id, job_id,
  instance_id)` and share a timestamp column per device? This requires storing the
  grouping fields in the series chunk header, a multi-series scan shape and a
  schema/template concept.
- **Typed values.** When do integer/boolean/string measurements arrive, and do they share
  the same file version or a new `schema_id`?
- **Canonical key in the data file.** Storing the escaped canonical series name in the
  series chunk header would allow index rebuild from data at the cost of space. Decide
  after the index checkpoint design.
- **Page size and codec per workload.** IoT scrape intervals (15s/60s) and historical
  backfills may want different page rows and precision bits. The format supports it; the
  policy is open.
- **Per-table format override.** Should a metric be able to pin `file_format`
  independently of the global default (Alternative 7)? If yes, the choice is persisted in
  the metrics metadata table, flush buckets samples by format, compaction never merges
  across formats, and changing a metric's format becomes a rewrite. It also makes the
  Parquet path permanent. Decide after T4 based on whether Parquet-ecosystem access
  (DuckDB, Spark, `parquet-tools`) is a real requirement rather than an internal
  performance preference.
- **Object-store-native layout.** Whether a future version splits a part into footer +
  per-series objects for finer ranged reads, or keeps one object and relies on caching.
- **Compaction cadence and dedup interval.** Daily close compaction is assumed; whether
  to add interval dedup or tiered compaction is a Phase 3 decision.
- **Late-write policy.** The proposed `OutOfRetention` rejection is stricter than
  accepting late data into a dropped partition. Confirm against operator expectations.
- **Manifest file size.** Extending `FileMeta.size` to `u64` would remove the 4 GiB cap;
  it needs a manifest version bump and a compatibility plan.

# References

- `docs/rfcs/20261028-mergeset-index-adr.md` (ADR-0001: TSID, indexDB, dual-read)
- `docs/rfcs/20261029-vectorized-decode-plan.md` (Phase 2 decode plan)
- `/media/wanli/WD_SSD1/home/wanli/workspace/esmetrics/crates/esm-encoding/src/`
- `/media/wanli/WD_SSD1/home/wanli/workspace/esmetrics/crates/esm-storage/src/block_header.rs`
- `/media/wanli/WD_SSD1/home/wanli/workspace/esmetrics/crates/esm-storage/src/metaindex_row.rs`
- `/media/wanli/WD_SSD1/home/wanli/workspace/esmetrics/crates/esm-storage/src/partition/`
- Apache IoTDB TsFile format and encoding documentation (chunk group, column chunk, page,
  TS_2DIFF, GORILLA/CHIMP, RLE, dictionary)
- `docs/roadmap.md`, Phase 2
