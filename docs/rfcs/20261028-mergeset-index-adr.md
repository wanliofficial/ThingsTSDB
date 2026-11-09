# ADR-0001: Mergeset LSM Index and TSID-Based Series Identity

- Status: Partially implemented (M1)
- Date: 2026-10-28
- Implemented: 2026-11-09 (M1 part layout and search; see §12.1)
- Related: `docs/roadmap.md`, `docs/rfcs/20240827-metric-engine.md`, `docs/3high-design.md`
- Reference implementation: `esmetrics` (Rust port of VictoriaMetrics v1.146.0,
  commit `4d9901f`), Apache-2.0

# Summary

Replace the current Parquet wide-table index (`series` + `index` tables scanned with
DataFusion predicates) with a **mergeset-style LSM tree over sorted opaque byte items**,
plus a **24-byte TSID** and an **indexDB** of namespaced index rows.

The mergeset owns only the index. Time-series data continues to use the existing
Arrow/Parquet path in Phase 1; the new data-file format is Phase 2. Historical Parquet
data remains readable through a dual-read path that separates the legacy and new
generations by `__seq__`.

This ADR fixes the on-disk item/part formats, TSID layout and allocation, index
namespaces, tag-filter semantics, merge policy, durability ordering, and the seam with
`metric_engine/query.rs`. It does not authorize porting code yet; Phase 1 milestones are
listed at the end.

# Motivation

The current inverted index stores one row per `(metric_id, tag_key, tag_value, tsid)` in
a Parquet table. Every label matcher becomes a separate predicate scan over the metric's
index rows, followed by an in-memory `HashSet` intersection. This has four structural
problems:

1. **Cost grows with index size, not with the result.** `job="api"` is evaluated by
   scanning Parquet rows under a predicate; even with min/max pruning the engine
   materializes candidate rows instead of reading one posting list.
2. **No regex support.** Prometheus `=~` / `!~` cannot be pushed down; adding them to a
   table scan is even more expensive.
3. **Row amplification.** A series with 10 labels produces 10 index rows, and every
   re-registration attempt must be deduplicated in process memory.
4. **Index and data compaction interfere.** Both are Parquet SSTs merged by the same
   manifest machinery, so index merges compete with data merges and are constrained by
   the data file TTL model.

At the target scale (10M-100M series) the index needs:

- ordered prefix scans instead of predicate scans,
- posting lists that can be unioned/intersected with `uint64` sets,
- incremental registration and background merge instead of whole-table rewrites,
- time-aware index pruning so a query only touches the relevant partitions,
- a series identity that can be sorted, compacted and used as a data-file key.

VictoriaMetrics demonstrates this design at scale. `esmetrics` provides a reviewed Rust
port with concrete formats and constants, which reduces both risk and implementation
time.

# Details

## 1. Decision

We will:

1. Implement a mergeset LSM table over sorted byte-string items, with in-memory blocks,
   in-memory parts, file parts, background merge workers and block caches.
2. Introduce a 24-byte `Tsid` as the series identity:
   `metric_group_id: u64 | job_id: u32 | instance_id: u32 | series_id: u64`.
3. Store index rows in namespaced indexDB tables, one indexDB per calendar month,
   covering global postings, series ID mappings, tombstones and per-day rows.
4. Keep time-series data in the existing Parquet path during Phase 1. The data table's
   `tsid` column becomes the allocated `series_id`, replacing the current
   `seahash(series_key)` for new writes.
5. Run the index on local disk in Phase 1, with a snapshot/checkpoint interface designed
   for object storage in Phase 4.
6. Put the new code in a new workspace crate `storage_kernel` with `mergeset` and
   `indexdb` modules, free of Arrow/DataFusion dependencies.

Non-goals for Phase 1:

- New TsFile-style data format (Phase 2).
- PromQL/MetricsQL execution and rollup tables (Phase 3).
- Multi-node sharding, Raft replication and object-store-native parts (Phase 4).
- Byte-level compatibility with VictoriaMetrics parts or TsFile.
- Changing the public `SearchRequest` result shape or the storage manifest format.

## 2. Identity Model

### 2.1 Canonical metric name

Inside the index a series is identified by a canonical, escaped encoding:

```
marshal_tag_value(metric_group)
  + marshal_tag_value(tag0.key) + marshal_tag_value(tag0.value)
  + ...
```

where tags are sorted by key and the special `__name__` label is **not** stored as a
tag; its value becomes `metric_group`. This is the VictoriaMetrics encoding:

| Byte | Meaning |
|---|---|
| `0x00 '0'` | escaped `0x00` |
| `0x00 '1'` | escaped `0x01` |
| `0x00 '2'` | escaped `0x02` |
| `0x01` | tag separator (terminates a key or value) |
| `0x02` | key/value separator |

Escaping is required because prefix scans treat `0x01` as the terminator. The existing
length-prefixed `Sample::series_key()` remains the API/legacy encoding but must not be
used for index keys: it does not preserve the prefix ordering that tag filters rely on.

### 2.2 TSID

The marshaled TSID is exactly 24 bytes, big-endian:

| Offset | Size | Field | Source | Purpose |
|---|---|---|---|---|
| 0 | 8 | `metric_group_id` | `xxhash64(metric_group, 0)` | groups all series of one metric name |
| 8 | 4 | `job_id` | `xxhash64(tags[0].value, 0)` truncated | groups by first sorted tag |
| 12 | 4 | `instance_id` | `xxhash64(tags[1].value, 0)` truncated | groups by second sorted tag |
| 16 | 8 | `series_id` | encoded allocation (see 2.3) | globally unique series identity |

Lexicographic order of the tuple is the physical sort order. `metric_group_id`,
`job_id` and `instance_id` are grouping hints only; collisions affect locality, not
correctness. `series_id` is the unique key.

Note on naming: upstream VictoriaMetrics calls the last field `metricID`. In this
project `metric_id` already means "hash of the metric name" and is a data-table column,
so the field is named `series_id` in Rust while the wire layout keeps the upstream
8-byte slot. `metric_group_id` is an index-local grouping hash; it is **not** the data
table's `metric_id` column, which keeps the existing metric-name hash for partition
pruning.

### 2.3 Series ID allocation

`series_id` is **globally unique by construction**, including across month partitions
and future nodes. It encodes its origin instead of relying on a shared allocator:

```
bits 63..56  node_id       u8   0 in Phase 1, up to 256 nodes later
bits 55..40  partition_id  u16  months since 1970-01 (valid for ~5,400 years)
bits 39..0   counter       u40  dense per (node, month, indexDB)
```

- The high bits are constant within a partition, so IDs remain dense in the low 40 bits
  and `uint64set` intersections keep their fast path.
- The counter is checkpointed every 65,536 allocations and on clean shutdown. Each
  `partHeader` also records `MaxSeriesId`, and each index WAL registration frame carries
  the allocation high-water mark. Recovery takes
  `max(persisted_watermark, WAL_high_water, max(part.MaxSeriesId)) + 1`, so an ID is
  never reused even after an unclean shutdown or loss of the small watermark file.
- A fresh indexDB starts its counter at 0. This differs from upstream's UnixNano seed,
  but the partition and node bits already guarantee global uniqueness, and recovery
  covers the crash window without leaving a multi-trillion-ID gap.
- Hash collisions on the grouping fields are ignored. A duplicate registration of the
  same canonical metric name is prevented by the lookup path, not by ID hashing.

Upstream VictoriaMetrics allocates this slot from a UnixNano-seeded counter. The encoded
layout above is a deliberate deviation: it keeps the 24-byte wire size and lexicographic
sort semantics while making IDs safe to mix across partitions in one data-table scan.

### 2.4 Legacy identity

Existing data files use `tsid = seahash(series_key)`. New writes use encoded
`series_id` values. We do **not** rewrite historical data and do **not** map old IDs to
new IDs in the index. Instead:

- at migration, flush all pending writes and record
  `migration_seq = max(FileMeta.max_sequence)` (a data-table `__seq__` watermark,
  unrelated to the mergeset's internal item sequences);
- legacy rows have `__seq__ <= migration_seq`; new rows have `__seq__ > migration_seq`
  (every data row already carries `__seq__`);
- while legacy data exists, a query runs both index paths for the full time range and
  merges results by canonical series key; a duplicate timestamp is resolved by the
  higher `__seq__`, preserving the storage LWW rule;
- the seq predicate makes the split exact even if a compaction merges old and new rows
  into the same file, and even if post-migration writes backfill old timestamps.

After the retention window (or after Phase 2 rewrites old data) the legacy tables and
the seq predicates are deleted. See section 10.

## 3. Mergeset Table

### 3.1 Item model

An item is an opaque sorted byte string of at most 64 KiB. Duplicates are allowed:
index rows are written idempotently, and a row that already exists can be written again
after recovery without corrupting the table. Reads and merge callbacks deduplicate.

```rust
struct Item { start: u32, end: u32 } // offsets into a shared data buffer

struct InmemoryBlock {
    common_prefix: Vec<u8>, // common prefix of all items in the block
    data: Vec<u8>,          // concatenated items, each including the prefix
    items: Vec<Item>,
}
```

- `data` plus a new item must not exceed 64 KiB; the writer starts a new block otherwise.
- Items longer than 64 KiB are dropped with a throttled warning and a counter.
- `common_prefix` is recomputed from the first and last item after sorting; blocks with
  fewer than 2 items have an empty prefix.

### 3.2 Block encoding

A block is written as one `itemsBlock` plus one `lensBlock`. The first item and the
common prefix live in the block header, not in either block.

**Plain** (selected when the block is small or compression saves less than 10%):

- `itemsData`: items `[1..]` with `commonPrefix` stripped;
- `lensData`: one 8-byte big-endian length per item in `[1..]`.

**ZSTD** (`marshalType = 1`):

- for each item `[1..]`, compute the prefix length shared with the previous item after
  stripping `commonPrefix`;
- `itemsData`: zstd of the concatenated suffixes;
- `lensData`: zstd of two varint arrays: `prefixLen[i] XOR prefixLen[i-1]` and
  `itemLen[i] XOR itemLen[i-1]` (previous starts at 0 and at the first item's length,
  respectively).

Single-item blocks are always plain and are reconstructed from the header alone on
read.

### 3.3 Block header and metaindex

Every data block has one header, stored in the part's index file:

```
varbytes(commonPrefix)
varbytes(firstItem)
u8     marshalType        // 0 = plain, 1 = zstd
u32be  itemsCount         // includes the first item
u64be  itemsBlockOffset
u64be  lensBlockOffset
u32be  itemsBlockSize     // <= 2 * 64 KiB
u32be  lensBlockSize      // <= 2 * 8 * 64 KiB
```

Headers are grouped into index blocks, sorted by `firstItem`, marshaled back-to-back and
zstd-compressed as a unit. An index block's uncompressed size is capped at 64 KiB (a
single header may exceed it; the hard cap when building from one header is 3x).

Each index block has one metaindex row:

```
varbytes(firstItem) u32be(blockHeadersCount) u64be(indexBlockOffset) u32be(indexBlockSize)
```

All metaindex rows are concatenated, sorted by `firstItem`, zstd-compressed and stored in
`metaindex.bin`. They are loaded fully into memory when the part is opened.

### 3.4 Part layout

```
{indexdb}/parts.json                  JSON array of part directory names, sorted
{indexdb}/{:016X}/metadata.json       partHeader (JSON, hex-encoded first/last item)
{indexdb}/{:016X}/metaindex.bin       zstd(metaindexRow*)
{indexdb}/{:016X}/index.bin           zstd(blockHeader*) per index block
{indexdb}/{:016X}/items.bin           itemsData blocks
{indexdb}/{:016X}/lens.bin            lensData blocks
```

`partHeader` records `ItemsCount`, `BlocksCount`, `FirstItem`, `LastItem` and, as a
Phase 1 extension, `MinSeq` / `MaxSeq` (index item sequences, see section 6.3) and
`MaxSeriesId` (allocator recovery, see section 2.3).

`parts.json` is the source of truth for the part list. It is replaced atomically inside
the parts lock: write a temp file, `fsync` the file, `rename` over `parts.json`, `fsync`
the directory. A crash before the rename leaves the old list intact; the orphaned part
directory is removed on the next open.

### 3.5 Read path

Lookup chain:

```
partHeader -> binary search metaindexRows by firstItem
           -> load index block -> binary search blockHeaders by firstItem
           -> read items/lens block -> decode InmemoryBlock
           -> binary search items (prefix-stripped comparison)
```

`TableSearch` is a cursor over an immutable snapshot of parts. `Seek(k)` advances every
part cursor and maintains a min-heap ordered by the current item; `NextItem()` advances
the heap top. Duplicates across parts are **not** removed; index search deduplicates
through `uint64set::Set`.

Three process-wide block caches are shared by all tables:

| Cache | Budget | Contents |
|---|---|---|
| index block cache | 10% of allowed memory | decompressed index blocks |
| data block cache | 25% | decoded `InmemoryBlock` |
| sparse data block cache | 5% | decoded blocks for cache-unfriendly scans |

Keys are `(PartId, offset)`; each part gets a global `PartId(u64)` so caches never hold a
raw pointer. A block enters a cache only on its second miss (`missesBeforeCaching = 2`),
which keeps one-shot scans from evicting hot blocks. Closing a part removes all its
entries.

### 3.6 Merge policy and backpressure

Constants, fixed by this ADR:

| Constant | Value | Purpose |
|---|---|---|
| `MAX_INMEMORY_BLOCK_SIZE` | 64 KiB | block and max item size |
| `MAX_BLOCKS_PER_SHARD` | 256 | raw block batch per write shard |
| `PENDING_ITEMS_FLUSH_INTERVAL` | 1 s | raw items become searchable |
| `MAX_INMEMORY_PARTS` | 30 | backpressure on index ingestion |
| `DATA_FLUSH_INTERVAL` | 5 s | in-memory part becomes a file part |
| `DEFAULT_PARTS_TO_MERGE` | 15 | max sources per merge |
| `MIN_MERGE_MULTIPLIER` | 1.7 | reject too-small sources |
| `MAX_PART_SIZE` | 400 GB | hard cap for a merged part |
| zstd level | -5..3 | by output item count, -5 for small parts |

Part selection sorts candidates by size and scans contiguous windows of length
`8..=15`. A window is rejected if it is too unbalanced
(`a[0].size * len(a) < a[last].size`) or exceeds `maxOutBytes`. Its score is
`sum(sizes) / a[last].size` (write-amplification proxy); the best window wins. A merge
runs only if the best score is at least `7.5`.

- In-memory ingestion is backpressured solely by the 30 in-memory part slots.
- `cpus` in-memory merger threads and `max(cpus, 4)` file merger threads run
  independently.
- Merges are stopped by a stop channel; an interrupted merge leaves sources untouched.
- Parts whose item count exceeds the cached-part threshold are written in a way that
  does not pollute the page cache; the merge itself still uses the normal compression
  level selection.

### 3.7 Merge callback

`merge_tag_to_metric_ids_rows` is applied to blocks containing ns1/ns6 rows during a
merge. Rows with the same `(date, tagKey, tagValue)` prefix are combined into one row
with a sorted, deduplicated posting list of at most **64** IDs. If combining rows would
make the block unsorted (only possible when concurrent writers inserted duplicate IDs),
the block is written unchanged and retried in a later merge. The callback is allowed to
rewrite bytes but must preserve sortedness and stay within the block's first/last item
bounds; the mergeset checks this.

## 4. IndexDB Layout

One indexDB exists per calendar month at `{root}/indexdb/{YYYY_MM}`. It owns a mergeset
directory, a TSID allocator, caches and a deleted-series set. The first byte of every
item is a namespace:

| Prefix | Row layout | Purpose |
|---|---|---|
| 0 | `metricName + 0x02 + TSID(24B)` | canonical name -> TSID; used for ingestion lookup |
| 1 | `tagKey + tagValue + seriesID(8B)*` | global posting list |
| 2 | `seriesID(8B) + TSID(24B)` | series ID -> TSID |
| 3 | `seriesID(8B) + metricName` | series ID -> canonical name; label reconstruction |
| 4 | `seriesID(8B)` | tombstone for a deleted series |
| 5 | `date(8B) + seriesID(8B)` | series with data on a day |
| 6 | `date(8B) + tagKey + tagValue + seriesID(8B)*` | per-day posting list |
| 7 | `date(8B) + metricName + 0x02 + TSID(24B)` | per-day name lookup |

Notes:

- `tagKey == ""` is the MetricGroup row: its value is the metric group. This lets a
  `{__name__="foo"}` filter share the same prefix scan path as ordinary tags.
- Composite tags use key prefix `0xfe`:
  `0xfe | varuint(len(name)) | name | key`. The composite key is placed in the tag-key
  slot and escaped/terminated like any other key. A composite row is written for every
  `(metric group, tag key)` pair so a filter with a known metric name can seek
  `composite(foo, bar)="baz"` directly instead of intersecting `foo` with `bar`.
- Posting lists use fixed-width 8-byte big-endian IDs; rows are capped at 64 IDs.
  Variable-length encoding was considered and rejected: it would make the merge callback
  rewrite neighboring rows and complicate in-place block validation.
- Prefix 0 (global name lookup) is written **always**, even when the per-day index is
  enabled. This is a deliberate deviation from VictoriaMetrics: it makes ingestion
  lookup O(1) across days and prevents duplicate TSIDs when a series appears in a new
  day within the same month, at the cost of one extra row per series. The cost is
  measured in M3; if it proves too large, the fallback is to search ns7 across the
  current month and keep an in-flight registration cache.
- `date` is the UTC day number (`unix_seconds / 86400`). Per-day rows are only written
  for days on which a series receives data.

### 4.1 Per-day index

The per-day index (`ns5/ns6/ns7`) is **enabled by default** in Phase 1. It is what makes
the query cost proportional to the requested time range instead of the whole retention
window: a 1-hour query intersects day-sized posting sets instead of 30-day sets, and an
expired day is never touched even before its month partition is dropped.

The cost is write amplification: every series with data on a day contributes one ns5
row, one ns6 row per label pair, and one ns7 row. For low-cardinality IoT labels the row
merge callback collapses these into rows of up to 64 IDs, but the first write of each
day is still proportional to the number of active series. M3 measures this at 10M
series and 30-day retention.

`per_day_index = false` disables ns5/ns6/ns7. Queries then enumerate the global posting
sets (ns1) and rely on the data scan to discard samples outside the time range; negative
queries use the metric-group posting row. This removes the daily write amplification but
gives up time-aware index pruning. The switch exists so M3 can make the decision with
measurements rather than assumption; the default changes only if the measured index
write cost exceeds the write-throughput budget.

### 4.2 Caches

| Cache | Budget | Scope |
|---|---|---|
| `tagFilters -> seriesID set` | mem / 32 | per indexDB |
| `seriesID -> TSID` | mem / 16 | shared across indexDBs |
| `seriesID -> metric name` | mem / 10 | shared across indexDBs |
| regexp / prefix simplification | mem / 20 | process-wide |
| per-day probe caches | bounded working set | per indexDB |

The tag-filter cache is invalidated by the mergeset flush callback, because newly
flushed in-memory parts become searchable. It is also invalidated on series deletion.

## 5. Tag Filters and Query Resolution

Supported matchers: `=`, `!=`, `=~`, `!~`, with Prometheus full-match semantics
(`^(?:expr)$`). Regexes are compiled with `regex::bytes` so non-UTF-8 label values
remain searchable. The planner works as follows:

1. **Simplify regexes.** Extract a literal prefix; if the remainder is empty, the
   matcher becomes a plain equality. If the regex is an alternation of literals, use the
   or-values fast path and union exact prefixes.
2. **Order filters by cost.** Composite filters first, then full match, prefix, literal,
   suffix, middle, and finally regex. `__name__`-only filters are converted into
   composite filters when possible.
3. **Intersect posting sets.** Start from the most selective positive filter, then
   intersect. Negative-only queries first enumerate the candidate set from ns5
   (`date -> seriesID`) or from the metric group's posting list, then subtract.
4. **Apply time range.** When the per-day index is enabled, rows in ns5/ns6/ns7
   constrain the search to the days covered by the query. The planner intersects the
   per-day candidate set with the global posting set; a small per-day row count is
   cheaper than scanning the global tag posting list. When it is disabled, the time
   range is applied only at the data scan.
5. **Convert IDs to TSIDs.** Look up ns2, with the `seriesID -> TSID` cache in front.
   TSIDs are used for deduplication and limit accounting. The data table stores only the
   trailing 8-byte `series_id`, so the query layer extracts `tsid.series_id()` for the
   scan and sorts those IDs.
6. **Enforce limits.** `max_metrics` returns `TooManyTimeseries`; a search deadline
   returns `DeadlineExceeded`. The limit is enforced per indexDB and re-checked after
   results are merged by canonical series key, because one logical series may have one
   TSID per month partition. Loops are paced so a long regex scan cannot monopolize a
   query thread.

Deleted series (ns4) are loaded into an in-memory `Set` at indexDB open and subtracted
after step 3.

Search returns `Vec<Tsid>`. Duplicate rows from different parts or overlapping
partitions are harmless because posting sets deduplicate by `series_id`.

## 6. Durability and Crash Consistency

### 6.1 Ordering

The invariant is: **no durable data row may reference a series ID that is not
recoverable**. To enforce it:

1. Canonicalize the series and look up its TSID in ns0 / cache.
2. If absent, allocate `series_id`, reserve an index item sequence range with
   `Table::reserve_seq_range(n)`, append the registration items (ns0, ns1, ns2, ns3)
   with that range to the **index WAL**, and fsync.
3. If the TSID exists but the current UTC day has not been indexed for this series
   (per-day probe cache miss), reserve a sequence range, append the per-day items
   (ns5, ns6 with 64-ID row splitting, ns7) with that range to the **index WAL**, and
   fsync. Skip this step when `per_day_index = false`.
4. Append the data batch to the existing **data WAL**, and fsync.
5. Only then acknowledge the write (when strict durability is enabled).
6. Feed index items into the mergeset shards with `add_items_with_seq_range` and the
   data batch into the data buffer.

A crash after step 2 or 3 but before step 4 leaves unreferenced index rows. They are
idempotent and harmless; a later registration produces identical items. A crash can
never leave a durable data row with a missing index row, because the index WAL is always
fsynced first.

The ordering guarantee applies when `wal.sync_on_write = true`. If fsync is disabled,
both WALs are best-effort; a crash may keep data frames while losing their registration
frames. Those data rows have no labels and are not automatically repairable in Phase 1,
because the data table does not store series keys. Production deployments must keep
`sync_on_write = true`; disabling it trades durability for latency and must be treated
as potential data loss at the index/data boundary.

Raw mergeset items are not searchable until the 1-second pending-items flusher moves
them into an in-memory part. Concurrent writers of the same new series would therefore
both miss ns0 and allocate two TSIDs. Registration is serialized per canonical name
through a sharded in-flight map (`pending_series`); an entry is removed after the flush
callback signals that its items are searchable. The map only covers the unflushed
window, so it stays small even at 10M series.

### 6.2 Index WAL

The index WAL uses the same framing style as the existing data WAL but a distinct file
extension (`*.iwal`) and frame type:

```
frame header: magic | len | frame_type | index_partition_id
              | min_seq | max_seq | max_series_id | checksum
payload:      Arrow-free custom encoding:
              varuint(count) then per item varuint(len) + bytes
```

The payload is intentionally Arrow-free so `storage_kernel` does not depend on Arrow.
Replaying an index WAL frame calls `Table::add_items_with_seq_range` with the frame's
reserved range; duplicate items are allowed.

### 6.3 WAL reclamation

Add `MinSeq` / `MaxSeq` to `partHeader`. A sequence range is reserved before the WAL
append (`Table::reserve_seq_range(n)`) and attached to the items later
(`Table::add_items_with_seq_range`), so the frame header and the items agree even
though the WAL is written first. A merged part's `[MinSeq, MaxSeq]` is the union of its
sources' ranges, so merging never creates a hole.

A WAL segment records every frame's `[min_seq, max_seq]`. It may be deleted only when
each of those frame ranges is covered by the union of persisted file-part ranges. This is
a per-frame coverage check, not a global low watermark:

- a part can have a high `MaxSeq` while an older mid-range part is still in memory, so
  the maximum `MaxSeq` alone is not sufficient;
- a gap left by a reserved-but-never-appended range does not block reclamation of any
  frame or segment;
- a reserved range whose items never reach a part (crash after WAL append) is replayed
  on recovery and becomes covered then.

The interval union and the pending segments are loaded from part metadata
(`metadata.json` for each entry in `parts.json`) and the WAL directory at open, and are
re-evaluated after every part swap. Replaying a segment that is already covered is safe
because index items are idempotent. A permanently uncovered gap only delays reclamation
of its own segment; it cannot cause data loss. Item-size validation happens before
reservation, which keeps such gaps rare.

### 6.4 Recovery order

1. Open indexDBs: read `parts.json`, open parts, replay remaining index WAL segments.
2. Recover the data storage from its own WAL.
3. Start background workers and the flush callback.

If the index WAL is lost entirely (disk loss), the index can be rebuilt from a snapshot
plus data only if a series catalog is available. Phase 1 does not promise rebuild from
data alone; snapshots are the recovery unit. The `seriesID -> metricName` rows (ns3)
and the canonical name are the catalog.

### 6.5 Snapshot and checkpoint

A checkpoint is a self-contained copy of one indexDB directory (all files plus
`parts.json`), taken while holding the parts lock and after every WAL segment is
covered by persisted parts (so the checkpoint needs no WAL replay). Phase 1 writes
checkpoints to a local path. Phase 4 uploads them to object storage and restores them
into a local cache before opening the table.

## 7. Query Integration

`SearchRequest` keeps its current shape; `MatcherType` gains `Re` and `Nre`.

New flow in `metric_engine/query.rs`:

```
search(req):
  metric_group = req.metric_name
  for each month partition overlapping [start, end):
      tfss = build_tag_filters(metric_group, req.matchers)   // canonical escaped form
      day_range = days_in_month ∩ [start, end)
      new_tsids += indexdb.search_tsids(tfss, day_range, MAX_METRICS, deadline)
  new_tsids.sort_unstable(); new_tsids.dedup();
  new_series_ids = new_tsids.iter().map(|t| t.series_id().0);
  new_series_ids.sort_unstable(); new_series_ids.dedup();

  new_batches = data.scan(metric_id = existing_metric_name_hash(metric_group),
                          tsid IN new_series_ids, time range, __seq__ > migration_seq)
  labels_new = ns3 lookup for each new series_id (cache in front)

  if legacy data exists:
      legacy_tsids = legacy_index.lookup(metric_group, req.matchers)
      legacy_batches = data.scan(metric_id = existing_metric_name_hash(metric_group),
                                 tsid IN legacy_tsids, time range,
                                 __seq__ <= migration_seq)
      labels_legacy = legacy series table lookup

  merge samples per canonical series key; for a duplicate timestamp keep the
  sample with the higher __seq__; sort by series key for deterministic output
```

Key properties:

- The index search returns globally unique TSIDs; the data table is sorted by
  `(metric_id, tsid, timestamp)`, so the scan is a set of range reads, not a full scan.
- New and legacy samples are scanned separately with disjoint `__seq__` predicates.
  This removes any ambiguity from a legacy `seahash` ID numerically colliding with an
  encoded `series_id`, and it works even if a compaction merges both generations into
  one file.
- Label reconstruction for new data comes from ns3 instead of a separate `series` table
  scan. The legacy `series` table is used only for the legacy scan.
- The legacy index has no regex support. A regex matcher on the legacy path falls back
  to an in-memory filter over the legacy index rows for the metric; this is temporary,
  bounded by the retention window, and never used once legacy data is gone. Legacy
  `series_key` values are decoded and re-encoded into the canonical form before the
  result merge.
- `matchers` are pushed into the index, not into DataFusion. DataFusion still handles
  the parquet scan, time filtering and merge of duplicate timestamps.
- A metric-name-only query uses the MetricGroup posting row, not a full index scan.
- A query with only negative matchers enumerates the per-day ID set first; this is the
  expensive case and is explicitly rate-limited by `max_metrics` and the deadline.

The old `lookup_index`, `fetch_all_tsids`, and `fetch_series_keys` functions are removed
for the new path. The legacy path keeps its own implementations behind the
seq-generation split.

## 8. Retention and Deletion

- **Data partitions** continue to use the existing `segment_duration` + file-level TTL
  in Phase 1. Phase 2 replaces this with whole-partition drops (default: one day), which
  become O(1) directory deletes.
- **IndexDB partitions** are dropped whole at month granularity. Per-day rows for days
  whose data has expired remain until the month partition is dropped; this is bounded
  garbage (at most one month of rows) and is never returned by a query because the
  planner only seeks the requested days. Label-name/value APIs must intersect with the
  requested time range for the same reason, since ns1/ns3 rows also live until the month
  drop.
- **Series deletion** appends ns4 tombstones in batches, updates the in-memory deleted
  set, and resets the tag-filter cache before acknowledging. The tombstones are
  persisted through the same index WAL and mergeset path.
- A background series GC (Phase 3) scans retained data and tombstones series with no
  remaining data. In Phase 1, deletion is driven by API calls and partition drops only.
- Prefix 3 (series ID -> metric name) is the canonical label source. Dropping a month
  partition removes the labels of its series, so a series that continues into the next
  month gets a new TSID and a new ns3 row there.

## 9. Concurrency and Lifecycle

- One `Table` per indexDB. `parts_lock` guards the in-memory and file part lists,
  merger spawning, and `parts.json` rewrites.
- Parts are wrapped in `Arc<PartWrapper>`; a `must_drop` flag plus `dec_ref` deletes a
  part directory when the last reader and merger release it.
- Readers take a snapshot of part references under the lock (`incRef`) and release it
  on close. A merge swaps sources for the destination under the lock and rewrites
  `parts.json` before releasing it.
- Write shards are padded to a cache line (`#[repr(align(64))]` or
  `crossbeam_utils::CachePadded`) and selected round-robin; each shard has its own
  mutex, block list and flush deadline.
- Background threads are OS threads (`std::thread`), not tokio tasks. Index merging
  must never occupy a tokio worker; an index merge can run for minutes.
- Stop is a broadcast channel checked between blocks. A stopped merge leaves its sources
  in place and releases its semaphore.

## 10. Migration and Rollout

Phase 1 ships behind a configuration switch:

```toml
[metric_engine.index]
kind = "mergeset"            # "legacy" | "mergeset"
max_inmemory_parts = 30
pending_flush_interval = "1s"
data_flush_interval = "5s"
parts_to_merge = 15
```

Migration procedure:

1. Flush all pending writes, record `migration_seq = max(FileMeta.max_sequence)`, and
   start dual-read.
2. New writes go only to the mergeset index and use encoded series IDs. The legacy
   `series` and `index` tables are frozen (read-only) but are not dropped.
3. While legacy data exists, every query runs both index paths and both data scans,
   separated by `__seq__ <= migration_seq` and `__seq__ > migration_seq`, then merges by
   canonical series key with the higher `__seq__` winning duplicate timestamps.
4. After the retention window has passed (or after Phase 2 rewrites old files), delete
   the legacy tables and the dual-scan path.

Rollback within Phase 1 is supported only while the new path is still dual-read. Once
new data exists only in the mergeset index, reverting `kind` without keeping the
mergeset reader makes that data unqueryable. The rollout therefore requires a backfill
or a forward-fix, not a flag flip, after the first write.

## 11. Module Layout and Licensing

```
src/storage_kernel/
  Cargo.toml
  src/lib.rs
  src/encoding.rs          # big-endian u32/u64, varint, zstd wrappers
  src/mergeset/
    mod.rs
    inmemory_block.rs
    block_header.rs
    metaindex_row.rs
    part.rs
    inmemory_part.rs
    table.rs
    table_merge.rs
    table_search.rs
    blockcache.rs
  src/indexdb/
    mod.rs
    tsid.rs
    create.rs
    search.rs
    tag_filters.rs
    row_merge.rs
    caches.rs
    wal.rs
```

Dependencies: `common`, `parking_lot`, `crossbeam-channel`, `crossbeam-utils`, `zstd`,
`xxhash-rust`, `regex`, `bytes`, `serde`/`serde_json`. No Arrow, DataFusion or
object-store dependency in this crate.

The implementation is a selective port of `esmetrics` (Apache-2.0), itself a port of
VictoriaMetrics v1.146.0 (Apache-2.0). Every ported file keeps the upstream copyright
header, `NOTICE` records the upstream project and commit, and
`src/storage_kernel/PORTING.md` lists the source path for each file. Files that are not
ported (Graphite, PromQL, cluster code) are not copied.

## 12. Phase 1 Work Breakdown

| Milestone | Scope | Exit criteria |
|---|---|---|
| ~~M1~~ | `encoding`, `inmemory_block`, block marshal/unmarshal, part read/write, single-table search | round-trip and fuzz tests; golden byte layouts |
| M2 | TSID, namespaces, row merge callback, tag-filter planner, regex simplification | Eq/Neq/Re/Nre integration tests; 10M-row posting intersection benchmark |
| M3 | in-memory parts, merge selection, background workers, block caches | merge policy unit tests; crash/restart test; write amplification measured |
| M4 | index WAL, segment coverage tracking, recovery ordering, periodic flush callback | kill -9 tests; no orphaned data rows; WAL reclamation verified |
| M5 | `metric_engine` integration, dual-read, config switch, metrics | end-to-end query parity with legacy; throughput and p99 benchmarks |

Acceptance targets from the roadmap:

- index memory under 200 B per series at 10M series;
- Eq/Regex resolution p99 under 5 ms at 10M series;
- write throughput and p99 no worse than the legacy index path;
- `kill -9` recovery under 30 s with no data row referencing a missing series;
- no query regression for the existing `SearchRequest` test suite.

### 12.1 Implementation status (2026-11-09)

Done, in `storage_kernel::mergeset`:

- The part is a **directory** (`metaindex.bin`, `index.bin`, `items.bin`,
  `lens.bin`, `metadata.json`) per §3.3/§3.4; `metadata.json` carries the part
  header with hex-encoded first/last item, item/block counts and the seq and
  series-ID ranges (fields default to zero until M2 populates them).
- `BlockHeader` (`block_header.rs`) and `MetaindexRow` (`metaindex_row.rs`) have
  golden-byte layout tests, and `PartWriter` groups headers into zstd index
  blocks with a fully in-memory metaindex loaded on open.
- `Part::open` walks the metaindex/index-block/header chain, bounds-checks the
  block ranges against the data files and decodes the last block to verify the
  metadata's first/last item, so an unfinished or corrupt part is rejected.
- `TableSearch` is the single-table k-way cursor; `Table::search_range`/
  `search_prefix` are deduplicated on the way out. `Table::merge_all` removes
  its input part directories.
- `parts.json` is written atomically (temp + fsync + rename + directory fsync)
  and rejected unless sorted and deduplicated.
- Tests: 23 mergeset tests (directory layout/metadata, duplicate items across
  blocks, multi-index-block builds, corrupt-directory rejection, parts.json
  atomicity, range search against a brute-force oracle) plus the
  `mergeset_part` fuzz target (30 s smoke run, ~8k executions, no crash).

Not in M1: in-memory parts, background merging (M3) and the index WAL (M4);
`PartWriter` requires pre-sorted items today.

## 13. Observability

Expose counters and gauges from the first milestone:

- items added, blocks flushed, in-memory part count, merge bytes in/out, merge
  duration by level, part count per table;
- WAL bytes, WAL segments awaiting coverage, coverage-check duration;
- index search calls, cache hit ratios, regex fallbacks, deadline/limit errors;
- deleted series count, tombstone rows, per-day row count;
- per-namespace item counts.

# Drawbacks

- **Operational complexity.** A second storage engine (mergeset) must be tuned,
  monitored, snapshotted and tested, on top of the Parquet path.
- **Local disk assumption.** Mergeset parts are optimized for local filesystem
  semantics. Object storage requires a checkpoint/cache layer, which is deferred to
  Phase 4.
- **Write amplification.** Index items are written to a WAL, then to in-memory parts,
  then merged into file parts and again by background merges. The 15-way/1.7 heuristic
  bounds this, but it is still higher than an append-only posting table.
- **Month-granularity index retention.** Dropping data daily leaves at most one month
  of per-day index rows in the current month partition. Fine-grained deletion needs
  tombstones or a finer indexDB cadence.
- **Duplicate output across parts.** Reads must tolerate duplicates; every consumer
  must go through a set. This is easy to get wrong in new code.
- **Extra WAL and ordering.** Index-first fsync adds latency unless group commit works
  well; getting the ordering wrong silently creates orphaned data.
- **Porting and licensing.** The code is a port, not a clean-room design. Attribution,
  NOTICE maintenance and upstream divergence tracking are ongoing obligations.
- **ID gaps after unclean shutdown.** The 64K watermark rounds up, so a restart can skip
  IDs. This is intentional, but it means IDs are not perfectly dense.
- **Labels are only as durable as the index.** Losing an indexDB snapshot loses the
  ability to reconstruct labels for that month's data.

# Alternatives

1. **Keep the Parquet wide-table index.**
   Lowest effort and no new engine, but the cost model is wrong for 10M+ series:
   matcher cost is proportional to index size, regex requires full scans, and every
   label pair is a separate row. Rejected.

2. **IoTDB-style tag index with bitmaps.**
   A sorted tag index plus Roaring bitmaps gives fast Eq filtering and compact
   intersections. But updates require rewriting bitmap pages or maintaining delta
   bitmaps, regex still scans values, and there is no natural LSM merge or time-partition
   drop. It also couples the index to the data file layout earlier than Phase 1 allows.
   Rejected for Phase 1; the bitmap intersection layer can still be used inside
   `uint64set` if profiling shows it wins.

3. **FST/term dictionary plus Roaring postings.**
   Excellent read performance and low memory for a static index. Incremental
   registration and high ingest rates require periodic rebuilds or delta segments,
   which is exactly the problem mergeset solves. Rejected.

4. **Use RocksDB/sled as the LSM.**
   Mature LSM with ordered iteration. However, the index needs custom block decoding,
   prefix-heavy scans, a merge callback that rewrites posting rows, explicit part
   snapshot/checkpoint semantics, and a small dependency surface. Embedding a generic
   KV store would add a large dependency and still require an index layer. Rejected for
   Phase 1; may be reconsidered for the object-store phase.

5. **Port the entire VictoriaMetrics storage engine, including data files.**
   Maximizes coherence with the index and gives proven data encodings. It abandons the
   Arrow/DataFusion ecosystem, invalidates the current Parquet path, and expands Phase 1
   into a full engine rewrite. Rejected; Phase 2 borrows the data-file ideas instead.

6. **Global (cross-partition) TSIDs.**
   Simplifies ID semantics and allows one global indexDB. But retention then requires
   scanning/rewriting global posting lists or keeping tombstones forever, and the index
   WAL grows without partition boundaries. Per-month TSID namespaces match partition
   drops and upstream behavior. Rejected.

7. **Always writing ns0 (chosen deviation from upstream).**
   The alternative is to rely only on per-day lookup (ns7) and an in-flight cache.
   That saves one row per series but risks duplicate TSIDs and makes ingestion lookup
   day-dependent. We choose the extra row for correctness and O(1) lookup; M3 measures
   its cost.

# Open Questions

- Final indexDB cadence: month (proposed) vs. day. Month reduces merge pressure and
  matches upstream; day makes retention/GC simpler. Decide after the M5 benchmark with
  the target 30-day retention.
- Whether prefix 0 stays always-on at 10M series, or is replaced by ns7 plus a bounded
  in-flight registration cache.
- Exact index checkpoint upload format and cadence for Phase 4 (hardlinks vs. packed
  archive; incremental vs. full).
- Whether the Phase 2 data file should store the canonical series key alongside the
  TSID, enabling index rebuild from data and removing the "labels only as durable as
  the index" drawback.
- Device wide-table model: whether aligned chunks for all measurements of one device
  should influence TSID field ordering. This is a separate Phase 0 decision; the 24-byte
  layout above is compatible with either choice because grouping fields are hints.
- Phase 2 data file format: decided in `docs/rfcs/20261028-data-file-adr.md` (TSF v1);
  whether the data file stores the canonical series key remains open there.

# References

- `/media/wanli/WD_SSD1/home/wanli/workspace/esmetrics/docs/mergeset-notes.md`
- `/media/wanli/WD_SSD1/home/wanli/workspace/esmetrics/crates/esm-mergeset/src/`
- `/media/wanli/WD_SSD1/home/wanli/workspace/esmetrics/crates/esm-storage/src/tsid.rs`
- `/media/wanli/WD_SSD1/home/wanli/workspace/esmetrics/crates/esm-storage/src/index/`
- `docs/roadmap.md`, sections 3-7
- VictoriaMetrics `lib/mergeset` and `lib/storage/index_db.go`, v1.146.0, Apache-2.0
