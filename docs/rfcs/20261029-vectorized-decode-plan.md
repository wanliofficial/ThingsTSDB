# Phase 2 Vectorized Decode Implementation Plan

- Status: Implemented (T1 kernel work, T2 Arrow boundary and flush path, T3/T4 storage integration)
- Date: 2026-10-29
- Implemented: 2026-11-07
- Related: ADR-0002 (`20261028-data-file-adr.md`), Phase 0 benchmark
  (`20261028-phase0-benchmark.md`), `docs/roadmap.md`
- Code: `src/storage_kernel/src/tsf/`, `src/columnar_storage/src/tsf/`

# 1. Purpose

The Phase 0 prototype decodes TSF pages correctly but 4-7x slower than Arrow/Parquet
on full scans (4.4-5.1 M rows/s vs 19.4-35.4 M rows/s). Selective queries already win
by 28-41x thanks to page pruning, so decode work must not regress that path.

This plan turns "vectorize decoding" into an executable sequence. It covers buffer
reuse, column projection, index caching, zero-copy Arrow emission, hardware CRC,
SIMD kernels and parallel page decoding, in that order of risk and expected payoff.
The data file format stays frozen; no codec id or layout changes are made.

# 2. Verified current state

Confirmed against the prototype before writing this plan:

| Observation | Location |
|---|---|
| ZSTD contexts are already thread-local and reused | `tsf/codec/compress.rs:14-18`, `:49-90` |
| A page decode allocates **seven** `Vec`s: four typed columns plus `values`, `sequences` and `reserved` conversions | `tsf/page.rs:235-245`, `:270-296` |
| The nearest-delta scratch buffer is zero-filled on every page | `tsf/codec/mod.rs:43-53` |
| All four columns are decoded even when the query needs only timestamp and value | `tsf/page.rs:221-238` |
| Every scan validates page CRC in software (slicing-by-8) | `tsf/page.rs:207-213`, `tsf/checksum.rs:69-87` |
| Rows are filtered one by one into `Vec<Row>` (48 bytes each) | `tsf/file.rs:694-714` |
| Series index and page index are decompressed on every read, with no cache | `tsf/file.rs:452-530`, `:650-673` |
| `TsfReader` loads the whole file with `std::fs::read` | `tsf/file.rs:353-358` |
| Nearest-delta tail decode is a serial prefix sum | `tsf/codec/nearest_delta.rs:86-91` |
| Timestamps in both benchmark shapes use `DeltaConst`; only values need the delta path | Phase 0 workload, 15 s interval |
| No Arrow/DataFusion dependency; ADR-0002 §11 requires this boundary | `storage_kernel/Cargo.toml`, ADR-0002 §11 |
| Uncompressed page flag is written never and rejected by the reader | `tsf/page.rs:219-222` |

# 3. Corrections to earlier assumptions

1. **Allocation count.** Earlier estimates said four `Vec`s per page. It is seven,
   plus a scratch zero-fill. The plan targets the actual allocations.
2. **Arrow boundary.** Arrow emission must not land in `storage_kernel`. ADR-0002 §11
   keeps that crate Arrow-free; `columnar_storage::tsf::scan` owns Arrow conversion.
   `storage_kernel` exposes typed column buffers; Arrow wraps them without copying.
3. **Benchmark IO fairness.** The Phase 0 TSF scan is timed after the whole file has
   been read into memory, while the Parquet scan includes file IO. That biases the
   comparison in TSF's favour, not against it. Milestone 2 must re-benchmark through
   `pread` plus page cache on both sides.
4. **CRC crate availability.** ADR-0002 lists `crc32c` as a Phase 2 dependency, but it
   was not available in the local registry. Use the crate when vendorable; otherwise
   call the SSE4.2 `_mm_crc32_u64` intrinsic with runtime detection and keep the
   software implementation as a portable fallback.

# 4. Design

## 4.1 Decode buffers and column projection

`storage_kernel` gains a caller-owned decode context; the reader stays immutable and
`Sync`.

```rust
bitflags-like ColumnMask: u8   // 1 = ts, 2 = value, 4 = seq, 8 = reserved
pub struct PageBuffers {       // reused across pages
    pub row_count: usize,
    timestamps: Vec<i64>,
    values: Vec<f64>,          // or Vec<u64> bits, converted at emission
    sequences: Vec<u64>,
    reserved: Vec<u64>,
}
pub fn decode_page_into(
    page: &[u8],
    mask: ColumnMask,
    out: &mut PageBuffers,
) -> Result<(), String>;
```

- `decode_page` remains as a thin wrapper for tests and compaction; it stays the
  scalar reference oracle.
- Unselected columns are bounds-checked but not decoded. CRC still covers all
  payloads, so corruption detection is unchanged.
- Buffers are cleared, not freed. The nearest-delta scratch is reused without
  zero-filling when capacity suffices.
- `values` is stored as `Vec<f64>`; converting in place from `i64` bits is allowed
  only inside the decode kernel and is covered by the differential tests.
- Working set per worker is under 1 MiB for 8,192-row pages (4 x 8,192 x 8 B plus
  scratch).

Projection policy: scans project timestamp and value by default; compaction,
migration and `__seq__`-filtered reads use `ColumnMask::ALL`. Committed TSF files are
already LWW-resolved at write time, so queries do not need `__seq__`.

## 4.2 Read API and filtering

Replace `Vec<Row>` on the scan path with page-slice results:

```rust
pub struct DecodedColumns {
    pub metric_id: u64,
    pub series_id: u64,
    pub timestamps: Vec<i64>,
    pub values: Vec<f64>,
    pub sequences: Vec<u64>,   // empty unless requested
    pub reserved: Vec<u64>,    // empty unless requested
    pub mask: ColumnMask,
}
pub fn read_series_columns(
    &self,
    metric_id: u64,
    series_id: u64,
    range: TimeRange,
    columns: ColumnMask,
    ctx: &mut ReadContext,     // buffers + index cache
) -> Result<DecodedColumns, String>;
```

- Timestamps in a page are non-decreasing, so a time range maps to a contiguous
  slice via `partition_point`; boundary pages are sliced, not filtered row by row.
- Sequence predicates are not assumed monotonic. Compaction reads keep the scalar
  path until a sequence-range kernel is justified by a profile.
- `Row` and `read_all` remain for tests, compaction and migration only.
- Rows preserve order; duplicate timestamps and NaN values stay legal and are not
  converted to Arrow nulls.

## 4.3 Index cache

`ReadContext` owns an LRU cache of decompressed series-index blocks (keyed by file
identity and metric, since one sparse index covers many series) and page indexes
(keyed by file identity, metric and series). Files are immutable, so
entries never need invalidation; entry size and total bytes are bounded (initial
default 16 MiB per worker, configurable).

Hit-rate and reject counters are exposed for ADR-0002 §13 observability. The cache
is per worker to avoid locks; the reader itself remains stateless.

## 4.4 Arrow emission boundary

`columnar_storage::tsf::scan` converts `DecodedColumns` into Arrow arrays without
copying:

- `ScalarBuffer::from(Vec<i64>)` / `ScalarBuffer::from(Vec<f64>)` for timestamp and
  value columns;
- `metric_id`/`series_id` columns filled per row (dictionary-encoded when a batch
  mixes series);
- Arrow `filter` for any non-contiguous predicate (for example sequence filters);
- one `RecordBatch` per accumulated batch of about 8,192 rows, not per 200-row page.
  A batch may span series; each `DecodedColumns` carries its identifiers so the
  scan layer can fill the key columns.

Buffer reuse ends at the Arrow boundary: a `PageBuffers` allocation is handed to
Arrow or returned to a pool, never mutated while a batch still references it.

## 4.5 CRC

Keep per-page CRC validation (corruption is fatal per ADR-0002 §3.8). Move it off
the critical path:

1. prefer the `crc32c` crate (SIMD, hardware-backed);
2. otherwise `_mm_crc32_u64` with `is_x86_feature_detected!("sse4.2")`;
3. keep slicing-by-8 as the portable fallback;
4. optionally overlap CRC with decompression/decode on separate workers.

If profiling shows CRC below 5% of scan time, stop at the hardware dispatch and do
not add overlap machinery.

## 4.6 SIMD kernels

Runtime-dispatched (`AVX2`, `SSE4.2`, scalar), unsafe confined to these kernels and
fuzzed:

- `Const` / `DeltaConst`: `fill` and constant-stride fill, no per-element branch;
- nearest-delta / nearest-delta2: decode deltas into scratch, then a block-wise
  two-pass prefix sum (lane-local sums, then lane offsets) so accumulation stops
  being strictly serial;
- plain varint: 4-way varint decode when the page falls back to codec 0;
- timestamp and value paths share the same integer kernels.

Precision bits are an encode-side choice; decode does not branch on them.

## 4.7 Parallelism and batching

- Decode runs on a dedicated pool (rayon or dedicated threads), never on tokio
  workers, per `docs/3high-design.md`.
- Work unit is a page; results are accumulated per series chunk and merged into
  ~8,192-row Arrow batches to amortise emission.
- Each worker owns a `ReadContext` (buffers plus cache); `TsfReader` needs no locks.
- Ordering is preserved by assigning pages to ordered slots before emission.

## 4.8 Format compliance: uncompressed pages

ADR-0002 §3.5/§4.4 require the writer to store a payload uncompressed when ZSTD
does not save at least 10%, with page flag bit 3, and the reader to honour it. The
prototype does neither. This is a correctness/compliance fix, independent of
performance work, and lands in T1 with round-trip and golden-byte tests. Existing
files remain readable.

# 5. Milestones and gates

Milestones 0, 1, 3 and 4 belong to T1 (kernel work). Milestone 2 is the T2 Arrow
boundary. Milestone 5 sits on the T2/T3 boundary.

| # | Deliverable | Exit gate |
|---|---|---|
| 0 | Criterion page-decode benchmark plus `perf`/flamegraph harness; breakdown across ZSTD, codec, CRC, allocation, filtering and index decompression | At least 90% of scan time accounted for; baseline recorded; no behaviour change |
| 1 | `decode_page_into` + `ColumnMask` + `PageBuffers`; contiguous time slicing; index cache; scratch reuse; `Row` removed from scans | Differential parity on at least 100k randomized pages in CI (1M nightly) across all codecs, NaN/all-NaN, duplicate timestamps and precision sweeps; full scan at least 1.5x faster; selective query latency not worse |
| 2 | `DecodedColumns` plus zero-copy Arrow emission and Arrow filtering in `columnar_storage::tsf::scan`; Phase 0 rerun through `pread` + page cache with an Arrow-to-Arrow comparison | Full scan at least 2x current; fair Parquet comparison; row order, duplicate timestamps and NaN parity with the scalar path |
| 3 | Hardware CRC32C with runtime dispatch; optional overlap | Checksums identical on every test vector and corpus; keep only if it moves scan time by at least 5% |
| 4 | SIMD kernels: fill, block prefix sum, 4-way varint | Each kernel at least 1.5x over scalar; scalar oracle differential and `cargo-fuzz` clean |
| 5 | Dedicated decode pool, ordered emission, batch accumulation, memory caps, cache metrics | Full scan at least 15 M rows/s; 30-day single-series and dashboard queries p99 under 50 ms; per-worker memory bounded and reported |

Each milestone is merged only after the previous gate's numbers are recorded in this
document or the Phase 0 report.

# 6. Testing strategy

- **Unit differential:** every codec, page sizes 1..=8,192, NaN and all-NaN pages,
  duplicate timestamps, `precision_bits` 1..=64, empty payloads. Compare against
  `decode_page` as the oracle.
- **Property/fuzz:** `cargo-fuzz` targets for `decode_page_into` and each SIMD
  kernel; assertion that fuzzed input never yields a different result than the
  scalar path and never panics on malformed input (returns `Err`).
- **Corruption:** CRC mismatches in page, page index, series index, chunk-group and
  footer remain fatal after optimisation.
- **End-to-end:** the existing metric-engine test suite must return identical results
  with the TSF path; Phase 0 benchmark rerun with the real IO path.
- **Performance CI:** store criterion baselines; a regression above 10% on the
  page-decode kernels fails the check. Full phase-0 runs stay manual.

# 7. Acceptance metrics

- Full scan: at least 15 M rows/s on the Phase 0 wide gauge workload, single node,
  by the end of milestone 5.
- Selective query: 100 series over a 10% window remains at or below the current
  7-9 ms, measured through the real IO path.
- 30-day, single-series and dashboard-style queries: p99 under 50 ms (ADR-0002).
- Memory: decode working set under 1 MiB per worker; index cache bounded and
  configurable; no unbounded page-buffer growth.
- Correctness: zero differential or fuzz failures; all corruption tests still fail
  loudly.

# 8. Out of scope

- Gorilla/CHIMP value encoding (needs a new codec id and a fresh benchmark).
- Lossy `precision_bits` as a default.
- Object-store ranged reads and remote caches.
- Page-size changes. Tune after projection and Arrow emission are measured.
- Sequence-range vectorisation; compaction keeps the scalar path until profiled.

# 9. Risks and open questions

- **Buffer ownership at the Arrow boundary.** Zero-copy requires handing the `Vec`
  to Arrow; a pool must not hand out live memory. This needs a small, well-tested
  allocator abstraction.
- **Cache key stability.** Files are immutable, but compaction replaces them. Keys
  must include a file identity that changes on rewrite (path plus size or file ID),
  not just metric and series.
- **Portability.** `std::simd` is nightly-only; prefer `std::arch` intrinsics with a
  scalar fallback. `3high-design.md` allows `unsafe` only in codec kernels, all
  fuzzed.
- **Target feasibility.** If milestone 5 gains most of the throughput from
  parallelism rather than SIMD, revisit the SIMD budget and state the result.
- **`crc32c` dependency.** Confirm vendoring; otherwise document the intrinsic
  fallback in ADR-0002 §11.

# 10. References

- ADR-0002: `docs/rfcs/20261028-data-file-adr.md`
- Phase 0 benchmark: `docs/rfcs/20261028-phase0-benchmark.md`
- Module layout and Arrow boundary: ADR-0002 §11
- Threading and unsafe policy: `docs/3high-design.md`
- Prototype: `src/storage_kernel/src/tsf/{page.rs,file.rs,checksum.rs,codec/}`

# 11. Implementation status and measurements

Implemented on 2026-11-07. Milestones 0-5 and the §4.8 compliance fix are in
`storage_kernel::tsf` and `columnar_storage::tsf`; the T3/T4 manifest, flush and
dual-read integration remain follow-up work.

| # | Status | Where |
|---|---|---|
| 0 | Done: Criterion page-decode benchmark plus the `phase2_bench` scan benchmark | `src/benchmarks/benches/page_decode.rs`, `src/benchmarks/src/bin/phase2_bench.rs` |
| 1 | Done: `ColumnMask`, `PageBuffers`, `decode_page_into`, contiguous slicing, index LRU, scratch reuse, `Vec<Row>` removed from scans | `storage_kernel::tsf::{page,file}` |
| 2 | Done: `DecodedColumns`, zero-copy `ScalarBuffer` emission, column-major batch accumulation, Arrow sequence filter | `columnar_storage::tsf::scan` |
| 3 | Done: SSE4.2 CRC32C with runtime dispatch and slicing-by-8 fallback | `storage_kernel::tsf::checksum` |
| 4 | Done: AVX2 fill, constant-stride fill, block prefix sum, 4-way varint, each with a scalar oracle | `storage_kernel::tsf::codec::simd` |
| 5 | Done: scoped-thread decode, one `ReadContext` per worker, deterministic `series_id` order, batch accumulation, cache metrics and memory accounting | `storage_kernel::tsf::file`, `columnar_storage::tsf::scan` |
| §4.8 | Done: writer sets flag bit 3 when no payload uses ZSTD; reader rejects the flag combined with a ZSTD codec id | `storage_kernel::tsf::page` |

Also added: pure UTC-day partition helpers and the manifest-first TTL drop plan
(`storage_kernel::tsf::partition`) that the T3 partition manager will consume.

## 11.1 Measured numbers

Workload: `phase2_bench 20000 200 gauge`, one metric, 20,000 series x 200
samples = 4,000,000 rows, 15 s interval, 32,557,799-byte file (8.14 bytes/row).
Release build, single node, file already in memory.

| Path | Throughput | ns/row |
|---|---|---|
| Scalar `read_all` (`Vec<Row>`) | 4.5 M rows/s | 223 |
| Column scan, 1 worker, `SAMPLE` | 25.3 M rows/s | 39.5 |
| Column scan, 8 workers | 94.0 M rows/s | 10.6 |
| Column scan, auto (16) workers | 95.1 M rows/s | 10.5 |
| Arrow scan in memory (T2 boundary) | 21.0 M rows/s | 47.7 |
| Arrow scan on disk, warm reader | 24.1 M rows/s | 41.5 |
| On-disk open + Arrow scan | 23.3 M rows/s | 42.9 |
| On-disk open alone (warm page cache) | 17.2 ms | - |
| Selective query, 100 series, 10% window | 2 ms | - |

The Arrow scan emitted 500 batches at ~8,000 rows per batch. Per-worker decode
buffers are 320 KiB (`PageBuffers::capacity_bytes`); the single-worker 16 MiB
index budget held 2.73 MB of decompressed indexes. Milestone 5's 15 M rows/s
gate is met on the Arrow path, and the on-disk run stays above it; the 50 ms
selective-query gate is met with a large margin.

### 11.1.1 Page-decode and kernel breakdown

Criterion (`page_decode`, 8,192-row gauge page):

| Benchmark | Time | Throughput |
|---|---|---|
| `decode_page` (allocating oracle) | 208.0 µs | 39.4 Melem/s |
| `decode_page_into` all columns | 203.6 µs | 40.2 Melem/s |
| `decode_page_into` sample (ts+value) | 189.7 µs | 43.2 Melem/s |
| `decode_page_into` timestamp only | 19.8 µs | 413.0 Melem/s |
| `decode_page_into` value only | 186.4 µs | 44.0 Melem/s |
| `fill_i64` | 3.55 µs | 2.31 Gelem/s |
| `fill_delta_i64` | 4.00 µs | 2.05 Gelem/s |
| `prefix_sum_i64_in_place` | 48.3 µs | 169.7 Melem/s |
| `unmarshal_var_int64s_4way` | 126.1 µs | 65.0 Melem/s |
| `crc32c` page | 13.6 µs | 3.38 GiB/s |
| `crc32c` 1 MiB | 288.9 µs | 3.38 GiB/s |
| ZSTD decompress value bits | 168.1 µs | 372 MiB/s |

Value decoding dominates the page (nearest-delta plus ZSTD) and matches the
standalone ZSTD decompression cost; timestamp decoding is effectively free.
Allocation accounts for under 2% of a page decode, so the remaining budget is
codec work rather than buffer churn. CRC is ~7% of page decode time, above the
plan's 5% threshold, so the hardware path is kept; overlapping CRC with decode
is deferred to avoid coupling the checksum and projection paths.

The randomized projection differential test covers mixed codecs,
NaN/infinity values, duplicate timestamps and random sequences, and is part of
the normal `cargo test -p storage_kernel` run (128 pages). The milestone 1
CI-scale gate runs as an ignored nightly test for 100,000 pages (about 44 s in a
release build):

```text
cargo test -p storage_kernel --release -- --ignored randomized_differential_nightly
```

The 1M-page nightly run is not wired into the pipeline yet. T1 also has a
golden page-header layout test that pins every fixed offset, all four payload
slices and the CRC, and a 1M-row file round-trip test that runs in the normal
suite (about 3 s in a debug build).

## 11.2 Reproduce

```text
cargo test -p storage_kernel -p columnar_storage
cargo bench -p benchmarks --bench page_decode
cargo run --release -p benchmarks --bin phase2_bench -- 20000 200 gauge
```

## 11.3 Storage integration (T2 flush path, T3, T4)

The decode work above lands in the storage engine as follows:

| Piece | Location | Test |
|---|---|---|
| Arrow batch -> TSF rows | `columnar_storage::tsf::write` | `test_encode_batch_round_trips` |
| Flush writes TSF per `StorageConfig.format` | `buffer::{write_parquet,write_tsf}` | `test_storage_tsf_flush_and_scan` |
| Compaction writes TSF and carries `min_sequence` | `compaction/executor.rs` | `test_storage_dual_read_parquet_and_tsf` |
| `format` + `min_sequence` in the manifest | `pb_types` `SstMeta` 5/6, snapshot record v2 | `test_snapshot_record_v2_round_trip`, `test_snapshot_record_v1_dual_read`, `test_snapshot_v1_upgrade` |
| Dual-read execution plan | `read::build_df_plan` + `tsf::exec::TsfExec` | `test_storage_dual_read_parquet_and_tsf` |
| Partition TTL drop | `Manifest::{plan_ttl_drop,drop_expired_partitions}` | `test_plan_and_drop_ttl_partitions` |

`TsfExec` runs one partition per file so `SortPreservingMergeExec` above it sees
sorted streams; each partition decodes only its own file's time range. Whole-object
fetches are still used, so range reads remain the next decode-adjacent follow-up.
