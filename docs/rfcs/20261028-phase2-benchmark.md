# Phase 2 Benchmark: End-to-End TSF vs Parquet

- Status: Results for review
- Date: 2026-11-08
- Scope: T5 of ADR-0002 (`20261028-data-file-adr.md` §12)
- Related: ADR-0002, Phase 0 benchmark (`20261028-phase0-benchmark.md`), decode plan
  (`20261029-vectorized-decode-plan.md`)
- Code: `src/benchmarks/src/bin/phase2_e2e_bench.rs`,
  `src/benchmarks/src/bin/phase2_bench.rs`

# Purpose

Phase 0 measured the format prototype in `storage_kernel`. This benchmark measures the
finished storage stack, for both formats, through the same public API:

- writes go through `ColumnarStorage::write` (WAL append + fsync, buffer, flush, encode,
  manifest commit), not a direct encoder call;
- reads go through `ColumnarStorage::scan`, including file pruning, the format plan,
  `SortPreservingMergeExec` and the merge operator, not a direct decode call;
- size and read volume come from the T4 per-format counters
  (`ColumnarStorage::stats`), so they are measured, not recomputed from the dataset.

The T5 matrix is: size, durable write throughput, query p50/p99, page pruning and
restart time.

# Environment

| Item | Value |
|---|---|
| CPU | AMD Ryzen 7 4800U, 8 cores / 16 threads |
| OS | Linux 6.18.48 |
| Toolchain | rustc 1.101.0-nightly (2026-10-26) |
| Profile | release: `codegen-units = 1`, `overflow-checks = true`, `debug = true` |
| Storage | local SSD, page cache warm after the warm-up scan |
| Parquet | `parquet` 53.2.0, Arrow 53, plain encoding, Snappy, 8,192-row groups |
| TSF | TSF v1 pages, ZSTD pages, 8,192 rows/page, CRC32C |

`overflow-checks = true` costs a few percent on tight arithmetic loops; both formats pay it,
so the comparison is unaffected.

# Workload

One dataset per value shape, identical for both formats:

| Dataset | Series | Days | Samples/day | Rows | Value shape |
|---|---:|---:|---:|---:|---|
| Gauge | 50 | 30 | 5,760 | 8,640,000 | `0.001*series + sin(0.01*i)`: noisy f64 gauge |
| Counter | 50 | 30 | 5,760 | 8,640,000 | `1000 + series + 10*i`: integer-valued counter |

15-second scrape interval, one metric, `metric_id = tsid = timestamp` primary key, overwrite
update mode. Each series' day is written as one `write()` call (1,500 calls per dataset,
5,760 rows each), so the WAL records the same frames a real ingest loop would.

The buffer is sized to hold the dataset and the periodic flush is disabled, so the run has
one deterministic flush and commit. The write path itself is unchanged: every `write`
appends to and fsyncs the WAL, and flush sorts, encodes and commits a manifest delta.
Compaction is scheduled an hour away so it cannot interleave with the query measurements;
it has its own counters and is not part of T5.

Queries, all against a single target series (`tsid = series/2`) unless noted:

| Shape | Range | Notes |
|---|---|---|
| Full scan | whole dataset | no predicate; all 30 files per format |
| 1-day query | last UTC day | Grafana default window; `tsid` and `timestamp` predicates |
| 30-day query | whole dataset | single series across all partitions |
| Pruning | one hour vs whole dataset | `bytes_read` delta from the per-format counters |

Latency is one warm-up scan followed by 20 measured scans; p50/p99 are over those 20.
Restart is measured cold: drop the storage, reopen it (manifest load, WAL replay) and run
one full scan.

# Results

## Gauge: 50 series x 30 days (8.64 M rows)

| Metric | Parquet | TSF |
|---|---:|---:|
| Files written | 30 | 30 |
| File bytes | 144,370,171 | 62,213,028 |
| Bytes/sample | 16.71 | **7.20** |
| Ingest, WAL + buffer | 2.7 M rows/s | 5.2 M rows/s |
| Flush + manifest commit | 6.65 s | 4.67 s |
| Ingest to durable (write + flush) | 0.9 M rows/s | **1.4 M rows/s** |
| Flush share of durable ingest | 67% | 74% |
| Full scan | 376 ms (24.6 M rows/s) | 302 ms (**29.7 M rows/s**) |
| 1-day query p50 / p99 | 5.01 / 5.32 ms | 21.91 / 24.46 ms |
| 30-day query p50 / p99 | 32.94 / 38.66 ms | 246.58 / 262.58 ms |
| 1 h / 30 d read volume | 3.33% | 3.34% |
| Restart (reopen + first scan) | 399 ms | 298 ms |

## Counter: 50 series x 30 days (8.64 M rows)

| Metric | Parquet | TSF |
|---|---:|---:|
| Files written | 30 | 30 |
| File bytes | 111,021,706 | 493,645 |
| Bytes/sample | 12.85 | **0.06** |
| Ingest, WAL + buffer | 4.1 M rows/s | 9.0 M rows/s |
| Flush + manifest commit | 3.24 s | 3.69 s |
| Ingest to durable (write + flush) | 1.6 M rows/s | **1.9 M rows/s** |
| Full scan | 362 ms (25.0 M rows/s) | 165 ms (**57.3 M rows/s**) |
| 1-day query p50 / p99 | 5.25 / 5.95 ms | 8.45 / 8.65 ms |
| 30-day query p50 / p99 | 34.51 / 37.07 ms | 103.41 / 113.12 ms |
| 1 h / 30 d read volume | 3.31% | 3.26% |
| Restart (reopen + first scan) | 360 ms | 162 ms |

The counter shape is deliberately regular: consecutive values differ by exactly 10 and
timestamps by exactly 15,000 ms, and each page holds one series' day. GORILLA/CHIMP and
delta-of-delta compress it to almost nothing (0.06 B/sample). Phase 0's counter workload
interleaved series inside a page and stored a per-row sequence, which cost 1.57 B/sample;
real device data sits between the two, so treat 0.06 as a lower bound, not a typical value.

## Kernel decode reference

`phase2_bench` on the same 8.64 M-row gauge dataset, one TSF file, no storage layer:

| Stage | Throughput |
|---|---:|
| Scalar `read_all` (`Vec<Row>`) | 4.9 M rows/s |
| Column scan, one worker | 18.3 M rows/s |
| Column scan, 8 workers | 88.6 M rows/s |
| Arrow scan (T2 boundary) | 35.6 M rows/s |
| On-disk Arrow scan (warm reader) | 34.0 M rows/s |
| Selective query (100 series, 10% window) | 24.3 M rows/s |

# Acceptance check

| Target (ADR-0002 §12) | Measured | Verdict |
|---|---|---|
| Size ≤ 2.5 B/sample (15 s scrape) | 7.20 B/sample gauge; 0.06 counter | Gauge misses (incompressible f64; Phase 0 measured 8.14 too); counter passes |
| Write throughput ≥ 90% of Parquet | 155% (gauge), 119% (counter) | Pass |
| Full scan ≥ 15 M rows/s, TSF within 2x of Parquet | 24.6-57.3 M rows/s; TSF 1.2x faster on gauge, 2.3x on counter | Pass |
| 1-day Grafana query p99 < 50 ms | 24.5 ms gauge, 8.7 ms counter | Pass |
| 30-day single-series query p99 < 50 ms | 263 ms gauge, 113 ms counter | **Fail**; Parquet passes at 39 ms |
| Page pruning ≤ 5% of pages for a 1 h query | 3.34% gauge, 3.26% counter | Pass (granularity-limited, see below) |
| `kill -9` recovery < 30 s | 298-399 ms clean restart; fault injection not run here | Not verified |
| TTL drop = one manifest delta, no query-visible data | Covered by the T3 tests, not this benchmark | Out of scope here |

## Findings

**Full-scan throughput is now decode-bound.** Before this measurement, `MergeStream`
sliced one `RecordBatch` per primary-key group and called the merge operator once per
group; with one timestamp per row that is one slice, one dynamic key comparison and one
operator call per row. A full scan managed 0.7 M rows/s. The fast path added in
`fc97ec9d` proves "no adjacent duplicate keys" with one vector comparison per primary-key
column and emits the batch in bulk. That took the full scan to 24-30 M rows/s, in line with
the 35.6 M rows/s Arrow boundary and within 2x of it in both directions. The general,
duplicate-key path is unchanged.

**TSF has no series predicate pushdown, and it is the one target that fails.** `TsfExec`
decodes every series of a metric inside the time window; the `tsid` predicate is applied
above the scan. Parquet does not have per-series pages either, but row-group statistics let
it skip row groups that cannot contain the target series: the 30-day single-series query
reads 9.1 MB instead of 62.5 MB and finishes in 39 ms, while TSF reads the whole 62.5 MB
and needs 263 ms. The gap grows linearly with series-per-day, because TSF currently pays
for every series to serve one. The series index already exists and the kernel can read a
single series chunk; what is missing is predicate extraction and a series-scoped scan plan
in `TsfExec`.

**Pruning is page-granular, and the page is one day here.** 5,760 samples per series per
day fit in one 8,192-row page, so a one-hour window must read the whole day page for every
selected series. The measured 3.3% is the 1-day/30-day floor, which still meets the 5%
target. With more samples per day (or smaller pages) the hour window would prune below
that floor; with fewer, it would rise, because the granule is a page, not an hour.

**Flush dominates durable ingest.** Sorting, encoding and committing is 61-79% of the
write path, so the ingest-to-durable number is mostly a flush throughput number. TSF
encodes faster than the Parquet writer (4.67 s vs 6.65 s for the gauge set) even though it
produces 2.3x fewer bytes.

**Size is shape-dependent.** On the noisy gauge, TSF is 2.3x smaller than Parquet
(7.20 vs 16.71 B/sample). On the regular counter it is 200x smaller. Both formats are
dominated by the gauge mantissa, which no codec compresses; the counter is where the
TSF-specific delta encodings pay off.

**Restart is manifest-bound and sub-second.** Reopening 30 files and running the first full
scan takes 162-399 ms, well inside the 30 s target. This is a clean restart; the fault
injection suite (`SIGKILL` mid-flush and mid-commit) is still a T3 follow-up.

# Reproduce

```
cargo run --release -p benchmarks --bin phase2_e2e_bench -- 50 30 gauge
cargo run --release -p benchmarks --bin phase2_e2e_bench -- 50 30 counter
cargo run --release -p benchmarks --bin phase2_bench -- 50 172800 gauge
```

The first argument is series, the second days, the third value shape. Each run creates its
own directory under the system temp dir and removes it afterwards.
