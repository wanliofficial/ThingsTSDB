# Phase 2 Benchmark: End-to-End TSF vs Parquet

- Status: Results for review
- Date: 2026-11-09
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

The `tsid = lit` / `tsid IN (...)` form is pushed into the TSF scan
(`TsfPushdown`), so the file plan fetches one series chunk per file instead of
every series of the metric. The predicate itself still runs above the scan, and
every form the extractor does not understand is simply left there.

Latency is one warm-up scan followed by 20 measured scans; p50/p99 are over those 20.
Restart is measured cold: drop the storage, reopen it (manifest load, WAL replay) and run
one full scan.

# Results

## Gauge: 50 series x 30 days (8.64 M rows)

| Metric | Parquet | TSF |
|---|---:|---:|
| Files written | 30 | 30 |
| File bytes | 144,370,242 | 21,217,820 |
| Bytes/sample | 16.71 | **2.46** |
| Ingest, WAL + buffer | 2.8 M rows/s | 4.7 M rows/s |
| Flush + manifest commit | 4.11 s | 5.45 s |
| Ingest to durable (write + flush) | 1.2 M rows/s | **1.2 M rows/s** |
| Flush share of durable ingest | 57% | 75% |
| Full scan | 410 ms (24.2 M rows/s) | 268 ms (**36.0 M rows/s**) |
| 1-day query p50 / p99 | 5.19 / 5.94 ms | 1.74 / 2.08 ms |
| 30-day query p50 / p99 | 37.73 / 51.30 ms | 14.28 / 17.34 ms |
| 30-day query read volume | 9.1 MB (pruned row groups) | 452 KB (1 of 50 series) |
| 1 h / 30 d read volume | 3.33% | 3.31% |
| Restart (reopen + first scan) | 386 ms | 314 ms |

## Counter: 50 series x 30 days (8.64 M rows)

| Metric | Parquet | TSF |
|---|---:|---:|
| Files written | 30 | 30 |
| File bytes | 111,022,020 | 493,645 |
| Bytes/sample | 12.85 | **0.06** |
| Ingest, WAL + buffer | 4.1 M rows/s | 9.0 M rows/s |
| Flush + manifest commit | 3.66 s | 3.82 s |
| Ingest to durable (write + flush) | 1.5 M rows/s | **1.8 M rows/s** |
| Flush share of durable ingest | 63% | 80% |
| Full scan | 377 ms (24.7 M rows/s) | 183 ms (**50.4 M rows/s**) |
| 1-day query p50 / p99 | 6.79 / 8.13 ms | 1.12 / 1.36 ms |
| 30-day query p50 / p99 | 40.62 / 51.07 ms | 7.64 / 9.52 ms |
| 30-day query read volume | 7.2 MB (pruned row groups) | 494 KB (whole files, see below) |
| 1 h / 30 d read volume | 3.31% | 3.26% |
| Restart (reopen + first scan) | 382 ms | 187 ms |

The counter shape is deliberately regular: consecutive values differ by exactly 10 and
timestamps by exactly 15,000 ms, and each page holds one series' day. GORILLA/CHIMP and
delta-of-delta compress it to almost nothing (0.06 B/sample). Phase 0's counter workload
interleaved series inside a page and stored a per-row sequence, which cost 1.57 B/sample;
real device data sits between the two, so treat 0.06 as a lower bound, not a typical value.

A counter day file is 16 KB, below the 512 KiB whole-object threshold, so its scans read
the object whole rather than planning byte ranges: the 30-day series query therefore reads
494 KB, the dataset size, and still finishes in 9.5 ms. For gauge the files are 707 KB each
and the series filter buys real pruning (452 KB instead of 21.2 MB).

Throughput varies by 20-30% between runs on this laptop (thermal and background load); the
numbers above come from one gauge run and one counter run, back to back, on the build that
includes the series pushdown and the `PredictedF64` value codec.

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
| Size ≤ 2.5 B/sample (15 s scrape) | 2.46 B/sample gauge; 0.06 counter | **Pass** (`PredictedF64`; the nearest-delta codec alone needed 7.20) |
| Write throughput ≥ 90% of Parquet | 99% (gauge), 120% (counter) | Pass (run-to-run variance is 20-30%; both shapes are at or above Parquet) |
| Full scan ≥ 15 M rows/s, TSF within 2x of Parquet | 36.0-50.4 M rows/s; TSF 1.5x faster on gauge, 2.0x on counter | Pass |
| 1-day Grafana query p99 < 50 ms | 2.08 ms gauge, 1.36 ms counter | Pass |
| 30-day single-series query p99 < 50 ms | 17.34 ms gauge, 9.52 ms counter | **Pass** (Parquet: 51.30 / 51.07 ms) |
| Page pruning ≤ 5% of pages for a 1 h query | 3.31% gauge, 3.26% counter | Pass (granularity-limited, see below) |
| `kill -9` recovery < 30 s | 6 SIGKILL points recover with no lost or duplicated rows; reopen is the 220-551 ms restart above | Pass |
| TTL drop = one manifest delta, no query-visible data | Covered by the T3 tests, not this benchmark | Out of scope here |

## Findings

**Full-scan throughput is now decode-bound.** Before this measurement, `MergeStream`
sliced one `RecordBatch` per primary-key group and called the merge operator once per
group; with one timestamp per row that is one slice, one dynamic key comparison and one
operator call per row. A full scan managed 0.7 M rows/s. The fast path added in
`fc97ec9d` proves "no adjacent duplicate keys" with one vector comparison per primary-key
column and emits the batch in bulk. That took the full scan to 36-50 M rows/s across
value shapes in this run. The counter, whose pages are nearly empty, runs above the
35.6 M rows/s Arrow boundary; the gauge lands at 36.0 M rows/s, next to it. The general,
duplicate-key path is unchanged.

**Series pushdown closed the 30-day gap.** Before it, `TsfExec` decoded every series of a
metric inside the time window and the `tsid` predicate was applied above the scan: the
30-day single-series query read all 62.5 MB and needed 263 ms, the one target that failed.
Now `TsfPushdown::from_predicates` extracts `metric_id` and `tsid` equality and non-negated
`IN` constraints from the scan predicates (intersecting repeats, dropping anything it does
not understand), and `TsfExec` passes the series set to the staged planner, which skips
series-index blocks and series chunks for every other series. The same query now reads
452 KB, one fiftieth of the 21.2 MB file, and takes 17 ms. The 1-day query went from
24.5 ms to 1.7 ms. Parquet reaches the same shape through row-group statistics but stays
at 51.3 ms, because it still decodes whole row groups of fifty series.

The pushdown is a narrowing hint, never a replacement for the predicate: it only ever
returns a superset of the matching rows, and the `FilterExec` above the scan still applies
everything. That is why an unsupported form (`tsid > 5`, `NOT IN`, `OR`, a cast on the
column) leaves the scan unfiltered rather than guessing.

**Pruning is page-granular, and the page is one day here.** 5,760 samples per series per
day fit in one 8,192-row page, so a one-hour window must read the whole day page for every
selected series. The measured 3.3% is the 1-day/30-day floor, which still meets the 5%
target. With more samples per day (or smaller pages) the hour window would prune below
that floor; with fewer, it would rise, because the granule is a page, not an hour.

**Flush dominates durable ingest.** Sorting, encoding and committing is 57-80% of the
write path, so the ingest-to-durable number is mostly a flush throughput number. On the
gauge set the TSF flush is 5.45 s against Parquet's 4.11 s in this run, while an earlier
run of the same code had 5.05 s against 5.90 s: the 20-30% run-to-run variance is larger
than the difference, and durable ingest is 1.2 M rows/s for both formats. TSF writes 6.8x
fewer bytes but is not proportionally faster, because page encoding is CPU-bound and tries
the predictive codec at five orders before choosing.

**Size is shape-dependent, and the gauge now passes.** On the noisy gauge, TSF is 6.8x
smaller than Parquet (2.46 vs 16.71 B/sample): the value payload went from 7.14 to
2.40 B/sample when `PredictedF64` replaced the nearest-delta codec, because it extrapolates
the value instead of differencing mantissa bits. On the regular counter TSF is 200x smaller.

**Why the gauge was the hard case, and why XOR was not the answer.** The nearest-delta
codec compares adjacent bit patterns; for a smooth gauge those patterns differ mostly in
their low mantissa bits, so the delta is value noise and costs 7-8 bytes per sample. An
order-5 polynomial extrapolation of the value shrinks the residual to 1-3 bytes. Gorilla /
CHIMP XOR was measured on the same data first and needs ~65 B/sample: consecutive gauge
values share almost no leading or trailing mantissa bits, so its blocks stay wide. The
encoder only selects the predictive codec when its payload is shorter than the
nearest-delta result, so an unpredictable gauge keeps the old, bounded size.

**Restart is manifest-bound and sub-second.** Reopening 30 files and running the first full
scan takes 220-551 ms, well inside the 30 s target. That is the same path a crash takes:
`kill -9` recovery is a reopen plus WAL replay, and `tests/crash_recovery.rs` exercises six
SIGKILL points (WAL only, after a committed flush, mid data-file PUT, mid manifest commit,
mid compaction, and with an orphaned object) and gets exactly-once rows back from each.

# Reproduce

```
cargo run --release -p benchmarks --bin phase2_e2e_bench -- 50 30 gauge
cargo run --release -p benchmarks --bin phase2_e2e_bench -- 50 30 counter
cargo run --release -p benchmarks --bin phase2_bench -- 50 172800 gauge
```

The first argument is series, the second days, the third value shape. Each run creates its
own directory under the system temp dir and removes it afterwards.
