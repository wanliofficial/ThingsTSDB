# Phase 0 Benchmark: TSF v1 vs Parquet, and the Mergeset Index

- Status: Results for review
- Date: 2026-10-28
- Scope: Phase 0 validation item in `docs/roadmap.md`
- Related: ADR-0001 (`20261028-mergeset-index-adr.md`),
  ADR-0002 (`20261028-data-file-adr.md`)
- Code: `src/storage_kernel` (prototype), `src/benchmarks/src/bin/phase0_bench.rs`

# Purpose

Validate two format decisions before Phase 1:

1. Is the TSF v1 single-value page format a credible replacement for Parquet on
   write throughput, file size, full scans and selective queries?
2. Does the ported minimal mergeset table provide usable ingest, search and merge
   throughput for the LSM index prototype?

The numbers below come from the prototypes in `src/storage_kernel`, not from the
production storage engine. They bound the format, not the final implementation.

# Environment

| Item | Value |
|---|---|
| CPU | AMD Ryzen 7 4800U, 8 cores / 16 threads |
| OS | Linux 6.18.27 (rolling) |
| Toolchain | nightly-2026-10-27 |
| Profile | release: `codegen-units = 1`, `overflow-checks = true`, `debug = true` |
| Storage | local SSD, page cache warm after the first iteration |
| Parquet | `parquet` 53.2.0, `arrow` 53, ZSTD level 1 |
| TSF | prototype writer/reader, ZSTD level 1 pages, level 3 indexes |

`overflow-checks = true` costs a few percent on tight arithmetic loops; both sides
pay it, so the comparison is unaffected.

# Workloads

All datasets use a 15-second scrape interval, one metric, `__seq__ = sample index`
and `__reserved__ = 0`.

| Dataset | Series | Samples | Rows | Value shape |
|---|---|---|---|---|
| Wide gauge | 20,000 | 200 | 4,000,000 | `0.001*series + sin(0.01*i)`: noisy f64 gauge |
| Dense gauge | 2,000 | 2,000 | 4,000,000 | same, but each series has one full 8,192-row page |
| Wide counter | 20,000 | 200 | 4,000,000 | `1000 + series + 10*i`: integer-valued counter |

The gauge shape is deliberately adversarial: successive `f64` bit patterns share
almost no low bits, so neither nearest-delta nor ZSTD can compress the mantissa.
The counter shape is the common IoT case where consecutive values differ by a
small constant.

**TSF.** `encode_file` sorts by `(metric, series, timestamp, sequence)`, dedups on
max `__seq__`, writes 8,192-row pages, CRC32C for every header/page/index and a
bloom filter over series IDs. Reader validates CRCs and prunes at chunk-group,
series and page level.

**Parquet.** Sorted by `(metric, series, timestamp)`; schema
`(metric_id u64, series_id u64, timestamp i64, value f64, sequence u64)`; ZSTD
level 1; 1,000,000-row row groups; column statistics enabled. The sort is timed on
both sides (TSF sorts inside `encode_file`).

**Queries.** 100 series chosen pseudo-randomly, 10% of the time range. TSF uses
`read_series` (group → sparse series index → series entry → page index → page).
Parquet reads every batch and filters on `series_id` and `timestamp`, because its
row-group statistics cannot prune a 100-of-20,000 series set and it has no
per-series page index.

# Results

Best of three iterations for TSF encode and for all reads; the Parquet write is
measured once.

## Wide gauge: 20,000 series x 200 samples

| Metric | TSF v1 | Parquet |
|---|---:|---:|
| Write time | 1.577 s | 1.109 s |
| Write throughput | 2.54 M rows/s | 3.61 M rows/s |
| File size | 32,557,799 B (8.14 B/row) | 28,277,678 B (7.07 B/row) |
| Full scan | 4.45 M rows/s (224.8 ns/row) | 19.76 M rows/s (50.6 ns/row) |
| Range query (2,000 rows) | **7.33 ms** | 291.0 ms |

## Dense gauge: 2,000 series x 2,000 samples

| Metric | TSF v1 | Parquet |
|---|---:|---:|
| Write time | 1.250 s | 1.129 s |
| Write throughput | 3.20 M rows/s | 3.54 M rows/s |
| File size | 28,826,133 B (7.21 B/row) | 32,760,968 B (8.19 B/row) |
| Full scan | 4.86 M rows/s (205.9 ns/row) | 19.39 M rows/s (51.6 ns/row) |
| Range query (20,000 rows) | **9.05 ms** | 290.9 ms |

## Wide counter: 20,000 series x 200 samples

| Metric | TSF v1 | Parquet |
|---|---:|---:|
| Write time | 1.009 s | 0.994 s |
| Write throughput | 3.97 M rows/s | 4.02 M rows/s |
| File size | 6,272,409 B (**1.57 B/row**) | 3,966,574 B (0.99 B/row) |
| Full scan | 5.07 M rows/s (197.4 ns/row) | 35.38 M rows/s (28.3 ns/row) |
| Range query (2,000 rows) | **7.04 ms** | 204.0 ms |

## Mergeset index

24-byte sorted keys (`metric_id`, `series_id`, `timestamp`, big-endian), three
flushes of 500,000 items each, 64 KiB blocks, ZSTD level 1, 100 per-series range
searches, then a full merge with an item-count assertion after the merge.

| Step | Result |
|---|---|
| Ingest + sort + flush | 1.26 M items/s (3 x 500,000 in 1.19 s) |
| Part size | 5,822,120 B for 1.5 M keys (3.9 B/key) |
| Per-series range search | 63.2 ms for 100 queries (1,582 queries/s) over 3 parts |
| Full merge (dedup) | 2.24 M items/s, 0.671 s |
| Merged part | 2,876,171 B, all 1,500,000 items verified present |

# Interpretation

**File size.** TSF is 12% smaller on the dense dataset, 15% larger on the wide
gauge dataset, and 58% larger on the counter dataset. On the counter workload TSF
uses 1.57 B/row, inside the ADR-0002 target of at most 2.5 B/sample; Parquet is
smaller (0.99 B/row) because ZSTD finds the repeated per-series delta pattern
across the whole column. On noisy gauges both formats
approach the entropy of the `f64` mantissa (7-8 B/row); neither the nearest-delta
codec nor ZSTD can compress that away. Real sensor data is usually quantized, so
Phase 2 must re-run this on captured workloads before setting a hard size SLO.

**Write throughput.** TSF reaches 70% (wide gauge), 90% (dense gauge) and 99%
(wide counter) of Parquet. The prototype tries multiple codecs per page, allocates
a `Vec` per page payload and computes CRCs in software; the 90% ADR target is met
on two of three shapes and missed on the noisiest one. Buffer reuse and a cheaper
codec-selection pass are Phase 2 work.

**Full scan.** Parquet is 4.4-7x faster. Arrow's vectorized decoding is the bar;
the TSF prototype decodes one page at a time and allocates four `Vec<i64>` per
page. This is the largest gap and the main Phase 2 optimization target: decode
into reusable buffers, fuse LWW and Arrow emission, and add a page cache.

**Selective queries.** TSF is 28-41x faster than Parquet scan-and-filter, and it
returns the requested 2,000 rows instead of scanning 4 M. The page index is the
decisive advantage: Parquet's row-group statistics cannot isolate 100 of 20,000
series, and it has no per-series time index. This is the workload the ADR is
designed for (Grafana-style dashboard queries), and the prototype wins it even
with a slower decoder.

**Mergeset.** 1.26 M items/s ingest including in-memory sort and ZSTD, 1.6 k
range queries/s over 1.5 M items in 3 parts, and 2.2 M items/s merge with
verification. This is sufficient for a single-node Phase 1 prototype. Background
merge workers, part selection, caches and object-store checkpoints remain to be
built; the merge path already deduplicates and removes input parts.

# Caveats

- Single machine, page cache warm; no object-store latency or ranged reads.
- One metric per file. Multi-metric files add more chunk groups but also more
  file-index entries; the per-metric index path is the same.
- All series share timestamps and are written in order; out-of-order and duplicate
  rates affect write cost. TSF dedups on the max `__seq__`; Parquet has no such
  step.
- The bloom filter uses SplitMix64 instead of xxhash64 (documented prototype
  deviation) and is not large enough to be exercised as a negative lookup in these
  datasets.
- Parquet has an ecosystem that TSF deliberately gives up; the ADR drawdown
  already records this.
- Reader memory: TSF loads the whole prototype file into memory (`from_bytes`);
  production must use ranged reads.

# Conclusions

1. The TSF v1 layout is viable: size parity with Parquet, comparable write
   throughput, and a large win on the selective range queries the product targets.
2. The prototype decoder is 4-7x slower than Arrow on full scans. Phase 2 is not
   ready to switch the default path until decode is vectorized and benchmarked
   again; dual-read (ADR-0002, T4) remains required.
3. The ported mergeset table is fast enough for Phase 1. Next steps are background
   compaction, part selection policy and the TSID/IndexDB integration from ADR-0001.
4. Re-run this benchmark (T5) on captured production shapes, with page-size and
   codec sweeps, before freezing size and latency SLOs.

# Reproduction

```bash
# TSF vs Parquet: wide gauge, dense gauge, wide counter
cargo run --release -p benchmarks --bin phase0_bench -- 20000 200 gauge
cargo run --release -p benchmarks --bin phase0_bench -- 2000 2000 gauge
cargo run --release -p benchmarks --bin phase0_bench -- 20000 200 counter

# Prototype correctness tests (46 tests: codecs, pages, file, mergeset)
cargo test -p storage_kernel
```
