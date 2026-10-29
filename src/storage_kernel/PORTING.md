# Porting Notes

This crate contains code ported from third-party Apache-2.0 projects. Each
ported file keeps its upstream attribution in the module documentation and the
source path is recorded here.

## esmetrics / VictoriaMetrics encodings

- Source: `esmetrics/crates/esm-encoding/src/`, commit `4d9901f`
  (a Rust port of VictoriaMetrics v1.146.0 `lib/encoding`).
- Upstream: <https://github.com/VictoriaMetrics/VictoriaMetrics>,
  Copyright 2019-2026 VictoriaMetrics, Inc.
- License: Apache-2.0.

Ported files under `src/tsf/codec/`:

| File | Upstream file |
|---|---|
| `compress.rs` | `compress.go` + `zstd/` |
| `float.rs` | `float.go` |
| `int.rs` | `int.go` |
| `nearest_delta.rs` | `nearest_delta.go` |
| `nearest_delta2.rs` | `nearest_delta2.go` |
| `encoding.rs` | `encoding.go` |
| `testutil.rs` | test helpers |

Local modifications:

- Import paths point at `super::` instead of the `esm-encoding`/`esm-common`
  crates.
- The SIMD `fastnum` helpers are replaced by plain iterators; this is a
  performance-only fast path for all-zero/all-one blocks.
- ZSTD compression uses the `zstd` crate directly.
- The block codec API is re-exported from `crate::tsf::codec`.

## mergeset block marshaling

- Source: `esmetrics/crates/esm-mergeset/src/inmemory_block.rs`, same commit.
- Local modifications: import paths, a local `hex_encode` helper, removal of
  unrelated runtime instrumentation, and removal of helpers the minimal Table
  does not use (`copy_from`, `size_bytes`, `StorageBlock::reset`). The sort,
  marshal and single-item helpers are kept and exercised by the ported tests.
