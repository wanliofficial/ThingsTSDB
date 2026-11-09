# Fuzz targets (ADR-0002 T1, ADR-0001 M1)

`cargo-fuzz` targets for the TSF file writer/reader, page codecs, CRC and the
mergeset part layout. They need a nightly toolchain
(`rust-toolchain.toml` pins one) and [`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz):

```bash
cargo install cargo-fuzz --locked   # once
cargo fuzz build
```

Run one target for a bounded time (seconds) or until a crash is found:

```bash
cargo fuzz run page_roundtrip -- -max_total_time=60
cargo fuzz run page_decode -- -max_total_time=60
cargo fuzz run file_read -- -max_total_time=120
cargo fuzz run checksum_differential -- -max_total_time=60
cargo fuzz run mergeset_part -- -max_total_time=60
```

| Target | Invariant |
|---|---|
| `page_decode` | Arbitrary bytes decode to an error or a page, never a panic, for every projection; a failed decode leaves the reusable buffers usable. |
| `page_roundtrip` | Rows derived from the input encode and decode with identical bit patterns (NaN included, duplicate timestamps allowed). |
| `file_read` | A file encoded from fuzz-derived rows opens and scans; arbitrary input is rejected or scanned defensively. |
| `checksum_differential` | The SSE4.2 and slicing-by-8 CRC32C paths agree on every input, and multi-part checksums equal the concatenation. |
| `mergeset_part` | A sorted item set derived from arbitrary bytes writes, reopens and returns exactly those items (duplicates included) through range and prefix search, never panicking on any input. |

The corpus, artifacts and coverage directories are ignored by
`fuzz/.gitignore`; a smoke run of each target is part of the T1 exit criteria
in `docs/rfcs/20261028-data-file-adr.md` §12.1.
