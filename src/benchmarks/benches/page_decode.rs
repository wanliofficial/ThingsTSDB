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

//! Milestone 0 of `docs/rfcs/20261029-vectorized-decode-plan.md`:
//! a Criterion benchmark for TSF page decoding.
//!
//! It records the allocating `decode_page` oracle against the caller-owned
//! `decode_page_into` path for several projections, and the SIMD kernel
//! breakdown (fill, constant-stride fill, prefix sum, 4-way varint). Run with:
//!
//! ```text
//! cargo bench -p benchmarks --bench page_decode
//! ```

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use storage_kernel::tsf::{
    codec::{marshal_var_int64s, simd},
    page::{decode_page, decode_page_into, encode_page, ColumnMask, PageBuffers, MAX_PAGE_ROWS},
    Row,
};

const BASE_TS: i64 = 1_700_000_000_000;
const STEP_MS: i64 = 15_000;

/// A 15 s gauge workload, matching the Phase 0 benchmark shape.
fn gauge_rows(n: usize) -> Vec<Row> {
    (0..n)
        .map(|i| Row {
            metric_id: 1,
            series_id: 7,
            timestamp: BASE_TS + i as i64 * STEP_MS,
            value: 42.5 + (i as f64 * 0.01).sin(),
            sequence: i as u64,
            reserved: 0,
        })
        .collect()
}

fn bench_page_decode(c: &mut Criterion) {
    let rows = gauge_rows(MAX_PAGE_ROWS);
    let (page, _) = encode_page(&rows).unwrap();

    let mut group = c.benchmark_group("tsf_page_decode");
    group.throughput(Throughput::Elements(rows.len() as u64));

    // Scalar reference oracle: allocates a fresh Vec for every column.
    group.bench_function("decode_page_allocating_all", |b| {
        b.iter(|| black_box(decode_page(black_box(&page)).unwrap()))
    });

    let mut buffers = PageBuffers::new();
    buffers.reserve_page();

    for (name, mask) in [
        ("into_all", ColumnMask::ALL),
        ("into_sample", ColumnMask::SAMPLE),
        ("into_timestamp_only", ColumnMask::TIMESTAMP),
        ("into_value_only", ColumnMask::VALUE),
    ] {
        group.bench_with_input(
            BenchmarkId::new("decode_page_into", name),
            &mask,
            |b, &mask| {
                b.iter(|| {
                    decode_page_into(black_box(&page), mask, &mut buffers).unwrap();
                    black_box(buffers.row_count)
                })
            },
        );
    }
    group.finish();
}

fn bench_kernels(c: &mut Criterion) {
    let mut group = c.benchmark_group("tsf_simd_kernels");
    group.throughput(Throughput::Elements(MAX_PAGE_ROWS as u64));

    let mut buf = vec![0i64; MAX_PAGE_ROWS];
    group.bench_function("fill_i64", |b| {
        b.iter(|| {
            simd::fill_i64(black_box(&mut buf), 42);
            black_box(buf[MAX_PAGE_ROWS - 1])
        })
    });

    group.bench_function("fill_delta_i64", |b| {
        b.iter(|| {
            simd::fill_delta_i64(black_box(&mut buf), BASE_TS, STEP_MS);
            black_box(buf[MAX_PAGE_ROWS - 1])
        })
    });

    let deltas: Vec<i64> = (0..MAX_PAGE_ROWS as i64).map(|i| i % 7).collect();
    group.bench_function("prefix_sum_i64_in_place", |b| {
        b.iter_batched(
            || deltas.clone(),
            |mut data| {
                simd::prefix_sum_i64_in_place(black_box(&mut data), BASE_TS);
                black_box(data[MAX_PAGE_ROWS - 1])
            },
            criterion::BatchSize::SmallInput,
        )
    });

    let values: Vec<i64> = (0..MAX_PAGE_ROWS as i64).map(|i| i * 3 - 7_000).collect();
    let mut encoded = Vec::new();
    marshal_var_int64s(&mut encoded, &values);
    group.bench_function("unmarshal_var_int64s_4way", |b| {
        b.iter(|| {
            let mut dst = vec![0i64; MAX_PAGE_ROWS];
            simd::unmarshal_var_int64s_4way(black_box(&mut dst), black_box(&encoded)).unwrap();
            black_box(dst[MAX_PAGE_ROWS - 1])
        })
    });

    group.finish();
}

criterion_group!(benches, bench_page_decode, bench_kernels);
criterion_main!(benches);
