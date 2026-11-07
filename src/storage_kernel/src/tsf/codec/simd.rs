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

//! Vectorised kernels used by the block codecs.
//!
//! Every kernel has a scalar oracle and an AVX2 implementation selected at
//! runtime. `unsafe` is confined to this file (and the CRC hardware path), as
//! required by `docs/3high-design.md`; the differential tests below compare
//! both implementations on every length class.

use std::sync::OnceLock;

use super::int::{read_var_uint64_fast, zigzag_decode};

#[inline]
fn has_avx2() -> bool {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        static HAS_AVX2: OnceLock<bool> = OnceLock::new();
        *HAS_AVX2.get_or_init(|| std::arch::is_x86_feature_detected!("avx2"))
    }
    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
    {
        false
    }
}

/// Fills `dst` with `value`.
pub fn fill_i64(dst: &mut [i64], value: i64) {
    #[cfg(target_arch = "x86_64")]
    {
        if has_avx2() {
            // SAFETY: the runtime check above guarantees AVX2 support.
            unsafe {
                fill_i64_avx2(dst, value);
            }
            return;
        }
    }
    dst.fill(value);
}

/// Fills `dst` with `first + i * delta` using wrapping arithmetic.
pub fn fill_delta_i64(dst: &mut [i64], first: i64, delta: i64) {
    #[cfg(target_arch = "x86_64")]
    {
        if has_avx2() {
            // SAFETY: the runtime check above guarantees AVX2 support.
            unsafe {
                fill_delta_i64_avx2(dst, first, delta);
            }
            return;
        }
    }
    fill_delta_i64_scalar(dst, first, delta);
}

fn fill_delta_i64_scalar(dst: &mut [i64], first: i64, delta: i64) {
    let mut v = first;
    for out in dst {
        *out = v;
        v = v.wrapping_add(delta);
    }
}

/// Converts `data` from deltas to an inclusive prefix sum:
/// `data[i] = init + data[0] + ... + data[i]` (wrapping).
pub fn prefix_sum_i64_in_place(data: &mut [i64], init: i64) {
    #[cfg(target_arch = "x86_64")]
    {
        if has_avx2() {
            // SAFETY: the runtime check above guarantees AVX2 support.
            unsafe {
                prefix_sum_i64_avx2(data, init);
            }
            return;
        }
    }
    prefix_sum_i64_scalar(data, init);
}

fn prefix_sum_i64_scalar(data: &mut [i64], init: i64) {
    let mut acc = init;
    for v in data {
        acc = acc.wrapping_add(*v);
        *v = acc;
    }
}

/// Decodes four varints per loop iteration.
///
/// The four decodes are independent, so the CPU can overlap their dependent
/// chains. Returns the unconsumed tail.
pub fn unmarshal_var_int64s_4way<'a>(dst: &mut [i64], src: &'a [u8]) -> Result<&'a [u8], String> {
    if src.len() < dst.len() {
        return Err(format!(
            "too small len(src)={}; it must be bigger or equal to len(dst)={}",
            src.len(),
            dst.len()
        ));
    }
    let mut idx = 0usize;
    let mut chunks = dst.chunks_exact_mut(4);
    for out in &mut chunks {
        let a = read_var_uint64_fast(src, &mut idx)?;
        let b = read_var_uint64_fast(src, &mut idx)?;
        let c = read_var_uint64_fast(src, &mut idx)?;
        let d = read_var_uint64_fast(src, &mut idx)?;
        out[0] = zigzag_decode(a);
        out[1] = zigzag_decode(b);
        out[2] = zigzag_decode(c);
        out[3] = zigzag_decode(d);
    }
    for out in chunks.into_remainder() {
        *out = zigzag_decode(read_var_uint64_fast(src, &mut idx)?);
    }
    Ok(&src[idx..])
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn fill_i64_avx2(dst: &mut [i64], value: i64) {
    use std::arch::x86_64::{_mm256_set1_epi64x, _mm256_storeu_si256};

    let v = _mm256_set1_epi64x(value);
    let mut chunks = dst.chunks_exact_mut(4);
    for chunk in &mut chunks {
        _mm256_storeu_si256(chunk.as_mut_ptr() as *mut _, v);
    }
    for out in chunks.into_remainder() {
        *out = value;
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn fill_delta_i64_avx2(dst: &mut [i64], first: i64, delta: i64) {
    use std::arch::x86_64::{
        _mm256_add_epi64, _mm256_set1_epi64x, _mm256_setr_epi64x, _mm256_storeu_si256,
    };

    let steps = _mm256_setr_epi64x(0, delta, delta.wrapping_mul(2), delta.wrapping_mul(3));
    let mut base = first;
    let mut chunks = dst.chunks_exact_mut(4);
    for chunk in &mut chunks {
        let res = _mm256_add_epi64(_mm256_set1_epi64x(base), steps);
        _mm256_storeu_si256(chunk.as_mut_ptr() as *mut _, res);
        base = base.wrapping_add(delta.wrapping_mul(4));
    }
    let mut v = base;
    for out in chunks.into_remainder() {
        *out = v;
        v = v.wrapping_add(delta);
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn prefix_sum_i64_avx2(data: &mut [i64], init: i64) {
    use std::arch::x86_64::{
        _mm256_add_epi64, _mm256_and_si256, _mm256_extract_epi64, _mm256_loadu_si256,
        _mm256_set1_epi64x, _mm256_setr_epi64x, _mm256_slli_si256, _mm256_storeu_si256,
    };

    // Adds the low 128-bit lane's total to the high lane after an in-lane
    // scan, turning [a, a+b, c, c+d] into [a, a+b, a+b+c, a+b+c+d].
    let hi_lane_mask = _mm256_setr_epi64x(0, 0, -1, -1);
    let mut carry = init;
    let mut chunks = data.chunks_exact_mut(4);
    for chunk in &mut chunks {
        let v = _mm256_loadu_si256(chunk.as_ptr() as *const _);
        let scan = _mm256_add_epi64(v, _mm256_slli_si256(v, 8));
        let low_total = _mm256_extract_epi64(scan, 1);
        let scan = _mm256_add_epi64(
            scan,
            _mm256_and_si256(_mm256_set1_epi64x(low_total), hi_lane_mask),
        );
        let res = _mm256_add_epi64(scan, _mm256_set1_epi64x(carry));
        _mm256_storeu_si256(chunk.as_mut_ptr() as *mut _, res);
        carry = _mm256_extract_epi64(res, 3);
    }
    for out in chunks.into_remainder() {
        carry = carry.wrapping_add(*out);
        *out = carry;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deterministic(len: usize, seed: u64) -> Vec<i64> {
        let mut state = seed;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as i64
            })
            .collect()
    }

    #[test]
    fn test_fill_differential() {
        for len in 0..257usize {
            let mut a = vec![0i64; len];
            let mut b = vec![0i64; len];
            fill_i64(&mut a, -123456789);
            b.fill(-123456789);
            assert_eq!(a, b, "fill mismatch at len {len}");
        }
        for len in 0..257usize {
            for (first, delta) in [(0i64, 1i64), (-5, 3), (i64::MAX - 8, 3), (10, -7)] {
                let mut a = vec![0i64; len];
                let mut b = vec![0i64; len];
                fill_delta_i64(&mut a, first, delta);
                fill_delta_i64_scalar(&mut b, first, delta);
                assert_eq!(a, b, "delta fill mismatch at len {len}");
            }
        }
    }

    #[test]
    fn test_prefix_sum_differential() {
        for len in 0..259usize {
            for init in [0i64, 1, -42, i64::MAX - 100] {
                let mut a = deterministic(len, 0x9E37_79B9_7F4A_7C15);
                let mut b = a.clone();
                prefix_sum_i64_in_place(&mut a, init);
                prefix_sum_i64_scalar(&mut b, init);
                assert_eq!(a, b, "prefix mismatch at len {len}, init {init}");
            }
        }
    }

    #[test]
    fn test_prefix_sum_simd_lanes() {
        let mut data = vec![1i64, 2, 3, 4, 5, 6, 7, 8];
        prefix_sum_i64_in_place(&mut data, 10);
        assert_eq!(data, vec![11, 13, 16, 20, 25, 31, 38, 46]);
    }

    #[test]
    fn test_varint_4way_matches_reference() {
        use super::super::int::{marshal_var_int64s, unmarshal_var_int64s};

        for len in 0..=257usize {
            let values: Vec<i64> = (0..len)
                .map(|i| {
                    let i = i as i64;
                    match i % 5 {
                        0 => i,
                        1 => -i * 1_000_000,
                        2 => i << 40,
                        3 => i64::MAX / (i + 1),
                        _ => i64::MIN / (i + 1),
                    }
                })
                .collect();
            let mut encoded = Vec::new();
            marshal_var_int64s(&mut encoded, &values);
            let mut a = vec![0i64; len];
            let tail = unmarshal_var_int64s_4way(&mut a, &encoded).unwrap();
            assert!(tail.is_empty());
            let mut b = vec![0i64; len];
            let tail = unmarshal_var_int64s(&mut b, &encoded).unwrap();
            assert!(tail.is_empty());
            assert_eq!(a, b, "4-way mismatch at len {len}");
        }
    }

    #[test]
    fn test_kernel_detection_is_stable() {
        // The hot path must not panic when AVX2 is unavailable; the scalar
        // fallback is exercised by the differential tests above.
        let mut data = vec![7i64; 17];
        prefix_sum_i64_in_place(&mut data, 0);
        assert_eq!(data[16], 7 * 17);
    }
}
