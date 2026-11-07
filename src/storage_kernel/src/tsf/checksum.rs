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

//! CRC32C (Castagnoli), used by TSF file structures and part indexes.
//!
//! The implementation prefers the SSE4.2 `crc32` instruction at runtime and
//! falls back to a portable slicing-by-8 implementation. Both paths are
//! checked against the same known vectors and against each other in tests.

const POLY: u32 = 0x82F6_3B78;

const fn make_tables() -> [[u32; 256]; 8] {
    let mut tables = [[0u32; 256]; 8];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ POLY
            } else {
                crc >> 1
            };
            bit += 1;
        }
        tables[0][i] = crc;
        i += 1;
    }
    let mut k = 1;
    while k < 8 {
        let mut i = 0;
        while i < 256 {
            let prev = tables[k - 1][i];
            tables[k][i] = (prev >> 8) ^ tables[0][(prev & 0xff) as usize];
            i += 1;
        }
        k += 1;
    }
    tables
}

static TABLES: [[u32; 256]; 8] = make_tables();

/// Returns the CRC32C checksum of `data`.
#[inline]
pub fn crc32c(data: &[u8]) -> u32 {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if has_sse42() {
            // SAFETY: the runtime check above guarantees SSE4.2 support.
            return unsafe { !crc32c_update_hw(!0, data) };
        }
    }
    !crc32c_update_sw(!0, data)
}

/// Returns the CRC32C checksum of the concatenation of `parts` without
/// copying them.
#[inline]
pub fn crc32c_multi(parts: &[&[u8]]) -> u32 {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if has_sse42() {
            let mut crc = !0u32;
            for part in parts {
                // SAFETY: the runtime check above guarantees SSE4.2 support.
                crc = unsafe { crc32c_update_hw(crc, part) };
            }
            return !crc;
        }
    }
    let mut crc = !0u32;
    for part in parts {
        crc = crc32c_update_sw(crc, part);
    }
    !crc
}

/// Portable implementation, exposed for differential tests.
#[doc(hidden)]
pub fn crc32c_software(data: &[u8]) -> u32 {
    !crc32c_update_sw(!0, data)
}

fn crc32c_update_sw(mut crc: u32, data: &[u8]) -> u32 {
    let (chunks, remainder) = data.as_chunks::<8>();
    for c in chunks {
        let a = u32::from_le_bytes([c[0], c[1], c[2], c[3]]) ^ crc;
        let b = u32::from_le_bytes([c[4], c[5], c[6], c[7]]);
        crc = TABLES[7][(a & 0xff) as usize]
            ^ TABLES[6][((a >> 8) & 0xff) as usize]
            ^ TABLES[5][((a >> 16) & 0xff) as usize]
            ^ TABLES[4][((a >> 24) & 0xff) as usize]
            ^ TABLES[3][(b & 0xff) as usize]
            ^ TABLES[2][((b >> 8) & 0xff) as usize]
            ^ TABLES[1][((b >> 16) & 0xff) as usize]
            ^ TABLES[0][((b >> 24) & 0xff) as usize];
    }
    for &b in remainder {
        crc = (crc >> 8) ^ TABLES[0][((crc ^ b as u32) & 0xff) as usize];
    }
    crc
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[inline]
fn has_sse42() -> bool {
    static HAS_SSE42: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *HAS_SSE42.get_or_init(|| std::arch::is_x86_feature_detected!("sse4.2"))
}

/// Hardware CRC32C update. `crc` is the running (inverted) value, and the
/// returned value is still inverted; callers apply the final `!`.
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "sse4.2")]
unsafe fn crc32c_update_hw(crc: u32, data: &[u8]) -> u32 {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::{_mm_crc32_u64, _mm_crc32_u8};
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::{_mm_crc32_u64, _mm_crc32_u8};

    let mut crc = crc as u64;
    let (chunks, remainder) = data.as_chunks::<8>();
    for c in chunks {
        crc = _mm_crc32_u64(crc, u64::from_le_bytes(*c));
    }
    for &b in remainder {
        crc = _mm_crc32_u8(crc as u32, b) as u64;
    }
    crc as u32
}

#[cfg(test)]
mod tests {
    use super::{crc32c, crc32c_multi, crc32c_software};

    #[test]
    fn test_crc32c_known_vectors() {
        assert_eq!(crc32c(b""), 0);
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(
            crc32c(b"The quick brown fox jumps over the lazy dog"),
            0x2262_0404
        );
    }

    #[test]
    fn test_crc32c_hardware_matches_software() {
        // Deterministic pseudo-random corpus covering every remainder modulo
        // 8 and multi-block inputs.
        let mut state = 0x1234_5678_9ABC_DEF0u64;
        for len in 0..=4096usize {
            let mut buf = vec![0u8; len];
            for b in &mut buf {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                *b = state as u8;
            }
            assert_eq!(crc32c(&buf), crc32c_software(&buf), "mismatch at len {len}");
        }
    }

    #[test]
    fn test_crc32c_multi_matches_concatenation() {
        let a = b"hello ".as_slice();
        let b = b"world, this is a multi-part crc check".as_slice();
        let c = [0u8; 13];
        let mut joined = Vec::new();
        joined.extend_from_slice(a);
        joined.extend_from_slice(b);
        joined.extend_from_slice(&c);
        assert_eq!(crc32c_multi(&[a, b, &c]), crc32c(&joined));
        assert_eq!(crc32c_multi(&[]), 0);
    }
}
