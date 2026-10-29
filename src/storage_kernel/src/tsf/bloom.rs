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

//! A small double-hashing bloom filter over `series_id`.
//!
//! The ADR specifies xxhash64; the prototype uses SplitMix64 so the kernel
//! crate does not take an extra dependency. The filter is an optimization
//! only, so the hash choice does not affect correctness.

pub const BITS_PER_KEY: u64 = 10;
pub const MAX_BLOOM_BYTES: usize = 4 * 1024 * 1024;

/// Builds a bloom filter for `ids`, or `None` if it would exceed the size cap.
pub fn build(ids: &[u64]) -> Option<Vec<u8>> {
    if ids.is_empty() {
        return None;
    }
    let bits = ids.len() as u64 * BITS_PER_KEY;
    let bytes = bits.div_ceil(8) as usize;
    if bytes > MAX_BLOOM_BYTES {
        return None;
    }
    let m = (bytes * 8) as u64;
    let mut filter = vec![0u8; bytes];
    for &id in ids {
        let (h1, h2) = hashes(id);
        for i in 0..7u64 {
            let pos = (h1.wrapping_add(i.wrapping_mul(h2)) % m) as usize;
            filter[pos / 8] |= 1 << (pos % 8);
        }
    }
    Some(filter)
}

/// Returns false only if `id` is definitely not in the filter.
pub fn might_contain(filter: &[u8], id: u64) -> bool {
    if filter.is_empty() {
        return true;
    }
    let m = (filter.len() * 8) as u64;
    let (h1, h2) = hashes(id);
    for i in 0..7u64 {
        let pos = (h1.wrapping_add(i.wrapping_mul(h2)) % m) as usize;
        if filter[pos / 8] & (1 << (pos % 8)) == 0 {
            return false;
        }
    }
    true
}

fn hashes(id: u64) -> (u64, u64) {
    (
        splitmix64(id ^ 0x9E37_79B9_7F4A_7C15),
        splitmix64(id ^ 0xD6E8_FEB8_6659_FD93) | 1,
    )
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_false_negatives() {
        let ids: Vec<u64> = (0..10_000).map(|i| i * 37 + 11).collect();
        let filter = build(&ids).unwrap();
        for &id in &ids {
            assert!(might_contain(&filter, id));
        }
        let false_positives = (10_000..20_000u64)
            .filter(|&id| might_contain(&filter, id))
            .count();
        // 10 bits/key targets roughly 1%; allow generous headroom.
        assert!(false_positives < 400, "false positives: {false_positives}");
    }

    #[test]
    fn test_size_cap() {
        assert!(build(&[]).is_none());
        let too_many: Vec<u64> = (0..(MAX_BLOOM_BYTES as u64 * 8 / BITS_PER_KEY + 1)).collect();
        assert!(build(&too_many).is_none());
    }
}
