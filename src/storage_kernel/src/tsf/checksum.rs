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
pub fn crc32c(data: &[u8]) -> u32 {
    !update(!0, data)
}

/// Returns the CRC32C checksum of the concatenation of `parts` without
/// copying them.
pub fn crc32c_multi(parts: &[&[u8]]) -> u32 {
    let mut crc = !0u32;
    for part in parts {
        crc = update(crc, part);
    }
    !crc
}

fn update(mut crc: u32, data: &[u8]) -> u32 {
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

#[cfg(test)]
mod tests {
    use super::crc32c;

    #[test]
    fn test_crc32c_known_vectors() {
        assert_eq!(crc32c(b""), 0);
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(
            crc32c(b"The quick brown fox jumps over the lazy dog"),
            0x2262_0404
        );
    }
}
