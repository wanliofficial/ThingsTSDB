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

//! Metaindex rows for a mergeset part (ADR-0001 §3.3).
//!
//! ```text
//! varbytes(firstItem) u32be(blockHeadersCount)
//! u64be(indexBlockOffset) u32be(indexBlockSize)
//! ```
//!
//! All rows are concatenated in `firstItem` order, zstd-compressed as one
//! unit and loaded fully into memory when the part is opened.

use crate::mergeset::block_header::{marshal_bytes, take_bytes, take_u32, take_u64, varint_len};

/// One index block's location and its first item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaindexRow {
    pub first_item: Vec<u8>,
    pub block_headers_count: u32,
    pub index_block_offset: u64,
    pub index_block_size: u32,
}

impl MetaindexRow {
    pub fn marshal(&self, dst: &mut Vec<u8>) {
        marshal_bytes(dst, &self.first_item);
        dst.extend_from_slice(&self.block_headers_count.to_be_bytes());
        dst.extend_from_slice(&self.index_block_offset.to_be_bytes());
        dst.extend_from_slice(&self.index_block_size.to_be_bytes());
    }

    pub fn encoded_len(&self) -> usize {
        varint_len(self.first_item.len() as u64) + self.first_item.len() + 4 + 8 + 4
    }

    pub fn unmarshal(src: &mut &[u8]) -> Result<Self, String> {
        Ok(Self {
            first_item: take_bytes(src)?.to_vec(),
            block_headers_count: take_u32(src)?,
            index_block_offset: take_u64(src)?,
            index_block_size: take_u32(src)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> MetaindexRow {
        MetaindexRow {
            first_item: b"row{a=1}".to_vec(),
            block_headers_count: 3,
            index_block_offset: 0x0102_0304_0506_0708,
            index_block_size: 0x1112_1314,
        }
    }

    #[test]
    fn test_metaindex_row_golden_bytes() {
        let mut got = Vec::new();
        sample().marshal(&mut got);
        let want: Vec<u8> = [
            &[8u8][..],
            b"row{a=1}",
            &3u32.to_be_bytes(),
            &0x0102_0304_0506_0708u64.to_be_bytes(),
            &0x1112_1314u32.to_be_bytes(),
        ]
        .concat();
        assert_eq!(got, want);
        assert_eq!(sample().encoded_len(), want.len());
    }

    #[test]
    fn test_metaindex_row_round_trip_and_truncation() {
        let mut bytes = Vec::new();
        sample().marshal(&mut bytes);
        let mut src = bytes.as_slice();
        assert_eq!(MetaindexRow::unmarshal(&mut src).unwrap(), sample());
        assert!(src.is_empty());
        for cut in 0..bytes.len() {
            let mut src = &bytes[..cut];
            assert!(MetaindexRow::unmarshal(&mut src).is_err(), "cut {cut}");
        }
    }

    #[test]
    fn test_metaindex_rows_marshal_back_to_back() {
        let rows: Vec<MetaindexRow> = (0..64)
            .map(|i| MetaindexRow {
                first_item: format!("item-{i:04}").into_bytes(),
                block_headers_count: i + 1,
                index_block_offset: i as u64 * 4096,
                index_block_size: 2048,
            })
            .collect();
        let mut bytes = Vec::new();
        for row in &rows {
            row.marshal(&mut bytes);
        }
        let mut src = bytes.as_slice();
        for want in &rows {
            assert_eq!(&MetaindexRow::unmarshal(&mut src).unwrap(), want);
        }
        assert!(src.is_empty());
    }
}
