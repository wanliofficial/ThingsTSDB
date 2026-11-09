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

//! On-disk block header for a mergeset part (ADR-0001 §3.3).
//!
//! ```text
//! varbytes(commonPrefix)
//! varbytes(firstItem)
//! u8     marshalType        // 0 = plain, 1 = zstd
//! u32be  itemsCount         // includes the first item
//! u64be  itemsBlockOffset
//! u64be  lensBlockOffset
//! u32be  itemsBlockSize
//! u32be  lensBlockSize
//! ```

use crate::{
    mergeset::inmemory_block::MarshalType,
    tsf::codec::{marshal_var_uint64, unmarshal_var_uint64},
};

/// One marshaled data block, described by its location and first item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockHeader {
    pub common_prefix: Vec<u8>,
    pub first_item: Vec<u8>,
    pub marshal_type: MarshalType,
    pub item_count: u32,
    pub items_offset: u64,
    pub lens_offset: u64,
    pub items_block_size: u32,
    pub lens_block_size: u32,
}

impl BlockHeader {
    /// Appends the header to `dst`. This is the exact on-disk form.
    pub fn marshal(&self, dst: &mut Vec<u8>) {
        marshal_bytes(dst, &self.common_prefix);
        marshal_bytes(dst, &self.first_item);
        dst.push(self.marshal_type.as_u8());
        dst.extend_from_slice(&self.item_count.to_be_bytes());
        dst.extend_from_slice(&self.items_offset.to_be_bytes());
        dst.extend_from_slice(&self.lens_offset.to_be_bytes());
        dst.extend_from_slice(&self.items_block_size.to_be_bytes());
        dst.extend_from_slice(&self.lens_block_size.to_be_bytes());
    }

    /// Number of bytes [`marshal`](Self::marshal) will append.
    pub fn encoded_len(&self) -> usize {
        varint_len(self.common_prefix.len() as u64)
            + self.common_prefix.len()
            + varint_len(self.first_item.len() as u64)
            + self.first_item.len()
            + 1
            + 4
            + 8
            + 8
            + 4
            + 4
    }

    /// Parses one header and advances `src`.
    pub fn unmarshal(src: &mut &[u8]) -> Result<Self, String> {
        let common_prefix = take_bytes(src)?.to_vec();
        let first_item = take_bytes(src)?.to_vec();
        let marshal_type = take_u8(src)?;
        let marshal_type = MarshalType::from_u8(marshal_type)?;
        let item_count = take_u32(src)?;
        let items_offset = take_u64(src)?;
        let lens_offset = take_u64(src)?;
        let items_block_size = take_u32(src)?;
        let lens_block_size = take_u32(src)?;
        Ok(Self {
            common_prefix,
            first_item,
            marshal_type,
            item_count,
            items_offset,
            lens_offset,
            items_block_size,
            lens_block_size,
        })
    }
}

pub fn marshal_bytes(dst: &mut Vec<u8>, src: &[u8]) {
    marshal_var_uint64(dst, src.len() as u64);
    dst.extend_from_slice(src);
}

pub fn take_u8(src: &mut &[u8]) -> Result<u8, String> {
    let (&v, tail) = src.split_first().ok_or("truncated u8")?;
    *src = tail;
    Ok(v)
}

pub fn take_u32(src: &mut &[u8]) -> Result<u32, String> {
    if src.len() < 4 {
        return Err("truncated u32".to_string());
    }
    let v = u32::from_be_bytes(src[..4].try_into().unwrap());
    *src = &src[4..];
    Ok(v)
}

pub fn take_u64(src: &mut &[u8]) -> Result<u64, String> {
    if src.len() < 8 {
        return Err("truncated u64".to_string());
    }
    let v = u64::from_be_bytes(src[..8].try_into().unwrap());
    *src = &src[8..];
    Ok(v)
}

pub fn take_bytes<'a>(src: &mut &'a [u8]) -> Result<&'a [u8], String> {
    let (len, n) = unmarshal_var_uint64(src).ok_or("truncated byte-string length")?;
    *src = &src[n..];
    let len = usize::try_from(len).map_err(|_| "byte-string length overflows usize")?;
    if src.len() < len {
        return Err(format!("truncated byte string: want {len} bytes"));
    }
    let (bytes, tail) = src.split_at(len);
    *src = tail;
    Ok(bytes)
}

pub fn varint_len(mut v: u64) -> usize {
    let mut n = 1;
    while v >= 0x80 {
        v >>= 7;
        n += 1;
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> BlockHeader {
        BlockHeader {
            common_prefix: b"metric{".to_vec(),
            first_item: b"metric{a=1}".to_vec(),
            marshal_type: MarshalType::Zstd,
            item_count: 7,
            items_offset: 0x0102_0304_0506_0708,
            lens_offset: 0x1112_1314_1516_1718,
            items_block_size: 0x2122_2324,
            lens_block_size: 0x3132_3334,
        }
    }

    #[test]
    fn test_block_header_golden_bytes() {
        let mut got = Vec::new();
        sample().marshal(&mut got);
        let want: Vec<u8> = [
            &[7u8][..],
            b"metric{",
            &[11u8][..],
            b"metric{a=1}",
            &[1u8][..],
            &7u32.to_be_bytes(),
            &0x0102_0304_0506_0708u64.to_be_bytes(),
            &0x1112_1314_1516_1718u64.to_be_bytes(),
            &0x2122_2324u32.to_be_bytes(),
            &0x3132_3334u32.to_be_bytes(),
        ]
        .concat();
        assert_eq!(got, want);
        assert_eq!(sample().encoded_len(), want.len());
    }

    #[test]
    fn test_block_header_round_trip() {
        let mut bytes = Vec::new();
        sample().marshal(&mut bytes);
        let mut src = bytes.as_slice();
        assert_eq!(BlockHeader::unmarshal(&mut src).unwrap(), sample());
        assert!(src.is_empty());
    }

    #[test]
    fn test_block_header_rejects_truncation_and_bad_type() {
        let mut bytes = Vec::new();
        sample().marshal(&mut bytes);
        for cut in 0..bytes.len() {
            let mut src = &bytes[..cut];
            assert!(BlockHeader::unmarshal(&mut src).is_err(), "cut {cut}");
        }
        // Marshal type 3 is not a mergeset block type.
        let mut bad = bytes.clone();
        bad[varint_len(7) + 7 + varint_len(11) + 11] = 3;
        let mut src = bad.as_slice();
        assert!(BlockHeader::unmarshal(&mut src).is_err());
    }
}
