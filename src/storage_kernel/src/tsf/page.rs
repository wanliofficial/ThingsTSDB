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

//! TSF v1 pages: encoding, statistics and decoding.

use super::Row;
use crate::tsf::{
    checksum::crc32c_multi,
    codec::{
        marshal_timestamps, marshal_values, marshal_var_int64, unmarshal_timestamps,
        unmarshal_values, unmarshal_var_int64s, MarshalType,
    },
};

pub const PAGE_HEADER_SIZE: usize = 128;
pub const MAX_PAGE_ROWS: usize = 8192;
pub const PRECISION_BITS: u8 = 64;

const FLAG_HAS_NAN: u16 = 1 << 0;
const FLAG_ALL_NAN: u16 = 1 << 1;
const FLAG_DUP_TIMESTAMPS: u16 = 1 << 2;
const FLAG_UNCOMPRESSED: u16 = 1 << 3;

/// Statistics needed by the page index and by the series chunk header.
#[derive(Debug, Clone, Default)]
pub struct PageMeta {
    pub row_count: u32,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub first_value_bits: u64,
    pub last_value_bits: u64,
    pub min_value_bits: u64,
    pub max_value_bits: u64,
    pub min_sequence: u64,
    pub max_sequence: u64,
    pub page_offset: u64,
    pub page_size: u32,
    pub has_nan: bool,
    pub has_duplicates: bool,
}

/// A decoded page.
#[derive(Debug, Clone)]
pub struct DecodedPage {
    pub timestamps: Vec<i64>,
    pub values: Vec<f64>,
    pub sequences: Vec<u64>,
    pub reserved: Vec<u64>,
}

impl DecodedPage {
    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    pub fn is_empty(&self) -> bool {
        self.timestamps.is_empty()
    }
}

/// Encodes one page and returns its bytes plus the statistics for the index.
pub fn encode_page(rows: &[Row]) -> Result<(Vec<u8>, PageMeta), String> {
    if rows.is_empty() || rows.len() > MAX_PAGE_ROWS {
        return Err(format!(
            "page must contain 1..={MAX_PAGE_ROWS} rows; got {}",
            rows.len()
        ));
    }
    let timestamps: Vec<i64> = rows.iter().map(|r| r.timestamp).collect();
    let values: Vec<i64> = rows.iter().map(|r| r.value.to_bits() as i64).collect();
    let sequences: Vec<i64> = rows.iter().map(|r| r.sequence as i64).collect();
    let reserved: Vec<i64> = rows.iter().map(|r| r.reserved as i64).collect();

    let mut ts_payload = Vec::new();
    let (ts_mt, first_timestamp) = marshal_timestamps(&mut ts_payload, &timestamps, PRECISION_BITS);
    let mut value_payload = Vec::new();
    let (value_mt, first_value) = marshal_values(&mut value_payload, &values, PRECISION_BITS);
    let mut seq_payload = Vec::new();
    let (seq_mt, first_sequence) = marshal_values(&mut seq_payload, &sequences, PRECISION_BITS);
    let mut reserved_payload = Vec::new();
    let (reserved_mt, first_reserved) =
        marshal_values(&mut reserved_payload, &reserved, PRECISION_BITS);

    let mut meta = PageMeta {
        row_count: rows.len() as u32,
        min_timestamp: timestamps[0],
        max_timestamp: timestamps[0],
        first_value_bits: rows[0].value.to_bits(),
        last_value_bits: rows[rows.len() - 1].value.to_bits(),
        min_value_bits: 0,
        max_value_bits: 0,
        min_sequence: rows[0].sequence,
        max_sequence: rows[0].sequence,
        page_offset: 0,
        page_size: 0,
        has_nan: false,
        has_duplicates: false,
    };
    let mut flags = 0u16;
    let mut has_nan = false;
    let mut all_nan = true;
    let mut saw_value = false;
    let mut prev_ts = timestamps[0];
    let mut min_value = f64::INFINITY;
    let mut max_value = f64::NEG_INFINITY;
    let mut min_value_bits = 0u64;
    let mut max_value_bits = 0u64;
    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            if timestamps[i] < prev_ts {
                return Err("page timestamps must be non-decreasing".to_string());
            }
            if timestamps[i] == prev_ts {
                flags |= FLAG_DUP_TIMESTAMPS;
            }
            prev_ts = timestamps[i];
        }
        meta.min_timestamp = meta.min_timestamp.min(row.timestamp);
        meta.max_timestamp = meta.max_timestamp.max(row.timestamp);
        meta.min_sequence = meta.min_sequence.min(row.sequence);
        meta.max_sequence = meta.max_sequence.max(row.sequence);
        if row.value.is_nan() {
            has_nan = true;
            continue;
        }
        all_nan = false;
        if !saw_value || row.value < min_value {
            min_value = row.value;
            min_value_bits = row.value.to_bits();
        }
        if !saw_value || row.value > max_value {
            max_value = row.value;
            max_value_bits = row.value.to_bits();
        }
        saw_value = true;
    }
    if has_nan {
        flags |= FLAG_HAS_NAN;
    }
    if all_nan {
        flags |= FLAG_ALL_NAN;
    }
    meta.min_value_bits = min_value_bits;
    meta.max_value_bits = max_value_bits;
    meta.has_nan = has_nan;
    meta.has_duplicates = flags & FLAG_DUP_TIMESTAMPS != 0;

    let ts_offset = PAGE_HEADER_SIZE;
    let value_offset = ts_offset + ts_payload.len();
    let seq_offset = value_offset + value_payload.len();
    let reserved_offset = seq_offset + seq_payload.len();
    let page_size = reserved_offset + reserved_payload.len();

    let mut page = vec![0u8; page_size];
    let h = &mut page[..PAGE_HEADER_SIZE];
    h[0..4].copy_from_slice(&(rows.len() as u32).to_le_bytes());
    h[4..6].copy_from_slice(&flags.to_le_bytes());
    h[6] = ts_mt.as_u8();
    h[7] = value_mt.as_u8();
    h[8] = seq_mt.as_u8();
    h[9] = reserved_mt.as_u8();
    h[10] = PRECISION_BITS;
    h[11] = 0;
    h[12..20].copy_from_slice(&meta.min_timestamp.to_le_bytes());
    h[20..28].copy_from_slice(&meta.max_timestamp.to_le_bytes());
    h[28..36].copy_from_slice(&first_timestamp.to_le_bytes());
    h[36..44].copy_from_slice(&(first_value as u64).to_le_bytes());
    h[44..52].copy_from_slice(&meta.min_value_bits.to_le_bytes());
    h[52..60].copy_from_slice(&meta.max_value_bits.to_le_bytes());
    h[60..68].copy_from_slice(&meta.min_sequence.to_le_bytes());
    h[68..76].copy_from_slice(&meta.max_sequence.to_le_bytes());
    h[76..84].copy_from_slice(&(first_sequence as u64).to_le_bytes());
    h[84..92].copy_from_slice(&(first_reserved as u64).to_le_bytes());
    h[92..96].copy_from_slice(&(ts_offset as u32).to_le_bytes());
    h[96..100].copy_from_slice(&(ts_payload.len() as u32).to_le_bytes());
    h[100..104].copy_from_slice(&(value_offset as u32).to_le_bytes());
    h[104..108].copy_from_slice(&(value_payload.len() as u32).to_le_bytes());
    h[108..112].copy_from_slice(&(seq_offset as u32).to_le_bytes());
    h[112..116].copy_from_slice(&(seq_payload.len() as u32).to_le_bytes());
    h[116..120].copy_from_slice(&(reserved_offset as u32).to_le_bytes());
    h[120..124].copy_from_slice(&(reserved_payload.len() as u32).to_le_bytes());
    page[ts_offset..value_offset].copy_from_slice(&ts_payload);
    page[value_offset..seq_offset].copy_from_slice(&value_payload);
    page[seq_offset..reserved_offset].copy_from_slice(&seq_payload);
    page[reserved_offset..page_size].copy_from_slice(&reserved_payload);
    let crc = crc32c_multi(&[&page[..124], &page[PAGE_HEADER_SIZE..]]);
    page[124..128].copy_from_slice(&crc.to_le_bytes());
    meta.page_size = page.len() as u32;
    Ok((page, meta))
}

/// Decodes one page. The CRC of the header and payloads is verified.
pub fn decode_page(page: &[u8]) -> Result<DecodedPage, String> {
    if page.len() < PAGE_HEADER_SIZE {
        return Err(format!("page is too small: {} bytes", page.len()));
    }
    let crc = u32::from_le_bytes(page[124..128].try_into().unwrap());
    if crc32c_multi(&[&page[..124], &page[PAGE_HEADER_SIZE..]]) != crc {
        return Err("page CRC32C mismatch".to_string());
    }
    let row_count = u32::from_le_bytes(page[0..4].try_into().unwrap()) as usize;
    if row_count == 0 || row_count > MAX_PAGE_ROWS {
        return Err(format!("invalid page row count {row_count}"));
    }
    let flags = u16::from_le_bytes(page[4..6].try_into().unwrap());
    if flags & FLAG_UNCOMPRESSED != 0 {
        return Err("uncompressed page flag is not supported by the prototype".to_string());
    }
    let ts_mt = page[6];
    let value_mt = page[7];
    let seq_mt = page[8];
    let reserved_mt = page[9];
    let first_timestamp = i64::from_le_bytes(page[28..36].try_into().unwrap());
    let first_value = i64::from_le_bytes(page[36..44].try_into().unwrap());
    let first_sequence = i64::from_le_bytes(page[76..84].try_into().unwrap());
    let first_reserved = i64::from_le_bytes(page[84..92].try_into().unwrap());

    let ts = payload(page, 92)?;
    let values = payload(page, 100)?;
    let sequences = payload(page, 108)?;
    let reserved = payload(page, 116)?;

    let timestamps = decode_i64s(ts, ts_mt, first_timestamp, row_count)?;
    let value_bits = decode_i64s(values, value_mt, first_value, row_count)?;
    let sequence_bits = decode_i64s(sequences, seq_mt, first_sequence, row_count)?;
    let reserved_bits = decode_i64s(reserved, reserved_mt, first_reserved, row_count)?;

    Ok(DecodedPage {
        timestamps,
        values: value_bits
            .into_iter()
            .map(|v| f64::from_bits(v as u64))
            .collect(),
        sequences: sequence_bits.into_iter().map(|v| v as u64).collect(),
        reserved: reserved_bits.into_iter().map(|v| v as u64).collect(),
    })
}

fn payload(page: &[u8], offset_field: usize) -> Result<&[u8], String> {
    let offset =
        u32::from_le_bytes(page[offset_field..offset_field + 4].try_into().unwrap()) as usize;
    let size =
        u32::from_le_bytes(page[offset_field + 4..offset_field + 8].try_into().unwrap()) as usize;
    let end = offset
        .checked_add(size)
        .ok_or_else(|| "page payload offset overflow".to_string())?;
    if offset < PAGE_HEADER_SIZE || end > page.len() {
        return Err(format!(
            "page payload out of range: offset={offset} size={size} page={}",
            page.len()
        ));
    }
    Ok(&page[offset..end])
}

fn decode_i64s(src: &[u8], codec_id: u8, first: i64, count: usize) -> Result<Vec<i64>, String> {
    if codec_id == 0 {
        let mut dst = vec![0i64; count];
        dst[0] = first;
        let tail = unmarshal_var_int64s(&mut dst[1..], src)
            .map_err(|e| format!("cannot decode plain varint payload: {e}"))?;
        if !tail.is_empty() {
            return Err(format!(
                "{} trailing bytes after plain varint payload",
                tail.len()
            ));
        }
        return Ok(dst);
    }
    let mt =
        MarshalType::from_u8(codec_id).ok_or_else(|| format!("unknown codec id {codec_id}"))?;
    let mut dst = Vec::with_capacity(count);
    if matches!(mt, MarshalType::Const | MarshalType::DeltaConst) {
        // Value codecs.
        unmarshal_values(&mut dst, src, mt, first, count)
            .map_err(|e| format!("cannot decode value payload: {e}"))?;
    } else {
        // Nearest-delta codecs are the same for timestamps and values.
        unmarshal_timestamps(&mut dst, src, mt, first, count)
            .map_err(|e| format!("cannot decode timestamp payload: {e}"))?;
    }
    Ok(dst)
}

/// Encodes `values` with the plain varint codec (id 0). Kept for completeness
/// and for tests; the page encoder currently selects the nearest-delta family.
pub fn encode_plain_varint(dst: &mut Vec<u8>, values: &[i64]) {
    for &v in &values[1..] {
        marshal_var_int64(dst, v);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(n: usize, interval: i64) -> Vec<Row> {
        (0..n)
            .map(|i| Row {
                metric_id: 1,
                series_id: 2,
                timestamp: 1_700_000_000_000 + interval * i as i64,
                value: 42.5 + i as f64,
                sequence: 7,
                reserved: 0,
            })
            .collect()
    }

    #[test]
    fn test_page_roundtrip() {
        for n in [1usize, 2, 17, 1000, MAX_PAGE_ROWS] {
            let input = rows(n, 15_000);
            let (bytes, meta) = encode_page(&input).unwrap();
            assert_eq!(meta.row_count as usize, n);
            assert_eq!(bytes.len(), meta.page_size as usize);
            let decoded = decode_page(&bytes).unwrap();
            assert_eq!(decoded.len(), n);
            for (i, row) in input.iter().enumerate() {
                assert_eq!(decoded.timestamps[i], row.timestamp);
                assert_eq!(decoded.values[i].to_bits(), row.value.to_bits());
                assert_eq!(decoded.sequences[i], row.sequence);
                assert_eq!(decoded.reserved[i], row.reserved);
            }
        }
    }

    #[test]
    fn test_page_nan_and_duplicate_flags() {
        let mut input = rows(4, 15_000);
        input[1].value = f64::NAN;
        input[2].timestamp = input[1].timestamp;
        let (bytes, _) = encode_page(&input).unwrap();
        let flags = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
        assert_ne!(flags & FLAG_HAS_NAN, 0);
        assert_ne!(flags & FLAG_DUP_TIMESTAMPS, 0);
        assert_eq!(flags & FLAG_ALL_NAN, 0);
        let decoded = decode_page(&bytes).unwrap();
        assert!(decoded.values[1].is_nan());
    }

    #[test]
    fn test_page_corruption_is_detected() {
        let input = rows(100, 15_000);
        let (mut bytes, _) = encode_page(&input).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        assert!(decode_page(&bytes).is_err());
    }

    #[test]
    fn test_plain_varint_roundtrip() {
        let values: Vec<i64> = (0..500).map(|i| i * 3 - 700).collect();
        let mut payload = Vec::new();
        encode_plain_varint(&mut payload, &values);
        let decoded = decode_i64s(&payload, 0, values[0], values.len()).unwrap();
        assert_eq!(decoded, values);
    }
}
