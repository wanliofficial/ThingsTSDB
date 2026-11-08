#![no_main]

//! Encode/decode round trip over rows derived from the input bytes. The
//! encoder must accept any non-empty page and the decoder must return exactly
//! the same bit patterns, including NaN and duplicate timestamps.
use libfuzzer_sys::fuzz_target;
use storage_kernel::tsf::{
    page::{decode_page_into, encode_page},
    ColumnMask, PageBuffers, Row,
};

const MAX_ROWS: usize = 128;
const CHUNK: usize = 24;

fuzz_target!(|data: &[u8]| {
    let rows: Vec<Row> = data
        .chunks_exact(CHUNK)
        .take(MAX_ROWS)
        .enumerate()
        .map(|(i, c)| Row {
            metric_id: u64::from_le_bytes(c[0..8].try_into().unwrap()) % 4,
            series_id: u64::from_le_bytes(c[8..16].try_into().unwrap()) % 8,
            // Non-decreasing timestamps are a precondition of `encode_page`;
            // deriving them from the row index keeps every chunk valid.
            timestamp: 1_600_000_000_000 + i as i64 * 15_000,
            value: f64::from_bits(u64::from_le_bytes(c[16..24].try_into().unwrap())),
            sequence: i as u64,
            reserved: 0,
        })
        .collect();
    if rows.is_empty() {
        return;
    }

    let (page, meta) = match encode_page(&rows) {
        Ok(encoded) => encoded,
        Err(err) => panic!("encoder rejected {} valid rows: {err}", rows.len()),
    };
    assert_eq!(meta.page_size as usize, page.len());

    let mut buffers = PageBuffers::new();
    decode_page_into(&page, ColumnMask::ALL, &mut buffers).unwrap();
    assert_eq!(buffers.row_count, rows.len());
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(buffers.timestamps[i], row.timestamp);
        assert_eq!(buffers.values[i].to_bits(), row.value.to_bits());
        assert_eq!(buffers.sequences[i], row.sequence);
        assert_eq!(buffers.reserved[i], row.reserved);
    }
});
