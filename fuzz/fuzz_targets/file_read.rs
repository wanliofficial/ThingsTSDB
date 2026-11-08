#![no_main]

//! Two ways into the file reader: a well-formed file encoded from fuzz-derived
//! rows (deep coverage of index and page decoding), and the raw input treated
//! as an arbitrary/corrupt object (error paths). Neither may panic.
use libfuzzer_sys::fuzz_target;
use storage_kernel::tsf::{
    file::{encode_file, TimeRange, TsfReader, WriterOptions},
    Row,
};

const MAX_ROWS: usize = 512;
const CHUNK: usize = 24;

fn rows_from(data: &[u8]) -> Vec<Row> {
    data.chunks_exact(CHUNK)
        .take(MAX_ROWS)
        .enumerate()
        .map(|(i, c)| Row {
            metric_id: u64::from_le_bytes(c[0..8].try_into().unwrap()) % 3,
            series_id: u64::from_le_bytes(c[8..16].try_into().unwrap()) % 4,
            // A quarter of a day, with wraparound, so the same series can have
            // out-of-order rows and the writer has to sort them.
            timestamp: i as i64 * 15_000 - 1_000,
            value: f64::from_bits(u64::from_le_bytes(c[16..24].try_into().unwrap())),
            sequence: i as u64,
            reserved: 0,
        })
        .collect()
}

fn scan(reader: &TsfReader) {
    let Ok(rows) = reader.read_all() else {
        return;
    };
    let metric = rows.first().map(|r| r.metric_id).unwrap_or(0);
    if let Ok(ids) = reader.series_ids(metric) {
        if let Some(&id) = ids.first() {
            let _ = reader.read_series(metric, id, TimeRange::new(i64::MIN, i64::MAX), None);
        }
    }
    let _ = reader.read_series(metric, 0, TimeRange::new(i64::MIN, 0), Some((0, u64::MAX)));
}

fuzz_target!(|data: &[u8]| {
    // A corrupt object must be rejected or decoded defensively.
    if let Ok(reader) = TsfReader::from_bytes(data.to_vec()) {
        scan(&reader);
    }

    let rows = rows_from(data);
    if rows.is_empty() {
        return;
    }
    let opts = WriterOptions::default();
    let Ok((bytes, _)) = encode_file(rows, (-2_000, 2_000), &opts) else {
        return;
    };
    let reader = TsfReader::from_bytes(bytes).unwrap();
    scan(&reader);
});
