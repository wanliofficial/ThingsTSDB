#![no_main]

//! Any byte string is a valid page candidate: decoding must return an error,
//! never panic, overflow or read out of bounds, for every projection.
use libfuzzer_sys::fuzz_target;
use storage_kernel::tsf::{
    page::{decode_page, decode_page_into},
    ColumnMask, PageBuffers,
};

fuzz_target!(|data: &[u8]| {
    let _ = decode_page(data);

    let mut buffers = PageBuffers::new();
    for mask in [
        ColumnMask::NONE,
        ColumnMask::TIMESTAMP,
        ColumnMask::VALUE,
        ColumnMask::SAMPLE,
        ColumnMask::SEQUENCE,
        ColumnMask::RESERVED,
        ColumnMask::ALL,
    ] {
        // A second decode reuses the same buffers, so a failed decode must
        // leave them in a reusable state.
        let _ = decode_page_into(data, mask, &mut buffers);
        let _ = decode_page_into(data, ColumnMask::ALL, &mut buffers);
    }
});
