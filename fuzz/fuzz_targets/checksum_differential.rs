#![no_main]

//! The SSE4.2 and slicing-by-8 CRC32C paths must agree on every input, and
//! checksumming parts must equal checksumming their concatenation.
use libfuzzer_sys::fuzz_target;
use storage_kernel::tsf::checksum::{crc32c, crc32c_multi, crc32c_software};

fuzz_target!(|data: &[u8]| {
    let expected = crc32c_software(data);
    assert_eq!(crc32c(data), expected);
    assert_eq!(crc32c_multi(&[data]), expected);

    let (left, right) = data.split_at(data.len() / 2);
    let mut joined = Vec::with_capacity(data.len());
    joined.extend_from_slice(left);
    joined.extend_from_slice(right);
    assert_eq!(crc32c_multi(&[left, right]), crc32c(&joined));
});
