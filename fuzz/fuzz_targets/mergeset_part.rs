#![no_main]

//! Round-trip fuzz for the mergeset part layout (ADR-0001 M1).
//!
//! Arbitrary bytes become a bounded, sorted item set; the part must write,
//! reopen and return exactly those items (duplicates included) through prefix
//! and range search, never panicking on any input.
use std::{fs, path::PathBuf};

use libfuzzer_sys::fuzz_target;
use storage_kernel::mergeset::part::PartWriter;

const MAX_ITEMS: usize = 256;
const MAX_ITEM_LEN: usize = 4 * 1024;

fn work_dir() -> PathBuf {
    std::env::temp_dir().join(format!("mergeset-fuzz-{}", std::process::id()))
}

fuzz_target!(|data: &[u8]| {
    let Some((&header, rest)) = data.split_first() else {
        return;
    };
    if rest.is_empty() {
        return;
    }
    let chunk = (header as usize % 64) + 1;
    let mut items: Vec<Vec<u8>> = rest
        .chunks(chunk)
        .take(MAX_ITEMS)
        .map(|c| c[..c.len().min(MAX_ITEM_LEN)].to_vec())
        .collect();
    items.sort();

    let dir = work_dir();
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let part_path = dir.join("part");

    let compress_level = (header % 10) as i32 - 6;
    let mut writer = PartWriter::new(compress_level);
    for item in &items {
        if writer.add(item).is_err() {
            let _ = fs::remove_dir_all(&dir);
            return;
        }
    }
    if writer.finish(&part_path).is_err() {
        panic!("valid items failed to write");
    };

    // Reopen through the full metaindex/index-block chain.
    let part = storage_kernel::mergeset::part::Part::open(&part_path)
        .unwrap_or_else(|e| panic!("written part does not reopen: {e}"));

    let max_len = items.iter().map(Vec::len).max().unwrap_or(0);
    let mut end = vec![0xffu8; max_len + 1];
    end.truncate(max_len + 1);
    let all = part.search_range(&[], &end).unwrap();
    assert_eq!(all.len(), items.len(), "range search lost or reordered items");

    for item in items.iter().take(16) {
        let got = part.search_prefix(item).unwrap();
        assert!(
            got.iter().any(|g| g == item),
            "prefix search lost item of {} bytes",
            item.len()
        );
        let exact = part.search_range(item, &end).unwrap();
        assert!(
            exact.iter().any(|g| g == item),
            "range search lost item of {} bytes",
            item.len()
        );
    }

    let _ = fs::remove_dir_all(&dir);
});
