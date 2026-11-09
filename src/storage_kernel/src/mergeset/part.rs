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

//! A directory-backed mergeset part (ADR-0001 §3.4).
//!
//! ```text
//! {indexdb}/parts.json                  JSON array of part directory names, sorted
//! {indexdb}/{:016X}/metadata.json       partHeader (JSON, hex-encoded first/last item)
//! {indexdb}/{:016X}/metaindex.bin       zstd(metaindexRow*)
//! {indexdb}/{:016X}/index.bin           zstd(blockHeader*) per index block
//! {indexdb}/{:016X}/items.bin           itemsData blocks
//! {indexdb}/{:016X}/lens.bin            lensData blocks
//! ```
//!
//! The lookup chain is `metaindexRow -> index block -> blockHeader -> data
//! block`; only the metaindex is loaded eagerly. `metadata.json` is written
//! last, so a crash during [`PartWriter::finish`] leaves a directory that
//! [`Part::open`] rejects instead of a half-readable part.

use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use serde::{Deserialize, Serialize};

use crate::{
    mergeset::{
        block_header::BlockHeader,
        inmemory_block::{InmemoryBlock, MarshalType, StorageBlock, MAX_INMEMORY_BLOCK_SIZE},
        metaindex_row::MetaindexRow,
    },
    tsf::codec::{compress_zstd_level, decompress_zstd},
};

/// Uncompressed cap for one index block. A single larger header is allowed.
const MAX_INDEX_BLOCK_SIZE: usize = 64 * 1024;
/// Version of the part directory layout.
const FORMAT_VERSION: u32 = 1;
const METADATA_FILE: &str = "metadata.json";
const METAINDEX_FILE: &str = "metaindex.bin";
const INDEX_FILE: &str = "index.bin";
const ITEMS_FILE: &str = "items.bin";
const LENS_FILE: &str = "lens.bin";
const PARTS_FILE: &str = "parts.json";

/// Contents of `metadata.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartHeader {
    pub format_version: u32,
    pub items_count: u64,
    pub blocks_count: u64,
    /// Hex-encoded bytes, because JSON strings cannot carry arbitrary bytes.
    pub first_item: String,
    pub last_item: String,
    /// Index item sequence range covered by this part (ADR-0001 §6.3).
    #[serde(default)]
    pub min_seq: u64,
    #[serde(default)]
    pub max_seq: u64,
    /// Highest `series_id` allocated at the time this part was written.
    #[serde(default)]
    pub max_series_id: u64,
}

impl PartHeader {
    pub fn first_item_bytes(&self) -> Result<Vec<u8>, String> {
        hex_decode(&self.first_item)
    }

    pub fn last_item_bytes(&self) -> Result<Vec<u8>, String> {
        hex_decode(&self.last_item)
    }
}

/// A file-backed sorted part.
pub struct Part {
    path: PathBuf,
    items: Mutex<File>,
    lens: Mutex<File>,
    index: Mutex<File>,
    metaindex: Vec<MetaindexRow>,
    header: PartHeader,
    first_item: Vec<u8>,
    last_item: Vec<u8>,
    items_size: u64,
    lens_size: u64,
    pub item_count: u64,
    /// Total bytes of the five files that make up the part.
    pub size: u64,
}

impl Clone for Part {
    /// Reopens the underlying files. `Part` holds `Mutex<File>` handles that
    /// are not `Clone`, but a part is just a directory on disk, so a clone is a
    /// fresh open of the same path. The metadata (header, bounds, counts) is
    /// copied from the source without re-reading it.
    fn clone(&self) -> Self {
        let items = File::open(self.path.join(ITEMS_FILE)).expect("reopen items");
        let lens = File::open(self.path.join(LENS_FILE)).expect("reopen lens");
        let index = File::open(self.path.join(INDEX_FILE)).expect("reopen index");
        Self {
            path: self.path.clone(),
            items: Mutex::new(items),
            lens: Mutex::new(lens),
            index: Mutex::new(index),
            metaindex: self.metaindex.clone(),
            header: self.header.clone(),
            first_item: self.first_item.clone(),
            last_item: self.last_item.clone(),
            items_size: self.items_size,
            lens_size: self.lens_size,
            item_count: self.item_count,
            size: self.size,
        }
    }
}

impl Part {
    /// Opens a part directory previously written by [`PartWriter`].
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        let path = path.as_ref().to_path_buf();
        let metadata_bytes = fs::read(path.join(METADATA_FILE))
            .map_err(|e| format!("cannot read {METADATA_FILE} from {path:?}: {e}"))?;
        let header: PartHeader = serde_json::from_slice(&metadata_bytes)
            .map_err(|e| format!("cannot parse {METADATA_FILE} in {path:?}: {e}"))?;
        if header.format_version != FORMAT_VERSION {
            return Err(format!(
                "part {path:?} has format version {}; expected {FORMAT_VERSION}",
                header.format_version
            ));
        }

        let metaindex_compressed = fs::read(path.join(METAINDEX_FILE))
            .map_err(|e| format!("cannot read {METAINDEX_FILE} from {path:?}: {e}"))?;
        let mut metaindex_raw = Vec::new();
        decompress_zstd(&mut metaindex_raw, &metaindex_compressed)
            .map_err(|e| format!("cannot decompress {METAINDEX_FILE} in {path:?}: {e}"))?;
        let metaindex = decode_metaindex(&metaindex_raw)?;
        if metaindex.is_empty() {
            return Err(format!("part {path:?} has an empty metaindex"));
        }

        let mut total_blocks = 0u64;
        for row in &metaindex {
            total_blocks += u64::from(row.block_headers_count);
        }
        if total_blocks != header.blocks_count {
            return Err(format!(
                "part {path:?} declares {} blocks but the metaindex has {total_blocks}",
                header.blocks_count
            ));
        }

        let index = File::open(path.join(INDEX_FILE))
            .map_err(|e| format!("cannot open {INDEX_FILE} in {path:?}: {e}"))?;
        let index_size = index
            .metadata()
            .map_err(|e| format!("cannot stat {INDEX_FILE} in {path:?}: {e}"))?
            .len();
        for row in &metaindex {
            let end = row
                .index_block_offset
                .checked_add(u64::from(row.index_block_size))
                .ok_or("index block offset overflows")?;
            if end > index_size {
                return Err(format!(
                    "part {path:?}: index block ends at {end} past the file size {index_size}"
                ));
            }
        }

        let items = File::open(path.join(ITEMS_FILE))
            .map_err(|e| format!("cannot open {ITEMS_FILE} in {path:?}: {e}"))?;
        let lens = File::open(path.join(LENS_FILE))
            .map_err(|e| format!("cannot open {LENS_FILE} in {path:?}: {e}"))?;
        let items_size = items
            .metadata()
            .map_err(|e| format!("cannot stat {ITEMS_FILE} in {path:?}: {e}"))?
            .len();
        let lens_size = lens
            .metadata()
            .map_err(|e| format!("cannot stat {LENS_FILE} in {path:?}: {e}"))?
            .len();

        let first_item = header.first_item_bytes()?;
        let last_item = header.last_item_bytes()?;
        let part = Self {
            path,
            items: Mutex::new(items),
            lens: Mutex::new(lens),
            index: Mutex::new(index),
            metaindex,
            header,
            first_item,
            last_item,
            items_size,
            lens_size,
            item_count: 0,
            size: 0,
        };

        // Validate the header chain against the data files and the metadata
        // before handing the part to a reader.
        let (first_headers, _) = part.load_index_block(0)?;
        let Some(first_header) = first_headers.first() else {
            return Err("first index block is empty".to_string());
        };
        if first_header.first_item != part.first_item {
            return Err(format!(
                "part {:?}: metadata first item {} does not match the first block {}",
                part.path,
                hex_encode(&part.first_item),
                hex_encode(&first_header.first_item)
            ));
        }
        let (last_headers, _) = part.load_index_block(part.metaindex.len() - 1)?;
        let Some(last_header) = last_headers.last() else {
            return Err("last index block is empty".to_string());
        };
        if last_header.items_offset + u64::from(last_header.items_block_size) > items_size
            || last_header.lens_offset + u64::from(last_header.lens_block_size) > lens_size
        {
            return Err(format!(
                "part {:?}: last block points outside the data files",
                part.path
            ));
        }
        let last_block = part.load_block_by_header(last_header)?;
        let Some(last_item) = last_block.items.last().map(|it| it.bytes(&last_block.data)) else {
            return Err("last data block is empty".to_string());
        };
        if last_item != part.last_item.as_slice() {
            return Err(format!(
                "part {:?}: metadata last item {} does not match the last block {}",
                part.path,
                hex_encode(&part.last_item),
                hex_encode(last_item)
            ));
        }

        let size = metaindex_compressed.len() as u64
            + index_size
            + items_size
            + lens_size
            + metadata_bytes.len() as u64;
        Ok(Self {
            item_count: part.header.items_count,
            size,
            ..part
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn header(&self) -> &PartHeader {
        &self.header
    }

    pub fn first_item(&self) -> &[u8] {
        &self.first_item
    }

    pub fn last_item(&self) -> &[u8] {
        &self.last_item
    }

    /// Returns the metaindex row and header index that may contain `key`.
    ///
    /// Duplicate first items are legal (a block can end in the middle of a run
    /// of equal items), so the search deliberately lands on the last block
    /// whose first item is strictly smaller than `key` and lets the cursor
    /// advance through the run.
    fn locate(&self, key: &[u8]) -> Result<(usize, usize), String> {
        let row = self
            .metaindex
            .partition_point(|r| r.first_item.as_slice() < key)
            .saturating_sub(1)
            .min(self.metaindex.len() - 1);
        let headers = self.load_index_block(row)?.0;
        let header = headers
            .partition_point(|h| h.first_item.as_slice() < key)
            .saturating_sub(1)
            .min(headers.len() - 1);
        Ok((row, header))
    }

    fn read_file(
        file: &Mutex<File>,
        offset: u64,
        len: usize,
        name: &str,
    ) -> Result<Vec<u8>, String> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let mut buf = vec![0u8; len];
        {
            use std::io::{Read, Seek, SeekFrom};
            let mut file = file.lock().unwrap_or_else(|e| e.into_inner());
            file.seek(SeekFrom::Start(offset))
                .and_then(|_| file.read_exact(&mut buf))
                .map_err(|e| format!("cannot read {len} bytes at {offset} from {name}: {e}"))?;
        }
        Ok(buf)
    }

    fn load_index_block(&self, row_idx: usize) -> Result<(Arc<Vec<BlockHeader>>, usize), String> {
        let row = self
            .metaindex
            .get(row_idx)
            .ok_or_else(|| format!("metaindex row {row_idx} out of range"))?;
        let compressed = Self::read_file(
            &self.index,
            row.index_block_offset,
            row.index_block_size as usize,
            INDEX_FILE,
        )?;
        let mut raw = Vec::new();
        decompress_zstd(&mut raw, &compressed)
            .map_err(|e| format!("cannot decompress index block {row_idx}: {e}"))?;
        let mut src = raw.as_slice();
        let mut headers = Vec::with_capacity(row.block_headers_count as usize);
        for _ in 0..row.block_headers_count {
            headers.push(BlockHeader::unmarshal(&mut src)?);
        }
        if !src.is_empty() {
            return Err(format!(
                "index block {row_idx} has {} trailing bytes",
                src.len()
            ));
        }
        Ok((Arc::new(headers), row_idx))
    }

    fn load_block_by_header(&self, header: &BlockHeader) -> Result<InmemoryBlock, String> {
        if header.items_offset + u64::from(header.items_block_size) > self.items_size {
            return Err(format!(
                "block items range {}..{} is past the {}-byte items file",
                header.items_offset,
                header.items_offset + u64::from(header.items_block_size),
                self.items_size
            ));
        }
        if header.lens_offset + u64::from(header.lens_block_size) > self.lens_size {
            return Err(format!(
                "block lens range {}..{} is past the {}-byte lens file",
                header.lens_offset,
                header.lens_offset + u64::from(header.lens_block_size),
                self.lens_size
            ));
        }
        let items_data = Self::read_file(
            &self.items,
            header.items_offset,
            header.items_block_size as usize,
            ITEMS_FILE,
        )?;
        let lens_data = Self::read_file(
            &self.lens,
            header.lens_offset,
            header.lens_block_size as usize,
            LENS_FILE,
        )?;
        let sb = StorageBlock {
            items_data,
            lens_data,
        };
        let mut block = InmemoryBlock::default();
        block.unmarshal_data(
            &sb,
            &header.first_item,
            &header.common_prefix,
            header.item_count,
            header.marshal_type,
        )?;
        Ok(block)
    }

    /// Returns all items with the given prefix.
    pub fn search_prefix(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, String> {
        let mut dst = Vec::new();
        let mut cursor = BlockCursor::new(self, Some(prefix))?;
        while let Some(item) = cursor.current() {
            if !item.starts_with(prefix) {
                break;
            }
            dst.push(item.to_vec());
            cursor.advance()?;
        }
        Ok(dst)
    }

    /// Returns all items in `[start, end)`.
    pub fn search_range(&self, start: &[u8], end: &[u8]) -> Result<Vec<Vec<u8>>, String> {
        let mut dst = Vec::new();
        let mut cursor = BlockCursor::new(self, Some(start))?;
        while let Some(item) = cursor.current() {
            if item >= end {
                break;
            }
            dst.push(item.to_vec());
            cursor.advance()?;
        }
        Ok(dst)
    }

    pub(crate) fn cursor(&self) -> Result<BlockCursor<'_>, String> {
        BlockCursor::new(self, None)
    }

    pub(crate) fn seek_cursor(&self, key: &[u8]) -> Result<BlockCursor<'_>, String> {
        BlockCursor::new(self, Some(key))
    }
}

/// A cursor over the sorted items of a part.
pub(crate) struct BlockCursor<'a> {
    part: &'a Part,
    row_idx: usize,
    headers: Arc<Vec<BlockHeader>>,
    header_idx: usize,
    block: InmemoryBlock,
    item_idx: usize,
    done: bool,
}

impl<'a> BlockCursor<'a> {
    fn new(part: &'a Part, key: Option<&[u8]>) -> Result<Self, String> {
        let (row_idx, header_idx) = match key {
            Some(k) => part.locate(k)?,
            None => (0, 0),
        };
        let (headers, _) = part.load_index_block(row_idx)?;
        let mut cursor = Self {
            part,
            row_idx,
            headers,
            header_idx,
            block: InmemoryBlock::default(),
            item_idx: 0,
            done: false,
        };
        cursor.reload_block()?;
        if let Some(k) = key {
            cursor.seek(k)?;
        }
        Ok(cursor)
    }

    fn reload_block(&mut self) -> Result<(), String> {
        let header = self
            .headers
            .get(self.header_idx)
            .ok_or_else(|| "cursor header index out of range".to_string())?;
        self.block = self.part.load_block_by_header(header)?;
        self.item_idx = 0;
        Ok(())
    }

    fn seek(&mut self, key: &[u8]) -> Result<(), String> {
        let data = &self.block.data;
        self.item_idx = self.block.items.partition_point(|it| it.bytes(data) < key);
        self.normalize()
    }

    fn normalize(&mut self) -> Result<(), String> {
        loop {
            if self.done {
                return Ok(());
            }
            if self.item_idx < self.block.items.len() {
                return Ok(());
            }
            self.header_idx += 1;
            if self.header_idx >= self.headers.len() {
                self.row_idx += 1;
                if self.row_idx >= self.part.metaindex.len() {
                    self.done = true;
                    self.block.reset();
                    return Ok(());
                }
                self.headers = self.part.load_index_block(self.row_idx)?.0;
                self.header_idx = 0;
            }
            self.reload_block()?;
        }
    }

    pub fn current(&self) -> Option<&[u8]> {
        if self.done || self.item_idx >= self.block.items.len() {
            return None;
        }
        Some(self.block.items[self.item_idx].bytes(&self.block.data))
    }

    pub fn advance(&mut self) -> Result<(), String> {
        self.item_idx += 1;
        self.normalize()
    }
}

/// Writes a sorted part from items supplied in non-decreasing order.
pub struct PartWriter {
    compress_level: i32,
    current: InmemoryBlock,
    sealed: Vec<InmemoryBlock>,
    last_item: Option<Vec<u8>>,
    item_count: u64,
    /// Index item sequence range covered by this part (ADR-0001 §6.3). `0..0`
    /// means no tracked range.
    min_seq: u64,
    max_seq: u64,
}

impl PartWriter {
    pub fn new(compress_level: i32) -> Self {
        Self {
            compress_level,
            current: InmemoryBlock::default(),
            sealed: Vec::new(),
            last_item: None,
            item_count: 0,
            min_seq: 0,
            max_seq: 0,
        }
    }

    /// Stamps the part header with the index item sequence range `[min_seq,
    /// max_seq]` so WAL reclamation can tell the part covers those frames.
    pub fn set_seq_range(&mut self, min_seq: u64, max_seq: u64) {
        self.min_seq = min_seq;
        self.max_seq = max_seq;
    }

    pub fn add(&mut self, item: &[u8]) -> Result<(), String> {
        if item.len() > MAX_INMEMORY_BLOCK_SIZE {
            return Err(format!(
                "item size {} exceeds the maximum block size {MAX_INMEMORY_BLOCK_SIZE}",
                item.len()
            ));
        }
        if let Some(last) = &self.last_item {
            if item < last.as_slice() {
                return Err("items must be added in sorted order".to_string());
            }
        }
        if !self.current.add(item) {
            self.seal_block();
            if !self.current.add(item) {
                return Err(format!("cannot add item of size {}", item.len()));
            }
        }
        self.last_item = Some(item.to_vec());
        self.item_count += 1;
        Ok(())
    }

    fn seal_block(&mut self) {
        if !self.current.items.is_empty() {
            self.sealed.push(std::mem::take(&mut self.current));
        }
    }

    /// Writes the part directory and returns an open handle to it.
    pub fn finish(mut self, path: impl AsRef<Path>) -> Result<Part, String> {
        self.seal_block();
        if self.sealed.is_empty() {
            return Err("cannot write a part without items".to_string());
        }
        let path = path.as_ref().to_path_buf();

        struct Marshaled {
            first_item: Vec<u8>,
            common_prefix: Vec<u8>,
            marsh_type: MarshalType,
            item_count: u32,
            items: Vec<u8>,
            lens: Vec<u8>,
        }

        let mut marshaled = Vec::with_capacity(self.sealed.len());
        let mut sb = StorageBlock::default();
        for mut block in self.sealed {
            let mut first_item = Vec::new();
            let mut common_prefix = Vec::new();
            let (item_count, marsh_type) = block.marshal_sorted_data(
                &mut sb,
                &mut first_item,
                &mut common_prefix,
                self.compress_level,
            );
            marshaled.push(Marshaled {
                first_item,
                common_prefix,
                marsh_type,
                item_count,
                items: std::mem::take(&mut sb.items_data),
                lens: std::mem::take(&mut sb.lens_data),
            });
        }

        // Every items block precedes every lens block, so the lens offsets
        // depend on the total items size.
        let total_items_size: usize = marshaled.iter().map(|m| m.items.len()).sum();
        let mut headers = Vec::with_capacity(marshaled.len());
        let mut items = Vec::with_capacity(total_items_size);
        let mut lens = Vec::new();
        let mut items_cursor = 0u64;
        let mut lens_cursor = 0u64;
        for m in marshaled {
            headers.push(BlockHeader {
                common_prefix: m.common_prefix,
                first_item: m.first_item,
                marshal_type: m.marsh_type,
                item_count: m.item_count,
                items_offset: items_cursor,
                items_block_size: m.items.len() as u32,
                lens_offset: lens_cursor,
                lens_block_size: m.lens.len() as u32,
            });
            items_cursor += m.items.len() as u64;
            lens_cursor += m.lens.len() as u64;
            items.extend_from_slice(&m.items);
            lens.extend_from_slice(&m.lens);
        }

        let (index, metaindex) = build_index(&headers, 3)?;
        let metaindex_raw = {
            let mut raw = Vec::new();
            for row in &metaindex {
                row.marshal(&mut raw);
            }
            raw
        };
        let mut metaindex_compressed = Vec::new();
        compress_zstd_level(&mut metaindex_compressed, &metaindex_raw, 3);

        let header = PartHeader {
            format_version: FORMAT_VERSION,
            items_count: self.item_count,
            blocks_count: headers.len() as u64,
            first_item: hex_encode(&headers[0].first_item),
            last_item: hex_encode(
                self.last_item
                    .as_deref()
                    .ok_or("part writer has no last item")?,
            ),
            min_seq: self.min_seq,
            max_seq: self.max_seq,
            max_series_id: 0,
        };

        if path.join(METADATA_FILE).exists() {
            return Err(format!("part {path:?} already has a {METADATA_FILE}"));
        }
        fs::create_dir_all(&path).map_err(|e| format!("cannot create {path:?}: {e}"))?;
        let result = (|| -> Result<(), String> {
            write_synced(&path.join(ITEMS_FILE), &items)?;
            write_synced(&path.join(LENS_FILE), &lens)?;
            write_synced(&path.join(INDEX_FILE), &index)?;
            write_synced(&path.join(METAINDEX_FILE), &metaindex_compressed)?;
            let metadata = serde_json::to_vec(&header)
                .map_err(|e| format!("cannot encode {METADATA_FILE}: {e}"))?;
            write_metadata(&path, &metadata)?;
            sync_dir(&path)?;
            Ok(())
        })();
        if let Err(err) = result {
            let _ = fs::remove_dir_all(&path);
            return Err(err);
        }

        Part::open(&path)
    }
}

/// Groups block headers into compressed index blocks and derives the
/// metaindex. Header order is preserved.
fn build_index(
    headers: &[BlockHeader],
    compress_level: i32,
) -> Result<(Vec<u8>, Vec<MetaindexRow>), String> {
    let mut index = Vec::new();
    let mut rows = Vec::new();
    let mut block_start = 0usize;
    let mut block_len = 0usize;
    let mut raw = Vec::new();
    for (i, header) in headers.iter().enumerate() {
        let len = header.encoded_len();
        if block_len > 0 && block_len + len > MAX_INDEX_BLOCK_SIZE {
            push_index_block(
                &mut index,
                &mut rows,
                &raw,
                &headers[block_start].first_item,
                i - block_start,
                compress_level,
            );
            raw.clear();
            block_len = 0;
            block_start = i;
        }
        header.marshal(&mut raw);
        block_len += len;
    }
    if block_len > 0 {
        push_index_block(
            &mut index,
            &mut rows,
            &raw,
            &headers[block_start].first_item,
            headers.len() - block_start,
            compress_level,
        );
    }
    Ok((index, rows))
}

fn push_index_block(
    index: &mut Vec<u8>,
    rows: &mut Vec<MetaindexRow>,
    raw: &[u8],
    first_item: &[u8],
    count: usize,
    compress_level: i32,
) {
    if count == 0 {
        return;
    }
    let offset = index.len() as u64;
    compress_zstd_level(index, raw, compress_level);
    let size = (index.len() as u64 - offset) as u32;
    rows.push(MetaindexRow {
        first_item: first_item.to_vec(),
        block_headers_count: count as u32,
        index_block_offset: offset,
        index_block_size: size,
    });
}

fn decode_metaindex(raw: &[u8]) -> Result<Vec<MetaindexRow>, String> {
    let mut src = raw;
    let mut rows = Vec::new();
    while !src.is_empty() {
        rows.push(MetaindexRow::unmarshal(&mut src)?);
    }
    Ok(rows)
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|e| format!("cannot create {path:?}: {e}"))?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|e| format!("cannot write {path:?}: {e}"))
}

fn write_metadata(dir: &Path, bytes: &[u8]) -> Result<(), String> {
    let tmp = dir.join(format!("{METADATA_FILE}.tmp"));
    let _ = fs::remove_file(&tmp);
    write_synced(&tmp, bytes)?;
    let final_path = dir.join(METADATA_FILE);
    fs::rename(&tmp, &final_path)
        .map_err(|e| format!("cannot rename {tmp:?} to {final_path:?}: {e}"))
}

fn sync_dir(dir: &Path) -> Result<(), String> {
    File::open(dir)
        .and_then(|f| f.sync_all())
        .map_err(|e| format!("cannot fsync {dir:?}: {e}"))
}

/// Writes `parts.json` atomically: temp file, fsync, rename, fsync directory.
pub fn write_parts_json(indexdb_dir: &Path, part_dirs: &[&Path]) -> Result<(), String> {
    let mut names = Vec::with_capacity(part_dirs.len());
    for dir in part_dirs {
        let name = dir
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| format!("part path {dir:?} has no UTF-8 file name"))?;
        names.push(name.to_string());
    }
    names.sort();
    let bytes =
        serde_json::to_vec(&names).map_err(|e| format!("cannot encode {PARTS_FILE}: {e}"))?;
    let tmp = indexdb_dir.join(format!("{PARTS_FILE}.tmp"));
    let _ = fs::remove_file(&tmp);
    write_synced(&tmp, &bytes)?;
    let final_path = indexdb_dir.join(PARTS_FILE);
    fs::rename(&tmp, &final_path)
        .map_err(|e| format!("cannot rename {tmp:?} to {final_path:?}: {e}"))?;
    sync_dir(indexdb_dir)
}

/// Reads and validates `parts.json`, returning the part directory names.
pub fn read_parts_json(indexdb_dir: &Path) -> Result<Vec<String>, String> {
    let path = indexdb_dir.join(PARTS_FILE);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        // A cold indexDB has no `parts.json` yet; treat it as an empty part
        // set.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(format!("cannot read {path:?}: {e}")),
    };
    let names: Vec<String> =
        serde_json::from_slice(&bytes).map_err(|e| format!("cannot parse {path:?}: {e}"))?;
    for pair in names.windows(2) {
        if pair[0] >= pair[1] {
            return Err(format!("{path:?} is not sorted and deduplicated"));
        }
    }
    Ok(names)
}

fn hex_encode(src: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut dst = String::with_capacity(src.len() * 2);
    for &b in src {
        dst.push(HEX[(b >> 4) as usize] as char);
        dst.push(HEX[(b & 0xf) as usize] as char);
    }
    dst
}

fn hex_decode(src: &str) -> Result<Vec<u8>, String> {
    if src.len() % 2 != 0 {
        return Err("hex string has an odd length".to_string());
    }
    let mut dst = Vec::with_capacity(src.len() / 2);
    let bytes = src.as_bytes();
    // The input length is validated even, so every pair is complete.
    #[allow(clippy::chunks_exact_to_as_chunks)]
    for pair in bytes.chunks_exact(2) {
        let hi = hex_value(pair[0])?;
        let lo = hex_value(pair[1])?;
        dst.push((hi << 4) | lo);
    }
    Ok(dst)
}

fn hex_value(b: u8) -> Result<u8, String> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(format!("invalid hex byte {b:#04x}")),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("mergeset-part-{}-{n}", std::process::id()));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn join(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn write_part(dir: &TempDir, name: &str, items: &[Vec<u8>]) -> Part {
        let mut writer = PartWriter::new(1);
        for item in items {
            writer.add(item).unwrap();
        }
        writer.finish(dir.join(name)).unwrap()
    }

    fn items(n: usize) -> Vec<Vec<u8>> {
        let mut items: Vec<Vec<u8>> = (0..n)
            .map(|i| format!("metric{{job=\"{:04}\"}}\x02series{:08}", i % 17, i).into_bytes())
            .collect();
        items.sort();
        items
    }

    #[test]
    fn test_part_directory_layout_and_metadata() {
        let dir = TempDir::new();
        let input = items(1_000);
        let part = write_part(&dir, "0000000000000001", &input);
        for name in [
            METADATA_FILE,
            METAINDEX_FILE,
            INDEX_FILE,
            ITEMS_FILE,
            LENS_FILE,
        ] {
            assert!(part.path().join(name).is_file(), "missing {name}");
        }
        let raw = fs::read(part.path().join(METADATA_FILE)).unwrap();
        let header: PartHeader = serde_json::from_slice(&raw).unwrap();
        assert_eq!(header.format_version, FORMAT_VERSION);
        assert_eq!(header.items_count, input.len() as u64);
        assert!(header.blocks_count >= 1);
        assert_eq!(header.first_item_bytes().unwrap(), input[0]);
        assert_eq!(header.last_item_bytes().unwrap(), input[input.len() - 1]);
        assert_eq!(header.min_seq, 0);
        assert_eq!(header.max_seq, 0);
        assert_eq!(header.max_series_id, 0);
        assert_eq!(part.item_count, input.len() as u64);
        assert!(part.size > 0);
        assert_eq!(part.header(), &header);

        let reopened = Part::open(part.path()).unwrap();
        assert_eq!(reopened.first_item(), input[0]);
        assert_eq!(reopened.last_item(), input[input.len() - 1]);
        assert_eq!(reopened.search_range(b"", b"\xff\xff").unwrap(), input,);
        assert_eq!(
            reopened.search_prefix(b"metric{job=\"0003\"}").unwrap(),
            input
                .iter()
                .filter(|i| i.starts_with(b"metric{job=\"0003\"}"))
                .cloned()
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_part_duplicate_items_across_blocks() {
        // 1 KiB items leave room for ~63 per 64 KiB block, so a run of equal
        // items necessarily spans blocks and the metaindex repeats firstItem.
        let dir = TempDir::new();
        let repeated = vec![b'x'; 1_024];
        let input = vec![repeated; 300];
        let part = write_part(&dir, "dups", &input);
        assert!(part.header().blocks_count > 1);
        let (headers, _) = part.load_index_block(0).unwrap();
        assert!(headers
            .windows(2)
            .any(|w| w[0].first_item == w[1].first_item));
        assert_eq!(part.search_prefix(b"x").unwrap().len(), input.len());
        let mut cursor = part.cursor().unwrap();
        let mut count = 0;
        while cursor.current().is_some() {
            count += 1;
            cursor.advance().unwrap();
        }
        assert_eq!(count, input.len());
    }

    #[test]
    fn test_build_index_spans_multiple_index_blocks() {
        let headers: Vec<BlockHeader> = (0..4_000)
            .map(|i| BlockHeader {
                common_prefix: b"metric{".to_vec(),
                first_item: format!("metric{{job=\"{i:06}\"}}").into_bytes(),
                marshal_type: MarshalType::Plain,
                item_count: 64,
                items_offset: i as u64 * 4096,
                lens_offset: i as u64 * 512,
                items_block_size: 4096,
                lens_block_size: 512,
            })
            .collect();
        let (index, rows) = build_index(&headers, 3).unwrap();
        assert!(rows.len() > 1, "headers must span index blocks");
        let total: u32 = rows.iter().map(|r| r.block_headers_count).sum();
        assert_eq!(total, headers.len() as u32);
        let mut decoded = Vec::new();
        for row in &rows {
            let compressed = &index[row.index_block_offset as usize
                ..(row.index_block_offset as usize + row.index_block_size as usize)];
            let mut raw = Vec::new();
            decompress_zstd(&mut raw, compressed).unwrap();
            let mut src = raw.as_slice();
            for _ in 0..row.block_headers_count {
                decoded.push(BlockHeader::unmarshal(&mut src).unwrap());
            }
            assert!(src.is_empty());
            assert_eq!(
                row.first_item,
                decoded[decoded.len() - row.block_headers_count as usize].first_item
            );
        }
        assert_eq!(decoded, headers);
    }

    #[test]
    fn test_part_rejects_incomplete_or_corrupt_directories() {
        let dir = TempDir::new();
        let input = items(16);
        let part = write_part(&dir, "good", &input);
        assert!(Part::open(part.path()).is_ok());

        // A missing metadata file is an unfinished part.
        let missing = dir.join("missing");
        fs::create_dir_all(missing.join(METAINDEX_FILE).parent().unwrap()).unwrap();
        assert!(Part::open(&missing).is_err());

        // Truncated metaindex.
        let truncated = dir.join("truncated");
        fs::create_dir_all(&truncated).unwrap();
        for name in [METADATA_FILE, INDEX_FILE, ITEMS_FILE, LENS_FILE] {
            fs::copy(part.path().join(name), truncated.join(name)).unwrap();
        }
        let metaindex = fs::read(part.path().join(METAINDEX_FILE)).unwrap();
        fs::write(
            truncated.join(METAINDEX_FILE),
            &metaindex[..metaindex.len() - 1],
        )
        .unwrap();
        assert!(Part::open(&truncated).is_err());

        // Wrong format version.
        let future = dir.join("future");
        fs::create_dir_all(&future).unwrap();
        for name in [METAINDEX_FILE, INDEX_FILE, ITEMS_FILE, LENS_FILE] {
            fs::copy(part.path().join(name), future.join(name)).unwrap();
        }
        let mut header: PartHeader =
            serde_json::from_slice(&fs::read(part.path().join(METADATA_FILE)).unwrap()).unwrap();
        header.format_version = FORMAT_VERSION + 1;
        fs::write(
            future.join(METADATA_FILE),
            serde_json::to_vec(&header).unwrap(),
        )
        .unwrap();
        assert!(Part::open(&future).is_err());
    }

    #[test]
    fn test_parts_json_is_sorted_and_atomic() {
        let dir = TempDir::new();
        let p0 = dir.join("0000000000000002");
        let p1 = dir.join("0000000000000001");
        fs::create_dir_all(&p0).unwrap();
        fs::create_dir_all(&p1).unwrap();
        write_parts_json(dir.0.as_path(), &[&p0, &p1]).unwrap();
        assert_eq!(
            read_parts_json(dir.0.as_path()).unwrap(),
            ["0000000000000001", "0000000000000002"]
        );
        assert!(!dir.join("parts.json.tmp").exists());

        // Replacing an existing list overwrites it atomically.
        write_parts_json(dir.0.as_path(), &[&p0]).unwrap();
        assert_eq!(
            read_parts_json(dir.0.as_path()).unwrap(),
            ["0000000000000002"]
        );

        // A hand-written unsorted list is rejected instead of silently used.
        fs::write(dir.join(PARTS_FILE), br#"["b","a"]"#).unwrap();
        assert!(read_parts_json(dir.0.as_path()).is_err());
    }

    #[test]
    fn test_search_matches_brute_force() {
        let dir = TempDir::new();
        // Deliberately unsorted input keys, sorted through a BTreeMap first.
        let mut input: Vec<Vec<u8>> = (0..5_000u32)
            .map(|i| format!("k{:05}", (i * 7919) % 5_000).into_bytes())
            .collect();
        input.sort();
        input.dedup();
        let part = write_part(&dir, "brute", &input);

        for lo in (0..5_000u32).step_by(97) {
            let start = format!("k{lo:05}").into_bytes();
            let end = format!("k{:05}", lo + 500).into_bytes();
            let got = part.search_range(&start, &end).unwrap();
            let want: Vec<Vec<u8>> = input
                .iter()
                .filter(|i| i.as_slice() >= start.as_slice() && i.as_slice() < end.as_slice())
                .cloned()
                .collect();
            assert_eq!(got, want, "range starting at k{lo:05}");
        }
    }
}
