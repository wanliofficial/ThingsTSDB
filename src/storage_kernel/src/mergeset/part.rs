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

//! A single-file mergeset part.
//!
//! Layout (all integers little-endian for the prototype):
//!
//! ```text
//! magic "MSETP001"
//! items.bin            concatenated marshaled item blocks
//! lens.bin             concatenated marshaled lens blocks
//! index.bin            zstd(block headers)
//! footer (64 bytes)    counts, offsets, index CRC32C
//! ```
//!
//! This is a deliberately reduced version of the ADR-0001 part layout: block
//! headers are loaded as one index block instead of `metaindex -> index
//! blocks -> headers`, and the footer replaces `metadata.json`. Search,
//! marshal/unmarshal and merge costs match the full design.

use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

use crate::{
    mergeset::inmemory_block::{InmemoryBlock, MarshalType, StorageBlock, MAX_INMEMORY_BLOCK_SIZE},
    tsf::{
        checksum::crc32c,
        codec::{compress_zstd_level, decompress_zstd, marshal_var_uint64, unmarshal_var_uint64},
    },
};

const MAGIC: &[u8; 8] = b"MSETP001";
const FOOTER_MAGIC: &[u8; 8] = b"MSETF001";
const FOOTER_SIZE: usize = 64;
const HEADER_SIZE: u64 = 8;

/// On-disk description of one marshaled block.
#[derive(Debug, Clone)]
pub(crate) struct BlockHeader {
    pub first_item: Vec<u8>,
    pub common_prefix: Vec<u8>,
    pub marsh_type: MarshalType,
    pub item_count: u32,
    pub items_offset: u64,
    pub items_size: u64,
    pub lens_offset: u64,
    pub lens_size: u64,
}

/// A file-backed sorted part.
pub struct Part {
    path: PathBuf,
    file: Mutex<File>,
    headers: Vec<BlockHeader>,
    pub item_count: u64,
    pub size: u64,
}

impl Part {
    /// Opens a part previously written by [`PartWriter`].
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        let path = path.as_ref().to_path_buf();
        let mut file = File::open(&path).map_err(|e| format!("cannot open {path:?}: {e}"))?;
        let size = file
            .metadata()
            .map_err(|e| format!("cannot stat {path:?}: {e}"))?
            .len();
        if size < HEADER_SIZE + FOOTER_SIZE as u64 {
            return Err(format!("part {path:?} is too small: {size} bytes"));
        }
        let mut magic = [0u8; 8];
        file.read_exact(&mut magic)
            .map_err(|e| format!("cannot read magic from {path:?}: {e}"))?;
        if &magic != MAGIC {
            return Err(format!("unexpected part magic {magic:?} in {path:?}"));
        }
        let mut footer = [0u8; FOOTER_SIZE];
        file.seek(SeekFrom::Start(size - FOOTER_SIZE as u64))
            .and_then(|_| file.read_exact(&mut footer))
            .map_err(|e| format!("cannot read footer from {path:?}: {e}"))?;
        if &footer[0..8] != FOOTER_MAGIC {
            return Err(format!("unexpected footer magic in {path:?}"));
        }
        let item_count = u64::from_le_bytes(footer[8..16].try_into().unwrap());
        let block_count = u64::from_le_bytes(footer[16..24].try_into().unwrap());
        let index_offset = u64::from_le_bytes(footer[24..32].try_into().unwrap());
        let index_size = u64::from_le_bytes(footer[32..40].try_into().unwrap());
        let _items_size = u64::from_le_bytes(footer[40..48].try_into().unwrap());
        let _lens_size = u64::from_le_bytes(footer[48..56].try_into().unwrap());
        let index_crc = u32::from_le_bytes(footer[56..60].try_into().unwrap());

        if index_offset + index_size + FOOTER_SIZE as u64 != size {
            return Err(format!(
                "inconsistent part {path:?}: index ends at {} but file size is {size}",
                index_offset + index_size
            ));
        }
        let mut compressed = vec![0u8; index_size as usize];
        file.seek(SeekFrom::Start(index_offset))
            .and_then(|_| file.read_exact(&mut compressed))
            .map_err(|e| format!("cannot read index from {path:?}: {e}"))?;
        if crc32c(&compressed) != index_crc {
            return Err(format!("index CRC32C mismatch in {path:?}"));
        }
        let mut raw = Vec::new();
        decompress_zstd(&mut raw, &compressed)
            .map_err(|e| format!("cannot decompress index in {path:?}: {e}"))?;
        let headers = decode_headers(&raw)?;
        if headers.len() as u64 != block_count {
            return Err(format!(
                "part {path:?} declares {block_count} blocks but has {} headers",
                headers.len()
            ));
        }
        validate_headers(&headers, size)?;

        Ok(Self {
            path,
            file: Mutex::new(file),
            headers,
            item_count,
            size,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn first_item(&self) -> &[u8] {
        &self.headers[0].first_item
    }

    /// Returns the index of the block that may contain `key`.
    fn find_block(&self, key: &[u8]) -> usize {
        let pos = self
            .headers
            .partition_point(|h| h.first_item.as_slice() <= key);
        pos.saturating_sub(1)
    }

    fn read_at(&self, offset: u64, len: usize) -> Result<Vec<u8>, String> {
        let mut buf = vec![0u8; len];
        let mut file = self.file.lock().unwrap_or_else(|e| e.into_inner());
        file.seek(SeekFrom::Start(offset))
            .and_then(|_| file.read_exact(&mut buf))
            .map_err(|e| {
                format!(
                    "cannot read {} bytes at {} from {:?}: {e}",
                    len, offset, self.path
                )
            })?;
        Ok(buf)
    }

    pub(crate) fn load_block(&self, idx: usize) -> Result<InmemoryBlock, String> {
        let h = self
            .headers
            .get(idx)
            .ok_or_else(|| format!("block index {idx} out of range"))?;
        let items_data = self.read_at(h.items_offset, h.items_size as usize)?;
        let lens_data = self.read_at(h.lens_offset, h.lens_size as usize)?;
        let sb = StorageBlock {
            items_data,
            lens_data,
        };
        let mut block = InmemoryBlock::default();
        block.unmarshal_data(
            &sb,
            &h.first_item,
            &h.common_prefix,
            h.item_count,
            h.marsh_type,
        )?;
        Ok(block)
    }

    /// Returns all items with the given prefix.
    pub fn search_prefix(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, String> {
        let mut dst = Vec::new();
        let start = self.find_block(prefix);
        for idx in start..self.headers.len() {
            let first = &self.headers[idx].first_item;
            if !first.starts_with(prefix) && first.as_slice() > prefix {
                break;
            }
            let block = self.load_block(idx)?;
            let data = &block.data;
            let pos = block.items.partition_point(|it| it.bytes(data) < prefix);
            for it in &block.items[pos..] {
                let item = it.bytes(data);
                if !item.starts_with(prefix) {
                    break;
                }
                dst.push(item.to_vec());
            }
        }
        Ok(dst)
    }

    /// Returns all items in `[start, end)`.
    pub fn search_range(&self, start: &[u8], end: &[u8]) -> Result<Vec<Vec<u8>>, String> {
        let mut dst = Vec::new();
        let mut idx = self.find_block(start);
        while idx < self.headers.len() {
            if self.headers[idx].first_item.as_slice() >= end {
                break;
            }
            let block = self.load_block(idx)?;
            let data = &block.data;
            let pos = block.items.partition_point(|it| it.bytes(data) < start);
            for it in &block.items[pos..] {
                let item = it.bytes(data);
                if item >= end {
                    return Ok(dst);
                }
                dst.push(item.to_vec());
            }
            idx += 1;
        }
        Ok(dst)
    }

    pub(crate) fn cursor(&self) -> Result<BlockCursor<'_>, String> {
        BlockCursor::new(self, None)
    }
}

/// A cursor over the sorted items of a part.
pub(crate) struct BlockCursor<'a> {
    part: &'a Part,
    block_idx: usize,
    block: InmemoryBlock,
    item_idx: usize,
    loaded: bool,
}

impl<'a> BlockCursor<'a> {
    fn new(part: &'a Part, key: Option<&[u8]>) -> Result<Self, String> {
        let block_idx = match key {
            Some(k) => part.find_block(k),
            None => 0,
        };
        let mut cursor = Self {
            part,
            block_idx,
            block: InmemoryBlock::default(),
            item_idx: 0,
            loaded: false,
        };
        cursor.reload()?;
        if let Some(k) = key {
            cursor.seek(k)?;
        }
        Ok(cursor)
    }

    fn reload(&mut self) -> Result<(), String> {
        if self.block_idx < self.part.headers.len() {
            self.block = self.part.load_block(self.block_idx)?;
            self.loaded = true;
        } else {
            self.block.reset();
            self.loaded = false;
        }
        Ok(())
    }

    fn seek(&mut self, key: &[u8]) -> Result<(), String> {
        if self.block.items.is_empty() {
            self.item_idx = 0;
        } else {
            let data = &self.block.data;
            self.item_idx = self.block.items.partition_point(|it| it.bytes(data) < key);
        }
        self.normalize()
    }

    fn normalize(&mut self) -> Result<(), String> {
        while self.item_idx >= self.block.items.len() {
            if self.block_idx + 1 >= self.part.headers.len() {
                self.block.reset();
                self.loaded = false;
                self.item_idx = 0;
                return Ok(());
            }
            self.block_idx += 1;
            self.reload()?;
            self.item_idx = 0;
        }
        Ok(())
    }

    pub fn current(&self) -> Option<&[u8]> {
        if !self.loaded || self.item_idx >= self.block.items.len() {
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
}

impl PartWriter {
    pub fn new(compress_level: i32) -> Self {
        Self {
            compress_level,
            current: InmemoryBlock::default(),
            sealed: Vec::new(),
            last_item: None,
            item_count: 0,
        }
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

        // All item blocks are written before the first lens block, so the
        // lens offsets depend on the total items size.
        let total_items_size: usize = marshaled.iter().map(|m| m.items.len()).sum();
        let mut headers = Vec::with_capacity(marshaled.len());
        let mut items = Vec::with_capacity(total_items_size);
        let mut lens = Vec::new();
        let mut items_cursor = 0u64;
        let mut lens_cursor = 0u64;
        for m in marshaled {
            headers.push(BlockHeader {
                first_item: m.first_item,
                common_prefix: m.common_prefix,
                marsh_type: m.marsh_type,
                item_count: m.item_count,
                items_offset: HEADER_SIZE + items_cursor,
                items_size: m.items.len() as u64,
                lens_offset: HEADER_SIZE + total_items_size as u64 + lens_cursor,
                lens_size: m.lens.len() as u64,
            });
            items_cursor += m.items.len() as u64;
            lens_cursor += m.lens.len() as u64;
            items.extend_from_slice(&m.items);
            lens.extend_from_slice(&m.lens);
        }

        let raw_headers = encode_headers(&headers);
        let mut index = Vec::new();
        compress_zstd_level(&mut index, &raw_headers, 3);
        let index_crc = crc32c(&index);
        let index_offset = HEADER_SIZE + items.len() as u64 + lens.len() as u64;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| format!("cannot create {path:?}: {e}"))?;
        let mut file = {
            let mut f = file;
            f.write_all(MAGIC)
                .and_then(|_| f.write_all(&items))
                .and_then(|_| f.write_all(&lens))
                .and_then(|_| f.write_all(&index))
                .map_err(|e| format!("cannot write {path:?}: {e}"))?;
            f
        };

        let mut footer = [0u8; FOOTER_SIZE];
        footer[0..8].copy_from_slice(FOOTER_MAGIC);
        footer[8..16].copy_from_slice(&self.item_count.to_le_bytes());
        footer[16..24].copy_from_slice(&(headers.len() as u64).to_le_bytes());
        footer[24..32].copy_from_slice(&index_offset.to_le_bytes());
        footer[32..40].copy_from_slice(&(index.len() as u64).to_le_bytes());
        footer[40..48].copy_from_slice(&(items.len() as u64).to_le_bytes());
        footer[48..56].copy_from_slice(&(lens.len() as u64).to_le_bytes());
        footer[56..60].copy_from_slice(&index_crc.to_le_bytes());
        file.write_all(&footer)
            .and_then(|_| file.sync_all())
            .map_err(|e| format!("cannot finalize {path:?}: {e}"))?;
        let size = index_offset + index.len() as u64 + FOOTER_SIZE as u64;

        Ok(Part {
            path,
            file: Mutex::new(file),
            headers,
            item_count: self.item_count,
            size,
        })
    }
}

fn encode_headers(headers: &[BlockHeader]) -> Vec<u8> {
    let mut dst = Vec::new();
    marshal_var_uint64(&mut dst, headers.len() as u64);
    for h in headers {
        encode_bytes(&mut dst, &h.first_item);
        encode_bytes(&mut dst, &h.common_prefix);
        dst.push(h.marsh_type.as_u8());
        marshal_var_uint64(&mut dst, h.item_count as u64);
        marshal_var_uint64(&mut dst, h.items_offset);
        marshal_var_uint64(&mut dst, h.items_size);
        marshal_var_uint64(&mut dst, h.lens_offset);
        marshal_var_uint64(&mut dst, h.lens_size);
    }
    dst
}

fn decode_headers(src: &[u8]) -> Result<Vec<BlockHeader>, String> {
    let mut src = src;
    let count = take_varint(&mut src)?;
    let mut headers = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let first_item = take_bytes(&mut src)?.to_vec();
        let common_prefix = take_bytes(&mut src)?.to_vec();
        let mt = *src.first().ok_or("truncated marshal type")?;
        src = &src[1..];
        let marsh_type = MarshalType::from_u8(mt)?;
        let item_count = take_varint(&mut src)?;
        let items_offset = take_varint(&mut src)?;
        let items_size = take_varint(&mut src)?;
        let lens_offset = take_varint(&mut src)?;
        let lens_size = take_varint(&mut src)?;
        headers.push(BlockHeader {
            first_item,
            common_prefix,
            marsh_type,
            item_count: u32::try_from(item_count).map_err(|_| "item_count overflows u32")?,
            items_offset,
            items_size,
            lens_offset,
            lens_size,
        });
    }
    if !src.is_empty() {
        return Err(format!("{} trailing bytes after block headers", src.len()));
    }
    Ok(headers)
}

fn validate_headers(headers: &[BlockHeader], size: u64) -> Result<(), String> {
    if headers.is_empty() {
        return Err("part has no block headers".to_string());
    }
    for h in headers {
        if h.items_offset < HEADER_SIZE
            || h.items_offset + h.items_size > size
            || h.lens_offset < HEADER_SIZE
            || h.lens_offset + h.lens_size > size
        {
            return Err("block header points outside the part file".to_string());
        }
    }
    Ok(())
}

fn encode_bytes(dst: &mut Vec<u8>, src: &[u8]) {
    marshal_var_uint64(dst, src.len() as u64);
    dst.extend_from_slice(src);
}

fn take_varint(src: &mut &[u8]) -> Result<u64, String> {
    let (v, n) = unmarshal_var_uint64(src).ok_or("truncated varint")?;
    *src = &src[n..];
    Ok(v)
}

fn take_bytes<'a>(src: &mut &'a [u8]) -> Result<&'a [u8], String> {
    let len = take_varint(src)? as usize;
    if src.len() < len {
        return Err(format!("truncated byte string: want {len} bytes"));
    }
    let (dst, tail) = src.split_at(len);
    *src = tail;
    Ok(dst)
}
