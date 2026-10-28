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

//! A local write-ahead log for buffered writes.
//!
//! Each write request is appended as one frame before it is acknowledged. A
//! frame holds the row sequence range, the time range and the record batch
//! encoded with Arrow IPC. After the buffered frames have been persisted into
//! an SST, the WAL files they came from are deleted.
//!
//! On startup every `*.wal` file is replayed in epoch order. A truncated tail
//! (e.g. after a crash in the middle of a write) stops replay of that file;
//! frames before it are still valid because writes are only acknowledged after
//! they have been fully appended.

use std::{
    io::Cursor,
    path::{Path, PathBuf},
};

use anyhow::Context;
use arrow::{
    array::RecordBatch,
    ipc::{reader::FileReader, writer::FileWriter},
};
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
    sync::Mutex,
};
use tracing::warn;

use crate::{
    config::WalConfig,
    sst::SstFile,
    types::{TimeRange, Timestamp},
    Result,
};

const FILE_MAGIC: &[u8; 8] = b"HRWAL001";
/// len, seq, start, end.
const FRAME_HEADER_LEN: usize = 32;
/// A single frame should never be larger than this. Anything bigger means the
/// file is corrupted and replay stops.
const MAX_FRAME_LEN: u64 = 1 << 30;

/// One write request buffered in the WAL.
pub struct WalFrame {
    /// Sequence of the first row in the batch.
    pub seq: u64,
    pub time_range: TimeRange,
    pub batch: RecordBatch,
}

struct WalState {
    current: Option<(PathBuf, File)>,
}

pub struct Wal {
    dir: PathBuf,
    sync_on_write: bool,
    state: Mutex<WalState>,
}

impl Wal {
    pub fn new(base_path: &str, config: &WalConfig) -> Result<Self> {
        let dir = match &config.dir {
            Some(dir) => PathBuf::from(dir),
            None => Path::new(base_path).join("wal"),
        };
        std::fs::create_dir_all(&dir).with_context(|| format!("create wal directory {:?}", dir))?;
        Ok(Self {
            dir,
            sync_on_write: config.sync_on_write,
            state: Mutex::new(WalState { current: None }),
        })
    }

    /// Append one frame. This must be called in the same critical section as
    /// pushing the frame into the write buffer, so a concurrent flush can not
    /// rotate the WAL and drain the buffer in between.
    pub async fn append(&self, frame: &WalFrame) -> Result<()> {
        let encoded = encode_frame(frame)?;
        let mut state = self.state.lock().await;
        if state.current.is_none() {
            let epoch = SstFile::allocate_id();
            let path = self.dir.join(format!("{epoch}.wal"));
            let mut file = OpenOptions::new()
                .create_new(true)
                .append(true)
                .open(&path)
                .await
                .with_context(|| format!("create wal file {path:?}"))?;
            file.write_all(FILE_MAGIC)
                .await
                .with_context(|| format!("write wal header {path:?}"))?;
            state.current = Some((path, file));
        }
        let (path, file) = state.current.as_mut().expect("wal file must be opened");
        file.write_all(&encoded)
            .await
            .with_context(|| format!("append wal file {path:?}"))?;
        if self.sync_on_write {
            file.sync_data()
                .await
                .with_context(|| format!("sync wal file {path:?}"))?;
        }
        Ok(())
    }

    /// Close the current WAL file and return its path. Frames appended after
    /// this call go to a new file. The returned file can be deleted once the
    /// frames read from it have been persisted.
    pub async fn rotate(&self) -> Result<Option<PathBuf>> {
        let mut state = self.state.lock().await;
        let Some((path, mut file)) = state.current.take() else {
            return Ok(None);
        };
        if let Err(e) = file.flush().await {
            state.current = Some((path.clone(), file));
            return Err(anyhow::Error::new(e)
                .context(format!("flush wal file {path:?}"))
                .into());
        }
        if let Err(e) = file.sync_data().await {
            state.current = Some((path.clone(), file));
            return Err(anyhow::Error::new(e)
                .context(format!("sync wal file {path:?}"))
                .into());
        }
        Ok(Some(path))
    }

    /// Replay all WAL files in epoch order. Returns the frames and the files
    /// they came from; the files must be deleted only after the frames have
    /// been persisted into an SST.
    pub async fn load(&self) -> Result<(Vec<WalFrame>, Vec<PathBuf>)> {
        let mut wal_files = Vec::new();
        let mut entries = tokio::fs::read_dir(&self.dir)
            .await
            .with_context(|| format!("read wal directory {:?}", self.dir))?;
        while let Some(entry) = entries
            .next_entry()
            .await
            .context("read wal directory entry")?
        {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("wal") {
                continue;
            }
            match path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(|stem| stem.parse::<u64>().ok())
            {
                Some(epoch) => wal_files.push((epoch, path)),
                None => warn!(path = ?path, "Ignore unrecognized wal file"),
            }
        }
        wal_files.sort_by_key(|(epoch, _)| *epoch);

        let mut frames = Vec::new();
        let mut loaded_files = Vec::new();
        for (_, path) in wal_files {
            let bytes = match tokio::fs::read(&path).await {
                Ok(bytes) => bytes,
                Err(e) => {
                    warn!(path = ?path, err = ?e, "Read wal file failed");
                    continue;
                }
            };
            let num_frames = match decode_frames(&bytes) {
                Ok(decoded) => {
                    let num_frames = decoded.len();
                    frames.extend(decoded);
                    num_frames
                }
                Err(e) => {
                    warn!(path = ?path, err = ?e, "Decode wal file failed, ignore it");
                    0
                }
            };
            if num_frames > 0 {
                loaded_files.push(path);
            } else if let Err(e) = tokio::fs::remove_file(&path).await {
                warn!(path = ?path, err = ?e, "Remove empty wal file failed");
            }
        }
        Ok((frames, loaded_files))
    }

    /// Delete a WAL file whose frames have been persisted.
    pub async fn remove(&self, path: &Path) {
        if let Err(e) = tokio::fs::remove_file(path).await {
            warn!(path = ?path, err = ?e, "Remove wal file failed");
        }
    }
}

fn encode_frame(frame: &WalFrame) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    {
        let schema = frame.batch.schema();
        let mut writer =
            FileWriter::try_new(&mut payload, schema.as_ref()).context("create ipc writer")?;
        writer.write(&frame.batch).context("write ipc batch")?;
        writer.finish().context("finish ipc writer")?;
    }

    let mut buf = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
    buf.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    buf.extend_from_slice(&frame.seq.to_le_bytes());
    buf.extend_from_slice(&frame.time_range.start.0.to_le_bytes());
    buf.extend_from_slice(&frame.time_range.end.0.to_le_bytes());
    buf.extend_from_slice(&payload);
    Ok(buf)
}

/// Decode all complete frames in a WAL file. A truncated tail stops decoding;
/// frames decoded before it are returned.
fn decode_frames(bytes: &[u8]) -> Result<Vec<WalFrame>> {
    if bytes.len() < FILE_MAGIC.len() || &bytes[..FILE_MAGIC.len()] != FILE_MAGIC {
        return Err(anyhow::anyhow!("invalid wal file header").into());
    }

    let mut frames = Vec::new();
    let mut offset = FILE_MAGIC.len();
    loop {
        if offset == bytes.len() {
            break;
        }
        if bytes.len() - offset < FRAME_HEADER_LEN {
            warn!(offset, "Truncated wal frame header, stop replay");
            break;
        }
        let len = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        if len == 0 || len > MAX_FRAME_LEN {
            warn!(offset, len, "Invalid wal frame length, stop replay");
            break;
        }
        let seq = u64::from_le_bytes(bytes[offset + 8..offset + 16].try_into().unwrap());
        let start = i64::from_le_bytes(bytes[offset + 16..offset + 24].try_into().unwrap());
        let end = i64::from_le_bytes(bytes[offset + 24..offset + 32].try_into().unwrap());
        let payload_start = offset + FRAME_HEADER_LEN;
        let payload_end = payload_start + len as usize;
        if payload_end > bytes.len() {
            warn!(offset, len, "Truncated wal frame payload, stop replay");
            break;
        }

        let reader = FileReader::try_new(Cursor::new(&bytes[payload_start..payload_end]), None)
            .context("create ipc reader")?;
        let Some(batch) = reader.into_iter().next() else {
            return Err(anyhow::anyhow!("wal frame contains no record batch").into());
        };
        let batch = batch.context("decode ipc batch")?;
        frames.push(WalFrame {
            seq,
            time_range: TimeRange::new(Timestamp(start), Timestamp(end)),
            batch,
        });
        offset = payload_end;
    }
    Ok(frames)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn test_batch(sequence: i32) -> RecordBatch {
        use arrow::array::Int32Array;
        let schema = Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]);
        RecordBatch::try_new(
            std::sync::Arc::new(schema),
            vec![
                std::sync::Arc::new(Int32Array::from(vec![sequence, sequence + 1])),
                std::sync::Arc::new(Int32Array::from(vec![10, 20])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_frame_round_trip() {
        let frame = WalFrame {
            seq: 42,
            time_range: TimeRange::new(Timestamp(1), Timestamp(10)),
            batch: test_batch(1),
        };
        let encoded = encode_frame(&frame).unwrap();
        let mut file = Vec::new();
        file.extend_from_slice(FILE_MAGIC);
        file.extend_from_slice(&encoded);
        let frames = decode_frames(&file).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].seq, 42);
        assert_eq!(frames[0].time_range.start.0, 1);
        assert_eq!(frames[0].time_range.end.0, 10);
        assert_eq!(frames[0].batch.num_rows(), 2);
    }

    #[test]
    fn test_truncated_tail_is_ignored() {
        let frame = WalFrame {
            seq: 1,
            time_range: TimeRange::new(Timestamp(0), Timestamp(1)),
            batch: test_batch(1),
        };
        let encoded = encode_frame(&frame).unwrap();
        let mut file = Vec::new();
        file.extend_from_slice(FILE_MAGIC);
        file.extend_from_slice(&encoded);
        // The second frame is only partially written.
        file.extend_from_slice(&encoded);

        // The trailing bytes of a partially written frame must not break
        // replay of the complete frames before it.
        let truncated = &file[..file.len() - 7];
        let frames = decode_frames(truncated).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].seq, 1);

        // A file with only a partial frame replays nothing.
        let only_partial = &file[..FILE_MAGIC.len() + 4];
        assert!(decode_frames(only_partial).unwrap().is_empty());
    }
}
