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

//! `kill -9` recovery test (ADR-0001 §6.4). A child process writes index WAL
//! frames and is SIGKILLed before it flushes any part; the parent then recovers
//! the index from the surviving WAL and asserts every item is searchable and no
//! data row references a missing series.

use std::{
    path::PathBuf,
    process::{Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};

use storage_kernel::mergeset::{
    part_manager::{ManagerConfig, PartManager},
    table::Table,
    table_search::TableSearch,
    wal::WalWriter,
};

/// The child helper binary path, resolved by Cargo for integration tests.
const CHILD_BIN: &str = env!("CARGO_BIN_EXE_index_wal_child");

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "mergeset-kill9-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn kill9_recovers_index_from_wal() {
    let dir = TempDir::new();
    const FRAMES: usize = 500;

    // Spawn the child; it writes FRAMES index items to the WAL, prints READY,
    // then blocks so we can SIGKILL it mid-flight (before any flush).
    let mut child = Command::new(CHILD_BIN)
        .env("INDEX_WAL_CHILD_DIR", &dir.0)
        .env("INDEX_WAL_CHILD_FRAMES", FRAMES.to_string())
        .env("INDEX_WAL_CHILD_PARTITION", "202610")
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn child");

    // Wait for READY: every frame is fsynced before this line is printed.
    let ready = wait_for_ready(&mut child, Duration::from_secs(30));
    assert!(ready, "child never signalled READY");

    // kill -9 (SIGKILL) the child: it is torn down without running destructors
    // or flushing any part.
    #[cfg(unix)]
    {
        let status = Command::new("kill")
            .arg("-9")
            .arg(child.id().to_string())
            .status()
            .expect("kill -9");
        assert!(status.success(), "kill -9 failed");
    }
    #[cfg(not(unix))]
    {
        child.kill().expect("kill child");
    }
    let _ = child.wait();

    // ---- Recovery: reopen the indexDB from disk ----
    // No parts.json exists (nothing was flushed), so recover_from_disk finds
    // no parts; replay_wal restores every item from the surviving WAL.
    let table = Arc::new(Mutex::new(Table::new()));
    let wal_dir = dir.0.join("wal");
    let wal = WalWriter::open(&wal_dir, 202610, true).expect("reopen wal");
    let pm = PartManager::new(table.clone(), &dir.0, ManagerConfig::default())
        .with_wal(wal, wal_dir, 202610);

    let recovered_parts = pm.recover_from_disk().expect("recover parts");
    assert_eq!(recovered_parts, 0, "no part was flushed before the crash");
    let replayed = pm.replay_wal().expect("replay wal");
    assert_eq!(replayed, FRAMES, "every WAL frame should be replayed");

    // Make the replayed items searchable.
    let outcome = pm.flush_once().expect("flush replayed items");
    assert_eq!(outcome.items_flushed, FRAMES);

    // Every item is now searchable exactly once: no data row references a
    // missing series, and no series is lost.
    let parts = pm.file_parts();
    assert_eq!(parts.len(), 1);
    let mut search = TableSearch::new(&parts, None).unwrap();
    let mut count = 0;
    while let Some(item) = search.next_item().unwrap() {
        let s = String::from_utf8(item).expect("utf8 item");
        assert!(s.starts_with("metric{id="), "unexpected item {s}");
        count += 1;
    }
    assert_eq!(count, FRAMES, "all replayed items must be searchable");
}

/// Reads the child's stdout line by line until `READY` is seen or the timeout.
fn wait_for_ready(child: &mut std::process::Child, timeout: Duration) -> bool {
    use std::io::BufRead as _;
    let stdout = match child.stdout.take() {
        Some(s) => s,
        None => return false,
    };
    let mut reader = std::io::BufReader::new(stdout);
    let deadline = std::time::Instant::now() + timeout;
    let mut line = String::new();
    loop {
        line.clear();
        if std::time::Instant::now() > deadline {
            return false;
        }
        match reader.read_line(&mut line) {
            Ok(0) => return false, // EOF
            Ok(_) => {
                if line.trim() == "READY" {
                    return true;
                }
            }
            Err(_) => return false,
        }
    }
}
