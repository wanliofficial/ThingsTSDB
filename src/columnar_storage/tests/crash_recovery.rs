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

//! `kill -9` recovery (ADR-0002 §5.3, §12 T3).
//!
//! Each case starts `crash_helper`, a child process that writes through a real
//! `ObjectBasedStorage` until it reaches a chosen point and then blocks. The
//! test sends `SIGKILL`, reopens the same root in-process and asserts that:
//!
//! - every acknowledged row is still readable, exactly once;
//! - the WAL is replayed and then truncated, not left behind;
//! - whatever the crash left unreferenced is reported as an orphan and removed,
//!   leaving only files the manifest still points at.
//!
//! The crash points span the commit sequence from ADR-0002 §5.3 — data file
//! PUT, manifest delta PUT, WAL truncation — plus a compaction killed with its
//! output written but not committed.

use std::{
    collections::HashMap,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{mpsc, Arc},
    time::Duration,
};

use arrow::{
    array::{Float64Array, Int64Array, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use columnar_storage::{
    config::{DataFileFormat, StorageConfig},
    storage::{ColumnarStorage, ObjectBasedStorage, ScanRequest, StorageRuntimes},
    types::{TimeRange, Timestamp},
};
use futures::TryStreamExt;
use object_store::local::LocalFileSystem;

const STEP_MS: i64 = 1_000;
const SERIES: u64 = 8;
const ROWS: usize = 160;
const DAY_MS: i64 = 86_400_000;

/// Start of the UTC day the dataset lives in: every write stays inside one 24 h
/// segment, so a flush produces exactly one data file.
fn base() -> i64 {
    1_700_000_000_000i64.div_euclid(DAY_MS) * DAY_MS
}

fn sample_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("metric_id", DataType::UInt64, false),
        Field::new("tsid", DataType::UInt64, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]))
}

/// Storage config for the *recovery* process: no background flush and no
/// automatic compaction, so the assertions see exactly what the crash left.
fn recovery_config() -> StorageConfig {
    let mut config = StorageConfig {
        format: DataFileFormat::Tsf,
        ..StorageConfig::default()
    };
    config.buffer.flush_interval = common::ReadableDuration::hours(1);
    config.scheduler.schedule_interval = common::ReadableDuration::hours(1);
    config.scheduler.input_sst_min_num = 1_000;
    config
}

/// What the child leaves on disk when it is killed.
struct Case {
    phase: &'static str,
    /// Objects under `data/`, orphans included.
    data_objects: usize,
    /// `*.wal` files still present.
    wal_files: usize,
    /// Orphans the first GC pass must remove.
    orphans: usize,
    /// Files the manifest still references, i.e. what the sweep keeps.
    retained: usize,
}

const CASES: &[Case] = &[
    // Rows only ever reached the WAL.
    Case {
        phase: "wal",
        data_objects: 0,
        wal_files: 1,
        orphans: 0,
        retained: 1,
    },
    // A clean flush: manifest committed, WAL truncated.
    Case {
        phase: "flushed",
        data_objects: 1,
        wal_files: 0,
        orphans: 0,
        retained: 1,
    },
    // Committed rows plus an object the manifest never mentions.
    Case {
        phase: "orphan",
        data_objects: 2,
        wal_files: 0,
        orphans: 1,
        retained: 1,
    },
    // Data object written, manifest commit never ran, WAL intact.
    Case {
        phase: "crash-flush",
        data_objects: 1,
        wal_files: 1,
        orphans: 1,
        retained: 1,
    },
    // Manifest delta written, in-memory update and WAL truncation never ran.
    Case {
        phase: "crash-commit",
        data_objects: 1,
        wal_files: 1,
        orphans: 0,
        // The committed file and the file the WAL replay wrote, both live;
        // they describe the same rows and the merge collapses the duplicates.
        retained: 2,
    },
    // Compaction output written, its manifest update never ran: ten committed
    // input files plus the uncommitted output.
    Case {
        phase: "crash-compact",
        data_objects: 11,
        wal_files: 0,
        orphans: 1,
        retained: 10,
    },
];

#[test]
fn test_kill_9_with_only_wal() {
    run_case(&CASES[0]);
}

#[test]
fn test_kill_9_after_flush() {
    run_case(&CASES[1]);
}

#[test]
fn test_kill_9_gc_removes_orphan() {
    run_case(&CASES[2]);
}

#[test]
fn test_kill_9_before_manifest_commit() {
    run_case(&CASES[3]);
}

#[test]
fn test_kill_9_after_manifest_commit() {
    run_case(&CASES[4]);
}

#[test]
fn test_kill_9_during_compaction() {
    run_case(&CASES[5]);
}

fn run_case(case: &Case) {
    let root_dir = temp_dir::TempDir::new().unwrap();
    let root = root_dir.path().to_path_buf();

    // 1. Run the child to the crash point, then SIGKILL it.
    let (child, ready) = spawn_helper(&root, case.phase);
    await_ready(case.phase, &ready);
    kill_child(case.phase, child);

    // 2. Check the state the kill actually left behind. Without this the rest
    //    of the test could pass on a run that crashed somewhere else.
    assert_eq!(
        data_objects(&root).len(),
        case.data_objects,
        "{}: data objects at the crash",
        case.phase
    );
    assert_eq!(
        wal_files(&root).len(),
        case.wal_files,
        "{}: WAL files at the crash",
        case.phase
    );

    // 3. Recover in-process: reopen, read everything back, then sweep.
    let worker = Arc::new(tokio::runtime::Runtime::new().unwrap());
    let runtimes = StorageRuntimes::new(worker.clone(), worker.clone());
    let driver = tokio::runtime::Runtime::new().unwrap();
    driver.block_on(async move {
        let storage = ObjectBasedStorage::try_new(
            root.to_string_lossy().into_owned(),
            Duration::from_hours(24),
            Arc::new(LocalFileSystem::new()),
            sample_schema(),
            3,
            recovery_config(),
            runtimes,
        )
        .await
        .expect("reopen storage");

        // The scan flushes anything replayed from the WAL, so `find_ssts` sees
        // the recovered rows.
        let stream = storage
            .scan(ScanRequest {
                range: TimeRange::new(
                    Timestamp(base()),
                    Timestamp(base() + ROWS as i64 * STEP_MS + 1),
                ),
                predicate: vec![],
                projections: None,
            })
            .await
            .expect("scan");
        let batches = stream.try_collect::<Vec<_>>().await.expect("collect");
        assert_rows(case.phase, &batches);

        // The flush inside the scan committed the WAL's frames and removed
        // them, so nothing may be left to replay on the next start.
        assert!(
            wal_files(&root).is_empty(),
            "{}: WAL not truncated after recovery: {:?}",
            case.phase,
            wal_files(&root)
        );

        // The first sweep must remove everything unreferenced, and a second one
        // must find nothing: what is left is exactly what the manifest points
        // at.
        let gc = storage.gc_orphans().await.expect("gc");
        assert_eq!(gc.failed_deletes, 0, "{}: failed deletes", case.phase);
        assert_eq!(
            gc.orphan_files, case.orphans,
            "{}: GC removed the wrong number of orphans",
            case.phase
        );
        assert_eq!(
            gc.retained_files, case.retained,
            "{}: GC retained the wrong files, it deleted or kept live data",
            case.phase
        );
        let again = storage.gc_orphans().await.expect("second gc");
        assert_eq!(
            again.orphan_files, 0,
            "{}: second GC pass still found orphans",
            case.phase
        );
        assert_eq!(
            again.retained_files, gc.retained_files,
            "{}: retained set changed between GC passes",
            case.phase
        );
    });
}

/// Every acknowledged row is readable exactly once, with its original series
/// and value. A replay may write a row twice (two files, two sequences); the
/// LWW merge must collapse that back to one.
fn assert_rows(phase: &str, batches: &[arrow::array::RecordBatch]) {
    let mut got: HashMap<i64, (u64, f64)> = HashMap::new();
    for batch in batches {
        let tsid = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("tsid column");
        let timestamp = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("timestamp column");
        let value = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("value column");
        for i in 0..batch.num_rows() {
            let previous = got.insert(timestamp.value(i), (tsid.value(i), value.value(i)));
            assert!(
                previous.is_none(),
                "{phase}: duplicate row at {}",
                timestamp.value(i)
            );
        }
    }

    assert_eq!(got.len(), ROWS, "{phase}: recovered row count");
    for i in 0..ROWS {
        let ts = base() + i as i64 * STEP_MS;
        let (tsid, value) = *got
            .get(&ts)
            .unwrap_or_else(|| panic!("{phase}: row {i} at {ts} is missing"));
        assert_eq!(tsid, i as u64 % SERIES, "{phase}: row {i} series");
        assert_eq!(value, i as f64 * 0.5, "{phase}: row {i} value");
    }
}

fn spawn_helper(root: &Path, phase: &str) -> (Child, mpsc::Receiver<String>) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_crash_helper"))
        .arg(root)
        .arg(phase)
        .arg(ROWS.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn crash_helper");

    let stdout = child.stdout.take().expect("child stdout");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            match line {
                Ok(line) => {
                    if tx.send(line).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
    (child, rx)
}

fn await_ready(phase: &str, ready: &mpsc::Receiver<String>) {
    loop {
        match ready.recv_timeout(Duration::from_secs(60)) {
            Ok(line) if line.starts_with("READY") => return,
            Ok(line) => eprintln!("crash_helper[{phase}]: {line}"),
            Err(e) => panic!("crash_helper[{phase}] did not reach its crash point: {e}"),
        }
    }
}

fn kill_child(phase: &str, mut child: Child) {
    child.kill().expect("SIGKILL");
    let status = child.wait().expect("wait for killed child");
    assert!(
        status.code().is_none(),
        "crash_helper[{phase}] exited normally with {status:?} instead of being killed"
    );
}

fn data_objects(root: &Path) -> Vec<PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else {
                out.push(path);
            }
        }
    }
    let mut out = Vec::new();
    walk(&root.join("data"), &mut out);
    out
}

fn wal_files(root: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(root.join("wal")) else {
        return Vec::new();
    };
    entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "wal"))
        .collect()
}
