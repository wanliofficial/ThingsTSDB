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

//! Mergeset index write-amplification benchmark (ADR-0001 §3.6, M3).
//!
//! Drives the steady state of the index LSM: repeated batches of flushes
//! followed by merges, and reports
//!
//! - ingest throughput (items/s over the flush phase),
//! - total flush bytes and merge bytes,
//! - write amplification, `(flush_bytes + merge_output_bytes) / flush_bytes`.
//!
//! The ADR target is write amplification below 5x for index ingestion.
//!
//! Usage: `cargo run --release -p benchmarks --bin index_mergeset_bench -- \
//! [items] [batch] [rounds]`
//!
//! Defaults: 1,000,000 items, 4,000 items per flush, 8 rounds.

use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use storage_kernel::mergeset::{
    part_manager::{ManagerConfig, PartManager},
    table::Table,
};

fn main() {
    let mut args = std::env::args().skip(1);
    let items: usize = args
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000);
    let batch: usize = args.next().and_then(|v| v.parse().ok()).unwrap_or(4_000);
    let rounds: usize = args.next().and_then(|v| v.parse().ok()).unwrap_or(8);

    let dir = std::env::temp_dir().join(format!(
        "index-mergeset-bench-{}-{}",
        std::process::id(),
        Instant::now().elapsed().as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create bench dir");

    let table = Arc::new(Mutex::new(Table::new()));
    let pm = std::sync::Arc::new(PartManager::new(
        Arc::clone(&table),
        &dir,
        ManagerConfig::default(),
    ));
    let stats = pm.stats();

    println!(
        "mergeset index bench: items={items} batch={batch} rounds={rounds} merge_threads={}",
        ManagerConfig::default().merge_threads
    );

    let per_round = items / rounds.max(1);
    let started = Instant::now();
    let mut flushed_items = 0usize;
    for round in 0..rounds {
        let mut written = 0usize;
        while written < per_round && flushed_items < items {
            let n = batch.min(per_round - written).min(items - flushed_items);
            {
                let mut t = table.lock().unwrap_or_else(|e| e.into_inner());
                for i in 0..n {
                    let id = flushed_items + i;
                    // A realistic index key: canonical-encoded posting row
                    // shape.
                    t.add(format!("{id:020}|series={:08}", id / 4).as_bytes());
                }
            }
            let outcome = pm.flush_once().expect("flush");
            assert!(outcome.flushed);
            written += n;
            flushed_items += n;
        }
        // Drain whatever the policy selects in this round.
        loop {
            let outcome = pm.merge_once().expect("merge");
            if !outcome.merged {
                break;
            }
        }
        println!(
            "round {round}: parts={} flush_bytes={} merge_out={} wa={:.2}",
            pm.file_parts().len(),
            stats.flush_bytes(),
            stats.merge_output_bytes(),
            stats.write_amplification()
        );
    }
    let elapsed = started.elapsed();

    let wa = stats.write_amplification();
    println!(
        "\nitems={} parts={} flushes={} merges={} flush_bytes={} merge_input_bytes={} merge_output_bytes={}",
        flushed_items,
        pm.file_parts().len(),
        stats.flushes(),
        stats.merges(),
        stats.flush_bytes(),
        stats.merge_input_bytes(),
        stats.merge_output_bytes()
    );
    println!(
        "ingest: {:.0} items/s in {:.2}s",
        flushed_items as f64 / elapsed.as_secs_f64(),
        elapsed.as_secs_f64()
    );
    println!("write amplification: {wa:.2}x (target < 5x)");
    if wa >= 5.0 {
        eprintln!("WARNING: write amplification {wa:.2} exceeds the ADR target");
    }

    let _ = std::fs::remove_dir_all(&dir);
}
