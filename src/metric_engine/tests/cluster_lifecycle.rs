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

//! Cluster lifecycle end to end (roadmap Phase 4: sharding, replication and
//! rebalancing).
//!
//! This drives the operator path a real deployment takes rather than a single
//! API call: write through a cluster, restart it, add a node, repair the copy
//! from disk, reopen and read. Each step asserts on data that must survive, so
//! a lost shard prefix or a merge that drops replica points shows up here.

use std::sync::Arc;

use columnar_storage::{config::StorageConfig, storage::StorageRuntimes, types::ObjectStoreRef};
use metric_engine::{
    cluster::{rebalance, rebalance_from_disk},
    Cluster, ClusterConfig, Label, RetentionPolicy, Sample, SearchRequest, ShardMap,
};
use object_store::local::LocalFileSystem;

/// A multiple of both an hour and a minute, so rollup buckets line up.
const BASE: i64 = 1_699_999_200_000;

fn map(nodes: &[&str], shards: usize, replicas: usize) -> ShardMap {
    ShardMap::new(ClusterConfig::new(
        nodes.iter().map(|n| n.to_string()).collect(),
        shards,
        replicas,
    ))
    .unwrap()
}

fn store() -> ObjectStoreRef {
    Arc::new(LocalFileSystem::new())
}

async fn open_cluster(root: &temp_dir::TempDir, map: ShardMap) -> Cluster {
    let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());
    let runtimes = StorageRuntimes::new(rt.clone(), rt);
    Cluster::open(
        &root.path().to_string_lossy(),
        store(),
        StorageConfig::default(),
        runtimes,
        map,
    )
    .await
    .unwrap()
}

fn sample(device: usize, tick: i64) -> Sample {
    Sample {
        name: b"http_requests".to_vec(),
        labels: vec![
            Label {
                name: b"host".to_vec(),
                value: format!("host{device:03}").into_bytes(),
            },
            Label {
                name: b"code".to_vec(),
                value: b"200".to_vec(),
            },
        ],
        timestamp: BASE + tick * 10_000,
        value: (device as f64) + (tick as f64) / 100.0,
        name_id: None,
        series_id: None,
    }
}

fn query(start: i64, end: i64) -> SearchRequest {
    SearchRequest {
        metric_name: b"http_requests".to_vec(),
        matchers: Vec::new(),
        start_ms: start,
        end_ms: end,
        limit: 0,
    }
}

#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn cluster_survives_a_restart_and_a_node_join() {
    const DEVICES: usize = 90;
    const TICKS: i64 = 6;

    let root = temp_dir::TempDir::new().unwrap();
    let before = map(&["node-a", "node-b", "node-c"], 32, 2);
    let cluster = open_cluster(&root, before.clone()).await;

    let mut samples = Vec::new();
    for device in 0..DEVICES {
        for tick in 0..TICKS {
            samples.push(sample(device, tick));
        }
    }
    let report = cluster.write(samples).await.unwrap();
    assert_eq!(report.samples, DEVICES * TICKS as usize);
    assert_eq!(report.failed, 0, "no replica should have failed");
    // Every sample went to `replicas` owners.
    assert_eq!(report.writes, report.shards * before.replicas());
    cluster.flush_all().await.unwrap();

    let expected = DEVICES * TICKS as usize;
    let series = cluster
        .search(&query(BASE, BASE + TICKS * 10_000))
        .await
        .unwrap();
    assert_eq!(
        series.len(),
        DEVICES,
        "one entry per series, not per replica"
    );
    assert_eq!(
        series.iter().map(|s| s.samples.len()).sum::<usize>(),
        expected,
        "every sample is present exactly once"
    );
    drop(cluster);

    // --- restart with the same topology: nothing changes -------------------
    let cluster = open_cluster(&root, before.clone()).await;
    let series = cluster
        .search(&query(BASE, BASE + TICKS * 10_000))
        .await
        .unwrap();
    assert_eq!(series.len(), DEVICES);
    assert_eq!(
        series.iter().map(|s| s.samples.len()).sum::<usize>(),
        expected
    );
    drop(cluster);

    // --- a node joins while the cluster is down ----------------------------
    let after = map(&["node-a", "node-b", "node-c", "node-d"], 32, 2);
    let migrations = after.migrations(&before);
    assert!(
        !migrations.is_empty(),
        "the new node has to take over shards"
    );

    // Plan against the old topology: only the shards that actually changed
    // owner are copied.
    let copied = rebalance(
        store().as_ref(),
        &root.path().to_string_lossy(),
        &before,
        &after,
    )
    .await
    .unwrap();
    assert_eq!(copied.shards_moved, migrations.len());
    assert!(copied.objects > 0);

    // The placement is now complete, so a second repair copies nothing.
    let repaired = rebalance_from_disk(store().as_ref(), &root.path().to_string_lossy(), &after)
        .await
        .unwrap();
    assert_eq!(repaired.shards_moved, 0, "a repaired cluster is a no-op");
    assert_eq!(repaired.objects, 0);

    // --- reopen with the new topology -------------------------------------
    let cluster = open_cluster(&root, after.clone()).await;
    let series = cluster
        .search(&query(BASE, BASE + TICKS * 10_000))
        .await
        .unwrap();
    assert_eq!(series.len(), DEVICES, "no series lost in the rebalance");
    assert_eq!(
        series.iter().map(|s| s.samples.len()).sum::<usize>(),
        expected,
        "no samples lost in the rebalance"
    );

    // The latest-value path uses a different code path (last cache) and has to
    // agree with the range read.
    let latest = cluster
        .last_values(&query(BASE, BASE + TICKS * 10_000))
        .await
        .unwrap();
    assert_eq!(latest.len(), DEVICES);
    for series in &latest {
        assert_eq!(series.timestamp, BASE + (TICKS - 1) * 10_000);
    }
}

#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn cluster_repairs_a_replica_that_lost_its_directory() {
    const DEVICES: usize = 40;

    let root = temp_dir::TempDir::new().unwrap();
    let topo = map(&["node-a", "node-b"], 8, 2);
    let cluster = open_cluster(&root, topo.clone()).await;

    let samples: Vec<Sample> = (0..DEVICES).map(|device| sample(device, 0)).collect();
    cluster.write(samples).await.unwrap();
    cluster.flush_all().await.unwrap();
    drop(cluster);

    // node-b loses every shard directory (disk replaced, restore went to the
    // wrong prefix, ...).
    let b_root = root.path().join("node-b");
    let mut removed = 0;
    for entry in std::fs::read_dir(&b_root).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name().to_string_lossy().starts_with("shard-") {
            std::fs::remove_dir_all(entry.path()).unwrap();
            removed += 1;
        }
    }
    assert_eq!(removed, topo.shard_count(), "all shards of node-b are gone");

    // Reads still answer from the surviving replicas, but a shard with only
    // one copy left is exactly the state the repair exists for.
    let cluster = open_cluster(&root, topo.clone()).await;
    assert_eq!(
        cluster.search(&query(BASE, BASE + 1)).await.unwrap().len(),
        DEVICES,
        "the surviving replicas still answer"
    );
    drop(cluster);

    let repaired = rebalance_from_disk(store().as_ref(), &root.path().to_string_lossy(), &topo)
        .await
        .unwrap();
    assert_eq!(repaired.shards_moved, topo.shard_count());
    assert!(repaired.objects > 0);

    // Every shard now has both copies again, and each replica of a shard holds
    // exactly the series routed to it.
    let cluster = open_cluster(&root, topo.clone()).await;
    // Which devices the placement sends to which shard, so a shard can be held
    // against the routing table instead of against the whole cluster.
    let mut expected: std::collections::BTreeMap<usize, Vec<String>> = Default::default();
    for device in 0..DEVICES {
        let shard = shard_of(&topo, &sample(device, 0));
        expected
            .entry(shard)
            .or_default()
            .push(format!("host{device:03}"));
    }

    let whole = cluster.search(&query(BASE, BASE + 1)).await.unwrap();
    assert_eq!(
        whole.len(),
        DEVICES,
        "the cluster still answers all devices"
    );
    for shard in 0..topo.shard_count() {
        let engines = cluster.shard_engines_by_node(shard);
        assert_eq!(engines.len(), 2, "shard {shard} has two replicas");
        let want = expected.get(&shard).cloned().unwrap_or_default();
        for (node, engine) in &engines {
            let series = engine.search(&query(BASE, BASE + 1)).await.unwrap();
            let mut got: Vec<String> = series.iter().map(|s| hostname(s)).collect();
            got.sort();
            assert_eq!(
                got, want,
                "{node}/shard-{shard} does not hold the series the routing table assigns it"
            );
        }
        // The two replicas agree, which is the property a repair restores.
        let first = engines[0].1.search(&query(BASE, BASE + 1)).await.unwrap();
        let second = engines[1].1.search(&query(BASE, BASE + 1)).await.unwrap();
        assert_eq!(first.len(), second.len(), "shard {shard} replicas diverged");
    }
}

/// The routing table's view of which shard a sample belongs to, derived from
/// the same canonical series key the engine uses.
fn shard_of(topo: &ShardMap, sample: &Sample) -> usize {
    use metric_engine::{hash, types::SeriesId};
    let key = metric_engine::types::build_series_key(&sample.name, &sample.labels);
    let series = SeriesId(hash(&key));
    topo.shard_of(series.0)
}

fn hostname(series: &metric_engine::SeriesSamples) -> String {
    let labels: Vec<_> = series
        .labels
        .iter()
        .filter(|l| l.name.as_slice() == b"host")
        .collect();
    let label = labels.first().expect("series carries a host label");
    String::from_utf8_lossy(&label.value).into_owned()
}

/// Retention is applied to every shard engine, so one metric's window is the
/// same no matter which shard (and replica) its series landed on.
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn cluster_applies_per_metric_retention_to_every_shard() {
    let root = temp_dir::TempDir::new().unwrap();
    let topo = map(&["node-a", "node-b"], 8, 1);
    let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());
    let runtimes = StorageRuntimes::new(rt.clone(), rt);
    let policy = RetentionPolicy::from_ms([(b"http_requests".to_vec(), 60_000)]);
    let cluster = Cluster::open_with_retention(
        &root.path().to_string_lossy(),
        store(),
        StorageConfig::default(),
        runtimes,
        topo,
        policy,
    )
    .await
    .unwrap();

    let now = common::now();
    let mut samples = Vec::new();
    for device in 0..40 {
        for age_ms in [0i64, 30_000, 120_000, 600_000] {
            let mut s = sample(device, 0);
            s.timestamp = now - age_ms;
            samples.push(s);
        }
    }
    cluster.write(samples).await.unwrap();
    cluster.flush_all().await.unwrap();

    let series = cluster.search(&query(0, i64::MAX)).await.unwrap();
    assert_eq!(series.len(), 40);
    for series in &series {
        assert_eq!(
            series.samples.len(),
            2,
            "only the samples inside the 1m window are visible"
        );
        for point in &series.samples {
            assert!(
                point.timestamp >= now - 60_000,
                "an aged-out sample leaked through"
            );
        }
    }
}
