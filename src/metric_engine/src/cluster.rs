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

//! Static sharding, replication and rebalancing (roadmap Phase 4).
//!
//! A [`ShardMap`] turns a series id into one **primary shard** and a set of
//! replica **nodes**:
//!
//! - series → shard by jump consistent hash ([`jump_hash`]). It is stateless
//!   and moves only ~`1/n` of the series when the shard count changes, which is
//!   what makes a shard-count change cheap.
//! - shard → node by rendezvous hashing over the node names, keeping the
//!   `replicas` highest-scoring nodes. Adding or losing a node moves only the
//!   shards it owned.
//!
//! Data is laid out as `{root}/{node}/shard-{n}`, so a shard is one engine
//! directory and **moving a shard is copying one prefix** — the same
//! [`copy_prefix`](crate::backup::copy_prefix) the backup path uses, which is
//! why the rebalance works for a local store and an object store alike.
//!
//! Writes are replicated: every sample is written to all owners of its shard,
//! and a write only succeeds when a quorum of them accepted it. Reads query
//! every engine of the cluster and merge the result; since a shard owns a
//! disjoint set of series, merging never has to reconcile two different series,
//! only duplicate copies of the same one.
//!
//! What this deliberately is **not**: a consensus protocol. There is no leader
//! election, no log replication and no repair after a node comes back with
//! stale data. The model is "N nodes holding identical copies of a shard,
//! quorum writes, read-any"; a node that missed writes is caught up by the
//! operator re-copying the shard prefix (the rebalance primitive) or by
//! [`crate::MetricEngine::restore_from`].

use std::collections::{BTreeMap, HashMap, HashSet};

use columnar_storage::{config::StorageConfig, storage::StorageRuntimes, types::ObjectStoreRef};
use common::ReadableDuration;
use futures::{future::join_all, TryStreamExt};
use object_store::{path::Path as ObjectStorePath, ObjectStore};

use crate::{
    backup::copy_prefix,
    engine::{MetricEngine, MetricEngineRef},
    query::{Aggregation, SearchRequest, SeriesLatest, SeriesSamples},
    retention::RetentionPolicy,
    types::Sample,
    Result,
};

/// How a cluster is laid out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterConfig {
    /// Node names. They are also the directory names under the cluster root, so
    /// renaming a node is a data move.
    pub nodes: Vec<String>,
    /// Number of shards. Fixed once data is written: changing it re-routes
    /// series, and the data of the moved series has to be copied by hand.
    pub shards: usize,
    /// Copies of every shard, including the primary. `1` means no replication.
    pub replicas: usize,
}

impl ClusterConfig {
    pub fn new(nodes: Vec<String>, shards: usize, replicas: usize) -> Self {
        Self {
            nodes,
            shards,
            replicas,
        }
    }
}

/// The routing table: series → shard, shard → nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardMap {
    config: ClusterConfig,
}

impl ShardMap {
    /// Builds a routing table, rejecting a topology that cannot be satisfied.
    pub fn new(config: ClusterConfig) -> Result<Self> {
        if config.nodes.is_empty() {
            return Err(anyhow::anyhow!("a cluster needs at least one node").into());
        }
        if config.shards == 0 {
            return Err(anyhow::anyhow!("a cluster needs at least one shard").into());
        }
        if config.replicas == 0 || config.replicas > config.nodes.len() {
            return Err(anyhow::anyhow!(
                "replicas must be between 1 and the node count ({}), got {}",
                config.nodes.len(),
                config.replicas
            )
            .into());
        }
        let unique: HashSet<&String> = config.nodes.iter().collect();
        if unique.len() != config.nodes.len() {
            return Err(anyhow::anyhow!("node names must be unique").into());
        }
        Ok(Self { config })
    }

    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    pub fn nodes(&self) -> &[String] {
        &self.config.nodes
    }

    pub fn shard_count(&self) -> usize {
        self.config.shards
    }

    pub fn replicas(&self) -> usize {
        self.config.replicas
    }

    /// The primary shard of a series.
    pub fn shard_of(&self, series_id: u64) -> usize {
        jump_hash(series_id, self.config.shards as u64) as usize
    }

    /// The nodes that hold a copy of `shard`, highest rendezvous score first.
    pub fn owners(&self, shard: usize) -> Vec<&str> {
        let mut scored: Vec<(u64, &str)> = self
            .config
            .nodes
            .iter()
            .map(|node| (rendezvous_score(shard as u64, node), node.as_str()))
            .collect();
        // Sort by score, then by name so two equal scores still give one
        // deterministic order on every node of the cluster.
        scored.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(b.1)));
        scored.truncate(self.config.replicas);
        scored.into_iter().map(|(_, node)| node).collect()
    }

    /// The nodes a series is written to.
    pub fn owners_of_series(&self, series_id: u64) -> Vec<&str> {
        self.owners(self.shard_of(series_id))
    }

    /// The nodes that have to change for `self` to become the topology.
    ///
    /// A shard appears here only when its owner set actually changed, so the
    /// caller copies the minimum: with rendezvous hashing, joining a node moves
    /// only the shards the new node scores highest on.
    pub fn migrations(&self, previous: &ShardMap) -> Vec<Migration> {
        let mut out = Vec::new();
        if previous.shard_count() != self.shard_count() {
            // A shard-count change re-routes series, not shards, so there is no
            // shard to copy; the caller has to re-shard the data.
            return out;
        }
        for shard in 0..self.shard_count() {
            let before: HashSet<&str> = previous.owners(shard).into_iter().collect();
            let after: HashSet<&str> = self.owners(shard).into_iter().collect();
            let mut added: Vec<String> = after
                .difference(&before)
                .map(|n| (*n).to_string())
                .collect();
            let mut removed: Vec<String> = before
                .difference(&after)
                .map(|n| (*n).to_string())
                .collect();
            // Stable order so two nodes computing the same migration agree on
            // the copy order.
            added.sort();
            removed.sort();
            if added.is_empty() && removed.is_empty() {
                continue;
            }
            out.push(Migration {
                shard,
                added,
                removed,
            });
        }
        out.sort_by_key(|m| m.shard);
        out
    }
}

/// One shard changing owners.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Migration {
    pub shard: usize,
    /// Nodes that must receive a copy.
    pub added: Vec<String>,
    /// Nodes that no longer hold the shard and may drop it.
    pub removed: Vec<String>,
}

/// Jump consistent hash (Lamping & Veach, 2014).
///
/// Maps a 64-bit key to `[0, buckets)` in O(ln buckets) with no table, and
/// moves only about `1/buckets` of the keys when `buckets` grows. Deterministic
/// across processes and machines, which is the whole point: every node of the
/// cluster derives the same routing from the same config.
pub fn jump_hash(key: u64, buckets: u64) -> u64 {
    debug_assert!(buckets > 0);
    let mut key = key;
    let mut b = -1i64;
    let mut j = 0i64;
    while j < buckets as i64 {
        b = j;
        key = key.wrapping_mul(2_862_933_555_777_941_757).wrapping_add(1);
        let mut x = key;
        x ^= x >> 33;
        x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
        x ^= x >> 33;
        x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
        x ^= x >> 33;
        j = ((b as f64 + 1.0) * (1.0 / (((x >> 11) as f64 + 1.0) / (1u64 << 53) as f64))) as i64;
        // Guard against a float round-trip that fails to advance, which would
        // otherwise spin forever on a degenerate key.
        if j <= b {
            j = b + 1;
        }
    }
    b as u64
}

/// The rendezvous score of `node` for `shard`.
fn rendezvous_score(shard: u64, node: &str) -> u64 {
    let mut buf = Vec::with_capacity(8 + node.len());
    buf.extend_from_slice(&shard.to_le_bytes());
    buf.extend_from_slice(node.as_bytes());
    crate::hash(&buf)
}

/// What a replicated write did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteReport {
    /// Samples accepted by the caller.
    pub samples: usize,
    /// Engine write calls that succeeded. One call carries every sample of a
    /// `(node, shard)` pair, so this is at most `shards * replicas` and less
    /// when a replica is down.
    pub writes: usize,
    /// Distinct shards touched.
    pub shards: usize,
    /// Replica writes that failed; the write still succeeded if the shard kept
    /// its quorum. Non-zero means the cluster is degraded.
    pub failed: usize,
}

/// A cluster of engines, one per (node, shard) pair it serves.
pub struct Cluster {
    map: ShardMap,
    /// `(node, shard) -> engine`.
    engines: HashMap<(String, usize), MetricEngineRef>,
    /// The cluster root, so a rebalance can name the prefixes.
    root: String,
}

impl Cluster {
    /// Opens every engine the topology needs, under
    /// `{root}/{node}/shard-{shard}`, with no per-metric retention.
    pub async fn open(
        root: &str,
        store: ObjectStoreRef,
        storage_opts: StorageConfig,
        runtimes: StorageRuntimes,
        map: ShardMap,
    ) -> Result<Self> {
        Self::open_with_retention(
            root,
            store,
            storage_opts,
            runtimes,
            map,
            RetentionPolicy::default(),
        )
        .await
    }

    /// [`Cluster::open`] with a per-metric retention policy applied to every
    /// shard engine, so a reading stays consistent across the cluster.
    pub async fn open_with_retention(
        root: &str,
        store: ObjectStoreRef,
        storage_opts: StorageConfig,
        runtimes: StorageRuntimes,
        map: ShardMap,
        retention: RetentionPolicy,
    ) -> Result<Self> {
        let mut engines = HashMap::new();
        for node in map.nodes() {
            // A node hosts every shard it owns a copy of. Two nodes hosting the
            // same shard is the replication case; two shards on one node is the
            // packing case.
            let shards: HashSet<usize> = (0..map.shard_count())
                .filter(|shard| map.owners(*shard).iter().any(|n| *n == node))
                .collect();
            for shard in shards {
                let path = shard_path(root, node, shard);
                let engine = MetricEngine::try_open(
                    &path,
                    ReadableDuration::hours(2).0,
                    store.clone(),
                    storage_opts.clone(),
                    runtimes.clone(),
                )
                .await?;
                engines.insert(
                    (node.clone(), shard),
                    std::sync::Arc::new(engine.with_retention(retention.clone())),
                );
            }
        }
        Ok(Self {
            map,
            engines,
            root: root.to_string(),
        })
    }

    pub fn map(&self) -> &ShardMap {
        &self.map
    }

    pub fn root(&self) -> &str {
        &self.root
    }

    /// Writes every sample to all owners of its shard.
    ///
    /// A shard succeeds when a quorum of its owners accepted its slice, so a
    /// single lost node costs availability only for the shards that had no
    /// spare copy. A degraded write (quorum reached, some replica failed) is
    /// reported through [`WriteReport::failed`] rather than hidden.
    pub async fn write(&self, samples: Vec<Sample>) -> Result<WriteReport> {
        if samples.is_empty() {
            return Ok(WriteReport {
                samples: 0,
                writes: 0,
                shards: 0,
                failed: 0,
            });
        }
        // Group by (node, shard): one engine call per pair, not per sample.
        let mut grouped: BTreeMap<(String, usize), Vec<Sample>> = BTreeMap::new();
        for sample in samples.iter() {
            let shard = self.map.shard_of(sample.series_id().0);
            for node in self.map.owners(shard) {
                grouped
                    .entry((node.to_string(), shard))
                    .or_default()
                    .push(sample.clone());
            }
        }

        let shards: HashSet<usize> = grouped.keys().map(|(_, shard)| *shard).collect();
        let quorum = self.map.replicas() / 2 + 1;
        let calls = grouped.into_iter().map(|((node, shard), batch)| {
            let engine = self.engines.get(&(node.clone(), shard)).cloned();
            async move {
                let result = match engine {
                    Some(engine) => engine
                        .write(batch)
                        .await
                        .map_err(|e| format!("{node}/shard-{shard}: {e}")),
                    None => Err(format!("{node}/shard-{shard}: no engine (node is down)")),
                };
                (shard, result)
            }
        });
        let outcomes = join_all(calls).await;

        // Per-shard quorum, and the failures that did not sink the write.
        let mut acked: HashMap<usize, usize> = HashMap::new();
        let mut writes = 0usize;
        let mut failed = 0usize;
        let mut fatal: Vec<String> = Vec::new();
        for (shard, result) in outcomes {
            match result {
                Ok(()) => {
                    *acked.entry(shard).or_default() += 1;
                    writes += 1;
                }
                Err(e) => {
                    failed += 1;
                    fatal.push(e);
                }
            }
        }
        let mut short: Vec<String> = Vec::new();
        for shard in &shards {
            let got = acked.get(shard).copied().unwrap_or(0);
            if got < quorum {
                short.push(format!("shard-{shard} got {got}/{quorum} acks"));
            }
        }
        if !short.is_empty() {
            return Err(anyhow::anyhow!(
                "write below quorum: {} (failures: {})",
                short.join(", "),
                fatal.join("; ")
            )
            .into());
        }
        Ok(WriteReport {
            samples: samples.len(),
            // Only what actually landed: a degraded write reports fewer
            // replica writes than the topology would suggest.
            writes,
            shards: shards.len(),
            failed,
        })
    }

    /// Queries every engine and merges the answers.
    ///
    /// A shard owns a disjoint set of series, so a series is normally found in
    /// exactly one shard (in each of its replicas). Duplicate copies are merged
    /// point by point; when two copies disagree on a timestamp the value from
    /// the lexicographically first node wins, so a repeated query returns the
    /// same answer instead of "whichever replica answered last".
    pub async fn search(&self, req: &SearchRequest) -> Result<Vec<SeriesSamples>> {
        let mut answers = Vec::new();
        for (key, engine) in self.sorted_engines() {
            answers.push(
                engine
                    .search(req)
                    .await
                    .map_err(|e| anyhow::anyhow!("query {key}: {e}"))?,
            );
        }
        Ok(merge_series(answers.into_iter()))
    }

    /// Latest value per series, merged across the cluster.
    pub async fn last_values(&self, req: &SearchRequest) -> Result<Vec<SeriesLatest>> {
        let mut by_series: BTreeMap<SeriesKey, SeriesLatest> = BTreeMap::new();
        for (key, engine) in self.sorted_engines() {
            let latest = engine
                .last_values(req)
                .await
                .map_err(|e| anyhow::anyhow!("latest {key}: {e}"))?;
            for series in latest {
                let id = (series.metric_name.clone(), labels_key(&series.labels));
                match by_series.get(&id) {
                    Some(existing) if existing.timestamp >= series.timestamp => {}
                    _ => {
                        by_series.insert(id, series);
                    }
                }
            }
        }
        Ok(by_series.into_values().collect())
    }

    /// Downsampled query, merged across the cluster.
    pub async fn search_downsampled(
        &self,
        req: &SearchRequest,
        step_ms: i64,
        agg: Aggregation,
    ) -> Result<Vec<SeriesSamples>> {
        let mut answers = Vec::new();
        for (key, engine) in self.sorted_engines() {
            answers.push(
                engine
                    .search_downsampled(req, step_ms, agg)
                    .await
                    .map_err(|e| anyhow::anyhow!("downsample {key}: {e}"))?,
            );
        }
        Ok(merge_series(answers.into_iter()))
    }

    /// Flushes every engine, so a following backup or rebalance sees a durable
    /// point.
    pub async fn flush_all(&self) -> Result<()> {
        for (key, engine) in self.sorted_engines() {
            engine
                .flush_all()
                .await
                .map_err(|e| anyhow::anyhow!("flush {key}: {e}"))?;
        }
        Ok(())
    }

    /// Triggers compaction on every engine.
    pub async fn compact(&self) -> Result<()> {
        for (key, engine) in self.sorted_engines() {
            engine
                .compact()
                .await
                .map_err(|e| anyhow::anyhow!("compact {key}: {e}"))?;
        }
        Ok(())
    }

    /// `(node, shard)` engines in a fixed order, so a merged answer is
    /// reproducible.
    fn sorted_engines(&self) -> Vec<(String, &MetricEngineRef)> {
        let mut engines: Vec<((String, usize), &MetricEngineRef)> = self
            .engines
            .iter()
            .map(|(key, engine)| (key.clone(), engine))
            .collect();
        engines.sort_by(|a, b| a.0.cmp(&b.0));
        engines
            .into_iter()
            .map(|((node, shard), engine)| (format!("{node}/shard-{shard}"), engine))
            .collect()
    }

    /// The engines serving `shard`, keyed by node.
    pub fn shard_engines_by_node(&self, shard: usize) -> Vec<(String, MetricEngineRef)> {
        self.map
            .owners(shard)
            .into_iter()
            .filter_map(|node| {
                self.engines
                    .get(&(node.to_string(), shard))
                    .map(|engine| (node.to_string(), std::sync::Arc::clone(engine)))
            })
            .collect()
    }
}

/// What a rebalance copied.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RebalanceReport {
    pub shards_moved: usize,
    pub objects: u64,
    pub bytes: u64,
}

/// Copies the shards that changed owners under `to`.
///
/// Run with the cluster quiesced: the copy has to see a stable object set, and
/// engines opened at the source prefixes would otherwise keep writing while
/// the bytes are read. The source is a node that keeps the shard, so the copy
/// never depends on the node that is leaving.
pub async fn rebalance(
    store: &dyn ObjectStore,
    root: &str,
    from: &ShardMap,
    to: &ShardMap,
) -> Result<RebalanceReport> {
    if from.shard_count() != to.shard_count() {
        return Err(anyhow::anyhow!(
            "rebalance needs the same shard count ({} -> {}): changing it re-routes \
             series and has to be done by rewriting the data",
            from.shard_count(),
            to.shard_count()
        )
        .into());
    }
    let mut report = RebalanceReport::default();
    for migration in to.migrations(from) {
        // Prefer a node that keeps the shard as the source; falling back to a
        // leaving node still works as long as it is still up.
        let source = to
            .owners(migration.shard)
            .into_iter()
            .find(|node| !migration.added.iter().any(|added| added == node))
            .map(|node| node.to_string())
            .or_else(|| migration.removed.first().cloned())
            .ok_or_else(|| {
                anyhow::anyhow!("shard-{} has no source node to copy from", migration.shard)
            })?;
        for target in &migration.added {
            let stats = copy_prefix(
                store,
                &shard_path(root, &source, migration.shard),
                store,
                &shard_path(root, target, migration.shard),
            )
            .await?;
            report.objects += stats.objects;
            report.bytes += stats.bytes;
            report.shards_moved += 1;
        }
    }
    Ok(report)
}

/// Copies the shards a node set is missing, discovering them from the store.
///
/// [`rebalance`] needs the previous topology, which a restarted process no
/// longer has. This variant instead compares the layout on disk with `to`: a
/// shard whose owner set has an empty prefix on some replica is copied from a
/// replica that has data. That covers both a node joining (its prefixes are
/// empty, so every shard it now owns is copied) and a replica that fell behind
/// or lost a directory (it is refilled from a healthy copy).
///
/// An empty shard prefix is indistinguishable from a shard that never received
/// data, so this only ever copies; it never deletes. Dropping data from a node
/// that no longer owns a shard is a separate, explicit operation.
pub async fn rebalance_from_disk(
    store: &dyn ObjectStore,
    root: &str,
    to: &ShardMap,
) -> Result<RebalanceReport> {
    // shard -> nodes whose prefix already holds objects.
    let mut populated: HashMap<usize, Vec<String>> = HashMap::new();
    for node in to.nodes() {
        let prefix = format!("{}/{node}", root.trim_end_matches('/'));
        let mut stream = store.list(Some(&ObjectStorePath::from(prefix.as_str())));
        while let Some(meta) = stream
            .try_next()
            .await
            .map_err(|e| anyhow::anyhow!("list {prefix}: {e}"))?
        {
            // `{root}/{node}/shard-{n}/...`: the shard directory is the first
            // component below the node prefix. Strip the prefix textually so a
            // multi-component root (`/data/horae`, `s3://bkt/db`) works too.
            let location = meta.location.to_string();
            let Some(rest) = strip_location_prefix(&location, &prefix) else {
                continue;
            };
            let Some(shard) = rest
                .split('/')
                .next()
                .and_then(|name| name.strip_prefix("shard-"))
                .and_then(|n| n.parse::<usize>().ok())
            else {
                continue;
            };
            let nodes = populated.entry(shard).or_default();
            if !nodes.iter().any(|n| n == node) {
                nodes.push(node.clone());
            }
        }
    }

    let mut report = RebalanceReport::default();
    for shard in 0..to.shard_count() {
        let owners: Vec<String> = to.owners(shard).into_iter().map(str::to_string).collect();
        let have = populated.get(&shard).cloned().unwrap_or_default();
        // Prefer a node that keeps the shard, so the copy never depends on the
        // node that is leaving. With `replicas = 1` a new node *replaces* the
        // old owner, so there is no owner left to read from and the leaving
        // node is the only source.
        let source = owners
            .iter()
            .find(|node| have.iter().any(|h| h == *node))
            .or_else(|| have.first())
            .cloned();
        let Some(source) = source else {
            // No node holds the shard: either it never received data or every
            // copy is gone. Nothing to copy from.
            continue;
        };
        for target in owners
            .iter()
            .filter(|node| !have.iter().any(|h| h == *node))
        {
            let stats = copy_prefix(
                store,
                &shard_path(root, &source, shard),
                store,
                &shard_path(root, target, shard),
            )
            .await?;
            if stats.objects > 0 {
                report.shards_moved += 1;
                report.objects += stats.objects;
                report.bytes += stats.bytes;
            }
        }
    }
    Ok(report)
}

/// Strips `prefix` from a listed object location.
///
/// `object_store::path::Path::to_string` drops a leading `/`, so a local
/// absolute root lists as `tmp/db/...` while the configured prefix is
/// `/tmp/db`. Normalising the leading separators on both sides keeps the local
/// and object-store cases identical.
fn strip_location_prefix<'a>(location: &'a str, prefix: &str) -> Option<&'a str> {
    let location = location.trim_start_matches('/');
    let prefix = prefix.trim_start_matches('/');
    location
        .strip_prefix(prefix)?
        .strip_prefix('/')
        .or(Some(""))
        .filter(|rest| !rest.is_empty())
}

/// The engine directory of one (node, shard) pair.
///
/// A shard is just an engine root, so "move a shard" is "copy one prefix"
/// (see [`rebalance`]). The path is plain string joining, not
/// [`ObjectPath`](object_store::path::Path): the latter strips a leading `/`,
/// which would silently turn an absolute local path into a relative one.
pub fn shard_path(root: &str, node: &str, shard: usize) -> String {
    format!("{}/{node}/shard-{shard}", root.trim_end_matches('/'))
}

/// The identity of one series inside the cluster: `(metric name, sorted
/// labels)`. Used to key the merge of per-engine answers.
type SeriesKey = (Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>);

fn labels_key(labels: &[crate::types::Label]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out: Vec<(Vec<u8>, Vec<u8>)> = labels
        .iter()
        .map(|label| (label.name.clone(), label.value.clone()))
        .collect();
    out.sort();
    out
}

/// Merges per-engine answers into one series list.
///
/// A shard owns a disjoint set of series, so two engines only ever return the
/// same series when they hold replicas of it. The points are therefore unioned
/// by timestamp; a replicated point is identical on both copies, so keeping one
/// is enough. The result is ordered by `(metric name, labels)` and every series
/// keeps the labels it was resolved with.
fn merge_series(answers: impl Iterator<Item = Vec<SeriesSamples>>) -> Vec<SeriesSamples> {
    let mut by_series: BTreeMap<SeriesKey, SeriesSamples> = BTreeMap::new();
    for series in answers.flatten() {
        let key = (series.metric_name.clone(), labels_key(&series.labels));
        match by_series.get_mut(&key) {
            Some(existing) => existing.samples.extend(series.samples),
            None => {
                by_series.insert(key, series);
            }
        }
    }
    by_series
        .into_values()
        .map(|mut series| {
            series.samples.sort_by_key(|point| point.timestamp);
            // Two replicas hold the same points; keep one of each timestamp.
            series.samples.dedup_by_key(|point| point.timestamp);
            series
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use columnar_storage::{config::StorageConfig, storage::StorageRuntimes};
    use object_store::local::LocalFileSystem;

    use super::*;
    use crate::{Label, SearchRequest};

    fn map(nodes: &[&str], shards: usize, replicas: usize) -> ShardMap {
        ShardMap::new(ClusterConfig::new(
            nodes.iter().map(|n| n.to_string()).collect(),
            shards,
            replicas,
        ))
        .unwrap()
    }

    #[test]
    fn test_shard_map_is_deterministic_and_balanced() {
        let map = map(&["a", "b", "c"], 16, 2);
        let mut counts = [0usize; 16];
        for series in 0..160_000u64 {
            let shard = map.shard_of(series);
            assert_eq!(shard, map.shard_of(series), "routing must be stable");
            counts[shard] += 1;
        }
        assert_eq!(counts.iter().sum::<usize>(), 160_000);
        // Jump hash spreads keys evenly; a 16-way split of 160k keys should be
        // within 25 % of 10k each.
        for (shard, count) in counts.iter().enumerate() {
            assert!(
                (7_500..=12_500).contains(count),
                "shard {shard} got {count} series, expected ~10000"
            );
        }
    }

    #[test]
    fn test_replicas_are_distinct_and_ordered() {
        let map = map(&["a", "b", "c"], 8, 2);
        for shard in 0..8 {
            let owners = map.owners(shard);
            assert_eq!(owners.len(), 2);
            assert_ne!(owners[0], owners[1]);
            // Same call, same order: every node derives the same placement.
            assert_eq!(owners, map.owners(shard));
        }
    }

    #[test]
    fn test_joining_a_node_moves_only_the_shards_it_owns() {
        let before = map(&["a", "b", "c"], 120, 1);
        let after = map(&["a", "b", "c", "d"], 120, 1);
        let migrations = after.migrations(&before);
        // Rendezvous hashing gives the new node its fair share (1/4 of the
        // shards) and takes it from the existing ones; it must not move more.
        assert!(
            (20..=45).contains(&migrations.len()),
            "expected ~30 of 120 shards to move, got {}",
            migrations.len()
        );
        for migration in &migrations {
            assert_eq!(migration.added, vec!["d".to_string()]);
            assert_eq!(migration.removed.len(), 1, "one owner is replaced");
        }
        // Removing the new node again moves the same shards back.
        assert_eq!(before.migrations(&after).len(), migrations.len());
    }

    #[test]
    fn test_shard_count_change_is_not_a_shard_migration() {
        let before = map(&["a", "b"], 4, 1);
        let after = map(&["a", "b"], 8, 1);
        assert!(
            after.migrations(&before).is_empty(),
            "a shard-count change re-routes series and has no shard to copy"
        );
    }

    #[test]
    fn test_invalid_topologies_are_rejected() {
        let err = ShardMap::new(ClusterConfig::new(Vec::new(), 4, 1)).unwrap_err();
        assert!(err.to_string().contains("at least one node"));
        let err = ShardMap::new(ClusterConfig::new(vec!["a".into()], 0, 1)).unwrap_err();
        assert!(err.to_string().contains("at least one shard"));
        let err =
            ShardMap::new(ClusterConfig::new(vec!["a".into(), "b".into()], 4, 3)).unwrap_err();
        assert!(err.to_string().contains("replicas must be between"));
        let err =
            ShardMap::new(ClusterConfig::new(vec!["a".into(), "a".into()], 4, 1)).unwrap_err();
        assert!(err.to_string().contains("unique"));
    }

    fn sample(name: &str, job: &str, ts: i64, value: f64) -> Sample {
        Sample {
            name: name.as_bytes().to_vec(),
            labels: vec![Label {
                name: b"job".to_vec(),
                value: job.as_bytes().to_vec(),
            }],
            timestamp: ts,
            value,
            name_id: None,
            series_id: None,
        }
    }

    fn query(name: &str, start: i64, end: i64) -> SearchRequest {
        SearchRequest {
            metric_name: name.as_bytes().to_vec(),
            matchers: Vec::new(),
            start_ms: start,
            end_ms: end,
            limit: 0,
        }
    }

    const BASE: i64 = 1_699_999_200_000;

    async fn open_cluster(root: &temp_dir::TempDir, map: ShardMap) -> Cluster {
        let store: ObjectStoreRef = Arc::new(LocalFileSystem::new());
        let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());
        let runtimes = StorageRuntimes::new(rt.clone(), rt);
        Cluster::open(
            &root.path().to_string_lossy(),
            store,
            StorageConfig::default(),
            runtimes,
            map,
        )
        .await
        .unwrap()
    }

    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn test_cluster_replicates_writes_to_every_owner() {
        let root = temp_dir::TempDir::new().unwrap();
        let map = map(&["a", "b"], 4, 2);
        let cluster = open_cluster(&root, map.clone()).await;

        let samples: Vec<Sample> = (0..40)
            .map(|i| sample("cpu", &format!("job{i}"), BASE + i * 1_000, i as f64))
            .collect();
        let report = cluster.write(samples.clone()).await.unwrap();
        assert_eq!(report.samples, 40);
        assert_eq!(report.failed, 0, "no replica should have failed");
        assert_eq!(
            report.writes,
            report.shards * map.replicas(),
            "one engine write call per (shard, owner)"
        );

        // Every owner of a shard holds exactly the series routed to it.
        for shard in 0..map.shard_count() {
            let engines = cluster.shard_engines_by_node(shard);
            assert_eq!(engines.len(), 2, "two replicas of shard {shard}");
            let expected: Vec<&Sample> = samples
                .iter()
                .filter(|s| map.shard_of(s.series_id().0) == shard)
                .collect();
            for (node, engine) in engines {
                let got = engine
                    .search(&query("cpu", BASE, BASE + 1_000_000))
                    .await
                    .unwrap();
                assert_eq!(
                    got.len(),
                    expected.len(),
                    "{node}/shard-{shard} has the wrong series count"
                );
            }
        }
    }

    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn test_cluster_read_merges_across_shards_without_duplicates() {
        let root = temp_dir::TempDir::new().unwrap();
        let cluster = open_cluster(&root, map(&["a", "b", "c"], 8, 2)).await;

        let mut samples = Vec::new();
        for device in 0..60 {
            for tick in 0..5 {
                samples.push(sample(
                    "cpu",
                    &format!("job{device}"),
                    BASE + tick * 10_000,
                    device as f64 + tick as f64,
                ));
            }
        }
        cluster.write(samples).await.unwrap();
        cluster.flush_all().await.unwrap();

        let series = cluster
            .search(&query("cpu", BASE, BASE + 100_000))
            .await
            .unwrap();
        assert_eq!(series.len(), 60, "every series once, despite replication");
        for series in &series {
            assert_eq!(series.samples.len(), 5, "replica points are deduplicated");
            for pair in series.samples.windows(2) {
                assert!(
                    pair[0].timestamp < pair[1].timestamp,
                    "points stay sorted by timestamp"
                );
            }
        }

        // The newest value per series also merges without duplicates.
        let latest = cluster
            .last_values(&query("cpu", BASE, BASE + 100_000))
            .await
            .unwrap();
        assert_eq!(latest.len(), 60);
        assert!(latest.iter().all(|s| s.timestamp == BASE + 40_000));

        // The rollup-backed path sees the same series set.
        let downsampled = cluster
            .search_downsampled(
                &query("cpu", BASE, BASE + 100_000),
                60_000,
                Aggregation::Avg,
            )
            .await
            .unwrap();
        assert_eq!(downsampled.len(), 60);
    }

    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn test_write_without_quorum_fails() {
        let root = temp_dir::TempDir::new().unwrap();
        // Three replicas: a quorum of two, and only two engines exist, so a
        // write cannot be acknowledged.
        let map = map(&["a", "b"], 4, 2);
        let cluster = open_cluster(&root, map).await;
        // Drop one replica of every shard by constructing a map that expects a
        // third node the cluster does not have.
        let degraded = Cluster {
            map: map_with_missing_node(),
            engines: cluster.engines,
            root: cluster.root,
        };
        let err = degraded
            .write(vec![sample("cpu", "job0", BASE, 1.0)])
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("below quorum"),
            "unexpected error: {err}"
        );
    }

    /// A two-node map where one node has no engine: shard quorum is 2, acks 1.
    fn map_with_missing_node() -> ShardMap {
        ShardMap::new(ClusterConfig::new(vec!["a".into(), "missing".into()], 1, 2)).unwrap()
    }

    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn test_rebalance_copies_the_moved_shards() {
        let root = temp_dir::TempDir::new().unwrap();
        let before = map(&["a", "b", "c"], 12, 1);
        let cluster = open_cluster(&root, before.clone()).await;

        let mut samples = Vec::new();
        for device in 0..80 {
            samples.push(sample("cpu", &format!("job{device}"), BASE, device as f64));
        }
        cluster.write(samples).await.unwrap();
        cluster.flush_all().await.unwrap();
        let all = cluster.search(&query("cpu", BASE, BASE + 1)).await.unwrap();
        assert_eq!(all.len(), 80);
        drop(cluster);

        let after = map(&["a", "b", "c", "d"], 12, 1);
        let store: ObjectStoreRef = Arc::new(LocalFileSystem::new());
        let report = rebalance(
            store.as_ref(),
            &root.path().to_string_lossy(),
            &before,
            &after,
        )
        .await
        .unwrap();
        assert_eq!(
            report.shards_moved,
            after.migrations(&before).len(),
            "one copy per shard that changed owner"
        );
        assert!(report.objects > 0);

        // The new topology reads everything: the moved shards came with their
        // data.
        let cluster = open_cluster(&root, after.clone()).await;
        let series = cluster.search(&query("cpu", BASE, BASE + 1)).await.unwrap();
        assert_eq!(series.len(), 80, "no series lost in the rebalance");
        for (node, engine) in cluster
            .engines
            .iter()
            .map(|((node, _), engine)| (node.clone(), engine))
        {
            if node == "d" {
                let got = engine.search(&query("cpu", BASE, BASE + 1)).await.unwrap();
                assert!(!got.is_empty(), "the new node received data");
            }
        }
    }

    /// A restarted process has no previous topology, so the rebalance has to
    /// discover the gaps from the store itself. This is the "node joined while
    /// we were down" case: its prefixes are empty, every shard it owns is
    /// copied from a replica that has data.
    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn test_rebalance_from_disk_fills_the_new_node() {
        let root = temp_dir::TempDir::new().unwrap();
        let before = map(&["a", "b"], 24, 1);
        let cluster = open_cluster(&root, before.clone()).await;
        let mut samples = Vec::new();
        for device in 0..60 {
            samples.push(sample("cpu", &format!("job{device}"), BASE, device as f64));
        }
        cluster.write(samples).await.unwrap();
        cluster.flush_all().await.unwrap();
        assert_eq!(
            cluster
                .search(&query("cpu", BASE, BASE + 1))
                .await
                .unwrap()
                .len(),
            60
        );
        drop(cluster);

        // A third node joins; it has no directory at all yet.
        let after = map(&["a", "b", "c"], 24, 1);
        let gained_by_c = (0..after.shard_count())
            .filter(|shard| after.owners(*shard).contains(&"c"))
            .count();
        assert!(gained_by_c > 0, "the new node has to own something");

        let store: ObjectStoreRef = Arc::new(LocalFileSystem::new());
        let report = rebalance_from_disk(store.as_ref(), &root.path().to_string_lossy(), &after)
            .await
            .unwrap();
        assert_eq!(
            report.shards_moved, gained_by_c,
            "one copy per shard the new node gained"
        );
        assert!(report.objects > 0);

        // A second run is a no-op: every owner now has its prefix.
        let again = rebalance_from_disk(store.as_ref(), &root.path().to_string_lossy(), &after)
            .await
            .unwrap();
        assert_eq!(again.shards_moved, 0);
        assert_eq!(again.objects, 0);

        let cluster = open_cluster(&root, after).await;
        assert_eq!(
            cluster
                .search(&query("cpu", BASE, BASE + 1))
                .await
                .unwrap()
                .len(),
            60,
            "no series lost"
        );
    }

    /// A replica whose directory was lost is refilled from a healthy copy.
    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn test_rebalance_from_disk_refills_a_missing_replica() {
        let root = temp_dir::TempDir::new().unwrap();
        let topo = map(&["a", "b"], 4, 2);
        let cluster = open_cluster(&root, topo.clone()).await;
        let mut samples = Vec::new();
        for device in 0..40 {
            samples.push(sample("cpu", &format!("job{device}"), BASE, device as f64));
        }
        cluster.write(samples).await.unwrap();
        cluster.flush_all().await.unwrap();
        drop(cluster);

        // Wipe every shard directory of node `b`.
        let b_root = root.path().join("b");
        for entry in std::fs::read_dir(&b_root).unwrap() {
            let entry = entry.unwrap();
            if entry.file_name().to_string_lossy().starts_with("shard-") {
                std::fs::remove_dir_all(entry.path()).unwrap();
            }
        }

        let store: ObjectStoreRef = Arc::new(LocalFileSystem::new());
        let report = rebalance_from_disk(store.as_ref(), &root.path().to_string_lossy(), &topo)
            .await
            .unwrap();
        assert_eq!(
            report.shards_moved,
            topo.shard_count(),
            "every replica via `b`"
        );

        let cluster = open_cluster(&root, topo).await;
        assert_eq!(
            cluster
                .search(&query("cpu", BASE, BASE + 1))
                .await
                .unwrap()
                .len(),
            40
        );
        for (_, engine) in cluster.shard_engines_by_node(0) {
            assert!(!engine
                .search(&query("cpu", BASE, BASE + 1))
                .await
                .unwrap()
                .is_empty());
        }
    }

    #[test]
    fn test_rebalance_rejects_a_shard_count_change() {
        let before = map(&["a"], 4, 1);
        let after = map(&["a"], 8, 1);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: ObjectStoreRef = Arc::new(LocalFileSystem::new());
        let err = rt
            .block_on(rebalance(store.as_ref(), "/tmp/x", &before, &after))
            .unwrap_err();
        assert!(err.to_string().contains("same shard count"));
    }

    #[test]
    fn test_shard_path_is_stable() {
        assert_eq!(shard_path("root", "node-a", 3), "root/node-a/shard-3");
        assert_eq!(shard_path("/tmp/root", "b", 0), "/tmp/root/b/shard-0");
    }
}
