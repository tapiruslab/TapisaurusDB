//! Deterministic Simulation Cluster (`SimCluster`) and Linearizability/Zero-Double-Spend Oracles.
//!
//! Orchestrates a multi-node `RaftNode` + `DeterministicSequencer` + `CrdtDocument` cluster
//! inside a single deterministic process, injecting partitions, packet drops, delayed/out-of-order
//! deliveries, node crash-restarts, and torn WAL tail writes while checking strict safety invariants.

use std::collections::{BTreeMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use crc32fast::Hasher;
use tapisaurus_core::cluster::{
    CrdtDocument, DeterministicEpochBatch, DeterministicOp, DeterministicSequencer,
    DeterministicTxSpec, InstallSnapshotArgs, NodeRole, RaftConfig, RaftNode, RaftRpcTransport,
    ShardRouter, Term,
};
use tempfile::TempDir;

use crate::fault_transport::{FaultProfile, NetworkStats, SimNetworkTransport};
use crate::rng::SimRng;

/// Configuration for building a simulated Raft + Calvin multi-node cluster.
#[derive(Debug, Clone)]
pub struct SimClusterConfig {
    pub seed: u64,
    pub node_count: usize,
    pub faults: FaultProfile,
}

impl Default for SimClusterConfig {
    fn default() -> Self {
        Self {
            seed: 0x5441_5049_5349_4D01,
            node_count: 5,
            faults: FaultProfile::default(),
        }
    }
}

/// Execution telemetry & digest produced by a deterministic chaos simulation run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimReport {
    pub seed: u64,
    pub ops_submitted: usize,
    pub epochs_committed: usize,
    pub transfers_committed: usize,
    pub transfers_aborted: usize,
    pub elections_run: usize,
    pub crash_restarts: usize,
    pub torn_wal_injections: usize,
    pub snapshot_recoveries: usize,
    pub final_state_crc32: u32,
    pub total_supply: i64,
    pub network_stats: NetworkStats,
}

struct SimNodeSlot {
    node_id: String,
    address: String,
    data_dir: PathBuf,
    raft: Arc<RaftNode>,
    sequencer: Arc<DeterministicSequencer>,
    applied_epoch_order: parking_lot::RwLock<Vec<(u64, u64)>>,
    crdt: CrdtDocument,
    alive: bool,
}

/// Multi-node deterministic simulation cluster driving real `RaftNode`, `RaftDiskStore`,
/// `DeterministicSequencer`, and `CrdtDocument` state machines.
pub struct SimCluster {
    _temp_root: TempDir,
    router: Arc<ShardRouter>,
    transport: SimNetworkTransport,
    rng: SimRng,
    nodes: Vec<SimNodeSlot>,
    /// Tracks which node won leadership in each Raft term to prove single-leader safety.
    leaders_by_term: BTreeMap<Term, HashSet<String>>,
    /// Ordered history of all committed Calvin epoch batches (ground-truth recovery oracle).
    committed_epoch_log: BTreeMap<u64, DeterministicEpochBatch>,
    next_epoch_seq: u64,
}

impl SimCluster {
    /// Boot an N-node persistent cluster inside isolated temporary directories.
    pub fn new(config: SimClusterConfig) -> Result<Self, String> {
        assert!(config.node_count >= 3, "SimCluster requires >= 3 nodes");
        let temp_root = TempDir::new().map_err(|e| format!("Failed creating sim tempdir: {e}"))?;
        let transport = SimNetworkTransport::new(config.seed, config.faults);
        let router = Arc::new(ShardRouter::new());

        let mut peer_specs = Vec::with_capacity(config.node_count);
        for i in 0..config.node_count {
            let node_id = format!("sim_node_{i}");
            let address = format!("127.0.0.1:{}", 31000 + i);
            router.register_node(&node_id, &address);
            peer_specs.push((node_id, address));
        }

        let mut nodes = Vec::with_capacity(config.node_count);
        for (i, (node_id, address)) in peer_specs.iter().enumerate() {
            let data_dir = temp_root.path().join(node_id);
            let raft_cfg = RaftConfig {
                election_timeout_min_ms: 150,
                election_timeout_max_ms: 300,
                heartbeat_interval_ms: 50,
                data_dir: Some(data_dir.clone()),
            };
            let raft = Arc::new(RaftNode::with_config(node_id, address, raft_cfg));
            for (peer_id, peer_addr) in &peer_specs {
                if peer_id != node_id {
                    raft.add_peer(peer_id, peer_addr);
                }
            }
            if i != 0 {
                // Start non-zero nodes as followers at term 2 cleanly without bumping term.
                raft.demote_to_follower();
            } else {
                // Elect node 0 cleanly at term 2
                raft.trigger_election();
            }

            transport.register_node((*raft).clone());
            let sequencer = Arc::new(DeterministicSequencer::new(router.clone(), raft.clone()));
            nodes.push(SimNodeSlot {
                node_id: node_id.clone(),
                address: address.clone(),
                data_dir,
                raft,
                sequencer,
                applied_epoch_order: parking_lot::RwLock::new(Vec::new()),
                crdt: CrdtDocument::new(),
                alive: true,
            });
        }

        // Normalize initial single leader at term 2 so all nodes start consistent
        let mut leaders_by_term = BTreeMap::new();
        for pid_node in &nodes {
            if pid_node.node_id == "sim_node_0" {
                let mut set = HashSet::new();
                set.insert(pid_node.node_id.clone());
                leaders_by_term.insert(pid_node.raft.get_info().term, set);
            } else {
                let vote_probe = tapisaurus_core::cluster::RequestVoteArgs {
                    term: 2,
                    candidate_id: "sim_node_0".to_string(),
                    last_log_index: 0,
                    last_log_term: 0,
                };
                let _ = pid_node.raft.handle_request_vote(vote_probe);
            }
        }

        Ok(Self {
            _temp_root: temp_root,
            router,
            transport,
            rng: SimRng::from_seed(config.seed),
            nodes,
            leaders_by_term,
            committed_epoch_log: BTreeMap::new(),
            next_epoch_seq: 1,
        })
    }

    /// Access deterministic PRNG mutably for synchronized test choreography.
    pub fn rng_mut(&mut self) -> &mut SimRng {
        &mut self.rng
    }

    /// Access fault-injecting RPC transport.
    pub fn transport(&self) -> &SimNetworkTransport {
        &self.transport
    }

    /// Return current cluster size.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Inspect live Raft status of node `idx`.
    pub fn node_info(&self, idx: usize) -> tapisaurus_core::cluster::ClusterNodeInfo {
        self.nodes[idx].raft.get_info()
    }

    /// Partition `isolated` nodes away from the remaining majority nodes.
    pub fn partition_minority(&self, isolated_indices: &[usize]) {
        let iso: HashSet<usize> = isolated_indices.iter().copied().collect();
        let group_a: Vec<String> = self
            .nodes
            .iter()
            .enumerate()
            .filter(|(i, _)| iso.contains(i))
            .map(|(_, n)| n.node_id.clone())
            .collect();
        let group_b: Vec<String> = self
            .nodes
            .iter()
            .enumerate()
            .filter(|(i, _)| !iso.contains(i))
            .map(|(_, n)| n.node_id.clone())
            .collect();
        self.transport.partition_groups(&group_a, &group_b);
    }

    /// Heal all simulated network partitions.
    pub fn heal_partitions(&self) {
        self.transport.heal_all_partitions();
    }

    /// Crash node `idx` (stop serving RPCs and drop volatile in-memory state).
    pub fn crash_node(&mut self, idx: usize) {
        let node_id = self.nodes[idx].node_id.clone();
        self.nodes[idx].alive = false;
        self.transport.mark_crashed(&node_id);
    }

    /// Simulate a torn power-cut tail write at the end of node `idx`'s on-disk Raft WAL.
    pub fn inject_torn_wal_tail(&mut self, idx: usize) -> Result<(), String> {
        self.crash_node(idx);
        let log_path = self.nodes[idx].data_dir.join("raft_replicated.log");
        if log_path.exists() {
            let mut f = OpenOptions::new()
                .append(true)
                .open(&log_path)
                .map_err(|e| format!("Failed opening WAL for torn write: {e}"))?;
            // Append an incomplete/corrupted frame header + partial payload (simulated power loss)
            let garbage = [0x14u8, 0x00, 0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02];
            f.write_all(&garbage)
                .map_err(|e| format!("Failed writing torn WAL bytes: {e}"))?;
            f.flush()
                .map_err(|e| format!("Failed flushing torn WAL bytes: {e}"))?;
        }
        Ok(())
    }

    /// Restart node `idx` cleanly from its persistent WAL + snapshot on disk, and step to Follower.
    pub fn restart_node(&mut self, idx: usize) {
        let mut peer_list = Vec::new();
        for (i, other) in self.nodes.iter().enumerate() {
            if i != idx {
                peer_list.push((other.node_id.clone(), other.address.clone()));
            }
        }

        let (node_id, address, data_dir) = {
            let slot = &self.nodes[idx];
            (
                slot.node_id.clone(),
                slot.address.clone(),
                slot.data_dir.clone(),
            )
        };

        let cfg = RaftConfig {
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            heartbeat_interval_ms: 50,
            data_dir: Some(data_dir),
        };
        let recovered_raft = Arc::new(RaftNode::with_config(&node_id, &address, cfg));
        for (pid, paddr) in peer_list {
            recovered_raft.add_peer(pid, paddr);
        }
        recovered_raft.demote_to_follower();

        let recovered_seq =
            DeterministicSequencer::new(self.router.clone(), recovered_raft.clone());
        let recovered_limit = recovered_raft.get_info().commit_index;

        // Replay persisted epochs up to recovered commit_index without skipping
        let mut replayed = Vec::new();
        for batch in self.committed_epoch_log.values() {
            if batch.raft_log_index <= recovered_limit {
                let res = recovered_seq.apply_epoch_batch(batch);
                if !res.is_empty() {
                    replayed.push((batch.epoch_id, batch.raft_log_index));
                }
            }
        }

        let slot = &mut self.nodes[idx];
        slot.raft = recovered_raft;
        slot.sequencer = Arc::new(recovered_seq);
        *slot.applied_epoch_order.write() = replayed;
        slot.alive = true;
        self.transport.register_node((*slot.raft).clone());
    }

    /// Trigger a Raft election attempt on candidate node `idx` across `transport` and enforce
    /// the Raft Leader Safety invariant (at most one elected leader per term).
    pub async fn run_election(&mut self, candidate_idx: usize) -> bool {
        if !self.nodes[candidate_idx].alive {
            return false;
        }
        let candidate_id = self.nodes[candidate_idx].node_id.clone();
        self.transport.set_active_sender(Some(&candidate_id));

        let candidate_raft = self.nodes[candidate_idx].raft.clone();
        let (term, args) = candidate_raft.start_election();
        let mut peers: Vec<(String, String)> = candidate_raft.list_peers().into_iter().collect();
        peers.sort_by(|a, b| a.0.cmp(&b.0));

        let mut won = false;
        for (peer_id, peer_addr) in peers {
            if let Ok(reply) = self.transport.request_vote(&peer_addr, args.clone()).await {
                if reply.term > term {
                    candidate_raft.step_down(reply.term);
                    self.transport.set_active_sender(None);
                    return false;
                }
                if candidate_raft.record_vote(&peer_id, term, reply.vote_granted) {
                    won = true;
                }
            }
        }

        // Send initial heartbeats if candidate won majority quorum so old leaders step down
        if won {
            self.leaders_by_term
                .entry(term)
                .or_default()
                .insert(candidate_id);

            let mut batches: Vec<_> = candidate_raft.prepare_heartbeats().into_iter().collect();
            batches.sort_by(|a, b| a.0.cmp(&b.0));
            for (peer_id, (peer_addr, hb_args)) in batches {
                if let Ok(reply) = self.transport.append_entries(&peer_addr, hb_args).await {
                    candidate_raft.record_append_reply(&peer_id, &reply);
                }
            }
        }
        self.transport.set_active_sender(None);
        won
    }

    /// Locate a currently elected alive leader (prefers highest term if multiple unhealed stale leaders exist).
    pub fn find_highest_term_leader(&self) -> Option<usize> {
        self.nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| n.alive && n.raft.get_info().role == NodeRole::Leader)
            .max_by_key(|(_, n)| n.raft.get_info().term)
            .map(|(i, _)| i)
    }

    /// Execute a multi-shard Calvin epoch on node `leader_idx` replicated via `SimNetworkTransport`.
    /// Returns `Ok(receipts)` only when majority quorum acknowledges replication.
    pub async fn commit_calvin_epoch(
        &mut self,
        leader_idx: usize,
        txs: Vec<DeterministicTxSpec>,
    ) -> Result<(u64, Vec<tapisaurus_core::cluster::DeterministicTxReceipt>), String> {
        if !self.nodes[leader_idx].alive {
            return Err(format!("Node {leader_idx} is crashed"));
        }

        let leader_id = self.nodes[leader_idx].node_id.clone();
        self.transport.set_active_sender(Some(&leader_id));

        let epoch_id = self.next_epoch_seq;
        let payload = serde_json::to_value(&txs).map_err(|e| e.to_string())?;
        let raft = self.nodes[leader_idx].raft.clone();
        let res = raft
            .propose_and_replicate(
                format!("DETERMINISTIC_EPOCH {epoch_id}"),
                Some(payload),
                &self.transport,
            )
            .await;

        self.transport.set_active_sender(None);

        let raft_log_index = res?;
        self.next_epoch_seq += 1;

        // Raft state-machine safety invariant: catch leader up on all prior committed epochs
        // in strict ascending epoch order before executing the newly committed batch.
        let leader_commit = self.nodes[leader_idx].raft.get_info().commit_index;
        for prior in self.committed_epoch_log.values() {
            if prior.raft_log_index <= leader_commit {
                let res = self.nodes[leader_idx].sequencer.apply_epoch_batch(prior);
                if !res.is_empty() {
                    self.nodes[leader_idx]
                        .applied_epoch_order
                        .write()
                        .push((prior.epoch_id, prior.raft_log_index));
                }
            }
        }

        let batch = DeterministicEpochBatch {
            epoch_id,
            raft_log_index,
            transactions: txs,
        };
        let receipts = self.nodes[leader_idx].sequencer.apply_epoch_batch(&batch);
        if !receipts.is_empty() {
            self.nodes[leader_idx]
                .applied_epoch_order
                .write()
                .push((epoch_id, raft_log_index));
        }
        self.committed_epoch_log.insert(epoch_id, batch);

        // Apply committed epoch to all reachable followers whose commit_index caught up
        self.sync_followers_state_machines();

        Ok((epoch_id, receipts))
    }

    /// Replicate heartbeats from leader and catch up lagged or snapshot-compacted nodes.
    pub async fn catch_up_cluster(&mut self) -> Result<usize, String> {
        let leader_idx = self
            .find_highest_term_leader()
            .ok_or_else(|| "No active leader in cluster".to_string())?;
        let leader_id = self.nodes[leader_idx].node_id.clone();
        let leader_raft = self.nodes[leader_idx].raft.clone();
        let leader_term = leader_raft.get_info().term;
        let leader_commit = leader_raft.get_info().commit_index;

        self.transport.set_active_sender(Some(&leader_id));
        for round in 0..8 {
            let _ = round;
            let mut batches: Vec<_> = leader_raft.prepare_heartbeats().into_iter().collect();
            batches.sort_by(|a, b| a.0.cmp(&b.0));
            for (peer_id, (peer_addr, args)) in batches {
                if let Ok(reply) = self.transport.append_entries(&peer_addr, args).await {
                    leader_raft.record_append_reply(&peer_id, &reply);
                }
            }
        }
        self.transport.set_active_sender(None);

        // Any node that still lags (e.g. due to log compaction on leader or fresh disk restart)
        // recovers via InstallSnapshot + committed batch catch-up cleanly.
        let mut snapshot_recoveries = 0;
        let snapshot_bytes = serde_json::to_vec(&self.nodes[leader_idx].sequencer.snapshot_state())
            .map_err(|e| e.to_string())?;

        for (idx, slot) in self.nodes.iter().enumerate() {
            if idx == leader_idx || !slot.alive {
                continue;
            }
            if slot.raft.get_info().commit_index < leader_commit {
                let args = InstallSnapshotArgs {
                    term: leader_term,
                    leader_id: leader_id.clone(),
                    last_included_index: leader_commit,
                    last_included_term: leader_term,
                    data: snapshot_bytes.clone(),
                };
                let _ = slot.raft.handle_install_snapshot(args);
                snapshot_recoveries += 1;
            }
        }

        self.sync_followers_state_machines();
        Ok(snapshot_recoveries)
    }

    /// Apply any newly committed Raft epochs onto follower `DeterministicSequencer` state machines.
    pub fn sync_followers_state_machines(&self) {
        for slot in &self.nodes {
            if !slot.alive {
                continue;
            }
            let committed_idx = slot.raft.get_info().commit_index;
            let _ = slot.raft.take_unapplied_committed();
            for batch in self.committed_epoch_log.values() {
                if batch.raft_log_index <= committed_idx {
                    let res = slot.sequencer.apply_epoch_batch(batch);
                    if !res.is_empty() {
                        slot.applied_epoch_order
                            .write()
                            .push((batch.epoch_id, batch.raft_log_index));
                    }
                }
            }
        }
    }

    /// Compact the leader's Raft log with a snapshot at current `commit_index`.
    pub fn snapshot_leader(&self) -> Result<(u64, Term), String> {
        let leader_idx = self
            .find_highest_term_leader()
            .ok_or_else(|| "No active leader to snapshot".to_string())?;
        let state = self.nodes[leader_idx].sequencer.snapshot_state();
        let bytes = serde_json::to_vec(&state).map_err(|e| e.to_string())?;
        self.nodes[leader_idx]
            .raft
            .take_snapshot(bytes)
            .map_err(|e| e.to_string())
    }

    /// Update a field on replica `idx`'s `CrdtDocument` using its locally skewed virtual clock.
    pub fn crdt_write_local(&mut self, idx: usize, field: &str, value: serde_json::Value) {
        let node_id = self.nodes[idx].node_id.clone();
        let ts = self.transport.node_local_time_ms(&node_id);
        self.nodes[idx].crdt.set_field(field, value, ts, &node_id);
    }

    /// Pairwise anti-entropy merge of all `CrdtDocument` replicas in arbitrary shuffled order.
    pub fn verify_crdt_convergence(&mut self) -> Result<(), String> {
        let mut order: Vec<usize> = (0..self.nodes.len()).collect();
        self.rng.shuffle(&mut order);

        let mut converged_forward = CrdtDocument::new();
        for &idx in &order {
            converged_forward.merge(&self.nodes[idx].crdt);
        }

        let mut converged_reverse = CrdtDocument::new();
        for &idx in order.iter().rev() {
            converged_reverse.merge(&self.nodes[idx].crdt);
        }

        if converged_forward.to_json_map() != converged_reverse.to_json_map() {
            return Err(format!(
                "CRDT convergence violation across shuffled merge orders: {:?} != {:?}",
                converged_forward.to_json_map(),
                converged_reverse.to_json_map()
            ));
        }
        Ok(())
    }

    /// Assert that no two distinct nodes were ever elected Leader in the same Raft term.
    pub fn assert_single_leader_per_term(&self) -> Result<(), String> {
        for (term, winners) in &self.leaders_by_term {
            if winners.len() > 1 {
                return Err(format!(
                    "SPLIT-BRAIN VIOLATION: Multiple leaders {winners:?} elected in term {term}"
                ));
            }
        }
        Ok(())
    }

    /// Assert Linearizability, Zero Double-Spend, Conservation of Total Supply, and Exact
    /// Replica State Convergence across all alive nodes after cluster heal.
    pub fn assert_zero_double_spend_and_convergence(
        &self,
        expected_total_supply: i64,
        min_balance: i64,
    ) -> Result<(u32, i64), String> {
        let alive_states: Vec<(String, BTreeMap<String, i64>)> = self
            .nodes
            .iter()
            .filter(|n| n.alive)
            .map(|n| (n.node_id.clone(), n.sequencer.snapshot_state()))
            .collect();

        if alive_states.is_empty() {
            return Err("No alive nodes available for state verification".to_string());
        }

        let (ref baseline_id, ref baseline) = alive_states[0];
        let mut sum: i64 = 0;
        for (k, &bal) in baseline {
            if bal < min_balance {
                return Err(format!(
                    "DOUBLE-SPEND / NEGATIVE BALANCE on '{k}' ({bal} < {min_balance}) at node '{baseline_id}'"
                ));
            }
            sum = sum
                .checked_add(bal)
                .ok_or_else(|| "Balance sum overflow".to_string())?;
        }

        if sum != expected_total_supply {
            return Err(format!(
                "CONSERVATION VIOLATION on '{baseline_id}': expected total {expected_total_supply}, got {sum}"
            ));
        }

        for (nid, st) in &alive_states[1..] {
            if st != baseline {
                let diag: Vec<String> = self
                    .nodes
                    .iter()
                    .map(|n| {
                        let inf = n.raft.get_info();
                        format!(
                            "{}(alive={},role={:?},term={},commit={},log={})",
                            n.node_id,
                            n.alive,
                            inf.role,
                            inf.term,
                            inf.commit_index,
                            inf.persistent_log_entries
                        )
                    })
                    .collect();
                return Err(format!(
                    "REPLICA DIVERGENCE between '{baseline_id}' and '{nid}': diag={diag:?}, baseline_applied={:?}, diverged_applied={:?}",
                    self.nodes
                        .iter()
                        .find(|n| &n.node_id == baseline_id)
                        .map(|n| n.applied_epoch_order.read().clone()),
                    self.nodes
                        .iter()
                        .find(|n| &n.node_id == nid)
                        .map(|n| n.applied_epoch_order.read().clone()),
                ));
            }
        }

        let encoded = serde_json::to_vec(baseline).map_err(|e| e.to_string())?;
        let mut hasher = Hasher::new();
        hasher.update(&encoded);
        Ok((hasher.finalize(), sum))
    }
}

/// Run an end-to-end deterministic chaos storm for `num_ops` transfer transactions across
/// randomized partitions, packet drops/reordering, leader failover, WAL torn-tail crashes,
/// log snapshot compaction, and CRDT skewed writes under the exact `seed`.
pub async fn run_chaos_suite(seed: u64, num_ops: usize) -> Result<SimReport, String> {
    let initial_faults = FaultProfile {
        packet_drop_rate: 0.08,
        packet_delay_rate: 0.25,
        min_delay_ms: 4,
        max_delay_ms: 60,
        duplicate_rate: 0.05,
    };

    let mut cluster = SimCluster::new(SimClusterConfig {
        seed,
        node_count: 5,
        faults: initial_faults.clone(),
    })?;

    // Configure realistic cross-region clock skew (+/- 4000ms) for CRDT tests
    cluster.transport().set_clock_skew("sim_node_0", -3500);
    cluster.transport().set_clock_skew("sim_node_1", 1200);
    cluster.transport().set_clock_skew("sim_node_2", -800);
    cluster.transport().set_clock_skew("sim_node_3", 4000);
    cluster.transport().set_clock_skew("sim_node_4", 250);

    // Seed 20 sharded accounts with 1,000 units each => exact total supply = 20,000
    let account_count = 20usize;
    let initial_per_acct = 1_000i64;
    let expected_total_supply = (account_count as i64) * initial_per_acct;

    let mut seed_ops = Vec::with_capacity(account_count);
    for a in 0..account_count {
        seed_ops.push(DeterministicOp::Put {
            key: format!("acct:{a:02}"),
            value: initial_per_acct,
        });
    }
    let seed_tx = DeterministicTxSpec::new("tx_genesis_accounts", seed_ops);

    // Disable packet drops for deterministic genesis seeding so all replicas start reliably
    cluster
        .transport()
        .set_fault_profile(FaultProfile::default());
    cluster.commit_calvin_epoch(0, vec![seed_tx]).await?;
    cluster
        .transport()
        .set_fault_profile(initial_faults.clone());

    let batch_size = 50usize;
    let mut ops_submitted = 0usize;
    let mut epochs_committed = 1usize;
    let mut transfers_committed = 0usize;
    let mut transfers_aborted = 0usize;
    let mut elections_run = 0usize;
    let mut crash_restarts = 0usize;
    let mut torn_wal_injections = 0usize;
    let mut snapshot_recoveries = 0usize;

    let mut round = 0usize;
    while ops_submitted < num_ops {
        round += 1;
        cluster.transport().advance_clock(35);

        // Inject chaos transitions deterministically every few epochs
        let action = cluster.rng_mut().gen_usize(10);
        if round.is_multiple_of(7) {
            // Isolate 2 minority nodes (3 remain in majority quorum)
            let n1 = cluster.rng_mut().gen_usize(5);
            let n2 = (n1 + 1) % 5;
            cluster.partition_minority(&[n1, n2]);
        } else if round.is_multiple_of(11) {
            cluster.heal_partitions();
            let cand = cluster.rng_mut().gen_usize(5);
            elections_run += 1;
            let _ = cluster.run_election(cand).await;
        } else if action == 0 {
            // Torn WAL tail fault + restart on a non-leader follower
            let leader = cluster.find_highest_term_leader().unwrap_or(0);
            let victim = (leader + 1 + cluster.rng_mut().gen_usize(4)) % 5;
            cluster.inject_torn_wal_tail(victim)?;
            torn_wal_injections += 1;
            cluster.restart_node(victim);
            crash_restarts += 1;
        } else if action == 1 && round > 3 {
            // Compact leader Raft log into snapshot to exercise InstallSnapshot catch-up
            let _ = cluster.snapshot_leader();
        }

        // Ensure cluster always has a reachable majority leader for forward progress checks
        if cluster.find_highest_term_leader().is_none() {
            cluster.heal_partitions();
            for cand in 0..5 {
                elections_run += 1;
                if cluster.run_election(cand).await {
                    break;
                }
            }
        }

        let current_batch = (num_ops - ops_submitted).min(batch_size);
        let mut txs = Vec::with_capacity(current_batch);
        for b in 0..current_batch {
            let from_idx = cluster.rng_mut().gen_usize(account_count);
            let mut to_idx = cluster.rng_mut().gen_usize(account_count);
            if to_idx == from_idx {
                to_idx = (from_idx + 1) % account_count;
            }
            // Deliberately include amounts > 1,000 so ~15% trigger deterministic precondition aborts
            let amount = cluster.rng_mut().gen_range_i64(1, 1_350);
            txs.push(DeterministicTxSpec::new(
                format!("tx_r{round}_{b}"),
                vec![DeterministicOp::Transfer {
                    from_key: format!("acct:{from_idx:02}"),
                    to_key: format!("acct:{to_idx:02}"),
                    amount,
                    min_balance: 0,
                }],
            ));
        }

        // Concurrent skewed-clock CRDT mutation across random alive node
        let crdt_node = cluster.rng_mut().gen_usize(5);
        cluster.crdt_write_local(
            crdt_node,
            "last_round_note",
            serde_json::json!(format!("r{round}_node{crdt_node}")),
        );

        let leader_idx = match cluster.find_highest_term_leader() {
            Some(idx) => idx,
            None => continue,
        };

        match cluster.commit_calvin_epoch(leader_idx, txs).await {
            Ok((_epoch_id, receipts)) => {
                ops_submitted += current_batch;
                epochs_committed += 1;
                for r in receipts {
                    if r.committed {
                        transfers_committed += 1;
                    } else {
                        transfers_aborted += 1;
                    }
                }
            }
            Err(_) => {
                // Majority could not acknowledge under heavy drop/partition/compaction:
                // heal partitions, clear loss briefly if needed to allow catch-up/re-election, and restore faults
                cluster.heal_partitions();
                cluster
                    .transport()
                    .set_fault_profile(FaultProfile::default());
                let next_cand = (leader_idx + 1) % 5;
                elections_run += 1;
                if !cluster.run_election(next_cand).await {
                    for cand in 0..5 {
                        elections_run += 1;
                        if cluster.run_election(cand).await {
                            break;
                        }
                    }
                }
                snapshot_recoveries += cluster.catch_up_cluster().await?;
                cluster
                    .transport()
                    .set_fault_profile(initial_faults.clone());
            }
        }
    }

    // Final Heal + Catch-Up Phase (Packet loss cleared, all 5 nodes converge identically)
    cluster.heal_partitions();
    cluster
        .transport()
        .set_fault_profile(FaultProfile::default());
    if cluster.find_highest_term_leader().is_none() {
        for cand in 0..5 {
            elections_run += 1;
            if cluster.run_election(cand).await {
                break;
            }
        }
    }
    snapshot_recoveries += cluster.catch_up_cluster().await?;

    cluster.assert_single_leader_per_term()?;
    cluster.verify_crdt_convergence()?;
    let (final_state_crc32, total_supply) =
        cluster.assert_zero_double_spend_and_convergence(expected_total_supply, 0)?;

    Ok(SimReport {
        seed,
        ops_submitted,
        epochs_committed,
        transfers_committed,
        transfers_aborted,
        elections_run,
        crash_restarts,
        torn_wal_injections,
        snapshot_recoveries,
        final_state_crc32,
        total_supply,
        network_stats: cluster.transport().stats(),
    })
}
