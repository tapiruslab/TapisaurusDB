//! Deterministic Fault-Injecting Raft RPC Transport & Virtual Clock.
//!
//! Wraps live `RaftNode` endpoints behind `RaftRpcTransport` so every `RequestVote`
//! and `AppendEntries` call passes through deterministic packet-drop, delay/reordering,
//! duplication, and asymmetric/symmetric network partition filters.

use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use parking_lot::RwLock;
use tapisaurus_core::cluster::{
    AppendEntriesArgs, AppendEntriesReply, RaftNode, RaftRpcTransport, RequestVoteArgs,
    RequestVoteReply,
};

use crate::rng::SimRng;

/// Chaos fault configuration controlling packet loss, jitter, duplication, and clock skew.
#[derive(Debug, Clone)]
pub struct FaultProfile {
    /// Probability `[0.0, 1.0]` of dropping a live RPC message silently.
    pub packet_drop_rate: f64,
    /// Probability `[0.0, 1.0]` of delaying / reordering an RPC in the simulated clock queue.
    pub packet_delay_rate: f64,
    /// Minimum virtual delay in milliseconds when delayed.
    pub min_delay_ms: u64,
    /// Maximum virtual delay in milliseconds when delayed.
    pub max_delay_ms: u64,
    /// Probability `[0.0, 1.0]` of replaying a duplicate idempotent delivery.
    pub duplicate_rate: f64,
}

impl Default for FaultProfile {
    fn default() -> Self {
        Self {
            packet_drop_rate: 0.0,
            packet_delay_rate: 0.0,
            min_delay_ms: 5,
            max_delay_ms: 85,
            duplicate_rate: 0.0,
        }
    }
}

/// Telemetry counters collected across a deterministic simulation run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NetworkStats {
    pub rpc_sent: u64,
    pub rpc_delivered: u64,
    pub rpc_dropped_partition: u64,
    pub rpc_dropped_crash: u64,
    pub rpc_dropped_chaos: u64,
    pub rpc_delayed: u64,
    pub rpc_duplicated: u64,
}

/// Logical simulated clock with per-node skewed offsets for CRDT & election testing.
#[derive(Debug, Clone, Default)]
pub struct SimClock {
    pub now_ms: u64,
    pub node_skew_ms: BTreeMap<String, i64>,
}

impl SimClock {
    /// Advance virtual simulation time cleanly without sleeping wall-clock time.
    pub fn advance(&mut self, delta_ms: u64) {
        self.now_ms = self.now_ms.saturating_add(delta_ms);
    }

    /// Return the local skewed timestamp observed on `node_id`.
    pub fn local_time_ms(&self, node_id: &str) -> u64 {
        let skew = self.node_skew_ms.get(node_id).copied().unwrap_or(0);
        if skew >= 0 {
            self.now_ms.saturating_add(skew as u64)
        } else {
            self.now_ms.saturating_sub(skew.unsigned_abs())
        }
    }
}

/// Deferred AppendEntries message parked in the simulated out-of-order network queue.
#[derive(Debug, Clone)]
pub struct DelayedAppend {
    pub deliver_at_ms: u64,
    pub seq: u64,
    pub from_id: String,
    pub target_addr: String,
    pub args: AppendEntriesArgs,
}

impl Eq for DelayedAppend {}

impl PartialEq for DelayedAppend {
    fn eq(&self, other: &Self) -> bool {
        self.deliver_at_ms == other.deliver_at_ms && self.seq == other.seq
    }
}

impl Ord for DelayedAppend {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse ordering so BinaryHeap behaves as earliest-delivery min-heap
        other
            .deliver_at_ms
            .cmp(&self.deliver_at_ms)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

impl PartialOrd for DelayedAppend {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

struct TransportState {
    nodes_by_addr: BTreeMap<String, RaftNode>,
    addr_to_id: BTreeMap<String, String>,
    crashed_nodes: BTreeSet<String>,
    isolated_nodes: BTreeSet<String>,
    blocked_links: BTreeSet<(String, String)>,
    faults: FaultProfile,
    clock: SimClock,
    rng: SimRng,
    stats: NetworkStats,
    delayed_queue: BinaryHeap<DelayedAppend>,
    next_seq: u64,
    active_sender: Option<String>,
}

/// Deterministic fault-injecting implementation of `RaftRpcTransport`.
#[derive(Clone)]
pub struct SimNetworkTransport {
    inner: Arc<RwLock<TransportState>>,
}

impl SimNetworkTransport {
    /// Construct a deterministic fault-injecting transport seeded with `seed`.
    pub fn new(seed: u64, faults: FaultProfile) -> Self {
        Self {
            inner: Arc::new(RwLock::new(TransportState {
                nodes_by_addr: BTreeMap::new(),
                addr_to_id: BTreeMap::new(),
                crashed_nodes: BTreeSet::new(),
                isolated_nodes: BTreeSet::new(),
                blocked_links: BTreeSet::new(),
                faults,
                clock: SimClock::default(),
                rng: SimRng::from_seed(seed ^ 0xA5A5_5A5A_1337_BEEF),
                stats: NetworkStats::default(),
                delayed_queue: BinaryHeap::new(),
                next_seq: 1,
                active_sender: None,
            })),
        }
    }

    /// Register or replace a node endpoint (used on initial boot and post-crash restart).
    pub fn register_node(&self, node: RaftNode) {
        let info = node.get_info();
        let mut st = self.inner.write();
        st.crashed_nodes.remove(&info.node_id);
        st.addr_to_id
            .insert(info.address.clone(), info.node_id.clone());
        st.nodes_by_addr.insert(info.address, node);
    }

    /// Update active chaos fault profile mid-simulation.
    pub fn set_fault_profile(&self, faults: FaultProfile) {
        self.inner.write().faults = faults;
    }

    /// Set currently attributed caller node ID for directional link partition checks.
    pub fn set_active_sender(&self, node_id: Option<&str>) {
        self.inner.write().active_sender = node_id.map(|s| s.to_string());
    }

    /// Mark a node as crashed (all RPCs fail until restarted).
    pub fn mark_crashed(&self, node_id: &str) {
        self.inner.write().crashed_nodes.insert(node_id.to_string());
    }

    /// Isolate a node completely from every other node in the cluster.
    pub fn isolate_node(&self, node_id: &str) {
        self.inner
            .write()
            .isolated_nodes
            .insert(node_id.to_string());
    }

    /// Create an asymmetric or symmetric partition blocking traffic between `group_a` and `group_b`.
    pub fn partition_groups(&self, group_a: &[String], group_b: &[String]) {
        let mut st = self.inner.write();
        for a in group_a {
            for b in group_b {
                st.blocked_links.insert((a.clone(), b.clone()));
                st.blocked_links.insert((b.clone(), a.clone()));
            }
        }
    }

    /// Heal all node isolations and link partitions cleanly.
    pub fn heal_all_partitions(&self) {
        let mut st = self.inner.write();
        st.isolated_nodes.clear();
        st.blocked_links.clear();
    }

    /// Configure per-node skewed clock offset (in ms) for distributed CRDT simulations.
    pub fn set_clock_skew(&self, node_id: &str, skew_ms: i64) {
        self.inner
            .write()
            .clock
            .node_skew_ms
            .insert(node_id.to_string(), skew_ms);
    }

    /// Advance the virtual simulation clock and return current simulated timestamp in ms.
    pub fn advance_clock(&self, delta_ms: u64) -> u64 {
        let mut st = self.inner.write();
        st.clock.advance(delta_ms);
        st.clock.now_ms
    }

    /// Read current virtual timestamp observed by `node_id`.
    pub fn node_local_time_ms(&self, node_id: &str) -> u64 {
        let st = self.inner.read();
        st.clock.local_time_ms(node_id)
    }

    /// Snapshot accumulated network fault telemetry.
    pub fn stats(&self) -> NetworkStats {
        self.inner.read().stats.clone()
    }

    /// Enqueue an asynchronous AppendEntries RPC with simulated network delay/reordering.
    pub fn enqueue_delayed_append(
        &self,
        from_id: &str,
        target_addr: &str,
        args: AppendEntriesArgs,
    ) {
        let mut st = self.inner.write();
        let delay = if st.faults.max_delay_ms > st.faults.min_delay_ms {
            let low = st.faults.min_delay_ms;
            let high = st.faults.max_delay_ms;
            st.rng.gen_range_u64(low, high)
        } else {
            st.faults.min_delay_ms
        };
        let deliver_at_ms = st.clock.now_ms.saturating_add(delay);
        let seq = st.next_seq;
        st.next_seq += 1;
        st.stats.rpc_delayed += 1;
        st.delayed_queue.push(DelayedAppend {
            deliver_at_ms,
            seq,
            from_id: from_id.to_string(),
            target_addr: target_addr.to_string(),
            args,
        });
    }

    /// Deliver all delayed packets whose simulated delivery time `<= now_ms`.
    pub fn drain_due_delayed(&self) -> Vec<(String, AppendEntriesReply)> {
        let due = {
            let mut st = self.inner.write();
            let now = st.clock.now_ms;
            let mut due = Vec::new();
            while let Some(top) = st.delayed_queue.peek() {
                if top.deliver_at_ms <= now {
                    if let Some(item) = st.delayed_queue.pop() {
                        due.push(item);
                    }
                } else {
                    break;
                }
            }
            due
        };

        let mut replies = Vec::new();
        for pkt in due {
            if let Ok(reply) =
                self.deliver_append_now(Some(&pkt.from_id), &pkt.target_addr, pkt.args)
            {
                replies.push((pkt.target_addr, reply));
            }
        }
        replies
    }

    fn check_route_and_faults(
        st: &mut TransportState,
        sender_id: Option<&str>,
        target_addr: &str,
    ) -> Result<(RaftNode, bool, u64), String> {
        st.stats.rpc_sent += 1;

        let target_id = st
            .addr_to_id
            .get(target_addr)
            .cloned()
            .ok_or_else(|| format!("Unknown peer endpoint '{target_addr}'"))?;

        if st.crashed_nodes.contains(&target_id) {
            st.stats.rpc_dropped_crash += 1;
            return Err(format!("Simulated node crash on '{target_id}'"));
        }

        if let Some(src) = sender_id {
            if st.crashed_nodes.contains(src) {
                st.stats.rpc_dropped_crash += 1;
                return Err(format!("Sender node '{src}' is crashed"));
            }
            if st.isolated_nodes.contains(src)
                || st.isolated_nodes.contains(&target_id)
                || st
                    .blocked_links
                    .contains(&(src.to_string(), target_id.clone()))
            {
                st.stats.rpc_dropped_partition += 1;
                return Err(format!(
                    "Simulated network partition between '{src}' and '{target_id}'"
                ));
            }
        } else if st.isolated_nodes.contains(&target_id) {
            st.stats.rpc_dropped_partition += 1;
            return Err(format!("Simulated isolation on '{target_id}'"));
        }

        let drop_rate = st.faults.packet_drop_rate;
        if st.rng.bernoulli(drop_rate) {
            st.stats.rpc_dropped_chaos += 1;
            return Err(format!("Simulated packet drop to '{target_id}'"));
        }

        let delay_rate = st.faults.packet_delay_rate;
        let delay_ms = if st.rng.bernoulli(delay_rate) {
            st.stats.rpc_delayed += 1;
            let lo = st.faults.min_delay_ms;
            let hi = st.faults.max_delay_ms;
            st.rng.gen_range_u64(lo, hi)
        } else {
            0
        };

        let dup_rate = st.faults.duplicate_rate;
        let duplicate = st.rng.bernoulli(dup_rate);
        if duplicate {
            st.stats.rpc_duplicated += 1;
        }

        let node = st
            .nodes_by_addr
            .get(target_addr)
            .cloned()
            .ok_or_else(|| format!("Target node '{target_id}' missing from transport table"))?;

        st.stats.rpc_delivered += 1;
        Ok((node, duplicate, delay_ms))
    }

    fn deliver_append_now(
        &self,
        sender_id: Option<&str>,
        peer_addr: &str,
        args: AppendEntriesArgs,
    ) -> Result<AppendEntriesReply, String> {
        let (node, duplicate, delay_ms) = {
            let mut st = self.inner.write();
            let effective_sender = sender_id
                .map(|s| s.to_string())
                .or_else(|| Some(args.leader_id.clone()));
            Self::check_route_and_faults(&mut st, effective_sender.as_deref(), peer_addr)?
        };

        if delay_ms > 0 {
            self.advance_clock(delay_ms);
        }

        let reply = node.handle_append_entries(args.clone());
        if duplicate {
            // Replay idempotent duplicate delivery against Raft state machine
            let _ = node.handle_append_entries(args);
        }
        Ok(reply)
    }
}

impl RaftRpcTransport for SimNetworkTransport {
    fn request_vote(
        &self,
        peer_addr: &str,
        args: RequestVoteArgs,
    ) -> Pin<Box<dyn Future<Output = Result<RequestVoteReply, String>> + Send>> {
        let transport = self.clone();
        let addr = peer_addr.to_string();
        Box::pin(async move {
            let (node, duplicate, delay_ms) = {
                let mut st = transport.inner.write();
                let sender = args.candidate_id.clone();
                Self::check_route_and_faults(&mut st, Some(&sender), &addr)?
            };
            if delay_ms > 0 {
                transport.advance_clock(delay_ms);
            }
            let reply = node.handle_request_vote(args.clone());
            if duplicate {
                let _ = node.handle_request_vote(args);
            }
            Ok(reply)
        })
    }

    fn append_entries(
        &self,
        peer_addr: &str,
        args: AppendEntriesArgs,
    ) -> Pin<Box<dyn Future<Output = Result<AppendEntriesReply, String>> + Send>> {
        let transport = self.clone();
        let addr = peer_addr.to_string();
        Box::pin(async move {
            let sender = transport.inner.read().active_sender.clone();
            transport.deliver_append_now(sender.as_deref(), &addr, args)
        })
    }
}
