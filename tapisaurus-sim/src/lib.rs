//! # TapisaurusDB Deterministic Simulation Testing (`tapisaurus-sim`)
//!
//! FoundationDB / TigerBeetle-style single-process deterministic chaos harness exercising
//! live `RaftNode`, `RaftDiskStore`, `DeterministicSequencer` (Calvin), and `CrdtDocument`
//! replicas across seeded network partitions, packet loss/delay/duplication, crash restarts,
//! torn WAL tail writes, and snapshot compaction recovery.

#![forbid(unsafe_code)]

pub mod fault_transport;
pub mod harness;
pub mod rng;

pub use fault_transport::{
    DelayedAppend, FaultProfile, NetworkStats, SimClock, SimNetworkTransport,
};
pub use harness::{run_chaos_suite, SimCluster, SimClusterConfig, SimReport};
pub use rng::SimRng;
