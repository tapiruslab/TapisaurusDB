//! # TapisaurusDB Core Storage Engine
//!
//! The foundational storage layer for TapisaurusDB — an AI-Native NoSQL database engine.
//!
//! ## Architecture
//!
//! The core engine is composed of several layers:
//!
//! 1. **Storage Layer** — Hybrid LSM-Tree + B-Tree storage engine
//!    - MemTable (in-memory write buffer using SkipList)
//!    - SSTable (sorted immutable disk files)
//!    - WAL (Write-Ahead Log for durability)
//!    - Buffer Pool (page cache for frequently accessed data)
//!
//! 2. **Document Layer** — BSON/JSON document management
//!    - Document encoding/decoding
//!    - Schema validation (optional)
//!    - Secondary indexing
//!
//! 3. **Transaction Layer** — ACID compliance
//!    - MVCC (Multi-Version Concurrency Control)
//!    - Snapshot Isolation
//!    - Lock management
//!
//! ## Design Principles
//!
//! - **Zero-copy where possible** — minimize memory allocations
//! - **Lock-free reads** — readers never block writers
//! - **Crash-safe** — WAL ensures no data loss on unexpected shutdown
//! - **Embeddable** — can be linked as a library or run as a server

pub mod backup;
pub mod cluster;
pub mod document;
pub mod error;
pub mod search;
pub mod storage;
pub mod stream;
pub mod tapirsync;
pub mod transaction;
pub mod ttl;
pub mod wasm_udf;

// Re-export commonly used types
pub use backup::{
    build_snapshot, load_and_verify_snapshot, save_snapshot_file, SnapshotArchive, SnapshotManifest,
};
pub use cluster::{
    DeterministicEpochBatch, DeterministicOp, DeterministicSequencer, DeterministicTxReceipt,
    DeterministicTxSpec, NodeRole, RaftNode, ShardDistribution, ShardRouter,
};
pub use document::{
    Document, DocumentId, HotColdCacheStats, HotColdCacheTracker, SlottedBinaryCodec, Value,
    SLOTTED_BINARY_MAGIC, SLOTTED_BINARY_VERSION,
};
pub use error::{TapisaurusError, TapisaurusResult};
pub use search::{InvertedIndex, SearchResult};
pub use storage::columnar::{
    ColumnData, ColumnDataType, ColumnZoneMap, ColumnarBatch, ColumnarSchema, ColumnarStripe,
    HtapColumnarTable, ZonePredicateOp,
};
pub use storage::engine::StorageEngine;
pub use stream::{ChangeEvent, ChangeStreamBus, OperationType};
pub use tapirsync::{
    TapirSyncAck, TapirSyncBundle, TapirSyncHlc, TapirSyncPeerStatus, TapirSyncRecord,
    TapirSyncRegistry, TapirSyncStatusSummary, TAPIRSYNC_PROTOCOL_VERSION, TAPIRSYNC_WIRE_MAGIC,
};
pub use ttl::{TtlManager, TtlStats};
pub use wasm_udf::{WasmSandboxModule, WasmTriggerAction, WasmUdfRegistry};

/// The database version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Default data directory name
pub const DEFAULT_DATA_DIR: &str = "tapisaurus_data";

/// Maximum document size (256 MB — far beyond MongoDB's 16MB limit)
pub const MAX_DOCUMENT_SIZE: usize = 256 * 1024 * 1024;

/// Default MemTable size before flush (64 MB)
pub const DEFAULT_MEMTABLE_SIZE: usize = 64 * 1024 * 1024;

/// Default buffer pool size (256 MB)
pub const DEFAULT_BUFFER_POOL_SIZE: usize = 256 * 1024 * 1024;

/// Magic bytes for TapisaurusDB data files
pub const MAGIC_BYTES: &[u8; 8] = b"TPSAUR01";
