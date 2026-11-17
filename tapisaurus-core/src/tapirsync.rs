//! # TapirSync (`TPSYNC01`) — Edge-to-Cloud Replication Engine for TapisaurusDB
//!
//! Implements the server/cloud side of the **TapirSync (`TPSYNC01`)** binary wire protocol,
//! enabling seamless bidirectional synchronization between embedded **TapirusDB (`.tapir`)**
//! edge databases and distributed **TapisaurusDB** clusters.

use crate::error::{TapisaurusError, TapisaurusResult};
use chrono::Utc;
use crc32fast::Hasher;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// 8-byte wire magic header identifying a `TapirSync` v1 binary replication bundle.
pub const TAPIRSYNC_WIRE_MAGIC: &[u8; 8] = b"TPSYNC01";

/// Current `TapirSync` wire protocol version.
pub const TAPIRSYNC_PROTOCOL_VERSION: u16 = 1;

/// Hybrid Logical Clock (HLC) stamp providing monotonic causal ordering and deterministic
/// Last-Write-Wins (LWW) conflict tie-breaking across edge and cloud nodes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TapirSyncHlc {
    pub wall_time_ms: u64,
    pub logical_counter: u32,
    pub node_id: String,
}

impl Ord for TapirSyncHlc {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.wall_time_ms
            .cmp(&other.wall_time_ms)
            .then_with(|| self.logical_counter.cmp(&other.logical_counter))
            .then_with(|| self.node_id.cmp(&other.node_id))
    }
}

impl PartialOrd for TapirSyncHlc {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl TapirSyncHlc {
    pub fn new(node_id: impl Into<String>, wall_time_ms: u64) -> Self {
        Self {
            wall_time_ms,
            logical_counter: 0,
            node_id: node_id.into(),
        }
    }

    pub fn tick(&mut self, now_ms: u64) -> Self {
        if now_ms > self.wall_time_ms {
            self.wall_time_ms = now_ms;
            self.logical_counter = 0;
        } else {
            self.logical_counter = self.logical_counter.saturating_add(1);
        }
        self.clone()
    }

    pub fn observe(&mut self, remote: &TapirSyncHlc, now_ms: u64) -> Self {
        let max_wall = self.wall_time_ms.max(remote.wall_time_ms).max(now_ms);
        if max_wall == self.wall_time_ms && max_wall == remote.wall_time_ms {
            self.logical_counter = self
                .logical_counter
                .max(remote.logical_counter)
                .saturating_add(1);
        } else if max_wall == self.wall_time_ms {
            self.logical_counter = self.logical_counter.saturating_add(1);
        } else if max_wall == remote.wall_time_ms {
            self.wall_time_ms = max_wall;
            self.logical_counter = remote.logical_counter.saturating_add(1);
        } else {
            self.wall_time_ms = max_wall;
            self.logical_counter = 0;
        }
        self.clone()
    }
}

/// Multi-modal logical replication record identical to `tapirus::TapirSyncRecord`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TapirSyncRecord {
    TableRowUpsert {
        table: String,
        row_id: String,
        columns: BTreeMap<String, serde_json::Value>,
        hlc: TapirSyncHlc,
    },
    TableRowDelete {
        table: String,
        row_id: String,
        hlc: TapirSyncHlc,
    },
    DocumentUpsert {
        collection: String,
        doc_id: String,
        document: serde_json::Value,
        hlc: TapirSyncHlc,
    },
    DocumentDelete {
        collection: String,
        doc_id: String,
        hlc: TapirSyncHlc,
    },
    GraphVertexUpsert {
        vertex_id: String,
        label: String,
        properties: serde_json::Value,
        vector: Option<Vec<f32>>,
        hlc: TapirSyncHlc,
    },
    GraphEdgeUpsert {
        edge_id: String,
        from_id: String,
        to_id: String,
        relation: String,
        weight: f32,
        properties: serde_json::Value,
        hlc: TapirSyncHlc,
    },
    AgentMemoryUpsert {
        memory_id: u64,
        content: String,
        vector: Option<Vec<f32>>,
        importance: f32,
        tags: Vec<String>,
        namespace: Option<String>,
        session_id: Option<String>,
        created_at: u64,
        hlc: TapirSyncHlc,
    },
}

impl TapirSyncRecord {
    pub fn canonical_key(&self) -> String {
        match self {
            Self::TableRowUpsert { table, row_id, .. }
            | Self::TableRowDelete { table, row_id, .. } => format!("tbl:{table}:{row_id}"),
            Self::DocumentUpsert {
                collection, doc_id, ..
            }
            | Self::DocumentDelete {
                collection, doc_id, ..
            } => format!("doc:{collection}:{doc_id}"),
            Self::GraphVertexUpsert { vertex_id, .. } => format!("gv:{vertex_id}"),
            Self::GraphEdgeUpsert {
                from_id,
                to_id,
                relation,
                ..
            } => format!("ge:{from_id}:{relation}:{to_id}"),
            Self::AgentMemoryUpsert { memory_id, .. } => format!("mem:{memory_id}"),
        }
    }

    pub fn hlc(&self) -> &TapirSyncHlc {
        match self {
            Self::TableRowUpsert { hlc, .. }
            | Self::TableRowDelete { hlc, .. }
            | Self::DocumentUpsert { hlc, .. }
            | Self::DocumentDelete { hlc, .. }
            | Self::GraphVertexUpsert { hlc, .. }
            | Self::GraphEdgeUpsert { hlc, .. }
            | Self::AgentMemoryUpsert { hlc, .. } => hlc,
        }
    }

    pub fn is_delete(&self) -> bool {
        matches!(
            self,
            Self::TableRowDelete { .. } | Self::DocumentDelete { .. }
        )
    }

    pub fn content_crc32(&self) -> u32 {
        let mut hasher = Hasher::new();
        match self {
            Self::TableRowUpsert {
                table,
                row_id,
                columns,
                ..
            } => {
                hasher.update(b"TRU");
                hasher.update(table.as_bytes());
                hasher.update(row_id.as_bytes());
                hasher.update(&(columns.len() as u32).to_le_bytes());
                for (k, v) in columns {
                    hasher.update(k.as_bytes());
                    hash_json_value(&mut hasher, v);
                }
            }
            Self::TableRowDelete { table, row_id, .. } => {
                hasher.update(b"TRD");
                hasher.update(table.as_bytes());
                hasher.update(row_id.as_bytes());
            }
            Self::DocumentUpsert {
                collection,
                doc_id,
                document,
                ..
            } => {
                hasher.update(b"DOU");
                hasher.update(collection.as_bytes());
                hasher.update(doc_id.as_bytes());
                hash_json_value(&mut hasher, document);
            }
            Self::DocumentDelete {
                collection, doc_id, ..
            } => {
                hasher.update(b"DOD");
                hasher.update(collection.as_bytes());
                hasher.update(doc_id.as_bytes());
            }
            Self::GraphVertexUpsert {
                vertex_id,
                label,
                properties,
                vector,
                ..
            } => {
                hasher.update(b"GVU");
                hasher.update(vertex_id.as_bytes());
                hasher.update(label.as_bytes());
                hash_json_value(&mut hasher, properties);
                if let Some(vec) = vector {
                    for f in vec {
                        hasher.update(&((*f as f64 * 1e6).round() as i64).to_le_bytes());
                    }
                }
            }
            Self::GraphEdgeUpsert {
                edge_id,
                from_id,
                to_id,
                relation,
                weight,
                properties,
                ..
            } => {
                hasher.update(b"GEU");
                hasher.update(edge_id.as_bytes());
                hasher.update(from_id.as_bytes());
                hasher.update(to_id.as_bytes());
                hasher.update(relation.as_bytes());
                hasher.update(&((*weight as f64 * 1e6).round() as i64).to_le_bytes());
                hash_json_value(&mut hasher, properties);
            }
            Self::AgentMemoryUpsert {
                memory_id,
                content,
                vector,
                importance,
                tags,
                namespace,
                session_id,
                created_at,
                ..
            } => {
                hasher.update(b"AMU");
                hasher.update(&memory_id.to_le_bytes());
                hasher.update(content.as_bytes());
                if let Some(vec) = vector {
                    for f in vec {
                        hasher.update(&((*f as f64 * 1e6).round() as i64).to_le_bytes());
                    }
                }
                hasher.update(&((*importance as f64 * 1e6).round() as i64).to_le_bytes());
                for tag in tags {
                    hasher.update(tag.as_bytes());
                }
                if let Some(ns) = namespace {
                    hasher.update(ns.as_bytes());
                }
                if let Some(sid) = session_id {
                    hasher.update(sid.as_bytes());
                }
                hasher.update(&created_at.to_le_bytes());
            }
        }
        hasher.finalize().max(1)
    }
}

fn hash_json_value(hasher: &mut Hasher, val: &serde_json::Value) {
    match val {
        serde_json::Value::Null => hasher.update(&[0]),
        serde_json::Value::Bool(b) => hasher.update(&[1, u8::from(*b)]),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                hasher.update(&[2]);
                hasher.update(&i.to_le_bytes());
            } else if let Some(u) = n.as_u64() {
                hasher.update(&[2]);
                hasher.update(&(u as i64).to_le_bytes());
            } else if let Some(f) = n.as_f64() {
                hasher.update(&[3]);
                let scaled = (f * 1e9).round() as i64;
                hasher.update(&scaled.to_le_bytes());
            }
        }
        serde_json::Value::String(s) => {
            hasher.update(&[4]);
            hasher.update(&(s.len() as u32).to_le_bytes());
            hasher.update(s.as_bytes());
        }
        serde_json::Value::Array(arr) => {
            hasher.update(&[5]);
            hasher.update(&(arr.len() as u32).to_le_bytes());
            for item in arr {
                hash_json_value(hasher, item);
            }
        }
        serde_json::Value::Object(map) => {
            hasher.update(&[6]);
            hasher.update(&(map.len() as u32).to_le_bytes());
            for (k, v) in map {
                hasher.update(k.as_bytes());
                hash_json_value(hasher, v);
            }
        }
    }
}

/// Atomic, CRC32-verified `TapirSync` (`TPSYNC01`) replication bundle.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TapirSyncBundle {
    pub protocol_version: u16,
    pub source_node_id: String,
    pub base_sequence: u64,
    pub target_sequence: u64,
    pub hlc: TapirSyncHlc,
    pub records: Vec<TapirSyncRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wal_chunk_bytes: Option<Vec<u8>>,
    pub bundle_crc32: u32,
}

impl TapirSyncBundle {
    pub fn new(
        source_node_id: impl Into<String>,
        base_sequence: u64,
        target_sequence: u64,
        hlc: TapirSyncHlc,
        records: Vec<TapirSyncRecord>,
        wal_chunk_bytes: Option<Vec<u8>>,
    ) -> Self {
        let mut bundle = Self {
            protocol_version: TAPIRSYNC_PROTOCOL_VERSION,
            source_node_id: source_node_id.into(),
            base_sequence,
            target_sequence,
            hlc,
            records,
            wal_chunk_bytes,
            bundle_crc32: 0,
        };
        bundle.bundle_crc32 = bundle.calculate_crc();
        bundle
    }

    pub fn calculate_crc(&self) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(TAPIRSYNC_WIRE_MAGIC);
        hasher.update(&self.protocol_version.to_le_bytes());
        hasher.update(self.source_node_id.as_bytes());
        hasher.update(&self.base_sequence.to_le_bytes());
        hasher.update(&self.target_sequence.to_le_bytes());
        hasher.update(&self.hlc.wall_time_ms.to_le_bytes());
        hasher.update(&self.hlc.logical_counter.to_le_bytes());
        hasher.update(self.hlc.node_id.as_bytes());
        hasher.update(&(self.records.len() as u32).to_le_bytes());
        for rec in &self.records {
            hasher.update(rec.canonical_key().as_bytes());
            hasher.update(&rec.content_crc32().to_le_bytes());
            hasher.update(&rec.hlc().wall_time_ms.to_le_bytes());
            hasher.update(&rec.hlc().logical_counter.to_le_bytes());
            hasher.update(rec.hlc().node_id.as_bytes());
        }
        if let Some(ref wal) = self.wal_chunk_bytes {
            hasher.update(&(wal.len() as u32).to_le_bytes());
            hasher.update(wal);
        }
        hasher.finalize()
    }

    pub fn verify(&self) -> TapisaurusResult<()> {
        if self.protocol_version != TAPIRSYNC_PROTOCOL_VERSION {
            return Err(TapisaurusError::InvalidDocument(format!(
                "Unsupported TapirSync protocol version: {}",
                self.protocol_version
            )));
        }
        if self.target_sequence < self.base_sequence {
            return Err(TapisaurusError::InvalidDocument(
                "TapirSync target_sequence cannot be less than base_sequence".into(),
            ));
        }
        let expected = self.calculate_crc();
        if self.bundle_crc32 != expected {
            return Err(TapisaurusError::ChecksumMismatch {
                expected,
                actual: self.bundle_crc32,
            });
        }
        Ok(())
    }

    pub fn to_bytes(&self) -> TapisaurusResult<Vec<u8>> {
        let body = serde_json::to_vec(self)?;
        let payload_len = u32::try_from(body.len()).map_err(|_| {
            TapisaurusError::InvalidDocument(
                "TapirSyncBundle payload exceeds 4 GiB wire limit".into(),
            )
        })?;
        let record_count = u32::try_from(self.records.len()).map_err(|_| {
            TapisaurusError::InvalidDocument(
                "TapirSyncBundle record count exceeds u32 limit".into(),
            )
        })?;

        let mut out = Vec::with_capacity(38 + body.len());
        out.extend_from_slice(TAPIRSYNC_WIRE_MAGIC);
        out.extend_from_slice(&self.protocol_version.to_le_bytes());
        out.extend_from_slice(&self.base_sequence.to_le_bytes());
        out.extend_from_slice(&self.target_sequence.to_le_bytes());
        out.extend_from_slice(&record_count.to_le_bytes());
        out.extend_from_slice(&self.bundle_crc32.to_le_bytes());
        out.extend_from_slice(&payload_len.to_le_bytes());
        out.extend_from_slice(&body);
        Ok(out)
    }

    pub fn from_bytes(bytes: &[u8]) -> TapisaurusResult<Self> {
        let bundle: Self = if bytes.starts_with(TAPIRSYNC_WIRE_MAGIC) {
            if bytes.len() < 38 {
                return Err(TapisaurusError::InvalidDocument(
                    "Truncated TPSYNC01 binary wire header (< 38 bytes)".into(),
                ));
            }
            let version = u16::from_le_bytes([bytes[8], bytes[9]]);
            let base_seq = u64::from_le_bytes(bytes[10..18].try_into().unwrap());
            let target_seq = u64::from_le_bytes(bytes[18..26].try_into().unwrap());
            let record_count = u32::from_le_bytes(bytes[26..30].try_into().unwrap()) as usize;
            let header_crc = u32::from_le_bytes(bytes[30..34].try_into().unwrap());
            let payload_len = u32::from_le_bytes(bytes[34..38].try_into().unwrap()) as usize;

            if bytes.len() != 38 + payload_len {
                return Err(TapisaurusError::InvalidDocument(format!(
                    "TPSYNC01 frame length mismatch: expected {}, got {}",
                    38 + payload_len,
                    bytes.len()
                )));
            }
            let decoded: Self = serde_json::from_slice(&bytes[38..])?;
            if decoded.protocol_version != version
                || decoded.base_sequence != base_seq
                || decoded.target_sequence != target_seq
                || decoded.records.len() != record_count
                || decoded.bundle_crc32 != header_crc
            {
                return Err(TapisaurusError::InvalidDocument(
                    "TPSYNC01 binary envelope header does not match payload metadata".into(),
                ));
            }
            decoded
        } else {
            serde_json::from_slice(bytes)?
        };
        bundle.verify()?;
        Ok(bundle)
    }
}

/// Replication acknowledgement receipt returned by `TapisaurusDB` after applying an edge bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TapirSyncAck {
    pub accepted: bool,
    pub responder_node_id: String,
    pub source_node_id: String,
    pub applied_sequence: u64,
    pub records_applied: usize,
    pub conflicts_resolved: usize,
    pub responder_hlc: TapirSyncHlc,
    pub ack_crc32: u32,
}

impl TapirSyncAck {
    pub fn new(
        accepted: bool,
        responder_node_id: impl Into<String>,
        source_node_id: impl Into<String>,
        applied_sequence: u64,
        records_applied: usize,
        conflicts_resolved: usize,
        responder_hlc: TapirSyncHlc,
    ) -> Self {
        let mut ack = Self {
            accepted,
            responder_node_id: responder_node_id.into(),
            source_node_id: source_node_id.into(),
            applied_sequence,
            records_applied,
            conflicts_resolved,
            responder_hlc,
            ack_crc32: 0,
        };
        ack.ack_crc32 = ack.calculate_crc();
        ack
    }

    pub fn calculate_crc(&self) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(&[u8::from(self.accepted)]);
        hasher.update(self.responder_node_id.as_bytes());
        hasher.update(self.source_node_id.as_bytes());
        hasher.update(&self.applied_sequence.to_le_bytes());
        hasher.update(&(self.records_applied as u64).to_le_bytes());
        hasher.update(&(self.conflicts_resolved as u64).to_le_bytes());
        hasher.update(&self.responder_hlc.wall_time_ms.to_le_bytes());
        hasher.update(&self.responder_hlc.logical_counter.to_le_bytes());
        hasher.update(self.responder_hlc.node_id.as_bytes());
        hasher.finalize()
    }
}

/// Per-edge-peer synchronization state tracked by a `TapisaurusDB` server node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TapirSyncPeerStatus {
    pub edge_node_id: String,
    pub last_sequence: u64,
    pub last_hlc: TapirSyncHlc,
    pub total_bundles_pushed: u64,
    pub total_records_applied: u64,
    pub total_conflicts_resolved: u64,
    pub last_synced_at_ms: u64,
}

/// Overall cluster-wide `TapirSync` status telemetry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TapirSyncStatusSummary {
    pub server_node_id: String,
    pub server_hlc: TapirSyncHlc,
    pub server_export_sequence: u64,
    pub tracked_keys_count: usize,
    pub total_records_applied: u64,
    pub total_conflicts_resolved: u64,
    pub connected_edge_peers: Vec<TapirSyncPeerStatus>,
}

/// Thread-safe server coordinator managing `TapirSync` HLC watermarks, per-key LWW conflict
/// resolution, and edge peer telemetry across a `TapisaurusDB` instance.
pub struct TapirSyncRegistry {
    server_node_id: String,
    hlc: RwLock<TapirSyncHlc>,
    export_sequence: AtomicU64,
    /// Canonical key -> (content_crc32, winning_hlc)
    key_versions: DashMap<String, (u32, TapirSyncHlc)>,
    /// Edge node ID -> peer status
    peers: DashMap<String, TapirSyncPeerStatus>,
    total_applied: AtomicU64,
    total_conflicts: AtomicU64,
}

impl Default for TapirSyncRegistry {
    fn default() -> Self {
        Self::new("tapisaurus-cloud-primary")
    }
}

impl TapirSyncRegistry {
    pub fn new(server_node_id: impl Into<String>) -> Self {
        let id = server_node_id.into();
        let now_ms = Utc::now().timestamp_millis().max(0) as u64;
        Self {
            server_node_id: id.clone(),
            hlc: RwLock::new(TapirSyncHlc::new(id, now_ms)),
            export_sequence: AtomicU64::new(0),
            key_versions: DashMap::new(),
            peers: DashMap::new(),
            total_applied: AtomicU64::new(0),
            total_conflicts: AtomicU64::new(0),
        }
    }

    pub fn server_node_id(&self) -> &str {
        &self.server_node_id
    }

    /// Tick and return a fresh server HLC timestamp.
    pub fn tick_hlc(&self) -> TapirSyncHlc {
        let now_ms = Utc::now().timestamp_millis().max(0) as u64;
        self.hlc.write().tick(now_ms)
    }

    /// Observe an incoming remote HLC timestamp and advance the server clock.
    pub fn observe_hlc(&self, remote: &TapirSyncHlc) -> TapirSyncHlc {
        let now_ms = Utc::now().timestamp_millis().max(0) as u64;
        self.hlc.write().observe(remote, now_ms)
    }

    /// Allocate the next server export sequence number.
    pub fn next_export_sequence(&self) -> (u64, u64) {
        let prev = self.export_sequence.fetch_add(1, Ordering::SeqCst);
        (prev, prev + 1)
    }

    /// Evaluate whether `record` wins LWW conflict resolution against existing state.
    /// If `record` wins, updates `key_versions` and returns `true`.
    /// If an existing version has a strictly newer HLC, returns `false`.
    pub fn try_accept_record(&self, record: &TapirSyncRecord) -> bool {
        let key = record.canonical_key();
        let incoming_hlc = record.hlc().clone();
        self.observe_hlc(&incoming_hlc);

        if let Some(existing) = self.key_versions.get(&key) {
            if incoming_hlc < existing.value().1 {
                self.total_conflicts.fetch_add(1, Ordering::Relaxed);
                return false;
            }
        }
        let crc = if record.is_delete() {
            0
        } else {
            record.content_crc32()
        };
        self.key_versions.insert(key, (crc, incoming_hlc));
        self.total_applied.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Record completion of an edge bundle push and return a signed `TapirSyncAck`.
    pub fn finalize_bundle_push(
        &self,
        bundle: &TapirSyncBundle,
        records_applied: usize,
        conflicts_resolved: usize,
    ) -> TapirSyncAck {
        let now_ms = Utc::now().timestamp_millis().max(0) as u64;
        let responder_hlc = self.observe_hlc(&bundle.hlc);

        self.peers
            .entry(bundle.source_node_id.clone())
            .and_modify(|p| {
                p.last_sequence = p.last_sequence.max(bundle.target_sequence);
                p.last_hlc = bundle.hlc.clone();
                p.total_bundles_pushed += 1;
                p.total_records_applied += records_applied as u64;
                p.total_conflicts_resolved += conflicts_resolved as u64;
                p.last_synced_at_ms = now_ms;
            })
            .or_insert_with(|| TapirSyncPeerStatus {
                edge_node_id: bundle.source_node_id.clone(),
                last_sequence: bundle.target_sequence,
                last_hlc: bundle.hlc.clone(),
                total_bundles_pushed: 1,
                total_records_applied: records_applied as u64,
                total_conflicts_resolved: conflicts_resolved as u64,
                last_synced_at_ms: now_ms,
            });

        TapirSyncAck::new(
            true,
            self.server_node_id.clone(),
            bundle.source_node_id.clone(),
            bundle.target_sequence,
            records_applied,
            conflicts_resolved,
            responder_hlc,
        )
    }

    /// Return a snapshot of all `TapirSync` metrics and connected edge peer watermarks.
    pub fn status_summary(&self) -> TapirSyncStatusSummary {
        let mut peers: Vec<TapirSyncPeerStatus> =
            self.peers.iter().map(|e| e.value().clone()).collect();
        peers.sort_by(|a, b| a.edge_node_id.cmp(&b.edge_node_id));

        TapirSyncStatusSummary {
            server_node_id: self.server_node_id.clone(),
            server_hlc: self.hlc.read().clone(),
            server_export_sequence: self.export_sequence.load(Ordering::Relaxed),
            tracked_keys_count: self.key_versions.len(),
            total_records_applied: self.total_applied.load(Ordering::Relaxed),
            total_conflicts_resolved: self.total_conflicts.load(Ordering::Relaxed),
            connected_edge_peers: peers,
        }
    }
}
