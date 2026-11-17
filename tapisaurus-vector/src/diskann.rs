//! # DiskANN / Vamana SSD-Resident Hybrid Vector Engine (`TPDANN01`)
//!
//! Combines LSM-style in-memory hot mutations with an SSD-resident **Vamana (DiskANN)**
//! single-layer graph and in-memory 1-bit Binary Quantized (`BinaryQuantizedVector`) routing codes.
//!
//! ## Architectural Advantages (vs. LanceDB & In-Memory HNSW)
//! 1. **32x Smaller RAM Footprint:** Only 1-bit quantized codes and 64-bit disk offsets reside in RAM;
//!    full `f32` vectors and Vamana adjacency lists are fetched on-demand from the `.dann` page file.
//! 2. **α-RNG Vamana Graph Pruning (`alpha = 1.2`):** Builds long-range navigable edges in a single
//!    graph layer, minimizing SSD random I/O hops during beam search.
//! 3. **Instant OLTP Point Mutations:** Real-time `insert` and `delete` operations land in the hot
//!    delta buffer (`hot_buffer`) immediately and merge into the on-disk Vamana graph upon `flush_to_disk()`.

use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::distance::{
    cosine_distance, dot_product_distance, euclidean_distance, manhattan_distance, DistanceMetric,
};
use crate::hnsw::VectorSearchResult;
use crate::quantization::{BinaryQuantizedVector, BinaryQuantizer};

/// Magic header for TapisaurusDB DiskANN Vamana page files
pub const DISKANN_MAGIC: &[u8; 8] = b"TPDANN01";

fn compute_crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &b in bytes {
        crc ^= b as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

/// Configuration for the SSD-resident DiskANN / Vamana index
#[derive(Debug, Clone)]
pub struct DiskAnnConfig {
    pub dimensions: usize,
    pub metric: DistanceMetric,
    /// Maximum out-degree `R` per Vamana graph vertex (default: 16)
    pub max_degree: usize,
    /// Diversity parameter `alpha` for Vamana α-RNG edge pruning (default: 1.2)
    pub alpha: f32,
    /// Default search beam list size `L` (default: 32)
    pub beam_width: usize,
    /// Path to the on-disk `.dann` page file
    pub disk_path: PathBuf,
}

impl DiskAnnConfig {
    pub fn new(dimensions: usize, disk_path: impl Into<PathBuf>) -> Self {
        Self {
            dimensions,
            metric: DistanceMetric::Cosine,
            max_degree: 16,
            alpha: 1.2,
            beam_width: 32,
            disk_path: disk_path.into(),
        }
    }
}

/// Compact in-memory directory entry for a disk-resident Vamana vertex
#[derive(Debug, Clone)]
struct DiskNodeMeta {
    id: String,
    bq_code: BinaryQuantizedVector,
    file_offset: u64,
    record_len: u32,
}

/// Decoded on-disk Vamana vertex page record
#[derive(Debug, Clone)]
struct DiskNodeRecord {
    id: String,
    vector: Vec<f32>,
    neighbors: Vec<u32>,
}

#[derive(Clone, Copy, PartialEq)]
struct BeamCandidate {
    node_idx: u32,
    dist: f32,
}

impl Eq for BeamCandidate {}

impl PartialOrd for BeamCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BeamCandidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Min-heap ordering (smallest distance first)
        other
            .dist
            .partial_cmp(&self.dist)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Hybrid LSM + SSD-Resident Vamana (`DiskANN`) Vector Index
pub struct DiskAnnIndex {
    pub config: DiskAnnConfig,
    /// Hot in-memory buffer for real-time OLTP inserts/updates before compaction to disk
    hot_buffer: HashMap<String, Vec<f32>>,
    /// Tombstone set for deleted document IDs
    deleted: HashSet<String>,
    /// Compact RAM routing table (1-bit BQ code + disk offset per flushed node)
    disk_nodes: Vec<DiskNodeMeta>,
    /// Map from document ID to disk node index
    id_to_disk_idx: HashMap<String, u32>,
    /// Entry-point medoid vertex index in `disk_nodes`
    medoid_idx: Option<u32>,
}

impl DiskAnnIndex {
    /// Create a new DiskANN index backed by `config.disk_path`
    pub fn new(config: DiskAnnConfig) -> Self {
        Self {
            config,
            hot_buffer: HashMap::new(),
            deleted: HashSet::new(),
            disk_nodes: Vec::new(),
            id_to_disk_idx: HashMap::new(),
            medoid_idx: None,
        }
    }

    /// Insert or update a vector in the real-time hot buffer (O(1) OLTP mutation)
    pub fn insert(&mut self, id: impl Into<String>, vector: Vec<f32>) -> Result<(), String> {
        if vector.len() != self.config.dimensions {
            return Err(format!(
                "Vector dimension mismatch: expected {}, got {}",
                self.config.dimensions,
                vector.len()
            ));
        }
        let doc_id = id.into();
        self.deleted.remove(&doc_id);
        self.hot_buffer.insert(doc_id, vector);
        Ok(())
    }

    /// Mark a vector ID as deleted (tombstone)
    pub fn delete(&mut self, id: &str) -> bool {
        let removed_hot = self.hot_buffer.remove(id).is_some();
        let in_disk = self.id_to_disk_idx.contains_key(id);
        if in_disk {
            self.deleted.insert(id.to_string());
        }
        removed_hot || in_disk
    }

    /// Total active vectors across both hot buffer and SSD-resident Vamana graph
    pub fn len(&self) -> usize {
        let disk_active = self
            .disk_nodes
            .iter()
            .filter(|n| !self.deleted.contains(&n.id) && !self.hot_buffer.contains_key(&n.id))
            .count();
        disk_active + self.hot_buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Estimate resident RAM bytes consumed by the compact BQ routing table
    pub fn ram_routing_bytes(&self) -> usize {
        self.disk_nodes
            .iter()
            .map(|n| n.bq_code.memory_bytes() + n.id.len() + 16)
            .sum()
    }

    fn compute_distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self.config.metric {
            DistanceMetric::Cosine => cosine_distance(a, b),
            DistanceMetric::Euclidean => euclidean_distance(a, b),
            DistanceMetric::DotProduct => dot_product_distance(a, b),
            DistanceMetric::Manhattan => manhattan_distance(a, b),
        }
    }

    /// Compact all active vectors (merging existing disk nodes + `hot_buffer` minus `deleted`),
    /// build the α-RNG Vamana graph, and flush full vectors + neighbor lists to the `.dann` SSD file.
    pub fn flush_to_disk(&mut self) -> Result<usize, String> {
        // 1. Collect all active (id, vector) pairs
        let mut active_items: Vec<(String, Vec<f32>)> = Vec::new();
        for meta in &self.disk_nodes {
            if self.deleted.contains(&meta.id) || self.hot_buffer.contains_key(&meta.id) {
                continue;
            }
            if let Ok(rec) = self.read_disk_node_record(meta.file_offset, meta.record_len) {
                active_items.push((rec.id, rec.vector));
            }
        }
        for (id, vec) in self.hot_buffer.drain() {
            if !self.deleted.contains(&id) {
                active_items.push((id, vec));
            }
        }
        self.deleted.clear();

        let n = active_items.len();
        if n == 0 {
            self.disk_nodes.clear();
            self.id_to_disk_idx.clear();
            self.medoid_idx = None;
            return Ok(0);
        }

        // 2. Compute dataset centroid & find the medoid vertex closest to the centroid
        let dim = self.config.dimensions;
        let mut centroid = vec![0.0f32; dim];
        for (_, v) in &active_items {
            for (c, &x) in centroid.iter_mut().zip(v.iter()) {
                *c += x;
            }
        }
        let inv_n = 1.0 / (n as f32);
        for c in &mut centroid {
            *c *= inv_n;
        }

        let mut best_medoid = 0u32;
        let mut best_medoid_dist = f32::MAX;
        for (idx, (_, v)) in active_items.iter().enumerate() {
            let d = self.compute_distance(v, &centroid);
            if d < best_medoid_dist {
                best_medoid_dist = d;
                best_medoid = idx as u32;
            }
        }

        // 3. Build Vamana graph with α-RNG pruning (diversified long-range edges)
        let max_deg = self.config.max_degree.max(2);
        let alpha = self.config.alpha.max(1.0);
        let mut adjacency: Vec<Vec<u32>> = vec![Vec::new(); n];

        for u in 0..n {
            let mut candidates: Vec<(u32, f32)> = (0..n)
                .filter(|&v| v != u)
                .map(|v| {
                    let d = self.compute_distance(&active_items[u].1, &active_items[v].1);
                    (v as u32, d)
                })
                .collect();
            candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

            let mut pruned_neighbors: Vec<u32> = Vec::with_capacity(max_deg);
            for &(cand_idx, dist_u_cand) in &candidates {
                if pruned_neighbors.len() >= max_deg {
                    break;
                }
                let mut occluded = false;
                for &existing_nbr in &pruned_neighbors {
                    let dist_nbr_cand = self.compute_distance(
                        &active_items[existing_nbr as usize].1,
                        &active_items[cand_idx as usize].1,
                    );
                    if alpha * dist_nbr_cand <= dist_u_cand {
                        occluded = true;
                        break;
                    }
                }
                if !occluded {
                    pruned_neighbors.push(cand_idx);
                }
            }

            // Fill remaining slots with closest unpicked candidates if α-pruning was strict
            if pruned_neighbors.len() < max_deg.min(n.saturating_sub(1)) {
                for &(cand_idx, _) in &candidates {
                    if pruned_neighbors.len() >= max_deg {
                        break;
                    }
                    if !pruned_neighbors.contains(&cand_idx) {
                        pruned_neighbors.push(cand_idx);
                    }
                }
            }

            adjacency[u] = pruned_neighbors;
        }

        // 4. Write framed `TPDANN01` page file to disk and build compact BQ directory in RAM
        if let Some(parent) = self.config.disk_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let mut file = File::create(&self.config.disk_path)
            .map_err(|e| format!("Failed to create DiskANN file: {e}"))?;

        // Header: [TPDANN01 (8B)][version: u32 (1)][dim: u32][node_count: u32][medoid_idx: u32]
        file.write_all(DISKANN_MAGIC)
            .map_err(|e| format!("Failed to write header: {e}"))?;
        file.write_all(&1u32.to_le_bytes()).unwrap();
        file.write_all(&(dim as u32).to_le_bytes()).unwrap();
        file.write_all(&(n as u32).to_le_bytes()).unwrap();
        file.write_all(&best_medoid.to_le_bytes()).unwrap();

        let mut current_offset = 24u64;
        let mut new_disk_nodes = Vec::with_capacity(n);
        let mut new_id_map = HashMap::with_capacity(n);

        for (idx, (doc_id, vec)) in active_items.into_iter().enumerate() {
            let bq_code = BinaryQuantizer::quantize(&vec);
            let nbrs = &adjacency[idx];

            let id_bytes = doc_id.as_bytes();
            let id_len = id_bytes.len() as u16;
            let nbr_count = nbrs.len() as u16;

            let mut payload = Vec::with_capacity(2 + id_bytes.len() + dim * 4 + 2 + nbrs.len() * 4);
            payload.extend_from_slice(&id_len.to_le_bytes());
            payload.extend_from_slice(id_bytes);
            for &val in &vec {
                payload.extend_from_slice(&val.to_le_bytes());
            }
            payload.extend_from_slice(&nbr_count.to_le_bytes());
            for &nbr in nbrs {
                payload.extend_from_slice(&nbr.to_le_bytes());
            }

            let crc = compute_crc32(&payload);

            let record_len = (payload.len() + 4) as u32;
            file.write_all(&payload)
                .map_err(|e| format!("Failed to write DiskANN node payload: {e}"))?;
            file.write_all(&crc.to_le_bytes())
                .map_err(|e| format!("Failed to write DiskANN node CRC: {e}"))?;

            new_id_map.insert(doc_id.clone(), idx as u32);
            new_disk_nodes.push(DiskNodeMeta {
                id: doc_id,
                bq_code,
                file_offset: current_offset,
                record_len,
            });
            current_offset += record_len as u64;
        }

        file.sync_data()
            .map_err(|e| format!("Failed to sync DiskANN file: {e}"))?;

        self.disk_nodes = new_disk_nodes;
        self.id_to_disk_idx = new_id_map;
        self.medoid_idx = Some(best_medoid);
        Ok(n)
    }

    fn read_disk_node_record(
        &self,
        offset: u64,
        record_len: u32,
    ) -> Result<DiskNodeRecord, String> {
        let mut file = File::open(&self.config.disk_path)
            .map_err(|e| format!("Failed to open DiskANN file: {e}"))?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| format!("Failed to seek DiskANN file: {e}"))?;

        let mut buf = vec![0u8; record_len as usize];
        file.read_exact(&mut buf)
            .map_err(|e| format!("Failed to read DiskANN node record: {e}"))?;

        if buf.len() < 8 {
            return Err("Corrupted DiskANN node record (too short)".into());
        }
        let payload_len = buf.len() - 4;
        let payload = &buf[..payload_len];
        let mut crc_bytes = [0u8; 4];
        crc_bytes.copy_from_slice(&buf[payload_len..]);
        let expected_crc = u32::from_le_bytes(crc_bytes);

        if compute_crc32(payload) != expected_crc {
            return Err("DiskANN node record CRC32 mismatch".into());
        }

        let id_len = u16::from_le_bytes([payload[0], payload[1]]) as usize;
        let mut cursor = 2;
        let id = String::from_utf8(payload[cursor..cursor + id_len].to_vec())
            .map_err(|e| format!("Invalid UTF-8 doc ID in DiskANN record: {e}"))?;
        cursor += id_len;

        let dim = self.config.dimensions;
        let mut vector = Vec::with_capacity(dim);
        for _ in 0..dim {
            let mut f_bytes = [0u8; 4];
            f_bytes.copy_from_slice(&payload[cursor..cursor + 4]);
            vector.push(f32::from_le_bytes(f_bytes));
            cursor += 4;
        }

        let nbr_count = u16::from_le_bytes([payload[cursor], payload[cursor + 1]]) as usize;
        cursor += 2;
        let mut neighbors = Vec::with_capacity(nbr_count);
        for _ in 0..nbr_count {
            let mut n_bytes = [0u8; 4];
            n_bytes.copy_from_slice(&payload[cursor..cursor + 4]);
            neighbors.push(u32::from_le_bytes(n_bytes));
            cursor += 4;
        }

        Ok(DiskNodeRecord {
            id,
            vector,
            neighbors,
        })
    }

    /// Execute hybrid SSD-backed Vamana beam search + hot buffer merge.
    /// Returns `(results, disk_pages_fetched)`.
    pub fn search_with_stats(
        &self,
        query: &[f32],
        top_k: usize,
    ) -> Result<(Vec<VectorSearchResult>, usize), String> {
        if query.len() != self.config.dimensions {
            return Err(format!(
                "Query dimension mismatch: expected {}, got {}",
                self.config.dimensions,
                query.len()
            ));
        }

        let mut results_map: HashMap<String, f32> = HashMap::new();
        let mut disk_pages_read = 0usize;

        // 1. Search SSD-resident Vamana graph using BQ routing + on-demand disk page reads
        if let Some(medoid) = self.medoid_idx {
            if !self.disk_nodes.is_empty() {
                let query_bq = BinaryQuantizer::quantize(query);
                let beam_l = self.config.beam_width.max(top_k * 2).max(8);

                let mut visited: HashSet<u32> = HashSet::new();
                let mut frontier: BinaryHeap<BeamCandidate> = BinaryHeap::new();

                let medoid_meta = &self.disk_nodes[medoid as usize];
                let approx_d0 =
                    BinaryQuantizer::normalized_hamming_distance(&query_bq, &medoid_meta.bq_code);
                frontier.push(BeamCandidate {
                    node_idx: medoid,
                    dist: approx_d0,
                });

                while let Some(cand) = frontier.pop() {
                    if visited.len() >= beam_l {
                        break;
                    }
                    if !visited.insert(cand.node_idx) {
                        continue;
                    }

                    let meta = &self.disk_nodes[cand.node_idx as usize];
                    let rec = self.read_disk_node_record(meta.file_offset, meta.record_len)?;
                    disk_pages_read += 1;

                    if !self.deleted.contains(&rec.id) && !self.hot_buffer.contains_key(&rec.id) {
                        let exact_dist = self.compute_distance(query, &rec.vector);
                        results_map.insert(rec.id, exact_dist);
                    }

                    for &nbr_idx in &rec.neighbors {
                        if !visited.contains(&nbr_idx) && (nbr_idx as usize) < self.disk_nodes.len()
                        {
                            let nbr_meta = &self.disk_nodes[nbr_idx as usize];
                            let approx_dist = BinaryQuantizer::normalized_hamming_distance(
                                &query_bq,
                                &nbr_meta.bq_code,
                            );
                            frontier.push(BeamCandidate {
                                node_idx: nbr_idx,
                                dist: approx_dist,
                            });
                        }
                    }
                }
            }
        }

        // 2. Merge real-time hot buffer vectors
        for (id, vec) in &self.hot_buffer {
            if !self.deleted.contains(id) {
                let exact_dist = self.compute_distance(query, vec);
                results_map.insert(id.clone(), exact_dist);
            }
        }

        let mut scored: Vec<VectorSearchResult> = results_map
            .into_iter()
            .map(|(id, distance)| VectorSearchResult {
                id,
                distance,
                similarity: 1.0 / (1.0 + distance.max(0.0)),
            })
            .collect();
        scored.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        scored.truncate(top_k);

        Ok((scored, disk_pages_read))
    }

    /// Convenience search returning the top-K nearest neighbors
    pub fn search(&self, query: &[f32], top_k: usize) -> Result<Vec<VectorSearchResult>, String> {
        self.search_with_stats(query, top_k).map(|(res, _)| res)
    }

    /// Recover a `DiskAnnIndex` from an existing `.dann` file on disk
    pub fn open_from_disk(config: DiskAnnConfig) -> Result<Self, String> {
        let path: &Path = &config.disk_path;
        if !path.exists() {
            return Ok(Self::new(config));
        }

        let raw = std::fs::read(path).map_err(|e| format!("Failed to read DiskANN file: {e}"))?;
        if raw.len() < 24 || &raw[..8] != DISKANN_MAGIC {
            return Err("Invalid TPDANN01 magic header".into());
        }

        let dim = u32::from_le_bytes([raw[12], raw[13], raw[14], raw[15]]) as usize;
        let node_count = u32::from_le_bytes([raw[16], raw[17], raw[18], raw[19]]) as usize;
        let medoid_idx = u32::from_le_bytes([raw[20], raw[21], raw[22], raw[23]]);

        let mut idx = Self::new(DiskAnnConfig {
            dimensions: dim,
            ..config
        });

        let mut cursor = 24usize;
        for i in 0..node_count {
            let start_offset = cursor as u64;
            let id_len = u16::from_le_bytes([raw[cursor], raw[cursor + 1]]) as usize;
            cursor += 2;
            let id = String::from_utf8(raw[cursor..cursor + id_len].to_vec())
                .map_err(|e| format!("Invalid doc ID in DiskANN file: {e}"))?;
            cursor += id_len;

            let mut vec = Vec::with_capacity(dim);
            for _ in 0..dim {
                let mut fb = [0u8; 4];
                fb.copy_from_slice(&raw[cursor..cursor + 4]);
                vec.push(f32::from_le_bytes(fb));
                cursor += 4;
            }
            let nbr_count = u16::from_le_bytes([raw[cursor], raw[cursor + 1]]) as usize;
            cursor += 2 + nbr_count * 4 + 4; // skip neighbors + crc32

            let record_len = (cursor as u64 - start_offset) as u32;
            let bq_code = BinaryQuantizer::quantize(&vec);
            idx.id_to_disk_idx.insert(id.clone(), i as u32);
            idx.disk_nodes.push(DiskNodeMeta {
                id,
                bq_code,
                file_offset: start_offset,
                record_len,
            });
        }

        idx.medoid_idx = if node_count > 0 {
            Some(medoid_idx)
        } else {
            None
        };
        Ok(idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_diskann_vamana_ssd_search_and_hot_oltp_mutations() {
        let dir = tempfile::tempdir().unwrap();
        let disk_path = dir.path().join("vectors_vamana.dann");
        let config = DiskAnnConfig {
            dimensions: 8,
            metric: DistanceMetric::Cosine,
            max_degree: 6,
            alpha: 1.2,
            beam_width: 12,
            disk_path: disk_path.clone(),
        };

        let mut index = DiskAnnIndex::new(config.clone());

        // Insert 40 vectors into hot buffer and flush to Vamana SSD file
        for i in 0..40 {
            let angle = (i as f32) * 0.15;
            let v = vec![
                angle.cos(),
                angle.sin(),
                (angle * 0.5).cos(),
                (angle * 0.5).sin(),
                0.1,
                0.2,
                0.3,
                0.4,
            ];
            index.insert(format!("vec_{i}"), v).unwrap();
        }

        let flushed = index.flush_to_disk().unwrap();
        assert_eq!(flushed, 40);
        assert!(disk_path.exists());
        assert!(index.ram_routing_bytes() > 0);

        // Insert a real-time hot vector without flushing (testing hybrid OLTP + SSD search)
        let target_vec = vec![1.0, 0.0, 1.0, 0.0, 0.1, 0.2, 0.3, 0.4];
        index
            .insert("hot_realtime_vec", target_vec.clone())
            .unwrap();

        let (results, pages_fetched) = index.search_with_stats(&target_vec, 5).unwrap();
        assert_eq!(results.len(), 5);
        assert!(
            pages_fetched > 0 && pages_fetched <= 12,
            "Beam search must only read bounded pages from disk (read {pages_fetched} of 40)"
        );
        assert!(results
            .iter()
            .any(|r| r.id == "hot_realtime_vec" || r.id == "vec_0"));

        // Re-open from disk and verify persistence
        let recovered = DiskAnnIndex::open_from_disk(config).unwrap();
        assert_eq!(recovered.len(), 40);
        let rec_results = recovered.search(&target_vec, 3).unwrap();
        assert_eq!(rec_results[0].id, "vec_0");
    }
}
