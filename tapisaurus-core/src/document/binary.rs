//! # Slotted Binary Document Format (`TPBIN001`) & Clock-Pro Hot-Cold Cache
//!
//! Eliminates JSON serialization/deserialization bottlenecks in `TapisaurusDB` by encoding
//! documents into a deterministic binary layout with a **sorted 16-byte field offset directory**.
//!
//! ## Key Capabilities
//! 1. **O(log F) Zero-Deserialization Field Lookup**: Binary searches the sorted field directory
//!    and reads target field bytes directly from `val_offset..val_offset + val_len` without
//!    parsing other fields or allocating a `Document` (`BTreeMap<String, Value>`).
//! 2. **Zero-Copy Filter Evaluation**: Evaluates `WHERE field = value` predicates directly on raw
//!    `TPBIN001` slices from LSM SSTables/MemTables, decoding full `Document` structs only for
//!    matching rows.
//! 3. **Zero-Copy Field Projection**: Materializes partial `Document` instances containing only
//!    requested `SELECT` columns directly from binary offsets.
//! 4. **Clock-Pro / Second-Chance Hot Cache Eviction**: Protects frequently accessed documents
//!    in RAM while evicting cold documents to LSM storage.

use chrono::{TimeZone, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use uuid::Uuid;

use super::model::{Document, DocumentId, DocumentMetadata, Value};
use crate::error::{TapisaurusError, TapisaurusResult};

/// 8-byte magic header for Slotted Binary Documents (`TPBIN001`).
pub const SLOTTED_BINARY_MAGIC: &[u8; 8] = b"TPBIN001";

/// Current `TPBIN001` binary layout version.
pub const SLOTTED_BINARY_VERSION: u16 = 1;

const FLAG_HAS_META: u16 = 0x0001;
const FIXED_HEADER_LEN: usize = 24;
const DIR_SLOT_LEN: usize = 16;

const TAG_NULL: u8 = 0;
const TAG_BOOL: u8 = 1;
const TAG_INT64: u8 = 2;
const TAG_FLOAT64: u8 = 3;
const TAG_STRING: u8 = 4;
const TAG_BINARY: u8 = 5;
const TAG_VECTOR_F32: u8 = 6;
const TAG_DATETIME_NS: u8 = 7;
const TAG_UUID: u8 = 8;
const TAG_JSON_COMPLEX: u8 = 9;

/// Telemetry for the Tiered Hot-Cold Document Cache and `TPBIN001` zero-copy operations.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HotColdCacheStats {
    /// Number of reads satisfied directly from the in-memory hot cache.
    pub hot_hits: u64,
    /// Number of reads fetched and decoded from cold LSM storage.
    pub cold_hits: u64,
    /// Number of cold `TPBIN001` records evaluated via zero-deserialization filter pushdown.
    pub zero_copy_filter_evals: u64,
    /// Number of cold `TPBIN001` records projected via zero-deserialization field offsets.
    pub zero_copy_projections: u64,
    /// Number of cold documents evicted from RAM by the Clock-Pro / Second-Chance sweeper.
    pub clock_evictions: u64,
}

/// Clock-Pro / Second-Chance reference tracker for hot-cold document residency.
pub struct HotColdCacheTracker {
    /// Document ID -> reference counter in `0..=3`
    ref_bits: DashMap<String, AtomicU8>,
    pub hot_hits: AtomicU64,
    pub cold_hits: AtomicU64,
    pub zero_copy_filter_evals: AtomicU64,
    pub zero_copy_projections: AtomicU64,
    pub clock_evictions: AtomicU64,
}

impl Default for HotColdCacheTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl HotColdCacheTracker {
    pub fn new() -> Self {
        Self {
            ref_bits: DashMap::new(),
            hot_hits: AtomicU64::new(0),
            cold_hits: AtomicU64::new(0),
            zero_copy_filter_evals: AtomicU64::new(0),
            zero_copy_projections: AtomicU64::new(0),
            clock_evictions: AtomicU64::new(0),
        }
    }

    /// Record an access to `doc_id`, promoting its hotness score up to `3`.
    #[inline]
    pub fn touch(&self, doc_id: &str) {
        if let Some(entry) = self.ref_bits.get(doc_id) {
            let cur = entry.load(Ordering::Relaxed);
            if cur < 3 {
                entry.store(cur + 1, Ordering::Relaxed);
            }
            return;
        }
        self.ref_bits.insert(doc_id.to_string(), AtomicU8::new(1));
    }

    /// Remove tracking state when a document is deleted or evicted.
    #[inline]
    pub fn remove(&self, doc_id: &str) {
        self.ref_bits.remove(doc_id);
    }

    pub fn clear(&self) {
        self.ref_bits.clear();
    }

    /// Select a cold victim key from `documents` using Clock-Pro / Second-Chance decay.
    /// Documents with `ref_bits > 0` have their reference count decremented and survive the sweep.
    pub fn select_victim_and_evict(
        &self,
        documents: &DashMap<String, Document>,
        keep_id: Option<&str>,
    ) -> bool {
        // Pass 1: decay hot entries and look for an immediate cold entry (ref_bit == 0)
        let mut fallback_key: Option<String> = None;
        let mut cold_key: Option<String> = None;

        for entry in documents.iter() {
            let k = entry.key();
            if keep_id.is_some_and(|kid| k == kid) {
                continue;
            }
            if fallback_key.is_none() {
                fallback_key = Some(k.clone());
            }
            if let Some(rb) = self.ref_bits.get(k) {
                let score = rb.load(Ordering::Relaxed);
                if score == 0 {
                    cold_key = Some(k.clone());
                    break;
                } else {
                    rb.store(score - 1, Ordering::Relaxed);
                }
            } else {
                cold_key = Some(k.clone());
                break;
            }
        }

        let target = cold_key.or(fallback_key);
        if let Some(victim) = target {
            documents.remove(&victim);
            self.ref_bits.remove(&victim);
            self.clock_evictions.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    pub fn stats(&self) -> HotColdCacheStats {
        HotColdCacheStats {
            hot_hits: self.hot_hits.load(Ordering::Relaxed),
            cold_hits: self.cold_hits.load(Ordering::Relaxed),
            zero_copy_filter_evals: self.zero_copy_filter_evals.load(Ordering::Relaxed),
            zero_copy_projections: self.zero_copy_projections.load(Ordering::Relaxed),
            clock_evictions: self.clock_evictions.load(Ordering::Relaxed),
        }
    }
}

/// Encoder, decoder, and zero-deserialization field projector for `TPBIN001` Slotted Binary Documents.
pub struct SlottedBinaryCodec;

impl SlottedBinaryCodec {
    /// Encode a [`Document`] into a compact `TPBIN001` slotted binary byte vector.
    ///
    /// Layout:
    /// - `[0..8]`: `b"TPBIN001"`
    /// - `[8..10]`: `version` (`u16` LE = 1)
    /// - `[10..12]`: `flags` (`u16` LE, bit 0 = has `_meta`)
    /// - `[12..16]`: `id_len` (`u32` LE)
    /// - `[16..20]`: `field_count` (`u32` LE)
    /// - `[20..24]`: `meta_offset` (`u32` LE, 0 if none)
    /// - `[24..24 + id_len]`: UTF-8 `_id` bytes
    /// - `[dir_start..dir_start + field_count * 16]`: Sorted 16-byte field directory slots:
    ///   `(name_offset: u32, name_len: u16, type_tag: u8, reserved: u8, val_offset: u32, val_len: u32)`
    /// - `[heap_start..]`: Contiguous field name & binary value heap + optional 24-byte metadata trailer
    pub fn encode(doc: &Document) -> TapisaurusResult<Vec<u8>> {
        let id_bytes = doc.id.as_str().as_bytes();
        let id_len = id_bytes.len();
        let field_count = doc.fields.len();
        let dir_start = FIXED_HEADER_LEN + id_len;
        let heap_start = dir_start + field_count * DIR_SLOT_LEN;

        let mut buf = Vec::with_capacity(heap_start + field_count * 24 + 32);
        let flags = if doc.metadata.is_some() {
            FLAG_HAS_META
        } else {
            0
        };

        buf.extend_from_slice(SLOTTED_BINARY_MAGIC);
        buf.extend_from_slice(&SLOTTED_BINARY_VERSION.to_le_bytes());
        buf.extend_from_slice(&flags.to_le_bytes());
        buf.extend_from_slice(&(id_len as u32).to_le_bytes());
        buf.extend_from_slice(&(field_count as u32).to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes()); // meta_offset placeholder
        buf.extend_from_slice(id_bytes);

        // Reserve directory space
        buf.resize(heap_start, 0);

        for (slot_idx, (name, val)) in doc.fields.iter().enumerate() {
            let name_bytes = name.as_bytes();
            let name_len = u16::try_from(name_bytes.len()).map_err(|_| {
                TapisaurusError::InvalidDocument("Field name exceeds 64 KiB limit".into())
            })?;
            let name_offset = buf.len() as u32;
            buf.extend_from_slice(name_bytes);

            let val_offset = buf.len() as u32;
            let type_tag = encode_value_payload(&mut buf, val)?;
            let val_len = (buf.len() as u32) - val_offset;

            let slot_off = dir_start + slot_idx * DIR_SLOT_LEN;
            buf[slot_off..slot_off + 4].copy_from_slice(&name_offset.to_le_bytes());
            buf[slot_off + 4..slot_off + 6].copy_from_slice(&name_len.to_le_bytes());
            buf[slot_off + 6] = type_tag;
            buf[slot_off + 7] = 0;
            buf[slot_off + 8..slot_off + 12].copy_from_slice(&val_offset.to_le_bytes());
            buf[slot_off + 12..slot_off + 16].copy_from_slice(&val_len.to_le_bytes());
        }

        if let Some(ref meta) = doc.metadata {
            let meta_offset = buf.len() as u32;
            buf[20..24].copy_from_slice(&meta_offset.to_le_bytes());
            buf.extend_from_slice(&meta.version.to_le_bytes());
            buf.extend_from_slice(
                &meta
                    .created_at
                    .timestamp_nanos_opt()
                    .unwrap_or(0)
                    .to_le_bytes(),
            );
            buf.extend_from_slice(
                &meta
                    .updated_at
                    .timestamp_nanos_opt()
                    .unwrap_or(0)
                    .to_le_bytes(),
            );
            if let Some(ref col) = meta.collection {
                let col_b = col.as_bytes();
                buf.extend_from_slice(&(col_b.len() as u16).to_le_bytes());
                buf.extend_from_slice(col_b);
            } else {
                buf.extend_from_slice(&0u16.to_le_bytes());
            }
        }

        Ok(buf)
    }

    /// Decode a [`Document`] from either `TPBIN001` slotted binary bytes or legacy UTF-8 JSON bytes.
    pub fn decode_any(bytes: &[u8]) -> TapisaurusResult<Document> {
        if bytes.starts_with(SLOTTED_BINARY_MAGIC) {
            Self::decode(bytes)
        } else {
            Ok(serde_json::from_slice::<Document>(bytes)?)
        }
    }

    /// Decode a full [`Document`] from a `TPBIN001` buffer.
    pub fn decode(bytes: &[u8]) -> TapisaurusResult<Document> {
        let (flags, id_str, field_count, meta_offset, dir_start) = parse_header(bytes)?;
        let mut fields = BTreeMap::new();

        for i in 0..field_count {
            let slot_off = dir_start + i * DIR_SLOT_LEN;
            let (name, type_tag, val_slice) = read_slot(bytes, slot_off)?;
            let val = decode_value_payload(type_tag, val_slice)?;
            fields.insert(name.to_string(), val);
        }

        let metadata = if (flags & FLAG_HAS_META) != 0 && meta_offset > 0 {
            decode_metadata(bytes, meta_offset as usize).ok()
        } else {
            None
        };

        Ok(Document {
            id: DocumentId::from_string(id_str),
            fields,
            metadata,
        })
    }

    /// Extract a single top-level field from a `TPBIN001` buffer in **O(log F)** time using
    /// binary search over the 16-byte directory slots, without deserializing any other field!
    pub fn extract_field(bytes: &[u8], target_field: &str) -> TapisaurusResult<Option<Value>> {
        if !bytes.starts_with(SLOTTED_BINARY_MAGIC) {
            let doc: Document = serde_json::from_slice(bytes)?;
            if target_field == "_id" || target_field == "id" {
                return Ok(Some(Value::String(doc.id.as_str().to_string())));
            }
            return Ok(doc.get_nested(target_field).cloned());
        }

        let (_flags, id_str, field_count, _meta_offset, dir_start) = parse_header(bytes)?;
        if target_field == "_id" || target_field == "id" {
            return Ok(Some(Value::String(id_str.to_string())));
        }

        // If nested dot-notation is requested (e.g. "profile.city"), binary search the root key first
        if let Some((root_key, sub_path)) = target_field.split_once('.') {
            if let Some(root_val) = binary_search_field(bytes, dir_start, field_count, root_key)? {
                return Ok(extract_subpath(&root_val, sub_path));
            }
            return Ok(None);
        }

        binary_search_field(bytes, dir_start, field_count, target_field)
    }

    /// Evaluate a slice of `(field, expected_value)` equality predicates directly against a raw
    /// `TPBIN001` byte slice **without deserializing the `Document`**.
    pub fn matches_filter(bytes: &[u8], filter: &[(String, Value)]) -> bool {
        if filter.is_empty() {
            return true;
        }
        if !bytes.starts_with(SLOTTED_BINARY_MAGIC) {
            return serde_json::from_slice::<Document>(bytes)
                .map(|doc| super::collection::Collection::matches_filter(&doc, filter))
                .unwrap_or(false);
        }

        for (key, expected) in filter {
            match Self::extract_field(bytes, key) {
                Ok(Some(ref actual)) if values_equal(actual, expected) => continue,
                _ => return false,
            }
        }
        true
    }

    /// Materialize a projected [`Document`] containing ONLY `_id` and the specified `fields`
    /// directly from `TPBIN001` binary offsets without decoding unrequested columns.
    pub fn project_fields(bytes: &[u8], fields: &[&str]) -> TapisaurusResult<Document> {
        if !bytes.starts_with(SLOTTED_BINARY_MAGIC) {
            let mut doc: Document = serde_json::from_slice(bytes)?;
            doc.fields.retain(|k, _| fields.contains(&k.as_str()));
            return Ok(doc);
        }

        let (_flags, id_str, field_count, _meta_offset, dir_start) = parse_header(bytes)?;
        let mut projected = BTreeMap::new();

        for &req_field in fields {
            if req_field == "_id" || req_field == "id" {
                continue;
            }
            if let Some(val) = binary_search_field(bytes, dir_start, field_count, req_field)? {
                projected.insert(req_field.to_string(), val);
            }
        }

        Ok(Document {
            id: DocumentId::from_string(id_str),
            fields: projected,
            metadata: None,
        })
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Integer(x), Value::Float(y)) | (Value::Float(y), Value::Integer(x)) => {
            (*x as f64) == *y
        }
        _ => a == b,
    }
}

fn extract_subpath(val: &Value, path: &str) -> Option<Value> {
    let mut curr = val;
    for part in path.split('.') {
        match curr {
            Value::Object(map) => curr = map.get(part)?,
            _ => return None,
        }
    }
    Some(curr.clone())
}

fn parse_header(bytes: &[u8]) -> TapisaurusResult<(u16, &str, usize, u32, usize)> {
    if bytes.len() < FIXED_HEADER_LEN || !bytes.starts_with(SLOTTED_BINARY_MAGIC) {
        return Err(TapisaurusError::InvalidMagicBytes);
    }
    let version = u16::from_le_bytes([bytes[8], bytes[9]]);
    if version != SLOTTED_BINARY_VERSION {
        return Err(TapisaurusError::InvalidDocument(format!(
            "Unsupported TPBIN001 version: {version}"
        )));
    }
    let flags = u16::from_le_bytes([bytes[10], bytes[11]]);
    let id_len = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
    let field_count = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
    let meta_offset = u32::from_le_bytes(bytes[20..24].try_into().unwrap());

    let dir_start = FIXED_HEADER_LEN
        .checked_add(id_len)
        .ok_or_else(|| TapisaurusError::InvalidDocument("TPBIN001 header overflow".into()))?;
    let heap_start = dir_start
        .checked_add(field_count.saturating_mul(DIR_SLOT_LEN))
        .ok_or_else(|| TapisaurusError::InvalidDocument("TPBIN001 directory overflow".into()))?;

    if bytes.len() < heap_start {
        return Err(TapisaurusError::InvalidDocument(
            "Truncated TPBIN001 directory".into(),
        ));
    }

    let id_str = std::str::from_utf8(&bytes[FIXED_HEADER_LEN..dir_start])
        .map_err(|e| TapisaurusError::InvalidDocument(format!("Invalid UTF-8 _id: {e}")))?;

    Ok((flags, id_str, field_count, meta_offset, dir_start))
}

#[inline]
fn read_slot(bytes: &[u8], slot_off: usize) -> TapisaurusResult<(&str, u8, &[u8])> {
    let slot = bytes
        .get(slot_off..slot_off + DIR_SLOT_LEN)
        .ok_or_else(|| TapisaurusError::InvalidDocument("Truncated TPBIN001 slot".into()))?;
    let name_offset = u32::from_le_bytes([slot[0], slot[1], slot[2], slot[3]]) as usize;
    let name_len = u16::from_le_bytes([slot[4], slot[5]]) as usize;
    let type_tag = slot[6];
    let val_offset = u32::from_le_bytes([slot[8], slot[9], slot[10], slot[11]]) as usize;
    let val_len = u32::from_le_bytes([slot[12], slot[13], slot[14], slot[15]]) as usize;

    let name_bytes = bytes
        .get(name_offset..name_offset.saturating_add(name_len))
        .ok_or_else(|| {
            TapisaurusError::InvalidDocument("TPBIN001 field name out of bounds".into())
        })?;
    let name = std::str::from_utf8(name_bytes)
        .map_err(|e| TapisaurusError::InvalidDocument(format!("Invalid UTF-8 field name: {e}")))?;
    let val_slice = bytes
        .get(val_offset..val_offset.saturating_add(val_len))
        .ok_or_else(|| {
            TapisaurusError::InvalidDocument("TPBIN001 field value out of bounds".into())
        })?;

    Ok((name, type_tag, val_slice))
}

fn binary_search_field(
    bytes: &[u8],
    dir_start: usize,
    field_count: usize,
    target: &str,
) -> TapisaurusResult<Option<Value>> {
    let mut low = 0usize;
    let mut high = field_count;
    while low < high {
        let mid = low + (high - low) / 2;
        let slot_off = dir_start + mid * DIR_SLOT_LEN;
        let (name, type_tag, val_slice) = read_slot(bytes, slot_off)?;
        match name.cmp(target) {
            std::cmp::Ordering::Equal => {
                return Ok(Some(decode_value_payload(type_tag, val_slice)?));
            }
            std::cmp::Ordering::Less => low = mid + 1,
            std::cmp::Ordering::Greater => high = mid,
        }
    }
    Ok(None)
}

fn encode_value_payload(buf: &mut Vec<u8>, val: &Value) -> TapisaurusResult<u8> {
    match val {
        Value::Null => Ok(TAG_NULL),
        Value::Boolean(b) => {
            buf.push(u8::from(*b));
            Ok(TAG_BOOL)
        }
        Value::Integer(i) => {
            buf.extend_from_slice(&i.to_le_bytes());
            Ok(TAG_INT64)
        }
        Value::Float(f) => {
            buf.extend_from_slice(&f.to_le_bytes());
            Ok(TAG_FLOAT64)
        }
        Value::String(s) => {
            buf.extend_from_slice(s.as_bytes());
            Ok(TAG_STRING)
        }
        Value::Binary(b) => {
            buf.extend_from_slice(b);
            Ok(TAG_BINARY)
        }
        Value::Vector(vec) => {
            for f in vec {
                buf.extend_from_slice(&f.to_le_bytes());
            }
            Ok(TAG_VECTOR_F32)
        }
        Value::DateTime(dt) => {
            let nanos = dt.timestamp_nanos_opt().unwrap_or(0);
            buf.extend_from_slice(&nanos.to_le_bytes());
            Ok(TAG_DATETIME_NS)
        }
        Value::Uuid(u) => {
            buf.extend_from_slice(u.as_bytes());
            Ok(TAG_UUID)
        }
        Value::Array(_) | Value::Object(_) => {
            let json_bytes = serde_json::to_vec(val)?;
            buf.extend_from_slice(&json_bytes);
            Ok(TAG_JSON_COMPLEX)
        }
    }
}

fn decode_value_payload(type_tag: u8, slice: &[u8]) -> TapisaurusResult<Value> {
    match type_tag {
        TAG_NULL => Ok(Value::Null),
        TAG_BOOL => {
            let b = slice.first().copied().unwrap_or(0) != 0;
            Ok(Value::Boolean(b))
        }
        TAG_INT64 => {
            let arr: [u8; 8] = slice
                .try_into()
                .map_err(|_| TapisaurusError::InvalidDocument("Invalid TAG_INT64 length".into()))?;
            Ok(Value::Integer(i64::from_le_bytes(arr)))
        }
        TAG_FLOAT64 => {
            let arr: [u8; 8] = slice.try_into().map_err(|_| {
                TapisaurusError::InvalidDocument("Invalid TAG_FLOAT64 length".into())
            })?;
            Ok(Value::Float(f64::from_le_bytes(arr)))
        }
        TAG_STRING => {
            let s = std::str::from_utf8(slice).map_err(|e| {
                TapisaurusError::InvalidDocument(format!("Invalid UTF-8 string value: {e}"))
            })?;
            Ok(Value::String(s.to_string()))
        }
        TAG_BINARY => Ok(Value::Binary(slice.to_vec())),
        TAG_VECTOR_F32 => {
            if !slice.len().is_multiple_of(4) {
                return Err(TapisaurusError::InvalidDocument(
                    "Invalid TAG_VECTOR_F32 byte length".into(),
                ));
            }
            let (chunks, _) = slice.as_chunks::<4>();
            let vec = chunks.iter().map(|&c| f32::from_le_bytes(c)).collect();
            Ok(Value::Vector(vec))
        }
        TAG_DATETIME_NS => {
            let arr: [u8; 8] = slice.try_into().map_err(|_| {
                TapisaurusError::InvalidDocument("Invalid TAG_DATETIME_NS length".into())
            })?;
            let nanos = i64::from_le_bytes(arr);
            Ok(Value::DateTime(Utc.timestamp_nanos(nanos)))
        }
        TAG_UUID => {
            let arr: [u8; 16] = slice
                .try_into()
                .map_err(|_| TapisaurusError::InvalidDocument("Invalid TAG_UUID length".into()))?;
            Ok(Value::Uuid(Uuid::from_bytes(arr)))
        }
        TAG_JSON_COMPLEX => Ok(serde_json::from_slice(slice)?),
        other => Err(TapisaurusError::InvalidDocument(format!(
            "Unknown TPBIN001 type tag: {other}"
        ))),
    }
}

fn decode_metadata(bytes: &[u8], offset: usize) -> TapisaurusResult<DocumentMetadata> {
    let tail = bytes
        .get(offset..)
        .ok_or_else(|| TapisaurusError::InvalidDocument("Invalid metadata offset".into()))?;
    if tail.len() < 26 {
        return Err(TapisaurusError::InvalidDocument(
            "Truncated TPBIN001 metadata trailer".into(),
        ));
    }
    let version = u64::from_le_bytes(tail[0..8].try_into().unwrap());
    let created_ns = i64::from_le_bytes(tail[8..16].try_into().unwrap());
    let updated_ns = i64::from_le_bytes(tail[16..24].try_into().unwrap());
    let col_len = u16::from_le_bytes([tail[24], tail[25]]) as usize;
    let collection = if col_len > 0 && tail.len() >= 26 + col_len {
        std::str::from_utf8(&tail[26..26 + col_len])
            .ok()
            .map(str::to_string)
    } else {
        None
    };

    Ok(DocumentMetadata {
        version,
        created_at: Utc.timestamp_nanos(created_ns),
        updated_at: Utc.timestamp_nanos(updated_ns),
        collection,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tpbin001_roundtrip_and_zero_deserialization_projection() {
        let doc = Document::with_id("usr_9001")
            .field("active", true)
            .field("bio", "Long biographical text that does not need to be parsed during selective numeric or status queries.")
            .field("city", "Kuala Lumpur")
            .field("embedding", Value::Vector(vec![0.25, -0.5, 0.75, 1.0]))
            .field("score", 42_500i64)
            .field("tier", "Enterprise");

        let encoded = SlottedBinaryCodec::encode(&doc).expect("encode TPBIN001");
        assert!(encoded.starts_with(SLOTTED_BINARY_MAGIC));

        // 1. O(log F) Zero-Deserialization single field extraction
        assert_eq!(
            SlottedBinaryCodec::extract_field(&encoded, "_id").unwrap(),
            Some(Value::String("usr_9001".into()))
        );
        assert_eq!(
            SlottedBinaryCodec::extract_field(&encoded, "score").unwrap(),
            Some(Value::Integer(42_500))
        );
        assert_eq!(
            SlottedBinaryCodec::extract_field(&encoded, "city").unwrap(),
            Some(Value::String("Kuala Lumpur".into()))
        );
        assert_eq!(
            SlottedBinaryCodec::extract_field(&encoded, "nonexistent").unwrap(),
            None
        );

        // 2. Zero-Deserialization filter evaluation
        assert!(SlottedBinaryCodec::matches_filter(
            &encoded,
            &[
                ("city".into(), Value::String("Kuala Lumpur".into())),
                ("score".into(), Value::Integer(42_500)),
            ]
        ));
        assert!(!SlottedBinaryCodec::matches_filter(
            &encoded,
            &[("city".into(), Value::String("Penang".into()))]
        ));

        // 3. Zero-Deserialization column projection (decodes only score & tier, skipping bio & embedding)
        let projected =
            SlottedBinaryCodec::project_fields(&encoded, &["score", "tier"]).expect("project");
        assert_eq!(projected.id.as_str(), "usr_9001");
        assert_eq!(projected.fields.len(), 2);
        assert_eq!(projected.get("score"), Some(&Value::Integer(42_500)));
        assert_eq!(
            projected.get("tier"),
            Some(&Value::String("Enterprise".into()))
        );
        assert!(projected.get("bio").is_none());

        // 4. Full decode roundtrip
        let decoded = SlottedBinaryCodec::decode(&encoded).expect("decode full");
        assert_eq!(decoded.id, doc.id);
        assert_eq!(decoded.fields, doc.fields);
    }
}
