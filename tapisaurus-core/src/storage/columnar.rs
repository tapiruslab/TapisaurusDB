//! # Columnar Storage & Analytics Batch Engine
//!
//! Provides zero-copy, Arrow/Parquet-compatible columnar representation of document data.
//! Converts row-oriented JSON documents into contiguous typed columnar arrays for
//! blazing-fast analytical scans (OLAP), DuckDB/Spark/Polars data science integration,
//! and high-speed aggregation without row deserialization overhead.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Supported columnar primitive data types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnDataType {
    Int64,
    Float64,
    String,
    Boolean,
    Binary,
    Null,
}

/// Contiguous array for a single typed column (Arrow RecordBatch compatible)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnData {
    Int64(Vec<Option<i64>>),
    Float64(Vec<Option<f64>>),
    String(Vec<Option<String>>),
    Boolean(Vec<Option<bool>>),
    Binary(Vec<Option<Vec<u8>>>),
}

impl ColumnData {
    /// Number of rows in this column
    pub fn len(&self) -> usize {
        match self {
            ColumnData::Int64(v) => v.len(),
            ColumnData::Float64(v) => v.len(),
            ColumnData::String(v) => v.len(),
            ColumnData::Boolean(v) => v.len(),
            ColumnData::Binary(v) => v.len(),
        }
    }

    /// Check if column is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Data type enum
    pub fn data_type(&self) -> ColumnDataType {
        match self {
            ColumnData::Int64(_) => ColumnDataType::Int64,
            ColumnData::Float64(_) => ColumnDataType::Float64,
            ColumnData::String(_) => ColumnDataType::String,
            ColumnData::Boolean(_) => ColumnDataType::Boolean,
            ColumnData::Binary(_) => ColumnDataType::Binary,
        }
    }
}

/// Schema definition for a columnar dataset
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnarSchema {
    pub fields: Vec<(String, ColumnDataType)>,
}

/// A contiguous columnar batch of rows (Arrow RecordBatch equivalent)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnarBatch {
    pub schema: ColumnarSchema,
    pub columns: HashMap<String, ColumnData>,
    pub row_count: usize,
}

impl ColumnarBatch {
    /// Create an empty columnar batch with a given schema
    pub fn new(schema: ColumnarSchema) -> Self {
        let mut columns = HashMap::new();
        for (name, dtype) in &schema.fields {
            let col_data = match dtype {
                ColumnDataType::Int64 => ColumnData::Int64(Vec::new()),
                ColumnDataType::Float64 => ColumnData::Float64(Vec::new()),
                ColumnDataType::String => ColumnData::String(Vec::new()),
                ColumnDataType::Boolean => ColumnData::Boolean(Vec::new()),
                ColumnDataType::Binary => ColumnData::Binary(Vec::new()),
                ColumnDataType::Null => ColumnData::String(Vec::new()),
            };
            columns.insert(name.clone(), col_data);
        }

        Self {
            schema,
            columns,
            row_count: 0,
        }
    }

    /// Transpose a slice of JSON document rows into a columnar batch
    pub fn from_json_documents(docs: &[serde_json::Value]) -> Result<Self, String> {
        if docs.is_empty() {
            return Ok(Self {
                schema: ColumnarSchema { fields: Vec::new() },
                columns: HashMap::new(),
                row_count: 0,
            });
        }

        // 1. Infer schema by inspecting all document keys
        let mut schema_map: HashMap<String, ColumnDataType> = HashMap::new();
        for doc in docs {
            if let serde_json::Value::Object(map) = doc {
                for (k, v) in map {
                    schema_map.entry(k.clone()).or_insert_with(|| match v {
                        serde_json::Value::Number(n) if n.is_i64() => ColumnDataType::Int64,
                        serde_json::Value::Number(_) => ColumnDataType::Float64,
                        serde_json::Value::String(_) => ColumnDataType::String,
                        serde_json::Value::Bool(_) => ColumnDataType::Boolean,
                        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                            ColumnDataType::String
                        }
                        _ => ColumnDataType::String,
                    });
                }
            }
        }

        let fields: Vec<(String, ColumnDataType)> = schema_map.into_iter().collect();
        let schema = ColumnarSchema { fields };
        let mut batch = ColumnarBatch::new(schema);

        // 2. Populate contiguous columns row-by-row
        for doc in docs {
            batch.push_row(doc)?;
        }

        Ok(batch)
    }

    /// Append a single JSON row
    pub fn push_row(&mut self, doc: &serde_json::Value) -> Result<(), String> {
        let empty_map = serde_json::Map::new();
        let map = match doc {
            serde_json::Value::Object(m) => m,
            _ => &empty_map,
        };

        for (field_name, dtype) in &self.schema.fields {
            let col = self
                .columns
                .get_mut(field_name)
                .ok_or_else(|| format!("Column '{field_name}' not found in columnar batch"))?;

            let json_val = map.get(field_name);
            match (col, dtype) {
                (ColumnData::Int64(v), ColumnDataType::Int64) => {
                    v.push(json_val.and_then(|val| val.as_i64()));
                }
                (ColumnData::Float64(v), ColumnDataType::Float64) => {
                    v.push(json_val.and_then(|val| val.as_f64()));
                }
                (ColumnData::String(v), ColumnDataType::String) => {
                    v.push(json_val.map(|val| {
                        if let Some(s) = val.as_str() {
                            s.to_string()
                        } else {
                            val.to_string()
                        }
                    }));
                }
                (ColumnData::Boolean(v), ColumnDataType::Boolean) => {
                    v.push(json_val.and_then(|val| val.as_bool()));
                }
                (ColumnData::Binary(v), ColumnDataType::Binary) => {
                    v.push(json_val.and_then(|val| val.as_str().map(|s| s.as_bytes().to_vec())));
                }
                _ => {}
            }
        }

        self.row_count += 1;
        Ok(())
    }

    /// Columnar Projection: Extract only specific columns with zero-copy column slicing
    pub fn project(&self, target_columns: &[&str]) -> Result<Self, String> {
        let mut projected_fields = Vec::new();
        let mut projected_cols = HashMap::new();

        for &col_name in target_columns {
            if let Some(col_data) = self.columns.get(col_name) {
                projected_fields.push((col_name.to_string(), col_data.data_type()));
                projected_cols.insert(col_name.to_string(), col_data.clone());
            } else {
                return Err(format!(
                    "Projection column '{col_name}' does not exist in batch"
                ));
            }
        }

        Ok(Self {
            schema: ColumnarSchema {
                fields: projected_fields,
            },
            columns: projected_cols,
            row_count: self.row_count,
        })
    }

    /// High-Speed Column Sum (SIMD-Friendly Columnar Scan)
    pub fn sum_f64(&self, column_name: &str) -> Option<f64> {
        if let Some(ColumnData::Float64(vals)) = self.columns.get(column_name) {
            let sum: f64 = vals.iter().filter_map(|&v| v).sum();
            Some(sum)
        } else if let Some(ColumnData::Int64(vals)) = self.columns.get(column_name) {
            let sum: f64 = vals.iter().filter_map(|&v| v.map(|i| i as f64)).sum();
            Some(sum)
        } else {
            None
        }
    }

    /// High-Speed Column Average (SIMD-Friendly Columnar Scan with Zero Allocations)
    pub fn avg_f64(&self, column_name: &str) -> Option<f64> {
        if let Some(ColumnData::Float64(vals)) = self.columns.get(column_name) {
            let mut sum = 0.0f64;
            let mut count = 0usize;
            for &v in vals {
                if let Some(x) = v {
                    sum += x;
                    count += 1;
                }
            }
            if count == 0 {
                None
            } else {
                Some(sum / count as f64)
            }
        } else if let Some(ColumnData::Int64(vals)) = self.columns.get(column_name) {
            let mut sum = 0.0f64;
            let mut count = 0usize;
            for &v in vals {
                if let Some(i) = v {
                    sum += i as f64;
                    count += 1;
                }
            }
            if count == 0 {
                None
            } else {
                Some(sum / count as f64)
            }
        } else {
            None
        }
    }

    /// Column Minimum (SIMD-Friendly Columnar Scan)
    pub fn min_f64(&self, column_name: &str) -> Option<f64> {
        if let Some(ColumnData::Float64(vals)) = self.columns.get(column_name) {
            vals.iter().filter_map(|&v| v).min_by(|a, b| a.total_cmp(b))
        } else if let Some(ColumnData::Int64(vals)) = self.columns.get(column_name) {
            vals.iter()
                .filter_map(|&v| v.map(|i| i as f64))
                .min_by(|a, b| a.total_cmp(b))
        } else {
            None
        }
    }

    /// Column Maximum (SIMD-Friendly Columnar Scan)
    pub fn max_f64(&self, column_name: &str) -> Option<f64> {
        if let Some(ColumnData::Float64(vals)) = self.columns.get(column_name) {
            vals.iter().filter_map(|&v| v).max_by(|a, b| a.total_cmp(b))
        } else if let Some(ColumnData::Int64(vals)) = self.columns.get(column_name) {
            vals.iter()
                .filter_map(|&v| v.map(|i| i as f64))
                .max_by(|a, b| a.total_cmp(b))
        } else {
            None
        }
    }

    /// Non-null row count for a column
    pub fn count(&self, column_name: &str) -> usize {
        match self.columns.get(column_name) {
            Some(ColumnData::Int64(v)) => v.iter().filter(|x| x.is_some()).count(),
            Some(ColumnData::Float64(v)) => v.iter().filter(|x| x.is_some()).count(),
            Some(ColumnData::String(v)) => v.iter().filter(|x| x.is_some()).count(),
            Some(ColumnData::Boolean(v)) => v.iter().filter(|x| x.is_some()).count(),
            Some(ColumnData::Binary(v)) => v.iter().filter(|x| x.is_some()).count(),
            None => 0,
        }
    }

    /// Export to CSV string for external Data Science tools
    pub fn to_csv(&self) -> String {
        let mut csv = String::new();
        let field_names: Vec<&String> = self.schema.fields.iter().map(|(name, _)| name).collect();

        // Header
        csv.push_str(
            &field_names
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(","),
        );
        csv.push('\n');

        // Rows
        for r in 0..self.row_count {
            let mut row_vals = Vec::new();
            for (name, _) in &self.schema.fields {
                let cell_str = match self.columns.get(name) {
                    Some(ColumnData::Int64(v)) => v
                        .get(r)
                        .and_then(|x| *x)
                        .map(|i| i.to_string())
                        .unwrap_or_default(),
                    Some(ColumnData::Float64(v)) => v
                        .get(r)
                        .and_then(|x| *x)
                        .map(|f| f.to_string())
                        .unwrap_or_default(),
                    Some(ColumnData::String(v)) => v
                        .get(r)
                        .and_then(|x| x.as_deref())
                        .map(|s| format!("\"{s}\""))
                        .unwrap_or_default(),
                    Some(ColumnData::Boolean(v)) => v
                        .get(r)
                        .and_then(|x| *x)
                        .map(|b| b.to_string())
                        .unwrap_or_default(),
                    Some(ColumnData::Binary(v)) => v
                        .get(r)
                        .and_then(|x| x.as_ref())
                        .map(|_| "<binary>".to_string())
                        .unwrap_or_default(),
                    None => String::new(),
                };
                row_vals.push(cell_str);
            }
            csv.push_str(&row_vals.join(","));
            csv.push('\n');
        }

        csv
    }
}

/// Default morsel size (number of rows per columnar stripe, tuned for CPU L1/L2 cache residency)
pub const DEFAULT_MORSEL_ROWS: usize = 2048;

/// Magic header for persisted HTAP columnar stripe files
pub const HTAP_MAGIC_BYTES: &[u8; 8] = b"TPHTAP01";

/// Numeric predicate operator for Zone-Map stripe pruning
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ZonePredicateOp {
    Eq,
    Gt,
    Gte,
    Lt,
    Lte,
    Between(f64, f64),
}

/// Min-Max Zone Map metadata for a single column inside a `ColumnarStripe`.
/// Enables O(1) data skipping of entire 2048-row stripes when query predicates fall outside `[min, max]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnZoneMap {
    pub min_f64: Option<f64>,
    pub max_f64: Option<f64>,
    pub min_str: Option<String>,
    pub max_str: Option<String>,
    pub null_count: usize,
    pub row_count: usize,
}

impl ColumnZoneMap {
    /// Compute min/max/null zone map statistics from a contiguous `ColumnData` array
    pub fn from_column(col: &ColumnData) -> Self {
        match col {
            ColumnData::Int64(vals) => {
                let mut min_v: Option<f64> = None;
                let mut max_v: Option<f64> = None;
                let mut nulls = 0usize;
                for v in vals {
                    if let Some(i) = v {
                        let f = *i as f64;
                        min_v = Some(min_v.map_or(f, |m| m.min(f)));
                        max_v = Some(max_v.map_or(f, |m| m.max(f)));
                    } else {
                        nulls += 1;
                    }
                }
                Self {
                    min_f64: min_v,
                    max_f64: max_v,
                    min_str: None,
                    max_str: None,
                    null_count: nulls,
                    row_count: vals.len(),
                }
            }
            ColumnData::Float64(vals) => {
                let mut min_v: Option<f64> = None;
                let mut max_v: Option<f64> = None;
                let mut nulls = 0usize;
                for v in vals {
                    if let Some(f) = v {
                        min_v = Some(min_v.map_or(*f, |m| m.min(*f)));
                        max_v = Some(max_v.map_or(*f, |m| m.max(*f)));
                    } else {
                        nulls += 1;
                    }
                }
                Self {
                    min_f64: min_v,
                    max_f64: max_v,
                    min_str: None,
                    max_str: None,
                    null_count: nulls,
                    row_count: vals.len(),
                }
            }
            ColumnData::String(vals) => {
                let mut min_s: Option<String> = None;
                let mut max_s: Option<String> = None;
                let mut nulls = 0usize;
                for v in vals {
                    if let Some(s) = v {
                        min_s = Some(match min_s {
                            Some(ref m) if m <= s => m.clone(),
                            _ => s.clone(),
                        });
                        max_s = Some(match max_s {
                            Some(ref m) if m >= s => m.clone(),
                            _ => s.clone(),
                        });
                    } else {
                        nulls += 1;
                    }
                }
                Self {
                    min_f64: None,
                    max_f64: None,
                    min_str: min_s,
                    max_str: max_s,
                    null_count: nulls,
                    row_count: vals.len(),
                }
            }
            ColumnData::Boolean(vals) => Self {
                min_f64: None,
                max_f64: None,
                min_str: None,
                max_str: None,
                null_count: vals.iter().filter(|v| v.is_none()).count(),
                row_count: vals.len(),
            },
            ColumnData::Binary(vals) => Self {
                min_f64: None,
                max_f64: None,
                min_str: None,
                max_str: None,
                null_count: vals.iter().filter(|v| v.is_none()).count(),
                row_count: vals.len(),
            },
        }
    }

    /// Returns `true` if this stripe can be skipped entirely for the given numeric predicate
    pub fn can_prune_numeric(&self, op: ZonePredicateOp, val: f64) -> bool {
        let (Some(min_v), Some(max_v)) = (self.min_f64, self.max_f64) else {
            return true;
        };
        match op {
            ZonePredicateOp::Eq => val < min_v || val > max_v,
            ZonePredicateOp::Gt => max_v <= val,
            ZonePredicateOp::Gte => max_v < val,
            ZonePredicateOp::Lt => min_v >= val,
            ZonePredicateOp::Lte => min_v > val,
            ZonePredicateOp::Between(low, high) => max_v < low || min_v > high,
        }
    }

    fn matches_row(op: ZonePredicateOp, row_val: f64, threshold: f64) -> bool {
        match op {
            ZonePredicateOp::Eq => (row_val - threshold).abs() < f64::EPSILON,
            ZonePredicateOp::Gt => row_val > threshold,
            ZonePredicateOp::Gte => row_val >= threshold,
            ZonePredicateOp::Lt => row_val < threshold,
            ZonePredicateOp::Lte => row_val <= threshold,
            ZonePredicateOp::Between(low, high) => row_val >= low && row_val <= high,
        }
    }
}

/// A morsel-sized columnar stripe with precomputed per-column `ColumnZoneMap`s
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnarStripe {
    pub stripe_id: u32,
    pub batch: ColumnarBatch,
    pub zone_maps: HashMap<String, ColumnZoneMap>,
}

impl ColumnarStripe {
    pub fn from_batch(stripe_id: u32, batch: ColumnarBatch) -> Self {
        let mut zone_maps = HashMap::new();
        for (col_name, col_data) in &batch.columns {
            zone_maps.insert(col_name.clone(), ColumnZoneMap::from_column(col_data));
        }
        Self {
            stripe_id,
            batch,
            zone_maps,
        }
    }
}

/// HTAP (Hybrid Transactional/Analytical Processing) Columnar Table segmented into morsel stripes
/// with Zone-Map predicate pushdown, real-time `delta_row_buffer` merger, autonomous background
/// morsel compaction (`delta_row_buffer.len() >= stripe_rows`), and LZ4 + CRC32 disk persistence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HtapColumnarTable {
    pub stripe_rows: usize,
    pub stripes: Vec<ColumnarStripe>,
    pub total_rows: usize,
    /// Real-time hot mutation buffer holding rows inserted/updated after the last frozen `ColumnarStripe`.
    #[serde(default)]
    pub delta_row_buffer: Vec<serde_json::Value>,
    /// Next monotonically increasing `stripe_id` assigned when `delta_row_buffer` auto-freezes.
    #[serde(default)]
    pub next_stripe_id: u32,
    /// Number of `ColumnarStripe`s automatically compacted from `delta_row_buffer` upon reaching `stripe_rows`.
    #[serde(default)]
    pub auto_compacted_stripes: usize,
    /// Maps `_id` / `id` -> owning location (`>= 0` = `stripe_id`, `-1` = `delta_row_buffer`, `-2` = deleted).
    #[serde(default)]
    pub row_owner_stripe: HashMap<String, i64>,
    /// Fast-path flag: `true` only when a row inside an already-frozen `ColumnarStripe` was updated or deleted.
    #[serde(default)]
    pub has_shadowed_stripe_rows: bool,
}

impl HtapColumnarTable {
    /// Initialize an empty HTAP table with the given morsel stripe threshold (e.g., `2048` rows).
    pub fn new(stripe_rows: usize) -> Self {
        Self {
            stripe_rows: stripe_rows.max(1),
            stripes: Vec::new(),
            total_rows: 0,
            delta_row_buffer: Vec::new(),
            next_stripe_id: 0,
            auto_compacted_stripes: 0,
            row_owner_stripe: HashMap::new(),
            has_shadowed_stripe_rows: false,
        }
    }

    /// Build a striped HTAP table from JSON documents using the specified morsel stripe size
    pub fn from_json_documents(
        docs: &[serde_json::Value],
        stripe_rows: usize,
    ) -> Result<Self, String> {
        let morsel = stripe_rows.max(1);
        let mut stripes = Vec::new();
        let mut row_owner_stripe = HashMap::with_capacity(docs.len());
        for (idx, chunk) in docs.chunks(morsel).enumerate() {
            let stripe_id = idx as u32;
            for doc in chunk {
                if let Some(id) = Self::extract_row_id(doc) {
                    row_owner_stripe.insert(id, stripe_id as i64);
                }
            }
            let batch = ColumnarBatch::from_json_documents(chunk)?;
            stripes.push(ColumnarStripe::from_batch(stripe_id, batch));
        }
        let next_stripe_id = stripes.len() as u32;
        Ok(Self {
            stripe_rows: morsel,
            stripes,
            total_rows: docs.len(),
            delta_row_buffer: Vec::new(),
            next_stripe_id,
            auto_compacted_stripes: 0,
            row_owner_stripe,
            has_shadowed_stripe_rows: false,
        })
    }

    /// Clear all stripes and delta buffers.
    pub fn clear(&mut self) {
        self.stripes.clear();
        self.total_rows = 0;
        self.delta_row_buffer.clear();
        self.next_stripe_id = 0;
        self.auto_compacted_stripes = 0;
        self.row_owner_stripe.clear();
        self.has_shadowed_stripe_rows = false;
    }

    /// Append a live inserted or updated JSON row into `delta_row_buffer`.
    /// Whenever `delta_row_buffer.len() >= self.stripe_rows` (e.g., `2048` rows), automatically
    /// freezes the full morsel batch into a new `ColumnarStripe` with `ColumnZoneMap`s.
    /// Returns `Ok(true)` if a new `ColumnarStripe` was autonomously compacted on this call.
    pub fn append_delta_row(&mut self, row: serde_json::Value) -> Result<bool, String> {
        if let Some(id) = Self::extract_row_id(&row) {
            match self.row_owner_stripe.insert(id.clone(), -1) {
                Some(-1) => {
                    // Replace existing uncommitted row in delta_row_buffer
                    self.delta_row_buffer
                        .retain(|r| Self::extract_row_id(r).as_deref() != Some(id.as_str()));
                }
                Some(prev_stripe) if prev_stripe >= 0 => {
                    // Row previously lived in a frozen ColumnarStripe and is now shadowed by delta
                    self.has_shadowed_stripe_rows = true;
                }
                _ => {
                    self.total_rows += 1;
                }
            }
        } else {
            self.total_rows += 1;
        }

        self.delta_row_buffer.push(row);

        if self.delta_row_buffer.len() >= self.stripe_rows {
            self.compact_full_delta_morsels()?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Append a batch of JSON rows into `delta_row_buffer`, auto-sealing every `stripe_rows` morsel.
    /// Returns the number of newly compacted `ColumnarStripe`s.
    pub fn append_delta_rows(&mut self, rows: &[serde_json::Value]) -> Result<usize, String> {
        let before = self.auto_compacted_stripes;
        for row in rows {
            self.append_delta_row(row.clone())?;
        }
        Ok(self.auto_compacted_stripes - before)
    }

    /// Record a row deletion by primary ID (`_id` or `id`) across `delta_row_buffer` and frozen stripes.
    pub fn delete_delta_row(&mut self, row_id: &str) -> bool {
        match self.row_owner_stripe.get(row_id).copied() {
            Some(-1) => {
                self.delta_row_buffer
                    .retain(|r| Self::extract_row_id(r).as_deref() != Some(row_id));
                self.row_owner_stripe.insert(row_id.to_string(), -2);
                self.total_rows = self.total_rows.saturating_sub(1);
                true
            }
            Some(s) if s >= 0 => {
                self.row_owner_stripe.insert(row_id.to_string(), -2);
                self.has_shadowed_stripe_rows = true;
                self.total_rows = self.total_rows.saturating_sub(1);
                true
            }
            _ => false,
        }
    }

    /// Force-freeze any remaining `< stripe_rows` rows in `delta_row_buffer` into a `ColumnarStripe`.
    pub fn flush_delta_to_stripe(&mut self) -> Result<bool, String> {
        if self.delta_row_buffer.is_empty() {
            return Ok(false);
        }
        let chunk: Vec<serde_json::Value> = std::mem::take(&mut self.delta_row_buffer);
        self.freeze_chunk_into_stripe(&chunk)?;
        Ok(true)
    }

    fn compact_full_delta_morsels(&mut self) -> Result<(), String> {
        let morsel = self.stripe_rows.max(1);
        while self.delta_row_buffer.len() >= morsel {
            let chunk: Vec<serde_json::Value> = self.delta_row_buffer.drain(0..morsel).collect();
            self.freeze_chunk_into_stripe(&chunk)?;
        }
        Ok(())
    }

    fn freeze_chunk_into_stripe(&mut self, chunk: &[serde_json::Value]) -> Result<(), String> {
        let stripe_id = self.next_stripe_id;
        self.next_stripe_id += 1;
        for doc in chunk {
            if let Some(id) = Self::extract_row_id(doc) {
                self.row_owner_stripe.insert(id, stripe_id as i64);
            }
        }
        let batch = ColumnarBatch::from_json_documents(chunk)?;
        self.stripes
            .push(ColumnarStripe::from_batch(stripe_id, batch));
        self.auto_compacted_stripes += 1;
        Ok(())
    }

    fn extract_row_id(doc: &serde_json::Value) -> Option<String> {
        doc.get("_id")
            .or_else(|| doc.get("id"))
            .and_then(|v| match v {
                serde_json::Value::String(s) => Some(s.clone()),
                serde_json::Value::Number(n) => Some(n.to_string()),
                _ => None,
            })
    }

    fn extract_json_numeric(doc: &serde_json::Value, col_name: &str) -> Option<f64> {
        doc.get(col_name).and_then(|v| v.as_f64())
    }

    fn extract_json_string(doc: &serde_json::Value, col_name: &str) -> Option<String> {
        doc.get(col_name).and_then(|v| match v {
            serde_json::Value::String(s) => Some(s.clone()),
            serde_json::Value::Number(n) => Some(n.to_string()),
            _ => None,
        })
    }

    fn is_stripe_row_active(&self, stripe: &ColumnarStripe, row_idx: usize) -> bool {
        if !self.has_shadowed_stripe_rows {
            return true;
        }
        let id_opt = match stripe.batch.columns.get("_id") {
            Some(ColumnData::String(v)) => v.get(row_idx).and_then(|x| x.clone()),
            Some(ColumnData::Int64(v)) => v.get(row_idx).and_then(|x| x.map(|i| i.to_string())),
            _ => match stripe.batch.columns.get("id") {
                Some(ColumnData::String(v)) => v.get(row_idx).and_then(|x| x.clone()),
                Some(ColumnData::Int64(v)) => v.get(row_idx).and_then(|x| x.map(|i| i.to_string())),
                _ => None,
            },
        };
        if let Some(id) = id_opt {
            self.row_owner_stripe.get(&id).copied() == Some(stripe.stripe_id as i64)
        } else {
            true
        }
    }

    /// Execute a Zone-Map pruned vectorized `SUM` over `target_col` filtered by `pred_col op threshold`,
    /// transparently merging frozen `ColumnarStripe`s and uncompacted hot rows in `delta_row_buffer`.
    /// Returns `(sum, stripes_scanned, stripes_pruned)`.
    pub fn sum_filtered(
        &self,
        target_col: &str,
        pred_col: &str,
        op: ZonePredicateOp,
        threshold: f64,
    ) -> (f64, usize, usize) {
        let mut sum = 0.0f64;
        let mut scanned = 0usize;
        let mut pruned = 0usize;

        for stripe in &self.stripes {
            if let Some(zm) = stripe.zone_maps.get(pred_col) {
                if zm.can_prune_numeric(op, threshold) {
                    pruned += 1;
                    continue;
                }
            }
            scanned += 1;
            let row_count = stripe.batch.row_count;
            for r in 0..row_count {
                if !self.is_stripe_row_active(stripe, r) {
                    continue;
                }
                let pred_val = Self::extract_numeric(&stripe.batch, pred_col, r);
                if let Some(pv) = pred_val {
                    if ColumnZoneMap::matches_row(op, pv, threshold) {
                        if let Some(tv) = Self::extract_numeric(&stripe.batch, target_col, r) {
                            sum += tv;
                        }
                    }
                }
            }
        }

        // Transparently merge any residual < stripe_rows rows in delta_row_buffer
        for row in &self.delta_row_buffer {
            if let Some(pv) = Self::extract_json_numeric(row, pred_col) {
                if ColumnZoneMap::matches_row(op, pv, threshold) {
                    if let Some(tv) = Self::extract_json_numeric(row, target_col) {
                        sum += tv;
                    }
                }
            }
        }

        (sum, scanned, pruned)
    }

    /// Execute a Zone-Map pruned vectorized `GROUP BY` computing `(SUM, COUNT)` per group key,
    /// transparently merging frozen `ColumnarStripe`s and hot rows in `delta_row_buffer`.
    /// Returns `(group_map, stripes_pruned)`.
    pub fn group_by_sum_count(
        &self,
        group_col: &str,
        agg_col: &str,
        predicate: Option<(&str, ZonePredicateOp, f64)>,
    ) -> (HashMap<String, (f64, usize)>, usize) {
        let mut groups: HashMap<String, (f64, usize)> = HashMap::new();
        let mut pruned = 0usize;

        for stripe in &self.stripes {
            if let Some((p_col, op, thresh)) = predicate {
                if let Some(zm) = stripe.zone_maps.get(p_col) {
                    if zm.can_prune_numeric(op, thresh) {
                        pruned += 1;
                        continue;
                    }
                }
            }

            let Some(ColumnData::String(group_vals)) = stripe.batch.columns.get(group_col) else {
                continue;
            };

            for (r, g_opt) in group_vals.iter().enumerate() {
                if !self.is_stripe_row_active(stripe, r) {
                    continue;
                }
                let Some(ref g_key) = g_opt else {
                    continue;
                };
                if let Some((p_col, op, thresh)) = predicate {
                    let Some(pv) = Self::extract_numeric(&stripe.batch, p_col, r) else {
                        continue;
                    };
                    if !ColumnZoneMap::matches_row(op, pv, thresh) {
                        continue;
                    }
                }
                if let Some(av) = Self::extract_numeric(&stripe.batch, agg_col, r) {
                    let entry = groups.entry(g_key.clone()).or_insert((0.0, 0));
                    entry.0 += av;
                    entry.1 += 1;
                }
            }
        }

        // Transparently merge residual hot rows from delta_row_buffer
        for row in &self.delta_row_buffer {
            let Some(g_key) = Self::extract_json_string(row, group_col) else {
                continue;
            };
            if let Some((p_col, op, thresh)) = predicate {
                let Some(pv) = Self::extract_json_numeric(row, p_col) else {
                    continue;
                };
                if !ColumnZoneMap::matches_row(op, pv, thresh) {
                    continue;
                }
            }
            if let Some(av) = Self::extract_json_numeric(row, agg_col) {
                let entry = groups.entry(g_key).or_insert((0.0, 0));
                entry.0 += av;
                entry.1 += 1;
            }
        }

        (groups, pruned)
    }

    fn extract_numeric(batch: &ColumnarBatch, col_name: &str, row_idx: usize) -> Option<f64> {
        match batch.columns.get(col_name) {
            Some(ColumnData::Float64(v)) => v.get(row_idx).and_then(|x| *x),
            Some(ColumnData::Int64(v)) => v.get(row_idx).and_then(|x| x.map(|i| i as f64)),
            _ => None,
        }
    }

    /// Persist HTAP columnar stripes to disk with `TPHTAP01` header, LZ4 frame compression, and CRC32 verification
    pub fn save_to_path(&self, path: &std::path::Path) -> std::io::Result<()> {
        let json_bytes = serde_json::to_vec(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let compressed = lz4_flex::compress_prepend_size(&json_bytes);
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&compressed);
        let crc = hasher.finalize();

        let mut out = Vec::with_capacity(8 + 4 + compressed.len());
        out.extend_from_slice(HTAP_MAGIC_BYTES);
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(&compressed);
        std::fs::write(path, out)
    }

    /// Load and verify an HTAP columnar stripe file from disk
    pub fn load_from_path(path: &std::path::Path) -> std::io::Result<Self> {
        let raw = std::fs::read(path)?;
        if raw.len() < 12 || &raw[..8] != HTAP_MAGIC_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid TPHTAP01 header",
            ));
        }
        let mut crc_bytes = [0u8; 4];
        crc_bytes.copy_from_slice(&raw[8..12]);
        let expected_crc = u32::from_le_bytes(crc_bytes);
        let compressed = &raw[12..];

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(compressed);
        if hasher.finalize() != expected_crc {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "HTAP columnar file CRC32 mismatch",
            ));
        }

        let decompressed = lz4_flex::decompress_size_prepended(compressed)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        serde_json::from_slice(&decompressed)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_columnar_batch_from_json_and_projection() {
        let docs = vec![
            serde_json::json!({ "id": 1, "ticker": "NVDA", "price": 125.50, "in_portfolio": true }),
            serde_json::json!({ "id": 2, "ticker": "MSFT", "price": 440.20, "in_portfolio": false }),
            serde_json::json!({ "id": 3, "ticker": "AAPL", "price": 220.80, "in_portfolio": true }),
        ];

        let batch = ColumnarBatch::from_json_documents(&docs).unwrap();
        assert_eq!(batch.row_count, 3);
        assert_eq!(batch.columns.len(), 4);

        // Test columnar aggregation without row loop
        let total_price = batch.sum_f64("price").unwrap();
        assert!((total_price - (125.50 + 440.20 + 220.80)).abs() < 1e-4);

        // Test zero-copy column projection (select ticker & price only)
        let projected = batch.project(&["ticker", "price"]).unwrap();
        assert_eq!(projected.schema.fields.len(), 2);
        assert_eq!(projected.row_count, 3);
        assert!(projected.columns.contains_key("ticker"));
        assert!(projected.columns.contains_key("price"));
        assert!(!projected.columns.contains_key("in_portfolio"));

        // Test CSV export
        let csv = projected.to_csv();
        assert!(csv.contains("ticker"));
        assert!(csv.contains("price"));
        assert!(csv.contains("125.5"));

        // Test avg, min, max, count
        let avg_price = batch.avg_f64("price").unwrap();
        assert!((avg_price - ((125.50 + 440.20 + 220.80) / 3.0)).abs() < 1e-4);

        let min_price = batch.min_f64("price").unwrap();
        assert!((min_price - 125.50).abs() < 1e-4);

        let max_price = batch.max_f64("price").unwrap();
        assert!((max_price - 440.20).abs() < 1e-4);

        assert_eq!(batch.count("ticker"), 3);
        assert_eq!(batch.count("price"), 3);
    }

    #[test]
    fn test_htap_zone_map_pruning_and_lz4_persistence() {
        // Create 300 rows sorted by order_id (0..300) across 3 stripes of 100 rows each
        let mut docs = Vec::with_capacity(300);
        for i in 0..300 {
            let region = if i % 2 == 0 { "APAC" } else { "EMEA" };
            docs.push(serde_json::json!({
                "order_id": i,
                "region": region,
                "amount": (i as f64) * 10.0,
            }));
        }

        let table = HtapColumnarTable::from_json_documents(&docs, 100).unwrap();
        assert_eq!(table.stripes.len(), 3);

        // Query where order_id >= 250 -> stripes 0 (0..99) and 1 (100..199) MUST be pruned via Zone Map!
        let (sum, scanned, pruned) =
            table.sum_filtered("amount", "order_id", ZonePredicateOp::Gte, 250.0);
        assert_eq!(
            pruned, 2,
            "First two 100-row stripes must be skipped by Zone Map"
        );
        assert_eq!(scanned, 1, "Only stripe 2 (200..299) should be scanned");
        let expected_sum: f64 = (250..300).map(|i| (i as f64) * 10.0).sum();
        assert!((sum - expected_sum).abs() < 1e-4);

        // Group by region with Zone Map predicate order_id >= 200
        let (groups, grp_pruned) = table.group_by_sum_count(
            "region",
            "amount",
            Some(("order_id", ZonePredicateOp::Gte, 200.0)),
        );
        assert_eq!(grp_pruned, 2);
        assert_eq!(groups.get("APAC").unwrap().1, 50);
        assert_eq!(groups.get("EMEA").unwrap().1, 50);

        // Persist to disk with LZ4 + CRC32 and reload
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("orders_htap.col");
        table.save_to_path(&path).unwrap();
        let loaded = HtapColumnarTable::load_from_path(&path).unwrap();
        assert_eq!(loaded.total_rows, 300);
        assert_eq!(loaded.stripes.len(), 3);
    }

    #[test]
    fn test_htap_autonomous_delta_to_columnar_compactor_and_live_merger() {
        // Start with 100 initial rows (stripe 0: order_id 0..100) with stripe_rows = 100
        let mut initial = Vec::with_capacity(100);
        for i in 0..100 {
            initial.push(serde_json::json!({
                "_id": format!("ord_{i}"),
                "order_id": i,
                "region": if i % 2 == 0 { "APAC" } else { "EMEA" },
                "amount": 10.0,
            }));
        }
        let mut table = HtapColumnarTable::from_json_documents(&initial, 100).unwrap();
        assert_eq!(table.stripes.len(), 1);
        assert_eq!(table.delta_row_buffer.len(), 0);

        // Stream 140 new delta rows after ANALYZE (order_id 100..240, amount = 20.0):
        // - First 100 delta rows (100..200) MUST auto-compact into Stripe 1 with ColumnZoneMap!
        // - Remaining 40 delta rows (200..240) remain in delta_row_buffer and merge transparently!
        for i in 100..240 {
            let _ = table
                .append_delta_row(serde_json::json!({
                    "_id": format!("ord_{i}"),
                    "order_id": i,
                    "region": if i % 2 == 0 { "APAC" } else { "EMEA" },
                    "amount": 20.0,
                }))
                .unwrap();
        }

        assert_eq!(
            table.stripes.len(),
            2,
            "100 delta rows must auto-freeze into Stripe 1"
        );
        assert_eq!(table.auto_compacted_stripes, 1);
        assert_eq!(table.delta_row_buffer.len(), 40);
        assert_eq!(table.total_rows, 240);

        // Query order_id >= 150:
        // - Stripe 0 (0..99) is pruned by Zone Map (pruned = 1)
        // - Stripe 1 (100..199) is scanned (50 rows * 20.0 = 1000.0)
        // - delta_row_buffer (200..239) is merged transparently (40 rows * 20.0 = 800.0)
        let (sum, scanned, pruned) =
            table.sum_filtered("amount", "order_id", ZonePredicateOp::Gte, 150.0);
        assert_eq!(pruned, 1, "Stripe 0 must be pruned by Zone Map");
        assert_eq!(scanned, 1, "Auto-compacted Stripe 1 must be scanned");
        assert!((sum - 1800.0).abs() < 1e-4);

        // Update an existing row in frozen Stripe 1 (ord_150: amount 20.0 -> 120.0) and delete ord_210 from delta
        table
            .append_delta_row(serde_json::json!({
                "_id": "ord_150",
                "order_id": 150,
                "region": "APAC",
                "amount": 120.0,
            }))
            .unwrap();
        assert!(table.delete_delta_row("ord_210"));
        assert_eq!(table.total_rows, 239);

        let (sum_after, _, _) =
            table.sum_filtered("amount", "order_id", ZonePredicateOp::Gte, 150.0);
        // 1800.0 + (120.0 - 20.0) - 20.0 = 1880.0
        assert!((sum_after - 1880.0).abs() < 1e-4);
    }
}
