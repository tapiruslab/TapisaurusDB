//! Core types for virtual system catalog materialization and SQL evaluation.

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::BTreeMap;

/// SQL protocol dialect controlling catalog defaults (schema names, type names).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogDialect {
    Postgres,
    MySql { database: String },
}

/// Typed cell value stored in materialized virtual catalog tables and query results.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CatalogValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    IntArray(Vec<i64>),
    TextArray(Vec<String>),
}

impl CatalogValue {
    /// Associated PostgreSQL type OID for wire RowDescription serialization.
    pub fn pg_type_oid(&self) -> i32 {
        match self {
            CatalogValue::Null | CatalogValue::Text(_) => 25, // text
            CatalogValue::Bool(_) => 16,                      // bool
            CatalogValue::Int(_) => 20,                       // int8
            CatalogValue::Float(_) => 701,                    // float8
            CatalogValue::IntArray(_) => 1016,                // _int8
            CatalogValue::TextArray(_) => 1009,               // _text
        }
    }

    /// Format value as PostgreSQL wire text representation (`None` -> SQL NULL).
    pub fn to_pg_text(&self) -> Option<String> {
        match self {
            CatalogValue::Null => None,
            CatalogValue::Bool(b) => Some(if *b { "t" } else { "f" }.to_string()),
            CatalogValue::Int(i) => Some(i.to_string()),
            CatalogValue::Float(f) => Some(f.to_string()),
            CatalogValue::Text(s) => Some(s.clone()),
            CatalogValue::IntArray(arr) => {
                let items: Vec<String> = arr.iter().map(ToString::to_string).collect();
                Some(format!("{{{}}}", items.join(",")))
            }
            CatalogValue::TextArray(arr) => {
                let items: Vec<String> = arr
                    .iter()
                    .map(|s| format!("\"{}\"", s.replace('"', "\\\"")))
                    .collect();
                Some(format!("{{{}}}", items.join(",")))
            }
        }
    }

    /// Format value as MySQL wire text representation (`None` -> SQL NULL).
    pub fn to_mysql_text(&self) -> Option<String> {
        match self {
            CatalogValue::Null => None,
            CatalogValue::Bool(b) => Some(if *b { "1" } else { "0" }.to_string()),
            CatalogValue::Int(i) => Some(i.to_string()),
            CatalogValue::Float(f) => Some(f.to_string()),
            CatalogValue::Text(s) => Some(s.clone()),
            CatalogValue::IntArray(arr) => {
                let items: Vec<String> = arr.iter().map(ToString::to_string).collect();
                Some(items.join(","))
            }
            CatalogValue::TextArray(arr) => Some(arr.join(",")),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, CatalogValue::Null)
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            CatalogValue::Bool(b) => Some(*b),
            CatalogValue::Int(i) => Some(*i != 0),
            CatalogValue::Text(s) => match s.to_ascii_lowercase().as_str() {
                "true" | "t" | "yes" | "1" | "on" => Some(true),
                "false" | "f" | "no" | "0" | "off" => Some(false),
                _ => None,
            },
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            CatalogValue::Int(i) => Some(*i),
            CatalogValue::Float(f) => Some(*f as i64),
            CatalogValue::Bool(b) => Some(i64::from(*b)),
            CatalogValue::Text(s) => s.trim().parse::<i64>().ok(),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            CatalogValue::Float(f) => Some(*f),
            CatalogValue::Int(i) => Some(*i as f64),
            CatalogValue::Text(s) => s.trim().parse::<f64>().ok(),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            CatalogValue::Text(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn to_string_lossy(&self) -> String {
        match self {
            CatalogValue::Null => String::new(),
            CatalogValue::Bool(b) => b.to_string(),
            CatalogValue::Int(i) => i.to_string(),
            CatalogValue::Float(f) => f.to_string(),
            CatalogValue::Text(s) => s.clone(),
            CatalogValue::IntArray(arr) => {
                let items: Vec<String> = arr.iter().map(ToString::to_string).collect();
                format!("{{{}}}", items.join(","))
            }
            CatalogValue::TextArray(arr) => format!("{{{}}}", arr.join(",")),
        }
    }

    /// Check whether this value (an array or PG `{a,b}` string) contains `needle`.
    pub fn contains_element(&self, needle: &CatalogValue) -> bool {
        if needle.is_null() {
            return false;
        }
        match self {
            CatalogValue::IntArray(arr) => needle.as_i64().is_some_and(|n| arr.contains(&n)),
            CatalogValue::TextArray(arr) => {
                let target = needle.to_string_lossy();
                arr.iter().any(|s| s == &target)
            }
            CatalogValue::Text(s) => {
                let trimmed = s.trim();
                if trimmed.starts_with('{') && trimmed.ends_with('}') {
                    let inner = &trimmed[1..trimmed.len() - 1];
                    if inner.is_empty() {
                        return false;
                    }
                    let target = needle.to_string_lossy();
                    return inner
                        .split(',')
                        .map(|p| p.trim().trim_matches('"').trim_matches('\''))
                        .any(|item| item == target);
                }
                false
            }
            _ => false,
        }
    }

    /// Check whether this array value contains all elements of `other`.
    pub fn contains_all(&self, other: &CatalogValue) -> bool {
        match other {
            CatalogValue::IntArray(items) => items
                .iter()
                .all(|i| self.contains_element(&CatalogValue::Int(*i))),
            CatalogValue::TextArray(items) => items
                .iter()
                .all(|s| self.contains_element(&CatalogValue::Text(s.clone()))),
            CatalogValue::Text(s) => {
                let trimmed = s.trim();
                if trimmed.starts_with('{') && trimmed.ends_with('}') {
                    let inner = &trimmed[1..trimmed.len() - 1];
                    if inner.is_empty() {
                        return true;
                    }
                    return inner
                        .split(',')
                        .map(|p| p.trim().trim_matches('"').trim_matches('\''))
                        .all(|item| self.contains_element(&CatalogValue::Text(item.to_string())));
                }
                self.contains_element(other)
            }
            _ => self.contains_element(other),
        }
    }

    /// Compare two catalog values cleanly across compatible numeric/text types.
    pub fn cmp_sql(&self, other: &CatalogValue) -> Option<Ordering> {
        match (self, other) {
            (CatalogValue::Null, _) | (_, CatalogValue::Null) => None,
            (CatalogValue::Bool(a), CatalogValue::Bool(b)) => Some(a.cmp(b)),
            (CatalogValue::Int(a), CatalogValue::Int(b)) => Some(a.cmp(b)),
            (CatalogValue::Float(a), CatalogValue::Float(b)) => a.partial_cmp(b),
            (CatalogValue::Int(a), CatalogValue::Float(b)) => (*a as f64).partial_cmp(b),
            (CatalogValue::Float(a), CatalogValue::Int(b)) => a.partial_cmp(&(*b as f64)),
            (CatalogValue::Int(a), CatalogValue::Text(b)) => {
                if let Ok(bi) = b.trim().parse::<i64>() {
                    Some(a.cmp(&bi))
                } else {
                    Some(a.to_string().cmp(b))
                }
            }
            (CatalogValue::Text(a), CatalogValue::Int(b)) => {
                if let Ok(ai) = a.trim().parse::<i64>() {
                    Some(ai.cmp(b))
                } else {
                    Some(a.cmp(&b.to_string()))
                }
            }
            (CatalogValue::Bool(a), CatalogValue::Text(_)) => other.as_bool().map(|bb| a.cmp(&bb)),
            (CatalogValue::Text(_), CatalogValue::Bool(b)) => self.as_bool().map(|ab| ab.cmp(b)),
            _ => Some(self.to_string_lossy().cmp(&other.to_string_lossy())),
        }
    }
}

/// Column header metadata for materialized tables and query outputs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogColumn {
    pub name: String,
    pub pg_type_oid: i32,
}

impl CatalogColumn {
    pub fn new(name: impl Into<String>, pg_type_oid: i32) -> Self {
        Self {
            name: name.into(),
            pg_type_oid,
        }
    }

    pub fn text(name: impl Into<String>) -> Self {
        Self::new(name, 25)
    }

    pub fn int8(name: impl Into<String>) -> Self {
        Self::new(name, 20)
    }

    pub fn int4(name: impl Into<String>) -> Self {
        Self::new(name, 23)
    }

    pub fn int2(name: impl Into<String>) -> Self {
        Self::new(name, 21)
    }

    pub fn float8(name: impl Into<String>) -> Self {
        Self::new(name, 701)
    }

    pub fn bool_col(name: impl Into<String>) -> Self {
        Self::new(name, 16)
    }
}

/// A materialized system catalog table (`pg_class`, `information_schema.columns`, etc.).
#[derive(Debug, Clone)]
pub struct CatalogTable {
    pub schema: String,
    pub name: String,
    pub columns: Vec<CatalogColumn>,
    pub rows: Vec<Vec<CatalogValue>>,
}

impl CatalogTable {
    pub fn column_index(&self, col_name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(col_name))
    }
}

/// Result returned by executing a query against the virtual catalog engine.
#[derive(Debug, Clone)]
pub struct CatalogResult {
    pub columns: Vec<CatalogColumn>,
    pub rows: Vec<Vec<Option<CatalogValue>>>,
}

/// Full materialized system catalog for one database snapshot.
#[derive(Debug, Clone)]
pub struct VirtualCatalog {
    pub dialect: CatalogDialect,
    pub database_name: String,
    pub server_version: String,
    pub tables: BTreeMap<String, CatalogTable>,
    /// Lookup map from `"table"` / `"public.table"` -> `pg_class.oid`
    pub relation_oids: BTreeMap<String, i64>,
}

impl VirtualCatalog {
    /// Lookup table by schema-qualified (`pg_catalog.pg_class`) or short (`pg_class`) name.
    pub fn get_table(&self, name: &str) -> Option<&CatalogTable> {
        let normalized = normalize_ident(name);
        if let Some(tbl) = self.tables.get(&normalized) {
            return Some(tbl);
        }
        // Unqualified lookup prioritizes pg_catalog first, then information_schema
        let pg_key = format!("pg_catalog.{normalized}");
        if let Some(tbl) = self.tables.get(&pg_key) {
            return Some(tbl);
        }
        let info_key = format!("information_schema.{normalized}");
        self.tables.get(&info_key)
    }

    /// Check if a table name refers to a virtual catalog relation.
    pub fn is_catalog_table_name(name: &str) -> bool {
        let normalized = normalize_ident(name);
        normalized.starts_with("pg_catalog.")
            || normalized.starts_with("information_schema.")
            || normalized.starts_with("pg_")
    }
}

pub fn normalize_ident(raw: &str) -> String {
    raw.split('.')
        .map(|part| {
            part.trim()
                .trim_matches(|c| c == '"' || c == '`' || c == '\'')
                .to_ascii_lowercase()
        })
        .collect::<Vec<_>>()
        .join(".")
}
