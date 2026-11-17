//! Relational schema metadata & SQL/JSON type mapping for TapisaurusDB collections.
//!
//! Bridges schema-optional NoSQL documents, JSON Schema definitions, and relational DDL
//! (`CREATE TABLE`, `ALTER TABLE`, `FOREIGN KEY`) so virtual system catalogs and ORMs
//! observe strongly typed columns, nullability, defaults, keys, and references.

use serde::{Deserialize, Serialize};

use super::model::Value;

/// Canonical PostgreSQL type OID constants used by catalog reflection.
pub const PG_OID_BOOL: i32 = 16;
pub const PG_OID_BYTEA: i32 = 17;
pub const PG_OID_INT8: i32 = 20;
pub const PG_OID_INT2: i32 = 21;
pub const PG_OID_INT4: i32 = 23;
pub const PG_OID_TEXT: i32 = 25;
pub const PG_OID_FLOAT4: i32 = 700;
pub const PG_OID_FLOAT8: i32 = 701;
pub const PG_OID_BPCHAR: i32 = 1042;
pub const PG_OID_VARCHAR: i32 = 1043;
pub const PG_OID_DATE: i32 = 1082;
pub const PG_OID_TIME: i32 = 1083;
pub const PG_OID_TIMESTAMP: i32 = 1114;
pub const PG_OID_TIMESTAMPTZ: i32 = 1184;
pub const PG_OID_NUMERIC: i32 = 1700;
pub const PG_OID_UUID: i32 = 2950;
pub const PG_OID_JSON: i32 = 114;
pub const PG_OID_JSONB: i32 = 3802;
pub const PG_OID_VECTOR: i32 = 16390;

fn default_fk_rule() -> String {
    "NO ACTION".to_string()
}

/// Column definition tracked per collection for DDL and ORM introspection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    /// SQL standard data_type (e.g. `"bigint"`, `"character varying"`, `"text"`, `"double precision"`).
    pub data_type: String,
    /// PostgreSQL udt_name (e.g. `"int8"`, `"varchar"`, `"text"`, `"float8"`, `"bool"`).
    pub udt_name: String,
    /// PostgreSQL pg_type.oid.
    pub pg_type_oid: i32,
    /// PostgreSQL atttypmod (-1 unless bounded length / precision).
    #[serde(default = "default_atttypmod")]
    pub pg_type_mod: i32,
    /// MySQL COLUMN_TYPE representation (e.g. `"bigint"`, `"varchar(255)"`, `"tinyint(1)"`).
    pub mysql_type: String,
    pub nullable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_expr: Option<String>,
    #[serde(default)]
    pub is_primary_key: bool,
    #[serde(default)]
    pub is_unique: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_length: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub numeric_precision: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub numeric_scale: Option<i32>,
}

fn default_atttypmod() -> i32 {
    -1
}

/// Foreign key constraint tracked per collection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignKeyDef {
    pub constraint_name: String,
    pub column: String,
    pub referenced_table: String,
    pub referenced_column: String,
    #[serde(default = "default_fk_rule")]
    pub on_delete: String,
    #[serde(default = "default_fk_rule")]
    pub on_update: String,
}

/// Collection-level relational schema persisted alongside collection data.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationalSchema {
    #[serde(default)]
    pub primary_key: Vec<String>,
    #[serde(default)]
    pub columns: Vec<ColumnSchema>,
    #[serde(default)]
    pub foreign_keys: Vec<ForeignKeyDef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

struct NormalizedType {
    data_type: String,
    udt_name: String,
    pg_type_oid: i32,
    pg_type_mod: i32,
    mysql_type: String,
    max_length: Option<i32>,
    numeric_precision: Option<i32>,
    numeric_scale: Option<i32>,
}

fn parse_type_params(raw_lower: &str) -> (Option<i32>, Option<i32>) {
    let Some(open) = raw_lower.find('(') else {
        return (None, None);
    };
    let Some(close) = raw_lower[open + 1..].find(')') else {
        return (None, None);
    };
    let inner = &raw_lower[open + 1..open + 1 + close];
    let mut parts = inner
        .split(',')
        .filter_map(|p| p.trim().parse::<i32>().ok());
    (parts.next(), parts.next())
}

fn normalize_sql_type(raw_type: &str) -> NormalizedType {
    let trimmed = raw_type.trim();
    let lower = trimmed.to_ascii_lowercase();
    let base = lower.split('(').next().unwrap_or(&lower).trim();
    let (p1, p2) = parse_type_params(&lower);

    if base == "bool" || base == "boolean" || lower == "tinyint(1)" {
        return NormalizedType {
            data_type: "boolean".into(),
            udt_name: "bool".into(),
            pg_type_oid: PG_OID_BOOL,
            pg_type_mod: -1,
            mysql_type: "tinyint(1)".into(),
            max_length: None,
            numeric_precision: None,
            numeric_scale: None,
        };
    }

    if base == "smallint" || base == "int2" || base == "smallserial" {
        return NormalizedType {
            data_type: "smallint".into(),
            udt_name: "int2".into(),
            pg_type_oid: PG_OID_INT2,
            pg_type_mod: -1,
            mysql_type: "smallint".into(),
            max_length: None,
            numeric_precision: Some(16),
            numeric_scale: Some(0),
        };
    }

    if base == "int" || base == "integer" || base == "int4" || base == "serial" {
        return NormalizedType {
            data_type: "integer".into(),
            udt_name: "int4".into(),
            pg_type_oid: PG_OID_INT4,
            pg_type_mod: -1,
            mysql_type: "int".into(),
            max_length: None,
            numeric_precision: Some(32),
            numeric_scale: Some(0),
        };
    }

    if base == "bigint" || base == "int8" || base == "bigserial" {
        return NormalizedType {
            data_type: "bigint".into(),
            udt_name: "int8".into(),
            pg_type_oid: PG_OID_INT8,
            pg_type_mod: -1,
            mysql_type: "bigint".into(),
            max_length: None,
            numeric_precision: Some(64),
            numeric_scale: Some(0),
        };
    }

    if base == "real" || base == "float4" {
        return NormalizedType {
            data_type: "real".into(),
            udt_name: "float4".into(),
            pg_type_oid: PG_OID_FLOAT4,
            pg_type_mod: -1,
            mysql_type: "float".into(),
            max_length: None,
            numeric_precision: Some(24),
            numeric_scale: None,
        };
    }

    if base == "double precision" || base == "double" || base == "float8" || base == "float" {
        return NormalizedType {
            data_type: "double precision".into(),
            udt_name: "float8".into(),
            pg_type_oid: PG_OID_FLOAT8,
            pg_type_mod: -1,
            mysql_type: "double".into(),
            max_length: None,
            numeric_precision: Some(53),
            numeric_scale: None,
        };
    }

    if base == "numeric" || base == "decimal" {
        let prec = p1.unwrap_or(10);
        let scale = p2.unwrap_or(0);
        let typmod = ((prec << 16) | scale) + 4;
        return NormalizedType {
            data_type: "numeric".into(),
            udt_name: "numeric".into(),
            pg_type_oid: PG_OID_NUMERIC,
            pg_type_mod: typmod,
            mysql_type: format!("decimal({prec},{scale})"),
            max_length: None,
            numeric_precision: Some(prec),
            numeric_scale: Some(scale),
        };
    }

    if base == "varchar" || base == "character varying" || base == "nvarchar" {
        let mysql_len = p1.unwrap_or(255);
        let pg_mod = p1.map(|len| len + 4).unwrap_or(-1);
        return NormalizedType {
            data_type: "character varying".into(),
            udt_name: "varchar".into(),
            pg_type_oid: PG_OID_VARCHAR,
            pg_type_mod: pg_mod,
            mysql_type: format!("varchar({mysql_len})"),
            max_length: p1,
            numeric_precision: None,
            numeric_scale: None,
        };
    }

    if base == "char" || base == "character" || base == "bpchar" {
        let len = p1.unwrap_or(1);
        return NormalizedType {
            data_type: "character".into(),
            udt_name: "bpchar".into(),
            pg_type_oid: PG_OID_BPCHAR,
            pg_type_mod: len + 4,
            mysql_type: format!("char({len})"),
            max_length: Some(len),
            numeric_precision: None,
            numeric_scale: None,
        };
    }

    if base == "uuid" {
        return NormalizedType {
            data_type: "uuid".into(),
            udt_name: "uuid".into(),
            pg_type_oid: PG_OID_UUID,
            pg_type_mod: -1,
            mysql_type: "char(36)".into(),
            max_length: Some(36),
            numeric_precision: None,
            numeric_scale: None,
        };
    }

    if base == "json" {
        return NormalizedType {
            data_type: "json".into(),
            udt_name: "json".into(),
            pg_type_oid: PG_OID_JSON,
            pg_type_mod: -1,
            mysql_type: "json".into(),
            max_length: None,
            numeric_precision: None,
            numeric_scale: None,
        };
    }

    if base == "jsonb" {
        return NormalizedType {
            data_type: "jsonb".into(),
            udt_name: "jsonb".into(),
            pg_type_oid: PG_OID_JSONB,
            pg_type_mod: -1,
            mysql_type: "json".into(),
            max_length: None,
            numeric_precision: None,
            numeric_scale: None,
        };
    }

    if base == "date" {
        return NormalizedType {
            data_type: "date".into(),
            udt_name: "date".into(),
            pg_type_oid: PG_OID_DATE,
            pg_type_mod: -1,
            mysql_type: "date".into(),
            max_length: None,
            numeric_precision: None,
            numeric_scale: None,
        };
    }

    if base == "time" || base == "time without time zone" {
        return NormalizedType {
            data_type: "time without time zone".into(),
            udt_name: "time".into(),
            pg_type_oid: PG_OID_TIME,
            pg_type_mod: -1,
            mysql_type: "time".into(),
            max_length: None,
            numeric_precision: None,
            numeric_scale: None,
        };
    }

    if base == "timestamptz" || base == "timestamp with time zone" {
        return NormalizedType {
            data_type: "timestamp with time zone".into(),
            udt_name: "timestamptz".into(),
            pg_type_oid: PG_OID_TIMESTAMPTZ,
            pg_type_mod: -1,
            mysql_type: "datetime".into(),
            max_length: None,
            numeric_precision: None,
            numeric_scale: None,
        };
    }

    if base == "timestamp" || base == "timestamp without time zone" || base == "datetime" {
        return NormalizedType {
            data_type: "timestamp without time zone".into(),
            udt_name: "timestamp".into(),
            pg_type_oid: PG_OID_TIMESTAMP,
            pg_type_mod: -1,
            mysql_type: "datetime".into(),
            max_length: None,
            numeric_precision: None,
            numeric_scale: None,
        };
    }

    if base == "bytea" || base == "blob" || base == "binary" || base == "varbinary" {
        return NormalizedType {
            data_type: "bytea".into(),
            udt_name: "bytea".into(),
            pg_type_oid: PG_OID_BYTEA,
            pg_type_mod: -1,
            mysql_type: "longblob".into(),
            max_length: None,
            numeric_precision: None,
            numeric_scale: None,
        };
    }

    if base == "vector" {
        let typmod = p1.unwrap_or(-1);
        return NormalizedType {
            data_type: "USER-DEFINED".into(),
            udt_name: "vector".into(),
            pg_type_oid: PG_OID_VECTOR,
            pg_type_mod: typmod,
            mysql_type: "json".into(),
            max_length: p1,
            numeric_precision: None,
            numeric_scale: None,
        };
    }

    NormalizedType {
        data_type: "text".into(),
        udt_name: "text".into(),
        pg_type_oid: PG_OID_TEXT,
        pg_type_mod: -1,
        mysql_type: "text".into(),
        max_length: None,
        numeric_precision: None,
        numeric_scale: None,
    }
}

impl ColumnSchema {
    /// Construct a column schema from an explicit SQL type expression.
    pub fn from_sql_type(
        name: impl Into<String>,
        raw_sql_type: &str,
        nullable: bool,
        default_expr: Option<String>,
        is_primary_key: bool,
        is_unique: bool,
    ) -> Self {
        let norm = normalize_sql_type(raw_sql_type);
        Self {
            name: name.into(),
            data_type: norm.data_type,
            udt_name: norm.udt_name,
            pg_type_oid: norm.pg_type_oid,
            pg_type_mod: norm.pg_type_mod,
            mysql_type: norm.mysql_type,
            nullable: if is_primary_key { false } else { nullable },
            default_expr,
            is_primary_key,
            is_unique,
            max_length: norm.max_length,
            numeric_precision: norm.numeric_precision,
            numeric_scale: norm.numeric_scale,
        }
    }

    /// Infer a column schema from a runtime document field value.
    pub fn from_value(name: impl Into<String>, value: &Value) -> Self {
        let sql_type = match value {
            Value::Boolean(_) => "boolean",
            Value::Integer(_) => "bigint",
            Value::Float(_) => "double precision",
            Value::String(_) => "text",
            Value::Array(_) | Value::Object(_) => "jsonb",
            Value::DateTime(_) => "timestamptz",
            Value::Uuid(_) => "uuid",
            Value::Vector(v) if !v.is_empty() => "vector",
            Value::Vector(_) => "jsonb",
            Value::Binary(_) => "bytea",
            Value::Null => "text",
        };
        Self::from_sql_type(name, sql_type, true, None, false, false)
    }

    /// Default primary key column `_id` synthesized for schemaless NoSQL collections.
    pub fn default_primary_id() -> Self {
        Self {
            name: "_id".into(),
            data_type: "text".into(),
            udt_name: "text".into(),
            pg_type_oid: PG_OID_TEXT,
            pg_type_mod: -1,
            mysql_type: "varchar(64)".into(),
            nullable: false,
            default_expr: None,
            is_primary_key: true,
            is_unique: true,
            max_length: Some(64),
            numeric_precision: None,
            numeric_scale: None,
        }
    }
}

impl RelationalSchema {
    /// Extract relational columns, primary key, and foreign keys from a JSON Schema object.
    pub fn from_json_schema(schema: &serde_json::Value) -> Option<Self> {
        let obj = schema.as_object()?;
        let props = obj.get("properties")?.as_object()?;
        let required: std::collections::HashSet<&str> = obj
            .get("required")
            .and_then(|r| r.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();

        let pk_list: Vec<String> = obj
            .get("x-primary-key")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|x| x.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();

        let mut columns = Vec::new();
        for (prop_name, prop_def) in props {
            let json_type = prop_def
                .get("type")
                .and_then(|t| t.as_str())
                .unwrap_or("string");
            let format = prop_def.get("format").and_then(|f| f.as_str());
            let sql_type = match (json_type, format) {
                ("string", Some("uuid")) => "uuid",
                ("string", Some("date-time")) => "timestamptz",
                ("string", Some("date")) => "date",
                ("integer", _) => "bigint",
                ("number", _) => "double precision",
                ("boolean", _) => "boolean",
                ("array", _) | ("object", _) => "jsonb",
                _ => "text",
            };
            let is_pk = pk_list.iter().any(|p| p == prop_name);
            let is_unique = prop_def
                .get("unique")
                .or_else(|| prop_def.get("x-unique"))
                .and_then(|u| u.as_bool())
                .unwrap_or(false);
            let nullable = !required.contains(prop_name.as_str()) && !is_pk;
            let default_expr = prop_def
                .get("default")
                .and_then(|d| serde_json::to_string(d).ok());

            columns.push(ColumnSchema::from_sql_type(
                prop_name.clone(),
                sql_type,
                nullable,
                default_expr,
                is_pk,
                is_unique,
            ));
        }

        let primary_key = if !pk_list.is_empty() {
            pk_list
        } else if columns.iter().any(|c| c.name == "id") {
            vec!["id".to_string()]
        } else {
            vec!["_id".to_string()]
        };

        let mut foreign_keys = Vec::new();
        if let Some(fks) = obj.get("x-foreign-keys").and_then(|v| v.as_array()) {
            for fk in fks {
                if let (Some(col), Some(ref_tbl), Some(ref_col)) = (
                    fk.get("column").and_then(|v| v.as_str()),
                    fk.get("referenced_table").and_then(|v| v.as_str()),
                    fk.get("referenced_column").and_then(|v| v.as_str()),
                ) {
                    let cname = fk
                        .get("constraint_name")
                        .and_then(|v| v.as_str())
                        .map(str::to_string)
                        .unwrap_or_else(|| format!("fk_{col}_{ref_tbl}"));
                    foreign_keys.push(ForeignKeyDef {
                        constraint_name: cname,
                        column: col.to_string(),
                        referenced_table: ref_tbl.to_string(),
                        referenced_column: ref_col.to_string(),
                        on_delete: fk
                            .get("on_delete")
                            .and_then(|v| v.as_str())
                            .unwrap_or("NO ACTION")
                            .to_string(),
                        on_update: fk
                            .get("on_update")
                            .and_then(|v| v.as_str())
                            .unwrap_or("NO ACTION")
                            .to_string(),
                    });
                }
            }
        }

        Some(Self {
            primary_key,
            columns,
            foreign_keys,
            comment: obj
                .get("description")
                .and_then(|d| d.as_str())
                .map(str::to_string),
        })
    }

    pub fn add_column(&mut self, col: ColumnSchema) {
        if let Some(existing) = self.columns.iter_mut().find(|c| c.name == col.name) {
            *existing = col;
        } else {
            self.columns.push(col);
        }
    }

    pub fn drop_column(&mut self, name: &str) {
        self.columns.retain(|c| c.name != name);
        self.primary_key.retain(|pk| pk != name);
        self.foreign_keys.retain(|fk| fk.column != name);
    }

    pub fn add_foreign_key(&mut self, fk: ForeignKeyDef) {
        self.foreign_keys
            .retain(|existing| existing.constraint_name != fk.constraint_name);
        self.foreign_keys.push(fk);
    }
}
