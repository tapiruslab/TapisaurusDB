//! Dynamic System Catalog Materializer.
//!
//! Converts live TapisaurusDB collection schemas, inferred fields, foreign keys,
//! secondary B-Tree indexes, and vector HNSW/DiskANN indexes into a consistent
//! relational set of `pg_catalog.*` and `information_schema.*` tables with
//! deterministic, cross-joinable OIDs.

use std::collections::{BTreeMap, HashMap, HashSet};
use tapisaurus_core::{ColumnSchema, ForeignKeyDef};

use crate::types::{CatalogColumn, CatalogDialect, CatalogTable, CatalogValue, VirtualCatalog};

/// Metadata describing an index on a collection (primary key, secondary B-Tree, or vector index).
#[derive(Debug, Clone)]
pub struct IndexCatalogMeta {
    pub name: String,
    pub fields: Vec<String>,
    pub unique: bool,
    /// `"btree"`, `"hash"`, `"hnsw"`, or `"diskann"`.
    pub method: String,
    pub is_primary: bool,
}

/// Snapshot metadata for a single collection / SQL table.
#[derive(Debug, Clone)]
pub struct CollectionCatalogMeta {
    pub name: String,
    pub row_count: u64,
    pub columns: Vec<ColumnSchema>,
    pub primary_key: Vec<String>,
    pub foreign_keys: Vec<ForeignKeyDef>,
    pub indexes: Vec<IndexCatalogMeta>,
    pub comment: Option<String>,
}

/// Database-wide snapshot used to materialize virtual catalogs.
#[derive(Debug, Clone)]
pub struct CatalogSnapshot {
    pub database_name: String,
    pub server_version: String,
    pub collections: Vec<CollectionCatalogMeta>,
}

struct OidAllocator {
    used: HashSet<i64>,
}

impl OidAllocator {
    fn new() -> Self {
        let mut used = HashSet::new();
        used.insert(11);
        used.insert(2200);
        used.insert(12345);
        used.insert(16384);
        Self { used }
    }

    fn alloc(&mut self, tag: &str, name: &str) -> i64 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(tag.as_bytes());
        hasher.update(b":");
        hasher.update(name.as_bytes());
        let mut candidate = 20_000i64 + i64::from(hasher.finalize() & 0x1FFF_FFFF);
        while !self.used.insert(candidate) {
            candidate += 1;
        }
        candidate
    }
}

fn pg_am_oid(method: &str) -> i64 {
    match method.to_ascii_lowercase().as_str() {
        "hash" => 405,
        "gist" => 783,
        "gin" => 2742,
        "hnsw" => 16395,
        "diskann" => 16396,
        _ => 403, // btree
    }
}

fn pg_fk_action_code(rule: &str) -> &'static str {
    match rule.to_ascii_uppercase().as_str() {
        "CASCADE" => "c",
        "RESTRICT" => "r",
        "SET NULL" => "n",
        "SET DEFAULT" => "d",
        _ => "a", // NO ACTION
    }
}

fn pg_type_len_align(oid: i32) -> (i64, bool, &'static str, &'static str) {
    match oid {
        16 => (1, true, "c", "p"),                 // bool
        21 => (2, true, "s", "p"),                 // int2
        23 => (4, true, "i", "p"),                 // int4
        20 => (8, true, "d", "p"),                 // int8
        700 => (4, true, "i", "p"),                // float4
        701 => (8, true, "d", "p"),                // float8
        2950 => (16, false, "c", "p"),             // uuid
        1082 => (4, true, "i", "p"),               // date
        1083 | 1114 | 1184 => (8, true, "d", "p"), // time / timestamp
        _ => (-1, false, "i", "x"),                // varlena
    }
}

struct MaterializedCollection {
    meta: CollectionCatalogMeta,
    oid: i64,
    attnums: HashMap<String, i64>,
    effective_indexes: Vec<(IndexCatalogMeta, i64)>,
}

/// Format PostgreSQL type name from OID + typmod (matching `pg_catalog.format_type`).
pub fn format_pg_type(oid: i64, typmod: i64) -> String {
    match oid {
        16 => "boolean".into(),
        17 => "bytea".into(),
        18 => "\"char\"".into(),
        19 => "name".into(),
        20 => "bigint".into(),
        21 => "smallint".into(),
        23 => "integer".into(),
        24 => "regproc".into(),
        25 => "text".into(),
        26 => "oid".into(),
        114 => "json".into(),
        700 => "real".into(),
        701 => "double precision".into(),
        1042 => {
            if typmod > 4 {
                format!("character({})", typmod - 4)
            } else {
                "character".into()
            }
        }
        1043 => {
            if typmod > 4 {
                format!("character varying({})", typmod - 4)
            } else {
                "character varying".into()
            }
        }
        1082 => "date".into(),
        1083 => "time without time zone".into(),
        1114 => "timestamp without time zone".into(),
        1184 => "timestamp with time zone".into(),
        1186 => "interval".into(),
        1700 => {
            if typmod > 4 {
                let raw = typmod - 4;
                let prec = (raw >> 16) & 0xFFFF;
                let scale = raw & 0xFFFF;
                format!("numeric({prec},{scale})")
            } else {
                "numeric".into()
            }
        }
        2205 => "regclass".into(),
        2950 => "uuid".into(),
        3802 => "jsonb".into(),
        16390 => {
            if typmod > 0 {
                format!("vector({typmod})")
            } else {
                "vector".into()
            }
        }
        _ => "text".into(),
    }
}

/// Materialize all `pg_catalog.*` and `information_schema.*` virtual tables from `snapshot`.
pub fn materialize_catalog(snapshot: &CatalogSnapshot, dialect: CatalogDialect) -> VirtualCatalog {
    let mut oids = OidAllocator::new();
    let default_schema = match &dialect {
        CatalogDialect::Postgres => "public".to_string(),
        CatalogDialect::MySql { database } => database.clone(),
    };
    let default_catalog = match &dialect {
        CatalogDialect::Postgres => snapshot.database_name.clone(),
        CatalogDialect::MySql { .. } => "def".to_string(),
    };

    // Sort collections alphabetically for deterministic ordering across snapshots
    let mut sorted = snapshot.collections.clone();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));

    let mut items: Vec<MaterializedCollection> = Vec::with_capacity(sorted.len());
    let mut relation_oids: BTreeMap<String, i64> = BTreeMap::new();
    let mut table_attnums: HashMap<String, HashMap<String, i64>> = HashMap::new();

    for col_meta in sorted {
        let tbl_oid = oids.alloc("table", &col_meta.name);
        relation_oids.insert(col_meta.name.to_ascii_lowercase(), tbl_oid);
        relation_oids.insert(
            format!("public.{}", col_meta.name.to_ascii_lowercase()),
            tbl_oid,
        );

        let mut attnums = HashMap::new();
        for (idx, col) in col_meta.columns.iter().enumerate() {
            attnums.insert(col.name.clone(), (idx + 1) as i64);
        }
        table_attnums.insert(col_meta.name.clone(), attnums.clone());

        // Deduplicate & ensure primary key index always appears first
        let mut eff_indexes = Vec::new();
        let mut seen_idx_names = HashSet::new();
        let pk_name = format!("{}_pkey", col_meta.name);
        let pk_fields = if col_meta.primary_key.is_empty() {
            col_meta
                .columns
                .first()
                .map(|c| vec![c.name.clone()])
                .unwrap_or_else(|| vec!["_id".to_string()])
        } else {
            col_meta.primary_key.clone()
        };
        let pk_meta = IndexCatalogMeta {
            name: pk_name.clone(),
            fields: pk_fields,
            unique: true,
            method: "btree".to_string(),
            is_primary: true,
        };
        let pk_oid = oids.alloc("index", &format!("{}:{}", col_meta.name, pk_name));
        seen_idx_names.insert(pk_name);
        eff_indexes.push((pk_meta, pk_oid));

        // Add explicit UNIQUE columns that do not yet have a dedicated index entry
        for c in &col_meta.columns {
            if c.is_unique && !c.is_primary_key {
                let uq_name = format!("{}_{}_key", col_meta.name, c.name);
                let already = col_meta
                    .indexes
                    .iter()
                    .any(|idx| idx.unique && idx.fields.len() == 1 && idx.fields[0] == c.name);
                if !already && seen_idx_names.insert(uq_name.clone()) {
                    let idx_oid = oids.alloc("index", &format!("{}:{}", col_meta.name, uq_name));
                    eff_indexes.push((
                        IndexCatalogMeta {
                            name: uq_name,
                            fields: vec![c.name.clone()],
                            unique: true,
                            method: "btree".to_string(),
                            is_primary: false,
                        },
                        idx_oid,
                    ));
                }
            }
        }

        for idx in &col_meta.indexes {
            if seen_idx_names.insert(idx.name.clone()) {
                let idx_oid = oids.alloc("index", &format!("{}:{}", col_meta.name, idx.name));
                eff_indexes.push((idx.clone(), idx_oid));
            }
        }

        items.push(MaterializedCollection {
            meta: col_meta,
            oid: tbl_oid,
            attnums,
            effective_indexes: eff_indexes,
        });
    }

    let mut tables: BTreeMap<String, CatalogTable> = BTreeMap::new();

    // 1. pg_catalog core static tables
    insert_table(&mut tables, build_pg_database(snapshot));
    insert_table(&mut tables, build_pg_namespace());
    insert_table(&mut tables, build_pg_type());
    insert_table(&mut tables, build_pg_am());
    insert_table(&mut tables, build_pg_settings(snapshot));
    insert_table(&mut tables, build_empty_pg_enum());
    insert_table(&mut tables, build_empty_pg_range());
    insert_table(&mut tables, build_empty_pg_inherits());
    insert_table(&mut tables, build_empty_pg_proc());
    insert_table(&mut tables, build_empty_pg_depend());
    insert_table(&mut tables, build_empty_pg_views());
    insert_table(&mut tables, build_empty_pg_sequences());
    insert_table(&mut tables, build_pg_collation());
    insert_table(&mut tables, build_pg_tablespace());
    insert_table(&mut tables, build_pg_roles());
    insert_table(&mut tables, build_empty_pg_matviews());
    insert_table(&mut tables, build_empty_pg_opclass());
    insert_table(&mut tables, build_empty_pg_language());
    insert_table(&mut tables, build_pg_extension());

    // 2. pg_catalog dynamic tables derived from collection metadata
    insert_table(&mut tables, build_pg_class(&items));
    insert_table(&mut tables, build_pg_attribute(&items));
    insert_table(&mut tables, build_pg_attrdef(&mut oids, &items));
    insert_table(&mut tables, build_pg_index(&items));
    insert_table(
        &mut tables,
        build_pg_constraint(&mut oids, &items, &relation_oids, &table_attnums),
    );
    insert_table(&mut tables, build_pg_indexes(&items));
    insert_table(&mut tables, build_pg_tables(&items));
    insert_table(
        &mut tables,
        build_pg_stat_tables("pg_stat_user_tables", &items),
    );
    insert_table(
        &mut tables,
        build_pg_stat_tables("pg_stat_all_tables", &items),
    );
    insert_table(&mut tables, build_pg_description(&items));

    // 3. ANSI / MySQL / PostgreSQL information_schema tables
    insert_table(
        &mut tables,
        build_info_schemata(&default_catalog, &default_schema, &dialect),
    );
    insert_table(
        &mut tables,
        build_info_tables(&default_catalog, &default_schema, &items),
    );
    insert_table(
        &mut tables,
        build_info_columns(&default_catalog, &default_schema, &items),
    );
    insert_table(
        &mut tables,
        build_info_table_constraints(&default_catalog, &default_schema, &items),
    );
    insert_table(
        &mut tables,
        build_info_key_column_usage(&default_catalog, &default_schema, &items),
    );
    insert_table(
        &mut tables,
        build_info_referential_constraints(&default_catalog, &default_schema, &items),
    );
    insert_table(
        &mut tables,
        build_info_constraint_column_usage(&default_catalog, &default_schema, &items),
    );
    insert_table(
        &mut tables,
        build_info_statistics(&default_catalog, &default_schema, &items),
    );
    insert_table(&mut tables, build_empty_info_views());
    insert_table(&mut tables, build_empty_info_triggers());
    insert_table(&mut tables, build_empty_info_routines());
    insert_table(&mut tables, build_empty_info_sequences());

    VirtualCatalog {
        dialect,
        database_name: snapshot.database_name.clone(),
        server_version: snapshot.server_version.clone(),
        tables,
        relation_oids,
    }
}

fn insert_table(map: &mut BTreeMap<String, CatalogTable>, table: CatalogTable) {
    let key = format!("{}.{}", table.schema, table.name);
    map.insert(key, table);
}

fn build_pg_database(snapshot: &CatalogSnapshot) -> CatalogTable {
    CatalogTable {
        schema: "pg_catalog".into(),
        name: "pg_database".into(),
        columns: vec![
            CatalogColumn::int8("oid"),
            CatalogColumn::text("datname"),
            CatalogColumn::int8("datdba"),
            CatalogColumn::int4("encoding"),
            CatalogColumn::text("datcollate"),
            CatalogColumn::text("datctype"),
            CatalogColumn::bool_col("datistemplate"),
            CatalogColumn::bool_col("datallowconn"),
            CatalogColumn::int4("datconnlimit"),
            CatalogColumn::int8("dattablespace"),
        ],
        rows: vec![vec![
            CatalogValue::Int(16384),
            CatalogValue::Text(snapshot.database_name.clone()),
            CatalogValue::Int(10),
            CatalogValue::Int(6),
            CatalogValue::Text("en_US.UTF-8".into()),
            CatalogValue::Text("en_US.UTF-8".into()),
            CatalogValue::Bool(false),
            CatalogValue::Bool(true),
            CatalogValue::Int(-1),
            CatalogValue::Int(1663),
        ]],
    }
}

fn build_pg_namespace() -> CatalogTable {
    CatalogTable {
        schema: "pg_catalog".into(),
        name: "pg_namespace".into(),
        columns: vec![
            CatalogColumn::int8("oid"),
            CatalogColumn::text("nspname"),
            CatalogColumn::int8("nspowner"),
            CatalogColumn::text("nspacl"),
        ],
        rows: vec![
            vec![
                CatalogValue::Int(2200),
                CatalogValue::Text("public".into()),
                CatalogValue::Int(10),
                CatalogValue::Null,
            ],
            vec![
                CatalogValue::Int(11),
                CatalogValue::Text("pg_catalog".into()),
                CatalogValue::Int(10),
                CatalogValue::Null,
            ],
            vec![
                CatalogValue::Int(12345),
                CatalogValue::Text("information_schema".into()),
                CatalogValue::Int(10),
                CatalogValue::Null,
            ],
        ],
    }
}

fn build_pg_type() -> CatalogTable {
    // (oid, typname, typlen, typbyval, typtype, typcategory, typarray)
    let defs: &[(i64, &str, i64, bool, &str, &str, i64)] = &[
        (16, "bool", 1, true, "b", "B", 1000),
        (17, "bytea", -1, false, "b", "U", 1001),
        (18, "char", 1, true, "b", "Z", 1002),
        (19, "name", 64, false, "b", "S", 1003),
        (20, "int8", 8, true, "b", "N", 1016),
        (21, "int2", 2, true, "b", "N", 1005),
        (22, "int2vector", -1, false, "b", "A", 1006),
        (23, "int4", 4, true, "b", "N", 1007),
        (24, "regproc", 4, true, "b", "N", 1008),
        (25, "text", -1, false, "b", "S", 1009),
        (26, "oid", 4, true, "b", "N", 1028),
        (114, "json", -1, false, "b", "U", 199),
        (142, "xml", -1, false, "b", "U", 143),
        (700, "float4", 4, true, "b", "N", 1021),
        (701, "float8", 8, true, "b", "N", 1022),
        (1042, "bpchar", -1, false, "b", "S", 1014),
        (1043, "varchar", -1, false, "b", "S", 1015),
        (1082, "date", 4, true, "b", "D", 1182),
        (1083, "time", 8, true, "b", "D", 1183),
        (1114, "timestamp", 8, true, "b", "D", 1115),
        (1184, "timestamptz", 8, true, "b", "D", 1185),
        (1186, "interval", 16, false, "b", "T", 1187),
        (1700, "numeric", -1, false, "b", "N", 1231),
        (2205, "regclass", 4, true, "b", "N", 2210),
        (2276, "any", 4, true, "p", "P", 0),
        (2950, "uuid", 16, false, "b", "U", 2951),
        (3802, "jsonb", -1, false, "b", "U", 3807),
        (16390, "vector", -1, false, "b", "U", 16391),
    ];

    let rows = defs
        .iter()
        .map(|&(oid, name, len, byval, typtype, cat, arr)| {
            let nsp = if oid >= 16390 { 2200 } else { 11 };
            vec![
                CatalogValue::Int(oid),
                CatalogValue::Text(name.into()),
                CatalogValue::Int(nsp),
                CatalogValue::Int(10),
                CatalogValue::Int(len),
                CatalogValue::Bool(byval),
                CatalogValue::Text(typtype.into()),
                CatalogValue::Text(cat.into()),
                CatalogValue::Bool(true),
                CatalogValue::Bool(true),
                CatalogValue::Text(",".into()),
                CatalogValue::Int(0),
                CatalogValue::Int(0),
                CatalogValue::Int(arr),
                CatalogValue::Text(format!("{name}_in")),
                CatalogValue::Text(format!("{name}_out")),
                CatalogValue::Int(0),
                CatalogValue::Int(-1),
                CatalogValue::Bool(false),
                CatalogValue::Null,
            ]
        })
        .collect();

    CatalogTable {
        schema: "pg_catalog".into(),
        name: "pg_type".into(),
        columns: vec![
            CatalogColumn::int8("oid"),
            CatalogColumn::text("typname"),
            CatalogColumn::int8("typnamespace"),
            CatalogColumn::int8("typowner"),
            CatalogColumn::int2("typlen"),
            CatalogColumn::bool_col("typbyval"),
            CatalogColumn::text("typtype"),
            CatalogColumn::text("typcategory"),
            CatalogColumn::bool_col("typispreferred"),
            CatalogColumn::bool_col("typisdefined"),
            CatalogColumn::text("typdelim"),
            CatalogColumn::int8("typrelid"),
            CatalogColumn::int8("typelem"),
            CatalogColumn::int8("typarray"),
            CatalogColumn::text("typinput"),
            CatalogColumn::text("typoutput"),
            CatalogColumn::int8("typbasetype"),
            CatalogColumn::int4("typtypmod"),
            CatalogColumn::bool_col("typnotnull"),
            CatalogColumn::text("typdefault"),
        ],
        rows,
    }
}

fn build_pg_am() -> CatalogTable {
    let ams = [
        (403i64, "btree", "bthandler", "i"),
        (405, "hash", "hashhandler", "i"),
        (783, "gist", "gisthandler", "i"),
        (2742, "gin", "ginhandler", "i"),
        (16395, "hnsw", "hnswhandler", "i"),
        (16396, "diskann", "diskannhandler", "i"),
    ];
    let rows = ams
        .iter()
        .map(|&(oid, name, handler, amtype)| {
            vec![
                CatalogValue::Int(oid),
                CatalogValue::Text(name.into()),
                CatalogValue::Text(handler.into()),
                CatalogValue::Text(amtype.into()),
            ]
        })
        .collect();
    CatalogTable {
        schema: "pg_catalog".into(),
        name: "pg_am".into(),
        columns: vec![
            CatalogColumn::int8("oid"),
            CatalogColumn::text("amname"),
            CatalogColumn::text("amhandler"),
            CatalogColumn::text("amtype"),
        ],
        rows,
    }
}

fn build_pg_class(items: &[MaterializedCollection]) -> CatalogTable {
    let mut rows = Vec::new();
    for item in items {
        let relnatts = item.meta.columns.len() as i64;
        let tuples = item.meta.row_count as f64;
        // Table relation ('r')
        rows.push(vec![
            CatalogValue::Int(item.oid),
            CatalogValue::Text(item.meta.name.clone()),
            CatalogValue::Int(2200), // public namespace
            CatalogValue::Int(0),
            CatalogValue::Int(0),
            CatalogValue::Int(10),
            CatalogValue::Int(2), // heap AM
            CatalogValue::Int(item.oid),
            CatalogValue::Int(0),
            CatalogValue::Int(((item.meta.row_count / 64) + 1) as i64),
            CatalogValue::Float(tuples),
            CatalogValue::Int(((item.meta.row_count / 64) + 1) as i64),
            CatalogValue::Int(0),
            CatalogValue::Bool(!item.effective_indexes.is_empty()),
            CatalogValue::Bool(false),
            CatalogValue::Text("p".into()),
            CatalogValue::Text("r".into()),
            CatalogValue::Int(relnatts),
            CatalogValue::Int(0),
            CatalogValue::Bool(false),
            CatalogValue::Bool(false),
            CatalogValue::Bool(false),
            CatalogValue::Bool(false),
            CatalogValue::Bool(false),
            CatalogValue::Bool(true),
            CatalogValue::Text("d".into()),
            CatalogValue::Bool(false),
            CatalogValue::Null,
            CatalogValue::Null,
        ]);

        // Index relations ('i')
        for (idx, idx_oid) in &item.effective_indexes {
            rows.push(vec![
                CatalogValue::Int(*idx_oid),
                CatalogValue::Text(idx.name.clone()),
                CatalogValue::Int(2200),
                CatalogValue::Int(0),
                CatalogValue::Int(0),
                CatalogValue::Int(10),
                CatalogValue::Int(pg_am_oid(&idx.method)),
                CatalogValue::Int(*idx_oid),
                CatalogValue::Int(0),
                CatalogValue::Int(1),
                CatalogValue::Float(tuples),
                CatalogValue::Int(1),
                CatalogValue::Int(0),
                CatalogValue::Bool(false),
                CatalogValue::Bool(false),
                CatalogValue::Text("p".into()),
                CatalogValue::Text("i".into()),
                CatalogValue::Int(idx.fields.len() as i64),
                CatalogValue::Int(0),
                CatalogValue::Bool(false),
                CatalogValue::Bool(false),
                CatalogValue::Bool(false),
                CatalogValue::Bool(false),
                CatalogValue::Bool(false),
                CatalogValue::Bool(true),
                CatalogValue::Text("n".into()),
                CatalogValue::Bool(false),
                CatalogValue::Null,
                CatalogValue::Null,
            ]);
        }
    }

    CatalogTable {
        schema: "pg_catalog".into(),
        name: "pg_class".into(),
        columns: vec![
            CatalogColumn::int8("oid"),
            CatalogColumn::text("relname"),
            CatalogColumn::int8("relnamespace"),
            CatalogColumn::int8("reltype"),
            CatalogColumn::int8("reloftype"),
            CatalogColumn::int8("relowner"),
            CatalogColumn::int8("relam"),
            CatalogColumn::int8("relfilenode"),
            CatalogColumn::int8("reltablespace"),
            CatalogColumn::int4("relpages"),
            CatalogColumn::float8("reltuples"),
            CatalogColumn::int4("relallvisible"),
            CatalogColumn::int8("reltoastrelid"),
            CatalogColumn::bool_col("relhasindex"),
            CatalogColumn::bool_col("relisshared"),
            CatalogColumn::text("relpersistence"),
            CatalogColumn::text("relkind"),
            CatalogColumn::int2("relnatts"),
            CatalogColumn::int2("relchecks"),
            CatalogColumn::bool_col("relhasrules"),
            CatalogColumn::bool_col("relhastriggers"),
            CatalogColumn::bool_col("relhassubclass"),
            CatalogColumn::bool_col("relrowsecurity"),
            CatalogColumn::bool_col("relhasrowsecurity"),
            CatalogColumn::bool_col("relispopulated"),
            CatalogColumn::text("relreplident"),
            CatalogColumn::bool_col("relispartition"),
            CatalogColumn::text("relacl"),
            CatalogColumn::new("reloptions", 1009),
        ],
        rows,
    }
}

fn build_pg_attribute(items: &[MaterializedCollection]) -> CatalogTable {
    let mut rows = Vec::new();
    for item in items {
        for (pos, col) in item.meta.columns.iter().enumerate() {
            let (attlen, attbyval, attalign, attstorage) = pg_type_len_align(col.pg_type_oid);
            rows.push(vec![
                CatalogValue::Int(item.oid),
                CatalogValue::Text(col.name.clone()),
                CatalogValue::Int(i64::from(col.pg_type_oid)),
                CatalogValue::Int(-1),
                CatalogValue::Int(attlen),
                CatalogValue::Int((pos + 1) as i64),
                CatalogValue::Int(0),
                CatalogValue::Int(-1),
                CatalogValue::Int(i64::from(col.pg_type_mod)),
                CatalogValue::Bool(attbyval),
                CatalogValue::Text(attalign.into()),
                CatalogValue::Text(attstorage.into()),
                CatalogValue::Text("".into()),
                CatalogValue::Bool(!col.nullable),
                CatalogValue::Bool(col.default_expr.is_some()),
                CatalogValue::Bool(false),
                CatalogValue::Text("".into()),
                CatalogValue::Text("".into()),
                CatalogValue::Bool(false),
                CatalogValue::Bool(true),
                CatalogValue::Int(0),
                CatalogValue::Int(100),
                CatalogValue::Null,
                CatalogValue::Null,
            ]);
        }
    }

    CatalogTable {
        schema: "pg_catalog".into(),
        name: "pg_attribute".into(),
        columns: vec![
            CatalogColumn::int8("attrelid"),
            CatalogColumn::text("attname"),
            CatalogColumn::int8("atttypid"),
            CatalogColumn::int4("attstattarget"),
            CatalogColumn::int2("attlen"),
            CatalogColumn::int2("attnum"),
            CatalogColumn::int4("attndims"),
            CatalogColumn::int4("attcacheoff"),
            CatalogColumn::int4("atttypmod"),
            CatalogColumn::bool_col("attbyval"),
            CatalogColumn::text("attalign"),
            CatalogColumn::text("attstorage"),
            CatalogColumn::text("attcompression"),
            CatalogColumn::bool_col("attnotnull"),
            CatalogColumn::bool_col("atthasdef"),
            CatalogColumn::bool_col("atthasmissing"),
            CatalogColumn::text("attidentity"),
            CatalogColumn::text("attgenerated"),
            CatalogColumn::bool_col("attisdropped"),
            CatalogColumn::bool_col("attislocal"),
            CatalogColumn::int4("attinhcount"),
            CatalogColumn::int8("attcollation"),
            CatalogColumn::text("attacl"),
            CatalogColumn::text("attoptions"),
        ],
        rows,
    }
}

fn build_pg_attrdef(oids: &mut OidAllocator, items: &[MaterializedCollection]) -> CatalogTable {
    let mut rows = Vec::new();
    for item in items {
        for (pos, col) in item.meta.columns.iter().enumerate() {
            if let Some(ref def_expr) = col.default_expr {
                let def_oid = oids.alloc("attrdef", &format!("{}:{}", item.meta.name, col.name));
                rows.push(vec![
                    CatalogValue::Int(def_oid),
                    CatalogValue::Int(item.oid),
                    CatalogValue::Int((pos + 1) as i64),
                    CatalogValue::Text(def_expr.clone()),
                    CatalogValue::Text(def_expr.clone()),
                ]);
            }
        }
    }
    CatalogTable {
        schema: "pg_catalog".into(),
        name: "pg_attrdef".into(),
        columns: vec![
            CatalogColumn::int8("oid"),
            CatalogColumn::int8("adrelid"),
            CatalogColumn::int2("adnum"),
            CatalogColumn::text("adbin"),
            CatalogColumn::text("adsrc"),
        ],
        rows,
    }
}

fn build_pg_index(items: &[MaterializedCollection]) -> CatalogTable {
    let mut rows = Vec::new();
    for item in items {
        for (idx, idx_oid) in &item.effective_indexes {
            let indkey: Vec<i64> = idx
                .fields
                .iter()
                .filter_map(|f| item.attnums.get(f).copied())
                .collect();
            let natts = indkey.len().max(1) as i64;
            rows.push(vec![
                CatalogValue::Int(*idx_oid),
                CatalogValue::Int(item.oid),
                CatalogValue::Int(natts),
                CatalogValue::Int(natts),
                CatalogValue::Bool(idx.unique),
                CatalogValue::Bool(false),
                CatalogValue::Bool(idx.is_primary),
                CatalogValue::Bool(false),
                CatalogValue::Bool(true),
                CatalogValue::Bool(false),
                CatalogValue::Bool(true),
                CatalogValue::Bool(false),
                CatalogValue::Bool(true),
                CatalogValue::Bool(true),
                CatalogValue::Bool(false),
                CatalogValue::IntArray(indkey),
                CatalogValue::IntArray(vec![0; natts as usize]),
                CatalogValue::IntArray(vec![0; natts as usize]),
                CatalogValue::Text(vec!["0"; natts as usize].join(" ")),
                CatalogValue::Null,
                CatalogValue::Null,
            ]);
        }
    }
    CatalogTable {
        schema: "pg_catalog".into(),
        name: "pg_index".into(),
        columns: vec![
            CatalogColumn::int8("indexrelid"),
            CatalogColumn::int8("indrelid"),
            CatalogColumn::int2("indnatts"),
            CatalogColumn::int2("indnkeyatts"),
            CatalogColumn::bool_col("indisunique"),
            CatalogColumn::bool_col("indnullsnotdistinct"),
            CatalogColumn::bool_col("indisprimary"),
            CatalogColumn::bool_col("indisexclusion"),
            CatalogColumn::bool_col("indimmediate"),
            CatalogColumn::bool_col("indisclustered"),
            CatalogColumn::bool_col("indisvalid"),
            CatalogColumn::bool_col("indcheckxmin"),
            CatalogColumn::bool_col("indisready"),
            CatalogColumn::bool_col("indislive"),
            CatalogColumn::bool_col("indisreplident"),
            CatalogColumn::new("indkey", 1016),
            CatalogColumn::new("indcollation", 1016),
            CatalogColumn::new("indclass", 1016),
            CatalogColumn::new("indoption", 22), // int2vector (space-separated integers)
            CatalogColumn::text("indexprs"),
            CatalogColumn::text("indpred"),
        ],
        rows,
    }
}

fn build_pg_constraint(
    oids: &mut OidAllocator,
    items: &[MaterializedCollection],
    relation_oids: &BTreeMap<String, i64>,
    table_attnums: &HashMap<String, HashMap<String, i64>>,
) -> CatalogTable {
    let mut rows = Vec::new();
    for item in items {
        for (idx, idx_oid) in &item.effective_indexes {
            if !idx.is_primary && !idx.unique {
                continue;
            }
            let con_oid = oids.alloc("constraint", &format!("{}:{}", item.meta.name, idx.name));
            let contype = if idx.is_primary { "p" } else { "u" };
            let conkey: Vec<i64> = idx
                .fields
                .iter()
                .filter_map(|f| item.attnums.get(f).copied())
                .collect();
            let consrc = if idx.is_primary {
                format!("PRIMARY KEY ({})", idx.fields.join(", "))
            } else {
                format!("UNIQUE ({})", idx.fields.join(", "))
            };
            rows.push(vec![
                CatalogValue::Int(con_oid),
                CatalogValue::Text(idx.name.clone()),
                CatalogValue::Int(2200),
                CatalogValue::Text(contype.into()),
                CatalogValue::Bool(false),
                CatalogValue::Bool(false),
                CatalogValue::Bool(true),
                CatalogValue::Int(item.oid),
                CatalogValue::Int(0),
                CatalogValue::Int(*idx_oid),
                CatalogValue::Int(0),
                CatalogValue::Int(0),
                CatalogValue::Text(" ".into()),
                CatalogValue::Text(" ".into()),
                CatalogValue::Text(" ".into()),
                CatalogValue::Bool(true),
                CatalogValue::Int(0),
                CatalogValue::Bool(true),
                CatalogValue::IntArray(conkey),
                CatalogValue::Null,
                CatalogValue::Null,
                CatalogValue::Text(consrc),
            ]);
        }

        for fk in &item.meta.foreign_keys {
            let con_oid = oids.alloc(
                "constraint",
                &format!("{}:{}", item.meta.name, fk.constraint_name),
            );
            let local_att = item.attnums.get(&fk.column).copied().unwrap_or(1);
            let ref_table_lower = fk.referenced_table.to_ascii_lowercase();
            let ref_oid = relation_oids.get(&ref_table_lower).copied().unwrap_or(0);
            let ref_att = table_attnums
                .get(&fk.referenced_table)
                .and_then(|m| m.get(&fk.referenced_column).copied())
                .unwrap_or(1);
            let consrc = format!(
                "FOREIGN KEY ({}) REFERENCES {}({}) ON UPDATE {} ON DELETE {}",
                fk.column, fk.referenced_table, fk.referenced_column, fk.on_update, fk.on_delete
            );
            rows.push(vec![
                CatalogValue::Int(con_oid),
                CatalogValue::Text(fk.constraint_name.clone()),
                CatalogValue::Int(2200),
                CatalogValue::Text("f".into()),
                CatalogValue::Bool(false),
                CatalogValue::Bool(false),
                CatalogValue::Bool(true),
                CatalogValue::Int(item.oid),
                CatalogValue::Int(0),
                CatalogValue::Int(0),
                CatalogValue::Int(0),
                CatalogValue::Int(ref_oid),
                CatalogValue::Text(pg_fk_action_code(&fk.on_update).into()),
                CatalogValue::Text(pg_fk_action_code(&fk.on_delete).into()),
                CatalogValue::Text("s".into()),
                CatalogValue::Bool(true),
                CatalogValue::Int(0),
                CatalogValue::Bool(true),
                CatalogValue::IntArray(vec![local_att]),
                CatalogValue::IntArray(vec![ref_att]),
                CatalogValue::Null,
                CatalogValue::Text(consrc),
            ]);
        }
    }

    CatalogTable {
        schema: "pg_catalog".into(),
        name: "pg_constraint".into(),
        columns: vec![
            CatalogColumn::int8("oid"),
            CatalogColumn::text("conname"),
            CatalogColumn::int8("connamespace"),
            CatalogColumn::text("contype"),
            CatalogColumn::bool_col("condeferrable"),
            CatalogColumn::bool_col("condeferred"),
            CatalogColumn::bool_col("convalidated"),
            CatalogColumn::int8("conrelid"),
            CatalogColumn::int8("contypid"),
            CatalogColumn::int8("conindid"),
            CatalogColumn::int8("conparentid"),
            CatalogColumn::int8("confrelid"),
            CatalogColumn::text("confupdtype"),
            CatalogColumn::text("confdeltype"),
            CatalogColumn::text("confmatchtype"),
            CatalogColumn::bool_col("conislocal"),
            CatalogColumn::int4("coninhcount"),
            CatalogColumn::bool_col("connoinherit"),
            CatalogColumn::new("conkey", 1016),
            CatalogColumn::new("confkey", 1016),
            CatalogColumn::text("conbin"),
            CatalogColumn::text("consrc"),
        ],
        rows,
    }
}

fn build_pg_indexes(items: &[MaterializedCollection]) -> CatalogTable {
    let mut rows = Vec::new();
    for item in items {
        for (idx, _) in &item.effective_indexes {
            let unique_kw = if idx.unique { "UNIQUE " } else { "" };
            let cols = idx.fields.join(", ");
            let def = format!(
                "CREATE {unique_kw}INDEX {} ON public.{} USING {} ({cols})",
                idx.name, item.meta.name, idx.method
            );
            rows.push(vec![
                CatalogValue::Text("public".into()),
                CatalogValue::Text(item.meta.name.clone()),
                CatalogValue::Text(idx.name.clone()),
                CatalogValue::Null,
                CatalogValue::Text(def),
            ]);
        }
    }
    CatalogTable {
        schema: "pg_catalog".into(),
        name: "pg_indexes".into(),
        columns: vec![
            CatalogColumn::text("schemaname"),
            CatalogColumn::text("tablename"),
            CatalogColumn::text("indexname"),
            CatalogColumn::text("tablespace"),
            CatalogColumn::text("indexdef"),
        ],
        rows,
    }
}

fn build_pg_tables(items: &[MaterializedCollection]) -> CatalogTable {
    let rows = items
        .iter()
        .map(|item| {
            vec![
                CatalogValue::Text("public".into()),
                CatalogValue::Text(item.meta.name.clone()),
                CatalogValue::Text("postgres".into()),
                CatalogValue::Null,
                CatalogValue::Bool(!item.effective_indexes.is_empty()),
                CatalogValue::Bool(false),
                CatalogValue::Bool(false),
                CatalogValue::Bool(false),
            ]
        })
        .collect();

    CatalogTable {
        schema: "pg_catalog".into(),
        name: "pg_tables".into(),
        columns: vec![
            CatalogColumn::text("schemaname"),
            CatalogColumn::text("tablename"),
            CatalogColumn::text("tableowner"),
            CatalogColumn::text("tablespace"),
            CatalogColumn::bool_col("hasindexes"),
            CatalogColumn::bool_col("hasrules"),
            CatalogColumn::bool_col("hastriggers"),
            CatalogColumn::bool_col("rowsecurity"),
        ],
        rows,
    }
}

fn build_pg_stat_tables(name: &str, items: &[MaterializedCollection]) -> CatalogTable {
    let rows = items
        .iter()
        .map(|item| {
            vec![
                CatalogValue::Int(item.oid),
                CatalogValue::Text("public".into()),
                CatalogValue::Text(item.meta.name.clone()),
                CatalogValue::Int(item.meta.row_count as i64),
                CatalogValue::Int(0),
                CatalogValue::Int(0),
                CatalogValue::Int(0),
            ]
        })
        .collect();
    CatalogTable {
        schema: "pg_catalog".into(),
        name: name.into(),
        columns: vec![
            CatalogColumn::int8("relid"),
            CatalogColumn::text("schemaname"),
            CatalogColumn::text("relname"),
            CatalogColumn::int8("n_live_tup"),
            CatalogColumn::int8("n_dead_tup"),
            CatalogColumn::int8("seq_scan"),
            CatalogColumn::int8("idx_scan"),
        ],
        rows,
    }
}

fn build_pg_description(items: &[MaterializedCollection]) -> CatalogTable {
    let mut rows = Vec::new();
    for item in items {
        if let Some(ref comment) = item.meta.comment {
            rows.push(vec![
                CatalogValue::Int(item.oid),
                CatalogValue::Int(1259),
                CatalogValue::Int(0),
                CatalogValue::Text(comment.clone()),
            ]);
        }
    }
    CatalogTable {
        schema: "pg_catalog".into(),
        name: "pg_description".into(),
        columns: vec![
            CatalogColumn::int8("objoid"),
            CatalogColumn::int8("classoid"),
            CatalogColumn::int4("objsubid"),
            CatalogColumn::text("description"),
        ],
        rows,
    }
}

fn build_pg_settings(snapshot: &CatalogSnapshot) -> CatalogTable {
    let entries = [
        ("server_version", "16.0", "", "Version"),
        ("server_version_num", "160000", "", "Version"),
        ("client_encoding", "UTF8", "", "Client Connection Defaults"),
        ("server_encoding", "UTF8", "", "Client Connection Defaults"),
        (
            "standard_conforming_strings",
            "on",
            "",
            "Client Connection Defaults",
        ),
        (
            "max_connections",
            "10000",
            "",
            "Connections and Authentication",
        ),
        ("max_identifier_length", "63", "", "Preset Options"),
        ("shared_buffers", "128MB", "MB", "Resource Usage"),
        (
            "tapisaurus_version",
            snapshot.server_version.as_str(),
            "",
            "TapisaurusDB",
        ),
    ];
    let rows = entries
        .into_iter()
        .map(|(n, s, u, c)| {
            vec![
                CatalogValue::Text(n.into()),
                CatalogValue::Text(s.into()),
                CatalogValue::Text(u.into()),
                CatalogValue::Text(c.into()),
            ]
        })
        .collect();
    CatalogTable {
        schema: "pg_catalog".into(),
        name: "pg_settings".into(),
        columns: vec![
            CatalogColumn::text("name"),
            CatalogColumn::text("setting"),
            CatalogColumn::text("unit"),
            CatalogColumn::text("category"),
        ],
        rows,
    }
}

macro_rules! empty_catalog_table {
    ($fn_name:ident, $schema:expr, $name:expr, [$(($col:expr, $oid:expr)),* $(,)?]) => {
        fn $fn_name() -> CatalogTable {
            CatalogTable {
                schema: $schema.into(),
                name: $name.into(),
                columns: vec![$(CatalogColumn::new($col, $oid)),*],
                rows: Vec::new(),
            }
        }
    };
}

empty_catalog_table!(
    build_empty_pg_enum,
    "pg_catalog",
    "pg_enum",
    [
        ("oid", 20),
        ("enumtypid", 20),
        ("enumsortorder", 701),
        ("enumlabel", 25),
    ]
);
empty_catalog_table!(
    build_empty_pg_range,
    "pg_catalog",
    "pg_range",
    [("rngtypid", 20), ("rngsubtype", 20)]
);
empty_catalog_table!(
    build_empty_pg_inherits,
    "pg_catalog",
    "pg_inherits",
    [("inhrelid", 20), ("inhparent", 20), ("inhseqno", 23)]
);
empty_catalog_table!(
    build_empty_pg_opclass,
    "pg_catalog",
    "pg_opclass",
    [
        ("oid", 20),
        ("opcmethod", 20),
        ("opcname", 25),
        ("opcnamespace", 20),
        ("opcowner", 20),
        ("opcfamily", 20),
        ("opcintype", 20),
        ("opcdefault", 16),
        ("opckeytype", 20),
    ]
);
empty_catalog_table!(
    build_empty_pg_language,
    "pg_catalog",
    "pg_language",
    [("oid", 20), ("lanname", 25)]
);
empty_catalog_table!(
    build_empty_pg_proc,
    "pg_catalog",
    "pg_proc",
    [
        ("oid", 20),
        ("proname", 25),
        ("pronamespace", 20),
        ("prolang", 20),
        ("prokind", 25),
        ("prorettype", 20),
        ("prosrc", 25),
    ]
);
empty_catalog_table!(
    build_empty_pg_depend,
    "pg_catalog",
    "pg_depend",
    [
        ("classid", 20),
        ("objid", 20),
        ("objsubid", 23),
        ("refclassid", 20),
        ("refobjid", 20),
        ("refobjsubid", 23),
        ("deptype", 25),
    ]
);
empty_catalog_table!(
    build_empty_pg_views,
    "pg_catalog",
    "pg_views",
    [
        ("schemaname", 25),
        ("viewname", 25),
        ("viewowner", 25),
        ("definition", 25),
    ]
);
empty_catalog_table!(
    build_empty_pg_sequences,
    "pg_catalog",
    "pg_sequences",
    [
        ("schemaname", 25),
        ("sequencename", 25),
        ("sequenceowner", 25),
        ("data_type", 25),
        ("start_value", 20),
        ("min_value", 20),
        ("max_value", 20),
        ("increment_by", 20),
        ("cycle", 16),
        ("cache_size", 20),
    ]
);
empty_catalog_table!(
    build_empty_pg_matviews,
    "pg_catalog",
    "pg_matviews",
    [
        ("schemaname", 25),
        ("matviewname", 25),
        ("matviewowner", 25),
        ("tablespace", 25),
        ("hasindexes", 16),
        ("ispopulated", 16),
        ("definition", 25),
    ]
);

fn build_pg_collation() -> CatalogTable {
    CatalogTable {
        schema: "pg_catalog".into(),
        name: "pg_collation".into(),
        columns: vec![
            CatalogColumn::int8("oid"),
            CatalogColumn::text("collname"),
            CatalogColumn::int8("collnamespace"),
            CatalogColumn::int8("collowner"),
            CatalogColumn::text("collprovider"),
            CatalogColumn::bool_col("collisdeterministic"),
            CatalogColumn::int4("collencoding"),
            CatalogColumn::text("collcollate"),
            CatalogColumn::text("collctype"),
        ],
        rows: vec![vec![
            CatalogValue::Int(100),
            CatalogValue::Text("default".into()),
            CatalogValue::Int(11),
            CatalogValue::Int(10),
            CatalogValue::Text("d".into()),
            CatalogValue::Bool(true),
            CatalogValue::Int(-1),
            CatalogValue::Text("en_US.UTF-8".into()),
            CatalogValue::Text("en_US.UTF-8".into()),
        ]],
    }
}

fn build_pg_tablespace() -> CatalogTable {
    CatalogTable {
        schema: "pg_catalog".into(),
        name: "pg_tablespace".into(),
        columns: vec![
            CatalogColumn::int8("oid"),
            CatalogColumn::text("spcname"),
            CatalogColumn::int8("spcowner"),
            CatalogColumn::text("spcacl"),
            CatalogColumn::text("spcoptions"),
        ],
        rows: vec![vec![
            CatalogValue::Int(1663),
            CatalogValue::Text("pg_default".into()),
            CatalogValue::Int(10),
            CatalogValue::Null,
            CatalogValue::Null,
        ]],
    }
}

fn build_pg_roles() -> CatalogTable {
    CatalogTable {
        schema: "pg_catalog".into(),
        name: "pg_roles".into(),
        columns: vec![
            CatalogColumn::int8("oid"),
            CatalogColumn::text("rolname"),
            CatalogColumn::bool_col("rolsuper"),
            CatalogColumn::bool_col("rolcanlogin"),
        ],
        rows: vec![vec![
            CatalogValue::Int(10),
            CatalogValue::Text("postgres".into()),
            CatalogValue::Bool(true),
            CatalogValue::Bool(true),
        ]],
    }
}

fn build_pg_extension() -> CatalogTable {
    CatalogTable {
        schema: "pg_catalog".into(),
        name: "pg_extension".into(),
        columns: vec![
            CatalogColumn::int8("oid"),
            CatalogColumn::text("extname"),
            CatalogColumn::int8("extnamespace"),
            CatalogColumn::text("extversion"),
            CatalogColumn::bool_col("extrelocatable"),
        ],
        rows: vec![vec![
            CatalogValue::Int(16388),
            CatalogValue::Text("vector".into()),
            CatalogValue::Int(2200),
            CatalogValue::Text("0.7.0".into()),
            CatalogValue::Bool(true),
        ]],
    }
}

fn build_info_schemata(
    catalog: &str,
    default_schema: &str,
    dialect: &CatalogDialect,
) -> CatalogTable {
    let schemas: Vec<&str> = match dialect {
        CatalogDialect::Postgres => vec!["public", "pg_catalog", "information_schema"],
        CatalogDialect::MySql { .. } => {
            vec![default_schema, "information_schema", "performance_schema"]
        }
    };
    let rows = schemas
        .into_iter()
        .map(|s| {
            vec![
                CatalogValue::Text(catalog.into()),
                CatalogValue::Text(s.into()),
                CatalogValue::Text("postgres".into()),
                CatalogValue::Null,
                CatalogValue::Null,
                CatalogValue::Text("UTF8".into()),
                CatalogValue::Null,
            ]
        })
        .collect();
    CatalogTable {
        schema: "information_schema".into(),
        name: "schemata".into(),
        columns: vec![
            CatalogColumn::text("catalog_name"),
            CatalogColumn::text("schema_name"),
            CatalogColumn::text("schema_owner"),
            CatalogColumn::text("default_character_set_catalog"),
            CatalogColumn::text("default_character_set_schema"),
            CatalogColumn::text("default_character_set_name"),
            CatalogColumn::text("sql_path"),
        ],
        rows,
    }
}

fn build_info_tables(
    catalog: &str,
    default_schema: &str,
    items: &[MaterializedCollection],
) -> CatalogTable {
    let rows = items
        .iter()
        .map(|item| {
            let count = item.meta.row_count as i64;
            vec![
                CatalogValue::Text(catalog.into()),
                CatalogValue::Text(default_schema.into()),
                CatalogValue::Text(item.meta.name.clone()),
                CatalogValue::Text("BASE TABLE".into()),
                CatalogValue::Text("TapisaurusDB".into()),
                CatalogValue::Int(count),
                CatalogValue::Int(128),
                CatalogValue::Int(count * 128),
                CatalogValue::Int(4096),
                CatalogValue::Null,
                CatalogValue::Null,
                CatalogValue::Text("utf8mb4_general_ci".into()),
                CatalogValue::Text(String::new()),
                item.meta
                    .comment
                    .clone()
                    .map_or(CatalogValue::Null, CatalogValue::Text),
            ]
        })
        .collect();
    CatalogTable {
        schema: "information_schema".into(),
        name: "tables".into(),
        columns: vec![
            CatalogColumn::text("table_catalog"),
            CatalogColumn::text("table_schema"),
            CatalogColumn::text("table_name"),
            CatalogColumn::text("table_type"),
            CatalogColumn::text("engine"),
            CatalogColumn::int8("table_rows"),
            CatalogColumn::int8("avg_row_length"),
            CatalogColumn::int8("data_length"),
            CatalogColumn::int8("index_length"),
            CatalogColumn::text("create_time"),
            CatalogColumn::text("update_time"),
            CatalogColumn::text("table_collation"),
            CatalogColumn::text("create_options"),
            CatalogColumn::text("table_comment"),
        ],
        rows,
    }
}

fn build_info_columns(
    catalog: &str,
    default_schema: &str,
    items: &[MaterializedCollection],
) -> CatalogTable {
    let mut rows = Vec::new();
    for item in items {
        let indexed_fields: HashMap<&str, bool> = item
            .effective_indexes
            .iter()
            .filter_map(|(idx, _)| idx.fields.first().map(|f| (f.as_str(), idx.unique)))
            .collect();
        for (pos, col) in item.meta.columns.iter().enumerate() {
            let col_key = if col.is_primary_key {
                "PRI"
            } else if col.is_unique || indexed_fields.get(col.name.as_str()).copied() == Some(true)
            {
                "UNI"
            } else if indexed_fields.contains_key(col.name.as_str()) {
                "MUL"
            } else {
                ""
            };
            rows.push(vec![
                CatalogValue::Text(catalog.into()),
                CatalogValue::Text(default_schema.into()),
                CatalogValue::Text(item.meta.name.clone()),
                CatalogValue::Text(col.name.clone()),
                CatalogValue::Int((pos + 1) as i64),
                col.default_expr
                    .clone()
                    .map_or(CatalogValue::Null, CatalogValue::Text),
                CatalogValue::Text(if col.nullable { "YES" } else { "NO" }.into()),
                CatalogValue::Text(col.data_type.clone()),
                CatalogValue::Text(col.udt_name.clone()),
                CatalogValue::Text("pg_catalog".into()),
                col.max_length
                    .map_or(CatalogValue::Null, |v| CatalogValue::Int(i64::from(v))),
                col.numeric_precision
                    .map_or(CatalogValue::Null, |v| CatalogValue::Int(i64::from(v))),
                col.numeric_scale
                    .map_or(CatalogValue::Null, |v| CatalogValue::Int(i64::from(v))),
                CatalogValue::Text("NO".into()),
                CatalogValue::Text("NEVER".into()),
                CatalogValue::Text(col.mysql_type.clone()),
                CatalogValue::Text(col_key.into()),
                CatalogValue::Text("".into()),
                CatalogValue::Text("".into()),
                CatalogValue::Text("select,insert,update,references".into()),
            ]);
        }
    }

    CatalogTable {
        schema: "information_schema".into(),
        name: "columns".into(),
        columns: vec![
            CatalogColumn::text("table_catalog"),
            CatalogColumn::text("table_schema"),
            CatalogColumn::text("table_name"),
            CatalogColumn::text("column_name"),
            CatalogColumn::int4("ordinal_position"),
            CatalogColumn::text("column_default"),
            CatalogColumn::text("is_nullable"),
            CatalogColumn::text("data_type"),
            CatalogColumn::text("udt_name"),
            CatalogColumn::text("udt_schema"),
            CatalogColumn::int4("character_maximum_length"),
            CatalogColumn::int4("numeric_precision"),
            CatalogColumn::int4("numeric_scale"),
            CatalogColumn::text("is_identity"),
            CatalogColumn::text("is_generated"),
            CatalogColumn::text("column_type"),
            CatalogColumn::text("column_key"),
            CatalogColumn::text("extra"),
            CatalogColumn::text("column_comment"),
            CatalogColumn::text("privileges"),
        ],
        rows,
    }
}

fn build_info_table_constraints(
    catalog: &str,
    default_schema: &str,
    items: &[MaterializedCollection],
) -> CatalogTable {
    let mut rows = Vec::new();
    for item in items {
        for (idx, _) in &item.effective_indexes {
            if !idx.is_primary && !idx.unique {
                continue;
            }
            let ctype = if idx.is_primary {
                "PRIMARY KEY"
            } else {
                "UNIQUE"
            };
            rows.push(vec![
                CatalogValue::Text(catalog.into()),
                CatalogValue::Text(default_schema.into()),
                CatalogValue::Text(idx.name.clone()),
                CatalogValue::Text(catalog.into()),
                CatalogValue::Text(default_schema.into()),
                CatalogValue::Text(item.meta.name.clone()),
                CatalogValue::Text(ctype.into()),
                CatalogValue::Text("NO".into()),
                CatalogValue::Text("NO".into()),
                CatalogValue::Text("YES".into()),
            ]);
        }
        for fk in &item.meta.foreign_keys {
            rows.push(vec![
                CatalogValue::Text(catalog.into()),
                CatalogValue::Text(default_schema.into()),
                CatalogValue::Text(fk.constraint_name.clone()),
                CatalogValue::Text(catalog.into()),
                CatalogValue::Text(default_schema.into()),
                CatalogValue::Text(item.meta.name.clone()),
                CatalogValue::Text("FOREIGN KEY".into()),
                CatalogValue::Text("NO".into()),
                CatalogValue::Text("NO".into()),
                CatalogValue::Text("YES".into()),
            ]);
        }
    }
    CatalogTable {
        schema: "information_schema".into(),
        name: "table_constraints".into(),
        columns: vec![
            CatalogColumn::text("constraint_catalog"),
            CatalogColumn::text("constraint_schema"),
            CatalogColumn::text("constraint_name"),
            CatalogColumn::text("table_catalog"),
            CatalogColumn::text("table_schema"),
            CatalogColumn::text("table_name"),
            CatalogColumn::text("constraint_type"),
            CatalogColumn::text("is_deferrable"),
            CatalogColumn::text("initially_deferred"),
            CatalogColumn::text("enforced"),
        ],
        rows,
    }
}

fn build_info_key_column_usage(
    catalog: &str,
    default_schema: &str,
    items: &[MaterializedCollection],
) -> CatalogTable {
    let mut rows = Vec::new();
    for item in items {
        for (idx, _) in &item.effective_indexes {
            if !idx.is_primary && !idx.unique {
                continue;
            }
            for (pos, field) in idx.fields.iter().enumerate() {
                rows.push(vec![
                    CatalogValue::Text(catalog.into()),
                    CatalogValue::Text(default_schema.into()),
                    CatalogValue::Text(idx.name.clone()),
                    CatalogValue::Text(catalog.into()),
                    CatalogValue::Text(default_schema.into()),
                    CatalogValue::Text(item.meta.name.clone()),
                    CatalogValue::Text(field.clone()),
                    CatalogValue::Int((pos + 1) as i64),
                    CatalogValue::Null,
                    CatalogValue::Null,
                    CatalogValue::Null,
                    CatalogValue::Null,
                ]);
            }
        }
        for fk in &item.meta.foreign_keys {
            rows.push(vec![
                CatalogValue::Text(catalog.into()),
                CatalogValue::Text(default_schema.into()),
                CatalogValue::Text(fk.constraint_name.clone()),
                CatalogValue::Text(catalog.into()),
                CatalogValue::Text(default_schema.into()),
                CatalogValue::Text(item.meta.name.clone()),
                CatalogValue::Text(fk.column.clone()),
                CatalogValue::Int(1),
                CatalogValue::Int(1),
                CatalogValue::Text(default_schema.into()),
                CatalogValue::Text(fk.referenced_table.clone()),
                CatalogValue::Text(fk.referenced_column.clone()),
            ]);
        }
    }
    CatalogTable {
        schema: "information_schema".into(),
        name: "key_column_usage".into(),
        columns: vec![
            CatalogColumn::text("constraint_catalog"),
            CatalogColumn::text("constraint_schema"),
            CatalogColumn::text("constraint_name"),
            CatalogColumn::text("table_catalog"),
            CatalogColumn::text("table_schema"),
            CatalogColumn::text("table_name"),
            CatalogColumn::text("column_name"),
            CatalogColumn::int4("ordinal_position"),
            CatalogColumn::int4("position_in_unique_constraint"),
            CatalogColumn::text("referenced_table_schema"),
            CatalogColumn::text("referenced_table_name"),
            CatalogColumn::text("referenced_column_name"),
        ],
        rows,
    }
}

fn build_info_referential_constraints(
    catalog: &str,
    default_schema: &str,
    items: &[MaterializedCollection],
) -> CatalogTable {
    let mut rows = Vec::new();
    for item in items {
        for fk in &item.meta.foreign_keys {
            rows.push(vec![
                CatalogValue::Text(catalog.into()),
                CatalogValue::Text(default_schema.into()),
                CatalogValue::Text(fk.constraint_name.clone()),
                CatalogValue::Text(catalog.into()),
                CatalogValue::Text(default_schema.into()),
                CatalogValue::Text(format!("{}_pkey", fk.referenced_table)),
                CatalogValue::Text("NONE".into()),
                CatalogValue::Text(fk.on_update.clone()),
                CatalogValue::Text(fk.on_delete.clone()),
                CatalogValue::Text(item.meta.name.clone()),
                CatalogValue::Text(fk.referenced_table.clone()),
            ]);
        }
    }
    CatalogTable {
        schema: "information_schema".into(),
        name: "referential_constraints".into(),
        columns: vec![
            CatalogColumn::text("constraint_catalog"),
            CatalogColumn::text("constraint_schema"),
            CatalogColumn::text("constraint_name"),
            CatalogColumn::text("unique_constraint_catalog"),
            CatalogColumn::text("unique_constraint_schema"),
            CatalogColumn::text("unique_constraint_name"),
            CatalogColumn::text("match_option"),
            CatalogColumn::text("update_rule"),
            CatalogColumn::text("delete_rule"),
            CatalogColumn::text("table_name"),
            CatalogColumn::text("referenced_table_name"),
        ],
        rows,
    }
}

fn build_info_constraint_column_usage(
    catalog: &str,
    default_schema: &str,
    items: &[MaterializedCollection],
) -> CatalogTable {
    let mut rows = Vec::new();
    for item in items {
        for (idx, _) in &item.effective_indexes {
            if !idx.is_primary && !idx.unique {
                continue;
            }
            for field in &idx.fields {
                rows.push(vec![
                    CatalogValue::Text(catalog.into()),
                    CatalogValue::Text(default_schema.into()),
                    CatalogValue::Text(item.meta.name.clone()),
                    CatalogValue::Text(field.clone()),
                    CatalogValue::Text(catalog.into()),
                    CatalogValue::Text(default_schema.into()),
                    CatalogValue::Text(idx.name.clone()),
                ]);
            }
        }
        for fk in &item.meta.foreign_keys {
            rows.push(vec![
                CatalogValue::Text(catalog.into()),
                CatalogValue::Text(default_schema.into()),
                CatalogValue::Text(fk.referenced_table.clone()),
                CatalogValue::Text(fk.referenced_column.clone()),
                CatalogValue::Text(catalog.into()),
                CatalogValue::Text(default_schema.into()),
                CatalogValue::Text(fk.constraint_name.clone()),
            ]);
        }
    }
    CatalogTable {
        schema: "information_schema".into(),
        name: "constraint_column_usage".into(),
        columns: vec![
            CatalogColumn::text("table_catalog"),
            CatalogColumn::text("table_schema"),
            CatalogColumn::text("table_name"),
            CatalogColumn::text("column_name"),
            CatalogColumn::text("constraint_catalog"),
            CatalogColumn::text("constraint_schema"),
            CatalogColumn::text("constraint_name"),
        ],
        rows,
    }
}

fn build_info_statistics(
    catalog: &str,
    default_schema: &str,
    items: &[MaterializedCollection],
) -> CatalogTable {
    let mut rows = Vec::new();
    for item in items {
        let cardinality = item.meta.row_count as i64;
        for (idx, _) in &item.effective_indexes {
            let idx_name = if idx.is_primary {
                "PRIMARY".to_string()
            } else {
                idx.name.clone()
            };
            for (seq, field) in idx.fields.iter().enumerate() {
                rows.push(vec![
                    CatalogValue::Text(catalog.into()),
                    CatalogValue::Text(default_schema.into()),
                    CatalogValue::Text(item.meta.name.clone()),
                    CatalogValue::Int(i64::from(!idx.unique)),
                    CatalogValue::Text(default_schema.into()),
                    CatalogValue::Text(idx_name.clone()),
                    CatalogValue::Int((seq + 1) as i64),
                    CatalogValue::Text(field.clone()),
                    CatalogValue::Text("A".into()),
                    CatalogValue::Int(cardinality),
                    CatalogValue::Null,
                    CatalogValue::Null,
                    CatalogValue::Text("".into()),
                    CatalogValue::Text(idx.method.to_ascii_uppercase()),
                    CatalogValue::Text("".into()),
                    CatalogValue::Text("".into()),
                    CatalogValue::Text("YES".into()),
                ]);
            }
        }
    }
    CatalogTable {
        schema: "information_schema".into(),
        name: "statistics".into(),
        columns: vec![
            CatalogColumn::text("table_catalog"),
            CatalogColumn::text("table_schema"),
            CatalogColumn::text("table_name"),
            CatalogColumn::int4("non_unique"),
            CatalogColumn::text("index_schema"),
            CatalogColumn::text("index_name"),
            CatalogColumn::int4("seq_in_index"),
            CatalogColumn::text("column_name"),
            CatalogColumn::text("collation"),
            CatalogColumn::int8("cardinality"),
            CatalogColumn::int4("sub_part"),
            CatalogColumn::text("packed"),
            CatalogColumn::text("nullable"),
            CatalogColumn::text("index_type"),
            CatalogColumn::text("comment"),
            CatalogColumn::text("index_comment"),
            CatalogColumn::text("is_visible"),
        ],
        rows,
    }
}

empty_catalog_table!(
    build_empty_info_views,
    "information_schema",
    "views",
    [
        ("table_catalog", 25),
        ("table_schema", 25),
        ("table_name", 25),
        ("view_definition", 25),
    ]
);
empty_catalog_table!(
    build_empty_info_triggers,
    "information_schema",
    "triggers",
    [
        ("trigger_catalog", 25),
        ("trigger_schema", 25),
        ("trigger_name", 25),
        ("event_object_table", 25),
    ]
);
empty_catalog_table!(
    build_empty_info_routines,
    "information_schema",
    "routines",
    [
        ("routine_catalog", 25),
        ("routine_schema", 25),
        ("routine_name", 25),
        ("routine_type", 25),
    ]
);
empty_catalog_table!(
    build_empty_info_sequences,
    "information_schema",
    "sequences",
    [
        ("sequence_catalog", 25),
        ("sequence_schema", 25),
        ("sequence_name", 25),
        ("data_type", 25),
    ]
);
