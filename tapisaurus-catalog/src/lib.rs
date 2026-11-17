//! # `tapisaurus-catalog` — Dynamic Virtual System Catalog & ORM Reflection Engine
//!
//! Materializes live `pg_catalog.*` and `information_schema.*` relations directly from
//! TapisaurusDB collections, relational DDL schemas, JSON Schema contracts, foreign keys,
//! secondary B-Tree indexes, and vector HNSW/DiskANN indexes, and evaluates relational
//! introspection `SELECT ... JOIN ... WHERE ...` queries for Prisma, SQLAlchemy, TypeORM,
//! Drizzle, and Laravel Eloquent.

#![forbid(unsafe_code)]

pub mod eval;
pub mod materializer;
pub mod parser;
pub mod types;

pub use eval::{try_describe_catalog_query, try_execute_catalog_query};
pub use materializer::{
    format_pg_type, materialize_catalog, CatalogSnapshot, CollectionCatalogMeta, IndexCatalogMeta,
};
pub use types::{
    CatalogColumn, CatalogDialect, CatalogResult, CatalogTable, CatalogValue, VirtualCatalog,
};

#[cfg(test)]
mod tests {
    use super::*;
    use tapisaurus_core::{ColumnSchema, ForeignKeyDef};

    fn sample_snapshot() -> CatalogSnapshot {
        CatalogSnapshot {
            database_name: "tapisaurus".into(),
            server_version: "0.1.0".into(),
            collections: vec![
                CollectionCatalogMeta {
                    name: "users".into(),
                    row_count: 42,
                    columns: vec![
                        ColumnSchema::from_sql_type("id", "bigint", false, None, true, true),
                        ColumnSchema::from_sql_type(
                            "email",
                            "varchar(255)",
                            false,
                            None,
                            false,
                            true,
                        ),
                        ColumnSchema::from_sql_type(
                            "is_active",
                            "boolean",
                            false,
                            Some("true".into()),
                            false,
                            false,
                        ),
                    ],
                    primary_key: vec!["id".into()],
                    foreign_keys: Vec::new(),
                    indexes: vec![IndexCatalogMeta {
                        name: "idx_users_email".into(),
                        fields: vec!["email".into()],
                        unique: true,
                        method: "btree".into(),
                        is_primary: false,
                    }],
                    comment: Some("Registered platform accounts".into()),
                },
                CollectionCatalogMeta {
                    name: "orders".into(),
                    row_count: 120,
                    columns: vec![
                        ColumnSchema::from_sql_type("id", "bigint", false, None, true, true),
                        ColumnSchema::from_sql_type("user_id", "bigint", false, None, false, false),
                        ColumnSchema::from_sql_type(
                            "total",
                            "numeric(12,2)",
                            false,
                            Some("0.00".into()),
                            false,
                            false,
                        ),
                        ColumnSchema::from_sql_type(
                            "embedding",
                            "vector(1536)",
                            true,
                            None,
                            false,
                            false,
                        ),
                    ],
                    primary_key: vec!["id".into()],
                    foreign_keys: vec![ForeignKeyDef {
                        constraint_name: "orders_user_id_fkey".into(),
                        column: "user_id".into(),
                        referenced_table: "users".into(),
                        referenced_column: "id".into(),
                        on_delete: "CASCADE".into(),
                        on_update: "NO ACTION".into(),
                    }],
                    indexes: vec![IndexCatalogMeta {
                        name: "idx_orders_embedding_hnsw".into(),
                        fields: vec!["embedding".into()],
                        unique: false,
                        method: "hnsw".into(),
                        is_primary: false,
                    }],
                    comment: None,
                },
            ],
        }
    }

    #[test]
    fn test_sqlalchemy_and_prisma_pg_catalog_joins() {
        let cat = materialize_catalog(&sample_snapshot(), CatalogDialect::Postgres);

        // 1. Table discovery query (SQLAlchemy / Prisma style)
        let q_tables = "
            SELECT c.relname, c.oid, n.nspname, c.reltuples
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND c.relkind = ANY(ARRAY['r', 'p'])
            ORDER BY c.relname ASC
        ";
        let res = try_execute_catalog_query(&cat, q_tables).unwrap().unwrap();
        assert_eq!(res.rows.len(), 2);
        assert_eq!(
            res.rows[0][0].as_ref().and_then(|v| v.as_str()),
            Some("orders")
        );
        assert_eq!(
            res.rows[1][0].as_ref().and_then(|v| v.as_str()),
            Some("users")
        );

        // 2. Column + format_type + LEFT JOIN pg_attrdef + scalar subquery
        let q_cols = "
            SELECT a.attname,
                   pg_catalog.format_type(a.atttypid, a.atttypmod) AS formatted_type,
                   a.attnotnull,
                   (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid)
                    FROM pg_catalog.pg_attrdef d
                    WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef) AS default_val,
                   t.typname
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
            WHERE c.relname = 'orders' AND n.nspname = 'public'
              AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
        ";
        let cols_res = try_execute_catalog_query(&cat, q_cols).unwrap().unwrap();
        assert_eq!(cols_res.rows.len(), 4);
        assert_eq!(
            cols_res.rows[2][0].as_ref().and_then(|v| v.as_str()),
            Some("total")
        );
        assert_eq!(
            cols_res.rows[2][1].as_ref().and_then(|v| v.as_str()),
            Some("numeric(12,2)")
        );
        assert_eq!(
            cols_res.rows[2][3].as_ref().and_then(|v| v.as_str()),
            Some("0.00")
        );
        assert_eq!(
            cols_res.rows[3][1].as_ref().and_then(|v| v.as_str()),
            Some("vector(1536)")
        );

        // 3. Foreign Key + Index Introspection (pg_constraint & pg_index = ANY(indkey))
        let q_fk = "
            SELECT con.conname, con.contype, ref_c.relname AS referenced_table, a.attname AS column_name
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
            JOIN pg_catalog.pg_class ref_c ON ref_c.oid = con.confrelid
            JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(con.conkey)
            WHERE c.relname = 'orders' AND con.contype = 'f'
        ";
        let fk_res = try_execute_catalog_query(&cat, q_fk).unwrap().unwrap();
        assert_eq!(fk_res.rows.len(), 1);
        assert_eq!(
            fk_res.rows[0][0].as_ref().and_then(|v| v.as_str()),
            Some("orders_user_id_fkey")
        );
        assert_eq!(
            fk_res.rows[0][2].as_ref().and_then(|v| v.as_str()),
            Some("users")
        );
        assert_eq!(
            fk_res.rows[0][3].as_ref().and_then(|v| v.as_str()),
            Some("user_id")
        );

        // 4. Index + AM join (btree & hnsw)
        let q_idx = "
            SELECT i.relname AS index_name, am.amname, ix.indisunique, a.attname
            FROM pg_catalog.pg_index ix
            JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid
            JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
            JOIN pg_catalog.pg_am am ON am.oid = i.relam
            JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            WHERE t.relname = 'orders' AND NOT ix.indisprimary
        ";
        let idx_res = try_execute_catalog_query(&cat, q_idx).unwrap().unwrap();
        assert_eq!(idx_res.rows.len(), 1);
        assert_eq!(
            idx_res.rows[0][0].as_ref().and_then(|v| v.as_str()),
            Some("idx_orders_embedding_hnsw")
        );
        assert_eq!(
            idx_res.rows[0][1].as_ref().and_then(|v| v.as_str()),
            Some("hnsw")
        );
    }

    #[test]
    fn test_describe_and_user_query_passthrough() {
        let cat = materialize_catalog(&sample_snapshot(), CatalogDialect::Postgres);

        // Describe works even with unresolved $1/$2 bind parameters
        let desc = try_describe_catalog_query(
            &cat,
            "SELECT c.relname AS table_name, n.nspname AS schema_name FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(desc.len(), 2);
        assert_eq!(desc[0].name, "table_name");
        assert_eq!(desc[1].name, "schema_name");

        // Regular user collection query returns Ok(None) so tapisaurus-query executes it
        let user_q =
            try_execute_catalog_query(&cat, "SELECT * FROM orders WHERE user_id = 1").unwrap();
        assert!(user_q.is_none());
    }
}
