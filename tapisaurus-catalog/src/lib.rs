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

    #[test]
    fn test_sqlalchemy2_pk_and_index_srf_queries() {
        let cat = materialize_catalog(&sample_snapshot(), CatalogDialect::Postgres);
        let orders_oid = *cat.relation_oids.get("orders").unwrap();

        // Exact SQLAlchemy 2.0 PGInspector._constraint_query (unnest + generate_subscripts + array_agg ORDER BY + bool_and)
        let q_pk = format!(
            "
            SELECT attr.conrelid,
                   array_agg(CAST(attr.attname AS TEXT) ORDER BY attr.ord) AS cols,
                   attr.conname,
                   min(attr.description) AS description,
                   min(attr.indnkeyatts) AS indnkeyatts,
                   bool_and(attr.indnullsnotdistinct) AS indnullsnotdistinct
            FROM (
                SELECT con.conrelid, con.conname, con.description, con.ord,
                       con.indnkeyatts, con.indnullsnotdistinct, pg_catalog.pg_attribute.attname
                FROM pg_catalog.pg_attribute
                JOIN (
                    SELECT pg_catalog.pg_constraint.conrelid,
                           pg_catalog.pg_constraint.conname,
                           unnest(pg_catalog.pg_index.indkey) AS attnum,
                           generate_subscripts(pg_catalog.pg_index.indkey, 1) AS ord,
                           pg_catalog.pg_index.indnkeyatts,
                           pg_catalog.pg_index.indnullsnotdistinct,
                           pg_catalog.pg_description.description
                    FROM pg_catalog.pg_constraint
                    JOIN pg_catalog.pg_index
                      ON pg_catalog.pg_constraint.conindid = pg_catalog.pg_index.indexrelid
                    LEFT OUTER JOIN pg_catalog.pg_description
                      ON pg_catalog.pg_description.objoid = pg_catalog.pg_constraint.oid
                     AND pg_catalog.pg_description.classoid = CAST('pg_catalog.pg_constraint' AS REGCLASS)
                    WHERE pg_catalog.pg_constraint.contype = 'p'
                      AND pg_catalog.pg_constraint.conrelid IN ({orders_oid})
                ) AS con
                  ON pg_catalog.pg_attribute.attnum = con.attnum
                 AND pg_catalog.pg_attribute.attrelid = con.conrelid
                WHERE con.conrelid IN ({orders_oid})
            ) AS attr
            GROUP BY attr.conrelid, attr.conname
            ORDER BY attr.conrelid, attr.conname
            "
        );
        let pk_res = try_execute_catalog_query(&cat, &q_pk).unwrap().unwrap();
        assert_eq!(pk_res.rows.len(), 1);
        assert_eq!(pk_res.rows[0][0], Some(CatalogValue::Int(orders_oid)));
        assert_eq!(
            pk_res.rows[0][1],
            Some(CatalogValue::TextArray(vec!["id".into()]))
        );
        assert_eq!(
            pk_res.rows[0][2],
            Some(CatalogValue::Text("orders_pkey".into()))
        );
        assert_eq!(pk_res.rows[0][4], Some(CatalogValue::Int(1)));
        assert_eq!(pk_res.columns[4].pg_type_oid, 21); // int2 (from pg_index.indnkeyatts)
        assert_eq!(pk_res.rows[0][5], Some(CatalogValue::Bool(false)));
        assert_eq!(pk_res.columns[5].pg_type_oid, 16);

        // Exact SQLAlchemy 2.0 PGInspector._pg_opclass_notdefault_query
        let q_opc = "SELECT pg_catalog.pg_opclass.oid, pg_catalog.pg_opclass.opcname FROM pg_catalog.pg_opclass WHERE NOT pg_catalog.pg_opclass.opcdefault";
        let opc_res = try_execute_catalog_query(&cat, q_opc).unwrap().unwrap();
        assert_eq!(opc_res.rows.len(), 0);

        // Exact SQLAlchemy 2.0 PGInspector._index_query (BoolArray + IntArray + TextArray OIDs)
        let q_idx = format!(
            "
            SELECT pg_catalog.pg_index.indrelid,
                   pg_catalog.pg_class.relname,
                   pg_catalog.pg_index.indisunique,
                   pg_catalog.pg_index.indisvalid,
                   pg_catalog.pg_constraint.conrelid IS NOT NULL AS has_constraint,
                   pg_catalog.pg_index.indoption,
                   pg_catalog.pg_class.reloptions,
                   pg_catalog.pg_class.relam,
                   pg_catalog.pg_index.indnkeyatts,
                   pg_catalog.pg_index.indnullsnotdistinct,
                   idx_cols.elements,
                   idx_cols.elements_is_expr,
                   idx_cols.elements_opclass
            FROM pg_catalog.pg_index
            JOIN pg_catalog.pg_class
              ON pg_catalog.pg_index.indexrelid = pg_catalog.pg_class.oid
            LEFT OUTER JOIN (
                SELECT idx_attr.indexrelid,
                       min(idx_attr.indrelid) AS min_indrelid,
                       array_agg(idx_attr.element ORDER BY idx_attr.ord) AS elements,
                       array_agg(idx_attr.is_expr ORDER BY idx_attr.ord) AS elements_is_expr,
                       array_agg(idx_attr.att_opclass ORDER BY idx_attr.ord) AS elements_opclass
                FROM (
                    SELECT idx.indexrelid,
                           idx.indrelid,
                           idx.ord,
                           CASE WHEN idx.attnum = 0 THEN pg_catalog.pg_get_indexdef(idx.indexrelid, idx.ord + 1, true)
                                ELSE CAST(pg_catalog.pg_attribute.attname AS TEXT) END AS element,
                           (idx.attnum = 0) AS is_expr,
                           CAST(idx.att_opclass AS BIGINT) AS att_opclass
                    FROM (
                        SELECT pg_catalog.pg_index.indexrelid,
                               pg_catalog.pg_index.indrelid,
                               unnest(pg_catalog.pg_index.indkey) AS attnum,
                               unnest(pg_catalog.pg_index.indclass) AS att_opclass,
                               generate_subscripts(pg_catalog.pg_index.indkey, 1) AS ord
                        FROM pg_catalog.pg_index
                        WHERE NOT pg_catalog.pg_index.indisprimary
                          AND pg_catalog.pg_index.indrelid IN ({orders_oid})
                    ) AS idx
                    LEFT OUTER JOIN pg_catalog.pg_attribute
                      ON pg_catalog.pg_attribute.attnum = idx.attnum
                     AND pg_catalog.pg_attribute.attrelid = idx.indrelid
                    WHERE idx.indrelid IN ({orders_oid})
                ) AS idx_attr
                GROUP BY idx_attr.indexrelid
            ) AS idx_cols
              ON pg_catalog.pg_index.indexrelid = idx_cols.indexrelid
            LEFT OUTER JOIN pg_catalog.pg_constraint
              ON pg_catalog.pg_index.indrelid = pg_catalog.pg_constraint.conrelid
             AND pg_catalog.pg_index.indexrelid = pg_catalog.pg_constraint.conindid
             AND pg_catalog.pg_constraint.contype = ANY(ARRAY['p', 'u', 'x'])
            WHERE pg_catalog.pg_index.indrelid IN ({orders_oid})
              AND NOT pg_catalog.pg_index.indisprimary
            ORDER BY pg_catalog.pg_index.indrelid, pg_catalog.pg_class.relname
            "
        );
        let idx_res = try_execute_catalog_query(&cat, &q_idx).unwrap().unwrap();
        assert_eq!(idx_res.rows.len(), 1);
        assert_eq!(
            idx_res.rows[0][1],
            Some(CatalogValue::Text("idx_orders_embedding_hnsw".into()))
        );
        assert_eq!(idx_res.rows[0][5], Some(CatalogValue::Text("0".into())));
        assert_eq!(idx_res.columns[5].pg_type_oid, 22); // int2vector
        assert_eq!(
            idx_res.rows[0][10],
            Some(CatalogValue::TextArray(vec!["embedding".into()]))
        );
        assert_eq!(idx_res.columns[10].pg_type_oid, 1009);
        assert_eq!(
            idx_res.rows[0][11],
            Some(CatalogValue::BoolArray(vec![false]))
        );
        assert_eq!(idx_res.columns[11].pg_type_oid, 1000);
        assert_eq!(idx_res.rows[0][12], Some(CatalogValue::IntArray(vec![0])));
        assert_eq!(idx_res.columns[12].pg_type_oid, 1016);

        // Prisma 6 PostgreSQL get_columns query (requires reltype > 0)
        let q_prisma_pg_cols = "
            SELECT oid.namespace, info.table_name, info.column_name,
                   format_type(att.atttypid, att.atttypmod) AS formatted_type,
                   info.numeric_precision, info.numeric_scale, info.numeric_precision_radix,
                   info.datetime_precision, info.is_nullable, info.is_identity
            FROM information_schema.columns info
            JOIN pg_attribute att ON att.attname = info.column_name
            JOIN (
                SELECT pg_class.oid, relname, pg_namespace.nspname AS namespace
                FROM pg_class
                JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
                  AND pg_namespace.nspname = ANY(ARRAY['public'])
                WHERE reltype > 0
            ) AS oid ON oid.oid = att.attrelid
              AND relname = info.table_name
              AND namespace = info.table_schema
            WHERE table_schema = ANY(ARRAY['public'])
            ORDER BY namespace, table_name, ordinal_position
        ";
        let pcols = try_execute_catalog_query(&cat, q_prisma_pg_cols)
            .unwrap()
            .unwrap();
        assert_eq!(pcols.rows.len(), 7); // 3 cols in users + 4 cols in orders

        // Prisma 6 MySQL get_all_columns (IF() + BINARY)
        let my_cat = materialize_catalog(
            &sample_snapshot(),
            CatalogDialect::MySql {
                database: "tapisaurus".into(),
            },
        );
        let q_prisma_my_cols = "
            SELECT column_name, table_name, IF(column_comment = '', NULL, column_comment) AS column_comment
            FROM information_schema.columns
            WHERE BINARY table_schema = 'tapisaurus'
            ORDER BY ordinal_position
        ";
        let my_cols = try_execute_catalog_query(&my_cat, q_prisma_my_cols)
            .unwrap()
            .unwrap();
        assert_eq!(my_cols.rows.len(), 7);
        assert_eq!(my_cols.rows[0][2], Some(CatalogValue::Null));
    }

    #[test]
    fn test_prisma6_mysql_full_introspection_suite() {
        let my_cat = materialize_catalog(
            &sample_snapshot(),
            CatalogDialect::MySql {
                database: "tapisaurus".into(),
            },
        );

        // 1. Describe with MySQL '?' placeholders (COM_STMT_PREPARE)
        let q_tables_prep = "
            SELECT DISTINCT
              BINARY table_info.table_name AS table_name,
              table_info.create_options AS create_options,
              table_info.table_comment AS table_comment
            FROM information_schema.tables AS table_info
            JOIN information_schema.columns AS column_info
                ON BINARY column_info.table_name = BINARY table_info.table_name
            WHERE
                table_info.table_schema = ?
                AND column_info.table_schema = ?
                -- Exclude views.
                AND table_info.table_type = 'BASE TABLE'
            ORDER BY BINARY table_info.table_name
        ";
        let desc = try_describe_catalog_query(&my_cat, q_tables_prep)
            .unwrap()
            .unwrap();
        assert_eq!(desc.len(), 3);
        assert_eq!(desc[0].name, "table_name");
        assert_eq!(desc[1].name, "create_options");
        assert_eq!(desc[2].name, "table_comment");

        // 2. Execute get_table_names
        let q_tables = q_tables_prep.replace('?', "'tapisaurus'");
        let tbls = try_execute_catalog_query(&my_cat, &q_tables)
            .unwrap()
            .unwrap();
        assert_eq!(tbls.rows.len(), 2);
        assert_eq!(tbls.rows[0][0], Some(CatalogValue::Text("orders".into())));
        assert_eq!(tbls.rows[1][0], Some(CatalogValue::Text("users".into())));

        // 3. Execute get_views
        let q_views = "
            SELECT TABLE_NAME AS view_name, VIEW_DEFINITION AS view_sql
            FROM INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_SCHEMA = 'tapisaurus'
        ";
        let views = try_execute_catalog_query(&my_cat, q_views)
            .unwrap()
            .unwrap();
        assert_eq!(views.rows.len(), 0);

        // 4. Execute get_constraints (CHECK_CONSTRAINTS)
        let q_constraints = "
            SELECT
                tc.table_schema AS namespace,
                tc.table_name AS table_name,
                tc.constraint_name AS constraint_name,
                LOWER(tc.constraint_type) AS constraint_type,
                cc.check_clause AS constraint_definition
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            LEFT JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc
                ON cc.constraint_schema = tc.table_schema
                AND cc.constraint_name = tc.constraint_name
            WHERE constraint_type = 'CHECK'
            ORDER BY namespace, table_name, constraint_type, constraint_name
        ";
        let cons = try_execute_catalog_query(&my_cat, q_constraints)
            .unwrap()
            .unwrap();
        assert_eq!(cons.rows.len(), 0);

        // 5. Execute get_all_columns (exact Prisma 6 query + MySQL data_type normalization)
        let q_cols = "
            SELECT
                column_name column_name,
                data_type data_type,
                column_type full_data_type,
                character_maximum_length character_maximum_length,
                numeric_precision numeric_precision,
                numeric_scale numeric_scale,
                datetime_precision datetime_precision,
                column_default column_default,
                is_nullable is_nullable,
                extra extra,
                table_name table_name,
                IF(column_comment = '', NULL, column_comment) AS column_comment
            FROM information_schema.columns
            WHERE table_schema = 'tapisaurus'
            ORDER BY ordinal_position
        ";
        let cols = try_execute_catalog_query(&my_cat, q_cols).unwrap().unwrap();
        assert_eq!(cols.rows.len(), 7);
        // Verify MySQL data_type normalization (varchar, decimal, tinyint)
        let email_row = cols
            .rows
            .iter()
            .find(|r| r[0] == Some(CatalogValue::Text("email".into())))
            .unwrap();
        assert_eq!(email_row[1], Some(CatalogValue::Text("varchar".into())));
        assert_eq!(email_row[3], Some(CatalogValue::Int(255)));
        let total_row = cols
            .rows
            .iter()
            .find(|r| r[0] == Some(CatalogValue::Text("total".into())))
            .unwrap();
        assert_eq!(total_row[1], Some(CatalogValue::Text("decimal".into())));
        assert_eq!(total_row[4], Some(CatalogValue::Int(12)));
        assert_eq!(total_row[5], Some(CatalogValue::Int(2)));

        // 6. Execute push_foreign_keys
        let q_fks = "
            SELECT
                kcu.constraint_name constraint_name,
                kcu.column_name column_name,
                kcu.referenced_table_name referenced_table_name,
                kcu.referenced_column_name referenced_column_name,
                kcu.ordinal_position ordinal_position,
                kcu.table_name table_name,
                rc.delete_rule delete_rule,
                rc.update_rule update_rule
            FROM information_schema.key_column_usage AS kcu
            INNER JOIN information_schema.referential_constraints AS rc ON
                BINARY kcu.constraint_name = BINARY rc.constraint_name
            WHERE
                BINARY kcu.table_schema = 'tapisaurus'
                AND BINARY rc.constraint_schema = 'tapisaurus'
                AND kcu.referenced_column_name IS NOT NULL
            ORDER BY
                BINARY kcu.table_schema,
                BINARY kcu.table_name,
                BINARY kcu.constraint_name,
                kcu.ordinal_position
        ";
        let fks = try_execute_catalog_query(&my_cat, q_fks).unwrap().unwrap();
        assert_eq!(fks.rows.len(), 1);
        assert_eq!(fks.rows[0][1], Some(CatalogValue::Text("user_id".into())));
        assert_eq!(fks.rows[0][2], Some(CatalogValue::Text("users".into())));

        // 7. Execute push_indexes
        let q_idx = "
            SELECT
                table_name AS table_name,
                index_name AS index_name,
                column_name AS column_name,
                sub_part AS partial,
                seq_in_index AS seq_in_index,
                collation AS column_order,
                non_unique AS non_unique,
                index_type AS index_type
            FROM information_schema.statistics
            WHERE table_schema = 'tapisaurus'
            ORDER BY BINARY table_name, BINARY index_name, seq_in_index
        ";
        let idxs = try_execute_catalog_query(&my_cat, q_idx).unwrap().unwrap();
        assert!(idxs.rows.len() >= 3);

        // 8. Execute get_procedures
        let q_procs = "
            SELECT routine_name AS name,
                routine_definition AS definition
            FROM information_schema.routines
            WHERE ROUTINE_SCHEMA = 'tapisaurus'
            AND ROUTINE_TYPE = 'PROCEDURE'
        ";
        let procs = try_execute_catalog_query(&my_cat, q_procs)
            .unwrap()
            .unwrap();
        assert_eq!(procs.rows.len(), 0);
    }
}
