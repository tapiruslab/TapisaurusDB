//! PostgreSQL Wire Protocol Query Handler.
//!
//! Processes PostgreSQL Frontend 'Q' (Simple Query) messages,
//! executes them against `tapisaurus-query` and `tapisaurus-core`,
//! and generates the appropriate binary response packets.

use std::sync::Arc;
use tapisaurus_core::document::model::Value;
use tapisaurus_query::{parse_query, DatabaseContext, QueryResult};

use super::codec::{
    encode_command_complete, encode_data_row, encode_empty_query_response, encode_error_response,
    encode_ready_for_query, encode_row_description, PgField, PG_TYPE_BOOL, PG_TYPE_FLOAT8,
    PG_TYPE_INT8, PG_TYPE_JSONB, PG_TYPE_TEXT,
};

fn rollback_txn_undo_log(
    db: &Arc<DatabaseContext>,
    txn: &mut tapisaurus_core::transaction::mvcc::Transaction,
) {
    let undo_entries = txn.take_undo_log();
    for (col_name, doc_id, before_image) in undo_entries.into_iter().rev() {
        let col = db.get_or_create_collection(&col_name);
        let _ = col.delete_by_id(&doc_id);
        if let Some(prev_val) = before_image {
            if let Ok(prev_doc) = serde_json::from_value::<tapisaurus_core::Document>(prev_val) {
                let _ = col.insert(prev_doc);
            }
        }
    }
}

/// Handles a raw SQL string received via PostgreSQL protocol 'Q' message with explicit transaction state
/// and an optional MVCC SSI `Transaction` handle:
/// - b'I': Idle (outside transaction)
/// - b'T': In active transaction block
/// - b'E': In aborted/failed transaction block (all commands rejected until ROLLBACK)
pub fn handle_postgres_query_state(
    db: &Arc<DatabaseContext>,
    query_str: &str,
    txn_state: &mut u8,
) -> Vec<u8> {
    let mut no_txn = None;
    handle_postgres_query_state_with_txn(db, query_str, txn_state, &mut no_txn)
}

pub fn handle_postgres_query_state_with_txn(
    db: &Arc<DatabaseContext>,
    query_str: &str,
    txn_state: &mut u8,
    current_txn: &mut Option<
        Arc<parking_lot::Mutex<tapisaurus_core::transaction::mvcc::Transaction>>,
    >,
) -> Vec<u8> {
    let stmts = split_sql_statements(query_str);
    if stmts.len() <= 1 {
        let single = stmts.first().copied().unwrap_or(query_str);
        return handle_single_postgres_query_state_with_txn(db, single, txn_state, current_txn);
    }

    let mut combined = Vec::new();
    for (idx, stmt) in stmts.iter().enumerate() {
        let mut part =
            handle_single_postgres_query_state_with_txn(db, stmt, txn_state, current_txn);
        if idx + 1 < stmts.len() && *txn_state != b'E' {
            if part.len() >= 6 && part[part.len() - 6] == b'Z' {
                part.truncate(part.len() - 6);
            }
            combined.extend_from_slice(&part);
        } else {
            combined.extend_from_slice(&part);
            break;
        }
    }
    combined
}

fn split_sql_statements(input: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut in_sq = false;
    let mut in_dq = false;
    let bytes = input.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' if !in_dq => {
                if in_sq && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_sq = !in_sq;
            }
            b'"' if !in_sq => {
                in_dq = !in_dq;
            }
            b'-' if !in_sq && !in_dq && i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            b';' if !in_sq && !in_dq => {
                let seg = input[start..i].trim();
                if has_executable_sql(seg) {
                    parts.push(seg);
                }
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }

    let tail = input[start..].trim();
    if has_executable_sql(tail) {
        parts.push(tail);
    }
    parts
}

fn has_executable_sql(seg: &str) -> bool {
    seg.lines()
        .map(str::trim)
        .any(|line| !line.is_empty() && !line.starts_with("--"))
}

fn handle_single_postgres_query_state_with_txn(
    db: &Arc<DatabaseContext>,
    query_str: &str,
    txn_state: &mut u8,
    current_txn: &mut Option<
        Arc<parking_lot::Mutex<tapisaurus_core::transaction::mvcc::Transaction>>,
    >,
) -> Vec<u8> {
    let trimmed = query_str.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        let mut out = encode_empty_query_response();
        out.extend_from_slice(&encode_ready_for_query(*txn_state));
        return out;
    }

    let upper = trimmed.to_uppercase();

    // In failed transaction state (b'E'), reject any command except ROLLBACK or COMMIT
    if *txn_state == b'E' {
        if upper == "ROLLBACK"
            || upper == "ROLLBACK TRANSACTION"
            || upper == "ROLLBACK WORK"
            || upper.starts_with("ROLLBACK TO")
        {
            if let Some(t_mutex) = current_txn.take() {
                let mut t = t_mutex.lock();
                rollback_txn_undo_log(db, &mut t);
                db.tx_manager().abort(&mut t);
            }
            *txn_state = b'I';
            let mut out = encode_command_complete("ROLLBACK");
            out.extend_from_slice(&encode_ready_for_query(b'I'));
            return out;
        }
        if upper == "COMMIT"
            || upper == "COMMIT TRANSACTION"
            || upper == "COMMIT WORK"
            || upper == "END"
        {
            if let Some(t_mutex) = current_txn.take() {
                let mut t = t_mutex.lock();
                rollback_txn_undo_log(db, &mut t);
                db.tx_manager().abort(&mut t);
            }
            *txn_state = b'I';
            let mut out = encode_command_complete("ROLLBACK");
            out.extend_from_slice(&encode_ready_for_query(b'I'));
            return out;
        }
        let mut out = encode_error_response(
            "ERROR",
            "25P02",
            "current transaction is aborted, commands ignored until end of transaction block",
        );
        out.extend_from_slice(&encode_ready_for_query(b'E'));
        return out;
    }

    // 1. Handle SET statements (e.g. SET client_encoding = 'UTF8', SET extra_float_digits = 3, SET NAMES 'utf8')
    if upper.starts_with("SET ") || upper == "SET" {
        let mut out = encode_command_complete("SET");
        out.extend_from_slice(&encode_ready_for_query(*txn_state));
        return out;
    }

    // 2. Handle RESET / DISCARD statements
    if upper.starts_with("RESET ") || upper.starts_with("DISCARD ") {
        let mut out = encode_command_complete("RESET");
        out.extend_from_slice(&encode_ready_for_query(*txn_state));
        return out;
    }

    // 3. Handle Transactions with Real MVCC SSI Validation
    if upper == "BEGIN" || upper == "BEGIN TRANSACTION" || upper.starts_with("START TRANSACTION") {
        let txn = db.tx_manager().begin();
        *current_txn = Some(Arc::new(parking_lot::Mutex::new(txn)));
        *txn_state = b'T';
        let mut out = encode_command_complete("BEGIN");
        out.extend_from_slice(&encode_ready_for_query(b'T'));
        return out;
    }
    if upper == "COMMIT"
        || upper == "COMMIT TRANSACTION"
        || upper == "COMMIT WORK"
        || upper == "END"
    {
        if let Some(t_mutex) = current_txn.take() {
            let mut t = t_mutex.lock();
            if let Err(conflict_err) = db.tx_manager().commit(&mut t) {
                rollback_txn_undo_log(db, &mut t);
                *txn_state = b'I';
                let mut out = encode_error_response(
                    "ERROR",
                    "40001",
                    &format!("could not serialize access due to concurrent update: {conflict_err}"),
                );
                out.extend_from_slice(&encode_ready_for_query(b'I'));
                return out;
            }
        }
        *txn_state = b'I';
        let mut out = encode_command_complete("COMMIT");
        out.extend_from_slice(&encode_ready_for_query(b'I'));
        return out;
    }
    if upper == "ROLLBACK" || upper == "ROLLBACK TRANSACTION" || upper == "ROLLBACK WORK" {
        if let Some(t_mutex) = current_txn.take() {
            let mut t = t_mutex.lock();
            rollback_txn_undo_log(db, &mut t);
            db.tx_manager().abort(&mut t);
        }
        *txn_state = b'I';
        let mut out = encode_command_complete("ROLLBACK");
        out.extend_from_slice(&encode_ready_for_query(b'I'));
        return out;
    }

    // 4. Handle SHOW statements
    if upper.starts_with("SHOW ") {
        let param = trimmed[5..].trim().to_lowercase();
        return handle_show_variable(db, &param, *txn_state);
    }

    // 5. Dynamic Virtual System Catalog & ORM Introspection Engine (pg_catalog.*, information_schema.*)
    if upper.starts_with("SELECT") || upper.starts_with("WITH") || upper.starts_with("(SELECT") {
        if upper == "SELECT 1" || upper.starts_with("SELECT 1 AS") || upper.starts_with("SELECT 1 ")
        {
            let col_name = if upper.contains(" AS ") {
                trimmed.split_whitespace().last().unwrap_or("?column?")
            } else {
                "?column?"
            };
            return single_value_result(col_name, "1", *txn_state);
        }

        match db.query_virtual_catalog(trimmed, tapisaurus_catalog::CatalogDialect::Postgres) {
            Ok(Some(cat_result)) => {
                return format_catalog_result(cat_result, *txn_state);
            }
            Ok(None) => {}
            Err(cat_err) => {
                if *txn_state == b'T' {
                    *txn_state = b'E';
                }
                let mut out = encode_error_response("ERROR", "42601", &cat_err);
                out.extend_from_slice(&encode_ready_for_query(*txn_state));
                return out;
            }
        }
    }

    // 6. Execute general SQL query through `tapisaurus-query`
    match parse_query(trimmed) {
        Ok(stmt) => {
            // Track MVCC read/write sets and before-images when inside an active transaction block
            if *txn_state == b'T' {
                if current_txn.is_none() {
                    *current_txn = Some(Arc::new(parking_lot::Mutex::new(db.tx_manager().begin())));
                }
                if let Some(ref t_mutex) = current_txn {
                    let mut t = t_mutex.lock();
                    match &stmt {
                        tapisaurus_query::ast::Statement::Insert {
                            collection,
                            documents,
                        } => {
                            let col = db.get_or_create_collection(collection);
                            for doc in documents {
                                let doc_id = doc.id.as_str().to_string();
                                let prev = col
                                    .find_by_id(&doc_id)
                                    .ok()
                                    .and_then(|d| serde_json::to_value(&d).ok());
                                let mvcc_key = format!("doc:{collection}:{doc_id}").into_bytes();
                                let mvcc_val = serde_json::to_vec(doc).unwrap_or_default();
                                let _ = t.put(mvcc_key, mvcc_val);
                                t.record_undo(collection.clone(), doc_id, prev);
                            }
                        }
                        tapisaurus_query::ast::Statement::Update {
                            collection, filter, ..
                        } => {
                            let col = db.get_or_create_collection(collection);
                            for doc in col.find_all(None) {
                                if filter.matches(&doc) {
                                    let doc_id = doc.id.as_str().to_string();
                                    let prev = serde_json::to_value(&doc).ok();
                                    let mvcc_key =
                                        format!("doc:{collection}:{doc_id}").into_bytes();
                                    t.record_read(&mvcc_key);
                                    let _ = t.put(
                                        mvcc_key,
                                        serde_json::to_vec(&doc).unwrap_or_default(),
                                    );
                                    t.record_undo(collection.clone(), doc_id, prev);
                                }
                            }
                        }
                        tapisaurus_query::ast::Statement::Delete { collection, filter } => {
                            let col = db.get_or_create_collection(collection);
                            for doc in col.find_all(None) {
                                if filter.matches(&doc) {
                                    let doc_id = doc.id.as_str().to_string();
                                    let prev = serde_json::to_value(&doc).ok();
                                    let mvcc_key =
                                        format!("doc:{collection}:{doc_id}").into_bytes();
                                    t.record_read(&mvcc_key);
                                    let _ = t.delete(mvcc_key);
                                    t.record_undo(collection.clone(), doc_id, prev);
                                }
                            }
                        }
                        tapisaurus_query::ast::Statement::Find { collection, .. } => {
                            let mvcc_scan_key = format!("scan:{collection}").into_bytes();
                            t.record_read(&mvcc_scan_key);
                        }
                        _ => {}
                    }
                }
            }

            let find_col = match &stmt {
                tapisaurus_query::ast::Statement::Find { collection, .. } => {
                    Some(collection.clone())
                }
                _ => None,
            };

            match db.execute(stmt) {
                Ok(result) => {
                    if *txn_state == b'T' {
                        if let (Some(ref col_name), QueryResult::Documents(ref docs)) =
                            (&find_col, &result)
                        {
                            if let Some(ref t_mutex) = current_txn {
                                let mut t = t_mutex.lock();
                                for doc in docs {
                                    let key =
                                        format!("doc:{col_name}:{}", doc.id.as_str()).into_bytes();
                                    t.record_read(&key);
                                }
                            }
                        }
                    }
                    format_query_result(result, *txn_state)
                }
                Err(exec_err) => {
                    if *txn_state == b'T' {
                        *txn_state = b'E'; // Transition to aborted transaction state
                    }
                    let mut out = encode_error_response("ERROR", "XX000", &exec_err);
                    out.extend_from_slice(&encode_ready_for_query(*txn_state));
                    out
                }
            }
        }
        Err(parse_err) => {
            if *txn_state == b'T' {
                *txn_state = b'E'; // Transition to aborted transaction state
            }
            let mut out =
                encode_error_response("ERROR", "42601", &format!("SQL syntax error: {parse_err}"));
            out.extend_from_slice(&encode_ready_for_query(*txn_state));
            out
        }
    }
}

/// Backward-compatible adapter for handle_postgres_query taking &mut bool
pub fn handle_postgres_query(
    db: &Arc<DatabaseContext>,
    query_str: &str,
    in_transaction: &mut bool,
) -> Vec<u8> {
    let mut state = if *in_transaction { b'T' } else { b'I' };
    let resp = handle_postgres_query_state(db, query_str, &mut state);
    *in_transaction = state == b'T';
    resp
}

/// Handle SHOW queries (e.g. SHOW client_encoding, SHOW TABLES)
fn handle_show_variable(db: &Arc<DatabaseContext>, var: &str, txn_state: u8) -> Vec<u8> {
    if var == "tables" || var == "collections" {
        return handle_list_tables(db, txn_state);
    }

    let val = match var {
        "client_encoding" | "server_encoding" => "UTF8",
        "server_version" => "16.0 (TapisaurusDB)",
        "server_version_num" => "160000",
        "transaction_isolation"
        | "default_transaction_isolation"
        | "transaction isolation level" => "read committed",
        "standard_conforming_strings" => "on",
        "datestyle" => "ISO, MDY",
        "timezone" => "UTC",
        "integer_datetimes" => "on",
        "max_identifier_length" => "63",
        "max_connections" => "10000",
        _ => "off",
    };

    single_value_result(var, val, txn_state)
}

/// Format a `CatalogResult` returned by `tapisaurus-catalog` into PostgreSQL wire packets.
fn format_catalog_result(res: tapisaurus_catalog::CatalogResult, txn_state: u8) -> Vec<u8> {
    let fields: Vec<PgField> = res
        .columns
        .iter()
        .map(|col| PgField::new(col.name.clone(), col.pg_type_oid))
        .collect();
    let mut out = encode_row_description(&fields);

    for row in &res.rows {
        let encoded_row: Vec<Option<String>> = row
            .iter()
            .map(|cell| {
                cell.as_ref()
                    .and_then(tapisaurus_catalog::CatalogValue::to_pg_text)
            })
            .collect();
        out.extend_from_slice(&encode_data_row(&encoded_row));
    }

    let tag = format!("SELECT {}", res.rows.len());
    out.extend_from_slice(&encode_command_complete(&tag));
    out.extend_from_slice(&encode_ready_for_query(txn_state));
    out
}

/// Handle table / collection listing for `SHOW TABLES`
fn handle_list_tables(db: &Arc<DatabaseContext>, txn_state: u8) -> Vec<u8> {
    let collections = db.list_collections();
    let fields = vec![
        PgField::text("table_name"),
        PgField::text("table_schema"),
        PgField::text("table_type"),
    ];

    let mut out = encode_row_description(&fields);

    for col in &collections {
        let row = vec![
            Some(col.clone()),
            Some("public".to_string()),
            Some("BASE TABLE".to_string()),
        ];
        out.extend_from_slice(&encode_data_row(&row));
    }

    let tag = format!("SELECT {}", collections.len());
    out.extend_from_slice(&encode_command_complete(&tag));
    out.extend_from_slice(&encode_ready_for_query(txn_state));
    out
}

/// Helper to return a single-column, single-row result
fn single_value_result(col_name: &str, value: &str, txn_state: u8) -> Vec<u8> {
    let fields = vec![PgField::text(col_name)];
    let mut out = encode_row_description(&fields);

    let row = vec![Some(value.to_string())];
    out.extend_from_slice(&encode_data_row(&row));

    out.extend_from_slice(&encode_command_complete("SELECT 1"));
    out.extend_from_slice(&encode_ready_for_query(txn_state));
    out
}

/// Formats a TapisaurusDB QueryResult into PostgreSQL DataRow / CommandComplete packets
fn format_query_result(result: QueryResult, txn_state: u8) -> Vec<u8> {
    let mut out = Vec::new();

    match result {
        QueryResult::Documents(docs) => {
            if docs.is_empty() {
                let fields = vec![PgField::text("id")];
                out.extend_from_slice(&encode_row_description(&fields));
                out.extend_from_slice(&encode_command_complete("SELECT 0"));
            } else {
                let mut col_names = Vec::new();
                let mut col_types = Vec::new();

                col_names.push("_id".to_string());
                col_types.push(PG_TYPE_TEXT);

                let mut seen = std::collections::HashSet::new();
                seen.insert("_id".to_string());
                seen.insert("id".to_string());

                for doc in &docs {
                    for (k, v) in doc.fields.iter() {
                        if seen.insert(k.clone()) {
                            col_names.push(k.clone());
                            let type_oid = match v {
                                Value::Boolean(_) => PG_TYPE_BOOL,
                                Value::Integer(_) => PG_TYPE_INT8,
                                Value::Float(_) => PG_TYPE_FLOAT8,
                                Value::String(_) => PG_TYPE_TEXT,
                                Value::Array(_) | Value::Object(_) => PG_TYPE_JSONB,
                                Value::Binary(_)
                                | Value::DateTime(_)
                                | Value::Uuid(_)
                                | Value::Vector(_)
                                | Value::Null => PG_TYPE_TEXT,
                            };
                            col_types.push(type_oid);
                        }
                    }
                }

                let fields: Vec<PgField> = col_names
                    .iter()
                    .zip(col_types.iter())
                    .map(|(name, &oid)| PgField::new(name.clone(), oid))
                    .collect();

                out.extend_from_slice(&encode_row_description(&fields));

                for doc in &docs {
                    let mut row_vals = Vec::with_capacity(col_names.len());
                    for name in &col_names {
                        let val_str = if name == "_id" || name == "id" {
                            Some(doc.id.as_str().to_string())
                        } else {
                            match doc.get(name) {
                                Some(Value::Null) | None => None,
                                Some(Value::String(s)) => Some(s.clone()),
                                Some(Value::Integer(i)) => Some(i.to_string()),
                                Some(Value::Float(f)) => Some(f.to_string()),
                                Some(Value::Boolean(b)) => Some(b.to_string()),
                                Some(Value::Array(arr)) => {
                                    Some(serde_json::to_string(arr).unwrap_or_default())
                                }
                                Some(Value::Object(obj)) => {
                                    Some(serde_json::to_string(obj).unwrap_or_default())
                                }
                                Some(Value::DateTime(dt)) => Some(dt.to_rfc3339()),
                                Some(Value::Uuid(u)) => Some(u.to_string()),
                                Some(Value::Vector(v)) => Some(format!("{v:?}")),
                                Some(Value::Binary(b)) => {
                                    Some(format!("<binary {} bytes>", b.len()))
                                }
                            }
                        };
                        row_vals.push(val_str);
                    }
                    out.extend_from_slice(&encode_data_row(&row_vals));
                }

                let tag = format!("SELECT {}", docs.len());
                out.extend_from_slice(&encode_command_complete(&tag));
            }
        }
        QueryResult::Inserted(ids) => {
            let tag = format!("INSERT 0 {}", ids.len());
            out.extend_from_slice(&encode_command_complete(&tag));
        }
        QueryResult::Updated(count) => {
            let tag = format!("UPDATE {}", count);
            out.extend_from_slice(&encode_command_complete(&tag));
        }
        QueryResult::Deleted(count) => {
            let tag = format!("DELETE {}", count);
            out.extend_from_slice(&encode_command_complete(&tag));
        }
        QueryResult::Count(c) => {
            let fields = vec![PgField::new("count", PG_TYPE_INT8)];
            out.extend_from_slice(&encode_row_description(&fields));
            let row = vec![Some(c.to_string())];
            out.extend_from_slice(&encode_data_row(&row));
            out.extend_from_slice(&encode_command_complete("SELECT 1"));
        }
        QueryResult::Success(msg) => {
            out.extend_from_slice(&encode_command_complete(&msg));
        }
        QueryResult::Explain(plan) => {
            let fields = vec![PgField::text("QUERY PLAN")];
            out.extend_from_slice(&encode_row_description(&fields));

            let lines: Vec<String> = if let Some(ref pg_tree) = plan.formatted_pg_tree {
                pg_tree.lines().map(|s| s.to_string()).collect()
            } else {
                vec![
                    format!("Plan: {}", plan.plan_type),
                    format!("Collection: {}", plan.collection),
                    format!(
                        "Index Used: {}",
                        plan.index_used
                            .unwrap_or_else(|| "None (Full Scan)".to_string())
                    ),
                    format!("Documents Examined: {}", plan.documents_examined),
                    format!("Documents Returned: {}", plan.documents_returned),
                    format!("Execution Time: {} µs", plan.execution_time_us),
                    format!("Cost Score: {:.2}", plan.estimated_cost_score),
                ]
            };

            for line in lines {
                out.extend_from_slice(&encode_data_row(&[Some(line)]));
            }

            out.extend_from_slice(&encode_command_complete("EXPLAIN"));
        }
    }

    out.extend_from_slice(&encode_ready_for_query(txn_state));
    out
}

/// Executes a query for PostgreSQL Extended Query Protocol ('E' message) without trailing ReadyForQuery ('Z')
pub fn handle_postgres_execute_query_state(
    db: &Arc<DatabaseContext>,
    query_str: &str,
    txn_state: &mut u8,
) -> Vec<u8> {
    let mut no_txn = None;
    handle_postgres_execute_query_state_with_txn(db, query_str, txn_state, &mut no_txn)
}

pub fn handle_postgres_execute_query_state_with_txn(
    db: &Arc<DatabaseContext>,
    query_str: &str,
    txn_state: &mut u8,
    current_txn: &mut Option<
        Arc<parking_lot::Mutex<tapisaurus_core::transaction::mvcc::Transaction>>,
    >,
) -> Vec<u8> {
    let mut out = handle_postgres_query_state_with_txn(db, query_str, txn_state, current_txn);
    if out.len() >= 6 && out[out.len() - 6] == b'Z' {
        out.truncate(out.len() - 6);
    }
    out
}

/// Backward-compatible adapter for handle_postgres_execute_query taking &mut bool
pub fn handle_postgres_execute_query(
    db: &Arc<DatabaseContext>,
    query_str: &str,
    in_transaction: &mut bool,
) -> Vec<u8> {
    let mut state = if *in_transaction { b'T' } else { b'I' };
    let resp = handle_postgres_execute_query_state(db, query_str, &mut state);
    *in_transaction = state == b'T';
    resp
}

/// Infers RowDescription for a query without executing it, used by PostgreSQL Describe ('D') messages.
/// Returns Some(fields) for queries that return rows (e.g. SELECT, SHOW, EXPLAIN),
/// or None for queries that return no rows (INSERT, UPDATE, DELETE, DDL, TRANSACTIONS).
pub fn infer_query_row_description(
    db: &Arc<DatabaseContext>,
    query_str: &str,
) -> Option<Vec<PgField>> {
    let trimmed = query_str.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return None;
    }
    let upper = trimmed.to_uppercase();

    if upper.starts_with("SHOW ") {
        let var = trimmed[5..].trim().to_lowercase();
        if var == "tables" || var == "collections" {
            return Some(vec![
                PgField::text("table_name"),
                PgField::text("table_schema"),
                PgField::text("table_type"),
            ]);
        }
        return Some(vec![PgField::text(var)]);
    }

    if upper.starts_with("EXPLAIN") {
        return Some(vec![PgField::text("QUERY PLAN")]);
    }

    if !upper.starts_with("SELECT") && !upper.starts_with("WITH") && !upper.starts_with("(SELECT") {
        return None;
    }

    if upper == "SELECT 1" || upper.starts_with("SELECT 1 AS") || upper.starts_with("SELECT 1 ") {
        let col_name = if upper.contains(" AS ") {
            trimmed.split_whitespace().last().unwrap_or("?column?")
        } else {
            "?column?"
        };
        return Some(vec![PgField::new(col_name, PG_TYPE_INT8)]);
    }

    if let Ok(Some(cols)) =
        db.describe_virtual_catalog(trimmed, tapisaurus_catalog::CatalogDialect::Postgres)
    {
        return Some(
            cols.into_iter()
                .map(|c| PgField::new(c.name, c.pg_type_oid))
                .collect(),
        );
    }

    // General collection query: extract normalized collection name and inspect effective schema
    if let Some(from_idx) = upper.find(" FROM ") {
        let after_from = trimmed[from_idx + 6..].trim();
        let raw_col = after_from.split_whitespace().next().unwrap_or("");
        let col_name = tapisaurus_query::ddl::normalize_table_ident(raw_col);
        if !col_name.is_empty() {
            let col = db.get_or_create_collection(&col_name);
            let fields: Vec<PgField> = col
                .effective_columns()
                .into_iter()
                .map(|c| PgField::new(c.name, c.pg_type_oid))
                .collect();
            return Some(if fields.is_empty() {
                vec![PgField::text("_id")]
            } else {
                fields
            });
        }
    }

    Some(vec![PgField::text("id")])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_simple_query_mvcc_rollback_and_ssi_conflict() {
        let db = Arc::new(DatabaseContext::new());
        let mut state1 = b'I';
        let mut txn1 = None;

        // 1. BEGIN -> INSERT -> ROLLBACK must undo the inserted document
        handle_postgres_query_state_with_txn(&db, "BEGIN", &mut state1, &mut txn1);
        assert_eq!(state1, b'T');
        handle_postgres_query_state_with_txn(
            &db,
            "INSERT INTO accounts (_id, balance) VALUES ('acc_1', 500)",
            &mut state1,
            &mut txn1,
        );
        assert!(db
            .get_or_create_collection("accounts")
            .find_by_id("acc_1")
            .is_ok());

        handle_postgres_query_state_with_txn(&db, "ROLLBACK", &mut state1, &mut txn1);
        assert_eq!(state1, b'I');
        assert!(db
            .get_or_create_collection("accounts")
            .find_by_id("acc_1")
            .is_err());

        // 2. Seed acc_1 outside txn, then test concurrent SSI conflict on COMMIT
        handle_postgres_query_state_with_txn(
            &db,
            "INSERT INTO accounts (_id, balance) VALUES ('acc_1', 500)",
            &mut state1,
            &mut txn1,
        );

        let mut state_a = b'I';
        let mut txn_a = None;
        let mut state_b = b'I';
        let mut txn_b = None;

        handle_postgres_query_state_with_txn(&db, "BEGIN", &mut state_a, &mut txn_a);
        handle_postgres_query_state_with_txn(&db, "BEGIN", &mut state_b, &mut txn_b);

        handle_postgres_query_state_with_txn(
            &db,
            "UPDATE accounts SET balance = 600 WHERE _id = 'acc_1'",
            &mut state_a,
            &mut txn_a,
        );
        handle_postgres_query_state_with_txn(
            &db,
            "UPDATE accounts SET balance = 700 WHERE _id = 'acc_1'",
            &mut state_b,
            &mut txn_b,
        );

        // Txn A commits first -> succeeds
        let out_a = handle_postgres_query_state_with_txn(&db, "COMMIT", &mut state_a, &mut txn_a);
        assert!(String::from_utf8_lossy(&out_a).contains("COMMIT"));

        // Txn B attempts to commit -> fails with SQLSTATE 40001 and rolls back to 600
        let out_b = handle_postgres_query_state_with_txn(&db, "COMMIT", &mut state_b, &mut txn_b);
        let out_b_str = String::from_utf8_lossy(&out_b);
        assert!(out_b_str.contains("40001"));
    }
}
