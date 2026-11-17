//! End-to-End Virtual System Catalog & ORM Introspection Integration Tests.
//!
//! Spins up live PostgreSQL Wire (5432) and MySQL Wire (3306) TCP listeners backed by a
//! persistent `DatabaseContext` and runs real Prisma, SQLAlchemy, TypeORM, Drizzle, and
//! Laravel/Doctrine DBAL introspection queries across `pg_catalog.*` and `information_schema.*`.

use bytes::{BufMut, BytesMut};
use std::sync::Arc;
use tapisaurus_query::{parse_query, DatabaseContext};
use tapisaurus_security::UserStore;
use tapisaurus_server::wire::mysql::codec::{
    encode_packet, read_lenenc_int, CLIENT_CONNECT_WITH_DB, CLIENT_PROTOCOL_41,
};
use tapisaurus_server::wire::mysql::handler::handle_mysql_query;
use tapisaurus_server::wire::mysql::run_mysql_server_with_shutdown;
use tapisaurus_server::wire::postgres::handler::{
    handle_postgres_query_state, infer_query_row_description,
};
use tapisaurus_server::wire::postgres::run_postgres_server_with_shutdown;
use tapisaurus_vector::{HnswConfig, HnswIndex};
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn build_pg_startup(user: &str, database: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i32(196_608); // v3.0
    body.extend_from_slice(b"user\0");
    body.extend_from_slice(user.as_bytes());
    body.put_u8(0);
    body.extend_from_slice(b"database\0");
    body.extend_from_slice(database.as_bytes());
    body.put_u8(0);
    body.put_u8(0);

    let mut pkt = BytesMut::with_capacity(4 + body.len());
    pkt.put_i32((4 + body.len()) as i32);
    pkt.extend_from_slice(&body);
    pkt.to_vec()
}

fn build_pg_password(password: &str) -> Vec<u8> {
    let payload_len = 4 + password.len() + 1;
    let mut pkt = BytesMut::with_capacity(1 + payload_len);
    pkt.put_u8(b'p');
    pkt.put_i32(payload_len as i32);
    pkt.extend_from_slice(password.as_bytes());
    pkt.put_u8(0);
    pkt.to_vec()
}

fn build_pg_parse(stmt_name: &str, query: &str, param_oids: &[i32]) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.extend_from_slice(stmt_name.as_bytes());
    body.put_u8(0);
    body.extend_from_slice(query.as_bytes());
    body.put_u8(0);
    body.put_i16(param_oids.len() as i16);
    for &oid in param_oids {
        body.put_i32(oid);
    }
    let mut pkt = BytesMut::with_capacity(5 + body.len());
    pkt.put_u8(b'P');
    pkt.put_i32((4 + body.len()) as i32);
    pkt.extend_from_slice(&body);
    pkt.to_vec()
}

fn build_pg_describe_stmt(stmt_name: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_u8(b'S');
    body.extend_from_slice(stmt_name.as_bytes());
    body.put_u8(0);
    let mut pkt = BytesMut::with_capacity(5 + body.len());
    pkt.put_u8(b'D');
    pkt.put_i32((4 + body.len()) as i32);
    pkt.extend_from_slice(&body);
    pkt.to_vec()
}

fn build_pg_bind(portal_name: &str, stmt_name: &str, params: &[&str]) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.extend_from_slice(portal_name.as_bytes());
    body.put_u8(0);
    body.extend_from_slice(stmt_name.as_bytes());
    body.put_u8(0);
    body.put_i16(0); // default text formats
    body.put_i16(params.len() as i16);
    for p in params {
        body.put_i32(p.len() as i32);
        body.extend_from_slice(p.as_bytes());
    }
    body.put_i16(0); // default result formats
    let mut pkt = BytesMut::with_capacity(5 + body.len());
    pkt.put_u8(b'B');
    pkt.put_i32((4 + body.len()) as i32);
    pkt.extend_from_slice(&body);
    pkt.to_vec()
}

fn build_pg_execute(portal_name: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.extend_from_slice(portal_name.as_bytes());
    body.put_u8(0);
    body.put_i32(0); // unlimited rows
    let mut pkt = BytesMut::with_capacity(5 + body.len());
    pkt.put_u8(b'E');
    pkt.put_i32((4 + body.len()) as i32);
    pkt.extend_from_slice(&body);
    pkt.to_vec()
}

fn build_pg_sync() -> Vec<u8> {
    let mut pkt = BytesMut::with_capacity(5);
    pkt.put_u8(b'S');
    pkt.put_i32(4);
    pkt.to_vec()
}

async fn read_pg_packet(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut hdr = [0u8; 5];
    stream.read_exact(&mut hdr).await.unwrap();
    let msg_type = hdr[0];
    let len = i32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
    let body_len = len.saturating_sub(4);
    let mut body = vec![0u8; body_len];
    if body_len > 0 {
        stream.read_exact(&mut body).await.unwrap();
    }
    (msg_type, body)
}

fn decode_pg_row_desc(body: &[u8]) -> Vec<String> {
    let count = i16::from_be_bytes([body[0], body[1]]) as usize;
    let mut pos = 2;
    let mut names = Vec::with_capacity(count);
    for _ in 0..count {
        let nul = body[pos..].iter().position(|&b| b == 0).unwrap();
        let name = String::from_utf8_lossy(&body[pos..pos + nul]).to_string();
        names.push(name);
        pos += nul + 1 + 4 + 2 + 4 + 2 + 4 + 2;
    }
    names
}

fn decode_pg_data_row(body: &[u8]) -> Vec<Option<String>> {
    let count = i16::from_be_bytes([body[0], body[1]]) as usize;
    let mut pos = 2;
    let mut cols = Vec::with_capacity(count);
    for _ in 0..count {
        let len = i32::from_be_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]]);
        pos += 4;
        if len < 0 {
            cols.push(None);
        } else {
            let l = len as usize;
            let s = String::from_utf8_lossy(&body[pos..pos + l]).to_string();
            cols.push(Some(s));
            pos += l;
        }
    }
    cols
}

fn read_lenenc_str(buf: &mut bytes::Bytes) -> Option<String> {
    if buf.first() == Some(&0xFB) {
        let _ = buf.split_to(1);
        return None;
    }
    let len = read_lenenc_int(buf)? as usize;
    let slice = buf.split_to(len);
    Some(String::from_utf8_lossy(&slice).to_string())
}

fn parse_mysql_result_rows(packets: &[bytes::Bytes]) -> (Vec<String>, Vec<Vec<Option<String>>>) {
    // Packet 0: column count lenenc
    let col_count = packets[0][4] as usize;
    let mut col_names = Vec::with_capacity(col_count);
    for pkt in &packets[1..=col_count] {
        // Parse 5 lenenc strings inside ColumnDef41: catalog, schema, table, org_table, name
        let mut cursor = pkt.slice(4..);
        let mut name = String::new();
        for i in 0..5 {
            if let Some(s) = read_lenenc_str(&mut cursor) {
                if i == 4 {
                    name = s;
                }
            }
        }
        col_names.push(name);
    }

    // After col_count column defs + 1 EOF packet, rows run until final EOF packet
    let mut rows = Vec::new();
    for pkt in &packets[1 + col_count + 1..] {
        let mut cursor = pkt.slice(4..);
        if cursor.first() == Some(&0xFE) && cursor.len() < 9 {
            break;
        }
        let mut row = Vec::with_capacity(col_count);
        while row.len() < col_count && !cursor.is_empty() {
            row.push(read_lenenc_str(&mut cursor));
        }
        rows.push(row);
    }

    (col_names, rows)
}

#[tokio::test]
async fn test_live_tcp_orm_introspection_postgres_and_mysql() {
    let dir = tempdir().unwrap();
    let db = Arc::new(DatabaseContext::with_storage_dir(dir.path()).unwrap());
    let user_store = Arc::new(UserStore::new());

    // 1. Seed relational schema DDL (users + orders with PK, UNIQUE, NOT NULL, DEFAULT, FK, HNSW)
    db.execute(
        parse_query(
            "CREATE TABLE users (
                id BIGINT PRIMARY KEY,
                email VARCHAR(255) NOT NULL UNIQUE,
                active BOOLEAN NOT NULL DEFAULT true
            )",
        )
        .unwrap(),
    )
    .unwrap();

    db.execute(
        parse_query(
            "CREATE TABLE orders (
                id BIGINT PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                total NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
                embedding VECTOR(4)
            )",
        )
        .unwrap(),
    )
    .unwrap();

    db.execute(parse_query("CREATE INDEX idx_orders_user ON orders (user_id)").unwrap())
        .unwrap();

    let hnsw = Arc::new(parking_lot::RwLock::new(HnswIndex::new(HnswConfig {
        dimensions: 4,
        ..Default::default()
    })));
    db.vector_indexes().insert("orders".to_string(), hnsw);

    // 2. Bind ephemeral localhost ports simulating PG 5432 and MySQL 3306
    let pg_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let pg_addr = pg_listener.local_addr().unwrap().to_string();
    drop(pg_listener);

    let mysql_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mysql_addr = mysql_listener.local_addr().unwrap().to_string();
    drop(mysql_listener);

    let (pg_tx, pg_rx) = tokio::sync::oneshot::channel::<()>();
    let (my_tx, my_rx) = tokio::sync::oneshot::channel::<()>();

    let pg_db = db.clone();
    let pg_users = user_store.clone();
    let pg_addr_srv = pg_addr.clone();
    let pg_task = tokio::spawn(async move {
        run_postgres_server_with_shutdown(&pg_addr_srv, pg_db, pg_users, async {
            let _ = pg_rx.await;
        })
        .await
        .unwrap();
    });

    let my_db = db.clone();
    let my_users = user_store.clone();
    let my_addr_srv = mysql_addr.clone();
    let my_task = tokio::spawn(async move {
        run_mysql_server_with_shutdown(&my_addr_srv, my_db, my_users, async {
            let _ = my_rx.await;
        })
        .await
        .unwrap();
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(40)).await;

    // ── A. PostgreSQL TCP Extended Query Protocol (Prisma / SQLAlchemy flow) ──
    let mut pg = TcpStream::connect(&pg_addr).await.unwrap();
    pg.write_all(&build_pg_startup("admin", "tapisaurus"))
        .await
        .unwrap();
    let (auth_t, auth_b) = read_pg_packet(&mut pg).await;
    assert_eq!(auth_t, b'R');
    if auth_b == [0, 0, 0, 3] {
        pg.write_all(&build_pg_password("tapisaurus-admin-2026"))
            .await
            .unwrap();
        let (ok_t, _) = read_pg_packet(&mut pg).await;
        assert_eq!(ok_t, b'R');
    }
    loop {
        let (t, _) = read_pg_packet(&mut pg).await;
        if t == b'Z' {
            break;
        }
    }

    // Extended Query: Parse -> Describe -> Bind($1 = "public") -> Execute -> Sync
    let orm_join_query = "
        SELECT c.relname AS table_name, a.attname AS col_name,
               pg_catalog.format_type(a.atttypid, a.atttypmod) AS formatted_type,
               a.attnotnull
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
        WHERE n.nspname = $1 AND c.relname = 'orders' AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
    ";
    pg.write_all(&build_pg_parse("s_cols", orm_join_query, &[]))
        .await
        .unwrap();
    pg.write_all(&build_pg_describe_stmt("s_cols"))
        .await
        .unwrap();
    pg.write_all(&build_pg_bind("p_cols", "s_cols", &["public"]))
        .await
        .unwrap();
    pg.write_all(&build_pg_execute("p_cols")).await.unwrap();
    pg.write_all(&build_pg_sync()).await.unwrap();
    pg.flush().await.unwrap();

    let mut described_cols = Vec::new();
    let mut rows = Vec::new();
    loop {
        let (t, body) = read_pg_packet(&mut pg).await;
        match t {
            b'T' => {
                // Describe Statement returns RowDescription AND Execute also emits RowDescription
                described_cols = decode_pg_row_desc(&body);
            }
            b'D' => rows.push(decode_pg_data_row(&body)),
            b'Z' => break,
            b'E' => panic!("PG Error packet: {}", String::from_utf8_lossy(&body)),
            _ => {}
        }
    }

    assert_eq!(
        described_cols,
        vec!["table_name", "col_name", "formatted_type", "attnotnull"]
    );
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0][1].as_deref(), Some("id"));
    assert_eq!(rows[0][2].as_deref(), Some("bigint"));
    assert_eq!(rows[0][3].as_deref(), Some("t"));
    assert_eq!(rows[2][1].as_deref(), Some("total"));
    assert_eq!(rows[2][2].as_deref(), Some("numeric(12,2)"));
    assert_eq!(rows[3][2].as_deref(), Some("vector(4)"));

    // Check FK + Index catalog introspection on PG
    let mut txn_state = b'I';
    let fk_pkt = handle_postgres_query_state(
        &db,
        "SELECT kcu.column_name, kcu.referenced_table_name, kcu.referenced_column_name, rc.delete_rule
         FROM information_schema.key_column_usage kcu
         JOIN information_schema.referential_constraints rc
           ON rc.constraint_name = kcu.constraint_name
         WHERE kcu.table_name = 'orders' AND kcu.referenced_table_name IS NOT NULL",
        &mut txn_state,
    );
    let fk_str = String::from_utf8_lossy(&fk_pkt);
    assert!(fk_str.contains("user_id"));
    assert!(fk_str.contains("users"));
    assert!(fk_str.contains("CASCADE"));

    let idx_pkt = handle_postgres_query_state(
        &db,
        "SELECT i.relname, am.amname FROM pg_catalog.pg_index ix JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid JOIN pg_catalog.pg_am am ON am.oid = i.relam WHERE t.relname = 'orders' ORDER BY i.relname",
        &mut txn_state,
    );
    let idx_str = String::from_utf8_lossy(&idx_pkt);
    assert!(idx_str.contains("idx_user_id"));
    assert!(idx_str.contains("btree"));
    assert!(idx_str.contains("orders_vector_hnsw_idx"));
    assert!(idx_str.contains("hnsw"));

    // Verify Describe inference for regular DDL table even before rows exist
    let desc_orders = infer_query_row_description(&db, "SELECT * FROM \"public\".\"orders\"")
        .expect("Describe must infer columns from relational schema");
    assert_eq!(desc_orders.len(), 4);
    assert_eq!(desc_orders[1].name, "user_id");

    // ── B. MySQL TCP Wire Introspection (SQLAlchemy MySQL / Laravel / Doctrine DBAL) ──
    let mut my = TcpStream::connect(&mysql_addr).await.unwrap();
    let mut hdr = [0u8; 4];
    my.read_exact(&mut hdr).await.unwrap();
    let g_len = (hdr[0] as usize) | ((hdr[1] as usize) << 8) | ((hdr[2] as usize) << 16);
    let mut g_payload = vec![0u8; g_len];
    my.read_exact(&mut g_payload).await.unwrap();
    assert_eq!(g_payload[0], 10);

    let mut hs = BytesMut::new();
    let caps = CLIENT_PROTOCOL_41 | CLIENT_CONNECT_WITH_DB;
    hs.extend_from_slice(&caps.to_le_bytes());
    hs.extend_from_slice(&16_777_216u32.to_le_bytes());
    hs.extend_from_slice(&[45u8]);
    hs.extend_from_slice(&[0u8; 23]);
    hs.extend_from_slice(b"root\0\0tapisaurus\0");
    my.write_all(&encode_packet(1, &hs)).await.unwrap();
    my.flush().await.unwrap();

    my.read_exact(&mut hdr).await.unwrap();
    let ok_len = (hdr[0] as usize) | ((hdr[1] as usize) << 8) | ((hdr[2] as usize) << 16);
    let mut ok_buf = vec![0u8; ok_len];
    my.read_exact(&mut ok_buf).await.unwrap();
    assert_eq!(ok_buf[0], 0x00);

    // Send live COM_QUERY against information_schema.columns over TCP
    let info_q = b"\x03SELECT column_name, column_type, is_nullable, column_key FROM information_schema.columns WHERE table_schema = 'tapisaurus' AND table_name = 'orders' ORDER BY ordinal_position";
    my.write_all(&encode_packet(0, info_q)).await.unwrap();
    my.flush().await.unwrap();

    let mut collected_pkts = Vec::new();
    let mut eof_seen = 0;
    loop {
        my.read_exact(&mut hdr).await.unwrap();
        let plen = (hdr[0] as usize) | ((hdr[1] as usize) << 8) | ((hdr[2] as usize) << 16);
        let mut pbuf = vec![0u8; plen];
        my.read_exact(&mut pbuf).await.unwrap();
        let is_eof = pbuf.first() == Some(&0xFE) && plen < 9;
        let mut full = BytesMut::with_capacity(4 + plen);
        full.extend_from_slice(&hdr);
        full.extend_from_slice(&pbuf);
        collected_pkts.push(full.freeze());
        if is_eof {
            eof_seen += 1;
            if eof_seen == 2 {
                break;
            }
        }
    }
    let (my_cols, my_rows) = parse_mysql_result_rows(&collected_pkts);
    assert_eq!(
        my_cols,
        vec!["column_name", "column_type", "is_nullable", "column_key"]
    );
    assert_eq!(my_rows.len(), 4);
    assert_eq!(my_rows[0][0].as_deref(), Some("id"));
    assert_eq!(my_rows[0][3].as_deref(), Some("PRI"));
    assert_eq!(my_rows[1][0].as_deref(), Some("user_id"));
    assert_eq!(my_rows[1][2].as_deref(), Some("NO"));
    assert_eq!(my_rows[1][3].as_deref(), Some("MUL"));

    // SHOW FULL TABLES and SHOW CREATE TABLE for SQLAlchemy MySQL inspector
    let full_tables_pkts = handle_mysql_query(&db, "tapisaurus", "SHOW FULL TABLES", 1);
    let (ft_cols, ft_rows) = parse_mysql_result_rows(&full_tables_pkts);
    assert_eq!(ft_cols, vec!["Tables_in_tapisaurus", "Table_type"]);
    assert!(ft_rows
        .iter()
        .any(|r| r[0].as_deref() == Some("orders") && r[1].as_deref() == Some("BASE TABLE")));

    let create_pkts = handle_mysql_query(&db, "tapisaurus", "SHOW CREATE TABLE `orders`", 1);
    let (_, create_rows) = parse_mysql_result_rows(&create_pkts);
    let create_sql = create_rows[0][1].as_deref().unwrap();
    assert!(create_sql.contains("PRIMARY KEY (`id`)"));
    assert!(
        create_sql.contains("FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE")
    );

    drop(pg);
    drop(my);
    let _ = pg_tx.send(());
    let _ = my_tx.send(());
    let _ = pg_task.await;
    let _ = my_task.await;

    // ── C. Persistent Schema & Secondary Index Recovery Across Restart ──
    drop(db);
    let reopened = Arc::new(DatabaseContext::with_storage_dir(dir.path()).unwrap());
    let rec_res = reopened
        .query_virtual_catalog(
            "SELECT c.relname, a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) AS dt
             FROM pg_catalog.pg_class c
             JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
             WHERE c.relname = 'orders' AND a.attname = 'user_id'",
            tapisaurus_catalog::CatalogDialect::Postgres,
        )
        .unwrap()
        .unwrap();
    assert_eq!(rec_res.rows.len(), 1);
    assert_eq!(
        rec_res.rows[0][2].as_ref().and_then(|v| v.as_str()),
        Some("bigint")
    );
}
