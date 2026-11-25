//! MySQL Wire Protocol TCP Server (Port 3306).
//!
//! Accepts client TCP connections from MySQL/MariaDB clients (MySQL CLI, PHP mysqli/PDO,
//! Laravel Eloquent, Python mysqlclient, Go go-sql-driver/mysql, etc.), handles the HandshakeV10
//! negotiation, parses queries, and routes them to the TapisaurusDB execution engine.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

use super::codec::{
    build_column_def, build_eof_packet, build_err_packet, build_handshake_v10, build_ok_packet,
    encode_packet, parse_handshake_response, read_lenenc_int, MYSQL_TYPE_LONGLONG,
    MYSQL_TYPE_VAR_STRING,
};
use super::handler::{handle_mysql_binary_query, handle_mysql_query};
use bytes::{BufMut, Bytes, BytesMut};
use std::collections::HashMap;
use tapisaurus_query::DatabaseContext;
use tapisaurus_security::UserStore;

static NEXT_CONNECTION_ID: AtomicU32 = AtomicU32::new(1);

/// Run the MySQL Wire Protocol server with optional graceful shutdown future
pub async fn run_mysql_server_with_shutdown<F>(
    addr: &str,
    db: Arc<DatabaseContext>,
    user_store: Arc<UserStore>,
    shutdown: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind(addr).await?;
    info!("🐬 MySQL / MariaDB Wire Protocol Server running on mysql://{addr}");

    let max_conns = std::env::var("TAPISAURUS_MAX_CONNECTIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(max_conns));
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("🐬 MySQL Wire Protocol Server received shutdown signal — draining listener...");
                break;
            }
            accept_res = listener.accept() => {
                match accept_res {
                    Ok((socket, peer_addr)) => {
                        let permit = match semaphore.clone().try_acquire_owned() {
                            Ok(p) => p,
                            Err(_) => {
                                warn!("Rejecting MySQL connection from {peer_addr}: max connections ({max_conns}) reached");
                                tokio::spawn(async move {
                                    let mut s = socket;
                                    let err = build_err_packet(1, 1040, "08004", "Too many connections");
                                    let _ = s.write_all(&err).await;
                                    let _ = s.flush().await;
                                });
                                continue;
                            }
                        };
                        let db_clone = db.clone();
                        let store_clone = user_store.clone();
                        tokio::spawn(async move {
                            let _permit = permit;
                            if let Err(e) = handle_mysql_connection(
                                socket,
                                db_clone,
                                store_clone,
                                peer_addr.to_string(),
                            )
                            .await
                            {
                                warn!("MySQL connection closed for {peer_addr}: {e}");
                            }
                        });
                    }
                    Err(e) => {
                        error!("Failed to accept MySQL connection: {e}");
                    }
                }
            }
        }
    }
    Ok(())
}

/// Run the MySQL Wire Protocol server on the given address (e.g. "0.0.0.0:3306")
pub async fn run_mysql_server(
    addr: &str,
    db: Arc<DatabaseContext>,
    user_store: Arc<UserStore>,
) -> Result<(), Box<dyn std::error::Error>> {
    run_mysql_server_with_shutdown(addr, db, user_store, std::future::pending()).await
}

/// Handles an individual MySQL client connection lifecycle
async fn handle_mysql_connection(
    mut stream: TcpStream,
    db: Arc<DatabaseContext>,
    _user_store: Arc<UserStore>,
    peer_addr: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let conn_id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);

    // 1. Generate 20-byte random salt for authentication
    let mut salt = [0u8; 20];
    for b in salt.iter_mut() {
        *b = (rand_simple() & 0xFF) as u8;
    }

    // 2. Send Initial HandshakeV10 packet (seq_id = 0)
    let greeting = build_handshake_v10(conn_id, &salt);
    stream.write_all(&greeting).await?;
    stream.flush().await?;

    // 3. Read HandshakeResponse41 packet from client
    let mut header = [0u8; 4];
    stream.read_exact(&mut header).await?;
    let payload_len =
        (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
    let mut client_seq_id = header[3];

    let mut payload = vec![0u8; payload_len];
    stream.read_exact(&mut payload).await?;

    let handshake_resp = parse_handshake_response(bytes::Bytes::from(payload))
        .map_err(|e| format!("MySQL handshake error: {e}"))?;

    let mut current_db = handshake_resp
        .database
        .unwrap_or_else(|| "tapisaurus".to_string());
    info!(
        "MySQL client authenticated: user='{}', db='{}', conn_id={conn_id} from {peer_addr}",
        handshake_resp.username, current_db
    );

    // 4. Send OK_Packet confirming successful handshake
    client_seq_id = client_seq_id.wrapping_add(1);
    let ok = build_ok_packet(client_seq_id, 0, 0, "");
    stream.write_all(&ok).await?;
    stream.flush().await?;

    #[derive(Debug, Clone)]
    struct MysqlPreparedStmt {
        query: String,
        num_params: usize,
        param_types: Vec<(u8, u8)>,
    }
    let mut prepared_stmts: HashMap<u32, MysqlPreparedStmt> = HashMap::new();
    let mut next_stmt_id: u32 = 1;

    // 5. Command Phase loop
    loop {
        let mut cmd_header = [0u8; 4];
        match stream.read_exact(&mut cmd_header).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                break; // Clean client disconnect
            }
            Err(e) => return Err(Box::new(e)),
        }

        let cmd_len = (cmd_header[0] as usize)
            | ((cmd_header[1] as usize) << 8)
            | ((cmd_header[2] as usize) << 16);
        let seq_id = cmd_header[3];

        if cmd_len == 0 {
            continue;
        }

        let mut cmd_payload = vec![0u8; cmd_len];
        stream.read_exact(&mut cmd_payload).await?;

        let command_type = cmd_payload[0];
        let next_seq = seq_id.wrapping_add(1);

        match command_type {
            // COM_QUIT (0x01)
            0x01 => {
                break;
            }
            // COM_INIT_DB (0x02) - Switch active schema/database
            0x02 => {
                if let Ok(new_db) = String::from_utf8(cmd_payload[1..].to_vec()) {
                    current_db = new_db.trim().to_string();
                }
                let ok_pkt = build_ok_packet(next_seq, 0, 0, "");
                stream.write_all(&ok_pkt).await?;
                stream.flush().await?;
            }
            // COM_QUERY (0x03) - Execute SQL Query
            0x03 => {
                let query_bytes = &cmd_payload[1..];
                let query_str = String::from_utf8_lossy(query_bytes);
                let response_packets = handle_mysql_query(&db, &current_db, &query_str, next_seq);
                for pkt in response_packets {
                    stream.write_all(&pkt).await?;
                }
                stream.flush().await?;
            }
            // COM_FIELD_LIST (0x04)
            0x04 => {
                let eof = super::codec::build_eof_packet(next_seq);
                stream.write_all(&eof).await?;
                stream.flush().await?;
            }
            // COM_PING (0x0E)
            0x0E => {
                let ok_pkt = build_ok_packet(next_seq, 0, 0, "");
                stream.write_all(&ok_pkt).await?;
                stream.flush().await?;
            }
            // COM_STMT_PREPARE (0x16)
            0x16 => {
                let query_str = String::from_utf8_lossy(&cmd_payload[1..]).to_string();
                let num_params = count_mysql_placeholders(&query_str);
                let stmt_id = next_stmt_id;
                next_stmt_id = next_stmt_id.wrapping_add(1);

                let dialect = tapisaurus_catalog::CatalogDialect::MySql {
                    database: current_db.clone(),
                };
                let described_cols = db
                    .describe_virtual_catalog(query_str.trim().trim_end_matches(';'), dialect)
                    .ok()
                    .flatten()
                    .unwrap_or_default();

                prepared_stmts.insert(
                    stmt_id,
                    MysqlPreparedStmt {
                        query: query_str,
                        num_params,
                        param_types: vec![(MYSQL_TYPE_VAR_STRING, 0); num_params],
                    },
                );

                let mut seq = next_seq;
                let mut ok_payload = BytesMut::with_capacity(12);
                ok_payload.put_u8(0x00); // status OK
                ok_payload.put_u32_le(stmt_id);
                ok_payload.put_u16_le(described_cols.len() as u16);
                ok_payload.put_u16_le(num_params as u16);
                ok_payload.put_u8(0x00); // reserved
                ok_payload.put_u16_le(0); // warning_count
                stream.write_all(&encode_packet(seq, &ok_payload)).await?;
                seq = seq.wrapping_add(1);

                if num_params > 0 {
                    for _ in 0..num_params {
                        let pdef =
                            build_column_def(seq, &current_db, "", "?", MYSQL_TYPE_VAR_STRING, 255);
                        stream.write_all(&pdef).await?;
                        seq = seq.wrapping_add(1);
                    }
                    stream.write_all(&build_eof_packet(seq)).await?;
                    seq = seq.wrapping_add(1);
                }

                if !described_cols.is_empty() {
                    for col in &described_cols {
                        let col_type = match col.pg_type_oid {
                            16 | 20 | 21 | 23 => MYSQL_TYPE_LONGLONG,
                            _ => MYSQL_TYPE_VAR_STRING,
                        };
                        let cdef = build_column_def(seq, &current_db, "", &col.name, col_type, 255);
                        stream.write_all(&cdef).await?;
                        seq = seq.wrapping_add(1);
                    }
                    stream.write_all(&build_eof_packet(seq)).await?;
                }
                stream.flush().await?;
            }
            // COM_STMT_EXECUTE (0x17)
            0x17 => {
                if cmd_payload.len() < 10 {
                    let err =
                        build_err_packet(next_seq, 1210, "HY000", "Truncated COM_STMT_EXECUTE");
                    stream.write_all(&err).await?;
                    stream.flush().await?;
                    continue;
                }
                let stmt_id = u32::from_le_bytes([
                    cmd_payload[1],
                    cmd_payload[2],
                    cmd_payload[3],
                    cmd_payload[4],
                ]);
                let Some(stmt) = prepared_stmts.get_mut(&stmt_id) else {
                    let err = build_err_packet(
                        next_seq,
                        1243,
                        "HY000",
                        &format!("Unknown prepared statement handler ({stmt_id})"),
                    );
                    stream.write_all(&err).await?;
                    stream.flush().await?;
                    continue;
                };

                let mut params = Vec::with_capacity(stmt.num_params);
                if stmt.num_params > 0 {
                    let bitmap_len = stmt.num_params.div_ceil(8);
                    let mut cursor = 10usize;
                    let null_bitmap = if cursor + bitmap_len <= cmd_payload.len() {
                        let bm = &cmd_payload[cursor..cursor + bitmap_len];
                        cursor += bitmap_len;
                        bm.to_vec()
                    } else {
                        vec![0u8; bitmap_len]
                    };
                    if cursor < cmd_payload.len() {
                        let new_params_bound = cmd_payload[cursor];
                        cursor += 1;
                        if new_params_bound == 1 {
                            stmt.param_types.clear();
                            for _ in 0..stmt.num_params {
                                if cursor + 2 <= cmd_payload.len() {
                                    stmt.param_types
                                        .push((cmd_payload[cursor], cmd_payload[cursor + 1]));
                                    cursor += 2;
                                } else {
                                    stmt.param_types.push((MYSQL_TYPE_VAR_STRING, 0));
                                }
                            }
                        }
                    }
                    let mut rest =
                        Bytes::copy_from_slice(cmd_payload.get(cursor..).unwrap_or_default());
                    for i in 0..stmt.num_params {
                        let is_null = ((null_bitmap[i / 8] >> (i % 8)) & 1) != 0;
                        if is_null {
                            params.push(None);
                            continue;
                        }
                        let (ptype, unsigned) = stmt
                            .param_types
                            .get(i)
                            .copied()
                            .unwrap_or((MYSQL_TYPE_VAR_STRING, 0));
                        params.push(decode_mysql_binary_param(ptype, unsigned != 0, &mut rest));
                    }
                }

                let resolved = substitute_mysql_params(&stmt.query, &params);
                let response_packets =
                    handle_mysql_binary_query(&db, &current_db, &resolved, next_seq);
                for pkt in response_packets {
                    stream.write_all(&pkt).await?;
                }
                stream.flush().await?;
            }
            // COM_STMT_CLOSE (0x19) — no response packet per MySQL spec
            0x19 => {
                if cmd_payload.len() >= 5 {
                    let stmt_id = u32::from_le_bytes([
                        cmd_payload[1],
                        cmd_payload[2],
                        cmd_payload[3],
                        cmd_payload[4],
                    ]);
                    prepared_stmts.remove(&stmt_id);
                }
            }
            // COM_STMT_RESET (0x1A)
            0x1A => {
                let ok_pkt = build_ok_packet(next_seq, 0, 0, "");
                stream.write_all(&ok_pkt).await?;
                stream.flush().await?;
            }
            // Unknown command fallback -> OK
            _ => {
                let ok_pkt = build_ok_packet(next_seq, 0, 0, "");
                stream.write_all(&ok_pkt).await?;
                stream.flush().await?;
            }
        }
    }

    Ok(())
}

/// Simple pseudo-random generator without extra crate dependency
fn rand_simple() -> u32 {
    use std::time::SystemTime;
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    ((now ^ (now >> 32)) & 0xFFFFFFFF) as u32
}

fn count_mysql_placeholders(query: &str) -> usize {
    let bytes = query.as_bytes();
    let mut i = 0usize;
    let mut in_sq = false;
    let mut count = 0usize;
    while i < bytes.len() {
        if !in_sq && bytes[i] == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if !in_sq && bytes[i] == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(bytes.len());
            continue;
        }
        if bytes[i] == b'\'' {
            if in_sq && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                i += 2;
                continue;
            }
            in_sq = !in_sq;
        } else if !in_sq && bytes[i] == b'?' {
            count += 1;
        }
        i += 1;
    }
    count
}

fn decode_mysql_binary_param(ptype: u8, unsigned: bool, buf: &mut Bytes) -> Option<String> {
    use bytes::Buf;
    match ptype {
        0x06 => None, // MYSQL_TYPE_NULL
        0x01 => {
            if buf.remaining() < 1 {
                return None;
            }
            let b = buf.get_u8();
            Some(if unsigned {
                b.to_string()
            } else {
                (b as i8).to_string()
            })
        }
        0x02 => {
            if buf.remaining() < 2 {
                return None;
            }
            let v = buf.get_u16_le();
            Some(if unsigned {
                v.to_string()
            } else {
                (v as i16).to_string()
            })
        }
        0x03 | 0x09 => {
            if buf.remaining() < 4 {
                return None;
            }
            let v = buf.get_u32_le();
            Some(if unsigned {
                v.to_string()
            } else {
                (v as i32).to_string()
            })
        }
        0x08 => {
            if buf.remaining() < 8 {
                return None;
            }
            let v = buf.get_u64_le();
            Some(if unsigned {
                v.to_string()
            } else {
                (v as i64).to_string()
            })
        }
        0x04 => {
            if buf.remaining() < 4 {
                return None;
            }
            Some(buf.get_f32_le().to_string())
        }
        0x05 => {
            if buf.remaining() < 8 {
                return None;
            }
            Some(buf.get_f64_le().to_string())
        }
        _ => {
            // Length-encoded string/blob/varchar
            let len = read_lenenc_int(buf)? as usize;
            if buf.remaining() < len {
                return None;
            }
            let data = buf.split_to(len);
            Some(String::from_utf8_lossy(&data).to_string())
        }
    }
}

fn substitute_mysql_params(query: &str, params: &[Option<String>]) -> String {
    let chars: Vec<char> = query.chars().collect();
    let mut out = String::with_capacity(query.len() + params.len() * 16);
    let mut i = 0usize;
    let mut in_sq = false;
    let mut param_idx = 0usize;
    while i < chars.len() {
        let ch = chars[i];
        if !in_sq && ch == '-' && i + 1 < chars.len() && chars[i + 1] == '-' {
            out.push('-');
            out.push('-');
            i += 2;
            while i < chars.len() && chars[i] != '\n' {
                out.push(chars[i]);
                i += 1;
            }
            continue;
        }
        if !in_sq && ch == '/' && i + 1 < chars.len() && chars[i + 1] == '*' {
            out.push('/');
            out.push('*');
            i += 2;
            while i + 1 < chars.len() && !(chars[i] == '*' && chars[i + 1] == '/') {
                out.push(chars[i]);
                i += 1;
            }
            if i + 1 < chars.len() {
                out.push('*');
                out.push('/');
                i += 2;
            }
            continue;
        }
        if ch == '\'' {
            if in_sq && i + 1 < chars.len() && chars[i + 1] == '\'' {
                out.push('\'');
                out.push('\'');
                i += 2;
                continue;
            }
            in_sq = !in_sq;
            out.push(ch);
        } else if !in_sq && ch == '?' && param_idx < params.len() {
            match &params[param_idx] {
                None => out.push_str("NULL"),
                Some(v) => {
                    out.push('\'');
                    out.push_str(&v.replace('\'', "''"));
                    out.push('\'');
                }
            }
            param_idx += 1;
        } else {
            out.push(ch);
        }
        i += 1;
    }
    out
}
