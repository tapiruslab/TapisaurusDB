//! Reproducible Release Benchmark Suite for TapisaurusDB (`tapisaurus-query`, `tapisaurus-core`, `tapisaurus-vector`, `tapisaurus-graph`).
//!
//! Run with:
//! ```bash
//! cargo run --release -p tapisaurus-query --example subsystem_bench
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use tapisaurus_core::storage::engine::{StorageConfig, StorageEngine};
use tapisaurus_core::{
    DeterministicOp, DeterministicTxSpec, Document, SlottedBinaryCodec, TapirSyncBundle,
    TapirSyncHlc, TapirSyncRecord, Value, WasmSandboxModule, WasmTriggerAction, ZonePredicateOp,
};
use tapisaurus_graph::{Edge, GraphStore, Vertex};
use tapisaurus_query::{parse_query, DatabaseContext};
use tapisaurus_vector::{DistanceMetric, HnswConfig, HnswIndex};

fn percentile_us(sorted_ns: &[u128], pct: f64) -> f64 {
    if sorted_ns.is_empty() {
        return 0.0;
    }
    let idx = ((sorted_ns.len() as f64 - 1.0) * pct).round() as usize;
    sorted_ns[idx.min(sorted_ns.len() - 1)] as f64 / 1_000.0
}

fn deterministic_vec(seed: usize, dims: usize) -> Vec<f32> {
    let mut v = Vec::with_capacity(dims);
    let mut s = (seed as u64)
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1);
    let mut norm = 0.0f32;
    for _ in 0..dims {
        s = s
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let val = ((s >> 33) as f32) / (u32::MAX as f32) - 0.5;
        norm += val * val;
        v.push(val);
    }
    let inv = if norm > 0.0 { 1.0 / norm.sqrt() } else { 1.0 };
    for x in &mut v {
        *x *= inv;
    }
    v
}

fn main() {
    println!("==========================================================================================");
    println!("  TAPISAURUSDB REPRODUCIBLE SUBSYSTEM BENCHMARK (Release Mode)");
    println!("==========================================================================================\n");

    let n_rows = 2_000usize;

    // 1. Raw KV Batch Write & Point Lookup
    let dir_kv = tempfile::tempdir().unwrap();
    let engine = StorageEngine::open(StorageConfig {
        data_dir: dir_kv.path().to_path_buf(),
        sync_writes: true,
        enable_wal: true,
        ..Default::default()
    })
    .unwrap();

    let t0 = Instant::now();
    let mut kv_pairs = Vec::with_capacity(n_rows);
    for i in 0..n_rows {
        let k = format!("doc:users:{i}").into_bytes();
        let v = format!(r#"{{"_id":"{i}","name":"user_{i}","score":{}}}"#, i * 7).into_bytes();
        kv_pairs.push((k, v));
    }
    let kv_refs: Vec<(&[u8], &[u8])> = kv_pairs
        .iter()
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect();
    engine.put_batch(&kv_refs).unwrap();
    let kv_batch_dur = t0.elapsed();

    let mut kv_lat = Vec::with_capacity(n_rows);
    for i in 0..n_rows {
        let key = format!("doc:users:{}", (i * 73) % n_rows);
        let t = Instant::now();
        let val = engine.get(key.as_bytes()).unwrap();
        std::hint::black_box(val);
        kv_lat.push(t.elapsed().as_nanos());
    }
    kv_lat.sort_unstable();
    println!(
        "[1] LSM KV Batch Write ({n_rows} rows, fsync): {:>8.2} ms | {:>10.0} rows/s | Get p50: {:>5.2} µs (p99: {:>5.2} µs)",
        kv_batch_dur.as_secs_f64() * 1000.0,
        n_rows as f64 / kv_batch_dur.as_secs_f64(),
        percentile_us(&kv_lat, 0.50),
        percentile_us(&kv_lat, 0.99),
    );

    // 2. SQL Multi-Row Batch Insert & O(1) Primary-Key Point Lookup
    let dir_sql = tempfile::tempdir().unwrap();
    let sql_storage = Arc::new(
        StorageEngine::open(StorageConfig {
            data_dir: dir_sql.path().to_path_buf(),
            sync_writes: true,
            enable_wal: true,
            ..Default::default()
        })
        .unwrap(),
    );
    let ctx = DatabaseContext::with_storage(sql_storage);
    let mut values_clauses = Vec::with_capacity(n_rows);
    for i in 0..n_rows {
        values_clauses.push(format!("('{i}', 'user_{i}', {})", i * 7));
    }
    let batch_sql = format!(
        "INSERT INTO users (_id, name, score) VALUES {}",
        values_clauses.join(", ")
    );
    let t_sql = Instant::now();
    ctx.execute(parse_query(&batch_sql).unwrap()).unwrap();
    let sql_batch_dur = t_sql.elapsed();

    let mut sql_lat = Vec::with_capacity(n_rows);
    for i in 0..n_rows {
        let target = (i * 73) % n_rows;
        let q = format!("SELECT * FROM users WHERE _id = '{target}'");
        let t = Instant::now();
        let res = ctx.execute(parse_query(&q).unwrap()).unwrap();
        std::hint::black_box(res);
        sql_lat.push(t.elapsed().as_nanos());
    }
    sql_lat.sort_unstable();
    println!(
        "[2] SQL Batch INSERT ({n_rows} rows, fsync) : {:>8.2} ms | {:>10.0} rows/s | SQL p50: {:>5.2} µs (p99: {:>5.2} µs)",
        sql_batch_dur.as_secs_f64() * 1000.0,
        n_rows as f64 / sql_batch_dur.as_secs_f64(),
        percentile_us(&sql_lat, 0.50),
        percentile_us(&sql_lat, 0.99),
    );

    // 3. 8-Thread Concurrent SkipMap Writes
    let threads = 8usize;
    let writes_per_thread = 50usize;
    let total_mt = threads * writes_per_thread;
    let dir_mt = tempfile::tempdir().unwrap();
    let mt_storage = Arc::new(
        StorageEngine::open(StorageConfig {
            data_dir: dir_mt.path().to_path_buf(),
            sync_writes: false,
            enable_wal: true,
            ..Default::default()
        })
        .unwrap(),
    );
    let mt_ctx = Arc::new(DatabaseContext::with_storage(mt_storage));
    let t_mt = Instant::now();
    let mut handles = Vec::new();
    for tid in 0..threads {
        let c = mt_ctx.clone();
        handles.push(std::thread::spawn(move || {
            let col = c.get_or_create_collection("events");
            for j in 0..writes_per_thread {
                let mut doc = Document::new();
                doc.set("_id", format!("t{tid}_{j}"));
                doc.set("thread", tid as i64);
                col.insert(doc).unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let mt_dur: Duration = t_mt.elapsed();
    println!(
        "[3] 8-Thread Concurrent Writes ({total_mt} ops): {:>8.2} ms | {:>10.0} ops/s",
        mt_dur.as_secs_f64() * 1000.0,
        total_mt as f64 / mt_dur.as_secs_f64()
    );

    // 4. HNSW Vector Index & Search (2,000 x 128D)
    let n_vecs = 2_000usize;
    let dims = 128usize;
    let vectors: Vec<Vec<f32>> = (0..n_vecs)
        .map(|i| deterministic_vec(i + 1, dims))
        .collect();
    let queries: Vec<Vec<f32>> = (0..100)
        .map(|i| deterministic_vec(i + 50_000, dims))
        .collect();
    let t_hb = Instant::now();
    let mut hnsw = HnswIndex::new(HnswConfig {
        dimensions: dims,
        m: 16,
        ef_construction: 64,
        ef_search: 32,
        metric: DistanceMetric::Cosine,
        ..Default::default()
    });
    for (i, v) in vectors.iter().enumerate() {
        hnsw.insert(format!("v_{i}"), v.clone()).unwrap();
    }
    let hb_dur = t_hb.elapsed();
    let mut h_lat = Vec::with_capacity(queries.len());
    for q in &queries {
        let t = Instant::now();
        let res = hnsw.search(q, 10);
        std::hint::black_box(res);
        h_lat.push(t.elapsed().as_nanos());
    }
    h_lat.sort_unstable();
    println!(
        "[4] HNSW Vector (2,000 x 128D, Top-10)   : {:>8.2} ms build | Search p50: {:>6.2} µs (p99: {:>6.2} µs)",
        hb_dur.as_secs_f64() * 1000.0,
        percentile_us(&h_lat, 0.50),
        percentile_us(&h_lat, 0.99),
    );

    // 5. Graph BFS Shortest-Path (500 nodes, 1,000 edges)
    let mut g = GraphStore::new();
    for i in 0..500 {
        g.add_vertex(Vertex::new(format!("n_{i}"), "Account"));
    }
    for i in 0..500 {
        g.add_edge(Edge::new(
            format!("n_{i}"),
            format!("n_{}", (i + 1) % 500),
            "TRANSFER",
        ));
        g.add_edge(Edge::new(
            format!("n_{i}"),
            format!("n_{}", (i + 7) % 500),
            "TRANSFER",
        ));
    }
    let mut g_lat = Vec::with_capacity(500);
    for i in 0..500 {
        let src = format!("n_{}", (i * 13) % 500);
        let dst = format!("n_{}", (src.len() + i * 29 + 25) % 500);
        let t = Instant::now();
        let p = g.shortest_path(&src, &dst);
        std::hint::black_box(p);
        g_lat.push(t.elapsed().as_nanos());
    }
    g_lat.sort_unstable();
    println!(
        "[5] Graph Shortest-Path BFS (500 nodes)  : p50: {:>5.2} µs | p99: {:>5.2} µs",
        percentile_us(&g_lat, 0.50),
        percentile_us(&g_lat, 0.99),
    );

    // 6. HTAP Zone-Map Analytical Scan + Wasm Trigger + Calvin Sequencer
    let wasm_bytes = WasmSandboxModule::assemble_single_fn_module(
        "double_xp",
        1,
        &[0x20, 0x00, 0x42, 0x02, 0x7e],
    );
    ctx.wasm_registry()
        .register_module("xp_mod", &wasm_bytes)
        .unwrap();
    ctx.wasm_registry().register_trigger(
        "users",
        WasmTriggerAction::ComputeField {
            module_name: "xp_mod".into(),
            export_fn: "double_xp".into(),
            input_field: "score".into(),
            output_field: "bonus".into(),
        },
    );
    ctx.execute(parse_query("ANALYZE users").unwrap()).unwrap();
    let t_htap = Instant::now();
    let (sum, scanned, pruned) = ctx
        .query_htap_sum("users", "score", "score", ZonePredicateOp::Gt, 12000.0)
        .unwrap();
    let htap_us = t_htap.elapsed().as_nanos() as f64 / 1000.0;

    ctx.deterministic_sequencer()
        .submit(DeterministicTxSpec::new(
            "calvin_1",
            vec![
                DeterministicOp::Put {
                    key: "acct:1".into(),
                    value: 5000,
                },
                DeterministicOp::Put {
                    key: "acct:2".into(),
                    value: 2000,
                },
                DeterministicOp::Transfer {
                    from_key: "acct:1".into(),
                    to_key: "acct:2".into(),
                    amount: 1500,
                    min_balance: 0,
                },
            ],
        ));
    let receipts = ctx
        .commit_deterministic_epoch("wallets", "balance")
        .unwrap();
    println!(
        "[6] HTAP ZoneMap SUM & Calvin Epoch      : {:>6.2} µs (sum={sum:.0}, scanned={scanned}, pruned={pruned}, calvin_committed={})",
        htap_us,
        receipts[0].committed
    );

    // 7. TapirSync (TPSYNC01) Edge-to-Cloud Incremental Replication Bundle (500 records)
    let mut sync_records = Vec::with_capacity(500);
    let mut hlc = TapirSyncHlc::new("edge-kl-01", 1_700_000_000_000);
    for i in 0..500 {
        let stamp = hlc.tick(1_700_000_000_000 + i as u64);
        sync_records.push(TapirSyncRecord::TableRowUpsert {
            table: "edge_telemetry".into(),
            row_id: i.to_string(),
            columns: std::collections::BTreeMap::from([
                ("id".into(), serde_json::json!(i)),
                (
                    "voltage".into(),
                    serde_json::json!(3.7 + (i as f64) * 0.001),
                ),
            ]),
            hlc: stamp,
        });
    }
    let t_sync = Instant::now();
    let bundle = TapirSyncBundle::new("edge-kl-01", 0, 1, hlc, sync_records, None);
    let wire_bytes = bundle.to_bytes().unwrap();
    let decoded = TapirSyncBundle::from_bytes(&wire_bytes).unwrap();
    let ack = ctx.tapirsync_apply_bundle(&decoded).unwrap();
    let sync_dur = t_sync.elapsed();
    println!(
        "[7] TapirSync (TPSYNC01) 500 Edge Records: {:>8.2} ms | {:>10.0} recs/s | Wire: {} KB (applied={})",
        sync_dur.as_secs_f64() * 1000.0,
        500.0 / sync_dur.as_secs_f64(),
        wire_bytes.len() / 1024,
        ack.records_applied
    );

    // 8. TPBIN001 Slotted Binary Zero-Deserialization Field Projection vs Full JSON Decode
    let sample_doc = Document::with_id("doc_bench_1")
        .field("active", true)
        .field("bio", "A".repeat(2048))
        .field("city", "Kuala Lumpur")
        .field("department", "Core Storage Engineering")
        .field("embedding", Value::Vector(deterministic_vec(42, 128)))
        .field("score", 98_765i64)
        .field("status", "verified");
    let tpbin_bytes = SlottedBinaryCodec::encode(&sample_doc).unwrap();
    let json_bytes = serde_json::to_vec(&sample_doc).unwrap();

    let mut tpbin_lat = Vec::with_capacity(n_rows);
    let mut json_lat = Vec::with_capacity(n_rows);
    for _ in 0..n_rows {
        let t1 = Instant::now();
        let val = SlottedBinaryCodec::extract_field(&tpbin_bytes, "score").unwrap();
        std::hint::black_box(val);
        tpbin_lat.push(t1.elapsed().as_nanos());

        let t2 = Instant::now();
        let full: Document = serde_json::from_slice(&json_bytes).unwrap();
        std::hint::black_box(full.get("score"));
        json_lat.push(t2.elapsed().as_nanos());
    }
    tpbin_lat.sort_unstable();
    json_lat.sort_unstable();
    let tpbin_p50 = percentile_us(&tpbin_lat, 0.50);
    let json_p50 = percentile_us(&json_lat, 0.50);
    println!(
        "[8] TPBIN001 Zero-Copy Field Extract     : p50: {:>5.2} µs vs JSON Decode {:>5.2} µs ({:.1}x faster)",
        tpbin_p50,
        json_p50,
        json_p50 / tpbin_p50.max(0.01)
    );
}
