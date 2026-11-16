# 📖 TapisaurusDB Universal Commands & Syntax Reference Manual

This is the definitive, exhaustive command and syntax manual for **TapisaurusDB**. It covers all **5 query dialects**, the **CLI toolchain**, the **HTTP REST endpoints**, the **gRPC Protocol Buffers RPCs**, and the **Embedded In-Process Engine**.

---

## 📑 Table of Contents

1. [CLI Toolchain Commands (`tapisaurus`)](#1-cli-toolchain-commands)
2. [Relational SQL Syntax (Port 5432 / 3306 / REST / Shell)](#2-relational-sql-syntax)
3. [openCypher Knowledge Graph Syntax (Port 5432 / 3306 / REST)](#3-opencypher-knowledge-graph-syntax)
4. [TapisaurusQL Native Multi-Model Syntax (REST / gRPC / Shell)](#4-tapisaurusql-native-multi-model-syntax)
5. [MongoDB Wire & Shell Syntax (Port 27017 / REST)](#5-mongodb-wire--shell-syntax)
6. [HTTP REST API Endpoints Reference (Port 27018)](#6-http-rest-api-endpoints-reference)
7. [gRPC Protocol Buffers RPC Reference (Port 50051)](#7-grpc-protocol-buffers-rpc-reference)
8. [Python SDK & LangGraph Native Saver](#8-python-sdk--langgraph-native-saver)
9. [Embedded In-Process Rust Engine (`tapisaurus-core`)](#9-embedded-in-process-rust-engine)
10. [Developer Tools & Observability Assets](#10-developer-tools--observability-assets)

---

## 1. CLI Toolchain Commands

All commands can be run via the compiled binary `tapisaurus` (or `./target/release/tapisaurus`):

```bash
tapisaurus [SUBCOMMAND] [OPTIONS]
```

### Core Subcommands

| Command | Description | Key Options / Flags |
| :--- | :--- | :--- |
| `tapisaurus serve` | Starts the 5-way universal database daemon | `--host 0.0.0.0`, `--http-port 27018`, `--grpc-port 50051`, `--mongo-port 27017`, `--pg-port 5432`, `--mysql-port 3306`, `--data-dir ./tapisaurus_data` |
| `tapisaurus doctor` | Runs preflight system diagnostics across 5 gateways, storage, WAL & security | `--host 127.0.0.1`, `--http-port 27018`, `--wire-port 27017`, `--pg-port 5432`, `--mysql-port 3306`, `--grpc-port 50051`, `--data-dir ./tapisaurus_data` |
| `tapisaurus seed` | Seeds multi-model datasets for instant evaluation (Relational, Vector, Graph) | `--dataset ecommerce\|agent-memory\|social-graph`, `--data-dir ./tapisaurus_data` |
| `tapisaurus shell` | Starts the interactive multi-dialect REPL | `--endpoint http://localhost:27018`, `--token <jwt>`, `--username <user>`, `--password <pass>` |
| `tapisaurus query "<query>"` | Runs a one-shot query from terminal | `--endpoint http://localhost:27018`, `--format json\|table\|csv` |
| `tapisaurus backup` | Creates a non-blocking snapshot archive | `--output ./backups/snap.enc.json`, `--encrypt`, `--key <aes_key>` |
| `tapisaurus restore` | Restores snapshot & replays WAL logs | `--input ./backups/snap.enc.json`, `--replay-wal`, `--target-lsn <lsn>` |
| `tapisaurus benchmark` | Executes high-concurrency throughput stress test | `--threads 16`, `--operations 100000`, `--workload read_heavy\|write_heavy\|mixed` |
| `tapisaurus ycsb` | Runs official standard YCSB benchmarks | `--workload A\|B\|C\|D\|E\|F`, `--recordcount 100000`, `--operationcount 500000` |
| `tapisaurus cluster join` | Joins node to a distributed Raft cluster | `--peer 10.0.0.1:27018`, `--node-id node_02`, `--raft-port 7000` |
| `tapisaurus cluster status` | Displays cluster topology and shard heatmaps | `--endpoint http://localhost:27018` |
| `tapisaurus version` | Prints binary version, git commit, and build info | None |
| `tapisaurus help` | Prints general or subcommand help screen | `tapisaurus help [subcommand]` |

### Practical CLI Examples

```bash
# Run comprehensive health preflight & port audit
tapisaurus doctor

# Seed rich multi-model ecommerce dataset (products, orders, 64-dim HNSW, knowledge graph)
tapisaurus seed --dataset ecommerce

# Seed autonomous AI agent memory tier (episodic, semantic, working, 64-dim embeddings)
tapisaurus seed --dataset agent-memory

# Seed social knowledge graph (profiles, posts, followers relationships)
tapisaurus seed --dataset social-graph

# Start full multi-gateway daemon in production mode
tapisaurus serve --pg-port 5432 --mysql-port 3306 --mongo-port 27017 --http-port 27018 --grpc-port 50051

# Execute a one-shot SQL query and output as formatted table
tapisaurus query "SELECT * FROM users WHERE score > 9000 LIMIT 5;" --format table

# Generate an encrypted disaster recovery backup
tapisaurus backup --output /var/backups/tapisaurus_20260906.enc.json --encrypt

# Point-In-Time Disaster Recovery to exact microsecond LSN
tapisaurus restore --input /var/backups/tapisaurus_20260906.enc.json --replay-wal --target-lsn 849204820
```

---

## 2. Relational SQL Syntax

TapisaurusDB parses standard ANSI SQL with Cost-Based Optimization (`tapisaurus-query`), available over **Port 5432 (Postgres)**, **Port 3306 (MySQL)**, **Port 27018 (`/v1/query`)**, and **CLI shell**.

### A. Data Definition Language (DDL)

```sql
-- Create a new table / collection
CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    role TEXT,
    score INT,
    created_at TIMESTAMP
);

-- Create a collection (TapisaurusQL / SQL)
CREATE COLLECTION users;

-- Drop a table or collection (purges both RAM and persistent disk storage)
DROP TABLE IF EXISTS users;
DROP COLLECTION users;

-- Create Secondary B-Tree Index for O(log N) lookup
CREATE INDEX idx_users_score ON users (score);

-- Create Unique Constraint Index
CREATE UNIQUE INDEX idx_users_email ON users (email);

-- Drop Secondary Index
DROP INDEX idx_users_score ON users;

-- Collect distribution statistics for the Cost-Based Optimizer
ANALYZE users;
```

### B. Data Manipulation Language (DML)

```sql
-- Insert Row using standard SQL syntax
INSERT INTO users (name, email, role, score) 
VALUES ('Ahmad Faiz', 'faiz@tapirusdb.com', 'Architect', 9900);

-- Insert Row using native JSON document syntax
INSERT INTO users {"name": "Alice Vance", "role": "Engineer", "score": 9200};

-- Select all columns with conditional filtering
SELECT * FROM users 
WHERE score >= 9000 AND role = 'Architect';

-- Select with sorting and pagination
SELECT name, role, score FROM users 
WHERE role <> 'Suspended' AND role != 'Banned'
ORDER BY score DESC 
LIMIT 20 OFFSET 0;

-- Range Evaluation (BETWEEN & NOT BETWEEN)
SELECT * FROM users WHERE score BETWEEN 8000 AND 9500;
SELECT * FROM users WHERE score NOT BETWEEN 1000 AND 4999;

-- Set Membership (IN & NOT IN)
SELECT * FROM users WHERE role IN ('Architect', 'Principal', 'Fellow');
SELECT * FROM users WHERE role NOT IN ('Guest', 'Suspended', 'Trial');

-- Nullity Testing (IS NULL & IS NOT NULL)
SELECT * FROM users WHERE email IS NOT NULL;
SELECT * FROM users WHERE deleted_at IS NULL;

-- Compound Boolean Logic with Parentheses & OR
SELECT * FROM users 
WHERE (role = 'Architect' OR role = 'Principal') AND score >= 9000;

-- String Matching Operators
SELECT * FROM users WHERE name LIKE '%Faiz%';
SELECT * FROM users WHERE email ENDS_WITH '@tapirusdb.com';
SELECT * FROM users WHERE role STARTS_WITH 'Lead';

-- Update Records (Arithmetic & Assignment)
UPDATE users 
SET role = 'Distinguished Architect', score = score + 500 
WHERE email = 'faiz@tapirusdb.com';

-- Delete Records
DELETE FROM users 
WHERE score < 5000 AND role = 'Trial';

-- Record Counting & Vectorized Columnar Analytics
SELECT COUNT(*) FROM users WHERE score >= 8000;
SELECT COUNT(*), AVG(score), MIN(score), MAX(score) FROM users;
```

### C. Multi-Table Relational Hash Joins

```sql
-- Relational INNER JOIN (In-Memory Vectorized Hash Join)
SELECT orders.id, customers.name, orders.amount 
FROM orders 
INNER JOIN customers ON orders.customer_id = customers.id
WHERE orders.amount > 500.00;

-- Relational LEFT OUTER JOIN
SELECT customers.name, orders.id, orders.status 
FROM customers 
LEFT JOIN orders ON customers.id = orders.customer_id;
```

### D. ACID Transactions & Snapshot Isolation

```sql
-- Begin multi-statement transaction
BEGIN TRANSACTION;

-- Atomic stock decrement with check constraint
UPDATE inventory 
SET qty = qty - 1 
WHERE item_id = 'A14' AND qty > 0;

-- Record customer purchase
INSERT INTO orders (item_id, customer_id, status) 
VALUES ('A14', 'cust_882', 'CONFIRMED');

-- Commit mutations atomically
COMMIT;

-- Or abort on failure:
-- ROLLBACK;
```

### E. Cost-Based Query Optimizer (`EXPLAIN`)

```sql
-- Generate hierarchical execution plan
EXPLAIN SELECT * FROM users WHERE score > 9000;

-- Execute query and measure actual shard execution timings
EXPLAIN ANALYZE SELECT orders.id, customers.name 
FROM orders 
INNER JOIN customers ON orders.customer_id = customers.id;

-- Output verbose cost scoring, selectivity percentages, and index rationales
EXPLAIN VERBOSE SELECT * FROM users WHERE email = 'faiz@tapirusdb.com';
```

---

## 3. openCypher Knowledge Graph Syntax

TapisaurusDB provides a high-performance in-memory and disk-backed **Directional Graph Engine** with native openCypher support.

### A. Graph Edge Creation

```cypher
-- 1. openCypher pattern syntax:
CREATE (a:Person {id: 'p1', name: 'Alice'})-[:KNOWS {weight: 1.0}]->(b:Person {id: 'p2', name: 'Bob'});
CREATE (b:Person {id: 'p2'})-[:WORKS_AT {since: 2024}]->(c:Company {id: 'c1', name: 'ICT House'});

-- 2. Direct DDL edge creation:
CREATE EDGE FROM 'p1' TO 'p2' VIA 'KNOWS' WEIGHT 1.0;
CREATE EDGE FROM 'p2' TO 'c1' VIA 'WORKS_AT';
```

### B. Graph Pattern Matching & Multi-Hop Traversal

```cypher
-- 1-Hop Neighbor Match
MATCH (a:Person)-[:KNOWS]->(b:Person) 
WHERE a.id = 'p1' 
RETURN b;

-- Variable-Length Path Traversal (1 to 3 hops)
MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) 
WHERE a.id = 'p1' 
RETURN b;

-- Filter by target node properties
MATCH (a:Person)-[:WORKS_AT]->(c:Company) 
WHERE c.name = 'ICT House' 
RETURN a;
```

### C. Tri-Hybrid GraphRAG (Graph Traversal + HNSW Vector Similarity)

```cypher
-- Match 2 hops out, then rank destination nodes by HNSW vector cosine similarity
MATCH (a:Entity)-[:relates_to*1..2]->(b:Entity) 
WHERE a.id = 'doc_root' 
VECTOR NEAR [0.12, 0.45, 0.88, 0.05] TOP 5 
RETURN b;
```

### D. Graph Edge Deletion

```cypher
-- Delete specific directional relationship
DELETE EDGE FROM 'p1' TO 'p2' VIA 'KNOWS';
```

---

## 4. TapisaurusQL Native Multi-Model Syntax

TapisaurusQL is TapisaurusDB's proprietary unified query syntax that allows executing relational queries, HNSW vector search, and graph traversal within a single statement.

### A. Search & Traversal Clauses

```sql
-- 1. Standard Document Finding
FIND users 
WHERE score > 8000 
SORT BY score DESC 
LIMIT 10 SKIP 0;

-- 2. Dense Vector ANN Similarity Search (< 1ms)
FIND articles 
VECTOR NEAR [0.95, 0.88, 0.12, 0.04] TOP 5;

-- 3. Vector Search using explicit index target
FIND embeddings 
VECTOR NEAR [0.11, 0.42, 0.77, 0.99] TOP 10 
USING INDEX idx_article_embeddings;

-- 4. Bounded Breadth-First Graph Traversal
FIND accounts 
TRAVERSE FROM "acc_suspicious_9" DEPTH 4 VIA "transferred_to";

-- 5. Hybrid Graph Traversal + Dense Vector Ranking (GraphRAG)
FIND agent_memories 
TRAVERSE FROM "agent_alpha" DEPTH 2 VIA "interacted_with" 
VECTOR [0.045, 0.812, 0.334] TOP 5;

-- 6. Combine WHERE Filter with Graph Traversal
FIND documents 
WHERE verified = true 
TRAVERSE FROM "root_doc" DEPTH 3 VIA "references";
```

---

## 5. MongoDB Wire & Shell Syntax

TapisaurusDB speaks native MongoDB wire protocol on **Port 27017**. All standard MongoDB commands, Compass queries, PyMongo, and Mongoose operations work out of the box.

### A. Inserting Documents

```javascript
// Insert single document with automatic 3600-second TTL expiration
db.users.insertOne({
    name: "Cyber Specialist",
    role: "Security",
    score: 9200,
    _ttl: 3600
});

// Bulk insert multiple documents
db.users.insertMany([
    { name: "Alice", role: "DevOps", active: true },
    { name: "Bob", role: "Frontend", active: false }
]);
```

### B. Querying Documents

```javascript
// Basic find with equality filter
db.users.find({ role: "Security" });

// Advanced comparison operators ($gt, $gte, $lt, $lte, $ne, $in)
db.users.find({ score: { $gte: 9000, $lt: 10000 } });

// Projection, Sorting, and Pagination
db.users.find({ role: "Security" }, { name: 1, email: 1, _id: 0 })
    .sort({ score: -1 })
    .limit(10)
    .skip(0);

// Find single document
db.users.findOne({ name: "Cyber Specialist" });

// Document count
db.users.count({ role: "Security" });
```

### C. Updating Documents

```javascript
// Atomic update with $set and $inc
db.users.updateOne(
    { name: "Cyber Specialist" },
    { $set: { status: "Active" }, $inc: { score: 100 } }
);

// Bulk update multiple matching documents
db.users.updateMany(
    { status: "Pending" },
    { $set: { status: "Verified" } }
);
```

### D. Deleting Documents

```javascript
// Delete single matching document
db.users.deleteOne({ name: "Cyber Specialist" });

// Delete multiple matching documents
db.users.deleteMany({ status: "Inactive" });
```

### E. Index & Collection Management

```javascript
// Create secondary index
db.users.createIndex({ score: 1 }, { unique: false });

// Create unique constraint index
db.users.createIndex({ email: 1 }, { unique: true });

// Drop index
db.users.dropIndex("score_1");

// Fetch collection telemetry statistics
db.users.stats();

// Drop entire collection
db.users.drop();
```

### F. MongoDB Graph Traversal ($traverse)

```javascript
// Execute graph traversal via MongoDB dialect
db.users.find({
    $traverse: {
        from: "usr_10",
        depth: 2,
        via: "friend_of"
    }
});
```

---

## 6. HTTP REST API Endpoints Reference

Base URL: `http://localhost:27018`

### A. Universal Query Execution

```http
POST /v1/query
Content-Type: application/json
Authorization: Bearer <JWT_TOKEN>

{
  "query": "SELECT * FROM users WHERE score > 8000 LIMIT 5;"
}
```

*Response:*
```json
{
  "success": true,
  "data": [
    { "id": "u1", "name": "Ahmad Faiz", "role": "Architect", "score": 9900 }
  ],
  "execution_time_us": 412
}
```

### B. Authentication & RBAC

```http
POST /v1/auth/login
Content-Type: application/json

{
  "username": "admin",
  "password": "tapisaurus-admin-2026"
}
```

```http
GET /v1/auth/whoami
Authorization: Bearer <JWT_TOKEN>
```

### C. Health & Metrics Probes

| Route | Method | Description |
| :--- | :---: | :--- |
| `/v1/health` | `GET` | Comprehensive system health and subsystem status |
| `/v1/health/liveness` | `GET` | Kubernetes liveness probe (returns 200 OK) |
| `/v1/health/readiness` | `GET` | Kubernetes readiness probe (verifies WAL & cluster quorum) |
| `/v1/metrics` | `GET` | Prometheus-compatible telemetry metrics |

### D. Collections & Document Endpoints

```http
# List all collections
GET /v1/collections

# Create collection
POST /v1/collections
{ "name": "customers" }

# Drop collection
DELETE /v1/collections/customers

# Insert Document
POST /v1/collections/customers/insert
{ "name": "Global Logistics Corp", "tier": "Enterprise" }

# Bulk Document Ingestion (High-Throughput)
POST /v1/collections/customers/import
{
  "documents": [
    { "name": "Client A", "tier": "Pro" },
    { "name": "Client B", "tier": "Enterprise" }
  ]
}

# Fetch Document by ID
GET /v1/collections/customers/documents/cust_101

# Delete Document by ID
DELETE /v1/collections/customers/documents/cust_101

# Okapi BM25 Full-Text Search with Fuzzy Typo-Tolerance
POST /v1/collections/customers/search
{
  "query": "logistics",
  "fuzzy": true,
  "top_k": 10
}

# Create Secondary B-Tree Index
POST /v1/collections/customers/indexes
{
  "field": "tier",
  "unique": false
}

# Collection Statistics
GET /v1/collections/customers/stats
```

### E. Change Data Capture (WebSocket Streaming)

```http
WS /v1/stream
```

Connect a standard WebSocket client to receive sub-millisecond mutation events:
```json
{
  "operation": "INSERT",
  "collection": "orders",
  "document_id": "ord_99",
  "timestamp": 1788339200000,
  "data": { "amount": 450.0, "status": "CONFIRMED" }
}
```

### F. Backup & Disaster Recovery

```http
# Create non-blocking snapshot
POST /v1/backup
{
  "output_path": "./backups/tapisaurus_snapshot.json",
  "encrypt": false
}

# Restore from snapshot
POST /v1/restore
{
  "input_path": "./backups/tapisaurus_snapshot.json"
}
```

### G. Multi-Region Geo-Replication Mesh

```http
GET /v1/cluster/regions
POST /v1/cluster/regions
{
  "region_id": "eu-central-1",
  "endpoint": "http://eu.tapirusdb.com:27018"
}

POST /v1/cluster/geo-sync
{
  "deltas": [ ... ]
}
```

---

## 7. gRPC Protocol Buffers RPC Reference

Port: `50051`. Protobuf definition: `proto/tapisaurus.proto`.

```protobuf
service TapisaurusDbService {
  // 1. Execute SQL, MongoDB JSON, or TapisaurusQL query
  rpc ExecuteQuery (QueryRequest) returns (QueryResponse);

  // 2. High-speed HNSW dense vector ANN search (< 1ms)
  rpc VectorSearch (VectorSearchRequest) returns (VectorSearchResponse);

  // 3. High-throughput bulk document ingestion
  rpc InsertDocuments (InsertRequest) returns (InsertResponse);

  // 4. Server-streaming Change Data Capture (CDC) feed
  rpc SubscribeChangeStream (StreamRequest) returns (stream ChangeEventMsg);

  // 5. Cluster health and readiness probe
  rpc HealthCheck (HealthRequest) returns (HealthResponse);
}
```

---

## 8. Python SDK & LangGraph Native Saver

Install or use local Python SDK from `bindings/python`:

```python
from tapisaurus import TapisaurusDB
from tapisaurus.langgraph import TapisaurusSaver
from langgraph.graph import StateGraph, END
from typing import TypedDict

# 1. Initialize Native TapisaurusDB Client
db = TapisaurusDB("http://localhost:27018")

# 2. Document CRUD
users = db.collection("users")
doc_id = users.insert({"name": "Ahmad Faiz", "role": "Architect"})
records = users.find({"role": "Architect"})

# 3. HNSW Vector Similarity Search
vectors = users.vector_search([0.95, 0.88, 0.12, 0.04], top_k=5)

# 4. Okapi BM25 Full-Text Search
matches = users.search("Faiz", fuzzy=True, top_k=10)

# 5. Native LangGraph Checkpointer (Zero PostgreSQL Dependency)
checkpointer = TapisaurusSaver(db, collection_prefix="production_agents")

class AgentState(TypedDict):
    query: str
    response: str

def agent_step(state: AgentState):
    facts = db.query("MATCH (a:Agent)-[:knows]->(b) RETURN b LIMIT 5")
    return {"response": f"Resolved: {facts}"}

workflow = StateGraph(AgentState)
workflow.add_node("agent", agent_step)
workflow.set_entry_point("agent")
workflow.add_edge("agent", END)

# Compile LangGraph with native TapisaurusSaver
app = workflow.compile(checkpointer=checkpointer)

# Sub-millisecond snapshot persistence & time-travel enabled
config = {"configurable": {"thread_id": "session_001"}}
result = app.invoke({"query": "Analyze system load"}, config)
print(result)
```

---

## 9. Embedded In-Process Rust Engine (`tapisaurus-core`)

Use TapisaurusDB directly inside your Rust binaries without running any server process.

```rust
use tapisaurus_core::storage::engine::{StorageConfig, StorageEngine};
use tapisaurus_vector::{HnswConfig, HnswIndex, DistanceMetric, QuantizationType};
use tapisaurus_core::storage::columnar::ColumnarBatch;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Embedded Storage Engine (LSM-Tree + WAL)
    let config = StorageConfig {
        data_dir: PathBuf::from("./embedded_db_data"),
        memtable_size: 4 * 1024 * 1024, // 4MB RAM footprint
        sync_writes: false,
        enable_wal: true,
    };
    let db = StorageEngine::open(config)?;
    db.put(b"device:sensor_01", b"{\"temp\": 24.5}")?;
    let val = db.get(b"device:sensor_01")?;

    // 2. Embedded HNSW Vector Index with Scalar8 Quantization
    let v_config = HnswConfig::new(1536, DistanceMetric::Cosine)
        .with_quantization(QuantizationType::Scalar8);
    let mut index = HnswIndex::new(v_config);
    index.insert("article_01", embedding_vec)?;
    let nearest = index.search(&query_vec, 5);

    // 3. Embedded SIMD Columnar Analytics
    let batch = ColumnarBatch::from_json_documents(&documents)?;
    let total_volume = batch.sum_f64("trade_volume").unwrap();

    Ok(())
}
```

---

## 10. Developer Tools & Observability Assets

TapisaurusDB ships with official, production-ready observability and testing assets in the repository:

### A. Official Grafana Dashboard Template
- **File**: [`dashboards/tapisaurus-overview.json`](../dashboards/tapisaurus-overview.json)
- **Features**:
  - Real-time engine health, uptime, and active connection tracking
  - Total QPS throughput and operation breakdown by command type
  - Read/Write I/O bandwidth monitoring
  - Query latency percentiles ($p_{50}$, $p_{90}$, $p_{99}$) and average execution times
  - Distributed Cost-Based Optimizer (CBO) join strategy metrics and cluster network transfer
  - Storage buffer cache hit ratio gauge and WAL fsync frequencies
- **How to Use**:
  1. Open Grafana (`http://localhost:3000`).
  2. Navigate to **Dashboards** → **New** → **Import**.
  3. Upload or paste the contents of `dashboards/tapisaurus-overview.json`.
  4. Select your Prometheus data source (configured to scrape TapisaurusDB at `http://localhost:27018/metrics`).

### B. Official Postman Collection
- **File**: [`docs/tapisaurus.postman_collection.json`](tapisaurus.postman_collection.json)
- **Features**:
  - Covers all 8 core API modules: Authentication, Health & Telemetry, Collections, TapisaurusQL Engine, Vector HNSW, Knowledge Graph, MVCC Transactions, and Backups.
  - Automatic JWT Bearer token capture and injection into collection variables upon login.
  - Pre-populated JSON payloads for instant verification.
- **How to Use**:
  1. Open Postman or Insomnia.
  2. Click **Import** and select `tapisaurus.postman_collection.json`.
  3. Run the **01. Authentication & Security / Login** request to authenticate.
  4. Test any query, vector search, or graph traversal with a single click.

---

*© 2026 TapisaurusDB Project · Created by Ahmad Faiz · All Rights Reserved.*
