const $ = id => document.getElementById(id);
const errorMessage = err => (err instanceof Error ? err.message : String(err));

// ---- Global Studio State ----
let engine = null;
let database = null;
let busy = false;
let savedName = 'ai-support-demo.tapir';
let activeView = 'data';
let activeTable = null;
let activeTableCols = [];
let activeTableRows = [];
let tableSort = { col: null, asc: true };
let tablePage = 1;
const PAGE_SIZE = 25;

let sqlHistory = [];
let lastSqlQuery = '';
let lastMemoryQuery = 'encrypted vaults';
let knownNamespaces = new Set(['policies', 'incidents', 'architecture']);
let memoryJournal = []; // Track stored docs locally for seamless encryption migration
let graphState = { nodes: [], edges: [], selectedId: null, hopMap: new Map(), activeEdges: new Set() };
let pendingEncryptedBytes = null;
let pendingEncryptedName = null;
let sdkContext = { kind: 'sql', payload: '' };
let activeSdkLang = 'python';
const embeddedServerMode = document.querySelector('meta[name="tapirus-mode"]')?.content === 'server';
let serverState = {
  connected: false,
  mode: 'local', // 'local' | 'remote'
  baseUrl: 'http://127.0.0.1:27018',
  token: '',
  info: null,
  readiness: null,
  collections: [],
  activeCollection: null,
};

// ---- IndexedDB Persistence ----
const storage = new Promise((resolve, reject) => {
  const req = indexedDB.open('tapirus-rust-workspace', 1);
  req.onupgradeneeded = () => req.result.createObjectStore('databases');
  req.onsuccess = () => resolve(req.result);
  req.onerror = () => reject(req.error);
  req.onblocked = () => reject(new Error('Close older Studio tabs to upgrade storage'));
});
storage.catch(err => setFooterStorage(`Browser storage unavailable: ${errorMessage(err)}`));

async function readSaved(name) {
  const store = (await storage).transaction('databases', 'readonly').objectStore('databases');
  return new Promise((resolve, reject) => {
    const req = store.get(name);
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

async function writeSaved(name, bytes) {
  const tx = (await storage).transaction('databases', 'readwrite', { durability: 'strict' });
  tx.objectStore('databases').put(bytes, name);
  return new Promise((resolve, reject) => {
    tx.oncomplete = resolve;
    tx.onerror = () => reject(tx.error);
    tx.onabort = () => reject(tx.error || new Error('Browser save aborted'));
  });
}

async function listSavedNames() {
  try {
    const store = (await storage).transaction('databases', 'readonly').objectStore('databases');
    return await new Promise((resolve, reject) => {
      const req = store.getAllKeys();
      req.onsuccess = () => resolve(req.result || []);
      req.onerror = () => reject(req.error);
    });
  } catch {
    return [];
  }
}

// ---- Utility Helpers ----
function setStatus(id, message, isError = false) {
  const node = $(id);
  if (!node) return;
  node.textContent = message;
  node.classList.toggle('error', isError);
  node.dataset.sequence = String(Number(node.dataset.sequence || 0) + 1);
}

function setFooterStorage(text) {
  const el = $('sb-storage');
  if (el) el.textContent = text;
}

function formatBytes(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}

function normalizeName(value) {
  const cleaned = value.trim().replace(/[^a-zA-Z0-9_.-]/g, '_');
  if (!cleaned) throw new Error('Enter a database filename');
  return cleaned.endsWith('.tapir') ? cleaned : `${cleaned}.tapir`;
}

function isQueryStatement(sql) {
  return /^\s*(SELECT|WITH|EXPLAIN|PRAGMA|MATCH|CYPHER|GRAPH\s+MATCH|GRAPH\s+TRAVERSE|GRAPH\s+SHORTEST)\b/i.test(sql)
    || /\bRETURNING\b/i.test(sql);
}

function splitSqlStatements(sql) {
  const stmts = [];
  let current = '';
  let quote = null;
  let inLineComment = false;
  for (let i = 0; i < sql.length; i++) {
    const ch = sql[i];
    const next = sql[i + 1];
    if (inLineComment) {
      if (ch === '\n') inLineComment = false;
      continue;
    }
    if (!quote && ch === '-' && next === '-') {
      inLineComment = true;
      i++;
      continue;
    }
    if (quote) {
      current += ch;
      if (ch === quote && next === quote) {
        current += next;
        i++;
      } else if (ch === quote) {
        quote = null;
      }
      continue;
    }
    if (ch === '\'' || ch === '"') {
      quote = ch;
      current += ch;
      continue;
    }
    if (ch === ';') {
      const trimmed = current.trim();
      if (trimmed) stmts.push(trimmed);
      current = '';
      continue;
    }
    current += ch;
  }
  const tail = current.trim();
  if (tail) stmts.push(tail);
  return stmts;
}

function formatSqlValue(val) {
  if (val === null || val === undefined) return 'NULL';
  if (typeof val === 'number') return Number.isFinite(val) ? String(val) : 'NULL';
  if (typeof val === 'boolean') return val ? '1' : '0';
  if (Array.isArray(val)) return `[${val.join(', ')}]`;
  return `'${String(val).replaceAll("'", "''")}'`;
}

// ---- Pre-Loaded Demo Scenarios ----
const SCENARIOS = {
  'ai-support': {
    file: 'ai-support-demo.tapir',
    encrypted: false,
    defaultSql: `SELECT c.name, c.region, t.title, t.priority, t.status, t.latency_ms\nFROM customers c\nJOIN tickets t ON t.customer_id = c.id\nORDER BY t.id ASC;`,
    defaultMemoryQuery: 'encrypted vaults',
    defaultGraphSeed: 'AuthGateway',
    sql: [
      `CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL, tier TEXT, region TEXT, mrr INTEGER)`,
      `INSERT INTO customers VALUES (1, 'Acme Robotics', 'enterprise', 'APAC', 14500)`,
      `INSERT INTO customers VALUES (2, 'Helios Health', 'enterprise', 'EU', 22000)`,
      `INSERT INTO customers VALUES (3, 'Kestrel Cloud', 'growth', 'US', 6400)`,
      `INSERT INTO customers VALUES (4, 'Nusantara FinTech', 'enterprise', 'APAC', 18200)`,
      `CREATE TABLE tickets (id INTEGER PRIMARY KEY, customer_id INTEGER, title TEXT NOT NULL, priority TEXT, status TEXT, latency_ms REAL)`,
      `INSERT INTO tickets VALUES (101, 1, 'APAC edge WAL batch sync spike', 'high', 'open', 4.82)`,
      `INSERT INTO tickets VALUES (102, 2, 'EU clinical vault key rotation', 'critical', 'in_progress', 1.15)`,
      `INSERT INTO tickets VALUES (103, 3, 'HNSW index rebuild after 50k vectors', 'medium', 'resolved', 3.41)`,
      `INSERT INTO tickets VALUES (104, 4, 'Offline .tapir sync over mobile link', 'high', 'open', 2.09)`,
      `INSERT INTO tickets VALUES (105, 1, 'Autonomous drone telemetry compression', 'low', 'resolved', 0.74)`,
      `CREATE TABLE knowledge_vectors (id INTEGER PRIMARY KEY, title TEXT, category TEXT, embedding VECTOR(4))`,
      `INSERT INTO knowledge_vectors VALUES (1, 'XChaCha20-Poly1305 Vault Encryption', 'security', [0.94, 0.12, 0.08, 0.05])`,
      `INSERT INTO knowledge_vectors VALUES (2, 'APAC Edge WAL Crash Recovery', 'storage', [0.14, 0.91, 0.11, 0.08])`,
      `INSERT INTO knowledge_vectors VALUES (3, 'GraphRAG Multi-Hop RRF Fusion', 'ai', [0.22, 0.15, 0.93, 0.14])`,
      `INSERT INTO knowledge_vectors VALUES (4, 'RaBitQ 1-bit Vector Compression', 'vector', [0.18, 0.24, 0.85, 0.42])`,
    ],
    memories: [
      { content: 'Enterprise customers use XChaCha20-Poly1305 encrypted vaults with PBKDF2 600k iterations and zero cloud egress.', namespace: 'policies', session_id: 'sec-01', importance: 0.95, tags: ['security', 'enterprise'] },
      { content: 'Acme Robotics reported WAL sync latency spike on APAC edge node during peak autonomous drone telemetry ingest.', namespace: 'incidents', session_id: 'inc-101', importance: 0.9, tags: ['apac', 'wal', 'telemetry'] },
      { content: 'Helios Health enforces HIPAA retention and EU clinical compliance using offline encrypted .tapir vaults.', namespace: 'policies', session_id: 'eu-09', importance: 0.92, tags: ['hipaa', 'eu', 'security'] },
      { content: 'Kestrel Cloud reduced vector RAM footprint by 32x using RaBitQ 1-bit quantization inside TapirusDB.', namespace: 'architecture', session_id: 'arch-04', importance: 0.82, tags: ['vector', 'rabitq'] },
      { content: 'Nusantara FinTech ships offline-first mobile agents backed by single-file encrypted .tapir vaults synced via TapirSync.', namespace: 'architecture', session_id: 'arch-07', importance: 0.85, tags: ['offline', 'fintech'] },
    ],
    graph: [
      `GRAPH INSERT NODE 1 LABEL 'Service' PROPERTIES '{"name":"AuthGateway","tier":1,"region":"APAC"}'`,
      `GRAPH INSERT NODE 2 LABEL 'Database' PROPERTIES '{"name":"TapirusVault","engine":"LSM+HNSW","encrypted":true}'`,
      `GRAPH INSERT NODE 3 LABEL 'Agent' PROPERTIES '{"name":"SupportBot","model":"local-rag"}'`,
      `GRAPH INSERT NODE 4 LABEL 'Customer' PROPERTIES '{"name":"AcmeRobotics","tier":"enterprise"}'`,
      `GRAPH INSERT NODE 5 LABEL 'Customer' PROPERTIES '{"name":"HeliosHealth","tier":"enterprise"}'`,
      `GRAPH INSERT EDGE 3 -> 1 LABEL 'CALLS' WEIGHT 0.92 PROPERTIES '{"auth":"mtls"}'`,
      `GRAPH INSERT EDGE 1 -> 2 LABEL 'DEPENDS_ON' WEIGHT 0.98 PROPERTIES '{"critical":true}'`,
      `GRAPH INSERT EDGE 3 -> 2 LABEL 'RECALLS_FROM' WEIGHT 1.00 PROPERTIES '{"mode":"hybrid-rrf"}'`,
      `GRAPH INSERT EDGE 4 -> 3 LABEL 'QUERIES' WEIGHT 0.88 PROPERTIES '{"sla":"10ms"}'`,
      `GRAPH INSERT EDGE 5 -> 2 LABEL 'AUDITS' WEIGHT 0.95 PROPERTIES '{"standard":"HIPAA"}'`,
    ],
  },
  'medical-vault': {
    file: 'clinical-encrypted-vault.tapir',
    encrypted: true,
    passphrase: 'tapirus-demo-vault',
    defaultSql: `SELECT id, patient_code, ward, risk_level, updated_at\nFROM clinical_notes\nORDER BY id ASC;`,
    defaultMemoryQuery: 'HIPAA encryption',
    defaultGraphSeed: 'ClinicalVault',
    sql: [
      `CREATE TABLE clinical_notes (id INTEGER PRIMARY KEY, patient_code TEXT NOT NULL, ward TEXT, risk_level TEXT, updated_at TEXT)`,
      `INSERT INTO clinical_notes VALUES (1, 'PT-9041', 'Cardiology', 'high', '2026-11-27 08:15')`,
      `INSERT INTO clinical_notes VALUES (2, 'PT-9042', 'Neurology', 'moderate', '2026-11-27 09:02')`,
      `INSERT INTO clinical_notes VALUES (3, 'PT-9043', 'ICU-North', 'critical', '2026-11-27 10:40')`,
      `CREATE TABLE diagnostic_vectors (id INTEGER PRIMARY KEY, protocol TEXT, embedding VECTOR(4))`,
      `INSERT INTO diagnostic_vectors VALUES (1, 'ECG Arrhythmia Triage', [0.91, 0.22, 0.14, 0.05])`,
      `INSERT INTO diagnostic_vectors VALUES (2, 'Sepsis Early Warning', [0.18, 0.88, 0.31, 0.12])`,
    ],
    memories: [
      { content: 'All clinical records stay inside XChaCha20-Poly1305 encrypted .tapir vaults for strict HIPAA and PDPA compliance.', namespace: 'compliance', session_id: 'hipaa-01', importance: 0.98, tags: ['hipaa', 'encryption'] },
      { content: 'ICU-North bedside tablets operate air-gapped with zero cloud egress and local AI memory retrieval.', namespace: 'protocols', session_id: 'icu-02', importance: 0.92, tags: ['offline', 'icu'] },
    ],
    graph: [
      `GRAPH INSERT NODE 1 LABEL 'Agent' PROPERTIES '{"name":"TriageAssistant","offline":true}'`,
      `GRAPH INSERT NODE 2 LABEL 'Database' PROPERTIES '{"name":"ClinicalVault","cipher":"XChaCha20"}'`,
      `GRAPH INSERT NODE 3 LABEL 'Service' PROPERTIES '{"name":"BedsideSync","ward":"ICU-North"}'`,
      `GRAPH INSERT EDGE 1 -> 2 LABEL 'READS_ENCRYPTED' WEIGHT 1.00 PROPERTIES '{"scope":"ward"}'`,
      `GRAPH INSERT EDGE 3 -> 2 LABEL 'REPLICATES' WEIGHT 0.94 PROPERTIES '{"delta":true}'`,
    ],
  },
  'edge-telemetry': {
    file: 'edge-telemetry.tapir',
    encrypted: false,
    defaultSql: `SELECT id, node_id, sensor, reading, VECTOR_COSINE(signature, [0.90, 0.10, 0.15, 0.05]) AS similarity\nFROM telemetry\nORDER BY similarity DESC;`,
    defaultMemoryQuery: 'thermal anomaly',
    defaultGraphSeed: 'EdgeGateway',
    sql: [
      `CREATE TABLE telemetry (id INTEGER PRIMARY KEY, node_id TEXT, sensor TEXT, reading REAL, signature VECTOR(4))`,
      `INSERT INTO telemetry VALUES (1, 'drone-07', 'battery_temp', 68.4, [0.92, 0.14, 0.18, 0.04])`,
      `INSERT INTO telemetry VALUES (2, 'drone-09', 'rotor_rpm', 4120.0, [0.12, 0.94, 0.08, 0.11])`,
      `INSERT INTO telemetry VALUES (3, 'rig-02', 'battery_temp', 71.2, [0.89, 0.11, 0.21, 0.06])`,
      `INSERT INTO telemetry VALUES (4, 'rig-04', 'pressure_kpa', 101.3, [0.15, 0.18, 0.92, 0.24])`,
    ],
    memories: [
      { content: 'Thermal anomaly alert triggers when drone battery_temp exceeds 65C for more than 12 seconds.', namespace: 'runbooks', session_id: 'ops-01', importance: 0.94, tags: ['thermal', 'anomaly'] },
      { content: 'EdgeGateway buffers 4KB slotted pages on NVMe and streams delta snapshots when satellite uplink returns.', namespace: 'hardware', session_id: 'ops-02', importance: 0.85, tags: ['edge', 'sync'] },
    ],
    graph: [
      `GRAPH INSERT NODE 1 LABEL 'Service' PROPERTIES '{"name":"EdgeGateway","ram_mb":23}'`,
      `GRAPH INSERT NODE 2 LABEL 'Database' PROPERTIES '{"name":"TelemetryStore","format":".tapir"}'`,
      `GRAPH INSERT NODE 3 LABEL 'Agent' PROPERTIES '{"name":"AnomalyWatcher","mode":"local"}'`,
      `GRAPH INSERT EDGE 3 -> 1 LABEL 'MONITORS' WEIGHT 0.96 PROPERTIES '{"interval":"1s"}'`,
      `GRAPH INSERT EDGE 1 -> 2 LABEL 'WRITES_WAL' WEIGHT 0.99 PROPERTIES '{"fsync":"batched"}'`,
    ],
  },
  blank: {
    file: 'workspace.tapir',
    encrypted: false,
    defaultSql: `CREATE TABLE notes (id INTEGER PRIMARY KEY, title TEXT NOT NULL, body TEXT);\nINSERT INTO notes VALUES (1, 'Hello TapirusDB', 'Single-file Rust database in WebAssembly.');\nSELECT * FROM notes;`,
    defaultMemoryQuery: '',
    defaultGraphSeed: '',
    sql: [],
    memories: [],
    graph: [],
  },
};

const SQL_SNIPPETS = {
  'select-customers': `SELECT c.name, c.region, t.title, t.priority, t.status, t.latency_ms\nFROM customers c\nJOIN tickets t ON t.customer_id = c.id\nORDER BY t.id ASC;`,
  'vector-near': `SELECT id, title, category\nFROM knowledge_vectors\nVECTOR NEAR embedding = [0.92, 0.10, 0.08, 0.05] TOP 3;`,
  'vector-cosine': `SELECT id, title, category,\n       VECTOR_COSINE(embedding, [0.92, 0.10, 0.08, 0.05]) AS similarity\nFROM knowledge_vectors\nORDER BY similarity DESC;`,
  aggregate: `SELECT c.region, COUNT(t.id) AS ticket_count, AVG(t.latency_ms) AS avg_latency_ms\nFROM customers c\nLEFT JOIN tickets t ON t.customer_id = c.id\nGROUP BY c.region\nORDER BY ticket_count DESC;`,
  'graph-match': `GRAPH MATCH (a)-[r]->(b)\nRETURN a, r, b;`,
  upsert: `INSERT INTO customers (id, name, tier, region, mrr)\nVALUES (5, 'Boreal AI', 'enterprise', 'EU', 19500)\nON CONFLICT DO UPDATE SET mrr = EXCLUDED.mrr\nRETURNING id, name, mrr;`,
  pragma: `PRAGMA table_info('customers');`,
};

// ---- Core Engine Runner ----
async function runAction(callback, statusId = 'sql-status') {
  if (busy || (!database && !serverState.connected)) return;
  busy = true;
  try {
    await callback();
  } catch (err) {
    setStatus(statusId, errorMessage(err), true);
  } finally {
    busy = false;
  }
}

async function persistSnapshot() {
  if (!database) return;
  if (database.is_in_transaction()) {
    setFooterStorage('Transaction open — COMMIT to save snapshot');
    return;
  }
  const bytes = database.export_bytes();
  $('sb-size').textContent = formatBytes(bytes.length);
  updateEncryptionUI();
  try {
    await writeSaved(savedName, bytes);
    localStorage.setItem('tapirus-studio-active-db', savedName);
    setFooterStorage(`Synced ${savedName} (IndexedDB)`);
    refreshSavedDbList();
  } catch (err) {
    setFooterStorage(`In-memory only (${errorMessage(err)})`);
  }
}

function updateEncryptionUI() {
  if (!database) return;
  const encrypted = database.is_encrypted();
  $('encrypt-icon').textContent = encrypted ? '🔒' : '🔓';
  $('encrypt-label').textContent = encrypted ? 'Encrypted' : 'Encrypt';
  $('btn-encrypt').classList.toggle('encrypted', encrypted);
  $('sb-security').textContent = encrypted ? '🔒 XChaCha20-Poly1305' : 'Plaintext .tapir';
}

function updateTransactionUI() {
  if (!database) return;
  const inTx = database.is_in_transaction();
  const badge = $('transaction-status');
  badge.textContent = inTx ? 'TRANSACTION OPEN' : 'AUTOCOMMIT';
  badge.classList.toggle('open', inTx);
  $('btn-tx-begin').disabled = inTx;
  $('btn-tx-commit').disabled = !inTx;
  $('btn-tx-rollback').disabled = !inTx;
}

// ---- Load Scenario into WASM ----
async function loadScenario(key) {
  const preset = SCENARIOS[key] || SCENARIOS['ai-support'];
  const next = preset.encrypted
    ? engine.TapirusWasm.new_encrypted(preset.passphrase)
    : new engine.TapirusWasm();

  memoryJournal = [];
  knownNamespaces = new Set();

  for (const stmt of preset.sql) {
    next.execute(stmt);
  }
  for (const mem of preset.memories) {
    next.store_memory_json(JSON.stringify({
      content: mem.content,
      namespace: mem.namespace,
      session_id: mem.session_id,
      importance: mem.importance,
      tags: mem.tags,
    }));
    memoryJournal.push({ ...mem });
    if (mem.namespace) knownNamespaces.add(mem.namespace);
  }
  for (const g of preset.graph) {
    next.execute(g);
  }

  if (database) database.free();
  database = next;
  savedName = preset.file;
  $('vault-name').value = savedName;
  $('sql').value = preset.defaultSql;
  if (preset.defaultMemoryQuery) {
    $('memory-query').value = preset.defaultMemoryQuery;
    lastMemoryQuery = preset.defaultMemoryQuery;
  }
  if (preset.defaultGraphSeed) {
    $('graphrag-query').value = preset.defaultGraphSeed;
  }

  activeTable = null;
  tablePage = 1;
  sqlHistory = [];
  renderSqlHistory();

  await refreshAllViews();
  await persistSnapshot();
  if (preset.defaultMemoryQuery) await runMemorySearch(preset.defaultMemoryQuery);
  if (preset.defaultGraphSeed) await runGraphRag(preset.defaultGraphSeed);
}

// ---- Refresh Explorer & Active Views ----
function getUserTables() {
  if (!database) return [];
  return JSON.parse(database.schema_json()).filter(t => !t.name.startsWith('__sys_'));
}

function safeIdent(name) {
  const cleaned = String(name || '').trim().replace(/[^a-zA-Z0-9_]/g, '_');
  if (!cleaned) throw new Error('Invalid SQL identifier');
  return /^[0-9]/.test(cleaned) ? `t_${cleaned}` : cleaned;
}

function getRowCount(tableName) {
  try {
    const res = JSON.parse(database.query_json(`SELECT COUNT(*) AS cnt FROM ${safeIdent(tableName)}`));
    return Number(res[0]?.cnt ?? 0);
  } catch {
    return 0;
  }
}

async function refreshAllViews() {
  if (!database) return;
  updateTransactionUI();
  updateEncryptionUI();
  refreshExplorer();
  refreshTableBrowser();
  refreshGraphCanvas();
  refreshMemoryStats();
  if (activeView === 'xray') refreshXray();
}

function refreshExplorer() {
  const tables = getUserTables();
  $('count-tables').textContent = String(tables.length);
  const list = $('table-list');
  list.innerHTML = '';

  if (tables.length === 0) {
    const li = document.createElement('li');
    li.className = 'tree-empty';
    li.textContent = 'No tables yet. Click + Table or drop a CSV.';
    list.appendChild(li);
    activeTable = null;
  } else {
    if (!activeTable || !tables.some(t => t.name === activeTable)) {
      activeTable = tables[0].name;
    }
    for (const tbl of tables) {
      const count = getRowCount(tbl.name);
      const li = document.createElement('li');
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = `tree-item-btn${tbl.name === activeTable ? ' active' : ''}`;
      btn.addEventListener('click', () => {
        activeTable = tbl.name;
        tablePage = 1;
        tableSort = { col: null, asc: true };
        switchView('data');
        refreshExplorer();
        refreshTableBrowser();
        $('sidebar').classList.remove('open');
      });

      const nameSpan = document.createElement('span');
      nameSpan.className = 'tree-item-name';
      nameSpan.textContent = `⊞ ${tbl.name}`;
      const metaSpan = document.createElement('span');
      metaSpan.className = 'tree-item-meta';
      metaSpan.textContent = String(count);
      btn.append(nameSpan, metaSpan);
      li.appendChild(btn);

      if (tbl.name === activeTable && Array.isArray(tbl.columns)) {
        const colList = document.createElement('ul');
        colList.className = 'tree-cols';
        for (const col of tbl.columns) {
          const cli = document.createElement('li');
          const left = document.createElement('span');
          left.textContent = col.primary_key ? `🔑 ${col.name}` : col.name;
          if (col.primary_key) left.className = 'col-pk';
          const right = document.createElement('span');
          right.className = 'col-type';
          right.textContent = formatDataType(col.data_type);
          cli.append(left, right);
          colList.appendChild(cli);
        }
        li.appendChild(colList);
      }
      list.appendChild(li);
    }
  }

  // Namespaces in Explorer
  const nsList = $('memory-ns-list');
  nsList.innerHTML = '';
  const nsArr = [...knownNamespaces];
  if (nsArr.length === 0) {
    const li = document.createElement('li');
    li.className = 'tree-empty';
    li.textContent = 'Default namespace';
    nsList.appendChild(li);
  } else {
    for (const ns of nsArr) {
      const li = document.createElement('li');
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'tree-item-btn';
      btn.textContent = `◈ ${ns}`;
      btn.addEventListener('click', () => {
        $('namespace').value = ns;
        switchView('memory');
        if ($('memory-query').value.trim()) runMemorySearch($('memory-query').value.trim());
        $('sidebar').classList.remove('open');
      });
      li.appendChild(btn);
      nsList.appendChild(li);
    }
  }
}

function formatDataType(dt) {
  if (typeof dt === 'string') return dt.toUpperCase();
  if (dt && typeof dt === 'object') {
    const key = Object.keys(dt)[0];
    return `${key.toUpperCase()}(${dt[key]})`;
  }
  return 'ANY';
}

async function refreshSavedDbList() {
  const names = await listSavedNames();
  const ul = $('saved-db-list');
  ul.innerHTML = '';
  if (!names.length) {
    const li = document.createElement('li');
    li.className = 'tree-empty';
    li.textContent = 'No browser copies yet';
    ul.appendChild(li);
    return;
  }
  for (const name of names) {
    const li = document.createElement('li');
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = `tree-item-btn${name === savedName ? ' active' : ''}`;
    btn.textContent = `⛁ ${name}`;
    btn.addEventListener('click', () => openSavedBrowserCopy(name));
    li.appendChild(btn);
    ul.appendChild(li);
  }
}

async function openSavedBrowserCopy(name) {
  await runAction(async () => {
    if (database.is_in_transaction()) throw new Error('COMMIT or ROLLBACK first.');
    const bytes = await readSaved(name);
    if (!bytes) throw new Error(`Saved copy "${name}" not found.`);
    try {
      const next = engine.TapirusWasm.import_bytes(bytes);
      database.free();
      database = next;
      savedName = name;
      $('vault-name').value = name;
      activeTable = null;
      await refreshAllViews();
      setFooterStorage(`Restored ${name} from IndexedDB`);
    } catch (err) {
      pendingEncryptedBytes = bytes;
      pendingEncryptedName = name;
      openEncryptModal('unlock');
    }
  });
}

// ---- TAB 1: TABLE DATA BROWSER ----
function refreshTableBrowser() {
  const tables = getUserTables();
  const schema = tables.find(t => t.name === activeTable);
  const strip = $('table-schema-bar');
  strip.innerHTML = '';

  if (!schema) {
    $('active-table-title').textContent = 'No table selected';
    $('active-table-meta').textContent = '0 rows';
    $('browser-grid-head').innerHTML = '';
    $('browser-grid-body').innerHTML = '<tr><td class="empty-cell">Create a table with <strong>+ Table</strong>, load a demo scenario, or drop a <code>.csv</code> file here.</td></tr>';
    $('btn-insert-row').disabled = true;
    $('btn-export-csv').disabled = true;
    return;
  }

  $('btn-insert-row').disabled = false;
  $('btn-export-csv').disabled = false;
  $('active-table-title').textContent = schema.name;
  activeTableCols = schema.columns || [];

  for (const col of activeTableCols) {
    const pill = document.createElement('span');
    const dt = formatDataType(col.data_type);
    pill.className = `col-pill${col.primary_key ? ' pk' : ''}${dt.startsWith('VECTOR') ? ' vec' : ''}`;
    const strong = document.createElement('strong');
    strong.textContent = col.name;
    const typeSpan = document.createElement('span');
    typeSpan.textContent = col.primary_key ? `${dt} · PK` : dt;
    pill.append(strong, typeSpan);
    strip.appendChild(pill);
  }

  const start = performance.now();
  try {
    activeTableRows = JSON.parse(database.query_json(`SELECT * FROM ${safeIdent(schema.name)}`));
  } catch (err) {
    activeTableRows = [];
    setStatus('browser-status', errorMessage(err), true);
  }
  const elapsed = performance.now() - start;
  $('sb-latency').textContent = `${elapsed.toFixed(2)} ms`;
  renderTableBrowserRows(elapsed);
}

function renderTableBrowserRows(elapsedMs = null) {
  const filter = $('table-filter').value.trim().toLowerCase();
  let rows = activeTableRows;
  if (filter) {
    rows = rows.filter(r => Object.values(r).some(v => String(v ?? '').toLowerCase().includes(filter)));
  }
  if (tableSort.col) {
    const col = tableSort.col;
    const dir = tableSort.asc ? 1 : -1;
    rows = [...rows].sort((a, b) => {
      const av = a[col], bv = b[col];
      if (av == null && bv == null) return 0;
      if (av == null) return 1;
      if (bv == null) return -1;
      if (typeof av === 'number' && typeof bv === 'number') return (av - bv) * dir;
      return String(av).localeCompare(String(bv)) * dir;
    });
  }

  const totalPages = Math.max(1, Math.ceil(rows.length / PAGE_SIZE));
  tablePage = Math.min(Math.max(1, tablePage), totalPages);
  const pageRows = rows.slice((tablePage - 1) * PAGE_SIZE, tablePage * PAGE_SIZE);

  $('active-table-meta').textContent = `${rows.length} rows · ${activeTableCols.length} cols`;
  $('page-indicator').textContent = `Page ${tablePage} / ${totalPages}`;
  $('btn-prev-page').disabled = tablePage <= 1;
  $('btn-next-page').disabled = tablePage >= totalPages;

  const pkCol = activeTableCols.find(c => c.primary_key)?.name || null;
  const colNames = activeTableCols.length
    ? activeTableCols.map(c => c.name)
    : (rows[0] ? Object.keys(rows[0]) : []);

  const thead = $('browser-grid-head');
  const tbody = $('browser-grid-body');
  thead.innerHTML = '';
  tbody.innerHTML = '';

  const headTr = document.createElement('tr');
  for (const name of colNames) {
    const th = document.createElement('th');
    const indicator = tableSort.col === name ? (tableSort.asc ? ' ▲' : ' ▼') : '';
    th.textContent = name + indicator;
    th.addEventListener('click', () => {
      if (tableSort.col === name) tableSort.asc = !tableSort.asc;
      else tableSort = { col: name, asc: true };
      renderTableBrowserRows();
    });
    headTr.appendChild(th);
  }
  if (pkCol) {
    const thAct = document.createElement('th');
    thAct.textContent = '';
    headTr.appendChild(thAct);
  }
  thead.appendChild(headTr);

  if (pageRows.length === 0) {
    const tr = document.createElement('tr');
    const td = document.createElement('td');
    td.colSpan = Math.max(1, colNames.length + (pkCol ? 1 : 0));
    td.className = 'empty-cell';
    td.textContent = filter ? `No rows match "${filter}".` : 'Table is empty. Click + Insert Row to add data.';
    tr.appendChild(td);
    tbody.appendChild(tr);
  } else {
    for (const row of pageRows) {
      const tr = document.createElement('tr');
      for (const colName of colNames) {
        const td = document.createElement('td');
        const val = row[colName];
        renderCellValue(td, val, colName, activeTable);
        tr.appendChild(td);
      }
      if (pkCol) {
        const tdAct = document.createElement('td');
        const del = document.createElement('button');
        del.type = 'button';
        del.className = 'row-del-btn';
        del.textContent = 'Delete';
        del.addEventListener('click', () => deleteRowByPk(activeTable, pkCol, row[pkCol]));
        tdAct.appendChild(del);
        tr.appendChild(tdAct);
      }
      tbody.appendChild(tr);
    }
  }

  if (elapsedMs !== null) {
    setStatus('browser-status', `Loaded ${rows.length} rows from "${activeTable}" in ${elapsedMs.toFixed(2)} ms.`);
  }
}

function renderCellValue(td, val, colName, tableName) {
  if (val === null || val === undefined) {
    td.className = 'null-cell';
    td.textContent = 'NULL';
    return;
  }
  if (Array.isArray(val) && val.every(x => typeof x === 'number')) {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'vec-chip';
    btn.title = 'Find nearest neighbors using this vector in SQL Workbench';
    btn.textContent = `VEC[${val.length}] · Find Similar ↗`;
    btn.addEventListener('click', () => {
      const rounded = val.map(v => Number(v.toFixed(3)));
      const q = `SELECT *, VECTOR_COSINE(${colName}, [${rounded.join(', ')}]) AS similarity\nFROM ${tableName}\nORDER BY similarity DESC\nLIMIT 5;`;
      $('sql').value = q;
      switchView('sql');
      executeSqlEditor();
    });
    td.appendChild(btn);
    return;
  }
  if (typeof val === 'number') {
    td.className = 'num-cell';
    td.textContent = Number.isInteger(val) ? String(val) : val.toFixed(4).replace(/\.?0+$/, '');
    return;
  }
  if (typeof val === 'object') {
    td.textContent = JSON.stringify(val);
    td.title = td.textContent;
    return;
  }
  td.textContent = String(val);
  td.title = String(val);
}

async function deleteRowByPk(tableName, pkCol, pkVal) {
  await runAction(async () => {
    database.execute_params(`DELETE FROM ${safeIdent(tableName)} WHERE ${safeIdent(pkCol)} = ?`, JSON.stringify([pkVal]));
    await persistSnapshot();
    refreshExplorer();
    refreshTableBrowser();
  }, 'browser-status');
}

// ---- TAB 2: SQL & VECTOR WORKBENCH ----
function renderGenericGrid(rows, headId, bodyId) {
  const thead = $(headId);
  const tbody = $(bodyId);
  thead.innerHTML = '';
  tbody.innerHTML = '';

  if (!Array.isArray(rows) || rows.length === 0) {
    const tr = document.createElement('tr');
    const td = document.createElement('td');
    td.className = 'empty-cell';
    td.textContent = 'Statement completed with 0 rows returned.';
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }

  const cols = [...new Set(rows.flatMap(r => (r && typeof r === 'object' ? Object.keys(r) : ['value'])))];
  const htr = document.createElement('tr');
  for (const c of cols) {
    const th = document.createElement('th');
    th.textContent = c;
    htr.appendChild(th);
  }
  thead.appendChild(htr);

  for (const r of rows) {
    const tr = document.createElement('tr');
    for (const c of cols) {
      const td = document.createElement('td');
      const val = r && typeof r === 'object' ? r[c] : r;
      renderCellValue(td, val, c, activeTable || 'table');
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
}

async function executeSqlEditor(overrideSql = null) {
  await runAction(async () => {
    const rawSql = (overrideSql ?? $('sql').value).trim();
    if (!rawSql) throw new Error('Enter at least one SQL, Vector, or Graph statement.');
    const paramsText = $('parameters').value.trim();
    const params = paramsText ? JSON.parse(paramsText) : [];
    if (!Array.isArray(params)) throw new Error('Bound parameters must be a JSON array.');

    const statements = splitSqlStatements(rawSql);
    if (statements.length === 0) throw new Error('No executable statement found.');
    if (statements.length > 1 && params.length > 0) {
      throw new Error('Bound parameters (?) can only be used with a single SQL statement.');
    }

    const start = performance.now();
    let lastOutput = [];
    let totalAffected = 0;
    let mutated = false;

    if (serverState.connected && serverState.mode === 'remote') {
      for (const stmt of statements) {
        const resp = await serverFetch('/v1/query', {
          method: 'POST',
          body: JSON.stringify({ query: stmt }),
        });
        const data = resp?.data ?? resp;
        if (Array.isArray(data)) lastOutput = data;
        else if (Array.isArray(data?.rows)) lastOutput = data.rows;
        else if (Array.isArray(data?.documents)) lastOutput = data.documents;
        else lastOutput = [data ?? { status: 'OK' }];
      }
      const elapsed = performance.now() - start;
      lastSqlQuery = rawSql;
      $('sb-latency').textContent = `${elapsed.toFixed(2)} ms`;
      $('sql-result').textContent = JSON.stringify(lastOutput, null, 2);
      renderGenericGrid(lastOutput, 'sql-grid-head', 'sql-grid-body');
      sqlHistory.unshift({ sql: rawSql, ms: elapsed.toFixed(2), rows: lastOutput.length });
      sqlHistory = sqlHistory.slice(0, 20);
      renderSqlHistory();
      await refreshServerCollections();
      setStatus('sql-status', `TapisaurusDB Server completed in ${elapsed.toFixed(2)} ms · ${lastOutput.length} rows.`);
      return;
    }

    if (!database) throw new Error('Local Rust WASM is not active in server-only mode. Connect to TapisaurusDB Server first.');

    for (const stmt of statements) {
      if (isQueryStatement(stmt)) {
        const json = params.length
          ? database.query_params_json(stmt, JSON.stringify(params))
          : database.query_json(stmt);
        lastOutput = JSON.parse(json);
      } else {
        const affected = params.length
          ? database.execute_params(stmt, JSON.stringify(params))
          : database.execute(stmt);
        totalAffected += Number(affected || 0);
        lastOutput = [{ status: 'OK', affectedRows: affected, statement: stmt.slice(0, 72) }];
        mutated = true;
      }
    }

    const elapsed = performance.now() - start;
    lastSqlQuery = rawSql;
    $('sb-latency').textContent = `${elapsed.toFixed(2)} ms`;
    $('sql-result').textContent = JSON.stringify(lastOutput, null, 2);
    renderGenericGrid(lastOutput, 'sql-grid-head', 'sql-grid-body');

    sqlHistory.unshift({ sql: rawSql, ms: elapsed.toFixed(2), rows: Array.isArray(lastOutput) ? lastOutput.length : 0 });
    sqlHistory = sqlHistory.slice(0, 20);
    renderSqlHistory();

    updateTransactionUI();
    if (mutated) {
      await persistSnapshot();
      refreshExplorer();
      refreshGraphCanvas();
    }

    const summary = statements.length > 1
      ? `${statements.length} statements ran in ${elapsed.toFixed(2)} ms (${Array.isArray(lastOutput) ? lastOutput.length : 0} rows).`
      : `Rust WASM completed in ${elapsed.toFixed(2)} ms · ${Array.isArray(lastOutput) ? lastOutput.length : totalAffected} rows.`;
    setStatus('sql-status', summary);
  }, 'sql-status');
}

function renderSqlHistory() {
  const ul = $('sql-history-list');
  ul.innerHTML = '';
  if (!sqlHistory.length) {
    const li = document.createElement('li');
    li.className = 'tree-empty';
    li.textContent = 'No queries executed yet.';
    ul.appendChild(li);
    return;
  }
  for (const item of sqlHistory) {
    const li = document.createElement('li');
    li.className = 'history-item';
    const code = document.createElement('code');
    code.textContent = item.sql.replace(/\s+/g, ' ');
    const meta = document.createElement('span');
    meta.className = 'meta-chip';
    meta.textContent = `${item.ms} ms · ${item.rows}r`;
    li.append(code, meta);
    li.addEventListener('click', () => {
      $('sql').value = item.sql;
      setSqlOutputTab('grid');
      executeSqlEditor(item.sql);
    });
    ul.appendChild(li);
  }
}

function setSqlOutputTab(mode) {
  document.querySelectorAll('[data-sql-out]').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.sqlOut === mode);
  });
  $('sql-out-grid').hidden = mode !== 'grid';
  $('sql-out-json').hidden = mode !== 'json';
  $('sql-out-history').hidden = mode !== 'history';
}

// ---- TAB 3: AI MEMORY & LIVE RAG CONTEXT BUILDER ----
function refreshMemoryStats() {
  if (!database) return;
  try {
    const stats = JSON.parse(database.memory_request_json('stats', '{}'));
    $('count-memories').textContent = String(stats.documents ?? 0);
    $('mem-stat-docs').textContent = String(stats.documents ?? 0);
    $('mem-stat-bytes').textContent = formatBytes(stats.estimated_resident_bytes ?? 0);
    $('mem-stat-spaces').textContent = String(stats.vector_spaces ?? 0);
  } catch {
    $('count-memories').textContent = '0';
  }
}

async function runMemorySearch(overrideQuery = null) {
  await runAction(async () => {
    const query = (overrideQuery ?? $('memory-query').value).trim();
    const mode = $('memory-mode').value;
    const vectorText = $('embedding').value.trim();
    const embedding = vectorText ? JSON.parse(vectorText) : undefined;
    if (!query && !embedding) throw new Error('Enter a search query or an optional vector embedding.');

    lastMemoryQuery = query;
    const start = performance.now();
    let hits = [];
    if (mode === 'rrf') {
      hits = JSON.parse(database.memory_recall_rrf_json(query, 8, 60));
    } else if (mode === 'scoped') {
      const req = {
        query: query || undefined,
        embedding,
        namespace: $('namespace').value.trim() || undefined,
        session_id: $('session').value.trim() || undefined,
        limit: 8,
      };
      hits = JSON.parse(database.search_memories_json(JSON.stringify(req)));
    } else {
      hits = JSON.parse(database.memory_recall_json(query, 8));
    }

    const promptText = query ? database.recall_prompt(query, 6) : '';
    const elapsed = performance.now() - start;
    $('sb-latency').textContent = `${elapsed.toFixed(2)} ms`;

    renderMemoryCards(hits);
    const cleanedPrompt = promptText && promptText.trim()
      ? promptText.trim()
      : 'No matching memories found for this query. Try another keyword or store a new source.';
    $('llm-prompt-preview').textContent = cleanedPrompt;
    $('prompt-tokens').textContent = `~${Math.max(1, Math.ceil(cleanedPrompt.length / 4))} tokens`;
    setStatus('memory-status', `Retrieved ${hits.length} sources in ${elapsed.toFixed(2)} ms (${mode.toUpperCase()} mode).`);
    refreshMemoryStats();
  }, 'memory-status');
}

function renderMemoryCards(hits) {
  const box = $('memory-cards');
  box.innerHTML = '';
  if (!hits.length) {
    const empty = document.createElement('p');
    empty.className = 'muted';
    empty.textContent = 'No sources matched this query. Click a suggested keyword chip above or store a new memory.';
    box.appendChild(empty);
    return;
  }

  for (const hit of hits) {
    const entry = hit.entry || {};
    if (entry.namespace) knownNamespaces.add(entry.namespace);
    const card = document.createElement('article');
    card.className = 'mem-card';

    const top = document.createElement('div');
    top.className = 'mem-card-top';
    const badges = document.createElement('div');
    badges.className = 'mem-badges';

    const idBadge = document.createElement('span');
    idBadge.className = 'mem-badge';
    idBadge.textContent = `#${entry.id}`;
    badges.appendChild(idBadge);

    if (entry.namespace) {
      const ns = document.createElement('span');
      ns.className = 'mem-badge ns';
      ns.textContent = entry.namespace;
      badges.appendChild(ns);
    }
    if (entry.session_id) {
      const s = document.createElement('span');
      s.className = 'mem-badge';
      s.textContent = entry.session_id;
      badges.appendChild(s);
    }
    for (const tag of (entry.tags || []).slice(0, 3)) {
      const t = document.createElement('span');
      t.className = 'mem-badge';
      t.textContent = `#${tag}`;
      badges.appendChild(t);
    }

    const forgetBtn = document.createElement('button');
    forgetBtn.type = 'button';
    forgetBtn.className = 'row-del-btn';
    forgetBtn.textContent = 'Forget';
    forgetBtn.addEventListener('click', () => forgetMemoryById(entry.id));

    top.append(badges, forgetBtn);

    const p = document.createElement('p');
    p.className = 'mem-content';
    p.textContent = entry.content || '';

    const scores = document.createElement('div');
    scores.className = 'mem-scores';
    const combined = Number(hit.combined_score ?? 0);
    const lexical = Number(hit.lexical_score ?? 0);
    const recency = Number(hit.recency_score ?? 0);
    scores.append(
      createScoreMeter('Combined', combined, Math.max(1, combined)),
      createScoreMeter('BM25 Lexical', lexical, Math.max(1.5, lexical)),
      createScoreMeter('Recency', recency, 1),
    );

    card.append(top, p, scores);
    box.appendChild(card);
  }
}

function createScoreMeter(label, value, max) {
  const wrap = document.createElement('div');
  wrap.className = 'score-meter';
  const row = document.createElement('div');
  const l = document.createElement('span');
  l.textContent = label;
  const v = document.createElement('span');
  v.textContent = value.toFixed(3);
  row.append(l, v);
  const meter = document.createElement('meter');
  meter.min = 0;
  meter.max = max;
  meter.value = Math.min(max, Math.max(0, value));
  wrap.append(row, meter);
  return wrap;
}

async function forgetMemoryById(id) {
  await runAction(async () => {
    database.forget_memory(BigInt(id));
    memoryJournal = memoryJournal.filter((_, idx) => idx + 1 !== Number(id));
    await persistSnapshot();
    refreshMemoryStats();
    runMemorySearch($('memory-query').value.trim());
  }, 'memory-status');
}

async function storeNewMemory() {
  await runAction(async () => {
    const content = $('memory-content').value.trim();
    if (!content) throw new Error('Enter memory content first.');
    const vecText = $('embedding').value.trim();
    const embedding = vecText ? JSON.parse(vecText) : undefined;
    const namespace = $('namespace').value.trim() || undefined;
    const session_id = $('session').value.trim() || undefined;
    const importance = Math.min(1, Math.max(0.1, Number($('memory-importance').value || 0.85)));
    const tags = $('memory-tags').value.split(',').map(s => s.trim()).filter(Boolean);

    const id = database.store_memory_json(JSON.stringify({
      content,
      embedding,
      namespace,
      session_id,
      importance,
      tags,
    }));
    memoryJournal.push({ content, embedding, namespace, session_id, importance, tags });
    if (namespace) knownNamespaces.add(namespace);
    $('memory-content').value = '';
    $('store-memory-drawer').open = false;

    await persistSnapshot();
    refreshExplorer();
    refreshMemoryStats();
    setStatus('memory-status', `Stored source #${id.toString()} in local .tapir memory.`);
    const firstWord = content.split(/\s+/).find(w => w.length > 4) || content.split(/\s+/)[0];
    if (firstWord) {
      $('memory-query').value = firstWord.replace(/[^a-zA-Z0-9_-]/g, '');
      runMemorySearch($('memory-query').value);
    }
  }, 'memory-status');
}

// ---- TAB 4: INTERACTIVE PROPERTY GRAPH & GRAPHRAG ----
function parseGraphNodeString(str) {
  const match = String(str).match(/^\(:([A-Za-z0-9_]+)\s+\{id:\s*(\d+),\s*properties:\s*(\{.*\})\}\)$/);
  if (!match) return null;
  let props = {};
  try { props = JSON.parse(match[3]); } catch {}
  return { id: Number(match[2]), label: match[1], props };
}

function parseGraphEdgeString(str) {
  const match = String(str).match(/^\[:([A-Za-z0-9_]+)\s+\{id:\s*(\d+),\s*weight:\s*([0-9.eE+-]+)\}\]$/);
  if (!match) return null;
  return { id: Number(match[2]), label: match[1], weight: Number(match[3]) };
}

function nodeColorForLabel(label) {
  const key = String(label || '').toLowerCase();
  if (key === 'service') return { fill: '#dbeafe', stroke: '#2563eb', dot: 'l-service' };
  if (key === 'database') return { fill: '#d1fae5', stroke: '#0b7261', dot: 'l-database' };
  if (key === 'agent') return { fill: '#ede9fe', stroke: '#7c3aed', dot: 'l-agent' };
  if (key === 'customer') return { fill: '#fef3c7', stroke: '#d97706', dot: 'l-customer' };
  return { fill: '#e2e8f0', stroke: '#475569', dot: 'l-other' };
}

function refreshGraphCanvas() {
  if (!database) return;
  let rows = [];
  try {
    rows = JSON.parse(database.query_json('GRAPH MATCH (a)-[r]->(b) RETURN a, r, b'));
  } catch {
    rows = [];
  }

  const nodeMap = new Map();
  const edges = [];
  for (const row of rows) {
    const a = parseGraphNodeString(row.a);
    const b = parseGraphNodeString(row.b);
    const r = parseGraphEdgeString(row.r);
    if (a) nodeMap.set(a.id, a);
    if (b) nodeMap.set(b.id, b);
    if (a && b && r) {
      edges.push({ ...r, from: a.id, to: b.id });
    }
  }

  graphState.nodes = [...nodeMap.values()].sort((x, y) => x.id - y.id);
  graphState.edges = edges;
  $('count-graph').textContent = `${graphState.nodes.length}N · ${edges.length}E`;
  $('graph-meta').textContent = `${graphState.nodes.length} nodes · ${edges.length} edges`;
  renderGraphSvg();
}

function renderGraphSvg() {
  const edgeLayer = $('graph-edges');
  const nodeLayer = $('graph-nodes');
  const legend = $('graph-legend');
  edgeLayer.innerHTML = '';
  nodeLayer.innerHTML = '';
  legend.innerHTML = '';

  const nodes = graphState.nodes;
  const edges = graphState.edges;
  const ns = 'http://www.w3.org/2000/svg';

  if (nodes.length === 0) {
    const txt = document.createElementNS(ns, 'text');
    txt.setAttribute('x', '380');
    txt.setAttribute('y', '220');
    txt.setAttribute('text-anchor', 'middle');
    txt.setAttribute('class', 'graph-node-sub');
    txt.textContent = 'No graph relationships yet. Load a demo scenario or click + Add Edge.';
    nodeLayer.appendChild(txt);
    return;
  }

  // Deterministic circular/radial layout centered in 760x440
  const cx = 380, cy = 218;
  const rx = Math.min(260, 120 + nodes.length * 22);
  const ry = Math.min(155, 85 + nodes.length * 14);
  const pos = new Map();
  nodes.forEach((n, i) => {
    const angle = -Math.PI / 2 + (2 * Math.PI * i) / nodes.length;
    pos.set(n.id, {
      x: Math.round(cx + rx * Math.cos(angle)),
      y: Math.round(cy + ry * Math.sin(angle)),
    });
  });

  // Render edges
  for (const e of edges) {
    const p1 = pos.get(e.from);
    const p2 = pos.get(e.to);
    if (!p1 || !p2) continue;
    const isActive = graphState.activeEdges.has(e.id);
    const line = document.createElementNS(ns, 'line');
    line.setAttribute('x1', String(p1.x));
    line.setAttribute('y1', String(p1.y));
    line.setAttribute('x2', String(p2.x));
    line.setAttribute('y2', String(p2.y));
    line.setAttribute('class', `graph-edge-line${isActive ? ' active' : ''}`);
    line.setAttribute('marker-end', isActive ? 'url(#arrow-active)' : 'url(#arrow)');
    edgeLayer.appendChild(line);

    const mx = Math.round((p1.x + p2.x) / 2);
    const my = Math.round((p1.y + p2.y) / 2) - 6;
    const lbl = document.createElementNS(ns, 'text');
    lbl.setAttribute('x', String(mx));
    lbl.setAttribute('y', String(my));
    lbl.setAttribute('class', 'graph-edge-label');
    lbl.textContent = `${e.label} (${e.weight.toFixed(2)})`;
    edgeLayer.appendChild(lbl);
  }

  // Render nodes
  const seenLabels = new Map();
  for (const n of nodes) {
    const p = pos.get(n.id);
    const colors = nodeColorForLabel(n.label);
    seenLabels.set(n.label, colors.dot);

    const g = document.createElementNS(ns, 'g');
    g.setAttribute('class', 'graph-node-group');
    g.addEventListener('click', () => selectGraphNode(n));

    const hop = graphState.hopMap.get(n.id);
    const circle = document.createElementNS(ns, 'circle');
    circle.setAttribute('cx', String(p.x));
    circle.setAttribute('cy', String(p.y));
    circle.setAttribute('r', graphState.selectedId === n.id ? '27' : '24');
    circle.setAttribute('fill', colors.fill);
    circle.setAttribute('stroke', colors.stroke);
    let cls = 'graph-node-circle';
    if (graphState.selectedId === n.id) cls += ' selected';
    if (hop === 0) cls += ' hop-0';
    else if (hop !== undefined) cls += ' hop-1';
    circle.setAttribute('class', cls);

    const title = document.createElementNS(ns, 'text');
    title.setAttribute('x', String(p.x));
    title.setAttribute('y', String(p.y + 40));
    title.setAttribute('class', 'graph-node-name');
    title.textContent = n.props.name || `${n.label} #${n.id}`;

    const sub = document.createElementNS(ns, 'text');
    sub.setAttribute('x', String(p.x));
    sub.setAttribute('y', String(p.y + 4));
    sub.setAttribute('class', 'graph-node-sub');
    sub.textContent = `#${n.id}`;

    g.append(circle, sub, title);
    nodeLayer.appendChild(g);
  }

  for (const [label, dotClass] of seenLabels.entries()) {
    const item = document.createElement('span');
    item.className = 'legend-item';
    const dot = document.createElement('span');
    dot.className = `legend-dot ${dotClass}`;
    item.append(dot, document.createTextNode(label));
    legend.appendChild(item);
  }
}

function selectGraphNode(node) {
  graphState.selectedId = node.id;
  renderGraphSvg();
  const inspector = $('node-inspector');
  inspector.innerHTML = '';

  const info = document.createElement('div');
  const strong = document.createElement('strong');
  strong.textContent = `:${node.label} #${node.id} `;
  const code = document.createElement('code');
  code.textContent = JSON.stringify(node.props);
  info.append(strong, code);

  const btn = document.createElement('button');
  btn.type = 'button';
  btn.className = 'btn-xs';
  btn.textContent = 'Run GraphRAG from Node →';
  btn.addEventListener('click', () => {
    const seed = node.props.name || node.label;
    $('graphrag-query').value = seed;
    runGraphRag(seed);
  });

  inspector.append(info, btn);
}

async function runGraphRag(overrideSeed = null) {
  await runAction(async () => {
    const seed = (overrideSeed ?? $('graphrag-query').value).trim();
    if (!seed) throw new Error('Enter an entity name or keyword to traverse.');
    const hops = Number($('graphrag-hops').value || 2);
    const start = performance.now();
    const res = JSON.parse(database.graph_rag_query_json(seed, 3, hops, 8));
    const elapsed = performance.now() - start;
    $('sb-latency').textContent = `${elapsed.toFixed(2)} ms`;

    graphState.hopMap = new Map();
    graphState.activeEdges = new Set();
    const hitsBox = $('graphrag-hits');
    hitsBox.innerHTML = '';

    for (const item of (res.results || [])) {
      graphState.hopMap.set(item.entity_id, item.hop_distance);
      for (const rel of (item.related_edges || [])) {
        graphState.activeEdges.add(rel.id);
      }
      let props = {};
      try { props = JSON.parse(item.properties); } catch {}
      const pill = document.createElement('div');
      pill.className = 'hit-pill';
      const left = document.createElement('span');
      left.textContent = `${item.label}: ${props.name || '#' + item.entity_id}`;
      const right = document.createElement('span');
      right.className = 'meta-chip';
      right.textContent = `Hop ${item.hop_distance} · RRF ${Number(item.rrf_score).toFixed(4)}`;
      pill.append(left, right);
      hitsBox.appendChild(pill);
    }

    $('graphrag-context').textContent = res.prompt_context || 'No related graph context found.';
    setStatus('graphrag-status', `GraphRAG traversed ${(res.results || []).length} entities in ${elapsed.toFixed(2)} ms.`);
    renderGraphSvg();
  }, 'graphrag-status');
}

// ---- TAB 5: VAULT X-RAY, RABITQ & TAP LITE ----
function refreshXray() {
  if (!database) return;
  if (database.is_in_transaction()) {
    $('xray-hex').textContent = 'COMMIT or ROLLBACK the active transaction to inspect raw binary .tapir bytes.';
    return;
  }
  const bytes = database.export_bytes();
  $('xray-size').textContent = formatBytes(bytes.length);
  const magic = String.fromCharCode(...bytes.slice(0, 7));
  $('xray-magic').textContent = `${magic}\\0`;
  $('xray-enc').textContent = database.is_encrypted() ? 'XChaCha20-Poly1305' : 'Plaintext';
  $('xray-opfs').textContent = typeof engine.OpfsSyncPager === 'function' ? '4KB LRU Ready' : 'Standard Heap';

  const slice = bytes.slice(0, 128);
  const lines = [];
  for (let offset = 0; offset < slice.length; offset += 16) {
    const row = slice.slice(offset, offset + 16);
    const hex = [...row].map(b => b.toString(16).padStart(2, '0')).join(' ').padEnd(47, ' ');
    const ascii = [...row].map(b => (b >= 32 && b <= 126 ? String.fromCharCode(b) : '.')).join('');
    lines.push(`${offset.toString(16).padStart(4, '0')}: ${hex}  |${ascii}|`);
  }
  $('xray-hex').textContent = lines.join('\n');
}

function runRaBitQ(numBits) {
  runAction(() => {
    const vec = JSON.parse($('rabitq-input').value.trim());
    if (!Array.isArray(vec) || vec.length === 0) throw new Error('Supply a non-empty JSON number array.');
    const start = performance.now();
    const out = JSON.parse(database.rabitq_quantize_json(JSON.stringify(vec), numBits));
    const elapsed = performance.now() - start;
    const origBytes = vec.length * 4;
    const compBytes = Math.ceil((vec.length * numBits) / 8);
    const ratio = compBytes > 0 ? (origBytes / compBytes).toFixed(1) : '—';
    $('rabitq-orig').textContent = `${origBytes} B (${vec.length}d f32)`;
    $('rabitq-comp').textContent = `${compBytes} B (${numBits}-bit)`;
    $('rabitq-ratio').textContent = `${ratio}× smaller`;
    $('rabitq-result').textContent = `// Rust WASM RaBitQ (${numBits}-bit) completed in ${elapsed.toFixed(2)} ms\n` + JSON.stringify(out, null, 2);
  }, 'sb-storage');
}

// ---- DRAG & DROP / MULTI-FORMAT IMPORTER ----
function parseCsv(text) {
  const rows = [];
  let row = [], cell = '', inQuotes = false;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i], next = text[i + 1];
    if (inQuotes) {
      if (ch === '"' && next === '"') { cell += '"'; i++; }
      else if (ch === '"') inQuotes = false;
      else cell += ch;
    } else if (ch === '"') {
      inQuotes = true;
    } else if (ch === ',') {
      row.push(cell.trim());
      cell = '';
    } else if (ch === '\r' && next === '\n') {
      row.push(cell.trim());
      if (row.some(Boolean)) rows.push(row);
      row = []; cell = ''; i++;
    } else if (ch === '\n') {
      row.push(cell.trim());
      if (row.some(Boolean)) rows.push(row);
      row = []; cell = '';
    } else {
      cell += ch;
    }
  }
  if (cell.length > 0 || row.length > 0) {
    row.push(cell.trim());
    if (row.some(Boolean)) rows.push(row);
  }
  return rows;
}

function inferColumnType(values) {
  const nonEmpty = values.filter(v => v !== '' && v !== null && v !== undefined);
  if (nonEmpty.length === 0) return 'TEXT';
  if (nonEmpty.every(v => /^-?\d+$/.test(String(v)))) return 'INTEGER';
  if (nonEmpty.every(v => /^-?\d+(\.\d+)?$/.test(String(v)))) return 'REAL';
  return 'TEXT';
}

async function openWithOpfs4kbPager(file) {
  if (typeof engine.OpfsSyncPager === 'function' && typeof engine.TapirusWasm.from_opfs_pager === 'function') {
    const cachePages = 256;
    const pager = new engine.OpfsSyncPager(BigInt(file.size), cachePages);
    const totalPages = Math.ceil(file.size / 4096);
    const initialPages = Math.min(totalPages, cachePages);
    for (let pid = 1; pid <= initialPages; pid++) {
      const start = (pid - 1) * 4096;
      const slice = file.slice(start, Math.min(start + 4096, file.size));
      pager.stage_page_4kb(pid, new Uint8Array(await slice.arrayBuffer()));
    }
    return engine.TapirusWasm.from_opfs_pager(pager, undefined);
  }
  throw new Error('Use the native SDK or OPFS 4KB Sync Pager for files larger than 50 MB.');
}

async function handleImportedFile(file) {
  if (!file) return;
  const lower = file.name.toLowerCase();

  if (lower.endsWith('.tapir')) {
    await runAction(async () => {
      if (database.is_in_transaction()) throw new Error('COMMIT or ROLLBACK before opening another vault.');
      const bytes = new Uint8Array(await file.arrayBuffer());
      try {
        const next = file.size > 50 * 1024 * 1024
          ? await openWithOpfs4kbPager(file)
          : engine.TapirusWasm.import_bytes(bytes);
        database.free();
        database = next;
        savedName = normalizeName(file.name);
        $('vault-name').value = savedName;
        activeTable = null;
        await refreshAllViews();
        if (file.size <= 50 * 1024 * 1024) await persistSnapshot();
        setFooterStorage(`Opened ${savedName}`);
      } catch {
        pendingEncryptedBytes = bytes;
        pendingEncryptedName = normalizeName(file.name);
        openEncryptModal('unlock');
      }
    });
    return;
  }

  if (lower.endsWith('.csv')) {
    await runAction(async () => {
      const text = await file.text();
      const parsed = parseCsv(text);
      if (parsed.length < 2) throw new Error('CSV must include a header row and at least one data row.');
      const tableName = safeIdent(file.name.replace(/\.csv$/i, '') || 'imported_csv');
      const headers = parsed[0].map((h, i) => safeIdent(h || `col_${i + 1}`));
      const dataRows = parsed.slice(1);
      const colTypes = headers.map((_, ci) => inferColumnType(dataRows.map(r => r[ci])));
      const ddlCols = headers.map((h, i) => `${h} ${colTypes[i]}`).join(', ');
      database.execute(`DROP TABLE IF EXISTS ${tableName}`);
      database.execute(`CREATE TABLE ${tableName} (${ddlCols})`);
      const placeholders = headers.map(() => '?').join(', ');
      const insertSql = `INSERT INTO ${tableName} VALUES (${placeholders})`;
      for (const r of dataRows) {
        const typed = headers.map((_, ci) => {
          const raw = r[ci] ?? '';
          if (raw === '') return null;
          if (colTypes[ci] === 'INTEGER') return parseInt(raw, 10);
          if (colTypes[ci] === 'REAL') return parseFloat(raw);
          return raw;
        });
        database.execute_params(insertSql, JSON.stringify(typed));
      }
      activeTable = tableName;
      await persistSnapshot();
      switchView('data');
      refreshExplorer();
      refreshTableBrowser();
      setStatus('browser-status', `Imported ${dataRows.length} rows from ${file.name} into table "${tableName}".`);
    }, 'browser-status');
    return;
  }

  if (lower.endsWith('.json')) {
    await runAction(async () => {
      const parsed = JSON.parse(await file.text());
      if (Array.isArray(parsed) && parsed.length > 0 && typeof parsed[0] === 'object') {
        const tableName = safeIdent(file.name.replace(/\.json$/i, '') || 'imported_json');
        const rawHeaders = [...new Set(parsed.flatMap(o => Object.keys(o)))];
        const headers = rawHeaders.map((h, i) => safeIdent(h || `col_${i + 1}`));
        const colTypes = rawHeaders.map(h => inferColumnType(parsed.map(o => o[h])));
        const ddlCols = headers.map((h, i) => `${h} ${colTypes[i]}`).join(', ');
        database.execute(`DROP TABLE IF EXISTS ${tableName}`);
        database.execute(`CREATE TABLE ${tableName} (${ddlCols})`);
        const placeholders = headers.map(() => '?').join(', ');
        const insertSql = `INSERT INTO ${tableName} VALUES (${placeholders})`;
        for (const obj of parsed) {
          const vals = rawHeaders.map(h => {
            const v = obj[h];
            return v && typeof v === 'object' ? JSON.stringify(v) : (v ?? null);
          });
          database.execute_params(insertSql, JSON.stringify(vals));
        }
        activeTable = tableName;
        await persistSnapshot();
        switchView('data');
        refreshExplorer();
        refreshTableBrowser();
        setStatus('browser-status', `Imported ${parsed.length} JSON records into "${tableName}".`);
      } else {
        database.store_memory_json(JSON.stringify({ content: JSON.stringify(parsed), namespace: 'imported', tags: ['json'] }));
        knownNamespaces.add('imported');
        await persistSnapshot();
        switchView('memory');
        refreshExplorer();
        refreshMemoryStats();
      }
    }, 'browser-status');
    return;
  }

  // .txt / .md → AI Memory documents
  await runAction(async () => {
    const text = (await file.text()).trim();
    const chunks = text.split(/\n{2,}/).map(s => s.trim()).filter(Boolean).slice(0, 20);
    for (const chunk of chunks) {
      database.store_memory_json(JSON.stringify({ content: chunk, namespace: 'imported', tags: [file.name] }));
      memoryJournal.push({ content: chunk, namespace: 'imported', tags: [file.name] });
    }
    knownNamespaces.add('imported');
    await persistSnapshot();
    switchView('memory');
    refreshExplorer();
    refreshMemoryStats();
    setStatus('memory-status', `Ingested ${chunks.length} paragraphs from ${file.name} into AI Memory.`);
  }, 'memory-status');
}

// ---- ENCRYPTION / UNLOCK MODAL ----
function openEncryptModal(mode = 'encrypt') {
  const modal = $('modal-encrypt');
  modal.dataset.mode = mode;
  $('encrypt-passphrase').value = '';
  $('encrypt-modal-error').hidden = true;

  if (mode === 'unlock') {
    $('modal-encrypt-title').textContent = `Unlock Encrypted Vault (${pendingEncryptedName || '.tapir'})`;
    $('encrypt-modal-desc').textContent = 'This .tapir file is encrypted with XChaCha20-Poly1305. Enter its passphrase to decrypt pages inside local WebAssembly.';
    $('encrypt-submit-btn').textContent = 'Unlock Vault';
  } else if (database?.is_encrypted()) {
    $('modal-encrypt-title').textContent = 'Vault Is Already Encrypted (XChaCha20-Poly1305)';
    $('encrypt-modal-desc').textContent = 'Enter a new passphrase to re-key this workspace into a fresh encrypted .tapir snapshot.';
    $('encrypt-submit-btn').textContent = 'Re-Key Vault';
  } else {
    $('modal-encrypt-title').textContent = 'Encrypt Vault with XChaCha20-Poly1305';
    $('encrypt-modal-desc').textContent = 'Creates a page-encrypted TapirusWasm vault and migrates your current tables, graph, and AI memories into it.';
    $('encrypt-submit-btn').textContent = 'Enable Encryption';
  }
  modal.showModal();
  $('encrypt-passphrase').focus();
}

async function submitEncryptForm(event) {
  event.preventDefault();
  const mode = $('modal-encrypt').dataset.mode;
  const passphrase = $('encrypt-passphrase').value;
  if (!passphrase || passphrase.length < 4) {
    $('encrypt-modal-error').textContent = 'Passphrase must be at least 4 characters.';
    $('encrypt-modal-error').hidden = false;
    return;
  }

  try {
    if (mode === 'unlock' && pendingEncryptedBytes) {
      const next = engine.TapirusWasm.import_encrypted_bytes(pendingEncryptedBytes, passphrase);
      database.free();
      database = next;
      savedName = pendingEncryptedName || 'encrypted-vault.tapir';
      $('vault-name').value = savedName;
      pendingEncryptedBytes = null;
      pendingEncryptedName = null;
      activeTable = null;
      $('modal-encrypt').close();
      await refreshAllViews();
      await persistSnapshot();
      setFooterStorage(`Unlocked encrypted vault ${savedName}`);
      return;
    }

    // Migrate current user tables, graph, and memories into a real encrypted WASM database
    const tables = getUserTables();
    const tableSnapshots = tables.map(t => ({
      schema: t,
      rows: JSON.parse(database.query_json(`SELECT * FROM ${safeIdent(t.name)}`)),
    }));
    const graphNodes = [...graphState.nodes];
    const graphEdges = [...graphState.edges];

    const enc = engine.TapirusWasm.new_encrypted(passphrase);
    for (const snap of tableSnapshots) {
      const tName = safeIdent(snap.schema.name);
      const colsSql = snap.schema.columns.map(c => {
        const dt = formatDataType(c.data_type);
        return `${safeIdent(c.name)} ${dt}${c.primary_key ? ' PRIMARY KEY' : ''}`;
      }).join(', ');
      enc.execute(`CREATE TABLE ${tName} (${colsSql})`);
      const colNames = snap.schema.columns.map(c => c.name);
      const placeholders = colNames.map(() => '?').join(', ');
      const ins = `INSERT INTO ${tName} (${colNames.map(n => safeIdent(n)).join(', ')}) VALUES (${placeholders})`;
      for (const r of snap.rows) {
        enc.execute_params(ins, JSON.stringify(colNames.map(n => r[n] ?? null)));
      }
    }
    for (const n of graphNodes) {
      enc.execute(`GRAPH INSERT NODE ${n.id} LABEL '${n.label}' PROPERTIES '${JSON.stringify(n.props).replaceAll("'", "''")}'`);
    }
    for (const e of graphEdges) {
      enc.execute(`GRAPH INSERT EDGE ${e.from} -> ${e.to} LABEL '${e.label}' WEIGHT ${e.weight}`);
    }
    for (const m of memoryJournal) {
      enc.store_memory_json(JSON.stringify(m));
    }

    database.free();
    database = enc;
    $('modal-encrypt').close();
    await refreshAllViews();
    await persistSnapshot();
    refreshXray();
    setFooterStorage(`Vault encrypted with XChaCha20-Poly1305`);
  } catch (err) {
    $('encrypt-modal-error').textContent = errorMessage(err);
    $('encrypt-modal-error').hidden = false;
  }
}

// ---- SDK CODE GENERATOR MODAL ----
function openSdkModal(kind, payload) {
  sdkContext = { kind, payload };
  renderSdkSnippet();
  $('sdk-copy-status').textContent = '';
  $('modal-code').showModal();
}

function renderSdkSnippet() {
  const { kind, payload } = sdkContext;
  const file = savedName;
  const encNote = database?.is_encrypted();
  let code = '';

  if (activeSdkLang === 'python') {
    if (kind === 'memory') {
      code = `from tapirusdb import SessionDatabase\n\n`
        + `db = SessionDatabase.open("${file}"${encNote ? ', passphrase="YOUR_PASSPHRASE"' : ''})\n`
        + `hits = db.recall(${JSON.stringify(payload || 'encrypted vaults')}, limit=5)\n`
        + `prompt_ctx = db.recall_prompt(${JSON.stringify(payload || 'encrypted vaults')}, limit=5)\n`
        + `print(prompt_ctx)`;
    } else if (kind === 'graph') {
      code = `from tapirusdb import SessionDatabase\n\n`
        + `db = SessionDatabase.open("${file}"${encNote ? ', passphrase="YOUR_PASSPHRASE"' : ''})\n`
        + `graph = db.graph_rag(${JSON.stringify(payload || 'AuthGateway')}, top_seeds=3, max_hops=2, limit=6)\n`
        + `print(graph["prompt_context"])`;
    } else {
      const sql = payload || `SELECT * FROM ${activeTable || 'customers'} LIMIT 25;`;
      code = `from tapirusdb import SessionDatabase\n\n`
        + `db = SessionDatabase.open("${file}"${encNote ? ', passphrase="YOUR_PASSPHRASE"' : ''})\n`
        + `rows = db.query("""\n${sql}\n""")\n`
        + `for row in rows:\n    print(row)`;
    }
  } else if (activeSdkLang === 'node') {
    if (kind === 'memory') {
      code = `import { open } from '@tapirus/db';\n\n`
        + `const db = await open('${file}'${encNote ? ", { passphrase: process.env.TAPIRUS_KEY }" : ''});\n`
        + `const hits = await db.searchMemories({ query: ${JSON.stringify(payload || 'encrypted vaults')}, limit: 5 });\n`
        + `const promptContext = await db.recallPrompt(${JSON.stringify(payload || 'encrypted vaults')}, 5);\n`
        + `console.log(promptContext);`;
    } else if (kind === 'graph') {
      code = `import { open } from '@tapirus/db';\n\n`
        + `const db = await open('${file}'${encNote ? ", { passphrase: process.env.TAPIRUS_KEY }" : ''});\n`
        + `const result = await db.graphRagQuery(${JSON.stringify(payload || 'AuthGateway')}, 3, 2, 6);\n`
        + `console.log(result.prompt_context);`;
    } else {
      const sql = payload || `SELECT * FROM ${activeTable || 'customers'} LIMIT 25;`;
      code = `import { open } from '@tapirus/db';\n\n`
        + `const db = await open('${file}'${encNote ? ", { passphrase: process.env.TAPIRUS_KEY }" : ''});\n`
        + `const rows = await db.query(\`\n${sql}\n\`);\n`
        + `console.table(rows);`;
    }
  } else {
    if (kind === 'memory') {
      code = `use tapirus::{Config, SessionDatabase};\n\n`
        + `let db = SessionDatabase::open_with_config("${file}", Config::default())?;\n`
        + `let prompt = db.recall_prompt(${JSON.stringify(payload || 'encrypted vaults')}, 5)?;\n`
        + `println!("{prompt}");`;
    } else if (kind === 'graph') {
      code = `use tapirus::{Config, SessionDatabase};\n\n`
        + `let db = SessionDatabase::open_with_config("${file}", Config::default())?;\n`
        + `let res = db.graph_rag_query(${JSON.stringify(payload || 'AuthGateway')}, 3, 2, 6)?;\n`
        + `println!("{}", res.prompt_context);`;
    } else {
      const sql = payload || `SELECT * FROM ${activeTable || 'customers'} LIMIT 25;`;
      code = `use tapirus::{Config, SessionDatabase};\n\n`
        + `let db = SessionDatabase::open_with_config("${file}", Config::default())?;\n`
        + `let rows = db.query(${JSON.stringify(sql)})?;\n`
        + `println!("{rows:?}");`;
    }
  }
  $('sdk-code-preview').textContent = code;
}

// ---- NEW TABLE & INSERT ROW BUILDERS ----
function addColumnBuilderRow(name = '', type = 'TEXT', isPk = false) {
  const row = document.createElement('div');
  row.className = 'col-row';
  const nameIn = document.createElement('input');
  nameIn.placeholder = 'column_name';
  nameIn.value = name;
  nameIn.required = true;

  const typeSel = document.createElement('select');
  for (const t of ['INTEGER', 'TEXT', 'REAL', 'VECTOR(4)', 'VECTOR(384)', 'BLOB']) {
    const opt = document.createElement('option');
    opt.value = t;
    opt.textContent = t;
    if (t === type) opt.selected = true;
    typeSel.appendChild(opt);
  }

  const pkLbl = document.createElement('label');
  const pkChk = document.createElement('input');
  pkChk.type = 'checkbox';
  pkChk.checked = isPk;
  pkLbl.append(pkChk, document.createTextNode('PK'));

  const rm = document.createElement('button');
  rm.type = 'button';
  rm.className = 'row-del-btn';
  rm.textContent = '×';
  rm.addEventListener('click', () => row.remove());

  row.append(nameIn, typeSel, pkLbl, rm);
  $('nt-columns').appendChild(row);
}

function openNewTableModal() {
  $('nt-name').value = '';
  $('nt-error').hidden = true;
  $('nt-columns').innerHTML = '';
  addColumnBuilderRow('id', 'INTEGER', true);
  addColumnBuilderRow('title', 'TEXT', false);
  addColumnBuilderRow('embedding', 'VECTOR(4)', false);
  $('modal-new-table').showModal();
  $('nt-name').focus();
}

function openInsertRowModal() {
  if (!activeTable || !activeTableCols.length) return;
  $('modal-row-title').textContent = `Insert Row into ${activeTable}`;
  $('ir-error').hidden = true;
  const wrap = $('insert-row-fields');
  wrap.innerHTML = '';
  for (const col of activeTableCols) {
    const dt = formatDataType(col.data_type);
    const lbl = document.createElement('label');
    lbl.className = 'modal-field';
    lbl.textContent = `${col.name} (${dt})`;
    const input = document.createElement('input');
    input.dataset.colName = col.name;
    input.dataset.colType = dt;
    if (dt === 'INTEGER') input.placeholder = 'e.g. 10';
    else if (dt === 'REAL') input.placeholder = 'e.g. 1.25';
    else if (dt.startsWith('VECTOR')) input.placeholder = '[0.5, 0.2, 0.1, 0.0]';
    else input.placeholder = `Enter ${col.name}…`;
    lbl.appendChild(input);
    wrap.appendChild(lbl);
  }
  $('modal-insert-row').showModal();
}

// ---- TAB NAVIGATION ----
function switchView(view) {
  activeView = view;
  document.querySelectorAll('.ws-tab').forEach(tab => {
    const isActive = tab.dataset.view === view;
    tab.classList.toggle('active', isActive);
    tab.setAttribute('aria-selected', String(isActive));
  });
  document.querySelectorAll('.ws-view').forEach(panel => {
    const isActive = panel.id === `view-${view}`;
    panel.classList.toggle('active', isActive);
    panel.hidden = !isActive;
  });
  if (view === 'xray') refreshXray();
  if (view === 'graph') refreshGraphCanvas();
  if (view === 'memory') refreshMemoryStats();
}

// ---- EVENT BINDINGS ----
function bindEvents() {
  // Tabs
  document.querySelectorAll('.ws-tab').forEach(tab => {
    tab.addEventListener('click', () => switchView(tab.dataset.view));
  });

  // Theme toggle
  const savedTheme = localStorage.getItem('tapirus-studio-theme');
  if (savedTheme === 'dark' || savedTheme === 'light') {
    document.documentElement.dataset.theme = savedTheme;
  }
  $('btn-theme').addEventListener('click', () => {
    const next = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
    document.documentElement.dataset.theme = next;
    localStorage.setItem('tapirus-studio-theme', next);
  });

  // Mobile sidebar toggle
  $('toggle-sidebar').addEventListener('click', () => {
    const open = $('sidebar').classList.toggle('open');
    $('toggle-sidebar').setAttribute('aria-expanded', String(open));
  });

  // Scenario switcher
  $('scenario-select').addEventListener('change', e => {
    runAction(() => loadScenario(e.target.value));
  });

  // Vault filename sync
  $('vault-name').addEventListener('change', () => {
    try {
      savedName = normalizeName($('vault-name').value);
      $('vault-name').value = savedName;
      persistSnapshot();
    } catch (err) {
      setFooterStorage(errorMessage(err));
    }
  });

  // Topbar actions
  $('btn-encrypt').addEventListener('click', () => openEncryptModal('encrypt'));
  $('encrypt-form').addEventListener('submit', submitEncryptForm);
  $('btn-import').addEventListener('click', () => $('file-input').click());
  $('sidebar-drop-btn').addEventListener('click', () => $('file-input').click());
  $('file-input').addEventListener('change', e => {
    const f = e.target.files?.[0];
    if (f) handleImportedFile(f);
    e.target.value = '';
  });
  $('btn-export').addEventListener('click', () => runAction(async () => {
    if (database.is_in_transaction()) throw new Error('COMMIT or ROLLBACK before exporting .tapir.');
    const bytes = database.export_bytes();
    const url = URL.createObjectURL(new Blob([bytes], { type: 'application/octet-stream' }));
    const a = document.createElement('a');
    a.href = url;
    a.download = savedName;
    a.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
    setFooterStorage(`Exported ${savedName} (${formatBytes(bytes.length)})`);
  }));

  // Sidebar actions
  $('btn-refresh').addEventListener('click', () => refreshAllViews());
  $('btn-save-copy').addEventListener('click', () => persistSnapshot());
  $('btn-new-table').addEventListener('click', openNewTableModal);
  $('sidebar-open-graph').addEventListener('click', () => {
    switchView('graph');
    $('sidebar').classList.remove('open');
  });

  // Table Browser controls
  $('table-filter').addEventListener('input', () => { tablePage = 1; renderTableBrowserRows(); });
  $('btn-prev-page').addEventListener('click', () => { tablePage--; renderTableBrowserRows(); });
  $('btn-next-page').addEventListener('click', () => { tablePage++; renderTableBrowserRows(); });
  $('btn-insert-row').addEventListener('click', openInsertRowModal);
  $('btn-open-table-sql').addEventListener('click', () => {
    if (!activeTable) return;
    $('sql').value = `SELECT * FROM ${safeIdent(activeTable)} LIMIT 50;`;
    switchView('sql');
    executeSqlEditor();
  });
  $('btn-export-csv').addEventListener('click', () => {
    if (!activeTable || !activeTableCols.length) return;
    const cols = activeTableCols.map(c => c.name);
    const csvLines = [cols.join(',')];
    for (const r of activeTableRows) {
      csvLines.push(cols.map(c => {
        const v = r[c] == null ? '' : (Array.isArray(r[c]) ? JSON.stringify(r[c]) : String(r[c]));
        return /[,"\n]/.test(v) ? `"${v.replaceAll('"', '""')}"` : v;
      }).join(','));
    }
    const url = URL.createObjectURL(new Blob([csvLines.join('\n')], { type: 'text/csv' }));
    const a = document.createElement('a');
    a.href = url;
    a.download = `${activeTable}.csv`;
    a.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  });
  $('btn-table-code').addEventListener('click', () => {
    openSdkModal('sql', activeTable ? `SELECT * FROM ${activeTable} LIMIT 25;` : 'SELECT * FROM customers;');
  });

  // SQL Workbench controls
  $('run-sql').addEventListener('click', () => executeSqlEditor());
  $('sql').addEventListener('keydown', e => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      e.preventDefault();
      executeSqlEditor();
    }
  });
  $('btn-explain-sql').addEventListener('click', () => {
    const first = splitSqlStatements($('sql').value)[0];
    if (first) executeSqlEditor(`EXPLAIN ${first.replace(/^EXPLAIN\s+/i, '')}`);
  });
  $('sql-snippets').addEventListener('change', e => {
    const snippet = SQL_SNIPPETS[e.target.value];
    if (snippet) {
      $('sql').value = snippet;
      executeSqlEditor(snippet);
    }
    e.target.value = '';
  });
  $('btn-tx-begin').addEventListener('click', () => executeSqlEditor('BEGIN'));
  $('btn-tx-commit').addEventListener('click', () => executeSqlEditor('COMMIT'));
  $('btn-tx-rollback').addEventListener('click', () => executeSqlEditor('ROLLBACK'));
  $('btn-sql-code').addEventListener('click', () => openSdkModal('sql', $('sql').value.trim()));
  document.querySelectorAll('[data-sql-out]').forEach(btn => {
    btn.addEventListener('click', () => setSqlOutputTab(btn.dataset.sqlOut));
  });

  // AI Memory controls
  $('memory-search-form').addEventListener('submit', e => {
    e.preventDefault();
    runMemorySearch();
  });
  $('memory-mode').addEventListener('change', () => {
    if ($('memory-query').value.trim()) runMemorySearch();
  });
  document.querySelectorAll('[data-mem-query]').forEach(btn => {
    btn.addEventListener('click', () => {
      $('memory-query').value = btn.dataset.memQuery;
      runMemorySearch(btn.dataset.memQuery);
    });
  });
  $('remember').addEventListener('click', storeNewMemory);
  $('btn-copy-prompt').addEventListener('click', async () => {
    await navigator.clipboard.writeText($('llm-prompt-preview').textContent);
    setStatus('memory-status', 'Copied LLM prompt context to clipboard.');
  });
  $('btn-memory-code').addEventListener('click', () => openSdkModal('memory', $('memory-query').value.trim() || lastMemoryQuery));

  // Graph & GraphRAG controls
  $('btn-graph-refresh').addEventListener('click', () => refreshGraphCanvas());
  $('btn-graph-add-toggle').addEventListener('click', () => {
    $('graph-add-bar').hidden = !$('graph-add-bar').hidden;
  });
  $('btn-graph-insert').addEventListener('click', () => runAction(async () => {
    const fromId = Number($('g-from-id').value);
    const fromLabel = $('g-from-label').value.trim() || 'Entity';
    const fromName = $('g-from-name').value.trim() || `Node${fromId}`;
    const edgeLabel = $('g-edge-label').value.trim().toUpperCase().replace(/[^A-Z0-9_]/g, '_') || 'LINKS_TO';
    const toId = Number($('g-to-id').value);
    if (!fromId || !toId) throw new Error('Supply valid numeric From ID and To ID.');
    database.execute(`GRAPH INSERT NODE ${fromId} LABEL '${fromLabel}' PROPERTIES '{"name":"${fromName.replaceAll('"', '')}"}'`);
    database.execute(`GRAPH INSERT EDGE ${fromId} -> ${toId} LABEL '${edgeLabel}' WEIGHT 0.90`);
    await persistSnapshot();
    refreshGraphCanvas();
    setStatus('graphrag-status', `Inserted edge :${fromLabel} #${fromId} -[${edgeLabel}]-> #${toId}.`);
  }, 'graphrag-status'));
  $('graphrag-form').addEventListener('submit', e => {
    e.preventDefault();
    runGraphRag();
  });
  document.querySelectorAll('[data-graph-seed]').forEach(btn => {
    btn.addEventListener('click', () => {
      $('graphrag-query').value = btn.dataset.graphSeed;
      runGraphRag(btn.dataset.graphSeed);
    });
  });
  $('btn-graph-code').addEventListener('click', () => openSdkModal('graph', $('graphrag-query').value.trim() || 'AuthGateway'));

  // X-Ray, RaBitQ & TAP Lite controls
  $('btn-xray-refresh').addEventListener('click', refreshXray);
  $('btn-rabitq-1').addEventListener('click', () => runRaBitQ(1));
  $('btn-rabitq-2').addEventListener('click', () => runRaBitQ(2));
  $('btn-rabitq-rand').addEventListener('click', () => {
    const v = Array.from({ length: 384 }, () => Number(((Math.random() * 2) - 1).toFixed(3)));
    $('rabitq-input').value = JSON.stringify(v);
    runRaBitQ(1);
  });
  $('tap-classify').addEventListener('click', () => runAction(() => {
    const input = $('tap-text').value.trim();
    const candidates = $('tap-candidates').value.trim();
    if (!input || !candidates) throw new Error('Enter text and candidate labels.');
    const res = JSON.parse(database.tap_classify(input, candidates));
    $('tap-classify-result').textContent = JSON.stringify(res, null, 2);
    setStatus('tap-classify-status', `Top choice: ${res.top_choice} (${(Number(res.confidence || 0) * 100).toFixed(1)}%) · ${res.latency_us ?? 0} µs`);
  }, 'tap-classify-status'));
  $('tap-verify').addEventListener('click', () => runAction(() => {
    const premise = $('tap-premise').value.trim();
    const hypothesis = $('tap-hypothesis').value.trim();
    if (!premise || !hypothesis) throw new Error('Enter both premise and hypothesis.');
    const res = JSON.parse(database.tap_verify(premise, hypothesis));
    $('tap-verify-result').textContent = JSON.stringify(res, null, 2);
    setStatus('tap-verify-status', `${res.is_verified ? 'Verified' : 'Not verified'} (score ${(Number(res.confidence || 0) * 100).toFixed(1)}%) · ${res.latency_us ?? 0} µs`);
  }, 'tap-verify-status'));

  // Modals: close buttons
  document.querySelectorAll('[data-close-modal]').forEach(btn => {
    btn.addEventListener('click', () => $(btn.dataset.closeModal)?.close());
  });

  // SDK modal tabs & copy
  document.querySelectorAll('[data-sdk-lang]').forEach(btn => {
    btn.addEventListener('click', () => {
      activeSdkLang = btn.dataset.sdkLang;
      document.querySelectorAll('[data-sdk-lang]').forEach(b => b.classList.toggle('active', b === btn));
      renderSdkSnippet();
    });
  });
  $('btn-copy-sdk').addEventListener('click', async () => {
    await navigator.clipboard.writeText($('sdk-code-preview').textContent);
    $('sdk-copy-status').textContent = 'Copied to clipboard!';
  });

  // New Table modal submit
  $('btn-add-col').addEventListener('click', () => addColumnBuilderRow('', 'TEXT', false));
  $('new-table-form').addEventListener('submit', async e => {
    e.preventDefault();
    const tableName = $('nt-name').value.trim();
    const colRows = [...$('nt-columns').querySelectorAll('.col-row')];
    if (!tableName || !colRows.length) return;
    try {
      const defs = colRows.map(r => {
        const name = r.querySelector('input:not([type="checkbox"])').value.trim();
        const type = r.querySelector('select').value;
        const pk = r.querySelector('input[type="checkbox"]').checked;
        if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) throw new Error(`Invalid column name: "${name}"`);
        return `${name} ${type}${pk ? ' PRIMARY KEY' : ''}`;
      });
      const safeName = safeIdent(tableName);
      database.execute(`CREATE TABLE ${safeName} (${defs.join(', ')})`);
      activeTable = safeName;
      $('modal-new-table').close();
      await persistSnapshot();
      switchView('data');
      refreshExplorer();
      refreshTableBrowser();
    } catch (err) {
      $('nt-error').textContent = errorMessage(err);
      $('nt-error').hidden = false;
    }
  });

  // Insert Row modal submit
  $('insert-row-form').addEventListener('submit', async e => {
    e.preventDefault();
    if (!activeTable) return;
    try {
      const inputs = [...$('insert-row-fields').querySelectorAll('input')];
      const cols = [];
      const vals = [];
      for (const inp of inputs) {
        const raw = inp.value.trim();
        cols.push(safeIdent(inp.dataset.colName));
        if (raw === '') {
          vals.push(null);
        } else if (inp.dataset.colType === 'INTEGER') {
          vals.push(parseInt(raw, 10));
        } else if (inp.dataset.colType === 'REAL') {
          vals.push(parseFloat(raw));
        } else if (inp.dataset.colType.startsWith('VECTOR')) {
          vals.push(JSON.parse(raw));
        } else {
          vals.push(raw);
        }
      }
      const placeholders = cols.map(() => '?').join(', ');
      database.execute_params(`INSERT INTO ${safeIdent(activeTable)} (${cols.join(', ')}) VALUES (${placeholders})`, JSON.stringify(vals));
      $('modal-insert-row').close();
      await persistSnapshot();
      refreshExplorer();
      refreshTableBrowser();
    } catch (err) {
      $('ir-error').textContent = errorMessage(err);
      $('ir-error').hidden = false;
    }
  });

  // Window Drag & Drop
  let dragDepth = 0;
  window.addEventListener('dragenter', e => {
    if (e.dataTransfer?.types?.includes('Files')) {
      dragDepth++;
      $('drop-overlay').hidden = false;
    }
  });
  window.addEventListener('dragover', e => {
    if (e.dataTransfer?.types?.includes('Files')) e.preventDefault();
  });
  window.addEventListener('dragleave', () => {
    dragDepth = Math.max(0, dragDepth - 1);
    if (dragDepth === 0) $('drop-overlay').hidden = true;
  });
  window.addEventListener('drop', e => {
    e.preventDefault();
    dragDepth = 0;
    $('drop-overlay').hidden = true;
    const f = e.dataTransfer?.files?.[0];
    if (f) handleImportedFile(f);
  });

  // TapisaurusDB Server & TapirSync Controls
  $('btn-server').addEventListener('click', () => {
    $('srv-error').hidden = true;
    $('modal-server').showModal();
  });
  $('server-connect-form').addEventListener('submit', async e => {
    e.preventDefault();
    await connectTapisaurusServer(
      $('srv-url').value.trim(),
      $('srv-user').value.trim(),
      $('srv-pass').value,
      $('srv-token').value.trim(),
    );
  });
  $('btn-srv-disconnect').addEventListener('click', disconnectTapisaurusServer);
  $('btn-srv-switch-remote').addEventListener('click', () => setExecutionMode('remote'));
  $('btn-srv-switch-local').addEventListener('click', () => setExecutionMode('local'));
  $('btn-mode-local').addEventListener('click', () => setExecutionMode('local'));
  $('btn-mode-remote').addEventListener('click', () => setExecutionMode('remote'));
  $('btn-tapirsync-push').addEventListener('click', pushTapirSyncToServer);
  $('btn-tapirsync-pull').addEventListener('click', pullTapirSyncFromServer);
  $('btn-quick-push').addEventListener('click', pushTapirSyncToServer);
  $('btn-quick-pull').addEventListener('click', pullTapirSyncFromServer);
}

// ---- TAPISAURUSDB SERVER & TAPIRSYNC ENGINE ----
async function serverFetch(path, options = {}) {
  const base = serverState.baseUrl.replace(/\/+$/, '');
  const headers = { 'Accept': 'application/json', ...(options.headers || {}) };
  if (options.body && !headers['Content-Type']) headers['Content-Type'] = 'application/json';
  if (serverState.token) headers['Authorization'] = `Bearer ${serverState.token}`;
  const res = await fetch(`${base}${path}`, { ...options, headers });
  const text = await res.text();
  let json = null;
  try { json = text ? JSON.parse(text) : null; } catch {}
  if (!res.ok) {
    const detail = json?.error || text || `HTTP ${res.status}`;
    throw new Error(`${path} failed (${res.status}): ${detail}`);
  }
  return json ?? text;
}

function updateServerUI() {
  const btn = $('btn-server');
  const lbl = $('server-label');
  const icon = $('server-icon');
  const treeGroup = $('server-tree-group');
  const connectedBox = $('srv-connected-box');
  const discBtn = $('btn-srv-disconnect');

  btn.classList.toggle('connected', serverState.connected);
  icon.textContent = serverState.connected ? '●' : '⚡';
  lbl.textContent = !serverState.connected
    ? 'Server'
    : (serverState.mode === 'remote' ? 'TapisaurusDB' : 'Server Ready');
  treeGroup.hidden = !serverState.connected;
  connectedBox.hidden = !serverState.connected;
  discBtn.hidden = !serverState.connected;
  $('btn-mode-local').classList.toggle('active', serverState.mode === 'local');
  $('btn-mode-remote').classList.toggle('active', serverState.mode === 'remote');
  $('btn-mode-local').disabled = !database;
  $('btn-srv-switch-local').disabled = !database;
  $('btn-tapirsync-push').disabled = !database;
  $('btn-tapirsync-pull').disabled = !database;
  $('btn-quick-push').disabled = !database;
  $('btn-quick-pull').disabled = !database;

  if (serverState.connected) {
    const ver = serverState.info?.version || '0.1.0';
    $('srv-kpi-engine').textContent = `v${ver}`;
    $('srv-kpi-raft').textContent = serverState.readiness
      ? `${serverState.readiness.raft_role || 'leader'} (T${serverState.readiness.raft_term ?? 0})`
      : 'Ready';
    $('srv-kpi-cols').textContent = String(serverState.collections.length);
    $('srv-kpi-mode').textContent = serverState.mode === 'remote' ? 'Remote Server' : 'Local WASM';
  }
}

function setExecutionMode(mode) {
  if (mode === 'local' && !database) return;
  if (mode === 'remote' && !serverState.connected) return;
  serverState.mode = mode;
  updateServerUI();
  setFooterStorage(
    mode === 'remote'
      ? `Connected to TapisaurusDB (${serverState.baseUrl})`
      : `Local .tapir workspace (${savedName})`,
  );
  if (mode === 'remote' && serverState.collections.length > 0) {
    browseServerCollection(serverState.activeCollection || serverState.collections[0].name);
  } else if (mode === 'local' && database) {
    refreshAllViews();
  }
}

async function refreshServerCollections() {
  if (!serverState.connected) return;
  const list = $('server-col-list');
  list.innerHTML = '';
  try {
    const resp = await serverFetch('/v1/collections');
    const items = Array.isArray(resp?.data) ? resp.data : [];
    serverState.collections = items;
    $('count-srv-cols').textContent = String(items.length);
    $('srv-kpi-cols').textContent = String(items.length);

    if (items.length === 0) {
      const li = document.createElement('li');
      li.className = 'tree-empty';
      li.textContent = 'No server collections yet';
      list.appendChild(li);
      return;
    }
    for (const col of items) {
      const name = typeof col === 'string' ? col : col.name;
      const cnt = typeof col === 'object' ? col.documents : '';
      const li = document.createElement('li');
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = `tree-item-btn${serverState.mode === 'remote' && serverState.activeCollection === name ? ' active' : ''}`;
      const left = document.createElement('span');
      left.className = 'tree-item-name';
      left.textContent = `⬡ ${name}`;
      const right = document.createElement('span');
      right.className = 'tree-item-meta';
      right.textContent = String(cnt);
      btn.append(left, right);
      btn.addEventListener('click', () => {
        serverState.mode = 'remote';
        updateServerUI();
        browseServerCollection(name);
        switchView('data');
        $('sidebar').classList.remove('open');
      });
      li.appendChild(btn);
      list.appendChild(li);
    }
  } catch (err) {
    const li = document.createElement('li');
    li.className = 'tree-empty';
    li.textContent = `Auth required for collections (${errorMessage(err).slice(0, 42)})`;
    list.appendChild(li);
  }
}

async function browseServerCollection(name) {
  if (!serverState.connected || !name) return;
  serverState.activeCollection = name;
  const start = performance.now();
  try {
    const resp = await serverFetch(`/v1/collections/${encodeURIComponent(name)}/documents?limit=100`);
    const docs = Array.isArray(resp?.data) ? resp.data : [];
    const elapsed = performance.now() - start;
    const cols = [...new Set(docs.flatMap(d => (d && typeof d === 'object' ? Object.keys(d) : [])))];

    activeTable = name;
    activeTableCols = cols.map(c => ({ name: c, data_type: c === '_id' ? 'TEXT' : 'ANY', primary_key: c === '_id' }));
    activeTableRows = docs;
    tablePage = 1;
    $('active-table-title').textContent = `TapisaurusDB · ${name}`;
    $('table-schema-bar').innerHTML = '';
    for (const c of activeTableCols) {
      const pill = document.createElement('span');
      pill.className = `col-pill${c.primary_key ? ' pk' : ''}`;
      const strong = document.createElement('strong');
      strong.textContent = c.name;
      const span = document.createElement('span');
      span.textContent = c.primary_key ? 'ID' : 'JSON';
      pill.append(strong, span);
      $('table-schema-bar').appendChild(pill);
    }
    $('btn-insert-row').disabled = true;
    $('btn-export-csv').disabled = docs.length === 0;
    renderTableBrowserRows(elapsed);
    setStatus('browser-status', `Loaded ${docs.length} live documents from TapisaurusDB "${name}" in ${elapsed.toFixed(2)} ms.`);
    await refreshServerCollections();
  } catch (err) {
    setStatus('browser-status', errorMessage(err), true);
  }
}

async function connectTapisaurusServer(url, username, password, manualToken) {
  $('srv-error').hidden = true;
  $('btn-srv-connect').disabled = true;
  try {
    serverState.baseUrl = url.replace(/\/+$/, '');
    serverState.token = manualToken || '';

    const health = await serverFetch('/v1/health');
    if (health?.status !== 'healthy') throw new Error('Unexpected /v1/health response');

    serverState.info = await serverFetch('/v1/info').catch(() => ({ version: health.version || '0.1.0' }));
    serverState.readiness = await serverFetch('/v1/health/readiness').catch(() => null);

    if (!serverState.token && username && password) {
      const loginResp = await serverFetch('/v1/auth/login', {
        method: 'POST',
        body: JSON.stringify({ username, password }),
      });
      serverState.token = loginResp?.data?.token || '';
      if (serverState.token) sessionStorage.setItem('tapisaurus-studio-jwt', serverState.token);
    }

    serverState.connected = true;
    if (embeddedServerMode) serverState.mode = 'remote';
    await refreshServerCollections();
    updateServerUI();
    setStatus('srv-sync-status', `Connected to ${serverState.info?.name || 'TapisaurusDB'} v${serverState.info?.version || '0.1.0'}.`);
    setFooterStorage(`Connected to TapisaurusDB (${serverState.baseUrl})`);
    if (embeddedServerMode && serverState.collections.length > 0) {
      await browseServerCollection(serverState.collections[0].name);
    }
  } catch (err) {
    $('srv-error').textContent = errorMessage(err);
    $('srv-error').hidden = false;
  } finally {
    $('btn-srv-connect').disabled = false;
  }
}

function disconnectTapisaurusServer() {
  serverState.connected = false;
  serverState.mode = database ? 'local' : 'remote';
  serverState.token = '';
  serverState.collections = [];
  serverState.activeCollection = null;
  sessionStorage.removeItem('tapisaurus-studio-jwt');
  updateServerUI();
  setFooterStorage(database ? `Disconnected server · Local ${savedName}` : 'Disconnected from TapisaurusDB');
  if (database) refreshAllViews();
}

async function pushTapirSyncToServer() {
  if (!serverState.connected || !database) return;
  try {
    setStatus('srv-sync-status', 'Packaging local .tapir tables, graph, and memories for TapirSync…');
    const tables = getUserTables().map(t => ({
      table: t.name,
      rows: JSON.parse(database.query_json(`SELECT * FROM ${safeIdent(t.name)}`)),
    }));
    const payload = {
      source_node_id: `studio-browser-${savedName}`,
      tables,
      graph_nodes: graphState.nodes.map(n => ({ id: String(n.id), label: n.label, properties: n.props })),
      graph_edges: graphState.edges.map(e => ({
        id: String(e.id),
        from_id: String(e.from),
        to_id: String(e.to),
        relation: e.label,
        weight: e.weight,
        properties: {},
      })),
      memories: memoryJournal.map((m, idx) => ({
        memory_id: idx + 1,
        content: m.content,
        importance: m.importance ?? 0.5,
        tags: m.tags ?? [],
        namespace: m.namespace ?? null,
        session_id: m.session_id ?? null,
      })),
    };

    const start = performance.now();
    const resp = await serverFetch('/v1/tapirsync/sync-tables', {
      method: 'POST',
      body: JSON.stringify(payload),
    });
    const ack = resp?.data || {};
    const ms = (performance.now() - start).toFixed(2);
    await refreshServerCollections();
    const msg = `TapirSync Push OK in ${ms} ms · ${ack.records_applied ?? 0} applied, ${ack.conflicts_resolved ?? 0} conflicts resolved (seq #${ack.applied_sequence ?? 0}).`;
    setStatus('srv-sync-status', msg);
    setFooterStorage(msg);
  } catch (err) {
    setStatus('srv-sync-status', `TapirSync Push error: ${errorMessage(err)}`, true);
  }
}

async function pullTapirSyncFromServer() {
  if (!serverState.connected || !database) return;
  try {
    setStatus('srv-sync-status', 'Pulling TapirSync bundle from TapisaurusDB…');
    const start = performance.now();
    const resp = await serverFetch('/v1/tapirsync/pull', {
      method: 'POST',
      body: JSON.stringify({}),
    });
    const bundle = resp?.data || {};
    const records = Array.isArray(bundle.records) ? bundle.records : [];
    const grouped = new Map();
    let pulledGraph = 0;
    let pulledMem = 0;

    for (const rec of records) {
      if (rec.DocumentUpsert) {
        const { collection, doc_id, document } = rec.DocumentUpsert;
        const tbl = safeIdent(collection);
        if (!grouped.has(tbl)) grouped.set(tbl, []);
        grouped.get(tbl).push({ _id: doc_id, ...(document || {}) });
      } else if (rec.TableRowUpsert) {
        const { table, row_id, columns } = rec.TableRowUpsert;
        const tbl = safeIdent(table);
        if (!grouped.has(tbl)) grouped.set(tbl, []);
        grouped.get(tbl).push({ _row_id: row_id, ...(columns || {}) });
      } else if (rec.GraphVertexUpsert) {
        const v = rec.GraphVertexUpsert;
        const numId = parseInt(String(v.vertex_id).replace(/\D+/g, ''), 10) || (100 + pulledGraph + 1);
        database.execute(`GRAPH INSERT NODE ${numId} LABEL '${safeIdent(v.label)}' PROPERTIES '${JSON.stringify(v.properties || {}).replaceAll("'", "''")}'`);
        pulledGraph++;
      } else if (rec.AgentMemoryUpsert) {
        const m = rec.AgentMemoryUpsert;
        database.store_memory_json(JSON.stringify({
          content: m.content,
          namespace: m.namespace || 'synced',
          session_id: m.session_id || undefined,
          importance: m.importance ?? 0.7,
          tags: m.tags || [],
        }));
        knownNamespaces.add(m.namespace || 'synced');
        pulledMem++;
      }
    }

    for (const [tbl, rows] of grouped.entries()) {
      if (!rows.length) continue;
      const rawHeaders = [...new Set(rows.flatMap(o => Object.keys(o)))];
      const headers = rawHeaders.map((h, i) => safeIdent(h || `col_${i + 1}`));
      const colTypes = rawHeaders.map(h => inferColumnType(rows.map(o => o[h])));
      const ddlCols = headers.map((h, i) => `${h} ${colTypes[i]}`).join(', ');
      database.execute(`DROP TABLE IF EXISTS ${tbl}`);
      database.execute(`CREATE TABLE ${tbl} (${ddlCols})`);
      const placeholders = headers.map(() => '?').join(', ');
      const ins = `INSERT INTO ${tbl} VALUES (${placeholders})`;
      for (const obj of rows) {
        const vals = rawHeaders.map(h => {
          const v = obj[h];
          return v && typeof v === 'object' ? JSON.stringify(v) : (v ?? null);
        });
        database.execute_params(ins, JSON.stringify(vals));
      }
    }

    const ms = (performance.now() - start).toFixed(2);
    serverState.mode = 'local';
    await refreshAllViews();
    await persistSnapshot();
    updateServerUI();
    const msg = `TapirSync Pull OK in ${ms} ms · ${grouped.size} tables, ${pulledGraph} graph nodes, ${pulledMem} memories materialized locally.`;
    setStatus('srv-sync-status', msg);
    setFooterStorage(msg);
  } catch (err) {
    setStatus('srv-sync-status', `TapirSync Pull error: ${errorMessage(err)}`, true);
  }
}

// ---- BOOTSTRAP ----
bindEvents();

if (embeddedServerMode) {
  // Running embedded directly inside `tapisaurus serve` (/studio/)
  serverState.baseUrl = window.location.origin;
  $('srv-url').value = window.location.origin;
  $('scenario-select').hidden = true;
  $('vault-name').value = 'tapisaurus-live-server';
  $('btn-encrypt').hidden = true;
  $('btn-export').hidden = true;
  const savedJwt = sessionStorage.getItem('tapisaurus-studio-jwt') || '';
  if (savedJwt) $('srv-token').value = savedJwt;
  try {
    const health = await serverFetch('/v1/health');
    serverState.info = await serverFetch('/v1/info').catch(() => ({ version: health.version || '0.1.0' }));
    serverState.readiness = await serverFetch('/v1/health/readiness').catch(() => null);
    serverState.connected = true;
    serverState.mode = 'remote';
    $('engine').textContent = `TapisaurusDB v${serverState.info?.version || '0.1.0'}`;
    $('engine').dataset.engineReady = 'true';
    updateServerUI();
    if (savedJwt) {
      serverState.token = savedJwt;
      await refreshServerCollections();
      if (serverState.collections.length > 0) await browseServerCollection(serverState.collections[0].name);
    } else {
      setFooterStorage(`Connected to ${window.location.origin} — sign in via Server to browse collections & run SQL.`);
      $('modal-server').showModal();
    }
  } catch (err) {
    setStatus('engine', `Server error: ${errorMessage(err)}`, true);
  }
} else {
  try {
    const moduleUrl = new URL(document.querySelector('meta[name="tapirus-engine"]').content, document.baseURI);
    engine = await import(moduleUrl.href);
    await engine.default();

    $('engine').textContent = `WASM v${engine.TapirusWasm.version()}`;
    $('engine').dataset.engineReady = 'true';

    // Always boot with an immediately useful pre-loaded interactive dataset
    // while preserving any saved browser databases in the Explorer sidebar!
    await loadScenario('ai-support');
    await refreshSavedDbList();
  } catch (err) {
    setStatus('engine', `WASM error: ${errorMessage(err)}`, true);
    setFooterStorage(`Engine failed to initialize: ${errorMessage(err)}`);
  }
}
