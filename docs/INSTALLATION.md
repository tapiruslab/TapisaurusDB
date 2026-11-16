# 🚀 TapisaurusDB — Universal Multi-Platform Installation Guide

This guide covers installing and deploying **TapisaurusDB** across Linux servers, macOS workstations, Windows PCs, and containerized Docker environments.

---

## ⚡ Quick 1-Line Installers

### 1. Linux & macOS (Apple Silicon & Intel)
Open your terminal and run:

```bash
curl -fsSL https://raw.githubusercontent.com/tapiruslab/TapisaurusDB/main/scripts/install.sh | bash
```

* **What it does:**
  - Auto-detects OS (`Linux` / `macOS`) and CPU architecture (`x86_64` / `aarch64` / `arm64`).
  - Installs the optimized `tapisaurus` binary to `/usr/local/bin` (or `~/.local/bin`).
  - Configures shell `PATH` in `.bashrc` / `.zshrc`.
  - On Linux servers (with root/sudo), automatically sets up and starts the **systemd 24/7 background service**.

---

### 2. Windows 10 / 11 & Windows Server
Open **PowerShell** (Run as Administrator for system-wide access) and execute:

```powershell
iwr -useb https://raw.githubusercontent.com/tapiruslab/TapisaurusDB/main/scripts/install.ps1 | iex
```

* **What it does:**
  - Installs `tapisaurus.exe` to `$HOME\.tapisaurus\bin`.
  - Adds TapisaurusDB permanently to your User `PATH` environment variable.

---

### 3. Docker & Containerized Environments

Deploy TapisaurusDB Server + Web Management Studio in one command:

```bash
# Clone repository
git clone https://github.com/tapiruslab/TapisaurusDB.git
cd tapisaurus

# Launch complete stack via Docker Compose
docker compose up -d
```

* **Exposed Endpoints:**
  - **MySQL Wire Protocol**: `mysql -h localhost -P 3306 -u root tapisaurus`
  - **PostgreSQL Wire Protocol**: `psql -h localhost -p 5432 -U postgres -d tapisaurus`
  - **MongoDB Wire Protocol**: `mongodb://localhost:27017`
  - **gRPC / Protocol Buffers**: `localhost:50051`
  - **HTTP REST & WebSocket API**: `http://localhost:27018`
  - **TapisaurusDB Web Studio**: `http://localhost:27020`

---

## 🛠️ Linux Production Daemon Setup (`systemd`)

For production Linux servers (Ubuntu, Debian, RHEL, CentOS), TapisaurusDB can be run as a managed system daemon:

### Create Systemd Service File
```bash
sudo tee /etc/systemd/system/tapisaurus.service <<EOF
[Unit]
Description=TapisaurusDB AI-Native NoSQL Database Server
After=network.target

[Service]
Type=simple
User=root
ExecStart=/usr/local/bin/tapisaurus serve --host 0.0.0.0 --mysql-port 3306 --pg-port 5432 --mongo-port 27017 --http-port 27018 --grpc-port 50051 --data-dir /var/lib/tapisaurus
Restart=always
RestartSec=3
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF
```

### Manage TapisaurusDB Service
```bash
# Reload daemon definitions
sudo systemctl daemon-reload

# Start TapisaurusDB
sudo systemctl start tapisaurus

# Enable auto-start on server reboot
sudo systemctl enable tapisaurus

# Check live server status
sudo systemctl status tapisaurus

# View real-time logs
sudo journalctl -u tapisaurus -f
```

---

## 🏗️ Build from Source (Any OS with Rust)

If you prefer building from source:

```bash
# 1. Install Rust toolchain (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# 2. Clone repository
git clone https://github.com/tapiruslab/TapisaurusDB.git
cd tapisaurus

# 3. Build optimized release binary
cargo build --release

# 4. Install globally
sudo cp target/release/tapisaurus /usr/local/bin/
```

---

## 🌐 Verify Installation

After installation, verify that TapisaurusDB is ready:

```bash
# Check version and engine information
tapisaurus --version
tapisaurus info

# Start interactive multi-dialect shell
tapisaurus shell

# Test HTTP Health endpoint
curl http://localhost:27018/v1/health
```

Output:
```json
{
  "status": "healthy",
  "engine": "TapisaurusDB",
  "version": "0.1.0"
}
```

---

---

## 🪶 Embedded & IoT / Edge Mode (Zero-Dependency SQLite-Style In-Process DB)

For lightweight, embedded, or edge environments (Raspberry Pi, IoT gateways, smart TVs, desktop tools, Android/iOS apps) where you do **not** want a separate background server daemon or open network ports:

### 1. Embedded in Rust Applications (`Cargo.toml`)
Add `tapisaurus-core` directly to your Rust project:

```toml
[dependencies]
tapisaurus-core = { git = "https://github.com/tapiruslab/TapisaurusDB.git" }
```

```rust
use tapisaurus_core::storage::engine::{StorageConfig, StorageEngine};
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = StorageEngine::open(StorageConfig {
        data_dir: PathBuf::from("./iot_data"),
        memtable_size: 4 * 1024 * 1024, // 4MB RAM footprint for constrained devices
        sync_writes: false,
        enable_wal: true,
    })?;

    // Direct in-process reads/writes (Sub-microsecond latency)
    db.put(b"sensor:01", b"{\"temp\": 28.5}")?;
    let val = db.get(b"sensor:01")?;
    println!("Value: {:?}", val);
    Ok(())
}
```

### 2. Standalone Static Binary for IoT (Zero Glibc Dependencies)
Download or cross-compile standalone static MUSL binaries for ARM / Raspberry Pi:

```bash
# Cross-compile static MUSL binary for ARM64 (Raspberry Pi 4/5, Orange Pi):
cross build --target aarch64-unknown-linux-musl --release -p tapisaurus-cli

# Cross-compile for ARMv7 (Raspberry Pi 2/3, 32-bit IoT boards):
cross build --target armv7-unknown-linux-musleabihf --release -p tapisaurus-cli
```

### 3. Mobile Library Compilation (Android NDK & iOS XCFramework)
```bash
# Android ARM64 (.so shared object):
cargo build --target aarch64-linux-android --release -p tapisaurus-core

# iOS Static Framework (.xcframework):
cargo build --target aarch64-apple-ios --release -p tapisaurus-core
```

---

## 🌐 Official Download Channels

| Artifact | Source / Method | Target Use Case |
|:---|:---|:---|
| **Server Binaries (.tar.gz / .zip)** | [**GitHub Releases**](https://github.com/tapiruslab/TapisaurusDB/releases) | Linux, macOS, Windows Production Servers |
| **Rust In-Process Crate** | `cargo add tapisaurus-core` / GitHub | Embedded Desktop, CLI, & In-Process Applications |
| **Python SDK** | `pip install .` / `pyproject.toml` | Python AI / Microservice Apps |
| **Node.js / TypeScript SDK** | `npm install ./bindings/node` | TypeScript backend microservices |
| **Docker Image** | `docker pull tapiruslab/TapisaurusDB:latest` | Kubernetes, Docker Compose, Cloud Clusters |
| **1-Line Shell Script** | `curl -fsSL ... | bash` | Fast automated terminal setup |

---

## 🔒 5-Way Universal Protocol Port Configuration Summary

| Port | Protocol | Usage |
|:---:|:---:|:---|
| **3306** | **🐬 MySQL / MariaDB Wire Protocol** | Native wire connection for MySQL CLI, PHP `mysqli`/PDO, Laravel Eloquent |
| **5432 / 5433** | **🐘 PostgreSQL Wire Protocol** | Native wire connection for `psql`, DBeaver, TablePlus, Grafana, SQL ORMs |
| **27017** | **🍃 MongoDB Wire Protocol** | Native wire connection for PyMongo, Mongoose, Prisma, PHP |
| **50051** | **⚡ gRPC & Protocol Buffers** | Ultra low-latency microservices and high-throughput Vector ANN streaming |
| **27018** | **🌐 HTTP REST & WebSockets** | TapisaurusQL queries, Change Streams, Health check, Backups, Geo-Replication |
| **27020** | **🎛️ HTTP (TapisaurusDB Studio)** | Visual Management Dashboard & Query Explorer |
