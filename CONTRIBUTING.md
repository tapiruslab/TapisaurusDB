# Contributing to TapisaurusDB

Thank you for considering contributing to **TapisaurusDB**! We welcome contributions of all kinds — bug fixes, new features, documentation, benchmarks, and more.

---

## Code of Conduct

Be respectful. We follow the [Contributor Covenant Code of Conduct](https://www.contributor-covenant.org/).

---

## How to Contribute

### 1. Fork & Clone
```sh
git clone https://github.com/tapiruslab/TapisaurusDB.git
cd tapisaurus
```

### 2. Create a Branch
```sh
git checkout -b feat/your-feature-name
# or
git checkout -b fix/your-bug-description
```

### 3. Build & Test
```sh
cargo build --all
cargo test --all
cargo clippy --all-targets -- -D warnings
cargo fmt --all
```

All CI checks must pass before a PR is reviewed.

### 4. Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(vector): add product quantization (PQ) support for HNSW
fix(wal): handle partial writes on crash recovery
docs(readme): update benchmark results for v0.2.0
chore(deps): bump tokio to 1.53
```

### 5. Open a Pull Request

- Target the `main` branch
- Write a clear PR description explaining **why** the change is needed
- Link to any relevant issues (`Closes #123`)
- PRs require at least one approval before merging

---

## Development Setup

### Prerequisites
- Rust 1.88+ (`rustup update stable`)
- Docker (for running integration tests against the full server)

### Running the Server Locally
```sh
cargo run --bin tapisaurus-server
```

### Running Benchmarks
```sh
cargo bench --all
```

### Generating Documentation
```sh
cargo doc --all --no-deps --open
```

---

## Architecture Guide

| Crate | Purpose |
|:------|:--------|
| `tapisaurus-core` | LSM-Tree storage engine, WAL, MemTable, SSTable, Raft, CRDT |
| `tapisaurus-query` | SQL/MongoDB/TapisaurusQL parser and executor |
| `tapisaurus-vector` | HNSW vector index with multiple distance metrics |
| `tapisaurus-graph` | Adjacency-list property graph engine |
| `tapisaurus-security` | Argon2id auth, EdDSA JWT, AES-256-GCM encryption |
| `tapisaurus-server` | Axum REST/WS + MongoDB Wire + PostgreSQL Wire + gRPC |
| `tapisaurus-cli` | REPL and benchmark tool |

---

## Reporting Bugs

Open an [Issue](https://github.com/tapiruslab/TapisaurusDB/issues/new) with:
- TapisaurusDB version (`tapisaurus-cli --version`)
- OS and Rust version
- Steps to reproduce
- Expected vs. actual behaviour
- Stack trace (if applicable)

---

## Security Issues

**Do not open public Issues for security vulnerabilities.**
See [SECURITY.md](./SECURITY.md) for the responsible disclosure process.
