#!/usr/bin/env python3
"""
End-to-End ORM Introspection Verification Harness (Port 5432 & Port 3306).

Launches a real TapisaurusDB server and runs live schema reflection via:
  1. SQLAlchemy `sqlalchemy.inspect(engine)` on PostgreSQL (`5432`) and MySQL (`3306`)
  2. Prisma CLI `prisma db pull` on PostgreSQL (`5432`) and MySQL (`3306`)
"""

import argparse
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time


def wait_for_port(host: str, port: int, timeout_s: float = 15.0) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return
        except OSError:
            time.sleep(0.15)
    raise RuntimeError(f"Timed out waiting for {host}:{port} to become ready")


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def seed_relational_ddl(pg_url: str) -> None:
    from sqlalchemy import create_engine, text

    engine = create_engine(pg_url)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE users (
                    id BIGINT PRIMARY KEY,
                    email VARCHAR(255) NOT NULL UNIQUE,
                    active BOOLEAN NOT NULL DEFAULT true
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE orders (
                    id BIGINT PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    total NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
                    status TEXT NOT NULL DEFAULT 'pending'
                )
                """
            )
        )
        conn.execute(text("CREATE INDEX idx_orders_user ON orders (user_id)"))
    engine.dispose()


def verify_sqlalchemy(dialect_name: str, db_url: str) -> None:
    from sqlalchemy import create_engine, inspect

    print(f"[ORM-CI] Verifying sqlalchemy.inspect(engine) for {dialect_name}...")
    engine = create_engine(db_url)
    inspector = inspect(engine)

    tables = set(inspector.get_table_names())
    assert "users" in tables and "orders" in tables, f"Unexpected tables for {dialect_name}: {tables}"

    order_cols = {c["name"]: c for c in inspector.get_columns("orders")}
    for required in ("id", "user_id", "total", "status"):
        assert required in order_cols, f"Missing column '{required}' in {dialect_name}: {order_cols.keys()}"
    assert order_cols["user_id"]["nullable"] is False, f"Expected user_id NOT NULL in {dialect_name}"

    pk = inspector.get_pk_constraint("orders")
    assert pk.get("constrained_columns") == ["id"], f"Unexpected PK for {dialect_name}: {pk}"

    fks = inspector.get_foreign_keys("orders")
    assert any(
        fk.get("referred_table") == "users"
        and fk.get("constrained_columns") == ["user_id"]
        and fk.get("referred_columns") == ["id"]
        for fk in fks
    ), f"Missing FK orders.user_id -> users.id in {dialect_name}: {fks}"

    indexes = inspector.get_indexes("orders")
    assert any(
        "user_id" in idx.get("column_names", []) for idx in indexes
    ), f"Missing index on orders.user_id in {dialect_name}: {indexes}"

    engine.dispose()
    print(f"[ORM-CI] PASS sqlalchemy.inspect ({dialect_name})")


def verify_prisma_db_pull(provider: str, db_url: str, work_dir: str) -> None:
    prisma_bin = shutil.which("prisma") or shutil.which("npx")
    if prisma_bin is None:
        raise RuntimeError("Neither 'prisma' nor 'npx' found in PATH")

    proj_dir = os.path.join(work_dir, f"prisma_{provider}")
    os.makedirs(os.path.join(proj_dir, "prisma"), exist_ok=True)
    schema_path = os.path.join(proj_dir, "prisma", "schema.prisma")

    schema_content = f"""datasource db {{
  provider = "{provider}"
  url      = env("DATABASE_URL")
}}

generator client {{
  provider = "prisma-client-js"
}}
"""
    with open(schema_path, "w", encoding="utf-8") as f:
        f.write(schema_content)

    cmd = (
        ["prisma", "db", "pull", "--schema", schema_path]
        if shutil.which("prisma")
        else ["npx", "--yes", "prisma", "db", "pull", "--schema", schema_path]
    )
    env = os.environ.copy()
    env["DATABASE_URL"] = db_url

    print(f"[ORM-CI] Verifying prisma db pull ({provider})...")
    proc = subprocess.run(cmd, cwd=proj_dir, env=env, capture_output=True, text=True, timeout=120)
    if proc.returncode != 0:
        raise RuntimeError(
            f"prisma db pull failed ({provider}):\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )

    with open(schema_path, "r", encoding="utf-8") as f:
        pulled = f.read()

    assert "model users" in pulled, f"Prisma output missing 'model users' ({provider}):\n{pulled}"
    assert "model orders" in pulled, f"Prisma output missing 'model orders' ({provider}):\n{pulled}"
    assert "@relation" in pulled, f"Prisma output missing FK '@relation' ({provider}):\n{pulled}"
    print(f"[ORM-CI] PASS prisma db pull ({provider})")


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify SQLAlchemy & Prisma introspection against TapisaurusDB")
    parser.add_argument("--binary", required=True, help="Path to compiled tapisaurus binary")
    parser.add_argument("--pg-port", type=int, default=5432, help="PostgreSQL wire port")
    parser.add_argument("--mysql-port", type=int, default=3306, help="MySQL wire port")
    parser.add_argument("--skip-prisma", action="store_true", help="Only run SQLAlchemy inspection checks")
    args = parser.parse_args()

    tmp_dir = tempfile.mkdtemp(prefix="tapisaurus_orm_ci_")
    data_dir = os.path.join(tmp_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    http_port = free_port()
    wire_port = free_port()
    grpc_port = free_port()

    env = os.environ.copy()
    env["TAPISAURUS_DATA_DIR"] = data_dir
    env["TAPISAURUS_NO_AUTH"] = "1"
    env.setdefault("TAPISAURUS_JWT_SECRET", "ci-orm-verification-jwt-secret-key-minimum-32chars")

    server_cmd = [
        args.binary,
        "serve",
        "--host",
        "127.0.0.1",
        "--pg-port",
        str(args.pg_port),
        "--mysql-port",
        str(args.mysql_port),
        "--http-port",
        str(http_port),
        "--wire-port",
        str(wire_port),
        "--grpc-port",
        str(grpc_port),
    ]

    print(f"[ORM-CI] Launching server: {' '.join(server_cmd)}")
    proc = subprocess.Popen(
        server_cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        wait_for_port("127.0.0.1", args.pg_port)
        wait_for_port("127.0.0.1", args.mysql_port)

        pg_sa_url = f"postgresql+psycopg2://postgres:tapisaurus@127.0.0.1:{args.pg_port}/tapisaurus"
        mysql_sa_url = f"mysql+pymysql://root@127.0.0.1:{args.mysql_port}/tapisaurus"

        seed_relational_ddl(pg_sa_url)
        verify_sqlalchemy("postgresql", pg_sa_url)
        verify_sqlalchemy("mysql", mysql_sa_url)

        if not args.skip_prisma:
            verify_prisma_db_pull(
                "postgresql",
                f"postgresql://postgres:tapisaurus@127.0.0.1:{args.pg_port}/tapisaurus?schema=public",
                tmp_dir,
            )
            verify_prisma_db_pull(
                "mysql",
                f"mysql://root@127.0.0.1:{args.mysql_port}/tapisaurus",
                tmp_dir,
            )

        print("[ORM-CI] All live ORM introspection checks passed.")
        return 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())

