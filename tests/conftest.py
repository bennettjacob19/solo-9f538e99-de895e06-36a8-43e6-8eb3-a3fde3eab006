"""Shared fixtures: a throwaway PostgreSQL cluster and an in-process API client.

The suite talks to real PostgreSQL (no mocks), because the behaviour under test is
catalog-level: invalid index husks, concurrent CREATE INDEX, advisory locks.
"""

from __future__ import annotations

import asyncio
import tempfile
import time
from contextlib import asynccontextmanager
from pathlib import Path

import pgserver
import psycopg
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient


@pytest.fixture(scope="session")
def pgdata_dir():
    return Path(tempfile.mkdtemp(prefix="audit-pg-"))


@pytest.fixture(scope="session")
def dsn(pgdata_dir):
    server = pgserver.get_server(pgdata_dir, cleanup_mode="delete")
    yield server.get_uri()


@pytest.fixture()
def schema(dsn):
    import uuid

    name = f"test_{uuid.uuid4().hex[:10]}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{name}"')
    yield name
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA "{name}" CASCADE')


@pytest.fixture()
def sql(dsn, schema):
    """Synchronous psycopg connection for assertions and fault injection."""
    conn = psycopg.connect(dsn, autocommit=True, row_factory=psycopg.rows.dict_row)
    conn.execute(f'SET search_path TO "{schema}"')
    yield conn
    conn.close()


def make_app(monkeypatch, dsn: str, schema: str):
    from app.config import Settings
    import app.config as config_module
    import app.db as db_module
    import app.indexer as indexer_module

    settings = Settings(
        database_url=dsn,
        db_schema=schema,
        events_table="audit_events",
        build_timeout_seconds=600,
        supervisor_interval_seconds=0.2,
        reconcile_grace_seconds=2,
    )
    monkeypatch.setattr(config_module, "settings", settings)
    monkeypatch.setattr(db_module, "settings", settings)
    monkeypatch.setattr(indexer_module, "settings", settings)

    from app.main import app

    return app


@asynccontextmanager
async def lifespan_client(app):
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


@pytest_asyncio.fixture()
async def client(monkeypatch, dsn, schema):
    app = make_app(monkeypatch, dsn, schema)
    async with lifespan_client(app) as c:
        yield c


# --------------------------------------------------------------------------- helpers


async def wait_for_status(client: AsyncClient, task_id: str, timeout: float = 45.0):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        r = await client.get(f"/index-tasks/{task_id}")
        assert r.status_code == 200, r.text
        last = r.json()
        if last["status"] in {"succeeded", "failed"}:
            return last
        await asyncio.sleep(0.05)
    raise AssertionError(f"task {task_id} did not finish, last={last}")


def index_catalog(sql, index_name: str):
    return sql.execute(
        """
        SELECT i.indisvalid AS valid, i.indisready AS ready, i.indislive AS live,
               c.relkind, t.relname AS table_name
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid
        JOIN pg_class t ON t.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = %s AND n.nspname = current_schema()
        """,
        (index_name,),
    ).fetchone()


def force_invalid(sql, index_name: str) -> None:
    """Simulate the catalog state left behind by an interrupted CREATE INDEX
    CONCURRENTLY (indisvalid=false). Requires superuser, which the test cluster is."""
    sql.execute(
        "UPDATE pg_index SET indisvalid = false, indisready = false "
        "WHERE indexrelid = %s::regclass",
        (index_name,),
    )
