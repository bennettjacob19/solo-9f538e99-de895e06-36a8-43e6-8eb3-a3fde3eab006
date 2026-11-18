"""Behaviour guarantees for the database-assigned per-tenant sequence ``tenant_seq``.

The suite talks to the same throwaway PostgreSQL cluster as the rest of the tests; the
behaviour under test lives in triggers, a counter table and the migration transaction, so
mocks would prove nothing.
"""

from __future__ import annotations

import threading

import psycopg
import pytest

from tests.conftest import lifespan_client, make_app

COUNTER = "audit_events_tenant_seq"


# --------------------------------------------------------------------------- assignment


async def test_single_insert_returns_database_assigned_seq(client):
    first = await client.post(
        "/events", json={"tenant_id": "acme", "event_type": "login"}
    )
    assert first.status_code == 201, first.text
    assert first.json()["tenant_seq"] == 1

    second = await client.post(
        "/events", json={"tenant_id": "acme", "event_type": "logout"}
    )
    assert second.status_code == 201
    assert second.json()["tenant_seq"] == 2

    # Sequences are per tenant: a new tenant restarts at 1.
    other = await client.post(
        "/events", json={"tenant_id": "globex", "event_type": "login"}
    )
    assert other.json()["tenant_seq"] == 1


async def test_batch_assigns_contiguous_seq_and_returns_each(client):
    r = await client.post(
        "/events/batch",
        json=[
            {"tenant_id": "t", "event_type": "a"},
            {"tenant_id": "t", "event_type": "b"},
            {"tenant_id": "u", "event_type": "c"},
            {"tenant_id": "t", "event_type": "d"},
        ],
    )
    assert r.status_code == 201, r.text
    rows = r.json()
    assert [row["event_type"] for row in rows] == ["a", "b", "c", "d"]
    by_tenant = {}
    for row in rows:
        by_tenant.setdefault(row["tenant_id"], []).append(row["tenant_seq"])
    assert by_tenant == {"t": [1, 2, 3], "u": [1]}


async def test_plain_sql_insert_is_sequenced_by_the_database(client, sql):
    """Writes that bypass the API entirely still get a contiguous tenant_seq."""
    sql.execute(
        "INSERT INTO audit_events (tenant_id, event_type) VALUES (%s, %s), (%s, %s)",
        ("raw", "x", "raw", "y"),
    )
    rows = sql.execute(
        "SELECT tenant_seq FROM audit_events WHERE tenant_id='raw' ORDER BY id"
    ).fetchall()
    assert [r["tenant_seq"] for r in rows] == [1, 2]

    # An API insert for the same tenant continues the numbering.
    r = await client.post("/events", json={"tenant_id": "raw", "event_type": "z"})
    assert r.json()["tenant_seq"] == 3


# --------------------------------------------------------------------------- rejects / immutability


async def test_tenant_seq_is_rejected_in_write_payloads(client):
    r = await client.post(
        "/events",
        json={"tenant_id": "acme", "event_type": "login", "tenant_seq": 99},
    )
    assert r.status_code == 422, r.text


async def test_manual_tenant_seq_via_sql_is_rejected(client, sql):
    with pytest.raises(psycopg.errors.CheckViolation):
        sql.execute(
            "INSERT INTO audit_events (tenant_id, event_type, tenant_seq) "
            "VALUES ('acme', 'login', 5)"
        )

    # The rejected insert consumed no number.
    r = await client.post("/events", json={"tenant_id": "acme", "event_type": "login"})
    assert r.json()["tenant_seq"] == 1


async def test_tenant_id_and_tenant_seq_are_immutable(client, sql):
    r = await client.post("/events", json={"tenant_id": "acme", "event_type": "login"})
    event_id = r.json()["id"]

    with pytest.raises(psycopg.errors.CheckViolation):
        sql.execute(
            "UPDATE audit_events SET tenant_id='other' WHERE id=%s", (event_id,)
        )
    with pytest.raises(psycopg.errors.CheckViolation):
        sql.execute(
            "UPDATE audit_events SET tenant_seq=99 WHERE id=%s", (event_id,)
        )

    # Ordinary column updates remain allowed.
    sql.execute(
        "UPDATE audit_events SET payload = %s WHERE id=%s", ('{"ok": true}', event_id)
    )
    row = sql.execute(
        "SELECT tenant_id, tenant_seq, payload FROM audit_events WHERE id=%s",
        (event_id,),
    ).fetchone()
    assert row["tenant_id"] == "acme"
    assert row["tenant_seq"] == 1


# --------------------------------------------------------------------------- rollback / deletion


async def test_rolled_back_transaction_consumes_no_number(client, dsn, schema, sql):
    table = f'"{schema}".audit_events'
    with psycopg.connect(dsn) as conn:
        try:
            with conn.transaction():
                conn.execute(
                    f"INSERT INTO {table} (tenant_id, event_type) VALUES ('roll', 'a')"
                )
                conn.execute(
                    f"INSERT INTO {table} (tenant_id, event_type) VALUES ('roll', 'b')"
                )
                # Abort the whole transaction: both counter increments roll back too.
                conn.execute("SELECT pg_terminate_backend(pg_backend_pid())")
        except psycopg.errors.AdminShutdown:
            pass

    # The session above was killed; the table must show no rows for that tenant and the
    # counter row must not exist.
    assert sql.execute(
        "SELECT count(*) AS n FROM audit_events WHERE tenant_id='roll'"
    ).fetchone()["n"] == 0
    assert sql.execute(
        f"SELECT count(*) AS n FROM {COUNTER} WHERE tenant_id='roll'"
    ).fetchone()["n"] == 0

    r = await client.post("/events", json={"tenant_id": "roll", "event_type": "c"})
    assert r.json()["tenant_seq"] == 1


async def test_failed_batch_is_atomic_and_leaves_no_gap(client):
    ok = await client.post(
        "/events/batch",
        json=[{"tenant_id": "gap", "event_type": "a"}],
    )
    assert ok.status_code == 201
    assert ok.json()[0]["tenant_seq"] == 1

    # tenant_seq cannot be supplied; the whole batch must fail together.
    bad = await client.post(
        "/events/batch",
        json=[
            {"tenant_id": "gap", "event_type": "b"},
            {"tenant_id": "gap", "event_type": "c", "tenant_seq": 5},
        ],
    )
    assert bad.status_code == 422, bad.text

    again = await client.post(
        "/events", json={"tenant_id": "gap", "event_type": "d"}
    )
    # No number was consumed by the rolled-back batch.
    assert again.json()["tenant_seq"] == 2


async def test_deleted_event_number_is_never_reused(client, sql):
    for name in ("a", "b", "c"):
        r = await client.post("/events", json={"tenant_id": "del", "event_type": name})
    assert r.json()["tenant_seq"] == 3

    sql.execute("DELETE FROM audit_events WHERE tenant_id='del' AND tenant_seq=2")
    nxt = await client.post("/events", json={"tenant_id": "del", "event_type": "d"})
    assert nxt.json()["tenant_seq"] == 4

    seqs = [
        row["tenant_seq"]
        for row in sql.execute(
            "SELECT tenant_seq FROM audit_events WHERE tenant_id='del' ORDER BY tenant_seq"
        ).fetchall()
    ]
    assert seqs == [1, 3, 4]


# --------------------------------------------------------------------------- concurrency


async def test_concurrent_same_tenant_writers_get_unique_ordered_seqs(client, dsn, schema, sql):
    table = f'"{schema}".audit_events'
    per_thread = 60
    threads = 6
    errors: list[Exception] = []

    def worker() -> None:
        try:
            with psycopg.connect(dsn) as conn:
                for i in range(per_thread):
                    conn.execute(
                        f"INSERT INTO {table} (tenant_id, event_type) VALUES (%s, %s)",
                        ("race", f"e-{threading.get_ident()}-{i}"),
                    )
        except Exception as exc:  # pragma: no cover - failure diagnostic
            errors.append(exc)

    workers = [threading.Thread(target=worker) for _ in range(threads)]
    for t in workers:
        t.start()
    for t in workers:
        t.join()

    assert not errors, errors
    rows = sql.execute(
        "SELECT tenant_seq FROM audit_events WHERE tenant_id='race'"
    ).fetchall()
    seqs = sorted(r["tenant_seq"] for r in rows)
    expected = threads * per_thread
    # No duplicates, no gaps: exactly 1..N regardless of commit interleaving.
    assert seqs == list(range(1, expected + 1))


async def test_other_tenants_keep_writing_while_one_tenant_is_blocked(client, dsn, schema, sql):
    """A holder keeping the counter row of tenant 'locked' open must not stall 'free'."""
    table = f'"{schema}".audit_events'
    holder = psycopg.connect(dsn)
    try:
        holder.execute("BEGIN")
        holder.execute(
            f"INSERT INTO {table} (tenant_id, event_type) VALUES ('locked', 'hold')"
        )
        # Transaction (and the FOR UPDATE row lock on the counter row) stays open.

        def write_free() -> list[int]:
            got = []
            with psycopg.connect(dsn) as conn:
                for _ in range(5):
                    row = conn.execute(
                        f"INSERT INTO {table} (tenant_id, event_type) "
                        "VALUES ('free', 'go') RETURNING tenant_seq"
                    ).fetchone()
                    got.append(row[0])
            return got

        result: dict[str, list[int]] = {}

        def run() -> None:
            result["seqs"] = write_free()

        t = threading.Thread(target=run)
        t.start()
        t.join(timeout=15)
        assert not t.is_alive(), "other-tenant writes blocked behind the row lock"
        assert result["seqs"] == [1, 2, 3, 4, 5]
    finally:
        holder.rollback()
        holder.close()


# --------------------------------------------------------------------------- range query


async def test_query_by_tenant_seq_range_and_audit_order(client):
    for i in range(6):
        r = await client.post(
            "/events", json={"tenant_id": "range", "event_type": f"e{i}"}
        )
        assert r.json()["tenant_seq"] == i + 1

    page = (
        await client.get(
            "/events",
            params={"tenant_id": "range", "seq_from": 2, "seq_to": 4,
                    "sort": "tenant_seq_asc"},
        )
    ).json()
    assert [e["tenant_seq"] for e in page["items"]] == [2, 3, 4]
    assert all("tenant_seq" in e for e in page["items"])

    # Open-ended bounds.
    page = (
        await client.get(
            "/events", params={"tenant_id": "range", "seq_from": 5, "sort": "tenant_seq_asc"}
        )
    ).json()
    assert [e["tenant_seq"] for e in page["items"]] == [5, 6]

    # Range filters require a tenant.
    r = await client.get("/events", params={"seq_from": 1})
    assert r.status_code == 422
    r = await client.get(
        "/events", params={"tenant_id": "range", "seq_from": 4, "seq_to": 2}
    )
    assert r.status_code == 422


# --------------------------------------------------------------------------- migration


async def test_backfill_numbers_existing_rows_by_id_within_tenant(monkeypatch, dsn, schema, sql):
    """Simulate first-time enablement on a table that predates tenant_seq."""
    sql.execute("DROP TABLE IF EXISTS legacy_events")
    sql.execute(
        """
        CREATE TABLE legacy_events (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            payload JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    sql.execute(
        "INSERT INTO legacy_events (tenant_id, event_type) VALUES "
        "('a', '1'), ('b', '1'), ('a', '2'), ('a', '3'), ('b', '2')"
    )

    from app.config import Settings
    import app.config as config_module
    import app.db as db_module

    settings = Settings(database_url=dsn, db_schema=schema, events_table="legacy_events")
    monkeypatch.setattr(config_module, "settings", settings)
    monkeypatch.setattr(db_module, "settings", settings)

    from app.db import Database

    db = Database(dsn=dsn, schema=schema, table="legacy_events")
    await db.open()
    try:
        await db.ensure_schema()
        rows = sql.execute(
            "SELECT id, tenant_id, tenant_seq FROM legacy_events "
            "ORDER BY tenant_id, tenant_seq"
        ).fetchall()
        assert [(r["tenant_id"], r["tenant_seq"]) for r in rows] == [
            ("a", 1),
            ("a", 2),
            ("a", 3),
            ("b", 1),
            ("b", 2),
        ]
        # Numbering matches id order inside each tenant.
        a_rows = sql.execute(
            "SELECT id, tenant_seq FROM legacy_events WHERE tenant_id='a' ORDER BY id"
        ).fetchall()
        assert [r["tenant_seq"] for r in a_rows] == [1, 2, 3]

        # New inserts continue after the backfilled maximum.
        nxt = await db.insert_event("a", "4", None, {})
        assert nxt["tenant_seq"] == 4

        # Restart: the migration is idempotent and never renumbers existing rows.
        before = sql.execute(
            "SELECT id, tenant_seq FROM legacy_events ORDER BY id"
        ).fetchall()
        await db.ensure_schema()
        after = sql.execute(
            "SELECT id, tenant_seq FROM legacy_events ORDER BY id"
        ).fetchall()
        assert before == after
    finally:
        await db.close()


async def test_failed_backfill_rolls_back_entire_batch(monkeypatch, dsn, schema, sql):
    """A failure inside the backfill transaction leaves no half-numbered table."""
    sql.execute("DROP TABLE IF EXISTS broken_events")
    sql.execute(
        """
        CREATE TABLE broken_events (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            payload JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    sql.execute(
        "INSERT INTO broken_events (tenant_id, event_type) VALUES ('a', '1'), ('a', '2')"
    )
    # A row-level trigger that aborts every UPDATE: the backfill's UPDATE must fail and
    # the whole migration transaction must roll back, leaving no half-numbered table.
    sql.execute(
        """
        CREATE FUNCTION abort_broken_update() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'simulated backfill failure';
        END;
        $$
        """
    )
    sql.execute(
        "CREATE TRIGGER abort_broken_update_trg BEFORE UPDATE ON broken_events "
        "FOR EACH ROW EXECUTE FUNCTION abort_broken_update()"
    )

    from app.config import Settings
    import app.config as config_module
    import app.db as db_module

    settings = Settings(database_url=dsn, db_schema=schema, events_table="broken_events")
    monkeypatch.setattr(config_module, "settings", settings)
    monkeypatch.setattr(db_module, "settings", settings)

    from app.db import Database

    db = Database(dsn=dsn, schema=schema, table="broken_events")
    await db.open()
    try:
        with pytest.raises(Exception):
            await db.ensure_schema()

        # Nothing was left behind: no column, no NOT NULL, no counter table/rows.
        col = sql.execute(
            """
            SELECT is_nullable FROM information_schema.columns
            WHERE table_schema = %s AND table_name = 'broken_events' AND column_name = 'tenant_seq'
            """,
            (schema,),
        ).fetchone()
        assert col is None or col["is_nullable"] == "YES"
        counter_exists = sql.execute(
            "SELECT to_regclass(%s) AS rel",
            (f"{schema}.broken_events_tenant_seq",),
        ).fetchone()["rel"]
        assert counter_exists is None
        # Original data is untouched.
        assert sql.execute(
            "SELECT count(*) AS n FROM broken_events"
        ).fetchone()["n"] == 2
    finally:
        sql.execute("DROP TRIGGER IF EXISTS abort_broken_update_trg ON broken_events")
        sql.execute("DROP FUNCTION IF EXISTS abort_broken_update()")
        await db.close()


async def test_restart_does_not_renumber_existing_events(monkeypatch, dsn, schema, sql):
    app = make_app(monkeypatch, dsn, schema)
    async with lifespan_client(app) as client:
        for i in range(3):
            await client.post("/events", json={"tenant_id": "keep", "event_type": f"e{i}"})

    before = sql.execute(
        "SELECT id, tenant_seq FROM audit_events WHERE tenant_id='keep' ORDER BY id"
    ).fetchall()

    app2 = make_app(monkeypatch, dsn, schema)
    async with lifespan_client(app2) as client2:
        r = await client2.post(
            "/events", json={"tenant_id": "keep", "event_type": "e3"}
        )
        assert r.json()["tenant_seq"] == 4

    after = sql.execute(
        "SELECT id, tenant_seq FROM audit_events WHERE tenant_id='keep' ORDER BY id"
    ).fetchall()
    assert after[:3] == before
