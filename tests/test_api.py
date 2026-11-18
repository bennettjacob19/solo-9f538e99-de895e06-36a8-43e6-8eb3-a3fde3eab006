"""End-to-end behaviour of the index task API against a real PostgreSQL catalog."""

from __future__ import annotations

import asyncio

import psycopg

from tests.conftest import (
    force_invalid,
    index_catalog,
    lifespan_client,
    make_app,
    wait_for_status,
)

TENANT_INDEX = "audit_events_tenant_id_occurred_at_idx"
TYPE_INDEX = "audit_events_event_type_occurred_at_idx"


async def request(client, key: str, mode: str):
    return await client.post("/index-tasks", json={"operation_key": key, "mode": mode})


# --------------------------------------------------------------------------- events


async def test_event_ingest_and_query_filters(client):
    r = await client.post(
        "/events",
        json={"tenant_id": "acme", "event_type": "login", "payload": {"ip": "10.0.0.1"}},
    )
    assert r.status_code == 201, r.text
    created = r.json()
    assert created["tenant_id"] == "acme"
    assert created["occurred_at"]

    r = await client.post(
        "/events",
        json={
            "tenant_id": "acme",
            "event_type": "logout",
            "occurred_at": "2026-01-02T03:04:05Z",
            "payload": {},
        },
    )
    assert r.status_code == 201

    page = (await client.get("/events", params={"tenant_id": "acme"})).json()
    assert {e["event_type"] for e in page["items"]} == {"login", "logout"}

    page = (await client.get("/events", params={"event_type": "logout"})).json()
    assert len(page["items"]) == 1
    assert page["items"][0]["occurred_at"].startswith("2026-01-02T03:04:05")

    page = (
        await client.get(
            "/events",
            params={"tenant_id": "acme", "since": "2026-01-01T00:00:00Z", "until": "2026-02-01T00:00:00Z"},
        )
    ).json()
    assert [e["event_type"] for e in page["items"]] == ["logout"]


async def test_batch_insert(client):
    r = await client.post(
        "/events/batch",
        json=[
            {"tenant_id": "t", "event_type": "a"},
            {"tenant_id": "t", "event_type": "b"},
        ],
    )
    assert r.status_code == 201, r.text
    assert [e["event_type"] for e in r.json()] == ["a", "b"]


# --------------------------------------------------------------------------- idempotency


async def test_same_key_same_mode_shares_one_task(client, sql):
    first = await request(client, "op-1", "tenant_time")
    assert first.status_code == 202, first.text
    task_id = first.json()["id"]
    done = await wait_for_status(client, task_id)
    assert done["status"] == "succeeded"
    assert done["index"]["valid"] is True
    assert done["index"]["columns"] == ["tenant_id", "occurred_at"]
    assert index_catalog(sql, TENANT_INDEX)["valid"] is True

    second = await request(client, "op-1", "tenant_time")
    assert second.status_code == 200, second.text
    assert second.json()["id"] == task_id
    assert second.json()["deduplicated"] is True
    count = sql.execute("SELECT count(*) AS n FROM index_tasks").fetchone()["n"]
    assert count == 1


async def test_stranded_pending_requests_share_task(client):
    """Kick off several identical requests before any worker runs; all must get one id."""
    responses = await asyncio.gather(*[request(client, "op-burst", "tenant_time") for _ in range(8)])
    ids = {r.json()["id"] for r in responses}
    assert len(ids) == 1, {r.text for r in responses}
    assert {r.status_code for r in responses} <= {200, 202}
    assert len([r for r in responses if r.status_code == 202]) == 1

    done = await wait_for_status(client, ids.pop())
    assert done["status"] == "succeeded"


async def test_same_key_different_mode_conflicts(client):
    assert (await request(client, "op-conflict", "tenant_time")).status_code == 202
    conflict = await request(client, "op-conflict", "event_type_time")
    assert conflict.status_code == 409, conflict.text
    assert "op-conflict" in conflict.json()["detail"]

    # Still a conflict after the first task finishes.
    await wait_for_status(client, (await request(client, "op-conflict", "tenant_time")).json()["id"])
    conflict = await request(client, "op-conflict", "event_type_time")
    assert conflict.status_code == 409


async def test_different_key_same_mode_builds_once(client, sql):
    first = await request(client, "op-aa", "tenant_time")
    # While the first build is active, a different key joins it.
    second = await request(client, "op-bb", "tenant_time")
    assert second.status_code == 200, second.text
    assert second.json()["id"] == first.json()["id"]
    await wait_for_status(client, first.json()["id"])

    third = await request(client, "op-cc", "tenant_time")
    assert third.status_code == 200, third.text
    assert third.json()["index"]["valid"] is True
    built = sql.execute(
        "SELECT count(*) AS n FROM pg_class WHERE relname = %s", (TENANT_INDEX,)
    ).fetchone()["n"]
    assert built == 1


async def test_two_modes_yield_two_indexes(client, sql):
    a = await request(client, "mode-a", "tenant_time")
    b = await request(client, "mode-b", "event_type_time")
    assert a.json()["id"] != b.json()["id"]
    assert (await wait_for_status(client, a.json()["id"]))["status"] == "succeeded"
    assert (await wait_for_status(client, b.json()["id"]))["status"] == "succeeded"
    assert index_catalog(sql, TENANT_INDEX)["valid"] is True
    assert index_catalog(sql, TYPE_INDEX)["valid"] is True
    cols = sql.execute(
        "SELECT a.attname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid "
        "CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum, ord) "
        "JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum "
        "WHERE c.relname=%s ORDER BY k.ord",
        (TYPE_INDEX,),
    ).fetchall()
    assert [r["attname"] for r in cols] == ["event_type", "occurred_at"]


# --------------------------------------------------------------------------- online


async def test_writes_and_queries_work_during_build_and_cleanup(client, sql):
    """Fill the table so a build takes long enough to overlap with traffic."""
    await client.post(
        "/events/batch",
        json=[{"tenant_id": f"t{i % 20}", "event_type": f"e{i % 5}"} for i in range(500)],
    )
    # Create an invalid husk so the worker has to clean up before building.
    sql.execute(f"CREATE INDEX {TENANT_INDEX} ON audit_events (tenant_id, occurred_at)")
    force_invalid(sql, TENANT_INDEX)

    task = (await request(client, "during-build", "tenant_time")).json()

    stop = asyncio.Event()
    errors: list[str] = []
    writes = 0

    async def writer():
        nonlocal writes
        while not stop.is_set():
            r = await client.post("/events", json={"tenant_id": "live", "event_type": "heartbeat"})
            if r.status_code != 201:
                errors.append(r.text)
            writes += 1

    async def reader():
        while not stop.is_set():
            r = await client.get("/events", params={"tenant_id": "live", "limit": 5})
            if r.status_code != 200:
                errors.append(r.text)
            r = await client.get("/events", params={"event_type": "e1", "limit": 5})
            if r.status_code != 200:
                errors.append(r.text)

    workers = [asyncio.create_task(writer()), asyncio.create_task(reader())]
    done = await wait_for_status(client, task["id"])
    stop.set()
    await asyncio.gather(*workers)

    assert not errors, errors[:3]
    assert writes > 0
    assert done["status"] == "succeeded", done
    assert done["index"]["valid"] is True


# --------------------------------------------------------------------------- restart


async def test_restart_does_not_report_invalid_husk_as_success(monkeypatch, dsn, schema, sql):
    app = make_app(monkeypatch, dsn, schema)
    async with lifespan_client(app) as client:
        task = (await request(client, "restart-op", "tenant_time")).json()
        assert (await wait_for_status(client, task["id"]))["status"] == "succeeded"

    # Simulate a crash mid-CREATE: task still 'running' (and stale, i.e. its worker is
    # gone), index present but invalid.
    force_invalid(sql, TENANT_INDEX)
    sql.execute(
        "INSERT INTO index_tasks (id, operation_key, mode, index_name, status, attempts, "
        "created_at, updated_at) VALUES "
        "('11111111-1111-1111-1111-111111111111', 'crash-op', 'tenant_time', %s, 'running', 1, "
        "now() - interval '1 hour', now() - interval '1 hour')",
        (TENANT_INDEX,),
    )

    app2 = make_app(monkeypatch, dsn, schema)
    async with lifespan_client(app2) as client2:
        # Startup reconciliation must fail the crashed task and drop the husk.
        stale = await client2.get("/index-tasks/11111111-1111-1111-1111-111111111111")
        assert stale.status_code == 200
        assert stale.json()["status"] == "failed", stale.text
        assert index_catalog(sql, TENANT_INDEX) is None

        # The previously succeeded task now points at a missing index: never success.
        r = await client2.get(f"/index-tasks/{task['id']}")
        assert r.json()["status"] == "failed"

        # Retrying rebuilds cleanly under the same task id.
        retry = await client2.post(f"/index-tasks/{task['id']}/retry")
        assert retry.status_code == 202, retry.text
        assert retry.json()["id"] == task["id"]
        done = await wait_for_status(client2, task["id"])
        assert done["status"] == "succeeded", done
        assert index_catalog(sql, TENANT_INDEX)["valid"] is True
        assert done["index"]["columns"] == ["tenant_id", "occurred_at"]


async def test_retry_after_failure_cleans_leftover_invalid_index(monkeypatch, dsn, schema, sql, client):
    task = (await request(client, "retry-op", "tenant_time")).json()
    await wait_for_status(client, task["id"])
    # Corrupt the index so it looks like a failed/incomplete build.
    force_invalid(sql, TENANT_INDEX)

    status = await client.get(f"/index-tasks/{task['id']}")
    assert status.json()["status"] == "failed"
    assert status.json()["index"]["valid"] is False

    retry = await client.post(f"/index-tasks/{task['id']}/retry")
    assert retry.status_code == 202, retry.text
    done = await wait_for_status(client, task["id"])
    assert done["status"] == "succeeded", done
    assert index_catalog(sql, TENANT_INDEX)["valid"] is True


async def test_valid_index_not_dropped_by_reconcile(client, sql):
    task = (await request(client, "keep-op", "event_type_time")).json()
    assert (await wait_for_status(client, task["id"]))["status"] == "succeeded"
    summary = (await client.post("/index-tasks/reconcile")).json()
    assert summary["invalid_indexes_dropped"] == 0
    assert index_catalog(sql, TYPE_INDEX)["valid"] is True

    # A successful re-request must not rebuild or delete it either.
    again = await request(client, "keep-op-2", "event_type_time")
    assert again.status_code == 200
    assert index_catalog(sql, TYPE_INDEX)["valid"] is True


async def test_health_and_docs(client):
    assert (await client.get("/health")).json() == {"status": "ok", "database": "ok"}
    assert (await client.get("/openapi.json")).status_code == 200


async def test_docs_available(client):
    r = await client.get("/docs")
    assert r.status_code == 200
    assert "swagger" in r.text.lower()


async def test_restart_marks_completed_but_unrecorded_build_succeeded(monkeypatch, dsn, schema, sql):
    """Worker died after CREATE finished but before it wrote 'succeeded'."""
    app = make_app(monkeypatch, dsn, schema)
    async with lifespan_client(app) as client:
        task = (await request(client, "unrecorded", "tenant_time")).json()
        await wait_for_status(client, task["id"])

    # Task row says running; catalog says the index is valid. Catalog wins.
    sql.execute(
        "UPDATE index_tasks SET status='running', finished_at=NULL, "
        "updated_at=now() - interval '1 hour' WHERE id=%s",
        (task["id"],),
    )
    app2 = make_app(monkeypatch, dsn, schema)
    async with lifespan_client(app2) as client2:
        r = await client2.get(f"/index-tasks/{task['id']}")
        assert r.json()["status"] == "succeeded", r.text
        assert index_catalog(sql, TENANT_INDEX)["valid"] is True


async def test_running_task_from_live_worker_is_not_failed(monkeypatch, dsn, schema, sql):
    """Reconciliation must not kill a task whose build lock is still held elsewhere."""

    from app.indexer import build_lock_key

    app = make_app(monkeypatch, dsn, schema)
    async with lifespan_client(app) as client:
        # A second process-owner holds the per-index lock while a task sits at 'running'.
        holder = psycopg.connect(dsn, autocommit=True)
        holder.execute("SELECT pg_advisory_lock(hashtext(%s))", (build_lock_key(TENANT_INDEX),))
        try:
            sql.execute(
                "INSERT INTO index_tasks (id, operation_key, mode, index_name, status, attempts) "
                "VALUES ('22222222-2222-2222-2222-222222222222', 'live-op', 'tenant_time', %s, "
                "'running', 1)",
                (TENANT_INDEX,),
            )
            summary = (await client.post("/index-tasks/reconcile")).json()
            assert summary["failed"] == 0, summary
            row = sql.execute(
                "SELECT status FROM index_tasks WHERE id='22222222-2222-2222-2222-222222222222'"
            ).fetchone()
            assert row["status"] == "running"
        finally:
            holder.execute("SELECT pg_advisory_unlock(hashtext(%s))", (build_lock_key(TENANT_INDEX),))
            holder.close()

    # Once the "process" is gone, the next reconcile fails the task.
    sql.execute(
        "UPDATE index_tasks SET updated_at=now() - interval '1 hour' "
        "WHERE id='22222222-2222-2222-2222-222222222222'"
    )
    app2 = make_app(monkeypatch, dsn, schema)
    async with lifespan_client(app2) as client2:
        r = await client2.get("/index-tasks/22222222-2222-2222-2222-222222222222")
        assert r.json()["status"] == "failed", r.text


async def test_concurrent_conflicting_requests_one_wins(client):
    results = await asyncio.gather(
        *[
            request(client, "race-key", "tenant_time" if i % 2 else "event_type_time")
            for i in range(10)
        ]
    )
    codes = sorted(r.status_code for r in results)
    # No 5xx: every request either created/reused a task or reported a clean conflict.
    assert set(codes) <= {200, 202, 409}, [r.text for r in results if r.status_code >= 400]
    ok_modes = {r.json()["mode"] for r in results if r.status_code in (200, 202)}
    assert len(ok_modes) == 1, ok_modes


async def test_valid_index_with_wrong_shape_is_reported_not_deleted(client, sql):
    """A valid same-named index with different columns must not be silently replaced."""
    sql.execute(f"CREATE INDEX {TENANT_INDEX} ON audit_events (event_type, tenant_id)")
    task = (await request(client, "shape-op", "tenant_time")).json()
    done = await wait_for_status(client, task["id"])
    assert done["status"] == "failed", done
    assert "unexpected definition" in (done["error"] or "")

    # The foreign valid index is still there, untouched, and still valid.
    row = index_catalog(sql, TENANT_INDEX)
    assert row is not None and row["valid"] is True

    # Reconciliation does not delete it either.
    summary = (await client.post("/index-tasks/reconcile")).json()
    assert summary["invalid_indexes_dropped"] == 0
    assert index_catalog(sql, TENANT_INDEX)["valid"] is True

    # A retry reports the same conflict, still without deleting anything.
    retry = await client.post(f"/index-tasks/{task['id']}/retry")
    assert retry.status_code == 202
    again = await wait_for_status(client, task["id"])
    assert again["status"] == "failed"
    assert index_catalog(sql, TENANT_INDEX)["valid"] is True


async def test_unmanaged_relations_are_never_dropped(client, sql):
    sql.execute("CREATE INDEX custom_events_tenant_idx ON audit_events (tenant_id)")
    task = (await request(client, "unmanaged", "tenant_time")).json()
    assert (await wait_for_status(client, task["id"]))["status"] == "succeeded"
    await client.post("/index-tasks/reconcile")
    found = sql.execute(
        "SELECT count(*) AS n FROM pg_class WHERE relname='custom_events_tenant_idx'"
    ).fetchone()["n"]
    assert found == 1
