# Audit Events Index Service

FastAPI + PostgreSQL service for a continuously written audit-event table. It appends
events and builds composite query indexes **online** (`CREATE INDEX CONCURRENTLY`), so
ingestion and lookups keep working while an index is built or cleaned up.

Every event carries a database-assigned **`tenant_seq`**: a gapless-on-commit, per-tenant
sequence number that records the exact order in which a tenant's events were committed.

## Quick start

```bash
docker compose up --build
```

Then open <http://localhost:8000/docs> and use the interactive docs. Everything below is
also reachable from there.

```bash
# append an event
curl -X POST localhost:8000/events -H 'content-type: application/json' \
  -d '{"tenant_id":"acme","event_type":"login","payload":{"ip":"10.0.0.1"}}'

# request a tenant_id + occurred_at index
curl -X POST localhost:8000/index-tasks -H 'content-type: application/json' \
  -d '{"operation_key":"tenant-index-2026-11-18","mode":"tenant_time"}'

# check the task (re-reads the PostgreSQL catalog every time)
curl localhost:8000/index-tasks/<task-id>

# query events by tenant and time
curl 'localhost:8000/events?tenant_id=acme&since=2026-11-01T00:00:00Z'

# audit/reconciliation: walk one tenant's events in commit order via tenant_seq
curl 'localhost:8000/events?tenant_id=acme&seq_from=100&seq_to=200&sort=tenant_seq_asc'
```

## API

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/events` | Append one event (`tenant_id`, `event_type`, optional `occurred_at`, `payload`). Response includes the database-assigned `tenant_seq`. |
| `POST` | `/events/batch` | Append up to 1000 events in one transaction; each response row carries its own `tenant_seq`. |
| `GET` | `/events` | Query by `tenant_id`, `event_type`, `since`, `until`, `seq_from`, `seq_to`; `sort` (`recent`/`tenant_seq_asc`), `limit`/`offset`. |
| `POST` | `/index-tasks` | Request an index. `mode` is `tenant_time` or `event_type_time`. `202` = new task, `200` = reused, `409` = key reused with another mode. |
| `GET` | `/index-tasks/{id}` | Task status plus the live catalog state of its index. |
| `GET` | `/index-tasks` | Recent tasks, or `?operation_key=` lookup. |
| `POST` | `/index-tasks/{id}/retry` | Retry a finished task; leftover invalid index is cleaned first. |
| `POST` | `/index-tasks/reconcile` | Force a catalog reconciliation now. |
| `GET` | `/indexes` | Inspect the two managed indexes. |
| `GET` | `/health` | API + database liveness. |

`mode` maps to a fixed index name and column order:

| mode | columns | index name |
| --- | --- | --- |
| `tenant_time` | `(tenant_id, occurred_at)` | `audit_events_tenant_id_occurred_at_idx` |
| `event_type_time` | `(event_type, occurred_at)` | `audit_events_event_type_occurred_at_idx` |

## Behaviour guarantees

**Per-tenant sequence (`tenant_seq`).** Each event stores a 1-based sequence number that is
unique within its tenant and reflects its commit order. Allocation is entirely
database-side:

* A `BEFORE INSERT` row trigger assigns the number from a per-tenant counter table
  (`audit_events_tenant_seq`). The counter row is locked `FOR UPDATE`, so concurrent
  transactions of the **same tenant never get the same number and cannot commit out of
  order**; different tenants lock different rows and never wait on each other.
* This applies to every insert path — the single API, the batch API and plain SQL
  `INSERT`. Clients cannot supply or rewrite `tenant_seq` (the write model forbids the
  field and the trigger rejects a non-null value); a `BEFORE UPDATE` trigger also forbids
  changing `tenant_id` or `tenant_seq` on an existing event.
* Because the counter increment lives in the same transaction as the insert, a rollback
  rolls the increment back too: aborted or failed writes consume **no** numbers. Deleting
  an event never frees its number, so old numbers are never reused.
* First-time enablement backfills pre-existing rows with `ROW_NUMBER() OVER (PARTITION BY
  tenant_id ORDER BY id)` — contiguous within each tenant, by ascending `id`. The whole
  migration (column, counter table, triggers, backfill, `NOT NULL`, unique index) runs in
  one transaction: a failure rolls everything back, leaving no half-backfilled table. It
  is idempotent, so a restart never renumbers existing events and never rewinds a counter.
* Single and batch write responses return each event's own `tenant_seq`. `GET /events`
  supports `seq_from`/`seq_to` (requires `tenant_id`) and `sort=tenant_seq_asc` for
  audit-order reconciliation; a unique index on `(tenant_id, tenant_seq)` backs the lookup.

**Idempotency.** Partial unique indexes on `index_tasks` allow at most one *active*
(`queued`/`running`) task per `operation_key` and per `index_name`:

* same key + same mode while active (or after success) → same task, `200`, `deduplicated: true`;
* same key + different mode → `409`, whether the first task is still running or finished;
* different key + same mode → the existing build is shared; only one index is ever built.

**Online build and cleanup.** Builds and invalid-index cleanup use `CREATE INDEX
CONCURRENTLY` / `DROP INDEX CONCURRENTLY` on dedicated autocommit connections, so they do
not take locks that block `INSERT` or `SELECT` on the events table.

**Catalog is the source of truth.** A task is only `succeeded` when the catalog reports an
index that is `indisvalid`, `indisready`, `indislive`, on the events table, with exactly
the expected columns, no predicate and no expressions.

* Every status read re-inspects the catalog; a `succeeded` row whose index vanished or
  became invalid is reported (and stored) as `failed`.
* A `running` row whose index is already valid (worker died after `CREATE` finished) is
  reported as `succeeded`.
* A `running` row whose worker is gone (nobody holds that index's advisory lock) is
  marked `failed` — an invalid husk is never reported as success.
* Invalid husks are dropped, but only while holding the build lock, so an in-flight
  `CREATE INDEX CONCURRENTLY` (which is temporarily `indisvalid = false`) is never
  interrupted. Valid indexes are never dropped by reconciliation.
* `retry` rebuilds under the same task id. The worker drops an invalid leftover before
  the new `CREATE`; if a *valid* index of the same name but wrong shape exists, it reports
  a conflict instead of deleting it.

**Restart.** Startup reconciles before serving traffic: stale `running` tasks are settled
against the catalog, queued tasks are re-spawned, and orphan invalid husks are removed.
A periodic supervisor repeats this for tasks abandoned at runtime.

**Concurrency model.** One advisory lock per index name (session-scoped, on a dedicated
connection) serialises builds/cleanup of that index across coroutines and replicas. The
worker waits on the lock, so a request arriving during another worker's cleanup still
gets built.

## Local development / tests

The tests require PostgreSQL but do not mock it: they spin up a throwaway cluster via the
`pgserver` package and drive the ASGI app with `httpx`.

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest tests -q
```

`requirements-dev.txt` adds `pytest`, `pytest-asyncio`, `httpx` and `pgserver`. The suite
also verifies the query planner uses each composite index and that writes/reads succeed
while a build is stalled behind an open snapshot.

## Configuration

Environment variables (prefix `AUDIT_`):

| Variable | Default | Meaning |
| --- | --- | --- |
| `AUDIT_DATABASE_URL` | `postgresql://audit:audit@db:5432/audit` | PostgreSQL DSN. |
| `AUDIT_DB_SCHEMA` | `public` | Schema holding `audit_events` and `index_tasks`. |
| `AUDIT_EVENTS_TABLE` | `audit_events` | Event table name. |
| `AUDIT_BUILD_TIMEOUT_SECONDS` | `900` | `statement_timeout` for the concurrent build. |
| `AUDIT_SUPERVISOR_INTERVAL_SECONDS` | `2` | Reconciliation/heartbeat cadence. |
| `AUDIT_RECONCILE_GRACE_SECONDS` | `10` | How long a `running` task may go without a heartbeat before it is considered abandoned when the lock is free. |

`docker-compose.yml` starts PostgreSQL 16 and the API on port 8000; the API waits for the
database health check before starting.

## Notes and limits

* The service owns exactly two index names (see table above). It never touches other
  indexes or tables.
* One API process is enough: builds run as in-process asyncio tasks guarded by PostgreSQL
  advisory locks. Running multiple replicas is safe (they coordinate through those locks
  and the partial unique indexes), but not required.
* No authentication/authorisation is implemented; put the service behind your platform's
  gateway before exposing it beyond a trusted network.
