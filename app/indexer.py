"""Task bookkeeping in PostgreSQL and the online index-build worker.

Design notes
------------
* ``index_tasks`` rows are the durable record of every request. Only one *active*
  (queued/running) row may exist per operation key and per index name, enforced by
  partial unique indexes. Concurrent retries with the same key/mode, and different
  keys for the same index shape, therefore converge on one build.
* The PostgreSQL catalog is the final authority on whether an index exists. Every
  status read re-inspects the catalog; the startup reconcile does the same before the
  API serves traffic. A missing or invalid index is never reported as success.
* A build worker holds a session-level advisory lock named after the *index*, for the
  whole build. Cleanup of a stray invalid index tries the same lock first, so an index
  that is currently being built (and is therefore ``indisvalid = false``) is never
  dropped underneath its worker.
* ``CREATE INDEX CONCURRENTLY`` / ``DROP INDEX CONCURRENTLY`` run on dedicated
  autocommit connections. Neither takes a lock that blocks INSERT or SELECT on the
  events table, so ingestion and lookups stay available during build and cleanup.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator
from uuid import UUID, uuid4

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from .config import settings
from .db import Database
from .models import (
    INDEX_NAMES,
    MODE_COLUMNS,
    IndexMode,
    IndexView,
    ReconcileSummary,
    TaskStatus,
)

log = logging.getLogger("audit.indexer")

ACTIVE_STATUSES = (TaskStatus.queued.value, TaskStatus.running.value)


class ConflictError(Exception):
    """An operation key is reused with a different index mode."""


class TaskNotFound(Exception):
    pass


def now() -> datetime:
    return datetime.now(timezone.utc)


def build_lock_key(index_name: str) -> str:
    return f"index-build:{index_name}"


class TaskRepository:
    """CRUD for ``index_tasks``."""

    def __init__(self, db: Database):
        self.db = db

    def _tasks(self) -> sql.Identifier:
        return sql.Identifier(self.db.schema, "index_tasks")

    async def ensure_schema(self) -> None:
        async with self.db.pool.connection() as conn:
            await conn.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {} (
                        id UUID PRIMARY KEY,
                        operation_key TEXT NOT NULL,
                        mode TEXT NOT NULL,
                        index_name TEXT NOT NULL,
                        status TEXT NOT NULL,
                        attempts INTEGER NOT NULL DEFAULT 0,
                        error TEXT,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        finished_at TIMESTAMPTZ
                    )
                    """
                ).format(self._tasks())
            )
            # One live task per operation key: a concurrent retry of the same operation
            # attaches to the existing row instead of creating a second task.
            await conn.execute(
                sql.SQL(
                    "CREATE UNIQUE INDEX IF NOT EXISTS index_tasks_one_active_per_key "
                    "ON {} (operation_key) WHERE status IN ('queued', 'running')"
                ).format(self._tasks())
            )
            # One live task per index: a different operation key asking for the same
            # index shape does not schedule a second build.
            await conn.execute(
                sql.SQL(
                    "CREATE UNIQUE INDEX IF NOT EXISTS index_tasks_one_active_per_index "
                    "ON {} (index_name) WHERE status IN ('queued', 'running')"
                ).format(self._tasks())
            )
            await conn.execute(
                sql.SQL(
                    "CREATE INDEX IF NOT EXISTS index_tasks_status_idx ON {} (status, updated_at)"
                ).format(self._tasks())
            )

    # ------------------------------------------------------------------ reads

    async def get_row(self, task_id: UUID) -> dict[str, Any] | None:
        async with self.db.pool.connection() as conn:
            cur = await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE id = %s").format(self._tasks()), (task_id,)
            )
            return await cur.fetchone()

    async def find_active(
        self, *, operation_key: str | None = None, index_name: str | None = None
    ) -> dict[str, Any] | None:
        column, value = (
            ("operation_key", operation_key) if operation_key is not None else ("index_name", index_name)
        )
        async with self.db.pool.connection() as conn:
            cur = await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE {} = %s AND status IN ('queued','running') "
                    "ORDER BY created_at LIMIT 1"
                ).format(self._tasks(), sql.Identifier(column)),
                (value,),
            )
            return await cur.fetchone()

    async def latest_for_key(self, operation_key: str) -> dict[str, Any] | None:
        async with self.db.pool.connection() as conn:
            cur = await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE operation_key = %s ORDER BY created_at DESC LIMIT 1").format(
                    self._tasks()
                ),
                (operation_key,),
            )
            return await cur.fetchone()

    async def list_recent(self, limit: int) -> list[dict[str, Any]]:
        async with self.db.pool.connection() as conn:
            cur = await conn.execute(
                sql.SQL("SELECT * FROM {} ORDER BY created_at DESC LIMIT %s").format(self._tasks()),
                (limit,),
            )
            return await cur.fetchall()

    async def list_active(self) -> list[dict[str, Any]]:
        async with self.db.pool.connection() as conn:
            cur = await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE status IN ('queued','running') ORDER BY created_at"
                ).format(self._tasks())
            )
            return await cur.fetchall()

    async def list_stale(self, grace_seconds: float) -> list[dict[str, Any]]:
        cutoff = now() - timedelta(seconds=grace_seconds)
        async with self.db.pool.connection() as conn:
            cur = await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE status IN ('queued','running') AND updated_at < %s "
                    "ORDER BY created_at"
                ).format(self._tasks()),
                (cutoff,),
            )
            return await cur.fetchall()

    # ------------------------------------------------------------------ writes

    async def create(self, operation_key: str, mode: IndexMode) -> dict[str, Any]:
        async with self.db.pool.connection() as conn:
            cur = await conn.execute(
                sql.SQL(
                    "INSERT INTO {} (id, operation_key, mode, index_name, status, created_at, updated_at) "
                    "VALUES (%s, %s, %s, %s, 'queued', now(), now()) RETURNING *"
                ).format(self._tasks()),
                (uuid4(), operation_key, mode.value, INDEX_NAMES[mode]),
            )
            return await cur.fetchone()

    async def claim(self, task_id: UUID) -> bool:
        """queued -> running. False when another worker claimed it first."""
        async with self.db.pool.connection() as conn:
            cur = await conn.execute(
                sql.SQL(
                    "UPDATE {} SET status='running', attempts=attempts+1, error=NULL, "
                    "updated_at=now(), finished_at=NULL WHERE id=%s AND status='queued' RETURNING id"
                ).format(self._tasks()),
                (task_id,),
            )
            return await cur.fetchone() is not None

    async def takeover(self, task_id: UUID) -> bool:
        """Adopt a task that is already marked running.

        The caller holds the per-index build lock, so no other worker is building this
        index: the previous owner is gone. Unconditional (no heartbeat-age check) so a
        user-initiated retry right after a crash recovers immediately.
        """
        async with self.db.pool.connection() as conn:
            cur = await conn.execute(
                sql.SQL(
                    "UPDATE {} SET status='running', attempts=attempts+1, error=NULL, "
                    "updated_at=now(), finished_at=NULL "
                    "WHERE id=%s AND status='running' RETURNING id"
                ).format(self._tasks()),
                (task_id,),
            )
            return await cur.fetchone() is not None

    async def requeue(self, task_id: UUID) -> bool:
        """Put a finished task back in the queue (used by retry)."""
        async with self.db.pool.connection() as conn:
            cur = await conn.execute(
                sql.SQL(
                    "UPDATE {} SET status='queued', error=NULL, updated_at=now(), finished_at=NULL "
                    "WHERE id=%s AND status IN ('failed','succeeded','running') RETURNING id"
                ).format(self._tasks()),
                (task_id,),
            )
            return await cur.fetchone() is not None

    async def touch(self, task_id: UUID) -> None:
        async with self.db.pool.connection() as conn:
            await conn.execute(
                sql.SQL("UPDATE {} SET updated_at=now() WHERE id=%s AND status='running'").format(
                    self._tasks()
                ),
                (task_id,),
            )

    async def finish(self, task_id: UUID, status: TaskStatus, error: str | None = None) -> None:
        """Record the outcome of a build this worker owns.

        'failed' is included so a transient 'failed' reported while this same worker was
        mid-build (a status read that saw the not-yet-valid index) cannot pin the task to
        a wrong terminal state after the build actually succeeds.
        """
        async with self.db.pool.connection() as conn:
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET status=%s, error=%s, updated_at=now(), finished_at=now() "
                    "WHERE id=%s AND status IN ('running', 'failed')"
                ).format(self._tasks()),
                (status.value, error, task_id),
            )

    async def force_status(self, task_id: UUID, status: TaskStatus, error: str | None = None) -> None:
        async with self.db.pool.connection() as conn:
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET status=%s, error=%s, updated_at=now(), "
                    "finished_at=CASE WHEN %s IN ('succeeded','failed') THEN now() ELSE NULL END "
                    "WHERE id=%s"
                ).format(self._tasks()),
                (status.value, error, status.value, task_id),
            )


class Indexer:
    """Coordinates task creation, index building and catalog reconciliation."""

    def __init__(self, db: Database, repo: TaskRepository):
        self.db = db
        self.repo = repo
        self._worker_tasks: dict[str, asyncio.Task] = {}
        self._supervisor: asyncio.Task | None = None
        self._stopping = asyncio.Event()

    # ------------------------------------------------------------------ lifecycle

    async def start(self) -> None:
        self._stopping.clear()
        self._supervisor = asyncio.create_task(self._supervise(), name="index-supervisor")
        # Tasks queued by a previous process (it died between insert and claim) get a
        # worker immediately; 'running' rows are handled by startup reconciliation and
        # the supervisor.
        for row in await self.repo.list_active():
            if row["status"] == TaskStatus.queued.value:
                self._spawn(row["id"], row["index_name"])

    async def stop(self) -> None:
        self._stopping.set()
        if self._supervisor is not None:
            self._supervisor.cancel()
            try:
                await self._supervisor
            except asyncio.CancelledError:
                pass
            self._supervisor = None
        workers = list(self._worker_tasks.values())
        for task in workers:
            task.cancel()
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)
        self._worker_tasks.clear()

    # ------------------------------------------------------------------ task ingest

    async def request(self, operation_key: str, mode: IndexMode) -> tuple[dict[str, Any], bool]:
        """Idempotently request an index build.

        Returns ``(row, reused)`` where ``reused`` is True when the response describes
        an existing task rather than a freshly created one.
        """
        index_name = INDEX_NAMES[mode]

        # 1. Same operation key, still active.
        active = await self.repo.find_active(operation_key=operation_key)
        if active is not None:
            if active["mode"] != mode.value:
                raise ConflictError(
                    f"operation_key {operation_key!r} is already in use for mode {active['mode']!r}"
                )
            self._spawn(active["id"], active["index_name"])
            return active, True

        # 2. Same operation key, previously used with another mode -> conflict, even if
        #    that task has already finished. One key names one operation.
        latest = await self.repo.latest_for_key(operation_key)
        if latest is not None and latest["mode"] != mode.value:
            raise ConflictError(
                f"operation_key {operation_key!r} was previously used for mode {latest['mode']!r}"
            )

        # 3. Same index shape already being built under a different key -> share it.
        shared = await self.repo.find_active(index_name=index_name)
        if shared is not None:
            self._spawn(shared["id"], shared["index_name"])
            return shared, True

        index = await self.db.inspect_index(index_name)

        # 4. The requested index already exists and is valid: no rebuild. Reuse the
        #    previous task row for this key (repairing its status from the catalog), or
        #    record a completed one. Either way nothing was enqueued, so the caller sees a
        #    200 "reused" response.
        if self.matches_expected(index, mode):
            if latest is not None:
                if latest["status"] != TaskStatus.succeeded.value:
                    await self.repo.force_status(latest["id"], TaskStatus.succeeded, None)
                    latest = {**latest, "status": TaskStatus.succeeded.value, "error": None}
                return latest, True
            row = await self._create_and_finish(operation_key, mode)
            return row, True

        # 5. The same key previously failed (or its index vanished): requeue that task
        #    so a client polling the same id sees the retry, unless another key now owns
        #    the index build.
        if latest is not None and latest["status"] in (
            TaskStatus.failed.value,
            TaskStatus.succeeded.value,
        ):
            requeued = await self._try_requeue(latest["id"], index_name)
            if requeued is not None:
                return requeued, True

        # 6. Fresh task. A concurrent request may win the unique indexes; attach to its
        #    row if so.
        try:
            row = await self.repo.create(operation_key, mode)
        except psycopg.errors.UniqueViolation:
            active = await self.repo.find_active(operation_key=operation_key)
            if active is None:
                active = await self.repo.find_active(index_name=index_name)
            if active is None:
                raise
            if active["operation_key"] == operation_key and active["mode"] != mode.value:
                raise ConflictError(
                    f"operation_key {operation_key!r} is already in use for mode {active['mode']!r}"
                )
            self._spawn(active["id"], active["index_name"])
            return active, True

        self._spawn(row["id"], row["index_name"])
        return row, False

    async def _try_requeue(self, task_id: UUID, index_name: str) -> dict[str, Any] | None:
        try:
            ok = await self.repo.requeue(task_id)
        except psycopg.errors.UniqueViolation:
            active = await self.repo.find_active(index_name=index_name)
            return active
        if not ok:
            return None
        row = await self.repo.get_row(task_id)
        if row is not None:
            self._spawn(row["id"], row["index_name"])
        return row

    async def _create_and_finish(self, operation_key: str, mode: IndexMode) -> dict[str, Any]:
        try:
            row = await self.repo.create(operation_key, mode)
        except psycopg.errors.UniqueViolation:
            active = await self.repo.find_active(operation_key=operation_key)
            if active is None:
                active = await self.repo.find_active(index_name=INDEX_NAMES[mode])
            if active is None:
                raise
            return active
        await self.repo.force_status(row["id"], TaskStatus.succeeded)
        return {**row, "status": TaskStatus.succeeded.value, "error": None}

    # ------------------------------------------------------------------ status

    async def get(self, task_id: UUID) -> dict[str, Any]:
        row = await self.repo.get_row(task_id)
        if row is None:
            raise TaskNotFound(str(task_id))
        return await self.refresh(row)

    async def refresh(self, row: dict[str, Any]) -> dict[str, Any]:
        """Reconcile one task row against the catalog before reporting it."""
        mode = IndexMode(row["mode"])
        index = await self.db.inspect_index(row["index_name"])
        expected = self.matches_expected(index, mode)
        status = row["status"]

        if status == TaskStatus.succeeded.value and not expected:
            error = "index is missing or invalid in the PostgreSQL catalog"
            await self.repo.force_status(row["id"], TaskStatus.failed, error)
            return {**row, "status": TaskStatus.failed.value, "error": error}

        if status == TaskStatus.failed.value and expected:
            # A concurrent build succeeded after this task was marked failed.
            await self.repo.force_status(row["id"], TaskStatus.succeeded, None)
            return {**row, "status": TaskStatus.succeeded.value, "error": None}

        if status == TaskStatus.running.value and expected:
            # The CREATE completed but the worker died before recording it.
            await self.repo.finish(row["id"], TaskStatus.succeeded)
            return {**row, "status": TaskStatus.succeeded.value, "error": None}

        return row

    async def retry(self, task_id: UUID) -> tuple[dict[str, Any], bool]:
        """Retry a task. Leftover invalid indexes are dropped by the worker before the
        new CREATE; a valid index is never touched."""
        row = await self.repo.get_row(task_id)
        if row is None:
            raise TaskNotFound(str(task_id))
        mode = IndexMode(row["mode"])
        index = await self.db.inspect_index(row["index_name"])

        if self.matches_expected(index, mode):
            if row["status"] != TaskStatus.succeeded.value:
                await self.repo.force_status(row["id"], TaskStatus.succeeded, None)
                row = {**row, "status": TaskStatus.succeeded.value, "error": None}
            return row, False

        if row["status"] in ACTIVE_STATUSES:
            self._spawn(row["id"], row["index_name"])
            return row, False

        # Another key may already be rebuilding this index; join it rather than queueing
        # a second build for the same shape.
        shared = await self.repo.find_active(index_name=row["index_name"])
        if shared is not None:
            self._spawn(shared["id"], shared["index_name"])
            return shared, False

        requeued = await self._try_requeue(row["id"], row["index_name"])
        if requeued is None:
            raise ConflictError("task cannot be retried in its current state")
        return requeued, True

    # ------------------------------------------------------------------ reconciliation

    async def reconcile(self, *, startup: bool = False) -> ReconcileSummary:
        """Make active task status and the catalog agree.

        The per-index build lock is the ownership signal: a 'running' task whose lock is
        still held belongs to a live worker and is left alone. Once the lock is free the
        worker is gone, so:

        * index valid  -> the build actually completed: mark the task succeeded;
        * index absent or invalid -> mark the task failed and remove the invalid husk.

        Succeeded tasks that later lose their index are corrected lazily by
        :meth:`refresh` when read. Valid indexes are never dropped.
        """
        summary = ReconcileSummary()
        for row in await self.repo.list_active():
            summary.checked += 1
            mode = IndexMode(row["mode"])
            index = await self.db.inspect_index(row["index_name"])
            if self.matches_expected(index, mode):
                if row["status"] == TaskStatus.running.value:
                    await self.repo.finish(row["id"], TaskStatus.succeeded)
                    summary.succeeded += 1
                else:
                    summary.unchanged += 1
                continue

            if row["status"] != TaskStatus.running.value:
                summary.unchanged += 1
                continue

            async with self._build_lock(row["index_name"], wait=False) as owned:
                if not owned:
                    # A live worker is still building; its status is not stale.
                    summary.unchanged += 1
                    continue
                fresh = await self.repo.get_row(row["id"])
                if fresh is None or fresh["status"] != TaskStatus.running.value:
                    summary.unchanged += 1
                    continue
                current = await self.db.inspect_index(row["index_name"])
                if self.matches_expected(current, mode):
                    await self.repo.finish(row["id"], TaskStatus.succeeded)
                    summary.succeeded += 1
                    continue
                await self.repo.finish(
                    row["id"],
                    TaskStatus.failed,
                    "build worker stopped before completion; index was not validly created",
                )
                summary.failed += 1
                if current.exists and not current.valid:
                    # We hold the build lock, so no live worker owns this husk.
                    try:
                        await self.db.drop_index_concurrently(row["index_name"])
                        summary.invalid_indexes_dropped += 1
                    except Exception:
                        log.exception("could not drop invalid index %s", row["index_name"])

        # Sweep invalid husks that no active task refers to (e.g. a CREATE interrupted
        # before its task row was written, or a task from an older release).
        for name in INDEX_NAMES.values():
            index = await self.db.inspect_index(name)
            if index.exists and not index.valid:
                if await self._drop_stray_invalid(name):
                    summary.invalid_indexes_dropped += 1
        return summary

    async def _drop_stray_invalid(self, index_name: str) -> bool:
        """Drop an invalid index unless a build worker currently owns it."""
        if not self.db.index_belongs_to_us(index_name):
            return False
        try:
            async with self._build_lock(index_name, wait=False) as acquired:
                if not acquired:
                    return False
                await self.db.drop_index_concurrently(index_name)
                return True
        except Exception:
            log.exception("could not drop stray invalid index %s", index_name)
            return False

    @asynccontextmanager
    async def _build_lock(self, index_name: str, *, wait: bool) -> AsyncIterator[bool]:
        """Session-level advisory lock serialising builds/cleanup of one index name.

        Uses a dedicated connection: a pooled connection could be handed to another
        coroutine while the lock is still held.
        """
        conn = await psycopg.AsyncConnection.connect(self.db.dsn, autocommit=True, row_factory=dict_row)
        key = build_lock_key(index_name)
        try:
            if wait:
                await conn.execute("SELECT pg_advisory_lock(hashtext(%s))", (key,))
                yield True
            else:
                cur = await conn.execute("SELECT pg_try_advisory_lock(hashtext(%s)) AS ok", (key,))
                acquired = bool((await cur.fetchone())["ok"])
                try:
                    yield acquired
                finally:
                    if acquired:
                        await conn.execute("SELECT pg_advisory_unlock(hashtext(%s))", (key,))
        finally:
            await conn.close()

    def matches_expected(self, index: IndexView, mode: IndexMode) -> bool:
        """True only when the catalog entry is a live, valid index of exactly the
        requested shape on our events table. Anything else is not a success."""
        return bool(
            index.exists
            and index.is_index
            and index.valid
            and index.ready
            and index.live
            and index.schema_name == self.db.schema
            and index.table_name == self.db.table
            and index.columns == list(MODE_COLUMNS[mode])
            and index.predicate is None
            and index.expressions is None
        )

    # ------------------------------------------------------------------ worker

    def _spawn(self, task_id: UUID, index_name: str) -> None:
        name = f"index-build:{task_id}"
        existing = self._worker_tasks.get(name)
        if existing is not None and not existing.done():
            return
        task = asyncio.create_task(self._run_task(task_id, index_name), name=name)
        self._worker_tasks[name] = task
        task.add_done_callback(lambda t, n=name: self._worker_tasks.pop(n, None))

    async def _run_task(self, task_id: UUID, index_name: str) -> None:
        heartbeat: asyncio.Task | None = None
        try:
            # Wait rather than skip: a request that arrives while cleanup of a stray husk
            # is in progress must still get its build, not silently sit in 'queued'
            # forever once the other worker exits.
            async with self._build_lock(index_name, wait=True):
                row = await self.repo.get_row(task_id)
                if row is None:
                    return
                if row["status"] == TaskStatus.queued.value:
                    claimed = await self.repo.claim(task_id)
                elif row["status"] == TaskStatus.running.value:
                    claimed = await self.repo.takeover(task_id)
                else:
                    claimed = False
                if not claimed:
                    return
                # Heartbeat starts only after the claim so the supervisor never sees a
                # fresh 'running' row as stale while this coroutine is queued on the lock.
                heartbeat = asyncio.create_task(self._heartbeat(task_id))
                await self._build(task_id)
        except asyncio.CancelledError:
            # Shutdown: leave the row running; reconciliation decides after restart.
            log.info("index build %s cancelled by shutdown", task_id)
            raise
        except Exception as exc:  # pragma: no cover - defensive
            log.exception("index build %s crashed", task_id)
            try:
                await self.repo.finish(task_id, TaskStatus.failed, f"{type(exc).__name__}: {exc}")
            except Exception:
                log.exception("could not record failure for task %s", task_id)
        finally:
            if heartbeat is not None:
                heartbeat.cancel()
                try:
                    await heartbeat
                except asyncio.CancelledError:
                    pass

    async def _build(self, task_id: UUID) -> None:
        row = await self.repo.get_row(task_id)
        if row is None:
            return
        mode = IndexMode(row["mode"])
        index_name = row["index_name"]
        expected = list(MODE_COLUMNS[mode])

        build_conn: psycopg.AsyncConnection | None = None
        try:
            index = await self.db.inspect_index(index_name)
            if self.matches_expected(index, mode):
                await self.repo.finish(task_id, TaskStatus.succeeded)
                return

            if index.exists and not index.valid:
                # Leftover of an interrupted CREATE: remove it concurrently, then rebuild.
                log.info("dropping invalid leftover index %s", index_name)
                await self.db.drop_index_concurrently(index_name)
            elif index.exists and not self.matches_expected(index, mode):
                # A *valid* index with this name but the wrong shape. Deleting it could
                # break someone else's workload, so report the conflict instead.
                await self.repo.finish(
                    task_id,
                    TaskStatus.failed,
                    f"index {index_name!r} exists with an unexpected definition: {index.definition}",
                )
                return

            build_conn = await psycopg.AsyncConnection.connect(
                self.db.dsn, autocommit=True, row_factory=dict_row
            )
            await build_conn.execute(
                "SELECT set_config('statement_timeout', %s, false)",
                (f"{int(settings.build_timeout_seconds * 1000)}ms",),
            )
            cols = sql.SQL(", ").join(sql.Identifier(c) for c in expected)
            statement = sql.SQL("CREATE INDEX CONCURRENTLY {} ON {} ({})").format(
                sql.Identifier(index_name), self.db.qualified_table(), cols
            )
            log.info("building index %s with CONCURRENTLY", index_name)
            await build_conn.execute(statement)

            verified = await self.db.inspect_index(index_name)
            if self.matches_expected(verified, mode):
                await self.repo.finish(task_id, TaskStatus.succeeded)
            else:
                await self.repo.finish(
                    task_id,
                    TaskStatus.failed,
                    "CREATE INDEX CONCURRENTLY returned but the index is not valid in the catalog",
                )
        except psycopg.errors.DuplicateTable:
            verified = await self.db.inspect_index(index_name)
            if self.matches_expected(verified, mode):
                await self.repo.finish(task_id, TaskStatus.succeeded)
            else:
                await self.repo.finish(
                    task_id,
                    TaskStatus.failed,
                    f"index {index_name!r} already exists with a different shape",
                )
        except Exception as exc:
            log.warning("index build %s failed: %s", index_name, exc)
            await self.repo.finish(task_id, TaskStatus.failed, f"{type(exc).__name__}: {exc}")
        finally:
            if build_conn is not None and not build_conn.closed:
                await build_conn.close()

    async def _heartbeat(self, task_id: UUID) -> None:
        while True:
            await asyncio.sleep(max(settings.supervisor_interval_seconds, 0.5))
            try:
                await self.repo.touch(task_id)
            except Exception:
                log.exception("heartbeat failed for task %s", task_id)

    async def _supervise(self) -> None:
        """Periodically adopt work left behind by a dead process.

        'running' rows are settled by :meth:`reconcile`, which consults the build lock to
        tell a live worker apart from a dead one. Only 'queued' rows need a worker here;
        the advisory lock plus the conditional claim make duplicate spawns harmless.
        """
        while not self._stopping.is_set():
            try:
                await asyncio.sleep(settings.supervisor_interval_seconds)
                for row in await self.repo.list_stale(settings.reconcile_grace_seconds):
                    if row["status"] == TaskStatus.queued.value:
                        self._spawn(row["id"], row["index_name"])
                await self.reconcile(startup=False)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("index supervisor iteration failed")
