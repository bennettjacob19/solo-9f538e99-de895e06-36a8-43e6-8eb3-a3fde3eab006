"""FastAPI application: audit event ingestion/query and online index task management."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from .db import Database
from .indexer import ConflictError, Indexer, TaskNotFound, TaskRepository
from .models import (
    EventCreate,
    EventPage,
    EventView,
    HealthView,
    IndexTaskCreate,
    IndexMode,
    IndexView,
    ReconcileSummary,
    TaskView,
)

log = logging.getLogger("audit.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    db = Database()
    await db.open()
    await db.ensure_schema()
    repo = TaskRepository(db)
    await repo.ensure_schema()
    indexer = Indexer(db, repo)
    app.state.db = db
    app.state.repo = repo
    app.state.indexer = indexer
    # Reconcile before serving: a task from the previous process could claim success for
    # an index that never became valid, and an interrupted CREATE can leave a husk behind.
    summary = await indexer.reconcile(startup=True)
    log.info("startup reconciliation: %s", summary)
    await indexer.start()
    try:
        yield
    finally:
        await indexer.stop()
        await db.close()


app = FastAPI(
    title="Audit Events Index Service",
    version="1.0.0",
    description=(
        "Ingests audit events and builds composite search indexes online. "
        "Index builds use CREATE INDEX CONCURRENTLY, so writers and readers stay available."
    ),
    lifespan=lifespan,
)


# --------------------------------------------------------------------------- dependencies


def get_db(request: Request) -> Database:
    return request.app.state.db


def get_indexer(request: Request) -> Indexer:
    return request.app.state.indexer


DBDep = Annotated[Database, Depends(get_db)]
IndexerDep = Annotated[Indexer, Depends(get_indexer)]


async def _task_view(indexer: Indexer, row: dict) -> TaskView:
    row = await indexer.refresh(row)
    index = await indexer.db.inspect_index(row["index_name"])
    index = index.model_copy(
        update={"matches_expected": indexer.matches_expected(index, IndexMode(row["mode"]))}
    )
    return TaskView(
        id=row["id"],
        operation_key=row["operation_key"],
        mode=row["mode"],
        index_name=row["index_name"],
        status=row["status"],
        attempts=row["attempts"],
        error=row["error"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        finished_at=row["finished_at"],
        index=index,
    )


# --------------------------------------------------------------------------- health


@app.get("/health", response_model=HealthView, tags=["ops"])
async def health(db: DBDep) -> HealthView:
    try:
        await db.health()
    except Exception as exc:  # pragma: no cover - operational path
        return JSONResponse(status_code=503, content={"status": "degraded", "database": str(exc)})
    return HealthView(status="ok", database="ok")


# --------------------------------------------------------------------------- events


@app.post(
    "/events",
    response_model=EventView,
    status_code=status.HTTP_201_CREATED,
    tags=["events"],
    summary="Append one audit event",
)
async def create_event(event: EventCreate, db: DBDep) -> EventView:
    row = await db.insert_event(event.tenant_id, event.event_type, event.occurred_at, event.payload)
    return EventView(**row)


@app.post(
    "/events/batch",
    response_model=list[EventView],
    status_code=status.HTTP_201_CREATED,
    tags=["events"],
    summary="Append a batch of audit events in one transaction",
)
async def create_events(events: list[EventCreate], db: DBDep) -> list[EventView]:
    if not events:
        return []
    if len(events) > 1000:
        raise HTTPException(status_code=422, detail="at most 1000 events per batch")
    inserted = await db.insert_events_batch(
        [(e.tenant_id, e.event_type, e.occurred_at, e.payload) for e in events]
    )
    return [EventView(**row) for row in inserted]


@app.get(
    "/events",
    response_model=EventPage,
    tags=["events"],
    summary="Query events by tenant and/or type within a time range",
)
async def list_events(
    db: DBDep,
    tenant_id: str | None = Query(None, description="Filter by tenant"),
    event_type: str | None = Query(None, description="Filter by event type"),
    since: datetime | None = Query(None, description="occurred_at >= since"),
    until: datetime | None = Query(None, description="occurred_at < until"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> EventPage:
    rows = await db.query_events(
        tenant_id=tenant_id,
        event_type=event_type,
        since=since,
        until=until,
        limit=limit,
        offset=offset,
    )
    return EventPage(items=[EventView(**r) for r in rows], limit=limit, offset=offset)


# --------------------------------------------------------------------------- index tasks


@app.post(
    "/index-tasks",
    response_model=TaskView,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["indexes"],
    summary="Request an index build (idempotent per operation key)",
    responses={
        200: {"description": "An equivalent task already exists; this response reuses it."},
        409: {"description": "The operation key was already used with a different mode."},
    },
)
async def create_index_task(
    payload: IndexTaskCreate, indexer: IndexerDep
) -> TaskView:
    try:
        row, reused = await indexer.request(payload.operation_key, payload.mode)
    except ConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    view = await _task_view(indexer, row)
    view.requested_operation_key = payload.operation_key
    view.deduplicated = reused
    if reused:
        # FastAPI builds a fresh response for the declared 202; signal "already exists"
        # per request with an explicit response object.
        return JSONResponse(status_code=status.HTTP_200_OK, content=jsonable_encoder(view))
    return view


@app.get(
    "/index-tasks/{task_id}",
    response_model=TaskView,
    tags=["indexes"],
    summary="Get task status, reconciled against the PostgreSQL catalog",
)
async def get_index_task(task_id: UUID, indexer: IndexerDep) -> TaskView:
    try:
        row = await indexer.get(task_id)
    except TaskNotFound:
        raise HTTPException(status_code=404, detail="task not found")
    return await _task_view(indexer, row)


@app.get(
    "/index-tasks",
    response_model=list[TaskView],
    tags=["indexes"],
    summary="List recent tasks",
)
async def list_index_tasks(
    indexer: IndexerDep,
    limit: int = Query(50, ge=1, le=500),
    operation_key: str | None = Query(None),
) -> list[TaskView]:
    repo = indexer.repo
    if operation_key:
        row = await repo.latest_for_key(operation_key)
        rows = [row] if row else []
    else:
        rows = await repo.list_recent(limit)
    return [await _task_view(indexer, row) for row in rows]


@app.post(
    "/index-tasks/{task_id}/retry",
    response_model=TaskView,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["indexes"],
    summary="Retry a failed task; leftover invalid index is cleaned up first",
)
async def retry_index_task(task_id: UUID, indexer: IndexerDep) -> TaskView:
    try:
        row, requeued = await indexer.retry(task_id)
    except TaskNotFound:
        raise HTTPException(status_code=404, detail="task not found")
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    view = await _task_view(indexer, row)
    if not requeued:
        return JSONResponse(status_code=status.HTTP_200_OK, content=jsonable_encoder(view))
    return view


@app.post(
    "/index-tasks/reconcile",
    response_model=ReconcileSummary,
    tags=["indexes"],
    summary="Re-check all active tasks against the catalog now",
)
async def reconcile(indexer: IndexerDep) -> ReconcileSummary:
    return await indexer.reconcile(startup=False)


@app.get(
    "/indexes",
    response_model=list[IndexView],
    tags=["indexes"],
    summary="Inspect the two managed indexes in the PostgreSQL catalog",
)
async def list_indexes(indexer: IndexerDep) -> list[IndexView]:
    from .models import INDEX_NAMES

    return [await indexer.db.inspect_index(name) for name in INDEX_NAMES.values()]
