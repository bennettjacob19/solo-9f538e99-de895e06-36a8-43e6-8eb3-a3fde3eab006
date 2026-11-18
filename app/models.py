"""Pydantic request/response models."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class IndexMode(str, Enum):
    """Supported composite index shapes."""

    tenant_time = "tenant_time"
    event_type_time = "event_type_time"


MODE_COLUMNS: dict[IndexMode, tuple[str, str]] = {
    IndexMode.tenant_time: ("tenant_id", "occurred_at"),
    IndexMode.event_type_time: ("event_type", "occurred_at"),
}

INDEX_NAMES: dict[IndexMode, str] = {
    IndexMode.tenant_time: "audit_events_tenant_id_occurred_at_idx",
    IndexMode.event_type_time: "audit_events_event_type_occurred_at_idx",
}


class TaskStatus(str, Enum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"


class IndexTaskCreate(BaseModel):
    operation_key: str = Field(
        ...,
        min_length=1,
        max_length=200,
        description="Client-side idempotency key identifying the requested operation.",
        examples=["tenant-time-index-2026-11-18"],
    )
    mode: IndexMode = Field(
        ...,
        description=(
            "tenant_time builds (tenant_id, occurred_at); "
            "event_type_time builds (event_type, occurred_at)."
        ),
    )


class IndexView(BaseModel):
    name: str
    schema_name: str
    table_name: str | None = None
    exists: bool
    relation_kind: str | None = None
    is_index: bool = False
    valid: bool = False
    ready: bool = False
    live: bool = False
    unique: bool = False
    columns: list[str] = Field(default_factory=list)
    definition: str | None = None
    predicate: str | None = None
    expressions: str | None = None
    matches_expected: bool = False


class TaskView(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    operation_key: str
    mode: IndexMode
    index_name: str
    status: TaskStatus
    attempts: int
    error: str | None = None
    created_at: datetime
    updated_at: datetime
    finished_at: datetime | None = None
    index: IndexView
    requested_operation_key: str | None = None
    deduplicated: bool = False


class EventCreate(BaseModel):
    tenant_id: str = Field(..., min_length=1, max_length=200)
    event_type: str = Field(..., min_length=1, max_length=200)
    occurred_at: datetime | None = Field(
        default=None, description="Defaults to the database clock when omitted."
    )
    payload: dict[str, Any] = Field(default_factory=dict)


class EventView(BaseModel):
    id: int
    tenant_id: str
    event_type: str
    occurred_at: datetime
    payload: dict[str, Any]
    created_at: datetime


class EventPage(BaseModel):
    items: list[EventView]
    limit: int
    offset: int


class ReconcileSummary(BaseModel):
    checked: int = 0
    succeeded: int = 0
    failed: int = 0
    queued: int = 0
    unchanged: int = 0
    invalid_indexes_dropped: int = 0


class HealthView(BaseModel):
    status: str
    database: str
