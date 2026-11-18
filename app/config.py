"""Application configuration."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="AUDIT_", env_file=".env", extra="ignore", protected_namespaces=()
    )

    database_url: str = "postgresql://audit:audit@db:5432/audit"
    # Not named `schema`: BaseSettings reserves that name.
    db_schema: str = "public"
    events_table: str = "audit_events"

    # Seconds a background index build may run before the task is reported failed.
    build_timeout_seconds: int = 900
    # How often the supervisor re-checks a build whose worker died (e.g. API restart).
    supervisor_interval_seconds: float = 2.0
    # Startup reconciliation grace period: a task younger than this is left alone so
    # a concurrently starting worker can claim it.
    reconcile_grace_seconds: float = 10.0

    def dsn(self) -> str:
        # SQLAlchemy-style prefixes are common in compose files; psycopg wants a plain one.
        url = self.database_url
        for prefix in ("postgresql+asyncpg://", "postgresql+psycopg://", "postgres://"):
            if url.startswith(prefix):
                url = "postgresql://" + url[len(prefix):]
                return url
        return url


settings = Settings()
