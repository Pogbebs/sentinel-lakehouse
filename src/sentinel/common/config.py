"""Runtime settings, read from environment variables so every service shares one contract."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    # Messaging
    kafka_bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "localhost:9092"))
    auth_topic: str = field(default_factory=lambda: _env("AUTH_TOPIC", "auth.events.v1"))
    alerts_topic: str = field(default_factory=lambda: _env("ALERTS_TOPIC", "security.alerts.v1"))

    # Lake. LAKE_ROOT is an s3a:// URI in Docker and a local path in the offline demo.
    lake_root: str = field(default_factory=lambda: _env("LAKE_ROOT", "./data/lake"))
    table_format: str = field(default_factory=lambda: _env("TABLE_FORMAT", "parquet"))
    checkpoint_root: str = field(
        default_factory=lambda: _env("CHECKPOINT_ROOT", "./data/checkpoints")
    )

    # Object storage (MinIO / S3)
    s3_endpoint: str = field(default_factory=lambda: _env("S3_ENDPOINT", "http://localhost:9000"))
    s3_access_key: str = field(default_factory=lambda: _env("S3_ACCESS_KEY", "minioadmin"))
    s3_secret_key: str = field(default_factory=lambda: _env("S3_SECRET_KEY", "minioadmin"))

    # Serving database
    pg_dsn: str = field(
        default_factory=lambda: _env(
            "SENTINEL_PG_DSN", "postgresql://sentinel:sentinel@localhost:5432/sentinel"
        )
    )

    # Streaming tuning
    watermark: str = field(default_factory=lambda: _env("WATERMARK_DELAY", "2 minutes"))
    trigger_interval: str = field(default_factory=lambda: _env("TRIGGER_INTERVAL", "30 seconds"))

    def table(self, layer: str, name: str) -> str:
        return f"{self.lake_root.rstrip('/')}/{layer}/{name}"

    def checkpoint(self, name: str) -> str:
        return f"{self.checkpoint_root.rstrip('/')}/{name}"


def get_settings() -> Settings:
    return Settings()
