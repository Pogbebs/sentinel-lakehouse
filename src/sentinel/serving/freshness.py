"""Freshness gate run before the batch layer: refuse to build marts on stale silver data."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone


def check_silver_freshness(max_lag_minutes: int = 30) -> datetime:
    import duckdb

    root = os.environ["LAKE_ROOT"].replace("s3a://", "s3://")
    con = duckdb.connect()
    if os.environ.get("TABLE_FORMAT", "parquet") == "delta":
        endpoint = os.environ.get("S3_ENDPOINT_HOST", "s3:8333")
        con.execute("install httpfs; load httpfs; install delta; load delta;")
        con.execute(f"""
            create secret lake (type s3, key_id '{os.environ["S3_ACCESS_KEY"]}',
                secret '{os.environ["S3_SECRET_KEY"]}', endpoint '{endpoint}',
                url_style 'path', use_ssl false, region 'us-east-1')
        """)
        source = f"delta_scan('{root}/silver/auth_events')"
    else:
        source = f"read_parquet('{root}/silver/auth_events/**/*.parquet', hive_partitioning=true)"
    latest = con.execute(f"select max(ingested_at) from {source}").fetchone()[0]
    if latest is None:
        raise RuntimeError("silver.auth_events is empty: is the streaming job running?")
    latest = latest.replace(tzinfo=timezone.utc) if latest.tzinfo is None else latest
    lag = datetime.now(timezone.utc) - latest
    if lag > timedelta(minutes=max_lag_minutes):
        raise RuntimeError(f"silver.auth_events is stale: last ingest {latest} ({lag} ago)")
    return latest
