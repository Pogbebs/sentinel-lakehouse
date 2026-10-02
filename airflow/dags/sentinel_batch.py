"""Hourly batch layer: freshness gate -> dbt build (models + tests) -> publish marts.

The streaming layer answers "is something happening right now?". This DAG answers the
questions that need history: impossible travel, breach exposure, per-user risk, and how
well the real-time rules are performing.

Portable by design: every task shells out to a dedicated virtualenv (dbt, DuckDB,
psycopg2) and imports the sentinel code from SENTINEL_HOME/src, so the DAG itself imports
nothing but Airflow. It runs unchanged in the bundled Airflow 2.10 container or in a shared
Airflow 3.x that has joined the `sentinel-net` Docker network (docs/shared-airflow.md).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

try:  # Airflow 3.x
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.sdk import DAG
except ImportError:  # Airflow 2.x
    from airflow import DAG
    from airflow.operators.bash import BashOperator

SENTINEL_HOME = os.environ.get("SENTINEL_HOME", "/opt/sentinel")
VENV = os.environ.get("SENTINEL_VENV", "/opt/sentinel-venv")
PY = f"{VENV}/bin/python"
DBT = f"{VENV}/bin/dbt"
DBT_DIR = f"{SENTINEL_HOME}/dbt/sentinel"

# Defaults address the Sentinel services by their names on the sentinel-net network, so a
# shared Airflow needs no extra configuration. Any of them can be overridden by env vars.
_DEFAULTS = {
    "LAKE_ROOT": "s3a://lake",
    "TABLE_FORMAT": "delta",
    "S3_ENDPOINT": "http://s3:8333",
    "S3_ENDPOINT_HOST": "s3:8333",
    "S3_ACCESS_KEY": "sentinel",
    "S3_SECRET_KEY": "sentinel-secret",
    "SENTINEL_PG_DSN": "postgresql://sentinel:sentinel@postgres:5432/sentinel",
    "DUCKDB_PATH": "/tmp/sentinel-warehouse/sentinel.duckdb",
    "DBT_TARGET": "docker",
}
TASK_ENV = {k: os.environ.get(k, v) for k, v in _DEFAULTS.items()}
TASK_ENV["PYTHONPATH"] = f"{SENTINEL_HOME}/src"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="sentinel_batch",
    description="Gold layer: dbt models, detection-quality gate, publish to Postgres",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # the DuckDB file is single-writer
    default_args=default_args,
    tags=["sentinel", "dbt", "gold"],
) as dag:
    freshness = BashOperator(
        task_id="check_silver_freshness",
        bash_command=f"{PY} -m sentinel.serving.freshness --max-lag-minutes 30",
        env=TASK_ENV,
        append_env=True,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f'mkdir -p "$(dirname "$DUCKDB_PATH")" && cd {DBT_DIR} && '
            f"{DBT} build --target docker --profiles-dir . "
            "--target-path /tmp/dbt-target --log-path /tmp/dbt-logs"
        ),
        env=TASK_ENV,
        append_env=True,
    )

    publish = BashOperator(
        task_id="publish_marts",
        bash_command=f"{PY} -m sentinel.serving.publish",
        env=TASK_ENV,
        append_env=True,
    )

    freshness >> dbt_build >> publish
