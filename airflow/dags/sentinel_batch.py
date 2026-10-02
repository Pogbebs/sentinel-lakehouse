"""Hourly batch layer: freshness gate -> dbt build (models + tests) -> publish marts.

The streaming layer answers "is something happening right now?". This DAG answers the
questions that need history: impossible travel, breach exposure, per-user risk, and how
well the real-time rules are performing.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DBT_DIR = "/opt/sentinel/dbt/sentinel"
DBT = "/opt/dbt-venv/bin/dbt"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
}


def _freshness() -> str:
    from sentinel.serving.freshness import check_silver_freshness

    return check_silver_freshness(max_lag_minutes=30).isoformat()


def _publish() -> dict:
    import os

    from sentinel.serving.publish import publish

    return publish(os.environ["DUCKDB_PATH"], os.environ["SENTINEL_PG_DSN"])


with DAG(
    dag_id="sentinel_batch",
    description="Gold layer: dbt models, detection-quality gate, publish to Postgres",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # DuckDB file is single-writer
    default_args=default_args,
    tags=["sentinel", "dbt", "gold"],
) as dag:
    freshness = PythonOperator(task_id="check_silver_freshness", python_callable=_freshness)

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && {DBT} build --target docker --profiles-dir . "
        "--target-path /tmp/dbt-target --log-path /tmp/dbt-logs",
        env={"DBT_TARGET": "docker"},
        append_env=True,
    )

    publish = PythonOperator(task_id="publish_marts", python_callable=_publish)

    freshness >> dbt_build >> publish
