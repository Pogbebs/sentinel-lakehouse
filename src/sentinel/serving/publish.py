"""Publish dbt marts from DuckDB to Postgres, where Grafana and other consumers read them.

Each table is loaded into a staging table and swapped in a single transaction, so readers
never see a half-loaded mart.
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import os

log = logging.getLogger("sentinel.publish")

DEFAULT_MARTS = (
    "fct_security_incidents",
    "fct_impossible_travel",
    "dim_exposed_accounts",
    "mart_user_risk_daily",
    "mart_detection_quality",
    "mart_data_quality_daily",
)

PG_TYPES = {
    "VARCHAR": "text",
    "BOOLEAN": "boolean",
    "DATE": "date",
    "TIMESTAMP": "timestamp",
    "TIMESTAMP WITH TIME ZONE": "timestamptz",
    "DOUBLE": "double precision",
    "FLOAT": "real",
    "BIGINT": "bigint",
    "INTEGER": "integer",
    "HUGEINT": "numeric",
}


def _pg_type(duck_type: str) -> str:
    if duck_type.startswith("DECIMAL"):
        return "numeric"
    return PG_TYPES.get(duck_type, "text")


def publish(
    duckdb_path: str, pg_dsn: str, marts: tuple[str, ...] = DEFAULT_MARTS, schema: str = "marts"
) -> dict[str, int]:
    import duckdb
    import psycopg2

    src = duckdb.connect(duckdb_path, read_only=True)
    counts: dict[str, int] = {}
    with psycopg2.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(f"create schema if not exists {schema}")
        for table in marts:
            cols = src.execute(f"describe {table}").fetchall()
            ddl = ", ".join(f'"{name}" {_pg_type(dtype)}' for name, dtype, *_ in cols)
            buf = io.StringIO()
            rows = src.execute(f"select * from {table}").fetchall()
            w = csv.writer(buf)
            for row in rows:
                w.writerow(["\\N" if v is None else v for v in row])
            buf.seek(0)
            tmp = f"{table}__load"
            cur.execute(f"drop table if exists {schema}.{tmp}")
            cur.execute(f"create table {schema}.{tmp} ({ddl})")
            cur.copy_expert(f"copy {schema}.{tmp} from stdin with (format csv, null '\\N')", buf)
            cur.execute(f"drop table if exists {schema}.{table}")
            cur.execute(f"alter table {schema}.{tmp} rename to {table}")
            counts[table] = len(rows)
            log.info("published %s.%s (%s rows)", schema, table, len(rows))
    src.close()
    return counts


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument(
        "--duckdb", default=os.environ.get("DUCKDB_PATH", "data/warehouse/sentinel.duckdb")
    )
    p.add_argument(
        "--dsn",
        default=os.environ.get(
            "SENTINEL_PG_DSN", "postgresql://sentinel:sentinel@localhost:5432/sentinel"
        ),
    )
    args = p.parse_args()
    publish(args.duckdb, args.dsn)


if __name__ == "__main__":
    main()
