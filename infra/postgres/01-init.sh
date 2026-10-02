#!/bin/bash
# Runs once, on first start of the Postgres volume.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE ROLE airflow  LOGIN PASSWORD '${AIRFLOW_DB_PASSWORD}';
    CREATE ROLE sentinel LOGIN PASSWORD '${SENTINEL_DB_PASSWORD}';
    CREATE ROLE grafana  LOGIN PASSWORD 'grafana';  -- read-only dashboard role
    CREATE DATABASE airflow  OWNER airflow;
    CREATE DATABASE sentinel OWNER sentinel;
EOSQL

psql -v ON_ERROR_STOP=1 --username sentinel --dbname sentinel <<-'EOSQL'
    -- Real-time alerts written by the Spark detectors (idempotent upsert on alert_id).
    CREATE TABLE IF NOT EXISTS alerts (
        alert_id              text PRIMARY KEY,
        rule                  text        NOT NULL,
        severity              text        NOT NULL,
        entity_type           text        NOT NULL,
        entity_value          text        NOT NULL,
        window_start          timestamptz NOT NULL,
        window_end            timestamptz NOT NULL,
        attempts              bigint,
        failures              bigint,
        successes             bigint,
        distinct_users        bigint,
        unknown_user_failures bigint,
        detected_at           timestamptz NOT NULL,
        inserted_at           timestamptz NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS alerts_window_idx ON alerts (window_start DESC);
    CREATE INDEX IF NOT EXISTS alerts_rule_idx   ON alerts (rule, window_start DESC);

    -- Batch marts published by Airflow land here.
    CREATE SCHEMA IF NOT EXISTS marts;

    -- Read-only access for dashboards, including tables created later by Airflow.
    GRANT USAGE ON SCHEMA public, marts TO grafana;
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO grafana;
    ALTER DEFAULT PRIVILEGES IN SCHEMA marts  GRANT SELECT ON TABLES TO grafana;
EOSQL
