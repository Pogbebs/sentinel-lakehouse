SHELL := /bin/bash
.DEFAULT_GOAL := help
PY ?= python
DATA := $(CURDIR)/data
export LAKE_ROOT ?= $(DATA)/lake
export TABLE_FORMAT ?= parquet
export DUCKDB_PATH ?= $(DATA)/warehouse/sentinel.duckdb

help: ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------- local dev (no Docker)
install: ## Install the package and dev tools into the current Python env
	$(PY) -m pip install -e ".[dev]"

lint: ## Ruff lint + format check
	ruff check src tests airflow scripts
	ruff format --check src tests airflow scripts

test: ## Unit tests (generator + Spark transforms)
	$(PY) -m pytest

demo: ## Offline end-to-end: generate -> Spark bronze/silver/alerts -> dbt gold + tests
	rm -rf $(DATA) && mkdir -p $(DATA)/warehouse
	$(PY) -m sentinel.generator.cli file --minutes $${MINUTES:-180} --out $(DATA)/raw/auth_events \
	    --start 2026-10-01T08:00:00Z
	$(PY) -m sentinel.streaming.replay --source "$(DATA)/raw/auth_events/*.jsonl"
	cd dbt/sentinel && dbt build --profiles-dir . --target local
	$(PY) scripts/report.py

# ---------------------------------------------------------------- Docker stack
up: ## Build and start the full stack
	docker compose up -d --build
	@echo "Grafana http://localhost:3000 | Airflow http://localhost:8080 (admin/admin)"
	@echo "Spark UI http://localhost:4040 | SeaweedFS http://localhost:9333"

down: ## Stop the stack, keep data
	docker compose down

nuke: ## Stop the stack and delete all volumes
	docker compose down -v

logs: ## Tail streaming + generator logs
	docker compose logs -f spark-streaming generator

ps: ## Service status
	docker compose ps

backfill: ## Stop streaming, rebuild silver + alerts from bronze, restart (see docs/operations.md)
	docker compose stop spark-streaming
	docker compose run --rm spark-streaming bash -c '/opt/spark/bin/spark-submit \
	    --conf spark.jars.ivy=$$IVY_HOME --packages $$SPARK_PACKAGES /opt/sentinel/scripts/run_replay.py'
	docker compose start spark-streaming

batch: ## Trigger the Airflow gold-layer DAG now
	docker compose exec airflow airflow dags trigger sentinel_batch

.PHONY: help install lint test demo up down nuke logs ps backfill batch
