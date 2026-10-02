FROM apache/airflow:2.10.5-python3.11

# dbt lives in its own virtualenv so its pins never fight Airflow's constraints.
USER root
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-core>=1.10" "dbt-duckdb>=1.9" "duckdb>=1.1" \
 && mkdir -p /opt/airflow/data && chown -R airflow:root /opt/airflow/data /opt/dbt-venv

USER airflow
RUN pip install --no-cache-dir "duckdb>=1.1" "psycopg2-binary>=2.9"

COPY --chown=airflow:root pyproject.toml README.md /opt/sentinel/
COPY --chown=airflow:root src /opt/sentinel/src
RUN pip install --no-cache-dir --no-deps /opt/sentinel \
 # Pre-install DuckDB extensions so tasks do not download them at run time.
 && python -c "import duckdb; c=duckdb.connect(); c.execute('install httpfs; install delta')" \
 && /opt/dbt-venv/bin/python -c "import duckdb; c=duckdb.connect(); c.execute('install httpfs; install delta')"
