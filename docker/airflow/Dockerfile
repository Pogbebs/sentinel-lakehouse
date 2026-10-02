FROM apache/airflow:2.10.5-python3.11

# Sentinel's tasks run in their own virtualenv so dbt's pins never fight Airflow's
# constraints. The sentinel code itself is imported from /opt/sentinel/src via PYTHONPATH.
USER root
RUN python -m venv /opt/sentinel-venv \
 && /opt/sentinel-venv/bin/pip install --no-cache-dir \
      "dbt-core>=1.10" "dbt-duckdb>=1.9" "duckdb>=1.1" "psycopg2-binary>=2.9" \
 && mkdir -p /opt/airflow/data \
 && chown -R airflow:root /opt/airflow/data /opt/sentinel-venv

USER airflow
COPY --chown=airflow:root src /opt/sentinel/src
# Pre-install DuckDB extensions so tasks never download them at run time.
RUN /opt/sentinel-venv/bin/python -c \
      "import duckdb; duckdb.connect().execute('install httpfs; install delta')"
