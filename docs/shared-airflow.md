# Running the batch DAG in a shared Airflow

The repo ships its own Airflow so anyone can clone it and run everything with one command.
If you already run an Airflow for several projects, you can run `sentinel_batch` there
instead and see every project's jobs in one place.

The DAG is built for this. It imports nothing but Airflow, works on Airflow 2.x and 3.x,
and runs all Sentinel work (freshness check, dbt, publishing) in a separate virtualenv, so
dbt's dependencies never clash with Airflow's.

## 1. Run Sentinel without its own Airflow

In `sentinel-lakehouse/.env`, turn the bundled Airflow off:

```
COMPOSE_PROFILES=
```

Then restart the stack. This also creates the `sentinel-net` Docker network that your
Airflow will join:

```bash
docker compose down
docker compose up -d
```

## 2. Give your Airflow a Sentinel virtualenv

Your Airflow image needs one extra layer. If your project uses a plain
`image: apache/airflow:...` line, create a `Dockerfile` next to its `docker-compose.yml`
(match the version to yours):

```dockerfile
FROM apache/airflow:3.1.3

USER root
RUN python -m venv /opt/sentinel-venv \
 && /opt/sentinel-venv/bin/pip install --no-cache-dir \
      "dbt-core>=1.10" "dbt-duckdb>=1.9" "duckdb>=1.1" "psycopg2-binary>=2.9" \
 && chown -R airflow:root /opt/sentinel-venv

USER airflow
RUN /opt/sentinel-venv/bin/python -c \
      "import duckdb; duckdb.connect().execute('install httpfs; install delta')"
```

If your project already has a Dockerfile, add the same lines to it.

## 3. Mount Sentinel and join its network

In your Airflow project's `docker-compose.yml`, in the shared Airflow settings (usually the
`x-airflow-common` block, so every Airflow service gets them):

```yaml
x-airflow-common: &airflow-common
  build: .                      # instead of image: apache/airflow:...
  volumes:
    # ...keep your existing volumes, then add (use your real path):
    - C:/path/to/sentinel-lakehouse:/opt/sentinel
    - C:/path/to/sentinel-lakehouse/airflow/dags/sentinel_batch.py:/opt/airflow/dags/sentinel_batch.py:ro
  networks:
    - default
    - sentinel-net
```

And at the bottom of the same file:

```yaml
networks:
  sentinel-net:
    external: true
```

Then rebuild and restart your Airflow project:

```bash
docker compose up -d --build
```

`sentinel_batch` appears in your Airflow alongside your other DAGs.

## How it finds everything

The DAG's defaults point at Sentinel's services by their names on `sentinel-net`
(`s3:8333`, `postgres:5432`), so no Airflow connections or variables are needed. To point it
elsewhere, set any of these environment variables on your Airflow services:

| Variable | Default |
|---|---|
| `SENTINEL_HOME` | `/opt/sentinel` |
| `SENTINEL_VENV` | `/opt/sentinel-venv` |
| `S3_ENDPOINT` / `S3_ENDPOINT_HOST` | `http://s3:8333` / `s3:8333` |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | `sentinel` / `sentinel-secret` |
| `SENTINEL_PG_DSN` | `postgresql://sentinel:sentinel@postgres:5432/sentinel` |
| `DUCKDB_PATH` | `/tmp/sentinel-warehouse/sentinel.duckdb` |

Sentinel must be running (step 1) before your Airflow starts, because `sentinel-net` has to
exist for the external network to attach.
