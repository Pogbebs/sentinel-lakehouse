# Operations runbook

## Health checks

| Question | Where to look |
|---|---|
| Are events arriving? | Spark UI (http://localhost:4040), Structured Streaming tab: input rate per query |
| Is the stream keeping up? | Same tab: processing rate should stay above input rate; batch duration below the 30 s trigger |
| Is data being rejected? | Grafana "Quarantine rate" panel, or `marts.mart_data_quality_daily` |
| Did the batch layer run? | Airflow DAG `sentinel_batch`. The first task fails loudly if silver is more than 30 minutes stale |
| Are the rules still good? | `marts.mart_detection_quality`. dbt fails the DAG if precision or recall drops below 0.8 |

## Backfill after a rule or contract change

Silver and the alerts can be rebuilt from bronze, which keeps every raw payload.

```bash
make backfill
```

This stops the streaming application, runs `scripts/run_replay.py` (the same transform
functions as the stream, in batch mode), and restarts streaming.

Because the backfill **overwrites** silver, the detection streams that read silver must
start again from the new table. Before restarting, delete their checkpoints:

```bash
docker compose run --rm minio-init sh -c \
  "mc alias set lake http://minio:9000 minioadmin minioadmin && \
   mc rm --recursive --force lake/lake/_checkpoints/detect_brute_force \
                            lake/lake/_checkpoints/detect_ip_abuse"
```

The alert sinks are idempotent on `alert_id`, so re-detected alerts do not duplicate in
Postgres.

## Changing the event contract

1. Add fields as nullable in `AUTH_EVENT_SCHEMA`. Old messages still parse, and Delta
   schema evolution handles the new silver column.
2. For a breaking change (renamed or retyped field), publish to a new topic
   (`auth.events.v2`), run both versions side by side, and map v1 to v2 in `parse_payload`.
3. Add a quarantine rule for anything the new contract requires.

## Table maintenance

Streaming appends write many small files. Run periodically (from a Spark shell, or as an
Airflow task in production):

```sql
OPTIMIZE delta.`s3a://lake/silver/auth_events` ZORDER BY (username, src_ip);
VACUUM   delta.`s3a://lake/silver/auth_events` RETAIN 168 HOURS;
```

Keep the `VACUUM` retention longer than the longest streaming checkpoint gap, or a stream
restarting after downtime may look for files that have been removed.

## Resetting everything

```bash
make nuke   # removes Kafka, MinIO, Postgres and Airflow volumes
make up
```
