# Operations runbook

## Health checks

Windows (PowerShell), from the repo root:

```powershell
.\scripts\start.ps1            # start Sentinel, then the shared Airflow; wait until healthy
.\scripts\check.ps1            # one pass of every check: PASS / WAIT / FAIL, exit code 0 or 1
.\scripts\check.ps1 -WaitMinutes 12
```

`check.ps1` covers containers, the streaming healthcheck, stalled streams in
`pipeline_heartbeat`, the newest alert, and the latest `sentinel_batch` run in Airflow.
`start.ps1` assumes the shared Airflow lives in `..\dispatchledger\pipelines`; pass
`-AirflowDir <path>` or `-SkipAirflow` otherwise.

| Question | Where to look |
|---|---|
| Is every streaming query alive? | `docker compose ps`: `spark-streaming` shows `healthy`. Grafana "Streaming health" row shows seconds since each query's last heartbeat |
| Are events arriving? | Spark UI (http://localhost:14040), Structured Streaming tab: input rate per query |
| Is the stream keeping up? | Same tab: processing rate should stay above input rate; batch duration below the 30 s trigger |
| Is data being rejected? | Grafana "Quarantine rate" panel, or `marts.mart_data_quality_daily` |
| Did the batch layer run? | Airflow DAG `sentinel_batch`. The first task fails loudly if silver is more than 30 minutes stale |
| Are the rules still good? | `marts.mart_detection_quality`. dbt fails the DAG if precision or recall drops below 0.8 |

### Streaming liveness (watchdog)

A streaming query can hang without failing: a corrupt checkpoint once made the job wait
15 minutes at a time, log nothing, and exit cleanly, over and over. The job now watches
itself ([`health.py`](../src/sentinel/streaming/health.py)):

- Every micro-batch, and every idle trigger (running, no new data), is a heartbeat for that
  query. A watchdog thread in the driver checks every 15 seconds.
- If any query has had no heartbeat for `STALL_TIMEOUT_SECONDS` (default 600), the job logs
  every query's state, marks the silent ones `stalled`, and exits with code 3.
  `restart: unless-stopped` brings it back, and it resumes from its checkpoints.
- New queries get `STALL_START_GRACE_SECONDS` (default 600) on top, because the first batch
  after a restart may be replaying a Kafka backlog.
- Heartbeats are written to `/tmp/sentinel-health.json` (read by the Docker healthcheck,
  `scripts/healthcheck.py`) and upserted into the Postgres table `pipeline_heartbeat`
  (read by Grafana). A Postgres outage never blocks the stream; writes back off for 60 s.

Exit codes in `docker inspect sentinel-lakehouse-spark-streaming-1 --format "{{.State.ExitCode}}"`:
`3` = watchdog restart (a query stalled), `1` = a query failed or stopped.

```
docker compose ps spark-streaming                       # healthy / unhealthy / starting
docker compose exec spark-streaming python3 /opt/sentinel/scripts/healthcheck.py
docker compose logs --tail 200 spark-streaming | Select-String "health|STALLED"   # PowerShell
```

If the job keeps restarting with exit code 3 on the same query, the restart is not fixing
the cause. Check the s3 logs for errors on that query's checkpoint path, then see
"Resetting everything" below; Kafka still holds the events, so nothing is lost.

## Load testing

Measures the running stack, so start it and wait for `check.ps1` to pass first:

```
docker compose build generator
docker compose run --rm generator python -m sentinel.loadtest            # both phases, ~10 min
docker compose run --rm generator python -m sentinel.loadtest --phase latency --probes 5
docker compose run --rm generator python -m sentinel.loadtest --phase throughput --events 500000
```

It ends by printing a Markdown table ready for the README.

**Attack-to-alert latency.** Each probe is 20 failed logins in about 20 seconds against
its own account. Latency runs from the last failed login to the alert row in Postgres. The
number is dominated by design choices, not compute:

| Component | Typical | Why |
|---|---|---|
| Kafka to bronze, bronze to silver, silver to detector | up to 30 s each | `TRIGGER_INTERVAL` micro-batches |
| Window close | up to 60 s | 5-minute windows sliding every minute |
| Watermark | 2 min | `WATERMARK_DELAY`: waits for late events before closing a window |

Lowering the watermark or trigger interval cuts latency, at the cost of dropping more late
events and running more (smaller) batches.

**Throughput.** Sends a burst of successful logins by one-off accounts, then follows the
heartbeat table until bronze and silver have absorbed it. Bronze reads at most
`MAX_OFFSETS_PER_TRIGGER` (200,000) records per batch, so with a 30 s trigger its ceiling is
about 6,700 events/s unless a batch runs longer than the trigger. Raise the setting in `.env`
to find the real limit of the machine.

**Load-test data never touches the metrics.** Every load-test username ends in
`@loadtest.invalid`. The dbt staging models filter that domain out, so marts, risk scores
and the detection-quality gate are unaffected, and probe alerts are deleted from Postgres
when the run ends (`--keep-alerts` keeps them).

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
docker compose run --rm s3-init python -c "
import boto3, os
s3 = boto3.resource('s3', endpoint_url=os.environ['S3_ENDPOINT'],
    aws_access_key_id=os.environ['S3_ACCESS_KEY'], aws_secret_access_key=os.environ['S3_SECRET_KEY'])
for q in ('detect_brute_force', 'detect_ip_abuse'):
    s3.Bucket('lake').objects.filter(Prefix=f'_checkpoints/{q}/').delete()
"
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
make nuke   # removes Kafka, object store, Postgres and Airflow volumes
make up
```
