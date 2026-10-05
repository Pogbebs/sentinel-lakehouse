"""Streaming pipeline: Kafka -> bronze -> silver (+ quarantine) -> real-time detections.

Run every stage in one Spark application (good for a laptop) or one stage per application
(good for scaling each stage independently):

    spark-submit -m sentinel.streaming.pipeline --stages bronze,silver,detect
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery, StreamingQueryListener

from sentinel.common.config import Settings, get_settings
from sentinel.streaming import transforms as T
from sentinel.streaming.alert_sink import AlertSink
from sentinel.streaming.health import DEFAULT_HEALTH_FILE, HealthMonitor
from sentinel.streaming.lake import build_spark

log = logging.getLogger("sentinel.streaming")


def start_bronze(spark: SparkSession, s: Settings) -> StreamingQuery:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", s.kafka_bootstrap)
        .option("subscribe", s.auth_topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 200_000)  # back-pressure: bound each micro-batch
        .option("failOnDataLoss", "false")
        .load()
    )
    bronze = raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("payload"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").cast("long").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.current_timestamp().alias("ingested_at"),
    ).withColumn("ingest_date", F.date_format("ingested_at", "yyyy-MM-dd"))
    return (
        bronze.writeStream.format(s.table_format)
        .queryName("bronze_auth_events")
        .option("checkpointLocation", s.checkpoint("bronze_auth_events"))
        .partitionBy("ingest_date")
        .trigger(processingTime=s.trigger_interval)
        .outputMode("append")
        .start(s.table("bronze", "auth_events"))
    )


def _checked_stream(spark: SparkSession, s: Settings):
    bronze = spark.readStream.format(s.table_format).load(s.table("bronze", "auth_events"))
    return T.with_quality_checks(T.parse_payload(bronze))


def start_silver(spark: SparkSession, s: Settings) -> list[StreamingQuery]:
    checked = _checked_stream(spark, s)
    silver = T.dedupe(T.conform(T.valid_rows(checked)), s.watermark)
    q_silver = (
        silver.writeStream.format(s.table_format)
        .queryName("silver_auth_events")
        .option("checkpointLocation", s.checkpoint("silver_auth_events"))
        .partitionBy("event_date")
        .trigger(processingTime=s.trigger_interval)
        .outputMode("append")
        .start(s.table("silver", "auth_events"))
    )
    q_quarantine = (
        T.quarantine_rows(checked)
        .writeStream.format(s.table_format)
        .queryName("quarantine_auth_events")
        .option("checkpointLocation", s.checkpoint("quarantine_auth_events"))
        .partitionBy("quarantine_date")
        .trigger(processingTime=s.trigger_interval)
        .outputMode("append")
        .start(s.table("quarantine", "auth_events"))
    )
    return [q_silver, q_quarantine]


def start_detections(
    spark: SparkSession, s: Settings, cfg: T.DetectionConfig = T.DEFAULT_DETECTION
) -> list[StreamingQuery]:
    silver = spark.readStream.format(s.table_format).load(s.table("silver", "auth_events"))
    queries = []
    for name, fn in (
        ("detect_brute_force", T.detect_brute_force),
        ("detect_ip_abuse", T.detect_ip_abuse),
    ):
        alerts = fn(silver, cfg, s.watermark)
        queries.append(
            alerts.writeStream.queryName(name)
            .option("checkpointLocation", s.checkpoint(name))
            .outputMode("append")  # a window is emitted once, after the watermark passes it
            .trigger(processingTime=s.trigger_interval)
            .foreachBatch(AlertSink(s, name))
            .start()
        )
    return queries


def ensure_tables(spark: SparkSession, s: Settings) -> None:
    """Create empty bronze/silver tables so downstream streams can start before data arrives.

    The silver schema is derived by running the real transforms over an empty bronze frame,
    so it can never drift from the code that writes it.
    """
    from delta.tables import DeltaTable

    from sentinel.streaming.schemas import BRONZE_SCHEMA

    empty_bronze = spark.createDataFrame([], BRONZE_SCHEMA)
    empty_silver = T.conform(T.valid_rows(T.with_quality_checks(T.parse_payload(empty_bronze))))
    for path, df, part in (
        (s.table("bronze", "auth_events"), empty_bronze, "ingest_date"),
        (s.table("silver", "auth_events"), empty_silver, "event_date"),
    ):
        if not DeltaTable.isDeltaTable(spark, path):
            df.write.format("delta").partitionBy(part).mode("append").save(path)
            log.info("created empty table %s", path)


class ProgressLogger(StreamingQueryListener):
    """Log one line per micro-batch and the reason any query stops.

    Spark's own logs are set to WARN to keep them readable, which also hides the fact
    that a query has stopped. This listener makes progress and termination visible, and
    feeds every batch and idle trigger to the HealthMonitor as a heartbeat.
    """

    def __init__(self, health: HealthMonitor | None = None) -> None:
        super().__init__()
        self.health = health

    def onQueryStarted(self, event) -> None:
        log.info("query started: %s (id=%s)", event.name, event.id)
        if self.health and event.name:
            self.health.register(event.name, query_id=str(event.id))

    def onQueryProgress(self, event) -> None:
        p = event.progress
        log.info(
            "progress %-24s batch=%-6s rows=%-7s rows/s=%-8.1f took=%sms",
            p.name,
            p.batchId,
            p.numInputRows,
            p.processedRowsPerSecond or 0.0,
            p.durationMs.get("triggerExecution"),
        )
        if self.health:
            self.health.beat(
                p.name, query_id=str(p.id), batch_id=p.batchId, input_rows=p.numInputRows
            )

    def onQueryIdle(self, event) -> None:  # Spark >= 3.5: no new data this trigger
        if self.health:
            self.health.beat(query_id=str(event.id), status="idle")

    def onQueryTerminated(self, event) -> None:
        if event.exception:
            log.error("query terminated with error (id=%s): %s", event.id, event.exception)
        else:
            log.error("query terminated WITHOUT an error (id=%s)", event.id)
        if self.health:
            self.health.mark(query_id=str(event.id), status="terminated")


def build_health_monitor(s: Settings) -> HealthMonitor:
    """Watchdog settings come from the environment so they can be tuned per deployment."""
    health = HealthMonitor(
        stall_seconds=float(os.environ.get("STALL_TIMEOUT_SECONDS", "600")),
        start_grace_seconds=float(os.environ.get("STALL_START_GRACE_SECONDS", "600")),
        health_file=os.environ.get("HEALTH_FILE", DEFAULT_HEALTH_FILE),
        pg_dsn=s.pg_dsn,
    )
    health.clear_file()  # /tmp survives a container restart; never report the last run
    return health


def wait_for_failure(spark: SparkSession, queries: list[StreamingQuery]) -> int:
    """Block until any query stops, report every query's state, return an exit code.

    A streaming job should never finish, so any termination is a failure: exiting
    non-zero makes the container restart visible instead of looking like success.
    """
    try:
        spark.streams.awaitAnyTermination()
    except Exception as exc:  # StreamingQueryException carries the root cause
        log.error("a streaming query failed: %s", exc)
    for q in queries:
        err = q.exception()
        log.error(
            "query %-24s active=%s status=%s error=%s",
            q.name,
            q.isActive,
            q.status.get("message"),
            err.desc if err else None,
        )
        if q.lastProgress:
            log.error("query %-24s last progress: %s", q.name, q.lastProgress)
    return 1


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--stages", default="bronze,silver,detect")
    args = p.parse_args(argv)
    stages = {x.strip() for x in args.stages.split(",")}

    s = get_settings()
    spark = build_spark("sentinel-streaming", s)
    spark.sparkContext.setLogLevel("WARN")
    health = build_health_monitor(s)
    spark.streams.addListener(ProgressLogger(health))
    if s.table_format == "delta":
        ensure_tables(spark, s)

    queries: list[StreamingQuery] = []
    if "bronze" in stages:
        queries.append(start_bronze(spark, s))
    if "silver" in stages:
        queries += start_silver(spark, s)
    if "detect" in stages:
        queries += start_detections(spark, s)
    for q in queries:  # onQueryStarted is asynchronous; registering here closes the gap
        health.register(q.name, query_id=str(q.id))
    health.start()
    log.info(
        "started: %s (watchdog restarts the job after %ss without a heartbeat)",
        [q.name for q in queries],
        int(health.stall_seconds),
    )
    sys.exit(wait_for_failure(spark, queries))


if __name__ == "__main__":
    main()
