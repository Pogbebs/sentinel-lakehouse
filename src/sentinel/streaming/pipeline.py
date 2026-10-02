"""Streaming pipeline: Kafka -> bronze -> silver (+ quarantine) -> real-time detections.

Run every stage in one Spark application (good for a laptop) or one stage per application
(good for scaling each stage independently):

    spark-submit -m sentinel.streaming.pipeline --stages bronze,silver,detect
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from sentinel.common.config import Settings, get_settings
from sentinel.streaming import transforms as T
from sentinel.streaming.alert_sink import AlertSink
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


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--stages", default="bronze,silver,detect")
    args = p.parse_args(argv)
    stages = {x.strip() for x in args.stages.split(",")}

    s = get_settings()
    spark = build_spark("sentinel-streaming", s)
    spark.sparkContext.setLogLevel("WARN")
    if s.table_format == "delta":
        ensure_tables(spark, s)

    queries: list[StreamingQuery] = []
    if "bronze" in stages:
        queries.append(start_bronze(spark, s))
    if "silver" in stages:
        queries += start_silver(spark, s)
    if "detect" in stages:
        queries += start_detections(spark, s)
    log.info("started: %s", [q.name for q in queries])
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
