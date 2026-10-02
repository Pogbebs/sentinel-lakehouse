"""Batch replay of the streaming logic.

Two uses:
1. Offline demo / CI: JSON-lines files -> bronze -> silver -> alerts, no Kafka required.
2. Backfills: rebuild silver and alerts from bronze after a rule or contract change.

Because it calls the exact same transform functions as the streaming jobs, a backfill and
the live stream produce identical results for the same input.
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sentinel.common.config import Settings, get_settings
from sentinel.streaming import transforms as T
from sentinel.streaming.lake import build_spark, read_table, write_batch

log = logging.getLogger("sentinel.replay")


def jsonl_to_bronze(spark: SparkSession, source: str) -> DataFrame:
    """Wrap raw JSON lines in the bronze envelope, as if they had come from Kafka."""
    lines = spark.read.text(source)
    return lines.select(
        F.lit(None).cast("string").alias("kafka_key"),
        F.col("value").alias("payload"),
        F.lit("file-replay").alias("kafka_topic"),
        F.lit(0).cast("long").alias("kafka_partition"),
        F.monotonically_increasing_id().alias("kafka_offset"),
        F.current_timestamp().alias("kafka_ts"),
        F.current_timestamp().alias("ingested_at"),
        F.date_format(F.current_timestamp(), "yyyy-MM-dd").alias("ingest_date"),
    )


def build_silver(spark: SparkSession, bronze: DataFrame, s: Settings) -> dict[str, int]:
    checked = T.with_quality_checks(T.parse_payload(bronze)).persist()
    silver = T.dedupe(T.conform(T.valid_rows(checked)), None)
    quarantine = T.quarantine_rows(checked)
    write_batch(
        silver, s.table("silver", "auth_events"), s, mode="overwrite", partition_by=["event_date"]
    )
    write_batch(
        quarantine,
        s.table("quarantine", "auth_events"),
        s,
        mode="overwrite",
        partition_by=["quarantine_date"],
    )
    stats = {"bronze": checked.count(), "quarantine": quarantine.count()}
    checked.unpersist()
    stats["silver"] = read_table(spark, s.table("silver", "auth_events"), s).count()
    return stats


def build_alerts(spark: SparkSession, s: Settings) -> int:
    silver = read_table(spark, s.table("silver", "auth_events"), s)
    alerts = T.detect_brute_force(silver).unionByName(T.detect_ip_abuse(silver))
    alerts = alerts.withColumn("alert_date", F.to_date("window_start").cast("string"))
    write_batch(
        alerts,
        s.table("gold", "streaming_alerts"),
        s,
        mode="overwrite",
        partition_by=["alert_date"],
    )
    return read_table(spark, s.table("gold", "streaming_alerts"), s).count()


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--source", help="JSON-lines glob; omit to replay from the bronze table")
    args = p.parse_args(argv)

    s = get_settings()
    spark = build_spark("sentinel-replay", s, master="local[*]")
    spark.sparkContext.setLogLevel("WARN")

    if args.source:
        bronze = jsonl_to_bronze(spark, args.source)
        write_batch(
            bronze,
            s.table("bronze", "auth_events"),
            s,
            mode="overwrite",
            partition_by=["ingest_date"],
        )
    bronze = read_table(spark, s.table("bronze", "auth_events"), s)
    stats = build_silver(spark, bronze, s)
    stats["alerts"] = build_alerts(spark, s)
    log.info("replay complete: %s", stats)
    spark.stop()


if __name__ == "__main__":
    main()
