"""Alert fan-out used inside foreachBatch: lake (audit), Postgres (serving), Kafka (downstream).

Each sink is idempotent so a micro-batch replayed after a crash does not double-count:
- Delta: `txnAppId` + `txnVersion` make the append exactly-once per (query, batch_id).
- Postgres: `ON CONFLICT (alert_id) DO NOTHING` on the deterministic alert id.
- Kafka: keyed by alert_id; consumers de-duplicate on key (at-least-once by design).
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sentinel.common.config import Settings

log = logging.getLogger(__name__)

UPSERT_SQL = """
INSERT INTO alerts (alert_id, rule, severity, entity_type, entity_value, window_start,
                    window_end, attempts, failures, successes, distinct_users,
                    unknown_user_failures, detected_at)
VALUES %s
ON CONFLICT (alert_id) DO NOTHING
"""


class AlertSink:
    def __init__(
        self, settings: Settings, query_name: str, *, postgres: bool = True, kafka: bool = True
    ) -> None:
        self.s = settings
        self.query_name = query_name
        self.postgres = postgres
        self.kafka = kafka

    def __call__(self, batch: DataFrame, batch_id: int) -> None:
        batch = batch.persist()
        try:
            n = batch.count()
            if n == 0:
                return
            self._to_lake(batch, batch_id)
            if self.postgres:
                self._to_postgres(batch)
            if self.kafka:
                self._to_kafka(batch)
            log.info("%s batch %s: %s alerts", self.query_name, batch_id, n)
        finally:
            batch.unpersist()

    def _to_lake(self, batch: DataFrame, batch_id: int) -> None:
        w = (
            batch.withColumn("alert_date", F.to_date("window_start").cast("string"))
            .write.format(self.s.table_format)
            .mode("append")
            .partitionBy("alert_date")
        )
        if self.s.table_format == "delta":
            w = w.option("txnAppId", self.query_name).option("txnVersion", batch_id)
        w.save(self.s.table("gold", "streaming_alerts"))

    def _to_postgres(self, batch: DataFrame) -> None:
        import psycopg2
        from psycopg2.extras import execute_values

        rows = [tuple(r) for r in batch.collect()]  # alerts are small by construction
        with psycopg2.connect(self.s.pg_dsn) as conn, conn.cursor() as cur:
            execute_values(cur, UPSERT_SQL, rows)

    def _to_kafka(self, batch: DataFrame) -> None:
        (
            batch.select(
                F.col("alert_id").alias("key"), F.to_json(F.struct(*batch.columns)).alias("value")
            )
            .write.format("kafka")
            .option("kafka.bootstrap.servers", self.s.kafka_bootstrap)
            .option("topic", self.s.alerts_topic)
            .save()
        )
