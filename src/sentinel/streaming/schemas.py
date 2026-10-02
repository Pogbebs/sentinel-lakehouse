"""Data contracts for the auth event stream and the alerts it produces."""

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Contract v1 of `auth.events.v1`. Timestamps arrive as ISO-8601 strings and are parsed in
# silver so a bad timestamp lands in quarantine instead of silently becoming null.
AUTH_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_ts", StringType()),
        StructField("user_id", StringType()),
        StructField("username", StringType()),
        StructField("src_ip", StringType()),
        StructField("geo_country", StringType()),
        StructField("geo_city", StringType()),
        StructField("geo_lat", DoubleType()),
        StructField("geo_lon", DoubleType()),
        StructField("device_id", StringType()),
        StructField("user_agent", StringType()),
        StructField("app", StringType()),
        StructField("auth_method", StringType()),
        StructField("outcome", StringType()),
        StructField("failure_reason", StringType()),
        # Ground-truth labels from the simulator. Used for evaluation only, never detection.
        StructField("attack_type", StringType()),
        StructField("attack_id", StringType()),
    ]
)

# Bronze keeps the payload exactly as received plus transport metadata, so silver can be
# rebuilt from it at any time.
BRONZE_SCHEMA = StructType(
    [
        StructField("kafka_key", StringType()),
        StructField("payload", StringType()),
        StructField("kafka_topic", StringType()),
        StructField("kafka_partition", LongType()),
        StructField("kafka_offset", LongType()),
        StructField("kafka_ts", TimestampType()),
        StructField("ingested_at", TimestampType()),
        StructField("ingest_date", StringType()),
    ]
)

ALERT_COLUMNS = [
    "alert_id",
    "rule",
    "severity",
    "entity_type",
    "entity_value",
    "window_start",
    "window_end",
    "attempts",
    "failures",
    "successes",
    "distinct_users",
    "unknown_user_failures",
    "detected_at",
]
