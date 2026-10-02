"""Pure DataFrame transformations shared by the streaming jobs and the offline/batch runner.

Every function works on both batch and streaming DataFrames, which is what makes the
streaming logic unit-testable with small in-memory fixtures.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from sentinel.streaming.schemas import ALERT_COLUMNS, AUTH_EVENT_SCHEMA

IPV4 = r"^((25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(25[0-5]|2[0-4]\d|1?\d?\d)$"
VALID_OUTCOMES = ("success", "failure")


# --------------------------------------------------------------------------- bronze -> silver
def parse_payload(bronze: DataFrame) -> DataFrame:
    """Parse the raw JSON payload against the v1 contract, keeping the original string."""
    parsed = F.from_json(F.col("payload"), AUTH_EVENT_SCHEMA, {"mode": "PERMISSIVE"})
    return bronze.withColumn("e", parsed).select(
        "payload", "kafka_partition", "kafka_offset", "ingested_at", "e.*"
    )


def _check(condition: Column, code: str) -> Column:
    return F.when(condition, F.lit(code))


def with_quality_checks(events: DataFrame) -> DataFrame:
    """Attach `dq_errors` (array of rule codes) and the parsed event timestamp."""
    ts = F.to_timestamp(F.col("event_ts"))
    unparseable = (
        F.col("event_id").isNull()
        & F.col("event_ts").isNull()
        & F.col("username").isNull()
        & F.col("outcome").isNull()
    )
    checks = F.array(
        _check(F.col("event_id").isNull(), "missing_event_id"),
        _check(ts.isNull(), "invalid_event_ts"),
        _check(F.col("username").isNull() | (F.trim("username") == ""), "missing_username"),
        _check(
            ~F.coalesce(F.col("outcome").isin(*VALID_OUTCOMES), F.lit(False)), "invalid_outcome"
        ),
        _check(~F.coalesce(F.col("src_ip").rlike(IPV4), F.lit(False)), "invalid_ip"),
        _check(
            ~F.coalesce(
                F.col("geo_lat").between(-90, 90) & F.col("geo_lon").between(-180, 180),
                F.lit(False),
            ),
            "invalid_geo",
        ),
    )
    errors = F.when(unparseable, F.array(F.lit("unparseable_payload"))).otherwise(
        F.filter(checks, lambda c: c.isNotNull())
    )
    return events.withColumn("event_time", ts).withColumn("dq_errors", errors)


def valid_rows(checked: DataFrame) -> DataFrame:
    return checked.filter(F.size("dq_errors") == 0).drop("dq_errors")


def quarantine_rows(checked: DataFrame) -> DataFrame:
    return checked.filter(F.size("dq_errors") > 0).select(
        "payload",
        "kafka_partition",
        "kafka_offset",
        "ingested_at",
        "dq_errors",
        F.current_timestamp().alias("quarantined_at"),
        F.date_format(F.current_timestamp(), "yyyy-MM-dd").alias("quarantine_date"),
    )


def conform(valid: DataFrame) -> DataFrame:
    """Shape validated events into the silver contract."""
    username = F.lower(F.trim("username"))
    return valid.select(
        "event_id",
        "event_time",
        F.to_date("event_time").cast("string").alias("event_date"),
        "user_id",
        username.alias("username"),
        F.sha2(username, 256).alias("username_sha256"),
        "src_ip",
        "geo_country",
        "geo_city",
        "geo_lat",
        "geo_lon",
        "device_id",
        "user_agent",
        "app",
        "auth_method",
        "outcome",
        "failure_reason",
        (F.col("outcome") == "failure").alias("is_failure"),
        F.coalesce("attack_type", F.lit("none")).alias("label_attack_type"),
        F.col("attack_id").alias("label_attack_id"),
        "kafka_partition",
        "kafka_offset",
        "ingested_at",
    )


def dedupe(silver: DataFrame, watermark: str | None) -> DataFrame:
    """Producers are at-least-once, so the same event_id can arrive twice.

    Streaming: bounded state via the watermark. Batch: a plain distinct on the key.
    """
    if silver.isStreaming:
        return silver.withWatermark(
            "event_time", watermark or "2 minutes"
        ).dropDuplicatesWithinWatermark(["event_id"])
    return silver.dropDuplicates(["event_id"])


# --------------------------------------------------------------------------- detections
@dataclass(frozen=True)
class DetectionConfig:
    window: str = "5 minutes"
    slide: str = "1 minute"
    brute_force_min_failures: int = 15  # tuned up from 10, see docs/detection-tuning.md
    brute_force_min_failure_ratio: float = 0.8
    stuffing_min_users: int = 20
    stuffing_min_unknown_ratio: float = 0.25
    spray_min_users: int = 10
    spray_max_attempts_per_user: float = 1.5
    spray_min_failure_ratio: float = 0.85


DEFAULT_DETECTION = DetectionConfig()


def _windowed(silver: DataFrame, cfg: DetectionConfig, watermark: str | None) -> DataFrame:
    if silver.isStreaming:
        silver = silver.withWatermark("event_time", watermark or "2 minutes")
    return silver.withColumn("w", F.window("event_time", cfg.window, cfg.slide))


def _finalise(df: DataFrame, rule: str | Column, entity_type: str) -> DataFrame:
    """Project an aggregate onto the alert contract with a deterministic alert_id.

    The id hashes (rule, entity, window start), so replays after a failure regenerate the
    same ids and the sinks can de-duplicate.
    """
    rule_col = F.lit(rule) if isinstance(rule, str) else rule
    return (
        df.withColumn("rule", rule_col)
        .withColumn("entity_type", F.lit(entity_type))
        .withColumn("window_start", F.col("w.start"))
        .withColumn("window_end", F.col("w.end"))
        .withColumn(
            "alert_id",
            F.substring(
                F.sha2(
                    F.concat_ws("|", "rule", "entity_value", F.col("window_start").cast("string")),
                    256,
                ),
                1,
                32,
            ),
        )
        .withColumn("detected_at", F.current_timestamp())
        .select(*ALERT_COLUMNS)
    )


def detect_brute_force(
    silver: DataFrame, cfg: DetectionConfig = DEFAULT_DETECTION, watermark: str | None = None
) -> DataFrame:
    """Many failed passwords against one account inside a short window."""
    agg = (
        _windowed(silver, cfg, watermark)
        .groupBy("w", F.col("username").alias("entity_value"))
        .agg(
            F.count("*").alias("attempts"),
            F.sum(F.col("is_failure").cast("int")).alias("failures"),
            F.sum((~F.col("is_failure")).cast("int")).alias("successes"),
            F.lit(1).cast("long").alias("distinct_users"),
            F.sum((F.col("failure_reason") == "unknown_user").cast("int")).alias(
                "unknown_user_failures"
            ),
        )
        .filter(
            (F.col("failures") >= cfg.brute_force_min_failures)
            & (F.col("failures") / F.col("attempts") >= cfg.brute_force_min_failure_ratio)
        )
        # A success at the end of a burst of failures means the guess probably worked.
        .withColumn("severity", F.when(F.col("successes") > 0, "critical").otherwise("high"))
    )
    return _finalise(agg, "brute_force", "account")


def detect_ip_abuse(
    silver: DataFrame, cfg: DetectionConfig = DEFAULT_DETECTION, watermark: str | None = None
) -> DataFrame:
    """One source IP touching many accounts: credential stuffing or password spraying.

    Stuffing replays leaked (email, password) pairs, so many usernames do not exist here.
    Spraying tries one common password against real accounts, roughly once each.
    """
    agg = (
        _windowed(silver, cfg, watermark)
        .groupBy("w", F.col("src_ip").alias("entity_value"))
        .agg(
            F.count("*").alias("attempts"),
            F.sum(F.col("is_failure").cast("int")).alias("failures"),
            F.sum((~F.col("is_failure")).cast("int")).alias("successes"),
            F.approx_count_distinct("username").alias("distinct_users"),
            F.sum((F.col("failure_reason") == "unknown_user").cast("int")).alias(
                "unknown_user_failures"
            ),
        )
    )
    unknown_ratio = F.col("unknown_user_failures") / F.col("attempts")
    failure_ratio = F.col("failures") / F.col("attempts")
    per_user = F.col("attempts") / F.col("distinct_users")
    stuffing = (F.col("distinct_users") >= cfg.stuffing_min_users) & (
        unknown_ratio >= cfg.stuffing_min_unknown_ratio
    )
    spray = (
        (F.col("distinct_users") >= cfg.spray_min_users)
        & (unknown_ratio < cfg.stuffing_min_unknown_ratio)
        & (failure_ratio >= cfg.spray_min_failure_ratio)
        & (per_user <= cfg.spray_max_attempts_per_user)
    )
    classified = (
        agg.withColumn(
            "rule_name",
            F.when(stuffing, "credential_stuffing").when(spray, "password_spray"),
        )
        .filter(F.col("rule_name").isNotNull())
        .withColumn("severity", F.when(F.col("successes") > 0, "critical").otherwise("high"))
    )
    return _finalise(classified, F.col("rule_name"), "source_ip")
