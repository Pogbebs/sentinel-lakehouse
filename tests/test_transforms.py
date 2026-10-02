import json
from datetime import datetime, timedelta, timezone

import pytest
from pyspark.sql import functions as F

from sentinel.streaming import transforms as T
from sentinel.streaming.schemas import BRONZE_SCHEMA

T0 = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)


def event(
    i,
    *,
    sec=0,
    username="alice@x.org",
    ip="23.1.2.3",
    outcome="failure",
    reason="bad_password",
    **over,
):
    e = {
        "event_id": f"e{i}",
        "event_ts": (T0 + timedelta(seconds=sec)).isoformat(),
        "user_id": "u1",
        "username": username,
        "src_ip": ip,
        "geo_country": "US",
        "geo_city": "Houston",
        "geo_lat": 29.7,
        "geo_lon": -95.3,
        "device_id": "d1",
        "user_agent": "ua",
        "app": "vpn",
        "auth_method": "password",
        "outcome": outcome,
        "failure_reason": reason if outcome == "failure" else None,
        "attack_type": "none",
        "attack_id": None,
    }
    e.update(over)
    return e


def bronze(spark, events):
    rows = [
        (None, json.dumps(e) if isinstance(e, dict) else e, "t", 0, i, T0, T0, "2026-10-01")
        for i, e in enumerate(events)
    ]
    return spark.createDataFrame(rows, BRONZE_SCHEMA)


def silver(spark, events):
    checked = T.with_quality_checks(T.parse_payload(bronze(spark, events)))
    return T.dedupe(T.conform(T.valid_rows(checked)), None)


# --------------------------------------------------------------------------- quality
def test_quality_checks_route_bad_rows_to_quarantine(spark):
    rows = [
        event(1),
        event(2, outcome="SUCESS"),
        event(3, geo_lat=999.0),
        event(4, event_ts="not-a-time"),
        event(5, src_ip="999.1.1.1"),
        "{not json",
    ]
    checked = T.with_quality_checks(T.parse_payload(bronze(spark, rows)))
    assert T.valid_rows(checked).count() == 1
    errors = {r.kafka_offset: r.dq_errors for r in T.quarantine_rows(checked).collect()}
    assert errors == {
        1: ["invalid_outcome"],
        2: ["invalid_geo"],
        3: ["invalid_event_ts"],
        4: ["invalid_ip"],
        5: ["unparseable_payload"],
    }


def test_conform_normalises_and_hashes_username(spark):
    row = silver(spark, [event(1, username="  Alice@X.org ")]).first()
    assert row.username == "alice@x.org"
    assert len(row.username_sha256) == 64
    assert row.is_failure is True
    assert row.label_attack_type == "none"


def test_dedupe_drops_producer_retries(spark):
    assert silver(spark, [event(1), event(1), event(2)]).count() == 2


# --------------------------------------------------------------------------- detections
def test_brute_force_fires_on_burst_and_flags_compromise(spark):
    burst = [event(i, sec=i * 5) for i in range(20)]
    burst.append(event(99, sec=101, outcome="success"))
    alerts = T.detect_brute_force(silver(spark, burst)).collect()
    assert alerts, "expected a brute-force alert"
    assert {a.rule for a in alerts} == {"brute_force"}
    assert {a.entity_value for a in alerts} == {"alice@x.org"}
    assert "critical" in {a.severity for a in alerts}


def test_brute_force_ignores_a_forgetful_user(spark):
    """12 slow failures is a person who forgot a password, not an attack (threshold 15)."""
    alerts = T.detect_brute_force(silver(spark, [event(i, sec=i * 15) for i in range(12)]))
    assert alerts.count() == 0


def test_ip_abuse_classifies_stuffing(spark):
    rows = [
        event(
            i,
            sec=i * 2,
            username=f"user{i}@x.org",
            reason="unknown_user" if i % 2 else "bad_password",
        )
        for i in range(40)
    ]
    rules = {a.rule for a in T.detect_ip_abuse(silver(spark, rows)).collect()}
    assert rules == {"credential_stuffing"}


def test_ip_abuse_classifies_spray(spark):
    rows = [event(i, sec=i * 4, username=f"user{i}@x.org") for i in range(25)]
    rules = {a.rule for a in T.detect_ip_abuse(silver(spark, rows)).collect()}
    assert rules == {"password_spray"}


def test_office_nat_is_not_an_attack(spark):
    """Hundreds of colleagues behind one NAT IP, mostly succeeding: no alert."""
    rows = [
        event(i, sec=i, username=f"user{i}@x.org", outcome="failure" if i % 15 == 0 else "success")
        for i in range(200)
    ]
    assert T.detect_ip_abuse(silver(spark, rows)).count() == 0


def test_alert_ids_are_deterministic(spark):
    rows = [event(i, sec=i * 5) for i in range(20)]
    a = sorted(r.alert_id for r in T.detect_brute_force(silver(spark, rows)).collect())
    b = sorted(r.alert_id for r in T.detect_brute_force(silver(spark, rows)).collect())
    assert a == b and len(set(a)) == len(a)


# --------------------------------------------------------------------------- streaming
@pytest.mark.parametrize("detector", [T.detect_brute_force, T.detect_ip_abuse])
def test_detectors_run_as_structured_streams(spark, tmp_path, detector):
    """Same functions, streaming source: proves every operator is streaming-compatible
    (watermarks, approx distinct, append mode) and that windows are emitted once closed."""
    spray = [event(i, sec=i * 3, username=f"user{i}@x.org") for i in range(30)]
    brute = [event(100 + i, sec=i * 3, ip="89.1.1.1") for i in range(30)]
    attack = spray + brute
    # A late event an hour later moves the watermark past the attack windows.
    attack.append(event(999, sec=3600, outcome="success"))
    batch = silver(spark, attack)
    src = tmp_path / "silver"
    batch.write.parquet(str(src))

    stream = spark.readStream.schema(batch.schema).parquet(str(src))
    stream = stream.withColumn("event_time", F.col("event_time"))
    out = tmp_path / "alerts"
    q = (
        detector(stream, watermark="1 minute")
        .writeStream.format("parquet")
        .option("checkpointLocation", str(tmp_path / "chk"))
        .outputMode("append")
        .trigger(availableNow=True)
        .start(str(out))
    )
    q.awaitTermination(120)
    assert q.exception() is None
    assert spark.read.parquet(str(out)).count() > 0
