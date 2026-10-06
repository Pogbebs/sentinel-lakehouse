"""Load-test logic with fakes: probe/benign events, drain detection, latency maths."""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

from sentinel.loadtest import (
    LOADTEST_DOMAIN,
    PROBE_FAILURES,
    StreamProgress,
    benign_event,
    percentile,
    probe_event,
    render_markdown,
    run_latency,
    run_throughput,
)

T0 = datetime(2026, 10, 6, 12, 0, tzinfo=timezone.utc)


class FakeClock:
    def __init__(self) -> None:
        self.t = T0

    def now(self) -> datetime:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.t += timedelta(seconds=seconds)


class FakeProducer:
    def flush(self, *_):
        return 0


class FakeSink:
    def __init__(self) -> None:
        self.events: list[dict] = []
        self.producer = FakeProducer()

    def send(self, event: dict) -> None:
        self.events.append(event)


def test_events_pass_the_silver_contract_and_stay_in_the_loadtest_domain():
    from sentinel.streaming.transforms import VALID_OUTCOMES

    for e in (probe_event("r1", 0, T0), benign_event("r1", 7, T0, random.Random(1))):
        assert e["username"].endswith("@" + LOADTEST_DOMAIN)
        assert e["outcome"] in VALID_OUTCOMES
        assert e["event_ts"].endswith("Z") and e["event_id"]
        assert all(0 <= int(o) <= 255 for o in e["src_ip"].split("."))
    assert probe_event("r1", 0, T0)["outcome"] == "failure"
    assert benign_event("r1", 0, T0, random.Random(1))["outcome"] == "success"


def test_benign_burst_cannot_trigger_streaming_rules():
    """Every benign event is a success by a one-off account: no failures, no repeat users."""
    rng = random.Random(3)
    burst = [benign_event("r2", n, T0, rng) for n in range(2_000)]
    assert len({e["username"] for e in burst}) == len(burst)
    assert {e["outcome"] for e in burst} == {"success"}


def test_stream_progress_counts_new_batches_only_and_finds_drain_time():
    p = StreamProgress("bronze_auth_events", start_batch_id=10)
    p.observe(10, 150, T0)  # before the burst: ignored
    p.observe(11, 120_000, T0 + timedelta(seconds=40))
    p.observe(11, 120_000, T0 + timedelta(seconds=42))  # same batch polled twice
    assert p.finished_at(200_000) is None
    p.observe(12, 80_150, T0 + timedelta(seconds=70))
    assert p.rows == 200_150
    assert p.finished_at(200_000) == T0 + timedelta(seconds=70)
    assert round(p.rate(T0, 200_000)) == round(200_000 / 70)


def test_stream_progress_survives_a_missed_heartbeat():
    p = StreamProgress("silver_auth_events", start_batch_id=5)
    p.observe(7, 90_000, T0 + timedelta(seconds=60))  # batch 6 was never seen
    assert p.finished_at(200_000) is None  # can't tell yet
    p.observe(8, 140, T0 + timedelta(seconds=90))  # background-only batch follows
    assert p.finished_at(200_000) == T0 + timedelta(seconds=60)


def test_percentile_nearest_rank():
    assert percentile([5, 1, 3], 50) == 3
    assert percentile([1, 2, 3, 4], 100) == 4
    assert percentile([7], 95) == 7


class FakePg:
    def __init__(self, clock: FakeClock, alert_after_s: float | None = None) -> None:
        self.clock, self.alert_after_s = clock, alert_after_s
        self.batch = 100
        self.sent_users: list[str] = []

    def first_alerts(self, usernames):
        if self.alert_after_s is None:
            return {}
        at = self.clock.now()
        return {u: at for u in usernames} if at - T0 > timedelta(seconds=self.alert_after_s) else {}

    def heartbeats(self):
        self.batch += 1
        rows = 150_000 if self.batch < 104 else 150
        at = self.clock.now()
        return {
            name: (self.batch, rows, at) for name in ("bronze_auth_events", "silver_auth_events")
        }


def test_latency_phase_measures_from_last_probe_event():
    clock = FakeClock()
    sink, pg = FakeSink(), FakePg(clock, alert_after_s=200)
    result = run_latency(sink, pg, "r3", probes=2, timeout_s=600, now=clock.now, sleep=clock.sleep)
    assert len(sink.events) == 2 * PROBE_FAILURES
    assert result["detected"] == 2
    # probes finish at 20 s and 40 s; alerts appear in the first poll after 200 s (205 s)
    assert result["latencies_s"] == [165.0, 185.0]


def test_latency_phase_reports_undetected_probes():
    clock = FakeClock()
    result = run_latency(
        FakeSink(), FakePg(clock), "r4", probes=1, timeout_s=0, now=clock.now, sleep=clock.sleep
    )
    assert result["detected"] == 0 and "median_s" not in result
    assert "not detected" in render_markdown(result, None)


def test_throughput_phase_and_markdown(monkeypatch):
    clock = FakeClock()
    sink, pg = FakeSink(), FakePg(clock)
    result = run_throughput(
        sink, pg, "r5", events=300_000, timeout_s=60, now=clock.now, sleep=clock.sleep
    )
    assert len(sink.events) == 300_000
    assert result["complete"] and result["bronze_rate_per_s"] and result["silver_batches"] >= 2
    table = render_markdown(None, result)
    assert "Bronze throughput (300,000 event burst)" in table


def test_probe_fires_brute_force_and_benign_burst_fires_nothing(spark):
    """Through the real Spark transforms: a probe is detected, a benign burst is not."""
    from test_transforms import silver

    from sentinel.streaming import transforms as T

    probe = [probe_event("r6", 0, T0 + timedelta(seconds=s)) for s in range(PROBE_FAILURES)]
    rng = random.Random(6)
    benign = [benign_event("r6", n, T0 + timedelta(milliseconds=n), rng) for n in range(500)]

    alerts = T.detect_brute_force(silver(spark, probe + benign)).collect()
    assert {a.entity_value for a in alerts} == {probe[0]["username"]}
    assert T.detect_ip_abuse(silver(spark, benign)).count() == 0
