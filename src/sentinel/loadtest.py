"""Load test for the running stack: attack-to-alert latency and ingest throughput.

Runs inside the Docker network, next to the live generator:

    docker compose run --rm generator python -m sentinel.loadtest

Two phases:

latency
    Sends a few short brute-force bursts (20 failed logins in ~20 s) against dedicated
    probe accounts, timestamped with the real clock, then polls the Postgres `alerts`
    table. Latency = alert inserted - last event of the burst sent. It includes Kafka,
    three 30 s micro-batch stages, the 5-minute sliding window closing and the 2-minute
    watermark, i.e. what an analyst would actually experience.

throughput
    Blasts N benign events into Kafka as fast as the producer can, then follows the
    streaming job's own heartbeats (`pipeline_heartbeat`) until bronze and silver have
    both absorbed them. Rate = rows processed / time from first send to last batch.

All load-test traffic uses usernames under ``@loadtest.invalid``. The dbt staging models
filter that domain out, and probe alerts are deleted from Postgres afterwards, so a load
test never changes the dashboards' numbers or the detection-quality gate.
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import statistics
import time
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone

from sentinel.common.config import get_settings

log = logging.getLogger("sentinel.loadtest")

LOADTEST_DOMAIN = "loadtest.invalid"
PROBE_FAILURES = 20  # comfortably above the brute-force threshold of 15
STREAMS = ("bronze_auth_events", "silver_auth_events")


def _iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _base_event(ts: datetime, username: str, ip: str, outcome: str) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_ts": _iso(ts),
        "user_id": None,
        "username": username,
        "src_ip": ip,
        "geo_country": "US",
        "geo_city": "Houston",
        "geo_lat": 29.7604,
        "geo_lon": -95.3698,
        "device_id": "loadtest",
        "user_agent": "sentinel-loadtest/1.0",
        "app": "vpn",
        "auth_method": "password",
        "outcome": outcome,
        "failure_reason": "bad_password" if outcome == "failure" else None,
        "attack_type": "none",
        "attack_id": None,
    }


def probe_username(run_id: str, i: int) -> str:
    return f"probe-{run_id}-{i}@{LOADTEST_DOMAIN}"


def probe_event(run_id: str, i: int, ts: datetime) -> dict:
    """One failed login of a brute-force probe. Each probe has its own documentation IP."""
    return _base_event(ts, probe_username(run_id, i), f"198.51.100.{10 + i}", "failure")


def benign_event(run_id: str, n: int, ts: datetime, rng: random.Random) -> dict:
    """A successful login by a one-off account: triggers no rule, in stream or batch."""
    ip = f"10.{rng.randrange(256)}.{rng.randrange(256)}.{rng.randrange(1, 255)}"
    return _base_event(ts, f"lt-{run_id}-{n}@{LOADTEST_DOMAIN}", ip, "success")


# ---------------------------------------------------------------------- pure helpers
def percentile(values: list[float], pct: float) -> float:
    """Nearest-rank percentile; fine for the handful of samples a load test produces."""
    ordered = sorted(values)
    k = max(0, min(len(ordered) - 1, round(pct / 100 * len(ordered) + 0.5) - 1))
    return ordered[k]


@dataclass
class StreamProgress:
    """Batches one streaming query completed after the burst started."""

    name: str
    start_batch_id: int | None
    batches: dict[int, tuple[int, datetime]] = field(default_factory=dict)  # id -> rows, at

    def observe(self, batch_id: int | None, rows: int | None, at: datetime) -> None:
        if batch_id is None or rows is None:
            return
        if self.start_batch_id is not None and batch_id <= self.start_batch_id:
            return
        self.batches.setdefault(batch_id, (rows, at))

    @property
    def rows(self) -> int:
        return sum(r for r, _ in self.batches.values())

    def finished_at(self, events: int) -> datetime | None:
        """When the burst was fully absorbed.

        Normally the batch where the running total reaches ``events``. Heartbeats are
        polled, so a batch can occasionally be missed; then the burst counts as absorbed
        at the last large batch once a small (background-only) batch follows it.
        """
        ordered = [self.batches[b] for b in sorted(self.batches)]
        total = 0
        for rows, at in ordered:
            total += rows
            if total >= events:
                return at
        small = max(1_000, events // 50)
        large = [i for i, (rows, _) in enumerate(ordered) if rows >= small]
        if large and large[-1] < len(ordered) - 1:
            return ordered[large[-1]][1]
        return None

    def rate(self, started_at: datetime, events: int) -> float | None:
        end = self.finished_at(events)
        if end is None or end <= started_at:
            return None
        return events / (end - started_at).total_seconds()


# ---------------------------------------------------------------------- Postgres
class Postgres:
    def __init__(self, dsn: str) -> None:
        import psycopg2

        self.conn = psycopg2.connect(dsn, connect_timeout=10)
        self.conn.autocommit = True

    def query(self, sql: str, params: Iterable = ()) -> list[tuple]:
        with self.conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            return cur.fetchall() if cur.description else []

    def heartbeats(self) -> dict[str, tuple[int | None, int | None, datetime]]:
        rows = self.query(
            "select query_name, batch_id, input_rows, last_seen_at from pipeline_heartbeat"
        )
        return {name: (batch, n, at) for name, batch, n, at in rows}

    def first_alerts(self, usernames: list[str]) -> dict[str, datetime]:
        rows = self.query(
            "select entity_value, min(inserted_at) from alerts "
            "where entity_value = any(%s) group by 1",
            [usernames],
        )
        return dict(rows)

    def delete_probe_alerts(self) -> int:
        with self.conn.cursor() as cur:
            cur.execute("delete from alerts where entity_value like %s", [f"%@{LOADTEST_DOMAIN}"])
            return cur.rowcount

    def close(self) -> None:
        self.conn.close()


# ---------------------------------------------------------------------- phases
def run_latency(
    sink,
    pg: Postgres,
    run_id: str,
    probes: int,
    timeout_s: float,
    *,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    sleep: Callable[[float], None] = time.sleep,
) -> dict:
    sent_at: dict[str, datetime] = {}
    for i in range(probes):
        user = probe_username(run_id, i)
        log.info("probe %s/%s: %s failed logins as %s", i + 1, probes, PROBE_FAILURES, user)
        for _ in range(PROBE_FAILURES):
            sink.send(probe_event(run_id, i, now()))
            sleep(1.0)
        sink.producer.flush(30)
        sent_at[user] = now()

    log.info("waiting for alerts (up to %.0f min)...", timeout_s / 60)
    deadline = time.monotonic() + timeout_s
    found: dict[str, datetime] = {}
    while len(found) < len(sent_at) and time.monotonic() < deadline:
        sleep(5)
        found = pg.first_alerts(list(sent_at))
        log.info("  %s/%s probes alerted", len(found), len(sent_at))

    latencies = sorted((found[u] - sent_at[u]).total_seconds() for u in found)
    result = {
        "probes": probes,
        "detected": len(found),
        "latencies_s": [round(x, 1) for x in latencies],
    }
    if latencies:
        result |= {
            "median_s": round(statistics.median(latencies), 1),
            "max_s": round(max(latencies), 1),
        }
    return result


def run_throughput(
    sink,
    pg: Postgres,
    run_id: str,
    events: int,
    timeout_s: float,
    *,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    sleep: Callable[[float], None] = time.sleep,
) -> dict:
    before = pg.heartbeats()
    missing = [s for s in STREAMS if s not in before]
    if missing:
        raise SystemExit(f"no heartbeat yet for {missing}: is the streaming job running?")
    progress = {s: StreamProgress(s, before[s][0]) for s in STREAMS}

    rng = random.Random(run_id)
    started = now()
    t0 = time.monotonic()
    for n in range(events):
        sink.send(benign_event(run_id, n, now(), rng))
        if n and n % 50_000 == 0:
            log.info("  sent %s events", n)
    sink.producer.flush(120)
    send_s = time.monotonic() - t0
    log.info(
        "sent %s events in %.1f s (%.0f/s); following the stream...",
        events,
        send_s,
        events / max(send_s, 1e-9),
    )

    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        for name, (batch, rows, at) in pg.heartbeats().items():
            if name in progress:
                progress[name].observe(batch, rows, at)
        log.info(
            "  rows processed: "
            + "  ".join(f"{p.name.split('_')[0]}={p.rows:,}" for p in progress.values())
        )
        if all(p.finished_at(events) for p in progress.values()):
            break
        sleep(2)

    result: dict = {
        "events": events,
        "producer_rate_per_s": round(events / max(send_s, 1e-9)),
        "complete": all(p.finished_at(events) for p in progress.values()),
    }
    for p in progress.values():
        key = p.name.split("_")[0]
        rate = p.rate(started, events)
        finished = p.finished_at(events)
        result[f"{key}_batches"] = len(p.batches)
        result[f"{key}_rate_per_s"] = round(rate) if rate else None
        if finished:
            result[f"{key}_drained_s"] = round((finished - started).total_seconds(), 1)
    return result


def render_markdown(latency: dict | None, throughput: dict | None) -> str:
    lines = ["| Measure | Result |", "|---|---|"]
    if latency:
        if latency.get("latencies_s"):
            lines.append(
                f"| Attack to alert (brute force, {latency['detected']}/{latency['probes']} "
                f"probes) | median {latency['median_s']:.0f} s, max {latency['max_s']:.0f} s |"
            )
        else:
            lines.append(f"| Attack to alert | not detected ({latency['probes']} probes) |")
    if throughput:
        lines.append(f"| Producer rate | {throughput['producer_rate_per_s']:,} events/s |")
        for key in ("bronze", "silver"):
            rate = throughput.get(f"{key}_rate_per_s")
            drained = throughput.get(f"{key}_drained_s")
            if rate:
                lines.append(
                    f"| {key.title()} throughput ({throughput['events']:,} event burst) "
                    f"| {rate:,} events/s, backlog cleared in {drained:.0f} s |"
                )
    return "\n".join(lines)


# ---------------------------------------------------------------------- CLI
def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--phase", choices=["all", "latency", "throughput"], default="all")
    p.add_argument("--probes", type=int, default=3, help="brute-force probes (latency)")
    p.add_argument("--events", type=int, default=100_000, help="burst size (throughput)")
    p.add_argument("--timeout-minutes", type=float, default=15)
    p.add_argument("--keep-alerts", action="store_true", help="leave probe alerts in Postgres")
    args = p.parse_args(argv)

    from sentinel.generator.sinks import KafkaSink

    s = get_settings()
    run_id = uuid.uuid4().hex[:8]
    log.info("load test %s against %s / %s", run_id, s.kafka_bootstrap, s.auth_topic)
    sink = KafkaSink(s.kafka_bootstrap, s.auth_topic)
    pg = Postgres(s.pg_dsn)
    latency = throughput = None
    try:
        if args.phase in ("all", "latency"):
            latency = run_latency(sink, pg, run_id, args.probes, args.timeout_minutes * 60)
            log.info("latency: %s", json.dumps(latency))
        if args.phase in ("all", "throughput"):
            throughput = run_throughput(sink, pg, run_id, args.events, args.timeout_minutes * 60)
            log.info("throughput: %s", json.dumps(throughput))
    finally:
        sink.close()
        if not args.keep_alerts:
            log.info("removed %s probe alerts from Postgres", pg.delete_probe_alerts())
        pg.close()

    print("\n" + render_markdown(latency, throughput) + "\n")
    ok = (latency is None or latency["detected"] == latency["probes"]) and (
        throughput is None or throughput["complete"]
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
