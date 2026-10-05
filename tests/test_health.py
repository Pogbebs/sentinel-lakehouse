"""Streaming liveness: stall detection, the heartbeat file and the Docker healthcheck verdict.

Pure Python with a fake clock, so these run in milliseconds and need no Spark.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from sentinel.streaming.health import HealthMonitor, evaluate_health_file, read_health_file

ROOT = Path(__file__).resolve().parents[1]


class FakeClock:
    def __init__(self, t: float = 1_000_000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def make_monitor(tmp_path, clock, stalls=None, **kw) -> HealthMonitor:
    kw.setdefault("stall_seconds", 300)
    kw.setdefault("start_grace_seconds", 600)
    return HealthMonitor(
        health_file=tmp_path / "health.json",
        on_stall=(stalls.append if stalls is not None else lambda stale: None),
        clock=clock,
        **kw,
    )


def test_new_stream_gets_start_grace_before_it_can_stall(tmp_path):
    clock, stalls = FakeClock(), []
    m = make_monitor(tmp_path, clock, stalls)
    m.register("bronze", query_id="q1")
    clock.advance(800)  # past stall_seconds, within stall + grace
    assert m.check() == [] and stalls == []
    clock.advance(200)
    assert [s.name for s in m.check()] == ["bronze"]
    assert len(stalls) == 1


def test_heartbeats_keep_a_stream_alive_and_idle_counts(tmp_path):
    clock, stalls = FakeClock(), []
    m = make_monitor(tmp_path, clock, stalls, start_grace_seconds=0)
    m.register("silver", query_id="q2")
    for _ in range(10):
        clock.advance(200)
        m.beat(query_id="q2", status="idle")  # idle events carry only the query id
        assert m.check() == []
    assert stalls == []
    assert m.snapshot()["silver"].status == "idle"


def test_only_the_silent_stream_is_reported_and_marked_stalled(tmp_path):
    clock, stalls = FakeClock(), []
    m = make_monitor(tmp_path, clock, stalls, start_grace_seconds=0)
    m.register("bronze")
    m.register("silver")
    clock.advance(250)
    m.beat("bronze", batch_id=7, input_rows=900)
    clock.advance(100)  # silver now 350 s silent, bronze 100 s
    stale = m.check()
    assert [s.name for s in stale] == ["silver"]
    assert [s.name for s in stalls[0]] == ["silver"]
    snap = m.snapshot()
    assert snap["silver"].status == "stalled"
    assert snap["bronze"].batch_id == 7 and snap["bronze"].input_rows == 900


def test_late_register_does_not_push_back_a_deadline(tmp_path):
    clock = FakeClock()
    m = make_monitor(tmp_path, clock, start_grace_seconds=0)
    m.register("bronze", query_id="q1")
    clock.advance(200)
    m.register("bronze", query_id="q1")  # e.g. a late onQueryStarted event
    clock.advance(150)
    assert [s.name for s in m.check()] == ["bronze"]


def test_unknown_query_ids_are_ignored(tmp_path):
    m = make_monitor(tmp_path, FakeClock())
    m.beat(query_id="never-registered")
    m.mark(query_id="never-registered", status="terminated")
    assert m.snapshot() == {}


def test_health_file_round_trips_and_is_cleared_on_restart(tmp_path):
    clock = FakeClock()
    m = make_monitor(tmp_path, clock)
    m.register("bronze")
    m.beat("bronze", batch_id=3, input_rows=10)
    states = read_health_file(tmp_path / "health.json")
    assert states["bronze"].batch_id == 3 and states["bronze"].status == "running"
    m.clear_file()
    assert not (tmp_path / "health.json").exists()


def test_healthcheck_verdicts(tmp_path):
    path = tmp_path / "health.json"
    assert evaluate_health_file(path)[0] is False  # not started yet

    clock = FakeClock()
    m = make_monitor(tmp_path, clock, start_grace_seconds=0)
    m.register("bronze")
    m.register("silver")
    m.beat("bronze")
    m.beat("silver")
    ok, reason = evaluate_health_file(path, now=clock() + 60)
    assert ok, reason

    m.beat("bronze")
    ok, reason = evaluate_health_file(path, now=clock() + 400)
    assert not ok and "silent" in reason

    m.mark("silver", status="terminated")
    ok, reason = evaluate_health_file(path, now=clock() + 1)
    assert not ok and "silver=terminated" in reason

    path.write_text("{not json")
    assert evaluate_health_file(path)[0] is False


def test_healthcheck_script_exit_codes(tmp_path):
    """The script Docker runs: exit 0 when healthy, 1 otherwise."""
    path = tmp_path / "health.json"
    env = {"HEALTH_FILE": str(path), "PYTHONPATH": str(ROOT / "src"), "PATH": "/usr/bin:/bin"}
    script = [sys.executable, str(ROOT / "scripts" / "healthcheck.py")]

    missing = subprocess.run(script, env=env, capture_output=True, text=True)
    assert missing.returncode == 1

    import time

    now = time.time()
    path.write_text(
        json.dumps(
            {
                "written_at": now,
                "stall_seconds": 300,
                "streams": {"bronze": {"name": "bronze", "last_seen": now, "status": "idle"}},
            }
        )
    )
    healthy = subprocess.run(script, env=env, capture_output=True, text=True)
    assert healthy.returncode == 0, healthy.stdout + healthy.stderr
    assert "healthy" in healthy.stdout


def test_postgres_heartbeat_upserts_and_backs_off_after_failure(tmp_path, monkeypatch):
    import types

    executed, connects = [], []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, sql, params=None):
            executed.append((sql.split()[0], params))

    class Conn:
        autocommit = False

        def cursor(self):
            return Cursor()

        def close(self):
            pass

    def connect(dsn, connect_timeout):
        connects.append(dsn)
        if len(connects) == 1:
            raise OSError("postgres is down")
        return Conn()

    monkeypatch.setitem(sys.modules, "psycopg2", types.SimpleNamespace(connect=connect))
    clock = FakeClock()
    m = make_monitor(tmp_path, clock, pg_dsn="postgresql://x")
    m.register("bronze")  # first connect fails
    m.beat("bronze")  # inside the 60 s back-off: no reconnect attempt
    assert len(connects) == 1 and executed == []

    clock.advance(61)
    m.beat("bronze", batch_id=5, input_rows=42)
    assert len(connects) == 2
    assert executed[0][0] == "CREATE"  # table created on first connection
    name, _last_seen, batch_id, rows, status = executed[-1][1]
    assert (name, batch_id, rows, status) == ("bronze", 5, 42, "running")
