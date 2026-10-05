"""Liveness tracking for the streaming job.

A streaming query can hang without failing: on Monday the job sat for 15 minutes at a
time waiting on a corrupt checkpoint file, logged nothing, then exited cleanly and
started over, for hours. This module turns "stuck" into a visible, self-correcting event.

Every micro-batch (or idle trigger, which means "running, nothing new to read") is a
heartbeat. Three things consume them:

1. A watchdog thread inside the driver. If any stream has had no heartbeat for
   ``stall_seconds`` it logs every stream's state and exits non-zero, so Docker's restart
   policy restarts the job instead of letting it hang.
2. A JSON heartbeat file read by ``scripts/healthcheck.py``, which backs the container's
   Docker healthcheck (``docker compose ps`` shows ``healthy`` / ``unhealthy``).
3. The ``pipeline_heartbeat`` table in Postgres, which the Grafana dashboard reads to show
   each stream's last activity.

Deliberately free of pyspark imports so the healthcheck can use it without a JVM.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("sentinel.streaming.health")

DEFAULT_HEALTH_FILE = "/tmp/sentinel-health.json"

HEARTBEAT_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_heartbeat (
    query_name   text PRIMARY KEY,
    last_seen_at timestamptz NOT NULL,
    batch_id     bigint,
    input_rows   bigint,
    status       text NOT NULL,
    updated_at   timestamptz NOT NULL DEFAULT now()
)
"""

HEARTBEAT_UPSERT = """
INSERT INTO pipeline_heartbeat (query_name, last_seen_at, batch_id, input_rows, status)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (query_name) DO UPDATE SET
    last_seen_at = EXCLUDED.last_seen_at,
    batch_id     = COALESCE(EXCLUDED.batch_id, pipeline_heartbeat.batch_id),
    input_rows   = COALESCE(EXCLUDED.input_rows, pipeline_heartbeat.input_rows),
    status       = EXCLUDED.status,
    updated_at   = now()
"""


@dataclass
class StreamState:
    name: str
    last_seen: float  # unix seconds
    batch_id: int | None = None
    input_rows: int | None = None
    status: str = "starting"


def stale_streams(
    states: dict[str, StreamState], now: float, stall_seconds: float
) -> list[StreamState]:
    """Streams with no heartbeat for longer than ``stall_seconds``."""
    return [s for s in states.values() if now - s.last_seen > stall_seconds]


def read_health_file(path: str | os.PathLike) -> dict[str, StreamState]:
    raw = json.loads(Path(path).read_text())
    return {name: StreamState(**fields) for name, fields in raw["streams"].items()}


def evaluate_health_file(path: str | os.PathLike, now: float | None = None) -> tuple[bool, str]:
    """Verdict for the Docker healthcheck: (healthy, one-line reason)."""
    now = time.time() if now is None else now
    try:
        raw = json.loads(Path(path).read_text())
        states = {n: StreamState(**f) for n, f in raw["streams"].items()}
        stall_seconds = float(raw["stall_seconds"])
    except FileNotFoundError:
        return False, f"no health file at {path}: streaming queries have not started"
    except (OSError, ValueError, KeyError, TypeError) as exc:
        return False, f"unreadable health file {path}: {exc}"
    if not states:
        return False, "no streaming queries registered"
    problems = [
        f"{s.name}={s.status}" for s in states.values() if s.status in ("terminated", "stalled")
    ]
    problems += [
        f"{s.name} silent {now - s.last_seen:.0f}s"
        for s in stale_streams(states, now, stall_seconds)
        if s.status not in ("terminated", "stalled")
    ]
    if problems:
        return False, "unhealthy: " + ", ".join(problems)
    return True, f"healthy: {len(states)} streams heartbeating"


class HealthMonitor:
    """Thread-safe heartbeat registry with a stall watchdog.

    ``start_grace_seconds`` gives every stream extra time before its first heartbeat,
    because the first micro-batch after a restart can legitimately be slow (for example
    replaying a Kafka backlog).
    """

    def __init__(
        self,
        stall_seconds: float = 300,
        start_grace_seconds: float = 600,
        health_file: str | os.PathLike | None = DEFAULT_HEALTH_FILE,
        pg_dsn: str | None = None,
        on_stall: Callable[[list[StreamState]], None] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.stall_seconds = stall_seconds
        self.start_grace_seconds = start_grace_seconds
        self.health_file = Path(health_file) if health_file else None
        self.pg_dsn = pg_dsn
        self.on_stall = on_stall or _exit_for_restart
        self.clock = clock
        self._states: dict[str, StreamState] = {}
        self._ids: dict[str, str] = {}  # query id -> name (idle events carry only the id)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._pg = None
        self._pg_failed_at = 0.0

    def clear_file(self) -> None:
        """Remove a health file left by a previous run, so its verdict is not reused."""
        if self.health_file:
            self.health_file.unlink(missing_ok=True)

    # ------------------------------------------------------------------ registration
    def register(self, name: str, query_id: str | None = None) -> None:
        """Start tracking a stream. Its first deadline includes the start-up grace.

        Idempotent: registering a stream that is already tracked only records its id, so
        a late onQueryStarted event cannot push back a deadline.
        """
        with self._lock:
            if query_id:
                self._ids[str(query_id)] = name
            if name in self._states:
                return
            self._states[name] = StreamState(
                name=name, last_seen=self.clock() + self.start_grace_seconds
            )
        self._publish(name)

    def beat(
        self,
        name: str | None = None,
        *,
        query_id: str | None = None,
        batch_id: int | None = None,
        input_rows: int | None = None,
        status: str = "running",
    ) -> None:
        with self._lock:
            name = name or self._ids.get(str(query_id))
            if name is None:
                return
            state = self._states.setdefault(name, StreamState(name=name, last_seen=0))
            state.last_seen = self.clock()
            state.status = status
            if batch_id is not None:
                state.batch_id = batch_id
            if input_rows is not None:
                state.input_rows = input_rows
        self._publish(name)

    def mark(self, name: str | None = None, *, query_id: str | None = None, status: str) -> None:
        """Record a status change (e.g. 'terminated') without counting it as a heartbeat."""
        with self._lock:
            name = name or self._ids.get(str(query_id))
            if name is None or name not in self._states:
                return
            self._states[name].status = status
        self._publish(name)

    def snapshot(self) -> dict[str, StreamState]:
        with self._lock:
            return {k: StreamState(**asdict(v)) for k, v in self._states.items()}

    # ------------------------------------------------------------------ watchdog
    def check(self) -> list[StreamState]:
        """Run one watchdog pass; call ``on_stall`` if any stream is stale."""
        stale = stale_streams(self.snapshot(), self.clock(), self.stall_seconds)
        if stale:
            now = self.clock()
            for s in self.snapshot().values():
                age = now - s.last_seen
                log.error(
                    "health %-24s status=%-10s last_heartbeat=%5.0fs ago batch=%s %s",
                    s.name,
                    s.status,
                    max(age, 0),
                    s.batch_id,
                    "STALLED" if s in stale else "",
                )
            for s in stale:
                self.mark(s.name, status="stalled")
            self.on_stall(stale)
        return stale

    def start(self, interval: float = 15.0) -> None:
        def loop() -> None:
            while not self._stop.wait(interval):
                try:
                    self.check()
                except Exception:  # the watchdog itself must never die quietly
                    log.exception("health watchdog pass failed")

        self._thread = threading.Thread(target=loop, name="sentinel-health", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    # ------------------------------------------------------------------ outputs
    def _publish(self, name: str) -> None:
        self._write_file()
        self._write_postgres(name)

    def _write_file(self) -> None:
        if not self.health_file:
            return
        payload = {
            "written_at": self.clock(),
            "stall_seconds": self.stall_seconds,
            "streams": {k: asdict(v) for k, v in self.snapshot().items()},
        }
        try:
            self.health_file.parent.mkdir(parents=True, exist_ok=True)
            # Atomic replace, so the healthcheck never reads a half-written file.
            fd, tmp = tempfile.mkstemp(dir=self.health_file.parent, prefix=".health-")
            with os.fdopen(fd, "w") as fh:
                json.dump(payload, fh)
            os.replace(tmp, self.health_file)
        except OSError:
            log.warning("could not write health file %s", self.health_file, exc_info=True)

    def _write_postgres(self, name: str) -> None:
        if not self.pg_dsn:
            return
        # After a failure, back off for a minute rather than slowing every heartbeat.
        if self._pg is None and self.clock() - self._pg_failed_at < 60:
            return
        state = self.snapshot().get(name)
        if state is None:
            return
        try:
            import psycopg2

            if self._pg is None:
                self._pg = psycopg2.connect(self.pg_dsn, connect_timeout=5)
                self._pg.autocommit = True
                with self._pg.cursor() as cur:
                    cur.execute(HEARTBEAT_DDL)
            with self._pg.cursor() as cur:
                cur.execute(
                    HEARTBEAT_UPSERT,
                    (
                        state.name,
                        datetime.fromtimestamp(min(state.last_seen, self.clock()), timezone.utc),
                        state.batch_id,
                        state.input_rows,
                        state.status,
                    ),
                )
        except Exception:
            log.warning("could not write heartbeat to Postgres", exc_info=True)
            self._pg_failed_at = self.clock()
            try:
                if self._pg is not None:
                    self._pg.close()
            except Exception:
                pass
            self._pg = None


def _exit_for_restart(stale: list[StreamState]) -> None:
    """Default stall action: exit hard so Docker's restart policy brings the job back.

    os._exit, not sys.exit: this runs on the watchdog thread, and a hung Spark query can
    block a graceful shutdown indefinitely, which is the very situation being escaped.
    """
    names = ", ".join(s.name for s in stale)
    log.error("streams stalled (%s): exiting with code 3 so the container restarts", names)
    logging.shutdown()
    os._exit(3)
