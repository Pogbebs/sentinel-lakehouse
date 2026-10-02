"""Command line entry point: `sentinel-generate kafka|file ...`."""

from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime, timedelta, timezone

from sentinel.common.config import get_settings
from sentinel.generator.simulator import SimConfig, Simulator
from sentinel.generator.sinks import JsonlSink, KafkaSink, write_breach_dump

log = logging.getLogger("sentinel.generator")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Synthetic auth traffic with labelled attacks.")
    p.add_argument("sink", choices=["kafka", "file"])
    p.add_argument("--minutes", type=int, default=60, help="simulated minutes to generate")
    p.add_argument("--out", default="./data/raw/auth_events", help="directory for file sink")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--users", type=int, default=5000)
    p.add_argument("--events-per-minute", type=int, default=300)
    p.add_argument("--attacks-per-hour", type=float, default=4.0)
    p.add_argument(
        "--speed",
        type=float,
        default=1.0,
        help="kafka mode: simulated seconds per wall second (0 = as fast as possible)",
    )
    p.add_argument("--start", help="ISO start time (default: now for kafka, now-minutes for file)")
    p.add_argument("--no-breach-dump", action="store_true")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _parse_args(argv)
    settings = get_settings()

    now = datetime.now(timezone.utc).replace(microsecond=0)
    if args.start:
        start = datetime.fromisoformat(args.start.replace("Z", "+00:00"))
    elif args.sink == "kafka":
        start = now
    else:
        start = now - timedelta(minutes=args.minutes)

    cfg = SimConfig(
        seed=args.seed,
        users=args.users,
        events_per_minute=args.events_per_minute,
        attacks_per_hour=args.attacks_per_hour,
    )
    sim = Simulator(cfg, start=start)

    if not args.no_breach_dump:
        where = write_breach_dump(sim.breach_records, settings.lake_root, start.date())
        log.info("breach dump with %s records written to %s", len(sim.breach_records), where)

    sink = (
        KafkaSink(settings.kafka_bootstrap, settings.auth_topic)
        if args.sink == "kafka"
        else JsonlSink(args.out)
    )

    wall_start = time.monotonic()
    n = 0
    try:
        for event in sim.run(args.minutes):
            if args.sink == "kafka" and args.speed > 0 and event.get("event_ts"):
                ts = datetime.fromisoformat(event["event_ts"].replace("Z", "+00:00"))
                due = (ts - start).total_seconds() / args.speed
                lag = due - (time.monotonic() - wall_start)
                if lag > 0:
                    time.sleep(lag)
            sink.send(event)
            n += 1
            if n % 50_000 == 0:
                log.info("sent %s events", n)
    finally:
        sink.close()
    log.info("done: %s events over %s simulated minutes", n, args.minutes)


if __name__ == "__main__":
    main()
