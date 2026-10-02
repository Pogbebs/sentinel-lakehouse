"""Output sinks for the simulator: Kafka (live mode), JSON lines (offline mode), breach dumps."""

from __future__ import annotations

import csv
import io
import json
import logging
import os
from datetime import date
from pathlib import Path
from typing import Protocol

log = logging.getLogger(__name__)


class EventSink(Protocol):
    def send(self, event: dict) -> None: ...
    def close(self) -> None: ...


class JsonlSink:
    """Writes events as JSON lines, rolling a new file every `rows_per_file` events."""

    def __init__(self, directory: str, rows_per_file: int = 100_000) -> None:
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.rows_per_file = rows_per_file
        self._n = 0
        self._part = 0
        self._fh = None

    def send(self, event: dict) -> None:
        if self._fh is None or self._n % self.rows_per_file == 0:
            if self._fh:
                self._fh.close()
            self._fh = open(self.dir / f"events-{self._part:05d}.jsonl", "w")  # noqa: SIM115
            self._part += 1
        self._fh.write(json.dumps(event, separators=(",", ":")) + "\n")
        self._n += 1

    def close(self) -> None:
        if self._fh:
            self._fh.close()


class KafkaSink:
    """Idempotent, compressed producer keyed by username so each account's events stay ordered."""

    def __init__(self, bootstrap: str, topic: str) -> None:
        from confluent_kafka import Producer

        self.topic = topic
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "enable.idempotence": True,
                "acks": "all",
                "compression.type": "lz4",
                "linger.ms": 50,
                "batch.size": 131072,
            }
        )
        self.delivered = 0
        self.failed = 0

    def _on_delivery(self, err, _msg) -> None:
        if err is not None:
            self.failed += 1
            log.warning("delivery failed: %s", err)
        else:
            self.delivered += 1

    def send(self, event: dict) -> None:
        key = (event.get("username") or "").encode()
        payload = json.dumps(event, separators=(",", ":")).encode()
        while True:
            try:
                self.producer.produce(
                    self.topic, key=key, value=payload, on_delivery=self._on_delivery
                )
                break
            except BufferError:  # local queue full: let the producer drain, then retry
                self.producer.poll(0.5)
        self.producer.poll(0)

    def close(self) -> None:
        self.producer.flush(30)
        log.info("kafka sink closed: delivered=%s failed=%s", self.delivered, self.failed)


def breach_dump_csv(records: list[dict]) -> str:
    """Serialise a breach dump. Only hashes leave the simulator, never plaintext emails."""
    buf = io.StringIO()
    writer = csv.DictWriter(
        buf, fieldnames=["email_sha256", "password_sha1", "source", "breach_date"]
    )
    writer.writeheader()
    for rec in records:
        writer.writerow({k: rec[k] for k in writer.fieldnames})
    return buf.getvalue()


def write_breach_dump(records: list[dict], lake_root: str, as_of: date) -> str:
    """Drop the dump into the landing zone, partitioned by the date it arrived."""
    key = f"landing/breach_dumps/dt={as_of.isoformat()}/dump.csv"
    body = breach_dump_csv(records)
    if lake_root.startswith(("s3://", "s3a://")):
        import boto3

        bucket = lake_root.split("://", 1)[1].split("/", 1)[0]
        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ.get("S3_ENDPOINT", "http://localhost:9000"),
            aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "minioadmin"),
        )
        s3.put_object(Bucket=bucket, Key=key, Body=body.encode())
        return f"s3://{bucket}/{key}"
    path = Path(lake_root) / key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    return str(path)
