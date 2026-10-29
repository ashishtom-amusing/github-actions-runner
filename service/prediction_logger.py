"""
Asynchronous, buffered prediction logger.

Design
------
The request path must never wait on S3. So `log()` only drops records onto a
bounded in-memory queue and returns immediately. A single background thread
drains the queue and writes batches to object storage when either:

  * the batch reaches `batch_size` records, or
  * `flush_seconds` have passed since the last write.

Each batch becomes one gzip-compressed JSON Lines object, partitioned by the
hour the predictions were made:

    s3://<bucket>/<model>/dt=YYYY-MM-DD/hour=HH/<role>-<pod>-<epoch_ms>-<id>.jsonl.gz

Hour partitions let the drift job read only the window it needs instead of
listing the whole bucket. The role prefix lets it pick champion or challenger
traffic without opening every file.

Failure behaviour
-----------------
* Queue full  -> the record is dropped and counted. Serving latency always wins
  over logging completeness. The drop counter is exported to Prometheus so this
  is visible, never silent.
* S3 failure  -> retried with exponential backoff, then the batch is counted as
  failed and discarded. A stuck object store must not grow memory without bound.
* Shutdown    -> `close()` flushes whatever is buffered. Called from the app's
  lifespan hook, so a normal pod termination loses nothing.

Things deliberately left for later optimisation: Parquet instead of JSONL, a
local disk spool to survive S3 outages, and compaction of small files.
"""

from __future__ import annotations

import gzip
import io
import json
import os
import queue
import socket
import threading
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from prometheus_client import Counter, Gauge

RECORDS = Counter(
    "prediction_log_records_total", "Prediction records written to object storage", ["model", "role"]
)
DROPPED = Counter(
    "prediction_log_dropped_total", "Records dropped because the buffer was full", ["model", "role"]
)
FLUSH_ERRORS = Counter(
    "prediction_log_flush_errors_total", "Batches that failed to upload after retries", ["model", "role"]
)
BATCHES = Counter(
    "prediction_log_batches_total", "Batches uploaded successfully", ["model", "role"]
)
QUEUE_DEPTH = Gauge(
    "prediction_log_queue_depth", "Records waiting to be written", ["model", "role"]
)


class PredictionLogger:
    def __init__(
        self,
        model: str,
        role: str,
        bucket: str | None,
        endpoint_url: str | None = None,
        batch_size: int = 500,
        flush_seconds: float = 30.0,
        max_queue: int = 50_000,
        max_retries: int = 3,
    ) -> None:
        self.model = model
        self.role = role
        self.bucket = bucket
        self.batch_size = batch_size
        self.flush_seconds = flush_seconds
        self.max_retries = max_retries
        self.enabled = bool(bucket)

        self._queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=max_queue)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._pod = os.getenv("POD_NAME") or socket.gethostname()
        self._labels = {"model": model, "role": role}

        self._client = None
        if self.enabled:
            import boto3
            from botocore.config import Config

            self._client = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
                config=Config(
                    s3={"addressing_style": "path"},
                    retries={"max_attempts": 2, "mode": "standard"},
                    connect_timeout=5,
                    read_timeout=15,
                ),
            )

    # ------------------------------------------------------------------ public
    def start(self) -> None:
        if not self.enabled:
            print("Prediction logging disabled (no bucket configured)")
            return
        self._thread = threading.Thread(target=self._run, name="prediction-logger", daemon=True)
        self._thread.start()
        print(f"Prediction logging to s3://{self.bucket}/{self.model}/ "
              f"(batch {self.batch_size}, every {self.flush_seconds:.0f}s)")

    def log(self, records: list[dict[str, Any]]) -> None:
        """Non-blocking. Safe to call from the request path."""
        if not self.enabled:
            return
        for record in records:
            try:
                self._queue.put_nowait(record)
            except queue.Full:
                DROPPED.labels(**self._labels).inc()
        QUEUE_DEPTH.labels(**self._labels).set(self._queue.qsize())

    def close(self, timeout: float = 20.0) -> None:
        """Flush what is buffered and stop the worker."""
        if not self.enabled or self._thread is None:
            return
        self._stop.set()
        self._thread.join(timeout=timeout)

    def stats(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "bucket": self.bucket,
            "queued": self._queue.qsize() if self.enabled else 0,
            "batch_size": self.batch_size,
            "flush_seconds": self.flush_seconds,
        }

    # ---------------------------------------------------------------- internal
    def _run(self) -> None:
        batch: list[dict[str, Any]] = []
        last_flush = time.monotonic()

        while not self._stop.is_set():
            timeout = max(0.1, self.flush_seconds - (time.monotonic() - last_flush))
            try:
                batch.append(self._queue.get(timeout=min(timeout, 1.0)))
            except queue.Empty:
                pass

            due = (time.monotonic() - last_flush) >= self.flush_seconds
            if len(batch) >= self.batch_size or (batch and due):
                self._write(batch)
                batch = []
                last_flush = time.monotonic()
            elif due:
                last_flush = time.monotonic()
            QUEUE_DEPTH.labels(**self._labels).set(self._queue.qsize())

        # Shutdown: drain everything still queued.
        while True:
            try:
                batch.append(self._queue.get_nowait())
            except queue.Empty:
                break
        if batch:
            self._write(batch)
        QUEUE_DEPTH.labels(**self._labels).set(0)

    def _write(self, batch: list[dict[str, Any]]) -> None:
        # A batch can straddle an hour boundary; write one object per hour so
        # partitions stay correct.
        by_hour: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for record in batch:
            ts = datetime.fromisoformat(record["ts"])
            by_hour[(ts.strftime("%Y-%m-%d"), ts.strftime("%H"))].append(record)

        for (day, hour), records in by_hour.items():
            key = (
                f"{self.model}/dt={day}/hour={hour}/"
                f"{self.role}-{self._pod}-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}.jsonl.gz"
            )
            body = io.BytesIO()
            with gzip.GzipFile(fileobj=body, mode="wb") as gz:
                for record in records:
                    gz.write((json.dumps(record, default=str) + "\n").encode("utf-8"))
            payload = body.getvalue()

            for attempt in range(1, self.max_retries + 1):
                try:
                    self._client.put_object(
                        Bucket=self.bucket,
                        Key=key,
                        Body=payload,
                        ContentType="application/x-ndjson",
                        ContentEncoding="gzip",
                    )
                    RECORDS.labels(**self._labels).inc(len(records))
                    BATCHES.labels(**self._labels).inc()
                    break
                except Exception as exc:  # noqa: BLE001 - must never kill the thread
                    if attempt == self.max_retries:
                        FLUSH_ERRORS.labels(**self._labels).inc()
                        print(f"Prediction log upload failed after {attempt} attempts, "
                              f"{len(records)} records lost: {exc}")
                    else:
                        time.sleep(2 ** attempt)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
