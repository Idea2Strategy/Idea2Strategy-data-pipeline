"""Readiness state and probe endpoint for `pipeline-worker`.

Lifecycle: STARTING -> READY -> DRAINING -> STOPPED, with FAILED reachable from
any state.  Only READY reports ready, so a load balancer or container probe
stops routing work the moment a drain begins or a fatal error is recorded.

Two ways to observe it, because deployments differ:

* :meth:`HealthState.write_to` publishes a JSON file, for an `exec` probe;
* :class:`HealthEndpoint` serves ``GET /health`` (liveness: 200 while the
  process is answering at all) and ``GET /ready`` (readiness: 200 only in READY,
  503 otherwise), for an HTTP probe.

Anything else is a 404 with a JSON body.  A probe endpoint that answered 200 to
every path would report a healthy worker for as long as the socket was open.
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import UTC, datetime
from enum import StrEnum
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger("apps.pipeline_worker.health")


class ReadinessStatus(StrEnum):
    STARTING = "STARTING"
    READY = "READY"
    DRAINING = "DRAINING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"


class HealthState:
    """Thread-safe readiness signal and processing counters."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._status = ReadinessStatus.STARTING
        self._stop_reason: str | None = None
        self._started_at: datetime | None = None
        self._last_activity_at: datetime | None = None
        self._processed = 0
        self._succeeded = 0
        self._rejected = 0
        self._duplicated = 0
        self._failed = 0
        self._dead_lettered = 0

    # -- state -----------------------------------------------------------
    @property
    def status(self) -> ReadinessStatus:
        with self._lock:
            return self._status

    @property
    def is_ready(self) -> bool:
        with self._lock:
            return self._status is ReadinessStatus.READY

    @property
    def processed(self) -> int:
        with self._lock:
            return self._processed

    @property
    def succeeded(self) -> int:
        with self._lock:
            return self._succeeded

    @property
    def rejected(self) -> int:
        with self._lock:
            return self._rejected

    @property
    def duplicated(self) -> int:
        with self._lock:
            return self._duplicated

    @property
    def failed(self) -> int:
        with self._lock:
            return self._failed

    @property
    def dead_lettered(self) -> int:
        with self._lock:
            return self._dead_lettered

    # -- transitions -----------------------------------------------------
    def mark_ready(self) -> None:
        with self._lock:
            self._status = ReadinessStatus.READY
            self._started_at = self._started_at or datetime.now(UTC)

    def mark_draining(self, reason: str) -> None:
        with self._lock:
            if self._status is not ReadinessStatus.FAILED:
                self._status = ReadinessStatus.DRAINING
            self._stop_reason = reason

    def mark_stopped(self) -> None:
        with self._lock:
            if self._status is not ReadinessStatus.FAILED:
                self._status = ReadinessStatus.STOPPED

    def mark_failed(self, reason: str) -> None:
        with self._lock:
            self._status = ReadinessStatus.FAILED
            self._stop_reason = reason

    # -- counters --------------------------------------------------------
    def record(self, outcome: str) -> None:
        with self._lock:
            self._processed += 1
            self._last_activity_at = datetime.now(UTC)
            if outcome == "SUCCEEDED":
                self._succeeded += 1
            elif outcome == "REJECTED":
                self._rejected += 1
            elif outcome == "DUPLICATE":
                self._duplicated += 1
            elif outcome == "DEAD_LETTERED":
                self._dead_lettered += 1
            else:
                self._failed += 1

    # -- reporting -------------------------------------------------------
    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "status": self._status.value,
                "ready": self._status is ReadinessStatus.READY,
                "stop_reason": self._stop_reason,
                "started_at": self._started_at.isoformat() if self._started_at else None,
                "last_activity_at": (
                    self._last_activity_at.isoformat() if self._last_activity_at else None
                ),
                "processed": self._processed,
                "succeeded": self._succeeded,
                "rejected": self._rejected,
                "duplicated": self._duplicated,
                "failed": self._failed,
                "dead_lettered": self._dead_lettered,
            }

    def write_to(self, path: Path) -> None:
        """Atomically publish the snapshot for an external probe."""

        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.tmp")
        temporary.write_text(
            json.dumps(self.snapshot(), separators=(",", ":")) + "\n", encoding="utf-8"
        )
        temporary.replace(path)

    @staticmethod
    def remove(path: Path) -> None:
        path.unlink(missing_ok=True)


class _HealthRequestHandler(BaseHTTPRequestHandler):
    """Serves the two probe routes and nothing else."""

    server_version = "pipeline-worker-health/1"
    #: Injected by :class:`HealthEndpoint` onto the server instance.
    state: HealthState

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler's contract
        state: HealthState = self.server.state  # type: ignore[attr-defined]
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        if path in {"/health", "/healthz", "/"}:
            self._respond(200, state.snapshot())
        elif path in {"/ready", "/readyz"}:
            snapshot = state.snapshot()
            self._respond(200 if snapshot["ready"] else 503, snapshot)
        else:
            self._respond(404, {"error": "NOT_FOUND", "path": path})

    def _respond(self, status: int, payload: dict[str, Any]) -> None:
        body = (json.dumps(payload, separators=(",", ":"), sort_keys=True) + "\n").encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 - stdlib signature
        # Probe traffic is high-frequency and uninteresting; route it to DEBUG
        # instead of stderr so it cannot drown the worker's own structured logs.
        LOGGER.debug("health.request", extra={"detail": format % args})


class HealthEndpoint:
    """A small HTTP server publishing one :class:`HealthState`.

    `port=0` binds an ephemeral port and reports it on :attr:`port`, so a test --
    or several workers on one host -- never has to guess a free one.
    """

    def __init__(self, state: HealthState, *, host: str = "127.0.0.1", port: int = 0) -> None:
        self._state = state
        self._host = host
        self._requested_port = port
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def port(self) -> int | None:
        return None if self._server is None else int(self._server.server_address[1])

    @property
    def is_running(self) -> bool:
        return self._server is not None

    def start(self) -> int:
        if self._server is not None:
            raise RuntimeError("health endpoint is already running")
        server = ThreadingHTTPServer((self._host, self._requested_port), _HealthRequestHandler)
        server.daemon_threads = True
        server.state = self._state  # type: ignore[attr-defined]
        self._server = server
        self._thread = threading.Thread(
            target=server.serve_forever, name="pipeline-worker-health", daemon=True
        )
        self._thread.start()
        bound = int(server.server_address[1])
        LOGGER.info("health.endpoint.started", extra={"host": self._host, "port": bound})
        return bound

    def stop(self) -> None:
        server, thread = self._server, self._thread
        self._server, self._thread = None, None
        if server is None:
            return
        server.shutdown()
        server.server_close()
        if thread is not None:
            thread.join(timeout=5.0)
        LOGGER.info("health.endpoint.stopped")
