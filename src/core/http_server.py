"""Shared embedded HTTP server for engine microservices.

All standalone engine processes (backtest, archiver, outcome-tracker, etc.)
need /health, /ready, and /metrics endpoints served from a daemon thread.
This module provides a single implementation to avoid 8 near-identical copies.

Endpoint semantics:
  /health  — 200 while process is alive (liveness probe — always responds)
  /ready   — 200 after set_ready() is called; 503 until then or after set_not_ready()
  /metrics — Prometheus scrape target

Thread-safety: GIL guarantees atomicity for simple dict reads/writes in CPython.
Each process has its own copy of this module's state (no cross-process sharing).
"""
from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

_state: dict[str, object] = {"ready": False, "service": "unknown"}
_server_started = False


def set_ready(service_name: str) -> None:
    """Call after DB/Redis are connected and the service loop is running."""
    _state["ready"] = True
    _state["service"] = service_name


def set_not_ready() -> None:
    """Call on shutdown or sustained dependency failure."""
    _state["ready"] = False


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *args) -> None:
        pass  # silence access log — structlog handles observability

    def do_GET(self) -> None:
        if self.path == "/health":
            body = json.dumps({"status": "ok", "service": _state["service"]}).encode()
            self._send(200, "application/json", body)
        elif self.path == "/ready":
            ready = bool(_state["ready"])
            payload = {
                "status": "ready" if ready else "not_ready",
                "service": _state["service"],
            }
            self._send(200 if ready else 503, "application/json", json.dumps(payload).encode())
        elif self.path == "/metrics":
            self._send(200, CONTENT_TYPE_LATEST, generate_latest())
        else:
            self._send(404, "text/plain", b"not found")

    def _send(self, code: int, content_type: str, body: bytes) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def start(port: int, service_name: str) -> None:
    """Start the embedded HTTP server once. Subsequent calls are no-ops."""
    global _server_started
    if _server_started:
        return
    _server_started = True
    _state["service"] = service_name
    server = HTTPServer(("0.0.0.0", port), _Handler)
    Thread(target=server.serve_forever, daemon=True).start()
