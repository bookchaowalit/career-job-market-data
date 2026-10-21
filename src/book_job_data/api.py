"""Read-only job.v1 API backed by Bronze Parquet/DuckDB."""

from __future__ import annotations

import json
import hmac
import ipaddress
import re
import sys
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from . import _sentry, config
from .lake import load_lineage
from .privacy import sanitize_record
from .store import envelope, load_dataset, paginate

_LOCAL_ORIGIN_RE = re.compile(r"^https?://(127\.0\.0\.1|localhost|\[::1\])(:\d+)?$", re.I)


class _DatasetCache:
    """Reuse loaded Bronze datasets for a short TTL instead of scanning per request."""

    def __init__(self, ttl_seconds: float):
        self.ttl = ttl_seconds
        self._lock = threading.Lock()
        self._entries: dict[tuple[str, bool], tuple[float, dict[str, Any]]] = {}

    def get(self, dataset: str, *, latest_only: bool = True) -> dict[str, Any]:
        key = (dataset, latest_only)
        now = time.monotonic()
        with self._lock:
            hit = self._entries.get(key)
            if hit and now - hit[0] < self.ttl:
                return hit[1]
        payload = load_dataset(dataset, latest_only=latest_only)
        payload = {**payload, "items": [sanitize_record(item) for item in payload.get("items", [])]}
        if self.ttl > 0:
            with self._lock:
                self._entries[key] = (now, payload)
        return payload

    def peek_status(self, dataset: str) -> str | None:
        with self._lock:
            hit = self._entries.get((dataset, True))
        return hit[1].get("data_status") if hit else None


class _RateLimiter:
    """Sliding one-minute window per client address."""

    def __init__(self, per_minute: int):
        self.per_minute = per_minute
        self._lock = threading.Lock()
        self._hits: dict[str, deque[float]] = {}

    def allow(self, client: str) -> bool:
        if self.per_minute <= 0:
            return True
        now = time.monotonic()
        with self._lock:
            window = self._hits.setdefault(client, deque())
            while window and now - window[0] >= 60:
                window.popleft()
            if len(window) >= self.per_minute:
                return False
            window.append(now)
            if len(self._hits) > 10_000:  # drop idle clients so memory stays bounded
                for key in [k for k, v in self._hits.items() if not v]:
                    del self._hits[key]
            return True


CACHE = _DatasetCache(config.CACHE_TTL_SECONDS)
LIMITER = _RateLimiter(config.RATE_LIMIT_PER_MINUTE)


def _client_address(handler: BaseHTTPRequestHandler) -> str:
    # Render's proxy appends the real peer to X-Forwarded-For; the rightmost
    # entry is the one a client cannot spoof.
    forwarded = handler.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",")[-1].strip()
    return handler.client_address[0]


def _is_loopback(host: str) -> bool:
    if host == "localhost":
        return True
    try:  # "" and "0.0.0.0" mean every interface, which is not loopback
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def require_token_for_bind(host: str, token: str) -> None:
    """Refuse to serve internal job data on a non-loopback address without a token."""
    if not token and not _is_loopback(host):
        raise SystemExit(
            f"Refusing to bind book-job-data API to {host} without BOOK_JOB_DATA_API_TOKEN; "
            "records are privacy_class=internal. Set the token or bind to 127.0.0.1."
        )


def _json_response(handler: BaseHTTPRequestHandler, status: int, body: dict[str, Any]) -> None:
    payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(payload)))
    handler.send_header("Cache-Control", "no-store")
    origin = handler.headers.get("Origin", "")
    if _LOCAL_ORIGIN_RE.match(origin):
        handler.send_header("Access-Control-Allow-Origin", origin)
        handler.send_header("Vary", "Origin")
    handler.end_headers()
    handler.wfile.write(payload)


def _dataset(qs: dict[str, list[str]], default: str) -> str:
    value = qs.get("dataset", [default])[0]
    return value if value in config.DATASETS.values() else default


def _limit(qs: dict[str, list[str]]) -> int:
    try:
        return max(1, min(int(qs.get("limit", ["50"])[0]), 500))
    except ValueError:
        return 50


def _storage_model(source_kind: Any) -> str:
    kind = str(source_kind or "")
    if kind.startswith("iceberg_"):
        return "lake_first_job_iceberg_rest_duckdb"
    if kind.startswith("bronze_s3_parquet"):
        return "lake_first_job_bronze_s3_duckdb"
    return "lake_first_job_bronze_duckdb"


def _configured_storage_model() -> str:
    """Storage model from configuration alone, so /healthz never reads the lake."""
    if config.LAKE_READ_MODE == "iceberg":
        return _storage_model("iceberg_rest_duckdb")
    if (config.DATA_LAKE_URI or "").lower().startswith("s3://"):
        return _storage_model("bronze_s3_parquet")
    return _storage_model("bronze_parquet")


def _authorized(handler: BaseHTTPRequestHandler) -> bool:
    expected = config.API_READ_TOKEN
    if not expected:
        return True
    supplied = handler.headers.get("Authorization", "")
    return hmac.compare_digest(supplied, f"Bearer {expected}")


class Handler(BaseHTTPRequestHandler):
    server_version = "book-job-data-api/0.1"

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    def do_OPTIONS(self) -> None:  # noqa: N802
        origin = self.headers.get("Origin", "")
        if not _LOCAL_ORIGIN_RE.match(origin):
            self.send_response(403)
            self.end_headers()
            return
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", origin)
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Accept, Content-Type")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_POST(self) -> None:  # noqa: N802
        if self.path.rstrip("/") == "/v1/refresh":
            _json_response(self, 403, envelope(items=[], data_status="forbidden", extra={"error": "refresh disabled"}))
            return
        _json_response(self, 405, envelope(items=[], data_status="forbidden", extra={"error": "GET only"}))

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        qs = parse_qs(parsed.query)
        if path == "/healthz":
            # Liveness only: no lake read, so platform probes stay cheap.
            return _json_response(
                self,
                200,
                {
                    "status": "ok",
                    "repository": config.REPO_NAME,
                    "domain": config.DOMAIN,
                    "data_status": CACHE.peek_status(config.DEFAULT_DATASET) or "not_loaded",
                    "storage_model": _configured_storage_model(),
                    "api_bind": f"{config.API_HOST}:{config.API_PORT}",
                },
            )
        if not LIMITER.allow(_client_address(self)):
            return _json_response(
                self,
                429,
                envelope(
                    items=[], data_status="rate_limited", extra={"error": "rate limit exceeded; retry in a minute"}
                ),
            )
        if not _authorized(self):
            return _json_response(
                self,
                401,
                envelope(
                    items=[],
                    data_status="forbidden",
                    extra={"error": "read token required"},
                ),
            )
        if path == "/v1/metadata":
            datasets = {
                name: CACHE.get(
                    name,
                    latest_only=name
                    not in {config.DEFAULT_HISTORY_DATASET, "job_matches_history", "job_leads_history"},
                )
                for name in config.DATASETS.values()
            }
            record_payload = datasets[config.DEFAULT_DATASET]
            item = {
                "repository": config.REPO_NAME,
                "domain": config.DOMAIN,
                "schema_version": config.SCHEMA_VERSION,
                "port": config.API_PORT,
                "storage_model": _storage_model(record_payload.get("source_kind")),
                "records_source_kind": record_payload.get("source_kind"),
                "dataset_counts": {name: len(value.get("items", [])) for name, value in datasets.items()},
                "lake_lineage": load_lineage(),
                "scraper_boundary": "file_capture_only",
                "api_bind": f"{config.API_HOST}:{config.API_PORT}",
            }
            return _json_response(
                self,
                200,
                envelope(
                    items=[item],
                    data_status=record_payload["data_status"],
                    retrieved_at=record_payload.get("retrieved_at"),
                ),
            )
        if path == "/v1/records":
            payload = CACHE.get(_dataset(qs, config.DEFAULT_DATASET), latest_only=True)
            page, cursor = paginate(payload["items"], limit=_limit(qs), cursor=qs.get("cursor", [None])[0])
            return _json_response(
                self,
                200,
                envelope(
                    items=page,
                    data_status=payload["data_status"],
                    next_cursor=cursor,
                    retrieved_at=payload.get("retrieved_at"),
                ),
            )
        if path == "/v1/history":
            dataset = _dataset(qs, config.DEFAULT_HISTORY_DATASET)
            if not dataset.endswith("_history"):
                dataset = (
                    f"{dataset}_history"
                    if f"{dataset}_history" in config.DATASETS.values()
                    else config.DEFAULT_HISTORY_DATASET
                )
            payload = CACHE.get(dataset, latest_only=False)
            page, cursor = paginate(payload["items"], limit=_limit(qs), cursor=qs.get("cursor", [None])[0])
            return _json_response(
                self,
                200,
                envelope(
                    items=page,
                    data_status=payload["data_status"],
                    next_cursor=cursor,
                    retrieved_at=payload.get("retrieved_at"),
                ),
            )
        if path.startswith("/v1/records/"):
            record_id = unquote(path[len("/v1/records/") :])
            rows: list[dict[str, Any]] = CACHE.get(config.DEFAULT_DATASET)["items"]
            record = next((row for row in rows if str(row.get("record_id")) == record_id), None)
            if record is None:
                return _json_response(
                    self, 404, envelope(items=[], data_status="not_found", extra={"error": "record not found"})
                )
            return _json_response(self, 200, envelope(items=[record], data_status="ok"))
        return _json_response(
            self, 404, envelope(items=[], data_status="not_found", extra={"error": "unknown endpoint"})
        )


class Server(ThreadingHTTPServer):
    def handle_error(self, request: Any, client_address: Any) -> None:
        # http.server swallows handler exceptions (500 + traceback on stderr); report them first.
        _sentry.capture(sys.exc_info()[1])
        super().handle_error(request, client_address)


def main() -> None:
    require_token_for_bind(config.API_HOST, config.API_READ_TOKEN)
    server = Server((config.API_HOST, config.API_PORT), Handler)
    print(f"book-job-data API listening on http://{config.API_HOST}:{config.API_PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
