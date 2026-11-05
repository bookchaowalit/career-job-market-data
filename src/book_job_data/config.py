"""Runtime configuration for the separate Track B job data product."""

from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = Path(os.environ.get("DATA_DIR", str(PROJECT_ROOT / "data")))
FIXTURES_DIR = Path(os.environ.get("FIXTURES_DIR", str(PROJECT_ROOT / "fixtures")))

REPO_NAME = "book-job-data"
DOMAIN = "jobs"
SCHEMA_VERSION = "job.v1"
# Render/containers provide API_HOST explicitly; local development remains loopback.
API_HOST = os.environ.get("API_HOST", "127.0.0.1")
API_PORT = int(os.environ.get("PORT", os.environ.get("API_PORT", "8109")))
API_READ_TOKEN = os.environ.get("BOOK_JOB_DATA_API_TOKEN", os.environ.get("API_READ_TOKEN", "")).strip()
# Loaded datasets are reused for this many seconds; the lake changes at most hourly.
CACHE_TTL_SECONDS = float(os.environ.get("BOOK_JOB_DATA_CACHE_TTL_SECONDS", "300"))
# Per-client request budget for authenticated endpoints (0 disables the limit).
RATE_LIMIT_PER_MINUTE = int(os.environ.get("BOOK_JOB_DATA_RATE_LIMIT_PER_MINUTE", "120"))

FREE_ONLY = os.environ.get("FREE_ONLY", "true").strip().lower() in {"1", "true", "yes", "on"}
ALLOW_PAID_PROVIDERS = os.environ.get("ALLOW_PAID_PROVIDERS", "false").strip().lower() in {"1", "true", "yes", "on"}
ALLOW_EXTERNAL_WRITES = os.environ.get("ALLOW_EXTERNAL_WRITES", "false").strip().lower() in {"1", "true", "yes", "on"}
ALLOW_REFRESH = os.environ.get("ALLOW_REFRESH", "false").strip().lower() in {"1", "true", "yes", "on"}

REQUEST_TIMEOUT_SECONDS = float(os.environ.get("REQUEST_TIMEOUT_SECONDS", "10"))
STALE_AFTER_HOURS = float(os.environ.get("STALE_AFTER_HOURS", "48"))
DATA_LAKE_URI = os.environ.get("SOLO_EMPIRE_DATA_LAKE_URI", os.environ.get("DATA_LAKE_URI", ""))
SOLO_DATA_LAKE_ROOT = os.environ.get("SOLO_DATA_LAKE_ROOT", "")
LAKE_READ_MODE = os.environ.get("LAKE_READ_MODE", "parquet").strip().lower()
LAKE_READ_FALLBACK = os.environ.get("LAKE_READ_FALLBACK", "error").strip().lower()
# Latest views of job_postings, job_matches and job_leads: "silver" reads the newest jobs
# Silver part (Bronze fallback); anything else keeps the full Bronze read.
# See docs/specs/silver-job-postings-read.md. The older variable name still works.
SILVER_READ_LAYER = (
    (os.environ.get("BOOK_JOB_DATA_READ_LAYER") or os.environ.get("BOOK_JOB_DATA_JOB_POSTINGS_LAYER") or "silver")
    .strip()
    .lower()
)
SILVER_MAX_AGE_HOURS = float(os.environ.get("BOOK_JOB_DATA_SILVER_MAX_AGE_HOURS", "6"))
LINEAGE_FILE = "lake_lineage.json"
BRONZE_SCHEMA_VERSION = "1"
PRIVACY_CLASS = "internal"
RETENTION_CLASS = "operational"

DATASETS = {
    "job_postings": "job_postings",
    "job_postings_history": "job_postings_history",
    "job_matches": "job_matches",
    "job_matches_history": "job_matches_history",
    "job_leads": "job_leads",
    "job_leads_history": "job_leads_history",
}
DEFAULT_DATASET = DATASETS["job_postings"]
DEFAULT_HISTORY_DATASET = DATASETS["job_postings_history"]
