# book-job-data

`book-job-data` is the separate Track B data boundary for job discovery. The
`book-job-scraping` repository remains collection-only: it produces capture
files and does not call Solo Empire APIs or write another application's
database.

```text
book-job-scraping capture CSV
  → exact bytes in landing/
  → Bronze job.v1 Parquet
  → manifest + lineage
  → read-only API :8109 / DuckDB (local or direct R2/S3)
  → optional CSV projection for CLI/legacy handoff
```

Datasets are `job_postings`, `job_postings_history`, `job_matches`,
`job_matches_history`, `job_leads`, and `job_leads_history`. Jobs are never
sent to `opportunity.v1`; opportunity intelligence remains a separate Gold
synthesis product.

## Run locally

```bash
cd book-job-data
python -m pip install -e ".[lake]"
python -m book_job_data.ingest \
  --input /path/to/book-job-scraping/data/job_postings.csv \
  --data-lake-uri /path/to/solo-data-lake/data/lake

# Offline validation without storage
python -m book_job_data.ingest --input fixtures/job_postings.csv --dry-run

# API reads Bronze only; no CSV is opened by GET handlers
SOLO_EMPIRE_DATA_LAKE_URI=/path/to/solo-data-lake/data/lake \
  python -m book_job_data.api
```

The shared `data_lake` adapter/runtime is installed from the public,
MIT-licensed [`solo-empire-data-lake`](https://github.com/bookchaowalit/data-lake-core)
repository at a pinned commit. The parent repository remains a compatibility
integration boundary for its existing monorepo CI and Render service; the
public product does not require the private parent checkout.

Run the parent-repository E2E contract gate after installing `pyarrow` and
`duckdb`:

```bash
cd /path/to/solo-empire
python infra/tests/job_data_e2e_test.py
```

The gate uses the fixture and a temporary lake, starts the real `:8109` API,
checks Bronze lineage and history, calls `job_client`, verifies refresh is
forbidden, and confirms the API creates no CSV projection.

On a Windows host, `.\ops\windows\scheduled-ingest.ps1 install` registers an
hourly Task Scheduler ingest (idempotent: unchanged captures reuse their batch
id). Check the gate from the parent repository with `task data:job:readiness`.

The default input location is the sibling `book-job-scraping/data/` boundary.
Set `BOOK_JOB_SCRAPING_DATA_DIR` when the capture directory is elsewhere.

Capture handling: blank lines and rows of empty cells are skipped (a capture
with no real rows fails before any lake write), surplus cells beyond the
header are ignored, a non-UTF-8 input exits with status 2, a file passed twice
lands once, and each capture is read once so the lake receives exactly the
bytes that were validated. `--project-csv` output is written atomically.

## Tests

```bash
python -m pip install -e ".[lake]" pytest ruff
ruff check .
python -m pytest -q -rs
```

The lake tests write a temporary local lake through the pinned
`solo-empire-data-lake` runtime; no network or parent checkout is needed.
`tests/test_end_to_end_quality.py` reads Bronze (snapshot and history) and the
API items back and asserts that only allowlisted fields exist and no contact
detail from the capture survives.

## Contract

`source=book-job-data`, `domain=jobs`, `schema_version=job.v1`, port `8109`,
privacy class `internal`. Input capture bytes are kept as source evidence and
each Bronze row carries `source_record_id`, `event_time`, `ingest_run_id`, and
`raw_object_key` lineage.

Consumer contract: `GET /v1/records` and `GET /v1/history` with optional
`dataset=job_leads`/`job_matches`; `POST /v1/refresh` is forbidden by default.
The migration client calls the API first and uses the same Bronze DuckDB path
as an explicit fallback. Set `BOOK_JOB_DATA_API_TOKEN` for a hosted API; the
client sends it as a Bearer token without logging it. When
`SOLO_EMPIRE_DATA_LAKE_URI` is an `s3://` URI,
the API reads the bounded Bronze Parquet glob directly from R2/S3 through
DuckDB `httpfs`; it does not download a local mirror or open a scraper CSV.

### Privacy and API limits

- **Token required off loopback.** The API exits at startup when bound to a
  non-loopback address (for example Render's `0.0.0.0`) without
  `BOOK_JOB_DATA_API_TOKEN`. Every endpoint except `/healthz` needs
  `Authorization: Bearer <token>`.
- **Field allowlist and redaction.** `privacy.py` keeps only known `job.v1`
  fields (so `pipeline.csv` `contact`/`email` columns never land in Bronze) and
  masks email addresses and phone numbers in free-text fields. It runs at ingest
  and again on every API response. The exact capture bytes remain only in the
  internal landing zone as replay evidence.
- **Cheap health check.** `/healthz` does not read the lake; `data_status` is
  `not_loaded` until a data endpoint has warmed the cache.
- **Cache and rate limit.** Loaded datasets are reused for
  `BOOK_JOB_DATA_CACHE_TTL_SECONDS` (default 300). Each client gets
  `BOOK_JOB_DATA_RATE_LIMIT_PER_MINUTE` requests (default 120, `0` disables),
  keyed on the rightmost `X-Forwarded-For` entry; excess requests get `429`.

### Direct R2/S3 Bronze read

Use the same read mode for local and hosted object storage. The URI selects the
location; the mode stays explicit and fail-closed:

```bash
SOLO_EMPIRE_DATA_LAKE_URI=s3://<bucket>/<prefix> \
DATA_LAKE_S3_ENDPOINT=https://<account-id>.r2.cloudflarestorage.com \
LAKE_READ_MODE=parquet \
LAKE_READ_FALLBACK=error \
python -m book_job_data.api
```

Inject `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` from Infisical or the
runtime secret provider. Do not put them in this README, `.env` committed to
Git, API metadata, or a manifest. The API reports
`storage_model=lake_first_job_bronze_s3_duckdb` and
`records_source_kind=bronze_s3_parquet` when this path is active.

## Hosted R2 parity

The operator-gated verifier writes only to a bounded demo prefix and never
prints credentials:

```bash
npx --yes @infisical/cli@0.43.120 run \
  --projectId=<solo-empire-project-id> --env=dev --path=/cloudflare --silent -- \
  bash -lc 'export DATA_LAKE_CLOUD_WRITE_ENABLED=true; \
    python scripts/r2_parity.py \
    --input fixtures/job_postings.csv \
    --data-lake-uri s3://<bucket>/portfolio-demo/book-job-data-001'
```

Verified evidence: snapshot/history each contained 2 rows, snapshot raw bytes
replayed exactly, both manifests were present, the R2 prefix contained 6
objects, and snapshot/history retries were idempotent. The same prefix can be
read directly by the API with the remote Bronze path above; the verifier is
still storage parity only and does not replace the API read contract.

## Hosted deployment

**Live (2026-10-14):** Railway project `book-job-data`, service `book-job-data-api`
(Singapore, serverless; `railway.toml`), deployed with `railway up --ci --service book-job-data-api`.
`/healthz` is public; every `/v1/*` route needs `BOOK_JOB_DATA_API_TOKEN`. `SENTRY_DSN` (Railway
variable; error reporting, unset = off, see `src/book_job_data/_sentry.py`) and `SENTRY_ENVIRONMENT`
turn on Sentry. The image ships
the AWS CLI because the pinned `data_lake` runtime lists and reads `s3://` objects through it.
The first read after the service wakes loads Bronze from R2 and can take a minute or more;
later reads come from the 300-second cache. The Render lane below is the earlier,
superseded target.

The read-only API can be deployed directly from this public repository's
bounded Render lane. Keep ingestion and CSV projection outside the hosted
service; the hosted API reads Bronze Parquet from R2/S3 only.

In Render Dashboard, create a Web Service from this repository and use the
Blueprint in `render.yaml`, or configure the same values manually:

```text
Runtime: Docker
Plan: Free
Dockerfile: Dockerfile
Docker context: repository root
Health check: /healthz
```

Set these values in the Render Environment tab. The four credential/token
values must be secret values, not YAML literals:

```text
SOLO_EMPIRE_DATA_LAKE_URI=s3://<bucket>/<bounded-prefix>
DATA_LAKE_S3_ENDPOINT=https://<account-id>.r2.cloudflarestorage.com
AWS_ACCESS_KEY_ID=<R2 access key from secret manager>
AWS_SECRET_ACCESS_KEY=<R2 secret from secret manager>
BOOK_JOB_DATA_API_TOKEN=<random read-only API token>
LAKE_READ_MODE=parquet
LAKE_READ_FALLBACK=error
```

After deployment, verify the hosted path without enabling the lake fallback:

```bash
BOOK_JOB_DATA_URL=https://<service>.onrender.com \
BOOK_JOB_DATA_API_TOKEN='<token from secret manager>' \
PYTHONPATH=/path/to/solo-data-lake/infra/scripts \
  /path/to/solo-data-lake/.venv/bin/python \
  /path/to/solo-data-lake/infra/scripts/data_lake/hosted_job_api_e2e.py
```

Expected output has `status=verified`,
`storage_model=lake_first_job_bronze_s3_duckdb`, and
`records_source_kind=bronze_s3_parquet`. The free service is a portfolio/demo
lane: it may sleep after inactivity and its filesystem is ephemeral, so R2 is
the only durable data source.

Current repository status: the Render Blueprint, container, auth guard, and
verification command are deploy-ready. No public Render service URL is stored
in Git; create the service from the connected Render account before running
the hosted verifier.
