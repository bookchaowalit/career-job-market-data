# book-job-data — Product Brief

Status on 2026-10-22: **live** as a read-only API on Railway (`book-job-data-api`), deployed from `main` after CI
passes. This brief covers what exists in this repository.

## Who it is for

Internal consumers in the owner's tooling that need job-market data without touching the scraper or another
service's database (the product card names `job_client`). The operator is the owner. No external users.

## Problem

Job postings collected by `book-job-scraping` must be kept exactly as captured, checked later, and read by other
systems. Before this boundary, consumers read scraper CSV files directly, so a scraper change could break them and
nothing proved where a record came from.

## What exists

| Capability | Where | State |
|---|---|---|
| Ingest a capture CSV: exact bytes to landing, Bronze `job.v1` Parquet, manifest and lineage; skips blank rows, rejects non-UTF-8, ignores duplicate files; `--dry-run`, atomic `--project-csv` | `book-job-data` CLI (`book_job_data.ingest`) | works locally and in the hosted ingest runbook |
| Datasets `job_postings`, `job_matches`, `job_leads` and their `_history` tables | Bronze in the data lake (local or R2/S3) | live |
| Read-only API: `/healthz` (public), `/v1/metadata`, `/v1/records`, `/v1/records/{id}`, `/v1/history` (token required); field allowlist and personal-data redaction; `/v1/refresh` refused | `book-job-data-api` (`book_job_data.api`, port 8109) | live on Railway |
| Error reporting | Sentry when `SENTRY_DSN` is set (scrubbed, errors only) | live |
| Uptime | `platform-uptime-worker` probes `/healthz` | live |

Jobs are never sent to `opportunity.v1`; opportunity intelligence is a separate product.

## Non-goals

- No scraping here (that is `book-job-scraping`). No writes through the API. No user interface.

## Success measures

| Measure | How it is read | Target | Status |
|---|---|---|---|
| Freshness | age of the newest Bronze batch in `/v1/metadata` | < 26 hours | readable; no alert yet |
| Availability | uptime monitor on `/healthz` | ≥ 99% monthly (serverless, cold starts allowed) | probe live |
| Consumer errors | 5xx responses and Sentry issues | none unresolved for > 7 days | Sentry live |

## Known limits

- The first read after the serverless service wakes loads Bronze from R2 and can take a minute or more.
- The Render lane in README is an earlier, superseded target.
