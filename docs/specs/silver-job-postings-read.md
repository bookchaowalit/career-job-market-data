# Spec: serve the `job_postings` latest view from Silver

Status: accepted (2026-11-05, owner chose "Silver, Bronze fallback"). Standard: ADR-0004.
Code: `src/book_job_data/store.py`, `src/book_job_data/config.py`. Tests: `tests/test_silver_read.py`.
Producer contract: data-lake-pipelines `docs/specs/jobs-gold-pointer.md`.

## Problem

`load_dataset("job_postings", latest_only=True)` reads every Bronze part ever written, parses
`payload_json` per row and keeps the newest row per record: ~140 s per load on R2, growing with
every capture batch. The jobs Silver part published every 2 h by `jobs_marts.py` is the same view
in one Parquet file.

## Behaviour

1. Only `job_postings` with `latest_only=True` changes. History reads and the other five datasets
   keep the Bronze read.
2. With `BOOK_JOB_DATA_JOB_POSTINGS_LAYER=silver` (the default) the store lists
   `control/pointers/gold/source=book-job-data/domain=jobs/`, takes the greatest `.json` key,
   and requires `pointer_version == "1"`, `layer == "gold"` and a `silver_key` under
   `silver/domain=jobs/`. It reads that one part and builds each item exactly as from Bronze
   (`payload_json` plus `record_id`, `event_time`, `observed_at`, `ingest_run_id` (from
   `bronze_ingest_run_id`), `source_record_id`, `raw_object_key`).
3. The response has the Bronze response's keys, with `source_kind` `silver_s3_parquet` (R2) or
   `silver_parquet` (local), `path` = the Silver part URI, `fallback_reason` `null`, plus
   `silver_run_id` and `gold_run_id`.
4. Fallback: if there is no pointer, the pointer is invalid, its `published_at` is older than
   `BOOK_JOB_DATA_SILVER_MAX_AGE_HOURS` (default 6, i.e. three missed marts runs), the part cannot
   be read, or it has no rows, the store serves the Bronze read and sets `fallback_reason` to
   `silver: <reason>`. Silver never turns a working Bronze read into an error.
5. `BOOK_JOB_DATA_JOB_POSTINGS_LAYER=bronze` restores the old read with no Silver attempt. Any
   other value is treated as `bronze`.
6. `/v1/metadata` reports `storage_model` `lake_first_job_silver_s3_duckdb` /
   `lake_first_job_silver_duckdb` for Silver loads.

## Known differences from the Bronze view

Silver drops quarantined rows (at most 2 % per run, else no Gold is published) and excluded test
providers (`offline_fixture`), and it lags Bronze by up to ~2.5 h (capture `:17` every 6 h, marts
`:47` every 2 h).
