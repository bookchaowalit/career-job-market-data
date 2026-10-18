#!/usr/bin/env python3
"""Ingest scraper capture files into the Track B job lake.

The scraper repository remains a collection-only producer. This boundary reads
its file handoff, archives exact CSV bytes, writes Bronze, then optionally
creates a CSV projection after every lake write succeeds.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import _sentry, config, lake
from .privacy import sanitize_record
from .store import canonical_url, stable_job_id, project_csv


SCRAPER_REPO = config.PROJECT_ROOT.parent / "career-job-scraper"


def _event_time(row: dict[str, Any]) -> str:
    for key in ("observed_at", "scraped_at", "posted", "created_date", "date"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_rows(rows: list[dict[str, Any]], *, provider: str, input_name: str) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for raw in rows:
        # DictReader puts surplus cells under a ``None`` key (a list); drop it.
        row = {str(key): str(value or "").strip() for key, value in raw.items() if key and not isinstance(value, list)}
        if not any(row.values()):
            continue  # blank line or a row of empty cells: never a record
        url = canonical_url(row.get("url") or row.get("source_url") or row.get("apply_url") or row.get("link"))
        if url:
            row["canonical_url"] = url
        row["id"] = stable_job_id(row)
        row["event_time"] = _event_time(row)
        row["observed_at"] = row["event_time"]
        row["provider"] = provider or row.get("source") or row.get("board") or "unknown"
        row["input_name"] = input_name
        deduped[row["id"]] = sanitize_record(row)
    return list(deduped.values())


def _dataset_for(path: Path) -> tuple[str, str]:
    name = path.name.lower()
    if name == "matched_jobs.csv":
        return config.DATASETS["job_matches"], config.DATASETS["job_matches_history"]
    if name in {"leads.csv", "pipeline.csv"} or "jobsdb_jobs" in name:
        return config.DATASETS["job_leads"], config.DATASETS["job_leads_history"]
    return config.DEFAULT_DATASET, config.DEFAULT_HISTORY_DATASET


def read_capture(path: Path) -> tuple[bytes, list[dict[str, Any]]]:
    raw = path.read_bytes()
    text = raw.decode("utf-8-sig")
    return raw, list(csv.DictReader(io.StringIO(text)))


def ingest_capture(
    path: Path,
    *,
    data_lake_uri: str,
    provider: str = "book-job-scraping",
    capture: tuple[bytes, list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Land one capture. ``capture`` reuses bytes/rows the caller already read,
    so the lake receives exactly what was validated (no second file read)."""
    raw, rows = capture if capture is not None else read_capture(path)
    current_dataset, history_dataset = _dataset_for(path)
    records = normalize_rows(rows, provider=provider, input_name=path.name)
    if not records:
        raise lake.LakeIngestError(f"capture has no valid rows: {path}")
    metadata = {
        "capture_source": "book-job-scraping",
        "input_name": path.name,
        "input_path": str(path),
        "record_count_before_dedupe": len(rows),
        "dataset_role": "snapshot",
    }
    snapshot = lake.ingest_to_lake(
        raw=raw,
        records=records,
        dataset=current_dataset,
        data_lake_uri=data_lake_uri,
        metadata=metadata,
        provider=provider,
    )
    # The shared ingest manifest keys batches by raw checksum. Add a
    # deterministic dataset marker for the history copy so the same capture
    # can safely land in both datasets without a manifest collision. The
    # snapshot above remains the exact source bytes used for replay.
    history_raw = raw + f"\n# book-job-data dataset={history_dataset}\n".encode("utf-8")
    history = lake.ingest_to_lake(
        raw=history_raw,
        records=records,
        dataset=history_dataset,
        data_lake_uri=data_lake_uri,
        metadata={**metadata, "dataset_role": "history"},
        provider=provider,
    )
    lake.write_lineage(snapshot, dataset=current_dataset, data_dir=config.DATA_DIR)
    lake.write_lineage(history, dataset=history_dataset, data_dir=config.DATA_DIR)
    return {
        "path": str(path),
        "dataset": current_dataset,
        "history_dataset": history_dataset,
        "records": len(records),
        "snapshot": snapshot,
        "history": history,
    }


def default_inputs() -> list[Path]:
    data = Path(__import__("os").environ.get("BOOK_JOB_SCRAPING_DATA_DIR", str(SCRAPER_REPO / "data")))
    candidates = [
        data / "job_postings.csv",
        data / "matched_jobs.csv",
        data / "leads.csv",
        data / "pipeline.csv",
        data / "exported" / "jobsdb_jobs.csv",
    ]
    return [path for path in candidates if path.is_file()]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", action="append", dest="inputs", help="Capture CSV; repeat for multiple files")
    parser.add_argument(
        "--data-lake-uri",
        default=None,
        help="Lake URI (defaults to the shared Solo Empire configuration)",
    )
    parser.add_argument("--provider", default="book-job-scraping")
    parser.add_argument(
        "--project-csv", action="store_true", help="Write projections only after all captures land successfully"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Validate and normalize captures without writing the lake"
    )
    args = parser.parse_args(argv)
    data_lake_uri = args.data_lake_uri or lake.default_data_lake_uri()
    inputs = [Path(value).expanduser() for value in args.inputs] if args.inputs else default_inputs()
    if not inputs:
        print("No scraper capture files found; run book-job-scraping first.")
        return 0
    normalized: list[tuple[Path, list[dict[str, Any]], tuple[str, str]]] = []
    captures: dict[Path, tuple[bytes, list[dict[str, Any]]]] = {}
    for path in dict.fromkeys(inputs):  # the same file twice would land twice
        if not path.is_file():
            print(f"Input does not exist: {path}", file=sys.stderr)
            return 2
        try:
            raw, rows = read_capture(path)
        except (UnicodeDecodeError, csv.Error) as exc:
            print(f"Input is not a UTF-8 CSV capture: {path}: {exc}", file=sys.stderr)
            return 2
        captures[path] = (raw, rows)
        datasets = _dataset_for(path)
        normalized.append((path, normalize_rows(rows, provider=args.provider, input_name=path.name), datasets))
    if args.dry_run:
        print(
            json.dumps(
                {
                    "status": "dry_run",
                    "captures": [
                        {"path": str(p), "records": len(rows), "dataset": ds[0]} for p, rows, ds in normalized
                    ],
                },
                indent=2,
            )
        )
        return 0

    results: list[dict[str, Any]] = []
    try:
        for path, _rows, _datasets in normalized:
            results.append(
                ingest_capture(path, data_lake_uri=data_lake_uri, provider=args.provider, capture=captures[path])
            )
    except Exception as exc:  # fail closed: no projection is touched
        _sentry.capture(exc)
        print(f"Lake ingest failed; CSV projection was not written: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    if args.project_csv:
        for result in results:
            project_csv(
                next(rows for path, rows, _ in normalized if str(path) == result["path"]),
                path=config.DATA_DIR / f"{result['dataset']}.csv",
            )
    print(
        json.dumps(
            {
                "status": "success",
                "captures": [
                    {
                        "path": r["path"],
                        "dataset": r["dataset"],
                        "history_dataset": r["history_dataset"],
                        "records": r["records"],
                    }
                    for r in results
                ],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
