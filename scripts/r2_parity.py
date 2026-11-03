#!/usr/bin/env python3
"""Operator-gated R2 parity verifier for the job-data capture boundary."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from book_job_data import lake
from book_job_data.ingest import ingest_capture


def check_parity_prefix(data_lake_uri: str) -> None:
    """Refuse any lake but a dedicated ``s3://…/<name-with-parity>`` prefix.

    The verifier writes fixture rows (provider ``offline_fixture``). It once ran
    against ``portfolio-demo/book-job-data-001``, which is the production Track B
    lake, and two fixture rows reached production Bronze.
    """
    prefix = data_lake_uri.rstrip("/")
    last = prefix.rsplit("/", 1)[-1] if prefix.startswith("s3://") and prefix.count("/") >= 3 else ""
    if "parity" not in last:
        raise SystemExit(
            "Refusing: --data-lake-uri must be a dedicated parity prefix such as "
            "s3://<bucket>/portfolio-demo/book-job-data-parity-001, never the production lake"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path("fixtures/job_postings.csv"))
    parser.add_argument("--data-lake-uri", required=True)
    parser.add_argument("--provider", default="offline_fixture")
    args = parser.parse_args()
    check_parity_prefix(args.data_lake_uri)
    if os.environ.get("DATA_LAKE_CLOUD_WRITE_ENABLED", "").lower() not in {"1", "true", "yes", "on"}:
        raise SystemExit("Refusing hosted replay: DATA_LAKE_CLOUD_WRITE_ENABLED=true is required")

    raw = args.input.read_bytes()
    first = ingest_capture(args.input, data_lake_uri=args.data_lake_uri, provider=args.provider)
    retry = ingest_capture(args.input, data_lake_uri=args.data_lake_uri, provider=args.provider)
    replayed = lake.landing_object_bytes(first["snapshot"]["raw_key"], data_lake_uri=args.data_lake_uri)
    for parent in [Path(__file__).resolve(), *Path(__file__).resolve().parents]:
        candidate = parent / "infra" / "scripts"
        if (candidate / "data_lake" / "storage.py").is_file():
            sys.path.insert(0, str(candidate))
            break
    from data_lake.storage import ObjectStore  # type: ignore

    keys = ObjectStore(args.data_lake_uri).list_keys()
    print(
        json.dumps(
            {
                "status": "verified",
                "prefix": args.data_lake_uri,
                "datasets": {
                    first["dataset"]: {
                        "rows": first["records"],
                        "raw_bytes_exact": replayed == raw,
                        "manifest": first["snapshot"]["manifest_key"],
                    },
                    first["history_dataset"]: {"rows": first["records"], "manifest": first["history"]["manifest_key"]},
                },
                "snapshot_retry_idempotent": first["snapshot"]["run_id"] == retry["snapshot"]["run_id"],
                "history_retry_idempotent": first["history"]["run_id"] == retry["history"]["run_id"],
                "object_count": len(keys),
                "provider": args.provider,
                "source_boundary": "book-job-scraping file capture only",
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
