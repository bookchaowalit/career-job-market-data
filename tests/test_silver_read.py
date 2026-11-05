from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

from book_job_data import config
from book_job_data.ingest import ingest_capture
from book_job_data.store import load_dataset, load_records


BRONZE_CSV = "title,company,url,source,scraped_at\nBronze Engineer,Example,https://example.com/jobs/a,RemoteOK,2026-11-05T00:00:00Z\n"
POINTER_PREFIX = "control/pointers/gold/source=book-job-data/domain=jobs/"
SILVER_KEY = "silver/domain=jobs/dataset=job_postings/schema_version=2/event_date=2026-11-05/part-silver-abc.parquet"


def _write_silver(lake: Path, titles: list[str]) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    rows = [
        {
            "source_record_id": f"job-{i}",
            "event_time": "2026-11-05T01:00:00Z",
            "received_at": "2026-11-05T01:05:00Z",
            "payload_json": json.dumps({"id": f"job-{i}", "title": title, "url": f"https://example.com/jobs/{i}"}),
            "bronze_ingest_run_id": "2026-11-05-batch",
            "raw_object_key": "landing/source=book-job-data/batch_id=x/payload.csv",
            "quality_status": "valid",
        }
        for i, title in enumerate(titles)
    ]
    path = lake / SILVER_KEY
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def _write_pointer(lake: Path, published: datetime, **overrides) -> None:
    pointer = {
        "pointer_version": "1",
        "layer": "gold",
        "source": "book-job-data",
        "domain": "jobs",
        "published_at": published.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "gold_run_id": "gold-abc",
        "manifest_key": "control/manifests/gold/source=book-job-data/domain=jobs/gold_run_id=gold-abc.json",
        "silver_run_id": "silver-abc",
        "silver_key": SILVER_KEY,
        "silver_record_count": 2,
    }
    pointer.update(overrides)
    path = lake / POINTER_PREFIX / f"published_at={published.strftime('%Y%m%dT%H%M%SZ')}-gold-abc.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(pointer), encoding="utf-8")


class SilverJobPostingsTests(unittest.TestCase):
    def setUp(self):
        try:
            import duckdb  # noqa: F401
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest("pyarrow/duckdb not installed")
        self._tmp = tempfile.TemporaryDirectory()
        root = Path(self._tmp.name)
        self.lake = root / "lake"
        source = root / "job_postings.csv"
        source.write_text(BRONZE_CSV, encoding="utf-8")
        with mock.patch.object(config, "DATA_DIR", root / "projection"):
            ingest_capture(source, data_lake_uri=str(self.lake), provider="fixture")
        self.now = datetime.now(timezone.utc)
        for patch in (
            mock.patch.object(config, "DATA_LAKE_URI", str(self.lake)),
            mock.patch.object(config, "JOB_POSTINGS_READ_LAYER", "silver"),
            mock.patch.object(config, "SILVER_MAX_AGE_HOURS", 6.0),
        ):
            patch.start()
            self.addCleanup(patch.stop)
        self.addCleanup(self._tmp.cleanup)

    def test_latest_job_postings_come_from_the_newest_silver_pointer(self):
        _write_silver(self.lake, ["Silver One", "Silver Two"])
        _write_pointer(self.lake, self.now - timedelta(hours=9), silver_key="silver/domain=jobs/missing.parquet")
        _write_pointer(self.lake, self.now - timedelta(minutes=30))
        loaded = load_records()
        self.assertEqual(loaded["source_kind"], "silver_parquet")
        self.assertIsNone(loaded["fallback_reason"])
        self.assertEqual(loaded["silver_run_id"], "silver-abc")
        self.assertEqual(loaded["gold_run_id"], "gold-abc")
        self.assertTrue(loaded["path"].endswith(SILVER_KEY))
        self.assertEqual(sorted(item["title"] for item in loaded["items"]), ["Silver One", "Silver Two"])
        item = loaded["items"][0]
        self.assertEqual(item["ingest_run_id"], "2026-11-05-batch")
        self.assertEqual(item["source_record_id"], "job-0")
        self.assertTrue(item["record_id"].startswith("job:"))
        self.assertIn(loaded["data_status"], {"ok", "stale"})

    def test_no_pointer_falls_back_to_bronze(self):
        loaded = load_records()
        self.assertEqual(loaded["source_kind"], "bronze_parquet")
        self.assertIn("silver:", loaded["fallback_reason"])
        self.assertEqual([item["title"] for item in loaded["items"]], ["Bronze Engineer"])

    def test_old_pointer_falls_back_to_bronze(self):
        _write_silver(self.lake, ["Silver One"])
        _write_pointer(self.lake, self.now - timedelta(hours=7))
        loaded = load_records()
        self.assertEqual(loaded["source_kind"], "bronze_parquet")
        self.assertIn("older than", loaded["fallback_reason"])

    def test_invalid_pointer_or_unreadable_part_falls_back_to_bronze(self):
        for overrides in (
            {"pointer_version": "2"},
            {"silver_key": "bronze/domain=jobs/x.parquet"},
            {"silver_key": "silver/domain=jobs/missing.parquet"},
            {
                "silver_key": "silver/domain=jobs/dataset=job_postings/schema_version=2/event_date=2026-11-04/part-silver-def.parquet"
            },
            {"silver_key": "silver/domain=jobs/../../control/secret.parquet"},
            {
                "silver_key": "silver/domain=jobs/dataset=job_postings/schema_version=2/event_date=*/part-silver-abc.parquet"
            },
        ):
            with self.subTest(overrides=overrides):
                for old in (self.lake / POINTER_PREFIX).glob("*.json"):
                    old.unlink()
                _write_pointer(self.lake, self.now, **overrides)
                loaded = load_records()
                self.assertEqual(loaded["source_kind"], "bronze_parquet")
                self.assertIn("silver:", loaded["fallback_reason"])
                self.assertEqual(len(loaded["items"]), 1)

    def test_bronze_layer_setting_skips_silver(self):
        _write_silver(self.lake, ["Silver One"])
        _write_pointer(self.lake, self.now)
        with mock.patch.object(config, "JOB_POSTINGS_READ_LAYER", "bronze"):
            loaded = load_records()
        self.assertEqual(loaded["source_kind"], "bronze_parquet")
        self.assertIsNone(loaded["fallback_reason"])

    def test_history_and_other_datasets_stay_on_bronze(self):
        _write_silver(self.lake, ["Silver One"])
        _write_pointer(self.lake, self.now)
        history = load_dataset(config.DEFAULT_DATASET, latest_only=False)
        self.assertEqual(history["source_kind"], "bronze_parquet")
        self.assertEqual([item["title"] for item in history["items"]], ["Bronze Engineer"])
        other = load_dataset(config.DATASETS["job_matches"], latest_only=True)
        self.assertTrue(other["source_kind"].startswith("bronze"))

    def test_metadata_storage_model_names_silver(self):
        from book_job_data.api import _storage_model

        self.assertEqual(_storage_model("silver_s3_parquet"), "lake_first_job_silver_s3_duckdb")
        self.assertEqual(_storage_model("silver_parquet"), "lake_first_job_silver_duckdb")
        self.assertEqual(_storage_model("bronze_s3_parquet"), "lake_first_job_bronze_s3_duckdb")


if __name__ == "__main__":
    unittest.main()
