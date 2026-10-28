"""PII allowlist/redaction and API hardening (token guard, cache, rate limit)."""

import json
import threading
import time
import unittest
import urllib.request
from pathlib import Path
from unittest.mock import patch

from book_job_data import api
from book_job_data.ingest import _dataset_for, normalize_rows
from book_job_data import config
from book_job_data.privacy import EMAIL_MASK, PHONE_MASK, redact_text, sanitize_record


class PrivacyTests(unittest.TestCase):
    def test_unknown_and_contact_columns_are_dropped(self):
        clean = sanitize_record({"title": "Dev", "email": "a@b.co", "contact": "Jane", "surprise": "x"})
        self.assertEqual(clean, {"title": "Dev"})

    def test_contacts_in_free_text_are_masked(self):
        text = "Apply to jobs@acme.io or call +66 81 234 5678 / (02) 555-0199-12"
        out = redact_text(text)
        self.assertNotIn("jobs@acme.io", out)
        self.assertNotIn("234 5678", out)
        self.assertIn(EMAIL_MASK, out)
        self.assertIn(PHONE_MASK, out)

    def test_salaries_dates_and_urls_survive(self):
        for text in ("$90k - $105k", "120000 - 170000 THB", "posted 2026-09-30", "30,000-50,000"):
            self.assertEqual(redact_text(text), text)
        row = sanitize_record({"url": "https://x.io/jobs/123456789012", "description": "id 2026-09-30T10:00"})
        self.assertEqual(row["url"], "https://x.io/jobs/123456789012")

    def test_ingest_normalization_applies_allowlist(self):
        rows = normalize_rows(
            [{"title": "Dev", "url": "https://e.com/1", "email": "hr@e.com", "description": "mail hr@e.com"}],
            provider="fixture",
            input_name="job_postings.csv",
        )
        self.assertNotIn("email", rows[0])
        self.assertEqual(rows[0]["description"], f"mail {EMAIL_MASK}")

    def test_pipeline_capture_maps_to_job_leads(self):
        self.assertEqual(
            _dataset_for(Path("pipeline.csv")),
            (config.DATASETS["job_leads"], config.DATASETS["job_leads_history"]),
        )

    def test_matched_capture_maps_to_job_matches(self):
        self.assertEqual(
            _dataset_for(Path("matched_jobs.csv")),
            (config.DATASETS["job_matches"], config.DATASETS["job_matches_history"]),
        )

    def test_normalize_rows_preserves_qualification_evidence(self):
        rows = [
            {
                "title": "Contract Engineer",
                "url": "https://example.com/contract",
                "qualification_status": "PASS",
                "policy_version": "contract-first-thailand-v1",
                "employment_type": "Contract",
                "application_readiness": "REVIEW_REQUIRED",
            }
        ]
        result = normalize_rows(rows, provider="fixture", input_name="matched_jobs.csv")
        self.assertEqual(result[0]["qualification_status"], "PASS")
        self.assertEqual(result[0]["application_readiness"], "REVIEW_REQUIRED")


class ApiHardeningTests(unittest.TestCase):
    def test_public_bind_without_token_is_refused(self):
        for host in ("0.0.0.0", "", "10.0.0.5", "::"):
            with self.assertRaises(SystemExit):
                api.require_token_for_bind(host, "")
        for host in ("127.0.0.1", "localhost", "::1"):
            api.require_token_for_bind(host, "")
        api.require_token_for_bind("0.0.0.0", "secret")

    def test_rate_limiter_blocks_after_budget(self):
        limiter = api._RateLimiter(3)
        self.assertEqual([limiter.allow("1.2.3.4") for _ in range(4)], [True, True, True, False])
        self.assertTrue(limiter.allow("5.6.7.8"))
        self.assertTrue(api._RateLimiter(0).allow("x"))

    def test_cache_reuses_loaded_dataset_and_sanitizes(self):
        cache = api._DatasetCache(ttl_seconds=60)
        payload = {"data_status": "ok", "items": [{"title": "Dev", "email": "x@y.io"}]}
        with patch.object(api, "load_dataset", return_value=payload) as load:
            first = cache.get("job_postings")
            second = cache.get("job_postings")
        self.assertEqual(load.call_count, 1)
        self.assertIs(first, second)
        self.assertEqual(first["items"], [{"title": "Dev"}])
        self.assertEqual(cache.peek_status("job_postings"), "ok")

    def test_stale_entry_is_served_while_one_refresh_runs_in_background(self):
        cache = api._DatasetCache(ttl_seconds=60)
        release, done = threading.Event(), threading.Event()
        new = {"data_status": "ok", "items": [{"title": "New"}]}

        def slow_load(dataset, *, latest_only=True):
            release.wait(5)
            done.set()
            return new

        with patch.object(api, "load_dataset", return_value={"data_status": "ok", "items": [{"title": "Old"}]}):
            cache.get("job_postings")
        with patch.object(api.time, "monotonic", return_value=time.monotonic() + 120):
            with patch.object(api, "load_dataset", side_effect=slow_load) as load:
                stale = cache.get("job_postings")
                again = cache.get("job_postings")
                release.set()
                self.assertTrue(done.wait(5))
                for _ in range(50):
                    if cache.peek("job_postings")["items"] == [{"title": "New"}]:
                        break
                    time.sleep(0.01)
        self.assertEqual(stale["items"], [{"title": "Old"}])
        self.assertEqual(again["items"], [{"title": "Old"}])
        self.assertEqual(load.call_count, 1)
        self.assertEqual(cache.peek("job_postings")["items"], [{"title": "New"}])

    def test_warm_preloads_datasets_in_background(self):
        cache = api._DatasetCache(ttl_seconds=60)
        with patch.object(api, "load_dataset", return_value={"data_status": "ok", "items": []}) as load:
            cache.warm(["job_postings", "job_leads"]).join(5)
        self.assertEqual([c.args[0] for c in load.call_args_list], ["job_postings", "job_leads"])
        self.assertEqual(cache.peek_status("job_leads"), "ok")

    def test_metadata_never_loads_full_history(self):
        calls = []

        def fake_load(dataset, *, latest_only=True):
            calls.append((dataset, latest_only))
            return {"data_status": "ok", "items": [{"title": "Dev"}], "source_kind": "bronze_s3"}

        cache = api._DatasetCache(ttl_seconds=60)
        server = api.Server(("127.0.0.1", 0), api.Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        url = f"http://127.0.0.1:{server.server_address[1]}/v1/metadata"
        try:
            with (
                patch.object(api, "load_dataset", side_effect=fake_load),
                patch.object(api, "CACHE", cache),
                patch.object(api, "load_lineage", return_value=None),
                patch.object(config, "API_READ_TOKEN", ""),
            ):
                with urllib.request.urlopen(url, timeout=10) as resp:
                    cold = json.load(resp)["items"][0]["dataset_counts"]
                cold_calls = list(calls)
                cache.get("job_postings_history", latest_only=False)
                with urllib.request.urlopen(url, timeout=10) as resp:
                    warm = json.load(resp)["items"][0]["dataset_counts"]
        finally:
            server.shutdown()
            server.server_close()
        self.assertEqual(
            sorted(cold_calls), sorted((name, True) for name in ("job_postings", "job_matches", "job_leads"))
        )
        self.assertEqual(cold["job_postings"], 1)
        self.assertIsNone(cold["job_postings_history"])
        self.assertIsNone(cold["job_leads_history"])
        self.assertEqual(warm["job_postings_history"], 1)


if __name__ == "__main__":
    unittest.main()
