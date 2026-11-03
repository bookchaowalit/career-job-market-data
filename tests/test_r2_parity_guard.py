from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "r2_parity.py"


def _load():
    spec = importlib.util.spec_from_file_location("r2_parity", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


class R2ParityGuardTests(unittest.TestCase):
    """The verifier writes fixture rows (provider=offline_fixture); it must never
    touch the production lake, where two such rows already leaked once."""

    def test_production_prefix_is_refused(self):
        guard = _load().check_parity_prefix
        for uri in (
            "s3://solo-empire-portfolio/portfolio-demo/book-job-data-001",
            "s3://solo-empire-portfolio/portfolio-demo/book-job-data-001/",
            "s3://other-bucket/x/book-job-data-001",
        ):
            with self.assertRaisesRegex(SystemExit, "parity"):
                guard(uri)

    def test_only_a_dedicated_parity_prefix_is_accepted(self):
        guard = _load().check_parity_prefix
        guard("s3://solo-empire-portfolio/portfolio-demo/book-job-data-parity-001")
        for uri in ("s3://solo-empire-portfolio/portfolio-demo/anything", "/tmp/lake", "s3://bucket"):
            with self.assertRaisesRegex(SystemExit, "parity"):
                guard(uri)


if __name__ == "__main__":
    unittest.main()
