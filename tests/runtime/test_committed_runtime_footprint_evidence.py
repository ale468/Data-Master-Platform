# Copyright (C) 2026 Alexandre Ferreira
# SPDX-License-Identifier: AGPL-3.0-only

import json
import re
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
EVIDENCE_DIR = REPO_ROOT / "tests" / "evidence" / "runtime-footprint"


class CommittedRuntimeFootprintEvidenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        evidence_files = sorted(EVIDENCE_DIR.glob("*.json"))
        if len(evidence_files) != 1:
            raise AssertionError("Expected exactly one committed runtime footprint summary.")
        cls.raw = evidence_files[0].read_text(encoding="utf-8")
        cls.evidence = json.loads(cls.raw)

    def test_identity_and_measurement_contract_are_explicit(self):
        self.assertEqual(
            self.evidence["evidence_kind"],
            "data_master_hive_deferral_public_summary",
        )
        self.assertEqual(self.evidence["status"], "PASS")
        self.assertEqual(self.evidence["issue"], 8)
        for revision in self.evidence["commits"].values():
            self.assertRegex(revision, r"^[0-9a-f]{40}$")
        contract = self.evidence["measurement_contract"]
        self.assertEqual(contract["phase"], "post-e2e")
        self.assertEqual(contract["sample_count"], 6)
        self.assertEqual(contract["sample_interval_seconds"], 10)

    def test_savings_recalculate_from_baseline_and_candidate(self):
        for section in (
            "object_counts",
            "configured_resources",
            "observed_resources",
        ):
            for name, measurement in self.evidence[section].items():
                self.assertEqual(
                    measurement["saved"],
                    measurement["baseline"] - measurement["candidate"],
                    f"{section}.{name}",
                )
                self.assertGreater(measurement["saved"], 0, f"{section}.{name}")

    def test_catalog_components_are_removed_and_functional_gates_pass(self):
        self.assertEqual(
            self.evidence["removed_catalog_runtime"]["workloads"],
            ["hive-metastore", "postgres-metastore"],
        )
        for variant in ("baseline", "candidate"):
            self.assertTrue(all(
                value == "PASS"
                for value in self.evidence["functional_equivalence"][variant].values()
            ))

    def test_duration_and_claim_limits_do_not_overstate_the_observation(self):
        duration = self.evidence["durations"]["e2e_seconds"]
        self.assertAlmostEqual(
            duration["delta"],
            duration["candidate"] - duration["baseline"],
            places=3,
        )
        self.assertGreater(duration["delta"], 0)
        limits = self.evidence["limitations"]
        self.assertIn(
            "e2e_candidate_was_25.312_seconds_slower_so_no_speedup_claim",
            limits,
        )
        self.assertIn(
            "no_financial_energy_cloud_or_production_savings_claim",
            limits,
        )

    def test_summary_is_publication_safe_and_source_hashes_are_immutable(self):
        for digest in self.evidence["source_documents"].values():
            self.assertRegex(digest, r"^[0-9a-f]{64}$")
        self.assertEqual(
            self.evidence["privacy"],
            {
                "classification": "technical_aggregate_only",
                "contains_pii": False,
                "contains_secrets": False,
                "contains_business_payload": False,
                "contains_local_paths": False,
            },
        )
        self.assertNotRegex(self.raw, r"[A-Za-z]:\\")
        self.assertIsNone(re.search(r"(?i)password|bearer\s+|secret_key", self.raw))


if __name__ == "__main__":
    unittest.main()