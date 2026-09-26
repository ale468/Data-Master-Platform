# Copyright (C) 2026 Alexandre Ferreira
# SPDX-License-Identifier: AGPL-3.0-only

import json
import re
import unittest
from datetime import datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
EVIDENCE_DIR = REPO_ROOT / "tests" / "evidence" / "multibatch"


def _timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class CommittedMultibatchEvidenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        evidence_files = sorted(EVIDENCE_DIR.glob("*.json"))
        if len(evidence_files) != 1:
            raise AssertionError("Expected exactly one committed multibatch evidence file.")
        cls.path = evidence_files[0]
        cls.raw = cls.path.read_text(encoding="utf-8")
        cls.evidence = json.loads(cls.raw)
        cls.runs = {run["name"]: run for run in cls.evidence["runs"]}

    def test_summary_is_passed_and_has_the_required_sequence(self):
        self.assertEqual(
            self.evidence["evidence_kind"],
            "data_master_deterministic_multibatch_validation",
        )
        self.assertEqual(self.evidence["status"], "PASS")
        self.assertEqual(
            self.evidence["sequence"],
            ["batch-1", "batch-2", "batch-2-replay", "batch-3"],
        )
        self.assertEqual(set(self.runs), {
            "batch_1", "batch_2", "batch_2_replay", "batch_3"
        })

    def test_replay_reuses_the_batch_but_not_the_airflow_run(self):
        batch_2 = self.runs["batch_2"]
        replay = self.runs["batch_2_replay"]
        self.assertEqual(batch_2["batch_id"], replay["batch_id"])
        self.assertNotEqual(batch_2["run_id"], replay["run_id"])
        self.assertEqual(batch_2["manifest_sha256"], replay["manifest_sha256"])
        self.assertRegex(batch_2["manifest_sha256"], r"^[0-9a-f]{64}$")
        for layer, delta in self.evidence["assertions"][
            "replay_layer_deltas"
        ].items():
            self.assertEqual(delta, 0, layer)
            self.assertEqual(
                batch_2["aggregate_counts"][layer],
                replay["aggregate_counts"][layer],
                layer,
            )

    def test_controlled_customer_and_relationship_history_is_preserved(self):
        self.assertEqual(
            [run["history"]["changed_customer_versions"] for run in self.evidence["runs"]],
            [1, 2, 2, 2],
        )
        self.assertTrue(all(
            run["history"]["unchanged_customer_versions"] == 1
            for run in self.evidence["runs"]
        ))
        batch_2_history = self.runs["batch_2"]["history"]
        self.assertTrue(batch_2_history["new_customer_present"])
        self.assertTrue(batch_2_history["new_customer_account_present"])
        self.assertEqual(batch_2_history["new_customer_relationship_rows"], 1)
        self.assertEqual(
            batch_2_history["existing_customer_new_relationship_rows"], 1
        )

    def test_late_arrival_keeps_event_and_load_time_distinct(self):
        batch_2_generated = _timestamp(self.runs["batch_2"]["generated_at"])
        late = self.runs["batch_3"]["late_arrival"]
        event_time = _timestamp(late["event_timestamp"] + "+00:00")
        load_time = _timestamp(late["load_timestamp"] + "+00:00")
        self.assertTrue(late["present"])
        self.assertLess(event_time, batch_2_generated)
        self.assertLess(batch_2_generated, load_time)
        self.assertGreater(
            self.runs["batch_3"]["aggregate_counts"]["gold"],
            self.runs["batch_2_replay"]["aggregate_counts"]["gold"],
        )

    def test_provenance_is_immutable_and_payload_is_technical_only(self):
        commits = {item["sha"] for item in self.evidence["commits"]}
        self.assertTrue(commits)
        for image in self.evidence["images"]:
            match = re.search(r":git-([0-9a-f]{7,40})$", image["reference"])
            self.assertIsNotNone(match)
            self.assertIn(match.group(1), commits)
            self.assertRegex(image["image_id"], r"^sha256:[0-9a-f]{64}$")
        self.assertEqual(
            self.evidence["privacy"],
            {
                "classification": "technical_aggregate_only",
                "contains_pii": False,
                "contains_secrets": False,
                "contains_business_payload": False,
            },
        )
        self.assertNotRegex(self.raw, r"[A-Za-z]:\\")
        self.assertNotRegex(self.raw, r"(?i)password|bearer\s+|secret_key")


if __name__ == "__main__":
    unittest.main()
