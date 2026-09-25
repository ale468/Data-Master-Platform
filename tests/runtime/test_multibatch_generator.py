# Copyright (C) 2026 Alexandre Ferreira
# SPDX-License-Identifier: AGPL-3.0-only

import csv
import json
import tempfile
import unittest
from pathlib import Path

from jobs.data_generation.multibatch_lifecycle import (
    CHANGED_CUSTOMER_ID,
    LATE_TRANSACTION_ID,
    NEW_CUSTOMER_ACCOUNT_ID,
    NEW_CUSTOMER_ID,
    generate_lifecycle_batch,
    get_source_contract,
)


class DeterministicMultibatchGeneratorTests(unittest.TestCase):
    def _generate(self, root: Path, name: str, source_batch: str, batch_id: str):
        return generate_lifecycle_batch(
            str(root / name),
            source_batch,
            scenario_id="test-scenario",
            batch_id=batch_id,
            runtime_profile="local-small",
        )

    @staticmethod
    def _csv(path: str):
        with Path(path).open("r", encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))

    @staticmethod
    def _json(path: str):
        return json.loads(Path(path).read_text(encoding="utf-8"))

    def test_replay_is_byte_deterministic(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            first = self._generate(root, "first", "batch-2", "scenario-b2")
            replay = self._generate(root, "replay", "batch-2", "scenario-b2")
            self.assertEqual(first["manifest"], replay["manifest"])
            self.assertEqual(
                Path(first["manifest_path"]).read_bytes(),
                Path(replay["manifest_path"]).read_bytes(),
            )
            for source_name in first["files"]:
                self.assertEqual(
                    Path(first["files"][source_name]).read_bytes(),
                    Path(replay["files"][source_name]).read_bytes(),
                )

    def test_lifecycle_contains_controlled_changes_and_late_arrival(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            batch_1 = self._generate(root, "b1", "batch-1", "scenario-b1")
            batch_2 = self._generate(root, "b2", "batch-2", "scenario-b2")
            batch_3 = self._generate(root, "b3", "batch-3", "scenario-b3")

            b1_customers = {
                row["cliente_id"]: row
                for row in self._csv(batch_1["files"]["clientes"])
            }
            b2_customers = {
                row["cliente_id"]: row
                for row in self._csv(batch_2["files"]["clientes"])
            }
            self.assertNotEqual(
                b1_customers[CHANGED_CUSTOMER_ID]["email"],
                b2_customers[CHANGED_CUSTOMER_ID]["email"],
            )
            self.assertIn(NEW_CUSTOMER_ID, b2_customers)
            self.assertIn(
                NEW_CUSTOMER_ACCOUNT_ID,
                {
                    row["conta_id"]
                    for row in self._csv(batch_2["files"]["contas"])
                },
            )

            b2_transactions = {
                row["transacao_id"]
                for row in self._json(batch_2["files"]["transacoes"])
            }
            b3_transactions = {
                row["transacao_id"]: row
                for row in self._json(batch_3["files"]["transacoes"])
            }
            self.assertNotIn(LATE_TRANSACTION_ID, b2_transactions)
            self.assertIn(LATE_TRANSACTION_ID, b3_transactions)
            batch_2_generated = batch_2["manifest"]["generated_at"]
            self.assertLess(
                b3_transactions[LATE_TRANSACTION_ID]["data_transacao"],
                batch_2_generated.replace("T", " ").replace("Z", ""),
            )

    def test_unknown_batch_fails_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaisesRegex(ValueError, "Unknown source batch"):
                generate_lifecycle_batch(
                    temp,
                    "batch-unknown",
                    scenario_id="scenario",
                    batch_id="batch",
                    runtime_profile="local-small",
                )

    def test_manifest_and_sources_are_explicitly_synthetic(self):
        with tempfile.TemporaryDirectory() as temp:
            generated = generate_lifecycle_batch(
                temp,
                "batch-1",
                scenario_id="synthetic-only",
                batch_id="synthetic-only-b1",
                runtime_profile="local-small",
            )
            manifest = generated["manifest"]
            self.assertEqual(manifest["data_classification"], "synthetic")
            self.assertFalse(manifest["contains_real_personal_data"])
            for source_name in generated["files"]:
                self.assertEqual(
                    get_source_contract(source_name)["source_system"],
                    "banking_sample",
                )


if __name__ == "__main__":
    unittest.main()
