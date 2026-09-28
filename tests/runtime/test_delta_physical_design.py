# Copyright (C) 2026 Alexandre Ferreira
# SPDX-License-Identifier: AGPL-3.0-only

import importlib.util
import sys
import unittest
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
COMMON = REPO_ROOT / "jobs" / "common"
sys.path.insert(0, str(COMMON))

from config import PipelineConfig  # noqa: E402


POLICY_PATH = REPO_ROOT / "config" / "delta" / "physical-design-policy.yml"
MODULE_PATH = REPO_ROOT / "scripts" / "delta_layout_health.py"
SPEC = importlib.util.spec_from_file_location("delta_layout_health", MODULE_PATH)
LAYOUT = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(LAYOUT)


class DeltaPhysicalDesignPolicyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.policy = yaml.safe_load(POLICY_PATH.read_text(encoding="utf-8"))
        cls.tables = cls.policy["physical_tables"]

    def test_every_configured_physical_table_is_covered_exactly_once(self):
        expected = {f"bronze.{name}" for name in PipelineConfig.BRONZE_TABLES}
        expected |= {f"raw_vault.{name}" for name in PipelineConfig.HUB_TABLES}
        expected |= {f"raw_vault.{name}" for name in PipelineConfig.LINK_TABLES}
        expected |= {f"raw_vault.{name}" for name in PipelineConfig.SATELLITE_TABLES}
        expected |= {f"gold.{name}" for name in PipelineConfig.GOLD_TABLES}
        actual = [table["id"] for table in self.tables]
        self.assertEqual(len(actual), len(set(actual)), "duplicate physical table policy entry")
        self.assertEqual(set(actual), expected)

    def test_current_layout_is_unpartitioned_and_actions_are_not_authorized(self):
        self.assertEqual(self.policy["current_decision"], "KEEP_UNPARTITIONED")
        self.assertTrue(all(table["partition_columns"] == [] for table in self.tables))
        self.assertFalse(self.policy["boundaries"]["automatic_maintenance_authorized"])
        self.assertFalse(self.policy["boundaries"]["destructive_vacuum_authorized"])
        self.assertFalse(self.policy["maintenance"]["compaction"]["authorized"])
        self.assertFalse(self.policy["maintenance"]["vacuum"]["authorized"])
        self.assertGreaterEqual(self.policy["maintenance"]["vacuum"]["minimum_retention_hours"], 168)

    def test_write_modes_match_pipeline_semantics(self):
        modes = {table["id"]: table["write_mode"] for table in self.tables}
        for name in PipelineConfig.BRONZE_TABLES:
            self.assertEqual(modes[f"bronze.{name}"], "INSERT_ONLY")
        for name in (*PipelineConfig.HUB_TABLES, *PipelineConfig.LINK_TABLES):
            self.assertEqual(modes[f"raw_vault.{name}"], "INSERT_ONLY")
        for name in PipelineConfig.SATELLITE_TABLES:
            self.assertEqual(modes[f"raw_vault.{name}"], "APPEND_HISTORIZED")
        for name in PipelineConfig.GOLD_TABLES:
            self.assertEqual(modes[f"gold.{name}"], "REBUILD_OVERWRITE")

    def test_business_vault_views_are_explicitly_non_materialized(self):
        views = self.policy["logical_business_vault_views"]
        self.assertFalse(views["materialized"])
        self.assertEqual(
            set(views["names"]),
            {"customers", "accounts", "cards", "transactions", "agencies"},
        )
        physical_names = {table["name"] for table in self.tables}
        self.assertTrue(set(views["names"]).isdisjoint(physical_names))

    def test_decision_state_transitions_are_fail_closed(self):
        states = self.policy["decision_states"]
        self.assertEqual(set(states), LAYOUT.DECISION_STATES)
        self.assertNotIn("ADOPT", states["KEEP_UNPARTITIONED"]["next"])
        self.assertNotIn("ADOPT", states["OBSERVE"]["next"])
        self.assertEqual(states["ADOPT"]["next"], ["OBSERVE"])

    def test_runtime_parallelism_is_not_declared_as_table_partitioning(self):
        boundaries = self.policy["boundaries"]
        self.assertIn("shuffle", boundaries["execution_partitioning"].lower())
        self.assertIn("directory", boundaries["table_partitioning"].lower())
        self.assertIn("discovery", boundaries["catalog_metadata"].lower())


class DeltaLayoutHealthTests(unittest.TestCase):
    def test_file_health_uses_explicit_byte_units_and_nearest_rank(self):
        metrics = LAYOUT.summarize_file_health(
            [1, 2, 3, 100],
            small_file_threshold_bytes=4,
            physical_partition_count=2,
            delta_version=7,
        )
        self.assertEqual(metrics["active_file_count"], 4)
        self.assertEqual(metrics["active_bytes"], 106)
        self.assertEqual(metrics["median_file_bytes"], 2)
        self.assertEqual(metrics["p95_file_bytes"], 100)
        self.assertEqual(metrics["small_file_ratio"], 0.75)
        self.assertEqual(metrics["physical_partition_count"], 2)
        self.assertEqual(metrics["delta_version"], 7)

    def test_incomplete_or_unsafe_evidence_never_recommends_adoption(self):
        self.assertEqual(LAYOUT.evaluate_layout_evidence({})["outcome"], "INCONCLUSIVE")
        incomplete = {
            "representative_query_families": ["daily_transaction_filter"],
            "repeated_runs_per_variant": 2,
            "functional_fingerprint_equivalent": True,
            "file_health_complete": True,
            "performance_comparison_complete": True,
            "write_overhead_measured": True,
        }
        self.assertEqual(LAYOUT.evaluate_layout_evidence(incomplete)["outcome"], "INCONCLUSIVE")
        incomplete["repeated_runs_per_variant"] = 3
        incomplete["functional_fingerprint_equivalent"] = False
        self.assertEqual(LAYOUT.evaluate_layout_evidence(incomplete)["outcome"], "NO_CHANGE")

    def test_complete_evidence_is_only_an_experiment_candidate(self):
        evidence = {
            "representative_query_families": ["daily_transaction_filter"],
            "repeated_runs_per_variant": 3,
            "functional_fingerprint_equivalent": True,
            "file_health_complete": True,
            "performance_comparison_complete": True,
            "write_overhead_measured": True,
        }
        self.assertEqual(
            LAYOUT.evaluate_layout_evidence(evidence)["outcome"],
            "EXPERIMENT_CANDIDATE",
        )

    def test_publication_document_contains_only_aggregate_contract_fields(self):
        metrics = LAYOUT.summarize_file_health(
            [1024, 2048],
            small_file_threshold_bytes=4096,
            delta_version=1,
        )
        document = LAYOUT.build_evidence_document(
            revision="a" * 40,
            runtime_profile="presentation-demo",
            policy_id="data-master-delta-physical-design-v1",
            tables=[{
                "table_id": "bronze.transacoes",
                "disposition": "OBSERVE",
                "metrics": metrics,
            }],
            captured_at="2026-09-28T00:00:00Z",
        )
        self.assertEqual(document["decision"]["outcome"], "INCONCLUSIVE")
        self.assertEqual(document["privacy"]["classification"], "technical_aggregate_only")
        serialized = str(document).lower()
        for forbidden in ("customer", "cliente_id", "cpf", "access_key", "s3a://"):
            self.assertNotIn(forbidden, serialized)

    def test_negative_sizes_and_extra_payload_fields_fail_closed(self):
        with self.assertRaises(ValueError):
            LAYOUT.summarize_file_health([-1], small_file_threshold_bytes=1)
        metrics = LAYOUT.summarize_file_health([], small_file_threshold_bytes=1)
        with self.assertRaisesRegex(ValueError, "accepts only"):
            LAYOUT.build_evidence_document(
                revision="b" * 40,
                runtime_profile="presentation-demo",
                policy_id="policy",
                tables=[{
                    "table_id": "bronze.transacoes",
                    "disposition": "OBSERVE",
                    "metrics": metrics,
                    "path": "s3a://not-public",
                }],
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
