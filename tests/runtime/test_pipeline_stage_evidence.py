import io
import sys
import types
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from jobs.kubernetes import run_pipeline_stage  # noqa: E402


class _Frame:
    def __init__(self, rows):
        self.rows = rows

    def count(self):
        return self.rows


class PipelineStageEvidenceTests(unittest.TestCase):
    def _count(self, configs, rows_by_path):
        module = types.ModuleType("delta_io")

        class _DeltaIO:
            @staticmethod
            def read_delta(_spark, path):
                rows = rows_by_path.get(path)
                return None if rows is None else _Frame(rows)

        module.DeltaIO = _DeltaIO
        with patch.dict(sys.modules, {"delta_io": module}):
            return run_pipeline_stage._count_tables(object(), configs)

    def test_mapping_with_path_and_metadata_is_supported(self):
        configs = {
            "gold_a": {
                "path": "s3a://lakehouse/gold/a",
                "format": "delta",
                "owner": "synthetic-demo",
            }
        }
        self.assertEqual(self._count(configs, {configs["gold_a"]["path"]: 3}), 3)

    def test_direct_string_and_pathlike_configs_are_supported(self):
        configs = {
            "gold_a": "s3a://lakehouse/gold/a",
            "gold_b": Path("/tmp/gold-b"),
        }
        self.assertEqual(
            self._count(
                configs,
                {
                    "s3a://lakehouse/gold/a": 4,
                    str(Path("/tmp/gold-b")): 6,
                },
            ),
            10,
        )

    def test_current_gold_config_and_multiple_table_counts_are_supported(self):
        from config import Config

        rows_by_path = {
            path: index
            for index, path in enumerate(Config.GOLD_TABLES.values(), start=1)
        }
        self.assertGreater(len(rows_by_path), 1)
        self.assertEqual(
            self._count(Config.GOLD_TABLES, rows_by_path),
            sum(rows_by_path.values()),
        )
        self.assertNotEqual(Config.BUSINESS_VAULT_PATH, Config.GOLD_PATH)
        self.assertEqual(len(Config.GOLD_TABLES), 7)
        for table_name, table_path in Config.GOLD_TABLES.items():
            self.assertEqual(table_path, f"{Config.GOLD_PATH}/{table_name}")
            self.assertFalse(
                table_path.startswith(Config.BUSINESS_VAULT_PATH.rstrip("/") + "/")
            )

    def test_invalid_or_unreadable_table_fails_without_config_values(self):
        sensitive_value = "synthetic-personal-value-must-not-leak"
        invalid = {"gold_invalid": {"owner": sensitive_value}}

        with self.assertRaises(ValueError) as invalid_error:
            self._count(invalid, {})
        self.assertIn("gold_invalid", str(invalid_error.exception))
        self.assertNotIn(sensitive_value, str(invalid_error.exception))

        with self.assertRaisesRegex(RuntimeError, "gold_missing"):
            self._count(
                {"gold_missing": "s3a://lakehouse/gold/missing"},
                {},
            )

        for registry in ({}, [], None):
            with self.subTest(registry=registry):
                with self.assertRaisesRegex(ValueError, "non-empty mapping"):
                    self._count(registry, {})

    def test_evidence_output_contains_counts_and_no_personal_values(self):
        output = io.StringIO()
        personal_values = (
            "123.456.789-00",
            "Synthetic Person",
            "person@example.invalid",
        )
        with patch.object(
            run_pipeline_stage,
            "_count_tables",
            side_effect=[7, 6, 7, 8, 5],
        ):
            with redirect_stdout(output):
                result = run_pipeline_stage._run_evidence(
                    object(),
                    "synthetic-batch",
                )

        rendered = output.getvalue()
        self.assertEqual(result["counts"]["gold"], 5)
        self.assertEqual(
            result["storage"]["business_vault_path"],
            "s3a://lakehouse/business_vault",
        )
        self.assertEqual(result["storage"]["gold_path"], "s3a://lakehouse/gold")
        self.assertEqual(len(result["storage"]["gold_tables"]), 7)
        self.assertIn("PRESENTATION_EVIDENCE_STATUS=PASS", rendered)
        for value in personal_values:
            self.assertNotIn(value, rendered)


if __name__ == "__main__":
    unittest.main()
