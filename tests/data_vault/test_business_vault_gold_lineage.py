"""Focused tests for DM-DV-003 Business Vault latest helpers and lineage."""

import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from pyspark.sql import SparkSession

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "jobs" / "business_vault"))
sys.path.insert(0, str(REPO_ROOT / "jobs" / "common"))

from raw_vault_views import latest_satellite_state, read_required_raw_table


class GoldMonetaryDeterminismContractTests(unittest.TestCase):
    def test_gold_monetary_aggregates_use_deterministic_decimals(self):
        source = (
            REPO_ROOT / "jobs" / "business_vault" / "load_gold.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn('.cast("double")', source)
        self.assertEqual(
            source.count('F.col("valor").cast(MONEY_DECIMAL_TYPE)'),
            3,
        )
        self.assertEqual(
            source.count('F.col("saldo").cast(MONEY_DECIMAL_TYPE)'),
            2,
        )
        self.assertIn(
            'F.col("limite").cast(MONEY_DECIMAL_TYPE)',
            source,
        )
        self.assertIn(".cast(MONEY_TOTAL_DECIMAL_TYPE)", source)


class BusinessVaultLatestTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("dm-dv-003-tests")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.base_time = datetime(2026, 7, 13, 12, 0, 0)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _history(self):
        return self.spark.createDataFrame(
            [
                ("p1", "A", "hd-a", self.base_time, "batch-1"),
                (
                    "p1",
                    "B",
                    "hd-b",
                    self.base_time + timedelta(minutes=1),
                    "batch-2",
                ),
                (
                    "p1",
                    "A",
                    "hd-a",
                    self.base_time + timedelta(minutes=2),
                    "batch-3",
                ),
                ("p2", "X", "hd-x", self.base_time, "batch-1"),
            ],
            "hk_parent string, state string, hd_state string, "
            "load_datetime timestamp, batch_id string",
        )

    def test_latest_helper_selects_final_a_in_a_b_a(self):
        latest = latest_satellite_state(self._history(), "hk_parent")
        p1 = latest.filter("hk_parent = 'p1'").collect()[0]
        self.assertEqual((p1.state, p1.batch_id), ("A", "batch-3"))

    def test_latest_helper_keeps_one_row_per_parent(self):
        latest = latest_satellite_state(self._history(), "hk_parent")
        self.assertEqual(latest.count(), 2)

    def test_latest_helper_requires_temporal_metadata(self):
        invalid = self.spark.createDataFrame(
            [("p1", "A")],
            "hk_parent string, state string",
        )
        with self.assertRaisesRegex(ValueError, "Satellite precisa"):
            latest_satellite_state(invalid, "hk_parent")

    def test_missing_raw_vault_fails_without_fallback(self):
        with patch("raw_vault_views.DeltaIO.read_delta", return_value=None):
            with self.assertRaisesRegex(FileNotFoundError, "Tabela Raw Vault"):
                read_required_raw_table(
                    self.spark,
                    "file:///missing/raw_vault",
                    "hub",
                    "hub_cliente",
                )

    def test_gold_loader_has_no_business_data_read_from_bronze(self):
        source = (
            REPO_ROOT / "jobs" / "business_vault" / "load_gold.py"
        ).read_text(encoding="utf-8")
        forbidden = ("_read_required_bronze", "bronze_path", "/bronze")
        self.assertEqual([value for value in forbidden if value in source], [])

    def test_gold_loader_declares_raw_lineage(self):
        source = (
            REPO_ROOT / "jobs" / "business_vault" / "load_gold.py"
        ).read_text(encoding="utf-8")
        self.assertIn("raw_vault->business_vault_latest->gold", source)
        self.assertIn("read_required_raw_table", source)
        self.assertIn("default=Config.GOLD_PATH", source)
        self.assertNotIn("default=Config.BUSINESS_VAULT_PATH", source)

    def test_dag_passes_raw_vault_path_to_gold(self):
        source = (
            REPO_ROOT / "dags" / "spark_application_factory.py"
        ).read_text(encoding="utf-8-sig")
        self.assertIn('"--raw-vault-path"', source)
        self.assertIn('paths["raw_vault"]', source)
        self.assertIn('"--gold-path"', source)
        self.assertIn('paths["gold"]', source)


if __name__ == "__main__":
    unittest.main(verbosity=2)
