"""Failure-mode and success tests for DM-DV-004."""

import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "jobs" / "raw_vault"))
sys.path.insert(0, str(REPO_ROOT / "jobs" / "common"))

from data_vault_quality_gate import (
    evaluate_data_vault_gate,
    gate_exit_code,
    render_gate_output,
)
from load_links import _add_hub_hash


class ExecutableDataVaultGateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("dm-dv-004-tests")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.base_time = datetime(2026, 7, 13, 12, 0, 0)
        cls.hub_specs = {
            "hub_customer": {
                "hash_key": "hk_customer",
                "business_keys": ["customer_id"],
            },
            "hub_account": {
                "hash_key": "hk_account",
                "business_keys": ["account_id"],
            },
        }
        cls.link_specs = {
            "link_customer_account": {
                "hk_customer": "hub_customer",
                "hk_account": "hub_account",
            }
        }
        cls.satellite_specs = {
            "sat_customer": {
                "parent_key": "hk_customer",
                "hashdiff": "hd_customer",
                "hub": "hub_customer",
                "pii": True,
            }
        }

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _hubs(self):
        schema = (
            "hash_key string, business_key string, load_datetime timestamp, "
            "record_source string, batch_id string"
        )
        customer = self.spark.createDataFrame(
            [("hc1", "c1", self.base_time, "core:customers", "batch-1")],
            schema,
        ).selectExpr(
            "hash_key as hk_customer",
            "business_key as customer_id",
            "load_datetime",
            "record_source",
            "batch_id",
        )
        account = self.spark.createDataFrame(
            [("ha1", "a1", self.base_time, "core:accounts", "batch-1")],
            schema,
        ).selectExpr(
            "hash_key as hk_account",
            "business_key as account_id",
            "load_datetime",
            "record_source",
            "batch_id",
        )
        return {"hub_customer": customer, "hub_account": account}

    def _links(self, account_hash="ha1", record_source="core:accounts"):
        return {
            "link_customer_account": self.spark.createDataFrame(
                [
                    (
                        "hl1",
                        "hc1",
                        account_hash,
                        self.base_time,
                        record_source,
                        "batch-1",
                    )
                ],
                "hk_link string, hk_customer string, hk_account string, "
                "load_datetime timestamp, record_source string, batch_id string",
            )
        }

    def _satellite(self, rows=None):
        rows = rows or [
            (
                "hc1",
                "hd-a",
                "masked input",
                self.base_time,
                "core:customers",
                self.base_time,
                "batch-1",
            )
        ]
        return self.spark.createDataFrame(
            rows,
            "hk_customer string, hd_customer string, state string, "
            "load_datetime timestamp, record_source string, "
            "effective_from timestamp, batch_id string",
        )

    def _gold(self, direct_pii=False):
        if direct_pii:
            return {
                "gold_customer": self.spark.createDataFrame(
                    [("123.456.789-10",)], "cpf string"
                )
            }
        return {
            "gold_customer": self.spark.createDataFrame(
                [("CLI_ABCD1234",)], "customer_pseudonym string"
            )
        }

    def _evaluate(
        self,
        hubs=None,
        links=None,
        satellite=None,
        gold=None,
        gold_source="raw_vault->business_vault_latest->gold",
        business_vault_path="s3a://lakehouse/business_vault",
        gold_path="s3a://lakehouse/gold",
        gold_table_paths=None,
    ):
        if gold_table_paths is None:
            gold_table_paths = {
                "gold_customer": "s3a://lakehouse/gold/gold_customer"
            }
        return evaluate_data_vault_gate(
            hubs or self._hubs(),
            links or self._links(),
            {
                "sat_customer": (
                    self._satellite() if satellite is None else satellite
                )
            },
            gold or self._gold(),
            {"sat_customer"},
            gold_source,
            hub_specs=self.hub_specs,
            link_specs=self.link_specs,
            satellite_specs=self.satellite_specs,
            expected_gold_tables=("gold_customer",),
            business_vault_path=business_vault_path,
            gold_path=gold_path,
            gold_table_paths=gold_table_paths,
        )

    def test_valid_dataset_passes(self):
        self.assertEqual(self._evaluate()["status"], "PASS")

    def test_duplicate_hub_fails(self):
        hubs = self._hubs()
        hubs["hub_customer"] = hubs["hub_customer"].unionByName(
            hubs["hub_customer"]
        )
        result = self._evaluate(hubs=hubs)
        self.assertIn("hub.hub_customer.duplicate_hash_key", result["failed_checks"])

    def test_hub_without_business_key_fails(self):
        hubs = self._hubs()
        hubs["hub_customer"] = hubs["hub_customer"].drop("customer_id")
        result = self._evaluate(hubs=hubs)
        self.assertTrue(
            any("business_key" in failure or "customer_id" in failure for failure in result["failed_checks"])
        )

    def test_orphan_link_fails(self):
        result = self._evaluate(links=self._links(account_hash="missing"))
        self.assertTrue(
            any(
                failure.startswith("link.link_customer_account.orphan:hk_account")
                for failure in result["failed_checks"]
            )
        )

    def test_link_hashing_rejects_null_business_key(self):
        source = self.spark.createDataFrame(
            [(None,), ("CARD_1",)], "cartao_id string"
        )
        result = _add_hub_hash(
            self.spark, source, "cartao_id", "hk_cartao"
        )
        self.assertEqual(result.count(), 1)
        self.assertEqual(result.collect()[0].cartao_id, "CARD_1")

    def test_satellite_without_parent_fails(self):
        rows = [
            (
                "missing",
                "hd-a",
                "state",
                self.base_time,
                "core:customers",
                self.base_time,
                "batch-1",
            )
        ]
        result = self._evaluate(satellite=self._satellite(rows))
        self.assertIn("satellite.sat_customer.orphan_parent", result["failed_checks"])

    def test_consecutive_duplicate_satellite_fails(self):
        rows = [
            (
                "hc1",
                "hd-a",
                "A",
                self.base_time,
                "core:customers",
                self.base_time,
                "batch-1",
            ),
            (
                "hc1",
                "hd-a",
                "A",
                self.base_time + timedelta(minutes=1),
                "core:customers",
                self.base_time + timedelta(minutes=1),
                "batch-2",
            ),
        ]
        result = self._evaluate(satellite=self._satellite(rows))
        self.assertIn(
            "satellite.sat_customer.consecutive_duplicate",
            result["failed_checks"],
        )

    def test_legitimate_a_b_a_recurrence_passes(self):
        rows = [
            (
                "hc1",
                state,
                state,
                self.base_time + timedelta(minutes=minute),
                "core:customers",
                self.base_time + timedelta(minutes=minute),
                f"batch-{minute}",
            )
            for state, minute in (("A", 1), ("B", 2), ("A", 3))
        ]
        result = self._evaluate(satellite=self._satellite(rows))
        self.assertEqual(result["statuses"]["satellites"], "PASS")

    def test_invalid_record_source_fails(self):
        result = self._evaluate(links=self._links(record_source="generic"))
        self.assertIn(
            "lineage.link.link_customer_account.invalid_metadata",
            result["failed_checks"],
        )

    def test_gold_reading_bronze_fails(self):
        result = self._evaluate(
            gold_source="bronze_path /bronze _read_required_bronze"
        )
        self.assertIn("gold.lineage.reads_bronze", result["failed_checks"])

    def test_gold_with_direct_pii_fails(self):
        result = self._evaluate(gold=self._gold(direct_pii=True))
        self.assertTrue(
            any("direct_pii:cpf" in failure for failure in result["failed_checks"])
        )

    def test_gold_storage_path_must_match_the_gold_root(self):
        result = self._evaluate(
            gold_table_paths={
                "gold_customer": "s3a://lakehouse/business_vault/gold_customer"
            }
        )
        self.assertEqual(result["statuses"]["gold_storage"], "FAILED")
        self.assertIn(
            "gold.storage.invalid_path:gold_customer",
            result["failed_checks"],
        )

    def test_business_vault_and_gold_roots_must_be_distinct(self):
        result = self._evaluate(
            business_vault_path="s3a://lakehouse/gold",
        )
        self.assertEqual(result["statuses"]["gold_path_separation"], "FAILED")
        self.assertIn(
            "gold.storage.business_vault_gold_same_root",
            result["failed_checks"],
        )

    def test_output_and_exit_code(self):
        passed = self._evaluate()
        self.assertEqual(gate_exit_code(passed), 0)
        output = render_gate_output(passed)
        self.assertIn("DATA_VAULT_QUALITY_GATE_STATUS=PASS", output)
        self.assertIn("GOLD_STORAGE_PATH_STATUS=PASS", output)
        self.assertIn(
            "BUSINESS_VAULT_GOLD_PATH_SEPARATION_STATUS=PASS",
            output,
        )
        failed = self._evaluate(gold=self._gold(direct_pii=True))
        self.assertEqual(gate_exit_code(failed), 1)
        output = render_gate_output(failed)
        self.assertIn("DATA_VAULT_QUALITY_GATE_STATUS=FAILED", output)
        self.assertIn("FAILED_CHECKS=", output)


if __name__ == "__main__":
    unittest.main(verbosity=2)
