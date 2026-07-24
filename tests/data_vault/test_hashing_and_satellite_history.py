"""Focused regression tests for DM-DV-001."""

import hashlib
import os
import sys
import unittest
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "jobs", "common"))
sys.path.insert(0, os.path.join(REPO_ROOT, "jobs", "raw_vault"))

from hashing import BusinessKeyHasher, HashingUtils
from load_satellites import filter_new_satellite_records


class OrderedHashingTests(unittest.TestCase):
    def test_simple_key_remains_compatible(self):
        expected = "HK_" + hashlib.sha256(b"cliente_001").hexdigest().upper()
        self.assertEqual(
            HashingUtils.calculate_hash("cliente_001", prefix="hk_"),
            expected,
        )

    def test_composite_key_preserves_declared_order(self):
        self.assertNotEqual(
            HashingUtils.calculate_hash(["customer", "account"]),
            HashingUtils.calculate_hash(["account", "customer"]),
        )

    def test_link_roles_remain_distinguishable(self):
        self.assertNotEqual(
            BusinessKeyHasher.generate_link_hash_key(["hk_customer", "hk_account"]),
            BusinessKeyHasher.generate_link_hash_key(["hk_account", "hk_customer"]),
        )

    def test_commutative_link_is_explicit(self):
        self.assertEqual(
            BusinessKeyHasher.generate_link_hash_key(
                ["hk_customer", "hk_account"], commutative=True
            ),
            BusinessKeyHasher.generate_link_hash_key(
                ["hk_account", "hk_customer"], commutative=True
            ),
        )

    def test_null_positions_are_preserved(self):
        self.assertNotEqual(
            HashingUtils.calculate_hash([None, "A"]),
            HashingUtils.calculate_hash(["A", None]),
        )

    def test_casting_is_deterministic(self):
        self.assertEqual(
            HashingUtils.calculate_hash([1, 2]),
            HashingUtils.calculate_hash(["1", "2"]),
        )


class SatelliteHistoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("dm-dv-001-tests")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.base_time = datetime(2026, 7, 13, 12, 0, 0)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _rows(self, values):
        return self.spark.createDataFrame(
            [
                (
                    parent,
                    state,
                    self.base_time + timedelta(minutes=minute),
                    self.base_time + timedelta(minutes=minute),
                    batch,
                )
                for parent, state, minute, batch in values
            ],
            "hk_parent string, hd_state string, load_datetime timestamp, "
            "effective_from timestamp, batch_id string",
        )

    def _filter(self, incoming, existing):
        return filter_new_satellite_records(
            incoming,
            existing,
            "hk_parent",
            "hd_state",
        )

    def test_a_to_a_does_not_duplicate_consecutive_state(self):
        existing = self._rows([("p1", "A", 1, "batch-1")])
        incoming = self._rows([("p1", "A", 2, "batch-2")])
        self.assertEqual(self._filter(incoming, existing).count(), 0)

    def test_a_to_b_creates_two_historical_occurrences(self):
        existing = self._rows([("p1", "A", 1, "batch-1")])
        incoming = self._rows([("p1", "B", 2, "batch-2")])
        accepted = self._filter(incoming, existing)
        self.assertEqual(existing.unionByName(accepted).count(), 2)

    def test_a_to_b_to_a_creates_three_historical_occurrences(self):
        existing = self._rows(
            [
                ("p1", "A", 1, "batch-1"),
                ("p1", "B", 2, "batch-2"),
            ]
        )
        incoming = self._rows([("p1", "A", 3, "batch-3")])
        accepted = self._filter(incoming, existing)
        self.assertEqual(accepted.count(), 1)
        self.assertEqual(existing.unionByName(accepted).count(), 3)

    def test_same_batch_reexecution_is_idempotent(self):
        existing = self._rows(
            [
                ("p1", "A", 1, "batch-1"),
                ("p1", "B", 2, "batch-2"),
            ]
        )
        reprocessed = self._rows([("p1", "A", 3, "batch-1")])
        self.assertEqual(self._filter(reprocessed, existing).count(), 0)

    def test_parents_are_evaluated_independently(self):
        existing = self._rows(
            [
                ("p1", "A", 1, "batch-1"),
                ("p2", "A", 1, "batch-1"),
            ]
        )
        incoming = self._rows(
            [
                ("p1", "A", 2, "batch-2"),
                ("p2", "B", 2, "batch-2"),
            ]
        )
        accepted = self._filter(incoming, existing).collect()
        self.assertEqual([(row.hk_parent, row.hd_state) for row in accepted], [("p2", "B")])


if __name__ == "__main__":
    unittest.main(verbosity=2)
