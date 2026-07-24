"""Focused regression tests for DM-DV-002 Raw Vault lineage."""

import os
import sys
import unittest

from pyspark.sql import SparkSession

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "jobs", "raw_vault"))

from raw_vault_lineage import (
    add_raw_vault_record_source,
    lineage_projection,
    require_bronze_lineage_schema,
    scope_to_source_batch,
)


class RawVaultLineageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("dm-dv-002-tests")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _source_rows(self):
        return self.spark.createDataFrame(
            [
                ("c1", "Alice", "banking_sample", "clientes", "batch-1"),
                ("c2", "Bob", "banking_partner", "clientes_partner", "batch-2"),
            ],
            "cliente_id string, nome string, source_system string, "
            "source_entity string, batch_id string",
        )

    def test_two_sources_generate_specific_record_source(self):
        result = add_raw_vault_record_source(self._source_rows())
        values = {
            row.record_source
            for row in result.select("record_source").collect()
        }
        self.assertEqual(
            values,
            {"banking_sample:clientes", "banking_partner:clientes_partner"},
        )

    def test_batch_id_is_propagated_without_replacement(self):
        scoped = scope_to_source_batch(self._source_rows(), "batch-2")
        result = add_raw_vault_record_source(scoped).select(
            "cliente_id", "record_source", "batch_id"
        )
        self.assertEqual(
            result.collect()[0].asDict(),
            {
                "cliente_id": "c2",
                "record_source": "banking_partner:clientes_partner",
                "batch_id": "batch-2",
            },
        )

    def test_required_metadata_is_not_nullable(self):
        invalid = self.spark.createDataFrame(
            [("c1", None, "clientes", "batch-1")],
            "cliente_id string, source_system string, source_entity string, batch_id string",
        )
        with self.assertRaisesRegex(ValueError, "não podem ser nulos"):
            scope_to_source_batch(invalid, "batch-1")

    def test_missing_metadata_fails_schema_validation(self):
        incomplete = self.spark.createDataFrame(
            [("c1", "banking_sample", "batch-1")],
            "cliente_id string, source_system string, batch_id string",
        )
        with self.assertRaisesRegex(ValueError, "source_entity"):
            require_bronze_lineage_schema(incomplete)

    def test_unknown_batch_fails_instead_of_using_generic_lineage(self):
        with self.assertRaisesRegex(ValueError, "Batch Bronze não encontrado"):
            scope_to_source_batch(self._source_rows(), "batch-unknown")

    def test_hub_link_and_satellite_projections_are_compatible(self):
        source = self._source_rows()
        for business_columns in (
            ["cliente_id"],
            ["cliente_id"],
            ["cliente_id", "nome"],
        ):
            projected = source.select(*lineage_projection(business_columns))
            result = add_raw_vault_record_source(projected)
            self.assertTrue(
                {"record_source", "batch_id"}.issubset(result.columns)
            )

    def test_lineage_metadata_contains_no_payload_fields(self):
        self.assertEqual(
            set(lineage_projection([])),
            {"source_system", "source_entity", "batch_id"},
        )

    def test_reapplying_lineage_is_idempotent(self):
        once = add_raw_vault_record_source(self._source_rows())
        twice = add_raw_vault_record_source(once)
        self.assertEqual(once.count(), twice.count())
        self.assertEqual(
            once.select("record_source").collect(),
            twice.select("record_source").collect(),
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
