# Copyright (C) 2026 Alexandre Ferreira
# SPDX-License-Identifier: AGPL-3.0-only

"""Executable acceptance test for the deterministic multibatch lifecycle."""

import hashlib
import os
import shutil
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

os.environ.setdefault("RUNTIME_PROFILE", "local-small")
os.environ.setdefault("DM_RUNTIME_PROFILE", "local-small")
os.environ.setdefault("SPARK_MASTER", "local[1]")
os.environ.setdefault("SPARK_JARS_PACKAGES", "")

REPO_ROOT = Path(__file__).resolve().parents[2]
for relative in (
    "jobs/common",
    "jobs/data_generation",
    "jobs/bronze",
    "jobs/raw_vault",
    "jobs/business_vault",
):
    sys.path.insert(0, str(REPO_ROOT / relative))

from pyspark.sql import SparkSession  # noqa: E402

from config import Config  # noqa: E402
from delta_io import DeltaIO  # noqa: E402
from load_bronze import (  # noqa: E402
    BronzeLoader,
    register_batch_manifest,
    run_bronze_pipeline,
)
from load_gold import run_business_vault_pipeline  # noqa: E402
from load_hubs import run_hubs_pipeline  # noqa: E402
from load_links import run_links_pipeline  # noqa: E402
from load_satellites import run_satellites_pipeline  # noqa: E402
from multibatch_lifecycle import (  # noqa: E402
    CHANGED_CUSTOMER_ID,
    LATE_TRANSACTION_ID,
    NEW_CUSTOMER_ACCOUNT_ID,
    NEW_CUSTOMER_ID,
    UNCHANGED_CUSTOMER_ID,
    generate_lifecycle_batch,
)


class MultibatchLifecycleAcceptanceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("data-master-multibatch-acceptance")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.databricks.delta.snapshotPartitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self._configure_paths((self.root / "lakehouse").as_uri())

    def tearDown(self):
        self.temp.cleanup()

    @staticmethod
    def _configure_paths(root: str):
        Config.BRONZE_PATH = f"{root}/bronze"
        Config.RAW_VAULT_PATH = f"{root}/raw_vault"
        Config.BUSINESS_VAULT_PATH = f"{root}/business_vault"
        Config.GOLD_PATH = f"{root}/gold"
        Config.MONITORING_PATH = f"{root}/monitoring"
        Config.MONITORING_TABLE = f"{Config.MONITORING_PATH}/pipeline_execution_log"
        Config.BRONZE_BATCH_MANIFEST_PATH = (
            f"{Config.BRONZE_PATH}/_control/batch_manifest"
        )
        for name in Config.BRONZE_TABLES:
            Config.BRONZE_TABLES[name]["path"] = f"{Config.BRONZE_PATH}/{name}"
        for group_name, tables, folder in (
            ("hub", Config.HUB_TABLES, "hubs"),
            ("link", Config.LINK_TABLES, "links"),
            ("satellite", Config.SATELLITE_TABLES, "satellites"),
        ):
            del group_name
            for name in tables:
                tables[name]["path"] = f"{Config.RAW_VAULT_PATH}/{folder}/{name}"
        for name in Config.GOLD_TABLES:
            Config.GOLD_TABLES[name] = f"{Config.GOLD_PATH}/{name}"

    def _execute(self, source_batch: str, batch_id: str, run_id: str):
        output = self.root / run_id
        generated = generate_lifecycle_batch(
            str(output),
            source_batch,
            scenario_id="acceptance",
            batch_id=batch_id,
            runtime_profile="local-small",
        )
        shutil.rmtree(output)
        bronze = run_bronze_pipeline(
            self.spark,
            str(output),
            Config.BRONZE_PATH,
            batch_id,
            run_id,
            batch_manifest=generated["manifest"],
            source_records=generated["records"],
        )
        hubs = run_hubs_pipeline(
            self.spark, Config.BRONZE_PATH, batch_id, run_id
        )
        links = run_links_pipeline(
            self.spark, Config.BRONZE_PATH, batch_id, run_id
        )
        satellites = run_satellites_pipeline(
            self.spark, Config.BRONZE_PATH, batch_id, run_id
        )
        gold = run_business_vault_pipeline(
            self.spark,
            Config.RAW_VAULT_PATH,
            Config.GOLD_PATH,
            batch_id,
            run_id,
        )
        for result in (bronze, hubs, links, satellites, gold):
            self.assertEqual(result["status"], "SUCCESS", result)
        return {
            "generated": generated,
            "bronze": bronze,
            "hubs": hubs,
            "links": links,
            "satellites": satellites,
            "gold": gold,
        }

    @staticmethod
    def _written(result):
        return sum(item["rows_written"] for item in result["results"].values())

    def _customer_history(self, customer_id: str):
        hub = DeltaIO.read_delta(
            self.spark, Config.HUB_TABLES["hub_cliente"]["path"]
        )
        hash_key = (
            hub.filter(hub["cliente_id"] == customer_id)
            .select("hk_cliente")
            .collect()[0]["hk_cliente"]
        )
        satellite = DeltaIO.read_delta(
            self.spark,
            Config.SATELLITE_TABLES["sat_cliente_dados_cadastrais"]["path"],
        )
        return satellite.filter(satellite["hk_cliente"] == hash_key)

    def test_bronze_collapses_identical_duplicates_and_rejects_conflicts(self):
        table_path = (self.root / "duplicate-contract").as_uri()
        identical = self.spark.createDataFrame(
            [
                ("batch", "CLI_1", "same"),
                ("batch", "CLI_1", "same"),
            ],
            ["batch_id", "cliente_id", "nome"],
        )
        filtered = BronzeLoader.filter_new_batch_records(
            self.spark,
            identical,
            table_path,
            ["cliente_id"],
        )
        self.assertEqual(filtered.count(), 1)

        conflicting = self.spark.createDataFrame(
            [
                ("batch", "CLI_1", "first"),
                ("batch", "CLI_1", "second"),
            ],
            ["batch_id", "cliente_id", "nome"],
        )
        with self.assertRaisesRegex(ValueError, "Conflicting duplicate"):
            BronzeLoader.filter_new_batch_records(
                self.spark,
                conflicting,
                table_path,
                ["cliente_id"],
            )

    def test_three_batches_and_replay_preserve_history(self):
        batch_1 = self._execute("batch-1", "acceptance-b1", "acceptance-b1")
        self.assertEqual(self._customer_history(CHANGED_CUSTOMER_ID).count(), 1)
        self.assertEqual(self._customer_history(UNCHANGED_CUSTOMER_ID).count(), 1)

        batch_2 = self._execute("batch-2", "acceptance-b2", "acceptance-b2")
        self.assertEqual(self._customer_history(CHANGED_CUSTOMER_ID).count(), 2)
        self.assertEqual(self._customer_history(UNCHANGED_CUSTOMER_ID).count(), 1)
        self.assertEqual(self._customer_history(NEW_CUSTOMER_ID).count(), 1)

        replay = self._execute(
            "batch-2", "acceptance-b2", "acceptance-b2-replay"
        )
        self.assertEqual(replay["bronze"]["total_rows"], 0)
        self.assertEqual(self._written(replay["hubs"]), 0)
        self.assertEqual(self._written(replay["links"]), 0)
        self.assertEqual(self._written(replay["satellites"]), 0)
        self.assertEqual(
            batch_2["generated"]["manifest"], replay["generated"]["manifest"]
        )
        self.assertEqual(self._customer_history(CHANGED_CUSTOMER_ID).count(), 2)

        batch_3 = self._execute("batch-3", "acceptance-b3", "acceptance-b3")
        self.assertEqual(self._customer_history(CHANGED_CUSTOMER_ID).count(), 2)
        self.assertEqual(self._customer_history(UNCHANGED_CUSTOMER_ID).count(), 1)

        transaction_hub = DeltaIO.read_delta(
            self.spark, Config.HUB_TABLES["hub_transacao"]["path"]
        )
        late_hash = (
            transaction_hub
            .filter(transaction_hub["transacao_id"] == LATE_TRANSACTION_ID)
            .select("hk_transacao")
            .collect()[0]["hk_transacao"]
        )
        transaction_satellite = DeltaIO.read_delta(
            self.spark,
            Config.SATELLITE_TABLES["sat_transacao_detalhes"]["path"],
        )
        late = (
            transaction_satellite
            .filter(transaction_satellite["hk_transacao"] == late_hash)
            .collect()[0]
        )
        self.assertEqual(late.batch_id, "acceptance-b3")
        self.assertEqual(late.run_id, "acceptance-b3")
        self.assertLess(
            datetime.fromisoformat(str(late.data_transacao)),
            datetime.fromisoformat(
                batch_2["generated"]["manifest"]["generated_at"].replace("Z", "+00:00")
            ).replace(tzinfo=None),
        )

        account_hub = DeltaIO.read_delta(
            self.spark, Config.HUB_TABLES["hub_conta"]["path"]
        )
        self.assertEqual(
            account_hub.filter(
                account_hub["conta_id"] == NEW_CUSTOMER_ACCOUNT_ID
            ).count(),
            1,
        )

        pseudonym = "CLI_" + hashlib.sha256(
            CHANGED_CUSTOMER_ID.encode("utf-8")
        ).hexdigest()[:8].upper()
        protected = DeltaIO.read_delta(
            self.spark, Config.GOLD_TABLES["gold_clientes_protegidos"]
        )
        changed_gold = protected.filter(
            protected["cliente_id_pseudonimizado"] == pseudonym
        ).collect()[0]
        self.assertEqual(changed_gold.email_mascarado, "c***@example.invalid")
        self.assertEqual(changed_gold.batch_id, "acceptance-b3")
        self.assertEqual(changed_gold.run_id, "acceptance-b3")

        conflicting = dict(batch_2["generated"]["manifest"])
        conflicting["generator_version"] = "conflict"
        with self.assertRaisesRegex(ValueError, "manifest conflict"):
            register_batch_manifest(
                self.spark, conflicting, "acceptance-conflicting-replay"
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
