import json
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "dags"))

from spark_application_factory import build_spark_application  # noqa: E402


PATHS = {
    "minio_endpoint": "http://minio.data-platform.svc.cluster.local:9000",
    "bronze": "s3a://lakehouse/bronze",
    "raw_vault": "s3a://lakehouse/raw_vault",
    "business_vault": "s3a://lakehouse/business_vault",
    "gold": "s3a://lakehouse/gold",
    "monitoring": "s3a://lakehouse/monitoring",
}


class SparkApplicationContractTests(unittest.TestCase):
    def _application(self, stage="bronze"):
        return build_spark_application(
            stage=stage,
            batch_id="batch-20260713",
            image="data-master-spark-jobs:git-abcdef0",
            runtime_profile="presentation-demo",
            namespace="data-platform",
            service_account="spark",
            paths=PATHS,
            application_name=f"dm-{stage}-20260713",
        )

    def test_driver_and_executor_are_separate_and_distributed(self):
        spec = self._application()["spec"]
        self.assertEqual(spec["mode"], "cluster")
        self.assertEqual(spec["driver"]["serviceAccount"], "spark")
        self.assertGreaterEqual(spec["executor"]["instances"], 1)
        self.assertEqual(spec["driver"]["memory"], "1g")
        self.assertEqual(spec["executor"]["memory"], "1536m")
        self.assertEqual(spec["executor"]["memoryOverhead"], "384m")
        self.assertEqual(
            spec["driver"]["labels"]["data-master.io/spark-role"],
            "driver",
        )
        self.assertEqual(
            spec["executor"]["labels"]["data-master.io/spark-role"],
            "executor",
        )

    def test_credentials_are_secret_references_not_values(self):
        application = self._application()
        serialized = json.dumps(application)
        self.assertNotIn("minio123", serialized)
        self.assertNotIn("minioadmin", serialized)
        driver_env = application["spec"]["driver"]["env"]
        access_key = next(
            item for item in driver_env if item["name"] == "AWS_ACCESS_KEY_ID"
        )
        self.assertEqual(
            access_key["valueFrom"]["secretKeyRef"]["name"],
            "data-master-minio-secret",
        )

    def test_runtime_dependencies_are_prebuilt_not_maven_packages(self):
        spec = self._application()["spec"]
        self.assertNotIn("spark.jars.packages", spec["sparkConf"])
        self.assertTrue(spec["mainApplicationFile"].startswith("local:///"))

    def test_business_vault_and_gold_paths_are_propagated_separately(self):
        spec = self._application("gold")["spec"]
        env = {
            item["name"]: item.get("value")
            for item in spec["driver"]["env"]
            if "value" in item
        }
        self.assertEqual(
            env["BUSINESS_VAULT_PATH"],
            "s3a://lakehouse/business_vault",
        )
        self.assertEqual(env["GOLD_PATH"], "s3a://lakehouse/gold")
        self.assertNotEqual(env["BUSINESS_VAULT_PATH"], env["GOLD_PATH"])

        arguments = spec["arguments"]
        gold_path_index = arguments.index("--gold-path") + 1
        self.assertEqual(arguments[gold_path_index], "s3a://lakehouse/gold")

    def test_all_pipeline_and_gate_stages_are_supported(self):
        for stage in (
            "bronze",
            "hubs",
            "links",
            "satellites",
            "gold",
            "data-vault-gate",
            "masking-gate",
            "evidence",
        ):
            self.assertEqual(self._application(stage)["kind"], "SparkApplication")

    def test_mutable_image_tags_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "Mutable"):
            build_spark_application(
                stage="integration",
                batch_id="batch",
                image="data-master-spark-jobs:latest",
                runtime_profile="minikube-integration",
                namespace="data-platform",
                service_account="spark",
                paths=PATHS,
                application_name="integration",
            )


if __name__ == "__main__":
    unittest.main()
