import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "jobs" / "common"))

from runtime_profiles import (  # noqa: E402
    get_runtime_profile,
    list_runtime_profiles,
    validate_runtime_profile,
)


class RuntimeProfilesTests(unittest.TestCase):
    def test_expected_profiles_are_declared(self):
        self.assertEqual(
            list_runtime_profiles(),
            [
                "local-small",
                "minikube-integration",
                "minikube-horizontal-1",
                "minikube-horizontal-3",
                "presentation-demo",
                "local-medium",
                "cloud-ready",
            ],
        )

    def test_local_small_remains_the_fast_local_path(self):
        profile = get_runtime_profile("local-small")
        self.assertEqual(profile["execution"]["mode"], "local")
        self.assertEqual(profile["submission"]["mode"], "local")
        self.assertEqual(profile["spark"]["master"], "local[*]")

    def test_minikube_integration_uses_direct_spark_operator_submission(self):
        profile = get_runtime_profile("minikube-integration")
        self.assertEqual(profile["execution"]["mode"], "kubernetes")
        self.assertEqual(profile["execution"]["processing"], "spark-operator")
        self.assertEqual(profile["submission"]["mode"], "spark-operator-direct")
        self.assertTrue(profile["spark"]["master"].startswith("k8s://"))
        self.assertGreaterEqual(profile["spark"]["executor_instances"], 1)
        self.assertEqual(
            profile["kubernetes"]["credentials_secret"],
            "data-master-minio-secret",
        )

    def test_presentation_demo_has_no_effective_local_spark(self):
        profile = get_runtime_profile("presentation-demo")
        self.assertEqual(
            profile["execution"]["mode"],
            "airflow-spark-operator",
        )
        self.assertEqual(profile["execution"]["orchestrator"], "airflow")
        self.assertEqual(profile["execution"]["processing"], "spark-operator")
        self.assertEqual(profile["submission"]["mode"], "airflow")
        self.assertNotIn("local", profile["spark"]["master"])
        self.assertEqual(profile["spark"]["executor_memory"], "1536m")
        self.assertGreaterEqual(profile["spark"]["executor_instances"], 1)
        self.assertEqual(
            profile["kubernetes"]["gold_path"],
            "s3a://lakehouse/gold",
        )

    def test_minikube_profiles_use_the_physical_gold_root(self):
        for name in ("minikube-integration", "presentation-demo"):
            with self.subTest(profile=name):
                self.assertEqual(
                    get_runtime_profile(name)["kubernetes"]["gold_path"],
                    "s3a://lakehouse/gold",
                )

    def test_cloud_ready_remains_reference_only(self):
        profile = get_runtime_profile("cloud-ready")
        self.assertEqual(profile["submission"]["mode"], "reference-only")
        self.assertIn("reference", profile["execution"]["processing"])

    def test_kubernetes_profiles_require_complete_runtime_fields(self):
        profile = get_runtime_profile("minikube-integration")
        del profile["kubernetes"]["service_account"]
        with self.assertRaisesRegex(ValueError, "service_account"):
            validate_runtime_profile(profile)

    def test_unknown_profile_fails_clearly(self):
        with self.assertRaisesRegex(ValueError, "Available profiles"):
            get_runtime_profile("unknown-profile")


if __name__ == "__main__":
    unittest.main()
