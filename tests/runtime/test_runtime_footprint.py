# Copyright (C) 2026 Alexandre Ferreira
# SPDX-License-Identifier: AGPL-3.0-only

import importlib.util
import json
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "scripts" / "minikube" / "runtime_footprint.py"
SPEC = importlib.util.spec_from_file_location("runtime_footprint", MODULE_PATH)
RUNTIME_FOOTPRINT = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(RUNTIME_FOOTPRINT)


class RuntimeFootprintUnitTests(unittest.TestCase):
    def test_profile_contract_accepts_iso_timestamp_markers(self):
        self.assertIsNotNone(
            RUNTIME_FOOTPRINT.PROFILE_NAME_PATTERN.fullmatch("data-master-hive-baseline-20260927T182500Z")
        )
        self.assertIsNone(RUNTIME_FOOTPRINT.PROFILE_NAME_PATTERN.fullmatch("minikube"))

    def test_kubernetes_quantities_are_normalized(self):
        self.assertEqual(RUNTIME_FOOTPRINT.cpu_millicores("250m"), 250)
        self.assertEqual(RUNTIME_FOOTPRINT.cpu_millicores("2"), 2000)
        self.assertEqual(RUNTIME_FOOTPRINT.cpu_millicores("1500000n"), 2)
        self.assertEqual(RUNTIME_FOOTPRINT.memory_bytes("512Mi"), 512 * 1024**2)
        self.assertEqual(RUNTIME_FOOTPRINT.memory_bytes("5Gi"), 5 * 1024**3)

    def test_pod_resources_and_metrics_are_aggregated_without_payloads(self):
        pods = {
            "items": [
                {
                    "spec": {
                        "containers": [
                            {
                                "resources": {
                                    "requests": {"cpu": "100m", "memory": "256Mi"},
                                    "limits": {"cpu": "200m", "memory": "512Mi"},
                                }
                            },
                            {"resources": {}},
                        ]
                    }
                }
            ]
        }
        totals = RUNTIME_FOOTPRINT.summarize_pod_resources(pods)
        self.assertEqual(totals["cpu_requests_millicores"], 100)
        self.assertEqual(totals["memory_limits_bytes"], 512 * 1024**2)

        metrics = {
            "items": [
                {
                    "containers": [
                        {"usage": {"cpu": "12m", "memory": "100Mi"}},
                        {"usage": {"cpu": "8m", "memory": "28Mi"}},
                    ]
                }
            ]
        }
        observed = RUNTIME_FOOTPRINT.summarize_metrics(metrics)
        self.assertEqual(observed["cpu_millicores"], 20)
        self.assertEqual(observed["memory_bytes"], 128 * 1024**2)

    def _snapshot(self, variant, cpu, memory, workloads, images):
        return {
            "schema_version": 1,
            "evidence_kind": "data_master_runtime_footprint",
            "captured_at": "2026-09-27T00:00:00Z",
            "variant": variant,
            "phase": "ready-idle",
            "source": {
                "collector_revision": "a" * 40,
                "collector_branch": "test",
                "deployed_revision": ("a" if variant == "baseline" else "b") * 40,
            },
            "environment": {
                "profile": f"data-master-{variant}",
                "namespace": "data-platform",
                "node_capacity": {
                    "node_count": 1,
                    "cpu_millicores": 4000,
                    "memory_bytes": 8 * 1024**3,
                },
                "sample_interval_seconds": 10,
            },
            "object_counts": {
                "argocd_applications": 8 if variant == "baseline" else 6,
                "deployments": 4 if variant == "baseline" else 2,
                "statefulsets": 0,
                "pods": 4 if variant == "baseline" else 2,
                "services": 4 if variant == "baseline" else 2,
                "persistent_volume_claims": 2 if variant == "baseline" else 1,
                "secrets": 4 if variant == "baseline" else 3,
            },
            "configured_resources": {
                "cpu_requests_millicores": 300 if variant == "baseline" else 200,
                "cpu_limits_millicores": 700 if variant == "baseline" else 500,
                "memory_requests_bytes": 768 * 1024**2 if variant == "baseline" else 512 * 1024**2,
                "memory_limits_bytes": 1536 * 1024**2 if variant == "baseline" else 1024 * 1024**2,
                "pvc_requested_bytes": 6 * 1024**3 if variant == "baseline" else 1024**3,
            },
            "observed_resources": {
                "samples": [
                    {"captured_at": "2026-09-27T00:00:00Z", "cpu_millicores": cpu, "memory_bytes": memory},
                    {"captured_at": "2026-09-27T00:00:10Z", "cpu_millicores": cpu, "memory_bytes": memory},
                ],
                "aggregate": {
                    "sample_count": 2,
                    "cpu_average_millicores": cpu,
                    "cpu_peak_millicores": cpu,
                    "memory_average_bytes": memory,
                    "memory_peak_bytes": memory,
                },
            },
            "inventory": {"workloads": workloads, "images": images},
            "durations": {"bootstrap_seconds": 100.0, "e2e_seconds": 200.0},
            "privacy": {
                "classification": "technical_aggregate_only",
                "contains_personal_data": False,
                "contains_secret_values": False,
            },
            "limitations": [],
        }

    def test_comparison_reports_positive_savings_and_removed_components(self):
        baseline = self._snapshot(
            "baseline",
            300,
            900 * 1024**2,
            ["airflow", "hive-metastore", "postgres-metastore"],
            ["data-master-airflow:git-aaaaaaa", "bde2020/hive:2.3.2-postgresql-metastore", "postgres:15"],
        )
        candidate = self._snapshot(
            "candidate",
            180,
            500 * 1024**2,
            ["airflow"],
            ["data-master-airflow:git-bbbbbbb"],
        )
        comparison = RUNTIME_FOOTPRINT.compare_snapshots(baseline, candidate)
        self.assertEqual(comparison["savings"]["object_counts"]["argocd_applications"]["saved"], 2)
        self.assertEqual(comparison["savings"]["configured_resources"]["pvc_requested_bytes"]["saved"], 5 * 1024**3)
        self.assertEqual(comparison["savings"]["observed_resources"]["cpu_average_millicores"]["saved"], 120)
        self.assertEqual(
            comparison["savings"]["removed_workloads"],
            ["hive-metastore", "postgres-metastore"],
        )
        self.assertEqual(
            comparison["functional_equivalence"]["status"],
            "REQUIRES_SEPARATE_E2E_EVIDENCE",
        )

    def test_comparison_fails_if_candidate_still_contains_catalog_components(self):
        baseline = self._snapshot("baseline", 100, 1000, ["hive-metastore"], ["bde2020/hive:old"])
        candidate = self._snapshot("candidate", 90, 900, ["hive-metastore"], ["bde2020/hive:old"])
        with self.assertRaisesRegex(ValueError, "still contains"):
            RUNTIME_FOOTPRINT.compare_snapshots(baseline, candidate)

    def test_comparison_fails_for_non_equivalent_cluster_capacity(self):
        baseline = self._snapshot("baseline", 100, 1000, ["airflow"], ["airflow:a"])
        candidate = self._snapshot("candidate", 90, 900, ["airflow"], ["airflow:b"])
        candidate["environment"]["node_capacity"]["cpu_millicores"] = 8000
        with self.assertRaisesRegex(ValueError, "node_capacity"):
            RUNTIME_FOOTPRINT.compare_snapshots(baseline, candidate)


class NoHiveRuntimeContractTests(unittest.TestCase):
    def test_catalog_runtime_artifacts_are_absent(self):
        self.assertFalse((REPO_ROOT / "infra" / "helm-charts" / "hive-metastore").exists())
        self.assertFalse((REPO_ROOT / "infra" / "helm-charts" / "postgres-metastore").exists())
        self.assertFalse((REPO_ROOT / "infra" / "validation_spark_hive.py").exists())
        templates = REPO_ROOT / "infra" / "argocd" / "applications" / "templates"
        self.assertFalse((templates / "hive-metastore-app.yaml").exists())
        self.assertFalse((templates / "postgres-metastore-app.yaml").exists())
        self.assertEqual(len(list(templates.glob("*-app.yaml"))), 5)

    def test_operational_contracts_do_not_require_removed_components(self):
        paths = [
            REPO_ROOT / "infra" / "helm-charts" / "jupyter" / "values.yaml",
            REPO_ROOT / "scripts" / "minikube" / "Import-DataMasterImages.ps1",
            REPO_ROOT / "scripts" / "minikube" / "Initialize-DataMasterSecrets.ps1",
            REPO_ROOT / "scripts" / "minikube" / "Install-DataMasterDirectRuntime.ps1",
            REPO_ROOT / "scripts" / "minikube" / "Invoke-AirflowEndToEndTest.ps1",
            REPO_ROOT / "scripts" / "minikube" / "Wait-DataMasterReady.ps1",
        ]
        combined = "\n".join(path.read_text(encoding="utf-8").lower() for path in paths)
        for forbidden in (
            "hive-metastore",
            "postgres-metastore",
            "data-master-postgres-secret",
            "hive.metastore.uris",
            "bde2020/hive",
            "postgres:15",
        ):
            self.assertNotIn(forbidden, combined)

    def test_metric_commands_and_claim_limits_are_explicit(self):
        measure = (REPO_ROOT / "scripts" / "minikube" / "Measure-DataMasterRuntimeFootprint.ps1").read_text(encoding="utf-8")
        compare = (REPO_ROOT / "scripts" / "minikube" / "Compare-DataMasterRuntimeFootprints.ps1").read_text(encoding="utf-8")
        source = MODULE_PATH.read_text(encoding="utf-8")
        self.assertIn("Assert-DataMasterSafeProfile", measure)
        self.assertIn("runtime_footprint.py", measure)
        self.assertIn("runtime_footprint.py", compare)
        self.assertIn("technical_aggregate_only", source)
        self.assertIn("no_financial_or_energy_savings_claim", source)
        self.assertIn("REQUIRES_SEPARATE_E2E_EVIDENCE", source)


if __name__ == "__main__":
    unittest.main()