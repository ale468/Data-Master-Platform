# Copyright (C) 2026 Alexandre Ferreira
# SPDX-License-Identifier: AGPL-3.0-only

import json
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from jobs.presentation.jupyter_delta_path import (  # noqa: E402
    GOLD_PRESENTATION_QUERY,
    PRESENTATION_LAYERS,
    normalize_lakehouse_root,
    presentation_paths,
)


class JupyterPresentationContractTests(unittest.TestCase):
    def test_paths_and_views_cover_bronze_raw_vault_and_gold(self):
        self.assertEqual(
            list(PRESENTATION_LAYERS),
            ["bronze", "raw_vault", "gold"],
        )
        self.assertEqual(
            presentation_paths("s3a://lakehouse/"),
            {
                "bronze": "s3a://lakehouse/bronze/transacoes",
                "raw_vault": (
                    "s3a://lakehouse/raw_vault/hubs/hub_transacao"
                ),
                "gold": (
                    "s3a://lakehouse/gold/gold_transacoes_por_dia"
                ),
            },
        )
        self.assertEqual(
            [item["view"] for item in PRESENTATION_LAYERS.values()],
            [
                "bronze_transacoes",
                "raw_hub_transacao",
                "gold_transacoes_por_dia",
            ],
        )
        self.assertIn("FROM gold_transacoes_por_dia", GOLD_PRESENTATION_QUERY)
        self.assertNotIn("s3a://", GOLD_PRESENTATION_QUERY)

    def test_lakehouse_root_is_explicit_and_fail_closed(self):
        self.assertEqual(
            normalize_lakehouse_root("s3a://lakehouse///"),
            "s3a://lakehouse",
        )
        for invalid in ("", "lakehouse", "/tmp/lakehouse"):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    normalize_lakehouse_root(invalid)

    def test_helper_is_read_only_and_uses_committed_delta_snapshots(self):
        source = (
            REPO_ROOT
            / "jobs"
            / "presentation"
            / "jupyter_delta_path.py"
        ).read_text(encoding="utf-8")
        self.assertIn('spark.read.format("delta").load(path)', source)
        self.assertIn("DeltaTable.forPath", source)
        self.assertIn("version_before != version_after", source)
        for forbidden in (".write", ".save(", ".mode(", "saveAsTable"):
            self.assertNotIn(forbidden, source)

    def test_notebook_is_output_free_and_uses_predefined_views(self):
        path = (
            REPO_ROOT
            / "jobs"
            / "presentation"
            / "notebooks"
            / "data_master_delta_presentation.ipynb"
        )
        document = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(document["nbformat"], 4)
        self.assertGreaterEqual(len(document["cells"]), 7)
        code = "\n".join(
            "".join(cell.get("source", []))
            for cell in document["cells"]
            if cell.get("cell_type") == "code"
        )
        self.assertIn("prepare_presentation_views", code)
        self.assertIn("run_prepared_gold_query", code)
        self.assertIn("bronze_transacoes", code)
        self.assertIn("raw_hub_transacao", code)
        self.assertIn("gold_transacoes_por_dia", code)
        self.assertNotIn("s3a://", code)
        self.assertTrue(
            all(
                not cell.get("outputs")
                for cell in document["cells"]
                if cell.get("cell_type") == "code"
            )
        )

    def test_image_chart_and_gitops_use_one_immutable_jupyter_image(self):
        dockerfile = (REPO_ROOT / "Dockerfile.jupyter").read_text(
            encoding="utf-8"
        )
        values = (
            REPO_ROOT / "infra" / "helm-charts" / "jupyter" / "values.yaml"
        ).read_text(encoding="utf-8")
        deployment = (
            REPO_ROOT
            / "infra"
            / "helm-charts"
            / "jupyter"
            / "templates"
            / "deployment.yaml"
        ).read_text(encoding="utf-8")
        root_app = (
            REPO_ROOT
            / "infra"
            / "argocd"
            / "applications"
            / "root"
            / "app-of-apps.yaml"
        ).read_text(encoding="utf-8")
        minio_values = (
            REPO_ROOT / "infra" / "helm-charts" / "minio" / "values.yaml"
        ).read_text(encoding="utf-8")
        minio_init = (
            REPO_ROOT
            / "infra"
            / "helm-charts"
            / "minio"
            / "templates"
            / "init-buckets-job.yaml"
        ).read_text(encoding="utf-8")
        secret_initializer = (
            REPO_ROOT
            / "scripts"
            / "minikube"
            / "Initialize-DataMasterSecrets.ps1"
        ).read_text(encoding="utf-8")
        self.assertIn("ARG SPARK_BASE_IMAGE=", dockerfile)
        self.assertIn("PRESENTATION_READ_ONLY", dockerfile)
        self.assertIn("IdentityProvider.token", dockerfile)
        self.assertIn("repository: data-master-jupyter", values)
        self.assertIn("git-unpublished", values)
        self.assertNotIn("hive.metastore", values)
        self.assertIn("EnvironmentVariableCredentialsProvider", values)
        self.assertIn("-XX:-UseContainerSupport -Duser.home=/tmp", values)
        self.assertIn("secretKeyRef", deployment)
        self.assertIn("data-master-jupyter-minio-secret", values)
        self.assertNotIn("name: data-master-minio-secret", deployment)
        self.assertIn("jupyterCredentialsSecretName", minio_values)
        self.assertIn("mc admin policy attach local readonly", minio_init)
        self.assertIn('"policyName":"readonly"', minio_init)
        self.assertIn("data-master-jupyter-minio-secret", secret_initializer)
        self.assertGreaterEqual(deployment.count("tcpSocket:"), 2)
        self.assertIn("__JUPYTER_IMAGE_REPOSITORY__", root_app)

    def test_validator_is_separate_from_pipeline_readiness(self):
        validator = (
            REPO_ROOT
            / "scripts"
            / "minikube"
            / "Invoke-JupyterPresentationValidation.ps1"
        ).read_text(encoding="utf-8")
        readiness = (
            REPO_ROOT
            / "scripts"
            / "minikube"
            / "Wait-DataMasterReady.ps1"
        ).read_text(encoding="utf-8")
        clean_room = (
            REPO_ROOT
            / "scripts"
            / "minikube"
            / "Invoke-DataMasterCleanRoomValidation.ps1"
        ).read_text(encoding="utf-8")
        self.assertIn("JUPYTER_PRESENTATION_VALIDATION_STATUS=PASS", validator)
        self.assertIn("JUPYTER_AUTHENTICATED_API_STATUS=PASS", validator)
        self.assertIn('PSObject.Properties["deletionTimestamp"]', validator)
        self.assertIn("technical_aggregate_only", validator)
        self.assertIn('$optionalApplicationNames = @("jupyter-app")', readiness)
        self.assertIn('PSObject.Properties["status"]', readiness)
        self.assertIn("OPTIONAL_NOT_READY", readiness)
        self.assertNotIn("Invoke-JupyterPresentationValidation", clean_room)

    def test_ci_builds_and_smokes_the_jupyter_image(self):
        workflow = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("jupyter-quality-gates:", workflow)
        self.assertIn("Dockerfile.jupyter", workflow)
        self.assertIn("JUPYTER_IMAGE_CONTRACT_STATUS=SUCCESS", workflow)
        self.assertIn("JUPYTER_DELTA_LOCAL_SMOKE_STATUS=SUCCESS", workflow)


if __name__ == "__main__":
    unittest.main()
