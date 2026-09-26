# Copyright (C) 2026 Alexandre Ferreira
# SPDX-License-Identifier: AGPL-3.0-only

import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
RUNNER = REPO_ROOT / "scripts" / "minikube" / "Invoke-DataMasterMultibatchValidation.ps1"
AIRFLOW_RUNNER = REPO_ROOT / "scripts" / "minikube" / "Invoke-AirflowEndToEndTest.ps1"
STAGE_RUNNER = REPO_ROOT / "jobs" / "kubernetes" / "run_pipeline_stage.py"


class MultibatchOrchestrationContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.runner = RUNNER.read_text(encoding="utf-8-sig")
        cls.airflow_runner = AIRFLOW_RUNNER.read_text(encoding="utf-8-sig")
        cls.stage_runner = STAGE_RUNNER.read_text(encoding="utf-8-sig")

    def test_sequence_has_three_logical_batches_and_explicit_replay(self):
        for name in ("batch_1", "batch_2", "batch_2_replay", "batch_3"):
            self.assertIn(f'Name = "{name}"', self.runner)
        self.assertIn('BatchId = "$ScenarioId-b2"; RunId = "$ScenarioId-b2"', self.runner)
        self.assertIn(
            'BatchId = "$ScenarioId-b2"; RunId = "$ScenarioId-b2-replay"',
            self.runner,
        )

    def test_replay_and_late_arrival_fail_closed(self):
        self.assertIn('if ($delta -ne 0)', self.runner)
        self.assertIn("deterministic_replay_manifest", self.runner)
        self.assertIn("$lateEvent -lt $batch2Generated", self.runner)
        self.assertIn("$batch2Generated -lt $lateLoad", self.runner)

    def test_runner_does_not_remove_the_isolated_profile(self):
        self.assertNotIn("Remove-DataMasterCluster", self.runner)
        self.assertNotIn("minikube delete", self.runner)

    def test_airflow_runner_validates_and_forwards_dag_conf(self):
        self.assertIn("DagRunConfigJson", self.airflow_runner)
        self.assertIn('source_batch -notin @("batch-1", "batch-2", "batch-3")', self.airflow_runner)
        self.assertIn('@("--conf", $DagRunConfigJson)', self.airflow_runner)
        self.assertIn('$evidence["multibatch"]', self.airflow_runner)

    def test_multibatch_is_added_after_evidence_object_exists(self):
        self.assertLess(
            self.airflow_runner.index("$evidence = [ordered]@{"),
            self.airflow_runner.index('$evidence["multibatch"] ='),
        )

    def test_storage_markers_are_retained_in_durable_task_logs(self):
        self.assertIn("GOLD_STORAGE_PATH_STATUS=PASS", self.airflow_runner)
        self.assertIn(
            "BUSINESS_VAULT_GOLD_PATH_SEPARATION_STATUS=PASS",
            self.airflow_runner,
        )

    def test_presentation_json_preserves_iso_timestamp_strings(self):
        self.assertIn(
            "$presentation = ConvertFrom-DataMasterJson -Json",
            self.airflow_runner,
        )

    def test_existing_step_evidence_is_validated_before_reuse(self):
        self.assertIn("Read-DataMasterExecutionEvidence", self.runner)
        self.assertIn("Assert-MultibatchStepEvidence", self.runner)
        self.assertIn(
            "MULTIBATCH_STEP_EVIDENCE_MODE=REUSED_VALIDATED:$($step.Name)",
            self.runner,
        )

    def test_dynamic_sources_are_forwarded_as_driver_records(self):
        self.assertIn('generated["records"]', self.stage_runner)


if __name__ == "__main__":
    unittest.main()
