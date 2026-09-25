# Copyright (C) 2026 Alexandre Ferreira
# SPDX-License-Identifier: AGPL-3.0-only

import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
RUNNER = REPO_ROOT / "scripts" / "minikube" / "Invoke-DataMasterMultibatchValidation.ps1"
AIRFLOW_RUNNER = REPO_ROOT / "scripts" / "minikube" / "Invoke-AirflowEndToEndTest.ps1"


class MultibatchOrchestrationContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.runner = RUNNER.read_text(encoding="utf-8-sig")
        cls.airflow_runner = AIRFLOW_RUNNER.read_text(encoding="utf-8-sig")

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


if __name__ == "__main__":
    unittest.main()
