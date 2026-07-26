import base64
import os
import re
import shutil
import subprocess
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = REPO_ROOT / "scripts" / "minikube"
POWERSHELL_EXECUTABLES = tuple(
    dict.fromkeys(
        executable
        for executable in (
            shutil.which("pwsh"),
            shutil.which("powershell.exe"),
            shutil.which("powershell"),
        )
        if executable
    )
)


class MinikubeReproducibilityContractTests(unittest.TestCase):
    def test_required_powershell_entrypoints_exist(self):
        required = {
            "Test-DataMasterPrerequisites.ps1",
            "New-DataMasterCluster.ps1",
            "Build-DataMasterImages.ps1",
            "Import-DataMasterImages.ps1",
            "Initialize-DataMasterSecrets.ps1",
            "Install-DataMasterArgoCD.ps1",
            "Deploy-DataMasterGitOps.ps1",
            "Wait-DataMasterReady.ps1",
            "Invoke-SparkIntegrationTest.ps1",
            "Invoke-AirflowEndToEndTest.ps1",
            "Invoke-DataMasterQualityGates.ps1",
            "Test-DataMasterExecutionEvidence.ps1",
            "Start-DataMasterPortForwards.ps1",
            "Stop-DataMasterPortForwards.ps1",
            "Remove-DataMasterCluster.ps1",
            "Invoke-DataMasterCleanRoomValidation.ps1",
        }
        self.assertEqual(required - {path.name for path in SCRIPTS.glob("*.ps1")}, set())

    def test_scripts_are_strict_portable_and_protect_existing_cluster(self):
        for path in SCRIPTS.glob("*.ps1"):
            source = path.read_text(encoding="utf-8")
            self.assertIn("Set-StrictMode", source, path.name)
            self.assertIn('$ErrorActionPreference = "Stop"', source, path.name)
            self.assertNotRegex(
                source,
                r"C:" + r"\\Users\\",
                path.name,
            )
        common = (SCRIPTS / "DataMaster.Minikube.Common.ps1").read_text(
            encoding="utf-8"
        )
        self.assertIn("protected profile 'data-master'", common)
        remove = (SCRIPTS / "Remove-DataMasterCluster.ps1").read_text(
            encoding="utf-8"
        )
        self.assertIn("ConfirmDeletion", remove)

    def test_gitops_root_and_children_share_explicit_revision(self):
        root = (
            REPO_ROOT
            / "infra"
            / "argocd"
            / "applications"
            / "root"
            / "app-of-apps.yaml"
        ).read_text(encoding="utf-8")
        self.assertGreaterEqual(root.count("__GIT_REVISION__"), 2)
        for path in (
            REPO_ROOT / "infra" / "argocd" / "applications" / "templates"
        ).glob("*-app.yaml"):
            source = path.read_text(encoding="utf-8")
            if "kubeflow.github.io/spark-operator" not in source:
                self.assertIn(".Values.git.revision", source, path.name)
        deploy = (SCRIPTS / "Deploy-DataMasterGitOps.ps1").read_text(
            encoding="utf-8"
        )
        self.assertIn("BLOCKED_NOT_PUBLISHED", deploy)
        self.assertIn("New-TemporaryFile", deploy)

    def test_manual_gitops_sequence_binds_revision_and_image_tag_to_head(self):
        guide = (REPO_ROOT / "infra" / "README-gitops.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("status --porcelain --untracked-files=all", guide)
        self.assertIn('$resolvedRevision -ne $localHead', guide)
        self.assertIn("Resolve-DataMasterRemoteRevision", guide)
        self.assertIn("$remoteRevision -ne $localHead", guide)
        self.assertIn(
            "manifests e imagens nunca podem vir de SHAs diferentes",
            guide,
        )
        self.assertIn(
            '$tag = "git-" + $localHead.Substring(0, 12)',
            guide,
        )
        self.assertLess(
            guide.index('$resolvedRevision -ne $localHead'),
            guide.index("$remoteRevision -ne $localHead"),
        )
        self.assertLess(
            guide.index("$remoteRevision -ne $localHead"),
            guide.index('$tag = "git-" + $localHead.Substring(0, 12)'),
        )
        self.assertLess(
            guide.index('$tag = "git-" + $localHead.Substring(0, 12)'),
            guide.index(
                "-Revision $revision",
                guide.index("Deploy-DataMasterGitOps.ps1"),
            ),
        )

    def test_remote_revision_resolution_is_exact_and_fail_closed(self):
        common = (SCRIPTS / "DataMaster.Minikube.Common.ps1").read_text(
            encoding="utf-8"
        )
        self.assertIn("function Resolve-DataMasterRemoteRevision", common)
        self.assertIn("git ls-remote $RepoUrl", common)
        self.assertIn('EndsWith("^{}")', common)
        self.assertIn("$resolvedShas.Count -ne 1", common)
        self.assertIn("Select-Object -ExpandProperty Sha -Unique", common)

        clean_room = (
            SCRIPTS / "Invoke-DataMasterCleanRoomValidation.ps1"
        ).read_text(encoding="utf-8")
        self.assertIn("BLOCKED_REMOTE_REVISION_MISMATCH", clean_room)
        self.assertIn("$remoteRevision -ne $localHead", clean_room)
        self.assertLess(
            clean_room.index("$remoteRevision -ne $localHead"),
            clean_room.index('"New-DataMasterCluster.ps1"'),
        )

    def test_clean_room_refuses_preexisting_profile_without_mutating_it(self):
        common = (SCRIPTS / "DataMaster.Minikube.Common.ps1").read_text(
            encoding="utf-8"
        )
        clean_room = (
            SCRIPTS / "Invoke-DataMasterCleanRoomValidation.ps1"
        ).read_text(encoding="utf-8")
        self.assertIn("minikube profile list --output=json", clean_room)
        self.assertIn(
            "function ConvertFrom-DataMasterMinikubeProfileInventory",
            common,
        )
        self.assertIn('foreach ($groupName in @("valid", "invalid"))', common)
        self.assertIn("$groupProperty.Value -is [System.Array]", common)
        self.assertIn(
            "ConvertFrom-DataMasterMinikubeProfileInventory",
            clean_room,
        )
        self.assertIn("BLOCKED_PROFILE_INVENTORY_INVALID", clean_room)
        self.assertIn("BLOCKED_PREEXISTING_PROFILE", clean_room)
        self.assertIn("it was not modified or deleted", clean_room)
        self.assertNotIn("minikube delete", clean_room)
        self.assertIn("CLEAN_ROOM_WORKTREE_STATUS=BLOCKED_DIRTY", clean_room)
        self.assertLess(
            clean_room.index(
                "Assert-DataMasterCleanWorktree -RepositoryRoot $root"
            ),
            clean_room.index('"New-DataMasterCluster.ps1"'),
        )
        self.assertLess(
            clean_room.index(
                "Assert-DataMasterFreshMinikubeProfile -TargetProfile $Profile"
            ),
            clean_room.index('"New-DataMasterCluster.ps1"'),
        )

    def test_profile_inventory_parser_is_behaviorally_fail_closed(self):
        self.assertTrue(
            POWERSHELL_EXECUTABLES,
            "PowerShell is required to validate the Minikube inventory parser.",
        )
        common_path = SCRIPTS / "DataMaster.Minikube.Common.ps1"
        command = (
            "[Console]::OutputEncoding = "
            "New-Object System.Text.UTF8Encoding($false); "
            "$OutputEncoding = [Console]::OutputEncoding; "
            "Set-StrictMode -Version Latest; "
            '$ErrorActionPreference = "Stop"; '
            ". $env:DATAMASTER_COMMON_SCRIPT; "
            "$jsonText = [System.Text.Encoding]::UTF8.GetString("
            "[System.Convert]::FromBase64String("
            "$env:DATAMASTER_PROFILE_INVENTORY_BASE64)); "
            "try { "
            "$names = @(ConvertFrom-DataMasterMinikubeProfileInventory "
            "-JsonText $jsonText) "
            "} catch { "
            '[Console]::Error.Write("PROFILE_INVENTORY_REJECTED"); '
            "exit 23 "
            "}; "
            "[Console]::Out.Write(($names -join [Environment]::NewLine))"
        )

        def run_parser(executable, json_text):
            environment = os.environ.copy()
            environment["DATAMASTER_COMMON_SCRIPT"] = str(common_path)
            environment["DATAMASTER_PROFILE_INVENTORY_BASE64"] = (
                base64.b64encode(json_text.encode("utf-8")).decode("ascii")
            )
            arguments = [
                executable,
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
            ]
            if os.name == "nt":
                arguments.extend(["-ExecutionPolicy", "Bypass"])
            arguments.extend(["-Command", command])
            return subprocess.run(
                arguments,
                cwd=REPO_ROOT,
                env=environment,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=False,
            )

        valid_fixtures = {
            "empty groups": ('{"valid":[],"invalid":[]}', []),
            "names in both groups": (
                '{"valid":[{"Name":"running","Status":"OK"},'
                '{"Name":"second"}],'
                '"invalid":[{"Name":"stopped","Status":"Error"}]}',
                ["running", "second", "stopped"],
            ),
        }
        for executable in POWERSHELL_EXECUTABLES:
            for fixture_name, (payload, expected_names) in valid_fixtures.items():
                with self.subTest(
                    executable=executable,
                    fixture=fixture_name,
                ):
                    result = run_parser(executable, payload)
                    self.assertEqual(
                        result.returncode,
                        0,
                        result.stdout + result.stderr,
                    )
                    self.assertEqual(result.stdout.splitlines(), expected_names)

        invalid_fixtures = {
            "empty text": "",
            "blank text": "  ",
            "malformed JSON": "{",
            "null root": "null",
            "array root": "[]",
            "scalar root": "7",
            "empty object root": "{}",
            "missing valid group": '{"invalid":[]}',
            "missing invalid group": '{"valid":[]}',
            "null valid group": '{"valid":null,"invalid":[]}',
            "null invalid group": '{"valid":[],"invalid":null}',
            "scalar valid group": '{"valid":{},"invalid":[]}',
            "scalar invalid group": '{"valid":[],"invalid":{}}',
            "null entry": '{"valid":[null],"invalid":[]}',
            "string entry": '{"valid":["profile"],"invalid":[]}',
            "numeric entry": '{"valid":[7],"invalid":[]}',
            "missing name": '{"valid":[{}],"invalid":[]}',
            "null name": '{"valid":[{"Name":null}],"invalid":[]}',
            "numeric name": '{"valid":[{"Name":7}],"invalid":[]}',
            "empty name": '{"valid":[{"Name":""}],"invalid":[]}',
            "blank name": '{"valid":[{"Name":"  "}],"invalid":[]}',
        }
        for executable in POWERSHELL_EXECUTABLES:
            for fixture_name, payload in invalid_fixtures.items():
                with self.subTest(
                    executable=executable,
                    fixture=fixture_name,
                ):
                    result = run_parser(executable, payload)
                    self.assertEqual(
                        result.returncode,
                        23,
                        result.stdout + result.stderr,
                    )
                    self.assertEqual(
                        result.stderr,
                        "PROFILE_INVENTORY_REJECTED",
                    )

    def test_crds_have_one_authority_and_spark_templates_are_not_auto_applied(self):
        operator = (
            REPO_ROOT
            / "infra"
            / "argocd"
            / "applications"
            / "templates"
            / "spark-operator-app.yaml"
        ).read_text(encoding="utf-8")
        self.assertIn("skipCrds: false", operator)
        spark_jobs = (
            REPO_ROOT
            / "infra"
            / "argocd"
            / "applications"
            / "templates"
            / "spark-jobs-app.yaml"
        ).read_text(encoding="utf-8")
        self.assertIn("path: infra/workloads/spark-apps/rbac", spark_jobs)
        self.assertIn("ServerSideApply=true", operator)

    def test_manifests_do_not_embed_known_demo_passwords_or_latest_images(self):
        paths = list((REPO_ROOT / "infra" / "helm-charts").rglob("*.yaml"))
        paths += list((REPO_ROOT / "infra" / "workloads").rglob("*.yaml"))
        rendered_source = "\n".join(
            path.read_text(encoding="utf-8") for path in paths
        )
        for forbidden in ("minio123", "spark123", "tag: latest"):
            self.assertNotIn(forbidden, rendered_source)
        self.assertNotRegex(
            rendered_source,
            re.compile(r"(?m)^\s*value:\s*(minio|hive|admin)\s*$"),
        )
        self.assertIn("secretKeyRef", rendered_source)

    def test_quality_gate_uses_durable_evidence_before_optional_pods(self):
        gate = (SCRIPTS / "Invoke-DataMasterQualityGates.ps1").read_text(
            encoding="utf-8"
        )
        self.assertIn("Read-DataMasterExecutionEvidence", gate)
        self.assertIn('POD_EVIDENCE_ROLE=COMPLEMENTARY', gate)
        self.assertIn('REPRODUCIBILITY_GATE_STATUS=PASS', gate)
        self.assertLess(
            gate.index("$evidence = Read-DataMasterExecutionEvidence"),
            gate.index("$podEvidenceStatus = Test-DataMasterComplementaryPodEvidence"),
        )

    def test_clean_room_passes_run_specific_evidence_to_gate(self):
        clean_room = (
            SCRIPTS / "Invoke-DataMasterCleanRoomValidation.ps1"
        ).read_text(encoding="utf-8")
        airflow = (SCRIPTS / "Invoke-AirflowEndToEndTest.ps1").read_text(
            encoding="utf-8"
        )
        self.assertIn("$resolvedRevision -ne $localHead", clean_room)
        self.assertIn("-EvidencePath $cleanRoomEvidencePath", clean_room)
        self.assertGreaterEqual(
            clean_room.count("-EvidencePath $cleanRoomEvidencePath"), 2
        )
        self.assertIn("Save-AirflowDurableEvidence", airflow)
        self.assertIn("AIRFLOW_DURABLE_EVIDENCE_STATUS=PASS", airflow)
        self.assertIn("will not be inferred", airflow)
        self.assertIn("[switch]$ResumeExistingRun", airflow)
        self.assertIn("AIRFLOW_DAG_TRIGGER_MODE=NEW_RUN", airflow)

    def test_clean_room_timeout_is_supported_by_all_downstream_steps(self):
        expected_ranges = {
            "Invoke-DataMasterCleanRoomValidation.ps1": "[ValidateRange(900, 10800)]",
            "Wait-DataMasterReady.ps1": "[ValidateRange(120, 10800)]",
            "Invoke-SparkIntegrationTest.ps1": "[ValidateRange(120, 10800)]",
            "Invoke-AirflowEndToEndTest.ps1": "[ValidateRange(300, 10800)]",
        }
        for script_name, expected_range in expected_ranges.items():
            source = (SCRIPTS / script_name).read_text(encoding="utf-8")
            self.assertIn(expected_range, source, script_name)

    def test_clean_room_preloads_versioned_runtime_dependencies(self):
        clean_room = (
            SCRIPTS / "Invoke-DataMasterCleanRoomValidation.ps1"
        ).read_text(encoding="utf-8")
        importer = (SCRIPTS / "Import-DataMasterImages.ps1").read_text(
            encoding="utf-8"
        )
        self.assertIn("-PreloadRuntimeDependencies", clean_room)
        for image in (
            "postgres:15",
            "bde2020/hive:2.3.2-postgresql-metastore",
            "minio/minio:RELEASE.2024-01-28T22-35-53Z",
            "minio/mc:RELEASE.2024-01-13T08-44-48Z",
            "quay.io/jupyter/pyspark-notebook:2024-04-01",
            "ghcr.io/kubeflow/spark-operator/controller:2.5.0",
        ):
            self.assertIn(image, importer)
        self.assertIn("MINIKUBE_RUNTIME_DEPENDENCY_IMPORT_STATUS=PASS", importer)
        self.assertIn("docker images --quiet $image", importer)
        self.assertIn("Import-DataMasterDockerImageStream", importer)
        self.assertIn("docker image load", importer)
        self.assertIn("docker\", \"image\", \"inspect\"", importer)
        self.assertIn("BaseStream.CopyTo", importer)

    def test_ready_helper_waits_for_statefulsets_by_namespace(self):
        ready_helper = (SCRIPTS / "Wait-DataMasterReady.ps1").read_text(
            encoding="utf-8"
        )
        self.assertIn("kubectl get statefulset -A -o json", ready_helper)
        self.assertIn('"statefulset/$($statefulSet.metadata.name)"', ready_helper)
        self.assertIn('$statefulSet.metadata.namespace', ready_helper)
        self.assertNotIn('"statefulset", "--all", "-A"', ready_helper)

    def test_e2e_observer_handles_optional_spark_labels(self):
        e2e = (SCRIPTS / "Invoke-AirflowEndToEndTest.ps1").read_text(
            encoding="utf-8"
        )
        self.assertIn("function Get-DataMasterLabelValue", e2e)
        self.assertIn("$dagRun.start_date", e2e)
        self.assertIn("$observationStart", e2e)
        self.assertIn('-Labels $_.spec.driver.labels', e2e)

    def test_e2e_observer_persists_spark_checkpoint_before_completion(self):
        e2e = (SCRIPTS / "Invoke-AirflowEndToEndTest.ps1").read_text(
            encoding="utf-8"
        )
        checkpoint = (SCRIPTS / "DataMaster.ExecutionEvidence.ps1").read_text(
            encoding="utf-8"
        )
        self.assertIn(".sparkapplications.checkpoint.json", e2e)
        self.assertIn("Save-DataMasterSparkApplicationObservation", e2e)
        self.assertIn(
            "-ObservationCheckpointPath $observationCheckpointPath", e2e
        )
        self.assertLess(
            e2e.index("Save-DataMasterSparkApplicationObservation"),
            e2e.index('if ($state -eq "success")'),
        )
        self.assertIn(
            "Read-DataMasterSparkApplicationObservationCheckpoint", checkpoint
        )
        self.assertIn(
            "SparkApplication checkpoint is missing stage", checkpoint
        )
        self.assertIn("[switch]$MaterializeExistingEvidence", e2e)
        self.assertIn(
            "AIRFLOW_DURABLE_EVIDENCE_MATERIALIZATION_STATUS=PASS", e2e
        )
        self.assertIn("Write-AirflowDurableEvidenceFailureRecord", e2e)
        self.assertIn("AIRFLOW_DURABLE_EVIDENCE_FAILURE_RECORD=", e2e)
        self.assertIn("Set-AirflowDurableEvidencePhase", e2e)
        self.assertIn(".progress.json", e2e)
        self.assertIn("IsPathRooted($EvidencePath)", e2e)
        self.assertIn("$EvidencePath = [System.IO.Path]::GetFullPath", e2e)

    def test_new_evidence_can_record_distinct_gold_storage_without_breaking_v1(self):
        e2e = (SCRIPTS / "Invoke-AirflowEndToEndTest.ps1").read_text(
            encoding="utf-8"
        )
        contract = (SCRIPTS / "DataMaster.ExecutionEvidence.ps1").read_text(
            encoding="utf-8"
        )
        gates = (SCRIPTS / "Invoke-DataMasterQualityGates.ps1").read_text(
            encoding="utf-8"
        )

        self.assertIn('$hasStorageEvidence', e2e)
        self.assertIn('$evidence["storage"]', e2e)
        self.assertIn('-Optional @("storage")', contract)
        self.assertIn("Business Vault and Gold paths must be distinct", contract)
        self.assertIn("REPRODUCIBILITY_GOLD_PATH_CHECK=PASS", gates)
        self.assertIn("NOT_RECORDED_LEGACY_EVIDENCE", gates)


if __name__ == "__main__":
    unittest.main()
