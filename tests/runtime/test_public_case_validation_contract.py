import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "jobs" / "demo"))
sys.path.insert(0, str(REPO_ROOT / "jobs" / "observability"))

import run_observability_smoke  # noqa: E402
import run_presentation_demo  # noqa: E402


EXPECTED_DAG_TASKS = [
    "run_bronze",
    "run_hubs",
    "run_links",
    "run_satellites",
    "run_gold",
    "run_data_vault_gate",
    "run_masking_gate",
    "run_evidence",
]


class PresentationDemoContractTests(unittest.TestCase):
    def test_expected_runtime_profile_defaults_to_presentation_demo(self):
        with patch.object(sys, "argv", ["run_presentation_demo.py"]):
            args = run_presentation_demo._parse_args()

        self.assertEqual(args.expected_runtime_profile, "presentation-demo")

    def test_local_small_can_be_declared_as_the_expected_profile(self):
        argv = [
            "run_presentation_demo.py",
            "--runtime-profile",
            "local-small",
            "--expected-runtime-profile",
            "local-small",
        ]
        with patch.object(sys, "argv", argv):
            args = run_presentation_demo._parse_args()

        self.assertEqual(args.runtime_profile, "local-small")
        self.assertEqual(args.expected_runtime_profile, "local-small")

    def test_local_small_scope_does_not_claim_integrated_demo_readiness(self):
        self.assertEqual(
            run_presentation_demo._execution_scope("local-small"),
            "local_direct_validation",
        )
        readiness = run_presentation_demo._readiness_status(
            "local-small",
            failed=False,
        )
        demo_gate = run_presentation_demo._demo_gate_result(
            "local-small",
            failed=False,
        )

        self.assertIn("Airflow", readiness)
        self.assertIn("Minikube", readiness)
        self.assertIn("not evaluated", readiness.lower())
        self.assertIn("not evaluated", demo_gate.lower())
        self.assertNotIn("Demo-ready", readiness)

    def test_failed_local_small_validation_is_not_ready(self):
        self.assertEqual(
            run_presentation_demo._readiness_status(
                "local-small",
                failed=True,
            ),
            "Not ready",
        )

    def test_local_small_pdf_readiness_is_conservative(self):
        masking_failures = {
            "masking_sample_failures": [],
            "forbidden_columns": [],
            "raw_pattern_hits": {},
            "protected_check_failures": {},
            "cliente_check_failures": {},
            "risco_check_failures": {},
            "secret_findings": [],
        }
        readiness = run_presentation_demo._readiness_by_pdf_criterion(
            {
                "bronze": 1,
                "raw_vault_hubs": 1,
                "raw_vault_links": 1,
                "raw_vault_satellites": 1,
                "gold": 1,
            },
            monitoring_rows=5,
            masking_failures=masking_failures,
            runtime_profile="local-small",
        )

        self.assertEqual(
            readiness["apresentacao"]["status"],
            "not_evaluated_by_local_direct_validation",
        )
        self.assertEqual(readiness["escalabilidade"]["status"], "baseline_only")
        self.assertIn(
            "does not prove distributed execution",
            readiness["escalabilidade"]["evidence"],
        )

    def test_runtime_profile_mismatch_is_generic_not_profile_specific(self):
        source = (
            REPO_ROOT / "jobs" / "demo" / "run_presentation_demo.py"
        ).read_text(encoding="utf-8")

        self.assertIn('"runtime_profile_mismatch"', source)
        self.assertNotIn("runtime_profile_not_presentation_demo", source)
        self.assertIn("PRESENTATION_DEMO_RESULT=", source)

    def test_runner_always_emits_a_sanitized_marker_after_bootstrap(self):
        source = (
            REPO_ROOT / "jobs" / "demo" / "run_presentation_demo.py"
        ).read_text(encoding="utf-8")
        summary_index = source.index('summary: Dict[str, Any] = {')
        bootstrap_index = source.index("spark = create_spark_session()")
        finally_index = source.index("\n    finally:", bootstrap_index)
        marker_index = source.index(
            'print("PRESENTATION_DEMO_RESULT="',
            finally_index,
        )

        self.assertLess(summary_index, bootstrap_index)
        self.assertLess(bootstrap_index, finally_index)
        self.assertLess(finally_index, marker_index)
        self.assertIn('"error": "Execution failed;', source)
        self.assertNotIn('"error": str(exc)', source)
        self.assertIn("cleanup_error_type", source)


class AirflowStaticSummaryContractTests(unittest.TestCase):
    def test_current_eight_dag_tasks_are_reported(self):
        summary = run_observability_smoke._airflow_static_summary()

        self.assertEqual(summary["status"], "STATIC_READABLE")
        self.assertEqual(summary["task_ids"], EXPECTED_DAG_TASKS)
        self.assertEqual(summary["expected_task_ids"], EXPECTED_DAG_TASKS)
        self.assertEqual(summary["missing_expected_task_ids"], [])

    def test_airflow_summary_does_not_expose_a_repository_path(self):
        summary = run_observability_smoke._airflow_static_summary()
        serialized = json.dumps(summary, sort_keys=True)

        self.assertNotIn("dag_file", summary)
        self.assertNotIn(str(REPO_ROOT), serialized)
        self.assertNotIn("\\", serialized)


class PublicCasePowerShellContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.script_path = (
            REPO_ROOT / "scripts" / "Invoke-PublicCaseValidation.ps1"
        )
        cls.source = cls.script_path.read_text(encoding="utf-8")

    def test_wrapper_is_strict_local_small_and_containerized(self):
        self.assertIn("Set-StrictMode -Version Latest", self.source)
        self.assertIn('$ErrorActionPreference = "Stop"', self.source)
        self.assertIn('[ValidateSet("local-small")]', self.source)
        self.assertIn('"--expected-runtime-profile"', self.source)
        self.assertIn('"65534:65534"', self.source)
        self.assertIn("target=/repo,readonly", self.source)

    def test_wrapper_has_fail_closed_marker_contracts(self):
        self.assertIn('PRESENTATION_DEMO_RESULT=', self.source)
        self.assertIn("$markerLines.Count -ne 1", self.source)
        self.assertIn("PRESENTATION_PAYLOAD_INVALID_JSON", self.source)
        self.assertIn("$RunnerExitCode -ne 0", self.source)
        self.assertIn("PUBLIC_CASE_GATE_FAILED", self.source)
        self.assertIn("CASE_VALIDATION_STATUS=", self.source)
        self.assertRegex(self.source, r"exit\s+\$finalExitCode")

    def test_wrapper_validates_every_required_public_gate(self):
        for token in (
            '"bronze"',
            '"raw_hubs"',
            '"raw_links"',
            '"raw_satellites"',
            '"gold"',
            '"data_vault_quality_gate"',
            '"masking_failures"',
            '"monitoring"',
            '"secret_findings"',
        ):
            with self.subTest(token=token):
                self.assertIn(token, self.source)

    def test_normalized_result_uses_an_explicit_safe_allowlist(self):
        function_start = self.source.index("function New-SanitizedCaseResult")
        function_end = self.source.index("function Write-SanitizedCaseResult")
        function_source = self.source[function_start:function_end]
        output_source = function_source.rsplit("return [ordered]@{", 1)[1]

        for forbidden in (
            "work_dir",
            "sample_data_path",
            "bronze_path",
            "raw_vault_path",
            "business_vault_path",
            "gold_path",
            "monitoring_path",
            "masking_samples",
            "spark_config",
            "environment",
            "error_message",
        ):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, output_source)

        for allowed in (
            "schema_version",
            "runtime_profile",
            "execution_scope",
            "source_revision",
            "batch_id",
            "checks",
            "failed_checks",
        ):
            with self.subTest(allowed=allowed):
                self.assertIn(allowed, output_source)

    def test_wrapper_rejects_unsafe_result_destinations(self):
        self.assertIn("FORBIDDEN_RESULT_PATH", self.source)
        self.assertIn("TRACKED_RESULT_PATH", self.source)
        self.assertIn('"evidence\\runtime"', self.source)
        self.assertIn('"spdd"', self.source)

    def test_wrapper_never_logs_in_pushes_or_deploys(self):
        lowered = self.source.lower()
        self.assertNotIn("docker login", lowered)
        self.assertNotIn("docker push", lowered)
        self.assertNotIn("kubectl ", lowered)
        self.assertNotIn("helm ", lowered)

    def test_wrapper_requires_a_clean_revision_and_suppresses_raw_runner_output(self):
        self.assertIn("Assert-CleanPublicWorktree", self.source)
        self.assertIn("PUBLIC_WORKTREE_NOT_CLEAN", self.source)
        self.assertIn('"--workdir", "/tmp"', self.source)
        self.assertIn('"/repo/jobs/demo/run_presentation_demo.py"', self.source)
        self.assertNotIn("Tee-Object", self.source)
        self.assertNotIn("Write-Output $demoOutput", self.source)

    def test_wrapper_preserves_specific_failures_when_the_outer_gate_fails(self):
        self.assertIn('$existingFailures = @($normalizedResult["failed_checks"])', self.source)
        self.assertIn("Select-Object -Unique", self.source)

    def test_wrapper_uses_platform_specific_path_comparison(self):
        self.assertIn("[System.StringComparison]::Ordinal", self.source)
        self.assertIn("[System.StringComparison]::OrdinalIgnoreCase", self.source)
        self.assertIn(
            '[System.IO.Path]::DirectorySeparatorChar -eq "\\"',
            self.source,
        )


if __name__ == "__main__":
    unittest.main()
