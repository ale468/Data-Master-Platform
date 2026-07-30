import hashlib
import json
import statistics
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
ARTIFACT_PATH = (
    REPO_ROOT
    / "tests"
    / "evidence"
    / "horizontal-scaling"
    / "hscale-20260728064640.json"
)
sys.path.insert(0, str(REPO_ROOT / "jobs" / "common"))
sys.path.insert(0, str(REPO_ROOT / "jobs" / "scalability"))

from run_scalability_benchmark import validate_public_payload  # noqa: E402


ARTIFACT_SHA256 = (
    "55c3e372fb8db3f4835c1997027163b2c8a31175b83c09d4da14c12cb66ead09"
)
EXECUTED_GIT_SHA = "ee198106abda668a833826ace0e16f4e56516025"
EXECUTED_IMAGE_DIGEST = (
    "sha256:88b8facb12967c01f157bfd1245b44e9c3d101ee4762b0794b2f706e9a85ccac"
)
BENCHMARK_ID = "hscale-20260728094640"
BASELINE_PROFILE = "minikube-horizontal-1"
SCALE_OUT_PROFILE = "minikube-horizontal-3"
NUMERIC_TOLERANCE = 0.0005
MINIMUM_MONITORING_EVENTS = 5


def load_json_object(path):
    """Load an evidence object while rejecting absence and malformed content."""
    if not path.is_file():
        raise AssertionError(f"Committed evidence artifact is missing: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AssertionError("Committed evidence artifact is malformed.") from exc
    if not isinstance(payload, dict):
        raise AssertionError("Committed evidence artifact must be a JSON object.")
    return payload


def walk_keys(value):
    if isinstance(value, dict):
        for key, item in value.items():
            yield str(key)
            yield from walk_keys(item)
    elif isinstance(value, list):
        for item in value:
            yield from walk_keys(item)


class CommittedHorizontalEvidenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.payload = load_json_object(ARTIFACT_PATH)
        cls.baseline_runs = cls.payload.get("baseline", {}).get("runs", [])
        cls.scale_out_runs = cls.payload.get("scale_out", {}).get("runs", [])
        cls.all_runs = [*cls.baseline_runs, *cls.scale_out_runs]

    def assertClose(self, observed, expected):
        self.assertAlmostEqual(
            float(observed),
            float(expected),
            delta=NUMERIC_TOLERANCE,
        )

    def test_loader_fails_closed_for_missing_and_malformed_artifacts(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            with self.assertRaisesRegex(AssertionError, "missing"):
                load_json_object(root / "missing.json")

            malformed = root / "malformed.json"
            malformed.write_text("{not-json", encoding="utf-8")
            with self.assertRaisesRegex(AssertionError, "malformed"):
                load_json_object(malformed)

            non_object = root / "non-object.json"
            non_object.write_text("[]", encoding="utf-8")
            with self.assertRaisesRegex(AssertionError, "JSON object"):
                load_json_object(non_object)

    def test_identity_and_immutable_execution_references(self):
        artifact_digest = hashlib.sha256(ARTIFACT_PATH.read_bytes()).hexdigest()
        self.assertEqual(artifact_digest, ARTIFACT_SHA256)
        self.assertEqual(self.payload.get("schema_version"), 1)
        self.assertEqual(
            self.payload.get("benchmark_kind"),
            "static-horizontal-spark-scale-out",
        )
        self.assertEqual(self.payload.get("change_id"), "DM-RUN-004")
        self.assertEqual(self.payload.get("benchmark_id"), BENCHMARK_ID)
        self.assertEqual(
            self.payload.get("topology"),
            "single-node-application-scale-out",
        )
        self.assertEqual(self.payload.get("result"), "PASS")
        self.assertEqual(self.payload.get("failures"), [])
        self.assertEqual(
            {run.get("git_sha") for run in self.all_runs},
            {EXECUTED_GIT_SHA},
        )
        self.assertEqual(
            {run.get("image_digest") for run in self.all_runs},
            {EXECUTED_IMAGE_DIGEST},
        )

    def test_experimental_design_and_measurement_counts(self):
        experiment = self.payload.get("experiment", {})
        warmup = self.payload.get("warmup", {})
        self.assertEqual(experiment.get("warmups_discarded"), 1)
        self.assertTrue(warmup.get("discarded"))
        self.assertEqual(warmup.get("status"), "PASS")
        self.assertEqual(warmup.get("profile_id"), BASELINE_PROFILE)
        self.assertEqual(experiment.get("measurements_per_profile"), 3)
        self.assertEqual(experiment.get("baseline_profile"), BASELINE_PROFILE)
        self.assertEqual(experiment.get("scale_out_profile"), SCALE_OUT_PROFILE)
        self.assertEqual(
            experiment.get("primary_variable"),
            "spark.executor_instances",
        )
        self.assertEqual(experiment.get("statistic"), "median")

        self.assertEqual(len(self.baseline_runs), 3)
        self.assertEqual(len(self.scale_out_runs), 3)
        for runs, profile in (
            (self.baseline_runs, BASELINE_PROFILE),
            (self.scale_out_runs, SCALE_OUT_PROFILE),
        ):
            self.assertEqual({run.get("profile_id") for run in runs}, {profile})
            self.assertEqual(
                {run.get("measurement_kind") for run in runs},
                {"measurement"},
            )
            self.assertEqual({run.get("repetition") for run in runs}, {1, 2, 3})
            self.assertEqual({run.get("status") for run in runs}, {"PASS"})

    def test_executors_were_observed_and_did_real_work_on_one_node(self):
        for group_name, requested in (("baseline", 1), ("scale_out", 3)):
            group = self.payload.get(group_name, {})
            self.assertEqual(group.get("executors_requested"), requested)
            self.assertEqual(group.get("executors_observed"), [requested])
            self.assertEqual(len(group.get("nodes_observed", [])), 1)

            for run in group.get("runs", []):
                with self.subTest(run_id=run.get("run_id")):
                    evidence = run.get("executor_evidence", {})
                    executors = evidence.get("executors", [])
                    self.assertEqual(run.get("driver_pods_observed"), 1)
                    self.assertEqual(run.get("executors_requested"), requested)
                    self.assertEqual(
                        evidence.get("executors_requested"),
                        requested,
                    )
                    self.assertEqual(
                        evidence.get("executors_observed"),
                        requested,
                    )
                    self.assertEqual(len(executors), requested)
                    self.assertTrue(evidence.get("tasks_distributed"))
                    self.assertEqual(len(evidence.get("nodes_observed", [])), 1)

                    for executor in executors:
                        self.assertTrue(executor.get("pod"))
                        self.assertIn(
                            executor.get("pod_status"),
                            {"Running", "Succeeded"},
                        )
                        self.assertGreater(executor.get("tasks", 0), 0)
                        self.assertEqual(executor.get("failed_tasks"), 0)
                        for metric in (
                            "input_bytes",
                            "input_records",
                            "output_bytes",
                            "output_records",
                            "runtime_ms",
                            "shuffle_read_bytes",
                            "shuffle_write_bytes",
                        ):
                            self.assertGreater(
                                executor.get(metric, 0),
                                0,
                                f"{run.get('run_id')}:{metric}",
                            )

        observed_nodes = {
            node
            for run in self.all_runs
            for node in run["executor_evidence"]["nodes_observed"]
        }
        self.assertEqual(len(observed_nodes), 1)
        self.assertTrue(
            any(
                "one node" in limitation.lower()
                for limitation in self.payload.get("limitations", [])
            )
        )

    def test_all_measurements_are_functionally_equivalent(self):
        equal_fields = (
            "git_sha",
            "image_digest",
            "dataset_seed",
            "dataset_volume",
            "dataset_fingerprint",
            "pipeline_contract_version",
            "input_rows",
            "source_counts",
            "layer_counts",
            "output_fingerprint",
            "functional_fingerprints",
        )
        for field in equal_fields:
            serialized = {
                json.dumps(run.get(field), sort_keys=True)
                for run in self.all_runs
            }
            self.assertEqual(len(serialized), 1, field)

        expected_layers = {
            "bronze",
            "raw_vault_hubs",
            "raw_vault_links",
            "raw_vault_satellites",
            "gold",
        }
        for run in self.all_runs:
            self.assertEqual(
                set(run.get("functional_fingerprints", {})),
                expected_layers,
            )
        self.assertTrue(
            all(self.payload.get("functional_equivalence", {}).values())
        )

    def test_every_measurement_passed_quality_and_safety_gates(self):
        for run in self.all_runs:
            with self.subTest(run_id=run.get("run_id")):
                self.assertEqual(run.get("quality", {}).get("status"), "PASS")
                self.assertEqual(run.get("lineage", {}).get("status"), "PASS")
                masking = run.get("masking", {})
                self.assertEqual(masking.get("status"), "PASS")
                self.assertEqual(masking.get("failure_count"), 0)
                self.assertTrue(
                    all(
                        value == 0
                        for value in masking.get(
                            "failure_categories",
                            {},
                        ).values()
                    )
                )
                monitoring = run.get("monitoring", {})
                self.assertEqual(monitoring.get("status"), "PASS")
                self.assertGreaterEqual(
                    monitoring.get("event_count", 0),
                    MINIMUM_MONITORING_EVENTS,
                )
                shared_storage = run.get("shared_storage", {})
                self.assertEqual(shared_storage.get("status"), "PASS")
                self.assertEqual(shared_storage.get("restart_count"), 0)
                self.assertEqual(run.get("secret_findings"), 0)
                self.assertEqual(run.get("validation_failures"), [])

    def test_aggregated_metrics_recalculate_from_measurements(self):
        baseline_duration = statistics.median(
            run["duration_seconds"] for run in self.baseline_runs
        )
        scale_out_duration = statistics.median(
            run["duration_seconds"] for run in self.scale_out_runs
        )
        baseline_throughput = statistics.median(
            run["throughput_records_per_second"]
            for run in self.baseline_runs
        )
        scale_out_throughput = statistics.median(
            run["throughput_records_per_second"]
            for run in self.scale_out_runs
        )
        speedup = baseline_duration / scale_out_duration
        parallel_efficiency = speedup / 3

        self.assertClose(
            self.payload["baseline"]["median_duration_seconds"],
            baseline_duration,
        )
        self.assertClose(
            self.payload["scale_out"]["median_duration_seconds"],
            scale_out_duration,
        )
        self.assertClose(
            self.payload["baseline"][
                "median_throughput_records_per_second"
            ],
            baseline_throughput,
        )
        self.assertClose(
            self.payload["scale_out"][
                "median_throughput_records_per_second"
            ],
            scale_out_throughput,
        )
        self.assertClose(self.payload["speedup"], speedup)
        self.assertClose(
            self.payload["parallel_efficiency"],
            parallel_efficiency,
        )

        for run in self.all_runs:
            calculated = round(
                run["input_rows"] / run["duration_seconds"],
                3,
            )
            self.assertClose(
                run["throughput_records_per_second"],
                calculated,
            )

    def test_artifact_remains_publication_safe(self):
        self.assertEqual(validate_public_payload(self.payload), [])
        serialized = json.dumps(self.payload, sort_keys=True).casefold()
        private_identifiers = (
            "Data-Master-Platform" + "-SPDD",
            "Data-Master-" + "Mastery",
        )
        for identifier in private_identifiers:
            self.assertNotIn(identifier.casefold(), serialized)
        self.assertNotIn("github.com/", serialized)
        forbidden_raw_error_keys = {
            "error",
            "error_message",
            "execution_error",
            "raw_error",
            "traceback",
        }
        self.assertTrue(
            forbidden_raw_error_keys.isdisjoint(set(walk_keys(self.payload)))
        )


if __name__ == "__main__":
    unittest.main()
