import copy
import inspect
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "jobs" / "common"))
sys.path.insert(0, str(REPO_ROOT / "jobs" / "scalability"))

import run_horizontal_scalability_benchmark as benchmark  # noqa: E402
from horizontal_spark_application import (  # noqa: E402
    ALLOWED_PROFILE_DIFFERENCES,
    build_horizontal_spark_application,
    build_horizontal_storage_paths,
    horizontal_profile_differences,
    validate_horizontal_profile,
    validate_horizontal_profile_pair,
)
from runtime_profiles import build_horizontal_profile, get_runtime_profile  # noqa: E402


GIT_SHA = "a" * 40
IMAGE_DIGEST = "sha256:" + ("b" * 64)
IMAGE = "data-master-spark-jobs@" + IMAGE_DIGEST


def workload(profile_id, duration, output="sha256:functional"):
    executors = 1 if profile_id.endswith("-1") else 3
    executor_metrics = []
    for index in range(executors):
        executor_metrics.append(
            {
                "executor_id": str(index + 1),
                "host": f"10.0.0.{index + 1}",
                "status": "ACTIVE",
                "tasks": 10,
                "failed_tasks": 0,
                "runtime_ms": 1000,
                "input_bytes": 100,
                "input_records": 10,
                "output_bytes": 100,
                "output_records": 10,
                "shuffle_read_bytes": 10,
                "shuffle_write_bytes": 10,
            }
        )
    return {
        "schema_version": 1,
        "benchmark_kind": benchmark.BENCHMARK_KIND,
        "change_id": benchmark.CHANGE_ID,
        "status": "PASS",
        "benchmark_id": "hscale-test",
        "run_id": f"run-e{executors}",
        "batch_id": f"batch-e{executors}",
        "measurement_kind": "measurement",
        "repetition": 1,
        "profile_id": profile_id,
        "topology": "single-node-application-scale-out",
        "git_sha": GIT_SHA,
        "image_digest": IMAGE_DIGEST,
        "dataset_seed": 42,
        "dataset_volume": "controlled-horizontal-v1",
        "dataset_fingerprint": "sha256:dataset",
        "pipeline_contract_version": "data-master-pipeline-v1",
        "input_rows": 1000,
        "source_counts": {"clientes": 1000},
        "output_fingerprint": output,
        "functional_fingerprints": {
            "bronze": {"fingerprint": "sha256:bronze"},
            "gold": {"fingerprint": output},
        },
        "executors_requested": executors,
        "duration_seconds": duration,
        "throughput_records_per_second": benchmark.calculate_throughput(
            1000, duration
        ),
        "stage_durations": [
            {"name": "bronze", "status": "PASS", "duration_seconds": 1.0}
        ],
        "layer_counts": {
            "bronze": 1000,
            "raw_vault_hubs": 100,
            "raw_vault_links": 100,
            "raw_vault_satellites": 100,
            "gold": 100,
        },
        "quality": {"status": "PASS", "checks": {}},
        "lineage": {"status": "PASS", "checks": {}},
        "masking": {"status": "PASS", "failure_count": 0},
        "secret_findings": 0,
        "monitoring": {"status": "PASS", "event_count": 5},
        "spark": {
            "master": "k8s://https://kubernetes.default.svc",
            "executor_memory": "1g",
            "executor_instances": executors,
            "shuffle_partitions": 24,
            "dynamic_allocation": False,
        },
        "spark_api": {
            "application_id": "application-test",
            "executors": executor_metrics,
            "stages": [],
        },
    }


def observation(payload, node="minikube"):
    pods = []
    for executor in payload["spark_api"]["executors"]:
        pods.append(
            {
                "name": f"executor-{executor['executor_id']}",
                "role": "executor",
                "status": "Succeeded",
                "node": node,
                "pod_ip": executor["host"],
            }
        )
    return {
        "schema_version": 1,
        "benchmark_id": payload["benchmark_id"],
        "run_id": payload["run_id"],
        "profile_id": payload["profile_id"],
        "application_name": "test",
        "application_status": "COMPLETED",
        "executors_requested": payload["executors_requested"],
        "driver_pods": [
            {
                "name": "driver",
                "role": "driver",
                "status": "Succeeded",
                "node": node,
                "pod_ip": "10.0.0.100",
            }
        ],
        "executor_pods": pods,
        "shared_storage": {
            "status": "PASS",
            "pod_count": 1,
            "restart_count": 0,
        },
    }


def combined(profile_id, duration, output="sha256:functional"):
    payload = workload(profile_id, duration, output)
    return benchmark._combine_measurement(payload, observation(payload))


class HorizontalProfileTests(unittest.TestCase):
    def test_profiles_are_built_from_one_constructor(self):
        baseline = build_horizontal_profile(1)
        scale_out = build_horizontal_profile(3)
        self.assertEqual(
            horizontal_profile_differences(baseline, scale_out),
            ALLOWED_PROFILE_DIFFERENCES,
        )
        validate_horizontal_profile_pair(
            benchmark.BASELINE_PROFILE,
            benchmark.SCALE_OUT_PROFILE,
        )

    def test_only_id_and_executor_instances_may_differ(self):
        baseline = get_runtime_profile(benchmark.BASELINE_PROFILE)
        scale_out = get_runtime_profile(benchmark.SCALE_OUT_PROFILE)
        self.assertEqual(
            horizontal_profile_differences(baseline, scale_out),
            {"id", "spark.executor_instances"},
        )
        for field in (
            "batch",
            "dataset",
            "kubernetes",
            "quality",
            "observability",
        ):
            self.assertEqual(baseline[field], scale_out[field], field)

    def test_local_master_is_rejected(self):
        profile = get_runtime_profile(benchmark.BASELINE_PROFILE)
        profile["spark"]["master"] = "local[*]"
        with self.assertRaisesRegex(ValueError, "local"):
            validate_horizontal_profile(profile)

    def test_per_executor_resources_and_static_allocation_are_equal(self):
        profiles = [
            get_runtime_profile(benchmark.BASELINE_PROFILE),
            get_runtime_profile(benchmark.SCALE_OUT_PROFILE),
        ]
        for profile in profiles:
            self.assertEqual(profile["spark"]["executor_memory"], "1g")
            self.assertEqual(
                profile["spark"]["executor_memory_overhead"],
                "768m",
            )
            self.assertEqual(profile["kubernetes"]["executor_cores"], 1)
            self.assertEqual(
                profile["kubernetes"]["executor_core_request"],
                "750m",
            )
            self.assertFalse(profile["spark"]["dynamic_allocation"])
            self.assertEqual(profile["spark"]["shuffle_partitions"], 24)
            self.assertEqual(
                profile["kubernetes"]["minio"]["resources"]["limits"][
                    "memory"
                ],
                "2560Mi",
            )
            self.assertEqual(
                profile["kubernetes"]["minio"]["go_memory_limit"],
                "2GiB",
            )


class HorizontalAdapterTests(unittest.TestCase):
    def application(self, profile=benchmark.SCALE_OUT_PROFILE, run_id="run-1"):
        return build_horizontal_spark_application(
            profile_id=profile,
            benchmark_id="hscale-test",
            run_id=run_id,
            batch_id="batch-1",
            git_sha=GIT_SHA,
            image=IMAGE,
            image_digest=IMAGE_DIGEST,
            topology="single-node-application-scale-out",
            measurement_kind="measurement",
            repetition=1,
        )

    def test_cluster_mode_digest_non_root_and_three_executors(self):
        application = self.application()
        spec = application["spec"]
        self.assertEqual(spec["mode"], "cluster")
        self.assertEqual(spec["image"], IMAGE)
        self.assertEqual(spec["executor"]["instances"], 3)
        self.assertEqual(spec["executor"]["cores"], 1)
        self.assertEqual(spec["executor"]["memory"], "1g")
        self.assertEqual(spec["executor"]["memoryOverhead"], "768m")
        self.assertEqual(spec["driver"]["memoryOverhead"], "512m")
        self.assertEqual(spec["executor"]["coreRequest"], "750m")
        self.assertEqual(spec["executor"]["coreLimit"], "1000m")
        self.assertEqual(spec["driver"]["coreRequest"], "250m")
        self.assertFalse(spec["executor"]["securityContext"]["allowPrivilegeEscalation"])
        self.assertTrue(spec["driver"]["securityContext"]["runAsNonRoot"])
        self.assertEqual(spec["sparkConf"]["spark.dynamicAllocation.enabled"], "false")

    def test_secret_references_labels_and_isolated_s3a_paths(self):
        application = self.application()
        spec = application["spec"]
        env = {item["name"]: item for item in spec["driver"]["env"]}
        for key in (
            "SAMPLE_DATA_PATH",
            "BRONZE_PATH",
            "RAW_VAULT_PATH",
            "BUSINESS_VAULT_PATH",
            "GOLD_PATH",
            "MONITORING_PATH",
            "CHECKPOINT_PATH",
        ):
            self.assertTrue(env[key]["value"].startswith("s3a://"))
        self.assertEqual(
            env["AWS_ACCESS_KEY_ID"]["valueFrom"]["secretKeyRef"]["name"],
            "data-master-minio-secret",
        )
        self.assertEqual(
            env["SPARK_S3_USE_ENV_CREDENTIALS"]["value"],
            "true",
        )
        self.assertEqual(
            spec["executor"]["labels"]["data-master.io/run-id"],
            "run-1",
        )

    def test_run_paths_are_isolated(self):
        profile = get_runtime_profile(benchmark.BASELINE_PROFILE)
        first = build_horizontal_storage_paths(
            profile=profile,
            benchmark_id="hscale-test",
            run_id="run-1",
        )
        second = build_horizontal_storage_paths(
            profile=profile,
            benchmark_id="hscale-test",
            run_id="run-2",
        )
        self.assertTrue(set(first.values()).isdisjoint(set(second.values())))

    def test_digest_and_local_style_image_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "digest"):
            build_horizontal_spark_application(
                profile_id=benchmark.BASELINE_PROFILE,
                benchmark_id="hscale-test",
                run_id="run-1",
                batch_id="batch-1",
                git_sha=GIT_SHA,
                image="data-master-spark-jobs:latest",
                image_digest=IMAGE_DIGEST,
                topology="single-node-application-scale-out",
                measurement_kind="measurement",
                repetition=1,
            )

    def test_local_ephemeral_registry_digest_is_accepted(self):
        image = "host.minikube.internal:5000/data-master-spark-jobs@" + IMAGE_DIGEST
        application = build_horizontal_spark_application(
            profile_id=benchmark.BASELINE_PROFILE,
            benchmark_id="hscale-test",
            run_id="run-1",
            batch_id="batch-1",
            git_sha=GIT_SHA,
            image=image,
            image_digest=IMAGE_DIGEST,
            topology="single-node-application-scale-out",
            measurement_kind="measurement",
            repetition=1,
        )
        self.assertEqual(application["spec"]["image"], image)

    def test_multi_node_adds_spread_and_single_node_does_not(self):
        single = self.application()["spec"]["executor"]
        multi = build_horizontal_spark_application(
            profile_id=benchmark.SCALE_OUT_PROFILE,
            benchmark_id="hscale-test",
            run_id="run-2",
            batch_id="batch-2",
            git_sha=GIT_SHA,
            image=IMAGE,
            image_digest=IMAGE_DIGEST,
            topology="multi-node-scale-out",
            measurement_kind="measurement",
            repetition=2,
        )["spec"]["executor"]
        self.assertNotIn("affinity", single)
        self.assertIn("podAntiAffinity", multi["affinity"])


class HorizontalMetricAndClassificationTests(unittest.TestCase):
    def test_median_throughput_speedup_and_efficiency(self):
        self.assertEqual(benchmark.median([12, 10, 11]), 11.0)
        self.assertEqual(benchmark.calculate_throughput(1100, 11), 100.0)
        self.assertEqual(benchmark.calculate_speedup(12, 6), 2.0)
        self.assertEqual(benchmark.calculate_parallel_efficiency(2, 3), 0.667)

    def test_pass_uses_three_runs_and_median_benefit(self):
        baseline = [
            combined(benchmark.BASELINE_PROFILE, value)
            for value in (12, 10, 11)
        ]
        scale_out = [
            combined(benchmark.SCALE_OUT_PROFILE, value)
            for value in (7, 6, 8)
        ]
        warmup = combined(benchmark.BASELINE_PROFILE, 20)
        result = benchmark.evaluate_benchmark_result(
            baseline_runs=baseline,
            scale_out_runs=scale_out,
            warmup=warmup,
        )
        self.assertEqual(result["result"], "PASS")
        self.assertEqual(result["baseline"]["median_duration_seconds"], 11.0)
        self.assertEqual(result["scale_out"]["median_duration_seconds"], 7.0)

    def test_inconclusive_is_not_promoted(self):
        baseline = [
            combined(benchmark.BASELINE_PROFILE, value)
            for value in (10, 11, 12)
        ]
        scale_out = [
            combined(benchmark.SCALE_OUT_PROFILE, value)
            for value in (12, 13, 14)
        ]
        result = benchmark.evaluate_benchmark_result(
            baseline_runs=baseline,
            scale_out_runs=scale_out,
            warmup=combined(benchmark.BASELINE_PROFILE, 20),
        )
        self.assertEqual(result["result"], "INCONCLUSIVE")
        self.assertFalse(result["measurable_benefit"])

    def test_functional_divergence_is_fail(self):
        baseline = [
            combined(benchmark.BASELINE_PROFILE, value)
            for value in (10, 11, 12)
        ]
        scale_out = [
            combined(benchmark.SCALE_OUT_PROFILE, value, "sha256:different")
            for value in (5, 6, 7)
        ]
        result = benchmark.evaluate_benchmark_result(
            baseline_runs=baseline,
            scale_out_runs=scale_out,
            warmup=combined(benchmark.BASELINE_PROFILE, 20),
        )
        self.assertEqual(result["result"], "FAIL")
        self.assertIn(
            "functional_equivalence:output_fingerprint",
            result["failures"],
        )

    def test_exit_codes_are_distinct(self):
        self.assertEqual(
            len(
                {
                    benchmark.EXIT_PASS,
                    benchmark.EXIT_INCONCLUSIVE,
                    benchmark.EXIT_FAIL,
                    benchmark.EXIT_HARNESS_ERROR,
                    benchmark.EXIT_BLOCKED,
                }
            ),
            5,
        )


class HorizontalEvidenceTests(unittest.TestCase):
    def test_spark_status_api_excludes_raw_stage_names_and_keeps_duration(self):
        class SparkContext:
            uiWebUrl = "http://spark-ui"
            applicationId = "application-test"

        class Spark:
            sparkContext = SparkContext()

        executors = [
            {
                "id": "1",
                "hostPort": "10.0.0.1:1234",
                "isActive": True,
                "totalTasks": 4,
                "failedTasks": 0,
                "totalDuration": 1200,
                "totalInputBytes": 10,
                "totalShuffleRead": 20,
                "totalShuffleWrite": 30,
            }
        ]
        stages = [
            {
                "stageId": 7,
                "attemptId": 0,
                "name": "/opt/spark/work-dir/jobs/private.py:42",
                "status": "COMPLETE",
                "submissionTime": "2026-07-28T02:00:00.000GMT",
                "completionTime": "2026-07-28T02:00:01.250GMT",
                "executorRunTime": 1100,
                "numTasks": 4,
                "inputRecords": 10,
                "outputRecords": 10,
                "shuffleReadBytes": 20,
                "shuffleWriteBytes": 30,
                "executorSummary": {
                    "1": {
                        "inputRecords": 10,
                        "outputBytes": 40,
                        "outputRecords": 10,
                    }
                },
            }
        ]
        with patch.object(
            benchmark,
            "_status_api_json",
            side_effect=[executors, stages],
        ):
            evidence = benchmark._collect_spark_status_api(Spark())

        self.assertEqual(evidence["stages"][0]["duration_ms"], 1250)
        self.assertEqual(evidence["stages"][0]["executor_runtime_ms"], 1100)
        self.assertNotIn("name", evidence["stages"][0])
        self.assertEqual(
            benchmark.validate_public_horizontal_payload(evidence),
            [],
        )

    def test_fingerprint_combines_count_and_hash_in_one_spark_action(self):
        source = inspect.getsource(benchmark._table_fingerprint)
        self.assertNotIn("frame.count()", source)
        self.assertIn("functions.count(functions.lit(1))", source)
        self.assertEqual(source.count(".first()"), 2)

    def test_executor_configured_but_not_observed_fails(self):
        payload = workload(benchmark.SCALE_OUT_PROFILE, 6)
        observed = observation(payload)
        observed["executor_pods"].pop()
        combined_result = benchmark._combine_measurement(payload, observed)
        self.assertEqual(combined_result["status"], "FAIL")
        self.assertIn(
            "executor_pod_count_mismatch",
            combined_result["validation_failures"],
        )

    def test_tasks_concentrated_in_one_executor_fails(self):
        payload = workload(benchmark.SCALE_OUT_PROFILE, 6)
        for executor in payload["spark_api"]["executors"][1:]:
            executor["tasks"] = 0
        combined_result = benchmark._combine_measurement(
            payload,
            observation(payload),
        )
        self.assertEqual(combined_result["status"], "FAIL")
        self.assertIn(
            "tasks_not_distributed",
            combined_result["validation_failures"],
        )

    def test_multi_node_requires_two_executor_nodes(self):
        payload = workload(benchmark.SCALE_OUT_PROFILE, 6)
        payload["topology"] = "multi-node-scale-out"
        combined_result = benchmark._combine_measurement(
            payload,
            observation(payload, node="only-node"),
        )
        self.assertIn(
            "multi_node_topology_not_observed",
            combined_result["validation_failures"],
        )

    def test_recursive_sanitization_rejects_paths_pii_and_private_names(self):
        unsafe_values = (
            {"nested": ["file:///tmp/input"]},
            {"nested": "s3a://lakehouse/full/path"},
            {"nested": "C:\\" + r"Users\person\input"},
            {"nested": "123.456.789-00"},
            {"nested": "person@example.com"},
            {"nested": "Data-Master-" + "Mastery"},
        )
        for value in unsafe_values:
            with self.subTest(value=value):
                self.assertTrue(
                    benchmark.validate_public_horizontal_payload(value)
                )

    def test_incomplete_payload_fails_closed(self):
        payload = workload(benchmark.BASELINE_PROFILE, 10)
        del payload["output_fingerprint"]
        failures = benchmark._validate_workload_payload(payload)
        self.assertIn("missing:output_fingerprint", failures)

    def test_run_plan_has_one_warmup_and_six_balanced_measurements(self):
        with tempfile.TemporaryDirectory() as work:
            output = Path(work) / "plan.json"
            args = type(
                "Args",
                (),
                {
                    "benchmark_id": "hscale-test",
                    "topology": "single-node-application-scale-out",
                    "output": str(output),
                },
            )()
            self.assertEqual(benchmark.write_run_plan(args), 0)
            plan = json.loads(output.read_text(encoding="utf-8"))
        warmups = [
            run for run in plan["runs"] if run["measurement_kind"] == "warmup"
        ]
        measurements = [
            run
            for run in plan["runs"]
            if run["measurement_kind"] == "measurement"
        ]
        self.assertEqual(len(warmups), 1)
        self.assertEqual(len(measurements), 6)
        self.assertEqual(plan["infrastructure"]["minikube"]["cpus"], 4)
        self.assertEqual(
            plan["infrastructure"]["minio"]["resources"]["limits"]["memory"],
            "2560Mi",
        )
        self.assertEqual(
            sum(
                run["profile_id"] == benchmark.BASELINE_PROFILE
                for run in measurements
            ),
            3,
        )

    def test_business_jobs_do_not_branch_on_horizontal_profile_names(self):
        for directory in (
            REPO_ROOT / "jobs" / "bronze",
            REPO_ROOT / "jobs" / "raw_vault",
            REPO_ROOT / "jobs" / "business_vault",
        ):
            for path in directory.glob("*.py"):
                source = path.read_text(encoding="utf-8")
                self.assertNotIn("minikube-horizontal-", source, str(path))

    def test_orchestrator_contract_is_fail_closed_and_run_scoped(self):
        source = (
            REPO_ROOT
            / "scripts"
            / "minikube"
            / "Invoke-HorizontalScalingBenchmark.ps1"
        ).read_text(encoding="utf-8")
        self.assertIn("Git worktree must be clean", source)
        self.assertIn("Docker engine is unavailable", source)
        self.assertLess(
            source.index("dockerMemoryText"),
            source.index("profileInventoryText"),
        )
        self.assertIn("Target Minikube profile already exists", source)
        self.assertIn("data-master.io/run-id=", source)
        self.assertIn("data-master.io/run-id=$($Run.run_id)", source)
        self.assertIn("minikube delete --profile $Profile", source)
        self.assertIn("host.minikube.internal:5000", source)
        self.assertIn(
            "ghcr.io/kubeflow/spark-operator/controller:2.5.0",
            source,
        )
        self.assertIn("docker container rm --force", source)
        self.assertIn("$secretAttempt -le 3", source)
        self.assertIn("Invoke-HorizontalKubernetesRead", source)
        self.assertIn("$readAttempt -le 5", source)
        self.assertIn("Get-HorizontalMinioObservation", source)
        self.assertIn("MinIO must remain ready with zero restarts", source)
        self.assertIn("workload_failure_type=", source)
        self.assertIn("validation_failures", source)
        self.assertIn("$state -in $terminalFailureStates", source)
        self.assertIn("resources.limits.memory=", source)
        self.assertIn("extraEnv[0].name=GOMEMLIMIT", source)
        self.assertIn("go_memory_limit", source)
        self.assertIn("$plan.infrastructure.minio", source)
        self.assertIn("$script:HorizontalExitBlocked = 5", source)
        self.assertNotIn("local[*]", source)

    def test_shared_storage_restart_fails_evidence(self):
        payload = workload(benchmark.BASELINE_PROFILE, 10)
        observed = observation(payload)
        observed["shared_storage"]["restart_count"] = 1
        observed["shared_storage"]["status"] = "FAIL"
        combined_result = benchmark._combine_measurement(payload, observed)
        self.assertEqual(combined_result["status"], "FAIL")
        self.assertIn(
            "shared_storage_restart_observed",
            combined_result["validation_failures"],
        )

    def test_spark_rbac_can_patch_executor_pods(self):
        source = (
            REPO_ROOT
            / "infra"
            / "workloads"
            / "spark-apps"
            / "rbac"
            / "spark-rbac.yaml"
        ).read_text(encoding="utf-8")
        self.assertIn('"patch"', source)


if __name__ == "__main__":
    unittest.main()
