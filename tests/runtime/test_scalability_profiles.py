import io
import json
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import call, patch


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from jobs.scalability import run_scalability_benchmark as benchmark  # noqa: E402


def _valid_run(
    profile_name,
    source_records,
    pipeline_duration,
    total_duration,
    driver_memory,
    executor_memory,
    shuffle_partitions,
    throughput=None,
):
    contract = benchmark.public_profile_contract(profile_name)
    volume = contract["configured_volume"]
    fixed_source_records = (
        volume["clientes"]
        + volume["agencias"]
        + volume["produtos"]
        + volume["transacoes"]
        + volume["eventos_digitais_file"]
    )
    variable_source_records = source_records - fixed_source_records
    account_count = max(
        volume["clientes"],
        (
            variable_source_records
            + volume["cards_per_account"]
        )
        // (volume["cards_per_account"] + 1),
    )
    card_count = variable_source_records - account_count
    if not (
        volume["clientes"]
        <= account_count
        <= volume["clientes"] * volume["accounts_per_client"]
        and 1 <= card_count <= account_count * volume["cards_per_account"]
    ):
        raise ValueError("test fixture source volume is outside profile bounds")

    by_source = {
        "clientes": volume["clientes"],
        "contas": account_count,
        "cartoes": card_count,
        "transacoes": volume["transacoes"],
        "eventos_digitais": volume["eventos_digitais_file"],
        "agencias": volume["agencias"],
        "produtos": volume["produtos"],
    }

    def duration_parts(duration, count):
        parts = [
            benchmark._round_metric(float(duration) / count)
            for _ in range(count - 1)
        ]
        parts.append(
            benchmark._round_metric(float(duration) - sum(parts))
        )
        return parts

    validation_duration = total_duration - pipeline_duration
    stage_durations = [
        *duration_parts(
            pipeline_duration,
            len(benchmark.PIPELINE_STAGE_NAMES),
        ),
        *duration_parts(
            validation_duration,
            len(benchmark.VALIDATION_STAGE_NAMES),
        ),
    ]
    stages = [
        {
            "name": name,
            "status": "SUCCESS",
            "duration_seconds": duration,
        }
        for name, duration in zip(
            benchmark.EXPECTED_STAGE_NAMES,
            stage_durations,
        )
    ]
    pipeline_throughput = (
        throughput
        if throughput is not None
        else benchmark.calculate_throughput(source_records, pipeline_duration)
    )
    layer_counts = {
        "bronze": source_records,
        "raw_vault_hubs": max(1, source_records // 4),
        "raw_vault_links": max(1, source_records // 3),
        "raw_vault_satellites": max(1, source_records // 2),
        "gold": max(1, source_records // 5),
    }
    monitoring_summary = [
        {
            "pipeline_name": pipeline_name,
            "task_name": task_name,
            "status": "SUCCESS",
            "rows_read": 0,
            "rows_written": layer_counts[layer_name],
            "duration_seconds": 1.0,
        }
        for pipeline_name, task_name, layer_name
        in benchmark.EXPECTED_MONITORING_EVENTS
    ]
    return {
        "schema_version": benchmark.SCHEMA_VERSION,
        "runtime_profile": profile_name,
        "status": "SUCCESS",
        "configured_volume": contract["configured_volume"],
        "observed_source_records": {
            "by_source": by_source,
            "total": source_records,
        },
        "spark": {
            "version": "3.3.1",
            "configured": {
                "master": "local[*]",
                "driver_memory": driver_memory,
                "executor_memory": executor_memory,
                "executor_instances": 1,
                "shuffle_partitions": shuffle_partitions,
                "adaptive_enabled": True,
            },
            "observed": {
                "master": "local[*]",
                "default_parallelism": 4,
                "driver_memory": driver_memory,
                "executor_memory": executor_memory,
                "executor_instances": "1",
                "shuffle_partitions": str(shuffle_partitions),
                "delta_snapshot_partitions": str(shuffle_partitions),
                "adaptive_enabled": "true",
            },
        },
        "resources": {
            "configured": contract["resources_configured"],
            "observed": {
                "master": "local[*]",
                "default_parallelism": 4,
                "separate_executor_processes": False,
                "interpretation": (
                    benchmark.LOCAL_RESOURCE_OBSERVED_INTERPRETATION
                ),
            },
        },
        "partitions": {
            "configured": {
                "shuffle_partitions": shuffle_partitions,
                "delta_snapshot_partitions": shuffle_partitions,
            },
            "observed": {
                "shuffle_partitions": str(shuffle_partitions),
                "delta_snapshot_partitions": str(shuffle_partitions),
                "default_parallelism": 4,
            },
            "tables": {},
        },
        "stages": stages,
        "durations_seconds": {
            "pipeline": pipeline_duration,
            "validation": validation_duration,
            "total": total_duration,
            "unattributed_overhead": 0.0,
            "by_stage": {
                stage["name"]: stage["duration_seconds"] for stage in stages
            },
        },
        "throughput": {
            "basis": "observed_source_records",
            "record_count": source_records,
            "pipeline_records_per_second": pipeline_throughput,
            "end_to_end_records_per_second": benchmark.calculate_throughput(
                source_records, total_duration
            ),
        },
        "layer_counts": layer_counts,
        "quality": {
            "status": "PASS",
            "checks": {"lineage": "PASS", "gold_lineage": "PASS"},
            "failed_checks": [],
        },
        "masking": {
            "status": "PASS",
            "failure_count": 0,
            "failure_categories": {},
            "tables_checked": 7,
        },
        "monitoring": {
            "status": "READABLE",
            "event_count": len(monitoring_summary),
            "summary": monitoring_summary,
        },
        "observed_bottlenecks": benchmark.build_observed_bottlenecks(stages),
        "validation_failures": [],
        "limitations": list(benchmark.CLAIM_LIMITS),
    }


class ScalabilityProfileContractTests(unittest.TestCase):
    def test_module_import_does_not_import_pyspark(self):
        self.assertNotIn("pyspark", benchmark.__dict__)
        self.assertFalse(hasattr(benchmark, "SparkSession"))

    def test_only_local_small_and_medium_are_executable(self):
        self.assertEqual(
            benchmark.validate_requested_profiles(
                ["local-small", "local-medium"]
            ),
            ("local-small", "local-medium"),
        )
        for rejected in (
            "cloud-ready",
            "presentation-demo",
            "minikube-integration",
            "unknown",
        ):
            with self.subTest(profile=rejected):
                with self.assertRaisesRegex(ValueError, "not executable"):
                    benchmark.validate_requested_profiles([rejected])

    def test_empty_and_duplicate_profile_requests_fail(self):
        with self.assertRaisesRegex(ValueError, "At least one"):
            benchmark.validate_requested_profiles([])
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            benchmark.validate_requested_profiles(
                ["local-small", "local-small"]
            )

    def test_medium_profile_expands_volume_memory_and_partitions(self):
        small = benchmark.public_profile_contract("local-small")
        medium = benchmark.public_profile_contract("local-medium")
        self.assertGreater(
            medium["configured_volume"]["transacoes"],
            small["configured_volume"]["transacoes"],
        )
        self.assertGreater(
            benchmark.parse_memory_mib(
                medium["spark_configured"]["driver_memory"]
            ),
            benchmark.parse_memory_mib(
                small["spark_configured"]["driver_memory"]
            ),
        )
        self.assertGreater(
            medium["spark_configured"]["shuffle_partitions"],
            small["spark_configured"]["shuffle_partitions"],
        )
        self.assertEqual(small["spark_configured"]["master"], "local[*]")
        self.assertEqual(medium["spark_configured"]["master"], "local[*]")
        self.assertFalse(
            medium["resources_configured"]["separate_executor_processes"]
        )
        self.assertIsNone(medium["resources_configured"]["executor_cores"])

    def test_cloud_ready_is_reference_only_and_not_executed(self):
        reference = benchmark.cloud_ready_reference()
        self.assertEqual(reference["status"], "REFERENCE_ONLY")
        self.assertFalse(reference["executed"])
        self.assertEqual(reference["submission_mode"], "reference-only")

    def test_spark_configuration_projection_is_allowlisted(self):
        projected = benchmark.public_profile_contract("local-small")
        self.assertEqual(
            set(projected["spark_configured"]),
            set(benchmark.SAFE_SPARK_KEYS),
        )
        rendered = json.dumps(projected)
        self.assertNotIn("access.key", rendered)
        self.assertNotIn("secret.key", rendered)
        self.assertNotIn("minioadmin", rendered)


class ScalabilityMetricTests(unittest.TestCase):
    def test_throughput_has_explicit_zero_behavior(self):
        self.assertEqual(benchmark.calculate_throughput(100, 4), 25.0)
        self.assertEqual(benchmark.calculate_throughput(100, 0), 0.0)
        self.assertEqual(benchmark.calculate_throughput(0, 4), 0.0)
        self.assertEqual(benchmark.calculate_throughput("invalid", 4), 0.0)

    def test_memory_parser_supports_profile_units(self):
        self.assertEqual(benchmark.parse_memory_mib("768m"), 768.0)
        self.assertEqual(benchmark.parse_memory_mib("2g"), 2048.0)
        self.assertIsNone(benchmark.parse_memory_mib("invalid"))

    def test_bottleneck_selects_slowest_and_first_on_tie(self):
        stages = [
            {"name": "bronze", "status": "SUCCESS", "duration_seconds": 4},
            {"name": "hubs", "status": "SUCCESS", "duration_seconds": 9},
            {"name": "links", "status": "SUCCESS", "duration_seconds": 9},
            {"name": "masking_gate", "status": "SUCCESS", "duration_seconds": 12},
        ]
        overall = benchmark.select_observed_bottleneck(stages)
        processing = benchmark.select_observed_bottleneck(
            stages, benchmark.PROCESSING_STAGE_NAMES
        )
        tied = benchmark.select_observed_bottleneck(
            stages[:3], benchmark.PROCESSING_STAGE_NAMES
        )
        self.assertEqual(overall["stage"], "masking_gate")
        self.assertEqual(processing["stage"], "hubs")
        self.assertEqual(tied["stage"], "hubs")
        self.assertEqual(
            benchmark.select_observed_bottleneck([])["status"],
            "UNAVAILABLE",
        )

    def test_profile_status_is_fail_closed(self):
        result = _valid_run(
            "local-small", 400, 20, 30, "768m", "768m", 2
        )
        status, failures = benchmark.evaluate_profile_status(result)
        self.assertEqual(status, "SUCCESS")
        self.assertEqual(failures, [])

        cases = {
            "stage": lambda item: item["stages"][0].update(status="FAILURE"),
            "count": lambda item: item["layer_counts"].update(gold=0),
            "quality": lambda item: item["quality"].update(status="FAILED"),
            "masking": lambda item: item["masking"].update(
                status="FAILURE", failure_count=1
            ),
            "monitoring": lambda item: item["monitoring"].update(event_count=4),
        }
        for name, mutation in cases.items():
            with self.subTest(failure=name):
                candidate = json.loads(json.dumps(result))
                mutation(candidate)
                status, failures = benchmark.evaluate_profile_status(candidate)
                self.assertEqual(status, "FAILURE")
                self.assertTrue(failures)

    def test_profile_status_rejects_volume_not_generated_from_profile(self):
        result = _valid_run(
            "local-small", 400, 20, 30, "768m", "768m", 2
        )
        cases = {
            "missing_source": (
                lambda item: item["observed_source_records"]["by_source"].pop(
                    "produtos"
                ),
                "observed_source_keys_mismatch",
            ),
            "sum_mismatch": (
                lambda item: item["observed_source_records"].update(total=401),
                "observed_source_total_mismatch",
            ),
            "configured_exact_mismatch": (
                lambda item: item["observed_source_records"]["by_source"].update(
                    clientes=21
                ),
                "configured_source_volume_mismatch:clientes",
            ),
            "accounts_outside_profile": (
                lambda item: item["observed_source_records"]["by_source"].update(
                    contas=19
                ),
                "configured_source_volume_mismatch:contas",
            ),
            "cards_outside_profile": (
                lambda item: item["observed_source_records"]["by_source"].update(
                    cartoes=999
                ),
                "configured_source_volume_mismatch:cartoes",
            ),
        }
        for name, (mutation, expected) in cases.items():
            with self.subTest(failure=name):
                candidate = json.loads(json.dumps(result))
                mutation(candidate)
                status, failures = benchmark.evaluate_profile_status(candidate)
                self.assertEqual(status, "FAILURE")
                self.assertIn(expected, failures)

    def test_semantic_gate_rejects_observed_spark_mismatch(self):
        result = _valid_run(
            "local-small", 400, 20, 30, "768m", "768m", 2
        )
        result["spark"]["observed"]["master"] = "local[1]"
        failures = benchmark.validate_profile_result_semantics(
            result,
            "local-small",
        )
        self.assertIn("spark_observed_mismatch:master", failures)

    def test_semantic_gate_rejects_internal_inconsistencies(self):
        result = _valid_run(
            "local-small", 400, 20, 30, "768m", "768m", 2
        )
        cases = {
            "duplicate_stage": (
                lambda item: item["stages"].append(dict(item["stages"][-1])),
                "stage_sequence_mismatch",
            ),
            "quality_failures": (
                lambda item: item["quality"]["failed_checks"].append(
                    "lineage"
                ),
                "quality_failed_checks_not_empty",
            ),
            "masking_totals": (
                lambda item: item["masking"]["failure_categories"].update(
                    cpf=1
                ),
                "masking_failure_count_mismatch",
            ),
            "monitoring_summary": (
                lambda item: item["monitoring"]["summary"].pop(),
                "monitoring_summary_mismatch",
            ),
            "monitoring_status": (
                lambda item: item["monitoring"]["summary"][0].update(
                    status="FAILURE"
                ),
                "monitoring_event_failed",
            ),
            "nonfinite_metric": (
                lambda item: item["throughput"].update(
                    pipeline_records_per_second=float("nan")
                ),
                "nonfinite_number:$.throughput.pipeline_records_per_second",
            ),
            "pipeline_duration": (
                lambda item: item["durations_seconds"].update(
                    pipeline=999.0
                ),
                "pipeline_duration_mismatch",
            ),
            "stage_duration_map": (
                lambda item: item["durations_seconds"].update(by_stage={}),
                "stage_duration_map_mismatch",
            ),
            "throughput_basis": (
                lambda item: item["throughput"].update(basis="invented"),
                "throughput_basis_mismatch",
            ),
            "throughput_record_count": (
                lambda item: item["throughput"].update(record_count=999999),
                "throughput_record_count_mismatch",
            ),
            "throughput_rate": (
                lambda item: item["throughput"].update(
                    pipeline_records_per_second=999999.0
                ),
                "pipeline_throughput_mismatch",
            ),
            "observed_bottleneck": (
                lambda item: item.update(observed_bottlenecks={}),
                "observed_bottlenecks_mismatch",
            ),
            "resource_observation": (
                lambda item: item["resources"]["observed"].update(
                    master="k8s://invalid",
                    separate_executor_processes=True,
                ),
                "resource_observation_mismatch",
            ),
            "partition_configuration": (
                lambda item: item["partitions"]["configured"].update(
                    shuffle_partitions=999
                ),
                "partition_configuration_mismatch",
            ),
            "claim_limits": (
                lambda item: item.update(limitations=[]),
                "claim_limits_mismatch",
            ),
            "monitoring_event_set": (
                lambda item: item["monitoring"].update(
                    summary=[
                        dict(item["monitoring"]["summary"][0])
                        for _ in range(
                            len(benchmark.EXPECTED_MONITORING_EVENTS)
                        )
                    ]
                ),
                "monitoring_event_set_mismatch",
            ),
        }
        for name, (mutation, expected) in cases.items():
            with self.subTest(failure=name):
                candidate = json.loads(json.dumps(result))
                mutation(candidate)
                failures = benchmark.validate_profile_result_semantics(
                    candidate,
                    "local-small",
                )
                self.assertIn(expected, failures)

    def test_comparison_does_not_require_linear_speedup(self):
        small = _valid_run(
            "local-small",
            400,
            pipeline_duration=10,
            total_duration=20,
            driver_memory="768m",
            executor_memory="768m",
            shuffle_partitions=2,
            throughput=40,
        )
        medium = _valid_run(
            "local-medium",
            17000,
            pipeline_duration=100,
            total_duration=150,
            driver_memory="2g",
            executor_memory="2g",
            shuffle_partitions=8,
            throughput=30,
        )
        comparison = benchmark.compare_profile_runs([small, medium])
        status, failures = benchmark.benchmark_status(
            ["local-small", "local-medium"], [small, medium]
        )
        self.assertEqual(status, "SUCCESS")
        self.assertEqual(failures, [])
        self.assertEqual(comparison["status"], "VALID")
        self.assertFalse(comparison["linear_speedup_required"])
        self.assertFalse(comparison["performance_threshold_applied"])
        self.assertFalse(comparison["horizontal_scaling_demonstrated"])
        self.assertLess(comparison["pipeline_throughput_ratio"], 1)

    def test_missing_or_failed_profile_fails_aggregate(self):
        small = _valid_run(
            "local-small", 400, 20, 30, "768m", "768m", 2
        )
        status, failures = benchmark.benchmark_status(
            ["local-small", "local-medium"], [small]
        )
        self.assertEqual(status, "FAILURE")
        self.assertIn("profile_result_missing:local-medium", failures)
        failed_medium = _valid_run(
            "local-medium", 17000, 100, 150, "2g", "2g", 8
        )
        failed_medium["status"] = "FAILURE"
        status, failures = benchmark.benchmark_status(
            ["local-small", "local-medium"], [small, failed_medium]
        )
        self.assertEqual(status, "FAILURE")
        self.assertIn("profile_failed:local-medium", failures)


class ScalabilityPayloadSafetyTests(unittest.TestCase):
    def test_valid_profile_result_matches_schema_and_public_contract(self):
        result = _valid_run(
            "local-small", 400, 20, 30, "768m", "768m", 2
        )
        self.assertEqual(benchmark.validate_profile_result_schema(result), [])
        self.assertEqual(benchmark.validate_public_payload(result), [])

    def test_schema_rejects_missing_fields(self):
        result = _valid_run(
            "local-small", 400, 20, 30, "768m", "768m", 2
        )
        del result["masking"]
        self.assertIn(
            "missing_field:masking",
            benchmark.validate_profile_result_schema(result),
        )

    def test_payload_rejects_paths_credentials_and_samples(self):
        windows_user_path = "C:" + "\\Users\\person\\runtime"
        unsafe_values = [
            {"work_dir": "relative"},
            {"message": "file:///private/runtime"},
            {"message": windows_user_path},
            {"message": "/tmp/private/runtime"},
            {"spark": {"spark.hadoop.fs.s3a.secret_key": "value"}},
            {"sample": "123.456.789-00"},
            {"sample": "person@example.com"},
            {"sample": "1111-2222-3333-4444"},
        ]
        for payload in unsafe_values:
            with self.subTest(payload=payload):
                self.assertTrue(benchmark.validate_public_payload(payload))

    def test_error_sanitizer_removes_known_paths(self):
        windows_user_path = "C:" + "\\Users\\person\\runtime"
        error = RuntimeError(
            f"failed at {REPO_ROOT / 'jobs'} and {windows_user_path}"
        )
        rendered = benchmark.sanitize_error_message(error)
        self.assertNotIn(str(REPO_ROOT), rendered)
        self.assertNotIn("C:" + "\\Users", rendered)
        self.assertIn("<REDACTED_PATH>", rendered)

    def test_worker_result_is_revalidated_for_the_expected_profile(self):
        wrong_profile = _valid_run(
            "local-medium", 17000, 100, 150, "2g", "2g", 8
        )
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "result.json"
            benchmark._write_json(path, wrong_profile)
            result = benchmark._read_profile_result(path, "local-small")

        self.assertEqual(result["status"], "FAILURE")
        self.assertIn(
            "worker_result_contract_failed",
            result["validation_failures"],
        )

    def test_worker_result_rejects_observed_spark_mismatch(self):
        mismatched = _valid_run(
            "local-small", 400, 20, 30, "768m", "768m", 2
        )
        mismatched["spark"]["observed"]["driver_memory"] = "8g"
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "result.json"
            benchmark._write_json(path, mismatched)
            result = benchmark._read_profile_result(path, "local-small")

        self.assertEqual(result["status"], "FAILURE")
        self.assertIn(
            "worker_result_contract_failed",
            result["validation_failures"],
        )

    def test_worker_result_rejects_invented_metrics_and_claims(self):
        mismatched = _valid_run(
            "local-small", 400, 20, 30, "768m", "768m", 2
        )
        mismatched["durations_seconds"]["pipeline"] = 999.0
        mismatched["throughput"]["pipeline_records_per_second"] = 999999.0
        mismatched["resources"]["observed"][
            "separate_executor_processes"
        ] = True
        mismatched["limitations"] = []
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "result.json"
            benchmark._write_json(path, mismatched)
            result = benchmark._read_profile_result(path, "local-small")

        self.assertEqual(result["status"], "FAILURE")
        self.assertIn(
            "worker_result_contract_failed",
            result["validation_failures"],
        )

    def test_malformed_worker_result_returns_safe_stub(self):
        malformed = _valid_run(
            "local-small", 400, 20, 30, "768m", "768m", 2
        )
        malformed["spark"] = {"configured": None, "observed": []}
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "result.json"
            benchmark._write_json(path, malformed)
            result = benchmark._read_profile_result(path, "local-small")

        self.assertEqual(result["status"], "FAILURE")
        self.assertEqual(
            benchmark.validate_public_payload(result),
            [],
        )

    def test_unsafe_aggregate_is_replaced_before_marker_or_artifact(self):
        windows_user_path = "C:" + "\\Users\\person\\runtime"
        unsafe = _valid_run(
            "local-small", 400, 20, 30, "768m", "768m", 2
        )
        unsafe["unsafe_message"] = windows_user_path

        with tempfile.TemporaryDirectory() as temp:
            with patch.object(
                benchmark,
                "run_profile_subprocess",
                return_value=unsafe,
            ):
                with redirect_stdout(io.StringIO()) as output:
                    payload, return_code = benchmark.orchestrate_benchmark(
                        runtime_profiles=("local-small",),
                        work_dir=Path(temp),
                    )

        self.assertEqual(return_code, 1)
        self.assertEqual(payload["status"], "FAILURE")
        self.assertEqual(benchmark.validate_public_payload(payload), [])
        self.assertNotIn(windows_user_path, output.getvalue())
        self.assertIn(
            "public_payload_contract_failed",
            payload["validation_failures"],
        )


class ScalabilitySubprocessTests(unittest.TestCase):
    def test_worker_environment_removes_profile_overrides(self):
        environment = benchmark.build_worker_environment(
            "local-small",
            {
                "PATH": "safe",
                "PYTHONPATH": "unsafe-python-path",
                "SPARK_MASTER": "k8s://invalid",
                "BRONZE_PATH": "file:///invalid",
                "RUNTIME_PROFILE": "cloud-ready",
                "SPARK_IVY_DIR": "file:///invalid",
                "SPARK_JARS_PACKAGES": "unsafe:package:1.0",
                "PYSPARK_SUBMIT_ARGS": "--master k8s://invalid",
                "SPARK_CONF_DIR": "unsafe-spark-conf",
                "HADOOP_CONF_DIR": "unsafe-hadoop-conf",
                "JAVA_TOOL_OPTIONS": "-javaagent:unsafe.jar",
                "AWS_ACCESS_KEY_ID": "unsafe",
                "AWS_SECRET_ACCESS_KEY": "unsafe",
                "AZURE_CLIENT_SECRET": "unsafe",
                "GOOGLE_APPLICATION_CREDENTIALS": "unsafe.json",
                "MINIO_ENDPOINT": "remote.example:9000",
                "MINIO_ACCESS_KEY": "unsafe",
                "MINIO_SECRET_KEY": "unsafe",
            },
            ivy_dir=Path("isolated-ivy"),
        )
        self.assertEqual(environment["PATH"], "safe")
        self.assertEqual(environment["RUNTIME_PROFILE"], "local-small")
        self.assertEqual(environment["DM_RUNTIME_PROFILE"], "local-small")
        self.assertNotIn("SPARK_MASTER", environment)
        self.assertNotIn("BRONZE_PATH", environment)
        self.assertNotIn("PYTHONPATH", environment)
        self.assertNotIn("PYSPARK_SUBMIT_ARGS", environment)
        self.assertNotIn("SPARK_CONF_DIR", environment)
        self.assertNotIn("HADOOP_CONF_DIR", environment)
        self.assertNotIn("AWS_ACCESS_KEY_ID", environment)
        self.assertNotIn("AWS_SECRET_ACCESS_KEY", environment)
        self.assertNotIn("AZURE_CLIENT_SECRET", environment)
        self.assertNotIn("GOOGLE_APPLICATION_CREDENTIALS", environment)
        self.assertEqual(environment["SPARK_JARS_PACKAGES"], "")
        self.assertEqual(environment["SPARK_IVY_DIR"], "isolated-ivy")
        self.assertEqual(
            environment["JAVA_TOOL_OPTIONS"],
            "-XX:-UseContainerSupport -Duser.home=/tmp",
        )
        self.assertEqual(environment["MINIO_ENDPOINT"], "127.0.0.1:9")
        self.assertEqual(
            environment["MINIO_ACCESS_KEY"],
            "benchmark-local-access",
        )
        self.assertEqual(
            environment["MINIO_SECRET_KEY"],
            "benchmark-local-secret",
        )

    def test_orchestrator_uses_distinct_clean_subprocesses(self):
        calls = []

        def fake_run(
            command,
            cwd,
            environment,
            timeout_seconds,
            log_path,
        ):
            profile = command[command.index("--_worker-profile") + 1]
            result_path = Path(
                command[command.index("--_worker-result-path") + 1]
            )
            work_dir = command[command.index("--_worker-work-dir") + 1]
            calls.append(
                {
                    "command": list(command),
                    "cwd": cwd,
                    "env": dict(environment),
                    "work_dir": work_dir,
                    "result_path": str(result_path),
                    "timeout": timeout_seconds,
                    "log_path": str(log_path),
                }
            )
            if profile == "local-small":
                result = _valid_run(
                    profile, 400, 20, 30, "768m", "768m", 2
                )
            else:
                result = _valid_run(
                    profile, 17000, 100, 150, "2g", "2g", 8
                )
            benchmark._write_json(result_path, result)
            return 0

        with tempfile.TemporaryDirectory() as temp:
            with patch.object(
                benchmark,
                "_run_worker_process",
                side_effect=fake_run,
            ):
                with redirect_stdout(io.StringIO()) as output:
                    payload, return_code = benchmark.orchestrate_benchmark(
                        runtime_profiles=("local-small", "local-medium"),
                        work_dir=Path(temp),
                    )

        self.assertEqual(return_code, 0)
        self.assertEqual(payload["status"], "SUCCESS")
        self.assertEqual(len(calls), 2)
        self.assertNotEqual(calls[0]["work_dir"], calls[1]["work_dir"])
        self.assertNotEqual(calls[0]["result_path"], calls[1]["result_path"])
        self.assertNotEqual(calls[0]["log_path"], calls[1]["log_path"])
        self.assertEqual(calls[0]["env"]["RUNTIME_PROFILE"], "local-small")
        self.assertEqual(calls[1]["env"]["RUNTIME_PROFILE"], "local-medium")
        self.assertEqual(
            calls[0]["timeout"],
            benchmark.worker_timeout_seconds("local-small"),
        )
        self.assertEqual(
            calls[1]["timeout"],
            benchmark.worker_timeout_seconds("local-medium"),
        )
        self.assertEqual(calls[0]["env"]["SPARK_JARS_PACKAGES"], "")
        self.assertNotEqual(
            calls[0]["env"]["SPARK_IVY_DIR"],
            calls[1]["env"]["SPARK_IVY_DIR"],
        )
        self.assertIn(benchmark.BENCHMARK_RESULT_MARKER, output.getvalue())
        self.assertFalse(payload["cloud_ready_reference"]["executed"])
        self.assertEqual(benchmark.validate_public_payload(payload), [])

    def test_nonzero_worker_exit_is_fail_closed(self):
        successful = _valid_run(
            "local-small", 400, 20, 30, "768m", "768m", 2
        )

        def fake_run(
            command,
            cwd,
            environment,
            timeout_seconds,
            log_path,
        ):
            result_path = Path(
                command[command.index("--_worker-result-path") + 1]
            )
            benchmark._write_json(result_path, successful)
            return 7

        with tempfile.TemporaryDirectory() as temp:
            with patch.object(
                benchmark,
                "_run_worker_process",
                side_effect=fake_run,
            ):
                result = benchmark.run_profile_subprocess(
                    "local-small",
                    Path(temp),
                    "WARN",
                )
        self.assertEqual(result["status"], "FAILURE")
        self.assertIn("worker_exit_code_nonzero", result["validation_failures"])

    def test_worker_timeout_and_spawn_failure_return_safe_stubs(self):
        cases = (
            (
                subprocess.TimeoutExpired(cmd=["python"], timeout=1),
                "worker_timeout",
            ),
            (OSError("spawn failed"), "worker_spawn_failed"),
        )
        for raised, expected in cases:
            with self.subTest(expected=expected):
                with tempfile.TemporaryDirectory() as temp:
                    with patch.object(
                        benchmark,
                        "_run_worker_process",
                        side_effect=raised,
                    ):
                        result = benchmark.run_profile_subprocess(
                            "local-small",
                            Path(temp),
                            "WARN",
                        )
                self.assertEqual(result["status"], "FAILURE")
                self.assertIn(expected, result["validation_failures"])
                self.assertEqual(
                    benchmark.validate_public_payload(result),
                    [],
                )

    def test_reused_workdir_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "stale.txt").write_text("stale", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "must be empty"):
                benchmark.orchestrate_benchmark(
                    runtime_profiles=("local-small",),
                    work_dir=root,
                )

    def test_preexisting_result_path_is_rejected_and_preserved(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            result_path = root / "existing-result.json"
            result_path.write_text('{"status":"SUCCESS"}\n', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "must not exist"):
                benchmark.orchestrate_benchmark(
                    runtime_profiles=("local-small",),
                    result_path=result_path,
                    work_dir=root / "work",
                )
            self.assertEqual(
                result_path.read_text(encoding="utf-8"),
                '{"status":"SUCCESS"}\n',
            )

    def test_posix_timeout_terminates_the_worker_process_group(self):
        class FakeProcess:
            pid = 1234

            def __init__(self):
                self.wait_calls = 0

            def poll(self):
                return None

            def wait(self, timeout=None):
                self.wait_calls += 1
                if self.wait_calls == 1:
                    raise subprocess.TimeoutExpired(
                        cmd=["python"],
                        timeout=timeout,
                    )
                return 0

            def kill(self):
                raise AssertionError("direct kill is not used on POSIX")

        process = FakeProcess()
        with patch.object(benchmark.os, "name", "posix"):
            with patch.object(benchmark.signal, "SIGKILL", 9, create=True):
                with patch.object(
                    benchmark.os,
                    "killpg",
                    create=True,
                ) as kill_process_group:
                    benchmark._terminate_worker_process_tree(process)

        self.assertEqual(
            kill_process_group.call_args_list,
            [
                call(process.pid, benchmark.signal.SIGTERM),
                call(process.pid, 9),
            ],
        )

    def test_posix_timeout_kills_group_after_leader_exits(self):
        class FakeProcess:
            pid = 4321

            def __init__(self):
                self.exited = False

            def poll(self):
                return 0 if self.exited else None

            def wait(self, timeout=None):
                self.exited = True
                return 0

            def kill(self):
                raise AssertionError("direct kill is not used on POSIX")

        process = FakeProcess()
        with patch.object(benchmark.os, "name", "posix"):
            with patch.object(benchmark.signal, "SIGKILL", 9, create=True):
                with patch.object(
                    benchmark.os,
                    "killpg",
                    create=True,
                ) as kill_process_group:
                    benchmark._terminate_worker_process_tree(process)

        self.assertEqual(
            kill_process_group.call_args_list,
            [
                call(process.pid, benchmark.signal.SIGTERM),
                call(process.pid, 9),
            ],
        )

    def test_windows_timeout_uses_recursive_taskkill(self):
        class FakeProcess:
            pid = 2468

            def __init__(self):
                self.exited = False

            def poll(self):
                return 0 if self.exited else None

            def wait(self, timeout=None):
                self.exited = True
                return 0

            def kill(self):
                raise AssertionError("taskkill should terminate the tree")

        process = FakeProcess()

        def fake_taskkill(command, **kwargs):
            self.assertEqual(
                command,
                [
                    "taskkill.exe",
                    "/PID",
                    str(process.pid),
                    "/T",
                    "/F",
                ],
            )
            process.exited = True
            return subprocess.CompletedProcess(command, 0)

        with patch.object(benchmark.os, "name", "nt"):
            with patch.object(
                benchmark.subprocess,
                "run",
                side_effect=fake_taskkill,
            ) as taskkill:
                benchmark._terminate_worker_process_tree(process)

        taskkill.assert_called_once()

    def test_result_write_failure_still_emits_a_failure_marker(self):
        successful = _valid_run(
            "local-small", 400, 20, 30, "768m", "768m", 2
        )
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "work"
            with patch.object(
                benchmark,
                "run_profile_subprocess",
                return_value=successful,
            ):
                with patch.object(
                    benchmark,
                    "_write_json",
                    side_effect=OSError("read only"),
                ):
                    with redirect_stdout(io.StringIO()) as output:
                        payload, return_code = benchmark.orchestrate_benchmark(
                            runtime_profiles=("local-small",),
                            result_path=Path(temp) / "result.json",
                            work_dir=root,
                        )

        self.assertEqual(return_code, 1)
        self.assertEqual(payload["status"], "FAILURE")
        self.assertIn(
            "benchmark_result_write_failed",
            payload["validation_failures"],
        )
        self.assertEqual(
            output.getvalue().count(benchmark.BENCHMARK_RESULT_MARKER),
            1,
        )

    def test_main_converts_unexpected_failure_to_safe_marker(self):
        with patch.object(
            benchmark,
            "orchestrate_benchmark",
            side_effect=OSError("C:" + "\\Users\\person\\private"),
        ):
            with redirect_stdout(io.StringIO()) as output:
                return_code = benchmark.main(
                    ["--runtime-profiles", "local-small"]
                )

        self.assertEqual(return_code, 2)
        rendered = output.getvalue()
        self.assertEqual(
            rendered.count(benchmark.BENCHMARK_RESULT_MARKER),
            1,
        )
        self.assertNotIn("C:" + "\\Users", rendered)
        payload = json.loads(
            rendered.split(benchmark.BENCHMARK_RESULT_MARKER, 1)[1]
        )
        self.assertEqual(payload["status"], "FAILURE")
        self.assertEqual(
            payload["error"]["message"],
            "Benchmark execution failed.",
        )


if __name__ == "__main__":
    unittest.main()
