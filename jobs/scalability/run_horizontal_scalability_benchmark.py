"""Static Spark horizontal scale-out workload, evidence, and comparison harness."""

import argparse
import hashlib
import json
import math
import os
import re
import statistics
import sys
import tempfile
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
COMMON_PATH = REPO_ROOT / "jobs" / "common"
LOCAL_BENCHMARK_PATH = REPO_ROOT / "jobs" / "scalability"
for import_path in (COMMON_PATH, LOCAL_BENCHMARK_PATH):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

from horizontal_spark_application import (  # noqa: E402
    SUPPORTED_TOPOLOGIES,
    build_horizontal_spark_application,
    validate_horizontal_profile_pair,
)
from runtime_profiles import get_runtime_profile  # noqa: E402
from run_scalability_benchmark import (  # noqa: E402
    _collect_metrics_bundle,
    _evaluate_data_vault_quality,
    _evaluate_masking,
    _source_records_from_bronze,
    sanitize_error_message,
    validate_public_payload,
)


SCHEMA_VERSION = 1
CHANGE_ID = "DM-RUN-004"
BENCHMARK_KIND = "static-horizontal-spark-scale-out"
BASELINE_PROFILE = "minikube-horizontal-1"
SCALE_OUT_PROFILE = "minikube-horizontal-3"
MEASUREMENT_REPETITIONS = 3
WARMUP_RUNS = 1
WORKLOAD_RESULT_MARKER = "HORIZONTAL_WORKLOAD_RESULT="
BENCHMARK_RESULT_MARKER = "HORIZONTAL_BENCHMARK_RESULT="

EXIT_PASS = 0
EXIT_INCONCLUSIVE = 2
EXIT_FAIL = 3
EXIT_HARNESS_ERROR = 4
EXIT_BLOCKED = 5

RESULT_EXIT_CODES = {
    "PASS": EXIT_PASS,
    "INCONCLUSIVE": EXIT_INCONCLUSIVE,
    "FAIL": EXIT_FAIL,
    "BLOCKED": EXIT_BLOCKED,
}

REQUIRED_EQUAL_FIELDS = (
    "git_sha",
    "image_digest",
    "dataset_fingerprint",
    "pipeline_contract_version",
    "input_rows",
    "output_fingerprint",
)
TECHNICAL_FINGERPRINT_COLUMNS = {
    "batch_id",
    "effective_from",
    "ingestion_date",
    "load_datetime",
    "run_id",
    "source_file",
}
PUBLIC_LIMITATIONS = (
    "Minikube is a local environment.",
    "Executor pods on one node prove application scale-out, not physical node distribution.",
    "This run does not establish production readiness, SLA, cost, or production sizing.",
    "Dynamic allocation and horizontal autoscaling were not implemented.",
    "No cloud environment was executed.",
)
_PRIVATE_IDENTIFIERS = (
    "Data-Master-Platform" + "-SPDD",
    "Data-Master-" + "Mastery",
)
PRIVATE_NAME_PATTERN = re.compile(
    "("
    + "|".join(re.escape(value) for value in _PRIVATE_IDENTIFIERS)
    + r"|github\.com/[^/\s]+/)",
    re.IGNORECASE,
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def round_metric(value: Any) -> float:
    return round(float(value), 3)


def calculate_throughput(records: Any, duration_seconds: Any) -> float:
    records_value = float(records)
    duration_value = float(duration_seconds)
    if records_value < 0 or not math.isfinite(records_value):
        raise ValueError("records must be finite and non-negative.")
    if duration_value <= 0 or not math.isfinite(duration_value):
        raise ValueError("duration_seconds must be finite and positive.")
    return round_metric(records_value / duration_value)


def calculate_speedup(
    baseline_median_seconds: Any,
    scale_out_median_seconds: Any,
) -> float:
    baseline = float(baseline_median_seconds)
    scale_out = float(scale_out_median_seconds)
    if baseline <= 0 or scale_out <= 0:
        raise ValueError("Median durations must be positive.")
    return round_metric(baseline / scale_out)


def calculate_parallel_efficiency(speedup: Any, executor_count: int = 3) -> float:
    if executor_count <= 0:
        raise ValueError("executor_count must be positive.")
    value = float(speedup)
    if value < 0 or not math.isfinite(value):
        raise ValueError("speedup must be finite and non-negative.")
    return round_metric(value / executor_count)


def median(values: Iterable[Any]) -> float:
    normalized = [float(value) for value in values]
    if not normalized or any(
        not math.isfinite(value) or value <= 0 for value in normalized
    ):
        raise ValueError("Median requires finite positive values.")
    return round_metric(statistics.median(normalized))


def _install_pipeline_import_paths() -> None:
    for relative_path in (
        "jobs/data_generation",
        "jobs/bronze",
        "jobs/raw_vault",
        "jobs/business_vault",
        "jobs/common",
    ):
        path = str(REPO_ROOT / relative_path)
        if path not in sys.path:
            sys.path.insert(0, path)


def _assert_workload_contract(args: argparse.Namespace) -> Dict[str, Any]:
    profile = get_runtime_profile(args.profile)
    if args.profile not in (BASELINE_PROFILE, SCALE_OUT_PROFILE):
        raise ValueError("Unsupported horizontal profile.")
    if str(profile["spark"]["master"]).startswith("local"):
        raise ValueError("Horizontal workload rejects local Spark masters.")
    expected = {
        "RUNTIME_PROFILE": args.profile,
        "HORIZONTAL_BENCHMARK_ID": args.benchmark_id,
        "HORIZONTAL_RUN_ID": args.run_id,
        "HORIZONTAL_BATCH_ID": args.batch_id,
        "HORIZONTAL_GIT_SHA": args.git_sha,
        "HORIZONTAL_IMAGE_DIGEST": args.image_digest,
        "HORIZONTAL_TOPOLOGY": args.topology,
    }
    mismatches = [
        key for key, value in expected.items() if os.getenv(key) != value
    ]
    if mismatches:
        raise ValueError(
            "SparkApplication environment disagrees with workload arguments: "
            + ",".join(sorted(mismatches))
        )
    shared_paths = [
        os.environ[name]
        for name in (
            "SAMPLE_DATA_PATH",
            "BRONZE_PATH",
            "RAW_VAULT_PATH",
            "BUSINESS_VAULT_PATH",
            "GOLD_PATH",
            "MONITORING_PATH",
            "CHECKPOINT_PATH",
        )
    ]
    if any(not path.startswith("s3a://") for path in shared_paths):
        raise ValueError("All shared horizontal workload paths must use s3a://.")
    if len(set(shared_paths)) != len(shared_paths):
        raise ValueError("Horizontal workload paths must be layer-isolated.")
    return profile


def _dataset_fingerprint(files: Mapping[str, str]) -> str:
    digest = hashlib.sha256()
    for name in sorted(files):
        path = Path(files[name])
        digest.update(name.encode("utf-8"))
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _copy_input_to_shared_storage(
    spark: Any,
    files: Mapping[str, str],
    destination: str,
) -> None:
    jvm = spark.sparkContext._jvm
    hadoop_configuration = spark.sparkContext._jsc.hadoopConfiguration()
    destination_path = jvm.org.apache.hadoop.fs.Path(destination)
    filesystem = destination_path.getFileSystem(hadoop_configuration)
    if filesystem.exists(destination_path):
        raise RuntimeError("Isolated input prefix already exists.")
    if not filesystem.mkdirs(destination_path):
        raise RuntimeError("Unable to create isolated input prefix.")
    for source in files.values():
        source_path = jvm.org.apache.hadoop.fs.Path(
            Path(source).resolve().as_uri()
        )
        target_path = jvm.org.apache.hadoop.fs.Path(
            f"{destination.rstrip('/')}/{Path(source).name}"
        )
        filesystem.copyFromLocalFile(False, False, source_path, target_path)


def _run_stage(
    name: str,
    action: Callable[[], Mapping[str, Any]],
) -> tuple:
    started = time.perf_counter()
    result = dict(action())
    duration = round_metric(time.perf_counter() - started)
    if result.get("status") != "SUCCESS":
        raise RuntimeError(f"Pipeline stage {name} reported failure.")
    return result, {
        "name": name,
        "status": "PASS",
        "duration_seconds": duration,
    }


def _resolve_table_path(config: Any) -> str:
    if isinstance(config, str):
        return config
    if isinstance(config, Mapping) and isinstance(config.get("path"), str):
        return str(config["path"])
    raise ValueError("Table configuration does not expose a readable path.")


def _table_fingerprint(spark: Any, delta_io: Any, config: Any) -> Dict[str, Any]:
    from pyspark.sql import functions as functions

    frame = delta_io.read_delta(spark, _resolve_table_path(config))
    if frame is None:
        raise RuntimeError("Fingerprint table is not readable.")
    columns = [
        column
        for column in sorted(frame.columns)
        if column.lower() not in TECHNICAL_FINGERPRINT_COLUMNS
    ]
    if not columns:
        aggregate = frame.agg(
            functions.count(functions.lit(1)).alias("row_count")
        ).first()
        summary = {
            "rows": int(aggregate["row_count"]),
            "min": None,
            "max": None,
            "sum": None,
        }
    else:
        values = [
            functions.coalesce(
                functions.col(column).cast("string"),
                functions.lit("<NULL>"),
            )
            for column in columns
        ]
        row_hash = functions.xxhash64(*values).alias("row_hash")
        aggregate = (
            frame.select(row_hash)
            .agg(
                functions.count(functions.lit(1)).alias("row_count"),
                functions.min("row_hash").alias("min_hash"),
                functions.max("row_hash").alias("max_hash"),
                functions.sum("row_hash").alias("sum_hash"),
            )
            .first()
        )
        summary = {
            "rows": int(aggregate["row_count"]),
            "min": aggregate["min_hash"],
            "max": aggregate["max_hash"],
            "sum": aggregate["sum_hash"],
        }
    encoded = json.dumps(
        summary,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return {
        "row_count": summary["rows"],
        "fingerprint": "sha256:" + hashlib.sha256(encoded).hexdigest(),
    }


def _group_fingerprint(
    spark: Any,
    delta_io: Any,
    registry: Mapping[str, Any],
) -> Dict[str, Any]:
    tables = {
        str(name): _table_fingerprint(spark, delta_io, config)
        for name, config in sorted(registry.items())
    }
    encoded = json.dumps(
        tables,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return {
        "tables": tables,
        "fingerprint": "sha256:" + hashlib.sha256(encoded).hexdigest(),
    }


def _collect_functional_fingerprints(
    spark: Any,
    config: Any,
    delta_io: Any,
) -> Dict[str, Any]:
    groups = {
        "bronze": _group_fingerprint(
            spark,
            delta_io,
            config.BRONZE_TABLES,
        ),
        "raw_vault_hubs": _group_fingerprint(
            spark,
            delta_io,
            config.HUB_TABLES,
        ),
        "raw_vault_links": _group_fingerprint(
            spark,
            delta_io,
            config.LINK_TABLES,
        ),
        "raw_vault_satellites": _group_fingerprint(
            spark,
            delta_io,
            config.SATELLITE_TABLES,
        ),
        "gold": _group_fingerprint(
            spark,
            delta_io,
            config.GOLD_TABLES,
        ),
    }
    return {
        "groups": groups,
        "output_fingerprint": groups["gold"]["fingerprint"],
    }


def _status_api_json(url: str) -> Any:
    with urllib.request.urlopen(url, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def _collect_spark_status_api(spark: Any) -> Dict[str, Any]:
    ui_url = spark.sparkContext.uiWebUrl
    if not ui_url:
        raise RuntimeError("Spark status API is unavailable.")
    application_id = str(spark.sparkContext.applicationId)
    base = f"{ui_url.rstrip('/')}/api/v1/applications/{application_id}"
    executors = _status_api_json(base + "/executors")
    stages = _status_api_json(base + "/stages")

    executor_metrics: Dict[str, Dict[str, Any]] = {}
    for executor in executors:
        executor_id = str(executor.get("id", ""))
        if not executor_id or executor_id == "driver":
            continue
        executor_metrics[executor_id] = {
            "executor_id": executor_id,
            "host": str(executor.get("hostPort", "")).split(":", 1)[0],
            "status": "ACTIVE" if executor.get("isActive") else "INACTIVE",
            "tasks": int(executor.get("totalTasks", 0)),
            "failed_tasks": int(executor.get("failedTasks", 0)),
            "runtime_ms": int(executor.get("totalDuration", 0)),
            "input_bytes": int(executor.get("totalInputBytes", 0)),
            "input_records": 0,
            "output_bytes": 0,
            "output_records": 0,
            "shuffle_read_bytes": int(executor.get("totalShuffleRead", 0)),
            "shuffle_write_bytes": int(executor.get("totalShuffleWrite", 0)),
        }

    stage_summaries = []
    for stage in stages:
        if str(stage.get("status", "")).upper() == "SKIPPED":
            continue
        executor_summary = stage.get("executorSummary") or {}
        for executor_id, summary in executor_summary.items():
            metrics = executor_metrics.get(str(executor_id))
            if metrics is None:
                continue
            metrics["input_records"] += int(summary.get("inputRecords", 0))
            metrics["output_bytes"] += int(summary.get("outputBytes", 0))
            metrics["output_records"] += int(summary.get("outputRecords", 0))
        stage_summaries.append(
            {
                "stage_id": int(stage.get("stageId", -1)),
                "attempt_id": int(stage.get("attemptId", 0)),
                "name": str(stage.get("name", ""))[:120],
                "status": str(stage.get("status", "")),
                "tasks": int(stage.get("numTasks", 0)),
                "input_records": int(stage.get("inputRecords", 0)),
                "output_records": int(stage.get("outputRecords", 0)),
                "shuffle_read_bytes": int(stage.get("shuffleReadBytes", 0)),
                "shuffle_write_bytes": int(stage.get("shuffleWriteBytes", 0)),
            }
        )
    ordered = [
        executor_metrics[key]
        for key in sorted(executor_metrics, key=lambda value: int(value))
    ]
    if not ordered:
        raise RuntimeError("Spark status API observed no real executor.")
    return {
        "application_id": application_id,
        "executors": ordered,
        "stages": stage_summaries,
    }


def _validate_workload_payload(payload: Mapping[str, Any]) -> List[str]:
    failures: List[str] = []
    for field in (
        "schema_version",
        "benchmark_id",
        "run_id",
        "batch_id",
        "profile_id",
        "git_sha",
        "image_digest",
        "dataset_fingerprint",
        "pipeline_contract_version",
        "input_rows",
        "output_fingerprint",
        "duration_seconds",
        "throughput_records_per_second",
        "layer_counts",
        "quality",
        "lineage",
        "masking",
        "secret_findings",
        "monitoring",
        "spark_api",
        "functional_fingerprints",
    ):
        if field not in payload:
            failures.append(f"missing:{field}")
    if payload.get("status") != "PASS":
        failures.append("workload_not_pass")
    if payload.get("quality", {}).get("status") != "PASS":
        failures.append("data_vault_not_pass")
    if payload.get("lineage", {}).get("status") != "PASS":
        failures.append("lineage_not_pass")
    if payload.get("masking", {}).get("status") != "PASS":
        failures.append("masking_not_pass")
    if payload.get("secret_findings") != 0:
        failures.append("secret_findings_nonzero")
    if any(
        int(value) <= 0 for value in payload.get("layer_counts", {}).values()
    ):
        failures.append("nonpositive_layer_count")
    api_executors = payload.get("spark_api", {}).get("executors", [])
    requested = payload.get("executors_requested")
    if len(api_executors) != requested:
        failures.append("spark_api_executor_count_mismatch")
    if any(int(item.get("tasks", 0)) <= 0 for item in api_executors):
        failures.append("executor_without_tasks")
    failures.extend(validate_public_horizontal_payload(payload))
    return list(dict.fromkeys(failures))


def validate_public_horizontal_payload(payload: Any) -> List[str]:
    failures = list(validate_public_payload(payload))

    def walk(value: Any, location: str) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                walk(item, f"{location}.{key}")
        elif isinstance(value, (list, tuple)):
            for index, item in enumerate(value):
                walk(item, f"{location}[{index}]")
        elif isinstance(value, str) and PRIVATE_NAME_PATTERN.search(value):
            failures.append(f"private_reference:{location}")

    walk(payload, "$")
    return list(dict.fromkeys(failures))


def run_workload(args: argparse.Namespace) -> int:
    spark_factory = None
    payload: Dict[str, Any]
    try:
        profile = _assert_workload_contract(args)
        _install_pipeline_import_paths()
        from config import Config
        from data_vault_quality_gate import evaluate_configured_gate
        from delta_io import DeltaIO
        from generate_banking_sample_data import generate_all_sample_data
        from load_bronze import run_bronze_pipeline
        from load_gold import run_business_vault_pipeline
        from load_hubs import run_hubs_pipeline
        from load_links import run_links_pipeline
        from load_satellites import run_satellites_pipeline
        from monitoring import MonitoringLogger
        from run_gold_masking_smoke import (
            _masking_function_samples,
            _scan_high_confidence_secrets,
            _validate_gold_outputs,
        )
        from spark_session import SparkSessionFactory, create_spark_session

        spark_factory = SparkSessionFactory
        spark = create_spark_session()
        if str(spark.sparkContext.master).startswith("local"):
            raise RuntimeError("Spark resolved a forbidden local master.")
        if (
            str(spark.conf.get("spark.dynamicAllocation.enabled", "false"))
            != "false"
        ):
            raise RuntimeError("Dynamic allocation is unexpectedly enabled.")

        dataset = profile["dataset"]
        with tempfile.TemporaryDirectory(prefix="dm-horizontal-input-") as work:
            files = generate_all_sample_data(
                output_dir=work,
                runtime_profile=args.profile,
                seed=int(dataset["seed"]),
                reference_time=str(dataset["reference_time"]),
            )
            dataset_fingerprint = _dataset_fingerprint(files)
            _copy_input_to_shared_storage(
                spark,
                files,
                os.environ["SAMPLE_DATA_PATH"],
            )

        stages: List[Dict[str, Any]] = []
        pipeline_started = time.perf_counter()
        bronze_result, stage = _run_stage(
            "bronze",
            lambda: run_bronze_pipeline(
                spark,
                os.environ["SAMPLE_DATA_PATH"],
                os.environ["BRONZE_PATH"],
                args.batch_id,
            ),
        )
        stages.append(stage)
        for name, action in (
            (
                "hubs",
                lambda: run_hubs_pipeline(
                    spark,
                    os.environ["BRONZE_PATH"],
                    args.batch_id,
                ),
            ),
            (
                "links",
                lambda: run_links_pipeline(
                    spark,
                    os.environ["BRONZE_PATH"],
                    args.batch_id,
                ),
            ),
            (
                "satellites",
                lambda: run_satellites_pipeline(
                    spark,
                    os.environ["BRONZE_PATH"],
                    args.batch_id,
                ),
            ),
            (
                "gold",
                lambda: run_business_vault_pipeline(
                    spark,
                    os.environ["RAW_VAULT_PATH"],
                    os.environ["GOLD_PATH"],
                    args.batch_id,
                ),
            ),
        ):
            _, stage = _run_stage(name, action)
            stages.append(stage)

        quality_result, stage = _run_stage(
            "data-vault-gate",
            lambda: _evaluate_data_vault_quality(
                spark,
                os.environ["RAW_VAULT_PATH"],
                os.environ["GOLD_PATH"],
                evaluate_configured_gate,
            ),
        )
        stages.append(stage)
        masking_result, stage = _run_stage(
            "masking-gate",
            lambda: _evaluate_masking(
                spark,
                Config,
                DeltaIO,
                _masking_function_samples,
                _validate_gold_outputs,
            ),
        )
        stages.append(stage)
        metrics_result, stage = _run_stage(
            "metrics",
            lambda: _collect_metrics_bundle(
                spark,
                Config,
                DeltaIO,
                MonitoringLogger,
                args.batch_id,
            ),
        )
        stages.append(stage)
        fingerprints = _collect_functional_fingerprints(
            spark,
            Config,
            DeltaIO,
        )
        secret_findings = len(_scan_high_confidence_secrets(REPO_ROOT))
        spark_api = _collect_spark_status_api(spark)
        duration = round_metric(time.perf_counter() - pipeline_started)
        source_records = _source_records_from_bronze(bronze_result)
        quality = quality_result["quality"]
        lineage_checks = {
            name: quality["checks"].get(name)
            for name in ("lineage", "gold_lineage")
        }
        lineage = {
            "status": (
                "PASS"
                if all(value == "PASS" for value in lineage_checks.values())
                else "FAIL"
            ),
            "checks": lineage_checks,
        }
        payload = {
            "schema_version": SCHEMA_VERSION,
            "benchmark_kind": BENCHMARK_KIND,
            "change_id": CHANGE_ID,
            "status": "PASS",
            "benchmark_id": args.benchmark_id,
            "run_id": args.run_id,
            "batch_id": args.batch_id,
            "measurement_kind": args.measurement_kind,
            "repetition": args.repetition,
            "profile_id": args.profile,
            "topology": args.topology,
            "git_sha": args.git_sha,
            "image_digest": args.image_digest,
            "dataset_seed": dataset["seed"],
            "dataset_volume": dataset["volume"],
            "dataset_fingerprint": dataset_fingerprint,
            "pipeline_contract_version": dataset[
                "pipeline_contract_version"
            ],
            "input_rows": source_records["total"],
            "source_counts": source_records["by_source"],
            "output_fingerprint": fingerprints["output_fingerprint"],
            "functional_fingerprints": fingerprints["groups"],
            "executors_requested": profile["spark"]["executor_instances"],
            "duration_seconds": duration,
            "throughput_records_per_second": calculate_throughput(
                source_records["total"],
                duration,
            ),
            "stage_durations": stages,
            "layer_counts": metrics_result["layer_counts"],
            "quality": quality,
            "lineage": lineage,
            "masking": masking_result["masking"],
            "secret_findings": secret_findings,
            "monitoring": {
                "status": (
                    "PASS"
                    if metrics_result["monitoring"]["status"] == "READABLE"
                    else "FAIL"
                ),
                "event_count": metrics_result["monitoring"]["event_count"],
            },
            "spark": {
                "master": str(spark.sparkContext.master),
                "executor_memory": str(
                    spark.conf.get("spark.executor.memory")
                ),
                "executor_instances": int(
                    spark.conf.get("spark.executor.instances")
                ),
                "shuffle_partitions": int(
                    spark.conf.get("spark.sql.shuffle.partitions")
                ),
                "dynamic_allocation": False,
            },
            "spark_api": spark_api,
        }
        workload_failures = _validate_workload_payload(payload)
        if workload_failures:
            payload["status"] = "FAIL"
            payload["validation_failures"] = workload_failures
    except BaseException as exc:
        payload = {
            "schema_version": SCHEMA_VERSION,
            "benchmark_kind": BENCHMARK_KIND,
            "change_id": CHANGE_ID,
            "status": "FAIL",
            "benchmark_id": args.benchmark_id,
            "run_id": args.run_id,
            "batch_id": args.batch_id,
            "measurement_kind": args.measurement_kind,
            "repetition": args.repetition,
            "profile_id": args.profile,
            "topology": args.topology,
            "git_sha": args.git_sha,
            "image_digest": args.image_digest,
            "failure": {
                "type": type(exc).__name__,
                "message": sanitize_error_message(exc),
            },
        }
    finally:
        if spark_factory is not None:
            try:
                spark_factory.stop()
            except Exception:
                pass

    print(
        WORKLOAD_RESULT_MARKER
        + json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            allow_nan=False,
        ),
        flush=True,
    )
    return EXIT_PASS if payload["status"] == "PASS" else EXIT_FAIL


def _read_json(path: str) -> Dict[str, Any]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("Expected a JSON object.")
    return value


def _combine_measurement(
    workload: Mapping[str, Any],
    observation: Mapping[str, Any],
) -> Dict[str, Any]:
    failures = _validate_workload_payload(workload)
    if observation.get("profile_id") != workload.get("profile_id"):
        failures.append("observation_profile_mismatch")
    if observation.get("run_id") != workload.get("run_id"):
        failures.append("observation_run_mismatch")
    if observation.get("application_status") != "COMPLETED":
        failures.append("spark_application_not_completed")
    shared_storage = observation.get("shared_storage", {})
    if shared_storage.get("status") != "PASS":
        failures.append("shared_storage_not_stable")
    if int(shared_storage.get("restart_count", -1)) != 0:
        failures.append("shared_storage_restart_observed")
    requested = workload.get("executors_requested")
    if observation.get("executors_requested") != requested:
        failures.append("observation_requested_executor_mismatch")
    pods = observation.get("executor_pods", [])
    if len(pods) != requested:
        failures.append("executor_pod_count_mismatch")
    if any(pod.get("status") not in {"Running", "Succeeded"} for pod in pods):
        failures.append("executor_pod_status_invalid")

    pods_by_ip = {
        str(pod.get("pod_ip")): pod
        for pod in pods
        if pod.get("pod_ip")
    }
    executors = []
    for metric in workload.get("spark_api", {}).get("executors", []):
        pod = pods_by_ip.get(str(metric.get("host")))
        if pod is None:
            failures.append(
                f"executor_pod_mapping_missing:{metric.get('executor_id')}"
            )
            pod = {}
        executors.append(
            {
                **dict(metric),
                "pod": pod.get("name"),
                "pod_status": pod.get("status"),
                "node": pod.get("node"),
            }
        )
    if len(executors) != requested:
        failures.append("executor_metric_count_mismatch")
    tasks_distributed = (
        sum(1 for item in executors if int(item.get("tasks", 0)) > 0) >= 2
        if requested == 3
        else len(executors) == 1 and int(executors[0].get("tasks", 0)) > 0
    )
    if not tasks_distributed:
        failures.append("tasks_not_distributed")

    nodes = sorted(
        {
            str(item.get("node"))
            for item in executors
            if item.get("node")
        }
    )
    topology = workload.get("topology")
    if topology == "single-node-application-scale-out" and len(nodes) != 1:
        failures.append("single_node_topology_not_observed")
    if topology == "multi-node-scale-out" and len(nodes) < 2:
        failures.append("multi_node_topology_not_observed")

    combined = {
        key: value
        for key, value in workload.items()
        if key != "spark_api"
    }
    combined["executor_evidence"] = {
        "executors_requested": requested,
        "executors_observed": len(executors),
        "tasks_distributed": tasks_distributed,
        "nodes_observed": nodes,
        "executors": executors,
    }
    combined["driver_pods_observed"] = len(
        observation.get("driver_pods", [])
    )
    combined["shared_storage"] = dict(shared_storage)
    combined["validation_failures"] = list(dict.fromkeys(failures))
    combined["status"] = "PASS" if not failures else "FAIL"
    return combined


def _profile_summary(runs: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    durations = [run["duration_seconds"] for run in runs]
    duration_median = median(durations)
    input_rows = int(runs[0]["input_rows"])
    return {
        "profile_id": runs[0]["profile_id"],
        "executors_requested": runs[0]["executors_requested"],
        "executors_observed": sorted(
            {
                run["executor_evidence"]["executors_observed"]
                for run in runs
            }
        ),
        "nodes_observed": sorted(
            {
                node
                for run in runs
                for node in run["executor_evidence"]["nodes_observed"]
            }
        ),
        "runs": [dict(run) for run in runs],
        "median_duration_seconds": duration_median,
        "median_throughput_records_per_second": calculate_throughput(
            input_rows,
            duration_median,
        ),
    }


def evaluate_benchmark_result(
    *,
    baseline_runs: Sequence[Mapping[str, Any]],
    scale_out_runs: Sequence[Mapping[str, Any]],
    warmup: Mapping[str, Any],
) -> Dict[str, Any]:
    failures: List[str] = []
    if warmup.get("status") != "PASS":
        failures.append("warmup_failed")
    if len(baseline_runs) != MEASUREMENT_REPETITIONS:
        failures.append("baseline_measurement_count_invalid")
    if len(scale_out_runs) != MEASUREMENT_REPETITIONS:
        failures.append("scale_out_measurement_count_invalid")
    all_runs = [*baseline_runs, *scale_out_runs]
    if any(run.get("status") != "PASS" for run in all_runs):
        failures.append("measurement_failed")
    if failures:
        return {
            "result": "FAIL",
            "failures": list(dict.fromkeys(failures)),
        }

    baseline = _profile_summary(baseline_runs)
    scale_out = _profile_summary(scale_out_runs)
    if baseline["executors_observed"] != [1]:
        failures.append("baseline_executor_observation_invalid")
    if any(value < 3 for value in scale_out["executors_observed"]):
        failures.append("scale_out_executor_observation_invalid")
    if any(
        not run["executor_evidence"]["tasks_distributed"] for run in all_runs
    ):
        failures.append("task_distribution_invalid")

    equality = {}
    for field in REQUIRED_EQUAL_FIELDS:
        values = {json.dumps(run.get(field), sort_keys=True) for run in all_runs}
        equality[field] = len(values) == 1
        if not equality[field]:
            failures.append(f"functional_equivalence:{field}")
    for field in ("source_counts", "layer_counts", "functional_fingerprints"):
        values = {json.dumps(run.get(field), sort_keys=True) for run in all_runs}
        equality[field] = len(values) == 1
        if not equality[field]:
            failures.append(f"functional_equivalence:{field}")
    for run in all_runs:
        if run["quality"]["status"] != "PASS":
            failures.append("data_vault_gate_failed")
        if run["lineage"]["status"] != "PASS":
            failures.append("lineage_gate_failed")
        if run["masking"]["status"] != "PASS":
            failures.append("masking_gate_failed")
        if run["secret_findings"] != 0:
            failures.append("secret_gate_failed")

    speedup = calculate_speedup(
        baseline["median_duration_seconds"],
        scale_out["median_duration_seconds"],
    )
    efficiency = calculate_parallel_efficiency(speedup, 3)
    benefit = (
        scale_out["median_duration_seconds"]
        < baseline["median_duration_seconds"]
        or scale_out["median_throughput_records_per_second"]
        > baseline["median_throughput_records_per_second"]
    )
    if failures:
        result = "FAIL"
    elif benefit:
        result = "PASS"
    else:
        result = "INCONCLUSIVE"
    return {
        "result": result,
        "failures": list(dict.fromkeys(failures)),
        "functional_equivalence": equality,
        "baseline": baseline,
        "scale_out": scale_out,
        "speedup": speedup,
        "parallel_efficiency": efficiency,
        "measurable_benefit": benefit,
    }


def aggregate_benchmark(args: argparse.Namespace) -> int:
    try:
        manifest = _read_json(args.measurement_manifest)
        warmup_entries = manifest.get("warmups", [])
        measurement_entries = manifest.get("measurements", [])
        if len(warmup_entries) != WARMUP_RUNS:
            raise ValueError("The benchmark requires exactly one discarded warm-up.")
        if len(measurement_entries) != MEASUREMENT_REPETITIONS * 2:
            raise ValueError("The benchmark requires exactly six measurements.")

        combined_warmups = [
            _combine_measurement(
                _read_json(entry["workload"]),
                _read_json(entry["observation"]),
            )
            for entry in warmup_entries
        ]
        combined_runs = [
            _combine_measurement(
                _read_json(entry["workload"]),
                _read_json(entry["observation"]),
            )
            for entry in measurement_entries
        ]
        baseline_runs = [
            run for run in combined_runs if run["profile_id"] == BASELINE_PROFILE
        ]
        scale_out_runs = [
            run for run in combined_runs if run["profile_id"] == SCALE_OUT_PROFILE
        ]
        evaluation = evaluate_benchmark_result(
            baseline_runs=baseline_runs,
            scale_out_runs=scale_out_runs,
            warmup=combined_warmups[0],
        )
        payload = {
            "schema_version": SCHEMA_VERSION,
            "benchmark_kind": BENCHMARK_KIND,
            "change_id": CHANGE_ID,
            "benchmark_id": manifest["benchmark_id"],
            "generated_at": utc_now(),
            "topology": manifest["topology"],
            "experiment": {
                "baseline_profile": BASELINE_PROFILE,
                "scale_out_profile": SCALE_OUT_PROFILE,
                "primary_variable": "spark.executor_instances",
                "controlled_infrastructure": manifest["infrastructure"],
                "warmups_discarded": WARMUP_RUNS,
                "measurements_per_profile": MEASUREMENT_REPETITIONS,
                "statistic": "median",
            },
            "warmup": {
                "profile_id": combined_warmups[0]["profile_id"],
                "status": combined_warmups[0]["status"],
                "discarded": True,
            },
            **evaluation,
            "limitations": list(PUBLIC_LIMITATIONS),
        }
        public_failures = validate_public_horizontal_payload(payload)
        if public_failures:
            payload["result"] = "FAIL"
            payload["failures"] = list(
                dict.fromkeys(
                    [*payload.get("failures", []), *public_failures]
                )
            )
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(
            json.dumps(
                payload,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
                allow_nan=False,
            )
            + "\n",
            encoding="utf-8",
        )
        print(
            BENCHMARK_RESULT_MARKER
            + json.dumps(
                {
                    "benchmark_id": payload["benchmark_id"],
                    "result": payload["result"],
                    "exit_code": RESULT_EXIT_CODES[payload["result"]],
                },
                sort_keys=True,
            ),
            flush=True,
        )
        return RESULT_EXIT_CODES[payload["result"]]
    except BaseException as exc:
        print(
            BENCHMARK_RESULT_MARKER
            + json.dumps(
                {
                    "result": "HARNESS_ERROR",
                    "exit_code": EXIT_HARNESS_ERROR,
                    "error": sanitize_error_message(exc),
                },
                sort_keys=True,
            ),
            flush=True,
        )
        return EXIT_HARNESS_ERROR


def render_application(args: argparse.Namespace) -> int:
    validate_horizontal_profile_pair(BASELINE_PROFILE, SCALE_OUT_PROFILE)
    application = build_horizontal_spark_application(
        profile_id=args.profile,
        benchmark_id=args.benchmark_id,
        run_id=args.run_id,
        batch_id=args.batch_id,
        git_sha=args.git_sha,
        image=args.image,
        image_digest=args.image_digest,
        topology=args.topology,
        measurement_kind=args.measurement_kind,
        repetition=args.repetition,
    )
    Path(args.output).write_text(
        yaml.safe_dump(application, sort_keys=False),
        encoding="utf-8",
    )
    return EXIT_PASS


def write_run_plan(args: argparse.Namespace) -> int:
    """Materialize the only component that knows both comparison profiles."""
    validate_horizontal_profile_pair(BASELINE_PROFILE, SCALE_OUT_PROFILE)
    baseline_profile = get_runtime_profile(BASELINE_PROFILE)
    runs = [
        {
            "profile_id": BASELINE_PROFILE,
            "measurement_kind": "warmup",
            "repetition": 0,
            "run_id": "warmup-1",
            "batch_id": "batch-warmup-1",
            "application_name": "dm-h-1-warmup-1",
        }
    ]
    for repetition in range(1, MEASUREMENT_REPETITIONS + 1):
        ordered_profiles = (
            (BASELINE_PROFILE, SCALE_OUT_PROFILE)
            if repetition % 2
            else (SCALE_OUT_PROFILE, BASELINE_PROFILE)
        )
        for profile_id in ordered_profiles:
            executor_count = get_runtime_profile(profile_id)["spark"][
                "executor_instances"
            ]
            runs.append(
                {
                    "profile_id": profile_id,
                    "measurement_kind": "measurement",
                    "repetition": repetition,
                    "run_id": f"measure-{repetition}-e{executor_count}",
                    "batch_id": f"batch-{repetition}-e{executor_count}",
                    "application_name": (
                        f"dm-h-{executor_count}-measure-{repetition}"
                        f"-e{executor_count}"
                    ),
                }
            )
    plan = {
        "schema_version": SCHEMA_VERSION,
        "benchmark_id": args.benchmark_id,
        "topology": args.topology,
        "infrastructure": {
            "minikube": dict(baseline_profile["kubernetes"]["minikube"]),
            "minio": dict(baseline_profile["kubernetes"]["minio"]),
        },
        "runs": runs,
    }
    Path(args.output).write_text(
        json.dumps(plan, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return EXIT_PASS


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run or aggregate the static horizontal Spark benchmark."
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--workload", action="store_true")
    mode.add_argument("--render-application", action="store_true")
    mode.add_argument("--aggregate", action="store_true")
    mode.add_argument("--plan", action="store_true")
    parser.add_argument("--profile")
    parser.add_argument("--benchmark-id")
    parser.add_argument("--run-id")
    parser.add_argument("--batch-id")
    parser.add_argument("--git-sha")
    parser.add_argument("--image")
    parser.add_argument("--image-digest")
    parser.add_argument("--topology", choices=sorted(SUPPORTED_TOPOLOGIES))
    parser.add_argument(
        "--measurement-kind",
        choices=("warmup", "measurement"),
    )
    parser.add_argument("--repetition", type=int, default=0)
    parser.add_argument("--measurement-manifest")
    parser.add_argument("--output")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    if args.workload:
        return run_workload(args)
    if args.render_application:
        return render_application(args)
    if args.plan:
        return write_run_plan(args)
    return aggregate_benchmark(args)


if __name__ == "__main__":
    raise SystemExit(main())
