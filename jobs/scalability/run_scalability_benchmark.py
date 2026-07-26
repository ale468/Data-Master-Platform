"""Controlled local scalability benchmark for the Data Master pipeline.

The public entrypoint launches one clean Python process per executable runtime
profile. PySpark and pipeline modules are imported only by the worker, after
the profile and isolated local paths have been configured.
"""

import argparse
import json
import math
import os
import re
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


REPO_ROOT = Path(__file__).resolve().parents[2]
COMMON_PATH = REPO_ROOT / "jobs" / "common"
if str(COMMON_PATH) not in sys.path:
    sys.path.insert(0, str(COMMON_PATH))

from runtime_profiles import get_runtime_profile  # noqa: E402


SCHEMA_VERSION = 1
BENCHMARK_KIND = "controlled-local-scalability"
EXECUTABLE_PROFILES: Tuple[str, ...] = ("local-small", "local-medium")
PROFILE_RESULT_MARKER = "SCALABILITY_PROFILE_RESULT="
BENCHMARK_RESULT_MARKER = "SCALABILITY_BENCHMARK_RESULT="
WORKER_TIMEOUT_GRACE_SECONDS = 15 * 60
WORKER_TERMINATION_GRACE_SECONDS = 10

PIPELINE_STAGE_NAMES: Tuple[str, ...] = (
    "generate_sample_data",
    "bronze",
    "hubs",
    "links",
    "satellites",
    "gold",
)
PROCESSING_STAGE_NAMES: Tuple[str, ...] = (
    "bronze",
    "hubs",
    "links",
    "satellites",
    "gold",
)
VALIDATION_STAGE_NAMES: Tuple[str, ...] = (
    "data_vault_quality_gate",
    "masking_gate",
    "metrics_collection",
)
EXPECTED_STAGE_NAMES: Tuple[str, ...] = PIPELINE_STAGE_NAMES + VALIDATION_STAGE_NAMES
EXPECTED_SOURCE_NAMES: Tuple[str, ...] = (
    "clientes",
    "contas",
    "cartoes",
    "transacoes",
    "eventos_digitais",
    "agencias",
    "produtos",
)

EXPECTED_MONITORING_EVENTS: Tuple[Tuple[str, str, str], ...] = (
    ("bronze_pipeline", "load_all_bronze_tables", "bronze"),
    ("raw_vault_pipeline", "load_hubs", "raw_vault_hubs"),
    ("raw_vault_pipeline", "load_links", "raw_vault_links"),
    (
        "raw_vault_pipeline",
        "load_satellites",
        "raw_vault_satellites",
    ),
    ("gold_materialization_pipeline", "load_all_gold_tables", "gold"),
)

SAFE_INHERITED_WORKER_ENV_KEYS: Tuple[str, ...] = (
    "JAVA_HOME",
    "LANG",
    "LC_ALL",
    "LD_LIBRARY_PATH",
    "PATH",
    "SPARK_HOME",
    "TZ",
)

SAFE_SPARK_KEYS: Tuple[str, ...] = (
    "master",
    "driver_memory",
    "executor_memory",
    "executor_instances",
    "shuffle_partitions",
    "adaptive_enabled",
)

REQUIRED_PROFILE_RESULT_FIELDS: Tuple[str, ...] = (
    "schema_version",
    "runtime_profile",
    "status",
    "configured_volume",
    "observed_source_records",
    "spark",
    "resources",
    "partitions",
    "stages",
    "durations_seconds",
    "throughput",
    "layer_counts",
    "quality",
    "masking",
    "monitoring",
    "observed_bottlenecks",
    "validation_failures",
    "limitations",
)

PROFILE_OVERRIDE_ENV_KEYS: Tuple[str, ...] = (
    "RUNTIME_PROFILE",
    "DM_RUNTIME_PROFILE",
    "SAMPLE_DATA_PATH",
    "LAKEHOUSE_ROOT",
    "BRONZE_PATH",
    "RAW_VAULT_PATH",
    "BUSINESS_VAULT_PATH",
    "GOLD_PATH",
    "MONITORING_PATH",
    "SPARK_MASTER",
    "SPARK_DRIVER_MEMORY",
    "SPARK_EXECUTOR_MEMORY",
    "SPARK_EXECUTOR_INSTANCES",
    "SPARK_SQL_SHUFFLE_PARTITIONS",
    "SPARK_DELTA_SNAPSHOT_PARTITIONS",
    "SPARK_ADAPTIVE_ENABLED",
    "SPARK_IVY_DIR",
    "SPARK_JARS_PACKAGES",
)

FORBIDDEN_PUBLIC_KEYS = {
    "access_key",
    "bronze_path",
    "business_vault_path",
    "credentials",
    "environment",
    "files",
    "gold_path",
    "minio_access_key",
    "minio_secret_key",
    "monitoring_path",
    "raw_vault_path",
    "sample_data_path",
    "secret",
    "secret_key",
    "work_dir",
}

FORBIDDEN_PUBLIC_STRING_PATTERNS: Tuple[Tuple[str, re.Pattern], ...] = (
    ("file_uri", re.compile(r"file:/+", re.IGNORECASE)),
    ("s3a_uri", re.compile(r"s3a://", re.IGNORECASE)),
    ("windows_path", re.compile(r"\b[A-Za-z]:[\\/]")),
    (
        "private_posix_path",
        re.compile(
            r"(?<![A-Za-z0-9])/"
            r"(?:home|mnt/[A-Za-z]|opt|repo|root|tmp|Users|var|workspace)/"
        ),
    ),
    ("cpf_like", re.compile(r"\b\d{3}\.\d{3}\.\d{3}-\d{2}\b")),
    (
        "email_like",
        re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE),
    ),
    ("card_like", re.compile(r"\b\d{4}-\d{4}-\d{4}-\d{4}\b")),
)

CLAIM_LIMITS: Tuple[str, ...] = (
    "This is one controlled local observation, not a production benchmark.",
    "local[*] does not demonstrate horizontally distributed executors.",
    "No linear speedup, SLA, cost optimization, or production sizing is claimed.",
    "cloud-ready is reference-only and is not executed by this benchmark.",
    "The observed bottleneck is the slowest wall-clock stage, not a causal diagnosis.",
)

LOCAL_RESOURCE_OBSERVED_INTERPRETATION = (
    "Observed parallelism belongs to the local scheduler and does not "
    "demonstrate distributed executors."
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _round_metric(value: float) -> float:
    return round(float(value), 3)


def calculate_throughput(record_count: Any, duration_seconds: Any) -> float:
    """Return records/second, or zero when the basis is not positive."""
    try:
        records = float(record_count)
        duration = float(duration_seconds)
    except (TypeError, ValueError):
        return 0.0
    if records <= 0 or duration <= 0:
        return 0.0
    return _round_metric(records / duration)


def _ratio(numerator: Any, denominator: Any) -> Optional[float]:
    try:
        top = float(numerator)
        bottom = float(denominator)
    except (TypeError, ValueError):
        return None
    if bottom <= 0:
        return None
    return _round_metric(top / bottom)


def parse_memory_mib(value: Any) -> Optional[float]:
    """Normalize Spark memory strings to MiB for an informational comparison."""
    if not isinstance(value, str):
        return None
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([kmgt])(?:i?b)?\s*", value, re.I)
    if not match:
        return None
    amount = float(match.group(1))
    unit = match.group(2).lower()
    factors = {"k": 1 / 1024, "m": 1, "g": 1024, "t": 1024 * 1024}
    return _round_metric(amount * factors[unit])


def validate_requested_profiles(profile_names: Sequence[str]) -> Tuple[str, ...]:
    """Validate a non-empty, duplicate-free subset of local benchmark profiles."""
    names = tuple(profile_names)
    if not names:
        raise ValueError("At least one runtime profile is required.")

    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise ValueError(
            "Duplicate runtime profiles are not allowed: " + ", ".join(duplicates)
        )

    invalid = [name for name in names if name not in EXECUTABLE_PROFILES]
    if invalid:
        raise ValueError(
            "Runtime profile(s) are not executable by this local benchmark: "
            + ", ".join(invalid)
            + ". Allowed profiles: "
            + ", ".join(EXECUTABLE_PROFILES)
            + ". cloud-ready remains REFERENCE_ONLY."
        )

    for name in names:
        profile = get_runtime_profile(name)
        if profile["execution"]["mode"] != "local":
            raise ValueError(f"Runtime profile '{name}' is not local.")
        if profile["submission"]["mode"] != "local":
            raise ValueError(f"Runtime profile '{name}' is not locally submitted.")
        if profile["submission"]["mode"] == "reference-only":
            raise ValueError(f"Runtime profile '{name}' is reference-only.")

    return names


def public_profile_contract(profile_name: str) -> Dict[str, Any]:
    """Project an executable profile onto the safe benchmark configuration."""
    validate_requested_profiles((profile_name,))
    profile = get_runtime_profile(profile_name)
    spark = profile["spark"]
    return {
        "runtime_profile": profile_name,
        "execution": {
            "mode": profile["execution"]["mode"],
            "processing": profile["execution"]["processing"],
            "max_runtime_minutes_expectation": profile["execution"].get(
                "max_runtime_minutes"
            ),
            "submission_mode": profile["submission"]["mode"],
        },
        "configured_volume": {
            name: int(value)
            for name, value in profile["batch"].items()
        },
        "spark_configured": {
            key: spark.get(key)
            for key in SAFE_SPARK_KEYS
        },
        "resources_configured": {
            "driver_memory": spark["driver_memory"],
            "executor_memory": spark["executor_memory"],
            "executor_instances": int(spark["executor_instances"]),
            "driver_cores": None,
            "executor_cores": None,
            "separate_executor_processes": False,
            "interpretation": (
                "local[*] uses local scheduler threads; configured executor values "
                "do not prove distributed executor processes."
            ),
        },
    }


def worker_timeout_seconds(profile_name: str) -> int:
    """Bound a worker by its profile expectation plus startup/validation grace."""
    validate_requested_profiles((profile_name,))
    profile = get_runtime_profile(profile_name)
    expected_minutes = int(profile["execution"]["max_runtime_minutes"])
    return expected_minutes * 60 + WORKER_TIMEOUT_GRACE_SECONDS


def cloud_ready_reference() -> Dict[str, Any]:
    profile = get_runtime_profile("cloud-ready")
    return {
        "runtime_profile": "cloud-ready",
        "status": "REFERENCE_ONLY",
        "executed": False,
        "submission_mode": profile["submission"]["mode"],
        "interpretation": (
            "Configuration reference for future environment validation; no cloud "
            "runtime, autoscaling, throughput, or cost evidence is produced."
        ),
    }


def select_observed_bottleneck(
    stages: Sequence[Mapping[str, Any]],
    eligible_stage_names: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """Select the first slowest successful stage with deterministic tie handling."""
    eligible = set(eligible_stage_names) if eligible_stage_names is not None else None
    measured: List[Tuple[int, str, float]] = []
    for index, stage in enumerate(stages):
        name = stage.get("name")
        if not isinstance(name, str):
            continue
        if eligible is not None and name not in eligible:
            continue
        if stage.get("status") != "SUCCESS":
            continue
        try:
            duration = float(stage.get("duration_seconds", 0))
        except (TypeError, ValueError):
            continue
        if duration < 0:
            continue
        measured.append((index, name, duration))

    if not measured:
        return {
            "status": "UNAVAILABLE",
            "method": "max_wall_clock_duration",
            "evidence_level": "observed_slowest_stage_only",
        }

    total = sum(item[2] for item in measured)
    selected = max(measured, key=lambda item: (item[2], -item[0]))
    return {
        "status": "OBSERVED",
        "stage": selected[1],
        "duration_seconds": _round_metric(selected[2]),
        "share_of_measured_stage_time_percent": (
            _round_metric((selected[2] / total) * 100) if total > 0 else 0.0
        ),
        "method": "max_wall_clock_duration",
        "evidence_level": "observed_slowest_stage_only",
    }


def build_observed_bottlenecks(
    stages: Sequence[Mapping[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    return {
        "overall": select_observed_bottleneck(stages),
        "processing": select_observed_bottleneck(
            stages,
            eligible_stage_names=PROCESSING_STAGE_NAMES,
        ),
    }


def _stage_duration_sum(
    stages: Sequence[Mapping[str, Any]],
    names: Iterable[str],
) -> float:
    selected = set(names)
    total = 0.0
    for stage in stages:
        if stage.get("name") not in selected:
            continue
        try:
            total += float(stage.get("duration_seconds", 0))
        except (TypeError, ValueError):
            continue
    return _round_metric(total)


def _nonnegative_int(value: Any) -> Optional[int]:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _finite_nonnegative_number(value: Any) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
        and float(value) >= 0
    )


def _numbers_match(left: Any, right: Any, tolerance: float = 0.01) -> bool:
    if not _finite_nonnegative_number(left):
        return False
    if not _finite_nonnegative_number(right):
        return False
    return abs(float(left) - float(right)) <= tolerance


def _source_volume_failures(
    observed: Any,
    configured_volume: Mapping[str, Any],
) -> List[str]:
    failures: List[str] = []
    if not isinstance(observed, Mapping):
        return ["observed_source_records_invalid"]

    by_source = observed.get("by_source")
    total = _nonnegative_int(observed.get("total"))
    if not isinstance(by_source, Mapping):
        return ["observed_source_map_invalid"]
    if set(by_source) != set(EXPECTED_SOURCE_NAMES):
        failures.append("observed_source_keys_mismatch")

    normalized: Dict[str, int] = {}
    for source_name in EXPECTED_SOURCE_NAMES:
        count = _nonnegative_int(by_source.get(source_name))
        if count is None:
            failures.append(f"observed_source_count_invalid:{source_name}")
        else:
            normalized[source_name] = count

    if total is None or total <= 0:
        failures.append("observed_source_records_empty")
    elif len(normalized) == len(EXPECTED_SOURCE_NAMES):
        if sum(normalized.values()) != total:
            failures.append("observed_source_total_mismatch")

    exact_expectations = {
        "clientes": configured_volume.get("clientes"),
        "agencias": configured_volume.get("agencias"),
        "produtos": configured_volume.get("produtos"),
        "transacoes": configured_volume.get("transacoes"),
        "eventos_digitais": configured_volume.get("eventos_digitais_file"),
    }
    for source_name, expected in exact_expectations.items():
        if normalized.get(source_name) != expected:
            failures.append(f"configured_source_volume_mismatch:{source_name}")

    client_count = _nonnegative_int(configured_volume.get("clientes"))
    accounts_per_client = _nonnegative_int(
        configured_volume.get("accounts_per_client")
    )
    account_count = normalized.get("contas")
    if (
        client_count is None
        or accounts_per_client is None
        or account_count is None
        or not (
            client_count
            <= account_count
            <= client_count * accounts_per_client
        )
    ):
        failures.append("configured_source_volume_mismatch:contas")

    cards_per_account = _nonnegative_int(
        configured_volume.get("cards_per_account")
    )
    card_count = normalized.get("cartoes")
    if (
        account_count is None
        or cards_per_account is None
        or card_count is None
        or not (1 <= card_count <= account_count * cards_per_account)
    ):
        failures.append("configured_source_volume_mismatch:cartoes")

    return failures


def _metric_consistency_failures(
    profile_result: Mapping[str, Any],
    stages: Sequence[Mapping[str, Any]],
) -> List[str]:
    failures: List[str] = []
    durations = profile_result.get("durations_seconds")
    if not isinstance(durations, Mapping):
        return ["durations_invalid"]

    duration_values: Dict[str, Any] = {}
    for name in ("pipeline", "validation", "total", "unattributed_overhead"):
        value = durations.get(name)
        if not _finite_nonnegative_number(value):
            failures.append(f"duration_invalid:{name}")
        else:
            duration_values[name] = value

    expected_pipeline = _stage_duration_sum(stages, PIPELINE_STAGE_NAMES)
    expected_validation = _stage_duration_sum(stages, VALIDATION_STAGE_NAMES)
    if not _numbers_match(duration_values.get("pipeline"), expected_pipeline):
        failures.append("pipeline_duration_mismatch")
    if not _numbers_match(
        duration_values.get("validation"),
        expected_validation,
    ):
        failures.append("validation_duration_mismatch")

    if len(duration_values) == 4:
        reconciled_total = (
            float(duration_values["pipeline"])
            + float(duration_values["validation"])
            + float(duration_values["unattributed_overhead"])
        )
        if not _numbers_match(duration_values["total"], reconciled_total):
            failures.append("total_duration_mismatch")
        if (
            float(duration_values["pipeline"]) <= 0
            or float(duration_values["total"]) <= 0
        ):
            failures.append("duration_basis_empty")

    by_stage = durations.get("by_stage")
    stage_durations = {
        str(stage.get("name")): stage.get("duration_seconds")
        for stage in stages
        if isinstance(stage, Mapping) and isinstance(stage.get("name"), str)
    }
    if not isinstance(by_stage, Mapping) or set(by_stage) != set(stage_durations):
        failures.append("stage_duration_map_mismatch")
    else:
        for name, expected_duration in stage_durations.items():
            if not _numbers_match(by_stage.get(name), expected_duration):
                failures.append(f"stage_duration_map_mismatch:{name}")

    observed = profile_result.get("observed_source_records")
    source_total = (
        _nonnegative_int(observed.get("total"))
        if isinstance(observed, Mapping)
        else None
    )
    throughput = profile_result.get("throughput")
    if not isinstance(throughput, Mapping):
        failures.append("throughput_invalid")
    else:
        if throughput.get("basis") != "observed_source_records":
            failures.append("throughput_basis_mismatch")
        if throughput.get("record_count") != source_total:
            failures.append("throughput_record_count_mismatch")
        expected_pipeline_rate = calculate_throughput(
            source_total,
            duration_values.get("pipeline"),
        )
        expected_total_rate = calculate_throughput(
            source_total,
            duration_values.get("total"),
        )
        if not _numbers_match(
            throughput.get("pipeline_records_per_second"),
            expected_pipeline_rate,
            tolerance=0.001,
        ):
            failures.append("pipeline_throughput_mismatch")
        if not _numbers_match(
            throughput.get("end_to_end_records_per_second"),
            expected_total_rate,
            tolerance=0.001,
        ):
            failures.append("end_to_end_throughput_mismatch")

    if profile_result.get("observed_bottlenecks") != build_observed_bottlenecks(
        stages
    ):
        failures.append("observed_bottlenecks_mismatch")

    return failures


def evaluate_profile_status(profile_result: Mapping[str, Any]) -> Tuple[str, List[str]]:
    """Apply functional, quality, masking, and monitoring gates fail-closed."""
    failures: List[str] = []
    stages = profile_result.get("stages")
    if not isinstance(stages, list):
        stages = []
        failures.append("stages_missing")

    stage_names = [
        stage.get("name")
        for stage in stages
        if isinstance(stage, Mapping)
    ]
    if stage_names != list(EXPECTED_STAGE_NAMES):
        failures.append("stage_sequence_mismatch")
    stages_by_name = {
        stage.get("name"): stage
        for stage in stages
        if isinstance(stage, Mapping) and isinstance(stage.get("name"), str)
    }
    for name in EXPECTED_STAGE_NAMES:
        stage = stages_by_name.get(name)
        if stage is None:
            failures.append(f"stage_missing:{name}")
        elif stage.get("status") != "SUCCESS":
            failures.append(f"stage_failed:{name}")
        elif not _finite_nonnegative_number(stage.get("duration_seconds")):
            failures.append(f"stage_duration_invalid:{name}")
    failures.extend(_metric_consistency_failures(profile_result, stages))

    observed = profile_result.get("observed_source_records", {})
    configured_volume = profile_result.get("configured_volume", {})
    if not isinstance(configured_volume, Mapping):
        configured_volume = {}
        failures.append("configured_volume_invalid")
    failures.extend(
        _source_volume_failures(observed, configured_volume)
    )

    counts = profile_result.get("layer_counts", {})
    for layer in (
        "bronze",
        "raw_vault_hubs",
        "raw_vault_links",
        "raw_vault_satellites",
        "gold",
    ):
        count = (
            _nonnegative_int(counts.get(layer))
            if isinstance(counts, Mapping)
            else None
        )
        if count is None or count <= 0:
            failures.append(f"layer_empty:{layer}")

    if isinstance(counts, Mapping) and isinstance(observed, Mapping):
        source_total = _nonnegative_int(observed.get("total"))
        bronze_total = _nonnegative_int(counts.get("bronze"))
        if (
            source_total is not None
            and source_total > 0
            and bronze_total != source_total
        ):
            failures.append("bronze_source_count_mismatch")

    quality = profile_result.get("quality", {})
    if not isinstance(quality, Mapping) or quality.get("status") != "PASS":
        failures.append("data_vault_quality_gate_failed")
    else:
        checks = quality.get("checks", {})
        for name in ("lineage", "gold_lineage"):
            if not isinstance(checks, Mapping) or checks.get(name) != "PASS":
                failures.append(f"quality_check_failed:{name}")
        if quality.get("failed_checks") != []:
            failures.append("quality_failed_checks_not_empty")

    masking = profile_result.get("masking", {})
    masking_failure_count = (
        _nonnegative_int(masking.get("failure_count"))
        if isinstance(masking, Mapping)
        else None
    )
    masking_categories = (
        masking.get("failure_categories")
        if isinstance(masking, Mapping)
        else None
    )
    if (
        not isinstance(masking, Mapping)
        or masking.get("status") != "PASS"
        or masking_failure_count != 0
    ):
        failures.append("masking_failed")
    if not isinstance(masking_categories, Mapping):
        failures.append("masking_categories_invalid")
    else:
        category_counts = [
            _nonnegative_int(value)
            for value in masking_categories.values()
        ]
        if any(value is None for value in category_counts):
            failures.append("masking_categories_invalid")
        elif masking_failure_count != sum(category_counts):
            failures.append("masking_failure_count_mismatch")

    monitoring = profile_result.get("monitoring", {})
    monitoring_count = (
        _nonnegative_int(monitoring.get("event_count"))
        if isinstance(monitoring, Mapping)
        else None
    )
    monitoring_summary = (
        monitoring.get("summary")
        if isinstance(monitoring, Mapping)
        else None
    )
    if (
        not isinstance(monitoring, Mapping)
        or monitoring.get("status") != "READABLE"
        or monitoring_count is None
        or monitoring_count < len(EXPECTED_MONITORING_EVENTS)
    ):
        failures.append("monitoring_events_insufficient")
    if (
        not isinstance(monitoring_summary, list)
        or monitoring_count != len(monitoring_summary)
    ):
        failures.append("monitoring_summary_mismatch")
    else:
        if monitoring_count != len(EXPECTED_MONITORING_EVENTS):
            failures.append("monitoring_event_count_mismatch")
        expected_event_layers = {
            (pipeline_name, task_name): layer_name
            for pipeline_name, task_name, layer_name in EXPECTED_MONITORING_EVENTS
        }
        observed_event_keys: List[Tuple[str, str]] = []
        for event in monitoring_summary:
            if not isinstance(event, Mapping):
                failures.append("monitoring_event_invalid")
                continue
            event_key = (
                event.get("pipeline_name"),
                event.get("task_name"),
            )
            if event.get("status") != "SUCCESS":
                failures.append("monitoring_event_failed")
            if event_key not in expected_event_layers:
                failures.append("monitoring_event_set_mismatch")
                continue
            observed_event_keys.append(event_key)
            layer_name = expected_event_layers[event_key]
            expected_rows = (
                _nonnegative_int(counts.get(layer_name))
                if isinstance(counts, Mapping)
                else None
            )
            if event.get("rows_written") != expected_rows:
                failures.append(
                    f"monitoring_rows_written_mismatch:{layer_name}"
                )
            if _nonnegative_int(event.get("rows_read")) is None:
                failures.append("monitoring_rows_read_invalid")
            if not _finite_nonnegative_number(event.get("duration_seconds")):
                failures.append("monitoring_duration_invalid")
        if (
            len(set(observed_event_keys)) != len(EXPECTED_MONITORING_EVENTS)
            or set(observed_event_keys) != set(expected_event_layers)
        ):
            failures.append("monitoring_event_set_mismatch")

    existing = profile_result.get("execution_error")
    if existing:
        failures.append("execution_error")

    unique_failures = list(dict.fromkeys(failures))
    return ("SUCCESS" if not unique_failures else "FAILURE", unique_failures)


def validate_profile_result_schema(profile_result: Mapping[str, Any]) -> List[str]:
    if not isinstance(profile_result, Mapping):
        return ["profile_result_not_mapping"]
    failures = [
        f"missing_field:{field}"
        for field in REQUIRED_PROFILE_RESULT_FIELDS
        if field not in profile_result
    ]
    if profile_result.get("schema_version") != SCHEMA_VERSION:
        failures.append("unsupported_schema_version")
    if profile_result.get("runtime_profile") not in EXECUTABLE_PROFILES:
        failures.append("invalid_runtime_profile")
    if profile_result.get("status") not in {"SUCCESS", "FAILURE"}:
        failures.append("invalid_status")
    if not isinstance(profile_result.get("stages"), list):
        failures.append("stages_not_list")
    return failures


def _nonfinite_number_locations(value: Any, location: str = "$") -> List[str]:
    failures: List[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            failures.extend(
                _nonfinite_number_locations(item, f"{location}.{key}")
            )
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            failures.extend(
                _nonfinite_number_locations(item, f"{location}[{index}]")
            )
    elif (
        isinstance(value, float)
        and not math.isfinite(value)
    ):
        failures.append(f"nonfinite_number:{location}")
    return failures


def _observed_spark_failures(
    profile_result: Mapping[str, Any],
    configured_spark: Mapping[str, Any],
) -> List[str]:
    failures: List[str] = []
    spark = profile_result.get("spark")
    observed = spark.get("observed") if isinstance(spark, Mapping) else None
    partitions = profile_result.get("partitions")
    observed_partitions = (
        partitions.get("observed")
        if isinstance(partitions, Mapping)
        else None
    )
    if not isinstance(observed, Mapping):
        return ["spark_observed_missing"]
    if not isinstance(observed_partitions, Mapping):
        failures.append("partitions_observed_missing")
        observed_partitions = {}

    expected_observed = {
        "master": str(configured_spark.get("master")),
        "driver_memory": str(configured_spark.get("driver_memory")),
        "executor_memory": str(configured_spark.get("executor_memory")),
        "executor_instances": str(
            configured_spark.get("executor_instances")
        ),
        "shuffle_partitions": str(
            configured_spark.get("shuffle_partitions")
        ),
        "delta_snapshot_partitions": str(
            configured_spark.get("shuffle_partitions")
        ),
        "adaptive_enabled": str(
            configured_spark.get("adaptive_enabled")
        ).lower(),
    }
    for key, expected in expected_observed.items():
        if observed.get(key) != expected:
            failures.append(f"spark_observed_mismatch:{key}")

    default_parallelism = _nonnegative_int(
        observed.get("default_parallelism")
    )
    if default_parallelism is None or default_parallelism <= 0:
        failures.append("spark_default_parallelism_invalid")

    expected_partitions = str(configured_spark.get("shuffle_partitions"))
    for key in ("shuffle_partitions", "delta_snapshot_partitions"):
        if observed_partitions.get(key) != expected_partitions:
            failures.append(f"partitions_observed_mismatch:{key}")
    if (
        _nonnegative_int(observed_partitions.get("default_parallelism"))
        != default_parallelism
    ):
        failures.append("partitions_default_parallelism_mismatch")

    return failures


def validate_profile_result_semantics(
    profile_result: Mapping[str, Any],
    expected_profile: str,
) -> List[str]:
    """Recompute trust-critical gates instead of trusting a worker status."""
    failures: List[str] = []
    contract = public_profile_contract(expected_profile)

    if profile_result.get("runtime_profile") != expected_profile:
        failures.append("runtime_profile_mismatch")
    if profile_result.get("configured_volume") != contract["configured_volume"]:
        failures.append("configured_volume_mismatch")

    spark = profile_result.get("spark")
    configured_spark = (
        spark.get("configured", {}) if isinstance(spark, Mapping) else {}
    )
    if configured_spark != contract["spark_configured"]:
        failures.append("spark_configuration_mismatch")
    else:
        failures.extend(
            _observed_spark_failures(profile_result, configured_spark)
        )

    resources = profile_result.get("resources")
    configured_resources = (
        resources.get("configured", {})
        if isinstance(resources, Mapping)
        else {}
    )
    expected_resources = contract["resources_configured"]
    if configured_resources != expected_resources:
        failures.append("resource_configuration_mismatch")

    observed_spark = (
        spark.get("observed", {}) if isinstance(spark, Mapping) else {}
    )
    observed_resources = (
        resources.get("observed", {})
        if isinstance(resources, Mapping)
        else {}
    )
    expected_observed_resources = {
        "master": observed_spark.get("master"),
        "default_parallelism": observed_spark.get("default_parallelism"),
        "separate_executor_processes": False,
        "interpretation": LOCAL_RESOURCE_OBSERVED_INTERPRETATION,
    }
    if observed_resources != expected_observed_resources:
        failures.append("resource_observation_mismatch")

    partitions = profile_result.get("partitions")
    configured_partitions = (
        partitions.get("configured", {})
        if isinstance(partitions, Mapping)
        else {}
    )
    expected_partition_count = contract["spark_configured"].get(
        "shuffle_partitions"
    )
    if configured_partitions != {
        "shuffle_partitions": expected_partition_count,
        "delta_snapshot_partitions": expected_partition_count,
    }:
        failures.append("partition_configuration_mismatch")

    if profile_result.get("limitations") != list(CLAIM_LIMITS):
        failures.append("claim_limits_mismatch")

    derived_status, derived_failures = evaluate_profile_status(profile_result)
    failures.extend(derived_failures)
    if profile_result.get("status") == "SUCCESS":
        if derived_status != "SUCCESS" or derived_failures:
            failures.append("success_status_not_supported_by_gates")
        if profile_result.get("validation_failures") != []:
            failures.append("success_contains_validation_failures")
    elif (
        profile_result.get("status") == "FAILURE"
        and not profile_result.get("validation_failures")
    ):
        failures.append("failure_without_validation_failures")

    failures.extend(_nonfinite_number_locations(profile_result))
    return list(dict.fromkeys(failures))


def validate_public_payload(payload: Any) -> List[str]:
    """Detect path, credential, environment, or sample leakage in public JSON."""
    failures: List[str] = []

    def walk(value: Any, location: str) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                normalized_key = str(key).lower()
                if normalized_key in FORBIDDEN_PUBLIC_KEYS:
                    failures.append(f"forbidden_key:{location}.{normalized_key}")
                if "access_key" in normalized_key or "secret_key" in normalized_key:
                    failures.append(f"credential_key:{location}.{normalized_key}")
                walk(item, f"{location}.{key}")
            return
        if isinstance(value, (list, tuple)):
            for index, item in enumerate(value):
                walk(item, f"{location}[{index}]")
            return
        if isinstance(value, str):
            for name, pattern in FORBIDDEN_PUBLIC_STRING_PATTERNS:
                if pattern.search(value):
                    failures.append(f"forbidden_string:{name}:{location}")

    walk(payload, "$")
    return list(dict.fromkeys(failures))


def sanitize_error_message(
    error: BaseException,
    sensitive_paths: Sequence[Path] = (),
) -> str:
    """Remove known local roots and generic absolute-path patterns from errors."""
    message = str(error).replace("\r", " ").replace("\n", " ")
    roots = [REPO_ROOT, *sensitive_paths]
    for root in roots:
        candidates = {str(root), str(root.resolve())}
        try:
            candidates.add(root.resolve().as_uri())
        except ValueError:
            pass
        for candidate in sorted(candidates, key=len, reverse=True):
            if candidate:
                message = message.replace(candidate, "<REDACTED_PATH>")

    message = re.sub(r"file:/+[^\s,;)\]\"']+", "<REDACTED_URI>", message)
    message = re.sub(
        r"\b[A-Za-z]:[\\/][^\s,;)\]\"']+",
        "<REDACTED_PATH>",
        message,
    )
    message = re.sub(
        r"(?<![A-Za-z0-9])/"
        r"(?:home|mnt/[A-Za-z]|opt|repo|root|tmp|Users|var|workspace)/"
        r"[^\s,;)\]\"']*",
        "<REDACTED_PATH>",
        message,
    )
    return message[:500] or type(error).__name__


def compare_profile_runs(profile_results: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    by_name = {
        result.get("runtime_profile"): result
        for result in profile_results
        if isinstance(result, Mapping)
    }
    small = by_name.get("local-small")
    medium = by_name.get("local-medium")
    comparison: Dict[str, Any] = {
        "profiles_compared": [
            name for name in EXECUTABLE_PROFILES if name in by_name
        ],
        "linear_speedup_required": False,
        "performance_threshold_applied": False,
        "horizontal_scaling_demonstrated": False,
        "acceptance_basis": (
            "completion, positive layer counts, Data Vault lineage, masking, "
            "monitoring, and payload safety"
        ),
    }
    if not small or not medium:
        comparison["status"] = "PARTIAL"
        return comparison

    small_spark = small.get("spark", {}).get("configured", {})
    medium_spark = medium.get("spark", {}).get("configured", {})
    small_observed = small.get("spark", {}).get("observed", {})
    medium_observed = medium.get("spark", {}).get("observed", {})
    comparison.update(
        {
            "status": (
                "VALID"
                if small.get("status") == medium.get("status") == "SUCCESS"
                else "FUNCTIONAL_FAILURE"
            ),
            "integrity_preserved": (
                small.get("status") == medium.get("status") == "SUCCESS"
            ),
            "observed_input_record_multiplier": _ratio(
                medium.get("observed_source_records", {}).get("total"),
                small.get("observed_source_records", {}).get("total"),
            ),
            "pipeline_duration_multiplier": _ratio(
                medium.get("durations_seconds", {}).get("pipeline"),
                small.get("durations_seconds", {}).get("pipeline"),
            ),
            "end_to_end_duration_multiplier": _ratio(
                medium.get("durations_seconds", {}).get("total"),
                small.get("durations_seconds", {}).get("total"),
            ),
            "pipeline_throughput_ratio": _ratio(
                medium.get("throughput", {}).get("pipeline_records_per_second"),
                small.get("throughput", {}).get("pipeline_records_per_second"),
            ),
            "driver_memory_multiplier": _ratio(
                parse_memory_mib(medium_spark.get("driver_memory")),
                parse_memory_mib(small_spark.get("driver_memory")),
            ),
            "executor_memory_multiplier": _ratio(
                parse_memory_mib(medium_spark.get("executor_memory")),
                parse_memory_mib(small_spark.get("executor_memory")),
            ),
            "executor_instances_multiplier": _ratio(
                medium_spark.get("executor_instances"),
                small_spark.get("executor_instances"),
            ),
            "shuffle_partitions_multiplier": _ratio(
                medium_spark.get("shuffle_partitions"),
                small_spark.get("shuffle_partitions"),
            ),
            "observed_default_parallelism_multiplier": _ratio(
                medium_observed.get("default_parallelism"),
                small_observed.get("default_parallelism"),
            ),
            "bottlenecks": {
                "local-small": small.get("observed_bottlenecks", {}),
                "local-medium": medium.get("observed_bottlenecks", {}),
            },
            "interpretation": (
                "Ratios are descriptive local observations. Both profiles use "
                "local[*] and do not prove horizontal executor scaling."
            ),
        }
    )
    return comparison


def benchmark_status(
    requested_profiles: Sequence[str],
    profile_results: Sequence[Mapping[str, Any]],
) -> Tuple[str, List[str]]:
    failures: List[str] = []
    by_name = {
        result.get("runtime_profile"): result
        for result in profile_results
        if isinstance(result, Mapping)
    }
    for name in requested_profiles:
        result = by_name.get(name)
        if result is None:
            failures.append(f"profile_result_missing:{name}")
        elif result.get("status") != "SUCCESS":
            failures.append(f"profile_failed:{name}")
    return ("SUCCESS" if not failures else "FAILURE", failures)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
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
    temporary.replace(path)


def _read_profile_result(path: Path, profile_name: str) -> Dict[str, Any]:
    if not path.exists():
        return _failed_profile_stub(profile_name, "worker_result_missing")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _failed_profile_stub(profile_name, "worker_result_invalid_json")

    try:
        schema_failures = validate_profile_result_schema(payload)
        public_failures = validate_public_payload(payload)
        semantic_failures = (
            validate_profile_result_semantics(payload, profile_name)
            if not schema_failures
            else []
        )
    except Exception:
        return _failed_profile_stub(
            profile_name,
            "worker_result_contract_failed",
        )
    if schema_failures or public_failures or semantic_failures:
        return _failed_profile_stub(profile_name, "worker_result_contract_failed")
    return dict(payload)


def _failed_profile_stub(profile_name: str, reason: str) -> Dict[str, Any]:
    contract = public_profile_contract(profile_name)
    return {
        "schema_version": SCHEMA_VERSION,
        "runtime_profile": profile_name,
        "status": "FAILURE",
        "configured_volume": contract["configured_volume"],
        "observed_source_records": {"by_source": {}, "total": 0},
        "spark": {
            "version": None,
            "configured": contract["spark_configured"],
            "observed": {},
        },
        "resources": {
            "configured": contract["resources_configured"],
            "observed": {},
        },
        "partitions": {"configured": {}, "observed": {}, "tables": {}},
        "stages": [],
        "durations_seconds": {
            "pipeline": 0.0,
            "validation": 0.0,
            "total": 0.0,
            "unattributed_overhead": 0.0,
        },
        "throughput": {
            "basis": "observed_source_records",
            "pipeline_records_per_second": 0.0,
            "end_to_end_records_per_second": 0.0,
        },
        "layer_counts": {
            "bronze": 0,
            "raw_vault_hubs": 0,
            "raw_vault_links": 0,
            "raw_vault_satellites": 0,
            "gold": 0,
        },
        "quality": {"status": "NOT_RUN", "checks": {}, "failed_checks": []},
        "masking": {"status": "NOT_RUN", "failure_count": 0, "failure_categories": {}},
        "monitoring": {"status": "NOT_RUN", "event_count": 0, "summary": []},
        "observed_bottlenecks": build_observed_bottlenecks([]),
        "validation_failures": [reason],
        "execution_error": {"type": "WorkerContractError", "message": reason},
        "limitations": list(CLAIM_LIMITS),
    }


def _failed_benchmark_payload(
    profile_names: Sequence[str],
    reason: str,
    started_at: Optional[str] = None,
) -> Dict[str, Any]:
    profiles = validate_requested_profiles(profile_names)
    runs = [_failed_profile_stub(profile, reason) for profile in profiles]
    return {
        "schema_version": SCHEMA_VERSION,
        "benchmark_kind": BENCHMARK_KIND,
        "benchmark_id": (
            "scalability-failed-"
            + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        ),
        "status": "FAILURE",
        "started_at": started_at or _utc_now(),
        "finished_at": _utc_now(),
        "runtime_profiles": list(profiles),
        "runs": runs,
        "comparison": compare_profile_runs(runs),
        "cloud_ready_reference": cloud_ready_reference(),
        "validation_failures": [reason],
        "claim_limits": list(CLAIM_LIMITS),
    }


def build_worker_environment(
    profile_name: str,
    base_environment: Optional[Mapping[str, str]] = None,
    ivy_dir: Optional[Path] = None,
) -> Dict[str, str]:
    validate_requested_profiles((profile_name,))
    source_environment = (
        os.environ if base_environment is None else base_environment
    )
    environment = {
        key: source_environment[key]
        for key in SAFE_INHERITED_WORKER_ENV_KEYS
        if key in source_environment
    }
    environment.update(
        {
            "RUNTIME_PROFILE": profile_name,
            "DM_RUNTIME_PROFILE": profile_name,
            "SPARK_JARS_PACKAGES": "",
            "SPARK_IVY_DIR": str(
                ivy_dir
                if ivy_dir is not None
                else Path(tempfile.gettempdir())
                / f"dm-scalability-ivy-{profile_name}"
            ),
            "SPARK_LOCAL_IP": "127.0.0.1",
            "SPARK_USER": "nobody",
            "JAVA_TOOL_OPTIONS": (
                "-XX:-UseContainerSupport -Duser.home=/tmp"
            ),
            "PYSPARK_PYTHON": sys.executable,
            "PYSPARK_DRIVER_PYTHON": sys.executable,
            "MINIO_ENDPOINT": "127.0.0.1:9",
            "MINIO_ACCESS_KEY": "benchmark-local-access",
            "MINIO_SECRET_KEY": "benchmark-local-secret",
        }
    )
    return environment


def build_worker_command(
    profile_name: str,
    result_path: Path,
    work_dir: Path,
    log_level: str,
) -> List[str]:
    validate_requested_profiles((profile_name,))
    return [
        sys.executable,
        str(Path(__file__).resolve()),
        "--_worker-profile",
        profile_name,
        "--_worker-result-path",
        str(result_path),
        "--_worker-work-dir",
        str(work_dir),
        "--log-level",
        log_level,
    ]


def _terminate_worker_process_tree(process: subprocess.Popen) -> None:
    if os.name != "nt":
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        if process.poll() is None:
            try:
                process.wait(timeout=WORKER_TERMINATION_GRACE_SECONDS)
            except subprocess.TimeoutExpired:
                pass
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    else:
        try:
            subprocess.run(
                [
                    "taskkill.exe",
                    "/PID",
                    str(process.pid),
                    "/T",
                    "/F",
                ],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=WORKER_TERMINATION_GRACE_SECONDS,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        except (OSError, subprocess.TimeoutExpired):
            if process.poll() is None:
                process.kill()
    if process.poll() is None:
        try:
            process.wait(timeout=WORKER_TERMINATION_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def _run_worker_process(
    command: Sequence[str],
    cwd: Path,
    environment: Mapping[str, str],
    timeout_seconds: int,
    log_path: Path,
) -> int:
    popen_options: Dict[str, Any] = {
        "cwd": str(cwd),
        "env": dict(environment),
        "stdout": None,
        "stderr": subprocess.STDOUT,
    }
    if os.name == "nt":
        popen_options["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        popen_options["start_new_session"] = True

    with log_path.open("w", encoding="utf-8", errors="replace") as log_handle:
        popen_options["stdout"] = log_handle
        process = subprocess.Popen(list(command), **popen_options)
        try:
            return process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            _terminate_worker_process_tree(process)
            raise


def run_profile_subprocess(
    profile_name: str,
    orchestration_dir: Path,
    log_level: str,
) -> Dict[str, Any]:
    validate_requested_profiles((profile_name,))
    profile_dir = orchestration_dir / profile_name
    worker_dir = profile_dir / "runtime"
    result_path = profile_dir / "profile-result.json"
    try:
        profile_dir.mkdir(parents=True, exist_ok=False)
    except OSError:
        return _failed_profile_stub(profile_name, "profile_workdir_unavailable")

    command = build_worker_command(
        profile_name,
        result_path=result_path,
        work_dir=worker_dir,
        log_level=log_level,
    )
    try:
        worker_exit_code = _run_worker_process(
            command,
            cwd=REPO_ROOT,
            environment=build_worker_environment(
                profile_name,
                ivy_dir=profile_dir / ".ivy2",
            ),
            timeout_seconds=worker_timeout_seconds(profile_name),
            log_path=profile_dir / "worker.log",
        )
    except subprocess.TimeoutExpired:
        result = _failed_profile_stub(profile_name, "worker_timeout")
    except OSError:
        result = _failed_profile_stub(profile_name, "worker_spawn_failed")
    else:
        result = _read_profile_result(result_path, profile_name)
        if worker_exit_code != 0 and result.get("status") == "SUCCESS":
            result = _failed_profile_stub(
                profile_name,
                "worker_exit_code_nonzero",
            )
    print(
        PROFILE_RESULT_MARKER
        + json.dumps(
            result,
            ensure_ascii=False,
            sort_keys=True,
            allow_nan=False,
        ),
        flush=True,
    )
    return result


def orchestrate_benchmark(
    runtime_profiles: Sequence[str] = EXECUTABLE_PROFILES,
    result_path: Optional[Path] = None,
    work_dir: Optional[Path] = None,
    log_level: str = "WARN",
) -> Tuple[Dict[str, Any], int]:
    profiles = validate_requested_profiles(runtime_profiles)
    aggregate_result_path = (
        Path(result_path) if result_path is not None else None
    )
    if (
        aggregate_result_path is not None
        and aggregate_result_path.exists()
    ):
        raise ValueError(
            "Benchmark result path must not exist before a fresh execution."
        )
    started_at = _utc_now()
    benchmark_id = (
        "scalability-"
        + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "-"
        + uuid.uuid4().hex[:8]
    )

    def execute(root: Path) -> List[Dict[str, Any]]:
        root.mkdir(parents=True, exist_ok=True)
        if any(root.iterdir()):
            raise ValueError(
                "Benchmark work directory must be empty to avoid stale state."
            )
        return [
            run_profile_subprocess(profile_name, root, log_level)
            for profile_name in profiles
        ]

    if work_dir is None:
        with tempfile.TemporaryDirectory(prefix="dm-scalability-benchmark-") as temp:
            runs = execute(Path(temp))
    else:
        runs = execute(Path(work_dir))

    status, failures = benchmark_status(profiles, runs)
    payload: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "benchmark_kind": BENCHMARK_KIND,
        "benchmark_id": benchmark_id,
        "status": status,
        "started_at": started_at,
        "finished_at": _utc_now(),
        "runtime_profiles": list(profiles),
        "runs": runs,
        "comparison": compare_profile_runs(runs),
        "cloud_ready_reference": cloud_ready_reference(),
        "validation_failures": failures,
        "claim_limits": list(CLAIM_LIMITS),
    }

    public_failures = validate_public_payload(payload)
    if public_failures:
        payload = _failed_benchmark_payload(
            profiles,
            "public_payload_contract_failed",
            started_at,
        )

    if aggregate_result_path is not None:
        try:
            _write_json(aggregate_result_path, payload)
        except Exception:
            payload["status"] = "FAILURE"
            payload["validation_failures"] = list(
                dict.fromkeys(
                    [
                        *payload.get("validation_failures", []),
                        "benchmark_result_write_failed",
                    ]
                )
            )

    print(
        BENCHMARK_RESULT_MARKER
        + json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            allow_nan=False,
        ),
        flush=True,
    )
    return payload, 0 if payload["status"] == "SUCCESS" else 1


def _as_file_uri(path: Path) -> str:
    return path.resolve().as_uri()


def _configure_worker_environment(
    profile_name: str,
    work_dir: Path,
    log_level: str,
) -> Dict[str, Any]:
    validate_requested_profiles((profile_name,))
    profile = get_runtime_profile(profile_name)
    spark = profile["spark"]

    sample_data = work_dir / "sample"
    local_paths = {
        "sample": sample_data,
        "bronze": work_dir / "bronze",
        "raw_vault": work_dir / "raw_vault",
        "business_vault": work_dir / "business_vault",
        "gold": work_dir / "gold",
        "monitoring": work_dir / "monitoring",
    }
    work_dir.mkdir(parents=True, exist_ok=True)
    if any(work_dir.iterdir()):
        raise ValueError(
            "Worker work directory must be empty to avoid stale state."
        )

    configured = {
        "RUNTIME_PROFILE": profile_name,
        "DM_RUNTIME_PROFILE": profile_name,
        "SAMPLE_DATA_PATH": str(sample_data),
        "BRONZE_PATH": _as_file_uri(local_paths["bronze"]),
        "RAW_VAULT_PATH": _as_file_uri(local_paths["raw_vault"]),
        "BUSINESS_VAULT_PATH": _as_file_uri(local_paths["business_vault"]),
        "GOLD_PATH": _as_file_uri(local_paths["gold"]),
        "MONITORING_PATH": _as_file_uri(local_paths["monitoring"]),
        "SPARK_MASTER": str(spark["master"]),
        "SPARK_DRIVER_MEMORY": str(spark["driver_memory"]),
        "SPARK_EXECUTOR_MEMORY": str(spark["executor_memory"]),
        "SPARK_EXECUTOR_INSTANCES": str(spark["executor_instances"]),
        "SPARK_SQL_SHUFFLE_PARTITIONS": str(spark["shuffle_partitions"]),
        "SPARK_DELTA_SNAPSHOT_PARTITIONS": str(spark["shuffle_partitions"]),
        "SPARK_ADAPTIVE_ENABLED": str(spark["adaptive_enabled"]).lower(),
        "SPARK_LOG_LEVEL": log_level,
    }
    os.environ.update(configured)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ["SPARK_JARS_PACKAGES"] = ""
    os.environ.setdefault(
        "SPARK_IVY_DIR",
        str(work_dir / ".ivy2"),
    )

    return {"profile": profile, "paths": local_paths}


def _install_worker_import_paths() -> None:
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


def _summarize_stage_rows(
    stage_name: str,
    raw_result: Mapping[str, Any],
) -> Dict[str, Any]:
    nested = raw_result.get("results", {})
    items = nested.values() if isinstance(nested, Mapping) else []
    input_records = 0
    output_records = 0
    has_input = False
    has_output = False
    for item in items:
        if not isinstance(item, Mapping):
            continue
        if isinstance(item.get("rows_read"), int):
            input_records += int(item["rows_read"])
            has_input = True
        if isinstance(item.get("rows_written"), int):
            output_records += int(item["rows_written"])
            has_output = True

    if stage_name == "generate_sample_data":
        return {
            "input_records": None,
            "output_records": None,
            "throughput_records_per_second": None,
            "throughput_basis": None,
        }
    return {
        "input_records": input_records if has_input else None,
        "output_records": output_records if has_output else None,
        "throughput_records_per_second": None,
        "throughput_basis": "output_records" if has_output else None,
    }


def _run_timed_stage(
    name: str,
    action: Callable[[], Mapping[str, Any]],
    sensitive_paths: Sequence[Path],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    started_at = _utc_now()
    started_clock = time.perf_counter()
    raw_result: Dict[str, Any] = {}
    error: Optional[BaseException] = None
    try:
        candidate = action()
        raw_result = dict(candidate)
        reported_status = raw_result.get("status", "SUCCESS")
        if reported_status != "SUCCESS":
            error = RuntimeError(f"Stage {name} reported {reported_status}.")
    except BaseException as exc:  # worker must materialize partial failure evidence
        error = exc

    duration = _round_metric(time.perf_counter() - started_clock)
    row_summary = _summarize_stage_rows(name, raw_result)
    if row_summary["output_records"] is not None:
        row_summary["throughput_records_per_second"] = calculate_throughput(
            row_summary["output_records"],
            duration,
        )

    measurement: Dict[str, Any] = {
        "name": name,
        "status": "FAILURE" if error else "SUCCESS",
        "started_at": started_at,
        "finished_at": _utc_now(),
        "duration_seconds": duration,
        **row_summary,
    }
    if error:
        measurement["error"] = {
            "type": type(error).__name__,
            "message": sanitize_error_message(error, sensitive_paths),
        }

    print(
        "SCALABILITY_STAGE_RESULT="
        + json.dumps(
            measurement,
            ensure_ascii=False,
            sort_keys=True,
            allow_nan=False,
        ),
        flush=True,
    )
    return raw_result, measurement


def _source_records_from_bronze(
    bronze_result: Mapping[str, Any],
) -> Dict[str, Any]:
    by_source: Dict[str, int] = {}
    nested = bronze_result.get("results", {})
    if isinstance(nested, Mapping):
        for source_name, result in nested.items():
            if isinstance(result, Mapping) and isinstance(result.get("rows_read"), int):
                by_source[str(source_name)] = int(result["rows_read"])
    return {
        "by_source": dict(sorted(by_source.items())),
        "total": sum(by_source.values()),
    }


def _resolve_table_path(config: Any) -> Optional[str]:
    if isinstance(config, os.PathLike):
        config = os.fspath(config)
    if isinstance(config, str):
        return config
    if isinstance(config, Mapping):
        value = config.get("path")
        if isinstance(value, os.PathLike):
            value = os.fspath(value)
        return value if isinstance(value, str) else None
    return None


def _collect_table_group(spark, delta_io, registry: Mapping[str, Any]) -> Dict[str, Any]:
    tables: Dict[str, Any] = {}
    for table_name, config in registry.items():
        path = _resolve_table_path(config)
        if not path:
            tables[str(table_name)] = {
                "status": "INVALID_CONFIG",
                "num_rows": 0,
                "read_partitions": 0,
                "input_file_count": 0,
            }
            continue
        frame = delta_io.read_delta(spark, path)
        if frame is None:
            tables[str(table_name)] = {
                "status": "MISSING",
                "num_rows": 0,
                "read_partitions": 0,
                "input_file_count": 0,
            }
            continue
        tables[str(table_name)] = {
            "status": "READABLE",
            "num_rows": int(frame.count()),
            "read_partitions": int(frame.rdd.getNumPartitions()),
            "input_file_count": len(frame.inputFiles()),
        }
    return tables


def _table_group_total(tables: Mapping[str, Any]) -> int:
    return sum(
        int(table.get("num_rows", 0) or 0)
        for table in tables.values()
        if isinstance(table, Mapping)
    )


def _collect_monitoring_summary(spark, monitoring_logger, batch_id: str) -> Dict[str, Any]:
    frame = monitoring_logger.get_execution_summary(spark, batch_id)
    if frame is None:
        return {"status": "MISSING", "event_count": 0, "summary": []}

    selected = frame.select(
        "pipeline_name",
        "task_name",
        "status",
        "rows_read",
        "rows_written",
        "duration_seconds",
    )
    rows = selected.collect()
    summary = []
    for row in rows:
        values = row.asDict()
        summary.append(
            {
                "pipeline_name": str(values.get("pipeline_name") or ""),
                "task_name": str(values.get("task_name") or ""),
                "status": str(values.get("status") or ""),
                "rows_read": int(values.get("rows_read") or 0),
                "rows_written": int(values.get("rows_written") or 0),
                "duration_seconds": _round_metric(values.get("duration_seconds") or 0),
            }
        )
    summary.sort(key=lambda item: (item["pipeline_name"], item["task_name"], item["status"]))
    return {"status": "READABLE", "event_count": len(summary), "summary": summary}


def _spark_conf_value(spark, key: str) -> Optional[str]:
    try:
        return str(spark.conf.get(key))
    except Exception:
        return None


def _observed_spark_configuration(spark) -> Dict[str, Any]:
    return {
        "master": str(spark.sparkContext.master),
        "default_parallelism": int(spark.sparkContext.defaultParallelism),
        "driver_memory": _spark_conf_value(spark, "spark.driver.memory"),
        "executor_memory": _spark_conf_value(spark, "spark.executor.memory"),
        "executor_instances": _spark_conf_value(spark, "spark.executor.instances"),
        "shuffle_partitions": _spark_conf_value(
            spark, "spark.sql.shuffle.partitions"
        ),
        "delta_snapshot_partitions": _spark_conf_value(
            spark, "spark.databricks.delta.snapshotPartitions"
        ),
        "adaptive_enabled": _spark_conf_value(
            spark, "spark.sql.adaptive.enabled"
        ),
    }


def _evaluate_data_vault_quality(
    spark,
    raw_vault_path: str,
    gold_path: str,
    evaluate_configured_gate,
) -> Dict[str, Any]:
    gate = evaluate_configured_gate(spark, raw_vault_path, gold_path, REPO_ROOT)
    quality = {
        "status": gate.get("status"),
        "checks": dict(gate.get("statuses", {})),
        "failed_checks": list(gate.get("failed_checks", [])),
    }
    return {
        "status": "SUCCESS" if quality["status"] == "PASS" else "FAILURE",
        "quality": quality,
    }


def _nested_positive_count(value: Any) -> int:
    if isinstance(value, Mapping):
        return sum(_nested_positive_count(item) for item in value.values())
    if isinstance(value, (list, tuple, set)):
        return len(value)
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value) if value > 0 else 0
    return 0


def _evaluate_masking(
    spark,
    config,
    delta_io,
    masking_function_samples,
    validate_gold_outputs,
) -> Dict[str, Any]:
    samples = masking_function_samples()
    gold = validate_gold_outputs(spark, config, delta_io)
    categories = {
        "sample_failures": sum(
            1
            for sample in samples.values()
            if isinstance(sample, Mapping) and not sample.get("passed")
        ),
        "forbidden_columns": _nested_positive_count(
            gold.get("forbidden_columns", {})
        ),
        "raw_pattern_hits": _nested_positive_count(
            gold.get("raw_pattern_hits", {})
        ),
        "protected_check_failures": _nested_positive_count(
            gold.get("protected_checks", {})
        ),
        "client_check_failures": _nested_positive_count(
            gold.get("cliente_checks", {})
        ),
        "risk_check_failures": _nested_positive_count(
            gold.get("risco_checks", {})
        ),
    }
    failure_count = sum(categories.values())
    masking = {
        "status": "PASS" if failure_count == 0 else "FAILURE",
        "failure_count": failure_count,
        "failure_categories": categories,
        "tables_checked": len(gold.get("tables", {})),
    }
    return {
        "status": "SUCCESS" if failure_count == 0 else "FAILURE",
        "masking": masking,
    }


def _collect_metrics_bundle(
    spark,
    config,
    delta_io,
    monitoring_logger,
    batch_id: str,
) -> Dict[str, Any]:
    table_groups = {
        "bronze": _collect_table_group(spark, delta_io, config.BRONZE_TABLES),
        "raw_vault_hubs": _collect_table_group(spark, delta_io, config.HUB_TABLES),
        "raw_vault_links": _collect_table_group(spark, delta_io, config.LINK_TABLES),
        "raw_vault_satellites": _collect_table_group(
            spark, delta_io, config.SATELLITE_TABLES
        ),
        "gold": _collect_table_group(spark, delta_io, config.GOLD_TABLES),
    }
    layer_counts = {
        name: _table_group_total(tables)
        for name, tables in table_groups.items()
    }
    unreadable = [
        f"{group}.{table_name}"
        for group, tables in table_groups.items()
        for table_name, table in tables.items()
        if table.get("status") != "READABLE"
    ]
    monitoring = _collect_monitoring_summary(spark, monitoring_logger, batch_id)
    observed_spark = _observed_spark_configuration(spark)
    return {
        "status": (
            "SUCCESS"
            if not unreadable and monitoring["status"] == "READABLE"
            else "FAILURE"
        ),
        "table_groups": table_groups,
        "layer_counts": layer_counts,
        "unreadable_tables": unreadable,
        "monitoring": monitoring,
        "spark_observed": observed_spark,
    }


def _empty_layer_counts() -> Dict[str, int]:
    return {
        "bronze": 0,
        "raw_vault_hubs": 0,
        "raw_vault_links": 0,
        "raw_vault_satellites": 0,
        "gold": 0,
    }


def _build_profile_payload(
    profile_name: str,
    spark_version: Optional[str],
    stages: Sequence[Mapping[str, Any]],
    bronze_result: Mapping[str, Any],
    quality_result: Mapping[str, Any],
    masking_result: Mapping[str, Any],
    metrics_result: Mapping[str, Any],
    total_duration: float,
    execution_error: Optional[Dict[str, str]],
) -> Dict[str, Any]:
    contract = public_profile_contract(profile_name)
    source_records = _source_records_from_bronze(bronze_result)
    layer_counts = dict(metrics_result.get("layer_counts", _empty_layer_counts()))
    quality = dict(
        quality_result.get(
            "quality",
            {"status": "NOT_RUN", "checks": {}, "failed_checks": []},
        )
    )
    masking = dict(
        masking_result.get(
            "masking",
            {
                "status": "NOT_RUN",
                "failure_count": 0,
                "failure_categories": {},
                "tables_checked": 0,
            },
        )
    )
    monitoring = dict(
        metrics_result.get(
            "monitoring",
            {"status": "NOT_RUN", "event_count": 0, "summary": []},
        )
    )
    spark_observed = dict(metrics_result.get("spark_observed", {}))
    table_groups = metrics_result.get("table_groups", {})

    partitions_by_table: Dict[str, Any] = {}
    if isinstance(table_groups, Mapping):
        for group, tables in table_groups.items():
            if not isinstance(tables, Mapping):
                continue
            partitions_by_table[str(group)] = {
                str(table_name): {
                    "read_partitions": int(table.get("read_partitions", 0) or 0),
                    "input_file_count": int(table.get("input_file_count", 0) or 0),
                }
                for table_name, table in tables.items()
                if isinstance(table, Mapping)
            }

    pipeline_duration = _stage_duration_sum(stages, PIPELINE_STAGE_NAMES)
    validation_duration = _stage_duration_sum(stages, VALIDATION_STAGE_NAMES)
    measured_duration = _round_metric(pipeline_duration + validation_duration)
    durations = {
        "pipeline": pipeline_duration,
        "validation": validation_duration,
        "total": _round_metric(total_duration),
        "unattributed_overhead": _round_metric(
            max(0.0, total_duration - measured_duration)
        ),
        "by_stage": {
            str(stage.get("name")): _round_metric(stage.get("duration_seconds", 0))
            for stage in stages
            if isinstance(stage, Mapping) and stage.get("name")
        },
    }
    configured_spark = contract["spark_configured"]
    result: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "runtime_profile": profile_name,
        "status": "UNKNOWN",
        "configured_volume": contract["configured_volume"],
        "observed_source_records": source_records,
        "spark": {
            "version": spark_version,
            "configured": configured_spark,
            "observed": spark_observed,
        },
        "resources": {
            "configured": contract["resources_configured"],
            "observed": {
                "master": spark_observed.get("master"),
                "default_parallelism": spark_observed.get("default_parallelism"),
                "separate_executor_processes": False,
                "interpretation": LOCAL_RESOURCE_OBSERVED_INTERPRETATION,
            },
        },
        "partitions": {
            "configured": {
                "shuffle_partitions": configured_spark.get("shuffle_partitions"),
                "delta_snapshot_partitions": configured_spark.get(
                    "shuffle_partitions"
                ),
            },
            "observed": {
                "shuffle_partitions": spark_observed.get("shuffle_partitions"),
                "delta_snapshot_partitions": spark_observed.get(
                    "delta_snapshot_partitions"
                ),
                "default_parallelism": spark_observed.get("default_parallelism"),
            },
            "tables": partitions_by_table,
        },
        "stages": [dict(stage) for stage in stages],
        "durations_seconds": durations,
        "throughput": {
            "basis": "observed_source_records",
            "record_count": source_records["total"],
            "pipeline_records_per_second": calculate_throughput(
                source_records["total"], pipeline_duration
            ),
            "end_to_end_records_per_second": calculate_throughput(
                source_records["total"], total_duration
            ),
        },
        "layer_counts": layer_counts,
        "quality": quality,
        "masking": masking,
        "monitoring": monitoring,
        "observed_bottlenecks": build_observed_bottlenecks(stages),
        "validation_failures": [],
        "limitations": list(CLAIM_LIMITS),
    }
    if execution_error is not None:
        result["execution_error"] = execution_error

    status, failures = evaluate_profile_status(result)
    result["status"] = status
    result["validation_failures"] = failures
    public_failures = validate_public_payload(result)
    if public_failures:
        result = _failed_profile_stub(
            profile_name,
            "public_payload_contract_failed",
        )
    return result


def run_worker(
    profile_name: str,
    result_path: Path,
    work_dir: Path,
    log_level: str,
) -> Tuple[Dict[str, Any], int]:
    if Path(result_path).exists():
        stale_result = _failed_profile_stub(
            profile_name,
            "worker_result_path_not_fresh",
        )
        print(
            PROFILE_RESULT_MARKER
            + json.dumps(
                stale_result,
                ensure_ascii=False,
                sort_keys=True,
                allow_nan=False,
            ),
            flush=True,
        )
        return stale_result, 1

    worker_started = time.perf_counter()
    paths = [Path(work_dir), Path(result_path)]
    stages: List[Dict[str, Any]] = []
    bronze_result: Dict[str, Any] = {}
    quality_result: Dict[str, Any] = {}
    masking_result: Dict[str, Any] = {}
    metrics_result: Dict[str, Any] = {}
    spark_version: Optional[str] = None
    execution_error: Optional[Dict[str, str]] = None
    spark_factory = None

    try:
        worker_config = _configure_worker_environment(
            profile_name,
            Path(work_dir),
            log_level,
        )
        profile = worker_config["profile"]
        local_paths = worker_config["paths"]
        paths.extend(local_paths.values())
        _install_worker_import_paths()

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
            _validate_gold_outputs,
        )
        from spark_session import SparkSessionFactory, create_spark_session

        spark_factory = SparkSessionFactory
        batch_id = (
            "scalability-"
            + profile_name.replace("-", "_")
            + "-"
            + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        )
        spark = create_spark_session()
        spark_version = str(spark.version)

        def execute(
            name: str,
            action: Callable[[], Mapping[str, Any]],
        ) -> Dict[str, Any]:
            raw, measurement = _run_timed_stage(name, action, paths)
            stages.append(measurement)
            if measurement["status"] != "SUCCESS":
                raise RuntimeError(f"Benchmark stage {name} failed.")
            return raw

        execute(
            "generate_sample_data",
            lambda: {
                "status": "SUCCESS",
                "files": generate_all_sample_data(
                    str(local_paths["sample"]),
                    runtime_profile=profile["id"],
                ),
            },
        )
        bronze_result = execute(
            "bronze",
            lambda: run_bronze_pipeline(
                spark,
                str(local_paths["sample"]),
                os.environ["BRONZE_PATH"],
                batch_id,
            ),
        )
        execute(
            "hubs",
            lambda: run_hubs_pipeline(
                spark,
                os.environ["BRONZE_PATH"],
                batch_id,
            ),
        )
        execute(
            "links",
            lambda: run_links_pipeline(
                spark,
                os.environ["BRONZE_PATH"],
                batch_id,
            ),
        )
        execute(
            "satellites",
            lambda: run_satellites_pipeline(
                spark,
                os.environ["BRONZE_PATH"],
                batch_id,
            ),
        )
        execute(
            "gold",
            lambda: run_business_vault_pipeline(
                spark,
                os.environ["RAW_VAULT_PATH"],
                os.environ["GOLD_PATH"],
                batch_id,
            ),
        )
        quality_result = execute(
            "data_vault_quality_gate",
            lambda: _evaluate_data_vault_quality(
                spark,
                os.environ["RAW_VAULT_PATH"],
                os.environ["GOLD_PATH"],
                evaluate_configured_gate,
            ),
        )
        masking_result = execute(
            "masking_gate",
            lambda: _evaluate_masking(
                spark,
                Config,
                DeltaIO,
                _masking_function_samples,
                _validate_gold_outputs,
            ),
        )
        metrics_result = execute(
            "metrics_collection",
            lambda: _collect_metrics_bundle(
                spark,
                Config,
                DeltaIO,
                MonitoringLogger,
                batch_id,
            ),
        )

    except BaseException as exc:  # worker must stop Spark and write a safe result
        execution_error = {
            "type": type(exc).__name__,
            "message": sanitize_error_message(exc, paths),
        }
    finally:
        if spark_factory is not None:
            try:
                spark_factory.stop()
            except Exception as exc:
                if execution_error is None:
                    execution_error = {
                        "type": type(exc).__name__,
                        "message": sanitize_error_message(exc, paths),
                    }

    total_duration = _round_metric(time.perf_counter() - worker_started)
    result = _build_profile_payload(
        profile_name=profile_name,
        spark_version=spark_version,
        stages=stages,
        bronze_result=bronze_result,
        quality_result=quality_result,
        masking_result=masking_result,
        metrics_result=metrics_result,
        total_duration=total_duration,
        execution_error=execution_error,
    )
    try:
        _write_json(Path(result_path), result)
    except Exception:
        result = _failed_profile_stub(
            profile_name,
            "worker_result_write_failed",
        )
    print(
        PROFILE_RESULT_MARKER
        + json.dumps(
            result,
            ensure_ascii=False,
            sort_keys=True,
            allow_nan=False,
        ),
        flush=True,
    )
    return result, 0 if result["status"] == "SUCCESS" else 1


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the controlled local scalability benchmark."
    )
    parser.add_argument(
        "--runtime-profiles",
        nargs="+",
        default=list(EXECUTABLE_PROFILES),
        help="Local profiles to execute. Defaults to local-small local-medium.",
    )
    parser.add_argument(
        "--result-path",
        default=None,
        help="Optional path for the aggregate public JSON result.",
    )
    parser.add_argument(
        "--work-dir",
        default=None,
        help="Optional private work directory. It is never included in the JSON.",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("SPARK_LOG_LEVEL", "WARN"),
        help="Spark log level.",
    )
    parser.add_argument("--_worker-profile", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--_worker-result-path", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--_worker-work-dir", default=None, help=argparse.SUPPRESS)
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        if args._worker_profile is not None:
            if not args._worker_result_path or not args._worker_work_dir:
                raise ValueError("Worker result and work directories are required.")
            _, return_code = run_worker(
                profile_name=args._worker_profile,
                result_path=Path(args._worker_result_path),
                work_dir=Path(args._worker_work_dir),
                log_level=args.log_level,
            )
            return return_code

        _, return_code = orchestrate_benchmark(
            runtime_profiles=args.runtime_profiles,
            result_path=Path(args.result_path) if args.result_path else None,
            work_dir=Path(args.work_dir) if args.work_dir else None,
            log_level=args.log_level,
        )
        return return_code
    except ValueError as exc:
        if args._worker_profile in EXECUTABLE_PROFILES:
            payload = _failed_profile_stub(
                args._worker_profile,
                "worker_configuration_invalid",
            )
            marker = PROFILE_RESULT_MARKER
        else:
            payload = {
                "schema_version": SCHEMA_VERSION,
                "benchmark_kind": BENCHMARK_KIND,
                "status": "FAILURE",
                "error": {
                    "type": type(exc).__name__,
                    "message": sanitize_error_message(exc),
                },
                "cloud_ready_reference": cloud_ready_reference(),
                "claim_limits": list(CLAIM_LIMITS),
            }
            marker = BENCHMARK_RESULT_MARKER
        print(
            marker
            + json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                allow_nan=False,
            ),
            flush=True,
        )
        return 2
    except Exception:
        if args._worker_profile in EXECUTABLE_PROFILES:
            payload = _failed_profile_stub(
                args._worker_profile,
                "worker_execution_failed",
            )
            marker = PROFILE_RESULT_MARKER
        else:
            payload = {
                "schema_version": SCHEMA_VERSION,
                "benchmark_kind": BENCHMARK_KIND,
                "status": "FAILURE",
                "error": {
                    "type": "BenchmarkExecutionError",
                    "message": "Benchmark execution failed.",
                },
                "cloud_ready_reference": cloud_ready_reference(),
                "claim_limits": list(CLAIM_LIMITS),
            }
            marker = BENCHMARK_RESULT_MARKER
        print(
            marker
            + json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                allow_nan=False,
            ),
            flush=True,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
