"""Controlled negative-path smoke for the local observability baseline.

The module keeps threshold loading, observation evaluation, payload validation,
and exit-code decisions free from Spark imports. Spark and the existing Bronze
pipeline are imported only by the runtime scenario executor.
"""

import argparse
import csv
import json
import math
import os
import re
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_THRESHOLDS_PATH = (
    REPO_ROOT / "config" / "observability" / "thresholds.yml"
)
RESULT_MARKER = "OBSERVABILITY_FAILURE_SMOKE_RESULT="

DETECTED_FAILURE_EXIT_CODE = 1
HARNESS_FAILURE_EXIT_CODE = 2
EXPECTED_THRESHOLD_SCOPE = "controlled_local_observability"
EXPECTED_RUNTIME_PROFILES = ("local-small",)
ALLOWED_LOG_LEVELS = ("ERROR", "WARN", "INFO", "DEBUG")
ALLOWED_OBSERVED_STAGE_STATUSES = {
    "FAILURE",
    "SUCCESS",
    "UNKNOWN",
    "NOT_EXECUTED",
}
SAFE_IDENTIFIER_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_.-]{0,127}")

EXPECTED_STAGES = (
    "generate_sample_data",
    "bronze",
    "raw_hubs",
    "raw_links",
    "raw_satellites",
    "gold",
)
EXPECTED_LAYERS = (
    "bronze",
    "raw_vault_hubs",
    "raw_vault_links",
    "raw_vault_satellites",
    "gold",
)
LAYER_STAGES = {
    "bronze": "bronze",
    "raw_vault_hubs": "raw_hubs",
    "raw_vault_links": "raw_links",
    "raw_vault_satellites": "raw_satellites",
    "gold": "gold",
}

SCENARIO_EXPECTATIONS = {
    "invalid-schema": {
        "failed_stage": "bronze",
        "rule_triggered": "source.schema.required_columns",
    },
    "missing-source": {
        "failed_stage": "bronze",
        "rule_triggered": "source.file.required",
    },
    "zero-volume": {
        "failed_stage": "bronze",
        "rule_triggered": "volume.minimum_rows",
    },
}

RULE_PRIORITY = {
    "source.file.required": 10,
    "source.schema.required_columns": 20,
    "volume.minimum_rows": 30,
    "volume.maximum_drop_percent": 40,
    "stage.maximum_duration_seconds": 50,
    "monitoring.minimum_events": 60,
    "quality.maximum_failures": 70,
    "masking.maximum_failures": 80,
}

REQUIRED_PAYLOAD_FIELDS = {
    "payload_version",
    "scenario",
    "runtime_profile",
    "detection_status",
    "pipeline_status",
    "failed_stage",
    "rule_triggered",
    "batch_id",
    "error_message",
    "started_at",
    "finished_at",
    "duration_seconds",
    "process_exit_code",
    "triggered_rules",
}
ALLOWED_PAYLOAD_FIELDS = REQUIRED_PAYLOAD_FIELDS | {
    "threshold_config_version",
    "observed_stage_status",
    "observation",
    "detections",
}

FORBIDDEN_PAYLOAD_KEYS = {
    "work_dir",
    "sample_data_path",
    "source_path",
    "bronze_path",
    "monitoring_path",
    "thresholds_path",
    "repo_root",
    "path",
    "password",
    "passwd",
    "secret",
    "token",
    "api_key",
    "access_key",
    "private_key",
    "credential",
    "authorization",
}

OBSERVATION_KEYS = {
    "source",
    "monitoring_events",
    "stage_durations_seconds",
    "layer_rows",
    "reference_layer_rows",
    "quality_failures",
    "masking_failures",
}


class ThresholdConfigError(ValueError):
    """Raised when the threshold document violates its public contract."""


class FailureSmokeError(RuntimeError):
    """Raised for safe, user-facing failure-smoke harness errors."""


def _default_log_level() -> str:
    configured = os.getenv("SPARK_LOG_LEVEL", "WARN").upper()
    return configured if configured in ALLOWED_LOG_LEVELS else "WARN"


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run controlled observability failure detection."
    )
    parser.add_argument(
        "--scenario",
        choices=tuple(SCENARIO_EXPECTATIONS),
        default="invalid-schema",
        help="Controlled source failure to inject.",
    )
    parser.add_argument(
        "--runtime-profile",
        choices=EXPECTED_RUNTIME_PROFILES,
        default="local-small",
        help="Runtime profile used by the controlled smoke.",
    )
    parser.add_argument(
        "--thresholds-path",
        default=str(DEFAULT_THRESHOLDS_PATH),
        help="Versioned threshold configuration.",
    )
    parser.add_argument(
        "--work-dir",
        default=None,
        help="Optional work directory. The default temporary directory is removed.",
    )
    parser.add_argument("--batch-id", default=None, help="Optional safe batch id.")
    parser.add_argument(
        "--log-level",
        choices=ALLOWED_LOG_LEVELS,
        default=_default_log_level(),
        help="Spark log level.",
    )
    return parser.parse_args(argv)


def _require_mapping(value: Any, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ThresholdConfigError(f"{context} must be a mapping.")
    return value


def _require_exact_keys(
    value: Mapping[str, Any],
    expected: Iterable[str],
    context: str,
) -> None:
    expected_set = set(expected)
    actual_set = set(value)
    missing = sorted(expected_set - actual_set)
    unknown = sorted(actual_set - expected_set)
    if missing:
        raise ThresholdConfigError(
            f"{context} is missing required keys: {', '.join(missing)}."
        )
    if unknown:
        raise ThresholdConfigError(
            f"{context} contains unknown keys: {', '.join(unknown)}."
        )


def _require_integer(
    value: Any,
    context: str,
    minimum: int = 0,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ThresholdConfigError(f"{context} must be an integer.")
    if value < minimum:
        raise ThresholdConfigError(
            f"{context} must be greater than or equal to {minimum}."
        )
    return value


def _require_number(
    value: Any,
    context: str,
    minimum: float = 0.0,
    maximum: Optional[float] = None,
    inclusive_minimum: bool = True,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ThresholdConfigError(f"{context} must be numeric.")
    number = float(value)
    if not math.isfinite(number):
        raise ThresholdConfigError(f"{context} must be finite.")
    if inclusive_minimum and number < minimum:
        raise ThresholdConfigError(
            f"{context} must be greater than or equal to {minimum}."
        )
    if not inclusive_minimum and number <= minimum:
        raise ThresholdConfigError(
            f"{context} must be greater than {minimum}."
        )
    if maximum is not None and number > maximum:
        raise ThresholdConfigError(
            f"{context} must be less than or equal to {maximum}."
        )
    return number


def validate_threshold_config(document: Any) -> Dict[str, Any]:
    """Validate and return the strict version-1 threshold document."""
    root = _require_mapping(document, "threshold configuration")
    _require_exact_keys(
        root,
        ("version", "scope", "runtime_profiles", "thresholds"),
        "threshold configuration",
    )

    if isinstance(root["version"], bool) or root["version"] != 1:
        raise ThresholdConfigError("threshold configuration version must be 1.")
    if root["scope"] != EXPECTED_THRESHOLD_SCOPE:
        raise ThresholdConfigError(
            "threshold configuration scope must be "
            f"{EXPECTED_THRESHOLD_SCOPE}."
        )

    profiles = root["runtime_profiles"]
    if profiles != list(EXPECTED_RUNTIME_PROFILES):
        raise ThresholdConfigError(
            "runtime_profiles must contain only local-small."
        )

    thresholds = _require_mapping(root["thresholds"], "thresholds")
    _require_exact_keys(
        thresholds,
        ("monitoring", "stage_duration", "volume", "quality", "masking"),
        "thresholds",
    )

    monitoring = _require_mapping(thresholds["monitoring"], "monitoring")
    _require_exact_keys(monitoring, ("minimum_events",), "monitoring")
    _require_integer(
        monitoring["minimum_events"],
        "monitoring.minimum_events",
        minimum=1,
    )

    stage_duration = _require_mapping(
        thresholds["stage_duration"],
        "stage_duration",
    )
    _require_exact_keys(
        stage_duration,
        ("maximum_seconds_by_stage",),
        "stage_duration",
    )
    stage_limits = _require_mapping(
        stage_duration["maximum_seconds_by_stage"],
        "stage_duration.maximum_seconds_by_stage",
    )
    _require_exact_keys(
        stage_limits,
        EXPECTED_STAGES,
        "stage_duration.maximum_seconds_by_stage",
    )
    for stage, limit in stage_limits.items():
        _require_number(
            limit,
            f"stage_duration.maximum_seconds_by_stage.{stage}",
            minimum=0.0,
            inclusive_minimum=False,
        )

    volume = _require_mapping(thresholds["volume"], "volume")
    _require_exact_keys(
        volume,
        (
            "minimum_source_rows",
            "minimum_layer_rows",
            "maximum_drop_percent",
        ),
        "volume",
    )
    _require_integer(
        volume["minimum_source_rows"],
        "volume.minimum_source_rows",
        minimum=1,
    )
    layer_limits = _require_mapping(
        volume["minimum_layer_rows"],
        "volume.minimum_layer_rows",
    )
    _require_exact_keys(
        layer_limits,
        EXPECTED_LAYERS,
        "volume.minimum_layer_rows",
    )
    for layer, limit in layer_limits.items():
        _require_integer(
            limit,
            f"volume.minimum_layer_rows.{layer}",
            minimum=1,
        )
    _require_number(
        volume["maximum_drop_percent"],
        "volume.maximum_drop_percent",
        minimum=0.0,
        maximum=100.0,
    )

    quality = _require_mapping(thresholds["quality"], "quality")
    _require_exact_keys(quality, ("maximum_failures",), "quality")
    _require_integer(
        quality["maximum_failures"],
        "quality.maximum_failures",
        minimum=0,
    )

    masking = _require_mapping(thresholds["masking"], "masking")
    _require_exact_keys(masking, ("maximum_failures",), "masking")
    _require_integer(
        masking["maximum_failures"],
        "masking.maximum_failures",
        minimum=0,
    )

    return dict(root)


def load_thresholds(path: Any = DEFAULT_THRESHOLDS_PATH) -> Dict[str, Any]:
    """Load YAML safely and apply the strict threshold schema."""
    try:
        import yaml
    except ImportError as exc:
        raise ThresholdConfigError(
            "PyYAML is required to load the threshold configuration."
        ) from exc

    try:
        content = Path(path).read_text(encoding="utf-8")
    except OSError as exc:
        raise ThresholdConfigError(
            "Threshold configuration could not be read."
        ) from exc

    try:
        document = yaml.safe_load(content)
    except yaml.YAMLError as exc:
        raise ThresholdConfigError(
            "Threshold configuration contains invalid YAML."
        ) from exc

    return validate_threshold_config(document)


def calculate_volume_drop_percent(
    observed_rows: int,
    reference_rows: Optional[int],
) -> Optional[float]:
    """Calculate a same-metric decrease; no positive reference means no rule."""
    if reference_rows is None or reference_rows <= 0:
        return None
    drop = max(0.0, (reference_rows - observed_rows) / reference_rows * 100.0)
    return round(drop, 3)


def _validate_nonnegative_observation_integer(
    value: Any,
    context: str,
    allow_none: bool = False,
) -> Optional[int]:
    if allow_none and value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{context} must be a non-negative integer.")
    return value


def validate_observation(observation: Any) -> Dict[str, Any]:
    """Validate the pure observation contract without importing Spark."""
    root = _require_mapping(observation, "observation")
    unknown = sorted(set(root) - OBSERVATION_KEYS)
    if unknown:
        raise ValueError(
            f"observation contains unknown keys: {', '.join(unknown)}."
        )

    if "source" in root:
        source = _require_mapping(root["source"], "observation.source")
        _require_exact_keys(
            source,
            (
                "name",
                "exists",
                "schema_valid",
                "missing_columns",
                "observed_rows",
                "reference_rows",
            ),
            "observation.source",
        )
        if (
            not isinstance(source["name"], str)
            or SAFE_IDENTIFIER_PATTERN.fullmatch(source["name"]) is None
        ):
            raise ValueError(
                "observation.source.name must be a safe identifier."
            )
        if not isinstance(source["exists"], bool):
            raise ValueError("observation.source.exists must be boolean.")
        if source["exists"] and not isinstance(source["schema_valid"], bool):
            raise ValueError(
                "observation.source.schema_valid must be boolean when present."
            )
        if not source["exists"] and source["schema_valid"] is not None:
            raise ValueError(
                "observation.source.schema_valid must be null when absent."
            )
        if (
            not isinstance(source["missing_columns"], list)
            or any(
                not isinstance(item, str) or not item
                or SAFE_IDENTIFIER_PATTERN.fullmatch(item) is None
                for item in source["missing_columns"]
            )
        ):
            raise ValueError(
                "observation.source.missing_columns must be a list of names."
            )
        if not source["exists"] and source["missing_columns"]:
            raise ValueError(
                "observation.source.missing_columns must be empty when absent."
            )
        if source["schema_valid"] is True and source["missing_columns"]:
            raise ValueError(
                "a schema-valid source cannot declare missing columns."
            )
        if source["schema_valid"] is False and not source["missing_columns"]:
            raise ValueError(
                "a schema-invalid source must declare missing columns."
            )
        _validate_nonnegative_observation_integer(
            source["observed_rows"],
            "observation.source.observed_rows",
        )
        _validate_nonnegative_observation_integer(
            source["reference_rows"],
            "observation.source.reference_rows",
            allow_none=True,
        )

    for key in ("monitoring_events", "quality_failures", "masking_failures"):
        if key in root:
            _validate_nonnegative_observation_integer(
                root[key],
                f"observation.{key}",
            )

    if "stage_durations_seconds" in root:
        durations = _require_mapping(
            root["stage_durations_seconds"],
            "observation.stage_durations_seconds",
        )
        unknown_stages = sorted(set(durations) - set(EXPECTED_STAGES))
        if unknown_stages:
            raise ValueError(
                "observation.stage_durations_seconds contains unknown stages: "
                + ", ".join(unknown_stages)
                + "."
            )
        for stage, value in durations.items():
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or value < 0
            ):
                raise ValueError(
                    f"observation.stage_durations_seconds.{stage} "
                    "must be finite and non-negative."
                )

    for key in ("layer_rows", "reference_layer_rows"):
        if key in root:
            rows = _require_mapping(root[key], f"observation.{key}")
            unknown_layers = sorted(set(rows) - set(EXPECTED_LAYERS))
            if unknown_layers:
                raise ValueError(
                    f"observation.{key} contains unknown layers: "
                    + ", ".join(unknown_layers)
                    + "."
                )
            for layer, value in rows.items():
                _validate_nonnegative_observation_integer(
                    value,
                    f"observation.{key}.{layer}",
                )

    return dict(root)


def _detection(
    rule_id: str,
    stage: str,
    metric: str,
    observed: Any,
    threshold: Any,
    message: str,
) -> Dict[str, Any]:
    return {
        "rule_id": rule_id,
        "stage": stage,
        "metric": metric,
        "observed": observed,
        "threshold": threshold,
        "message": message,
    }


def _select_primary_detection(
    detections: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    ordered = sorted(
        detections,
        key=lambda item: (
            RULE_PRIORITY.get(item["rule_id"], 999),
            item["stage"],
            item["metric"],
        ),
    )
    return ordered[0] if ordered else None


def evaluate_observation(
    observation: Any,
    thresholds: Any,
    expected_stage: Optional[str] = None,
    expected_rule: Optional[str] = None,
) -> Dict[str, Any]:
    """Evaluate only present metrics without biasing toward an expectation."""
    if (expected_stage is None) != (expected_rule is None):
        raise ValueError(
            "expected_stage and expected_rule must be provided together."
        )
    validated_observation = validate_observation(observation)
    validated_thresholds = validate_threshold_config(thresholds)
    configured = validated_thresholds["thresholds"]
    detections: List[Dict[str, Any]] = []

    source = validated_observation.get("source")
    if source is not None:
        source_name = source["name"]
        if not source["exists"]:
            detections.append(
                _detection(
                    "source.file.required",
                    "bronze",
                    f"source.{source_name}.exists",
                    False,
                    True,
                    f"Required source '{source_name}' was not found.",
                )
            )
        elif not source["schema_valid"]:
            missing = sorted(source["missing_columns"])
            detections.append(
                _detection(
                    "source.schema.required_columns",
                    "bronze",
                    f"source.{source_name}.required_columns",
                    missing,
                    [],
                    f"Source '{source_name}' is missing required columns: "
                    + ", ".join(missing)
                    + ".",
                )
            )

        minimum_source_rows = configured["volume"]["minimum_source_rows"]
        observed_rows = source["observed_rows"]
        if observed_rows < minimum_source_rows:
            detections.append(
                _detection(
                    "volume.minimum_rows",
                    "bronze",
                    f"source.{source_name}.rows",
                    observed_rows,
                    minimum_source_rows,
                    f"Source '{source_name}' has {observed_rows} rows; "
                    f"minimum is {minimum_source_rows}.",
                )
            )

        drop_percent = calculate_volume_drop_percent(
            observed_rows,
            source["reference_rows"],
        )
        maximum_drop = float(configured["volume"]["maximum_drop_percent"])
        if drop_percent is not None and drop_percent > maximum_drop:
            detections.append(
                _detection(
                    "volume.maximum_drop_percent",
                    "bronze",
                    f"source.{source_name}.drop_percent",
                    drop_percent,
                    maximum_drop,
                    f"Source '{source_name}' volume dropped by "
                    f"{drop_percent:.3f}%; maximum allowed is "
                    f"{maximum_drop:.3f}%.",
                )
            )

    durations = validated_observation.get("stage_durations_seconds", {})
    duration_limits = configured["stage_duration"]["maximum_seconds_by_stage"]
    for stage, duration in durations.items():
        maximum_duration = float(duration_limits[stage])
        if float(duration) > maximum_duration:
            detections.append(
                _detection(
                    "stage.maximum_duration_seconds",
                    stage,
                    f"stage.{stage}.duration_seconds",
                    float(duration),
                    maximum_duration,
                    f"Stage '{stage}' duration {float(duration):.3f}s "
                    f"exceeds maximum {maximum_duration:.3f}s.",
                )
            )

    layer_rows = validated_observation.get("layer_rows", {})
    reference_layer_rows = validated_observation.get(
        "reference_layer_rows",
        {},
    )
    layer_limits = configured["volume"]["minimum_layer_rows"]
    maximum_drop = float(configured["volume"]["maximum_drop_percent"])
    for layer, rows in layer_rows.items():
        minimum_rows = layer_limits[layer]
        if rows < minimum_rows:
            detections.append(
                _detection(
                    "volume.minimum_rows",
                    LAYER_STAGES[layer],
                    f"layer.{layer}.rows",
                    rows,
                    minimum_rows,
                    f"Layer '{layer}' has {rows} rows; "
                    f"minimum is {minimum_rows}.",
                )
            )
        drop_percent = calculate_volume_drop_percent(
            rows,
            reference_layer_rows.get(layer),
        )
        if drop_percent is not None and drop_percent > maximum_drop:
            detections.append(
                _detection(
                    "volume.maximum_drop_percent",
                    LAYER_STAGES[layer],
                    f"layer.{layer}.drop_percent",
                    drop_percent,
                    maximum_drop,
                    f"Layer '{layer}' volume dropped by {drop_percent:.3f}%; "
                    f"maximum allowed is {maximum_drop:.3f}%.",
                )
            )

    if "monitoring_events" in validated_observation:
        observed_events = validated_observation["monitoring_events"]
        minimum_events = configured["monitoring"]["minimum_events"]
        if observed_events < minimum_events:
            detections.append(
                _detection(
                    "monitoring.minimum_events",
                    "observability",
                    "monitoring.events",
                    observed_events,
                    minimum_events,
                    f"Monitoring emitted {observed_events} events; "
                    f"minimum is {minimum_events}.",
                )
            )

    if "quality_failures" in validated_observation:
        failures = validated_observation["quality_failures"]
        maximum_failures = configured["quality"]["maximum_failures"]
        if failures > maximum_failures:
            detections.append(
                _detection(
                    "quality.maximum_failures",
                    "data_quality",
                    "quality.failures",
                    failures,
                    maximum_failures,
                    f"Data quality reported {failures} failures; "
                    f"maximum is {maximum_failures}.",
                )
            )

    if "masking_failures" in validated_observation:
        failures = validated_observation["masking_failures"]
        maximum_failures = configured["masking"]["maximum_failures"]
        if failures > maximum_failures:
            detections.append(
                _detection(
                    "masking.maximum_failures",
                    "masking",
                    "masking.failures",
                    failures,
                    maximum_failures,
                    f"Masking reported {failures} failures; "
                    f"maximum is {maximum_failures}.",
                )
            )

    ordered = sorted(
        detections,
        key=lambda item: (
            RULE_PRIORITY.get(item["rule_id"], 999),
            item["stage"],
            item["metric"],
        ),
    )
    primary = _select_primary_detection(ordered)
    triggered_rules = list(dict.fromkeys(item["rule_id"] for item in ordered))

    return {
        "detection_status": "DETECTED" if primary else "NOT_DETECTED",
        "pipeline_status": "FAILURE" if primary else "SUCCESS",
        "failed_stage": None if primary is None else primary["stage"],
        "rule_triggered": None if primary is None else primary["rule_id"],
        "error_message": None if primary is None else primary["message"],
        "triggered_rules": triggered_rules,
        "detections": ordered,
    }


def _parse_payload_timestamp(value: Any, field_name: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be a non-empty ISO timestamp.")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO timestamp.") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must include a timezone.")
    return parsed


def _find_forbidden_payload_key(value: Any) -> Optional[str]:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if str(key).lower() in FORBIDDEN_PAYLOAD_KEYS:
                return str(key)
            nested = _find_forbidden_payload_key(item)
            if nested is not None:
                return nested
    elif isinstance(value, list):
        for item in value:
            nested = _find_forbidden_payload_key(item)
            if nested is not None:
                return nested
    return None


def _validate_detection_contracts(value: Any) -> List[Dict[str, Any]]:
    if not isinstance(value, list):
        raise ValueError("payload detections must be a list.")

    allowed_stages_by_rule = {
        "source.file.required": {"bronze"},
        "source.schema.required_columns": {"bronze"},
        "volume.minimum_rows": set(LAYER_STAGES.values()),
        "volume.maximum_drop_percent": set(LAYER_STAGES.values()),
        "stage.maximum_duration_seconds": set(EXPECTED_STAGES),
        "monitoring.minimum_events": {"observability"},
        "quality.maximum_failures": {"data_quality"},
        "masking.maximum_failures": {"masking"},
    }
    validated = []
    for index, item in enumerate(value):
        detection = _require_mapping(
            item,
            f"payload.detections[{index}]",
        )
        _require_exact_keys(
            detection,
            ("rule_id", "stage", "metric", "observed", "threshold", "message"),
            f"payload.detections[{index}]",
        )
        rule_id = detection["rule_id"]
        stage = detection["stage"]
        if rule_id not in RULE_PRIORITY:
            raise ValueError(
                f"payload.detections[{index}].rule_id is invalid."
            )
        if stage not in allowed_stages_by_rule[rule_id]:
            raise ValueError(
                f"payload.detections[{index}].stage is invalid for its rule."
            )
        if (
            not isinstance(detection["metric"], str)
            or SAFE_IDENTIFIER_PATTERN.fullmatch(detection["metric"]) is None
        ):
            raise ValueError(
                f"payload.detections[{index}].metric must be a safe identifier."
            )
        if (
            not isinstance(detection["message"], str)
            or not detection["message"]
            or len(detection["message"]) > 512
        ):
            raise ValueError(
                f"payload.detections[{index}].message is invalid."
            )
        validated.append(dict(detection))
    return validated


def validate_payload_contract(
    payload: Any,
    forbidden_values: Iterable[str] = (),
) -> Dict[str, Any]:
    """Validate required evidence fields and reject local/sensitive material."""
    root = _require_mapping(payload, "payload")
    missing = sorted(REQUIRED_PAYLOAD_FIELDS - set(root))
    if missing:
        raise ValueError(
            "payload is missing required fields: " + ", ".join(missing) + "."
        )
    unknown = sorted(set(root) - ALLOWED_PAYLOAD_FIELDS)
    if unknown:
        raise ValueError(
            "payload contains unknown fields: " + ", ".join(unknown) + "."
        )

    if root["payload_version"] != 1:
        raise ValueError("payload_version must be 1.")
    if root["threshold_config_version"] != 1:
        raise ValueError("threshold_config_version must be 1.")
    if root["scenario"] not in SCENARIO_EXPECTATIONS:
        raise ValueError("payload scenario is not supported.")
    if root["runtime_profile"] != "local-small":
        raise ValueError("payload runtime_profile must be local-small.")
    if root["detection_status"] not in {"DETECTED", "NOT_DETECTED", "ERROR"}:
        raise ValueError("payload detection_status is invalid.")
    if root["pipeline_status"] not in {"SUCCESS", "FAILURE"}:
        raise ValueError("payload pipeline_status is invalid.")
    if (
        not isinstance(root["batch_id"], str)
        or re.fullmatch(r"[A-Za-z0-9._-]{1,128}", root["batch_id"]) is None
    ):
        raise ValueError("payload batch_id must be a safe identifier.")
    if (
        not isinstance(root["error_message"], str)
        or not root["error_message"]
        or len(root["error_message"]) > 512
    ):
        raise ValueError("payload error_message is invalid.")
    if root["process_exit_code"] not in {
        DETECTED_FAILURE_EXIT_CODE,
        HARNESS_FAILURE_EXIT_CODE,
    }:
        raise ValueError("payload process_exit_code must be 1 or 2.")
    if not isinstance(root["triggered_rules"], list) or any(
        not isinstance(item, str) or item not in RULE_PRIORITY
        for item in root["triggered_rules"]
    ):
        raise ValueError("payload triggered_rules must be a list of rule ids.")
    if len(root["triggered_rules"]) != len(set(root["triggered_rules"])):
        raise ValueError("payload triggered_rules must not contain duplicates.")
    if root["observed_stage_status"] not in ALLOWED_OBSERVED_STAGE_STATUSES:
        raise ValueError("payload observed_stage_status is invalid.")

    validate_observation(root["observation"])
    detections = _validate_detection_contracts(root["detections"])
    detected_rule_order = list(
        dict.fromkeys(item["rule_id"] for item in detections)
    )
    if root["triggered_rules"] != detected_rule_order:
        raise ValueError(
            "payload triggered_rules must match the ordered detections."
        )

    started_at = _parse_payload_timestamp(root["started_at"], "started_at")
    finished_at = _parse_payload_timestamp(root["finished_at"], "finished_at")
    if finished_at < started_at:
        raise ValueError("finished_at must not precede started_at.")
    if (
        isinstance(root["duration_seconds"], bool)
        or not isinstance(root["duration_seconds"], (int, float))
        or not math.isfinite(float(root["duration_seconds"]))
        or root["duration_seconds"] < 0
    ):
        raise ValueError("duration_seconds must be finite and non-negative.")
    expected_duration = round((finished_at - started_at).total_seconds(), 3)
    if abs(float(root["duration_seconds"]) - expected_duration) > 0.001:
        raise ValueError("duration_seconds must match the payload timestamps.")

    if root["detection_status"] == "DETECTED":
        if not isinstance(root["failed_stage"], str) or not root["failed_stage"]:
            raise ValueError("detected payload requires failed_stage.")
        if (
            not isinstance(root["rule_triggered"], str)
            or not root["rule_triggered"]
        ):
            raise ValueError("detected payload requires rule_triggered.")
        if root["pipeline_status"] != "FAILURE" or not detections:
            raise ValueError(
                "detected payload requires a failed pipeline and detections."
            )
        if not any(
            item["rule_id"] == root["rule_triggered"]
            and item["stage"] == root["failed_stage"]
            for item in detections
        ):
            raise ValueError(
                "detected payload primary rule must match a detection."
            )
    elif root["detection_status"] == "NOT_DETECTED":
        if (
            root["pipeline_status"] != "SUCCESS"
            or root["failed_stage"] is not None
            or root["rule_triggered"] is not None
            or detections
            or root["triggered_rules"]
        ):
            raise ValueError("not-detected payload fields are inconsistent.")
    else:
        if (
            root["pipeline_status"] != "FAILURE"
            or root["failed_stage"] != "failure_smoke"
            or root["rule_triggered"] != "failure_smoke.internal_error"
            or root["process_exit_code"] != HARNESS_FAILURE_EXIT_CODE
            or root["observed_stage_status"] != "NOT_EXECUTED"
            or root["observation"]
            or detections
            or root["triggered_rules"]
        ):
            raise ValueError("error payload fields are inconsistent.")

    forbidden_key = _find_forbidden_payload_key(root)
    if forbidden_key is not None:
        raise ValueError(f"payload contains forbidden key: {forbidden_key}.")

    rendered = json.dumps(root, ensure_ascii=False, sort_keys=True)
    rendered_lower = rendered.lower()
    forbidden_patterns = (
        "file://",
        "\\users\\",
        "/users/",
        "/tmp/",
        "/home/",
        "/repo/",
    )
    if any(pattern in rendered_lower for pattern in forbidden_patterns):
        raise ValueError("payload contains a forbidden local path.")
    if re.search(r"(?i)\b[a-z]:[\\/]", rendered):
        raise ValueError("payload contains a forbidden absolute path.")
    for forbidden_value in forbidden_values:
        if forbidden_value and forbidden_value in rendered:
            raise ValueError("payload contains a forbidden value.")
    if root["process_exit_code"] != exit_code_for_payload(root):
        raise ValueError(
            "payload process_exit_code does not match the scenario contract."
        )

    return dict(root)


def scenario_contract_matches(payload: Mapping[str, Any]) -> bool:
    expectation = SCENARIO_EXPECTATIONS.get(payload.get("scenario"))
    if expectation is None:
        return False
    return (
        payload.get("detection_status") == "DETECTED"
        and payload.get("pipeline_status") == "FAILURE"
        and payload.get("failed_stage") == expectation["failed_stage"]
        and payload.get("rule_triggered") == expectation["rule_triggered"]
        and payload.get("observed_stage_status") == "FAILURE"
        and bool(payload.get("error_message"))
    )


def exit_code_for_payload(payload: Mapping[str, Any]) -> int:
    """Failure smoke never returns zero."""
    if scenario_contract_matches(payload):
        return DETECTED_FAILURE_EXIT_CODE
    return HARNESS_FAILURE_EXIT_CODE


def build_failure_payload(
    scenario: str,
    runtime_profile: str,
    batch_id: str,
    started_at: datetime,
    finished_at: datetime,
    evaluation: Mapping[str, Any],
    observation: Mapping[str, Any],
    observed_stage_status: str,
    threshold_config_version: int = 1,
) -> Dict[str, Any]:
    duration = round((finished_at - started_at).total_seconds(), 3)
    payload = {
        "payload_version": 1,
        "threshold_config_version": threshold_config_version,
        "scenario": scenario,
        "runtime_profile": runtime_profile,
        "detection_status": evaluation["detection_status"],
        "pipeline_status": evaluation["pipeline_status"],
        "failed_stage": evaluation["failed_stage"],
        "rule_triggered": evaluation["rule_triggered"],
        "triggered_rules": evaluation["triggered_rules"],
        "batch_id": batch_id,
        "error_message": evaluation["error_message"]
        or "Injected failure was not detected.",
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": duration,
        "process_exit_code": HARNESS_FAILURE_EXIT_CODE,
        "observed_stage_status": observed_stage_status,
        "observation": dict(observation),
        "detections": evaluation["detections"],
    }
    payload["process_exit_code"] = exit_code_for_payload(payload)
    return payload


def _build_error_payload(
    scenario: str,
    runtime_profile: str,
    batch_id: str,
    started_at: datetime,
    finished_at: datetime,
    error_message: str,
) -> Dict[str, Any]:
    return {
        "payload_version": 1,
        "threshold_config_version": 1,
        "scenario": scenario,
        "runtime_profile": runtime_profile,
        "detection_status": "ERROR",
        "pipeline_status": "FAILURE",
        "failed_stage": "failure_smoke",
        "rule_triggered": "failure_smoke.internal_error",
        "triggered_rules": [],
        "batch_id": batch_id,
        "error_message": error_message,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": round(
            (finished_at - started_at).total_seconds(),
            3,
        ),
        "process_exit_code": HARNESS_FAILURE_EXIT_CODE,
        "observed_stage_status": "NOT_EXECUTED",
        "observation": {},
        "detections": [],
    }


def _safe_harness_error(error: Exception) -> str:
    if isinstance(error, ThresholdConfigError):
        return "Threshold configuration validation failed."
    if isinstance(error, FailureSmokeError):
        return str(error)
    return (
        "Failure smoke harness failed with "
        f"{error.__class__.__name__}."
    )


def _validate_batch_id(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,128}", value):
        raise FailureSmokeError(
            "Batch id must use only letters, digits, dot, underscore, or dash."
        )
    return value


def _as_file_uri(path: Path) -> str:
    return path.resolve().as_uri()


def _rewrite_csv_without_column(path: Path, column: str) -> None:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)
    if column not in fieldnames:
        raise FailureSmokeError(
            "Controlled schema mutation could not find its target column."
        )
    remaining = [name for name in fieldnames if name != column]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=remaining,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)


def _rewrite_csv_as_header_only(path: Path) -> None:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
    if not fieldnames:
        raise FailureSmokeError(
            "Controlled zero-volume mutation requires a CSV header."
        )
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()


def _read_csv_header(path: Path) -> List[str]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        fieldnames = list(csv.DictReader(handle).fieldnames or [])
    if not fieldnames:
        raise FailureSmokeError(
            "Controlled source does not contain a readable CSV header."
        )
    return fieldnames


def _find_source_result(
    preflight: Mapping[str, Any],
    source_name: str,
) -> Optional[Mapping[str, Any]]:
    return next(
        (
            item
            for item in preflight.get("sources", [])
            if item.get("source_name") == source_name
        ),
        None,
    )


def build_source_observation(
    preflight: Mapping[str, Any],
    source_name: str,
    source_exists: bool,
    reference_rows: int,
    required_columns: Iterable[str],
    observed_columns: Iterable[str],
) -> Dict[str, Any]:
    """Extract a safe source observation without retaining filesystem paths."""
    result = _find_source_result(preflight, source_name)
    missing_columns = (
        sorted(set(required_columns) - set(observed_columns))
        if source_exists
        else []
    )

    return {
        "name": source_name,
        "exists": source_exists,
        "schema_valid": (
            None
            if not source_exists
            else not missing_columns
        ),
        "missing_columns": missing_columns,
        "observed_rows": (
            0 if result is None else int(result.get("record_count", 0))
        ),
        "reference_rows": reference_rows,
    }


def resolve_bronze_stage_status(
    stage_result: Mapping[str, Any],
    source_observation: Mapping[str, Any],
    thresholds: Mapping[str, Any],
) -> str:
    """Combine the Bronze operation with its fail-closed source threshold gate."""
    operational_status = stage_result.get("status", "UNKNOWN")
    if operational_status not in {"SUCCESS", "FAILURE"}:
        operational_status = "UNKNOWN"

    source_evaluation = evaluate_observation(
        {"source": dict(source_observation)},
        thresholds,
    )
    source_gate_failed = any(
        detection["stage"] == "bronze"
        for detection in source_evaluation["detections"]
    )
    if source_gate_failed:
        return "FAILURE"
    return operational_status


def _inject_scenario(
    scenario: str,
    source_path: Path,
) -> None:
    if scenario == "invalid-schema":
        _rewrite_csv_without_column(source_path, "cliente_id")
    elif scenario == "missing-source":
        source_path.unlink()
    elif scenario == "zero-volume":
        _rewrite_csv_as_header_only(source_path)
    else:
        raise FailureSmokeError("Unsupported controlled scenario.")


def _run_scenario_in_directory(
    args: argparse.Namespace,
    work_dir: Path,
    thresholds: Mapping[str, Any],
) -> Dict[str, Any]:
    sample_data_path = work_dir / "sample"
    bronze_path = _as_file_uri(work_dir / "bronze")
    monitoring_path = _as_file_uri(work_dir / "monitoring")

    os.environ["RUNTIME_PROFILE"] = args.runtime_profile
    os.environ["DM_RUNTIME_PROFILE"] = args.runtime_profile
    os.environ["SPARK_LOG_LEVEL"] = args.log_level
    os.environ["BRONZE_PATH"] = bronze_path
    os.environ["MONITORING_PATH"] = monitoring_path
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_IVY_DIR", "/tmp/.ivy2")

    for relative_path in (
        "jobs/data_generation",
        "jobs/bronze",
        "jobs/common",
    ):
        path = str(REPO_ROOT / relative_path)
        if path not in sys.path:
            sys.path.insert(0, path)

    from generate_banking_sample_data import generate_all_sample_data
    from source_registry import get_source_contract
    from validate_source_contracts import validate_sample_files

    generate_all_sample_data(
        str(sample_data_path),
        runtime_profile=args.runtime_profile,
    )
    baseline = validate_sample_files(str(sample_data_path))
    if not baseline.get("passed"):
        raise FailureSmokeError(
            "Generated synthetic baseline failed source preflight."
        )

    source_name = "clientes"
    source_contract = get_source_contract(source_name)
    if source_contract["format"] != "csv":
        raise FailureSmokeError(
            "Controlled source must use the expected CSV contract."
        )
    source_path = sample_data_path / source_contract["file_name"]
    baseline_result = _find_source_result(baseline, source_name)
    if baseline_result is None:
        raise FailureSmokeError(
            "Generated synthetic baseline did not report the target source."
        )
    reference_rows = int(baseline_result["record_count"])
    if reference_rows <= 0:
        raise FailureSmokeError(
            "Generated synthetic baseline has no reference rows."
        )

    _inject_scenario(args.scenario, source_path)
    preflight = validate_sample_files(str(sample_data_path))
    observed_columns = (
        _read_csv_header(source_path) if source_path.exists() else []
    )
    source_observation = build_source_observation(
        preflight,
        source_name,
        source_path.exists(),
        reference_rows,
        source_contract["required_columns"],
        observed_columns,
    )

    from load_bronze import run_bronze_pipeline
    from spark_session import SparkSessionFactory, create_spark_session

    spark = None
    stage_started_at = datetime.now(timezone.utc)
    try:
        spark = create_spark_session()
        stage_result = run_bronze_pipeline(
            spark,
            str(sample_data_path),
            bronze_path,
            args.batch_id,
        )
    finally:
        stage_finished_at = datetime.now(timezone.utc)
        if spark is not None:
            SparkSessionFactory.stop()

    return {
        "observation": {
            "source": source_observation,
            "stage_durations_seconds": {
                "bronze": round(
                    (stage_finished_at - stage_started_at).total_seconds(),
                    3,
                )
            },
        },
        "observed_stage_status": resolve_bronze_stage_status(
            stage_result,
            source_observation,
            thresholds,
        ),
    }


def execute_runtime_scenario(
    args: argparse.Namespace,
    thresholds: Mapping[str, Any],
) -> Dict[str, Any]:
    """Run one controlled scenario in an explicit or disposable workdir."""
    allowed_profiles = thresholds["runtime_profiles"]
    if args.runtime_profile not in allowed_profiles:
        raise FailureSmokeError(
            "Runtime profile is not allowed by the threshold configuration."
        )

    if args.work_dir:
        work_dir = Path(args.work_dir).resolve()
        work_dir.mkdir(parents=True, exist_ok=True)
        if any(work_dir.iterdir()):
            raise FailureSmokeError(
                "Explicit work directory must be empty to avoid stale state."
            )
        return _run_scenario_in_directory(args, work_dir, thresholds)

    with tempfile.TemporaryDirectory(
        prefix="dm-observability-failure-"
    ) as temporary_directory:
        return _run_scenario_in_directory(
            args,
            Path(temporary_directory),
            thresholds,
        )


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    started_at = datetime.now(timezone.utc)
    generated_batch_id = (
        "observability_failure_"
        + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    )
    batch_id = generated_batch_id

    try:
        if args.batch_id is not None:
            batch_id = _validate_batch_id(args.batch_id)
        args.batch_id = batch_id
        thresholds = load_thresholds(args.thresholds_path)
        runtime_result = execute_runtime_scenario(args, thresholds)
        expectation = SCENARIO_EXPECTATIONS[args.scenario]
        evaluation = evaluate_observation(
            runtime_result["observation"],
            thresholds,
            expected_stage=expectation["failed_stage"],
            expected_rule=expectation["rule_triggered"],
        )
        finished_at = datetime.now(timezone.utc)
        payload = build_failure_payload(
            scenario=args.scenario,
            runtime_profile=args.runtime_profile,
            batch_id=batch_id,
            started_at=started_at,
            finished_at=finished_at,
            evaluation=evaluation,
            observation=runtime_result["observation"],
            observed_stage_status=runtime_result["observed_stage_status"],
            threshold_config_version=thresholds["version"],
        )
        validate_payload_contract(payload)
        return_code = payload["process_exit_code"]
    except Exception as error:
        finished_at = datetime.now(timezone.utc)
        payload = _build_error_payload(
            scenario=args.scenario,
            runtime_profile=args.runtime_profile,
            batch_id=batch_id,
            started_at=started_at,
            finished_at=finished_at,
            error_message=_safe_harness_error(error),
        )
        validate_payload_contract(payload)
        return_code = HARNESS_FAILURE_EXIT_CODE

    print(
        RESULT_MARKER
        + json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
        ),
        flush=True,
    )
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
