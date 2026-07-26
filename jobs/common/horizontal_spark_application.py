"""Single adapter from a horizontal runtime profile to SparkApplication."""

import copy
import re
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Set

import yaml

from runtime_profiles import get_runtime_profile


ALLOWED_PROFILE_DIFFERENCES = {
    "id",
    "spark.executor_instances",
}
HORIZONTAL_PROFILE_PREFIX = "minikube-horizontal-"
SPARK_APPLICATION_TEMPLATE = (
    Path(__file__).resolve().parents[2]
    / "infra"
    / "workloads"
    / "spark-apps"
    / "templates"
    / "spark-horizontal-benchmark.yaml"
)
SUPPORTED_TOPOLOGIES = {
    "single-node-application-scale-out",
    "multi-node-scale-out",
}
_KUBERNETES_ID = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_DIGEST_IMAGE = re.compile(
    r"^[a-z0-9][a-z0-9./_-]*@sha256:[0-9a-f]{64}$"
)


def _flatten(value: Any, prefix: str = "") -> Dict[str, Any]:
    flattened: Dict[str, Any] = {}
    if isinstance(value, Mapping):
        for key in sorted(value):
            child = f"{prefix}.{key}" if prefix else str(key)
            flattened.update(_flatten(value[key], child))
        return flattened
    flattened[prefix] = value
    return flattened


def horizontal_profile_differences(
    baseline: Mapping[str, Any],
    scale_out: Mapping[str, Any],
) -> Set[str]:
    """Return every leaf path whose value or presence differs."""
    left = _flatten(baseline)
    right = _flatten(scale_out)
    return {
        path
        for path in set(left) | set(right)
        if left.get(path, object()) != right.get(path, object())
    }


def validate_horizontal_profile(profile: Mapping[str, Any]) -> None:
    profile_id = str(profile.get("id", ""))
    if not profile_id.startswith(HORIZONTAL_PROFILE_PREFIX):
        raise ValueError("Runtime profile is not a horizontal benchmark profile.")
    spark = profile["spark"]
    if str(spark["master"]).startswith("local"):
        raise ValueError("Horizontal Spark benchmark rejects local masters.")
    if spark.get("dynamic_allocation") is not False:
        raise ValueError("Horizontal Spark benchmark requires static executors.")
    if spark["executor_instances"] not in (1, 3):
        raise ValueError("Horizontal Spark benchmark requires 1 or 3 executors.")
    if spark["executor_memory"] != "1g":
        raise ValueError("Horizontal Spark benchmark requires 1g per executor.")
    if profile["kubernetes"]["executor_cores"] != 1:
        raise ValueError("Horizontal Spark benchmark requires one core per executor.")


def validate_horizontal_profile_pair(
    baseline_id: str,
    scale_out_id: str,
) -> None:
    baseline = get_runtime_profile(baseline_id)
    scale_out = get_runtime_profile(scale_out_id)
    validate_horizontal_profile(baseline)
    validate_horizontal_profile(scale_out)
    differences = horizontal_profile_differences(baseline, scale_out)
    if differences != ALLOWED_PROFILE_DIFFERENCES:
        raise ValueError(
            "Horizontal profiles violate the controlled-difference contract: "
            + ",".join(sorted(differences))
        )
    if baseline["spark"]["executor_instances"] != 1:
        raise ValueError("Horizontal baseline must request one executor.")
    if scale_out["spark"]["executor_instances"] != 3:
        raise ValueError("Horizontal scale-out must request three executors.")


def build_horizontal_storage_paths(
    *,
    profile: Mapping[str, Any],
    benchmark_id: str,
    run_id: str,
) -> Dict[str, str]:
    """Resolve isolated shared-storage paths for one application run."""
    _validate_kubernetes_id("benchmark_id", benchmark_id)
    _validate_kubernetes_id("run_id", run_id)
    root = str(profile["kubernetes"]["storage_root"]).rstrip("/")
    if not root.startswith("s3a://"):
        raise ValueError("Horizontal shared storage must use s3a://.")
    prefix = f"{root}/{benchmark_id}/{profile['id']}/{run_id}"
    return {
        "input": f"{prefix}/input",
        "bronze": f"{prefix}/bronze",
        "raw_vault": f"{prefix}/raw-vault",
        "business_vault": f"{prefix}/business-vault",
        "gold": f"{prefix}/gold",
        "monitoring": f"{prefix}/monitoring",
        "checkpoints": f"{prefix}/checkpoints",
        "event_logs": f"{prefix}/event-logs",
    }


def _validate_kubernetes_id(name: str, value: str) -> None:
    if not _KUBERNETES_ID.fullmatch(value):
        raise ValueError(f"{name} must be a Kubernetes-safe identifier.")


def _secret_env(secret_name: str, env_name: str, secret_key: str) -> Dict[str, Any]:
    return {
        "name": env_name,
        "valueFrom": {
            "secretKeyRef": {
                "name": secret_name,
                "key": secret_key,
            }
        },
    }


def _runtime_env(
    *,
    profile: Mapping[str, Any],
    paths: Mapping[str, str],
    benchmark_id: str,
    run_id: str,
    batch_id: str,
    git_sha: str,
    image_digest: str,
    topology: str,
) -> list:
    kubernetes = profile["kubernetes"]
    dataset = profile["dataset"]
    values = {
        "RUNTIME_PROFILE": profile["id"],
        "DM_RUNTIME_PROFILE": profile["id"],
        "SAMPLE_DATA_PATH": paths["input"],
        "BRONZE_PATH": paths["bronze"],
        "RAW_VAULT_PATH": paths["raw_vault"],
        "BUSINESS_VAULT_PATH": paths["business_vault"],
        "GOLD_PATH": paths["gold"],
        "MONITORING_PATH": paths["monitoring"],
        "CHECKPOINT_PATH": paths["checkpoints"],
        "SPARK_EVENT_LOG_DIR": paths["event_logs"],
        "MINIO_ENDPOINT": kubernetes["minio_endpoint"],
        "SPARK_S3_USE_ENV_CREDENTIALS": "true",
        "SPARK_JARS_PACKAGES": "",
        "SPARK_MASTER": profile["spark"]["master"],
        "SPARK_EXECUTOR_INSTANCES": str(
            profile["spark"]["executor_instances"]
        ),
        "SPARK_EXECUTOR_MEMORY": profile["spark"]["executor_memory"],
        "SPARK_SQL_SHUFFLE_PARTITIONS": str(
            profile["spark"]["shuffle_partitions"]
        ),
        "SPARK_DELTA_SNAPSHOT_PARTITIONS": str(
            profile["spark"]["shuffle_partitions"]
        ),
        "SPARK_ADAPTIVE_ENABLED": str(
            profile["spark"]["adaptive_enabled"]
        ).lower(),
        "HORIZONTAL_BENCHMARK_ID": benchmark_id,
        "HORIZONTAL_RUN_ID": run_id,
        "HORIZONTAL_BATCH_ID": batch_id,
        "HORIZONTAL_GIT_SHA": git_sha,
        "HORIZONTAL_IMAGE_DIGEST": image_digest,
        "HORIZONTAL_TOPOLOGY": topology,
        "HORIZONTAL_DATASET_SEED": str(dataset["seed"]),
        "HORIZONTAL_DATASET_REFERENCE_TIME": dataset["reference_time"],
        "HORIZONTAL_PIPELINE_CONTRACT_VERSION": dataset[
            "pipeline_contract_version"
        ],
    }
    env = [{"name": key, "value": str(value)} for key, value in values.items()]
    secret = kubernetes["credentials_secret"]
    env.extend(
        [
            _secret_env(secret, "MINIO_ACCESS_KEY", "MINIO_ACCESS_KEY"),
            _secret_env(secret, "MINIO_SECRET_KEY", "MINIO_SECRET_KEY"),
            _secret_env(secret, "AWS_ACCESS_KEY_ID", "MINIO_ACCESS_KEY"),
            _secret_env(secret, "AWS_SECRET_ACCESS_KEY", "MINIO_SECRET_KEY"),
        ]
    )
    return env


def _load_template(template_path: Optional[Path] = None) -> Dict[str, Any]:
    path = template_path or SPARK_APPLICATION_TEMPLATE
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ValueError("SparkApplication template must contain one mapping.")
    return document


def build_horizontal_spark_application(
    *,
    profile_id: str,
    benchmark_id: str,
    run_id: str,
    batch_id: str,
    git_sha: str,
    image: str,
    image_digest: str,
    topology: str,
    measurement_kind: str,
    repetition: int,
    template_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Translate the generic runtime contract into one cluster-mode CRD."""
    profile = get_runtime_profile(profile_id)
    validate_horizontal_profile(profile)
    for name, value in (
        ("benchmark_id", benchmark_id),
        ("run_id", run_id),
        ("batch_id", batch_id),
    ):
        _validate_kubernetes_id(name, value)
    if not _GIT_SHA.fullmatch(git_sha):
        raise ValueError("git_sha must be an exact lowercase 40-character SHA.")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", image_digest):
        raise ValueError("image_digest must be an exact sha256 digest.")
    if not _DIGEST_IMAGE.fullmatch(image):
        raise ValueError("Spark image must be pinned by repository digest.")
    if image.rsplit("@", 1)[1] != image_digest:
        raise ValueError("Spark image reference and image_digest disagree.")
    if topology not in SUPPORTED_TOPOLOGIES:
        raise ValueError("Unsupported horizontal topology.")
    if measurement_kind not in {"warmup", "measurement"}:
        raise ValueError("measurement_kind must be warmup or measurement.")
    if repetition < 0 or repetition > 3:
        raise ValueError("repetition must be between zero and three.")

    paths = build_horizontal_storage_paths(
        profile=profile,
        benchmark_id=benchmark_id,
        run_id=run_id,
    )
    application = _load_template(template_path)
    name = f"dm-h-{profile['spark']['executor_instances']}-{run_id}"
    labels = {
        "app.kubernetes.io/part-of": "data-master-platform",
        "data-master.io/benchmark-id": benchmark_id,
        "data-master.io/run-id": run_id,
        "data-master.io/runtime-profile": profile_id,
        "data-master.io/topology": topology,
    }
    spec = application["spec"]
    application["metadata"] = {
        "name": name,
        "namespace": profile["kubernetes"]["namespace"],
        "labels": copy.deepcopy(labels),
    }
    spec["image"] = image
    spec["imagePullPolicy"] = profile["kubernetes"]["image_pull_policy"]
    spec["arguments"] = [
        "--workload",
        "--profile",
        profile_id,
        "--benchmark-id",
        benchmark_id,
        "--run-id",
        run_id,
        "--batch-id",
        batch_id,
        "--git-sha",
        git_sha,
        "--image-digest",
        image_digest,
        "--topology",
        topology,
        "--measurement-kind",
        measurement_kind,
        "--repetition",
        str(repetition),
    ]
    spark = profile["spark"]
    kubernetes = profile["kubernetes"]
    spec["sparkConf"].update(
        {
            "spark.sql.adaptive.enabled": str(
                spark["adaptive_enabled"]
            ).lower(),
            "spark.sql.shuffle.partitions": str(
                spark["shuffle_partitions"]
            ),
            "spark.databricks.delta.snapshotPartitions": str(
                spark["shuffle_partitions"]
            ),
            "spark.hadoop.fs.s3a.endpoint": kubernetes["minio_endpoint"],
        }
    )
    env = _runtime_env(
        profile=profile,
        paths=paths,
        benchmark_id=benchmark_id,
        run_id=run_id,
        batch_id=batch_id,
        git_sha=git_sha,
        image_digest=image_digest,
        topology=topology,
    )
    pod_security = {
        "runAsNonRoot": True,
        "runAsUser": 185,
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
    }
    spec["driver"].update(
        {
            "cores": kubernetes["driver_cores"],
            "coreLimit": f"{kubernetes['driver_cores'] * 1000}m",
            "memory": spark["driver_memory"],
            "serviceAccount": kubernetes["service_account"],
            "labels": {**labels, "data-master.io/spark-role": "driver"},
            "env": copy.deepcopy(env),
            "securityContext": copy.deepcopy(pod_security),
        }
    )
    spec["executor"].update(
        {
            "cores": kubernetes["executor_cores"],
            "instances": spark["executor_instances"],
            "memory": spark["executor_memory"],
            "labels": {**labels, "data-master.io/spark-role": "executor"},
            "env": copy.deepcopy(env),
            "securityContext": copy.deepcopy(pod_security),
        }
    )
    if topology == "multi-node-scale-out":
        spec["executor"]["affinity"] = {
            "podAntiAffinity": {
                "preferredDuringSchedulingIgnoredDuringExecution": [
                    {
                        "weight": 100,
                        "podAffinityTerm": {
                            "topologyKey": "kubernetes.io/hostname",
                            "labelSelector": {
                                "matchLabels": {
                                    "data-master.io/run-id": run_id,
                                    "data-master.io/spark-role": "executor",
                                }
                            },
                        },
                    }
                ]
            }
        }
    return application
