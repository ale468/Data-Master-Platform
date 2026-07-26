"""
Runtime profile catalog for the Data Master case pipeline.

Profiles keep demo volume, Spark resource, and expectation settings explicit
without forcing each job to duplicate environment-specific constants.
"""
import copy
import os
from typing import Any, Dict, List, Mapping, Optional


DEFAULT_RUNTIME_PROFILE = "presentation-demo"

HORIZONTAL_DATASET_SEED = 42
HORIZONTAL_DATASET_REFERENCE_TIME = "2026-01-01T00:00:00+00:00"
HORIZONTAL_DATASET_VOLUME = "controlled-horizontal-v1"
HORIZONTAL_PIPELINE_CONTRACT_VERSION = "data-master-pipeline-v1"
HORIZONTAL_SHUFFLE_PARTITIONS = 24


def build_horizontal_profile(executor_instances: int) -> Dict[str, Any]:
    """Build one static horizontal profile from the shared experiment contract."""
    if executor_instances not in (1, 3):
        raise ValueError("Horizontal executor instances must be exactly 1 or 3.")

    profile_id = f"minikube-horizontal-{executor_instances}"
    return {
        "id": profile_id,
        "label": "Minikube static horizontal benchmark",
        "description": (
            "Controlled Spark Operator cluster-mode profile for static scale-out."
        ),
        "execution": {
            "mode": "kubernetes",
            "orchestrator": "direct",
            "processing": "spark-operator",
            "max_runtime_minutes": 45,
            "expectation": "controlled static horizontal Spark scale-out evidence",
        },
        "submission": {
            "mode": "spark-operator-direct",
        },
        "dataset": {
            "seed": HORIZONTAL_DATASET_SEED,
            "reference_time": HORIZONTAL_DATASET_REFERENCE_TIME,
            "volume": HORIZONTAL_DATASET_VOLUME,
            "pipeline_contract_version": HORIZONTAL_PIPELINE_CONTRACT_VERSION,
        },
        "batch": {
            "clientes": 5000,
            "agencias": 100,
            "produtos": 50,
            "accounts_per_client": 3,
            "cards_per_account": 2,
            "transacoes": 100000,
            "eventos_digitais_file": 200000,
        },
        "streaming": {
            "enabled": False,
            "status": "outside-this-profile",
            "expected_events_per_second": 0,
            "checkpoint_required": True,
            "demo_event_count": 0,
            "demo_file_count": 0,
            "trigger": "external",
        },
        "cdc": {
            "enabled": False,
            "status": "outside-this-profile",
            "snapshot_required": False,
            "operations": ["snapshot", "insert", "update", "delete"],
            "demo_event_count": 0,
            "demo_source_table": "core_clientes",
        },
        "spark": {
            "master": "k8s://https://kubernetes.default.svc",
            "driver_memory": "1g",
            "executor_memory": "1g",
            "executor_instances": executor_instances,
            "shuffle_partitions": HORIZONTAL_SHUFFLE_PARTITIONS,
            "adaptive_enabled": False,
            "dynamic_allocation": False,
        },
        "kubernetes": {
            "namespace": "data-platform",
            "service_account": "spark",
            "image": "${DATA_MASTER_SPARK_IMAGE_DIGEST}",
            "image_pull_policy": "IfNotPresent",
            "driver_cores": 1,
            "executor_cores": 1,
            "minio_endpoint": (
                "http://minio.data-platform.svc.cluster.local:9000"
            ),
            "credentials_secret": "data-master-minio-secret",
            "storage_root": "s3a://lakehouse/horizontal",
            "bronze_path": "s3a://lakehouse/horizontal",
            "raw_vault_path": "s3a://lakehouse/horizontal",
            "gold_path": "s3a://lakehouse/horizontal",
        },
        "quality": {
            "data_vault_gate": True,
            "lineage_gate": True,
            "masking_gate": True,
            "secret_scan": True,
        },
        "observability": {
            "enabled": True,
            "write_metrics_to_delta": True,
            "event_log_enabled": False,
            "spark_status_api_enabled": True,
            "log_level": "INFO",
        },
    }


RUNTIME_PROFILES: Dict[str, Dict[str, Any]] = {
    "local-small": {
        "id": "local-small",
        "label": "Local small",
        "description": "Fast local smoke profile for developer validation.",
        "execution": {
            "mode": "local",
            "orchestrator": "direct",
            "processing": "spark-local",
            "max_runtime_minutes": 5,
            "expectation": "schema and wiring smoke validation",
        },
        "submission": {
            "mode": "local",
        },
        "batch": {
            "clientes": 20,
            "agencias": 5,
            "produtos": 5,
            "accounts_per_client": 2,
            "cards_per_account": 2,
            "transacoes": 100,
            "eventos_digitais_file": 200,
        },
        "streaming": {
            "enabled": True,
            "status": "local-demo",
            "expected_events_per_second": 2,
            "checkpoint_required": True,
            "demo_event_count": 40,
            "demo_file_count": 2,
            "trigger": "once",
        },
        "cdc": {
            "enabled": True,
            "status": "local-demo",
            "snapshot_required": False,
            "operations": ["snapshot", "insert", "update", "delete"],
            "demo_event_count": 5,
            "demo_source_table": "core_clientes",
        },
        "spark": {
            "master": "local[*]",
            "driver_memory": "768m",
            "executor_memory": "768m",
            "executor_instances": 1,
            "shuffle_partitions": 2,
            "adaptive_enabled": True,
        },
        "observability": {
            "enabled": True,
            "write_metrics_to_delta": False,
            "log_level": "INFO",
        },
    },
    "minikube-integration": {
        "id": "minikube-integration",
        "label": "Minikube Spark integration",
        "description": "Direct SparkApplication validation in an isolated Minikube profile.",
        "execution": {
            "mode": "kubernetes",
            "orchestrator": "direct",
            "processing": "spark-operator",
            "max_runtime_minutes": 20,
            "expectation": "direct distributed Spark and MinIO integration evidence",
        },
        "submission": {
            "mode": "spark-operator-direct",
        },
        "batch": {
            "clientes": 20,
            "agencias": 5,
            "produtos": 5,
            "accounts_per_client": 2,
            "cards_per_account": 2,
            "transacoes": 100,
            "eventos_digitais_file": 200,
        },
        "streaming": {
            "enabled": False,
            "status": "outside-this-profile",
            "expected_events_per_second": 0,
            "checkpoint_required": True,
            "demo_event_count": 0,
            "demo_file_count": 0,
            "trigger": "external",
        },
        "cdc": {
            "enabled": False,
            "status": "outside-this-profile",
            "snapshot_required": False,
            "operations": ["snapshot", "insert", "update", "delete"],
            "demo_event_count": 0,
            "demo_source_table": "core_clientes",
        },
        "spark": {
            "master": "k8s://https://kubernetes.default.svc",
            "driver_memory": "768m",
            "executor_memory": "768m",
            "executor_instances": 1,
            "shuffle_partitions": 2,
            "adaptive_enabled": True,
        },
        "kubernetes": {
            "namespace": "data-platform",
            "service_account": "spark",
            "image": "${DATA_MASTER_SPARK_IMAGE}",
            "image_pull_policy": "IfNotPresent",
            "driver_cores": 1,
            "executor_cores": 1,
            "minio_endpoint": "http://minio.data-platform.svc.cluster.local:9000",
            "credentials_secret": "data-master-minio-secret",
            "bronze_path": "s3a://lakehouse/bronze",
            "raw_vault_path": "s3a://lakehouse/raw_vault",
            "gold_path": "s3a://lakehouse/gold",
        },
        "observability": {
            "enabled": True,
            "write_metrics_to_delta": True,
            "log_level": "INFO",
        },
    },
    "minikube-horizontal-1": build_horizontal_profile(1),
    "minikube-horizontal-3": build_horizontal_profile(3),
    "presentation-demo": {
        "id": "presentation-demo",
        "label": "Presentation demo",
        "description": "Official local E2E through Airflow and Spark Operator on Minikube.",
        "execution": {
            "mode": "airflow-spark-operator",
            "orchestrator": "airflow",
            "processing": "spark-operator",
            "max_runtime_minutes": 15,
            "expectation": "Airflow-submitted distributed Spark demo with evidence capture",
        },
        "submission": {
            "mode": "airflow",
        },
        "batch": {
            "clientes": 100,
            "agencias": 20,
            "produtos": 15,
            "accounts_per_client": 2,
            "cards_per_account": 2,
            "transacoes": 1000,
            "eventos_digitais_file": 2000,
        },
        "streaming": {
            "enabled": True,
            "status": "local-demo",
            "expected_events_per_second": 2,
            "checkpoint_required": True,
            "demo_event_count": 240,
            "demo_file_count": 4,
            "trigger": "once",
        },
        "cdc": {
            "enabled": True,
            "status": "local-demo",
            "snapshot_required": True,
            "operations": ["snapshot", "insert", "update", "delete"],
            "demo_event_count": 5,
            "demo_source_table": "core_clientes",
        },
        "spark": {
            "master": "k8s://https://kubernetes.default.svc",
            "driver_memory": "1g",
            "executor_memory": "1536m",
            "executor_instances": 1,
            "shuffle_partitions": 4,
            "adaptive_enabled": True,
        },
        "kubernetes": {
            "namespace": "data-platform",
            "service_account": "spark",
            "image": "${DATA_MASTER_SPARK_IMAGE}",
            "image_pull_policy": "IfNotPresent",
            "driver_cores": 1,
            "executor_cores": 1,
            "minio_endpoint": "http://minio.data-platform.svc.cluster.local:9000",
            "credentials_secret": "data-master-minio-secret",
            "bronze_path": "s3a://lakehouse/bronze",
            "raw_vault_path": "s3a://lakehouse/raw_vault",
            "gold_path": "s3a://lakehouse/gold",
        },
        "observability": {
            "enabled": True,
            "write_metrics_to_delta": True,
            "log_level": "INFO",
        },
    },
    "local-medium": {
        "id": "local-medium",
        "label": "Local medium",
        "description": "Expanded local validation profile for heavier batch tests.",
        "execution": {
            "mode": "local",
            "orchestrator": "direct",
            "processing": "spark-local",
            "max_runtime_minutes": 30,
            "expectation": "larger local batch validation",
        },
        "submission": {
            "mode": "local",
        },
        "batch": {
            "clientes": 500,
            "agencias": 30,
            "produtos": 25,
            "accounts_per_client": 2,
            "cards_per_account": 2,
            "transacoes": 5000,
            "eventos_digitais_file": 10000,
        },
        "streaming": {
            "enabled": True,
            "status": "local-demo",
            "expected_events_per_second": 5,
            "checkpoint_required": True,
            "demo_event_count": 1000,
            "demo_file_count": 10,
            "trigger": "once",
        },
        "cdc": {
            "enabled": True,
            "status": "local-demo",
            "snapshot_required": True,
            "operations": ["snapshot", "insert", "update", "delete"],
            "demo_event_count": 5,
            "demo_source_table": "core_clientes",
        },
        "spark": {
            "master": "local[*]",
            "driver_memory": "2g",
            "executor_memory": "2g",
            "executor_instances": 1,
            "shuffle_partitions": 8,
            "adaptive_enabled": True,
        },
        "observability": {
            "enabled": True,
            "write_metrics_to_delta": True,
            "log_level": "INFO",
        },
    },
    "cloud-ready": {
        "id": "cloud-ready",
        "label": "Cloud ready",
        "description": "Reference profile for Kubernetes/Spark scaling beyond the local demo.",
        "execution": {
            "mode": "kubernetes",
            "orchestrator": "future",
            "processing": "spark-operator-reference",
            "max_runtime_minutes": 60,
            "expectation": "cloud-oriented configuration reference; requires environment validation",
        },
        "submission": {
            "mode": "reference-only",
        },
        "batch": {
            "clientes": 5000,
            "agencias": 100,
            "produtos": 50,
            "accounts_per_client": 3,
            "cards_per_account": 2,
            "transacoes": 100000,
            "eventos_digitais_file": 200000,
        },
        "streaming": {
            "enabled": False,
            "status": "reference-only",
            "expected_events_per_second": 1000,
            "checkpoint_required": True,
            "demo_event_count": 0,
            "demo_file_count": 0,
            "trigger": "external",
        },
        "cdc": {
            "enabled": False,
            "status": "reference-only",
            "snapshot_required": True,
            "operations": ["snapshot", "insert", "update", "delete"],
            "demo_event_count": 0,
            "demo_source_table": "external_core_table",
        },
        "spark": {
            "master": "k8s://https://kubernetes.default.svc",
            "driver_memory": "4g",
            "executor_memory": "4g",
            "executor_instances": 4,
            "shuffle_partitions": 64,
            "adaptive_enabled": True,
        },
        "kubernetes": {
            "namespace": "data-platform",
            "service_account": "spark",
            "image": "${DATA_MASTER_SPARK_IMAGE}",
            "image_pull_policy": "IfNotPresent",
            "driver_cores": 1,
            "executor_cores": 1,
            "minio_endpoint": "${CLOUD_OBJECT_STORAGE_ENDPOINT}",
            "credentials_secret": "${CLOUD_OBJECT_STORAGE_SECRET}",
            "bronze_path": "${CLOUD_BRONZE_PATH}",
            "raw_vault_path": "${CLOUD_RAW_VAULT_PATH}",
            "gold_path": "${CLOUD_GOLD_PATH}",
        },
        "observability": {
            "enabled": True,
            "write_metrics_to_delta": True,
            "log_level": "INFO",
        },
    },
}


REQUIRED_PROFILE_SECTIONS = (
    "execution",
    "submission",
    "batch",
    "streaming",
    "cdc",
    "spark",
    "observability",
)

REQUIRED_KUBERNETES_FIELDS = (
    "namespace",
    "service_account",
    "image",
    "image_pull_policy",
    "driver_cores",
    "executor_cores",
    "minio_endpoint",
    "credentials_secret",
    "bronze_path",
    "raw_vault_path",
    "gold_path",
)

REQUIRED_BATCH_FIELDS = (
    "clientes",
    "agencias",
    "produtos",
    "accounts_per_client",
    "cards_per_account",
    "transacoes",
    "eventos_digitais_file",
)


def list_runtime_profiles() -> List[str]:
    """Return profile names in declaration order."""
    return list(RUNTIME_PROFILES.keys())


def resolve_runtime_profile_name(profile_name: Optional[str] = None) -> str:
    """Resolve the explicit, env, or default runtime profile name."""
    return (
        profile_name
        or os.getenv("RUNTIME_PROFILE")
        or os.getenv("DM_RUNTIME_PROFILE")
        or DEFAULT_RUNTIME_PROFILE
    )


def get_runtime_profile(profile_name: Optional[str] = None) -> Dict[str, Any]:
    """Return a validated copy of a runtime profile or raise a clear error."""
    resolved_name = resolve_runtime_profile_name(profile_name)
    if resolved_name not in RUNTIME_PROFILES:
        available = ", ".join(list_runtime_profiles())
        raise ValueError(
            f"Invalid runtime profile '{resolved_name}'. "
            f"Available profiles: {available}."
        )

    profile = copy.deepcopy(RUNTIME_PROFILES[resolved_name])
    validate_runtime_profile(profile)
    return profile


def validate_runtime_profile(profile: Mapping[str, Any]) -> None:
    """Validate minimum fields required by jobs, DAGs, and validation records."""
    profile_id = profile.get("id", "<unknown>")

    missing_sections = [
        section for section in REQUIRED_PROFILE_SECTIONS if section not in profile
    ]
    if missing_sections:
        raise ValueError(
            f"Runtime profile '{profile_id}' is missing sections: "
            f"{', '.join(missing_sections)}."
        )

    batch = profile["batch"]
    missing_batch_fields = [
        field for field in REQUIRED_BATCH_FIELDS if field not in batch
    ]
    if missing_batch_fields:
        raise ValueError(
            f"Runtime profile '{profile_id}' is missing batch fields: "
            f"{', '.join(missing_batch_fields)}."
        )

    for field in REQUIRED_BATCH_FIELDS:
        value = batch[field]
        if not isinstance(value, int) or value < 0:
            raise ValueError(
                f"Runtime profile '{profile_id}' batch field '{field}' "
                "must be a non-negative integer."
            )

    spark = profile["spark"]
    for field in ("master", "driver_memory", "executor_memory", "executor_instances", "shuffle_partitions"):
        if field not in spark:
            raise ValueError(
                f"Runtime profile '{profile_id}' is missing spark field '{field}'."
            )

    if str(profile_id).startswith("minikube-horizontal-"):
        if spark["master"].startswith("local"):
            raise ValueError(
                f"Runtime profile '{profile_id}' cannot use a local Spark master."
            )
        if spark.get("dynamic_allocation") is not False:
            raise ValueError(
                f"Runtime profile '{profile_id}' must disable dynamic allocation."
            )
        if spark["executor_instances"] not in (1, 3):
            raise ValueError(
                f"Runtime profile '{profile_id}' must request 1 or 3 executors."
            )
        dataset = profile.get("dataset")
        if not isinstance(dataset, Mapping):
            raise ValueError(
                f"Runtime profile '{profile_id}' requires a dataset section."
            )
        for field in (
            "seed",
            "reference_time",
            "volume",
            "pipeline_contract_version",
        ):
            if dataset.get(field) in (None, ""):
                raise ValueError(
                    f"Runtime profile '{profile_id}' is missing dataset field "
                    f"'{field}'."
                )

    execution = profile["execution"]
    for field in ("mode", "orchestrator", "processing"):
        if not execution.get(field):
            raise ValueError(
                f"Runtime profile '{profile_id}' is missing execution field '{field}'."
            )

    if not profile["submission"].get("mode"):
        raise ValueError(
            f"Runtime profile '{profile_id}' is missing submission field 'mode'."
        )

    if execution["mode"] in {"kubernetes", "airflow-spark-operator"}:
        kubernetes = profile.get("kubernetes")
        if not kubernetes:
            raise ValueError(
                f"Runtime profile '{profile_id}' requires a kubernetes section."
            )
        missing_kubernetes_fields = [
            field for field in REQUIRED_KUBERNETES_FIELDS if not kubernetes.get(field)
        ]
        if missing_kubernetes_fields:
            raise ValueError(
                f"Runtime profile '{profile_id}' is missing kubernetes fields: "
                f"{', '.join(missing_kubernetes_fields)}."
            )
