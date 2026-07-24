"""SparkApplication contracts shared by the Airflow Kubernetes DAG."""

from copy import deepcopy
from typing import Any, Dict


ALLOWED_STAGES = {
    "integration",
    "bronze",
    "hubs",
    "links",
    "satellites",
    "gold",
    "data-vault-gate",
    "masking-gate",
    "evidence",
}


def _secret_env(name: str, key: str) -> Dict[str, Any]:
    return {
        "name": name,
        "valueFrom": {
            "secretKeyRef": {
                "name": "data-master-minio-secret",
                "key": key,
            }
        },
    }


def _runtime_env(
    runtime_profile: str,
    paths: Dict[str, str],
) -> list:
    return [
        {"name": "RUNTIME_PROFILE", "value": runtime_profile},
        {"name": "DM_RUNTIME_PROFILE", "value": runtime_profile},
        {"name": "SPARK_JARS_PACKAGES", "value": ""},
        {"name": "MINIO_ENDPOINT", "value": paths["minio_endpoint"]},
        {"name": "BRONZE_PATH", "value": paths["bronze"]},
        {"name": "RAW_VAULT_PATH", "value": paths["raw_vault"]},
        {"name": "BUSINESS_VAULT_PATH", "value": paths["business_vault"]},
        {"name": "GOLD_PATH", "value": paths["gold"]},
        {"name": "MONITORING_PATH", "value": paths["monitoring"]},
        _secret_env("MINIO_ACCESS_KEY", "MINIO_ACCESS_KEY"),
        _secret_env("MINIO_SECRET_KEY", "MINIO_SECRET_KEY"),
        _secret_env("AWS_ACCESS_KEY_ID", "MINIO_ACCESS_KEY"),
        _secret_env("AWS_SECRET_ACCESS_KEY", "MINIO_SECRET_KEY"),
    ]


def _validate_image(image: str) -> None:
    if ":" not in image:
        raise ValueError("Spark image must use an explicit immutable tag.")
    tag = image.rsplit(":", 1)[1].lower()
    if tag in {"latest", "dev", "0.1.0"}:
        raise ValueError(f"Mutable Spark image tag is not allowed: {tag}")


def build_spark_application(
    *,
    stage: str,
    batch_id: str,
    image: str,
    runtime_profile: str,
    namespace: str,
    service_account: str,
    paths: Dict[str, str],
    application_name: str,
) -> Dict[str, Any]:
    if stage not in ALLOWED_STAGES:
        raise ValueError(f"Unsupported Spark stage: {stage}")
    _validate_image(image)

    env = _runtime_env(runtime_profile, paths)
    arguments = [
        "--stage",
        stage,
        "--runtime-profile",
        runtime_profile,
        "--batch-id",
        batch_id,
        "--sample-data-path",
        "/opt/spark/work-dir/data/sample",
        "--bronze-path",
        paths["bronze"],
        "--raw-vault-path",
        paths["raw_vault"],
        "--gold-path",
        paths["gold"],
        "--monitoring-path",
        paths["monitoring"],
    ]
    labels = {
        "app.kubernetes.io/part-of": "data-master-platform",
        "data-master.io/runtime-profile": runtime_profile,
        "data-master.io/stage": stage,
    }

    return {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": application_name,
            "namespace": namespace,
            "labels": labels,
        },
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "image": image,
            "imagePullPolicy": "IfNotPresent",
            "mainApplicationFile": (
                "local:///opt/spark/work-dir/jobs/kubernetes/"
                "run_pipeline_stage.py"
            ),
            "arguments": arguments,
            "sparkVersion": "3.3.1",
            "timeToLiveSeconds": 900,
            "restartPolicy": {
                "type": "OnFailure",
                "onFailureRetries": 1,
                "onFailureRetryInterval": 10,
                "onSubmissionFailureRetries": 2,
                "onSubmissionFailureRetryInterval": 20,
            },
            "sparkConf": {
                "spark.sql.extensions": (
                    "io.delta.sql.DeltaSparkSessionExtension"
                ),
                "spark.sql.catalog.spark_catalog": (
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog"
                ),
                "spark.databricks.delta.schema.autoMerge.enabled": "true",
                "spark.hadoop.fs.s3a.endpoint": paths["minio_endpoint"],
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
                "spark.hadoop.fs.s3a.impl": (
                    "org.apache.hadoop.fs.s3a.S3AFileSystem"
                ),
                "spark.hadoop.fs.s3a.aws.credentials.provider": (
                    "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
                ),
                "spark.dynamicAllocation.enabled": "false",
                "spark.kubernetes.executor.deleteOnTermination": "false",
                "spark.driver.extraJavaOptions": "-XX:-UseContainerSupport",
                "spark.executor.extraJavaOptions": "-XX:-UseContainerSupport",
                "spark.sql.shuffle.partitions": "4",
                "spark.databricks.delta.snapshotPartitions": "4",
            },
            "driver": {
                "cores": 1,
                "coreLimit": "1000m",
                "memory": "1g",
                "memoryOverhead": "256m",
                "serviceAccount": service_account,
                "labels": {
                    **labels,
                    "data-master.io/spark-role": "driver",
                },
                "env": deepcopy(env),
            },
            "executor": {
                "cores": 1,
                "instances": 1,
                "memory": "1536m",
                "memoryOverhead": "384m",
                "labels": {
                    **labels,
                    "data-master.io/spark-role": "executor",
                },
                "env": deepcopy(env),
            },
        },
    }
