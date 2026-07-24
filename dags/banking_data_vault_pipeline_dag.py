"""Official local E2E DAG: Airflow orchestrates SparkApplications on Minikube."""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)

from spark_application_factory import build_spark_application


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "description": "Pipeline bancário Data Vault pelo Spark Operator",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

RUNTIME_PROFILE = Variable.get("RUNTIME_PROFILE", "presentation-demo")
SPARK_IMAGE = Variable.get(
    "DATA_MASTER_SPARK_IMAGE",
    os.getenv(
        "DATA_MASTER_SPARK_IMAGE",
        "data-master-spark-jobs:git-unpublished",
    ),
)
KUBERNETES_NAMESPACE = Variable.get(
    "KUBERNETES_NAMESPACE",
    "data-platform",
)
KUBERNETES_CONN_ID = Variable.get(
    "KUBERNETES_CONN_ID",
    "kubernetes_default",
)
SPARK_SERVICE_ACCOUNT = Variable.get(
    "SPARK_SERVICE_ACCOUNT",
    "spark",
)

PATHS = {
    "minio_endpoint": Variable.get(
        "MINIO_ENDPOINT",
        "http://minio.data-platform.svc.cluster.local:9000",
    ),
    "bronze": Variable.get("BRONZE_PATH", "s3a://lakehouse/bronze"),
    "raw_vault": Variable.get(
        "RAW_VAULT_PATH",
        "s3a://lakehouse/raw_vault",
    ),
    "business_vault": Variable.get(
        "BUSINESS_VAULT_PATH",
        "s3a://lakehouse/business_vault",
    ),
    "gold": Variable.get(
        "GOLD_PATH",
        "s3a://lakehouse/gold",
    ),
    "monitoring": Variable.get(
        "MONITORING_PATH",
        "s3a://lakehouse/monitoring",
    ),
}

BATCH_ID = "{{ ts_nodash | lower }}"
APPLICATION_SUFFIX = "{{ ts_nodash | lower }}"


def _spark_task(stage: str) -> SparkKubernetesOperator:
    application_name = f"dm-{stage}-{APPLICATION_SUFFIX}"
    return SparkKubernetesOperator(
        task_id=f"run_{stage.replace('-', '_')}",
        name=f"dm-{stage}",
        labels={
            "app.kubernetes.io/part-of": "data-master-platform",
            "data-master.io/runtime-profile": RUNTIME_PROFILE,
            "data-master.io/stage": stage,
        },
        namespace=KUBERNETES_NAMESPACE,
        kubernetes_conn_id=KUBERNETES_CONN_ID,
        template_spec=build_spark_application(
            stage=stage,
            batch_id=BATCH_ID,
            image=SPARK_IMAGE,
            runtime_profile=RUNTIME_PROFILE,
            namespace=KUBERNETES_NAMESPACE,
            service_account=SPARK_SERVICE_ACCOUNT,
            paths=PATHS,
            application_name=application_name,
        ),
        get_logs=True,
        log_events_on_failure=True,
        reattach_on_restart=True,
        delete_on_termination=False,
        success_run_history_limit=2,
        startup_timeout_seconds=600,
        do_xcom_push=False,
    )


with DAG(
    dag_id="banking_data_vault_pipeline",
    default_args=DEFAULT_ARGS,
    description=(
        "Airflow submete SparkApplications para Bronze, Raw Vault, "
        "Business Vault lógica, Gold e gates"
    ),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["banking", "data-vault", "minikube", "spark-operator"],
) as dag:
    stages = [
        "bronze",
        "hubs",
        "links",
        "satellites",
        "gold",
        "data-vault-gate",
        "masking-gate",
        "evidence",
    ]
    tasks = [_spark_task(stage) for stage in stages]

    for upstream, downstream in zip(tasks, tasks[1:]):
        upstream >> downstream


if __name__ == "__main__":
    print(f"DAG={dag.dag_id}")
    print(f"RUNTIME_PROFILE={RUNTIME_PROFILE}")
    print("AIRFLOW_ROLE=ORCHESTRATION_ONLY")
