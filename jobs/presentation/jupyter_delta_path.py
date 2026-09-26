# Copyright (C) 2026 Alexandre Ferreira
# SPDX-License-Identifier: AGPL-3.0-only

"""Read-only helpers for the Jupyter presentation path.

The helpers expose stable temporary-view names so a presentation can focus on
SQL instead of repeating physical S3A paths. They never write to the lakehouse.
"""

from __future__ import annotations

import os
from collections import OrderedDict
from importlib.metadata import version
from typing import Dict, Mapping


PRESENTATION_LAYERS = OrderedDict(
    (
        (
            "bronze",
            {
                "relative_path": "bronze/transacoes",
                "view": "bronze_transacoes",
            },
        ),
        (
            "raw_vault",
            {
                "relative_path": "raw_vault/hubs/hub_transacao",
                "view": "raw_hub_transacao",
            },
        ),
        (
            "gold",
            {
                "relative_path": "gold/gold_transacoes_por_dia",
                "view": "gold_transacoes_por_dia",
            },
        ),
    )
)

GOLD_PRESENTATION_QUERY = """
SELECT
    data,
    tipo_transacao,
    quantidade_transacoes,
    ROUND(valor_total, 2) AS valor_total
FROM gold_transacoes_por_dia
ORDER BY data DESC, tipo_transacao
LIMIT 10
""".strip()


def normalize_lakehouse_root(value: str | None = None) -> str:
    """Return one explicit lakehouse root without a trailing separator."""

    root = (
        value
        if value is not None
        else os.getenv("LAKEHOUSE_ROOT", "s3a://lakehouse")
    )
    root = root.strip().rstrip("/")
    if not root or "://" not in root:
        raise ValueError("LAKEHOUSE_ROOT must be an explicit URI.")
    return root


def presentation_paths(lakehouse_root: str | None = None) -> Dict[str, str]:
    """Resolve the three presentation datasets from one physical root."""

    root = normalize_lakehouse_root(lakehouse_root)
    return {
        name: f"{root}/{definition['relative_path']}"
        for name, definition in PRESENTATION_LAYERS.items()
    }


def create_presentation_session(app_name: str = "data-master-jupyter-presentation"):
    """Create a local Spark session with the same Delta/S3A stack as jobs."""

    from pyspark.sql import SparkSession

    endpoint = os.getenv(
        "MINIO_ENDPOINT",
        "http://minio.data-platform.svc.cluster.local:9000",
    )
    master = os.getenv("JUPYTER_SPARK_MASTER", "local[2]")
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
        )
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.databricks.delta.snapshotPartitions", "4")
        .config("spark.ui.enabled", "false")
    )
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return session


def _latest_delta_version(spark, path: str) -> int:
    from delta.tables import DeltaTable

    row = DeltaTable.forPath(spark, path).history(1).select("version").first()
    if row is None:
        raise RuntimeError("Delta history is empty for a presentation dataset.")
    return int(row["version"])


def prepare_presentation_views(
    spark,
    lakehouse_root: str | None = None,
) -> Dict[str, Mapping[str, object]]:
    """Read committed Delta snapshots and register three temporary views."""

    paths = presentation_paths(lakehouse_root)
    results: Dict[str, Mapping[str, object]] = {}
    for layer, definition in PRESENTATION_LAYERS.items():
        path = paths[layer]
        version_before = _latest_delta_version(spark, path)
        frame = spark.read.format("delta").load(path)
        view = definition["view"]
        frame.createOrReplaceTempView(view)
        row_count = int(spark.sql(f"SELECT COUNT(*) AS n FROM {view}").first()["n"])
        version_after = _latest_delta_version(spark, path)
        if row_count < 1:
            raise RuntimeError(f"Presentation layer '{layer}' is empty.")
        if version_before != version_after:
            raise RuntimeError(
                f"Presentation layer '{layer}' changed during snapshot inspection."
            )
        results[layer] = {
            "view": view,
            "row_count": row_count,
            "delta_version": version_before,
            "snapshot_stable": True,
        }
    return results


def validate_presentation_session(spark) -> Dict[str, str]:
    """Fail closed when Delta or the S3A credential provider is not active."""

    expected = {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": (
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        ),
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
        ),
    }
    observed = {key: spark.conf.get(key, "") for key in expected}
    mismatches = {
        key: value
        for key, value in expected.items()
        if observed.get(key) != value
    }
    if mismatches:
        raise RuntimeError(
            "Jupyter Spark session is missing required Delta/S3A configuration."
        )
    return {
        "spark_version": spark.version,
        "delta_version": version("delta-spark"),
        "delta_extension": observed["spark.sql.extensions"],
        "delta_catalog": observed["spark.sql.catalog.spark_catalog"],
        "credentials_provider": observed[
            "spark.hadoop.fs.s3a.aws.credentials.provider"
        ],
    }


def run_prepared_gold_query(spark):
    """Return the presentation query DataFrame without printing business rows."""

    return spark.sql(GOLD_PRESENTATION_QUERY)
