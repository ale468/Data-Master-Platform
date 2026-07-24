"""Business Vault helpers mínimos sobre a Raw Vault para DM-DV-003."""

from typing import Iterable, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../common"))

from delta_io import DeltaIO


RAW_GROUP_PATHS = {
    "hub": "hubs",
    "link": "links",
    "satellite": "satellites",
}


def read_required_raw_table(
    spark: SparkSession,
    raw_vault_path: str,
    group: str,
    table: str,
) -> DataFrame:
    """Lê uma estrutura Raw Vault obrigatória e falha sem fallback."""
    if group not in RAW_GROUP_PATHS:
        raise ValueError(f"Grupo Raw Vault desconhecido: {group}")
    path = f"{raw_vault_path}/{RAW_GROUP_PATHS[group]}/{table}"
    df = DeltaIO.read_delta(spark, path)
    if df is None:
        raise FileNotFoundError(f"Tabela Raw Vault não encontrada: {path}")
    return df


def latest_satellite_state(
    df: DataFrame,
    parent_hash_key: str,
) -> DataFrame:
    """Seleciona deterministicamente o último estado por parent hash key."""
    if parent_hash_key not in df.columns:
        raise ValueError(f"Parent hash key ausente: {parent_hash_key}")

    temporal_columns = [
        column
        for column in ("load_datetime", "effective_from", "batch_id")
        if column in df.columns
    ]
    if not temporal_columns:
        raise ValueError(
            "Satellite precisa de load_datetime, effective_from ou batch_id "
            "para latest state"
        )

    hashdiff_columns = sorted(
        column for column in df.columns if column.startswith("hd_")
    )
    order_by = [F.col(column).desc_nulls_last() for column in temporal_columns]
    order_by.extend(
        F.col(column).desc_nulls_last() for column in hashdiff_columns
    )
    window = Window.partitionBy(parent_hash_key).orderBy(*order_by)
    return (
        df.withColumn("__latest_row_number", F.row_number().over(window))
        .filter(F.col("__latest_row_number") == 1)
        .drop("__latest_row_number")
    )


def descriptive_columns(
    df: DataFrame,
    parent_hash_key: str,
) -> List[str]:
    """Exclui metadata técnica duplicada ao montar uma view de consumo."""
    technical = {
        parent_hash_key,
        "load_datetime",
        "record_source",
        "effective_from",
        "batch_id",
    }
    return [
        column
        for column in df.columns
        if column not in technical and not column.startswith("hd_")
    ]


def hub_with_latest_satellites(
    spark: SparkSession,
    raw_vault_path: str,
    hub_name: str,
    parent_hash_key: str,
    satellite_names: Iterable[str],
) -> DataFrame:
    """Monta uma view latest enxuta preservando business key e parent hash key."""
    result = read_required_raw_table(
        spark, raw_vault_path, "hub", hub_name
    )
    hub_columns = [
        column
        for column in result.columns
        if column not in {"load_datetime", "record_source", "batch_id"}
    ]
    result = result.select(*hub_columns)

    for satellite_name in satellite_names:
        satellite = latest_satellite_state(
            read_required_raw_table(
                spark,
                raw_vault_path,
                "satellite",
                satellite_name,
            ),
            parent_hash_key,
        )
        attributes = descriptive_columns(satellite, parent_hash_key)
        result = result.join(
            satellite.select(parent_hash_key, *attributes),
            on=parent_hash_key,
            how="left",
        )
    return result
