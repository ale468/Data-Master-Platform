"""Helpers mínimos de lineage Bronze -> Raw Vault para DM-DV-002."""

from typing import Iterable, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


BRONZE_LINEAGE_COLUMNS: Tuple[str, ...] = (
    "source_system",
    "source_entity",
    "batch_id",
)


def require_bronze_lineage_schema(df: DataFrame) -> None:
    """Exige somente metadados técnicos já definidos no contrato Bronze."""
    missing = sorted(set(BRONZE_LINEAGE_COLUMNS) - set(df.columns))
    if missing:
        raise ValueError(
            "Metadados Bronze obrigatórios para lineage ausentes: "
            + ", ".join(missing)
        )


def scope_to_source_batch(df: DataFrame, batch_id: str) -> DataFrame:
    """Seleciona o batch Bronze solicitado e reprova metadados vazios."""
    require_bronze_lineage_schema(df)
    scoped = df.filter(F.col("batch_id") == F.lit(batch_id))
    if not scoped.limit(1).count():
        raise ValueError(f"Batch Bronze não encontrado para Raw Vault: {batch_id}")

    invalid_condition = None
    for column in BRONZE_LINEAGE_COLUMNS:
        condition = F.col(column).isNull() | (F.trim(F.col(column)) == "")
        invalid_condition = (
            condition
            if invalid_condition is None
            else invalid_condition | condition
        )
    if scoped.filter(invalid_condition).limit(1).count():
        raise ValueError(
            "Metadados Bronze de lineage não podem ser nulos ou vazios: "
            + ", ".join(BRONZE_LINEAGE_COLUMNS)
        )
    return scoped


def add_raw_vault_record_source(df: DataFrame) -> DataFrame:
    """Materializa ``source_system:source_entity`` sem payload de negócio."""
    require_bronze_lineage_schema(df)
    return df.withColumn(
        "record_source",
        F.concat_ws(
            ":",
            F.trim(F.col("source_system")),
            F.trim(F.col("source_entity")),
        ),
    )


def lineage_projection(data_columns: Iterable[str]) -> Tuple[str, ...]:
    """Retorna projeção explícita que preserva lineage antes da escrita Raw."""
    return tuple(data_columns) + BRONZE_LINEAGE_COLUMNS
