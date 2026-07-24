"""
Carregamento de Satellites do Raw Vault.

Satellites guardam atributos descritivos e historico de mudancas. Cada novo
conjunto de atributos gera um hashdiff; quando o hashdiff ainda não existe
para a entidade, uma nova linha historica e adicionada.
"""
import logging
import os
import sys
from typing import Any, Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../common"))

from config import Config
from delta_io import DeltaIO
from hashing import BusinessKeyHasher, HashingUtils
from monitoring import ExecutionMetrics, MonitoringLogger
from raw_vault_lineage import (
    add_raw_vault_record_source,
    lineage_projection,
    scope_to_source_batch,
)
from spark_session import create_spark_session

logger = logging.getLogger(__name__)


SATELLITE_SPECS: List[Dict[str, Any]] = [
    {
        "satellite": "sat_cliente_dados_cadastrais",
        "source_table": "clientes",
        "business_key": "cliente_id",
        "hash_key": "hk_cliente",
        "hashdiff": "hd_cliente_dados",
        "attributes": ["nome", "email", "telefone", "data_nascimento", "estado", "cidade", "endereco"],
    },
    {
        "satellite": "sat_cliente_documentos",
        "source_table": "clientes",
        "business_key": "cliente_id",
        "hash_key": "hk_cliente",
        "hashdiff": "hd_cliente_documentos",
        "attributes": ["cpf", "data_cadastro"],
    },
    {
        "satellite": "sat_conta_detalhes",
        "source_table": "contas",
        "business_key": "conta_id",
        "hash_key": "hk_conta",
        "hashdiff": "hd_conta_detalhes",
        "attributes": ["cliente_id", "agencia_id", "produto_id", "tipo_conta", "agencia", "numero_conta", "saldo", "limite", "data_abertura", "status"],
    },
    {
        "satellite": "sat_cartao_detalhes",
        "source_table": "cartoes",
        "business_key": "cartao_id",
        "hash_key": "hk_cartao",
        "hashdiff": "hd_cartao_detalhes",
        "attributes": ["conta_id", "numero_cartao", "tipo_cartao", "bandeira", "data_emissao", "data_expiracao", "status"],
    },
    {
        "satellite": "sat_transacao_detalhes",
        "source_table": "transacoes",
        "business_key": "transacao_id",
        "hash_key": "hk_transacao",
        "hashdiff": "hd_transacao_detalhes",
        "attributes": ["conta_id", "cartao_id", "tipo_transacao", "valor", "data_transacao", "data_liquidacao", "status", "descricao"],
    },
    {
        "satellite": "sat_agencia_detalhes",
        "source_table": "agencias",
        "business_key": "agencia_id",
        "hash_key": "hk_agencia",
        "hashdiff": "hd_agencia_detalhes",
        "attributes": ["numero_agencia", "nome", "estado", "cidade", "endereco", "telefone", "gerente", "data_inauguracao"],
    },
    {
        "satellite": "sat_produto_detalhes",
        "source_table": "produtos",
        "business_key": "produto_id",
        "hash_key": "hk_produto",
        "hashdiff": "hd_produto_detalhes",
        "attributes": ["nome_produto", "descricao", "taxa_juros", "comissao", "data_lancamento", "status"],
    },
    {
        "satellite": "sat_evento_digital_detalhes",
        "source_table": "eventos_digitais",
        "business_key": "canal_id",
        "hash_key": "hk_canal_digital",
        "hashdiff": "hd_evento_digital_detalhes",
        "attributes": ["canal", "tipo_evento", "resultado", "detalhes"],
    },
]


def _read_required_bronze(spark: SparkSession, bronze_path: str, table: str) -> DataFrame:
    df = DeltaIO.read_delta(spark, f"{bronze_path}/{table}")
    if df is None:
        raise FileNotFoundError(f"Tabela Bronze não encontrada: {bronze_path}/{table}")
    return df


def _existing_columns(df: DataFrame, columns: List[str]) -> List[str]:
    return [col for col in columns if col in df.columns]


def latest_satellite_state(
    existing_df: DataFrame,
    hash_key: str,
    hashdiff: str,
) -> DataFrame:
    """Retorna o último hashdiff por parent hash key com desempate estável."""
    temporal_columns = [
        column
        for column in ("load_datetime", "effective_from", "batch_id")
        if column in existing_df.columns
    ]
    if not temporal_columns:
        raise ValueError(
            "Satellite existente precisa de load_datetime, effective_from "
            "ou batch_id para determinar o último estado"
        )

    order_by = [F.col(column).desc_nulls_last() for column in temporal_columns]
    order_by.append(F.col(hashdiff).desc_nulls_last())
    latest_window = Window.partitionBy(hash_key).orderBy(*order_by)

    return (
        existing_df
        .withColumn("__satellite_row_number", F.row_number().over(latest_window))
        .filter(F.col("__satellite_row_number") == 1)
        .select(
            F.col(hash_key).alias("__latest_hash_key"),
            F.col(hashdiff).alias("__latest_hashdiff"),
        )
    )


def filter_new_satellite_records(
    incoming_df: DataFrame,
    existing_df: DataFrame,
    hash_key: str,
    hashdiff: str,
) -> DataFrame:
    """Mantém estados novos comparando apenas com o último estado do parent.

    A identidade ``hash_key + hashdiff + batch_id`` também é usada para impedir
    duplicação quando um batch já processado é executado novamente. Um retorno
    legítimo ao mesmo hashdiff em outro batch continua permitido.
    """
    latest_df = latest_satellite_state(existing_df, hash_key, hashdiff)
    candidates = (
        incoming_df
        .join(
            latest_df,
            incoming_df[hash_key] == latest_df["__latest_hash_key"],
            how="left",
        )
        .filter(
            F.col("__latest_hash_key").isNull()
            | (incoming_df[hashdiff] != F.col("__latest_hashdiff"))
        )
        .drop("__latest_hash_key", "__latest_hashdiff")
    )

    if "batch_id" in incoming_df.columns and "batch_id" in existing_df.columns:
        processed_events = existing_df.select(
            F.col(hash_key).alias("__processed_hash_key"),
            F.col(hashdiff).alias("__processed_hashdiff"),
            F.col("batch_id").alias("__processed_batch_id"),
        ).dropDuplicates()
        candidates = (
            candidates
            .join(
                processed_events,
                (candidates[hash_key] == processed_events["__processed_hash_key"])
                & (candidates[hashdiff] == processed_events["__processed_hashdiff"])
                & (candidates["batch_id"] == processed_events["__processed_batch_id"]),
                how="left_anti",
            )
        )

    return candidates


def load_satellite(
    spark: SparkSession,
    bronze_path: str,
    spec: Dict[str, Any],
    batch_id: str,
) -> Dict[str, Any]:
    sat_name = spec["satellite"]
    business_key = spec["business_key"]
    hash_key = spec["hash_key"]
    hashdiff = spec["hashdiff"]
    logger.info("Carregando %s...", sat_name)

    source_df = scope_to_source_batch(
        _read_required_bronze(spark, bronze_path, spec["source_table"]),
        batch_id,
    )
    attributes = _existing_columns(source_df, spec["attributes"])
    required = [business_key, *attributes]
    missing_key = business_key not in source_df.columns
    if missing_key:
        raise ValueError(f"{sat_name}: business key ausente no Bronze: {business_key}")

    df_sat = (
        source_df
        .select(*lineage_projection(required))
        .dropna(subset=[business_key])
        .dropDuplicates(required)
    )
    df_sat = BusinessKeyHasher.spark_generate_hub_hash_key(
        spark, df_sat, [business_key], output_col=hash_key
    )
    df_sat = df_sat.withColumn(
        hashdiff,
        HashingUtils.spark_hash_diff([F.col(col).cast("string") for col in attributes], prefix="hd_"),
    )
    df_sat = add_raw_vault_record_source(df_sat) \
        .withColumn("load_datetime", F.current_timestamp()) \
        .withColumn("effective_from", F.current_timestamp()) \
        .select(hash_key, hashdiff, *attributes, "load_datetime", "record_source", "effective_from", "batch_id")

    path = Config.get_satellite_table_config(sat_name)["path"]
    existing_df = DeltaIO.read_delta(spark, path)
    if existing_df is not None:
        df_sat = filter_new_satellite_records(
            df_sat,
            existing_df,
            hash_key,
            hashdiff,
        )

    rows_written = df_sat.count()
    DeltaIO.create_table_if_not_exists(spark, path, df_sat)
    if rows_written:
        DeltaIO.write_delta_append(df_sat, path)
    total_rows = DeltaIO.read_delta(spark, path).count()

    return {
        "satellite": sat_name,
        "rows_written": rows_written,
        "total_rows": total_rows,
        "status": "SUCCESS",
    }


def run_satellites_pipeline(
    spark: SparkSession,
    bronze_path: str,
    batch_id: str,
) -> Dict[str, Any]:
    logger.info("=" * 80)
    logger.info("INICIANDO PIPELINE DE SATELLITES")
    logger.info("=" * 80)

    metrics = ExecutionMetrics("raw_vault_pipeline", "load_satellites", batch_id=batch_id)
    results: Dict[str, Dict[str, Any]] = {}

    try:
        for spec in SATELLITE_SPECS:
            result = load_satellite(spark, bronze_path, spec, batch_id)
            results[result["satellite"]] = result
            metrics.record_rows_written(result["rows_written"])

        metrics.record_success()
        return {"status": "SUCCESS", "results": results, "batch_id": batch_id}
    except Exception as exc:
        logger.exception("Erro no pipeline de Satellites")
        metrics.record_error(exc)
        return {"status": "FAILURE", "error": str(exc), "batch_id": batch_id}
    finally:
        metrics.finalize(spark)


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Pipeline de Satellites - Data Vault 2.0")
    parser.add_argument("--bronze-path", type=str, default=Config.BRONZE_PATH)
    parser.add_argument("--batch-id", type=str, default=None)
    args = parser.parse_args()

    spark = create_spark_session()
    batch_id = args.batch_id or MonitoringLogger.get_batch_id()
    result = run_satellites_pipeline(spark, args.bronze_path, batch_id)
    return 0 if result["status"] == "SUCCESS" else 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    sys.exit(main())
