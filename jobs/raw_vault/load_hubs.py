"""
Carregamento de Hubs do Raw Vault.

Cada Hub guarda a business key única da entidade, uma hash key determinística,
data/hora de carga, origem e batch id.
"""
import logging
import os
import sys
from typing import Any, Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../common"))

from config import Config
from delta_io import DeltaIO
from hashing import BusinessKeyHasher
from monitoring import ExecutionMetrics, MonitoringLogger
from raw_vault_lineage import (
    add_raw_vault_record_source,
    lineage_projection,
    scope_to_source_batch,
)
from spark_session import create_spark_session

logger = logging.getLogger(__name__)


HUB_SPECS: List[Dict[str, Any]] = [
    {
        "hub": "hub_cliente",
        "source_table": "clientes",
        "business_key": ["cliente_id"],
        "hash_key": "hk_cliente",
    },
    {
        "hub": "hub_conta",
        "source_table": "contas",
        "business_key": ["conta_id"],
        "hash_key": "hk_conta",
    },
    {
        "hub": "hub_cartao",
        "source_table": "cartoes",
        "business_key": ["cartao_id"],
        "hash_key": "hk_cartao",
    },
    {
        "hub": "hub_transacao",
        "source_table": "transacoes",
        "business_key": ["transacao_id"],
        "hash_key": "hk_transacao",
    },
    {
        "hub": "hub_agencia",
        "source_table": "agencias",
        "business_key": ["agencia_id"],
        "hash_key": "hk_agencia",
    },
    {
        "hub": "hub_produto",
        "source_table": "produtos",
        "business_key": ["produto_id"],
        "hash_key": "hk_produto",
    },
    {
        "hub": "hub_canal_digital",
        "source_table": "eventos_digitais",
        "business_key": ["canal_id"],
        "hash_key": "hk_canal_digital",
    },
]


def _read_required_bronze(spark: SparkSession, bronze_path: str, table: str) -> DataFrame:
    df = DeltaIO.read_delta(spark, f"{bronze_path}/{table}")
    if df is None:
        raise FileNotFoundError(f"Tabela Bronze não encontrada: {bronze_path}/{table}")
    return df


def load_hub(
    spark: SparkSession,
    bronze_path: str,
    spec: Dict[str, Any],
    batch_id: str,
) -> Dict[str, Any]:
    hub_name = spec["hub"]
    business_key = spec["business_key"]
    hash_key = spec["hash_key"]
    logger.info("Carregando %s...", hub_name)

    source_df = scope_to_source_batch(
        _read_required_bronze(spark, bronze_path, spec["source_table"]),
        batch_id,
    )
    missing = set(business_key) - set(source_df.columns)
    if missing:
        raise ValueError(f"{hub_name}: colunas ausentes no Bronze: {sorted(missing)}")

    df_hub = (
        source_df
        .select(*lineage_projection(business_key))
        .dropna(subset=business_key)
        .dropDuplicates(business_key)
    )
    df_hub = BusinessKeyHasher.spark_generate_hub_hash_key(
        spark, df_hub, business_key, output_col=hash_key
    )
    df_hub = add_raw_vault_record_source(df_hub) \
        .withColumn("load_datetime", F.current_timestamp()) \
        .select(hash_key, *business_key, "load_datetime", "record_source", "batch_id")

    path = Config.get_hub_table_config(hub_name)["path"]
    DeltaIO.create_table_if_not_exists(spark, path, df_hub)
    before = DeltaIO.read_delta(spark, path).count()
    DeltaIO.write_delta_merge(spark, df_hub, path, [hash_key])
    after = DeltaIO.read_delta(spark, path).count()

    return {
        "hub": hub_name,
        "rows_written": after - before,
        "total_rows": after,
        "status": "SUCCESS",
    }


def run_hubs_pipeline(
    spark: SparkSession,
    bronze_path: str,
    batch_id: str,
) -> Dict[str, Any]:
    logger.info("=" * 80)
    logger.info("INICIANDO PIPELINE DE HUBS")
    logger.info("=" * 80)

    metrics = ExecutionMetrics("raw_vault_pipeline", "load_hubs", batch_id=batch_id)
    results: Dict[str, Dict[str, Any]] = {}

    try:
        for spec in HUB_SPECS:
            result = load_hub(spark, bronze_path, spec, batch_id)
            results[result["hub"]] = result
            metrics.record_rows_written(result["rows_written"])

        metrics.record_success()
        return {"status": "SUCCESS", "results": results, "batch_id": batch_id}
    except Exception as exc:
        logger.exception("Erro no pipeline de Hubs")
        metrics.record_error(exc)
        return {"status": "FAILURE", "error": str(exc), "batch_id": batch_id}
    finally:
        metrics.finalize(spark)


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Pipeline de Hubs - Data Vault 2.0")
    parser.add_argument("--bronze-path", type=str, default=Config.BRONZE_PATH)
    parser.add_argument("--batch-id", type=str, default=None)
    args = parser.parse_args()

    spark = create_spark_session()
    batch_id = args.batch_id or MonitoringLogger.get_batch_id()
    result = run_hubs_pipeline(spark, args.bronze_path, batch_id)
    return 0 if result["status"] == "SUCCESS" else 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    sys.exit(main())
