"""
Carregamento de Links do Raw Vault.

Links representam relacionamentos entre Hubs. Cada link recebe uma hash key
calculada a partir das hash keys dos Hubs participantes.
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


def _read_required_bronze(spark: SparkSession, bronze_path: str, table: str) -> DataFrame:
    df = DeltaIO.read_delta(spark, f"{bronze_path}/{table}")
    if df is None:
        raise FileNotFoundError(f"Tabela Bronze não encontrada: {bronze_path}/{table}")
    return df


def _add_hub_hash(
    spark: SparkSession,
    df: DataFrame,
    business_key: str,
    hash_key: str,
) -> DataFrame:
    df = df.filter(
        F.col(business_key).isNotNull()
        & (F.trim(F.col(business_key).cast("string")) != "")
    )
    return BusinessKeyHasher.spark_generate_hub_hash_key(
        spark, df, [business_key], output_col=hash_key
    )


def _write_link(
    spark: SparkSession,
    df: DataFrame,
    link_name: str,
    hub_hash_cols: List[str],
    batch_id: str,
) -> Dict[str, Any]:
    logger.info("Carregando %s...", link_name)

    df_link = df.dropna(subset=hub_hash_cols).dropDuplicates(hub_hash_cols)
    df_link = BusinessKeyHasher.spark_generate_link_hash_key(
        spark, df_link, hub_hash_cols, output_col="hk_link"
    )
    df_link = add_raw_vault_record_source(df_link) \
        .withColumn("load_datetime", F.current_timestamp()) \
        .select("hk_link", *hub_hash_cols, "load_datetime", "record_source", "batch_id")

    path = Config.get_link_table_config(link_name)["path"]
    DeltaIO.create_table_if_not_exists(spark, path, df_link)
    before = DeltaIO.read_delta(spark, path).count()
    DeltaIO.write_delta_merge(spark, df_link, path, ["hk_link"])
    after = DeltaIO.read_delta(spark, path).count()

    return {
        "link": link_name,
        "rows_written": after - before,
        "total_rows": after,
        "status": "SUCCESS",
    }


def build_links(spark: SparkSession, bronze_path: str, batch_id: str) -> List[Dict[str, Any]]:
    clientes = scope_to_source_batch(
        _read_required_bronze(spark, bronze_path, "clientes"), batch_id
    )
    contas = scope_to_source_batch(
        _read_required_bronze(spark, bronze_path, "contas"), batch_id
    )
    cartoes = scope_to_source_batch(
        _read_required_bronze(spark, bronze_path, "cartoes"), batch_id
    )
    transacoes = scope_to_source_batch(
        _read_required_bronze(spark, bronze_path, "transacoes"), batch_id
    )
    eventos = scope_to_source_batch(
        _read_required_bronze(spark, bronze_path, "eventos_digitais"), batch_id
    )

    results: List[Dict[str, Any]] = []

    cliente_conta = contas.select(*lineage_projection(["cliente_id", "conta_id"]))
    cliente_conta = _add_hub_hash(spark, cliente_conta, "cliente_id", "hk_cliente")
    cliente_conta = _add_hub_hash(spark, cliente_conta, "conta_id", "hk_conta")
    results.append(_write_link(spark, cliente_conta, "link_cliente_conta", ["hk_cliente", "hk_conta"], batch_id))

    conta_transacao = transacoes.select(*lineage_projection(["conta_id", "transacao_id"]))
    conta_transacao = _add_hub_hash(spark, conta_transacao, "conta_id", "hk_conta")
    conta_transacao = _add_hub_hash(spark, conta_transacao, "transacao_id", "hk_transacao")
    results.append(_write_link(spark, conta_transacao, "link_conta_transacao", ["hk_conta", "hk_transacao"], batch_id))

    cliente_cartao = cartoes.select(
        *lineage_projection(["conta_id", "cartao_id"])
    ).join(contas.select("conta_id", "cliente_id"), on="conta_id", how="inner") \
        .select(*lineage_projection(["cliente_id", "cartao_id"]))
    cliente_cartao = _add_hub_hash(spark, cliente_cartao, "cliente_id", "hk_cliente")
    cliente_cartao = _add_hub_hash(spark, cliente_cartao, "cartao_id", "hk_cartao")
    results.append(_write_link(spark, cliente_cartao, "link_cliente_cartao", ["hk_cliente", "hk_cartao"], batch_id))

    cartao_transacao = transacoes.select(*lineage_projection(["cartao_id", "transacao_id"]))
    cartao_transacao = _add_hub_hash(spark, cartao_transacao, "cartao_id", "hk_cartao")
    cartao_transacao = _add_hub_hash(spark, cartao_transacao, "transacao_id", "hk_transacao")
    results.append(_write_link(spark, cartao_transacao, "link_cartao_transacao", ["hk_cartao", "hk_transacao"], batch_id))

    if "agencia_id" not in contas.columns:
        raise ValueError("Tabela contas deve conter agencia_id para link_conta_agencia")
    conta_agencia = contas.select(*lineage_projection(["conta_id", "agencia_id"]))
    conta_agencia = _add_hub_hash(spark, conta_agencia, "conta_id", "hk_conta")
    conta_agencia = _add_hub_hash(spark, conta_agencia, "agencia_id", "hk_agencia")
    results.append(_write_link(spark, conta_agencia, "link_conta_agencia", ["hk_conta", "hk_agencia"], batch_id))

    if "produto_id" not in contas.columns:
        raise ValueError("Tabela contas deve conter produto_id para link_conta_produto")
    conta_produto = contas.select(*lineage_projection(["conta_id", "produto_id"]))
    conta_produto = _add_hub_hash(spark, conta_produto, "conta_id", "hk_conta")
    conta_produto = _add_hub_hash(spark, conta_produto, "produto_id", "hk_produto")
    results.append(_write_link(spark, conta_produto, "link_conta_produto", ["hk_conta", "hk_produto"], batch_id))

    cliente_canal = eventos.select(
        *lineage_projection(["cliente_id", "canal_id"])
    ).join(clientes.select("cliente_id"), on="cliente_id", how="inner") \
        .select(*lineage_projection(["cliente_id", "canal_id"]))
    cliente_canal = _add_hub_hash(spark, cliente_canal, "cliente_id", "hk_cliente")
    cliente_canal = _add_hub_hash(spark, cliente_canal, "canal_id", "hk_canal_digital")
    results.append(_write_link(spark, cliente_canal, "link_cliente_evento_digital", ["hk_cliente", "hk_canal_digital"], batch_id))

    return results


def run_links_pipeline(spark: SparkSession, bronze_path: str, batch_id: str) -> Dict[str, Any]:
    logger.info("=" * 80)
    logger.info("INICIANDO PIPELINE DE LINKS")
    logger.info("=" * 80)

    metrics = ExecutionMetrics("raw_vault_pipeline", "load_links", batch_id=batch_id)
    try:
        results = build_links(spark, bronze_path, batch_id)
        for result in results:
            metrics.record_rows_written(result["rows_written"])
        metrics.record_success()
        return {
            "status": "SUCCESS",
            "results": {item["link"]: item for item in results},
            "batch_id": batch_id,
        }
    except Exception as exc:
        logger.exception("Erro no pipeline de Links")
        metrics.record_error(exc)
        return {"status": "FAILURE", "error": str(exc), "batch_id": batch_id}
    finally:
        metrics.finalize(spark)


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Pipeline de Links - Data Vault 2.0")
    parser.add_argument("--bronze-path", type=str, default=Config.BRONZE_PATH)
    parser.add_argument("--batch-id", type=str, default=None)
    args = parser.parse_args()

    spark = create_spark_session()
    batch_id = args.batch_id or MonitoringLogger.get_batch_id()
    result = run_links_pipeline(spark, args.bronze_path, batch_id)
    return 0 if result["status"] == "SUCCESS" else 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    sys.exit(main())
