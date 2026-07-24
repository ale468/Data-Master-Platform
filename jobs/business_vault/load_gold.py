"""Gold protegida derivada de Raw Vault e helpers mínimos de Business Vault."""

import logging
import os
import sys
from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../common"))

from config import Config
from delta_io import DeltaIO
from monitoring import ExecutionMetrics, MonitoringLogger
from raw_vault_views import (
    hub_with_latest_satellites,
    read_required_raw_table,
)
from spark_session import create_spark_session

logger = logging.getLogger(__name__)


def _write_gold(
    df: DataFrame,
    table_name: str,
    gold_path: str,
) -> Dict[str, Any]:
    path = f"{gold_path}/{table_name}"
    rows_written = df.count()
    DeltaIO.write_delta_overwrite(df, path)
    return {"table": table_name, "rows_written": rows_written, "status": "SUCCESS"}


def _with_gold_metadata(df: DataFrame, batch_id: str) -> DataFrame:
    return (
        df.withColumn("load_datetime", F.current_timestamp())
        .withColumn("batch_id", F.lit(batch_id))
    )


def _pseudonymize_column(column, prefix: str):
    return F.concat(
        F.lit(f"{prefix}_"),
        F.upper(F.substring(F.sha2(column.cast("string"), 256), 1, 8)),
    )


def _mask_name_column(column):
    return F.when(column.isNull(), F.lit(None)).otherwise(
        F.concat(F.substring(column.cast("string"), 1, 1), F.lit("***"))
    )


def _mask_email_column(column):
    return F.when(column.isNull(), F.lit(None)).otherwise(
        F.concat(
            F.substring(column.cast("string"), 1, 1),
            F.lit("***@"),
            F.element_at(F.split(column.cast("string"), "@"), 2),
        )
    )


def _mask_cpf_column(column_name: str):
    return F.concat(
        F.lit("*********"),
        F.expr(f"right(regexp_replace({column_name}, '[^0-9]', ''), 2)"),
    )


def _mask_phone_column(column_name: str):
    return F.concat(
        F.lit("*** (*) ****-"),
        F.expr(f"right(regexp_replace({column_name}, '[^0-9]', ''), 4)"),
    )


class RawBusinessViews:
    """Views mínimas necessárias para as sete saídas Gold existentes."""

    @staticmethod
    def customers(spark: SparkSession, raw_vault_path: str) -> DataFrame:
        return hub_with_latest_satellites(
            spark,
            raw_vault_path,
            "hub_cliente",
            "hk_cliente",
            ["sat_cliente_dados_cadastrais", "sat_cliente_documentos"],
        )

    @staticmethod
    def accounts(spark: SparkSession, raw_vault_path: str) -> DataFrame:
        return hub_with_latest_satellites(
            spark,
            raw_vault_path,
            "hub_conta",
            "hk_conta",
            ["sat_conta_detalhes"],
        )

    @staticmethod
    def cards(spark: SparkSession, raw_vault_path: str) -> DataFrame:
        return hub_with_latest_satellites(
            spark,
            raw_vault_path,
            "hub_cartao",
            "hk_cartao",
            ["sat_cartao_detalhes"],
        )

    @staticmethod
    def transactions(spark: SparkSession, raw_vault_path: str) -> DataFrame:
        return hub_with_latest_satellites(
            spark,
            raw_vault_path,
            "hub_transacao",
            "hk_transacao",
            ["sat_transacao_detalhes"],
        )

    @staticmethod
    def agencies(spark: SparkSession, raw_vault_path: str) -> DataFrame:
        return hub_with_latest_satellites(
            spark,
            raw_vault_path,
            "hub_agencia",
            "hk_agencia",
            ["sat_agencia_detalhes"],
        )


class GoldLayerBuilder:
    """Construtor das tabelas Gold a partir de Raw/Business Vault."""

    @staticmethod
    def create_gold_transacoes_por_dia(
        spark: SparkSession,
        raw_vault_path: str,
        gold_path: str,
        batch_id: str,
    ) -> Dict[str, Any]:
        transactions = RawBusinessViews.transactions(spark, raw_vault_path) \
            .withColumn("valor_decimal", F.col("valor").cast("double"))
        gold = transactions.groupBy(
            F.to_date(F.col("data_transacao")).alias("data"),
            F.col("tipo_transacao"),
        ).agg(
            F.count("transacao_id").alias("quantidade_transacoes"),
            F.sum("valor_decimal").alias("valor_total"),
            F.avg("valor_decimal").alias("valor_medio"),
            F.min("valor_decimal").alias("valor_minimo"),
            F.max("valor_decimal").alias("valor_maximo"),
        )
        return _write_gold(
            _with_gold_metadata(gold, batch_id),
            "gold_transacoes_por_dia",
            gold_path,
        )

    @staticmethod
    def create_gold_transacoes_por_cliente(
        spark: SparkSession,
        raw_vault_path: str,
        gold_path: str,
        batch_id: str,
    ) -> Dict[str, Any]:
        transactions = RawBusinessViews.transactions(spark, raw_vault_path) \
            .withColumn("valor_decimal", F.col("valor").cast("double"))
        link_transaction_account = read_required_raw_table(
            spark, raw_vault_path, "link", "link_conta_transacao"
        ).select("hk_conta", "hk_transacao")
        link_customer_account = read_required_raw_table(
            spark, raw_vault_path, "link", "link_cliente_conta"
        ).select("hk_cliente", "hk_conta")
        customers = RawBusinessViews.customers(spark, raw_vault_path) \
            .select("hk_cliente", "cliente_id")

        joined = transactions.join(
            link_transaction_account, on="hk_transacao", how="inner"
        ).join(
            link_customer_account, on="hk_conta", how="inner"
        ).join(
            customers, on="hk_cliente", how="inner"
        )
        gold = joined.groupBy("cliente_id").agg(
            F.count("transacao_id").alias("quantidade_transacoes"),
            F.sum("valor_decimal").alias("valor_total_transacionado"),
            F.avg("valor_decimal").alias("valor_medio_transacao"),
            F.countDistinct(F.to_date("data_transacao")).alias("dias_com_transacoes"),
        ).withColumn(
            "cliente_id_pseudonimizado",
            _pseudonymize_column(F.col("cliente_id"), prefix="CLI"),
        ).withColumn(
            "nome_cliente", F.lit("[Mascarado]")
        ).drop("cliente_id")
        return _write_gold(
            _with_gold_metadata(gold, batch_id),
            "gold_transacoes_por_cliente",
            gold_path,
        )

    @staticmethod
    def create_gold_clientes_protegidos(
        spark: SparkSession,
        raw_vault_path: str,
        gold_path: str,
        batch_id: str,
    ) -> Dict[str, Any]:
        customers = RawBusinessViews.customers(spark, raw_vault_path)
        gold = customers.select(
            _pseudonymize_column(F.col("cliente_id"), prefix="CLI").alias(
                "cliente_id_pseudonimizado"
            ),
            _mask_name_column(F.col("nome")).alias("nome_cliente"),
            _mask_cpf_column("cpf").alias("cpf_mascarado"),
            _mask_email_column(F.col("email")).alias("email_mascarado"),
            _mask_phone_column("telefone").alias("telefone_mascarado"),
            F.col("estado"),
            F.col("cidade"),
        )
        return _write_gold(
            _with_gold_metadata(gold, batch_id),
            "gold_clientes_protegidos",
            gold_path,
        )

    @staticmethod
    def create_gold_volume_por_produto(
        spark: SparkSession,
        raw_vault_path: str,
        gold_path: str,
        batch_id: str,
    ) -> Dict[str, Any]:
        accounts = RawBusinessViews.accounts(spark, raw_vault_path) \
            .withColumn("saldo_decimal", F.col("saldo").cast("double"))
        cards = RawBusinessViews.cards(spark, raw_vault_path)
        account_aggregate = accounts.groupBy(
            F.col("tipo_conta").alias("tipo_produto")
        ).agg(
            F.count("conta_id").alias("quantidade"),
            F.sum("saldo_decimal").alias("valor_total"),
        ).withColumn("categoria_produto", F.lit("Conta"))
        card_aggregate = cards.groupBy(
            F.col("tipo_cartao").alias("tipo_produto")
        ).agg(
            F.count("cartao_id").alias("quantidade"),
            F.lit(0.0).alias("valor_total"),
        ).withColumn("categoria_produto", F.lit("Cartao"))
        gold = account_aggregate.unionByName(card_aggregate)
        return _write_gold(
            _with_gold_metadata(gold, batch_id),
            "gold_volume_por_produto",
            gold_path,
        )

    @staticmethod
    def create_gold_eventos_digitais_por_canal(
        spark: SparkSession,
        raw_vault_path: str,
        gold_path: str,
        batch_id: str,
    ) -> Dict[str, Any]:
        event_history = read_required_raw_table(
            spark,
            raw_vault_path,
            "satellite",
            "sat_evento_digital_detalhes",
        )
        gold = event_history.groupBy("canal", "tipo_evento", "resultado").agg(
            F.count("*").alias("quantidade_eventos")
        ).withColumn(
            "percentual_no_canal",
            F.round(
                F.col("quantidade_eventos")
                / F.sum("quantidade_eventos").over(Window.partitionBy("canal"))
                * 100,
                2,
            ),
        )
        return _write_gold(
            _with_gold_metadata(gold, batch_id),
            "gold_eventos_digitais_por_canal",
            gold_path,
        )

    @staticmethod
    def create_gold_contas_por_agencia(
        spark: SparkSession,
        raw_vault_path: str,
        gold_path: str,
        batch_id: str,
    ) -> Dict[str, Any]:
        accounts = RawBusinessViews.accounts(spark, raw_vault_path) \
            .withColumn("saldo_decimal", F.col("saldo").cast("double")) \
            .drop("agencia_id")
        agencies = RawBusinessViews.agencies(spark, raw_vault_path).select(
            "hk_agencia",
            "agencia_id",
            "numero_agencia",
            F.col("nome").alias("nome_agencia"),
            "cidade",
            "estado",
        )
        account_agency = read_required_raw_table(
            spark, raw_vault_path, "link", "link_conta_agencia"
        ).select("hk_conta", "hk_agencia")
        gold = accounts.join(
            account_agency, on="hk_conta", how="inner"
        ).join(
            agencies, on="hk_agencia", how="left"
        ).groupBy(
            "agencia_id",
            "numero_agencia",
            "nome_agencia",
            "cidade",
            "estado",
        ).agg(
            F.count("conta_id").alias("quantidade_contas"),
            F.sum("saldo_decimal").alias("saldo_total"),
            F.avg("saldo_decimal").alias("saldo_medio"),
        )
        return _write_gold(
            _with_gold_metadata(gold, batch_id),
            "gold_contas_por_agencia",
            gold_path,
        )

    @staticmethod
    def create_gold_risco_transacional_simplificado(
        spark: SparkSession,
        raw_vault_path: str,
        gold_path: str,
        batch_id: str,
    ) -> Dict[str, Any]:
        transactions = RawBusinessViews.transactions(spark, raw_vault_path) \
            .withColumn("valor_decimal", F.col("valor").cast("double")) \
            .drop("conta_id")
        accounts = RawBusinessViews.accounts(spark, raw_vault_path) \
            .withColumn("limite_decimal", F.col("limite").cast("double")) \
            .select("hk_conta", "conta_id", "limite_decimal")
        transaction_account = read_required_raw_table(
            spark, raw_vault_path, "link", "link_conta_transacao"
        ).select("hk_conta", "hk_transacao")
        gold = transactions.join(
            transaction_account, on="hk_transacao", how="inner"
        ).join(
            accounts, on="hk_conta", how="left"
        ).withColumn(
            "acima_limite",
            F.when(F.col("valor_decimal") > F.col("limite_decimal"), 1).otherwise(0),
        ).groupBy(
            "conta_id",
            F.to_date("data_transacao").alias("data"),
        ).agg(
            F.count("transacao_id").alias("quantidade_transacoes"),
            F.sum("acima_limite").alias("transacoes_acima_limite"),
            F.sum("valor_decimal").alias("valor_total"),
        ).withColumn(
            "score_risco",
            F.round(
                F.col("transacoes_acima_limite")
                / F.col("quantidade_transacoes")
                * 100,
                2,
            ),
        ).withColumn(
            "nivel_risco",
            F.when(F.col("score_risco") > 50, "Alto")
            .when(F.col("score_risco") > 20, "Medio")
            .otherwise("Baixo"),
        ).withColumn(
            "conta_id_pseudonimizada",
            _pseudonymize_column(F.col("conta_id"), prefix="ACC"),
        ).drop("conta_id")
        return _write_gold(
            _with_gold_metadata(gold, batch_id),
            "gold_risco_transacional_simplificado",
            gold_path,
        )


def run_business_vault_pipeline(
    spark: SparkSession,
    raw_vault_path: str,
    gold_path: str,
    batch_id: str,
) -> Dict[str, Any]:
    logger.info("=" * 80)
    logger.info("INICIANDO MATERIALIZAÇÃO GOLD A PARTIR DA BUSINESS VAULT LÓGICA")
    logger.info("=" * 80)
    metrics = ExecutionMetrics(
        "gold_materialization_pipeline",
        "load_all_gold_tables",
        batch_id=batch_id,
    )
    builders = [
        GoldLayerBuilder.create_gold_transacoes_por_dia,
        GoldLayerBuilder.create_gold_transacoes_por_cliente,
        GoldLayerBuilder.create_gold_clientes_protegidos,
        GoldLayerBuilder.create_gold_volume_por_produto,
        GoldLayerBuilder.create_gold_eventos_digitais_por_canal,
        GoldLayerBuilder.create_gold_contas_por_agencia,
        GoldLayerBuilder.create_gold_risco_transacional_simplificado,
    ]
    results: Dict[str, Dict[str, Any]] = {}
    try:
        for builder in builders:
            result = builder(spark, raw_vault_path, gold_path, batch_id)
            results[result["table"]] = result
            metrics.record_rows_written(result["rows_written"])
        total_rows = sum(item["rows_written"] for item in results.values())
        metrics.record_success()
        return {
            "status": "SUCCESS",
            "results": results,
            "total_rows": total_rows,
            "batch_id": batch_id,
            "lineage": "raw_vault->business_vault_latest->gold",
        }
    except Exception as exc:
        logger.exception("Erro na materialização Gold a partir da Business Vault lógica")
        metrics.record_error(exc)
        return {"status": "FAILURE", "error": str(exc), "batch_id": batch_id}
    finally:
        metrics.finalize(spark)


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="Materialização Gold a partir da Business Vault lógica"
    )
    parser.add_argument(
        "--raw-vault-path",
        type=str,
        default=Config.RAW_VAULT_PATH,
    )
    parser.add_argument(
        "--gold-path",
        type=str,
        default=Config.GOLD_PATH,
    )
    parser.add_argument("--batch-id", type=str, default=None)
    args = parser.parse_args()
    spark = create_spark_session()
    batch_id = args.batch_id or MonitoringLogger.get_batch_id()
    result = run_business_vault_pipeline(
        spark,
        args.raw_vault_path,
        args.gold_path,
        batch_id,
    )
    return 0 if result["status"] == "SUCCESS" else 1


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    sys.exit(main())
