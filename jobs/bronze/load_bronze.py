"""
Pipeline de ingestão Bronze.
Carrega dados brutos de fontes (CSV, JSON) para Delta Lake em formato bruto,
preservando estrutura original e adicionando metadados técnicos.
"""
import sys
import os
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from datetime import datetime
import logging

# Adicionar path comum
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../common'))

from config import Config
from spark_session import create_spark_session, SparkUtils
from delta_io import DeltaIO
from monitoring import ExecutionMetrics
from validations import DataQualityValidations
from source_registry import (
    assert_bronze_metadata_columns,
    assert_required_columns,
    get_source_contract,
    list_registered_sources,
)

logger = logging.getLogger(__name__)


class BronzeLoader:
    """Carregador especializado para camada Bronze."""

    @staticmethod
    def validate_delta_read(
        spark: SparkSession,
        table_path: str,
        rows_written: int
    ) -> int:
        """Valida que a tabela Bronze foi gravada e pode ser lida em Delta."""
        df_read = DeltaIO.read_delta(spark, table_path)
        if df_read is None:
            raise RuntimeError(f"Tabela Delta não pode ser lida: {table_path}")

        rows_delta_read = df_read.count()
        if rows_delta_read < rows_written:
            raise RuntimeError(
                f"Leitura Delta inconsistente em {table_path}: "
                f"{rows_delta_read} linhas lidas para {rows_written} escritas."
            )

        return rows_delta_read
    
    @staticmethod
    def add_technical_columns(
        df: DataFrame,
        source_contract: Dict[str, Any],
        batch_id: str,
        source_file: str,
        source_record_count: int,
        load_datetime: Optional[str] = None
    ) -> DataFrame:
        """
        Adiciona colunas técnicas ao DataFrame.
        
        Args:
            df: DataFrame de origem
            record_source: Fonte de origem dos dados
            batch_id: ID do lote de carga
            source_file: Caminho do arquivo de origem
            load_datetime: Data/hora de carga (usa now se não informado)
            
        Returns:
            DataFrame com colunas técnicas adicionadas
        """
        if load_datetime is None:
            load_datetime = datetime.now().isoformat()
        
        # Adicionar colunas técnicas
        df = df \
            .withColumn("load_datetime", F.lit(load_datetime).cast("timestamp")) \
            .withColumn("record_source", F.lit(source_contract["record_source"])) \
            .withColumn("source_system", F.lit(source_contract["source_system"])) \
            .withColumn("source_entity", F.lit(source_contract["source_entity"])) \
            .withColumn("ingestion_mode", F.lit(source_contract["ingestion_mode"])) \
            .withColumn("schema_version", F.lit(source_contract["schema_version"])) \
            .withColumn("batch_id", F.lit(batch_id)) \
            .withColumn("run_id", F.lit(batch_id)) \
            .withColumn("ingestion_date", F.lit(datetime.now().strftime("%Y-%m-%d"))) \
            .withColumn("source_file", F.lit(source_file)) \
            .withColumn("source_record_count", F.lit(source_record_count).cast("long"))
        
        return df
    
    @staticmethod
    def load_bronze_from_csv(
        spark: SparkSession,
        file_path: str,
        table_name: str,
        bronze_path: str,
        batch_id: str,
        **csv_options
    ) -> Dict[str, Any]:
        """
        Carrega arquivo CSV para Bronze em Delta.
        
        Args:
            spark: SparkSession
            file_path: Caminho do arquivo CSV
            table_name: Nome da tabela
            bronze_path: Caminho base da camada Bronze
            batch_id: ID do lote
            **csv_options: Opções adicionais para read.csv()
            
        Returns:
            Dict com estatísticas de carga
        """
        logger.info(f"Carregando CSV {file_path} para Bronze ({table_name})")
        
        # Valores padrão
        csv_options.setdefault("header", True)
        csv_options.setdefault("encoding", "UTF-8")
        
        try:
            source_contract = get_source_contract(table_name)
            if source_contract["format"] != "csv":
                raise ValueError(
                    f"Fonte '{table_name}' registrada como {source_contract['format']}, "
                    "mas chamada como CSV."
                )

            # Ler CSV
            df = spark.read.csv(file_path, **csv_options)
            assert_required_columns(table_name, df.columns)
            rows_read = df.count()
            
            # Adicionar colunas técnicas
            df_bronze = BronzeLoader.add_technical_columns(
                df,
                source_contract=source_contract,
                batch_id=batch_id,
                source_file=file_path,
                source_record_count=rows_read
            )
            assert_bronze_metadata_columns(df_bronze.columns)
            
            # Definir caminho da tabela
            table_path = f"{bronze_path}/{table_name}"
            
            # Escrever em Delta
            rows_written = df_bronze.count()
            
            DeltaIO.create_table_if_not_exists(spark, table_path, df_bronze)
            DeltaIO.write_delta_append(df_bronze, table_path)
            
            # Validações
            rows_delta_read = BronzeLoader.validate_delta_read(
                spark,
                table_path,
                rows_written
            )
            stats = {
                "table_name": table_name,
                "source_id": source_contract["source_id"],
                "source_system": source_contract["source_system"],
                "source_entity": source_contract["source_entity"],
                "schema_version": source_contract["schema_version"],
                "source_type": "csv",
                "rows_read": rows_read,
                "rows_written": rows_written,
                "rows_delta_read": rows_delta_read,
                "columns": len(df_bronze.columns),
                "technical_columns": Config.TECHNICAL_COLUMNS,
                "bronze_path": table_path,
                "status": "SUCCESS"
            }
            
            logger.info(f"✓ Tabela Bronze '{table_name}' carregada com sucesso")
            logger.info(f"  Linhas: {rows_written} | Colunas: {stats['columns']}")
            
            return stats
        
        except Exception as e:
            logger.error(f"✗ Erro ao carregar CSV {table_name}: {str(e)}")
            return {
                "table_name": table_name,
                "source_type": "csv",
                "rows_read": 0,
                "rows_written": 0,
                "columns": 0,
                "bronze_path": f"{bronze_path}/{table_name}",
                "status": "FAILURE",
                "error": str(e)
            }
    
    @staticmethod
    def load_bronze_from_json(
        spark: SparkSession,
        file_path: str,
        table_name: str,
        bronze_path: str,
        batch_id: str,
        **json_options
    ) -> Dict[str, Any]:
        """
        Carrega arquivo JSON para Bronze em Delta.
        
        Args:
            spark: SparkSession
            file_path: Caminho do arquivo JSON
            table_name: Nome da tabela
            bronze_path: Caminho base da camada Bronze
            batch_id: ID do lote
            **json_options: Opções adicionais para read.json()
            
        Returns:
            Dict com estatísticas de carga
        """
        logger.info(f"Carregando JSON {file_path} para Bronze ({table_name})")
        
        # Valores padrão
        json_options.setdefault("encoding", "UTF-8")
        json_options.setdefault("multiLine", True)
        
        try:
            source_contract = get_source_contract(table_name)
            if source_contract["format"] != "json":
                raise ValueError(
                    f"Fonte '{table_name}' registrada como {source_contract['format']}, "
                    "mas chamada como JSON."
                )

            # Ler JSON
            df = spark.read.json(file_path, **json_options)
            assert_required_columns(table_name, df.columns)
            rows_read = df.count()
            
            # Adicionar colunas técnicas
            df_bronze = BronzeLoader.add_technical_columns(
                df,
                source_contract=source_contract,
                batch_id=batch_id,
                source_file=file_path,
                source_record_count=rows_read
            )
            assert_bronze_metadata_columns(df_bronze.columns)
            
            # Definir caminho da tabela
            table_path = f"{bronze_path}/{table_name}"
            
            # Escrever em Delta
            rows_written = df_bronze.count()
            
            DeltaIO.create_table_if_not_exists(spark, table_path, df_bronze)
            DeltaIO.write_delta_append(df_bronze, table_path)
            
            # Validações
            rows_delta_read = BronzeLoader.validate_delta_read(
                spark,
                table_path,
                rows_written
            )
            stats = {
                "table_name": table_name,
                "source_id": source_contract["source_id"],
                "source_system": source_contract["source_system"],
                "source_entity": source_contract["source_entity"],
                "schema_version": source_contract["schema_version"],
                "source_type": "json",
                "rows_read": rows_read,
                "rows_written": rows_written,
                "rows_delta_read": rows_delta_read,
                "columns": len(df_bronze.columns),
                "technical_columns": Config.TECHNICAL_COLUMNS,
                "bronze_path": table_path,
                "status": "SUCCESS"
            }
            
            logger.info(f"✓ Tabela Bronze '{table_name}' carregada com sucesso")
            logger.info(f"  Linhas: {rows_written} | Colunas: {stats['columns']}")
            
            return stats
        
        except Exception as e:
            logger.error(f"✗ Erro ao carregar JSON {table_name}: {str(e)}")
            return {
                "table_name": table_name,
                "source_type": "json",
                "rows_read": 0,
                "rows_written": 0,
                "columns": 0,
                "bronze_path": f"{bronze_path}/{table_name}",
                "status": "FAILURE",
                "error": str(e)
            }


def run_bronze_pipeline(
    spark: SparkSession,
    sample_data_path: str,
    bronze_path: str,
    batch_id: str
) -> Dict[str, Any]:
    """
    Executa pipeline completo de ingestão Bronze.
    
    Args:
        spark: SparkSession
        sample_data_path: Caminho dos dados de amostra
        bronze_path: Caminho base Bronze
        batch_id: ID do lote
        
    Returns:
        Dict com resumo de execução
    """
    logger.info("="*80)
    logger.info("INICIANDO PIPELINE BRONZE")
    logger.info("="*80)
    
    metrics = ExecutionMetrics("bronze_pipeline", "load_all_bronze_tables", batch_id=batch_id)
    
    results = {source_name: None for source_name in list_registered_sources("batch")}
    
    try:
        logger.info("\n--- Carregando fontes registradas ---\n")

        for source_name in list_registered_sources("batch"):
            source_contract = get_source_contract(source_name)
            source_path = os.path.join(sample_data_path, source_contract["file_name"])

            if source_contract["format"] == "csv":
                results[source_name] = BronzeLoader.load_bronze_from_csv(
                    spark,
                    source_path,
                    source_name,
                    bronze_path,
                    batch_id
                )
            elif source_contract["format"] == "json":
                results[source_name] = BronzeLoader.load_bronze_from_json(
                    spark,
                    source_path,
                    source_name,
                    bronze_path,
                    batch_id
                )
            else:
                raise ValueError(
                    f"Formato não suportado para fonte '{source_name}': "
                    f"{source_contract['format']}"
                )

            metrics.record_rows_written(results[source_name]["rows_written"])
        
        metrics.record_success()
        
        # Resumo
        logger.info("\n" + "="*80)
        logger.info("RESUMO DE CARGAS BRONZE")
        logger.info("="*80 + "\n")
        
        total_rows = 0
        failed_tables = []
        for table_name, result in results.items():
            if result:
                status = result.get("status", "UNKNOWN")
                rows = result.get("rows_written", 0)
                total_rows += rows
                if status != "SUCCESS":
                    failed_tables.append(table_name)
                marker = "OK" if status == "SUCCESS" else "ERRO"
                print(f"{marker:4} {table_name:20} | {rows:8,} linhas | {status}")
        
        print(f"\n{'='*80}")
        print(f"TOTAL DE LINHAS CARREGADAS: {total_rows:,}")
        print(f"{'='*80}\n")
        
        if failed_tables:
            raise RuntimeError(
                "Falha ao carregar tabelas Bronze: " + ", ".join(failed_tables)
            )

        return {
            "status": "SUCCESS",
            "results": results,
            "total_rows": total_rows,
            "batch_id": batch_id
        }
    
    except Exception as e:
        logger.error(f"Erro no pipeline Bronze: {str(e)}")
        metrics.record_error(e)
        return {
            "status": "FAILURE",
            "error": str(e),
            "batch_id": batch_id
        }
    
    finally:
        metrics.finalize(spark)


def main():
    """Função main para execução como script."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Pipeline de ingestão Bronze")
    parser.add_argument("--sample-data-path", type=str, 
                       default=Config.SAMPLE_DATA_PATH,
                       help="Caminho dos dados de amostra")
    parser.add_argument("--bronze-path", type=str,
                       default=Config.BRONZE_PATH,
                       help="Caminho base da camada Bronze")
    parser.add_argument("--batch-id", type=str,
                       default=None,
                       help="ID do lote (gera automaticamente se não informado)")
    
    args = parser.parse_args()
    
    # Criar sessão Spark
    spark = create_spark_session()
    
    # Gerar batch_id se não informado
    if args.batch_id is None:
        from monitoring import MonitoringLogger
        args.batch_id = MonitoringLogger.get_batch_id()
    
    # Executar pipeline
    result = run_bronze_pipeline(
        spark,
        args.sample_data_path,
        args.bronze_path,
        args.batch_id
    )
    
    # Retornar código de saída
    return 0 if result["status"] == "SUCCESS" else 1


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    exit_code = main()
    sys.exit(exit_code)
