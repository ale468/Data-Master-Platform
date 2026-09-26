"""
Pipeline de ingestão Bronze.
Carrega dados brutos de fontes (CSV, JSON) para Delta Lake em formato bruto,
preservando estrutura original e adicionando metadados técnicos.
"""
import sys
import os
import hashlib
import json
from typing import Dict, Any, List, Optional
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
    def dataframe_from_driver_records(
        spark: SparkSession,
        records: List[Dict[str, Any]],
        source_format: str,
    ) -> DataFrame:
        """Create a distributed frame without executor access to driver files."""
        if not records:
            raise ValueError("Driver-memory Bronze source cannot be empty.")
        if source_format == "csv":
            return spark.createDataFrame([dict(record) for record in records])
        if source_format == "json":
            payloads = [
                json.dumps(record, sort_keys=True, ensure_ascii=False)
                for record in records
            ]
            return spark.read.json(spark.sparkContext.parallelize(payloads))
        raise ValueError(f"Unsupported driver-memory source format: {source_format}")

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
        run_id: Optional[str] = None,
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
        effective_run_id = run_id or batch_id
        
        # Adicionar colunas técnicas
        df = df \
            .withColumn("load_datetime", F.lit(load_datetime).cast("timestamp")) \
            .withColumn("record_source", F.lit(source_contract["record_source"])) \
            .withColumn("source_system", F.lit(source_contract["source_system"])) \
            .withColumn("source_entity", F.lit(source_contract["source_entity"])) \
            .withColumn("ingestion_mode", F.lit(source_contract["ingestion_mode"])) \
            .withColumn("schema_version", F.lit(source_contract["schema_version"])) \
            .withColumn("batch_id", F.lit(batch_id)) \
            .withColumn("run_id", F.lit(effective_run_id)) \
            .withColumn("ingestion_date", F.lit(datetime.now().strftime("%Y-%m-%d"))) \
            .withColumn("source_file", F.lit(source_file)) \
            .withColumn("source_record_count", F.lit(source_record_count).cast("long"))
        
        return df

    @staticmethod
    def filter_new_batch_records(
        spark: SparkSession,
        df: DataFrame,
        table_path: str,
        primary_key: list,
    ) -> DataFrame:
        """Return only rows not already stored for batch_id + primary key."""
        identity_columns = ["batch_id", *primary_key]
        missing = sorted(set(identity_columns) - set(df.columns))
        if missing:
            raise ValueError(
                "Bronze idempotency columns missing: " + ", ".join(missing)
            )

        invalid = None
        for column in primary_key:
            condition = F.col(column).isNull() | (
                F.trim(F.col(column).cast("string")) == ""
            )
            invalid = condition if invalid is None else invalid | condition
        if invalid is not None and df.filter(invalid).limit(1).count():
            raise ValueError(
                "Bronze primary key cannot be null or blank: "
                + ", ".join(primary_key)
            )

        payload_columns = sorted(
            column for column in df.columns
            if column not in Config.TECHNICAL_COLUMNS
        )
        row_hash = F.sha2(
            F.to_json(F.struct(*[F.col(column) for column in payload_columns])),
            256,
        )
        conflict = (
            df.withColumn("__payload_sha256", row_hash)
            .groupBy(*identity_columns)
            .agg(F.countDistinct("__payload_sha256").alias("payload_versions"))
            .filter(F.col("payload_versions") > 1)
            .limit(1)
            .count()
        )
        if conflict:
            raise ValueError(
                "Conflicting duplicate primary key inside source batch: "
                + ", ".join(identity_columns)
            )

        df = df.dropDuplicates(identity_columns)

        existing = DeltaIO.read_delta(spark, table_path)
        if existing is None:
            return df
        existing_identity = existing.select(*identity_columns).dropDuplicates()
        return df.join(existing_identity, on=identity_columns, how="left_anti")
    
    @staticmethod
    def load_bronze_from_csv(
        spark: SparkSession,
        file_path: str,
        table_name: str,
        bronze_path: str,
        batch_id: str,
        run_id: Optional[str] = None,
        source_records: Optional[List[Dict[str, Any]]] = None,
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

            # Dynamic lifecycle files exist only in the driver pod. Convert
            # their already parsed rows into a distributed frame so executors
            # never need access to the driver's temporary filesystem.
            if source_records is None:
                df = spark.read.csv(file_path, **csv_options)
            else:
                df = BronzeLoader.dataframe_from_driver_records(
                    spark,
                    source_records,
                    "csv",
                )
            assert_required_columns(table_name, df.columns)
            rows_read = df.count()
            
            # Adicionar colunas técnicas
            df_bronze = BronzeLoader.add_technical_columns(
                df,
                source_contract=source_contract,
                batch_id=batch_id,
                source_file=file_path,
                source_record_count=rows_read,
                run_id=run_id,
            )
            assert_bronze_metadata_columns(df_bronze.columns)
            
            # Definir caminho da tabela
            table_path = f"{bronze_path}/{table_name}"
            
            # Escrever em Delta
            df_bronze = BronzeLoader.filter_new_batch_records(
                spark,
                df_bronze,
                table_path,
                source_contract["primary_key"],
            )
            rows_written = df_bronze.count()
            
            DeltaIO.create_table_if_not_exists(spark, table_path, df_bronze)
            if rows_written:
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
                "rows_skipped_existing": rows_read - rows_written,
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
        run_id: Optional[str] = None,
        source_records: Optional[List[Dict[str, Any]]] = None,
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

            if source_records is None:
                df = spark.read.json(file_path, **json_options)
            else:
                df = BronzeLoader.dataframe_from_driver_records(
                    spark,
                    source_records,
                    "json",
                )
            assert_required_columns(table_name, df.columns)
            rows_read = df.count()
            
            # Adicionar colunas técnicas
            df_bronze = BronzeLoader.add_technical_columns(
                df,
                source_contract=source_contract,
                batch_id=batch_id,
                source_file=file_path,
                source_record_count=rows_read,
                run_id=run_id,
            )
            assert_bronze_metadata_columns(df_bronze.columns)
            
            # Definir caminho da tabela
            table_path = f"{bronze_path}/{table_name}"
            
            # Escrever em Delta
            df_bronze = BronzeLoader.filter_new_batch_records(
                spark,
                df_bronze,
                table_path,
                source_contract["primary_key"],
            )
            rows_written = df_bronze.count()
            
            DeltaIO.create_table_if_not_exists(spark, table_path, df_bronze)
            if rows_written:
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
                "rows_skipped_existing": rows_read - rows_written,
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


def register_batch_manifest(
    spark: SparkSession,
    manifest: Dict[str, Any],
    run_id: str,
) -> Dict[str, Any]:
    """Register an immutable source manifest or validate an exact replay."""
    batch_id = str(manifest.get("batch_id", "")).strip()
    if not batch_id:
        raise ValueError("Batch manifest requires a non-empty batch_id.")
    manifest_json = json.dumps(
        manifest,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    manifest_sha256 = hashlib.sha256(manifest_json.encode("utf-8")).hexdigest()
    path = Config.BRONZE_BATCH_MANIFEST_PATH
    existing = DeltaIO.read_delta(spark, path)
    if existing is not None:
        matches = existing.filter(F.col("batch_id") == batch_id).collect()
        if matches:
            observed = {row["manifest_sha256"] for row in matches}
            if observed != {manifest_sha256}:
                raise ValueError(
                    f"Batch manifest conflict for immutable batch_id: {batch_id}"
                )
            return {
                "status": "REPLAY",
                "batch_id": batch_id,
                "manifest_sha256": manifest_sha256,
            }

    row = {
        "scenario_id": str(manifest.get("scenario_id", "")),
        "batch_id": batch_id,
        "source_batch": str(manifest.get("source_batch", "")),
        "run_id": run_id,
        "generator_version": str(manifest.get("generator_version", "")),
        "generated_at": str(manifest.get("generated_at", "")),
        "manifest_sha256": manifest_sha256,
        "manifest_json": manifest_json,
    }
    frame = spark.createDataFrame([row])
    DeltaIO.create_table_if_not_exists(spark, path, frame)
    DeltaIO.write_delta_append(frame, path)
    return {
        "status": "REGISTERED",
        "batch_id": batch_id,
        "manifest_sha256": manifest_sha256,
    }


def run_bronze_pipeline(
    spark: SparkSession,
    sample_data_path: str,
    bronze_path: str,
    batch_id: str,
    run_id: Optional[str] = None,
    batch_manifest: Optional[Dict[str, Any]] = None,
    source_records: Optional[Dict[str, List[Dict[str, Any]]]] = None,
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
    
    effective_run_id = run_id or batch_id
    metrics = ExecutionMetrics(
        "bronze_pipeline",
        "load_all_bronze_tables",
        batch_id=batch_id,
        run_id=effective_run_id,
    )
    
    results = {source_name: None for source_name in list_registered_sources("batch")}
    
    try:
        registered_sources = list_registered_sources("batch")
        if source_records is not None:
            missing_sources = sorted(set(registered_sources) - set(source_records))
            unexpected_sources = sorted(set(source_records) - set(registered_sources))
            if missing_sources or unexpected_sources:
                raise ValueError(
                    "Driver-memory Bronze sources do not match registry: "
                    f"missing={missing_sources}, unexpected={unexpected_sources}"
                )

        manifest_result = None
        if batch_manifest is not None:
            if batch_manifest.get("batch_id") != batch_id:
                raise ValueError("Batch manifest batch_id does not match pipeline batch_id.")
            manifest_result = register_batch_manifest(
                spark,
                batch_manifest,
                effective_run_id,
            )

        logger.info("\n--- Carregando fontes registradas ---\n")

        for source_name in registered_sources:
            source_contract = get_source_contract(source_name)
            source_path = os.path.join(sample_data_path, source_contract["file_name"])
            records = (
                source_records[source_name]
                if source_records is not None
                else None
            )

            if source_contract["format"] == "csv":
                results[source_name] = BronzeLoader.load_bronze_from_csv(
                    spark,
                    source_path,
                    source_name,
                    bronze_path,
                    batch_id,
                    effective_run_id,
                    source_records=records,
                )
            elif source_contract["format"] == "json":
                results[source_name] = BronzeLoader.load_bronze_from_json(
                    spark,
                    source_path,
                    source_name,
                    bronze_path,
                    batch_id,
                    effective_run_id,
                    source_records=records,
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
            "batch_id": batch_id,
            "run_id": effective_run_id,
            "manifest": manifest_result,
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
    parser.add_argument("--run-id", type=str, default=None)
    
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
        args.batch_id,
        args.run_id,
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
