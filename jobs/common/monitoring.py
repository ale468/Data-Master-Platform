"""
Monitoramento e logging estruturado para pipeline.
Registra execução de tasks em tabela Delta de observabilidade.
"""
from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from datetime import datetime
import uuid
import logging

from config import Config

logger = logging.getLogger(__name__)


class MonitoringLogger:
    """Logger de execução para pipeline de Data Vault."""
    
    @staticmethod
    def get_batch_id() -> str:
        """Gera ID único para lote de execução."""
        return str(uuid.uuid4())
    
    @staticmethod
    def log_pipeline_execution(
        spark: SparkSession,
        pipeline_name: str,
        task_name: str,
        batch_id: str,
        status: str,
        rows_read: int = 0,
        rows_written: int = 0,
        duration_seconds: float = 0,
        error_message: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None
    ) -> None:
        """
        Registra execução de pipeline/task em tabela de monitoramento.
        
        Args:
            spark: SparkSession
            pipeline_name: Nome do pipeline
            task_name: Nome da task
            batch_id: ID do lote
            status: Status (SUCCESS, FAILURE, RUNNING)
            rows_read: Número de linhas lidas
            rows_written: Número de linhas escritas
            duration_seconds: Duração em segundos
            error_message: Mensagem de erro (se houver)
            start_time: Hora de início (ISO format)
            end_time: Hora de fim (ISO format)
        """
        try:
            # Preparar dados
            execution_log = {
                "pipeline_name": pipeline_name,
                "task_name": task_name,
                "batch_id": batch_id,
                "start_time": start_time or datetime.now().isoformat(),
                "end_time": end_time or datetime.now().isoformat(),
                "duration_seconds": duration_seconds,
                "status": status,
                "rows_read": rows_read,
                "rows_written": rows_written,
                "error_message": error_message,
                "load_datetime": datetime.now().isoformat()
            }
            
            schema = StructType([
                StructField("pipeline_name", StringType(), True),
                StructField("task_name", StringType(), True),
                StructField("batch_id", StringType(), True),
                StructField("start_time", StringType(), True),
                StructField("end_time", StringType(), True),
                StructField("duration_seconds", DoubleType(), True),
                StructField("status", StringType(), True),
                StructField("rows_read", LongType(), True),
                StructField("rows_written", LongType(), True),
                StructField("error_message", StringType(), True),
                StructField("load_datetime", StringType(), True),
            ])

            # Criar DataFrame com schema explícito para aceitar error_message nulo.
            df_log = spark.createDataFrame([execution_log], schema=schema)
            
            # Escrever em tabela de monitoramento
            from delta_io import DeltaIO
            
            # Criar tabela se não existir
            try:
                DeltaIO.create_table_if_not_exists(
                    spark, Config.MONITORING_TABLE, df_log
                )
                DeltaIO.write_delta_append(df_log, Config.MONITORING_TABLE)
            except:
                # Fallback: append direto
                df_log.write.format("delta").mode("append").save(Config.MONITORING_TABLE)
            
            logger.info(
                f"Execução registrada: {pipeline_name}.{task_name} "
                f"[{status}] - {rows_written} linhas escritas"
            )
        
        except Exception as e:
            logger.error(f"Erro ao registrar execução: {str(e)}")
    
    @staticmethod
    def get_execution_summary(
        spark: SparkSession,
        batch_id: str
    ) -> Optional[DataFrame]:
        """
        Retorna resumo de execução para um lote.
        
        Args:
            spark: SparkSession
            batch_id: ID do lote
            
        Returns:
            DataFrame com resumo de execução
        """
        try:
            from delta_io import DeltaIO
            
            df_monitoring = DeltaIO.read_delta(spark, Config.MONITORING_TABLE)
            
            if df_monitoring is None:
                return None
            
            return df_monitoring.filter(F.col("batch_id") == batch_id)
        
        except Exception as e:
            logger.error(f"Erro ao obter resumo de execução: {str(e)}")
            return None
    
    @staticmethod
    def print_execution_report(
        spark: SparkSession,
        batch_id: str
    ) -> None:
        """
        Imprime relatório de execução de um lote.
        
        Args:
            spark: SparkSession
            batch_id: ID do lote
        """
        try:
            df_summary = MonitoringLogger.get_execution_summary(spark, batch_id)
            
            if df_summary is None:
                logger.info(f"Nenhuma execução encontrada para batch_id: {batch_id}")
                return
            
            print(f"\n{'='*80}")
            print(f"RELATÓRIO DE EXECUÇÃO - BATCH: {batch_id}")
            print(f"{'='*80}\n")
            
            # Converter para Pandas para melhor visualização
            summary_pd = df_summary.select(
                "pipeline_name",
                "task_name",
                "status",
                "rows_read",
                "rows_written",
                "duration_seconds",
                "start_time",
                "end_time",
                "error_message"
            ).toPandas()
            
            print(summary_pd.to_string(index=False))
            
            # Resumo por status
            print(f"\n{'='*80}\nRESUMO POR STATUS\n{'='*80}\n")
            
            status_summary = df_summary.groupBy("status").agg(
                F.count("*").alias("count"),
                F.sum("rows_read").alias("total_rows_read"),
                F.sum("rows_written").alias("total_rows_written"),
                F.sum("duration_seconds").alias("total_duration_seconds")
            )
            
            status_pd = status_summary.toPandas()
            print(status_pd.to_string(index=False))
            
            print(f"\n{'='*80}\n")
        
        except Exception as e:
            logger.error(f"Erro ao imprimir relatório: {str(e)}")


class DataQualityLogger:
    """Logger para validações de qualidade de dados."""
    
    @staticmethod
    def log_validation(
        spark: SparkSession,
        validation_name: str,
        table_name: str,
        validation_type: str,  # "row_count", "null_check", "duplicate_check", "key_check"
        passed: bool,
        expected_value: Any,
        actual_value: Any,
        batch_id: str
    ) -> None:
        """
        Registra resultado de validação de qualidade.
        
        Args:
            spark: SparkSession
            validation_name: Nome da validação
            table_name: Nome da tabela validada
            validation_type: Tipo de validação
            passed: Se a validação passou
            expected_value: Valor esperado
            actual_value: Valor obtido
            batch_id: ID do lote
        """
        try:
            validation_log = {
                "validation_name": validation_name,
                "table_name": table_name,
                "validation_type": validation_type,
                "passed": passed,
                "expected_value": str(expected_value),
                "actual_value": str(actual_value),
                "batch_id": batch_id,
                "validation_timestamp": datetime.now().isoformat()
            }
            
            logger.info(
                f"Validação: {validation_name} [{table_name}] - "
                f"Tipo: {validation_type} - Resultado: {'PASSOU' if passed else 'FALHOU'}"
            )
        
        except Exception as e:
            logger.error(f"Erro ao registrar validação: {str(e)}")


class ExecutionMetrics:
    """Coletador de métricas de execução."""
    
    def __init__(self, pipeline_name: str, task_name: str, batch_id: Optional[str] = None):
        """Inicializa coletor de métricas."""
        self.pipeline_name = pipeline_name
        self.task_name = task_name
        self.batch_id = batch_id or MonitoringLogger.get_batch_id()
        self.start_time = datetime.now()
        self.start_time_str = self.start_time.isoformat()
        self.rows_read = 0
        self.rows_written = 0
        self.status = "RUNNING"
        self.error_message = None
    
    def record_rows_read(self, count: int) -> None:
        """Registra número de linhas lidas."""
        self.rows_read += count
    
    def record_rows_written(self, count: int) -> None:
        """Registra número de linhas escritas."""
        self.rows_written += count
    
    def record_error(self, error: Exception) -> None:
        """Registra erro de execução."""
        self.status = "FAILURE"
        self.error_message = str(error)
        logger.error(f"Erro em {self.task_name}: {self.error_message}")
    
    def record_success(self) -> None:
        """Marca execução como bem-sucedida."""
        self.status = "SUCCESS"
    
    def finalize(self, spark: SparkSession) -> None:
        """Finaliza coleta e registra métricas."""
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        
        MonitoringLogger.log_pipeline_execution(
            spark,
            self.pipeline_name,
            self.task_name,
            self.batch_id,
            self.status,
            rows_read=self.rows_read,
            rows_written=self.rows_written,
            duration_seconds=duration,
            error_message=self.error_message,
            start_time=self.start_time_str,
            end_time=end_time.isoformat()
        )


def setup_logging(log_level: str = "INFO") -> None:
    """
    Configura logging para o pipeline.
    
    Args:
        log_level: Nível de logging (DEBUG, INFO, WARNING, ERROR)
    """
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('/tmp/data_vault_pipeline.log')
        ]
    )


if __name__ == "__main__":
    from spark_session import create_spark_session
    
    spark = create_spark_session()
    
    # Teste básico
    batch_id = MonitoringLogger.get_batch_id()
    print(f"Batch ID: {batch_id}")
    
    # Registrar execução de teste
    MonitoringLogger.log_pipeline_execution(
        spark,
        "test_pipeline",
        "test_task",
        batch_id,
        "SUCCESS",
        rows_read=100,
        rows_written=100,
        duration_seconds=5.0
    )
    
    # Imprimir relatório
    MonitoringLogger.print_execution_report(spark, batch_id)
