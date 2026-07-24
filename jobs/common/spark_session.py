"""
Factory para criar e configurar sessões Spark com Delta Lake e S3.
Garante consistência nas configurações Spark em todos os jobs.
"""
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery
import logging
import os

from config import Config

logger = logging.getLogger(__name__)


class SparkSessionFactory:
    """Factory para criar sessões Spark configuradas."""
    
    _session: Optional[SparkSession] = None
    
    @staticmethod
    def get_or_create(app_name: str = Config.SPARK_APP_NAME) -> SparkSession:
        """
        Obtém ou cria uma sessão Spark com configurações do projeto.
        
        Args:
            app_name: Nome da aplicação Spark
            
        Returns:
            SparkSession configurada
        """
        if SparkSessionFactory._session is not None:
            logger.info("Reutilizando sessão Spark existente")
            return SparkSessionFactory._session
        
        logger.info(f"Criando nova sessão Spark: {app_name}")
        
        builder = SparkSession.builder \
            .appName(app_name) \
            .master(Config.SPARK_MASTER)
        
        # Aplicar configurações Spark
        for key, value in Config.SPARK_CONFIGS.items():
            builder = builder.config(key, value)
        
        # Criar sessão
        spark = builder.getOrCreate()
        
        # Habilitar log SQL
        spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "INFO").upper())
        
        SparkSessionFactory._session = spark
        
        logger.info("Sessão Spark criada e configurada com sucesso")
        return spark
    
    @staticmethod
    def stop():
        """Para a sessão Spark atual."""
        if SparkSessionFactory._session is not None:
            logger.info("Parando sessão Spark")
            SparkSessionFactory._session.stop()
            SparkSessionFactory._session = None
    
    @staticmethod
    def get_active_session() -> Optional[SparkSession]:
        """Retorna a sessão Spark ativa, se existir."""
        return SparkSessionFactory._session


class SparkUtils:
    """Utilitários comuns para operações Spark."""
    
    @staticmethod
    def read_csv(spark: SparkSession, path: str, **kwargs) -> 'DataFrame':
        """
        Lê arquivo CSV com configurações padrão.
        
        Args:
            spark: SparkSession
            path: Caminho do arquivo
            **kwargs: Argumentos adicionais para read.csv()
            
        Returns:
            DataFrame
        """
        default_options = {
            "header": True,
            "inferSchema": False,  # Será especificado no schema
            "encoding": "UTF-8",
        }
        default_options.update(kwargs)
        
        return spark.read.csv(path, **default_options)
    
    @staticmethod
    def read_json(spark: SparkSession, path: str, **kwargs) -> 'DataFrame':
        """
        Lê arquivo JSON com configurações padrão.
        
        Args:
            spark: SparkSession
            path: Caminho do arquivo
            **kwargs: Argumentos adicionais para read.json()
            
        Returns:
            DataFrame
        """
        default_options = {
            "encoding": "UTF-8",
        }
        default_options.update(kwargs)
        
        return spark.read.json(path, **default_options)
    
    @staticmethod
    def read_delta(spark: SparkSession, path: str) -> 'DataFrame':
        """
        Lê tabela Delta.
        
        Args:
            spark: SparkSession
            path: Caminho da tabela Delta
            
        Returns:
            DataFrame
        """
        return spark.read.format("delta").load(path)
    
    @staticmethod
    def write_delta(df: 'DataFrame', path: str, mode: str = "overwrite", 
                   partition_cols: Optional[list] = None) -> None:
        """
        Escreve DataFrame em formato Delta.
        
        Args:
            df: DataFrame para escrever
            path: Caminho de destino
            mode: Modo de escrita (overwrite, append, ignore, error)
            partition_cols: Colunas para particionar (opcional)
        """
        writer = df.write.format("delta").mode(mode)
        
        if partition_cols:
            writer = writer.partitionBy(partition_cols)
        
        writer.save(path)
    
    @staticmethod
    def get_or_create_delta_table(spark: SparkSession, path: str) -> 'DeltaTable':
        """
        Obtém referência a uma tabela Delta, criando-a se não existir.
        
        Args:
            spark: SparkSession
            path: Caminho da tabela
            
        Returns:
            DeltaTable
        """
        from delta.tables import DeltaTable
        
        try:
            return DeltaTable.forPath(spark, path)
        except Exception as e:
            logger.warning(f"Tabela Delta não encontrada em {path}: {str(e)}")
            return None


def create_spark_session() -> SparkSession:
    """
    Função convenience para criar sessão Spark.
    Uso: spark = create_spark_session()
    """
    return SparkSessionFactory.get_or_create()


if __name__ == "__main__":
    # Teste básico
    spark = create_spark_session()
    print(f"Spark Session: {spark.version}")
    
    # Verificar configurações Delta
    print("\nConfiguracoes Delta Lake:")
    for key, value in Config.SPARK_CONFIGS.items():
        print(f"  {key}: {value}")
    
    SparkSessionFactory.stop()
