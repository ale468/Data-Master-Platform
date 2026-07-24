"""
Operações de I/O com Delta Lake.
Fornece abstrações para leitura e escrita eficiente em Delta Lake.
"""
from typing import Optional, List, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import logging
from datetime import datetime

from config import Config

logger = logging.getLogger(__name__)


class DeltaIO:
    """Utilitários para operações com Delta Lake."""
    
    @staticmethod
    def create_table_if_not_exists(
        spark: SparkSession,
        path: str,
        df: DataFrame,
        mode: str = "error",
        partition_cols: Optional[List[str]] = None
    ) -> None:
        """
        Cria tabela Delta se não existir, caso contrário valida schema.
        
        Args:
            spark: SparkSession
            path: Caminho da tabela Delta
            df: DataFrame com schema
            mode: Modo de criação ("error", "ignore")
            partition_cols: Colunas para particionar
        """
        try:
            # Verificar se tabela existe
            delta_table = DeltaTable.forPath(spark, path)
            logger.info(f"Tabela Delta já existe em {path}")
            
            # Validar schema se modo é 'error'
            if mode == "error":
                existing_schema = delta_table.toDF().schema
                new_schema = df.schema
                if existing_schema != new_schema:
                    logger.warning(
                        f"Schema mismatch para {path}. "
                        f"Esperado: {new_schema}, "
                        f"Existente: {existing_schema}"
                    )
        
        except Exception as e:
            logger.info(f"Tabela não existe em {path}, criando...")
            
            # Criar a tabela somente com schema; a carga real acontece no metodo
            # de escrita chamado pelo pipeline.
            writer = df.limit(0).write.format("delta").mode(mode)
            
            if partition_cols:
                writer = writer.partitionBy(partition_cols)
            
            writer.save(path)
            logger.info(f"Tabela Delta criada em {path}")
    
    @staticmethod
    def read_delta(spark: SparkSession, path: str) -> Optional[DataFrame]:
        """
        Lê tabela Delta.
        
        Args:
            spark: SparkSession
            path: Caminho da tabela
            
        Returns:
            DataFrame ou None se não existir
        """
        try:
            return spark.read.format("delta").load(path)
        except Exception as e:
            logger.warning(f"Erro ao ler Delta em {path}: {str(e)}")
            return None
    
    @staticmethod
    def write_delta_append(
        df: DataFrame,
        path: str,
        partition_cols: Optional[List[str]] = None
    ) -> None:
        """
        Adiciona dados a tabela Delta (append).
        
        Args:
            df: DataFrame para adicionar
            path: Caminho da tabela
            partition_cols: Colunas para particionar
        """
        writer = df.write.format("delta").mode("append")
        
        if partition_cols:
            writer = writer.partitionBy(partition_cols)
        
        writer.save(path)
        logger.info(f"Dados adicionados à tabela Delta em {path}")
    
    @staticmethod
    def write_delta_overwrite(
        df: DataFrame,
        path: str,
        partition_cols: Optional[List[str]] = None
    ) -> None:
        """
        Sobrescreve tabela Delta.
        
        Args:
            df: DataFrame para sobrescrever
            path: Caminho da tabela
            partition_cols: Colunas para particionar
        """
        writer = df.write.format("delta").mode("overwrite")
        
        if partition_cols:
            writer = writer.partitionBy(partition_cols)
        
        writer.save(path)
        logger.info(f"Tabela Delta sobrescrita em {path}")
    
    @staticmethod
    def write_delta_merge(
        spark: SparkSession,
        df: DataFrame,
        path: str,
        merge_keys: List[str],
        merge_condition: Optional[str] = None
    ) -> None:
        """
        Faz merge (upsert) em tabela Delta.
        
        Args:
            spark: SparkSession
            df: DataFrame com dados para merge
            path: Caminho da tabela
            merge_keys: Colunas chave para merge
            merge_condition: Condição SQL customizada (optional)
        """
        try:
            delta_table = DeltaTable.forPath(spark, path)
            
            # Construir condição de merge padrão
            if merge_condition is None:
                conditions = []
                for key in merge_keys:
                    conditions.append(f"t.{key} = s.{key}")
                merge_condition = " AND ".join(conditions)
            
            # Executar merge
            delta_table.alias("t") \
                .merge(df.alias("s"), merge_condition) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
            
            logger.info(f"Merge executado em {path} com sucesso")
        
        except Exception as e:
            logger.error(f"Erro ao fazer merge em {path}: {str(e)}")
            raise
    
    @staticmethod
    def get_table_stats(spark: SparkSession, path: str) -> Dict[str, Any]:
        """
        Retorna estatísticas de uma tabela Delta.
        
        Args:
            spark: SparkSession
            path: Caminho da tabela
            
        Returns:
            Dict com estatísticas (num_rows, num_files, size_mb, etc)
        """
        try:
            df = DeltaIO.read_delta(spark, path)
            
            if df is None:
                return {}
            
            num_rows = df.count()
            
            stats = {
                "path": path,
                "num_rows": num_rows,
                "num_columns": len(df.columns),
                "columns": df.columns,
                "read_timestamp": datetime.now().isoformat()
            }
            
            return stats
        
        except Exception as e:
            logger.warning(f"Erro ao obter stats de {path}: {str(e)}")
            return {}
    
    @staticmethod
    def optimize_delta_table(spark: SparkSession, path: str) -> None:
        """
        Otimiza tabela Delta (compacta arquivos pequenos).
        
        Args:
            spark: SparkSession
            path: Caminho da tabela
        """
        try:
            delta_table = DeltaTable.forPath(spark, path)
            delta_table.optimize().executeCompaction()
            logger.info(f"Tabela Delta otimizada em {path}")
        
        except Exception as e:
            logger.warning(f"Erro ao otimizar Delta em {path}: {str(e)}")
    
    @staticmethod
    def vacuum_delta_table(spark: SparkSession, path: str, retention_hours: int = 168) -> None:
        """
        Remove arquivos antigos da tabela Delta (limpeza).
        
        Args:
            spark: SparkSession
            path: Caminho da tabela
            retention_hours: Horas para reter (default: 1 semana)
        """
        try:
            spark.sql(f"VACUUM delta.`{path}` RETAIN {retention_hours} HOURS")
            logger.info(f"Vacuum executado em {path}")
        
        except Exception as e:
            logger.warning(f"Erro ao fazer vacuum em {path}: {str(e)}")


class DeltaVaultLoader:
    """Carregador especializado para Data Vault em Delta."""
    
    @staticmethod
    def load_hub(
        spark: SparkSession,
        df: DataFrame,
        hub_name: str,
        business_key_cols: List[str],
        hash_key_col: str = "hk_"
    ) -> None:
        """
        Carrega dados em Hub de Data Vault.
        Implementa inserção sem duplicatas.
        
        Args:
            spark: SparkSession
            df: DataFrame com dados
            hub_name: Nome do Hub
            business_key_cols: Colunas da business key
            hash_key_col: Nome da coluna hash key
        """
        from hashing import BusinessKeyHasher
        
        config = Config.get_hub_table_config(hub_name)
        path = config.get("path")
        
        if not path:
            raise ValueError(f"Configuração não encontrada para hub: {hub_name}")
        
        # Adicionar hash key
        df_with_hk = BusinessKeyHasher.spark_generate_hub_hash_key(
            spark, df, business_key_cols, output_col=hash_key_col
        )
        
        # Adicionar colunas técnicas
        load_datetime = F.current_timestamp()
        df_with_hk = df_with_hk \
            .withColumn("load_datetime", load_datetime) \
            .withColumn("record_source", F.lit(Config.DEFAULT_RECORD_SOURCE))
        
        # Selecionar apenas hash key e colunas técnicas
        df_hub = df_with_hk.select(
            hash_key_col,
            "load_datetime",
            "record_source",
            *business_key_cols
        )
        
        # Eliminar duplicatas em hash key
        df_hub = df_hub.dropDuplicates([hash_key_col])
        
        # Merge no Hub (evita duplicatas)
        try:
            DeltaIO.write_delta_merge(
                spark, df_hub, path,
                merge_keys=[hash_key_col]
            )
        except:
            # Se tabela não existe, criar
            DeltaIO.create_table_if_not_exists(spark, path, df_hub)
            DeltaIO.write_delta_append(df_hub, path)
    
    @staticmethod
    def load_satellite(
        spark: SparkSession,
        df: DataFrame,
        satellite_name: str,
        hash_key_col: str = "hk_",
        hash_diff_col: str = "hd_",
        track_columns: Optional[List[str]] = None
    ) -> None:
        """
        Carrega dados em Satellite de Data Vault.
        Implementa SCD Tipo 2 com hashdiff.
        
        Args:
            spark: SparkSession
            df: DataFrame com dados
            satellite_name: Nome do Satellite
            hash_key_col: Nome da coluna hash key
            hash_diff_col: Nome da coluna hash diff
            track_columns: Colunas a rastrear mudanças
        """
        from hashing import HashingUtils
        
        config = Config.get_satellite_table_config(satellite_name)
        path = config.get("path")
        
        if not path:
            raise ValueError(f"Configuração não encontrada para satellite: {satellite_name}")
        
        # Calcular hashdiff para colunas de rastreamento
        if track_columns:
            cols_to_hash = [F.col(c).cast("string") for c in track_columns]
            hash_diff_udf = F.udf(
                lambda *args: HashingUtils.calculate_hash(list(args), prefix="hd_"),
                "string"
            )
            df = df.withColumn(hash_diff_col, hash_diff_udf(*cols_to_hash))
        
        # Adicionar colunas técnicas
        load_datetime = F.current_timestamp()
        df_sat = df \
            .withColumn("load_datetime", load_datetime) \
            .withColumn("record_source", F.lit(Config.DEFAULT_RECORD_SOURCE)) \
            .withColumn("effective_from", load_datetime)
        
        # Verificar se há mudanças (hashdiff diferente)
        try:
            existing_df = DeltaIO.read_delta(spark, path)
            
            if existing_df is not None:
                # Apenas inserir se hashdiff for diferente
                df_sat = df_sat.join(
                    existing_df.select(hash_key_col, hash_diff_col),
                    on=hash_key_col,
                    how="left_anti"
                )
        
        except:
            pass
        
        # Append no Satellite
        if df_sat.count() > 0:
            try:
                DeltaIO.create_table_if_not_exists(spark, path, df_sat)
                DeltaIO.write_delta_append(df_sat, path)
            except:
                DeltaIO.write_delta_append(df_sat, path)


if __name__ == "__main__":
    # Importar para teste
    from spark_session import create_spark_session
    
    spark = create_spark_session()
    
    # Teste básico de stats
    print("Módulo Delta IO carregado com sucesso")
    print(f"Bucket: {Config.LAKEHOUSE_BUCKET}")
    print(f"Bronze Path: {Config.BRONZE_PATH}")
    print(f"Raw Vault Path: {Config.RAW_VAULT_PATH}")
    print(f"Business Vault Path: {Config.BUSINESS_VAULT_PATH}")
    print(f"Gold Path: {Config.GOLD_PATH}")
