"""
Configurações centralizadas para o pipeline de Data Vault 2.0
Aplicável a todos os jobs do pipeline bancário.
"""
import os
from typing import Dict, Any

try:
    from .runtime_profiles import get_runtime_profile, list_runtime_profiles
except ImportError:
    from runtime_profiles import get_runtime_profile, list_runtime_profiles

class PipelineConfig:
    """Configuração centralizada do pipeline."""
    
    # ===== AMBIENTE E DIRETORIOS =====
    ENVIRONMENT = os.getenv("ENV", "local")
    PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/app")
    RUNTIME_PROFILE_NAME = os.getenv(
        "RUNTIME_PROFILE",
        os.getenv("DM_RUNTIME_PROFILE", "presentation-demo"),
    )
    RUNTIME_PROFILE = get_runtime_profile(RUNTIME_PROFILE_NAME)
    
    # ===== MINIO / S3 STORAGE =====
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    S3_USE_SSL = os.getenv("S3_USE_SSL", "False").lower() == "true"
    
    # Bucket raiz e caminho base do lakehouse.
    # Em Kubernetes/MinIO, use o padrão s3a://lakehouse.
    # Para demo local, defina LAKEHOUSE_ROOT=file:///repo/data/lakehouse.
    LAKEHOUSE_BUCKET = os.getenv("LAKEHOUSE_BUCKET", "lakehouse")
    LAKEHOUSE_ROOT = os.getenv("LAKEHOUSE_ROOT", f"s3a://{LAKEHOUSE_BUCKET}")
    
    BRONZE_PATH = os.getenv("BRONZE_PATH", f"{LAKEHOUSE_ROOT}/bronze")
    RAW_VAULT_PATH = os.getenv("RAW_VAULT_PATH", f"{LAKEHOUSE_ROOT}/raw_vault")
    BUSINESS_VAULT_PATH = os.getenv("BUSINESS_VAULT_PATH", f"{LAKEHOUSE_ROOT}/business_vault")
    GOLD_PATH = os.getenv("GOLD_PATH", f"{LAKEHOUSE_ROOT}/gold")
    MONITORING_PATH = os.getenv("MONITORING_PATH", f"{LAKEHOUSE_ROOT}/monitoring")
    
    # Caminhos locais para dados de amostra
    SAMPLE_DATA_PATH = os.getenv("SAMPLE_DATA_PATH", os.path.join(PROJECT_ROOT, "data/sample"))
    
    # ===== SPARK CONFIGURACAO =====
    SPARK_PROFILE = RUNTIME_PROFILE["spark"]
    SPARK_MASTER = os.getenv("SPARK_MASTER", SPARK_PROFILE["master"])
    SPARK_APP_NAME = "banking-data-vault-pipeline"
    
    # Configurações Spark para Delta e S3
    SPARK_JARS_PACKAGES = os.getenv(
        "SPARK_JARS_PACKAGES",
        ",".join([
            "io.delta:delta-core_2.12:2.2.0",
            "org.apache.hadoop:hadoop-aws:3.3.2",
            "com.amazonaws:aws-java-sdk-bundle:1.11.1026",
        ]),
    )
    SPARK_CONFIGS = {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
        "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": str(S3_USE_SSL).lower(),
        "spark.jars.ivy": os.getenv("SPARK_IVY_DIR", "/tmp/.ivy2"),
        "spark.databricks.delta.schema.autoMerge.enabled": "true",
        "spark.sql.adaptive.enabled": os.getenv(
            "SPARK_ADAPTIVE_ENABLED",
            str(SPARK_PROFILE["adaptive_enabled"]).lower(),
        ),
        "spark.driver.memory": os.getenv(
            "SPARK_DRIVER_MEMORY",
            SPARK_PROFILE["driver_memory"],
        ),
        "spark.executor.memory": os.getenv(
            "SPARK_EXECUTOR_MEMORY",
            SPARK_PROFILE["executor_memory"],
        ),
        "spark.executor.instances": os.getenv(
            "SPARK_EXECUTOR_INSTANCES",
            str(SPARK_PROFILE["executor_instances"]),
        ),
        "spark.sql.shuffle.partitions": os.getenv(
            "SPARK_SQL_SHUFFLE_PARTITIONS",
            str(SPARK_PROFILE["shuffle_partitions"]),
        ),
        "spark.databricks.delta.snapshotPartitions": os.getenv(
            "SPARK_DELTA_SNAPSHOT_PARTITIONS",
            str(SPARK_PROFILE["shuffle_partitions"]),
        ),
    }
    if SPARK_JARS_PACKAGES.strip():
        SPARK_CONFIGS["spark.jars.packages"] = SPARK_JARS_PACKAGES
    
    # ===== DATA VAULT 2.0 CONFIGURACAO =====
    HASH_ALGORITHM = "SHA256"  # Para deterministic hash keys
    HASH_PREFIX = "hk_"  # Prefixo para hash keys
    
    # Fonte de dados padrão
    DEFAULT_RECORD_SOURCE = "banking_data_platform"
    
    # ===== TABELAS BRONZE =====
    BRONZE_TABLES = {
        "clientes": {"path": f"{BRONZE_PATH}/clientes", "format": "delta"},
        "contas": {"path": f"{BRONZE_PATH}/contas", "format": "delta"},
        "transacoes": {"path": f"{BRONZE_PATH}/transacoes", "format": "delta"},
        "cartoes": {"path": f"{BRONZE_PATH}/cartoes", "format": "delta"},
        "eventos_digitais": {"path": f"{BRONZE_PATH}/eventos_digitais", "format": "delta"},
        "agencias": {"path": f"{BRONZE_PATH}/agencias", "format": "delta"},
        "produtos": {"path": f"{BRONZE_PATH}/produtos", "format": "delta"},
    }
    
    # ===== HUBS DATA VAULT =====
    HUB_TABLES = {
        "hub_cliente": {"path": f"{RAW_VAULT_PATH}/hubs/hub_cliente", "business_key": ["cliente_id"]},
        "hub_conta": {"path": f"{RAW_VAULT_PATH}/hubs/hub_conta", "business_key": ["conta_id"]},
        "hub_cartao": {"path": f"{RAW_VAULT_PATH}/hubs/hub_cartao", "business_key": ["cartao_id"]},
        "hub_transacao": {"path": f"{RAW_VAULT_PATH}/hubs/hub_transacao", "business_key": ["transacao_id"]},
        "hub_agencia": {"path": f"{RAW_VAULT_PATH}/hubs/hub_agencia", "business_key": ["agencia_id"]},
        "hub_produto": {"path": f"{RAW_VAULT_PATH}/hubs/hub_produto", "business_key": ["produto_id"]},
        "hub_canal_digital": {"path": f"{RAW_VAULT_PATH}/hubs/hub_canal_digital", "business_key": ["canal_id"]},
    }
    
    # ===== LINKS DATA VAULT =====
    LINK_TABLES = {
        "link_cliente_conta": {
            "path": f"{RAW_VAULT_PATH}/links/link_cliente_conta",
            "hubs": ["hub_cliente", "hub_conta"],
        },
        "link_conta_transacao": {
            "path": f"{RAW_VAULT_PATH}/links/link_conta_transacao",
            "hubs": ["hub_conta", "hub_transacao"],
        },
        "link_cliente_cartao": {
            "path": f"{RAW_VAULT_PATH}/links/link_cliente_cartao",
            "hubs": ["hub_cliente", "hub_cartao"],
        },
        "link_cartao_transacao": {
            "path": f"{RAW_VAULT_PATH}/links/link_cartao_transacao",
            "hubs": ["hub_cartao", "hub_transacao"],
        },
        "link_conta_agencia": {
            "path": f"{RAW_VAULT_PATH}/links/link_conta_agencia",
            "hubs": ["hub_conta", "hub_agencia"],
        },
        "link_conta_produto": {
            "path": f"{RAW_VAULT_PATH}/links/link_conta_produto",
            "hubs": ["hub_conta", "hub_produto"],
        },
        "link_cliente_evento_digital": {
            "path": f"{RAW_VAULT_PATH}/links/link_cliente_evento_digital",
            "hubs": ["hub_cliente", "hub_canal_digital"],
        },
    }
    
    # ===== SATELLITES DATA VAULT =====
    SATELLITE_TABLES = {
        "sat_cliente_dados_cadastrais": {
            "path": f"{RAW_VAULT_PATH}/satellites/sat_cliente_dados_cadastrais",
            "hub": "hub_cliente",
        },
        "sat_cliente_documentos": {
            "path": f"{RAW_VAULT_PATH}/satellites/sat_cliente_documentos",
            "hub": "hub_cliente",
        },
        "sat_conta_detalhes": {
            "path": f"{RAW_VAULT_PATH}/satellites/sat_conta_detalhes",
            "hub": "hub_conta",
        },
        "sat_cartao_detalhes": {
            "path": f"{RAW_VAULT_PATH}/satellites/sat_cartao_detalhes",
            "hub": "hub_cartao",
        },
        "sat_transacao_detalhes": {
            "path": f"{RAW_VAULT_PATH}/satellites/sat_transacao_detalhes",
            "hub": "hub_transacao",
        },
        "sat_agencia_detalhes": {
            "path": f"{RAW_VAULT_PATH}/satellites/sat_agencia_detalhes",
            "hub": "hub_agencia",
        },
        "sat_produto_detalhes": {
            "path": f"{RAW_VAULT_PATH}/satellites/sat_produto_detalhes",
            "hub": "hub_produto",
        },
        "sat_evento_digital_detalhes": {
            "path": f"{RAW_VAULT_PATH}/satellites/sat_evento_digital_detalhes",
            "hub": "hub_canal_digital",
        },
    }
    
    # ===== TABELAS GOLD / INFORMATION MARTS =====
    GOLD_TABLES = {
        "gold_transacoes_por_dia": f"{GOLD_PATH}/gold_transacoes_por_dia",
        "gold_transacoes_por_cliente": f"{GOLD_PATH}/gold_transacoes_por_cliente",
        "gold_volume_por_produto": f"{GOLD_PATH}/gold_volume_por_produto",
        "gold_eventos_digitais_por_canal": f"{GOLD_PATH}/gold_eventos_digitais_por_canal",
        "gold_contas_por_agencia": f"{GOLD_PATH}/gold_contas_por_agencia",
        "gold_risco_transacional_simplificado": f"{GOLD_PATH}/gold_risco_transacional_simplificado",
        "gold_clientes_protegidos": f"{GOLD_PATH}/gold_clientes_protegidos",
    }
    
    # ===== TABELA DE MONITORAMENTO =====
    MONITORING_TABLE = f"{MONITORING_PATH}/pipeline_execution_log"
    
    # ===== LIMITES E VALIDACOES =====
    MAX_RETRIES = 3
    RETRY_DELAY = 5  # segundos
    
    # Colunas técnicas obrigatórias
    TECHNICAL_COLUMNS = [
        "load_datetime",
        "record_source",
        "source_system",
        "source_entity",
        "ingestion_mode",
        "schema_version",
        "batch_id",
        "run_id",
        "ingestion_date",
        "source_file",
        "source_record_count",
    ]
    
    @classmethod
    def get_bronze_table_config(cls, table_name: str) -> Dict[str, Any]:
        """Retorna configuração de uma tabela Bronze."""
        return cls.BRONZE_TABLES.get(table_name, {})
    
    @classmethod
    def get_hub_table_config(cls, hub_name: str) -> Dict[str, Any]:
        """Retorna configuração de um Hub."""
        return cls.HUB_TABLES.get(hub_name, {})
    
    @classmethod
    def get_link_table_config(cls, link_name: str) -> Dict[str, Any]:
        """Retorna configuração de um Link."""
        return cls.LINK_TABLES.get(link_name, {})
    
    @classmethod
    def get_satellite_table_config(cls, sat_name: str) -> Dict[str, Any]:
        """Retorna configuração de um Satellite."""
        return cls.SATELLITE_TABLES.get(sat_name, {})

    @classmethod
    def get_runtime_profile_config(cls) -> Dict[str, Any]:
        """Retorna o runtime profile ativo."""
        return get_runtime_profile(cls.RUNTIME_PROFILE_NAME)

    @classmethod
    def list_runtime_profiles(cls):
        """Lista os runtime profiles disponiveis."""
        return list_runtime_profiles()

    @classmethod
    def get_batch_record_counts(cls) -> Dict[str, int]:
        """Retorna os volumes batch definidos pelo runtime profile ativo."""
        return dict(cls.RUNTIME_PROFILE["batch"])


# Alias para facilitar importação
Config = PipelineConfig
