"""
Funções de hashing para Data Vault 2.0.
Implementa hash keys determinísticas usando SHA-256 conforme requisitos de DV2.0.
"""
import hashlib
from typing import Any, List, Union, Optional
from pyspark.sql import functions as F
import logging

logger = logging.getLogger(__name__)


class HashingUtils:
    """Utilitários para hashing determinístico em Data Vault 2.0."""
    
    ALGORITHM = "sha256"
    ENCODING = "utf-8"
    DELIMITER = "||"
    NULL_TOKEN = "^^"

    @staticmethod
    def _normalize_value(value: Any) -> str:
        """Normaliza valores sem perder a posição de nulls ou strings vazias."""
        if value is None:
            return HashingUtils.NULL_TOKEN
        normalized = str(value).strip()
        return normalized if normalized else HashingUtils.NULL_TOKEN
    
    @staticmethod
    def calculate_hash(
        values: Union[List[Any], Any],
        prefix: str = "",
        uppercase: bool = True,
        commutative: bool = False,
    ) -> str:
        """
        Calcula hash SHA-256 determinístico de um ou mais valores.
        
        Args:
            values: Valor único ou lista de valores para hashear
            prefix: Prefixo para adicionar ao hash (ex: "hk_" para hash keys)
            uppercase: Se True, retorna hash em maiúsculas
            commutative: Se True, ordena explicitamente os componentes antes
                do hash. O padrão preserva a ordem declarada.
            
        Returns:
            Hash em hexadecimal com prefixo
            
        Example:
            >>> HashingUtils.calculate_hash(["cliente_123"], prefix="hk_")
            'hk_abc123def456...'
        """
        # Garantir que values é uma lista
        if not isinstance(values, list):
            values = [values]
        
        # Converter para strings preservando a posição de nulls/vazios.
        str_values = [HashingUtils._normalize_value(value) for value in values]

        # Ordenação só existe quando a relação é explicitamente comutativa.
        if commutative:
            str_values.sort()
        
        # Concatenar com separador
        combined = HashingUtils.DELIMITER.join(str_values)
        
        # Calcular hash
        hash_obj = hashlib.sha256(combined.encode(HashingUtils.ENCODING))
        hash_hex = hash_obj.hexdigest()
        
        # Adicionar prefixo e formatar
        result = f"{prefix}{hash_hex}"
        
        if uppercase:
            result = result.upper()
        
        return result
    
    @staticmethod
    def calculate_hash_diff(
        values: Union[List[Any], Any],
        prefix: str = "",
        uppercase: bool = True
    ) -> str:
        """
        Calcula hash difference (hashdiff) para Satellites em Data Vault 2.0.
        Usado para detectar mudanças em atributos.
        
        Args:
            values: Valor único ou lista de valores para hashear
            prefix: Prefixo para adicionar ao hash
            uppercase: Se True, retorna hash em maiúsculas
            
        Returns:
            Hash em hexadecimal com prefixo
        """
        return HashingUtils.calculate_hash(
            values,
            prefix=prefix,
            uppercase=uppercase,
            commutative=False,
        )
    
    @staticmethod
    def spark_hash_key(
        spark_col,
        prefix: str = "hk_"
    ) -> 'Column':
        """
        Expressão Spark nativa para calcular hash key em Spark SQL.
        
        Args:
            spark_col: Coluna ou expressão Spark
            prefix: Prefixo para hash key
            
        Returns:
            Expressão Spark com hash key calculada
            
        Example:
            df = df.withColumn(
                "hk_cliente",
                HashingUtils.spark_hash_key(F.col("cliente_id"), prefix="hk_")
            )
        """
        return F.upper(
            F.concat(
                F.lit(prefix),
                F.sha2(F.trim(spark_col.cast("string")), 256),
            )
        )
    
    @staticmethod
    def spark_hash_diff(
        spark_cols: List,
        prefix: str = "hd_"
    ) -> 'Column':
        """
        Calcula hashdiff para múltiplas colunas em Spark.
        Usado em Satellites para detectar mudanças.
        
        Args:
            spark_cols: Lista de colunas Spark
            prefix: Prefixo para hashdiff
            
        Returns:
            Expressão Spark com hashdiff calculado
            
        Example:
            df = df.withColumn(
                "hd_cliente",
                HashingUtils.spark_hash_diff([
                    F.col("nome"),
                    F.col("email"),
                    F.col("telefone")
                ], prefix="hd_")
            )
        """
        # Concatenar colunas com || como separador.
        combined = F.concat_ws("||", *spark_cols)

        return F.upper(
            F.concat(
                F.lit(prefix),
                F.sha2(combined, 256),
            )
        )


class BusinessKeyHasher:
    """Gerador de Hash Keys para Data Vault baseado em Business Keys."""

    @staticmethod
    def _spark_hash_expr(
        spark_cols: List,
        prefix: str,
        commutative: bool = False,
    ) -> 'Column':
        """Calcula hash Spark preservando ordem, salvo opção comutativa explícita."""
        normalized = []
        for spark_col in spark_cols:
            string_col = F.trim(spark_col.cast("string"))
            normalized.append(
                F.when(
                    string_col.isNull() | (string_col == ""),
                    F.lit(HashingUtils.NULL_TOKEN),
                ).otherwise(string_col)
            )

        values = F.array(*normalized)
        if commutative:
            values = F.array_sort(values)
        combined = F.array_join(values, HashingUtils.DELIMITER)
        return F.upper(F.concat(F.lit(prefix), F.sha2(combined, 256)))

    @staticmethod
    def spark_ordered_hash_expr(spark_cols: List, prefix: str = "hk_") -> 'Column':
        """Expressão pública para chaves compostas e links role-sensitive."""
        return BusinessKeyHasher._spark_hash_expr(
            spark_cols,
            prefix,
            commutative=False,
        )

    @staticmethod
    def spark_commutative_hash_expr(spark_cols: List, prefix: str = "hk_") -> 'Column':
        """Expressão explícita para relações em que a ordem não possui semântica."""
        return BusinessKeyHasher._spark_hash_expr(
            spark_cols,
            prefix,
            commutative=True,
        )
    
    @staticmethod
    def generate_hub_hash_key(business_key_values: List[Any]) -> str:
        """
        Gera hash key para Hub.
        
        Args:
            business_key_values: Valores da business key
            
        Returns:
            Hash key para o Hub
        """
        return HashingUtils.calculate_hash(business_key_values, prefix="hk_")
    
    @staticmethod
    def generate_link_hash_key(
        hub_hash_keys: List[str],
        commutative: bool = False,
    ) -> str:
        """
        Gera hash key para Link.
        
        Args:
            hub_hash_keys: Hash keys dos Hubs relacionados
            
        Returns:
            Hash key para o Link
        """
        # Links combinam os hash keys dos hubs
        return HashingUtils.calculate_hash(
            hub_hash_keys,
            prefix="hk_",
            commutative=commutative,
        )
    
    @staticmethod
    def spark_generate_hub_hash_key(
        spark,
        df: 'DataFrame',
        business_key_cols: List[str],
        output_col: str = "hk_"
    ) -> 'DataFrame':
        """
        Adiciona coluna de hash key a um DataFrame para Hub.
        
        Args:
            spark: SparkSession
            df: DataFrame fonte
            business_key_cols: Colunas que formam a business key
            output_col: Nome da coluna de saída
            
        Returns:
            DataFrame com coluna de hash key adicionada
        """
        # Validar que todas as colunas existem
        missing_cols = set(business_key_cols) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Colunas não encontradas: {missing_cols}")
        
        # Preparar as colunas para hashing
        cols_to_hash = [F.col(c).cast("string") for c in business_key_cols]
        
        return df.withColumn(
            output_col,
            BusinessKeyHasher.spark_ordered_hash_expr(cols_to_hash, "hk_"),
        )
    
    @staticmethod
    def spark_generate_link_hash_key(
        spark,
        df: 'DataFrame',
        hub_hash_key_cols: List[str],
        output_col: str = "hk_",
        commutative: bool = False,
    ) -> 'DataFrame':
        """
        Adiciona coluna de hash key a um DataFrame para Link.
        
        Args:
            spark: SparkSession
            df: DataFrame fonte
            hub_hash_key_cols: Colunas com hash keys dos hubs
            output_col: Nome da coluna de saída
            
        Returns:
            DataFrame com coluna de hash key adicionada
        """
        # Validar que todas as colunas existem
        missing_cols = set(hub_hash_key_cols) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Colunas não encontradas: {missing_cols}")
        
        # Preparar as colunas para hashing
        cols_to_hash = [F.col(c) for c in hub_hash_key_cols]
        
        return df.withColumn(
            output_col,
            BusinessKeyHasher._spark_hash_expr(
                cols_to_hash,
                "hk_",
                commutative=commutative,
            ),
        )


if __name__ == "__main__":
    # Testes básicos
    print("=== Teste de Hashing ===")
    
    # Teste 1: Hash key simples
    hash_key = HashingUtils.calculate_hash("cliente_001", prefix="hk_")
    print(f"Hash key para cliente_001: {hash_key}")
    
    # Teste 2: Hash key com múltiplos valores
    hash_key = HashingUtils.calculate_hash(["cliente_001", "nome_client"], prefix="hk_")
    print(f"Hash key para múltiplos valores: {hash_key}")
    
    # Teste 3: Determinismo
    hash1 = HashingUtils.calculate_hash(["A", "B"], prefix="hk_")
    hash2 = HashingUtils.calculate_hash(["A", "B"], prefix="hk_")
    print(f"Determinismo: hash1 == hash2? {hash1 == hash2}")
    
    # Teste 4: Hub hash key
    hub_key = BusinessKeyHasher.generate_hub_hash_key(["cliente_123"])
    print(f"Hub hash key: {hub_key}")
    
    # Teste 5: Link hash key
    link_key = BusinessKeyHasher.generate_link_hash_key(["hk_abc123", "hk_def456"])
    print(f"Link hash key: {link_key}")
