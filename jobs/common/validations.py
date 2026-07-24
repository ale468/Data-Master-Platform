"""
Validações de qualidade de dados para o pipeline.
Implementa regras de validação para Data Vault.
"""
from typing import Dict, Optional, Tuple, List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
import logging

from monitoring import DataQualityLogger

logger = logging.getLogger(__name__)


class DataQualityValidations:
    """Validações de qualidade de dados."""
    
    @staticmethod
    def validate_row_count(
        df: DataFrame,
        min_rows: int = 0,
        max_rows: Optional[int] = None,
        validation_name: str = "row_count_validation"
    ) -> Tuple[bool, int]:
        """
        Valida número de linhas em DataFrame.
        
        Args:
            df: DataFrame
            min_rows: Número mínimo de linhas esperado
            max_rows: Número máximo de linhas esperado (opcional)
            validation_name: Nome da validação
            
        Returns:
            Tuple (validação_passou, contagem_linhas)
        """
        row_count = df.count()
        
        passed = row_count >= min_rows
        if max_rows is not None:
            passed = passed and row_count <= max_rows
        
        logger.info(
            f"{validation_name}: {'PASSOU' if passed else 'FALHOU'} "
            f"({row_count} linhas, esperado: {min_rows}-{max_rows or 'ilimitado'})"
        )
        
        return passed, row_count
    
    @staticmethod
    def validate_null_keys(
        df: DataFrame,
        key_columns: List[str],
        validation_name: str = "null_key_validation"
    ) -> Tuple[bool, Dict[str, int]]:
        """
        Valida que colunas chave não contêm nulos.
        
        Args:
            df: DataFrame
            key_columns: Colunas chave a validar
            validation_name: Nome da validação
            
        Returns:
            Tuple (validação_passou, dict com contagem de nulos por coluna)
        """
        null_counts = {}
        has_nulls = False
        
        for col in key_columns:
            if col not in df.columns:
                logger.warning(f"Coluna chave não encontrada: {col}")
                continue
            
            null_count = df.filter(F.col(col).isNull()).count()
            null_counts[col] = null_count
            
            if null_count > 0:
                has_nulls = True
                logger.warning(f"Nulos encontrados em {col}: {null_count}")
        
        passed = not has_nulls
        logger.info(
            f"{validation_name}: {'PASSOU' if passed else 'FALHOU'} - "
            f"Nulos por coluna: {null_counts}"
        )
        
        return passed, null_counts
    
    @staticmethod
    def validate_uniqueness(
        df: DataFrame,
        key_columns: List[str],
        validation_name: str = "uniqueness_validation"
    ) -> Tuple[bool, int]:
        """
        Valida uniqueness de colunas chave.
        
        Args:
            df: DataFrame
            key_columns: Colunas chave a validar
            validation_name: Nome da validação
            
        Returns:
            Tuple (validação_passou, contagem de duplicatas)
        """
        total_rows = df.count()
        unique_rows = df.select(*key_columns).distinct().count()
        
        duplicate_count = total_rows - unique_rows
        passed = duplicate_count == 0
        
        logger.info(
            f"{validation_name}: {'PASSOU' if passed else 'FALHOU'} - "
            f"Total: {total_rows}, Únicos: {unique_rows}, Duplicatas: {duplicate_count}"
        )
        
        return passed, duplicate_count
    
    @staticmethod
    def validate_data_type(
        df: DataFrame,
        col_name: str,
        expected_type: str,
        validation_name: str = "data_type_validation"
    ) -> bool:
        """
        Valida tipo de dado de uma coluna.
        
        Args:
            df: DataFrame
            col_name: Nome da coluna
            expected_type: Tipo esperado (string, int, double, timestamp, etc)
            validation_name: Nome da validação
            
        Returns:
            True se tipo é correto
        """
        if col_name not in df.columns:
            logger.warning(f"Coluna não encontrada: {col_name}")
            return False
        
        actual_type = dict(df.dtypes)[col_name]
        passed = expected_type.lower() in actual_type.lower()
        
        logger.info(
            f"{validation_name}: {'PASSOU' if passed else 'FALHOU'} - "
            f"{col_name}: esperado={expected_type}, atual={actual_type}"
        )
        
        return passed
    
    @staticmethod
    def validate_range(
        df: DataFrame,
        col_name: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        validation_name: str = "range_validation"
    ) -> Tuple[bool, int]:
        """
        Valida que valores estão dentro de range.
        
        Args:
            df: DataFrame
            col_name: Nome da coluna
            min_value: Valor mínimo (opcional)
            max_value: Valor máximo (opcional)
            validation_name: Nome da validação
            
        Returns:
            Tuple (validação_passou, contagem de valores fora do range)
        """
        if col_name not in df.columns:
            logger.warning(f"Coluna não encontrada: {col_name}")
            return False, 0
        
        condition = F.col(col_name).isNotNull()
        
        if min_value is not None:
            condition = condition & (F.col(col_name) >= min_value)
        
        if max_value is not None:
            condition = condition & (F.col(col_name) <= max_value)
        
        out_of_range = df.filter(~condition).count()
        passed = out_of_range == 0
        
        logger.info(
            f"{validation_name}: {'PASSOU' if passed else 'FALHOU'} - "
            f"Fora do range [{min_value}, {max_value}]: {out_of_range}"
        )
        
        return passed, out_of_range
    
    @staticmethod
    def validate_pattern(
        df: DataFrame,
        col_name: str,
        pattern: str,
        validation_name: str = "pattern_validation"
    ) -> Tuple[bool, int]:
        """
        Valida que valores correspondem a padrão regex.
        
        Args:
            df: DataFrame
            col_name: Nome da coluna
            pattern: Padrão regex
            validation_name: Nome da validação
            
        Returns:
            Tuple (validação_passou, contagem de valores que não correspondem)
        """
        if col_name not in df.columns:
            logger.warning(f"Coluna não encontrada: {col_name}")
            return False, 0
        
        non_matching = df.filter(
            ~F.col(col_name).rlike(pattern)
        ).count()
        
        passed = non_matching == 0
        
        logger.info(
            f"{validation_name}: {'PASSOU' if passed else 'FALHOU'} - "
            f"Padrão: {pattern}, Não correspondentes: {non_matching}"
        )
        
        return passed, non_matching


class DataVaultValidations:
    """Validações específicas para Data Vault 2.0."""
    
    @staticmethod
    def validate_hub_structure(
        df: DataFrame,
        hash_key_col: str,
        business_key_cols: List[str]
    ) -> bool:
        """
        Valida estrutura de Hub.
        
        Args:
            df: DataFrame do Hub
            hash_key_col: Nome da coluna hash key
            business_key_cols: Colunas da business key
            
        Returns:
            True se estrutura é válida
        """
        logger.info("Validando estrutura de Hub...")
        
        # Validar colunas obrigatórias
        required_cols = [hash_key_col, "load_datetime", "record_source"] + business_key_cols
        missing_cols = set(required_cols) - set(df.columns)
        
        if missing_cols:
            logger.error(f"Colunas obrigatórias faltando em Hub: {missing_cols}")
            return False
        
        # Validar uniqueness de hash key
        passed, _ = DataQualityValidations.validate_uniqueness(
            df, [hash_key_col], "hub_uniqueness"
        )
        
        # Validar sem nulos em hash key e business key
        passed_nulls, _ = DataQualityValidations.validate_null_keys(
            df, [hash_key_col] + business_key_cols, "hub_null_keys"
        )
        
        return passed and passed_nulls
    
    @staticmethod
    def validate_satellite_structure(
        df: DataFrame,
        hash_key_col: str,
        load_datetime_col: str = "load_datetime",
        hash_diff_col: Optional[str] = "hd_"
    ) -> bool:
        """
        Valida estrutura de Satellite.
        
        Args:
            df: DataFrame do Satellite
            hash_key_col: Nome da coluna hash key
            load_datetime_col: Nome da coluna load_datetime
            hash_diff_col: Nome da coluna hash diff
            
        Returns:
            True se estrutura é válida
        """
        logger.info("Validando estrutura de Satellite...")
        
        # Validar colunas obrigatórias
        required_cols = [hash_key_col, load_datetime_col, "record_source"]
        if hash_diff_col:
            required_cols.append(hash_diff_col)
        
        missing_cols = set(required_cols) - set(df.columns)
        
        if missing_cols:
            logger.error(f"Colunas obrigatórias faltando em Satellite: {missing_cols}")
            return False
        
        # Validar sem nulos em colunas chave
        passed, _ = DataQualityValidations.validate_null_keys(
            df, [hash_key_col], "satellite_null_keys"
        )
        
        return passed
    
    @staticmethod
    def validate_link_structure(
        df: DataFrame,
        hash_key_col: str,
        hub_hash_key_cols: List[str]
    ) -> bool:
        """
        Valida estrutura de Link.
        
        Args:
            df: DataFrame do Link
            hash_key_col: Nome da coluna hash key
            hub_hash_key_cols: Colunas com hash keys dos Hubs
            
        Returns:
            True se estrutura é válida
        """
        logger.info("Validando estrutura de Link...")
        
        # Validar colunas obrigatórias
        required_cols = [hash_key_col, "load_datetime", "record_source"] + hub_hash_key_cols
        missing_cols = set(required_cols) - set(df.columns)
        
        if missing_cols:
            logger.error(f"Colunas obrigatórias faltando em Link: {missing_cols}")
            return False
        
        # Validar uniqueness de hash key
        passed, _ = DataQualityValidations.validate_uniqueness(
            df, [hash_key_col], "link_uniqueness"
        )
        
        # Validar sem nulos em hash keys
        passed_nulls, _ = DataQualityValidations.validate_null_keys(
            df, [hash_key_col] + hub_hash_key_cols, "link_null_keys"
        )
        
        return passed and passed_nulls


def run_quality_checks(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    checks_config: dict,
    batch_id: str
) -> bool:
    """
    Executa série de validações de qualidade.
    
    Args:
        spark: SparkSession
        df: DataFrame a validar
        table_name: Nome da tabela
        checks_config: Dict com configuração de validações
        batch_id: ID do lote
        
    Returns:
        True se todas as validações passaram
    """
    all_passed = True
    
    logger.info(f"Iniciando validações para {table_name}")
    
    # Executar validações conforme configuração
    for check_name, check_config in checks_config.items():
        try:
            check_type = check_config.get("type")
            
            if check_type == "row_count":
                passed, _ = DataQualityValidations.validate_row_count(
                    df,
                    min_rows=check_config.get("min_rows", 0),
                    max_rows=check_config.get("max_rows"),
                    validation_name=check_name
                )
            
            elif check_type == "null_keys":
                passed, _ = DataQualityValidations.validate_null_keys(
                    df,
                    key_columns=check_config.get("columns", []),
                    validation_name=check_name
                )
            
            elif check_type == "uniqueness":
                passed, _ = DataQualityValidations.validate_uniqueness(
                    df,
                    key_columns=check_config.get("columns", []),
                    validation_name=check_name
                )
            
            elif check_type == "data_type":
                passed = DataQualityValidations.validate_data_type(
                    df,
                    col_name=check_config.get("column"),
                    expected_type=check_config.get("type_expected"),
                    validation_name=check_name
                )
            
            elif check_type == "range":
                passed, _ = DataQualityValidations.validate_range(
                    df,
                    col_name=check_config.get("column"),
                    min_value=check_config.get("min_value"),
                    max_value=check_config.get("max_value"),
                    validation_name=check_name
                )
            
            else:
                logger.warning(f"Tipo de validação desconhecido: {check_type}")
                continue
            
            if not passed:
                all_passed = False
        
        except Exception as e:
            logger.error(f"Erro ao executar validação {check_name}: {str(e)}")
            all_passed = False
    
    status = "PASSOU" if all_passed else "FALHOU"
    logger.info(f"Validações para {table_name}: {status}")
    
    return all_passed


if __name__ == "__main__":
    from spark_session import create_spark_session
    
    spark = create_spark_session()
    
    # Teste: criar DataFrame de teste
    test_data = [
        {"id": "1", "name": "Cliente A", "value": 100},
        {"id": "2", "name": "Cliente B", "value": 200},
    ]
    
    df_test = spark.createDataFrame(test_data)
    
    # Executar validações
    print("=== Teste de Validações ===\n")
    
    passed, count = DataQualityValidations.validate_row_count(df_test, min_rows=1)
    print(f"Row count validation: {passed}, count={count}\n")
    
    passed, nulls = DataQualityValidations.validate_null_keys(df_test, ["id", "name"])
    print(f"Null keys validation: {passed}, nulls={nulls}\n")
    
    passed, dups = DataQualityValidations.validate_uniqueness(df_test, ["id"])
    print(f"Uniqueness validation: {passed}, duplicates={dups}\n")
