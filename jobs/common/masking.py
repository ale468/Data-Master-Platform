"""
Funções de mascaramento de dados sensíveis conforme LGPD.
Implementa mascaramento e pseudonimização para PII (Personally Identifiable Information).
"""
import re
from typing import Optional, Union
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import logging

logger = logging.getLogger(__name__)


class MaskingUtils:
    """Utilitários para mascaramento de dados sensíveis conforme LGPD."""
    
    # Padrões de validação
    CPF_PATTERN = r"^\d{3}\.\d{3}\.\d{3}-\d{2}$|^\d{11}$"
    EMAIL_PATTERN = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    PHONE_PATTERN = r"^\+?[\d\s\-\(\)]{10,}$"
    CARD_PATTERN = r"^\d{13,19}$"
    
    @staticmethod
    def mask_cpf(cpf: Optional[str]) -> str:
        """
        Mascara CPF mantendo últimos 2 dígitos.
        Exemplo: 123.456.789-10 -> ***.***.***.10
        
        Args:
            cpf: CPF a mascarar
            
        Returns:
            CPF mascarado
        """
        if cpf is None:
            return None
        
        # Remover pontuação
        clean_cpf = re.sub(r"[^\d]", "", str(cpf))
        
        if len(clean_cpf) < 11:
            return "*" * 11
        
        # Manter últimos 2 dígitos
        masked = "*" * 9 + clean_cpf[-2:]
        return masked
    
    @staticmethod
    def mask_email(email: Optional[str]) -> str:
        """
        Mascara e-mail mantendo domínio.
        Exemplo: user@example.com -> u***@example.com
        
        Args:
            email: E-mail a mascarar
            
        Returns:
            E-mail mascarado
        """
        if email is None:
            return None
        
        email_str = str(email).strip()
        
        if "@" not in email_str:
            return "*" * len(email_str)
        
        local, domain = email_str.split("@", 1)
        
        # Manter primeira letra e domínio
        if len(local) <= 1:
            masked_local = "*"
        else:
            masked_local = local[0] + "*" * (len(local) - 1)
        
        return f"{masked_local}@{domain}"
    
    @staticmethod
    def mask_phone(phone: Optional[str]) -> str:
        """
        Mascara telefone mantendo últimos 4 dígitos.
        Exemplo: +55 (11) 98765-4321 -> +55 (*) ****-4321
        
        Args:
            phone: Telefone a mascarar
            
        Returns:
            Telefone mascarado
        """
        if phone is None:
            return None
        
        phone_str = str(phone).strip()
        
        # Extrair apenas dígitos
        digits = re.sub(r"[^\d]", "", phone_str)
        
        if len(digits) < 4:
            return "*" * len(phone_str)
        
        # Manter apenas últimos 4 dígitos
        masked_digits = "*" * (len(digits) - 4) + digits[-4:]
        
        # Reformatar mantendo estrutura original se possível
        if "(" in phone_str and ")" in phone_str:
            # Telefone formatado: +55 (11) 98765-4321
            return f"*** (*) ****-{digits[-4:]}"
        else:
            return masked_digits
    
    @staticmethod
    def mask_card_number(card: Optional[str]) -> str:
        """
        Mascara número de cartão mantendo últimos 4 dígitos.
        Exemplo: 4532015112830366 -> ****-****-****-0366
        
        Args:
            card: Número do cartão
            
        Returns:
            Número mascarado
        """
        if card is None:
            return None
        
        # Remover espaços e hífens
        clean_card = re.sub(r"[\s\-]", "", str(card))
        
        if len(clean_card) < 4:
            return "*" * len(clean_card)
        
        # Manter apenas últimos 4 dígitos
        masked = "*" * (len(clean_card) - 4) + clean_card[-4:]
        
        # Formatar em grupos de 4
        formatted = "-".join([masked[i:i+4] for i in range(0, len(masked), 4)])
        
        return formatted
    
    @staticmethod
    def mask_name(name: Optional[str], partial: bool = False) -> str:
        """
        Mascara nome completo.
        Exemplo (partial=False): João da Silva -> J***
        Exemplo (partial=True): João da Silva -> João S***
        
        Args:
            name: Nome a mascarar
            partial: Se True, mantém primeiro nome
            
        Returns:
            Nome mascarado
        """
        if name is None:
            return None
        
        name_str = str(name).strip()
        parts = name_str.split()
        
        if len(parts) == 0:
            return "*" * len(name_str)
        
        if partial and len(parts) >= 2:
            # Manter primeiro nome e primeira letra do segundo
            return f"{parts[0]} {parts[-1][0]}***"
        else:
            # Manter apenas primeira letra
            return f"{parts[0][0]}***"
    
    @staticmethod
    def mask_address(address: Optional[str]) -> str:
        """
        Mascara endereço completo.
        Exemplo: Rua A, 123, Apt 456 -> Rua *, nº ***, apt ***
        
        Args:
            address: Endereço a mascarar
            
        Returns:
            Endereço mascarado
        """
        if address is None:
            return None
        
        # Substituir números por asteriscos, manter estrutura
        masked = re.sub(r"\d+", "*", str(address))
        return masked
    
    @staticmethod
    def pseudonymize_id(id_value: Optional[str], prefix: str = "USER") -> str:
        """
        Substitui ID por pseudônimo.
        Exemplo: 12345 -> USER_12345 (hash-based)
        
        Args:
            id_value: ID original
            prefix: Prefixo para pseudônimo
            
        Returns:
            ID pseudonimizado
        """
        if id_value is None:
            return None
        
        # Gerar pseudônimo baseado em hash
        import hashlib
        hash_obj = hashlib.sha256(str(id_value).encode())
        hash_hex = hash_obj.hexdigest()[:8].upper()
        
        return f"{prefix}_{hash_hex}"
    
    # ===== UDFs Spark =====
    
    @staticmethod
    def spark_mask_cpf(col):
        """UDF Spark para mascarar CPF."""
        mask_udf = F.udf(MaskingUtils.mask_cpf, StringType())
        return mask_udf(col)
    
    @staticmethod
    def spark_mask_email(col):
        """UDF Spark para mascarar e-mail."""
        mask_udf = F.udf(MaskingUtils.mask_email, StringType())
        return mask_udf(col)
    
    @staticmethod
    def spark_mask_phone(col):
        """UDF Spark para mascarar telefone."""
        mask_udf = F.udf(MaskingUtils.mask_phone, StringType())
        return mask_udf(col)
    
    @staticmethod
    def spark_mask_card(col):
        """UDF Spark para mascarar cartão."""
        mask_udf = F.udf(MaskingUtils.mask_card_number, StringType())
        return mask_udf(col)
    
    @staticmethod
    def spark_mask_name(col, partial: bool = False):
        """UDF Spark para mascarar nome."""
        mask_udf = F.udf(
            lambda x: MaskingUtils.mask_name(x, partial=partial),
            StringType()
        )
        return mask_udf(col)
    
    @staticmethod
    def spark_pseudonymize_id(col, prefix: str = "USER"):
        """UDF Spark para pseudonimizar ID."""
        pseudonym_udf = F.udf(
            lambda x: MaskingUtils.pseudonymize_id(x, prefix=prefix),
            StringType()
        )
        return pseudonym_udf(col)


class MaskingPolicy:
    """Política de mascaramento por tipo de dado."""
    
    # Definição de campos sensíveis e suas regras
    SENSITIVE_FIELDS = {
        "cpf": MaskingUtils.mask_cpf,
        "documento": MaskingUtils.mask_cpf,
        "email": MaskingUtils.mask_email,
        "telefone": MaskingUtils.mask_phone,
        "celular": MaskingUtils.mask_phone,
        "cartao": MaskingUtils.mask_card_number,
        "numero_cartao": MaskingUtils.mask_card_number,
        "endereco": MaskingUtils.mask_address,
        "nome": MaskingUtils.mask_name,
        "cliente_id": lambda x: MaskingUtils.pseudonymize_id(x, "CLIENTE"),
    }
    
    @staticmethod
    def mask_dataframe(df, column_masks: dict) -> 'DataFrame':
        """
        Aplica mascaramento a um DataFrame.
        
        Args:
            df: DataFrame
            column_masks: Dict com {nome_coluna: funcao_mascaramento}
                         ou {nome_coluna: tipo_dado} (usa política padrão)
            
        Returns:
            DataFrame com colunas mascaradas
        """
        for col_name, mask_func in column_masks.items():
            if col_name not in df.columns:
                logger.warning(f"Coluna {col_name} não encontrada no DataFrame")
                continue
            
            # Se for string, buscar na política padrão
            if isinstance(mask_func, str):
                mask_func = MaskingPolicy.SENSITIVE_FIELDS.get(mask_func.lower())
                if mask_func is None:
                    logger.warning(f"Mascarador não encontrado para tipo: {mask_func}")
                    continue
            
            # Aplicar mascaramento
            mask_udf = F.udf(mask_func, StringType())
            df = df.withColumn(col_name, mask_udf(F.col(col_name)))
        
        return df
    
    @staticmethod
    def get_default_policy_for_layer(layer: str) -> dict:
        """
        Retorna política de mascaramento padrão por camada.
        
        Args:
            layer: "bronze", "raw_vault" ou "gold"
            
        Returns:
            Dict com mapeamento de colunas e funções
        """
        if layer == "bronze":
            # Bronze: sem mascaramento (dados brutos)
            return {}
        
        elif layer == "raw_vault":
            # Raw Vault: sem mascaramento (necessário para histórico)
            return {}
        
        elif layer == "gold":
            # Gold: mascarar todos os dados sensíveis
            return {
                "cpf": MaskingUtils.mask_cpf,
                "email": MaskingUtils.mask_email,
                "telefone": MaskingUtils.mask_phone,
                "celular": MaskingUtils.mask_phone,
                "numero_cartao": MaskingUtils.mask_card_number,
                "endereco": MaskingUtils.mask_address,
                "nome_cliente": MaskingUtils.mask_name,
            }
        
        return {}


if __name__ == "__main__":
    # Testes básicos
    print("=== Teste de Mascaramento LGPD ===\n")
    
    # Teste CPF
    print("CPF:")
    print(f"  Original: 123.456.789-10")
    print(f"  Mascarado: {MaskingUtils.mask_cpf('123.456.789-10')}\n")
    
    # Teste E-mail
    print("E-mail:")
    print(f"  Original: joao.silva@example.com")
    print(f"  Mascarado: {MaskingUtils.mask_email('joao.silva@example.com')}\n")
    
    # Teste Telefone
    print("Telefone:")
    print(f"  Original: +55 (11) 98765-4321")
    print(f"  Mascarado: {MaskingUtils.mask_phone('+55 (11) 98765-4321')}\n")
    
    # Teste Cartão
    print("Cartão:")
    print(f"  Original: 4532-0151-1283-0366")
    print(f"  Mascarado: {MaskingUtils.mask_card_number('4532-0151-1283-0366')}\n")
    
    # Teste Nome
    print("Nome:")
    print(f"  Original: João da Silva")
    print(f"  Mascarado (full): {MaskingUtils.mask_name('João da Silva', partial=False)}")
    print(f"  Mascarado (partial): {MaskingUtils.mask_name('João da Silva', partial=True)}\n")
    
    # Teste Pseudonimização
    print("Pseudonimização:")
    print(f"  Original: CLIENTE_12345")
    print(f"  Pseudonimizado: {MaskingUtils.pseudonymize_id('CLIENTE_12345', 'CLT')}\n")
