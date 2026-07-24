"""
Gerador de dados bancários sintéticos para demonstração.
Cria datasets simulados em formatos CSV e JSON para testes do pipeline.
"""
import os
import argparse
import csv
import json
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging

try:
    from jobs.common.runtime_profiles import get_runtime_profile, list_runtime_profiles
except ImportError:
    import sys
    sys.path.insert(
        0,
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "common")),
    )
    from runtime_profiles import get_runtime_profile, list_runtime_profiles

logger = logging.getLogger(__name__)

# Configuração de seed para reprodutibilidade
random.seed(42)


class BankingDataGenerator:
    """Gerador de dados bancários sintéticos."""
    
    # Domínios de dados
    PRIMEIRO_NOMES = [
        "João", "Maria", "Pedro", "Ana", "Carlos", "Fernanda", 
        "Lucas", "Paula", "Ricardo", "Juliana", "André", "Beatriz"
    ]
    
    SOBRENOMES = [
        "Silva", "Santos", "Oliveira", "Pereira", "Costa", "Martins",
        "Gomes", "Alves", "Ribeiro", "Ferreira", "Rodrigues", "Medeiros"
    ]
    
    BANCOS = ["Bradesco", "Itaú", "Caixa", "Santander", "Banco do Brasil"]
    
    TIPOS_CONTA = ["Corrente", "Poupança", "Investimento"]
    
    TIPOS_CARTAO = ["Débito", "Crédito", "Pré-pago"]
    
    ESTADOS = ["SP", "RJ", "MG", "RS", "BA", "PE", "PR", "SC"]
    
    CANAIS_DIGITAIS = ["Mobile App", "Web Banking", "ATM", "Caixa Eletrônico", "Agência"]
    
    TIPOS_EVENTO = ["Login", "Consulta Saldo", "Transferência", "Pagamento", "Investimento", "Logout"]
    
    TIPOS_PRODUTO = ["Conta Corrente", "Poupança", "Cartão de Crédito", "Empréstimo", "Seguro"]
    
    CIDADES = {
        "SP": ["São Paulo", "Campinas", "Santos", "Araraquara"],
        "RJ": ["Rio de Janeiro", "Niterói", "Duque de Caxias"],
        "MG": ["Belo Horizonte", "Uberlândia", "Contagem"],
        "RS": ["Porto Alegre", "Caxias do Sul", "Novo Hamburgo"],
    }
    
    @staticmethod
    def generate_cpf() -> str:
        """Gera CPF simulado válido."""
        def generate_digit(cpf):
            total = sum(int(cpf[i]) * (len(cpf) + 1 - i) for i in range(len(cpf)))
            digit = 11 - (total % 11)
            return str(digit if digit < 10 else 0)
        
        base = ''.join(str(random.randint(0, 9)) for _ in range(9))
        digit1 = generate_digit(base)
        digit2 = generate_digit(base + digit1)
        
        cpf = base + digit1 + digit2
        return f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:]}"
    
    @staticmethod
    def generate_email(name: str) -> str:
        """Gera e-mail simulado."""
        user = name.lower().replace(" ", ".").replace("ã", "a").replace("á", "a")
        domain = random.choice(["gmail.com", "hotmail.com", "outlook.com", "bancobr.com"])
        return f"{user}.{random.randint(1, 999)}@{domain}"
    
    @staticmethod
    def generate_phone() -> str:
        """Gera telefone simulado."""
        ddd = random.randint(11, 99)
        prefix = random.randint(90000, 99999)
        sufix = random.randint(1000, 9999)
        return f"+55 ({ddd}) {prefix}-{sufix}"
    
    @staticmethod
    def generate_clients(count: int = 100) -> List[Dict[str, Any]]:
        """Gera dados de clientes."""
        logger.info(f"Gerando {count} clientes...")
        clients = []
        
        for i in range(count):
            first_name = random.choice(BankingDataGenerator.PRIMEIRO_NOMES)
            last_name = random.choice(BankingDataGenerator.SOBRENOMES)
            full_name = f"{first_name} {last_name}"
            
            client = {
                "cliente_id": f"CLI_{i+1:06d}",
                "nome": full_name,
                "cpf": BankingDataGenerator.generate_cpf(),
                "email": BankingDataGenerator.generate_email(full_name),
                "telefone": BankingDataGenerator.generate_phone(),
                "data_nascimento": BankingDataGenerator.generate_birth_date(),
                "estado": random.choice(BankingDataGenerator.ESTADOS),
                "cidade": "São Paulo",  # Simplificado
                "endereco": f"Rua {i}, {random.randint(100, 9999)}",
                "data_cadastro": BankingDataGenerator.generate_date(days_back=730)
            }
            clients.append(client)
        
        logger.info(f"✓ {len(clients)} clientes gerados")
        return clients
    
    @staticmethod
    def generate_accounts(
        clients: List[Dict],
        agencies: List[Dict],
        products: List[Dict],
        count_per_client: int = 1
    ) -> List[Dict[str, Any]]:
        """Gera dados de contas."""
        logger.info(f"Gerando contas...")
        accounts = []
        
        for client in clients:
            for j in range(random.randint(1, count_per_client)):
                agency = random.choice(agencies)
                product = random.choice(products)
                account = {
                    "conta_id": f"ACC_{len(accounts)+1:08d}",
                    "cliente_id": client["cliente_id"],
                    "agencia_id": agency["agencia_id"],
                    "produto_id": product["produto_id"],
                    "tipo_conta": random.choice(BankingDataGenerator.TIPOS_CONTA),
                    "agencia": agency["numero_agencia"],
                    "numero_conta": f"{random.randint(100000, 999999)}",
                    "saldo": round(random.uniform(0, 100000), 2),
                    "limite": round(random.uniform(0, 50000), 2),
                    "data_abertura": BankingDataGenerator.generate_date(days_back=365),
                    "status": random.choice(["Ativa", "Inativa", "Bloqueada"])
                }
                accounts.append(account)
        
        logger.info(f"✓ {len(accounts)} contas geradas")
        return accounts
    
    @staticmethod
    def generate_cards(accounts: List[Dict], count_per_account: int = 1) -> List[Dict[str, Any]]:
        """Gera dados de cartões."""
        logger.info(f"Gerando cartões...")
        cards = []
        
        for account in accounts:
            for j in range(random.randint(0, count_per_account)):
                card = {
                    "cartao_id": f"CARD_{len(cards)+1:08d}",
                    "conta_id": account["conta_id"],
                    "numero_cartao": BankingDataGenerator.generate_card_number(),
                    "tipo_cartao": random.choice(BankingDataGenerator.TIPOS_CARTAO),
                    "bandeira": random.choice(["Visa", "Mastercard", "Elo"]),
                    "data_emissao": BankingDataGenerator.generate_date(days_back=365),
                    "data_expiracao": BankingDataGenerator.generate_date(days_forward=1000),
                    "cvv": f"{random.randint(100, 999)}",
                    "status": random.choice(["Ativo", "Cancelado", "Bloqueado"])
                }
                cards.append(card)
        
        logger.info(f"✓ {len(cards)} cartões gerados")
        return cards
    
    @staticmethod
    def generate_transactions(accounts: List[Dict], cards: List[Dict], 
                             count: int = 1000) -> List[Dict[str, Any]]:
        """Gera dados de transações."""
        logger.info(f"Gerando {count} transações...")
        transactions = []
        
        for i in range(count):
            # Escolher entre conta ou cartão
            source_type = random.choice(["conta", "cartao"])
            
            if source_type == "conta":
                account = random.choice(accounts)
                source_id = account["conta_id"]
            else:
                card = random.choice(cards)
                source_id = card["cartao_id"]
            
            transaction = {
                "transacao_id": f"TXN_{i+1:09d}",
                "conta_id": random.choice(accounts)["conta_id"],
                "cartao_id": random.choice(cards)["cartao_id"] if random.random() > 0.7 else None,
                "tipo_transacao": random.choice(["Transferência", "Pagamento", "Saque", "Depósito"]),
                "valor": round(random.uniform(10, 10000), 2),
                "data_transacao": BankingDataGenerator.generate_datetime(hours_back=24*30),
                "data_liquidacao": BankingDataGenerator.generate_date(days_back=30),
                "status": random.choice(["Concluída", "Pendente", "Cancelada"]),
                "descricao": random.choice([
                    "Transferência eletrônica",
                    "Pagamento de fatura",
                    "Saque em caixa",
                    "Depósito",
                    "Compra em estabelecimento"
                ])
            }
            transactions.append(transaction)
        
        logger.info(f"✓ {len(transactions)} transações geradas")
        return transactions
    
    @staticmethod
    def generate_digital_events(clients: List[Dict], count: int = 2000) -> List[Dict[str, Any]]:
        """Gera eventos de canais digitais."""
        logger.info(f"Gerando {count} eventos digitais...")
        events = []
        
        for i in range(count):
            canal = random.choice(BankingDataGenerator.CANAIS_DIGITAIS)
            event = {
                "evento_id": f"EVT_{i+1:09d}",
                "cliente_id": random.choice(clients)["cliente_id"],
                "canal_id": f"CANAL_{BankingDataGenerator.CANAIS_DIGITAIS.index(canal)+1:03d}",
                "canal": canal,
                "tipo_evento": random.choice(BankingDataGenerator.TIPOS_EVENTO),
                "timestamp": BankingDataGenerator.generate_datetime(hours_back=24*30),
                "resultado": random.choice(["Sucesso", "Falha", "Timeout"]),
                "detalhes": f"Evento #{i+1}"
            }
            events.append(event)
        
        logger.info(f"✓ {len(events)} eventos gerados")
        return events
    
    @staticmethod
    def generate_agencies(count: int = 20) -> List[Dict[str, Any]]:
        """Gera dados de agências."""
        logger.info(f"Gerando {count} agências...")
        agencies = []
        
        for i in range(count):
            agency = {
                "agencia_id": f"AG_{i+1:04d}",
                "numero_agencia": f"{random.randint(1000, 9999)}",
                "nome": f"Agência {random.choice(BankingDataGenerator.CIDADES['SP'])}",
                "estado": random.choice(BankingDataGenerator.ESTADOS),
                "cidade": random.choice(BankingDataGenerator.CIDADES.get("SP", ["São Paulo"])),
                "endereco": f"Avenida Principal, {random.randint(100, 9999)}",
                "telefone": BankingDataGenerator.generate_phone(),
                "gerente": f"{random.choice(BankingDataGenerator.PRIMEIRO_NOMES)} {random.choice(BankingDataGenerator.SOBRENOMES)}",
                "data_inauguracao": BankingDataGenerator.generate_date(days_back=3650)
            }
            agencies.append(agency)
        
        logger.info(f"✓ {len(agencies)} agências geradas")
        return agencies
    
    @staticmethod
    def generate_products(count: int = 15) -> List[Dict[str, Any]]:
        """Gera dados de produtos."""
        logger.info(f"Gerando {count} produtos...")
        products = []
        
        for i in range(count):
            product = {
                "produto_id": f"PROD_{i+1:04d}",
                "nome_produto": random.choice(BankingDataGenerator.TIPOS_PRODUTO),
                "descricao": f"Produto bancário {i+1}",
                "taxa_juros": round(random.uniform(0, 25), 2),
                "comissao": round(random.uniform(0, 5), 2),
                "data_lancamento": BankingDataGenerator.generate_date(days_back=1825),
                "status": random.choice(["Ativo", "Inativo"])
            }
            products.append(product)
        
        logger.info(f"✓ {len(products)} produtos gerados")
        return products
    
    @staticmethod
    def generate_card_number() -> str:
        """Gera número de cartão simulado."""
        # Prefixo Visa (4) ou Mastercard (5)
        prefix = random.choice(["4", "5"])
        card_number = prefix + ''.join(str(random.randint(0, 9)) for _ in range(15))
        return f"{card_number[:4]}-{card_number[4:8]}-{card_number[8:12]}-{card_number[12:]}"
    
    @staticmethod
    def generate_date(days_back: int = 0, days_forward: int = 0) -> str:
        """Gera data simulada."""
        if days_back > 0:
            days = -random.randint(0, days_back)
        else:
            days = random.randint(0, days_forward)
        
        date = datetime.now() + timedelta(days=days)
        return date.strftime("%Y-%m-%d")
    
    @staticmethod
    def generate_datetime(hours_back: int = 0) -> str:
        """Gera datetime simulado."""
        hours = -random.randint(0, hours_back)
        dt = datetime.now() + timedelta(hours=hours)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    
    @staticmethod
    def generate_birth_date() -> str:
        """Gera data de nascimento simulada."""
        years_back = random.randint(18, 80)
        date = datetime.now() - timedelta(days=random.randint(0, years_back * 365))
        return date.strftime("%Y-%m-%d")


class SampleDataWriter:
    """Escritor de dados de amostra em arquivos."""
    
    @staticmethod
    def write_csv(data: List[Dict[str, Any]], filepath: str) -> None:
        """Escreve dados em arquivo CSV."""
        if not data:
            logger.warning(f"Nenhum dado para escrever em {filepath}")
            return
        
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        fieldnames = data[0].keys()
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        
        logger.info(f"✓ CSV escrito: {filepath} ({len(data)} linhas)")
    
    @staticmethod
    def write_json(data: List[Dict[str, Any]], filepath: str) -> None:
        """Escreve dados em arquivo JSON."""
        if not data:
            logger.warning(f"Nenhum dado para escrever em {filepath}")
            return
        
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"✓ JSON escrito: {filepath} ({len(data)} linhas)")


def generate_all_sample_data(
    output_dir: str = "./data/sample",
    runtime_profile: Optional[str] = None,
) -> Dict[str, str]:
    """
    Gera todos os dados de amostra.
    
    Args:
        output_dir: Diretório de saída
        
    Returns:
        Dict com caminhos dos arquivos gerados
    """
    profile = get_runtime_profile(runtime_profile)
    batch_counts = profile["batch"]

    logger.info("="*80)
    logger.info("INICIANDO GERAÇÃO DE DADOS SINTÉTICOS")
    logger.info("Runtime profile: %s", profile["id"])
    logger.info("Batch counts: %s", batch_counts)
    logger.info("="*80)
    
    # Gerar dados
    clients = BankingDataGenerator.generate_clients(count=batch_counts["clientes"])
    agencies = BankingDataGenerator.generate_agencies(count=batch_counts["agencias"])
    products = BankingDataGenerator.generate_products(count=batch_counts["produtos"])
    accounts = BankingDataGenerator.generate_accounts(
        clients,
        agencies,
        products,
        count_per_client=batch_counts["accounts_per_client"],
    )
    cards = BankingDataGenerator.generate_cards(
        accounts,
        count_per_account=batch_counts["cards_per_account"],
    )
    transactions = BankingDataGenerator.generate_transactions(
        accounts,
        cards,
        count=batch_counts["transacoes"],
    )
    digital_events = BankingDataGenerator.generate_digital_events(
        clients,
        count=batch_counts["eventos_digitais_file"],
    )
    
    # Escrever arquivos
    os.makedirs(output_dir, exist_ok=True)
    
    files = {
        "clientes": os.path.join(output_dir, "clientes.csv"),
        "contas": os.path.join(output_dir, "contas.csv"),
        "cartoes": os.path.join(output_dir, "cartoes.csv"),
        "transacoes": os.path.join(output_dir, "transacoes.json"),
        "eventos_digitais": os.path.join(output_dir, "eventos_digitais.json"),
        "agencias": os.path.join(output_dir, "agencias.csv"),
        "produtos": os.path.join(output_dir, "produtos.csv"),
    }
    
    SampleDataWriter.write_csv(clients, files["clientes"])
    SampleDataWriter.write_csv(accounts, files["contas"])
    SampleDataWriter.write_csv(cards, files["cartoes"])
    SampleDataWriter.write_json(transactions, files["transacoes"])
    SampleDataWriter.write_json(digital_events, files["eventos_digitais"])
    SampleDataWriter.write_csv(agencies, files["agencias"])
    SampleDataWriter.write_csv(products, files["produtos"])
    
    logger.info("="*80)
    logger.info("GERAÇÃO CONCLUÍDA COM SUCESSO")
    logger.info("="*80)
    
    return files


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Data Master sample data.")
    parser.add_argument(
        "--output-dir",
        default="./data/sample",
        help="Output directory for generated sample files.",
    )
    parser.add_argument(
        "--runtime-profile",
        default=None,
        help="Runtime profile to use. Defaults to RUNTIME_PROFILE or presentation-demo.",
    )
    parser.add_argument(
        "--list-runtime-profiles",
        action="store_true",
        help="List available runtime profiles and exit.",
    )
    args = parser.parse_args()

    if args.list_runtime_profiles:
        for profile_name in list_runtime_profiles():
            print(profile_name)
        raise SystemExit(0)

    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Gerar dados
    files = generate_all_sample_data(
        output_dir=args.output_dir,
        runtime_profile=args.runtime_profile,
    )
    
    print("\n" + "="*80)
    print("Arquivos gerados:")
    for name, path in files.items():
        if os.path.exists(path):
            size = os.path.getsize(path)
            print(f"  ✓ {name}: {path} ({size:,} bytes)")
    print("="*80)
