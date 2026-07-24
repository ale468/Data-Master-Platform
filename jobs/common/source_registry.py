"""
Source registry and Bronze contract for Data Master batch ingestion.

This module is intentionally free of Spark imports so the contract can be
validated in lightweight smoke tests before executing ingestion jobs.
"""
import argparse
import copy
import json
from typing import Any, Dict, Iterable, List, Mapping, Optional

try:
    from .runtime_profiles import get_runtime_profile
except ImportError:
    from runtime_profiles import get_runtime_profile


DEFAULT_SOURCE_SYSTEM = "banking_sample"
DEFAULT_OWNER = "data-engineering"
DEFAULT_RECORD_SOURCE = "banking_data_platform"
DEFAULT_SCHEMA_VERSION = "v1"
DEFAULT_INGESTION_MODE = "batch"
STREAMING_INGESTION_MODE = "streaming"
CDC_INGESTION_MODE = "cdc"


BRONZE_TECHNICAL_COLUMNS: List[str] = [
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


SOURCE_REGISTRY: Dict[str, Dict[str, Any]] = {
    "clientes": {
        "source_id": "banking_sample.clientes.v1",
        "source_system": DEFAULT_SOURCE_SYSTEM,
        "source_entity": "clientes",
        "owner": DEFAULT_OWNER,
        "record_source": DEFAULT_RECORD_SOURCE,
        "ingestion_mode": DEFAULT_INGESTION_MODE,
        "format": "csv",
        "file_name": "clientes.csv",
        "schema_version": DEFAULT_SCHEMA_VERSION,
        "primary_key": ["cliente_id"],
        "required_columns": [
            "cliente_id",
            "nome",
            "cpf",
            "email",
            "telefone",
            "data_nascimento",
            "estado",
            "cidade",
            "endereco",
            "data_cadastro",
        ],
        "sensitive_columns": ["nome", "cpf", "email", "telefone", "endereco"],
        "profile_volume_key": "clientes",
    },
    "contas": {
        "source_id": "banking_sample.contas.v1",
        "source_system": DEFAULT_SOURCE_SYSTEM,
        "source_entity": "contas",
        "owner": DEFAULT_OWNER,
        "record_source": DEFAULT_RECORD_SOURCE,
        "ingestion_mode": DEFAULT_INGESTION_MODE,
        "format": "csv",
        "file_name": "contas.csv",
        "schema_version": DEFAULT_SCHEMA_VERSION,
        "primary_key": ["conta_id"],
        "required_columns": [
            "conta_id",
            "cliente_id",
            "agencia_id",
            "produto_id",
            "tipo_conta",
            "agencia",
            "numero_conta",
            "saldo",
            "limite",
            "data_abertura",
            "status",
        ],
        "sensitive_columns": ["numero_conta"],
        "profile_volume_rule": "clientes * [1, accounts_per_client]",
    },
    "cartoes": {
        "source_id": "banking_sample.cartoes.v1",
        "source_system": DEFAULT_SOURCE_SYSTEM,
        "source_entity": "cartoes",
        "owner": DEFAULT_OWNER,
        "record_source": DEFAULT_RECORD_SOURCE,
        "ingestion_mode": DEFAULT_INGESTION_MODE,
        "format": "csv",
        "file_name": "cartoes.csv",
        "schema_version": DEFAULT_SCHEMA_VERSION,
        "primary_key": ["cartao_id"],
        "required_columns": [
            "cartao_id",
            "conta_id",
            "numero_cartao",
            "tipo_cartao",
            "bandeira",
            "data_emissao",
            "data_expiracao",
            "cvv",
            "status",
        ],
        "sensitive_columns": ["numero_cartao", "cvv"],
        "profile_volume_rule": "contas * [0, cards_per_account]",
    },
    "transacoes": {
        "source_id": "banking_sample.transacoes.v1",
        "source_system": DEFAULT_SOURCE_SYSTEM,
        "source_entity": "transacoes",
        "owner": DEFAULT_OWNER,
        "record_source": DEFAULT_RECORD_SOURCE,
        "ingestion_mode": DEFAULT_INGESTION_MODE,
        "format": "json",
        "file_name": "transacoes.json",
        "schema_version": DEFAULT_SCHEMA_VERSION,
        "primary_key": ["transacao_id"],
        "required_columns": [
            "transacao_id",
            "conta_id",
            "cartao_id",
            "tipo_transacao",
            "valor",
            "data_transacao",
            "data_liquidacao",
            "status",
            "descricao",
        ],
        "sensitive_columns": [],
        "profile_volume_key": "transacoes",
    },
    "eventos_digitais": {
        "source_id": "banking_sample.eventos_digitais.v1",
        "source_system": DEFAULT_SOURCE_SYSTEM,
        "source_entity": "eventos_digitais",
        "owner": DEFAULT_OWNER,
        "record_source": DEFAULT_RECORD_SOURCE,
        "ingestion_mode": DEFAULT_INGESTION_MODE,
        "format": "json",
        "file_name": "eventos_digitais.json",
        "schema_version": DEFAULT_SCHEMA_VERSION,
        "primary_key": ["evento_id"],
        "required_columns": [
            "evento_id",
            "cliente_id",
            "canal_id",
            "canal",
            "tipo_evento",
            "timestamp",
            "resultado",
            "detalhes",
        ],
        "sensitive_columns": [],
        "profile_volume_key": "eventos_digitais_file",
        "roadmap_note": "File-based digital events remain batch input here; streaming is governed by DM-ING-003.",
    },
    "eventos_digitais_streaming": {
        "source_id": "banking_sample.eventos_digitais.streaming.v1",
        "source_system": DEFAULT_SOURCE_SYSTEM,
        "source_entity": "eventos_digitais",
        "owner": DEFAULT_OWNER,
        "record_source": DEFAULT_RECORD_SOURCE,
        "ingestion_mode": STREAMING_INGESTION_MODE,
        "format": "json",
        "file_name": "streaming/events/*.jsonl",
        "schema_version": DEFAULT_SCHEMA_VERSION,
        "primary_key": ["evento_id"],
        "required_columns": [
            "evento_id",
            "cliente_id",
            "canal_id",
            "canal",
            "tipo_evento",
            "timestamp",
            "resultado",
            "detalhes",
        ],
        "sensitive_columns": [],
        "profile_volume_key": "streaming.demo_event_count",
        "roadmap_note": (
            "DM-ING-003 proves a local Spark Structured Streaming microbatch "
            "without requiring Kafka in the case demo."
        ),
    },
    "clientes_cdc": {
        "source_id": "banking_core.clientes.cdc.v1",
        "source_system": "banking_core",
        "source_entity": "core_clientes",
        "owner": DEFAULT_OWNER,
        "record_source": DEFAULT_RECORD_SOURCE,
        "ingestion_mode": CDC_INGESTION_MODE,
        "format": "json",
        "file_name": "cdc/clientes/*.jsonl",
        "schema_version": DEFAULT_SCHEMA_VERSION,
        "primary_key": ["cliente_id"],
        "required_columns": [
            "cdc_event_id",
            "cdc_operation",
            "cdc_event_timestamp",
            "cdc_transaction_id",
            "cdc_sequence",
            "source_database",
            "source_table",
            "primary_key",
            "before_image",
            "after_image",
        ],
        "sensitive_columns": [],
        "profile_volume_key": "cdc.demo_event_count",
        "roadmap_note": (
            "DM-ING-004 proves local CDC semantics without claiming "
            "Debezium or Airbyte runtime operation."
        ),
    },
    "agencias": {
        "source_id": "banking_sample.agencias.v1",
        "source_system": DEFAULT_SOURCE_SYSTEM,
        "source_entity": "agencias",
        "owner": DEFAULT_OWNER,
        "record_source": DEFAULT_RECORD_SOURCE,
        "ingestion_mode": DEFAULT_INGESTION_MODE,
        "format": "csv",
        "file_name": "agencias.csv",
        "schema_version": DEFAULT_SCHEMA_VERSION,
        "primary_key": ["agencia_id"],
        "required_columns": [
            "agencia_id",
            "numero_agencia",
            "nome",
            "estado",
            "cidade",
            "endereco",
            "telefone",
            "gerente",
            "data_inauguracao",
        ],
        "sensitive_columns": ["telefone", "gerente", "endereco"],
        "profile_volume_key": "agencias",
    },
    "produtos": {
        "source_id": "banking_sample.produtos.v1",
        "source_system": DEFAULT_SOURCE_SYSTEM,
        "source_entity": "produtos",
        "owner": DEFAULT_OWNER,
        "record_source": DEFAULT_RECORD_SOURCE,
        "ingestion_mode": DEFAULT_INGESTION_MODE,
        "format": "csv",
        "file_name": "produtos.csv",
        "schema_version": DEFAULT_SCHEMA_VERSION,
        "primary_key": ["produto_id"],
        "required_columns": [
            "produto_id",
            "nome_produto",
            "descricao",
            "taxa_juros",
            "comissao",
            "data_lancamento",
            "status",
        ],
        "sensitive_columns": [],
        "profile_volume_key": "produtos",
    },
}


def list_registered_sources(ingestion_mode: Optional[str] = None) -> List[str]:
    """Return registered source names, optionally filtered by ingestion mode."""
    if ingestion_mode is None:
        return list(SOURCE_REGISTRY.keys())

    return [
        source_name
        for source_name, contract in SOURCE_REGISTRY.items()
        if contract["ingestion_mode"] == ingestion_mode
    ]


def get_source_contract(source_name: str) -> Dict[str, Any]:
    """Return a copy of a registered source contract or raise a clear error."""
    if source_name not in SOURCE_REGISTRY:
        available = ", ".join(list_registered_sources())
        raise ValueError(
            f"Unregistered source '{source_name}'. Available sources: {available}."
        )

    return copy.deepcopy(SOURCE_REGISTRY[source_name])


def get_bronze_contract() -> Dict[str, Any]:
    """Return the common Bronze contract for batch sources."""
    return {
        "technical_columns": list(BRONZE_TECHNICAL_COLUMNS),
        "required_metadata": {
            "source_system": "Source system identifier.",
            "source_entity": "Source entity/table/file logical name.",
            "ingestion_mode": "batch, streaming, cdc or connector.",
            "schema_version": "Contract schema version.",
            "batch_id": "Batch execution identifier.",
            "run_id": "Execution identifier aligned with batch_id for batch runs.",
            "source_record_count": "Rows read from the source before Bronze write.",
        },
        "allowed_ingestion_modes": ["batch", "streaming", "connector", "cdc"],
        "roadmap_ingestion_modes": [],
    }


def validate_required_columns(
    source_name: str,
    actual_columns: Iterable[str],
) -> Dict[str, Any]:
    """Validate that a source payload includes the registered required columns."""
    contract = get_source_contract(source_name)
    actual = set(actual_columns)
    expected = set(contract["required_columns"])
    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected)

    return {
        "source_name": source_name,
        "source_id": contract["source_id"],
        "passed": not missing,
        "missing_columns": missing,
        "unexpected_columns": unexpected,
        "required_columns": contract["required_columns"],
    }


def assert_required_columns(source_name: str, actual_columns: Iterable[str]) -> None:
    """Raise ValueError when required columns are missing."""
    result = validate_required_columns(source_name, actual_columns)
    if not result["passed"]:
        raise ValueError(
            f"Source '{source_name}' is missing required columns: "
            f"{', '.join(result['missing_columns'])}."
        )


def validate_bronze_metadata_columns(columns: Iterable[str]) -> Dict[str, Any]:
    """Validate that a Bronze DataFrame has every mandatory metadata column."""
    actual = set(columns)
    expected = set(BRONZE_TECHNICAL_COLUMNS)
    missing = sorted(expected - actual)

    return {
        "passed": not missing,
        "missing_columns": missing,
        "required_columns": list(BRONZE_TECHNICAL_COLUMNS),
    }


def assert_bronze_metadata_columns(columns: Iterable[str]) -> None:
    """Raise ValueError when Bronze metadata columns are missing."""
    result = validate_bronze_metadata_columns(columns)
    if not result["passed"]:
        raise ValueError(
            "Bronze metadata columns missing: "
            f"{', '.join(result['missing_columns'])}."
        )


def expected_volume_for_profile(
    source_name: str,
    runtime_profile_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Return the expected count or range for a source under a runtime profile."""
    profile = get_runtime_profile(runtime_profile_name)
    batch = profile["batch"]
    contract = get_source_contract(source_name)

    volume_key = contract.get("profile_volume_key")
    if volume_key:
        if "." in volume_key:
            section_name, field_name = volume_key.split(".", 1)
            expected = profile[section_name][field_name]
        else:
            expected = batch[volume_key]
        return {
            "source_name": source_name,
            "runtime_profile": profile["id"],
            "expected_min": expected,
            "expected_max": expected,
            "rule": volume_key,
        }

    if source_name == "contas":
        return {
            "source_name": source_name,
            "runtime_profile": profile["id"],
            "expected_min": batch["clientes"],
            "expected_max": batch["clientes"] * batch["accounts_per_client"],
            "rule": contract["profile_volume_rule"],
        }

    if source_name == "cartoes":
        max_accounts = batch["clientes"] * batch["accounts_per_client"]
        return {
            "source_name": source_name,
            "runtime_profile": profile["id"],
            "expected_min": 0,
            "expected_max": max_accounts * batch["cards_per_account"],
            "rule": contract["profile_volume_rule"],
        }

    return {
        "source_name": source_name,
        "runtime_profile": profile["id"],
        "expected_min": None,
        "expected_max": None,
        "rule": contract.get("profile_volume_rule", "not_defined"),
    }


def validate_registry(runtime_profile_name: Optional[str] = None) -> Dict[str, Any]:
    """Validate registry completeness and profile volume expectations."""
    sources = []
    failures = []

    for source_name in list_registered_sources(DEFAULT_INGESTION_MODE):
        contract = get_source_contract(source_name)
        volume = expected_volume_for_profile(source_name, runtime_profile_name)
        missing_contract_fields = [
            field
            for field in (
                "source_id",
                "source_system",
                "source_entity",
                "owner",
                "record_source",
                "ingestion_mode",
                "format",
                "file_name",
                "schema_version",
                "primary_key",
                "required_columns",
            )
            if not contract.get(field)
        ]
        if missing_contract_fields:
            failures.append(
                {
                    "source_name": source_name,
                    "missing_contract_fields": missing_contract_fields,
                }
            )

        sources.append(
            {
                "source_name": source_name,
                "source_id": contract["source_id"],
                "format": contract["format"],
                "file_name": contract["file_name"],
                "schema_version": contract["schema_version"],
                "required_columns": len(contract["required_columns"]),
                "technical_columns": len(BRONZE_TECHNICAL_COLUMNS),
                "expected_volume": volume,
            }
        )

    return {
        "passed": not failures,
        "source_count": len(sources),
        "sources": sources,
        "failures": failures,
        "bronze_contract": get_bronze_contract(),
    }


def _json_default(value: Any) -> str:
    return str(value)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate Data Master source registry and Bronze contract.")
    parser.add_argument(
        "--runtime-profile",
        default=None,
        help="Runtime profile used to resolve volume expectations.",
    )
    parser.add_argument(
        "--source",
        default=None,
        help="Optional source name to print a single contract.",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output.",
    )
    args = parser.parse_args()

    if args.source:
        output = {
            "contract": get_source_contract(args.source),
            "expected_volume": expected_volume_for_profile(
                args.source,
                runtime_profile_name=args.runtime_profile,
            ),
            "bronze_contract": get_bronze_contract(),
        }
    else:
        output = validate_registry(runtime_profile_name=args.runtime_profile)

    print(
        json.dumps(
            output,
            ensure_ascii=False,
            default=_json_default,
            indent=2 if args.pretty else None,
            sort_keys=True,
        )
    )

    return 0 if output.get("passed", True) else 1


if __name__ == "__main__":
    raise SystemExit(main())
