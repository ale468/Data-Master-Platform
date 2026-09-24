"""Deterministic three-batch source lifecycle for incremental validation.

The lifecycle is intentionally synthetic and bounded.  It reuses the canonical
sample generator, then applies a small set of named mutations so tests and
runtime evidence can assert exact business outcomes without depending on
randomly selected records.
"""

import argparse
import csv
import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

try:
    from jobs.common.runtime_profiles import get_runtime_profile
    from jobs.common.source_registry import get_source_contract, list_registered_sources
    from jobs.data_generation.generate_banking_sample_data import (
        SampleDataWriter,
        generate_all_sample_data,
    )
except ImportError:
    import sys

    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir))
    sys.path.insert(0, str(module_dir.parent / "common"))
    from generate_banking_sample_data import SampleDataWriter, generate_all_sample_data
    from runtime_profiles import get_runtime_profile
    from source_registry import get_source_contract, list_registered_sources


LIFECYCLE_SCHEMA_VERSION = 1
LIFECYCLE_GENERATOR_VERSION = "deterministic-multibatch-v1"
DEFAULT_REFERENCE_TIME = "2026-01-01T00:00:00+00:00"
SOURCE_BATCHES = ("batch-1", "batch-2", "batch-3")

CHANGED_CUSTOMER_ID = "CLI_000001"
UNCHANGED_CUSTOMER_ID = "CLI_000003"
NEW_CUSTOMER_ID = "CLI_MB_NEW_000001"
NEW_CUSTOMER_ACCOUNT_ID = "ACC_MB_NEW_00000001"
EXISTING_CUSTOMER_ACCOUNT_ID = "ACC_MB_REL_00000001"
BATCH_2_TRANSACTION_ID = "TXN_MB_B2_000000001"
LATE_TRANSACTION_ID = "TXN_MB_LATE_000000001"


def _parse_reference_time(value: Optional[str]) -> datetime:
    normalized = (value or DEFAULT_REFERENCE_TIME).replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _business_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _read_csv(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _read_json(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, list):
        raise ValueError(f"Expected JSON array in {path.name}.")
    return payload


def _read_sources(files: Mapping[str, str]) -> Dict[str, List[Dict[str, Any]]]:
    result: Dict[str, List[Dict[str, Any]]] = {}
    for source_name, raw_path in files.items():
        path = Path(raw_path)
        contract = get_source_contract(source_name)
        if contract["format"] == "csv":
            result[source_name] = _read_csv(path)
        else:
            result[source_name] = _read_json(path)
    return result


def _write_sources(
    records: Mapping[str, List[Dict[str, Any]]],
    output_dir: Path,
) -> Dict[str, str]:
    files: Dict[str, str] = {}
    for source_name in list_registered_sources("batch"):
        contract = get_source_contract(source_name)
        path = output_dir / contract["file_name"]
        rows = records[source_name]
        if contract["format"] == "csv":
            SampleDataWriter.write_csv(rows, str(path))
        else:
            SampleDataWriter.write_json(rows, str(path))
        files[source_name] = str(path)
    return files


def _first(records: Mapping[str, List[Dict[str, Any]]], source: str) -> Dict[str, Any]:
    try:
        return records[source][0]
    except (KeyError, IndexError) as exc:
        raise ValueError(f"Lifecycle requires at least one {source} record.") from exc


def _apply_batch_2_changes(
    records: Dict[str, List[Dict[str, Any]]],
    reference_time: datetime,
    include_incremental_facts: bool = True,
) -> None:
    customers = records["clientes"]
    changed = next(
        (item for item in customers if item["cliente_id"] == CHANGED_CUSTOMER_ID),
        None,
    )
    if changed is None:
        raise ValueError(f"Required customer not generated: {CHANGED_CUSTOMER_ID}")
    changed["email"] = "cliente.alterado@example.invalid"
    changed["endereco"] = "Rua Multibatch, 200"

    template_customer = dict(_first(records, "clientes"))
    template_customer.update(
        {
            "cliente_id": NEW_CUSTOMER_ID,
            "nome": "Cliente Multibatch",
            "cpf": "900.000.000-00",
            "email": "cliente.multibatch@example.invalid",
            "telefone": "+55 (11) 90000-0000",
            "data_nascimento": "1990-01-01",
            "estado": "SP",
            "cidade": "São Paulo",
            "endereco": "Rua Sintética, 100",
            "data_cadastro": reference_time.strftime("%Y-%m-%d"),
        }
    )
    customers.append(template_customer)

    agency = _first(records, "agencias")
    product = _first(records, "produtos")
    template_account = dict(_first(records, "contas"))
    new_account = dict(template_account)
    new_account.update(
        {
            "conta_id": NEW_CUSTOMER_ACCOUNT_ID,
            "cliente_id": NEW_CUSTOMER_ID,
            "agencia_id": agency["agencia_id"],
            "produto_id": product["produto_id"],
            "agencia": agency["numero_agencia"],
            "numero_conta": "900001",
            "saldo": "2500.00",
            "limite": "1000.00",
            "data_abertura": (reference_time + timedelta(days=2)).strftime("%Y-%m-%d"),
            "status": "Ativa",
        }
    )
    relationship_account = dict(template_account)
    relationship_account.update(
        {
            "conta_id": EXISTING_CUSTOMER_ACCOUNT_ID,
            "cliente_id": UNCHANGED_CUSTOMER_ID,
            "agencia_id": agency["agencia_id"],
            "produto_id": product["produto_id"],
            "agencia": agency["numero_agencia"],
            "numero_conta": "900002",
            "saldo": "500.00",
            "limite": "250.00",
            "data_abertura": (reference_time + timedelta(days=2)).strftime("%Y-%m-%d"),
            "status": "Ativa",
        }
    )
    records["contas"].extend([new_account, relationship_account])

    if include_incremental_facts:
        records["transacoes"].append(
            {
                "transacao_id": BATCH_2_TRANSACTION_ID,
                "conta_id": NEW_CUSTOMER_ACCOUNT_ID,
                "cartao_id": None,
                "tipo_transacao": "Depósito",
                "valor": 750.0,
                "data_transacao": _business_datetime(reference_time + timedelta(days=2, hours=-1)),
                "data_liquidacao": (reference_time + timedelta(days=2)).strftime("%Y-%m-%d"),
                "status": "Concluída",
                "descricao": "Transação sintética do Batch 2",
            }
        )
        records["eventos_digitais"].append(
            {
                "evento_id": "EVT_MB_B2_000000001",
                "cliente_id": NEW_CUSTOMER_ID,
                "canal_id": "CANAL_001",
                "canal": "Mobile App",
                "tipo_evento": "Login",
                "timestamp": _business_datetime(reference_time + timedelta(days=2)),
                "resultado": "Sucesso",
                "detalhes": "Evento sintético do Batch 2",
            }
        )


def _apply_batch_3_changes(
    records: Dict[str, List[Dict[str, Any]]],
    reference_time: datetime,
) -> None:
    records["transacoes"].append(
        {
            "transacao_id": LATE_TRANSACTION_ID,
            "conta_id": NEW_CUSTOMER_ACCOUNT_ID,
            "cartao_id": None,
            "tipo_transacao": "Pagamento",
            "valor": 125.5,
            "data_transacao": _business_datetime(reference_time + timedelta(days=1)),
            "data_liquidacao": (reference_time + timedelta(days=1)).strftime("%Y-%m-%d"),
            "status": "Concluída",
            "descricao": "Transação sintética com chegada tardia",
        }
    )
    records["eventos_digitais"].append(
        {
            "evento_id": "EVT_MB_B3_000000001",
            "cliente_id": NEW_CUSTOMER_ID,
            "canal_id": "CANAL_001",
            "canal": "Mobile App",
            "tipo_evento": "Consulta Saldo",
            "timestamp": _business_datetime(reference_time + timedelta(days=4)),
            "resultado": "Sucesso",
            "detalhes": "Evento sintético do Batch 3",
        }
    )


def _assert_unique_primary_keys(records: Mapping[str, List[Dict[str, Any]]]) -> None:
    for source_name in list_registered_sources("batch"):
        contract = get_source_contract(source_name)
        primary_key = contract["primary_key"]
        seen = set()
        for row in records[source_name]:
            identity = tuple(str(row.get(column, "")) for column in primary_key)
            if identity in seen:
                raise ValueError(
                    f"Duplicate primary key in lifecycle source {source_name}: {identity}"
                )
            seen.add(identity)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _source_manifest(
    files: Mapping[str, str],
    records: Mapping[str, List[Dict[str, Any]]],
) -> Dict[str, Dict[str, Any]]:
    return {
        source_name: {
            "file_name": Path(path).name,
            "record_count": len(records[source_name]),
            "sha256": _sha256(Path(path)),
        }
        for source_name, path in sorted(files.items())
    }


def generate_lifecycle_batch(
    output_dir: str,
    source_batch: str,
    *,
    scenario_id: str,
    batch_id: str,
    runtime_profile: Optional[str] = None,
    seed: Optional[int] = None,
    reference_time: Optional[str] = None,
) -> Dict[str, Any]:
    """Generate one deterministic lifecycle batch and its immutable manifest."""
    if source_batch not in SOURCE_BATCHES:
        raise ValueError(
            f"Unknown source batch '{source_batch}'. Expected one of: "
            + ", ".join(SOURCE_BATCHES)
        )
    if not scenario_id or not batch_id:
        raise ValueError("scenario_id and batch_id must be non-empty.")

    profile = get_runtime_profile(runtime_profile)
    dataset = profile.get("dataset", {})
    effective_seed = int(seed if seed is not None else dataset.get("seed", 42))
    effective_reference_time = reference_time or dataset.get("reference_time") or DEFAULT_REFERENCE_TIME
    reference = _parse_reference_time(effective_reference_time)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    files = generate_all_sample_data(
        output_dir=str(output_path),
        runtime_profile=profile["id"],
        seed=effective_seed,
        reference_time=_iso(reference),
    )
    records = _read_sources(files)
    if source_batch in {"batch-2", "batch-3"}:
        records["transacoes"] = []
        records["eventos_digitais"] = []
        _apply_batch_2_changes(
            records,
            reference,
            include_incremental_facts=source_batch == "batch-2",
        )
    if source_batch == "batch-3":
        _apply_batch_3_changes(records, reference)
    _assert_unique_primary_keys(records)
    files = _write_sources(records, output_path)

    batch_offset = SOURCE_BATCHES.index(source_batch) * 2
    generated_at = reference + timedelta(days=batch_offset)
    manifest: Dict[str, Any] = {
        "schema_version": LIFECYCLE_SCHEMA_VERSION,
        "generator_version": LIFECYCLE_GENERATOR_VERSION,
        "data_classification": "synthetic",
        "contains_real_personal_data": False,
        "scenario_id": scenario_id,
        "batch_id": batch_id,
        "source_batch": source_batch,
        "seed": effective_seed,
        "reference_time": _iso(reference),
        "generated_at": _iso(generated_at),
        "sources": _source_manifest(files, records),
        "known_records": {
            "changed_customer_id": CHANGED_CUSTOMER_ID,
            "unchanged_customer_id": UNCHANGED_CUSTOMER_ID,
            "new_customer_id": NEW_CUSTOMER_ID,
            "new_customer_account_id": NEW_CUSTOMER_ACCOUNT_ID,
            "existing_customer_account_id": EXISTING_CUSTOMER_ACCOUNT_ID,
            "batch_2_transaction_id": BATCH_2_TRANSACTION_ID,
            "late_transaction_id": LATE_TRANSACTION_ID if source_batch == "batch-3" else None,
        },
    }
    manifest_path = output_path / "batch_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return {"files": files, "manifest": manifest, "manifest_path": str(manifest_path)}


def _main() -> int:
    parser = argparse.ArgumentParser(description="Generate a deterministic lifecycle batch.")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--source-batch", required=True, choices=SOURCE_BATCHES)
    parser.add_argument("--scenario-id", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--runtime-profile", default=None)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--reference-time", default=None)
    args = parser.parse_args()
    result = generate_lifecycle_batch(
        args.output_dir,
        args.source_batch,
        scenario_id=args.scenario_id,
        batch_id=args.batch_id,
        runtime_profile=args.runtime_profile,
        seed=args.seed,
        reference_time=args.reference_time,
    )
    print("MULTIBATCH_MANIFEST=" + json.dumps(result["manifest"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
