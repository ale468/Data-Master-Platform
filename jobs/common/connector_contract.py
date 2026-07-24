"""Connector ingestion contract for DM-CONN-001.

The module validates the shape of connector-produced payloads without claiming
that Airbyte, Debezium, Kafka Connect, or another external connector runtime is
installed. It proves the contract a connector must satisfy before data can be
mapped into the common Bronze metadata model.
"""
import argparse
import json
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional

try:
    from .source_registry import BRONZE_TECHNICAL_COLUMNS
except ImportError:
    from source_registry import BRONZE_TECHNICAL_COLUMNS


ALLOWED_CONNECTOR_TYPES = [
    "api",
    "saas",
    "jdbc_snapshot",
    "jdbc_incremental",
    "file",
    "event_stream",
    "custom",
]

ALLOWED_SYNC_MODES = [
    "snapshot",
    "incremental",
    "append",
]

CONNECTOR_REQUIRED_FIELDS = [
    "connector_name",
    "connector_type",
    "source_system",
    "source_entity",
    "sync_mode",
    "schema_version",
    "batch_id",
    "run_id",
    "emitted_at",
    "source_uri",
    "records",
]

CONNECTOR_OPTIONAL_FIELDS = [
    "cursor_field",
    "cursor_value",
    "watermark",
    "partition",
]

CONNECTOR_BRONZE_COLUMNS = [
    *BRONZE_TECHNICAL_COLUMNS,
    "connector_name",
    "connector_type",
    "sync_mode",
    "cursor_field",
    "cursor_value",
    "source_uri",
    "payload",
]


def get_connector_contract() -> Dict[str, Any]:
    """Return the connector envelope contract."""
    return {
        "required_fields": list(CONNECTOR_REQUIRED_FIELDS),
        "optional_fields": list(CONNECTOR_OPTIONAL_FIELDS),
        "allowed_connector_types": list(ALLOWED_CONNECTOR_TYPES),
        "allowed_sync_modes": list(ALLOWED_SYNC_MODES),
        "bronze_columns": list(CONNECTOR_BRONZE_COLUMNS),
        "explicit_limits": [
            "This contract does not install or claim Airbyte, Debezium, or Kafka Connect.",
            "CDC operation semantics are handled by DM-ING-004, not by this contract smoke.",
            "Connector payloads must still pass Bronze validation before demo promotion.",
        ],
    }


def sample_connector_batch(batch_id: Optional[str] = None) -> Dict[str, Any]:
    """Create a deterministic local connector envelope for smoke validation."""
    resolved_batch_id = batch_id or "connector_contract_smoke"
    emitted_at = "2026-07-05T10:00:00"
    return {
        "connector_name": "local-api-accounts-sample",
        "connector_type": "api",
        "source_system": "synthetic_external_api",
        "source_entity": "accounts",
        "sync_mode": "incremental",
        "cursor_field": "updated_at",
        "cursor_value": "2026-07-05T09:59:59",
        "schema_version": "v1",
        "batch_id": resolved_batch_id,
        "run_id": resolved_batch_id,
        "emitted_at": emitted_at,
        "source_uri": "connector://synthetic_external_api/accounts",
        "records": [
            {
                "external_account_id": "ext-0001",
                "status": "active",
                "updated_at": "2026-07-05T09:57:00",
                "balance_bucket": "low",
            },
            {
                "external_account_id": "ext-0002",
                "status": "blocked",
                "updated_at": "2026-07-05T09:58:00",
                "balance_bucket": "medium",
            },
            {
                "external_account_id": "ext-0003",
                "status": "active",
                "updated_at": "2026-07-05T09:59:00",
                "balance_bucket": "high",
            },
        ],
    }


def _missing_fields(payload: Mapping[str, Any], required_fields: Iterable[str]) -> List[str]:
    return [
        field
        for field in required_fields
        if field not in payload or payload[field] in (None, "")
    ]


def validate_connector_batch(batch: Mapping[str, Any]) -> Dict[str, Any]:
    """Validate a connector envelope and its local records."""
    missing_fields = _missing_fields(batch, CONNECTOR_REQUIRED_FIELDS)
    connector_type = batch.get("connector_type")
    sync_mode = batch.get("sync_mode")
    records = batch.get("records")
    cursor_field = batch.get("cursor_field")

    failures: List[str] = []
    if missing_fields:
        failures.append("missing_required_fields")
    if connector_type not in ALLOWED_CONNECTOR_TYPES:
        failures.append("invalid_connector_type")
    if sync_mode not in ALLOWED_SYNC_MODES:
        failures.append("invalid_sync_mode")
    if not isinstance(records, list) or not records:
        failures.append("records_empty_or_invalid")

    record_failures = []
    if isinstance(records, list):
        for index, record in enumerate(records):
            if not isinstance(record, dict):
                record_failures.append({"index": index, "reason": "record_not_object"})
                continue
            if sync_mode == "incremental" and cursor_field and cursor_field not in record:
                record_failures.append({"index": index, "reason": "cursor_field_missing"})

    if record_failures:
        failures.append("record_validation_failed")

    return {
        "passed": not failures,
        "failures": failures,
        "missing_fields": missing_fields,
        "record_failures": record_failures,
        "record_count": len(records) if isinstance(records, list) else 0,
        "connector_type": connector_type,
        "sync_mode": sync_mode,
        "cursor_field": cursor_field,
    }


def build_bronze_connector_records(batch: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Map connector records into a Bronze-compatible envelope."""
    records = batch["records"]
    load_datetime = batch["emitted_at"]
    ingestion_date = load_datetime.split("T")[0]
    record_count = len(records)

    bronze_records = []
    for record in records:
        bronze_records.append({
            "load_datetime": load_datetime,
            "record_source": "connector_ingestion_contract",
            "source_system": batch["source_system"],
            "source_entity": batch["source_entity"],
            "ingestion_mode": "connector",
            "schema_version": batch["schema_version"],
            "batch_id": batch["batch_id"],
            "run_id": batch["run_id"],
            "ingestion_date": ingestion_date,
            "source_file": batch["source_uri"],
            "source_record_count": record_count,
            "connector_name": batch["connector_name"],
            "connector_type": batch["connector_type"],
            "sync_mode": batch["sync_mode"],
            "cursor_field": batch.get("cursor_field"),
            "cursor_value": record.get(batch.get("cursor_field")) if batch.get("cursor_field") else batch.get("cursor_value"),
            "source_uri": batch["source_uri"],
            "payload": record,
        })

    return bronze_records


def validate_bronze_connector_records(records: List[Mapping[str, Any]]) -> Dict[str, Any]:
    """Validate Bronze-compatible connector records."""
    failures = []
    missing_by_record = []

    for index, record in enumerate(records):
        missing = _missing_fields(record, CONNECTOR_BRONZE_COLUMNS)
        if missing:
            missing_by_record.append({"index": index, "missing_columns": missing})

    if not records:
        failures.append("bronze_records_empty")
    if missing_by_record:
        failures.append("missing_bronze_columns")

    return {
        "passed": not failures,
        "failures": failures,
        "record_count": len(records),
        "missing_by_record": missing_by_record,
        "required_columns": list(CONNECTOR_BRONZE_COLUMNS),
    }


def run_connector_contract_smoke(batch_id: Optional[str] = None) -> Dict[str, Any]:
    """Run the local contract smoke and return evidence payload."""
    started_at = datetime.now()
    batch = sample_connector_batch(batch_id=batch_id)
    connector_validation = validate_connector_batch(batch)
    bronze_records = build_bronze_connector_records(batch) if connector_validation["passed"] else []
    bronze_validation = validate_bronze_connector_records(bronze_records)
    finished_at = datetime.now()

    failed = not connector_validation["passed"] or not bronze_validation["passed"]
    return {
        "status": "SUCCESS" if not failed else "FAILURE",
        "quality_gate_result": "Passed" if not failed else "Failed",
        "batch_id": batch["batch_id"],
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": round((finished_at - started_at).total_seconds(), 3),
        "contract": get_connector_contract(),
        "connector_validation": connector_validation,
        "bronze_validation": bronze_validation,
        "sample": {
            "connector_name": batch["connector_name"],
            "connector_type": batch["connector_type"],
            "source_system": batch["source_system"],
            "source_entity": batch["source_entity"],
            "sync_mode": batch["sync_mode"],
            "cursor_field": batch["cursor_field"],
            "source_uri": batch["source_uri"],
            "source_record_count": len(batch["records"]),
        },
        "tooling_status": {
            "airbyte": "not_installed_not_claimed",
            "debezium": "not_installed_not_claimed",
            "kafka_connect": "not_installed_not_claimed",
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate the local connector ingestion contract.")
    parser.add_argument("--batch-id", default=None, help="Optional batch/run id.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    args = parser.parse_args()

    result = run_connector_contract_smoke(batch_id=args.batch_id)
    print(
        json.dumps(
            result,
            ensure_ascii=False,
            indent=2 if args.pretty else None,
            sort_keys=True,
        )
    )
    return 0 if result["status"] == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
