"""
Validate registered batch sources and Bronze contract metadata without Spark.

This smoke validator is used before the full Bronze ingestion job to prove that
source files match the declarative source registry.
"""
import argparse
import csv
import json
import os
import sys
import tempfile
from typing import Any, Dict, List

COMMON_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "common"))
DATA_GENERATION_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data_generation"))
sys.path.insert(0, COMMON_PATH)
sys.path.insert(0, DATA_GENERATION_PATH)

from source_registry import (  # noqa: E402
    BRONZE_TECHNICAL_COLUMNS,
    get_source_contract,
    list_registered_sources,
    validate_bronze_metadata_columns,
    validate_registry,
    validate_required_columns,
)


def _read_columns_and_count(file_path: str, source_format: str) -> Dict[str, Any]:
    if source_format == "csv":
        with open(file_path, encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        columns = list(rows[0].keys()) if rows else []
        return {"columns": columns, "record_count": len(rows)}

    if source_format == "json":
        with open(file_path, encoding="utf-8") as handle:
            rows = json.load(handle)
        columns = list(rows[0].keys()) if rows else []
        return {"columns": columns, "record_count": len(rows)}

    raise ValueError(f"Unsupported source format: {source_format}")


def validate_sample_files(sample_data_path: str) -> Dict[str, Any]:
    source_results: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []

    for source_name in list_registered_sources("batch"):
        contract = get_source_contract(source_name)
        file_path = os.path.join(sample_data_path, contract["file_name"])

        if not os.path.exists(file_path):
            failures.append(
                {
                    "source_name": source_name,
                    "error": f"File not found: {file_path}",
                }
            )
            continue

        file_info = _read_columns_and_count(file_path, contract["format"])
        schema_result = validate_required_columns(source_name, file_info["columns"])

        if not schema_result["passed"]:
            failures.append(
                {
                    "source_name": source_name,
                    "missing_columns": schema_result["missing_columns"],
                }
            )

        source_results.append(
            {
                "source_name": source_name,
                "source_id": contract["source_id"],
                "format": contract["format"],
                "schema_version": contract["schema_version"],
                "record_count": file_info["record_count"],
                "required_columns": len(contract["required_columns"]),
                "schema_valid": schema_result["passed"],
            }
        )

    metadata_result = validate_bronze_metadata_columns(BRONZE_TECHNICAL_COLUMNS)
    if not metadata_result["passed"]:
        failures.append(
            {
                "source_name": "bronze_contract",
                "missing_columns": metadata_result["missing_columns"],
            }
        )

    return {
        "passed": not failures,
        "sample_data_path": sample_data_path,
        "source_count": len(source_results),
        "technical_columns": len(BRONZE_TECHNICAL_COLUMNS),
        "sources": source_results,
        "failures": failures,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate source registry and sample files.")
    parser.add_argument(
        "--runtime-profile",
        default=None,
        help="Runtime profile used for registry volume expectations.",
    )
    parser.add_argument(
        "--sample-data-path",
        default=None,
        help="Optional path with generated sample files to validate.",
    )
    parser.add_argument(
        "--generate-sample-data",
        action="store_true",
        help="Generate sample data in a temporary directory before validating files.",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output.",
    )
    args = parser.parse_args()

    registry_result = validate_registry(runtime_profile_name=args.runtime_profile)

    sample_data_path = args.sample_data_path
    if args.generate_sample_data:
        from generate_banking_sample_data import generate_all_sample_data  # noqa: WPS433

        sample_data_path = sample_data_path or os.path.join(
            tempfile.gettempdir(),
            "dm-source-contract-smoke",
        )
        generate_all_sample_data(
            output_dir=sample_data_path,
            runtime_profile=args.runtime_profile,
        )

    sample_result = None
    if sample_data_path:
        sample_result = validate_sample_files(sample_data_path)

    output = {
        "passed": registry_result["passed"]
        and (sample_result["passed"] if sample_result else True),
        "registry": registry_result,
        "sample_files": sample_result,
    }

    print(
        json.dumps(
            output,
            ensure_ascii=False,
            indent=2 if args.pretty else None,
            sort_keys=True,
        )
    )

    return 0 if output["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
