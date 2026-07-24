"""Execute one Data Master pipeline stage inside a SparkApplication driver."""

import argparse
import json
import os
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Dict


REPO_ROOT = Path(__file__).resolve().parents[2]
for relative_path in (
    "jobs/data_generation",
    "jobs/bronze",
    "jobs/raw_vault",
    "jobs/business_vault",
    "jobs/common",
):
    sys.path.insert(0, str(REPO_ROOT / relative_path))


PIPELINE_STAGES = (
    "integration",
    "bronze",
    "hubs",
    "links",
    "satellites",
    "gold",
    "data-vault-gate",
    "masking-gate",
    "evidence",
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Execute a Data Master SparkApplication stage."
    )
    parser.add_argument("--stage", required=True, choices=PIPELINE_STAGES)
    parser.add_argument("--runtime-profile", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument(
        "--sample-data-path",
        default="/opt/spark/work-dir/data/sample",
    )
    parser.add_argument("--bronze-path", required=True)
    parser.add_argument("--raw-vault-path", required=True)
    parser.add_argument("--gold-path", required=True)
    parser.add_argument(
        "--monitoring-path",
        default="s3a://lakehouse/monitoring",
    )
    return parser.parse_args()


def _configure_environment(args: argparse.Namespace) -> None:
    values = {
        "RUNTIME_PROFILE": args.runtime_profile,
        "DM_RUNTIME_PROFILE": args.runtime_profile,
        "SAMPLE_DATA_PATH": args.sample_data_path,
        "BRONZE_PATH": args.bronze_path,
        "RAW_VAULT_PATH": args.raw_vault_path,
        "GOLD_PATH": args.gold_path,
        "MONITORING_PATH": args.monitoring_path,
        "SPARK_JARS_PACKAGES": "",
    }
    for name, value in values.items():
        os.environ[name] = value


def _assert_success(stage: str, result: Dict[str, Any]) -> Dict[str, Any]:
    if result.get("status") != "SUCCESS":
        raise RuntimeError(f"Stage {stage} failed: {json.dumps(result, default=str)}")
    return result


def _run_integration(spark, gold_path: str, batch_id: str) -> Dict[str, Any]:
    path = f"{gold_path}/_runtime_evidence/spark_integration/{batch_id}"
    source = spark.range(0, 100, 1, 2).repartition(2)
    source.write.format("delta").mode("overwrite").save(path)
    rows = spark.read.format("delta").load(path).count()
    if rows != 100:
        raise RuntimeError(f"Integration Delta row count mismatch: {rows}")
    print("SPARK_MINIO_CONNECTIVITY_STATUS=PASS", flush=True)
    print("SPARK_DELTA_WRITE_STATUS=PASS", flush=True)
    return {"status": "SUCCESS", "path": path, "rows": rows}


def _run_data_vault_gate(spark, raw_vault_path: str, gold_path: str):
    from data_vault_quality_gate import evaluate_configured_gate, render_gate_output

    result = evaluate_configured_gate(
        spark,
        raw_vault_path,
        gold_path,
        REPO_ROOT,
    )
    print(render_gate_output(result), flush=True)
    if result["status"] != "PASS":
        raise RuntimeError("Data Vault quality gate failed.")
    return {"status": "SUCCESS", "gate": result}


def _run_masking_gate(spark):
    from config import Config
    from delta_io import DeltaIO
    from run_gold_masking_smoke import (
        _masking_function_samples,
        _scan_high_confidence_secrets,
        _validate_gold_outputs,
    )

    gold = _validate_gold_outputs(spark, Config, DeltaIO)
    samples = _masking_function_samples()
    secrets = _scan_high_confidence_secrets(REPO_ROOT)
    failures = {
        "sample_failures": [
            name for name, sample in samples.items() if not sample["passed"]
        ],
        "forbidden_columns": gold["forbidden_columns"],
        "raw_pattern_hits": gold["raw_pattern_hits"],
        "protected_checks": {
            name: count
            for name, count in gold["protected_checks"].items()
            if count
        },
        "cliente_checks": {
            name: count
            for name, count in gold["cliente_checks"].items()
            if count
        },
        "risco_checks": {
            name: count
            for name, count in gold["risco_checks"].items()
            if count
        },
        "secret_findings": secrets,
    }
    if any(bool(value) for value in failures.values()):
        raise RuntimeError(f"Masking/security gate failed: {json.dumps(failures)}")
    print("MASKING_STATUS=PASS", flush=True)
    print("GOLD_PII_EXPOSURE_STATUS=PASS", flush=True)
    return {"status": "SUCCESS", "gold": gold, "samples": samples}


def _resolve_table_path(table_name: str, config: Any) -> str:
    """Normalize supported table configs without exposing configuration values."""
    if isinstance(config, os.PathLike):
        config = os.fspath(config)

    if isinstance(config, str):
        path = config
    elif isinstance(config, Mapping):
        path = config.get("path")
        if isinstance(path, os.PathLike):
            path = os.fspath(path)
    else:
        path = None

    if not isinstance(path, str) or not path.strip():
        if isinstance(config, Mapping):
            shape = "mapping keys=" + ",".join(sorted(str(key) for key in config))
        else:
            shape = type(config).__name__
        raise ValueError(
            f"Invalid evidence table configuration for {table_name}: {shape}; "
            "expected a non-empty path string/PathLike or a mapping with "
            "a non-empty 'path' field."
        )

    return path.strip()


def _count_tables(spark, table_paths: Mapping[str, Any]) -> int:
    from delta_io import DeltaIO

    if not isinstance(table_paths, Mapping) or not table_paths:
        raise ValueError("Evidence table registry must be a non-empty mapping.")

    total = 0
    for table_name, config in table_paths.items():
        path = _resolve_table_path(str(table_name), config)
        frame = DeltaIO.read_delta(spark, path)
        if frame is None:
            raise RuntimeError(f"Evidence table not readable: {table_name}")
        total += frame.count()
    return total


def _run_evidence(spark, batch_id: str) -> Dict[str, Any]:
    from config import Config

    counts = {
        "bronze": _count_tables(spark, Config.BRONZE_TABLES),
        "raw_vault_hubs": _count_tables(spark, Config.HUB_TABLES),
        "raw_vault_links": _count_tables(spark, Config.LINK_TABLES),
        "raw_vault_satellites": _count_tables(spark, Config.SATELLITE_TABLES),
        "gold": _count_tables(spark, Config.GOLD_TABLES),
    }
    if any(value <= 0 for value in counts.values()):
        raise RuntimeError(f"Evidence contains empty layers: {counts}")
    payload = {
        "status": "SUCCESS",
        "batch_id": batch_id,
        "lineage": "bronze->raw_vault->business_vault_latest->gold",
        "counts": counts,
        "storage": {
            "business_vault_path": Config.BUSINESS_VAULT_PATH,
            "gold_path": Config.GOLD_PATH,
            "gold_tables": dict(Config.GOLD_TABLES),
        },
    }
    print("PRESENTATION_EVIDENCE=" + json.dumps(payload, sort_keys=True), flush=True)
    print("PRESENTATION_EVIDENCE_STATUS=PASS", flush=True)
    return payload


def main() -> int:
    args = _parse_args()
    _configure_environment(args)

    from spark_session import SparkSessionFactory, create_spark_session

    spark = create_spark_session()
    try:
        if args.stage == "integration":
            result = _run_integration(spark, args.gold_path, args.batch_id)
        elif args.stage == "bronze":
            from load_bronze import run_bronze_pipeline

            result = _assert_success(
                args.stage,
                run_bronze_pipeline(
                    spark,
                    args.sample_data_path,
                    args.bronze_path,
                    args.batch_id,
                ),
            )
        elif args.stage == "hubs":
            from load_hubs import run_hubs_pipeline

            result = _assert_success(
                args.stage,
                run_hubs_pipeline(spark, args.bronze_path, args.batch_id),
            )
        elif args.stage == "links":
            from load_links import run_links_pipeline

            result = _assert_success(
                args.stage,
                run_links_pipeline(spark, args.bronze_path, args.batch_id),
            )
        elif args.stage == "satellites":
            from load_satellites import run_satellites_pipeline

            result = _assert_success(
                args.stage,
                run_satellites_pipeline(spark, args.bronze_path, args.batch_id),
            )
        elif args.stage == "gold":
            from load_gold import run_business_vault_pipeline

            result = _assert_success(
                args.stage,
                run_business_vault_pipeline(
                    spark,
                    args.raw_vault_path,
                    args.gold_path,
                    args.batch_id,
                ),
            )
        elif args.stage == "data-vault-gate":
            result = _run_data_vault_gate(
                spark,
                args.raw_vault_path,
                args.gold_path,
            )
        elif args.stage == "masking-gate":
            result = _run_masking_gate(spark)
        else:
            result = _run_evidence(spark, args.batch_id)

        print(
            "SPARK_STAGE_RESULT="
            + json.dumps(
                {
                    "stage": args.stage,
                    "batch_id": args.batch_id,
                    "status": result["status"],
                },
                sort_keys=True,
            ),
            flush=True,
        )
        return 0
    finally:
        SparkSessionFactory.stop()


if __name__ == "__main__":
    raise SystemExit(main())
