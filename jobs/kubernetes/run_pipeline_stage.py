"""Execute one Data Master pipeline stage inside a SparkApplication driver."""

import argparse
import json
import os
import sys
import tempfile
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
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--scenario-id", default="baseline")
    parser.add_argument(
        "--source-batch",
        default="static",
        choices=("static", "batch-1", "batch-2", "batch-3"),
    )
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


def _count_batch_rows(
    spark,
    table_paths: Mapping[str, Any],
    batch_id: str,
) -> int:
    from delta_io import DeltaIO

    total = 0
    for table_name, config in table_paths.items():
        path = _resolve_table_path(str(table_name), config)
        frame = DeltaIO.read_delta(spark, path)
        if frame is None:
            raise RuntimeError(f"Evidence table not readable: {table_name}")
        if "batch_id" not in frame.columns:
            raise RuntimeError(f"Evidence table lacks batch_id: {table_name}")
        total += frame.filter(frame["batch_id"] == batch_id).count()
    return total


def _one_hash_key(frame, business_key: str, value: str, hash_key: str):
    rows = frame.filter(frame[business_key] == value).select(hash_key).collect()
    if not rows:
        return None
    return rows[0][hash_key]


def _multibatch_evidence(
    spark,
    *,
    batch_id: str,
    run_id: str,
    scenario_id: str,
    source_batch: str,
) -> Dict[str, Any]:
    from config import Config
    from delta_io import DeltaIO
    from multibatch_lifecycle import (
        CHANGED_CUSTOMER_ID,
        EXISTING_CUSTOMER_ACCOUNT_ID,
        LATE_TRANSACTION_ID,
        NEW_CUSTOMER_ACCOUNT_ID,
        NEW_CUSTOMER_ID,
        UNCHANGED_CUSTOMER_ID,
    )

    hubs = {
        name: config["path"] for name, config in Config.HUB_TABLES.items()
    }
    links = {
        name: config["path"] for name, config in Config.LINK_TABLES.items()
    }
    satellites = {
        name: config["path"] for name, config in Config.SATELLITE_TABLES.items()
    }
    batch_counts = {
        "bronze": _count_batch_rows(spark, Config.BRONZE_TABLES, batch_id),
        "raw_vault_hubs": _count_batch_rows(spark, hubs, batch_id),
        "raw_vault_links": _count_batch_rows(spark, links, batch_id),
        "raw_vault_satellites": _count_batch_rows(spark, satellites, batch_id),
    }
    manifest_table = DeltaIO.read_delta(spark, Config.BRONZE_BATCH_MANIFEST_PATH)
    manifest_rows = (
        manifest_table
        .filter(manifest_table["batch_id"] == batch_id)
        .select("manifest_sha256", "manifest_json")
        .collect()
    )
    if len(manifest_rows) != 1:
        raise RuntimeError("Multibatch evidence requires exactly one batch manifest.")
    source_manifest = json.loads(manifest_rows[0]["manifest_json"])

    customer_hub = DeltaIO.read_delta(
        spark, Config.HUB_TABLES["hub_cliente"]["path"]
    )
    account_hub = DeltaIO.read_delta(
        spark, Config.HUB_TABLES["hub_conta"]["path"]
    )
    customer_satellite = DeltaIO.read_delta(
        spark,
        Config.SATELLITE_TABLES["sat_cliente_dados_cadastrais"]["path"],
    )
    customer_account_link = DeltaIO.read_delta(
        spark, Config.LINK_TABLES["link_cliente_conta"]["path"]
    )
    changed_hk = _one_hash_key(
        customer_hub, "cliente_id", CHANGED_CUSTOMER_ID, "hk_cliente"
    )
    unchanged_hk = _one_hash_key(
        customer_hub, "cliente_id", UNCHANGED_CUSTOMER_ID, "hk_cliente"
    )
    new_customer_hk = _one_hash_key(
        customer_hub, "cliente_id", NEW_CUSTOMER_ID, "hk_cliente"
    )
    new_account_hk = _one_hash_key(
        account_hub, "conta_id", NEW_CUSTOMER_ACCOUNT_ID, "hk_conta"
    )
    relationship_account_hk = _one_hash_key(
        account_hub,
        "conta_id",
        EXISTING_CUSTOMER_ACCOUNT_ID,
        "hk_conta",
    )

    def history_count(hash_key):
        if hash_key is None:
            return 0
        return customer_satellite.filter(
            customer_satellite["hk_cliente"] == hash_key
        ).count()

    relation_count = 0
    if new_customer_hk and new_account_hk:
        relation_count = customer_account_link.filter(
            (customer_account_link["hk_cliente"] == new_customer_hk)
            & (customer_account_link["hk_conta"] == new_account_hk)
        ).count()

    existing_relation_count = 0
    if unchanged_hk and relationship_account_hk:
        existing_relation_count = customer_account_link.filter(
            (customer_account_link["hk_cliente"] == unchanged_hk)
            & (customer_account_link["hk_conta"] == relationship_account_hk)
        ).count()

    late_arrival = {
        "transaction_id": LATE_TRANSACTION_ID,
        "present": False,
    }
    transaction_hub = DeltaIO.read_delta(
        spark, Config.HUB_TABLES["hub_transacao"]["path"]
    )
    late_hk = _one_hash_key(
        transaction_hub, "transacao_id", LATE_TRANSACTION_ID, "hk_transacao"
    )
    if late_hk:
        transaction_satellite = DeltaIO.read_delta(
            spark,
            Config.SATELLITE_TABLES["sat_transacao_detalhes"]["path"],
        )
        late_rows = (
            transaction_satellite
            .filter(transaction_satellite["hk_transacao"] == late_hk)
            .select("data_transacao", "load_datetime", "batch_id", "run_id")
            .collect()
        )
        if len(late_rows) != 1:
            raise RuntimeError("Late-arriving transaction history must contain one row.")
        late = late_rows[0]
        late_arrival = {
            "transaction_id": LATE_TRANSACTION_ID,
            "present": True,
            "event_timestamp": str(late["data_transacao"]),
            "load_timestamp": str(late["load_datetime"]),
            "batch_id": late["batch_id"],
            "run_id": late["run_id"],
        }

    gold_rows = _count_tables(spark, Config.GOLD_TABLES)
    return {
        "schema_version": 1,
        "scenario_id": scenario_id,
        "source_batch": source_batch,
        "batch_id": batch_id,
        "run_id": run_id,
        "batch_counts": batch_counts,
        "source_manifest": {
            "generator_version": source_manifest["generator_version"],
            "generated_at": source_manifest["generated_at"],
            "manifest_sha256": manifest_rows[0]["manifest_sha256"],
            "source_counts": {
                name: value["record_count"]
                for name, value in source_manifest["sources"].items()
            },
        },
        "history": {
            "changed_customer_versions": history_count(changed_hk),
            "unchanged_customer_versions": history_count(unchanged_hk),
            "new_customer_present": new_customer_hk is not None,
            "new_customer_account_present": new_account_hk is not None,
            "new_customer_relationship_rows": relation_count,
            "existing_customer_new_relationship_rows": existing_relation_count,
        },
        "late_arrival": late_arrival,
        "gold_rows": gold_rows,
    }


def _run_evidence(
    spark,
    batch_id: str,
    run_id: str = None,
    scenario_id: str = "baseline",
    source_batch: str = "static",
) -> Dict[str, Any]:
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
        "run_id": run_id or batch_id,
        "lineage": "bronze->raw_vault->business_vault_latest->gold",
        "counts": counts,
        "storage": {
            "business_vault_path": Config.BUSINESS_VAULT_PATH,
            "gold_path": Config.GOLD_PATH,
            "gold_tables": dict(Config.GOLD_TABLES),
        },
    }
    if source_batch != "static":
        payload["multibatch"] = _multibatch_evidence(
            spark,
            batch_id=batch_id,
            run_id=run_id or batch_id,
            scenario_id=scenario_id,
            source_batch=source_batch,
        )
        print(
            "MULTIBATCH_EVIDENCE="
            + json.dumps(payload["multibatch"], sort_keys=True),
            flush=True,
        )
    print("PRESENTATION_EVIDENCE=" + json.dumps(payload, sort_keys=True), flush=True)
    print("PRESENTATION_EVIDENCE_STATUS=PASS", flush=True)
    return payload


def _run_bronze_stage(spark, args: argparse.Namespace) -> Dict[str, Any]:
    from load_bronze import run_bronze_pipeline

    run_id = args.run_id or args.batch_id
    if args.source_batch == "static":
        return run_bronze_pipeline(
            spark,
            args.sample_data_path,
            args.bronze_path,
            args.batch_id,
            run_id,
        )

    from multibatch_lifecycle import generate_lifecycle_batch

    with tempfile.TemporaryDirectory(prefix="data-master-multibatch-") as temp_dir:
        generated = generate_lifecycle_batch(
            temp_dir,
            args.source_batch,
            scenario_id=args.scenario_id,
            batch_id=args.batch_id,
            runtime_profile=args.runtime_profile,
        )
        print(
            "MULTIBATCH_MANIFEST="
            + json.dumps(generated["manifest"], sort_keys=True),
            flush=True,
        )
        return run_bronze_pipeline(
            spark,
            temp_dir,
            args.bronze_path,
            args.batch_id,
            run_id,
            batch_manifest=generated["manifest"],
            source_records=generated["records"],
        )


def main() -> int:
    args = _parse_args()
    _configure_environment(args)

    from spark_session import SparkSessionFactory, create_spark_session

    spark = create_spark_session()
    run_id = args.run_id or args.batch_id
    try:
        if args.stage == "integration":
            result = _run_integration(spark, args.gold_path, args.batch_id)
        elif args.stage == "bronze":
            result = _assert_success(
                args.stage,
                _run_bronze_stage(spark, args),
            )
        elif args.stage == "hubs":
            from load_hubs import run_hubs_pipeline

            result = _assert_success(
                args.stage,
                run_hubs_pipeline(
                    spark, args.bronze_path, args.batch_id, run_id
                ),
            )
        elif args.stage == "links":
            from load_links import run_links_pipeline

            result = _assert_success(
                args.stage,
                run_links_pipeline(
                    spark, args.bronze_path, args.batch_id, run_id
                ),
            )
        elif args.stage == "satellites":
            from load_satellites import run_satellites_pipeline

            result = _assert_success(
                args.stage,
                run_satellites_pipeline(
                    spark, args.bronze_path, args.batch_id, run_id
                ),
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
                    run_id,
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
            result = _run_evidence(
                spark,
                args.batch_id,
                run_id,
                args.scenario_id,
                args.source_batch,
            )

        print(
            "SPARK_STAGE_RESULT="
            + json.dumps(
                {
                    "stage": args.stage,
                    "batch_id": args.batch_id,
                    "run_id": run_id,
                    "scenario_id": args.scenario_id,
                    "source_batch": args.source_batch,
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
