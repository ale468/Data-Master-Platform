"""Local CDC smoke runner for DM-ING-004.

The runner proves CDC semantics without requiring Debezium, Airbyte, Kafka, or
a source database. It creates a deterministic local changelog, writes a Bronze
CDC Delta table, and validates snapshot/insert/update/delete counts, ordering,
deduplication, and monitoring evidence.
"""
import argparse
import json
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[2]
for relative_path in ("jobs/common",):
    sys.path.insert(0, str(REPO_ROOT / relative_path))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run local CDC smoke.")
    parser.add_argument(
        "--runtime-profile",
        default=os.getenv("RUNTIME_PROFILE", os.getenv("DM_RUNTIME_PROFILE", "local-small")),
        help="Runtime profile used to validate CDC local-demo settings.",
    )
    parser.add_argument("--work-dir", default=None, help="Optional work directory.")
    parser.add_argument("--batch-id", default=None, help="Optional batch id.")
    parser.add_argument(
        "--log-level",
        default=os.getenv("SPARK_LOG_LEVEL", "WARN"),
        help="Spark log level for the smoke run.",
    )
    return parser.parse_args()


def _as_file_uri(path: Path) -> str:
    return path.resolve().as_uri()


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "asDict"):
        return {key: _json_safe(item) for key, item in value.asDict().items()}
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def _json_image(value: Optional[Dict[str, Any]]) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _cdc_events(batch_id: str) -> List[Dict[str, Any]]:
    source_database = "banking_core_local"
    source_table = "core_clientes"
    base_time = "2026-07-05T10:30:00"
    return [
        {
            "cdc_event_id": f"{batch_id}-000001",
            "cdc_operation": "snapshot",
            "cdc_event_timestamp": base_time,
            "cdc_transaction_id": f"{batch_id}-tx-snapshot",
            "cdc_sequence": 1,
            "source_database": source_database,
            "source_table": source_table,
            "primary_key": "cliente_id=cli-000001",
            "before_image": None,
            "after_image": _json_image({"cliente_id": "cli-000001", "status": "active", "segmento": "varejo"}),
        },
        {
            "cdc_event_id": f"{batch_id}-000002",
            "cdc_operation": "snapshot",
            "cdc_event_timestamp": "2026-07-05T10:30:01",
            "cdc_transaction_id": f"{batch_id}-tx-snapshot",
            "cdc_sequence": 2,
            "source_database": source_database,
            "source_table": source_table,
            "primary_key": "cliente_id=cli-000002",
            "before_image": None,
            "after_image": _json_image({"cliente_id": "cli-000002", "status": "active", "segmento": "alta_renda"}),
        },
        {
            "cdc_event_id": f"{batch_id}-000003",
            "cdc_operation": "insert",
            "cdc_event_timestamp": "2026-07-05T10:31:00",
            "cdc_transaction_id": f"{batch_id}-tx-insert",
            "cdc_sequence": 3,
            "source_database": source_database,
            "source_table": source_table,
            "primary_key": "cliente_id=cli-000003",
            "before_image": None,
            "after_image": _json_image({"cliente_id": "cli-000003", "status": "active", "segmento": "varejo"}),
        },
        {
            "cdc_event_id": f"{batch_id}-000004",
            "cdc_operation": "update",
            "cdc_event_timestamp": "2026-07-05T10:32:00",
            "cdc_transaction_id": f"{batch_id}-tx-update",
            "cdc_sequence": 4,
            "source_database": source_database,
            "source_table": source_table,
            "primary_key": "cliente_id=cli-000002",
            "before_image": _json_image({"cliente_id": "cli-000002", "status": "active", "segmento": "alta_renda"}),
            "after_image": _json_image({"cliente_id": "cli-000002", "status": "blocked", "segmento": "alta_renda"}),
        },
        {
            "cdc_event_id": f"{batch_id}-000005",
            "cdc_operation": "delete",
            "cdc_event_timestamp": "2026-07-05T10:33:00",
            "cdc_transaction_id": f"{batch_id}-tx-delete",
            "cdc_sequence": 5,
            "source_database": source_database,
            "source_table": source_table,
            "primary_key": "cliente_id=cli-000001",
            "before_image": _json_image({"cliente_id": "cli-000001", "status": "active", "segmento": "varejo"}),
            "after_image": None,
        },
    ]


def main() -> int:
    args = _parse_args()

    os.environ["RUNTIME_PROFILE"] = args.runtime_profile
    os.environ["DM_RUNTIME_PROFILE"] = args.runtime_profile
    os.environ["SPARK_LOG_LEVEL"] = args.log_level
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_IVY_DIR", "/tmp/.ivy2")

    work_dir = Path(args.work_dir or tempfile.mkdtemp(prefix="dm-cdc-smoke-"))
    bronze_path = _as_file_uri(work_dir / "bronze")
    cdc_bronze_path = f"{bronze_path}/clientes_cdc"
    monitoring_path = _as_file_uri(work_dir / "monitoring")
    batch_id = args.batch_id or "cdc_smoke_" + datetime.now().strftime("%Y%m%d%H%M%S")

    os.environ["BRONZE_PATH"] = bronze_path
    os.environ["MONITORING_PATH"] = monitoring_path

    from pyspark.sql import functions as F
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    from delta_io import DeltaIO
    from monitoring import MonitoringLogger
    from runtime_profiles import get_runtime_profile
    from source_registry import (
        BRONZE_TECHNICAL_COLUMNS,
        get_source_contract,
        validate_bronze_metadata_columns,
        validate_required_columns,
    )
    from spark_session import SparkSessionFactory, create_spark_session

    started_at = datetime.now()
    profile = get_runtime_profile(args.runtime_profile)
    cdc_profile = profile["cdc"]
    source_contract = get_source_contract("clientes_cdc")
    events = _cdc_events(batch_id)
    source_record_count = len(events)

    spark = create_spark_session()
    summary: Dict[str, Any] = {
        "runtime_profile": args.runtime_profile,
        "batch_id": batch_id,
        "work_dir": str(work_dir),
        "bronze_path": cdc_bronze_path,
        "monitoring_path": monitoring_path,
        "spark_version": spark.version,
        "status": "UNKNOWN",
    }

    try:
        schema = StructType([
            StructField("cdc_event_id", StringType(), False),
            StructField("cdc_operation", StringType(), False),
            StructField("cdc_event_timestamp", StringType(), False),
            StructField("cdc_transaction_id", StringType(), False),
            StructField("cdc_sequence", LongType(), False),
            StructField("source_database", StringType(), False),
            StructField("source_table", StringType(), False),
            StructField("primary_key", StringType(), False),
            StructField("before_image", StringType(), True),
            StructField("after_image", StringType(), True),
        ])
        df = spark.createDataFrame(events, schema=schema)
        df_bronze = (
            df
            .withColumn("load_datetime", F.current_timestamp())
            .withColumn("record_source", F.lit(source_contract["record_source"]))
            .withColumn("source_system", F.lit(source_contract["source_system"]))
            .withColumn("source_entity", F.lit(source_contract["source_entity"]))
            .withColumn("ingestion_mode", F.lit(source_contract["ingestion_mode"]))
            .withColumn("schema_version", F.lit(source_contract["schema_version"]))
            .withColumn("batch_id", F.lit(batch_id))
            .withColumn("run_id", F.lit(batch_id))
            .withColumn("ingestion_date", F.to_date(F.current_timestamp()))
            .withColumn("source_file", F.lit("cdc://banking_core_local/core_clientes"))
            .withColumn("source_record_count", F.lit(source_record_count).cast("long"))
        )

        df_bronze.write.format("delta").mode("overwrite").save(cdc_bronze_path)
        df_read = DeltaIO.read_delta(spark, cdc_bronze_path)
        if df_read is None:
            raise RuntimeError(f"CDC Bronze Delta table is not readable: {cdc_bronze_path}")

        rows_written = df_read.count()
        columns = df_read.columns
        rows_by_operation = {
            row["cdc_operation"]: row["count"]
            for row in df_read.groupBy("cdc_operation").count().collect()
        }
        sequence_stats = _json_safe(
            df_read.agg(
                F.min("cdc_sequence").alias("min_sequence"),
                F.max("cdc_sequence").alias("max_sequence"),
                F.countDistinct("cdc_event_id").alias("distinct_event_ids"),
                F.countDistinct("cdc_transaction_id").alias("distinct_transaction_ids"),
            ).collect()[0]
        )
        duplicate_event_count = rows_written - int(sequence_stats["distinct_event_ids"])
        ordered_sequences = [
            row["cdc_sequence"]
            for row in df_read.select("cdc_sequence").orderBy("cdc_sequence").collect()
        ]

        required_source_validation = validate_required_columns(
            "clientes_cdc",
            [column for column in columns if column not in BRONZE_TECHNICAL_COLUMNS],
        )
        bronze_metadata_validation = validate_bronze_metadata_columns(columns)

        duration_seconds = round((datetime.now() - started_at).total_seconds(), 3)
        MonitoringLogger.log_pipeline_execution(
            spark,
            pipeline_name="cdc_local_demo",
            task_name="write_bronze_cdc",
            batch_id=batch_id,
            status="SUCCESS",
            rows_read=source_record_count,
            rows_written=rows_written,
            duration_seconds=duration_seconds,
            start_time=started_at.isoformat(),
            end_time=datetime.now().isoformat(),
        )
        monitoring_df = MonitoringLogger.get_execution_summary(spark, batch_id)
        monitoring_rows = monitoring_df.count() if monitoring_df is not None else 0

        expected_operation_counts = {
            "snapshot": 2,
            "insert": 1,
            "update": 1,
            "delete": 1,
        }
        validation_failures = {
            "cdc_profile_disabled": not bool(cdc_profile["enabled"]),
            "source_rows_mismatch": source_record_count != int(cdc_profile["demo_event_count"]),
            "bronze_rows_mismatch": rows_written != source_record_count,
            "operation_counts_mismatch": rows_by_operation != expected_operation_counts,
            "required_source_columns": required_source_validation["missing_columns"],
            "bronze_metadata_columns": bronze_metadata_validation["missing_columns"],
            "duplicate_event_count": duplicate_event_count,
            "ordering_mismatch": ordered_sequences != sorted(ordered_sequences) or ordered_sequences != list(range(1, source_record_count + 1)),
            "monitoring_missing": monitoring_rows < 1,
        }
        failed = any(bool(value) for value in validation_failures.values())

        summary.update({
            "status": "SUCCESS" if not failed else "FAILURE",
            "quality_gate_result": "Passed" if not failed else "Failed",
            "duration_seconds": duration_seconds,
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now().isoformat(),
            "source_contract": {
                "source_id": source_contract["source_id"],
                "ingestion_mode": source_contract["ingestion_mode"],
                "schema_version": source_contract["schema_version"],
                "primary_key": source_contract["primary_key"],
            },
            "rows_written": rows_written,
            "rows_by_operation": rows_by_operation,
            "expected_operation_counts": expected_operation_counts,
            "sequence_stats": sequence_stats,
            "ordered_sequences": ordered_sequences,
            "required_source_validation": required_source_validation,
            "bronze_metadata_validation": bronze_metadata_validation,
            "monitoring": {
                "status": "READABLE" if monitoring_rows else "MISSING",
                "rows": monitoring_rows,
            },
            "tooling_status": {
                "debezium": "not_installed_not_claimed",
                "airbyte": "not_installed_not_claimed",
                "kafka": "not_installed_not_claimed",
            },
            "explicit_limits": [
                "Local changelog proves CDC semantics without source database log capture.",
                "Debezium, Airbyte, and Kafka are not installed or claimed by this feature.",
                "This is not a production CDC latency or connector benchmark.",
            ],
            "validation_failures": validation_failures,
        })
        return_code = 0 if summary["status"] == "SUCCESS" else 1

    except Exception as exc:
        summary.update({
            "status": "FAILURE",
            "duration_seconds": round((datetime.now() - started_at).total_seconds(), 3),
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now().isoformat(),
            "error": str(exc),
        })
        return_code = 1

    finally:
        SparkSessionFactory.stop()
        print("CDC_SMOKE_RESULT=" + json.dumps(summary, sort_keys=True))

    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
