"""Local streaming smoke runner for DM-ING-003.

The runner proves a bounded Spark Structured Streaming path without requiring
Kafka: deterministic local event files are consumed as a file stream, written
to Bronze Delta, and validated through row counts, checkpoint artifacts, and
monitoring records.
"""
import argparse
import json
import os
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "jobs" / "common"))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run local digital events streaming smoke.")
    parser.add_argument(
        "--runtime-profile",
        default=os.getenv("RUNTIME_PROFILE", os.getenv("DM_RUNTIME_PROFILE", "local-small")),
        help="Runtime profile used to size the local streaming demo.",
    )
    parser.add_argument("--work-dir", default=None, help="Optional work directory.")
    parser.add_argument("--batch-id", default=None, help="Optional batch id.")
    parser.add_argument("--event-count", type=int, default=None, help="Override generated event count.")
    parser.add_argument("--file-count", type=int, default=None, help="Override generated source file count.")
    parser.add_argument("--timeout-seconds", type=int, default=120, help="Streaming query timeout.")
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


def _event_payloads(event_count: int, batch_id: str) -> Iterable[Dict[str, Any]]:
    channels = [
        ("APP", "mobile_app"),
        ("WEB", "internet_banking"),
        ("ATM", "autoatendimento"),
        ("CHAT", "chatbot"),
    ]
    event_types = ["login", "pix_created", "balance_viewed", "card_limit_viewed", "profile_updated"]
    results = ["SUCCESS", "SUCCESS", "SUCCESS", "DENIED"]
    base_timestamp = datetime(2026, 7, 5, 9, 0, 0)

    for index in range(event_count):
        channel_id, channel_name = channels[index % len(channels)]
        yield {
            "evento_id": f"evt-{batch_id}-{index:06d}",
            "cliente_id": f"cli-{(index % 100) + 1:06d}",
            "canal_id": channel_id,
            "canal": channel_name,
            "tipo_evento": event_types[index % len(event_types)],
            "timestamp": (base_timestamp + timedelta(seconds=index * 3)).isoformat(),
            "resultado": results[index % len(results)],
            "detalhes": {
                "session_id": f"sess-{batch_id}-{index // 5:06d}",
                "device_type": "android" if index % 2 == 0 else "web",
                "synthetic": True,
            },
        }


def _write_event_files(input_dir: Path, event_count: int, file_count: int, batch_id: str) -> List[Dict[str, Any]]:
    input_dir.mkdir(parents=True, exist_ok=True)
    files: List[Dict[str, Any]] = []
    events = list(_event_payloads(event_count, batch_id))
    file_count = max(1, min(file_count, event_count))

    for file_index in range(file_count):
        chunk = events[file_index::file_count]
        file_path = input_dir / f"digital_events_{file_index:03d}.jsonl"
        with file_path.open("w", encoding="utf-8") as handle:
            for event in chunk:
                handle.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
        files.append({
            "path": str(file_path),
            "rows": len(chunk),
        })

    return files


def _checkpoint_summary(checkpoint_dir: Path) -> Dict[str, Any]:
    files = []
    if checkpoint_dir.exists():
        for path in checkpoint_dir.rglob("*"):
            if path.is_file():
                files.append(str(path.relative_to(checkpoint_dir)).replace("\\", "/"))

    return {
        "path": str(checkpoint_dir),
        "exists": checkpoint_dir.exists(),
        "file_count": len(files),
        "sample_files": sorted(files)[:20],
        "has_offsets": any(path.startswith("offsets/") for path in files),
        "has_commits": any(path.startswith("commits/") for path in files),
        "has_sources": any(path.startswith("sources/") for path in files),
    }


def _required_columns_present(columns: List[str], required_columns: List[str]) -> Dict[str, Any]:
    missing = sorted(set(required_columns) - set(columns))
    return {
        "passed": not missing,
        "missing_columns": missing,
        "required_columns": required_columns,
    }


def main() -> int:
    args = _parse_args()

    os.environ["RUNTIME_PROFILE"] = args.runtime_profile
    os.environ["DM_RUNTIME_PROFILE"] = args.runtime_profile
    os.environ["SPARK_LOG_LEVEL"] = args.log_level
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_IVY_DIR", "/tmp/.ivy2")

    work_dir = Path(args.work_dir or tempfile.mkdtemp(prefix="dm-streaming-smoke-"))
    input_dir = work_dir / "streaming_input"
    checkpoint_dir = work_dir / "checkpoint" / "digital_events"
    bronze_path = _as_file_uri(work_dir / "bronze")
    streaming_bronze_path = f"{bronze_path}/eventos_digitais_streaming"
    monitoring_path = _as_file_uri(work_dir / "monitoring")
    batch_id = args.batch_id or "streaming_smoke_" + datetime.now().strftime("%Y%m%d%H%M%S")

    os.environ["BRONZE_PATH"] = bronze_path
    os.environ["MONITORING_PATH"] = monitoring_path

    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        BooleanType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    from config import Config
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

    profile = get_runtime_profile(args.runtime_profile)
    streaming_profile = profile["streaming"]
    event_count = args.event_count if args.event_count is not None else int(streaming_profile["demo_event_count"])
    file_count = args.file_count if args.file_count is not None else int(streaming_profile["demo_file_count"])
    source_contract = get_source_contract("eventos_digitais_streaming")
    started_at = datetime.now()
    source_files = _write_event_files(input_dir, event_count, file_count, batch_id)

    spark = create_spark_session()
    summary: Dict[str, Any] = {
        "runtime_profile": args.runtime_profile,
        "batch_id": batch_id,
        "work_dir": str(work_dir),
        "input_dir": str(input_dir),
        "source_files": source_files,
        "checkpoint_path": str(checkpoint_dir),
        "bronze_path": streaming_bronze_path,
        "monitoring_path": monitoring_path,
        "spark_version": spark.version,
        "event_count_requested": event_count,
        "file_count_requested": file_count,
        "status": "UNKNOWN",
    }

    try:
        schema = StructType([
            StructField("evento_id", StringType(), False),
            StructField("cliente_id", StringType(), False),
            StructField("canal_id", StringType(), False),
            StructField("canal", StringType(), False),
            StructField("tipo_evento", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("resultado", StringType(), False),
            StructField(
                "detalhes",
                StructType([
                    StructField("session_id", StringType(), True),
                    StructField("device_type", StringType(), True),
                    StructField("synthetic", BooleanType(), True),
                ]),
                True,
            ),
        ])

        stream_df = spark.readStream.schema(schema).json(str(input_dir))
        bronze_df = (
            stream_df
            .withColumn("load_datetime", F.current_timestamp())
            .withColumn("record_source", F.lit(source_contract["record_source"]))
            .withColumn("source_system", F.lit(source_contract["source_system"]))
            .withColumn("source_entity", F.lit(source_contract["source_entity"]))
            .withColumn("ingestion_mode", F.lit(source_contract["ingestion_mode"]))
            .withColumn("schema_version", F.lit(source_contract["schema_version"]))
            .withColumn("batch_id", F.lit(batch_id))
            .withColumn("run_id", F.lit(batch_id))
            .withColumn("ingestion_date", F.to_date(F.current_timestamp()))
            .withColumn("source_file", F.input_file_name())
            .withColumn("source_record_count", F.lit(event_count).cast("long"))
        )

        query = (
            bronze_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", str(checkpoint_dir))
            .trigger(once=True)
            .start(streaming_bronze_path)
        )

        if not query.awaitTermination(args.timeout_seconds):
            query.stop()
            raise TimeoutError(f"Streaming query did not finish within {args.timeout_seconds} seconds.")

        progress_events = [_json_safe(progress) for progress in query.recentProgress]
        processed_rows = sum(int(progress.get("numInputRows", 0)) for progress in query.recentProgress)

        df_bronze = DeltaIO.read_delta(spark, streaming_bronze_path)
        if df_bronze is None:
            raise RuntimeError(f"Streaming Bronze Delta table is not readable: {streaming_bronze_path}")

        rows_written = df_bronze.count()
        columns = df_bronze.columns
        required_source_validation = validate_required_columns(
            "eventos_digitais_streaming",
            [column for column in columns if column not in BRONZE_TECHNICAL_COLUMNS],
        )
        bronze_metadata_validation = validate_bronze_metadata_columns(columns)
        contract_validation = _required_columns_present(columns, Config.TECHNICAL_COLUMNS)
        checkpoint = _checkpoint_summary(checkpoint_dir)

        duration_seconds = round((datetime.now() - started_at).total_seconds(), 3)
        MonitoringLogger.log_pipeline_execution(
            spark,
            pipeline_name="digital_events_streaming",
            task_name="file_microbatch_to_bronze",
            batch_id=batch_id,
            status="SUCCESS",
            rows_read=event_count,
            rows_written=rows_written,
            duration_seconds=duration_seconds,
            start_time=started_at.isoformat(),
            end_time=datetime.now().isoformat(),
        )

        monitoring_df = MonitoringLogger.get_execution_summary(spark, batch_id)
        monitoring_rows = monitoring_df.count() if monitoring_df is not None else 0

        validation_failures = {
            "streaming_profile_disabled": not bool(streaming_profile["enabled"]),
            "generated_events_mismatch": sum(item["rows"] for item in source_files) != event_count,
            "processed_rows_mismatch": processed_rows != event_count,
            "bronze_rows_mismatch": rows_written != event_count,
            "required_source_columns": required_source_validation["missing_columns"],
            "bronze_metadata_columns": bronze_metadata_validation["missing_columns"],
            "technical_columns": contract_validation["missing_columns"],
            "checkpoint_missing": not checkpoint["exists"],
            "checkpoint_offsets_missing": not checkpoint["has_offsets"],
            "checkpoint_commits_missing": not checkpoint["has_commits"],
            "monitoring_missing": monitoring_rows < 1,
        }
        failed = any(bool(value) for value in validation_failures.values())

        summary.update({
            "status": "SUCCESS" if not failed else "FAILURE",
            "duration_seconds": duration_seconds,
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now().isoformat(),
            "source_contract": {
                "source_id": source_contract["source_id"],
                "ingestion_mode": source_contract["ingestion_mode"],
                "schema_version": source_contract["schema_version"],
                "primary_key": source_contract["primary_key"],
            },
            "streaming_query": {
                "id": str(query.id),
                "run_id": str(query.runId),
                "name": query.name,
                "status": query.status,
                "last_progress": _json_safe(query.lastProgress),
                "recent_progress": progress_events,
                "processed_rows": processed_rows,
            },
            "checkpoint": checkpoint,
            "bronze_validation": {
                "rows_written": rows_written,
                "columns": columns,
                "required_source_validation": required_source_validation,
                "bronze_metadata_validation": bronze_metadata_validation,
                "technical_columns_validation": contract_validation,
            },
            "monitoring": {
                "status": "READABLE" if monitoring_rows else "MISSING",
                "rows": monitoring_rows,
            },
            "quality_gate_result": "Passed" if not failed else "Failed",
            "explicit_limits": [
                "Local file source proves bounded Spark Structured Streaming semantics.",
                "Kafka/Kinesis/Event Hubs are not installed or claimed by this feature.",
                "This is not a productive low-latency SLA benchmark.",
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
        print("STREAMING_SMOKE_RESULT=" + json.dumps(summary, sort_keys=True))

    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
