"""Smoke validation for the local observability baseline.

This script runs the demonstrable batch path in temporary local Delta paths
and emits one compact JSON evidence payload with statuses, durations,
monitoring rows, and row counts by layer.
"""
import argparse
import ast
import json
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict


REPO_ROOT = Path(__file__).resolve().parents[2]
for relative_path in (
    "jobs/data_generation",
    "jobs/bronze",
    "jobs/raw_vault",
    "jobs/business_vault",
    "jobs/common",
):
    sys.path.insert(0, str(REPO_ROOT / relative_path))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run observability baseline smoke test.")
    parser.add_argument(
        "--runtime-profile",
        default=os.getenv("RUNTIME_PROFILE", os.getenv("DM_RUNTIME_PROFILE", "local-small")),
        help="Runtime profile used to generate sample data and configure Spark.",
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


def _run_stage(name: str, action: Callable[[], Dict[str, Any]]) -> Dict[str, Any]:
    started_at = datetime.now()
    print(f"OBS_STAGE_START={name} started_at={started_at.isoformat()}", flush=True)

    try:
        result = action()
        status = result.get("status", "SUCCESS")
        error = None
    except Exception as exc:
        result = {}
        status = "FAILURE"
        error = str(exc)

    finished_at = datetime.now()
    stage_result = {
        "status": status,
        "duration_seconds": round((finished_at - started_at).total_seconds(), 3),
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "result": result,
    }
    if error:
        stage_result["error"] = error

    print(
        "OBS_STAGE_RESULT="
        + json.dumps({"stage": name, **stage_result}, sort_keys=True),
        flush=True,
    )

    if status != "SUCCESS":
        raise RuntimeError(f"Stage {name} failed: {error or result}")

    return stage_result


def _table_stats(spark, delta_io, table_paths: Dict[str, str]) -> Dict[str, Any]:
    stats = {}
    for table_name, table_path in table_paths.items():
        df = delta_io.read_delta(spark, table_path)
        if df is None:
            stats[table_name] = {
                "path": table_path,
                "status": "MISSING",
                "num_rows": 0,
                "num_columns": 0,
                "columns": [],
            }
            continue

        stats[table_name] = {
            "path": table_path,
            "status": "READABLE",
            "num_rows": df.count(),
            "num_columns": len(df.columns),
            "columns": df.columns,
        }
    return stats


def _sum_rows(stats: Dict[str, Any]) -> int:
    return sum(table.get("num_rows", 0) for table in stats.values())


def _monitoring_summary(spark, monitoring_logger, batch_id: str) -> Dict[str, Any]:
    from pyspark.sql import functions as F

    df = monitoring_logger.get_execution_summary(spark, batch_id)
    if df is None:
        return {
            "status": "MISSING",
            "rows": 0,
            "events": [],
            "summary": [],
        }

    events = [
        _json_safe(row)
        for row in df.orderBy("start_time").collect()
    ]
    grouped = df.groupBy("pipeline_name", "task_name", "status").agg(
        F.count("*").alias("event_count"),
        F.sum("rows_read").alias("rows_read"),
        F.sum("rows_written").alias("rows_written"),
        F.sum("duration_seconds").alias("duration_seconds"),
    )
    summary = [
        _json_safe(row)
        for row in grouped.orderBy("pipeline_name", "task_name", "status").collect()
    ]

    return {
        "status": "READABLE",
        "rows": len(events),
        "events": events,
        "summary": summary,
    }


def _airflow_static_summary() -> Dict[str, Any]:
    dag_path = REPO_ROOT / "dags" / "banking_data_vault_pipeline_dag.py"
    expected_stage_tasks = [
        ("bronze", "run_bronze"),
        ("hubs", "run_hubs"),
        ("links", "run_links"),
        ("satellites", "run_satellites"),
        ("gold", "run_gold"),
        ("data-vault-gate", "run_data_vault_gate"),
        ("masking-gate", "run_masking_gate"),
        ("evidence", "run_evidence"),
    ]
    expected_tasks = [task_id for _, task_id in expected_stage_tasks]
    if not dag_path.exists():
        return {
            "status": "MISSING",
            "dag_id": "banking_data_vault_pipeline",
            "task_ids": [],
            "expected_task_ids": expected_tasks,
            "missing_expected_task_ids": expected_tasks,
        }

    content = dag_path.read_text(encoding="utf-8", errors="ignore")
    declared_stages = []
    try:
        tree = ast.parse(content)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign):
                continue
            if not any(
                isinstance(target, ast.Name) and target.id == "stages"
                for target in node.targets
            ):
                continue
            value = ast.literal_eval(node.value)
            if isinstance(value, list) and all(
                isinstance(stage, str) for stage in value
            ):
                declared_stages = value
                break
    except (SyntaxError, ValueError):
        declared_stages = []

    present_tasks = [
        task_id
        for stage, task_id in expected_stage_tasks
        if stage in declared_stages
    ]
    missing_tasks = sorted(set(expected_tasks) - set(present_tasks))
    return {
        "status": "STATIC_READABLE" if not missing_tasks else "STATIC_INCOMPLETE",
        "dag_id": "banking_data_vault_pipeline",
        "task_ids": present_tasks,
        "expected_task_ids": expected_tasks,
        "missing_expected_task_ids": missing_tasks,
    }


def main() -> int:
    args = _parse_args()

    os.environ["RUNTIME_PROFILE"] = args.runtime_profile
    os.environ["DM_RUNTIME_PROFILE"] = args.runtime_profile
    os.environ["SPARK_LOG_LEVEL"] = args.log_level
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_IVY_DIR", "/tmp/.ivy2")

    work_dir = Path(args.work_dir or tempfile.mkdtemp(prefix="dm-observability-smoke-"))
    sample_data_path = work_dir / "sample"
    bronze_path = _as_file_uri(work_dir / "bronze")
    raw_vault_path = _as_file_uri(work_dir / "raw_vault")
    business_vault_path = _as_file_uri(work_dir / "business_vault")
    gold_path = _as_file_uri(work_dir / "gold")
    monitoring_path = _as_file_uri(work_dir / "monitoring")
    batch_id = args.batch_id or "observability_smoke_" + datetime.now().strftime("%Y%m%d%H%M%S")

    os.environ["BRONZE_PATH"] = bronze_path
    os.environ["RAW_VAULT_PATH"] = raw_vault_path
    os.environ["BUSINESS_VAULT_PATH"] = business_vault_path
    os.environ["GOLD_PATH"] = gold_path
    os.environ["MONITORING_PATH"] = monitoring_path

    from config import Config
    from delta_io import DeltaIO
    from generate_banking_sample_data import generate_all_sample_data
    from load_bronze import run_bronze_pipeline
    from load_gold import run_business_vault_pipeline
    from load_hubs import run_hubs_pipeline
    from load_links import run_links_pipeline
    from load_satellites import run_satellites_pipeline
    from monitoring import MonitoringLogger
    from spark_session import SparkSessionFactory, create_spark_session

    smoke_started_at = datetime.now()
    spark = create_spark_session()
    summary = {
        "runtime_profile": args.runtime_profile,
        "batch_id": batch_id,
        "work_dir": str(work_dir),
        "sample_data_path": str(sample_data_path),
        "bronze_path": bronze_path,
        "raw_vault_path": raw_vault_path,
        "business_vault_path": business_vault_path,
        "gold_path": gold_path,
        "monitoring_path": monitoring_path,
        "spark_version": spark.version,
        "status": "UNKNOWN",
        "airflow_static": _airflow_static_summary(),
    }

    try:
        stage_results = {}
        stage_results["generate_sample_data"] = _run_stage(
            "generate_sample_data",
            lambda: {
                "status": "SUCCESS",
                "files": generate_all_sample_data(
                    str(sample_data_path),
                    runtime_profile=args.runtime_profile,
                ),
            },
        )
        stage_results["bronze"] = _run_stage(
            "bronze",
            lambda: run_bronze_pipeline(spark, str(sample_data_path), bronze_path, batch_id),
        )
        stage_results["raw_hubs"] = _run_stage(
            "raw_hubs",
            lambda: run_hubs_pipeline(spark, bronze_path, batch_id),
        )
        stage_results["raw_links"] = _run_stage(
            "raw_links",
            lambda: run_links_pipeline(spark, bronze_path, batch_id),
        )
        stage_results["raw_satellites"] = _run_stage(
            "raw_satellites",
            lambda: run_satellites_pipeline(spark, bronze_path, batch_id),
        )
        stage_results["gold"] = _run_stage(
            "gold",
            lambda: run_business_vault_pipeline(
                spark, raw_vault_path, gold_path, batch_id
            ),
        )

        bronze_stats = _table_stats(
            spark,
            DeltaIO,
            {name: cfg["path"] for name, cfg in Config.BRONZE_TABLES.items()},
        )
        hub_stats = _table_stats(
            spark,
            DeltaIO,
            {name: cfg["path"] for name, cfg in Config.HUB_TABLES.items()},
        )
        link_stats = _table_stats(
            spark,
            DeltaIO,
            {name: cfg["path"] for name, cfg in Config.LINK_TABLES.items()},
        )
        satellite_stats = _table_stats(
            spark,
            DeltaIO,
            {name: cfg["path"] for name, cfg in Config.SATELLITE_TABLES.items()},
        )
        gold_stats = _table_stats(spark, DeltaIO, Config.GOLD_TABLES)
        monitoring = _monitoring_summary(spark, MonitoringLogger, batch_id)

        layer_counts = {
            "bronze": _sum_rows(bronze_stats),
            "raw_vault_hubs": _sum_rows(hub_stats),
            "raw_vault_links": _sum_rows(link_stats),
            "raw_vault_satellites": _sum_rows(satellite_stats),
            "gold": _sum_rows(gold_stats),
        }
        duration_by_stage = {
            stage_name: result["duration_seconds"]
            for stage_name, result in stage_results.items()
        }

        validation_failures = {
            "stage_failures": [
                stage_name
                for stage_name, result in stage_results.items()
                if result["status"] != "SUCCESS"
            ],
            "missing_monitoring_rows": monitoring["rows"] < 5,
            "missing_layer_counts": [
                layer
                for layer, rows in layer_counts.items()
                if rows <= 0
            ],
            "airflow_static_missing_tasks": summary["airflow_static"]["missing_expected_task_ids"],
        }
        failed = any([
            bool(validation_failures["stage_failures"]),
            validation_failures["missing_monitoring_rows"],
            bool(validation_failures["missing_layer_counts"]),
            bool(validation_failures["airflow_static_missing_tasks"]),
        ])

        smoke_finished_at = datetime.now()
        summary.update({
            "status": "SUCCESS" if not failed else "FAILURE",
            "duration_seconds": round((smoke_finished_at - smoke_started_at).total_seconds(), 3),
            "started_at": smoke_started_at.isoformat(),
            "finished_at": smoke_finished_at.isoformat(),
            "stage_results": stage_results,
            "duration_by_stage_seconds": duration_by_stage,
            "layer_counts": layer_counts,
            "table_stats": {
                "bronze": bronze_stats,
                "raw_vault_hubs": hub_stats,
                "raw_vault_links": link_stats,
                "raw_vault_satellites": satellite_stats,
                "gold": gold_stats,
            },
            "monitoring": monitoring,
            "validation_failures": validation_failures,
        })

        return_code = 0 if summary["status"] == "SUCCESS" else 1

    except Exception as exc:
        smoke_finished_at = datetime.now()
        summary.update({
            "status": "FAILURE",
            "duration_seconds": round((smoke_finished_at - smoke_started_at).total_seconds(), 3),
            "started_at": smoke_started_at.isoformat(),
            "finished_at": smoke_finished_at.isoformat(),
            "error": str(exc),
        })
        return_code = 1

    finally:
        SparkSessionFactory.stop()
        print("OBSERVABILITY_SMOKE_RESULT=" + json.dumps(summary, sort_keys=True))

    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
