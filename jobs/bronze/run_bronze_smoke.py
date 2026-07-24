"""Reusable smoke runner for Bronze batch ingestion.

The script generates synthetic sample data for a runtime profile, executes the
Bronze pipeline, reads every Delta table back, and prints a compact JSON record
that can be copied into a validation record.
"""
import argparse
import json
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
for relative_path in ("jobs/data_generation", "jobs/bronze", "jobs/common"):
    sys.path.insert(0, str(REPO_ROOT / relative_path))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Bronze ingestion smoke test.")
    parser.add_argument(
        "--runtime-profile",
        default=os.getenv("RUNTIME_PROFILE", os.getenv("DM_RUNTIME_PROFILE", "local-small")),
        help="Runtime profile used to generate sample data and configure Spark.",
    )
    parser.add_argument(
        "--work-dir",
        default=None,
        help="Optional work directory. Defaults to a temporary directory.",
    )
    parser.add_argument(
        "--sample-data-path",
        default=None,
        help="Optional input path. If omitted, sample data is generated.",
    )
    parser.add_argument(
        "--bronze-path",
        default=None,
        help="Optional Bronze Delta base path. If omitted, a file URI under work-dir is used.",
    )
    parser.add_argument(
        "--batch-id",
        default=None,
        help="Optional batch id. Defaults to a timestamped smoke id.",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("SPARK_LOG_LEVEL", "WARN"),
        help="Spark log level for the smoke run.",
    )
    return parser.parse_args()


def _as_file_uri(path: Path) -> str:
    return path.resolve().as_uri()


def main() -> int:
    args = _parse_args()

    os.environ["RUNTIME_PROFILE"] = args.runtime_profile
    os.environ["DM_RUNTIME_PROFILE"] = args.runtime_profile
    os.environ["SPARK_LOG_LEVEL"] = args.log_level
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_IVY_DIR", "/tmp/.ivy2")

    work_dir = Path(args.work_dir or tempfile.mkdtemp(prefix="dm-bronze-smoke-"))
    sample_data_path = Path(args.sample_data_path or work_dir / "sample")
    bronze_path = args.bronze_path or _as_file_uri(work_dir / "bronze")
    batch_id = args.batch_id or "bronze_smoke_" + datetime.now().strftime("%Y%m%d%H%M%S")
    os.environ.setdefault("MONITORING_PATH", _as_file_uri(work_dir / "monitoring"))

    from config import Config
    from delta_io import DeltaIO
    from generate_banking_sample_data import generate_all_sample_data
    from load_bronze import run_bronze_pipeline
    from monitoring import MonitoringLogger
    from source_registry import list_registered_sources
    from spark_session import SparkSessionFactory, create_spark_session

    if args.sample_data_path is None:
        generate_all_sample_data(
            str(sample_data_path),
            runtime_profile=args.runtime_profile,
        )

    spark = create_spark_session()
    summary = {
        "runtime_profile": args.runtime_profile,
        "batch_id": batch_id,
        "work_dir": str(work_dir),
        "sample_data_path": str(sample_data_path),
        "bronze_path": bronze_path,
        "spark_version": spark.version,
        "tables": {},
        "required_technical_columns": Config.TECHNICAL_COLUMNS,
        "status": "UNKNOWN",
    }

    try:
        started_at = datetime.now()
        result = run_bronze_pipeline(
            spark,
            str(sample_data_path),
            bronze_path,
            batch_id,
        )
        finished_at = datetime.now()
        summary["pipeline_status"] = result.get("status")
        summary["pipeline_total_rows"] = result.get("total_rows", 0)
        summary["duration_seconds"] = round((finished_at - started_at).total_seconds(), 3)

        missing_columns = {}
        zero_count_tables = []
        row_mismatches = {}

        for source_name in list_registered_sources("batch"):
            table_path = f"{bronze_path}/{source_name}"
            stats = DeltaIO.get_table_stats(spark, table_path)
            expected_rows = result.get("results", {}).get(source_name, {}).get("rows_written")
            columns = stats.get("columns", [])
            missing = [col for col in Config.TECHNICAL_COLUMNS if col not in columns]

            if missing:
                missing_columns[source_name] = missing
            if stats.get("num_rows", 0) <= 0:
                zero_count_tables.append(source_name)
            if expected_rows is not None and stats.get("num_rows") != expected_rows:
                row_mismatches[source_name] = {
                    "expected_rows": expected_rows,
                    "delta_rows": stats.get("num_rows"),
                }

            summary["tables"][source_name] = {
                "path": table_path,
                "num_rows": stats.get("num_rows"),
                "num_columns": stats.get("num_columns"),
                "missing_technical_columns": missing,
            }

        monitoring_stats = DeltaIO.get_table_stats(spark, Config.MONITORING_TABLE)
        monitoring_summary = MonitoringLogger.get_execution_summary(spark, batch_id)
        monitoring_batch_rows = 0 if monitoring_summary is None else monitoring_summary.count()
        summary["monitoring"] = {
            "path": Config.MONITORING_TABLE,
            "num_rows": monitoring_stats.get("num_rows"),
            "num_columns": monitoring_stats.get("num_columns"),
            "batch_rows": monitoring_batch_rows,
        }
        monitoring_missing = monitoring_stats.get("num_rows", 0) <= 0 or monitoring_batch_rows <= 0

        summary["validations"] = {
            "missing_technical_columns": missing_columns,
            "zero_count_tables": zero_count_tables,
            "row_mismatches": row_mismatches,
            "monitoring_missing": monitoring_missing,
        }

        if result.get("status") != "SUCCESS":
            raise RuntimeError(f"Bronze pipeline failed: {result}")
        if missing_columns or zero_count_tables or row_mismatches or monitoring_missing:
            raise RuntimeError(f"Bronze smoke validation failed: {summary['validations']}")

        summary["status"] = "SUCCESS"
        return_code = 0

    except Exception as exc:
        summary["status"] = "FAILURE"
        summary["error"] = str(exc)
        return_code = 1

    finally:
        SparkSessionFactory.stop()
        print("BRONZE_SMOKE_RESULT=" + json.dumps(summary, sort_keys=True))

    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
