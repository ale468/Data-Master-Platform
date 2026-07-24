"""Build a synthetic local flow and execute the DM-DV-004 gate."""

import argparse
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
for relative_path in (
    "jobs/data_generation",
    "jobs/bronze",
    "jobs/raw_vault",
    "jobs/business_vault",
    "jobs/common",
):
    sys.path.insert(0, str(REPO_ROOT / relative_path))


def _as_file_uri(path: Path) -> str:
    return path.resolve().as_uri()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run executable Data Vault quality gate.")
    parser.add_argument("--runtime-profile", default="local-small")
    parser.add_argument("--batch-id", default=None)
    parser.add_argument("--work-dir", default=None)
    parser.add_argument("--log-level", default="WARN")
    args = parser.parse_args()

    os.environ["RUNTIME_PROFILE"] = args.runtime_profile
    os.environ["DM_RUNTIME_PROFILE"] = args.runtime_profile
    os.environ["SPARK_LOG_LEVEL"] = args.log_level
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_IVY_DIR", "/tmp/.ivy2")

    work_dir = Path(args.work_dir or tempfile.mkdtemp(prefix="dm-dv-gate-"))
    sample_path = work_dir / "sample"
    bronze_path = _as_file_uri(work_dir / "bronze")
    raw_vault_path = _as_file_uri(work_dir / "raw_vault")
    business_vault_path = _as_file_uri(work_dir / "business_vault")
    gold_path = _as_file_uri(work_dir / "gold")
    monitoring_path = _as_file_uri(work_dir / "monitoring")
    batch_id = args.batch_id or "dm_dv_gate_" + datetime.now().strftime("%Y%m%d%H%M%S")

    os.environ["BRONZE_PATH"] = bronze_path
    os.environ["RAW_VAULT_PATH"] = raw_vault_path
    os.environ["BUSINESS_VAULT_PATH"] = business_vault_path
    os.environ["GOLD_PATH"] = gold_path
    os.environ["MONITORING_PATH"] = monitoring_path

    from data_vault_quality_gate import (
        evaluate_configured_gate,
        gate_exit_code,
        render_gate_output,
    )
    from generate_banking_sample_data import generate_all_sample_data
    from load_bronze import run_bronze_pipeline
    from load_gold import run_business_vault_pipeline
    from load_hubs import run_hubs_pipeline
    from load_links import run_links_pipeline
    from load_satellites import run_satellites_pipeline
    from spark_session import SparkSessionFactory, create_spark_session

    spark = create_spark_session()
    try:
        generate_all_sample_data(str(sample_path), runtime_profile=args.runtime_profile)
        stages = {
            "bronze": run_bronze_pipeline(
                spark, str(sample_path), bronze_path, batch_id
            ),
            "hubs": run_hubs_pipeline(spark, bronze_path, batch_id),
            "links": run_links_pipeline(spark, bronze_path, batch_id),
            "satellites": run_satellites_pipeline(spark, bronze_path, batch_id),
        }
        stages["gold"] = run_business_vault_pipeline(
            spark, raw_vault_path, gold_path, batch_id
        )
        failed_stages = [
            name for name, result in stages.items() if result.get("status") != "SUCCESS"
        ]
        if failed_stages:
            print("DATA_VAULT_QUALITY_GATE_STATUS=FAILED")
            print("FAILED_CHECKS=stage_failure:" + ",".join(failed_stages))
            return 1

        result = evaluate_configured_gate(
            spark, raw_vault_path, gold_path, REPO_ROOT
        )
        print(render_gate_output(result))
        return gate_exit_code(result)
    except Exception as exc:
        print("DATA_VAULT_QUALITY_GATE_STATUS=FAILED")
        print(f"FAILED_CHECKS=gate_runtime:{type(exc).__name__}:{exc}")
        return 1
    finally:
        SparkSessionFactory.stop()


if __name__ == "__main__":
    raise SystemExit(main())
