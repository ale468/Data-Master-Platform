"""Presentation demo smoke runner for the Data Master case.

Runs the demonstrable end-to-end path with the `presentation-demo` profile,
validates Bronze, Raw Vault, Gold, masking, monitoring, and emits a compact
JSON payload for the DM-DEMO-001 evidence record.
"""
import argparse
import json
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict


REPO_ROOT = Path(__file__).resolve().parents[2]
for relative_path in (
    "jobs/data_generation",
    "jobs/bronze",
    "jobs/raw_vault",
    "jobs/business_vault",
    "jobs/observability",
    "jobs/common",
):
    sys.path.insert(0, str(REPO_ROOT / relative_path))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the presentation demo smoke.")
    parser.add_argument(
        "--runtime-profile",
        default=os.getenv("RUNTIME_PROFILE", os.getenv("DM_RUNTIME_PROFILE", "presentation-demo")),
        help="Runtime profile. DM-DEMO-001 expects presentation-demo.",
    )
    parser.add_argument(
        "--expected-runtime-profile",
        default="presentation-demo",
        help="Runtime profile expected by this validation invocation.",
    )
    parser.add_argument("--work-dir", default=None, help="Optional work directory.")
    parser.add_argument("--batch-id", default=None, help="Optional batch id.")
    parser.add_argument(
        "--log-level",
        default=os.getenv("SPARK_LOG_LEVEL", "WARN"),
        help="Spark log level for the demo run.",
    )
    return parser.parse_args()


def _as_file_uri(path: Path) -> str:
    return path.resolve().as_uri()


def _extract_masking_failures(masking_validation: Dict[str, Any]) -> Dict[str, Any]:
    gold_validation = masking_validation["gold_validation"]
    masking_samples = masking_validation["masking_samples"]
    secret_findings = masking_validation["secret_findings"]

    return {
        "masking_sample_failures": [
            name for name, sample in masking_samples.items() if not sample["passed"]
        ],
        "forbidden_columns": gold_validation["forbidden_columns"],
        "raw_pattern_hits": gold_validation["raw_pattern_hits"],
        "protected_check_failures": {
            name: count
            for name, count in gold_validation["protected_checks"].items()
            if count > 0
        },
        "cliente_check_failures": {
            name: count
            for name, count in gold_validation["cliente_checks"].items()
            if count > 0
        },
        "risco_check_failures": {
            name: count
            for name, count in gold_validation["risco_checks"].items()
            if count > 0
        },
        "secret_findings": secret_findings,
    }


def _has_masking_failure(masking_failures: Dict[str, Any]) -> bool:
    return any(bool(value) for value in masking_failures.values())


def _run_data_vault_quality_gate(
    spark,
    raw_vault_path: str,
    gold_path: str,
    evaluate_gate,
    render_output,
) -> Dict[str, Any]:
    result = evaluate_gate(spark, raw_vault_path, gold_path, REPO_ROOT)
    print(render_output(result), flush=True)
    return {
        "status": "SUCCESS" if result["status"] == "PASS" else "FAILURE",
        "gate_result": result,
    }


def _readiness_by_pdf_criterion(
    layer_counts: Dict[str, int],
    monitoring_rows: int,
    masking_failures: Dict[str, Any],
    runtime_profile: str,
) -> Dict[str, Dict[str, str]]:
    local_direct_validation = runtime_profile == "local-small"
    return {
        "extracao_dados": {
            "status": "covered",
            "evidence": (
                f"{runtime_profile} generated synthetic banking input files."
            ),
        },
        "ingestao_batch": {
            "status": "covered" if layer_counts["bronze"] > 0 else "failed",
            "evidence": f"Bronze Delta rows: {layer_counts['bronze']}.",
        },
        "streaming_tempo_real": {
            "status": "covered_local_outside_runner",
            "evidence": "Local streaming evidence is maintained in DM-ING-003; this runner does not prove broker readiness.",
        },
        "cdc_conector": {
            "status": "covered_local_outside_runner",
            "evidence": "Connector contract and local CDC evidence are maintained in DM-CONN-001 and DM-ING-004; this runner does not prove external tooling or production CDC.",
        },
        "armazenamento": {
            "status": "covered",
            "evidence": "Bronze, Raw Vault, and Gold Delta tables were written and read in the demo workdir.",
        },
        "data_vault": {
            "status": "covered"
            if all(layer_counts[key] > 0 for key in ("raw_vault_hubs", "raw_vault_links", "raw_vault_satellites"))
            else "failed",
            "evidence": (
                f"Hubs {layer_counts['raw_vault_hubs']}, "
                f"Links {layer_counts['raw_vault_links']}, "
                f"Satellites {layer_counts['raw_vault_satellites']}."
            ),
        },
        "gold": {
            "status": "covered" if layer_counts["gold"] > 0 else "failed",
            "evidence": f"Gold Delta rows: {layer_counts['gold']}.",
        },
        "mascaramento": {
            "status": "covered" if not _has_masking_failure(masking_failures) else "failed",
            "evidence": "Gold protected outputs and masking samples validated.",
        },
        "observabilidade": {
            "status": "baseline_only" if monitoring_rows >= 5 else "failed",
            "evidence": f"Monitoring Delta events: {monitoring_rows}; K8s/ArgoCD live health is validated by separate environment probes.",
        },
        "seguranca": {
            "status": "baseline_only" if not masking_failures["secret_findings"] else "failed",
            "evidence": "High-confidence secret scan and protected Gold baseline validated.",
        },
        "escalabilidade": {
            "status": "baseline_only" if local_direct_validation else "covered",
            "evidence": (
                "local-small executed as a direct local validation; it does not "
                "prove distributed execution, autoscaling, or cloud readiness."
                if local_direct_validation
                else "presentation-demo profile loaded and executed; cloud-ready "
                "remains a reference profile."
            ),
        },
        "reprodutibilidade": {
            "status": "with_prerequisites",
            "evidence": "Containerized command, image, runtime profile, and environment gaps recorded.",
        },
        "readme_relatorio": {
            "status": "gate_controlled",
            "evidence": "README claims remain controlled by public validation and review gates.",
        },
        "apresentacao": {
            "status": (
                "not_evaluated_by_local_direct_validation"
                if local_direct_validation
                else "ready_local_with_declared_limitations"
            ),
            "evidence": (
                "local-small validates the direct Spark data path only; Airflow, "
                "Minikube, integrated demo readiness, and production claims are "
                "outside this execution."
                if local_direct_validation
                else "Evidence payload covers must-have technical criteria; "
                "production broker, external connector tooling, production CDC "
                "and cloud remain outside this runner."
            ),
        },
    }


def _execution_scope(runtime_profile: str) -> str:
    if runtime_profile == "local-small":
        return "local_direct_validation"
    return "presentation_demo_runner"


def _readiness_status(runtime_profile: str, failed: bool) -> str:
    if failed:
        return "Not ready"
    if runtime_profile == "local-small":
        return (
            "Local data-path validation passed; Airflow, Minikube, and integrated "
            "demo readiness were not evaluated"
        )
    return "Demo-ready local with declared limitations"


def _demo_gate_result(runtime_profile: str, failed: bool) -> str:
    if failed:
        return "Not passed"
    if runtime_profile == "local-small":
        return (
            "Not evaluated by local-small; public and integrated demo claims "
            "remain gate-controlled"
        )
    return "Baseline passed; public claims remain gate-controlled"


def main() -> int:
    args = _parse_args()
    batch_id = args.batch_id or "presentation_demo_" + datetime.now().strftime("%Y%m%d%H%M%S")
    started_at = datetime.now()
    summary: Dict[str, Any] = {
        "runtime_profile": args.runtime_profile,
        "expected_runtime_profile": args.expected_runtime_profile,
        "execution_scope": _execution_scope(args.runtime_profile),
        "batch_id": batch_id,
        "status": "UNKNOWN",
    }
    spark_factory = None
    return_code = 1

    try:
        os.environ["RUNTIME_PROFILE"] = args.runtime_profile
        os.environ["DM_RUNTIME_PROFILE"] = args.runtime_profile
        os.environ["SPARK_LOG_LEVEL"] = args.log_level
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ.setdefault("SPARK_IVY_DIR", "/tmp/.ivy2")

        work_dir = Path(
            args.work_dir or tempfile.mkdtemp(prefix="dm-presentation-demo-")
        )
        sample_data_path = work_dir / "sample"
        bronze_path = _as_file_uri(work_dir / "bronze")
        raw_vault_path = _as_file_uri(work_dir / "raw_vault")
        business_vault_path = _as_file_uri(work_dir / "business_vault")
        gold_path = _as_file_uri(work_dir / "gold")
        monitoring_path = _as_file_uri(work_dir / "monitoring")

        os.environ["BRONZE_PATH"] = bronze_path
        os.environ["RAW_VAULT_PATH"] = raw_vault_path
        os.environ["BUSINESS_VAULT_PATH"] = business_vault_path
        os.environ["GOLD_PATH"] = gold_path
        os.environ["MONITORING_PATH"] = monitoring_path

        from config import Config
        from data_vault_quality_gate import (
            evaluate_configured_gate,
            render_gate_output,
        )
        from delta_io import DeltaIO
        from generate_banking_sample_data import generate_all_sample_data
        from load_bronze import run_bronze_pipeline
        from load_gold import run_business_vault_pipeline
        from load_hubs import run_hubs_pipeline
        from load_links import run_links_pipeline
        from load_satellites import run_satellites_pipeline
        from monitoring import MonitoringLogger
        from run_gold_masking_smoke import (
            _masking_function_samples,
            _scan_high_confidence_secrets,
            _validate_gold_outputs,
        )
        from run_observability_smoke import (
            _airflow_static_summary,
            _monitoring_summary,
            _run_stage,
            _sum_rows,
            _table_stats,
        )
        from spark_session import SparkSessionFactory, create_spark_session

        spark_factory = SparkSessionFactory
        spark = create_spark_session()
        summary.update({
            "work_dir": str(work_dir),
            "sample_data_path": str(sample_data_path),
            "bronze_path": bronze_path,
            "raw_vault_path": raw_vault_path,
            "business_vault_path": business_vault_path,
            "gold_path": gold_path,
            "monitoring_path": monitoring_path,
            "spark_version": spark.version,
            "airflow_static": _airflow_static_summary(),
        })

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
        stage_results["data_vault_quality_gate"] = _run_stage(
            "data_vault_quality_gate",
            lambda: _run_data_vault_quality_gate(
                spark,
                raw_vault_path,
                gold_path,
                evaluate_configured_gate,
                render_gate_output,
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
        masking_validation = {
            "masking_samples": _masking_function_samples(),
            "gold_validation": _validate_gold_outputs(spark, Config, DeltaIO),
            "secret_findings": _scan_high_confidence_secrets(REPO_ROOT),
        }
        masking_failures = _extract_masking_failures(masking_validation)

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
        readiness_by_pdf = _readiness_by_pdf_criterion(
            layer_counts,
            monitoring["rows"],
            masking_failures,
            args.runtime_profile,
        )
        data_vault_gate = stage_results["data_vault_quality_gate"]["result"][
            "gate_result"
        ]

        validation_failures = {
            "runtime_profile_mismatch": (
                args.runtime_profile != args.expected_runtime_profile
            ),
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
            "masking_failures": masking_failures,
            "data_vault_quality_gate_failed": data_vault_gate["status"] != "PASS",
        }
        failed = any([
            validation_failures["runtime_profile_mismatch"],
            bool(validation_failures["stage_failures"]),
            validation_failures["missing_monitoring_rows"],
            bool(validation_failures["missing_layer_counts"]),
            bool(validation_failures["airflow_static_missing_tasks"]),
            _has_masking_failure(masking_failures),
            validation_failures["data_vault_quality_gate_failed"],
        ])

        finished_at = datetime.now()
        summary.update({
            "status": "SUCCESS" if not failed else "FAILURE",
            "readiness_status": _readiness_status(args.runtime_profile, failed),
            "demo_gate_result": _demo_gate_result(args.runtime_profile, failed),
            "duration_seconds": round((finished_at - started_at).total_seconds(), 3),
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "stage_results": stage_results,
            "duration_by_stage_seconds": duration_by_stage,
            "layer_counts": layer_counts,
            "monitoring": monitoring,
            "masking_validation": masking_validation,
            "data_vault_quality_gate": data_vault_gate,
            "readiness_by_pdf_criterion": readiness_by_pdf,
            "explicit_gaps": [
                "Streaming, connector and CDC feature evidence is maintained outside this runner; do not infer broker, external connector tooling or production CDC readiness from this demo command.",
                "Live Kubernetes/ArgoCD health is captured by separate environment probes, not by this runner.",
                "Cloud-ready is an architectural reference, not a validated cloud deployment.",
                "LGPD support is demonstrative masking/security baseline, not formal compliance certification.",
            ],
            "validation_failures": validation_failures,
        })
        return_code = 0 if summary["status"] == "SUCCESS" else 1

    except Exception as exc:
        finished_at = datetime.now()
        summary.update({
            "status": "FAILURE",
            "readiness_status": "Not ready",
            "duration_seconds": round((finished_at - started_at).total_seconds(), 3),
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "error_type": type(exc).__name__,
            "error": "Execution failed; inspect the preceding stage logs.",
        })
        return_code = 1

    finally:
        if spark_factory is not None:
            try:
                spark_factory.stop()
            except Exception as exc:
                summary["status"] = "FAILURE"
                summary["readiness_status"] = "Not ready"
                summary["cleanup_error_type"] = type(exc).__name__
                return_code = 1
        print("PRESENTATION_DEMO_RESULT=" + json.dumps(summary, sort_keys=True))

    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
