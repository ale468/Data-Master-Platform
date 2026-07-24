"""Smoke validation for Gold masking and high-confidence secret exposure.

The script builds Bronze, Raw Vault and Gold in temporary local Delta paths, validates
protected Gold outputs, exercises masking helpers, scans the repository for
high-confidence secret patterns, and prints a compact JSON evidence record.
"""
import argparse
import json
import os
import re
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List


REPO_ROOT = Path(__file__).resolve().parents[2]
for relative_path in (
    "jobs/data_generation",
    "jobs/bronze",
    "jobs/raw_vault",
    "jobs/business_vault",
    "jobs/common",
):
    sys.path.insert(0, str(REPO_ROOT / relative_path))


SECRET_PATTERNS = {
    "aws_access_key_id": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    "private_key": re.compile(r"-----BEGIN (?:RSA |OPENSSH |EC |DSA )?PRIVATE KEY-----"),
    "github_token": re.compile(r"\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{36,}\b"),
    "github_pat": re.compile(r"\bgithub_pat_[A-Za-z0-9_]{20,}\b"),
    "slack_token": re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{20,}\b"),
}

SCAN_EXTENSIONS = {
    ".cfg",
    ".conf",
    ".env",
    ".ini",
    ".json",
    ".md",
    ".properties",
    ".ps1",
    ".py",
    ".sh",
    ".tf",
    ".toml",
    ".txt",
    ".yaml",
    ".yml",
}

SKIP_DIRS = {
    ".git",
    ".pytest_cache",
    ".venv",
    "__pycache__",
    "data",
    "node_modules",
    "venv",
}

RAW_SENSITIVE_COLUMNS = {
    "cliente_id",
    "conta_id",
    "cpf",
    "documento",
    "email",
    "telefone",
    "celular",
    "numero_cartao",
    "cvv",
    "endereco",
    "nome",
}

RAW_PII_PATTERNS = {
    "cpf": r"\d{3}\.\d{3}\.\d{3}-\d{2}",
    "email": r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
    "phone": r"\+55\s*\(\d{2}\)\s*\d{5}-\d{4}",
    "card": r"\d{4}-\d{4}-\d{4}-\d{4}",
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Gold masking smoke test.")
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


def _iter_scan_files(root: Path) -> Iterable[Path]:
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [dirname for dirname in dirnames if dirname not in SKIP_DIRS]
        for filename in filenames:
            path = Path(dirpath) / filename
            if path.suffix.lower() in SCAN_EXTENSIONS or filename.lower() == "dockerfile":
                yield path


def _scan_high_confidence_secrets(root: Path) -> List[Dict[str, object]]:
    findings = []
    for path in _iter_scan_files(root):
        try:
            lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except OSError:
            continue

        for line_number, line in enumerate(lines, start=1):
            for pattern_name, pattern in SECRET_PATTERNS.items():
                if pattern.search(line):
                    findings.append({
                        "pattern": pattern_name,
                        "path": str(path.relative_to(root)),
                        "line": line_number,
                    })
    return findings


def _masking_function_samples() -> Dict[str, Dict[str, object]]:
    from masking import MaskingUtils

    samples = {
        "cpf": {
            "input": "123.456.789-10",
            "output": MaskingUtils.mask_cpf("123.456.789-10"),
            "expected": "*********10",
        },
        "email": {
            "input": "joao.silva@example.com",
            "output": MaskingUtils.mask_email("joao.silva@example.com"),
            "expected": "j*********@example.com",
        },
        "phone": {
            "input": "+55 (11) 98765-4321",
            "output": MaskingUtils.mask_phone("+55 (11) 98765-4321"),
            "expected": "*** (*) ****-4321",
        },
        "name": {
            "input": "Joao da Silva",
            "output": MaskingUtils.mask_name("Joao da Silva"),
            "expected": "J***",
        },
        "card": {
            "input": "4532-0151-1283-0366",
            "output": MaskingUtils.mask_card_number("4532-0151-1283-0366"),
            "expected": "****-****-****-0366",
        },
    }

    for sample in samples.values():
        sample["passed"] = sample["output"] == sample["expected"]

    return samples


def _count_raw_pattern_hits(df, pattern: str) -> int:
    from functools import reduce
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    string_columns = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, StringType)
    ]
    if not string_columns:
        return 0

    predicate = reduce(
        lambda left, right: left | right,
        [F.col(column).rlike(pattern) for column in string_columns],
    )
    return df.filter(predicate).count()


def _validate_gold_outputs(spark, config, delta_io) -> Dict[str, object]:
    from pyspark.sql import functions as F

    tables = {}
    forbidden_columns = {}
    raw_pattern_hits = {}

    for table_name, table_path in config.GOLD_TABLES.items():
        df = delta_io.read_delta(spark, table_path)
        if df is None:
            raise RuntimeError(f"Gold table cannot be read: {table_name} at {table_path}")

        columns = df.columns
        row_count = df.count()
        forbidden = sorted(RAW_SENSITIVE_COLUMNS.intersection(set(columns)))
        table_pattern_hits = {
            pattern_name: _count_raw_pattern_hits(df, pattern)
            for pattern_name, pattern in RAW_PII_PATTERNS.items()
        }

        if forbidden:
            forbidden_columns[table_name] = forbidden
        if any(count > 0 for count in table_pattern_hits.values()):
            raw_pattern_hits[table_name] = table_pattern_hits

        tables[table_name] = {
            "path": table_path,
            "num_rows": row_count,
            "columns": columns,
        }

    protected = delta_io.read_delta(spark, config.GOLD_TABLES["gold_clientes_protegidos"])
    cliente = delta_io.read_delta(spark, config.GOLD_TABLES["gold_transacoes_por_cliente"])
    risco = delta_io.read_delta(
        spark,
        config.GOLD_TABLES["gold_risco_transacional_simplificado"],
    )

    protected_checks = {
        "cpf_mask_failures": protected.filter(~F.col("cpf_mascarado").rlike(r"^\*{9}\d{2}$")).count(),
        "email_mask_failures": protected.filter(~F.col("email_mascarado").contains("*")).count(),
        "phone_mask_failures": protected.filter(~F.col("telefone_mascarado").contains("*")).count(),
        "name_mask_failures": protected.filter(~F.col("nome_cliente").contains("*")).count(),
        "client_pseudonym_failures": protected.filter(
            ~F.col("cliente_id_pseudonimizado").rlike(r"^CLI_[0-9A-F]{8}$")
        ).count(),
    }
    cliente_checks = {
        "masked_name_failures": cliente.filter(F.col("nome_cliente") != "[Mascarado]").count(),
        "client_pseudonym_failures": cliente.filter(
            ~F.col("cliente_id_pseudonimizado").rlike(r"^CLI_[0-9A-F]{8}$")
        ).count(),
    }
    risco_checks = {
        "account_pseudonym_failures": risco.filter(
            ~F.col("conta_id_pseudonimizada").rlike(r"^ACC_[0-9A-F]{8}$")
        ).count(),
    }

    return {
        "tables": tables,
        "forbidden_columns": forbidden_columns,
        "raw_pattern_hits": raw_pattern_hits,
        "protected_checks": protected_checks,
        "cliente_checks": cliente_checks,
        "risco_checks": risco_checks,
    }


def main() -> int:
    args = _parse_args()

    os.environ["RUNTIME_PROFILE"] = args.runtime_profile
    os.environ["DM_RUNTIME_PROFILE"] = args.runtime_profile
    os.environ["SPARK_LOG_LEVEL"] = args.log_level
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_IVY_DIR", "/tmp/.ivy2")

    work_dir = Path(args.work_dir or tempfile.mkdtemp(prefix="dm-gold-mask-smoke-"))
    sample_data_path = work_dir / "sample"
    bronze_path = _as_file_uri(work_dir / "bronze")
    raw_vault_path = _as_file_uri(work_dir / "raw_vault")
    business_vault_path = _as_file_uri(work_dir / "business_vault")
    gold_path = _as_file_uri(work_dir / "gold")
    monitoring_path = _as_file_uri(work_dir / "monitoring")
    batch_id = args.batch_id or "gold_masking_smoke_" + datetime.now().strftime("%Y%m%d%H%M%S")

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

    summary = {
        "runtime_profile": args.runtime_profile,
        "batch_id": batch_id,
        "work_dir": str(work_dir),
        "bronze_path": bronze_path,
        "raw_vault_path": raw_vault_path,
        "business_vault_path": business_vault_path,
        "gold_path": gold_path,
        "monitoring_path": monitoring_path,
        "status": "UNKNOWN",
    }

    spark = create_spark_session()
    try:
        started_at = datetime.now()
        generate_all_sample_data(str(sample_data_path), runtime_profile=args.runtime_profile)
        bronze_result = run_bronze_pipeline(spark, str(sample_data_path), bronze_path, batch_id)
        hubs_result = run_hubs_pipeline(spark, bronze_path, batch_id)
        links_result = run_links_pipeline(spark, bronze_path, batch_id)
        satellites_result = run_satellites_pipeline(spark, bronze_path, batch_id)
        gold_result = run_business_vault_pipeline(
            spark, raw_vault_path, gold_path, batch_id
        )
        finished_at = datetime.now()

        masking_samples = _masking_function_samples()
        gold_validation = _validate_gold_outputs(spark, Config, DeltaIO)
        secret_findings = _scan_high_confidence_secrets(REPO_ROOT)
        monitoring_summary = MonitoringLogger.get_execution_summary(spark, batch_id)
        monitoring_rows = 0 if monitoring_summary is None else monitoring_summary.count()

        summary.update({
            "duration_seconds": round((finished_at - started_at).total_seconds(), 3),
            "bronze_status": bronze_result.get("status"),
            "bronze_total_rows": bronze_result.get("total_rows"),
            "raw_vault_status": {
                "hubs": hubs_result.get("status"),
                "links": links_result.get("status"),
                "satellites": satellites_result.get("status"),
            },
            "gold_status": gold_result.get("status"),
            "gold_total_rows": gold_result.get("total_rows"),
            "gold_tables": {
                table_name: {
                    "rows_written": result["rows_written"],
                    "status": result["status"],
                }
                for table_name, result in gold_result.get("results", {}).items()
            },
            "masking_samples": masking_samples,
            "gold_validation": gold_validation,
            "secret_findings": secret_findings,
            "monitoring_rows": monitoring_rows,
            "spark_version": spark.version,
        })

        validation_failures = {
            "bronze_failed": bronze_result.get("status") != "SUCCESS",
            "raw_vault_failed": [
                name
                for name, result in (
                    ("hubs", hubs_result),
                    ("links", links_result),
                    ("satellites", satellites_result),
                )
                if result.get("status") != "SUCCESS"
            ],
            "gold_failed": gold_result.get("status") != "SUCCESS",
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
            "monitoring_missing": monitoring_rows <= 0,
        }
        summary["validation_failures"] = validation_failures

        failed = any([
            validation_failures["bronze_failed"],
            bool(validation_failures["raw_vault_failed"]),
            validation_failures["gold_failed"],
            bool(validation_failures["masking_sample_failures"]),
            bool(validation_failures["forbidden_columns"]),
            bool(validation_failures["raw_pattern_hits"]),
            bool(validation_failures["protected_check_failures"]),
            bool(validation_failures["cliente_check_failures"]),
            bool(validation_failures["risco_check_failures"]),
            bool(validation_failures["secret_findings"]),
            validation_failures["monitoring_missing"],
        ])

        if failed:
            raise RuntimeError(f"Gold masking smoke validation failed: {validation_failures}")

        summary["status"] = "SUCCESS"
        return_code = 0

    except Exception as exc:
        summary["status"] = "FAILURE"
        summary["error"] = str(exc)
        return_code = 1

    finally:
        SparkSessionFactory.stop()
        print("GOLD_MASKING_SMOKE_RESULT=" + json.dumps(summary, sort_keys=True))

    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
