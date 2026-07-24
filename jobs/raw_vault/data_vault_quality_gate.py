"""Executable Data Vault quality gate used by DM-DV-004."""

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Set, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../common"))

from delta_io import DeltaIO
from config import Config


HUB_GATE_SPECS: Dict[str, Dict[str, Any]] = {
    "hub_cliente": {"hash_key": "hk_cliente", "business_keys": ["cliente_id"]},
    "hub_conta": {"hash_key": "hk_conta", "business_keys": ["conta_id"]},
    "hub_cartao": {"hash_key": "hk_cartao", "business_keys": ["cartao_id"]},
    "hub_transacao": {"hash_key": "hk_transacao", "business_keys": ["transacao_id"]},
    "hub_agencia": {"hash_key": "hk_agencia", "business_keys": ["agencia_id"]},
    "hub_produto": {"hash_key": "hk_produto", "business_keys": ["produto_id"]},
    "hub_canal_digital": {"hash_key": "hk_canal_digital", "business_keys": ["canal_id"]},
}

LINK_GATE_SPECS: Dict[str, Dict[str, Any]] = {
    "link_cliente_conta": {"hk_cliente": "hub_cliente", "hk_conta": "hub_conta"},
    "link_conta_transacao": {"hk_conta": "hub_conta", "hk_transacao": "hub_transacao"},
    "link_cliente_cartao": {"hk_cliente": "hub_cliente", "hk_cartao": "hub_cartao"},
    "link_cartao_transacao": {"hk_cartao": "hub_cartao", "hk_transacao": "hub_transacao"},
    "link_conta_agencia": {"hk_conta": "hub_conta", "hk_agencia": "hub_agencia"},
    "link_conta_produto": {"hk_conta": "hub_conta", "hk_produto": "hub_produto"},
    "link_cliente_evento_digital": {
        "hk_cliente": "hub_cliente",
        "hk_canal_digital": "hub_canal_digital",
    },
}

SATELLITE_GATE_SPECS: Dict[str, Dict[str, Any]] = {
    "sat_cliente_dados_cadastrais": {
        "parent_key": "hk_cliente",
        "hashdiff": "hd_cliente_dados",
        "hub": "hub_cliente",
        "pii": True,
    },
    "sat_cliente_documentos": {
        "parent_key": "hk_cliente",
        "hashdiff": "hd_cliente_documentos",
        "hub": "hub_cliente",
        "pii": True,
    },
    "sat_conta_detalhes": {
        "parent_key": "hk_conta",
        "hashdiff": "hd_conta_detalhes",
        "hub": "hub_conta",
        "pii": True,
    },
    "sat_cartao_detalhes": {
        "parent_key": "hk_cartao",
        "hashdiff": "hd_cartao_detalhes",
        "hub": "hub_cartao",
        "pii": True,
    },
    "sat_transacao_detalhes": {
        "parent_key": "hk_transacao",
        "hashdiff": "hd_transacao_detalhes",
        "hub": "hub_transacao",
        "pii": True,
    },
    "sat_agencia_detalhes": {
        "parent_key": "hk_agencia",
        "hashdiff": "hd_agencia_detalhes",
        "hub": "hub_agencia",
        "pii": True,
    },
    "sat_produto_detalhes": {
        "parent_key": "hk_produto",
        "hashdiff": "hd_produto_detalhes",
        "hub": "hub_produto",
        "pii": False,
    },
    "sat_evento_digital_detalhes": {
        "parent_key": "hk_canal_digital",
        "hashdiff": "hd_evento_digital_detalhes",
        "hub": "hub_canal_digital",
        "pii": True,
    },
}

EXPECTED_GOLD_TABLES = (
    "gold_transacoes_por_dia",
    "gold_transacoes_por_cliente",
    "gold_volume_por_produto",
    "gold_eventos_digitais_por_canal",
    "gold_contas_por_agencia",
    "gold_risco_transacional_simplificado",
    "gold_clientes_protegidos",
)

FORBIDDEN_GOLD_COLUMNS = {
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

RAW_PII_PATTERNS = (
    r"\d{3}\.\d{3}\.\d{3}-\d{2}",
    r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
    r"\+55\s*\(\d{2}\)\s*\d{5}-\d{4}",
    r"\d{4}-\d{4}-\d{4}-\d{4}",
)


def _is_blank(column: str):
    return F.col(column).isNull() | (F.trim(F.col(column).cast("string")) == "")


def _duplicate_count(df: DataFrame, columns: Iterable[str]) -> int:
    return df.groupBy(*columns).count().filter(F.col("count") > 1).count()


def _required_columns_failure(
    df: DataFrame,
    required: Iterable[str],
    prefix: str,
) -> List[str]:
    missing = sorted(set(required) - set(df.columns))
    return [f"{prefix}.missing_columns:{','.join(missing)}"] if missing else []


def validate_hubs(
    hubs: Mapping[str, DataFrame],
    specs: Mapping[str, Mapping[str, Any]],
) -> List[str]:
    failures: List[str] = []
    for name, spec in specs.items():
        if name not in hubs:
            failures.append(f"hub.{name}.missing_table")
            continue
        df = hubs[name]
        hash_key = str(spec["hash_key"])
        business_keys = list(spec["business_keys"])
        required = [hash_key, *business_keys, "load_datetime", "record_source", "batch_id"]
        missing = _required_columns_failure(df, required, f"hub.{name}")
        failures.extend(missing)
        if missing:
            continue
        if df.count() <= 0:
            failures.append(f"hub.{name}.empty")
        if df.filter(_is_blank(hash_key)).limit(1).count():
            failures.append(f"hub.{name}.null_hash_key")
        for business_key in business_keys:
            if df.filter(_is_blank(business_key)).limit(1).count():
                failures.append(f"hub.{name}.null_business_key:{business_key}")
        if _duplicate_count(df, [hash_key]):
            failures.append(f"hub.{name}.duplicate_hash_key")
    return failures


def validate_links(
    links: Mapping[str, DataFrame],
    hubs: Mapping[str, DataFrame],
    specs: Mapping[str, Mapping[str, str]],
    hub_specs: Mapping[str, Mapping[str, Any]],
) -> List[str]:
    failures: List[str] = []
    for name, references in specs.items():
        if name not in links:
            failures.append(f"link.{name}.missing_table")
            continue
        df = links[name]
        role_columns = list(references.keys())
        required = ["hk_link", *role_columns, "load_datetime", "record_source", "batch_id"]
        missing = _required_columns_failure(df, required, f"link.{name}")
        failures.extend(missing)
        if missing:
            continue
        if len(set(role_columns)) < 2:
            failures.append(f"link.{name}.roles_not_preserved")
        if df.count() <= 0:
            failures.append(f"link.{name}.empty")
        if df.filter(_is_blank("hk_link")).limit(1).count():
            failures.append(f"link.{name}.null_hash_key")
        if _duplicate_count(df, ["hk_link"]):
            failures.append(f"link.{name}.duplicate_hash_key")
        for link_column, hub_name in references.items():
            if df.filter(_is_blank(link_column)).limit(1).count():
                failures.append(f"link.{name}.null_role:{link_column}")
                continue
            if hub_name not in hubs:
                failures.append(f"link.{name}.missing_hub:{hub_name}")
                continue
            hub_key = str(hub_specs[hub_name]["hash_key"])
            orphan_rows = df.select(link_column).dropDuplicates().join(
                hubs[hub_name].select(F.col(hub_key).alias(link_column)).dropDuplicates(),
                on=link_column,
                how="left_anti",
            )
            orphan_count = orphan_rows.count()
            if orphan_count:
                sample = orphan_rows.limit(1).collect()[0][link_column]
                failures.append(
                    f"link.{name}.orphan:{link_column}:count={orphan_count}:sample={sample}"
                )
    return failures


def validate_satellites(
    satellites: Mapping[str, DataFrame],
    hubs: Mapping[str, DataFrame],
    specs: Mapping[str, Mapping[str, Any]],
    hub_specs: Mapping[str, Mapping[str, Any]],
) -> List[str]:
    failures: List[str] = []
    for name, spec in specs.items():
        if name not in satellites:
            failures.append(f"satellite.{name}.missing_table")
            continue
        df = satellites[name]
        parent_key = str(spec["parent_key"])
        hashdiff = str(spec["hashdiff"])
        required = [
            parent_key,
            hashdiff,
            "load_datetime",
            "record_source",
            "effective_from",
            "batch_id",
        ]
        missing = _required_columns_failure(df, required, f"satellite.{name}")
        failures.extend(missing)
        if missing:
            continue
        if df.count() <= 0:
            failures.append(f"satellite.{name}.empty")
        if df.filter(_is_blank(parent_key) | _is_blank(hashdiff)).limit(1).count():
            failures.append(f"satellite.{name}.null_key_or_hashdiff")
        hub_name = str(spec["hub"])
        if hub_name not in hubs:
            failures.append(f"satellite.{name}.missing_hub:{hub_name}")
        else:
            hub_key = str(hub_specs[hub_name]["hash_key"])
            orphans = df.select(parent_key).dropDuplicates().join(
                hubs[hub_name].select(F.col(hub_key).alias(parent_key)).dropDuplicates(),
                on=parent_key,
                how="left_anti",
            ).count()
            if orphans:
                failures.append(f"satellite.{name}.orphan_parent")
        if df.filter(
            F.col("load_datetime").isNull()
            | F.col("effective_from").isNull()
            | (F.col("effective_from") > F.col("load_datetime"))
        ).limit(1).count():
            failures.append(f"satellite.{name}.invalid_temporal_order")
        order_by = [
            F.col("load_datetime").asc_nulls_last(),
            F.col("effective_from").asc_nulls_last(),
            F.col("batch_id").asc_nulls_last(),
            F.col(hashdiff).asc_nulls_last(),
        ]
        window = Window.partitionBy(parent_key).orderBy(*order_by)
        consecutive = df.withColumn(
            "__previous_hashdiff", F.lag(hashdiff).over(window)
        ).filter(F.col(hashdiff) == F.col("__previous_hashdiff")).limit(1).count()
        if consecutive:
            failures.append(f"satellite.{name}.consecutive_duplicate")
    return failures


def validate_lineage(
    frames: Iterable[Tuple[str, DataFrame]],
) -> List[str]:
    failures: List[str] = []
    for name, df in frames:
        required = {"load_datetime", "record_source", "batch_id"}
        missing = sorted(required - set(df.columns))
        if missing:
            failures.append(f"lineage.{name}.missing:{','.join(missing)}")
            continue
        invalid = df.filter(
            _is_blank("record_source")
            | _is_blank("batch_id")
            | F.col("load_datetime").isNull()
            | ~F.col("record_source").rlike(r"^[^:]+:[^:]+$")
        ).limit(1).count()
        if invalid:
            failures.append(f"lineage.{name}.invalid_metadata")
    return failures


def validate_gold(
    gold_tables: Mapping[str, DataFrame],
    expected_tables: Iterable[str],
    gold_source_text: str,
) -> List[str]:
    failures: List[str] = []
    for name in expected_tables:
        if name not in gold_tables:
            failures.append(f"gold.{name}.missing_table")
            continue
        df = gold_tables[name]
        if df.count() <= 0:
            failures.append(f"gold.{name}.empty")
        forbidden = sorted(FORBIDDEN_GOLD_COLUMNS.intersection(df.columns))
        if forbidden:
            failures.append(f"gold.{name}.direct_pii:{','.join(forbidden)}")
        string_columns = [
            field.name
            for field in df.schema.fields
            if isinstance(field.dataType, StringType)
        ]
        if string_columns:
            pattern = "(?:" + ")|(?:".join(RAW_PII_PATTERNS) + ")"
            predicate = None
            for column in string_columns:
                condition = F.col(column).rlike(pattern)
                predicate = condition if predicate is None else predicate | condition
            if df.filter(predicate).limit(1).count():
                failures.append(f"gold.{name}.raw_pii_pattern")

    forbidden_source_markers = ("_read_required_bronze", "bronze_path", "/bronze")
    if any(marker in gold_source_text for marker in forbidden_source_markers):
        failures.append("gold.lineage.reads_bronze")
    if "raw_vault->business_vault_latest->gold" not in gold_source_text:
        failures.append("gold.lineage.marker_missing")
    return failures


def validate_lgpd_references(
    satellite_specs: Mapping[str, Mapping[str, Any]],
    classified_pii_satellites: Set[str],
) -> List[str]:
    expected = {
        name for name, spec in satellite_specs.items() if bool(spec.get("pii"))
    }
    missing = sorted(expected - classified_pii_satellites)
    return [f"lgpd.unclassified_pii_satellites:{','.join(missing)}"] if missing else []


def validate_gold_storage_paths(
    gold_path: str,
    gold_table_paths: Mapping[str, str],
    expected_tables: Iterable[str],
) -> List[str]:
    """Valida que todas as tabelas Gold estão sob o root físico Gold."""
    if not isinstance(gold_path, str) or not gold_path.strip():
        return ["gold.storage.missing_root"]
    if not isinstance(gold_table_paths, Mapping):
        return ["gold.storage.invalid_registry"]

    root = gold_path.rstrip("/")
    failures: List[str] = []
    for name in expected_tables:
        expected_path = f"{root}/{name}"
        if gold_table_paths.get(name) != expected_path:
            failures.append(f"gold.storage.invalid_path:{name}")
    return failures


def validate_business_vault_gold_path_separation(
    business_vault_path: str,
    gold_path: str,
) -> List[str]:
    """Impede que a Business Vault lógica e a Gold compartilhem o mesmo root."""
    if not isinstance(business_vault_path, str) or not business_vault_path.strip():
        return ["gold.storage.missing_business_vault_root"]
    if not isinstance(gold_path, str) or not gold_path.strip():
        return ["gold.storage.missing_gold_root"]
    if business_vault_path.rstrip("/") == gold_path.rstrip("/"):
        return ["gold.storage.business_vault_gold_same_root"]
    return []


def evaluate_data_vault_gate(
    hubs: Mapping[str, DataFrame],
    links: Mapping[str, DataFrame],
    satellites: Mapping[str, DataFrame],
    gold_tables: Mapping[str, DataFrame],
    classified_pii_satellites: Set[str],
    gold_source_text: str,
    hub_specs: Mapping[str, Mapping[str, Any]] = HUB_GATE_SPECS,
    link_specs: Mapping[str, Mapping[str, str]] = LINK_GATE_SPECS,
    satellite_specs: Mapping[str, Mapping[str, Any]] = SATELLITE_GATE_SPECS,
    expected_gold_tables: Iterable[str] = EXPECTED_GOLD_TABLES,
    business_vault_path: str = "",
    gold_path: str = "",
    gold_table_paths: Mapping[str, str] = None,
) -> Dict[str, Any]:
    expected_gold_tables = tuple(expected_gold_tables)
    groups = {
        "hubs": validate_hubs(hubs, hub_specs),
        "links": validate_links(links, hubs, link_specs, hub_specs),
        "satellites": validate_satellites(
            satellites, hubs, satellite_specs, hub_specs
        ),
        "lineage": validate_lineage(
            [(f"hub.{name}", df) for name, df in hubs.items()]
            + [(f"link.{name}", df) for name, df in links.items()]
            + [(f"satellite.{name}", df) for name, df in satellites.items()]
        ),
        "gold_lineage": validate_gold(
            gold_tables, expected_gold_tables, gold_source_text
        ),
        "lgpd_reference": validate_lgpd_references(
            satellite_specs, classified_pii_satellites
        ),
        "gold_storage": validate_gold_storage_paths(
            gold_path,
            gold_table_paths,
            expected_gold_tables,
        ),
        "gold_path_separation": validate_business_vault_gold_path_separation(
            business_vault_path,
            gold_path,
        ),
    }
    failed_checks = [failure for failures in groups.values() for failure in failures]
    return {
        "status": "PASS" if not failed_checks else "FAILED",
        "statuses": {
            name: "PASS" if not failures else "FAILED"
            for name, failures in groups.items()
        },
        "failed_checks": failed_checks,
    }


def render_gate_output(result: Mapping[str, Any]) -> str:
    statuses = result["statuses"]
    lines = [
        f"DATA_VAULT_HUBS_STATUS={statuses['hubs']}",
        f"DATA_VAULT_LINKS_STATUS={statuses['links']}",
        f"DATA_VAULT_SATELLITES_STATUS={statuses['satellites']}",
        f"DATA_VAULT_LINEAGE_STATUS={statuses['lineage']}",
        f"DATA_VAULT_GOLD_LINEAGE_STATUS={statuses['gold_lineage']}",
        f"DATA_VAULT_LGPD_REFERENCE_STATUS={statuses['lgpd_reference']}",
        f"GOLD_STORAGE_PATH_STATUS={statuses['gold_storage']}",
        "BUSINESS_VAULT_GOLD_PATH_SEPARATION_STATUS="
        f"{statuses['gold_path_separation']}",
        f"DATA_VAULT_QUALITY_GATE_STATUS={result['status']}",
    ]
    if result["failed_checks"]:
        lines.append("FAILED_CHECKS=" + ",".join(result["failed_checks"]))
    lines.append(
        "DATA_VAULT_QUALITY_GATE_RESULT="
        + json.dumps(result, ensure_ascii=False, sort_keys=True)
    )
    return "\n".join(lines)


def gate_exit_code(result: Mapping[str, Any]) -> int:
    return 0 if result["status"] == "PASS" else 1


def load_configured_frames(
    spark: SparkSession,
    raw_vault_path: str,
    gold_path: str,
) -> Tuple[Dict[str, DataFrame], Dict[str, DataFrame], Dict[str, DataFrame], Dict[str, DataFrame]]:
    def required(path: str) -> DataFrame:
        df = DeltaIO.read_delta(spark, path)
        if df is None:
            raise FileNotFoundError(f"Tabela obrigatória do gate não encontrada: {path}")
        return df

    hubs = {
        name: required(f"{raw_vault_path}/hubs/{name}")
        for name in HUB_GATE_SPECS
    }
    links = {
        name: required(f"{raw_vault_path}/links/{name}")
        for name in LINK_GATE_SPECS
    }
    satellites = {
        name: required(f"{raw_vault_path}/satellites/{name}")
        for name in SATELLITE_GATE_SPECS
    }
    gold_tables = {
        name: required(f"{gold_path}/{name}")
        for name in EXPECTED_GOLD_TABLES
    }
    return hubs, links, satellites, gold_tables


def classified_pii_satellites_from_config(config_path: Path) -> Set[str]:
    import yaml

    document = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    result = set()
    for table_name, table in document.get("tables", {}).items():
        if (
            table_name.startswith("raw_vault.sat_")
            and table.get("pii_boundary") == "restricted_pii_satellite"
        ):
            result.add(table_name.split(".", 1)[1])
    return result


def evaluate_configured_gate(
    spark: SparkSession,
    raw_vault_path: str,
    gold_path: str,
    repo_root: Path,
) -> Dict[str, Any]:
    hubs, links, satellites, gold_tables = load_configured_frames(
        spark, raw_vault_path, gold_path
    )
    classification = classified_pii_satellites_from_config(
        repo_root / "config" / "privacy" / "data-classification.yml"
    )
    gold_source = (
        repo_root / "jobs" / "business_vault" / "load_gold.py"
    ).read_text(encoding="utf-8")
    return evaluate_data_vault_gate(
        hubs,
        links,
        satellites,
        gold_tables,
        classification,
        gold_source,
        business_vault_path=Config.BUSINESS_VAULT_PATH,
        gold_path=gold_path,
        gold_table_paths=Config.GOLD_TABLES,
    )
