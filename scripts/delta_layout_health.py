#!/usr/bin/env python3
# Copyright (C) 2026 Alexandre Ferreira
# SPDX-License-Identifier: AGPL-3.0-only
"""Read-only, publication-safe Delta file-health evidence helpers."""

from __future__ import annotations

import math
import re
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence


SCHEMA_VERSION = 1
EVIDENCE_KIND = "data_master_delta_layout_health"
DECISION_STATES = {"KEEP_UNPARTITIONED", "OBSERVE", "EXPERIMENT", "ADOPT"}
OUTCOMES = {"NO_CHANGE", "INCONCLUSIVE", "EXPERIMENT_CANDIDATE"}
SHA_PATTERN = re.compile(r"[0-9a-f]{40}")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _nearest_rank(values: Sequence[int], percentile: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    rank = max(1, math.ceil(percentile * len(ordered)))
    return ordered[rank - 1]


def summarize_file_health(
    file_sizes_bytes: Iterable[int],
    *,
    small_file_threshold_bytes: int,
    physical_partition_count: int = 0,
    delta_version: int | None = None,
) -> dict[str, Any]:
    """Aggregate technical file metadata without reading business rows."""
    if small_file_threshold_bytes <= 0:
        raise ValueError("small_file_threshold_bytes must be positive")
    sizes = [int(size) for size in file_sizes_bytes]
    if any(size < 0 for size in sizes):
        raise ValueError("file sizes cannot be negative")
    if physical_partition_count < 0:
        raise ValueError("physical_partition_count cannot be negative")
    small_count = sum(size < small_file_threshold_bytes for size in sizes)
    return {
        "active_file_count": len(sizes),
        "active_bytes": sum(sizes),
        "min_file_bytes": min(sizes, default=0),
        "median_file_bytes": _nearest_rank(sizes, 0.50),
        "p95_file_bytes": _nearest_rank(sizes, 0.95),
        "max_file_bytes": max(sizes, default=0),
        "small_file_count": small_count,
        "small_file_ratio": round(small_count / len(sizes), 6) if sizes else 0.0,
        "small_file_threshold_bytes": small_file_threshold_bytes,
        "physical_partition_count": physical_partition_count,
        "delta_version": delta_version,
    }


def evaluate_layout_evidence(evidence: Mapping[str, Any]) -> dict[str, Any]:
    """Classify evidence without authorizing or executing a layout change."""
    required = {
        "representative_query_families",
        "repeated_runs_per_variant",
        "functional_fingerprint_equivalent",
        "file_health_complete",
        "performance_comparison_complete",
        "write_overhead_measured",
    }
    missing = sorted(required - set(evidence))
    if missing:
        return {"outcome": "INCONCLUSIVE", "reasons": [f"missing:{name}" for name in missing]}

    query_families = evidence["representative_query_families"]
    if not isinstance(query_families, list) or not query_families:
        return {"outcome": "INCONCLUSIVE", "reasons": ["representative_workload_missing"]}
    if int(evidence["repeated_runs_per_variant"]) < 3:
        return {"outcome": "INCONCLUSIVE", "reasons": ["insufficient_repeated_runs"]}
    if evidence["functional_fingerprint_equivalent"] is not True:
        return {"outcome": "NO_CHANGE", "reasons": ["functional_equivalence_not_proven"]}

    completeness = (
        evidence["file_health_complete"] is True
        and evidence["performance_comparison_complete"] is True
        and evidence["write_overhead_measured"] is True
    )
    if not completeness:
        return {"outcome": "INCONCLUSIVE", "reasons": ["comparison_incomplete"]}
    return {
        "outcome": "EXPERIMENT_CANDIDATE",
        "reasons": ["complete_evidence_requires_separate_review"],
    }


def build_evidence_document(
    *,
    revision: str,
    runtime_profile: str,
    policy_id: str,
    tables: Sequence[Mapping[str, Any]],
    workload_evidence: Mapping[str, Any] | None = None,
    captured_at: str | None = None,
) -> dict[str, Any]:
    """Build an aggregate evidence document with explicit claim limits."""
    if not SHA_PATTERN.fullmatch(revision):
        raise ValueError("revision must be a lowercase 40-character Git SHA")
    if not runtime_profile or not policy_id:
        raise ValueError("runtime_profile and policy_id are required")
    normalized_tables = []
    required_metrics = {
        "active_file_count",
        "active_bytes",
        "min_file_bytes",
        "median_file_bytes",
        "p95_file_bytes",
        "max_file_bytes",
        "small_file_count",
        "small_file_ratio",
        "small_file_threshold_bytes",
        "physical_partition_count",
        "delta_version",
    }
    for table in tables:
        if set(table) != {"table_id", "disposition", "metrics"}:
            raise ValueError("table evidence accepts only table_id, disposition and metrics")
        if table["disposition"] not in DECISION_STATES:
            raise ValueError(f"unsupported disposition: {table['disposition']}")
        if set(table["metrics"]) != required_metrics:
            raise ValueError(f"incomplete metrics for {table['table_id']}")
        normalized_tables.append(dict(table))

    decision = evaluate_layout_evidence(workload_evidence or {})
    if decision["outcome"] not in OUTCOMES:
        raise ValueError("unsupported decision outcome")
    return {
        "schema_version": SCHEMA_VERSION,
        "evidence_kind": EVIDENCE_KIND,
        "captured_at": captured_at or utc_now(),
        "source": {"revision": revision, "runtime_profile": runtime_profile},
        "policy_id": policy_id,
        "tables": sorted(normalized_tables, key=lambda item: item["table_id"]),
        "decision": decision,
        "privacy": {
            "classification": "technical_aggregate_only",
            "contains_business_payload": False,
            "contains_personal_data": False,
            "contains_secret_values": False,
        },
        "claim_limits": [
            "read_only_analysis_does_not_authorize_maintenance",
            "local_synthetic_results_do_not_establish_production_performance",
            "no_sla_cost_energy_or_financial_savings_claim",
        ],
    }


def collect_delta_table_health(
    spark: Any,
    *,
    table_id: str,
    path: str,
    small_file_threshold_bytes: int,
) -> dict[str, Any]:
    """Inspect active Delta file metadata through Spark/Hadoop without reading rows."""
    from delta.tables import DeltaTable

    frame = spark.read.format("delta").load(path)
    files = frame.inputFiles()
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    java_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path
    sizes = []
    partitions = set()
    for file_uri in files:
        candidate = java_path(file_uri)
        status = candidate.getFileSystem(hadoop_conf).getFileStatus(candidate)
        sizes.append(int(status.getLen()))
        partition_segments = tuple(
            segment for segment in file_uri.split("/") if "=" in segment
        )
        if partition_segments:
            partitions.add(partition_segments)

    history = DeltaTable.forPath(spark, path).history(1).select("version").collect()
    delta_version = int(history[0]["version"]) if history else None
    return {
        "table_id": table_id,
        "metrics": summarize_file_health(
            sizes,
            small_file_threshold_bytes=small_file_threshold_bytes,
            physical_partition_count=len(partitions),
            delta_version=delta_version,
        ),
    }
