#!/usr/bin/env python3
# Copyright (C) 2026 Alexandre Ferreira
# SPDX-License-Identifier: AGPL-3.0-only
"""Capture and compare aggregate local Minikube resource footprints."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

SCHEMA_VERSION = 1
SNAPSHOT_KIND = "data_master_runtime_footprint"
COMPARISON_KIND = "data_master_runtime_footprint_comparison"
PROFILE_NAME_PATTERN = re.compile(r"data-master-[A-Za-z0-9-]+")
FORBIDDEN_CANDIDATE_TOKENS = (
    "hive-metastore",
    "postgres-metastore",
    "bde2020/hive",
    "postgres:15",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def cpu_millicores(value: str | int | float | None) -> int:
    if value in (None, ""):
        return 0
    text = str(value).strip()
    suffixes = {"n": 1 / 1_000_000, "u": 1 / 1_000, "m": 1}
    if text[-1:] in suffixes:
        return int(round(float(text[:-1]) * suffixes[text[-1]]))
    return int(round(float(text) * 1000))


def memory_bytes(value: str | int | float | None) -> int:
    if value in (None, ""):
        return 0
    text = str(value).strip()
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)([A-Za-z]+)?", text)
    if not match:
        raise ValueError(f"Unsupported Kubernetes memory quantity: {value}")
    number = float(match.group(1))
    suffix = match.group(2) or ""
    factors = {
        "": 1,
        "K": 1000,
        "M": 1000**2,
        "G": 1000**3,
        "T": 1000**4,
        "Ki": 1024,
        "Mi": 1024**2,
        "Gi": 1024**3,
        "Ti": 1024**4,
    }
    if suffix not in factors:
        raise ValueError(f"Unsupported Kubernetes memory suffix: {suffix}")
    return int(round(number * factors[suffix]))


def _items(document: dict[str, Any]) -> list[dict[str, Any]]:
    items = document.get("items", [])
    if not isinstance(items, list):
        raise ValueError("Kubernetes list document has an invalid items field")
    return items


def summarize_pod_resources(pods: dict[str, Any]) -> dict[str, int]:
    totals = {
        "cpu_requests_millicores": 0,
        "cpu_limits_millicores": 0,
        "memory_requests_bytes": 0,
        "memory_limits_bytes": 0,
    }
    for pod in _items(pods):
        for container in pod.get("spec", {}).get("containers", []):
            resources = container.get("resources", {})
            requests = resources.get("requests", {})
            limits = resources.get("limits", {})
            totals["cpu_requests_millicores"] += cpu_millicores(requests.get("cpu"))
            totals["cpu_limits_millicores"] += cpu_millicores(limits.get("cpu"))
            totals["memory_requests_bytes"] += memory_bytes(requests.get("memory"))
            totals["memory_limits_bytes"] += memory_bytes(limits.get("memory"))
    return totals


def summarize_pvcs(pvcs: dict[str, Any]) -> int:
    return sum(
        memory_bytes(item.get("spec", {}).get("resources", {}).get("requests", {}).get("storage"))
        for item in _items(pvcs)
    )


def summarize_node_capacity(nodes: dict[str, Any]) -> dict[str, int]:
    return {
        "node_count": len(_items(nodes)),
        "cpu_millicores": sum(
            cpu_millicores(item.get("status", {}).get("capacity", {}).get("cpu"))
            for item in _items(nodes)
        ),
        "memory_bytes": sum(
            memory_bytes(item.get("status", {}).get("capacity", {}).get("memory"))
            for item in _items(nodes)
        ),
    }


def summarize_metrics(document: dict[str, Any]) -> dict[str, int]:
    cpu = 0
    memory = 0
    for pod in _items(document):
        for container in pod.get("containers", []):
            usage = container.get("usage", {})
            cpu += cpu_millicores(usage.get("cpu"))
            memory += memory_bytes(usage.get("memory"))
    return {"cpu_millicores": cpu, "memory_bytes": memory}


def aggregate_samples(samples: list[dict[str, Any]]) -> dict[str, int]:
    if not samples:
        raise ValueError("At least one metrics sample is required")
    cpu = [int(sample["cpu_millicores"]) for sample in samples]
    memory = [int(sample["memory_bytes"]) for sample in samples]
    return {
        "sample_count": len(samples),
        "cpu_average_millicores": int(round(sum(cpu) / len(cpu))),
        "cpu_peak_millicores": max(cpu),
        "memory_average_bytes": int(round(sum(memory) / len(memory))),
        "memory_peak_bytes": max(memory),
    }


def _run(command: list[str]) -> str:
    completed = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return completed.stdout.strip()


def _json_command(command: list[str]) -> dict[str, Any]:
    output = _run(command)
    if not output:
        raise RuntimeError(f"Command returned no JSON: {' '.join(command)}")
    value = json.loads(output)
    if not isinstance(value, dict):
        raise ValueError("Expected a JSON object from command")
    return value


def _kubectl_json(profile: str, arguments: Iterable[str]) -> dict[str, Any]:
    return _json_command(["minikube", "-p", profile, "kubectl", "--", *arguments])


def _resource_document(profile: str, resource: str, namespace: str) -> dict[str, Any]:
    return _kubectl_json(profile, ["get", resource, "-n", namespace, "-o", "json"])


def _names(document: dict[str, Any]) -> list[str]:
    return sorted(
        str(item.get("metadata", {}).get("name", ""))
        for item in _items(document)
        if item.get("metadata", {}).get("name")
    )


def _images(pods: dict[str, Any]) -> list[str]:
    values = {
        str(container.get("image"))
        for pod in _items(pods)
        for container in pod.get("spec", {}).get("containers", [])
        if container.get("image")
    }
    return sorted(values)


def capture_snapshot(args: argparse.Namespace) -> dict[str, Any]:
    if not PROFILE_NAME_PATTERN.fullmatch(args.profile):
        raise ValueError("Profile must be dedicated and start with data-master-")
    if args.samples < 2:
        raise ValueError("At least two samples are required")
    if args.interval_seconds < 1:
        raise ValueError("Sample interval must be at least one second")

    pods = _resource_document(args.profile, "pods", args.namespace)
    deployments = _resource_document(args.profile, "deployments", args.namespace)
    statefulsets = _resource_document(args.profile, "statefulsets", args.namespace)
    services = _resource_document(args.profile, "services", args.namespace)
    pvcs = _resource_document(args.profile, "pvc", args.namespace)
    secrets = _resource_document(args.profile, "secrets", args.namespace)
    applications = _resource_document(args.profile, "applications.argoproj.io", "argocd")
    nodes = _kubectl_json(args.profile, ["get", "nodes", "-o", "json"])

    samples: list[dict[str, Any]] = []
    metrics_path = f"/apis/metrics.k8s.io/v1beta1/namespaces/{args.namespace}/pods"
    for index in range(args.samples):
        metrics = _kubectl_json(args.profile, ["get", f"--raw={metrics_path}"])
        sample = summarize_metrics(metrics)
        sample["captured_at"] = utc_now()
        samples.append(sample)
        if index + 1 < args.samples:
            time.sleep(args.interval_seconds)

    collector_revision = _run(["git", "rev-parse", "HEAD"])
    branch = _run(["git", "branch", "--show-current"])
    deployed_revision = args.deployed_revision or collector_revision
    if not re.fullmatch(r"[0-9a-f]{40}", deployed_revision):
        raise ValueError("Deployed revision must be a full 40-character Git SHA")

    durations = {
        "bootstrap_seconds": args.bootstrap_duration_seconds,
        "e2e_seconds": args.e2e_duration_seconds,
    }
    snapshot = {
        "schema_version": SCHEMA_VERSION,
        "evidence_kind": SNAPSHOT_KIND,
        "captured_at": utc_now(),
        "variant": args.variant,
        "phase": args.phase,
        "source": {
            "collector_revision": collector_revision,
            "collector_branch": branch,
            "deployed_revision": deployed_revision,
        },
        "environment": {
            "profile": args.profile,
            "namespace": args.namespace,
            "node_capacity": summarize_node_capacity(nodes),
            "sample_interval_seconds": args.interval_seconds,
        },
        "object_counts": {
            "argocd_applications": len(_items(applications)),
            "deployments": len(_items(deployments)),
            "statefulsets": len(_items(statefulsets)),
            "pods": len(_items(pods)),
            "services": len(_items(services)),
            "persistent_volume_claims": len(_items(pvcs)),
            "secrets": len(_items(secrets)),
        },
        "configured_resources": {
            **summarize_pod_resources(pods),
            "pvc_requested_bytes": summarize_pvcs(pvcs),
        },
        "observed_resources": {
            "samples": samples,
            "aggregate": aggregate_samples(samples),
        },
        "inventory": {
            "workloads": sorted(set(_names(deployments) + _names(statefulsets))),
            "images": _images(pods),
        },
        "durations": durations,
        "privacy": {
            "classification": "technical_aggregate_only",
            "contains_personal_data": False,
            "contains_secret_values": False,
        },
        "limitations": [
            "local_single_node_minikube_measurement",
            "resource_metrics_depend_on_metrics_server_sampling",
            "no_financial_or_energy_savings_claim",
        ],
    }
    validate_snapshot(snapshot)
    return snapshot


def validate_snapshot(snapshot: dict[str, Any]) -> None:
    if snapshot.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("Unsupported footprint schema version")
    if snapshot.get("evidence_kind") != SNAPSHOT_KIND:
        raise ValueError("Unsupported footprint evidence kind")
    if snapshot.get("variant") not in {"baseline", "candidate"}:
        raise ValueError("Footprint variant must be baseline or candidate")
    if snapshot.get("phase") not in {"ready-idle", "e2e-active", "post-e2e"}:
        raise ValueError("Unsupported measurement phase")
    aggregate = snapshot.get("observed_resources", {}).get("aggregate", {})
    if int(aggregate.get("sample_count", 0)) < 2:
        raise ValueError("Footprint evidence requires at least two metrics samples")
    privacy = snapshot.get("privacy", {})
    if privacy.get("contains_personal_data") is not False:
        raise ValueError("Footprint evidence must not contain personal data")
    if privacy.get("contains_secret_values") is not False:
        raise ValueError("Footprint evidence must not contain secret values")


def _delta(baseline: int | float, candidate: int | float) -> dict[str, Any]:
    saved = baseline - candidate
    percent = None if baseline == 0 else round((saved / baseline) * 100, 3)
    return {"baseline": baseline, "candidate": candidate, "saved": saved, "saved_percent": percent}


def compare_snapshots(baseline: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    validate_snapshot(baseline)
    validate_snapshot(candidate)
    if baseline["variant"] != "baseline" or candidate["variant"] != "candidate":
        raise ValueError("Comparison requires baseline then candidate evidence")
    for field in ("phase",):
        if baseline[field] != candidate[field]:
            raise ValueError(f"Comparison mismatch for {field}")
    for field in ("namespace", "node_capacity", "sample_interval_seconds"):
        if baseline["environment"][field] != candidate["environment"][field]:
            raise ValueError(f"Comparison mismatch for environment.{field}")
    if baseline["observed_resources"]["aggregate"]["sample_count"] != candidate["observed_resources"]["aggregate"]["sample_count"]:
        raise ValueError("Comparison requires the same metrics sample count")

    candidate_inventory = json.dumps(candidate["inventory"], sort_keys=True).lower()
    present = [token for token in FORBIDDEN_CANDIDATE_TOKENS if token in candidate_inventory]
    if present:
        raise ValueError(f"Candidate still contains removed catalog components: {present}")

    object_counts = {
        key: _delta(int(baseline["object_counts"][key]), int(candidate["object_counts"][key]))
        for key in baseline["object_counts"]
    }
    configured = {
        key: _delta(int(baseline["configured_resources"][key]), int(candidate["configured_resources"][key]))
        for key in baseline["configured_resources"]
    }
    observed = {
        key: _delta(
            int(baseline["observed_resources"]["aggregate"][key]),
            int(candidate["observed_resources"]["aggregate"][key]),
        )
        for key in (
            "cpu_average_millicores",
            "cpu_peak_millicores",
            "memory_average_bytes",
            "memory_peak_bytes",
        )
    }
    durations: dict[str, Any] = {}
    for key in ("bootstrap_seconds", "e2e_seconds"):
        before = baseline.get("durations", {}).get(key)
        after = candidate.get("durations", {}).get(key)
        durations[key] = None if before is None or after is None else _delta(float(before), float(after))

    baseline_workloads = set(baseline["inventory"]["workloads"])
    candidate_workloads = set(candidate["inventory"]["workloads"])
    baseline_images = set(baseline["inventory"]["images"])
    candidate_images = set(candidate["inventory"]["images"])
    return {
        "schema_version": SCHEMA_VERSION,
        "evidence_kind": COMPARISON_KIND,
        "captured_at": utc_now(),
        "phase": baseline["phase"],
        "baseline": {
            "deployed_revision": baseline["source"]["deployed_revision"],
            "captured_at": baseline["captured_at"],
        },
        "candidate": {
            "deployed_revision": candidate["source"]["deployed_revision"],
            "captured_at": candidate["captured_at"],
        },
        "savings": {
            "object_counts": object_counts,
            "configured_resources": configured,
            "observed_resources": observed,
            "durations": durations,
            "removed_workloads": sorted(baseline_workloads - candidate_workloads),
            "removed_images": sorted(baseline_images - candidate_images),
        },
        "functional_equivalence": {
            "status": "REQUIRES_SEPARATE_E2E_EVIDENCE",
            "evidence_reference": None,
        },
        "claim_limits": [
            "local_single_node_minikube_only",
            "observed_sampling_not_capacity_planning",
            "no_financial_or_energy_savings_claim",
        ],
    }


def _canonical_bytes(value: dict[str, Any]) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8")


def write_document(path: Path, value: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = _canonical_bytes(value)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(payload)
    temporary.replace(path)
    digest = hashlib.sha256(payload).hexdigest()
    path.with_suffix(path.suffix + ".sha256").write_text(f"{digest}  {path.name}\n", encoding="ascii")
    return digest


def read_document(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Evidence must be a JSON object: {path}")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    capture = subparsers.add_parser("capture", help="capture one aggregate footprint")
    capture.add_argument("--profile", required=True)
    capture.add_argument("--variant", choices=("baseline", "candidate"), required=True)
    capture.add_argument("--phase", choices=("ready-idle", "e2e-active", "post-e2e"), required=True)
    capture.add_argument("--output", type=Path, required=True)
    capture.add_argument("--namespace", default="data-platform")
    capture.add_argument("--samples", type=int, default=6)
    capture.add_argument("--interval-seconds", type=int, default=10)
    capture.add_argument("--deployed-revision")
    capture.add_argument("--bootstrap-duration-seconds", type=float)
    capture.add_argument("--e2e-duration-seconds", type=float)

    compare = subparsers.add_parser("compare", help="compare equivalent baseline and candidate footprints")
    compare.add_argument("--baseline", type=Path, required=True)
    compare.add_argument("--candidate", type=Path, required=True)
    compare.add_argument("--output", type=Path, required=True)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.command == "capture":
        document = capture_snapshot(args)
        output = args.output
        marker = "RUNTIME_FOOTPRINT_CAPTURE_STATUS"
    else:
        baseline = read_document(args.baseline)
        candidate = read_document(args.candidate)
        document = compare_snapshots(baseline, candidate)
        document["baseline"]["document_sha256"] = hashlib.sha256(args.baseline.read_bytes()).hexdigest()
        document["candidate"]["document_sha256"] = hashlib.sha256(args.candidate.read_bytes()).hexdigest()
        output = args.output
        marker = "RUNTIME_FOOTPRINT_COMPARISON_STATUS"
    digest = write_document(output, document)
    print(f"{marker}=PASS")
    print(f"RUNTIME_FOOTPRINT_EVIDENCE_SHA256={digest}")
    print(f"RUNTIME_FOOTPRINT_EVIDENCE_PATH={output.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())