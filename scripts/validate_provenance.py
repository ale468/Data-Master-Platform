#!/usr/bin/env python3
# Copyright (C) 2026 Alexandre Ferreira
# SPDX-License-Identifier: AGPL-3.0-only

"""Fail-closed validation of the public provenance and copyright policy."""

from __future__ import annotations

import argparse
import fnmatch
import json
import subprocess
import sys
from pathlib import Path
from typing import Iterable, List, Mapping, Sequence


POLICY_PATH = Path("config/provenance/copyright-policy.json")
REQUIRED_DOCUMENTS = ("LICENSE", "COPYRIGHT", "NOTICE", "PROVENANCE.md", "README.md")


def _run_git(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(root), *args],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def tracked_files(root: Path) -> List[Path]:
    result = _run_git(
        root,
        "ls-files",
        "-z",
        "--cached",
        "--others",
        "--exclude-standard",
    )
    if result.returncode != 0:
        raise RuntimeError("git ls-files failed; run this validator inside a Git clone")
    return [root / item for item in result.stdout.split("\0") if item]


def load_policy(root: Path) -> Mapping[str, object]:
    path = root / POLICY_PATH
    try:
        policy = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"invalid provenance policy: {POLICY_PATH}") from exc
    if not isinstance(policy, Mapping):
        raise RuntimeError(f"invalid provenance policy: {POLICY_PATH}")
    return policy


def _relative(root: Path, path: Path) -> str:
    return path.relative_to(root).as_posix()


def _matches(relative: str, patterns: Iterable[str]) -> bool:
    return any(fnmatch.fnmatchcase(relative, pattern) for pattern in patterns)


def _review_patterns(policy: Mapping[str, object]) -> List[str]:
    entries = policy.get("review_required_patterns", [])
    if not isinstance(entries, list):
        return []
    return [
        str(entry["pattern"])
        for entry in entries
        if isinstance(entry, Mapping) and isinstance(entry.get("pattern"), str)
    ]


def _is_source(relative: str, policy: Mapping[str, object]) -> bool:
    extensions = {str(value) for value in policy.get("source_extensions", [])}
    filenames = {str(value) for value in policy.get("source_filenames", [])}
    return Path(relative).suffix in extensions or Path(relative).name in filenames


def classify_source(
    relative: str,
    policy: Mapping[str, object],
    legacy_paths: Iterable[str] = (),
) -> str:
    if not _is_source(relative, policy):
        return "not_source"
    owned_patterns = [str(value) for value in policy.get("owned_source_patterns", [])]
    if _matches(relative, owned_patterns):
        return "owned"
    if relative in set(legacy_paths):
        return "legacy_review"
    if _matches(relative, _review_patterns(policy)):
        return "review_required"
    return "unclassified"


def legacy_source_paths(
    root: Path,
    policy: Mapping[str, object],
) -> set[str]:
    baseline = str(policy.get("legacy_review_baseline_commit", ""))
    if not baseline:
        raise RuntimeError("missing legacy_review_baseline_commit in provenance policy")
    result = _run_git(root, "ls-tree", "-r", "--name-only", baseline)
    if result.returncode != 0:
        raise RuntimeError(
            "legacy provenance baseline is unavailable; use a full Git checkout"
        )
    return {line for line in result.stdout.splitlines() if line}


def _header_offset(lines: Sequence[str]) -> int:
    return 1 if lines and lines[0].startswith("#!") else 0


def validate_source_headers(
    root: Path,
    files: Sequence[Path],
    policy: Mapping[str, object],
    legacy_paths: Iterable[str] = (),
) -> List[str]:
    failures: List[str] = []
    copyright_notice = str(policy.get("copyright_notice", ""))
    license_expression = str(policy.get("license_expression", ""))
    expected = (
        f"# {copyright_notice}",
        f"# SPDX-License-Identifier: {license_expression}",
    )

    for path in files:
        relative = _relative(root, path)
        classification = classify_source(relative, policy, legacy_paths)
        if classification == "unclassified":
            failures.append(f"source.unclassified:{relative}")
            continue
        if classification != "owned":
            continue
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except (OSError, UnicodeDecodeError):
            failures.append(f"source.unreadable:{relative}")
            continue
        offset = _header_offset(lines)
        actual = tuple(lines[offset : offset + 2])
        if actual != expected:
            failures.append(f"header.missing_or_invalid:{relative}")

    return sorted(set(failures))


def validate_no_project_claims(
    root: Path,
    policy: Mapping[str, object],
) -> List[str]:
    failures: List[str] = []
    copyright_notice = str(policy.get("copyright_notice", ""))
    license_line = (
        "SPDX-License-Identifier: "
        + str(policy.get("license_expression", ""))
    )
    paths = policy.get("files_without_project_copyright", [])
    if not isinstance(paths, list):
        return ["policy.invalid:files_without_project_copyright"]

    for relative_value in paths:
        relative = str(relative_value)
        path = root / relative
        if not path.is_file():
            failures.append(f"exception.missing:{relative}")
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            failures.append(f"exception.unreadable:{relative}")
            continue
        if copyright_notice in text:
            failures.append(f"exception.project_copyright_present:{relative}")
        if license_line in text:
            failures.append(f"exception.project_spdx_present:{relative}")

    return sorted(set(failures))


def validate_documents(root: Path, policy: Mapping[str, object]) -> List[str]:
    failures: List[str] = []
    canonical = str(policy.get("canonical_repository", ""))
    license_expression = str(policy.get("license_expression", ""))
    copyright_notice = str(policy.get("copyright_notice", ""))

    for relative in REQUIRED_DOCUMENTS:
        path = root / relative
        if not path.is_file():
            failures.append(f"document.missing:{relative}")
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            failures.append(f"document.unreadable:{relative}")
            continue
        if relative != "LICENSE":
            if canonical not in text:
                failures.append(f"document.canonical_repository_missing:{relative}")
            if license_expression not in text:
                failures.append(f"document.license_expression_missing:{relative}")
        if relative in {"COPYRIGHT", "NOTICE", "README.md"} and copyright_notice not in text:
            failures.append(f"document.copyright_notice_missing:{relative}")

    notice_path = root / "NOTICE"
    if notice_path.is_file():
        notice = notice_path.read_text(encoding="utf-8").casefold()
        components = policy.get("notice_components", [])
        if not isinstance(components, list):
            failures.append("policy.invalid:notice_components")
        else:
            for component in components:
                if str(component).casefold() not in notice:
                    failures.append(f"notice.component_missing:{component}")

    return sorted(set(failures))


def validate_policy(policy: Mapping[str, object]) -> List[str]:
    failures: List[str] = []
    if policy.get("schema_version") != 1:
        failures.append("policy.invalid:schema_version")
    for key in (
        "canonical_repository",
        "copyright_notice",
        "license_expression",
        "legacy_review_baseline_commit",
        "owned_source_patterns",
        "review_required_patterns",
    ):
        if not policy.get(key):
            failures.append(f"policy.missing:{key}")
    return sorted(set(failures))


def run_checks(root: Path) -> List[str]:
    policy = load_policy(root)
    files = tracked_files(root)
    legacy_paths = legacy_source_paths(root, policy)
    failures: List[str] = []
    failures.extend(validate_policy(policy))
    failures.extend(validate_documents(root, policy))
    failures.extend(validate_source_headers(root, files, policy, legacy_paths))
    failures.extend(validate_no_project_claims(root, policy))
    return sorted(set(failures))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate copyright, SPDX, attribution and canonical provenance."
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path.cwd(),
        help="Repository root. Defaults to the current directory.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    try:
        failures = run_checks(args.root.resolve())
    except RuntimeError as exc:
        print(f"PROVENANCE_COMPLIANCE_ERROR={exc}", file=sys.stderr)
        return 2

    for failure in failures:
        print(f"PROVENANCE_COMPLIANCE_FAILURE={failure}")
    status = "SUCCESS" if not failures else "FAILURE"
    print(f"PROVENANCE_COMPLIANCE_STATUS={status}")
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
