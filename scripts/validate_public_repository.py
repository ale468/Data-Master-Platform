#!/usr/bin/env python3
"""Fail-closed validation for the public repository boundary and text assets."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Iterable, List, Mapping, Sequence, Tuple
from urllib.parse import unquote

try:
    import yaml
except ImportError:  # pragma: no cover - exercised by dependency preflight.
    yaml = None


ALLOWED_TOP_LEVEL_DIRECTORIES = {
    ".github",
    "config",
    "dags",
    "data",
    "infra",
    "jobs",
    "scripts",
    "tests",
}

ALLOWED_ROOT_FILES = {
    ".dockerignore",
    ".gitattributes",
    ".gitignore",
    ".pre-commit-config.yaml",
    "AGENTS.md",
    "CODE_OF_CONDUCT.md",
    "CONTRIBUTING.md",
    "COPYRIGHT",
    "Dockerfile.airflow",
    "Dockerfile.spark",
    "GOVERNANCE.md",
    "LICENSE",
    "NOTICE",
    "PROVENANCE.md",
    "README.md",
    "SECURITY.md",
    "requirements-spark.txt",
    "requirements.txt",
}

ALLOWED_WORKFLOWS = {
    "ci.yml",
    "case-validation.yml",
    "provenance-compliance.yml",
}

TEXT_SUFFIXES = {
    "",
    ".cfg",
    ".conf",
    ".csv",
    ".dockerignore",
    ".env",
    ".gitignore",
    ".ini",
    ".json",
    ".md",
    ".properties",
    ".ps1",
    ".py",
    ".sh",
    ".sql",
    ".txt",
    ".yaml",
    ".yml",
}

SENSITIVE_FILE_SUFFIXES = {
    ".der",
    ".jks",
    ".key",
    ".p12",
    ".pem",
    ".pfx",
}

MAX_SCANNED_FILE_BYTES = 5 * 1024 * 1024

MARKDOWN_INLINE_LINK = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")
MARKDOWN_REFERENCE_LINK = re.compile(r"^\s*\[[^\]]+\]:\s*(\S+)", re.MULTILINE)

SECRET_PATTERNS = {
    "aws_access_key": re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"),
    "github_token": re.compile(r"\bgh[pousr]_[A-Za-z0-9]{36,255}\b"),
    "private_key": re.compile(
        r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"
    ),
}

LOCAL_PATH_PATTERNS = {
    "windows_user_path": re.compile(r"(?i)\b[A-Z]:[\\/]+Users[\\/]+"),
    "mac_user_path": re.compile("/" + r"Users/[^/\s]+/"),
    "linux_home_path": re.compile("/" + r"home/[^/\s]+/"),
}


def _private_identifiers() -> Tuple[str, ...]:
    # Split literals so this validator does not contain the forbidden values it
    # is responsible for detecting in an exported repository.
    return (
        "Data-Master-Platform" + "-SPDD",
        "Data-Master-" + "Mastery",
    )


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
    return [
        root / item
        for item in result.stdout.split("\0")
        if item
    ]


def _relative(root: Path, path: Path) -> str:
    return path.relative_to(root).as_posix()


def _read_text(path: Path) -> str | None:
    try:
        content = path.read_bytes()
    except OSError:
        return None
    if len(content) > MAX_SCANNED_FILE_BYTES or b"\x00" in content:
        return None
    try:
        return content.decode("utf-8")
    except UnicodeDecodeError:
        return None


def validate_boundary(root: Path, files: Sequence[Path]) -> List[str]:
    failures: List[str] = []
    private_identifiers = tuple(value.casefold() for value in _private_identifiers())

    for path in files:
        relative = _relative(root, path)
        parts = Path(relative).parts
        lower_relative = relative.casefold()

        if len(parts) == 1:
            if relative not in ALLOWED_ROOT_FILES:
                failures.append(f"allowlist.root_file:{relative}")
        elif parts[0] not in ALLOWED_TOP_LEVEL_DIRECTORIES:
            failures.append(f"allowlist.top_level:{relative}")

        if lower_relative.startswith("data/") and not lower_relative.startswith(
            "data/sample/"
        ):
            failures.append(f"allowlist.synthetic_data_only:{relative}")

        if lower_relative.startswith(".github/workflows/"):
            workflow_name = Path(relative).name
            if workflow_name not in ALLOWED_WORKFLOWS:
                failures.append(f"allowlist.workflow:{relative}")

        if (
            lower_relative.startswith("spdd/")
            or lower_relative.startswith("evidence/runtime/")
            or lower_relative.startswith(".codex/")
            or lower_relative.startswith(".chatgpt-projects/")
            or lower_relative.startswith("prompts/")
            or lower_relative.startswith("private/")
            or lower_relative.startswith("internal/")
            or lower_relative.endswith(".prompt.md")
            or lower_relative.endswith(".private.md")
            or lower_relative.endswith(".pdf")
        ):
            failures.append(f"denylist.path:{relative}")

        if path.suffix.casefold() in SENSITIVE_FILE_SUFFIXES:
            failures.append(f"denylist.sensitive_extension:{relative}")

        text = _read_text(path)
        if text is None:
            failures.append(f"content.unreadable_or_binary:{relative}")
            continue

        folded_text = text.casefold()
        for identifier in private_identifiers:
            if identifier in folded_text:
                failures.append(f"denylist.private_reference:{relative}")
                break

        for rule, pattern in LOCAL_PATH_PATTERNS.items():
            if pattern.search(text):
                failures.append(f"denylist.{rule}:{relative}")

        for rule, pattern in SECRET_PATTERNS.items():
            if pattern.search(text):
                failures.append(f"secret.{rule}:{relative}")

    return sorted(set(failures))


def _is_template_yaml(path: Path, text: str) -> bool:
    folded_parts = {part.casefold() for part in path.parts}
    return "infra" in folded_parts and "templates" in folded_parts


def _validate_workflow_document(document: object) -> bool:
    if not isinstance(document, Mapping):
        return False
    triggers = document.get("on")
    jobs = document.get("jobs")
    if triggers is None or not isinstance(jobs, Mapping) or not jobs:
        return False
    for job in jobs.values():
        if not isinstance(job, Mapping):
            return False
        if "runs-on" not in job or not isinstance(job.get("steps"), list):
            return False
    return True


def validate_formats(root: Path, files: Sequence[Path]) -> List[str]:
    failures: List[str] = []

    for path in files:
        relative = _relative(root, path)
        suffix = path.suffix.lower()
        try:
            if suffix == ".json":
                json.loads(path.read_text(encoding="utf-8"))
            elif suffix in {".yaml", ".yml"}:
                if yaml is None:
                    failures.append("format.yaml_dependency_missing:PyYAML")
                    continue
                text = path.read_text(encoding="utf-8")
                if not _is_template_yaml(path, text):
                    list(yaml.safe_load_all(text))
                    if relative.startswith(".github/workflows/"):
                        workflow_document = yaml.load(text, Loader=yaml.BaseLoader)
                        if not _validate_workflow_document(workflow_document):
                            failures.append(f"workflow.invalid:{relative}")
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            failures.append(f"format.invalid:{relative}")
        except Exception as exc:
            if yaml is not None and isinstance(exc, yaml.YAMLError):
                failures.append(f"format.invalid:{relative}")
            else:
                raise

    return sorted(set(failures))


def _markdown_targets(text: str) -> Iterable[str]:
    for match in MARKDOWN_INLINE_LINK.finditer(text):
        yield match.group(1).strip()
    for match in MARKDOWN_REFERENCE_LINK.finditer(text):
        yield match.group(1).strip()


def _normalize_markdown_target(raw_target: str) -> str:
    target = raw_target.strip()
    if target.startswith("<") and ">" in target:
        target = target[1 : target.index(">")]
    elif " " in target:
        target = target.split(" ", 1)[0]
    return unquote(target.strip())


def validate_markdown_links(root: Path, files: Sequence[Path]) -> List[str]:
    failures: List[str] = []

    for path in files:
        if path.suffix.lower() != ".md":
            continue
        relative = _relative(root, path)
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            failures.append(f"markdown.unreadable:{relative}")
            continue

        for raw_target in _markdown_targets(text):
            target = _normalize_markdown_target(raw_target)
            if (
                not target
                or target.startswith("#")
                or re.match(r"^[A-Za-z][A-Za-z0-9+.-]*:", target)
            ):
                continue

            target_without_fragment = target.split("#", 1)[0].split("?", 1)[0]
            if not target_without_fragment:
                continue
            candidate = (path.parent / target_without_fragment).resolve()
            try:
                candidate.relative_to(root.resolve())
            except ValueError:
                failures.append(f"markdown.outside_root:{relative}")
                continue
            if not candidate.exists():
                failures.append(f"markdown.missing_target:{relative}")

    return sorted(set(failures))


def run_checks(root: Path, checks: Sequence[str]) -> List[str]:
    files = tracked_files(root)
    failures: List[str] = []
    selected = set(checks)

    if "all" in selected or "boundary" in selected:
        failures.extend(validate_boundary(root, files))
    if "all" in selected or "formats" in selected:
        failures.extend(validate_formats(root, files))
    if "all" in selected or "links" in selected:
        failures.extend(validate_markdown_links(root, files))

    return sorted(set(failures))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate the public repository boundary, formats, and links."
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path.cwd(),
        help="Public repository root. Defaults to the current directory.",
    )
    parser.add_argument(
        "--check",
        action="append",
        choices=("all", "boundary", "formats", "links"),
        default=[],
        help="Check to execute. Repeat for multiple checks; defaults to all.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    root = args.root.resolve()
    checks = args.check or ["all"]

    try:
        failures = run_checks(root, checks)
    except RuntimeError as exc:
        print(f"PUBLIC_REPOSITORY_VALIDATION_ERROR={exc}", file=sys.stderr)
        return 2

    for failure in failures:
        print(f"PUBLIC_REPOSITORY_VALIDATION_FAILURE={failure}")

    status = "SUCCESS" if not failures else "FAILURE"
    print(f"PUBLIC_REPOSITORY_VALIDATION_STATUS={status}")
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
