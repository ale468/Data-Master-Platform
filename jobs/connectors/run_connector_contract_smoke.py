"""CLI smoke runner for DM-CONN-001 connector contract evidence."""
import argparse
import json
import os
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "jobs" / "common"))

from connector_contract import run_connector_contract_smoke


def main() -> int:
    parser = argparse.ArgumentParser(description="Run connector contract smoke.")
    parser.add_argument("--batch-id", default=None, help="Optional batch/run id.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    args = parser.parse_args()

    result = run_connector_contract_smoke(batch_id=args.batch_id)
    print(
        "CONNECTOR_CONTRACT_SMOKE_RESULT="
        + json.dumps(
            result,
            ensure_ascii=False,
            indent=2 if args.pretty else None,
            sort_keys=True,
        )
    )
    return 0 if result["status"] == "SUCCESS" else 1


if __name__ == "__main__":
    os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")
    raise SystemExit(main())
