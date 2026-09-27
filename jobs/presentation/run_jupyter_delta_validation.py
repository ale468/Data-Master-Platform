# Copyright (C) 2026 Alexandre Ferreira
# SPDX-License-Identifier: AGPL-3.0-only

"""Execute the Jupyter-to-Delta presentation contract inside the Jupyter pod."""

from __future__ import annotations

import json
import os

from jobs.presentation.jupyter_delta_path import (
    create_presentation_session,
    prepare_presentation_views,
    run_prepared_gold_query,
    validate_presentation_session,
)


def main() -> int:
    spark = create_presentation_session()
    try:
        session = validate_presentation_session(spark)
        layers = prepare_presentation_views(spark)
        gold_result_rows = int(run_prepared_gold_query(spark).count())
        if gold_result_rows < 1:
            raise RuntimeError("Prepared Gold query returned no rows.")
        payload = {
            "schema_version": 1,
            "evidence_kind": "data_master_jupyter_presentation_validation",
            "status": "PASS",
            "image_role": os.getenv("JUPYTER_IMAGE_ROLE", ""),
            "session": session,
            "layers": layers,
            "prepared_gold_query": {
                "view": "gold_transacoes_por_dia",
                "result_rows": gold_result_rows,
                "status": "PASS",
            },
            "snapshot_consistency": "PASS",
            "privacy": {
                "classification": "technical_aggregate_only",
                "contains_pii": False,
                "contains_secrets": False,
                "contains_business_payload": False,
            },
            "limitations": [
                "synthetic-local-only",
                "read-only-diagnostic-path",
                "not-production-serving",
                "not-a-pipeline-dependency",
            ],
        }
        print(
            "JUPYTER_PRESENTATION_RESULT="
            + json.dumps(payload, sort_keys=True, separators=(",", ":"))
        )
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
