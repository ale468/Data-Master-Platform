#!/bin/sh
# Copyright (C) 2026 Alexandre Ferreira
# SPDX-License-Identifier: AGPL-3.0-only

set -eu

: "${JUPYTER_TOKEN:?JUPYTER_TOKEN is required}"

python3 - <<'PY'
import os
from pathlib import Path

config_path = Path(os.environ["JUPYTER_CONFIG_DIR"]) / "jupyter_server_config.py"
config_path.parent.mkdir(parents=True, exist_ok=True)
config_path.write_text(
    "c = get_config()\n"
    + "c.IdentityProvider.token = "
    + repr(os.environ["JUPYTER_TOKEN"])
    + "\n",
    encoding="utf-8",
)
config_path.chmod(0o600)
PY

unset JUPYTER_TOKEN

exec python3 -m jupyterlab \
  --ServerApp.ip=0.0.0.0 \
  --ServerApp.port="${JUPYTER_PORT}" \
  --ServerApp.open_browser=False \
  --ServerApp.allow_remote_access=True \
  --ServerApp.root_dir="${JUPYTER_ROOT_DIR}"
