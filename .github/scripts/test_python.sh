#!/usr/bin/env bash
set -euo pipefail

cd "$PACKAGE"
uv run --no-sync python - "$IMPORT_NAME" "$PACKAGE_TYPE" <<'PY'
import importlib
import sys
import sysconfig

importlib.import_module(sys.argv[1])
if sysconfig.get_config_var("Py_GIL_DISABLED"):
    state = "enabled" if sys._is_gil_enabled() else "disabled"
    print(f"CPython free-threaded runtime GIL after import: {state}")
    if sys.argv[2] == "python-rust":
        assert not sys._is_gil_enabled()
PY
pytest_args=(tests --ignore=tests/tooling --ignore=tests/test_public_docs.py)
if [[ "${FREETHREADED:-false}" == true ]]; then
  pytest_args+=(-n 2)
fi
uv run --no-sync python -m pytest "${pytest_args[@]}"
