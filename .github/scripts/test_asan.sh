#!/usr/bin/env bash
set -euo pipefail

build_args=(--locked --target x86_64-unknown-linux-gnu -Zbuild-std
  --config 'target.x86_64-unknown-linux-gnu.rustflags=["-Zsanitizer=address"]')
test_args=()
if [[ "$PACKAGE_TYPE" != rust ]]; then
  uv sync --project "$PACKAGE"
  PYO3_PYTHON="$(uv run --project "$PACKAGE" --no-sync python -c 'import sys; print(sys.executable)')"
  PYTHONPATH="$("$PYO3_PYTHON" -c 'import os, site; print(os.pathsep.join(site.getsitepackages()))')"
  PATH="$(dirname "$PYO3_PYTHON"):$PATH"
  export PYO3_PYTHON PYTHONPATH PATH
  test_args+=(--no-tests=pass)
fi
cargo nextest run -p "$CARGO_PACKAGE" "${build_args[@]}" "${test_args[@]}"
if [[ "$PACKAGE_TYPE" == rust ]]; then
  exit 0
fi

runtime="$(clang -print-file-name=libclang_rt.asan-x86_64.so)"
test -f "$runtime"

wheel_dir="$(mktemp -d "$RUNNER_TEMP/asan-wheels.XXXXXX")"
MATURIN_PEP517_ARGS="$("$PYO3_PYTHON" -c 'import shlex, sys; print(shlex.join(sys.argv[1:]))' --profile dev "${build_args[@]}")"
export MATURIN_PEP517_ARGS
uv build --wheel --out-dir "$wheel_dir" "$PACKAGE"
wheels=("$wheel_dir"/*.whl)
test "${#wheels[@]}" -eq 1
uv pip install --python "$PYO3_PYTHON" --reinstall --no-deps "${wheels[0]}"

LD_PRELOAD="$runtime${LD_PRELOAD:+:$LD_PRELOAD}" \
  "$PYO3_PYTHON" - "$PACKAGE/pyproject.toml" <<'PY'
import importlib
import subprocess
import sys
import tomllib

with open(sys.argv[1], "rb") as source:
    module = tomllib.load(source)["tool"]["maturin"]["module-name"]
extension = importlib.import_module(module).__file__
symbols = subprocess.check_output(["readelf", "-Ws", extension], text=True)
assert "__asan_init" in symbols and "__asan_report_" in symbols, (
    f"imported extension lacks ASan instrumentation: {extension}"
)
print(f"ASan extension: {extension}", flush=True)
PY

cd "$PACKAGE"
LD_PRELOAD="$runtime${LD_PRELOAD:+:$LD_PRELOAD}" \
  "$PYO3_PYTHON" -m pytest tests --ignore=tests/tooling --ignore=tests/test_public_docs.py
