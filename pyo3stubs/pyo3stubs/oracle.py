"""mypy-backed gates: stub validity and the stub/runtime stubtest oracle.

Two independent battle-tested detectors replace the former homegrown
``parity``/``nullability`` modules:

* **validity** — ``mypy`` type-checks the ``.pyi`` itself. Catches everything a
  non-pyright checker would choke on: bare implementation defs in a stub
  (PEP 484), undecorated duplicate defs (the silent dead-overload class —
  pyright applies last-def-wins without complaint), inconsistent overloads,
  unresolvable annotations.
* **stubtest** — ``mypy.stubtest`` imports the compiled module and compares it
  against the stub: missing/extra members, signature names/kinds/defaults
  (by value, not repr), classmethod/staticmethod/property mismatches, variables
  whose runtime value does not satisfy the declared type (which subsumes the
  old attribute-nullability gate: ``attr: str`` that is ``None`` at runtime
  fails), ``__all__`` parity, and overload/runtime compatibility.

Both run the interpreter's own installed mypy, so results match what a user's
``mypy`` sees.
"""

from __future__ import annotations

import os
import subprocess
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from pyo3stubs.config import StubConfig


def _stub_root(cfg: StubConfig) -> Path:
    """Directory that makes ``cfg.module`` importable as stubs.

    ``python/shaper/_lib.pyi`` with module ``shaper._lib`` → ``python``.
    """
    return cfg.stub_path.parents[len(cfg.module.split('.')) - 1]


def collect_validity_errors(cfg: StubConfig) -> list[str]:
    """Type-check the stub file with mypy; any diagnostic is an error."""
    from mypy import api

    command = [
        str(cfg.stub_path),
        '--no-error-summary',
        '--no-color-output',
        '--soft-error-limit=-1',
        *cfg.mypy_args,
    ]
    if cfg.mypy_config is not None:
        command += ['--config-file', str(cfg.mypy_config)]
    stdout, stderr, status = api.run(command)
    if status == 0:
        return []
    output = (stdout + stderr).strip()
    return [line for line in output.splitlines() if line.strip()]


def collect_stubtest_errors(cfg: StubConfig) -> list[str]:
    """Run ``mypy.stubtest`` on the compiled module against its stub.

    The stub resolves via ``MYPYPATH`` pointed at the stub's package root, so
    the gate checks the working tree even under an editable install. Allowlist
    entries that no longer match fail the run (no rot).
    """
    command = [sys.executable, '-m', 'mypy.stubtest', cfg.module]
    if cfg.stubtest_allowlist is not None:
        command += ['--allowlist', str(cfg.stubtest_allowlist)]
    if cfg.mypy_config is not None:
        command += ['--mypy-config-file', str(cfg.mypy_config)]
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, 'MYPYPATH': str(_stub_root(cfg))},
    )
    if result.returncode == 0:
        return []
    output = (result.stdout + result.stderr).strip()
    return [line for line in output.splitlines() if line.strip()]
