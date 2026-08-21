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
from tempfile import TemporaryDirectory
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
    with TemporaryDirectory() as cache_dir:
        command = [
            sys.executable,
            '-m',
            'mypy',
            *(str(path) for path in (cfg.mypy_targets or (cfg.stub_path,))),
            '--no-error-summary',
            '--no-color-output',
            '--soft-error-limit=-1',
            f'--cache-dir={cache_dir}',
            *cfg.mypy_args,
        ]
        if cfg.mypy_config is not None:
            command += ['--config-file', str(cfg.mypy_config)]
        result = subprocess.run(
            command,
            cwd=cache_dir,  # neutral cwd is part of the gate contract
            capture_output=True,
            text=True,
            check=False,
            env={**os.environ, 'MYPYPATH': str(_stub_root(cfg)), 'PYTHONPATH': ''},
        )
    if result.returncode == 0:
        return []
    output = (result.stdout + result.stderr).strip()
    return [line for line in output.splitlines() if line.strip()]


def collect_stubtest_errors(cfg: StubConfig) -> list[str]:
    """Run ``mypy.stubtest`` on the compiled module against its stub.

    The stub resolves via ``MYPYPATH`` pointed at the stub's package root, so
    the gate checks the working tree even under an editable install. Runtime
    imports use this process's explicit import path, which also supports test
    fixtures and config shims that add an uninstalled package to ``sys.path``.
    Allowlist entries that no longer match fail the run (no rot).
    """
    command = [sys.executable, '-m', 'mypy.stubtest', cfg.module]
    if cfg.stubtest_allowlist is not None:
        command += ['--allowlist', str(cfg.stubtest_allowlist)]
    if cfg.mypy_config is not None:
        command += ['--mypy-config-file', str(cfg.mypy_config)]
    with TemporaryDirectory() as cwd:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            env={
                **os.environ,
                'MYPYPATH': str(_stub_root(cfg)),
                'PYTHONPATH': os.pathsep.join(path for path in sys.path if path),
            },
            cwd=cwd,
        )
    if result.returncode == 0:
        return []
    output = (result.stdout + result.stderr).strip()
    return [line for line in output.splitlines() if line.strip()]
