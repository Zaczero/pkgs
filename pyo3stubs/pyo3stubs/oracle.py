"""mypy-backed gates: stub validity and the stub/runtime stubtest oracle.

Two independent battle-tested detectors replace the former homegrown
``parity``/``nullability`` modules:

* **validity** — ``mypy`` type-checks the ``.pyi`` itself. Catches everything a
  non-pyright checker would choke on: bare implementation defs in a stub
  (PEP 484), undecorated duplicate defs (the silent dead-overload class —
  pyright applies last-def-wins without complaint), inconsistent overloads,
  unresolvable annotations.
* **pyright** — the other checker most consumers type against. It reports what
  mypy does not (an ``__all__`` entry with no definition, for one), under the
  project's own ``[tool.pyright]`` settings. It cannot replace ``validity``: a
  stub that turns off ``reportOverlappingOverload`` for deliberate
  scalar-or-batch overlap also turns off pyright's dead-overload detection,
  which mypy files under a separate code.
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

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

from pyo3stubs.report import Findings

if TYPE_CHECKING:
    from pyo3stubs.config import StubConfig


def _stub_root(cfg: StubConfig) -> Path:
    """Directory that makes ``cfg.module`` importable as stubs.

    ``python/shaper/_lib.pyi`` with module ``shaper._lib`` → ``python``.
    """
    return cfg.stub_path.parents[len(cfg.module.split('.')) - 1]


def _mypy_cache_dir(cfg: StubConfig) -> Path:
    """A persistent mypy cache per stub, outside the project tree.

    mypy keys each cache entry on the source's hash, its own version and the
    options, so reuse is exact; a warm run re-analyses only what changed
    (about half a second against eighteen cold for gometry's stub). stubtest
    cannot share it: it needs full semantic trees, which it builds with
    incremental mode off.
    """
    root = Path(os.environ.get('XDG_CACHE_HOME') or Path.home() / '.cache')
    key = hashlib.sha256(str(cfg.stub_path.resolve()).encode()).hexdigest()[:16]
    return root / 'pyo3stubs' / key / 'mypy'


def collect_validity_errors(cfg: StubConfig) -> list[str]:
    """Type-check the stub file with mypy; any diagnostic is an error."""
    cache_dir = _mypy_cache_dir(cfg)
    cache_dir.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory() as cwd:
        command = [
            sys.executable,
            '-m',
            'mypy',
            *(str(path) for path in (cfg.typecheck_targets or (cfg.stub_path,))),
            '--no-error-summary',
            '--no-color-output',
            '--soft-error-limit=-1',
            f'--cache-dir={cache_dir}',
            # A shadowed overload is dead code, never a deliberate overlap, so
            # a project disabling mypy's overlap codes does not silence it.
            # pyright files both under one rule, which stubs routinely turn
            # off for scalar-or-batch overlap, so this is the only detector.
            '--enable-error-code=overload-cannot-match',
            *cfg.mypy_args,
        ]
        if cfg.mypy_config is not None:
            command += ['--config-file', str(cfg.mypy_config)]
        result = subprocess.run(
            command,
            cwd=cwd,  # neutral cwd is part of the gate contract
            capture_output=True,
            text=True,
            check=False,
            env={**os.environ, 'MYPYPATH': str(_stub_root(cfg)), 'PYTHONPATH': ''},
        )
    if result.returncode == 0:
        return []
    output = (result.stdout + result.stderr).strip()
    return [line for line in output.splitlines() if line.strip()] or [
        f'mypy exited with status {result.returncode} without diagnostics'
    ]


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
    if not output:
        return [f'stubtest exited with status {result.returncode} without diagnostics']
    diagnostics: list[str] = []
    current: list[str] = []
    for line in output.splitlines():
        if line.startswith('error: ') and current:
            diagnostics.append('\n'.join(current).strip())
            current = []
        current.append(line)
    if current:
        diagnostics.append('\n'.join(current).strip())
    return diagnostics


def _pyright_project(cfg: StubConfig) -> Path:
    """The directory whose pyright settings apply to the stub.

    The nearest ancestor holding a ``pyrightconfig.json`` or a ``pyproject.toml``
    with a ``[tool.pyright]`` table, so the gate reads the same settings the
    developer's editor does, wherever it was invoked from.
    """
    for directory in cfg.stub_path.resolve().parents:
        if (directory / 'pyrightconfig.json').is_file():
            return directory
        project = directory / 'pyproject.toml'
        if project.is_file() and '[tool.pyright]' in project.read_text(
            encoding='utf-8'
        ):
            return directory
    return cfg.stub_path.resolve().parent


def collect_pyright_errors(cfg: StubConfig) -> Findings:
    """Check the stub with pyright; every error and warning is a violation.

    Information-level output is advice, not a defect, so it is dropped. The
    interpreter running the gates supplies the environment, which keeps the
    result independent of whatever ``venvPath`` the project config names.
    """
    targets = cfg.typecheck_targets or (cfg.stub_path,)
    project = _pyright_project(cfg)
    result = subprocess.run(
        [
            sys.executable,
            '-m',
            'pyright',
            '--outputjson',
            '--pythonpath',
            sys.executable,
            '--project',
            str(project),
            *(str(path) for path in targets),
        ],
        capture_output=True,
        text=True,
        check=False,
        cwd=project,
        env={**os.environ, 'PYTHONPATH': ''},
    )
    try:
        report = json.loads(result.stdout)
    except json.JSONDecodeError:
        output = (result.stderr or result.stdout).strip()
        message = (
            f'pyright could not run: {output}'
            if output
            else 'pyright produced no report'
        )
        raise RuntimeError(message) from None
    errors = [
        '{file}:{line}: {severity}: {message}{rule}'.format(
            file=item['file'],
            line=item['range']['start']['line'] + 1,
            severity=item['severity'],
            message=item['message'].replace('\n', ' '),
            rule=f' ({item["rule"]})' if item.get('rule') else '',
        )
        for item in report.get('generalDiagnostics', ())
        if item['severity'] in ('error', 'warning')
    ]
    if result.returncode != 0 and not errors:
        detail = (
            result.stderr.strip() or 'no error or warning diagnostics in its report'
        )
        errors.append(f'pyright exited with status {result.returncode}: {detail}')
    return Findings(errors, examined=report.get('summary', {}).get('filesAnalyzed'))
