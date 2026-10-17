#!/usr/bin/env python3
"""Prepare gometry's identified runtime, then run its quality checks.

The default command applies Rust and Python formatting, machine-applicable Clippy
and Ruff fixes, prepares the DEV runtime, generates and formats stub docstrings,
then runs all checks. `--prepare` prepares the runtime, regenerates and formats
stub docstrings, and prints the runtime package root. `--no-fix` runs all checks
without rewriting the checkout. `--contracts-only` runs the read-only public
contract checks without applying fixes or preparing a stale runtime.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# `cargo` and `ruff` are system tools; every other step runs under this interpreter.
SYSTEM_TOOLS = frozenset({'cargo', 'ruff'})

FIXES: tuple[tuple[str, ...], ...] = (
    (
        'cargo',
        'clippy',
        '--fix',
        '--allow-dirty',
        '--allow-staged',
        '--all-targets',
        '--locked',
        '-p',
        'gometry',
    ),
    ('cargo', 'fmt', '--package', 'gometry'),
    ('ruff', 'check', '--fix', '--quiet', '.'),
    ('ruff', 'format', '--quiet', '.'),
)

GENERATE_STUBS = (
    '-m',
    'pyo3stubs',
    'gen-docs',
    '--config',
    'tools/stubs/stubconfig.py',
)


class CheckCategory(Enum):
    LOCAL = 'local'
    CONTRACT = 'contract'


@dataclass(frozen=True, slots=True)
class Check:
    command: tuple[str, ...]
    category: CheckCategory
    requires_runtime: bool = False
    prerequisites: tuple[str, ...] = ()

    @property
    def label(self) -> str:
        return ' '.join(self.command)


DOCS_BUILD = Check(
    ('-m', 'properdocs', 'build', '--strict'),
    CheckCategory.CONTRACT,
    requires_runtime=True,
)
DOCS_CHECK = Check(
    ('tools/docs/check.py',),
    CheckCategory.CONTRACT,
    requires_runtime=True,
    prerequisites=(DOCS_BUILD.label,),
)

CHECKS: tuple[Check, ...] = (
    Check(('ruff', 'check', '.'), CheckCategory.LOCAL),
    Check(('ruff', 'format', '--check', '.'), CheckCategory.LOCAL),
    Check(
        ('cargo', 'fmt', '--package', 'gometry', '--', '--check'),
        CheckCategory.LOCAL,
    ),
    Check(
        (
            '-m',
            'pyo3stubs',
            'check-all',
            '--config',
            'tools/stubs/stubconfig.py',
        ),
        CheckCategory.CONTRACT,
        requires_runtime=True,
    ),
    Check(
        ('tools/gates/_check_typing_runtime.py',),
        CheckCategory.CONTRACT,
        requires_runtime=True,
    ),
    Check(
        ('tools/gates/_check_typed_returns.py',),
        CheckCategory.LOCAL,
    ),
    Check(
        ('tools/stubs/_doc_coverage.py',),
        CheckCategory.CONTRACT,
        requires_runtime=True,
    ),
    Check(
        ('tools/gates/_check_doc_integrity.py',),
        CheckCategory.CONTRACT,
        requires_runtime=True,
    ),
    Check(
        ('tools/gates/_check_doc_model.py',),
        CheckCategory.CONTRACT,
        requires_runtime=True,
    ),
    Check(
        ('tools/gates/_check_docstring_examples.py',),
        CheckCategory.CONTRACT,
        requires_runtime=True,
    ),
    Check(
        ('tools/gates/_check_doctest_types.py',),
        CheckCategory.CONTRACT,
        requires_runtime=True,
    ),
    Check(
        ('tools/gates/_check_examples.py',),
        CheckCategory.CONTRACT,
        requires_runtime=True,
    ),
    Check(
        ('tools/docs/check_nav.py',),
        CheckCategory.CONTRACT,
    ),
    Check(
        ('tools/gates/_check_algebraic_float.py',),
        CheckCategory.LOCAL,
    ),
    Check(
        ('tools/gates/_check_packed_execution.py',),
        CheckCategory.LOCAL,
    ),
    # `-D warnings` because Clippy exits 0 on warnings.
    Check(
        (
            'cargo',
            'clippy',
            '--all-targets',
            '--locked',
            '-p',
            'gometry',
            '--',
            '-D',
            'warnings',
        ),
        CheckCategory.LOCAL,
    ),
    DOCS_BUILD,
    DOCS_CHECK,
)


def _runtime_command(arguments: tuple[str, ...]) -> list[str]:
    tools = ROOT / 'tools'
    code = f"""
import runpy
import sys
from pathlib import Path

sys.path.insert(0, {str(tools)!r})
from _gatelib import prepend_tools_import_paths

prepend_tools_import_paths()
from _gatelib import require_runtime

require_runtime(Path({str(ROOT)!r}))
arguments = sys.argv[1:]
if not arguments:
    raise SystemExit("missing runtime consumer command")
if arguments[0] == "-m":
    module = arguments[1]
    sys.argv = [module, *arguments[2:]]
    runpy.run_module(module, run_name="__main__", alter_sys=True)
else:
    sys.argv = arguments
    runpy.run_path(arguments[0], run_name="__main__")
"""
    return [sys.executable, '-c', code, *arguments]


def run(
    arguments: tuple[str, ...],
    *,
    environment: dict[str, str] | None = None,
    require_runtime: bool = False,
) -> bool:
    print(f'\n==> {" ".join(arguments)}', file=sys.stderr, flush=True)
    child_environment = os.environ.copy() if environment is None else environment.copy()
    if require_runtime and arguments == DOCS_BUILD.command:
        child_environment.pop('MARKDOWN_EXEC_AUTO', None)
    if require_runtime:
        interpreter_bin = str(Path(sys.executable).parent)
        child_environment['PATH'] = os.pathsep.join((
            interpreter_bin,
            child_environment.get('PATH', ''),
        ))
    if arguments[0] == 'cargo':
        child_environment['PYO3_PYTHON'] = sys.executable
    command = (
        _runtime_command(arguments)
        if require_runtime
        else [sys.executable, *arguments]
        if arguments[0] not in SYSTEM_TOOLS
        else list(arguments)
    )
    try:
        result = subprocess.run(command, cwd=ROOT, check=False, env=child_environment)
    except FileNotFoundError as error:
        print(f'could not execute {command[0]}: {error}', file=sys.stderr)
        return False
    return result.returncode == 0


def _quality_runtime_module():
    tools = ROOT / 'tools'
    if str(tools) not in sys.path:
        sys.path.insert(0, str(tools))
    import quality_runtime

    return quality_runtime


def _prepare_runtime() -> tuple[dict[str, object], dict[str, str]] | None:
    quality_runtime = _quality_runtime_module()
    try:
        quality_runtime.prepare_quality_runtime(
            ROOT,
            scratch=ROOT / 'target' / 'quality-runtime',
        )
        receipt = quality_runtime.read_quality_receipt(ROOT)
        environment = _child_environment(receipt)
    except Exception as error:
        print(f'quality runtime preparation failed: {error}', file=sys.stderr)
        return None
    if not run(
        GENERATE_STUBS,
        environment=environment,
        require_runtime=True,
    ):
        _report_failure(' '.join(GENERATE_STUBS))
        return None
    if not run(
        ('ruff', 'format', '--quiet', 'python/gometry/_lib.pyi'),
        environment=environment,
    ):
        _report_failure('format generated _lib.pyi')
        return None
    try:
        quality_runtime.refresh_quality_runtime(ROOT)
        receipt = quality_runtime.read_quality_receipt(ROOT)
        environment = _child_environment(receipt)
    except Exception as error:
        print(f'final quality runtime staging failed: {error}', file=sys.stderr)
        return None
    return receipt, environment


def _child_environment(receipt: dict[str, object]) -> dict[str, str]:
    package_root = receipt.get('package_root')
    if not isinstance(package_root, str):
        raise TypeError('quality runtime receipt has no prepared package root')
    environment = os.environ.copy()
    current = environment.get('PYTHONPATH')
    environment['PYTHONPATH'] = (
        package_root if not current else package_root + os.pathsep + current
    )
    environment['PYO3_PYTHON'] = sys.executable
    return environment


def _report_failure(label: str) -> int:
    print(f'quality prerequisite failed: {label}', file=sys.stderr)
    print('dependent quality checks were not run', file=sys.stderr)
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__.splitlines()[0],
        allow_abbrev=False,
        **({'color': False} if sys.version_info >= (3, 14) else {}),
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        '--no-fix', action='store_true', help='run checks without rewriting files'
    )
    mode.add_argument(
        '--prepare',
        action='store_true',
        help=(
            'prepare the runtime, regenerate and format stub docstrings, then '
            'print its package root without running checks'
        ),
    )
    parser.add_argument(
        '--contracts-only',
        action='store_true',
        help='run read-only public contract checks without preparing a stale runtime',
    )
    options = parser.parse_args()
    if options.prepare and options.contracts_only:
        parser.error('--prepare cannot be combined with --contracts-only')
    no_fix = options.no_fix or options.contracts_only
    prepare_only = options.prepare
    environment = os.environ.copy()
    runtime_failure: str | None = None
    if no_fix:
        try:
            tools = ROOT / 'tools'
            if str(tools) not in sys.path:
                sys.path.insert(0, str(tools))
            from _gatelib import require_runtime
        except Exception as error:
            runtime_failure = f'could not load runtime admission: {error}'
        else:
            try:
                require_runtime(ROOT)
            except (Exception, SystemExit) as error:
                runtime_failure = str(error)
    else:
        if not prepare_only:
            for step in FIXES:
                if not run(step):
                    return _report_failure(' '.join(step))
        prepared = _prepare_runtime()
        if prepared is None:
            return 1
        receipt, environment = prepared
        if prepare_only:
            print(receipt['package_root'])
            return 0

    failed: list[str] = []
    skipped: list[str] = []
    if runtime_failure is not None:
        print(
            f'quality runtime prerequisite failed: {runtime_failure}',
            file=sys.stderr,
        )
    selected_checks = (
        check
        for check in CHECKS
        if not options.contracts_only or check.category is CheckCategory.CONTRACT
    )
    failed_labels: set[str] = set()
    for check in selected_checks:
        if any(prerequisite in failed_labels for prerequisite in check.prerequisites):
            skipped.append(check.label)
            continue
        if check.requires_runtime and runtime_failure is not None:
            skipped.append(check.label)
            continue
        if not run(
            check.command,
            environment=environment,
            require_runtime=check.requires_runtime,
        ):
            failed.append(check.label)
            failed_labels.add(check.label)
    if failed:
        print('\nFailed quality checks:', file=sys.stderr)
        for label in failed:
            print(f'  {label}', file=sys.stderr)
    if skipped:
        print('\nSkipped dependent quality checks:', file=sys.stderr)
        for label in skipped:
            print(f'  {label}', file=sys.stderr)
    failure_count = len(failed) + (runtime_failure is not None)
    print(f'\nTOTAL QUALITY FAILURES: {failure_count}', file=sys.stderr)
    print(f'TOTAL QUALITY SKIPPED: {len(skipped)}', file=sys.stderr)
    return 1 if failure_count else 0


if __name__ == '__main__':
    raise SystemExit(main())
