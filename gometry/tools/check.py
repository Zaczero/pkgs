#!/usr/bin/env python3
"""Apply gometry's formatters and mechanical fixes, then run its quality checks.

The fix phase rewrites the checkout: Rust and Python formatting, machine-applicable
Clippy and Ruff fixes, and stub docstrings regenerated from the runtime. The checks
then report only what needs judgment. `--no-fix` runs the checks alone, for a
checkout that should not be rewritten, such as CI.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
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
    ('cargo', 'fmt'),
    ('ruff', 'check', '--fix', '--quiet', '.'),
    ('ruff', 'format', '--quiet', '.'),
    ('-m', 'pyo3stubs', 'gen-docs', '--config', 'tools/stubs/stubconfig.py'),
)

CHECKS: tuple[tuple[str, ...], ...] = (
    (
        '-m',
        'pyo3stubs',
        'check-all',
        '--config',
        'tools/stubs/stubconfig.py',
    ),
    ('tools/gates/_check_typing_runtime.py',),
    ('tools/gates/_check_typed_returns.py',),
    ('tools/gates/_check_algebraic_float.py',),
    ('tools/gates/_check_packed_execution.py',),
    ('tools/stubs/_doc_coverage.py',),
    ('tools/gates/_check_doc_integrity.py',),
    ('tools/gates/_check_doc_model.py',),
    ('tools/gates/_check_docstring_examples.py',),
    ('tools/gates/_check_doctest_types.py',),
    ('tools/gates/_check_examples.py',),
    ('tools/docs/check_nav.py',),
    # `-D warnings` because clippy exits 0 on warnings, and a lint gate that
    # cannot fail lets findings accumulate silently.
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
)


def run(arguments: tuple[str, ...]) -> bool:
    print(f'\n==> {" ".join(arguments)}', flush=True)
    command = list(arguments)
    environment = None
    if arguments[0] == 'cargo':
        environment = os.environ.copy()
        environment.setdefault('PYO3_PYTHON', sys.executable)
    if arguments[0] not in SYSTEM_TOOLS:
        command.insert(0, sys.executable)
    return (
        subprocess.run(command, cwd=ROOT, check=False, env=environment).returncode == 0
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        '--no-fix', action='store_true', help='run the checks without rewriting files'
    )
    steps = CHECKS if parser.parse_args().no_fix else (*FIXES, *CHECKS)
    failed = [' '.join(step) for step in steps if not run(step)]
    if failed:
        print('\nFailed quality checks:', file=sys.stderr)
        for label in failed:
            print(f'  {label}', file=sys.stderr)
    print(f'\nTOTAL QUALITY FAILURES: {len(failed)}')
    return 1 if failed else 0


if __name__ == '__main__':
    raise SystemExit(main())
