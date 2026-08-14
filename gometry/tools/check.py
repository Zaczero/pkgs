#!/usr/bin/env python3
"""Run gometry's environment-invariant source and documentation checks."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

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
    ('tools/gates/_check_docstyle.py',),
    ('tools/gates/_check_docstring_examples.py',),
    ('tools/gates/_check_doctest_types.py',),
    ('tools/gates/_check_examples.py',),
    ('tools/docs/check_nav.py',),
)


def main() -> int:
    failed: list[str] = []
    for arguments in CHECKS:
        label = ' '.join(arguments)
        print(f'\n==> {label}', flush=True)
        result = subprocess.run(
            [sys.executable, *arguments],
            cwd=ROOT,
            check=False,
        )
        if result.returncode:
            failed.append(label)

    if failed:
        print('\nFailed quality checks:', file=sys.stderr)
        for label in failed:
            print(f'  {label}', file=sys.stderr)
    print(f'\nTOTAL QUALITY FAILURES: {len(failed)}')
    return 1 if failed else 0


if __name__ == '__main__':
    raise SystemExit(main())
