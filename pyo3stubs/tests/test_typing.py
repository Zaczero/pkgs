"""The toolkit type-checks itself.

Its product is type-checking, and CI runs pytest per package and nothing else,
so `mypy --strict` only ever runs where the suite runs it. This is the same
shape as the typing gate gometry drives from its own suite.
"""

from __future__ import annotations

from pathlib import Path

PACKAGE = Path(__file__).resolve().parents[1]


def test_package_is_mypy_strict_clean():
    from mypy import api

    stdout, stderr, status = api.run([
        str(PACKAGE / 'pyo3stubs'),
        '--config-file',
        str(PACKAGE / 'pyproject.toml'),
        '--no-color-output',
        '--no-error-summary',
    ])
    assert status == 0, stdout + stderr
