"""pyo3stubs configuration for h2corn."""

import tomllib
from pathlib import Path

from pyo3stubs import StubConfig

ROOT = Path(__file__).resolve().parent.parent


def config() -> StubConfig:
    return StubConfig(
        docstring_style=tomllib.loads((ROOT / 'pyproject.toml').read_text())['tool'][
            'ruff'
        ]['lint']['pydocstyle']['convention'],
        module='h2corn._lib',
        stub_path=ROOT / 'python' / 'h2corn' / '_lib.pyi',
        src_root=ROOT / 'src',
    )
