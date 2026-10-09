"""Shared helpers for the ``tools/gates/_check_*.py`` gate scripts.

Gate scripts run through ``tools/check.py`` and remain independently executable
for focused diagnosis. Unit tests import their internals through
``conftest.load_tool``. Typical import pattern::

    _TOOLS_ROOT = Path(__file__).resolve().parents[1]
    if str(_TOOLS_ROOT) not in sys.path:
        sys.path.insert(0, str(_TOOLS_ROOT))

    from _gatelib import (
        prepend_rust_source_import_paths,
        report_errors,
        rust_source_config,
    )

    prepend_rust_source_import_paths()
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyo3stubs.config import StubConfig

TOOLS_DIR = Path(__file__).resolve().parent
TOOL_IMPORT_DIRS = (
    TOOLS_DIR,
    TOOLS_DIR / 'gates',
    TOOLS_DIR / 'stubs',
    TOOLS_DIR / 'docs',
)
ROOT = TOOLS_DIR.parent


def prepend_tools_import_paths() -> None:
    """Prepend checkout tool paths and an available workspace tool dependency."""
    for entry in TOOL_IMPORT_DIRS:
        path = str(entry)
        if path not in sys.path:
            sys.path.insert(0, path)
    sibling_package = ROOT.parent / 'pyo3stubs'
    if (sibling_package / 'pyo3stubs' / '__init__.py').is_file():
        path = str(sibling_package)
        if path in sys.path:
            sys.path.remove(path)
        last_tools_path = max(sys.path.index(str(entry)) for entry in TOOL_IMPORT_DIRS)
        # Editable installs can resolve another worktree; select this sibling before site-packages.
        sys.path.insert(last_tools_path + 1, path)


def prepend_rust_source_import_paths() -> None:
    """Resolve shared tools and the sibling grammar package from this checkout."""
    prepend_tools_import_paths()
    path = str(ROOT.parent / 'pyo3stubs')
    if path not in sys.path:
        sys.path.insert(0, path)


def rust_source_config(src_root: Path = ROOT / 'src') -> StubConfig:
    """Minimal source paths for grammar-backed checks that do not load gometry."""
    from pyo3stubs.config import StubConfig

    return StubConfig(
        module='gometry._lib',
        stub_path=ROOT / 'python' / 'gometry' / '_lib.pyi',
        src_root=src_root,
    )


def report_errors(errors: list[str], label: str) -> int:
    """The gate-script exit protocol: errors to stderr, TOTAL line, 1 on red."""
    for error in errors:
        print(f'  {error}', file=sys.stderr)
    print(f'\nTOTAL {label}: {len(errors)}')
    return 1 if errors else 0
