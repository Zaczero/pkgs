"""Shared source and runtime checks for gometry's local quality tools."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from types import ModuleType

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
    tool_paths = [str(entry) for entry in TOOL_IMPORT_DIRS]
    tool_path_set = set(tool_paths)
    sys.path[:] = [entry for entry in sys.path if entry not in tool_path_set]
    sys.path[:0] = tool_paths
    sibling_package = ROOT.parent / 'pyo3stubs'
    if (sibling_package / 'pyo3stubs' / '__init__.py').is_file():
        path = str(sibling_package)
        sys.path[:] = [entry for entry in sys.path if entry != path]
        last_tools_path = max(sys.path.index(str(entry)) for entry in TOOL_IMPORT_DIRS)
        # Editable installs can resolve another worktree; select this sibling before site-packages.
        sys.path.insert(last_tools_path + 1, path)


def rust_source_config(src_root: Path = ROOT / 'src') -> StubConfig:
    """Minimal source paths for grammar-backed checks that do not load gometry."""
    from pyo3stubs.config import StubConfig

    return StubConfig(
        module='gometry._lib',
        stub_path=ROOT / 'python' / 'gometry' / '_lib.pyi',
        src_root=src_root,
    )


def require_runtime(
    root: Path = ROOT,
) -> tuple[ModuleType, ModuleType, dict[str, Any]]:
    """Reject an imported extension built from different package sources."""
    from _artifact_common import SOURCE_INPUTS_FORMAT, _package_version
    from artifact_content import source_input_digests

    module = importlib.import_module('gometry')
    library = importlib.import_module('gometry._lib')
    build = dict(library.__build_info__)
    expected = source_input_digests(root)
    if (
        library.__version__ != _package_version(root)
        or build.get('source_inputs_format') != SOURCE_INPUTS_FORMAT
        or build.get('native_source_inputs_format') != SOURCE_INPUTS_FORMAT
        or any(build.get(name) != digest for name, digest in expected.items())
    ):
        raise SystemExit(
            f'gometry extension {library.__file__} was built from different sources; '
            'rebuild it for the selected Python interpreter'
        )
    return module, library, build


def report_errors(errors: list[str], label: str) -> int:
    """The gate-script exit protocol: errors to stderr, TOTAL line, 1 on red."""
    for error in errors:
        print(f'  {error}', file=sys.stderr)
    print(f'\nTOTAL {label}: {len(errors)}')
    return 1 if errors else 0
