"""Gometry-specific :class:`StubConfig` for pyo3stubs gates."""

from __future__ import annotations

import re
import sys
from pathlib import Path

import gometry as gm
from pyo3stubs.config import DualityConfig, StubConfig, TokenConfig
from pyo3stubs.leaked_types import DEFAULT_IGNORED_TYPE_NAMES

_TOOLS_ROOT = Path(__file__).resolve().parents[1]
if str(_TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TOOLS_ROOT))
from _gatelib import prepend_tools_import_paths

prepend_tools_import_paths()
_STUBS = Path(__file__).resolve().parent
ROOT = _TOOLS_ROOT.parent
STUB = ROOT / 'python' / 'gometry' / '_lib.pyi'
SRC = ROOT / 'src'

# Two capture groups: Rust ident then Python export name.
COVERAGE_ITERATOR = re.compile(
    r'coverage_iterator!\s*\(\s*(\w+)\s*,\s*"([^"]+)"',
    re.MULTILINE,
)
GEOMETRY_LEAF = re.compile(
    r'geometry_leaf!\s*\(\s*(\w+)\s*,\s*"([^"]+)"',
    re.MULTILINE,
)

LEAK_ALLOWLIST: dict[str, str] = {
    '_Float64Buffer': 'buffer-protocol internal (Arrow float64 mover)',
    '_Int32Buffer': 'buffer-protocol internal (Arrow int32 mover)',
}

# (empty since gm.index was folded into the SpatialIndex constructor — the
# 2026-07 form review resolved the index name collision at the source.)
KNOWN_DIVERGENCES: dict[str, str] = {}

GOMETRY_IGNORED_TYPE_NAMES = DEFAULT_IGNORED_TYPE_NAMES | frozenset({
    '_GeometryT',
    '_GeometryT_co',
})

SURFACES = (
    ('Geometry', gm.Geometry),
    ('GeometryArray', gm.GeometryArray),
    ('PreparedGeometry', gm.PreparedGeometry),
    ('gometry', gm),
)


def config() -> StubConfig:
    """Build the gometry gate configuration."""
    return StubConfig(
        module='gometry._lib',
        stub_path=STUB,
        src_root=SRC,
        surfaces=SURFACES,
        known_divergences=KNOWN_DIVERGENCES,
        leak_allowlist=LEAK_ALLOWLIST,
        stubtest_allowlist=_STUBS / 'stubtest_allowlist.txt',
        # Error-code policy (e.g. the overload-overlap rationale) lives in
        # [tool.mypy] there — one source for validity, stubtest, and bare mypy.
        mypy_config=ROOT / 'pyproject.toml',
        ignored_type_names=GOMETRY_IGNORED_TYPE_NAMES,
        pyclass_patterns=(COVERAGE_ITERATOR, GEOMETRY_LEAF),
        # Array method returns are DERIVED from the scalar contract.
        duality=DualityConfig(pairs=(('Geometry', 'GeometryArray'),)),
        tokens=TokenConfig(
            types_module='gometry._types',
            vocabulary_export='_token_vocabulary',
        ),
    )
