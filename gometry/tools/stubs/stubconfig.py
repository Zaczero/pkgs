"""Gometry-specific :class:`StubConfig` for pyo3stubs gates."""

from __future__ import annotations

import sys
from pathlib import Path

import gometry as gm
from pyo3stubs import (
    DualityConfig,
    MacroExport,
    StubConfig,
    SurfaceConfig,
    TokenConfig,
)

_TOOLS_ROOT = Path(__file__).resolve().parents[1]
if str(_TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TOOLS_ROOT))
from _gatelib import prepend_tools_import_paths

prepend_tools_import_paths()
_STUBS = Path(__file__).resolve().parent
ROOT = _TOOLS_ROOT.parent
STUB = ROOT / 'python' / 'gometry' / '_lib.pyi'
SRC = ROOT / 'src'

# `geometry_leaf!(PyPoint, "Point", "docstring")` declares a `#[pyclass]` from a
# macro, so the attribute scan cannot see it: the Rust identifier and the Python
# export name are the invocation's first two arguments.
GEOMETRY_LEAF = MacroExport(macro='geometry_leaf', rust=0, python=1)

# (empty since gm.index was folded into the SpatialIndex constructor — the
# 2026-07 form review resolved the index name collision at the source.)
KNOWN_DIVERGENCES: dict[str, str] = {}

EXTRA_IGNORED_TYPE_NAMES = frozenset({'_GeometryT', '_GeometryT_co'})

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
        surface=SurfaceConfig(
            targets=SURFACES,
            known_divergences=KNOWN_DIVERGENCES,
        ),
        stubtest_allowlist=_STUBS / 'stubtest_allowlist.txt',
        # Error-code policy (e.g. the overload-overlap rationale) lives in
        # [tool.mypy] there — one source for validity, stubtest, and bare mypy.
        mypy_config=ROOT / 'pyproject.toml',
        extra_ignored_type_names=EXTRA_IGNORED_TYPE_NAMES,
        macro_exports=(GEOMETRY_LEAF,),
        # Array method returns are DERIVED from the scalar contract.
        duality=DualityConfig(pairs=(('Geometry', 'GeometryArray'),)),
        tokens=TokenConfig(
            types_module='gometry._types',
            vocabulary_export='_token_vocabulary',
            extra_vocabulary=frozenset({'ParseFormat'}),
        ),
    )
