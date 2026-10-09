"""Gometry-specific :class:`StubConfig` for pyo3stubs gates."""

from __future__ import annotations

import sys
from pathlib import Path

_TOOLS_ROOT = Path(__file__).resolve().parents[1]
if str(_TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TOOLS_ROOT))
from _gatelib import prepend_tools_import_paths
from runtime_artifact import require_runtime

prepend_tools_import_paths()
_STUBS = Path(__file__).resolve().parent
ROOT = _TOOLS_ROOT.parent
require_runtime(ROOT)

import gometry as gm
from pyo3stubs import (
    DualityConfig,
    MacroExport,
    StubConfig,
    SurfaceConfig,
    TextSignatureDefault,
    TokenConfig,
)

STUB = ROOT / 'python' / 'gometry' / '_lib.pyi'
SRC = ROOT / 'src'

# `geometry_leaf!(PyPoint, "Point", "docstring")` declares a `#[pyclass]` from a
# macro, so the attribute scan cannot see it: the Rust identifier and the Python
# export name are the invocation's first two arguments.
GEOMETRY_LEAF = MacroExport(macro='geometry_leaf', rust=0, python=1)

# (empty since gm.index was folded into the SpatialIndex constructor — the
# 2026-07 form review resolved the index name collision at the source.)
KNOWN_DIVERGENCES: dict[str, str] = {}

UNINSPECTABLE_ALLOWLIST: dict[str, str] = {
    'AccuracyWarning': (
        'UserWarning subclass created without a custom constructor; '
        'it keeps the standard exception args semantics.'
    ),
    'CRSError': (
        'GeometryError subclass created without a custom constructor; '
        'it inherits the standard ValueError-derived exception args.'
    ),
    'CRSMismatchError': (
        'CRSError subclass created without a custom constructor; '
        'it inherits the standard ValueError-derived exception args.'
    ),
    'GeometryError': (
        'ValueError subclass created without a custom constructor; '
        'it uses the standard exception args semantics.'
    ),
    'GeometryTypeError': (
        'Dynamic GeometryError/TypeError subclass with only doc and module '
        'attributes; it inherits the standard exception args constructor.'
    ),
    'InvalidGeometryError': (
        'GeometryError subclass created without a custom constructor; '
        'it inherits the standard ValueError-derived exception args.'
    ),
    'ParseError': (
        'GeometryError subclass created without a custom constructor; '
        'it inherits the standard ValueError-derived exception args.'
    ),
    'TransformError': (
        'CRSError subclass created without a custom constructor; '
        'it inherits the standard ValueError-derived exception args.'
    ),
}

# Exact Rust defaults whose Python text-signature presentation is deliberately
# a public token, effective scalar, coordinate tuple, or unwrapped Option value.
# These entries name values; the checker does not evaluate Rust expressions.
TEXT_SIGNATURE_DEFAULT_ALIASES = {
    'ArrowEncoding::Auto': TextSignatureDefault(
        value='auto',
        reason='The ArrowEncoding variant is exposed as its public token.',
    ),
    'BufferCapStyle::Round': TextSignatureDefault(
        value='round',
        reason='The BufferCapStyle variant is exposed as its public token.',
    ),
    'BufferJoinStyle::Round': TextSignatureDefault(
        value='round',
        reason='The BufferJoinStyle variant is exposed as its public token.',
    ),
    'BufferSide::Both': TextSignatureDefault(
        value='both',
        reason='The BufferSide variant is exposed as its public token.',
    ),
    'CellRule::Overlap': TextSignatureDefault(
        value='overlap',
        reason='The CellRule variant is exposed as its public token.',
    ),
    'DEFAULT_MITER_LIMIT': TextSignatureDefault(
        value=5.0,
        reason='The named miter-limit constant is exposed as the numeric default.',
    ),
    'DefaultedF64Input::Default(0.0)': TextSignatureDefault(
        value=0.0,
        reason='The wrapper Default payload is the exposed effective scalar value.',
    ),
    'DefaultedF64Input::Default(2.0)': TextSignatureDefault(
        value=2.0,
        reason='The wrapper Default payload is the exposed effective scalar value.',
    ),
    'DefaultedI64Input::Default(2)': TextSignatureDefault(
        value=2,
        reason='The wrapper Default payload is the exposed effective scalar value.',
    ),
    'GridOrigin::DEFAULT': TextSignatureDefault(
        value=(0.0, 0.0),
        reason='GridOrigin::DEFAULT is the exposed coordinate pair.',
    ),
    'LineReferenceBasis::Distance': TextSignatureDefault(
        value='distance',
        reason='The LineReferenceBasis variant is exposed as its public token.',
    ),
    'MinConfidence::DEFAULT': TextSignatureDefault(
        value=70,
        reason='MinConfidence::DEFAULT is the public confidence threshold.',
    ),
    'NavigationPath::Geodesic': TextSignatureDefault(
        value='geodesic',
        reason='The NavigationPath variant is exposed as its public token.',
    ),
    'OriginSpec::Centroid': TextSignatureDefault(
        value='centroid',
        reason='The centroid anchor is exposed as the public origin token.',
    ),
    'QUADRANT_SEGMENTS_DEFAULT_I64': TextSignatureDefault(
        value=8,
        reason='The named quadrant-segment constant is exposed as an integer.',
    ),
    'RepairMethod::Linework': TextSignatureDefault(
        value='linework',
        reason='The RepairMethod variant is exposed as its public token.',
    ),
    'SimplifyMethod::Vw': TextSignatureDefault(
        value='vw',
        reason='The SimplifyMethod variant is exposed as its public token.',
    ),
    'SmoothMethod::Chaikin': TextSignatureDefault(
        value='chaikin',
        reason='The SmoothMethod variant is exposed as its public token.',
    ),
    'Some(1_000_000)': TextSignatureDefault(
        value=1_000_000,
        reason='The Python signature exposes the inner value of the Rust Some default.',
    ),
    'Some(crate::grid::GRID_MAX_CELLS)': TextSignatureDefault(
        value=1_000_000,
        reason='The Python signature exposes the inner maximum-cell count.',
    ),
    'SpatialCurve::Hilbert': TextSignatureDefault(
        value='hilbert',
        reason='The SpatialCurve variant is exposed as its public token.',
    ),
    'VoronoiClipInput::DEFAULT': TextSignatureDefault(
        value='padded',
        reason='The default clip wrapper is presented as the public padded mode.',
    ),
    'Wgs84DefaultCrs::Default': TextSignatureDefault(
        value='OGC:CRS84',
        reason='The omitted CRS resolves to the public WGS84 longitude/latitude label.',
    ),
    'crate::grid::GRID_MAX_CELLS': TextSignatureDefault(
        value=1_000_000,
        reason='The named maximum-cell constant is exposed as its integer value.',
    ),
    'crs::ProjDirection::Forward': TextSignatureDefault(
        value='forward',
        reason='The ProjDirection variant is exposed as its public token.',
    ),
    'crs::WktAxisRule::Auto': TextSignatureDefault(
        value='auto',
        reason='The WktAxisRule variant is exposed as its public token.',
    ),
    'crs::WktVersion::Wkt2_2019': TextSignatureDefault(
        value='WKT2_2019',
        reason='The WktVersion variant is exposed as its public token.',
    ),
    'geometry::CoverageOverlapRule::LongestBorder': TextSignatureDefault(
        value='longest_border',
        reason='The overlap-rule variant is exposed as its public token.',
    ),
}

EXTRA_IGNORED_TYPE_NAMES = frozenset({'_GeometryT', '_GeometryT_co', '_CellT'})

SURFACES = (
    ('Geometry', gm.Geometry),
    ('GeometryArray', gm.GeometryArray),
    ('gometry', gm),
)


def config() -> StubConfig:
    """Build the gometry gate configuration."""
    return StubConfig(
        module='gometry._lib',
        stub_path=STUB,
        src_root=SRC,
        leak_allowlist={
            'NativeBoundaryIterator': 'cfg(test)-only allocation-boundary fixture',
        },
        uninspectable_allowlist=UNINSPECTABLE_ALLOWLIST,
        surface=SurfaceConfig(
            targets=SURFACES,
            known_divergences=KNOWN_DIVERGENCES,
        ),
        stubtest_allowlist=_STUBS / 'stubtest_allowlist.txt',
        # Error-code policy (e.g. the overload-overlap rationale) lives in
        # [tool.mypy] there — one source for validity, stubtest, and bare mypy.
        mypy_config=ROOT / 'pyproject.toml',
        # Pin cwd and paths so adapter classification cannot depend on the
        # gate process's import path; validity deliberately covers the facade.
        typecheck_targets=(ROOT / 'python' / 'gometry',),
        extra_ignored_type_names=EXTRA_IGNORED_TYPE_NAMES,
        macro_exports=(GEOMETRY_LEAF,),
        constructor_doc_macros=frozenset({'h3_index_array_common'}),
        text_signature_default_aliases=TEXT_SIGNATURE_DEFAULT_ALIASES,
        # Array method returns are DERIVED from the scalar contract.
        duality=DualityConfig(pairs=(('Geometry', 'GeometryArray'),)),
        tokens=TokenConfig(
            types_module='gometry._types',
            vocabulary_export='_token_vocabulary',
            extra_vocabulary=frozenset({'ParseFormat'}),
        ),
    )
