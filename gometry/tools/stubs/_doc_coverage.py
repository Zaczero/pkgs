"""Gometry's exception-content policy over shared public documentation subjects.

Presence and signature structure belong to ``pyo3stubs``. This module keeps the
Gometry-specific Raises taxonomy, including actual built-in and foreign-provider
failures, and does not define a second section or subject parser.
"""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING

_TOOLS_ROOT = Path(__file__).resolve().parents[1]
if str(_TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TOOLS_ROOT))
from _gatelib import prepend_rust_source_import_paths
from runtime_artifact import require_runtime

prepend_rust_source_import_paths()
from pyo3stubs.cli import load_config
from pyo3stubs.context import CheckContext
from pyo3stubs.doc_structure import (
    DocSubject,
    iter_doc_subjects,
    parse_doc,
    raises_entries,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

_ROOT = _TOOLS_ROOT.parent

ERROR_EXPORTS = frozenset({
    'CRSError',
    'CRSMismatchError',
    'GeometryError',
    'GeometryTypeError',
    'InvalidGeometryError',
    'ParseError',
    'TransformError',
})
ALLOWED_BUILTIN_RAISES = frozenset({
    'TypeError',
    'AttributeError',
    'IndexError',
    'KeyError',
    'BufferError',
    'MemoryError',
    'OSError',
    'OverflowError',
    'StopIteration',
    'ModuleNotFoundError',
    'ImportError',
    'NotImplementedError',
})
RAISES_VALUEERROR_EXEMPT: dict[str, str] = {
    'GeometryArray.index': 'list.index convention: a missing element is a plain ValueError',
    'GeometryArray.fill_missing': 'pandas fillna row-alignment convention: length mismatch is a plain ValueError',
    'GeometryCollection.index': 'Sequence index lookup uses the list.index missing-value convention.',
    'CellArray.index': 'list.index convention: a missing element is a plain ValueError',
    'Groups.index': 'list.index convention: a missing element is a plain ValueError',
    'Coordinates.index': 'list.index convention: a missing element is a plain ValueError',
    'GeometryParts.index': 'list.index convention: a missing element is a plain ValueError',
    'MultiLineString.index': 'Sequence index lookup uses the list.index missing-value convention.',
    'MultiPoint.index': 'Sequence index lookup uses the list.index missing-value convention.',
    'MultiPolygon.index': 'Sequence index lookup uses the list.index missing-value convention.',
    'H3VertexArray.index': 'list.index convention: a missing element is a plain ValueError',
    'H3EdgeArray.index': 'list.index convention: a missing element is a plain ValueError',
}
_MIRRORED_RAISES = frozenset({
    'coverage_is_valid',
    'coverage_invalid_edges',
    'coverage_simplify',
    'coverage_clean',
    'coverage_union',
    'union_all',
    'intersection_all',
    'symmetric_difference_all',
})
_FRAME_GUARDED = (
    'contains',
    'contains_properly',
    'within',
    'covers',
    'covered_by',
    'intersects',
    'disjoint',
    'touches',
    'crosses',
    'overlaps',
    'relate',
    'relate_pattern',
    'equals',
    'equals_exact',
    'dwithin',
    'distance',
    'distance_3d',
    'hausdorff_distance',
    'frechet_distance',
    'nearest_points',
    'shortest_line',
    'snap',
    'shared_paths',
    'split',
    'intersection',
    'union',
    'difference',
    'symmetric_difference',
    'line_locate',
)
_POLICY_GATED = (
    'buffer',
    'centroid',
    'point_on_surface',
    'envelope',
    'polylabel',
    'maximum_inscribed_circle',
    'voronoi_polygons',
    'voronoi_edges',
    'minimum_rotated_rectangle',
)
_METRIC_CRS_AWARE: dict[str, frozenset[str]] = {
    'area': frozenset({'CRSError', 'GeometryError'}),
    'length': frozenset({'CRSError', 'GeometryError'}),
    'length_3d': frozenset({'CRSError'}),
    'distance': frozenset({'CRSError', 'GeometryError'}),
    'distance_3d': frozenset({'CRSError'}),
}
_LINREF_SCALAR_INVALID_GEOMETRY_ERROR = frozenset({
    'line_interpolate',
    'line_substring',
    'line_locate',
})
_OP_EXTRAS: dict[str, frozenset[str]] = {
    'buffer': frozenset({'GeometryError'}),
    'simplify': frozenset({'GeometryError'}),
    'smooth': frozenset({'GeometryError'}),
    'offset_curve': frozenset({'GeometryError'}),
    'quantize': frozenset({'GeometryError'}),
    'concave_hull': frozenset({'GeometryError'}),
    'affine_transform': frozenset({'GeometryError'}),
    'equals_exact': frozenset({'GeometryError'}),
    'snap': frozenset({'GeometryError'}),
    'segmentize': frozenset({'GeometryError'}),
    'remove_repeated_points': frozenset({'GeometryError'}),
    'clip_by_rect': frozenset({'GeometryError'}),
    'to_wkt': frozenset({'CRSError', 'GeometryError'}),
    'to_wkb': frozenset({'CRSError'}),
    'contains_xy': frozenset({'InvalidGeometryError'}),
    'intersects_xy': frozenset({'InvalidGeometryError'}),
    'require': frozenset({'InvalidGeometryError', 'CRSMismatchError'}),
    'dwithin': frozenset({'GeometryError'}),
    'relate_pattern': frozenset({'GeometryError'}),
    'frechet_distance': frozenset({'GeometryTypeError', 'InvalidGeometryError'}),
    'split': frozenset({'GeometryTypeError'}),
    'shared_paths': frozenset({'GeometryTypeError'}),
    'line_merge': frozenset({'GeometryTypeError'}),
    'line_interpolate': frozenset({'GeometryTypeError', 'GeometryError'}),
    'line_substring': frozenset({'GeometryTypeError', 'GeometryError'}),
    'line_locate': frozenset({'GeometryTypeError', 'CRSError'}),
    'intersection': frozenset({'InvalidGeometryError'}),
    'union': frozenset({'InvalidGeometryError'}),
    'difference': frozenset({'InvalidGeometryError'}),
    'symmetric_difference': frozenset({'InvalidGeometryError'}),
    'to_geojson': frozenset({'CRSError', 'InvalidGeometryError'}),
}
_OP_SURFACES = ('', 'Geometry', 'GeometryArray')


def required_raises(
    qualname: str, parameters: set[str] | None = None
) -> frozenset[str]:
    """The exception classes ``qualname``'s Raises section must document.

    ``parameters`` are the runtime signature's parameter names; a ``crs``
    parameter implies CRS validation, so ``CRSError`` is required wherever the
    signature accepts one.
    """
    owner, _, name = qualname.rpartition('.')
    required = set()
    if owner in _OP_SURFACES:
        if name in _FRAME_GUARDED:
            required.add('CRSMismatchError')
        if name in _POLICY_GATED:
            required.add('InvalidGeometryError')
        extras = set(_OP_EXTRAS.get(name, frozenset()))
        if name == 'to_wkt' and owner == 'CRS':
            extras.discard('GeometryError')
        required |= extras
        metric = set(_METRIC_CRS_AWARE.get(name, frozenset()))
        # Properties ``area``/``length`` have no ``unit=`` (Rule 4 free fn owns
        # that override). Natural CRS-aware measurement raises ``CRSError`` only
        # (axis units / geographic domain); ``GeometryError`` is free-fn-only.
        if name in ('area', 'length') and owner in ('Geometry', 'GeometryArray'):
            metric.discard('GeometryError')
        required |= metric
        if name in _LINREF_SCALAR_INVALID_GEOMETRY_ERROR and owner in ('', 'Geometry'):
            required.add('InvalidGeometryError')
        if name == 'distance_3d' and owner in ('', 'Geometry'):
            required.add('InvalidGeometryError')
    if name in (
        'union_all',
        'intersection_all',
        'symmetric_difference_all',
    ) and owner in ('', 'GeometryArray'):
        # A GeometryArray carries ONE frame by construction, so the ARRAY
        # aggregate methods can never see a frame mismatch; only the free
        # functions (mixed iterables) carry the CRSMismatchError floor.
        required.add('InvalidGeometryError')
        if owner == '':
            required.add('CRSMismatchError')
    # Polygonal-coverage free duals + GeometryArray methods. Free forms take
    # mixed iterables and therefore require CRSMismatchError; array methods
    # share one frame. GeometryTypeError is the wrong-kind floor except for
    # coverage_union (empty/invalid-coverage content only). InvalidGeometryError
    # is content: simplify/union require a valid coverage; clean can fail when
    # snap-repair cannot converge. Param-value lanes keep base GeometryError.
    if name in (
        'coverage_is_valid',
        'coverage_invalid_edges',
        'coverage_simplify',
        'coverage_union',
        'coverage_clean',
    ) and owner in ('', 'GeometryArray'):
        if name != 'coverage_union':
            required.add('GeometryTypeError')
        if name in (
            'coverage_is_valid',
            'coverage_invalid_edges',
            'coverage_simplify',
            'coverage_clean',
        ):
            required.add('GeometryError')
        if name in ('coverage_simplify', 'coverage_union', 'coverage_clean'):
            required.add('InvalidGeometryError')
        if owner == '':
            required.add('CRSMismatchError')
    if name in ('from_wkt', 'from_wkb', 'from_geojson', 'from_arrow') and owner in (
        '',
        'GeometryArray',
    ):
        required.add('ParseError')
    if qualname == 'from_arrow':
        # Arrow-C stream callback failures remain native protocol errors:
        # status 12 is MemoryError, other nonzero statuses are OSError.
        required |= {'MemoryError', 'OSError'}
    if qualname in ('box', 'points'):
        required.add('InvalidGeometryError')
    if name == 'to_crs':
        required |= {'CRSError', 'TransformError'}
    if qualname in ('crs_transform', 'crs_apply', 'crs_roundtrip'):
        required |= {
            'CRSError',
            'TransformError',
            'InvalidGeometryError',
            'GeometryError',
        }
    if qualname == 'crs_transform_bounds':
        required |= {'CRSError', 'TransformError', 'GeometryError'}
    if owner == 'CRS':
        if name == 'factors':
            required.add('InvalidGeometryError')
        elif name in (
            '__init__',
            'to_wkt',
            'to_proj',
            'to_projjson',
            'to_cf',
            'identify',
            'operation',
            'to_epsg',
            'to_authority',
        ):
            required.add('CRSError')
    if owner == 'H3Cell' and name in (
        'parent',
        'children',
        'children_count',
        'center_child',
        'child_at',
        'grid_disk',
        'grid_ring',
    ):
        required.add('GeometryError')
    if owner == 'H3Cell' and name in (
        'grid_path',
        'grid_distance',
        'is_neighbor',
        'local_ij',
    ):
        required |= {'GeometryError', 'ParseError'}

    if owner == 'S2Cell' and name == 'parent':
        required.add('GeometryError')
    if qualname in (
        'h3_cover',
        's2_cover',
        'geohash_cover',
        'tile_cover',
    ):
        required.add('GeometryError')
    if qualname == 'h3_pentagons':
        required.add('GeometryError')
    if qualname in (
        'h3_union',
        'h3_intersection',
        'h3_difference',
        's2_union',
        's2_intersection',
        's2_difference',
    ):
        required.add('ParseError')
    if owner == 'SpatialIndex' and name in ('insert', 'query', 'nearest', 'candidates'):
        required.add('CRSMismatchError')
        if name in ('query', 'nearest', 'candidates'):
            required.add('GeometryError')
    if qualname == 'join':
        required.add('CRSMismatchError')
    if qualname in ('array', 'index'):
        required.add('CRSMismatchError')
    if parameters and 'crs' in parameters:
        required.add('CRSError')
    if parameters and 'epoch' in parameters:
        required.add('GeometryError')
    return frozenset(required)


def _check_raises(
    qualname: str,
    raises: list[str],
    errors: list[str],
    parameters: set[str] | None = None,
    *,
    policy_qualname: str | None = None,
) -> None:
    known = set(ERROR_EXPORTS) | ALLOWED_BUILTIN_RAISES
    for entry in raises:
        if entry == 'ValueError':
            if (policy_qualname or qualname) not in RAISES_VALUEERROR_EXEMPT:
                errors.append(
                    f'{qualname}: Raises names bare ValueError — name the specific gometry exception class (GeometryError if generic)'
                )
        elif entry not in known:
            errors.append(f'{qualname}: Raises names unknown exception {entry!r}')
    missing = required_raises(policy_qualname or qualname, parameters) - set(raises)
    errors.extend(
        f'{qualname}: Raises must document {entry} (fallible-family contract)'
        for entry in sorted(missing)
    )


def check_subject_raises(subject: DocSubject) -> list[str]:
    errors: list[str] = []
    doc = parse_doc(subject.prose)
    entries = raises_entries(doc)
    errors.extend(
        f'{subject.qualname}: Raises {exception!r} requires a non-empty description'
        for entry in entries
        for exception in entry.names
        if not entry.description.strip()
    )
    parameters = (
        {parameter.name for parameter in subject.parameters}
        if subject.parameters is not None
        else None
    )
    policy_qualname = subject.qualname
    if subject.kind == 'constructor' and policy_qualname.endswith('.__new__'):
        policy_qualname = policy_qualname.removesuffix('.__new__') + '.__init__'
    _check_raises(
        subject.qualname,
        [name for entry in entries for name in entry.names],
        errors,
        parameters=parameters,
        policy_qualname=policy_qualname,
    )
    return errors


def check_mirrored_raises(subjects: Iterable[DocSubject]) -> list[str]:
    """Keep free and GeometryArray error contracts aligned for true mirrors."""
    by_name = {
        subject.qualname: subject
        for subject in subjects
        if subject.prose and subject.kind != 'class'
    }
    errors: list[str] = []
    for name in sorted(_MIRRORED_RAISES):
        free = by_name.get(name)
        array = by_name.get(f'GeometryArray.{name}')
        if free is None or array is None:
            continue
        free_raises = {
            exception
            for entry in raises_entries(parse_doc(free.prose))
            for exception in entry.names
        }
        array_raises = {
            exception
            for entry in raises_entries(parse_doc(array.prose))
            for exception in entry.names
        }
        expected_array = free_raises - {'CRSMismatchError'}
        if array_raises != expected_array:
            errors.append(
                f'{name} / GeometryArray.{name}: Raises sets differ; '
                f'array has {sorted(array_raises)}, expected '
                f'{sorted(expected_array)}'
            )
    return errors


def main() -> int:
    gm, _, _ = require_runtime(_ROOT)
    cfg = load_config(_TOOLS_ROOT / 'stubs' / 'stubconfig.py')
    ctx = CheckContext(cfg)
    subjects = tuple(iter_doc_subjects(ctx, runtime_module=gm))
    errors: list[str] = []
    checked: Counter[str] = Counter()
    errors.extend(check_mirrored_raises(subjects))
    for subject in subjects:
        if subject.kind == 'class':
            continue
        owner = subject.qualname.rpartition('.')[0] or '<top-level>'
        checked[owner] += 1
        errors.extend(check_subject_raises(subject))
    for owner, count in sorted(checked.items()):
        print(f'{owner:18} {count:3d} members checked')
    for error in sorted(set(errors)):
        print(f'  {error}', file=sys.stderr)
    print(f'\nTOTAL MISSING: {len(set(errors))}')
    return 1 if errors else 0


if __name__ == '__main__':
    sys.exit(main())
