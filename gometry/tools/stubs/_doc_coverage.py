"""Validate the docstring contract of gometry's public surface.

Beyond presence, every public callable's runtime ``__doc__`` must honor the
documentation contract the stubs and docs are generated from:

* a non-empty docstring;
* every signature parameter documented in the ``Parameters`` section (and no
  documented parameter that is not in the signature — stale docs fail);
* a ``Returns`` section (house style documents ``None`` explicitly);
* any ``default X`` stated in a parameter's doc line matching the runtime
  default;
* every ``Raises`` entry naming a real top-level gometry class or an allowed
  builtin — bare ``ValueError`` is banned (name the specific class), and the
  fallible API families in ``RAISES_REQUIRED`` must document their canonical
  exception (binary frame-guarded ops -> ``CRSMismatchError``, parse fns ->
  ``ParseError``, constructors -> ``InvalidGeometryError``, CRS transforms ->
  ``TransformError``, grid surfaces -> ``GeometryError``/``ParseError``).

Sections are parsed format-agnostically: numpydoc (``Parameters`` over a
``----------`` underline) and Google style (``Args:`` / ``Returns:``) are both
understood, so the checker stays universal if conventions evolve.

Exit code is non-zero on any violation; the final line is the machine-readable
``TOTAL MISSING: N`` summary the gates grep for.
"""

from __future__ import annotations

import ast
import inspect
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

import gometry as gm

_TOOLS_ROOT = Path(__file__).resolve().parents[1]
if str(_TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TOOLS_ROOT))
from _gatelib import prepend_tools_import_paths

prepend_tools_import_paths()
ERROR_EXPORTS = frozenset({
    'CRSError',
    'CRSMismatchError',
    'GeometryError',
    'GeometryTypeError',
    'InvalidGeometryError',
    'ParseError',
    'TransformError',
})
_DOC_COVERAGE_SKIP = ERROR_EXPORTS | frozenset({
    'Cell',
    'Coordinates',
    'Extremes',
    'Features',
    'GeometryCollection',
    'Groups',
    'H3Edge',
    'H3Vertex',
    'LineString',
    'MultiLineString',
    'MultiPoint',
    'MultiPolygon',
    'Point',
    'Polygon',
    'PolygonizeResult',
    'ValidationReport',
})
CLASSES = tuple(
    sorted(
        name
        for name in gm.__all__
        if name not in _DOC_COVERAGE_SKIP and isinstance(getattr(gm, name, None), type)
    )
)
_NUMPYDOC_SECTION = re.compile('^([A-Z][A-Za-z ]+)\\n-+$', re.MULTILINE)
_GOOGLE_SECTION = re.compile('^(Args|Arguments|Returns|Yields|Raises):$', re.MULTILINE)
_DOC_DEFAULT = re.compile('(?:,\\s*|\\()default(?:\\s+|=)(.+?)\\)?\\s*$')
ALLOWED_BUILTIN_RAISES = frozenset({
    'TypeError',
    'IndexError',
    'KeyError',
    'BufferError',
    'OverflowError',
    'StopIteration',
    'ModuleNotFoundError',
    'ImportError',
    'NotImplementedError',
})
RAISES_VALUEERROR_EXEMPT: dict[str, str] = {
    'GeometryArray.index': 'list.index convention: a missing element is a plain ValueError',
    'GeometryArray.fill_missing': 'pandas fillna row-alignment convention: length mismatch is a plain ValueError',
    'CellArray.index': 'list.index convention: a missing element is a plain ValueError',
    'Groups.index': 'list.index convention: a missing element is a plain ValueError',
    'Coordinates.index': 'list.index convention: a missing element is a plain ValueError',
    'GeometryParts.index': 'list.index convention: a missing element is a plain ValueError',
    'H3VertexArray.index': 'list.index convention: a missing element is a plain ValueError',
    'H3EdgeArray.index': 'list.index convention: a missing element is a plain ValueError',
}
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
_OP_SURFACES = ('', 'Geometry', 'GeometryArray', 'PreparedGeometry')


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
    if qualname == 'crs_info':
        required.add('CRSError')
    if owner == 'CRS':
        if name == 'factors':
            required.add('InvalidGeometryError')
        elif name == 'geodesic':
            required |= {'InvalidGeometryError', 'GeometryError'}
        elif name == '__init__' or name in (
            'to_wkt',
            'to_proj',
            'to_projjson',
            'to_cf',
            'identify',
        ):
            required.add('CRSError')
        elif name in ('geodesic_direct', 'geodesic_interpolate'):
            required |= {'CRSError', 'InvalidGeometryError'}
        elif name in ('operation', 'to_epsg', 'to_authority'):
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
    if qualname in ('h3_cover', 's2_cover', 'geohash_cover', 'tile_cover'):
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


@dataclass(slots=True)
class DocContract:
    """The documented parameters and sections of one docstring."""

    has_doc: bool
    parameters: dict[str, str | None] = field(default_factory=dict)
    has_returns: bool = False
    raises: list[str] = field(default_factory=list)


def _numpydoc_sections(doc: str) -> dict[str, str]:
    sections: dict[str, str] = {}
    matches = list(_NUMPYDOC_SECTION.finditer(doc))
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(doc)
        sections[match.group(1).strip()] = doc[match.end() : end]
    return sections


def _google_sections(doc: str) -> dict[str, str]:
    sections: dict[str, str] = {}
    matches = list(_GOOGLE_SECTION.finditer(doc))
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(doc)
        name = {'Args': 'Parameters', 'Arguments': 'Parameters'}.get(
            match.group(1), match.group(1)
        )
        sections[name] = doc[match.end() : end]
    return sections


def _parameter_entries(body: str, *, google: bool) -> dict[str, str | None]:
    """``name -> documented default`` for each parameter the section names.

    numpydoc entries sit at column 0 as ``a, b : type[, default X]`` — the
    ``: type`` part is REQUIRED (griffe tolerates a bare ``name`` line, but
    house style states the type and default on every entry, so a bare name
    reads as an undocumented parameter here); Google entries are indented
    ``name (type): description``.
    """
    entries: dict[str, str | None] = {}
    for line in body.splitlines():
        if not line.strip():
            continue
        if google:
            match = re.match(
                '\\s+([*\\w]+(?:\\s*,\\s*[*\\w]+)*)\\s*(?:\\(([^)]*)\\))?:', line
            )
            if match is None:
                continue
            names, meta = (match.group(1), match.group(2) or '')
        else:
            if line[:1].isspace():
                continue
            names, separator, meta = line.partition(':')
            if not separator or not meta.strip():
                continue
            if re.fullmatch('[*\\w]+(\\s*,\\s*[*\\w]+)*', names.strip()) is None:
                continue
        default_match = _DOC_DEFAULT.search(meta.strip())
        default = default_match.group(1).strip() if default_match else None
        if default and default.count('(') > default.count(')'):
            default += ')'
        for name in names.split(','):
            entries[name.strip().lstrip('*')] = default
    return entries


def parse_doc_contract(doc: str | None) -> DocContract:
    if not (doc or '').strip():
        return DocContract(has_doc=False)
    doc = inspect.cleandoc(doc or '')
    sections = _numpydoc_sections(doc)
    google = False
    if not sections:
        google_sections = _google_sections(doc)
        if google_sections:
            sections, google = (google_sections, True)
    return DocContract(
        has_doc=True,
        parameters=_parameter_entries(sections.get('Parameters', ''), google=google),
        has_returns='Returns' in sections or 'Yields' in sections,
        raises=_raises_entries(sections.get('Raises', ''), google=google),
    )


def _raises_entries(body: str, *, google: bool) -> list[str]:
    """Exception type names heading each ``Raises`` entry."""
    entries = []
    for line in body.splitlines():
        if not line.strip():
            continue
        if google:
            match = re.match('\\s+`?(\\w+)`?\\s*:', line)
        else:
            if line[:1].isspace():
                continue
            match = re.fullmatch('`?(\\w+)`?:?', line.strip())
        if match is not None:
            entries.append(match.group(1))
    return entries


def _defaults_match(documented: str, runtime: object) -> bool:
    text = documented.rstrip('.').strip('`')
    if text in (repr(runtime), str(runtime)):
        return True
    try:
        return ast.literal_eval(text) == runtime
    except (SyntaxError, ValueError):
        return False


def _signature_parameters(obj: object) -> list[inspect.Parameter] | None:
    try:
        signature = inspect.signature(obj)
    except (TypeError, ValueError):
        return None
    return [
        param
        for param in signature.parameters.values()
        if param.name not in ('self', 'cls')
    ]


def check_callable(qualname: str, obj: object, errors: list[str]) -> None:
    contract = parse_doc_contract(getattr(obj, '__doc__', None))
    if not contract.has_doc:
        errors.append(f'{qualname}: missing docstring')
        return
    parameters = _signature_parameters(obj)
    if parameters is None:
        return
    names = {param.name for param in parameters}
    for param in parameters:
        if param.name not in contract.parameters:
            errors.append(f'{qualname}: parameter {param.name!r} not documented')
            continue
        documented = contract.parameters[param.name]
        if (
            documented is not None
            and param.default is not inspect.Parameter.empty
            and (param.default is not ...)
            and (not _defaults_match(documented, param.default))
        ):
            errors.append(
                f'{qualname}.{param.name}: documented default {documented} != runtime {param.default!r}'
            )
    errors.extend(
        f'{qualname}: documents parameter {name!r} not in the signature'
        for name in contract.parameters
        if name not in names
    )
    if not contract.has_returns:
        errors.append(f'{qualname}: missing Returns section')
    _check_raises(qualname, contract.raises, errors, parameters=names)


def _check_raises(
    qualname: str,
    raises: list[str],
    errors: list[str],
    parameters: set[str] | None = None,
) -> None:
    known = set(ERROR_EXPORTS) | ALLOWED_BUILTIN_RAISES
    for entry in raises:
        if entry == 'ValueError':
            if qualname not in RAISES_VALUEERROR_EXEMPT:
                errors.append(
                    f'{qualname}: Raises names bare ValueError — name the specific gometry exception class (GeometryError if generic)'
                )
        elif entry not in known:
            errors.append(f'{qualname}: Raises names unknown exception {entry!r}')
    missing = required_raises(qualname, parameters) - set(raises)
    errors.extend(
        f'{qualname}: Raises must document {entry} (fallible-family contract)'
        for entry in sorted(missing)
    )


def check_property(qualname: str, doc: str | None, errors: list[str]) -> None:
    contract = parse_doc_contract(doc)
    if not contract.has_doc:
        errors.append(f'{qualname}: missing docstring')
        return
    if not contract.has_returns:
        errors.append(f'{qualname}: missing Returns section')
    _check_raises(qualname, contract.raises, errors)


def _is_property_like(cls: type, name: str) -> bool:
    static = inspect.getattr_static(cls, name)
    return isinstance(static, property) or type(static).__name__ in {
        'getset_descriptor',
        'member_descriptor',
    }


def main() -> int:
    errors: list[str] = []
    for cls_name in CLASSES:
        cls = getattr(gm, cls_name, None)
        if cls is None:
            errors.append(f'{cls_name}: class missing from gometry')
            continue
        checked = 0
        for name in sorted(vars(cls)):
            if name.startswith('_'):
                continue
            qualname = f'{cls_name}.{name}'
            if _is_property_like(cls, name):
                checked += 1
                static = inspect.getattr_static(cls, name)
                check_property(qualname, getattr(static, '__doc__', None), errors)
            elif callable(getattr(cls, name, None)):
                checked += 1
                check_callable(qualname, getattr(cls, name), errors)
        print(f'{cls_name:18} {checked:3d} members checked')
    top_level = [
        name
        for name in gm.__all__
        if callable(getattr(gm, name, None))
        and (not isinstance(getattr(gm, name), type))
    ]
    for name in sorted(top_level):
        check_callable(name, getattr(gm, name), errors)
    print(f'{"<top-level>":18} {len(top_level):3d} members checked')
    for error in sorted(set(errors)):
        print(f'  {error}', file=sys.stderr)
    print(f'\nTOTAL MISSING: {len(set(errors))}')
    return 1 if errors else 0


if __name__ == '__main__':
    sys.exit(main())
