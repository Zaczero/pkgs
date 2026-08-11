#!/usr/bin/env python3
"""Docstring style gate for gometry's public surface.

Operates on the generated stub ``python/gometry/_lib.pyi`` as the docstring
oracle. Public inventory is ``__all__`` plus exported-class public members.
Hard checks follow the detection SPEC; STYLE_GUIDE section 11 checks emit
``style.*`` classes. Exit: 0 clean / 1 violations / 2 incomplete.
"""

from __future__ import annotations

import argparse
import ast
import collections
import doctest
import inspect
import io
import json
import re
import sys
import tokenize
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from collections.abc import Sequence

ROOT = Path(__file__).resolve().parents[2]
STUB_PATH = ROOT / 'python' / 'gometry' / '_lib.pyi'
INIT_PATH = ROOT / 'python' / 'gometry' / '__init__.py'
SRC_ROOT = ROOT / 'src'

SECTION_NAMES = frozenset({
    'Parameters',
    'Other Parameters',
    'Keyword Arguments',
    'Returns',
    'Yields',
    'Receives',
    'Attributes',
    'Raises',
    'Warns',
    'See Also',
    'Notes',
    'References',
    'Examples',
})
NAMED_TYPE_SECTIONS = frozenset({
    'Parameters',
    'Other Parameters',
    'Keyword Arguments',
    'Attributes',
})
RETURN_TYPE_SECTIONS = frozenset({'Returns', 'Yields', 'Receives'})

UNDERLINE_RE = re.compile(r'-{3,}')
PARAM_NAMES_RE = re.compile(
    r'\*{0,2}[A-Za-z_]\w*'
    r'(?:\s*,\s*\*{0,2}[A-Za-z_]\w*)*'
)
NDARRAY_RE = re.compile(r'(?<![\w.])(?:np\.ndarray|ndarray)(?![\w.])')
DOUBLED_WORD_RE = re.compile(
    r"(?<![\w'-])(?P<word>[A-Za-z][A-Za-z'-]*)"
    r'(?P<separator>[ \t\n]+)(?P=word)(?![\w\'-])',
    re.IGNORECASE,
)
TRAILING_DOUBLE_PERIOD_RE = re.compile(r"(?<!\.)\.\.(?!\.)(?=(?:[)\]}'\"`*_]*)$)")
SEE_ALSO_LINE_RE = re.compile(
    r'^(?P<name>[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)'
    r' : '
    r'(?P<description>[A-Z].*\.)$'
)
MERGED_SEE_ALSO_RE = re.compile(
    r'[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?\s*:\s+[A-Z].*?\.'
    r'\s+[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?\s*:'
)
UNQUOTED_ENUM_RE = re.compile(r'\{(?P<body>[^{}\'"`]*[A-Za-z_][^{}\'"`]*)\}')
ASTERISK_AB_RE = re.compile(r'\*[ab]\*')
GM_PREFIX_RE = re.compile(r'\b(?:gometry|gm)\.[A-Za-z_]\w*')
PER_ELEMENT_RE = re.compile(r'\bper element\b', re.IGNORECASE)
PLANAR_TAG_RE = re.compile(r'\(planar\)', re.IGNORECASE)

BANNED_TERM_PATTERNS: tuple[tuple[str, str], ...] = (
    ('the natural way', 'style.banned_term.natural_way'),
    ('metres', 'style.banned_term.metres'),
    ('Amortises', 'style.banned_term.amortises'),
    ('iterable of float', 'style.banned_term.iterable_of_float'),
    ('array-like', 'style.banned_term.array_like'),
)

# Invert the STYLE_GUIDE section 11.1 heuristic: flag noun-phrase openers on
# callables (and "Test whether" is allowed). Properties should open with
# "Whether" or a noun phrase — flag imperative "Test " on properties.
CALLABLE_NOUN_OPENERS = frozenset({
    'whether',
    'the',
    'a',
    'an',
    'area',
    'length',
    'distance',
    'lazy',
    'read-only',
    'readonly',
    'crs-aware',
    'native',
    'packed',
    'vectorized',
    'kind-preserving',
    'element-wise',
    'row-wise',
    'one',
    'same',
    'full',
    'partial',
    'strict',
    'lenient',
    'exact',
    'approximate',
    'geodesic',
    'planar',
    'geographic',
    'projected',
    'ellipsoidal',
    'spherical',
    'cartesian',
    'minimum',
    'maximum',
    'total',
    'true',
    'false',
    'empty',
    'missing',
    'null',
    'valid',
    'invalid',
    'simple',
    'complex',
    'convex',
    'concave',
    'closed',
    'open',
    'ring',
    'boundary',
    'interior',
    'exterior',
    'bounds',
    'envelope',
    'extent',
    'range',
    'span',
    'width',
    'height',
    'depth',
    'volume',
    'mass',
    'weight',
    'density',
    'pressure',
    'temperature',
    'energy',
    'power',
    'work',
    'force',
    'torque',
    'moment',
    'impulse',
    'momentum',
    'velocity',
    'acceleration',
    'jerk',
    'speed',
    'displacement',
    'position',
    'location',
    'place',
    'site',
    'spot',
    'coordinate',
    'coordinates',
    'ordinate',
    'vertex',
    'vertices',
    'edge',
    'edges',
    'face',
    'faces',
    'cell',
    'cells',
    'token',
    'tokens',
    'id',
    'ids',
    'index',
    'indexes',
    'indices',
    'row',
    'rows',
    'column',
    'columns',
    'array',
    'arrays',
    'geometry',
    'geometries',
    'shape',
    'shapes',
    'polygon',
    'polygons',
    'line',
    'lines',
    'point',
    'points',
    'rings',
    'multipart',
    'collection',
    'collections',
    'feature',
    'features',
    'coverage',
    'coverages',
    'prepared',
    'unprepared',
    'spatial',
    'topological',
    'metric',
})

# Ops that auto-split geographic antimeridian crossings — "(planar)" is wrong.
AUTO_SPLIT_OPS = frozenset({
    'contains',
    'within',
    'covers',
    'covered_by',
    'intersects',
    'disjoint',
    'touches',
    'crosses',
    'overlaps',
    'equals',
    'relate',
    'relate_pattern',
    'intersection',
    'union',
    'difference',
    'symmetric_difference',
    'clip_by_rect',
})
# Genuinely planar-only — "(planar)" is allowed.
GENUINELY_PLANAR_OPS = frozenset({
    'simplify',
    'smooth',
    'convex_hull',
    'buffer',
    'offset_curve',
    'segmentize',
})

EXAMPLES_EXEMPTIONS: dict[str, str] = {
    # collections.abc.Sequence protocol — no domain semantics beyond list.count/index.
    'CellArray.count': 'Sequence protocol membership count; no grid-specific meaning.',
    'CellArray.index': 'Sequence protocol first-index lookup; no grid-specific meaning.',
    'Coordinates.count': 'Sequence protocol membership count on coordinate tuples.',
    'Coordinates.index': 'Sequence protocol first-index lookup on coordinate tuples.',
    'GeometryArray.count': 'Sequence protocol membership count; no geometry-op meaning.',
    'GeometryArray.index': 'Sequence protocol first-index lookup; raises ValueError by Python convention.',
    'GeometryParts.count': 'Sequence protocol membership count over lazy parts view.',
    'GeometryParts.index': 'Sequence protocol first-index lookup over lazy parts view.',
    'Groups.count': 'Sequence protocol membership count over ragged row groups.',
    'Groups.index': 'Sequence protocol first-index lookup over ragged row groups.',
    'H3EdgeArray.count': 'Sequence protocol membership count over directed edges.',
    'H3EdgeArray.index': 'Sequence protocol first-index lookup over directed edges.',
    'H3VertexArray.count': 'Sequence protocol membership count over H3 vertices.',
    'H3VertexArray.index': 'Sequence protocol first-index lookup over H3 vertices.',
}

MIRROR_PAIRS = frozenset({
    'coverage_is_valid',
    'coverage_invalid_edges',
    'coverage_simplify',
    'coverage_clean',
    'coverage_union',
    'union_all',
    'intersection_all',
    'symmetric_difference_all',
})

SAME_NAME_NON_MIRRORS = {
    'polygonize': 'GeometryArray method is row-wise; free function pools all linework',
}

# Doubled-word intentional allowlist: (symbol, normalized_phrase) -> rationale.
DOUBLED_WORD_ALLOWLIST: dict[tuple[str, str], str] = {}


@dataclass(frozen=True, slots=True)
class ReciprocalFamily:
    key: str
    left: frozenset[str]
    right: frozenset[str]
    reason: str


def _owner_pairs(*names: str) -> frozenset[str]:
    """Expand bare names over Geometry and GeometryArray."""
    out: set[str] = set()
    for name in names:
        if '.' in name:
            out.add(name)
        else:
            out.add(f'Geometry.{name}')
            out.add(f'GeometryArray.{name}')
    return frozenset(out)


RECIPROCAL_FAMILIES: tuple[ReciprocalFamily, ...] = (
    ReciprocalFamily(
        'contains_within',
        frozenset({'contains'}),
        frozenset({'within'}),
        'inverse containment predicates',
    ),
    ReciprocalFamily(
        'covers_covered_by',
        frozenset({'covers'}),
        frozenset({'covered_by'}),
        'inverse covering predicates',
    ),
    ReciprocalFamily(
        'intersects_disjoint',
        frozenset({'intersects'}),
        frozenset({'disjoint'}),
        'negation predicates',
    ),
    ReciprocalFamily(
        'from_wkt_to_wkt',
        frozenset({'from_wkt'}),
        frozenset({'Geometry.to_wkt', 'GeometryArray.to_wkt'}),
        'WKT encode/decode',
    ),
    ReciprocalFamily(
        'from_wkb_to_wkb',
        frozenset({'from_wkb'}),
        frozenset({'Geometry.to_wkb', 'GeometryArray.to_wkb'}),
        'WKB encode/decode',
    ),
    ReciprocalFamily(
        'from_geojson_to_geojson',
        frozenset({'from_geojson'}),
        frozenset({'Geometry.to_geojson', 'GeometryArray.to_geojson'}),
        'GeoJSON encode/decode',
    ),
    ReciprocalFamily(
        'from_polyline_to_polyline',
        frozenset({'from_polyline'}),
        frozenset({'Geometry.to_polyline'}),
        'polyline encode/decode',
    ),
    ReciprocalFamily(
        'from_arrow_to_arrow',
        frozenset({'from_arrow'}),
        frozenset({'GeometryArray.to_arrow'}),
        'Arrow encode/decode',
    ),
    ReciprocalFamily(
        'buffer_offset_curve',
        _owner_pairs('buffer'),
        _owner_pairs('offset_curve'),
        'buffer vs offset curve',
    ),
    ReciprocalFamily(
        'convex_concave_hull',
        _owner_pairs('convex_hull'),
        _owner_pairs('concave_hull'),
        'hull family',
    ),
    ReciprocalFamily(
        'force_2d_3d',
        _owner_pairs('force_2d'),
        _owner_pairs('force_3d'),
        'dimension force verbs',
    ),
    ReciprocalFamily(
        'set_z_m',
        _owner_pairs('set_z'),
        _owner_pairs('set_m'),
        'ordinate setters',
    ),
    ReciprocalFamily(
        'mic_mir',
        _owner_pairs('maximum_inscribed_circle'),
        _owner_pairs('maximum_inscribed_radius'),
        'inscribed circle/radius',
    ),
    ReciprocalFamily(
        'linref',
        _owner_pairs(
            'line_interpolate',
            'line_substring',
            'line_locate',
            'interpolate_m',
        ),
        frozenset(),  # complete group — any links any other
        'linear referencing complete group',
    ),
    ReciprocalFamily(
        'centroids',
        _owner_pairs(
            'centroid',
            'point_on_surface',
            'polylabel',
            'maximum_inscribed_circle',
        ),
        frozenset(),
        'representative-point complete group',
    ),
    ReciprocalFamily(
        'distance_family',
        frozenset({
            'distance',
            'dwithin',
            'hausdorff_distance',
            'frechet_distance',
            'nearest_points',
            'shortest_line',
        }),
        frozenset(),
        'top-level distance complete group',
    ),
    ReciprocalFamily(
        'coverage_valid_invalid',
        frozenset({'coverage_is_valid', 'GeometryArray.coverage_is_valid'}),
        frozenset({
            'coverage_invalid_edges',
            'GeometryArray.coverage_invalid_edges',
        }),
        'coverage validity dual (free + GeometryArray)',
    ),
    ReciprocalFamily(
        'coverage_valid_clean',
        frozenset({'coverage_is_valid', 'GeometryArray.coverage_is_valid'}),
        frozenset({'coverage_clean', 'GeometryArray.coverage_clean'}),
        'coverage validity vs clean (free + GeometryArray)',
    ),
    ReciprocalFamily(
        'coverage_union_union_all',
        frozenset({'coverage_union', 'GeometryArray.coverage_union'}),
        frozenset({'union_all', 'GeometryArray.union_all'}),
        'coverage_union vs union_all (free + GeometryArray dual)',
    ),
    ReciprocalFamily(
        'simplify_coverage_simplify',
        frozenset({'Geometry.simplify', 'GeometryArray.simplify'}),
        frozenset({'coverage_simplify', 'GeometryArray.coverage_simplify'}),
        'geometry simplify vs coverage simplify (free + GeometryArray)',
    ),
)


@dataclass(frozen=True, slots=True)
class Span:
    path: Path
    line: int
    column: int
    end_line: int = 0
    end_column: int = 0


@dataclass(frozen=True, slots=True)
class DocLine:
    text: str
    start: int
    end: int


@dataclass(frozen=True, slots=True)
class PublicDoc:
    symbol: str
    text: str
    kind: Literal['function', 'method', 'property', 'class', 'dunder']
    is_operation: bool
    owner_class: str | None = None


@dataclass(frozen=True, slots=True)
class Section:
    name: str
    header_index: int
    body_start: int
    body_end: int


@dataclass(frozen=True, slots=True)
class Entry:
    header: DocLine
    description: tuple[DocLine, ...]


@dataclass(slots=True)
class Violation:
    file: str
    line: int | None
    column: int | None
    owner_file: str | None
    owner_line: int | None
    source_kind: str
    symbol: str
    class_: str
    section: str | None
    entry: str | None
    current_text: str | None
    suggested_fix: str
    message: str
    related_symbol: str | None = None
    shared_count: int | None = None

    def to_json(self) -> dict[str, object]:
        value = asdict(self)
        value['class'] = value.pop('class_')
        # Drop shared_count when null for cleaner schema, but keep for multi-site.
        if value.get('shared_count') is None:
            value.pop('shared_count', None)
        return value


@dataclass(slots=True)
class ScanError:
    message: str
    symbol: str | None = None
    file: str | None = None
    line: int | None = None

    def to_json(self) -> dict[str, object]:
        return asdict(self)


def doc_lines(text: str) -> tuple[DocLine, ...]:
    lines: list[DocLine] = []
    offset = 0
    for raw in text.splitlines(keepends=True):
        logical = raw.removesuffix('\n')
        lines.append(DocLine(logical, offset, offset + len(logical)))
        offset += len(raw)
    if not lines or text.endswith('\n'):
        lines.append(DocLine('', offset, offset))
    return tuple(lines)


def is_unindented(line: DocLine) -> bool:
    return not line.text[:1].isspace()


def segment_sections(
    lines: Sequence[DocLine],
) -> tuple[tuple[Section, ...], tuple[tuple[int, str], ...]]:
    starts: list[tuple[int, str]] = []
    for index in range(len(lines) - 1):
        header = lines[index]
        underline = lines[index + 1]
        if (
            is_unindented(header)
            and header.text in SECTION_NAMES
            and is_unindented(underline)
            and UNDERLINE_RE.fullmatch(underline.text)
        ):
            starts.append((index, header.text))

    sections: list[Section] = []
    missing_blanks: list[tuple[int, str]] = []
    for position, (index, name) in enumerate(starts):
        if index > 0 and lines[index - 1].text.strip():
            missing_blanks.append((index, lines[index - 1].text))
        body_end = starts[position + 1][0] if position + 1 < len(starts) else len(lines)
        sections.append(Section(name, index, index + 2, body_end))
    return tuple(sections), tuple(missing_blanks)


def entries(section: Section, lines: Sequence[DocLine]) -> tuple[Entry, ...]:
    result: list[Entry] = []
    index = section.body_start
    while index < section.body_end:
        if not lines[index].text.strip():
            index += 1
            continue
        header = lines[index]
        if not is_unindented(header):
            index += 1
            continue
        end = index + 1
        while end < section.body_end:
            candidate = lines[end]
            if candidate.text.strip() and is_unindented(candidate):
                break
            end += 1
        result.append(Entry(header, tuple(lines[index + 1 : end])))
        index = end
    return tuple(result)


def type_column(
    section: Section,
    entry: Entry,
) -> tuple[int, int, str] | None:
    text = entry.header.text
    if section.name in NAMED_TYPE_SECTIONS:
        if ':' not in text:
            return None
        lhs, _rhs = text.split(':', 1)
        clean_lhs = lhs.strip().replace('`', '')
        if not PARAM_NAMES_RE.fullmatch(clean_lhs):
            return None
        local_start = text.index(':') + 1
        while local_start < len(text) and text[local_start].isspace():
            local_start += 1
        return (
            entry.header.start + local_start,
            entry.header.end,
            text[local_start:],
        )
    if section.name in RETURN_TYPE_SECTIONS:
        if ':' in text:
            lhs, _ = text.split(':', 1)
            if PARAM_NAMES_RE.fullmatch(lhs.strip().replace('`', '')):
                local_start = text.index(':') + 1
                while local_start < len(text) and text[local_start].isspace():
                    local_start += 1
                return (
                    entry.header.start + local_start,
                    entry.header.end,
                    text[local_start:],
                )
        return entry.header.start, entry.header.end, text
    return None


def summary_paragraph(lines: Sequence[DocLine], sections: Sequence[Section]) -> str:
    """First non-empty paragraph before any section header."""
    end = sections[0].header_index if sections else len(lines)
    parts: list[str] = []
    for i in range(end):
        t = lines[i].text
        if not t.strip():
            if parts:
                break
            continue
        parts.append(t.strip())
    return ' '.join(parts)


def extended_summary(lines: Sequence[DocLine], sections: Sequence[Section]) -> str:
    """Prose between the summary paragraph and the first section."""
    end = sections[0].header_index if sections else len(lines)
    # skip summary (first paragraph)
    i = 0
    saw = False
    while i < end:
        if lines[i].text.strip():
            saw = True
        elif saw:
            i += 1
            break
        i += 1
    parts = [lines[j].text for j in range(i, end)]
    return '\n'.join(parts)


def mask_backticks(text: str) -> str:
    """Replace inline code spans with spaces of equal length."""
    out: list[str] = []
    i = 0
    n = len(text)
    while i < n:
        if text.startswith('``', i):
            j = text.find('``', i + 2)
            if j < 0:
                out.append(text[i:])
                break
            out.append(' ' * (j + 2 - i))
            i = j + 2
        elif text[i] == '`':
            j = text.find('`', i + 1)
            if j < 0:
                out.append(text[i:])
                break
            out.append(' ' * (j + 1 - i))
            i = j + 1
        else:
            out.append(text[i])
            i += 1
    return ''.join(out)


def _is_property(node: ast.AST) -> bool:
    if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return False
    for dec in node.decorator_list:
        if isinstance(dec, ast.Name) and dec.id == 'property':
            return True
        if isinstance(dec, ast.Attribute) and dec.attr == 'property':
            return True
        # @foo.getter
        if isinstance(dec, ast.Attribute) and dec.attr == 'getter':
            return True
    return False


def _doc_from_node(node: ast.AST) -> str | None:
    doc = ast.get_docstring(node, clean=True)
    if doc is None:
        return None
    return inspect.cleandoc(doc).rstrip()


def load_public_all(init_path: Path = INIT_PATH) -> frozenset[str]:
    tree = ast.parse(init_path.read_text(encoding='utf-8'), filename=str(init_path))
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == '__all__':
                    try:
                        value = ast.literal_eval(node.value)
                    except Exception as exc:
                        raise RuntimeError(f'cannot eval __all__: {exc}') from exc
                    if not isinstance(value, (list, tuple)):
                        raise RuntimeError('__all__ is not a list')
                    return frozenset(str(x) for x in value)
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):  # noqa: SIM102
            if node.target.id == '__all__' and node.value is not None:
                value = ast.literal_eval(node.value)
                return frozenset(str(x) for x in value)
    raise RuntimeError('__all__ not found in __init__.py')


def load_inventory(
    stub_path: Path = STUB_PATH,
    init_path: Path = INIT_PATH,
) -> tuple[list[PublicDoc], list[ScanError], frozenset[str]]:
    """Return public docs, scan errors, and the public symbol name set."""
    public_all = load_public_all(init_path)
    source = stub_path.read_text(encoding='utf-8')
    tree = ast.parse(source, filename=str(stub_path))
    docs: list[PublicDoc] = []
    errors: list[ScanError] = []
    seen_qnames: set[str] = set()

    # Group overloads by name: keep the one with a non-empty docstring.
    top_funcs: dict[str, list[ast.FunctionDef | ast.AsyncFunctionDef]] = (
        collections.defaultdict(list)
    )
    classes: dict[str, ast.ClassDef] = {}

    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            top_funcs[node.name].append(node)
        elif isinstance(node, ast.ClassDef):
            classes[node.name] = node

    # Top-level public functions
    for name, group in sorted(top_funcs.items()):
        if name not in public_all:
            continue
        carrier = None
        for fn in group:
            d = _doc_from_node(fn)
            if d:
                carrier = (fn, d)
                break
        if carrier is None:
            # Prefer last (implementation) overload
            carrier = (group[-1], _doc_from_node(group[-1]) or '')
        fn, doc = carrier
        qname = name
        seen_qnames.add(qname)
        if not doc.strip():
            errors.append(
                ScanError(
                    message=f'public function has empty docstring: {qname}',
                    symbol=qname,
                    file='python/gometry/_lib.pyi',
                )
            )
            continue
        docs.append(
            PublicDoc(
                symbol=qname,
                text=doc,
                kind='function',
                is_operation=True,
            )
        )

    # Exported classes and their public members
    for name, cls in sorted(classes.items()):
        if name not in public_all:
            continue
        class_doc = _doc_from_node(cls) or ''
        seen_qnames.add(name)
        if class_doc.strip():
            docs.append(
                PublicDoc(
                    symbol=name,
                    text=class_doc,
                    kind='class',
                    is_operation=False,
                )
            )
        else:
            # Class may legitimately have empty docs if it's an exception/marker;
            # still record as scanned via a placeholder so completeness holds.
            docs.append(
                PublicDoc(
                    symbol=name,
                    text='',
                    kind='class',
                    is_operation=False,
                )
            )

        # Members: group overloads by name
        members: dict[str, list[ast.FunctionDef | ast.AsyncFunctionDef]] = (
            collections.defaultdict(list)
        )
        for item in cls.body:
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if item.name.startswith('_') and not (
                    item.name.startswith('__') and item.name.endswith('__')
                ):
                    continue  # private
                members[item.name].append(item)

        for mname, group in sorted(members.items()):
            carrier = None
            for fn in group:
                d = _doc_from_node(fn)
                if d:
                    carrier = (fn, d)
                    break
            if carrier is None:
                carrier = (group[-1], _doc_from_node(group[-1]) or '')
            fn, doc = carrier
            qname = f'{name}.{mname}'
            seen_qnames.add(qname)
            is_prop = _is_property(fn)
            is_dunder = (
                mname.startswith('__') and mname.endswith('__') and mname != '__new__'
            )
            if mname in {'__new__', '__init__'}:
                kind: Literal['function', 'method', 'property', 'class', 'dunder'] = (
                    'method'
                )
                is_op = False  # constructor — class docs own Examples
            elif is_prop:
                kind = 'property'
                is_op = False
            elif is_dunder:
                kind = 'dunder'
                is_op = False
            else:
                kind = 'method'
                is_op = True
            if not doc.strip() and kind in ('method', 'property', 'function'):  # noqa: SIM102
                # Still scan empty to completeness; flag if operation.
                if is_op:
                    errors.append(
                        ScanError(
                            message=f'public operation has empty docstring: {qname}',
                            symbol=qname,
                            file='python/gometry/_lib.pyi',
                        )
                    )
            docs.append(
                PublicDoc(
                    symbol=qname,
                    text=doc or '',
                    kind=kind,
                    is_operation=is_op,
                    owner_class=name,
                )
            )

    return docs, errors, frozenset(seen_qnames)


_RUST_INDEX: dict[str, list[tuple[str, int]]] | None = None


def _build_rust_index(root: Path = SRC_ROOT) -> dict[str, list[tuple[str, int]]]:
    """Map stripped docstring lines to (relpath, 1-based line)."""
    index: dict[str, list[tuple[str, int]]] = collections.defaultdict(list)
    if not root.is_dir():
        return {}
    for path in sorted(root.rglob('*.rs')):
        try:
            text = path.read_text(encoding='utf-8')
        except OSError:
            continue
        rel = path.relative_to(ROOT).as_posix()
        for i, line in enumerate(text.splitlines(), start=1):
            stripped = line.lstrip()
            if stripped.startswith('///'):
                body = stripped[3:]
                body = body.removeprefix(' ')
                key = body.strip()
                if key:
                    index[key].append((rel, i))
            elif 'doc =' in stripped or 'concat!' in stripped:
                # Capture string literals in doc attributes roughly.
                for m in re.finditer(r'"((?:\\.|[^"\\])*)"', line):
                    lit = m.group(1)
                    # Unescape common sequences lightly
                    lit = (
                        lit
                        .replace('\\n', '\n')
                        .replace('\\t', '\t')
                        .replace('\\"', '"')
                    )
                    for piece in lit.splitlines():
                        key = piece.strip()
                        if key:
                            index[key].append((rel, i))
    return dict(index)


def rust_index() -> dict[str, list[tuple[str, int]]]:
    global _RUST_INDEX
    if _RUST_INDEX is None:
        _RUST_INDEX = _build_rust_index()
    return _RUST_INDEX


def locate_rust(
    current_text: str | None,
    *,
    max_snippet: int = 80,
) -> tuple[str | None, int | None, int | None]:
    """Return (file, line, shared_count) for offending text."""
    if not current_text:
        return None, None, None
    # Prefer a short distinctive line from the text.
    candidates: list[str] = []
    for raw in current_text.splitlines():
        s = raw.strip()
        if s:
            candidates.append(s[:max_snippet])
    if not candidates:
        candidates = [current_text.strip()[:max_snippet]]
    idx = rust_index()
    best: list[tuple[str, int]] = []
    for cand in candidates:
        hits = idx.get(cand)
        if hits:
            best = hits
            break
        # substring search over keys (expensive but rare fallback)
        for key, hits in idx.items():
            if cand in key or key in cand:
                best = hits
                break
        if best:
            break
    if not best:
        return None, None, None
    # Prefer macro-ish paths (methods/, macros)
    best_sorted = sorted(
        best,
        key=lambda h: (
            0 if 'macro' in h[0] or 'methods' in h[0] else 1,
            h[0],
            h[1],
        ),
    )
    file, line = best_sorted[0]
    shared = len(best)
    return file, line, shared if shared > 1 else None


def _v(
    doc: PublicDoc,
    *,
    class_: str,
    message: str,
    suggested_fix: str,
    section: str | None = None,
    entry: str | None = None,
    current_text: str | None = None,
    related_symbol: str | None = None,
    file: str | None = None,
    line: int | None = None,
    column: int | None = 0,
    shared_count: int | None = None,
) -> Violation:
    rfile, rline, shared = locate_rust(current_text)
    if file is None:
        file = rfile or 'python/gometry/_lib.pyi'
    if line is None:
        line = rline
    if shared_count is None:
        shared_count = shared
    return Violation(
        file=file,
        line=line,
        column=column,
        owner_file=rfile,
        owner_line=rline,
        source_kind='stub',
        symbol=doc.symbol,
        class_=class_,
        section=section,
        entry=entry,
        current_text=current_text,
        suggested_fix=suggested_fix,
        message=message,
        related_symbol=related_symbol,
        shared_count=shared_count,
    )


def check_type_columns(
    doc: PublicDoc,
    lines: Sequence[DocLine],
    sections: Sequence[Section],
) -> list[Violation]:
    findings: list[Violation] = []
    for section in sections:
        if section.name not in NAMED_TYPE_SECTIONS | RETURN_TYPE_SECTIONS:
            continue
        for entry in entries(section, lines):
            column = type_column(section, entry)
            if column is None:
                continue
            _start, _end, text = column
            if '`' in text:
                findings.append(
                    _v(
                        doc,
                        class_='type_column.backticks',
                        section=section.name,
                        entry=entry.header.text,
                        current_text=text,
                        suggested_fix=text.replace('`', ''),
                        message='Type columns use bare identifiers, never code markup.',
                    )
                )
            for match in NDARRAY_RE.finditer(text):
                fixed = text[: match.start()] + 'numpy.ndarray' + text[match.end() :]
                findings.append(
                    _v(
                        doc,
                        class_='type_column.ndarray_spelling',
                        section=section.name,
                        entry=entry.header.text,
                        current_text=text,
                        suggested_fix=fixed,
                        message='Use numpy.ndarray in NumPy-doc type columns.',
                    )
                )
            # parameter name markup on the LHS
            if (
                section.name in NAMED_TYPE_SECTIONS
                and '`' in entry.header.text.split(':', 1)[0]
            ):
                findings.append(
                    _v(
                        doc,
                        class_='numpydoc.parameter_name_markup',
                        section=section.name,
                        entry=entry.header.text,
                        current_text=entry.header.text,
                        suggested_fix=entry.header.text.replace('`', ''),
                        message='Parameter names in type lines must be bare (no backticks).',
                    )
                )
    return findings


def check_empty_raises(
    doc: PublicDoc,
    lines: Sequence[DocLine],
    sections: Sequence[Section],
) -> list[Violation]:
    findings: list[Violation] = []
    for section in sections:
        if section.name != 'Raises':
            continue
        for entry in entries(section, lines):
            has_description = any(
                line.text.strip() and line.text[:1].isspace()
                for line in entry.description
            )
            if has_description:
                continue
            findings.append(
                _v(
                    doc,
                    class_='numpydoc.empty_raises_description',
                    section='Raises',
                    entry=entry.header.text,
                    current_text=entry.header.text,
                    suggested_fix=(
                        'Add an indented complete sentence explaining exactly '
                        'when this exception is raised.'
                    ),
                    message='Every Raises heading requires an indented description.',
                )
            )
    return findings


def check_missing_blank_before_section(
    doc: PublicDoc,
    lines: Sequence[DocLine],
    missing_blanks: Sequence[tuple[int, str]],
) -> list[Violation]:
    findings: list[Violation] = []
    for index, prev in missing_blanks:
        findings.append(
            _v(
                doc,
                class_='numpydoc.missing_blank_before_section',
                section=lines[index].text,
                entry=None,
                current_text=prev,
                suggested_fix='Insert one blank line before the section header.',
                message='A recognized section header must be preceded by a blank line.',
            )
        )
    return findings


def check_prose(
    doc: PublicDoc,
    lines: Sequence[DocLine],
    sections: Sequence[Section],
) -> list[Violation]:
    findings: list[Violation] = []
    # Build prose regions: everything except Examples body and type columns.
    examples_ranges: list[tuple[int, int]] = []
    for section in sections:
        if section.name == 'Examples':
            examples_ranges.append((section.body_start, section.body_end))  # noqa: PERF401

    def in_examples(line_idx: int) -> bool:
        return any(a <= line_idx < b for a, b in examples_ranges)

    prose_parts: list[str] = []
    for i, line in enumerate(lines):
        if in_examples(i):
            continue
        if line.text in SECTION_NAMES or UNDERLINE_RE.fullmatch(line.text or ''):
            continue
        # skip type-column headers roughly (unindented with colon in param sections)
        prose_parts.append(line.text)
    prose = '\n'.join(prose_parts)
    masked = mask_backticks(prose)

    for match in DOUBLED_WORD_RE.finditer(masked):
        phrase = f'{match.group("word")} {match.group("word")}'.lower()
        key = (doc.symbol, phrase)
        if key in DOUBLED_WORD_ALLOWLIST:
            continue
        findings.append(
            _v(
                doc,
                class_='prose.doubled_word',
                section=None,
                entry=None,
                current_text=match.group(0),
                suggested_fix=match.group('word'),
                message=f'Doubled word {match.group("word")!r}.',
            )
        )

    for i, line in enumerate(lines):
        if in_examples(i):
            continue
        m = TRAILING_DOUBLE_PERIOD_RE.search(line.text.rstrip())
        if m:
            findings.append(
                _v(
                    doc,
                    class_='prose.trailing_double_period',
                    section=None,
                    entry=None,
                    current_text=line.text,
                    suggested_fix=line.text.rstrip()[:-1],
                    message='Trailing double period in prose.',
                )
            )
    return findings


def _parse_see_also_entry(header_text: str) -> dict[str, Any]:
    """Classify a See Also entry line; return fields for checks."""
    text = header_text.rstrip()
    result: dict[str, Any] = {
        'raw': text,
        'valid': False,
        'name': None,
        'description': None,
        'issues': [],
    }
    if MERGED_SEE_ALSO_RE.search(text):
        result['issues'].append('see_also.merged_entries')
    # backticked name
    if text.lstrip().startswith('`') or text.startswith('``'):
        result['issues'].append('see_also.backticked_name')
    # spacing around colon
    if ' : ' not in text and ':' in text:
        result['issues'].append('see_also.spacing')
    m = SEE_ALSO_LINE_RE.match(text)
    if m:
        result['valid'] = True
        result['name'] = m.group('name')
        result['description'] = m.group('description')
        if result['description'].endswith('..'):
            result['issues'].append('see_also.double_period')
            result['valid'] = False
        return result
    # Try recovery for partial classification
    if ':' in text:
        left, right = text.split(':', 1)
        name = left.strip().strip('`')
        desc = right.strip()
        result['name'] = name
        result['description'] = desc
        if not desc:
            result['issues'].append('see_also.missing_description')
        else:
            if desc and not desc[0].isupper():
                result['issues'].append('see_also.description_capitalization')
            if desc and not desc.endswith('.'):
                result['issues'].append('see_also.missing_period')
            if desc.endswith('..'):
                result['issues'].append('see_also.double_period')
        if name.startswith(('gm.', 'gometry.')):
            result['issues'].append('see_also.noncanonical_name')
        if '`' in left and 'see_also.backticked_name' not in result['issues']:
            result['issues'].append('see_also.backticked_name')
    else:
        result['name'] = text.strip().strip('`')
        result['issues'].append('see_also.missing_description')
        if text.strip().startswith('`'):
            result['issues'].append('see_also.backticked_name')
    return result


def check_see_also(
    doc: PublicDoc,
    lines: Sequence[DocLine],
    sections: Sequence[Section],
    public_symbols: frozenset[str],
) -> tuple[list[Violation], list[tuple[str, str]]]:
    """Return findings and list of (logical_target, description) for valid entries."""
    findings: list[Violation] = []
    resolved: list[tuple[str, str]] = []
    for section in sections:
        if section.name != 'See Also':
            continue
        for entry in entries(section, lines):
            # multiline entry?
            if any(d.text.strip() and d.text[:1].isspace() for d in entry.description):
                findings.append(
                    _v(
                        doc,
                        class_='see_also.multiline_entry',
                        section='See Also',
                        entry=entry.header.text,
                        current_text=entry.header.text,
                        suggested_fix='Rewrite as one short physical line.',
                        message='See Also entries must be a single physical line.',
                    )
                )
            info = _parse_see_also_entry(entry.header.text)
            for issue in info['issues']:
                findings.append(  # noqa: PERF401
                    _v(
                        doc,
                        class_=issue,
                        section='See Also',
                        entry=entry.header.text,
                        current_text=entry.header.text,
                        suggested_fix='Normalize to `name : Description.`',
                        message=f'See Also format: {issue}',
                    )
                )
            if info['valid'] and info['name']:
                name = info['name']
                # noncanonical: gm./gometry. prefix
                if name.startswith(('gm.', 'gometry.')):
                    findings.append(
                        _v(
                            doc,
                            class_='see_also.noncanonical_name',
                            section='See Also',
                            entry=entry.header.text,
                            current_text=name,
                            suggested_fix=name.split('.', 1)[-1],
                            message='See Also names must be bare, without gm./gometry. prefix.',
                        )
                    )
                    name = name.split('.', 1)[-1]
                # resolve
                logical = _resolve_see_also_target(name, doc, public_symbols)
                if logical is None:
                    findings.append(
                        _v(
                            doc,
                            class_='see_also.unresolved_target',
                            section='See Also',
                            entry=entry.header.text,
                            current_text=name,
                            suggested_fix='Correct or delete the reference.',
                            message=f'See Also target {name!r} does not resolve.',
                        )
                    )
                else:
                    desc = re.sub(r'\s+', ' ', (info['description'] or '').strip())
                    resolved.append((logical, desc))
    return findings, resolved


def _resolve_see_also_target(
    name: str,
    doc: PublicDoc,
    public_symbols: frozenset[str],
) -> str | None:
    # Strip legacy gm.
    name = name.removeprefix('gm.')
    name = name.removeprefix('gometry.')
    if name in public_symbols:
        return name
    if doc.owner_class:
        q = f'{doc.owner_class}.{name}'
        if q in public_symbols:
            return q
        # sibling class member
        for prefix in ('Geometry.', 'GeometryArray.'):
            q2 = f'{prefix}{name}'
            if q2 in public_symbols:
                return q2
    # Class.member
    if '.' in name and name in public_symbols:
        return name
    # bare top-level
    if name in public_symbols:
        return name
    # try Geometry.X / GeometryArray.X
    for prefix in ('Geometry.', 'GeometryArray.'):
        q = f'{prefix}{name}'
        if q in public_symbols:
            return q
    return None


def check_examples(
    doc: PublicDoc,
    lines: Sequence[DocLine],
    sections: Sequence[Section],
) -> list[Violation]:
    findings: list[Violation] = []
    if not doc.is_operation:
        return findings
    if doc.symbol in EXAMPLES_EXEMPTIONS:
        return findings
    ex_sections = [s for s in sections if s.name == 'Examples']
    if not ex_sections:
        findings.append(
            _v(
                doc,
                class_='examples.missing',
                section='Examples',
                entry=None,
                current_text=None,
                suggested_fix='Add an Examples section with >>> import gometry as gm',
                message='Public operation is missing an Examples section.',
            )
        )
        return findings
    body_lines = lines[ex_sections[0].body_start : ex_sections[0].body_end]
    body = '\n'.join(ln.text for ln in body_lines)
    if not body.strip():
        findings.append(
            _v(
                doc,
                class_='examples.missing',
                section='Examples',
                entry=None,
                current_text=None,
                suggested_fix='Add at least one doctest example.',
                message='Examples section is empty.',
            )
        )
        return findings
    parser = doctest.DocTestParser()
    try:
        examples = parser.get_examples(body)
    except Exception:
        examples = []
    if not examples:
        findings.append(
            _v(
                doc,
                class_='examples.missing',
                section='Examples',
                entry=None,
                current_text=None,
                suggested_fix='Add at least one >>> doctest example.',
                message='Examples section has no doctest examples.',
            )
        )
        return findings

    has_canonical_import = False
    for ex in examples:
        src = ex.source
        try:
            tree = ast.parse(src)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == 'gometry':
                        if alias.asname == 'gm' and len(node.names) == 1:
                            has_canonical_import = True
                        else:
                            findings.append(
                                _v(
                                    doc,
                                    class_='examples.import_style',
                                    section='Examples',
                                    entry=None,
                                    current_text=src.strip(),
                                    suggested_fix='import gometry as gm',
                                    message='gometry imports must be exactly `import gometry as gm`.',
                                )
                            )
            if (
                isinstance(node, ast.ImportFrom)
                and node.module
                and (node.module == 'gometry' or node.module.startswith('gometry.'))
            ):
                findings.append(
                    _v(
                        doc,
                        class_='examples.import_style',
                        section='Examples',
                        entry=None,
                        current_text=src.strip(),
                        suggested_fix='import gometry as gm',
                        message='Use `import gometry as gm`, not from-import.',
                    )
                )
        # space after open paren
        for issue in _space_after_open_paren(src):
            findings.append(  # noqa: PERF401
                _v(
                    doc,
                    class_='examples.space_after_open_paren',
                    section='Examples',
                    entry=None,
                    current_text=issue,
                    suggested_fix='Remove space after opening parenthesis.',
                    message='Doctest has stray space after `(`.',
                )
            )
    if not has_canonical_import:
        findings.append(
            _v(
                doc,
                class_='examples.import_style',
                section='Examples',
                entry=None,
                current_text=None,
                suggested_fix='import gometry as gm',
                message='Examples block must include `import gometry as gm`.',
            )
        )
    return findings


def _space_after_open_paren(source: str) -> list[str]:
    issues: list[str] = []
    try:
        tokens = list(tokenize.generate_tokens(io.StringIO(source).readline))
    except tokenize.TokenError:
        return issues
    for i, tok in enumerate(tokens):
        if tok.type == tokenize.OP and tok.string == '(':
            # find next significant token
            j = i + 1
            while j < len(tokens) and tokens[j].type in (
                tokenize.NL,
                tokenize.NEWLINE,
                tokenize.INDENT,
                tokenize.DEDENT,
                tokenize.COMMENT,
            ):
                j += 1
            if j >= len(tokens):
                continue
            nxt = tokens[j]
            if nxt.type == tokenize.OP and nxt.string == ')':
                continue
            # same physical line?
            if nxt.start[0] != tok.start[0]:
                continue
            # intervening source on the line
            line = source.splitlines()[tok.start[0] - 1] if source.splitlines() else ''
            between = line[tok.end[1] : nxt.start[1]]
            if between and between[0] in ' \t':
                issues.append(line.strip())
    return issues


def check_style(
    doc: PublicDoc,
    lines: Sequence[DocLine],
    sections: Sequence[Section],
) -> list[Violation]:
    findings: list[Violation] = []
    if not doc.text.strip():
        return findings
    summary = summary_paragraph(lines, sections)
    extended = extended_summary(lines, sections)
    first_word = ''
    if summary:
        # strip leading backticks/markup
        cleaned = summary.lstrip('`').strip()
        first_word = (
            cleaned.split()[0].lower().rstrip('.,;:') if cleaned.split() else ''
        )

    # 1. mood heuristic
    if doc.kind in ('function', 'method') and doc.is_operation and summary:  # noqa: SIM102
        if summary.startswith('Whether ') or first_word in CALLABLE_NOUN_OPENERS:  # noqa: SIM102
            # allow "Test whether"
            if not summary.startswith('Test whether') and not summary.startswith(
                'Test '
            ):
                findings.append(
                    _v(
                        doc,
                        class_='style.summary_not_imperative',
                        section=None,
                        entry=None,
                        current_text=summary[:120],
                        suggested_fix='Rewrite summary as an imperative verb phrase.',
                        message='Callable summary should start with an imperative verb or "Test whether".',
                    )
                )
    if doc.kind == 'property' and summary and summary.startswith('Test '):
        findings.append(
            _v(
                doc,
                class_='style.property_imperative',
                section=None,
                entry=None,
                current_text=summary[:120],
                suggested_fix='Use a noun phrase or "Whether ..." for properties.',
                message='Property summary should be a noun phrase or "Whether ...".',
            )
        )

    # 2. unquoted enum tokens in param type fields
    for section in sections:
        if section.name not in NAMED_TYPE_SECTIONS:
            continue
        for entry in entries(section, lines):
            col = type_column(section, entry)
            if col is None:
                continue
            _s, _e, text = col
            for m in UNQUOTED_ENUM_RE.finditer(text):
                body = m.group('body')
                # if any token lacks quotes
                if re.search(r'[A-Za-z_]\w*', body):
                    findings.append(
                        _v(
                            doc,
                            class_='style.unquoted_enum_tokens',
                            section=section.name,
                            entry=entry.header.text,
                            current_text=text,
                            suggested_fix="Quote enum tokens: {'a', 'b'}",
                            message='Enum tokens in type fields must be quoted.',
                        )
                    )
                    break

    # 3. *a* / *b* operand names
    prose = summary + '\n' + extended
    if ASTERISK_AB_RE.search(prose):
        findings.append(
            _v(
                doc,
                class_='style.asterisk_ab_operands',
                section=None,
                entry=None,
                current_text=ASTERISK_AB_RE.search(prose).group(0),  # type: ignore[union-attr]
                suggested_fix='Use ``left`` / ``right`` operand names.',
                message='Operand names *a*/*b* are forbidden; use left/right.',
            )
        )

    # 4. gometry./gm. inside summary/extended
    for region_name, region in (('summary', summary), ('extended', extended)):
        for m in GM_PREFIX_RE.finditer(region):
            findings.append(  # noqa: PERF401
                _v(
                    doc,
                    class_='style.gm_prefix_in_prose',
                    section=None,
                    entry=None,
                    current_text=m.group(0),
                    suggested_fix='Use a backticked bare name.',
                    message=f'{m.group(0)!r} inside {region_name} prose; reserve gm. for See Also.',
                )
            )

    # 5. self cross-reference
    bare = doc.symbol.split('.')[-1]
    self_pat = re.compile(
        rf'\b(?:gometry|gm)\.{re.escape(bare)}\b'
        rf'|\blike\s+``(?:gometry\.)?{re.escape(bare)}``'
    )
    if self_pat.search(summary) or self_pat.search(extended):
        findings.append(
            _v(
                doc,
                class_='style.self_cross_reference',
                section=None,
                entry=None,
                current_text=summary[:120],
                suggested_fix='Remove self-referential gometry.<this> wording.',
                message='Docstring self-references the same symbol via gometry./gm.',
            )
        )

    # 6. banned terms
    full_prose = doc.text
    for term, cls in BANNED_TERM_PATTERNS:
        if term in full_prose:
            findings.append(
                _v(
                    doc,
                    class_=cls,
                    section=None,
                    entry=None,
                    current_text=term,
                    suggested_fix=f'Remove or replace {term!r}.',
                    message=f'Banned term {term!r}.',
                )
            )

    # 7. per element on array types
    if doc.owner_class in ('GeometryArray', 'CellArray') or doc.symbol in (  # noqa: SIM102
        'GeometryArray',
        'CellArray',
    ):
        if PER_ELEMENT_RE.search(full_prose):
            findings.append(
                _v(
                    doc,
                    class_='style.per_element',
                    section=None,
                    entry=None,
                    current_text='per element',
                    suggested_fix='Use "per row".',
                    message='GeometryArray/CellArray prose should say "per row", not "per element".',
                )
            )

    # 8. (planar) on auto-split ops
    bare_name = doc.symbol.split('.')[-1]
    if bare_name in AUTO_SPLIT_OPS and bare_name not in GENUINELY_PLANAR_OPS:  # noqa: SIM102
        if PLANAR_TAG_RE.search(summary) or PLANAR_TAG_RE.search(extended):
            findings.append(
                _v(
                    doc,
                    class_='style.planar_on_autosplit',
                    section=None,
                    entry=None,
                    current_text='(planar)',
                    suggested_fix=(
                        'Remove (planar); document antimeridian split-normalization instead.'
                    ),
                    message='Auto-split topology ops must not claim "(planar)".',
                )
            )

    # 9. Raises description not starting with "If "
    for section in sections:
        if section.name != 'Raises':
            continue
        for entry in entries(section, lines):
            desc_lines = [
                ln.text.strip()
                for ln in entry.description
                if ln.text.strip() and ln.text[:1].isspace()
            ]
            if not desc_lines:
                continue
            first = desc_lines[0]
            if not first.startswith('If '):
                findings.append(
                    _v(
                        doc,
                        class_='style.raises_not_if',
                        section='Raises',
                        entry=entry.header.text,
                        current_text=first,
                        suggested_fix='Start Raises descriptions with "If ".',
                        message='Raises description should start with "If ".',
                    )
                )
    return findings


def check_reciprocity(
    docs: Sequence[PublicDoc],
    see_also_graph: dict[str, list[tuple[str, str]]],
    public_symbols: frozenset[str],
) -> list[Violation]:
    findings: list[Violation] = []
    doc_by_symbol = {d.symbol: d for d in docs}

    def side_members(side: frozenset[str]) -> list[str]:
        members: list[str] = []
        for s in sorted(side):
            if s in public_symbols:
                members.append(s)
            else:
                # expand bare over owners already done; stale policy is fatal
                findings.append(
                    Violation(
                        file='tools/gates/_check_docstyle.py',
                        line=None,
                        column=None,
                        owner_file=None,
                        owner_line=None,
                        source_kind='config',
                        symbol=s,
                        class_='inventory.stale_reciprocity_policy',
                        section=None,
                        entry=None,
                        current_text=s,
                        suggested_fix='Update RECIPROCAL_FAMILIES to current public symbols.',
                        message=f'ReciprocalFamily policy symbol {s!r} is not public.',
                    )
                )
        return members

    for fam in RECIPROCAL_FAMILIES:
        left = side_members(fam.left)
        right = side_members(fam.right)
        # complete group: left only, right empty — every member must link some other
        if not right and left:
            members = left
            for src in members:
                targets = {t for t, _ in see_also_graph.get(src, [])}
                if not any(m in targets for m in members if m != src):
                    doc = doc_by_symbol.get(src)
                    if doc is None:
                        continue
                    findings.append(
                        _v(
                            doc,
                            class_='see_also.missing_reciprocal',
                            section='See Also',
                            entry=None,
                            current_text=None,
                            suggested_fix=f'Add a See Also link within family {fam.key}.',
                            message=f'Missing reciprocal link in complete group {fam.key}.',
                            related_symbol=fam.key,
                        )
                    )
            continue
        for src in left:
            targets = {t for t, _ in see_also_graph.get(src, [])}
            if not any(r in targets for r in right):
                doc = doc_by_symbol.get(src)
                if doc is None:
                    continue
                findings.append(
                    _v(
                        doc,
                        class_='see_also.missing_reciprocal',
                        section='See Also',
                        entry=None,
                        current_text=None,
                        suggested_fix=f'Link to one of: {", ".join(sorted(right))}.',
                        message=f'Missing reciprocal See Also toward {fam.key} right side.',
                        related_symbol=min(right) if right else fam.key,
                    )
                )
        for src in right:
            targets = {t for t, _ in see_also_graph.get(src, [])}
            if not any(l in targets for l in left):
                doc = doc_by_symbol.get(src)
                if doc is None:
                    continue
                findings.append(
                    _v(
                        doc,
                        class_='see_also.missing_reciprocal',
                        section='See Also',
                        entry=None,
                        current_text=None,
                        suggested_fix=f'Link to one of: {", ".join(sorted(left))}.',
                        message=f'Missing reciprocal See Also toward {fam.key} left side.',
                        related_symbol=min(left) if left else fam.key,
                    )
                )
    return findings


def _raises_headings(doc: PublicDoc) -> set[str]:
    lines = doc_lines(doc.text)
    sections, _ = segment_sections(lines)
    heads: set[str] = set()
    for section in sections:
        if section.name != 'Raises':
            continue
        for entry in entries(section, lines):
            heads.add(entry.header.text.strip())
    return heads


def _logical_see_also(doc: PublicDoc, public_symbols: frozenset[str]) -> dict[str, str]:
    lines = doc_lines(doc.text)
    sections, _ = segment_sections(lines)
    out: dict[str, str] = {}
    for section in sections:
        if section.name != 'See Also':
            continue
        for entry in entries(section, lines):
            info = _parse_see_also_entry(entry.header.text)
            if not info['valid'] or not info['name']:
                continue
            name = info['name']
            name = name.removeprefix('gm.')
            logical = _resolve_see_also_target(name, doc, public_symbols)
            if logical is None:
                continue
            # collapse GeometryArray.X and bare X for mirror targets
            bare = logical.split('.')[-1]
            key = bare if bare in MIRROR_PAIRS else logical
            desc = re.sub(r'\s+', ' ', (info['description'] or '').strip())
            out[key] = desc
    return out


def _unwrap_geometry_array_receiver(node: ast.AST) -> ast.AST:
    """gm.GeometryArray(expr) / GeometryArray(expr) → expr when sole constructor wrap."""
    if not isinstance(node, ast.Call):
        return node
    func = node.func
    is_ga = (isinstance(func, ast.Name) and func.id == 'GeometryArray') or (
        isinstance(func, ast.Attribute) and func.attr == 'GeometryArray'
    )
    if is_ga and len(node.args) == 1 and not node.keywords:
        return node.args[0]
    return node


def _dump_expr(node: ast.AST, env: dict[str, str]) -> str:
    """AST-dump an expression, resolving names and unwrapping GeometryArray."""
    node = _unwrap_geometry_array_receiver(node)
    if isinstance(node, ast.Name) and node.id in env:
        return env[node.id]
    node = _unwrap_geometry_array_receiver(node)
    return ast.dump(node, include_attributes=False)


def _normalize_mirror_examples(doc: PublicDoc, name: str) -> collections.Counter:
    """Normalize free ``gm.NAME(recv, …)`` and method ``recv.NAME(…)`` to one key.

    Spec §4.6: both become
    ``(NAME, normalized_receiver, normalized_args, normalized_kwargs, expected)``.
    """
    lines = doc_lines(doc.text)
    sections, _ = segment_sections(lines)
    ex = [s for s in sections if s.name == 'Examples']
    if not ex:
        return collections.Counter()
    body = '\n'.join(ln.text for ln in lines[ex[0].body_start : ex[0].body_end])
    parser = doctest.DocTestParser()
    try:
        examples = parser.get_examples(body)
    except Exception:
        return collections.Counter()
    env: dict[str, str] = {}
    counter: collections.Counter = collections.Counter()
    for ex_item in examples:
        src = ex_item.source.strip()
        if src.startswith('import gometry'):
            continue
        try:
            tree = ast.parse(src)
        except SyntaxError:
            continue
        # simple single-name assignments for later receiver resolution.
        # Unwrap GeometryArray(...) so free list receivers and method
        # GeometryArray receivers compare equal after normalization.
        for node in tree.body:
            if isinstance(node, ast.Assign) and len(node.targets) == 1:
                t = node.targets[0]
                if isinstance(t, ast.Name):
                    val = _unwrap_geometry_array_receiver(node.value)
                    env[t.id] = ast.dump(val, include_attributes=False)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not isinstance(func, ast.Attribute) or func.attr != name:
                continue

            # Free: gm.NAME(receiver, *args, **kwargs)
            # Method: receiver.NAME(*args, **kwargs)
            # Order matters: gm.NAME is also an Attribute with attr==NAME, so
            # detect the free form first (func.value is Name 'gm').
            if isinstance(func.value, ast.Name) and func.value.id == 'gm':
                if not node.args:
                    continue
                receiver_node = _unwrap_geometry_array_receiver(node.args[0])
                arg_nodes = list(node.args[1:])
            else:
                receiver_node = _unwrap_geometry_array_receiver(func.value)
                arg_nodes = list(node.args)

            receiver_dump = _dump_expr(receiver_node, env)
            args_dump = tuple(_dump_expr(a, env) for a in arg_nodes)
            kwargs = sorted(
                ((kw.arg, _dump_expr(kw.value, env)) for kw in node.keywords if kw.arg),
                key=lambda x: x[0] or '',
            )
            expected = (ex_item.want or '').strip()
            key = (name, receiver_dump, args_dump, tuple(kwargs), expected)
            counter[key] += 1
    return counter


def check_mirror_parity(
    docs: Sequence[PublicDoc],
    public_symbols: frozenset[str],
) -> list[Violation]:
    findings: list[Violation] = []
    by_symbol = {d.symbol: d for d in docs}
    # Discover overlaps: top-level function ∩ GeometryArray method
    top_level = {d.symbol for d in docs if d.kind == 'function' and '.' not in d.symbol}
    ga_methods = {
        d.symbol.split('.', 1)[1]
        for d in docs
        if d.symbol.startswith('GeometryArray.')
        and d.kind in ('method', 'property')
        and d.is_operation
    }
    overlaps = sorted(top_level & ga_methods)
    for name in overlaps:
        if name in SAME_NAME_NON_MIRRORS:
            continue
        if name not in MIRROR_PAIRS:
            findings.append(
                Violation(
                    file='tools/gates/_check_docstyle.py',
                    line=None,
                    column=None,
                    owner_file=None,
                    owner_line=None,
                    source_kind='config',
                    symbol=name,
                    class_='inventory.unclassified_mirror_overlap',
                    section=None,
                    entry=None,
                    current_text=name,
                    suggested_fix='Add to MIRROR_PAIRS or SAME_NAME_NON_MIRRORS.',
                    message=f'Unclassified free/GeometryArray overlap: {name}',
                )
            )
            continue
        free = by_symbol.get(name)
        method = by_symbol.get(f'GeometryArray.{name}')
        if free is None or method is None:
            continue
        # Raises parity
        free_raises = _raises_headings(free)
        method_raises = _raises_headings(method)
        expected_method = free_raises - {'CRSMismatchError'}
        if method_raises != expected_method:
            findings.append(
                _v(
                    method,
                    class_='mirror.raises_parity',
                    section='Raises',
                    entry=None,
                    current_text=str(sorted(method_raises)),
                    suggested_fix=(
                        f'method Raises should equal free Raises - {{CRSMismatchError}}; '
                        f'expected {sorted(expected_method)}'
                    ),
                    message='Mirror Raises heading sets diverge.',
                    related_symbol=name,
                )
            )
            findings.append(
                _v(
                    free,
                    class_='mirror.raises_parity',
                    section='Raises',
                    entry=None,
                    current_text=str(sorted(free_raises)),
                    suggested_fix='Align free-function Raises with GeometryArray method (+ CRSMismatchError only).',
                    message='Mirror Raises heading sets diverge.',
                    related_symbol=f'GeometryArray.{name}',
                )
            )
        # See Also parity
        free_sa = _logical_see_also(free, public_symbols)
        method_sa = _logical_see_also(method, public_symbols)
        if set(free_sa) != set(method_sa):
            findings.append(
                _v(
                    free,
                    class_='mirror.see_also_parity',
                    section='See Also',
                    entry=None,
                    current_text=str(sorted(free_sa)),
                    suggested_fix=f'Align See Also targets with GeometryArray.{name}.',
                    message='Mirror See Also logical target sets diverge.',
                    related_symbol=f'GeometryArray.{name}',
                )
            )
        for target in sorted(set(free_sa) & set(method_sa)):
            if free_sa[target] != method_sa[target]:
                findings.append(  # noqa: PERF401
                    _v(
                        free,
                        class_='mirror.see_also_parity',
                        section='See Also',
                        entry=target,
                        current_text=free_sa[target],
                        suggested_fix=method_sa[target],
                        message=f'See Also description for {target} differs across mirror.',
                        related_symbol=f'GeometryArray.{name}',
                    )
                )
        # Examples parity
        free_ex = _normalize_mirror_examples(free, name)
        method_ex = _normalize_mirror_examples(method, name)
        if free_ex != method_ex:
            findings.append(
                _v(
                    free,
                    class_='mirror.examples_parity',
                    section='Examples',
                    entry=None,
                    current_text=None,
                    suggested_fix=f'Align Examples with GeometryArray.{name} (AST-normalized).',
                    message='Mirror Examples counters diverge.',
                    related_symbol=f'GeometryArray.{name}',
                )
            )
    return findings


def scan_doc(
    doc: PublicDoc,
    public_symbols: frozenset[str],
) -> tuple[list[Violation], list[tuple[str, str]]]:
    findings: list[Violation] = []
    if not doc.text.strip():
        return findings, []
    lines = doc_lines(doc.text)
    sections, missing_blanks = segment_sections(lines)
    findings.extend(check_type_columns(doc, lines, sections))
    findings.extend(check_empty_raises(doc, lines, sections))
    findings.extend(check_missing_blank_before_section(doc, lines, missing_blanks))
    findings.extend(check_prose(doc, lines, sections))
    sa_findings, resolved = check_see_also(doc, lines, sections, public_symbols)
    findings.extend(sa_findings)
    findings.extend(check_examples(doc, lines, sections))
    findings.extend(check_style(doc, lines, sections))
    return findings, resolved


def collect_report(root: Path = ROOT) -> dict[str, object]:
    global ROOT, STUB_PATH, INIT_PATH, SRC_ROOT, _RUST_INDEX
    ROOT = root
    STUB_PATH = root / 'python' / 'gometry' / '_lib.pyi'
    INIT_PATH = root / 'python' / 'gometry' / '__init__.py'
    SRC_ROOT = root / 'src'
    _RUST_INDEX = None

    violations: list[Violation] = []
    errors: list[ScanError] = []

    if not STUB_PATH.is_file():
        errors.append(ScanError(message=f'stub missing: {STUB_PATH}'))
        return _finalize(violations, errors, 0, 0)

    if not INIT_PATH.is_file():
        errors.append(ScanError(message=f'__init__.py missing: {INIT_PATH}'))
        return _finalize(violations, errors, 0, 0)

    try:
        docs, inv_errors, public_symbols = load_inventory(STUB_PATH, INIT_PATH)
    except Exception as exc:
        errors.append(ScanError(message=f'inventory failed: {exc}'))
        return _finalize(violations, errors, 0, 0)

    errors.extend(inv_errors)

    see_also_graph: dict[str, list[tuple[str, str]]] = {}
    scanned = 0
    for doc in docs:
        scanned += 1
        try:
            findings, resolved = scan_doc(doc, public_symbols)
        except Exception as exc:
            errors.append(
                ScanError(
                    message=f'failed to scan {doc.symbol}: {exc}',
                    symbol=doc.symbol,
                )
            )
            continue
        violations.extend(findings)
        see_also_graph[doc.symbol] = resolved

    violations.extend(check_reciprocity(docs, see_also_graph, public_symbols))
    violations.extend(check_mirror_parity(docs, public_symbols))

    return _finalize(violations, errors, len(public_symbols), scanned)


def _sort_key(v: Violation) -> tuple:
    return (
        v.file or '',
        v.line if v.line is not None else -1,
        v.column if v.column is not None else -1,
        v.symbol or '',
        v.class_ or '',
        v.section or '',
        v.entry or '',
        v.related_symbol or '',
    )


def _finalize(
    violations: list[Violation],
    errors: list[ScanError],
    public_symbols: int,
    scanned: int,
) -> dict[str, object]:
    violations_sorted = sorted(violations, key=_sort_key)
    by_class: dict[str, int] = collections.Counter(v.class_ for v in violations_sorted)
    symbols_with = len({v.symbol for v in violations_sorted})
    complete = len(errors) == 0
    status = 'pass'
    if errors:
        status = 'incomplete'
    elif violations_sorted:
        status = 'fail'
    return {
        'schema_version': 1,
        'tool': 'gometry-docstyle',
        'status': status,
        'complete': complete,
        'summary': {
            'public_symbols': public_symbols,
            'docstrings_scanned': scanned,
            'symbols_with_violations': symbols_with,
            'violations': len(violations_sorted),
            'errors': len(errors),
            'by_class': dict(sorted(by_class.items())),
        },
        'violations': [v.to_json() for v in violations_sorted],
        'errors': [
            e.to_json()
            for e in sorted(errors, key=lambda e: (e.symbol or '', e.message))
        ],
    }


def format_human(report: dict[str, object]) -> tuple[str, str]:
    """Return (stderr_text, stdout_total_line)."""
    summary = report['summary']  # type: ignore[index]
    by_class = summary['by_class']  # type: ignore[index]
    lines: list[str] = []
    lines.append(
        f'docstyle: checked {summary["docstrings_scanned"]} public docstrings; '
        f'{summary["violations"]} violations in {summary["symbols_with_violations"]} symbols'
    )
    if not report['complete']:
        lines.append(f'INCOMPLETE: {summary["errors"]} errors')
        for err in report['errors']:  # type: ignore[union-attr]
            lines.append(f'  ERROR: {err.get("symbol") or ""}: {err["message"]}')  # noqa: PERF401
    for cls, count in by_class.items():
        lines.append(f'  {cls}  {count}')
    lines.append('')
    for v in report['violations']:  # type: ignore[union-attr]
        loc = v.get('file') or 'unknown'
        if v.get('line') is not None:
            loc = f'{loc}:{v["line"]}'
            if v.get('column') is not None:
                loc = f'{loc}:{v["column"]}'
        lines.append(f'{loc}: [{v["class"]}] {v["symbol"]}')
        if v.get('section'):
            lines.append(f'  section: {v["section"]}')
        if v.get('current_text') is not None:
            ct = v['current_text']
            if isinstance(ct, str) and len(ct) > 100:
                ct = ct[:100] + '...'
            lines.append(f'  current: {ct}')
        lines.append(f'  fix: {v["suggested_fix"]}')
        lines.append('')
    stderr = '\n'.join(lines).rstrip() + '\n'
    total = f'TOTAL DOCSTYLE VIOLATIONS: {summary["violations"]}'
    return stderr, total


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='gometry docstring style gate')
    parser.add_argument(
        '--format',
        choices=('human', 'json'),
        default='human',
    )
    parser.add_argument(
        '--root',
        type=Path,
        default=None,
        help='Repository root (default: auto-detect from this file).',
    )
    args = parser.parse_args(list(argv) if argv is not None else None)
    root = args.root if args.root is not None else ROOT
    report = collect_report(root)
    if args.format == 'json':
        text = json.dumps(report, indent=2, sort_keys=True) + '\n'
        sys.stdout.write(text)
    else:
        stderr, total = format_human(report)
        sys.stderr.write(stderr)
        sys.stdout.write(total + '\n')
    if not report['complete']:
        return 2
    if report['violations']:
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
