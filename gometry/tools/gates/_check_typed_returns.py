"""Typed-return gate for the PyO3 boundary.

Every ``#[pymethods]`` / ``#[pyfunction]`` that returns a geometry to Python
must use ``Typed`` (AGENTS.md), never bare ``PyGeometry`` or
``PyResult<PyGeometry>`` — the latter silently yields the base ``Geometry``
class at runtime with no compile error.

Scans ``src/**/*.rs`` with a lightweight Rust item parser (attribute-aware,
brace-balanced — not a line-regex sweep).

Usage::

    .venv/bin/python tools/gates/_check_typed_returns.py

Exit 1 on any violation. The final line is ``TOTAL VIOLATIONS: N``.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path

_TOOLS_ROOT = Path(__file__).resolve().parents[1]
if str(_TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TOOLS_ROOT))
from _gatelib import prepend_tools_import_paths

prepend_tools_import_paths()

from _gatelib import iter_rust_sources
from _gatelib import strip_rust_comments as _strip_rust_comments

SRC = Path(__file__).resolve().parent.parent.parent / 'src'

PYFUNCTION_ATTR = re.compile(r'#\[\s*(?:pyo3::)?pyfunction\b[^\]]*\]')
PYMETHODS_ATTR = re.compile(r'#\[\s*(?:pyo3::)?pymethods\s*\]')

# Internal Rust helpers that legitimately materialize a raw ``PyGeometry`` and
# are wrapped in ``Typed`` at the Python boundary. Listed so a future
# accidental ``#[pyfunction]``/``#[pymethods]`` on them fails loudly with a
# pointer here, and so broad scans stay documented.
ALLOWLIST: dict[tuple[str, str], str] = {
    # geometry_io parse pipeline (wrapped by ``from_wkt``/``from_wkb``/…)
    ('geometry_io.rs', 'parse_wkb_value'): 'internal WKB parse; pyfunctions wrap Typed',
    ('geometry_io.rs', 'parse_geojson_geometry_input'): (
        'internal GeoJSON parse; pyfunctions wrap Typed'
    ),
    # array storage materializer (``__getitem__``/iteration wrap Typed)
    (
        'array_methods.rs',
        'geometry_at',
    ): 'internal row materializer; pymethods wrap Typed',
    # cell kernels (``cell_methods!`` center/boundary wrap Typed)
    ('py/cells/coverage_ops.rs', 'rect_cell_polygon'): 'cell boundary kernel',
    ('py/cells/h3/functions.rs', 'h3_boundary_geometry'): 'cell boundary kernel',
    ('py/cells/s2/cell.rs', 's2_boundary_geometry'): 'cell boundary kernel',
    ('py/cells/mod.rs', 'lonlat_point_geometry'): 'shared lon/lat point builder',
    # shared codecs
    ('broadcast/io.rs', 'parse_wkb_geometry'): 'internal WKB parse helper',
}


@dataclass(frozen=True, slots=True)
class Violation:
    path: str
    line: int
    name: str
    return_type: str
    surface: str

    def format(self) -> str:
        return (
            f'{self.path}:{self.line}: {self.name} ({self.surface}) returns '
            f'{self.return_type!r} — use Typed (AGENTS.md typed-hierarchy seam)'
        )


def _line_number(text: str, index: int) -> int:
    return text.count('\n', 0, index) + 1


def _skip_ws_and_attrs(text: str, index: int) -> int:
    length = len(text)
    while index < length:
        while index < length and text[index].isspace():
            index += 1
        if index + 1 < length and text[index] == '#' and text[index + 1] == '[':
            depth = 0
            index += 2
            while index < length:
                if text[index] == '[':
                    depth += 1
                elif text[index] == ']':
                    if depth == 0:
                        index += 1
                        break
                    depth -= 1
                index += 1
            continue
        break
    return index


def _matching_brace(text: str, open_index: int) -> int:
    """Index after the closing ``}`` matching ``text[open_index] == '{'``."""
    depth = 0
    index = open_index
    length = len(text)
    while index < length:
        ch = text[index]
        if ch in '"\'':
            quote = ch
            index += 1
            while index < length:
                if text[index] == '\\':
                    index += 2
                    continue
                if text[index] == quote:
                    index += 1
                    break
                index += 1
            continue
        if ch == '{':
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0:
                return index + 1
        index += 1
    return length


def _generic_inner(type_text: str, prefix: str) -> str | None:
    """Inner type of ``Prefix<Inner>`` when ``type_text`` is exactly that."""
    type_text = type_text.strip()
    if not type_text.startswith(prefix) or not type_text.endswith('>'):
        return None
    start = len(prefix)
    if start >= len(type_text) or type_text[start] != '<':
        return None
    depth = 0
    for position in range(start, len(type_text)):
        ch = type_text[position]
        if ch == '<':
            depth += 1
        elif ch == '>':
            depth -= 1
            if depth == 0:
                return type_text[start + 1 : position].strip()
    return None


def _unwrap_geometry_return_type(return_type: str) -> str:
    """Peel ``PyResult`` / ``Option`` wrappers down to the leaf type."""
    current = return_type.strip()
    while True:
        peeled: str | None = None
        for prefix in ('PyResult', 'Option'):
            inner = _generic_inner(current, prefix)
            if inner is not None:
                peeled = inner
                break
        if peeled is None:
            return current
        current = peeled


def _is_untyped_geometry_return(return_type: str) -> bool:
    leaf = _unwrap_geometry_return_type(return_type)
    return leaf == 'PyGeometry'


def _fn_keyword_at(text: str, index: int) -> int | None:
    """Start of a ``fn`` item keyword at ``index``, or ``None``."""
    for keyword in (
        'pub fn',
        'pub(crate) fn',
        'pub(super) fn',
        'async fn',
        'unsafe fn',
        'fn',
    ):
        if text.startswith(keyword, index):
            before = text[index - 1] if index else ' '
            after = text[index + len(keyword)]
            if before.isalnum() or before == '_':
                continue
            if after.isalnum() or after == '_':
                continue
            return index
    return None


def _parse_fn_signature(text: str, fn_start: int) -> tuple[str, str | None, int]:
    """Return ``(name, return_type, body_open_brace_index)`` for one ``fn`` item."""
    header_match = re.match(
        r'(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?(?:unsafe\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)',
        text[fn_start:],
    )
    if header_match is None:
        return '', None, fn_start
    name = header_match.group(1)
    index = fn_start + header_match.end()
    length = len(text)
    while index < length and text[index].isspace():
        index += 1
    if index >= length or text[index] != '(':
        return '', None, index

    paren_depth = 0
    angle_depth = 0
    while index < length:
        ch = text[index]
        if ch == '(':
            paren_depth += 1
        elif ch == ')':
            paren_depth -= 1
            if paren_depth == 0:
                index += 1
                break
        elif ch == '<' and paren_depth > 0:
            angle_depth += 1
        elif ch == '>' and angle_depth > 0:
            angle_depth -= 1
        index += 1

    return_type: str | None = None
    while index < length and text[index].isspace():
        index += 1
    if text.startswith('->', index):
        index += 2
        ret_start = index
        angle_depth = 0
        while index < length:
            ch = text[index]
            if ch == '<':
                angle_depth += 1
            elif ch == '>':
                angle_depth -= 1
            elif angle_depth == 0 and (
                ch == '{' or text[index : index + 6] == 'where '
            ):
                break
            index += 1
        return_type = text[ret_start:index].strip()

    while index < length and text[index].isspace():
        index += 1
    if index < length and text[index] == '{':
        return name, return_type, index
    return name, return_type, index


def _iter_impl_functions(
    body: str, body_offset: int, raw_text: str
) -> list[tuple[str, str | None, int]]:
    """Top-level ``fn`` items inside an ``impl`` block body (between outer ``{``…``}``)."""
    items: list[tuple[str, str | None, int]] = []
    index = 0
    depth = 0
    length = len(body)
    while index < length:
        ch = body[index]
        if ch == '{':
            depth += 1
            index += 1
            continue
        if ch == '}':
            depth -= 1
            index += 1
            continue
        if depth == 0:
            fn_at = _fn_keyword_at(body, index)
            if fn_at is not None:
                name, return_type, brace = _parse_fn_signature(body, fn_at)
                if name and brace < length and body[brace] == '{':
                    line = _line_number(raw_text, body_offset + fn_at)
                    items.append((name, return_type, line))
                    index = _matching_brace(body, brace)
                    continue
        index += 1
    return items


def _is_allowlisted(rel_path: str, name: str) -> bool:
    suffix = Path(rel_path).name
    return (rel_path, name) in ALLOWLIST or (suffix, name) in ALLOWLIST


def _scan_pymethods(text: str, raw_text: str, rel_path: str) -> list[Violation]:
    violations: list[Violation] = []
    for match in PYMETHODS_ATTR.finditer(text):
        index = _skip_ws_and_attrs(text, match.end())
        if not text.startswith('impl', index):
            continue
        brace = text.find('{', index)
        if brace < 0:
            continue
        body_end = _matching_brace(text, brace)
        body = text[brace + 1 : body_end - 1]
        for name, return_type, line in _iter_impl_functions(body, brace + 1, raw_text):
            if return_type is None or not _is_untyped_geometry_return(return_type):
                continue
            if _is_allowlisted(rel_path, name):
                continue
            violations.append(
                Violation(rel_path, line, name, return_type, '#[pymethods]')
            )
    return violations


def _scan_pyfunctions(text: str, raw_text: str, rel_path: str) -> list[Violation]:
    violations: list[Violation] = []
    for match in PYFUNCTION_ATTR.finditer(text):
        index = _skip_ws_and_attrs(text, match.end())
        fn_at = _fn_keyword_at(text, index)
        if fn_at is None:
            continue
        name, return_type, _brace = _parse_fn_signature(text, fn_at)
        if return_type is None or not _is_untyped_geometry_return(return_type):
            continue
        if _is_allowlisted(rel_path, name):
            continue
        violations.append(
            Violation(
                rel_path,
                _line_number(raw_text, fn_at),
                name,
                return_type,
                '#[pyfunction]',
            )
        )
    return violations


def collect_violations() -> list[Violation]:
    violations: list[Violation] = []
    for path in iter_rust_sources(SRC.parent):
        rel_path = path.relative_to(SRC).as_posix()
        raw_text = path.read_text()
        text = _strip_rust_comments(raw_text)
        violations.extend(_scan_pymethods(text, raw_text, rel_path))
        violations.extend(_scan_pyfunctions(text, raw_text, rel_path))
    violations.sort(key=lambda item: (item.path, item.line, item.name))
    return violations


def main() -> int:
    violations = collect_violations()
    for violation in violations:
        print(violation.format())
    print(f'TOTAL VIOLATIONS: {len(violations)}')
    return 1 if violations else 0


if __name__ == '__main__':
    sys.exit(main())
