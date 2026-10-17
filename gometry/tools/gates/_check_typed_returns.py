"""Typed-return gate for the PyO3 boundary.

Every ``#[pymethods]`` / ``#[pyfunction]`` that returns a geometry to Python
must use ``Typed`` (AGENTS.md), never bare ``PyGeometry`` or
``PyResult<PyGeometry>`` — the latter silently yields the base ``Geometry``
class at runtime with no compile error.

Scans ``src/**/*.rs`` with the shared Rust grammar, including declarations
recovered from macro invocations and declarations compiled only under tests.

Usage::

    .venv/bin/python tools/gates/_check_typed_returns.py

Exit 1 on any violation. The final line is ``TOTAL VIOLATIONS: N``.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

_TOOLS_ROOT = Path(__file__).resolve().parents[1]
if str(_TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TOOLS_ROOT))
from _gatelib import (
    prepend_tools_import_paths,
    rust_source_config,
)

prepend_tools_import_paths()

from pyo3stubs.rust_scan import iter_sources

SRC = Path(__file__).resolve().parent.parent.parent / 'src'

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pyo3stubs.rust_scan import Item
    from tree_sitter import Node

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


def _path_leaf(node: Node | None) -> str | None:
    if node is None:
        return None
    if node.type in {'identifier', 'type_identifier'}:
        return node.text.decode() if node.text is not None else ''
    if node.type == 'generic_type':
        return _path_leaf(node.child_by_field_name('type'))
    children = node.named_children
    return _path_leaf(children[-1]) if children else None


def _generic_inner(node: Node, wrapper: str) -> Node | None:
    if node.type != 'generic_type':
        return None
    base = node.child_by_field_name('type')
    if _path_leaf(base) != wrapper:
        return None
    arguments = node.child_by_field_name('type_arguments')
    if arguments is None:
        return None
    types = [child for child in arguments.named_children if child.type != 'lifetime']
    return types[0] if len(types) == 1 else None


def _is_untyped_geometry_return(return_type: Node | None) -> bool:
    current = return_type
    while current is not None:
        inner = _generic_inner(current, 'PyResult')
        if inner is None:
            inner = _generic_inner(current, 'Option')
        if inner is None:
            break
        current = inner
    return _path_leaf(current) == 'PyGeometry'


def _functions(
    items: tuple[Item, ...], in_pymethods: bool = False
) -> Iterator[tuple[Item, str]]:
    for item in items:
        if item.kind == 'function_item':
            if item.attr('pyfunction') is not None:
                yield item, '#[pyfunction]'
            elif in_pymethods:
                yield item, '#[pymethods]'

        if item.kind == 'impl_item':
            child_context = in_pymethods or item.attr('pymethods') is not None
        elif item.kind == 'macro_invocation':
            # This wrapper emits the attribute outside its recovered impl body.
            child_context = in_pymethods or item.name == 'frozen_pymethods'
        else:
            child_context = False
        yield from _functions(item.children, child_context)


def _is_allowlisted(rel_path: str, name: str) -> bool:
    suffix = Path(rel_path).name
    return (rel_path, name) in ALLOWLIST or (suffix, name) in ALLOWLIST


def collect_violations(src_root: Path = SRC) -> list[Violation]:
    violations: list[Violation] = []
    cfg = rust_source_config(src_root)
    for source in iter_sources(cfg, scope='all'):
        for item, surface in _functions(source.items):
            if not _is_untyped_geometry_return(item.type_node):
                continue
            if _is_allowlisted(source.rel, item.name):
                continue
            violations.append(
                Violation(
                    source.rel,
                    item.line,
                    item.name,
                    item.ty,
                    surface,
                )
            )
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
