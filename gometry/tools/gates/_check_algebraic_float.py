"""Algebraic-float placement gate for approved measurement owners.

Nightly ``algebraic_{add,sub,mul}`` reassociate float ops (vectorize reductions)
but CHANGE results. Policy: SAFE only in MEASUREMENT/aggregation (area/centroid
magnitude); FORBIDDEN in anything feeding a topology/orientation DECISION.
This gate scans Rust syntax trees, including test bodies and recovered macro
items, for ``.algebraic_`` uses outside the measurement kernels that own
them, and forbids raw signed-area branching in topology modules.

Usage::

    .venv/bin/python tools/gates/_check_algebraic_float.py

Exit 1 on any violation. The final line is ``TOTAL VIOLATIONS: N``.
"""

from __future__ import annotations

import sys
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

import operator

from pyo3stubs.rust_scan import RustSource, iter_sources

if TYPE_CHECKING:
    from collections.abc import Iterator

    from tree_sitter import Node

ROOT = Path(__file__).resolve().parents[2]
_DECISION_NAMES = ('area', 'shoelace', 'reduce_sum', 'column_sum')
MEASUREMENT_OWNERS = frozenset({
    ('src/geometry/access/reduce.rs', 'centroid_ring_sums'),
    ('src/geometry/access/reduce.rs', 'column_sum'),
    ('src/geometry/area/shoelace.rs', 'shoelace_shifted'),
    ('src/geometry/area/tests/reference/simd.rs', 'shoelace_shifted'),
    ('src/geometry/derived/centroid/line.rs', 'lineal_centroid_column_sums'),
    ('src/geometry/derived/centroid/line.rs', 'lineal_centroid_scalar_algebraic'),
    ('src/geometry/derived/centroid/line.rs', 'lineal_centroid_segment_contrib'),
})
MEASUREMENT_FILES = frozenset({'src/geometry/area.rs'})
DECISION_MODULES = frozenset({
    'predicates',
    'constructive',
    'overlay',
    'topology',
    'relate',
    'tessellation',
    'clean_union',
    'antimeridian',
    'arrangement',
})


def in_decision_module(rel: str) -> bool:
    parts = rel.split('/')
    return any(part in DECISION_MODULES for part in parts)


def _text(node: Node | None) -> str:
    return '' if node is None or node.text is None else node.text.decode()


def _last_name(node: Node | None) -> str:
    if node is None:
        return ''
    if node.type in {'identifier', 'field_identifier', 'type_identifier'}:
        return _text(node)
    for child in reversed(node.named_children):
        name = _last_name(child)
        if name:
            return name
    return ''


def _walk(
    node: Node,
    enclosing_fn: str | None = None,
    scope: tuple[str, ...] = (),
) -> Iterator[tuple[Node, str | None]]:
    if node.type in {'mod_item', 'trait_item'}:
        scope = (*scope, _text(node.child_by_field_name('name')))
        enclosing_fn = None
    elif node.type == 'impl_item':
        target = _text(node.child_by_field_name('type'))
        trait = _text(node.child_by_field_name('trait'))
        scope = (*scope, f'impl {trait} for {target}' if trait else f'impl {target}')
        enclosing_fn = None
    if node.type == 'function_item':
        name = _last_name(node.child_by_field_name('name'))
        if name:
            scope = (*scope, name)
            enclosing_fn = '::'.join(scope)
    yield node, enclosing_fn
    for child in node.children:
        yield from _walk(child, enclosing_fn, scope)


def _source_roots(source: RustSource) -> Iterator[Node]:
    yield source.tree.root_node
    yield from (tree.root_node for tree in source.macro_trees)


def _algebraic_member(node: Node) -> bool:
    if node.type != 'field_expression':
        return False
    field = node.child_by_field_name('field')
    return _text(field).startswith('algebraic_')


def _decision_name(node: Node) -> bool:
    return any(
        current.type in {'identifier', 'field_identifier'}
        and _text(current).startswith(_DECISION_NAMES)
        for current, _ in _walk(node)
    )


def _zero_float(node: Node | None) -> bool:
    return (
        node is not None
        and node.type == 'float_literal'
        and _text(node).replace('_', '') == '0.0'
    )


def _is_signed_area_function(node: Node) -> bool:
    if node.type != 'function_item':
        return False
    name = _last_name(node.child_by_field_name('name'))
    return (
        (name.startswith('signed_') and name.endswith('area'))
        or name == 'cycle_area2'
        or name.endswith('_area2')
    )


def _shoelace_measure_call(node: Node) -> bool:
    if node.type != 'call_expression':
        return False
    return (
        _last_name(node.child_by_field_name('function')) == 'shoelace_measure_columns'
    )


def _decision_float_branch(node: Node) -> bool:
    if node.type == 'binary_expression':
        operator = _text(node.child_by_field_name('operator'))
        if operator in {'==', '!=', '>=', '<=', '>', '<'}:
            left = node.child_by_field_name('left')
            right = node.child_by_field_name('right')
            return (
                _zero_float(left) and right is not None and _decision_name(right)
            ) or (_zero_float(right) and left is not None and _decision_name(left))
    if node.type == 'call_expression':
        function = node.child_by_field_name('function')
        if function is None or function.type != 'field_expression':
            return False
        method = _text(function.child_by_field_name('field'))
        receiver = function.child_by_field_name('value')
        return (
            method in {'is_sign_positive', 'is_sign_negative', 'is_sign_zero'}
            and receiver is not None
            and _decision_name(receiver)
        )
    return False


def collect_algebraic_errors(
    src_root: Path = ROOT / 'src',
) -> tuple[list[str], int, int]:
    errors: list[str] = []
    total_uses = 0
    measurement_owners: set[tuple[str, str]] = set()
    for source in iter_sources(rust_source_config(src_root), scope='all'):
        uses: list[tuple[int, str | None, bool]] = []
        for root in _source_roots(source):
            for node, fn_name in _walk(root):
                if _algebraic_member(node):
                    # Recovered macro items lack their invocation's lexical
                    # scope. All approved owners are ordinary source functions.
                    approved = (
                        root == source.tree.root_node
                        and (source.label, fn_name) in MEASUREMENT_OWNERS
                    )
                    uses.append((node.start_point[0] + 1, fn_name, approved))
        total_uses += len(uses)
        for line, fn_name, approved in sorted(uses, key=operator.itemgetter(0)):
            if fn_name is None:
                errors.append(f'{source.label}:{line}:<unknown>: no enclosing fn')
            elif approved:
                measurement_owners.add((source.label, fn_name))
            else:
                errors.append(f'{source.label}:{line}:{fn_name}')
    return (errors, total_uses, len(measurement_owners))


def is_test_source(rel: str) -> bool:
    return rel.endswith(('/tests.rs', '_tests.rs'))


def collect_decision_surface_errors(src_root: Path = ROOT / 'src') -> list[str]:
    errors: list[str] = []
    for source in iter_sources(rust_source_config(src_root), scope='all'):
        rel = source.label
        if is_test_source(rel):
            continue
        findings: list[tuple[int, str]] = []
        for root in _source_roots(source):
            for node, _ in _walk(root):
                line = node.start_point[0] + 1
                if _shoelace_measure_call(node) and rel not in MEASUREMENT_FILES:
                    findings.append((
                        line,
                        f'{rel}:{line}: shoelace_measure_columns outside measurement files',
                    ))
                if _is_signed_area_function(node) and rel.startswith('src/geometry/'):
                    findings.append((
                        line,
                        f'{rel}:{line}: signed-area fn returning f64 in geometry',
                    ))
                if in_decision_module(rel) and _decision_float_branch(node):
                    findings.append((
                        line,
                        f'{rel}:{line}: raw area float branch in decision module',
                    ))
        errors.extend(
            error for _, error in sorted(findings, key=operator.itemgetter(0))
        )
    return errors


def main() -> int:
    algebraic_errors, total_uses, fn_count = collect_algebraic_errors()
    decision_errors = collect_decision_surface_errors()
    errors = algebraic_errors + decision_errors
    for error in errors:
        print(f'  {error}', file=sys.stderr)
    if not errors:
        print(
            f'algebraic-float placement OK ({total_uses} uses in {fn_count} approved owners); decision surface clean'
        )
    print(f'\nTOTAL VIOLATIONS: {len(errors)}')
    return 1 if errors else 0


if __name__ == '__main__':
    sys.exit(main())
