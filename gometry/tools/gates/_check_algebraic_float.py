"""Algebraic-float placement gate: ``.algebraic_*`` only in whitelisted fns.

Nightly ``algebraic_{add,sub,mul}`` reassociate float ops (vectorize reductions)
but CHANGE results. Policy: SAFE only in MEASUREMENT/aggregation (area/centroid
magnitude); FORBIDDEN in anything feeding a topology/orientation DECISION.
This gate scans ``src/**/*.rs`` for ``.algebraic_`` uses outside the two
measurement kernels that own them, and forbids raw signed-area branching in
topology modules.

Usage::

    .venv/bin/python tools/gates/_check_algebraic_float.py

Exit 1 on any violation. The final line is ``TOTAL VIOLATIONS: N``.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

_TOOLS_ROOT = Path(__file__).resolve().parents[1]
if str(_TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TOOLS_ROOT))
from _gatelib import prepend_tools_import_paths

prepend_tools_import_paths()

from _gatelib import iter_rust_sources

ROOT = Path(__file__).resolve().parents[2]
WHITELIST = frozenset({
    'centroid_ring_sums',
    'column_mean',
    'column_mean2',
    'column_sum',
    'lineal_centroid_column_sums',
    'lineal_centroid_scalar_algebraic',
    'lineal_centroid_segment_contrib',
    'open_cycle_magnitude_columns',
    'open_point_cycle_magnitude',
    'open_xy_cycle_magnitude',
    'shoelace_measure_columns',
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
_ALGEBRAIC_USE = re.compile('\\.algebraic_')
_FN_DEF = re.compile(
    '^\\s*(?:pub(?:\\([^)]*\\))?\\s+)?(?:async\\s+)?(?:const\\s+)?fn\\s+(\\w+)'
)
_SIGNED_AREA_FN = re.compile(
    '^\\s*(?:pub(?:\\([^)]*\\))?\\s+)?fn\\s+(signed_\\w*area|cycle_area2|\\w*_area2)\\b'
)
_DECISION_FLOAT_BRANCH = re.compile(
    '((?:area|shoelace|reduce_sum|column_sum)[\\w:]*\\s*(?:==|!=|>=|<=|>|<)\\s*0\\.0|(?:area|shoelace|reduce_sum|column_sum)[\\w:]*\\.is_sign_(?:positive|negative|zero)\\s*\\()'
)
_SHOELACE_MEASURE_OUTSIDE = re.compile('\\bshoelace_measure_columns\\s*\\(')


def enclosing_fn(lines: list[str], lineno: int) -> str | None:
    for index in range(lineno - 1, -1, -1):
        match = _FN_DEF.match(lines[index])
        if match:
            return match.group(1)
    return None


def in_decision_module(rel: str) -> bool:
    parts = rel.split('/')
    return any(part in DECISION_MODULES for part in parts)


def collect_algebraic_errors() -> tuple[list[str], int, int]:
    errors: list[str] = []
    total_uses = 0
    whitelisted_fns: set[str] = set()
    for path in iter_rust_sources():
        rel = path.relative_to(ROOT).as_posix()
        lines = path.read_text(encoding='utf-8').splitlines()
        for lineno, raw in enumerate(lines, start=1):
            if raw.lstrip().startswith('//'):
                continue
            if not _ALGEBRAIC_USE.search(raw):
                continue
            total_uses += 1
            fn_name = enclosing_fn(lines, lineno)
            if fn_name is None:
                errors.append(f'{rel}:{lineno}:<unknown>: no enclosing fn')
                continue
            if fn_name in WHITELIST:
                whitelisted_fns.add(fn_name)
                continue
            errors.append(f'{rel}:{lineno}:{fn_name}')
    return (errors, total_uses, len(whitelisted_fns))


def is_test_source(rel: str) -> bool:
    return rel.endswith(('/tests.rs', '_tests.rs'))


def collect_decision_surface_errors() -> list[str]:
    errors: list[str] = []
    for path in iter_rust_sources():
        rel = path.relative_to(ROOT).as_posix()
        if is_test_source(rel):
            continue
        lines = path.read_text(encoding='utf-8').splitlines()
        for lineno, raw in enumerate(lines, start=1):
            if raw.lstrip().startswith('//'):
                continue
            if _SHOELACE_MEASURE_OUTSIDE.search(raw) and rel not in MEASUREMENT_FILES:
                errors.append(
                    f'{rel}:{lineno}: shoelace_measure_columns outside measurement files'
                )
            if _SIGNED_AREA_FN.search(raw) and rel.startswith('src/geometry/'):
                errors.append(
                    f'{rel}:{lineno}: signed-area fn returning f64 in geometry'
                )
            if in_decision_module(rel) and _DECISION_FLOAT_BRANCH.search(raw):
                errors.append(
                    f'{rel}:{lineno}: raw area float branch in decision module'
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
            f'algebraic-float placement OK ({total_uses} uses in {fn_count} whitelisted fns); decision surface clean'
        )
    print(f'\nTOTAL VIOLATIONS: {len(errors)}')
    return 1 if errors else 0


if __name__ == '__main__':
    sys.exit(main())
