"""Packed-column execution gate: heavy packed work must use the detached layer.

The packed-column execution layer (`src/array/packed_columns.rs`) is the only
place that may resolve `row_map`, release the GIL, and hand kernels logical
contiguous columns. Public method surfaces must route through
`map/reduce/pair_packed_*_detached` instead of reimplementing that seam.

This gate forbids the live regressions:

* ``.xy_columns()`` on array method/metric surfaces outside
  ``src/array/packed_columns.rs``.
* Direct calls to ``packed_line_measure``, ``packed_polygon_measure``,
  ``transform_packed_columns``, ``segmentize_packed_*``, and ``densify_packed_*``
  from ``src/array/methods_*.rs`` and ``src/broadcast/metrics/arrays.rs``.

Explicit exceptions remain narrow: ``swap_xy_packed_storage``, indexing/gather
internals, and storage constructors.

Usage::

    .venv/bin/python tools/gates/_check_packed_execution.py

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

from _gatelib import report_errors

ROOT = Path(__file__).resolve().parents[2]
SCAN_ROOT = ROOT / 'src'

_FORBID_XY_COLUMNS = re.compile(r'\.xy_columns\s*\(')

_FORBID_PACKED_KERNELS = re.compile(
    r'\b(?:'
    r'packed_line_measure|'
    r'packed_polygon_measure|'
    r'packed_lines_equal_exact(?:_columns)?|'
    r'packed_polygons_equal_exact(?:_columns)?|'
    r'transform_packed_columns|'
    r'segmentize_packed_(?:lines|polygons)|'
    r'densify_packed_(?:lines|polygons)'
    r')\s*\('
)

_XY_COLUMNS_PATHS = [
    SCAN_ROOT / 'array' / 'linref_kernels.rs',
    SCAN_ROOT / 'broadcast' / 'metrics' / 'arrays.rs',
    SCAN_ROOT / 'broadcast' / 'predicates.rs',
]

_KERNEL_PATHS = [
    *sorted((SCAN_ROOT / 'array').glob('methods_*.rs')),
    SCAN_ROOT / 'broadcast' / 'metrics' / 'arrays.rs',
]


def _is_comment(line: str) -> bool:
    return line.lstrip().startswith('//')


def collect_errors() -> list[str]:
    errors: list[str] = []

    xy_paths = [*(SCAN_ROOT / 'array').glob('methods_*.rs'), *_XY_COLUMNS_PATHS]
    for path in sorted(xy_paths):
        rel = path.relative_to(ROOT).as_posix()
        for lineno, raw in enumerate(
            path.read_text(encoding='utf-8').splitlines(), start=1
        ):
            if _is_comment(raw):
                continue
            if _FORBID_XY_COLUMNS.search(raw):
                errors.append(
                    f'{rel}:{lineno}: forbids `.xy_columns()` on array '
                    f'method surfaces — normalize row maps in '
                    f'`packed_columns.rs` first.'
                )

    for path in _KERNEL_PATHS:
        rel = path.relative_to(ROOT).as_posix()
        for lineno, raw in enumerate(
            path.read_text(encoding='utf-8').splitlines(), start=1
        ):
            if _is_comment(raw):
                continue
            match = _FORBID_PACKED_KERNELS.search(raw)
            if match:
                errors.append(
                    f'{rel}:{lineno}: forbids direct packed kernel call '
                    f'(`{match.group(0)}`) — use the detached packed-column '
                    f'execution layer.'
                )

    return errors


def main() -> int:
    return report_errors(collect_errors(), 'VIOLATIONS')


if __name__ == '__main__':
    sys.exit(main())
