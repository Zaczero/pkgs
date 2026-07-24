"""Shared helpers for the ``tools/gates/_check_*.py`` gate scripts.

Gate scripts run standalone (``.venv/bin/python tools/gates/_check_x.py``) and via
the pytest wrappers (``conftest.load_tool``). Import this module the way
``_check_bench_regression.py`` imports ``summarize_bench``::

    _TOOLS_ROOT = Path(__file__).resolve().parents[1]
    if str(_TOOLS_ROOT) not in sys.path:
        sys.path.insert(0, str(_TOOLS_ROOT))

    from _gatelib import iter_rust_sources, prepend_tools_import_paths, report_errors, strip_rust_comments

    prepend_tools_import_paths()
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

TOOLS_DIR = Path(__file__).resolve().parent
TOOL_IMPORT_DIRS = (
    TOOLS_DIR,
    TOOLS_DIR / 'gates',
    TOOLS_DIR / 'stubs',
    TOOLS_DIR / 'docs',
)
ROOT = TOOLS_DIR.parent


def prepend_tools_import_paths() -> None:
    """Prepend ``tools/`` and subdirs so sibling imports resolve from any gate."""
    for entry in TOOL_IMPORT_DIRS:
        path = str(entry)
        if path not in sys.path:
            sys.path.insert(0, path)


def strip_rust_comments(text: str) -> str:
    """Remove ``//`` and ``/* */`` comments while preserving string contents."""
    out: list[str] = []
    index = 0
    length = len(text)
    while index < length:
        ch = text[index]
        if ch == '/' and index + 1 < length:
            nxt = text[index + 1]
            if nxt == '/':
                index += 2
                while index < length and text[index] != '\n':
                    index += 1
                continue
            if nxt == '*':
                index += 2
                while index + 1 < length and not (
                    text[index] == '*' and text[index + 1] == '/'
                ):
                    index += 1
                index = min(index + 2, length)
                continue
        if ch in '"\'':
            quote = ch
            out.append(ch)
            index += 1
            while index < length:
                cur = text[index]
                out.append(cur)
                index += 1
                if cur == '\\' and index < length:
                    out.append(text[index])
                    index += 1
                    continue
                if cur == quote:
                    break
            continue
        if ch == 'b' and index + 1 < length and text[index + 1] in '"\'':
            quote = text[index + 1]
            out.append(ch)
            out.append(quote)
            index += 2
            while index < length:
                cur = text[index]
                out.append(cur)
                index += 1
                if cur == '\\' and index < length:
                    out.append(text[index])
                    index += 1
                    continue
                if cur == quote:
                    break
            continue
        out.append(ch)
        index += 1
    return ''.join(out)


def iter_rust_sources(root: Path | None = None) -> Iterator[Path]:
    """Every ``src/**/*.rs`` file, sorted, under ``root`` (default: repo root)."""
    yield from sorted(((root or ROOT) / 'src').rglob('*.rs'))


def report_errors(errors: list[str], label: str) -> int:
    """The gate-script exit protocol: errors to stderr, TOTAL line, 1 on red."""
    for error in errors:
        print(f'  {error}', file=sys.stderr)
    print(f'\nTOTAL {label}: {len(errors)}')
    return 1 if errors else 0
