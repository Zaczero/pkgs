#!/usr/bin/env python3
"""Fail on numpydoc-block corruption that the parity/example gates can't see.

A bad doc-injection pass once duplicated docstring tails (a second
``Parameters``/``Returns`` section, the summary leaked into the last ``Raises``
entry). Because the corruption was mirrored identically into Rust ``///`` and the
``.pyi`` stub, ``pyo3stubs check-all`` (parity) and ``_check_docstring_examples``
(``>>>`` only) stayed green. This gate scans the canonical Rust ``///`` blocks **and** the
stub docstrings for the corruption signature and rejects it outright.

Signature of a corrupt block (any one):
  * a section header (Parameters/Returns/Raises/Yields) appears twice,
  * a ``Parameters``/``Returns`` header appears *after* a ``Raises`` header,
  * the summary's first line reappears verbatim later in the same block.
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
SECTIONS = {
    'Parameters',
    'Returns',
    'Raises',
    'Yields',
    'Other Parameters',
    'Attributes',
}
# Every numpydoc section header — used to bound the Raises-block scan so a summary
# echoed in a narrative section (Examples/Notes/See Also) after Raises is not a leak.
_ALL_HEADERS = SECTIONS | {
    'Examples',
    'Example',
    'See Also',
    'Notes',
    'Note',
    'Warnings',
    'References',
}
_DOC = re.compile(r'^\s*///( ?)(.*)$')


def _block_is_corrupt(lines: list[str]) -> str | None:
    headers = [c.strip() for c in lines if c.strip() in SECTIONS]
    seen: set[str] = set()
    raises_seen = False
    for name in headers:
        if name == 'Raises':
            raises_seen = True
        if name in seen:
            return f"duplicate '{name}' section"
        if raises_seen and name in ('Parameters', 'Returns'):
            return f"'{name}' section after 'Raises'"
        seen.add(name)
    # A summary that merely reappears as a (legit) Returns/param *description*
    # before any Raises section is fine; the corruption is the summary leaking
    # *into/after* the Raises block, or trailing the docstring as orphan text.
    if lines and lines[0].strip():
        summary = lines[0].strip()
        raises_idx = next(
            (i for i, c in enumerate(lines) if c.strip() == 'Raises'), None
        )
        if raises_idx is not None:
            # Scan only WITHIN the Raises block (stop at the next section header),
            # so a summary legitimately echoed in a later Examples/See Also/Notes
            # section placed after Raises is not mistaken for a leak.
            for c in lines[raises_idx + 1 :]:
                if c.strip() in _ALL_HEADERS:
                    break
                if c.strip() == summary:
                    return 'summary line leaked into Raises section'
    return None


def _scan_rust() -> list[str]:
    bad: list[str] = []
    for path in sorted((ROOT / 'src').rglob('*.rs')):
        cur: list[str] = []
        start = 0
        for i, line in enumerate(path.read_text().splitlines()):
            m = _DOC.match(line)
            if m:
                if not cur:
                    start = i + 1
                cur.append(m.group(2))
            else:
                if cur and (why := _block_is_corrupt(cur)):
                    bad.append(f'{path.relative_to(ROOT)}:{start}: {why}')
                cur = []
        if cur and (why := _block_is_corrupt(cur)):
            bad.append(f'{path.relative_to(ROOT)}:{start}: {why}')
    return bad


def _scan_stub() -> list[str]:
    stub = ROOT / 'python' / 'gometry' / '_lib.pyi'
    tree = ast.parse(stub.read_text())
    bad: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            doc = ast.get_docstring(node, clean=False)
            if doc and (why := _block_is_corrupt(doc.splitlines())):
                bad.append(f'_lib.pyi:{node.lineno}: {node.name}: {why}')
    return bad


# A shared doc macro once shipped array-speak on every scalar method ("Returns
# ... `GeometryArray` — One result per element" on `Point.buffer`); this scan
# rejects that class on the scalar surfaces: the Returns TYPE LINE may only be
# `GeometryArray` when the signature itself returns one. (Array classes are
# out of scope — their `Self` IS a GeometryArray, and grouped/tuple results
# legitimately mention it in prose.)
_RETURNS_BLOCK = re.compile(
    r'\nReturns\n-------\n(.*?)(?=\n\S[^\n]*\n-+\n|\Z)', re.DOTALL
)
_ARRAY_CLASSES = {'GeometryArray', 'CellArray'}


def _scan_stub_returns_semantics() -> list[str]:
    stub = ROOT / 'python' / 'gometry' / '_lib.pyi'
    tree = ast.parse(stub.read_text())
    bad: list[str] = []
    for cls in tree.body:
        if not isinstance(cls, ast.ClassDef) or cls.name in _ARRAY_CLASSES:
            continue
        for node in cls.body:
            if not isinstance(node, ast.FunctionDef) or node.returns is None:
                continue
            # clean=True dedents the class-body indentation so the section
            # headers sit flush-left for the regex.
            doc = ast.get_docstring(node, clean=True)
            if not doc:
                continue
            m = _RETURNS_BLOCK.search('\n' + doc)
            if not m:
                continue
            type_line = m.group(1).strip().splitlines()[0].strip().strip('`')
            annotation = ast.unparse(node.returns)
            if (
                type_line.startswith('GeometryArray')
                and 'GeometryArray' not in annotation
            ):
                bad.append(
                    f'_lib.pyi:{node.lineno}: {cls.name}.{node.name}: Returns '
                    f'type line is `GeometryArray` but the signature returns '
                    f'`{annotation}` (surface-mismatched doc macro arm?)'
                )
    return bad


# The historical rustfmt wrap_comments mangle shattered prose into orphan
# fragments ("the\ndefault)\nremoves the\nleast"). Reject any docstring line
# that is a single short unpunctuated word continued by same-indent prose.
_ORPHAN = re.compile(r'^(\s*)([A-Za-z(]{1,12})$')


def _shattered_lines(doc: str) -> list[int]:
    lines = doc.splitlines()
    hits: list[int] = []
    in_doctest = False
    for i, line in enumerate(lines[:-1]):
        s = line.strip()
        if s.startswith('>>>'):
            in_doctest = True
        elif not s:
            in_doctest = False
        if in_doctest:
            continue
        m = _ORPHAN.match(line)
        if not m:
            continue
        nxt = lines[i + 1]
        # a definition-list header legitimately stands alone before an
        # indented description (Returns type lines like "float")
        if nxt.startswith(m.group(1) + '    ') or not nxt.strip():
            continue
        if nxt.lstrip()[:1].islower() or nxt.lstrip()[:2] == '``':
            hits.append(i + 1)
    return hits


def _scan_stub_shatter() -> list[str]:
    stub = ROOT / 'python' / 'gometry' / '_lib.pyi'
    tree = ast.parse(stub.read_text())
    bad: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            doc = ast.get_docstring(node, clean=False)
            if not doc:
                continue
            bad.extend(
                f'_lib.pyi:{node.lineno}: {node.name}: shattered prose at '
                f'docstring line {lineno} (orphan word mid-sentence)'
                for lineno in _shattered_lines(doc)
            )
    return bad


def main() -> int:
    bad = (
        _scan_rust()
        + _scan_stub()
        + _scan_stub_returns_semantics()
        + _scan_stub_shatter()
    )
    if bad:
        print(f'doc-integrity: {len(bad)} corrupt docstring block(s):', file=sys.stderr)
        for b in bad:
            print(f'  {b}', file=sys.stderr)
        return 1
    print('doc-integrity: OK (no duplicated/orphaned numpydoc blocks)')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
