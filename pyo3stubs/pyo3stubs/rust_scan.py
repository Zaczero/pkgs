"""Shared Rust-source scanning: the ONE ``#[pyclass]`` view every gate uses.

``leaked_types`` and ``rust_nullability`` once carried divergent private
copies of this scan (one honored ``pyclass_patterns``, one did not), which
made whole leaf-class families invisible to the nullability gate.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from pyo3stubs.config import StubConfig

#: ``#[pyclass(...)]`` (attributes tolerated) followed by the struct/enum item.
DEFAULT_PYCLASS = re.compile(
    r'#\s*\[\s*pyclass\s*(?:\((?P<args>[^)]*)\))?\s*\]'
    r'(?:\s*#\s*\[[^\]]*\])*'
    r'\s*(?:pub(?:\([^)]*\))?\s+)?(?:struct|enum)\s+(?P<ident>\w+)',
)
PYCLASS_NAME_ARG = re.compile(r'\bname\s*=\s*"([^"]+)"')

#: ``#[cfg(test)]`` (other attributes tolerated) followed by an inline module.
_CFG_TEST_MOD = re.compile(
    r'#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\]'
    r'(?:\s*#\s*\[[^\]]*\])*'
    r'\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+\w+\s*\{',
)


def _pattern_export(match: re.Match[str]) -> tuple[str, str] | None:
    """``(rust_ident, py_name)`` from a project ``pyclass_patterns`` match.

    Group 1 alone → both names equal that group (Python name is the key).
    Groups 1 and 2 → Rust identifier then Python export name.
    """
    if not match.lastindex:
        return None
    if match.lastindex >= 2:
        return match.group(1), match.group(2)
    name = match.group(1)
    return name, name


def _test_module_spans(sanitized: str) -> list[tuple[int, int]]:
    """Half-open offset spans of ``#[cfg(test)]``-gated inline modules."""
    spans: list[tuple[int, int]] = []
    for match in _CFG_TEST_MOD.finditer(sanitized):
        depth = 1
        index = match.end()
        while index < len(sanitized) and depth:
            if sanitized[index] == '{':
                depth += 1
            elif sanitized[index] == '}':
                depth -= 1
            index += 1
        spans.append((match.start(), index))
    return spans


def _file_exports(cfg: StubConfig, text: str) -> Iterator[tuple[str, str]]:
    """``(rust_ident, py_name)`` pairs from one source file.

    Matches inside ``#[cfg(test)]`` modules are skipped: those types never
    compile into the shipped extension, so they are not part of the surface.
    """
    spans = _test_module_spans(sanitize(text))

    def gated(offset: int) -> bool:
        return any(start <= offset < end for start, end in spans)

    for match in DEFAULT_PYCLASS.finditer(text):
        if gated(match.start()):
            continue
        name_arg = PYCLASS_NAME_ARG.search(match.group('args') or '')
        yield match.group('ident'), name_arg.group(1) if name_arg else match.group('ident')
    for pattern in cfg.pyclass_patterns:
        for match in pattern.finditer(text):
            if gated(match.start()):
                continue
            export = _pattern_export(match)
            if export is not None:
                yield export


def pyclass_names(cfg: StubConfig) -> dict[str, str]:
    """Exported Python class name -> defining source path (relative to src)."""
    names: dict[str, str] = {}
    for path in _sources(cfg.src_root):
        rel = path.relative_to(cfg.src_root).as_posix()
        for _rust_ident, py_name in _file_exports(cfg, path.read_text()):
            names.setdefault(py_name, rel)
    return names


def rust_class_map(cfg: StubConfig) -> dict[str, str]:
    """Rust struct/enum identifier -> exported Python class name."""
    mapping: dict[str, str] = {}
    for path in _sources(cfg.src_root):
        for rust_ident, py_name in _file_exports(cfg, path.read_text()):
            mapping.setdefault(rust_ident, py_name)
    return mapping


def _sources(src_root: Path) -> list[Path]:
    return sorted(src_root.rglob('*.rs'))


def sanitize(text: str) -> str:
    """Blank out comments and string-literal contents (structure preserved).

    Brace-scanning over raw Rust miscounts when a ``{`` lives inside a string
    or comment; scanners should walk the sanitized text with original offsets.
    """
    out = list(text)
    index = 0
    length = len(text)
    while index < length:
        ch = text[index]
        if ch == '/' and index + 1 < length:
            nxt = text[index + 1]
            if nxt == '/':
                while index < length and text[index] != '\n':
                    out[index] = ' '
                    index += 1
                continue
            if nxt == '*':
                out[index] = out[index + 1] = ' '
                index += 2
                while index + 1 < length and not (
                    text[index] == '*' and text[index + 1] == '/'
                ):
                    if text[index] != '\n':
                        out[index] = ' '
                    index += 1
                if index + 1 < length:
                    out[index] = out[index + 1] = ' '
                    index += 2
                continue
        if ch == '"':
            index += 1
            while index < length:
                if text[index] == '\\':
                    out[index] = ' '
                    if index + 1 < length:
                        out[index + 1] = ' '
                    index += 2
                    continue
                if text[index] == '"':
                    index += 1
                    break
                if text[index] != '\n':
                    out[index] = ' '
                index += 1
            continue
        index += 1
    return ''.join(out)
