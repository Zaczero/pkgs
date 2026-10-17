#!/usr/bin/env python3
"""Validate YAML frontmatter at the source of a package documentation build."""

from __future__ import annotations

import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

if TYPE_CHECKING:
    from properdocs.config.defaults import ProperDocsConfig


def _error_context(path: Path, lines: Sequence[str], mark: object) -> str:
    line = getattr(mark, 'line', 0) + 2
    column = getattr(mark, 'column', 0) + 1
    source_index = min(line - 1, len(lines) - 1)
    source = lines[source_index].rstrip('\n') if lines else ''
    return f'{path}:{line}:{column}: {source}'


def collect_errors(docs: Path) -> list[str]:
    """Return malformed YAML frontmatter errors below *docs*."""
    if not docs.is_dir():
        return [f'{docs}: documentation directory does not exist']

    errors: list[str] = []
    for path in sorted(docs.rglob('*.md')):
        try:
            lines = path.read_text(encoding='utf-8').splitlines(keepends=True)
        except UnicodeDecodeError as error:
            errors.append(f'{path}: cannot decode Markdown as UTF-8: {error}')
            continue

        if not lines or lines[0].strip() != '---':
            continue

        try:
            end = next(
                index
                for index, line in enumerate(lines[1:], start=1)
                if line.strip() in {'---', '...'}
            )
        except StopIteration:
            errors.append(
                f'{path}:1: unterminated YAML frontmatter (expected --- or ...)'
            )
            continue

        try:
            metadata = yaml.safe_load(''.join(lines[1:end]))
        except yaml.YAMLError as error:
            mark = getattr(error, 'problem_mark', None)
            location = _error_context(path, lines, mark) if mark else str(path)
            problem = getattr(error, 'problem', None) or str(error).splitlines()[0]
            errors.append(f'{location}: malformed YAML frontmatter: {problem}')
            continue

        if not isinstance(metadata, Mapping):
            kind = 'empty' if metadata is None else type(metadata).__name__
            errors.append(f'{path}:1: YAML frontmatter must be a mapping, got {kind}')

    return errors


def on_pre_build(*, config: ProperDocsConfig) -> None:
    """Reject frontmatter and stale Gometry imports before ProperDocs reads pages."""
    from properdocs.exceptions import PluginError

    docs = Path(config.docs_dir).resolve()
    package_root = docs.parent
    if (package_root / 'tools' / 'wheel_smoke.py').is_file():
        tools = package_root / 'tools'
        if str(tools) not in sys.path:
            sys.path.insert(0, str(tools))
        from _gatelib import prepend_tools_import_paths, require_runtime

        prepend_tools_import_paths()
        require_runtime(package_root)
    errors = collect_errors(docs)
    if errors:
        raise PluginError(
            'Malformed YAML frontmatter:\n'
            + '\n'.join(f'  {error}' for error in errors)
        )


def main(argv: Sequence[str] | None = None) -> int:
    args = list(argv if argv is not None else sys.argv[1:])
    if len(args) > 1:
        print(f'usage: {Path(sys.argv[0]).name} [docs-directory]', file=sys.stderr)
        return 2

    docs = Path(args[0]) if args else Path.cwd() / 'docs'
    errors = collect_errors(docs)
    if errors:
        print(f'docs: {len(errors)} malformed frontmatter block(s)', file=sys.stderr)
        for error in errors:
            print(f'  {error}', file=sys.stderr)
        return 1

    print(f'docs: YAML frontmatter is valid ({docs})')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
