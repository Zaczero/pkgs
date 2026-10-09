#!/usr/bin/env python3
"""Check shared parsed-doc section integrity across stub, Python, and Rust sources."""

from __future__ import annotations

import ast
import importlib
import sys
from pathlib import Path
from types import ModuleType

_TOOLS_ROOT = Path(__file__).resolve().parents[1]
if str(_TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TOOLS_ROOT))

from _gatelib import prepend_rust_source_import_paths, rust_source_config

prepend_rust_source_import_paths()

from pyo3stubs.ast_util import annotated_data_descriptors
from pyo3stubs.config import StubConfig
from pyo3stubs.context import CheckContext
from pyo3stubs.doc_structure import duplicate_section_errors, iter_doc_subjects
from pyo3stubs.rust_scan import _doc_line, iter_sources

ROOT = Path(__file__).resolve().parents[2]


def _stub_doc_errors(ctx: CheckContext, runtime: ModuleType) -> list[str]:
    path = ctx.cfg.stub_path
    errors: list[str] = []
    for node in ast.walk(ctx.stub_ast):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        doc = ast.get_docstring(node, clean=False)
        if doc:
            errors.extend(
                f'{path}:{node.lineno}: {error}'
                for error in duplicate_section_errors(doc, node.name)
            )

    for class_name, node in ctx.stub_classes.items():
        cls = getattr(runtime, class_name, None)
        if not isinstance(cls, type):
            continue
        for name, statement, doc, _descriptor in annotated_data_descriptors(
            cls, node.body
        ):
            if doc:
                errors.extend(
                    f'{path}:{statement.lineno}: {error}'
                    for error in duplicate_section_errors(doc, f'{class_name}.{name}')
                )
    return errors


def _rust_doc_blocks(source) -> list[tuple[int, str]]:
    """Read contiguous doc-comment nodes from the owner-retained syntax trees."""
    comments: set[tuple[int, str]] = set()
    for tree in (source.tree, *source.macro_trees):
        pending = [tree.root_node]
        while pending:
            node = pending.pop()
            if node.type == 'line_comment':
                raw = node.text
                if raw is not None and raw.lstrip().startswith(b'///'):
                    if (doc := _doc_line(node)) is not None:
                        comments.add((node.start_point[0] + 1, doc))
            pending.extend(reversed(node.children))

    blocks: list[tuple[int, str]] = []
    start_line = 0
    previous_line = 0
    lines: list[str] = []
    for line, text in sorted(comments):
        if lines and line != previous_line + 1:
            blocks.append((start_line, '\n'.join(lines)))
            lines = []
        if not lines:
            start_line = line
        lines.append(text)
        previous_line = line
    if lines:
        blocks.append((start_line, '\n'.join(lines)))
    return blocks


def _rust_doc_errors(cfg: StubConfig) -> list[str]:
    errors: list[str] = []
    for source in iter_sources(cfg, scope='all'):
        for start_line, doc in _rust_doc_blocks(source):
            errors.extend(
                duplicate_section_errors(doc, f'{source.label}:{start_line}')
            )
    return errors


def _python_doc_errors(
    ctx: CheckContext,
    runtime: ModuleType,
) -> list[str]:
    stub_path = ctx.cfg.stub_path.resolve()
    python_root = ROOT / 'python'
    errors: list[str] = []
    for subject in iter_doc_subjects(ctx, runtime_module=runtime):
        if not subject.prose:
            continue
        source_path = Path(subject.source.path).resolve()
        if source_path == stub_path or not source_path.is_relative_to(python_root):
            continue
        errors.extend(
            f'{source_path}:{subject.source.line}: {error}'
            for error in duplicate_section_errors(subject.prose, subject.qualname)
        )
    return errors


def collect_errors(
    cfg: StubConfig | None = None,
    runtime_module: ModuleType | None = None,
) -> list[str]:
    """Return duplicated sections in authored doc carriers."""
    cfg = cfg or rust_source_config()
    context = CheckContext(cfg)
    runtime = runtime_module or importlib.import_module('gometry')
    errors = _stub_doc_errors(context, runtime)
    errors.extend(_python_doc_errors(context, runtime))
    errors.extend(_rust_doc_errors(cfg))
    return list(dict.fromkeys(errors))


def main() -> int:
    errors = collect_errors()
    if errors:
        print(
            f'doc-integrity: {len(errors)} duplicate doc section(s):',
            file=sys.stderr,
        )
        for error in errors:
            print(f'  {error}', file=sys.stderr)
        return 1
    print('doc-integrity: OK (no duplicate doc sections)')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
