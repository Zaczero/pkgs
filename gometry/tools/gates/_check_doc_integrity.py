#!/usr/bin/env python3
"""Check duplicate sections on resolved public documentation carriers."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import TYPE_CHECKING

_TOOLS_ROOT = Path(__file__).resolve().parents[1]
if str(_TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TOOLS_ROOT))

from _gatelib import prepend_tools_import_paths, rust_source_config

prepend_tools_import_paths()

from public_inventory import load_public_symbols
from pyo3stubs.context import CheckContext
from pyo3stubs.doc_structure import duplicate_section_errors, iter_doc_subjects

if TYPE_CHECKING:
    from types import ModuleType

    from pyo3stubs.config import StubConfig

ROOT = Path(__file__).resolve().parents[2]


def _resolved_doc_errors(
    ctx: CheckContext,
    runtime: ModuleType,
) -> list[str]:
    errors: list[str] = []
    inventory = load_public_symbols(
        ctx.cfg.stub_path,
        ROOT / 'python' / 'gometry' / '__init__.py',
    )
    seen: set[tuple[tuple[int, ...], str] | tuple[str, str, int, str]] = set()
    for subject in iter_doc_subjects(
        ctx,
        runtime_module=runtime,
        source_classes=inventory.source_classes,
    ):
        if not subject.prose:
            continue
        if subject.declarations:
            carrier = (tuple(id(node) for node in subject.declarations), subject.prose)
        else:
            carrier = (
                subject.qualname,
                subject.source.path,
                subject.source.line,
                subject.prose,
            )
        if carrier in seen:
            continue
        seen.add(carrier)
        errors.extend(
            f'{subject.source.path}:{subject.source.line}: {error}'
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
    return _resolved_doc_errors(context, runtime)


def main() -> int:
    from _gatelib import require_runtime

    runtime, _, _ = require_runtime(ROOT)
    errors = collect_errors(runtime_module=runtime)
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
