#!/usr/bin/env python3
"""Validate that every documentation page has one intentional navigation route."""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / 'docs'
CONFIG = ROOT / 'properdocs.yml'

# Pages intentionally published outside the navigation belong here with a reason.
ALLOWED_UNLISTED: dict[str, str] = {}


def _nav_pages(node: object) -> set[str]:
    pages: set[str] = set()
    if isinstance(node, str):
        pages.add(node)
    elif isinstance(node, list):
        for item in node:
            pages.update(_nav_pages(item))
    elif isinstance(node, dict):
        for value in node.values():
            pages.update(_nav_pages(value))
    return pages


def collect_errors() -> list[str]:
    config = yaml.safe_load(
        # Material configs carry Python-name tags that safe_load rejects; the
        # navigation itself contains only ordinary YAML scalars.
        CONFIG.read_text(encoding='utf-8').replace('!!python/name:', '')
    )
    nav = _nav_pages(config.get('nav'))
    orphans = sorted(
        relative
        for path in DOCS.rglob('*.md')
        if (relative := path.relative_to(DOCS).as_posix()) not in nav
        and relative not in ALLOWED_UNLISTED
    )
    stale = sorted(
        relative for relative in ALLOWED_UNLISTED if not (DOCS / relative).is_file()
    )

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from api_structure import generated_api_nav_paths

    expected = generated_api_nav_paths()
    actual = {page for page in nav if page.startswith('api/')}

    errors: list[str] = []
    if orphans:
        errors.append(f'documentation pages absent from navigation: {orphans}')
    if stale:
        errors.append(f'stale allowed-unlisted documentation pages: {stale}')
    if actual != expected:
        errors.append(
            'generated API routes differ from navigation: '
            f'missing={sorted(expected - actual)}, stale={sorted(actual - expected)}'
        )
    return errors


def main() -> int:
    errors = collect_errors()
    for error in errors:
        print(error, file=sys.stderr)
    print(f'TOTAL NAVIGATION FAILURES: {len(errors)}')
    return 1 if errors else 0


if __name__ == '__main__':
    raise SystemExit(main())
