"""Every ``docs/**/*.md`` page must be reachable from the ``properdocs.yml`` nav.

MkDocs publishes pages that are absent from the nav as orphan URLs (findable
via search and sitemap) — an internal planning document once shipped to the
public site this way. A page is either in the nav or explicitly registered
below with a reason.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from conftest import load_tool

_ROOT = Path(__file__).resolve().parent.parent
_DOCS = _ROOT / 'docs'
_CONFIG = _ROOT / 'properdocs.yml'

#: ``docs/-relative path -> reason`` for pages deliberately outside the nav.
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


def test_every_docs_page_is_in_nav() -> None:
    config = yaml.safe_load(
        # mkdocs-material configs carry python/name tags safe_load rejects;
        # the nav section is plain scalars, so strip unknown tags first.
        _CONFIG.read_text(encoding='utf-8').replace('!!python/name:', '')
    )
    nav = _nav_pages(config.get('nav'))
    orphans = sorted(
        rel
        for path in _DOCS.rglob('*.md')
        if (rel := path.relative_to(_DOCS).as_posix()) not in nav
        and rel not in ALLOWED_UNLISTED
    )
    assert not orphans, (
        'docs pages not reachable from the properdocs.yml nav (they publish as '
        f'orphan URLs; add to nav or register in ALLOWED_UNLISTED): {orphans}'
    )


def test_allowed_unlisted_entries_exist() -> None:
    stale = [rel for rel in ALLOWED_UNLISTED if not (_DOCS / rel).is_file()]
    assert not stale, f'ALLOWED_UNLISTED names deleted docs pages: {stale}'


def test_generated_api_routes_match_nav() -> None:
    config = yaml.safe_load(
        _CONFIG.read_text(encoding='utf-8').replace('!!python/name:', '')
    )
    nav = _nav_pages(config.get('nav'))
    expected = load_tool('api_structure').generated_api_nav_paths()
    nested = {page for page in nav if page.startswith('api/')}
    assert nested == expected, (
        'generated API routes and nested navigation must match exactly: '
        f'missing={sorted(expected - nested)}, stale={sorted(nested - expected)}'
    )
