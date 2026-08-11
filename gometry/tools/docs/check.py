#!/usr/bin/env python3
"""Validate the built documentation users will receive.

Run after ``properdocs build --strict``.  Properdocs owns Markdown parsing and
cross-reference validation; this check adds only the structural facts that the
builder cannot know: one canonical page per exported symbol, unique HTML ids,
public signatures without private helper names, and linked See Also entries.
"""

from __future__ import annotations

import html
import re
import shutil
import subprocess
import sys
from collections import Counter
from html.parser import HTMLParser
from pathlib import Path

import gometry as gm

ROOT = Path(__file__).resolve().parents[2]
SITE = ROOT / 'site'
API_SITE = SITE / 'api'

sys.path.insert(0, str(Path(__file__).resolve().parent))
from api_structure import OPTIONAL_EXPORTS, generated_api_nav_paths

_SIGNATURE = re.compile(
    r'<div class="language-python doc-signature highlight">(?P<body>.*?)</div>',
    re.DOTALL,
)
_SEE_ALSO = re.compile(
    r'<details[^>]*class="[^"]*\bsee-also\b[^"]*"[^>]*>(?P<body>.*?)</details>',
    re.IGNORECASE | re.DOTALL,
)
_SEE_ALSO_ENTRY = re.compile(r'<(?:p|li)\b[^>]*>(.*?)</(?:p|li)>', re.DOTALL)
_PRIVATE_NAME = re.compile(r'(?<![\w.])_[A-Za-z]\w*')
_TAG = re.compile(r'<[^>]+>')
_LITERAL_FENCE = re.compile(r'<p[^>]*>\s*`{3}')
_TOOL_TOKEN = re.compile(
    r'(?:<|&lt;)/?(?:antml:)?(?:invoke|parameter|function_calls|function_results)'
    r'(?:\s[^<>]*?)?(?:>|&gt;)',
)
_UNRESOLVED_GOMETRY_TYPE = re.compile(
    r'<span\b[^>]*title="gometry\.(?:(?:_lib|_types)\.)?'
    r'(?P<name>[A-Za-z_]\w*)"[^>]*>'
)
_RAISES_TABLE = re.compile(
    r'<span\b[^>]*class="[^"]*\bdoc-section-title\b[^"]*"[^>]*>'
    r'\s*Raises:\s*</span>\s*</p>\s*'
    r'<table>(?P<body>.*?)</table>',
    re.IGNORECASE | re.DOTALL,
)
_DOC_DESCRIPTION = re.compile(
    r'<div\b[^>]*class="[^"]*\bdoc-md-description\b[^"]*"[^>]*>'
    r'(?P<body>.*?)</div>',
    re.DOTALL,
)
_RENDERED_DOUBLE_PERIOD = re.compile(r'(?<!\.)\.\s*\.(?!\.)(?=\s*$)')


class _Ids(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.values: list[str] = []

    def handle_starttag(
        self,
        _tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        self.values.extend(value for name, value in attrs if name == 'id' and value)


def _plain(fragment: str) -> str:
    return html.unescape(_TAG.sub(' ', fragment))


def collect_errors(site: Path = SITE) -> list[str]:
    """Return structural problems in an already-built site."""
    api_site = site / 'api'
    if not api_site.is_dir():
        return ['site/api is missing; run `properdocs build --strict` first']

    errors: list[str] = []
    pages: dict[Path, str] = {}
    for path in sorted(site.rglob('*.html')):
        source = path.read_text(encoding='utf-8', errors='replace')
        pages[path] = source
        relative = path.relative_to(site)

        ids = _Ids()
        ids.feed(source)
        duplicates = sorted(
            ident for ident, count in Counter(ids.values).items() if count > 1
        )
        if duplicates:
            errors.append(f'{relative}: duplicate HTML ids: {duplicates[:5]}')
        if _LITERAL_FENCE.search(source):
            errors.append(f'{relative}: a Markdown fence rendered as prose')
        if token := _TOOL_TOKEN.search(source):
            errors.append(f'{relative}: leaked tool token {token.group(0)!r}')

        for section in _RAISES_TABLE.finditer(source):
            for description in _DOC_DESCRIPTION.finditer(section.group('body')):
                visible = ' '.join(_plain(description.group('body')).split())
                if _RENDERED_DOUBLE_PERIOD.search(visible):
                    errors.append(
                        f'{relative}: doubled terminal period in rendered '
                        f'Raises description: {visible[:80]!r}'
                    )

    expected_pages: set[Path] = set()
    for source_path in generated_api_nav_paths():
        path = Path(source_path)
        expected_pages.add(
            site / path.with_suffix('.html')
            if path.stem == 'index'
            else site / path.with_suffix('') / 'index.html'
        )
    missing_pages = sorted(
        path.relative_to(site) for path in expected_pages - pages.keys()
    )
    if missing_pages:
        errors.append(f'missing generated API pages: {missing_pages}')

    api_html = '\n'.join(
        source for path, source in pages.items() if path.is_relative_to(api_site)
    )
    exports = set(gm.__all__) | set(OPTIONAL_EXPORTS)
    public_classes = {
        name for name in exports if isinstance(getattr(gm, name, None), type)
    }
    for path, source in pages.items():
        unresolved = sorted({
            match.group('name')
            for match in _UNRESOLVED_GOMETRY_TYPE.finditer(source)
            if match.group('name') in public_classes
        })
        if unresolved:
            errors.append(
                f'{path.relative_to(site)}: unresolved public type links: {unresolved}'
            )
    for name in sorted(exports):
        count = api_html.count(f'id="gometry.{name}"')
        if count != 1:
            errors.append(
                f'gometry.{name}: expected one canonical API anchor, found {count}'
            )

    for match in _SIGNATURE.finditer(api_html):
        visible = _plain(match.group('body'))
        leaked = sorted({
            name for name in _PRIVATE_NAME.findall(visible) if not name.startswith('__')
        })
        if leaked:
            errors.append(f'private names in rendered signature: {leaked}')

    for block in _SEE_ALSO.finditer(api_html):
        entries = _SEE_ALSO_ENTRY.findall(block.group('body'))
        unlinked = [
            text
            for entry in entries or [block.group('body')]
            if '<a ' not in entry and (text := _plain(entry).strip())
        ]
        errors.extend(f'unlinked See Also entry: {text[:80]!r}' for text in unlinked)

    return errors


def main() -> int:
    errors = collect_errors()
    lychee = shutil.which('lychee')
    if lychee is None:
        errors.append('lychee is unavailable; enter the project development shell')
    elif not errors:
        links = subprocess.run(
            [
                lychee,
                '--offline',
                '--include-fragments=full',
                '--index-files=index.html',
                '--root-dir=site',
                'site/**/*.html',
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        if links.returncode:
            errors.append(
                'broken local links/fragments:\n'
                + (links.stdout + links.stderr).strip()[-4000:]
            )
    if errors:
        print(f'docs: {len(errors)} built-site issue(s):', file=sys.stderr)
        for error in errors:
            print(f'  {error}', file=sys.stderr)
        return 1
    print('docs: OK (canonical API anchors and local links/fragments are clean)')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
