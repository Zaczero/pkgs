"""Release gates for h2corn's rendered public documentation."""

from __future__ import annotations

import shutil
import subprocess
import sys
import tomllib
from collections import Counter
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urljoin, urlparse

ROOT = Path(__file__).resolve().parents[2]
SITE = ROOT / 'site'
EXAMPLES = ROOT / 'examples'
sys.path.insert(0, str(ROOT))


def _site_text() -> str:
    pages = sorted(SITE.rglob('*.html'))
    if not pages:
        raise RuntimeError(f'no rendered HTML pages found below {SITE}')
    return '\n'.join(path.read_text(encoding='utf-8') for path in pages)


class _Anchors(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.ids: list[str] = []
        self.hrefs: list[str] = []
        self.sections: dict[str, list[str]] = {}
        self._section: tuple[str | None, list[str]] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {'h1', 'h2', 'h3', 'h4', 'h5', 'h6'}:
            self._finish_section()
            self._section = (None, [])
        for name, value in attrs:
            if name == 'id' and value is not None:
                self.ids.append(value)
                if self._section is not None and self._section[0] is None:
                    self._section = (value, self._section[1])
            elif name == 'href' and value is not None:
                self.hrefs.append(value)

    def handle_data(self, data: str) -> None:
        if self._section is not None:
            self._section[1].append(data)

    def _finish_section(self) -> None:
        if self._section is None:
            return
        identifier, parts = self._section
        if identifier is not None:
            text = ' '.join(''.join(parts).split())
            self.sections.setdefault(identifier, []).append(text)
        self._section = None

    def close(self) -> None:
        self._finish_section()
        super().close()


def check_rendered_anchors() -> None:
    """Resolve every local fragment against the rendered HTML IDs.

    The generic link checker treats dots in Python API IDs as CSS class
    separators. Those dots are intentional stable API anchors, so resolve the
    rendered HTML directly instead of weakening the public anchors.
    """
    missing: list[str] = []
    for page in sorted(SITE.rglob('*.html')):
        parser = _Anchors()
        parser.feed(page.read_text(encoding='utf-8'))
        parser.close()
        duplicates = sorted(
            identifier for identifier, count in Counter(parser.ids).items() if count > 1
        )
        if duplicates:
            missing.append(f'{page}: duplicate rendered IDs: {duplicates}')
        relative = page.relative_to(SITE)
        base_url = f'https://h2corn.invalid/{relative.parent.as_posix()}/'
        for href in parser.hrefs:
            resolved = urljoin(base_url, href)
            target_url = urlparse(resolved)
            if target_url.scheme not in {'http', 'https'} or not target_url.fragment:
                continue
            if target_url.netloc != 'h2corn.invalid':
                continue
            target = SITE / target_url.path.lstrip('/')
            if target.is_dir() or (not target.exists() and target.suffix == ''):
                target /= 'index.html'
            if not target.exists():
                missing.append(f'{page}: {href} (target is missing)')
                continue
            target_parser = _Anchors()
            target_parser.feed(target.read_text(encoding='utf-8'))
            target_parser.close()
            fragment = unquote(target_url.fragment)
            if fragment not in target_parser.ids:
                missing.append(f'{page}: {href} (fragment is missing)')
    if missing:
        raise AssertionError(
            'rendered local anchors are broken:\n' + '\n'.join(missing)
        )


def _rendered_site() -> dict[str, _Anchors]:
    rendered: dict[str, _Anchors] = {}
    for path in sorted(SITE.rglob('*.html')):
        parser = _Anchors()
        parser.feed(path.read_text(encoding='utf-8'))
        parser.close()
        rendered[path.relative_to(SITE).as_posix()] = parser
    return rendered


def check_configuration(
    _site_text: str | None = None,
    rendered: dict[str, _Anchors] | None = None,
) -> None:
    """Check exact option and command-control entries in rendered Config docs."""
    import h2corn
    from h2corn._cli import build_parser
    from h2corn._config import config_options

    rendered = _rendered_site() if rendered is None else rendered
    config_page = rendered.get('configuration/index.html')
    if config_page is None:
        raise AssertionError('rendered Configuration page is missing')
    config_ids = Counter(config_page.ids)
    missing: list[str] = []
    for option in config_options():
        identifier = f'option-{option.name}'
        if config_ids[identifier] != 1:
            missing.append(
                f'Config field {option.name!r}: expected one canonical heading'
            )
            continue
        entry = config_page.sections[identifier][0]
        required = [
            option.name,
            option.env_var,
            *option.cli_flags,
            'TOML key',
            'Precedence',
        ]
        if any(token not in entry for token in required):
            missing.append(f'Config field {option.name!r}: incomplete rendered entry')

    # These are controls, not Config options, so they must have their own
    # stable entries rather than merely occurring somewhere in the site.
    parser = build_parser(h2corn.Config(), None)
    config_names = {option.name for option in config_options()}
    seen: set[str] = set()
    for action in parser._actions:
        if action.dest in config_names or not action.option_strings:
            continue
        if action.dest in seen:
            continue
        seen.add(action.dest)
        identifier = f'command-{action.dest}'
        if config_ids[identifier] != 1:
            missing.append(
                f'command-only flag group {action.dest!r}: '
                'expected one canonical heading'
            )
            continue
        entry = config_page.sections[identifier][0]
        if any(flag not in entry for flag in action.option_strings):
            missing.append(
                f'command-only flag group {action.dest!r}: incomplete rendered entry'
            )
    if missing:
        raise AssertionError(
            'rendered Configuration is incomplete: ' + ', '.join(missing)
        )


def check_api(
    _site_text: str | None = None,
    rendered: dict[str, _Anchors] | None = None,
) -> None:
    """Check exact canonical anchors for exports and TypedDict fields."""
    import h2corn
    import h2corn._types as public_types

    rendered = _rendered_site() if rendered is None else rendered
    api_pages = {
        relative: page
        for relative, page in rendered.items()
        if relative.startswith('api/')
    }
    api_ids = Counter(
        identifier for page in api_pages.values() for identifier in page.ids
    )

    expected = tuple(h2corn.__all__)
    missing = [name for name in expected if api_ids[f'h2corn.{name}'] != 1]
    if missing:
        raise AssertionError(
            'rendered API anchors must be an exact, non-overlapping __all__ '
            f'partition: invalid={missing}'
        )

    missing_fields: list[str] = []
    for name in expected:
        value = getattr(public_types, name, None)
        annotations = getattr(value, '__annotations__', None)
        if not annotations or not hasattr(value, '__required_keys__'):
            continue
        for field in annotations:
            identifier = f'h2corn.{name}.{field}'
            if api_ids[identifier] != 1:
                missing_fields.append(f'{name}.{field}')
    if missing_fields:
        raise AssertionError(
            'rendered TypedDict fields must have one canonical anchor: '
            f'{missing_fields}'
        )


def check_examples() -> None:
    """Compile examples and exercise the deterministic embedded lifecycle."""
    import h2corn

    for path in sorted(EXAMPLES.glob('*.py')):
        compile(path.read_text(encoding='utf-8'), str(path), 'exec')
    with (EXAMPLES / 'h2corn.toml').open('rb') as handle:
        h2corn.Config.from_mapping(tomllib.load(handle))
    subprocess.run(
        [
            sys.executable,
            '-m',
            'h2corn',
            '--check-config',
            '--config',
            str(EXAMPLES / 'h2corn.toml'),
        ],
        cwd=ROOT,
        check=True,
        timeout=30,
    )
    result = subprocess.run(
        [sys.executable, str(EXAMPLES / 'embedded.py')],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if 'embedded request: 200' not in result.stdout:
        raise AssertionError(
            f'embedded example did not prove its request: {result.stdout!r}'
        )


def check_proxy_syntax() -> None:
    """Validate the shipped Caddyfile through caddy's own parser.

    Opportunistic: caddy is not in the development shell, so this passes
    trivially where it is absent. It says so rather than reporting a silent
    success, because a gate that cannot tell those apart is worse than none.
    """
    if shutil.which('caddy') is None:
        print('h2corn docs: caddy is not on PATH; examples/Caddyfile was NOT validated')
    else:
        subprocess.run(
            [
                'caddy',
                'validate',
                '--config',
                str(EXAMPLES / 'Caddyfile'),
                '--adapter',
                'caddyfile',
            ],
            cwd=ROOT,
            check=True,
            timeout=30,
            stdout=subprocess.DEVNULL,
        )


def main() -> int:
    site_text = _site_text()
    check_rendered_anchors()
    check_configuration(site_text)
    check_api(site_text)
    check_examples()
    check_proxy_syntax()
    print('h2corn docs: rendered Config/API, examples, and proxy gates passed')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
