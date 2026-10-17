#!/usr/bin/env python3
"""Validate the built documentation users will receive.

Run after ``properdocs build --strict``. Properdocs owns Markdown parsing and
cross-reference validation; this check verifies the built API against source
declarations and retains the canonical-anchor, privacy, and local-link checks.
"""

from __future__ import annotations

import ast
import html
import inspect
import re
import shutil
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[2]
SITE = ROOT / 'site'
API_SITE = SITE / 'api'

_TOOLS_ROOT = ROOT / 'tools'
if str(_TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TOOLS_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from api_structure import generated_api_nav_paths
from public_inventory import load_public_symbols
from site_contracts import collect_errors as collect_site_contract_errors

_SEE_ALSO = re.compile(
    r'<details[^>]*class="[^"]*\bsee-also\b[^"]*"[^>]*>(?P<body>.*?)</details>',
    re.IGNORECASE | re.DOTALL,
)
_SEE_ALSO_ENTRY = re.compile(r'<(?:p|li)\b[^>]*>(.*?)</(?:p|li)>', re.DOTALL)
_PRIVATE_NAME = re.compile(r'(?<![\w.])_[A-Za-z]\w*')
_TAG = re.compile(r'<[^>]+>')
_LITERAL_FENCE = re.compile(r'<p[^>]*>\s*`{3}')
_UNRESOLVED_GOMETRY_TYPE = re.compile(
    r'<span\b[^>]*title="gometry\.(?:(?:_lib|_types)\.)?'
    r'(?P<name>[A-Za-z_]\w*)"[^>]*>'
)


@dataclass(frozen=True, slots=True)
class RenderedCallForm:
    parameters: tuple[tuple[str, str, str | None, str | None], ...]
    returns: str | None


@dataclass(frozen=True, slots=True)
class RenderedSignature:
    subject: str
    source: str
    form: RenderedCallForm | None
    error: str | None = None


@dataclass(slots=True)
class _HtmlElement:
    tag: str
    classes: frozenset[str]
    subject: str | None
    is_signature: bool


@dataclass(slots=True)
class _SignatureText:
    subject: str
    parts: list[str] = field(default_factory=list)


def _top_level_index(source: str, separator: str) -> int:
    quote: str | None = None
    escaped = False
    depth = 0
    index = 0
    while index < len(source):
        char = source[index]
        if quote is not None:
            if escaped:
                escaped = False
            elif char == '\\':
                escaped = True
            elif char == quote:
                quote = None
        elif char in {"'", '"'}:
            quote = char
        elif char in '([{':
            depth += 1
        elif char in ')]}':
            depth -= 1
        elif depth == 0 and source.startswith(separator, index):
            return index
        index += 1
    return -1


def _top_level_parts(source: str, separator: str) -> list[str]:
    parts: list[str] = []
    start = 0
    while (index := _top_level_index(source[start:], separator)) >= 0:
        index += start
        parts.append(source[start:index])
        start = index + len(separator)
    parts.append(source[start:])
    return parts


def _signature_parameter_source(parameter: str) -> tuple[str, str | None]:
    """Remove its annotation for Python's AST parser; retain the type text."""
    parameter = parameter.strip()
    if parameter in {'/', '*'}:
        return parameter, None
    colon = _top_level_index(parameter, ':')
    if colon < 0:
        return parameter, None
    head = parameter[:colon].rstrip()
    annotation_default = parameter[colon + 1 :].strip()
    equals = _top_level_index(annotation_default, '=')
    if equals < 0:
        return head, annotation_default
    annotation = annotation_default[:equals].strip()
    default = annotation_default[equals + 1 :].strip()
    return f'{head} = {default}', annotation


def _normalized_expression(source: str | None) -> str | None:
    if source is None:
        return None
    try:
        return ast.unparse(ast.parse(source, mode='eval').body)
    except SyntaxError:
        return ' '.join(source.split())


def _signature_form(source: str) -> RenderedCallForm:
    """Parse call shape and defaults from highlighted text without splitting tokens."""
    visible = source.strip()
    opening = visible.find('(')
    if opening < 0:
        raise ValueError(f'signature has no parameter list: {visible!r}')

    quote: str | None = None
    escaped = False
    depth = 0
    closing = -1
    for index in range(opening, len(visible)):
        char = visible[index]
        if quote is not None:
            if escaped:
                escaped = False
            elif char == '\\':
                escaped = True
            elif char == quote:
                quote = None
        elif char in {"'", '"'}:
            quote = char
        elif char == '(':
            depth += 1
        elif char == ')':
            depth -= 1
            if depth == 0:
                closing = index
                break
    if closing < 0:
        raise ValueError(f'signature has an unclosed parameter list: {visible!r}')

    raw_parameters = visible[opening + 1 : closing]
    parameter_parts = (
        _top_level_parts(raw_parameters, ',') if raw_parameters.strip() else []
    )
    sanitized = []
    annotations: dict[str, str | None] = {}
    for parameter in parameter_parts:
        call_parameter, annotation = _signature_parameter_source(parameter)
        if call_parameter in {'/', '*'}:
            sanitized.append(call_parameter)
            continue
        name = call_parameter.lstrip('*').split('=', 1)[0].strip()
        annotations[name] = annotation
        sanitized.append(call_parameter)
    declaration = f'def _rendered({", ".join(sanitized)}):\n    pass\n'
    try:
        function = ast.parse(declaration).body[0]
    except SyntaxError as error:
        raise ValueError(f'invalid rendered signature {visible!r}: {error}') from error

    args = function.args
    positional = [*args.posonlyargs, *args.args]
    defaults: list[ast.expr | None] = [None] * (
        len(positional) - len(args.defaults)
    ) + list(args.defaults)
    parameters: list[tuple[str, str, str | None, str | None]] = []
    for index, (argument, default) in enumerate(zip(positional, defaults, strict=True)):
        parameters.append((
            'positional-only'
            if index < len(args.posonlyargs)
            else 'positional or keyword',
            argument.arg,
            _normalized_expression(annotations.get(argument.arg)),
            ast.unparse(default) if default is not None else None,
        ))
    if args.vararg is not None:
        parameters.append((
            'variadic positional',
            args.vararg.arg,
            _normalized_expression(annotations.get(args.vararg.arg)),
            None,
        ))
    for argument, default in zip(args.kwonlyargs, args.kw_defaults, strict=True):
        parameters.append((
            'keyword-only',
            argument.arg,
            _normalized_expression(annotations.get(argument.arg)),
            ast.unparse(default) if default is not None else None,
        ))
    if args.kwarg is not None:
        parameters.append((
            'variadic keyword',
            args.kwarg.arg,
            _normalized_expression(annotations.get(args.kwarg.arg)),
            None,
        ))

    return_text = visible[closing + 1 :].strip()
    if return_text.startswith('->'):
        return_text = return_text[2:].strip()
    else:
        return_text = ''
    return RenderedCallForm(
        tuple(parameters), _normalized_expression(return_text or None)
    )


class _RenderedSignatures(HTMLParser):
    """Associate highlighted signatures with their nearest canonical object."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.stack: list[_HtmlElement] = []
        self.active: _SignatureText | None = None
        self.values: list[tuple[str, str]] = []
        self.attributes: set[str] = set()

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        attributes = dict(attrs)
        classes = frozenset((attributes.get('class') or '').split())
        parent_subject = next(
            (element.subject for element in reversed(self.stack) if element.subject),
            None,
        )
        subject = parent_subject
        if (ident := attributes.get('id')) and ident.startswith('gometry.'):
            for element in reversed(self.stack):
                if 'doc-object' in element.classes:
                    element.subject = ident
                    subject = ident
                    break
        if 'doc-symbol-attribute' in classes and subject:
            self.attributes.add(subject)
        is_signature = 'doc-signature' in classes or any(
            element.is_signature for element in self.stack
        )
        element = _HtmlElement(tag, classes, subject, is_signature)
        self.stack.append(element)
        if tag == 'pre' and is_signature and subject:
            self.active = _SignatureText(subject)

    def handle_endtag(self, tag: str) -> None:
        if tag == 'pre' and self.active is not None:
            self.values.append((self.active.subject, ''.join(self.active.parts)))
            self.active = None
        for index in range(len(self.stack) - 1, -1, -1):
            if self.stack[index].tag == tag:
                del self.stack[index:]
                break

    def handle_data(self, data: str) -> None:
        if self.active is not None:
            self.active.parts.append(data)


def rendered_call_forms(source: str) -> dict[str, tuple[RenderedSignature, ...]]:
    """Parse canonical subjects and exact displayed signatures from rendered HTML."""
    parser = _RenderedSignatures()
    parser.feed(source)
    parser.close()
    grouped: dict[str, list[RenderedSignature]] = {}
    for subject, signature in parser.values:
        if subject in parser.attributes:
            form = None
            error = None
        else:
            try:
                form = _signature_form(signature)
                error = None
            except ValueError as parse_error:
                form = None
                error = str(parse_error)
        grouped.setdefault(subject, []).append(
            RenderedSignature(subject, signature, form, error)
        )
    return {subject: tuple(signatures) for subject, signatures in grouped.items()}


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


class _Hrefs(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.values: list[str] = []

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        if tag.lower() == 'a':
            self.values.extend(
                value for name, value in attrs if name == 'href' and value
            )


def _plain(fragment: str) -> str:
    return html.unescape(_TAG.sub(' ', fragment))


def collect_errors(site: Path = SITE) -> list[str]:
    """Return structural problems in an already-built site."""
    import gometry as gm

    api_site = site / 'api'
    if not api_site.is_dir():
        return ['site/api is missing; run `properdocs build --strict` first']

    errors: list[str] = []
    pages: dict[Path, str] = {}
    canonical_pages: dict[str, Path] = {}
    for path in sorted(site.rglob('*.html')):
        source = path.read_text(encoding='utf-8', errors='replace')
        pages[path] = source
        relative = path.relative_to(site)

        ids = _Ids()
        ids.feed(source)
        canonical_pages.update({
            ident: relative for ident in ids.values if ident.startswith('gometry.')
        })
        duplicates = sorted(
            ident for ident, count in Counter(ids.values).items() if count > 1
        )
        if duplicates:
            errors.append(f'{relative}: duplicate HTML ids: {duplicates[:5]}')
        if _LITERAL_FENCE.search(source):
            errors.append(f'{relative}: a Markdown fence rendered as prose')

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
    exports = set(gm.__all__)
    public_symbols = load_public_symbols(
        ROOT / 'python' / 'gometry' / '_lib.pyi',
        ROOT / 'python' / 'gometry' / '__init__.py',
    )
    public_classes = {
        name for name in exports if inspect.isclass(getattr(gm, name, None))
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

    rendered = rendered_call_forms(api_html)
    for subject, signatures in rendered.items():
        for signature in signatures:
            visible = signature.source
            leaked = sorted({
                name
                for name in _PRIVATE_NAME.findall(visible)
                if not name.startswith('__')
            })
            if leaked:
                errors.append(
                    f'{subject}: private names in rendered signature: {leaked}'
                )

    errors.extend(
        collect_site_contract_errors(
            site,
            pages,
            rendered,
            canonical_pages,
            gm,
            ROOT / 'python' / 'gometry',
        )
    )

    for path, source in pages.items():
        if not path.is_relative_to(api_site):
            continue
        relative = path.relative_to(site)
        for block in _SEE_ALSO.finditer(source):
            entries = _SEE_ALSO_ENTRY.findall(block.group('body'))
            for entry in entries or [block.group('body')]:
                links = _Hrefs()
                links.feed(entry)
                if not links.values:
                    text = _plain(entry).strip()
                    if text:
                        errors.append(f'unlinked See Also entry: {text[:80]!r}')
                    continue
                for href in links.values:
                    try:
                        fragment = unquote(urlsplit(html.unescape(href)).fragment)
                    except ValueError:
                        errors.append(
                            f'{relative}: malformed See Also link target {href!r}'
                        )
                        continue
                    target = fragment.removeprefix('gometry.')
                    if (
                        not fragment.startswith('gometry.')
                        or target not in public_symbols
                    ):
                        errors.append(
                            f'{relative}: See Also link {href!r} does not target '
                            'a public API symbol'
                        )

    return errors


def main() -> int:
    tools = ROOT / 'tools'
    if str(tools) not in sys.path:
        sys.path.insert(0, str(tools))
    from _gatelib import prepend_tools_import_paths, require_runtime

    prepend_tools_import_paths()
    require_runtime(ROOT)
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
