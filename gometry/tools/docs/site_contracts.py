"""Check source-derived contracts in the documentation site output."""

from __future__ import annotations

import ast
import json
import re
import sys
from dataclasses import dataclass
from functools import cache
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True, slots=True)
class _Table:
    owner: str | None
    headers: tuple[str, ...]
    rows: tuple[tuple[str, ...], ...]


@dataclass(frozen=True, slots=True)
class _Link:
    page: Path
    label: str
    target: str
    contract_region: bool


@dataclass(slots=True)
class _Frame:
    tag: str
    subject: str | None
    is_doc_object: bool
    is_signature: bool
    is_parameter_row: bool


class _Tables(HTMLParser):
    """Collect docstring parameter tables and generated type-definition tables."""

    def __init__(self, *, definitions_page: bool = False) -> None:
        super().__init__(convert_charrefs=True)
        self.definitions_page = definitions_page
        self.stack: list[_Frame] = []
        self.definition: str | None = None
        self.tables: list[dict[str, Any]] = []
        self.table: dict[str, Any] | None = None
        self.row: list[str] | None = None
        self.row_is_header = False
        self.cell: list[str] | None = None
        self.cell_tag: str | None = None

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        attributes = dict(attrs)
        classes = frozenset((attributes.get('class') or '').split())
        subject = next(
            (frame.subject for frame in reversed(self.stack) if frame.subject),
            None,
        )
        if self.definitions_page and tag == 'h2':
            ident = attributes.get('id')
            self.definition = ident if ident and ident.startswith('gometry.') else None
        if (ident := attributes.get('id')) and ident.startswith('gometry.'):
            for frame in reversed(self.stack):
                if frame.is_doc_object:
                    frame.subject = ident
                    subject = ident
                    break

        is_signature = 'doc-signature' in classes or any(
            frame.is_signature for frame in self.stack
        )
        is_parameter_row = 'doc-section-item' in classes or any(
            frame.is_parameter_row for frame in self.stack
        )
        frame = _Frame(
            tag,
            subject,
            'doc-object' in classes,
            is_signature,
            is_parameter_row,
        )
        self.stack.append(frame)

        if tag == 'table':
            self.table = {
                'owner': self.definition if self.definitions_page else subject,
                'headers': (),
                'rows': [],
                'definition': self.definition,
            }
        elif tag == 'tr' and self.table is not None:
            self.row = []
            self.row_is_header = False
        elif tag in {'td', 'th'} and self.row is not None:
            self.cell = []
            self.cell_tag = tag
            self.row_is_header = self.row_is_header or tag == 'th'

    def handle_endtag(self, tag: str) -> None:
        if tag in {'td', 'th'} and self.cell is not None and self.row is not None:
            self.row.append(' '.join(''.join(self.cell).split()))
            self.cell = None
            self.cell_tag = None
        elif tag == 'tr' and self.row is not None and self.table is not None:
            if self.row_is_header:
                self.table['headers'] = tuple(self.row)
            elif self.row:
                self.table['rows'].append(tuple(self.row))
            self.row = None
            self.row_is_header = False
        elif tag == 'table' and self.table is not None:
            self.tables.append(self.table)
            self.table = None

        for index in range(len(self.stack) - 1, -1, -1):
            if self.stack[index].tag == tag:
                del self.stack[index:]
                break

    def handle_data(self, data: str) -> None:
        if self.cell is not None:
            self.cell.append(data)


class _Links(HTMLParser):
    def __init__(self, page: Path) -> None:
        super().__init__(convert_charrefs=True)
        self.page = page
        self.stack: list[_Frame] = []
        self.active: dict[str, Any] | None = None
        self.values: list[_Link] = []

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        attributes = dict(attrs)
        classes = frozenset((attributes.get('class') or '').split())
        subject = next(
            (frame.subject for frame in reversed(self.stack) if frame.subject),
            None,
        )
        if (ident := attributes.get('id')) and ident.startswith('gometry.'):
            for frame in reversed(self.stack):
                if frame.is_doc_object:
                    frame.subject = ident
                    subject = ident
                    break
        frame = _Frame(
            tag,
            subject,
            'doc-object' in classes,
            'doc-signature' in classes or any(item.is_signature for item in self.stack),
            'doc-section-item' in classes
            or any(item.is_parameter_row for item in self.stack),
        )
        self.stack.append(frame)
        if tag == 'a' and attributes.get('href') is not None:
            in_definition_table = self.page.as_posix().endswith(
                'api/types/index.html'
            ) and any(item.tag == 'table' for item in self.stack)
            contract_region = (
                frame.is_signature or frame.is_parameter_row or in_definition_table
            )
            self.active = {
                'href': attributes['href'] or '',
                'label': [],
                'contract_region': contract_region,
            }

    def handle_endtag(self, tag: str) -> None:
        if tag == 'a' and self.active is not None:
            self.values.append(
                _Link(
                    self.page,
                    ' '.join(''.join(self.active['label']).split()),
                    self.active['href'],
                    self.active['contract_region'],
                )
            )
            self.active = None
        for index in range(len(self.stack) - 1, -1, -1):
            if self.stack[index].tag == tag:
                del self.stack[index:]
                break

    def handle_data(self, data: str) -> None:
        if self.active is not None:
            self.active['label'].append(data)


def _expression_key(value: str | None) -> str | None:
    if value is None or not value.strip():
        return None
    try:
        expression = ast.parse(value, mode='eval').body
    except SyntaxError:
        return _flatten_display_union(' '.join(value.split()))

    def union_items(node: ast.expr) -> list[ast.expr]:
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
            return [*union_items(node.left), *union_items(node.right)]
        return [node]

    return ' | '.join(ast.unparse(item) for item in union_items(expression))


def _split_type_union(source: str) -> list[str]:
    parts = []
    quote: str | None = None
    escaped = False
    depth = 0
    start = 0
    for index, char in enumerate(source):
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
        elif char == '|' and depth == 0:
            parts.append(source[start:index].strip())
            start = index + 1
    parts.append(source[start:].strip())
    return parts


def _outer_group(source: str) -> str | None:
    if not source.startswith('(') or not source.endswith(')'):
        return None
    quote: str | None = None
    escaped = False
    depth = 0
    for index, char in enumerate(source):
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
            if depth == 0 and index != len(source) - 1:
                return None
    return source[1:-1].strip() if depth == 0 else None


def _flatten_display_union(source: str) -> str:
    def parts(expression: str) -> list[str]:
        result = []
        for part in _split_type_union(expression):
            inner = _outer_group(part)
            if inner is not None and len(_split_type_union(inner)) > 1:
                result.extend(parts(inner))
            else:
                result.append(part)
        return result

    return ' | '.join(parts(source))


@cache
def _module_aliases() -> frozenset[str]:
    aliases = set()
    package_root = ROOT / 'python' / 'gometry'
    for path in (*package_root.rglob('*.py'), *package_root.rglob('*.pyi')):
        if '__pycache__' in path.parts:
            continue
        tree = ast.parse(path.read_text(encoding='utf-8'))
        for statement in ast.walk(tree):
            if not isinstance(statement, ast.Import):
                continue
            aliases.update(
                item.asname or item.name.split('.', 1)[0] for item in statement.names
            )
    return frozenset(aliases)


def _strip_module_aliases(source: str) -> str:
    aliases = sorted(_module_aliases(), key=len, reverse=True)
    if not aliases:
        return source
    prefix = re.compile(
        r'(?<![A-Za-z0-9_])(?:'
        + '|'.join(re.escape(alias) for alias in aliases)
        + r')\.'
    )
    result = []
    quote: str | None = None
    escaped = False
    index = 0
    while index < len(source):
        char = source[index]
        if quote is not None:
            result.append(char)
            if escaped:
                escaped = False
            elif char == '\\':
                escaped = True
            elif char == quote:
                quote = None
            index += 1
            continue
        if char in {"'", '"'}:
            quote = char
            result.append(char)
            index += 1
            continue
        match = prefix.match(source, index)
        if match is not None:
            index = match.end()
            continue
        result.append(char)
        index += 1
    return ''.join(result)


def _brief_type(value: str | None) -> str | None:
    """Apply the handler's brief path to module-qualified type names."""
    if value is None:
        return None
    try:
        expression = ast.parse(value, mode='eval').body
    except SyntaxError:
        return _flatten_display_union(' '.join(_strip_module_aliases(value).split()))
    module_aliases = _module_aliases()

    def terminal_name(node: ast.expr) -> str | None:
        while isinstance(node, ast.Attribute):
            node = node.value
        return node.id if isinstance(node, ast.Name) else None

    class Brief(ast.NodeTransformer):
        def visit_Subscript(self, node: ast.Subscript) -> ast.expr:
            if terminal_name(node.value) == 'Literal':
                return ast.copy_location(
                    ast.Subscript(
                        value=self.visit(node.value),
                        slice=node.slice,
                        ctx=node.ctx,
                    ),
                    node,
                )
            return self.generic_visit(node)

        def visit_Attribute(self, node: ast.Attribute) -> ast.expr:
            if terminal_name(node) in module_aliases:
                return ast.copy_location(ast.Name(node.attr, ast.Load()), node)
            return self.generic_visit(node)

    return ast.unparse(Brief().visit(expression))


def _table_rows(pages: dict[Path, str]) -> list[_Table]:
    result = []
    for source in pages.values():
        parser = _Tables()
        parser.feed(source)
        parser.close()
        result.extend(
            _Table(
                table['owner'],
                table['headers'],
                tuple(table['rows']),
            )
            for table in parser.tables
        )
    return result


def _link_targets(pages: dict[Path, str]) -> list[_Link]:
    result = []
    for path, source in pages.items():
        parser = _Links(path)
        parser.feed(source)
        parser.close()
        result.extend(parser.values)
    return result


def _call_form_errors(
    contracts: Any,
    rendered: dict[str, tuple[Any, ...]],
) -> list[str]:
    errors = []
    expected = {
        path: tuple(
            (
                tuple(
                    (kind, name, _expression_key(_brief_type(annotation)), default)
                    for kind, name, annotation, default in parameters
                ),
                _expression_key(_brief_type(returns)),
            )
            for parameters, returns in forms
        )
        for path, forms in contracts.call_forms.items()
    }
    actual: dict[str, tuple[tuple[Any, ...], ...]] = {}
    for path, signatures in rendered.items():
        forms = []
        for signature in signatures:
            if signature.error is not None:
                errors.append(
                    f'{path}: cannot parse rendered signature: {signature.error}'
                )
            if signature.form is not None:
                forms.append((
                    tuple(
                        (kind, name, _expression_key(annotation), default)
                        for kind, name, annotation, default in signature.form.parameters
                    ),
                    _expression_key(signature.form.returns),
                ))
        if forms:
            actual[path] = tuple(forms)

    for path in sorted(expected.keys() | actual.keys()):
        expected_forms = expected.get(path, ())
        actual_forms = actual.get(path, ())
        if actual_forms != expected_forms:
            errors.append(
                f'{path}: rendered call forms differ from source declarations '
                f'(expected {expected_forms!r}, found {actual_forms!r})'
            )
    return errors


def _parameter_row_errors(
    contracts: Any,
    tables: list[_Table],
) -> list[str]:
    errors = []
    for path, expected in contracts.constructor_rows.items():
        candidates = [
            table
            for table in tables
            if table.owner == path
            and tuple(header.lower() for header in table.headers)
            == ('name', 'type', 'description', 'default')
        ]
        if len(candidates) != 1:
            errors.append(
                f'{path}: expected one rendered constructor parameter table, '
                f'found {len(candidates)}'
            )
            continue

        rows: dict[str, list[tuple[str, ...]]] = {}
        for row in candidates[0].rows:
            if row:
                rows.setdefault(row[0], []).append(row)
        if set(rows) != set(expected):
            errors.append(
                f'{path}: rendered constructor parameter names differ from source '
                f'(missing={sorted(set(expected) - rows.keys())}, '
                f'extra={sorted(rows.keys() - set(expected))})'
            )
        for name, (status, default, annotation) in expected.items():
            occurrences = rows.get(name, [])
            if len(occurrences) != 1:
                if occurrences:
                    errors.append(f'{path}.{name}: duplicate rendered parameter rows')
                continue
            row = occurrences[0]
            actual_annotation = _expression_key(row[1] if len(row) > 1 else None)
            expected_annotation = _expression_key(_brief_type(annotation))
            if actual_annotation != expected_annotation:
                errors.append(
                    f'{path}.{name}: rendered parameter type {actual_annotation!r} '
                    f'differs from source {expected_annotation!r}'
                )
            actual_default = row[3] if len(row) > 3 else ''
            expected_default = (
                default
                if status == 'defaulted'
                else 'varies by call form'
                if status == 'varies'
                else 'not applicable'
                if status == 'variadic'
                else 'required'
            )
            if actual_default != expected_default:
                errors.append(
                    f'{path}.{name}: rendered parameter default {actual_default!r} '
                    f'differs from source status {status!r} ({expected_default!r})'
                )
    return errors


def _schema_errors(
    package_root: Path,
    runtime_module: Any,
    pages: dict[Path, str],
    canonical_pages: dict[str, Path],
    site: Path,
) -> tuple[list[str], Any]:
    from reference_types import build_public_type_graph

    errors = []
    try:
        graph = build_public_type_graph(package_root, runtime_module)
    except Exception as error:
        return [f'cannot derive reference type definitions: {error}'], None

    definitions_page = Path('api/types/index.html')
    definition_ids = {
        declaration.path: canonical_pages.get(declaration.path)
        for declaration in graph.definitions
    }
    missing = sorted(
        path for path, page in definition_ids.items() if page != definitions_page
    )
    if missing:
        errors.append(f'reference type definitions missing from api/types: {missing}')

    source = pages.get(site / definitions_page)
    if source is None:
        return [*errors, 'api/types/index.html is missing'], graph
    parser = _Tables(definitions_page=True)
    parser.feed(source)
    parser.close()
    tables = [
        table
        for table in parser.tables
        if tuple(header.lower() for header in table['headers'])
        == ('field', 'type', 'required', 'description')
    ]
    declarations = {declaration.path: declaration for declaration in graph.definitions}
    for declaration in graph.definitions:
        if declaration.kind != 'typed-dict':
            continue
        matching = [
            table for table in tables if table['definition'] == declaration.path
        ]
        if len(matching) != 1:
            errors.append(
                f'{declaration.path}: expected one rendered schema field table, '
                f'found {len(matching)}'
            )
            continue
        errors.extend(
            _typed_dict_field_errors(
                declaration,
                declarations,
                graph,
                matching[0]['rows'],
            )
        )
    return errors, graph


def _typed_dict_field_errors(
    declaration: Any,
    declarations: dict[str, Any],
    graph: Any,
    rows: list[tuple[str, ...]],
) -> list[str]:
    from reference_types import _expression_source, _typed_dict_fields

    expected_fields = {
        name: (
            _expression_key(_expression_source(annotation, module, graph)),
            'yes' if required else 'no',
            ' '.join((description or '').split()),
        )
        for name, annotation, required, description, module in _typed_dict_fields(
            declaration,
            declarations,
        )
    }
    actual_fields = {
        row[0]: (
            _expression_key(row[1] if len(row) > 1 else None),
            row[2].lower() if len(row) > 2 else '',
            row[3] if len(row) > 3 else '',
        )
        for row in rows
        if row
    }
    if actual_fields == expected_fields:
        return []
    return [
        f'{declaration.path}: rendered schema fields differ from source '
        f'(expected {expected_fields!r}, found {actual_fields!r})'
    ]


def _local_target(href: str) -> str:
    try:
        return unquote(urlsplit(href).fragment)
    except ValueError:
        return ''


def _type_link_errors(
    graph: Any,
    links: list[_Link],
) -> list[str]:
    errors = []
    targets = {
        'Point': 'gometry.Point',
        'GeohashCell': 'gometry.GeohashCell',
        'FloatScalar': 'gometry._types.FloatScalar',
        'ScalarFloatArray': 'gometry._types.ScalarFloatArray',
        'GeoJsonFeature': 'gometry._types.GeoJsonFeature',
    }
    known = {declaration.path for declaration in graph.definitions}
    targets = {
        label: target
        for label, target in targets.items()
        if target.startswith('gometry.')
        and (target in known or label in {'Point', 'GeohashCell'})
    }
    for label, target in targets.items():
        candidates = [
            link for link in links if link.label == label and link.contract_region
        ]
        matching = [link for link in candidates if _local_target(link.target) == target]
        if not matching:
            errors.append(f'{label}: no signature or type-table link reaches {target}')
        wrong = [
            (link.page.as_posix(), _local_target(link.target))
            for link in candidates
            if _local_target(link.target).startswith('gometry.')
            and _local_target(link.target) != target
        ]
        if wrong:
            errors.append(f'{label}: links target the wrong declarations: {wrong}')
    return errors


def _search_path(location: str) -> tuple[str, str]:
    parsed = urlsplit(location)
    path = unquote(parsed.path).lstrip('/')
    if path.endswith('/index.html'):
        path = path.removesuffix('index.html')
    elif path == 'index.html':
        path = ''
    return path, unquote(parsed.fragment)


def _search_errors(
    site: Path,
    canonical_pages: dict[str, Path],
) -> list[str]:
    path = site / 'search' / 'search_index.json'
    if not path.is_file():
        return ['search/search_index.json is missing from the built site']
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError) as error:
        return [f'search/search_index.json cannot be read: {error}']
    documents = payload.get('docs') if isinstance(payload, dict) else None
    if not isinstance(documents, list):
        return ['search/search_index.json has no document list']

    targets = (
        ('Point', 'gometry.Point', 'point'),
        ('GeohashCell', 'gometry.GeohashCell', 'geohashcell'),
        ('FloatScalar', 'gometry._types.FloatScalar', 'floatscalar'),
        (
            'ScalarFloatArray',
            'gometry._types.ScalarFloatArray',
            'scalarfloatarray',
        ),
        ('GeoJsonFeature', 'gometry._types.GeoJsonFeature', 'geojsonfeature'),
        ('GeoJSON literal option', 'gometry.ParseError.format', 'geojson'),
    )
    errors = []
    for label, target, term in targets:
        page = canonical_pages.get(target)
        if page is None and target == 'gometry.ParseError.format':
            target = 'gometry.ParseError'
            page = canonical_pages.get(target)
        if page is None:
            errors.append(f'{label}: canonical search target {target} is missing')
            continue
        expected_path = page.as_posix()
        if expected_path.endswith('/index.html'):
            expected_path = expected_path.removesuffix('index.html')
        matching = []
        for document in documents:
            if not isinstance(document, dict):
                continue
            location = document.get('location')
            text = document.get('text')
            if not isinstance(location, str) or not isinstance(text, str):
                continue
            actual_path, fragment = _search_path(location)
            if (
                actual_path == expected_path
                and fragment == target
                and term in text.casefold()
            ):
                matching.append(document)
        if not matching:
            errors.append(
                f'{label}: search index has no entry for {target} containing {term!r}'
            )
    return errors


def collect_errors(
    site: Path,
    pages: dict[Path, str],
    rendered: dict[str, tuple[Any, ...]],
    canonical_pages: dict[str, Path],
    runtime_module: Any,
    package_root: Path,
) -> list[str]:
    """Compare visible forms, parameter tables, schemas, links, and search entries."""
    errors = []
    tools = package_root.parent.parent / 'tools'
    gates = tools / 'gates'
    for path in (tools, gates):
        if str(path) not in sys.path:
            sys.path.insert(0, str(path))
    try:
        from _check_doc_model import source_documentation_contracts

        contracts = source_documentation_contracts()
    except Exception as error:
        return [f'cannot derive source documentation contracts: {error}']

    errors.extend(_call_form_errors(contracts, rendered))
    errors.extend(_parameter_row_errors(contracts, _table_rows(pages)))
    schema_errors, graph = _schema_errors(
        package_root,
        runtime_module,
        pages,
        canonical_pages,
        site,
    )
    errors.extend(schema_errors)
    if graph is not None:
        links = _link_targets(pages)
        errors.extend(_type_link_errors(graph, links))
    errors.extend(_search_errors(site, canonical_pages))

    unresolved = re.compile(r'<span\b[^>]*title="(?P<target>gometry\.[^"]+)"[^>]*>')
    for path, source in pages.items():
        targets = sorted({
            match.group('target')
            for match in unresolved.finditer(source)
            if match.group('target') not in canonical_pages
        })
        if targets:
            errors.append(
                f'{path.relative_to(site)}: unresolved in-package type targets: '
                f'{targets}'
            )
    return errors
