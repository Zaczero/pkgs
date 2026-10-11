"""Render the source-derived reference type graph as one API page."""

from __future__ import annotations

import ast
import html
import re

from reference_types import (
    TypeGraph,
    _Declaration,
    _expression_source,
    _Module,
    _resolve_name,
    _typed_dict_fields,
)


def _humanized_name(name: str) -> str:
    words = re.sub(r'([a-z0-9])([A-Z])', r'\1 \2', name.lstrip('_'))
    return (
        words
        .replace('Geo Json', 'GeoJSON')
        .replace('Crs ', 'CRS ')
        .replace('Cf ', 'CF ')
        .replace('Wkt ', 'WKT ')
        .replace('Wkb ', 'WKB ')
    )


def _meaning(
    declaration: _Declaration,
    graph: TypeGraph,
) -> str:
    if declaration.module.name == 'gometry._lib':
        owners = graph.public_owners.get(declaration.path, ())
        if owners:
            owner = owners[0].removeprefix('gometry.')
            if 'iterator' in declaration.name.casefold():
                return f'Iteration results from {owner}'
            if declaration.kind == 'typevar':
                name = declaration.name
                if 'GroupKey' in name:
                    meaning = 'Group key element type'
                elif 'Default' in name:
                    meaning = 'Default result type'
                elif 'Row' in name:
                    meaning = 'Geometry row type'
                elif 'Other' in name:
                    meaning = 'Other geometry element type'
                elif 'Cell' in name:
                    meaning = 'Cell element type'
                else:
                    meaning = 'Generic element type'
                if _typevar_variance(declaration) == 'covariant':
                    meaning = f'Covariant {meaning[0].lower()}{meaning[1:]}'
                return f'{meaning} for {owner}'
            if declaration.kind == 'protocol':
                if 'Token' in declaration.name:
                    return f'Token access behavior used by {owner}'
                if 'Geometry' in declaration.name:
                    return f'Geometry access behavior used by {owner}'
                return f'Compatible object behavior used by {owner}'
            if declaration.kind == 'class':
                return f'Geometry values used by {owner}'
            return f'Signature value shape used by {owner}'
        return 'Public signature values'

    name = _humanized_name(declaration.name)
    if declaration.kind == 'typed-dict':
        return f'{name} mapping'
    if declaration.kind == 'protocol':
        return f'{name} object behavior'
    if declaration.kind == 'namedtuple':
        return f'{name} record shape'
    if declaration.kind == 'typevar':
        return 'Generic element type'
    if declaration.kind == 'alias':
        return f'{name} annotation shape'
    return f'{name} value shape'


def _owner_links(declaration: _Declaration, graph: TypeGraph) -> str:
    owners = [
        owner
        for owner in graph.public_owners.get(declaration.path, ())
        if owner.startswith('gometry.')
    ]
    links = [f'[{owner.removeprefix("gometry.")}][{owner}]' for owner in owners[:4]]
    if len(owners) > len(links):
        links.append(f'and {len(owners) - len(links)} more')
    return ', '.join(links)


def _public_type_links(declaration: _Declaration, graph: TypeGraph) -> str:
    targets = [
        target
        for target in graph.references.get(declaration.path, ())
        if target.startswith('gometry.')
        and '._lib.' not in target
        and '._types.' not in target
    ]
    return ', '.join(
        f'[{target.removeprefix("gometry.")}][{target}]' for target in targets
    )


def _related_shape_links(
    declaration: _Declaration,
    graph: TypeGraph,
    declarations: dict[str, _Declaration],
) -> str:
    links = [
        f'[{_meaning(referenced, graph)}][{owner}]'
        for target in graph.references.get(declaration.path, ())
        if (referenced := declarations.get(target)) is not None
        for owner in graph.public_owners.get(target, ())
        if owner.startswith('gometry.')
    ]
    return ', '.join(list(dict.fromkeys(links))[:4])


def _annotation_shape(
    expression: ast.expr,
    module: _Module,
    graph: TypeGraph,
    declarations: dict[str, _Declaration],
) -> tuple[str, bool]:
    private_names: dict[str, str] = {}
    for node in ast.walk(expression):
        if not isinstance(node, ast.Name):
            continue
        target = _resolve_name(module, node)
        referenced = declarations.get(target or '')
        if referenced is None or target in graph.public_targets:
            continue
        private_names[node.id] = _meaning(referenced, graph)

    value = _expression_source(expression, module, graph)
    for name, meaning in sorted(
        private_names.items(),
        key=lambda item: len(item[0]),
        reverse=True,
    ):
        value = re.sub(rf'\b{re.escape(name)}\b', meaning, value)
    return value, bool(private_names)


def _code_expression(expression: str) -> str:
    escaped = html.escape(expression, quote=False).replace('|', '&#124;')
    return f'<code>{escaped}</code>'


def _definition_prose(
    declaration: _Declaration,
    graph: TypeGraph,
) -> str | None:
    if declaration.module.name != 'gometry._types' or not declaration.prose:
        return None
    prose = re.sub(
        r'gometry\._(?:lib|types)\.([A-Za-z_]\w*)',
        lambda match: _humanized_name(match.group(1)),
        declaration.prose.strip(),
    )
    for path in graph.references.get(declaration.path, ()):
        referenced = next(
            (item for item in graph.definitions if item.path == path),
            None,
        )
        if referenced is None or path in graph.public_targets:
            continue
        prose = re.sub(
            rf'\b{re.escape(referenced.name)}\b',
            _meaning(referenced, graph),
            prose,
        )
    return prose


def _typevar_parts(
    declaration: _Declaration,
) -> tuple[list[ast.expr], ast.expr | None, ast.expr | None]:
    call = declaration.expression
    if not isinstance(call, ast.Call):
        return [], None, None
    constraints = list(call.args[1:])
    bound = next((item.value for item in call.keywords if item.arg == 'bound'), None)
    default = next(
        (item.value for item in call.keywords if item.arg == 'default'), None
    )
    return constraints, bound, default


def _typevar_variance(declaration: _Declaration) -> str | None:
    call = declaration.expression
    if not isinstance(call, ast.Call):
        return None
    values = {
        keyword.arg: keyword.value
        for keyword in call.keywords
        if keyword.arg in {'covariant', 'contravariant', 'infer_variance'}
    }
    if (
        isinstance(values.get('infer_variance'), ast.Constant)
        and values['infer_variance'].value
    ):
        return 'inferred'
    if isinstance(values.get('covariant'), ast.Constant) and values['covariant'].value:
        return 'covariant'
    if (
        isinstance(values.get('contravariant'), ast.Constant)
        and values['contravariant'].value
    ):
        return 'contravariant'
    return 'invariant'


def render_type_page(graph: TypeGraph) -> str:
    """Render public type meanings while preserving legacy fragment anchors."""
    declarations = {declaration.path: declaration for declaration in graph.definitions}
    lines = [
        '---',
        'api_reference: true',
        'description: "Public annotation forms, result records, and structured values."',
        '---',
        '',
        '# Public annotation forms',
        '',
        'Public signatures show the accepted and returned annotation forms. This',
        'page explains those forms and links to the API operations that own them;',
        'its descriptive labels are not additional import names. Other aliases',
        'and structural types follow the signature where they are used; they do',
        'not have standalone `gometry` imports.',
        '',
        'The named result records `Cell`, `Extremes`, `Features`, and',
        '`PolygonizeResult` are importable with:',
        '',
        '```python',
        'from gometry import Cell, Extremes, Features, PolygonizeResult',
        '```',
        '',
    ]
    for declaration in graph.definitions:
        meaning = _meaning(declaration, graph)
        lines.extend([
            f'<span class="reference-type-anchor" id="{html.escape(declaration.path, quote=True)}" aria-hidden="true"></span>',
            f'## {meaning}',
            '',
        ])
        if prose := _definition_prose(declaration, graph):
            lines.extend([prose, ''])

        if owners := _owner_links(declaration, graph):
            lines.extend([f'**Used by:** {owners}', ''])
        if public_types := _public_type_links(declaration, graph):
            lines.extend([f'**Public types:** {public_types}', ''])
        if related_shapes := _related_shape_links(
            declaration,
            graph,
            declarations,
        ):
            lines.extend([f'**Related meanings:** {related_shapes}', ''])

        if declaration.kind == 'alias' and declaration.expression is not None:
            expression, contains_private_names = _annotation_shape(
                declaration.expression,
                declaration.module,
                graph,
                declarations,
            )
            label = 'Shape' if contains_private_names else 'Annotation shape'
            lines.extend([
                f'**{label}:** {_code_expression(expression)}',
                '',
            ])
        elif declaration.kind == 'typevar':
            constraints, bound, default = _typevar_parts(declaration)
            lines.extend([
                f'**Variance:** {_typevar_variance(declaration)}',
                '',
            ])
            if constraints:
                values = ', '.join(
                    _code_expression(
                        _annotation_shape(
                            item,
                            declaration.module,
                            graph,
                            declarations,
                        )[0]
                    )
                    for item in constraints
                )
                lines.extend([f'**Allowed values:** {values}', ''])
            if bound is not None:
                value = _annotation_shape(
                    bound,
                    declaration.module,
                    graph,
                    declarations,
                )[0]
                lines.extend([f'**Bound:** {_code_expression(value)}', ''])
            if default is not None:
                value = _annotation_shape(
                    default,
                    declaration.module,
                    graph,
                    declarations,
                )[0]
                lines.extend([
                    f'**Default element type:** {_code_expression(value)}',
                    '',
                ])
        elif declaration.kind == 'typed-dict':
            lines.extend([
                '| Field | Value shape | Required | Description |',
                '|---|---|---|---|',
            ])
            for name, annotation, required, description, module in _typed_dict_fields(
                declaration,
                declarations,
            ):
                expression, _ = _annotation_shape(
                    annotation,
                    module,
                    graph,
                    declarations,
                )
                text = html.escape(description or '', quote=False).replace(
                    '|', '&#124;'
                )
                lines.append(
                    f'| `{name}` | {_code_expression(expression)} | '
                    f'{"yes" if required else "no"} | {text} |'
                )
            lines.append('')
        elif declaration.kind in {'protocol', 'namedtuple', 'class'} and isinstance(
            declaration.node,
            ast.ClassDef,
        ):
            class_node = declaration.node
            if declaration.kind == 'protocol':
                lines.extend([
                    '**Required behavior:**',
                    '',
                ])
                for member in class_node.body:
                    if isinstance(member, ast.AnnAssign) and isinstance(
                        member.target, ast.Name
                    ):
                        lines.append(f'- `{member.target.id}`')
                    elif isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        lines.append(f'- `{member.name}()`')
                lines.append('')
            else:
                lines.extend([
                    '| Field | Value shape |',
                    '|---|---|',
                ])
                for member in class_node.body:
                    if isinstance(member, ast.AnnAssign) and isinstance(
                        member.target, ast.Name
                    ):
                        value, _ = _annotation_shape(
                            member.annotation,
                            declaration.module,
                            graph,
                            declarations,
                        )
                        lines.append(
                            f'| `{member.target.id}` | {_code_expression(value)} |'
                        )
                lines.append('')
    return '\n'.join(lines).rstrip() + '\n'
