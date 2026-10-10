"""Render the source-derived reference type graph as one API page."""

from __future__ import annotations

import ast
import html

from reference_types import (
    TypeGraph,
    _Declaration,
    _expression_source,
    _typed_dict_fields,
)


def _markdown_reference(path: str, graph: TypeGraph) -> str:
    label = path.rsplit('.', 1)[-1]
    if any(declaration.path == path for declaration in graph.definitions):
        return f'[`{label}`](#{path})'
    return f'[`{label}`][{path}]'


def _code_expression(expression: str) -> str:
    escaped = html.escape(expression, quote=False).replace('|', '&#124;')
    return f'<code>{escaped}</code>'


def _definition_prose(declaration: _Declaration) -> str | None:
    return declaration.prose.strip() if declaration.prose else None


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
    """Render reachable definitions with qualified, stable anchors."""
    declarations = {declaration.path: declaration for declaration in graph.definitions}
    lines = [
        '---',
        'api_reference: true',
        'description: "Source definitions for named types used by public signatures."',
        '---',
        '',
        '# Reference type definitions',
        '',
        'These definitions explain named types used by the API signatures. Names are',
        'shown at their actual declaration, such as `gometry._types.FloatScalar`.',
        'They are source references, not additional top-level exports.',
        '',
    ]
    for declaration in graph.definitions:
        source = (
            f'{declaration.module.name}{declaration.module.path.suffix}:'
            f'{declaration.line}'
        )
        lines.extend([
            f'## `{declaration.path}` {{ #{declaration.path} }}',
            '',
            f'**Kind:** {declaration.kind}  ',
            f'**Source:** `{source}`',
            '',
        ])
        if prose := _definition_prose(declaration):
            lines.extend([prose, ''])

        if declaration.kind == 'alias' and declaration.expression is not None:
            expression = _expression_source(
                declaration.expression,
                declaration.module,
                graph,
            )
            lines.extend([
                '```python',
                f'{declaration.name}: TypeAlias = {expression}',
                '```',
                '',
            ])
        elif declaration.kind == 'typevar':
            constraints, bound, default = _typevar_parts(declaration)
            lines.extend(['`TypeVar`', ''])
            lines.extend([f'**Variance:** {_typevar_variance(declaration)}', ''])
            if constraints:
                values = ', '.join(
                    _code_expression(
                        _expression_source(item, declaration.module, graph)
                    )
                    for item in constraints
                )
                lines.extend([f'**Constraints:** {values}', ''])
            if bound is not None:
                value = _expression_source(bound, declaration.module, graph)
                lines.extend([f'**Bound:** {_code_expression(value)}', ''])
            if default is not None:
                value = _expression_source(default, declaration.module, graph)
                lines.extend([f'**Default:** {_code_expression(value)}', ''])
        elif declaration.kind == 'typed-dict':
            lines.extend([
                '| Field | Type | Required | Description |',
                '|---|---|---|---|',
            ])
            for name, annotation, required, description, module in _typed_dict_fields(
                declaration,
                declarations,
            ):
                expression = _expression_source(annotation, module, graph)
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
                    '| Member | Contract | Description |',
                    '|---|---|---|',
                ])
                for member in class_node.body:
                    if isinstance(member, ast.AnnAssign) and isinstance(
                        member.target, ast.Name
                    ):
                        annotation = _expression_source(
                            member.annotation,
                            declaration.module,
                            graph,
                        )
                        lines.append(
                            f'| `{member.target.id}` | {_code_expression(annotation)} |  |'
                        )
                    elif isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        signature = ast.unparse(member.args)
                        returns = (
                            _expression_source(
                                member.returns,
                                declaration.module,
                                graph,
                            )
                            if member.returns is not None
                            else 'Any'
                        )
                        lines.append(
                            f'| `{member.name}` | '
                            f'{_code_expression(f"{member.name}{signature} -> {returns}")} |  |'
                        )
                lines.append('')
            else:
                bases = ', '.join(
                    _expression_source(base, declaration.module, graph)
                    for base in class_node.bases
                )
                lines.extend([
                    '```python',
                    f'class {declaration.name}({bases}):',
                ])
                for member in class_node.body:
                    if isinstance(member, ast.AnnAssign) and isinstance(
                        member.target, ast.Name
                    ):
                        value = (
                            f' = {ast.unparse(member.value)}'
                            if member.value is not None
                            else ''
                        )
                        lines.append(
                            f'    {member.target.id}: '
                            f'{_expression_source(member.annotation, declaration.module, graph)}'
                            f'{value}'
                        )
                if len(lines) >= 2 and lines[-1].startswith('class '):
                    lines.append('    ...')
                lines.extend(['```', ''])

        if targets := graph.references.get(declaration.path):
            lines.append(
                '**Referenced types:** '
                + ', '.join(_markdown_reference(target, graph) for target in targets)
            )
            lines.append('')
    return '\n'.join(lines).rstrip() + '\n'
