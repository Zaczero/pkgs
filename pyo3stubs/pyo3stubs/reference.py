"""Render the public configuration and gate reference from their owners."""

from __future__ import annotations

import dataclasses
import inspect
import re
import types
from dataclasses import MISSING, Field
from pathlib import Path
from typing import Any, Union, cast, get_args, get_origin, get_type_hints

import pyo3stubs
from pyo3stubs import config as config_module
from pyo3stubs.gates import GATES, Status


def _parameter_docs(doc: str) -> dict[str, str]:
    """Read field prose from a config class's NumPy-style ``Parameters``."""
    lines = inspect.cleandoc(doc).splitlines()
    try:
        section = lines.index('Parameters')
    except ValueError:
        return {}
    if section + 1 >= len(lines) or not set(lines[section + 1]) <= {'-'}:
        return {}

    result: dict[str, str] = {}
    name: str | None = None
    description: list[str] = []
    for line in lines[section + 2 :]:
        match = re.fullmatch(r'([A-Za-z_]\w*)\s*(?::.*)?', line)
        if match:
            if name is not None:
                result[name] = ' '.join(part.strip() for part in description).strip()
            name = match.group(1)
            description = []
        elif name is not None and line.strip():
            description.append(line.strip())
    if name is not None:
        result[name] = ' '.join(part.strip() for part in description).strip()
    return result


def _type_name(annotation: object) -> str:
    if annotation is type(None):
        return 'None'
    if annotation is Ellipsis:
        return '...'
    origin = get_origin(annotation)
    if origin is None:
        if isinstance(annotation, type):
            return annotation.__name__
        return str(annotation).replace('typing.', '')

    args = get_args(annotation)
    if origin in (Union, types.UnionType):
        return ' | '.join(_type_name(arg) for arg in args)
    origin_name = getattr(origin, '__name__', str(origin).replace('typing.', ''))
    if not args:
        return origin_name
    return f'{origin_name}[{", ".join(_type_name(arg) for arg in args)}]'


def _default(field: Field[object]) -> str:
    if field.default is not MISSING:
        return repr(field.default)
    if field.default_factory is not MISSING:
        factory = field.default_factory
        name = getattr(factory, '__name__', None)
        return f'{name}()' if name else 'a new value from the configured factory'
    return 'required'


def _class_summary(cls: type[object]) -> str:
    doc = inspect.getdoc(cls) or ''
    return _markdown_text(doc.split('\n\n', maxsplit=1)[0].replace('\n', ' '))


def _markdown_text(text: str) -> str:
    text = re.sub(r':(?:class|data|func):`~?([^`]+)`', r'`\1`', text)
    return text.replace('``', '`').strip()


def _configuration_classes() -> tuple[type[object], ...]:
    """Public config records, in the order declared by the package exports."""
    return tuple(
        value
        for name in pyo3stubs.__all__
        if isinstance(value := getattr(config_module, name, None), type)
        and dataclasses.is_dataclass(value)
    )


def _fields(cls: type[object]) -> tuple[tuple[str, str, str, str], ...]:
    hints = get_type_hints(cls, localns={'Path': Path})
    descriptions = _parameter_docs(cls.__doc__ or '')
    return tuple(
        (
            field.name,
            _type_name(hints[field.name]),
            _default(field),
            descriptions.get(field.name, ''),
        )
        for field in dataclasses.fields(cast('Any', cls))
    )


def _field_reference(cls: type[object]) -> list[str]:
    lines = [f'### `{cls.__name__}`', '', _class_summary(cls), '']
    for name, annotation, default, description in _fields(cls):
        requirement = 'Required' if default == 'required' else f'Default: `{default}`'
        lines.extend((
            f'#### `{cls.__name__}.{name}`',
            '',
            f'`{annotation}` · {requirement}',
            '',
        ))
        if description:
            lines.append(_markdown_text(description))
        lines.append('')
    return lines


def _other_exports() -> list[str]:
    lines = ['## Other public exports', '']
    records = {cls.__name__ for cls in _configuration_classes()}
    for name in pyo3stubs.__all__:
        if name in records:
            continue
        if name == 'Reasons':
            lines.extend((
                f'### `{name}`',
                '',
                'A `dict[str, str]` mapping each allowlisted or deliberately '
                'divergent name to its reason.',
                '',
            ))
        elif name == 'DEFAULT_IGNORED_TYPE_NAMES':
            values = ', '.join(
                f'`{value}`'
                for value in sorted(config_module.DEFAULT_IGNORED_TYPE_NAMES)
            )
            lines.extend((
                f'### `{name}`',
                '',
                'Annotation names omitted by the leaked-types scan. Add '
                'project-specific names with '
                '`StubConfig.extra_ignored_type_names`.',
                '',
                f'Current names: {values}.',
                '',
            ))
        elif name == 'render_stub_with_docs':
            from pyo3stubs.gen import render_stub_with_docs

            summary = (inspect.getdoc(render_stub_with_docs) or '').split(
                '\n\n', maxsplit=1
            )[0]
            lines.extend((
                f'### `{name}`',
                '',
                f'`render_stub_with_docs{inspect.signature(render_stub_with_docs)}`',
                '',
                _markdown_text(summary),
                '',
                '- Runtime dependency: LibCST (declared for CPython).',
                '',
            ))
    return lines


def _gate_reference() -> list[str]:
    lines = [
        '## Gates',
        '',
        'Gates run by default unless they require an optional configuration below. '
        'CPython-only gates report a skip on other interpreters.',
        '',
    ]
    for entry in GATES:
        requirements = ', '.join(f'`{item}`' for item in entry.requirements)
        lines.extend((
            f'### `{entry.name}`',
            '',
            entry.summary[0].upper() + entry.summary[1:] + '.',
            '',
        ))
        if entry.activated_by:
            lines.append(f'- Enable with `StubConfig.{entry.activated_by}`.')
        if requirements:
            lines.append(f'- Requires: {requirements}.')
        if entry.requires_cpython:
            lines.append(f'- CPython only: {entry.cpython_reason}.')
        lines.extend((f'- On failure: {entry.remedy}.', ''))
    return lines


def render_reference() -> str:
    """Return the anchorable configuration, export, gate, and status reference."""
    lines = [
        '# Configuration and gate reference',
        '',
        'This reference describes the `pyo3stubs` configuration fields, CLI '
        'gates, and result statuses.',
        '',
        'Your configuration file exposes `config() -> StubConfig` or a `CONFIG` '
        'value. It runs as ordinary Python code, so load only trusted files. '
        '`stub_path` must name an existing file and `src_root` an existing directory.',
        '',
        'The `reference` command requires LibCST.',
        '',
        'Build and install your extension in the environment running the checks. '
        'See the [quickstart](README.md#first-check) for setup.',
        '',
        '## Configuration records',
        '',
    ]
    for cls in _configuration_classes():
        lines.extend(_field_reference(cls))

    lines.extend(_other_exports())
    lines.extend(_gate_reference())
    lines.extend(('## Result statuses', ''))
    for status in Status:
        lines.extend((f'### `{status.value}`', '', status.description, ''))
    lines.extend((
        '## Command and pytest outcomes',
        '',
        '`pyo3stubs check-all` exits with status 1 when any gate reports a '
        'violation and 0 otherwise. A zero exit status does not mean every '
        'gate examined a subject: disabled, skipped, inactive, and '
        '`nothing to check` outcomes are reported separately and do not '
        'change the CLI exit status. The pytest adapter skips those same '
        'non-violation outcomes with their reason; only `ok` is a pytest '
        'pass.',
        '',
        '`pyo3stubs gen-docs` writes the rendered stub even when the '
        '`doc-contract` gate reports violations. Its nonzero status reports '
        'the contract failure; the generated file remains available for '
        'editing.',
        '',
    ))
    return '\n'.join(lines)
