"""Docstring *structure*: callable and descriptor prose matches its contract.

``doc-contract`` owns presence — every public symbol has prose. This owns what
that prose has to contain, which is the half that rots silently:

* every callable signature parameter appears in ``Parameters``;
* no documented parameter has left the signature (stale docs fail);
* a ``Returns`` section exists for callables and annotated data descriptors;
* a ``default X`` stated in prose matches the runtime default.

Sections are parsed format-agnostically — numpydoc (``Parameters`` over a
``----------`` underline) and Google (``Args:``) are both understood — so the
gate accepts either unless ``docstring_style`` selects a package convention.

Domain rules stay with their package: a ``Raises`` taxonomy names a project's
own exception classes and operation families, and does not travel.
"""

from __future__ import annotations

import ast
import inspect
import re
from dataclasses import dataclass
from types import ModuleType
from typing import TYPE_CHECKING, Any, Literal, cast

from pyo3stubs.ast_util import (
    annotated_data_descriptors,
    class_prohibits_construction,
    decorator_names,
    function_groups,
    is_synthetic_doc,
    passive_class_member,
    runtime_doc_for_stub,
)
from pyo3stubs.context import CheckContext
from pyo3stubs.report import Findings, unused_allowlist_errors

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence

    from pyo3stubs.config import StubConfig
    from pyo3stubs.rust_scan import ConstructorDocSource

_NUMPYDOC_SECTION = re.compile(r'^([A-Za-z][A-Za-z ]*)\n-+\n', re.MULTILINE)
_GOOGLE_SECTION = re.compile(r'^([A-Za-z][A-Za-z ]*):[ \t]*$', re.MULTILINE)
_DEFAULT_MARKER = re.compile(
    r'\bdefaults?(?:\s+to\b|\s*[:=]\s*|\s+)',
    re.IGNORECASE,
)
_ENTRY_NAMES = re.compile(r'[*\w]+(?:\s*,\s*[*\w]+)*')
_SECTION_ALIASES = {'Args': 'Parameters', 'Arguments': 'Parameters'}
_PARAMETER_SECTIONS = frozenset({
    'Parameters',
    'Other Parameters',
    'Keyword Arguments',
})
_NAMED_TYPE_SECTIONS = _PARAMETER_SECTIONS | {'Attributes'}
_TYPED_VALUE_SECTIONS = frozenset({'Returns', 'Yields', 'Receives'})
_OUTPUT_SECTIONS = frozenset({'Returns', 'Yields'})
_STYLE_SECTIONS = (
    _NAMED_TYPE_SECTIONS
    | _TYPED_VALUE_SECTIONS
    | {
        'Args',
        'Arguments',
        'Keyword Args',
        'Raises',
        'Warns',
        'Warnings',
        'See Also',
        'Notes',
        'References',
        'Examples',
        'Methods',
    }
)


def doc_style_errors(prose: str, style: Literal['numpy', 'google']) -> list[str]:
    """Reject foreign section syntax without requiring sections in short prose."""
    lines = inspect.cleandoc(prose).splitlines()
    visible: list[str] = []
    fence: str | None = None
    for line in lines:
        stripped = line.lstrip()
        if stripped.startswith(('```', '~~~')):
            marker = stripped[0]
            if fence is None:
                fence = marker
            elif fence == marker:
                fence = None
            visible.append('')
        else:
            visible.append(line if fence is None else '')
    text = '\n'.join(visible) + '\n'
    foreign = _GOOGLE_SECTION if style == 'numpy' else _NUMPYDOC_SECTION
    return [
        f'{match.group(1)}: expected {style}-style section syntax'
        for match in foreign.finditer(text)
        if match.group(1).title() in _STYLE_SECTIONS
    ]


@dataclass(frozen=True, slots=True)
class DocSection:
    """One ordered section in cleaned docstring prose."""

    name: str
    body: str
    line: int


@dataclass(frozen=True, slots=True)
class DocEntry:
    """One parameter, exception, return, or named-reference entry."""

    names: tuple[str, ...]
    type_text: str
    description: str
    default_text: str | None
    line: int


@dataclass(frozen=True, slots=True)
class ParsedDoc:
    """An ordered parse of either NumPy or Google docstring sections."""

    sections: tuple[DocSection, ...]
    style: Literal['numpy', 'google']


@dataclass(frozen=True, slots=True)
class DocLocation:
    path: str
    line: int


@dataclass(frozen=True, slots=True)
class DocMemberSource:
    """One effective Python source declaration for a public class member."""

    node: ast.FunctionDef | ast.AsyncFunctionDef | ast.AnnAssign
    kind: Literal['callable', 'property', 'attribute', 'schema']
    prose: str | None
    source: DocLocation


@dataclass(frozen=True, slots=True)
class DocInitializationSource:
    """The one class-call documentation carrier selected from Python source."""

    node: ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef
    prose: str | None
    source: DocLocation


@dataclass(frozen=True, slots=True)
class DocClassSource:
    """A facade class declaration, one init carrier, and effective members."""

    node: ast.ClassDef
    source: DocLocation
    members: Mapping[str, DocMemberSource]
    initialization: DocInitializationSource | None = None


@dataclass(frozen=True, slots=True)
class DocSubject:
    """One public doc carrier and the binding signature it describes."""

    qualname: str
    kind: Literal[
        'callable',
        'property',
        'constructor',
        'class',
        'attribute',
        'schema',
    ]
    prose: str | None
    parameters: tuple[inspect.Parameter, ...] | None
    source: DocLocation
    declarations: tuple[ast.AST, ...] = ()


@dataclass(frozen=True, slots=True)
class SignatureParameter:
    """One parameter occurrence in an exact declared call form."""

    name: str
    kind: str
    annotation: str | None
    default: str | None


@dataclass(frozen=True, slots=True)
class ParameterOccurrence:
    """One parameter's status in one call form; ``None`` means absent."""

    kind: str
    annotation: str | None
    default: str | None
    status: Literal['required', 'defaulted', 'variadic']


@dataclass(frozen=True, slots=True)
class ResolvedParameter:
    """One prose row reconciled with every exact call-form occurrence."""

    name: str
    annotation: str | None
    description: str
    status: Literal['required', 'defaulted', 'variadic', 'varies']
    default: str | None
    occurrences: tuple[ParameterOccurrence | None, ...]


@dataclass(frozen=True, slots=True)
class ParameterResolution:
    """Merged parameter rows plus source-located conflicts and mismatches."""

    rows: tuple[ResolvedParameter, ...]
    errors: tuple[str, ...]


def _rust_constructor_sources(cfg: StubConfig) -> dict[str, ConstructorDocSource]:
    """Map exported classes to retained Rust constructor-doc carriers."""
    from pyo3stubs.rust_scan import constructor_doc_sources

    return constructor_doc_sources(cfg).by_class


def parse_doc(doc: str | None) -> ParsedDoc:
    """Parse ordered NumPy- or Google-style sections from cleaned prose."""
    cleaned = inspect.cleandoc(doc or '')
    patterns: tuple[tuple[re.Pattern[str], Literal['numpy', 'google']], ...] = (
        (_NUMPYDOC_SECTION, 'numpy'),
        (_GOOGLE_SECTION, 'google'),
    )
    for pattern, style in patterns:
        matches = list(pattern.finditer(cleaned))
        if not matches:
            continue
        sections: list[DocSection] = []
        for index, match in enumerate(matches):
            end = (
                matches[index + 1].start() if index + 1 < len(matches) else len(cleaned)
            )
            raw_name = match.group(1).strip()
            name = _SECTION_ALIASES.get(raw_name, raw_name)
            sections.append(
                DocSection(
                    name=name,
                    body=cleaned[match.end() : end],
                    line=cleaned.count('\n', 0, match.start()) + 1,
                )
            )
        return ParsedDoc(tuple(sections), style)
    return ParsedDoc((), 'numpy')


def _default_markers(
    text: str,
    *,
    allow_parenthesized: bool,
) -> Iterator[re.Match[str]]:
    """Yield default markers outside quoted and nested type values."""
    square_depth = 0
    brace_depth = 0
    paren_depth = 0
    quote: str | None = None
    escaped = False
    index = 0
    while index < len(text):
        char = text[index]
        if quote is not None:
            if escaped:
                escaped = False
            elif char == '\\':
                escaped = True
            elif char == quote:
                quote = None
            index += 1
            continue
        if char in {'"', "'"}:
            quote = char
        elif char == '[':
            square_depth += 1
        elif char == ']':
            square_depth = max(0, square_depth - 1)
        elif char == '{':
            brace_depth += 1
        elif char == '}':
            brace_depth = max(0, brace_depth - 1)
        elif char == '(':
            paren_depth += 1
        elif char == ')':
            paren_depth = max(0, paren_depth - 1)
        elif square_depth == brace_depth == 0:
            match = _DEFAULT_MARKER.match(text, index)
            top_level = paren_depth == 0
            parenthesized = (
                allow_parenthesized
                and paren_depth == 1
                and text[:index].rstrip().endswith('(')
            )
            if match is not None and (top_level or parenthesized):
                yield match
                index = match.end()
                continue
        index += 1


def _default_clause(
    text: str,
    *,
    description: bool,
) -> tuple[str, str] | None:
    """Split a supported trailing default claim without reading surrounding prose."""
    matches = tuple(_default_markers(text, allow_parenthesized=not description))
    for match in reversed(matches):
        prefix = text[: match.start()].rstrip()
        marker = match.group().lower()
        if description:
            if (
                prefix
                and not prefix.endswith((',', ';'))
                and 'to' not in marker
                and '=' not in marker
            ):
                continue
        elif prefix and not prefix.endswith((',', '(')):
            continue
        value = text[match.end() :].strip()
        if prefix.endswith('(') and value.endswith(')'):
            value = value[:-1].rstrip()
        if value.endswith('.') and not value.endswith('..'):
            value = value[:-1].rstrip()
        if value:
            return prefix.rstrip(' ,('), value
    return None


def _entry_names(text: str) -> tuple[str, ...]:
    names = tuple(part.strip().strip('`').strip() for part in text.split(','))
    normalized = ', '.join(names)
    if _ENTRY_NAMES.fullmatch(normalized) is None:
        return ()
    return tuple(name.lstrip('*') for name in names if name)


def _entry_indent(line: str) -> int:
    return len(line) - len(line.lstrip(' \t'))


def _top_level_separator(text: str, separator: str) -> int | None:
    """Find a separator outside balanced brackets and quoted literals."""
    closing: list[str] = []
    quote: str | None = None
    escaped = False
    pairs = {'(': ')', '[': ']', '{': '}'}
    for index, char in enumerate(text):
        if quote is not None:
            if escaped:
                escaped = False
            elif char == '\\':
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char in {'"', "'"}:
            quote = char
        elif char in pairs:
            closing.append(pairs[char])
        elif char in ')]}':
            if not closing or char != closing.pop():
                return None
        elif char == separator and not closing:
            return index
    return None


def _matching_delimiter(text: str, start: int) -> int | None:
    """Index the matching close for one opening delimiter, respecting quotes."""
    pairs = {'(': ')', '[': ']', '{': '}'}
    first = text[start]
    if first not in pairs:
        return None
    closing = [pairs[first]]
    quote: str | None = None
    escaped = False
    for index in range(start + 1, len(text)):
        char = text[index]
        if quote is not None:
            if escaped:
                escaped = False
            elif char == '\\':
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char in {'"', "'"}:
            quote = char
        elif char in pairs:
            closing.append(pairs[char])
        elif char in ')]}':
            if not closing or char != closing.pop():
                return None
            if not closing:
                return index
    return None


def _split_google_return_header(line: str) -> tuple[str, str]:
    """Split a Google value header at its top-level type/description colon."""
    separator = _top_level_separator(line, ':')
    if separator is None:
        return line.strip(), ''
    return line[:separator].strip(), line[separator + 1 :].strip()


def _google_entry_header(line: str) -> tuple[str, str, str] | None:
    """Parse a Google name, balanced optional type wrapper, and description."""
    separator = _top_level_separator(line, ':')
    if separator is None:
        return None
    left = line[:separator].strip()
    description = line[separator + 1 :].strip()
    wrapper = left.find('(')
    if wrapper < 0:
        names = left
        type_text = ''
    else:
        if not left[:wrapper].endswith((' ', '\t')):
            return None
        names = left[:wrapper].strip()
        close = _matching_delimiter(left, wrapper)
        if close != len(left) - 1:
            return None
        type_text = left[wrapper + 1 : close].strip()
    if not _entry_names(names):
        return None
    return names, type_text, description


def _has_indented_description(
    lines: list[str],
    index: int,
    base_indent: int,
) -> bool:
    for line in lines[index + 1 :]:
        if line.strip():
            return _entry_indent(line) > base_indent
    return False


def _is_entry_header(
    line: str,
    section: DocSection,
    style: Literal['numpy', 'google'],
    *,
    has_indented_description: bool = False,
) -> bool:
    if not line.strip():
        return False
    if style == 'google' and section.name in _TYPED_VALUE_SECTIONS:
        type_text, _description = _split_google_return_header(line.strip())
        return bool(type_text)
    if style == 'google':
        return _google_entry_header(line.strip()) is not None
    if section.name in _NAMED_TYPE_SECTIONS:
        names, separator, type_text = line.partition(':')
        if separator:
            return bool(type_text.strip() and _entry_names(names))
        return has_indented_description and bool(_entry_names(line))
    return True


def _parse_entry(
    line: str,
    section: DocSection,
    style: Literal['numpy', 'google'],
    description_lines: list[str],
    line_number: int,
) -> DocEntry:
    inline_description = ''
    names: tuple[str, ...] = ()
    type_text = ''
    default_text = None
    if style == 'google' and section.name in _TYPED_VALUE_SECTIONS:
        type_text, inline_description = _split_google_return_header(line.strip())
    elif style == 'google':
        parsed = _google_entry_header(line.strip())
        if parsed is None:
            return DocEntry((), '', '', None, line_number)
        left, google_type, inline_description = parsed
        if section.name in _NAMED_TYPE_SECTIONS:
            names = _entry_names(left)
            type_text = google_type
        else:
            names = _entry_names(left) if not google_type else ()
    elif section.name in _NAMED_TYPE_SECTIONS:
        left, _, type_text = line.partition(':')
        names = _entry_names(left)
        type_text = type_text.strip()
    elif section.name in _TYPED_VALUE_SECTIONS:
        left, separator, right = line.partition(':')
        if separator and _entry_names(left):
            names = _entry_names(left)
            type_text = right.strip()
        else:
            type_text = line.strip()
    else:
        left, separator, right = line.partition(':')
        names = _entry_names(left) if separator else _entry_names(line)
        inline_description = right.strip() if separator else ''

    if section.name in _NAMED_TYPE_SECTIONS:
        default = _default_clause(type_text, description=False)
        if default is not None:
            type_text, default_text = default
    description_parts = (
        inline_description,
        *(line.strip() for line in description_lines),
    )
    description = '\n'.join(part for part in description_parts if part)
    if (
        default_text is None
        and style == 'google'
        and section.name in _NAMED_TYPE_SECTIONS
    ):
        default = _default_clause(description, description=True)
        if default is not None:
            _, default_text = default
    return DocEntry(names, type_text.strip(), description, default_text, line_number)


def _entries(
    section: DocSection,
    style: Literal['numpy', 'google'],
) -> tuple[DocEntry, ...]:
    lines = section.body.splitlines()
    nonempty_indents = [_entry_indent(line) for line in lines if line.strip()]
    if not nonempty_indents:
        return ()
    base_indent = min(nonempty_indents) if style == 'google' else 0
    starts: list[int] = []
    for index, line in enumerate(lines):
        if _entry_indent(line) != base_indent:
            continue
        if _is_entry_header(
            line,
            section,
            style,
            has_indented_description=_has_indented_description(
                lines,
                index,
                base_indent,
            ),
        ):
            starts.append(index)
    entries: list[DocEntry] = []
    body_offset = 1 if style == 'google' else 2
    for position, index in enumerate(starts):
        end = starts[position + 1] if position + 1 < len(starts) else len(lines)
        entries.append(
            _parse_entry(
                lines[index].strip(),
                section,
                style,
                lines[index + 1 : end],
                section.line + body_offset + index,
            )
        )
    return tuple(entries)


def parameter_entries(doc: ParsedDoc) -> tuple[DocEntry, ...]:
    """Parameter entries in every supported parameter section, in source order."""
    return tuple(
        entry
        for section in doc.sections
        if section.name in _PARAMETER_SECTIONS
        for entry in _entries(section, doc.style)
    )


def attribute_entries(doc: ParsedDoc) -> tuple[DocEntry, ...]:
    return tuple(
        entry
        for section in doc.sections
        if section.name == 'Attributes'
        for entry in _entries(section, doc.style)
    )


def raises_entries(doc: ParsedDoc) -> tuple[DocEntry, ...]:
    return tuple(
        entry
        for section in doc.sections
        if section.name == 'Raises'
        for entry in _entries(section, doc.style)
    )


def return_entries(doc: ParsedDoc) -> tuple[DocEntry, ...]:
    return tuple(
        entry
        for section in doc.sections
        if section.name in _OUTPUT_SECTIONS
        for entry in _entries(section, doc.style)
    )


def _expression_text(expression: ast.expr | None) -> str | None:
    return ast.unparse(expression) if expression is not None else None


def _type_terminal_name(expression: ast.expr | None) -> str | None:
    if isinstance(expression, ast.Name):
        return expression.id
    if isinstance(expression, ast.Attribute):
        return expression.attr
    if isinstance(expression, ast.Subscript):
        return _type_terminal_name(expression.value)
    return None


def _has_required_never_input(
    function: ast.FunctionDef | ast.AsyncFunctionDef,
) -> bool:
    args = function.args
    positional = [*args.posonlyargs, *args.args]
    defaults: list[ast.expr | None] = [None] * (
        len(positional) - len(args.defaults)
    ) + list(args.defaults)
    if any(
        default is None
        and _type_terminal_name(argument.annotation) in {'Never', 'NoReturn'}
        for argument, default in zip(positional, defaults, strict=True)
    ):
        return True
    return any(
        default is None
        and _type_terminal_name(argument.annotation) in {'Never', 'NoReturn'}
        for argument, default in zip(args.kwonlyargs, args.kw_defaults, strict=True)
    )


def _function_forms(
    declarations: tuple[ast.AST, ...],
    *,
    kind: str,
    qualname: str,
) -> tuple[tuple[SignatureParameter, ...], ...]:
    functions: list[ast.FunctionDef | ast.AsyncFunctionDef] = []
    for declaration in declarations:
        if isinstance(declaration, (ast.FunctionDef, ast.AsyncFunctionDef)):
            functions.append(declaration)
        elif isinstance(declaration, ast.ClassDef) and kind == 'constructor':
            functions.extend(
                node
                for node in declaration.body
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name in {'__new__', '__init__'}
            )
    if not functions:
        return ()

    overloads = [
        function for function in functions if decorator_names(function) & {'overload'}
    ]
    if overloads:
        functions = overloads
    elif len(functions) > 1:
        functions = functions[-1:]

    result: list[tuple[SignatureParameter, ...]] = []
    for function in functions:
        if _type_terminal_name(function.returns) in {
            'Never',
            'NoReturn',
        } or _has_required_never_input(function):
            continue
        args = function.args
        positional = [*args.posonlyargs, *args.args]
        defaults: list[ast.expr | None] = [None] * (
            len(positional) - len(args.defaults)
        ) + list(args.defaults)
        receiver = None
        if kind == 'constructor':
            receiver = 'cls' if function.name == '__new__' else 'self'
        elif (
            '.' in qualname
            and not (decorator_names(function) & {'staticmethod'})
            and positional
            and positional[0].arg in {'self', 'cls'}
        ):
            receiver = positional[0].arg
        if receiver and positional and positional[0].arg == receiver:
            positional = positional[1:]
            defaults = defaults[1:]

        parameters: list[SignatureParameter] = []
        for index, (argument, default) in enumerate(
            zip(positional, defaults, strict=True)
        ):
            parameters.append(
                SignatureParameter(
                    argument.arg,
                    (
                        'positional-only'
                        if index
                        < max(0, len(args.posonlyargs) - int(receiver is not None))
                        else 'positional or keyword'
                    ),
                    _expression_text(argument.annotation),
                    _expression_text(default),
                )
            )
        if args.vararg is not None:
            parameters.append(
                SignatureParameter(
                    args.vararg.arg,
                    'variadic positional',
                    _expression_text(args.vararg.annotation),
                    None,
                )
            )
        for argument, default in zip(args.kwonlyargs, args.kw_defaults, strict=True):
            parameters.append(
                SignatureParameter(
                    argument.arg,
                    'keyword-only',
                    _expression_text(argument.annotation),
                    _expression_text(default),
                )
            )
        if args.kwarg is not None:
            parameters.append(
                SignatureParameter(
                    args.kwarg.arg,
                    'variadic keyword',
                    _expression_text(args.kwarg.annotation),
                    None,
                )
            )
        result.append(tuple(parameters))
    return tuple(result)


def _parameter_forms(
    subject: DocSubject,
) -> tuple[tuple[SignatureParameter, ...], ...]:
    if subject.kind not in {'callable', 'constructor'}:
        return ()
    forms = _function_forms(
        subject.declarations,
        kind=subject.kind,
        qualname=subject.qualname,
    )
    if forms or subject.parameters is None:
        return forms

    kind_names = {
        inspect.Parameter.POSITIONAL_ONLY: 'positional-only',
        inspect.Parameter.POSITIONAL_OR_KEYWORD: 'positional or keyword',
        inspect.Parameter.VAR_POSITIONAL: 'variadic positional',
        inspect.Parameter.KEYWORD_ONLY: 'keyword-only',
        inspect.Parameter.VAR_KEYWORD: 'variadic keyword',
    }
    return (
        tuple(
            SignatureParameter(
                parameter.name,
                kind_names[parameter.kind],
                (
                    str(parameter.annotation)
                    if parameter.annotation is not inspect.Parameter.empty
                    else None
                ),
                (
                    None
                    if parameter.default is inspect.Parameter.empty
                    else repr(parameter.default)
                ),
            )
            for parameter in subject.parameters
        ),
    )


def _same_default(left: str, right: str) -> bool:
    left_text = left.rstrip('.').strip('`')
    right_text = right.rstrip('.').strip('`')
    if left_text == right_text:
        return True
    try:
        left_expression = ast.parse(left_text, mode='eval').body
        right_expression = ast.parse(right_text, mode='eval').body
    except (SyntaxError, ValueError):
        return False
    if ast.dump(left_expression, include_attributes=False) == ast.dump(
        right_expression,
        include_attributes=False,
    ):
        return True
    unparsed = object()
    try:
        left_value = ast.literal_eval(left_expression)
    except (SyntaxError, ValueError):
        left_value = unparsed
    try:
        right_value = ast.literal_eval(right_expression)
    except (SyntaxError, ValueError):
        right_value = unparsed
    if (
        left_value is not unparsed
        and right_value is not unparsed
        and type(left_value) is type(right_value)
        and left_value == right_value
    ):
        return True

    return (
        isinstance(left_expression, ast.Name)
        and isinstance(right_value, str)
        and left_expression.id == right_value
    ) or (
        isinstance(right_expression, ast.Name)
        and isinstance(left_value, str)
        and right_expression.id == left_value
    )


def resolve_parameter_rows(
    qualname: str,
    carriers: Sequence[tuple[str, DocLocation]],
    forms: Sequence[Sequence[SignatureParameter]],
    *,
    require_documentation: bool = True,
) -> ParameterResolution:
    """Resolve one documented row per parameter against exact call-form states."""
    entries: dict[str, list[tuple[DocEntry, DocLocation]]] = {}
    for prose, source in carriers:
        for entry in parameter_entries(parse_doc(prose)):
            for name in entry.names:
                entries.setdefault(name, []).append((entry, source))

    ordered_names = list(
        dict.fromkeys(parameter.name for form in forms for parameter in form)
    )
    ordered_names.extend(name for name in entries if name not in ordered_names)
    errors: list[str] = []
    rows: list[ResolvedParameter] = []

    for name in ordered_names:
        occurrences = tuple(
            next((parameter for parameter in form if parameter.name == name), None)
            for form in forms
        )
        states: list[ParameterOccurrence] = []
        per_form: list[ParameterOccurrence | None] = []
        for parameter in occurrences:
            if parameter is None:
                per_form.append(None)
                continue
            status: Literal['required', 'defaulted', 'variadic'] = (
                'variadic'
                if parameter.kind.startswith('variadic')
                else ('defaulted' if parameter.default is not None else 'required')
            )
            occurrence = ParameterOccurrence(
                parameter.kind,
                parameter.annotation,
                parameter.default,
                status,
            )
            states.append(occurrence)
            per_form.append(occurrence)
        present = states
        display_default: str | None
        if not present:
            row_status: Literal['required', 'defaulted', 'variadic', 'varies'] = (
                'varies'
            )
            display_default = 'varies by call form'
        else:
            state_keys = {
                (
                    occurrence.status,
                    occurrence.kind if occurrence.status == 'variadic' else None,
                    occurrence.default,
                )
                for occurrence in states
            }
            varies = len(present) != len(occurrences) or len(state_keys) != 1
            if varies:
                row_status = 'varies'
                display_default = 'varies by call form'
            else:
                row_status = states[0].status
                display_default = (
                    states[0].default
                    if row_status == 'defaulted'
                    else 'not applicable'
                    if row_status == 'variadic'
                    else None
                )

        claims = entries.get(name, [])
        locations = tuple(
            f'{source.path}:{source.line + entry.line - 1}' for entry, source in claims
        )
        type_claims = list(
            dict.fromkeys(
                ' '.join(entry.type_text.split())
                for entry, _source in claims
                if entry.type_text.strip()
            )
        )
        if len(type_claims) > 1:
            errors.append(
                f'{qualname}.{name}: conflicting documented annotations '
                f'{type_claims!r} at {locations!r}'
            )
        documented_defaults: list[tuple[str, str]] = []
        for entry, source in claims:
            if entry.default_text is None:
                continue
            if not any(
                _same_default(entry.default_text, previous)
                for previous, _location in documented_defaults
            ):
                documented_defaults.append((
                    entry.default_text,
                    f'{source.path}:{source.line + entry.line - 1}',
                ))
        if len(documented_defaults) > 1:
            errors.append(
                f'{qualname}.{name}: conflicting documented defaults '
                f'{documented_defaults!r}'
            )

        if documented_defaults:
            stated, location = documented_defaults[0]
            if row_status == 'required':
                errors.append(
                    f'{qualname}.{name}: documented default {stated} but the '
                    'signature has no default'
                )
            elif row_status == 'variadic':
                errors.append(
                    f'{qualname}.{name}: variadic parameter has a documented default '
                    f'{stated} at {location}'
                )
            elif row_status == 'varies' and stated != 'varies by call form':
                errors.append(
                    f'{qualname}.{name}: documented default {stated} at {location} '
                    'but applicability varies by call form'
                )
            elif row_status == 'defaulted' and not _same_default(
                stated,
                display_default or '',
            ):
                try:
                    expected_default: object = ast.literal_eval(
                        (display_default or '').rstrip('.').strip('`')
                    )
                except (SyntaxError, ValueError):
                    expected_default = display_default
                errors.append(
                    f'{qualname}.{name}: documented default {stated} '
                    f'!= runtime {expected_default!r}'
                )

        if require_documentation and forms and present and not claims:
            errors.append(f'{qualname}: parameter {name!r} is not documented')
        if claims and not present:
            errors.append(
                f'{qualname}: documented parameter {name!r} is not in any call form'
            )

        signature_annotations = list(
            dict.fromkeys(
                parameter.annotation
                for parameter in present
                if parameter.annotation is not None
            )
        )
        annotation = (
            ' | '.join(signature_annotations)
            if signature_annotations
            else (type_claims[0] if type_claims else None)
        )
        descriptions: list[str] = []
        for entry, _source in claims:
            text = entry.description.strip()
            if text and text not in descriptions:
                descriptions.append(text)
        rows.append(
            ResolvedParameter(
                name,
                annotation,
                '\n\n'.join(descriptions),
                row_status,
                display_default,
                tuple(per_form),
            )
        )

    return ParameterResolution(tuple(rows), tuple(errors))


def duplicate_section_errors(doc: str | ParsedDoc, qualname: str) -> list[str]:
    parsed = parse_doc(doc) if isinstance(doc, str) else doc
    seen: set[str] = set()
    errors: list[str] = []
    for section in parsed.sections:
        if section.name in seen:
            errors.append(f'{qualname}: duplicate {section.name!r} section')
        seen.add(section.name)
    return errors


def _parameters(obj: object) -> list[inspect.Parameter] | None:
    """Signature parameters worth documenting, or None when unreadable.

    An unreadable signature is the `structural` gate's finding, not this one's.
    """
    try:
        signature = inspect.signature(obj)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return [
        param
        for param in signature.parameters.values()
        if param.name not in ('self', 'cls')
    ]


def _stub_parameters(
    node: ast.FunctionDef | ast.AsyncFunctionDef,
) -> list[inspect.Parameter]:
    """Build signature parameters from a stub's canonical (last) def."""
    args = node.args
    positional = [*args.posonlyargs, *args.args]
    defaults: list[ast.expr | None] = [None] * (
        len(positional) - len(args.defaults)
    ) + list(args.defaults)
    parameters: list[inspect.Parameter] = []

    def default_value(default: ast.expr | None) -> object:
        if default is None:
            return inspect.Parameter.empty
        if isinstance(default, ast.Constant) and default.value is Ellipsis:
            return ...
        try:
            return ast.literal_eval(default)
        except (ValueError, SyntaxError):
            return ast.unparse(default)

    for arg, default in zip(positional, defaults, strict=True):
        kind = (
            inspect.Parameter.POSITIONAL_ONLY
            if arg in args.posonlyargs
            else inspect.Parameter.POSITIONAL_OR_KEYWORD
        )
        parameters.append(
            inspect.Parameter(arg.arg, kind, default=default_value(default))
        )
    if args.vararg:
        parameters.append(
            inspect.Parameter(args.vararg.arg, inspect.Parameter.VAR_POSITIONAL)
        )
    for arg, default in zip(args.kwonlyargs, args.kw_defaults, strict=True):
        parameters.append(
            inspect.Parameter(
                arg.arg,
                inspect.Parameter.KEYWORD_ONLY,
                default=default_value(default),
            )
        )
    if args.kwarg:
        parameters.append(
            inspect.Parameter(args.kwarg.arg, inspect.Parameter.VAR_KEYWORD)
        )
    return [param for param in parameters if param.name not in ('self', 'cls')]


def _doc_location(
    path: str,
    node: ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef,
) -> DocLocation:
    line = node.lineno
    if (
        node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
        and isinstance(node.body[0].value.value, str)
    ):
        line = node.body[0].lineno
    return DocLocation(path, line)


def _runtime_location(obj: Any, fallback: DocLocation) -> DocLocation:
    if isinstance(obj, property):
        if obj.fget is None:
            return fallback
        obj = obj.fget
    try:
        path = inspect.getsourcefile(obj)
        if path is not None:
            _, line = inspect.getsourcelines(obj)
            return DocLocation(path, line)
    except (OSError, TypeError):
        pass
    return fallback


def _is_public(name: str) -> bool:
    return not name.startswith('_')


def _public_names(ctx: CheckContext, runtime: ModuleType) -> tuple[str, ...]:
    exported = getattr(runtime, '__all__', None)
    names = (
        [name for name in cast('Sequence[object]', exported) if isinstance(name, str)]
        if isinstance(exported, (list, tuple))
        else []
    )
    names.extend(function_groups(ctx.stub_ast.body))
    names.extend(ctx.stub_classes)
    return tuple(dict.fromkeys(name for name in names if _is_public(name)))


def _property_getter(
    defs: Sequence[ast.FunctionDef | ast.AsyncFunctionDef],
) -> ast.FunctionDef | ast.AsyncFunctionDef:
    for node in defs:
        if decorator_names(node) & {'property', 'getter'}:
            return node
    return defs[-1]


def _subject(
    qualname: str,
    kind: Literal[
        'callable',
        'property',
        'constructor',
        'class',
        'attribute',
        'schema',
    ],
    prose: str | None,
    parameters: list[inspect.Parameter] | None,
    source: DocLocation,
    declarations: tuple[ast.AST, ...] = (),
) -> DocSubject:
    return DocSubject(
        qualname,
        kind,
        prose,
        tuple(parameters) if parameters is not None else None,
        source,
        declarations,
    )


def _class_subjects(
    ctx: CheckContext,
    name: str,
    cls: type,
    class_node: ast.ClassDef | None,
    constructor_sources: Mapping[str, ConstructorDocSource],
    source_class: DocClassSource | None,
) -> Iterator[DocSubject]:
    class_path = (
        source_class.source.path if source_class is not None else str(ctx.cfg.stub_path)
    )
    class_source = (
        source_class.source
        if source_class is not None
        else (
            _doc_location(class_path, class_node)
            if class_node is not None
            else _runtime_location(cls, DocLocation(class_path, 1))
        )
    )
    class_doc = (
        ast.get_docstring(source_class.node)
        if source_class is not None
        else runtime_doc_for_stub(name, cls)
    )
    groups = function_groups(class_node.body) if class_node is not None else {}
    new_defs = groups.get('__new__', [])
    prohibited_stub_constructor = (
        class_node is not None and class_prohibits_construction(class_node)
    )
    constructible_stub = bool(new_defs) and not prohibited_stub_constructor
    source_constructor = constructor_sources.get(name)
    source_members: Mapping[str, DocMemberSource] = (
        source_class.members if source_class is not None else {}
    )
    descriptor_carriers: dict[str, tuple[object, DocLocation, ast.AnnAssign]] = {}
    if class_node is not None:
        for (
            member_name,
            statement,
            _docstring,
            descriptor,
        ) in annotated_data_descriptors(cls, class_node.body):
            index = class_node.body.index(statement)
            source_line = statement.lineno
            if index + 1 < len(class_node.body):
                source_line = class_node.body[index + 1].lineno
            descriptor_carriers[member_name] = (
                descriptor,
                DocLocation(class_path, source_line),
                statement,
            )

    if not prohibited_stub_constructor and (
        source_constructor is not None
        or constructible_stub
        or (source_class is not None and source_class.initialization is not None)
    ):
        carrier = new_defs[-1] if new_defs else None
        constructor_declarations: tuple[ast.AST, ...] = tuple(new_defs)
        if source_constructor is not None:
            prose = source_constructor.prose
            source = DocLocation(source_constructor.path, source_constructor.line)
        elif source_class is not None and source_class.initialization is not None:
            initialization = source_class.initialization
            prose = initialization.prose
            source = initialization.source
            constructor_declarations = (initialization.node,)
        else:
            runtime_new = inspect.getattr_static(cls, '__new__', None)
            prose = runtime_doc_for_stub('__new__', runtime_new)
            source = _runtime_location(runtime_new, class_source)
            if prose is None and carrier is not None:
                prose = ast.get_docstring(carrier)
                if is_synthetic_doc('__new__', prose):
                    prose = None
                source = _doc_location(class_path, carrier)
        yield _subject(
            f'{name}.__new__',
            'constructor',
            prose,
            _parameters(cls),
            source,
            declarations=constructor_declarations,
        )

    yield _subject(
        name,
        'class',
        class_doc,
        None,
        class_source,
        declarations=(class_node,) if class_node is not None else (),
    )
    if class_node is None:
        return

    stub_groups = groups
    runtime_names = tuple(
        member_name for member_name in vars(cls) if _is_public(member_name)
    )
    member_names = tuple(dict.fromkeys((*stub_groups, *runtime_names, *source_members)))
    seen: set[str] = set()
    for member_name in member_names:
        if not _is_public(member_name) or member_name in {'__new__', '__init__'}:
            continue
        source_member = source_members.get(member_name)
        if source_member is not None and source_member.kind in {'attribute', 'schema'}:
            seen.add(member_name)
            yield _subject(
                f'{name}.{member_name}',
                source_member.kind,
                source_member.prose,
                None,
                source_member.source,
                declarations=(source_member.node,),
            )
            continue
        defs = stub_groups.get(member_name, [])
        member_carrier: ast.FunctionDef | ast.AsyncFunctionDef | None = (
            _property_getter(defs)
            if defs
            else (
                source_member.node
                if source_member is not None
                and isinstance(
                    source_member.node,
                    (ast.FunctionDef, ast.AsyncFunctionDef),
                )
                else None
            )
        )
        stub_property = member_carrier is not None and bool(
            decorator_names(member_carrier) & {'property', 'getter'}
        )
        static = inspect.getattr_static(cls, member_name, None)
        property_like = (
            (source_member is not None and source_member.kind == 'property')
            or stub_property
            or isinstance(static, property)
            or (
                static is not None
                and inspect.isdatadescriptor(static)
                and not callable(static)
            )
        )
        if source_member is not None:
            source = source_member.source
        elif member_name in descriptor_carriers:
            source = descriptor_carriers[member_name][1]
        else:
            source = (
                _doc_location(class_path, member_carrier)
                if member_carrier is not None
                else _runtime_location(static, class_source)
            )
        if property_like:
            seen.add(member_name)
            prose = (
                source_member.prose
                if source_member is not None
                else (
                    runtime_doc_for_stub(member_name, static)
                    if static is not None
                    else None
                )
            )
            yield _subject(
                f'{name}.{member_name}',
                'property',
                prose,
                None,
                source,
                declarations=(
                    (source_member.node,)
                    if source_member is not None
                    else (
                        (descriptor_carriers[member_name][2],)
                        if member_name in descriptor_carriers
                        else ((member_carrier,) if member_carrier is not None else ())
                    )
                ),
            )
            continue

        runtime_member = passive_class_member(cls, member_name)
        if runtime_member is None and not defs and source_member is None:
            continue
        if source_member is not None:
            yield _subject(
                f'{name}.{member_name}',
                'callable',
                source_member.prose,
                _parameters(runtime_member) if runtime_member is not None else None,
                source_member.source,
                declarations=(source_member.node,),
            )
            seen.add(member_name)
            continue
        if member_name not in vars(cls) and defs:
            assert member_carrier is not None
            stub_doc = ast.get_docstring(member_carrier)
            prose = stub_doc or runtime_doc_for_stub(member_name, runtime_member)
            yield _subject(
                f'{name}.{member_name}',
                'callable',
                prose,
                _stub_parameters(member_carrier),
                source,
                declarations=tuple(defs),
            )
            continue
        yield _subject(
            f'{name}.{member_name}',
            'callable',
            runtime_doc_for_stub(member_name, runtime_member),
            _parameters(runtime_member),
            source,
            declarations=tuple(defs),
        )
        seen.add(member_name)

    for member_name, (descriptor, source, declaration) in descriptor_carriers.items():
        if not _is_public(member_name) or member_name in seen:
            continue
        yield _subject(
            f'{name}.{member_name}',
            'property',
            runtime_doc_for_stub(member_name, descriptor),
            None,
            source,
            declarations=(declaration,),
        )


def iter_doc_subjects(
    ctx: CheckContext,
    *,
    runtime_module: ModuleType | None = None,
    constructor_sources: Mapping[str, ConstructorDocSource] | None = None,
    source_classes: Mapping[str, DocClassSource] | None = None,
) -> Iterator[DocSubject]:
    """Enumerate documented public subjects from the stub and runtime surface.

    Runtime ``__all__`` adds facade names; stub declarations supply carrier
    locations and overload ownership. Source class declarations fill the same
    subjects for facade re-exports absent from the native stub.
    """
    if constructor_sources is None:
        constructor_sources = _rust_constructor_sources(ctx.cfg)
    modules = [runtime_module] if runtime_module is not None else []
    classes: list[tuple[str, type]] = []
    if ctx.cfg.surface is not None:
        for name, owner in ctx.cfg.surface.targets:
            if isinstance(owner, ModuleType):
                modules.append(owner)
            elif isinstance(owner, type):
                classes.append((name, owner))
    modules.append(ctx.runtime_module)

    seen_modules: set[int] = set()
    seen_subjects: set[tuple[str, str]] = set()
    for runtime in modules:
        if id(runtime) in seen_modules:
            continue
        seen_modules.add(id(runtime))
        groups = function_groups(ctx.stub_ast.body)
        for name in _public_names(ctx, runtime):
            obj = getattr(runtime, name, None)
            if obj is None:
                continue
            if isinstance(obj, type):
                source_class = (
                    source_classes.get(name)
                    if source_classes is not None and name not in ctx.stub_classes
                    else None
                )
                subjects = _class_subjects(
                    ctx,
                    name,
                    obj,
                    ctx.stub_classes.get(name)
                    or (source_class.node if source_class is not None else None),
                    constructor_sources,
                    source_class,
                )
                for subject in subjects:
                    key = (subject.qualname, subject.kind)
                    if key not in seen_subjects:
                        seen_subjects.add(key)
                        yield subject
                continue
            if not callable(obj):
                continue
            defs = groups.get(name, [])
            source = (
                _doc_location(str(ctx.cfg.stub_path), defs[-1])
                if defs
                else _runtime_location(obj, DocLocation(str(ctx.cfg.stub_path), 1))
            )
            subject = _subject(
                name,
                'callable',
                runtime_doc_for_stub(name, obj),
                _parameters(obj),
                source,
                declarations=tuple(defs),
            )
            key = (subject.qualname, subject.kind)
            if key not in seen_subjects:
                seen_subjects.add(key)
                yield subject

    for name, cls in classes:
        source_class = (
            source_classes.get(name)
            if source_classes is not None and name not in ctx.stub_classes
            else None
        )
        for subject in _class_subjects(
            ctx,
            name,
            cls,
            ctx.stub_classes.get(name)
            or (source_class.node if source_class is not None else None),
            constructor_sources,
            source_class,
        ):
            key = (subject.qualname, subject.kind)
            if key not in seen_subjects:
                seen_subjects.add(key)
                yield subject


def check_doc_subject(subject: DocSubject) -> list[str]:
    """Check one carrier's selected signature against the shared parsed prose."""
    parsed = parse_doc(subject.prose)
    errors: list[str] = []
    forms = _parameter_forms(subject)
    if forms:
        resolution = resolve_parameter_rows(
            subject.qualname,
            ((subject.prose or '', subject.source),),
            forms,
        )
        errors.extend(resolution.errors)
    if subject.kind in {'callable', 'property', 'constructor'} and not return_entries(
        parsed
    ):
        errors.append(
            f'{subject.qualname}: no Returns section — document what it gives back'
        )
    return errors


def collect_errors(cfg: StubConfig) -> Findings:
    """Public subjects whose prose does not describe their binding contract."""
    ctx = CheckContext(cfg)
    errors: list[str] = []
    examined = 0
    exempt: set[str] = set()

    if cfg.docstring_style is not None:
        for path in sorted(cfg.stub_path.parent.rglob('*')):
            if path.suffix not in {'.py', '.pyi'}:
                continue
            for node in ast.walk(ast.parse(path.read_text(encoding='utf-8'))):
                if not isinstance(
                    node,
                    (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef),
                ):
                    continue
                if prose := ast.get_docstring(node):
                    errors.extend(
                        f'{path}:{node.body[0].lineno}: {error}'
                        for error in doc_style_errors(prose, cfg.docstring_style)
                    )
    for subject in iter_doc_subjects(ctx):
        if cfg.docstring_style is not None and subject.prose:
            errors.extend(
                f'{subject.source.path}:{subject.source.line}: {subject.qualname}: {error}'
                for error in doc_style_errors(subject.prose, cfg.docstring_style)
            )
        if subject.qualname in cfg.doc_structure_allowlist:
            if subject.prose and subject.prose.strip():
                exempt.add(subject.qualname)
            continue
        if not (subject.prose or '').strip():
            continue  # presence belongs to doc-contract
        examined += 1
        errors.extend(
            f'{subject.source.path}:{subject.source.line}: {error}'
            for error in check_doc_subject(subject)
        )
    errors += unused_allowlist_errors(
        'doc structure', cfg.doc_structure_allowlist, exempt
    )
    return Findings(errors, examined=examined)
