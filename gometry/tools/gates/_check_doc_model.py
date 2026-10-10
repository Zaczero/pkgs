#!/usr/bin/env python3
"""Validate the transformed Griffe model used by the documentation build."""

from __future__ import annotations

import ast
import copy
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import griffe

ROOT = Path(__file__).resolve().parents[2]
STUB = ROOT / 'python/gometry/_lib.pyi'
_TOOLS_ROOT = ROOT / 'tools'
if str(_TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TOOLS_ROOT))

from _gatelib import prepend_rust_source_import_paths

prepend_rust_source_import_paths()

from griffe_expand_aliases import (
    ExpandTokenAliases,
    PromoteStubOverloads,
    annotation_aliases,
    constructor_binding,
    expand_annotation,
)
from public_inventory import load_public_symbols
from pyo3stubs.cli import load_config
from pyo3stubs.context import CheckContext
from pyo3stubs.doc_structure import (
    DocSubject,
    SignatureParameter,
    attribute_entries,
    check_doc_subject,
    iter_doc_subjects,
    parameter_entries,
    parse_doc,
    resolve_parameter_rows,
    return_entries,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

_PUBLIC_TYPEVAR_NAMES = {
    '_CellT': 'CellT',
    '_CellT_co': 'CellT_co',
    '_DefaultT': 'DefaultT',
    '_ExtremeT': 'ExtremeT',
    '_GeometryOtherT': 'GeometryOtherT',
    '_GeometryRowT': 'GeometryRowT',
    '_GeometryT': 'GeometryT',
    '_GeometryT_co': 'GeometryT_co',
    '_GroupKeyT': 'GroupKeyT',
    '_GroupMissingT_co': 'GroupMissingT_co',
    '_GroupRowT_co': 'GroupRowT_co',
}

ParameterForm = tuple[str | None, str, str | None, str | None]
CallForm = tuple[tuple[ParameterForm, ...], str | None]


@dataclass(frozen=True)
class _StubConstructor:
    source_class: str
    name: str
    forms: tuple[CallForm, ...]
    error: str | None = None


@dataclass(frozen=True, slots=True)
class SourceDocumentationContracts:
    call_forms: dict[str, tuple[CallForm, ...]]
    constructor_rows: dict[
        str,
        dict[str, tuple[str, str | None, str | None]],
    ]


def _is_never(function: griffe.Function) -> bool:
    returns = function.returns
    return isinstance(returns, griffe.ExprName) and returns.name in {
        'Never',
        'NoReturn',
    }


def _public_annotation(annotation: str) -> str:
    for private, public in _PUBLIC_TYPEVAR_NAMES.items():
        annotation = annotation.replace(private, public)
    return annotation


def _mentions_type(type_text: str, name: str) -> bool:
    """Find a type name as a parsed identifier, not a substring."""
    normalized = type_text.replace('`', '').replace(' or ', ' | ')
    try:
        expression = ast.parse(normalized, mode='eval')
    except SyntaxError:
        return False
    return any(
        isinstance(node, ast.Name) and node.id == name for node in ast.walk(expression)
    )


def _declaration_annotation(declaration: ast.AST) -> ast.expr | None:
    if isinstance(declaration, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return declaration.returns
    if isinstance(declaration, ast.AnnAssign):
        return declaration.annotation
    return None


def _ast_mentions_type(expression: ast.expr, name: str) -> bool:
    return any(
        isinstance(node, (ast.Name, ast.Attribute)) and _stub_base_name(node) == name
        for node in ast.walk(expression)
    )


def _scalar_array_return_errors(
    subjects: Iterable[DocSubject],
    stub_tree: ast.Module,
) -> list[str]:
    """Keep return prose aligned with the selected binding declarations."""
    errors: list[str] = []
    class_nodes = _stub_class_nodes(stub_tree)
    generic_arguments = _stub_generic_class_arguments(stub_tree)
    for subject in subjects:
        if (
            subject.kind not in {'callable', 'property'}
            or not subject.prose
            or not subject.declarations
        ):
            continue
        if not any(
            _mentions_type(entry.type_text, 'GeometryArray')
            for entry in return_entries(parse_doc(subject.prose))
        ):
            continue
        declarations = tuple(
            annotation
            for declaration in subject.declarations
            if (annotation := _declaration_annotation(declaration)) is not None
        )
        if any(
            _ast_mentions_type(annotation, 'GeometryArray')
            for annotation in declarations
        ):
            continue
        class_name = subject.qualname.rpartition('.')[0]
        generic_support = tuple(
            _generic_class_return_support(
                annotation,
                class_nodes.get(class_name),
                generic_arguments,
                class_nodes,
                'GeometryArray',
            )
            for annotation in declarations
        )
        if True in generic_support:
            continue
        if None in generic_support:
            # An open class TypeVar can be specialized outside this stub. Without
            # a concrete in-tree instantiation, its possible values are unknown.
            continue
        rendered = ', '.join(ast.unparse(annotation) for annotation in declarations)
        errors.append(
            f'{subject.source.path}:{subject.source.line}: {subject.qualname}: '
            'Returns type names GeometryArray but the binding declarations return '
            f'{rendered or "no declared type"}'
        )
    return errors


def _has_unquoted_enum(type_text: str) -> bool:
    for match in re.finditer(r'\{([^{}]*)\}', type_text):
        try:
            expression = ast.parse('{' + match.group(1) + '}', mode='eval').body
        except SyntaxError:
            continue
        if isinstance(expression, ast.Set) and any(
            isinstance(node, (ast.Name, ast.Attribute))
            for element in expression.elts
            for node in ast.walk(element)
        ):
            return True
    return False


def _doc_type_expression_errors(subjects: Iterable[DocSubject]) -> list[str]:
    """Keep literal string choices quoted in parsed type expressions."""
    errors: list[str] = []
    for subject in subjects:
        if not subject.prose:
            continue
        qualname = subject.qualname
        parsed = parse_doc(subject.prose)
        named_entries = (
            *parameter_entries(parsed),
            *attribute_entries(parsed),
        )
        errors.extend(
            f'{qualname}: enum values in a type field must be quoted'
            for entry in named_entries
            if _has_unquoted_enum(entry.type_text)
        )
    return errors


def _expressions(expr: Any) -> list[griffe.ExprName]:
    if isinstance(expr, griffe.ExprName):
        return [expr]
    found: list[griffe.ExprName] = []
    for attr in ('left', 'right', 'slice', 'elements'):
        child = getattr(expr, attr, None)
        if isinstance(child, list):
            for item in child:
                found.extend(_expressions(item))
        elif child is not None:
            found.extend(_expressions(child))
    return found


def _functions(obj: Any, seen: set[int]) -> list[griffe.Function]:
    if id(obj) in seen or isinstance(obj, griffe.Alias):
        return []
    seen.add(id(obj))
    result = [obj] if isinstance(obj, griffe.Function) else []
    for member in getattr(obj, 'members', {}).values():
        result.extend(_functions(member, seen))
    return result


def _expression_text(expression: Any) -> str | None:
    if expression is None:
        return None
    source = str(expression)
    try:
        return ast.unparse(ast.parse(source, mode='eval').body)
    except SyntaxError:
        return f'<unparsed:{source}>'


def _stub_annotation(
    expression: ast.expr | None,
    stub_module: griffe.Module,
    aliases: dict[str, Any],
) -> str | None:
    if expression is None:
        return None
    parsed = griffe.get_expression(expression, stub_module, parse_strings=True)
    if parsed is None:
        return None
    return _public_annotation(str(expand_annotation(parsed, aliases)))


def _model_annotation(expression: Any) -> str | None:
    return _public_annotation(str(expression)) if expression is not None else None


def _stub_is_never(
    expression: ast.expr | None,
    stub_module: griffe.Module,
    aliases: dict[str, Any],
) -> bool:
    annotation = _stub_annotation(expression, stub_module, aliases)
    return annotation is not None and annotation.rsplit('.', 1)[-1] in {
        'Never',
        'NoReturn',
    }


def _stub_call_form(
    function: ast.FunctionDef | ast.AsyncFunctionDef,
    stub_module: griffe.Module,
    aliases: dict[str, Any],
    *,
    receiver: str | None = None,
    constructor: str | None = None,
    class_result: str | None = None,
) -> CallForm | None:
    args = function.args
    positional = [*args.posonlyargs, *args.args]
    defaults = [None] * (len(positional) - len(args.defaults)) + list(args.defaults)
    if receiver is not None and (not positional or positional[0].arg != receiver):
        return None
    parameters: list[ParameterForm] = []
    for index, (argument, default) in enumerate(zip(positional, defaults, strict=True)):
        if receiver is not None and index == 0:
            continue
        kind = (
            'positional-only'
            if index < len(args.posonlyargs)
            else 'positional or keyword'
        )
        parameters.append((
            kind,
            argument.arg,
            _stub_annotation(argument.annotation, stub_module, aliases),
            _expression_text(ast.unparse(default)) if default is not None else None,
        ))
    if args.vararg is not None:
        parameters.append((
            'variadic positional',
            args.vararg.arg,
            _stub_annotation(args.vararg.annotation, stub_module, aliases),
            None,
        ))
    for argument, default in zip(args.kwonlyargs, args.kw_defaults, strict=True):
        parameters.append((
            'keyword-only',
            argument.arg,
            _stub_annotation(argument.annotation, stub_module, aliases),
            _expression_text(ast.unparse(default)) if default is not None else None,
        ))
    if args.kwarg is not None:
        parameters.append((
            'variadic keyword',
            args.kwarg.arg,
            _stub_annotation(args.kwarg.annotation, stub_module, aliases),
            None,
        ))
    returns = _stub_annotation(function.returns, stub_module, aliases)
    if constructor is not None and (
        constructor == '__init__'
        or (returns is not None and returns.rsplit('.', 1)[-1] == 'Self')
    ):
        returns = class_result
    return tuple(parameters), returns


def _griffe_call_form(function: griffe.Function) -> CallForm:
    parameters: list[ParameterForm] = []
    for parameter in function.parameters:
        kind = parameter.kind.value if parameter.kind is not None else None
        default = (
            None
            if kind in {'variadic positional', 'variadic keyword'}
            else _expression_text(parameter.default)
        )
        parameters.append((
            kind,
            parameter.name,
            _model_annotation(parameter.annotation),
            default,
        ))
    return tuple(parameters), _model_annotation(function.returns)


def _stub_overloads(
    stub_module: griffe.Module,
    aliases: dict[str, Any],
    tree: ast.Module | None = None,
) -> dict[str, tuple[CallForm, ...]]:
    """All non-Never overload forms by model path, in authored order."""
    if tree is None:
        tree = ast.parse(STUB.read_text(encoding='utf-8'))
    groups: list[tuple[str, list[ast.FunctionDef | ast.AsyncFunctionDef]]] = []

    def collect(prefix: str, body: list[ast.stmt]) -> None:
        by_name: dict[str, list[ast.FunctionDef | ast.AsyncFunctionDef]] = {}
        for node in body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                by_name.setdefault(node.name, []).append(node)
            elif isinstance(node, ast.ClassDef):
                collect(f'{prefix}{node.name}.', node.body)
        groups.extend((f'{prefix}{name}', defs) for name, defs in by_name.items())

    collect('gometry.', tree.body)
    result: dict[str, tuple[CallForm, ...]] = {}
    for path, defs in groups:
        overloads = [
            node
            for node in defs
            if any(
                isinstance(decorator, ast.Name) and decorator.id == 'overload'
                for decorator in node.decorator_list
            )
        ]
        if not overloads:
            continue
        concrete = [
            node
            for node in overloads
            if not _stub_is_never(node.returns, stub_module, aliases)
        ]
        if not concrete:
            # Never-only groups have no accepted forms to publish.
            continue
        forms = []
        for function in concrete:
            form = _stub_call_form(function, stub_module, aliases)
            if form is None:
                raise ValueError(f'{path}: unexpected missing method call form')
            forms.append(form)
        result[path] = tuple(forms)
    return result


def _stub_base_name(expression: ast.expr) -> str | None:
    if isinstance(expression, ast.Subscript):
        return _stub_base_name(expression.value)
    if isinstance(expression, ast.Name):
        return expression.id
    if isinstance(expression, ast.Attribute):
        return expression.attr
    return None


def _stub_class_nodes(tree: ast.Module) -> dict[str, ast.ClassDef]:
    return {node.name: node for node in tree.body if isinstance(node, ast.ClassDef)}


def _stub_type_variables(tree: ast.Module) -> set[str]:
    constructors = {'TypeVar', 'TypeVarTuple', 'ParamSpec'}
    names = set()
    for node in tree.body:
        if isinstance(node, ast.Assign):
            targets = node.targets
            value = node.value
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
            value = node.value
        else:
            continue
        if (
            not isinstance(value, ast.Call)
            or _stub_base_name(value.func) not in constructors
        ):
            continue
        names.update(target.id for target in targets if isinstance(target, ast.Name))
    return names


def _stub_expression_names(expression: ast.expr) -> list[str]:
    if isinstance(expression, ast.Name):
        return [expression.id]
    names = []
    for child in ast.iter_child_nodes(expression):
        if isinstance(child, ast.expr):
            names.extend(_stub_expression_names(child))
    return names


def _stub_class_type_parameters(
    class_node: ast.ClassDef,
    type_variables: set[str],
) -> tuple[str, ...]:
    parameters = [parameter.name for parameter in class_node.type_params]
    if parameters:
        return tuple(dict.fromkeys(parameters))

    explicit_generic = []
    for base in class_node.bases:
        if isinstance(base, ast.Subscript) and _stub_base_name(base.value) == 'Generic':
            explicit_generic.extend(
                name
                for name in _stub_expression_names(base.slice)
                if name in type_variables
            )
    if explicit_generic:
        return tuple(dict.fromkeys(explicit_generic))

    inherited_generic = []
    for base in class_node.bases:
        if not isinstance(base, ast.Subscript):
            continue
        inherited_generic.extend(
            name
            for name in _stub_expression_names(base.slice)
            if name in type_variables
        )
    return tuple(dict.fromkeys(inherited_generic))


def _stub_generic_class_arguments(
    tree: ast.Module,
) -> dict[str, dict[str, tuple[ast.expr, ...]]]:
    """Index concrete in-stub specializations by class TypeVar."""
    type_variables = _stub_type_variables(tree)
    classes = _stub_class_nodes(tree)
    parameters = {
        name: _stub_class_type_parameters(class_node, type_variables)
        for name, class_node in classes.items()
    }
    generic_symbols = set(type_variables)
    for class_parameters in parameters.values():
        generic_symbols.update(class_parameters)

    arguments: dict[str, dict[str, list[ast.expr]]] = {
        name: {parameter: [] for parameter in class_parameters}
        for name, class_parameters in parameters.items()
        if class_parameters
    }
    for node in ast.walk(tree):
        if not isinstance(node, ast.Subscript):
            continue
        class_name = _stub_base_name(node.value)
        class_parameters = parameters.get(class_name, ())
        if not class_parameters:
            continue
        values = node.slice.elts if isinstance(node.slice, ast.Tuple) else (node.slice,)
        for index, parameter in enumerate(class_parameters):
            if index >= len(values):
                continue
            value = values[index]
            if isinstance(value, ast.Name) and value.id in generic_symbols:
                continue
            if _stub_base_name(value) in {'Any', 'object'}:
                continue
            arguments[class_name][parameter].append(value)
    return {
        class_name: {
            parameter: tuple(values) for parameter, values in class_arguments.items()
        }
        for class_name, class_arguments in arguments.items()
    }


def _generic_class_return_support(
    annotation: ast.expr,
    class_node: ast.ClassDef | None,
    generic_arguments: dict[str, dict[str, tuple[ast.expr, ...]]],
    class_nodes: dict[str, ast.ClassDef],
    target_type: str,
) -> bool | None:
    """Check whether stub specializations include a documented result type."""
    if class_node is None:
        return False
    if class_node.name == target_type and _ast_mentions_type(annotation, 'Self'):
        return True
    if _descriptor_returns_type(annotation, class_node, class_nodes, target_type):
        return True

    class_arguments = generic_arguments.get(class_node.name, {})
    class_parameters = tuple(class_arguments)
    referenced = set(_stub_expression_names(annotation)) & set(class_parameters)
    if not referenced:
        return False

    concrete_arguments: list[ast.expr] = []
    for parameter in referenced:
        concrete_arguments.extend(class_arguments[parameter])

    if any(
        _ast_mentions_type(argument, target_type) for argument in concrete_arguments
    ):
        return True
    if concrete_arguments:
        return False
    return None


def _descriptor_returns_type(
    annotation: ast.expr,
    owner: ast.ClassDef,
    class_nodes: dict[str, ast.ClassDef],
    target_type: str,
) -> bool:
    """Resolve a typed descriptor through its instance ``__get__`` overloads."""
    for node in ast.walk(annotation):
        if not isinstance(node, ast.Subscript):
            continue
        descriptor = class_nodes.get(_stub_base_name(node.value))
        if descriptor is None:
            continue
        for member in descriptor.body:
            if (
                not isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef))
                or member.name != '__get__'
                or member.returns is None
            ):
                continue
            parameters = [*member.args.posonlyargs, *member.args.args]
            if len(parameters) < 2 or parameters[1].annotation is None:
                continue
            instance_type = parameters[1].annotation
            if (
                isinstance(instance_type, ast.Constant) and instance_type.value is None
            ) or (isinstance(instance_type, ast.Name) and instance_type.id == 'None'):
                continue
            if _ast_mentions_type(instance_type, owner.name) and _ast_mentions_type(
                member.returns,
                target_type,
            ):
                return True
    return False


def _stub_class_call_result(
    class_name: str,
    class_node: ast.ClassDef,
    type_variables: set[str],
) -> str:
    parameters = tuple(
        _public_annotation(name)
        for name in _stub_class_type_parameters(class_node, type_variables)
    )
    return f'{class_name}[{", ".join(parameters)}]' if parameters else class_name


def _stub_mro(
    name: str,
    classes: dict[str, ast.ClassDef],
    active: frozenset[str] = frozenset(),
) -> tuple[str, ...] | None:
    if name in active:
        return None
    node = classes.get(name)
    if node is None:
        return (name,)
    bases = [
        base_name
        for expression in node.bases
        if (base_name := _stub_base_name(expression)) is not None
    ] or ['object']
    sequences = []
    for base in bases:
        base_mro = _stub_mro(base, classes, active | {name})
        if base_mro is None:
            return None
        sequences.append(list(base_mro))
    sequences.append(list(bases))
    result = [name]
    while any(sequences):
        candidate = next(
            (
                sequence[0]
                for sequence in sequences
                if sequence and not any(sequence[0] in other[1:] for other in sequences)
            ),
            None,
        )
        if candidate is None:
            return None
        result.append(candidate)
        for sequence in sequences:
            if sequence and sequence[0] == candidate:
                sequence.pop(0)
    return tuple(result)


def _stub_constructor_declarations(
    class_node: ast.ClassDef,
    name: str,
) -> list[ast.FunctionDef | ast.AsyncFunctionDef]:
    functions = [
        member
        for member in class_node.body
        if isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef))
        and member.name == name
    ]
    overloads = [
        function
        for function in functions
        if any(
            isinstance(decorator, ast.Name) and decorator.id == 'overload'
            for decorator in function.decorator_list
        )
    ]
    if overloads:
        return overloads
    return functions[-1:]


def _stub_constructor_declared(class_node: ast.ClassDef, name: str) -> bool:
    if _stub_constructor_declarations(class_node, name):
        return True

    def assigned_name(target: ast.expr) -> str | None:
        if isinstance(target, ast.Name):
            return target.id
        if isinstance(target, ast.Attribute):
            return target.attr
        return None

    return any(
        assigned_name(target) == name
        for member in class_node.body
        for target in (
            member.targets
            if isinstance(member, ast.Assign)
            else [member.target]
            if isinstance(member, ast.AnnAssign)
            else ()
        )
    )


def _stub_has_required_never_input(
    function: ast.FunctionDef | ast.AsyncFunctionDef,
    stub_module: griffe.Module,
    aliases: dict[str, Any],
    receiver: str,
) -> bool:
    args = function.args
    positional = [*args.posonlyargs, *args.args]
    defaults = [None] * (len(positional) - len(args.defaults)) + list(args.defaults)
    for index, (argument, default) in enumerate(zip(positional, defaults, strict=True)):
        if index == 0 and argument.arg == receiver:
            continue
        annotation = _stub_annotation(argument.annotation, stub_module, aliases)
        if (
            default is None
            and annotation is not None
            and annotation.rsplit('.', 1)[-1] in {'Never', 'NoReturn'}
        ):
            return True
    for argument, default in zip(args.kwonlyargs, args.kw_defaults, strict=True):
        annotation = _stub_annotation(argument.annotation, stub_module, aliases)
        if (
            default is None
            and annotation is not None
            and annotation.rsplit('.', 1)[-1] in {'Never', 'NoReturn'}
        ):
            return True
    return False


def _stub_constructor_source(
    name: str,
    classes: dict[str, ast.ClassDef],
) -> tuple[str, str, list[ast.FunctionDef | ast.AsyncFunctionDef]] | str | None:
    class_node = classes.get(name)
    if class_node is None:
        return None
    own = {
        constructor: (
            _stub_constructor_declarations(class_node, constructor),
            _stub_constructor_declared(class_node, constructor),
        )
        for constructor in ('__new__', '__init__')
    }
    own = {constructor: value for constructor, value in own.items() if value[1]}
    if len(own) > 1:
        return 'both __new__ and __init__ are declared'
    if own:
        constructor, (functions, _) = next(iter(own.items()))
        other = '__init__' if constructor == '__new__' else '__new__'
        mro = _stub_mro(name, classes)
        if mro is None:
            return 'class MRO cannot be resolved from authored bases'
        for base_name in mro[1:]:
            base = classes.get(base_name)
            if base is None:
                continue
            if _stub_constructor_declared(base, other):
                return (
                    f'{name}.{constructor} and {base_name}.{other} '
                    'both constrain construction'
                )
        return name, constructor, functions

    mro = _stub_mro(name, classes)
    if mro is None:
        return 'class MRO cannot be resolved from authored bases'
    inherited: dict[
        str,
        tuple[str, list[ast.FunctionDef | ast.AsyncFunctionDef]],
    ] = {}
    for base_name in mro[1:]:
        base = classes.get(base_name)
        if base is None:
            if base_name not in {'Generic', 'object'}:
                return None
            continue
        for constructor in ('__new__', '__init__'):
            if constructor in inherited or not _stub_constructor_declared(
                base, constructor
            ):
                continue
            inherited[constructor] = (
                base_name,
                _stub_constructor_declarations(base, constructor),
            )
    if len(inherited) > 1:
        return 'inherited __new__ and __init__ declarations both constrain construction'
    if inherited:
        constructor, (source_class, functions) = next(iter(inherited.items()))
        return source_class, constructor, functions
    return None


def _stub_constructor_contracts(
    stub_module: griffe.Module,
    aliases: dict[str, Any],
    public_names: set[str],
    tree: ast.Module | None = None,
) -> dict[str, _StubConstructor]:
    if tree is None:
        tree = ast.parse(STUB.read_text(encoding='utf-8'))
    classes = _stub_class_nodes(tree)
    type_variables = _stub_type_variables(tree)
    result: dict[str, _StubConstructor] = {}
    for class_name in sorted(public_names):
        source = _stub_constructor_source(class_name, classes)
        if source is None:
            continue
        class_path = f'gometry.{class_name}'
        if isinstance(source, str):
            result[class_path] = _StubConstructor(
                class_name,
                '',
                (),
                error=source,
            )
            continue
        source_class, constructor, functions = source
        class_result = _stub_class_call_result(
            class_name,
            classes[class_name],
            type_variables,
        )
        receiver = 'cls' if constructor == '__new__' else 'self'
        forms = []
        error = None
        for function in functions:
            if _stub_is_never(function.returns, stub_module, aliases) or (
                _stub_has_required_never_input(
                    function,
                    stub_module,
                    aliases,
                    receiver,
                )
            ):
                continue
            form = _stub_call_form(
                function,
                stub_module,
                aliases,
                receiver=receiver,
                constructor=constructor,
                class_result=class_result,
            )
            if form is None:
                error = (
                    f'{source_class}.{constructor} has no leading {receiver} receiver'
                )
                break
            forms.append(form)
        result[class_path] = _StubConstructor(
            source_class,
            constructor,
            tuple(forms),
            error=error,
        )
    return result


def _source_module_for_path(
    pkg: griffe.Module,
    path: Path,
) -> griffe.Module | None:
    package_root = ROOT / 'python' / 'gometry'
    try:
        relative = path.resolve().relative_to(package_root.resolve())
    except ValueError:
        return None
    parts = list(relative.with_suffix('').parts)
    if parts and parts[-1] == '__init__':
        parts.pop()
    module_path = '.'.join(('gometry', *parts))
    value: Any = pkg
    for part in module_path.split('.')[1:]:
        value = getattr(value, 'members', {}).get(part)
    return value if isinstance(value, griffe.Module) else None


def _namedtuple_call_form(
    class_name: str,
    class_node: ast.ClassDef,
    module: griffe.Module,
    aliases: dict[str, Any],
) -> CallForm:
    fields = [
        member
        for member in class_node.body
        if isinstance(member, ast.AnnAssign)
        and isinstance(member.target, ast.Name)
        and not member.target.id.startswith('_')
        and not (
            isinstance(member.annotation, ast.Subscript)
            and _stub_base_name(member.annotation.value) == 'ClassVar'
        )
    ]
    parameters = tuple(
        (
            'positional or keyword',
            field.target.id,
            _stub_annotation(field.annotation, module, aliases),
            _expression_text(field.value),
        )
        for field in fields
        if isinstance(field.target, ast.Name)
    )
    generic_arguments = next(
        (
            tuple(
                _public_annotation(ast.unparse(argument))
                for argument in (
                    base.slice.elts
                    if isinstance(base.slice, ast.Tuple)
                    else (base.slice,)
                )
            )
            for base in class_node.bases
            if isinstance(base, ast.Subscript)
            and _stub_base_name(base.value) == 'Generic'
        ),
        (),
    )
    result = (
        f'{class_name}[{", ".join(generic_arguments)}]'
        if generic_arguments
        else class_name
    )
    return parameters, result


def _source_constructor_contracts(
    pkg: griffe.Module,
    inventory: Any,
    aliases: dict[str, Any],
) -> dict[str, _StubConstructor]:
    result: dict[str, _StubConstructor] = {}
    for name, facade in inventory.classes.items():
        if facade.imported.source_path.resolve() == STUB.resolve():
            continue
        class_node = facade.declaration.node
        path = f'gometry.{name}'
        module = _source_module_for_path(pkg, facade.imported.source_path)
        if module is None:
            result[path] = _StubConstructor(
                name,
                '',
                (),
                error=f'cannot load source module for {facade.imported.source_path}',
            )
            continue
        namedtuple = any(
            _stub_base_name(base) == 'NamedTuple' for base in class_node.bases
        )
        if namedtuple:
            result[path] = _StubConstructor(
                name,
                'NamedTuple',
                (_namedtuple_call_form(name, class_node, module, aliases),),
            )
            continue

        phases = {
            phase: _stub_constructor_declarations(class_node, phase)
            for phase in ('__new__', '__init__')
        }
        phases = {phase: functions for phase, functions in phases.items() if functions}
        if not phases:
            continue
        if len(phases) > 1:
            result[path] = _StubConstructor(
                name,
                '',
                (),
                error='both __new__ and __init__ are declared in the source facade class',
            )
            continue
        constructor, functions = next(iter(phases.items()))
        receiver = 'cls' if constructor == '__new__' else 'self'
        forms = []
        error = None
        for function in functions:
            form = _stub_call_form(
                function,
                module,
                aliases,
                receiver=receiver,
                constructor=constructor,
                class_result=name,
            )
            if form is None:
                error = f'{name}.{constructor} has no leading {receiver} receiver'
                break
            forms.append(form)
        result[path] = _StubConstructor(name, constructor, tuple(forms), error=error)
    return result


def _source_class_call_result(class_node: ast.ClassDef) -> str:
    parameters = []
    for base in class_node.bases:
        if (
            not isinstance(base, ast.Subscript)
            or _stub_base_name(base.value) != 'Generic'
        ):
            continue
        values = base.slice.elts if isinstance(base.slice, ast.Tuple) else (base.slice,)
        parameters.extend(_public_annotation(ast.unparse(value)) for value in values)
    return (
        f'{class_node.name}[{", ".join(parameters)}]' if parameters else class_node.name
    )


def _source_namedtuple_form(
    class_node: ast.ClassDef,
    module: griffe.Module,
    aliases: dict[str, Any],
) -> CallForm:
    parameters = []
    for member in class_node.body:
        if (
            not isinstance(member, ast.AnnAssign)
            or not isinstance(member.target, ast.Name)
            or member.target.id.startswith('_')
            or (
                isinstance(member.annotation, ast.Subscript)
                and _stub_base_name(member.annotation.value) == 'ClassVar'
            )
        ):
            continue
        parameters.append((
            'positional or keyword',
            member.target.id,
            _stub_annotation(member.annotation, module, aliases),
            ast.unparse(member.value) if member.value is not None else None,
        ))
    return tuple(parameters), _source_class_call_result(class_node)


def _source_facade_constructor_contracts(
    pkg: griffe.Module,
    inventory: Any,
    aliases: dict[str, Any],
) -> dict[str, _StubConstructor]:
    package_root = (ROOT / 'python' / 'gometry').resolve()
    contracts: dict[str, _StubConstructor] = {}
    for name, facade in inventory.classes.items():
        source_path = Path(facade.imported.source_path).resolve()
        if source_path == STUB.resolve():
            continue
        relative = source_path.relative_to(package_root)
        parts = list(relative.with_suffix('').parts)
        if parts and parts[-1] == '__init__':
            parts.pop()
        module_path = '.'.join(('gometry', *parts))
        module: Any = pkg
        for part in module_path.split('.')[1:]:
            module = getattr(module, 'members', {}).get(part)
        if not isinstance(module, griffe.Module):
            contracts[f'gometry.{name}'] = _StubConstructor(
                name,
                '',
                (),
                error=f'source module {module_path!r} is missing from the Griffe model',
            )
            continue

        class_node = facade.declaration.node
        if any(_stub_base_name(base) == 'NamedTuple' for base in class_node.bases):
            contracts[f'gometry.{name}'] = _StubConstructor(
                name,
                'NamedTuple',
                (_source_namedtuple_form(class_node, module, aliases),),
            )
            continue

        phases = {
            phase: _stub_constructor_declarations(class_node, phase)
            for phase in ('__new__', '__init__')
        }
        phases = {phase: forms for phase, forms in phases.items() if forms}
        if not phases:
            continue
        if len(phases) > 1:
            contracts[f'gometry.{name}'] = _StubConstructor(
                name,
                '',
                (),
                error='both __new__ and __init__ are declared in the source facade class',
            )
            continue
        constructor, functions = next(iter(phases.items()))
        receiver = 'cls' if constructor == '__new__' else 'self'
        forms = []
        error = None
        for function in functions:
            form = _stub_call_form(
                function,
                module,
                aliases,
                receiver=receiver,
                constructor=constructor,
                class_result=_source_class_call_result(class_node),
            )
            if form is None:
                error = f'{name}.{constructor} has no leading {receiver} receiver'
                break
            forms.append(form)
        contracts[f'gometry.{name}'] = _StubConstructor(
            name,
            constructor,
            tuple(forms),
            error=error,
        )
    return contracts


def _call_form_context_defaults(
    forms: tuple[CallForm, ...],
) -> dict[str, str | None]:
    values: dict[str, str | None] = {}
    for parameters, _ in forms:
        for _, name, _, default in parameters:
            values.setdefault(name, None)
            if values[name] is None and default is not None:
                values[name] = default
    return values


def _call_form_parameter_rows(
    forms: tuple[CallForm, ...],
) -> dict[str, tuple[str, str | None]]:
    names = tuple(
        dict.fromkeys(
            parameter[1] for parameters, _ in forms for parameter in parameters
        )
    )

    result: dict[str, tuple[str, str | None]] = {}
    for name in names:
        occurrences = tuple(
            next(
                (parameter for parameter in parameters if parameter[1] == name),
                None,
            )
            for parameters, _ in forms
        )
        if any(occurrence is None for occurrence in occurrences):
            result[name] = ('varies', 'varies by call form')
            continue
        actual = [occurrence for occurrence in occurrences if occurrence is not None]
        states = {
            (
                'variadic'
                if kind in {'variadic positional', 'variadic keyword'}
                else ('defaulted' if default is not None else 'required'),
                kind if kind in {'variadic positional', 'variadic keyword'} else None,
                default,
            )
            for kind, _name, _annotation, default in actual
        }
        if len(states) != 1:
            result[name] = ('varies', 'varies by call form')
            continue
        status, _kind, default = next(iter(states))
        result[name] = (
            status,
            default
            if status == 'defaulted'
            else 'not applicable'
            if status == 'variadic'
            else None,
        )
    return result


def _public_overload_paths(
    overloads: dict[str, tuple[CallForm, ...]], public_names: set[str]
) -> set[str]:
    return {
        path
        for path in overloads
        if (parts := path.split('.'))[1] in public_names
        and not parts[-1].startswith('_')
        and not any(part.startswith('_') for part in parts[2:-1])
    }


def _python_function_sources(
    function: griffe.Function,
    pkg: griffe.Module,
    native: griffe.Module,
) -> list[tuple[griffe.Alias, griffe.Function]]:
    package_prefix = f'{pkg.path}.'
    sources: list[tuple[griffe.Alias, griffe.Function]] = []
    for alias in function.aliases.values():
        if not isinstance(alias, griffe.Alias) or alias.parent is not pkg:
            continue
        target_path = getattr(alias, 'target_path', '')
        if not target_path.startswith(package_prefix):
            continue
        try:
            target = alias.target
        except (griffe.AliasResolutionError, griffe.CyclicAliasError):
            continue
        parent = getattr(target, 'parent', None)
        if (
            isinstance(target, griffe.Function)
            and isinstance(parent, griffe.Module)
            and parent.path != native.path
            and parent.path.startswith(package_prefix)
        ):
            sources.append((alias, target))
    return sources


def _class_provenance_error(
    name: str,
    public: griffe.Class,
    inventory: Any,
) -> str | None:
    facade = inventory.classes.get(name)
    if facade is None:
        return f'{name}: public class has no source declaration in the facade inventory'
    imported = facade.imported
    package_root = (ROOT / 'python' / 'gometry').resolve()
    try:
        relative = Path(imported.source_path).resolve().relative_to(package_root)
    except ValueError:
        return f'{name}: class source is outside the Python package'
    parts = list(relative.with_suffix('').parts)
    if parts and parts[-1] == '__init__':
        parts.pop()
    module = '.'.join(('gometry', *parts))
    expected = f'{module}.{imported.declared_name}'
    source = public.extra.get('gometry', {}).get('class_source')
    if source is None:
        return f'{name}: materialized class has no declaration source location'
    if (
        getattr(source, 'qualname', None) != expected
        or Path(getattr(source, 'path', '')).resolve()
        != Path(imported.source_path).resolve()
    ):
        return (
            f'{name}: materialized class source '
            f'{getattr(source, "path", None)!r}::{getattr(source, "qualname", None)!r} '
            f'does not match {imported.source_path}::{expected}'
        )
    targets = {
        alias.target_path
        for alias in public.aliases.values()
        if isinstance(alias, griffe.Alias)
    }
    if expected not in targets:
        return (
            f'{name}: missing class provenance for {expected!r} '
            f'from {imported.source_path}'
        )
    return None


def _source_paths_match(actual: str, expected: Path) -> bool:
    actual_path = Path(actual)
    expected_path = expected.resolve()
    if actual_path.is_absolute():
        return actual_path.resolve() == expected_path
    try:
        return actual_path == expected_path.relative_to(ROOT.parent.resolve())
    except ValueError:
        return False


def _source_function_forms(
    qualname: str,
    declarations: Iterable[ast.AST],
    stub_module: griffe.Module,
    aliases: dict[str, Any],
) -> tuple[CallForm, ...]:
    functions = [
        declaration
        for declaration in declarations
        if isinstance(declaration, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]
    overloads = [
        function
        for function in functions
        if any(
            (
                decorator.id
                if isinstance(decorator, ast.Name)
                else decorator.attr
                if isinstance(decorator, ast.Attribute)
                else None
            )
            == 'overload'
            for decorator in function.decorator_list
        )
    ]
    result = []
    for function in overloads or functions[-1:]:
        positional = [*function.args.posonlyargs, *function.args.args]
        decorators = {
            decorator.id
            if isinstance(decorator, ast.Name)
            else decorator.attr
            if isinstance(decorator, ast.Attribute)
            else None
            for decorator in function.decorator_list
        }
        receiver = (
            positional[0].arg
            if (
                '.' in qualname
                and 'staticmethod' not in decorators
                and positional
                and positional[0].arg in {'self', 'cls'}
            )
            else None
        )
        if _stub_is_never(function.returns, stub_module, aliases) or (
            _stub_has_required_never_input(
                function,
                stub_module,
                aliases,
                receiver or '',
            )
        ):
            continue
        form = _stub_call_form(
            function,
            stub_module,
            aliases,
            receiver=receiver,
        )
        if form is None:
            raise ValueError(f'{qualname}: source call form has an invalid receiver')
        result.append(form)
    return tuple(result)


def _visible_member_filters() -> tuple[tuple[re.Pattern[str], bool], ...]:
    import yaml

    config = yaml.safe_load(
        (ROOT / 'properdocs.yml')
        .read_text(encoding='utf-8')
        .replace('!!python/name:', '')
    )
    plugin = next(
        item['mkdocstrings'] for item in config['plugins'] if 'mkdocstrings' in item
    )
    filters = plugin['handlers']['python']['options']['filters']
    return tuple(
        (re.compile(pattern.removeprefix('!')), not pattern.startswith('!'))
        for pattern in filters
    )


def _griffe_function(pkg: griffe.Module, path: str) -> griffe.Function | None:
    current: Any = pkg
    for component in path.removeprefix('gometry.').split('.'):
        current = getattr(current, 'members', {}).get(component)
        if isinstance(current, griffe.Alias):
            current = current.target
        if current is None:
            return None
    return current if isinstance(current, griffe.Function) else None


def _source_facade_function_contracts(
    pkg: griffe.Module,
    aliases: dict[str, Any],
    public_names: set[str],
) -> dict[str, tuple[CallForm, ...]]:
    init_path = ROOT / 'python' / 'gometry' / '__init__.py'
    package_root = init_path.parent
    tree = ast.parse(init_path.read_text(encoding='utf-8'))
    result: dict[str, tuple[CallForm, ...]] = {}
    for statement in tree.body:
        if not isinstance(statement, ast.ImportFrom):
            continue
        if statement.level:
            module_name = '.'.join((
                'gometry',
                *((statement.module or '').split('.') if statement.module else ()),
            ))
        else:
            module_name = statement.module or ''
        if not module_name.startswith('gometry'):
            continue
        relative = (
            module_name.removeprefix('gometry').removeprefix('.').replace('.', '/')
        )
        candidates = (
            package_root / f'{relative}.pyi',
            package_root / f'{relative}.py',
            package_root / relative / '__init__.py',
        )
        source_path = next((path for path in candidates if path.is_file()), None)
        if source_path is None:
            continue
        source_module = _source_module_for_path(pkg, source_path)
        if source_module is None:
            raise ValueError(f'cannot load facade function module {module_name!r}')
        source_tree = ast.parse(source_path.read_text(encoding='utf-8'))
        groups: dict[str, list[ast.FunctionDef | ast.AsyncFunctionDef]] = {}
        for declaration in source_tree.body:
            if isinstance(declaration, (ast.FunctionDef, ast.AsyncFunctionDef)):
                groups.setdefault(declaration.name, []).append(declaration)
        for imported in statement.names:
            public_name = imported.asname or imported.name
            if public_name not in public_names or public_name in result:
                continue
            declarations = groups.get(imported.name)
            if declarations:
                forms = _source_function_forms(
                    public_name,
                    declarations,
                    source_module,
                    aliases,
                )
                path = f'gometry.{public_name}'
                function = _griffe_function(pkg, path)
                if forms and function is None:
                    raise ValueError(
                        f'{path}: source facade callable is missing from Griffe'
                    )
                if forms and function.docstring is not None:
                    result[path] = forms
    return result


def source_documentation_contracts() -> SourceDocumentationContracts:
    """Extract visible call forms and authored constructor rows from source."""
    import gometry as gm

    loader = griffe.GriffeLoader(
        search_paths=[str(ROOT / 'python')],
        extensions=griffe.Extensions(PromoteStubOverloads(), ExpandTokenAliases()),
        allow_inspection=False,
    )
    pkg = loader.load('gometry')
    loader.resolve_aliases(external=False)
    lib = pkg.members.get('_lib')
    if not isinstance(lib, griffe.Module):
        raise TypeError('source call-form inventory has no gometry._lib module')

    cfg = load_config(ROOT / 'tools' / 'stubs' / 'stubconfig.py')
    tree = ast.parse(STUB.read_text(encoding='utf-8'))
    inventory = load_public_symbols(STUB, ROOT / 'python' / 'gometry' / '__init__.py')
    aliases = annotation_aliases(pkg)
    subjects = tuple(
        iter_doc_subjects(
            CheckContext(cfg),
            runtime_module=gm,
            source_classes=inventory.source_classes,
        )
    )

    result: dict[str, tuple[CallForm, ...]] = {}
    for subject in subjects:
        if subject.kind != 'callable':
            continue
        forms = _source_function_forms(
            subject.qualname,
            subject.declarations,
            lib,
            aliases,
        )
        if forms:
            path = (
                subject.qualname
                if subject.qualname.startswith('gometry.')
                else f'gometry.{subject.qualname}'
            )
            function = _griffe_function(pkg, path)
            if function is None:
                raise ValueError(f'{path}: source callable is missing from Griffe')
            # The reference handler does not display members without a docstring.
            if function.docstring is not None:
                result[path] = tuple(forms)

    result.update({
        path: forms
        for path, forms in _source_facade_function_contracts(
            pkg,
            aliases,
            set(gm.__all__),
        ).items()
        if path not in result
    })

    class_nodes = {
        node.name: node for node in tree.body if isinstance(node, ast.ClassDef)
    }
    class_nodes.update({
        name: source.node
        for name, source in inventory.source_classes.items()
        if isinstance(source.node, ast.ClassDef)
    })
    filters = _visible_member_filters()
    for class_name in gm.__all__:
        class_node = class_nodes.get(class_name)
        if class_node is None:
            continue
        groups: dict[str, list[ast.FunctionDef | ast.AsyncFunctionDef]] = {}
        for member in class_node.body:
            if isinstance(
                member, (ast.FunctionDef, ast.AsyncFunctionDef)
            ) and member.name.startswith('__'):
                groups.setdefault(member.name, []).append(member)
        for member_name, declarations in groups.items():
            visible = True
            for pattern, include in filters:
                if pattern.search(member_name):
                    visible = include
            if not visible:
                continue
            path = f'gometry.{class_name}.{member_name}'
            forms = _source_function_forms(path, declarations, lib, aliases)
            function = _griffe_function(pkg, path)
            if forms and function is None:
                raise ValueError(
                    f'{path}: source special method is missing from Griffe'
                )
            if forms and function.docstring is not None:
                result[path] = forms

    constructors = _stub_constructor_contracts(lib, aliases, set(gm.__all__), tree)
    constructors.update({
        path: contract
        for path, contract in _source_facade_constructor_contracts(
            pkg,
            inventory,
            aliases,
        ).items()
        if path not in constructors
    })
    for path, contract in constructors.items():
        if contract.error is not None:
            raise ValueError(f'{path}: {contract.error}')
        if contract.forms:
            result[path] = contract.forms

    class_subjects = {
        subject.qualname: subject for subject in subjects if subject.kind == 'class'
    }
    constructor_subjects = {
        subject.qualname.removesuffix('.__new__').removesuffix('.__init__'): subject
        for subject in subjects
        if subject.kind == 'constructor'
    }
    constructor_rows: dict[str, dict[str, tuple[str, str | None, str | None]]] = {}
    for path, forms in result.items():
        if path.count('.') != 1:
            continue
        name = path.rsplit('.', 1)[-1]
        carriers = [
            (subject.prose, subject.source)
            for subject in (
                class_subjects.get(name),
                constructor_subjects.get(name),
            )
            if subject is not None and subject.prose
        ]
        documented_names = {
            parameter
            for prose, _source in carriers
            for entry in parameter_entries(parse_doc(prose))
            for parameter in entry.names
        }
        if not documented_names:
            continue
        parameter_forms = tuple(
            tuple(
                SignatureParameter(name, kind, annotation, default)
                for kind, name, annotation, default in parameters
            )
            for parameters, _returns in forms
        )
        resolution = resolve_parameter_rows(
            path,
            carriers,
            parameter_forms,
            require_documentation=False,
        )
        if resolution.errors:
            raise ValueError('; '.join(resolution.errors))
        constructor_rows[path] = {
            row.name: (row.status, row.default, row.annotation)
            for row in resolution.rows
            if row.name in documented_names
        }
    return SourceDocumentationContracts(result, constructor_rows)


def collect_errors() -> list[str]:
    import gometry as gm

    loader = griffe.GriffeLoader(
        search_paths=[str(ROOT / 'python')],
        extensions=griffe.Extensions(PromoteStubOverloads(), ExpandTokenAliases()),
        allow_inspection=False,
    )
    pkg = loader.load('gometry')
    loader.resolve_aliases(external=False)
    lib = pkg.members.get('_lib')
    errors: list[str] = []
    cfg = load_config(ROOT / 'tools' / 'stubs' / 'stubconfig.py')
    stub_tree = ast.parse(STUB.read_text(encoding='utf-8'))
    inventory = load_public_symbols(
        STUB,
        ROOT / 'python' / 'gometry' / '__init__.py',
    )
    subjects = tuple(
        iter_doc_subjects(
            CheckContext(cfg),
            runtime_module=gm,
            source_classes=inventory.source_classes,
        )
    )
    errors.extend(_scalar_array_return_errors(subjects, stub_tree))
    errors.extend(_doc_type_expression_errors(subjects))
    python_root = ROOT / 'python'
    for subject in subjects:
        source_path = Path(subject.source.path).resolve()
        if not source_path.is_relative_to(python_root):
            continue
        if not (subject.prose or '').strip():
            errors.append(
                f'{source_path}:{subject.source.line}: {subject.qualname}: '
                'source docstring missing or empty'
            )
            continue
        errors.extend(
            f'{source_path}:{subject.source.line}: {error}'
            for error in check_doc_subject(subject)
        )
    if not isinstance(lib, griffe.Module):
        return ['transformed model has no gometry._lib module']
    aliases = annotation_aliases(pkg)
    stub_overloads = _stub_overloads(lib, aliases, stub_tree)
    public_overloads = _public_overload_paths(stub_overloads, set(gm.__all__))
    public_classes = {
        name: member
        for name, member in pkg.members.items()
        if name in gm.__all__ and isinstance(member, griffe.Class)
    }
    stub_constructors = _stub_constructor_contracts(
        lib,
        aliases,
        set(gm.__all__),
        stub_tree,
    )
    source_constructors = _source_facade_constructor_contracts(
        pkg,
        inventory,
        aliases,
    )
    stub_constructors.update({
        path: contract
        for path, contract in source_constructors.items()
        if path not in stub_constructors
    })

    # The import inventory is authoritative: every flat public native
    # function/class must still be represented by its same-named native member;
    # Python-defined functions carry provenance through their facade alias.
    for name in gm.__all__:
        public = pkg.members.get(name)
        native = lib.members.get(name)
        if public is None:
            errors.append(f'{name}: missing public/native model member')
            continue
        if name in inventory.classes and not isinstance(public, griffe.Class):
            errors.append(f'{name}: public source class was not materialized')
        if public.canonical_path != f'gometry.{name}':
            errors.append(f'{name}: canonical path is {public.canonical_path!r}')
        if isinstance(public, griffe.Function):
            python_sources = _python_function_sources(public, pkg, lib)
            if python_sources:
                for alias, source in python_sources:
                    if source.canonical_path != alias.target_path:
                        errors.append(
                            f'{name}: Python target canonical path is '
                            f'{source.canonical_path!r}'
                        )
            elif not isinstance(native, griffe.Function):
                errors.append(f'{name}: missing native function provenance')
        elif isinstance(public, griffe.Class):
            if provenance_error := _class_provenance_error(name, public, inventory):
                errors.append(provenance_error)

    model_overloads: set[str] = set()
    for function in _functions(pkg, set()):
        if function.name.startswith('_'):
            continue
        path = function.canonical_path
        if path in stub_overloads:
            if path in public_overloads:
                model_overloads.add(path)
            actual_forms = tuple(
                _griffe_call_form(variant) for variant in function.overloads or ()
            )
            if actual_forms != stub_overloads[path]:
                errors.append(
                    f'{path}: accepted call forms differ from the authored overload sequence'
                )
        variants = [function, *(function.overloads or ())]
        if any(_is_never(variant) for variant in variants):
            errors.append(f'{function.canonical_path}: public Never overload')
        for variant in variants:
            annotations = [parameter.annotation for parameter in variant.parameters]
            annotations.append(variant.returns)
            # Private TypeAlias names are implementation details; their expanded
            # members, rather than the alias spelling, determine the public type.
            private_names = set()
            for annotation in annotations:
                expressions = _expressions(annotation)
                if any(expr.name.startswith('_') for expr in expressions):
                    expressions = _expressions(
                        expand_annotation(copy.deepcopy(annotation), aliases)
                    )
                private_names.update(
                    expr.name for expr in expressions if expr.name.startswith('_')
                )
            private = sorted(private_names)
            if private:
                errors.append(
                    f'{function.canonical_path}: private annotations {private}'
                )

    errors.extend(
        f'{path}: accepted overload group missing from the Griffe model'
        for path in sorted(public_overloads - model_overloads)
        if stub_overloads[path]
    )

    for path, expected in sorted(stub_constructors.items()):
        class_name = path.rsplit('.', 1)[-1]
        class_obj = public_classes.get(class_name)
        if expected.error is not None:
            errors.append(f'{path}: {expected.error}')
            continue
        binding = constructor_binding(class_obj) if class_obj is not None else None
        if not expected.forms:
            if binding is not None:
                errors.append(f'{path}: prohibited constructor was exposed')
            continue
        expected_defaults = _call_form_context_defaults(expected.forms)
        expected_rows = _call_form_parameter_rows(expected.forms)
        if class_obj is None:
            errors.append(f'{path}: constructor class is missing from the Griffe model')
            continue
        if binding is None:
            errors.append(
                f'{path}: accepted class-call forms are missing from the Griffe model'
            )
            continue

        actual_forms = tuple(
            _griffe_call_form(form) for form in binding.overloads or ()
        )
        if actual_forms != expected.forms:
            errors.append(
                f'{path}: class-call forms differ from the authored constructor sequence'
            )
        constructor_data = binding.extra.get('gometry', {})
        parameter_resolution = constructor_data.get('parameter_resolution')
        if parameter_resolution is None:
            errors.append(f'{path}: constructor parameter rows were not resolved')
        else:
            errors.extend(f'{path}: {error}' for error in parameter_resolution.errors)
            actual_rows = {
                row.name: (row.status, row.default) for row in parameter_resolution.rows
            }
            if actual_rows != expected_rows:
                errors.append(
                    f'{path}: prose-row applicability differs from the authored call forms'
                )
        source = constructor_data.get('source_function')
        if expected.name == 'NamedTuple':
            class_source = constructor_data.get('class_source')
            facade = inventory.classes.get(class_name)
            if (
                class_source is None
                or facade is None
                or class_source.qualname != path
                or not _source_paths_match(
                    class_source.path,
                    facade.imported.source_path,
                )
            ):
                errors.append(
                    f'{path}: NamedTuple constructor has incorrect source provenance'
                )
        elif (
            not isinstance(source, griffe.Function)
            or source.name != expected.name
            or not isinstance(source.parent, griffe.Class)
            or source.parent.name != expected.source_class
        ):
            errors.append(
                f'{path}: constructor binding has incorrect source provenance'
            )
        else:
            facade = inventory.classes.get(class_name)
            source_declaration = source.extra.get('gometry', {}).get(
                'declaration_source'
            )
            if facade is not None and (
                source_declaration is None
                or not _source_paths_match(
                    source_declaration.path,
                    facade.imported.source_path,
                )
            ):
                errors.append(
                    f'{path}: constructor declaration came from '
                    f'{getattr(source_declaration, "path", None)!r}, expected '
                    f'{facade.imported.source_path!r}'
                )
        if binding in class_obj.members.values():
            errors.append(f'{path}: private constructor binding became a public member')

        actual_defaults = {
            parameter.name: _expression_text(parameter.default)
            for parameter in binding.parameters
        }
        if actual_defaults != expected_defaults:
            errors.append(f'{path}: constructor parameter context defaults differ')

        sections = constructor_data.get('docstring_sections', ())
        rendered_defaults: dict[str, str | None] = {}
        for section in sections:
            if section.kind not in {
                griffe.DocstringSectionKind.parameters,
                griffe.DocstringSectionKind.other_parameters,
            }:
                continue
            for parameter in section.value:
                actual = (
                    parameter.value
                    if isinstance(parameter.value, str)
                    else _expression_text(parameter.value)
                )
                expected_row = expected_rows.get(parameter.name)
                expected_default = expected_row[1] if expected_row is not None else None
                if expected_row is not None and actual != expected_default:
                    errors.append(
                        f'{path}({parameter.name}): rendered documentation default '
                        f'{actual!r} differs from call-form status '
                        f'{expected_row[0]!r} ({expected_default!r})'
                    )
                key = f'{section.kind.value}:{parameter.name}'
                if key in rendered_defaults:
                    errors.append(f'{path}({parameter.name}): duplicate parameter row')
                rendered_defaults[key] = actual

    expected_constructor_paths = set(stub_constructors)
    for name, class_obj in public_classes.items():
        path = f'gometry.{name}'
        if (
            path not in expected_constructor_paths
            and constructor_binding(class_obj) is not None
        ):
            errors.append(
                f'{path}: constructor binding has no authored local provenance'
            )

    cell_array = pkg.members.get('CellArray')
    if isinstance(cell_array, griffe.Class):
        for name, geometry_type in (('center', 'Point'), ('polygon', 'Polygon')):
            attribute = cell_array.members.get(name)
            expected = f'GeometryArray[{geometry_type} | None]'
            if not isinstance(attribute, griffe.Attribute):
                errors.append(f'gometry.CellArray.{name}: property reference missing')
                continue
            if (
                attribute.canonical_path != f'gometry.CellArray.{name}'
                or _public_annotation(str(attribute.annotation)) != expected
            ):
                errors.append(
                    f'gometry.CellArray.{name}: property annotation/path is not '
                    f'{expected} at its public anchor'
                )
            if not {'property', 'readonly'}.issubset(attribute.labels):
                errors.append(
                    f'gometry.CellArray.{name}: property/read-only labels missing'
                )
            if attribute.docstring is None or not attribute.docstring.value.strip():
                errors.append(f'gometry.CellArray.{name}: property prose missing')

    for module_name in ('_lib', '_types'):
        module = pkg.members.get(module_name)
        if not isinstance(module, griffe.Module):
            continue
        for name in gm.__all__:
            member = module.members.get(name)
            if (
                isinstance(member, griffe.Alias)
                and name == 'Cell'
                and member.canonical_path != f'gometry.{name}'
            ):
                errors.append(
                    f'{module_name}.{name}: canonical path is {member.canonical_path!r}'
                )
    return errors


def main() -> int:
    _TOOLS = ROOT / 'tools'
    if str(_TOOLS) not in sys.path:
        sys.path.insert(0, str(_TOOLS))
    from _gatelib import prepend_tools_import_paths
    from runtime_artifact import require_runtime

    prepend_tools_import_paths()
    require_runtime(ROOT)
    errors = collect_errors()
    if errors:
        print(f'doc model: {len(errors)} issue(s):', file=sys.stderr)
        for error in errors:
            print(f'  {error}', file=sys.stderr)
        return 1
    print('doc model: OK')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
