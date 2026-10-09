#!/usr/bin/env python3
"""Validate the transformed Griffe model used by the documentation build."""

from __future__ import annotations

import ast
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
    attribute_entries,
    check_doc_subject,
    iter_doc_subjects,
    parameter_entries,
    parse_doc,
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
    '_GroupValuesT_co': 'GroupValuesT_co',
}

ParameterForm = tuple[str | None, str, str | None, str | None]
CallForm = tuple[tuple[ParameterForm, ...], str | None]


@dataclass(frozen=True)
class _StubConstructor:
    source_class: str
    name: str
    forms: tuple[CallForm, ...]
    error: str | None = None


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


def _scalar_array_return_errors(subjects: Iterable[DocSubject]) -> list[str]:
    """Keep return prose aligned with the selected binding declarations."""
    errors: list[str] = []
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
            any(
                isinstance(node, ast.Name) and node.id == 'GeometryArray'
                for node in ast.walk(annotation)
            )
            for annotation in declarations
        ):
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


def _call_form_defaults(
    forms: tuple[CallForm, ...],
) -> tuple[dict[str, str | None], list[str]]:
    values: dict[str, str | None] = {}
    for parameters, _ in forms:
        for _, name, _, default in parameters:
            values.setdefault(name, None)
            if values[name] is None and default is not None:
                values[name] = default
    return values, []


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
    errors.extend(_scalar_array_return_errors(subjects))
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
    stub_tree = ast.parse(STUB.read_text(encoding='utf-8'))
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

    # The import inventory is authoritative: every flat public native
    # function/class must still be represented by its same-named native member;
    # Python-defined functions carry provenance through their facade alias.
    for name in gm.__all__:
        public = pkg.members.get(name)
        native = lib.members.get(name)
        if public is None:
            errors.append(f'{name}: missing public/native model member')
            continue
        # Structured result classes are intentionally private-stub-owned; they
        # have no native class and are still public canonical anchors.
        stub_owned = name in {'Extremes', 'Features', 'PolygonizeResult'}
        if public.canonical_path != f'gometry.{name}' and not stub_owned:
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
            targets = {
                alias.target_path
                for alias in public.aliases.values()
                if isinstance(alias, griffe.Alias)
            }
            if f'gometry._lib.{name}' not in targets and (
                name == 'Cell' and f'gometry._types.{name}' not in targets
            ):
                errors.append(f'{name}: missing native class provenance')

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
            private = sorted({
                expr.name
                for annotation in annotations
                for expr in _expressions(annotation)
                if expr.name.startswith('_')
            })
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
        expected_defaults, conflicts = _call_form_defaults(expected.forms)
        errors.extend(f'{path}: {conflict}' for conflict in conflicts)
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
        source = constructor_data.get('source_function')
        if (
            not isinstance(source, griffe.Function)
            or source.name != expected.name
            or not isinstance(source.parent, griffe.Class)
            or source.parent.name != expected.source_class
        ):
            errors.append(
                f'{path}: constructor binding has incorrect source provenance'
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
                actual = _expression_text(parameter.value)
                expected_default = expected_defaults.get(parameter.name)
                if parameter.name in expected_defaults and actual != expected_default:
                    errors.append(
                        f'{path}({parameter.name}): parsed documentation default '
                        f'{actual!r} differs from {expected_default!r}'
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
