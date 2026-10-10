"""Minimal Griffe repairs for a handwritten extension-module stub.

Griffe does not create members for overload-only ``.pyi`` definitions and
cannot resolve aliases to those missing members.  The public facade also
re-exports Python-defined adapter functions from private modules, so those
functions need public canonical model paths before the reference is rendered.
The docs also expand private annotation aliases so reference signatures
describe accepted values rather than exposing implementation names. Public API
prose remains untouched.
"""

from __future__ import annotations

import builtins
import copy
import re
from dataclasses import dataclass
from dataclasses import fields as dataclass_fields
from functools import partial
from pathlib import Path
from typing import Any, Literal

import griffe

_PRIVATE_TYPEVAR_RENAMES = {
    '_CellT': 'CellT',
    '_DefaultT': 'DefaultT',
    '_ExtremeT': 'ExtremeT',
    '_CellT_co': 'CellT_co',
    '_GeometryOtherT': 'GeometryOtherT',
    '_GeometryRowT': 'GeometryRowT',
    '_GeometryT': 'GeometryT',
    '_GeometryT_co': 'GeometryT_co',
    '_GroupKeyT': 'GroupKeyT',
    '_GroupValuesT_co': 'GroupValuesT_co',
}
_SEE_ALSO_ENTRY = re.compile(
    r'(?:^|(?<=\. ))(?P<ticks>`?)'
    r'(?P<name>(?:gm\.)?[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)'
    r'(?P=ticks)(?P<sep>[ \t]*(?:\n[ \t]*)?:)(?=[ \t]|$)',
    re.MULTILINE,
)
_CONSTRUCTOR_NAMESPACE = 'gometry'
_CONSTRUCTOR_KEY = 'constructor'


@dataclass(frozen=True, slots=True)
class ConstructorSource:
    qualname: str
    path: str
    line: int


@dataclass(frozen=True, slots=True)
class UnavailableConstructor:
    state: Literal['unavailable']
    reason: str
    source: ConstructorSource


@dataclass(frozen=True, slots=True)
class RenderableConstructor:
    state: Literal['renderable']
    binding: griffe.Function
    source: ConstructorSource


@dataclass(frozen=True, slots=True)
class UnsupportedConstructor:
    state: Literal['unsupported']
    reason: str
    source: ConstructorSource


ConstructorOutcome = (
    UnavailableConstructor | RenderableConstructor | UnsupportedConstructor
)


class UnsupportedConstructorError(RuntimeError):
    """An exported constructor cannot be represented by the reference adapter."""


def _is_never(function: griffe.Function) -> bool:
    returns = function.returns
    return isinstance(returns, griffe.ExprName) and returns.name in {
        'Never',
        'NoReturn',
    }


def constructor_outcome(obj: griffe.Class) -> ConstructorOutcome | None:
    """Return the constructor adapter state attached to a public class."""
    outcome = obj.extra.get(_CONSTRUCTOR_NAMESPACE, {}).get(_CONSTRUCTOR_KEY)
    if isinstance(
        outcome,
        UnavailableConstructor | RenderableConstructor | UnsupportedConstructor,
    ):
        return outcome
    return None


def constructor_binding(obj: griffe.Class) -> griffe.Function | None:
    """Return the rendered class-call model, if the class has one."""
    outcome = constructor_outcome(obj)
    return outcome.binding if isinstance(outcome, RenderableConstructor) else None


def ordered_call_forms(
    obj: griffe.Function | griffe.Class | ConstructorOutcome,
) -> tuple[griffe.Function, ...]:
    """Return exact visible forms in declaration order, without a prose merge."""
    if isinstance(obj, RenderableConstructor):
        return tuple(obj.binding.overloads or (obj.binding,))
    if isinstance(obj, UnsupportedConstructor | UnavailableConstructor):
        return ()
    if isinstance(obj, griffe.Class):
        outcome = constructor_outcome(obj)
        return ordered_call_forms(outcome) if outcome is not None else ()
    return tuple(obj.overloads or (obj,))


def _constructor_source(obj: Any) -> ConstructorSource:
    namespace = getattr(obj, 'extra', {}).get(_CONSTRUCTOR_NAMESPACE, {})
    declaration = namespace.get('declaration_source')
    if isinstance(declaration, ConstructorSource):
        return ConstructorSource(
            getattr(obj, 'path', declaration.qualname),
            declaration.path,
            declaration.line,
        )
    path = getattr(obj, 'filepath', None) or getattr(obj, 'relative_filepath', None)
    return ConstructorSource(
        qualname=getattr(obj, 'path', getattr(obj, 'name', '<unknown>')),
        path=str(path or '<unknown>'),
        line=getattr(obj, 'lineno', None) or 1,
    )


def _class_source(obj: griffe.Class) -> ConstructorSource:
    source = obj.extra.get(_CONSTRUCTOR_NAMESPACE, {}).get('class_source')
    if isinstance(source, ConstructorSource):
        return ConstructorSource(obj.path, source.path, source.line)
    return _constructor_source(obj)


def _unsupported_constructor_error(outcome: UnsupportedConstructor) -> str:
    source = outcome.source
    return (
        f'{source.path}:{source.line}: {source.qualname}: '
        f'unsupported class-call signature: {outcome.reason}'
    )


def _module_type_variables(module: griffe.Module) -> set[str]:
    names = set()
    modules = [module]
    modules.extend(
        member
        for name in ('_lib', '_types')
        if isinstance((member := module.members.get(name)), griffe.Module)
    )
    for source in modules:
        for name, member in source.members.items():
            if isinstance(member, griffe.Alias):
                continue
            value = getattr(member, 'value', None)
            function = getattr(value, 'function', None)
            function_name = getattr(function, 'name', None)
            if function_name in {
                'TypeVar',
                'TypeVarTuple',
                'ParamSpec',
                '_DefaultTypeVar',
            }:
                names.add(name)
    return names


def _expression_names(expression: Any) -> list[str]:
    if isinstance(expression, griffe.ExprName):
        return [expression.name]
    names = []
    for attribute in ('left', 'right', 'slice', 'elements'):
        child = getattr(expression, attribute, None)
        if isinstance(child, list):
            for item in child:
                names.extend(_expression_names(item))
        elif child is not None:
            names.extend(_expression_names(child))
    return names


def _class_type_parameters(
    obj: griffe.Class,
    type_variables: set[str],
) -> tuple[str, ...]:
    names = [parameter.name for parameter in obj.type_parameters]
    if not names:
        for base in obj.bases:
            if not isinstance(base, griffe.ExprSubscript):
                continue
            names.extend(
                name for name in _expression_names(base.slice) if name in type_variables
            )
    return tuple(
        dict.fromkeys(_PRIVATE_TYPEVAR_RENAMES.get(name, name) for name in names)
    )


def _copy_expression(expression: Any) -> Any:
    if expression is None or isinstance(expression, str):
        return expression
    if isinstance(expression, griffe.ExprName):
        return griffe.ExprName(
            expression.name,
            parent=expression.parent,
            member=expression.member,
        )
    copied = copy.copy(expression)
    for field in dataclass_fields(expression):
        value = getattr(expression, field.name)
        if isinstance(value, griffe.Expr):
            value = _copy_expression(value)
        elif isinstance(value, list):
            value = [
                _copy_expression(item) if isinstance(item, griffe.Expr) else item
                for item in value
            ]
        elif isinstance(value, tuple):
            value = tuple(
                _copy_expression(item) if isinstance(item, griffe.Expr) else item
                for item in value
            )
        setattr(copied, field.name, value)
    return copied


def _copy_extra(extra: dict[str, Any]) -> dict[str, Any]:
    return {
        name: _copy_extra(value) if isinstance(value, dict) else value
        for name, value in extra.items()
    }


def _copy_docstring(docstring: Any, parent: Any) -> Any:
    if docstring is None:
        return None
    return griffe.Docstring(
        docstring.value,
        lineno=docstring.lineno,
        endlineno=docstring.endlineno,
        parent=parent,
        parser=docstring.parser,
        parser_options=docstring.parser_options,
    )


def _copy_function_for_owner(
    function: griffe.Function,
    parent: Any,
    *,
    name: str | None = None,
    seen: dict[int, griffe.Function] | None = None,
) -> griffe.Function:
    if seen is None:
        seen = {}
    if id(function) in seen:
        return seen[id(function)]
    local = copy.copy(function)
    seen[id(function)] = local
    local.name = function.name if name is None else name
    local.parent = parent
    local.extra = _copy_extra(function.extra)
    namespace = dict(local.extra.get(_CONSTRUCTOR_NAMESPACE, {}))
    namespace['declaration_source'] = ConstructorSource(
        function.path,
        str(getattr(function, 'filepath', None) or function.relative_filepath),
        function.lineno or 1,
    )
    local.extra[_CONSTRUCTOR_NAMESPACE] = namespace
    local.parameters = griffe.Parameters(
        *(
            griffe.Parameter(
                parameter.name,
                annotation=_copy_expression(parameter.annotation),
                kind=parameter.kind,
                default=_copy_expression(parameter.default),
                docstring=parameter.docstring,
            )
            for parameter in function.parameters
        )
    )
    local.overloads = [
        _copy_function_for_owner(overload, parent, name=local.name, seen=seen)
        for overload in function.overloads or ()
        if not _is_never(overload)
    ]
    local.docstring = _copy_docstring(function.docstring, local)
    return local


def _copy_class_member(member: Any, parent: griffe.Class) -> Any:
    if isinstance(member, griffe.Function):
        return _copy_function_for_owner(member, parent)
    if isinstance(member, griffe.Class):
        return _copy_class_for_owner(member, member.name, parent)
    local = copy.copy(member)
    local.parent = parent
    if hasattr(member, 'extra'):
        local.extra = _copy_extra(member.extra)
    if isinstance(member, griffe.Attribute):
        local.annotation = _copy_expression(member.annotation)
        local.value = _copy_expression(member.value)
        local.labels = set(member.labels)
    if hasattr(member, 'docstring'):
        local.docstring = _copy_docstring(member.docstring, local)
    return local


def _copy_class_for_owner(
    source: griffe.Class,
    name: str,
    parent: Any,
) -> griffe.Class:
    local = copy.copy(source)
    local.aliases = dict(source.aliases)
    local.name = name
    local.parent = parent
    local.extra = _copy_extra(source.extra)
    source_location = ConstructorSource(
        source.path,
        str(getattr(source, 'filepath', None) or _constructor_source(source).path),
        getattr(source, 'lineno', None) or 1,
    )
    namespace = dict(local.extra.get(_CONSTRUCTOR_NAMESPACE, {}))
    namespace['class_source'] = source_location
    local.extra[_CONSTRUCTOR_NAMESPACE] = namespace
    local.bases = [_copy_expression(base) for base in source.bases]
    local.members = {
        member_name: _copy_class_member(member, local)
        for member_name, member in source.members.items()
    }
    local.docstring = _copy_docstring(source.docstring, local)
    return local


def _class_call_result(
    obj: griffe.Class,
    type_variables: set[str],
) -> Any:
    result: Any = griffe.ExprName(obj.name)
    type_parameters = _class_type_parameters(obj, type_variables)
    if type_parameters:
        arguments = [griffe.ExprName(name) for name in type_parameters]
        result = griffe.ExprSubscript(
            result,
            arguments[0] if len(arguments) == 1 else griffe.ExprTuple(arguments),
        )
    return result


def _replace_self(expression: Any, result: Any) -> Any:
    if isinstance(expression, griffe.ExprName) and expression.name == 'Self':
        return _copy_expression(result)
    if isinstance(expression, griffe.ExprBinOp):
        expression.left = _replace_self(expression.left, result)
        expression.right = _replace_self(expression.right, result)
    elif isinstance(expression, griffe.ExprSubscript):
        expression.left = _replace_self(expression.left, result)
        expression.slice = _replace_self(expression.slice, result)
    elif isinstance(expression, griffe.ExprTuple):
        expression.elements = [
            _replace_self(element, result) for element in expression.elements
        ]
    return expression


def _has_required_never_input(function: griffe.Function) -> bool:
    for parameter in function.parameters:
        if parameter.kind is not None and parameter.kind.value.startswith('variadic'):
            continue
        if (
            parameter.default is None
            and isinstance(parameter.annotation, griffe.ExprName)
            and parameter.annotation.name in {'Never', 'NoReturn'}
        ):
            return True
    return False


def _class_call_form(
    function: griffe.Function,
    class_obj: griffe.Class,
    constructor_name: str,
    type_variables: set[str],
) -> griffe.Function | None:
    receiver = 'cls' if constructor_name == '__new__' else 'self'
    parameters = list(function.parameters)
    if (
        not parameters
        or parameters[0].name != receiver
        or parameters[0].kind is None
        or parameters[0].kind.value not in {'positional-only', 'positional or keyword'}
    ):
        return None

    copied_parameters = griffe.Parameters(
        *(
            griffe.Parameter(
                parameter.name,
                annotation=_copy_expression(parameter.annotation),
                kind=parameter.kind,
                default=_copy_expression(parameter.default),
                docstring=parameter.docstring,
            )
            for parameter in parameters[1:]
        )
    )
    class_result = _class_call_result(class_obj, type_variables)
    returns = (
        class_result
        if constructor_name == '__init__'
        else _replace_self(_copy_expression(function.returns), class_result)
    )
    # The class handler suppresses returns for __init__; this private callable
    # keeps the class-call result visible while the template supplies its name.
    form = griffe.Function(
        '__call__',
        parameters=copied_parameters,
        returns=returns,
        type_parameters=class_obj.type_parameters,
        parent=class_obj,
        lineno=function.lineno,
        endlineno=function.endlineno,
        runtime=function.runtime,
        lines_collection=function.lines_collection,
        modules_collection=function.modules_collection,
        git_info=function.git_info,
        analysis=function.analysis,
    )
    form.extra[_CONSTRUCTOR_NAMESPACE] = {'class_call_form': True}
    return form


def _documentation_parameters(
    forms: tuple[griffe.Function, ...],
) -> griffe.Parameters:
    collected: dict[str, list[griffe.Parameter]] = {}
    for form in forms:
        for parameter in form.parameters:
            collected.setdefault(parameter.name, []).append(parameter)

    parameters = []
    for name, variants in collected.items():
        annotations: list[Any] = []
        annotation_text: set[str] = set()
        for parameter in variants:
            if parameter.annotation is None:
                continue
            text = str(parameter.annotation)
            if text not in annotation_text:
                annotation_text.add(text)
                annotations.append(_copy_expression(parameter.annotation))
        annotation = None
        if len(annotations) == 1:
            annotation = annotations[0]
        elif annotations and all(isinstance(item, str) for item in annotations):
            annotation = ' | '.join(annotations)
        elif annotations and all(not isinstance(item, str) for item in annotations):
            annotation = annotations[0]
            for item in annotations[1:]:
                annotation = griffe.ExprBinOp(annotation, '|', item)

        default = next(
            (
                parameter.default
                for parameter in variants
                if parameter.default is not None
            ),
            None,
        )
        parameters.append(
            griffe.Parameter(
                name,
                annotation=annotation,
                kind=variants[0].kind,
                default=_copy_expression(default),
            )
        )
    return griffe.Parameters(*parameters)


def _local_constructor_function(
    member: Any,
    package_path: str,
) -> griffe.Function | None:
    if isinstance(member, griffe.Alias):
        try:
            member = member.target
        except (griffe.AliasResolutionError, griffe.CyclicAliasError):
            return None
    if not isinstance(member, griffe.Function):
        return None
    parent = member.parent
    if not isinstance(parent, griffe.Class) or not parent.path.startswith(
        f'{package_path}.'
    ):
        return None
    return member


def _builtin_exception_base(
    class_obj: griffe.Class,
    seen: set[int] | None = None,
) -> str | None:
    if seen is None:
        seen = set()
    if id(class_obj) in seen:
        return None
    seen.add(id(class_obj))
    module = class_obj.module
    for base in class_obj.bases:
        if not isinstance(base, griffe.ExprName):
            continue
        builtin_type = getattr(builtins, base.name, None)
        if isinstance(builtin_type, type) and issubclass(builtin_type, BaseException):
            return base.name
        member = module.members.get(base.name)
        if isinstance(member, griffe.Alias):
            try:
                member = member.target
            except (griffe.AliasResolutionError, griffe.CyclicAliasError):
                continue
        if isinstance(member, griffe.Class) and (
            exception_base := _builtin_exception_base(member, seen)
        ):
            return exception_base
    return None


def _class_import_target(class_obj: griffe.Class, name: str) -> str | None:
    root = class_obj.module
    modules = [root]
    modules.extend(
        member
        for member in root.members.values()
        if isinstance(member, griffe.Module) and member.path.startswith(f'{root.path}.')
    )
    for module in modules:
        alias = module.members.get(name)
        if isinstance(alias, griffe.Alias):
            return alias.target_path
    return None


def _named_tuple_base_path(class_obj: griffe.Class, base: Any) -> str | None:
    if isinstance(base, griffe.ExprSubscript):
        base = base.left
    if not isinstance(base, griffe.ExprName):
        return None
    canonical = base.canonical_path
    if canonical in {'typing.NamedTuple', 'typing_extensions.NamedTuple'}:
        return canonical
    if canonical == 'typing.Generic':
        return canonical
    return _class_import_target(class_obj, base.name) or canonical


def _is_named_tuple_class(class_obj: griffe.Class) -> bool:
    return any(
        _named_tuple_base_path(class_obj, base)
        in {'typing.NamedTuple', 'typing_extensions.NamedTuple'}
        for base in class_obj.bases
    )


def _named_tuple_constructor(
    class_obj: griffe.Class,
    type_variables: set[str],
) -> RenderableConstructor | UnsupportedConstructor:
    source = _class_source(class_obj)
    allowed_bases = {
        'typing.NamedTuple',
        'typing_extensions.NamedTuple',
        'typing.Generic',
    }
    if any(
        _named_tuple_base_path(class_obj, base) not in allowed_bases
        for base in class_obj.bases
    ):
        return UnsupportedConstructor(
            'unsupported',
            'NamedTuple constructor inheritance is not a direct NamedTuple/Generic declaration',
            source,
        )
    if '__new__' in class_obj.members or '__init__' in class_obj.members:
        return UnsupportedConstructor(
            'unsupported',
            'a NamedTuple subclass overrides a synthesized constructor phase',
            source,
        )

    parameters = griffe.Parameters(
        *(
            griffe.Parameter(
                name,
                annotation=_copy_expression(member.annotation),
                kind=griffe.ParameterKind.positional_or_keyword,
                default=_copy_expression(member.value),
                docstring=member.docstring,
            )
            for name, member in class_obj.members.items()
            if (
                not name.startswith('_')
                and isinstance(member, griffe.Attribute)
                and member.annotation is not None
            )
        )
    )
    form = griffe.Function(
        '__call__',
        parameters=parameters,
        returns=_class_call_result(class_obj, type_variables),
        type_parameters=class_obj.type_parameters,
        parent=class_obj,
        lineno=class_obj.lineno,
        endlineno=class_obj.endlineno,
        runtime=class_obj.runtime,
        lines_collection=class_obj.lines_collection,
        modules_collection=class_obj.modules_collection,
        git_info=class_obj.git_info,
        analysis=class_obj.analysis,
    )
    form.extra[_CONSTRUCTOR_NAMESPACE] = {'class_call_form': True}
    binding = griffe.Function(
        '__call__',
        parameters=_documentation_parameters((form,)),
        returns=_class_call_result(class_obj, type_variables),
        type_parameters=class_obj.type_parameters,
        parent=class_obj,
        lineno=class_obj.lineno,
        endlineno=class_obj.endlineno,
        runtime=class_obj.runtime,
        lines_collection=class_obj.lines_collection,
        modules_collection=class_obj.modules_collection,
        git_info=class_obj.git_info,
        analysis=class_obj.analysis,
    )
    binding.overloads = [form]
    binding.extra[_CONSTRUCTOR_NAMESPACE] = {
        'source_function': None,
        'constructor_name': 'NamedTuple',
        'class_source': source,
        'source_kind': 'namedtuple',
    }
    return RenderableConstructor('renderable', binding, source)


def _inherited_constructor_owner(member: Any) -> griffe.Class | None:
    if isinstance(member, griffe.Alias):
        try:
            member = member.target
        except (griffe.AliasResolutionError, griffe.CyclicAliasError):
            return None
    if not isinstance(member, griffe.Function):
        return None
    parent = member.parent
    return parent if isinstance(parent, griffe.Class) else None


def _constructor_outcome(
    class_obj: griffe.Class,
    package_path: str,
    inherited_members: dict[str, Any] | None = None,
    type_variables: set[str] | None = None,
) -> ConstructorOutcome:
    class_source = _class_source(class_obj)

    def unavailable(reason: str) -> UnavailableConstructor:
        return UnavailableConstructor('unavailable', reason, class_source)

    def unsupported(
        reason: str,
        source_obj: Any = class_obj,
    ) -> UnsupportedConstructor:
        return UnsupportedConstructor(
            'unsupported',
            reason,
            _constructor_source(source_obj),
        )

    if type_variables is None:
        type_variables = _module_type_variables(class_obj.module)
    inherited_members = (
        class_obj.inherited_members if inherited_members is None else inherited_members
    )
    if _is_named_tuple_class(class_obj):
        return _named_tuple_constructor(class_obj, type_variables)

    effective: dict[str, tuple[griffe.Function | None, bool]] = {}
    for name in ('__new__', '__init__'):
        if name in class_obj.members:
            effective[name] = (
                _local_constructor_function(class_obj.members[name], package_path),
                True,
            )
        elif name in inherited_members:
            effective[name] = (
                _local_constructor_function(inherited_members[name], package_path),
                False,
            )

    if not effective:
        if exception_base := _builtin_exception_base(class_obj):
            return unavailable(
                f'opaque inherited built-in exception constructor '
                f'{exception_base}; its standard exception argument semantics '
                'are not restated as a Gometry signature'
            )
        base_names = {
            base.name for base in class_obj.bases if isinstance(base, griffe.ExprName)
        }
        if not base_names or base_names <= {'object', 'Protocol'}:
            return unavailable(
                'the class declares no package-owned __new__ or __init__ signature'
            )
        return unsupported(
            'the class inherits from a non-exception base whose constructor '
            'cannot be resolved'
        )
    unresolved_direct = [
        name
        for name, (function, direct) in effective.items()
        if direct and function is None
    ]
    if unresolved_direct:
        return unsupported(
            f'cannot resolve the direct {unresolved_direct[0]} declaration'
        )

    local = [(name, function) for name, (function, _) in effective.items() if function]
    if not local:
        if any(
            owner is not None and owner.name == 'object'
            for owner in (
                _inherited_constructor_owner(inherited_members.get(name))
                for name in ('__new__', '__init__')
            )
        ):
            return unavailable(
                'the class uses default object construction and declares no '
                'package-owned class-call signature'
            )
        if exception_base := _builtin_exception_base(class_obj):
            return unavailable(
                f'opaque inherited built-in exception constructor '
                f'{exception_base}; its standard exception argument semantics '
                'are not restated as a Gometry signature'
            )
        return unsupported(
            'cannot resolve an inherited external constructor into a '
            'package-owned call form'
        )
    if len(local) != 1:
        return unsupported('both __new__ and __init__ contribute local call forms')
    if len(effective) != 1:
        return unsupported(
            'the class combines a local constructor phase with an inherited opposite phase'
        )
    constructor_name, source_function = local[0]

    forms = []
    rejected_form: griffe.Function | None = None
    for variant in source_function.overloads or (source_function,):
        if _is_never(variant) or _has_required_never_input(variant):
            continue
        form = _class_call_form(
            variant,
            class_obj,
            constructor_name,
            type_variables,
        )
        if form is None:
            rejected_form = variant
            break
        forms.append(form)
    if rejected_form is not None:
        return unsupported(
            f'{constructor_name} has a receiver or parameter form the class-call '
            'adapter does not support',
            rejected_form,
        )
    if not forms:
        return unavailable('all declared constructor overloads are non-constructible')

    documentation_parameters = _documentation_parameters(tuple(forms))

    binding = griffe.Function(
        '__call__',
        parameters=documentation_parameters,
        returns=_class_call_result(class_obj, type_variables),
        type_parameters=class_obj.type_parameters,
        parent=class_obj,
        lineno=source_function.lineno,
        endlineno=source_function.endlineno,
        runtime=source_function.runtime,
        lines_collection=class_obj.lines_collection,
        modules_collection=class_obj.modules_collection,
        git_info=class_obj.git_info,
        analysis=source_function.analysis,
    )
    binding.overloads = forms
    binding.extra[_CONSTRUCTOR_NAMESPACE] = {
        'source_function': source_function,
        'constructor_name': constructor_name,
    }
    return RenderableConstructor(
        'renderable',
        binding,
        _constructor_source(source_function),
    )


def _constructor_binding(
    class_obj: griffe.Class,
    package_path: str,
    inherited_members: dict[str, Any] | None = None,
    type_variables: set[str] | None = None,
) -> griffe.Function | None:
    """Compatibility accessor for internal callers that need the binding."""
    outcome = _constructor_outcome(
        class_obj,
        package_path,
        inherited_members,
        type_variables,
    )
    return outcome.binding if isinstance(outcome, RenderableConstructor) else None


def _promote_overloads(obj: Any, seen: set[int]) -> None:
    if id(obj) in seen or isinstance(obj, griffe.Alias):
        return
    seen.add(id(obj))
    stranded = getattr(obj, 'overloads', None)
    if isinstance(stranded, dict):
        for name, variants in list(stranded.items()):
            if not variants or name in obj.members:
                continue
            usable = [variant for variant in variants if not _is_never(variant)]
            if not usable:
                continue
            # Keep the final authored form as the model's prose carrier. The
            # reference template renders the complete overload list separately.
            carrier = usable[-1]
            carrier.overloads = [
                variant
                for variant in variants
                if variant is not carrier and not _is_never(variant)
            ]
            carrier.overloads.append(copy.copy(carrier))
            obj.set_member(name, carrier)
            del stranded[name]
    for member in list(getattr(obj, 'members', {}).values()):
        _promote_overloads(member, seen)


def _native_alias_target(member: Any) -> str | None:
    if isinstance(member, griffe.Alias):
        target_path = getattr(member, 'target_path', '')
        if target_path.startswith('gometry._lib.'):
            return target_path.rsplit('.', 1)[-1]
        return None
    if not isinstance(member, griffe.Attribute):
        return None
    parts = getattr(getattr(member, 'value', None), 'values', None)
    if (
        parts
        and len(parts) == 2
        and getattr(parts[0], 'name', None) == '_lib'
        and isinstance(getattr(parts[1], 'name', None), str)
    ):
        return parts[1].name
    return None


def _function_alias_target(
    pkg: griffe.Module,
    member: Any,
) -> griffe.Function | None:
    """Resolve a top-level function alias kept inside this package."""
    if not isinstance(member, griffe.Alias):
        return None
    target_path = getattr(member, 'target_path', '')
    package_prefix = f'{pkg.path}.'
    if not target_path.startswith(package_prefix):
        return None
    try:
        target = member.target
    except (griffe.AliasResolutionError, griffe.CyclicAliasError):
        return None
    parent = getattr(target, 'parent', None)
    if (
        not isinstance(target, griffe.Function)
        or not isinstance(parent, griffe.Module)
        or parent.path == pkg.path
        or not parent.path.startswith(package_prefix)
    ):
        return None
    return target


def _public_function_copy(
    function: griffe.Function,
    name: str,
    parent: griffe.Module,
) -> griffe.Function:
    return _copy_function_for_owner(function, parent, name=name)


def _materialize_function_aliases(pkg: griffe.Module) -> None:
    for alias_name, member in list(pkg.members.items()):
        target = _function_alias_target(pkg, member)
        if target is None:
            target_name = _native_alias_target(member)
            lib = pkg.members.get('_lib')
            target = (
                lib.members.get(target_name)
                if target_name is not None and isinstance(lib, griffe.Module)
                else None
            )
        if not isinstance(target, griffe.Function):
            continue
        pkg.set_member(alias_name, _public_function_copy(target, alias_name, pkg))


def _class_alias_target(
    pkg: griffe.Module,
    member: Any,
) -> griffe.Class | None:
    if isinstance(member, griffe.Alias):
        if not getattr(member, 'target_path', '').startswith(f'{pkg.path}.'):
            return None
        try:
            target = member.target
        except (griffe.AliasResolutionError, griffe.CyclicAliasError):
            return None
        return target if isinstance(target, griffe.Class) else None

    target_name = _native_alias_target(member)
    lib = pkg.members.get('_lib')
    if (
        target_name is None
        or not isinstance(lib, griffe.Module)
        or not isinstance((target := lib.members.get(target_name)), griffe.Class)
    ):
        return None
    return target


def _materialize_class_aliases(pkg: griffe.Module) -> None:
    """Materialize in-package class aliases from their declared source owners."""
    for alias_name, member in list(pkg.members.items()):
        if alias_name.startswith('_') or isinstance(member, griffe.Class):
            continue
        target = _class_alias_target(pkg, member)
        if target is not None:
            local = _copy_class_for_owner(target, alias_name, pkg)
            if isinstance(member, griffe.Alias):
                local.aliases[member.path] = member
            pkg.set_member(alias_name, local)


def _cache_ordered_call_forms(obj: Any, seen: set[int]) -> None:
    if id(obj) in seen or isinstance(obj, griffe.Alias):
        return
    seen.add(id(obj))
    if isinstance(obj, griffe.Function):
        extra = dict(obj.extra)
        namespace = dict(extra.get(_CONSTRUCTOR_NAMESPACE, {}))
        namespace['ordered_call_forms'] = ordered_call_forms(obj)
        extra[_CONSTRUCTOR_NAMESPACE] = namespace
        obj.extra = extra
    for member in getattr(obj, 'members', {}).values():
        _cache_ordered_call_forms(member, seen)


def _bind_public_class_constructors(pkg: griffe.Module) -> None:
    type_variables = _module_type_variables(pkg)
    exported = (
        set(pkg.exports)
        if pkg.exports is not None
        else {name for name in pkg.members if not name.startswith('_')}
    )
    for name, member in pkg.members.items():
        if name.startswith('_') or not isinstance(member, griffe.Class):
            continue
        member.extra = {
            namespace: dict(values) for namespace, values in member.extra.items()
        }
        outcome = _constructor_outcome(
            member,
            pkg.path,
            type_variables=type_variables,
        )
        namespace = member.extra.setdefault(_CONSTRUCTOR_NAMESPACE, {})
        namespace[_CONSTRUCTOR_KEY] = outcome
        namespace['ordered_call_forms'] = ordered_call_forms(outcome)
        if name in exported and isinstance(outcome, UnsupportedConstructor):
            raise UnsupportedConstructorError(_unsupported_constructor_error(outcome))


def _canonicalize_public_classes(pkg: griffe.Module) -> None:
    """Make public materialized classes canonical in the docs model.

    Private ``_lib``/``_types`` classes become aliases of the public
    materialization so annotation crossrefs (signatures and numpydoc type
    columns) resolve to ``gometry.X`` anchors instead of unresolved private
    paths.
    """
    for module_name in ('_lib', '_types'):
        module = pkg.members.get(module_name)
        if not isinstance(module, griffe.Module):
            continue
        for name, public in list(pkg.members.items()):
            private = module.members.get(name)
            if not isinstance(public, griffe.Class) or not isinstance(
                private, griffe.Class
            ):
                continue
            module.set_member(
                name,
                griffe.Alias(name, public, parent=module),
            )


class PromoteStubOverloads(griffe.Extension):
    """Make overload-only functions and their public aliases renderable."""

    def on_package(self, *, pkg: griffe.Module, **kwargs: Any) -> None:
        if pkg.name != 'gometry':
            return
        _promote_overloads(pkg, set())
        _materialize_class_aliases(pkg)
        _canonicalize_public_classes(pkg)
        _materialize_function_aliases(pkg)
        _cache_ordered_call_forms(pkg, set())
        _bind_public_class_constructors(pkg)


def expand_annotation(
    expr: Any,
    aliases: dict[str, Any],
    expanding: frozenset[str] = frozenset(),
) -> Any:
    """Apply the public annotation rewrites shared by Griffe and model checks."""
    if expr is None or isinstance(expr, str):
        return expr
    if isinstance(expr, griffe.ExprName):
        if expr.name in _PRIVATE_TYPEVAR_RENAMES:
            return griffe.ExprName(_PRIVATE_TYPEVAR_RENAMES[expr.name])
        if expr.name == '_CellArrayIterator':
            return griffe.ExprName('Iterator')
        if expr.name in {'_NumericCell', '_Cell'}:
            return griffe.ExprName('object')
        alias_path = getattr(expr, 'canonical_path', None)
        alias_name = (
            alias_path
            if alias_path in aliases
            else expr.name
            if expr.name in aliases
            else None
        )
        if alias_name is not None and alias_name not in expanding:
            return expand_annotation(
                _copy_expression(aliases[alias_name]),
                aliases,
                expanding | {alias_name},
            )
        return expr
    if isinstance(expr, griffe.ExprBinOp):
        expr.left = expand_annotation(expr.left, aliases, expanding)
        expr.right = expand_annotation(expr.right, aliases, expanding)
    elif isinstance(expr, griffe.ExprSubscript):
        if (
            isinstance(expr.left, griffe.ExprName)
            and expr.left.name == '_CellGeometryProjection'
        ):
            element = expand_annotation(expr.slice, aliases, expanding)
            return griffe.ExprSubscript(
                griffe.ExprName('GeometryArray'),
                griffe.ExprBinOp(element, '|', griffe.ExprName('None')),
            )
        expr.left = expand_annotation(expr.left, aliases, expanding)
        expr.slice = expand_annotation(expr.slice, aliases, expanding)
    elif isinstance(expr, griffe.ExprTuple):
        expr.elements = [
            expand_annotation(element, aliases, expanding) for element in expr.elements
        ]
    return expr


def _expand_base(expr: Any, aliases: dict[str, Any]) -> Any:
    if (
        isinstance(expr, griffe.ExprSubscript)
        and isinstance(expr.left, griffe.ExprName)
        and expr.left.name == '_CellArrayIterator'
    ):
        return griffe.ExprName('Iterator')
    return expand_annotation(expr, aliases)


def _walk_annotations(obj: Any, aliases: dict[str, Any], seen: set[int]) -> None:
    if id(obj) in seen or isinstance(obj, griffe.Alias):
        return
    seen.add(id(obj))
    if isinstance(obj, griffe.Function):
        for function in (obj, *(obj.overloads or ())):
            for parameter in function.parameters:
                parameter.annotation = expand_annotation(parameter.annotation, aliases)
            function.returns = expand_annotation(function.returns, aliases)
    elif isinstance(obj, griffe.Class):
        obj.bases = [_expand_base(base, aliases) for base in obj.bases]
    elif isinstance(obj, griffe.Attribute):
        is_cell_projection = (
            isinstance(obj.annotation, griffe.ExprSubscript)
            and isinstance(obj.annotation.left, griffe.ExprName)
            and obj.annotation.left.name == '_CellGeometryProjection'
        )
        obj.annotation = expand_annotation(obj.annotation, aliases)
        if is_cell_projection:
            obj.labels.discard('instance-attribute')
            obj.labels.update({'property', 'readonly'})
    for member in getattr(obj, 'members', {}).values():
        _walk_annotations(member, aliases, seen)


def _public_members(pkg: griffe.Module) -> tuple[set[str], dict[str, set[str]]]:
    top = {name for name in pkg.members if not name.startswith('_')}
    classes: dict[str, set[str]] = {}
    for name, member in pkg.members.items():
        if isinstance(member, griffe.Class):
            classes[name] = set(member.members)
    return top, classes


def _replace_see_also(
    match: re.Match[str],
    *,
    owner: str | None,
    top: set[str],
    classes: dict[str, set[str]],
) -> str:
    shown = match.group('name')
    target = shown.removeprefix('gm.')
    if '.' in target:
        head, _, tail = target.partition('.')
        path = f'gometry.{head}.{tail}' if tail in classes.get(head, ()) else None
    elif owner and target in classes.get(owner, ()):
        path = f'gometry.{owner}.{target}'
    else:
        path = f'gometry.{target}' if target in top else None
    if path is None:
        return match.group(0)
    ticks = match.group('ticks')
    return f'[{ticks}{shown}{ticks}][{path}]{match.group("sep")}'


def _link_see_also(
    obj: Any,
    top: set[str],
    classes: dict[str, set[str]],
    seen: set[int],
) -> None:
    if id(obj) in seen or isinstance(obj, griffe.Alias):
        return
    seen.add(id(obj))
    parent = getattr(obj, 'parent', None)
    owner_class = parent if isinstance(parent, griffe.Class) else None
    docstrings = [getattr(obj, 'docstring', None)]
    binding = constructor_binding(obj) if isinstance(obj, griffe.Class) else None
    if binding is not None:
        docstrings.append(binding.docstring)
        docstrings.append(binding.extra[_CONSTRUCTOR_NAMESPACE].get('prose_docstring'))

    for docstring in docstrings:
        if docstring is None:
            continue
        doc_parent = getattr(docstring, 'parent', None)
        if isinstance(doc_parent, griffe.Function):
            doc_parent = doc_parent.parent
        owner = (
            doc_parent.name
            if isinstance(doc_parent, griffe.Class)
            else (owner_class.name if owner_class is not None else None)
        )

        for section in getattr(docstring, 'parsed', ()):
            if section.kind is not griffe.DocstringSectionKind.admonition:
                continue
            admonition = section.value
            if getattr(admonition, 'kind', None) != 'see-also':
                continue
            contents = getattr(admonition, 'contents', None)
            if isinstance(contents, str):
                replace = partial(
                    _replace_see_also,
                    owner=owner,
                    top=top,
                    classes=classes,
                )
                admonition.contents = _SEE_ALSO_ENTRY.sub(replace, contents)
    for member in getattr(obj, 'members', {}).values():
        _link_see_also(member, top, classes, seen)


def _merge_parameter_section(kind: Any, sections: list[Any]) -> Any:
    parameters: dict[str, griffe.DocstringParameter] = {}
    ordered = []
    title = next((section.title for section in sections if section.title), None)
    for section in sections:
        for parameter in section.value:
            current = parameters.get(parameter.name)
            if current is None:
                current = copy.copy(parameter)
                parameters[parameter.name] = current
                ordered.append(current)
                continue
            if (
                parameter.description
                and parameter.description != current.description
                and parameter.description not in current.description
            ):
                current.description = '\n\n'.join(
                    part
                    for part in (current.description, parameter.description)
                    if part
                )
            if current.annotation is None:
                current.annotation = parameter.annotation
            if current.value is None:
                current.value = parameter.value

    section_type = (
        griffe.DocstringSectionOtherParameters
        if kind is griffe.DocstringSectionKind.other_parameters
        else griffe.DocstringSectionParameters
    )
    return section_type(ordered, title=title)


def _merge_parameter_sections(
    class_sections: list[Any],
    constructor_sections: list[Any],
) -> list[Any]:
    kinds = (
        griffe.DocstringSectionKind.parameters,
        griffe.DocstringSectionKind.other_parameters,
    )
    result = list(class_sections)

    for kind in kinds:
        positions = [
            index for index, section in enumerate(result) if section.kind is kind
        ]
        if len(positions) < 2:
            continue
        first = positions[0]
        sections = [result[index] for index in positions]
        result = [section for section in result if section.kind is not kind]
        result.insert(first, _merge_parameter_section(kind, sections))

    for section in constructor_sections:
        if section.kind not in kinds:
            result.append(section)
            continue
        position = next(
            (index for index, item in enumerate(result) if item.kind is section.kind),
            None,
        )
        if position is None:
            result.append(section)
            continue
        result[position] = _merge_parameter_section(
            section.kind,
            [result[position], section],
        )
    return result


def _resolved_parameter_sections(
    sections: list[Any],
    binding: griffe.Function,
    resolution: Any,
) -> list[Any]:
    kinds = {
        griffe.DocstringSectionKind.parameters,
        griffe.DocstringSectionKind.other_parameters,
    }
    positions = [
        index for index, section in enumerate(sections) if section.kind in kinds
    ]
    if not resolution.rows:
        return sections

    insertion = min(positions) if positions else len(sections)
    title = next(
        (
            section.title
            for section in sections
            if section.kind in kinds and section.title
        ),
        None,
    )
    by_name = {
        parameter.name: parameter
        for section in sections
        if section.kind in kinds
        for parameter in section.value
    }
    parameters = []
    for row in resolution.rows:
        parameter = copy.copy(
            by_name.get(row.name)
            or griffe.DocstringParameter(row.name, description=''),
        )
        binding_parameter = next(
            (
                parameter
                for parameter in binding.parameters
                if parameter.name == row.name
            ),
            None,
        )
        parameter.description = row.description
        parameter.annotation = (
            _copy_expression(binding_parameter.annotation)
            if binding_parameter is not None
            and binding_parameter.annotation is not None
            else row.annotation
        )
        if row.status == 'defaulted':
            parameter.value = (
                _copy_expression(binding_parameter.default)
                if binding_parameter is not None
                else row.default
            )
        elif row.status == 'varies':
            parameter.value = 'varies by call form'
        elif row.status == 'variadic':
            parameter.value = 'not applicable'
        else:
            parameter.value = None
        parameters.append(parameter)

    sections = [section for section in sections if section.kind not in kinds]
    sections.insert(
        insertion,
        griffe.DocstringSectionParameters(
            parameters,
            title=title,
        ),
    )
    return sections


def _prepare_constructor_docstrings(
    class_obj: griffe.Class,
    binding: griffe.Function,
) -> None:
    from pyo3stubs.doc_structure import (
        DocLocation,
        SignatureParameter,
        resolve_parameter_rows,
    )

    extra = binding.extra[_CONSTRUCTOR_NAMESPACE]
    source_function = extra.get('source_function')
    class_docstring = class_obj.docstring
    class_source = _class_source(class_obj)
    carriers = []

    if class_docstring is not None:
        binding.docstring = griffe.Docstring(
            class_docstring.value,
            lineno=class_docstring.lineno,
            endlineno=class_docstring.endlineno,
            parent=binding,
            parser=class_docstring.parser,
            parser_options=class_docstring.parser_options,
        )
        class_sections = binding.docstring.parsed
        carriers.append((
            class_docstring.value,
            DocLocation(class_source.path, class_docstring.lineno or class_source.line),
        ))
    else:
        class_sections = []

    constructor_sections = []
    if isinstance(source_function, griffe.Function) and source_function.docstring:
        source_docstring = source_function.docstring
        source_location = _constructor_source(source_function)
        source_context = griffe.Function(
            source_function.name,
            parameters=griffe.Parameters(
                *(
                    griffe.Parameter(
                        parameter.name,
                        annotation=_copy_expression(parameter.annotation),
                        kind=parameter.kind,
                        default=_copy_expression(parameter.default),
                    )
                    for parameter in binding.parameters
                )
            ),
            returns=_copy_expression(source_function.returns),
            type_parameters=source_function.type_parameters,
            parent=source_function.parent,
            lineno=source_function.lineno,
            endlineno=source_function.endlineno,
            runtime=source_function.runtime,
            lines_collection=source_function.lines_collection,
            modules_collection=source_function.modules_collection,
            git_info=source_function.git_info,
            analysis=source_function.analysis,
        )
        extra['prose_docstring'] = griffe.Docstring(
            source_docstring.value,
            lineno=source_docstring.lineno,
            endlineno=source_docstring.endlineno,
            parent=source_context,
            parser=source_docstring.parser,
            parser_options=source_docstring.parser_options,
        )
        constructor_sections = extra['prose_docstring'].parsed
        carriers.append((
            source_docstring.value,
            DocLocation(
                source_location.path,
                source_docstring.lineno or source_location.line,
            ),
        ))

    forms = tuple(
        tuple(
            SignatureParameter(
                parameter.name,
                parameter.kind.value if parameter.kind is not None else '',
                str(parameter.annotation) if parameter.annotation is not None else None,
                str(parameter.default) if parameter.default is not None else None,
            )
            for parameter in form.parameters
        )
        for form in ordered_call_forms(binding)
    )
    resolution = resolve_parameter_rows(
        class_obj.path,
        carriers,
        forms,
        require_documentation=isinstance(source_function, griffe.Function),
    )
    extra['parameter_resolution'] = resolution
    sections = _merge_parameter_sections(class_sections, constructor_sections)
    has_authored_parameters = any(
        section.kind
        in {
            griffe.DocstringSectionKind.parameters,
            griffe.DocstringSectionKind.other_parameters,
        }
        for section in sections
    )
    if has_authored_parameters:
        sections = _resolved_parameter_sections(sections, binding, resolution)
    extra['docstring_sections'] = sections


def _expand_docstring_sections(sections: list[Any], aliases: dict[str, Any]) -> None:
    for section in sections:
        values = section.value if isinstance(section.value, list) else [section.value]
        for value in values:
            if not hasattr(value, 'annotation'):
                continue
            value.annotation = expand_annotation(value.annotation, aliases)
            if hasattr(value, 'value'):
                value.value = expand_annotation(value.value, aliases)


def annotation_aliases(pkg: griffe.Module) -> dict[str, Any]:
    """Return reachable source expansions keyed by declaration identity."""
    package_file = getattr(pkg, 'filepath', None)
    package_root = Path(package_file).parent if package_file is not None else None
    if package_root is None or not (package_root / '_lib.pyi').is_file():
        return {}

    import gometry
    from reference_types import build_public_type_graph

    graph = build_public_type_graph(package_root, gometry)
    aliases: dict[str, Any] = {}
    names: dict[str, list[Any]] = {}
    for path, source in graph.expansions.items():
        module: Any = pkg
        for part in source.module.name.split('.')[1:]:
            module = getattr(module, 'members', {}).get(part)
            if not isinstance(module, griffe.Module):
                break
        if not isinstance(module, griffe.Module):
            continue
        expression = griffe.get_expression(
            source.expression,
            module,
            parse_strings=True,
        )
        if expression is None:
            continue
        aliases[path] = expression
        names.setdefault(path.rsplit('.', 1)[-1], []).append(expression)

    aliases.update({
        name: variants[0]
        for name, variants in names.items()
        if len({str(variant) for variant in variants}) == 1
    })
    return aliases


class ExpandTokenAliases(griffe.Extension):
    """Expand private Literal/input aliases in rendered annotations only."""

    def on_package(self, *, pkg: griffe.Module, **kwargs: Any) -> None:
        if pkg.name != 'gometry':
            return
        aliases = annotation_aliases(pkg)
        seen: set[int] = set()
        lib = pkg.members.get('_lib')
        if isinstance(lib, griffe.Module):
            _walk_annotations(lib, aliases, seen)
        _walk_annotations(pkg, aliases, seen)
        for member in pkg.members.values():
            if not isinstance(member, griffe.Class):
                continue
            binding = constructor_binding(member)
            if binding is None:
                continue
            _walk_annotations(binding, aliases, seen)
            _prepare_constructor_docstrings(member, binding)
            _expand_docstring_sections(
                binding.extra[_CONSTRUCTOR_NAMESPACE]['docstring_sections'],
                aliases,
            )
        top, classes = _public_members(pkg)
        _link_see_also(pkg, top, classes, set())
