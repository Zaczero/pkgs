"""Shared AST helpers used by multiple gates (one definition each)."""

from __future__ import annotations

import ast
import inspect
import types
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator


def doc_of(obj: object) -> str | None:
    """Non-empty ``__doc__`` stripped of leading/trailing newlines, or ``None``."""
    if obj is None:
        return None
    doc = getattr(obj, '__doc__', None)
    if not isinstance(doc, str) or not doc.strip():
        return None
    return doc.strip('\n')


# Most PyO3 protocol slots are `WrapperDescriptorType` and are excluded by kind.
# CPython exposes `__new__` and generic `__class_getitem__` as ordinary built-in
# functions instead, so identify their synthesized wording from this interpreter.
_BUILTIN_DOCS = frozenset(
    doc
    for owner in (object, type, dict, list, tuple, str, int)
    for name in dir(owner)
    if (doc := doc_of(getattr(owner, name, None))) is not None
)


def is_synthetic_doc(name: str, doc: str | None) -> bool:
    return doc is not None and name.startswith('__') and doc in _BUILTIN_DOCS


def runtime_doc_for_stub(name: str, obj: object) -> str | None:
    """Runtime prose suitable for a stub carrier, or None when it is synthetic.

    PyO3 installs wrapper descriptors for protocol slots, and CPython supplies
    boilerplate text for ``__new__`` and generic ``__class_getitem__``. When
    runtime docs are synthetic, keep the authored stub carrier instead.
    Static-method descriptors are unwrapped so their own builtin docs are not
    mistaken for the wrapped callable's prose.
    """
    if isinstance(obj, staticmethod):
        obj = obj.__func__
    if isinstance(obj, types.WrapperDescriptorType):
        return None
    doc = doc_of(obj)
    if is_synthetic_doc(name, doc):
        return None
    return doc


def passive_class_member(cls: type, name: str) -> object | None:
    """Select a class member without invoking an arbitrary descriptor.

    Plain functions are already unbound declarations. Standard staticmethod
    and classmethod wrappers are unwrapped to their functions; known native
    callable and data descriptors and properties remain raw. User-defined descriptors
    are not bound or inspected as callables.
    """
    member = inspect.getattr_static(cls, name, None)
    if isinstance(member, (staticmethod, classmethod)):
        # Runtime descriptors do not carry their callable's static signature.
        member = member.__func__  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]
    if isinstance(
        member,
        (
            types.FunctionType,
            types.BuiltinFunctionType,
            types.MethodDescriptorType,
            types.WrapperDescriptorType,
            types.ClassMethodDescriptorType,
            types.GetSetDescriptorType,
            types.MemberDescriptorType,
            property,
        ),
    ):
        return member
    return None


def decorator_names(
    node: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef,
) -> set[str]:
    """Bare names of a def's decorators (`@x.setter` contributes ``setter``)."""
    names: set[str] = set()
    for dec in node.decorator_list:
        target = dec.func if isinstance(dec, ast.Call) else dec
        if isinstance(target, ast.Attribute):
            names.add(target.attr)
        elif isinstance(target, ast.Name):
            names.add(target.id)
    return names


def function_groups(body: list[ast.stmt]) -> dict[str, list[ast.FunctionDef]]:
    """Group top-level function defs in a body by name (preserves order)."""
    groups: dict[str, list[ast.FunctionDef]] = {}
    for stmt in body:
        if isinstance(stmt, ast.FunctionDef):
            groups.setdefault(stmt.name, []).append(stmt)
    return groups


def class_prohibits_construction(cls: ast.ClassDef) -> bool:
    """Whether every declared constructor signature explicitly forbids calls.

    A required ``Never``/``NoReturn`` input or a ``Never``/``NoReturn`` result
    makes that constructor impossible to call. A viable overload keeps the
    class constructible, and ordinary annotations or a private-looking name do
    not imply a prohibition.
    """

    def never_type(annotation: ast.expr | None) -> bool:
        return (
            isinstance(annotation, ast.Name) and annotation.id in {'Never', 'NoReturn'}
        ) or (
            isinstance(annotation, ast.Attribute)
            and annotation.attr in {'Never', 'NoReturn'}
        )

    def forbids_call(node: ast.FunctionDef) -> bool:
        if never_type(node.returns):
            return True
        positional = [*node.args.posonlyargs, *node.args.args]
        defaults: list[ast.expr | None] = [None] * (
            len(positional) - len(node.args.defaults)
        ) + list(node.args.defaults)
        required_never = any(
            argument.arg not in {'self', 'cls'}
            and default is None
            and never_type(argument.annotation)
            for argument, default in zip(positional, defaults, strict=True)
        )
        required_never |= any(
            argument.arg not in {'self', 'cls'}
            and default is None
            and never_type(argument.annotation)
            for argument, default in zip(
                node.args.kwonlyargs, node.args.kw_defaults, strict=True
            )
        )
        return required_never

    for name in ('__new__', '__init__'):
        constructors = [
            node
            for node in cls.body
            if isinstance(node, ast.FunctionDef) and node.name == name
        ]
        if constructors and all(forbids_call(node) for node in constructors):
            return True
    return False


def annotated_data_descriptors(
    cls: type,
    body: list[ast.stmt],
) -> Iterator[tuple[str, ast.AnnAssign, str | None, object]]:
    """Enumerate locally declared annotated data descriptors and their prose.

    A bare string expression immediately following a class annotation is that
    attribute's docstring. Only a data descriptor in the class's own namespace
    qualifies; reading it through the class or an instance would invoke it.
    """
    namespace = vars(cls)
    for index, stmt in enumerate(body):
        if not (isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name)):
            continue
        name = stmt.target.id
        descriptor = namespace.get(name)
        if not inspect.isdatadescriptor(descriptor):
            continue
        docstring = None
        if index + 1 < len(body):
            following = body[index + 1]
            if (
                isinstance(following, ast.Expr)
                and isinstance(following.value, ast.Constant)
                and isinstance(following.value.value, str)
            ):
                docstring = following.value.value
        yield name, stmt, docstring, descriptor
