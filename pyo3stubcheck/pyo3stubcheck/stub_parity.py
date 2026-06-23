"""Stub/runtime signature parity gate."""

from __future__ import annotations

import ast
import inspect
import sys
from typing import TYPE_CHECKING

from pyo3stubcheck.context import CheckContext

if TYPE_CHECKING:
    from pyo3stubcheck.config import StubCheckConfig

# Runtime-only dunders PyO3/CPython add that a stub never declares.
DEFAULT_IGNORED_RUNTIME_NAMES = frozenset({
    '__abstractmethods__',
    '__annotations__',
    '__class__',
    '__class_getitem__',
    '__delattr__',
    '__dict__',
    '__dictoffset__',
    '__dir__',
    '__doc__',
    '__entries',
    '__firstlineno__',
    '__ge__',
    '__getattribute__',
    '__getstate__',
    '__gt__',
    '__init_subclass__',
    '__le__',
    '__loader__',
    '__lt__',
    '__module__',
    '__name__',
    '__ne__',
    '__new__',
    '__package__',
    '__qualname__',
    '__rand__',
    '__reduce__',
    '__repr__',
    '__str__',
    '__reduce_ex__',
    '__release_buffer__',
    '__ror__',
    '__rsub__',
    '__rxor__',
    '__setattr__',
    '__spec__',
    '__static_attributes__',
    '__subclasshook__',
    '__type_params__',
    '__weakref__',
    '__weaklistoffset__',
})


def _decorator_names(node: ast.FunctionDef) -> set[str]:
    names = set()
    for dec in node.decorator_list:
        target = dec.func if isinstance(dec, ast.Call) else dec
        if isinstance(target, ast.Attribute):
            names.add(target.attr)
        elif isinstance(target, ast.Name):
            names.add(target.id)
    return names


class StubFunction:
    """The canonical (non-overload) def for one stub symbol."""

    def __init__(self, node: ast.FunctionDef) -> None:
        self.node = node
        self.overloads: list[StubFunction] = []
        decorators = _decorator_names(node)
        self.is_property = 'property' in decorators or 'setter' in decorators
        self.is_classmethod = 'classmethod' in decorators
        self.is_staticmethod = 'staticmethod' in decorators

    def parameters(self) -> list[tuple[str, str, str | None]]:
        """``(name, kind, default-source)`` triples mirroring inspect kinds."""
        args = self.node.args
        params: list[tuple[str, str, str | None]] = []
        positional = [*args.posonlyargs, *args.args]
        defaults: list[ast.expr | None] = [None] * (
            len(positional) - len(args.defaults)
        ) + list(args.defaults)
        for arg, default in zip(positional, defaults, strict=True):
            kind = (
                'POSITIONAL_ONLY'
                if arg in args.posonlyargs
                else 'POSITIONAL_OR_KEYWORD'
            )
            params.append((
                arg.arg,
                kind,
                None if default is None else ast.unparse(default),
            ))
        if args.vararg:
            params.append((args.vararg.arg, 'VAR_POSITIONAL', None))
        for arg, default in zip(args.kwonlyargs, args.kw_defaults, strict=True):
            params.append((
                arg.arg,
                'KEYWORD_ONLY',
                None if default is None else ast.unparse(default),
            ))
        if args.kwarg:
            params.append((args.kwarg.arg, 'VAR_KEYWORD', None))
        return params


def _collect_defs(body: list[ast.stmt]) -> dict[str, StubFunction]:
    """Per-name canonical def: the bare implementation when overloaded."""
    defs: dict[str, StubFunction] = {}
    for stmt in body:
        if not isinstance(stmt, ast.FunctionDef):
            continue
        if 'overload' in _decorator_names(stmt):
            existing = defs.setdefault(stmt.name, StubFunction(stmt))
            existing.overloads.append(StubFunction(stmt))
            continue
        fn = StubFunction(stmt)
        prior = defs.get(stmt.name)
        if prior is not None:
            if 'overload' in _decorator_names(prior.node):
                fn.overloads = prior.overloads
            elif fn.is_property:
                continue
        defs[stmt.name] = fn
    return defs


def _check_overloads(qualname: str, stub: StubFunction, errors: list[str]) -> None:
    if stub.overloads and 'overload' in _decorator_names(stub.node):
        errors.append(
            f'{qualname}: overloaded symbol has no bare implementation def '
            '(required: it carries the docstring and the canonical signature)'
        )
        return
    canonical = {name: (kind, default) for name, kind, default in stub.parameters()}
    order = [name for name, _, _ in stub.parameters()]
    for variant in stub.overloads:
        last_index = -1
        for name, kind, default in variant.parameters():
            if name not in canonical:
                errors.append(
                    f'{qualname}: overload parameter {name!r} not on the canonical def'
                )
                continue
            if kind not in ('KEYWORD_ONLY', 'VAR_KEYWORD'):
                index = order.index(name)
                if index <= last_index:
                    errors.append(
                        f'{qualname}: overload parameter {name!r} out of order'
                    )
                last_index = index
            base_kind, base_default = canonical[name]
            if kind != base_kind:
                errors.append(
                    f'{qualname}.{name}: overload kind {kind} != canonical {base_kind}'
                )
            if (
                default is not None
                and base_default is not None
                and not _defaults_equal(default, base_default)
            ):
                errors.append(
                    f'{qualname}.{name}: overload default {default} '
                    f'!= canonical {base_default}'
                )


def _runtime_signature(obj: object) -> list[tuple[str, str, str | None]] | None:
    try:
        signature = inspect.signature(obj)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return [
        (
            param.name,
            param.kind.name,
            None if param.default is inspect.Parameter.empty else repr(param.default),
        )
        for param in signature.parameters.values()
    ]


def _defaults_equal(stub: str | None, runtime: str | None) -> bool:
    if runtime == 'Ellipsis':
        runtime = '...'
    if stub == runtime:
        return True
    if stub is None or runtime is None:
        return False
    try:
        return ast.literal_eval(stub) == ast.literal_eval(runtime)
    except (SyntaxError, ValueError):
        return False


def _compare_signature(
    qualname: str,
    stub: StubFunction,
    runtime: object,
    *,
    drop_first: bool,
    errors: list[str],
) -> None:
    runtime_params = _runtime_signature(runtime)
    if runtime_params is None:
        return
    stub_params = stub.parameters()
    if drop_first and stub_params:
        stub_params = stub_params[1:]
    if len(stub_params) != len(runtime_params):
        errors.append(
            f'{qualname}: parameter count differs — '
            f'stub {[p[0] for p in stub_params]} != '
            f'runtime {[p[0] for p in runtime_params]}'
        )
        return
    for index, ((s_name, s_kind, s_default), (r_name, r_kind, r_default)) in enumerate(
        zip(stub_params, runtime_params, strict=True)
    ):
        if index == 0 and s_name == r_name == 'self':
            continue
        dunder = qualname.rsplit('.', 1)[-1].startswith('__')
        if s_name != r_name and not (dunder and r_kind == 'POSITIONAL_ONLY'):
            errors.append(f'{qualname}: parameter {r_name!r} named {s_name!r} in stub')
        if s_kind != r_kind and not (
            dunder and r_kind == 'POSITIONAL_ONLY' and s_kind == 'POSITIONAL_OR_KEYWORD'
        ):
            errors.append(f'{qualname}.{r_name}: kind {s_kind} != runtime {r_kind}')
        if not _defaults_equal(s_default, r_default):
            errors.append(
                f'{qualname}.{r_name}: default {s_default} != runtime {r_default}'
            )


def _is_property_like(cls: type, name: str) -> bool:
    static = inspect.getattr_static(cls, name)
    return isinstance(static, property) or type(static).__name__ in {
        'getset_descriptor',
        'member_descriptor',
    }


def _check_class(
    cls_name: str,
    node: ast.ClassDef,
    cls: type,
    *,
    ignored_names: frozenset[str],
    errors: list[str],
) -> None:
    stub_defs = _collect_defs(node.body)
    stub_attrs = {
        stmt.target.id
        for stmt in node.body
        if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name)
    }
    runtime_names = set(cls.__dict__) - ignored_names
    errors.extend(
        f'{cls_name}.{name}: missing from stub'
        for name in sorted(runtime_names - set(stub_defs) - stub_attrs)
    )
    errors.extend(
        f'{cls_name}.{name}: in stub but not at runtime'
        for name in sorted((set(stub_defs) | stub_attrs) - set(dir(cls)))
    )
    for name, stub_fn in sorted(stub_defs.items()):
        if not hasattr(cls, name):
            continue
        qualname = f'{cls_name}.{name}'
        _check_overloads(qualname, stub_fn, errors)
        if stub_fn.is_property:
            if not _is_property_like(cls, name):
                errors.append(f'{qualname}: property in stub, not at runtime')
            continue
        if _is_property_like(cls, name):
            errors.append(f'{qualname}: method in stub, property at runtime')
            continue
        runtime = getattr(cls, name)
        if name == '__init__':
            _compare_signature(qualname, stub_fn, cls, drop_first=True, errors=errors)
        elif stub_fn.is_classmethod:
            _compare_signature(
                qualname, stub_fn, runtime, drop_first=True, errors=errors
            )
        elif stub_fn.is_staticmethod:
            _compare_signature(
                qualname, stub_fn, runtime, drop_first=False, errors=errors
            )
        else:
            _compare_signature(
                qualname, stub_fn, runtime, drop_first=False, errors=errors
            )


def _check_class_bases(
    cls_name: str,
    node: ast.ClassDef,
    cls: type,
    stub_class_names: set[str],
    runtime_module: object,
    errors: list[str],
) -> None:
    stub_bases = {
        base.id for base in node.bases if isinstance(base, ast.Name)
    } & stub_class_names
    for base in sorted(stub_bases):
        runtime_base = getattr(runtime_module, base, None)
        if not (
            isinstance(runtime_base, type)
            and cls is not runtime_base
            and issubclass(cls, runtime_base)
        ):
            errors.append(f'{cls_name}: stub base {base!r} is not a runtime base')
    runtime_bases = {base.__name__ for base in cls.__bases__} & stub_class_names
    errors.extend(
        f'{cls_name}: runtime base {base!r} missing from the stub bases'
        for base in sorted(runtime_bases - stub_bases)
    )


def collect_errors(cfg: StubCheckConfig) -> list[str]:
    """Compare the stub against the compiled module; run plugins first."""
    ctx = CheckContext(cfg)
    runtime = ctx.runtime_module
    module = ctx.stub_ast
    ignored = (
        cfg.ignored_runtime_names
        if cfg.ignored_runtime_names
        else DEFAULT_IGNORED_RUNTIME_NAMES
    )
    errors: list[str] = []
    for plugin in cfg.plugins:
        errors.extend(plugin.collect(cfg, ctx))

    stub_functions = _collect_defs(module.body)
    stub_classes = {
        stmt.name: stmt for stmt in module.body if isinstance(stmt, ast.ClassDef)
    }

    runtime_public = {name for name in dir(runtime) if not name.startswith('_')}
    stub_public = set(stub_functions) | set(stub_classes)

    errors.extend(
        f'{name}: missing from stub' for name in sorted(runtime_public - stub_public)
    )
    errors.extend(
        f'{name}: in stub but not at runtime'
        for name in sorted(stub_public - set(dir(runtime)))
        if not name.startswith('_')
    )

    for name, stub_fn in sorted(stub_functions.items()):
        _check_overloads(name, stub_fn, errors)
        if hasattr(runtime, name):
            _compare_signature(
                name, stub_fn, getattr(runtime, name), drop_first=False, errors=errors
            )

    for name, node in sorted(stub_classes.items()):
        cls = getattr(runtime, name, None)
        if isinstance(cls, type):
            _check_class(
                name, node, cls, ignored_names=ignored, errors=errors
            )
            _check_class_bases(
                name, node, cls, set(stub_classes), runtime, errors
            )

    return errors


def main(argv: list[str] | None = None) -> int:
    """CLI entry point — requires a project shim to build :class:`StubCheckConfig`."""
    _ = argv
    print(
        'pyo3stubcheck.stub_parity: supply a project shim that builds StubCheckConfig',
        file=sys.stderr,
    )
    return 2