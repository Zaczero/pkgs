"""Structural stub checks mypy cannot express.

Four gates over the stub AST + live runtime:

* **overload hygiene** — in a same-name multi-def group every def must carry
  ``@overload``, and the variants must agree on parameter names, kinds, order
  and defaults (``gen-docs-sync`` owns where the shared docstring sits).
  An undecorated def in a group is the silent dead-overload class: pyright
  applies last-def-wins without a diagnostic, so the narrowing simply vanishes
  (this exact corruption shipped once — 22 dead defs from a stray codemod).
* **finality** — a runtime class that cannot be subclassed (any PyO3 class
  without ``#[pyclass(subclass)]``) must be ``@final`` in the stub, and vice
  versa, so user subclassing fails statically instead of at import time.
* **signature coverage** — a public runtime callable or constructible class
  binding where ``inspect.signature`` fails is invisible to every signature
  gate (stubtest skips it silently); require an explicit allowlist entry per
  blind spot.
* **``__match_args__`` parity** — the stub's literal tuple must equal the
  runtime value (match statements silently misbind otherwise).
* **exports** — every ``__all__`` entry names something the stub defines,
  once. stubtest compares ``__all__`` with the runtime's, so a name removed
  from both but left in the stub's list survives it; star imports and IDEs
  then offer a symbol that does not exist.

A shadowed overload (one an earlier variant fully covers) is the ``validity``
gate's, through mypy's ``overload-cannot-match``.

Scalar↔array return duality is the separate ``duality`` gate
(:mod:`pyo3stubs.duality`), activated by ``StubConfig.duality``.
"""

from __future__ import annotations

import ast
import inspect
from typing import TYPE_CHECKING

from pyo3stubs.ast_util import (
    class_prohibits_construction,
    decorator_names,
    function_groups,
    passive_class_member,
)
from pyo3stubs.context import CheckContext
from pyo3stubs.report import Findings, loc, unused_allowlist_errors

if TYPE_CHECKING:
    from pyo3stubs.config import StubConfig


def _parameters(node: ast.FunctionDef) -> list[tuple[str, str, str | None]]:
    """``(name, kind, default-source)`` triples mirroring inspect kinds."""
    args = node.args
    params: list[tuple[str, str, str | None]] = []
    positional = [*args.posonlyargs, *args.args]
    defaults: list[ast.expr | None] = [None] * (
        len(positional) - len(args.defaults)
    ) + list(args.defaults)
    for arg, default in zip(positional, defaults, strict=True):
        kind = 'POSITIONAL_ONLY' if arg in args.posonlyargs else 'POSITIONAL_OR_KEYWORD'
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


def _check_overload_group(
    qualname: str,
    defs: list[ast.FunctionDef],
    path: str,
    errors: list[str],
    *,
    bound_receiver: bool = False,
) -> None:
    decorators = [decorator_names(node) for node in defs]
    if any('property' in names or 'setter' in names for names in decorators):
        return  # property/setter pairs share a name by design
    if len(defs) == 1:
        if 'overload' in decorators[0]:
            errors.append(
                f'{loc(path, defs[0])}: {qualname}: single @overload def — '
                'an overload set needs at least two variants'
            )
        return
    undecorated = [
        node
        for node, names in zip(defs, decorators, strict=True)
        if 'overload' not in names
    ]
    if undecorated:
        lines = ', '.join(str(node.lineno) for node in undecorated)
        errors.append(
            f'{loc(path, defs[0])}: {qualname}: duplicate defs without '
            f'@overload at line(s) {lines} — pyright silently drops every def '
            'but the last (dead overloads); decorate all variants'
        )
        return
    parameters = [_parameters(node) for node in defs]
    if bound_receiver:
        for index, names in enumerate(decorators):
            if 'staticmethod' not in names:
                # A bound receiver is implicit at instance-call sites. A slash
                # before an operand also makes `self` or `cls` positional-only,
                # which must not create a false overload mismatch.
                parameters[index] = parameters[index][1:]

    # The last real variant is the docstring carrier. It is canonical only for
    # parameter names, kinds, ordering, and defaults; its annotations stay
    # narrow, because a synthetic union overload weakens type narrowing.
    canonical = {name: (kind, default) for name, kind, default in parameters[-1]}
    order = [name for name, _, _ in parameters[-1]]
    for variant, variant_parameters in zip(defs[:-1], parameters[:-1], strict=True):
        last_index = -1
        for name, kind, default in variant_parameters:
            if name not in canonical:
                # Selector-dependent overloads intentionally omit mutually
                # exclusive parameters (for example, a literal mode that
                # makes a keyword invalid).  Requiring a synthetic union
                # fallback would silently re-admit that invalid call shape.
                continue
            if kind not in ('KEYWORD_ONLY', 'VAR_KEYWORD'):
                index = order.index(name)
                if index <= last_index:
                    errors.append(
                        f'{loc(path, variant)}: {qualname}: overload '
                        f'parameter {name!r} out of order'
                    )
                last_index = index
            base_kind, base_default = canonical[name]
            if kind != base_kind:
                errors.append(
                    f'{loc(path, variant)}: {qualname}.{name}: overload kind '
                    f'{kind} != canonical {base_kind}'
                )
            if (
                default is not None
                and base_default is not None
                and default != base_default
            ):
                errors.append(
                    f'{loc(path, variant)}: {qualname}.{name}: overload '
                    f'default {default} != canonical {base_default}'
                )


def _collect_overload_hygiene(ctx: CheckContext, errors: list[str]) -> None:
    path = str(ctx.cfg.stub_path)
    for name, defs in function_groups(ctx.stub_ast.body).items():
        _check_overload_group(name, defs, path, errors)
    for class_name, node in ctx.stub_classes.items():
        for name, defs in function_groups(node.body).items():
            _check_overload_group(
                f'{class_name}.{name}',
                defs,
                path,
                errors,
                bound_receiver=True,
            )
    _check_duplicate_decorators(ctx, path, errors)


def _check_duplicate_decorators(
    ctx: CheckContext, path: str, errors: list[str]
) -> None:
    """A def carrying the same decorator twice is codemod residue (60 stacked
    ``@overload`` lines once shipped silently — Python tolerates the
    duplicates, so only this lint sees them).
    """
    for node in ast.walk(ctx.stub_ast):
        if not isinstance(node, ast.FunctionDef):
            continue
        names = [dec.id for dec in node.decorator_list if isinstance(dec, ast.Name)]
        errors.extend(
            f'{loc(path, node)}: {node.name}: decorator `@{name}` '
            f'appears {names.count(name)} times (codemod residue)'
            for name in {n for n in names if names.count(n) > 1}
        )


def _runtime_is_final(cls: type) -> bool:
    try:
        type('_pyo3stubs_probe', (cls,), {})
    except TypeError:
        return True
    return False


def _collect_finality(ctx: CheckContext, errors: list[str]) -> None:
    path = str(ctx.cfg.stub_path)
    runtime = ctx.runtime_module
    for name, node in ctx.stub_classes.items():
        if name.startswith('_'):
            continue
        cls = getattr(runtime, name, None)
        if not isinstance(cls, type):
            continue  # stub-only typing helper (protocols)
        stub_final = 'final' in decorator_names(node)
        runtime_final = _runtime_is_final(cls)
        if runtime_final and not stub_final:
            errors.append(
                f'{loc(path, node)}: {name}: runtime class cannot be '
                'subclassed but the stub is not @final — user subclasses fail '
                'only at import time; add @final'
            )
        elif stub_final and not runtime_final:
            errors.append(
                f'{loc(path, node)}: {name}: stub is @final but the '
                'runtime class is subclassable — drop @final or close the '
                'runtime class'
            )


def _inspectable(obj: object) -> bool:
    try:
        inspect.signature(obj)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return False
    return True


def _collect_signature_coverage(ctx: CheckContext, errors: list[str]) -> None:
    runtime = ctx.runtime_module
    allow = ctx.cfg.uninspectable_allowlist
    seen: set[str] = set()
    blind: list[str] = []

    def probe(qualname: str, obj: object) -> None:
        if not callable(obj) or isinstance(obj, type):
            return
        if _inspectable(obj):
            return  # an allowlist entry for an inspectable callable is stale
        if qualname in allow:
            seen.add(qualname)
            return
        blind.append(qualname)

    def probe_class(qualname: str, cls: type, node: ast.ClassDef | None) -> None:
        if node is not None and class_prohibits_construction(node):
            return
        if _inspectable(cls):
            return  # an allowlist entry for an inspectable binding is stale
        if qualname in allow:
            seen.add(qualname)
            return
        blind.append(qualname)

    for name in dir(runtime):
        if name.startswith('_'):
            continue
        obj = getattr(runtime, name)
        if isinstance(obj, type):
            probe_class(name, obj, ctx.stub_classes.get(name))
            for member in obj.__dict__:
                if member.startswith('_'):
                    continue
                selected = passive_class_member(obj, member)
                if selected is not None and not isinstance(selected, property):
                    probe(f'{name}.{member}', selected)
        else:
            probe(name, obj)

    errors.extend(
        f'{qualname}: inspect.signature fails — every signature gate is blind '
        'here; fix text_signature or allowlist with a reason'
        for qualname in sorted(blind)
    )
    errors.extend(
        f'uninspectable allowlist entry {name!r} needs a non-empty reason'
        for name, reason in sorted(allow.items())
        if not reason.strip()
    )
    errors.extend(
        unused_allowlist_errors(
            'uninspectable', allow, seen, why='is inspectable again'
        )
    )


def _stub_match_args(
    node: ast.ClassDef,
) -> tuple[ast.stmt, tuple[str, ...] | None] | None:
    """``(statement, literal value)`` when the class declares ``__match_args__``;
    the value is ``None`` when it is declared without a comparable string tuple.
    """
    for stmt in node.body:
        target = None
        value = None
        if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
            target, value = stmt.target.id, stmt.value
        elif (
            isinstance(stmt, ast.Assign)
            and len(stmt.targets) == 1
            and isinstance(stmt.targets[0], ast.Name)
        ):
            target, value = stmt.targets[0].id, stmt.value
        if target != '__match_args__':
            continue
        if not isinstance(value, ast.Tuple):
            return stmt, None
        # Narrowing by construction: a non-string element simply does not
        # reach `names`, and a short list is what "not comparable" means.
        names = [
            elt.value
            for elt in value.elts
            if isinstance(elt, ast.Constant) and isinstance(elt.value, str)
        ]
        return stmt, tuple(names) if len(names) == len(value.elts) else None
    return None


def _collect_match_args(ctx: CheckContext, errors: list[str]) -> None:
    path = str(ctx.cfg.stub_path)
    runtime = ctx.runtime_module
    for name, node in ctx.stub_classes.items():
        if name.startswith('_'):
            continue
        cls = getattr(runtime, name, None)
        if not isinstance(cls, type):
            continue
        declared = _stub_match_args(node)
        actual = cls.__dict__.get('__match_args__')
        if declared is None:
            if actual is not None:
                errors.append(
                    f'{loc(path, node)}: {name}: runtime defines '
                    f'__match_args__ = {actual!r} but the stub does not declare '
                    'it — match statements lose positional patterns'
                )
            continue
        stmt, literal = declared
        if actual is None:
            errors.append(
                f'{loc(path, stmt)}: {name}.__match_args__: declared in the '
                'stub but missing at runtime'
            )
        elif literal is not None and tuple(actual) != literal:
            errors.append(
                f'{loc(path, stmt)}: {name}.__match_args__: stub {literal!r} '
                f'!= runtime {tuple(actual)!r}'
            )


#: `type X = ...` statements (Python 3.12+; none can parse on 3.11).
_TYPE_ALIAS_NODES: tuple[type[ast.stmt], ...] = tuple(
    node for node in (getattr(ast, 'TypeAlias', None),) if node is not None
)


def _module_bindings(body: list[ast.stmt]) -> set[str]:
    """Names a module body binds, through version and platform branches."""
    names: set[str] = set()
    for stmt in body:
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(stmt.name)
        elif isinstance(stmt, (ast.Import, ast.ImportFrom)):
            names.update(
                (alias.asname or alias.name).split('.')[0] for alias in stmt.names
            )
        elif isinstance(stmt, ast.Assign):
            names.update(
                target.id for target in stmt.targets if isinstance(target, ast.Name)
            )
        elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
            names.add(stmt.target.id)
        elif isinstance(stmt, _TYPE_ALIAS_NODES):
            names.add(stmt.name.id)  # type: ignore[attr-defined]
        elif isinstance(stmt, ast.If):
            names |= _module_bindings(stmt.body) | _module_bindings(stmt.orelse)
        elif isinstance(stmt, ast.Try):
            names |= _module_bindings(stmt.body)
            for handler in stmt.handlers:
                names |= _module_bindings(handler.body)
    return names


def _exported_names(body: list[ast.stmt]) -> ast.List | ast.Tuple | None:
    """The literal ``__all__`` list or tuple of a module body."""
    for stmt in body:
        if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1:
            target, value = stmt.targets[0], stmt.value
        elif isinstance(stmt, ast.AnnAssign) and stmt.value is not None:
            target, value = stmt.target, stmt.value
        else:
            continue
        if (
            isinstance(target, ast.Name)
            and target.id == '__all__'
            and isinstance(value, (ast.List, ast.Tuple))
        ):
            return value
    return None


def _collect_exports(ctx: CheckContext, errors: list[str]) -> None:
    path = str(ctx.cfg.stub_path)
    exported = _exported_names(ctx.stub_ast.body)
    if exported is None:
        return
    bound = _module_bindings(ctx.stub_ast.body)
    seen: set[str] = set()
    for elt in exported.elts:
        if not (isinstance(elt, ast.Constant) and isinstance(elt.value, str)):
            continue
        name = elt.value
        if name in seen:
            errors.append(f'{loc(path, elt)}: __all__: {name!r} listed twice')
        seen.add(name)
        if name not in bound:
            errors.append(
                f'{loc(path, elt)}: __all__: {name!r} is exported but the stub '
                'never defines it'
            )


def collect_errors(cfg: StubConfig) -> Findings:
    """Run all structural gates; empty when clean."""
    ctx = CheckContext(cfg)
    errors: list[str] = []
    _collect_overload_hygiene(ctx, errors)
    _collect_finality(ctx, errors)
    _collect_signature_coverage(ctx, errors)
    _collect_match_args(ctx, errors)
    _collect_exports(ctx, errors)
    defs = sum(
        len(group)
        for body in (ctx.stub_ast.body, *(c.body for c in ctx.stub_classes.values()))
        for group in function_groups(body).values()
    )
    return Findings(errors, examined=defs + len(ctx.stub_classes))
