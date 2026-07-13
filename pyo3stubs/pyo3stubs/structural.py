"""Structural stub checks mypy cannot express.

Four gates over the stub AST + live runtime:

* **overload hygiene** — in a same-name multi-def group every def must carry
  ``@overload`` and the last real variant carries the shared docstring.
  An undecorated def in a group is the silent dead-overload class: pyright
  applies last-def-wins without a diagnostic, so the narrowing simply vanishes
  (this exact corruption shipped once — 22 dead defs from a stray codemod).
* **finality** — a runtime class that cannot be subclassed (any PyO3 class
  without ``#[pyclass(subclass)]``) must be ``@final`` in the stub, and vice
  versa, so user subclassing fails statically instead of at import time.
* **signature coverage** — a public runtime callable where
  ``inspect.signature`` fails is invisible to every signature gate (stubtest
  skips it silently); require an explicit allowlist entry per blind spot.
* **``__match_args__`` parity** — the stub's literal tuple must equal the
  runtime value (match statements silently misbind otherwise).

Scalar↔array return duality is the separate ``duality`` gate
(:mod:`pyo3stubs.duality`), activated by ``StubConfig.duality``.
"""

from __future__ import annotations

import ast
import inspect
from typing import TYPE_CHECKING

from pyo3stubs.ast_util import function_groups
from pyo3stubs.context import CheckContext

if TYPE_CHECKING:
    from pyo3stubs.config import StubConfig


def _decorator_names(node: ast.FunctionDef) -> set[str]:
    names = set()
    for dec in node.decorator_list:
        target = dec.func if isinstance(dec, ast.Call) else dec
        if isinstance(target, ast.Attribute):
            names.add(target.attr)
        elif isinstance(target, ast.Name):
            names.add(target.id)
    return names


def _parameters(node: ast.FunctionDef) -> list[tuple[str, str, str | None]]:
    """``(name, kind, default-source)`` triples mirroring inspect kinds."""
    args = node.args
    params: list[tuple[str, str, str | None]] = []
    positional = [*args.posonlyargs, *args.args]
    defaults: list[ast.expr | None] = [None] * (
        len(positional) - len(args.defaults)
    ) + list(args.defaults)
    for arg, default in zip(positional, defaults, strict=True):
        kind = (
            'POSITIONAL_ONLY' if arg in args.posonlyargs else 'POSITIONAL_OR_KEYWORD'
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


def _check_overload_group(
    qualname: str,
    defs: list[ast.FunctionDef],
    path: str,
    errors: list[str],
) -> None:
    decorators = [_decorator_names(node) for node in defs]
    if any('property' in names or 'setter' in names for names in decorators):
        return  # property/setter pairs share a name by design
    if len(defs) == 1:
        if 'overload' in decorators[0]:
            errors.append(
                f'{path}:{defs[0].lineno}: {qualname}: single @overload def — '
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
            f'{path}:{defs[0].lineno}: {qualname}: duplicate defs without '
            f'@overload at line(s) {lines} — pyright silently drops every def '
            'but the last (dead overloads); decorate all variants'
        )
        return
    # The last real variant is the docstring carrier. It is canonical only for
    # parameter names, kinds, ordering, and defaults; its annotations stay
    # narrow, because a synthetic union overload weakens type narrowing.
    canonical = {
        name: (kind, default) for name, kind, default in _parameters(defs[-1])
    }
    order = [name for name, _, _ in _parameters(defs[-1])]
    for variant in defs[:-1]:
        last_index = -1
        for name, kind, default in _parameters(variant):
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
                        f'{path}:{variant.lineno}: {qualname}: overload '
                        f'parameter {name!r} out of order'
                    )
                last_index = index
            base_kind, base_default = canonical[name]
            if kind != base_kind:
                errors.append(
                    f'{path}:{variant.lineno}: {qualname}.{name}: overload kind '
                    f'{kind} != canonical {base_kind}'
                )
            if (
                default is not None
                and base_default is not None
                and default != base_default
            ):
                errors.append(
                    f'{path}:{variant.lineno}: {qualname}.{name}: overload '
                    f'default {default} != canonical {base_default}'
                )


def _collect_overload_hygiene(ctx: CheckContext, errors: list[str]) -> None:
    path = str(ctx.cfg.stub_path)
    for name, defs in function_groups(ctx.stub_ast.body).items():
        _check_overload_group(name, defs, path, errors)
    for node in ctx.stub_ast.body:
        if isinstance(node, ast.ClassDef):
            for name, defs in function_groups(node.body).items():
                _check_overload_group(f'{node.name}.{name}', defs, path, errors)
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
        names = [
            dec.id
            for dec in node.decorator_list
            if isinstance(dec, ast.Name)
        ]
        for name in {n for n in names if names.count(n) > 1}:
            errors.append(
                f'{path}:{node.lineno}: {node.name}: decorator `@{name}` '
                f'appears {names.count(name)} times (codemod residue)'
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
    for node in ctx.stub_ast.body:
        if not isinstance(node, ast.ClassDef) or node.name.startswith('_'):
            continue
        cls = getattr(runtime, node.name, None)
        if not isinstance(cls, type):
            continue  # stub-only typing helper (protocols)
        stub_final = 'final' in {
            getattr(dec, 'id', getattr(dec, 'attr', None))
            for dec in node.decorator_list
        }
        runtime_final = _runtime_is_final(cls)
        if runtime_final and not stub_final:
            errors.append(
                f'{path}:{node.lineno}: {node.name}: runtime class cannot be '
                'subclassed but the stub is not @final — user subclasses fail '
                'only at import time; add @final'
            )
        elif stub_final and not runtime_final:
            errors.append(
                f'{path}:{node.lineno}: {node.name}: stub is @final but the '
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

    for name in dir(runtime):
        if name.startswith('_'):
            continue
        obj = getattr(runtime, name)
        if isinstance(obj, type):
            for member in obj.__dict__:
                if member.startswith('_'):
                    continue
                static = inspect.getattr_static(obj, member)
                if isinstance(static, (property, staticmethod, classmethod)):
                    static = getattr(obj, member)
                if not isinstance(static, property):
                    probe(f'{name}.{member}', getattr(obj, member))
        else:
            probe(name, obj)

    errors.extend(
        f'{qualname}: inspect.signature fails — every signature gate is blind '
        'here; fix text_signature or allowlist with a reason'
        for qualname in sorted(blind)
    )
    errors.extend(
        f'uninspectable allowlist entry {qualname!r} is inspectable again — '
        'drop it'
        for qualname in sorted(set(allow) - seen)
    )


def _stub_match_args(node: ast.ClassDef) -> tuple[int, tuple[str, ...] | None] | None:
    """``(lineno, literal value)`` when the class declares ``__match_args__``;
    value is ``None`` when declared without a comparable tuple literal.
    """
    for stmt in node.body:
        target = None
        value = None
        if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
            target, value = stmt.target.id, stmt.value
        elif isinstance(stmt, ast.Assign) and len(stmt.targets) == 1 and isinstance(
            stmt.targets[0], ast.Name
        ):
            target, value = stmt.targets[0].id, stmt.value
        if target != '__match_args__':
            continue
        if isinstance(value, ast.Tuple) and all(
            isinstance(elt, ast.Constant) and isinstance(elt.value, str)
            for elt in value.elts
        ):
            literal = tuple(elt.value for elt in value.elts)  # type: ignore[misc]
            return stmt.lineno, literal
        return stmt.lineno, None
    return None


def _collect_match_args(ctx: CheckContext, errors: list[str]) -> None:
    path = str(ctx.cfg.stub_path)
    runtime = ctx.runtime_module
    for node in ctx.stub_ast.body:
        if not isinstance(node, ast.ClassDef) or node.name.startswith('_'):
            continue
        cls = getattr(runtime, node.name, None)
        if not isinstance(cls, type):
            continue
        declared = _stub_match_args(node)
        actual = cls.__dict__.get('__match_args__')
        if declared is None:
            if actual is not None:
                errors.append(
                    f'{path}:{node.lineno}: {node.name}: runtime defines '
                    f'__match_args__ = {actual!r} but the stub does not declare '
                    'it — match statements lose positional patterns'
                )
            continue
        lineno, literal = declared
        if actual is None:
            errors.append(
                f'{path}:{lineno}: {node.name}.__match_args__: declared in the '
                'stub but missing at runtime'
            )
        elif literal is not None and tuple(actual) != literal:
            errors.append(
                f'{path}:{lineno}: {node.name}.__match_args__: stub {literal!r} '
                f'!= runtime {tuple(actual)!r}'
            )


def collect_errors(cfg: StubConfig) -> list[str]:
    """Run all structural gates; empty when clean."""
    ctx = CheckContext(cfg)
    errors: list[str] = []
    _collect_overload_hygiene(ctx, errors)
    _collect_finality(ctx, errors)
    _collect_signature_coverage(ctx, errors)
    _collect_match_args(ctx, errors)
    return errors
