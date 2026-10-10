"""Always-on gate: a manual ``text_signature`` must not contradict ``signature``.

PyO3 writes ``signature`` into the real call contract and ``text_signature``
into ``__doc__``/``__text_signature__``, and nothing checks that they agree —
so a renamed parameter or a drifted default documents a call that raises.

Both sides are compared as *structure and values*, never as source text. The
same string is ``"x"`` in a Rust ``signature`` and ``'x'`` in the
``text_signature`` that documents it, and the same integer is ``1_000`` in one
and ``1000`` in the other; comparing spellings made those permanent false
positives while letting genuinely different values through whenever the
grammar failed to recognise one of them.

The documented side is parsed as the Python signature it claims to be, which
is what CPython's ``inspect`` does with it too: a ``text_signature`` that is
not valid Python is itself the defect.
"""

from __future__ import annotations

import ast
from typing import TYPE_CHECKING

from pyo3stubs.report import Findings
from pyo3stubs.rust_scan import iter_sources, token_params, unquote

if TYPE_CHECKING:
    from collections.abc import Mapping

    from pyo3stubs.config import StubConfig, TextSignatureDefault
    from pyo3stubs.rust_scan import Item, RustSource

#: PyO3 spells the receiver `$self` (or `$module`/`$cls`) in a text_signature;
#: the Rust `signature` never lists it.
_RECEIVERS = ('$self', '$module', '$cls')

Param = tuple[str, str | None]


class _Incomparable:
    """Sentinel: this default names no value the two sides can share."""

    __slots__ = ()

    def __repr__(self) -> str:
        return 'INCOMPARABLE'


INCOMPARABLE = _Incomparable()


def _value(source: str) -> object:
    """A literal default value, or ``INCOMPARABLE`` for an expression."""
    if source in ('true', 'false'):
        return source == 'true'
    try:
        return ast.literal_eval(source)
    except (ValueError, SyntaxError, TypeError):
        return INCOMPARABLE


def _rust_params(source: str) -> list[Param]:
    """``(token, default source)`` of a Rust ``signature = (...)`` body.

    Tokens keep their ``*`` / ``**`` / ``/`` markers, so parameter *kinds* are
    compared and not just names.
    """
    return [(token, default or None) for token, default in token_params(source)]


def _python_params(body: str) -> list[Param]:
    """``(token, default source)`` of a documented ``text_signature`` body."""
    for receiver in _RECEIVERS:
        body = body.replace(receiver, 'self', 1)
    node = ast.parse(f'def _{body}: ...').body[0]
    assert isinstance(node, ast.FunctionDef)
    args = node.args
    params: list[Param] = []
    positional = [*args.posonlyargs, *args.args]
    padding: list[ast.expr | None] = [None] * (len(positional) - len(args.defaults))
    for index, (arg, default) in enumerate(
        zip(positional, [*padding, *args.defaults], strict=True)
    ):
        params.append((arg.arg, None if default is None else ast.unparse(default)))
        if index + 1 == len(args.posonlyargs):
            params.append(('/', None))
    if args.vararg:
        params.append((f'*{args.vararg.arg}', None))
    elif args.kwonlyargs:
        params.append(('*', None))
    for arg, kw_default in zip(args.kwonlyargs, args.kw_defaults, strict=True):
        params.append((
            arg.arg,
            None if kw_default is None else ast.unparse(kw_default),
        ))
    if args.kwarg:
        params.append((f'**{args.kwarg.arg}', None))
    return [param for param in params if param[0] != 'self']


def _defaults_agree(
    rust: str,
    text: str,
    aliases: Mapping[str, TextSignatureDefault],
) -> bool:
    """Compare literal values or a declared nonliteral public presentation."""
    rust_value, text_value = _value(rust), _value(text)
    if text_value is INCOMPARABLE:
        return False
    if rust_value is not INCOMPARABLE:
        return bool(rust_value == text_value)
    presentation = aliases.get(rust)
    return presentation is not None and bool(presentation.value == text_value)


def _declared(item: Item) -> tuple[str | None, str | None, int]:
    """``(signature, text_signature, line)`` across one item's attributes.

    PyO3 accepts them on one attribute or several — the fixture's own function
    carries ``#[pyfunction]`` and ``#[pyo3(signature = ..., text_signature =
    ...)]`` — so the item, not the attribute, is the unit of comparison.
    """
    signature = text_signature = None
    line = item.line
    for attr in item.attrs:
        signature = attr.value('signature') or signature
        documented = attr.value('text_signature')
        if documented:
            text_signature, line = documented, attr.line
    return signature, text_signature, line


def _compare(
    item: Item,
    source: RustSource,
    default_aliases: Mapping[str, TextSignatureDefault],
) -> list[str]:
    signature, text_signature, line = _declared(item)
    if signature is None or text_signature is None:
        return []
    rust = _rust_params(signature)
    location = f'{source.label}:{line}'
    try:
        text = _python_params(unquote(text_signature))
    except SyntaxError as exc:
        return [
            (
                f'{location}: text_signature {text_signature} is not a valid Python '
                f'signature ({exc.msg}) — inspect.signature cannot read it either'
            )
        ]
    if any(token.startswith('**') for token, _ in rust):
        # Rust **kwargs hides the exposed keyword names, but the Python syntax
        # of their manual presentation must still be valid.
        return []
    if [token for token, _ in rust] != [token for token, _ in text]:
        return [
            (
                f'{location}: text_signature params {[t for t, _ in text]} != '
                f'signature params {[t for t, _ in rust]}'
            )
        ]
    errors: list[str] = []
    for (token, rust_default), (_, text_default) in zip(rust, text, strict=True):
        if (rust_default is None) != (text_default is None):
            errors.append(
                f'{location}: {token}: text_signature default {text_default!r} != '
                f'signature default {rust_default!r} (has-default shape differs)'
            )
        elif (
            rust_default is not None
            and text_default is not None
            and not _defaults_agree(rust_default, text_default, default_aliases)
        ):
            presentation = default_aliases.get(rust_default)
            if presentation is not None and _value(rust_default) is INCOMPARABLE:
                errors.append(
                    f'{location}: {token}: text_signature default {text_default} != '
                    f'source-owned public default {presentation.value!r} for Rust '
                    f'expression {rust_default!r} ({presentation.reason})'
                )
            else:
                rust_value, text_value = _value(rust_default), _value(text_default)
                if rust_value is INCOMPARABLE or text_value is INCOMPARABLE:
                    errors.append(
                        f'{location}: {token}: cannot verify text_signature default '
                        f'{text_default} against Rust default expression '
                        f'{rust_default}; declare a reasoned '
                        '`text_signature_default_aliases` entry'
                    )
                else:
                    errors.append(
                        f'{location}: {token}: text_signature default {text_default} '
                        f'!= signature default {rust_default}'
                    )
    return errors


def collect_errors(cfg: StubConfig) -> Findings:
    """Manual ``text_signature`` attributes that contradict their ``signature``."""
    errors: list[str] = []
    examined = 0
    used_aliases: set[str] = set()
    for source in iter_sources(cfg):
        for item in source.walk():
            signature, text_signature, _ = _declared(item)
            if signature is None or text_signature is None:
                continue
            examined += 1
            used_aliases.update(
                default
                for _, default in _rust_params(signature)
                if default is not None
                and default in cfg.text_signature_default_aliases
                and _value(default) is INCOMPARABLE
            )
            errors.extend(_compare(item, source, cfg.text_signature_default_aliases))
    errors.extend(
        f'text_signature_default_aliases: unused Rust default expression {expression!r}'
        for expression in sorted(
            cfg.text_signature_default_aliases.keys() - used_aliases
        )
    )
    return Findings(errors, examined=examined)
