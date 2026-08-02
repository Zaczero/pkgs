"""Cross-surface option-parameter parity gate.

Compares the option block (trailing params after required data inputs) of every
operation that appears on more than one surface — e.g. class method, array
method, free function — so a knob added to one spelling cannot silently miss
the others. Activated by ``StubConfig.surface``; a reported no-op when unset.

Defaults are compared as *values*, the way ``stubtest`` compares them. Their
``repr`` is prose: two surfaces spelling one default differently is not drift,
and a ``repr`` that omits state would hide drift that is real.
"""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING

from pyo3stubs.report import Findings

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyo3stubs.config import StubConfig

#: ``(name, kind, default)`` — the option-block shape one surface declares.
Param = tuple[str, str, object]
Blocks = tuple[int, tuple[Param, ...]]


def _callables(owner: object) -> dict[str, Callable[..., object]]:
    """Public function/method members of one surface."""
    members: dict[str, Callable[..., object]] = {}
    for name in dir(owner):
        if name.startswith('_'):
            continue
        try:
            static = inspect.getattr_static(owner, name)
        except AttributeError:
            # PEP 562 lazy exports (``__getattr__``) are visible in ``dir`` but
            # not in the module dict until accessed — skip them here.
            continue
        if inspect.isclass(static) or isinstance(static, property):
            continue
        member = getattr(owner, name)
        if callable(member):
            members[name] = member
    return members


def _blocks(func: Callable[..., object]) -> Blocks | None:
    """Split a signature into (data-input count, option block)."""
    try:
        parameters = list(inspect.signature(func).parameters.values())
    except (TypeError, ValueError):
        return None
    if parameters and parameters[0].name == 'self':
        parameters = parameters[1:]
    data = 0
    while (
        data < len(parameters)
        and parameters[data].default is inspect.Parameter.empty
        and parameters[data].kind
        in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
    ):
        data += 1
    options = tuple(
        (parameter.name, parameter.kind.name, parameter.default)
        for parameter in parameters[data:]
    )
    return data, options


def collect_errors(cfg: StubConfig) -> Findings:
    """Flag unregistered cross-surface option-block drift."""
    conf = cfg.surface
    if conf is None:
        return Findings()
    shared: dict[str, list[tuple[str, Blocks]]] = {}
    for label, owner in conf.targets:
        for name, func in _callables(owner).items():
            blocks = _blocks(func)
            if blocks is not None:
                shared.setdefault(name, []).append((label, blocks))

    errors: list[str] = []
    for name, variants in sorted(shared.items()):
        if len(variants) < 2 or name in conf.known_divergences:
            continue
        (reference_label, (_, reference_options)) = variants[0]
        for label, (_, options) in variants[1:]:
            if options != reference_options:
                errors.append(
                    f'{name}: option parameters diverge — '
                    f'{reference_label}{[p[0] for p in reference_options]} != '
                    f'{label}{[p[0] for p in options]} '
                    f'({reference_options} != {options})'
                )
    errors.extend(
        f'{name}: registered divergence no longer exists on two surfaces '
        '— drop the known_divergences entry'
        for name in sorted(conf.known_divergences)
        if len(shared.get(name, [])) < 2
    )
    compared = sum(1 for variants in shared.values() if len(variants) >= 2)
    return Findings(errors, examined=compared)
