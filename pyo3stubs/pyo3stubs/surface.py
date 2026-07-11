"""Cross-surface option-parameter parity gate.

Compares the option block (trailing params after required data inputs) of every
operation that appears on more than one surface — e.g. class method, array
method, free function — so a knob added to one spelling cannot silently miss
the others. Surfaces come from ``StubConfig.surfaces``; empty means no-op.
"""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyo3stubs.config import StubConfig

Param = tuple[str, str, str]
Blocks = tuple[int, tuple[Param, ...]]


def _callables(owner: object) -> dict[str, object]:
    """Public function/method members of one surface."""
    members: dict[str, object] = {}
    for name in dir(owner):
        if name.startswith('_'):
            continue
        try:
            value = inspect.getattr_static(owner, name)
        except AttributeError:
            # PEP 562 lazy exports (``__getattr__``) are visible in ``dir`` but
            # not in the module dict until accessed — skip them here.
            continue
        if inspect.isclass(value) or isinstance(value, property):
            continue
        if callable(getattr(owner, name)):
            members[name] = getattr(owner, name)
    return members


def _blocks(func: object) -> Blocks | None:
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
        (
            parameter.name,
            parameter.kind.name,
            repr(parameter.default),
        )
        for parameter in parameters[data:]
    )
    return data, options


def collect_errors(cfg: StubConfig) -> list[str]:
    """Flag unregistered cross-surface option-block drift."""
    surfaces = [(label, _callables(owner)) for label, owner in cfg.surfaces]
    shared: dict[str, list[tuple[str, Blocks]]] = {}
    for label, members in surfaces:
        for name, func in members.items():
            blocks = _blocks(func)
            if blocks is not None:
                shared.setdefault(name, []).append((label, blocks))

    errors: list[str] = []
    for name, variants in sorted(shared.items()):
        if len(variants) < 2 or name in cfg.known_divergences:
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
        '— drop the KNOWN_DIVERGENCES entry'
        for name in cfg.known_divergences
        if len(shared.get(name, [])) < 2
    )
    return errors
