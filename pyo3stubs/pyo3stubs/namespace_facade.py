"""Config-activated gate: namespace facades alias ``_lib`` entry points.

Activated by ``StubConfig.namespace``; no-op when unset.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

from pyo3stubs.context import CheckContext

if TYPE_CHECKING:
    from pyo3stubs.config import StubConfig


def collect_errors(cfg: StubConfig) -> list[str]:
    """Gate ``package.<ns>`` re-exports of ``_lib.<prefix>*`` under short aliases."""
    conf = cfg.namespace
    if conf is None or not conf.modules:
        return []

    ctx = CheckContext(cfg)
    package = importlib.import_module(conf.package_module)
    runtime = ctx.runtime_module
    lib_ids = {id(getattr(runtime, name)) for name in dir(runtime)}
    errors: list[str] = []

    for namespace in conf.modules:
        prefix = conf.prefix_template.format(namespace=namespace)
        module = getattr(package, namespace)
        exported = getattr(module, '__all__', None) or [
            name for name in dir(module) if not name.startswith('_')
        ]
        aliased: set[int] = set()
        for name in exported:
            member = getattr(module, name, None)
            if not callable(member) or isinstance(member, type):
                continue
            if id(member) in lib_ids:
                aliased.add(id(member))
            else:
                errors.append(
                    f'ns:{namespace}.{name}: facade member is not a plain `_lib` alias'
                )
        errors.extend(
            f'ns:{namespace}: runtime `_lib.{name}` has no facade alias'
            for name in sorted(dir(runtime))
            if name.startswith(prefix) and id(getattr(runtime, name)) not in aliased
        )
    return errors
