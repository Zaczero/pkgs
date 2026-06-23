"""Check namespace submodule facades alias ``_lib`` entry points."""

from __future__ import annotations

import importlib
from dataclasses import dataclass

from pyo3stubcheck.config import StubCheckConfig
from pyo3stubcheck.context import CheckContext


@dataclass(frozen=True)
class NamespaceFacadeCheck:
    """Gate ``package.<ns>`` re-exports of ``_lib.<prefix>*`` under short aliases."""

    def collect(self, cfg: StubCheckConfig, ctx: CheckContext) -> list[str]:
        if not cfg.package_module or not cfg.namespace_modules:
            return []

        package = importlib.import_module(cfg.package_module)
        runtime = ctx.runtime_module
        lib_ids = {id(getattr(runtime, name)) for name in dir(runtime)}
        errors: list[str] = []

        for namespace in cfg.namespace_modules:
            prefix = cfg.namespace_prefix_template.format(namespace=namespace)
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