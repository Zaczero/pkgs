"""Unified CLI for the PyO3 stub toolkit.

A project drives every gate through one entry point::

    python -m pyo3stubs <command> --config tools/stubconfig.py [--check]

``--config`` points at a Python file that exposes either a ``config()`` callable
or a ``CONFIG`` attribute returning a :class:`~pyo3stubs.config.StubConfig`.

Commands:
  gen-docs       inject runtime docstrings into the stub (``--check`` verifies
                 instead of writing); also enforces the docstring contract
  parity         stub signatures match the compiled runtime
  surface        cross-surface option-parameter parity
  leaked-types   no public pyclass leaks past registration/reachability
  doc-contract   public symbols documented (stdlib ast; no libcst needed)
  check-all      parity + surface + leaked-types + gen-docs --check
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
import uuid
from typing import Callable

from pyo3stubs.config import StubConfig
from pyo3stubs.doc_contract import collect_doc_contract_errors
from pyo3stubs.leaked_types import collect_errors as collect_leaked_types_errors
from pyo3stubs.parity import collect_errors as collect_parity_errors
from pyo3stubs.surface import collect_errors as collect_surface_errors

#: name -> (collect_errors(cfg) -> list[str], success label)
_CHECKS: dict[str, tuple[Callable[[StubConfig], list[str]], str]] = {
    'parity': (collect_parity_errors, 'stub parity OK: signatures match the runtime'),
    'surface': (collect_surface_errors, 'surface parity OK'),
    'leaked-types': (collect_leaked_types_errors, 'no leaked types'),
    'doc-contract': (collect_doc_contract_errors, 'doc contract OK'),
}


def load_config(path: str) -> StubConfig:
    """Import the project's config shim and return its :class:`StubConfig`."""
    module_name = f'_pyo3stubs_config_{uuid.uuid4().hex}'
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise SystemExit(f'cannot import stub config from {path!r}')
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    factory = getattr(module, 'config', None)
    cfg = factory() if callable(factory) else getattr(module, 'CONFIG', None)
    if not isinstance(cfg, StubConfig):
        raise SystemExit(
            f'{path}: must define `config() -> StubConfig` or `CONFIG: StubConfig`'
        )
    return cfg


def _report(errors: list[str], success: str) -> int:
    if errors:
        for error in errors:
            sys.stderr.write(f'  {error}\n')
        sys.stderr.write(f'{len(errors)} problem(s)\n')
        return 1
    print(success)
    return 0


def _gen_docs(cfg: StubConfig, *, check: bool) -> int:
    from pyo3stubs.gen import render_stub_with_docs  # libcst only needed here

    code, missing = render_stub_with_docs(cfg)
    if missing:
        for violation in missing:
            sys.stderr.write(f'  {violation}\n')
        sys.stderr.write(f'{len(missing)} docstring contract violation(s)\n')
        return 1
    if check:
        if cfg.stub_path.read_text(encoding='utf-8') != code:
            sys.stderr.write(
                f'{cfg.stub_path} is out of sync; run `pyo3stubs gen-docs`\n'
            )
            return 1
        print('stub docstrings in sync')
        return 0
    cfg.stub_path.write_text(code, encoding='utf-8')
    print(f'wrote {cfg.stub_path}')
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog='pyo3stubs', description=__doc__)
    parser.add_argument(
        'command',
        choices=['gen-docs', 'check-all', *_CHECKS],
    )
    parser.add_argument('--config', required=True, help='path to the StubConfig shim')
    parser.add_argument(
        '--check',
        action='store_true',
        help='gen-docs: verify the stub is in sync instead of writing it',
    )
    args = parser.parse_args(argv)
    cfg = load_config(args.config)

    if args.command == 'gen-docs':
        return _gen_docs(cfg, check=args.check)
    if args.command in _CHECKS:
        collect, success = _CHECKS[args.command]
        return _report(collect(cfg), success)
    # check-all: every check + a gen-docs sync verification.
    status = 0
    for name in ('parity', 'surface', 'leaked-types'):
        collect, success = _CHECKS[name]
        status |= _report(collect(cfg), success)
    status |= _gen_docs(cfg, check=True)
    return status


if __name__ == '__main__':
    raise SystemExit(main())
