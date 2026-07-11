"""Unified CLI for the PyO3 stub toolkit.

A project drives every gate through one entry point::

    python -m pyo3stubs <command> --config tools/stubconfig.py [--check]

``--config`` points at a Python file that exposes either a ``config()`` callable
or a ``CONFIG`` attribute returning a :class:`~pyo3stubs.config.StubConfig`.

Commands mirror the gate registry (see :mod:`pyo3stubs.gates`), plus::

  gen-docs       inject runtime docstrings into the stub (``--check`` verifies)
  check-all      every gate in registry order
  init           scaffold ``tools/stubconfig.py`` + ``tests/test_stubs.py``
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
import uuid

from pyo3stubs.config import StubConfig
from pyo3stubs.gates import REGISTRY, SUCCESS, run_all


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

    status = _report(REGISTRY['doc-contract'](cfg), SUCCESS['doc-contract'])
    code = render_stub_with_docs(cfg)
    if check:
        if cfg.stub_path.read_text(encoding='utf-8') != code:
            sys.stderr.write(
                f'{cfg.stub_path} is out of sync; run `pyo3stubs gen-docs`\n'
            )
            return 1
        print(SUCCESS['gen-docs-sync'])
        return status
    cfg.stub_path.write_text(code, encoding='utf-8')
    print(f'wrote {cfg.stub_path}')
    return status


_INIT_CONFIG_TEMPLATE = (
    '\"\"\"pyo3stubs configuration for {package}.\"\"\"\n'
    '\n'
    'from pathlib import Path\n'
    '\n'
    'from pyo3stubs import StubConfig\n'
    '\n'
    'ROOT = Path(__file__).resolve().parent.parent\n'
    '\n'
    '\n'
    'def config() -> StubConfig:\n'
    '    return StubConfig(\n'
    "        module='{package}._lib',\n"
    "        stub_path=ROOT / 'python' / '{package}' / '_lib.pyi',\n"
    "        src_root=ROOT / 'src',\n"
    '    )\n'
)

_INIT_TEST_TEMPLATE = (
    '\"\"\"pyo3stubs gates for {package} (one test per gate).\"\"\"\n'
    '\n'
    'from pyo3stubs.testing import gate_test\n'
    '\n'
    "test_pyo3stubs_gate = gate_test('tools/stubconfig.py')\n"
)


def _init(package: str) -> int:
    """Scaffold ``tools/stubconfig.py`` + ``tests/test_stubs.py`` in cwd."""
    from pathlib import Path

    tools = Path('tools')
    tests = Path('tests')
    tools.mkdir(exist_ok=True)
    tests.mkdir(exist_ok=True)
    for path, template in (
        (tools / 'stubconfig.py', _INIT_CONFIG_TEMPLATE),
        (tests / 'test_stubs.py', _INIT_TEST_TEMPLATE),
    ):
        if path.exists():
            print(f'{path} already exists; skipping')
            continue
        path.write_text(template.format(package=package))
        print(f'wrote {path}')
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog='pyo3stubs', description=__doc__)
    parser.add_argument(
        'command',
        choices=['gen-docs', 'check-all', 'init', *REGISTRY],
    )
    parser.add_argument('--config', help='path to the StubConfig shim')
    parser.add_argument(
        '--package', help='init: the Python package name to scaffold for'
    )
    parser.add_argument(
        '--check',
        action='store_true',
        help='gen-docs: verify the stub is in sync instead of writing it',
    )
    args = parser.parse_args(argv)
    if args.command == 'init':
        if not args.package:
            parser.error('init requires --package <name>')
        return _init(args.package)
    if not args.config:
        parser.error(f'{args.command} requires --config <path>')
    cfg = load_config(args.config)

    if args.command == 'gen-docs':
        return _gen_docs(cfg, check=args.check)
    if args.command in REGISTRY:
        return _report(REGISTRY[args.command](cfg), SUCCESS[args.command])
    status = 0
    for name, errors in run_all(cfg).items():
        status |= _report(errors, SUCCESS[name])
    return status


if __name__ == '__main__':
    raise SystemExit(main())
