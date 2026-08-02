"""Unified CLI for the PyO3 stub toolkit.

    pyo3stubs <command> --config tools/stubconfig.py

``--config`` points at a Python file exposing either a ``config()`` callable or
a ``CONFIG`` attribute returning a :class:`~pyo3stubs.config.StubConfig`.

Commands are the gate names (see :mod:`pyo3stubs.gates`), plus ``gen-docs``,
``check-all``, and ``init``. Exit status is 0 when every gate that ran was
clean, and 1 when any reported a violation — a gate that is skipped, disabled,
or inactive is reported as such and does not affect the status.
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
import uuid
from pathlib import Path

from pyo3stubs.config import StubConfig
from pyo3stubs.gates import GATES, Result, Status, gate, gate_names, run_all


def load_config(path: str | Path) -> StubConfig:
    """Import the project's config shim and return its :class:`StubConfig`."""
    module_name = f'_pyo3stubs_config_{uuid.uuid4().hex}'
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise SystemExit(f'cannot import stub config from {str(path)!r}')
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


def _report(result: Result, config_path: str) -> int:
    """Print one gate's outcome; 1 when it found violations."""
    if result.status is not Status.VIOLATIONS:
        print(result.line)
        return 0
    sys.stderr.write(f'{result.line}\n')
    for error in result.errors:
        sys.stderr.write(f'  {error}\n')
    remedy = result.gate.remedy.replace('<shim>', config_path)
    sys.stderr.write(f'  -> {remedy}\n')
    return 1


def _gen_docs(cfg: StubConfig, config_path: str) -> int:
    """Write runtime docstrings into the stub, and report contract violations.

    The write is unconditional on purpose: running this is how a missing
    docstring gets fixed, so refusing to write when the contract is unmet would
    withhold the remedy. The exit status still carries the violations.
    """
    from pyo3stubs.gen import render_stub_with_docs  # libcst only needed here

    status = _report(gate('doc-contract').run(cfg), config_path)
    cfg.stub_path.write_text(render_stub_with_docs(cfg), encoding='utf-8')
    print(f'wrote {cfg.stub_path}')
    return status


_INIT_CONFIG_TEMPLATE = '''\
"""pyo3stubs configuration for {package}."""

from pathlib import Path

from pyo3stubs import StubConfig

ROOT = Path(__file__).resolve().parent.parent


def config() -> StubConfig:
    return StubConfig(
        module='{package}._lib',
        stub_path=ROOT / 'python' / '{package}' / '_lib.pyi',
        src_root=ROOT / 'src',
    )
'''

_INIT_TEST_TEMPLATE = '''\
"""pyo3stubs gates for {package} (one test per gate)."""

from pyo3stubs.testing import gate_test

test_pyo3stubs_gate = gate_test('tools/stubconfig.py')
'''


def _init(package: str) -> int:
    """Scaffold ``tools/stubconfig.py`` + ``tests/test_stubs.py`` in cwd."""
    for path, template in (
        (Path('tools/stubconfig.py'), _INIT_CONFIG_TEMPLATE),
        (Path('tests/test_stubs.py'), _INIT_TEST_TEMPLATE),
    ):
        path.parent.mkdir(exist_ok=True)
        if path.exists():
            print(f'{path} already exists; skipping')
            continue
        path.write_text(template.format(package=package))
        print(f'wrote {path}')
    return 0


def _gate_help() -> str:
    width = max(len(name) for name in gate_names())
    return '\n'.join(
        f'  {entry.name:<{width}}  {entry.summary} [{entry.tier}]' for entry in GATES
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog='pyo3stubs',
        description='Stub/runtime/Rust parity gates for PyO3 packages.',
        epilog=f'gates:\n{_gate_help()}',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        'command',
        metavar='COMMAND',
        choices=['gen-docs', 'check-all', 'init', *gate_names()],
        help='a gate name, or gen-docs / check-all / init',
    )
    parser.add_argument('--config', help='path to the StubConfig shim')
    parser.add_argument('--package', help='init: the package name to scaffold for')
    args = parser.parse_args(argv)

    if args.command == 'init':
        if not args.package:
            parser.error('init requires --package <name>')
        return _init(args.package)
    if not args.config:
        parser.error(f'{args.command} requires --config <path>')
    cfg = load_config(args.config)

    if args.command == 'gen-docs':
        return _gen_docs(cfg, args.config)
    if args.command == 'check-all':
        status = 0
        for result in run_all(cfg):
            status |= _report(result, args.config)
        return status
    return _report(gate(args.command).run(cfg), args.config)


if __name__ == '__main__':
    raise SystemExit(main())
