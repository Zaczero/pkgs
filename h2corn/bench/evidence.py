"""Artifact and runtime identities retained with benchmark observations."""

from __future__ import annotations

import asyncio
import importlib.machinery
import importlib.metadata
import json
import os
import platform
import shutil
import subprocess
import sys
import sysconfig
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'tools'))

from wheels import build_wheel, file_identity, installed_wheel, source_identity

__all__ = (
    'build_wheel',
    'file_identity',
    'run_identity',
    'verify_workers',
    'worker_identity',
)

RUNTIME_PREFIXES = (
    'h2corn',
    'uvicorn',
    'hypercorn',
    'gunicorn',
    'zttp',
    'connectrpc',
    'protobuf',
    'grpc',
    'grpclib',
)


def worker_identity() -> dict[str, Any]:
    """Identify modules actually loaded by the serving process."""
    modules = {}
    for name, module in tuple(sys.modules.items()):
        filename = getattr(module, '__file__', None)
        if (
            filename
            and (
                name.startswith(RUNTIME_PREFIXES)
                or any(
                    filename.endswith(suffix)
                    for suffix in importlib.machinery.EXTENSION_SUFFIXES
                )
            )
            and Path(filename).is_file()
        ):
            modules[name] = file_identity(Path(filename))
    return {
        'pid': os.getpid(),
        'executable': sys.executable,
        'python': sys.version,
        'gil_enabled': getattr(sys, '_is_gil_enabled', lambda: True)(),
        'event_loop': type(asyncio.get_running_loop()).__module__,
        'modules': modules,
    }


def run_identity(
    root: Path,
    wheel: Path | None,
    *,
    commands_required: tuple[str, ...] = ('h2corn', 'uvicorn', 'hypercorn', 'gunicorn'),
    tools_required: tuple[str, ...] = ('oha', 'k6', 'rustc'),
) -> dict[str, Any]:
    packages = {}
    for distribution in importlib.metadata.distributions():
        files = {}
        for relative in distribution.files or ():
            path = Path(str(distribution.locate_file(relative)))
            if path.is_file() and path.suffix in {'.py', '.so', '.pyd'}:
                files[str(relative)] = file_identity(path)
        packages[distribution.metadata['Name']] = {
            'version': distribution.version,
            'files': files,
        }
    commands = {}
    for name in commands_required:
        executable = Path(sys.executable).parent / name
        if not executable.is_file():
            raise RuntimeError(f'{name} must be installed beside {sys.executable}')
        if executable.read_text().splitlines()[0] != f'#!{sys.executable}':
            raise RuntimeError(
                f'{executable} does not select the benchmark interpreter'
            )
        commands[name] = file_identity(executable)
    tools = {}
    for name in tools_required:
        path = shutil.which(name)
        if path is None:
            raise RuntimeError(f'{name} is required')
        tools[name] = {
            **file_identity(Path(path)),
            'version': subprocess.check_output(
                [path, '-vV' if name == 'rustc' else '--version'],
                text=True,
                timeout=15,
            ).strip(),
        }
    wheel_record = None
    build_receipt = None
    if wheel is not None:
        installation = installed_wheel(wheel, 'h2corn._lib')
        wheel_record = {**installation['wheel'], 'members': installation['members']}
        receipt_path = wheel.with_suffix('.build.json')
        if receipt_path.is_file():
            build_receipt = json.loads(receipt_path.read_text())
            if build_receipt['wheel']['sha256'] != wheel_record['sha256']:
                raise RuntimeError('wheel differs from its release build receipt')
    cpuinfo = Path('/proc/cpuinfo').read_text()
    return {
        'source': source_identity(root / 'h2corn'),
        'packages': packages,
        'commands': commands,
        'tools': tools,
        'wheel': wheel_record,
        'build': build_receipt,
        'harness': {
            path.name: file_identity(path)
            for path in sorted((root / 'h2corn/bench').glob('*.py'))
        },
        'python': {
            **file_identity(Path(sys.executable)),
            'version': sys.version,
            'free_threaded': bool(sysconfig.get_config_var('Py_GIL_DISABLED')),
            'gil_enabled': getattr(sys, '_is_gil_enabled', lambda: True)(),
        },
        'system': {
            'kernel': platform.release(),
            'cpuinfo': cpuinfo,
            'affinity': sorted(os.sched_getaffinity(0)),
            'cgroup': Path('/proc/self/cgroup').read_text(),
            'load_average': os.getloadavg(),
        },
    }


def verify_workers(trial: dict[str, Any], identity: dict[str, Any]) -> None:
    installed = {
        file['path']: file['sha256']
        for package in identity['packages'].values()
        for file in package['files'].values()
    }
    for receipt in trial['workers'].values():
        if str(Path(receipt['executable']).resolve()) != identity['python']['path']:
            raise RuntimeError(
                'worker interpreter differs from the benchmark interpreter'
            )
        if receipt['event_loop'] != 'asyncio.unix_events':
            raise RuntimeError('worker did not select the standard asyncio loop')
        for name, module in receipt['modules'].items():
            if (
                name.startswith(RUNTIME_PREFIXES)
                and installed.get(module['path']) != module['sha256']
            ):
                raise RuntimeError(
                    f'worker module {name} differs from installed inventory'
                )
