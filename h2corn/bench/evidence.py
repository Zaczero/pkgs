"""Artifact and runtime identities retained with benchmark observations."""

from __future__ import annotations

import asyncio
import hashlib
import importlib.machinery
import importlib.metadata
import json
import os
import platform
import shutil
import subprocess
import sys
import sysconfig
import zipfile
from pathlib import Path
from typing import Any

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


def file_identity(path: Path) -> dict[str, str]:
    return {
        'path': str(path.resolve()),
        'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def source_identity(root: Path) -> dict[str, Any]:
    def git(*arguments: str) -> str:
        return subprocess.check_output(
            ['git', '-C', str(root), *arguments],
            text=True,
            timeout=15,
        ).strip()

    files = [
        Path('Cargo.toml'),
        Path('Cargo.lock'),
        Path('rust-toolchain.toml'),
        Path('.cargo/config.toml'),
        Path('h2corn/Cargo.toml'),
        Path('h2corn/build.rs'),
        Path('h2corn/pyproject.toml'),
    ]
    files.extend(
        path.relative_to(root)
        for directory in ('h2corn/src', 'h2corn/python', 'vendor/pyo3')
        for path in sorted((root / directory).rglob('*'))
        if path.is_file() and path.suffix in {'.rs', '.toml', '.py', '.pyi'}
    )
    inputs = {
        str(path): hashlib.sha256((root / path).read_bytes()).hexdigest()
        for path in files
        if (root / path).is_file()
    }
    return {
        'revision': git('rev-parse', 'HEAD'),
        'status': git('status', '--porcelain', '--untracked-files=no'),
        'build_inputs': inputs,
        'build_inputs_sha256': hashlib.sha256(
            json.dumps(inputs, sort_keys=True).encode(),
        ).hexdigest(),
    }


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


def build_wheel(root: Path, output: Path) -> Path:
    """Retain the source/command receipt beside an explicit release wheel."""
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise RuntimeError('release wheel output directory must be empty')
    before = source_identity(root)
    command = [
        'maturin',
        'build',
        '--release',
        '--locked',
        '--interpreter',
        sys.executable,
        '--manifest-path',
        str(root / 'h2corn/Cargo.toml'),
        '--out',
        str(output),
    ]
    compiler = subprocess.check_output(['rustc', '-vV'], text=True, timeout=15)
    cargo_home = Path(os.environ.get('CARGO_HOME', Path.home() / '.cargo'))
    configurations = {
        str(path): file_identity(path)
        for path in (cargo_home / 'config', cargo_home / 'config.toml')
        if path.is_file()
    }
    with (output / 'build.log').open('w') as log:
        subprocess.run(
            command,
            check=True,
            stdout=log,
            stderr=subprocess.STDOUT,
            timeout=900,
            cwd=root,
        )
    after = source_identity(root)
    if before['build_inputs'] != after['build_inputs']:
        raise RuntimeError('build inputs changed while compiling the benchmark wheel')
    wheels = list(output.glob('*.whl'))
    if len(wheels) != 1:
        raise RuntimeError('release build did not produce exactly one wheel')
    wheel = wheels[0]
    receipt = {
        'source': before,
        'wheel': file_identity(wheel),
        'command': command,
        'compiler': compiler,
        'cargo_configurations': configurations,
        'environment': {
            name: value
            for name, value in os.environ.items()
            if name.startswith(('RUST', 'CARGO_PROFILE_', 'CARGO_TARGET_', 'PYO3_'))
        },
        'log': file_identity(output / 'build.log'),
    }
    wheel.with_suffix('.build.json').write_text(
        json.dumps(receipt, sort_keys=True, indent=2) + '\n',
    )
    return wheel


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
        import h2corn

        package = Path(h2corn.__file__).parent
        with zipfile.ZipFile(wheel) as archive:
            members = {
                name: hashlib.sha256(archive.read(name)).hexdigest()
                for name in archive.namelist()
                if name.startswith('h2corn/') and not name.endswith('/')
            }
        for name, expected in members.items():
            path = package / name.removeprefix('h2corn/')
            if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
                raise RuntimeError(f'installed {path} differs from {wheel}')
        wheel_record = {**file_identity(wheel), 'members': members}
        receipt_path = wheel.with_suffix('.build.json')
        if receipt_path.is_file():
            build_receipt = json.loads(receipt_path.read_text())
            if build_receipt['wheel']['sha256'] != wheel_record['sha256']:
                raise RuntimeError('wheel differs from its release build receipt')
            if (
                build_receipt['source']['build_inputs']
                != source_identity(root)['build_inputs']
            ):
                raise RuntimeError(
                    'current build inputs differ from the wheel build receipt'
                )
    cpuinfo = Path('/proc/cpuinfo').read_text()
    return {
        'source': source_identity(root),
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
