"""Distribution wheel builds and identities shared by benchmarks and PGO."""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import subprocess
import sys
import tomllib
import zipfile
from contextlib import nullcontext
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def file_identity(path: Path) -> dict:
    return {
        'path': str(path.resolve()),
        'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def compilation_identity(package: Path) -> dict:
    paths = [
        ROOT / name
        for name in (
            'Cargo.toml',
            'Cargo.lock',
            'rust-toolchain.toml',
            '.cargo/config.toml',
        )
    ]
    paths.extend(
        path
        for directory in (
            package / 'src',
            package / 'python',
            package / 'native',
            ROOT / 'vendor',
        )
        for path in directory.rglob('*')
        if path.is_file()
        and not {'.git', '__pycache__', 'target'}.intersection(path.parts)
        and path.suffix not in {'.pyc', '.pyo'}
    )
    paths.extend(
        path
        for path in (package / 'Cargo.toml', package / 'build.rs')
        if path.is_file()
    )
    manifest = tomllib.loads((package / 'pyproject.toml').read_text())
    return {
        'files': {
            str(path.relative_to(ROOT)): file_identity(path)['sha256']
            for path in sorted(paths)
        },
        'packaging': {
            'build-system': manifest['build-system'],
            'maturin': manifest['tool']['maturin'],
        },
    }


def source_identity(package: Path) -> dict:
    inputs = compilation_identity(package)['files']
    inputs[str((package / 'pyproject.toml').relative_to(ROOT))] = file_identity(
        package / 'pyproject.toml'
    )['sha256']
    return {
        'revision': subprocess.check_output(
            ['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True, timeout=15
        ).strip(),
        'build_inputs': inputs,
        'build_inputs_sha256': hashlib.sha256(
            json.dumps(inputs, sort_keys=True).encode()
        ).hexdigest(),
    }


def one_wheel(directory: Path) -> Path:
    wheels = list(directory.glob('*.whl'))
    if len(wheels) != 1:
        raise RuntimeError(f'expected one wheel in {directory}, found {wheels}')
    return wheels[0]


def build_wheel(
    package: Path,
    output: Path,
    *,
    profile: str = 'release-dist',
    target: str | None = None,
    cargo_config: tuple[str, ...] = (),
    compatibility: str | None = None,
    stage=None,
) -> Path:
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise RuntimeError('wheel output directory must be empty')
    compilation = compilation_identity(package)
    original = source_identity(package)
    command = [
        'maturin',
        'build',
        '--manifest-path',
        str(package / 'Cargo.toml'),
        '--locked',
        '--profile',
        profile,
        '--interpreter',
        sys.executable,
        '--out',
        str(output),
        '--verbose',
    ]
    if target is not None:
        command.extend(('--target', target))
    if compatibility is not None:
        command.extend(('--compatibility', compatibility))
    for setting in cargo_config:
        command.extend(('--config', setting))
    compiler = subprocess.check_output(['rustc', '-vV'], text=True, timeout=15)
    cargo_home = Path(os.environ.get('CARGO_HOME', Path.home() / '.cargo'))
    configurations = {
        str(path): file_identity(path)
        for path in (cargo_home / 'config', cargo_home / 'config.toml')
        if path.is_file()
    }
    with (
        stage if stage is not None else nullcontext(),
        (output / 'build.log').open('w') as log,
    ):
        staged = source_identity(package)
        subprocess.run(
            command,
            cwd=ROOT,
            check=True,
            timeout=900,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    if source_identity(package)['build_inputs'] != original['build_inputs']:
        raise RuntimeError('source changed while building the wheel')
    wheel = one_wheel(output)
    wheel.with_suffix('.build.json').write_text(
        json.dumps(
            {
                'source': original,
                'staged_source': staged,
                'compilation': compilation,
                'target': target,
                'command': command,
                'compiler': compiler,
                'python': sys.version,
                'cargo_configurations': configurations,
                'environment': {
                    name: value
                    for name, value in os.environ.items()
                    if name.startswith((
                        'RUST',
                        'CARGO_ENCODED_RUSTFLAGS',
                        'CARGO_PROFILE_',
                        'CARGO_TARGET_',
                        'PYO3_',
                    ))
                },
                'wheel': file_identity(wheel),
                'log': file_identity(output / 'build.log'),
            },
            indent=2,
        )
        + '\n'
    )
    return wheel


def installed_wheel(wheel: Path, module_name: str) -> dict:
    module = importlib.import_module(module_name)
    package_name = module_name.split('.', maxsplit=1)[0]
    package = Path(importlib.import_module(package_name).__file__).parent
    with zipfile.ZipFile(wheel) as archive:
        members = {
            name: hashlib.sha256(archive.read(name)).hexdigest()
            for name in archive.namelist()
            if name.startswith((package_name + '/', package_name + '.libs/'))
            and not name.endswith('/')
        }
    if not members:
        raise RuntimeError('wheel contains no files for its declared Python package')
    for name, digest in members.items():
        path = package.parent / name
        if file_identity(path)['sha256'] != digest:
            raise RuntimeError(f'installed wheel member differs: {path}')
    return {
        'wheel': file_identity(wheel),
        'native': file_identity(Path(module.__file__)),
        'members': members,
        'python': sys.version,
    }
