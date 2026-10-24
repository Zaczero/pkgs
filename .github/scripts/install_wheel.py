#!/usr/bin/env python3
"""Install the current interpreter's wheel and verify its imported payload."""

from __future__ import annotations

import hashlib
import importlib
import os
import subprocess
import sys
import tomllib
import zipfile
from pathlib import Path

from packaging.tags import sys_tags
from packaging.utils import canonicalize_name, parse_wheel_filename


def select_wheel(package: Path, directory: Path) -> Path:
    project = tomllib.loads((package / 'pyproject.toml').read_text())
    name = canonicalize_name(project['project']['name'])
    supported = set(sys_tags())
    wheels = []
    for wheel in directory.glob('*.whl'):
        wheel_name, _, _, tags = parse_wheel_filename(wheel.name)
        if wheel_name == name and tags & supported:
            wheels.append(wheel.resolve())
    if len(wheels) != 1:
        raise SystemExit(f'expected one compatible {name} wheel, found {wheels}')
    return wheels[0]


def main() -> None:
    package, directory = map(Path, sys.argv[1:])
    wheel = select_wheel(package, directory)
    subprocess.run(
        ['uv', 'pip', 'install', '--python', sys.executable, '--no-deps', str(wheel)],
        check=True,
    )
    project = tomllib.loads((package / 'pyproject.toml').read_text())
    module_name = project.get('tool', {}).get('maturin', {}).get('module-name')
    if module_name is None:
        module_name = os.environ['IMPORT_NAME']
    module = importlib.import_module(module_name)
    installed = Path(module.__file__).resolve()
    module_parent = module_name.rsplit('.', maxsplit=1)[0].replace('.', '/')
    member = (
        f'{module_parent}/{installed.name}'
        if '.' in module_name
        else f'{module_name}/{installed.name}'
        if installed.name == '__init__.py'
        else installed.name
    )
    with zipfile.ZipFile(wheel) as archive:
        expected = hashlib.sha256(archive.read(member)).hexdigest()
    actual = hashlib.sha256(installed.read_bytes()).hexdigest()
    if actual != expected:
        raise SystemExit(f'imported {installed} does not match {wheel.name}:{member}')
    print(
        f'Wheel: {wheel.name} sha256={hashlib.sha256(wheel.read_bytes()).hexdigest()}'
    )
    print(f'Imported payload: {installed} sha256={actual}')


if __name__ == '__main__':
    main()
