"""Build and smoke-test the release wheel artifact.

This is intentionally heavier than the default pytest lane. There is no
``pytest -m release`` marker — run this script directly before publishing::

    .venv/bin/python tools/wheel_smoke.py
    .venv/bin/python tools/wheel_smoke.py --inspect path/to/gometry-*.whl
    .venv/bin/python tools/wheel_smoke.py --installed
    .venv/bin/python tools/wheel_smoke.py --smoke-sdist path/to/gometry-*.tar.gz

Default mode builds an sdist via maturin, rebuilds the wheel from that sdist,
clean-installs it into a temporary venv, and runs a functional import/API smoke.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tarfile
import tempfile
import textwrap
import tomllib
import venv
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MONOREPO = ROOT.parent
LICENSE_SCRIPT = MONOREPO / '.github' / 'scripts' / 'gen_licenses.py'
H3_GIT_REV = '2b5bde491449c776cb9c1ae305a5f826f6d8e968'
H3_SOURCE = f'git+https://github.com/Zaczero/h3o.git?rev={H3_GIT_REV}#{H3_GIT_REV}'
TEXT_SUFFIXES = frozenset({
    '.json',
    '.lock',
    '.md',
    '.py',
    '.pyi',
    '.rs',
    '.toml',
    '.txt',
    '.yaml',
    '.yml',
})
ABSOLUTE_BUILD_PATH = re.compile(
    r'(?i)(?:/home/[^/\s]+/|/Users/[^/\s]+/|/tmp/|/private/tmp/|'
    r'/var/folders/|[a-z]:\\(?:Users|Temp)\\)'
)


def run(cmd: list[str], *, cwd: Path = ROOT, env: dict[str, str] | None = None) -> None:
    subprocess.run(cmd, cwd=cwd, env=env, check=True)


def release_build_env() -> dict[str, str]:
    env = os.environ.copy()
    env.pop('RUSTC_WRAPPER', None)
    env.pop('NIX_LDFLAGS', None)
    env.pop('MATURIN_PEP517_ARGS', None)
    # The Nix compiler wrapper otherwise injects this workstation's closure
    # paths into ELF RUNPATH. manylinux repair does not need the variable, but
    # setting it is harmless and makes local release artifacts equally clean.
    env['NIX_DONT_SET_RPATH'] = '1'
    env.setdefault('CARGO_TARGET_DIR', str(ROOT / 'target'))
    return env


def build_wheel_from_sdist(sdist: Path, out_dir: Path, *, env: dict[str, str]) -> Path:
    sdist_version = inspect_sdist(sdist)
    verify_sdist_dependency_sources(sdist)
    run(
        [
            'uv',
            'build',
            '--wheel',
            '--python',
            sys.executable,
            '--out-dir',
            str(out_dir),
            str(sdist),
        ],
        env=env,
    )
    wheels = sorted(out_dir.glob('gometry-*.whl'))
    if len(wheels) != 1:
        raise AssertionError(f'expected one gometry wheel, found {wheels!r}')
    wheel_version = inspect_wheel(wheels[0])
    if wheel_version != sdist_version:
        raise AssertionError(
            f'sdist/wheel version mismatch: {sdist_version!r} != {wheel_version!r}'
        )
    return wheels[0]


def build_wheel(out_dir: Path) -> Path:
    run([sys.executable, str(LICENSE_SCRIPT), 'gometry'], cwd=MONOREPO)
    env = release_build_env()
    maturin = str(Path(sys.executable).with_name('maturin'))
    run([maturin, 'sdist', '--out', str(out_dir)], env=env)
    sdists = sorted(out_dir.glob('gometry-*.tar.gz'))
    if len(sdists) != 1:
        raise AssertionError(f'expected one gometry sdist, found {sdists!r}')
    return build_wheel_from_sdist(sdists[0], out_dir, env=env)


def _is_text_member(name: str) -> bool:
    path = Path(name)
    return path.suffix.lower() in TEXT_SUFFIXES or path.name in {
        'METADATA',
        'PKG-INFO',
        'WHEEL',
    }


def _content_leaks(members: dict[str, str]) -> list[str]:
    leaks: list[str] = []
    for name, value in members.items():
        if match := ABSOLUTE_BUILD_PATH.search(value):
            leaks.append(f'{name}: absolute build path {match.group(0)!r}')
    return leaks


def native_notice_markers() -> list[tuple[str, str]]:
    """Component name and a notice phrase the wheel must reproduce."""
    manifest = ROOT / 'native-licenses.toml'
    if not manifest.exists():
        return []
    with manifest.open('rb') as stream:
        components = tomllib.load(stream)['component']
    markers: list[tuple[str, str]] = []
    for component in components:
        name = component['name']
        if copyright := component.get('copyright'):
            markers.append((name, copyright))
            continue
        with tarfile.open(ROOT / component['archive']) as archive:
            member = archive.extractfile(component['notice'])
            if member is None:
                raise AssertionError(f'native component {name} has no notice member')
            text = member.read().decode('utf-8')
        marker = next((line.strip() for line in text.splitlines() if line.strip()), '')
        if not marker:
            raise AssertionError(f'native component {name} has an empty notice')
        markers.append((name, marker))
    return markers


def inspect_sdist(path: Path) -> str:
    with tarfile.open(path) as archive:
        names = archive.getnames()
        text_members = {
            member.name: archive
            .extractfile(member)
            .read()
            .decode('utf-8', errors='ignore')
            for member in archive.getmembers()
            if member.isfile() and _is_text_member(member.name)
        }
    root = f'gometry-{path.name.removeprefix("gometry-").removesuffix(".tar.gz")}'
    required = {
        f'{root}/LICENSE-APACHE.md',
        f'{root}/LICENSE-MIT.md',
        f'{root}/LICENSE-THIRD-PARTY.md',
        f'{root}/Cargo.lock',
        f'{root}/pyproject.toml',
        f'{root}/rust-toolchain.toml',
    }
    if missing := sorted(required - set(names)):
        raise AssertionError(f'sdist missing root release files: {missing}')
    package_info = text_members.get(f'{root}/PKG-INFO', '')
    version_line = next(
        (line for line in package_info.splitlines() if line.startswith('Version: ')),
        '',
    )
    metadata_version = version_line.removeprefix('Version: ')
    filename_version = path.name.removeprefix('gometry-').removesuffix('.tar.gz')
    if not metadata_version or metadata_version != filename_version:
        raise AssertionError(
            'sdist filename/metadata version mismatch: '
            f'{filename_version!r} != {metadata_version!r}'
        )
    pyproject = text_members[f'{root}/pyproject.toml']
    if 'patch.crates-io' in pyproject or 'vendor/' in pyproject:
        raise AssertionError('sdist still configures vendored Cargo sources')
    forbidden_configs = (
        'target.x86_64-',
        'profile.release.',
        'unstable.trim-paths',
    )
    if forbidden := [
        line.strip()
        for line in pyproject.splitlines()
        if any(token in line for token in forbidden_configs)
    ]:
        raise AssertionError(
            f'sdist Maturin config duplicates root release policy: {forbidden}'
        )
    forbidden_parts = {'site', 'dev', '__pycache__'}
    suspect = [
        name
        for name in names
        if forbidden_parts.intersection(Path(name).parts)
        or Path(name).suffix in {'.pyc', '.pyo'}
        or any(
            part == 'target' or part.startswith('target-') for part in Path(name).parts
        )
    ]
    if suspect:
        raise AssertionError(
            f'sdist contains old/generated/internal paths: {suspect[:20]}'
        )
    if leaks := _content_leaks(text_members):
        raise AssertionError(f'sdist text leaks absolute build paths: {leaks[:20]}')
    return metadata_version


def verify_sdist_dependency_sources(path: Path) -> None:
    """Prove an extracted sdist resolves its pinned dependency sources."""
    expected = {
        'geographiclib-rs': 'registry+https://github.com/rust-lang/crates.io-index',
        'h3o': H3_SOURCE,
        'proj-sys': 'registry+https://github.com/rust-lang/crates.io-index',
    }
    with tempfile.TemporaryDirectory(prefix='gometry-sdist-') as tmp:
        extracted = Path(tmp)
        with tarfile.open(path) as archive:
            archive.extractall(extracted, filter='data')
        roots = [item for item in extracted.iterdir() if item.is_dir()]
        if len(roots) != 1:
            raise AssertionError(f'expected one extracted sdist root, found {roots!r}')
        root = roots[0]
        env = os.environ.copy()
        env.pop('RUSTC_WRAPPER', None)
        result = subprocess.run(
            [
                'cargo',
                'metadata',
                '--format-version=1',
                '--manifest-path',
                str(root / 'gometry' / 'Cargo.toml'),
            ],
            cwd=root,
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )
        metadata_packages = json.loads(result.stdout)['packages']
        packages = {package['name']: package for package in metadata_packages}
        for package, source in expected.items():
            if packages[package].get('source') != source:
                raise AssertionError(
                    f'sdist resolved {package} from '
                    f'{packages[package].get("source")!r}, expected {source!r}'
                )
        # Maturin regenerates a one-member workspace around Gometry while
        # copying the monorepo lock. Cargo must prune unrelated workspace
        # packages from that copy, so `metadata --locked` cannot succeed even
        # though dependency resolution is unchanged. Prove that resolution
        # selected no registry or Git tuple outside the archived lock.
        lock = tomllib.loads((root / 'Cargo.lock').read_text())
        locked = {
            (package['name'], package['version'], package.get('source'))
            for package in lock['package']
        }
        resolved = {
            (package['name'], package['version'], package.get('source'))
            for package in metadata_packages
            if package.get('source') is not None
        }
        if unlocked := sorted(resolved - locked):
            raise AssertionError(
                f'sdist resolved packages absent from Cargo.lock: {unlocked}'
            )


def inspect_wheel(path: Path) -> str:
    with zipfile.ZipFile(path) as wheel:
        names = set(wheel.namelist())
        text_members = {
            name: wheel.read(name).decode('utf-8', errors='ignore')
            for name in names
            if _is_text_member(name)
        }
    package = {name for name in names if name.startswith('gometry/')}
    dist_info = {name for name in names if '.dist-info/' in name}
    extension_suffixes = ('.so', '.pyd', '.dylib')
    has_extension = any(
        name.startswith('gometry/_lib') and name.endswith(extension_suffixes)
        for name in package
    )
    required = {'gometry/_lib.pyi', 'gometry/py.typed'}
    missing = sorted(required - names)
    required_licenses = {
        'LICENSE-APACHE.md',
        'LICENSE-MIT.md',
        'LICENSE-THIRD-PARTY.md',
    }
    bundled_licenses = {Path(name).name for name in dist_info if '/licenses/' in name}
    pth_files = [name for name in names if name.endswith('.pth')]
    generated_members = sorted(
        name
        for name in names
        if '__pycache__' in Path(name).parts or name.endswith(('.pyc', '.pyo'))
    )
    if not has_extension:
        raise AssertionError('wheel does not contain gometry/_lib native extension')
    if missing:
        raise AssertionError(f'wheel missing required files: {missing}')
    if missing_licenses := sorted(required_licenses - bundled_licenses):
        raise AssertionError(f'wheel missing dist-info licenses: {missing_licenses}')
    third_party = [
        name for name in dist_info if name.endswith('/licenses/LICENSE-THIRD-PARTY.md')
    ]
    if len(third_party) != 1:
        raise AssertionError(f'expected one third-party license file: {third_party!r}')
    third_party_text = text_members[third_party[0]]
    for component, marker in native_notice_markers():
        if component not in third_party_text or marker not in third_party_text:
            raise AssertionError(
                f'wheel third-party license missing native component {component!r}'
            )
    if generated_members:
        raise AssertionError(
            f'wheel contains generated Python bytecode: {generated_members[:20]}'
        )
    if leaks := _content_leaks(text_members):
        raise AssertionError(f'wheel metadata leaks absolute build paths: {leaks}')
    if pth_files and not package:
        raise AssertionError(f'wheel is .pth-only: {pth_files}')
    metadata_names = [name for name in dist_info if name.endswith('/METADATA')]
    if len(metadata_names) != 1:
        raise AssertionError(f'expected one wheel METADATA file: {metadata_names!r}')
    metadata = text_members[metadata_names[0]]
    version_line = next(
        (line for line in metadata.splitlines() if line.startswith('Version: ')),
        '',
    )
    metadata_version = version_line.removeprefix('Version: ')
    filename_parts = path.name.split('-', 2)
    filename_version = filename_parts[1] if len(filename_parts) == 3 else ''
    if not metadata_version or metadata_version != filename_version:
        raise AssertionError(
            'wheel filename/metadata version mismatch: '
            f'{filename_version!r} != {metadata_version!r}'
        )
    return metadata_version


SMOKE = r"""
import importlib.metadata
import json
import sys

import numpy as np
import gometry as gm

assert gm.__version__ == importlib.metadata.version('gometry')

optional = {'pyarrow', 'pandas', 'polars', 'geopandas', 'lonboard'}
loaded_optional = sorted(optional & set(sys.modules))
assert loaded_optional == [], loaded_optional

point = gm.Point(21.0, 52.0, crs=4326)
other = gm.Point(22.0, 52.0, crs=4326)
projected = point.to_crs(3857)
assert projected.crs == 'EPSG:3857'
assert point.buffer(1000.0).area > 0
assert gm.distance(point, other) > 0

wkb = point.to_wkb(include_srid=True)
assert gm.from_wkb(wkb).crs == 'EPSG:4326'
assert gm.from_wkt(point.to_wkt(), crs=4326).crs == 'EPSG:4326'
geojson = json.loads(point.to_geojson())
assert gm.from_geojson(geojson).to_wkt() == point.to_wkt()

mask = gm.contains_xy(gm.box(20, 51, 22, 53), [21.0, 30.0], [52.0, 52.0])
assert isinstance(mask, np.ndarray)
assert mask.flags.writeable is False
np.testing.assert_array_equal(mask, [True, False])
"""


def smoke_install(wheel: Path) -> None:
    with tempfile.TemporaryDirectory(prefix='gometry-wheel-smoke-') as tmp:
        venv_dir = Path(tmp) / 'venv'
        venv.EnvBuilder(with_pip=True).create(venv_dir)
        python = venv_dir / 'bin' / 'python'
        run([str(python), '-m', 'pip', 'install', str(wheel)])
        run([str(python), '-W', 'error', '-c', textwrap.dedent(SMOKE)])


def inspect_artifact(path: Path) -> None:
    if path.name.endswith('.whl'):
        inspect_wheel(path)
    elif path.name.endswith('.tar.gz'):
        inspect_sdist(path)
        verify_sdist_dependency_sources(path)
    else:
        raise AssertionError(f'unsupported release artifact: {path}')


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        '--inspect',
        nargs='+',
        type=Path,
        metavar='ARTIFACT',
        help='inspect existing wheel/sdist artifacts without rebuilding',
    )
    mode.add_argument(
        '--installed',
        action='store_true',
        help='run the functional smoke against the active installed wheel',
    )
    mode.add_argument(
        '--smoke-wheel',
        type=Path,
        metavar='WHEEL',
        help='inspect, clean-install, and functionally smoke one existing wheel',
    )
    mode.add_argument(
        '--smoke-sdist',
        type=Path,
        metavar='SDIST',
        help='inspect an sdist, rebuild its wheel, and smoke the clean install',
    )
    args = parser.parse_args()
    if args.installed:
        run([sys.executable, '-W', 'error', '-c', textwrap.dedent(SMOKE)])
        print('installed wheel smoke OK')
        return 0
    if args.inspect:
        for path in args.inspect:
            path = path.resolve()
            inspect_artifact(path)
            print(f'artifact inspection OK: {path.name}')
        return 0
    if args.smoke_wheel:
        wheel = args.smoke_wheel.resolve()
        inspect_wheel(wheel)
        smoke_install(wheel)
        print(f'wheel smoke OK: {wheel.name}')
        return 0
    if args.smoke_sdist:
        sdist = args.smoke_sdist.resolve()
        with tempfile.TemporaryDirectory(prefix='gometry-sdist-smoke-') as tmp:
            wheel = build_wheel_from_sdist(
                sdist,
                Path(tmp),
                env=release_build_env(),
            )
            smoke_install(wheel)
        print(f'sdist rebuild smoke OK: {sdist.name}')
        return 0

    with tempfile.TemporaryDirectory(prefix='gometry-wheel-build-') as tmp:
        wheel = build_wheel(Path(tmp))
        inspect_wheel(wheel)
        smoke_install(wheel)
        print(f'wheel smoke OK: {wheel.name}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
