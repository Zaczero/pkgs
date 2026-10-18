#!/usr/bin/env python3
"""Resolve monorepo CI test/build matrix from package metadata and uv.

Policy (internal monorepo defaults — keep cost proportional to risk):

* Linux tests every supported stable CPython minor (GIL).
* macOS and Windows test the newest supported stable minor only (GIL).
* Free-threaded pytest: native packages, every shippable ABI on Linux and
  the newest supported minor on macOS and Windows.
* PyPy pytest: one Linux cell when the package advertises PyPy (classifier),
  using the newest PyPy minor uv can install that satisfies requires-python.
* Build runners: pure Python → ubuntu only; native → multi-arch including
  Windows ARM.
* Development dependencies are installed for every cell. Packages whose native
  tooling lacks free-threaded builds supply a compatible ``freethreaded`` group.

Usage (from repo root):

    python3 .github/scripts/route_packages.py < changed-paths.txt |
        python3 .github/scripts/matrix.py
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tomllib
import urllib.request
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

from package_version import package_info, write_outputs

VERSION_RE = re.compile(
    r"""
    ^
    (?P<major>\d+)\.(?P<minor>\d+)
    (?:
        \.(?P<micro>\d+)
        (?P<pre>[a-zA-Z]+\d*)?
    )?
    $
    """,
    re.VERBOSE,
)

LINUX_TEST_RUNNER = 'ubuntu-26.04'
MACOS_TEST_RUNNER = 'macos-26'
WINDOWS_TEST_RUNNER = 'windows-2025'
OTHER_TEST_RUNNERS = {'macos': MACOS_TEST_RUNNER, 'windows': WINDOWS_TEST_RUNNER}

# Free-threaded wheels in cibuildwheel 4 start at CPython 3.14 (3.13t removed).
MIN_FREETHREADED_MINOR = (3, 14)

NATIVE_BUILD_RUNNERS = [
    LINUX_TEST_RUNNER,
    'ubuntu-26.04-arm',
    WINDOWS_TEST_RUNNER,
    'windows-11-vs2026-arm',
    'macos-26-intel',
    MACOS_TEST_RUNNER,
]
PURE_BUILD_RUNNERS = [LINUX_TEST_RUNNER]


@dataclass(frozen=True, slots=True, order=True)
class Minor:
    major: int
    minor: int

    def __str__(self) -> str:
        return f'{self.major}.{self.minor}'

    @property
    def freethreaded_selector(self) -> str:
        return f'{self.major}.{self.minor}t'

    @property
    def as_tuple(self) -> tuple[int, int]:
        return (self.major, self.minor)


@dataclass(frozen=True, slots=True, order=True)
class ParsedVersion:
    major: int
    minor: int
    micro: int
    pre: str | None

    @property
    def as_minor(self) -> Minor:
        return Minor(self.major, self.minor)

    @property
    def is_prerelease(self) -> bool:
        return self.pre is not None


def fail(message: str) -> None:
    raise SystemExit(message)


def parse_version(text: str) -> ParsedVersion | None:
    match = VERSION_RE.fullmatch(text.strip())
    if match is None:
        return None
    return ParsedVersion(
        major=int(match.group('major')),
        minor=int(match.group('minor')),
        micro=int(match.group('micro') or 0),
        pre=match.group('pre'),
    )


def parse_minor(text: str) -> Minor | None:
    parsed = parse_version(text)
    if parsed is None:
        return None
    return parsed.as_minor


def read_declared_minors(path: Path) -> list[Minor]:
    minors: list[Minor] = []
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith('#'):
            continue
        minor = parse_minor(line)
        if minor is None:
            fail(f'invalid Python version {line!r} in {path}')
        minors.append(minor)
    if not minors:
        fail(f'no Python versions declared in {path}')
    return minors


def requires_python_floor(requires_python: str) -> Minor | None:
    match = re.fullmatch(r'>=\s*(\d+)\.(\d+)', requires_python.strip())
    if match is None:
        return None
    return Minor(int(match.group(1)), int(match.group(2)))


def satisfies_requires_python(requires_python: str, minor: Minor) -> bool:
    floor = requires_python_floor(requires_python)
    if floor is None:
        return True
    return minor >= floor


def load_uv_catalog(catalog_path: Path | None = None) -> list[dict[str, Any]]:
    if catalog_path is not None:
        return json.loads(catalog_path.read_text())

    result = subprocess.run(
        [
            'uv',
            'python',
            'list',
            '--all-platforms',
            '--only-downloads',
            '--output-format',
            'json',
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        fail(f'uv python list failed:\n{result.stderr.strip()}')
    return json.loads(result.stdout)


def cpython_entries(catalog: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        entry
        for entry in catalog
        if entry.get('implementation') == 'cpython'
        and isinstance(entry.get('version'), str)
        and isinstance(entry.get('os'), str)
    ]


def entry_version(entry: dict[str, Any]) -> ParsedVersion | None:
    return parse_version(str(entry['version']))


def is_freethreaded(entry: dict[str, Any]) -> bool:
    return entry.get('variant') == 'freethreaded'


def is_default_variant(entry: dict[str, Any]) -> bool:
    variant = entry.get('variant')
    return variant in (None, '', 'default')


def available_stable_cpython_minors(
    catalog: Sequence[dict[str, Any]],
    *,
    freethreaded: bool,
    os_name: str | None = None,
) -> set[Minor]:
    minors: set[Minor] = set()
    for entry in cpython_entries(catalog):
        if freethreaded != is_freethreaded(entry):
            continue
        if not freethreaded and not is_default_variant(entry):
            continue
        if os_name is not None and entry.get('os') != os_name:
            continue
        parsed = entry_version(entry)
        if parsed is None or parsed.is_prerelease:
            continue
        minors.add(parsed.as_minor)
    return minors


def newest_pypy_selector(
    catalog: Sequence[dict[str, Any]],
    *,
    requires_python: str,
    os_name: str = 'linux',
) -> str | None:
    """Newest PyPy minor uv offers on *os_name* that satisfies requires-python."""
    candidates: list[tuple[Minor, str]] = []
    for entry in catalog:
        if entry.get('implementation') != 'pypy':
            continue
        if entry.get('os') != os_name:
            continue
        if not isinstance(entry.get('version'), str):
            continue
        parsed = parse_version(str(entry['version']))
        if parsed is None or parsed.is_prerelease:
            continue
        minor = parsed.as_minor
        if not satisfies_requires_python(requires_python, minor):
            continue
        # uv / setup-uv accept pypy3.N selectors.
        candidates.append((minor, f'pypy3.{minor.minor}'))
    if not candidates:
        return None
    candidates.sort()
    return candidates[-1][1]


def versions_file_for(package: str) -> Path:
    package_file = Path(package) / '.python-versions'
    if package_file.is_file():
        return package_file
    root_file = Path('.python-versions')
    if root_file.is_file():
        return root_file
    fail(f'no .python-versions found for package {package!r}')


@cache
def resolve_rust_channel(channel: str) -> str:
    if channel != 'nightly':
        return channel
    with urllib.request.urlopen(
        'https://static.rust-lang.org/dist/channel-rust-nightly.toml', timeout=30
    ) as response:
        manifest = tomllib.loads(response.read().decode())
    return f'nightly-{manifest["date"]}'


def rust_toolchain_channel(package: str) -> str:
    package_file = Path(package) / 'rust-toolchain.toml'
    path = package_file if package_file.is_file() else Path('rust-toolchain.toml')
    channel = tomllib.loads(path.read_text())['toolchain']['channel']
    return resolve_rust_channel(channel)


def build_test_matrix(
    *,
    stable_minors: Sequence[Minor],
    primary: Minor,
    has_rust: bool,
    freethreaded_linux: Sequence[Minor],
    pypy_selector: str | None,
    freethreaded_group: str,
) -> list[dict[str, str | bool]]:
    matrix: list[dict[str, str | bool]] = [
        {
            'os': LINUX_TEST_RUNNER,
            'python-version': str(minor),
            'freethreaded': False,
            'primary': minor == primary,
            'dependency-group': 'dev',
        }
        for minor in stable_minors
    ]
    matrix.extend(
        {
            'os': runner,
            'python-version': str(primary),
            'freethreaded': False,
            'primary': False,
            'dependency-group': 'dev',
        }
        for runner in (
            *OTHER_TEST_RUNNERS.values(),
            *(['ubuntu-26.04-arm'] if has_rust else []),
        )
    )
    if has_rust:
        matrix.extend(
            {
                'os': LINUX_TEST_RUNNER,
                'python-version': minor.freethreaded_selector,
                'freethreaded': True,
                'primary': False,
                'dependency-group': freethreaded_group,
            }
            for minor in freethreaded_linux
            if minor.as_tuple >= MIN_FREETHREADED_MINOR
        )
        if primary in freethreaded_linux:
            matrix.extend(
                {
                    'os': runner,
                    'python-version': primary.freethreaded_selector,
                    'freethreaded': True,
                    'primary': False,
                    'dependency-group': freethreaded_group,
                }
                for runner in OTHER_TEST_RUNNERS.values()
            )
    if pypy_selector is not None:
        matrix.append({
            'os': LINUX_TEST_RUNNER,
            'python-version': pypy_selector,
            'freethreaded': False,
            'primary': False,
            'dependency-group': 'dev',
        })
    return matrix


def resolve_matrix(
    package: str,
    *,
    catalog: Sequence[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    info = package_info(package)
    if info['type'] == 'rust':
        return {
            'type': 'rust',
            'crate': info['crate'],
            'rust-test-runners': [LINUX_TEST_RUNNER, 'ubuntu-26.04-arm'],
            'rust-toolchain': rust_toolchain_channel(package),
        }
    catalog_entries = list(catalog) if catalog is not None else load_uv_catalog()

    declared = read_declared_minors(versions_file_for(package))
    requires_python = info.get('requires-python', '')

    stable_available = available_stable_cpython_minors(
        catalog_entries, freethreaded=False
    )
    missing = [
        minor
        for minor in declared
        if satisfies_requires_python(requires_python, minor)
        and minor not in stable_available
    ]
    if missing:
        fail(
            f'uv does not offer declared CPython versions '
            f'{", ".join(map(str, missing))}; update uv'
        )
    stable_minors = [
        minor
        for minor in declared
        if satisfies_requires_python(requires_python, minor)
        and minor in stable_available
    ]
    if not stable_minors:
        fail(
            f'no supported stable CPython minors for {package!r} '
            f'(requires-python={requires_python!r})'
        )
    primary = max(stable_minors)

    has_rust = bool(info.get('crate'))
    pypy = info['pypy']

    freethreaded_linux: list[Minor] = []
    if has_rust:
        ft_linux = available_stable_cpython_minors(
            catalog_entries, freethreaded=True, os_name='linux'
        )
        freethreaded_linux = [
            minor for minor in stable_minors if minor.as_tuple >= MIN_FREETHREADED_MINOR
        ]
        missing_ft = [minor for minor in freethreaded_linux if minor not in ft_linux]
        if missing_ft:
            fail(
                f'uv does not offer declared free-threaded CPython versions '
                f'{", ".join(minor.freethreaded_selector for minor in missing_ft)}; update uv'
            )
        if primary in freethreaded_linux:
            for os_name in OTHER_TEST_RUNNERS:
                if primary not in available_stable_cpython_minors(
                    catalog_entries, freethreaded=True, os_name=os_name
                ):
                    fail(
                        f'uv does not offer declared free-threaded CPython '
                        f'{primary.freethreaded_selector} on {os_name}; update uv'
                    )

    pypy_selector = (
        newest_pypy_selector(catalog_entries, requires_python=requires_python)
        if pypy
        else None
    )
    if pypy and pypy_selector is None:
        fail(
            f'package {package!r} advertises PyPy but no matching PyPy download '
            f'was found for requires-python={requires_python!r}'
        )

    test_matrix = build_test_matrix(
        stable_minors=stable_minors,
        primary=primary,
        has_rust=has_rust,
        freethreaded_linux=freethreaded_linux,
        pypy_selector=pypy_selector,
        freethreaded_group=info['freethreaded-group'],
    )
    build_matrix = [
        {'runner': runner, 'abi': abi}
        for runner in (NATIVE_BUILD_RUNNERS if has_rust else PURE_BUILD_RUNNERS)
        for abi in (('optimized', 'abi3t') if has_rust else ('optimized',))
    ]

    return {
        'type': info['type'],
        'import-name': info['import-name'],
        'publishable': info['publishable'],
        'pypy': info['pypy'],
        'crate': info.get('crate', ''),
        'python-versions': [str(m) for m in stable_minors],
        'primary-python': str(primary),
        'test-matrix': test_matrix,
        'build-matrix': build_matrix,
        'rust-toolchain': rust_toolchain_channel(package) if has_rust else '',
    }


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--catalog',
        type=Path,
        default=None,
        help='Optional uv python list JSON (for offline tests)',
    )
    args = parser.parse_args(argv)
    selection = json.load(sys.stdin)
    catalog = load_uv_catalog(args.catalog)
    plans = [
        {**item, 'plan': resolve_matrix(item['package'], catalog=catalog)}
        for item in selection
    ]
    primary = max(available_stable_cpython_minors(catalog, freethreaded=False))
    write_outputs({
        'plans': json.dumps(plans, separators=(',', ':')),
        'primary-python': str(primary),
        'rust-toolchain': rust_toolchain_channel('.'),
    })


if __name__ == '__main__':
    main()
