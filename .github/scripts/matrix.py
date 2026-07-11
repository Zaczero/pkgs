#!/usr/bin/env python3
"""Resolve monorepo CI test/build matrix from package metadata and uv.

Policy (internal monorepo defaults — keep cost proportional to risk):

* Linux tests every supported stable CPython minor (GIL).
* macOS and Windows test the newest supported stable minor only (GIL).
* Free-threaded pytest: rusty packages only, Linux only, shippable ABIs
  (CPython 3.14t+) — matches cibuildwheel 4 free-threaded wheel set.
* PyPy pytest: one Linux cell when the package advertises PyPy (classifier),
  using the newest PyPy minor uv can install that satisfies requires-python.
* Build runners: pure → ubuntu only; rusty → native multi-arch including
  Windows ARM.

Usage (from repo root):

    python3 .github/scripts/matrix.py <package>
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

sys.path.insert(0, str(Path(__file__).resolve().parent))
from package_version import package_info

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

LINUX_TEST_RUNNER = 'ubuntu-latest'
MACOS_TEST_RUNNER = 'macos-15'
WINDOWS_TEST_RUNNER = 'windows-latest'

# Free-threaded wheels in cibuildwheel 4 start at CPython 3.14 (3.13t removed).
MIN_FREETHREADED_MINOR = (3, 14)

RUSTY_BUILD_RUNNERS = [
    'ubuntu-latest',
    'ubuntu-24.04-arm',
    'windows-latest',
    'windows-11-arm',
    'macos-15-intel',
    'macos-15',
]
PURE_BUILD_RUNNERS = ['ubuntu-latest']


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


def rust_toolchain_channel(package: str) -> str:
    package_file = Path(package) / 'rust-toolchain.toml'
    path = package_file if package_file.is_file() else Path('rust-toolchain.toml')
    text = path.read_text()
    match = re.search(r'(?m)^channel\s*=\s*"([^"]+)"\s*$', text)
    if match is None:
        fail(f'could not read toolchain channel from {path}')
    return match.group(1)


def build_test_matrix(
    *,
    stable_minors: Sequence[Minor],
    primary: Minor,
    is_rusty: bool,
    freethreaded_linux: Sequence[Minor],
    pypy_selector: str | None,
) -> list[dict[str, str]]:
    matrix: list[dict[str, str]] = [
        {
            'os': LINUX_TEST_RUNNER,
            'python-version': str(minor),
            'freethreaded': 'false',
            'pypy': 'false',
        }
        for minor in stable_minors
    ]
    matrix.extend(
        {
            'os': runner,
            'python-version': str(primary),
            'freethreaded': 'false',
            'pypy': 'false',
        }
        for runner in (MACOS_TEST_RUNNER, WINDOWS_TEST_RUNNER)
    )
    if is_rusty:
        matrix.extend(
            {
                'os': LINUX_TEST_RUNNER,
                'python-version': minor.freethreaded_selector,
                'freethreaded': 'true',
                'pypy': 'false',
            }
            for minor in freethreaded_linux
            if minor.as_tuple >= MIN_FREETHREADED_MINOR
        )
    if pypy_selector is not None:
        matrix.append({
            'os': LINUX_TEST_RUNNER,
            'python-version': pypy_selector,
            'freethreaded': 'false',
            'pypy': 'true',
        })
    return matrix


def resolve_matrix(
    package: str,
    *,
    catalog: Sequence[dict[str, Any]] | None = None,
) -> dict[str, str]:
    info = package_info(package)
    catalog_entries = list(catalog) if catalog is not None else load_uv_catalog()

    declared = read_declared_minors(versions_file_for(package))
    requires_python = info.get('requires-python', '')

    stable_available = available_stable_cpython_minors(
        catalog_entries, freethreaded=False
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

    is_rusty = info['is-rusty'] == 'true'
    pypy = info['pypy'] == 'true'

    freethreaded_linux: list[Minor] = []
    if is_rusty:
        ft_linux = available_stable_cpython_minors(
            catalog_entries, freethreaded=True, os_name='linux'
        )
        freethreaded_linux = [
            minor
            for minor in stable_minors
            if minor in ft_linux and minor.as_tuple >= MIN_FREETHREADED_MINOR
        ]

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
        is_rusty=is_rusty,
        freethreaded_linux=freethreaded_linux,
        pypy_selector=pypy_selector,
    )

    build_runners = RUSTY_BUILD_RUNNERS if is_rusty else PURE_BUILD_RUNNERS

    return {
        'import-name': info['import-name'],
        'is-rusty': info['is-rusty'],
        'pypy': info['pypy'],
        'package': info['package'],
        'requires-python': requires_python,
        'primary-python-version': str(primary),
        'python-versions': json.dumps([str(m) for m in stable_minors]),
        'test-matrix': json.dumps(test_matrix, separators=(',', ':')),
        'build-runners': json.dumps(build_runners, separators=(',', ':')),
        'rust-toolchain': rust_toolchain_channel(package) if is_rusty else '',
    }


def write_outputs(outputs: dict[str, str]) -> None:
    for key, value in outputs.items():
        print(f'{key}={value}')

    output_path = os.environ.get('GITHUB_OUTPUT')
    if output_path is not None:
        with Path(output_path).open('a') as file:
            for key, value in outputs.items():
                if '\n' in value:
                    file.write(f'{key}<<EOF\n{value}\nEOF\n')
                else:
                    file.write(f'{key}={value}\n')


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('package', help='Top-level package directory name')
    parser.add_argument(
        '--catalog',
        type=Path,
        default=None,
        help='Optional uv python list JSON (for offline tests)',
    )
    args = parser.parse_args(argv)
    catalog = load_uv_catalog(args.catalog) if args.catalog else None
    write_outputs(resolve_matrix(args.package, catalog=catalog))


if __name__ == '__main__':
    main()
