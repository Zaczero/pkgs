from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

SCRIPT_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = SCRIPT_DIR.parents[1]
original_path = sys.path[:]
try:
    sys.path.insert(0, str(SCRIPT_DIR))
    matrix = importlib.import_module('matrix')
finally:
    sys.path[:] = original_path


def catalog(*, linux_ft: tuple[str, ...] = ('3.14', '3.15')) -> list[dict[str, str]]:
    entries = []
    versions = ('3.11', '3.12', '3.13', '3.14', '3.15')
    entries.extend(
        {
            'implementation': 'cpython',
            'version': f'{version}.0',
            'variant': 'default',
            'os': os_name,
        }
        for version in versions
        for os_name in ('linux', 'macos', 'windows')
    )
    entries.extend(
        {
            'implementation': 'cpython',
            'version': f'{version}.0',
            'variant': 'freethreaded',
            'os': 'linux',
        }
        for version in linux_ft
    )
    entries.extend(
        {
            'implementation': 'cpython',
            'version': '3.15.0',
            'variant': 'freethreaded',
            'os': os_name,
        }
        for os_name in ('macos', 'windows')
    )
    return entries


def cell(
    runner: str,
    version: str,
    *,
    freethreaded: bool = False,
    primary: bool = False,
    group: str = 'dev',
) -> dict[str, str | bool]:
    return {
        'os': runner,
        'python-version': version,
        'freethreaded': freethreaded,
        'primary': primary,
        'dependency-group': group,
    }


@pytest.fixture(autouse=True)
def repository_cwd(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(REPO_ROOT)
    monkeypatch.setattr(
        matrix, 'rust_toolchain_channel', lambda _package: 'nightly-test'
    )


def test_pure_python_plan_tests_latest_free_threaded_cpython_on_linux() -> None:
    plan = matrix.resolve_matrix('sizestr', catalog=catalog())

    assert plan['test-matrix'] == [
        *[
            cell(matrix.LINUX_TEST_RUNNER, f'3.{minor}', primary=minor == 15)
            for minor in range(11, 16)
        ],
        cell(matrix.MACOS_TEST_RUNNER, '3.15'),
        cell(matrix.WINDOWS_TEST_RUNNER, '3.15'),
        cell(
            matrix.LINUX_TEST_RUNNER,
            '3.15t',
            freethreaded=True,
        ),
    ]
    assert plan['build-matrix'] == [
        {'runner': matrix.LINUX_TEST_RUNNER, 'abi': 'optimized'}
    ]


def test_native_plan_keeps_every_free_threaded_abi_boundary() -> None:
    plan = matrix.resolve_matrix('h2corn', catalog=catalog())

    assert plan['test-matrix'] == [
        *[
            cell(matrix.LINUX_TEST_RUNNER, f'3.{minor}', primary=minor == 15)
            for minor in range(11, 16)
        ],
        cell(matrix.MACOS_TEST_RUNNER, '3.15'),
        cell(matrix.WINDOWS_TEST_RUNNER, '3.15'),
        cell('ubuntu-26.04-arm', '3.15'),
        cell(
            matrix.LINUX_TEST_RUNNER,
            '3.14t',
            freethreaded=True,
            group='freethreaded',
        ),
        cell(
            matrix.LINUX_TEST_RUNNER,
            '3.15t',
            freethreaded=True,
            group='freethreaded',
        ),
        cell(
            matrix.MACOS_TEST_RUNNER,
            '3.15t',
            freethreaded=True,
            group='freethreaded',
        ),
        cell(
            matrix.WINDOWS_TEST_RUNNER,
            '3.15t',
            freethreaded=True,
            group='freethreaded',
        ),
    ]


@pytest.mark.parametrize(
    'package',
    [
        'starlette-compress',
        'httpx-secure',
        'jinja2-htmlmin',
        'sizestr',
        'githead',
        'benchroom',
    ],
)
def test_each_pure_python_plan_has_one_linux_free_threaded_cell(
    package: str,
) -> None:
    plan = matrix.resolve_matrix(package, catalog=catalog())

    assert [
        candidate
        for candidate in plan['test-matrix']
        if candidate['freethreaded'] is True
    ] == [
        cell(
            matrix.LINUX_TEST_RUNNER,
            '3.15t',
            freethreaded=True,
        )
    ]


@pytest.mark.parametrize(
    ('package', 'reason'),
    sorted(matrix.FREETHREADED_TEST_EXCLUSIONS.items()),
)
def test_pure_python_free_threaded_exclusions_name_the_blocker(
    package: str,
    reason: str,
) -> None:
    plan = matrix.resolve_matrix(package, catalog=catalog())

    assert plan['freethreaded-test-exclusion'] == reason
    assert not [cell for cell in plan['test-matrix'] if cell['freethreaded'] is True]


def test_pure_python_plan_fails_if_latest_free_threaded_runtime_is_unavailable() -> (
    None
):
    with pytest.raises(
        SystemExit,
        match=r'uv does not offer declared free-threaded CPython versions 3.15t',
    ):
        matrix.resolve_matrix(
            'sizestr',
            catalog=catalog(linux_ft=('3.14',)),
        )
