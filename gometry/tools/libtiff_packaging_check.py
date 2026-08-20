#!/usr/bin/env python3
"""Exercise initialized and deinitialized libtiff packaging paths."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CRATE = ROOT / 'native' / 'libtiff-sys'
LIBTIFF = CRATE / 'libtiff'
PIN = CRATE / 'libtiff.pin'
COMMIT = 'd01a94be176f5f6a87f7ee1c0b32e65416aa2b4d'
HINT = 'git submodule update --init gometry/native/libtiff-sys/libtiff'


def run(command: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=cwd, check=False, text=True, capture_output=True)


def initialized_check() -> None:
    assert LIBTIFF.joinpath('CMakeLists.txt').is_file(), HINT
    assert LIBTIFF.joinpath('VERSION').read_text().strip() == '4.7.2'
    assert 'add_subdirectory(build)' in LIBTIFF.joinpath('CMakeLists.txt').read_text()
    assert PIN.read_text() == (
        'url = https://gitlab.com/libtiff/libtiff.git\n'
        f'commit = {COMMIT}\n'
        'tag = v4.7.2\n'
        'version = 4.7.2\n'
    )
    source = source_members(LIBTIFF)
    parent_pin = run(
        ['git', 'ls-tree', 'HEAD', 'gometry/native/libtiff-sys/libtiff'], cwd=ROOT.parent
    )
    assert COMMIT in run(['git', '-C', str(LIBTIFF), 'rev-parse', 'HEAD'], cwd=ROOT).stdout
    assert f'160000 commit {COMMIT}\t' in parent_pin.stdout
    assert_clean_submodule(LIBTIFF)

    package = run(
        ['cargo', 'package', '--list', '--allow-dirty', '--manifest-path', 'Cargo.toml'],
        cwd=CRATE,
    )
    assert package.returncode == 0, package.stderr
    packaged = package_members(package.stdout.splitlines())
    assert packaged == source, (
        f'Cargo libtiff file set differs: missing={sorted(source - packaged)[:20]}, '
        f'extra={sorted(packaged - source)[:20]}'
    )


def negative_check() -> None:
    with tempfile.TemporaryDirectory(prefix='gometry-libtiff-negative-') as temporary:
        copy = Path(temporary) / 'libtiff-sys'
        shutil.copytree(CRATE, copy, ignore=shutil.ignore_patterns('target'))
        shutil.rmtree(copy / 'libtiff')

        package = run(
            ['cargo', 'package', '--list', '--allow-dirty', '--manifest-path', 'Cargo.toml'],
            cwd=copy,
        )
        assert package.returncode == 0, package.stderr
        packaged = package_members(package.stdout.splitlines())
        assert not packaged, f'deinitialized package unexpectedly contains {packaged}'

        build = run(['cargo', 'check', '--offline'], cwd=copy)
        output = build.stdout + build.stderr
        assert build.returncode != 0
        assert 'libtiff source is not initialized' in output
        assert HINT in output


def source_members(path: Path) -> set[str]:
    return {
        member.relative_to(path).as_posix()
        for member in path.rglob('*')
        if member.is_file() and '.git' not in member.parts
    }


def package_members(lines: list[str]) -> set[str]:
    members = [line.removeprefix('libtiff/') for line in lines if line.startswith('libtiff/')]
    assert len(members) == len(set(members)), 'Cargo package list contains duplicate libtiff paths'
    for member in members:
        path = Path(member)
        assert member and not path.is_absolute() and '..' not in path.parts
    return set(members)


def assert_clean_submodule(path: Path) -> None:
    for diff_args in (('diff', '--exit-code'), ('diff', '--cached', '--exit-code')):
        result = run(['git', '-C', str(path), *diff_args], cwd=ROOT)
        assert result.returncode == 0, result.stdout + result.stderr
    status = run(
        ['git', '-C', str(path), 'status', '--porcelain', '--untracked-files=all'], cwd=ROOT
    )
    assert status.returncode == 0, status.stderr
    assert not status.stdout, status.stdout


def dirty_check() -> None:
    with tempfile.TemporaryDirectory(prefix='gometry-libtiff-dirty-') as temporary:
        crate = Path(temporary) / 'libtiff-sys'
        shutil.copytree(CRATE, crate, ignore=shutil.ignore_patterns('target', 'libtiff'))
        copy = crate / 'libtiff'
        clone = run(['git', 'clone', '--no-local', str(LIBTIFF), str(copy)], cwd=ROOT)
        assert clone.returncode == 0, clone.stderr
        assert_clean_submodule(copy)

        source = copy / 'CMakeLists.txt'
        original = source.read_bytes()
        source.write_bytes(original + b'\n# dirty test\n')
        expect_dirty_rejected(copy, 'tracked libtiff mutation')
        build = run(['cargo', 'check', '--offline'], cwd=crate)
        assert build.returncode != 0
        assert 'tracked changes' in build.stdout + build.stderr
        source.write_bytes(original)

        source.write_bytes(original + b'\n# staged dirty test\n')
        staged = run(['git', '-C', str(copy), 'add', 'CMakeLists.txt'], cwd=ROOT)
        assert staged.returncode == 0, staged.stderr
        cached = run(
            ['git', '-C', str(copy), 'diff', '--cached', '--exit-code'], cwd=ROOT
        )
        assert cached.returncode != 0, 'staged mutation did not enter git diff --cached'
        expect_dirty_rejected(copy, 'staged libtiff mutation')
        build = run(['cargo', 'check', '--offline'], cwd=crate)
        assert build.returncode != 0
        assert 'tracked changes' in build.stdout + build.stderr
        restored = run(['git', '-C', str(copy), 'reset', '--hard', 'HEAD'], cwd=ROOT)
        assert restored.returncode == 0, restored.stderr
        assert_clean_submodule(copy)

        (copy / 'dirty-test.txt').write_text('untracked\n')
        expect_dirty_rejected(copy, 'untracked libtiff file')
        build = run(['cargo', 'check', '--offline'], cwd=crate)
        assert build.returncode != 0
        assert 'untracked files' in build.stdout + build.stderr
        (copy / 'dirty-test.txt').unlink()
        assert_clean_submodule(copy)


def expect_dirty_rejected(path: Path, label: str) -> None:
    try:
        assert_clean_submodule(path)
    except AssertionError:
        return
    raise AssertionError(f'{label} was not rejected')


def sdist_check(path: Path) -> None:
    sys.path.insert(0, str(ROOT / 'tools'))
    from wheel_smoke import inspect_libtiff_sdist

    inspect_libtiff_sdist(path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('sdist', type=Path, nargs='?')
    args = parser.parse_args()
    initialized_check()
    negative_check()
    dirty_check()
    if args.sdist is not None:
        sdist_check(args.sdist)
    print('libtiff initialized and negative packaging checks OK')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
