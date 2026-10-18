#!/usr/bin/env python3
"""Exercise initialized and deinitialized libtiff source packaging paths."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO = PACKAGE_ROOT.parent
CRATE = REPO / 'native' / 'libtiff-sys'
HELPER = REPO / 'native' / 'cmake-flags'
PROVIDER = REPO / 'vendor' / 'libtiff'
LIBTIFF = PROVIDER / 'source'
COMMIT = 'd01a94be176f5f6a87f7ee1c0b32e65416aa2b4d'
HINT = 'git submodule update --init vendor/libtiff/source'
sys.path.insert(0, str(PACKAGE_ROOT / 'tools'))
import source_artifact


def run(
    command: list[str], *, cwd: Path, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=cwd,
        env=env,
        check=False,
        text=True,
        capture_output=True,
    )


def initialized_check() -> None:
    parent_pin = run(
        ['git', 'ls-files', '-s', '--', 'vendor/libtiff/source'],
        cwd=REPO,
    )
    assert parent_pin.returncode == 0, parent_pin.stderr
    assert (
        COMMIT in run(['git', '-C', str(LIBTIFF), 'rev-parse', 'HEAD'], cwd=REPO).stdout
    )
    assert f'160000 {COMMIT} 0\tvendor/libtiff/source' in parent_pin.stdout
    verify_checkout_for(REPO, LIBTIFF)


def workspace_copy(destination: Path) -> Path:
    root = destination / 'workspace'
    root.mkdir(parents=True)
    (root / 'Cargo.toml').write_text(
        '[workspace]\n'
        'members = ["native/cmake-flags", "native/libtiff-sys"]\n'
        'resolver = "3"\n\n'
        '[workspace.package]\n'
        'edition = "2024"\n'
        'publish = false\n\n'
        '[workspace.lints.rust]\n'
        'unused_crate_dependencies = "warn"\n'
    )
    (root / 'native').mkdir()
    shutil.copytree(HELPER, root / 'native' / 'cmake-flags')
    shutil.copytree(CRATE, root / 'native' / 'libtiff-sys')
    shutil.copytree(PROVIDER, root / 'vendor' / 'libtiff')
    shutil.rmtree(root / 'vendor' / 'libtiff' / 'source')
    return root


def cargo_check(root: Path, target: Path) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment['CARGO_NET_OFFLINE'] = 'true'
    environment['CARGO_TARGET_DIR'] = str(target)
    return run(
        [
            'cargo',
            'check',
            '--offline',
            '--manifest-path',
            str(root / 'native' / 'libtiff-sys' / 'Cargo.toml'),
        ],
        cwd=root,
        env=environment,
    )


def negative_check() -> None:
    with tempfile.TemporaryDirectory(prefix='gometry-libtiff-negative-') as temporary:
        base = Path(temporary)
        root = workspace_copy(base)
        build = cargo_check(root, base / 'target')
        output = build.stdout + build.stderr
        assert build.returncode != 0
        assert 'libtiff source is not initialized' in output
        assert HINT in output


def dirty_check() -> None:
    with tempfile.TemporaryDirectory(prefix='gometry-libtiff-dirty-') as temporary:
        base = Path(temporary)
        root = workspace_copy(base)
        copy = root / 'vendor' / 'libtiff' / 'source'
        clone = run(['git', 'clone', '--no-local', str(LIBTIFF), str(copy)], cwd=REPO)
        assert clone.returncode == 0, clone.stderr
        verify_checkout_for(root, copy)

        source = copy / 'CMakeLists.txt'
        original = source.read_bytes()
        source.write_bytes(original + b'\n# dirty test\n')
        expect_dirty_rejected(root, copy, 'tracked libtiff mutation')
        source.write_bytes(original)

        source.write_bytes(original + b'\n# staged dirty test\n')
        staged = run(['git', '-C', str(copy), 'add', 'CMakeLists.txt'], cwd=REPO)
        assert staged.returncode == 0, staged.stderr
        cached = run(
            ['git', '-C', str(copy), 'diff', '--cached', '--exit-code'], cwd=REPO
        )
        assert cached.returncode != 0, 'staged mutation did not enter git diff --cached'
        expect_dirty_rejected(root, copy, 'staged libtiff mutation')
        restored = run(['git', '-C', str(copy), 'reset', '--hard', 'HEAD'], cwd=REPO)
        assert restored.returncode == 0, restored.stderr
        verify_checkout_for(root, copy)

        (copy / 'dirty-test.txt').write_text('untracked\n')
        expect_dirty_rejected(root, copy, 'untracked libtiff file')
        (copy / 'dirty-test.txt').unlink()
        verify_checkout_for(root, copy)


def verify_checkout_for(root: Path, path: Path) -> None:
    saved = (
        source_artifact.ROOT,
        source_artifact.LIBTIFF,
        source_artifact.LIBTIFF_PIN,
        source_artifact.LIBTIFF_COMMIT,
        source_artifact.LIBTIFF_VERSION,
    )
    try:
        source_artifact.ROOT = root / 'gometry'
        source_artifact.LIBTIFF = path
        source_artifact.LIBTIFF_PIN = path.parent / 'libtiff.pin'
        source_artifact.LIBTIFF_COMMIT = COMMIT
        source_artifact.LIBTIFF_VERSION = '4.7.2'
        source_artifact.verify_libtiff_checkout()
    finally:
        (
            source_artifact.ROOT,
            source_artifact.LIBTIFF,
            source_artifact.LIBTIFF_PIN,
            source_artifact.LIBTIFF_COMMIT,
            source_artifact.LIBTIFF_VERSION,
        ) = saved


def expect_dirty_rejected(root: Path, path: Path, label: str) -> None:
    try:
        verify_checkout_for(root, path)
    except AssertionError:
        return
    raise AssertionError(f'{label} was not rejected')


def sdist_check(path: Path) -> None:
    from artifact_content import inspect_libtiff_sdist

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
    print('libtiff source initialization and dirty-source checks OK')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
