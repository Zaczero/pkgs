"""Build and smoke-test the release wheel artifact.

This is intentionally heavier than the default pytest lane. There is no
``pytest -m release`` marker — run this script directly before publishing::

    .venv/bin/python tools/wheel_smoke.py
    .venv/bin/python tools/wheel_smoke.py --inspect path/to/gometry-*.whl
    .venv/bin/python tools/wheel_smoke.py --installed
    .venv/bin/python tools/wheel_smoke.py --smoke-sdist path/to/gometry-*.tar.gz
    .venv/bin/python tools/wheel_smoke.py --stage-sdist dist

Default mode builds an sdist via maturin, rebuilds the wheel from that sdist,
clean-installs it into a temporary venv, and runs a functional import/API smoke.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

from _artifact_common import RELEASE_MANIFEST_NAME, release_build_env
from artifact_content import (
    inspect_libtiff_sdist,
    inspect_wheel,
    libtiff_source_members,
)
from release_artifact import (
    assemble_release,
    build_wheel,
    build_wheel_from_sdist,
    record_build,
    record_repaired_wheel,
    smoke_install,
    smoke_installed,
    validate_release_manifest,
    write_runner_receipt,
)
from runtime_artifact import write_native_test_receipt
from source_artifact import (
    inspect_sdist,
    stage_sdist,
    verify_sdist_dependency_sources,
    verify_source_artifact,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


def _inspect_artifact(path: Path) -> None:
    if path.name.endswith('.whl'):
        inspect_wheel(path)
    elif path.name.endswith('.tar.gz'):
        inspect_sdist(path)
        verify_sdist_dependency_sources(path)
    else:
        raise AssertionError(f'unsupported release artifact: {path}')


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        allow_abbrev=False,
        **({'color': False} if sys.version_info >= (3, 14) else {}),
    )
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
        '--record-build',
        action='store_true',
        help='record actual before-build dispatch and toolchain facts',
    )
    mode.add_argument(
        '--record-repaired-wheel',
        action='store_true',
        help='bind the CIBW raw and repaired wheel artifacts',
    )
    mode.add_argument(
        '--write-native-test-receipt',
        nargs=4,
        type=Path,
        metavar=('BINARY_JSON', 'JUNIT_XML', 'OUTPUT_JSON', 'PYTHON'),
        help='bind the running Nextest ELF, embedded source identity and JUnit report',
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
    mode.add_argument(
        '--inspect-libtiff-sdist',
        type=Path,
        metavar='SDIST',
        help='inspect representative libtiff members without requiring generated licenses',
    )
    mode.add_argument(
        '--stage-sdist',
        type=Path,
        metavar='DIRECTORY',
        help='stage one source-verified sdist and its sibling manifest',
    )
    mode.add_argument(
        '--verify-source',
        nargs=2,
        type=Path,
        metavar=('SDIST', 'MANIFEST'),
        help='verify a staged sdist against its source manifest',
    )
    mode.add_argument(
        '--write-wheel-receipt',
        nargs=3,
        type=Path,
        metavar=('WHEEL_DIR', 'IDENTIFIERS', 'SOURCE_MANIFEST'),
        help='validate planned wheels and write one runner receipt',
    )
    mode.add_argument(
        '--assemble-release',
        nargs=3,
        type=Path,
        metavar=('OUTPUT_DIR', 'SOURCE_DIR', 'RUNNERS_DIR'),
        help='validate runner receipts and assemble release distributions',
    )
    mode.add_argument(
        '--verify-release',
        type=Path,
        metavar='DIRECTORY',
        help='verify a complete gometry release distribution directory',
    )
    parser.add_argument(
        '--expected-runners',
        type=Path,
        metavar='JSON',
        help='JSON array of expected runners for --assemble-release',
    )
    parser.add_argument(
        '--expected-source-manifest',
        type=Path,
        metavar='JSON',
        help='staged source manifest whose identity the installed smoke must load',
    )
    parser.add_argument('--identifier', metavar='IDENTIFIER')
    parser.add_argument('--project-root', type=Path, metavar='DIR')
    parser.add_argument('--source-manifest', type=Path, metavar='PATH')
    parser.add_argument('--evidence-dir', type=Path, metavar='DIR')
    parser.add_argument('--wheel', type=Path, metavar='WHEEL')
    parser.add_argument('--unrepaired-wheel', type=Path, metavar='WHEEL')
    parser.add_argument('--wheel-dir', type=Path, metavar='DIR')
    parser.add_argument('--build-log', type=Path, metavar='PATH')
    parser.add_argument('--build-exit-code', type=int, metavar='INTEGER')
    parser.add_argument('--capture-exit-code', type=int, metavar='INTEGER')
    parser.add_argument('--runner', metavar='RUNNER')
    parser.add_argument('--profile', choices=('debug', 'release'))
    parser.add_argument('--run-id', metavar='UUID')
    args = parser.parse_args(argv)

    if args.record_build and not all((
        args.identifier,
        args.project_root,
        args.source_manifest,
        args.evidence_dir,
    )):
        parser.error(
            '--record-build requires --identifier, --project-root, '
            '--source-manifest and --evidence-dir'
        )
    if args.record_repaired_wheel and not all((
        args.identifier,
        args.unrepaired_wheel,
        args.wheel_dir,
        args.evidence_dir,
    )):
        parser.error(
            '--record-repaired-wheel requires --identifier, --unrepaired-wheel, '
            '--wheel-dir and --evidence-dir'
        )
    if args.installed:
        bound = (
            args.identifier is not None,
            args.wheel is not None,
            args.source_manifest is not None,
            args.evidence_dir is not None,
        )
        if any(bound) and not all(bound):
            parser.error(
                'recorded --installed requires --identifier, --wheel, '
                '--source-manifest and --evidence-dir'
            )
        if (
            args.expected_source_manifest is not None
            and args.source_manifest is not None
        ):
            parser.error('use only one installed-smoke source manifest option')
    if args.write_wheel_receipt and not all((
        args.evidence_dir,
        args.build_log,
        args.build_exit_code is not None,
        args.capture_exit_code is not None,
    )):
        parser.error(
            '--write-wheel-receipt requires --evidence-dir, --build-log, '
            '--build-exit-code and --capture-exit-code'
        )
    if args.write_native_test_receipt and not all((
        args.runner,
        args.profile,
        args.run_id,
    )):
        parser.error(
            '--write-native-test-receipt requires --runner, --profile and --run-id'
        )
    if not any((
        args.installed,
        args.record_build,
        args.record_repaired_wheel,
        args.write_wheel_receipt is not None,
    )) and any((
        args.identifier,
        args.project_root,
        args.source_manifest,
        args.evidence_dir,
        args.wheel,
        args.unrepaired_wheel,
        args.wheel_dir,
    )):
        parser.error('hook identity options require their matching hook mode')
    if args.project_root is not None and not args.record_build:
        parser.error('--project-root is only valid with --record-build')
    if args.record_build and any((
        args.unrepaired_wheel,
        args.wheel_dir,
        args.wheel,
    )):
        parser.error('--record-build cannot be combined with wheel artifact options')
    if args.record_repaired_wheel and any((
        args.project_root,
        args.source_manifest,
        args.wheel,
    )):
        parser.error(
            '--record-repaired-wheel cannot be combined with build or installed options'
        )
    if not args.write_wheel_receipt and any((
        args.build_log,
        args.build_exit_code is not None,
        args.capture_exit_code is not None,
    )):
        parser.error('build log and exit status options require --write-wheel-receipt')

    try:
        if args.record_build:
            record_build(
                args.identifier,
                args.source_manifest.resolve(),
                args.evidence_dir.resolve(),
                args.project_root.resolve(),
            )
            return 0
        if args.record_repaired_wheel:
            record_repaired_wheel(
                args.identifier,
                args.unrepaired_wheel.resolve(),
                args.wheel_dir.resolve(),
                args.evidence_dir.resolve(),
            )
            return 0
        if args.write_native_test_receipt:
            observation, junit, output, python = (
                path.resolve() for path in args.write_native_test_receipt
            )
            receipt = write_native_test_receipt(
                observation,
                junit,
                output,
                python=python,
                runner=args.runner,
                profile=args.profile,
                run_id=args.run_id,
            )
            print(receipt)
            return 0
        if args.installed:
            expected_source_manifest = (
                args.source_manifest.resolve()
                if args.source_manifest is not None
                else args.expected_source_manifest.resolve()
                if args.expected_source_manifest is not None
                else None
            )
            smoke_installed(
                expected_source_manifest=expected_source_manifest,
                wheel=args.wheel.resolve() if args.wheel is not None else None,
                identifier=args.identifier,
                evidence_dir=(
                    args.evidence_dir.resolve()
                    if args.evidence_dir is not None
                    else None
                ),
            )
            print('installed wheel smoke OK')
            return 0
        if args.inspect:
            for path in args.inspect:
                path = path.resolve()
                if path.name.endswith('.tar.gz'):
                    inspect_sdist(path, expected_libtiff=libtiff_source_members())
                    verify_sdist_dependency_sources(path)
                    print(f'artifact inspection OK: {path.name}')
                    continue
                _inspect_artifact(path)
                print(f'artifact inspection OK: {path.name}')
            return 0
        if args.smoke_wheel:
            wheel = args.smoke_wheel.resolve()
            inspect_wheel(wheel)
            expected_source_manifest = (
                args.expected_source_manifest.resolve()
                if args.expected_source_manifest is not None
                else None
            )
            smoke_install(wheel, expected_source_manifest=expected_source_manifest)
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
        if args.inspect_libtiff_sdist:
            sdist = args.inspect_libtiff_sdist.resolve()
            inspect_libtiff_sdist(sdist)
            print(f'libtiff sdist inspection OK: {sdist.name}')
            return 0
        if args.stage_sdist:
            path = stage_sdist(args.stage_sdist)
            print(path)
            return 0
        if args.verify_source:
            sdist, manifest = (path.resolve() for path in args.verify_source)
            verify_source_artifact(sdist, manifest)
            inspect_sdist(sdist, expected_libtiff=libtiff_source_members())
            print(f'source artifact verification OK: {sdist.name}')
            return 0
        if args.write_wheel_receipt:
            wheel_dir, identifiers, source_manifest = (
                path.resolve() for path in args.write_wheel_receipt
            )
            receipt = write_runner_receipt(
                wheel_dir,
                identifiers,
                source_manifest,
                evidence_dir=args.evidence_dir.resolve(),
                build_log=args.build_log.resolve(),
                build_exit_code=args.build_exit_code,
                capture_exit_code=args.capture_exit_code,
                runner=args.runner,
            )
            print(receipt)
            return 0
        if args.assemble_release:
            if args.expected_runners is None:
                parser.error('--assemble-release requires --expected-runners JSON')
            expected_text = args.expected_runners.read_text(encoding='utf-8')
            try:
                expected_value = json.loads(expected_text)
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
                parser.error(f'cannot read --expected-runners JSON: {error}')
            expected_runners = (
                expected_value
                if isinstance(expected_value, list)
                else expected_value.get('runners')
                if isinstance(expected_value, dict)
                else None
            )
            if not isinstance(expected_runners, list) or not all(
                isinstance(runner, str) for runner in expected_runners
            ):
                parser.error(
                    '--expected-runners JSON must contain a `runners` string array'
                )
            output_dir, source_dir, runners_dir = (
                path.resolve() for path in args.assemble_release
            )
            manifest = assemble_release(
                output_dir,
                source_dir,
                runners_dir,
                expected_runners,
            )
            print(manifest)
            return 0
        if args.verify_release:
            directory = args.verify_release.resolve()
            manifest = directory / RELEASE_MANIFEST_NAME
            validate_release_manifest(manifest, directory)
            print(f'release artifact verification OK: {directory}')
            return 0

        with tempfile.TemporaryDirectory(prefix='gometry-wheel-build-') as tmp:
            wheel = build_wheel(Path(tmp))
            inspect_wheel(wheel)
            smoke_install(wheel)
            print(f'wheel smoke OK: {wheel.name}')
    except KeyboardInterrupt:
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        os.kill(os.getpid(), signal.SIGINT)
        raise
    except BrokenPipeError:
        if not sys.stdout.closed:
            try:
                os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
            except OSError:
                pass
        return 0
    except (
        AssertionError,
        OSError,
        TypeError,
        ValueError,
        subprocess.SubprocessError,
        tarfile.TarError,
        zipfile.BadZipFile,
    ) as error:
        print(f'wheel_smoke: {error}', file=sys.stderr)
        return 1
    else:
        return 0


if __name__ == '__main__':
    raise SystemExit(main())
