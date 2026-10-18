"""Inspect and smoke-test Gometry distributions.

The default command builds one sdist, rebuilds a release wheel from it, then
installs that wheel in a temporary environment and exercises its public API.
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

from _artifact_common import release_build_env
from artifact_content import (
    inspect_libtiff_sdist,
    inspect_wheel,
    libtiff_source_members,
)
from release_artifact import (
    build_wheel,
    build_wheel_from_sdist,
    smoke_install,
    smoke_installed,
)
from source_artifact import (
    inspect_sdist,
    stage_sdist,
    verify_sdist_dependency_sources,
    verify_source_artifact,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


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
        help='Inspect existing wheels or source distributions',
    )
    mode.add_argument(
        '--installed',
        action='store_true',
        help='Exercise the active installed wheel',
    )
    mode.add_argument(
        '--smoke-wheel',
        type=Path,
        metavar='WHEEL',
        help='Inspect, clean-install, and exercise one wheel',
    )
    mode.add_argument(
        '--smoke-sdist',
        type=Path,
        metavar='SDIST',
        help='Rebuild a wheel from one source distribution and exercise its clean install',
    )
    mode.add_argument(
        '--inspect-libtiff-sdist',
        type=Path,
        metavar='SDIST',
        help='Inspect bundled libtiff without requiring generated licenses',
    )
    mode.add_argument(
        '--stage-sdist',
        type=Path,
        metavar='DIRECTORY',
        help='Build one source distribution and its source manifest',
    )
    mode.add_argument(
        '--verify-source',
        nargs=2,
        type=Path,
        metavar=('SDIST', 'MANIFEST'),
        help='Verify a source distribution against its manifest',
    )
    parser.add_argument(
        '--expected-source-manifest',
        type=Path,
        metavar='JSON',
        help='Bind a wheel smoke check to the selected source manifest',
    )
    args = parser.parse_args(argv)
    if args.expected_source_manifest is not None and not (
        args.installed or args.smoke_wheel is not None
    ):
        parser.error('--expected-source-manifest requires --installed or --smoke-wheel')
    try:
        if args.installed:
            smoke_installed(expected_source_manifest=args.expected_source_manifest)
            print('installed wheel smoke OK')
        elif args.inspect:
            for path in args.inspect:
                path = path.resolve()
                if path.name.endswith('.whl'):
                    inspect_wheel(path)
                elif path.name.endswith('.tar.gz'):
                    inspect_sdist(path, expected_libtiff=libtiff_source_members())
                    verify_sdist_dependency_sources(path)
                else:
                    parser.error(f'unsupported artifact: {path}')
                print(f'artifact inspection OK: {path.name}')
        elif args.smoke_wheel:
            wheel = args.smoke_wheel.resolve()
            smoke_install(wheel, expected_source_manifest=args.expected_source_manifest)
            print(f'wheel smoke OK: {wheel.name}')
        elif args.smoke_sdist:
            sdist = args.smoke_sdist.resolve()
            with tempfile.TemporaryDirectory(
                prefix='gometry-sdist-smoke-'
            ) as temporary:
                wheel = build_wheel_from_sdist(
                    sdist,
                    Path(temporary),
                    env=release_build_env(),
                )
                smoke_install(wheel)
            print(f'sdist rebuild smoke OK: {sdist.name}')
        elif args.inspect_libtiff_sdist:
            inspect_libtiff_sdist(args.inspect_libtiff_sdist.resolve())
            print(f'libtiff sdist inspection OK: {args.inspect_libtiff_sdist.name}')
        elif args.stage_sdist:
            print(stage_sdist(args.stage_sdist))
        elif args.verify_source:
            sdist, manifest = (path.resolve() for path in args.verify_source)
            verify_source_artifact(sdist, manifest)
            inspect_sdist(sdist, expected_libtiff=libtiff_source_members())
            print(f'source artifact verification OK: {sdist.name}')
        else:
            with tempfile.TemporaryDirectory(
                prefix='gometry-wheel-build-'
            ) as temporary:
                wheel = build_wheel(Path(temporary))
                smoke_install(wheel)
                print(f'wheel smoke OK: {wheel.name}')
    except KeyboardInterrupt:
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        os.kill(os.getpid(), signal.SIGINT)
        raise
    except BrokenPipeError:
        if not sys.stdout.closed:
            os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
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
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
