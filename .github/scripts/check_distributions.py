#!/usr/bin/env python3
"""Verify that built distributions contain their declared license files."""

from __future__ import annotations

import argparse
import sys
import tarfile
import zipfile
from email import message_from_bytes
from pathlib import Path, PurePosixPath


def check_licenses(metadata: bytes, prefix: str, files: set[str]) -> None:
    licenses = message_from_bytes(metadata).get_all('License-File', [])
    if not licenses:
        raise ValueError('metadata declares no license files')
    for name in licenses:
        path = PurePosixPath(name)
        if not name or path.is_absolute() or '..' in path.parts:
            raise ValueError(f'invalid License-File path: {name!r}')
        if f'{prefix}/{name}' not in files:
            raise ValueError(f'missing declared license file: {name}')


def check_distribution(path: Path) -> None:
    if path.suffix == '.whl':
        with zipfile.ZipFile(path) as archive:
            files = {info.filename for info in archive.infolist() if not info.is_dir()}
            metadata = [
                name
                for name in files
                if name.endswith('.dist-info/METADATA') and name.count('/') == 1
            ]
            if len(metadata) != 1:
                raise ValueError('expected one wheel METADATA file')
            check_licenses(
                archive.read(metadata[0]),
                f'{metadata[0].rsplit("/", 1)[0]}/licenses',
                files,
            )
    else:
        with tarfile.open(path, 'r:gz') as archive:
            files = {info.name for info in archive.getmembers() if info.isfile()}
            metadata = [
                name
                for name in files
                if name.endswith('/PKG-INFO') and name.count('/') == 1
            ]
            if len(metadata) != 1:
                raise ValueError('expected one top-level PKG-INFO file')
            stream = archive.extractfile(metadata[0])
            assert stream is not None
            check_licenses(stream.read(), metadata[0].rsplit('/', 1)[0], files)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    paths = sorted(args.directory.glob('*.whl')) + sorted(
        args.directory.glob('*.tar.gz')
    )
    if not paths:
        parser.error(f'no distributions found under {args.directory}')
    failed = False
    for path in paths:
        try:
            check_distribution(path)
        except (OSError, ValueError, tarfile.TarError, zipfile.BadZipFile) as error:
            print(f'{path.name}: {error}', file=sys.stderr, flush=True)
            failed = True
        else:
            print(f'{path.name}: license files verified', flush=True)
    return int(failed)


if __name__ == '__main__':
    raise SystemExit(main())
