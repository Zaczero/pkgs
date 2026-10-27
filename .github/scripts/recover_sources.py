#!/usr/bin/env python3
"""Complete the October 2026 releases from their original CI artifacts."""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import subprocess
import tarfile
from email import message_from_bytes
from pathlib import Path

from check_distributions import check_distribution

RUNS = {
    'blurhash-rs': 40450450286,
    'h2corn': 40450215916,
    'osm-shortlink': 40452651632,
    'polyline-rs': 40452754452,
    'totp-rs': 40453388101,
    'zid': 40453541710,
    'lrucache-rs': 40452406083,
}


def restore(source: Path, metadata: Path, destination: Path) -> None:
    with tarfile.open(source, 'r:gz') as original:
        infos = [
            item
            for item in original.getmembers()
            if item.isfile()
            and item.name.endswith('/PKG-INFO')
            and item.name.count('/') == 1
        ]
        if len(infos) != 1:
            raise ValueError(f'{source}: expected one PKG-INFO')
        info = infos[0]
        stream = original.extractfile(info)
        assert stream is not None
        declared = message_from_bytes(stream.read()).get_all('License-File', [])
        notices = {
            path.name: path.read_bytes() for path in metadata.glob('LICENSE*.md')
        }
        if not declared or not set(declared) <= notices.keys():
            raise ValueError(f'{source}: matching metadata lacks declared notices')
        prefix = info.name.rsplit('/', 1)[0]
        existing = set(original.getnames())
        with (
            destination.open('wb') as output,
            gzip.GzipFile(
                filename='', mode='wb', fileobj=output, mtime=int(info.mtime)
            ) as compressed,
            tarfile.open(fileobj=compressed, mode='w') as repaired,
        ):
            for member in original.getmembers():
                repaired.addfile(
                    member, original.extractfile(member) if member.isfile() else None
                )
            for name in declared:
                if f'{prefix}/{name}' in existing:
                    continue
                content = notices[name]
                member = tarfile.TarInfo(f'{prefix}/{name}')
                member.size = len(content)
                member.mode = 0o644
                member.mtime = info.mtime
                repaired.addfile(member, io.BytesIO(content))
    check_distribution(destination)
    print(
        destination.name,
        hashlib.sha256(destination.read_bytes()).hexdigest(),
        flush=True,
    )


def main() -> None:
    repository = os.environ['GITHUB_REPOSITORY']
    output = Path('dist')
    output.mkdir()
    for package, run_id in RUNS.items():
        endpoint = f'repos/{repository}/actions/runs/{run_id}'
        run = json.loads(subprocess.check_output(['gh', 'api', endpoint], text=True))
        if run['status'] != 'completed':
            subprocess.run(['gh', 'run', 'watch', str(run_id)], check=True)
            run = json.loads(
                subprocess.check_output(['gh', 'api', endpoint], text=True)
            )
        if (
            run['path'] != '.github/workflows/ci.yaml'
            or run['event'] not in ('push', 'workflow_dispatch')
            or run['head_branch'] != f'{package}/2.0.0'
            or run['status'] != 'completed'
            or run['conclusion'] != 'failure'
        ):
            raise ValueError(
                f'{package}: original run is not the expected failed release'
            )
        pages = json.loads(
            subprocess.check_output(
                ['gh', 'api', '--paginate', '--slurp', f'{endpoint}/jobs?per_page=100'],
                text=True,
            )
        )
        jobs = [job for page in pages for job in page['jobs']]
        failed = [
            job for job in jobs if job['conclusion'] not in ('success', 'skipped')
        ]
        if len(failed) != 1 or (failed[0]['name'], failed[0]['conclusion']) != (
            'Publish',
            'failure',
        ):
            raise ValueError(
                f'{package}: original release has failures outside publication'
            )
        staging = Path(os.environ['RUNNER_TEMP']) / package
        for name, directory in (
            (f'metadata-{package}', staging / 'metadata'),
            (f'dist-{package}-ubuntu-26.04-optimized', staging / 'distribution'),
        ):
            subprocess.run(
                [
                    'gh',
                    'run',
                    'download',
                    str(run_id),
                    '--name',
                    name,
                    '--dir',
                    str(directory),
                ],
                check=True,
            )
        sources = list((staging / 'distribution').glob('*.tar.gz'))
        if len(sources) != 1:
            raise ValueError(f'{package}: expected one original source archive')
        restore(sources[0], staging / 'metadata', output / sources[0].name)


if __name__ == '__main__':
    main()
