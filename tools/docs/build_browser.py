"""Compile shared browser assets into the documentation site with Bun."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]


def on_post_build(config, **_kwargs):
    settings = {
        'site': str(Path(config.site_dir).resolve()),
        'siteURL': config.site_url,
        'fonts': 'assets/shared/fonts.css' in config.extra_css,
        'stylesheets': {
            path: str((Path(config.docs_dir) / path).resolve())
            for path in config.extra_css
            if (Path(config.docs_dir) / path).is_file()
        },
    }
    subprocess.run(['bun', 'install', '--frozen-lockfile'], cwd=_ROOT, check=True)
    if python := config.extra.get('browser_python'):
        if command := python.get('build'):
            subprocess.run(command, cwd=_ROOT, check=True)
        package = Path(config.config_file_path).resolve().parent
        wheels = []
        for pattern in python['wheels']:
            matches = list(package.glob(pattern))
            if len(matches) != 1:
                raise FileNotFoundError(
                    f'Expected one browser wheel matching {package / pattern}; '
                    'build the package WebAssembly wheel before building its docs.'
                )
            wheels.append(str(matches[0].resolve()))
        settings['python'] = {
            'packages': python.get('packages', []),
            'wheels': wheels,
        }
    for command in (
        ['bun', 'run', 'typecheck'],
        ['bun', 'run', 'docs:browser', json.dumps(settings)],
    ):
        subprocess.run(command, cwd=_ROOT, check=True)
