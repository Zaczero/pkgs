#!/usr/bin/env python3
"""Generate every license file a package's distributions must ship.

The single source of truth is `project.license` (an SPDX expression) in each
package's pyproject.toml. This script derives the rest at build time:

- `<package>/LICENSE.md` — the package's own license text, rendered from the
  SPDX expression and pyproject author. Skipped when the package commits its
  own `LICEN[CS]E*` files (e.g. dual-licensed packages).
- `<package>/LICENSE-THIRD-PARTY.md` — rusty packages only. Every maturin
  package ships a statically linked cdylib, so it redistributes its Rust
  dependencies in binary form; MIT/BSD/ISC require their notice in binary
  distributions and Apache-2.0 requires the License + NOTICE. `cargo about`
  walks the package's Cargo.lock and reproduces each linked crate's license
  text using the shared `about.toml` config and `about.hbs` template next to
  this script.

Both file names match the PEP 639 default license-files glob (`LICEN[CS]E*`),
so hatchling and maturin bundle them into `.dist-info/licenses/` automatically —
no per-package `license-files` entry needed. Generated files are gitignored.

Usage (from the monorepo root):
    .github/scripts/gen_licenses.py <package>     # write the package's files
    .github/scripts/gen_licenses.py --all         # every released package

`cargo-about` comes from the dev shell (shell.nix) locally and from
`taiki-e/install-action` in CI; it is called by bare name.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
import tomllib
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = Path(__file__).resolve().parent
CONFIG = SCRIPTS / 'about.toml'
TEMPLATE = SCRIPTS / 'about.hbs'
OWN_NAME = 'LICENSE.md'
THIRD_PARTY_NAME = 'LICENSE-THIRD-PARTY.md'
LICENSE_GLOB = 'LICEN[CS]E*'

ZERO_BSD = """\
# 0BSD License

Copyright (c) {year} {author}

Permission to use, copy, modify, and/or distribute this software for any
purpose with or without fee is hereby granted.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
PERFORMANCE OF THIS SOFTWARE.
"""

MIT = """\
# MIT License

Copyright (c) {year} {author}

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

SPDX_TEXTS = {'0BSD': ZERO_BSD, 'MIT': MIT}


def _pyproject(package: str) -> dict:
    path = ROOT / package / 'pyproject.toml'
    if not path.exists():
        raise SystemExit(f'no pyproject.toml for package {package!r}')
    with path.open('rb') as stream:
        return tomllib.load(stream)


def _is_rusty(package: str) -> bool:
    return (ROOT / package / 'Cargo.toml').exists()


def _released_packages() -> list[str]:
    """Every package that declares a license — internal tooling is skipped."""
    return sorted(
        p.parent.name
        for p in ROOT.glob('*/pyproject.toml')
        if _pyproject(p.parent.name).get('project', {}).get('license')
    )


def _committed_license_files(package: str) -> list[Path]:
    return sorted(
        f
        for f in (ROOT / package).glob(LICENSE_GLOB)
        if f.name not in (OWN_NAME, THIRD_PARTY_NAME)
    )


def _render_own(package: str) -> str | None:
    """The package's own license text, or None when committed files own it."""
    if _committed_license_files(package):
        return None
    project = _pyproject(package).get('project', {})
    expression = project['license']
    text = SPDX_TEXTS.get(expression)
    if text is None:
        raise SystemExit(
            f'{package}: no template for license expression {expression!r}; '
            f'add it to SPDX_TEXTS or commit the license files next to '
            f'pyproject.toml.'
        )
    authors = project.get('authors') or []
    if not authors or 'name' not in authors[0]:
        raise SystemExit(f'{package}: project.authors must name the copyright holder')
    return text.format(year=datetime.now(UTC).year, author=authors[0]['name'])


def _generate_third_party(package: str, out_path: Path) -> None:
    if not shutil.which('cargo-about'):
        raise SystemExit(
            'cargo-about not found on PATH — it is provided by the dev shell; '
            'enter it (`nix-shell` from the pkgs root) and retry.'
        )
    cmd = [
        'cargo',
        'about',
        'generate',
        str(TEMPLATE),
        '--config',
        str(CONFIG),
        '--manifest-path',
        str(ROOT / package / 'Cargo.toml'),
        '-o',
        str(out_path),
    ]
    result = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        sys.stderr.write(result.stdout)
        sys.stderr.write(result.stderr)
        raise SystemExit(
            f'cargo about failed for {package} (exit {result.returncode}); a new '
            f'dependency may carry a license not in about.toml `accepted`.'
        )


def _fresh_files(package: str) -> dict[str, str]:
    """Name → content of every license file this package must generate."""
    files: dict[str, str] = {}
    own = _render_own(package)
    if own is not None:
        files[OWN_NAME] = own
    if _is_rusty(package):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / THIRD_PARTY_NAME
            _generate_third_party(package, out)
            files[THIRD_PARTY_NAME] = out.read_text(encoding='utf-8')
    return files


def _run(package: str) -> None:
    for name, content in _fresh_files(package).items():
        (ROOT / package / name).write_text(content, encoding='utf-8')
        print(f'wrote {package}/{name}')


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('package', nargs='?', help='package directory name')
    parser.add_argument('--all', action='store_true', help='every released package')
    args = parser.parse_args()

    if args.all:
        packages = _released_packages()
    elif args.package:
        packages = [args.package]
    else:
        parser.error('give a package name or --all')
    for package in packages:
        _run(package)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
