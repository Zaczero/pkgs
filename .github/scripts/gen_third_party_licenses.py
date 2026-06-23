#!/usr/bin/env python3
"""Generate <package>/LICENSE-THIRD-PARTY.md for a rusty monorepo package.

Every maturin package ships a statically linked cdylib, so it redistributes its
Rust dependencies in binary form; MIT/BSD/ISC require their notice in binary
distributions and Apache-2.0 requires the License + NOTICE. `cargo about` walks
the package's Cargo.lock and reproduces each linked crate's license text using the
shared `about.toml` config and `about.hbs` template next to this script.

The output file name matches the PEP 639 default license-files glob (`LICENSE-*`),
so maturin bundles it into the wheel's `.dist-info/licenses/` automatically — no
per-package `license-files` entry needed.

Usage (from the monorepo root):
    .github/scripts/gen_third_party_licenses.py <package>            # write it
    .github/scripts/gen_third_party_licenses.py <package> --check    # verify fresh
    .github/scripts/gen_third_party_licenses.py --all [--check]      # every rusty package

`cargo-about` comes from the dev shell (shell.nix) locally and from
`taiki-e/install-action` in CI; it is called by bare name.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = Path(__file__).resolve().parent
CONFIG = SCRIPTS / 'about.toml'
TEMPLATE = SCRIPTS / 'about.hbs'
OUTPUT_NAME = 'LICENSE-THIRD-PARTY.md'


def _rusty_packages() -> list[str]:
    return sorted(
        p.parent.name
        for p in ROOT.glob('*/Cargo.toml')
        if (p.parent / 'pyproject.toml').exists()
    )


def _generate(package: str, out_path: Path) -> None:
    if not shutil.which('cargo-about'):
        raise SystemExit(
            'cargo-about not found on PATH — it is provided by the dev shell; '
            'enter it (`nix-shell` from the pkgs root) and retry.'
        )
    manifest = ROOT / package / 'Cargo.toml'
    if not manifest.exists():
        raise SystemExit(f'no Cargo.toml for package {package!r}')
    cmd = [
        'cargo',
        'about',
        'generate',
        str(TEMPLATE),
        '--config',
        str(CONFIG),
        '--manifest-path',
        str(manifest),
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


def _run(package: str, *, check: bool) -> bool:
    committed = ROOT / package / OUTPUT_NAME
    if check:
        with tempfile.TemporaryDirectory() as tmp:
            fresh_path = Path(tmp) / OUTPUT_NAME
            _generate(package, fresh_path)
            fresh = fresh_path.read_text(encoding='utf-8')
        current = committed.read_text(encoding='utf-8') if committed.exists() else ''
        if fresh != current:
            sys.stderr.write(
                f'{package}/{OUTPUT_NAME} is stale; run '
                f'.github/scripts/gen_third_party_licenses.py {package}\n'
            )
            return False
        print(f'{package}/{OUTPUT_NAME} is up to date')
        return True
    _generate(package, committed)
    print(f'wrote {package}/{OUTPUT_NAME}')
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('package', nargs='?', help='package directory name')
    parser.add_argument('--all', action='store_true', help='every rusty package')
    parser.add_argument(
        '--check', action='store_true', help='verify instead of writing'
    )
    args = parser.parse_args()

    if args.all:
        packages = _rusty_packages()
    elif args.package:
        packages = [args.package]
    else:
        parser.error('give a package name or --all')

    return 0 if all(_run(pkg, check=args.check) for pkg in packages) else 1


if __name__ == '__main__':
    raise SystemExit(main())
