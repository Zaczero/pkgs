#!/usr/bin/env python3
"""Generate every license file a package's distributions must ship.

The single source of truth is `project.license` (an SPDX expression) in each
package's pyproject.toml. This script derives the rest at build time:

- The package's own license text, rendered from the SPDX expression and
  pyproject author: `LICENSE.md` for a single license, one
  `LICENSE-<NAME>.md` per choice for an `OR` dual license.
- `<package>/LICENSE-THIRD-PARTY.md` — rusty packages only. Every maturin
  package ships a statically linked cdylib, so it redistributes its Rust
  dependencies in binary form; MIT/BSD/ISC require their notice in binary
  distributions and Apache-2.0 requires the License + NOTICE. `cargo about`
  walks the package's Cargo.lock and reproduces each linked crate's license
  text using the shared `about.toml` config and `about.hbs` template next to
  this script.

All file names match the PEP 639 default license-files glob (`LICEN[CS]E*`),
so hatchling and maturin bundle them into `.dist-info/licenses/` automatically —
no per-package `license-files` entry needed. Every generated license file is
named `LICENSE*.md` and gitignored; no license text is ever committed.

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

APACHE_2_0 = """\
# Apache License 2.0

Apache License
Version 2.0, January 2004
https://www.apache.org/licenses/

TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION

1. Definitions.

"License" shall mean the terms and conditions for use, reproduction, and
distribution as defined by Sections 1 through 9 of this document.

"Licensor" shall mean the copyright owner or entity authorized by the
copyright owner that is granting the License.

"Legal Entity" shall mean the union of the acting entity and all other
entities that control, are controlled by, or are under common control with
that entity. For the purposes of this definition, "control" means (i) the
power, direct or indirect, to cause the direction or management of such
entity, whether by contract or otherwise, or (ii) ownership of fifty percent
(50%) or more of the outstanding shares, or (iii) beneficial ownership of
such entity.

"You" (or "Your") shall mean an individual or Legal Entity exercising
permissions granted by this License.

"Source" form shall mean the preferred form for making modifications,
including but not limited to software source code, documentation source, and
configuration files.

"Object" form shall mean any form resulting from mechanical transformation
or translation of a Source form, including but not limited to compiled object
code, generated documentation, and conversions to other media types.

"Work" shall mean the work of authorship, whether in Source or Object form,
made available under the License, as indicated by a copyright notice that is
included in or attached to the work.

"Derivative Works" shall mean any work, whether in Source or Object form,
that is based on or derived from the Work and for which the editorial
revisions, annotations, elaborations, or other modifications represent, as a
whole, an original work of authorship. For the purposes of this License,
Derivative Works shall not include works that remain separable from, or
merely link (or bind by name) to the interfaces of, the Work and Derivative
Works thereof.

"Contribution" shall mean any work of authorship, including the original
version of the Work and any modifications or additions to that Work or
Derivative Works thereof, that is intentionally submitted to Licensor for
inclusion in the Work by the copyright owner or by an individual or Legal
Entity authorized to submit on behalf of the copyright owner. For the purposes
of this definition, "submitted" means any form of electronic, verbal, or
written communication sent to the Licensor or its representatives, including
but not limited to communication on electronic mailing lists, source code
control systems, and issue tracking systems that are managed by, or on behalf
of, the Licensor for the purpose of discussing and improving the Work, but
excluding communication that is conspicuously marked or otherwise designated
in writing by the copyright owner as "Not a Contribution."

"Contributor" shall mean Licensor and any individual or Legal Entity on
behalf of whom a Contribution has been received by Licensor and subsequently
incorporated within the Work.

2. Grant of Copyright License. Subject to the terms and conditions of this
License, each Contributor hereby grants to You a perpetual, worldwide,
non-exclusive, no-charge, royalty-free, irrevocable copyright license to
reproduce, prepare Derivative Works of, publicly display, publicly perform,
sublicense, and distribute the Work and such Derivative Works in Source or
Object form.

3. Grant of Patent License. Subject to the terms and conditions of this
License, each Contributor hereby grants to You a perpetual, worldwide,
non-exclusive, no-charge, royalty-free, irrevocable patent license to make,
have made, use, offer to sell, sell, import, and otherwise transfer the Work,
where such license applies only to those patent claims licensable by such
Contributor that are necessarily infringed by their Contribution alone or by
combination of their Contribution with the Work to which such Contribution was
submitted. If You institute patent litigation against any entity (including a
cross-claim or counterclaim in a lawsuit) alleging that the Work or a
Contribution incorporated within the Work constitutes direct or contributory
patent infringement, then any patent licenses granted to You under this
License for that Work shall terminate as of the date such litigation is filed.

4. Redistribution. You may reproduce and distribute copies of the Work or
Derivative Works thereof in any medium, with or without modifications, and in
Source or Object form, provided that You meet the following conditions:

(a) You must give any other recipients of the Work or Derivative Works a copy
of this License; and

(b) You must cause any modified files to carry prominent notices stating that
You changed the files; and

(c) You must retain, in the Source form of any Derivative Works that You
distribute, all copyright, patent, trademark, and attribution notices from the
Source form of the Work, excluding those notices that do not pertain to any
part of the Derivative Works; and

(d) If the Work includes a "NOTICE" text file as part of its distribution,
then any Derivative Works that You distribute must include a readable copy of
the attribution notices contained within such NOTICE file, excluding those
notices that do not pertain to any part of the Derivative Works, in at least
one of the following places: within a NOTICE text file distributed as part of
the Derivative Works; within the Source form or documentation, if provided
along with the Derivative Works; or within a display generated by the
Derivative Works, if and wherever such third-party notices normally appear.
The contents of the NOTICE file are for informational purposes only and do not
modify the License. You may add Your own attribution notices within
Derivative Works that You distribute, alongside or as an addendum to the
NOTICE text from the Work, provided that such additional attribution notices
cannot be construed as modifying the License.

You may add Your own copyright statement to Your modifications and may
provide additional or different license terms and conditions for use,
reproduction, or distribution of Your modifications, or for any such
Derivative Works as a whole, provided Your use, reproduction, and distribution
of the Work otherwise complies with the conditions stated in this License.

5. Submission of Contributions. Unless You explicitly state otherwise, any
Contribution intentionally submitted for inclusion in the Work by You to the
Licensor shall be under the terms and conditions of this License, without any
additional terms or conditions. Notwithstanding the above, nothing herein
shall supersede or modify the terms of any separate license agreement you may
have executed with Licensor regarding such Contributions.

6. Trademarks. This License does not grant permission to use the trade names,
trademarks, service marks, or product names of the Licensor, except as
required for reasonable and customary use in describing the origin of the Work
and reproducing the content of the NOTICE file.

7. Disclaimer of Warranty. Unless required by applicable law or agreed to in
writing, Licensor provides the Work (and each Contributor provides its
Contributions) on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied, including, without limitation, any warranties
or conditions of TITLE, NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A
PARTICULAR PURPOSE. You are solely responsible for determining the
appropriateness of using or redistributing the Work and assume any risks
associated with Your exercise of permissions under this License.

8. Limitation of Liability. In no event and under no legal theory, whether in
tort (including negligence), contract, or otherwise, unless required by
applicable law (such as deliberate and grossly negligent acts) or agreed to in
writing, shall any Contributor be liable to You for damages, including any
direct, indirect, special, incidental, or consequential damages of any
character arising as a result of this License or out of the use or inability
to use the Work (including but not limited to damages for loss of goodwill,
work stoppage, computer failure or malfunction, or any and all other
commercial damages or losses), even if such Contributor has been advised of
the possibility of such damages.

9. Accepting Warranty or Additional Liability. While redistributing the Work
or Derivative Works thereof, You may choose to offer, and charge a fee for,
acceptance of support, warranty, indemnity, or other liability obligations
and/or rights consistent with this License. However, in accepting such
obligations, You may act only on Your own behalf and on Your sole
responsibility, not on behalf of any other Contributor, and only if You agree
to indemnify, defend, and hold each Contributor harmless for any liability
incurred by, or claims asserted against, such Contributor by reason of your
accepting any such warranty or additional liability.

END OF TERMS AND CONDITIONS
"""

SPDX_TEXTS = {'0BSD': ZERO_BSD, 'MIT': MIT, 'Apache-2.0': APACHE_2_0}
FILE_TAGS = {'0BSD': '0BSD', 'MIT': 'MIT', 'Apache-2.0': 'APACHE'}


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


def _render_own(package: str) -> dict[str, str]:
    """File name -> license text for the package's own SPDX expression."""
    project = _pyproject(package).get('project', {})
    expression = project['license']
    identifiers = expression.split(' OR ')
    unknown = [i for i in identifiers if i not in SPDX_TEXTS]
    if unknown:
        raise SystemExit(
            f'{package}: no template for license expression {expression!r}; '
            f'add the missing identifiers {unknown!r} to SPDX_TEXTS.'
        )
    authors = project.get('authors') or []
    if not authors or 'name' not in authors[0]:
        raise SystemExit(f'{package}: project.authors must name the copyright holder')
    rendered = {
        identifier: SPDX_TEXTS[identifier].format(
            year=datetime.now(UTC).year, author=authors[0]['name']
        )
        for identifier in identifiers
    }
    if len(identifiers) == 1:
        return {OWN_NAME: rendered[identifiers[0]]}
    return {
        f'LICENSE-{FILE_TAGS[identifier]}.md': rendered[identifier]
        for identifier in identifiers
    }


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
    files = _render_own(package)
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
