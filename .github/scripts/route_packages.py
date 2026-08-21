#!/usr/bin/env python3
"""Route changed paths to monorepo packages that need testing.

Reads paths on stdin. Docs-only paths are skipped, then the remaining paths are
mapped to sorted unique package names on stdout (ci.yaml pipes them to jq).
Diagnostics go to stderr; metadata/TOML failures exit nonzero.
"""

from __future__ import annotations

import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parent))
from package_version import ci_package_sort_key, list_packages, load_toml

ROOT_RUST_PATHS = frozenset({'Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml'})
IGNORED_PATHS = re.compile(
    r'^(?:'
    r'[^/]+/(?:docs|overrides|_snippets|assets)/'  # per-package docs and theme
    r'|[^/]+/properdocs\.yml$'  # per-package docs site config
    r'|[^/]+\.md$'  # repo-root markdown
    r')'
)


def filter_changed_paths(paths: Iterable[str]) -> list[str]:
    """Normalize stdin paths and skip docs-only changes."""
    filtered: list[str] = []
    for raw in paths:
        path = raw.strip()
        if path and not IGNORED_PATHS.search(path):
            filtered.append(path)
    return filtered


def discover_rusty_packages(test_packages: Iterable[str]) -> set[str]:
    return {name for name in test_packages if (Path(name) / 'Cargo.toml').is_file()}


def reverse_uv_consumers(test_packages: Sequence[str]) -> dict[str, set[str]]:
    """Map dependency package → set of consumers that pin it via tool.uv.sources."""
    repo = Path.cwd()
    consumers: dict[str, set[str]] = defaultdict(set)
    test_set = set(test_packages)

    for consumer in test_packages:
        data = load_toml(Path(consumer) / 'pyproject.toml')
        sources = data.get('tool', {}).get('uv', {}).get('sources', {})
        if not isinstance(sources, dict):
            continue
        for source in sources.values():
            if not isinstance(source, dict):
                continue
            path = source.get('path')
            if not isinstance(path, str) or not path:
                continue
            resolved = (Path(consumer) / path).resolve()
            try:
                rel = resolved.relative_to(repo)
            except ValueError:
                continue
            if not rel.parts:
                continue
            dep = rel.parts[0]
            if dep in test_set:
                consumers[dep].add(consumer)

    return dict(consumers)


def route_paths(
    paths: Sequence[str],
    *,
    test_packages: set[str] | None = None,
    rusty_packages: set[str] | None = None,
    reverse_consumers: Mapping[str, set[str]] | None = None,
) -> list[str]:
    """Select packages impacted by raw changed paths.

    Rules (exact root-Cargo match; package-local Cargo is not root):
    - Root Cargo.toml / Cargo.lock / rust-toolchain.toml / .cargo/** → all rusty
    - Root .python-versions → all test packages
    - <pkg>/… → that package + reverse uv.sources consumers
    - Anything else (shared dirs, unknown root files, .github/**) → all test packages
    - Per-package docs/overrides/_snippets/assets, properdocs.yml, and root .md
      files are ignored
    """
    if test_packages is None:
        packages = set(list_packages('test'))
    else:
        packages = set(test_packages)

    if rusty_packages is None:
        rusty = discover_rusty_packages(packages)
    else:
        rusty = set(rusty_packages)

    if reverse_consumers is None:
        reverse = reverse_uv_consumers(sorted(packages))
    else:
        reverse = {key: set(value) for key, value in reverse_consumers.items()}

    selected: set[str] = set()
    for path in filter_changed_paths(paths):
        if path in ROOT_RUST_PATHS or path == '.cargo' or path.startswith('.cargo/'):
            selected.update(rusty)
        elif path == '.python-versions':
            selected.update(packages)
        else:
            package = path.partition('/')[0]
            if package in packages:
                selected.add(package)
                selected.update(reverse.get(package, ()))
            else:
                selected.update(packages)

    return sorted(selected, key=ci_package_sort_key)


def main() -> None:
    paths = [line.rstrip('\n') for line in sys.stdin]
    try:
        selected = route_paths(paths)
    except SystemExit:
        raise
    except Exception as error:
        print(f'route_packages failed: {error}', file=sys.stderr)
        raise SystemExit(1) from error

    for name in selected:
        print(name)


if __name__ == '__main__':
    try:
        main()
    except KeyError as error:
        raise SystemExit(f'missing expected metadata key: {error}') from error
