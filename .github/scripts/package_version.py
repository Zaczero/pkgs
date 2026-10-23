import argparse
import io
import os
import re
import subprocess
import tarfile
import tempfile
import tomllib
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal, Never, assert_never

VERSION_RE = re.compile(r'[0-9]+\.[0-9]+\.[0-9]+\Z')


def fail(message: str) -> Never:
    raise SystemExit(message)


def validate_version(version: str) -> None:
    if not VERSION_RE.fullmatch(version):
        fail(f'invalid version {version!r}; expected X.Y.Z')


def package_path(package: str) -> Path:
    path = Path(package)
    if path.name != package or not any(
        (path / manifest).is_file() for manifest in ('pyproject.toml', 'Cargo.toml')
    ):
        valid = ', '.join(
            sorted({
                path.parent.name
                for pattern in ('*/pyproject.toml', '*/Cargo.toml')
                for path in Path('.').glob(pattern)
            })
        )
        fail(f'unknown package {package!r}; valid packages: {valid}')
    return path


def load_toml(path: Path) -> dict:
    with path.open('rb') as file:
        return tomllib.load(file)


def ci_package_sort_key(package: str) -> tuple[bool, str]:
    """Put native builds ahead of pure-Python matrix entries."""
    return (
        not (Path(package) / 'Cargo.toml').is_file(),
        package,
    )


def list_packages(kind: Literal['release', 'test'] = 'release') -> list[str]:
    """List monorepo packages by capability set.

    kind='release' (default): dynamic-version packages eligible for release tags.
    kind='test': every top-level Python or Cargo package.
    """
    if kind == 'release':
        packages = []
        for pyproject in sorted(Path('.').glob('*/pyproject.toml')):
            data = load_toml(pyproject)
            if 'version' in data.get('project', {}).get('dynamic', []):
                packages.append(pyproject.parent.name)
        return packages

    if kind == 'test':
        packages = {
            manifest.parent.name
            for pattern in ('*/pyproject.toml', '*/Cargo.toml')
            for manifest in Path('.').glob(pattern)
        }
        # GitHub schedules matrix entries in declaration order when capacity is
        # constrained. Start the critical-path native builds before short jobs.
        return sorted(
            packages,
            key=ci_package_sort_key,
        )

    assert_never(kind)


def require_release_package(package: str) -> None:
    if package not in list_packages('release'):
        fail(f'{package!r} is not a release package')


def package_info(package: str) -> dict[str, Any]:
    path = package_path(package)
    if not (path / 'pyproject.toml').is_file():
        cargo = load_toml(path / 'Cargo.toml')['package']
        return {
            'import-name': '',
            'publishable': False,
            'package': package,
            'freethreaded-group': 'dev',
            'requires-python': '',
            'type': 'rust',
            'version': cargo['version'],
            'version-path': (path / 'Cargo.toml').as_posix(),
            'crate': cargo['name'],
        }
    pyproject = load_toml(path / 'pyproject.toml')
    project = pyproject['project']
    dependency_groups = pyproject.get('dependency-groups', {})
    freethreaded_group = (
        'freethreaded' if 'freethreaded' in dependency_groups else 'dev'
    )
    requires_python = project.get('requires-python', '')
    publishable = package in list_packages('release')
    cargo_toml = path / 'Cargo.toml'
    common = {
        'publishable': publishable,
        'package': package,
        'freethreaded-group': freethreaded_group,
        'requires-python': requires_python,
    }
    exclusion = (
        pyproject.get('tool', {}).get('ci', {}).get('freethreaded-test-exclusion')
    )
    if exclusion is not None:
        if not isinstance(exclusion, str) or not exclusion.strip():
            fail(f'{package}: freethreaded-test-exclusion must explain the blocker')
        common['freethreaded-test-exclusion'] = exclusion

    if cargo_toml.is_file():
        cargo = load_toml(cargo_toml)['package']
        module_name = pyproject['tool']['maturin']['module-name']
        import_name = module_name.split('.', maxsplit=1)[0]
        wheel_test = (
            pyproject
            .get('tool', {})
            .get('cibuildwheel', {})
            .get('test-command', f'python -c "import {import_name}"')
        )
        if isinstance(wheel_test, list):
            wheel_test = ' && '.join(wheel_test)
        return {
            **common,
            'import-name': import_name,
            'type': 'python-rust',
            'version': cargo['version'],
            'version-path': cargo_toml.as_posix(),
            'crate': cargo['name'],
            'wheel-test-command': wheel_test.strip(),
        }

    # Pure-Python with a static project.version (no hatch dynamic version path).
    if 'version' not in project.get('dynamic', []) and 'version' in project:
        packages = (
            pyproject
            .get('tool', {})
            .get('hatch', {})
            .get('build', {})
            .get('targets', {})
            .get('wheel', {})
            .get('packages')
        )
        if packages is None:
            import_name = project['name'].replace('-', '_')
        elif (
            not isinstance(packages, list)
            or len(packages) != 1
            or not isinstance(packages[0], str)
            or not packages[0]
        ):
            fail(f'{package}: expected exactly one Hatch wheel package')
        else:
            import_name = Path(packages[0]).name
        return {
            **common,
            'import-name': import_name,
            'type': 'python',
            'version': str(project['version']),
        }

    version_path = path / pyproject['tool']['hatch']['version']['path']
    return {
        **common,
        'import-name': version_path.relative_to(path).parts[0],
        'type': 'python',
        'version': read_python_version(version_path),
        'version-path': version_path.as_posix(),
    }


def read_python_version(path: Path) -> str:
    match = re.search(
        r"(?m)^__version__\s*=\s*(['\"])([^'\"]+)\1[ \t]*$",
        path.read_text(),
    )
    if match is None:
        fail(f'could not find __version__ in {path}')
    return match.group(2)


def set_rust_version(path: Path, version: str) -> None:
    text = path.read_text()
    pattern = re.compile(r'(?ms)^(\[package\]\s+.*?^version\s*=\s*\")([^\"]+)(\")')
    text, count = pattern.subn(rf'\g<1>{version}\3', text, count=1)
    if count != 1:
        fail(f'could not update [package].version in {path}')
    path.write_text(text)


def set_python_version(path: Path, version: str) -> None:
    text = path.read_text()
    pattern = re.compile(r"(?m)^(__version__\s*=\s*)(['\"])([^'\"]+)(\2)([ \t]*)$")
    text, count = pattern.subn(rf'\g<1>\g<2>{version}\g<4>\g<5>', text, count=1)
    if count != 1:
        fail(f'could not update __version__ in {path}')
    path.write_text(text)


def write_outputs(outputs: Mapping[str, object]) -> None:
    outputs = {
        key: str(value).lower() if isinstance(value, bool) else value
        for key, value in outputs.items()
    }
    for key, value in outputs.items():
        print(f'{key}={value}')

    output_path = os.environ.get('GITHUB_OUTPUT')
    if output_path is not None:
        with Path(output_path).open('a') as file:
            for key, value in outputs.items():
                text = str(value)
                if '\n' in text:
                    delimiter = 'PKGS_OUTPUT'
                    while delimiter in text.splitlines():
                        delimiter += '_'
                    file.write(f'{key}<<{delimiter}\n{text}\n{delimiter}\n')
                else:
                    file.write(f'{key}={text}\n')


def command_list(args: argparse.Namespace) -> None:
    for name in list_packages(args.kind):
        print(name)


def command_info(args: argparse.Namespace) -> None:
    write_outputs(package_info(args.package))


def command_set(args: argparse.Namespace) -> None:
    require_release_package(args.package)
    validate_version(args.version)
    info = package_info(args.package)

    if info['type'] == 'python-rust':
        set_rust_version(Path(info['version-path']), args.version)
    else:
        set_python_version(Path(info['version-path']), args.version)

    info = package_info(args.package)
    if info['version'] != args.version:
        fail(
            f'version update failed for {args.package}: '
            f'expected {args.version}, got {info["version"]}'
        )
    write_outputs(info)


def command_check(args: argparse.Namespace) -> None:
    require_release_package(args.package)
    info = package_info(args.package)
    if info['version'] != args.version:
        fail(
            f'version mismatch for {args.package}: '
            f'expected {args.version}, got {info["version"]}'
        )
    write_outputs(info)


def command_check_api(args: argparse.Namespace) -> None:
    import griffe

    require_release_package(args.package)
    validate_version(args.version)
    current = tuple(map(int, args.version.split('.')))
    compatibility_series = current[:2] if current[0] == 0 else current[:1]
    tags = subprocess.check_output(
        ['git', 'tag', '--list', f'{args.package}/*'], text=True
    ).splitlines()
    candidates = []
    for tag in tags:
        version = tag.removeprefix(f'{args.package}/')
        if VERSION_RE.fullmatch(version):
            parsed = tuple(map(int, version.split('.')))
            if (
                parsed[: len(compatibility_series)] == compatibility_series
                and parsed < current
            ):
                candidates.append((parsed, tag))
    if not candidates:
        series = '.'.join(map(str, compatibility_series))
        print(f'{args.package}: no earlier release in {series}.x to compare')
        return
    previous = max(candidates)[1]
    info = package_info(args.package)
    metadata = load_toml(Path(args.package) / 'pyproject.toml')
    source = metadata.get('tool', {}).get('maturin', {}).get('python-source', '.')
    old_metadata = tomllib.loads(
        subprocess.check_output(
            ['git', 'show', f'{previous}:{args.package}/pyproject.toml'], text=True
        )
    )
    old_source = (
        old_metadata.get('tool', {}).get('maturin', {}).get('python-source', '.')
    )
    source_path = Path(args.package) / old_source
    archive = subprocess.check_output([
        'git',
        'archive',
        previous,
        '--',
        str(source_path),
    ])
    with tempfile.TemporaryDirectory(prefix='api-compat-') as directory:
        with tarfile.open(fileobj=io.BytesIO(archive)) as files:
            files.extractall(directory, filter='data')
        old = griffe.load(
            info['import-name'],
            search_paths=[Path(directory) / source_path],
            allow_inspection=False,
            resolve_aliases=True,
            resolve_external=False,
        )
        new = griffe.load(
            info['import-name'],
            search_paths=[Path(args.package) / source],
            allow_inspection=False,
            resolve_aliases=True,
            resolve_external=False,
        )
        changes = list(griffe.find_breaking_changes(old, new))
    breaking = []
    for change in changes:
        if (
            isinstance(change, griffe.AttributeChangedValueBreakage)
            and change.obj.name == '__version__'
        ):
            continue
        if isinstance(change, griffe.AttributeChangedValueBreakage) and (
            isinstance(change.old_value, griffe.ExprCall)
            or isinstance(change.new_value, griffe.ExprCall)
        ):
            print(f'Review computed value change: {change.obj.path}')
        else:
            breaking.append(change)
            print(change.explain(griffe.ExplanationStyle.GITHUB))
    if breaking:
        fail(f'{args.package}: {len(breaking)} public API breakages since {previous}')
    print(f'{args.package}: public API is compatible with {previous}')


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(required=True)

    list_parser = subparsers.add_parser('list')
    list_kind = list_parser.add_mutually_exclusive_group()
    list_kind.add_argument(
        '--release',
        action='store_const',
        const='release',
        dest='kind',
        help='List release packages (default)',
    )
    list_kind.add_argument(
        '--test',
        action='store_const',
        const='test',
        dest='kind',
        help='List all testable packages (includes static-version tooling)',
    )
    list_parser.set_defaults(func=command_list, kind='release')

    info_parser = subparsers.add_parser('info')
    info_parser.add_argument('package')
    info_parser.set_defaults(func=command_info)

    set_parser = subparsers.add_parser('set')
    set_parser.add_argument('package')
    set_parser.add_argument('version')
    set_parser.set_defaults(func=command_set)

    check_parser = subparsers.add_parser('check')
    check_parser.add_argument('package')
    check_parser.add_argument('version')
    check_parser.set_defaults(func=command_check)

    api_parser = subparsers.add_parser('check-api')
    api_parser.add_argument('package')
    api_parser.add_argument('version')
    api_parser.set_defaults(func=command_check_api)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    try:
        main()
    except KeyError as error:
        raise SystemExit(f'missing expected metadata key: {error}') from error
