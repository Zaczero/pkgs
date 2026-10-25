"""Prepare profile-guided distribution wheels through Maturin and cibuildwheel."""

from __future__ import annotations

import argparse
import importlib
import json
import os
import shlex
import shutil
import subprocess
import sys
import tomllib
from contextlib import nullcontext
from pathlib import Path

from pgo_profile import merge
from wheels import (
    build_wheel,
    compilation_identity,
    file_identity,
    installed_wheel,
    one_wheel,
    source_identity,
)

ROOT = Path(__file__).resolve().parents[1]


def configuration(package: str):
    path = ROOT / package
    if path.parent != ROOT:
        raise ValueError('package must name a monorepo package directory')
    manifest = tomllib.loads((path / 'pyproject.toml').read_text())
    config = manifest['tool']['ci']['pgo']
    sys.path.insert(0, str(path))
    adapter = importlib.import_module(config['adapter'])
    return path, manifest['tool']['maturin']['module-name'], config, adapter


def check_profile(profile: Path, package: Path, config: dict) -> dict:
    reject_overriding_flags()
    manifest = json.loads(profile.with_name('profile.json').read_text())
    if file_identity(profile)['sha256'] != manifest['sha256']:
        raise RuntimeError('accepted profile digest changed')
    compiler = subprocess.check_output(['rustc', '-vV'], text=True, timeout=15)
    if compiler != manifest['compiler']:
        raise RuntimeError('accepted profile belongs to a different Rust compiler')
    if manifest['compilation'] != compilation_identity(package):
        raise RuntimeError(
            'accepted profile belongs to different source or dependencies'
        )
    if manifest['target'] != config['target'] or manifest['python'] != sys.version:
        raise RuntimeError(
            'accepted profile belongs to a different target or Python build'
        )
    return manifest


def reject_overriding_flags() -> None:
    for name in ('RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS'):
        if name in os.environ:
            raise RuntimeError(
                f'{name} overrides the target PGO configuration; put target flags in Cargo configuration'
            )


def flags(phase: str, adapter, profile: Path | None) -> list[str]:
    if phase == 'baseline':
        return []
    if phase == 'generate':
        return [
            '-Cprofile-generate=/tmp/pgo-parent',
            '-Cllvm-args=-instrprof-atomic-counter-update-all=true',
            *adapter.GENERATE_FLAGS,
        ]
    if profile is None:
        raise ValueError('profile-use requires an accepted indexed profile')
    return [
        f'-Cprofile-use={profile.resolve()}',
        '-Cllvm-args=-pgo-warn-missing-function',
    ]


def verify_feedback(log: Path, adapter) -> None:
    for line in log.read_text().splitlines():
        if 'warning:' not in line:
            continue
        if any(
            word in line for word in ('profile', 'control flow change', 'function hash')
        ) and not adapter.known_profile_diagnostic(line):
            raise RuntimeError(f'unclassified profile-use diagnostic: {line}')


def verify_wheel(
    wheel: Path, module_name: str, adapter, *, generator: bool = False
) -> dict:
    identity = installed_wheel(wheel, module_name)
    adapter.verify_runtime(generator=generator)
    sections = subprocess.check_output(
        ['readelf', '-SW', identity['native']['path']], text=True, timeout=15
    )
    if ('__llvm_prf' in sections) != generator:
        raise RuntimeError('wheel has the wrong instrumentation sections')
    return identity


def build(
    package: Path, config: dict, adapter, phase: str, output: Path, profile: Path | None
) -> Path:
    reject_overriding_flags()
    if phase == 'use':
        if profile is None:
            raise ValueError('profile-use requires --profile')
        check_profile(profile, package, config)
    delta = flags(phase, adapter, profile)
    wheel = build_wheel(
        package,
        output,
        target=config['target'],
        compatibility='linux',
        cargo_config=(f'target.{config["target"]}.rustflags={json.dumps(delta)}',),
        stage=adapter.stage_generator() if phase == 'generate' else None,
    )
    if phase == 'use':
        verify_feedback(output / 'build.log', adapter)
    return wheel


def python_members(record: dict) -> dict:
    return {
        name: digest
        for name, digest in record['members'].items()
        if name.endswith(('.py', '.pyi'))
    }


def release(package: Path, config: dict, adapter, output: Path, work: Path) -> None:
    work.mkdir(parents=True)
    output.mkdir(parents=True, exist_ok=True)
    source = source_identity(package)
    (work / 'build-spec.json').write_text(
        json.dumps(
            {
                'compilation': compilation_identity(package),
                'target': config['target'],
            },
            indent=2,
        )
        + '\n'
    )
    for name, groups in (
        ('training', ['pgo']),
        ('tests', config.get('test-groups', ['dev'])),
    ):
        subprocess.run(
            [
                'uv',
                'export',
                '--project',
                str(package),
                '--no-default-groups',
                *(argument for group in groups for argument in ('--group', group)),
                '--no-emit-project',
                '--no-emit-package',
                package.name,
                '--no-hashes',
                '--output-file',
                str(work / f'{name}.txt'),
            ],
            check=True,
            timeout=120,
            stdout=subprocess.DEVNULL,
        )
    engine = os.environ['CIBW_CONTAINER_ENGINE']
    if 'create_args:' not in engine:
        raise RuntimeError(
            'PGO release requires the existing Linux container mount configuration'
        )
    environment = {
        **os.environ,
        'CIBW_BUILD': config['build'],
        'CIBW_SKIP': '',
        'CIBW_TEST_SKIP': '',
        'CIBW_CONTAINER_ENGINE': engine + f' --volume {work}:/pgo',
        'CIBW_BEFORE_ALL_LINUX': os.environ['CIBW_BEFORE_ALL_LINUX']
        + ' && rustup component add llvm-tools-preview --toolchain "$RUSTUP_TOOLCHAIN"',
    }
    wheels = {}
    profile = None
    for phase in ('baseline', 'generate', 'use'):
        stage = work / phase
        stage.mkdir()
        compiler_config = work / f'{phase}.toml'
        container_profile = (
            f'/pgo/profiles/{profile.parent.name}/profile.profdata' if profile else ''
        )
        delta = (
            [
                f'-Cprofile-use={container_profile}',
                '-Cllvm-args=-pgo-warn-missing-function',
            ]
            if phase == 'use'
            else flags(phase, adapter, None)
        )
        compiler_config.write_text(
            f'[target.{config["target"]}]\nrustflags = {json.dumps(delta)}\n'
        )
        entry = f'python {{project}}/tools/pgo.py {package.name}'
        test = (
            entry
            + ' collect --wheel {wheel} --output /pgo/collection --profiles /pgo/profiles --build-spec /pgo/build-spec.json'
            if phase == 'generate'
            else entry + f' test --wheel {{wheel}} --output /pgo/{phase}/tests'
        )
        env = {
            **environment,
            'MATURIN_PEP517_ARGS': shlex.join([
                '--profile',
                'release-dist',
                '--locked',
                '--target',
                config['target'],
                '--config',
                f'/pgo/{phase}.toml',
            ]),
            'CIBW_BEFORE_TEST_LINUX': os.environ['CIBW_BEFORE_TEST_LINUX']
            + ' && cd {package} && uv pip install -r /pgo/'  # ruff: ignore[missing-f-string-syntax] -- cibuildwheel expands the container path
            + ('training.txt' if phase == 'generate' else 'tests.txt'),
            'CIBW_TEST_COMMAND': test,
        }
        if phase == 'use':
            env['CIBW_BEFORE_BUILD'] = (
                entry + f' check-profile --profile {container_profile}'
            )
        context = adapter.stage_generator() if phase == 'generate' else nullcontext()
        with context, (stage / 'build.log').open('w') as log:
            print(
                f'building {phase} {config["build"]}; log: {stage / "build.log"}',
                file=sys.stderr,
                flush=True,
            )
            try:
                subprocess.run(
                    [
                        'uv',
                        'tool',
                        'run',
                        'cibuildwheel==4.3.4',
                        str(package),
                        '--output-dir',
                        str(stage / 'wheels'),
                    ],
                    env=env,
                    cwd=ROOT,
                    check=True,
                    timeout=2700,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
            finally:
                if token := env.get('ACTIONS_RUNTIME_TOKEN'):
                    log.flush()
                    redacted = (stage / 'build.log').read_text().replace(token, '***')
                    log.seek(0)
                    log.write(redacted)
                    log.truncate()
        wheels[phase] = one_wheel(stage / 'wheels')
        if phase == 'generate':
            pointer = json.loads((work / 'profiles/profile.json').read_text())
            profile = work / 'profiles' / pointer['sha256'] / 'profile.profdata'
            if file_identity(profile)['sha256'] != pointer['sha256']:
                raise RuntimeError('profile transfer changed accepted bytes')
        elif phase == 'use':
            verify_feedback(stage / 'build.log', adapter)
    if len({wheel.name for wheel in wheels.values()}) != 1:
        raise RuntimeError('baseline/generator/use wheel identities differ')
    baseline = json.loads((work / 'baseline/tests/wheel.json').read_text())
    candidate = json.loads((work / 'use/tests/wheel.json').read_text())
    if python_members(baseline) != python_members(candidate):
        raise RuntimeError('profile-use wheel retained Python collection changes')
    if source['build_inputs'] != source_identity(package)['build_inputs']:
        raise RuntimeError('release source changed during PGO')
    destination = output / wheels['use'].name
    if destination.exists():
        raise RuntimeError(f'ordinary and PGO wheels collide: {destination}')
    shutil.copyfile(wheels['use'], destination)
    (work / 'release.json').write_text(
        json.dumps(
            {
                'identifier': config['build'],
                'optimization': 'pgo',
                'source': source,
                'profile': file_identity(profile),
                'wheels': {
                    phase: file_identity(wheel) for phase, wheel in wheels.items()
                },
                'published': file_identity(destination),
            },
            indent=2,
        )
        + '\n'
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument('package', help='Monorepo package with tool.ci.pgo metadata')
    commands = parser.add_subparsers(dest='command', required=True)
    build_parser = commands.add_parser(
        'build', help='Build a local distribution wheel through managed Cargo'
    )
    build_parser.add_argument('phase', choices=('baseline', 'generate', 'use'))
    build_parser.add_argument('--output', type=Path, required=True)
    build_parser.add_argument('--profile', type=Path)
    collect_parser = commands.add_parser(
        'collect', help='Train the installed generator and freeze its profile'
    )
    collect_parser.add_argument('--wheel', type=Path, required=True)
    collect_parser.add_argument('--output', type=Path, required=True)
    collect_parser.add_argument('--profiles', type=Path, required=True)
    collect_parser.add_argument('--build-spec', type=Path)
    test_parser = commands.add_parser(
        'test', help='Verify and test the exact installed wheel'
    )
    test_parser.add_argument('--wheel', type=Path, required=True)
    test_parser.add_argument('--output', type=Path, required=True)
    check_parser = commands.add_parser(
        'check-profile',
        help='Verify profile bytes and compiler identity before building',
    )
    check_parser.add_argument('--profile', type=Path, required=True)
    release_parser = commands.add_parser(
        'release', help='Prepare the selected publication wheel with cibuildwheel'
    )
    release_parser.add_argument('--output', type=Path, required=True)
    release_parser.add_argument('--work', type=Path, required=True)
    arguments = parser.parse_args()
    package, module, config, adapter = configuration(arguments.package)
    if arguments.command == 'build':
        print(
            build(
                package,
                config,
                adapter,
                arguments.phase,
                arguments.output.resolve(),
                arguments.profile,
            )
        )
    elif arguments.command == 'collect':
        spec_path = arguments.build_spec or arguments.wheel.with_suffix('.build.json')
        spec = json.loads(spec_path.read_text())
        identity = verify_wheel(arguments.wheel, module, adapter, generator=True)
        if 'wheel' in spec and spec['wheel']['sha256'] != identity['wheel']['sha256']:
            raise RuntimeError('generator wheel differs from its build specification')
        if 'compiler' in spec and spec['compiler'] != subprocess.check_output(
            ['rustc', '-vV'], text=True, timeout=15
        ):
            raise RuntimeError('generator wheel belongs to a different compiler')
        receipt = adapter.collect(arguments.output.resolve())
        profile = merge(
            receipt,
            arguments.profiles.resolve(),
            build={
                'compilation': spec['compilation'],
                'target': spec['target'],
                'python': sys.version,
                'generator_wheel': identity['wheel'],
                'generator_image_sha256': identity['native']['sha256'],
            },
        )
        (arguments.output / 'wheel.json').write_text(
            json.dumps(identity, indent=2) + '\n'
        )
        (arguments.profiles / 'profile.json').write_text(
            profile.with_name('profile.json').read_text()
        )
        print(profile)
    elif arguments.command == 'test':
        output = arguments.output.resolve()
        output.mkdir(parents=True)
        identity = verify_wheel(arguments.wheel, module, adapter)
        (output / 'wheel.json').write_text(json.dumps(identity, indent=2) + '\n')
        adapter.test(output)
        verify_wheel(arguments.wheel, module, adapter)
    elif arguments.command == 'check-profile':
        check_profile(arguments.profile, package, config)
    else:
        release(
            package,
            config,
            adapter,
            arguments.output.resolve(),
            arguments.work.resolve(),
        )


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(f'pgo: {error}', file=sys.stderr)
        raise SystemExit(1) from None
