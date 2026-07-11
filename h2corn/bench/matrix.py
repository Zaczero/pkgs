"""Run the bounded, declarative h2corn end-to-end A/B workload matrix."""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import shutil
import socket
import sys
import tomllib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if __package__:
    from . import compare
else:
    import compare

if TYPE_CHECKING:
    from collections.abc import Sequence

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = ROOT / 'bench/matrix.toml'


@dataclass(frozen=True, slots=True)
class Scenario:
    family: str
    description: str
    transport: str
    protocol: str
    workload: str
    path: str
    load_driver: str
    workers: int
    loop_threads: int
    duration: str
    concurrency: int
    method: str
    body_size: int
    default: bool

    @property
    def id(self) -> str:
        return f'{self.family}-w{self.workers}-l{self.loop_threads}'


def load_manifest(path: Path) -> tuple[dict[str, Any], tuple[Scenario, ...]]:
    raw = tomllib.loads(path.read_text())
    if raw.get('schema_version') != 1:
        raise ValueError('matrix manifest schema_version must be 1')
    defaults = raw.get('defaults')
    families = raw.get('families')
    if not isinstance(defaults, dict) or not isinstance(families, list):
        raise TypeError('matrix manifest requires [defaults] and [[families]]')

    scenarios: list[Scenario] = []
    seen: set[str] = set()
    for family in families:
        if not isinstance(family, dict):
            raise TypeError('every matrix family must be a table')
        family_id = family.get('id')
        if not isinstance(family_id, str) or not compare.NAME_PATTERN.fullmatch(
            family_id
        ):
            raise ValueError(f'invalid matrix family id: {family_id!r}')
        for workers in family.get('workers', defaults.get('workers', [])):
            for loop_threads in family.get(
                'loop_threads', defaults.get('loop_threads', [])
            ):
                scenario = Scenario(
                    family=family_id,
                    description=str(family['description']),
                    transport=str(family['transport']),
                    protocol=str(family['protocol']),
                    workload=str(family['workload']),
                    path=str(family['path']),
                    load_driver=str(family['load_driver']),
                    workers=int(workers),
                    loop_threads=int(loop_threads),
                    duration=str(family.get('duration', defaults['duration'])),
                    concurrency=int(family.get('concurrency', defaults['concurrency'])),
                    method=str(family.get('method', 'GET')),
                    body_size=int(family.get('body_size', 0)),
                    default=bool(family.get('default', False))
                    and workers == 1
                    and loop_threads == 1,
                )
                if scenario.id in seen:
                    raise ValueError(f'duplicate matrix scenario: {scenario.id}')
                if scenario.transport not in {'tcp', 'tls', 'uds'}:
                    raise ValueError(f'{scenario.id}: invalid transport')
                if scenario.protocol not in {'h1', 'h2'}:
                    raise ValueError(f'{scenario.id}: invalid protocol')
                if scenario.load_driver not in {'oha', 'k6'}:
                    raise ValueError(f'{scenario.id}: invalid load_driver')
                if (
                    min(scenario.workers, scenario.loop_threads, scenario.concurrency)
                    < 1
                ):
                    raise ValueError(f'{scenario.id}: topology values must be positive')
                compare.parse_duration_seconds(scenario.duration)
                seen.add(scenario.id)
                scenarios.append(scenario)
    return raw, tuple(scenarios)


def _gil_enabled() -> bool:
    probe = getattr(sys, '_is_gil_enabled', None)
    return True if probe is None else bool(probe())


def unsupported_reason(
    scenario: Scenario, *, gil_enabled: bool | None = None
) -> str | None:
    if scenario.transport == 'uds' and not hasattr(socket, 'AF_UNIX'):
        return 'Unix-domain sockets are unavailable on this platform'
    if scenario.loop_threads > 1 and (
        _gil_enabled() if gil_enabled is None else gil_enabled
    ):
        return 'loop_threads > 1 is intentionally inactive on a GIL-enabled build'
    executable = 'oha' if scenario.load_driver == 'oha' else 'k6'
    if shutil.which(executable) is None:
        return f'required load generator is unavailable: {executable}'
    if scenario.transport == 'tls':
        try:
            import trustme  # noqa: F401
        except ImportError:
            return 'TLS scenarios require the trustme development dependency'
    return None


def select_scenarios(
    scenarios: Sequence[Scenario], patterns: Sequence[str], *, full: bool
) -> tuple[Scenario, ...]:
    if full:
        return tuple(scenarios)
    if patterns:
        selected = tuple(
            scenario
            for scenario in scenarios
            if any(fnmatch.fnmatchcase(scenario.id, pattern) for pattern in patterns)
        )
        if not selected:
            raise ValueError(f'no scenarios match: {", ".join(patterns)}')
        return selected
    return tuple(scenario for scenario in scenarios if scenario.default)


def _issue_tls_certificate(directory: Path) -> tuple[Path, Path]:
    import trustme

    directory.mkdir(parents=True, exist_ok=True)
    cert_path = directory / 'server.pem'
    key_path = directory / 'server.key'
    authority = trustme.CA()
    certificate = authority.issue_cert('localhost', '127.0.0.1')
    certificate.cert_chain_pems[0].write_to_path(cert_path)
    certificate.private_key_pem.write_to_path(key_path)
    return cert_path, key_path


def _scenario_commands(
    scenario: Scenario,
    control: compare.NamedCommand,
    candidate: compare.NamedCommand,
    *,
    port: int,
    socket_path: Path,
    cert: Path | None,
    key: Path | None,
) -> tuple[compare.NamedCommand, compare.NamedCommand]:
    bind = f'unix:{socket_path}' if scenario.transport == 'uds' else f'127.0.0.1:{port}'
    common = [
        '--bind',
        bind,
        '--workers',
        str(scenario.workers),
        '--loop-threads',
        str(scenario.loop_threads),
    ]
    if cert is not None and key is not None:
        common.extend(['--certfile', str(cert), '--keyfile', str(key)])
    return (
        compare.NamedCommand(control.name, (*control.argv, *common)),
        compare.NamedCommand(candidate.name, (*candidate.argv, *common)),
    )


def build_compare_argv(
    scenario: Scenario,
    control: compare.NamedCommand,
    candidate: compare.NamedCommand,
    *,
    port: int,
    socket_path: Path,
    cert: Path | None,
    key: Path | None,
    output: Path,
    args: argparse.Namespace,
) -> list[str]:
    control_command, candidate_command = _scenario_commands(
        scenario,
        control,
        candidate,
        port=port,
        socket_path=socket_path,
        cert=cert,
        key=key,
    )
    secure = scenario.transport == 'tls'
    if scenario.load_driver == 'k6':
        scheme = 'wss' if secure else 'ws'
    else:
        scheme = 'https' if secure else 'http'
    url = f'{scheme}://127.0.0.1:{port}{scenario.path}'
    ready_scheme = 'https' if secure else 'http'
    command = [
        '--control',
        f'{control_command.name}={compare.shlex.join(control_command.argv)}',
        '--candidate',
        f'{candidate_command.name}={compare.shlex.join(candidate_command.argv)}',
        '--url',
        url,
        '--ready-url',
        f'{ready_scheme}://127.0.0.1:{port}/',
        '--duration',
        args.duration or scenario.duration,
        '--concurrency',
        str(args.concurrency or scenario.concurrency),
        '--trials',
        str(args.trials),
        '--warmups',
        str(args.warmups),
        '--seed',
        str(args.seed),
        '--load-driver',
        scenario.load_driver,
        '--method',
        scenario.method,
        '--output',
        str(output),
    ]
    if scenario.protocol == 'h2':
        command.append('--http2')
    if secure:
        command.append('--insecure')
    if scenario.transport == 'uds':
        command.extend([
            '--unix-socket',
            str(socket_path),
            '--ready-unix-socket',
            str(socket_path),
        ])
    if scenario.body_size:
        command.extend(['--body', 'x' * scenario.body_size])
    if args.server_cpus:
        command.extend(['--server-cpus', args.server_cpus])
    if args.load_cpus:
        command.extend(['--load-cpus', args.load_cpus])
    return command


def _atomic_write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    temporary.replace(path)


def _is_complete(path: Path, expected_identity: dict[str, Any]) -> bool:
    try:
        record = json.loads(path.read_text())
        return (
            record.get('schema_version') == 2
            and record.get('status') == 'complete'
            and record.get('comparison_identity') == expected_identity
        )
    except (OSError, json.JSONDecodeError, AttributeError):
        return False


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument('--control', required=True, type=compare.parse_named_command)
    parser.add_argument('--candidate', required=True, type=compare.parse_named_command)
    parser.add_argument('--manifest', type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument('--output-dir', type=Path)
    parser.add_argument('--select', action='append', default=[], metavar='GLOB')
    parser.add_argument('--full', action='store_true')
    parser.add_argument('--list', action='store_true')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--duration')
    parser.add_argument('--concurrency', type=int)
    parser.add_argument('--trials', type=int, default=compare.DEFAULT_TRIALS)
    parser.add_argument('--warmups', type=int, default=2)
    parser.add_argument('--seed', type=int, default=compare.DEFAULT_SEED)
    parser.add_argument('--server-cpus')
    parser.add_argument('--load-cpus')
    parser.add_argument('--port-start', type=int, default=18080)
    parser.add_argument(
        '--runtime-gil',
        choices=('auto', 'enabled', 'disabled'),
        default='auto',
        help=(
            'GIL capability of the tested variants; auto follows the harness '
            'interpreter, while disabled enables four-loop scenarios'
        ),
    )
    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = create_parser()
    args = parser.parse_args(argv)
    if args.control.name == args.candidate.name:
        parser.error('control and candidate names must differ')
    if args.full and args.select:
        parser.error('--full and --select are mutually exclusive')
    if args.duration is not None:
        try:
            compare.parse_duration_seconds(args.duration)
        except argparse.ArgumentTypeError as error:
            parser.error(str(error))
    if args.concurrency is not None and args.concurrency < 1:
        parser.error('--concurrency must be positive')
    if args.trials < 5 or args.warmups < 1:
        parser.error('use at least five trials and one warmup')
    if not 1 <= args.port_start <= 65_535:
        parser.error('--port-start must be in [1, 65535]')
    for value in (args.server_cpus, args.load_cpus):
        if value is not None:
            compare.parse_cpu_set(value)
    timestamp = datetime.now(UTC).strftime('%Y%m%dT%H%M%S.%fZ')
    args.output_dir = args.output_dir or ROOT / 'bench/results/matrix' / timestamp
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    raw_manifest, all_scenarios = load_manifest(args.manifest)
    try:
        selected = select_scenarios(all_scenarios, args.select, full=args.full)
    except ValueError as error:
        raise SystemExit(str(error)) from error

    gil_enabled = (
        _gil_enabled() if args.runtime_gil == 'auto' else args.runtime_gil == 'enabled'
    )

    if args.list:
        for scenario in all_scenarios:
            marker = '*' if scenario.default else ' '
            reason = unsupported_reason(scenario, gil_enabled=gil_enabled)
            suffix = f' [SKIP: {reason}]' if reason else ''
            print(f'{marker} {scenario.id}: {scenario.description}{suffix}')
        return 0

    matrix_path = args.output_dir / 'matrix.json'
    record: dict[str, Any] = {
        'schema_version': 1,
        'status': 'dry-run' if args.dry_run else 'running',
        'created_at': datetime.now(UTC).isoformat(),
        'manifest': str(args.manifest),
        'manifest_sha256': hashlib.sha256(args.manifest.read_bytes()).hexdigest(),
        'manifest_data': raw_manifest,
        'variants': {
            'control': {'name': args.control.name, 'argv': list(args.control.argv)},
            'candidate': {
                'name': args.candidate.name,
                'argv': list(args.candidate.argv),
            },
        },
        'selection': [scenario.id for scenario in selected],
        'scenarios': {},
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    tls_paths: tuple[Path, Path] | None = None
    failed = False
    for index, scenario in enumerate(selected):
        output = args.output_dir / f'{scenario.id}.json'
        socket_path = args.output_dir / f'{scenario.id}.sock'
        reason = unsupported_reason(scenario, gil_enabled=gil_enabled)
        if reason is not None:
            record['scenarios'][scenario.id] = {'status': 'skipped', 'reason': reason}
            _atomic_write(matrix_path, record)
            continue
        if scenario.transport == 'tls' and tls_paths is None:
            tls_paths = _issue_tls_certificate(args.output_dir / 'tls')
        cert, key = tls_paths if scenario.transport == 'tls' else (None, None)
        compare_argv = build_compare_argv(
            scenario,
            args.control,
            args.candidate,
            port=args.port_start + index,
            socket_path=socket_path,
            cert=cert,
            key=key,
            output=output,
            args=args,
        )
        compare_args = compare.parse_args(compare_argv)
        expected_identity = compare.comparison_identity(compare_args)
        entry = {
            'status': 'planned',
            'result': str(output),
            'compare_argv': compare_argv,
        }
        record['scenarios'][scenario.id] = entry
        if _is_complete(output, expected_identity):
            entry['status'] = 'resumed-complete'
        elif not args.dry_run:
            entry['status'] = 'running'
            _atomic_write(matrix_path, record)
            return_code = compare.main(compare_argv)
            entry['status'] = 'complete' if return_code == 0 else 'failed'
            failed |= return_code != 0
        _atomic_write(matrix_path, record)

    record['status'] = 'failed' if failed else 'dry-run' if args.dry_run else 'complete'
    record['completed_at'] = datetime.now(UTC).isoformat()
    _atomic_write(matrix_path, record)
    print(f'matrix evidence: {matrix_path}')
    return int(failed)


if __name__ == '__main__':
    raise SystemExit(main())
