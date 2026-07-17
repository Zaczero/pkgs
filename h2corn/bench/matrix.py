"""Run the bounded, declarative h2corn end-to-end A/B workload matrix."""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import shlex
import shutil
import socket
import sys
import tomllib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, NotRequired, TypedDict, TypeVar, cast

try:
    # Not ``from bench import compare``: under direct-script execution the
    # sibling bench.py shadows the package and raises ImportError instead of
    # the ModuleNotFoundError this fallback relies on.
    import bench.compare as compare  # noqa: PLR0402
    from bench.provenance import (
        file_identity,
        git_metadata,
        result_environment,
        tool_identity,
        variant_artifacts,
        variant_environment_evidence,
    )
    from bench.system import (
        MAX_AMBIENT_CPU_UTILIZATION,
        MAX_AMBIENT_SINGLE_CPU_UTILIZATION,
        BenchmarkError,
        benchmark_system_state_matches,
        capture_system_state,
        durable_json,
        pin_benchmark_driver,
        validate_cpu_roles,
    )
except ModuleNotFoundError:  # Direct ``python bench/matrix.py`` execution.
    import compare  # type: ignore[import-not-found, no-redef]
    from provenance import (  # type: ignore[import-not-found, no-redef]
        file_identity,
        git_metadata,
        result_environment,
        tool_identity,
        variant_artifacts,
        variant_environment_evidence,
    )
    from system import (  # type: ignore[import-not-found, no-redef]
        MAX_AMBIENT_CPU_UTILIZATION,
        MAX_AMBIENT_SINGLE_CPU_UTILIZATION,
        BenchmarkError,
        benchmark_system_state_matches,
        capture_system_state,
        durable_json,
        pin_benchmark_driver,
        validate_cpu_roles,
    )

if TYPE_CHECKING:
    from collections.abc import Sequence

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = ROOT / 'bench/matrix.toml'
MATRIX_SCHEMA_VERSION = 6


class ManifestDefaults(TypedDict):
    duration: str
    concurrency: int
    workers: list[int]
    loop_threads: list[int]


class ManifestFamily(TypedDict):
    id: str
    description: str
    transport: Literal['tcp', 'tls', 'uds']
    protocol: Literal['h1', 'h2']
    workload: str
    path: str
    load_driver: Literal['oha', 'k6']
    workers: NotRequired[list[int]]
    loop_threads: NotRequired[list[int]]
    duration: NotRequired[str]
    concurrency: NotRequired[int]
    method: NotRequired[str]
    body_size: NotRequired[int]
    default: NotRequired[bool]


class Manifest(TypedDict):
    schema_version: int
    defaults: ManifestDefaults
    families: list[ManifestFamily]


_MANIFEST_KEYS = frozenset({'schema_version', 'defaults', 'families'})
_DEFAULT_KEYS = frozenset({'duration', 'concurrency', 'workers', 'loop_threads'})
_FAMILY_REQUIRED_KEYS = frozenset({
    'id',
    'description',
    'transport',
    'protocol',
    'workload',
    'path',
    'load_driver',
})
_FAMILY_OPTIONAL_KEYS = frozenset({
    'workers',
    'loop_threads',
    'duration',
    'concurrency',
    'method',
    'body_size',
    'default',
})


@dataclass(frozen=True, slots=True)
class Scenario:
    family: str
    description: str
    transport: Literal['tcp', 'tls', 'uds']
    protocol: Literal['h1', 'h2']
    workload: str
    path: str
    load_driver: Literal['oha', 'k6']
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


def _table(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise TypeError(f'{label} must be a table with string keys')
    return {key: item for key, item in value.items() if isinstance(key, str)}


def _validate_keys(
    table: dict[str, object],
    *,
    required: frozenset[str],
    optional: frozenset[str] = frozenset(),
    label: str,
) -> None:
    missing = sorted(required - table.keys())
    if missing:
        raise ValueError(f'{label} is missing required keys: {missing!r}')
    unexpected = sorted(table.keys() - required - optional)
    if unexpected:
        raise ValueError(f'{label} has unsupported keys: {unexpected!r}')


def _string(table: dict[str, object], key: str, label: str) -> str:
    value = table[key]
    if not isinstance(value, str):
        raise TypeError(f'{label}.{key} must be a string')
    return value


def _integer(table: dict[str, object], key: str, label: str) -> int:
    value = table[key]
    if type(value) is not int:
        raise TypeError(f'{label}.{key} must be an integer')
    return value


def _boolean(table: dict[str, object], key: str, label: str) -> bool:
    value = table[key]
    if type(value) is not bool:
        raise TypeError(f'{label}.{key} must be a boolean')
    return value


def _integer_list(table: dict[str, object], key: str, label: str) -> list[int]:
    value = table[key]
    if not isinstance(value, list) or any(type(item) is not int for item in value):
        raise TypeError(f'{label}.{key} must be a list of integers')
    return value.copy()


_ChoiceT = TypeVar('_ChoiceT', bound=str)
_TRANSPORTS: tuple[Literal['tcp', 'tls', 'uds'], ...] = ('tcp', 'tls', 'uds')
_PROTOCOLS: tuple[Literal['h1', 'h2'], ...] = ('h1', 'h2')
_LOAD_DRIVERS: tuple[Literal['oha', 'k6'], ...] = ('oha', 'k6')


def _choice(
    table: dict[str, object],
    key: str,
    options: tuple[_ChoiceT, ...],
    label: str,
) -> _ChoiceT:
    value = _string(table, key, label)
    for option in options:
        if value == option:
            return option
    raise ValueError(f'{label}.{key} is invalid: {value!r}')


def _manifest_defaults(value: object) -> ManifestDefaults:
    label = 'matrix defaults'
    table = _table(value, label)
    _validate_keys(table, required=_DEFAULT_KEYS, label=label)
    return {
        'duration': _string(table, 'duration', label),
        'concurrency': _integer(table, 'concurrency', label),
        'workers': _integer_list(table, 'workers', label),
        'loop_threads': _integer_list(table, 'loop_threads', label),
    }


def _manifest_family(value: object, index: int) -> ManifestFamily:
    label = f'matrix family {index}'
    table = _table(value, label)
    _validate_keys(
        table,
        required=_FAMILY_REQUIRED_KEYS,
        optional=_FAMILY_OPTIONAL_KEYS,
        label=label,
    )
    family: ManifestFamily = {
        'id': _string(table, 'id', label),
        'description': _string(table, 'description', label),
        'transport': _choice(table, 'transport', _TRANSPORTS, label),
        'protocol': _choice(table, 'protocol', _PROTOCOLS, label),
        'workload': _string(table, 'workload', label),
        'path': _string(table, 'path', label),
        'load_driver': _choice(table, 'load_driver', _LOAD_DRIVERS, label),
    }
    for key in ('workers', 'loop_threads'):
        if key in table:
            family[key] = _integer_list(table, key, label)
    for key in ('duration', 'method'):
        if key in table:
            family[key] = _string(table, key, label)
    for key in ('concurrency', 'body_size'):
        if key in table:
            family[key] = _integer(table, key, label)
    if 'default' in table:
        family['default'] = _boolean(table, 'default', label)
    return family


def load_manifest(path: Path) -> tuple[Manifest, tuple[Scenario, ...]]:
    raw = _table(tomllib.loads(path.read_text()), 'matrix manifest')
    _validate_keys(raw, required=_MANIFEST_KEYS, label='matrix manifest')
    schema_version = _integer(raw, 'schema_version', 'matrix manifest')
    if schema_version != 1:
        raise ValueError('matrix manifest schema_version must be 1')
    defaults = _manifest_defaults(raw['defaults'])
    raw_families = raw['families']
    if not isinstance(raw_families, list):
        raise TypeError('matrix manifest.families must be a list of tables')
    families = [
        _manifest_family(family, index)
        for index, family in enumerate(raw_families, start=1)
    ]
    manifest: Manifest = {
        'schema_version': schema_version,
        'defaults': defaults,
        'families': families,
    }

    scenarios: list[Scenario] = []
    seen: set[str] = set()
    for family in families:
        family_id = family['id']
        if not compare.NAME_PATTERN.fullmatch(family_id):
            raise ValueError(f'invalid matrix family id: {family_id!r}')
        for workers in family.get('workers', defaults['workers']):
            for loop_threads in family.get('loop_threads', defaults['loop_threads']):
                scenario = Scenario(
                    family=family_id,
                    description=family['description'],
                    transport=family['transport'],
                    protocol=family['protocol'],
                    workload=family['workload'],
                    path=family['path'],
                    load_driver=family['load_driver'],
                    workers=workers,
                    loop_threads=loop_threads,
                    duration=family.get('duration', defaults['duration']),
                    concurrency=family.get('concurrency', defaults['concurrency']),
                    method=family.get('method', 'GET'),
                    body_size=family.get('body_size', 0),
                    default=family.get('default', False)
                    and workers == 1
                    and loop_threads == 1,
                )
                if scenario.id in seen:
                    raise ValueError(f'duplicate matrix scenario: {scenario.id}')
                if (
                    min(scenario.workers, scenario.loop_threads, scenario.concurrency)
                    < 1
                ):
                    raise ValueError(f'{scenario.id}: topology values must be positive')
                if scenario.body_size < 0:
                    raise ValueError(f'{scenario.id}: body_size must not be negative')
                compare.parse_duration_seconds(scenario.duration)
                seen.add(scenario.id)
                scenarios.append(scenario)
    return manifest, tuple(scenarios)


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


def _tls_certificate(directory: Path) -> tuple[Path, Path]:
    cert_path = directory / 'server.pem'
    key_path = directory / 'server.key'
    if cert_path.is_file() and key_path.is_file():
        return cert_path, key_path
    if cert_path.exists() or key_path.exists():
        raise ValueError(
            f'incomplete reusable TLS identity under {directory}; remove both files'
        )
    return _issue_tls_certificate(directory)


def _expected_response_body(scenario: Scenario) -> bytes | None:
    if scenario.load_driver == 'k6':
        return None
    if scenario.workload == 'unary':
        return b'Hello, World!'
    if scenario.workload == 'stream-upload':
        return str(scenario.body_size).encode()
    if scenario.workload == 'stream-download':
        return b'x' * (128 * 1024)
    if scenario.workload == 'pathsend':
        return b'\0' * (128 * 1024)
    raise ValueError(f'{scenario.id}: no exact response contract for its workload')


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
        f'{control_command.name}={shlex.join(control_command.argv)}',
        '--candidate',
        f'{candidate_command.name}={shlex.join(candidate_command.argv)}',
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
        '--expected-workers',
        str(scenario.workers),
        '--output',
        str(output),
        '--identity-input',
        str(args.manifest),
        '--identity-input',
        str(Path(__file__)),
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
    if (expected_body := _expected_response_body(scenario)) is not None:
        command.extend([
            '--expected-body-sha256',
            hashlib.sha256(expected_body).hexdigest(),
            '--expected-body-size',
            str(len(expected_body)),
        ])
    if args.server_cpus:
        command.extend(['--server-cpus', args.server_cpus])
    if args.load_cpus:
        command.extend(['--load-cpus', args.load_cpus])
    if args.management_cpus:
        command.extend(['--management-cpus', args.management_cpus])
    command.extend(['--load-warmup-duration', args.load_warmup_duration])
    command.extend(['--max-load-utilization', str(args.max_load_utilization)])
    command.extend([
        '--ambient-cpu-probe-seconds',
        str(args.ambient_cpu_probe_seconds),
        '--max-ambient-cpu-utilization',
        str(args.max_ambient_cpu_utilization),
        '--max-ambient-single-cpu-utilization',
        str(args.max_ambient_single_cpu_utilization),
    ])
    if getattr(args, 'allow_variant_environment_drift', False):
        command.append('--allow-variant-environment-drift')
    return command


def _is_complete(path: Path, expected_identity: dict[str, Any]) -> bool:
    try:
        record = json.loads(path.read_text())
        return (
            record.get('schema_version') == compare.COMPARISON_SCHEMA_VERSION
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
    parser.add_argument('--management-cpus')
    parser.add_argument(
        '--load-warmup-duration', default=compare.DEFAULT_LOAD_WARMUP_DURATION
    )
    parser.add_argument(
        '--max-load-utilization',
        type=float,
        default=compare.MAX_LOAD_UTILIZATION,
    )
    parser.add_argument(
        '--ambient-cpu-probe-seconds',
        type=float,
        default=compare.DEFAULT_AMBIENT_CPU_PROBE_SECONDS,
    )
    parser.add_argument(
        '--max-ambient-cpu-utilization',
        type=float,
        default=MAX_AMBIENT_CPU_UTILIZATION,
    )
    parser.add_argument(
        '--max-ambient-single-cpu-utilization',
        type=float,
        default=MAX_AMBIENT_SINGLE_CPU_UTILIZATION,
    )
    parser.add_argument('--allow-variant-environment-drift', action='store_true')
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
    try:
        compare.parse_duration_seconds(args.load_warmup_duration)
    except argparse.ArgumentTypeError as error:
        parser.error(str(error))
    if args.concurrency is not None and args.concurrency < 1:
        parser.error('--concurrency must be positive')
    if args.trials < 6 or args.trials % 2 or args.warmups < 1:
        parser.error('use an even number of at least six trials and one warmup')
    if not 1 <= args.port_start <= 65_535:
        parser.error('--port-start must be in [1, 65535]')
    if not 0.0 < args.max_load_utilization < 1.0:
        parser.error('--max-load-utilization must be in (0, 1)')
    if args.ambient_cpu_probe_seconds <= 0.0:
        parser.error('--ambient-cpu-probe-seconds must be positive')
    if not 0.0 < args.max_ambient_cpu_utilization <= 2.0:
        parser.error('--max-ambient-cpu-utilization must be in (0, 2]')
    if not 0.0 < args.max_ambient_single_cpu_utilization <= 1.0:
        parser.error('--max-ambient-single-cpu-utilization must be in (0, 1]')
    for value in (args.server_cpus, args.load_cpus, args.management_cpus):
        if value is not None:
            compare.parse_cpu_set(value)
    roles = (args.server_cpus, args.load_cpus, args.management_cpus)
    if any(role is not None for role in roles) and any(role is None for role in roles):
        parser.error(
            'CPU affinity requires --server-cpus, --load-cpus, and --management-cpus'
        )
    if args.management_cpus is not None:
        management_cpus = compare.parse_cpu_set(args.management_cpus)
        if len(management_cpus) != 1:
            parser.error('--management-cpus requires exactly one CPU')
    if all(role is not None for role in roles):
        server_cpus = set(compare.parse_cpu_set(cast('str', args.server_cpus)))
        load_cpus = set(compare.parse_cpu_set(cast('str', args.load_cpus)))
        management_cpus = set(compare.parse_cpu_set(cast('str', args.management_cpus)))
        overlap = (server_cpus & load_cpus) | (
            management_cpus & (server_cpus | load_cpus)
        )
        if overlap:
            parser.error('server/load/management CPU sets must be disjoint')
    timestamp = datetime.now(UTC).strftime('%Y%m%dT%H%M%S.%fZ')
    args.output_dir = args.output_dir or ROOT / 'bench/results/matrix' / timestamp
    return args


def _matrix_identity(
    args: argparse.Namespace,
    selected: Sequence[Scenario],
    tls_paths: tuple[Path, Path] | None,
) -> dict[str, Any]:
    control_artifacts = variant_artifacts(args.control)
    candidate_artifacts = variant_artifacts(args.candidate)
    variant_environment = variant_environment_evidence(
        control_artifacts,
        candidate_artifacts,
        allow_drift=args.allow_variant_environment_drift,
    )
    input_paths = [
        args.manifest,
        Path(__file__),
        Path(compare.__file__),
        ROOT / 'bench/provenance.py',
        ROOT / 'bench/system.py',
        ROOT / 'bench/bench_app.py',
        ROOT / 'bench/k6/ws.js',
    ]
    if tls_paths is not None:
        input_paths.extend(tls_paths)
    inputs = {
        identity['path']: identity
        for path in input_paths
        if (identity := file_identity(path))
    }
    server_cpus = compare.parse_cpu_set(args.server_cpus) if args.server_cpus else None
    load_cpus = compare.parse_cpu_set(args.load_cpus) if args.load_cpus else None
    management_cpus = (
        compare.parse_cpu_set(args.management_cpus) if args.management_cpus else None
    )
    benchmark_system = capture_system_state(server_cpus, load_cpus, management_cpus)
    validate_cpu_roles(server_cpus, load_cpus, management_cpus, benchmark_system)
    return {
        'selection': [scenario.id for scenario in selected],
        'manifest': str(args.manifest.resolve()),
        'inputs': inputs,
        'control': {
            'name': args.control.name,
            'argv': list(args.control.argv),
            'artifacts': control_artifacts,
        },
        'candidate': {
            'name': args.candidate.name,
            'argv': list(args.candidate.argv),
            'artifacts': candidate_artifacts,
        },
        'variant_environment': variant_environment,
        'load_tools': {
            'oha': tool_identity('oha', ('--version',)),
            'k6': tool_identity('k6', ('version',)),
        },
        'settings': {
            'duration': args.duration,
            'concurrency': args.concurrency,
            'trials': args.trials,
            'warmups': args.warmups,
            'seed': args.seed,
            'server_cpus': args.server_cpus,
            'load_cpus': args.load_cpus,
            'management_cpus': args.management_cpus,
            'load_warmup_duration': args.load_warmup_duration,
            'maximum_load_utilization': args.max_load_utilization,
            'ambient_cpu_probe_seconds': args.ambient_cpu_probe_seconds,
            'maximum_ambient_cpu_utilization': args.max_ambient_cpu_utilization,
            'maximum_ambient_single_cpu_utilization': (
                args.max_ambient_single_cpu_utilization
            ),
            'allow_variant_environment_drift': (args.allow_variant_environment_drift),
            'host_noise_mode': compare.host_noise_mode(
                args.server_cpus,
                args.max_ambient_cpu_utilization,
                args.max_ambient_single_cpu_utilization,
            ),
            'port_start': args.port_start,
            'runtime_gil': args.runtime_gil,
        },
        'runtime_environment': result_environment(),
        'benchmark_system': benchmark_system,
        'source': git_metadata(),
    }


def _verify_matrix_identity(
    args: argparse.Namespace,
    selected: Sequence[Scenario],
    tls_paths: tuple[Path, Path] | None,
    frozen: dict[str, Any],
) -> None:
    current = _matrix_identity(args, selected, tls_paths)
    frozen_system = frozen['benchmark_system']
    current_system = current['benchmark_system']
    frozen_without_system = {
        key: value for key, value in frozen.items() if key != 'benchmark_system'
    }
    current_without_system = {
        key: value for key, value in current.items() if key != 'benchmark_system'
    }
    if (
        frozen_without_system != current_without_system
        or not benchmark_system_state_matches(frozen_system, current_system)
    ):
        raise BenchmarkError(
            'frozen matrix identity changed during execution; refusing mixed evidence'
        )


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

    management_cpus = (
        compare.parse_cpu_set(args.management_cpus) if args.management_cpus else None
    )
    try:
        pin_benchmark_driver(management_cpus)
    except BenchmarkError as error:
        raise SystemExit(str(error)) from error

    args.output_dir.mkdir(parents=True, exist_ok=True)
    tls_paths: tuple[Path, Path] | None = None
    if any(
        scenario.transport == 'tls'
        and unsupported_reason(scenario, gil_enabled=gil_enabled) is None
        for scenario in selected
    ):
        tls_paths = _tls_certificate(args.output_dir / 'tls')
    try:
        matrix_identity = _matrix_identity(args, selected, tls_paths)
    except BenchmarkError as error:
        raise SystemExit(str(error)) from error
    matrix_path = args.output_dir / 'matrix.json'
    noise_mode = compare.host_noise_mode(
        args.server_cpus,
        args.max_ambient_cpu_utilization,
        args.max_ambient_single_cpu_utilization,
    )
    variant_environment = matrix_identity['variant_environment']
    # The frozen matrix_identity is the single stored copy of the selection,
    # variants, environment evidence, and host-noise settings.
    record: dict[str, Any] = {
        'schema_version': MATRIX_SCHEMA_VERSION,
        'status': 'dry-run' if args.dry_run else 'running',
        'created_at': datetime.now(UTC).isoformat(),
        'manifest': str(args.manifest),
        'manifest_sha256': hashlib.sha256(args.manifest.read_bytes()).hexdigest(),
        'manifest_data': raw_manifest,
        'matrix_identity': matrix_identity,
        'scenarios': {},
    }
    failed = False
    for index, scenario in enumerate(selected):
        _verify_matrix_identity(args, selected, tls_paths, matrix_identity)
        output = args.output_dir / f'{scenario.id}.json'
        socket_path = args.output_dir / f'{scenario.id}.sock'
        reason = unsupported_reason(scenario, gil_enabled=gil_enabled)
        if reason is not None:
            record['scenarios'][scenario.id] = {
                'status': 'skipped',
                'host_noise_mode': noise_mode,
                'variant_environment_mode': variant_environment['mode'],
                'reason': reason,
            }
            durable_json(matrix_path, record)
            continue
        if scenario.transport == 'tls':
            if tls_paths is None:
                raise RuntimeError('supported TLS scenario has no frozen certificate')
            cert, key = tls_paths
        else:
            cert, key = None, None
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
            'host_noise_mode': noise_mode,
            'variant_environment_mode': variant_environment['mode'],
            'result': str(output),
            'compare_argv': compare_argv,
        }
        record['scenarios'][scenario.id] = entry
        if _is_complete(output, expected_identity):
            entry['status'] = 'resumed-complete'
        elif not args.dry_run:
            entry['status'] = 'running'
            durable_json(matrix_path, record)
            return_code = compare.main(compare_argv)
            entry['status'] = 'complete' if return_code == 0 else 'failed'
            failed |= return_code != 0
        durable_json(matrix_path, record)

    _verify_matrix_identity(args, selected, tls_paths, matrix_identity)
    record['status'] = 'failed' if failed else 'dry-run' if args.dry_run else 'complete'
    record['completed_at'] = datetime.now(UTC).isoformat()
    durable_json(matrix_path, record)
    if noise_mode == 'diagnostic-unpinned':
        print(
            'host_noise_mode: DIAGNOSTIC_UNPINNED '
            '(no role-aware ambient/interference gate)'
        )
    elif noise_mode == 'diagnostic-pinned-noisy':
        print(
            'host_noise_mode: DIAGNOSTIC_PINNED_NOISY '
            '(CPU roles and interference were recorded, but relaxed limits do not '
            'support publication-grade claims)'
        )
    if variant_environment['mode'] == 'confounded-opt-out':
        print(
            'variant_environment_mode: CONFOUNDED_OPT_OUT '
            '(Python runtime or shared dependencies differ)'
        )
    print(f'matrix evidence: {matrix_path}')
    return int(failed)


if __name__ == '__main__':
    raise SystemExit(main())
