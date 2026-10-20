"""gRPC plans and evidence collected by the existing cross-server harness."""

from __future__ import annotations

import asyncio
import hashlib
import importlib.metadata
import json
import math
import ssl
import subprocess
import sys
import time
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

from bench._core import (
    BenchmarkError,
    Metrics,
    ResponseError,
    Scenario,
    _run,
    allocated_listener,
    measure_peak_memory,
    peer_settings,
    running_server,
    write_json,
)
from bench.evidence import file_identity, run_identity, verify_workers

ROOT = Path(__file__).resolve().parents[1]
SERVERS = ('h2corn', 'hypercorn', 'grpcio', 'grpclib')
LABELS = {
    'h2corn': 'Connect / h2corn',
    'hypercorn': 'Connect / Hypercorn',
    'grpcio': 'grpc.aio',
    'grpclib': 'grpclib',
}


@dataclass(frozen=True)
class RpcScenario:
    case: str
    shape: str
    size: int
    messages: int
    concurrency: int
    tls: bool = False
    grpclib: bool = False
    type: Literal['grpc'] = 'grpc'
    workers: int = 1

    @property
    def name(self) -> str:
        return f'gRPC {self.case}{" TLS" if self.tls else ""}'

    def slug(self) -> str:
        return f'benchmark_grpc_{self.case.lower().replace("-", "_")}'

    @property
    def unit(self) -> str:
        return 'RPC/s' if self.shape in {'unary', 'upload'} else 'messages/s'

    @property
    def workload(self) -> str:
        return (
            f'{self.shape}; {self.messages} message(s), {self.size} payload bytes; '
            f'1 H2 connection, {self.concurrency} concurrent RPC(s); identity compression'
        )


def scenarios() -> tuple[RpcScenario, ...]:
    return (
        RpcScenario('U0', 'unary', 0, 1, 1),
        RpcScenario('U32', 'unary', 32, 1, 64, grpclib=True),
        RpcScenario('U64K', 'unary', 65536, 1, 16, grpclib=True),
        RpcScenario('Upload', 'upload', 65536, 16, 8),
        RpcScenario('Download', 'download', 65536, 16, 8),
        RpcScenario('Duplex', 'duplex', 256, 64, 8, grpclib=True),
        RpcScenario('TLS-U32', 'unary', 32, 1, 64, tls=True),
        RpcScenario('TLS-Duplex', 'duplex', 256, 64, 8, tls=True),
    )


def process_snapshot(pids: set[int]) -> dict[str, Any]:
    snapshots = {}
    for pid in sorted(pids):
        # comm may contain spaces and closing parentheses.
        fields = Path(f'/proc/{pid}/stat').read_text().rpartition(') ')[2].split()
        snapshots[str(pid)] = {
            'start_ticks': int(fields[19]),
            'user_ticks': int(fields[11]),
            'system_ticks': int(fields[12]),
            'threads': sorted(int(p.name) for p in Path(f'/proc/{pid}/task').iterdir()),
        }
    return snapshots


def verify_wheel(wheel: Path, distribution: str) -> dict[str, Any]:
    installed = importlib.metadata.distribution(distribution)
    members = {}
    with zipfile.ZipFile(wheel) as archive:
        for name in archive.namelist():
            if name.endswith(('.py', '.so', '.pyd')) and '.dist-info/' not in name:
                digest = hashlib.sha256(archive.read(name)).hexdigest()
                path = Path(str(installed.locate_file(name)))
                if not path.is_file() or file_identity(path)['sha256'] != digest:
                    raise BenchmarkError(
                        f'{distribution} installed bytes differ from {wheel}: {name}'
                    )
                members[name] = digest
    if not members:
        raise BenchmarkError(f'{wheel} contains no runtime files')
    return {**file_identity(wheel), 'members': members}


def release_artifacts(
    directory: Path,
    connect_wheel: Path | None,
    development_revision: str | None,
    connect_source: Path | None,
    selections: dict[str, Any],
) -> dict[str, Any]:
    """Verify serving packages against published wheels, before starting trials."""
    from packaging.tags import sys_tags
    from packaging.utils import parse_wheel_filename
    from packaging.version import Version

    tags = set(sys_tags())
    directory.mkdir()
    artifacts = {}
    for name in (
        'connectrpc',
        'hypercorn',
        'grpcio',
        'grpclib',
        'protobuf-py',
        'protobuf-py-ext',
    ):
        version = importlib.metadata.version(name)
        metadata_path = directory / f'{name}.json'
        _run(
            [
                'curl',
                '-fsSL',
                '--max-time',
                '30',
                f'https://pypi.org/pypi/{name}/json',
                '-o',
                str(metadata_path),
            ],
            35,
        )
        metadata = json.loads(metadata_path.read_text())
        development = name == 'connectrpc' and development_revision is not None
        if not development and version != metadata['info']['version']:
            decision = selections.get(name, {})
            newer = {
                v
                for v, files in metadata['releases'].items()
                if files
                and Version(v) > Version(version)
                and not Version(v).is_prerelease
                and not Version(v).is_devrelease
            }
            rejected = {r['version']: r for r in decision.get('rejected_releases', [])}
            if decision.get('version') != version or set(rejected) != newer:
                raise BenchmarkError(
                    f'{name} {version} requires --comparator-selection evidence '
                    'for every newer stable PyPI release'
                )
            for rejected_release in rejected.values():
                evidence = rejected_release['evidence']
                if (
                    not rejected_release['reason']
                    or file_identity(Path(evidence['path']))['sha256']
                    != evidence['sha256']
                ):
                    raise BenchmarkError(
                        f'{name} older-release selection has missing failure evidence'
                    )
            _run(
                [
                    'curl',
                    '-fsSL',
                    '--max-time',
                    '30',
                    f'https://pypi.org/pypi/{name}/{version}/json',
                    '-o',
                    str(metadata_path),
                ],
                35,
            )
            metadata = json.loads(metadata_path.read_text())
        candidates = [
            f
            for f in metadata['urls']
            if f['filename'].endswith('.whl')
            and tags.intersection(parse_wheel_filename(f['filename'])[3])
        ]
        if not candidates:
            raise BenchmarkError(f'no published {name} wheel matches this runtime')
        candidate = candidates[0]
        wheel = (
            connect_wheel
            if name == 'connectrpc' and connect_wheel
            else directory / candidate['filename']
        )
        if wheel == directory / candidate['filename']:
            _run(
                [
                    'curl',
                    '-fsSL',
                    '--max-time',
                    '60',
                    candidate['url'],
                    '-o',
                    str(wheel),
                ],
                65,
            )
        artifact = verify_wheel(wheel, name)
        if development:
            assert development_revision is not None
            if connect_source is None:
                raise BenchmarkError(
                    'development Connect requires its source repository'
                )
            revision = subprocess.check_output(
                ['git', '-C', str(connect_source), 'rev-parse', development_revision],
                text=True,
                timeout=15,
            ).strip()
            for member, digest in artifact['members'].items():
                source = subprocess.check_output(
                    [
                        'git',
                        '-C',
                        str(connect_source),
                        'show',
                        f'{revision}:src/{member}',
                    ],
                    timeout=15,
                )
                if hashlib.sha256(source).hexdigest() != digest:
                    raise BenchmarkError(
                        f'development Connect wheel differs from {revision}: {member}'
                    )
            artifact['source_revision'] = revision
        if not development and artifact['sha256'] != candidate['digests']['sha256']:
            raise BenchmarkError(f'{name} wheel differs from its PyPI digest')
        artifacts[name] = {
            **artifact,
            'version': version,
            'kind': 'development' if development else 'release',
            'development_revision': development_revision if development else None,
            'pypi_url': candidate['url'],
            'pypi_sha256': candidate['digests']['sha256'],
            'selection': selections.get(name),
        }
    return artifacts


def prepare(
    directory: Path,
    wheel: Path | None,
    *,
    connect_wheel: Path | None = None,
    development_revision: str | None = None,
    connect_source: Path | None = None,
    comparator_selection: Path | None = None,
) -> dict[str, Any]:
    if development_revision and connect_wheel is None:
        raise BenchmarkError(
            'development classification requires an explicit Connect wheel'
        )
    identity = run_identity(
        ROOT.parent, wheel, commands_required=(), tools_required=('rustc',)
    )
    if identity['python']['free_threaded'] or not identity['python']['gil_enabled']:
        raise BenchmarkError('the initial gRPC comparison requires regular CPython')
    selections = (
        json.loads(comparator_selection.read_text()) if comparator_selection else {}
    )
    identity['rpc_artifacts'] = release_artifacts(
        directory / 'packages',
        connect_wheel,
        development_revision,
        connect_source,
        selections,
    )
    if comparator_selection:
        identity['comparator_selection'] = file_identity(comparator_selection)
    client_directory = ROOT / 'bench/grpc_client'
    binary = directory / 'grpc-client'
    command = ['go', 'build', '-buildvcs=false', '-trimpath', '-o', str(binary), '.']
    subprocess.run(
        command, cwd=client_directory, check=True, timeout=180, capture_output=True
    )
    descriptor = directory / 'notebook.pb'
    subprocess.run(
        ['buf', 'build', '--as-file-descriptor-set', '-o', str(descriptor)],
        cwd=ROOT / 'examples/grpc',
        check=True,
        timeout=30,
        capture_output=True,
    )
    cert = directory / 'tls'
    cert.mkdir()
    subprocess.run(
        [
            'openssl',
            'req',
            '-x509',
            '-newkey',
            'rsa:2048',
            '-nodes',
            '-keyout',
            str(cert / 'key.pem'),
            '-out',
            str(cert / 'cert.pem'),
            '-days',
            '2',
            '-subj',
            '/CN=localhost',
            '-addext',
            'subjectAltName=DNS:localhost',
            '-addext',
            'basicConstraints=critical,CA:TRUE',
        ],
        check=True,
        timeout=30,
        capture_output=True,
    )
    identity['rpc_client'] = {
        **file_identity(binary),
        'build_command': command,
        'build_info': subprocess.check_output(
            ['go', 'version', '-m', str(binary)], text=True, timeout=15
        ),
        'sources': {
            str(p.relative_to(ROOT)): file_identity(p)
            for p in sorted(client_directory.iterdir())
            if p.suffix in {'.go', '.mod', '.sum'}
        },
    }
    identity['descriptor'] = file_identity(descriptor)
    identity['rpc_sources'] = {
        str(p.relative_to(ROOT)): file_identity(p)
        for base in (ROOT / 'examples/grpc', ROOT / 'bench')
        for p in sorted(base.rglob('*'))
        if p.is_file() and p.suffix in {'.proto', '.py'} and 'results' not in p.parts
    }
    identity['tls_certificate'] = file_identity(cert / 'cert.pem')
    return identity


def load(plan: dict[str, Any], identity: dict[str, Any], path: Path) -> dict[str, Any]:
    write_json(path.with_suffix('.plan.json'), plan)
    stdout = _run(
        [identity['rpc_client']['path'], str(path.with_suffix('.plan.json'))],
        plan['duration_seconds'] + plan['drain_seconds'] + 15,
        evidence_path=path.with_suffix('.process.json'),
    )
    raw = json.loads(stdout)
    write_json(path, raw)
    validate_outcomes(raw, plan)
    return raw


def validate_outcomes(raw: dict[str, Any], plan: dict[str, Any]) -> None:
    buckets = (
        'verified_ok',
        'expected_cancelled',
        'deadline_exceeded',
        'other_non_ok',
        'invalid_response',
        'unfinished',
    )
    if raw['attempted'] != raw['started'] + raw['admission_failures'] or raw[
        'started'
    ] != sum(raw[key] for key in buckets):
        raise BenchmarkError('gRPC call outcomes do not reconcile')
    if plan.get('mode') == 'cancel':
        if raw['expected_cancelled'] != 1 or raw['invalid_response']:
            raise ResponseError(f'gRPC cancellation did not settle: {raw["errors"]}')
        return
    if (
        not raw['verified_ok']
        or raw['verified_ok'] != raw['started']
        or raw['admission_failures']
        or (plan['requests'] and raw['verified_ok'] != plan['requests'])
    ):
        raise ResponseError(f'gRPC calls did not all complete and verify OK: {raw}')
    messages = 1 if plan['shape'] in {'unary', 'upload'} else plan['messages']
    payload = plan['size'] * (
        plan['messages'] if plan['shape'] == 'upload' else messages
    )
    if (
        raw['verified_messages'] != raw['verified_ok'] * messages
        or raw['verified_payload_bytes'] != raw['verified_ok'] * payload
    ):
        raise ResponseError(
            'gRPC useful message/byte totals do not match completed calls'
        )
    if not math.isfinite(raw['elapsed_seconds']) or raw['elapsed_seconds'] <= 0:
        raise BenchmarkError('gRPC client reported an invalid duration')
    if raw['connections'] != 1:
        raise BenchmarkError('gRPC client did not use one connection')
    if (
        sum(raw['statuses'].values()) != raw['started']
        or raw['statuses'].get('OK') != raw['verified_ok']
    ):
        raise BenchmarkError('gRPC status counts do not reconcile with completed calls')
    if plan['ca'] and (not raw.get('tls') or raw['tls'].get('alpn') != 'h2'):
        raise BenchmarkError('gRPC TLS connection did not negotiate H2')


def measure_cell(
    name: str,
    scenario: RpcScenario,
    duration: str,
    warmup: str,
    *,
    evidence_directory: Path,
    identity: dict[str, Any],
    qualification: bool = False,
    cancellation: bool = False,
) -> dict[str, Any]:
    from bench._core import duration_seconds
    from bench.bench import TrialError

    evidence_directory.mkdir(parents=True)
    receipt_path = evidence_directory / 'startup.json'
    service_plan = {**asdict(scenario), 'hold_after_first': cancellation}
    write_json(evidence_directory / 'service-plan.json', service_plan)
    trial: dict[str, Any] = {
        'server': name,
        'status': 'running',
        'phase': 'startup',
        'scenario': asdict(scenario),
    }
    started = time.monotonic()
    try:
        with allocated_listener(Scenario('gRPC listener', 1, 'h2')) as (_, listener):
            command = [
                sys.executable,
                '-m',
                'bench.grpc_app',
                '--server',
                name,
                '--plan',
                str(evidence_directory / 'service-plan.json'),
                '--receipt',
                str(receipt_path),
            ]
            if name != 'grpcio':
                command += ['--listener-fd', str(listener.fileno())]
            if scenario.tls:
                command += [
                    '--cert',
                    str(Path(identity['tls_certificate']['path']).parent),
                ]
            trial['command'] = command

            def ready(pid: int) -> bool:
                if not receipt_path.is_file():
                    return False
                receipt = json.loads(receipt_path.read_text())
                if receipt['pid'] != pid:
                    raise BenchmarkError(
                        'gRPC startup receipt belongs to a different process'
                    )
                return True

            with (
                (evidence_directory / 'server.log').open('w') as log,
                running_server(
                    command,
                    workers=1,
                    listener_fd=listener.fileno() if name != 'grpcio' else None,
                    readiness=ready,
                    include_descendants=True,
                    output=log.fileno(),
                ) as server,
            ):
                receipt = json.loads(receipt_path.read_text())
                trial['workers'] = {str(receipt['pid']): receipt}
                verify_workers(trial, identity)
                context = None
                if scenario.tls:
                    context = ssl.create_default_context(
                        cafile=identity['tls_certificate']['path']
                    )
                    context.set_alpn_protocols(['h2'])
                trial['http2_peer'] = peer_settings(
                    Scenario('gRPC settings', 1, 'h2', port=receipt['port']),
                    tls_context=context,
                )
                trial['http2_peer']['scope'] = 'separate pre-load connection'
                plan = {
                    'endpoint': f'127.0.0.1:{receipt["port"]}',
                    'descriptor': identity['descriptor']['path'],
                    'ca': identity['tls_certificate']['path'] if scenario.tls else '',
                    'case': scenario.case,
                    'shape': scenario.shape,
                    'size': scenario.size,
                    'messages': scenario.messages,
                    'concurrency': scenario.concurrency,
                    'duration_seconds': duration_seconds(duration),
                    'requests': 0,
                    'drain_seconds': 15,
                    'mode': '',
                    'verify_identity': False,
                }
                trial['phase'] = 'correctness-before'
                probe = {
                    **plan,
                    'requests': 1,
                    'concurrency': 1,
                    'duration_seconds': 0,
                    'mode': 'duplex-probe' if scenario.shape == 'duplex' else '',
                    'verify_identity': True,
                }
                if cancellation:
                    raw = load(
                        {**probe, 'mode': 'cancel', 'drain_seconds': 3},
                        identity,
                        evidence_directory / 'cancellation.json',
                    )
                    trial.update(
                        status='completed', phase='completed', qualification=raw
                    )
                    return {}
                load(probe, identity, evidence_directory / 'before.json')
                trial['phase'] = 'warmup'
                if not qualification:
                    load(
                        {**plan, 'duration_seconds': duration_seconds(warmup)},
                        identity,
                        evidence_directory / 'warmup.json',
                    )
                if qualification:
                    plan = probe
                before = process_snapshot(server.process_pids())
                trial['phase'] = 'load'
                raw: dict[str, Any] = {}

                def action() -> Metrics:
                    nonlocal raw
                    raw = load(plan, identity, evidence_directory / 'load.json')
                    return {
                        'rps': (
                            raw['verified_ok']
                            if scenario.unit == 'RPC/s'
                            else raw['verified_messages']
                        )
                        / raw['elapsed_seconds'],
                        'completed': raw['verified_ok'],
                        'latency_percentiles': raw['latency_percentiles'],
                    }

                metrics, peak = measure_peak_memory(
                    server, action, evidence_path=evidence_directory / 'memory.json'
                )
                after = process_snapshot(server.process_pids())
                if before.keys() != after.keys() or any(
                    before[p]['start_ticks'] != after[p]['start_ticks'] for p in before
                ):
                    raise BenchmarkError(
                        'serving process population changed during gRPC load'
                    )
                trial['processes_before'] = before
                trial['processes_after'] = after
                trial['phase'] = 'correctness-after'
                final = load(probe, identity, evidence_directory / 'after.json')
                worker = json.loads(final['server_identity'])
                trial['workers'] = {str(worker['pid']): worker}
                verify_workers(trial, identity)
                if worker['pid'] not in server.process_pids():
                    raise BenchmarkError(
                        'gRPC serving receipt belongs to an unowned process'
                    )
                if worker['active'] or worker['readers']:
                    raise ResponseError('gRPC work remains active after complete calls')
                measured = {
                    **metrics,
                    'peak_memory_bytes': peak,
                    'post_settlement_pss_bytes': server.memory_bytes(),
                    'rpc_rate': raw['verified_ok'] / raw['elapsed_seconds'],
                    'message_rate': raw['verified_messages'] / raw['elapsed_seconds'],
                    'payload_bytes_rate': raw['verified_payload_bytes']
                    / raw['elapsed_seconds'],
                }
                trial.update(
                    metrics=measured,
                    correctness_before='passed',
                    correctness_after='passed',
                    supervisor_pid=server.pid,
                    tcp_nodelay=True if name != 'grpcio' else None,
                    tcp_nodelay_source='inherited listener'
                    if name != 'grpcio'
                    else 'runtime-owned socket; not directly observed',
                    listener_owner='runtime-port-zero'
                    if name == 'grpcio'
                    else 'harness-inherited',
                )
    except (OSError, ValueError, RuntimeError) as error:
        trial.update(status='failed', error=str(error))
        raise TrialError(
            trial['phase'], f'{name} {trial["phase"]} failed: {error}'
        ) from error
    else:
        trial.update(status='completed', phase='completed')
        return measured
    finally:
        trial['elapsed_seconds'] = time.monotonic() - started
        write_json(evidence_directory / 'trial.json', trial)


def run_suite(args: Any) -> Path:
    from bench.bench import (
        assert_publishable,
        plot_results,
        publish,
        run_scenario,
        system_summary,
    )

    selected = [
        s for s in scenarios() if not args.rpc_cases or s.case in args.rpc_cases
    ]
    servers = args.servers or list(SERVERS)
    directory = (
        args.run_directory
        or args.output_directory.resolve() / 'runs' / f'grpc-{time.time_ns()}'
    )
    directory.mkdir(parents=True, exist_ok=False)
    (directory / 'raw').mkdir()
    (directory / 'plots').mkdir()
    identity = prepare(
        directory,
        args.wheel,
        connect_wheel=args.connect_wheel,
        development_revision=args.connect_development_revision,
        connect_source=args.connect_source,
        comparator_selection=args.comparator_selection,
    )
    kind = identity['rpc_artifacts']['connectrpc']['kind']
    write_json(directory / 'identity.json', identity)
    write_json(
        directory / 'profile.json',
        {'profile': 'grpc', 'benchmark_family': 'grpc', 'status': 'running'},
    )
    deadline = time.monotonic() + args.suite_budget

    def qualify() -> list[dict[str, Any]]:
        from bench.bench import TrialError

        qualifications = []
        for scenario in selected:
            for name in servers:
                if name == 'grpclib' and not scenario.grpclib:
                    continue
                path = directory / 'evidence' / scenario.slug() / name
                try:
                    measure_cell(
                        name,
                        scenario,
                        args.duration,
                        args.warmup_duration,
                        evidence_directory=path,
                        identity=identity,
                        qualification=True,
                    )
                except TrialError as error:
                    if not isinstance(error.__cause__, ResponseError):
                        raise
                    qualifications.append({
                        'case': scenario.case,
                        'server': name,
                        'status': 'failed',
                        'error': str(error),
                    })
                else:
                    qualifications.append({
                        'case': scenario.case,
                        'server': name,
                        'status': 'passed',
                    })
        for name in ('h2corn', 'hypercorn', 'grpcio'):
            if name not in servers:
                continue
            cancellation = RpcScenario('Cancellation', 'download', 32, 2, 1)
            try:
                measure_cell(
                    name,
                    cancellation,
                    args.duration,
                    args.warmup_duration,
                    evidence_directory=directory / 'evidence/cancellation' / name,
                    identity=identity,
                    qualification=True,
                    cancellation=True,
                )
            except TrialError as error:
                if not isinstance(error.__cause__, ResponseError):
                    raise
                qualifications.append({
                    'case': 'Cancellation',
                    'server': name,
                    'status': 'failed',
                    'error': str(error),
                })
            else:
                qualifications.append({
                    'case': 'Cancellation',
                    'server': name,
                    'status': 'passed',
                })
        return qualifications

    qualifications = {
        'application_artifact_kind': kind,
        'connect_sha256': identity['rpc_artifacts']['connectrpc']['sha256'],
        'cells': qualify(),
        'framework': asyncio.run(qualify_framework()),
    }
    write_json(directory / 'qualification.json', qualifications)
    identity['rpc_qualification'] = qualifications['cells']
    write_json(directory / 'identity.json', identity)
    if args.qualify_only:
        write_json(
            directory / 'profile.json',
            {
                'profile': 'grpc',
                'benchmark_family': 'grpc',
                'status': 'qualified',
                'application_artifact_kind': kind,
            },
        )
        return directory
    for scenario in selected:
        record = run_scenario(
            scenario,
            servers,
            duration=args.duration,
            warmup=args.warmup_duration,
            max_trials=args.max_trials,
            deadline=min(deadline, time.monotonic() + args.scenario_budget),
            hypercorn_config=directory / 'unused.toml',
            evidence_directory=directory / 'evidence' / scenario.slug(),
            identity=identity,
        )
        record.update(
            benchmark_family='grpc',
            application_artifact_kind=kind,
            stack_labels={
                **LABELS,
                **(
                    {
                        n: LABELS[n]
                        + f' (development {args.connect_development_revision[:7]})'
                        for n in ('h2corn', 'hypercorn')
                    }
                    if kind == 'development'
                    else {}
                ),
            },
            rate_unit=scenario.unit,
            reference_label='Connect / h2corn',
            rpc_case=scenario.case,
            qualification=qualifications,
            system_summary=system_summary(),
            network_profile='grpc',
            duration=args.duration,
            warmup_duration=args.warmup_duration,
            package_versions={
                n: importlib.metadata.version(n)
                for n in (
                    'connectrpc',
                    'hypercorn',
                    'grpcio',
                    'grpclib',
                    'protobuf-py',
                    'protobuf-py-ext',
                )
            },
            identity='../evidence/grpc/identity.json',
            evidence=f'../evidence/grpc/trials/{scenario.slug()}',
        )
        messages_per_rpc = (
            scenario.messages if scenario.shape in {'download', 'duplex'} else 1
        )
        for result in record['aggregate'].values():
            result['rpc_rate'] = result['rps'] / messages_per_rpc
        write_json(directory / 'raw' / f'{scenario.slug()}.json', record)
        plot_results(record, directory / 'plots' / f'{scenario.slug()}.svg')
    write_json(
        directory / 'profile.json',
        {'profile': 'grpc', 'benchmark_family': 'grpc', 'status': 'completed'},
    )
    if args.publish:
        assert_publishable([directory])
        publish([directory])
    return directory


def assert_rpc_publishable(directory: Path) -> None:
    identity = json.loads((directory / 'identity.json').read_text())
    qualification_path = directory / 'qualification.json'
    if not qualification_path.is_file():
        raise BenchmarkError(
            'gRPC publication requires retained qualification outcomes'
        )
    qualification = json.loads(qualification_path.read_text())
    connect = identity['rpc_artifacts']['connectrpc']
    if (
        connect['kind'] != 'release'
        or qualification.get('application_artifact_kind') != 'release'
        or qualification.get('connect_sha256') != connect['sha256']
    ):
        raise BenchmarkError(
            'gRPC publication requires matching released application identity'
        )
    records = [json.loads(p.read_text()) for p in (directory / 'raw').glob('*.json')]
    if (
        {r.get('rpc_case') for r in records} != {s.case for s in scenarios()}
        or len(records) != 8
        or any(
            r['planned_trials'] != 12 or set(r['requested_servers']) != set(SERVERS)
            for r in records
        )
    ):
        raise BenchmarkError(
            'gRPC publication requires the complete eight-row, twelve-block matrix'
        )
    expected = {
        (s.case, name)
        for s in scenarios()
        for name in SERVERS
        if name != 'grpclib' or s.grpclib
    }
    cells = {(c['case'], c['server']): c for c in qualification['cells']}
    if not expected.issubset(cells) or any(
        cells[key]['status'] not in {'passed', 'failed'} for key in expected
    ):
        raise BenchmarkError('gRPC publication has missing per-shape qualification')
    for key in expected:
        scenario = next(s for s in scenarios() if s.case == key[0])
        trial_path = directory / 'evidence' / scenario.slug() / key[1] / 'trial.json'
        if not trial_path.is_file() or json.loads(trial_path.read_text())['status'] != (
            'completed' if cells[key]['status'] == 'passed' else 'failed'
        ):
            raise BenchmarkError(f'gRPC qualification evidence is missing for {key}')
    if {c['case'] for c in qualification['framework']} != {
        'reserved-flag',
        'truncated-suffix',
        'expired-deadline',
    }:
        raise BenchmarkError('gRPC publication has missing framework qualification')


async def qualify_framework() -> list[dict[str, Any]]:
    """Bounded public-ASGI witnesses, separate from transport qualification."""
    from asgi_lifespan import LifespanManager

    from bench.grpc_app import Work, connect_app
    from examples.grpc.gen.h2corn.interop.v1.notebook_pb import GetNoteRequest

    body = GetNoteRequest(note_id='x' * 32).to_binary()
    valid = b'\0' + len(body).to_bytes(4, 'big') + body
    cases = (
        ('reserved-flag', bytes([2]) + valid[1:], [], '13'),
        ('truncated-suffix', valid + b'\0', [], '13'),
        ('expired-deadline', valid, [(b'grpc-timeout', b'0n')], '4'),
    )
    results = []
    for name, request, extra_headers, expected in cases:
        work = Work({'size': 32, 'messages': 1, 'case': 'qualification'})
        app = connect_app(work)
        sent: list[dict[str, Any]] = []
        delivered = False

        async def receive() -> dict[str, Any]:
            nonlocal delivered
            if not delivered:
                delivered = True
                return {'type': 'http.request', 'body': request, 'more_body': False}  # ruff: ignore[function-uses-loop-variable] -- Invocation settles before the next case.
            await asyncio.Event().wait()
            raise AssertionError

        async def send(message: dict[str, Any]) -> None:
            sent.append(message)  # ruff: ignore[function-uses-loop-variable] -- Invocation settles before the next case.

        scope = {
            'type': 'http',
            'asgi': {'version': '3.0', 'spec_version': '2.4'},
            'http_version': '2',
            'method': 'POST',
            'scheme': 'http',
            'path': '/h2corn.interop.v1.NotebookService/GetNote',
            'raw_path': b'/h2corn.interop.v1.NotebookService/GetNote',
            'query_string': b'',
            'root_path': '',
            'headers': [
                (b'content-type', b'application/grpc'),
                (b'te', b'trailers'),
                *extra_headers,
            ],
            'extensions': {'http.response.trailers': {}},
            'state': {},
        }
        async with asyncio.timeout(5), LifespanManager(app):
            await app(scope, receive, send)
        statuses = [
            value.decode()
            for message in sent
            for key, value in message.get('headers', [])
            if key == b'grpc-status'
        ]
        results.append({
            'case': name,
            'expected_status': expected,
            'observed_statuses': statuses,
            'status': 'passed'
            if statuses == [expected] and work.finished == 0
            else 'failed',
            'handler_calls': work.finished,
        })
    return results
