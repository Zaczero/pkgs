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
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from collections.abc import Sequence

from bench._core import (
    BenchmarkError,
    DeadlineError,
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
from bench.sampling import (
    CalibrationPoint,
    calibration_duration,
    calibration_record,
    calibration_status,
)

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
    _run(
        command,
        180,
        evidence_path=directory / 'grpc-client-build.process.json',
        cwd=client_directory,
    )
    descriptor = directory / 'notebook.pb'
    _run(
        ['buf', 'build', '--as-file-descriptor-set', '-o', str(descriptor)],
        30,
        evidence_path=directory / 'buf-build.process.json',
        cwd=ROOT / 'examples/grpc',
    )
    cert = directory / 'tls'
    cert.mkdir()
    _run(
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
        30,
        evidence_path=directory / 'tls-certificate.process.json',
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


def calibrate_rpc_server(
    name: str,
    scenario: RpcScenario,
    *,
    plan: dict[str, Any],
    probe: dict[str, Any],
    warmup: str,
    windows: Sequence[float],
    identity: dict[str, Any],
    server: Any,
    evidence_directory: Path,
) -> dict[str, Any]:
    """Measure adaptive windows through one verified serving process."""
    from bench._core import duration_seconds

    load(
        {**plan, 'duration_seconds': duration_seconds(warmup)},
        identity,
        evidence_directory / 'warmup.json',
    )
    before = process_snapshot(server.process_pids())
    points: list[CalibrationPoint] = []
    for window in windows:
        window_directory = evidence_directory / f'window-{window:g}s'
        raw = load(
            {**plan, 'duration_seconds': window},
            identity,
            window_directory / 'load.json',
        )
        useful = (
            raw['verified_ok'] if scenario.unit == 'RPC/s' else raw['verified_messages']
        )
        points.append(
            CalibrationPoint(
                duration_seconds=window,
                rate=useful / raw['elapsed_seconds'],
                elapsed_seconds=raw['elapsed_seconds'],
                completed=raw['verified_ok'],
            )
        )
        if calibration_duration(points, fixed_count=False) is None:
            break
    after = process_snapshot(server.process_pids())
    if before.keys() != after.keys() or any(
        before[pid]['start_ticks'] != after[pid]['start_ticks'] for pid in before
    ):
        raise BenchmarkError('serving process population changed during calibration')
    final = load(probe, identity, evidence_directory / 'after.json')
    worker = json.loads(final['server_identity'])
    if worker['pid'] not in server.process_pids():
        raise BenchmarkError('gRPC serving receipt belongs to an unowned process')
    if worker['active'] or worker['readers']:
        raise ResponseError('gRPC work remains active after calibration')
    status, selected_duration = calibration_status(points, fixed_count=False)
    return {
        'server': name,
        'status': status,
        'selected_duration_seconds': selected_duration,
        'windows': calibration_record(points),
        'processes_before': before,
        'processes_after': after,
        'server_identity': worker,
    }


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
    calibration_windows: Sequence[float] | None = None,
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
                if calibration_windows is not None:
                    trial['phase'] = 'calibration'
                    measured = calibrate_rpc_server(
                        name,
                        scenario,
                        plan=plan,
                        probe=probe,
                        warmup=warmup,
                        windows=calibration_windows,
                        identity=identity,
                        server=server,
                        evidence_directory=evidence_directory,
                    )
                    worker = measured.pop('server_identity')
                    trial['workers'] = {str(worker['pid']): worker}
                    verify_workers(trial, identity)
                    trial.update(
                        calibration=measured,
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
                else:
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
                        before[p]['start_ticks'] != after[p]['start_ticks']
                        for p in before
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
                        raise ResponseError(
                            'gRPC work remains active after complete calls'
                        )
                    measured = {
                        **metrics,
                        'peak_memory_bytes': peak,
                        'post_settlement_pss_bytes': server.memory_bytes(),
                        'rpc_rate': raw['verified_ok'] / raw['elapsed_seconds'],
                        'message_rate': raw['verified_messages']
                        / raw['elapsed_seconds'],
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
    except DeadlineError as error:
        trial.update(status='failed', error=str(error))
        raise
    except (OSError, ValueError, RuntimeError) as error:
        trial.update(status='failed', error=str(error))
        raise TrialError(
            trial['phase'], f'{name} {trial["phase"]} failed: {error}'
        ) from error
    else:
        trial.update(status='completed', phase='completed')
        measured['elapsed_seconds'] = time.monotonic() - started
        return measured
    finally:
        trial['elapsed_seconds'] = time.monotonic() - started
        write_json(evidence_directory / 'trial.json', trial)


def assert_rpc_publishable(directory: Path) -> None:
    profile = json.loads((directory / 'profile.json').read_text())
    if (
        profile.get('benchmark_family') != 'grpc'
        or profile.get('profile') != 'grpc'
        or profile.get('status') not in {'completed', 'completed_with_failures'}
    ):
        raise BenchmarkError('gRPC profile did not complete')
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
    plan_path = directory / 'suite-plan.json'
    if not plan_path.is_file():
        raise BenchmarkError('gRPC publication has no frozen adaptive plan')
    suite_plan = json.loads(plan_path.read_text())
    if suite_plan.get('status') != 'frozen':
        raise BenchmarkError('gRPC adaptive plan was not frozen before production')
    planned = {
        item['slug']: item
        for item in suite_plan.get('scenarios', ())
        if item.get('family') == 'grpc' and item.get('profile') == 'grpc'
    }
    records = [json.loads(p.read_text()) for p in (directory / 'raw').glob('*.json')]
    raw_paths = list((directory / 'raw').glob('*.json'))
    if not records or {path.stem for path in raw_paths} != set(planned):
        raise BenchmarkError('gRPC publication does not match its frozen scenario plan')
    requested_servers = set(records[0]['requested_servers'])
    if requested_servers != set(SERVERS):
        raise BenchmarkError('gRPC publication requires every server')
    if any(set(record['requested_servers']) != requested_servers for record in records):
        raise BenchmarkError('gRPC rows do not share one requested-server set')
    scenarios_by_slug = {scenario.slug(): scenario for scenario in scenarios()}
    expected = {
        (scenarios_by_slug[slug].case, name)
        for slug, item in planned.items()
        for name in item['supported_servers']
    }
    cells = {(c['case'], c['server']): c for c in qualification['cells']}
    if not expected.issubset(cells) or any(
        cells[key]['status'] not in {'passed', 'failed'} for key in expected
    ):
        raise BenchmarkError('gRPC publication has missing per-shape qualification')
    for slug, item in planned.items():
        scenario = scenarios_by_slug[slug]
        for name in item['supported_servers']:
            key = (scenario.case, name)
            calibration = item['calibration']['servers'][name]
            expected_status = (
                'failed' if calibration['status'] == 'failed' else 'passed'
            )
            if cells[key]['status'] != expected_status:
                raise BenchmarkError(
                    f'gRPC qualification disagrees with calibration for {key}'
                )
            trial_path = (
                directory / 'evidence' / slug / 'calibration' / name / 'trial.json'
            )
            if not trial_path.is_file() or json.loads(trial_path.read_text())[
                'status'
            ] != ('failed' if expected_status == 'failed' else 'completed'):
                raise BenchmarkError(f'gRPC calibration evidence is missing for {key}')

    cancellation_cells = {
        cell['server']: cell
        for cell in qualification['cells']
        if cell['case'] == 'Cancellation'
    }
    for name in ('h2corn', 'hypercorn', 'grpcio'):
        if name not in requested_servers:
            continue
        cell = cancellation_cells.get(name)
        if cell is None or cell['status'] not in {'passed', 'failed'}:
            raise BenchmarkError(f'gRPC publication is missing {name} cancellation')
        trial_path = directory / 'evidence' / 'cancellation' / name / 'trial.json'
        expected_status = 'completed' if cell['status'] == 'passed' else 'failed'
        if (
            not trial_path.is_file()
            or json.loads(trial_path.read_text())['status'] != expected_status
        ):
            raise BenchmarkError(f'gRPC cancellation evidence is missing for {name}')
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
