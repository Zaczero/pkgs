"""Cross-server benchmark suite: runs every scenario against every server and plots it.

Servers and load generators run unpinned, exactly as a deployment runs them.
Trials are rotation-balanced cold starts with a fixed configured count.
Scenario and suite deadlines bound the run; confidence intervals describe the
result without deciding when sampling stops.

    uv run python bench/bench.py            # stage a run under bench/results/runs/
    uv run python bench/bench.py --publish  # also replace the checked-in plots
"""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import math
import os
import platform
import secrets
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import textwrap
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypedDict

import matplotlib as mpl
from benchroom.plot import save, theme

mpl.use('Agg')
import matplotlib.pyplot as plt

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bench._core import (
    RESULTS_DIRECTORY,
    BenchmarkError,
    MeasuredMetrics,
    ResponseError,
    Scenario,
    allocated_listener,
    bootstrap_median_ci,
    check_response,
    deadline_bound,
    duration_seconds,
    ensure_large_upload_payload,
    ensure_static_file_payload,
    measure_peak_memory,
    peer_settings,
    request_response,
    rotations,
    run_load,
    running_server,
    terminate_process_group,
    write_json,
)
from bench.evidence import build_wheel, run_identity, verify_workers
from bench.grpc import RpcScenario
from bench.report import (
    SERVER_COLORS,
    SERVER_LINESTYLES,
    SERVER_MARKERS,
    alt_text,
    format_rate,
    render_accessible_results,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

DURATION = '10s'
WARMUP_DURATION = '2s'
CONCURRENCY = 100
STREAMING_CONCURRENCY = 1000
#: Fixed cold-start trials avoid stopping on a favourable confidence interval.
MIN_TRIALS = 3
MAX_TRIALS = 8
#: Hard wall-clock ceiling per scenario and for the whole suite. Trial duration
#: is never shortened to fit — the suite reports what it measured and says which
#: scenarios stopped on time rather than completing the trial count. The suite ceiling has to
#: cover every scenario spending its own, or a full run dies partway with
#: nothing published; `main` refuses a combination that cannot fit.
SCENARIO_BUDGET_SECONDS = 1800.0
SUITE_BUDGET_SECONDS = 48600.0
SEED = 20_260_726

NETEM_DELAY_MS = 25.0
NETEM_RTT_MS = NETEM_DELAY_MS * 2
NETEM_RATE = '1gbit'
NETEM_RTT_TOLERANCE_MS = 15.0
NETEM_RTT_SAMPLES = 10
NETEM_NAMESPACE_ENV = 'H2CORN_BENCH_NETEM_NAMESPACE'

# Every server runs the STDLIB asyncio event loop, and a pure-Python HTTP parser
# where it has one (uvicorn `--http h11`, gunicorn's Python parser). The
# comparison is meant to isolate the server, not its optional accelerators, and
# uvloop is not equally load-bearing across them: it does all of uvicorn's I/O,
# while h2corn's I/O runs in Rust and the loop only schedules application
# callbacks. Giving it to some and not others measured the accelerator instead.
#
# Every server runs with access logging ENABLED — that is how these servers are
# deployed, and request-log construction is part of the work a real deployment
# does. h2corn and uvicorn log by default; hypercorn and gunicorn need an
# explicit flag. Server output goes to /dev/null (see _core.running_server), so
# no server pays for a log sink the others avoid.
SERVERS: dict[str, list[str]] = {
    'h2corn': [
        'h2corn',
        'bench.bench_app:app',
        '--backlog',
        '2048',
        '--loop',
        'asyncio',
        '--timeout-keep-alive',
        '120',
        '--server-header',
        'off',
        '--date-header',
        '--no-proxy-headers',
        '--max-concurrent-streams',
        '256',
        '--h2-max-header-list-size',
        '1048576',
        '--h2-max-inbound-frame-size',
        '65536',
    ],
    'uvicorn': [
        'uvicorn',
        'bench.bench_app:app',
        '--loop',
        'asyncio',
        '--http',
        'h11',
        '--no-proxy-headers',
        '--timeout-keep-alive',
        '120',
        '--no-server-header',
        '--date-header',
        '--backlog',
        '2048',
    ],
    'hypercorn': [
        'hypercorn',
        'bench.bench_app:app',
        '-k',
        'asyncio',
        '--access-logfile',
        '-',
        '--backlog',
        '2048',
    ],
    'gunicorn': [
        'gunicorn',
        'bench.bench_app:app',
        '-k',
        'asgi',
        '--asgi-loop',
        'asyncio',
        '--no-control-socket',
        '--access-logfile',
        '-',
        '--backlog',
        '2048',
        '--keep-alive',
        '120',
    ],
}


@dataclass(frozen=True, slots=True)
class NetworkProfile:
    name: str
    description: str
    target_rtt_ms: float | None = None


class RTT50Netem(TypedDict):
    """Verified shape of the isolated netem profile written to each run."""

    name: str
    description: str
    status: str
    target_rtt_ms: float
    measured_rtt_ms: float
    one_way_delay_ms: float
    rate: str
    qdisc: str


LOOPBACK_PROFILE = NetworkProfile('loopback', 'loopback')
RTT50_PROFILE = NetworkProfile(
    'rtt50', f'{NETEM_RTT_MS:.0f} ms RTT, {NETEM_RATE} netem', NETEM_RTT_MS
)


def benchmark_scenarios() -> tuple[Scenario, ...]:
    return (
        Scenario('HTTP/1 GET (1 Worker)', 1, 'h1'),
        Scenario('HTTP/1 GET (4 Workers)', 4, 'h1'),
        Scenario('HTTP/1 GET over UDS (1 Worker)', 1, 'h1_uds'),
        Scenario('HTTP/1 GET over UDS (4 Workers)', 4, 'h1_uds'),
        Scenario('HTTP/2 GET (1 Worker)', 1, 'h2'),
        Scenario('HTTP/2 GET (4 Workers)', 4, 'h2'),
        Scenario(
            'HTTP/2 GET multiplexed (1 Worker)',
            1,
            'h2',
            concurrency=1,
            http2_parallelism=10,
        ),
        Scenario('HTTP/1 Static file (1 Worker)', 1, 'h1_file'),
        Scenario('HTTP/1 Static file (4 Workers)', 4, 'h1_file'),
        Scenario('HTTP/2 Static file (1 Worker)', 1, 'h2_file'),
        Scenario('HTTP/2 Static file (4 Workers)', 4, 'h2_file'),
        Scenario('HTTP/1 Portable streaming download (1 Worker)', 1, 'h1_download'),
        Scenario('HTTP/1 Portable streaming download (4 Workers)', 4, 'h1_download'),
        Scenario('HTTP/2 Portable streaming download (1 Worker)', 1, 'h2_download'),
        Scenario('HTTP/2 Portable streaming download (4 Workers)', 4, 'h2_download'),
        Scenario(
            'HTTP/1 Streaming POST (1 Worker)', 1, 'h1_stream', STREAMING_CONCURRENCY
        ),
        Scenario(
            'HTTP/1 Streaming POST (4 Workers)', 4, 'h1_stream', STREAMING_CONCURRENCY
        ),
        Scenario(
            'HTTP/2 Streaming POST (1 Worker)', 1, 'h2_stream', STREAMING_CONCURRENCY
        ),
        Scenario(
            'HTTP/2 Streaming POST (4 Workers)', 4, 'h2_stream', STREAMING_CONCURRENCY
        ),
        Scenario('HTTP/1 WebSocket (1 Worker)', 1, 'ws'),
        Scenario('HTTP/1 WebSocket (4 Workers)', 4, 'ws'),
    )


def rtt50_scenarios() -> tuple[Scenario, ...]:
    """WAN-shaped cells where bytes in flight or HTTP/2 windows matter.

    Small requests are intentionally absent: at 50 ms RTT their rate is a
    concurrency/RTT property of the generator, not a server comparison. Four
    workers are the deployment headline shape, except for the existing HTTP/2
    multiplexing cell whose ten streams per connection are the point of the
    workload.

    The streaming POSTs run at `CONCURRENCY`, not `STREAMING_CONCURRENCY`, for
    that same reason. A 1 Gbit, 50 ms path holds about 6.25 MiB in flight, so a
    thousand simultaneous uploads are bounded by the link and the generator
    rather than by the server -- measurably so: every one of the 1000 virtual
    users aborted on the deadline without a single completed request, which
    failed the whole suite rather than producing a slow number.
    """
    return (
        Scenario(
            'HTTP/1 Portable streaming download (4 Workers, 50 ms RTT)',
            4,
            'h1_download',
        ),
        Scenario(
            'HTTP/2 Portable streaming download (4 Workers, 50 ms RTT)',
            4,
            'h2_download',
        ),
        Scenario(
            'HTTP/1 Streaming POST (4 Workers, 50 ms RTT)',
            4,
            'h1_stream',
            CONCURRENCY,
        ),
        Scenario(
            'HTTP/2 Streaming POST (4 Workers, 50 ms RTT)',
            4,
            'h2_stream',
            CONCURRENCY,
        ),
        Scenario('HTTP/2 8 MiB upload (4 Workers, 50 ms RTT)', 4, 'h2_upload', 8),
        Scenario(
            'HTTP/2 GET multiplexed (1 Worker, 50 ms RTT)',
            1,
            'h2',
            concurrency=1,
            http2_parallelism=10,
        ),
    )


def _run_checked(command: list[str]) -> str:
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=15,
        )
    except OSError as error:
        raise BenchmarkError(f'failed to run {command[0]!r}: {error}') from error
    except subprocess.TimeoutExpired as error:
        raise BenchmarkError(f'setup command timed out: {" ".join(command)}') from error
    if result.returncode:
        detail = (
            result.stderr.strip() or result.stdout.strip() or 'no diagnostic output'
        )
        raise BenchmarkError(f'{" ".join(command)} failed: {detail}')
    return result.stdout


def loopback_rtt_ms() -> float:
    """Measure TCP round trips through loopback without a benchmark server."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(('127.0.0.1', 0))
    listener.listen(1)
    address = listener.getsockname()
    failures: list[BaseException] = []

    def echo() -> None:
        try:
            with listener.accept()[0] as connection:
                connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                for _ in range(NETEM_RTT_SAMPLES):
                    payload = connection.recv(1)
                    if not payload:
                        raise OSError('RTT client closed before completing probes')
                    connection.sendall(payload)
        except BaseException as error:  # pragma: no cover - reported to the caller
            failures.append(error)

    thread = threading.Thread(target=echo, name='h2corn-bench-rtt', daemon=True)
    thread.start()
    samples = []
    try:
        with socket.create_connection(address, timeout=5) as connection:
            connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            for _ in range(NETEM_RTT_SAMPLES):
                started = time.perf_counter()
                connection.sendall(b'x')
                if connection.recv(1) != b'x':
                    raise BenchmarkError('RTT echo returned the wrong payload')
                samples.append((time.perf_counter() - started) * 1000)
    finally:
        listener.close()
        thread.join(timeout=5)
    if thread.is_alive():
        raise BenchmarkError('RTT echo server did not stop')
    if failures:
        raise BenchmarkError(f'RTT echo failed: {failures[0]}')
    return statistics.median(samples)


def configure_rtt50_profile() -> RTT50Netem:
    """Install and prove the isolated 50 ms RTT profile before any cell runs."""
    for tool in ('ip', 'tc'):
        if shutil.which(tool) is None:
            raise BenchmarkError(f'{tool} is unavailable')
    _run_checked(['ip', 'link', 'set', 'lo', 'up'])
    _run_checked([
        'tc',
        'qdisc',
        'replace',
        'dev',
        'lo',
        'root',
        'netem',
        'delay',
        f'{NETEM_DELAY_MS:g}ms',
        'rate',
        NETEM_RATE,
    ])
    qdisc = _run_checked(['tc', 'qdisc', 'show', 'dev', 'lo']).strip()
    if 'netem' not in qdisc:
        raise BenchmarkError(f'netem did not attach to loopback: {qdisc or "no qdisc"}')
    measured_rtt = loopback_rtt_ms()
    if abs(measured_rtt - NETEM_RTT_MS) > NETEM_RTT_TOLERANCE_MS:
        raise BenchmarkError(
            f'netem RTT was {measured_rtt:.1f} ms, expected '
            f'{NETEM_RTT_MS:.0f}±{NETEM_RTT_TOLERANCE_MS:.0f} ms'
        )
    print(
        f'  verified {measured_rtt:.1f} ms loopback RTT (target {NETEM_RTT_MS:.0f} ms)',
        flush=True,
    )
    return {
        'name': RTT50_PROFILE.name,
        'description': RTT50_PROFILE.description,
        'status': 'ready',
        'target_rtt_ms': NETEM_RTT_MS,
        'measured_rtt_ms': measured_rtt,
        'one_way_delay_ms': NETEM_DELAY_MS,
        'rate': NETEM_RATE,
        'qdisc': qdisc,
    }


def write_hypercorn_config(path: Path) -> None:
    """Write the exposed Hypercorn counterparts to h2corn's chosen settings."""
    path.write_text(
        '# Generated by bench.py; retained with the run record.\n'
        'h2_max_concurrent_streams = 256\n'
        'h2_max_header_list_size = 1048576\n'
        'h2_max_inbound_frame_size = 65536\n'
        'keep_alive_timeout = 120\n'
        'keep_alive_max_requests = 9223372036854775807\n'
        'include_server_header = false\n'
        'include_date_header = true\n'
    )


def unsupported_reason(name: str, scenario: Scenario | RpcScenario) -> str | None:
    if scenario.type == 'grpc':
        return None
    if scenario.contract.protocol == '2' and name in {'uvicorn', 'gunicorn'}:
        return 'The selected release has no prior-knowledge cleartext HTTP/2 mode.'
    return None


def server_command(
    name: str,
    scenario: Scenario,
    *,
    hypercorn_config: Path,
    listener_fd: int | None = None,
) -> list[str]:
    if reason := unsupported_reason(name, scenario):
        raise BenchmarkError(f'{name}: {reason}')
    command = SERVERS[name].copy()
    command[0] = str(Path(sys.executable).parent / name)
    socket_path = scenario.socket_path
    if name == 'uvicorn':
        command += ['--ws', 'websockets' if scenario.type == 'ws' else 'none']
        command += (
            ['--fd', str(listener_fd)]
            if listener_fd is not None
            else ['--uds', str(socket_path)]
            if socket_path is not None
            else ['--host', '127.0.0.1', '--port', str(scenario.port)]
        )
    elif listener_fd is not None:
        command += ['-b', f'fd://{listener_fd}']
    else:
        target = (
            f'unix:{socket_path}'
            if socket_path is not None
            else f'127.0.0.1:{scenario.port}'
        )
        command += ['-b', target]
    if name == 'hypercorn':
        command += ['--config', str(hypercorn_config)]
    # uvicorn and hypercorn only spell it long; h2corn and gunicorn take both.
    workers_flag = '--workers' if name in {'hypercorn', 'uvicorn'} else '-w'
    command += [workers_flag, str(scenario.workers)]
    return command


def package_versions() -> dict[str, str | None]:
    versions: dict[str, str | None] = {}
    for package in (*SERVERS, 'starlette', 'h11', 'h2', 'websockets'):
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = None
    return versions


def system_summary() -> str:
    kernel = f'{platform.system()} {platform.release()}'.strip()
    summary = f'Python {platform.python_version()} | {kernel} | {platform.machine()}'
    model = None
    cpuinfo = Path('/proc/cpuinfo')
    if cpuinfo.exists():
        for line in cpuinfo.read_text(errors='ignore').splitlines():
            if line.startswith('model name'):
                model = line.partition(':')[2].strip()
                break
    elif shutil.which('sysctl'):
        result = subprocess.run(
            ['sysctl', '-n', 'machdep.cpu.brand_string'],
            capture_output=True,
            text=True,
            check=False,
        )
        model = result.stdout.strip() or None
    if model is None:
        raise BenchmarkError('failed to determine the CPU model for plot metadata')
    return f'{summary}\nCPU: {model}'


def aligned_setting(name: str, value: object, default: object) -> dict[str, object]:
    return {'setting': name, 'value': value, 'default': default}


def no_equivalent(
    h2corn_setting: str, h2corn_value: object, reason: str
) -> dict[str, object]:
    return {
        'h2corn_setting': h2corn_setting,
        'h2corn_value': h2corn_value,
        'reason': reason,
    }


def server_profiles(scenario: Scenario) -> dict[str, dict[str, object]]:
    """The aligned knobs and deliberately unmatched settings for one cell.

    Values are data, not prose: a reader can see the value passed to every
    competitor alongside its upstream default without making a plot carry
    configuration archaeology.
    """
    workers = scenario.workers
    h2corn_http2 = [
        aligned_setting('max_concurrent_streams', 256, 256),
        aligned_setting('h2_max_header_list_size', 1_048_576, 1_048_576),
        aligned_setting('h2_max_inbound_frame_size', 65_536, 65_536),
    ]
    profiles: dict[str, dict[str, object]] = {
        'h2corn': {
            'profile_choice': [aligned_setting('loop', 'asyncio', 'auto')],
            'aligned': [
                aligned_setting('workers', workers, 1),
                aligned_setting('backlog', 2_048, 1_024),
                aligned_setting('timeout_keep_alive', 120, 120),
                aligned_setting('server_header', 'off', 'off'),
                aligned_setting('date_header', True, True),
                aligned_setting('proxy_headers', False, False),
                *h2corn_http2,
            ],
            'no_equivalent': [],
        },
        'hypercorn': {
            'profile_choice': [aligned_setting('worker_class', 'asyncio', 'asyncio')],
            'aligned': [
                aligned_setting('workers', workers, 1),
                aligned_setting('backlog', 2_048, 100),
                aligned_setting('keep_alive_timeout', 120, 5),
                aligned_setting('keep_alive_max_requests', 2**63 - 1, 1000),
                aligned_setting('include_server_header', False, True),
                aligned_setting('include_date_header', True, True),
                aligned_setting('h2_max_concurrent_streams', 256, 100),
                aligned_setting('h2_max_header_list_size', 1_048_576, 65_536),
                aligned_setting('h2_max_inbound_frame_size', 65_536, 16_384),
            ],
            'no_equivalent': [
                no_equivalent(
                    'proxy_headers',
                    False,
                    'Hypercorn exposes no server-level proxy-header parsing setting.',
                ),
                no_equivalent(
                    'h2_initial_stream_window_size',
                    8_388_608,
                    'Hypercorn exposes no HTTP/2 flow-control window; it uses '
                    "h2's 65,535-byte default.",
                ),
                no_equivalent(
                    'h2_initial_connection_window_size',
                    8_388_608,
                    'Hypercorn exposes no HTTP/2 connection receive-window setting.',
                ),
                no_equivalent(
                    'h2_max_header_block_size',
                    1_048_576,
                    'Hypercorn exposes no compressed HTTP/2 header-block limit.',
                ),
            ],
        },
        'uvicorn': {
            'aligned': [
                aligned_setting('workers', workers, 1),
                aligned_setting('backlog', 2_048, 2_048),
                aligned_setting('timeout_keep_alive', 120, 5),
                aligned_setting('server_header', False, True),
                aligned_setting('date_header', True, True),
                aligned_setting('proxy_headers', False, True),
            ],
            'profile_choice': [
                aligned_setting(
                    'loop',
                    'asyncio',
                    'auto',
                )
            ],
            'http_backend': 'h11',
            'no_equivalent': [],
        },
        'gunicorn': {
            'aligned': [
                aligned_setting('workers', workers, 1),
                aligned_setting('backlog', 2_048, 2_048),
                aligned_setting('keep_alive', 120, 2),
            ],
            'profile_choice': [aligned_setting('asgi_loop', 'asyncio', 'auto')],
            'no_equivalent': [
                no_equivalent(
                    'server_header',
                    'off',
                    'Gunicorn exposes no response Server-header setting.',
                ),
                no_equivalent(
                    'date_header',
                    True,
                    'Gunicorn exposes no response Date-header setting.',
                ),
                no_equivalent(
                    'proxy_headers',
                    False,
                    'Gunicorn has no ASGI proxy-header parsing counterpart.',
                ),
            ],
            'http_backend': 'python',
        },
    }
    for name in profiles:
        if reason := unsupported_reason(name, scenario):
            profiles[name] = {'status': 'unsupported', 'reason': reason}
    return profiles


def aggregate(samples: Sequence[MeasuredMetrics]) -> dict[str, Any]:
    rates = [sample['rps'] for sample in samples]
    peak_memory = [sample['peak_memory_bytes'] for sample in samples]
    percentile_names = sorted(
        {name for sample in samples for name in sample['latency_percentiles']},
        key=lambda name: float(name.removeprefix('p')),
    )
    return {
        'rps': statistics.median(rates),
        'rps_samples': rates,
        # Median completions, beside the rate they produced. A server that
        # answers once per connection and then stalls still reports a rate;
        # only this count against the cell's concurrency reveals it.
        'completed': statistics.median([sample['completed'] for sample in samples]),
        'rps_range': [min(rates), max(rates)],
        # Throughput is the median trial, but deployment capacity must cover
        # the largest sampled PSS high-water mark from every measured trial.
        'peak_memory_bytes': max(peak_memory),
        'latency_percentiles': {
            name: statistics.median([
                sample['latency_percentiles'][name]
                for sample in samples
                if name in sample['latency_percentiles']
            ])
            for name in percentile_names
        },
    }


class TrialError(BenchmarkError):
    def __init__(self, phase: str, message: str) -> None:
        super().__init__(message)
        self.phase = phase


def load_plan(scenario: Scenario, duration: str, warmup: str) -> dict[str, Any]:
    if scenario.contract.request_body_path is not None:
        concurrency = scenario.concurrency or CONCURRENCY
        return {
            'mode': 'fixed-count',
            'requests': 2 * concurrency,
            'warmup_requests': concurrency,
            'timeout_seconds': 120,
        }
    return {'mode': 'time-window', 'duration': duration, 'warmup_duration': warmup}


def measure_cell(
    name: str,
    scenario: Scenario | RpcScenario,
    duration: str,
    warmup: str,
    *,
    hypercorn_config: Path,
    evidence_directory: Path,
    identity: dict[str, Any] | None = None,
) -> MeasuredMetrics:
    """One cold-start trial: start, prove correctness, warm up, measure, prove again."""
    if scenario.type == 'grpc':
        from bench.grpc import measure_cell as measure_rpc

        assert identity is not None
        rpc_measured = measure_rpc(
            name,
            scenario,
            duration,
            warmup,
            evidence_directory=evidence_directory,
            identity=identity,
        )
        return {
            'rps': rpc_measured['rps'],
            'completed': rpc_measured['completed'],
            'latency_percentiles': rpc_measured['latency_percentiles'],
            'peak_memory_bytes': rpc_measured['peak_memory_bytes'],
        }
    concurrency = scenario.concurrency or CONCURRENCY
    plan = load_plan(scenario, duration, warmup)
    evidence_directory.mkdir(parents=True)
    identity_directory = evidence_directory / 'worker-identities'
    identity_directory.mkdir()
    started = time.monotonic()
    trial: dict[str, Any] = {
        'server': name,
        'phase': 'listener',
        'status': 'running',
        'started_at': datetime.now(UTC).isoformat(),
        'load_plan': plan,
    }
    try:
        with allocated_listener(scenario) as (live, listener):
            command = server_command(
                name,
                live,
                hypercorn_config=hypercorn_config,
                listener_fd=listener.fileno(),
            )
            trial.update(
                command=command,
                port=live.port,
                socket_path=str(live.socket_path),
                tcp_nodelay=not live.contract.unix_socket,
                phase='startup',
            )
            write_json(evidence_directory / 'trial.json', trial)
            with running_server(
                command,
                workers=live.workers,
                socket_path=live.socket_path,
                scenario=live,
                listener_fd=listener.fileno(),
                identity_directory=identity_directory,
            ) as server:
                trial['phase'] = 'identity'
                receipts = {
                    pid: json.loads((identity_directory / f'{pid}.json').read_text())
                    for pid in server.worker_pids
                }
                receipt = json.loads(
                    request_response(live, '/__bench/identity', timeout=5)
                )
                if receipt['pid'] not in receipts:
                    raise BenchmarkError('serving worker changed after readiness')
                receipts[receipt['pid']] = receipt
                trial['workers'] = receipts
                if identity is not None:
                    verify_workers(trial, identity)
                trial['supervisor_pid'] = server.pid
                if live.contract.protocol == '2':
                    trial['phase'] = 'settings'
                    trial['http2_peer'] = peer_settings(live)
                trial['phase'] = 'correctness-before'
                check_response(live)
                trial['correctness_before'] = 'passed'
                trial['phase'] = 'warmup'
                run_load(
                    live,
                    duration=warmup,
                    concurrency=concurrency,
                    evidence_path=evidence_directory / 'warmup.json',
                    requests=plan.get('warmup_requests'),
                )
                trial['phase'] = 'load'
                metrics, peak_memory_bytes = measure_peak_memory(
                    server,
                    lambda: run_load(
                        live,
                        duration=duration,
                        concurrency=concurrency,
                        evidence_path=evidence_directory / 'load.json',
                        requests=plan.get('requests'),
                    ),
                    evidence_path=evidence_directory / 'memory.json',
                )
                trial['phase'] = 'correctness-after'
                check_response(live)
                trial['correctness_after'] = 'passed'
                measured: MeasuredMetrics = {
                    **metrics,
                    'peak_memory_bytes': peak_memory_bytes,
                }
                trial['metrics'] = measured
                trial['phase'] = 'cleanup'
    except (OSError, ValueError, RuntimeError) as error:
        trial.update(status='failed', error=str(error))
        raise TrialError(
            trial['phase'],
            f'{name} {trial["phase"]} failed: {error}',
        ) from error
    else:
        trial.update(status='completed', phase='completed')
        return measured
    finally:
        trial['elapsed_seconds'] = time.monotonic() - started
        write_json(evidence_directory / 'trial.json', trial)


def paired_contrasts(samples: dict[str, list[MeasuredMetrics]]) -> dict[str, Any]:
    """Resample complete trial blocks, keeping each server pair together."""
    if 'h2corn' not in samples:
        return {}
    candidate = samples['h2corn']
    contrasts = {}
    for name, values in samples.items():
        if name == 'h2corn':
            continue
        ratios = [
            first['rps'] / second['rps']
            for first, second in zip(candidate, values, strict=True)
        ]
        low, high = bootstrap_median_ci(ratios, SEED)
        contrasts[name] = {
            'h2corn_over_competitor': statistics.median(ratios),
            'interval_95': [low, high],
            'block_ratios': ratios,
            'method': 'percentile bootstrap of paired-block median rate ratios',
        }
    return contrasts


def workload_description(scenario: Scenario) -> str:
    descriptions = {
        'h1': '13-byte text response',
        'h1_uds': '13-byte text response over a Unix socket',
        'h2': '13-byte text response',
        'h1_file': '128 KiB file response; native file-send extension where supported',
        'h2_file': '128 KiB file response; native file-send extension where supported',
        'h1_download': '128 KiB download in eight 16 KiB ASGI chunks',
        'h2_download': '128 KiB download in eight 16 KiB ASGI chunks',
        'h1_stream': '1 KiB upload, then three response chunks with 20 ms application delay',
        'h2_stream': '1 KiB upload, then three response chunks with 20 ms application delay',
        'h2_upload': '8 MiB upload, then the received byte count',
        'ws': 'Connect, echo one text message, disconnect; fresh sessions',
    }
    concurrency = scenario.concurrency or CONCURRENCY
    return (
        descriptions[scenario.type]
        + f'; {concurrency} client connection{"s" if concurrency != 1 else ""}'
        + (
            f' with {scenario.http2_parallelism} stream{"s" if scenario.http2_parallelism != 1 else ""} per connection'
            if scenario.contract.protocol == '2'
            else ''
        )
    )


def run_scenario(
    scenario: Scenario | RpcScenario,
    servers: Sequence[str],
    *,
    duration: str,
    warmup: str,
    max_trials: int,
    deadline: float,
    hypercorn_config: Path,
    evidence_directory: Path,
    identity: dict[str, Any],
) -> dict[str, Any]:
    names = list(servers)
    rpc = scenario.type == 'grpc'
    rpc_case = scenario.case if isinstance(scenario, RpcScenario) else None
    not_selected = (
        {'grpclib': 'Representative no-compression subset'}
        if rpc and not scenario.grpclib and 'grpclib' in names
        else {}
    )
    unsupported = {
        name: reason for name in names if (reason := unsupported_reason(name, scenario))
    }
    samples: dict[str, list[MeasuredMetrics]] = {
        name: []
        for name in names
        if name not in unsupported and name not in not_selected
    }
    if not samples:
        raise BenchmarkError(f'no selected server supports {scenario.name}')
    failures: dict[str, dict[str, Any]] = {
        cell['server']: {
            'phase': 'qualification',
            'error': cell['error'],
            'block': 0,
            'evidence': cell['server'],
            'status': 'response_validation_failed',
        }
        for cell in identity.get('rpc_qualification', [])
        if rpc
        and cell['case'] == rpc_case
        and cell['status'] == 'failed'
        and cell['server'] in samples
    }
    stopped = 'max-trials'
    trials = 0
    blocks = []
    for trial in range(1, max_trials + 1):
        remaining = [name for name in samples if name not in failures]
        if not remaining:
            break
        order = rotations(remaining, max_trials, SEED)[trial - 1]
        blocks.append({
            'block': trial,
            'order': order,
            'not_repeated_after_failure': list(failures),
        })
        for name in order:
            if name in failures:
                continue
            if time.monotonic() >= deadline:
                raise BenchmarkError(
                    f'{scenario.name} deadline before block {trial}, {name}'
                )
            try:
                with deadline_bound(deadline):
                    samples[name].append(
                        measure_cell(
                            name,
                            scenario,
                            duration,
                            warmup,
                            hypercorn_config=hypercorn_config,
                            evidence_directory=evidence_directory
                            / f'block-{trial:02d}'
                            / name,
                            identity=identity,
                        )
                    )
            except TrialError as error:
                write_json(
                    evidence_directory / f'failure-{name}.json',
                    {
                        'scenario': scenario.name,
                        'blocks': blocks,
                        'failed_server': name,
                        'error': str(error),
                        'samples': samples,
                    },
                )
                if (name == 'h2corn' and not rpc) or not isinstance(
                    error.__cause__, ResponseError
                ):
                    raise
                failures[name] = {
                    'phase': error.phase,
                    'error': str(error),
                    'block': trial,
                    'evidence': f'block-{trial:02d}/{name}',
                    'status': 'response_validation_failed',
                }
                print(f'  FAILED {name}: {error}', flush=True)
        remaining = [name for name in samples if name not in failures]
        if not remaining:
            if not rpc:
                raise BenchmarkError(f'every server failed {scenario.name}')
            trials = trial
            break
        trials = trial
        medians = ', '.join(
            f'{name} {format_rate(statistics.median([s["rps"] for s in samples[name]]))}'
            for name in remaining
        )
        print(
            f'  after block {trial}/{max_trials}: median {medians}',
            flush=True,
        )
        if trial < max_trials and time.monotonic() > deadline:
            stopped = 'time-budget'
            break
    plan = (
        {'mode': 'time-window', 'duration': duration, 'warmup_duration': warmup}
        if rpc
        else load_plan(scenario, duration, warmup)
    )
    description = scenario.workload if rpc else workload_description(scenario)
    if plan['mode'] == 'fixed-count':
        description += f'; {plan["requests"]} completed requests per trial'
    return {
        'scenario': scenario.name,
        'status': 'attempted_with_failures' if failures else 'completed',
        'workload': description,
        'load_plan': plan,
        'type': scenario.type,
        'workers': scenario.workers,
        'trials': trials,
        'planned_trials': max_trials,
        'stopped': stopped,
        'unsupported': unsupported,
        **({'not_selected': not_selected} if rpc else {}),
        'failures': failures,
        'requested_servers': names,
        'server_profiles': (
            {
                name: {
                    'loop': 'asyncio',
                    'processes': 1,
                    'compression': 'identity',
                    'access_log': False,
                }
                for name in names
            }
            if rpc
            else server_profiles(scenario)
        ),
        'blocks': blocks,
        'samples': samples,
        'paired_contrasts': paired_contrasts({
            name: values for name, values in samples.items() if name not in failures
        }),
        'aggregate': {
            name: aggregate(values)
            for name, values in samples.items()
            if name not in failures
        },
    }


def plot_results(
    record: dict[str, Any],
    path: Path,
) -> None:
    for narrow in (False, True):
        for dark in (False, True):
            suffix = ('-narrow' if narrow else '') + ('-dark' if dark else '')
            destination = path.with_stem(path.stem + suffix)
            with theme(dark=dark):
                _plot_results(record, destination, dark=dark, narrow=narrow)
    path.with_suffix('.md').write_text(
        render_accessible_results(record), encoding='utf-8'
    )
    print(f'Saved plot to {path}')
    print(
        alt_text(
            record['scenario'],
            record['aggregate'],
            record.get(
                'rate_unit', 'sessions/s' if record['type'] == 'ws' else 'requests/s'
            ),
            record.get('failures'),
            record.get('stack_labels'),
        )
    )


def _plot_results(
    record: dict[str, Any], path: Path, *, dark: bool, narrow: bool
) -> None:
    results = record['aggregate']
    title = record['scenario']
    websocket = record['type'] == 'ws'
    failures = record.get('failures', {})
    figure, axes = plt.subplots(
        2 if narrow else 1,
        1 if narrow else 2,
        figsize=(5.2, 8.4) if narrow else (10.5, 5.6),
        layout='constrained',
    )
    figure.suptitle(
        textwrap.fill(title, width=44) if narrow else title,
        fontsize=15 if narrow else 17,
        fontweight='bold',
    )

    names = sorted(
        record.get('requested_servers', [*results, *failures]),
        key=lambda name: list(SERVER_COLORS).index(name),
    )
    labels = [record.get('stack_labels', {}).get(name, name) for name in names]
    rates = [results[name]['rps'] if name in results else math.nan for name in names]
    colors = [SERVER_COLORS[name][dark] for name in names]
    foreground = mpl.rcParams['text.color']

    throughput = axes[0]
    bars = throughput.bar(
        [label.replace(' / ', ' /\n') for label in labels], rates, color=colors
    )
    throughput.set_xlim(-0.6, len(names) - 0.4)
    for bar, name in zip(bars, names, strict=True):
        if name not in results:
            throughput.text(
                bar.get_x() + bar.get_width() / 2,
                0.05,
                'failed'
                if name in failures
                else 'not selected'
                if name in record.get('not_selected', {})
                else 'unsupported',
                ha='center',
                va='bottom',
                transform=throughput.get_xaxis_transform(),
                fontsize=9,
            )
            continue
        low, high = results[name]['rps_range']
        median = results[name]['rps']
        if high > low:
            throughput.errorbar(
                bar.get_x() + bar.get_width() / 2,
                median,
                yerr=[[median - low], [high - median]],
                fmt='none',
                color=foreground,
                capsize=4,
            )
        samples = results[name]['rps_samples']
        throughput.scatter(
            [
                bar.get_x()
                + bar.get_width() / 2
                + (index - (len(samples) - 1) / 2) * 0.045
                for index in range(len(samples))
            ],
            samples,
            s=20,
            facecolors='#111923' if dark else '#ffffff',
            edgecolors=foreground,
            linewidths=0.8,
            zorder=3,
        )
    unit = record.get('rate_unit', 'sessions/s' if websocket else 'requests/s')
    throughput.set_title('Throughput · higher is better')
    throughput.set_ylabel(unit)
    throughput.margins(y=0.24)
    for bar, name in zip(bars, names, strict=True):
        if name not in results:
            continue
        _, high = results[name]['rps_range']
        result = results[name]
        value = result['rps']
        center = bar.get_x() + bar.get_width() / 2
        top = max(value, high)
        throughput.annotate(
            format_rate(value),
            (center, top),
            xytext=(0, 17),
            textcoords='offset points',
            ha='center',
            va='bottom',
            fontsize=10,
        )
        throughput.annotate(
            f'{result["peak_memory_bytes"] / 1e6:.1f} MB PSS',
            (center, top),
            xytext=(0, 4),
            textcoords='offset points',
            ha='center',
            va='bottom',
            fontsize=8,
            alpha=0.85,
        )

    latency = axes[1]
    plotted = False
    for name, label in zip(names, labels, strict=True):
        if name not in results:
            continue
        percentiles = results[name]['latency_percentiles']
        if not percentiles:
            continue
        points = sorted(
            percentiles.items(), key=lambda item: float(item[0].removeprefix('p'))
        )
        latency.plot(
            [float(key.removeprefix('p')) for key, _ in points],
            [value * 1000 for _, value in points],
            color=SERVER_COLORS[name][dark],
            linestyle=SERVER_LINESTYLES.get(name, '-'),
            marker=SERVER_MARKERS.get(name, 'o'),
            label=label,
            linewidth=2,
            markersize=6,
            markeredgewidth=0,
        )
        plotted = True
    if plotted:
        latency.set_title(
            ('Session duration' if websocket else 'Latency')
            + ' percentiles · lower is better'
        )
        latency.set_xlabel('Percentile')
        latency.set_ylabel('Latency (ms)')
        latency.legend()
        latency.grid(True)
    else:
        latency.set_title('Latency data unavailable')

    footer = (
        'Bars: median rate · Dots: fresh-start trials · Whiskers: observed range'
        + (
            '\nLatency: median trial histogram bounds (1% buckets)'
            if record.get('benchmark_family') == 'grpc'
            else '\nLatency: median trial percentiles'
        )
        + ' · PSS: maximum sampled, decimal MB\n'
        + textwrap.fill(record.get('workload', ''), width=55 if narrow else 110)
    )
    if narrow:
        footer = '\n'.join(
            textwrap.fill(line, width=55) for line in footer.splitlines()
        )
    figure.supxlabel(footer, fontsize=9)
    save(figure, path)
    plt.close(figure)


def profile_record(
    profile: NetworkProfile,
    status: str,
    *,
    reason: str | None = None,
    measured_rtt_ms: float | None = None,
    netem: RTT50Netem | None = None,
) -> dict[str, Any]:
    record = {
        'profile': profile.name,
        'description': profile.description,
        'target_rtt_ms': profile.target_rtt_ms,
        'measured_rtt_ms': measured_rtt_ms,
        'status': status,
        'reason': reason,
    }
    if netem is not None:
        record['netem'] = netem
    return record


def record_skipped_profile(
    run_directory: Path,
    profile: NetworkProfile,
    scenarios: Sequence[Scenario],
    reason: str,
) -> None:
    raw_directory = run_directory / 'raw'
    raw_directory.mkdir(parents=True, exist_ok=True)
    write_json(
        run_directory / 'profile.json',
        profile_record(profile, 'skipped', reason=reason),
    )
    for scenario in scenarios:
        print(
            f'  EXCLUDED {profile.description} from {scenario.name}: {reason}',
            flush=True,
        )
        write_json(
            raw_directory / f'{scenario.slug()}.json',
            {
                'scenario': scenario.name,
                'type': scenario.type,
                'workers': scenario.workers,
                'network_profile': profile.name,
                'target_rtt_ms': profile.target_rtt_ms,
                'measured_rtt_ms': None,
                'excluded': {profile.description: reason},
                'server_profiles': server_profiles(scenario),
                'status': 'skipped',
            },
        )


def namespace_arguments(run_directory: Path) -> list[str]:
    """Forward the user arguments once, replacing profile-local switches."""
    forwarded = []
    index = 1
    while index < len(sys.argv):
        argument = sys.argv[index]
        if argument in {'--network-profile', '--run-directory'}:
            index += 2
        elif argument.startswith(('--network-profile=', '--run-directory=')):
            index += 1
        elif argument == '--publish':
            # The parent publishes both staged profiles together. Passing this
            # through would make the shaped child reject its partial profile.
            index += 1
        else:
            forwarded.append(argument)
            index += 1
    return [
        *forwarded,
        '--network-profile',
        RTT50_PROFILE.name,
        '--run-directory',
        str(run_directory),
    ]


def run_rtt50_namespace(
    run_directory: Path,
    timeout: float = SUITE_BUDGET_SECONDS + 60,
) -> tuple[int | None, str | None]:
    """Start the 50 ms profile in a throwaway unprivileged network namespace."""
    try:
        process = subprocess.Popen(
            [
                'unshare',
                '-rn',
                '--',
                sys.executable,
                sys.argv[0],
                *namespace_arguments(run_directory),
            ],
            env={**os.environ, NETEM_NAMESPACE_ENV: '1'},
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
    except OSError as error:
        return None, str(error)
    try:
        _, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired as error:
        terminate_process_group(process)
        raise BenchmarkError(
            '50 ms RTT namespace exceeded its suite deadline'
        ) from error
    except BaseException:
        terminate_process_group(process)
        raise
    detail = stderr.strip() or None
    if process.returncode and detail:
        print(detail, file=sys.stderr)
    return process.returncode, detail


def publish(run_directories: Sequence[Path]) -> None:
    profiles = [json.loads((d / 'profile.json').read_text()) for d in run_directories]
    if len({p.get('benchmark_family', 'http') for p in profiles}) != 1:
        raise BenchmarkError('publish one benchmark family at a time')
    rpc = all(p.get('benchmark_family') == 'grpc' for p in profiles)
    root = RESULTS_DIRECTORY / 'grpc' if rpc else RESULTS_DIRECTORY
    evidence = root / 'evidence'
    if evidence.exists():
        shutil.rmtree(evidence)
    evidence.mkdir(parents=True)
    for directory in run_directories:
        profile = json.loads((directory / 'profile.json').read_text())['profile']
        destination = evidence / profile
        destination.mkdir()
        for name in (
            'identity.json',
            'profile.json',
            'hypercorn.toml',
            'build.log',
            'qualification.json',
            'notebook.pb',
        ):
            if (directory / name).exists():
                shutil.copy2(directory / name, destination / name)
        if (directory / 'evidence').exists():
            shutil.copytree(directory / 'evidence', destination / 'trials')
    for kind, suffix, destination in (
        ('raw', '.json', root / 'raw'),
        ('plots', '.svg', root / 'plots'),
        ('plots', '.md', root / 'plots'),
    ):
        destination.mkdir(parents=True, exist_ok=True)
        for stale in destination.glob(f'*{suffix}'):
            stale.unlink()
        for run_directory in run_directories:
            profile = json.loads(
                (run_directory / 'profile.json').read_text(encoding='utf-8')
            )['profile']
            for artifact in sorted((run_directory / kind).glob(f'*{suffix}')):
                variant = next(
                    (
                        suffix
                        for suffix in ('-narrow-dark', '-narrow', '-dark')
                        if kind == 'plots' and artifact.stem.endswith(suffix)
                    ),
                    '',
                )
                stem = artifact.stem.removesuffix(variant)
                published_name = f'{stem}-{profile}{variant}{suffix}'
                shutil.copy2(artifact, destination / published_name)
    print(
        f'Published {len(run_directories)} profile(s) with matched raw records, '
        f'tables, and plots to {root / "raw"} and {root / "plots"}'
    )


def assert_publishable(run_directories: Sequence[Path]) -> None:
    incomplete = []
    for run_directory in run_directories:
        records = sorted((run_directory / 'raw').glob('*.json'))
        if not records:
            raise BenchmarkError(f'no benchmark results in {run_directory}')
        identity_path = run_directory / 'identity.json'
        if (
            not identity_path.is_file()
            or json.loads(identity_path.read_text()).get('build') is None
        ):
            raise BenchmarkError(f'no identified release build in {run_directory}')
        profile = (
            json.loads((run_directory / 'profile.json').read_text())
            if (run_directory / 'profile.json').exists()
            else {}
        )
        if profile.get('benchmark_family') == 'grpc':
            from bench.grpc import assert_rpc_publishable

            assert_rpc_publishable(run_directory)
        for record_path in records:
            record = json.loads(record_path.read_text())
            if record.get('planned_trials', 0) < MIN_TRIALS:
                incomplete.append(f'{record["scenario"]}: insufficient planned trials')
            if record.get('status') == 'skipped':
                incomplete.append(record['scenario'])
                continue
            if record.get('stopped') == 'time-budget' or (
                not record.get('aggregate') and not record.get('failures')
            ):
                incomplete.append(record['scenario'])
            represented = (
                set(record.get('aggregate', {}))
                | set(record.get('failures', {}))
                | set(record.get('unsupported', {}))
                | set(record.get('not_selected', {}))
            )
            if represented != set(record['requested_servers']):
                incomplete.append(
                    f'{record["scenario"]}: unrepresented requested servers'
                )
            for name, result in record.get('aggregate', {}).items():
                if len(result['rps_samples']) != record['planned_trials']:
                    incomplete.append(
                        f'{record["scenario"]}: incomplete {name} samples'
                    )
                for block in range(1, record['planned_trials'] + 1):
                    trial_path = (
                        run_directory
                        / 'evidence'
                        / record_path.stem
                        / f'block-{block:02d}'
                        / name
                        / 'trial.json'
                    )
                    if (
                        not trial_path.is_file()
                        or json.loads(trial_path.read_text()).get('status')
                        != 'completed'
                    ):
                        incomplete.append(
                            f'{record["scenario"]}: incomplete {name} block {block}'
                        )
            for name, failure in record.get('failures', {}).items():
                trial_path = (
                    run_directory
                    / 'evidence'
                    / record_path.stem
                    / failure['evidence']
                    / 'trial.json'
                )
                if (
                    name in record['aggregate']
                    or not trial_path.is_file()
                    or json.loads(trial_path.read_text()).get('status') != 'failed'
                ):
                    incomplete.append(
                        f'{record["scenario"]}: missing failed-cell evidence for {name}'
                    )
    if incomplete:
        raise BenchmarkError(
            f'incomplete benchmark scenarios: {", ".join(incomplete)}; '
            'increase the time budget and re-run'
        )


def run_suite(
    *,
    servers: Sequence[str],
    scenarios: Sequence[Scenario],
    duration: str,
    warmup: str,
    max_trials: int,
    scenario_budget: float,
    suite_budget: float,
    run_directory: Path,
    profile: NetworkProfile,
    measured_rtt_ms: float | None = None,
    netem: RTT50Netem | None = None,
    wheel: Path | None = None,
) -> Path:
    ensure_static_file_payload()
    if any(scenario.type == 'h2_upload' for scenario in scenarios):
        ensure_large_upload_payload()
    for tool in {'oha'} | ({'k6'} if any(s.type == 'ws' for s in scenarios) else set()):
        if shutil.which(tool) is None:
            raise BenchmarkError(f'{tool} is required but not installed')

    if run_directory.exists():
        raise BenchmarkError(f'refusing to overwrite existing run {run_directory}')
    (run_directory / 'raw').mkdir(parents=True)
    (run_directory / 'plots').mkdir()
    hypercorn_config = run_directory / 'hypercorn.toml'
    write_hypercorn_config(hypercorn_config)
    try:
        identity = run_identity(Path(__file__).resolve().parents[2], wheel)
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        raise BenchmarkError(f'cannot establish benchmark identity: {error}') from error
    write_json(run_directory / 'identity.json', identity)
    if identity['build'] is not None:
        shutil.copy2(identity['build']['log']['path'], run_directory / 'build.log')

    summary = system_summary()
    versions = package_versions()
    active_profile = profile_record(
        profile,
        'running',
        measured_rtt_ms=measured_rtt_ms,
        netem=netem,
    )
    write_json(run_directory / 'profile.json', active_profile)

    suite_deadline = time.monotonic() + suite_budget
    records = []
    for scenario in scenarios:
        print(f'\n=== {scenario.name} ===', flush=True)
        record = run_scenario(
            scenario,
            servers,
            duration=duration,
            warmup=warmup,
            max_trials=max_trials,
            deadline=min(time.monotonic() + scenario_budget, suite_deadline),
            hypercorn_config=hypercorn_config,
            evidence_directory=run_directory / 'evidence' / scenario.slug(),
            identity=identity,
        )
        records.append(record)
        record['network_profile'] = profile.name
        record['target_rtt_ms'] = profile.target_rtt_ms
        record['measured_rtt_ms'] = measured_rtt_ms
        record['system_summary'] = summary
        record['package_versions'] = versions
        if identity['build'] is not None:
            record['source_build'] = {
                'revision': identity['build']['source']['revision'],
                'wheel_sha256': identity['wheel']['sha256'],
            }
        record['duration'] = duration
        record['warmup_duration'] = warmup
        record['identity'] = f'../evidence/{profile.name}/identity.json'
        record['evidence'] = f'../evidence/{profile.name}/trials/{scenario.slug()}'
        write_json(run_directory / 'raw' / f'{scenario.slug()}.json', record)
        if record['stopped'] != 'max-trials':
            raise BenchmarkError(f'incomplete trial blocks for {scenario.name}')
        plot_results(
            record,
            run_directory / 'plots' / f'{scenario.slug()}.svg',
        )
        if time.monotonic() > suite_deadline:
            raise BenchmarkError(
                f'suite budget exhausted after {scenario.name}; '
                'raise --suite-budget or select fewer scenarios'
            )

    failed_cells = {
        record['scenario']: record['failures']
        for record in records
        if record['failures']
    }
    completed_profile = profile_record(
        profile,
        'completed_with_failures' if failed_cells else 'completed',
        measured_rtt_ms=measured_rtt_ms,
        netem=netem,
    )
    completed_profile['failed_cells'] = failed_cells
    write_json(
        run_directory / 'profile.json',
        completed_profile,
    )
    return run_directory


def main() -> int:
    parser = argparse.ArgumentParser(
        description='h2corn cross-server benchmark suite',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument('--family', choices=('http', 'grpc'), default='http')
    parser.add_argument('--servers', nargs='+', choices=[*SERVERS, 'grpcio', 'grpclib'])
    parser.add_argument('--rpc-cases', nargs='+')
    parser.add_argument('--connect-wheel', type=Path)
    parser.add_argument('--connect-development-revision')
    parser.add_argument('--connect-source', type=Path)
    parser.add_argument('--comparator-selection', type=Path)
    parser.add_argument('--qualify-only', action='store_true')
    benchmark_types = sorted({
        scenario.type for scenario in (*benchmark_scenarios(), *rtt50_scenarios())
    })
    parser.add_argument('--types', nargs='+', choices=benchmark_types)
    parser.add_argument(
        '--network-profile', choices=('all', 'loopback', 'rtt50'), default='all'
    )
    parser.add_argument(
        '--output-directory',
        type=Path,
        default=RESULTS_DIRECTORY,
        help='directory containing staged run artifacts',
    )
    parser.add_argument('--run-directory', type=Path, help=argparse.SUPPRESS)
    parser.add_argument('--duration', default=DURATION)
    parser.add_argument('--warmup-duration', default=WARMUP_DURATION)
    parser.add_argument('--max-trials', type=int)
    parser.add_argument(
        '--wheel',
        type=Path,
        help='Verify installed h2corn bytes against this explicitly built wheel',
    )
    parser.add_argument(
        '--build-wheel',
        type=Path,
        help='Build an identified release wheel into an empty directory, then exit',
    )
    parser.add_argument(
        '--scenario-budget', type=float, default=SCENARIO_BUDGET_SECONDS
    )
    parser.add_argument('--suite-budget', type=float, default=SUITE_BUDGET_SECONDS)
    parser.add_argument(
        '--publish',
        action='store_true',
        help='replace the checked-in plots and raw records with this run',
    )
    args = parser.parse_args()

    def interrupted(_signum: int, _frame: Any) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupted)
    if args.build_wheel is not None:
        print(
            f'Building release wheel; log: {args.build_wheel / "build.log"}',
            file=sys.stderr,
            flush=True,
        )
        try:
            wheel = build_wheel(
                Path(__file__).resolve().parents[2],
                args.build_wheel.resolve(),
            )
        except (OSError, RuntimeError, subprocess.SubprocessError) as error:
            print(f'release build failed: {error}', file=sys.stderr)
            return 1
        print(wheel)
        return 0
    if args.max_trials is None:
        args.max_trials = 12 if args.family == 'grpc' else MAX_TRIALS
    if args.family == 'grpc':
        from bench.grpc import SERVERS as RPC_SERVERS
        from bench.grpc import run_suite as run_rpc_suite
        from bench.grpc import scenarios

        if args.types or args.network_profile != 'all':
            parser.error(
                '--family grpc uses --rpc-cases and its own cleartext/TLS plans'
            )
        if args.servers and set(args.servers) - set(RPC_SERVERS):
            parser.error('gRPC servers are h2corn, hypercorn, grpcio, and grpclib')
        if args.rpc_cases and set(args.rpc_cases) - {s.case for s in scenarios()}:
            parser.error('unknown --rpc-cases selection')
        if not sys.platform.startswith('linux'):
            parser.error('gRPC benchmarking requires Linux PSS accounting')
        duration_seconds(args.duration)
        duration_seconds(args.warmup_duration)
        if args.connect_development_revision and (
            not args.qualify_only or not args.rpc_cases or args.connect_source is None
        ):
            parser.error(
                'development Connect requires --qualify-only, explicit --rpc-cases, and --connect-source'
            )
        if args.max_trials < MIN_TRIALS:
            parser.error(f'--max-trials must be at least {MIN_TRIALS}')
        if args.publish and (
            args.wheel is None
            or args.servers
            or args.rpc_cases
            or args.max_trials != 12
            or args.connect_development_revision
            or args.qualify_only
        ):
            parser.error(
                'gRPC publication requires the full released matrix, --wheel, and 12 blocks'
            )
        try:
            run_rpc_suite(args)
        except (BenchmarkError, OSError, subprocess.SubprocessError) as error:
            print(f'benchmark failed: {error}', file=sys.stderr)
            return 1
        return 0
    if (
        args.qualify_only
        or args.rpc_cases
        or args.connect_wheel
        or args.connect_development_revision
        or args.connect_source
        or args.comparator_selection
    ):
        parser.error('RPC qualification and application options require --family grpc')
    if args.servers and set(args.servers) - set(SERVERS):
        parser.error('grpcio and grpclib require --family grpc')
    if args.publish and args.wheel is None:
        parser.error('--publish requires --wheel to establish native artifact identity')
    if not sys.platform.startswith('linux'):
        parser.error(
            'this benchmark requires Linux PSS accounting and inherited listeners'
        )

    if args.max_trials < MIN_TRIALS:
        parser.error(f'--max-trials must be at least {MIN_TRIALS}')
    duration_seconds(args.duration)
    duration_seconds(args.warmup_duration)

    loopback_scenarios = [
        scenario
        for scenario in benchmark_scenarios()
        if not args.types or scenario.type in args.types
    ]
    shaped_scenarios = [
        scenario
        for scenario in rtt50_scenarios()
        if not args.types or scenario.type in args.types
    ]
    servers = args.servers or list(SERVERS)
    if args.publish and (args.servers or args.types or args.network_profile != 'all'):
        parser.error('--publish requires every server and both network profiles')
    selected_count = {
        'all': len(loopback_scenarios) + len(shaped_scenarios),
        'loopback': len(loopback_scenarios),
        'rtt50': len(shaped_scenarios),
    }[args.network_profile]
    if not selected_count:
        parser.error('scenario selection is empty')
    required = selected_count * args.scenario_budget
    if args.suite_budget < required:
        parser.error(
            f'--suite-budget {args.suite_budget:.0f}s cannot cover {selected_count} '
            f'scenarios at --scenario-budget {args.scenario_budget:.0f}s '
            f'(needs {required:.0f}s)'
        )

    try:
        output_directory = args.output_directory.resolve()
        if args.network_profile == 'all':
            suite_id = (
                datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')
                + f'-{secrets.token_hex(3)}'
            )
            suite_directory = output_directory / 'runs' / suite_id
            complete = []
            if loopback_scenarios:
                complete.append(
                    run_suite(
                        servers=servers,
                        scenarios=loopback_scenarios,
                        duration=args.duration,
                        warmup=args.warmup_duration,
                        max_trials=args.max_trials,
                        scenario_budget=args.scenario_budget,
                        suite_budget=args.suite_budget,
                        run_directory=suite_directory / LOOPBACK_PROFILE.name,
                        profile=LOOPBACK_PROFILE,
                        wheel=args.wheel,
                    )
                )
            if shaped_scenarios:
                shaped_directory = suite_directory / RTT50_PROFILE.name
                result, namespace_reason = run_rtt50_namespace(
                    shaped_directory,
                    timeout=args.suite_budget + 60,
                )
                profile_path = shaped_directory / 'profile.json'
                status = (
                    json.loads(profile_path.read_text()).get('status')
                    if profile_path.exists()
                    else None
                )
                if result not in (0, None) and status != 'skipped':
                    detail = f': {namespace_reason}' if namespace_reason else ''
                    raise BenchmarkError(
                        f'50 ms RTT profile exited with {result}{detail}'
                    )
                if status in {'completed', 'completed_with_failures'}:
                    complete.append(shaped_directory)
                elif status == 'skipped':
                    # Preserve the explicit skip record in checked-in raw data
                    # while emitting no unshaped plot for this profile.
                    complete.append(shaped_directory)
                elif status != 'skipped':
                    reason = (
                        f'unshare is unavailable: {namespace_reason}'
                        if result is None
                        else namespace_reason
                        or 'unshare failed before the netem profile could start'
                    )
                    print(
                        f'  SKIPPED {RTT50_PROFILE.description}: {reason}', flush=True
                    )
                    record_skipped_profile(
                        shaped_directory, RTT50_PROFILE, shaped_scenarios, reason
                    )
                    complete.append(shaped_directory)
            if args.publish:
                assert_publishable(complete)
                publish(complete)
        else:
            profile = (
                LOOPBACK_PROFILE
                if args.network_profile == 'loopback'
                else RTT50_PROFILE
            )
            scenarios = (
                loopback_scenarios if profile is LOOPBACK_PROFILE else shaped_scenarios
            )
            run_directory = args.run_directory or (
                output_directory
                / 'runs'
                / (
                    datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')
                    + f'-{secrets.token_hex(3)}'
                )
            )
            if profile is RTT50_PROFILE:
                if os.environ.get(NETEM_NAMESPACE_ENV) != '1':
                    result, namespace_reason = run_rtt50_namespace(
                        run_directory,
                        timeout=args.suite_budget + 60,
                    )
                    profile_path = run_directory / 'profile.json'
                    status = (
                        json.loads(profile_path.read_text()).get('status')
                        if profile_path.exists()
                        else None
                    )
                    if result == 0 or status == 'skipped':
                        return 0
                    if status is not None:
                        return result or 1
                    reason = (
                        f'unshare is unavailable: {namespace_reason}'
                        if result is None
                        else namespace_reason
                        or 'unshare failed before the netem profile could start'
                    )
                    print(
                        f'  SKIPPED {RTT50_PROFILE.description}: {reason}', flush=True
                    )
                    record_skipped_profile(run_directory, profile, scenarios, reason)
                    return 0
                try:
                    netem = configure_rtt50_profile()
                    measured_rtt_ms = netem['measured_rtt_ms']
                except BenchmarkError as error:
                    print(f'  SKIPPED {RTT50_PROFILE.description}: {error}', flush=True)
                    record_skipped_profile(
                        run_directory, profile, scenarios, str(error)
                    )
                    return 0
            else:
                measured_rtt_ms = None
                netem = None
            completed = run_suite(
                servers=servers,
                scenarios=scenarios,
                duration=args.duration,
                warmup=args.warmup_duration,
                max_trials=args.max_trials,
                scenario_budget=args.scenario_budget,
                suite_budget=args.suite_budget,
                run_directory=run_directory,
                profile=profile,
                measured_rtt_ms=measured_rtt_ms,
                netem=netem,
                wheel=args.wheel,
            )
            if args.publish:
                assert_publishable([completed])
                publish([completed])
    except BenchmarkError as error:
        print(f'benchmark failed: {error}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
