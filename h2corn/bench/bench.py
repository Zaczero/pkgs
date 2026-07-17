import argparse
import base64
import hashlib
import importlib.metadata
import importlib.util
import json
import math
import os
import random
import re
import resource
import secrets
import shutil
import socket
import statistics
import struct
import subprocess
import tempfile
import time
import traceback
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, fields, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, TypeAlias, TypedDict

import matplotlib as mpl
from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.events import DataReceived, ResponseReceived, StreamEnded

mpl.use('Agg')
import matplotlib.pyplot as plt

try:
    from bench.system import (
        BenchmarkError,
        BenchmarkSystemState,
        CpuSetState,
        PollableProcess,
        ProcessGroupResourceSampler,
        ProcessGroupUsage,
        capture_system_state,
        derive_cpu_roles,
        parse_linux_cpu_list,
        physical_core_capacity,
        pin_benchmark_driver,
        terminate_process_group,
        validate_k6_result,
        validate_oha_result,
        wait_for_http_server,
        wait_for_unix_server,
        write_json,
    )
except ModuleNotFoundError:  # Direct ``python bench/bench.py`` execution.
    from system import (  # type: ignore[import-not-found, no-redef]
        BenchmarkError,
        BenchmarkSystemState,
        CpuSetState,
        PollableProcess,
        ProcessGroupResourceSampler,
        ProcessGroupUsage,
        capture_system_state,
        derive_cpu_roles,
        parse_linux_cpu_list,
        physical_core_capacity,
        pin_benchmark_driver,
        terminate_process_group,
        validate_k6_result,
        validate_oha_result,
        wait_for_http_server,
        wait_for_unix_server,
        write_json,
    )

DURATION = '3s'
WARMUP_DURATION = '1s'
CONCURRENCY = 100
STREAMING_CONCURRENCY = 1000
#: Trials adapt per scenario: at least MIN_TRIALS rotation-balanced rounds,
#: extended up to MAX_TRIALS only while any server's median is still unstable
#: (trial-to-trial IQR/median above STABLE_RELATIVE_SPREAD) and the time
#: budget allows. The spread bar is sized for a live development host —
#: single-digit jitter is normal; medians plus retained ranges carry it.
MIN_TRIALS = 3
MAX_TRIALS = 8
STABLE_RELATIVE_SPREAD = 0.10
#: Hard wall-clock cap for the whole suite. The scheduler divides the
#: remaining budget across remaining scenarios and shrinks per-trial load
#: duration (never below the floor) so the minimum trials always fit.
TIME_BUDGET = '15m'
TIME_BUDGET_SECONDS = 900.0
MIN_TRIAL_DURATION_SECONDS = 1.0
#: k6 pays a heavy per-run startup cost; WebSocket cells never shrink below
#: this so the measured window dominates the evidence.
K6_MIN_TRIAL_DURATION_SECONDS = 3.0
#: Estimated per-server-trial fixed cost: start + settle + warmup + teardown.
PER_CELL_OVERHEAD_SECONDS = 2.5
SETTLE_SECONDS = 0.25
ORDER_SEED = 20_260_712
RATE_LIMIT = None
SERVER_CPUS = None
LOAD_CPUS = None
LOAD_WORKER_THREADS = 16
MANAGEMENT_CPUS = None
MAX_LOAD_UTILIZATION = 0.85
MAX_LOAD_SCALING_GAIN = 0.02
HEADROOM_BLOCKS = 1
#: Quick plateau check margin: the fastest cell must not gain more than this
#: from extra load-generator workers, or the generator was the bottleneck.
HEADROOM_PLATEAU_MARGIN = 0.05
LOAD_GENERATOR_GRACE_SECONDS = 15.0
SERVER_READY_TIMEOUT_SECONDS = 5.0
HOST = '127.0.0.1'
PORT = 8000
UNIX_SOCKET_PATH = Path('bench/results/benchmark.sock')
RUNS_DIRECTORY = Path('bench/results/runs')
CANONICAL_RAW_DIRECTORY = Path('bench/results/raw')
CANONICAL_PLOT_DIRECTORY = Path('bench/results/plots')
STATIC_FILE_RESPONSE_PATH = Path('bench/_file_response_payload.bin')
STATIC_FILE_RESPONSE_BODY = b'\x00' * (128 * 1024)

# Every server runs with access logging ENABLED, matching each project's
# realistic production/default configuration and keeping the comparison fair:
# request-log construction is part of the work a real deployment does. h2corn
# and uvicorn log by default; hypercorn and gunicorn do not, so they are given
# an explicit `--access-logfile -`. Server stdout/stderr (including these logs)
# is captured to a bounded per-run temp file, so no server pays terminal I/O the
# others avoid. Do not disable access logging here: h2corn's log path is native
# (see src/access_log.rs), so turning it off would hide a real h2corn advantage.
SERVERS = {
    # h2corn: supports h1, h2, ws
    'h2corn': [
        'h2corn',
        'bench.bench_app:app',
        '--loop',
        'uvloop',
        '--runtime-threads',
        '2',
        '--loop-threads',
        '1',
        '--backlog',
        '2048',
    ],
    # uvicorn: supports h1, ws (no h2)
    'uvicorn': [
        'uvicorn',
        'bench.bench_app:app',
        '--loop',
        'asyncio',
        '--http',
        'h11',
        '--no-proxy-headers',
        '--backlog',
        '2048',
    ],
    # hypercorn: supports h1, h2, ws
    'hypercorn': [
        'hypercorn',
        'bench.bench_app:app',
        '-k',
        'uvloop',
        '--access-logfile',
        '-',
        '--backlog',
        '2048',
    ],
    # Gunicorn's first-party ASGI worker supports h1 and ws (no h2).
    'gunicorn': [
        'gunicorn',
        'bench.bench_app:app',
        '-k',
        'asgi',
        '--asgi-loop',
        'uvloop',
        '--http-parser',
        'python',
        '--no-control-socket',
        '--access-logfile',
        '-',
        '--backlog',
        '2048',
    ],
}

SERVER_PROFILES = {
    'h2corn': 'uvloop + 2 Tokio runtime threads + 1 Python loop; access logging enabled; backlog 2048',
    'uvicorn': 'classic install; stdlib asyncio + h11; access logging enabled; WebSockets disabled outside the WebSocket cell and separately installed websockets selected there; backlog 2048',
    'hypercorn': 'uvloop worker; access logging enabled; backlog 2048',
    'gunicorn': 'first-party ASGI worker + uvloop + Python HTTP parser; access logging enabled; backlog 2048',
}

SERVER_COLORS = {
    'h2corn': '#4477AA',
    'gunicorn': '#228833',
    'hypercorn': '#CC3311',
    'uvicorn': '#B39B00',
}

SERVER_MARKERS = {
    'h2corn': 'o',
    'gunicorn': 's',
    'hypercorn': 'D',
    'uvicorn': '^',
}

SERVER_LINESTYLES = {
    'h2corn': '-',
    'gunicorn': ':',
    'hypercorn': '-.',
    'uvicorn': '--',
}

FALLBACK_COLOR = '#4C4C4C'

# Source trees which can materially affect a comparison, including optional
# protocol and event-loop backends selected by the explicit server profiles.
BENCHMARK_PYTHON_MODULES: tuple[str, ...] = (
    'uvicorn',
    'hypercorn',
    'gunicorn',
    'starlette',
    'anyio',
    'h11',
    'h2',
    'hpack',
    'hyperframe',
    'priority',
    'wsproto',
    'websockets',
    'uvloop',
)

BenchmarkType: TypeAlias = Literal[
    'h1',
    'h1_uds',
    'h2',
    'h1_file',
    'h2_file',
    'h1_download',
    'h2_download',
    'h1_stream',
    'h2_stream',
    'ws',
]
HeadroomPhase: TypeAlias = Literal['warmup', 'measured']
HeadroomVariant: TypeAlias = Literal['reduced', 'full']
RunStatus: TypeAlias = Literal['complete', 'failed']


class CommandEnvironment(TypedDict, total=False):
    TOKIO_WORKER_THREADS: str
    GOMAXPROCS: str


class CommandIdentity(TypedDict):
    argv: list[str]
    environment: CommandEnvironment


class Metrics(TypedDict):
    rps: float
    latency_percentiles: dict[str, float]


class AggregateMetrics(Metrics):
    rps_samples: list[float]
    rps_range: list[float]
    latency_percentile_samples: dict[str, list[float]]
    latency_percentile_ranges: dict[str, list[float]]


class LoadGeneratorUsage(TypedDict):
    elapsed_seconds: float
    cpu_seconds: float
    logical_cpu_capacity: int
    physical_core_capacity: int
    logical_cpu_utilization: float
    physical_core_utilization: float
    physical_core_headroom: float
    maximum_publish_physical_core_utilization: float
    sufficient_headroom: bool


class ServerProcessRecord(TypedDict):
    command: list[str]
    log: str
    worker_pids: list[int]
    process_group_id: int


class HeadroomRun(TypedDict):
    phase: HeadroomPhase
    variant: HeadroomVariant
    command: CommandIdentity
    load_generator_usage: LoadGeneratorUsage
    server_resource_usage: ProcessGroupUsage
    raw: dict[str, Any]
    metrics: Metrics


class ScenarioLoadEvidence(TypedDict):
    load_command: CommandIdentity
    load_generator_usage: LoadGeneratorUsage
    raw: dict[str, Any]
    metrics: Metrics | None


class HeadroomLadder(TypedDict):
    server: str
    warmup_order: list[HeadroomVariant]
    order: list[HeadroomVariant]
    load_cpus: list[int]
    reduced_worker_threads: int
    full_worker_threads: int
    reduced_rps_samples: list[float]
    full_rps_samples: list[float]
    paired_gain_samples: list[float]
    full_vs_reduced_gain: float
    maximum_publish_gain: float
    all_runs_have_cpu_headroom: bool
    plateau_observed: bool
    runs: list[HeadroomRun]
    correctness: dict[str, Any]


class ScenarioRecord(TypedDict):
    status: RunStatus
    scenario: dict[str, Any]
    excluded_servers: dict[str, str]
    runs: list[dict[str, Any]]
    aggregate: dict[str, AggregateMetrics]
    trials_run: int
    stopping_reason: str | None
    load_generator_headroom: HeadroomLadder | dict[str, str] | None
    error: str | None


class ManifestRecord(TypedDict):
    status: RunStatus
    run_identity: dict[str, Any]
    scenarios: dict[str, RunStatus]
    publication_failures: list[str]
    load_generator_headroom: dict[str, HeadroomLadder | dict[str, str]]
    error: str | None


@dataclass(frozen=True, slots=True)
class HarnessConfig:
    duration: str = DURATION
    warmup_duration: str = WARMUP_DURATION
    concurrency: int = CONCURRENCY
    min_trials: int = MIN_TRIALS
    max_trials: int = MAX_TRIALS
    stable_relative_spread: float = STABLE_RELATIVE_SPREAD
    time_budget_seconds: float = TIME_BUDGET_SECONDS
    settle_seconds: float = SETTLE_SECONDS
    order_seed: int = ORDER_SEED
    rate_limit_qps: int | None = RATE_LIMIT
    server_cpus: tuple[int, ...] | None = SERVER_CPUS
    load_cpus: tuple[int, ...] | None = LOAD_CPUS
    management_cpus: tuple[int, ...] | None = MANAGEMENT_CPUS
    load_worker_threads: int = LOAD_WORKER_THREADS
    max_load_utilization: float = MAX_LOAD_UTILIZATION
    max_load_scaling_gain: float = MAX_LOAD_SCALING_GAIN
    load_generator_grace_seconds: float = LOAD_GENERATOR_GRACE_SECONDS


@dataclass(frozen=True, slots=True)
class ResponseContract:
    """Single per-benchmark-type descriptor.

    Owns the request shape (path/method/body), the load driver, the wire
    protocol, and the exact expected response. The oha/k6 invocation, the
    correctness probe, and server eligibility all derive from this one source.
    """

    path: str
    driver: Literal['oha', 'k6'] = 'oha'
    protocol: Literal['1.1', '2', 'websocket'] = '1.1'
    unix_socket: bool = False
    method: Literal['GET', 'POST'] = 'GET'
    request_body: bytes = b''
    request_content_type: str | None = None
    response_body: bytes = b''
    content_type: str = 'text/plain'

    def load_url(self) -> str:
        if self.driver == 'k6':
            return f'ws://{HOST}:{PORT}{self.path}'
        if self.unix_socket:
            return f'http://localhost{self.path}'
        return f'http://{HOST}:{PORT}{self.path}'


RESPONSE_CONTRACTS: dict[BenchmarkType, ResponseContract] = {
    'h1': ResponseContract('/', response_body=b'Hello, World!'),
    'h1_uds': ResponseContract('/', unix_socket=True, response_body=b'Hello, World!'),
    'h2': ResponseContract('/', protocol='2', response_body=b'Hello, World!'),
    'h1_file': ResponseContract(
        '/static-file',
        response_body=STATIC_FILE_RESPONSE_BODY,
        content_type='application/octet-stream',
    ),
    'h2_file': ResponseContract(
        '/static-file',
        protocol='2',
        response_body=STATIC_FILE_RESPONSE_BODY,
        content_type='application/octet-stream',
    ),
    'h1_download': ResponseContract(
        '/streaming-download',
        response_body=b'x' * (128 * 1024),
        content_type='application/octet-stream',
    ),
    'h2_download': ResponseContract(
        '/streaming-download',
        protocol='2',
        response_body=b'x' * (128 * 1024),
        content_type='application/octet-stream',
    ),
    'h1_stream': ResponseContract(
        '/streaming-post',
        method='POST',
        request_body=b'x' * 1024,
        request_content_type='application/octet-stream',
        response_body=b'stream-started\n1024\nstream-finished\n',
    ),
    'h2_stream': ResponseContract(
        '/streaming-post',
        protocol='2',
        method='POST',
        request_body=b'x' * 1024,
        request_content_type='application/octet-stream',
        response_body=b'stream-started\n1024\nstream-finished\n',
    ),
    'ws': ResponseContract(
        '/ws',
        driver='k6',
        protocol='websocket',
        response_body=b'h2corn-bench-echo',
    ),
}


@dataclass(frozen=True, slots=True)
class Scenario:
    name: str
    workers: int
    type: BenchmarkType
    concurrency: int | None = None
    http2_parallelism: int = 1


@dataclass(frozen=True, slots=True)
class LoadResult:
    raw: dict[str, Any]
    command: CommandIdentity
    usage: LoadGeneratorUsage


def get_bind_args(server_name, socket_path=None):
    if server_name == 'uvicorn':
        if socket_path is None:
            return ['--host', HOST, '--port', str(PORT)]
        return ['--uds', str(socket_path)]

    bind_target = f'{HOST}:{PORT}' if socket_path is None else f'unix:{socket_path}'
    if server_name in {'h2corn', 'hypercorn', 'gunicorn'}:
        return ['-b', bind_target]

    raise ValueError(f'unsupported server binding mode for {server_name}')


def get_server_command(
    server_name: str,
    workers: int,
    benchmark_type: BenchmarkType,
    socket_path: Path | None = None,
    *,
    server_cpus: tuple[int, ...] | None = SERVER_CPUS,
) -> list[str]:
    cmd = _affinity_prefix(server_cpus) + SERVERS[server_name].copy()
    if server_name == 'uvicorn':
        cmd.extend([
            '--ws',
            'websockets' if benchmark_type == 'ws' else 'none',
        ])
    cmd.extend(get_bind_args(server_name, socket_path))
    if server_name == 'h2corn':
        cmd.extend(['-w', str(workers)])
    elif server_name in {'uvicorn', 'hypercorn'}:
        cmd.extend(['--workers', str(workers)])
    elif server_name == 'gunicorn':
        cmd.extend(['-w', str(workers)])
    return cmd


def _affinity_prefix(cpus):
    if not cpus:
        return []
    return ['taskset', '--cpu-list', ','.join(map(str, cpus))]


def _worker_pid_response(socket_path=None):
    request = (
        b'GET /__bench/worker-pid HTTP/1.1\r\n'
        b'Host: localhost\r\n'
        b'Connection: close\r\n\r\n'
    )
    family = socket.AF_UNIX if socket_path is not None else socket.AF_INET
    address = str(socket_path) if socket_path is not None else (HOST, PORT)
    with socket.socket(family, socket.SOCK_STREAM) as client:
        client.settimeout(1)
        client.connect(address)
        client.sendall(request)
        chunks = []
        while chunk := client.recv(4096):
            chunks.append(chunk)

    response = b''.join(chunks)
    head, separator, body = response.partition(b'\r\n\r\n')
    if not separator or not head.startswith(b'HTTP/1.1 200 '):
        raise BenchmarkError('worker identity probe returned a non-200 response')
    try:
        return int(body.strip())
    except ValueError as error:
        raise BenchmarkError('worker identity probe returned an invalid PID') from error


def wait_for_worker_pids(
    workers: int,
    socket_path: Path | None = None,
    timeout: float = 15.0,
    *,
    process: PollableProcess | None = None,
) -> tuple[bool, set[int]]:
    """Collect the distinct responsive worker-PID set within the deadline.

    With ``process`` supplied (startup readiness), an exited server aborts the
    wait early; without it (post-load observation), only the deadline applies.
    """
    seen = set()
    deadline = time.monotonic() + timeout
    while len(seen) < workers and time.monotonic() < deadline:
        if process is not None and process.poll() is not None:
            return False, seen
        try:
            seen.add(_worker_pid_response(socket_path))
        except (OSError, BenchmarkError):
            pass
        time.sleep(0.01)
    return len(seen) == workers, seen


def response_contract(config_type: BenchmarkType) -> ResponseContract:
    try:
        return RESPONSE_CONTRACTS[config_type]
    except KeyError as error:
        raise BenchmarkError(f'unsupported benchmark type: {config_type}') from error


def _decode_chunked_body(body: bytes) -> bytes:
    decoded = bytearray()
    offset = 0
    while True:
        line_end = body.find(b'\r\n', offset)
        if line_end < 0:
            raise BenchmarkError('correctness probe received malformed chunked body')
        size_text = body[offset:line_end].partition(b';')[0]
        try:
            size = int(size_text, 16)
        except ValueError as error:
            raise BenchmarkError(
                'correctness probe received invalid chunk size'
            ) from error
        offset = line_end + 2
        if size == 0:
            return bytes(decoded)
        end = offset + size
        if body[end : end + 2] != b'\r\n':
            raise BenchmarkError('correctness probe received truncated chunk')
        decoded.extend(body[offset:end])
        offset = end + 2


def _http_correctness_probe(
    contract: ResponseContract, socket_path: Path | None
) -> dict[str, Any]:
    family = socket.AF_UNIX if socket_path is not None else socket.AF_INET
    address: str | tuple[str, int]
    address = str(socket_path) if socket_path is not None else (HOST, PORT)
    headers = [
        f'{contract.method} {contract.path} HTTP/1.1',
        'Host: localhost',
        'Connection: close',
    ]
    if contract.request_body:
        headers.extend([
            f'Content-Length: {len(contract.request_body)}',
            'Content-Type: application/octet-stream',
        ])
    request = ('\r\n'.join(headers) + '\r\n\r\n').encode() + contract.request_body
    with socket.socket(family, socket.SOCK_STREAM) as connection:
        connection.settimeout(5)
        connection.connect(address)
        connection.sendall(request)
        chunks: list[bytes] = []
        while chunk := connection.recv(64 * 1024):
            chunks.append(chunk)
    response = b''.join(chunks)
    head, separator, body = response.partition(b'\r\n\r\n')
    if not separator:
        raise BenchmarkError('correctness probe received malformed HTTP response')
    lines = head.split(b'\r\n')
    status_parts = lines[0].split(b' ', 2)
    if len(status_parts) < 2 or status_parts[1] != b'200':
        raise BenchmarkError(f'correctness probe expected HTTP 200, got {lines[0]!r}')
    response_headers: dict[str, str] = {}
    for line in lines[1:]:
        name, separator, value = line.partition(b':')
        if not separator:
            raise BenchmarkError('correctness probe received malformed HTTP headers')
        response_headers[name.decode().lower()] = value.decode().strip()
    if response_headers.get('transfer-encoding', '').lower() == 'chunked':
        body = _decode_chunked_body(body)
    content_type = response_headers.get('content-type', '')
    if not content_type.startswith(contract.content_type):
        raise BenchmarkError(
            f'correctness probe expected {contract.content_type}, got {content_type!r}'
        )
    if body != contract.response_body:
        raise BenchmarkError(
            'correctness probe body mismatch: '
            f'expected {len(contract.response_body)} bytes with SHA-256 '
            f'{hashlib.sha256(contract.response_body).hexdigest()}, got '
            f'{len(body)} bytes with SHA-256 {hashlib.sha256(body).hexdigest()}'
        )
    return {
        'protocol': 'http/1.1',
        'status': 200,
        'content_type': content_type,
        'body_bytes': len(body),
        'body_sha256': hashlib.sha256(body).hexdigest(),
    }


def _http2_correctness_probe(contract: ResponseContract) -> dict[str, Any]:
    protocol = H2Connection(H2Configuration(header_encoding='utf-8'))
    protocol.initiate_connection()
    stream_id = protocol.get_next_available_stream_id()
    headers = [
        (':method', contract.method),
        (':scheme', 'http'),
        (':authority', f'{HOST}:{PORT}'),
        (':path', contract.path),
    ]
    if contract.request_body:
        headers.extend([
            ('content-length', str(len(contract.request_body))),
            ('content-type', 'application/octet-stream'),
        ])
    protocol.send_headers(stream_id, headers, end_stream=not contract.request_body)
    if contract.request_body:
        protocol.send_data(stream_id, contract.request_body, end_stream=True)

    status: str | None = None
    content_type = ''
    body = bytearray()
    ended = False
    with socket.create_connection((HOST, PORT), timeout=5) as connection:
        connection.sendall(protocol.data_to_send())
        while not ended:
            data = connection.recv(64 * 1024)
            if not data:
                raise BenchmarkError('HTTP/2 correctness probe response ended early')
            for event in protocol.receive_data(data):
                if isinstance(event, ResponseReceived) and event.stream_id == stream_id:
                    response_headers = {
                        key.decode() if isinstance(key, bytes) else key: value.decode()
                        if isinstance(value, bytes)
                        else value
                        for key, value in event.headers
                    }
                    status = response_headers.get(':status')
                    content_type = response_headers.get('content-type', '')
                elif isinstance(event, DataReceived) and event.stream_id == stream_id:
                    body.extend(event.data)
                    protocol.acknowledge_received_data(
                        event.flow_controlled_length, stream_id
                    )
                elif isinstance(event, StreamEnded) and event.stream_id == stream_id:
                    ended = True
            outbound = protocol.data_to_send()
            if outbound:
                connection.sendall(outbound)

    if status != '200':
        raise BenchmarkError(f'correctness probe expected HTTP 200, got {status!r}')
    if not content_type.startswith(contract.content_type):
        raise BenchmarkError(
            f'correctness probe expected {contract.content_type}, got {content_type!r}'
        )
    response_body = bytes(body)
    if response_body != contract.response_body:
        raise BenchmarkError(
            'HTTP/2 correctness probe body mismatch: '
            f'expected {len(contract.response_body)} bytes with SHA-256 '
            f'{hashlib.sha256(contract.response_body).hexdigest()}, got '
            f'{len(response_body)} bytes with SHA-256 '
            f'{hashlib.sha256(response_body).hexdigest()}'
        )
    return {
        'protocol': 'h2',
        'status': 200,
        'content_type': content_type,
        'body_bytes': len(response_body),
        'body_sha256': hashlib.sha256(response_body).hexdigest(),
    }


def _receive_until(
    connection: socket.socket,
    buffer: bytearray,
    delimiter: bytes,
    *,
    label: str,
) -> None:
    while delimiter not in buffer:
        chunk = connection.recv(4096)
        if not chunk:
            raise BenchmarkError(f'{label} ended before {delimiter!r}')
        buffer.extend(chunk)


def _receive_at_least(
    connection: socket.socket,
    buffer: bytearray,
    size: int,
    *,
    label: str,
) -> None:
    while len(buffer) < size:
        chunk = connection.recv(size - len(buffer))
        if not chunk:
            raise BenchmarkError(f'{label} ended after {len(buffer)}/{size} bytes')
        buffer.extend(chunk)


def _websocket_correctness_probe(contract: ResponseContract) -> dict[str, Any]:
    key = base64.b64encode(secrets.token_bytes(16)).decode()
    request = (
        f'GET {contract.path} HTTP/1.1\r\n'
        f'Host: {HOST}:{PORT}\r\n'
        'Upgrade: websocket\r\n'
        'Connection: Upgrade\r\n'
        f'Sec-WebSocket-Key: {key}\r\n'
        'Sec-WebSocket-Version: 13\r\n\r\n'
    ).encode()
    expected_accept = base64.b64encode(
        hashlib.sha1(  # noqa: S324 - required by RFC 6455
            (key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()
        ).digest()
    ).decode()
    with socket.create_connection((HOST, PORT), timeout=5) as connection:
        connection.sendall(request)
        handshake = bytearray()
        _receive_until(
            connection,
            handshake,
            b'\r\n\r\n',
            label='WebSocket handshake',
        )
        head, _, remainder = bytes(handshake).partition(b'\r\n\r\n')
        if not head.startswith(b'HTTP/1.1 101 '):
            raise BenchmarkError('WebSocket probe did not receive HTTP 101')
        headers = {
            name.decode().lower(): value.decode().strip()
            for line in head.split(b'\r\n')[1:]
            for name, separator, value in [line.partition(b':')]
            if separator
        }
        if headers.get('sec-websocket-accept') != expected_accept:
            raise BenchmarkError('WebSocket probe received an invalid accept key')
        if headers.get('upgrade', '').lower() != 'websocket':
            raise BenchmarkError('WebSocket probe received an invalid Upgrade header')
        if 'upgrade' not in {
            token.strip().lower() for token in headers.get('connection', '').split(',')
        }:
            raise BenchmarkError(
                'WebSocket probe received an invalid Connection header'
            )
        mask = secrets.token_bytes(4)
        payload = contract.response_body
        masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        connection.sendall(bytes([0x81, 0x80 | len(payload)]) + mask + masked)
        frame = bytearray(remainder)
        _receive_at_least(connection, frame, 2, label='WebSocket response frame')
        first, second = frame[:2]
        offset = 2
        length = second & 0x7F
        if length == 126:
            _receive_at_least(
                connection,
                frame,
                offset + 2,
                label='WebSocket response frame length',
            )
            length = struct.unpack('!H', frame[offset : offset + 2])[0]
            offset += 2
        elif length == 127:
            _receive_at_least(
                connection,
                frame,
                offset + 8,
                label='WebSocket response frame length',
            )
            length = struct.unpack('!Q', frame[offset : offset + 8])[0]
            offset += 8
        _receive_at_least(
            connection,
            frame,
            offset + length,
            label='WebSocket response frame payload',
        )
        echoed = bytes(frame[offset : offset + length])
        if first != 0x81 or second & 0x80 or echoed != payload:
            raise BenchmarkError('WebSocket probe did not echo the exact text frame')
    return {
        'protocol': 'websocket',
        'status': 101,
        'body_bytes': len(payload),
        'body_sha256': hashlib.sha256(payload).hexdigest(),
    }


def validate_response_contract(
    config_type: BenchmarkType, socket_path: Path | None = None
) -> dict[str, Any]:
    contract = response_contract(config_type)
    try:
        if contract.protocol == 'websocket':
            return _websocket_correctness_probe(contract)
        if contract.protocol == '2':
            return _http2_correctness_probe(contract)
        return _http_correctness_probe(contract, socket_path)
    except OSError as error:
        raise BenchmarkError(
            f'{config_type} correctness probe transport failed: {error}'
        ) from error


def get_versions() -> dict[str, str | None]:
    # httptools is recorded only as ambient environment metadata. The Uvicorn
    # benchmark profile is the classic install and explicitly selects h11.
    packages = ('h2corn', *BENCHMARK_PYTHON_MODULES, 'httptools')
    versions: dict[str, str | None] = {}
    for package in packages:
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = None
    return versions


def balanced_orders(
    names: list[str], trials: int, seed: int = ORDER_SEED
) -> list[list[str]]:
    """Return seeded Latin-rotation trial orders, alternating direction.

    Consecutive trials interleave forward and reverse rotations so lead and
    position bias cancels as quickly as the adaptive trial count allows;
    medians over rotated trials absorb the remainder.
    """
    if not names:
        raise ValueError('at least one benchmark server is required')
    if trials < 1:
        raise ValueError('at least one trial is required')
    base = list(names)
    random.Random(seed).shuffle(base)
    forward = [base[index:] + base[:index] for index in range(len(base))]
    reverse = [list(reversed(order)) for order in forward]
    cycle = [order for pair in zip(forward, reverse, strict=True) for order in pair]
    repeats = -(-trials // len(cycle))
    return (cycle * repeats)[:trials]


def _relative_spread(values: list[float]) -> float:
    """Trial-to-trial IQR relative to the median; inf when degenerate."""
    median = statistics.median(values)
    if median <= 0.0:
        return math.inf
    lower, _, upper = statistics.quantiles(values, n=4, method='inclusive')
    return (upper - lower) / median


def _server_stable(values: list[Metrics], harness: HarnessConfig) -> bool:
    rps = [metrics['rps'] for metrics in values]
    return (
        len(rps) >= harness.min_trials
        and _relative_spread(rps) <= harness.stable_relative_spread
    )


def _scenario_stable(
    samples: dict[str, list[Metrics]],
    excluded_servers: dict[str, str],
    harness: HarnessConfig,
) -> bool:
    """Every active server's throughput median has stabilized."""
    active = {
        server: values
        for server, values in samples.items()
        if server not in excluded_servers
    }
    return bool(active) and all(
        _server_stable(values, harness) for values in active.values()
    )


def _trial_floor_seconds(scenario: Scenario) -> float:
    return (
        K6_MIN_TRIAL_DURATION_SECONDS
        if scenario.type == 'ws'
        else MIN_TRIAL_DURATION_SECONDS
    )


def _scenario_minimum_seconds(
    scenario: Scenario, harness: HarnessConfig, server_count: int
) -> float:
    """Cost of this scenario's minimum trials at the duration floor."""
    return (
        harness.min_trials
        * max(1, server_count)
        * (PER_CELL_OVERHEAD_SECONDS + _trial_floor_seconds(scenario))
    )


def _scenario_budget(
    scenario: Scenario,
    harness: HarnessConfig,
    eligible: list[str],
    deadline: float,
    remaining_minimums: list[float],
) -> tuple[float, str]:
    """This scenario's extension deadline and per-trial load duration.

    The per-trial duration comes from a cost-weighted share of the remaining
    budget (a four-server scenario earns a proportionally larger share than a
    two-server one) and shrinks from the configured base toward the floor.
    Extension rounds beyond the minimum may borrow any slack the remaining
    scenarios' minimums do not need — early noisy scenarios converge instead
    of starving while later stable ones return their budget.
    """
    remaining = max(0.0, deadline - time.monotonic())
    weight = remaining_minimums[0] / max(1e-9, sum(remaining_minimums))
    share = remaining * weight
    base = _duration_seconds(harness.duration)
    floor = _trial_floor_seconds(scenario)
    per_trial = share / (harness.min_trials * max(1, len(eligible)))
    duration = min(
        max(base, floor),
        max(floor, per_trial - PER_CELL_OVERHEAD_SECONDS),
    )
    reserve = sum(remaining_minimums[1:])
    return deadline - reserve, f'{int(duration * 1000)}ms'


def _finite_positive(value, label):
    if not isinstance(value, int | float) or not math.isfinite(value) or value <= 0:
        raise BenchmarkError(f'invalid benchmark metric {label}: {value!r}')
    return float(value)


def aggregate_metrics(samples: list[Metrics]) -> AggregateMetrics:
    if not samples:
        raise BenchmarkError('cannot aggregate an empty benchmark sample')
    percentiles = tuple(samples[0]['latency_percentiles'])
    if any(tuple(sample['latency_percentiles']) != percentiles for sample in samples):
        raise BenchmarkError('benchmark samples expose inconsistent percentiles')
    rps_samples = [sample['rps'] for sample in samples]
    latency_samples = {
        percentile: [sample['latency_percentiles'][percentile] for sample in samples]
        for percentile in percentiles
    }
    return {
        'rps': statistics.median(rps_samples),
        'rps_samples': rps_samples,
        'rps_range': [min(rps_samples), max(rps_samples)],
        'latency_percentiles': {
            percentile: statistics.median(values)
            for percentile, values in latency_samples.items()
        },
        'latency_percentile_samples': latency_samples,
        'latency_percentile_ranges': {
            percentile: [min(values), max(values)]
            for percentile, values in latency_samples.items()
        },
    }


def _ensure_static_file_response_payload() -> None:
    try:
        if STATIC_FILE_RESPONSE_PATH.read_bytes() == STATIC_FILE_RESPONSE_BODY:
            return
    except OSError:
        pass
    STATIC_FILE_RESPONSE_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary = STATIC_FILE_RESPONSE_PATH.with_suffix('.bin.tmp')
    try:
        temporary.write_bytes(STATIC_FILE_RESPONSE_BODY)
        temporary.replace(STATIC_FILE_RESPONSE_PATH)
    finally:
        temporary.unlink(missing_ok=True)


def _git_head() -> str | None:
    result = subprocess.run(
        ['git', 'rev-parse', 'HEAD'], capture_output=True, text=True, check=False
    )
    return result.stdout.strip() if result.returncode == 0 else None


def _child_cpu_seconds() -> float:
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return usage.ru_utime + usage.ru_stime


def _duration_seconds(value: str) -> float:
    match = re.fullmatch(r'([0-9]+(?:\.[0-9]+)?)(ns|us|ms|s|m|h)', value)
    if match is None:
        raise BenchmarkError(
            f'invalid benchmark duration {value!r}; expected e.g. 500ms, 10s, or 2m'
        )
    magnitude = float(match.group(1))
    scale = {
        'ns': 1e-9,
        'us': 1e-6,
        'ms': 1e-3,
        's': 1.0,
        'm': 60.0,
        'h': 3600.0,
    }[match.group(2)]
    seconds = magnitude * scale
    if not 0.0 < seconds <= 3600.0:
        raise BenchmarkError('benchmark duration must be in (0, 3600] seconds')
    return seconds


def _load_capacity(harness: HarnessConfig) -> tuple[int, int]:
    cpus = harness.load_cpus or tuple(sorted(os.sched_getaffinity(0)))
    return len(cpus), physical_core_capacity(cpus)


def _run_load_command(
    command: list[str],
    harness: HarnessConfig,
    environment: CommandEnvironment | None = None,
) -> tuple[subprocess.CompletedProcess[str], LoadGeneratorUsage]:
    before_cpu = _child_cpu_seconds()
    started = time.monotonic()
    env = os.environ.copy()
    env.pop('NO_COLOR', None)
    if environment is not None:
        if (tokio_workers := environment.get('TOKIO_WORKER_THREADS')) is not None:
            env['TOKIO_WORKER_THREADS'] = tokio_workers
        if (go_workers := environment.get('GOMAXPROCS')) is not None:
            env['GOMAXPROCS'] = go_workers
    process: subprocess.Popen[str] | None = None
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
            start_new_session=True,
        )
        stdout, stderr = process.communicate(
            timeout=(
                _duration_seconds(harness.duration)
                + harness.load_generator_grace_seconds
            )
        )
    except subprocess.TimeoutExpired as error:
        if process is not None:
            terminate_process_group(process)
        raise BenchmarkError(
            f'load generator exceeded its bounded timeout: {" ".join(command)}'
        ) from error
    except OSError as error:
        if process is not None:
            terminate_process_group(process)
        raise BenchmarkError(
            f'failed to execute load generator {command[0]!r}: {error}'
        ) from error
    except BaseException:
        if process is not None:
            terminate_process_group(process)
        raise
    assert process.returncode is not None
    result = subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
    elapsed = max(time.monotonic() - started, 1e-9)
    cpu_seconds = max(_child_cpu_seconds() - before_cpu, 0.0)
    logical_capacity, physical_capacity = _load_capacity(harness)
    logical_utilization = cpu_seconds / (elapsed * logical_capacity)
    physical_utilization = cpu_seconds / (elapsed * physical_capacity)
    usage: LoadGeneratorUsage = {
        'elapsed_seconds': elapsed,
        'cpu_seconds': cpu_seconds,
        'logical_cpu_capacity': logical_capacity,
        'physical_core_capacity': physical_capacity,
        'logical_cpu_utilization': logical_utilization,
        'physical_core_utilization': physical_utilization,
        'physical_core_headroom': max(0.0, 1.0 - physical_utilization),
        'maximum_publish_physical_core_utilization': harness.max_load_utilization,
        'sufficient_headroom': physical_utilization <= harness.max_load_utilization,
    }
    return result, usage


def _oha_command(
    url: str,
    *,
    harness: HarnessConfig,
    http_version: Literal['1.1', '2'],
    method: str,
    body: str | None,
    content_type: str | None,
    concurrency: int,
    http2_parallelism: int,
    unix_socket: Path | None,
) -> list[str]:
    command = [
        *_affinity_prefix(harness.load_cpus),
        'oha',
        '-z',
        harness.duration,
        '-c',
        str(concurrency),
        '-p',
        str(http2_parallelism),
        '--output-format',
        'json',
        '--http-version',
        http_version,
        '-m',
        method,
    ]
    if body is not None:
        command.extend(['-d', body])
    if content_type is not None:
        command.extend(['-T', content_type])
    if unix_socket is not None:
        command.extend(['--unix-socket', str(unix_socket)])
    if harness.rate_limit_qps is not None:
        command.extend([
            '-q',
            str(harness.rate_limit_qps),
            '--latency-correction',
        ])
    command.append(url)
    return command


def run_oha(
    url: str,
    *,
    harness: HarnessConfig,
    http_version: Literal['1.1', '2'] = '1.1',
    method: str = 'GET',
    body: str | None = None,
    content_type: str | None = None,
    concurrency: int | None = None,
    http2_parallelism: int = 1,
    unix_socket: Path | None = None,
) -> LoadResult:
    if concurrency is None:
        concurrency = harness.concurrency
    cmd = _oha_command(
        url,
        harness=harness,
        http_version=http_version,
        method=method,
        body=body,
        content_type=content_type,
        concurrency=concurrency,
        http2_parallelism=http2_parallelism,
        unix_socket=unix_socket,
    )

    environment: CommandEnvironment = {
        'TOKIO_WORKER_THREADS': str(harness.load_worker_threads)
    }
    result, usage = _run_load_command(cmd, harness, environment)
    if result.returncode != 0:
        raise BenchmarkError(f'oha failed: {result.stderr.strip()}')
    try:
        raw = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise BenchmarkError('oha returned invalid JSON') from error
    if not isinstance(raw, dict):
        raise BenchmarkError('oha returned a non-object JSON result')
    return LoadResult(
        raw=raw,
        command={'argv': cmd, 'environment': environment},
        usage=usage,
    )


def run_k6(
    url: str,
    *,
    harness: HarnessConfig,
    summary_file: Path,
    concurrency: int | None = None,
) -> LoadResult:
    if concurrency is None:
        concurrency = harness.concurrency
    if summary_file.exists():
        summary_file.unlink()
    summary_file.parent.mkdir(parents=True, exist_ok=True)

    cmd = _k6_command(url, harness, summary_file, concurrency)

    environment: CommandEnvironment = {'GOMAXPROCS': str(harness.load_worker_threads)}
    result, usage = _run_load_command(cmd, harness, environment)
    if result.returncode != 0:
        raise BenchmarkError(f'k6 failed: {result.stderr.strip()}')
    if not summary_file.exists():
        raise BenchmarkError('k6 did not write its summary JSON')
    try:
        with open(summary_file) as file:
            raw = json.load(file)
    except json.JSONDecodeError as error:
        raise BenchmarkError('k6 returned invalid summary JSON') from error
    if not isinstance(raw, dict):
        raise BenchmarkError('k6 returned a non-object JSON result')
    return LoadResult(
        raw=raw,
        command={'argv': cmd, 'environment': environment},
        usage=usage,
    )


def _k6_command(
    url: str,
    harness: HarnessConfig,
    summary_file: Path,
    concurrency: int,
) -> list[str]:
    return [
        *_affinity_prefix(harness.load_cpus),
        'k6',
        'run',
        '--duration',
        harness.duration,
        '--vus',
        str(concurrency),
        '--summary-export',
        str(summary_file),
        '--summary-trend-stats',
        'med,p(90),p(95),p(99),p(99.9)',
        '-e',
        f'WS_URL={url}',
        'bench/k6/ws.js',
    ]


@contextmanager
def running_server(
    server_name: str,
    workers: int,
    benchmark_type: BenchmarkType,
    socket_path: Path | None = None,
    *,
    server_cpus: tuple[int, ...] | None = SERVER_CPUS,
) -> Iterator[ServerProcessRecord]:
    if socket_path is not None:
        socket_path.unlink(missing_ok=True)
    cmd = get_server_command(
        server_name,
        workers,
        benchmark_type,
        socket_path,
        server_cpus=server_cpus,
    )
    print(f'Starting {" ".join(cmd)}')
    process = None
    run: ServerProcessRecord = {
        'command': cmd,
        'log': '',
        'worker_pids': [],
        'process_group_id': 0,
    }
    with tempfile.TemporaryFile() as server_log:
        try:
            try:
                process = subprocess.Popen(
                    cmd,
                    stdout=server_log,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
            except OSError as error:
                raise BenchmarkError(
                    f'failed to start {server_name}: {error}'
                ) from error
            run['process_group_id'] = process.pid
            try:
                if socket_path is not None:
                    wait_for_unix_server(
                        process, socket_path, SERVER_READY_TIMEOUT_SECONDS
                    )
                else:
                    wait_for_http_server(
                        process,
                        f'http://{HOST}:{PORT}/',
                        SERVER_READY_TIMEOUT_SECONDS,
                    )
            except BenchmarkError as error:
                raise BenchmarkError(
                    f'failed to start {server_name} with {workers} workers: {error}'
                ) from error
            all_ready, worker_pids = wait_for_worker_pids(
                workers, socket_path, process=process
            )
            run['worker_pids'] = sorted(worker_pids)
            if not all_ready:
                raise BenchmarkError(
                    f'{server_name} exposed only {len(worker_pids)}/{workers} workers '
                    f'before the readiness deadline: {sorted(worker_pids)}'
                )
            yield run
        finally:
            if process is not None:
                terminate_process_group(process)
            server_log.flush()
            server_log.seek(0)
            run['log'] = server_log.read().decode(errors='replace')[-100_000:]
            if socket_path is not None:
                socket_path.unlink(missing_ok=True)


def extract_oha_metrics(oha_json: dict[str, Any]) -> Metrics | None:
    if not oha_json:
        return None

    rps = _finite_positive(
        oha_json.get('summary', {}).get('requestsPerSec'), 'requestsPerSec'
    )
    percentiles = oha_json.get('latencyPercentiles', {})

    mapped_pcts = {}
    for p in ['p50', 'p75', 'p90', 'p95', 'p99', 'p99.9']:
        val = percentiles.get(p)
        if val is not None:
            mapped_pcts[p] = _finite_positive(val, f'latencyPercentiles.{p}')

    return {'rps': rps, 'latency_percentiles': mapped_pcts}


def extract_k6_metrics(k6_json: dict[str, Any]) -> Metrics | None:
    if not k6_json:
        return None

    metrics = k6_json.get('metrics', {})
    if not metrics:
        return None

    # K6 RPS (iterations per second)
    rps_obj = metrics.get('iterations', {})
    rps = _finite_positive(
        rps_obj.get('rate') if isinstance(rps_obj, dict) else None,
        'iterations.rate',
    )

    # K6 Session Duration
    ws_duration = metrics.get('ws_session_duration', {})
    if not isinstance(ws_duration, dict):
        ws_duration = {}

    mapped_pcts = {}
    for source, target in (
        ('med', 'p50'),
        ('p(90)', 'p90'),
        ('p(95)', 'p95'),
        ('p(99)', 'p99'),
        ('p(99.9)', 'p99.9'),
    ):
        value = ws_duration.get(source)
        if value is not None:
            mapped_pcts[target] = (
                _finite_positive(value, f'ws_session_duration.{source}') / 1000.0
            )

    return {'rps': rps, 'latency_percentiles': mapped_pcts}


def extract_metrics(raw, config_type):
    contract = response_contract(config_type)  # Raises on an unknown type.
    if contract.driver == 'k6':
        return extract_k6_metrics(raw)
    return extract_oha_metrics(raw)


def metric_p99_ms(metrics):
    p99 = metrics.get('latency_percentiles', {}).get('p99')
    if p99 is None:
        return None
    return p99 * 1000.0


def format_server_summary(name, metrics, version=None, unit='RPS'):
    summary = f'{name}'
    if version is not None:
        summary += f' {version}'
    summary += f': {metrics["rps"]:,.2f} {unit}'
    p99_ms = metric_p99_ms(metrics)
    if p99_ms is not None:
        summary += f', p99 {p99_ms:.1f} ms'
    return summary


def format_alt_text(title, all_results, versions, unit='RPS'):
    parts = [
        format_server_summary(name, all_results[name], versions[name], unit)
        for name in all_results
    ]
    return f'{title}. {"; ".join(parts)}.'


def plot_results(
    all_results,
    title,
    filename,
    *,
    system_summary: str,
    websocket=False,
):
    fig, axes = plt.subplots(1, 2, figsize=(15, 6))
    fig.suptitle(title, fontsize=16)

    names = list(all_results.keys())
    versions = get_versions()
    display_names = [f'{name}\n{versions[name]}' for name in names]
    rps_values = [all_results[n].get('rps', 0) for n in names]
    bar_colors = [SERVER_COLORS.get(name, FALLBACK_COLOR) for name in names]

    ax1 = axes[0]
    bars = ax1.bar(
        display_names,
        rps_values,
        color=bar_colors,
        edgecolor='#222222',
        linewidth=1.0,
    )
    for bar, name in zip(bars, names, strict=True):
        samples = sorted(all_results[name].get('rps_samples', []))
        if len(samples) > 1:
            median = all_results[name]['rps']
            ax1.errorbar(
                bar.get_x() + bar.get_width() / 2,
                median,
                yerr=[[median - samples[0]], [samples[-1] - median]],
                fmt='none',
                color='#222222',
                capsize=4,
            )
    throughput_unit = 'sessions/s' if websocket else 'RPS'
    ax1.set_title(
        ('WebSocket Sessions Per Second' if websocket else 'Requests Per Second')
        + ' (Higher is better)'
    )
    ax1.set_ylabel(throughput_unit)
    for i, v in enumerate(rps_values):
        ax1.text(i, v, f'{v:,.0f}', ha='center', va='bottom')

    ax2 = axes[1]
    has_latency = False
    for name, display_name in zip(names, display_names, strict=False):
        res = all_results[name]
        if res.get('latency_percentiles'):
            pcts = res['latency_percentiles']
            sorted_pct_items = sorted(
                pcts.items(), key=lambda item: float(item[0].replace('p', ''))
            )
            x = [float(k.replace('p', '')) for k, _ in sorted_pct_items]
            y = [
                v * 1000 for _, v in sorted_pct_items if v is not None
            ]  # Convert to ms

            if len(x) == len(y) and len(y) > 0:
                ax2.plot(
                    x,
                    y,
                    color=SERVER_COLORS.get(name, FALLBACK_COLOR),
                    linestyle=SERVER_LINESTYLES.get(name, '-'),
                    marker=SERVER_MARKERS.get(name, 'o'),
                    label=display_name,
                    linewidth=2,
                    markersize=6,
                    markeredgecolor='#222222',
                    markeredgewidth=1.0,
                )
                has_latency = True

    if has_latency:
        latency_kind = 'Session duration' if websocket else 'Saturation latency'
        ax2.set_title(f'{latency_kind} Distribution (Lower is better)')
        ax2.set_xlabel('Percentile')
        ax2.set_ylabel('Latency (ms)')
        ax2.legend()
        ax2.grid(True, linestyle='--', alpha=0.7)
    else:
        ax2.set_title('Latency data unavailable')

    plt.figtext(
        0.02,
        0.02,
        system_summary,
        fontsize=9,
        verticalalignment='bottom',
        bbox={'boxstyle': 'round', 'facecolor': 'whitesmoke', 'alpha': 0.75},
    )

    plt.tight_layout(rect=(0.0, 0.05, 1.0, 0.95))
    plt.savefig(filename, format='svg')
    plt.close()
    svg_path = Path(filename)
    svg_path.write_text(
        '\n'.join(line.rstrip() for line in svg_path.read_text().splitlines()) + '\n'
    )
    print(f'Saved plot to {filename}')
    print(format_alt_text(title, all_results, versions, throughput_unit))


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
            concurrency=10,
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
            'HTTP/1 Streaming POST (1 Worker)',
            1,
            'h1_stream',
            STREAMING_CONCURRENCY,
        ),
        Scenario(
            'HTTP/1 Streaming POST (4 Workers)',
            4,
            'h1_stream',
            STREAMING_CONCURRENCY,
        ),
        Scenario(
            'HTTP/2 Streaming POST (1 Worker)',
            1,
            'h2_stream',
            STREAMING_CONCURRENCY,
        ),
        Scenario(
            'HTTP/2 Streaming POST (4 Workers)',
            4,
            'h2_stream',
            STREAMING_CONCURRENCY,
        ),
        Scenario('HTTP/1 WebSocket (1 Worker)', 1, 'ws'),
        Scenario('HTTP/1 WebSocket (4 Workers)', 4, 'ws'),
    )


# Bonferroni correction for the headroom plateau gate is derived from the
# actual scenario family, so adding or removing a scenario recalibrates the
# family-wise error rate instead of silently invalidating it. Publication
# re-asserts this size against the published scenario set.
def _clean_scenario_name(name: str) -> str:
    return (
        name
        .lower()
        .replace('/', '_')
        .replace(' ', '_')
        .replace('(', '')
        .replace(')', '')
    )


def _eligible_servers(scenario: Scenario, servers: list[str]) -> list[str]:
    if response_contract(scenario.type).protocol == '2':
        return [server for server in servers if server not in {'uvicorn', 'gunicorn'}]
    return servers.copy()


def _run_scenario_load(
    scenario: Scenario,
    harness: HarnessConfig,
    socket_path: Path | None,
    summary_file: Path,
) -> LoadResult:
    contract = response_contract(scenario.type)
    concurrency = scenario.concurrency or harness.concurrency
    if contract.driver == 'k6':
        return run_k6(
            contract.load_url(),
            harness=harness,
            summary_file=summary_file,
            concurrency=concurrency,
        )
    return run_oha(
        contract.load_url(),
        harness=harness,
        http_version='2' if contract.protocol == '2' else '1.1',
        method=contract.method,
        body=contract.request_body.decode() if contract.request_body else None,
        content_type=contract.request_content_type,
        concurrency=concurrency,
        http2_parallelism=scenario.http2_parallelism,
        unix_socket=socket_path if contract.unix_socket else None,
    )


def _run_scenario_warmup(
    scenario: Scenario,
    harness: HarnessConfig,
    socket_path: Path | None,
    summary_file: Path,
) -> LoadResult:
    """Exercise the request path under load before measuring this process."""
    return _run_scenario_load(
        scenario,
        replace(harness, duration=harness.warmup_duration),
        socket_path,
        summary_file,
    )


def _validate_scenario_load(
    scenario: Scenario,
    harness: HarnessConfig,
    load: LoadResult,
) -> Metrics:
    concurrency = (
        scenario.concurrency or harness.concurrency
    ) * scenario.http2_parallelism
    if scenario.type == 'ws':
        validate_k6_result(load.raw)
    else:
        validate_oha_result(load.raw, max_deadline_aborts=concurrency)
    metrics = extract_metrics(load.raw, scenario.type)
    if metrics is None:
        raise BenchmarkError(f'{scenario.name}: load generation produced no metrics')
    return metrics


def _scenario_load_evidence(
    load: LoadResult | None,
    metrics: Metrics | None,
) -> ScenarioLoadEvidence | None:
    if load is None:
        return None
    return {
        'load_command': load.command,
        'load_generator_usage': load.usage,
        'raw': load.raw,
        'metrics': metrics,
    }


def _measure_load_headroom(
    scenario: Scenario,
    server: str,
    harness: HarnessConfig,
    run_directory: Path,
) -> HeadroomLadder:
    if harness.load_cpus is None:
        raise BenchmarkError('load-generator headroom ladder requires pinned CPUs')
    if harness.load_worker_threads < 2:
        raise BenchmarkError(
            'load-generator headroom ladder requires at least two worker threads'
        )
    reduced_worker_threads = max(
        1,
        math.floor(harness.load_worker_threads * 0.75),
    )
    variants: dict[HeadroomVariant, HarnessConfig] = {
        'reduced': replace(
            harness,
            load_worker_threads=reduced_worker_threads,
        ),
        'full': harness,
    }
    warmup_order: list[HeadroomVariant] = ['reduced', 'full']
    order: list[HeadroomVariant] = [
        'reduced',
        'full',
        'full',
        'reduced',
    ] * HEADROOM_BLOCKS
    phases: tuple[
        tuple[HeadroomPhase, list[HeadroomVariant]],
        tuple[HeadroomPhase, list[HeadroomVariant]],
    ] = (('warmup', warmup_order), ('measured', order))
    socket_path = UNIX_SOCKET_PATH if scenario.type == 'h1_uds' else None
    runs: list[HeadroomRun] = []
    samples: dict[HeadroomVariant, list[float]] = {'reduced': [], 'full': []}
    with running_server(
        server,
        scenario.workers,
        scenario.type,
        socket_path,
        server_cpus=harness.server_cpus,
    ) as server_run:
        time.sleep(harness.settle_seconds)
        before = validate_response_contract(scenario.type, socket_path)
        for phase, phase_order in phases:
            for index, variant in enumerate(phase_order, start=1):
                resource_sampler = ProcessGroupResourceSampler(
                    server_run['process_group_id']
                )
                resource_sampler.start()
                try:
                    load = _run_scenario_load(
                        scenario,
                        variants[variant],
                        socket_path,
                        run_directory
                        / 'load'
                        / (
                            f'{_clean_scenario_name(scenario.name)}-headroom-'
                            f'{phase}-{index}-{variant}.json'
                        ),
                    )
                finally:
                    server_resources = resource_sampler.stop()
                metrics = _validate_scenario_load(scenario, variants[variant], load)
                if phase == 'measured':
                    samples[variant].append(metrics['rps'])
                run: HeadroomRun = {
                    'phase': phase,
                    'variant': variant,
                    'command': load.command,
                    'load_generator_usage': load.usage,
                    'server_resource_usage': server_resources,
                    'raw': load.raw,
                    'metrics': metrics,
                }
                runs.append(run)
        after = validate_response_contract(scenario.type, socket_path)
        all_workers, observed_worker_pids = wait_for_worker_pids(
            scenario.workers, socket_path
        )
        worker_pids_after = sorted(observed_worker_pids)
        if not all_workers or worker_pids_after != server_run['worker_pids']:
            raise BenchmarkError(
                f'{server} worker set changed across headroom loads: '
                f'before={server_run["worker_pids"]}, after={worker_pids_after}'
            )
    paired_gains: list[float] = []
    measured_runs = [run for run in runs if run['phase'] == 'measured']
    for first, second in zip(measured_runs[::2], measured_runs[1::2], strict=True):
        pair = {first['variant']: first, second['variant']: second}
        paired_gains.append(
            pair['full']['metrics']['rps'] / pair['reduced']['metrics']['rps'] - 1.0
        )
    gain = statistics.median(paired_gains)
    all_runs_have_headroom = all(
        run['load_generator_usage']['sufficient_headroom'] for run in runs
    )
    result: HeadroomLadder = {
        'server': server,
        'warmup_order': warmup_order,
        'order': order,
        'load_cpus': list(harness.load_cpus),
        'reduced_worker_threads': reduced_worker_threads,
        'full_worker_threads': harness.load_worker_threads,
        'reduced_rps_samples': samples['reduced'],
        'full_rps_samples': samples['full'],
        'paired_gain_samples': paired_gains,
        'full_vs_reduced_gain': gain,
        'maximum_publish_gain': HEADROOM_PLATEAU_MARGIN,
        'all_runs_have_cpu_headroom': all_runs_have_headroom,
        'plateau_observed': (gain < HEADROOM_PLATEAU_MARGIN and all_runs_have_headroom),
        'runs': runs,
        'correctness': {
            'before': before,
            'after': after,
            'worker_pids_before': server_run['worker_pids'],
            'worker_pids_after': worker_pids_after,
        },
    }
    return result


def _publish_artifacts(
    run_directory: Path, scenario_names: list[str], manifest: ManifestRecord
) -> None:
    """Replace the canonical raw and plot artifacts with this run's."""
    if manifest['status'] != 'complete':
        raise BenchmarkError('only a complete benchmark run can be published')
    sources: list[tuple[Path, Path]] = []
    for clean_name in scenario_names:
        for kind, suffix, canonical in (
            ('raw', '.json', CANONICAL_RAW_DIRECTORY),
            ('plots', '.svg', CANONICAL_PLOT_DIRECTORY),
        ):
            source = run_directory / kind / f'benchmark_{clean_name}{suffix}'
            if not source.is_file():
                raise BenchmarkError(f'publication artifact is missing: {source}')
            sources.append((source, canonical / source.name))
    for directory in (CANONICAL_RAW_DIRECTORY, CANONICAL_PLOT_DIRECTORY):
        directory.mkdir(parents=True, exist_ok=True)
        for stale in directory.glob('benchmark_*'):
            stale.unlink()
    for source, destination in sources:
        shutil.copy2(source, destination)
    (CANONICAL_RAW_DIRECTORY / 'run_manifest.json').write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + '\n'
    )


def _validate_publication_harness(
    harness: HarnessConfig, system: BenchmarkSystemState
) -> None:
    """Require one stable, documented methodology for canonical artifacts."""
    # CPU roles, worker threads, and the time budget are host-derived or
    # pacing knobs; everything else is the one documented methodology.
    canonical = HarnessConfig(
        server_cpus=harness.server_cpus,
        load_cpus=harness.load_cpus,
        management_cpus=harness.management_cpus,
        load_worker_threads=harness.load_worker_threads,
        time_budget_seconds=harness.time_budget_seconds,
    )
    changed = [
        field.name
        for field in fields(HarnessConfig)
        if getattr(harness, field.name) != getattr(canonical, field.name)
    ]
    if changed:
        raise BenchmarkError(
            'publication requires the canonical benchmark configuration; '
            'changed fields: ' + ', '.join(changed)
        )
    if harness.load_cpus is None or harness.management_cpus is None:
        raise BenchmarkError(
            'publication requires explicit load-generator and management CPUs'
        )
    if len(harness.management_cpus) != 1:
        raise BenchmarkError('publication requires exactly one management CPU')
    if len(harness.load_cpus) < 4:
        raise BenchmarkError(
            'publication requires at least four load-generator CPUs for '
            'a meaningful headroom comparison'
        )
    overlap = set(harness.management_cpus) & set(harness.load_cpus)
    if overlap:
        raise BenchmarkError(
            'publication requires disjoint load/management CPU sets; overlap: '
            + ','.join(map(str, sorted(overlap)))
        )
    load = system['load_generator']
    management = system['management']
    if load is None or management is None:
        raise BenchmarkError('publication requires captured CPU-role topology')
    selected = [*load['topology'], *management['topology']]
    offline = [entry['cpu'] for entry in selected if not entry['online']]
    if offline:
        raise BenchmarkError(
            'publication CPUs must be online; offline: ' + ','.join(map(str, offline))
        )
    if system['allowed_cpus'] != list(harness.management_cpus):
        raise BenchmarkError(
            'publication harness must be pinned to the management CPU; found '
            + repr(system['allowed_cpus'])
        )
    thread_masks = system['thread_affinity_masks']
    if (
        not thread_masks
        or any(mask['thread_count'] < 1 for mask in thread_masks)
        or any(mask['cpus'] != list(harness.management_cpus) for mask in thread_masks)
    ):
        raise BenchmarkError(
            'every benchmark harness thread must be pinned to the management CPU; '
            'found ' + repr(thread_masks)
        )
    if system['cgroup_allowed_cpus'] is None:
        raise BenchmarkError('publication requires cgroup CPU-availability provenance')
    disallowed = sorted(
        set(harness.load_cpus + harness.management_cpus)
        - set(system['cgroup_allowed_cpus'])
    )
    if disallowed:
        raise BenchmarkError(
            'publication CPUs are outside the cgroup CPU allowance: '
            + ','.join(map(str, disallowed))
        )

    def physical_keys(cpu_set: CpuSetState) -> set[tuple[int, int, int]]:
        return {
            (
                entry['physical_package_id'],
                entry['die_id'],
                entry['core_id'],
            )
            for entry in cpu_set['topology']
        }

    physical_overlap = physical_keys(management) & physical_keys(load)
    if physical_overlap:
        raise BenchmarkError(
            'publication requires disjoint physical load/management cores; overlap: '
            + repr(sorted(physical_overlap))
        )
    selected_load_cpus = set(load['cpus'])
    incomplete_siblings = {
        entry['cpu']: sorted(set(entry['thread_siblings']) - selected_load_cpus)
        for entry in load['topology']
        if not set(entry['thread_siblings']) <= selected_load_cpus
    }
    if incomplete_siblings:
        raise BenchmarkError(
            'publication load CPUs must contain complete SMT sibling sets: '
            + repr(incomplete_siblings)
        )

    def llc_domains(
        cpu_set: CpuSetState,
    ) -> set[tuple[int, int, tuple[int, ...]]]:
        return {
            (
                entry['physical_package_id'],
                entry['last_level_cache']['cache_id'],
                tuple(entry['last_level_cache']['shared_cpus']),
            )
            for entry in cpu_set['topology']
        }

    if len(llc_domains(load)) != 1:
        raise BenchmarkError(
            'publication requires the load generator to occupy one '
            'last-level-cache domain'
        )


def _scenario_record(
    scenario: Scenario,
    *,
    status: RunStatus,
    excluded_servers: dict[str, str],
    runs: list[dict[str, Any]],
    aggregate: dict[str, AggregateMetrics],
    headroom: HeadroomLadder | dict[str, str] | None,
    trials_run: int = 0,
    stopping_reason: str | None = None,
    error: str | None = None,
) -> ScenarioRecord:
    return {
        'status': status,
        'scenario': asdict(scenario),
        'excluded_servers': excluded_servers,
        'runs': runs,
        'aggregate': aggregate,
        'trials_run': trials_run,
        'stopping_reason': stopping_reason,
        'load_generator_headroom': headroom,
        'error': error,
    }


def _manifest_record(
    identity: dict[str, Any],
    scenario_records: dict[str, ScenarioRecord],
    publication_failures: list[str],
    headroom_ladders: dict[str, HeadroomLadder | dict[str, str]],
    *,
    status: RunStatus,
    error: str | None = None,
) -> ManifestRecord:
    return {
        'status': status,
        'run_identity': identity,
        'scenarios': {
            name: record['status'] for name, record in scenario_records.items()
        },
        'publication_failures': publication_failures,
        'load_generator_headroom': headroom_ladders,
        'error': error,
    }


def run_benchmarks(
    selected_servers: set[str] | None = None,
    selected_types: set[str] | None = None,
    *,
    harness: HarnessConfig | None = None,
    publish: bool = False,
) -> Path:
    harness = harness or HarnessConfig()
    _duration_seconds(harness.duration)
    if harness.load_worker_threads < 1:
        raise BenchmarkError('load-generator worker threads must be positive')
    if not 0.0 < harness.load_generator_grace_seconds <= 300.0:
        raise BenchmarkError('load-generator grace period must be in (0, 300] seconds')
    _ensure_static_file_response_payload()
    system_state = capture_system_state(
        harness.server_cpus, harness.load_cpus, harness.management_cpus
    )
    if publish:
        if selected_servers or selected_types:
            raise BenchmarkError(
                'publication requires the complete server and scenario suite'
            )
        _validate_publication_harness(harness, system_state)
    scenarios = [
        scenario
        for scenario in benchmark_scenarios()
        if not selected_types or scenario.type in selected_types
    ]
    if publish and len(scenarios) != len(benchmark_scenarios()):
        raise BenchmarkError(
            'publication requires the complete scenario matrix of '
            f'{len(benchmark_scenarios())}; selected {len(scenarios)}'
        )
    servers = [
        server
        for server in SERVERS
        if not selected_servers or server in selected_servers
    ]
    if not scenarios or not servers:
        raise BenchmarkError('benchmark selection is empty')

    run_id = (
        datetime.now(UTC).strftime('%Y%m%dT%H%M%S.%fZ') + f'-{secrets.token_hex(4)}'
    )
    run_directory = RUNS_DIRECTORY / run_id
    (run_directory / 'raw').mkdir(parents=True)
    (run_directory / 'plots').mkdir()
    (run_directory / 'load').mkdir()

    server_commands = {
        f'{server}:w{scenario.workers}:{scenario.type}': get_server_command(
            server,
            scenario.workers,
            scenario.type,
            UNIX_SOCKET_PATH if scenario.type == 'h1_uds' else None,
            server_cpus=harness.server_cpus,
        )
        for scenario in scenarios
        for server in _eligible_servers(scenario, servers)
    }
    versions = get_versions()
    identity = {
        'run_id': run_id,
        'harness': asdict(harness),
        'scenarios': [asdict(scenario) for scenario in scenarios],
        'servers': servers,
        'server_profiles': {server: SERVER_PROFILES[server] for server in servers},
        'commands': server_commands,
        'git_head': _git_head(),
        'versions': versions,
        'system': system_state,
    }
    write_json(run_directory / 'identity.json', identity)

    scenario_records: dict[str, ScenarioRecord] = {}
    headroom_ladders: dict[str, HeadroomLadder | dict[str, str]] = {}
    publication_failures: list[str] = []
    deadline = time.monotonic() + harness.time_budget_seconds
    scenario_minimums = [
        _scenario_minimum_seconds(
            entry, harness, len(_eligible_servers(entry, servers))
        )
        for entry in scenarios
    ]
    fastest_cell: tuple[float, Scenario, str] | None = None
    for scenario_index, scenario in enumerate(scenarios):
        print(f'\n=== Benchmarking {scenario.name} ===')
        eligible = _eligible_servers(scenario, servers)
        try:
            orders = balanced_orders(eligible, harness.max_trials, harness.order_seed)
        except ValueError as error:
            raise BenchmarkError(f'{scenario.name}: {error}') from error
        scenario_deadline, cell_duration = _scenario_budget(
            scenario,
            harness,
            eligible,
            deadline,
            scenario_minimums[scenario_index:],
        )
        cell_harness = replace(harness, duration=cell_duration)
        samples: dict[str, list[Metrics]] = {server: [] for server in eligible}
        raw_runs: list[dict[str, Any]] = []
        excluded_servers: dict[str, str] = {}
        clean_name = _clean_scenario_name(scenario.name)
        raw_path = run_directory / 'raw' / f'benchmark_{clean_name}.json'
        trials_run = 0
        stopping_reason = 'max-trials'
        for trial, order in enumerate(orders, start=1):
            if trial > harness.min_trials:
                if _scenario_stable(samples, excluded_servers, harness):
                    stopping_reason = 'stable'
                    break
                if time.monotonic() >= scenario_deadline:
                    stopping_reason = 'time-budget'
                    break
            print(
                f'--- trial {trial}/{harness.max_trials} '
                f'({cell_duration} load): {" -> ".join(order)} ---'
            )
            for server in order:
                if server in excluded_servers:
                    continue
                if trial > harness.min_trials and _server_stable(
                    samples[server], harness
                ):
                    # Converged; extension rounds only revisit the cells
                    # still above the spread bar.
                    continue
                socket_path = UNIX_SOCKET_PATH if scenario.type == 'h1_uds' else None
                command = get_server_command(
                    server,
                    scenario.workers,
                    scenario.type,
                    socket_path,
                    server_cpus=harness.server_cpus,
                )
                server_run: ServerProcessRecord | None = None
                server_resources: ProcessGroupUsage | None = None
                warmup_result: LoadResult | None = None
                warmup_metrics: Metrics | None = None
                load_result: LoadResult | None = None
                preflight: dict[str, Any] | None = None
                postflight: dict[str, Any] | None = None
                post_worker_pids: list[int] | None = None
                try:
                    with running_server(
                        server,
                        scenario.workers,
                        scenario.type,
                        socket_path,
                        server_cpus=harness.server_cpus,
                    ) as server_run:
                        time.sleep(harness.settle_seconds)
                        preflight = validate_response_contract(
                            scenario.type, socket_path
                        )
                        print(f'Warming request path for {server}...')
                        warmup_result = _run_scenario_warmup(
                            scenario,
                            cell_harness,
                            socket_path,
                            run_directory
                            / 'load'
                            / (
                                f'{_clean_scenario_name(scenario.name)}-'
                                f'{trial}-{server}-warmup.json'
                            ),
                        )
                        warmup_metrics = _validate_scenario_load(
                            scenario,
                            replace(harness, duration=harness.warmup_duration),
                            warmup_result,
                        )
                        print(f'Running load generation for {server}...')
                        resource_sampler = ProcessGroupResourceSampler(
                            server_run['process_group_id']
                        )
                        resource_sampler.start()
                        try:
                            load_result = _run_scenario_load(
                                scenario,
                                cell_harness,
                                socket_path,
                                run_directory
                                / 'load'
                                / f'{_clean_scenario_name(scenario.name)}-{trial}-{server}.json',
                            )
                        finally:
                            server_resources = resource_sampler.stop()
                        metrics = _validate_scenario_load(
                            scenario,
                            cell_harness,
                            load_result,
                        )
                        postflight = validate_response_contract(
                            scenario.type, socket_path
                        )
                        all_workers, observed_worker_pids = wait_for_worker_pids(
                            scenario.workers, socket_path
                        )
                        post_worker_pids = sorted(observed_worker_pids)
                        if (
                            not all_workers
                            or post_worker_pids != server_run['worker_pids']
                        ):
                            raise BenchmarkError(
                                f'{server} worker set changed across load: '
                                f'before={server_run["worker_pids"]}, '
                                f'after={post_worker_pids}'
                            )
                    samples[server].append(metrics)
                    if not load_result.usage['sufficient_headroom']:
                        # Recorded evidence, not a publication blocker: the
                        # fastest-cell plateau check is the hard generator
                        # gate. A saturated generator can only understate
                        # the measured server.
                        print(
                            f'warning: {scenario.name}/{server}/trial '
                            f'{trial}: load generator used '
                            f'{load_result.usage["physical_core_utilization"]:.1%} '
                            'of physical-core CPU capacity'
                        )
                    raw_runs.append({
                        'trial': trial,
                        'order': order,
                        'server': server,
                        'server_command': command,
                        'warmup': _scenario_load_evidence(
                            warmup_result,
                            warmup_metrics,
                        ),
                        'load_command': load_result.command,
                        'load_generator_usage': load_result.usage,
                        'correctness': {
                            'before': preflight,
                            'after': postflight,
                            'worker_pids_before': server_run['worker_pids'],
                            'worker_pids_after': post_worker_pids,
                        },
                        'server_process': server_run,
                        'server_resource_usage': server_resources,
                        'raw': load_result.raw,
                        'metrics': metrics,
                    })
                    print(
                        format_server_summary(
                            server,
                            metrics,
                            unit='sessions/s' if scenario.type == 'ws' else 'RPS',
                        )
                    )
                except BenchmarkError as error:
                    traceback.print_exception(error)
                    raw_runs.append({
                        'trial': trial,
                        'order': order,
                        'server': server,
                        'server_command': command,
                        'warmup': _scenario_load_evidence(
                            warmup_result,
                            warmup_metrics,
                        ),
                        'load_command': load_result.command if load_result else None,
                        'load_generator_usage': load_result.usage
                        if load_result
                        else None,
                        'correctness': {
                            'before': preflight,
                            'after': postflight,
                            'worker_pids_before': server_run['worker_pids']
                            if server_run
                            else None,
                            'worker_pids_after': post_worker_pids,
                        },
                        'server_process': server_run,
                        'server_resource_usage': server_resources,
                        'raw': load_result.raw if load_result else None,
                        'error': str(error),
                    })
                    excluded_servers[server] = str(error)
                    print(f'Excluding {server} from {scenario.name}: {error}')
            trials_run = trial

        results: dict[str, AggregateMetrics] = {
            server: aggregate_metrics(values)
            for server, values in samples.items()
            if server not in excluded_servers and len(values) >= harness.min_trials
        }
        status = (
            'complete'
            if not excluded_servers
            and len(results) == len(eligible)
            and trials_run >= harness.min_trials
            else 'failed'
        )
        print(f'{scenario.name}: {trials_run} trials ({stopping_reason})')
        if results:
            scenario_fastest = max(results, key=lambda name: results[name]['rps'])
            fastest_rps = results[scenario_fastest]['rps']
            if fastest_cell is None or fastest_rps > fastest_cell[0]:
                fastest_cell = (fastest_rps, scenario, scenario_fastest)
        scenario_records[clean_name] = _scenario_record(
            scenario,
            status=status,
            excluded_servers=excluded_servers,
            runs=raw_runs,
            aggregate=results,
            headroom=None,
            trials_run=trials_run,
            stopping_reason=stopping_reason,
        )
        write_json(raw_path, scenario_records[clean_name])
        if results:
            concurrency = scenario.concurrency or harness.concurrency
            load_shape = f'{concurrency} conn'
            if scenario.http2_parallelism > 1:
                load_shape += f' x {scenario.http2_parallelism} streams'
            plot_results(
                results,
                f'Benchmark: {scenario.name} '
                f'({cell_duration} sustained x {trials_run} trials, {load_shape})',
                run_directory / 'plots' / f'benchmark_{clean_name}.svg',
                system_summary=system_state['summary'],
                websocket=scenario.type == 'ws',
            )

    if publish and fastest_cell is not None:
        _rps, headroom_scenario, headroom_server = fastest_cell
        headroom_name = _clean_scenario_name(headroom_scenario.name)
        if time.monotonic() >= deadline:
            headroom_ladders[headroom_name] = {
                'skipped': 'time budget exhausted before the headroom check'
            }
        else:
            print(
                f'\n=== Load-generator headroom check: '
                f'{headroom_scenario.name} / {headroom_server} ==='
            )
            try:
                ladder = _measure_load_headroom(
                    headroom_scenario,
                    headroom_server,
                    harness,
                    run_directory,
                )
                headroom_ladders[headroom_name] = ladder
                if not ladder['plateau_observed']:
                    publication_failures.append(
                        f'{headroom_scenario.name}: the fastest cell gained '
                        f'{ladder["full_vs_reduced_gain"]:.1%} from extra '
                        f'load-generator workers (margin '
                        f'{ladder["maximum_publish_gain"]:.0%}) — the '
                        f'generator, not the server, may be the bottleneck'
                    )
            except BenchmarkError as error:
                headroom_ladders[headroom_name] = {'error': str(error)}
                publication_failures.append(
                    f'{headroom_scenario.name}: headroom check failed: {error}'
                )
        record = dict(scenario_records[headroom_name])
        record['load_generator_headroom'] = headroom_ladders[headroom_name]
        scenario_records[headroom_name] = record  # type: ignore[assignment]
        write_json(run_directory / 'raw' / f'benchmark_{headroom_name}.json', record)

    failed_scenarios = [
        name
        for name, record in scenario_records.items()
        if record['status'] != 'complete'
    ]
    manifest_status: RunStatus = (
        'complete' if not failed_scenarios and not publication_failures else 'failed'
    )
    manifest = _manifest_record(
        identity,
        scenario_records,
        publication_failures,
        headroom_ladders,
        status=manifest_status,
    )
    write_json(run_directory / 'manifest.json', manifest)
    if publish:
        if failed_scenarios:
            raise BenchmarkError(
                'refusing publication because scenarios failed: '
                + ', '.join(failed_scenarios)
            )
        if publication_failures:
            raise BenchmarkError(
                'refusing publication because load-generator headroom was insufficient: '
                + '; '.join(publication_failures)
            )
        _publish_artifacts(run_directory, list(scenario_records), manifest)
        print(f'Published canonical benchmark artifacts from {run_directory}')
    else:
        print(
            f'Staged benchmark artifacts in {run_directory}; canonical plots unchanged'
        )
    return run_directory


def main():
    parser = argparse.ArgumentParser(
        description='h2corn benchmark suite',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        '--servers',
        nargs='+',
        choices=list(SERVERS.keys()),
        help='List of servers to benchmark',
    )
    parser.add_argument(
        '--time-budget',
        default=TIME_BUDGET,
        help='hard wall-clock cap for the whole suite (e.g. 15m)',
    )
    parser.add_argument('--settle-seconds', type=float, default=SETTLE_SECONDS)
    parser.add_argument(
        '--qps',
        type=int,
        help='fixed offered request rate for equal-load latency measurements',
    )
    parser.add_argument(
        '--types',
        nargs='+',
        choices=list(RESPONSE_CONTRACTS),
        help='List of benchmark types to run',
    )
    parser.add_argument(
        '--duration',
        default=DURATION,
        help='Sustained load duration per benchmark cell (e.g. 10s)',
    )
    parser.add_argument(
        '--warmup-duration',
        default=WARMUP_DURATION,
        help='Unmeasured request-path load before each measured server process',
    )
    parser.add_argument(
        '--concurrency',
        type=int,
        default=CONCURRENCY,
        help='Number of concurrent connections',
    )
    parser.add_argument('--server-cpus', help='CPU list for server processes')
    parser.add_argument('--load-cpus', help='CPU list for load generators')
    parser.add_argument(
        '--management-cpus',
        help='CPU list for the benchmark orchestrator and resource sampler',
    )
    parser.add_argument(
        '--load-worker-threads',
        type=int,
        help='fixed oha Tokio workers / k6 GOMAXPROCS (default: one per load CPU)',
    )
    parser.add_argument(
        '--max-load-utilization',
        type=float,
        default=MAX_LOAD_UTILIZATION,
        help='maximum aggregate load-generator CPU utilization for publication',
    )
    parser.add_argument(
        '--publish',
        action='store_true',
        help='replace canonical raw records and plots after a fully staged clean run',
    )
    args = parser.parse_args()

    if args.settle_seconds < 0:
        parser.error('--settle-seconds cannot be negative')
    if args.qps is not None and args.qps < 1:
        parser.error('--qps must be positive')
    if args.concurrency < 1:
        parser.error('--concurrency must be positive')
    if args.load_worker_threads is not None and args.load_worker_threads < 1:
        parser.error('--load-worker-threads must be positive')
    try:
        _duration_seconds(args.duration)
        _duration_seconds(args.warmup_duration)
        time_budget_seconds = _duration_seconds(args.time_budget)
    except BenchmarkError as error:
        parser.error(str(error))
    if not 0 < args.max_load_utilization < 1:
        parser.error('--max-load-utilization must be between 0 and 1')
    try:
        server_cpus = (
            parse_linux_cpu_list(args.server_cpus) if args.server_cpus else None
        )
        load_cpus = parse_linux_cpu_list(args.load_cpus) if args.load_cpus else None
        management_cpus = (
            parse_linux_cpu_list(args.management_cpus) if args.management_cpus else None
        )
    except ValueError as error:
        parser.error(str(error))
    if not (load_cpus or management_cpus):
        try:
            roles = derive_cpu_roles()
        except RuntimeError as error:
            if args.publish:
                parser.error(str(error))
            roles = None
        if roles is not None:
            load_cpus = roles['load']
            management_cpus = roles['management']
            print(
                'Auto-derived instrument CPU roles (servers stay unpinned): '
                f'load={",".join(map(str, load_cpus))} '
                f'management={",".join(map(str, management_cpus))}'
            )
    if (server_cpus or load_cpus or management_cpus) and not shutil.which('taskset'):
        parser.error('CPU affinity requested, but taskset is not installed')
    try:
        pin_benchmark_driver(management_cpus)
    except BenchmarkError as error:
        parser.error(str(error))

    selected_servers = set(args.servers) if args.servers else None
    selected_types = set(args.types) if args.types else None
    if args.publish and (selected_servers or selected_types):
        parser.error('--publish requires the complete server and scenario suite')

    needed_types = selected_types or {
        scenario.type for scenario in benchmark_scenarios()
    }
    needed_tools = {
        response_contract(config_type).driver for config_type in needed_types
    }
    missing_tools = sorted(tool for tool in needed_tools if not shutil.which(tool))
    if missing_tools:
        parser.error(
            'required load generators are missing: ' + ', '.join(missing_tools)
        )

    load_worker_threads = args.load_worker_threads or (
        len(load_cpus) if load_cpus else min(16, os.cpu_count() or 4)
    )
    harness = HarnessConfig(
        duration=args.duration,
        warmup_duration=args.warmup_duration,
        concurrency=args.concurrency,
        time_budget_seconds=time_budget_seconds,
        settle_seconds=args.settle_seconds,
        rate_limit_qps=args.qps,
        server_cpus=server_cpus,
        load_cpus=load_cpus,
        management_cpus=management_cpus,
        load_worker_threads=load_worker_threads,
        max_load_utilization=args.max_load_utilization,
    )
    run_benchmarks(
        selected_servers,
        selected_types,
        harness=harness,
        publish=args.publish,
    )


if __name__ == '__main__':
    main()
