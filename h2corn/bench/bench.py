import argparse
import base64
import ctypes
import fcntl
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
from contextlib import contextmanager, suppress
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
    from bench.provenance import (
        file_sha256,
        path_fingerprint,
        tree_sha256,
    )
    from bench.system import (
        MAX_AMBIENT_CPU_UTILIZATION,
        MAX_AMBIENT_SINGLE_CPU_UTILIZATION,
        AmbientCpuError,
        AmbientCpuProbe,
        BenchmarkError,
        BenchmarkSystemState,
        CpuActivity,
        CpuActivityMonitor,
        CpuSetState,
        PollableProcess,
        ProcessGroupResourceSampler,
        ProcessGroupUsage,
        benchmark_system_state_matches,
        capture_role_ambient_cpu,
        capture_system_state,
        durable_json,
        fsync_directory,
        parse_linux_cpu_list,
        physical_core_capacity,
        pin_benchmark_driver,
        terminate_process_group,
        validate_ambient_cpu,
        validate_interference_cpu,
        validate_k6_result,
        validate_oha_result,
        wait_for_http_server,
        wait_for_unix_server,
    )
except ModuleNotFoundError:  # Direct ``python bench/bench.py`` execution.
    from provenance import (  # type: ignore[import-not-found, no-redef]
        file_sha256,
        path_fingerprint,
        tree_sha256,
    )
    from system import (  # type: ignore[import-not-found, no-redef]
        MAX_AMBIENT_CPU_UTILIZATION,
        MAX_AMBIENT_SINGLE_CPU_UTILIZATION,
        AmbientCpuError,
        AmbientCpuProbe,
        BenchmarkError,
        BenchmarkSystemState,
        CpuActivity,
        CpuActivityMonitor,
        CpuSetState,
        PollableProcess,
        ProcessGroupResourceSampler,
        ProcessGroupUsage,
        benchmark_system_state_matches,
        capture_role_ambient_cpu,
        capture_system_state,
        durable_json,
        fsync_directory,
        parse_linux_cpu_list,
        physical_core_capacity,
        pin_benchmark_driver,
        terminate_process_group,
        validate_ambient_cpu,
        validate_interference_cpu,
        validate_k6_result,
        validate_oha_result,
        wait_for_http_server,
        wait_for_unix_server,
    )

DURATION = '10s'
WARMUP_DURATION = '1s'
CONCURRENCY = 100
STREAMING_CONCURRENCY = 1000
TRIALS = 8
SETTLE_SECONDS = 1.0
ORDER_SEED = 20_260_712
RATE_LIMIT = None
SERVER_CPUS = None
LOAD_CPUS = None
LOAD_WORKER_THREADS = 16
MANAGEMENT_CPUS = None
MAX_LOAD_UTILIZATION = 0.85
AMBIENT_CPU_PROBE_SECONDS = 1.0
MAX_LOAD_SCALING_GAIN = 0.02
HEADROOM_BLOCKS = 7
LOAD_GENERATOR_GRACE_SECONDS = 15.0
SERVER_READY_TIMEOUT_SECONDS = 5.0
HOST = '127.0.0.1'
PORT = 8000
UNIX_SOCKET_PATH = Path('bench/results/benchmark.sock')
RUNS_DIRECTORY = Path('bench/results/runs')
CANONICAL_RAW_DIRECTORY = Path('bench/results/raw')
CANONICAL_PLOT_DIRECTORY = Path('bench/results/plots')
PUBLICATION_TRANSACTION_PREFIX = '.benchmark-publication-'
PUBLICATION_GENERATION_NAME = '.benchmark-generation.json'
PUBLICATION_SCHEMA_VERSION = 1
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
RunStatus: TypeAlias = Literal['running', 'complete', 'failed']


class ArtifactIdentity(TypedDict):
    path: str
    sha256: str | None


class CommandEnvironment(TypedDict, total=False):
    TOKIO_WORKER_THREADS: str
    GOMAXPROCS: str


class CommandIdentity(TypedDict):
    argv: list[str]
    executable: str | None
    executable_sha256: str | None
    environment: CommandEnvironment


class GitIdentity(TypedDict):
    git_head: str | None
    git_diff_sha256: str | None
    git_status_sha256: str | None
    git_untracked_sha256: str | None


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


class LoadCommandContract(TypedDict):
    driver: Literal['oha', 'k6']
    url: str
    protocol: Literal['1.1', '2', 'websocket']
    method: Literal['GET', 'POST']
    body_bytes: int
    content_type: str | None
    concurrency: int
    http2_parallelism: int
    duration: str
    rate_limit_qps: int | None
    load_cpus: tuple[int, ...] | None
    worker_threads: int
    environment: CommandEnvironment


class HeadroomRun(TypedDict):
    phase: HeadroomPhase
    variant: HeadroomVariant
    ambient_cpu_before: AmbientCpuProbe
    interference_cpu_during: CpuActivity
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
    server_start_ambient_cpu: AmbientCpuProbe
    warmup_order: list[HeadroomVariant]
    order: list[HeadroomVariant]
    load_cpus: list[int]
    reduced_worker_threads: int
    full_worker_threads: int
    reduced_rps_samples: list[float]
    full_rps_samples: list[float]
    paired_gain_samples: list[float]
    full_vs_reduced_gain: float
    paired_gain_iqr: float
    paired_gain_lower_quartile: float
    paired_gain_upper_quartile: float
    maximum_publish_gain: float
    full_win_count: int
    paired_comparisons: int
    sign_test_one_sided_p: float
    equivalence_below_margin_count: int
    equivalence_comparisons: int
    equivalence_sign_test_one_sided_p: float
    sign_test_alpha: float
    all_runs_have_cpu_headroom: bool
    plateau_observed: bool
    runs: list[HeadroomRun]
    correctness: dict[str, Any]


class PendingAttempt(TypedDict):
    """Durable evidence for a measured cell that has started but not finished."""

    recorded_at: str
    trial: int
    order: list[str]
    server: str
    ambient_cpu_before: AmbientCpuProbe | None
    server_command: CommandIdentity
    warmup: ScenarioLoadEvidence


class ScenarioRecord(TypedDict):
    schema_version: int
    status: RunStatus
    run_identity_sha256: str
    scenario: dict[str, Any]
    excluded_servers: dict[str, str]
    provenance: dict[str, Any]
    runs: list[dict[str, Any]]
    aggregate: dict[str, AggregateMetrics]
    load_generator_headroom: HeadroomLadder | dict[str, str] | None
    pending_attempt: PendingAttempt | None
    error: str | None


class ManifestRecord(TypedDict):
    schema_version: int
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
    trials: int = TRIALS
    settle_seconds: float = SETTLE_SECONDS
    order_seed: int = ORDER_SEED
    rate_limit_qps: int | None = RATE_LIMIT
    server_cpus: tuple[int, ...] | None = SERVER_CPUS
    load_cpus: tuple[int, ...] | None = LOAD_CPUS
    management_cpus: tuple[int, ...] | None = MANAGEMENT_CPUS
    load_worker_threads: int = LOAD_WORKER_THREADS
    max_load_utilization: float = MAX_LOAD_UTILIZATION
    ambient_cpu_probe_seconds: float = AMBIENT_CPU_PROBE_SECONDS
    max_ambient_cpu_utilization: float = MAX_AMBIENT_CPU_UTILIZATION
    max_ambient_single_cpu_utilization: float = MAX_AMBIENT_SINGLE_CPU_UTILIZATION
    max_load_scaling_gain: float = MAX_LOAD_SCALING_GAIN
    load_generator_grace_seconds: float = LOAD_GENERATOR_GRACE_SECONDS


@dataclass(frozen=True, slots=True)
class ResponseContract:
    """Single per-benchmark-type descriptor.

    Owns the request shape (path/method/body), the load driver, the wire
    protocol, and the exact expected response. The recorded
    LoadCommandContract, the oha/k6 invocation, the correctness probe, and
    server eligibility all derive from this one source, so the published
    provenance can never de-sync from the executed load command.
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


class PublicationArtifact(TypedDict):
    path: str
    sha256: str


class PublicationGeneration(TypedDict):
    schema_version: int
    token: str
    artifacts: list[PublicationArtifact]


class PublicationManifest(TypedDict):
    status: RunStatus


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
    """Return a seeded Latin rotation with exact lead/position balance.

    An even trial count makes each server run equally often in the forward and
    reverse direction. Requiring complete rotations prevents a partial suite
    from giving one server systematically warmer or cooler positions.
    """
    if not names:
        raise ValueError('at least one benchmark server is required')
    if trials < 2 or trials % 2:
        raise ValueError('trials must be a positive even number')
    cycle_length = 2 * len(names)
    if trials % cycle_length:
        raise ValueError('trials must be divisible by twice the eligible server count')
    base = list(names)
    random.Random(seed).shuffle(base)
    forward = [base[index:] + base[:index] for index in range(len(base))]
    reverse = [list(reversed(order)) for order in forward]
    cycle = [order for pair in zip(forward, reverse, strict=True) for order in pair]
    return cycle * (trials // cycle_length)


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


def _artifact_identity(path: str | Path | None) -> ArtifactIdentity:
    if path is None:
        return {'path': '', 'sha256': None}
    resolved = Path(path).resolve()
    return {'path': str(resolved), 'sha256': tree_sha256(resolved)}


def _resolved_executable(command: list[str]) -> str | None:
    executable_arg = (
        command[3] if command[:2] == ['taskset', '--cpu-list'] else command[0]
    )
    executable = shutil.which(executable_arg)
    if executable is None and Path(executable_arg).is_file():
        executable = str(Path(executable_arg).resolve())
    return executable


def command_provenance(
    command: list[str],
    environment: CommandEnvironment | None = None,
) -> CommandIdentity:
    executable = _resolved_executable(command)
    return {
        'argv': command,
        'executable': executable,
        'executable_sha256': file_sha256(Path(executable)) if executable else None,
        'environment': {} if environment is None else environment.copy(),
    }


def _module_artifact(module_name: str) -> ArtifactIdentity:
    try:
        spec = importlib.util.find_spec(module_name)
    except (ImportError, ValueError):
        spec = None
    if spec is None:
        return _artifact_identity(None)
    if spec.submodule_search_locations:
        return _artifact_identity(next(iter(spec.submodule_search_locations), None))
    return _artifact_identity(spec.origin)


def artifact_snapshot(
    server_commands: dict[str, list[str]], load_tools: set[str]
) -> dict[str, ArtifactIdentity]:
    artifacts = {
        'benchmark_harness': _artifact_identity(Path(__file__)),
        'benchmark_application': _artifact_identity('bench/bench_app.py'),
        'static_file_payload': _artifact_identity(STATIC_FILE_RESPONSE_PATH),
        'h2corn_package': _module_artifact('h2corn'),
        'h2corn_native_extension': _module_artifact('h2corn._lib'),
    }
    artifacts.update({
        f'python_module:{module}': _module_artifact(module)
        for module in BENCHMARK_PYTHON_MODULES
    })
    if 'k6' in load_tools:
        artifacts['websocket_load_script'] = _artifact_identity('bench/k6/ws.js')
    for server, command in server_commands.items():
        artifacts[f'server_executable:{server}'] = _artifact_identity(
            _resolved_executable(command)
        )
    for tool in sorted(load_tools):
        artifacts[f'load_generator:{tool}'] = _artifact_identity(shutil.which(tool))
    return artifacts


def _git_identity() -> GitIdentity:
    root = subprocess.run(
        ['git', 'rev-parse', '--show-toplevel'],
        capture_output=True,
        text=True,
        check=False,
    )
    if root.returncode != 0:
        return {
            'git_head': None,
            'git_diff_sha256': None,
            'git_status_sha256': None,
            'git_untracked_sha256': None,
        }
    source_root = Path.cwd().resolve()
    source_pathspec = ['--', '.', ':(exclude)bench/results/**']
    head = subprocess.run(
        ['git', 'rev-parse', 'HEAD'],
        capture_output=True,
        text=True,
        check=False,
    )
    diff = subprocess.run(
        [
            'git',
            'diff',
            '--binary',
            'HEAD',
            *source_pathspec,
        ],
        capture_output=True,
        check=False,
    )
    status = subprocess.run(
        [
            'git',
            'status',
            '--porcelain=v1',
            '-z',
            '--untracked-files=all',
            *source_pathspec,
        ],
        capture_output=True,
        check=False,
    )
    untracked = subprocess.run(
        [
            'git',
            'ls-files',
            '--others',
            '--exclude-standard',
            '-z',
            *source_pathspec,
        ],
        capture_output=True,
        check=False,
    )
    untracked_sha256 = None
    if untracked.returncode == 0:
        digest = hashlib.sha256()
        for encoded_path in sorted(filter(None, untracked.stdout.split(b'\0'))):
            path = source_root / os.fsdecode(encoded_path)
            digest.update(encoded_path)
            digest.update(b'\0')
            try:
                if path.is_symlink():
                    digest.update(b'symlink\0')
                    digest.update(os.fsencode(os.readlink(path)))
                else:
                    digest.update(b'file\0')
                    digest.update(path.read_bytes())
            except OSError as error:
                # A concurrently removed or unreadable source must still alter
                # the identity deterministically instead of aborting cleanup.
                digest.update(b'unreadable\0')
                digest.update(str(error.errno).encode())
            digest.update(b'\0')
        untracked_sha256 = digest.hexdigest()
    return {
        'git_head': head.stdout.strip() if head.returncode == 0 else None,
        'git_diff_sha256': hashlib.sha256(diff.stdout).hexdigest()
        if diff.returncode == 0
        else None,
        'git_status_sha256': hashlib.sha256(status.stdout).hexdigest()
        if status.returncode == 0
        else None,
        'git_untracked_sha256': untracked_sha256,
    }


def benchmark_provenance(
    harness: HarnessConfig,
    artifacts: dict[str, ArtifactIdentity],
    git_identity: GitIdentity,
    versions: dict[str, str | None],
    system: BenchmarkSystemState,
) -> dict[str, Any]:
    return {
        'recorded_at': datetime.now(UTC).isoformat(),
        **git_identity,
        'versions': versions,
        'server_profiles': SERVER_PROFILES,
        'system': system,
        'runtime_environment': _runtime_environment(),
        'harness': asdict(harness),
        'artifacts': artifacts,
    }


def _runtime_environment() -> dict[str, str]:
    exact = {
        'ASAN_OPTIONS',
        'GOMAXPROCS',
        'K6_NO_USAGE_REPORT',
        'LD_LIBRARY_PATH',
        'MALLOC_CONF',
        'PATH',
        'PYTHONHASHSEED',
        'PYTHONPATH',
        'SSL_CERT_DIR',
        'SSL_CERT_FILE',
        'UBSAN_OPTIONS',
    }
    prefixes = ('H2CORN_', 'MALLOC_', 'PYTHON', 'RUST_', 'TOKIO_', 'UVLOOP_')
    return {
        name: value
        for name, value in sorted(os.environ.items())
        if name in exact or name.startswith(prefixes)
    }


def verify_artifact_snapshot(
    expected: dict[str, ArtifactIdentity],
    server_commands: dict[str, list[str]],
    load_tools: set[str],
) -> None:
    actual = artifact_snapshot(server_commands, load_tools)
    if actual != expected:
        changed = sorted(
            key for key in expected | actual if expected.get(key) != actual.get(key)
        )
        raise BenchmarkError(
            'benchmark artifacts changed during the run: ' + ', '.join(changed)
        )


def verify_run_snapshot(
    expected_artifacts: dict[str, ArtifactIdentity],
    expected_git: GitIdentity,
    expected_system: BenchmarkSystemState,
    harness: HarnessConfig,
    server_commands: dict[str, list[str]],
    load_tools: set[str],
) -> None:
    verify_artifact_snapshot(expected_artifacts, server_commands, load_tools)
    if _git_identity() != expected_git:
        raise BenchmarkError('source-tree identity changed during the benchmark run')
    current_system = capture_system_state(
        harness.server_cpus, harness.load_cpus, harness.management_cpus
    )
    if not benchmark_system_state_matches(expected_system, current_system):
        raise BenchmarkError(
            'CPU topology, policy, affinity, or benchmark system state changed '
            'during the run'
        )


def artifact_fingerprint(
    artifacts: dict[str, ArtifactIdentity],
) -> dict[str, object]:
    """Cheap (path, size, mtime_ns, inode) identity over the hashed file set."""
    return {
        key: path_fingerprint(Path(identity['path']))
        for key, identity in artifacts.items()
        if identity['path']
    }


class RunSnapshotGate:
    """Fingerprint-gated integrity check between measured cells.

    The full artifact tree hash, git identity, and system state are frozen
    once at run start. Each cell re-checks only a stat fingerprint of the
    same file set; the expensive full verification (re-hash, git subprocesses,
    sysfs capture) runs only when the fingerprint changes — failing exactly as
    the full check would — and once at suite completion.
    """

    def __init__(
        self,
        artifacts: dict[str, ArtifactIdentity],
        git_identity: GitIdentity,
        system: BenchmarkSystemState,
        harness: HarnessConfig,
        server_commands: dict[str, list[str]],
        load_tools: set[str],
    ) -> None:
        self._artifacts = artifacts
        self._git_identity = git_identity
        self._system = system
        self._harness = harness
        self._server_commands = server_commands
        self._load_tools = load_tools
        self._fingerprint = artifact_fingerprint(artifacts)

    def verify_cell(self) -> None:
        current = artifact_fingerprint(self._artifacts)
        if current == self._fingerprint:
            return
        self.verify_full()
        self._fingerprint = current

    def verify_full(self) -> None:
        verify_run_snapshot(
            self._artifacts,
            self._git_identity,
            self._system,
            self._harness,
            self._server_commands,
            self._load_tools,
        )


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


def _capture_ambient_cpu(
    harness: HarnessConfig, system: BenchmarkSystemState
) -> AmbientCpuProbe:
    return capture_role_ambient_cpu(system, harness.ambient_cpu_probe_seconds)


def _validate_ambient_cpu(probe: AmbientCpuProbe, harness: HarnessConfig) -> None:
    validate_ambient_cpu(
        probe,
        max_aggregate=harness.max_ambient_cpu_utilization,
        max_single=harness.max_ambient_single_cpu_utilization,
    )


def _interference_monitor(
    harness: HarnessConfig, system: BenchmarkSystemState
) -> CpuActivityMonitor:
    server = system['server']
    if server is None or harness.server_cpus is None or harness.management_cpus is None:
        raise AmbientCpuError('interference monitor requires pinned CPU roles')
    server_llc_cpus = {
        cpu
        for entry in server['topology']
        for cpu in entry['last_level_cache']['shared_cpus']
    }
    interference_cpus = tuple(
        sorted(
            server_llc_cpus - set(harness.server_cpus) - set(harness.management_cpus)
        )
    )
    return CpuActivityMonitor(
        interference_cpus,
        physical_core_capacity(interference_cpus),
    )


def _validate_interference_cpu(activity: CpuActivity, harness: HarnessConfig) -> None:
    validate_interference_cpu(
        activity,
        max_aggregate=harness.max_ambient_cpu_utilization,
        max_single=harness.max_ambient_single_cpu_utilization,
    )


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
        command=command_provenance(cmd, environment),
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
        command=command_provenance(cmd, environment),
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
HEADROOM_FAMILY_SIZE = len(benchmark_scenarios())
HEADROOM_SIGN_TEST_ALPHA = 0.05 / HEADROOM_FAMILY_SIZE


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


def _load_command_contract(
    scenario: Scenario, harness: HarnessConfig
) -> LoadCommandContract:
    contract = response_contract(scenario.type)
    return {
        'driver': contract.driver,
        'url': contract.load_url(),
        'protocol': contract.protocol,
        'method': contract.method,
        'body_bytes': len(contract.request_body),
        'content_type': contract.request_content_type,
        'concurrency': scenario.concurrency or harness.concurrency,
        'http2_parallelism': scenario.http2_parallelism,
        'duration': harness.duration,
        'rate_limit_qps': harness.rate_limit_qps,
        'load_cpus': harness.load_cpus,
        'worker_threads': harness.load_worker_threads,
        'environment': {'GOMAXPROCS': str(harness.load_worker_threads)}
        if contract.driver == 'k6'
        else {'TOKIO_WORKER_THREADS': str(harness.load_worker_threads)},
    }


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


def _one_sided_sign_test_p(gains: list[float]) -> tuple[int, int, float]:
    """Return wins, non-tied comparisons, and P(full is not better)."""
    non_tied = [gain for gain in gains if gain != 0.0]
    comparisons = len(non_tied)
    wins = sum(gain > 0.0 for gain in non_tied)
    if comparisons == 0:
        return wins, comparisons, 1.0
    probability = sum(
        math.comb(comparisons, count) for count in range(wins, comparisons + 1)
    ) / (2**comparisons)
    return wins, comparisons, probability


def _equivalence_sign_test_p(
    gains: list[float], maximum_gain: float
) -> tuple[int, int, float]:
    """Test whether the paired median is strictly below a practical margin."""
    return _one_sided_sign_test_p([maximum_gain - paired_gain for paired_gain in gains])


def _measure_load_headroom(
    scenario: Scenario,
    server: str,
    harness: HarnessConfig,
    run_directory: Path,
    snapshot_gate: RunSnapshotGate,
    expected_system: BenchmarkSystemState,
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
    ambient_attempts: list[AmbientCpuProbe] = []
    samples: dict[HeadroomVariant, list[float]] = {'reduced': [], 'full': []}
    progress_path = (
        run_directory
        / 'raw'
        / f'benchmark_{_clean_scenario_name(scenario.name)}_headroom_progress.json'
    )

    def checkpoint(
        status: Literal['running', 'complete', 'failed'],
        error: str | None = None,
    ) -> None:
        durable_json(
            progress_path,
            {
                'schema_version': 4,
                'status': status,
                'scenario': asdict(scenario),
                'server': server,
                'ambient_cpu_attempts': ambient_attempts,
                'runs': runs,
                'error': error,
            },
        )

    def validate_ambient(
        probe: AmbientCpuProbe, *, persist_success: bool = True
    ) -> None:
        ambient_attempts.append(probe)
        if persist_success:
            checkpoint('running')
        try:
            _validate_ambient_cpu(probe, harness)
        except AmbientCpuError as error:
            checkpoint('failed', str(error))
            raise

    @contextmanager
    def persist_failure() -> Iterator[None]:
        try:
            yield
        except BaseException as error:
            checkpoint('failed', _exception_summary(error))
            raise

    try:
        snapshot_gate.verify_cell()
    except BaseException as error:
        checkpoint('failed', _exception_summary(error))
        raise
    server_start_ambient = _capture_ambient_cpu(harness, expected_system)
    validate_ambient(server_start_ambient)
    with (
        persist_failure(),
        running_server(
            server,
            scenario.workers,
            scenario.type,
            socket_path,
            server_cpus=harness.server_cpus,
        ) as server_run,
    ):
        time.sleep(harness.settle_seconds)
        before = validate_response_contract(scenario.type, socket_path)
        for phase, phase_order in phases:
            for index, variant in enumerate(phase_order, start=1):
                ambient = _capture_ambient_cpu(harness, expected_system)
                validate_ambient(ambient)
                # The durable checkpoint above performs real filesystem work.
                # Gate the host once more after it has reached storage so the
                # measurement starts after, rather than during, that activity.
                ambient = _capture_ambient_cpu(harness, expected_system)
                validate_ambient(ambient, persist_success=False)
                interference_monitor = _interference_monitor(harness, expected_system)
                resource_sampler = ProcessGroupResourceSampler(
                    server_run['process_group_id']
                )
                interference_monitor.start()
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
                    interference = interference_monitor.stop()
                metrics = _validate_scenario_load(scenario, variants[variant], load)
                if phase == 'measured':
                    samples[variant].append(metrics['rps'])
                run: HeadroomRun = {
                    'phase': phase,
                    'variant': variant,
                    'ambient_cpu_before': ambient,
                    'interference_cpu_during': interference,
                    'command': load.command,
                    'load_generator_usage': load.usage,
                    'server_resource_usage': server_resources,
                    'raw': load.raw,
                    'metrics': metrics,
                }
                runs.append(run)
                checkpoint('running')
                try:
                    _validate_interference_cpu(interference, harness)
                except AmbientCpuError as error:
                    checkpoint('failed', str(error))
                    raise
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
    lower_quartile, _, upper_quartile = statistics.quantiles(
        paired_gains, n=4, method='inclusive'
    )
    full_wins, paired_comparisons, sign_test_p = _one_sided_sign_test_p(paired_gains)
    below_margin, equivalence_comparisons, equivalence_p = _equivalence_sign_test_p(
        paired_gains, harness.max_load_scaling_gain
    )
    all_runs_have_headroom = all(
        run['load_generator_usage']['sufficient_headroom'] for run in runs
    )
    result: HeadroomLadder = {
        'server': server,
        'server_start_ambient_cpu': server_start_ambient,
        'warmup_order': warmup_order,
        'order': order,
        'load_cpus': list(harness.load_cpus),
        'reduced_worker_threads': reduced_worker_threads,
        'full_worker_threads': harness.load_worker_threads,
        'reduced_rps_samples': samples['reduced'],
        'full_rps_samples': samples['full'],
        'paired_gain_samples': paired_gains,
        'full_vs_reduced_gain': gain,
        'paired_gain_iqr': upper_quartile - lower_quartile,
        'paired_gain_lower_quartile': lower_quartile,
        'paired_gain_upper_quartile': upper_quartile,
        'maximum_publish_gain': harness.max_load_scaling_gain,
        'full_win_count': full_wins,
        'paired_comparisons': paired_comparisons,
        'sign_test_one_sided_p': sign_test_p,
        'equivalence_below_margin_count': below_margin,
        'equivalence_comparisons': equivalence_comparisons,
        'equivalence_sign_test_one_sided_p': equivalence_p,
        'sign_test_alpha': HEADROOM_SIGN_TEST_ALPHA,
        'all_runs_have_cpu_headroom': all_runs_have_headroom,
        'plateau_observed': (
            equivalence_p < HEADROOM_SIGN_TEST_ALPHA and all_runs_have_headroom
        ),
        'runs': runs,
        'correctness': {
            'before': before,
            'after': after,
            'worker_pids_before': server_run['worker_pids'],
            'worker_pids_after': worker_pids_after,
        },
    }
    checkpoint('complete')
    return result


def _durable_copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with source.open('rb') as input_file, destination.open('wb') as output_file:
        shutil.copyfileobj(input_file, output_file)
        output_file.flush()
        os.fsync(output_file.fileno())


def _durable_write(path: Path, contents: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('wb') as output:
        output.write(contents)
        output.flush()
        os.fsync(output.fileno())


def _publication_results_directory() -> Path:
    if CANONICAL_RAW_DIRECTORY.parent != CANONICAL_PLOT_DIRECTORY.parent:
        raise BenchmarkError(
            'canonical raw and plot directories must share one results directory'
        )
    return CANONICAL_RAW_DIRECTORY.parent


@contextmanager
def _publication_lock(control_directory: Path) -> Iterator[None]:
    descriptor = os.open(control_directory, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _publication_checkpoint(_transition: str) -> None:
    """Fault-injection seam for durable publication transition tests."""


def _publication_hash(path: Path) -> str:
    digest = file_sha256(path)
    if digest is None:
        raise BenchmarkError(f'publication artifact cannot be read: {path}')
    return digest


def _clone_results_tree(
    source: Path,
    destination: Path,
    *,
    excluded_names: frozenset[str] = frozenset(),
) -> None:
    destination.mkdir(mode=source.stat().st_mode & 0o777)
    for entry in os.scandir(source):
        if entry.name in excluded_names:
            continue
        source_path = Path(entry.path)
        destination_path = destination / entry.name
        if entry.is_symlink():
            destination_path.symlink_to(os.readlink(source_path))
        elif entry.is_dir(follow_symlinks=False):
            _clone_results_tree(source_path, destination_path)
        elif entry.is_file(follow_symlinks=False):
            os.link(source_path, destination_path)
        # Runtime sockets and other special files are intentionally ephemeral.


def _fsync_tree(root: Path) -> None:
    for path in root.rglob('*'):
        if path.is_file() and not path.is_symlink():
            with path.open('rb') as artifact:
                os.fsync(artifact.fileno())
    for directory, _subdirectories, _files in os.walk(root, topdown=False):
        fsync_directory(Path(directory))


def _exchange_directories(first: Path, second: Path) -> None:
    if first.stat().st_dev != second.stat().st_dev:
        raise BenchmarkError('publication generations must share one filesystem')
    libc = ctypes.CDLL(None, use_errno=True)
    try:
        renameat2 = libc.renameat2
    except AttributeError as error:
        raise BenchmarkError(
            'atomic directory exchange requires Linux renameat2'
        ) from error
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    if (
        renameat2(
            -100,
            os.fsencode(first),
            -100,
            os.fsencode(second),
            2,
        )
        != 0
    ):
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number), first, second)


def _validated_publication_artifacts(
    results_directory: Path, value: object
) -> list[PublicationArtifact]:
    if not isinstance(value, list) or not value:
        raise BenchmarkError('publication record contains no artifacts')
    allowed_directories = {
        results_directory / CANONICAL_RAW_DIRECTORY.name,
        results_directory / CANONICAL_PLOT_DIRECTORY.name,
    }
    artifacts: list[PublicationArtifact] = []
    seen: set[Path] = set()
    for raw_artifact in value:
        if not isinstance(raw_artifact, dict):
            raise BenchmarkError('publication record contains an invalid artifact')
        raw_path = raw_artifact.get('path')
        digest = raw_artifact.get('sha256')
        if not isinstance(raw_path, str) or not isinstance(digest, str):
            raise BenchmarkError('publication artifact identity is incomplete')
        relative = Path(raw_path)
        candidate = results_directory / relative
        if (
            relative.is_absolute()
            or '..' in relative.parts
            or candidate.parent not in allowed_directories
            or candidate in seen
        ):
            raise BenchmarkError('publication artifact path is invalid or duplicated')
        if re.fullmatch(r'[0-9a-f]{64}', digest) is None:
            raise BenchmarkError('publication artifact hash is invalid')
        seen.add(candidate)
        artifacts.append({'path': raw_path, 'sha256': digest})
    return artifacts


def _load_generation(results_directory: Path) -> PublicationGeneration | None:
    marker = (
        results_directory / CANONICAL_RAW_DIRECTORY.name / PUBLICATION_GENERATION_NAME
    )
    if not marker.exists():
        return None
    if marker.is_symlink() or not marker.is_file():
        raise BenchmarkError(f'publication generation marker is invalid: {marker}')
    try:
        value = json.loads(marker.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise BenchmarkError(
            f'publication generation marker cannot be read: {marker}'
        ) from error
    if (
        not isinstance(value, dict)
        or value.get('schema_version') != PUBLICATION_SCHEMA_VERSION
    ):
        raise BenchmarkError('publication generation marker has an unsupported schema')
    token = value.get('token')
    if not isinstance(token, str) or re.fullmatch(r'[0-9a-f]{16}', token) is None:
        raise BenchmarkError('publication generation marker has an invalid token')
    return {
        'schema_version': PUBLICATION_SCHEMA_VERSION,
        'token': token,
        'artifacts': _validated_publication_artifacts(
            results_directory, value.get('artifacts')
        ),
    }


def _validate_generation_files(
    results_directory: Path, artifacts: list[PublicationArtifact]
) -> None:
    expected = {results_directory / artifact['path'] for artifact in artifacts}
    actual = set(
        (results_directory / CANONICAL_RAW_DIRECTORY.name).glob('benchmark_*.json')
    )
    actual.update(
        (results_directory / CANONICAL_PLOT_DIRECTORY.name).glob('benchmark_*.svg')
    )
    actual.add(results_directory / CANONICAL_RAW_DIRECTORY.name / 'run_manifest.json')
    if actual != expected:
        raise BenchmarkError('publication generation has stale or missing artifacts')
    for artifact in artifacts:
        path = results_directory / artifact['path']
        if (
            path.is_symlink()
            or not path.is_file()
            or file_sha256(path) != artifact['sha256']
        ):
            raise BenchmarkError(f'publication generation artifact is stale: {path}')


def _generation_token(results_directory: Path) -> str | None:
    generation = _load_generation(results_directory)
    return None if generation is None else generation['token']


def _cleanup_publication_transaction(
    control_directory: Path, transaction: Path
) -> None:
    if transaction.is_symlink() or (transaction.exists() and not transaction.is_dir()):
        transaction.unlink(missing_ok=True)
    elif transaction.exists():
        shutil.rmtree(transaction)
    fsync_directory(control_directory)


def _cleanup_orphaned_publication_files(control_directory: Path) -> None:
    changed = False
    for transaction in control_directory.glob(f'{PUBLICATION_TRANSACTION_PREFIX}*'):
        if transaction.is_symlink() or not transaction.is_dir():
            transaction.unlink(missing_ok=True)
        else:
            shutil.rmtree(transaction)
        changed = True
    if changed:
        fsync_directory(control_directory)


def _recover_publication_locked(results_directory: Path) -> None:
    """Return the results namespace to a clean single-generation state.

    RENAME_EXCHANGE is the single commit point, so a crash leaves at most an
    orphaned staging or swapped-out ``.benchmark-publication-*`` tree beside
    an untouched or fully committed canonical generation. Recovery removes
    the orphans and validates the canonical generation marker when present.
    """
    _cleanup_orphaned_publication_files(results_directory.parent)
    generation = _load_generation(results_directory)
    if generation is not None:
        _validate_generation_files(results_directory, generation['artifacts'])


def _recover_publication() -> None:
    results_directory = _publication_results_directory()
    control_directory = results_directory.parent
    control_directory.mkdir(parents=True, exist_ok=True)
    with _publication_lock(control_directory):
        _recover_publication_locked(results_directory)


def _stage_publication(
    results_directory: Path,
    run_directory: Path,
    scenario_names: list[str],
    manifest: PublicationManifest,
    token: str,
) -> Path:
    """Build, fsync, and validate the complete staged generation tree."""
    if manifest.get('status') != 'complete':
        raise BenchmarkError('only a complete benchmark run can be published')
    if not scenario_names or len(set(scenario_names)) != len(scenario_names):
        raise BenchmarkError('publication scenario names must be nonempty and unique')
    if any(re.fullmatch(r'[a-z0-9_]+', name) is None for name in scenario_names):
        raise BenchmarkError('publication scenario name is not canonical')

    sources: list[tuple[Path, str, str]] = []
    for clean_name in scenario_names:
        for kind, suffix, canonical_directory in (
            ('raw', '.json', CANONICAL_RAW_DIRECTORY.name),
            ('plots', '.svg', CANONICAL_PLOT_DIRECTORY.name),
        ):
            source = run_directory / kind / f'benchmark_{clean_name}{suffix}'
            if source.is_symlink() or not source.is_file():
                raise BenchmarkError(f'publication artifact is missing: {source}')
            sources.append((source, canonical_directory, source.name))

    control_directory = results_directory.parent
    transaction = control_directory / f'{PUBLICATION_TRANSACTION_PREFIX}{token}'
    _clone_results_tree(
        results_directory,
        transaction,
        excluded_names=frozenset({
            CANONICAL_RAW_DIRECTORY.name,
            CANONICAL_PLOT_DIRECTORY.name,
        }),
    )
    staged_raw = transaction / CANONICAL_RAW_DIRECTORY.name
    staged_plots = transaction / CANONICAL_PLOT_DIRECTORY.name
    staged_raw.mkdir()
    staged_plots.mkdir()
    for source, directory, name in sources:
        _durable_copy(source, transaction / directory / name)
    _durable_write(
        staged_raw / 'run_manifest.json',
        (json.dumps(manifest, indent=2, sort_keys=True) + '\n').encode(),
    )

    artifacts: list[PublicationArtifact] = []
    for directory, pattern in (
        (staged_raw, '*.json'),
        (staged_plots, '*.svg'),
    ):
        artifacts.extend(
            PublicationArtifact(
                path=str(artifact.relative_to(transaction)),
                sha256=_publication_hash(artifact),
            )
            for artifact in sorted(directory.glob(pattern))
        )
    generation: PublicationGeneration = {
        'schema_version': PUBLICATION_SCHEMA_VERSION,
        'token': token,
        'artifacts': artifacts,
    }
    _durable_write(
        staged_raw / PUBLICATION_GENERATION_NAME,
        (json.dumps(generation, indent=2, sort_keys=True) + '\n').encode(),
    )
    _fsync_tree(transaction)
    fsync_directory(control_directory)
    _validate_generation_files(transaction, artifacts)
    return transaction


def _publish_artifacts(
    run_directory: Path, scenario_names: list[str], manifest: PublicationManifest
) -> None:
    """Atomically replace the canonical generation.

    The staged generation is built and fsynced beside the canonical tree,
    then one RENAME_EXCHANGE is the single commit point. Either the exchange
    happened (the new generation is fully visible) or it did not (the
    canonical tree is untouched); the swapped-out tree is deleted afterwards,
    and a crash leaves only an orphan that recovery removes at next run start.
    """
    results_directory = _publication_results_directory()
    control_directory = results_directory.parent
    control_directory.mkdir(parents=True, exist_ok=True)
    with _publication_lock(control_directory):
        _recover_publication_locked(results_directory)
        token = secrets.token_hex(8)
        transaction: Path | None = None
        try:
            transaction = _stage_publication(
                results_directory, run_directory, scenario_names, manifest, token
            )
            _publication_checkpoint('staged')
            _exchange_directories(results_directory, transaction)
            fsync_directory(control_directory)
            _publication_checkpoint('exchanged')
            _cleanup_publication_transaction(control_directory, transaction)
            _publication_checkpoint('cleaned')
        except Exception:
            # The exchange is the commit point: once the new generation is
            # visible, the publication succeeded and at worst an orphaned
            # swapped-out tree remains. Clean it up best-effort here; the
            # next run start removes anything left behind.
            if _generation_token(results_directory) == token:
                assert transaction is not None
                with suppress(Exception):
                    _cleanup_publication_transaction(control_directory, transaction)
                return
            if transaction is not None:
                _cleanup_publication_transaction(control_directory, transaction)
            raise


def _validate_publication_harness(
    harness: HarnessConfig, system: BenchmarkSystemState
) -> None:
    """Require one stable, documented methodology for canonical artifacts."""
    canonical = HarnessConfig(
        server_cpus=harness.server_cpus,
        load_cpus=harness.load_cpus,
        management_cpus=harness.management_cpus,
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
    if (
        harness.server_cpus is None
        or harness.load_cpus is None
        or harness.management_cpus is None
    ):
        raise BenchmarkError(
            'publication requires explicit server, load-generator, and management CPUs'
        )
    if len(harness.management_cpus) != 1:
        raise BenchmarkError('publication requires exactly one management CPU')
    if len(harness.server_cpus) != 4:
        raise BenchmarkError(
            'publication requires exactly four server CPUs for the 1/4-worker suite'
        )
    if len(harness.load_cpus) < 4:
        raise BenchmarkError(
            'publication requires at least four load-generator CPUs for '
            'a meaningful headroom comparison'
        )
    server_cpu_set = set(harness.server_cpus)
    load_cpu_set = set(harness.load_cpus)
    management_cpu_set = set(harness.management_cpus)
    overlap = (server_cpu_set & load_cpu_set) | (
        management_cpu_set & (server_cpu_set | load_cpu_set)
    )
    if overlap:
        raise BenchmarkError(
            'publication requires disjoint server/load/management CPU sets; overlap: '
            + ','.join(map(str, sorted(overlap)))
        )
    server = system['server']
    load = system['load_generator']
    management = system['management']
    if server is None or load is None or management is None:
        raise BenchmarkError('publication requires captured CPU-role topology')
    selected = [*server['topology'], *load['topology'], *management['topology']]
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
        set(harness.server_cpus + harness.load_cpus + harness.management_cpus)
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

    server_cores = physical_keys(server)
    load_cores = physical_keys(load)
    management_cores = physical_keys(management)
    if len(server_cores) != 4:
        raise BenchmarkError('publication requires four distinct physical server cores')
    physical_overlap = (server_cores & load_cores) | (
        management_cores & (server_cores | load_cores)
    )
    if physical_overlap:
        raise BenchmarkError(
            'publication requires disjoint physical CPU roles; overlap: '
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
    policies = [entry['frequency'] for entry in selected]
    governors = {policy['scaling_governor'] for policy in policies}
    if governors != {'performance'}:
        raise BenchmarkError(
            'publication requires the performance CPU governor; found '
            + repr(sorted(governor for governor in governors if governor is not None))
        )
    drivers = {policy['scaling_driver'] for policy in policies}
    if None in drivers or len(drivers) != 1:
        raise BenchmarkError(
            'publication requires one recorded CPU-frequency driver; found '
            + repr(sorted(driver for driver in drivers if driver is not None))
        )
    preferences = {
        policy['energy_performance_preference']
        for policy in policies
        if policy['energy_performance_preference'] is not None
    }
    if preferences and preferences != {'performance'}:
        raise BenchmarkError(
            'publication requires performance energy preference when available; found '
            + repr(sorted(preferences))
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

    server_llcs = llc_domains(server)
    load_llcs = llc_domains(load)
    if len(server_llcs) != 1 or len(load_llcs) != 1:
        raise BenchmarkError(
            'publication requires each CPU role to occupy one last-level-cache domain'
        )
    if server_llcs & load_llcs:
        raise BenchmarkError(
            'publication requires distinct server/load last-level-cache domains'
        )


def _scenario_record(
    scenario: Scenario,
    *,
    status: RunStatus,
    identity_sha256: str,
    excluded_servers: dict[str, str],
    provenance: dict[str, Any],
    runs: list[dict[str, Any]],
    aggregate: dict[str, AggregateMetrics],
    headroom: HeadroomLadder | dict[str, str] | None,
    pending_attempt: PendingAttempt | None = None,
    error: str | None = None,
) -> ScenarioRecord:
    return {
        'schema_version': 4,
        'status': status,
        'run_identity_sha256': identity_sha256,
        'scenario': asdict(scenario),
        'excluded_servers': excluded_servers,
        'provenance': provenance,
        'runs': runs,
        'aggregate': aggregate,
        'load_generator_headroom': headroom,
        'pending_attempt': pending_attempt,
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
        'schema_version': 4,
        'status': status,
        'run_identity': identity,
        'scenarios': {
            name: record['status'] for name, record in scenario_records.items()
        },
        'publication_failures': publication_failures,
        'load_generator_headroom': headroom_ladders,
        'error': error,
    }


def _pending_attempt_record(
    *,
    trial: int,
    order: list[str],
    server: str,
    ambient_cpu: AmbientCpuProbe | None,
    server_command: CommandIdentity,
    warmup_result: LoadResult,
    warmup_metrics: Metrics,
) -> PendingAttempt:
    return {
        'recorded_at': datetime.now(UTC).isoformat(),
        'trial': trial,
        'order': order.copy(),
        'server': server,
        'ambient_cpu_before': ambient_cpu,
        'server_command': server_command,
        'warmup': {
            'load_command': warmup_result.command,
            'load_generator_usage': warmup_result.usage,
            'raw': warmup_result.raw,
            'metrics': warmup_metrics,
        },
    }


def _exception_summary(error: BaseException) -> str:
    detail = str(error)
    name = type(error).__name__
    return f'{name}: {detail}' if detail else name


def _finalize_failed_run(
    run_directory: Path,
    identity: dict[str, Any],
    scenario_records: dict[str, ScenarioRecord],
    publication_failures: list[str],
    headroom_ladders: dict[str, HeadroomLadder | dict[str, str]],
    error: BaseException,
) -> None:
    """Atomically retain the latest evidence when an in-process run aborts."""
    summary = _exception_summary(error)
    for name, record in tuple(scenario_records.items()):
        if record['status'] != 'running':
            continue
        failed_record: ScenarioRecord = {
            **record,
            'status': 'failed',
            'error': summary,
        }
        scenario_records[name] = failed_record
        durable_json(run_directory / 'raw' / f'benchmark_{name}.json', failed_record)
    durable_json(
        run_directory / 'manifest.json',
        _manifest_record(
            identity,
            scenario_records,
            publication_failures,
            headroom_ladders,
            status='failed',
            error=summary,
        ),
    )


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
    if publish and len(scenarios) != HEADROOM_FAMILY_SIZE:
        raise BenchmarkError(
            'publication requires the complete Bonferroni scenario family of '
            f'{HEADROOM_FAMILY_SIZE}; selected {len(scenarios)}'
        )
    servers = [
        server
        for server in SERVERS
        if not selected_servers or server in selected_servers
    ]
    if not scenarios or not servers:
        raise BenchmarkError('benchmark selection is empty')

    # Stabilize the results namespace before placing a new run inside it.  Delaying
    # recovery until publication could exchange an interrupted generation after
    # this run was created, moving the run into the transaction tree that cleanup
    # removes.
    _recover_publication()

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
    load_tools = {response_contract(scenario.type).driver for scenario in scenarios}
    artifacts = artifact_snapshot(server_commands, load_tools)
    git_identity = _git_identity()
    snapshot_gate = RunSnapshotGate(
        artifacts, git_identity, system_state, harness, server_commands, load_tools
    )
    versions = get_versions()
    provenance = benchmark_provenance(
        harness, artifacts, git_identity, versions, system_state
    )
    identity = {
        'schema_version': 4,
        'run_id': run_id,
        'harness': asdict(harness),
        'scenarios': [asdict(scenario) for scenario in scenarios],
        'servers': servers,
        'server_profiles': {server: SERVER_PROFILES[server] for server in servers},
        'commands': {
            key: command_provenance(command) for key, command in server_commands.items()
        },
        'load_command_contracts': {
            _clean_scenario_name(scenario.name): _load_command_contract(
                scenario, harness
            )
            for scenario in scenarios
        },
        'artifacts': artifacts,
        'git': git_identity,
        'versions': versions,
        'system': system_state,
        'runtime_environment': _runtime_environment(),
    }
    identity['sha256'] = hashlib.sha256(
        json.dumps(identity, sort_keys=True).encode()
    ).hexdigest()
    durable_json(run_directory / 'identity.json', identity)

    scenario_records: dict[str, ScenarioRecord] = {}
    headroom_ladders: dict[str, HeadroomLadder | dict[str, str]] = {}
    publication_failures: list[str] = []
    durable_json(
        run_directory / 'manifest.json',
        _manifest_record(
            identity,
            scenario_records,
            publication_failures,
            headroom_ladders,
            status='running',
        ),
    )
    try:
        for scenario in scenarios:
            print(f'\n=== Benchmarking {scenario.name} ===')
            eligible = _eligible_servers(scenario, servers)
            try:
                orders = balanced_orders(eligible, harness.trials, harness.order_seed)
            except ValueError as error:
                raise BenchmarkError(f'{scenario.name}: {error}') from error
            samples: dict[str, list[Metrics]] = {server: [] for server in eligible}
            raw_runs: list[dict[str, Any]] = []
            excluded_servers: dict[str, str] = {}
            clean_name = _clean_scenario_name(scenario.name)
            raw_path = run_directory / 'raw' / f'benchmark_{clean_name}.json'

            # The checkpoint closure reads the current iteration's loop
            # variables directly; it is never called across iterations, so
            # the late-binding hazard B023 warns about cannot occur.
            def checkpoint(
                status: RunStatus,
                aggregate: dict[str, AggregateMetrics] | None = None,
                headroom: HeadroomLadder | dict[str, str] | None = None,
                *,
                pending_attempt: PendingAttempt | None = None,
                error: str | None = None,
                manifest_status: RunStatus = 'running',
            ) -> ScenarioRecord:
                record = _scenario_record(
                    scenario,  # noqa: B023
                    status=status,
                    identity_sha256=identity['sha256'],
                    excluded_servers=excluded_servers.copy(),  # noqa: B023
                    provenance=provenance,
                    runs=raw_runs.copy(),  # noqa: B023
                    aggregate={} if aggregate is None else aggregate,
                    headroom=headroom,
                    pending_attempt=pending_attempt,
                    error=error,
                )
                scenario_records[clean_name] = record  # noqa: B023
                durable_json(raw_path, record)  # noqa: B023
                durable_json(
                    run_directory / 'manifest.json',
                    _manifest_record(
                        identity,
                        scenario_records,
                        publication_failures,
                        headroom_ladders,
                        status=manifest_status,
                    ),
                )
                return record

            checkpoint('running')
            for trial, order in enumerate(orders, start=1):
                print(f'--- trial {trial}/{harness.trials}: {" -> ".join(order)} ---')
                for server in order:
                    if server in excluded_servers:
                        continue
                    socket_path = (
                        UNIX_SOCKET_PATH if scenario.type == 'h1_uds' else None
                    )
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
                    ambient_cpu: AmbientCpuProbe | None = None
                    interference_cpu: CpuActivity | None = None
                    post_worker_pids: list[int] | None = None
                    try:
                        snapshot_gate.verify_cell()
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
                                harness,
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
                            if publish:
                                ambient_cpu = _capture_ambient_cpu(
                                    harness, system_state
                                )
                                _validate_ambient_cpu(ambient_cpu, harness)
                            pending_attempt = _pending_attempt_record(
                                trial=trial,
                                order=order,
                                server=server,
                                ambient_cpu=ambient_cpu,
                                server_command=command_provenance(command),
                                warmup_result=warmup_result,
                                warmup_metrics=warmup_metrics,
                            )
                            checkpoint('running', pending_attempt=pending_attempt)
                            # Persisting the pending attempt is deliberately
                            # synchronous. Recheck host quietness afterwards so
                            # its I/O cannot become unmeasured benchmark noise.
                            if publish:
                                ambient_cpu = _capture_ambient_cpu(
                                    harness, system_state
                                )
                                _validate_ambient_cpu(ambient_cpu, harness)
                            print(f'Running load generation for {server}...')
                            interference_monitor = (
                                _interference_monitor(harness, system_state)
                                if publish
                                else None
                            )
                            resource_sampler = ProcessGroupResourceSampler(
                                server_run['process_group_id']
                            )
                            if interference_monitor is not None:
                                interference_monitor.start()
                            resource_sampler.start()
                            try:
                                load_result = _run_scenario_load(
                                    scenario,
                                    harness,
                                    socket_path,
                                    run_directory
                                    / 'load'
                                    / f'{_clean_scenario_name(scenario.name)}-{trial}-{server}.json',
                                )
                            finally:
                                server_resources = resource_sampler.stop()
                                if interference_monitor is not None:
                                    interference_cpu = interference_monitor.stop()
                            if publish:
                                assert interference_cpu is not None
                                _validate_interference_cpu(interference_cpu, harness)
                            metrics = _validate_scenario_load(
                                scenario,
                                harness,
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
                            publication_failures.append(
                                f'{scenario.name}/{server}/trial {trial}: load generator '
                                'used '
                                f'{load_result.usage["physical_core_utilization"]:.1%} '
                                'of physical-core CPU capacity'
                            )
                        raw_runs.append({
                            'trial': trial,
                            'order': order,
                            'server': server,
                            'ambient_cpu_before': ambient_cpu,
                            'interference_cpu_during': interference_cpu,
                            'server_command': command_provenance(command),
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
                        checkpoint('running')
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
                            'ambient_cpu_before': ambient_cpu,
                            'interference_cpu_during': interference_cpu,
                            'server_command': command_provenance(command),
                            'warmup': _scenario_load_evidence(
                                warmup_result,
                                warmup_metrics,
                            ),
                            'load_command': load_result.command
                            if load_result
                            else None,
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
                        if isinstance(error, AmbientCpuError):
                            publication_failures.append(
                                f'{scenario.name}/{server}/trial {trial}: {error}'
                            )
                            checkpoint(
                                'failed', error=str(error), manifest_status='failed'
                            )
                            raise
                        excluded_servers[server] = str(error)
                        checkpoint('running', error=str(error))
                        print(f'Excluding {server} from {scenario.name}: {error}')

            results: dict[str, AggregateMetrics] = {
                server: aggregate_metrics(values)
                for server, values in samples.items()
                if server not in excluded_servers and len(values) == harness.trials
            }
            status = (
                'complete'
                if not excluded_servers and len(results) == len(eligible)
                else 'failed'
            )
            if publish and status == 'complete':
                fastest_server = max(results, key=lambda server: results[server]['rps'])
                try:
                    ladder = _measure_load_headroom(
                        scenario,
                        fastest_server,
                        harness,
                        run_directory,
                        snapshot_gate,
                        system_state,
                    )
                    headroom_ladders[clean_name] = ladder
                    if not ladder['plateau_observed']:
                        detail = (
                            f'paired median gain '
                            f'{ladder["full_vs_reduced_gain"]:.1%}, quartiles '
                            f'[{ladder["paired_gain_lower_quartile"]:.1%}, '
                            f'{ladder["paired_gain_upper_quartile"]:.1%}], '
                            f'full wins {ladder["full_win_count"]}/'
                            f'{ladder["paired_comparisons"]}, one-sided sign-test '
                            f'p={ladder["sign_test_one_sided_p"]:.3f}; '
                            f'equivalence below '
                            f'{ladder["maximum_publish_gain"]:.1%}: '
                            f'{ladder["equivalence_below_margin_count"]}/'
                            f'{ladder["equivalence_comparisons"]}, one-sided '
                            f'p={ladder["equivalence_sign_test_one_sided_p"]:.4f}'
                        )
                        if not ladder['all_runs_have_cpu_headroom']:
                            detail += (
                                ', at least one calibration run exceeded the CPU gate'
                            )
                        publication_failures.append(
                            f'{scenario.name}: load-generator plateau not proven ({detail}) '
                            f'when increasing load-generator worker threads from '
                            f'{ladder["reduced_worker_threads"]} to '
                            f'{ladder["full_worker_threads"]} on the same CPU set'
                        )
                except AmbientCpuError as error:
                    headroom_ladders[clean_name] = {'error': str(error)}
                    publication_failures.append(
                        f'{scenario.name}: headroom ladder aborted: {error}'
                    )
                    checkpoint(
                        'failed',
                        results,
                        headroom_ladders[clean_name],
                        manifest_status='failed',
                    )
                    raise
                except BenchmarkError as error:
                    headroom_ladders[clean_name] = {'error': str(error)}
                    publication_failures.append(
                        f'{scenario.name}: headroom ladder failed: {error}'
                    )
            checkpoint(
                status,
                results,
                headroom_ladders.get(clean_name),
            )
            if results:
                concurrency = scenario.concurrency or harness.concurrency
                load_shape = f'{concurrency} conn'
                if scenario.http2_parallelism > 1:
                    load_shape += f' x {scenario.http2_parallelism} streams'
                plot_results(
                    results,
                    f'Benchmark: {scenario.name} '
                    f'({harness.duration} sustained, {load_shape})',
                    run_directory / 'plots' / f'benchmark_{clean_name}.svg',
                    system_summary=system_state['summary'],
                    websocket=scenario.type == 'ws',
                )

        snapshot_gate.verify_full()
        failed_scenarios = [
            name
            for name, record in scenario_records.items()
            if record['status'] != 'complete'
        ]
        manifest_status: RunStatus = (
            'complete'
            if not failed_scenarios and not publication_failures
            else 'failed'
        )
        manifest = _manifest_record(
            identity,
            scenario_records,
            publication_failures,
            headroom_ladders,
            status=manifest_status,
        )
        durable_json(run_directory / 'manifest.json', manifest)
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
    except BaseException as error:
        try:
            _finalize_failed_run(
                run_directory,
                identity,
                scenario_records,
                publication_failures,
                headroom_ladders,
                error,
            )
        except Exception as persistence_error:
            traceback.print_exception(persistence_error)
        raise
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
    parser.add_argument('--trials', type=int, default=TRIALS)
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
        default=LOAD_WORKER_THREADS,
        help='fixed oha Tokio workers / k6 GOMAXPROCS',
    )
    parser.add_argument(
        '--max-load-utilization',
        type=float,
        default=MAX_LOAD_UTILIZATION,
        help='maximum aggregate load-generator CPU utilization for publication',
    )
    parser.add_argument(
        '--ambient-cpu-probe-seconds',
        type=float,
        default=AMBIENT_CPU_PROBE_SECONDS,
        help='idle-host CPU probe before each published load',
    )
    parser.add_argument(
        '--max-ambient-cpu-utilization',
        type=float,
        default=MAX_AMBIENT_CPU_UTILIZATION,
        help='maximum system/server/load CPU use before each published load',
    )
    parser.add_argument(
        '--publish',
        action='store_true',
        help='replace canonical raw records and plots after a fully staged clean run',
    )
    args = parser.parse_args()

    if args.trials < 2 or args.trials % 2:
        parser.error('--trials must be a positive even number')
    if args.settle_seconds < 0:
        parser.error('--settle-seconds cannot be negative')
    if args.qps is not None and args.qps < 1:
        parser.error('--qps must be positive')
    if args.concurrency < 1:
        parser.error('--concurrency must be positive')
    if args.load_worker_threads < 1:
        parser.error('--load-worker-threads must be positive')
    try:
        _duration_seconds(args.duration)
        _duration_seconds(args.warmup_duration)
    except BenchmarkError as error:
        parser.error(str(error))
    if not 0 < args.max_load_utilization < 1:
        parser.error('--max-load-utilization must be between 0 and 1')
    if args.ambient_cpu_probe_seconds <= 0:
        parser.error('--ambient-cpu-probe-seconds must be positive')
    if not 0 < args.max_ambient_cpu_utilization < 1:
        parser.error('--max-ambient-cpu-utilization must be between 0 and 1')
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

    harness = HarnessConfig(
        duration=args.duration,
        warmup_duration=args.warmup_duration,
        concurrency=args.concurrency,
        trials=args.trials,
        settle_seconds=args.settle_seconds,
        rate_limit_qps=args.qps,
        server_cpus=server_cpus,
        load_cpus=load_cpus,
        management_cpus=management_cpus,
        load_worker_threads=args.load_worker_threads,
        max_load_utilization=args.max_load_utilization,
        ambient_cpu_probe_seconds=args.ambient_cpu_probe_seconds,
        max_ambient_cpu_utilization=args.max_ambient_cpu_utilization,
    )
    run_benchmarks(
        selected_servers,
        selected_types,
        harness=harness,
        publish=args.publish,
    )


if __name__ == '__main__':
    main()
