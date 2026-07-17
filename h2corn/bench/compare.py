"""Reproducible paired A/B benchmark harness for h2corn server builds.

Each block starts both named server variants in a seed-controlled AB/BA order.
Warmup blocks are retained in the evidence record but excluded from statistics.
Measured samples are compared within their block so slow ambient drift is less
likely to be mistaken for a code change.
"""

from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import math
import os
import random
import re
import shlex
import shutil
import socket
import ssl
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, TypedDict, cast

import h2.config
import h2.connection
import h2.events
import h2.exceptions

try:
    from bench.provenance import (
        FileIdentity,
        NamedCommand,
        VariantArtifacts,
        VariantEnvironmentMode,
        command_version,
        file_identity,
        file_sha256,
        git_metadata,
        path_fingerprint,
        result_environment,
        subprocess_environment,
        tool_identity,
        variant_artifacts,
        variant_environment_evidence,
    )
    from bench.system import (
        MAX_AMBIENT_CPU_UTILIZATION,
        MAX_AMBIENT_SINGLE_CPU_UTILIZATION,
        RESOURCE_SAMPLE_INTERVAL_SECONDS,
        AmbientCpuProbe,
        BenchmarkError,
        BenchmarkSystemState,
        CpuActivity,
        CpuActivityMonitor,
        ProcessGroupResourceSampler,
        ProcessGroupUsage,
        benchmark_system_state_matches,
        capture_role_ambient_cpu,
        capture_system_state,
        durable_json,
        parse_linux_cpu_list,
        physical_core_capacity,
        pin_benchmark_driver,
        terminate_process_group,
        validate_ambient_cpu,
        validate_cpu_roles,
        validate_interference_cpu,
        validate_k6_result,
        validate_oha_result,
        wait_for_http_server,
        wait_for_unix_server,
    )
except ModuleNotFoundError:  # Direct ``python bench/compare.py`` execution.
    from provenance import (  # type: ignore[import-not-found, no-redef]
        FileIdentity,
        NamedCommand,
        VariantArtifacts,
        VariantEnvironmentMode,
        command_version,
        file_identity,
        file_sha256,
        git_metadata,
        path_fingerprint,
        result_environment,
        subprocess_environment,
        tool_identity,
        variant_artifacts,
        variant_environment_evidence,
    )
    from system import (  # type: ignore[import-not-found, no-redef]
        MAX_AMBIENT_CPU_UTILIZATION,
        MAX_AMBIENT_SINGLE_CPU_UTILIZATION,
        RESOURCE_SAMPLE_INTERVAL_SECONDS,
        AmbientCpuProbe,
        BenchmarkError,
        BenchmarkSystemState,
        CpuActivity,
        CpuActivityMonitor,
        ProcessGroupResourceSampler,
        ProcessGroupUsage,
        benchmark_system_state_matches,
        capture_role_ambient_cpu,
        capture_system_state,
        durable_json,
        parse_linux_cpu_list,
        physical_core_capacity,
        pin_benchmark_driver,
        terminate_process_group,
        validate_ambient_cpu,
        validate_cpu_roles,
        validate_interference_cpu,
        validate_k6_result,
        validate_oha_result,
        wait_for_http_server,
        wait_for_unix_server,
    )

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SEED = 20_260_710
DEFAULT_TRIALS = 10
DEFAULT_BOOTSTRAP_SAMPLES = 10_000
DEFAULT_LOAD_WARMUP_DURATION = '1s'
DEFAULT_AMBIENT_CPU_PROBE_SECONDS = 1.0
COMPARISON_SCHEMA_VERSION = 9
MAX_DURATION_SECONDS = 3_600.0
MAX_LOAD_UTILIZATION = 0.85
NAME_PATTERN = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.-]*$')
DURATION_PATTERN = re.compile(r'^(\d+(?:\.\d+)?)(ms|s|m|h)$')
SHA256_PATTERN = re.compile(r'^[0-9a-f]{64}$')
HTTPProtocol = Literal['1.0', '1.1', '2', 'unknown']
HostNoiseMode = Literal[
    'pinned-noise-gated',
    'diagnostic-pinned-noisy',
    'diagnostic-unpinned',
]
ComparisonVerdict = Literal[
    'STABLE_ABOVE_IQR',
    'INCONCLUSIVE',
    'DIAGNOSTIC_PINNED_NOISY',
    'DIAGNOSTIC_UNPINNED',
    'DIAGNOSTIC_VARIANT_ENVIRONMENT_DRIFT',
    'DIAGNOSTIC_PINNED_NOISY_AND_VARIANT_ENVIRONMENT_DRIFT',
    'DIAGNOSTIC_UNPINNED_AND_VARIANT_ENVIRONMENT_DRIFT',
]


class PairedComparison(TypedDict):
    control_samples: list[float]
    candidate_samples: list[float]
    control_median: float
    candidate_median: float
    candidate_over_control: float
    paired_delta_percent: list[float]
    paired_delta_median_percent: float
    paired_delta_ci95_percent: list[float]
    paired_delta_iqr_percent: float
    higher_is_better: bool
    improvement_median_percent: float
    directionally_stable_above_iqr: bool


class MetricComparison(PairedComparison):
    host_noise_mode: HostNoiseMode
    variant_environment_mode: VariantEnvironmentMode
    verdict: ComparisonVerdict


class LoadGeneratorResources(ProcessGroupUsage):
    logical_cpu_capacity: int
    physical_core_capacity: int
    logical_cpu_utilization: float
    physical_core_utilization: float
    physical_core_headroom: float
    maximum_physical_core_utilization: float
    sufficient_headroom: bool


class ResponseEvidence(TypedDict):
    status: int
    body_size: int
    body_sha256: str
    protocol: HTTPProtocol


class CorrectnessEvidence(TypedDict):
    pre_load: ResponseEvidence | None
    post_warmup: ResponseEvidence | None
    post_load: ResponseEvidence | None
    pre_load_worker_pids: list[int]
    post_warmup_worker_pids: list[int] | None
    post_load_worker_pids: list[int]


class LoadExecution(TypedDict):
    command: list[str]
    elapsed_seconds: float
    exit_code: int
    resources: LoadGeneratorResources
    raw: dict[str, Any]


class LoadWarmupEvidence(TypedDict):
    duration: str
    duration_seconds: float
    response: ResponseEvidence | None
    worker_pids: list[int]
    load: LoadExecution


class HostCpuNoiseEvidence(TypedDict):
    mode: HostNoiseMode
    ambient_cpu_attempts: list[AmbientCpuProbe]
    ambient_cpu_before: AmbientCpuProbe | None
    interference_cpu_during: CpuActivity | None


class VariantResources(TypedDict):
    server: ProcessGroupUsage
    load_generator: LoadGeneratorResources


class VariantRun(TypedDict):
    variant: str
    server_command: list[str]
    started_at: str
    elapsed_seconds: float
    metrics: dict[str, float]
    correctness: CorrectnessEvidence
    resources: VariantResources
    load_warmup: LoadWarmupEvidence | None
    load: LoadExecution
    host_cpu: HostCpuNoiseEvidence


class VariantRunProgress(TypedDict, total=False):
    variant: str
    server_command: list[str]
    started_at: str
    host_cpu: HostCpuNoiseEvidence
    elapsed_seconds: float
    metrics: dict[str, float]
    correctness: CorrectnessEvidence
    resources: VariantResources
    load_warmup: LoadWarmupEvidence | None
    load: LoadExecution


def parse_named_command(value: str) -> NamedCommand:
    """Parse ``NAME=COMMAND`` without invoking a shell."""
    name, separator, command = value.partition('=')
    if not separator or not NAME_PATTERN.fullmatch(name):
        raise argparse.ArgumentTypeError(
            'expected NAME=COMMAND with a name containing letters, digits, ., _, or -'
        )
    try:
        argv = tuple(shlex.split(command))
    except ValueError as error:
        raise argparse.ArgumentTypeError(f'invalid command quoting: {error}') from error
    if not argv:
        raise argparse.ArgumentTypeError('server command must not be empty')
    return NamedCommand(name=name, argv=argv)


def parse_cpu_set(value: str) -> tuple[int, ...]:
    try:
        return parse_linux_cpu_list(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            'CPU sets use non-negative IDs and inclusive ranges, for example 2,4-6'
        ) from error


def parse_duration_seconds(value: str) -> float:
    match = DURATION_PATTERN.fullmatch(value)
    if match is None:
        raise argparse.ArgumentTypeError(
            'duration must look like 500ms, 10s, 2m, or 1h'
        )
    magnitude = float(match.group(1))
    multiplier = {'ms': 0.001, 's': 1.0, 'm': 60.0, 'h': 3_600.0}[match.group(2)]
    seconds = magnitude * multiplier
    if not 0.0 < seconds <= MAX_DURATION_SECONDS:
        raise argparse.ArgumentTypeError(
            f'duration must be greater than zero and at most {MAX_DURATION_SECONDS:g}s'
        )
    return seconds


def blocked_orders(blocks: int, seed: int) -> tuple[tuple[str, str], ...]:
    """Return balanced, interleaved control/candidate lead orders."""
    if blocks < 0:
        raise ValueError('blocks must be non-negative')
    control_first = bool(random.Random(seed).getrandbits(1))
    return tuple(
        ('control', 'candidate')
        if control_first == (index % 2 == 0)
        else ('candidate', 'control')
        for index in range(blocks)
    )


def percentile(values: Sequence[float], quantile: float) -> float:
    if not values:
        raise ValueError('percentile requires at least one value')
    if not 0.0 <= quantile <= 1.0:
        raise ValueError('quantile must be between zero and one')
    ordered = sorted(values)
    position = quantile * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def bootstrap_median_ci(
    values: Sequence[float],
    seed: int,
    *,
    samples: int = DEFAULT_BOOTSTRAP_SAMPLES,
) -> tuple[float, float]:
    if not values:
        raise ValueError('bootstrap requires at least one value')
    if samples < 1:
        raise ValueError('bootstrap samples must be positive')
    rng = random.Random(seed)
    size = len(values)
    medians = [
        statistics.median(values[rng.randrange(size)] for _ in range(size))
        for _ in range(samples)
    ]
    return percentile(medians, 0.025), percentile(medians, 0.975)


def paired_comparison(
    control: Sequence[float],
    candidate: Sequence[float],
    *,
    seed: int,
    higher_is_better: bool,
    bootstrap_samples: int = DEFAULT_BOOTSTRAP_SAMPLES,
) -> PairedComparison:
    """Summarize paired candidate/control percentage changes."""
    if len(control) != len(candidate) or not control:
        raise ValueError('paired comparison requires equal non-empty samples')
    if any(
        not math.isfinite(value) or value <= 0.0 for value in [*control, *candidate]
    ):
        raise ValueError('paired comparison samples must be finite and positive')

    deltas = [
        (candidate_value / control_value - 1.0) * 100.0
        for control_value, candidate_value in zip(control, candidate, strict=True)
    ]
    delta = statistics.median(deltas)
    ci_low, ci_high = bootstrap_median_ci(deltas, seed, samples=bootstrap_samples)
    paired_delta_iqr = percentile(deltas, 0.75) - percentile(deltas, 0.25)
    stable_above_iqr = (ci_low > 0.0 or ci_high < 0.0) and abs(delta) > paired_delta_iqr
    improvement = delta if higher_is_better else -delta
    return {
        'control_samples': list(control),
        'candidate_samples': list(candidate),
        'control_median': statistics.median(control),
        'candidate_median': statistics.median(candidate),
        'candidate_over_control': statistics.median(candidate)
        / statistics.median(control),
        'paired_delta_percent': deltas,
        'paired_delta_median_percent': delta,
        'paired_delta_ci95_percent': [ci_low, ci_high],
        'paired_delta_iqr_percent': paired_delta_iqr,
        'higher_is_better': higher_is_better,
        'improvement_median_percent': improvement,
        'directionally_stable_above_iqr': stable_above_iqr,
    }


def _affinity_prefix(cpus: tuple[int, ...] | None) -> list[str]:
    if cpus is None:
        return []
    return ['taskset', '--cpu-list', ','.join(map(str, cpus))]


@contextmanager
def _owned_process_group(
    process: subprocess.Popen[bytes], interval: float
) -> Iterator[ProcessGroupResourceSampler]:
    """Own a subprocess group from launch through monitoring and cleanup.

    The cleanup region begins before the monitor starts, so an observation or
    monitor-thread failure cannot leave a server or load generator behind.
    """
    monitor = ProcessGroupResourceSampler(process.pid, interval)
    monitor_started = False
    try:
        monitor.start()
        monitor_started = True
        yield monitor
    finally:
        try:
            terminate_process_group(process)
        finally:
            if monitor_started:
                monitor.stop()


def _headers(values: Sequence[str]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for value in values:
        name, separator, content = value.partition(':')
        if not separator or not name.strip():
            raise BenchmarkError(
                f'invalid HTTP header, expected NAME: VALUE: {value!r}'
            )
        headers[name.strip()] = content.strip()
    return headers


def _http_protocol(version: int) -> HTTPProtocol:
    if version == 10:
        return '1.0'
    if version == 11:
        return '1.1'
    return 'unknown'


def _fetch_http_response(
    url: str,
    *,
    method: str = 'GET',
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
    unix_socket: Path | None = None,
    insecure: bool = False,
    timeout: float = 2.0,
) -> tuple[int, bytes, HTTPProtocol]:
    """Fetch a complete HTTP response without invoking a shell."""
    request_headers = headers or {}
    if unix_socket is None:
        context = ssl._create_unverified_context() if insecure else None  # noqa: S323
        request = urllib.request.Request(  # noqa: S310
            url, data=body, headers=request_headers, method=method
        )
        try:
            with urllib.request.urlopen(  # noqa: S310
                request, timeout=timeout, context=context
            ) as response:
                protocol = _http_protocol(response.version)
                return response.status, response.read(), protocol
        except urllib.error.HTTPError as error:
            protocol = _http_protocol(error.version)
            return error.code, error.read(), protocol

    parsed = urllib.parse.urlsplit(url)
    target = urllib.parse.urlunsplit(('', '', parsed.path or '/', parsed.query, ''))
    wire_headers = {'Host': parsed.netloc or 'localhost', 'Connection': 'close'}
    wire_headers.update(request_headers)
    payload = body or b''
    if body is not None:
        wire_headers.setdefault('Content-Length', str(len(payload)))
    header_lines = ''.join(
        f'{name}: {value}\r\n' for name, value in wire_headers.items()
    )
    request_bytes = (
        f'{method} {target} HTTP/1.1\r\n{header_lines}\r\n'.encode() + payload
    )
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(timeout)
        client.connect(str(unix_socket))
        client.sendall(request_bytes)
        response = http.client.HTTPResponse(client)
        response.begin()
        protocol = _http_protocol(response.version)
        return response.status, response.read(), protocol


def _fetch_h2_response(
    url: str,
    *,
    method: str,
    body: bytes | None,
    headers: dict[str, str],
    unix_socket: Path | None,
    insecure: bool,
    timeout: float = 2.0,
) -> tuple[int, bytes, HTTPProtocol]:
    """Fetch one exact response over direct HTTP/2 and prove the protocol."""
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme not in {'http', 'https'} or parsed.hostname is None:
        raise BenchmarkError(f'HTTP/2 probe requires an http(s) URL: {url!r}')
    if unix_socket is not None and parsed.scheme == 'https':
        raise BenchmarkError(
            'TLS HTTP/2 correctness probes do not support Unix sockets'
        )

    raw_socket: socket.socket
    if unix_socket is not None:
        raw_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        raw_socket.settimeout(timeout)
        raw_socket.connect(str(unix_socket))
    else:
        port = parsed.port or (443 if parsed.scheme == 'https' else 80)
        raw_socket = socket.create_connection((parsed.hostname, port), timeout=timeout)

    connection_socket: socket.socket = raw_socket
    try:
        if parsed.scheme == 'https':
            context = (
                ssl._create_unverified_context()  # noqa: S323
                if insecure
                else ssl.create_default_context()
            )
            context.set_alpn_protocols(['h2'])
            connection_socket = context.wrap_socket(
                raw_socket, server_hostname=parsed.hostname
            )
            if connection_socket.selected_alpn_protocol() != 'h2':
                raise BenchmarkError(
                    'TLS HTTP/2 correctness probe did not negotiate ALPN h2'
                )

        connection = h2.connection.H2Connection(
            config=h2.config.H2Configuration(
                client_side=True,
                header_encoding=None,
            )
        )
        connection.initiate_connection()
        connection_socket.sendall(connection.data_to_send())
        stream_id = connection.get_next_available_stream_id()
        target = urllib.parse.urlunsplit(('', '', parsed.path or '/', parsed.query, ''))
        authority = parsed.netloc
        request_headers: list[tuple[bytes, bytes]] = [
            (b':method', method.encode()),
            (b':scheme', parsed.scheme.encode()),
            (b':authority', authority.encode()),
            (b':path', target.encode()),
        ]
        for name, value in headers.items():
            lowered = name.lower()
            if lowered in {'connection', 'host', 'transfer-encoding'}:
                continue
            request_headers.append((lowered.encode(), value.encode()))
        payload = body or b''
        if body is not None and not any(
            name == b'content-length' for name, _ in request_headers
        ):
            request_headers.append((b'content-length', str(len(payload)).encode()))
        connection.send_headers(stream_id, request_headers, end_stream=not payload)
        payload_offset = _queue_h2_request_body(
            connection,
            stream_id,
            payload,
            offset=0,
        )
        connection_socket.sendall(connection.data_to_send())

        status: int | None = None
        response_body = bytearray()
        ended = False
        while not ended:
            data = connection_socket.recv(64 * 1024)
            if not data:
                raise BenchmarkError(
                    'HTTP/2 correctness probe closed before stream end'
                )
            for event in connection.receive_data(data):
                if isinstance(event, h2.events.ResponseReceived):
                    raw_status = next(
                        (value for name, value in event.headers if name == b':status'),
                        None,
                    )
                    if raw_status is None:
                        raise BenchmarkError('HTTP/2 response omitted :status')
                    status = int(raw_status)
                elif isinstance(event, h2.events.DataReceived):
                    response_body.extend(event.data)
                    connection.acknowledge_received_data(
                        event.flow_controlled_length, event.stream_id
                    )
                elif isinstance(event, h2.events.StreamEnded):
                    if event.stream_id == stream_id:
                        ended = True
                elif isinstance(event, h2.events.StreamReset):
                    if event.stream_id == stream_id:
                        raise BenchmarkError(
                            f'HTTP/2 correctness stream reset: {event.error_code}'
                        )
                elif isinstance(event, h2.events.ConnectionTerminated):
                    raise BenchmarkError(
                        f'HTTP/2 correctness connection terminated: {event.error_code}'
                    )
            if not ended:
                payload_offset = _queue_h2_request_body(
                    connection,
                    stream_id,
                    payload,
                    offset=payload_offset,
                )
            pending = connection.data_to_send()
            if pending:
                connection_socket.sendall(pending)
        if payload_offset != len(payload):
            raise BenchmarkError(
                'HTTP/2 response ended before the request body was sent'
            )
        if status is None:
            raise BenchmarkError('HTTP/2 correctness response omitted final status')
        return status, bytes(response_body), '2'
    except (OSError, h2.exceptions.H2Error, ValueError) as error:
        raise BenchmarkError(f'HTTP/2 correctness probe failed: {error}') from error
    finally:
        connection_socket.close()


def _queue_h2_request_body(
    connection: h2.connection.H2Connection,
    stream_id: int,
    payload: bytes,
    *,
    offset: int,
) -> int:
    """Queue every DATA frame currently permitted by HTTP/2 flow control."""
    while offset < len(payload):
        chunk_size = min(
            len(payload) - offset,
            connection.local_flow_control_window(stream_id),
            connection.max_outbound_frame_size,
        )
        if chunk_size <= 0:
            break
        next_offset = offset + chunk_size
        connection.send_data(
            stream_id,
            payload[offset:next_offset],
            end_stream=next_offset == len(payload),
        )
        offset = next_offset
    return offset


def _expected_response(args: argparse.Namespace) -> ResponseEvidence | None:
    if args.expected_body_sha256 is None:
        return None
    request_body = args.body.encode() if args.body is not None else None
    request_headers = _headers(args.header)
    if args.http2:
        status, body, protocol = _fetch_h2_response(
            args.url,
            method=args.method,
            body=request_body,
            headers=request_headers,
            unix_socket=args.unix_socket,
            insecure=args.insecure,
        )
    else:
        status, body, protocol = _fetch_http_response(
            args.url,
            method=args.method,
            body=request_body,
            headers=request_headers,
            unix_socket=args.unix_socket,
            insecure=args.insecure,
        )
    evidence: ResponseEvidence = {
        'status': status,
        'body_size': len(body),
        'body_sha256': hashlib.sha256(body).hexdigest(),
        'protocol': protocol,
    }
    expected_protocol = '2' if args.http2 else '1.1'
    if (
        status != args.expected_status
        or evidence['body_size'] != args.expected_body_size
        or evidence['body_sha256'] != args.expected_body_sha256
        or protocol != expected_protocol
    ):
        raise BenchmarkError(
            'HTTP response contract failed: '
            f'observed={evidence!r}, expected_status={args.expected_status}, '
            f'expected_body_size={args.expected_body_size}, '
            f'expected_body_sha256={args.expected_body_sha256}, '
            f'expected_protocol={expected_protocol}'
        )
    return evidence


def _worker_pid_url(ready_url: str) -> str:
    parsed = urllib.parse.urlsplit(ready_url)
    return urllib.parse.urlunsplit((
        parsed.scheme,
        parsed.netloc,
        '/__bench/worker-pid',
        '',
        '',
    ))


def _wait_for_workers(
    process: subprocess.Popen[bytes], args: argparse.Namespace
) -> list[int]:
    seen: set[int] = set()
    deadline = time.monotonic() + args.startup_timeout
    url = _worker_pid_url(args.ready_url)
    while len(seen) < args.expected_workers and time.monotonic() < deadline:
        if process.poll() is not None:
            raise BenchmarkError(
                f'server exited during worker convergence with status {process.returncode}'
            )
        try:
            status, body, _protocol = _fetch_http_response(
                url,
                unix_socket=args.ready_unix_socket,
                insecure=args.insecure,
                timeout=min(0.5, max(deadline - time.monotonic(), 0.05)),
            )
            if status == 200:
                pid = int(body)
                if pid > 0:
                    seen.add(pid)
        except (OSError, ValueError, urllib.error.URLError):
            pass
    if len(seen) != args.expected_workers:
        raise BenchmarkError(
            f'expected {args.expected_workers} workers but observed '
            f'{len(seen)} distinct PIDs before timeout: {sorted(seen)}'
        )
    return sorted(seen)


def _default_ready_url(url: str, load_driver: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    if load_driver == 'k6':
        scheme = {'ws': 'http', 'wss': 'https'}.get(parsed.scheme)
        if scheme is None:
            raise ValueError('k6 load URLs must use ws:// or wss://')
        return urllib.parse.urlunsplit((scheme, parsed.netloc, '/', '', ''))
    if parsed.scheme not in {'http', 'https'}:
        raise ValueError('oha load URLs must use http:// or https://')
    return url


def _run_oha(
    args: argparse.Namespace,
    *,
    duration: str | None = None,
    duration_seconds: float | None = None,
) -> LoadExecution:
    duration = args.duration if duration is None else duration
    duration_seconds = (
        args.duration_seconds if duration_seconds is None else duration_seconds
    )
    command = [
        *_affinity_prefix(args.load_cpus),
        args.oha,
        '-z',
        duration,
        '-c',
        str(args.concurrency),
        '--output-format',
        'json',
        '-m',
        args.method,
    ]
    command.extend(['--http2'] if args.http2 else ['--http-version', '1.1'])
    if args.insecure:
        command.append('--insecure')
    if args.disable_keepalive:
        command.append('--disable-keepalive')
    if args.unix_socket is not None:
        command.extend(['--unix-socket', str(args.unix_socket)])
    if args.body is not None:
        command.extend(['-d', args.body])
    for header in args.header:
        command.extend(['-H', header])
    command.append(args.url)

    started = time.monotonic()
    process = subprocess.Popen(
        command,
        cwd=ROOT,
        env=subprocess_environment(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    try:
        with _owned_process_group(process, args.rss_sample_interval) as monitor:
            stdout, stderr = process.communicate(
                timeout=duration_seconds + args.load_grace
            )
    except subprocess.TimeoutExpired as error:
        stdout, stderr = process.communicate()
        raise BenchmarkError(
            f'oha exceeded its bounded timeout; stderr={stderr[-2_000:].decode(errors="replace")!r}'
        ) from error
    resources = _load_generator_resources(args, monitor.stop())

    elapsed = time.monotonic() - started
    if process.returncode != 0:
        raise BenchmarkError(
            f'oha exited with status {process.returncode}; '
            f'stderr={stderr[-2_000:].decode(errors="replace")!r}'
        )
    try:
        raw = json.loads(stdout)
    except json.JSONDecodeError as error:
        raise BenchmarkError(
            f'oha did not return JSON; stdout={stdout[-2_000:].decode(errors="replace")!r}'
        ) from error
    return {
        'command': command,
        'elapsed_seconds': elapsed,
        'exit_code': process.returncode,
        'resources': resources,
        'raw': raw,
    }


def _run_k6(
    args: argparse.Namespace,
    *,
    duration: str | None = None,
    duration_seconds: float | None = None,
) -> LoadExecution:
    duration = args.duration if duration is None else duration
    duration_seconds = (
        args.duration_seconds if duration_seconds is None else duration_seconds
    )
    with tempfile.NamedTemporaryFile(suffix='.json') as summary:
        command = [*_affinity_prefix(args.load_cpus), args.k6, 'run']
        if args.insecure:
            command.append('--insecure-skip-tls-verify')
        command.extend([
            '--quiet',
            '--duration',
            duration,
            '--vus',
            str(args.concurrency),
            '--summary-trend-stats',
            'med,p(99),p(99.9)',
            '--summary-export',
            summary.name,
            '-e',
            f'WS_URL={args.url}',
            str(args.k6_script),
        ])
        started = time.monotonic()
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            env=subprocess_environment(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        try:
            with _owned_process_group(process, args.rss_sample_interval) as monitor:
                stdout, stderr = process.communicate(
                    timeout=duration_seconds + args.load_grace
                )
        except subprocess.TimeoutExpired as error:
            stdout, stderr = process.communicate()
            raise BenchmarkError(
                'k6 exceeded its bounded timeout; '
                f'stderr={stderr[-2_000:].decode(errors="replace")!r}'
            ) from error
        resources = _load_generator_resources(args, monitor.stop())

        elapsed = time.monotonic() - started
        if process.returncode != 0:
            raise BenchmarkError(
                f'k6 exited with status {process.returncode}; '
                f'stderr={stderr[-2_000:].decode(errors="replace")!r}'
            )
        try:
            raw = json.loads(Path(summary.name).read_text())
        except (OSError, json.JSONDecodeError) as error:
            raise BenchmarkError(
                'k6 did not write valid summary JSON; '
                f'stdout={stdout[-2_000:].decode(errors="replace")!r}'
            ) from error
        return {
            'command': command,
            'elapsed_seconds': elapsed,
            'exit_code': process.returncode,
            'resources': resources,
            'raw': raw,
        }


def _load_generator_resources(
    args: argparse.Namespace, resources: ProcessGroupUsage
) -> LoadGeneratorResources:
    cpus = args.load_cpus or tuple(sorted(os.sched_getaffinity(0)))
    logical_capacity = len(cpus)
    physical_capacity = physical_core_capacity(cpus)
    logical_utilization = resources['average_cpu_cores'] / logical_capacity
    physical_utilization = resources['average_cpu_cores'] / physical_capacity
    sufficient = (
        resources['sample_count'] > 0
        and physical_utilization <= args.max_load_utilization
    )
    return {
        **resources,
        'logical_cpu_capacity': logical_capacity,
        'physical_core_capacity': physical_capacity,
        'logical_cpu_utilization': logical_utilization,
        'physical_core_utilization': physical_utilization,
        'physical_core_headroom': max(0.0, 1.0 - physical_utilization),
        'maximum_physical_core_utilization': args.max_load_utilization,
        'sufficient_headroom': sufficient,
    }


def _extract_k6_metrics(raw: dict[str, Any]) -> dict[str, float]:
    metrics = raw.get('metrics')
    if not isinstance(metrics, dict):
        raise BenchmarkError('k6 JSON is missing metrics')
    sources = {
        'requests_per_second': ('iterations', 'rate'),
        'latency_p50_seconds': ('ws_session_duration', 'med'),
        'latency_p99_seconds': ('ws_session_duration', 'p(99)'),
        'latency_p99_9_seconds': ('ws_session_duration', 'p(99.9)'),
    }
    extracted: dict[str, float] = {}
    for target, (metric, key) in sources.items():
        metric_values = metrics.get(metric, {})
        values = (
            metric_values.get('values', metric_values)
            if isinstance(metric_values, dict)
            else {}
        )
        value = values.get(key) if isinstance(values, dict) else None
        if not isinstance(value, int | float) or not math.isfinite(value) or value <= 0:
            raise BenchmarkError(f'k6 JSON has invalid {metric}.{key}: {value!r}')
        # k6 durations are milliseconds, while compare's canonical latency unit is s.
        extracted[target] = (
            float(value) / 1_000.0 if target.startswith('latency_') else float(value)
        )
    return extracted


def _run_load(
    args: argparse.Namespace,
    *,
    duration: str | None = None,
    duration_seconds: float | None = None,
) -> tuple[LoadExecution, dict[str, float]]:
    if args.load_driver == 'oha':
        load = _run_oha(args, duration=duration, duration_seconds=duration_seconds)
        validate_oha_result(load['raw'], args.expected_status, args.concurrency)
        metrics = _extract_oha_metrics(load['raw'])
    else:
        load = _run_k6(args, duration=duration, duration_seconds=duration_seconds)
        validate_k6_result(load['raw'])
        metrics = _extract_k6_metrics(load['raw'])
    resources = load['resources']
    if not resources['sufficient_headroom']:
        raise BenchmarkError(
            'load-generator CPU headroom gate failed: '
            f'physical-core usage={resources["physical_core_utilization"]:.1%}, '
            'maximum='
            f'{resources["maximum_physical_core_utilization"]:.1%}, '
            f'samples={resources["sample_count"]}'
        )
    return load, metrics


def host_noise_mode(
    server_cpus: tuple[int, ...] | str | None,
    maximum_ambient_cpu_utilization: float = MAX_AMBIENT_CPU_UTILIZATION,
    maximum_ambient_single_cpu_utilization: float = (
        MAX_AMBIENT_SINGLE_CPU_UTILIZATION
    ),
) -> HostNoiseMode:
    """Classify whether role-aware host-noise evidence can be collected."""
    if server_cpus is None:
        return 'diagnostic-unpinned'
    if (
        maximum_ambient_cpu_utilization > MAX_AMBIENT_CPU_UTILIZATION
        or maximum_ambient_single_cpu_utilization > MAX_AMBIENT_SINGLE_CPU_UTILIZATION
    ):
        return 'diagnostic-pinned-noisy'
    return 'pinned-noise-gated'


def _args_host_noise_mode(args: argparse.Namespace) -> HostNoiseMode:
    return host_noise_mode(
        args.server_cpus,
        args.max_ambient_cpu_utilization,
        args.max_ambient_single_cpu_utilization,
    )


def _capture_ambient_cpu(
    args: argparse.Namespace, system: BenchmarkSystemState
) -> AmbientCpuProbe:
    return capture_role_ambient_cpu(system, args.ambient_cpu_probe_seconds)


def _validate_ambient_cpu(probe: AmbientCpuProbe, args: argparse.Namespace) -> None:
    validate_ambient_cpu(
        probe,
        max_aggregate=args.max_ambient_cpu_utilization,
        max_single=args.max_ambient_single_cpu_utilization,
    )


def _interference_monitor(
    args: argparse.Namespace, system: BenchmarkSystemState
) -> CpuActivityMonitor:
    if (
        args.server_cpus is None
        or args.load_cpus is None
        or args.management_cpus is None
    ):
        raise BenchmarkError('interference monitor requires pinned CPU roles')
    online = set(system['online_cpus'])
    interference_cpus = tuple(
        sorted(
            online
            - set(args.server_cpus)
            - set(args.load_cpus)
            - set(args.management_cpus)
        )
    )
    if not interference_cpus:
        raise BenchmarkError(
            'pinned CPU noise gating requires at least one unused logical CPU'
        )
    return CpuActivityMonitor(
        interference_cpus,
        physical_core_capacity(interference_cpus),
    )


def _validate_interference_cpu(activity: CpuActivity, args: argparse.Namespace) -> None:
    validate_interference_cpu(
        activity,
        max_aggregate=args.max_ambient_cpu_utilization,
        max_single=args.max_ambient_single_cpu_utilization,
    )


def _run_noise_gated_load(
    args: argparse.Namespace,
    system: BenchmarkSystemState,
    evidence: HostCpuNoiseEvidence,
    checkpoint: Callable[[], None] | None = None,
) -> tuple[LoadExecution, dict[str, float]]:
    """Run one retained load with role-aware foreign-CPU evidence when pinned."""
    if evidence['mode'] == 'diagnostic-unpinned':
        return _run_load(args)

    first_probe = _capture_ambient_cpu(args, system)
    evidence['ambient_cpu_attempts'].append(first_probe)
    if checkpoint is not None:
        checkpoint()
    _validate_ambient_cpu(first_probe, args)

    # The durable checkpoint above performs filesystem work. Probe again so
    # measured load starts after, rather than during, that activity.
    ambient = _capture_ambient_cpu(args, system)
    evidence['ambient_cpu_attempts'].append(ambient)
    evidence['ambient_cpu_before'] = ambient
    _validate_ambient_cpu(ambient, args)

    monitor = _interference_monitor(args, system)
    monitor.start()
    load: LoadExecution | None = None
    metrics: dict[str, float] | None = None
    load_error: Exception | None = None
    try:
        load, metrics = _run_load(args)
    except Exception as error:
        load_error = error
    finally:
        interference = monitor.stop()
        evidence['interference_cpu_during'] = interference
        if checkpoint is not None:
            checkpoint()

    _validate_interference_cpu(interference, args)
    if load_error is not None:
        raise load_error.with_traceback(load_error.__traceback__)
    assert load is not None and metrics is not None
    return load, metrics


def _warm_fresh_server(
    process: subprocess.Popen[bytes],
    args: argparse.Namespace,
    expected_worker_pids: list[int],
) -> LoadWarmupEvidence:
    """Exercise and validate the exact load path before measuring this process."""
    load, _metrics = _run_load(
        args,
        duration=args.load_warmup_duration,
        duration_seconds=args.load_warmup_duration_seconds,
    )
    if process.poll() is not None:
        raise BenchmarkError(
            f'server exited during load warmup with status {process.returncode}'
        )
    response = _expected_response(args)
    worker_pids = _wait_for_workers(process, args)
    if worker_pids != expected_worker_pids:
        raise BenchmarkError(
            'worker PID set changed during load warmup: '
            f'before={expected_worker_pids}, after={worker_pids}'
        )
    return {
        'duration': args.load_warmup_duration,
        'duration_seconds': args.load_warmup_duration_seconds,
        'response': response,
        'worker_pids': worker_pids,
        'load': load,
    }


def _extract_oha_metrics(raw: dict[str, Any]) -> dict[str, float]:
    summary = raw.get('summary')
    percentiles = raw.get('latencyPercentiles')
    if not isinstance(summary, dict) or not isinstance(percentiles, dict):
        raise BenchmarkError('oha JSON is missing summary or latencyPercentiles')
    metrics: dict[str, float] = {}
    source_keys = {
        'requests_per_second': (summary, 'requestsPerSec'),
        'latency_p50_seconds': (percentiles, 'p50'),
        'latency_p99_seconds': (percentiles, 'p99'),
        'latency_p99_9_seconds': (percentiles, 'p99.9'),
    }
    for target, (source, key) in source_keys.items():
        value = source.get(key)
        if not isinstance(value, int | float) or not math.isfinite(value) or value <= 0:
            raise BenchmarkError(f'oha JSON has invalid {key}: {value!r}')
        metrics[target] = float(value)
    return metrics


def run_variant(
    command: NamedCommand,
    args: argparse.Namespace,
    *,
    measured: bool,
    system: BenchmarkSystemState,
    progress: VariantRunProgress | None = None,
    checkpoint: Callable[[], None] | None = None,
) -> VariantRun:
    server_command = [*_affinity_prefix(args.server_cpus), *command.argv]
    started_at = datetime.now(UTC).isoformat()
    started = time.monotonic()
    host_cpu: HostCpuNoiseEvidence = {
        'mode': _args_host_noise_mode(args),
        'ambient_cpu_attempts': [],
        'ambient_cpu_before': None,
        'interference_cpu_during': None,
    }
    if progress is None:
        progress = {}
    progress.update({
        'variant': command.name,
        'server_command': server_command,
        'started_at': started_at,
        'host_cpu': host_cpu,
    })
    if checkpoint is not None:
        checkpoint()
    with tempfile.TemporaryFile() as server_log:
        process = subprocess.Popen(
            server_command,
            cwd=ROOT,
            env=subprocess_environment(),
            stdout=server_log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            with _owned_process_group(process, args.rss_sample_interval) as monitor:
                if args.ready_unix_socket is None:
                    wait_for_http_server(
                        process,
                        args.ready_url,
                        args.startup_timeout,
                        insecure=args.insecure,
                    )
                else:
                    wait_for_unix_server(
                        process, args.ready_unix_socket, args.startup_timeout
                    )
                pre_load_worker_pids = _wait_for_workers(process, args)
                pre_load_response = _expected_response(args)
                load_warmup = (
                    _warm_fresh_server(process, args, pre_load_worker_pids)
                    if measured
                    else None
                )
                time.sleep(args.settle)
                load, metrics = _run_noise_gated_load(
                    args,
                    system,
                    host_cpu,
                    checkpoint,
                )
                if process.poll() is not None:
                    raise BenchmarkError(
                        f'server exited during load with status {process.returncode}'
                    )
                post_load_response = _expected_response(args)
                post_load_worker_pids = _wait_for_workers(process, args)
                if post_load_worker_pids != pre_load_worker_pids:
                    raise BenchmarkError(
                        'worker PID set changed during the measured run: '
                        f'before={pre_load_worker_pids}, '
                        f'after={post_load_worker_pids}'
                    )
        except BaseException as error:
            server_log.seek(0)
            tail = server_log.read()[-4_000:].decode(errors='replace')
            if isinstance(error, BenchmarkError):
                raise BenchmarkError(f'{error}; server log tail={tail!r}') from error
            raise
        server_resources = monitor.stop()

    peak_rss = server_resources['peak_rss_bytes']
    if peak_rss > 0:
        metrics['server_peak_rss_bytes'] = float(peak_rss)
    # The caller-visible progress dict is the single result object: fill it in
    # place so a checkpointed record and the returned run can never diverge.
    progress.update({
        'elapsed_seconds': time.monotonic() - started,
        'metrics': metrics,
        'correctness': {
            'pre_load': pre_load_response,
            'post_warmup': None if load_warmup is None else load_warmup['response'],
            'post_load': post_load_response,
            'pre_load_worker_pids': pre_load_worker_pids,
            'post_warmup_worker_pids': None
            if load_warmup is None
            else load_warmup['worker_pids'],
            'post_load_worker_pids': post_load_worker_pids,
        },
        'resources': {
            'server': server_resources,
            'load_generator': load['resources'],
        },
        'load_warmup': load_warmup,
        'load': load,
    })
    return cast('VariantRun', progress)


def _validate_benchmark_system(
    args: argparse.Namespace, system: BenchmarkSystemState
) -> None:
    validate_cpu_roles(args.server_cpus, args.load_cpus, args.management_cpus, system)


def _capture_benchmark_system(args: argparse.Namespace) -> BenchmarkSystemState:
    system = capture_system_state(
        args.server_cpus, args.load_cpus, args.management_cpus
    )
    _validate_benchmark_system(args, system)
    return system


def collect_environment(
    args: argparse.Namespace,
    frozen_variant_artifacts: dict[str, VariantArtifacts],
) -> dict[str, Any]:
    """Collect ambient environment evidence around the frozen identity.

    The expensive variant-artifact probes are reused from the frozen
    comparison identity instead of being recomputed.
    """
    system = _capture_benchmark_system(args)
    build_environment = {
        name: os.environ.get(name)
        for name in (
            'CARGO_BUILD_RUSTFLAGS',
            'CARGO_ENCODED_RUSTFLAGS',
            'CARGO_PROFILE_RELEASE_CODEGEN_UNITS',
            'CARGO_PROFILE_RELEASE_LTO',
            'CARGO_TARGET_DIR',
            'PYO3_CONFIG_FILE',
            'PYO3_PYTHON',
            'RUSTC_BOOTSTRAP',
            'RUSTC_WRAPPER',
            'RUSTFLAGS',
            '_PYTHON_HOST_PLATFORM',
            '_PYTHON_PROJECT_BASE',
            '_PYTHON_SYSCONFIGDATA_NAME',
        )
    }
    input_hashes = {
        str(path.relative_to(ROOT.parent)): file_sha256(path)
        for path in (
            ROOT / 'Cargo.toml',
            ROOT / 'pyproject.toml',
            ROOT / 'build.rs',
            ROOT.parent / 'Cargo.lock',
            ROOT.parent / 'Cargo.toml',
            ROOT.parent / 'rust-toolchain.toml',
        )
        if path.is_file()
    }
    return {
        'captured_at': datetime.now(UTC).isoformat(),
        'benchmark_system': system,
        'load_average': list(os.getloadavg()) if hasattr(os, 'getloadavg') else None,
        'oha_version': command_version([args.oha, '--version']),
        'k6_version': command_version([args.k6, 'version']),
        'tool_versions': {
            'cargo': command_version(['cargo', '--version', '--verbose']),
            'maturin': command_version(['maturin', '--version']),
            'rustc': command_version(['rustc', '--version', '--verbose']),
            'uv': command_version(['uv', '--version']),
        },
        'variant_artifacts': frozen_variant_artifacts,
        'build_environment': build_environment,
        'input_sha256': input_hashes,
        'git': git_metadata(),
    }


METRICS = {
    'requests_per_second': True,
    'latency_p50_seconds': False,
    'latency_p99_seconds': False,
    'latency_p99_9_seconds': False,
    'server_peak_rss_bytes': False,
}


def _identity_inputs(args: argparse.Namespace) -> dict[str, FileIdentity]:
    paths = [
        Path(__file__),
        ROOT / 'bench/bench_app.py',
        ROOT / 'bench/_file_response_payload.bin',
        ROOT / 'pyproject.toml',
        ROOT / 'uv.lock',
        *args.identity_input,
    ]
    if args.load_driver == 'k6':
        paths.append(args.k6_script)
    identities: dict[str, FileIdentity] = {}
    for path in paths:
        identity = file_identity(path)
        identities[identity['path']] = identity
    return identities


def comparison_identity(args: argparse.Namespace) -> dict[str, Any]:
    """Return the complete semantic identity required to resume a run safely."""
    control_artifacts = variant_artifacts(args.control)
    candidate_artifacts = variant_artifacts(args.candidate)
    variant_environment = variant_environment_evidence(
        control_artifacts,
        candidate_artifacts,
        allow_drift=args.allow_variant_environment_drift,
    )
    return {
        'seed': args.seed,
        'warmup_blocks': args.warmups,
        'measured_blocks': args.trials,
        'duration': args.duration,
        'concurrency': args.concurrency,
        'url': args.url,
        'ready_url': args.ready_url,
        'unix_socket': str(args.unix_socket) if args.unix_socket is not None else None,
        'ready_unix_socket': str(args.ready_unix_socket)
        if args.ready_unix_socket is not None
        else None,
        'load_driver': args.load_driver,
        'http2': args.http2,
        'insecure': args.insecure,
        'disable_keepalive': args.disable_keepalive,
        'method': args.method,
        'expected_status': args.expected_status,
        'expected_body_size': args.expected_body_size,
        'expected_body_sha256': args.expected_body_sha256,
        'expected_workers': args.expected_workers,
        'body': args.body,
        'headers': list(args.header),
        'server_cpus': list(args.server_cpus) if args.server_cpus else None,
        'load_cpus': list(args.load_cpus) if args.load_cpus else None,
        'management_cpus': list(args.management_cpus) if args.management_cpus else None,
        'load_warmup_duration': args.load_warmup_duration,
        'startup_timeout': args.startup_timeout,
        'load_grace': args.load_grace,
        'settle': args.settle,
        'rss_sample_interval': args.rss_sample_interval,
        'maximum_load_utilization': args.max_load_utilization,
        'ambient_cpu_probe_seconds': args.ambient_cpu_probe_seconds,
        'maximum_ambient_cpu_utilization': args.max_ambient_cpu_utilization,
        'maximum_ambient_single_cpu_utilization': (
            args.max_ambient_single_cpu_utilization
        ),
        'host_noise_mode': _args_host_noise_mode(args),
        'variant_environment': variant_environment,
        'bootstrap_samples': args.bootstrap_samples,
        'load_tool': tool_identity(
            args.oha if args.load_driver == 'oha' else args.k6,
            ('--version',) if args.load_driver == 'oha' else ('version',),
        ),
        'input_files': _identity_inputs(args),
        'runtime_environment': result_environment(),
        'benchmark_system': _capture_benchmark_system(args),
        'source': git_metadata(),
        'variants': {
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
        },
    }


def _verify_comparison_identity(
    args: argparse.Namespace, frozen: dict[str, Any], *, phase: str
) -> None:
    current = comparison_identity(args)
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
            f'frozen benchmark identity changed during {phase}; refusing mixed evidence'
        )


def _identity_file_roots(identity: dict[str, Any]) -> list[Path]:
    """Every file or tree whose contents the frozen identity hashed."""
    roots = [Path(path) for path in identity['input_files']]
    load_tool = identity['load_tool']['executable']
    if load_tool is not None:
        roots.append(Path(load_tool))
    for variant in identity['variants'].values():
        artifacts = variant['artifacts']
        roots.extend(
            Path(artifacts[key])
            for key in (
                'executable',
                'extension',
                'python_executable',
                'python_package',
            )
            if artifacts[key] is not None
        )
        roots.extend(
            Path(dependency['path'])
            for dependency in artifacts['dependencies'].values()
            if dependency['path'] is not None
        )
        roots.extend(Path(path) for path in artifacts['command_inputs'])
    return roots


def _identity_fingerprint(identity: dict[str, Any]) -> dict[str, object]:
    return {
        str(root): path_fingerprint(root) for root in _identity_file_roots(identity)
    }


class ComparisonIdentityGate:
    """Cheap per-phase integrity gate over the frozen comparison identity.

    Every hashed identity input is fingerprinted by (path, size, mtime_ns,
    inode) once at freeze time. Each phase re-checks only the fingerprint;
    the full identity (hashes, probes, git, system state) is recomputed only
    when the fingerprint changes — failing exactly as the full check would —
    and once at suite completion.
    """

    def __init__(self, args: argparse.Namespace, identity: dict[str, Any]) -> None:
        self._args = args
        self._identity = identity
        self._fingerprint = _identity_fingerprint(identity)

    def verify(self, phase: str) -> None:
        current = _identity_fingerprint(self._identity)
        if current == self._fingerprint:
            return
        self.verify_full(phase)
        self._fingerprint = current

    def verify_full(self, phase: str) -> None:
        _verify_comparison_identity(self._args, self._identity, phase=phase)


def summarize_trials(
    trials: Sequence[dict[str, Any]],
    seed: int,
    bootstrap_samples: int,
    noise_mode: HostNoiseMode,
    variant_environment_mode: VariantEnvironmentMode = 'equivalent',
) -> dict[str, MetricComparison]:
    summary: dict[str, MetricComparison] = {}
    for metric_index, (metric, higher_is_better) in enumerate(METRICS.items()):
        controls = [trial['runs']['control']['metrics'].get(metric) for trial in trials]
        candidates = [
            trial['runs']['candidate']['metrics'].get(metric) for trial in trials
        ]
        if any(value is None for value in [*controls, *candidates]):
            continue
        paired = paired_comparison(
            controls,
            candidates,
            seed=seed ^ (0x9E3779B9 * (metric_index + 1)),
            higher_is_better=higher_is_better,
            bootstrap_samples=bootstrap_samples,
        )
        if variant_environment_mode == 'confounded-opt-out' and noise_mode != (
            'pinned-noise-gated'
        ):
            verdict: ComparisonVerdict = (
                'DIAGNOSTIC_UNPINNED_AND_VARIANT_ENVIRONMENT_DRIFT'
                if noise_mode == 'diagnostic-unpinned'
                else 'DIAGNOSTIC_PINNED_NOISY_AND_VARIANT_ENVIRONMENT_DRIFT'
            )
        elif variant_environment_mode == 'confounded-opt-out':
            verdict = 'DIAGNOSTIC_VARIANT_ENVIRONMENT_DRIFT'
        elif noise_mode == 'diagnostic-unpinned':
            verdict = 'DIAGNOSTIC_UNPINNED'
        elif noise_mode == 'diagnostic-pinned-noisy':
            verdict = 'DIAGNOSTIC_PINNED_NOISY'
        elif paired['directionally_stable_above_iqr']:
            verdict = 'STABLE_ABOVE_IQR'
        else:
            verdict = 'INCONCLUSIVE'
        comparison: MetricComparison = {
            **paired,
            'host_noise_mode': noise_mode,
            'variant_environment_mode': variant_environment_mode,
            'verdict': verdict,
        }
        summary[metric] = comparison
    return summary


def _default_output(control: str, candidate: str) -> Path:
    timestamp = datetime.now(UTC).strftime('%Y%m%dT%H%M%S.%fZ')
    return ROOT / 'bench/results/compare' / f'{timestamp}-{control}-vs-{candidate}.json'


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        '--control', required=True, type=parse_named_command, metavar='NAME=COMMAND'
    )
    parser.add_argument(
        '--candidate', required=True, type=parse_named_command, metavar='NAME=COMMAND'
    )
    parser.add_argument('--url', default='http://127.0.0.1:8000/')
    parser.add_argument(
        '--ready-url',
        help='HTTP readiness URL; defaults to --url and treats any status below 500 as ready',
    )
    parser.add_argument('--duration', default='10s', type=str)
    parser.add_argument('--concurrency', default=100, type=int)
    parser.add_argument('--trials', default=DEFAULT_TRIALS, type=int)
    parser.add_argument('--warmups', default=2, type=int)
    parser.add_argument('--seed', default=DEFAULT_SEED, type=int)
    parser.add_argument('--server-cpus', type=parse_cpu_set)
    parser.add_argument('--load-cpus', type=parse_cpu_set)
    parser.add_argument('--management-cpus', type=parse_cpu_set)
    parser.add_argument('--load-warmup-duration', default=DEFAULT_LOAD_WARMUP_DURATION)
    parser.add_argument('--http2', action='store_true')
    parser.add_argument('--insecure', action='store_true')
    parser.add_argument(
        '--disable-keepalive',
        action='store_true',
        help='disable HTTP/1 connection reuse in oha (unsupported with --http2)',
    )
    parser.add_argument('--unix-socket', type=Path)
    parser.add_argument('--ready-unix-socket', type=Path)
    parser.add_argument('--method', default='GET')
    parser.add_argument('--expected-status', default=200, type=int)
    expected_body = parser.add_mutually_exclusive_group()
    expected_body.add_argument(
        '--expected-body',
        help='exact UTF-8 response body required before and after an HTTP load',
    )
    expected_body.add_argument(
        '--expected-body-sha256',
        help='SHA-256 of the exact response body required before and after HTTP load',
    )
    parser.add_argument(
        '--expected-body-size',
        type=int,
        help='byte length paired with --expected-body-sha256',
    )
    parser.add_argument(
        '--expected-workers',
        default=1,
        type=int,
        help='distinct worker PIDs that must converge before and after every load',
    )
    parser.add_argument('--body')
    parser.add_argument('--header', action='append', default=[])
    parser.add_argument('--oha', default='oha', help='oha executable')
    parser.add_argument('--k6', default='k6', help='k6 executable')
    parser.add_argument('--k6-script', default=ROOT / 'bench/k6/ws.js', type=Path)
    parser.add_argument('--load-driver', choices=('oha', 'k6'), default='oha')
    parser.add_argument('--startup-timeout', default=10.0, type=float)
    parser.add_argument('--load-grace', default=15.0, type=float)
    parser.add_argument('--settle', default=0.25, type=float)
    parser.add_argument(
        '--rss-sample-interval',
        default=RESOURCE_SAMPLE_INTERVAL_SECONDS,
        type=float,
    )
    parser.add_argument(
        '--max-load-utilization', default=MAX_LOAD_UTILIZATION, type=float
    )
    parser.add_argument(
        '--ambient-cpu-probe-seconds',
        default=DEFAULT_AMBIENT_CPU_PROBE_SECONDS,
        type=float,
        help='idle-host CPU probe before each retained paired load',
    )
    parser.add_argument(
        '--max-ambient-cpu-utilization',
        default=MAX_AMBIENT_CPU_UTILIZATION,
        type=float,
        help=(
            'maximum aggregate foreign CPU use in physical-core equivalents; '
            'SMT activity can reach 2, which retains rather than rejects it'
        ),
    )
    parser.add_argument(
        '--max-ambient-single-cpu-utilization',
        default=MAX_AMBIENT_SINGLE_CPU_UTILIZATION,
        type=float,
        help=(
            'maximum foreign utilization of one logical CPU; use 1 to retain '
            'but not reject full-core activity on a known noisy host'
        ),
    )
    parser.add_argument(
        '--identity-input',
        action='append',
        default=[],
        type=Path,
        help='additional result-shaping file to hash into the frozen identity',
    )
    parser.add_argument(
        '--allow-variant-environment-drift',
        action='store_true',
        help=(
            'allow differing Python runtimes or shared dependencies, while marking '
            'the result explicitly confounded and diagnostic'
        ),
    )
    parser.add_argument(
        '--bootstrap-samples', default=DEFAULT_BOOTSTRAP_SAMPLES, type=int
    )
    parser.add_argument('--output', type=Path)
    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = create_parser()
    args = parser.parse_args(argv)
    if args.control.name == args.candidate.name:
        parser.error('control and candidate names must differ')
    if args.http2 and args.disable_keepalive:
        parser.error('--disable-keepalive is unsupported with --http2')
    if args.load_driver != 'oha' and args.disable_keepalive:
        parser.error('--disable-keepalive is supported only by the oha load driver')
    try:
        args.duration_seconds = parse_duration_seconds(args.duration)
        args.load_warmup_duration_seconds = parse_duration_seconds(
            args.load_warmup_duration
        )
    except argparse.ArgumentTypeError as error:
        parser.error(str(error))
    if args.trials < 6 or args.trials % 2:
        parser.error('--trials must be an even number of at least 6')
    if args.warmups < 1:
        parser.error('--warmups must be at least 1')
    if args.concurrency < 1:
        parser.error('--concurrency must be positive')
    if not 100 <= args.expected_status <= 599:
        parser.error('--expected-status must be in [100, 599]')
    if args.expected_workers < 1:
        parser.error('--expected-workers must be positive')
    if args.expected_body is not None:
        body = args.expected_body.encode()
        args.expected_body_size = len(body)
        args.expected_body_sha256 = hashlib.sha256(body).hexdigest()
    elif args.expected_body_sha256 is not None:
        if not SHA256_PATTERN.fullmatch(args.expected_body_sha256):
            parser.error('--expected-body-sha256 must be 64 lowercase hex characters')
        if args.expected_body_size is None or args.expected_body_size < 0:
            parser.error(
                '--expected-body-sha256 requires a non-negative --expected-body-size'
            )
    elif args.load_driver == 'oha':
        parser.error(
            'HTTP loads require --expected-body or '
            '--expected-body-sha256 with --expected-body-size'
        )
    elif args.expected_body_size is not None:
        parser.error('--expected-body-size requires --expected-body-sha256')
    if not 0.0 < args.startup_timeout <= 300.0:
        parser.error('--startup-timeout must be in (0, 300] seconds')
    if not 0.0 < args.load_grace <= 300.0:
        parser.error('--load-grace must be in (0, 300] seconds')
    if not 0.0 <= args.settle <= 30.0:
        parser.error('--settle must be in [0, 30] seconds')
    if not 0.005 <= args.rss_sample_interval <= 1.0:
        parser.error('--rss-sample-interval must be in [0.005, 1] seconds')
    if not 0.0 < args.max_load_utilization < 1.0:
        parser.error('--max-load-utilization must be in (0, 1)')
    if args.ambient_cpu_probe_seconds <= 0.0:
        parser.error('--ambient-cpu-probe-seconds must be positive')
    if not 0.0 < args.max_ambient_cpu_utilization <= 2.0:
        parser.error('--max-ambient-cpu-utilization must be in (0, 2]')
    if not 0.0 < args.max_ambient_single_cpu_utilization <= 1.0:
        parser.error('--max-ambient-single-cpu-utilization must be in (0, 1]')
    if args.bootstrap_samples < 1_000:
        parser.error('--bootstrap-samples must be at least 1000')
    try:
        default_ready_url = _default_ready_url(args.url, args.load_driver)
    except ValueError as error:
        parser.error(str(error))
    args.ready_url = args.ready_url or default_ready_url
    ready_scheme = urllib.parse.urlsplit(args.ready_url).scheme
    if ready_scheme not in {'http', 'https'}:
        parser.error('--ready-url must use http:// or https://')
    if args.load_driver == 'k6' and args.unix_socket is not None:
        parser.error('k6 WebSocket loads do not support Unix sockets')
    if args.load_driver == 'k6' and args.ready_unix_socket is not None:
        parser.error('k6 WebSocket loads do not support Unix socket readiness')
    if args.ready_unix_socket is not None and not hasattr(socket, 'AF_UNIX'):
        parser.error('Unix socket readiness is not supported on this platform')
    roles = (args.server_cpus, args.load_cpus, args.management_cpus)
    if any(role is not None for role in roles) and any(role is None for role in roles):
        parser.error(
            'CPU affinity requires --server-cpus, --load-cpus, and --management-cpus'
        )
    if args.management_cpus is not None and len(args.management_cpus) != 1:
        parser.error('--management-cpus requires exactly one CPU')
    if all(role is not None for role in roles):
        server_cpus, load_cpus, management_cpus = map(set, roles)
        overlap = (server_cpus & load_cpus) | (
            management_cpus & (server_cpus | load_cpus)
        )
        if overlap:
            parser.error('server/load/management CPU sets must be disjoint')
    try:
        _headers(args.header)
    except BenchmarkError as error:
        parser.error(str(error))
    for path in args.identity_input:
        if not path.is_file():
            parser.error(f'--identity-input is not a file: {path}')
    args.output = args.output or _default_output(args.control.name, args.candidate.name)
    return args


def _print_summary(summary: dict[str, Any]) -> None:
    units = {
        'requests_per_second': 'req/s',
        'latency_p50_seconds': 's',
        'latency_p99_seconds': 's',
        'latency_p99_9_seconds': 's',
        'server_peak_rss_bytes': 'B',
    }
    if any(
        result['host_noise_mode'] == 'diagnostic-unpinned'
        for result in summary.values()
    ):
        print(
            'host_noise_mode: DIAGNOSTIC_UNPINNED '
            '(no role-aware ambient/interference gate)'
        )
    if any(
        result['host_noise_mode'] == 'diagnostic-pinned-noisy'
        for result in summary.values()
    ):
        print(
            'host_noise_mode: DIAGNOSTIC_PINNED_NOISY '
            '(CPU roles and interference were recorded, but relaxed limits do not '
            'support publication-grade claims)'
        )
    if any(
        result['variant_environment_mode'] == 'confounded-opt-out'
        for result in summary.values()
    ):
        print(
            'variant_environment_mode: CONFOUNDED_OPT_OUT '
            '(Python runtime or shared dependencies differ)'
        )
    for metric, result in summary.items():
        ci_low, ci_high = result['paired_delta_ci95_percent']
        verdict = result['verdict']
        print(
            f'{metric}: control={result["control_median"]:.6g}{units[metric]} '
            f'candidate={result["candidate_median"]:.6g}{units[metric]} '
            f'delta={result["paired_delta_median_percent"]:+.2f}% '
            f'95%CI=[{ci_low:+.2f}%, {ci_high:+.2f}%] '
            f'paired-IQR={result["paired_delta_iqr_percent"]:.2f}% '
            f'improvement={result["improvement_median_percent"]:+.2f}% '
            f'verdict={verdict}'
        )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if (args.server_cpus or args.load_cpus) and shutil.which('taskset') is None:
        raise SystemExit('CPU affinity requested, but taskset is not installed')
    try:
        pin_benchmark_driver(args.management_cpus)
    except BenchmarkError as error:
        raise SystemExit(str(error)) from error
    load_executable = args.oha if args.load_driver == 'oha' else args.k6
    if shutil.which(load_executable) is None and not Path(load_executable).is_file():
        raise SystemExit(f'{args.load_driver} executable not found: {load_executable}')
    if args.load_driver == 'k6' and not args.k6_script.is_file():
        raise SystemExit(f'k6 script not found: {args.k6_script}')

    commands = {'control': args.control, 'candidate': args.candidate}
    orders = blocked_orders(args.warmups + args.trials, args.seed)
    try:
        identity = comparison_identity(args)
    except BenchmarkError as error:
        raise SystemExit(str(error)) from error
    benchmark_system: BenchmarkSystemState = identity['benchmark_system']
    noise_mode = _args_host_noise_mode(args)
    variant_environment_mode: VariantEnvironmentMode = identity['variant_environment'][
        'mode'
    ]
    identity_gate = ComparisonIdentityGate(args, identity)
    record: dict[str, Any] = {
        'schema_version': COMPARISON_SCHEMA_VERSION,
        'status': 'running',
        'comparison_identity': identity,
        'lead_orders': [list(order) for order in orders],
        'environment': collect_environment(
            args,
            {
                role: variant['artifacts']
                for role, variant in identity['variants'].items()
            },
        ),
        'warmups': [],
        'trials': [],
    }
    durable_json(args.output, record)
    try:
        for block_index, order in enumerate(orders):
            measured = block_index >= args.warmups
            block = {
                'block': block_index,
                'lead_order': list(order),
                'runs': {},
            }
            destination = record['trials'] if measured else record['warmups']
            destination.append(block)
            for role in order:
                identity_gate.verify(f'block {block_index} before {role} launch')
                tag = 'measure' if measured else 'warmup'
                print(
                    f'[{tag} block={block_index} lead={order[0]}] '
                    f'{role} ({commands[role].name})'
                )
                progress: VariantRunProgress = {}
                block['runs'][role] = progress
                durable_json(args.output, record)
                run_variant(
                    commands[role],
                    args,
                    measured=measured,
                    system=benchmark_system,
                    progress=progress,
                    checkpoint=lambda: durable_json(args.output, record),
                )
                durable_json(args.output, record)
            identity_gate.verify(f'block {block_index} completion')
        identity_gate.verify_full('suite completion')
        record['summary'] = summarize_trials(
            record['trials'],
            args.seed,
            args.bootstrap_samples,
            noise_mode,
            variant_environment_mode,
        )
        record['status'] = 'complete'
        record['completed_at'] = datetime.now(UTC).isoformat()
    except KeyboardInterrupt:
        record['status'] = 'interrupted'
        record['error'] = 'KeyboardInterrupt'
        record['completed_at'] = datetime.now(UTC).isoformat()
        durable_json(args.output, record)
        print(
            f'benchmark interrupted; partial evidence: {args.output}', file=sys.stderr
        )
        return 130
    except Exception as error:
        record['status'] = 'failed'
        record['error'] = f'{type(error).__name__}: {error}'
        record['completed_at'] = datetime.now(UTC).isoformat()
        durable_json(args.output, record)
        print(f'benchmark failed: {error}', file=sys.stderr)
        print(f'partial evidence: {args.output}', file=sys.stderr)
        return 1

    durable_json(args.output, record)
    _print_summary(record['summary'])
    print(f'raw evidence: {args.output}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
