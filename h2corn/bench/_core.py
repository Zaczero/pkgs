"""Shared benchmark plumbing: scenarios, server processes, load drivers, statistics.

Nothing is pinned to a CPU. Server and load generator run the way a deployment
runs them, and the noise of a shared development host is handled where it
belongs — in the statistics: trials are rotation-balanced, the reported value is
a median, and sampling stops when a bootstrap confidence interval is tight
enough (or a hard time ceiling is reached), never after a fixed count.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import os
import random
import secrets
import socket
import statistics
import struct
import subprocess
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, TypedDict

import h2.config
import h2.connection
import h2.events

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

HOST = '127.0.0.1'
PORT = 8000
RESULTS_DIRECTORY = Path('bench/results')
UNIX_SOCKET_PATH = RESULTS_DIRECTORY / 'benchmark.sock'
STATIC_FILE_PATH = Path('bench/_file_response_payload.bin')
STATIC_FILE_BODY = b'\x00' * (128 * 1024)
DOWNLOAD_BODY = b'x' * (128 * 1024)
K6_SCRIPT = Path('bench/k6/ws.js')

#: Load generators are given the deadline plus this much slack before the run
#: is declared hung.
LOAD_GRACE_SECONDS = 30.0
SERVER_READY_TIMEOUT_SECONDS = 15.0
#: Time between server start and the first request of a trial.
SETTLE_SECONDS = 0.25

BenchmarkType = Literal[
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


class BenchmarkError(RuntimeError):
    """A benchmark could not be measured, so no number may be reported."""


class Metrics(TypedDict):
    rps: float
    latency_percentiles: dict[str, float]


@dataclass(frozen=True, slots=True)
class ResponseContract:
    """One descriptor per benchmark type.

    Owns the request shape, the load driver, the wire protocol and the exact
    expected response, so the load command, the correctness probe and server
    eligibility all derive from a single source.
    """

    path: str
    driver: Literal['oha', 'k6'] = 'oha'
    protocol: Literal['1.1', '2', 'websocket'] = '1.1'
    unix_socket: bool = False
    method: Literal['GET', 'POST'] = 'GET'
    request_body: bytes = b''
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
        response_body=STATIC_FILE_BODY,
        content_type='application/octet-stream',
    ),
    'h2_file': ResponseContract(
        '/static-file',
        protocol='2',
        response_body=STATIC_FILE_BODY,
        content_type='application/octet-stream',
    ),
    'h1_download': ResponseContract(
        '/streaming-download',
        response_body=DOWNLOAD_BODY,
        content_type='application/octet-stream',
    ),
    'h2_download': ResponseContract(
        '/streaming-download',
        protocol='2',
        response_body=DOWNLOAD_BODY,
        content_type='application/octet-stream',
    ),
    'h1_stream': ResponseContract(
        '/streaming-post',
        method='POST',
        request_body=b'x' * 1024,
        response_body=b'stream-started\n1024\nstream-finished\n',
    ),
    'h2_stream': ResponseContract(
        '/streaming-post',
        protocol='2',
        method='POST',
        request_body=b'x' * 1024,
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

    @property
    def contract(self) -> ResponseContract:
        return RESPONSE_CONTRACTS[self.type]

    @property
    def socket_path(self) -> Path | None:
        return UNIX_SOCKET_PATH if self.contract.unix_socket else None

    def slug(self) -> str:
        cleaned = (
            self.name
            .lower()
            .replace('/', '_')
            .replace('(', '')
            .replace(')', '')
            .replace(' ', '_')
        )
        return f'benchmark_{cleaned}'


def ensure_static_file_payload() -> None:
    if STATIC_FILE_PATH.exists() and STATIC_FILE_PATH.stat().st_size == len(
        STATIC_FILE_BODY
    ):
        return
    STATIC_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATIC_FILE_PATH.write_bytes(STATIC_FILE_BODY)


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


# ---------------------------------------------------------------- statistics


def percentile(values: Sequence[float], fraction: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError('percentile of an empty sample')
    position = fraction * (len(ordered) - 1)
    low = math.floor(position)
    high = math.ceil(position)
    if low == high:
        return ordered[low]
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def bootstrap_median_ci(
    values: Sequence[float], seed: int, samples: int = 10_000
) -> tuple[float, float]:
    """Percentile bootstrap 95 % confidence interval for the median."""
    if not values:
        raise ValueError('bootstrap requires at least one value')
    rng = random.Random(seed)
    size = len(values)
    medians = [
        statistics.median(values[rng.randrange(size)] for _ in range(size))
        for _ in range(samples)
    ]
    return percentile(medians, 0.025), percentile(medians, 0.975)


def relative_half_width(values: Sequence[float], seed: int) -> float:
    """Half the median's 95 % CI width, relative to the median.

    Scale-free, so one threshold works for every scenario.
    """
    if len(values) < 2:
        return math.inf
    median = statistics.median(values)
    if median <= 0:
        return math.inf
    low, high = bootstrap_median_ci(values, seed)
    return (high - low) / 2 / median


def _ranked_intervals(
    samples: Sequence[Sequence[float]], seed: int
) -> list[tuple[float, float, float]] | None:
    if any(len(values) < 2 for values in samples):
        return None
    return sorted(
        (
            (statistics.median(values), *bootstrap_median_ci(values, seed))
            for values in samples
        ),
        reverse=True,
    )


def comparison_is_settled(
    samples: Sequence[Sequence[float]], seed: int, target_half_width: float
) -> bool:
    """Is a comparison precise enough to stop sampling?

    Both of the things the chart is read for: the leader is ahead of the field,
    and its number is tight.

    Two things deliberately *not* required. Every server reaching the same
    precision never happens — the slowest is the noisiest in relative terms, so
    a scenario burns its whole budget without the published claim improving.
    Nor every adjacent pair separating: two rivals can be genuinely tied, and
    no amount of sampling separates them. In the published 21-scenario run,
    gunicorn and uvicorn tie for second in one cell and uvicorn and hypercorn
    tie for third in another, while h2corn clears the field in both.
    """
    intervals = _ranked_intervals(samples, seed)
    if intervals is None:
        return False
    leader_median, leader_low, leader_high = intervals[0]
    if leader_median <= 0:
        return False
    if (leader_high - leader_low) / 2 / leader_median > target_half_width:
        return False
    return leader_is_separated(samples, seed)


def leader_is_separated(samples: Sequence[Sequence[float]], seed: int) -> bool:
    """Is the winner the chart draws actually ahead of the runner-up?

    The claim a bar chart cannot walk back, and the one that survives a noisy
    host. Ties further down are a fact about those servers, and overlapping
    whiskers show them for what they are.
    """
    intervals = _ranked_intervals(samples, seed)
    if intervals is None:
        return False
    if len(intervals) < 2:
        return True
    return intervals[0][1] > intervals[1][2]


def rotations(names: Sequence[str], rounds: int, seed: int) -> list[list[str]]:
    """Per-round orders in which every name leads an equal share of rounds.

    Rotating the order is what keeps slow host drift from favouring whichever
    variant happens to run first.
    """
    rng = random.Random(seed)
    base = list(names)
    rng.shuffle(base)
    # Past one full cycle the offset has to wrap: an unbounded index slices to
    # the empty list and every later round repeats the same order, which put
    # one server in the lead for six of nine rounds.
    return [
        base[index % len(base) :] + base[: index % len(base)] for index in range(rounds)
    ]


@dataclass(frozen=True, slots=True)
class PairedComparison:
    control_median: float
    candidate_median: float
    delta_percent: float
    ci_percent: tuple[float, float]
    significant: bool

    def describe(self, unit: str = 'RPS') -> str:
        low, high = self.ci_percent
        verdict = 'significant' if self.significant else 'not significant'
        return (
            f'control {self.control_median:,.0f} {unit} -> '
            f'candidate {self.candidate_median:,.0f} {unit}: '
            f'{self.delta_percent:+.2f}% (95% CI {low:+.2f}%..{high:+.2f}%, {verdict})'
        )


def trimmed_mean(values: Sequence[float], trim: float = 0.2) -> float:
    """Mean of the sample with the extreme `trim` fraction dropped per side.

    The estimator for paired deltas. Pairing has already removed the level, so
    the remaining spread is close to symmetric noise and a mean uses every
    sample — where a median of a handful of rounds can only ever land on one of
    them, and its interval collapses to the gaps between them. Trimming keeps
    the host's occasional stalled round from dragging the estimate.
    """
    ordered = sorted(values)
    drop = int(len(ordered) * trim)
    kept = ordered[drop : len(ordered) - drop] or ordered
    return statistics.fmean(kept)


def bootstrap_trimmed_mean_ci(
    values: Sequence[float], seed: int, samples: int = 10_000
) -> tuple[float, float]:
    """Percentile bootstrap 95 % confidence interval for the trimmed mean."""
    if not values:
        raise ValueError('bootstrap requires at least one value')
    rng = random.Random(seed)
    size = len(values)
    estimates = [
        trimmed_mean([values[rng.randrange(size)] for _ in range(size)])
        for _ in range(samples)
    ]
    return percentile(estimates, 0.025), percentile(estimates, 0.975)


def paired_comparison(
    control: Sequence[float], candidate: Sequence[float], seed: int
) -> PairedComparison:
    """Compare same-round samples pairwise so host drift cancels out."""
    if len(control) != len(candidate) or not control:
        raise ValueError('paired comparison requires equal, non-empty samples')
    deltas = [
        (after - before) / before * 100.0
        for before, after in zip(control, candidate, strict=True)
    ]
    low, high = bootstrap_trimmed_mean_ci(deltas, seed)
    return PairedComparison(
        control_median=statistics.median(control),
        candidate_median=statistics.median(candidate),
        delta_percent=trimmed_mean(deltas),
        ci_percent=(low, high),
        significant=low > 0.0 or high < 0.0,
    )


# ----------------------------------------------------------- server lifetime


def terminate_process_group(process: subprocess.Popen[Any]) -> None:
    """Stop the server and every worker it forked, then reap them."""
    if process.poll() is None:
        try:
            os.killpg(process.pid, 15)
        except (OSError, PermissionError):
            process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, 9)
            except (OSError, PermissionError):
                process.kill()
            process.wait(timeout=10)
    try:
        os.killpg(process.pid, 9)
    except (OSError, PermissionError, ProcessLookupError):
        pass


def _http_ready(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=1) as response:  # noqa: S310
            return response.status < 500
    except urllib.error.HTTPError as error:
        return error.code < 500
    except (OSError, urllib.error.URLError):
        return False


def _unix_ready(socket_path: Path) -> bool:
    if not socket_path.exists():
        return False
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(1)
            client.connect(str(socket_path))
            client.sendall(b'GET / HTTP/1.1\r\nHost: localhost\r\n\r\n')
            return client.recv(64).startswith(b'HTTP/1.1 ')
    except OSError:
        return False


def _worker_pids(socket_path: Path | None, workers: int, deadline: float) -> set[int]:
    """Collect the pids answering `/__bench/worker-pid`.

    A worker that has not accepted yet cannot answer, so this doubles as the
    "every configured worker is actually serving" readiness gate.
    """
    seen: set[int] = set()
    while len(seen) < workers and time.monotonic() < deadline:
        try:
            if socket_path is not None:
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                    client.settimeout(2)
                    client.connect(str(socket_path))
                    client.sendall(
                        b'GET /__bench/worker-pid HTTP/1.1\r\n'
                        b'Host: localhost\r\nConnection: close\r\n\r\n'
                    )
                    body = b''
                    while chunk := client.recv(4096):
                        body += chunk
            else:
                request = urllib.request.Request(
                    f'http://{HOST}:{PORT}/__bench/worker-pid',
                    headers={'Connection': 'close'},
                )
                with urllib.request.urlopen(request, timeout=2) as response:  # noqa: S310
                    body = response.read()
        except (OSError, urllib.error.URLError):
            time.sleep(0.02)
            continue
        pid = body.rpartition(b'\r\n\r\n')[2] if b'\r\n\r\n' in body else body
        try:
            seen.add(int(pid.strip()))
        except ValueError:
            time.sleep(0.02)
    return seen


@dataclass(frozen=True, slots=True)
class ServerRun:
    command: list[str]
    pid: int
    worker_pids: tuple[int, ...]

    def memory_bytes(self) -> int:
        """Proportional set size of the supervisor and every worker.

        Not summed `VmHWM`: peak RSS counts every shared file-backed page in
        full, in each process that maps it, so N workers sharing one extension
        module report it N times. Two builds whose code merely pages in
        differently then look megabytes apart while costing the same memory.
        PSS divides each shared page by the number of processes mapping it,
        which is the number that answers "what does this deployment cost".

        Read once at the end of the measured window, so it is the steady state
        rather than a high-water mark.
        """
        total = 0
        for pid in (self.pid, *self.worker_pids):
            try:
                rollup = Path(f'/proc/{pid}/smaps_rollup').read_text()
            except OSError:
                continue
            for line in rollup.splitlines():
                if line.startswith('Pss:'):
                    total += int(line.split()[1]) * 1024
                    break
        return total


@contextmanager
def running_server(
    command: Sequence[str],
    *,
    workers: int,
    socket_path: Path | None = None,
    output: int = subprocess.DEVNULL,
) -> Iterator[ServerRun]:
    """Run a server for the duration of the block.

    Output goes to /dev/null by default: every server here logs one line per
    request, and pointing several workers at one regular file serializes them
    all on its shared file position — measuring the log sink instead of the
    server. Pass an `output` descriptor only when the sink itself is the
    subject of the measurement.
    """
    argv = list(command)
    if socket_path is not None:
        socket_path.unlink(missing_ok=True)
        socket_path.parent.mkdir(parents=True, exist_ok=True)
    print(f'Starting {" ".join(argv)}', flush=True)
    process = None
    try:
        try:
            process = subprocess.Popen(
                argv,
                stdout=output,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        except OSError as error:
            raise BenchmarkError(f'failed to start {argv[0]}: {error}') from error

        deadline = time.monotonic() + SERVER_READY_TIMEOUT_SECONDS
        while True:
            if process.poll() is not None:
                raise BenchmarkError(
                    f'{argv[0]} exited with {process.returncode} before serving'
                )
            ready = (
                _unix_ready(socket_path)
                if socket_path is not None
                else _http_ready(f'http://{HOST}:{PORT}/')
            )
            if ready:
                break
            if time.monotonic() > deadline:
                raise BenchmarkError(f'{argv[0]} did not become ready')
            time.sleep(0.05)

        pids = _worker_pids(
            socket_path, workers, time.monotonic() + SERVER_READY_TIMEOUT_SECONDS
        )
        if len(pids) != workers:
            raise BenchmarkError(
                f'{argv[0]} exposed {len(pids)}/{workers} workers: {sorted(pids)}'
            )
        time.sleep(SETTLE_SECONDS)
        yield ServerRun(command=argv, pid=process.pid, worker_pids=tuple(sorted(pids)))
    finally:
        if process is not None:
            terminate_process_group(process)
        if socket_path is not None:
            socket_path.unlink(missing_ok=True)


# --------------------------------------------------------------- load driver


def duration_seconds(value: str) -> float:
    units = {'ms': 0.001, 's': 1.0, 'm': 60.0}
    for suffix, scale in units.items():
        if value.endswith(suffix):
            return float(value[: -len(suffix)]) * scale
    return float(value)


def _run(command: list[str], timeout: float) -> str:
    # oha parses NO_COLOR as its --no-color flag and rejects an empty value.
    environment = {key: value for key, value in os.environ.items() if key != 'NO_COLOR'}
    process = None
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=environment,
            start_new_session=True,
        )
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired as error:
        if process is not None:
            terminate_process_group(process)
        raise BenchmarkError(f'load generator hung: {" ".join(command)}') from error
    except OSError as error:
        raise BenchmarkError(f'failed to run {command[0]!r}: {error}') from error
    if process.returncode != 0:
        raise BenchmarkError(f'{command[0]} failed: {stderr.strip()}')
    return stdout


def _validate_oha(raw: dict[str, Any], max_deadline_aborts: int) -> None:
    # Requests still in flight are cut when the deadline hits — that is
    # generator lifecycle, not a server error, and a connection can report
    # both its cut request and a cancelled operation. Every other error class
    # is fatal, and the status distribution must be exclusively 200.
    shutdown = {'aborted due to deadline', 'operation was canceled'}
    errors: dict[str, int] = raw.get('errorDistribution', {})
    unexpected = set(errors) - shutdown
    if unexpected:
        raise BenchmarkError(f'oha reported request errors: {errors!r}')
    if sum(errors.get(key, 0) for key in shutdown) > max_deadline_aborts:
        raise BenchmarkError(f'oha reported excessive deadline aborts: {errors!r}')
    statuses: dict[str, int] = raw.get('statusCodeDistribution', {})
    ok = statuses.get('200', 0)
    if not ok or ok != sum(statuses.values()):
        raise BenchmarkError(f'oha saw non-200 responses: {statuses!r}')


def _oha_metrics(raw: dict[str, Any]) -> Metrics:
    rps = raw.get('summary', {}).get('requestsPerSec')
    if not isinstance(rps, int | float) or not math.isfinite(rps) or rps <= 0:
        raise BenchmarkError(f'oha reported an invalid rate: {rps!r}')
    percentiles = raw.get('latencyPercentiles', {})
    return {
        'rps': float(rps),
        'latency_percentiles': {
            name: float(percentiles[name])
            for name in ('p50', 'p75', 'p90', 'p95', 'p99', 'p99.9')
            if isinstance(percentiles.get(name), int | float)
        },
    }


def run_oha(
    contract: ResponseContract,
    *,
    duration: str,
    concurrency: int,
    http2_parallelism: int = 1,
    socket_path: Path | None = None,
) -> Metrics:
    command = [
        'oha',
        '-z',
        duration,
        '-c',
        str(concurrency),
        '-p',
        str(http2_parallelism),
        '--output-format',
        'json',
        '--http-version',
        '2' if contract.protocol == '2' else '1.1',
        '-m',
        contract.method,
    ]
    if contract.request_body:
        command += [
            '-d',
            contract.request_body.decode(),
            '-T',
            'application/octet-stream',
        ]
    if socket_path is not None:
        command += ['--unix-socket', str(socket_path)]
    command.append(contract.load_url())

    stdout = _run(command, duration_seconds(duration) + LOAD_GRACE_SECONDS)
    try:
        raw = json.loads(stdout)
    except json.JSONDecodeError as error:
        raise BenchmarkError('oha returned invalid JSON') from error
    # At the deadline each in-flight request/stream is cut, and its connection
    # may also report a cancelled operation — so allow two per in-flight slot.
    # A stalled server produces orders of magnitude more than that.
    _validate_oha(raw, 2 * concurrency * http2_parallelism)
    return _oha_metrics(raw)


def _validate_k6(raw: dict[str, Any]) -> None:
    # Every handshake must succeed and every session must echo its exact nonce:
    # some passes, and not one failure. (k6 also reports the ratio, under
    # `rate` or `value` depending on the metric — pass/fail counts say it
    # unambiguously.)
    metrics: dict[str, Any] = raw.get('metrics', {})
    for name in ('checks', 'bench_echo_success'):
        values = metrics.get(name, {})
        values = values.get('values', values)
        if values.get('fails') or not values.get('passes'):
            raise BenchmarkError(
                f'k6 {name} did not pass on every iteration: {values!r}'
            )


def _k6_metrics(raw: dict[str, Any]) -> Metrics:
    metrics: dict[str, Any] = raw.get('metrics', {})
    rate = metrics.get('iterations', {}).get('rate')
    if not isinstance(rate, int | float) or not math.isfinite(rate) or rate <= 0:
        raise BenchmarkError(f'k6 reported an invalid iteration rate: {rate!r}')
    session = metrics.get('ws_session_duration', {})
    names = (('med', 'p50'), ('p(90)', 'p90'), ('p(95)', 'p95'), ('p(99)', 'p99'))
    return {
        'rps': float(rate),
        'latency_percentiles': {
            target: float(session[source]) / 1000.0
            for source, target in names
            if isinstance(session.get(source), int | float)
        },
    }


def run_k6(contract: ResponseContract, *, duration: str, concurrency: int) -> Metrics:
    summary = RESULTS_DIRECTORY / 'k6_summary.json'
    summary.parent.mkdir(parents=True, exist_ok=True)
    summary.unlink(missing_ok=True)
    command = [
        'k6',
        'run',
        '--duration',
        duration,
        '--vus',
        str(concurrency),
        '--summary-export',
        str(summary),
        '--summary-trend-stats',
        'med,p(90),p(95),p(99)',
        '-e',
        f'WS_URL={contract.load_url()}',
        str(K6_SCRIPT),
    ]
    _run(command, duration_seconds(duration) + LOAD_GRACE_SECONDS)
    try:
        raw = json.loads(summary.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise BenchmarkError('k6 wrote no usable summary') from error
    _validate_k6(raw)
    return _k6_metrics(raw)


def run_load(scenario: Scenario, *, duration: str, concurrency: int) -> Metrics:
    contract = scenario.contract
    if contract.driver == 'k6':
        return run_k6(contract, duration=duration, concurrency=concurrency)
    return run_oha(
        contract,
        duration=duration,
        concurrency=concurrency,
        http2_parallelism=scenario.http2_parallelism,
        socket_path=scenario.socket_path,
    )


# ---------------------------------------------------------- correctness gate


def _decode_chunked(body: bytes) -> bytes:
    decoded = bytearray()
    offset = 0
    while True:
        line_end = body.find(b'\r\n', offset)
        if line_end < 0:
            raise BenchmarkError('malformed chunked body')
        try:
            size = int(body[offset:line_end].partition(b';')[0], 16)
        except ValueError as error:
            raise BenchmarkError('invalid chunk size') from error
        offset = line_end + 2
        if size == 0:
            return bytes(decoded)
        end = offset + size
        if body[end : end + 2] != b'\r\n':
            raise BenchmarkError('truncated chunk')
        decoded.extend(body[offset:end])
        offset = end + 2


def _check_body(contract: ResponseContract, body: bytes, content_type: str) -> None:
    if not content_type.startswith(contract.content_type):
        raise BenchmarkError(
            f'expected content type {contract.content_type}, got {content_type!r}'
        )
    if body != contract.response_body:
        raise BenchmarkError(
            f'response body mismatch: expected {len(contract.response_body)} bytes '
            f'({hashlib.sha256(contract.response_body).hexdigest()[:16]}), got '
            f'{len(body)} bytes ({hashlib.sha256(body).hexdigest()[:16]})'
        )


def _probe_http1(contract: ResponseContract, socket_path: Path | None) -> None:
    lines = [
        f'{contract.method} {contract.path} HTTP/1.1',
        'Host: localhost',
        'Connection: close',
    ]
    if contract.request_body:
        lines += [
            f'Content-Length: {len(contract.request_body)}',
            'Content-Type: application/octet-stream',
        ]
    request = ('\r\n'.join(lines) + '\r\n\r\n').encode() + contract.request_body

    family = socket.AF_UNIX if socket_path is not None else socket.AF_INET
    address: str | tuple[str, int] = (
        str(socket_path) if socket_path is not None else (HOST, PORT)
    )
    with socket.socket(family, socket.SOCK_STREAM) as connection:
        connection.settimeout(10)
        connection.connect(address)
        connection.sendall(request)
        chunks = []
        while chunk := connection.recv(64 * 1024):
            chunks.append(chunk)

    head, separator, body = b''.join(chunks).partition(b'\r\n\r\n')
    if not separator:
        raise BenchmarkError('malformed HTTP response')
    status_line, *header_lines = head.split(b'\r\n')
    if status_line.split(b' ')[1:2] != [b'200']:
        raise BenchmarkError(f'expected HTTP 200, got {status_line!r}')
    headers = {
        name.decode().lower(): value.decode().strip()
        for line in header_lines
        for name, separator, value in [line.partition(b':')]
        if separator
    }
    if headers.get('transfer-encoding', '').lower() == 'chunked':
        body = _decode_chunked(body)
    _check_body(contract, body, headers.get('content-type', ''))


def _text(value: str | bytes) -> str:
    return value.decode() if isinstance(value, bytes) else value


def _probe_http2(contract: ResponseContract) -> None:
    connection = h2.connection.H2Connection(
        h2.config.H2Configuration(header_encoding='utf-8')
    )
    connection.initiate_connection()
    stream_id = connection.get_next_available_stream_id()
    headers = [
        (':method', contract.method),
        (':scheme', 'http'),
        (':authority', f'{HOST}:{PORT}'),
        (':path', contract.path),
    ]
    if contract.request_body:
        headers += [
            ('content-length', str(len(contract.request_body))),
            ('content-type', 'application/octet-stream'),
        ]
    connection.send_headers(stream_id, headers, end_stream=not contract.request_body)
    if contract.request_body:
        connection.send_data(stream_id, contract.request_body, end_stream=True)

    status = None
    content_type = ''
    body = bytearray()
    ended = False
    with socket.create_connection((HOST, PORT), timeout=10) as transport:
        transport.sendall(connection.data_to_send())
        while not ended:
            data = transport.recv(64 * 1024)
            if not data:
                raise BenchmarkError('HTTP/2 response ended early')
            for event in connection.receive_data(data):
                if getattr(event, 'stream_id', None) != stream_id:
                    continue
                if isinstance(event, h2.events.ResponseReceived):
                    # header_encoding='utf-8' decodes for us, but the stubs
                    # still describe the raw bytes form.
                    response = {
                        _text(name): _text(value) for name, value in event.headers
                    }
                    status = response.get(':status')
                    content_type = response.get('content-type', '')
                elif isinstance(event, h2.events.DataReceived):
                    body.extend(event.data)
                    connection.acknowledge_received_data(
                        event.flow_controlled_length, stream_id
                    )
                elif isinstance(event, h2.events.StreamEnded):
                    ended = True
            if outbound := connection.data_to_send():
                transport.sendall(outbound)

    if status != '200':
        raise BenchmarkError(f'expected HTTP 200, got {status!r}')
    _check_body(contract, bytes(body), content_type)


def _receive_at_least(connection: socket.socket, buffer: bytearray, size: int) -> None:
    while len(buffer) < size:
        chunk = connection.recv(size - len(buffer))
        if not chunk:
            raise BenchmarkError(f'WebSocket frame ended at {len(buffer)}/{size} bytes')
        buffer.extend(chunk)


def _probe_websocket(contract: ResponseContract) -> None:
    key = base64.b64encode(secrets.token_bytes(16)).decode()
    accept = base64.b64encode(
        hashlib.sha1(  # noqa: S324 - the value RFC 6455 specifies
            (key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()
        ).digest()
    ).decode()
    request = (
        f'GET {contract.path} HTTP/1.1\r\n'
        f'Host: {HOST}:{PORT}\r\n'
        'Upgrade: websocket\r\n'
        'Connection: Upgrade\r\n'
        f'Sec-WebSocket-Key: {key}\r\n'
        'Sec-WebSocket-Version: 13\r\n\r\n'
    ).encode()

    with socket.create_connection((HOST, PORT), timeout=10) as connection:
        connection.sendall(request)
        handshake = bytearray()
        while b'\r\n\r\n' not in handshake:
            chunk = connection.recv(4096)
            if not chunk:
                raise BenchmarkError('WebSocket handshake ended early')
            handshake.extend(chunk)
        head, _, remainder = bytes(handshake).partition(b'\r\n\r\n')
        if not head.startswith(b'HTTP/1.1 101 '):
            raise BenchmarkError('WebSocket handshake was not accepted')
        headers = {
            name.decode().lower(): value.decode().strip()
            for line in head.split(b'\r\n')[1:]
            for name, separator, value in [line.partition(b':')]
            if separator
        }
        if headers.get('sec-websocket-accept') != accept:
            raise BenchmarkError('WebSocket handshake returned a wrong accept key')

        payload = contract.response_body
        mask = secrets.token_bytes(4)
        masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        connection.sendall(bytes([0x81, 0x80 | len(payload)]) + mask + masked)

        frame = bytearray(remainder)
        _receive_at_least(connection, frame, 2)
        first, second = frame[0], frame[1]
        offset, length = 2, second & 0x7F
        if length == 126:
            _receive_at_least(connection, frame, offset + 2)
            length = struct.unpack('!H', frame[offset : offset + 2])[0]
            offset += 2
        elif length == 127:
            _receive_at_least(connection, frame, offset + 8)
            length = struct.unpack('!Q', frame[offset : offset + 8])[0]
            offset += 8
        _receive_at_least(connection, frame, offset + length)
        echoed = bytes(frame[offset : offset + length])
        if first != 0x81 or second & 0x80 or echoed != payload:
            raise BenchmarkError('WebSocket did not echo the exact text frame')


def check_response(scenario: Scenario) -> None:
    """Prove the server answers this scenario exactly, or refuse to measure it."""
    contract = scenario.contract
    try:
        if contract.protocol == 'websocket':
            _probe_websocket(contract)
        elif contract.protocol == '2':
            _probe_http2(contract)
        else:
            _probe_http1(contract, scenario.socket_path)
    except OSError as error:
        raise BenchmarkError(
            f'{scenario.type} correctness probe transport failed: {error}'
        ) from error
