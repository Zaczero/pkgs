"""Shared benchmark plumbing: scenarios, server processes, load drivers, statistics.

Nothing is pinned to a CPU. Server and load generator run the way a deployment
runs them, and the noise of a shared development host is handled where it
belongs — in the statistics: trials are rotation-balanced and rate-ratio
intervals resample paired blocks. Sampling uses the configured fixed trial
count, bounded by a hard time ceiling.
"""

from __future__ import annotations

import base64
import hashlib
import http.client
import json
import math
import os
import random
import secrets
import signal
import socket
import statistics
import struct
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, TypedDict

import h2.config
import h2.connection
import h2.events

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Sequence
    from ssl import SSLContext

HOST = '127.0.0.1'
PORT = 8000
RESULTS_DIRECTORY = Path('bench/results')
UNIX_SOCKET_PATH = RESULTS_DIRECTORY / 'benchmark.sock'
STATIC_FILE_PATH = Path('bench/_file_response_payload.bin')
STATIC_FILE_BODY = b'\x00' * (128 * 1024)
DOWNLOAD_BODY = b'x' * (128 * 1024)
LARGE_UPLOAD_PATH = Path('bench/_large_upload_payload.bin')
# Exceeds the 6.25 MiB BDP of the shaped 1 Gbit, 50 ms path and matches
# h2corn's advertised HTTP/2 receive window.
LARGE_UPLOAD_SIZE = 8 * 1024 * 1024
K6_SCRIPT = Path('bench/k6/ws.js')

#: Load generators are given the deadline plus this much slack before the run
#: is declared hung.
LOAD_GRACE_SECONDS = 30.0
SERVER_READY_TIMEOUT_SECONDS = 15.0
#: Time between server start and the first request of a trial.
SETTLE_SECONDS = 0.25
#: ``smaps_rollup`` walks a process's mappings. One snapshot per second keeps
#: that work far below the request path while still making a ten-second trial
#: a high-water sample rather than a steady-state reading.
PSS_SAMPLE_INTERVAL_SECONDS = 1.0

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
    'h2_upload',
    'h1_upload_fast',
    'h2_upload_fast',
    'h1_response_stream',
    'h2_response_stream',
    'h1_response_content_length',
    'h2_response_content_length',
    'ws',
]


class BenchmarkError(RuntimeError):
    """A benchmark could not be measured, so no number may be reported."""


class ResponseError(BenchmarkError):
    """A serving process failed the workload's response contract."""


@contextmanager
def deadline_bound(deadline: float) -> Generator[None, None, None]:
    """Interrupt a trial at its scenario deadline, then run bounded cleanup."""

    def expired(_signum: int, _frame: Any) -> None:
        raise BenchmarkError('scenario deadline expired')

    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise BenchmarkError('scenario deadline expired')
    previous = signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, remaining)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


class Metrics(TypedDict):
    rps: float
    latency_percentiles: dict[str, float]
    #: Completed responses in the trial. Kept because it is what makes `rps`
    #: interpretable: a server that answers once per connection and then stalls
    #: still reports a rate, and only the count against the concurrency shows
    #: that it never reused a connection.
    completed: int


class MeasuredMetrics(Metrics):
    """Load metrics together with the deployment's sampled peak PSS."""

    peak_memory_bytes: int


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
    request_body_path: Path | None = None
    response_body: bytes = b''
    content_type: str = 'text/plain'
    response_content_length: int | None = None

    def load_url(self, port: int = PORT) -> str:
        if self.driver == 'k6':
            return f'ws://{HOST}:{port}{self.path}'
        if self.unix_socket:
            return f'http://localhost{self.path}'
        return f'http://{HOST}:{port}{self.path}'


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
    # ``request_body`` is the compact exact-body probe. The load generator
    # reads the 8 MiB body from disk so it does not put an enormous argv or a
    # persistent 8 MiB Python object into the server-memory measurement.
    'h2_upload': ResponseContract(
        '/streaming-post-fast',
        protocol='2',
        method='POST',
        request_body=b'x' * 1024,
        request_body_path=LARGE_UPLOAD_PATH,
        response_body=b'1024',
    ),
    'h1_upload_fast': ResponseContract(
        '/streaming-post-fast',
        method='POST',
        request_body=b'x' * 1024,
        response_body=b'1024',
    ),
    'h2_upload_fast': ResponseContract(
        '/streaming-post-fast',
        protocol='2',
        method='POST',
        request_body=b'x' * 1024,
        response_body=b'1024',
    ),
    'h1_response_stream': ResponseContract(
        '/response-stream',
        response_body=b'x' * 1024,
        content_type='application/octet-stream',
    ),
    'h2_response_stream': ResponseContract(
        '/response-stream',
        protocol='2',
        response_body=b'x' * 1024,
        content_type='application/octet-stream',
    ),
    'h1_response_content_length': ResponseContract(
        '/response-content-length',
        response_body=b'x' * 1024,
        content_type='application/octet-stream',
        response_content_length=1024,
    ),
    'h2_response_content_length': ResponseContract(
        '/response-content-length',
        protocol='2',
        response_body=b'x' * 1024,
        content_type='application/octet-stream',
        response_content_length=1024,
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
    port: int = PORT
    unix_socket: Path | None = None

    @property
    def contract(self) -> ResponseContract:
        return RESPONSE_CONTRACTS[self.type]

    @property
    def socket_path(self) -> Path | None:
        return (
            self.unix_socket or UNIX_SOCKET_PATH if self.contract.unix_socket else None
        )

    def slug(self) -> str:
        cleaned = (
            self.name
            .lower()
            .replace('/', '_')
            .replace('(', '')
            .replace(')', '')
            .replace(',', '')
            .replace('-', '_')
            .replace(' ', '_')
        )
        return f'benchmark_{cleaned}'


@contextmanager
def allocated_listener(
    scenario: Scenario,
) -> Generator[tuple[Scenario, socket.socket], None, None]:
    """Keep the OS-allocated listener owned until its server has stopped."""
    with tempfile.TemporaryDirectory(prefix='h2c-bench-') as directory:
        family = socket.AF_UNIX if scenario.contract.unix_socket else socket.AF_INET
        protocol = socket.IPPROTO_TCP if family == socket.AF_INET else 0
        with socket.socket(family, socket.SOCK_STREAM, protocol) as listener:
            path = Path(directory) / 'server.sock'
            if family == socket.AF_INET:
                listener.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            listener.bind(str(path) if family == socket.AF_UNIX else (HOST, 0))
            listener.listen(2048)
            listener.setblocking(False)
            yield (
                replace(scenario, unix_socket=path)
                if family == socket.AF_UNIX
                else replace(scenario, port=listener.getsockname()[1]),
                listener,
            )


def ensure_static_file_payload() -> None:
    if STATIC_FILE_PATH.exists() and STATIC_FILE_PATH.stat().st_size == len(
        STATIC_FILE_BODY
    ):
        return
    STATIC_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATIC_FILE_PATH.write_bytes(STATIC_FILE_BODY)


def ensure_large_upload_payload() -> None:
    if (
        LARGE_UPLOAD_PATH.exists()
        and LARGE_UPLOAD_PATH.stat().st_size == LARGE_UPLOAD_SIZE
    ):
        return
    LARGE_UPLOAD_PATH.parent.mkdir(parents=True, exist_ok=True)
    LARGE_UPLOAD_PATH.write_bytes(b'x' * LARGE_UPLOAD_SIZE)


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
        with urllib.request.urlopen(url, timeout=1) as response:  # ruff: ignore[suspicious-url-open-usage]
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
                with urllib.request.urlopen(request, timeout=2) as response:  # ruff: ignore[suspicious-url-open-usage]
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
    include_descendants: bool = False

    def process_pids(self) -> set[int]:
        pids = {self.pid, *self.worker_pids}
        if self.include_descendants:
            pending = list(pids)
            while pending:
                pid = pending.pop()
                for task in Path(f'/proc/{pid}/task').glob('*'):
                    try:
                        children = (task / 'children').read_text().split()
                    except FileNotFoundError:
                        continue
                    for child in map(int, children):
                        if child not in pids:
                            pids.add(child)
                            pending.append(child)
        return pids

    def memory_bytes(self) -> int:
        """Return a current PSS snapshot of the supervisor and every worker.

        Not summed `VmHWM`: peak RSS counts every shared file-backed page in
        full, in each process that maps it, so N workers sharing one extension
        module report it N times. Two builds whose code merely pages in
        differently then look megabytes apart while costing the same memory.
        PSS divides each shared page by the number of processes mapping it,
        which is the number that answers "what does this deployment cost".

        """
        total = 0
        for pid in sorted(self.process_pids()):
            try:
                rollup = Path(f'/proc/{pid}/smaps_rollup').read_text()
            except OSError as error:
                raise BenchmarkError(
                    f'PSS unavailable for required process {pid}'
                ) from error
            for line in rollup.splitlines():
                if line.startswith('Pss:'):
                    total += int(line.split()[1]) * 1024
                    break
            else:
                raise BenchmarkError(f'PSS missing for required process {pid}')
        return total


def measure_peak_memory(
    server: ServerRun,
    action: Callable[[], Metrics],
    *,
    evidence_path: Path | None = None,
) -> tuple[Metrics, int]:
    """Run one measured action and return its maximum periodically sampled PSS.

    ``smaps_rollup`` is deliberately read only once per second: a PSS snapshot
    requires walking every mapping of the supervisor and its workers, so a
    higher cadence would make the observer part of a short benchmark's load.
    A normal ten-second trial still receives an initial sample, about nine
    periodic samples, and a final sample.
    """
    started = time.monotonic()
    samples = [
        {
            'elapsed_seconds': 0.0,
            'pss_bytes': server.memory_bytes(),
            'unique_pids': sorted(server.process_pids()),
        }
    ]
    failures: list[BenchmarkError] = []
    stop = threading.Event()

    def sample() -> None:
        while not stop.wait(PSS_SAMPLE_INTERVAL_SECONDS):
            try:
                samples.append({
                    'elapsed_seconds': time.monotonic() - started,
                    'pss_bytes': server.memory_bytes(),
                    'unique_pids': sorted(server.process_pids()),
                })
            except BenchmarkError as error:
                failures.append(error)
                return

    sampler = threading.Thread(target=sample, name='h2corn-bench-pss', daemon=True)
    sampler.start()
    try:
        metrics = action()
    finally:
        stop.set()
        sampler.join()
        samples.append({
            'elapsed_seconds': time.monotonic() - started,
            'pss_bytes': server.memory_bytes(),
            'unique_pids': sorted(server.process_pids()),
        })
        if evidence_path is not None:
            write_json(
                evidence_path,
                {
                    'unique_pids': sorted(server.process_pids()),
                    'sample_interval_seconds': PSS_SAMPLE_INTERVAL_SECONDS,
                    'samples': samples,
                    'errors': [str(error) for error in failures],
                },
            )
    if failures:
        raise failures[0]
    return metrics, max(int(sample['pss_bytes']) for sample in samples)


@contextmanager
def running_server(
    command: Sequence[str],
    *,
    workers: int,
    socket_path: Path | None = None,
    output: int = subprocess.DEVNULL,
    scenario: Scenario | None = None,
    listener_fd: int | None = None,
    identity_directory: Path | None = None,
    readiness: Callable[[int], bool] | None = None,
    include_descendants: bool = False,
) -> Generator[ServerRun, None, None]:
    """Run a server for the duration of the block.

    Output goes to /dev/null by default: every server here logs one line per
    request, and pointing several workers at one regular file serializes them
    all on its shared file position — measuring the log sink instead of the
    server. Pass an `output` descriptor only when the sink itself is the
    subject of the measurement.
    """
    argv = list(command)
    if socket_path is not None and listener_fd is None:
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
                pass_fds=() if listener_fd is None else (listener_fd,),
                cwd=Path(__file__).resolve().parents[1],
                env={
                    **os.environ,
                    **(
                        {'ASGI_BENCH_WORKER_RECEIPTS': str(identity_directory)}
                        if identity_directory is not None
                        else {}
                    ),
                },
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
                readiness(process.pid)
                if readiness is not None
                else _protocol_ready(scenario)
                if scenario is not None
                else _unix_ready(socket_path)
                if socket_path is not None
                else _http_ready(f'http://{HOST}:{PORT}/')
            )
            if ready:
                break
            if time.monotonic() > deadline:
                raise BenchmarkError(f'{argv[0]} did not become ready')
            time.sleep(0.05)

        pids = (
            {process.pid}
            if readiness is not None
            else _protocol_worker_pids(scenario, workers)
            if scenario is not None
            else _worker_pids(
                socket_path, workers, time.monotonic() + SERVER_READY_TIMEOUT_SECONDS
            )
        )
        if len(pids) != workers:
            raise BenchmarkError(
                f'{argv[0]} exposed {len(pids)}/{workers} workers: {sorted(pids)}'
            )
        time.sleep(SETTLE_SECONDS)
        yield ServerRun(
            command=argv,
            pid=process.pid,
            worker_pids=tuple(sorted(pids)),
            include_descendants=include_descendants,
        )
    finally:
        if process is not None:
            terminate_process_group(process)
        if socket_path is not None and listener_fd is None:
            socket_path.unlink(missing_ok=True)


# --------------------------------------------------------------- load driver


def duration_seconds(value: str) -> float:
    """Parse a duration the load generator will also accept.

    A unit is required because `oha` requires one. Accepting a bare number here
    used to defer the rejection to load-generation time, after the servers for
    that cell had already been started -- so the run failed several seconds in,
    with `oha`'s error rather than one naming the option.
    """
    units = {'ms': 0.001, 's': 1.0, 'm': 60.0}
    for suffix, scale in units.items():
        if value.endswith(suffix):
            return float(value[: -len(suffix)]) * scale
    raise ValueError(
        f'duration {value!r} needs a unit: one of '
        + ', '.join(f'{unit!r}' for unit in units)
    )


def _run(
    command: list[str], timeout: float, *, evidence_path: Path | None = None
) -> str:
    # oha parses NO_COLOR as its --no-color flag and rejects an empty value.
    environment = {key: value for key, value in os.environ.items() if key != 'NO_COLOR'}
    process = None
    started = time.monotonic()
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
            stdout, stderr = process.communicate()
            if evidence_path is not None:
                write_json(
                    evidence_path,
                    {
                        'command': command,
                        'stdout': stdout,
                        'stderr': stderr,
                        'elapsed_seconds': time.monotonic() - started,
                        'timeout_seconds': timeout,
                        'returncode': process.returncode,
                        'status': 'timeout',
                    },
                )
        raise BenchmarkError(f'load generator hung: {" ".join(command)}') from error
    except OSError as error:
        raise BenchmarkError(f'failed to run {command[0]!r}: {error}') from error
    except BaseException:
        if process is not None:
            terminate_process_group(process)
            stdout, stderr = process.communicate()
            if evidence_path is not None:
                write_json(
                    evidence_path,
                    {
                        'command': command,
                        'stdout': stdout,
                        'stderr': stderr,
                        'elapsed_seconds': time.monotonic() - started,
                        'returncode': process.returncode,
                        'status': 'interrupted',
                    },
                )
        raise
    if evidence_path is not None:
        write_json(
            evidence_path,
            {
                'command': command,
                'stdout': stdout,
                'stderr': stderr,
                'elapsed_seconds': time.monotonic() - started,
                'timeout_seconds': timeout,
                'returncode': process.returncode,
            },
        )
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
        raise ResponseError(f'oha reported request errors: {errors!r}')
    if sum(errors.get(key, 0) for key in shutdown) > max_deadline_aborts:
        raise ResponseError(f'oha reported excessive deadline aborts: {errors!r}')
    statuses: dict[str, int] = raw.get('statusCodeDistribution', {})
    ok = statuses.get('200', 0)
    if not ok or ok != sum(statuses.values()):
        raise ResponseError(f'oha saw non-200 responses: {statuses!r}')


def _oha_metrics(raw: dict[str, Any]) -> Metrics:
    elapsed = raw.get('summary', {}).get('total')
    if (
        not isinstance(elapsed, int | float)
        or not math.isfinite(elapsed)
        or elapsed <= 0
    ):
        raise BenchmarkError(f'oha reported an invalid duration: {elapsed!r}')
    percentiles = raw.get('latencyPercentiles', {})
    statuses: dict[str, int] = raw.get('statusCodeDistribution', {})
    return {
        'rps': sum(statuses.values()) / elapsed,
        'completed': sum(statuses.values()),
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
    port: int = PORT,
    evidence_path: Path | None = None,
    requests: int | None = None,
) -> Metrics:
    command = [
        'oha',
        '-z' if requests is None else '-n',
        duration if requests is None else str(requests),
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
    if contract.request_body_path is not None:
        command += [
            '-D',
            str(contract.request_body_path),
            '-T',
            'application/octet-stream',
        ]
    elif contract.request_body:
        command += [
            '-d',
            contract.request_body.decode(),
            '-T',
            'application/octet-stream',
        ]
    if socket_path is not None:
        command += ['--unix-socket', str(socket_path)]
    command.append(contract.load_url(port))

    stdout = _run(
        command,
        duration_seconds(duration) + LOAD_GRACE_SECONDS if requests is None else 120,
        evidence_path=evidence_path.with_suffix('.process.json')
        if evidence_path
        else None,
    )
    try:
        raw = json.loads(stdout)
    except json.JSONDecodeError as error:
        raise BenchmarkError('oha returned invalid JSON') from error
    if evidence_path is not None:
        write_json(evidence_path, raw)
    # At the deadline each in-flight request/stream is cut, and its connection
    # may also report a cancelled operation — so allow two per in-flight slot.
    # A stalled server produces orders of magnitude more than that.
    _validate_oha(raw, 2 * concurrency * http2_parallelism)
    expected = (
        str(contract.request_body_path.stat().st_size).encode()
        if contract.request_body_path is not None
        else contract.response_body
    )
    completed = sum(raw['statusCodeDistribution'].values())
    if requests is not None and completed != requests:
        raise ResponseError(f'oha completed {completed}/{requests} required requests')
    if raw.get('summary', {}).get('totalData') != completed * len(expected):
        raise ResponseError(
            'oha response-byte total differs from completed response bodies'
        )
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
            raise ResponseError(
                f'k6 {name} did not pass on every iteration: {values!r}'
            )


def _k6_metrics(raw: dict[str, Any]) -> Metrics:
    metrics: dict[str, Any] = raw.get('metrics', {})
    rate = metrics.get('iterations', {}).get('rate')
    if not isinstance(rate, int | float) or not math.isfinite(rate) or rate <= 0:
        raise BenchmarkError(f'k6 reported an invalid iteration rate: {rate!r}')
    session = metrics.get('ws_session_duration', {})
    names = (('med', 'p50'), ('p(90)', 'p90'), ('p(95)', 'p95'), ('p(99)', 'p99'))
    completed = metrics.get('iterations', {}).get('count', 0)
    return {
        'rps': float(rate),
        'completed': int(completed) if isinstance(completed, int | float) else 0,
        'latency_percentiles': {
            target: float(session[source]) / 1000.0
            for source, target in names
            if isinstance(session.get(source), int | float)
        },
    }


def run_k6(
    contract: ResponseContract,
    *,
    duration: str,
    concurrency: int,
    port: int = PORT,
    evidence_path: Path | None = None,
) -> Metrics:
    with tempfile.TemporaryDirectory(prefix='h2c-k6-') as directory:
        return _run_k6(
            contract,
            duration=duration,
            concurrency=concurrency,
            port=port,
            evidence_path=evidence_path,
            summary=Path(directory) / 'summary.json',
        )


def _run_k6(
    contract: ResponseContract,
    *,
    duration: str,
    concurrency: int,
    port: int,
    evidence_path: Path | None,
    summary: Path,
) -> Metrics:
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
        f'WS_URL={contract.load_url(port)}',
        str(K6_SCRIPT),
    ]
    _run(
        command,
        duration_seconds(duration) + LOAD_GRACE_SECONDS,
        evidence_path=evidence_path.with_suffix('.process.json')
        if evidence_path
        else None,
    )
    try:
        raw = json.loads(summary.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise BenchmarkError('k6 wrote no usable summary') from error
    if evidence_path is not None:
        write_json(evidence_path, raw)
    _validate_k6(raw)
    return _k6_metrics(raw)


def run_load(
    scenario: Scenario,
    *,
    duration: str,
    concurrency: int,
    evidence_path: Path | None = None,
    requests: int | None = None,
) -> Metrics:
    contract = scenario.contract
    if contract.driver == 'k6':
        return run_k6(
            contract,
            duration=duration,
            concurrency=concurrency,
            port=scenario.port,
            evidence_path=evidence_path,
        )
    return run_oha(
        contract,
        duration=duration,
        concurrency=concurrency,
        http2_parallelism=scenario.http2_parallelism,
        socket_path=scenario.socket_path,
        port=scenario.port,
        evidence_path=evidence_path,
        requests=requests,
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
            f'{len(body)} bytes ({hashlib.sha256(body).hexdigest()[:16]}); '
            f'expected prefix {contract.response_body[:32]!r}, got {body[:32]!r}'
        )


def _check_content_length(contract: ResponseContract, headers: dict[str, str]) -> None:
    expected = contract.response_content_length
    if expected is not None and headers.get('content-length') != str(expected):
        raise BenchmarkError(
            f'expected response content-length {expected}, '
            f'got {headers.get("content-length")!r}'
        )


def _http1_response(
    contract: ResponseContract,
    socket_path: Path | None,
    port: int,
    *,
    timeout: float = 10,
) -> tuple[dict[str, str], bytes]:
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
        str(socket_path) if socket_path is not None else (HOST, port)
    )
    with socket.socket(family, socket.SOCK_STREAM) as connection:
        deadline = time.monotonic() + timeout
        connection.settimeout(timeout)
        connection.connect(address)
        connection.sendall(request)
        chunks = []
        while chunk := connection.recv(64 * 1024):
            chunks.append(chunk)
            connection.settimeout(max(0.001, deadline - time.monotonic()))
            if time.monotonic() >= deadline:
                raise BenchmarkError('HTTP/1 probe exceeded its deadline')

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
    return headers, body


def _probe_http1(
    contract: ResponseContract,
    socket_path: Path | None,
    port: int = PORT,
) -> None:
    connection = http.client.HTTPConnection(HOST, port, timeout=10)
    try:
        if socket_path is not None:
            connection.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            connection.sock.settimeout(10)
            connection.sock.connect(str(socket_path))
        for request_number in (1, 2):
            try:
                connection.request(
                    contract.method,
                    contract.path,
                    body=contract.request_body or None,
                    headers={
                        'Connection': 'keep-alive',
                        **(
                            {'Content-Type': 'application/octet-stream'}
                            if contract.request_body
                            else {}
                        ),
                    },
                )
                with connection.getresponse() as response:
                    if response.status != 200:
                        raise BenchmarkError(
                            f'expected HTTP 200, got {response.status}'
                        )
                    headers = {
                        name.lower(): value for name, value in response.getheaders()
                    }
                    _check_content_length(contract, headers)
                    body = response.read(len(contract.response_body) + 1)
                    _check_body(contract, body, headers.get('content-type', ''))
                    if response.will_close:
                        break
            except (OSError, http.client.HTTPException) as error:
                raise BenchmarkError(
                    f'HTTP/1 persistent request {request_number} failed: {error}'
                ) from error
    finally:
        connection.close()


def _text(value: str | bytes) -> str:
    return value.decode() if isinstance(value, bytes) else value


def _http2_response(
    contract: ResponseContract,
    port: int,
    *,
    timeout: float = 10,
) -> tuple[dict[str, str], bytes]:
    connection = h2.connection.H2Connection(
        h2.config.H2Configuration(header_encoding='utf-8')
    )
    connection.initiate_connection()
    stream_id = connection.get_next_available_stream_id()
    headers = [
        (':method', contract.method),
        (':scheme', 'http'),
        (':authority', f'{HOST}:{port}'),
        (':path', contract.path),
    ]
    if contract.request_body:
        headers += [
            ('content-length', str(len(contract.request_body))),
            ('content-type', 'application/octet-stream'),
        ]
    connection.send_headers(stream_id, headers, end_stream=not contract.request_body)
    status = None
    response: dict[str, str] = {}
    body = bytearray()
    ended = False
    sent = 0
    deadline = time.monotonic() + timeout
    with socket.create_connection((HOST, port), timeout=timeout) as transport:
        while not ended:
            while sent < len(contract.request_body):
                length = min(
                    connection.local_flow_control_window(stream_id),
                    connection.max_outbound_frame_size,
                    len(contract.request_body) - sent,
                )
                if not length:
                    break
                connection.send_data(
                    stream_id,
                    contract.request_body[sent : sent + length],
                    end_stream=sent + length == len(contract.request_body),
                )
                sent += length
            if outbound := connection.data_to_send():
                transport.sendall(outbound)
            if time.monotonic() >= deadline:
                raise BenchmarkError('HTTP/2 probe exceeded its deadline')
            transport.settimeout(max(0.001, deadline - time.monotonic()))
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
                elif isinstance(event, h2.events.DataReceived):
                    body.extend(event.data)
                    connection.acknowledge_received_data(
                        event.flow_controlled_length, stream_id
                    )
                elif isinstance(event, h2.events.StreamEnded):
                    ended = True
                elif isinstance(event, h2.events.StreamReset):
                    raise BenchmarkError(f'HTTP/2 probe reset: {event.error_code}')

    if status != '200':
        raise BenchmarkError(f'expected HTTP 200, got {status!r}')
    return response, bytes(body)


def _probe_http2(
    contract: ResponseContract,
    port: int = PORT,
    *,
    timeout: float = 10,
) -> None:
    response, body = _http2_response(contract, port, timeout=timeout)
    _check_content_length(contract, response)
    _check_body(contract, body, response.get('content-type', ''))


def request_response(
    scenario: Scenario,
    path: str,
    *,
    timeout: float = 1,
) -> bytes:
    contract = ResponseContract(path)
    _, body = (
        _http2_response(contract, scenario.port, timeout=timeout)
        if scenario.contract.protocol == '2'
        else _http1_response(
            contract, scenario.socket_path, scenario.port, timeout=timeout
        )
    )
    return body


def peer_settings(
    scenario: Scenario, *, tls_context: SSLContext | None = None
) -> dict[str, Any]:
    """Read effective HTTP/2 settings after a round-trip ordering barrier."""
    connection = h2.connection.H2Connection()
    connection.initiate_connection()
    connection.ping(b'bench-h2')
    changes = []
    deadline = time.monotonic() + 5
    with ExitStack() as owners:
        transport = owners.enter_context(
            socket.create_connection((HOST, scenario.port), timeout=5)
        )
        if tls_context is not None:
            transport = owners.enter_context(
                tls_context.wrap_socket(transport, server_hostname='localhost')
            )
        transport.sendall(connection.data_to_send())
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise BenchmarkError('HTTP/2 settings probe exceeded five seconds')
            transport.settimeout(remaining)
            data = transport.recv(65536)
            if not data:
                raise BenchmarkError(
                    'HTTP/2 settings probe ended before PING acknowledgment'
                )
            for event in connection.receive_data(data):
                if isinstance(event, h2.events.RemoteSettingsChanged):
                    changes.append({
                        getattr(key, 'name', str(key)): value.new_value
                        for key, value in event.changed_settings.items()
                    })
                elif isinstance(event, h2.events.PingAckReceived):
                    return {
                        'effective_settings': {
                            getattr(key, 'name', str(key)): value
                            for key, value in connection.remote_settings.items()
                        },
                        'connection_send_credit': connection.outbound_flow_control_window,
                        'settings_frames': changes,
                    }
            if outbound := connection.data_to_send():
                transport.sendall(outbound)


def _protocol_ready(scenario: Scenario) -> bool:
    try:
        return request_response(scenario, '/') == b'Hello, World!'
    except (OSError, BenchmarkError):
        return False


def _protocol_worker_pids(scenario: Scenario, workers: int) -> set[int]:
    deadline = time.monotonic() + SERVER_READY_TIMEOUT_SECONDS
    seen: set[int] = set()
    while len(seen) < workers and time.monotonic() < deadline:
        try:
            seen.add(int(request_response(scenario, '/__bench/worker-pid').strip()))
        except (OSError, ValueError, BenchmarkError):
            time.sleep(0.02)
    return seen


def _receive_at_least(connection: socket.socket, buffer: bytearray, size: int) -> None:
    while len(buffer) < size:
        chunk = connection.recv(size - len(buffer))
        if not chunk:
            raise BenchmarkError(f'WebSocket frame ended at {len(buffer)}/{size} bytes')
        buffer.extend(chunk)


def _probe_websocket(contract: ResponseContract, port: int = PORT) -> None:
    key = base64.b64encode(secrets.token_bytes(16)).decode()
    accept = base64.b64encode(
        hashlib.sha1(  # ruff: ignore[hashlib-insecure-hash-function] - the value RFC 6455 specifies
            (key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()
        ).digest()
    ).decode()
    request = (
        f'GET {contract.path} HTTP/1.1\r\n'
        f'Host: {HOST}:{port}\r\n'
        'Upgrade: websocket\r\n'
        'Connection: Upgrade\r\n'
        f'Sec-WebSocket-Key: {key}\r\n'
        'Sec-WebSocket-Version: 13\r\n\r\n'
    ).encode()

    with socket.create_connection((HOST, port), timeout=10) as connection:
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
    if contract.request_body_path is not None:
        payload = contract.request_body_path.read_bytes()
        contract = replace(
            contract,
            request_body=payload,
            response_body=str(len(payload)).encode(),
        )
    try:
        if contract.protocol == 'websocket':
            _probe_websocket(contract, scenario.port)
        elif contract.protocol == '2':
            _probe_http2(
                contract,
                scenario.port,
                timeout=60 if contract.request_body_path is not None else 10,
            )
        else:
            _probe_http1(contract, scenario.socket_path, scenario.port)
    except (OSError, BenchmarkError) as error:
        raise ResponseError(
            f'{scenario.type} correctness probe transport failed: {error}'
        ) from error
