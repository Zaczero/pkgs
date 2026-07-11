"""Reproducible paired A/B benchmark harness for h2corn server builds.

Each block starts both named server variants in a seed-controlled AB/BA order.
Warmup blocks are retained in the evidence record but excluded from statistics.
Measured samples are compared within their block so slow ambient drift is less
likely to be mistaken for a code change.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import random
import re
import shlex
import shutil
import signal
import socket
import ssl
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SEED = 20_260_710
DEFAULT_TRIALS = 9
DEFAULT_BOOTSTRAP_SAMPLES = 10_000
MAX_DURATION_SECONDS = 3_600.0
NAME_PATTERN = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.-]*$')
DURATION_PATTERN = re.compile(r'^(\d+(?:\.\d+)?)(ms|s|m|h)$')


class BenchmarkError(RuntimeError):
    """A benchmark subprocess failed or produced invalid evidence."""


@dataclass(frozen=True, slots=True)
class NamedCommand:
    name: str
    argv: tuple[str, ...]


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
    cpus: set[int] = set()
    try:
        for part in value.split(','):
            if not part:
                raise ValueError
            endpoints = part.split('-', 1)
            start = int(endpoints[0])
            stop = int(endpoints[-1])
            if start < 0 or stop < start:
                raise ValueError
            cpus.update(range(start, stop + 1))
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            'CPU sets use non-negative IDs and inclusive ranges, for example 2,4-6'
        ) from error
    return tuple(sorted(cpus))


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
) -> dict[str, Any]:
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
    noise_floor = percentile(deltas, 0.75) - percentile(deltas, 0.25)
    significant = (ci_low > 0.0 or ci_high < 0.0) and abs(delta) > noise_floor
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
        'empirical_noise_floor_percent': noise_floor,
        'higher_is_better': higher_is_better,
        'improvement_median_percent': improvement,
        'significant': significant,
    }


class PeakRssMonitor:
    """Sample aggregate resident memory for a subprocess process group."""

    def __init__(self, process_group: int, interval: float) -> None:
        self.process_group = process_group
        self.interval = interval
        self.peak_bytes: int | None = None
        self.peak_processes = 0
        self.samples = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> dict[str, int | None]:
        self._stop.set()
        self._thread.join(timeout=max(1.0, self.interval * 4.0))
        return {
            'peak_rss_bytes': self.peak_bytes,
            'peak_processes': self.peak_processes,
            'rss_samples': self.samples,
        }

    def _run(self) -> None:
        while not self._stop.is_set():
            resident, processes = _process_group_rss(self.process_group)
            if resident is not None:
                self.samples += 1
                if self.peak_bytes is None or resident > self.peak_bytes:
                    self.peak_bytes = resident
                    self.peak_processes = processes
            self._stop.wait(self.interval)


def _process_group_rss(process_group: int) -> tuple[int | None, int]:
    proc = Path('/proc')
    if not proc.is_dir():
        return None, 0
    page_size = os.sysconf('SC_PAGE_SIZE')
    resident_pages = 0
    processes = 0
    for entry in proc.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            stat = (entry / 'stat').read_text()
            closing_paren = stat.rfind(')')
            fields = stat[closing_paren + 2 :].split()
            if int(fields[2]) != process_group:
                continue
            statm = (entry / 'statm').read_text().split()
            resident_pages += int(statm[1])
            processes += 1
        # Processes can disappear between any of the procfs reads; Linux may
        # surface that race as ENOENT or ESRCH depending on the file.
        except (OSError, IndexError, ValueError):
            continue
    return resident_pages * page_size, processes


def _affinity_prefix(cpus: tuple[int, ...] | None) -> list[str]:
    if cpus is None:
        return []
    return ['taskset', '--cpu-list', ','.join(map(str, cpus))]


def _terminate_process_group(process: subprocess.Popen[bytes]) -> None:
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline:
        process.poll()
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            break
        time.sleep(0.05)
    else:
        os.killpg(process.pid, signal.SIGKILL)
    try:
        process.wait(timeout=3.0)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=3.0)


def _wait_for_server(
    process: subprocess.Popen[bytes], ready_url: str, timeout: float, *, insecure: bool
) -> None:
    context = ssl._create_unverified_context() if insecure else None  # noqa: S323
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise BenchmarkError(
                f'server exited during startup with status {process.returncode}'
            )
        try:
            with urllib.request.urlopen(  # noqa: S310
                ready_url, timeout=0.5, context=context
            ) as response:
                if response.status < 500:
                    return
        except urllib.error.HTTPError as error:
            if error.code < 500:
                return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.05)
    raise BenchmarkError(f'server did not become ready within {timeout:g}s')


def _wait_for_unix_server(
    process: subprocess.Popen[bytes], socket_path: Path, timeout: float
) -> None:
    request = b'GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n'
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise BenchmarkError(
                f'server exited during startup with status {process.returncode}'
            )
        if socket_path.exists():
            try:
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                    client.settimeout(0.5)
                    client.connect(str(socket_path))
                    client.sendall(request)
                    if client.recv(64).startswith(b'HTTP/1.'):
                        return
            except OSError:
                pass
        time.sleep(0.05)
    raise BenchmarkError(
        f'Unix server did not become ready within {timeout:g}s: {socket_path}'
    )


def _run_oha(args: argparse.Namespace) -> dict[str, Any]:
    command = [
        *_affinity_prefix(args.load_cpus),
        args.oha,
        '-z',
        args.duration,
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

    env = os.environ.copy()
    env.pop('NO_COLOR', None)
    started = time.monotonic()
    process = subprocess.Popen(
        command,
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    monitor = PeakRssMonitor(process.pid, args.rss_sample_interval)
    monitor.start()
    try:
        stdout, stderr = process.communicate(
            timeout=args.duration_seconds + args.load_grace
        )
    except subprocess.TimeoutExpired as error:
        _terminate_process_group(process)
        stdout, stderr = process.communicate()
        raise BenchmarkError(
            f'oha exceeded its bounded timeout; stderr={stderr[-2_000:].decode(errors="replace")!r}'
        ) from error
    except BaseException:
        _terminate_process_group(process)
        raise
    finally:
        resources = monitor.stop()

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


def _run_k6(args: argparse.Namespace) -> dict[str, Any]:
    with tempfile.NamedTemporaryFile(suffix='.json') as summary:
        command = [*_affinity_prefix(args.load_cpus), args.k6, 'run']
        if args.insecure:
            command.append('--insecure-skip-tls-verify')
        command.extend([
            '--quiet',
            '--duration',
            args.duration,
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
        env = os.environ.copy()
        env.pop('NO_COLOR', None)
        started = time.monotonic()
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        monitor = PeakRssMonitor(process.pid, args.rss_sample_interval)
        monitor.start()
        try:
            stdout, stderr = process.communicate(
                timeout=args.duration_seconds + args.load_grace
            )
        except subprocess.TimeoutExpired as error:
            _terminate_process_group(process)
            stdout, stderr = process.communicate()
            raise BenchmarkError(
                'k6 exceeded its bounded timeout; '
                f'stderr={stderr[-2_000:].decode(errors="replace")!r}'
            ) from error
        except BaseException:
            _terminate_process_group(process)
            raise
        finally:
            resources = monitor.stop()

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


def _run_load(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, float]]:
    if args.load_driver == 'oha':
        load = _run_oha(args)
        return load, _extract_oha_metrics(load['raw'])
    load = _run_k6(args)
    return load, _extract_k6_metrics(load['raw'])


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


def run_variant(command: NamedCommand, args: argparse.Namespace) -> dict[str, Any]:
    server_command = [*_affinity_prefix(args.server_cpus), *command.argv]
    started_at = datetime.now(UTC).isoformat()
    started = time.monotonic()
    with tempfile.TemporaryFile() as server_log:
        process = subprocess.Popen(
            server_command,
            cwd=ROOT,
            stdout=server_log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        monitor = PeakRssMonitor(process.pid, args.rss_sample_interval)
        monitor.start()
        try:
            if args.ready_unix_socket is None:
                _wait_for_server(
                    process,
                    args.ready_url,
                    args.startup_timeout,
                    insecure=args.insecure,
                )
            else:
                _wait_for_unix_server(
                    process, args.ready_unix_socket, args.startup_timeout
                )
            time.sleep(args.settle)
            load, metrics = _run_load(args)
            if process.poll() is not None:
                raise BenchmarkError(
                    f'server exited during load with status {process.returncode}'
                )
        except BaseException as error:
            _terminate_process_group(process)
            monitor.stop()
            server_log.seek(0)
            tail = server_log.read()[-4_000:].decode(errors='replace')
            if isinstance(error, BenchmarkError):
                raise BenchmarkError(f'{error}; server log tail={tail!r}') from error
            raise
        else:
            _terminate_process_group(process)
            server_resources = monitor.stop()

    peak_rss = server_resources['peak_rss_bytes']
    if isinstance(peak_rss, int) and peak_rss > 0:
        metrics['server_peak_rss_bytes'] = float(peak_rss)
    return {
        'variant': command.name,
        'server_command': server_command,
        'started_at': started_at,
        'elapsed_seconds': time.monotonic() - started,
        'metrics': metrics,
        'resources': {
            'server': server_resources,
            'load_generator': load['resources'],
        },
        'load': load,
    }


def _read_first(path: Path) -> str | None:
    try:
        return path.read_text().strip()
    except OSError:
        return None


def _command_version(command: Sequence[str]) -> str | None:
    try:
        result = subprocess.run(
            command,
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=5.0,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    output = (result.stdout or result.stderr).strip()
    return output.splitlines()[0] if output else None


def _file_sha256(path: Path) -> str | None:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


def _resolved_executable(command: NamedCommand) -> Path | None:
    executable = shutil.which(command.argv[0])
    if executable is None:
        path = Path(command.argv[0])
        if not path.is_file():
            return None
        executable = str(path)
    try:
        return Path(executable).resolve(strict=True)
    except OSError:
        return None


def _invoked_executable(command: NamedCommand) -> Path | None:
    """Return the executable invocation path without resolving venv symlinks."""
    executable = shutil.which(command.argv[0])
    if executable is None:
        path = Path(command.argv[0])
        if not path.is_file():
            return None
        executable = str(path)
    return Path(executable).absolute()


def _command_python(command: NamedCommand, executable: Path) -> Path | None:
    invoked = _invoked_executable(command)
    if invoked is not None and invoked.name.startswith('python'):
        return invoked
    try:
        first_line = (
            executable.open('rb').readline(512).decode(errors='replace').strip()
        )
    except OSError:
        return None
    if not first_line.startswith('#!'):
        return None
    try:
        shebang = shlex.split(first_line[2:])
        interpreter = shebang[0]
    except (ValueError, IndexError):
        return None
    if Path(interpreter).name == 'env' and len(shebang) >= 2:
        resolved = shutil.which(shebang[1])
        return Path(resolved).absolute() if resolved is not None else None
    path = Path(interpreter)
    return (
        path.absolute() if path.name.startswith('python') and path.is_file() else None
    )


def _variant_artifacts(command: NamedCommand) -> dict[str, Any]:
    executable = _resolved_executable(command)
    record: dict[str, Any] = {
        'executable': str(executable) if executable is not None else None,
        'executable_sha256': _file_sha256(executable)
        if executable is not None
        else None,
        'extension': None,
        'extension_sha256': None,
        'python_executable': None,
        'python_executable_sha256': None,
        'python_version': None,
    }
    if executable is None or (python := _command_python(command, executable)) is None:
        return record
    record['python_executable'] = str(python)
    record['python_executable_sha256'] = _file_sha256(python)
    probe = subprocess.run(
        [
            str(python),
            '-c',
            'import h2corn._lib,sys; print(sys.version); print(h2corn._lib.__file__)',
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=10.0,
        check=False,
    )
    if probe.returncode != 0:
        record['probe_error'] = (probe.stderr or probe.stdout)[-2_000:]
        return record
    lines = probe.stdout.splitlines()
    if len(lines) < 2:
        record['probe_error'] = f'unexpected probe output: {probe.stdout[-2_000:]!r}'
        return record
    extension = Path(lines[-1]).resolve()
    record.update({
        'extension': str(extension),
        'extension_sha256': _file_sha256(extension),
        'python_version': '\n'.join(lines[:-1]),
    })
    return record


def _git_metadata() -> dict[str, Any]:
    def git(*command: str, binary: bool = False) -> bytes | str | None:
        try:
            result = subprocess.run(
                ['git', *command],
                cwd=ROOT,
                capture_output=True,
                text=not binary,
                timeout=10.0,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            return None
        return result.stdout if result.returncode == 0 else None

    head = git('rev-parse', 'HEAD')
    status = git('status', '--short')
    diff = git('diff', '--binary', 'HEAD', binary=True)
    untracked = git('ls-files', '--others', '--exclude-standard', '-z', binary=True)
    untracked_hashes: dict[str, str | None] = {}
    if isinstance(untracked, bytes):
        for raw_path in sorted(filter(None, untracked.split(b'\0'))):
            path = raw_path.decode(errors='surrogateescape')
            untracked_hashes[path] = _file_sha256(ROOT.parent / path)
    return {
        'head': head.strip() if isinstance(head, str) else None,
        'status_short': status.splitlines() if isinstance(status, str) else None,
        'tracked_diff_sha256': hashlib.sha256(diff).hexdigest()
        if isinstance(diff, bytes)
        else None,
        'untracked_sha256': untracked_hashes,
    }


def collect_environment(args: argparse.Namespace) -> dict[str, Any]:
    cpu_model = None
    cpuinfo = _read_first(Path('/proc/cpuinfo'))
    if cpuinfo is not None:
        for line in cpuinfo.splitlines():
            if line.startswith('model name'):
                cpu_model = line.partition(':')[2].strip()
                break
    governors = sorted({
        value
        for path in Path('/sys/devices/system/cpu').glob(
            'cpu*/cpufreq/scaling_governor'
        )
        if (value := _read_first(path)) is not None
    })
    affinity = (
        sorted(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else None
    )
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
        str(path.relative_to(ROOT.parent)): _file_sha256(path)
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
        'hostname': socket.gethostname(),
        'platform': platform.platform(),
        'kernel': platform.release(),
        'machine': platform.machine(),
        'python': sys.version,
        'cpu_model': cpu_model,
        'logical_cpus': os.cpu_count(),
        'harness_affinity': affinity,
        'server_affinity': list(args.server_cpus) if args.server_cpus else None,
        'load_generator_affinity': list(args.load_cpus) if args.load_cpus else None,
        'cpu_governors': governors,
        'load_average': list(os.getloadavg()) if hasattr(os, 'getloadavg') else None,
        'oha_version': _command_version([args.oha, '--version']),
        'k6_version': _command_version([args.k6, 'version']),
        'tool_versions': {
            'cargo': _command_version(['cargo', '--version', '--verbose']),
            'maturin': _command_version(['maturin', '--version']),
            'rustc': _command_version(['rustc', '--version', '--verbose']),
            'uv': _command_version(['uv', '--version']),
        },
        'variant_artifacts': {
            'control': _variant_artifacts(args.control),
            'candidate': _variant_artifacts(args.candidate),
        },
        'build_environment': build_environment,
        'input_sha256': input_hashes,
        'kernel_command_line': _read_first(Path('/proc/cmdline')),
        'transparent_hugepages': _read_first(
            Path('/sys/kernel/mm/transparent_hugepage/enabled')
        ),
        'git': _git_metadata(),
    }


METRICS = {
    'requests_per_second': True,
    'latency_p50_seconds': False,
    'latency_p99_seconds': False,
    'latency_p99_9_seconds': False,
    'server_peak_rss_bytes': False,
}


def comparison_identity(args: argparse.Namespace) -> dict[str, Any]:
    """Return the complete semantic identity required to resume a run safely."""
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
        'body': args.body,
        'headers': list(args.header),
        'server_cpus': list(args.server_cpus) if args.server_cpus else None,
        'load_cpus': list(args.load_cpus) if args.load_cpus else None,
        'variants': {
            'control': {
                'name': args.control.name,
                'argv': list(args.control.argv),
            },
            'candidate': {
                'name': args.candidate.name,
                'argv': list(args.candidate.argv),
            },
        },
    }


def summarize_trials(
    trials: Sequence[dict[str, Any]], seed: int, bootstrap_samples: int
) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for metric_index, (metric, higher_is_better) in enumerate(METRICS.items()):
        controls = [trial['runs']['control']['metrics'].get(metric) for trial in trials]
        candidates = [
            trial['runs']['candidate']['metrics'].get(metric) for trial in trials
        ]
        if any(value is None for value in [*controls, *candidates]):
            continue
        summary[metric] = paired_comparison(
            controls,
            candidates,
            seed=seed ^ (0x9E3779B9 * (metric_index + 1)),
            higher_is_better=higher_is_better,
            bootstrap_samples=bootstrap_samples,
        )
    return summary


def _atomic_write(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f'{path.suffix}.tmp')
    temporary.write_text(json.dumps(record, indent=2, sort_keys=True) + '\n')
    temporary.replace(path)


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
    parser.add_argument('--body')
    parser.add_argument('--header', action='append', default=[])
    parser.add_argument('--oha', default='oha', help='oha executable')
    parser.add_argument('--k6', default='k6', help='k6 executable')
    parser.add_argument('--k6-script', default=ROOT / 'bench/k6/ws.js', type=Path)
    parser.add_argument('--load-driver', choices=('oha', 'k6'), default='oha')
    parser.add_argument('--startup-timeout', default=10.0, type=float)
    parser.add_argument('--load-grace', default=15.0, type=float)
    parser.add_argument('--settle', default=0.25, type=float)
    parser.add_argument('--rss-sample-interval', default=0.05, type=float)
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
    except argparse.ArgumentTypeError as error:
        parser.error(str(error))
    if args.trials < 5:
        parser.error('--trials must be at least 5 for a useful paired interval')
    if args.warmups < 1:
        parser.error('--warmups must be at least 1')
    if args.concurrency < 1:
        parser.error('--concurrency must be positive')
    if not 0.0 < args.startup_timeout <= 300.0:
        parser.error('--startup-timeout must be in (0, 300] seconds')
    if not 0.0 < args.load_grace <= 300.0:
        parser.error('--load-grace must be in (0, 300] seconds')
    if not 0.0 <= args.settle <= 30.0:
        parser.error('--settle must be in [0, 30] seconds')
    if not 0.005 <= args.rss_sample_interval <= 1.0:
        parser.error('--rss-sample-interval must be in [0.005, 1] seconds')
    if args.bootstrap_samples < 1_000:
        parser.error('--bootstrap-samples must be at least 1000')
    args.ready_url = args.ready_url or args.url
    if args.load_driver == 'k6' and args.unix_socket is not None:
        parser.error('k6 WebSocket loads do not support Unix sockets')
    if args.ready_unix_socket is not None and not hasattr(socket, 'AF_UNIX'):
        parser.error('Unix socket readiness is not supported on this platform')
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
    for metric, result in summary.items():
        ci_low, ci_high = result['paired_delta_ci95_percent']
        verdict = 'SIGNAL' if result['significant'] else 'NOISE'
        print(
            f'{metric}: control={result["control_median"]:.6g}{units[metric]} '
            f'candidate={result["candidate_median"]:.6g}{units[metric]} '
            f'delta={result["paired_delta_median_percent"]:+.2f}% '
            f'95%CI=[{ci_low:+.2f}%, {ci_high:+.2f}%] '
            f'noise={result["empirical_noise_floor_percent"]:.2f}% '
            f'improvement={result["improvement_median_percent"]:+.2f}% '
            f'verdict={verdict}'
        )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if (args.server_cpus or args.load_cpus) and shutil.which('taskset') is None:
        raise SystemExit('CPU affinity requested, but taskset is not installed')
    load_executable = args.oha if args.load_driver == 'oha' else args.k6
    if shutil.which(load_executable) is None and not Path(load_executable).is_file():
        raise SystemExit(f'{args.load_driver} executable not found: {load_executable}')
    if args.load_driver == 'k6' and not args.k6_script.is_file():
        raise SystemExit(f'k6 script not found: {args.k6_script}')

    commands = {'control': args.control, 'candidate': args.candidate}
    orders = blocked_orders(args.warmups + args.trials, args.seed)
    identity = comparison_identity(args)
    record: dict[str, Any] = {
        'schema_version': 2,
        'status': 'running',
        **identity,
        'comparison_identity': identity,
        'lead_orders': [list(order) for order in orders],
        'environment': collect_environment(args),
        'warmups': [],
        'trials': [],
    }
    _atomic_write(args.output, record)
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
                tag = 'measure' if measured else 'warmup'
                print(
                    f'[{tag} block={block_index} lead={order[0]}] '
                    f'{role} ({commands[role].name})'
                )
                block['runs'][role] = run_variant(commands[role], args)
                _atomic_write(args.output, record)
        record['summary'] = summarize_trials(
            record['trials'], args.seed, args.bootstrap_samples
        )
        record['status'] = 'complete'
        record['completed_at'] = datetime.now(UTC).isoformat()
    except KeyboardInterrupt:
        record['status'] = 'interrupted'
        record['error'] = 'KeyboardInterrupt'
        record['completed_at'] = datetime.now(UTC).isoformat()
        _atomic_write(args.output, record)
        print(
            f'benchmark interrupted; partial evidence: {args.output}', file=sys.stderr
        )
        return 130
    except Exception as error:
        record['status'] = 'failed'
        record['error'] = f'{type(error).__name__}: {error}'
        record['completed_at'] = datetime.now(UTC).isoformat()
        _atomic_write(args.output, record)
        print(f'benchmark failed: {error}', file=sys.stderr)
        print(f'partial evidence: {args.output}', file=sys.stderr)
        return 1

    _atomic_write(args.output, record)
    _print_summary(record['summary'])
    print(f'raw evidence: {args.output}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
