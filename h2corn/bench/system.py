"""Typed shared Linux layer for the bench harnesses.

Owns CPU topology and affinity, /proc/stat activity gates, the /proc/<pid>
process-group observation stack, server-readiness polling, durable JSON
writes, and oha/k6 load-result validation. bench.py, compare.py, matrix.py,
and mask_kernel_compare.py import this module instead of carrying copies.
"""

from __future__ import annotations

import json
import math
import os
import platform
import re
import secrets
import signal
import socket
import ssl
import subprocess
import threading
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from errno import ESRCH
from pathlib import Path
from typing import Any, Protocol, TypedDict

CPU_SYSFS_ROOT = Path('/sys/devices/system/cpu')
CPU_LIST_PATTERN = re.compile(r'^[0-9,-]+$')
PAGE_SIZE = os.sysconf('SC_PAGE_SIZE')
RESOURCE_SAMPLE_INTERVAL_SECONDS = 0.05
MAX_AMBIENT_CPU_UTILIZATION = 0.10
MAX_AMBIENT_SINGLE_CPU_UTILIZATION = 0.15


class BenchmarkError(RuntimeError):
    """A benchmark subprocess failed or produced invalid evidence."""


class AmbientCpuError(BenchmarkError):
    """The host was not quiet enough for publication-quality evidence."""


class PollableProcess(Protocol):
    def poll(self) -> int | None: ...


class CpuFrequencyPolicy(TypedDict):
    scaling_governor: str | None
    scaling_driver: str | None
    energy_performance_preference: str | None


class LastLevelCache(TypedDict):
    level: int
    cache_id: int
    cache_type: str
    shared_cpus: list[int]


class CpuTopology(TypedDict):
    cpu: int
    online: bool
    physical_package_id: int
    die_id: int
    core_id: int
    thread_siblings: list[int]
    last_level_cache: LastLevelCache
    frequency: CpuFrequencyPolicy


class CpuSetState(TypedDict):
    cpus: list[int]
    logical_cpu_count: int
    physical_core_count: int
    topology: list[CpuTopology]


class ThreadAffinityMask(TypedDict):
    cpus: list[int]
    thread_count: int


class BenchmarkSystemState(TypedDict):
    summary: str
    python_version: str
    kernel: str
    machine: str
    cpu_model: str
    online_cpus: list[int]
    allowed_cpus: list[int]
    cgroup_allowed_cpus: list[int] | None
    online_physical_core_count: int
    thread_affinity_masks: list[ThreadAffinityMask]
    boost_enabled: bool | None
    kernel_command_line: str | None
    transparent_hugepages: str | None
    server: CpuSetState | None
    load_generator: CpuSetState | None
    management: CpuSetState | None


class CpuUtilization(TypedDict):
    cpu: int
    total_ticks: int
    active_ticks: int
    utilization: float


class AmbientCpuProbe(TypedDict):
    started_at: str
    completed_at: str
    elapsed_seconds: float
    system_physical_core_utilization: float
    server_physical_core_utilization: float
    server_llc_physical_core_utilization: float
    load_llc_physical_core_utilization: float
    maximum_cpu_utilization: float
    per_cpu: list[CpuUtilization]


class CpuActivityWindow(TypedDict):
    started_at: str
    completed_at: str
    elapsed_seconds: float
    cpus: list[int]
    physical_core_count: int
    total_ticks: int
    active_ticks: int
    physical_core_utilization: float
    maximum_cpu_utilization: float
    per_cpu: list[CpuUtilization]


class CpuActivity(CpuActivityWindow):
    interval_seconds: float
    windows: list[CpuActivityWindow]
    maximum_raw_window_physical_core_utilization: float
    maximum_raw_window_cpu_utilization: float
    maximum_window_physical_core_utilization: float
    maximum_window_cpu_utilization: float


class CpuTimeSample(TypedDict):
    total_ticks: int
    idle_ticks: int


def _fixed_interval_window_utilization(
    window: CpuActivityWindow, interval_seconds: float
) -> tuple[float, float]:
    """Normalize a partial tail to the monitor's declared gate interval.

    The raw window remains in the evidence. Scaling its utilization by its
    observed fraction of the interval converts the work to fixed-window
    exposure without treating a subsecond denominator as a full interval.
    """
    elapsed = window['elapsed_seconds']
    if elapsed <= 0.0:
        raise RuntimeError('CPU activity window did not advance wall time')
    exposure = min(elapsed / interval_seconds, 1.0)
    return (
        window['physical_core_utilization'] * exposure,
        window['maximum_cpu_utilization'] * exposure,
    )


class CpuActivityMonitor:
    """Measure fixed-interval CPU exposure and retain the raw trailing sample."""

    __slots__ = (
        '_cpus',
        '_error',
        '_initial',
        '_interval_seconds',
        '_latest',
        '_latest_at',
        '_latest_monotonic',
        '_physical_core_count',
        '_started',
        '_started_at',
        '_stop_event',
        '_thread',
        '_windows',
    )

    def __init__(
        self,
        cpus: tuple[int, ...],
        physical_core_count: int,
        *,
        interval_seconds: float = 1.0,
    ) -> None:
        if interval_seconds <= 0.0:
            raise ValueError('CPU activity interval must be positive')
        if not cpus or len(set(cpus)) != len(cpus):
            raise ValueError('CPU activity requires unique CPUs')
        if not 1 <= physical_core_count <= len(cpus):
            raise ValueError('invalid physical-core count for CPU activity')
        self._cpus = cpus
        self._physical_core_count = physical_core_count
        self._interval_seconds = interval_seconds
        self._initial: dict[int, CpuTimeSample] | None = None
        self._latest: dict[int, CpuTimeSample] | None = None
        self._started = 0.0
        self._started_at = ''
        self._latest_monotonic = 0.0
        self._latest_at = ''
        self._windows: list[CpuActivityWindow] = []
        self._error: BaseException | None = None
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._initial is not None:
            raise RuntimeError('CPU activity monitor is already running')
        self._error = None
        self._windows.clear()
        self._started = time.monotonic()
        self._started_at = datetime.now(UTC).isoformat()
        self._initial = read_cpu_times(self._cpus)
        self._latest = self._initial
        self._latest_monotonic = self._started
        self._latest_at = self._started_at
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._sample_loop,
            name='h2corn-benchmark-cpu-monitor',
            daemon=True,
        )
        self._thread.start()

    def _sample_loop(self) -> None:
        try:
            deadline = self._started + self._interval_seconds
            while not self._stop_event.wait(max(0.0, deadline - time.monotonic())):
                self._capture_window()
                deadline += self._interval_seconds
                while deadline <= self._latest_monotonic:
                    deadline += self._interval_seconds
        except BaseException as error:
            self._error = error
            self._stop_event.set()

    def _capture_window(
        self,
        after: dict[int, CpuTimeSample] | None = None,
        *,
        completed: float | None = None,
        completed_at: str | None = None,
    ) -> None:
        assert self._latest is not None
        if after is None:
            after = read_cpu_times(self._cpus)
        if completed is None:
            completed = time.monotonic()
        if completed_at is None:
            completed_at = datetime.now(UTC).isoformat()
        self._windows.append(
            summarize_cpu_activity(
                self._latest,
                after,
                self._cpus,
                physical_core_count=self._physical_core_count,
                started_at=self._latest_at,
                completed_at=completed_at,
                elapsed_seconds=completed - self._latest_monotonic,
            )
        )
        self._latest = after
        self._latest_monotonic = completed
        self._latest_at = completed_at

    def stop(self) -> CpuActivity:
        if self._initial is None or self._thread is None:
            raise RuntimeError('CPU activity monitor is not running')
        self._stop_event.set()
        self._thread.join()
        if self._error is not None:
            error = self._error
            self._reset()
            raise error
        try:
            after = read_cpu_times(self._cpus)
            completed = time.monotonic()
            completed_at = datetime.now(UTC).isoformat()
            elapsed = completed - self._started
            latest = self._latest
            assert latest is not None
            if any(
                after[cpu]['total_ticks'] > latest[cpu]['total_ticks']
                for cpu in self._cpus
            ):
                self._capture_window(
                    after,
                    completed=completed,
                    completed_at=completed_at,
                )
            aggregate = summarize_cpu_activity(
                self._initial,
                after,
                self._cpus,
                physical_core_count=self._physical_core_count,
                started_at=self._started_at,
                completed_at=completed_at,
                elapsed_seconds=elapsed,
            )
            windows = self._windows.copy()
            gate_utilizations = [
                _fixed_interval_window_utilization(window, self._interval_seconds)
                for window in windows
            ]
            return {
                **aggregate,
                'interval_seconds': self._interval_seconds,
                'windows': windows,
                'maximum_raw_window_physical_core_utilization': max(
                    (window['physical_core_utilization'] for window in windows),
                    default=0.0,
                ),
                'maximum_raw_window_cpu_utilization': max(
                    (window['maximum_cpu_utilization'] for window in windows),
                    default=0.0,
                ),
                'maximum_window_physical_core_utilization': max(
                    (physical for physical, _cpu in gate_utilizations),
                    default=0.0,
                ),
                'maximum_window_cpu_utilization': max(
                    (cpu for _physical, cpu in gate_utilizations),
                    default=0.0,
                ),
            }
        finally:
            self._reset()

    def _reset(self) -> None:
        self._initial = None
        self._latest = None
        self._thread = None
        self._error = None


def _read_text(path: Path) -> str | None:
    try:
        value = path.read_text(encoding='utf-8').strip()
    except OSError:
        return None
    return value or None


def _required_text(path: Path, label: str) -> str:
    if (value := _read_text(path)) is None:
        raise RuntimeError(f'failed to read {label} from {path}')
    return value


def _required_int(path: Path, label: str) -> int:
    value = _required_text(path, label)
    try:
        return int(value)
    except ValueError as error:
        raise RuntimeError(f'invalid {label} in {path}: {value!r}') from error


def parse_linux_cpu_list(value: str) -> tuple[int, ...]:
    """Parse the Linux sysfs CPU-list representation into sorted CPU IDs."""
    if not value or CPU_LIST_PATTERN.fullmatch(value) is None:
        raise ValueError(f'invalid Linux CPU list: {value!r}')
    cpus: set[int] = set()
    for part in value.split(','):
        endpoints = part.split('-', 1)
        start = int(endpoints[0])
        stop = int(endpoints[-1])
        if stop < start:
            raise ValueError(f'invalid Linux CPU range: {part!r}')
        cpus.update(range(start, stop + 1))
    return tuple(sorted(cpus))


def _task_ids(task_root: Path) -> tuple[int, ...]:
    try:
        task_ids = tuple(
            sorted(
                int(path.name) for path in task_root.iterdir() if path.name.isdigit()
            )
        )
    except OSError as error:
        raise RuntimeError(
            f'failed to enumerate process threads in {task_root}'
        ) from error
    if not task_ids:
        raise RuntimeError(f'no process threads found in {task_root}')
    return task_ids


def _thread_affinities(task_ids: tuple[int, ...]) -> dict[int, tuple[int, ...]]:
    affinities: dict[int, tuple[int, ...]] = {}
    for task_id in task_ids:
        try:
            affinities[task_id] = tuple(sorted(os.sched_getaffinity(task_id)))
        except OSError as error:
            if error.errno == ESRCH:
                continue
            raise RuntimeError(
                f'failed to read CPU affinity for thread {task_id}'
            ) from error
    return affinities


def _group_thread_affinities(
    affinities: dict[int, tuple[int, ...]],
) -> list[ThreadAffinityMask]:
    counts: dict[tuple[int, ...], int] = {}
    for cpus in affinities.values():
        counts[cpus] = counts.get(cpus, 0) + 1
    return [
        {'cpus': list(cpus), 'thread_count': count}
        for cpus, count in sorted(counts.items())
    ]


def capture_thread_affinity_masks(
    *, task_root: Path = Path('/proc/self/task')
) -> list[ThreadAffinityMask]:
    """Capture a stable, compact affinity snapshot of every process thread."""
    for _attempt in range(100):
        before = _task_ids(task_root)
        affinities = _thread_affinities(before)
        after = _task_ids(task_root)
        if set(affinities) == set(before) == set(after):
            return _group_thread_affinities(affinities)
    raise RuntimeError('process thread set did not stabilize while reading affinity')


def pin_process_threads(
    cpus: tuple[int, ...], *, task_root: Path = Path('/proc/self/task')
) -> list[ThreadAffinityMask]:
    """Pin every current process thread, including threads created during pinning.

    Linux affinity is per-thread: setting PID 0 affects only the calling thread.
    Repeated stable passes close the race with libraries that create native worker
    threads while the benchmark harness is initializing.
    """
    if not cpus:
        raise ValueError('process thread affinity requires at least one CPU')
    desired = tuple(sorted(set(cpus)))
    if len(desired) != len(cpus):
        raise ValueError('process thread affinity CPUs must be unique')
    previous: tuple[int, ...] | None = None
    for _attempt in range(100):
        before = _task_ids(task_root)
        for task_id in before:
            try:
                os.sched_setaffinity(task_id, desired)
            except OSError as error:
                if error.errno == ESRCH:
                    continue
                raise RuntimeError(
                    f'failed to set CPU affinity for thread {task_id}'
                ) from error
        affinities = _thread_affinities(before)
        after = _task_ids(task_root)
        stable = set(affinities) == set(before) == set(after)
        correctly_pinned = stable and all(
            affinity == desired for affinity in affinities.values()
        )
        if correctly_pinned and previous == after:
            return _group_thread_affinities(affinities)
        previous = after if correctly_pinned else None
    raise RuntimeError('process thread set did not stabilize while setting affinity')


def _cpu_model() -> str:
    cpuinfo = Path('/proc/cpuinfo')
    if cpuinfo.exists():
        for line in cpuinfo.read_text(encoding='utf-8', errors='ignore').splitlines():
            if line.startswith('model name'):
                return line.partition(':')[2].strip()
    raise RuntimeError('failed to determine CPU model for benchmark provenance')


def _online_cpus(sysfs_root: Path) -> tuple[int, ...]:
    value = _read_text(sysfs_root / 'online')
    if value is not None:
        try:
            return parse_linux_cpu_list(value)
        except ValueError as error:
            raise RuntimeError(f'invalid online CPU list: {value!r}') from error
    cpus = sorted(
        int(path.name[3:])
        for path in sysfs_root.glob('cpu[0-9]*')
        if path.name[3:].isdigit()
    )
    if not cpus:
        raise RuntimeError(f'failed to determine online CPUs from {sysfs_root}')
    return tuple(cpus)


def _last_level_cache(cpu_root: Path) -> LastLevelCache:
    candidates: list[LastLevelCache] = []
    for cache_root in cpu_root.joinpath('cache').glob('index[0-9]*'):
        cache_type = _read_text(cache_root / 'type')
        if cache_type not in {'Data', 'Unified'}:
            continue
        level = _required_int(cache_root / 'level', 'cache level')
        cache_id = _required_int(cache_root / 'id', 'cache ID')
        shared = _required_text(cache_root / 'shared_cpu_list', 'cache sharing set')
        try:
            shared_cpus = list(parse_linux_cpu_list(shared))
        except ValueError as error:
            raise RuntimeError(
                f'invalid cache sharing set in {cache_root}: {shared!r}'
            ) from error
        candidates.append({
            'level': level,
            'cache_id': cache_id,
            'cache_type': cache_type,
            'shared_cpus': shared_cpus,
        })
    if not candidates:
        raise RuntimeError(f'failed to determine last-level cache for {cpu_root.name}')
    return max(candidates, key=lambda cache: cache['level'])


def capture_cpu_topology(
    cpu: int,
    *,
    sysfs_root: Path = CPU_SYSFS_ROOT,
    online_cpus: tuple[int, ...] | None = None,
) -> CpuTopology:
    online_cpus = _online_cpus(sysfs_root) if online_cpus is None else online_cpus
    cpu_root = sysfs_root / f'cpu{cpu}'
    topology_root = cpu_root / 'topology'
    siblings = _required_text(
        topology_root / 'thread_siblings_list', 'thread sibling set'
    )
    try:
        thread_siblings = list(parse_linux_cpu_list(siblings))
    except ValueError as error:
        raise RuntimeError(
            f'invalid thread sibling set for CPU {cpu}: {siblings!r}'
        ) from error
    return {
        'cpu': cpu,
        'online': cpu in online_cpus,
        'physical_package_id': _required_int(
            topology_root / 'physical_package_id', 'physical package ID'
        ),
        'die_id': _required_int(topology_root / 'die_id', 'die ID'),
        'core_id': _required_int(topology_root / 'core_id', 'core ID'),
        'thread_siblings': thread_siblings,
        'last_level_cache': _last_level_cache(cpu_root),
        'frequency': {
            'scaling_governor': _read_text(cpu_root / 'cpufreq/scaling_governor'),
            'scaling_driver': _read_text(cpu_root / 'cpufreq/scaling_driver'),
            'energy_performance_preference': _read_text(
                cpu_root / 'cpufreq/energy_performance_preference'
            ),
        },
    }


def capture_cpu_set(
    cpus: tuple[int, ...], *, sysfs_root: Path = CPU_SYSFS_ROOT
) -> CpuSetState:
    online = _online_cpus(sysfs_root)
    topology = [
        capture_cpu_topology(cpu, sysfs_root=sysfs_root, online_cpus=online)
        for cpu in cpus
    ]
    physical_cores = {
        (entry['physical_package_id'], entry['die_id'], entry['core_id'])
        for entry in topology
    }
    return {
        'cpus': list(cpus),
        'logical_cpu_count': len(cpus),
        'physical_core_count': len(physical_cores),
        'topology': topology,
    }


def physical_core_capacity(
    cpus: tuple[int, ...], *, sysfs_root: Path = CPU_SYSFS_ROOT
) -> int:
    """Return physical cores represented by a logical CPU set.

    This is intentionally strict: benchmark CPU saturation must not silently
    fall back to an SMT-thread count that overstates available generator
    capacity.
    """
    capacity = capture_cpu_set(cpus, sysfs_root=sysfs_root)['physical_core_count']
    if capacity < 1:
        raise RuntimeError('load-generator CPU set has no physical cores')
    return capacity


def read_cpu_times(
    cpus: tuple[int, ...], *, proc_stat: Path = Path('/proc/stat')
) -> dict[int, CpuTimeSample]:
    wanted = set(cpus)
    samples: dict[int, CpuTimeSample] = {}
    try:
        lines = proc_stat.read_text(encoding='utf-8').splitlines()
    except OSError as error:
        raise RuntimeError(
            f'failed to read CPU utilization from {proc_stat}'
        ) from error
    for line in lines:
        fields = line.split()
        label = fields[0] if fields else ''
        if not label.startswith('cpu') or not label[3:].isdigit():
            continue
        cpu = int(label[3:])
        if cpu not in wanted:
            continue
        try:
            # Linux includes guest and guest_nice in user and nice already.
            ticks = [int(value) for value in fields[1:9]]
        except ValueError as error:
            raise RuntimeError(f'invalid CPU counters for CPU {cpu}') from error
        if len(ticks) < 5:
            raise RuntimeError(f'incomplete CPU counters for CPU {cpu}')
        samples[cpu] = {
            'total_ticks': sum(ticks),
            'idle_ticks': ticks[3] + ticks[4],
        }
    missing = sorted(wanted - samples.keys())
    if missing:
        raise RuntimeError(
            'missing /proc/stat counters for CPUs ' + ','.join(map(str, missing))
        )
    return samples


def summarize_cpu_activity(
    before: dict[int, CpuTimeSample],
    after: dict[int, CpuTimeSample],
    cpus: tuple[int, ...],
    *,
    physical_core_count: int,
    started_at: str,
    completed_at: str,
    elapsed_seconds: float,
) -> CpuActivityWindow:
    if not cpus or physical_core_count < 1:
        raise ValueError('CPU activity requires CPUs and physical cores')
    per_cpu: list[CpuUtilization] = []
    total_ticks = 0
    active_ticks = 0
    for cpu in cpus:
        total = after[cpu]['total_ticks'] - before[cpu]['total_ticks']
        idle = after[cpu]['idle_ticks'] - before[cpu]['idle_ticks']
        if total < 0 or not 0 <= idle <= total:
            raise RuntimeError(f'invalid CPU counter delta for CPU {cpu}')
        active = total - idle
        total_ticks += total
        active_ticks += active
        per_cpu.append({
            'cpu': cpu,
            'total_ticks': total,
            'active_ticks': active,
            'utilization': 0.0 if total == 0 else active / total,
        })
    if total_ticks == 0:
        raise RuntimeError('CPU counters did not advance during activity window')
    one_logical_cpu_ticks = total_ticks / len(cpus)
    physical_capacity_ticks = one_logical_cpu_ticks * physical_core_count
    return {
        'started_at': started_at,
        'completed_at': completed_at,
        'elapsed_seconds': elapsed_seconds,
        'cpus': list(cpus),
        'physical_core_count': physical_core_count,
        'total_ticks': total_ticks,
        'active_ticks': active_ticks,
        'physical_core_utilization': active_ticks / physical_capacity_ticks,
        'maximum_cpu_utilization': max(entry['utilization'] for entry in per_cpu),
        'per_cpu': per_cpu,
    }


def capture_ambient_cpu_probe(
    online_cpus: tuple[int, ...],
    server_physical_cpus: tuple[int, ...],
    server_llc_cpus: tuple[int, ...],
    load_llc_cpus: tuple[int, ...],
    *,
    online_physical_core_count: int,
    server_physical_core_count: int,
    server_llc_physical_core_count: int,
    load_llc_physical_core_count: int,
    duration_seconds: float,
    proc_stat: Path = Path('/proc/stat'),
) -> AmbientCpuProbe:
    """Measure foreign CPU activity while no benchmark load is running."""
    if duration_seconds <= 0.0:
        raise ValueError('CPU utilization probe duration must be positive')
    started_at = datetime.now(UTC).isoformat()
    before = read_cpu_times(online_cpus, proc_stat=proc_stat)
    started = time.monotonic()
    time.sleep(duration_seconds)
    elapsed = time.monotonic() - started
    completed_at = datetime.now(UTC).isoformat()
    after = read_cpu_times(online_cpus, proc_stat=proc_stat)

    def activity(cpus: tuple[int, ...], physical_cores: int) -> CpuActivityWindow:
        return summarize_cpu_activity(
            before,
            after,
            cpus,
            physical_core_count=physical_cores,
            started_at=started_at,
            completed_at=completed_at,
            elapsed_seconds=elapsed,
        )

    system = activity(online_cpus, online_physical_core_count)
    server_physical = activity(server_physical_cpus, server_physical_core_count)
    server_llc = activity(server_llc_cpus, server_llc_physical_core_count)
    load_llc = activity(load_llc_cpus, load_llc_physical_core_count)
    return {
        'started_at': started_at,
        'completed_at': completed_at,
        'elapsed_seconds': elapsed,
        'system_physical_core_utilization': system['physical_core_utilization'],
        'server_physical_core_utilization': server_physical[
            'physical_core_utilization'
        ],
        'server_llc_physical_core_utilization': server_llc['physical_core_utilization'],
        'load_llc_physical_core_utilization': load_llc['physical_core_utilization'],
        'maximum_cpu_utilization': system['maximum_cpu_utilization'],
        'per_cpu': system['per_cpu'],
    }


def capture_system_state(
    server_cpus: tuple[int, ...] | None,
    load_cpus: tuple[int, ...] | None,
    management_cpus: tuple[int, ...] | None = None,
    *,
    sysfs_root: Path = CPU_SYSFS_ROOT,
) -> BenchmarkSystemState:
    kernel = f'{platform.system()} {platform.release()}'.strip()
    machine = platform.machine()
    python_version = platform.python_version()
    cpu_model = _cpu_model()
    online = _online_cpus(sysfs_root)
    allowed = sorted(os.sched_getaffinity(0))
    cgroup_allowed_text = _read_text(Path('/sys/fs/cgroup/cpuset.cpus.effective'))
    cgroup_allowed = (
        None
        if cgroup_allowed_text is None
        else list(parse_linux_cpu_list(cgroup_allowed_text))
    )
    boost = _read_text(sysfs_root / 'cpufreq/boost')
    if boost not in {None, '0', '1'}:
        raise RuntimeError(f'invalid CPU boost state: {boost!r}')
    return {
        'summary': f'Python {python_version} | {kernel} | {machine}\nCPU: {cpu_model}',
        'python_version': python_version,
        'kernel': kernel,
        'machine': machine,
        'cpu_model': cpu_model,
        'online_cpus': list(online),
        'allowed_cpus': allowed,
        'cgroup_allowed_cpus': cgroup_allowed,
        'online_physical_core_count': physical_core_capacity(
            online, sysfs_root=sysfs_root
        ),
        'thread_affinity_masks': capture_thread_affinity_masks(),
        'boost_enabled': None if boost is None else boost == '1',
        'kernel_command_line': _read_text(Path('/proc/cmdline')),
        'transparent_hugepages': _read_text(
            Path('/sys/kernel/mm/transparent_hugepage/enabled')
        ),
        'server': None
        if server_cpus is None
        else capture_cpu_set(server_cpus, sysfs_root=sysfs_root),
        'load_generator': None
        if load_cpus is None
        else capture_cpu_set(load_cpus, sysfs_root=sysfs_root),
        'management': None
        if management_cpus is None
        else capture_cpu_set(management_cpus, sysfs_root=sysfs_root),
    }


def benchmark_system_state_matches(
    expected: BenchmarkSystemState, actual: BenchmarkSystemState
) -> bool:
    """Compare frozen host state while allowing harmless thread-count churn.

    Native helper threads may finish or start between checkpoints. Their CPU
    masks are an invariant; their exact count is provenance, not a
    result-shaping setting.
    """
    expected_masks = [mask['cpus'] for mask in expected['thread_affinity_masks']]
    actual_masks = [mask['cpus'] for mask in actual['thread_affinity_masks']]
    expected_state = expected.copy()
    actual_state = actual.copy()
    expected_state['thread_affinity_masks'] = []
    actual_state['thread_affinity_masks'] = []
    return expected_state == actual_state and expected_masks == actual_masks


class ProcessResourceSample(TypedDict):
    cpu_ticks: int
    rss_bytes: int


class ProcessGroupUsage(TypedDict):
    elapsed_seconds: float
    cpu_seconds: float
    average_cpu_cores: float
    peak_rss_bytes: int
    peak_process_count: int
    sample_count: int
    sampling_interval_seconds: float


def read_process_group_resources(
    process_group: int,
    proc_root: Path = Path('/proc'),
) -> dict[int, ProcessResourceSample]:
    """Read cumulative CPU and resident memory for every process in a group."""
    try:
        entries = [entry for entry in proc_root.iterdir() if entry.name.isdecimal()]
    except OSError:
        return {}

    samples: dict[int, ProcessResourceSample] = {}
    for entry in entries:
        try:
            stat = (entry / 'stat').read_text()
            closing_paren = stat.rfind(')')
            if closing_paren < 0:
                continue
            fields = stat[closing_paren + 2 :].split()
            if int(fields[2]) != process_group:
                continue
            cpu_ticks = int(fields[11]) + int(fields[12])
        except (OSError, IndexError, ValueError):
            # A process can exit between directory enumeration and either read;
            # Linux may surface that race as ENOENT or ESRCH per file.
            continue

        try:
            statm = (entry / 'statm').read_text().split()
            rss_bytes = int(statm[1]) * PAGE_SIZE
        except (OSError, IndexError, ValueError):
            # Retain the cumulative CPU observation if the process disappeared
            # between the stat and statm reads.
            rss_bytes = 0
        samples[int(entry.name)] = {
            'cpu_ticks': cpu_ticks,
            'rss_bytes': rss_bytes,
        }
    return samples


class ProcessGroupResourceSampler:
    """Measure whole-group CPU and peak aggregate RSS during one load run."""

    def __init__(
        self,
        process_group: int,
        interval: float = RESOURCE_SAMPLE_INTERVAL_SECONDS,
    ) -> None:
        if interval <= 0:
            raise ValueError('resource sample interval must be positive')
        self.process_group = process_group
        self.interval = interval
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._baseline_ticks: dict[int, int] = {}
        self._maximum_ticks: dict[int, int] = {}
        self._peak_rss_bytes = 0
        self._peak_process_count = 0
        self._sample_count = 0
        self._started_at: float | None = None
        self._result: ProcessGroupUsage | None = None

    def start(self) -> None:
        if self._started_at is not None:
            raise RuntimeError('process-group resource sampler already started')
        self._started_at = time.monotonic()
        initial = self._observe()
        self._baseline_ticks = {
            pid: sample['cpu_ticks'] for pid, sample in initial.items()
        }
        self._thread.start()

    def stop(self) -> ProcessGroupUsage:
        if self._started_at is None:
            raise RuntimeError('process-group resource sampler was not started')
        if self._result is not None:
            return self._result

        self._stop.set()
        self._thread.join(timeout=max(1.0, self.interval * 4.0))
        if self._thread.is_alive():
            raise BenchmarkError('process-group resource sampler did not stop')
        self._observe()
        elapsed = max(time.monotonic() - self._started_at, 1e-9)
        consumed_ticks = sum(
            maximum - self._baseline_ticks.get(pid, 0)
            for pid, maximum in self._maximum_ticks.items()
        )
        cpu_seconds = consumed_ticks / os.sysconf('SC_CLK_TCK')
        self._result = {
            'elapsed_seconds': elapsed,
            'cpu_seconds': cpu_seconds,
            'average_cpu_cores': cpu_seconds / elapsed,
            'peak_rss_bytes': self._peak_rss_bytes,
            'peak_process_count': self._peak_process_count,
            'sample_count': self._sample_count,
            'sampling_interval_seconds': self.interval,
        }
        return self._result

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            self._observe()

    def _observe(self) -> dict[int, ProcessResourceSample]:
        samples = read_process_group_resources(self.process_group)
        if samples:
            self._sample_count += 1
            self._peak_rss_bytes = max(
                self._peak_rss_bytes,
                sum(sample['rss_bytes'] for sample in samples.values()),
            )
            self._peak_process_count = max(self._peak_process_count, len(samples))
            for pid, sample in samples.items():
                self._maximum_ticks[pid] = max(
                    self._maximum_ticks.get(pid, 0), sample['cpu_ticks']
                )
        return samples


def terminate_process_group(process: subprocess.Popen[Any]) -> None:
    """Terminate the complete process tree and verify no child survives."""
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        process.poll()
        return
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        process.poll()
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.05)
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=3.0)
    except subprocess.TimeoutExpired as error:
        raise BenchmarkError('process group survived SIGKILL') from error


def wait_for_http_server(
    process: PollableProcess,
    ready_url: str,
    timeout: float,
    *,
    insecure: bool = False,
) -> None:
    """Poll a TCP HTTP(S) readiness URL; any status below 500 is ready."""
    context = ssl._create_unverified_context() if insecure else None  # noqa: S323
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise BenchmarkError(
                f'server exited during startup with status {process.poll()}'
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


def wait_for_unix_server(
    process: PollableProcess, socket_path: Path, timeout: float
) -> None:
    """Poll a Unix-domain HTTP server until it answers with HTTP/1.1."""
    request = b'GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n'
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise BenchmarkError(
                f'server exited during startup with status {process.poll()}'
            )
        if socket_path.exists():
            try:
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                    client.settimeout(0.5)
                    client.connect(str(socket_path))
                    client.sendall(request)
                    if client.recv(64).startswith(b'HTTP/1.1 '):
                        return
            except OSError:
                pass
        time.sleep(0.05)
    raise BenchmarkError(
        f'Unix server did not become ready within {timeout:g}s: {socket_path}'
    )


def cpu_set_physical_cpus(cpu_set: CpuSetState) -> tuple[int, ...]:
    return tuple(
        sorted({
            sibling
            for entry in cpu_set['topology']
            for sibling in entry['thread_siblings']
        })
    )


def cpu_set_llc_cpus(cpu_set: CpuSetState) -> tuple[int, ...]:
    return tuple(
        sorted({
            cpu
            for entry in cpu_set['topology']
            for cpu in entry['last_level_cache']['shared_cpus']
        })
    )


def capture_role_ambient_cpu(
    system: BenchmarkSystemState, duration_seconds: float
) -> AmbientCpuProbe:
    """Probe ambient activity over the host and the server/load role domains.

    Role CPU sets are filtered to online CPUs: offline siblings have no
    /proc/stat counters and cannot carry foreign activity.
    """
    server = system['server']
    load = system['load_generator']
    if server is None or load is None:
        raise AmbientCpuError('ambient CPU gate requires pinned server/load CPUs')
    online = set(system['online_cpus'])
    server_physical_cpus = tuple(
        cpu for cpu in cpu_set_physical_cpus(server) if cpu in online
    )
    server_llc_cpus = tuple(cpu for cpu in cpu_set_llc_cpus(server) if cpu in online)
    load_llc_cpus = tuple(cpu for cpu in cpu_set_llc_cpus(load) if cpu in online)
    return capture_ambient_cpu_probe(
        tuple(system['online_cpus']),
        server_physical_cpus,
        server_llc_cpus,
        load_llc_cpus,
        online_physical_core_count=system['online_physical_core_count'],
        server_physical_core_count=server['physical_core_count'],
        server_llc_physical_core_count=physical_core_capacity(server_llc_cpus),
        load_llc_physical_core_count=physical_core_capacity(load_llc_cpus),
        duration_seconds=duration_seconds,
    )


def validate_ambient_cpu(
    probe: AmbientCpuProbe,
    *,
    max_aggregate: float,
    max_single: float,
) -> None:
    aggregate_utilizations = (
        probe['system_physical_core_utilization'],
        probe['server_physical_core_utilization'],
        probe['server_llc_physical_core_utilization'],
        probe['load_llc_physical_core_utilization'],
    )
    if (
        max(aggregate_utilizations) > max_aggregate
        or probe['maximum_cpu_utilization'] > max_single
    ):
        raise AmbientCpuError(
            'ambient CPU quiet-window gate failed: '
            f'system={probe["system_physical_core_utilization"]:.1%}, '
            f'server-cores={probe["server_physical_core_utilization"]:.1%}, '
            f'server-llc={probe["server_llc_physical_core_utilization"]:.1%}, '
            f'load-llc={probe["load_llc_physical_core_utilization"]:.1%}, '
            f'max-cpu={probe["maximum_cpu_utilization"]:.1%}; limits '
            f'{max_aggregate:.1%} aggregate / {max_single:.1%} per CPU'
        )


def validate_interference_cpu(
    activity: CpuActivity,
    *,
    max_aggregate: float,
    max_single: float,
) -> None:
    maximum_physical = max(
        activity['physical_core_utilization'],
        activity['maximum_window_physical_core_utilization'],
    )
    maximum_cpu = max(
        activity['maximum_cpu_utilization'],
        activity['maximum_window_cpu_utilization'],
    )
    if maximum_physical > max_aggregate or maximum_cpu > max_single:
        raise AmbientCpuError(
            'CPU interference gate failed during measured load: '
            f'aggregate-physical-cores={activity["physical_core_utilization"]:.1%}, '
            'worst-window-physical-cores='
            f'{activity["maximum_window_physical_core_utilization"]:.1%}, '
            f'aggregate-max-cpu={activity["maximum_cpu_utilization"]:.1%}, '
            f'worst-window-max-cpu={activity["maximum_window_cpu_utilization"]:.1%}, '
            'raw-window-physical-cores='
            f'{activity["maximum_raw_window_physical_core_utilization"]:.1%}, '
            f'raw-window-max-cpu={activity["maximum_raw_window_cpu_utilization"]:.1%}'
            f'; limits {max_aggregate:.1%} aggregate / {max_single:.1%} per CPU'
        )


def physical_core_keys(cpu_set: CpuSetState) -> set[tuple[int, int, int]]:
    return {
        (entry['physical_package_id'], entry['die_id'], entry['core_id'])
        for entry in cpu_set['topology']
    }


def validate_cpu_roles(
    server_cpus: tuple[int, ...] | None,
    load_cpus: tuple[int, ...] | None,
    management_cpus: tuple[int, ...] | None,
    system: BenchmarkSystemState,
) -> None:
    roles = (server_cpus, load_cpus, management_cpus)
    if all(role is None for role in roles):
        return
    if any(role is None for role in roles):
        raise BenchmarkError(
            'CPU affinity requires explicit server, load-generator, and management CPUs'
        )
    server_cpus, load_cpus, management_cpus = roles
    assert server_cpus is not None
    assert load_cpus is not None
    assert management_cpus is not None
    if len(management_cpus) != 1:
        raise BenchmarkError('benchmarking requires exactly one management CPU')

    role_sets = tuple(map(set, (server_cpus, load_cpus, management_cpus)))
    logical_overlap = (role_sets[0] & role_sets[1]) | (
        role_sets[2] & (role_sets[0] | role_sets[1])
    )
    if logical_overlap:
        raise BenchmarkError(
            'server/load/management CPU sets overlap: '
            + ','.join(map(str, sorted(logical_overlap)))
        )

    server = system['server']
    load = system['load_generator']
    management = system['management']
    if server is None or load is None or management is None:
        raise BenchmarkError('benchmark CPU-role topology is incomplete')
    selected = [*server['topology'], *load['topology'], *management['topology']]
    offline = [entry['cpu'] for entry in selected if not entry['online']]
    if offline:
        raise BenchmarkError(
            'benchmark CPUs must be online; offline: '
            + ','.join(map(str, sorted(offline)))
        )
    if system['cgroup_allowed_cpus'] is None:
        raise BenchmarkError('benchmarking requires cgroup CPU-availability evidence')
    allowed = set(system['cgroup_allowed_cpus'])
    disallowed = sorted(set().union(*role_sets) - allowed)
    if disallowed:
        raise BenchmarkError(
            'benchmark CPUs are outside the cgroup CPU allowance: '
            + ','.join(map(str, disallowed))
        )
    if system['allowed_cpus'] != list(management_cpus):
        raise BenchmarkError(
            'benchmark driver is not pinned to its management CPU: '
            + repr(system['allowed_cpus'])
        )
    masks = system['thread_affinity_masks']
    if (
        not masks
        or any(mask['thread_count'] < 1 for mask in masks)
        or any(mask['cpus'] != list(management_cpus) for mask in masks)
    ):
        raise BenchmarkError(
            'every benchmark-driver thread must use the management CPU: ' + repr(masks)
        )

    server_cores = physical_core_keys(server)
    load_cores = physical_core_keys(load)
    management_cores = physical_core_keys(management)
    physical_overlap = (server_cores & load_cores) | (
        management_cores & (server_cores | load_cores)
    )
    if physical_overlap:
        raise BenchmarkError(
            'server/load/management roles overlap on physical cores: '
            + repr(sorted(physical_overlap))
        )

    online = set(system['online_cpus'])
    usable = online & allowed
    load_cpu_set = set(load_cpus)
    incomplete_siblings = {
        entry['cpu']: sorted((set(entry['thread_siblings']) & usable) - load_cpu_set)
        for entry in load['topology']
        if not (set(entry['thread_siblings']) & usable) <= load_cpu_set
    }
    if incomplete_siblings:
        raise BenchmarkError(
            'load CPUs must contain every usable SMT sibling: '
            + repr(incomplete_siblings)
        )


def pin_benchmark_driver(management_cpus: tuple[int, ...] | None) -> None:
    if management_cpus is None:
        return
    try:
        pin_process_threads(management_cpus)
    except (OSError, RuntimeError, ValueError) as error:
        raise BenchmarkError(
            f'failed to pin benchmark-driver threads: {error}'
        ) from error


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def durable_json(path: Path, value: object) -> None:
    """Atomically replace ``path`` with JSON and make the update durable."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f'.{path.name}.{secrets.token_hex(8)}.temporary')
    try:
        with temporary.open('w', encoding='utf-8') as stream:
            stream.write(json.dumps(value, indent=2, sort_keys=True) + '\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _finite_positive_rate(value: object) -> bool:
    return isinstance(value, int | float) and math.isfinite(value) and 0 < value <= 1


def validate_oha_result(
    raw: object,
    expected_status: int = 200,
    max_deadline_aborts: int = 100,
) -> None:
    """Require a fully successful oha run: exclusively expected responses."""
    if not isinstance(raw, dict):
        raise BenchmarkError('oha returned no JSON result')
    summary = raw.get('summary')
    if not isinstance(summary, dict):
        raise BenchmarkError('oha result is missing summary')
    success_rate = summary.get('successRate')
    if success_rate is not None and not _finite_positive_rate(success_rate):
        raise BenchmarkError(f'oha success rate is invalid: {success_rate!r}')
    errors = raw.get('errorDistribution', {})
    unexpected_errors = set(errors) - {'aborted due to deadline'}
    if unexpected_errors:
        raise BenchmarkError(f'oha reported request errors: {errors!r}')
    deadline_aborts = errors.get('aborted due to deadline', 0)
    if not isinstance(deadline_aborts, int) or deadline_aborts > max_deadline_aborts:
        raise BenchmarkError(f'oha reported excessive deadline aborts: {errors!r}')
    statuses = raw.get('statusCodeDistribution')
    if not isinstance(statuses, dict):
        raise BenchmarkError('oha result is missing statusCodeDistribution')
    expected = statuses.get(str(expected_status), statuses.get(expected_status))
    total_responses = sum(statuses.values())
    if not isinstance(expected, int) or expected <= 0 or expected != total_responses:
        raise BenchmarkError(
            f'oha status distribution is not exclusively HTTP {expected_status}: '
            f'{statuses!r}'
        )


def validate_k6_result(raw: object) -> None:
    """Require every k6 WebSocket handshake and exact echo check to pass.

    The handshake and echo success rates are required outright: a summary
    missing either rate fails validation rather than being tolerated.
    """
    if not isinstance(raw, dict):
        raise BenchmarkError('k6 returned no JSON result')
    metrics = raw.get('metrics')
    if not isinstance(metrics, dict):
        raise BenchmarkError('k6 result is missing metrics')
    checks = metrics.get('checks')
    if not isinstance(checks, dict):
        raise BenchmarkError('k6 result is missing the WebSocket handshake check')
    values = checks.get('values', checks)
    if not isinstance(values, dict):
        raise BenchmarkError('k6 handshake check has invalid values')
    failures = values.get('fails', checks.get('fails'))
    passes = values.get('passes', checks.get('passes'))
    rate = values.get('rate', checks.get('value'))
    if failures != 0 or not isinstance(passes, int | float) or passes <= 0 or rate != 1:
        raise BenchmarkError(f'k6 WebSocket handshake checks failed: {checks!r}')
    echo = metrics.get('bench_echo_success')
    if not isinstance(echo, dict):
        raise BenchmarkError('k6 result is missing the exact WebSocket echo check')
    echo_values = echo.get('values', echo)
    if not isinstance(echo_values, dict):
        raise BenchmarkError('k6 exact WebSocket echo check has invalid values')
    echo_failures = echo_values.get('fails', echo.get('fails'))
    echo_passes = echo_values.get('passes', echo.get('passes'))
    echo_rate = echo_values.get('rate', echo.get('value'))
    if (
        echo_failures != 0
        or not isinstance(echo_passes, int | float)
        or echo_passes <= 0
        or echo_rate != 1
    ):
        raise BenchmarkError(f'k6 WebSocket echo checks failed: {echo!r}')
