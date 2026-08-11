from __future__ import annotations

import argparse
import importlib.util
import json
import os
import platform
import subprocess
import sys
from pathlib import Path
from typing import Any

GOMETRY_ROOT = Path(__file__).resolve().parents[2]
REPO_ROOT = GOMETRY_ROOT.parent
LOAD_BUSY_MIN = 1.0
LOAD_BUSY_MAX = 2.0
PROCESS_CPU_WARN_PERCENT = 50.0
PROCESS_CENSUS_LIMIT = 5
BUILD_INPUT_PATHS = (
    'gometry',
    '.cargo/config.toml',
    'Cargo.toml',
    'Cargo.lock',
)


def _run(args: list[str], cwd: Path = GOMETRY_ROOT) -> str | None:
    try:
        result = subprocess.run(
            args,
            cwd=cwd,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
    except FileNotFoundError:
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip()


def _git_build_inputs_dirty() -> bool | None:
    result = _run(
        [
            'git',
            'status',
            '--porcelain',
            '--untracked-files=all',
            '--',
            *BUILD_INPUT_PATHS,
        ],
        cwd=REPO_ROOT,
    )
    if result is None:
        return None
    return bool(result)


def _cpu_model() -> str | None:
    cpuinfo = Path('/proc/cpuinfo')
    if cpuinfo.exists():
        for line in cpuinfo.read_text(encoding='utf-8', errors='replace').splitlines():
            if line.startswith('model name'):
                return line.partition(':')[2].strip()
    return platform.processor() or None


def _governor() -> str | None:
    policy = Path('/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor')
    if not policy.exists():
        return None
    return policy.read_text(encoding='utf-8').strip()


def _read_cpu_state(base: Path, name: str) -> str | None:
    path = base / name
    return path.read_text(encoding='utf-8').strip() if path.exists() else None


def _per_cpu_frequency_state() -> dict[str, dict[str, str | None]]:
    state: dict[str, dict[str, str | None]] = {}
    for cpu in sorted(os.sched_getaffinity(0)):
        base = Path(f'/sys/devices/system/cpu/cpu{cpu}/cpufreq')
        state[str(cpu)] = {
            'governor': _read_cpu_state(base, 'scaling_governor'),
            'frequency_khz': _read_cpu_state(base, 'scaling_cur_freq'),
            'driver': _read_cpu_state(base, 'scaling_driver'),
        }
    return state


def _isolated_cpus() -> str | None:
    path = Path('/sys/devices/system/cpu/isolated')
    return path.read_text(encoding='utf-8').strip() if path.exists() else None


def _cpu_list(text: str) -> set[int]:
    cpus: set[int] = set()
    for part in text.strip().split(','):
        if not part:
            continue
        endpoints = part.split('-', 1)
        start = int(endpoints[0])
        stop = int(endpoints[-1])
        cpus.update(range(start, stop + 1))
    return cpus


def select_benchmark_cpu(requested: int | None) -> tuple[int, str]:
    """Select one allowed CPU, preferring kernel-isolated CPUs."""
    allowed = set(os.sched_getaffinity(0))
    isolated_text = _isolated_cpus() or ''
    isolated = _cpu_list(isolated_text) & allowed
    if requested is not None:
        if requested not in allowed:
            raise SystemExit(
                f'CPU {requested} is outside this process affinity: {sorted(allowed)}'
            )
        source = (
            'explicit-kernel-isolated'
            if requested in isolated
            else 'explicit-not-isolated'
        )
        return requested, source
    if isolated:
        return max(isolated), 'kernel-isolated'
    return max(allowed), 'affinity-fallback-not-isolated'


def _package_version(name: str) -> str | None:
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version(name)
    except PackageNotFoundError:
        return None


def _load_average(cpu_count: int | None) -> dict[str, float] | None:
    try:
        one, five, fifteen = os.getloadavg()
    except (AttributeError, OSError):
        return None
    ncpu = max(1, cpu_count or 1)
    # Benchmarks are sensitive to scheduler contention well before a workstation
    # is saturated. Cap the threshold at 2 runnable tasks so high-core machines
    # still flag real concurrent load instead of hiding it behind CPU count.
    threshold = max(LOAD_BUSY_MIN, min(LOAD_BUSY_MAX, 0.5 * ncpu))
    return {'1m': one, '5m': five, '15m': fifteen, 'busy_threshold_1m': threshold}


def _top_cpu_processes(
    limit: int = PROCESS_CENSUS_LIMIT, *, exclude_pgids: set[int] | None = None
) -> list[dict[str, Any]]:
    output = _run(
        ['ps', '-eo', 'pid=,ppid=,pgid=,pcpu=,comm=', '--sort=-pcpu'], cwd=GOMETRY_ROOT
    )
    if output is None:
        return []
    current_pid = os.getpid()
    exclude_pgids = exclude_pgids or set()
    processes: list[dict[str, Any]] = []
    for line in output.splitlines():
        parts = line.strip().split(maxsplit=4)
        if len(parts) < 5:
            continue
        pid_text, ppid_text, pgid_text, cpu_text, command = parts
        try:
            pid = int(pid_text)
            ppid = int(ppid_text)
            pgid = int(pgid_text)
            cpu_percent = float(cpu_text)
        except ValueError:
            continue
        if current_pid in {pid, ppid}:
            continue
        if pgid in exclude_pgids:
            continue
        processes.append({
            'pid': pid,
            'ppid': ppid,
            'pgid': pgid,
            'cpu_percent': cpu_percent,
            'command': command,
        })
        if len(processes) >= limit:
            break
    return processes


def _format_processes(processes: list[dict[str, Any]]) -> str:
    if not processes:
        return 'none observed'
    return ', '.join(
        f'{item["command"]}[{item["pid"]}]={item["cpu_percent"]:.1f}%'
        for item in processes
    )


def collect_contention(*, exclude_pgids: set[int] | None = None) -> dict[str, Any]:
    cpu_count = os.cpu_count()
    load_average = _load_average(cpu_count)
    top_cpu_processes = _top_cpu_processes(exclude_pgids=exclude_pgids)
    warnings: list[str] = []
    if (
        load_average is not None
        and load_average['1m'] > load_average['busy_threshold_1m']
    ):
        warnings.append(
            'system load average is high for benchmark timing: '
            f'1m={load_average["1m"]:.2f} > {load_average["busy_threshold_1m"]:.2f} '
            f'(cpu_count={cpu_count}); top CPU processes: {_format_processes(top_cpu_processes)}'
        )
    busy_processes = [
        process
        for process in top_cpu_processes
        if process['cpu_percent'] >= PROCESS_CPU_WARN_PERCENT
    ]
    if busy_processes:
        warnings.append(
            f'CPU contention observed from non-self processes over {PROCESS_CPU_WARN_PERCENT:.0f}%: '
            f'{_format_processes(busy_processes)}'
        )
    return {
        'metadata': {
            'cpu_count': cpu_count,
            'load_average': load_average,
            'top_cpu_processes': top_cpu_processes,
        },
        'warnings': warnings,
    }


# Full inventory for doctor output (import name → distribution name).
PACKAGE_INVENTORY: dict[str, str] = {
    'gometry': 'gometry',
    'numpy': 'numpy',
    'pyperf': 'pyperf',
    'shapely': 'shapely',
    'pyproj': 'pyproj',
    'h3': 'h3',
    's2sphere': 's2sphere',
    'geopandas': 'geopandas',
    'pyarrow': 'pyarrow',
    'mercantile': 'mercantile',
}

# Display names for competitor libraries (summarizer still prefers competitor_label).
PACKAGE_DISPLAY_NAMES: dict[str, str] = {
    'gometry': 'gometry',
    'numpy': 'NumPy',
    'pyperf': 'pyperf',
    'shapely': 'Shapely',
    'pyproj': 'pyproj',
    'h3': 'h3-py',
    's2sphere': 's2sphere',
    'geopandas': 'GeoPandas',
    'pyarrow': 'pyarrow',
    'mercantile': 'Mercantile',
}

# Competitor-label tokens → import names (multi-lib labels split on ' + ').
_LABEL_PACKAGE: dict[str, str] = {
    'Shapely': 'shapely',
    'GeoPandas': 'geopandas',
    'pyproj': 'pyproj',
    'h3-py': 'h3',
    'Mercantile': 'mercantile',
    's2sphere': 's2sphere',
}

# Row-name prefix → import name for internal/fallback pairing.
_PREFIX_PACKAGE: dict[str, str] = {
    'shapely': 'shapely',
    'pyproj': 'pyproj',
    'h3': 'h3',
    's2sphere': 's2sphere',
    'geopandas': 'geopandas',
    'mercantile': 'mercantile',
    'rtree': 'rtree',
}


def packages_required_by_operations(operations: Any) -> set[str]:
    """Derive required import names from selected public operations.

    Always includes gometry/numpy/pyperf. Internal-only packages such as
    s2sphere are required only when a selected competitor row names them —
    the public S2 gometry-only row does not pull s2sphere in.
    """
    required = {'gometry', 'numpy', 'pyperf'}
    if operations is None:
        # Standalone doctor / full public RELEASE surface (no s2sphere).
        required.update({
            'shapely',
            'pyproj',
            'h3',
            'geopandas',
            'pyarrow',
            'mercantile',
        })
        return required
    for op in operations:
        gometry = getattr(op, 'gometry', '') or ''
        if 'from_arrow' in gometry or 'arrow' in gometry:
            required.add('pyarrow')
        competitor = getattr(op, 'competitor', None)
        if competitor:
            prefix = str(competitor).split('.', 1)[0]
            if prefix in _PREFIX_PACKAGE:
                required.add(_PREFIX_PACKAGE[prefix])
        label = getattr(op, 'competitor_label', None)
        if label:
            for token in str(label).split(' + '):
                token = token.strip()
                if token in _LABEL_PACKAGE:
                    required.add(_LABEL_PACKAGE[token])
    return required


def collect(*, operations: Any = None) -> dict[str, Any]:
    packages = {
        name: {
            'available': importlib.util.find_spec(name) is not None,
            'version': _package_version(dist),
            'display_name': PACKAGE_DISPLAY_NAMES.get(name, name),
        }
        for name, dist in PACKAGE_INVENTORY.items()
    }
    contention = collect_contention()
    frequencies = _per_cpu_frequency_state()
    metadata: dict[str, Any] = {
        'git_commit': _run(['git', 'rev-parse', 'HEAD']),
        'git_dirty_build_inputs': _git_build_inputs_dirty(),
        'rustc': _run(['rustc', '--version']),
        'cargo': _run(['cargo', '--version']),
        'python': sys.version.split()[0],
        'platform': platform.platform(),
        'kernel': platform.release(),
        'cpu_model': _cpu_model(),
        'cpu_count': contention['metadata']['cpu_count'],
        'cpu_governor': _governor(),
        'cpu_affinity': sorted(os.sched_getaffinity(0)),
        'isolated_cpus': _isolated_cpus(),
        'cpu_frequency_state': frequencies,
        'load_average': contention['metadata']['load_average'],
        'top_cpu_processes': contention['metadata']['top_cpu_processes'],
        'packages': packages,
        'package_display_names': dict(PACKAGE_DISPLAY_NAMES),
    }
    warnings = list(contention['warnings'])
    if metadata['git_dirty_build_inputs']:
        warnings.append(
            'gometry or its workspace Cargo build inputs have uncommitted changes'
        )
    if metadata['cpu_governor'] not in {None, 'performance'}:
        warnings.append(
            f"CPU governor is {metadata['cpu_governor']!r}, not 'performance'"
        )
    non_performance = {
        cpu: value['governor']
        for cpu, value in frequencies.items()
        if value['governor'] not in {None, 'performance'}
    }
    if non_performance:
        warnings.append(
            f'non-performance governors in the allowed CPU set: {non_performance}'
        )
    required = packages_required_by_operations(operations)
    for package in sorted(required):
        info = packages.get(package)
        if info is None:
            # Optional inventory extension (e.g. rtree on internal filters).
            available = importlib.util.find_spec(package) is not None
            if not available:
                warnings.append(f'competitor package {package!r} is not importable')
            continue
        if not info['available']:
            if package in {'gometry', 'numpy', 'pyperf'}:
                warnings.append(
                    f'required Python package {package!r} is not importable'
                )
            else:
                display = PACKAGE_DISPLAY_NAMES.get(package, package)
                warnings.append(
                    f'competitor package {package!r} ({display}) is not importable'
                )
    return {'metadata': metadata, 'warnings': warnings}


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Report benchmark environment metadata.'
    )
    parser.add_argument(
        '--json', action='store_true', help='write JSON instead of text'
    )
    args = parser.parse_args()
    report = collect()
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
        return
    print('Benchmark doctor')
    print('================')
    for key, value in report['metadata'].items():
        if key in {'packages', 'top_cpu_processes'}:
            continue
        print(f'{key}: {value}')
    print('top_cpu_processes:')
    for process in report['metadata']['top_cpu_processes']:
        print(
            f'  pid={process["pid"]} ppid={process["ppid"]} pgid={process["pgid"]} '
            f'cpu={process["cpu_percent"]:.1f}% command={process["command"]}'
        )
    print('packages:')
    for name, info in report['metadata']['packages'].items():
        version = info['version'] if info['version'] is not None else 'unknown'
        display = info.get('display_name', name)
        print(f'  {name} ({display}): available={info["available"]} version={version}')
    if report['warnings']:
        print('warnings:')
        for warning in report['warnings']:
            print(f'  - {warning}')


if __name__ == '__main__':
    main()
