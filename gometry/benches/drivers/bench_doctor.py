from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import platform
import subprocess
import sys
import sysconfig
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
SOURCE_INPUTS_FORMAT = 'sha256-path-nul-content-nul-v1'
BUILD_INFO_SCHEMA_VERSION = 1
BUILD_INFO_KEYS = {
    'schema_version',
    'source_inputs_format',
    'source_inputs_sha256',
    'profile_family',
    'opt_level',
    'debug',
    'debug_assertions',
    'target',
    'target_features',
}


def selected_cpu_governor(
    frequency_state: dict[str, dict[str, str | None]], cpu: int
) -> str | None:
    return frequency_state[str(cpu)]['governor']


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


def _source_inputs_digest() -> str | None:
    """Digest the deterministic inputs Cargo can receive in a checkout or sdist."""
    files: list[tuple[str, Path]] = [
        ('Cargo.toml', REPO_ROOT / 'Cargo.toml'),
        ('Cargo.lock', REPO_ROOT / 'Cargo.lock'),
        ('rust-toolchain.toml', REPO_ROOT / 'rust-toolchain.toml'),
        ('gometry/Cargo.toml', GOMETRY_ROOT / 'Cargo.toml'),
        ('gometry/build.rs', GOMETRY_ROOT / 'build.rs'),
    ]
    pyproject = GOMETRY_ROOT / 'pyproject.toml'
    if not pyproject.is_file():
        pyproject = REPO_ROOT / 'pyproject.toml'
    files.append(('pyproject.toml', pyproject))
    config = REPO_ROOT / '.cargo' / 'config.toml'
    if config.is_file():
        files.append(('.cargo/config.toml', config))
    for directory in ('src', 'native'):
        root = GOMETRY_ROOT / directory
        if not root.is_dir():
            return None
        for path in root.rglob('*'):
            if '.git' in path.parts or not path.is_file():
                continue
            files.append((f'gometry/{path.relative_to(GOMETRY_ROOT)}', path))
    files.sort(key=lambda item: item[0])
    digest = hashlib.sha256()
    for name, path in files:
        try:
            payload = path.read_bytes()
        except OSError:
            return None
        digest.update(name.encode('utf-8'))
        digest.update(b'\0')
        digest.update(payload)
        digest.update(b'\0')
    return digest.hexdigest()


def _uv_lock_identity() -> dict[str, Any]:
    candidates = (REPO_ROOT / 'uv.lock', GOMETRY_ROOT / 'uv.lock')
    for path in candidates:
        if path.is_file():
            return {
                'path': str(path),
                'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                'size_bytes': path.stat().st_size,
            }
    return {'path': None, 'sha256': None, 'size_bytes': None}


def _extension_identity() -> dict[str, Any]:
    try:
        import gometry._lib as lib

        path = Path(lib.__file__).resolve()
        payload = path.read_bytes()
        build_info = getattr(lib, '__build_info__', None)
        if not isinstance(build_info, dict):
            build_info = None
        return {
            'path': str(path),
            'sha256': hashlib.sha256(payload).hexdigest(),
            'size_bytes': path.stat().st_size,
            'build_info': build_info,
        }
    except (ImportError, OSError, AttributeError) as exc:
        return {
            'path': None,
            'sha256': None,
            'size_bytes': None,
            'build_info': None,
            'error': f'{type(exc).__name__}: {exc}',
        }


def _rustc_host_target() -> str | None:
    output = _run(['rustc', '-vV'])
    if output is None:
        return None
    for line in output.splitlines():
        key, separator, value = line.partition(':')
        if separator and key == 'host':
            return value.strip() or None
    return None


def _build_info_failures(
    build_info: Any, source_inputs_sha256: str | None, rustc_host_target: str | None
) -> list[str]:
    if not isinstance(build_info, dict):
        return ['loaded gometry extension has no private build-info record']
    missing = sorted(BUILD_INFO_KEYS - build_info.keys())
    if missing:
        return ['gometry extension build-info is missing: ' + ', '.join(missing)]
    failures: list[str] = []
    if build_info['schema_version'] != BUILD_INFO_SCHEMA_VERSION:
        failures.append(
            'gometry extension build-info schema is unsupported: '
            f'{build_info["schema_version"]!r}'
        )
    if build_info['source_inputs_format'] != SOURCE_INPUTS_FORMAT:
        failures.append(
            'gometry extension uses an unsupported source-input identity format: '
            f'{build_info["source_inputs_format"]!r}'
        )
    if not source_inputs_sha256:
        failures.append('relevant source/build input digest is unavailable')
    elif build_info['source_inputs_sha256'] != source_inputs_sha256:
        failures.append(
            'loaded gometry extension is stale: embedded source-input identity '
            f'{build_info["source_inputs_sha256"]!r} != current {source_inputs_sha256!r}'
        )
    if build_info['profile_family'] != 'release':
        failures.append(
            'loaded gometry extension is not a release-family build: '
            f'{build_info["profile_family"]!r}'
        )
    if build_info['opt_level'] != '3':
        failures.append(
            'loaded gometry extension has unexpected optimization level: '
            f'{build_info["opt_level"]!r}'
        )
    if not isinstance(build_info['debug'], bool):
        failures.append('gometry extension debug-info flag is malformed')
    if build_info['debug_assertions'] is not False:
        failures.append('loaded gometry extension has debug assertions enabled')
    if rustc_host_target is None:
        failures.append('current Rust host target is unavailable')
    elif build_info['target'] != rustc_host_target:
        failures.append(
            'loaded gometry extension targets '
            f'{build_info["target"]!r}, current Rust host is {rustc_host_target!r}'
        )
    if not isinstance(build_info['target_features'], str):
        failures.append('gometry extension target features are malformed')
    return failures


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


def _geos_version() -> str | None:
    try:
        import shapely

        value = getattr(shapely, 'geos_version_string', None)
        return str(value) if value is not None else None
    except (ImportError, AttributeError):
        return None


def _proj_version() -> str | None:
    try:
        import pyproj

        value = getattr(pyproj, 'proj_version_str', None)
        return str(value) if value is not None else None
    except (ImportError, AttributeError):
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
    'shapely': 'shapely',
    'pyproj': 'pyproj',
    'h3': 'h3',
    's2sphere': 's2sphere',
    'geopandas': 'geopandas',
    'pyarrow': 'pyarrow',
    'mercantile': 'mercantile',
    'pygeohash': 'pygeohash',
}

# Display names for competitor libraries (summarizer still prefers competitor_label).
PACKAGE_DISPLAY_NAMES: dict[str, str] = {
    'gometry': 'gometry',
    'numpy': 'NumPy',
    'shapely': 'Shapely',
    'pyproj': 'pyproj',
    'h3': 'h3-py',
    's2sphere': 's2sphere',
    'geopandas': 'GeoPandas',
    'pyarrow': 'pyarrow',
    'mercantile': 'Mercantile',
    'pygeohash': 'pygeohash',
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

# Competitor row-name prefix → import name.
_PREFIX_PACKAGE: dict[str, str] = {
    'shapely': 'shapely',
    'pyproj': 'pyproj',
    'h3': 'h3',
    's2sphere': 's2sphere',
    'geopandas': 'geopandas',
    'mercantile': 'mercantile',
    'pygeohash': 'pygeohash',
    'rtree': 'rtree',
}


def packages_required_by_operations(operations: Any) -> set[str]:
    """Derive required import names from selected public operations.

    Always includes gometry/numpy. Optional competitor packages are
    required only when a selected public row names them.
    """
    required = {'gometry', 'numpy'}
    if operations is None:
        # Standalone doctor / full public RELEASE surface.
        required.update({
            'shapely',
            'pyproj',
            'h3',
            'geopandas',
            'pyarrow',
            'mercantile',
            'pygeohash',
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
    extension = _extension_identity()
    uv_lock = _uv_lock_identity()
    rustc_host_target = _rustc_host_target()
    metadata: dict[str, Any] = {
        'git_commit': _run(['git', 'rev-parse', 'HEAD']),
        'git_dirty_build_inputs': _git_build_inputs_dirty(),
        'source_inputs_format': SOURCE_INPUTS_FORMAT,
        'source_inputs_sha256': _source_inputs_digest(),
        'uv_lock': uv_lock,
        'python_executable': str(Path(sys.executable).resolve()),
        'rustc': _run(['rustc', '--version']),
        'rustc_host_target': rustc_host_target,
        'cargo': _run(['cargo', '--version']),
        'python': sys.version.split()[0],
        'python_version': sys.version,
        'python_build': platform.python_build(),
        'python_implementation': platform.python_implementation(),
        'python_debug': bool(sysconfig.get_config_var('Py_DEBUG')),
        'python_cache_tag': getattr(sys.implementation, 'cache_tag', None),
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
        'extension': extension,
        'geos_version': _geos_version(),
        'proj_version': _proj_version(),
    }
    warnings = list(contention['warnings'])
    hard_failures: list[str] = []
    if metadata['git_dirty_build_inputs']:
        hard_failures.append(
            'gometry or its workspace Cargo build inputs have uncommitted changes'
        )
    hard_failures.extend(
        _build_info_failures(
            extension.get('build_info'),
            metadata['source_inputs_sha256'],
            rustc_host_target,
        )
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
                hard_failures.append(f'competitor package {package!r} is not importable')
            continue
        if not info['available']:
            if package in {'gometry', 'numpy'}:
                hard_failures.append(
                    f'required Python package {package!r} is not importable'
                )
            else:
                display = PACKAGE_DISPLAY_NAMES.get(package, package)
                hard_failures.append(
                    f'competitor package {package!r} ({display}) is not importable'
                )
        elif not info['version']:
            hard_failures.append(
                f'package {package!r} has no installed distribution version'
            )
    if {'shapely', 'geopandas'} & required and not metadata['geos_version']:
        hard_failures.append('GEOS version is unavailable for the selected benchmark packages')
    if {'pyproj', 'geopandas'} & required and not metadata['proj_version']:
        hard_failures.append('PROJ version is unavailable for the selected benchmark packages')
    return {
        'metadata': metadata,
        'warnings': warnings,
        'hard_failures': hard_failures,
    }


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
    if report['hard_failures']:
        print('hard_failures:')
        for failure in report['hard_failures']:
            print(f'  - {failure}')


if __name__ == '__main__':
    main()
