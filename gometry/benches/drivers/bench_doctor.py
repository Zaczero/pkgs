from __future__ import annotations

import argparse
import csv
import hashlib
import importlib
import importlib.metadata
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
_TOOLS = GOMETRY_ROOT / 'tools'
if str(_TOOLS) not in sys.path:
    sys.path.insert(0, str(_TOOLS))

from _artifact_common import SOURCE_INPUTS_FORMAT
from artifact_content import source_input_digests

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
BUILD_INFO_SCHEMA_VERSION = 1
BUILD_INFO_KEYS = {
    'schema_version',
    'source_inputs_format',
    'source_inputs_sha256',
    'native_source_inputs_format',
    'native_source_inputs_sha256',
    'profile_family',
    'opt_level',
    'debug_assertions',
    'target',
    'target_arch',
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


def _source_input_digests() -> dict[str, str] | None:
    """Use the artifact owner for checkout and embedded-source identities."""
    inputs = (
        GOMETRY_ROOT / 'build.rs',
        GOMETRY_ROOT / 'src',
        GOMETRY_ROOT / 'native',
    )
    if not any(path.exists() for path in inputs):
        return None
    if not all(path.exists() for path in inputs):
        raise AssertionError('Gometry source tree is incomplete')
    return source_input_digests(GOMETRY_ROOT)


def _source_inputs_digest() -> str | None:
    inputs = _source_input_digests()
    return None if inputs is None else inputs['source_inputs_sha256']


def benchmark_source_inputs_identity(
    build_info: Any,
    checkout_source_inputs_sha256: str | None,
    expected_source_inputs_sha256: str | None = None,
) -> str:
    """Bind one benchmark arm to its embedded build inputs.

    Parent-versus-candidate runs may intentionally load artifacts from different
    source revisions. An explicit expected digest is accepted only when the
    loaded extension embeds the same digest; without one, the live checkout
    remains the expected source.
    """
    expected = (
        checkout_source_inputs_sha256
        if expected_source_inputs_sha256 is None
        else expected_source_inputs_sha256
    )
    if (
        not isinstance(expected, str)
        or len(expected) != 64
        or any(character not in '0123456789abcdef' for character in expected)
    ):
        raise ValueError('expected source inputs must be a lowercase SHA-256 digest')
    if not isinstance(build_info, dict):
        raise TypeError('loaded extension has no embedded build information')
    if build_info.get('source_inputs_sha256') != expected:
        raise ValueError(
            'loaded extension source identity does not match the expected '
            f'build inputs: {build_info.get("source_inputs_sha256")!r} != {expected!r}'
        )
    return expected


def _native_source_inputs_digest() -> str | None:
    inputs = _source_input_digests()
    return None if inputs is None else inputs['native_source_inputs_sha256']


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


def _content_digest(path: Path) -> tuple[str, int]:
    """Identify current bytes, including replacements preserving file metadata."""
    payload = path.read_bytes()
    return hashlib.sha256(payload).hexdigest(), len(payload)


def _file_identity(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    try:
        path = path.resolve()
        sha256, size = _content_digest(path)
    except OSError:
        return None
    return {'path': str(path), 'sha256': sha256, 'size_bytes': size}


def _module_identity(name: str) -> dict[str, Any] | None:
    module = sys.modules.get(name)
    path: Path | None = None
    if module is not None:
        value = getattr(module, '__file__', None)
        if isinstance(value, str):
            path = Path(value)
    if path is None:
        try:
            spec = importlib.util.find_spec(name)
        except (ImportError, ModuleNotFoundError, ValueError):
            spec = None
        origin = spec.origin if spec is not None else None
        if isinstance(origin, str) and origin not in {'built-in', 'frozen'}:
            path = Path(origin)
    return _file_identity(path)


def _distribution_owned_paths(distribution: dict[str, Any]) -> set[str] | None:
    private_paths = distribution.get('_record_files')
    if isinstance(private_paths, list):
        return {str(path) for path in private_paths if isinstance(path, str)}
    record = distribution.get('record')
    if not isinstance(record, dict) or distribution.get('record_available') is not True:
        return None
    record_path = record.get('path')
    location = distribution.get('location')
    if not isinstance(record_path, str) or not isinstance(location, str):
        return None
    paths: set[str] = set()
    try:
        with Path(record_path).open(
            newline='', encoding='utf-8', errors='replace'
        ) as stream:
            for row in csv.reader(stream):
                if not row:
                    continue
                entry = Path(row[0])
                owned = entry if entry.is_absolute() else Path(location) / entry
                paths.add(str(owned.resolve()))
    except OSError:
        return None
    return paths


def _distribution_identity(name: str) -> dict[str, Any] | None:
    try:
        distribution = importlib.metadata.distribution(name)
    except importlib.metadata.PackageNotFoundError:
        return None
    location = Path(distribution.locate_file('')).resolve()
    metadata_path: Path | None = None
    record_path: Path | None = None
    dist_info = getattr(distribution, '_path', None)
    if dist_info is not None:
        dist_info = Path(dist_info)
        metadata_path = dist_info / 'METADATA'
        record_path = dist_info / 'RECORD'
    record_available = record_path is not None and record_path.is_file()
    return {
        'name': name,
        'version': distribution.version,
        'location': str(location),
        'metadata': _file_identity(metadata_path),
        'record': _file_identity(record_path),
        'record_available': record_available,
    }


def _native_library_paths() -> list[Path]:
    """Return native files actually mapped by this process on Linux."""
    maps = Path('/proc/self/maps')
    if not maps.is_file():
        return []
    paths: set[Path] = set()
    for line in maps.read_text(encoding='utf-8', errors='replace').splitlines():
        candidate = line.rsplit(' ', 1)[-1]
        if not candidate.startswith('/'):
            continue
        path = Path(candidate)
        if '.so' in path.name or path.suffix.lower() in {'.dylib', '.dll'}:
            paths.add(path)
    return sorted(paths, key=str)


def _loaded_native_libraries() -> list[dict[str, Any]]:
    return [
        identity
        for path in _native_library_paths()
        if (identity := _file_identity(path)) is not None
    ]


def _package_identity(name: str) -> dict[str, Any] | None:
    distribution = _distribution_identity(name)
    module = _module_identity(name)
    if distribution is None or module is None:
        return None
    return {
        'available': True,
        'version': distribution['version'],
        'display_name': PACKAGES[name],
        'module': module,
        'distribution': distribution,
        'native_libraries': [],
    }


_ENGINE_LIBRARY_TOKENS: dict[str, tuple[str, ...]] = {
    'shapely': ('geos',),
    'pyproj': ('proj',),
    'geopandas': ('geos', 'proj'),
}


def implementation_identity(
    row: str,
    extension: dict[str, Any],
    *,
    package_info: dict[str, Any] | None = None,
    loaded_native_libraries: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Attribute module, distribution, and loaded native files to one row."""
    runtime = 'gometry' if row.startswith('gometry.') else 'comparator'
    package = 'gometry' if runtime == 'gometry' else row.split('.', 1)[0]
    package_info = (
        package_info if package_info is not None else _package_identity(package)
    )
    module = package_info.get('module') if package_info else None
    distribution = package_info.get('distribution') if package_info else None
    libraries = (
        loaded_native_libraries
        if loaded_native_libraries is not None
        else _loaded_native_libraries()
    )
    if runtime == 'gometry':
        extension_file = {
            key: extension[key]
            for key in ('path', 'sha256', 'size_bytes')
            if key in extension
        }
        libraries = [extension_file]
        status = 'identified'
    else:
        owned_paths = set()
        distribution_info = package_info.get('distribution') if package_info else None
        if isinstance(distribution_info, dict):
            owned_paths.update(_distribution_owned_paths(distribution_info) or ())
        module_path = module.get('path') if isinstance(module, dict) else None
        engine_tokens = _ENGINE_LIBRARY_TOKENS.get(package, ())
        libraries = [
            item
            for item in libraries
            if item.get('path') in owned_paths
            or item.get('path') == module_path
            or any(
                token in Path(str(item.get('path'))).name.lower()
                for token in engine_tokens
            )
        ]
        native_module = isinstance(module, dict) and any(
            suffix in str(module.get('path', '')).lower()
            for suffix in ('.so', '.dylib', '.dll')
        )
        if native_module and isinstance(module, dict) and module not in libraries:
            libraries.append(module)
        if libraries:
            status = 'identified'
        elif engine_tokens:
            status = 'unavailable'
        else:
            status = 'not-applicable'
    return {
        'runtime': runtime,
        'row': row,
        'module_name': 'gometry._lib' if runtime == 'gometry' else package,
        'module': module,
        'distribution': distribution,
        'native_libraries': libraries,
        'native_library_status': status,
    }


def _extension_identity() -> dict[str, Any]:
    try:
        import gometry._lib as lib

        path = Path(lib.__file__).resolve()
        sha256, size = _content_digest(path)
        build_info = getattr(lib, '__build_info__', None)
        if not isinstance(build_info, dict):
            build_info = None
        return {
            'path': str(path),
            'sha256': sha256,
            'size_bytes': size,
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
    build_info: Any,
    source_inputs_sha256: str | None,
    rustc_host_target: str | None,
    native_source_inputs_sha256: str | None = None,
) -> list[str]:
    if not isinstance(build_info, dict):
        return ['loaded gometry extension has no private build-info record']
    missing = sorted(BUILD_INFO_KEYS - build_info.keys())
    if missing:
        return ['gometry extension build-info is missing: ' + ', '.join(missing)]
    extra = sorted(build_info.keys() - BUILD_INFO_KEYS)
    if extra:
        return [
            'gometry extension build-info has unexpected fields: ' + ', '.join(extra)
        ]
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
    if build_info['native_source_inputs_format'] != SOURCE_INPUTS_FORMAT:
        failures.append(
            'gometry extension uses an unsupported native source-input identity format: '
            f'{build_info["native_source_inputs_format"]!r}'
        )
    if not isinstance(build_info['native_source_inputs_sha256'], str):
        failures.append('gometry extension native source-input digest is malformed')
    elif (
        native_source_inputs_sha256 is not None
        and build_info['native_source_inputs_sha256'] != native_source_inputs_sha256
    ):
        failures.append(
            'loaded gometry extension has a stale native source-input identity: '
            f'{build_info["native_source_inputs_sha256"]!r} != '
            f'current {native_source_inputs_sha256!r}'
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
    if not isinstance(build_info['target_arch'], str) or not build_info['target_arch']:
        failures.append('gometry extension target architecture is malformed')
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


def _read_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding='utf-8').strip()
    except OSError:
        return None


def parse_cpu_topology(root: Path = Path('/sys/devices/system/cpu')) -> dict[str, Any]:
    """Parse CPU/core/socket/SMT facts without assuming CPU numbering."""
    cpus: dict[str, Any] = {}
    for cpu_path in sorted(root.glob('cpu[0-9]*'), key=lambda path: path.name):
        try:
            cpu = int(cpu_path.name.removeprefix('cpu'))
        except ValueError:
            continue
        topology = cpu_path / 'topology'
        sibling = _read_text(topology / 'thread_siblings_list')
        core_cpus = _read_text(topology / 'core_cpus_list')
        cpus[str(cpu)] = {
            'cpu': cpu,
            'online': _read_text(cpu_path / 'online') != '0',
            'socket': _read_text(topology / 'physical_package_id'),
            'core': _read_text(topology / 'core_id'),
            'thread_siblings': sorted(_cpu_list(sibling)) if sibling else None,
            'core_cpus': sorted(_cpu_list(core_cpus)) if core_cpus else None,
        }
    return {
        'available': bool(cpus),
        'cpus': cpus,
        'allowed_cpus': sorted(os.sched_getaffinity(0)),
    }


def parse_cache_topology(
    root: Path = Path('/sys/devices/system/cpu'),
) -> list[dict[str, Any]]:
    caches: list[dict[str, Any]] = []
    seen: set[str] = set()
    for cpu_path in sorted(root.glob('cpu[0-9]*'), key=lambda path: path.name):
        for cache in sorted(
            (cpu_path / 'cache').glob('index*'), key=lambda path: path.name
        ):
            identity = str(cache.resolve())
            if identity in seen:
                continue
            seen.add(identity)
            shared = _read_text(cache / 'shared_cpu_list')
            caches.append({
                'level': _read_text(cache / 'level'),
                'type': _read_text(cache / 'type'),
                'size': _read_text(cache / 'size'),
                'shared_cpus': sorted(_cpu_list(shared)) if shared else None,
            })
    return caches


def parse_numa_topology(
    root: Path = Path('/sys/devices/system/node'),
) -> dict[str, Any]:
    nodes: dict[str, Any] = {}
    for node_path in sorted(root.glob('node[0-9]*'), key=lambda path: path.name):
        name = node_path.name.removeprefix('node')
        cpulist = _read_text(node_path / 'cpulist')
        nodes[name] = {
            'cpus': sorted(_cpu_list(cpulist)) if cpulist else None,
            'memory_total_kib': _read_meminfo_value(node_path / 'meminfo', 'MemTotal'),
            'memory_free_kib': _read_meminfo_value(node_path / 'meminfo', 'MemFree'),
        }
    return {'available': bool(nodes), 'nodes': nodes}


def _read_meminfo_value(path: Path, key: str) -> int | None:
    text = _read_text(path)
    if text is None:
        return None
    for line in text.splitlines():
        name, separator, value = line.partition(':')
        if separator and (name == key or name.endswith(f' {key}')):
            try:
                return int(value.strip().split()[0])
            except (IndexError, ValueError):
                return None
    return None


def _proc_cgroup_path(proc_cgroup: Path = Path('/proc/self/cgroup')) -> str | None:
    text = _read_text(proc_cgroup)
    if text is None:
        return None
    for line in text.splitlines():
        hierarchy, separator, path = line.partition('::')
        if separator and hierarchy == '0':
            return path
    return None


def parse_cgroup_v2(
    proc_cgroup: Path = Path('/proc/self/cgroup'),
    mount: Path = Path('/sys/fs/cgroup'),
) -> dict[str, Any]:
    """Record cgroup v2 limits and pressure, or explicit unavailability."""
    relative = _proc_cgroup_path(proc_cgroup)
    if relative is None:
        return {
            'available': False,
            'path': None,
            'reason': 'unified cgroup v2 path unavailable',
        }
    path = (mount / relative.lstrip('/')).resolve()
    if not path.is_dir():
        return {
            'available': False,
            'path': relative,
            'reason': 'cgroup v2 mount unavailable',
        }
    names = (
        'memory.max',
        'memory.high',
        'memory.current',
        'memory.events',
        'cpu.max',
        'cpu.weight',
        'cpu.stat',
        'cpuset.cpus.effective',
        'cpuset.mems.effective',
        'memory.pressure',
        'cpu.pressure',
    )
    values = {name: _read_text(path / name) for name in names}
    return {'available': True, 'path': relative, 'mount': str(mount), 'values': values}


def parse_memory_placement(
    proc_status: Path = Path('/proc/self/status'),
    proc_numa_maps: Path = Path('/proc/self/numa_maps'),
) -> dict[str, Any]:
    """Summarize observed NUMA page placement; this is not an affinity claim."""
    status = _read_text(proc_status)
    allowed: str | None = None
    if status is not None:
        for line in status.splitlines():
            if line.startswith('Mems_allowed_list:'):
                allowed = line.partition(':')[2].strip() or None
                break
    pages: dict[str, int] = {}
    policies: set[str] = set()
    if proc_numa_maps.is_file():
        text = _read_text(proc_numa_maps)
        if text is not None:
            for line in text.splitlines():
                fields = line.split()
                if len(fields) > 1 and '=' not in fields[1]:
                    policies.add(fields[1])
                for token in line.split():
                    if token.startswith('N') and '=' in token:
                        node, value = token[1:].split('=', 1)
                        try:
                            pages[node] = pages.get(node, 0) + int(value)
                        except ValueError:
                            continue
    return {
        'available': bool(pages),
        'allowed_memory_nodes': allowed,
        'memory_policy': sorted(policies) if policies else None,
        'observed_pages_by_node': pages,
        'source': str(proc_numa_maps) if pages else None,
        'reason': None if pages else 'NUMA page-placement observations unavailable',
    }


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


# Every package a benchmark row loads, by import name (also its distribution
# name), with the label its comparator rows carry.
PACKAGES: dict[str, str] = {
    'gometry': 'gometry',
    'numpy': 'NumPy',
    'shapely': 'Shapely',
    'pyproj': 'pyproj',
    'h3': 'h3-py',
    'geopandas': 'GeoPandas',
    'pyarrow': 'pyarrow',
    'mercantile': 'Mercantile',
    'pygeohash': 'pygeohash',
}
_PACKAGE_BY_LABEL = {label: name for name, label in PACKAGES.items()}


def packages_required_by_operations(operations: Any) -> set[str]:
    """Derive required import names from selected public operations.

    Always includes gometry/numpy. Optional competitor packages are
    required only when a selected public row names them.
    """
    if operations is None:
        # Standalone doctor / full public release surface.
        return set(PACKAGES)
    required = {'gometry', 'numpy'}
    for op in operations:
        gometry = getattr(op, 'gometry', '') or ''
        if 'from_arrow' in gometry or 'arrow' in gometry:
            required.add('pyarrow')
        competitor = getattr(op, 'competitor', None)
        if competitor:
            prefix = str(competitor).split('.', 1)[0]
            if prefix in PACKAGES:
                required.add(prefix)
        label = getattr(op, 'competitor_label', None)
        if label:
            for token in str(label).split(' + '):
                token = token.strip()
                if token in _PACKAGE_BY_LABEL:
                    required.add(_PACKAGE_BY_LABEL[token])
    return required


def collect(
    *,
    operations: Any = None,
    expected_source_inputs_sha256: str | None = None,
    expected_native_source_inputs_sha256: str | None = None,
) -> dict[str, Any]:
    required_runtime_modules = packages_required_by_operations(operations)
    for name in sorted(required_runtime_modules):
        try:
            importlib.import_module(name)
        except (ImportError, ModuleNotFoundError):
            pass
    packages: dict[str, dict[str, Any]] = {}
    for name, label in PACKAGES.items():
        available = importlib.util.find_spec(name) is not None
        packages[name] = {
            'available': available,
            'version': _package_version(name),
            'display_name': label,
        }
        if available:
            identity = _package_identity(name)
            if identity is not None:
                packages[name].update(identity)
    contention = collect_contention()
    frequencies = _per_cpu_frequency_state()
    extension = _extension_identity()
    uv_lock = _uv_lock_identity()
    rustc_host_target = _rustc_host_target()
    loaded_native_libraries = _loaded_native_libraries()
    source_digests = _source_input_digests()
    source_inputs_sha256 = (
        None if source_digests is None else source_digests['source_inputs_sha256']
    )
    native_source_inputs_sha256 = (
        None
        if source_digests is None
        else source_digests['native_source_inputs_sha256']
    )
    build_info = extension.get('build_info')
    if expected_source_inputs_sha256 is not None:
        source_inputs_sha256 = benchmark_source_inputs_identity(
            build_info,
            source_inputs_sha256,
            expected_source_inputs_sha256,
        )
    if expected_native_source_inputs_sha256 is not None:
        if (
            not isinstance(build_info, dict)
            or build_info.get('native_source_inputs_sha256')
            != expected_native_source_inputs_sha256
        ):
            raise ValueError(
                'loaded extension native source identity does not match the '
                'validated receipt'
            )
        native_source_inputs_sha256 = expected_native_source_inputs_sha256
    cpu_topology = parse_cpu_topology()
    numa_topology = parse_numa_topology()
    cgroup = parse_cgroup_v2()
    memory_placement = parse_memory_placement()
    metadata: dict[str, Any] = {
        'harness_commit': _run(['git', 'rev-parse', 'HEAD']),
        'git_dirty_build_inputs': _git_build_inputs_dirty(),
        'source_inputs_format': SOURCE_INPUTS_FORMAT,
        'source_inputs_sha256': source_inputs_sha256,
        'native_source_inputs_sha256': native_source_inputs_sha256,
        'uv_lock': uv_lock,
        'python_executable': str(Path(sys.executable).resolve()),
        'python_prefix': sys.prefix,
        'rustc': _run(['rustc', '--version']),
        'rustc_host_target': rustc_host_target,
        'cargo': _run(['cargo', '--version']),
        'python': sys.version.split()[0],
        'python_version': sys.version,
        'python_build': platform.python_build(),
        'python_implementation': platform.python_implementation(),
        'python_debug': bool(sysconfig.get_config_var('Py_DEBUG')),
        'python_cache_tag': getattr(sys.implementation, 'cache_tag', None),
        'python_ext_suffix': sysconfig.get_config_var('EXT_SUFFIX'),
        'python_gil_disabled': bool(sysconfig.get_config_var('Py_GIL_DISABLED')),
        'python_gil_enabled': (
            bool(sys._is_gil_enabled())
            if callable(getattr(sys, '_is_gil_enabled', None))
            else True
        ),
        'platform': platform.platform(),
        'kernel': platform.release(),
        'cpu_model': _cpu_model(),
        'cpu_count': contention['metadata']['cpu_count'],
        'cpu_governor': _governor(),
        'cpu_affinity': sorted(os.sched_getaffinity(0)),
        'isolated_cpus': _isolated_cpus(),
        'cpu_frequency_state': frequencies,
        'cpu_topology': cpu_topology,
        'cache_topology': parse_cache_topology(),
        'numa_topology': numa_topology,
        'allowed_memory_nodes': memory_placement.get('allowed_memory_nodes'),
        'memory_policy': memory_placement.get('memory_policy'),
        'memory_placement': memory_placement,
        'cgroup_v2': cgroup,
        'load_average': contention['metadata']['load_average'],
        'top_cpu_processes': contention['metadata']['top_cpu_processes'],
        'packages': packages,
        'extension': extension,
        'loaded_native_libraries': loaded_native_libraries,
        'native_library_identity': {
            'available': bool(loaded_native_libraries),
            'source': '/proc/self/maps' if Path('/proc/self/maps').is_file() else None,
            'reason': None
            if loaded_native_libraries
            else 'loaded native-library mapping is unavailable',
        },
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
            metadata['native_source_inputs_sha256'],
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
    required = required_runtime_modules
    for package in sorted(required):
        info = packages[package]
        if not info['available']:
            if package in {'gometry', 'numpy'}:
                hard_failures.append(
                    f'required Python package {package!r} is not importable'
                )
            else:
                hard_failures.append(
                    f'competitor package {package!r} ({PACKAGES[package]}) is not importable'
                )
        elif not info['version']:
            hard_failures.append(
                f'package {package!r} has no installed distribution version'
            )
    if {'shapely', 'geopandas'} & required and not metadata['geos_version']:
        hard_failures.append(
            'GEOS version is unavailable for the selected benchmark packages'
        )
    if {'pyproj', 'geopandas'} & required and not metadata['proj_version']:
        hard_failures.append(
            'PROJ version is unavailable for the selected benchmark packages'
        )
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
