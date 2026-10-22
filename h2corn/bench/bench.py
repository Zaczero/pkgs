"""Cross-server benchmark suite: runs every scenario against every server and plots it.

Servers and load generators run unpinned, exactly as a deployment runs them.
Same-process calibration selects equal scenario exposure and forecasts six
fresh-process paired production blocks. One monotonic deadline bounds the run.

    uv run python bench/bench.py            # stage a run under bench/results/runs/
    uv run python bench/bench.py --publish  # also replace the checked-in plots
"""

from __future__ import annotations

import argparse
import contextlib
import importlib.metadata
import json
import math
import platform
import shutil
import signal
import statistics
import subprocess
import sys
import textwrap
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import matplotlib as mpl
from benchroom.plot import save, theme

mpl.use('Agg')
import matplotlib.pyplot as plt

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bench._core import (
    RESULTS_DIRECTORY,
    BenchmarkError,
    DeadlineError,
    MeasuredMetrics,
    Metrics,
    NoCompletionsError,
    ResponseError,
    Scenario,
    allocated_listener,
    check_response,
    deadline_bound,
    measure_peak_memory,
    peer_settings,
    request_response,
    rotations,
    run_load,
    running_server,
    write_json,
)
from bench.evidence import build_wheel, verify_workers
from bench.grpc import RpcScenario
from bench.report import (
    SERVER_COLORS,
    SERVER_LINESTYLES,
    SERVER_MARKERS,
    alt_text,
    format_rate,
    render_accessible_results,
)
from bench.sampling import (
    CLEANUP_RESERVE_SECONDS,
    HARD_TOTAL_SECONDS,
    PRODUCTION_BLOCKS,
    REPORT_RESERVE_SECONDS,
    CalibrationPoint,
    ScenarioSamples,
    calibration_record,
    calibration_settled,
    calibration_status,
    median_order_interval,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

CONCURRENCY = 100
STREAMING_CONCURRENCY = 1000
SEED = 20_260_726

# Every server runs the STDLIB asyncio event loop, and a pure-Python HTTP parser
# where it has one (uvicorn `--http h11`, gunicorn's Python parser). The
# comparison is meant to isolate the server, not its optional accelerators, and
# uvloop is not equally load-bearing across them: it does all of uvicorn's I/O,
# while h2corn's I/O runs in Rust and the loop only schedules application
# callbacks. Giving it to some and not others measured the accelerator instead.
#
# Every server runs with access logging ENABLED — that is how these servers are
# deployed, and request-log construction is part of the work a real deployment
# does. h2corn and uvicorn log by default; hypercorn and gunicorn need an
# explicit flag. Server output goes to /dev/null (see _core.running_server), so
# no server pays for a log sink the others avoid.
SERVERS: dict[str, list[str]] = {
    'h2corn': [
        'h2corn',
        'bench.bench_app:app',
        '--backlog',
        '2048',
        '--loop',
        'asyncio',
        '--timeout-keep-alive',
        '120',
        '--server-header',
        'off',
        '--date-header',
        '--no-proxy-headers',
        '--max-concurrent-streams',
        '256',
        '--h2-max-header-list-size',
        '1048576',
        '--h2-max-inbound-frame-size',
        '65536',
    ],
    'uvicorn': [
        'uvicorn',
        'bench.bench_app:app',
        '--loop',
        'asyncio',
        '--http',
        'h11',
        '--no-proxy-headers',
        '--timeout-keep-alive',
        '120',
        '--no-server-header',
        '--date-header',
        '--backlog',
        '2048',
    ],
    'hypercorn': [
        'hypercorn',
        'bench.bench_app:app',
        '-k',
        'asyncio',
        '--access-logfile',
        '-',
        '--backlog',
        '2048',
    ],
    'gunicorn': [
        'gunicorn',
        'bench.bench_app:app',
        '-k',
        'asgi',
        '--asgi-loop',
        'asyncio',
        '--no-control-socket',
        '--access-logfile',
        '-',
        '--backlog',
        '2048',
        '--keep-alive',
        '120',
    ],
}


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
            concurrency=1,
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
            'HTTP/1 Streaming POST (1 Worker)', 1, 'h1_stream', STREAMING_CONCURRENCY
        ),
        Scenario(
            'HTTP/1 Streaming POST (4 Workers)', 4, 'h1_stream', STREAMING_CONCURRENCY
        ),
        Scenario(
            'HTTP/2 Streaming POST (1 Worker)', 1, 'h2_stream', STREAMING_CONCURRENCY
        ),
        Scenario(
            'HTTP/2 Streaming POST (4 Workers)', 4, 'h2_stream', STREAMING_CONCURRENCY
        ),
        Scenario('HTTP/1 WebSocket (1 Worker)', 1, 'ws'),
        Scenario('HTTP/1 WebSocket (4 Workers)', 4, 'ws'),
    )


def write_hypercorn_config(path: Path) -> None:
    """Write the exposed Hypercorn counterparts to h2corn's chosen settings."""
    path.write_text(
        '# Generated by bench.py; retained with the run record.\n'
        'h2_max_concurrent_streams = 256\n'
        'h2_max_header_list_size = 1048576\n'
        'h2_max_inbound_frame_size = 65536\n'
        'keep_alive_timeout = 120\n'
        'keep_alive_max_requests = 9223372036854775807\n'
        'include_server_header = false\n'
        'include_date_header = true\n'
    )


def unsupported_reason(name: str, scenario: Scenario | RpcScenario) -> str | None:
    if scenario.type == 'grpc':
        return None
    if scenario.contract.protocol == '2' and name in {'uvicorn', 'gunicorn'}:
        return 'The selected release has no prior-knowledge cleartext HTTP/2 mode.'
    return None


def server_command(
    name: str,
    scenario: Scenario,
    *,
    hypercorn_config: Path,
    listener_fd: int | None = None,
) -> list[str]:
    if reason := unsupported_reason(name, scenario):
        raise BenchmarkError(f'{name}: {reason}')
    command = SERVERS[name].copy()
    command[0] = str(Path(sys.executable).parent / name)
    socket_path = scenario.socket_path
    if name == 'uvicorn':
        command += ['--ws', 'websockets' if scenario.type == 'ws' else 'none']
        command += (
            ['--fd', str(listener_fd)]
            if listener_fd is not None
            else ['--uds', str(socket_path)]
            if socket_path is not None
            else ['--host', '127.0.0.1', '--port', str(scenario.port)]
        )
    elif listener_fd is not None:
        command += ['-b', f'fd://{listener_fd}']
    else:
        target = (
            f'unix:{socket_path}'
            if socket_path is not None
            else f'127.0.0.1:{scenario.port}'
        )
        command += ['-b', target]
    if name == 'hypercorn':
        command += ['--config', str(hypercorn_config)]
    # uvicorn and hypercorn only spell it long; h2corn and gunicorn take both.
    workers_flag = '--workers' if name in {'hypercorn', 'uvicorn'} else '-w'
    command += [workers_flag, str(scenario.workers)]
    return command


def package_versions() -> dict[str, str | None]:
    versions: dict[str, str | None] = {}
    for package in (*SERVERS, 'starlette', 'h11', 'h2', 'websockets'):
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = None
    return versions


def system_summary() -> str:
    kernel = f'{platform.system()} {platform.release()}'.strip()
    summary = f'Python {platform.python_version()} | {kernel} | {platform.machine()}'
    model = None
    cpuinfo = Path('/proc/cpuinfo')
    if cpuinfo.exists():
        for line in cpuinfo.read_text(errors='ignore').splitlines():
            if line.startswith('model name'):
                model = line.partition(':')[2].strip()
                break
    elif shutil.which('sysctl'):
        result = subprocess.run(
            ['sysctl', '-n', 'machdep.cpu.brand_string'],
            capture_output=True,
            text=True,
            check=False,
        )
        model = result.stdout.strip() or None
    if model is None:
        raise BenchmarkError('failed to determine the CPU model for plot metadata')
    return f'{summary}\nCPU: {model}'


def aligned_setting(name: str, value: object, default: object) -> dict[str, object]:
    return {'setting': name, 'value': value, 'default': default}


def no_equivalent(
    h2corn_setting: str, h2corn_value: object, reason: str
) -> dict[str, object]:
    return {
        'h2corn_setting': h2corn_setting,
        'h2corn_value': h2corn_value,
        'reason': reason,
    }


def server_profiles(scenario: Scenario) -> dict[str, dict[str, object]]:
    """The aligned knobs and deliberately unmatched settings for one cell.

    Values are data, not prose: a reader can see the value passed to every
    competitor alongside its upstream default without making a plot carry
    configuration archaeology.
    """
    workers = scenario.workers
    h2corn_http2 = [
        aligned_setting('max_concurrent_streams', 256, 256),
        aligned_setting('h2_max_header_list_size', 1_048_576, 1_048_576),
        aligned_setting('h2_max_inbound_frame_size', 65_536, 65_536),
    ]
    profiles: dict[str, dict[str, object]] = {
        'h2corn': {
            'profile_choice': [aligned_setting('loop', 'asyncio', 'auto')],
            'aligned': [
                aligned_setting('workers', workers, 1),
                aligned_setting('backlog', 2_048, 1_024),
                aligned_setting('timeout_keep_alive', 120, 120),
                aligned_setting('server_header', 'off', 'off'),
                aligned_setting('date_header', True, True),
                aligned_setting('proxy_headers', False, False),
                *h2corn_http2,
            ],
            'no_equivalent': [],
        },
        'hypercorn': {
            'profile_choice': [aligned_setting('worker_class', 'asyncio', 'asyncio')],
            'aligned': [
                aligned_setting('workers', workers, 1),
                aligned_setting('backlog', 2_048, 100),
                aligned_setting('keep_alive_timeout', 120, 5),
                aligned_setting('keep_alive_max_requests', 2**63 - 1, 1000),
                aligned_setting('include_server_header', False, True),
                aligned_setting('include_date_header', True, True),
                aligned_setting('h2_max_concurrent_streams', 256, 100),
                aligned_setting('h2_max_header_list_size', 1_048_576, 65_536),
                aligned_setting('h2_max_inbound_frame_size', 65_536, 16_384),
            ],
            'no_equivalent': [
                no_equivalent(
                    'proxy_headers',
                    False,
                    'Hypercorn exposes no server-level proxy-header parsing setting.',
                ),
                no_equivalent(
                    'h2_initial_stream_window_size',
                    8_388_608,
                    'Hypercorn exposes no HTTP/2 flow-control window; it uses '
                    "h2's 65,535-byte default.",
                ),
                no_equivalent(
                    'h2_initial_connection_window_size',
                    8_388_608,
                    'Hypercorn exposes no HTTP/2 connection receive-window setting.',
                ),
                no_equivalent(
                    'h2_max_header_block_size',
                    1_048_576,
                    'Hypercorn exposes no compressed HTTP/2 header-block limit.',
                ),
            ],
        },
        'uvicorn': {
            'aligned': [
                aligned_setting('workers', workers, 1),
                aligned_setting('backlog', 2_048, 2_048),
                aligned_setting('timeout_keep_alive', 120, 5),
                aligned_setting('server_header', False, True),
                aligned_setting('date_header', True, True),
                aligned_setting('proxy_headers', False, True),
            ],
            'profile_choice': [
                aligned_setting(
                    'loop',
                    'asyncio',
                    'auto',
                )
            ],
            'http_backend': 'h11',
            'no_equivalent': [],
        },
        'gunicorn': {
            'aligned': [
                aligned_setting('workers', workers, 1),
                aligned_setting('backlog', 2_048, 2_048),
                aligned_setting('keep_alive', 120, 2),
            ],
            'profile_choice': [aligned_setting('asgi_loop', 'asyncio', 'auto')],
            'no_equivalent': [
                no_equivalent(
                    'server_header',
                    'off',
                    'Gunicorn exposes no response Server-header setting.',
                ),
                no_equivalent(
                    'date_header',
                    True,
                    'Gunicorn exposes no response Date-header setting.',
                ),
                no_equivalent(
                    'proxy_headers',
                    False,
                    'Gunicorn has no ASGI proxy-header parsing counterpart.',
                ),
            ],
            'http_backend': 'python',
        },
    }
    for name in profiles:
        if reason := unsupported_reason(name, scenario):
            profiles[name] = {'status': 'unsupported', 'reason': reason}
    return profiles


def aggregate(samples: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rates = [sample['rps'] for sample in samples]
    peak_memory = [sample['peak_memory_bytes'] for sample in samples]
    percentile_names = sorted(
        {name for sample in samples for name in sample['latency_percentiles']},
        key=lambda name: float(name.removeprefix('p')),
    )
    return {
        'rps': statistics.median(rates),
        'rps_samples': rates,
        # Median completions, beside the rate they produced. A server that
        # answers once per connection and then stalls still reports a rate;
        # only this count against the cell's concurrency reveals it.
        'completed': statistics.median([sample['completed'] for sample in samples]),
        'elapsed_seconds': statistics.median([
            sample['elapsed_seconds'] for sample in samples
        ]),
        'rps_range': [min(rates), max(rates)],
        # Throughput is the median trial, but deployment capacity must cover
        # the largest sampled PSS high-water mark from every measured trial.
        'peak_memory_bytes': max(peak_memory),
        'latency_percentiles': {
            name: statistics.median([
                sample['latency_percentiles'][name]
                for sample in samples
                if name in sample['latency_percentiles']
            ])
            for name in percentile_names
        },
    }


class TrialError(BenchmarkError):
    def __init__(self, phase: str, message: str) -> None:
        super().__init__(message)
        self.phase = phase


def measure_cell(
    name: str,
    scenario: Scenario | RpcScenario,
    duration: str,
    warmup: str,
    *,
    hypercorn_config: Path,
    evidence_directory: Path,
    identity: dict[str, Any] | None = None,
    calibration_windows: Sequence[float] | None = None,
) -> MeasuredMetrics | dict[str, Any]:
    """Calibrate in one verified process or measure one fresh production block."""
    if scenario.type == 'grpc':
        from bench.grpc import measure_cell as measure_rpc

        assert identity is not None
        return measure_rpc(
            name,
            scenario,
            duration,
            warmup,
            evidence_directory=evidence_directory,
            identity=identity,
            calibration_windows=calibration_windows,
        )
    concurrency = scenario.concurrency or CONCURRENCY
    evidence_directory.mkdir(parents=True)
    identity_directory = evidence_directory / 'worker-identities'
    identity_directory.mkdir()
    started = time.monotonic()
    trial: dict[str, Any] = {
        'server': name,
        'phase': 'listener',
        'status': 'running',
        'started_at': datetime.now(UTC).isoformat(),
        'load_plan': {'duration': duration, 'warmup_duration': warmup},
    }
    try:
        with allocated_listener(scenario) as (live, listener):
            command = server_command(
                name,
                live,
                hypercorn_config=hypercorn_config,
                listener_fd=listener.fileno(),
            )
            trial.update(
                command=command,
                port=live.port,
                socket_path=str(live.socket_path),
                tcp_nodelay=not live.contract.unix_socket,
                phase='startup',
            )
            write_json(evidence_directory / 'trial.json', trial)
            with running_server(
                command,
                workers=live.workers,
                socket_path=live.socket_path,
                scenario=live,
                listener_fd=listener.fileno(),
                identity_directory=identity_directory,
            ) as server:
                trial['phase'] = 'identity'
                receipts = {
                    pid: json.loads((identity_directory / f'{pid}.json').read_text())
                    for pid in server.worker_pids
                }
                receipt = json.loads(
                    request_response(live, '/__bench/identity', timeout=5)
                )
                if receipt['pid'] not in receipts:
                    raise BenchmarkError('serving worker changed after readiness')
                receipts[receipt['pid']] = receipt
                trial['workers'] = receipts
                if identity is not None:
                    verify_workers(trial, identity)
                trial['supervisor_pid'] = server.pid
                if live.contract.protocol == '2':
                    trial['phase'] = 'settings'
                    trial['http2_peer'] = peer_settings(live)
                trial['phase'] = 'correctness-before'
                check_response(live)
                trial['correctness_before'] = 'passed'
                if calibration_windows is None:
                    trial['phase'] = 'warmup'
                    # Warmup measures nothing; a slow server may finish no request
                    # in it, and the calibrated measured window covers that latency.
                    with contextlib.suppress(NoCompletionsError):
                        run_load(
                            live,
                            duration=warmup,
                            concurrency=concurrency,
                            evidence_path=evidence_directory / 'warmup.json',
                        )
                if calibration_windows is not None:
                    calibration_pids = sorted(server.process_pids())
                    points: list[CalibrationPoint] = []
                    for window in calibration_windows:
                        label = f'{window:g}s'
                        window_directory = evidence_directory / f'window-{label}'
                        window_started = time.monotonic()
                        trial['phase'] = f'calibration-{label}'
                        # Use the production PSS observer so its cost is in the forecast.

                        def load_window(
                            selected_window: float = window,
                            directory: Path = window_directory,
                        ) -> Metrics:
                            return run_load(
                                live,
                                duration=f'{selected_window:g}s',
                                concurrency=concurrency,
                                evidence_path=directory / 'load.json',
                            )

                        try:
                            metrics, _ = measure_peak_memory(
                                server,
                                load_window,
                                evidence_path=window_directory / 'memory.json',
                            )
                        except NoCompletionsError:
                            # A window shorter than one request's latency proves
                            # nothing about the server; only the longest may fail.
                            if window == calibration_windows[-1]:
                                raise
                            metrics = {
                                'rps': 0.0,
                                'completed': 0,
                                'latency_percentiles': {},
                            }
                        points.append(
                            CalibrationPoint(
                                duration_seconds=window,
                                rate=metrics['rps'],
                                elapsed_seconds=time.monotonic() - window_started,
                                completed=metrics['completed'],
                            )
                        )
                        if calibration_settled(points):
                            break
                    if calibration_pids != sorted(server.process_pids()):
                        raise BenchmarkError(
                            f'{name} process population changed during calibration'
                        )
                    trial['phase'] = 'correctness-after'
                    check_response(live)
                    trial['correctness_after'] = 'passed'
                    status, selected_duration = calibration_status(points)
                    measured = {
                        'server': name,
                        'status': status,
                        'selected_duration_seconds': selected_duration,
                        'windows': calibration_record(points),
                    }
                    trial['calibration'] = measured
                else:
                    trial['phase'] = 'load'
                    metrics, peak_memory_bytes = measure_peak_memory(
                        server,
                        lambda: run_load(
                            live,
                            duration=duration,
                            concurrency=concurrency,
                            evidence_path=evidence_directory / 'load.json',
                        ),
                        evidence_path=evidence_directory / 'memory.json',
                    )
                    trial['phase'] = 'correctness-after'
                    check_response(live)
                    trial['correctness_after'] = 'passed'
                    measured = {
                        **metrics,
                        'peak_memory_bytes': peak_memory_bytes,
                    }
                    trial['metrics'] = measured
                trial['phase'] = 'cleanup'
    except DeadlineError as error:
        trial.update(status='failed', error=str(error))
        raise
    except (OSError, ValueError, RuntimeError) as error:
        trial.update(status='failed', error=str(error))
        raise TrialError(
            trial['phase'],
            f'{name} {trial["phase"]} failed: {error}',
        ) from error
    else:
        trial.update(status='completed', phase='completed')
        measured['elapsed_seconds'] = time.monotonic() - started
        return measured
    finally:
        trial['elapsed_seconds'] = time.monotonic() - started
        write_json(evidence_directory / 'trial.json', trial)


def paired_contrasts(
    samples: Mapping[str, Sequence[Mapping[str, Any]]],
    sample_blocks: dict[str, list[int]] | None = None,
    *,
    calibration_limited: bool = False,
) -> dict[str, Any]:
    """Compare medians of rates paired by their independent production block."""
    if 'h2corn' not in samples:
        return {}
    blocks = sample_blocks or {
        name: list(range(len(values))) for name, values in samples.items()
    }
    candidate_by_block = dict(zip(blocks['h2corn'], samples['h2corn'], strict=True))
    contrasts = {}
    for name, values in samples.items():
        if name == 'h2corn':
            continue
        competitor_by_block = dict(zip(blocks[name], values, strict=True))
        common_blocks = sorted(candidate_by_block.keys() & competitor_by_block.keys())
        ratios = [
            candidate_by_block[block]['rps'] / competitor_by_block[block]['rps']
            for block in common_blocks
        ]
        contrast = {
            'h2corn_over_competitor': statistics.median(ratios) if ratios else None,
            'block_ratios': ratios,
            'block_ids': common_blocks,
            'sample_size': len(ratios),
        }
        if len(ratios) == PRODUCTION_BLOCKS:
            contrast.update(median_order_interval(ratios))
        else:
            contrast.update(
                h2corn_over_competitor=None,
                interval=None,
                achieved_coverage=None,
                method='too few independent paired blocks for an interval',
            )
        if calibration_limited:
            contrast['decision'] = 'inconclusive'
            contrast['decision_reason'] = 'calibration-limited'
        elif contrast['interval'] is None:
            contrast['decision'] = 'inconclusive'
            contrast['decision_reason'] = 'insufficient paired blocks'
        else:
            low, high = contrast['interval']
            contrast['decision'] = (
                'slower' if low > 1 else 'faster' if high < 1 else 'inconclusive'
            )
        contrasts[name] = contrast
    return contrasts


def workload_description(scenario: Scenario) -> str:
    descriptions = {
        'h1': '13-byte text response',
        'h1_uds': '13-byte text response over a Unix socket',
        'h2': '13-byte text response',
        'h1_file': '128 KiB file response; native file-send extension where supported',
        'h2_file': '128 KiB file response; native file-send extension where supported',
        'h1_download': '128 KiB download in eight 16 KiB ASGI chunks',
        'h2_download': '128 KiB download in eight 16 KiB ASGI chunks',
        'h1_stream': '1 KiB upload, then three response chunks with 20 ms application delay',
        'h2_stream': '1 KiB upload, then three response chunks with 20 ms application delay',
        'ws': 'Connect, echo one text message, disconnect; fresh sessions',
    }
    concurrency = scenario.concurrency or CONCURRENCY
    return (
        descriptions[scenario.type]
        + f'; {concurrency} client connection{"s" if concurrency != 1 else ""}'
        + (
            f' with {scenario.http2_parallelism} stream{"s" if scenario.http2_parallelism != 1 else ""} per connection'
            if scenario.contract.protocol == '2'
            else ''
        )
    )


def run_scenario(
    scenario: Scenario | RpcScenario,
    servers: Sequence[str],
    *,
    duration: str,
    warmup: str,
    max_trials: int,
    deadline: float,
    hypercorn_config: Path,
    evidence_directory: Path,
    identity: dict[str, Any],
    phase: str = 'production',
    profile: str = 'loopback',
    calibration: dict[str, Any] | None = None,
    calibration_limited: bool = False,
    initial_failures: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    scenario_started = time.monotonic()
    names = list(servers)
    rpc = scenario.type == 'grpc'
    rpc_case = scenario.case if isinstance(scenario, RpcScenario) else None
    not_selected = (
        {'grpclib': 'Representative no-compression subset'}
        if rpc and not scenario.grpclib and 'grpclib' in names
        else {}
    )
    unsupported = {
        name: reason for name in names if (reason := unsupported_reason(name, scenario))
    }
    supported = tuple(
        name for name in names if name not in unsupported and name not in not_selected
    )
    if not supported:
        raise BenchmarkError(f'no selected server supports {scenario.name}')
    print(
        f'phase={phase} profile={profile} scenario={scenario.name} '
        f'duration={duration} warmup={warmup} blocks={max_trials}',
        file=sys.stderr,
        flush=True,
    )
    sampled = ScenarioSamples(supported)
    failures: dict[str, dict[str, Any]] = {
        cell['server']: {
            'phase': 'qualification',
            'error': cell['error'],
            'block': 0,
            'evidence': f'calibration/{cell["server"]}',
            'status': 'response_validation_failed',
        }
        for cell in identity.get('rpc_qualification', [])
        if rpc
        and cell['case'] == rpc_case
        and cell['status'] == 'failed'
        and cell['server'] in supported
    }
    for name, failure in (initial_failures or {}).items():
        failures.setdefault(name, failure)
    for name, failure in failures.items():
        sampled.fail(name, failure)
    stopped = 'max-trials'
    trials = 0
    blocks = []
    for trial in range(1, max_trials + 1):
        remaining = sampled.remaining()
        if not remaining:
            break
        order = rotations(remaining, max_trials, SEED)[trial - 1]
        blocks.append({
            'block': trial,
            'order': order,
            'not_repeated_after_failure': list(sampled.failures),
        })
        for name in order:
            if name in sampled.failures:
                continue
            if time.monotonic() >= deadline:
                raise DeadlineError(
                    f'{scenario.name} deadline before block {trial}, {name}'
                )
            try:
                with deadline_bound(deadline):
                    measured = measure_cell(
                        name,
                        scenario,
                        duration,
                        warmup,
                        hypercorn_config=hypercorn_config,
                        evidence_directory=evidence_directory
                        / f'block-{trial:02d}'
                        / name,
                        identity=identity,
                    )
                    if 'rps' not in measured or 'peak_memory_bytes' not in measured:
                        raise BenchmarkError(
                            f'{name} production block returned calibration data'
                        )
                    sampled.add(name, trial, measured)
            except TrialError as error:
                failure = {
                    'phase': error.phase,
                    'error': str(error),
                    'block': trial,
                    'evidence': f'block-{trial:02d}/{name}',
                    'status': 'response_validation_failed',
                }
                write_json(
                    evidence_directory / f'failure-{name}.json',
                    {
                        'scenario': scenario.name,
                        'blocks': blocks,
                        'failed_server': name,
                        'error': str(error),
                        'samples': sampled.values(),
                    },
                )
                if not isinstance(error.__cause__, ResponseError):
                    raise
                sampled.fail(name, failure)
                print(
                    f'  FAILED {phase} {scenario.name} {name}: {error}',
                    file=sys.stderr,
                    flush=True,
                )
        remaining = sampled.remaining()
        if not remaining:
            trials = trial
            break
        trials = trial
        medians = ', '.join(
            f'{name} {format_rate(statistics.median([s["rps"] for s in sampled.values()[name]]))}'
            for name in remaining
        )
        print(
            f'phase={phase} profile={profile} scenario={scenario.name} '
            f'block={trial}/{max_trials} elapsed={time.monotonic() - scenario_started:.1f}s '
            f'remaining={max(0.0, deadline - time.monotonic()):.1f}s median={medians}',
            file=sys.stderr,
            flush=True,
        )
        if trial < max_trials and time.monotonic() > deadline:
            raise DeadlineError(f'{scenario.name} deadline after block {trial}')
    description = scenario.workload if rpc else workload_description(scenario)
    samples = sampled.values()
    failures = sampled.failures
    successful_samples = sampled.successful_values()
    successful_blocks = sampled.successful_block_ids()
    return {
        'scenario': scenario.name,
        'status': 'attempted_with_failures' if failures else 'completed',
        'workload': description,
        'load_plan': {'duration': duration, 'warmup_duration': warmup},
        'type': scenario.type,
        'workers': scenario.workers,
        'trials': trials,
        'planned_trials': max_trials,
        'stopped': stopped,
        'unsupported': unsupported,
        **({'not_selected': not_selected} if rpc else {}),
        'failures': failures,
        'requested_servers': names,
        'server_profiles': (
            {
                name: {
                    'loop': 'asyncio',
                    'processes': 1,
                    'compression': 'identity',
                    'access_log': False,
                }
                for name in names
            }
            if rpc
            else server_profiles(scenario)
        ),
        'blocks': blocks,
        'samples': samples,
        'sample_blocks': sampled.block_ids(),
        'calibration': calibration or {},
        'calibration_limited': calibration_limited,
        'paired_contrasts': paired_contrasts(
            successful_samples,
            successful_blocks,
            calibration_limited=calibration_limited,
        ),
        'aggregate': {
            name: aggregate(values) for name, values in successful_samples.items()
        },
    }


def plot_results(
    record: dict[str, Any],
    path: Path,
) -> None:
    for narrow in (False, True):
        for dark in (False, True):
            suffix = ('-narrow' if narrow else '') + ('-dark' if dark else '')
            destination = path.with_stem(path.stem + suffix)
            with theme(dark=dark):
                _plot_results(record, destination, dark=dark, narrow=narrow)
    path.with_suffix('.md').write_text(
        render_accessible_results(record), encoding='utf-8'
    )
    print(f'Saved plot to {path}')
    print(
        alt_text(
            record['scenario'],
            record['aggregate'],
            record.get(
                'rate_unit', 'sessions/s' if record['type'] == 'ws' else 'requests/s'
            ),
            record.get('failures'),
            record.get('stack_labels'),
        )
    )


def _plot_results(
    record: dict[str, Any], path: Path, *, dark: bool, narrow: bool
) -> None:
    results = record['aggregate']
    title = record['scenario']
    websocket = record['type'] == 'ws'
    failures = record.get('failures', {})
    figure, axes = plt.subplots(
        2 if narrow else 1,
        1 if narrow else 2,
        figsize=(5.2, 8.4) if narrow else (10.5, 5.6),
        layout='constrained',
    )
    figure.suptitle(
        textwrap.fill(title, width=44) if narrow else title,
        fontsize=15 if narrow else 17,
        fontweight='bold',
    )

    names = sorted(
        record.get('requested_servers', [*results, *failures]),
        key=lambda name: list(SERVER_COLORS).index(name),
    )
    labels = [record.get('stack_labels', {}).get(name, name) for name in names]
    rates = [results[name]['rps'] if name in results else math.nan for name in names]
    colors = [SERVER_COLORS[name][dark] for name in names]
    foreground = mpl.rcParams['text.color']

    throughput = axes[0]
    bars = throughput.bar(
        [label.replace(' / ', ' /\n') for label in labels], rates, color=colors
    )
    throughput.set_xlim(-0.6, len(names) - 0.4)
    for bar, name in zip(bars, names, strict=True):
        if name not in results:
            throughput.text(
                bar.get_x() + bar.get_width() / 2,
                0.05,
                'failed'
                if name in failures
                else 'not selected'
                if name in record.get('not_selected', {})
                else 'unsupported',
                ha='center',
                va='bottom',
                transform=throughput.get_xaxis_transform(),
                fontsize=9,
            )
            continue
        low, high = results[name]['rps_range']
        median = results[name]['rps']
        if high > low:
            throughput.errorbar(
                bar.get_x() + bar.get_width() / 2,
                median,
                yerr=[[median - low], [high - median]],
                fmt='none',
                color=foreground,
                capsize=4,
            )
        samples = results[name]['rps_samples']
        throughput.scatter(
            [
                bar.get_x()
                + bar.get_width() / 2
                + (index - (len(samples) - 1) / 2) * 0.045
                for index in range(len(samples))
            ],
            samples,
            s=20,
            facecolors='#111923' if dark else '#ffffff',
            edgecolors=foreground,
            linewidths=0.8,
            zorder=3,
        )
    unit = record.get('rate_unit', 'sessions/s' if websocket else 'requests/s')
    throughput.set_title('Throughput · higher is better')
    throughput.set_ylabel(unit)
    throughput.margins(y=0.24)
    for bar, name in zip(bars, names, strict=True):
        if name not in results:
            continue
        _, high = results[name]['rps_range']
        result = results[name]
        value = result['rps']
        center = bar.get_x() + bar.get_width() / 2
        top = max(value, high)
        throughput.annotate(
            format_rate(value),
            (center, top),
            xytext=(0, 17),
            textcoords='offset points',
            ha='center',
            va='bottom',
            fontsize=10,
        )
        throughput.annotate(
            f'{result["peak_memory_bytes"] / 1e6:.1f} MB PSS',
            (center, top),
            xytext=(0, 4),
            textcoords='offset points',
            ha='center',
            va='bottom',
            fontsize=8,
            alpha=0.85,
        )

    latency = axes[1]
    plotted = False
    for name, label in zip(names, labels, strict=True):
        if name not in results:
            continue
        percentiles = results[name]['latency_percentiles']
        if not percentiles:
            continue
        points = sorted(
            percentiles.items(), key=lambda item: float(item[0].removeprefix('p'))
        )
        latency.plot(
            [float(key.removeprefix('p')) for key, _ in points],
            [value * 1000 for _, value in points],
            color=SERVER_COLORS[name][dark],
            linestyle=SERVER_LINESTYLES.get(name, '-'),
            marker=SERVER_MARKERS.get(name, 'o'),
            label=label,
            linewidth=2,
            markersize=6,
            markeredgewidth=0,
        )
        plotted = True
    if plotted:
        latency.set_title(
            ('Session duration' if websocket else 'Latency')
            + ' percentiles · lower is better'
        )
        latency.set_xlabel('Percentile')
        latency.set_ylabel('Latency (ms)')
        latency.legend()
        latency.grid(True)
    else:
        latency.set_title('Latency data unavailable')

    sampling = (
        f'\nSampling: {record["duration"]} load · {record["warmup_duration"]} warmup · '
        f'{record["planned_trials"]} production blocks · calibration excluded'
    )
    calibration_note = (
        ' · calibration-limited; comparisons inconclusive'
        if record.get('calibration_limited')
        else ''
    )
    footer = (
        'Bars: median rate · Dots: fresh-start production blocks · Whiskers: observed range'
        + sampling
        + calibration_note
        + (
            '\nLatency: median trial histogram bounds (1% buckets)'
            if record.get('benchmark_family') == 'grpc'
            else '\nLatency: median trial percentiles'
        )
        + ' · PSS: maximum of initial, 1s periodic, and final samples; decimal MB\n'
        + textwrap.fill(record.get('workload', ''), width=55 if narrow else 110)
    )
    if narrow:
        footer = '\n'.join(
            textwrap.fill(line, width=55) for line in footer.splitlines()
        )
    figure.supxlabel(footer, fontsize=9)
    save(figure, path)
    plt.close(figure)


def publish(run_directories: Sequence[Path]) -> None:
    profiles = [json.loads((d / 'profile.json').read_text()) for d in run_directories]
    if len({p.get('benchmark_family', 'http') for p in profiles}) != 1:
        raise BenchmarkError('publish one benchmark family at a time')
    rpc = all(p.get('benchmark_family') == 'grpc' for p in profiles)
    root = RESULTS_DIRECTORY / 'grpc' if rpc else RESULTS_DIRECTORY
    evidence = root / 'evidence'
    if evidence.exists():
        shutil.rmtree(evidence)
    evidence.mkdir(parents=True)
    for directory in run_directories:
        profile = json.loads((directory / 'profile.json').read_text())['profile']
        destination = evidence / profile
        destination.mkdir()
        for name in (
            'identity.json',
            'profile.json',
            'suite-plan.json',
            'hypercorn.toml',
            'build.log',
            'qualification.json',
            'notebook.pb',
        ):
            if (directory / name).exists():
                shutil.copy2(directory / name, destination / name)
        if (directory / 'evidence').exists():
            shutil.copytree(directory / 'evidence', destination / 'trials')
    for kind, suffix, destination in (
        ('raw', '.json', root / 'raw'),
        ('plots', '.svg', root / 'plots'),
        ('plots', '.md', root / 'plots'),
    ):
        destination.mkdir(parents=True, exist_ok=True)
        for stale in destination.glob(f'*{suffix}'):
            stale.unlink()
        for run_directory in run_directories:
            profile = json.loads(
                (run_directory / 'profile.json').read_text(encoding='utf-8')
            )['profile']
            for artifact in sorted((run_directory / kind).glob(f'*{suffix}')):
                variant = next(
                    (
                        suffix
                        for suffix in ('-narrow-dark', '-narrow', '-dark')
                        if kind == 'plots' and artifact.stem.endswith(suffix)
                    ),
                    '',
                )
                stem = artifact.stem.removesuffix(variant)
                published_name = f'{stem}-{profile}{variant}{suffix}'
                shutil.copy2(artifact, destination / published_name)
    print(
        f'Published {len(run_directories)} profile(s) with matched raw records, '
        f'tables, and plots to {root / "raw"} and {root / "plots"}'
    )


def assert_publishable(run_directories: Sequence[Path]) -> None:
    incomplete = []
    for run_directory in run_directories:
        records = sorted((run_directory / 'raw').glob('*.json'))
        if not records:
            raise BenchmarkError(f'no benchmark results in {run_directory}')
        identity_path = run_directory / 'identity.json'
        if (
            not identity_path.is_file()
            or json.loads(identity_path.read_text()).get('build') is None
        ):
            raise BenchmarkError(f'no identified release build in {run_directory}')
        profile = (
            json.loads((run_directory / 'profile.json').read_text())
            if (run_directory / 'profile.json').exists()
            else {}
        )
        if profile.get('status') not in {'completed', 'completed_with_failures'}:
            incomplete.append(f'{run_directory.name}: profile did not complete')
        suite_plan_path = run_directory / 'suite-plan.json'
        if not suite_plan_path.is_file():
            raise BenchmarkError(
                f'adaptive publication plan is missing in {run_directory}'
            )
        suite_plan = json.loads(suite_plan_path.read_text())
        if suite_plan.get('status') != 'frozen':
            raise BenchmarkError(
                f'adaptive plan was not frozen before production in {run_directory}'
            )
        family = profile.get('benchmark_family', 'http')
        profile_name = profile.get('profile')
        planned = {
            item['slug']: item
            for item in suite_plan.get('scenarios', ())
            if item.get('family') == family and item.get('profile') == profile_name
        }
        if set(planned) != {path.stem for path in records}:
            incomplete.append(
                f'{run_directory.name}: raw scenarios do not match the frozen plan'
            )
        if family == 'grpc':
            from bench.grpc import assert_rpc_publishable

            assert_rpc_publishable(run_directory)
        for record_path in records:
            record = json.loads(record_path.read_text())
            scenario_plan = planned.get(record_path.stem)
            if scenario_plan is None:
                continue
            expected_blocks = scenario_plan['production_blocks']
            if (
                expected_blocks != PRODUCTION_BLOCKS
                or record.get('planned_trials') != expected_blocks
                or record.get('sampling', {}).get('production_blocks')
                != expected_blocks
            ):
                incomplete.append(
                    f'{record["scenario"]}: production blocks differ from the frozen plan'
                )
            if record.get('stopped') == 'time-budget' or (
                not record.get('aggregate') and not record.get('failures')
            ):
                incomplete.append(record['scenario'])
            represented = (
                set(record.get('aggregate', {}))
                | set(record.get('failures', {}))
                | set(record.get('unsupported', {}))
                | set(record.get('not_selected', {}))
            )
            if represented != set(record['requested_servers']):
                incomplete.append(
                    f'{record["scenario"]}: unrepresented requested servers'
                )
            if record.get('calibration_limited') != scenario_plan.get(
                'calibration_limited', False
            ):
                incomplete.append(
                    f'{record["scenario"]}: calibration status differs from the frozen plan'
                )
            calibration = scenario_plan.get('calibration', {})
            for name, cell in calibration.get('servers', {}).items():
                trial_path = (
                    run_directory
                    / 'evidence'
                    / record_path.stem
                    / 'calibration'
                    / name
                    / 'trial.json'
                )
                expected_status = (
                    'failed' if cell.get('status') == 'failed' else 'completed'
                )
                if (
                    not trial_path.is_file()
                    or json.loads(trial_path.read_text()).get('status')
                    != expected_status
                ):
                    incomplete.append(
                        f'{record["scenario"]}: missing {name} calibration evidence'
                    )
            for name, result in record.get('aggregate', {}).items():
                block_ids = record.get('sample_blocks', {}).get(name, [])
                if (
                    name in record.get('failures', {})
                    or len(result['rps_samples']) != expected_blocks
                    or block_ids != list(range(1, expected_blocks + 1))
                ):
                    incomplete.append(
                        f'{record["scenario"]}: incomplete {name} paired production blocks'
                    )
                for block in range(1, expected_blocks + 1):
                    trial_path = (
                        run_directory
                        / 'evidence'
                        / record_path.stem
                        / f'block-{block:02d}'
                        / name
                        / 'trial.json'
                    )
                    if (
                        not trial_path.is_file()
                        or json.loads(trial_path.read_text()).get('status')
                        != 'completed'
                    ):
                        incomplete.append(
                            f'{record["scenario"]}: incomplete {name} block {block}'
                        )
            for name, failure in record.get('failures', {}).items():
                trial_path = (
                    run_directory
                    / 'evidence'
                    / record_path.stem
                    / failure['evidence']
                    / 'trial.json'
                )
                if (
                    name in record['aggregate']
                    or not trial_path.is_file()
                    or json.loads(trial_path.read_text()).get('status') != 'failed'
                ):
                    incomplete.append(
                        f'{record["scenario"]}: missing failed-cell evidence for {name}'
                    )
    if incomplete:
        raise BenchmarkError(
            f'incomplete benchmark scenarios: {", ".join(incomplete)}; '
            'increase the time budget and re-run'
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Adaptive h2corn HTTP and RPC benchmark suite',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        '--family',
        choices=('all', 'http', 'grpc'),
        default='all',
        help='benchmark both families together or select one for a diagnostic run',
    )
    parser.add_argument('--servers', nargs='+', choices=[*SERVERS, 'grpcio', 'grpclib'])
    parser.add_argument('--rpc-cases', nargs='+')
    parser.add_argument('--connect-wheel', type=Path)
    parser.add_argument('--connect-development-revision')
    parser.add_argument('--connect-source', type=Path)
    parser.add_argument('--comparator-selection', type=Path)
    parser.add_argument('--qualify-only', action='store_true')
    benchmark_types = sorted({scenario.type for scenario in benchmark_scenarios()})
    parser.add_argument('--types', nargs='+', choices=benchmark_types)
    parser.add_argument(
        '--output-directory',
        type=Path,
        default=RESULTS_DIRECTORY,
        help='directory containing staged run artifacts',
    )
    parser.add_argument('--run-directory', type=Path, help=argparse.SUPPRESS)
    parser.add_argument(
        '--wheel',
        type=Path,
        help='Verify installed h2corn bytes against this explicitly built wheel',
    )
    parser.add_argument(
        '--build-wheel',
        type=Path,
        help='Build an identified release wheel into an empty directory, then exit',
    )
    parser.add_argument(
        '--publish',
        action='store_true',
        help='replace both published benchmark families after the complete adaptive run',
    )
    args = parser.parse_args()

    def interrupted(_signum: int, _frame: Any) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupted)
    if args.build_wheel is not None:
        print(
            f'Building release wheel; log: {args.build_wheel / "build.log"}',
            file=sys.stderr,
            flush=True,
        )
        try:
            wheel = build_wheel(
                Path(__file__).resolve().parents[2],
                args.build_wheel.resolve(),
            )
        except (OSError, RuntimeError, subprocess.SubprocessError) as error:
            print(f'release build failed: {error}', file=sys.stderr)
            return 1
        print(wheel)
        return 0
    if not sys.platform.startswith('linux'):
        parser.error('benchmark collection requires Linux PSS accounting and listeners')
    from bench import grpc

    if args.family == 'grpc' and args.types:
        parser.error('--family grpc uses --rpc-cases and its cleartext/TLS plans')
    if args.family == 'http' and (
        args.rpc_cases
        or args.connect_wheel
        or args.connect_development_revision
        or args.connect_source
        or args.comparator_selection
        or args.qualify_only
    ):
        parser.error('RPC qualification and application options require --family grpc')
    if args.family == 'all' and args.qualify_only:
        parser.error('--qualify-only requires --family grpc')
    if args.family == 'http' and args.servers and set(args.servers) - set(SERVERS):
        parser.error('gRPC servers require --family grpc')
    if args.family == 'grpc' and args.servers and set(args.servers) - set(grpc.SERVERS):
        parser.error('gRPC servers are h2corn, hypercorn, grpcio, and grpclib')
    if (
        args.family in {'all', 'grpc'}
        and args.rpc_cases
        and set(args.rpc_cases) - {scenario.case for scenario in grpc.scenarios()}
    ):
        parser.error('unknown --rpc-cases selection')
    if args.connect_development_revision and (
        not args.qualify_only or not args.rpc_cases or args.connect_source is None
    ):
        parser.error(
            'development Connect requires --qualify-only, explicit --rpc-cases, and --connect-source'
        )
    if args.connect_source is not None and args.connect_development_revision is None:
        parser.error('--connect-source requires --connect-development-revision')
    if args.publish and (
        args.family != 'all'
        or args.wheel is None
        or args.servers
        or args.types
        or args.rpc_cases
        or args.qualify_only
        or args.connect_wheel
        or args.connect_development_revision
        or args.connect_source
        or args.comparator_selection
    ):
        parser.error(
            '--publish requires the complete HTTP+gRPC matrix, a released wheel, and no selectors'
        )

    started = time.monotonic()
    started_at = datetime.now(UTC)
    hard_deadline = started + HARD_TOTAL_SECONDS
    work_deadline = hard_deadline - REPORT_RESERVE_SECONDS - CLEANUP_RESERVE_SECONDS
    try:
        from bench.adaptive import run_adaptive_suite

        with deadline_bound(hard_deadline):
            suite_directory = run_adaptive_suite(
                args,
                sys.modules[__name__],
                started=started,
                started_at=started_at,
                work_deadline=work_deadline,
                hard_deadline=hard_deadline,
            )
    except (BenchmarkError, OSError, subprocess.SubprocessError, ValueError) as error:
        print(f'benchmark failed: {error}', file=sys.stderr, flush=True)
        return 1
    except KeyboardInterrupt:
        print('benchmark interrupted before publication', file=sys.stderr, flush=True)
        return 1
    print(
        f'Benchmark evidence is staged in {suite_directory}',
        file=sys.stderr,
        flush=True,
    )
    return 0


if __name__ == '__main__':
    # Run the importable module rather than this `__main__` copy: sibling
    # modules import `bench.bench`, and a second copy would define a distinct
    # `TrialError`, so a competitor's failed cell would escape their handlers
    # and abort the whole run instead of being recorded.
    from bench.bench import main as package_main

    raise SystemExit(package_main())
