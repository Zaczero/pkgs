"""Combined adaptive HTTP and RPC benchmark collection."""

from __future__ import annotations

import asyncio
import json
import secrets
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

from bench._core import (
    BenchmarkError,
    DeadlineError,
    ResponseError,
    Scenario,
    deadline_bound,
    ensure_large_upload_payload,
    ensure_static_file_payload,
    terminate_process_tree,
    write_json,
)
from bench.evidence import run_identity
from bench.sampling import (
    CALIBRATION_WINDOWS_SECONDS,
    CLEANUP_RESERVE_SECONDS,
    HARD_TOTAL_SECONDS,
    PRODUCTION_BLOCKS,
    PRODUCTION_WARMUP_SECONDS,
    REPORT_RESERVE_SECONDS,
    TARGET_TOTAL_SECONDS,
    BudgetError,
    allocate_production_blocks,
    calibration_production_cost,
    common_duration,
)


@dataclass(slots=True)
class ProfileRun:
    family: str
    name: str
    network_profile: Any | None
    scenarios: tuple[Any, ...]
    servers: tuple[str, ...]
    directory: Path
    identity: dict[str, Any]
    hypercorn_config: Path
    system_summary: str
    package_versions: dict[str, str | None]
    calibrations: dict[str, dict[str, Any]] = field(default_factory=dict)
    scenario_plans: dict[str, dict[str, Any]] = field(default_factory=dict)
    netem_phases: list[dict[str, Any]] = field(default_factory=list)
    qualification: dict[str, Any] | None = None


def _slug(scenario: Any) -> str:
    return scenario.slug()


def _fixed_count(scenario: Any) -> bool:
    return (
        isinstance(scenario, Scenario)
        and scenario.contract.request_body_path is not None
    )


def _window_text(seconds: float | None) -> str:
    return 'fixed-count' if seconds is None else f'{seconds:g}s'


def _elapsed(started: float, deadline: float) -> str:
    return (
        f'elapsed={time.monotonic() - started:.1f}s '
        f'remaining={max(0.0, deadline - time.monotonic()):.1f}s'
    )


def _scenario_coverage(
    http: Any, profile: ProfileRun, scenario: Any
) -> tuple[tuple[str, ...], dict[str, str], dict[str, str]]:
    supported = []
    unsupported = {}
    not_selected = {}
    for name in profile.servers:
        if profile.family == 'grpc':
            if name == 'grpclib' and not scenario.grpclib:
                not_selected[name] = 'Representative no-compression subset'
                continue
        elif http.unsupported_reason(name, scenario):
            unsupported[name] = http.unsupported_reason(name, scenario)
            continue
        supported.append(name)
    return tuple(supported), unsupported, not_selected


def _profile_scenarios(profile: ProfileRun) -> list[dict[str, Any]]:
    output = []
    for scenario in profile.scenarios:
        calibration = profile.calibrations[_slug(scenario)]
        plan = profile.scenario_plans.get(_slug(scenario), {})
        output.append({
            'family': profile.family,
            'profile': profile.name,
            'slug': _slug(scenario),
            'scenario': scenario.name,
            'supported_servers': list(calibration['supported_servers']),
            'unsupported_servers': calibration.get('unsupported_servers', {}),
            'not_selected_servers': calibration.get('not_selected_servers', {}),
            'calibration': calibration,
            **plan,
        })
    return output


def _initial_failures(calibration: dict[str, Any]) -> dict[str, dict[str, Any]]:
    failures = {}
    for name, cell in calibration.get('servers', {}).items():
        if cell.get('status') == 'failed':
            failures[name] = {
                'phase': 'calibration',
                'error': cell['error'],
                'block': 0,
                'evidence': f'calibration/{name}',
                'status': 'response_validation_failed',
            }
    return failures


def _prepare_http_profiles(
    http: Any,
    suite_directory: Path,
    *,
    profiles: Sequence[Any],
    scenarios: dict[str, tuple[Any, ...]],
    servers: Sequence[str],
    wheel: Path | None,
) -> list[ProfileRun]:
    ensure_static_file_payload()
    if any(
        scenario.type == 'h2_upload'
        for cases in scenarios.values()
        for scenario in cases
    ):
        ensure_large_upload_payload()
    required_tools = {'oha'}
    if any(scenario.type == 'ws' for cases in scenarios.values() for scenario in cases):
        required_tools.add('k6')
    for tool in required_tools:
        if shutil.which(tool) is None:
            raise BenchmarkError(f'{tool} is required but not installed')

    identity = run_identity(
        Path(http.__file__).resolve().parents[2],
        wheel,
    )
    servers = tuple(servers)
    system_summary = http.system_summary()
    package_versions = http.package_versions()
    output = []
    for profile in profiles:
        directory = suite_directory / profile.name
        if directory.exists():
            raise BenchmarkError(f'refusing to overwrite staged run {directory}')
        (directory / 'raw').mkdir(parents=True)
        (directory / 'plots').mkdir()
        hypercorn_config = directory / 'hypercorn.toml'
        http.write_hypercorn_config(hypercorn_config)
        write_json(directory / 'identity.json', identity)
        if identity['build'] is not None:
            shutil.copy2(identity['build']['log']['path'], directory / 'build.log')
        write_json(
            directory / 'profile.json',
            http.profile_record(profile, 'prepared'),
        )
        output.append(
            ProfileRun(
                family='http',
                name=profile.name,
                network_profile=profile,
                scenarios=scenarios.get(profile.name, ()),
                servers=servers,
                directory=directory,
                identity=identity,
                hypercorn_config=hypercorn_config,
                system_summary=system_summary,
                package_versions=package_versions,
            )
        )
    return output


def _prepare_rpc_profile(
    http: Any,
    grpc: Any,
    args: Any,
    suite_directory: Path,
    *,
    servers: Sequence[str],
    wheel: Path | None,
) -> ProfileRun:
    directory = suite_directory / 'grpc'
    if directory.exists():
        raise BenchmarkError(f'refusing to overwrite staged run {directory}')
    (directory / 'raw').mkdir(parents=True)
    (directory / 'plots').mkdir()
    scenarios = tuple(
        scenario
        for scenario in grpc.scenarios()
        if not args.rpc_cases or scenario.case in args.rpc_cases
    )
    identity = grpc.prepare(
        directory,
        wheel,
        connect_wheel=args.connect_wheel,
        development_revision=args.connect_development_revision,
        connect_source=args.connect_source,
        comparator_selection=args.comparator_selection,
    )
    write_json(directory / 'identity.json', identity)
    if identity['build'] is not None:
        shutil.copy2(identity['build']['log']['path'], directory / 'build.log')
    write_json(
        directory / 'profile.json',
        {'profile': 'grpc', 'benchmark_family': 'grpc', 'status': 'prepared'},
    )
    package_versions = {
        name: identity['rpc_artifacts'][name]['version']
        for name in (
            'connectrpc',
            'hypercorn',
            'grpcio',
            'grpclib',
            'protobuf-py',
            'protobuf-py-ext',
        )
    }
    return ProfileRun(
        family='grpc',
        name='grpc',
        network_profile=None,
        scenarios=scenarios,
        servers=tuple(servers),
        directory=directory,
        identity=identity,
        hypercorn_config=directory / 'unused.toml',
        system_summary=http.system_summary(),
        package_versions=package_versions,
    )


def _calibrate_profile(
    http: Any,
    profile: ProfileRun,
    *,
    deadline: float,
    started: float,
) -> None:
    from bench.bench import TrialError

    for index, scenario in enumerate(profile.scenarios):
        slug = _slug(scenario)
        supported, unsupported, not_selected = _scenario_coverage(
            http, profile, scenario
        )
        servers = {}
        fixed_count = _fixed_count(scenario)
        orders = (
            http.rotations(supported, len(supported), http.SEED) if supported else []
        )
        order = orders[index % len(orders)] if orders else []
        print(
            f'phase=calibration profile={profile.name} scenario={scenario.name} '
            f'cells={len(supported)} {_elapsed(started, deadline)}',
            file=sys.stderr,
            flush=True,
        )
        for name in order:
            evidence = profile.directory / 'evidence' / slug / 'calibration' / name
            try:
                with deadline_bound(deadline):
                    result = http.measure_cell(
                        name,
                        scenario,
                        _window_text(CALIBRATION_WINDOWS_SECONDS[0]),
                        '0s',
                        hypercorn_config=profile.hypercorn_config,
                        evidence_directory=evidence,
                        identity=profile.identity,
                        calibration_windows=CALIBRATION_WINDOWS_SECONDS,
                    )
                if 'status' not in result or 'windows' not in result:
                    raise BenchmarkError(
                        f'{name} calibration returned production metrics'
                    )
                servers[name] = result
            except TrialError as error:
                if not isinstance(error.__cause__, ResponseError):
                    raise
                servers[name] = {
                    'server': name,
                    'status': 'failed',
                    'error': str(error),
                    'phase': error.phase,
                }
            print(
                f'  phase=calibration profile={profile.name} '
                f'scenario={scenario.name} server={name} '
                f'status={servers[name]["status"]} '
                f'duration={servers[name].get("selected_duration_seconds")} '
                f'{_elapsed(started, deadline)}',
                file=sys.stderr,
                flush=True,
            )
        try:
            duration, limited = common_duration(
                servers,
                fixed_count=fixed_count,
            )
            status = 'calibrated'
        except ValueError:
            duration, limited = None, True
            status = 'all-cells-failed'
        profile.calibrations[slug] = {
            'scenario': scenario.name,
            'fixed_count': fixed_count,
            'supported_servers': list(supported),
            'unsupported_servers': unsupported,
            'not_selected_servers': not_selected,
            'servers': servers,
            'status': status,
            'duration_seconds': duration,
            'calibration_limited': limited,
        }
        write_json(
            profile.directory / 'evidence' / slug / 'calibration.json',
            profile.calibrations[slug],
        )
    write_json(profile.directory / 'calibrations.json', profile.calibrations)


def _calibrate_rpc_qualification(
    profile: ProfileRun, grpc: Any, *, deadline: float
) -> None:
    """Reuse calibration correctness witnesses; retain distinct cancellation and framework checks."""
    from bench._core import ResponseError
    from bench.bench import TrialError

    cells = []
    for scenario in profile.scenarios:
        calibration = profile.calibrations[_slug(scenario)]
        for name, result in calibration['servers'].items():
            cells.append({
                'case': scenario.case,
                'server': name,
                'status': 'failed' if result['status'] == 'failed' else 'passed',
                **({'error': result['error']} if result['status'] == 'failed' else {}),
            })

    for name in ('h2corn', 'hypercorn', 'grpcio'):
        if name not in profile.servers:
            continue
        cancellation = grpc.RpcScenario('Cancellation', 'download', 32, 2, 1)
        path = profile.directory / 'evidence/cancellation' / name
        try:
            with deadline_bound(deadline):
                grpc.measure_cell(
                    name,
                    cancellation,
                    '0.5s',
                    '0.25s',
                    evidence_directory=path,
                    identity=profile.identity,
                    qualification=True,
                    cancellation=True,
                )
        except TrialError as error:
            if not isinstance(error.__cause__, ResponseError):
                raise
            cells.append({
                'case': 'Cancellation',
                'server': name,
                'status': 'failed',
                'error': str(error),
            })
        else:
            cells.append({
                'case': 'Cancellation',
                'server': name,
                'status': 'passed',
            })

    framework = asyncio.run(grpc.qualify_framework())
    qualification = {
        'application_artifact_kind': profile.identity['rpc_artifacts']['connectrpc'][
            'kind'
        ],
        'connect_sha256': profile.identity['rpc_artifacts']['connectrpc']['sha256'],
        'cells': cells,
        'framework': framework,
    }
    profile.identity['rpc_qualification'] = cells
    profile.qualification = qualification
    write_json(profile.directory / 'identity.json', profile.identity)
    write_json(profile.directory / 'qualification.json', qualification)


def _suite_cost_plan(
    profiles: Sequence[ProfileRun], *, started: float
) -> dict[str, Any]:
    scenario_costs = {}
    scenario_cell_costs = {}
    setup_reserve = 0.0
    for profile in profiles:
        if profile.name == 'rtt50':
            setup_reserve += max(
                (
                    phase.get('netem_setup_elapsed_seconds', 0.0)
                    + phase.get('process_startup_overhead_seconds', 0.0)
                    for phase in profile.netem_phases
                ),
                default=0.0,
            )
        for scenario in profile.scenarios:
            slug = _slug(scenario)
            key = f'{profile.family}/{profile.name}/{slug}'
            calibration = profile.calibrations[slug]
            fixed_count = calibration['fixed_count']
            duration = calibration['duration_seconds']
            cell_costs = {
                name: calibration_production_cost(
                    cell,
                    duration_seconds=None if fixed_count else duration,
                    warmup_seconds=0.0 if fixed_count else PRODUCTION_WARMUP_SECONDS,
                )
                for name, cell in calibration['servers'].items()
                if cell.get('status') != 'failed'
            }
            cost = sum(cell_costs.values())
            scenario_cell_costs[key] = cell_costs
            if cost > 0:
                scenario_costs[key] = cost
    if not scenario_costs:
        raise BenchmarkError('calibration produced no measurable cells')
    plan = allocate_production_blocks(
        scenario_costs=scenario_costs,
        elapsed_seconds=time.monotonic() - started,
        setup_reserve_seconds=setup_reserve,
    )
    for profile in profiles:
        for scenario in profile.scenarios:
            slug = _slug(scenario)
            key = f'{profile.family}/{profile.name}/{slug}'
            calibration = profile.calibrations[slug]
            cell_costs = scenario_cell_costs[key]
            scenario_cost = sum(cell_costs.values())
            profile.scenario_plans[slug] = {
                'family': profile.family,
                'profile': profile.name,
                'slug': slug,
                'scenario': scenario.name,
                'supported_servers': calibration['supported_servers'],
                'duration_seconds': calibration['duration_seconds'],
                'fixed_count': calibration['fixed_count'],
                'calibration_limited': calibration['calibration_limited'],
                'calibration_status': calibration['status'],
                'calibration_servers': calibration['servers'],
                'production_cell_cost_seconds': cell_costs,
                'production_blocks': plan['blocks'].get(key, PRODUCTION_BLOCKS),
                'scenario_cost_seconds': scenario_cost,
            }
    return plan


def _freeze_suite_plan(
    profiles: Sequence[ProfileRun],
    allocation: dict[str, Any],
    *,
    started: float,
    started_at: datetime,
    suite_directory: Path,
) -> dict[str, Any]:
    scenarios = [item for profile in profiles for item in _profile_scenarios(profile)]
    plan = {
        'schema': 1,
        'status': 'frozen',
        'frozen_before_production': True,
        'started_at': started_at.isoformat(),
        'frozen_at_elapsed_seconds': time.monotonic() - started,
        'target_seconds': TARGET_TOTAL_SECONDS,
        'hard_seconds': HARD_TOTAL_SECONDS,
        'report_reserve_seconds': REPORT_RESERVE_SECONDS,
        'cleanup_reserve_seconds': CLEANUP_RESERVE_SECONDS,
        'scenario_count': len(scenarios),
        'supported_cell_count': sum(
            len(item['supported_servers']) for item in scenarios
        ),
        'allocation': allocation,
        'scenarios': scenarios,
    }
    write_json(suite_directory / 'suite-plan.json', plan)
    for profile in profiles:
        write_json(profile.directory / 'suite-plan.json', plan)
    return plan


def _production_profile(
    http: Any,
    grpc: Any,
    profile: ProfileRun,
    *,
    deadline: float,
    started: float,
) -> None:
    for scenario in profile.scenarios:
        slug = _slug(scenario)
        calibration = profile.calibrations[slug]
        scenario_plan = profile.scenario_plans[slug]
        duration_seconds = scenario_plan['duration_seconds']
        fixed_count = scenario_plan['fixed_count']
        duration = '1s' if duration_seconds is None else f'{duration_seconds:g}s'
        warmup = '0s' if fixed_count else f'{PRODUCTION_WARMUP_SECONDS:g}s'
        failures = _initial_failures(calibration)
        record = http.run_scenario(
            scenario,
            profile.servers,
            duration=duration,
            warmup=warmup,
            max_trials=scenario_plan['production_blocks'],
            deadline=deadline,
            hypercorn_config=profile.hypercorn_config,
            evidence_directory=profile.directory / 'evidence' / slug,
            identity=profile.identity,
            phase='production',
            profile=profile.name,
            calibration=calibration['servers'],
            calibration_limited=calibration['calibration_limited'],
            initial_failures=failures,
        )
        record['sampling'] = scenario_plan
        record['duration'] = 'fixed-count' if fixed_count else duration
        record['warmup_duration'] = (
            'none; each client process uses fresh connections'
            if fixed_count
            else warmup
        )
        record['production_started_elapsed_seconds'] = time.monotonic() - started
        record['system_summary'] = profile.system_summary
        record['package_versions'] = profile.package_versions
        record['identity'] = f'../evidence/{profile.name}/identity.json'
        record['evidence'] = f'../evidence/{profile.name}/trials/{slug}'
        if profile.family == 'grpc':
            scenario_rpc = scenario
            record.update(
                benchmark_family='grpc',
                application_artifact_kind=profile.identity['rpc_artifacts'][
                    'connectrpc'
                ]['kind'],
                stack_labels=grpc.LABELS,
                rate_unit=scenario_rpc.unit,
                rpc_case=scenario_rpc.case,
                qualification=profile.qualification,
                network_profile='grpc',
            )
            messages_per_rpc = (
                scenario_rpc.messages
                if scenario_rpc.shape in {'download', 'duplex'}
                else 1
            )
            for result in record['aggregate'].values():
                result['rpc_rate'] = result['rps'] / messages_per_rpc
        else:
            network_profile = profile.network_profile
            assert network_profile is not None
            record.update(
                network_profile=profile.name,
                target_rtt_ms=network_profile.target_rtt_ms,
                measured_rtt_ms=(
                    profile.netem_phases[-1]['measured_rtt_ms']
                    if profile.netem_phases
                    else None
                ),
            )
        write_json(profile.directory / 'raw' / f'{slug}.json', record)
        print(
            f'phase=production profile={profile.name} scenario={scenario.name} '
            f'duration={record["duration"]} blocks={record["planned_trials"]} '
            f'{_elapsed(started, deadline)}',
            file=sys.stderr,
            flush=True,
        )


def _write_profile_state(profile: ProfileRun) -> None:
    write_json(
        profile.directory / 'adaptive-state.json',
        {
            'calibrations': profile.calibrations,
            'scenario_plans': profile.scenario_plans,
            'netem_phases': profile.netem_phases,
        },
    )


def _read_profile_state(profile: ProfileRun) -> None:
    path = profile.directory / 'adaptive-state.json'
    if not path.is_file():
        return
    state = json.loads(path.read_text())
    profile.calibrations = state.get('calibrations', {})
    profile.scenario_plans = state.get('scenario_plans', {})
    profile.netem_phases = state.get('netem_phases', [])


def execute_rtt50_phase(job_path: Path) -> None:
    """Execute one RTT-shaped phase in an isolated network namespace."""
    from bench import bench as http
    from bench import grpc

    job = json.loads(job_path.read_text())
    directory = Path(job['profile_directory'])
    all_scenarios = {scenario.slug(): scenario for scenario in http.rtt50_scenarios()}
    scenarios = tuple(all_scenarios[slug] for slug in job['scenario_slugs'])
    profile = ProfileRun(
        family='http',
        name='rtt50',
        network_profile=http.RTT50_PROFILE,
        scenarios=scenarios,
        servers=tuple(job['servers']),
        directory=directory,
        identity=json.loads((directory / 'identity.json').read_text()),
        hypercorn_config=directory / 'hypercorn.toml',
        system_summary=job['system_summary'],
        package_versions=job['package_versions'],
    )
    _read_profile_state(profile)
    if job['phase'] == 'production':
        suite_plan = json.loads(Path(job['suite_plan_path']).read_text())
        profile.scenario_plans = {
            item['slug']: item
            for item in suite_plan['scenarios']
            if item['family'] == 'http' and item['profile'] == 'rtt50'
        }

    work_deadline = float(job['work_deadline'])
    started = float(job['started_monotonic'])
    with deadline_bound(work_deadline):
        phase_started = time.monotonic()
        netem_started = time.monotonic()
        netem = http.configure_rtt50_profile()
        phase_record = {
            'phase': job['phase'],
            'netem_setup_elapsed_seconds': time.monotonic() - netem_started,
            **netem,
        }
        profile.netem_phases.append(phase_record)
        write_json(
            directory / 'evidence' / 'netem' / f'{job["phase"]}.json',
            netem,
        )
        if job['phase'] == 'calibration':
            _calibrate_profile(http, profile, deadline=work_deadline, started=started)
        elif job['phase'] == 'production':
            _production_profile(
                http,
                grpc,
                profile,
                deadline=work_deadline,
                started=started,
            )
        else:
            raise BenchmarkError(f'unknown RTT phase {job["phase"]!r}')
        phase_record['worker_phase_elapsed_seconds'] = time.monotonic() - phase_started
        _write_profile_state(profile)
        profile_record = json.loads((directory / 'profile.json').read_text())
        profile_record.update(
            netem_phases=profile.netem_phases,
            measured_rtt_ms=netem['measured_rtt_ms'],
        )
        write_json(directory / 'profile.json', profile_record)


def _run_rtt50_phase(
    http: Any,
    profile: ProfileRun,
    phase: str,
    *,
    suite_plan_path: Path | None,
    work_deadline: float,
    started: float,
) -> None:
    unshare = shutil.which('unshare')
    if unshare is None:
        raise BenchmarkError('unshare is required for the isolated RTT profile')
    job_path = profile.directory / f'netem-{phase}.job.json'
    write_json(
        job_path,
        {
            'phase': phase,
            'profile_directory': str(profile.directory),
            'scenario_slugs': [_slug(scenario) for scenario in profile.scenarios],
            'servers': profile.servers,
            'system_summary': profile.system_summary,
            'package_versions': profile.package_versions,
            'work_deadline': work_deadline,
            'started_monotonic': started,
            'suite_plan_path': str(suite_plan_path) if suite_plan_path else None,
        },
    )
    command = [
        unshare,
        '-rn',
        '--',
        sys.executable,
        '-m',
        'bench.netem_worker',
        '--job',
        str(job_path),
    ]
    process_started = time.monotonic()
    try:
        process = subprocess.Popen(
            command,
            cwd=Path(http.__file__).resolve().parents[1],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
    except OSError as error:
        raise BenchmarkError(f'failed to start RTT worker: {error}') from error
    try:
        stdout, stderr = process.communicate(
            timeout=max(0.001, work_deadline - time.monotonic())
        )
    except BaseException as error:
        terminated = terminate_process_tree(
            process,
            timeout=CLEANUP_RESERVE_SECONDS,
        )
        stdout, stderr = process.communicate()
        write_json(
            profile.directory / 'evidence' / 'netem' / f'{phase}.process.json',
            {
                'command': command,
                'status': (
                    'timed_out'
                    if isinstance(error, subprocess.TimeoutExpired)
                    else 'interrupted'
                ),
                'elapsed_seconds': time.monotonic() - process_started,
                'terminated_pids': terminated,
                'stdout': stdout,
                'stderr': stderr,
                'error': str(error) or type(error).__name__,
            },
        )
        if isinstance(error, subprocess.TimeoutExpired):
            raise DeadlineError(
                f'RTT {phase} worker exceeded the remaining suite deadline'
            ) from error
        raise
    result = {
        'command': command,
        'returncode': process.returncode,
        'stdout': stdout,
        'stderr': stderr,
        'elapsed_seconds': time.monotonic() - process_started,
        'status': 'completed' if process.returncode == 0 else 'failed',
    }
    write_json(
        profile.directory / 'evidence' / 'netem' / f'{phase}.process.json',
        result,
    )
    if process.returncode:
        if time.monotonic() >= work_deadline:
            raise DeadlineError(f'RTT {phase} worker reached the suite work deadline')
        raise BenchmarkError(
            f'RTT {phase} worker failed: {stderr.strip() or stdout.strip()}'
        )
    _read_profile_state(profile)
    profile_record = json.loads((profile.directory / 'profile.json').read_text())
    profile.netem_phases = profile_record.get('netem_phases', profile.netem_phases)
    if profile.netem_phases:
        phase_record = profile.netem_phases[-1]
        phase_record['worker_process_elapsed_seconds'] = result['elapsed_seconds']
        phase_record['process_startup_overhead_seconds'] = max(
            0.0,
            result['elapsed_seconds']
            - phase_record.get('worker_phase_elapsed_seconds', 0.0),
        )
        _write_profile_state(profile)
        profile_record['netem_phases'] = profile.netem_phases
        write_json(profile.directory / 'profile.json', profile_record)


def _selected_profiles(
    http: Any,
    grpc: Any,
    args: Any,
) -> tuple[
    list[Any],
    dict[str, tuple[Any, ...]],
    list[Any] | None,
    tuple[str, ...],
    tuple[str, ...],
]:
    http_profiles = []
    http_scenarios: dict[str, tuple[Any, ...]] = {}
    http_servers = tuple(
        name for name in (args.servers or http.SERVERS) if name in http.SERVERS
    )
    grpc_servers = tuple(
        name for name in (args.servers or grpc.SERVERS) if name in grpc.SERVERS
    )
    if args.family in {'all', 'http'}:
        if not http_servers:
            raise BenchmarkError('HTTP family has no selected server')
        if args.network_profile in {'all', 'loopback'}:
            http_scenarios['loopback'] = tuple(
                scenario
                for scenario in http.benchmark_scenarios()
                if not args.types or scenario.type in args.types
            )
            if http_scenarios['loopback']:
                http_profiles.append(http.LOOPBACK_PROFILE)
        if args.network_profile in {'all', 'rtt50'}:
            http_scenarios['rtt50'] = tuple(
                scenario
                for scenario in http.rtt50_scenarios()
                if not args.types or scenario.type in args.types
            )
            if http_scenarios['rtt50']:
                http_profiles.append(http.RTT50_PROFILE)
    if args.family in {'all', 'grpc'} and not grpc_servers:
        raise BenchmarkError('gRPC family has no selected server')
    rpc_selected = (
        [
            scenario
            for scenario in grpc.scenarios()
            if not args.rpc_cases or scenario.case in args.rpc_cases
        ]
        if args.family in {'all', 'grpc'}
        else None
    )
    if not http_profiles and not rpc_selected:
        raise BenchmarkError('scenario selection is empty')
    return http_profiles, http_scenarios, rpc_selected, http_servers, grpc_servers


def _write_suite_status(
    suite_directory: Path,
    status: str,
    *,
    started: float,
    hard_deadline: float,
    error: str | None = None,
    forecast: dict[str, Any] | None = None,
) -> None:
    write_json(
        suite_directory / 'suite-result.json',
        {
            'status': status,
            'elapsed_seconds': time.monotonic() - started,
            'hard_deadline_monotonic': hard_deadline,
            'remaining_seconds': max(0.0, hard_deadline - time.monotonic()),
            'error': error,
            'forecast': forecast,
        },
    )


def _combined_publish_gate(
    profiles: Sequence[ProfileRun],
    suite_plan: dict[str, Any],
) -> None:
    from bench.bench import SERVERS as HTTP_SERVERS
    from bench.grpc import SERVERS as RPC_SERVERS

    profile_names = {profile.name for profile in profiles}
    if profile_names != {'loopback', 'rtt50', 'grpc'}:
        raise BenchmarkError(
            'publication requires loopback HTTP, RTT-shaped HTTP, and gRPC profiles'
        )
    if any(
        set(profile.servers)
        != (set(RPC_SERVERS) if profile.family == 'grpc' else set(HTTP_SERVERS))
        for profile in profiles
    ):
        raise BenchmarkError('publication requires every benchmark server')
    if (
        suite_plan.get('scenario_count') != 35
        or suite_plan.get('supported_cell_count') != 109
    ):
        raise BenchmarkError(
            'publication requires all 35 scenarios and 109 supported cells'
        )
    planned = {
        (item['family'], item['profile'], item['slug'])
        for item in suite_plan.get('scenarios', ())
    }
    expected = {
        (profile.family, profile.name, _slug(scenario))
        for profile in profiles
        for scenario in profile.scenarios
    }
    if planned != expected:
        raise BenchmarkError('frozen plan does not cover every selected scenario')


def run_adaptive_suite(
    args: Any,
    http: Any,
    *,
    started: float,
    started_at: datetime,
    work_deadline: float,
    hard_deadline: float,
) -> Path:
    from bench import grpc

    http_profiles, http_scenarios, rpc_scenarios, http_servers, grpc_servers = (
        _selected_profiles(http, grpc, args)
    )
    suite_directory = args.run_directory or args.output_directory.resolve() / 'runs' / (
        datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ') + f'-{secrets.token_hex(3)}'
    )
    suite_directory = Path(suite_directory).resolve()
    suite_directory.mkdir(parents=True, exist_ok=False)
    http_directory = suite_directory / 'http'
    http_directory.mkdir()
    profiles: list[ProfileRun] = []
    try:
        with http.deadline_bound(work_deadline):
            print(
                f'phase=prepare profile=all {_elapsed(started, work_deadline)}',
                file=sys.stderr,
                flush=True,
            )
            if http_profiles:
                profiles.extend(
                    _prepare_http_profiles(
                        http,
                        http_directory,
                        profiles=http_profiles,
                        scenarios=http_scenarios,
                        servers=http_servers,
                        wheel=args.wheel,
                    )
                )
            rpc_profile = (
                _prepare_rpc_profile(
                    http,
                    grpc,
                    args,
                    suite_directory,
                    servers=grpc_servers,
                    wheel=args.wheel,
                )
                if rpc_scenarios is not None
                else None
            )
            if rpc_profile is not None:
                assert rpc_scenarios is not None
                rpc_profile.scenarios = tuple(rpc_scenarios)
                profiles.append(rpc_profile)

            for profile in profiles:
                if profile.name == 'rtt50':
                    _run_rtt50_phase(
                        http,
                        profile,
                        'calibration',
                        suite_plan_path=None,
                        work_deadline=work_deadline,
                        started=started,
                    )
                else:
                    _calibrate_profile(
                        http, profile, deadline=work_deadline, started=started
                    )
                    _write_profile_state(profile)

            if rpc_profile is not None:
                _calibrate_rpc_qualification(rpc_profile, grpc, deadline=work_deadline)
                _write_profile_state(rpc_profile)
                if args.qualify_only:
                    _write_suite_status(
                        suite_directory,
                        'qualified',
                        started=started,
                        hard_deadline=hard_deadline,
                    )
                    return suite_directory
            elif args.qualify_only:
                raise BenchmarkError('--qualify-only requires --family grpc')

            allocation = _suite_cost_plan(profiles, started=started)
            suite_plan = _freeze_suite_plan(
                profiles,
                allocation,
                started=started,
                started_at=started_at,
                suite_directory=suite_directory,
            )
            suite_plan.update(
                allocation=allocation,
                scenario_count=sum(len(profile.scenarios) for profile in profiles),
                supported_cell_count=sum(
                    len(item['supported_servers']) for item in suite_plan['scenarios']
                ),
            )
            write_json(suite_directory / 'suite-plan.json', suite_plan)
            for profile in profiles:
                write_json(profile.directory / 'suite-plan.json', suite_plan)
                print(
                    f'phase=plan profile={profile.name} '
                    f'scenarios={len(profile.scenarios)} '
                    f'forecast={allocation["forecast_total_seconds"]:.1f}s '
                    f'target={TARGET_TOTAL_SECONDS}s '
                    f'hard={HARD_TOTAL_SECONDS}s '
                    f'{_elapsed(started, hard_deadline)}',
                    file=sys.stderr,
                    flush=True,
                )

            if args.publish:
                _combined_publish_gate(profiles, suite_plan)

            for profile in profiles:
                if profile.name == 'rtt50':
                    _run_rtt50_phase(
                        http,
                        profile,
                        'production',
                        suite_plan_path=suite_directory / 'suite-plan.json',
                        work_deadline=work_deadline,
                        started=started,
                    )
                else:
                    _production_profile(
                        http,
                        grpc,
                        profile,
                        deadline=work_deadline,
                        started=started,
                    )
                    _write_profile_state(profile)

        for profile in profiles:
            raw_directory = profile.directory / 'raw'
            for path in sorted(raw_directory.glob('*.json')):
                record = json.loads(path.read_text(encoding='utf-8'))
                http.plot_results(
                    record, profile.directory / 'plots' / f'{path.stem}.svg'
                )
            records = [
                json.loads(path.read_text()) for path in raw_directory.glob('*.json')
            ]
            failures = any(record.get('failures') for record in records)
            profile_data = json.loads((profile.directory / 'profile.json').read_text())
            profile_data.update(
                status='completed_with_failures' if failures else 'completed',
                benchmark_family=profile.family,
                suite_plan='suite-plan.json',
            )
            write_json(profile.directory / 'profile.json', profile_data)

        _write_suite_status(
            suite_directory,
            'completed',
            started=started,
            hard_deadline=hard_deadline,
            forecast=suite_plan['allocation'],
        )

        if args.publish:
            from bench.bench import assert_publishable, publish

            http_directories = [
                profile.directory for profile in profiles if profile.family == 'http'
            ]
            rpc_directories = [
                profile.directory for profile in profiles if profile.family == 'grpc'
            ]
            if len(rpc_directories) != 1:
                raise BenchmarkError(
                    'combined publication requires one complete gRPC profile'
                )
            assert_publishable([*http_directories, *rpc_directories])
            publish(http_directories)
            publish(rpc_directories)

    except (BudgetError, DeadlineError) as error:
        _write_suite_status(
            suite_directory,
            'budget_limited',
            started=started,
            hard_deadline=hard_deadline,
            error=str(error),
        )
        raise BenchmarkError(str(error)) from error
    except BaseException as error:
        _write_suite_status(
            suite_directory,
            'incomplete',
            started=started,
            hard_deadline=hard_deadline,
            error=str(error) or type(error).__name__,
        )
        raise
    return suite_directory
