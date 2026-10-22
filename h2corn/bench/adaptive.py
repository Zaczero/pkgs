"""Combined adaptive HTTP and RPC benchmark collection."""

from __future__ import annotations

import asyncio
import json
import secrets
import shutil
import sys
import time
from dataclasses import asdict, dataclass, field
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
    ensure_static_file_payload,
    write_json,
)
from bench.evidence import run_identity
from bench.sampling import (
    CLEANUP_RESERVE_SECONDS,
    HARD_TOTAL_SECONDS,
    REPORT_RESERVE_SECONDS,
    SAMPLING_POLICY,
    TARGET_TOTAL_SECONDS,
    BudgetError,
    allocate_production_blocks,
    calibration_production_cost,
    calibration_windows,
    common_duration,
    production_warmup_seconds,
)


@dataclass(slots=True)
class ProfileRun:
    family: str
    name: str
    scenarios: tuple[Any, ...]
    servers: tuple[str, ...]
    directory: Path
    identity: dict[str, Any]
    hypercorn_config: Path
    system_summary: str
    package_versions: dict[str, str | None]
    calibrations: dict[str, dict[str, Any]] = field(default_factory=dict)
    scenario_plans: dict[str, dict[str, Any]] = field(default_factory=dict)
    qualification: dict[str, Any] | None = None


def _slug(scenario: Any) -> str:
    return scenario.slug()


def _minimum_window(scenario: Any) -> float:
    return (
        scenario.contract.minimum_duration_seconds
        if isinstance(scenario, Scenario)
        else 0.0
    )


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


def _prepare_http_profile(
    http: Any,
    suite_directory: Path,
    *,
    scenarios: tuple[Any, ...],
    servers: Sequence[str],
    wheel: Path | None,
) -> ProfileRun:
    ensure_static_file_payload()
    required_tools = {'oha'}
    if any(scenario.type == 'ws' for scenario in scenarios):
        required_tools.add('k6')
    for tool in required_tools:
        if shutil.which(tool) is None:
            raise BenchmarkError(f'{tool} is required but not installed')

    identity = run_identity(
        Path(http.__file__).resolve().parents[2],
        wheel,
    )
    directory = suite_directory / 'loopback'
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
        {'profile': 'loopback', 'benchmark_family': 'http', 'status': 'prepared'},
    )
    return ProfileRun(
        family='http',
        name='loopback',
        scenarios=scenarios,
        servers=tuple(servers),
        directory=directory,
        identity=identity,
        hypercorn_config=hypercorn_config,
        system_summary=http.system_summary(),
        package_versions=http.package_versions(),
    )


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
            windows = calibration_windows(_minimum_window(scenario))
            try:
                with deadline_bound(deadline):
                    result = http.measure_cell(
                        name,
                        scenario,
                        f'{windows[0]:g}s',
                        '0s',
                        hypercorn_config=profile.hypercorn_config,
                        evidence_directory=evidence,
                        identity=profile.identity,
                        calibration_windows=windows,
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
            duration, limited = common_duration(servers)
            status = 'calibrated'
        except ValueError:
            duration, limited = None, True
            status = 'all-cells-failed'
        profile.calibrations[slug] = {
            'scenario': scenario.name,
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
    if any(cell['status'] != 'passed' for cell in framework):
        raise BenchmarkError('gRPC application framework qualification failed')


def _suite_cost_plan(
    profiles: Sequence[ProfileRun], *, started: float
) -> dict[str, Any]:
    scenario_costs = {}
    scenario_cell_costs = {}
    for profile in profiles:
        for scenario in profile.scenarios:
            slug = _slug(scenario)
            key = f'{profile.family}/{profile.name}/{slug}'
            calibration = profile.calibrations[slug]
            duration = calibration['duration_seconds']
            cell_costs = {
                name: calibration_production_cost(
                    cell,
                    duration_seconds=duration,
                    warmup_seconds=production_warmup_seconds(_minimum_window(scenario)),
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
                'calibration_limited': calibration['calibration_limited'],
                'calibration_status': calibration['status'],
                'calibration_servers': calibration['servers'],
                'production_cell_cost_seconds': cell_costs,
                'minimum_blocks': SAMPLING_POLICY.looks[0],
                'maximum_blocks': SAMPLING_POLICY.looks[-1],
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
        'sampling_policy': asdict(SAMPLING_POLICY),
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
    required_seconds_other_profiles: float = 0.0,
) -> None:
    frozen_scenarios = _profile_scenarios(profile)
    for index, scenario in enumerate(profile.scenarios):
        slug = _slug(scenario)
        calibration = profile.calibrations[slug]
        scenario_plan = frozen_scenarios[index]
        duration = f'{scenario_plan["duration_seconds"]:g}s'
        warmup = f'{production_warmup_seconds(_minimum_window(scenario)):g}s'
        failures = _initial_failures(calibration)
        record = http.run_scenario(
            scenario,
            profile.servers,
            duration=duration,
            warmup=warmup,
            deadline=deadline,
            hypercorn_config=profile.hypercorn_config,
            evidence_directory=profile.directory / 'evidence' / slug,
            identity=profile.identity,
            phase='production',
            profile=profile.name,
            calibration=calibration['servers'],
            calibration_limited=calibration['calibration_limited'],
            initial_failures=failures,
            estimated_block_seconds=scenario_plan['scenario_cost_seconds'],
            required_seconds_after_scenario=(
                required_seconds_other_profiles
                + sum(
                    profile.scenario_plans[_slug(later)]['scenario_cost_seconds']
                    * SAMPLING_POLICY.looks[0]
                    for later in profile.scenarios[index + 1 :]
                )
            ),
        )
        record['sampling'] = scenario_plan
        record['duration'] = duration
        record['warmup_duration'] = warmup
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
            record['network_profile'] = profile.name
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
        },
    )


def _selected_profiles(
    http: Any,
    grpc: Any,
    args: Any,
) -> tuple[
    tuple[Any, ...],
    list[Any] | None,
    tuple[str, ...],
    tuple[str, ...],
]:
    http_scenarios: tuple[Any, ...] = ()
    http_servers = tuple(
        name for name in (args.servers or http.SERVERS) if name in http.SERVERS
    )
    grpc_servers = tuple(
        name for name in (args.servers or grpc.SERVERS) if name in grpc.SERVERS
    )
    if args.family in {'all', 'http'}:
        if not http_servers:
            raise BenchmarkError('HTTP family has no selected server')
        http_scenarios = tuple(
            scenario
            for scenario in http.benchmark_scenarios()
            if not args.types or scenario.type in args.types
        )
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
    if not http_scenarios and not rpc_selected:
        raise BenchmarkError('scenario selection is empty')
    return http_scenarios, rpc_selected, http_servers, grpc_servers


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
    from bench.bench import benchmark_scenarios
    from bench.grpc import SERVERS as RPC_SERVERS
    from bench.grpc import scenarios as rpc_scenarios

    if any(
        set(profile.servers)
        != (set(RPC_SERVERS) if profile.family == 'grpc' else set(HTTP_SERVERS))
        for profile in profiles
    ):
        raise BenchmarkError('publication requires every benchmark server')
    planned = {
        (item['family'], item['profile'], item['slug'])
        for item in suite_plan.get('scenarios', ())
    }
    complete = {
        ('http', 'loopback', scenario.slug()) for scenario in benchmark_scenarios()
    } | {('grpc', 'grpc', scenario.slug()) for scenario in rpc_scenarios()}
    if planned != complete:
        raise BenchmarkError('publication requires every defined scenario')


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

    http_scenarios, rpc_scenarios, http_servers, grpc_servers = _selected_profiles(
        http, grpc, args
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
            if http_scenarios:
                profiles.append(
                    _prepare_http_profile(
                        http,
                        http_directory,
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

            for index, profile in enumerate(profiles):
                _production_profile(
                    http,
                    grpc,
                    profile,
                    deadline=work_deadline,
                    started=started,
                    required_seconds_other_profiles=sum(
                        scenario_plan['scenario_cost_seconds']
                        * SAMPLING_POLICY.looks[0]
                        for later in profiles[index + 1 :]
                        for scenario_plan in later.scenario_plans.values()
                    ),
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
