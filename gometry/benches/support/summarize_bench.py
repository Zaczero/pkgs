"""Normalize self-contained paired benchmark block artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import sys
from dataclasses import dataclass
from glob import glob
from html import escape
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from collections.abc import Sequence

_BENCHES = Path(__file__).resolve().parents[1]
GOMETRY_ROOT = _BENCHES.parent
_TOOLS = GOMETRY_ROOT / 'tools'
if str(_TOOLS) not in sys.path:
    sys.path.insert(0, str(_TOOLS))

from _gatelib import prepend_tools_import_paths

prepend_tools_import_paths()

from benchroom.sampling import SamplingPolicy, assess_ratios
from _bench_registry import RESOURCE_MODES, paired_block_identity, public_api_symbols
from benchmark_observation import measurement_summary, validate_observation
from benchroom.stats import median_interval
from benchmark_identity import common_identity, harness_commits_match
from benchmark_runtime import identity_sha256
from public_inventory import unknown_public_symbols

DOMAIN_ORDER = (
    'Array construction & I/O',
    'Geometry',
    'CRS & geodesy',
    'Discrete global grids',
    'Spatial index',
)
PRACTICAL_EQUIVALENCE_MARGIN = 0.03
DEFAULT_BOOTSTRAP_SEED = 20_260_824
SMOKE_BANNER = 'Smoke/debug run — not publishable release evidence.'
SAMPLING_PLAN_ARTIFACT = 'sampling-plan.json'


@dataclass(frozen=True, slots=True)
class PairedBlock:
    artifact: str
    block_id: str
    pair_id: str
    lead: str
    metric: str
    unit: str
    gometry_median: float
    competitor_median: float
    ratio: float
    arm_identities: dict[str, dict[str, Any]]
    phase: str
    scaling: dict[str, Any] | None
    useful_work: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class BenchStats:
    name: str
    metric: str
    unit: str
    mean: float
    median: float
    stdev: float
    nrun: int
    samples: int
    blocks: tuple[PairedBlock, ...]


@dataclass(frozen=True, slots=True)
class PublicOpMeta:
    domain: str
    label: str
    workload: str
    suite: str
    canonical_symbols: tuple[str, ...]
    gometry: str
    competitor: str
    competitor_label: str
    footnotes: tuple[str, ...]
    phase: str
    scaling: dict[str, Any] | None


@dataclass(slots=True)
class OpRow:
    meta: PublicOpMeta
    gometry: BenchStats
    competitor: BenchStats
    ratio: float
    ratio_interval: tuple[float, float] | None
    blocks: tuple[PairedBlock, ...]
    status: str

    @property
    def speedup(self) -> float:
        return self.ratio

    @property
    def tie(self) -> bool:
        return self.status == 'similar'

    @property
    def noisy(self) -> bool:
        return self.status == 'inconclusive'


def _manifest_payload(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    if 'artifact_admission' in payload:
        from benchmark_evidence import expand_run_manifest

        try:
            return expand_run_manifest(payload, path)
        except (OSError, ValueError, KeyError, TypeError) as error:
            raise SystemExit(f'invalid run admission evidence: {error}') from error
    return payload


def load_run_artifacts(
    payload: dict[str, Any], manifest_path: Path
) -> dict[Path, dict[str, Any]]:
    """Resolve and parse every artifact one run manifest lists."""
    artifacts = payload.get('artifacts')
    if not isinstance(artifacts, list):
        return {}
    documents: dict[Path, dict[str, Any]] = {}
    from benchmark_evidence import expand_artifact, run_admission

    try:
        admission, admission_sha256 = run_admission(payload, manifest_path)
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise SystemExit(f'invalid run admission evidence: {error}') from error
    for artifact in artifacts:
        if not isinstance(artifact, str):
            raise SystemExit(
                f'run manifest {manifest_path} has a non-string artifact entry'
            )
        candidate = Path(artifact)
        if not candidate.is_absolute():
            candidate = manifest_path.parent / candidate
        if not candidate.is_file():
            raise SystemExit(
                f'run manifest {manifest_path} references a missing artifact: {artifact}'
            )
        document = _manifest_payload(candidate)
        if document is None:
            raise SystemExit(f'invalid benchmark artifact {candidate}')
        if candidate in documents:
            raise SystemExit(f'duplicate benchmark artifact reference: {artifact}')
        try:
            documents[candidate] = expand_artifact(
                document, admission, admission_sha256
            )
        except (ValueError, KeyError, TypeError) as error:
            raise SystemExit(
                f'invalid benchmark evidence {candidate}: {error}'
            ) from error
    return documents


def _resolve_run(pattern: str) -> tuple[dict[str, Any], dict[Path, dict[str, Any]]]:
    path = Path(pattern)
    if path.is_dir():
        manifests = [
            candidate
            for candidate in sorted(path.glob('*.json'))
            if (_manifest_payload(candidate) or {}).get('profile')
        ]
        if len(manifests) > 1:
            raise SystemExit(
                f'{path} contains multiple benchmark runs; pass one exact run manifest'
            )
        if not manifests:
            raise SystemExit(f'{path} has no bench.py run manifest')
        path = manifests[0]
    if not path.exists():
        matched = sorted(Path(match) for match in glob(pattern))
        if len(matched) == 1 and matched[0].suffix == '.json':
            path = matched[0]
        elif matched:
            raise SystemExit(
                'summarize_bench requires one run manifest JSON; '
                f'matched {len(matched)} paths'
            )
        else:
            raise SystemExit(f'no benchmark result files matched: {pattern}')
    payload = _manifest_payload(path)
    if payload is None or 'profile' not in payload:
        raise SystemExit(f'{path} is not a bench.py run manifest')
    if 'public_operations' not in payload:
        raise SystemExit(f'run manifest {path} lacks public_operations metadata')
    return payload, load_run_artifacts(payload, path)


def _parse_public_operations(raw: Any) -> list[PublicOpMeta]:
    if not isinstance(raw, list) or not raw:
        raise SystemExit('public_operations must be a non-empty list')
    operations: list[PublicOpMeta] = []
    seen: set[str] = set()
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            raise SystemExit(f'public_operations[{index}] is not an object')
        try:
            domain = str(item['domain'])
            label = str(item['label'])
            workload = str(item['workload'])
            suite = str(item['suite'])
            raw_canonical = item['canonical_symbols']
            gometry = str(item['gometry'])
            competitor = str(item['competitor'])
            competitor_label = str(item['competitor_label'])
            phase = str(item['phase'])
        except KeyError as exc:
            raise SystemExit(
                f'public_operations[{index}] missing field: {exc.args[0]}'
            ) from exc
        if phase not in {'warm', 'steady', 'first-use'}:
            raise SystemExit(f'public_operations[{index}] has an invalid phase')
        raw_scaling = item.get('scaling')
        scaling = raw_scaling if isinstance(raw_scaling, dict) else None
        if scaling is not None:
            required = {
                'curve_id',
                'dimension',
                'unit',
                'point',
                'point_label',
                'useful_work_unit',
            }
            if set(scaling) != required:
                raise SystemExit(
                    f'public_operations[{index}] has incomplete scaling metadata'
                )
        if gometry in seen:
            raise SystemExit(f'duplicate public gometry row: {gometry}')
        if not competitor or not competitor_label:
            raise SystemExit(f'public_operations[{index}] must be fully paired')
        if not isinstance(raw_canonical, (list, tuple)) or not raw_canonical:
            raise SystemExit(
                f'public_operations[{index}] must declare canonical_symbols'
            )
        canonical_symbols = tuple(str(value) for value in raw_canonical)
        unknown = unknown_public_symbols(canonical_symbols, public_api_symbols())
        if unknown:
            raise SystemExit(
                f'public_operations[{index}] maps to non-public API symbol(s): '
                + ', '.join(unknown)
            )
        seen.add(gometry)
        operations.append(
            PublicOpMeta(
                domain=domain,
                label=label,
                workload=workload,
                suite=suite,
                canonical_symbols=canonical_symbols,
                gometry=gometry,
                competitor=competitor,
                competitor_label=competitor_label,
                footnotes=tuple(str(value) for value in (item.get('footnotes') or ())),
                phase=phase,
                scaling=scaling,
            )
        )
    _validate_scaling_metadata(operations)
    return operations


def _sampling_policy(sampling: dict[str, Any]) -> SamplingPolicy:
    record = sampling.get('policy')
    if not isinstance(record, dict):
        raise SystemExit('adaptive run manifest lacks its prespecified policy')
    try:
        policy = SamplingPolicy(
            looks=tuple(record['looks']),
            error_probabilities=tuple(record['error_probabilities']),
            precision_ratio=record['precision_ratio'],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise SystemExit(
            f'run manifest has an invalid sampling policy: {error}'
        ) from error
    if (
        record.get('confidence') != policy.confidence
        or sampling.get('initial_look') != policy.looks[0]
        or sampling.get('maximum_look') != policy.looks[-1]
    ):
        raise SystemExit('run manifest sampling policy summary disagrees')
    return policy


def expected_paired_matrix(
    payload: dict[str, Any],
) -> dict[tuple[str, str, str, str, str], dict[str, Any]]:
    """Derive every accepted paired artifact identity from one run manifest."""
    operations = _parse_public_operations(payload.get('public_operations'))
    sampling = payload.get('sampling')
    if not isinstance(sampling, dict):
        raise SystemExit('run manifest lacks paired sampling identity')
    if sampling.get('mode') == 'adaptive':
        actual_plan = sampling.get('actual_plan')
        policy = _sampling_policy(sampling)
        if not isinstance(actual_plan, dict) or set(actual_plan) != {
            operation.gometry for operation in operations
        }:
            raise SystemExit('run manifest has incomplete adaptive operation plan')
        expected: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
        for index, operation in enumerate(operations, start=1):
            record = actual_plan[operation.gometry]
            if not isinstance(record, dict):
                raise SystemExit(
                    f'run manifest has malformed adaptive plan for {operation.gometry}'
                )
            block_ids = record.get('block_ids')
            leads = record.get('lead_orders')
            if (
                not isinstance(block_ids, list)
                or not isinstance(leads, list)
                or len(block_ids) != len(leads)
                or len(block_ids) not in policy.looks
            ):
                raise SystemExit(
                    f'run manifest has an unallowed block count for {operation.gometry}'
                )
            for block_number, (block_id, lead) in enumerate(
                zip(block_ids, leads, strict=True), start=1
            ):
                pair_id, expected_id, expected_lead = paired_block_identity(
                    operation.suite, index, block_number
                )
                if block_id != expected_id or lead != expected_lead:
                    raise SystemExit(
                        f'run manifest sampling plan changed for {operation.gometry}'
                    )
                key = (
                    operation.gometry,
                    operation.competitor,
                    pair_id,
                    block_id,
                    lead,
                )
                expected[key] = {
                    'operation': operation,
                    'block_number': block_number,
                }
        return expected
    blocks = sampling.get('measured_blocks')
    leads = sampling.get('lead_orders')
    if not isinstance(blocks, int) or blocks < 1 or not isinstance(leads, list):
        raise SystemExit('run manifest has malformed paired sampling identity')
    if len(leads) != blocks or any(lead not in {'ab', 'ba'} for lead in leads):
        raise SystemExit('run manifest has malformed lead-order matrix')
    expected: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    for number, operation in enumerate(operations, start=1):
        for block_number, _lead in enumerate(leads, start=1):
            pair_id, block_id, lead = paired_block_identity(
                operation.suite, number, block_number
            )
            key = (operation.gometry, operation.competitor, pair_id, block_id, lead)
            expected[key] = {
                'operation': operation,
                'block_number': block_number,
            }
    return expected


def validate_exact_paired_matrix(
    payload: dict[str, Any], artifacts: dict[Path, dict[str, Any]]
) -> None:
    """Reject any artifact set other than the run's exact paired matrix."""
    sampling = payload.get('sampling')
    if isinstance(sampling, dict) and sampling.get('mode') == 'adaptive':
        policy = _sampling_policy(sampling)
        if (
            payload.get('profile') == 'release'
            and payload.get('full_manifest') is True
            and payload.get('filter') is None
        ):
            planned = payload.get('planned_commands')
            resource_plans = (
                [
                    command
                    for command in planned
                    if isinstance(command, dict) and command.get('kind') == 'resource'
                ]
                if isinstance(planned, list)
                else []
            )
            if {plan.get('label') for plan in resource_plans} != {
                f'resource-{mode}' for mode in RESOURCE_MODES
            }:
                raise SystemExit(
                    'release sampling plan must retain all four required resource probes'
                )
        artifact_name = payload.get('sampling_plan_artifact')
        expected_digest = sampling.get('plan_sha256')
        if (
            not isinstance(artifact_name, str)
            or artifact_name != SAMPLING_PLAN_ARTIFACT
            or not isinstance(expected_digest, str)
        ):
            raise SystemExit('run manifest lacks its pre-measurement sampling plan')
        plans = [
            document
            for path, document in artifacts.items()
            if path.name == Path(artifact_name).name
            and document.get('artifact_type') == 'gometry-benchmark-sampling-plan'
        ]
        if len(plans) != 1:
            raise SystemExit(
                'run manifest sampling plan artifact is missing or duplicated'
            )
        plan = plans[0]
        try:
            digest = hashlib.sha256(
                json.dumps(plan, sort_keys=True, separators=(',', ':')).encode()
            ).hexdigest()
        except (TypeError, ValueError) as error:
            raise SystemExit(
                f'run manifest sampling plan is malformed: {error}'
            ) from error
        if digest != expected_digest:
            raise SystemExit('run manifest sampling plan changed after collection')
        expected_plan_operations: dict[str, dict[str, Any]] = {}
        operations = _parse_public_operations(payload.get('public_operations'))
        for index, operation in enumerate(operations, start=1):
            pair_id, _, _ = paired_block_identity(operation.suite, index, 1)
            blocks = []
            for number in range(1, policy.looks[-1] + 1):
                _, block_id, lead = paired_block_identity(
                    operation.suite, index, number
                )
                blocks.append({'block_id': block_id, 'lead': lead})
            expected_plan_operations[operation.gometry] = {
                'competitor': operation.competitor,
                'pair_id': pair_id,
                'blocks': blocks,
            }
        expected_plan = {
            'schema_version': 1,
            'artifact_type': 'gometry-benchmark-sampling-plan',
            'policy': {
                'looks': list(policy.looks),
                'error_probabilities': list(policy.error_probabilities),
                'precision_ratio': policy.precision_ratio,
                'confidence': policy.confidence,
            },
            'initial_look': policy.looks[0],
            'maximum_look': policy.looks[-1],
            'operations': expected_plan_operations,
        }
        if plan != expected_plan:
            raise SystemExit('run manifest pre-measurement sampling plan changed')
    expected = expected_paired_matrix(payload)
    actual: dict[tuple[str, str, str, str, str], Path] = {}
    nonpaired: set[str] = set()
    for path, artifact in artifacts.items():
        if artifact.get('artifact_type') != 'gometry-paired-block':
            nonpaired.add(path.name)
            continue
        operation = artifact.get('operation')
        order = artifact.get('lead_order')
        if not isinstance(operation, dict) or order not in (['A', 'B'], ['B', 'A']):
            raise SystemExit(f'{path} has incomplete paired matrix identity')
        key = (
            operation.get('gometry'),
            operation.get('competitor'),
            artifact.get('pair_id'),
            artifact.get('block_id'),
            'ab' if order == ['A', 'B'] else 'ba',
        )
        if not all(isinstance(value, str) and value for value in key):
            raise SystemExit(f'{path} has malformed paired matrix identity')
        typed_key = cast('tuple[str, str, str, str, str]', key)
        if typed_key in actual:
            raise SystemExit(f'duplicate paired matrix identity: {typed_key!r}')
        expected_entry = expected.get(typed_key)
        if expected_entry is None:
            raise SystemExit(
                f'{path} is extra or swapped in the paired matrix: {typed_key!r}'
            )
        declared = expected_entry['operation']
        if (
            tuple(operation.get('canonical_symbols') or ())
            != declared.canonical_symbols
        ):
            raise SystemExit(f'{path} canonical operation identity mismatch')
        actual[typed_key] = path
    if set(actual) != set(expected):
        missing = sorted(set(expected) - set(actual))
        raise SystemExit(
            'exact paired matrix is incomplete; missing identities: '
            + ', '.join(repr(value) for value in missing)
        )
    expected_nonpaired: set[str] = set()
    if isinstance(sampling, dict) and sampling.get('mode') == 'adaptive':
        expected_nonpaired.add(Path(str(payload['sampling_plan_artifact'])).name)
    oracle_artifact = payload.get('oracle_artifact')
    if isinstance(oracle_artifact, str) and oracle_artifact:
        expected_nonpaired.add(Path(oracle_artifact).name)
    planned = payload.get('planned_commands')
    if isinstance(planned, list):
        expected_nonpaired.update(
            Path(str(command['output'])).name
            for command in planned
            if isinstance(command, dict)
            and command.get('kind') in {'oracle', 'resource'}
            and isinstance(command.get('output'), str)
        )
    if nonpaired != expected_nonpaired:
        raise SystemExit(
            'run contains unexpected or missing non-paired artifacts: '
            f'actual={sorted(nonpaired)!r} expected={sorted(expected_nonpaired)!r}'
        )


def _validate_scaling_metadata(operations: list[PublicOpMeta]) -> None:
    curves: dict[str, list[PublicOpMeta]] = {}
    for operation in operations:
        if operation.scaling is not None:
            curves.setdefault(str(operation.scaling['curve_id']), []).append(operation)
    for curve_id, rows in curves.items():
        if len(rows) != 3:
            raise SystemExit(f'scaling curve {curve_id} must have exactly three points')
        dimensions = {
            (row.scaling['dimension'], row.scaling['unit'])
            for row in rows
            if row.scaling
        }
        if len(dimensions) != 1:
            raise SystemExit(f'scaling curve {curve_id} mixes dimensions or units')
        useful_work_units = {
            row.scaling['useful_work_unit'] for row in rows if row.scaling
        }
        if len(useful_work_units) != 1:
            raise SystemExit(f'scaling curve {curve_id} mixes useful-work units')
        points = [row.scaling['point'] for row in rows if row.scaling]
        if len(set(points)) != 3:
            raise SystemExit(f'scaling curve {curve_id} points must be unique')


def _validate_block(
    payload: dict[str, Any], path: Path
) -> tuple[str, str, PairedBlock]:
    if (
        payload.get('schema_version') != 1
        or payload.get('artifact_type') != 'gometry-paired-block'
    ):
        raise SystemExit(f'{path} is not a typed paired block artifact')
    block_id = payload.get('block_id')
    pair_id = payload.get('pair_id')
    operation = payload.get('operation')
    order = payload.get('lead_order')
    arms = payload.get('arms')
    timestamps = payload.get('timestamps_monotonic_ns')
    process = payload.get('process')
    if (
        not isinstance(block_id, str)
        or not block_id
        or not isinstance(pair_id, str)
        or not pair_id
        or not isinstance(operation, dict)
        or not isinstance(order, list)
        or order not in (['A', 'B'], ['B', 'A'])
        or not isinstance(arms, dict)
        or set(arms) != {'A', 'B'}
        or not isinstance(timestamps, dict)
        or not isinstance(process, dict)
    ):
        raise SystemExit(f'{path} has incomplete paired block identity')
    process_pid = process.get('pid')
    invocation_id = process.get('invocation_id')
    if (
        not isinstance(process_pid, int)
        or isinstance(process_pid, bool)
        or process_pid <= 0
        or not isinstance(invocation_id, str)
        or not invocation_id
    ):
        raise SystemExit(f'{path} has no measured-process identity')
    block_started = timestamps.get('block_started')
    block_ended = timestamps.get('block_ended')
    if (
        not isinstance(block_started, int)
        or not isinstance(block_ended, int)
        or block_started > block_ended
    ):
        raise SystemExit(f'{path} has invalid monotonic block timestamps')
    summaries: dict[str, tuple[str, str, float]] = {}
    arm_times: dict[str, tuple[int, int]] = {}
    provenances: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    for side in ('A', 'B'):
        arm = arms[side]
        if not isinstance(arm, dict):
            raise SystemExit(f'{path} arm {side} is malformed')
        row = arm.get('row')
        started = arm.get('started_monotonic_ns')
        ended = arm.get('ended_monotonic_ns')
        before = arm.get('provenance_before')
        after = arm.get('provenance_after')
        observation = arm.get('observation')
        case_process = arm.get('case_process')
        if (
            not isinstance(row, str)
            or not isinstance(started, int)
            or not isinstance(ended, int)
            or not isinstance(before, dict)
            or not isinstance(after, dict)
            or not isinstance(observation, dict)
            or not isinstance(case_process, dict)
        ):
            raise SystemExit(f'{path} arm {side} lacks typed timing/provenance')
        if not block_started <= started <= ended <= block_ended:
            raise SystemExit(f'{path} arm {side} timestamps are outside the block')
        if before != after:
            raise SystemExit(f'{path} arm {side} producer identity changed')
        if observation.get('producer_identity') != before:
            raise SystemExit(
                f'{path} arm {side} observation producer identity mismatch'
            )
        if (
            case_process != observation.get('case_process')
            or case_process.get('pid') != process_pid
            or case_process.get('invocation_id') != invocation_id
            or case_process.get('row') != row
            or case_process.get('identity_sha256') != identity_sha256(before)
        ):
            raise SystemExit(
                f'{path} arm {side} process identity differs from its producer'
            )
        try:
            validate_observation(observation)
            metric, unit, median, _raw = measurement_summary(observation)
        except (TypeError, ValueError) as exc:
            raise SystemExit(f'{path} arm {side} observation rejected: {exc}') from exc
        summaries[side] = (metric, unit, median)
        arm_times[side] = (started, ended)
        provenances[side] = (before, after)
    if summaries['A'][:2] != summaries['B'][:2]:
        raise SystemExit(f'{path} arms have mismatched metric types')
    if (
        arms['A']['observation']['useful_work']
        != arms['B']['observation']['useful_work']
    ):
        raise SystemExit(f'{path} arms have mismatched useful work')
    sampling = payload.get('sampling')
    if not isinstance(sampling, dict):
        raise SystemExit(f'{path} is missing sampling metadata')
    phase = operation.get('phase')
    scaling = operation.get('scaling')
    if scaling is not None:
        required_scaling = {
            'curve_id',
            'dimension',
            'unit',
            'point',
            'point_label',
            'useful_work_unit',
        }
        if not isinstance(scaling, dict) or set(scaling) != required_scaling:
            raise SystemExit(f'{path} has incomplete scaling metadata')
        if (
            arms['A']['observation']['useful_work']['unit']
            != scaling['useful_work_unit']
        ):
            raise SystemExit(f'{path} has mismatched useful-work unit')
    if phase not in {'warm', 'steady', 'first-use'}:
        raise SystemExit(f'{path} has an invalid operation phase')
    if not isinstance(sampling.get('phase'), str) or sampling['phase'] != phase:
        raise SystemExit(f'{path} has mismatched phase metadata')
    if sampling.get('scaling') != scaling:
        raise SystemExit(f'{path} has mismatched scaling metadata')
    for side in ('A', 'B'):
        observed_phase = arms[side]['observation'].get('phase')
        if observed_phase != phase:
            raise SystemExit(f'{path} arm {side} has mismatched phase metadata')
        observation = arms[side]['observation']
        if observation['warmup']['iterations'] != sampling.get('warmup_iterations'):
            raise SystemExit(
                f'{path} arm {side} has mismatched sampling warmup_iterations'
            )
        if len(measurement_summary(observation)[3]) != sampling.get('repetitions'):
            raise SystemExit(f'{path} arm {side} has mismatched sampling repetitions')
        if sampling.get('calibration_strategy') == 'bounded-doubling' and (
            observation['calibration']['strategy'] != 'bounded-doubling'
            or 'batch' not in observation
        ):
            raise SystemExit(f'{path} arm {side} lacks calibrated batch evidence')
    if common_identity(provenances['A'][0]) != common_identity(provenances['B'][0]):
        raise SystemExit(f'{path} arms have mismatched common producer identities')
    for side in ('A', 'B'):
        if provenances[side][0].get('arm', {}).get('side') != side:
            raise SystemExit(f'{path} arm {side} producer attribution is swapped')
    gometry_row = arms['A']['row']
    competitor_row = arms['B']['row']
    if (
        operation.get('gometry') != gometry_row
        or operation.get('competitor') != competitor_row
    ):
        raise SystemExit(f'{path} operation and arm row identity disagree')
    first, second = order
    first_start, first_end = arm_times[first]
    second_start, second_end = arm_times[second]
    if (
        not block_started
        <= first_start
        <= first_end
        <= second_start
        <= second_end
        <= block_ended
    ):
        raise SystemExit(
            f'{path} lead order has overlapping or reversed monotonic timestamps'
        )
    expected_oracle_identity = f'public:{gometry_row}:oracle'
    oracle_a = arms['A']['observation']['oracle']
    oracle_b = arms['B']['observation']['oracle']
    if oracle_a != oracle_b or oracle_a.get('identity') != expected_oracle_identity:
        raise SystemExit(f'{path} arms have mismatched or unexpected oracle identity')
    metric, unit, gometry_median = summaries['A']
    _, _, competitor_median = summaries['B']
    if gometry_median <= 0.0 or competitor_median <= 0.0:
        raise SystemExit(f'{path} contains a non-positive estimate')
    return (
        gometry_row,
        competitor_row,
        PairedBlock(
            artifact=str(path),
            block_id=block_id,
            pair_id=pair_id,
            lead='ab' if order == ['A', 'B'] else 'ba',
            metric=metric,
            unit=unit,
            gometry_median=gometry_median,
            competitor_median=competitor_median,
            ratio=competitor_median / gometry_median,
            arm_identities={
                side: provenance[0] for side, provenance in provenances.items()
            },
            phase=phase,
            scaling=scaling,
            useful_work=arms['A']['observation']['useful_work'],
        ),
    )


def _load_all_blocks(artifacts: dict[Path, dict[str, Any]]) -> dict[str, BenchStats]:
    """Load one typed paired block per artifact; resource artifacts are ignored."""
    by_operation: dict[str, list[PairedBlock]] = {}
    for path, payload in artifacts.items():
        if payload.get('artifact_type') != 'gometry-paired-block':
            continue
        gometry_row, competitor_row, block = _validate_block(payload, path)
        for row in (gometry_row, competitor_row):
            if any(
                item.block_id == block.block_id for item in by_operation.get(row, ())
            ):
                raise SystemExit(
                    f'duplicate paired block identity for {row}: {block.block_id}'
                )
            by_operation.setdefault(row, []).append(block)
    stats: dict[str, BenchStats] = {}
    for row, blocks in by_operation.items():
        blocks = sorted(blocks, key=lambda item: item.block_id)
        if row.startswith('gometry.'):
            values = [block.gometry_median for block in blocks]
        else:
            values = [block.competitor_median for block in blocks]
        metric = blocks[0].metric
        unit = blocks[0].unit
        stats[row] = BenchStats(
            name=row,
            metric=metric,
            unit=unit,
            mean=statistics.fmean(values),
            median=statistics.median(values),
            stdev=statistics.stdev(values) if len(values) > 1 else 0.0,
            nrun=len(values),
            samples=len(values),
            blocks=tuple(blocks),
        )
    return stats


def paired_block_ratios(blocks: Sequence[PairedBlock]) -> list[float]:
    if not blocks:
        raise ValueError('paired block samples cannot be empty')
    return [block.ratio for block in blocks]


def normalize_scaling_values(
    values: Sequence[float], useful_work: Sequence[int | float]
) -> list[float]:
    """Return descriptive values per useful-work unit without classifying shape."""
    if len(values) != len(useful_work) or not values:
        raise ValueError('scaling normalization requires matching non-empty sequences')
    if any(value <= 0 for value in values) or any(work <= 0 for work in useful_work):
        raise ValueError('scaling values and useful work must be positive')
    return [value / work for value, work in zip(values, useful_work, strict=True)]


def classify_ratio(
    ratio_interval: tuple[float, float] | None,
    equivalence_margin: float = PRACTICAL_EQUIVALENCE_MARGIN,
) -> str:
    if ratio_interval is None:
        return 'inconclusive'
    low, high = ratio_interval
    if low > 1.0 + equivalence_margin:
        return 'faster'
    if high < 1.0 - equivalence_margin:
        return 'slower'
    if low >= 1.0 - equivalence_margin and high <= 1.0 + equivalence_margin:
        return 'similar'
    return 'inconclusive'


def _build_rows(
    operations: list[PublicOpMeta],
    stats: dict[str, BenchStats],
    *,
    equivalence_margin: float,
    seed: int = DEFAULT_BOOTSTRAP_SEED,
    expected_blocks: int | None = None,
    expected_leads: tuple[str, ...] | None = None,
    sampling: dict[str, Any] | None = None,
) -> list[OpRow]:
    rows: list[OpRow] = []
    harness_equivalence: dict[tuple[str, str], bool] = {}
    if isinstance(sampling, dict) and sampling.get('mode') not in {
        'adaptive',
        'diagnostic',
        'plan-only',
    }:
        if expected_blocks is None and isinstance(sampling.get('measured_blocks'), int):
            expected_blocks = sampling['measured_blocks']
        if expected_leads is None and isinstance(sampling.get('lead_orders'), list):
            expected_leads = tuple(sampling['lead_orders'])
    adaptive_policy: SamplingPolicy | None = None
    adaptive_plan: dict[str, Any] = {}
    adaptive_decisions: dict[str, Any] = {}
    if isinstance(sampling, dict) and sampling.get('mode') == 'adaptive':
        adaptive_policy = _sampling_policy(sampling)
        raw_plan = sampling.get('actual_plan')
        raw_decisions = sampling.get('decisions')
        if not isinstance(raw_plan, dict) or not isinstance(raw_decisions, dict):
            raise SystemExit('run manifest lacks adaptive decisions and actual plan')
        adaptive_plan = raw_plan
        adaptive_decisions = raw_decisions
    missing: list[str] = []
    for meta in operations:
        gometry = stats.get(meta.gometry)
        competitor = stats.get(meta.competitor)
        if gometry is None:
            missing.append(meta.gometry)
        if competitor is None:
            missing.append(meta.competitor)
        if gometry is None or competitor is None:
            continue
        if gometry.metric != competitor.metric or gometry.unit != competitor.unit:
            raise SystemExit(f'metric type mismatch for {meta.gometry}')
        if gometry.metric != 'duration' or gometry.unit != 'seconds':
            raise SystemExit(
                f'public comparative rows require duration/seconds; got '
                f'{gometry.metric}/{gometry.unit} for {meta.gometry}'
            )
        blocks = gometry.blocks
        if competitor.blocks != blocks:
            raise SystemExit(f'paired block identity mismatch for {meta.gometry}')
        if any(block.phase != meta.phase for block in blocks):
            raise SystemExit(f'{meta.gometry} has mismatched phase across blocks')
        if any(block.scaling != meta.scaling for block in blocks):
            raise SystemExit(
                f'{meta.gometry} has mismatched scaling point across blocks'
            )
        if meta.scaling is not None:
            block_work = [block.useful_work for block in blocks]
            if not block_work or any(not isinstance(work, dict) for work in block_work):
                raise SystemExit(f'{meta.gometry} lacks useful-work evidence')
            if len({json.dumps(work, sort_keys=True) for work in block_work}) != 1:
                raise SystemExit(
                    f'{meta.gometry} has inconsistent useful-work evidence'
                )
            if block_work[0].get('unit') != meta.scaling['useful_work_unit']:
                raise SystemExit(f'{meta.gometry} has mismatched useful-work unit')
        if sampling is not None and sampling.get('mode') == 'diagnostic':
            measured_blocks = sampling.get('measured_blocks')
            if (
                not isinstance(measured_blocks, int)
                or isinstance(measured_blocks, bool)
                or len(blocks) != measured_blocks
            ):
                raise SystemExit(
                    f'{meta.gometry} has an incomplete diagnostic block set'
                )
        elif expected_blocks is not None and len(blocks) != expected_blocks:
            raise SystemExit(
                f'{meta.gometry} requires exactly {expected_blocks} paired blocks; got {len(blocks)}'
            )
        if expected_leads is not None:
            actual = tuple(sorted(block.lead for block in blocks))
            expected = tuple(sorted(expected_leads))
            if actual != expected:
                raise SystemExit(
                    f'{meta.gometry} has incomplete lead-order matrix: {actual!r}'
                )
        if len({block.pair_id for block in blocks}) != 1:
            raise SystemExit(
                f'{meta.gometry} has inconsistent pair identity across blocks'
            )
        reference_commit = blocks[0].arm_identities['A']['harness_commit']
        identities = []
        for block in blocks:
            comparable = {}
            for side, identity in block.arm_identities.items():
                commit = identity['harness_commit']
                if commit != reference_commit:
                    key = (commit, reference_commit)
                    if key not in harness_equivalence:
                        harness_equivalence[key] = harness_commits_match(*key)
                    if not harness_equivalence[key]:
                        raise SystemExit(
                            f'{meta.gometry} has inconsistent harness inputs across blocks'
                        )
                comparable[side] = {
                    key: value
                    for key, value in identity.items()
                    if key != 'harness_commit'
                }
            identities.append(json.dumps(comparable, sort_keys=True))
        if len(set(identities)) != 1:
            raise SystemExit(
                f'{meta.gometry} has inconsistent producer identities across blocks'
            )
        ratios = paired_block_ratios(blocks)
        if adaptive_policy is not None:
            plan = adaptive_plan.get(meta.gometry)
            record = adaptive_decisions.get(meta.gometry)
            block_ids = [block.block_id for block in blocks]
            leads = [block.lead for block in blocks]
            if (
                not isinstance(plan, dict)
                or not isinstance(record, dict)
                or plan.get('block_ids') != block_ids
                or plan.get('lead_orders') != leads
                or record.get('block_ids') != block_ids
                or record.get('lead_orders') != leads
                or len(block_ids) not in adaptive_policy.looks
            ):
                raise SystemExit(
                    f'{meta.gometry} actual blocks differ from its adaptive plan'
                )
            decision = assess_ratios(
                ratios,
                block_ids=block_ids,
                policy=adaptive_policy,
            )
            inference = record.get('inference')
            interval = decision.uncertainty.interval
            expected_interval = list(interval) if interval is not None else None
            if (
                not isinstance(inference, dict)
                or inference.get('estimate') != decision.estimate
                or inference.get('interval') != expected_interval
                or inference.get('achieved_coverage')
                != decision.uncertainty.achieved_coverage
                or inference.get('sample_size') != decision.uncertainty.sample_size
                or inference.get('lower_order') != decision.uncertainty.lower_order
                or inference.get('upper_order') != decision.uncertainty.upper_order
                or inference.get('confidence') != decision.confidence
                or inference.get('error_probability') != decision.error_probability
                or inference.get('direction') != decision.direction
                or inference.get('reason') != decision.reason
                or inference.get('next_look') != decision.next_look
            ):
                raise SystemExit(
                    f'{meta.gometry} recorded inference disagrees with paired ratios'
                )
            collection_status = record.get('collection_status')
            if collection_status == 'budget-limited':
                if (
                    len(block_ids) != adaptive_policy.looks[0]
                    or decision.next_look is None
                ):
                    raise SystemExit(
                        f'{meta.gometry} has an invalid budget-limited sampling record'
                    )
            elif collection_status == 'complete':
                if decision.next_look is not None:
                    raise SystemExit(
                        f'{meta.gometry} stopped before its recorded look was complete'
                    )
            else:
                raise SystemExit(
                    f'{meta.gometry} has an incomplete adaptive collection'
                )
            ratio_interval = interval
        elif sampling is not None and sampling.get('mode') == 'diagnostic':
            ratio_interval = None
        else:
            # Records published before adaptive sampling retain their original
            # fixed-six bootstrap analysis.
            ratio_interval = median_interval(ratios, seed=seed)
        rows.append(
            OpRow(
                meta=meta,
                gometry=gometry,
                competitor=competitor,
                ratio=statistics.median(ratios),
                ratio_interval=ratio_interval,
                blocks=blocks,
                status=classify_ratio(ratio_interval, equivalence_margin),
            )
        )
    if missing:
        raise SystemExit(
            'run is missing declared public benchmark rows (or artifacts):\n'
            + '\n'.join(f'  - {name}' for name in missing)
        )
    declared = {op.gometry for op in operations} | {op.competitor for op in operations}
    extras = sorted(name for name in stats if name not in declared)
    if extras:
        raise SystemExit(
            'run contains undeclared public benchmark rows:\n'
            + '\n'.join(f'  - {name}' for name in extras)
        )
    return rows


def _humanize_seconds(value: float) -> str:
    if value >= 0.001:
        return f'{value * 1000.0:.2f} ms'
    if value >= 1e-6:
        return f'{value * 1e6:.2f} us'
    return f'{value * 1e9:.1f} ns'


def format_speedup(row: OpRow) -> str:
    if row.status == 'similar':
        label = 'Similar'
    elif row.status == 'inconclusive':
        label = 'Inconclusive'
    elif row.ratio >= 1.0:
        label = f'{row.ratio:.2f}× faster'  # ruff: ignore[ambiguous-unicode-character-string]
    else:
        label = f'{1.0 / row.ratio:.2f}× slower'  # ruff: ignore[ambiguous-unicode-character-string]
    return f'<span class="benchmark-status">{label}</span>'


def _section_table(
    rows: list[OpRow],
    normalized: list[tuple[float, float]] | None = None,
) -> list[str]:
    lines = [
        '<table class="benchmark-table">',
        '<thead><tr><th scope="col">API / input</th>'
        '<th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>',
        '<tbody>',
    ]
    for index, row in enumerate(rows):
        label = escape(row.meta.label)
        workload = escape(row.meta.workload)
        gometry_work = competitor_work = ''
        if normalized is not None:
            scaling = row.meta.scaling
            assert scaling is not None
            useful = row.blocks[0].useful_work
            label = escape(f'{scaling["point_label"]} {scaling["unit"]}')
            workload = escape(f'{useful["count"]} {useful["unit"]}')
            unit = {
                'predicate-results': 'query',
                'candidate-pairs': 'candidate pair',
                'output-tiles': 'tile',
                'input-geometries': 'input geometry',
                'holes': 'hole',
            }.get(useful['unit'], useful['unit'])
            gometry_work, competitor_work = (
                f'<small class="benchmark-detail">{_humanize_seconds(value)} / {escape(unit)}</small> '
                for value in normalized[index]
            )
        markers = ''.join(
            marker
            for name, marker in (('geodesic', '†'), ('in_core', '‡'))
            if name in row.meta.footnotes
        )
        lines.append(
            f'<tr class="benchmark-{row.status}">'
            f'<th scope="row"><span class="benchmark-api">{label}</span> '
            f'<small class="benchmark-detail">{workload} {markers}</small></th>'
            '<td><span class="benchmark-mobile-label">gometry</span> '
            f'<span class="benchmark-time">{_humanize_seconds(row.gometry.median)}</span> '
            f'{gometry_work}{format_speedup(row)}</td>'
            '<td>'
            f'<span class="benchmark-time">{_humanize_seconds(row.competitor.median)}</span> '
            f'{competitor_work}'
            f'<span class="benchmark-runner">{escape(row.meta.competitor_label)}</span> '
            '</td></tr>'
        )
    lines.extend(('</tbody>', '</table>'))
    return lines


def _scaling_sections(rows: list[OpRow]) -> list[str]:
    by_curve: dict[str, list[OpRow]] = {}
    for row in rows:
        if row.meta.scaling is not None:
            by_curve.setdefault(str(row.meta.scaling['curve_id']), []).append(row)
    lines: list[str] = []
    for curve_rows in by_curve.values():
        ordered = sorted(curve_rows, key=lambda row: row.meta.scaling['point'])
        scaling = ordered[0].meta.scaling
        if scaling is None:
            raise SystemExit('scaling curve has no metadata')
        work: list[dict[str, Any]] = []
        for row in ordered:
            value = row.blocks[0].useful_work if row.blocks else None
            if not isinstance(value, dict) or not isinstance(value.get('count'), int):
                raise SystemExit(f'{row.meta.gometry} lacks useful-work evidence')
            work.append(value)
        gometry_normalized = normalize_scaling_values(
            [row.gometry.median for row in ordered],
            [int(value['count']) for value in work],
        )
        competitor_normalized = normalize_scaling_values(
            [row.competitor.median for row in ordered],
            [int(value['count']) for value in work],
        )
        lines.extend((
            f'#### {ordered[0].meta.label}',
            '',
            f'Dimension `{scaling["dimension"]}` in `{scaling["unit"]}`; '
            'each point is an independent paired workload.',
            '',
        ))
        lines.extend(
            _section_table(
                ordered,
                list(zip(gometry_normalized, competitor_normalized, strict=True)),
            )
        )
        lines.extend((
            '',
            f'Endpoint growth ({ordered[0].meta.scaling["point_label"]} → '
            f'{ordered[-1].meta.scaling["point_label"]}): '
            f'gometry median {ordered[-1].gometry.median / ordered[0].gometry.median:.3f}x, '
            f'comparator median {ordered[-1].competitor.median / ordered[0].competitor.median:.3f}x, '
            f'gometry/work {gometry_normalized[-1] / gometry_normalized[0]:.3f}x, '
            f'comparator/work {competitor_normalized[-1] / competitor_normalized[0]:.3f}x.',
        ))
        lines.append('')
    return lines


def _run_environment(payload: dict[str, Any]) -> dict[str, Any]:
    doctor = payload.get('doctor')
    metadata = doctor.get('metadata') if isinstance(doctor, dict) else {}
    if not isinstance(metadata, dict):
        metadata = {}
    packages = metadata.get('packages')
    versions = {
        info.get('display_name') or name: info['version']
        for name, info in (packages.items() if isinstance(packages, dict) else ())
        if isinstance(info, dict) and info.get('version') and name != 'gometry'
    }
    python = metadata.get('python_version', metadata.get('python'))
    return {
        'cpu_model': metadata.get('cpu_model'),
        'kernel': metadata.get('kernel'),
        'python': python.split()[0] if isinstance(python, str) else None,
        'harness_commit': metadata.get('harness_commit'),
        'geos_version': metadata.get('geos_version'),
        'proj_version': metadata.get('proj_version'),
        'package_versions': versions,
    }


def render(
    rows: list[OpRow],
    *,
    publishable: bool,
    environment: dict[str, Any] | None = None,
    sampling: dict[str, Any] | None = None,
) -> list[str]:
    lines: list[str] = []
    if not publishable:
        lines.extend((f'> **{SMOKE_BANNER}**', ''))
    if environment is not None and any(environment.values()):
        lines.extend(('<!-- --8<-- [start:environment] -->', ''))
        commit = environment.get('harness_commit') or 'unknown'
        lines.extend((
            '## Run environment',
            '',
            f'Measured on {environment.get("cpu_model") or "an unknown CPU"} with Linux '
            f'{environment.get("kernel") or "unknown"} and CPython '
            f'{environment.get("python") or "unknown"}. Benchmark harness commit: '
            f'`{commit[:8]}`.',
        ))
        producer_commits: set[str] = set()
        producer_unknown = False
        for row in rows:
            for block in row.blocks:
                for identity in block.arm_identities.values():
                    implementation = identity.get('implementation')
                    if (
                        not isinstance(implementation, dict)
                        or implementation.get('runtime') != 'gometry'
                    ):
                        continue
                    source_commit = implementation.get('source_commit')
                    if isinstance(source_commit, str):
                        producer_commits.add(source_commit)
                    else:
                        producer_unknown = True
        if len(producer_commits) == 1 and not producer_unknown:
            source_commit = next(iter(producer_commits))
            lines.append(
                f'Gometry producer source commit: `{source_commit[:8]}` '
                '(bound to the loaded wheel by its receipt).'
            )
        elif producer_commits:
            sources = ', '.join(
                f'`{source_commit[:8]}`' for source_commit in sorted(producer_commits)
            )
            suffix = (
                ' and unknown (producer source is not commit-bound)'
                if producer_unknown
                else ''
            )
            lines.append(f'Gometry producer source commits: {sources}{suffix}.')
        else:
            lines.append(
                'Gometry producer source commit: unknown '
                '(producer source is not commit-bound).'
            )
        lines.append('')
        versions = [
            f'{name} {version}'
            for name, version in sorted(
                (environment.get('package_versions') or {}).items(),
                key=lambda item: item[0].lower(),
            )
        ]
        versions += [
            f'{engine} {environment[key]}'
            for engine, key in (('GEOS', 'geos_version'), ('PROJ', 'proj_version'))
            if environment.get(key)
        ]
        if versions:
            lines.append(f'Versions: {", ".join(versions)}.')
        lines.append('')
        lines.extend(('<!-- --8<-- [end:environment] -->', ''))
    if sampling is not None and sampling.get('mode') == 'adaptive':
        policy = _sampling_policy(sampling)
        lines.extend((
            'Ratios are comparator duration divided by gometry duration. '
            f'Each comparison uses exact median order intervals with at least '
            f'{policy.confidence:.0%} coverage across its permitted looks. '
            f'“Similar” requires the entire interval within the '
            f'{PRACTICAL_EQUIVALENCE_MARGIN:.0%} practical-equivalence margin; '
            'a narrow interval crossing 1 is not by itself evidence of equality.',
            '',
        ))
    elif sampling is not None and sampling.get('mode') == 'diagnostic':
        lines.extend((
            'One-block smoke measurements are diagnostic only and do not carry '
            'a confidence interval.',
            '',
        ))
    elif sampling is not None and 'measured_blocks' in sampling:
        lines.extend((
            'Historical fixed-six measurements use a 95% paired bootstrap '
            'interval and the recorded 3% practical-equivalence margin.',
            '',
        ))
    lines.extend(('<!-- --8<-- [start:comparisons] -->', ''))
    by_domain: dict[str, list[OpRow]] = {}
    for row in rows:
        by_domain.setdefault(row.meta.domain, []).append(row)
    for domain in DOMAIN_ORDER:
        domain_rows = [
            row
            for row in by_domain.get(domain, ())
            if row.meta.scaling is None and 'reference' not in row.meta.footnotes
        ]
        if not domain_rows:
            continue
        lines.extend((
            f'### {domain}',
            '',
            *_section_table(domain_rows),
            '',
        ))
    if any('geodesic' in row.meta.footnotes for row in rows):
        lines.append('† geodesic/ellipsoidal metric.')
    if any('in_core' in row.meta.footnotes for row in rows):
        lines.append(
            '‡ gometry uses its admitted in-core projection path; this is not a claim about arbitrary CRS transforms.'
        )
    lines.extend(('', '<!-- --8<-- [end:comparisons] -->'))
    reference_rows = [row for row in rows if 'reference' in row.meta.footnotes]
    lines.extend(('', '<!-- --8<-- [start:references] -->', ''))
    if reference_rows:
        lines.extend(_section_table(reference_rows))
    lines.extend(('', '<!-- --8<-- [end:references] -->'))
    scaling_rows = [row for row in rows if row.meta.scaling is not None]
    if scaling_rows:
        lines.extend((
            '',
            '<!-- --8<-- [start:scaling] -->',
            '',
            '### Scaling Curves',
            '',
            *_scaling_sections(scaling_rows),
            '<!-- --8<-- [end:scaling] -->',
        ))
    while lines and not lines[-1]:
        lines.pop()
    return lines


def summarize_payload(
    payload: dict[str, Any],
    stats: dict[str, BenchStats],
    *,
    equivalence_margin: float = PRACTICAL_EQUIVALENCE_MARGIN,
) -> list[str]:
    operations = _parse_public_operations(payload.get('public_operations'))
    rows = _build_rows(
        operations,
        stats,
        equivalence_margin=equivalence_margin,
        sampling=payload.get('sampling')
        if isinstance(payload.get('sampling'), dict)
        else None,
    )
    return render(
        rows,
        publishable=bool(payload.get('publishable')),
        environment=_run_environment(payload),
        sampling=payload.get('sampling')
        if isinstance(payload.get('sampling'), dict)
        else None,
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        'results', help='one bench.py run manifest JSON or its directory'
    )
    parser.add_argument(
        '--equivalence-margin',
        type=float,
        default=PRACTICAL_EQUIVALENCE_MARGIN,
        help='predeclared practical equivalence margin as a ratio',
    )
    args = parser.parse_args(argv)
    payload, artifacts = _resolve_run(args.results)
    stats = _load_all_blocks(artifacts)
    if payload.get('plan_only'):
        sampling = payload.get('sampling')
        plan = sampling.get('maximum_plan') if isinstance(sampling, dict) else None
        if not isinstance(plan, dict):
            raise SystemExit('plan-only run lacks its maximum allowed sampling plan')
        block_counts = {
            operation: len(value.get('block_ids', ()))
            for operation, value in plan.items()
            if isinstance(value, dict)
        }
        print(
            f'Plan only: {len(block_counts)} operations; maximum '
            f'{max(block_counts.values(), default=0)} paired process blocks per '
            'operation; no measurements collected.'
        )
        return
    validate_exact_paired_matrix(payload, artifacts)
    if not stats:
        raise SystemExit('no typed paired benchmark block artifacts found in run')
    print(
        '\n'.join(
            summarize_payload(
                payload,
                stats,
                equivalence_margin=args.equivalence_margin,
            )
        )
    )


if __name__ == '__main__':
    main()
