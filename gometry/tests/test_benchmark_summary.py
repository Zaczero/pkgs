"""Focused validation of self-contained paired block artifacts."""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

import pytest

_BENCHES = Path(__file__).resolve().parents[1] / 'benches'
for _path in (_BENCHES / 'support',):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

import benchmark_observation as bo  # noqa: E402
import summarize_bench as sb  # noqa: E402


def _observation(metric: str, value: float) -> dict:
    return bo.make_observation(
        metric=metric,
        raw_values=[value],
        useful_work=(
            {
                'numerator': {'count': 320, 'unit': 'rows'},
                'denominator': {'count': 80, 'unit': 'rows'},
            }
            if metric == 'scaling_ratio'
            else {'count': 10, 'unit': 'calls'}
        ),
        oracle_identity='public:gometry.op/10k:oracle-v1',
        warmup_iterations=1,
        calibration_iterations=1,
    )


def _block(
    *,
    path: str = 'block-01.json',
    block_id: str = 'pair-1-block-01-ab',
    lead: str = 'ab',
    metric: str = 'duration',
    gometry_value: float = 1.0,
    competitor_value: float = 2.0,
    identity: str = 'same',
) -> dict:
    order = ['A', 'B'] if lead == 'ab' else ['B', 'A']
    return {
        'schema_version': 2,
        'artifact_type': 'gometry-paired-block',
        'block_id': block_id,
        'pair_id': 'pair-1',
        'operation': {
            'gometry': 'gometry.op/10k',
            'competitor': 'shapely.op/10k',
            'canonical_symbols': ['area'],
        },
        'lead_order': order,
        'state_scope': 'shared-process-prepared-fixture',
        'timestamps_monotonic_ns': {'block_started': 1, 'block_ended': 20},
        'sampling': {
            'warmup_iterations': 1,
            'repetitions': 1,
            'calibration_strategy': 'fixed-repetitions',
        },
        'arms': {
            'A': {
                'row': 'gometry.op/10k',
                'started_monotonic_ns': 2 if lead == 'ab' else 12,
                'ended_monotonic_ns': 3 if lead == 'ab' else 13,
                'provenance_before': {'extension': identity},
                'provenance_after': {'extension': identity},
                'observation': _observation(metric, gometry_value),
            },
            'B': {
                'row': 'shapely.op/10k',
                'started_monotonic_ns': 12 if lead == 'ab' else 2,
                'ended_monotonic_ns': 13 if lead == 'ab' else 3,
                'provenance_before': {'extension': identity},
                'provenance_after': {'extension': identity},
                'observation': _observation(metric, competitor_value),
            },
        },
    }


def _meta() -> sb.PublicOpMeta:
    return sb.PublicOpMeta(
        domain='Geometry',
        label='op',
        workload='10k inputs',
        suite='competitors',
        canonical_symbols=('area',),
        gometry='gometry.op/10k',
        competitor='shapely.op/10k',
        competitor_label='Shapely',
        footnotes=(),
    )


def test_real_block_populates_both_arm_stats_without_ordinal_matching(tmp_path: Path) -> None:
    first = _block()
    second = _block(path='block-02.json', block_id='pair-1-block-02-ba', lead='ba')
    first_path = tmp_path / 'block-01.json'
    second_path = tmp_path / 'block-02.json'
    first_path.write_text(json.dumps(first), encoding='utf-8')
    second_path.write_text(json.dumps(second), encoding='utf-8')
    stats = sb._load_all_blocks([first_path, second_path])
    assert set(stats) == {'gometry.op/10k', 'shapely.op/10k'}
    assert stats['gometry.op/10k'].nrun == stats['shapely.op/10k'].nrun == 2
    assert stats['gometry.op/10k'].blocks == stats['shapely.op/10k'].blocks


def test_missing_lead_or_one_block_rejects_declared_release_matrix() -> None:
    meta = _meta()
    first = sb.PairedBlock(
        'a', 'block-a', 'pair-1', 'ab', 'duration', 'seconds', 1.0, 2.0, 2.0, {'id': 'same'}
    )
    stats = {
        name: sb.BenchStats(name, 'duration', 'seconds', value, value, 0.0, 1, 1, (first,))
        for name, value in ((meta.gometry, 1.0), (meta.competitor, 2.0))
    }
    with pytest.raises(SystemExit, match='exactly 6 paired blocks'):
        sb._build_rows([meta], stats, equivalence_margin=0.03, expected_blocks=6)
    with pytest.raises(SystemExit, match='incomplete lead-order matrix'):
        sb._build_rows(
            [meta], stats, equivalence_margin=0.03, expected_blocks=1, expected_leads=('ab', 'ba')
        )


def test_swapped_arm_identity_and_metric_type_fail_closed(tmp_path: Path) -> None:
    payload = _block()
    payload['arms']['B']['provenance_before'] = {'extension': 'other'}
    path = tmp_path / 'bad-identity.json'
    path.write_text(json.dumps(payload), encoding='utf-8')
    with pytest.raises(SystemExit, match='producer identity'):
        sb._load_all_blocks([path])

    payload = _block()
    payload['arms']['B']['observation'] = _observation('scaling_ratio', 2.0)
    path = tmp_path / 'bad-metric.json'
    path.write_text(json.dumps(payload), encoding='utf-8')
    with pytest.raises(SystemExit, match='mismatched metric types'):
        sb._load_all_blocks([path])


@pytest.mark.parametrize('mutation', ['reversed', 'overlap'])
def test_block_timestamps_must_match_lead_without_overlap(
    tmp_path: Path, mutation: str
) -> None:
    payload = _block()
    if mutation == 'reversed':
        payload['lead_order'] = ['B', 'A']
    else:
        payload['arms']['B']['started_monotonic_ns'] = 2
    path = tmp_path / f'{mutation}-timestamps.json'
    path.write_text(json.dumps(payload), encoding='utf-8')
    with pytest.raises(SystemExit, match='timestamps'):
        sb._load_all_blocks([path])


def test_paired_arms_require_the_same_operation_oracle_identity(tmp_path: Path) -> None:
    payload = _block()
    payload['arms']['B']['observation']['oracle']['identity'] = 'public:other:oracle-v1'
    path = tmp_path / 'bad-oracle-identity.json'
    path.write_text(json.dumps(payload), encoding='utf-8')
    with pytest.raises(SystemExit, match='oracle identity'):
        sb._load_all_blocks([path])


def test_typed_ratio_cannot_be_rendered_as_seconds() -> None:
    with pytest.raises(SystemExit, match='metric type mismatch'):
        # A duration-only row metadata path cannot consume scaling observations.
        sb._build_rows(
            [_meta()],
            {
                'gometry.op/10k': sb.BenchStats('gometry.op/10k', 'scaling_ratio', 'ratio', 1.0, 1.0, 0.0, 1, 1, ()),
                'shapely.op/10k': sb.BenchStats('shapely.op/10k', 'duration', 'seconds', 1.0, 1.0, 0.0, 1, 1, ()),
            },
            equivalence_margin=0.03,
        )


def test_block_artifact_is_immutable_in_identity_fields(tmp_path: Path) -> None:
    payload = _block()
    path = tmp_path / 'block.json'
    path.write_text(json.dumps(payload), encoding='utf-8')
    loaded = json.loads(path.read_text(encoding='utf-8'))
    mutated = copy.deepcopy(loaded)
    mutated['arms']['A']['row'] = 'shapely.other'
    path.write_text(json.dumps(mutated), encoding='utf-8')
    with pytest.raises(SystemExit, match='operation and arm row identity'):
        sb._load_all_blocks([path])
