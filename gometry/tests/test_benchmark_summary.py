"""Focused tests for paired process evidence and compact rendering."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_BENCHES = Path(__file__).resolve().parents[1] / 'benches'
_SUPPORT = _BENCHES / 'support'
if str(_SUPPORT) not in sys.path:
    sys.path.insert(0, str(_SUPPORT))

import summarize_bench as sb  # noqa: E402


def _stats(name: str, median: float) -> sb.BenchStats:
    return sb.BenchStats(
        name=name,
        mean=median,
        median=median,
        stdev=0.0,
        nrun=1,
        samples=1,
        processes=(
            sb.ProcessSample('unit.json', 0, (median,), median, 'unit', 'ab'),
        ),
    )


def _meta(
    *,
    domain: str = 'Geometry',
    label: str = 'op',
    workload: str = '10k inputs',
    gometry: str = 'gometry.op/10k',
    competitor: str = 'shapely.op/10k',
    competitor_label: str = 'Shapely',
    footnotes: tuple[str, ...] = (),
) -> sb.PublicOpMeta:
    return sb.PublicOpMeta(
        domain=domain,
        label=label,
        workload=workload,
        suite='competitors',
        gometry=gometry,
        competitor=competitor,
        competitor_label=competitor_label,
        footnotes=footnotes,
    )


def test_process_values_do_not_inflate_independent_n() -> None:
    ratios = sb.paired_process_ratios(
        [[1.0, 1.0, 1.0, 1.0], [2.0, 2.0]],
        [[2.0, 2.0, 2.0, 2.0], [4.0, 4.0]],
    )
    assert ratios == [2.0, 2.0]
    assert sb.bootstrap_paired_ratio_ci(ratios, seed=123, samples=200) == (2.0, 2.0)


def test_common_process_drift_cancels_in_paired_ratios() -> None:
    assert sb.paired_process_ratios(
        [[1.0, 1.1], [10.0, 10.5], [2.0, 1.9]],
        [[2.0, 2.2], [20.0, 21.0], [4.0, 3.8]],
    ) == [2.0, 2.0, 2.0]


def _process_stats(
    name: str,
    *,
    artifact: str = 'pair.json',
    pair_id: str | None = 'pair-1',
    lead: str | None = 'ab',
    count: int = 2,
) -> sb.BenchStats:
    processes = tuple(
        sb.ProcessSample(
            artifact,
            index,
            (1.0, 1.0),
            1.0,
            pair_id,
            lead,
        )
        for index in range(count)
    )
    return sb.BenchStats(
        name=name,
        mean=1.0,
        median=1.0,
        stdev=0.0,
        nrun=count,
        samples=count * 2,
        processes=processes,
    )


@pytest.mark.parametrize(
    'mutation,match',
    [
        (lambda _left, right: _process_stats(right.name, count=1), 'identity mismatch'),
        (lambda _left, right: _process_stats(right.name, pair_id=None), 'missing pair_id'),
        (lambda _left, right: _process_stats(right.name, lead=None), 'missing pair lead'),
        (lambda _left, right: _process_stats(right.name, pair_id='other'), 'pair_id mismatch'),
        (lambda _left, right: _process_stats(right.name, lead='ba'), 'pair lead mismatch'),
    ],
)
def test_artifact_process_pairing_rejects_metadata_and_key_mutations(
    mutation, match: str
) -> None:
    left = _process_stats('gometry.op/10k')
    right = mutation(left, _process_stats('shapely.op/10k'))
    with pytest.raises(SystemExit, match=match):
        sb._paired_blocks(left, right)


def test_public_rows_reject_synthetic_fallback_without_raw_processes() -> None:
    meta = _meta()
    left = sb.BenchStats(meta.gometry, 1.0, 1.0, 0.0, 1, 1)
    right = sb.BenchStats(meta.competitor, 2.0, 2.0, 0.0, 1, 1)
    with pytest.raises(SystemExit, match='synthetic blocks are not publishable'):
        sb._build_rows([meta], {meta.gometry: left, meta.competitor: right}, noise_band=0.03)


@pytest.mark.parametrize(
    'interval,expected',
    [
        ((1.10, 1.20), 'faster'),
        ((0.80, 0.90), 'slower'),
        ((0.98, 1.02), 'similar'),
        ((0.98, 1.05), 'inconclusive'),
    ],
)
def test_classification_uses_entire_interval(interval: tuple[float, float], expected: str) -> None:
    assert sb.classify_ratio(interval) == expected


def test_render_has_compact_accessible_status_and_no_aggregate_claims() -> None:
    meta = _meta(
        label='Prepared contains XY',
        workload='100k probes',
        footnotes=('in_core',),
    )
    row = sb.OpRow(
        meta=meta,
        gometry=_stats(meta.gometry, 0.001),
        competitor=_stats(meta.competitor, 0.002),
        ratio=2.0,
        ratio_ci95=(2.0, 2.0),
        blocks=(),
        status='faster',
    )
    text = '\n'.join(sb.render(
        [row],
        markdown=True,
        publishable=True,
        environment={
            'cpu': 7,
            'cpu_source': 'kernel-isolated',
            'cpu_model': 'AMD Ryzen 9 5950X',
            'kernel': '6.1',
            'python': '3.14.7',
            'gometry_version': '0.1',
            'git_commit': 'deadbeef',
            'geos_version': '3.13.0',
            'proj_version': '9.5.0',
            'package_versions': {'numpy': '2.0', 'shapely': '2.1'},
        },
    ))
    from markdown import markdown

    html = markdown(text, extensions=['tables', 'md_in_html'])
    assert '| API / input | gometry | Comparator | Relative |' in text
    assert 'Shapely · 2.00 ms' in text
    assert '2.00× faster' in text  # noqa: RUF001 — product glyph
    assert 'benchmark-status benchmark-faster' in text
    assert 'benchmark-table-scroll' in text
    assert 'markdown="1"' in text
    assert '<table>' in html
    assert '| API / input |' not in html
    assert 'CPU `AMD Ryzen 9 5950X` on core `7`' in text
    assert 'numpy 2.0, shapely 2.1' in text
    assert 'GEOS 3.13.0; PROJ 9.5.0' in text
    assert '95%' not in text
    assert 'Geomean' not in text
    assert 'Overall' not in text
    assert '‡' in text


def test_summary_retains_process_identity_blocks() -> None:
    meta = _meta()
    gometry = sb.BenchStats(
        name=meta.gometry,
        mean=1.1,
        median=1.1,
        stdev=0.1,
        nrun=2,
        samples=4,
        processes=(
            sb.ProcessSample('pair.json', 0, (1.0, 1.2), 1.1, 'pair-1', 'ab'),
            sb.ProcessSample('pair.json', 1, (1.0, 1.2), 1.1, 'pair-1', 'ab'),
        ),
    )
    competitor = sb.BenchStats(
        name=meta.competitor,
        mean=2.2,
        median=2.2,
        stdev=0.1,
        nrun=2,
        samples=4,
        processes=(
            sb.ProcessSample('pair.json', 0, (2.0, 2.4), 2.2, 'pair-1', 'ab'),
            sb.ProcessSample('pair.json', 1, (2.0, 2.4), 2.2, 'pair-1', 'ab'),
        ),
    )
    payload = {
        'profile': 'release',
        'publishable': True,
        'timestamp': '20260824T120000Z',
        'sampling_args': ['--processes', '6'],
        'paired_sampling_args': ['--processes', '3'],
        'row_timeout_seconds': 60,
        'command_timeout_seconds': 900,
        'total_timeout_seconds': 2400,
        'public_operations': [{
            'domain': meta.domain,
            'label': meta.label,
            'workload': meta.workload,
            'suite': meta.suite,
            'gometry': meta.gometry,
            'competitor': meta.competitor,
            'competitor_label': meta.competitor_label,
            'footnotes': [],
        }],
    }
    normalized = sb.normalized_summary_payload(
        payload, {meta.gometry: gometry, meta.competitor: competitor}
    )
    operation = normalized['operations'][0]
    assert operation['status'] == 'faster'
    assert operation['ratio_ci95'] == [2.0, 2.0]
    assert len(operation['blocks']) == 2
    assert operation['blocks'][0]['process_index'] == 0
    assert operation['blocks'][0]['pair_id'] == 'pair-1'
    assert normalized['run']['timestamp'] == '20260824T120000Z'
    assert normalized['sampling']['paired_sampling_args'] == ['--processes', '3']
    assert normalized['methodology']['bootstrap_samples'] == sb.BOOTSTRAP_SAMPLES
    assert 'geomean' not in normalized
    assert 'winner' not in json_keys(normalized)


def json_keys(value: object) -> set[str]:
    return set(value) if isinstance(value, dict) else set()


def test_missing_public_operations_fails() -> None:
    with pytest.raises(SystemExit, match='public_operations'):
        sb.summarize_payload({'profile': 'smoke', 'publishable': False}, {})


def test_undeclared_public_gometry_row_fails_manifest_gate() -> None:
    meta = _meta()
    stats = {
        meta.gometry: _stats(meta.gometry, 1.0),
        meta.competitor: _stats(meta.competitor, 2.0),
        'gometry.undeclared/1': _stats('gometry.undeclared/1', 3.0),
    }
    with pytest.raises(SystemExit, match='undeclared public benchmark'):
        sb._build_rows([meta], stats, noise_band=0.03)
