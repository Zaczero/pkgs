"""Focused summarizer tests for the public competitive presentation (Lane 3)."""

from __future__ import annotations

import math
import sys
from pathlib import Path

import pytest

_BENCHES = Path(__file__).resolve().parents[1] / 'benches'
_SUPPORT = _BENCHES / 'support'
if str(_SUPPORT) not in sys.path:
    sys.path.insert(0, str(_SUPPORT))

import summarize_bench as sb  # noqa: E402
from _bench_registry import DOMAIN_ORDER, RELEASE_OPERATIONS  # noqa: E402


def _stats(
    name: str, mean: float, *, stdev: float = 0.0, samples: int = 30
) -> sb.BenchStats:
    return sb.BenchStats(
        name=name,
        mean=mean,
        median=mean,
        stdev=stdev,
        nrun=3,
        samples=samples,
    )


def _meta(
    *,
    domain: str = 'Geometry',
    label: str = 'op',
    workload: str = '10k / 1,316 vertices',
    gometry: str = 'gometry.op/10k',
    competitor: str | None = 'shapely.op/10k',
    competitor_label: str | None = 'Shapely',
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


def test_speedup_parity_and_as_fast() -> None:
    win = sb.OpRow(
        meta=_meta(gometry='gometry.a/1', competitor='shapely.a/1'),
        gometry=_stats('gometry.a/1', 1.0),
        competitor=_stats('shapely.a/1', 2.0),
        speedup=2.0,
        tie=False,
    )
    loss = sb.OpRow(
        meta=_meta(gometry='gometry.b/1', competitor='shapely.b/1'),
        gometry=_stats('gometry.b/1', 1.0),
        competitor=_stats('shapely.b/1', 0.82),
        speedup=0.82,
        tie=False,
    )
    parity = sb.OpRow(
        meta=_meta(gometry='gometry.c/1', competitor='shapely.c/1'),
        gometry=_stats('gometry.c/1', 1.0),
        competitor=_stats('shapely.c/1', 1.01),
        speedup=1.01,
        tie=True,
    )
    solo = sb.OpRow(
        meta=_meta(
            gometry='gometry.s2/1',
            competitor=None,
            competitor_label=None,
            domain='Discrete global grids',
        ),
        gometry=_stats('gometry.s2/1', 0.5),
        competitor=None,
        speedup=None,
        tie=False,
    )
    assert sb.format_speedup(win) == '2.00× faster'  # noqa: RUF001 — product glyph
    assert sb.format_speedup(loss) == '0.82× as fast'  # noqa: RUF001 — product glyph
    assert sb.format_speedup(parity) == '≈ parity'
    assert sb.format_speedup(solo) == '—'
    # Never emit the forbidden "x faster" form for a loss.
    assert 'faster' not in sb.format_speedup(loss)
    assert 'slower' not in sb.format_speedup(loss)


def test_solo_excluded_from_speedup_and_wtl() -> None:
    rows = [
        sb.OpRow(
            meta=_meta(
                domain='Discrete global grids',
                label='h3',
                gometry='gometry.h3/1',
                competitor='h3.x/1',
                competitor_label='h3-py',
            ),
            gometry=_stats('gometry.h3/1', 1.0),
            competitor=_stats('h3.x/1', 2.0),
            speedup=2.0,
            tie=False,
        ),
        sb.OpRow(
            meta=_meta(
                domain='Discrete global grids',
                label='s2',
                gometry='gometry.s2/1',
                competitor=None,
                competitor_label=None,
            ),
            gometry=_stats('gometry.s2/1', 0.1),
            competitor=None,
            speedup=None,
            tie=False,
        ),
    ]
    summaries = sb.domain_stats(rows)
    d = summaries['Discrete global grids']
    assert d['n_paired'] == 1
    assert d['wins'] == 1
    assert d['parity'] == 0
    assert d['losses'] == 0
    assert d['geomean'] == pytest.approx(2.0)


def test_equal_domain_overall_geomean() -> None:
    """Overall = geomean of domain geomeans (Geometry's many rows must not dominate)."""
    # Geometry: two rows with ratios 1.0 and 1.0 → domain geomean 1.0
    rows: list[sb.OpRow] = [
        sb.OpRow(
            meta=_meta(
                domain='Geometry',
                gometry=f'gometry.g{i}/1',
                competitor=f'shapely.g{i}/1',
            ),
            gometry=_stats(f'gometry.g{i}/1', 1.0),
            competitor=_stats(f'shapely.g{i}/1', 1.0),
            speedup=1.0,
            tie=False,
        )
        for i in range(2)
    ]
    # Array construction: one row ratio 8.0 → domain geomean 8.0
    rows.append(
        sb.OpRow(
            meta=_meta(
                domain='Array construction & I/O',
                gometry='gometry.io/1',
                competitor='shapely.io/1',
            ),
            gometry=_stats('gometry.io/1', 1.0),
            competitor=_stats('shapely.io/1', 8.0),
            speedup=8.0,
            tie=False,
        )
    )
    summaries = sb.domain_stats(rows)
    # Row-weighted geomean would be (1*1*8)^(1/3) ≈ 2.0
    row_weighted = math.exp(math.log(8.0) / 3.0)
    overall = sb.overall_geomean(summaries)
    assert overall is not None
    # Equal domain weight: geomean(1.0, 8.0) = sqrt(8) ≈ 2.828
    assert overall == pytest.approx(math.sqrt(8.0))
    assert overall != pytest.approx(row_weighted)


def test_smoke_banner_and_workload_labels() -> None:
    ops = [
        {
            'domain': 'Geometry',
            'label': 'Prepared polygon contains XY',
            'workload': '100k probes / 1,316-coordinate holed polygon',
            'suite': 'competitors',
            'gometry': 'gometry.contains_xy/prepared_100k_probes_1316_vertex_polygon',
            'competitor': 'shapely.prepare.contains_xy/100k_probes_1316_vertex_polygon',
            'competitor_label': 'Shapely',
            'footnotes': ['batched'],
        }
    ]
    payload = {
        'profile': 'smoke',
        'publishable': False,
        'public_operations': ops,
    }
    stats = {
        ops[0]['gometry']: _stats(ops[0]['gometry'], 0.001),
        ops[0]['competitor']: _stats(ops[0]['competitor'], 0.002),
    }
    lines = sb.summarize_payload(payload, stats, markdown=True)
    text = '\n'.join(lines)
    assert sb.SMOKE_BANNER in text
    assert '100k probes / 1,316-coordinate holed polygon' in text
    assert 'Prepared polygon contains XY' in text
    assert 'Shapely ·' in text
    assert '2.00× faster' in text  # noqa: RUF001 — product glyph


def test_undeclared_public_gometry_row_fails_manifest_gate() -> None:
    meta = _meta(gometry='gometry.declared/1', competitor='shapely.declared/1')
    stats = {
        meta.gometry: _stats(meta.gometry, 1.0),
        meta.competitor: _stats(meta.competitor, 2.0),
        'gometry.undeclared/1': _stats('gometry.undeclared/1', 3.0),
    }
    with pytest.raises(SystemExit, match=r'undeclared public gometry[\s\S]*undeclared'):
        sb._build_rows([meta], stats, noise_band=0.05)


def test_six_domain_manifest_order() -> None:
    assert list(DOMAIN_ORDER) == list(sb.DOMAIN_ORDER)
    assert len(DOMAIN_ORDER) == 6
    assert DOMAIN_ORDER[0] == 'Array construction & I/O'
    # Build one synthetic row per domain in reverse order; render must restore
    # DOMAIN_ORDER.
    reverse_ops = list(reversed(DOMAIN_ORDER))
    public_ops = []
    stats: dict[str, sb.BenchStats] = {}
    for i, domain in enumerate(reverse_ops):
        g = f'gometry.d{i}/1'
        c = f'shapely.d{i}/1'
        public_ops.append({
            'domain': domain,
            'label': f'label {domain}',
            'workload': f'work {i}',
            'suite': 'competitors',
            'gometry': g,
            'competitor': c,
            'competitor_label': 'Shapely',
            'footnotes': [],
        })
        stats[g] = _stats(g, 1.0)
        stats[c] = _stats(c, 2.0)
    # Re-order public_ops to match reverse domain appearance then force
    # summarizer domain order.
    payload = {
        'profile': 'release',
        'publishable': True,
        'public_operations': public_ops,
    }
    lines = sb.summarize_payload(payload, stats, markdown=True)
    headings = [line[4:] for line in lines if line.startswith('### ')]
    assert headings == list(DOMAIN_ORDER)


def test_missing_public_operations_fails() -> None:
    with pytest.raises(SystemExit, match='public_operations'):
        sb.summarize_payload(
            {'profile': 'smoke', 'publishable': False},
            {},
        )


def test_release_operations_embeddable_shape() -> None:
    """Registry records have the fields the run JSON / summarizer need."""
    for op in RELEASE_OPERATIONS:
        assert op.domain in DOMAIN_ORDER
        assert op.label
        assert op.workload
        assert op.gometry.startswith('gometry.')
        if op.competitor is None:
            assert op.competitor_label is None
        else:
            assert op.competitor_label
