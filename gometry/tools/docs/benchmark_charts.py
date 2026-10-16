"""Render scaling figures from the benchmark page's published measurements."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

from benchroom.plot import curves, save
from matplotlib.ticker import EngFormatter

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT / 'benches' / 'support'))
from summarize_bench import (
    PRACTICAL_EQUIVALENCE_MARGIN,
    _build_rows,
    _load_all_blocks,
    _parse_public_operations,
    load_run_artifacts,
    validate_exact_paired_matrix,
)


def on_pre_build(**_kwargs):
    summary = (_ROOT / 'benches/results/current/summary.md').read_text()
    match = re.search(r'assets/benchmark-runs/([0-9a-f]+)/run\.json', summary)
    if match is None:
        raise ValueError('Published benchmark summary has no run manifest')
    manifest = _ROOT / 'docs/assets/benchmark-runs' / match[1] / 'run.json'
    payload = json.loads(manifest.read_text())
    artifacts = load_run_artifacts(payload, manifest)
    validate_exact_paired_matrix(payload, artifacts)
    rows = _build_rows(
        _parse_public_operations(payload['public_operations']),
        _load_all_blocks(artifacts),
        equivalence_margin=PRACTICAL_EQUIVALENCE_MARGIN,
        expected_blocks=payload['sampling']['measured_blocks'],
        expected_leads=tuple(payload['sampling']['lead_orders']),
    )
    for curve, title, xlabel in (
        (
            'spatial-index-cardinality',
            'Spatial index queries',
            'Indexed polygons · 1,000 queries per call',
        ),
        (
            'overlay-disjoint-input-count',
            'Union of disjoint polygons',
            'Input polygons',
        ),
    ):
        selected = [
            row
            for row in rows
            if row.meta.scaling and row.meta.scaling['curve_id'] == curve
        ]
        values = {
            'gometry': {
                row.meta.scaling['point']: row.gometry.median for row in selected
            },
            'Shapely': {
                row.meta.scaling['point']: row.competitor.median for row in selected
            },
        }
        for dark in (False, True):
            figure = curves(
                values,
                title=title,
                xlabel=xlabel,
                ylabel='Median seconds per call',
                dark=dark,
            )
            for index, name in enumerate(('gometry', 'competitor')):
                for row in selected:
                    samples = [getattr(block, f'{name}_median') for block in row.blocks]
                    median = getattr(row, name).median
                    figure.axes[0].errorbar(
                        row.meta.scaling['point'],
                        median,
                        yerr=[[median - min(samples)], [max(samples) - median]],
                        color=figure.axes[0].lines[index].get_color(),
                        capsize=4,
                        fmt='none',
                    )
            figure.axes[0].yaxis.set_major_formatter(EngFormatter(unit='s'))
            figure.axes[0].set_ylim(bottom=0)
            theme = 'dark' if dark else 'light'
            save(figure, _ROOT / f'docs/assets/benchmark-{curve}-{theme}.svg')


if __name__ == '__main__':
    on_pre_build()
