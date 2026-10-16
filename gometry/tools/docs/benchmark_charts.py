"""Render scaling figures from the benchmark page's published measurements."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

from benchroom.plot import curves, save
from matplotlib.ticker import EngFormatter, PercentFormatter, StrMethodFormatter

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT / 'benches' / 'support'))
from summarize_bench import (
    DOMAIN_ORDER,
    PRACTICAL_EQUIVALENCE_MARGIN,
    _build_rows,
    _humanize_seconds,
    _load_all_blocks,
    _parse_public_operations,
    load_run_artifacts,
    validate_exact_paired_matrix,
)


def on_pre_build(config, **_kwargs):
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
    by_curve = {}
    for row in rows:
        if row.meta.scaling is not None:
            by_curve.setdefault(row.meta.scaling['curve_id'], []).append(row)
    cards = ['<div class="benchmark-scaling-grid" markdown="1">', '']
    for curve, selected in sorted(
        by_curve.items(), key=lambda item: DOMAIN_ORDER.index(item[1][0].meta.domain)
    ):
        selected.sort(key=lambda row: row.meta.scaling['point'])
        scaling = selected[0].meta.scaling
        label = selected[0].meta.label
        cards.extend((
            '<div class="benchmark-scaling-card" markdown="1">',
            '',
            f'### {label}',
            '',
            selected[0].meta.workload + '.',
            '',
        ))
        if selected[0].meta.domain == 'Spatial index':
            cards.extend((
                'Bounding-box candidates, without exact predicate refinement.',
                '',
            ))
        measurements = '; '.join(
            f'{row.meta.scaling["point"]:g} {scaling["unit"]}: '
            f'gometry {_humanize_seconds(row.gometry.median)}, '
            f'{row.meta.competitor_label} {_humanize_seconds(row.competitor.median)}'
            for row in selected
        )
        values = {
            'gometry': {
                row.meta.scaling['point']: row.gometry.median for row in selected
            },
            selected[0].meta.competitor_label: {
                row.meta.scaling['point']: row.competitor.median for row in selected
            },
        }
        for dark in (False, True):
            figure = curves(
                values,
                title='',
                xlabel=scaling['dimension'].replace('-', ' ').capitalize(),
                ylabel='Call time',
                dark=dark,
            )
            figure.set_size_inches(4, 3)
            axes = figure.axes[0]
            axes.xaxis.label.set_fontsize(12)
            axes.yaxis.label.set_fontsize(12)
            axes.tick_params(labelsize=12, pad=6)
            axes.set_xticks(sorted(values['gometry']))
            axes.xaxis.set_major_formatter(
                PercentFormatter(xmax=1)
                if scaling['unit'] == 'fraction'
                else StrMethodFormatter('{x:,.0f}')
            )
            axes.legend(
                labelcolor=axes.xaxis.label.get_color(), frameon=False, fontsize=12
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
            cards.extend((
                f'![{label}. Median call times — {measurements}.]'
                f'(../assets/benchmark-{curve}-{theme}.svg#only-{theme})',
                '',
            ))
        cards.extend(('</div>', ''))
    cards.append('</div>')
    config.extra['_benchmark_scaling'] = '\n'.join(cards)


def on_page_markdown(markdown, page, config, **_kwargs):
    if page.file.src_uri == 'about/benchmarks.md':
        return markdown.replace(
            '<!-- benchmark-scaling -->', config.extra['_benchmark_scaling']
        )
    return markdown


if __name__ == '__main__':
    from types import SimpleNamespace

    on_pre_build(SimpleNamespace(extra={}))
