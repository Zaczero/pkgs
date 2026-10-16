"""Render pyperf timing files collected with benchmark.py.

From the totp-rs checkout, collect both implementations with ``benchmark.py
--impl pyotp -o pyotp.json`` and ``--impl totp-rs -o totp-rs.json``, using the
bench dependency group. Then call ``report`` with those two paths. Result
metadata supplies the implementation versions; file names label older results.
"""

from pathlib import Path

import pyperf
from benchroom import Report
from benchroom.pyperf import load
from matplotlib.ticker import EngFormatter


def report(baseline: str, candidate: str, output: str = 'benchmark.svg') -> None:
    """Write a TOTP timing chart and table from two pyperf result files."""
    series = load(candidate) + load(baseline)
    metadata = pyperf.BenchmarkSuite.load(candidate).get_metadata()
    comparison = Report(
        'TOTP generation and verification',
        series,
        unit='seconds',
        caption=(
            f'{metadata["cpu_model_name"]} · CPython {metadata["python_version"]}\n'
            'SHA-1 · 6 digits · 30-second period · ±1 verification window\n'
            'Independent worker-process means · setup excluded'
        ),
    )
    path = Path(output)
    cases = {
        case: index
        for index, case in enumerate(dict.fromkeys(item.case for item in series))
    }
    ordered = sorted(comparison.results, key=lambda item: cases[item.case])
    for dark in (False, True):
        figure = comparison.figure(dark=dark)
        axes = figure.axes[0]
        axes.set_xscale('log')
        axes.xaxis.set_major_formatter(EngFormatter(unit='s'))
        axes.set_xlabel('Time per call · logarithmic scale · lower is better')
        axes.set_xlim(
            min(item.median for item in ordered) * 0.6,
            max(item.median for item in ordered) * 2,
        )
        for index, item in enumerate(ordered):
            axes.annotate(
                f'{item.median * 1e6:.3g} µs',
                (item.median, index),
                xytext=(0, -14),
                textcoords='offset points',
                ha='center',
                fontsize=9,
                color=axes.xaxis.label.get_color(),
            )
        destination = path.with_stem(path.stem + '-dark') if dark else path
        figure.savefig(destination, dpi=180, facecolor=figure.get_facecolor())
    path.with_suffix('.md').write_text(comparison.markdown(), encoding='utf-8')
