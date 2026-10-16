"""Render the complete Pillow-image benchmark from its pyperf results."""

from pathlib import Path

import pyperf
from benchroom import Report
from benchroom.plot import curves, save
from benchroom.pyperf import load
from matplotlib.ticker import EngFormatter


def report(paths: list[str], output_directory: str) -> None:
    """Write encoding and decoding curves, with one table for every case."""
    series = tuple(item for path in paths for item in load(path))
    metadata = pyperf.BenchmarkSuite.load(paths[0]).get_metadata()
    caption = (
        f'{metadata["cpu_model_name"]} · CPython {metadata["python_version"]}\n'
        'RGB Pillow images · 4x3 components · input preserved · RGB image output'
    )
    comparison = Report(
        'BlurHash image processing', series, unit='seconds', caption=caption
    )
    root = Path(output_directory)
    root.mkdir(parents=True, exist_ok=True)
    root.joinpath('results.md').write_text(comparison.markdown(), encoding='utf-8')
    for operation in ('encode', 'decode'):
        selected = [
            item for item in comparison.results if item.case.startswith(operation)
        ]
        names = tuple(dict.fromkeys(item.name for item in selected))
        values = {
            name: {
                float(item.case.split()[1].split('x')[0]): item.median
                for item in selected
                if item.name == name
            }
            for name in names
        }
        for dark in (False, True):
            figure = curves(
                values,
                title=f'BlurHash {operation}',
                xlabel='Square image side (pixels)',
                ylabel='Time per image · logarithmic scale · lower is better',
                dark=dark,
            )
            axes = figure.axes[0]
            axes.set_xticks((32, 64, 128))
            axes.set_yscale('log')
            axes.yaxis.set_major_formatter(EngFormatter(unit='s'))
            for name, line in zip(names, tuple(axes.lines), strict=True):
                for item in selected:
                    if item.name != name or item.interval is None:
                        continue
                    low, high = item.interval
                    axes.errorbar(
                        float(item.case.split()[1].split('x')[0]),
                        item.median,
                        yerr=[[item.median - low], [high - item.median]],
                        fmt='none',
                        color=line.get_color(),
                        capsize=4,
                    )
            figure.supxlabel(
                caption
                + '\nMedians · Whiskers: 95% bootstrap intervals across worker processes',
                fontsize=9,
                color=axes.xaxis.label.get_color(),
            )
            name = f'{operation}-dark.svg' if dark else f'{operation}.svg'
            save(figure, root / name)
