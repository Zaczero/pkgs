"""Comparison figures and tables from ordinary run samples."""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure
from matplotlib.ticker import EngFormatter

from benchroom.stats import median_interval


@dataclass(frozen=True)
class Series:
    """One implementation's measurements of one case.

    Values are independent-run summaries in the report's unit. Keep readings
    from the same process together before constructing a series. ``x`` is an
    optional numeric input size for scaling plots.
    """

    case: str
    name: str
    values: tuple[float, ...]
    x: float | None = None

    def __post_init__(self) -> None:
        if not self.values or any(not math.isfinite(value) for value in self.values):
            raise ValueError('series requires nonempty finite measurements')

    @property
    def median(self) -> float:
        """Median run measurement."""
        return statistics.median(self.values)

    @property
    def interval(self) -> tuple[float, float] | None:
        """95% bootstrap interval, or None when only one run exists."""
        return median_interval(self.values) if len(self.values) > 1 else None


@dataclass(frozen=True)
class Report:
    """Display comparable measurements with shared units and readable labels.

    The caption supplies context such as the machine, workload and setup.
    Rendering does not collect measurements or infer a winner.
    """

    title: str
    series: tuple[Series, ...]
    unit: str
    better: Literal['lower', 'higher'] = 'lower'
    caption: str = ''

    def __post_init__(self) -> None:
        if not self.series:
            raise ValueError('report requires at least one series')
        if self.better not in {'lower', 'higher'}:
            raise ValueError('better must be lower or higher')
        keys = [(series.case, series.name, series.x) for series in self.series]
        if len(set(keys)) != len(keys):
            raise ValueError('duplicate case, implementation and size')

    def figure(self, *, dark: bool = False) -> Figure:
        """Build an editable Matplotlib comparison figure with 95% intervals."""
        background, foreground = (
            ('#101820', '#ecf2f7') if dark else ('#ffffff', '#18232e')
        )
        muted = '#a8bac9' if dark else '#526475'
        colors = (
            ('#4aa8ec', '#e5a343', '#64c4ac', '#c797df')
            if dark
            else ('#1769aa', '#9a5700', '#007d66', '#8852a0')
        )
        names = list(dict.fromkeys(series.name for series in self.series))
        cases = list(dict.fromkeys(series.case for series in self.series))
        ordered = sorted(self.series, key=lambda series: cases.index(series.case))
        figure = Figure(
            figsize=(9, max(3.2, 1.8 + len(self.series) * 0.48)), layout='constrained'
        )
        FigureCanvasAgg(figure)
        figure.set_facecolor(background)
        axes = figure.subplots()
        axes.set_facecolor(background)
        for index, series in enumerate(ordered):
            color = colors[names.index(series.name) % len(colors)]
            middle = series.median
            interval = series.interval
            if interval is not None:
                axes.plot(interval, [index, index], color=color, linewidth=2.5)
            axes.plot(middle, index, marker='o', color=color, markersize=7)
        axes.set_yticks(
            range(len(self.series)),
            [
                f'{s.case} · {s.name}' + (f' · {s.x:g}' if s.x is not None else '')
                for s in ordered
            ],
        )
        axes.invert_yaxis()
        axes.set_xlabel(f'{self.unit} · {self.better} is better', color=muted)
        if self.unit == 'seconds':
            axes.xaxis.set_major_formatter(EngFormatter(unit='s'))
        axes.set_title(
            self.title,
            loc='left',
            fontsize=17,
            fontweight='bold',
            color=foreground,
            pad=20,
        )
        axes.tick_params(colors=foreground, length=0, pad=9)
        axes.grid(axis='x', alpha=0.18, color=muted)
        axes.set_axisbelow(True)
        for spine in axes.spines.values():
            spine.set_visible(False)
        figure.supxlabel(
            '\n'.join(
                filter(
                    None,
                    [
                        self.caption,
                        'Dots: medians · Lines: 95% bootstrap intervals across runs',
                    ],
                )
            ),
            fontsize=9,
            color=muted,
        )
        return figure

    def markdown(self) -> str:
        """Return an accessible table using the same estimates as the figure."""

        def escape(text: str) -> str:
            return text.replace('|', r'\|').replace('\n', ' ')

        rows = [
            f'# {self.title}',
            '',
            self.caption,
            '',
            f'Values in {self.unit}; {self.better} is better.',
            '',
            '| Case | Implementation | Size | Median | 95% interval | Runs |',
            '| --- | --- | ---: | ---: | --- | ---: |',
        ]
        for series in self.series:
            interval = series.interval
            bounds = (
                '—' if interval is None else f'{interval[0]:.4g} to {interval[1]:.4g}'
            )
            size = '—' if series.x is None else f'{series.x:g}'
            rows.append(
                f'| {escape(series.case)} | {escape(series.name)} | {size} | '
                f'{series.median:.4g} | {bounds} | {len(series.values)} |'
            )
        rows.extend([
            '',
            'Intervals use a percentile bootstrap over independent runs. A single run has no interval.',
            '',
        ])
        return '\n'.join(rows)

    def save(self, path: str | Path, *, dark: bool = False) -> None:
        """Write SVG or PNG and an adjacent Markdown table; parents must exist."""
        path = Path(path)
        if path.suffix.lower() not in {'.svg', '.png'}:
            raise ValueError('chart path must end in .svg or .png')
        figure = self.figure(dark=dark)
        figure.savefig(path, dpi=180, facecolor=figure.get_facecolor())
        path.with_suffix('.md').write_text(self.markdown(), encoding='utf-8')
