"""Comparison figures and tables from ordinary run samples."""

from __future__ import annotations

import math
import statistics
from dataclasses import InitVar, dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from matplotlib.figure import Figure

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
class Summary:
    """One series' calculated values, shared by every presentation."""

    case: str
    name: str
    median: float
    interval: tuple[float, float] | None
    runs: int
    x: float | None = None


@dataclass(frozen=True)
class Report:
    """Display comparable measurements with shared units and readable labels.

    The caption supplies context such as the machine, workload and setup.
    Rendering does not collect measurements or infer a winner.
    """

    title: str
    series: InitVar[tuple[Series, ...]]
    unit: str
    better: Literal['lower', 'higher'] = 'lower'
    caption: str = ''
    results: tuple[Summary, ...] = field(init=False)

    def __post_init__(self, series: tuple[Series, ...]) -> None:
        if not series:
            raise ValueError('report requires at least one series')
        if self.better not in {'lower', 'higher'}:
            raise ValueError('better must be lower or higher')
        keys = [(item.case, item.name, item.x) for item in series]
        if len(set(keys)) != len(keys):
            raise ValueError('duplicate case, implementation and size')
        object.__setattr__(
            self,
            'results',
            tuple(
                Summary(
                    item.case,
                    item.name,
                    item.median,
                    item.interval,
                    len(item.values),
                    item.x,
                )
                for item in series
            ),
        )

    def figure(self, *, dark: bool = False) -> Figure:
        """Build an editable Matplotlib comparison figure with 95% intervals."""
        from matplotlib.ticker import EngFormatter

        from benchroom.plot import _COLORS, _figure

        colors = _COLORS[dark]
        names = dict.fromkeys(item.name for item in self.results)
        styles = {name: colors[index % len(colors)] for index, name in enumerate(names)}
        cases = {
            case: index
            for index, case in enumerate(
                dict.fromkeys(item.case for item in self.results)
            )
        }
        ordered = sorted(self.results, key=lambda item: cases[item.case])
        figure = _figure(self.title, f'{self.unit} · {self.better} is better', '', dark)
        figure.set_size_inches(9, max(3.2, 1.8 + len(self.results) * 0.48))
        axes = figure.axes[0]
        axes.grid(False, axis='y')
        for index, series in enumerate(ordered):
            color = styles[series.name]
            middle = series.median
            interval = series.interval
            if interval is not None:
                axes.plot(interval, [index, index], color=color, linewidth=2.5)
            axes.plot(middle, index, marker='o', color=color, markersize=7)
        axes.set_yticks(
            range(len(self.results)),
            [
                f'{s.case} · {s.name}' + (f' · {s.x:g}' if s.x is not None else '')
                for s in ordered
            ],
        )
        axes.invert_yaxis()
        if self.unit == 'seconds':
            axes.xaxis.set_major_formatter(EngFormatter(unit='s'))
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
            color=axes.xaxis.label.get_color(),
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
        for series in self.results:
            interval = series.interval
            bounds = (
                '—' if interval is None else f'{interval[0]:.4g} to {interval[1]:.4g}'
            )
            size = '—' if series.x is None else f'{series.x:g}'
            rows.append(
                f'| {escape(series.case)} | {escape(series.name)} | {size} | '
                f'{series.median:.4g} | {bounds} | {series.runs} |'
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
        from benchroom.plot import save

        save(figure, path)
        path.with_suffix('.md').write_text(self.markdown(), encoding='utf-8')
