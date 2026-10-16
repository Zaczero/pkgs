"""Distribution and scaling plots for measured observations."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from matplotlib import rc_context
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure
from matplotlib.ticker import EngFormatter, PercentFormatter

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from benchroom.report import Series


_COLORS = {
    False: ('#1769aa', '#9a5700', '#007d66', '#8852a0'),
    True: ('#4aa8ec', '#e5a343', '#64c4ac', '#c797df'),
}


def save(figure: Figure, path: str | Path) -> None:
    """Save a figure with stable SVG identifiers and no generation timestamp."""
    path = Path(path)
    metadata = {'Date': None} if path.suffix.lower() == '.svg' else None
    with rc_context({'svg.hashsalt': 'benchroom'}):
        figure.savefig(
            path, dpi=180, facecolor=figure.get_facecolor(), metadata=metadata
        )


def _figure(title: str, xlabel: str, ylabel: str, dark: bool) -> Figure:
    background, foreground = ('#101820', '#ecf2f7') if dark else ('#ffffff', '#18232e')
    figure = Figure(figsize=(9, 5), layout='constrained', facecolor=background)
    FigureCanvasAgg(figure)
    axes = figure.subplots()
    axes.set_facecolor(background)
    axes.set_title(
        title, loc='left', fontsize=17, fontweight='bold', color=foreground, pad=20
    )
    axes.set_xlabel(xlabel, color=foreground)
    axes.set_ylabel(ylabel, color=foreground)
    axes.tick_params(colors=foreground, length=0, pad=9)
    axes.grid(alpha=0.18, color=foreground)
    axes.set_axisbelow(True)
    for spine in axes.spines.values():
        spine.set_visible(False)
    axes.set_prop_cycle(color=_COLORS[dark])
    return figure


def distribution(
    series: Sequence[Series], *, title: str, unit: str, dark: bool = False
) -> Figure:
    """Plot the fraction of observations at or below each measured value.

    This is an empirical cumulative distribution, without smoothing or bins.
    Each input value has equal weight. Run summaries describe variation across
    runs; individual request times describe variation across requests. Do not
    mix the two in one plot.
    """
    if not series:
        raise ValueError('distribution requires at least one series')
    figure = _figure(title, unit, 'Observations at or below this value', dark)
    axes = figure.axes[0]
    for index, item in enumerate(series):
        ordered = sorted(item.values)
        count = len(ordered)
        axes.step(
            [ordered[0], *ordered],
            [0, *(position / count for position in range(1, count + 1))],
            where='post',
            label=f'{item.case} · {item.name}',
            linestyle=('-', '--', '-.', ':')[index % 4],
            linewidth=2,
        )
    axes.yaxis.set_major_formatter(PercentFormatter(xmax=1))
    axes.set_ylim(0, 1.02)
    if unit == 'seconds':
        axes.xaxis.set_major_formatter(EngFormatter(unit='s'))
    axes.legend(labelcolor='linecolor', frameon=False)
    return figure


def curves(
    values: Mapping[str, Mapping[float, float]],
    *,
    title: str,
    xlabel: str,
    ylabel: str,
    dark: bool = False,
) -> Figure:
    """Plot measured values against input size, load or percentile.

    Each named mapping supplies x-to-y points. Points are sorted by x and
    connected with straight lines; no fitted model or smooth density is
    inferred. The returned figure exposes ordinary Matplotlib axis controls.
    """
    if not values or any(not points for points in values.values()):
        raise ValueError('curves require nonempty measured series')
    figure = _figure(title, xlabel, ylabel, dark)
    axes = figure.axes[0]
    for index, (name, points) in enumerate(values.items()):
        ordered = sorted(points.items())
        x, y = zip(*ordered, strict=True)
        axes.plot(
            x,
            y,
            label=name,
            linewidth=2,
            marker=('o', 's', '^', 'D')[index % 4],
            linestyle=('-', '--', '-.', ':')[index % 4],
        )
    axes.legend(labelcolor='linecolor', frameon=False)
    return figure
