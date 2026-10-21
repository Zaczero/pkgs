"""Distribution and scaling plots for measured observations."""

# Matplotlib leaves plotting keyword arguments and cycler parameters untyped.
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false

from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING

from matplotlib import rc_context
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure
from matplotlib.rcsetup import cycler
from matplotlib.ticker import EngFormatter, PercentFormatter

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from contextlib import AbstractContextManager

    from benchroom.report import Series


PALETTE = {
    'blue': ('#4477aa', '#79addc'),
    'green': ('#228833', '#70bf80'),
    'red': ('#cc3311', '#ee8877'),
    'gold': ('#917600', '#d6bd58'),
}
_COLORS = {
    dark: tuple(pair[dark] for pair in PALETTE.values()) for dark in (False, True)
}


def theme(*, dark: bool = False) -> AbstractContextManager[None]:
    """Use transparent surfaces and legible foregrounds on light or dark pages."""
    foreground = '#dce1ea' if dark else '#253044'
    return rc_context({
        'figure.facecolor': 'none',
        'axes.facecolor': 'none',
        'text.color': foreground,
        'axes.labelcolor': foreground,
        'axes.edgecolor': foreground,
        'xtick.color': foreground,
        'ytick.color': foreground,
        'grid.color': foreground,
        'grid.alpha': 0.16,
        'axes.spines.top': False,
        'axes.spines.right': False,
        'axes.spines.bottom': False,
        'axes.spines.left': False,
        'axes.axisbelow': True,
        'legend.frameon': False,
        'axes.prop_cycle': cycler(color=_COLORS[dark]),
    })


def save(figure: Figure, path: str | Path) -> None:
    """Save a figure, preserving unchanged SVG files and their timestamps."""
    path = Path(path)
    svg = path.suffix.lower() == '.svg'
    output = BytesIO() if svg else path
    with rc_context({'svg.hashsalt': 'benchroom'}):
        figure.savefig(
            output,
            format='svg' if svg else None,
            dpi=180,
            facecolor=figure.get_facecolor(),
            metadata={'Date': None} if svg else None,
        )
    if isinstance(output, BytesIO):
        content = (
            '\n'.join(
                line.rstrip() for line in output.getvalue().decode('utf-8').splitlines()
            )
            + '\n'
        ).encode('utf-8')
        if not path.exists() or path.read_bytes() != content:
            path.write_bytes(content)


def _figure(title: str, xlabel: str, ylabel: str, dark: bool) -> Figure:
    with theme(dark=dark):
        figure = Figure(figsize=(9, 5), layout='constrained')
        axes = figure.subplots()
        FigureCanvasAgg(figure)
        axes.set_title(title, loc='left', fontsize=17, fontweight='bold', pad=20)
        axes.set_xlabel(xlabel)
        axes.set_ylabel(ylabel)
        axes.tick_params(length=0, pad=9)
        axes.grid(True)
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
        raise ValueError('series must not be empty')
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
    axes.legend(labelcolor=axes.xaxis.label.get_color(), frameon=False)
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
    if not values:
        raise ValueError('values must not be empty')
    for name, points in values.items():
        if not points:
            raise ValueError(f'values for series {name!r} must not be empty')
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
    axes.legend(labelcolor=axes.xaxis.label.get_color(), frameon=False)
    return figure
