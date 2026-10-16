from benchroom import Series
from benchroom.plot import curves, distribution


def test_distribution_preserves_observations_and_ties():
    figure = distribution(
        [Series('request', 'A', (3, 1, 1))],
        title='Request latency',
        unit='seconds',
    )
    (line,) = figure.axes[0].lines
    assert list(line.get_xdata()) == [1, 1, 1, 3]
    assert list(line.get_ydata()) == [0, 1 / 3, 2 / 3, 1]


def test_curves_order_by_measured_x_without_fitting():
    figure = curves(
        {'A': {100: 8, 10: 2, 50: 5}},
        title='Input scaling',
        xlabel='Items',
        ylabel='milliseconds',
    )
    (line,) = figure.axes[0].lines
    assert list(line.get_xdata()) == [10, 50, 100]
    assert list(line.get_ydata()) == [2, 5, 8]
