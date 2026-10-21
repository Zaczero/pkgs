import os
import re

import pytest
from numpy.testing import assert_array_equal

from benchroom import Series
from benchroom.plot import curves, distribution, save


def test_distribution_preserves_observations_and_ties():
    figure = distribution(
        [Series('request', 'A', (3, 1, 1))],
        title='Request latency',
        unit='seconds',
    )
    (line,) = figure.axes[0].lines
    assert_array_equal(line.get_xydata(), [[1, 0], [1, 1 / 3], [1, 2 / 3], [3, 1]])


def test_curves_order_by_measured_x_without_fitting():
    figure = curves(
        {'A': {100: 8, 10: 2, 50: 5}},
        title='Input scaling',
        xlabel='Items',
        ylabel='milliseconds',
    )
    (line,) = figure.axes[0].lines
    assert_array_equal(line.get_xydata(), [[10, 2], [50, 5], [100, 8]])


def test_distribution_requires_series():
    with pytest.raises(ValueError, match='series must not be empty'):
        distribution([], title='Request latency', unit='seconds')


@pytest.mark.parametrize(
    ('values', 'message'),
    [
        pytest.param({}, 'values must not be empty', id='empty-values'),
        pytest.param(
            {'A': {}},
            "values for series 'A' must not be empty",
            id='empty-series',
        ),
    ],
)
def test_curves_name_empty_measurements(values, message):
    with pytest.raises(ValueError, match=re.escape(message)) as caught:
        curves(values, title='Input scaling', xlabel='Items', ylabel='ms')
    assert str(caught.value) == message


def test_save_preserves_unchanged_svg(tmp_path):
    path = tmp_path / 'latency.svg'
    figure = distribution(
        [Series('request', 'A', (3, 1, 1))],
        title='Request latency',
        unit='seconds',
    )
    save(figure, path)
    os.utime(path, ns=(1_000_000_000, 1_000_000_000))
    original = path.read_bytes()
    figure = distribution(
        [Series('request', 'A', (3, 1, 1))],
        title='Request latency',
        unit='seconds',
    )
    save(figure, path)
    assert path.stat().st_mtime_ns == 1_000_000_000
    assert path.read_bytes() == original


def test_save_updates_changed_svg(tmp_path):
    path = tmp_path / 'latency.svg'
    figure = distribution(
        [Series('request', 'A', (3, 1, 1))],
        title='Request latency',
        unit='seconds',
    )
    save(figure, path)
    original = path.read_bytes()
    figure.axes[0].set_title('Changed latency')
    save(figure, path)
    assert path.read_bytes() != original
    assert 'Changed latency' in path.read_text()
