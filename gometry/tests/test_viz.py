"""Tests for optional lonboard visualization glue."""

from __future__ import annotations

import re
import subprocess
import sys
from unittest import mock

import gometry as gm
import pytest


def _lonboard_html_markers() -> tuple[str, ...]:
    return (
        'application/vnd.jupyter.widget-state+json',
        'jupyter-widgets',
        'embed-amd.js',
    )


@pytest.fixture
def geographic_points() -> gm.GeometryArray:
    return gm.GeometryArray([
        gm.Point(2.35, 48.86, crs=4326),
        gm.Point(2.36, 48.87, crs=4326),
    ])


def test_lonboard_integration_in_subprocess() -> None:
    """Lonboard widget creation is isolated — one map per interpreter."""
    scripts = (
        """
import gometry as gm
arr = gm.GeometryArray([gm.Point(2.35, 48.86, crs=4326), gm.Point(2.36, 48.87, crs=4326)])
assert gm.explore(arr).__class__.__name__ == 'Map'
""",
        """
import gometry as gm
projected = gm.GeometryArray([gm.Point(300000.0, 5000000.0, crs=32633)])
assert gm.explore(projected).__class__.__name__ == 'Map'
""",
        """
import gometry as gm
arr = gm.GeometryArray([gm.Point(2.35, 48.86, crs=4326), gm.Point(2.36, 48.87, crs=4326)])
html = arr._repr_html_()
assert '<!DOCTYPE html>' in html
assert '<title>gometry map</title>' in html
assert 'application/vnd.jupyter.widget-state+json' in html
assert '<svg' in arr[0]._repr_html_()
""",
    )
    for script in scripts:
        subprocess.run([sys.executable, '-c', script], check=True)


def test_viz_extra_operates_without_pyarrow() -> None:
    subprocess.run(
        [
            sys.executable,
            '-c',
            """
import sys
sys.modules['pyarrow'] = None
import gometry as gm
arr = gm.points([2.35, 2.36], [48.86, 48.87], crs=4326)
assert gm.explore(arr).__class__.__name__ == 'Map'
assert gm.explore(arr, attributes={'name': ['a', 'b']}).__class__.__name__ == 'Map'
""",
        ],
        check=True,
    )


def test_array_repr_html_falls_back_to_svg_when_empty() -> None:
    arr = gm.GeometryArray([], crs=4326)
    html = arr._repr_html_()
    assert '0 geometries' in html
    assert '<svg' not in html


def test_explore_requires_lonboard(geographic_points: gm.GeometryArray) -> None:
    with (
        mock.patch(
            'gometry._viz._require_lonboard',
            side_effect=ModuleNotFoundError(
                "interactive map visualization requires lonboard; install the 'gometry[viz]' extra"
            ),
        ),
        pytest.raises(ModuleNotFoundError, match=r'gometry\[viz\]'),
    ):
        gm.explore(geographic_points)


def test_array_repr_html_falls_back_without_lonboard(
    geographic_points: gm.GeometryArray,
) -> None:
    with mock.patch('gometry._viz._lonboard_available', False):
        html = geographic_points._repr_html_()
    assert '<svg' in html
    assert not any(marker in html for marker in _lonboard_html_markers())


def test_explore_rejects_unsuitable_input() -> None:
    with pytest.raises(gm.GeometryError, match='CRS and finite bounds'):
        gm.explore(gm.GeometryArray([gm.box(0, 0, 1, 1)]))


def test_map_normalizes_non_wgs84_geographic_crs() -> None:
    from gometry._viz import _array_for_map

    nad27 = gm.GeometryArray([gm.Point(-100.0, 40.0, crs=4267)])
    # PROJ lacks the CONUS and Kansas NAD27 grids, so map normalization degrades.
    with pytest.warns(
        gm.AccuracyWarning, match='us_noaa_conus[.]tif.*us_noaa_kshpgn[.]tif'
    ):
        normalized = _array_for_map(nad27)
    assert normalized.crs == 'EPSG:4326'
    assert normalized[0] != nad27[0]


def test_explore_carries_attributes_into_lonboard(
    geographic_points: gm.GeometryArray,
) -> None:
    received: list[object] = []

    class FakeLonboard:
        @staticmethod
        def viz(data: object, **kwargs: object) -> object:
            del kwargs
            received.append(data)
            return object()

    with mock.patch('gometry._viz._require_lonboard', return_value=FakeLonboard):
        gm.explore(geographic_points, attributes={'name': ['a', 'b']})
    table = received[0]
    assert table.column_names == ['geometry', 'name']
    assert table.column('name').to_pylist() == ['a', 'b']


def test_explore_applies_readable_defaults_and_keeps_overrides(
    geographic_points: gm.GeometryArray,
) -> None:
    received: list[dict[str, object]] = []

    class FakeLonboard:
        @staticmethod
        def viz(data: object, **kwargs: object) -> object:
            del data
            received.append(kwargs)
            return object()

    with mock.patch('gometry._viz._require_lonboard', return_value=FakeLonboard):
        gm.explore(
            geographic_points,
            scatterplot_kwargs={'radius_min_pixels': 7},
            polygon_kwargs={'opacity': 0.7},
            map_kwargs={'height': 640},
        )

    kwargs = received[0]
    assert kwargs['scatterplot_kwargs'] == {
        'get_fill_color': [28, 119, 195],
        'radius_min_pixels': 7,
        'radius_max_pixels': 18,
        'opacity': 0.9,
        'pickable': True,
        'auto_highlight': True,
    }
    assert kwargs['path_kwargs']['width_min_pixels'] == 2  # type: ignore[index]
    assert kwargs['polygon_kwargs']['opacity'] == 0.7  # type: ignore[index]
    assert kwargs['map_kwargs']['height'] == 640  # type: ignore[index]


def test_lonboard_capsule_fallback_preserves_feature_properties(
    geographic_points: gm.GeometryArray,
) -> None:
    received: list[object] = []

    class FakeLonboard:
        @staticmethod
        def viz(data: object, **kwargs: object) -> object:
            del kwargs
            received.append(data)
            if len(received) == 1:
                raise TypeError('capsule rejected')
            return object()

    with mock.patch('gometry._viz._require_lonboard', return_value=FakeLonboard):
        gm.explore(geographic_points, attributes={'name': ['a', 'b']})
    feature_collection = received[1]
    assert isinstance(feature_collection, dict)
    assert [
        feature['properties']['name'] for feature in feature_collection['features']
    ] == ['a', 'b']


def test_lonboard_unexpected_errors_are_not_masked(
    geographic_points: gm.GeometryArray,
) -> None:
    class FakeLonboard:
        @staticmethod
        def viz(data: object, **kwargs: object) -> object:
            del data, kwargs
            raise RuntimeError('lonboard bug')

    with (
        mock.patch('gometry._viz._require_lonboard', return_value=FakeLonboard),
        pytest.raises(RuntimeError, match='lonboard bug'),
    ):
        gm.explore(geographic_points)


def test_repr_html_svg_renders_compact_grid() -> None:
    arr = gm.GeometryArray([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)])
    html = arr._repr_html_svg()
    assert '<svg' in html
    assert '2 geometries' in html
    assert re.search(r'<div class="gometry-geom-array">', html)
