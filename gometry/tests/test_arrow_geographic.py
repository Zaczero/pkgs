"""Finite geographic drawings at typed Arrow and Parquet boundaries."""

from __future__ import annotations

import json
import math
from typing import TYPE_CHECKING, Any, Literal

import gometry as gm
import numpy as np
import pytest
from gometry._arrow import _extension_type_from_storage

from tests._support import _capsule_only_from_pyarrow, _StreamOnly

if TYPE_CHECKING:
    from pathlib import Path

pa = pytest.importorskip('pyarrow', reason='typed Arrow interchange requires pyarrow')
pq = pytest.importorskip('pyarrow.parquet', reason='GeoParquet requires pyarrow')


def _foreign_column(
    source: gm.GeometryArray[gm.Geometry | None],
    encoding: Literal['auto', 'wkb'],
    *,
    epoch: float | None = None,
) -> Any:
    """Attach a geographic frame to an independently literal planar drawing."""
    raw: Any = source.to_arrow(encoding=encoding)
    extension = _extension_type_from_storage(
        pa, raw.type.extension_name, raw.storage.type, 'EPSG:4326', epoch
    )
    return pa.ExtensionArray.from_storage(extension, raw.storage)


def _import(column: Any, frontend: str) -> gm.GeometryArray[gm.Geometry | None]:
    if frontend == 'cdata':
        column = _capsule_only_from_pyarrow(column)
    elif frontend == 'stream':
        column = _StreamOnly(pa.table({'geometry': column}))
    return gm.from_arrow(column)


def _export(
    values: gm.Geometry | gm.GeometryArray[gm.Geometry | None], frontend: str
) -> Any:
    if frontend == 'cdata':
        return pa.array(values)
    if frontend == 'stream':
        return pa.chunked_array(_StreamOnly(values))
    return values.to_arrow(encoding='wkb' if frontend == 'wkb' else 'auto')


@pytest.mark.parametrize('encoding', ['auto', 'wkb'])
@pytest.mark.parametrize('frontend', ['pyarrow', 'cdata', 'stream'])
def test_literal_long_edge_is_refined_before_geographic_admission(
    encoding: Literal['auto', 'wkb'], frontend: str
) -> None:
    source = gm.GeometryArray([
        gm.LineString([(170, 0), (-170, 0)], z=[7, 19], m=[23, 47]),
        None,
    ])
    result = _import(_foreign_column(source, encoding, epoch=2020.5), frontend)
    assert result.is_missing.tolist() == [False, True]
    assert result.crs == gm.CRS(4326)
    assert result.epoch == 2020.5
    line = result[0]
    assert line is not None
    assert gm.intersects(line, gm.Point(0, 0, crs=4326, epoch=2020.5))
    assert not gm.intersects(line, gm.Point(180, 0, crs=4326, epoch=2020.5))
    coordinates = list(line.coords.select('XYZM'))
    assert coordinates[0] == (170, 0, 7, 23)
    assert coordinates[-1] == (-170, 0, 19, 47)
    assert (0, 0, 13, 35) in coordinates
    # The raw codec continues to publish the foreign coordinates literally.
    original = source[0]
    assert original is not None
    assert (
        gm.from_wkb(original.to_wkb()).coords.to_nested() == original.coords.to_nested()
    )


@pytest.mark.parametrize('encoding', ['auto', 'wkb'])
@pytest.mark.parametrize('frontend', ['pyarrow', 'cdata', 'stream'])
def test_adjacent_binary64_heights_produce_a_finite_long_branch_drawing(
    encoding: Literal['auto', 'wkb'], frontend: str
) -> None:
    low = 0.1
    high = math.nextafter(low, math.inf)
    source = gm.GeometryArray([gm.LineString([(170, low), (-170, high)])])
    result = _import(_foreign_column(source, encoding), frontend)[0]
    assert result is not None
    coordinates = result.coords.to_nested()
    assert coordinates[0] == (170, low)
    assert coordinates[-1] == (-170, high)
    assert len(coordinates) > 2
    assert np.isfinite(np.asarray(coordinates)).all()
    assert gm.intersects(result, gm.LineString([(0, 0), (0, 1)], crs=4326))
    assert not gm.intersects(result, gm.LineString([(180, 0), (180, 1)], crs=4326))


def test_geographic_export_canonicalizes_aliases_without_mutating_source() -> None:
    source = gm.LineString([(530, 2), (190, 4)], z=[7, 19], crs=4326)
    result = gm.from_arrow(source.to_arrow())[0]
    assert result is not None
    xy = np.asarray(list(result.coords.select('XY')))
    assert np.abs(xy[:, 0]).max() <= 180
    assert (170, 2, 7) in list(result.coords.select('XYZ'))
    assert (-170, 4, 19) in list(result.coords.select('XYZ'))
    assert source.coords.to_nested() == [(530, 2, 7), (190, 4, 19)]


@pytest.mark.parametrize(
    ('wkt', 'inside', 'outside'),
    [
        pytest.param('LINESTRING (170 75, -170 75)', (180, 75), (0, 75), id='seam'),
        pytest.param(
            'POLYGON ((-135 80, -45 80, 45 80, 135 80, -135 80))',
            (0, 89),
            (0, 0),
            id='pole',
        ),
    ],
)
def test_geographic_wkb_drawing_is_readable_by_shapely(
    wkt: str, inside: tuple[int, int], outside: tuple[int, int]
) -> None:
    shapely = pytest.importorskip('shapely', reason='independent WKB reader')
    source = gm.from_wkt(wkt, crs=4326)
    column: Any = source.to_arrow(encoding='wkb')
    drawing = shapely.from_wkb(column.storage[0].as_py())
    assert drawing.intersects(shapely.Point(inside))
    assert not drawing.intersects(shapely.Point(outside))


@pytest.mark.parametrize('frontend', ['pyarrow', 'cdata', 'stream', 'wkb'])
def test_adjacent_binary64_heights_export_as_a_finite_seam_drawing(
    frontend: str,
) -> None:
    low = 0.1
    high = math.nextafter(low, math.inf)
    source = gm.LineString([(170, low), (-170, high)], crs=4326)
    result = gm.from_arrow(_export(gm.GeometryArray([source]), frontend))[0]
    assert result is not None
    coordinates = list(result.coords)
    assert (170, low) in coordinates
    assert (-170, high) in coordinates
    assert len(coordinates) > 2
    assert np.isfinite(np.asarray(coordinates)).all()
    drawing = gm.from_wkb(result.to_wkb())
    assert gm.intersects(drawing, gm.LineString([(180, 0), (180, 1)]))
    assert not gm.intersects(drawing, gm.LineString([(0, 0), (0, 1)]))


def test_literal_turn_expansion_uses_the_existing_generated_work_budget() -> None:
    source = gm.GeometryArray([gm.LineString([(0, 1), (360_000_000_000, 2)])])
    column = _foreign_column(source, 'wkb')
    with pytest.raises(gm.ResourceLimitError, match='literal edge subdivision'):
        gm.from_arrow(column)


@pytest.mark.parametrize('frontend', ['pyarrow', 'cdata', 'stream', 'wkb'])
def test_outgoing_seam_drawing_uses_source_arclength_ordinates(frontend: str) -> None:
    line = gm.LineString([(170, 70), (-170, 80)], z=[0, 1000], m=[0, 1000], crs=4326)
    values = gm.GeometryArray([line, None, gm.LineString([(1, 2), (3, 4)], crs=4326)])
    restored = gm.from_arrow(_export(values, frontend))
    assert restored.is_missing.tolist() == [False, True, False]
    first = restored[0]
    assert first is not None
    drawing = gm.from_wkb(first.to_wkb())
    assert gm.intersects(drawing, gm.Point(180, 75))
    assert not gm.intersects(drawing, gm.Point(0, 75))
    coordinates = np.asarray(list(drawing.coords.select('XYZM')))
    seam = coordinates[np.abs(coordinates[:, 0]) == 180]
    np.testing.assert_allclose(seam[:, 2:], 516.9666283425563)
    assert (170, 70, 0, 0) in list(drawing.coords.select('XYZM'))
    assert (-170, 80, 1000, 1000) in list(drawing.coords.select('XYZM'))
    # Typed export never mutates the source or changes raw WKB.
    assert gm.from_wkb(line.to_wkb()).coords.to_nested() == line.coords.to_nested()
    last = restored[2]
    original_last = values[2]
    assert last is not None
    assert original_last is not None
    assert list(last.coords) == list(original_last.coords)


@pytest.mark.parametrize('axes', ['Z', 'M', 'ZM'])
@pytest.mark.parametrize('frontend', ['pyarrow', 'cdata', 'stream', 'wkb'])
def test_polar_drawing_extends_incident_ordinates(axes: str, frontend: str) -> None:
    extra = ' 17 29' if axes == 'ZM' else ' 17'
    ring = ', '.join(f'{x} 80{extra}' for x in (-135, -45, 45, 135, -135))
    source = gm.from_wkt(f'POLYGON {axes} (({ring}))', crs=4326)
    result = gm.from_arrow(_export(source, frontend))[0]
    assert result is not None
    drawing = gm.from_wkb(result.to_wkb())
    assert gm.intersects(drawing, gm.Point(0, 89))
    assert not gm.intersects(drawing, gm.Point(0, 0))
    if 'Z' in axes:
        assert set(drawing.coords.z) == {17}
    if 'M' in axes:
        assert set(drawing.coords.m) == ({29} if axes == 'ZM' else {17})


@pytest.mark.parametrize('encoding', ['wkb', 'geoarrow', 'geometry'])
def test_parquet_metadata_covering_and_rows_follow_published_drawing(
    tmp_path: Path, encoding: Literal['wkb', 'geoarrow', 'geometry']
) -> None:
    path = tmp_path / 'seam.parquet'
    source = gm.GeometryArray([
        gm.LineString([(170, 0), (-170, 0)], crs=4326),
        None,
        gm.LineString([(1, 2), (3, 4)], crs=4326),
    ])
    source.to_geoparquet(
        path, encoding=encoding, attributes={'id': [31, 47, 59]}, row_group_size=1
    )
    metadata = json.loads(pq.read_schema(path).metadata[b'geo'])
    assert metadata['columns']['geometry']['geometry_types'] == ['MultiLineString']
    restored, attributes = gm.from_geoparquet(path, row_groups=[2, 0, 2, 1])
    assert restored.is_missing.tolist() == [False, False, False, True]
    assert attributes['id'].to_pylist() == [59, 31, 59, 47]
    cut = restored[1]
    assert cut is not None
    drawing = gm.from_wkb(cut.to_wkb())
    assert gm.intersects(drawing, gm.Point(180, 0))
    assert not gm.intersects(drawing, gm.Point(0, 0))
    selected, selected_attributes = gm.from_geoparquet(path, bbox=(179, -1, 180, 1))
    assert len(selected) == 1
    assert selected_attributes['id'].to_pylist() == [31]


def test_plain_native_parquet_admits_literal_long_edges(tmp_path: Path) -> None:
    path = tmp_path / 'foreign.parquet'
    literal = gm.GeometryArray([gm.LineString([(170, 0), (-170, 0)]), None])
    column = _foreign_column(literal, 'wkb')
    pq.write_table(pa.table({'geometry': column, 'id': [31, 47]}), path)
    result, attributes = gm.from_geoparquet(path)
    assert result.is_missing.tolist() == [False, True]
    line = result[0]
    assert line is not None
    assert gm.intersects(line, gm.Point(0, 0, crs=4326))
    assert not gm.intersects(line, gm.Point(180, 0, crs=4326))
    assert attributes['id'].to_pylist() == [31, 47]
