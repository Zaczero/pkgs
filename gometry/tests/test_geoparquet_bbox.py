"""Spatial GeoParquet selections preserve geometry and attribute rows."""

from __future__ import annotations

import io
import json
import math
from typing import TYPE_CHECKING

import gometry as gm
import pytest

if TYPE_CHECKING:
    from pathlib import Path

pa = pytest.importorskip('pyarrow', reason='GeoParquet bbox reads require pyarrow')
pq = pytest.importorskip(
    'pyarrow.parquet', reason='GeoParquet bbox reads require pyarrow'
)


@pytest.mark.parametrize(
    ('encoding', 'statistics'),
    [('wkb', True), ('geometry', True), ('geometry', False)],
)
def test_bbox_refines_candidates_and_aligns_projected_attributes(
    tmp_path: Path, encoding: str, statistics: bool
) -> None:
    path = tmp_path / 'features.parquet'
    source = gm.GeometryArray([
        gm.from_wkt(
            'POLYGON ((-5 -5,5 -5,5 5,-5 5,-5 -5),(-3 -3,-3 3,3 3,3 -3,-3 -3))'
        ),
        gm.Point(1, 1),
        gm.Point(2, 2),
        gm.Point(10, 10),
        gm.from_wkt('POINT EMPTY'),
        None,
    ])
    source.to_geoparquet(
        path,
        encoding=encoding,
        attributes={'id': [11, 13, 17, 19, 23, 29], 'label': list('abcdef')},
        row_group_size=2,
        write_statistics=statistics,
    )
    features = gm.from_geoparquet(
        path, bbox=(0, 0, 2, 2), columns=['label'], filters=[('id', '>', 12)]
    )
    restored = features.geometries
    assert restored.to_wkt() == ['POINT (1 1)', 'POINT (2 2)']
    assert list(features.column('label')) == ['b', 'c']
    unfiltered = gm.from_geoparquet(path, bbox=(0, 0, 2, 2)).geometries
    assert unfiltered.to_wkt() == ['POINT (1 1)', 'POINT (2 2)']


def test_covering_filter_prunes_geometry_decoding(tmp_path: Path) -> None:
    path = tmp_path / 'covering-pruned.parquet'
    region = pa.StructArray.from_arrays(
        [
            pa.array(values)
            for values in ([7.0, 1007.0], [13.0, 1013.0], [7.0, 1007.0], [13.0, 1013.0])
        ],
        names=['xmin', 'ymin', 'xmax', 'ymax'],
    )
    table = pa.table({
        'geometry': [gm.Point(7, 13).to_wkb(), b'\xff'],
        'region': region,
        'id': [71, 73],
    })
    metadata = {
        'version': '1.1.0',
        'primary_column': 'geometry',
        'columns': {
            'geometry': {
                'encoding': 'WKB',
                'geometry_types': ['Point'],
                'crs': None,
                'covering': {
                    'bbox': {
                        axis: ['region', axis]
                        for axis in ('xmin', 'ymin', 'xmax', 'ymax')
                    }
                },
            }
        },
    }
    pq.write_table(
        table.replace_schema_metadata({b'geo': json.dumps(metadata).encode()}), path
    )
    features = gm.from_geoparquet(path, bbox=(0, 0, 20, 20))
    restored = features.geometries
    assert restored.to_wkt() == ['POINT (7 13)']
    assert list(features.column('id')) == [71]
    with pytest.raises(gm.ParseError):
        gm.from_geoparquet(path)


@pytest.mark.parametrize('encoding', ['wkb', 'geoarrow', 'geometry'])
def test_bbox_row_groups_keep_order_duplicates_and_stream_ownership(
    encoding: str,
) -> None:
    stream = io.BytesIO()
    gm.points([1, 3, 5], [2, 4, 6]).to_geoparquet(
        stream,
        encoding=encoding,
        row_group_size=1,
        attributes={'id': [31, 37, 41], 'label': ['a', 'b', 'c']},
    )
    stream.seek(0)
    features = gm.from_geoparquet(
        stream,
        row_groups=iter([2, 0, 2, 1]),
        bbox=iter([0, 0, 10, 10]),
        filters=[('id', '!=', 37)],
        columns=['label'],
    )
    restored = features.geometries
    assert restored.to_wkt() == ['POINT (5 6)', 'POINT (1 2)', 'POINT (5 6)']
    assert list(features.column('label')) == ['c', 'a', 'c']
    assert not stream.closed


@pytest.mark.parametrize('encoding', ['wkb', 'geoarrow'])
def test_covering_does_not_replace_user_bbox_attribute(
    tmp_path: Path, encoding: str
) -> None:
    path = tmp_path / 'covering.parquet'
    source = gm.GeometryArray([gm.Point(1, 2), gm.from_wkt('POINT EMPTY'), None])
    source.to_geoparquet(path, encoding=encoding, attributes={'bbox': [43, 47, 53]})
    features = gm.from_geoparquet(path)
    restored = features.geometries
    assert restored.to_wkt() == source.to_wkt()
    assert list(features.column('bbox')) == [43, 47, 53]
    table = pq.read_table(path)
    geo = json.loads(table.schema.metadata[b'geo'])
    covering_name = geo['columns']['geometry']['covering']['bbox']['xmin'][0]
    assert table[covering_name].is_null().to_pylist() == [False, False, True]
    selected = gm.from_geoparquet(path, bbox=(0, 0, 3, 3), columns=['bbox'])
    assert selected.geometries.to_wkt() == ['POINT (1 2)']
    assert list(selected.column('bbox')) == [43]


@pytest.mark.parametrize('encoding', ['wkb', 'geometry'])
@pytest.mark.parametrize('bbox', [(1, 2, 1, 2), (1, 0, 1, 4)])
def test_bbox_includes_degenerate_rectangle_contacts(
    tmp_path: Path, encoding: str, bbox: tuple[float, ...]
) -> None:
    path = tmp_path / 'degenerate.parquet'
    gm.points([1, 2], [2, 3]).to_geoparquet(path, encoding=encoding)
    restored = gm.from_geoparquet(path, bbox=bbox, columns=[])
    assert restored.geometries.to_wkt() == ['POINT (1 2)']
    assert len(restored) == 1


@pytest.mark.parametrize('encoding', ['wkb', 'geometry'])
def test_geographic_bbox_keeps_seam_and_pole_aliases(
    tmp_path: Path, encoding: str
) -> None:
    path = tmp_path / 'geographic.parquet'
    gm.GeometryArray([
        gm.LineString([(179, 10), (-179, 10)], crs=4326),
        gm.Point(360, 90, crs=4326),
        gm.Point(-20, -40, crs=4326),
    ]).to_geoparquet(
        path, encoding=encoding, row_group_size=1, attributes={'id': [59, 61, 67]}
    )
    seam = gm.from_geoparquet(path, bbox=(179.5, 9, 180, 11))
    assert len(seam) == 1
    assert list(seam.column('id')) == [59]
    pole = gm.from_geoparquet(path, bbox=(-80, 89, -70, 90))
    assert len(pole) == 1
    assert gm.intersects(pole.geometries, gm.Point(0, 90, crs=4326)).tolist() == [True]
    assert pole.geometries.crs == gm.CRS(4326)
    assert list(pole.column('id')) == [61]


@pytest.mark.parametrize('encoding', ['wkb', 'geometry'])
@pytest.mark.parametrize(
    ('latitude', 'bbox'),
    [
        (math.nextafter(90, math.inf), (0, 89, 1, 90)),
        (math.nextafter(-90, -math.inf), (0, -90, 1, -89)),
    ],
)
def test_bbox_keeps_admitted_exterior_pole_neighbors(
    tmp_path: Path, encoding: str, latitude: float, bbox: tuple[float, ...]
) -> None:
    path = tmp_path / 'pole-neighbor.parquet'
    source = gm.GeometryArray([gm.Point(73, latitude, crs=4326)])
    source.to_geoparquet(path, encoding=encoding, attributes={'id': [101]})
    features = gm.from_geoparquet(path, bbox=bbox)
    restored = features.geometries
    assert len(restored) == 1
    assert gm.intersects(
        restored, gm.Point(0, math.copysign(90, latitude), crs=4326)
    ).tolist() == [True]
    assert restored.crs == source.crs
    assert list(features.column('id')) == [101]


@pytest.mark.parametrize('encoding', ['wkb', 'geometry'])
def test_bbox_retains_geographic_polar_faces_beyond_vertex_latitudes(
    tmp_path: Path, encoding: str
) -> None:
    path = tmp_path / 'polar-face.parquet'
    cap = gm.Polygon(
        [(-180, 80), (-90, 80), (0, 80), (90, 80), (180, 80), (-180, 80)],
        crs=4326,
    )
    gm.GeometryArray([cap]).to_geoparquet(
        path, encoding=encoding, attributes={'id': [97]}
    )
    features = gm.from_geoparquet(path, bbox=(10, 85, 20, 89))
    restored = features.geometries
    assert len(restored) == 1
    assert gm.intersects(restored, gm.Point(15, 87, crs=4326)).tolist() == [True]
    assert gm.intersects(restored, gm.Point(15, 0, crs=4326)).tolist() == [False]
    assert restored.crs == cap.crs
    assert list(features.column('id')) == [97]


@pytest.mark.parametrize('encoding', ['wkb', 'geometry'])
def test_wrapped_bbox_uses_automatic_geographic_box_split(
    tmp_path: Path, encoding: str
) -> None:
    path = tmp_path / 'wrapped.parquet'
    gm.points([179.75, -179.75, 0.0], [10.0, 10.0, 10.0], crs=4326).to_geoparquet(
        path,
        encoding=encoding,
        row_group_size=1,
        attributes={'id': [79, 83, 89]},
    )
    restored = gm.from_geoparquet(path, bbox=(179.5, 9, -179.5, 11))
    assert restored.geometries.to_wkt() == ['POINT (179.75 10)', 'POINT (-179.75 10)']
    assert list(restored.column('id')) == [79, 83]
