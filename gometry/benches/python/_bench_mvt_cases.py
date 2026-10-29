"""MVT comparisons over indexed source and tile-local features."""

from __future__ import annotations

import time
from dataclasses import dataclass
from functools import lru_cache, partial
from typing import Any


from _bench_mvt_workload import (
    BUFFER,
    EXTENT,
    FRACTION,
    MIXED,
    POINTS,
    TILE_X,
    TILE_Y,
    UNIT,
    Workload,
    X_STEP,
    Y_STEP,
    ZOOM,
)
from _bench_oracles import MVT_CASE_BUILDERS, OracleContext, OracleMismatch, PairCase
from _bench_postgis import PostGIS
from _bench_public_cases import PublicCase


@dataclass(frozen=True, slots=True)
class _Row:
    id: int
    x: int
    y: int
    rank: int
    kind: str

    def geometry(self) -> dict[str, Any]:
        if self.kind == 'place':
            return {'type': 'Point', 'coordinates': (self.x, self.y)}
        if self.kind == 'road':
            return {
                'type': 'LineString',
                'coordinates': ((self.x, self.y), (self.x + 37, self.y + 13)),
            }
        return {
            'type': 'Polygon',
            'coordinates': (
                (
                    (self.x, self.y),
                    (self.x + 16, self.y),
                    (self.x + 16, self.y + 11),
                    (self.x, self.y + 11),
                    (self.x, self.y),
                ),
            ),
        }


@dataclass(frozen=True, slots=True)
class _Fixture:
    workload: Workload
    tile: Any
    local: Any
    mapbox_layer: dict[str, Any]
    rows: tuple[_Row, ...]
    expected: tuple[_Row, ...]


def _kind(index: int, workload: Workload) -> str:
    if index < workload.points:
        return 'place'
    if index < workload.points + workload.lines:
        return 'road'
    return 'building'


@lru_cache(maxsize=2)
def _fixture(workload: Workload) -> _Fixture:
    import gometry as gm
    from shapely.geometry import shape

    rows = tuple(
        _Row(
            id=index + 1,
            x=index * X_STEP % workload.period
            + workload.offset
            + (index % workload.select_every != 0) * workload.alternate_x,
            y=index * Y_STEP % workload.period + workload.offset,
            rank=index % 11,
            kind=_kind(index, workload),
        )
        for index in range(workload.rows)
    )
    expected = tuple(
        row
        for row in rows
        if -BUFFER <= row.x <= EXTENT + BUFFER and -BUFFER <= row.y <= EXTENT + BUFFER
    )
    expected_count = 1331 if workload == POINTS else 1750
    if len(expected) != expected_count:
        raise AssertionError('MVT fixture selection changed')
    local_shapes = [shape(row.geometry()) for row in expected]
    local_geometries = gm.from_wkt(value.wkt for value in local_shapes)
    local_features = gm.Features.from_columns(
        local_geometries,
        columns={
            'class': [row.kind for row in expected],
            'rank': [row.rank for row in expected],
        },
        ids=[row.id for row in expected],
    )
    local = gm.MVTLayer(local_features, extent=EXTENT)
    mapbox_layer = {
        'name': workload.layer,
        'features': [
            {
                'id': row.id,
                'geometry': geometry,
                'properties': {'class': row.kind, 'rank': row.rank},
            }
            for row, geometry in zip(expected, local_shapes, strict=True)
        ],
    }
    return _Fixture(
        workload=workload,
        tile=gm.Tile(x=TILE_X, y=TILE_Y, zoom=ZOOM),
        local=local,
        mapbox_layer=mapbox_layer,
        rows=rows,
        expected=expected,
    )


def _check_rows(features: Any, expected: tuple[_Row, ...]) -> None:
    from shapely.geometry import shape

    if len(features) != len(expected):
        raise OracleMismatch('MVT feature count differs from the independent fixture')
    for feature, row in zip(features, expected, strict=True):
        if feature['id'] != row.id or feature['properties'] != {
            'class': row.kind,
            'rank': row.rank,
        }:
            raise OracleMismatch(f'MVT feature {row.id} has different ID or properties')
        actual = feature['geometry']
        wanted = row.geometry()
        if actual['type'] != wanted['type']:
            raise OracleMismatch(f'MVT feature {row.id} has a different geometry type')
        if row.kind == 'place':
            equal = tuple(actual['coordinates']) == wanted['coordinates']
        elif row.kind == 'road':
            equal = tuple(map(tuple, actual['coordinates'])) == wanted['coordinates']
        else:
            equal = shape(actual).equals(shape(wanted))
        if not equal:
            raise OracleMismatch(f'MVT feature {row.id} has different tile geometry')


def _decoded_rows(value: Any, fixture: _Fixture) -> Any:
    layer = value[fixture.workload.layer]
    if layer['version'] != 2 or layer['extent'] != EXTENT:
        raise OracleMismatch('MVT layer version or extent differs')
    return layer['features']


def _check_bytes(
    left: object, right: object | None, context: OracleContext, *, fixture: _Fixture
) -> None:
    import mapbox_vector_tile

    if not isinstance(left, bytes) or not isinstance(right, bytes):
        raise OracleMismatch('MVT encoders must both return bytes')
    for value in (left, right):
        decoded = mapbox_vector_tile.decode(
            value, default_options={'y_coord_down': True}
        )
        _check_rows(_decoded_rows(decoded, fixture), fixture.expected)


def _check_decode(
    left: object, right: object | None, context: OracleContext, *, fixture: _Fixture
) -> None:
    import gometry as gm
    from shapely import from_wkb

    if not isinstance(left, gm.MVTLayer) or not isinstance(right, dict):
        raise OracleMismatch('MVT decoders returned unexpected results')
    rows = [
        {
            'id': feature.id,
            'geometry': from_wkb(feature.geometry.to_wkb()).__geo_interface__,
            'properties': dict(feature.properties),
        }
        for feature in left.features
    ]
    _check_rows(rows, fixture.expected)
    _check_rows(_decoded_rows(right, fixture), fixture.expected)


def _bytes(value: object) -> None:
    if not isinstance(value, bytes) or not value:
        raise ValueError('expected nonempty MVT bytes')


def _source(fixture: _Fixture) -> Any:
    import gometry as gm

    rows = fixture.rows
    if fixture.workload == POINTS:
        geometries = gm.points(
            [(row.x + FRACTION) * UNIT for row in rows],
            [-(row.y + FRACTION) * UNIT for row in rows],
            crs=3857,
        )
    else:

        def wkt(row: _Row) -> str:
            geometry = row.geometry()
            coords = geometry['coordinates']
            if row.kind == 'place':
                coords = (coords,)
            elif row.kind == 'building':
                coords = coords[0]
            text = ','.join(
                f'{(x + FRACTION) * UNIT:.17g} {-(y + FRACTION) * UNIT:.17g}'
                for x, y in coords
            )
            if row.kind == 'building':
                return f'POLYGON (({text}))'
            name = 'POINT' if row.kind == 'place' else 'LINESTRING'
            return f'{name} ({text})'

        geometries = gm.from_wkt((wkt(row) for row in rows), crs=3857)
    features = gm.Features.from_columns(
        geometries,
        columns={
            'class': [row.kind for row in rows],
            'rank': [row.rank for row in rows],
        },
        ids=[row.id for row in rows],
    )
    return gm.MVTSource(features, extent=EXTENT, buffer=BUFFER, tolerance=0)


def _postgis_case(workload: Workload) -> PublicCase:
    import gometry as gm

    fixture = _fixture(workload)
    started = time.perf_counter()
    source = _source(fixture)
    source_setup_seconds = time.perf_counter() - started
    database = PostGIS(workload)
    return PublicCase(
        pair=PairCase(
            gometry_call=lambda: gm.to_mvt(
                source, layer=workload.layer, tile=fixture.tile
            ),
            competitor_call=database.tile,
            compare=partial(_check_bytes, fixture=fixture),
        ),
        kind='mvt-bytes',
        useful_work={'count': len(fixture.expected), 'unit': 'encoded-features'},
        output_validator=_bytes,
        close=database.close,
        fixture_metadata={
            'source_rows': workload.rows,
            'selected_rows': len(fixture.expected),
            'extent': EXTENT,
            'buffer': BUFFER,
            'source_setup_seconds': source_setup_seconds,
            'postgis': database.metadata,
        },
    )


def _encode_case(workload: Workload) -> PublicCase:
    import gometry as gm
    import mapbox_vector_tile

    fixture = _fixture(workload)
    return PublicCase(
        pair=PairCase(
            gometry_call=lambda: gm.to_mvt(fixture.local, layer=workload.layer),
            competitor_call=lambda: mapbox_vector_tile.encode(
                fixture.mapbox_layer,
                default_options={'y_coord_down': True, 'extents': EXTENT},
            ),
            compare=partial(_check_bytes, fixture=fixture),
        ),
        kind='mvt-bytes',
        useful_work={'count': len(fixture.expected), 'unit': 'encoded-features'},
        output_validator=_bytes,
        fixture_metadata={
            'input': 'prepared integer geometries with IDs and scalar properties',
            'gometry_input': 'immutable MVTLayer',
            'competitor_input': 'preconstructed Shapely geometries and Python feature mappings',
        },
    )


def _decode_case(workload: Workload) -> PublicCase:
    import gometry as gm
    import mapbox_vector_tile

    fixture = _fixture(workload)
    payload = gm.to_mvt(fixture.local, layer=workload.layer)
    return PublicCase(
        pair=PairCase(
            gometry_call=lambda: gm.from_mvt(payload, layer=workload.layer),
            competitor_call=lambda: mapbox_vector_tile.decode(
                payload, default_options={'y_coord_down': True}
            ),
            compare=partial(_check_decode, fixture=fixture),
        ),
        kind='mvt-features',
        useful_work={'count': len(fixture.expected), 'unit': 'decoded-features'},
        fixture_metadata={
            'input_bytes': len(payload),
            'gometry_output': 'immutable MVTLayer',
            'competitor_output': 'Python feature mappings',
        },
    )


MVT_CASE_BUILDERS.update({
    'gometry.to_mvt.source/5k_points_xyz': partial(_postgis_case, POINTS),
    'gometry.to_mvt.local/points': partial(_encode_case, POINTS),
    'gometry.from_mvt.local/points': partial(_decode_case, POINTS),
    'gometry.to_mvt.source/7k_mixed_xyz': partial(_postgis_case, MIXED),
    'gometry.to_mvt.local/mixed': partial(_encode_case, MIXED),
    'gometry.from_mvt.local/mixed': partial(_decode_case, MIXED),
})
