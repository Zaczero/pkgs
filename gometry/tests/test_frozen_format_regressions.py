"""Regression coverage for v1 on-disk and cross-process formats."""

from __future__ import annotations

import pickle

import gometry as gm
import pytest

# Protocol-4 reader fixtures for the intended first public wire format.
# Revise these with an intentional format change while the package is unreleased.
_V1_PICKLES = {
    'point': (
        '8004956b000000000000008c0c676f6d657472792e5f6c6962948c155f756e7069636b6c65'
        '5f67656f6d657472795f7631949394432501b90b0000000000000000f03f0000000000000040'
        '00000000000008400000000000001040948c09455053473a343332369447409f920000000000'
        '879452942e'
    ),
    'points': (
        '8004959b000000000000008c0c676f6d657472792e5f6c6962948c195f756e7069636b6c65'
        '5f7061636b65645f61727261795f7631949394284310000000000000f03f0000000000001440'
        '9443100000000000000040000000000000184094431000000000000008400000000000001c40'
        '9443100000000000001040000000000000204094298c09455053473a343332369447409f9500'
        '00000000430300010094749452942e'
    ),
    'lines': (
        '8004956f000000000000008c0c676f6d657472792e5f6c6962948c195f756e7069636b6c65'
        '5f7061636b65645f61727261795f7631949394284310000000000000f03f0000000000000840'
        '94431000000000000000400000000000001040944e4e430800000000020000009485944e4e'
        '4302000194749452942e'
    ),
    'polygons': (
        '8004959a000000000000008c0c676f6d657472792e5f6c6962948c195f756e7069636b6c65'
        '5f7061636b65645f61727261795f7631949394284320000000000000f03f0000000000000840'
        '0000000000000840000000000000f03f944320000000000000004000000000000000400000'
        '0000000010400000000000000040944e4e430800000000040000009443080000000001000000'
        '9486944e4e4302000194749452942e'
    ),
    'mixed': (
        '80049588000000000000008c0c676f6d657472792e5f6c6962948c1b5f756e7069636b6c65'
        '5f67656f6d657472795f61727261795f7631949394285d942843150101000000000000000000'
        'f03f000000000000004094432901020000000200000000000000000008400000000000001040'
        '0000000000001440000000000000184094430094654e4e430300000194749452942e'
    ),
    'groups': (
        '80049548000000000000008c0c676f6d657472792e5f6c6962948c195f756e7069636b6c65'
        '5f696e7436345f67726f7570735f76319493945d94284b074b0b4b0d655d94284b004b024b02'
        '4b0365869452942e'
    ),
    'crs': (
        '80049535000000000000008c0c676f6d657472792e5f6c6962948c105f756e7069636b6c65'
        '5f6372735f76319493948c09455053473a3338353794859452942e'
    ),
    'cell': (
        '80049539000000000000008c0c676f6d657472792e5f6c6962948c195f756e7069636b6c65'
        '5f67656f686173685f63656c6c5f76319493948c047533336494859452942e'
    ),
    'cells': (
        '80049549000000000000008c0c676f6d657472792e5f6c6962948c175f756e7069636b6c65'
        '5f63656c6c5f61727261795f76319493945d948c047533336494618c0767656f686173689443'
        '02000194879452942e'
    ),
    'index': (
        '8004958e000000000000008c0c676f6d657472792e5f6c6962948c1a5f756e7069636b6c65'
        '5f7370617469616c5f696e6465785f76319493942868008c195f756e7069636b6c655f7061636b'
        '65645f61727261795f7631949394284310000000000000f03f0000000000001440944310000000'
        '00000000400000000000001840944e4e294e4e4e749452944b004b0286944b0388749452942e'
    ),
}


def test_v1_geometry_pickle_preserves_frame_and_ordinates() -> None:
    point = pickle.loads(bytes.fromhex(_V1_PICKLES['point']))
    assert type(point) is gm.Point
    assert (point.x, point.y, point.z, point.m) == (1, 2, 3, 4)
    assert point.crs == gm.CRS(4326)
    assert point.epoch == 2020.5


@pytest.mark.parametrize(
    ('kind', 'expected'),
    [
        ('points', ['POINT ZM (1 2 3 4)', None, 'POINT ZM (5 6 7 8)']),
        ('lines', ['LINESTRING (1 2, 3 4)', None]),
        ('polygons', ['POLYGON ((1 2, 3 2, 3 4, 1 2))', None]),
        ('mixed', ['POINT (1 2)', 'LINESTRING (3 4, 5 6)', None]),
    ],
)
def test_v1_array_pickles_preserve_rows(kind: str, expected: list) -> None:
    values = pickle.loads(bytes.fromhex(_V1_PICKLES[kind]))
    assert values.to_wkt(drop_epoch=True) == expected
    if kind == 'points':
        assert values.crs == gm.CRS(4326)
        assert values.epoch == 2021.25
    else:
        assert values.crs is None
        assert values.epoch is None


def test_v1_groups_pickle_preserves_empty_groups() -> None:
    values = pickle.loads(bytes.fromhex(_V1_PICKLES['groups']))
    assert [row.tolist() for row in values] == [[7, 11], [], [13]]
    assert values.is_missing.tolist() == [False, False, False]


def test_v1_crs_and_cell_pickles_preserve_identity() -> None:
    crs = pickle.loads(bytes.fromhex(_V1_PICKLES['crs']))
    cell = pickle.loads(bytes.fromhex(_V1_PICKLES['cell']))
    cells = pickle.loads(bytes.fromhex(_V1_PICKLES['cells']))
    assert crs.definition == 'EPSG:3857'
    assert cell.token == 'u33d'
    assert cells.token == ['u33d', None]
    assert cells.is_missing.tolist() == [False, True]


def test_v1_index_pickle_preserves_removed_handles_and_next_handle() -> None:
    index = pickle.loads(bytes.fromhex(_V1_PICKLES['index']))
    assert list(index) == [0, 2]
    assert index[0] == gm.Point(1, 2)
    assert index[2] == gm.Point(5, 6)
    with pytest.raises(KeyError):
        index[1]
    assert index.insert(gm.Point(7, 8)) == 3
    assert index.query(gm.Point(5, 6), predicate='intersects').tolist() == [2]


def test_unknown_pickle_version_is_not_decoded_as_v1() -> None:
    payload = bytes.fromhex(_V1_PICKLES['point']).replace(b'_v1', b'_v9')
    with pytest.raises(AttributeError, match='_v9'):
        pickle.loads(payload)


def test_nullable_homogeneous_ingress_stays_geoarrow_native() -> None:
    """D2: missing rows scatter after, rather than before, packed inference."""
    line = gm.LineString([(0, 0), (1, 1)])
    geojson_line = {'type': 'LineString', 'coordinates': [[0, 0], [1, 1]]}
    feature_collection = {
        'type': 'FeatureCollection',
        'features': [
            {'type': 'Feature', 'geometry': geojson_line, 'properties': {}},
            {'type': 'Feature', 'geometry': None, 'properties': {}},
        ],
    }
    arrays = (
        gm.GeometryArray([line, None]),
        gm.from_wkt([line.to_wkt(), None]),
        gm.from_wkb([line.to_wkb(), None]),
        gm.from_geojson([geojson_line, None]),
        gm.from_geojson(feature_collection),
        gm.from_geojson(feature_collection, features=True).geometries,
    )
    for values in arrays:
        assert values.is_missing.tolist() == [False, True]
        assert values.to_arrow().type.extension_name == 'geoarrow.linestring'
