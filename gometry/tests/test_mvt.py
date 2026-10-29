"""Public MVT wire and local-layer contracts."""

import copy
import itertools
import math
import pickle

import gometry as gm
import numpy as np
import pytest

INDEPENDENT_TILE = bytes.fromhex(
    '1a570a0873746174696f6e73120d1204000001011801220309184412100800120400020103'
    '1801220409709c011a046e616d651a046f70656e22090a0743656e7472616c220238012206'
    '0a0457657374220238002880207802'
)


def test_independent_mvt_tile_keeps_missing_and_zero_ids() -> None:
    layer = gm.from_mvt(INDEPENDENT_TILE, layer='stations')
    assert isinstance(layer, gm.MVTLayer)
    assert layer.extent == 4096
    assert layer.version == 2
    assert layer.features.ids == (None, 0)
    assert layer.features.geometries.to_wkt() == ['POINT (12 34)', 'POINT (56 78)']
    assert layer.features[0].properties == {'name': 'Central', 'open': True}
    assert layer.features[1].properties == {'name': 'West', 'open': False}


def test_local_mvt_roundtrip_preserves_scalar_kinds_and_sparse_properties() -> None:
    features = gm.Features(
        [gm.Point(25, 17), gm.LineString([(2, 2), (2, 10), (10, 10)])],
        properties=[
            {'boolean': True, 'integer': 1, 'real': 1.0, 'negative_zero': -0.0},
            {'name': 'road', 'missing': None},
        ],
        ids=[0, None],
    )
    layer = gm.MVTLayer(features, extent=4096)
    decoded = gm.from_mvt(gm.to_mvt({'places': layer}), layer='places')
    assert decoded.features.ids == (0, None)
    assert decoded.features.geometries.to_wkt() == features.geometries.to_wkt()
    values = decoded.features[0].properties
    assert values['boolean'] is True
    assert type(values['integer']) is int
    assert type(values['real']) is float
    assert math.copysign(1.0, values['negative_zero']) == -1.0
    assert decoded.features[1].properties == {'name': 'road'}
    assert layer.features[1].properties == {'name': 'road', 'missing': None}


def test_located_decode_uses_explicit_custom_or_xyz_frame() -> None:
    source = gm.MVTLayer(gm.Features([gm.Point(2048, 2048)], ids=[0]))
    tile = gm.to_mvt({'center': source})
    local = gm.from_mvt(tile, layer='center')
    frame = gm.TileFrame((0, 0, 10, 10), crs=None)
    custom = local.to_features(frame=frame)
    assert custom.geometries.to_wkt() == ['POINT (5 5)']
    assert custom.geometries.crs is None
    assert custom.ids == (0,)
    world = gm.Tile(x=0, y=0, zoom=0)
    located = gm.from_mvt(tile, layer='center', tile=world)
    assert isinstance(located, gm.Features)
    assert located.geometries[0].x == 0 and located.geometries[0].y == 0
    assert located.geometries.crs == gm.CRS(3857)
    geographic = gm.from_mvt(tile, layer='center', tile=world, crs=4326)
    assert geographic.geometries.crs == gm.CRS(4326)
    assert geographic.geometries[0].x == 0 and geographic.geometries[0].y == 0
    with pytest.raises(gm.CRSError):
        local.to_features(frame=frame, crs=4326)


def test_one_shot_encoding_matches_explicit_preparation() -> None:
    features = gm.Features(
        [gm.Point(2, 3), gm.LineString([(1, 2), (7, 5)])],
        properties=[{'name': 'point'}, {'name': 'line'}],
        ids=[0, 3],
    )
    frame = gm.TileFrame((0, 0, 8, 8), crs=None)
    prepared = gm.mvt_prepare(features, frame=frame, extent=8, buffer=1)
    direct = gm.to_mvt(features, layer='items', frame=frame, extent=8, buffer=1)
    explicit = gm.to_mvt({'items': prepared})
    direct_features = gm.from_mvt(direct, layer='items').features
    explicit_features = gm.from_mvt(explicit, layer='items').features
    assert direct_features.ids == explicit_features.ids
    assert direct_features.properties == explicit_features.properties
    assert direct_features.geometries.to_wkt() == explicit_features.geometries.to_wkt()


def test_local_layer_rejects_fractional_geometry_and_invalid_ids() -> None:
    with pytest.raises(gm.GeometryError):
        gm.MVTLayer(gm.Features([gm.Point(0.5, 2)]))
    with pytest.raises((TypeError, gm.GeometryError)):
        gm.MVTLayer(gm.Features([gm.Point(1, 2)], ids=['station']))


def test_local_layer_is_immutable_and_replaceable() -> None:
    layer = gm.MVTLayer(gm.Features([gm.Point(1, 2)]))
    with pytest.raises(AttributeError):
        del layer._features
    assert copy.copy(layer) is layer
    assert copy.deepcopy(layer) is layer
    replacement = layer.__replace__(extent=8192)
    assert replacement.extent == 8192
    assert replacement.features is layer.features
    assert layer.extent == 4096
    for invalid in (True, np.bool_(True), 0, -1, 1 << 32):
        with pytest.raises((TypeError, ValueError)):
            gm.MVTLayer(layer.features, extent=invalid)


def test_tile_frame_validates_and_preserves_its_coordinate_frame() -> None:
    frame = gm.TileFrame((1, 2, 5, 8), crs=4326, epoch=2020.5)
    assert frame.bounds == (1, 2, 5, 8)
    assert frame.crs == gm.CRS(4326)
    assert frame.epoch == 2020.5
    assert frame.__replace__(bounds=(0, 0, 1, 1)).crs == frame.crs
    assert copy.copy(frame) is frame
    with pytest.raises(AttributeError):
        del frame._bounds
    with pytest.raises(ValueError, match='four coordinates'):
        gm.TileFrame(itertools.count(), crs=None)
    with pytest.raises(ValueError, match='positive width'):
        gm.TileFrame((1, 2, 1, 4), crs=None)
    with pytest.raises(gm.CRSError):
        gm.TileFrame((0, 0, 1, 1), crs=None, epoch=2020.5)


def test_query_regions_include_grid_halo_and_split_world_wrap() -> None:
    frame = gm.TileFrame((0, 0, 9, 9), crs=None)
    assert gm.mvt_query_regions(frame=frame, extent=9, buffer=0)[0].bounds == (
        -1,
        -1,
        10,
        10,
    )
    world = math.pi * 6_378_137
    tile = gm.Tile(x=0, y=0, zoom=1)
    regions = gm.mvt_query_regions(tile=tile, extent=16, buffer=0)
    assert len(regions) == 2
    assert regions[0].bounds[0] == -world
    assert regions[1].bounds[2] == world
    assert all(region.crs == gm.CRS(3857) for region in regions)
    assert regions[0].bounds[3] > world
    whole = gm.mvt_query_regions(tile=tile, extent=16, buffer=1 << 100)
    assert len(whole) == 1
    assert whole[0].bounds[0] == -world and whole[0].bounds[2] == world


def test_decode_layer_and_property_selection_preserve_wire_order() -> None:
    selected = gm.from_mvt(INDEPENDENT_TILE, layers=['stations'], properties=['name'])
    assert list(selected) == ['stations']
    assert selected['stations'].features[0].properties == {'name': 'Central'}
    with pytest.raises(KeyError):
        gm.from_mvt(INDEPENDENT_TILE, layer='missing')


def test_selected_decode_skips_unrequested_layer_contents_and_versions() -> None:
    malformed_unselected_layer = b'\x1a\x0a\x0a\x03bad\x12\x01\xff\x78\x01'
    tile = INDEPENDENT_TILE + malformed_unselected_layer
    selected = gm.from_mvt(tile, layer='stations', properties=[])
    assert selected.features.properties == ({}, {})
    with pytest.raises(gm.ParseError):
        gm.from_mvt(tile)


def test_decoded_sparse_properties_support_selection_edits_and_pickle() -> None:
    source = gm.from_mvt(INDEPENDENT_TILE, layer='stations').features
    edited = source[[1, 0]].with_columns({'name': ['changed', 'kept'], 'rank': [1, 2]})
    assert edited[0].properties == {'name': 'changed', 'open': False, 'rank': 1}
    assert edited.select_columns(['rank'])[1].properties == {'rank': 2}
    assert pickle.loads(pickle.dumps(edited)).properties == edited.properties
    assert source[0].properties == {'name': 'Central', 'open': True}
    restored = gm.from_mvt(
        gm.to_mvt({'stations': gm.MVTLayer(edited)}), layer='stations'
    )
    assert restored.features.properties == edited.properties


def test_layer_checks_attributes_and_ids_even_for_missing_geometry() -> None:
    with pytest.raises(TypeError, match='field'):
        gm.MVTLayer(gm.Features([None], properties=[{'unsupported': [1]}]))
    with pytest.raises(TypeError, match='ID'):
        gm.MVTLayer(gm.Features([None], ids=['not-an-integer']))


def test_local_layer_omits_missing_and_empty_geometry_rows() -> None:
    source = gm.Features(
        [None, gm.from_wkt('POINT EMPTY'), gm.Point(2, 3)],
        properties=[{'name': 'missing'}, {'name': 'empty'}, {'name': 'kept'}],
        ids=[1, 2, 3],
    )
    result = gm.MVTLayer(source)
    assert result.features.ids == (3,)
    assert result.features[0].properties == {'name': 'kept'}
    assert len(source) == 3


def test_layer_rejects_non_utf8_property_strings_with_field_context() -> None:
    for properties in ({'label': '\ud800'}, {'\ud800': 'value'}):
        with pytest.raises(TypeError, match='row 0'):
            gm.MVTLayer(gm.Features([gm.Point(1, 2)], properties=[properties]))


def test_float_special_values_are_not_treated_as_missing() -> None:
    layer = gm.MVTLayer(
        gm.Features(
            [gm.Point(1, 2)],
            properties=[{'nan': math.nan, 'positive': math.inf, 'negative': -math.inf}],
        )
    )
    values = (
        gm.from_mvt(gm.to_mvt({'values': layer}), layer='values').features[0].properties
    )
    assert math.isnan(values['nan'])
    assert values['positive'] == math.inf
    assert values['negative'] == -math.inf


def test_integer_values_and_ids_keep_full_wire_range() -> None:
    source = gm.Features(
        [gm.Point(1, 2)],
        properties=[{'signed': -(1 << 63), 'unsigned': (1 << 64) - 1}],
        ids=[(1 << 64) - 1],
    )
    result = gm.from_mvt(gm.to_mvt({'values': gm.MVTLayer(source)}), layer='values')
    assert result.features.ids == ((1 << 64) - 1,)
    assert result.features[0].properties == {
        'signed': -(1 << 63),
        'unsigned': (1 << 64) - 1,
    }
    with pytest.raises(TypeError, match='range'):
        gm.MVTLayer(gm.Features([gm.Point(1, 2)], ids=[1 << 64]))


def test_prepare_routes_hole_near_diagonal_onto_integer_grid() -> None:
    geometry = gm.from_wkt('POLYGON ((0 9,9 9,9 6,0 9),(3 8.5,4.6 7.49,6 8.5,3 8.5))')
    rows = gm.Features([geometry], properties=[{'name': 'building'}], ids=[7])
    frame = gm.TileFrame((0, 0, 9, 9), crs=None)
    result = gm.mvt_prepare(rows, frame=frame, extent=9, buffer=0)
    assert result.features.ids == (7,)
    assert result.features[0].properties == {'name': 'building'}
    shape = result.features.geometries[0]
    assert shape.is_valid
    assert shape.area == 13
    for x, y in shape.coords:
        assert x == round(x)
        assert y == round(y)


def test_adjacent_frames_keep_a_shared_line_boundary_anchor() -> None:
    rows = gm.Features(
        [gm.LineString([(12, 13.6), (20, 11.2)])],
        properties=[{'name': 'road'}],
    )
    west = gm.mvt_prepare(
        rows, frame=gm.TileFrame((0, 0, 16, 16), crs=None), extent=16, buffer=2
    )
    east = gm.mvt_prepare(
        rows, frame=gm.TileFrame((16, 0, 32, 16), crs=None), extent=16, buffer=2
    )
    west_edge = gm.LineString([(16, -2), (16, 18)])
    east_edge = gm.LineString([(0, -2), (0, 18)])
    assert (
        gm.intersection(west.features.geometries[0], west_edge).to_wkt()
        == 'POINT (16 4)'
    )
    assert (
        gm.intersection(east.features.geometries[0], east_edge).to_wkt()
        == 'POINT (0 4)'
    )
