"""pandas extension storage and explicit conversion interop."""

from __future__ import annotations

import importlib

import gometry as gm
import numpy as np
import pytest

from tests._support import canon

pd = pytest.importorskip('pandas', reason='pandas is an optional interop dependency')
from_pandas = importlib.import_module('gometry._from_pandas').from_pandas
_pandas = importlib.import_module('gometry._pandas')
GeometryDtype = _pandas.GeometryDtype
GeometryExtensionArray = _pandas.GeometryExtensionArray
to_pandas = _pandas.to_pandas


@pytest.fixture
def points() -> gm.GeometryArray:
    return gm.GeometryArray([
        gm.Point(0, 0, crs=4326),
        gm.box(0, 0, 1, 1, crs=4326),
        gm.Point(2, 3, crs=4326),
    ])


def test_geometry_dtype_is_a_concrete_unregistered_type() -> None:
    assert isinstance(GeometryDtype(), pd.api.extensions.ExtensionDtype)
    assert GeometryDtype.name == 'gometry.geometry'
    assert GeometryDtype.type is gm.Geometry
    with pytest.raises(TypeError):
        pd.api.types.pandas_dtype('gometry.geometry')


def test_geometry_dtype_from_arrow_roundtrip(points: gm.GeometryArray) -> None:
    pa = pytest.importorskip('pyarrow')
    source = to_pandas(points, name='geometry')
    table = pa.Table.from_pandas(source.to_frame())
    restored = GeometryDtype().__from_arrow__(table['geometry'])

    assert canon(restored.geometry_array) == canon(points)
    assert restored.geometry_array.crs.same_as(points.crs, mode='exact')

    arrow = points.to_arrow()
    chunked = pa.chunked_array([arrow.slice(0, 1), arrow.slice(1)])
    rebuilt = GeometryDtype().__from_arrow__(chunked)
    assert canon(rebuilt.geometry_array) == canon(points)


def test_to_pandas(points: gm.GeometryArray) -> None:
    series = to_pandas(points)
    assert isinstance(series.dtype, GeometryDtype)
    assert len(series) == 3
    assert isinstance(series.array, GeometryExtensionArray)


def test_extension_assignment_views_and_copies(points: gm.GeometryArray) -> None:
    array = GeometryExtensionArray(points)
    view = array.view()
    copied = array.copy()

    replacement = gm.Point(9, 9, crs=4326)
    array[0] = replacement
    assert view[0] == replacement
    assert copied[0] == points[0]

    array[[1, 2]] = [None, gm.Point(8, 8, crs=4326)]
    assert view[1] is None
    assert view[2] == gm.Point(8, 8, crs=4326)
    assert copied.geometry_array.to_wkt() == points.to_wkt()


def test_extension_array_view_rejects_dtype_argument(
    points: gm.GeometryArray,
) -> None:
    array = GeometryExtensionArray(points)

    with pytest.raises(NotImplementedError) as error:
        array.view(dtype=object)

    assert str(error.value) == 'GeometryExtensionArray.view supports only dtype=None'


def test_extension_array_pickle_preserves_view_state_aliasing(
    points: gm.GeometryArray,
) -> None:
    import pickle

    values = GeometryExtensionArray(points)
    view = values.view()
    restored, restored_view = pickle.loads(pickle.dumps((values, view)))
    assert restored._state is restored_view._state
    replacement = gm.GeometryArray([gm.Point(9, 9)] * len(points))
    restored._geoms = replacement
    assert canon(restored_view.geometry_array) == canon(replacement)


def test_extension_array_pickle_preserves_readonly(points: gm.GeometryArray) -> None:
    import pickle

    values = GeometryExtensionArray(points)
    values._readonly = True
    restored = pickle.loads(pickle.dumps(values))
    with pytest.raises(ValueError, match='read-only'):
        restored[0] = gm.Point(9, 8, crs=4326)


def test_geometry_array_to_pandas_series_and_frame(points: gm.GeometryArray) -> None:
    series = points.to_pandas(name='geom')
    assert isinstance(series.dtype, GeometryDtype)
    assert series.name == 'geom'
    frame = points.to_pandas().to_frame(name='geometry')
    assert isinstance(frame.dtypes['geometry'], GeometryDtype)
    assert canon(from_pandas(frame['geometry'])) == canon(points)


def test_geometry_array_roundtrip(points: gm.GeometryArray) -> None:
    series = to_pandas(points)
    restored = from_pandas(series)
    assert restored is points
    assert canon(restored) == canon(points)
    assert restored.crs == points.crs


def test_from_pandas_imports_object_geometry_rows_and_missing_values() -> None:
    class GeoInterfacePoint:
        @property
        def __geo_interface__(self) -> dict[str, object]:
            return {'type': 'Point', 'coordinates': [7, 8]}

    series = pd.Series(
        [
            gm.Point(1, 2),
            pd.NA,
            gm.Point(3, 4).to_wkb(),
            {'type': 'Point', 'coordinates': [5, 6]},
            GeoInterfacePoint(),
            np.nan,
        ],
        dtype=object,
    )

    restored = from_pandas(series)

    assert restored.to_wkt() == [
        'POINT (1 2)',
        None,
        'POINT (3 4)',
        'POINT (5 6)',
        'POINT (7 8)',
        None,
    ]


def test_from_pandas_keeps_geopandas_crs_on_the_metadata_aware_path() -> None:
    geopandas = pytest.importorskip('geopandas')
    series = geopandas.GeoSeries([gm.Point(1, 2)], crs=4326)

    with pytest.raises(TypeError, match='use from_geopandas'):
        from_pandas(series)


def test_format_readers_normalize_nullable_pandas_columns() -> None:
    point = gm.Point(1, 2, crs=4326)
    wkts = pd.Series(['POINT (1 2)', pd.NA], dtype='string')
    wkbs = pd.Series([point.to_wkb(), pd.NA], dtype=object)
    geojson = pd.Series(
        [{'type': 'Point', 'coordinates': [1, 2]}, pd.NA],
        dtype=object,
    )
    polylines = pd.Series([point.to_polyline(), pd.NA], dtype='string')

    assert gm.from_wkt(wkts).to_wkt() == ['POINT (1 2)', None]
    assert gm.from_wkb(wkbs).to_wkt() == ['POINT (1 2)', None]
    assert gm.from_geojson(geojson).to_wkt() == ['POINT (1 2)', None]
    assert gm.from_polyline(polylines).to_wkt() == ['POINT (1 2)', None]


def test_extension_array_getitem_slice_take(points: gm.GeometryArray) -> None:
    data = GeometryExtensionArray(points)
    sliced = data[1:]
    assert len(sliced) == 2
    assert isinstance(sliced, GeometryExtensionArray)

    taken = data.take([2, 0])
    assert len(taken) == 2
    assert canon(taken.geometry_array) == canon(points[[2, 0]])


def test_extension_array_take_allow_fill_fills_missing(
    points: gm.GeometryArray,
) -> None:
    data = GeometryExtensionArray(points)
    taken = data.take([0, -1, 2], allow_fill=True)
    assert len(taken) == 3
    # default fill is MISSING (reindex/merge semantics), not an empty geometry
    assert taken.isna().tolist() == [False, True, False]
    assert taken[1] is None
    assert canon(taken.geometry_array[[0, 2]]) == canon(points[[0, 2]])


def test_extension_array_take_allow_fill_merges_existing_missing_mask(
    points: gm.GeometryArray,
) -> None:
    source = points._with_missing(np.array([False, True, False]))
    data = GeometryExtensionArray(source)

    taken = data.take([1, -1, 0], allow_fill=True)

    assert taken.isna().tolist() == [True, True, False]
    assert taken._geoms.to_wkb() == [None, None, source.to_wkb()[0]]
    assert taken._geoms.crs == source.crs
    assert taken._geoms.epoch == source.epoch


def test_extension_array_take_allow_fill_canonicalizes_missing_ordinates() -> None:
    """NA fill cannot retain the substituted row's packed Z/M ordinates."""
    hidden = gm.LineString([(0, 0, 100, 1000), (1, 1, 200, 2000)])
    visible = gm.LineString([(2, 2, 3, 30), (3, 3, 4, 40)])
    source = GeometryExtensionArray(gm.GeometryArray([hidden, visible]))

    taken = source.take([-1, 1], allow_fill=True).geometry_array
    direct = gm.GeometryArray([None, visible])

    assert np.isnan(taken.min_z[0])
    assert np.isnan(taken.max_z[0])
    assert np.isnan(taken.min_m[0])
    assert np.isnan(taken.max_m[0])
    assert taken.min_z.tolist()[1:] == [3.0]
    assert taken.max_z.tolist()[1:] == [4.0]
    assert taken.min_m.tolist()[1:] == [30.0]
    assert taken.max_m.tolist()[1:] == [40.0]
    assert taken.__reduce_ex__(5)[0].__name__ == direct.__reduce_ex__(5)[0].__name__
    assert taken.to_arrow().type.extension_name == direct.to_arrow().type.extension_name


def test_pandas_formatter_accepts_coordinate_epoch() -> None:
    values = gm.GeometryArray([gm.Point(1, 2, crs=4326, epoch=2020.0)])
    assert 'POINT (1 2)' in str(values.to_pandas())


def test_extension_array_take_allow_fill_custom_fill(points: gm.GeometryArray) -> None:
    data = GeometryExtensionArray(points)
    fill = gm.from_wkt('POINT EMPTY', crs=4326)
    taken = data.take([-1, 1], allow_fill=True, fill_value=fill)
    filled = taken[0]
    assert filled is not None
    assert filled.is_empty
    assert filled.to_wkt() == 'POINT EMPTY'
    assert canon(taken[1]) == canon(points[1])


def test_extension_array_take_allow_fill_custom_fill_from_empty_source() -> None:
    source = gm.GeometryArray([], crs=4326).set_epoch(2020.0)
    data = GeometryExtensionArray(source)
    fill = gm.Point(9, 9, crs=4326, epoch=2020.0)

    taken = data.take([-1, -1], allow_fill=True, fill_value=fill)

    assert taken.isna().tolist() == [False, False]
    assert taken.geometry_array.to_wkt(drop_epoch=True) == [
        'POINT (9 9)',
        'POINT (9 9)',
    ]
    assert taken.geometry_array.crs == source.crs
    assert taken.geometry_array.epoch == source.epoch


def test_extension_array_take_allow_fill_custom_fill_scatter_preserves_rows() -> None:
    source = gm.GeometryArray([
        gm.Point(0, 0, crs=4326),
        None,
        gm.Point(2, 2, crs=4326),
    ])
    data = GeometryExtensionArray(source)
    fill = gm.Point(9, 9, crs=4326)

    taken = data.take([-1, 2, -1, 0], allow_fill=True, fill_value=fill)

    assert taken.isna().tolist() == [False, False, False, False]
    assert taken.geometry_array.to_wkt() == [
        'POINT (9 9)',
        'POINT (2 2)',
        'POINT (9 9)',
        'POINT (0 0)',
    ]
    assert source.to_wkt() == ['POINT (0 0)', None, 'POINT (2 2)']


def test_extension_array_take_allow_fill_missing_fill_stays_missing(
    points: gm.GeometryArray,
) -> None:
    source = points._with_missing(np.array([False, True, False]))
    data = GeometryExtensionArray(source)

    taken = data.take([-1, 1, 0], allow_fill=True, fill_value=None)

    assert taken.isna().tolist() == [True, True, False]
    assert taken[0] is None
    assert taken[1] is None
    assert canon(taken[2]) == canon(points[0])


def test_extension_array_take_allow_fill_rejects_bad_negatives(
    points: gm.GeometryArray,
) -> None:
    data = GeometryExtensionArray(points)
    # pandas contract: with allow_fill=True only -1 marks a fill slot.
    with pytest.raises(ValueError, match='only -1'):
        data.take([0, -2], allow_fill=True)


@pytest.mark.parametrize(
    'indices',
    [[3, -1], [0, 3]],
    ids=['out-of-bounds-before-fill', 'one-past-end'],
)
def test_extension_array_take_allow_fill_rejects_out_of_bounds_indices(
    points: gm.GeometryArray,
    indices: list[int],
) -> None:
    data = GeometryExtensionArray(points)

    with pytest.raises(IndexError, match='take index out of bounds'):
        data.take(indices, allow_fill=True)


def test_extension_array_take_preserves_view_and_copy_ownership(
    points: gm.GeometryArray,
) -> None:
    data = GeometryExtensionArray(points)
    view = data.view()
    copied = data.copy()
    fill = gm.Point(9, 9, crs=4326)

    taken = data.take([-1, 1], allow_fill=True, fill_value=fill)

    assert data._state is view._state
    assert data._state is not copied._state
    assert taken._state is not data._state
    assert view.geometry_array.to_wkt() == points.to_wkt()
    assert copied.geometry_array.to_wkt() == points.to_wkt()


def test_extension_array_factorize_roundtrip(points: gm.GeometryArray) -> None:
    data = GeometryExtensionArray(points)
    codes, uniques = pd.factorize(data)
    assert codes.tolist() == [0, 1, 2]
    assert len(uniques) == 3
    assert isinstance(uniques, GeometryExtensionArray)
    assert canon(uniques.geometry_array) == canon(points)


def test_extension_array_factorize_groupby_unique(points: gm.GeometryArray) -> None:
    series = to_pandas(points)
    duplicated = pd.concat([series, series.iloc[[0]]], ignore_index=True)
    codes, uniques = pd.factorize(duplicated)
    grouped = duplicated.groupby(codes).size()
    assert grouped.tolist() == [2, 1, 1]
    assert len(uniques) == 3
    assert len(duplicated.unique()) == 3


def test_empty_geometry_is_a_value_not_missing() -> None:
    """Empty geometries are VALUES (dropna keeps them); only None/pd.NA rows
    are missing — the geopandas-compatible model.
    """
    arr = gm.GeometryArray([
        gm.Point(0, 0, crs=4326),
        gm.from_wkt('POINT EMPTY', crs=4326),
    ])
    data = GeometryExtensionArray(arr)
    assert data.isna().tolist() == [False, False]
    series = pd.Series(data)
    assert len(series.dropna()) == 2


def test_missing_rows_via_mask() -> None:
    data = GeometryExtensionArray._from_sequence([
        gm.Point(0, 0),
        None,
        gm.from_wkt('POINT EMPTY'),
    ])
    assert data.isna().tolist() == [False, True, False]
    assert data[1] is None
    series = pd.Series(data)
    assert len(series.dropna()) == 2


def test_core_missing_rows_are_first_class() -> None:
    arr = gm.GeometryArray([gm.Point(0, 0), None])
    data = GeometryExtensionArray(arr)

    assert data.isna().tolist() == [False, True]
    assert data[1] is None


@pytest.mark.parametrize(
    'rows',
    [
        [gm.Point(0, 0), None, gm.Point(2, 2)],
        [
            gm.LineString([(0, 0), (1, 1)]),
            None,
            gm.LineString([(2, 2), (3, 3)]),
        ],
        [gm.box(0, 0, 1, 1), None, gm.box(2, 2, 3, 3)],
    ],
)
def test_pandas_nullable_packed_roundtrip_preserves_payload_rows(
    rows: list[object],
) -> None:
    values = gm.GeometryArray(rows, crs=4326).set_epoch(2020.0)
    restored = from_pandas(to_pandas(values))
    assert restored.to_wkt(drop_epoch=True) == values.to_wkt(drop_epoch=True)
    assert restored.is_missing.tolist() == [False, True, False]
    assert restored.crs == values.crs
    assert restored.epoch == values.epoch


def test_empty_selection_take_and_all_missing_mutation_preserve_frame() -> None:
    arr = gm.GeometryArray([gm.Point(0, 0, crs=4326)]).set_epoch(2020.0)
    series = to_pandas(arr)

    selected = series[np.array([False])]
    assert from_pandas(selected).crs == arr.crs
    assert from_pandas(selected).epoch == 2020.0

    empty = selected.array
    taken = empty.take([-1], allow_fill=True)
    assert taken.geometry_array.crs == arr.crs
    assert taken.geometry_array.epoch == 2020.0
    assert taken.isna().tolist() == [True]

    series.iloc[0] = None
    assert from_pandas(series).crs == arr.crs
    assert from_pandas(series).epoch == 2020.0
    assert series.isna().tolist() == [True]


def test_elementwise_equality() -> None:
    left = pd.Series(
        GeometryExtensionArray(gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)]))
    )
    right = pd.Series(
        GeometryExtensionArray(gm.GeometryArray([gm.Point(0, 0), gm.Point(9, 9)]))
    )
    assert (left == right).tolist() == [True, False]
    assert (left != right).tolist() == [False, True]
    # frames are part of the value (scalar __eq__ parity)
    tagged = pd.Series(
        GeometryExtensionArray(
            gm.GeometryArray([gm.Point(0, 0, crs=4326), gm.Point(1, 1, crs=4326)])
        )
    )
    assert (left == tagged).tolist() == [False, False]


def test_extension_array_concat(points: gm.GeometryArray) -> None:
    left = GeometryExtensionArray(points[:2])
    right = GeometryExtensionArray(points[2:])
    merged = GeometryExtensionArray._concat_same_type([left, right])
    assert canon(merged.geometry_array) == canon(points)


def test_from_geopandas_roundtrip(points: gm.GeometryArray) -> None:
    geoseries = points.to_geopandas()
    restored = gm.from_geopandas(geoseries)
    assert canon(restored) == canon(points)
    assert restored.crs == points.crs


def test_to_geopandas_rejects_epoch_loss_without_flag(points: gm.GeometryArray) -> None:
    epoch_points = points.set_epoch(2020.0)
    with pytest.raises(ValueError, match='drop_epoch=True'):
        epoch_points.to_geopandas()
    restored = gm.from_geopandas(epoch_points.to_geopandas(drop_epoch=True))
    assert canon(restored) == canon(points)
    assert restored.crs == points.crs
    assert restored.epoch is None


def test_from_geopandas_gdf_roundtrip(points: gm.GeometryArray) -> None:
    import geopandas as gpd

    gdf = gpd.GeoDataFrame({'id': [1, 2, 3]}, geometry=points.to_geopandas())
    restored = gm.from_geopandas(gdf)
    assert canon(restored) == canon(points)
    assert restored.crs == points.crs


def test_gometry_extension_array_from_geopandas(points: gm.GeometryArray) -> None:
    series = to_pandas(points)
    with pytest.raises(TypeError, match='use from_pandas'):
        gm.from_geopandas(series)
    restored = gm.from_pandas(series)
    assert restored is series.array.geometry_array


def test_extension_setitem_iterable_values() -> None:
    arr = gm.GeometryArray([
        gm.Point(0, 0, crs=4326),
        gm.Point(1, 1, crs=4326),
        gm.Point(2, 2, crs=4326),
    ])
    data = GeometryExtensionArray(arr)

    data[[0, 1]] = iter([gm.Point(9, 9, crs=4326), gm.Point(8, 8, crs=4326)])
    assert data.geometry_array.to_wkt()[:2] == ['POINT (9 9)', 'POINT (8 8)']
    assert data.geometry_array.crs == 'EPSG:4326'

    data[[0, 1]] = [gm.Point(3, 3, crs=4326), gm.Point(4, 4, crs=4326)]
    assert data.geometry_array.to_wkt()[:2] == ['POINT (3 3)', 'POINT (4 4)']

    with pytest.raises(ValueError, match='cannot set'):
        data[[0, 1]] = [gm.Point(0, 0, crs=4326)]


def test_pandas_setitem_identity() -> None:
    """Fancy-index assignment scatters into the column; CRS preserved.

    Selection-size scaling lives in ``benches/cases/case_pandas_setitem_scale.py``.
    """
    pytest.importorskip('pandas')
    import numpy as np
    from gometry._pandas import GeometryExtensionArray

    arr = gm.points([0.0, 1.0, 2.0, 3.0], [0.0, 1.0, 2.0, 3.0])
    ext = GeometryExtensionArray(arr)

    pt = gm.Point(9.0, 9.0)
    ext[2] = pt
    assert ext[2] == pt
    assert ext._geoms.crs == arr.crs

    positions = np.array([0, 1, 3], dtype=np.intp)
    values = [gm.Point(10.0, 10.0), gm.Point(11.0, 11.0), gm.Point(12.0, 12.0)]
    ext[positions] = values
    for position, expected in zip(positions, values, strict=True):
        assert ext[int(position)] == expected
    assert ext._geoms.crs == arr.crs
