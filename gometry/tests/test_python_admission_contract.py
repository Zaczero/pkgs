"""Admission-contract regressions for Python values and providers."""

from __future__ import annotations

import itertools
import math
import subprocess
import sys
import textwrap

import gometry as gm
import pytest
from gometry import _lib


@pytest.mark.parametrize('value', [2**100, 2**1023])
def test_float_like_coordinates_admit_finite_huge_integers(value: int) -> None:
    point = gm.Point(value, 0)
    assert math.isfinite(point.x)


@pytest.mark.parametrize('value', [2**1024, math.inf, math.nan])
def test_float_like_coordinates_reject_nonfinite_values(value: float) -> None:
    with pytest.raises(gm.InvalidGeometryError, match=r'^x must be finite$'):
        gm.Point(value, 0)


@pytest.mark.parametrize('value', [2**1024, math.inf, math.nan])
@pytest.mark.parametrize(
    ('construct', 'message'),
    [
        pytest.param(lambda value: gm.points([value], [0]), 'x must be finite', id='points'),
        pytest.param(lambda value: gm.box(value, 0, 1, 1), 'minx must be finite', id='box'),
        pytest.param(
            lambda value: gm.LineString([(value, 0), (1, 1)]),
            'coordinate must be finite',
            id='LineString',
        ),
        pytest.param(
            lambda value: gm.Polygon([(value, 0), (1, 0), (0, 1)]),
            'coordinate must be finite',
            id='Polygon',
        ),
    ],
)
def test_constructor_coordinate_lanes_reject_nonfinite_values(
    construct, message, value: float
) -> None:
    with pytest.raises(gm.InvalidGeometryError, match=message):
        construct(value)


@pytest.mark.parametrize('value', [2**1024, math.inf, math.nan])
def test_geodesic_coordinate_fast_path_rejects_nonfinite_values(value: float) -> None:
    with pytest.raises(gm.InvalidGeometryError, match=r'^lon1 must be finite$'):
        gm.CRS(4326).geodesic_inverse(value, 0, 1, 1)


def test_integer_domains_reject_bool_and_report_large_ids() -> None:
    with pytest.raises(TypeError, match='H3 cell'):
        gm.H3Cell(True)
    with pytest.raises(gm.GeometryError, match='64-bit'):
        gm.H3Cell(2**64)
    with pytest.raises(TypeError, match='integer'):
        gm.h3_cover(gm.Point(0, 0), resolution=True)
    with pytest.raises(TypeError, match='integer'):
        gm.Point(0, 0).spatial_key(level=True)


def test_array_spatial_key_rejects_bool_level() -> None:
    points = gm.GeometryArray([gm.Point(0, 0)])
    with pytest.raises(TypeError, match='integer'):
        points.sort_by_spatial_key(level=True)


def test_spatial_key_reports_oversized_level_as_geometry_error() -> None:
    points = gm.GeometryArray([gm.Point(0, 0)])
    for operation in (
        lambda: gm.Point(0, 0).spatial_key(level=10**100),
        lambda: points.sort_by_spatial_key(level=10**100),
    ):
        with pytest.raises(gm.GeometryError, match='integer is too large') as excinfo:
            operation()
        assert not isinstance(excinfo.value, OverflowError)


def test_scalar_float_providers_are_admitted_once_and_must_be_finite() -> None:
    with pytest.raises(gm.GeometryError, match='x_angle must be finite'):
        gm.Point(0, 0).skew(x_angle=math.nan)

    class RaisingFloat:
        def __float__(self):
            raise RuntimeError('float provider boom')

    with pytest.raises(RuntimeError, match='float provider boom'):
        gm.Point(0, 0).skew(x_angle=RaisingFloat())


def test_scalar_float_classification_does_not_reprobe_stateful_values() -> None:
    class OnceFloat:
        def __init__(self, value: float) -> None:
            self.value = value
            self.calls = 0

        def __float__(self) -> float:
            self.calls += 1
            if self.calls > 1:
                raise RuntimeError('float provider called twice')
            return self.value

    bounds_value = OnceFloat(0.0)
    assert gm.crs_transform_bounds(
        4326, 4326, [bounds_value, 0.0, 1.0, 1.0], densify=0
    ) == (0.0, 0.0, 1.0, 1.0)
    assert bounds_value.calls == 1

    at = OnceFloat(1.0)
    assert gm.LineString([(0, 0), (10, 0)]).line_interpolate(at).to_wkt() == 'POINT (1 0)'
    assert at.calls == 1

    lon1 = OnceFloat(0.0)
    gm.CRS(4326).geodesic_inverse(lon1, 0.0, 1.0, 1.0)
    assert lon1.calls == 1


@pytest.mark.parametrize(
    'operation',
    [
        pytest.param(lambda: gm.nearest([gm.Point(0, 0)], gm.Point(0, 0), k=True), id='k'),
        pytest.param(lambda: gm.s2_cover(gm.Point(0, 0), level=True), id='level'),
        pytest.param(
            lambda: gm.Point(0, 0).buffer(1, quadrant_segments=True), id='count'
        ),
        pytest.param(lambda: gm.crs_search('WGS', limit=True), id='limit'),
        pytest.param(lambda: gm.pluscode_encode(0, 0, length=True), id='length'),
        pytest.param(
            lambda: gm.H3Cell('8928308280fffff').cell_from_local_ij(True, 0), id='local ij'
        ),
        pytest.param(lambda: gm.CRS(4326).to_epsg(min_confidence=True), id='confidence'),
    ],
)
def test_integer_parameter_lanes_reject_bool(operation) -> None:
    with pytest.raises(TypeError, match='integer'):
        operation()


def test_boolean_options_remain_boolean_options() -> None:
    assert gm.CRS(4326).to_proj(pretty=True).startswith('+')
    indices, distances = gm.nearest(
        [gm.Point(0, 0)], gm.Point(0, 0), return_distance=True
    )
    assert indices.tolist() == [0]
    assert distances.tolist() == [0.0]


def test_mapping_subclass_uses_mapping_protocol_and_preserves_provider_error() -> None:
    class RaisingKeys(dict[str, object]):
        def keys(self):
            raise RuntimeError('keys provider boom')

    with pytest.raises(RuntimeError, match='keys provider boom'):
        gm.from_geojson(RaisingKeys(type='Point', coordinates=[0, 0]))

    accesses = []

    class MappingSubclass(dict[str, object]):
        def __len__(self):
            return 1

        def keys(self):
            return ('type', 'coordinates')

        def __getitem__(self, key):
            accesses.append(key)
            return super().__getitem__(key)

    value = MappingSubclass()
    dict.__setitem__(value, 'type', 'Point')
    dict.__setitem__(value, 'coordinates', [0, 0])
    assert gm.from_geojson(value).to_wkt() == 'POINT (0 0)'
    assert accesses == ['type', 'coordinates']

    class RaisingGetItem(MappingSubclass):
        def __getitem__(self, key):
            raise LookupError(f'getitem provider boom: {key}')

    bad_value = RaisingGetItem()
    dict.__setitem__(bad_value, 'type', 'Point')
    dict.__setitem__(bad_value, 'coordinates', [0, 0])
    with pytest.raises(LookupError, match='getitem provider boom'):
        gm.from_geojson(bad_value)


def test_arrow_generic_table_shape_is_not_treated_as_pyarrow() -> None:
    class RaisingColumn:
        @property
        def type(self):
            raise RuntimeError('column type provider boom')

    class Table:
        column_names = ('candidate',)

        def column(self, index):
            assert index == 0
            return RaisingColumn()

    with pytest.raises(TypeError, match='expected a GeoArrow-encoded Arrow'):
        gm.from_arrow(Table())


def test_arrow_generic_type_shape_is_not_treated_as_pyarrow() -> None:
    class RaisingType:
        @property
        def type(self):
            raise RuntimeError('top-level type provider boom')

    with pytest.raises(TypeError, match='expected a GeoArrow-encoded Arrow'):
        gm.from_arrow(RaisingType())


def test_iter_and_len_provider_errors_are_not_reclassified() -> None:
    class RaisingIter:
        def __iter__(self):
            raise RuntimeError('iter provider boom')

    class RaisingLen:
        def __len__(self):
            raise RuntimeError('len provider boom')

        def __iter__(self):
            return iter([gm.Point(0, 0)])

    with pytest.raises(RuntimeError, match='iter provider boom'):
        gm.GeometryArray(RaisingIter())
    with pytest.raises(RuntimeError, match='len provider boom'):
        gm.GeometryArray(RaisingLen())

    class LyingLen:
        def __len__(self):
            return 1

        def __iter__(self):
            return iter([gm.Point(0, 0), gm.Point(1, 1)])

    assert len(gm.GeometryArray(LyingLen())) == 2


def test_index_provider_errors_are_not_reclassified() -> None:
    class RaisingIndex:
        def __index__(self):
            raise RuntimeError('index provider boom')

    with pytest.raises(RuntimeError, match='index provider boom'):
        gm.h3_cover(gm.Point(0, 0), resolution=RaisingIndex())
    with pytest.raises(RuntimeError, match='index provider boom'):
        gm.SpatialIndex([gm.Point(0, 0)]).__contains__(RaisingIndex())


def test_positional_index_provider_exception_instances_are_preserved() -> None:
    array = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)])

    class Provider:
        def __init__(self, error: BaseException) -> None:
            self.error = error

        def __index__(self):
            raise self.error

    class IntProvider(int):
        def __new__(cls, error: BaseException):
            value = int.__new__(cls, 0)
            value.error = error
            return value

        def __index__(self):
            raise self.error

    def assert_same(call, error: BaseException) -> None:
        with pytest.raises(type(error)) as excinfo:
            call()
        assert excinfo.value is error

    point = gm.Point(0, 0)
    row_views = (
        point.coords,
        gm.GeometryCollection([point]).parts,
        gm.get_coordinates(
            gm.GeometryArray([point, gm.Point(1, 1)]), return_index=True
        ),
    )
    for error_type in (gm.GeometryError, TypeError, RuntimeError):
        error = error_type('provider error')
        assert_same(lambda error=error: array[Provider(error)], error)

        for view in row_views:
            error = error_type('provider error')
            assert_same(lambda error=error, view=view: view[Provider(error)], error)

        error = error_type('provider error')
        assert_same(lambda error=error: array[[Provider(error)]], error)

        error = error_type('provider error')
        assert_same(
            lambda error=error: array._replace_at([Provider(error)], [None]), error
        )

        error = error_type('provider error')
        assert_same(lambda error=error: array[(IntProvider(error),)], error)


def test_empty_replace_at_consumes_both_inputs_before_returning() -> None:
    array = gm.GeometryArray(
        [gm.Point(0, 0, z=1, m=2, crs=4326, epoch=2020.0), None],
        crs=4326,
        epoch=2020.0,
    )

    class Tracked:
        def __init__(self, values):
            self.values = values
            self.consumed = False

        def __iter__(self):
            self.consumed = True
            return iter(self.values)

    positions = Tracked([])
    values = Tracked([])
    result = array._replace_at(positions, values)

    assert positions.consumed and values.consumed
    assert result.to_wkt(drop_epoch=True) == array.to_wkt(drop_epoch=True)
    assert result.coordinate_axes == array.coordinate_axes == ['XYZM', None]
    assert result.is_missing.tolist() == [False, True]
    assert result.crs == array.crs
    assert result.epoch == array.epoch

    mismatch_positions = Tracked([])
    mismatch_values = Tracked([object()])
    with pytest.raises(
        ValueError, match='positions length 0 does not match values length 1'
    ):
        array._replace_at(mismatch_positions, mismatch_values)
    assert mismatch_positions.consumed and mismatch_values.consumed


def test_empty_replace_at_does_not_swallow_provider_errors() -> None:
    array = gm.GeometryArray([gm.Point(0, 0)])

    class Raising:
        def __init__(self, message: str) -> None:
            self.message = message

        def __iter__(self):
            raise RuntimeError(self.message)

    with pytest.raises(RuntimeError, match='positions provider'):
        array._replace_at(Raising('positions provider'), [])
    with pytest.raises(RuntimeError, match='values provider'):
        array._replace_at([], Raising('values provider'))


def test_one_shot_coordinate_and_part_sources_are_consumed_once() -> None:
    class Once:
        def __init__(self, values):
            self.values = values
            self.used = False

        def __iter__(self):
            if self.used:
                raise RuntimeError('source iterated twice')
            self.used = True
            return iter(self.values)

    line = gm.LineString(Once([(0, 0), (1, 0)]))
    assert len(line.coords) == 2
    multi = gm.MultiPoint(Once([(0, 0), (1, 1)]))
    assert len(multi) == 2

    polygon = gm.Polygon(
        Once([(0, 0), (4, 0), (4, 4), (0, 4)]),
        holes=Once([Once([(1, 1), (2, 1), (2, 2), (1, 2)])]),
    )
    multi_polygon = gm.MultiPolygon(Once([Once([Once([(0, 0), (4, 0), (4, 4), (0, 4)])])]))
    assert len(multi_polygon.parts) == 1
    assert len(gm.rings(Once([polygon]))) == 2
    assert len(gm.parts(Once([polygon]))) == 1


def test_fixed_arity_sources_stop_after_one_excess_item() -> None:
    with pytest.raises(gm.GeometryError, match='too many'):
        gm.MultiPoint([itertools.repeat(1)])
    with pytest.raises(gm.GeometryError, match='too many'):
        gm.MultiPolygon([[itertools.repeat(1)]])
    with pytest.raises(gm.GeometryError, match='4-tuple or 6-tuple'):
        gm.crs_transform_bounds(4326, 3857, itertools.repeat(0))


def test_groups_materialize_one_shot_row_needles_once() -> None:
    groups = _lib._unpickle_int64_groups([1, 2, 3, 4], [0, 2, 4])

    assert iter([3, 4]) in groups
    assert groups.index(iter([3, 4])) == 1
    assert groups.count(iter([3, 4])) == 1

    class RaisingIndex:
        def __index__(self):
            raise RuntimeError('row index provider boom')

    with pytest.raises(RuntimeError, match='row index provider boom'):
        groups.count([RaisingIndex()])


def test_native_python_iterator_checks_signals() -> None:
    script = textwrap.dedent(
        """
        import itertools
        import signal
        import gometry as gm

        def interrupt(_signum, _frame):
            raise KeyboardInterrupt

        signal.signal(signal.SIGALRM, interrupt)
        signal.alarm(1)
        try:
            gm.GeometryArray(itertools.repeat(gm.Point(0, 0)))
        except KeyboardInterrupt:
            pass
        else:
            raise AssertionError('infinite native iterator was not interrupted')
        """
    )
    completed = subprocess.run(
        [sys.executable, '-c', script],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
