"""Admission-contract regressions for Python values and providers."""

from __future__ import annotations

import collections.abc
import itertools
import math
import subprocess
import sys
import textwrap
from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    from collections.abc import Iterable

import gometry as gm
import pytest
from gometry import _lib

_CHILD_HANG_TIMEOUT_SECONDS = 30


def assert_same(call, error: BaseException) -> None:
    with pytest.raises(type(error)) as excinfo:
        call()
    assert excinfo.value is error


class ExactLengthRows:
    def __init__(self, rows: Iterable[object]) -> None:
        self.rows = list(rows)
        self.length_calls = 0

    def __iter__(self):
        return iter(self.rows)

    def __len__(self) -> int:
        self.length_calls += 1
        return len(self.rows)


class ExactLengthList(list):
    def __init__(self, rows: Iterable[object]) -> None:
        self.length_calls = 0
        super().__init__(rows)
        self.length_calls = 0

    def __len__(self) -> int:
        self.length_calls += 1
        return super().__len__()


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
        pytest.param(
            lambda value: gm.points([value], [0]), 'x must be finite', id='points'
        ),
        pytest.param(
            lambda value: gm.box(value, 0, 1, 1), 'minx must be finite', id='box'
        ),
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


def test_integer_domains_reject_bool_and_report_large_ids() -> None:
    with pytest.raises(TypeError, match='H3 cell'):
        gm.H3Cell(True)
    with pytest.raises(gm.GeometryError, match='64-bit'):
        gm.H3Cell(2**64)
    with pytest.raises(TypeError, match='integer'):
        gm.h3_cover(gm.Point(0, 0), resolution=True)


def test_array_spatial_key_rejects_bool_level() -> None:
    points = gm.GeometryArray([gm.Point(0, 0)])
    with pytest.raises(TypeError, match='integer'):
        points.sort_by_spatial_key(level=True)


def test_spatial_key_reports_oversized_level_as_geometry_error() -> None:
    points = gm.GeometryArray([gm.Point(0, 0)])
    with pytest.raises(gm.GeometryError, match='integer is too large') as excinfo:
        points.sort_by_spatial_key(level=10**100)
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
    assert (
        gm.LineString([(0, 0), (10, 0)]).line_interpolate(at).to_wkt() == 'POINT (1 0)'
    )
    assert at.calls == 1


def test_crs_transform_bounds_preserves_second_next_error_identity() -> None:
    error = RuntimeError('bounds iterator boom')

    class Rows:
        def __init__(self) -> None:
            self.iter_calls = 0
            self.next_calls = 0

        def __iter__(self):
            self.iter_calls += 1

            def rows():
                self.next_calls = 1
                yield (0.0, 0.0, 1.0, 1.0)
                self.next_calls = 2
                raise error

            return rows()

    rows = Rows()
    assert_same(
        lambda: gm.crs_transform_bounds('not-a-crs', 3857, rows, densify=0),
        error,
    )
    assert (rows.iter_calls, rows.next_calls) == (1, 2)


def test_crs_transform_bounds_preserves_len_error_identity_after_first_row() -> None:
    error = RuntimeError('bounds len boom')

    class Rows:
        def __init__(self) -> None:
            self.iter_calls = 0
            self.next_calls = 0
            self.len_calls = 0

        def __iter__(self):
            self.iter_calls += 1

            def rows():
                self.next_calls = 1
                yield (0.0, 0.0, 1.0, 1.0)
                self.next_calls = 2
                yield (0.0, 0.0, 1.0, 1.0)

            return rows()

        def __len__(self) -> int:
            self.len_calls += 1
            assert self.next_calls == 1
            raise error

    rows = Rows()
    assert_same(
        lambda: gm.crs_transform_bounds(4326, 3857, rows, densify=0),
        error,
    )
    assert (rows.iter_calls, rows.next_calls, rows.len_calls) == (1, 1, 1)


def test_crs_transform_bounds_parses_crs_before_scalar_finiteness() -> None:
    with pytest.raises(gm.CRSError):
        gm.crs_transform_bounds(
            'not-a-crs',
            3857,
            (math.inf, 0.0, 1.0, 1.0),
            densify=0,
        )


def test_crs_transform_bounds_parses_crs_before_batch_row_shape() -> None:
    with pytest.raises(gm.CRSError):
        gm.crs_transform_bounds(
            'not-a-crs',
            3857,
            [(0.0, 0.0, 1.0)],
            densify=0,
        )


def test_crs_transform_bounds_scalar_lane_probes_only_one_excess_value() -> None:
    class Repeating:
        def __init__(self) -> None:
            self.iter_calls = 0
            self.next_calls = 0

        def __iter__(self):
            self.iter_calls += 1
            return self

        def __next__(self) -> float:
            self.next_calls += 1
            if self.next_calls <= 7:
                return 0.0
            raise AssertionError('scalar bounds consumed a second excess value')

    values = Repeating()
    with pytest.raises(gm.GeometryError, match='4-tuple or 6-tuple'):
        gm.crs_transform_bounds(4326, 3857, values, densify=0)
    assert (values.iter_calls, values.next_calls) == (1, 7)


def test_line_interpolate_drains_at_provider_before_processing_array_rows() -> None:
    class RaisingDistances:
        def __len__(self) -> int:
            return 2

        def __iter__(self):
            yield 1.0
            raise RuntimeError('at provider boom')

    rows = gm.GeometryArray([gm.Point(0, 0), gm.LineString([(0, 0), (10, 0)])])

    with pytest.raises(RuntimeError, match='at provider boom'):
        rows.line_interpolate(RaisingDistances())


@pytest.mark.parametrize(
    'operation',
    [
        pytest.param(
            lambda: gm.nearest([gm.Point(0, 0)], gm.Point(0, 0), k=True), id='k'
        ),
        pytest.param(lambda: gm.s2_cover(gm.Point(0, 0), level=True), id='level'),
        pytest.param(
            lambda: gm.Point(0, 0).buffer(1, quadrant_segments=True), id='count'
        ),
        pytest.param(lambda: gm.crs_search('WGS', limit=True), id='limit'),
        pytest.param(lambda: gm.pluscode_encode(0, 0, length=True), id='length'),
        pytest.param(
            lambda: gm.H3Cell('8928308280fffff').cell_from_local_ij(True, 0),
            id='local ij',
        ),
        pytest.param(
            lambda: gm.CRS(4326).to_epsg(min_confidence=True), id='confidence'
        ),
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


@pytest.mark.parametrize(
    'parse',
    [
        gm.MultiPoint,
        gm.line_strings,
        gm.from_wkt,
        gm.from_wkb,
        gm.from_geojson,
        gm.from_polyline,
        gm.require,
    ],
    ids=[
        'MultiPoint',
        'line_strings',
        'from_wkt',
        'from_wkb',
        'from_geojson',
        'from_polyline',
        'require',
    ],
)
def test_row_adapters_acquire_iterators_before_exact_lengths(parse) -> None:
    iter_error = RuntimeError('iter provider boom')
    len_error = RuntimeError('len provider boom')

    class RaisingProviders:
        def __iter__(self):
            raise iter_error

        def __len__(self):
            raise len_error

    assert_same(lambda: parse(RaisingProviders()), iter_error)


def test_from_wkt_stream_reserves_from_exact_source_length_once() -> None:
    rows = ExactLengthRows(['POINT (1 2)', 'POINT (3 4)'])

    geometries = gm.from_wkt(rows)

    assert rows.length_calls == 1
    assert geometries.to_wkt() == ['POINT (1 2)', 'POINT (3 4)']


def test_from_wkb_stream_reserves_from_exact_source_length_once() -> None:
    rows = ExactLengthRows([
        gm.Point(1, 2).to_wkb(),
        gm.Point(3, 4).to_wkb(),
    ])

    geometries = gm.from_wkb(rows)

    assert rows.length_calls == 1
    assert geometries.to_wkt() == ['POINT (1 2)', 'POINT (3 4)']


def test_from_geojson_stream_reserves_from_exact_source_length_once() -> None:
    rows = ExactLengthRows([
        {'type': 'Point', 'coordinates': [1, 2]},
        {'type': 'Point', 'coordinates': [3, 4]},
    ])

    geometries = gm.from_geojson(rows)

    assert rows.length_calls == 1
    assert geometries.to_wkt() == ['POINT (1 2)', 'POINT (3 4)']


def test_from_polyline_stream_reserves_from_exact_source_length_once() -> None:
    encoded = '_p~iF~ps|U_ulLnnqC_mqNvxq`@'
    rows = ExactLengthRows([encoded, encoded])

    geometries = gm.from_polyline(rows)

    assert rows.length_calls == 1
    assert geometries.to_wkt() == [
        'LINESTRING (-120.2 38.5, -120.95 40.7, -126.453 43.252)',
        'LINESTRING (-120.2 38.5, -120.95 40.7, -126.453 43.252)',
    ]


def test_require_stream_reserves_from_exact_source_length_once() -> None:
    rows = ExactLengthRows([gm.Point(1, 2), gm.Point(3, 4)])

    geometries = gm.require(rows)

    assert rows.length_calls == 1
    assert geometries.to_wkt() == ['POINT (1 2)', 'POINT (3 4)']


def test_generic_collection_rejects_unrepresentable_exact_length_before_next() -> None:
    class ExactHugeGeometryRows:
        def __init__(self) -> None:
            self.remaining = sys.maxsize
            self.events: list[str] = []
            self.next_calls = 0

        def __len__(self) -> int:
            self.events.append('len')
            return self.remaining

        def __iter__(self):
            self.events.append('iter')
            return self

        def __next__(self) -> gm.Point:
            self.next_calls += 1
            if self.remaining == 0:
                raise StopIteration
            self.remaining -= 1
            return gm.Point(1, 2)

    rows = ExactHugeGeometryRows()
    with pytest.raises(OverflowError, match='resource size arithmetic overflowed'):
        gm.to_feature_collection(rows)

    assert rows.events == ['iter', 'len']
    assert rows.next_calls == 0


def test_concrete_wkt_batch_reserves_from_exact_source_length_once() -> None:
    wkt_rows = ExactLengthList(['POINT (1 2)', 'POINT (3 4)'])
    wkt = gm.from_wkt(wkt_rows)
    assert wkt_rows.length_calls == 1
    assert wkt.to_wkt() == ['POINT (1 2)', 'POINT (3 4)']


def test_concrete_wkb_batch_reserves_from_exact_source_length_once() -> None:
    wkb_rows = ExactLengthList([
        gm.Point(1, 2).to_wkb(),
        gm.Point(3, 4).to_wkb(),
    ])
    wkb = gm.from_wkb(wkb_rows)
    assert wkb_rows.length_calls == 1
    assert wkb.to_wkt() == ['POINT (1 2)', 'POINT (3 4)']


def test_concrete_geojson_batch_reserves_from_exact_source_length_once() -> None:
    geojson_rows = ExactLengthList([
        {'type': 'Point', 'coordinates': [1, 2]},
        {'type': 'Point', 'coordinates': [3, 4]},
    ])
    geojson = gm.from_geojson(geojson_rows)
    assert geojson_rows.length_calls == 1
    assert geojson.to_wkt() == ['POINT (1 2)', 'POINT (3 4)']


def test_exact_max_len_overflow_is_reported_at_mixed_shape_demotion() -> None:
    class ExactMaximumLength:
        def __init__(self) -> None:
            self.yielded = 0

        def __len__(self) -> int:
            return sys.maxsize

        def __iter__(self):
            for row in (
                gm.Point(0, 0),
                gm.LineString([(0, 0), (1, 1)]),
                object(),
            ):
                self.yielded += 1
                yield row
            for _ in range(sys.maxsize - 3):
                self.yielded += 1
                yield gm.Point(2, 2)

    rows = ExactMaximumLength()
    with pytest.raises(OverflowError):
        gm.GeometryArray(rows)
    assert rows.yielded == 2


def test_exact_max_len_overflow_is_reported_at_packed_line_offsets() -> None:
    class ExactMaximumLength:
        def __init__(self) -> None:
            self.yielded = 0

        def __len__(self) -> int:
            return sys.maxsize

        def __iter__(self):
            for row in (gm.LineString([(0, 0), (1, 1)]), object()):
                self.yielded += 1
                yield row
            for _ in range(sys.maxsize - 2):
                self.yielded += 1
                yield gm.Point(2, 2)

    rows = ExactMaximumLength()
    with pytest.raises(OverflowError):
        gm.GeometryArray(rows)
    assert rows.yielded == 1


def test_missing_mask_preserves_iterator_acquisition_error() -> None:
    error = TypeError('provider not iterable sentinel')

    class RaisingIter:
        def __iter__(self):
            raise error

    array = gm.points([1.0, 2.0], [3.0, 4.0])
    assert_same(lambda: array._with_missing(RaisingIter()), error)


@pytest.mark.parametrize(
    'prefix',
    [[], [True], [True, False]],
    ids=['first-item', 'second-item', 'surplus-probe'],
)
def test_missing_mask_preserves_iteration_error(prefix: list[bool]) -> None:
    error = TypeError('provider not iterable sentinel')

    def values():
        yield from prefix
        raise error

    array = gm.points([1.0, 2.0], [3.0, 4.0])
    assert_same(lambda: array._with_missing(values()), error)


@pytest.mark.parametrize(
    ('mask', 'observed'),
    [([], 0), ([True], 1), ([True, False, object()], 3)],
    ids=['empty', 'short', 'surplus-before-type-check'],
)
def test_missing_mask_reports_observed_length(mask: list, observed: int) -> None:
    array = gm.points([1.0, 2.0], [3.0, 4.0])
    with pytest.raises(
        ValueError, match=f'^mask length {observed} does not match array length 2$'
    ):
        array._with_missing(iter(mask))


def test_index_provider_errors_are_not_reclassified() -> None:
    class RaisingIndex:
        def __index__(self):
            raise RuntimeError('index provider boom')

    with pytest.raises(RuntimeError, match='index provider boom'):
        gm.h3_cover(gm.Point(0, 0), resolution=RaisingIndex())
    with pytest.raises(RuntimeError, match='index provider boom'):
        gm.SpatialIndex([gm.Point(0, 0)]).__contains__(RaisingIndex())


def test_index_protocol_uses_the_type_slot_once() -> None:
    class ClassIndex:
        def __init__(self, value: int) -> None:
            self.value = value
            self.calls = 0

        def __index__(self) -> int:
            self.calls += 1
            return self.value

    provider = ClassIndex(7)
    cells = gm.h3_cover(gm.Point(0, 0), resolution=provider)
    assert len(cells) == 1
    assert provider.calls == 1


def test_instance_only_index_attribute_is_not_the_index_protocol() -> None:
    class InstanceIndex:
        def __init__(self) -> None:
            self.calls = 0

            def index() -> int:
                self.calls += 1
                return 7

            self.__index__ = index

    provider = InstanceIndex()
    with pytest.raises(TypeError):
        gm.h3_cover(gm.Point(0, 0), resolution=provider)
    assert provider.calls == 0


def test_index_protocol_uses_the_builtin_int_slot_for_int_subclasses() -> None:
    array = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)])

    class IntSubclass(int):
        def __new__(cls, value: int) -> Self:
            return int.__new__(cls, value)

        def __init__(self, value: int) -> None:
            self.calls = 0

        def __index__(self) -> int:
            self.calls += 1
            raise RuntimeError('int subclass provider must not run')

    provider = IntSubclass(1)
    assert array[(provider,)].to_wkt() == ['POINT (1 1)']
    assert provider.calls == 0


def test_positional_index_provider_exception_instances_are_preserved() -> None:
    array = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)])

    class Provider:
        def __init__(self, error: BaseException) -> None:
            self.error = error

        def __index__(self):
            raise self.error

    point = gm.Point(0, 0)
    row_views = (
        point.coords,
        gm.GeometryCollection([point]).parts,
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


def test_list_and_tuple_keep_distinct_mixed_provider_admission_order() -> None:
    array = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)])

    class Provider:
        def __init__(self, error: BaseException) -> None:
            self.error = error
            self.calls = 0

        def __index__(self) -> int:
            self.calls += 1
            raise self.error

    for error_type in (gm.GeometryError, TypeError, RuntimeError):
        error = error_type('list provider error')
        provider = Provider(error)
        assert_same(lambda provider=provider: array[[True, provider]], error)
        assert provider.calls == 1

        tuple_provider = Provider(TypeError('tuple provider must not run'))
        with pytest.raises(TypeError) as excinfo:
            array[(True, tuple_provider)]
        assert type(excinfo.value) is TypeError
        assert tuple_provider.calls == 0


def test_mixed_mask_and_oversized_integer_stays_a_type_error() -> None:
    array = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)])
    with pytest.raises(TypeError, match='indices must be'):
        array[[True, 1 << 100]]


def test_list_fancy_index_snapshots_before_ordered_provider_mutation() -> None:
    array = gm.GeometryArray([gm.Point(0, 10), gm.Point(1, 11), gm.Point(2, 12)])
    selectors: list[object] = []
    order: list[str] = []

    class MutatingIndex:
        def __init__(self, name: str, value: int) -> None:
            self.name = name
            self.value = value

        def __index__(self) -> int:
            order.append(self.name)
            if self.name == 'first':
                selectors.clear()
            return self.value

    selectors.extend([
        MutatingIndex('first', 2),
        MutatingIndex('second', 0),
    ])
    selected = array[selectors]

    assert order == ['first', 'second']
    assert selectors == []
    assert [tuple(row.coords[0]) for row in selected] == [
        (2.0, 12.0),
        (0.0, 10.0),
    ]


def test_generic_fancy_sequence_failure_precedes_item_provider() -> None:
    array = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)])
    calls: list[str] = []

    class Provider:
        def __index__(self) -> int:
            calls.append('provider')
            return 0

    provider = Provider()
    iteration_error = RuntimeError('sequence iterator error')

    class FailingSequence(collections.abc.Sequence):
        def __len__(self) -> int:
            return 2

        def __getitem__(self, index: int) -> object:
            if index == 0:
                return provider
            if index == 1:
                return 1
            raise IndexError(index)

        def __iter__(self):
            yield provider
            raise iteration_error

    assert_same(lambda: array[FailingSequence()], iteration_error)
    assert calls == []


def test_empty_replace_at_consumes_both_inputs_before_returning() -> None:
    array = gm.GeometryArray(
        [gm.Point(0, 0, z=1, m=2, crs=4326, epoch=2020.0), None],
        crs=4326,
        epoch=2020.0,
    )

    class Tracked:
        def __init__(self, values):
            self.values = values
            self.started = False
            self.exhausted = False

        def __iter__(self):
            self.started = True
            yield from self.values
            self.exhausted = True

    positions = Tracked([])
    values = Tracked([])
    assert isinstance(array._replace_at(positions, values), gm.GeometryArray)

    assert positions.started and positions.exhausted
    assert values.started and values.exhausted

    mismatch_positions = Tracked([])
    mismatch_values = Tracked([object()])
    with pytest.raises(ValueError) as excinfo:
        array._replace_at(mismatch_positions, mismatch_values)
    assert type(excinfo.value) is ValueError
    assert mismatch_positions.started and mismatch_positions.exhausted
    assert mismatch_values.started and mismatch_values.exhausted


def test_empty_replace_at_does_not_swallow_provider_errors() -> None:
    array = gm.GeometryArray([gm.Point(0, 0)])

    class Raising:
        def __init__(self, error: BaseException) -> None:
            self.error = error

        def __iter__(self):
            raise self.error

    positions_error = RuntimeError('positions provider')
    assert_same(
        lambda: array._replace_at(Raising(positions_error), []), positions_error
    )
    values_error = RuntimeError('values provider')
    assert_same(lambda: array._replace_at([], Raising(values_error)), values_error)


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
    multi_polygon = gm.MultiPolygon(
        Once([Once([Once([(0, 0), (4, 0), (4, 4), (0, 4)])])])
    )
    assert len(multi_polygon.parts) == 1
    rings = gm.GeometryArray(Once([polygon])).boundary().parts
    assert len(rings) == 1 and len(rings[0]) == 2
    parts = gm.GeometryArray(Once([polygon])).parts
    assert len(parts) == 1 and len(parts[0]) == 1


def test_fixed_arity_sources_stop_after_one_excess_item() -> None:
    with pytest.raises(gm.GeometryError, match='too many'):
        gm.MultiPoint([itertools.repeat(1)])
    with pytest.raises(gm.GeometryError, match='too many'):
        gm.MultiPolygon([[itertools.repeat(1)]])
    with pytest.raises(gm.GeometryError, match='4-tuple or 6-tuple'):
        gm.crs_transform_bounds(4326, 3857, itertools.repeat(0))


def test_groups_materialize_one_shot_row_needles_once() -> None:
    groups = _lib._unpickle_int64_groups_v1([1, 2, 3, 4], [0, 2, 4])

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
    # SIGALRM is the behavior oracle; this outer timeout only contains a child
    # that never handles the signal.
    completed = subprocess.run(
        [sys.executable, '-c', script],
        capture_output=True,
        text=True,
        timeout=_CHILD_HANG_TIMEOUT_SECONDS,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
