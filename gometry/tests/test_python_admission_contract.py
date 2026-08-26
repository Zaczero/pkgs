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


def test_float_like_coordinates_reject_integer_outside_float_domain() -> None:
    with pytest.raises(TypeError, match='finite float'):
        gm.Point(2**1024, 0)


def test_integer_domains_reject_bool_and_report_large_ids() -> None:
    with pytest.raises(TypeError, match='H3 cell'):
        gm.H3Cell(True)
    with pytest.raises(gm.GeometryError, match='64-bit'):
        gm.H3Cell(2**64)
    with pytest.raises(TypeError, match='integer'):
        gm.h3_cover(gm.Point(0, 0), resolution=True)
    with pytest.raises(TypeError, match='integer'):
        gm.Point(0, 0).spatial_key(level=True)


def test_scalar_float_providers_are_admitted_once_and_must_be_finite() -> None:
    with pytest.raises(gm.GeometryError, match='x_angle must be finite'):
        gm.Point(0, 0).skew(x_angle=math.nan)

    class RaisingFloat:
        def __float__(self):
            raise RuntimeError('float provider boom')

    with pytest.raises(RuntimeError, match='float provider boom'):
        gm.Point(0, 0).skew(x_angle=RaisingFloat())


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


def test_arrow_table_probe_preserves_column_provider_error() -> None:
    class RaisingColumn:
        @property
        def type(self):
            raise RuntimeError('column type provider boom')

    class Table:
        column_names = ('candidate',)

        def column(self, index):
            assert index == 0
            return RaisingColumn()

    with pytest.raises(RuntimeError, match='column type provider boom'):
        gm.from_arrow(Table())


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
    assert len(gm.rings(Once([polygon]))) == 2
    assert len(gm.parts(Once([polygon]))) == 1


def test_fixed_arity_sources_stop_after_one_excess_item() -> None:
    with pytest.raises(gm.GeometryError, match='too many'):
        gm.MultiPoint([itertools.repeat(1)])
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
