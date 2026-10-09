"""pandas' own extension-array conformance suite over GeometryExtensionArray.

Borrowed gate: ``pandas.tests.extension.base`` exercises the behavior classes
bespoke probes cannot enumerate (this suite would have caught both live glue
bugs — the scalar ``__eq__`` and the empty-deleting ``dropna``). Ordering,
reductions, and arithmetic are out of scope for an unordered geometry array;
focused interop tests cover the mutable pandas-container contract.
"""

from __future__ import annotations

import itertools

import gometry as gm
import numpy as np
import pandas as pd
import pytest
from gometry._pandas import GeometryDtype, GeometryExtensionArray
from pandas.tests.extension import base


def _points(n: int) -> list[gm.Point]:
    return [gm.Point(float(i), float(i % 7)) for i in range(n)]


@pytest.fixture
def dtype() -> GeometryDtype:
    return GeometryDtype()


@pytest.fixture
def data() -> GeometryExtensionArray:
    """Length-10, first two distinct non-missing (suite contract)."""
    return GeometryExtensionArray(gm.GeometryArray(_points(10)))


@pytest.fixture
def data_for_twos() -> None:
    pytest.skip('geometries have no numeric "2" value')


@pytest.fixture
def data_missing() -> GeometryExtensionArray:
    """[missing, valid] (suite requirement)."""
    return GeometryExtensionArray._from_sequence([None, gm.Point(1.0, 1.0)])


@pytest.fixture(params=['data', 'data_missing'])
def all_data(request, data, data_missing):
    if request.param == 'data':
        return data
    return data_missing


@pytest.fixture
def data_repeated(data):
    def gen(count):
        for _ in range(count):
            yield data

    return gen


@pytest.fixture
def data_for_sorting() -> None:
    pytest.skip('geometries are unordered')


@pytest.fixture
def data_missing_for_sorting() -> None:
    pytest.skip('geometries are unordered')


@pytest.fixture
def na_cmp():
    return lambda x, y: x is None and y is None


@pytest.fixture
def na_value():
    return None


@pytest.fixture
def data_for_grouping() -> GeometryExtensionArray:
    """[B, B, NA, NA, A, A, B, C] with A < B < C under the argsort order.

    Geometries sort by WKB bytes (`_values_for_argsort`), and IEEE-754
    little-endian bytes do not sort numerically — so pick x values that
    differ only in the low mantissa byte, where byte order == value order.
    """
    base = 1.0
    a = gm.Point(base, 0.0)
    b = gm.Point(np.nextafter(base, 2.0), 0.0)
    c = gm.Point(np.nextafter(np.nextafter(base, 2.0), 2.0), 0.0)
    return GeometryExtensionArray._from_sequence([b, b, None, None, a, a, b, c])


@pytest.fixture(params=[True, False])
def box_in_series(request):
    return request.param


@pytest.fixture(
    params=[
        lambda _x: 1,
        lambda x: [1] * len(x),
        lambda x: pd.Series([1] * len(x)),
        lambda x: x,
    ],
    ids=['scalar', 'list', 'series', 'object'],
)
def groupby_apply_op(request):
    return request.param


@pytest.fixture(params=[True, False])
def as_frame(request):
    return request.param


@pytest.fixture(params=[True, False])
def as_series(request):
    return request.param


@pytest.fixture(params=[True, False])
def use_numpy(request):
    return request.param


@pytest.fixture(params=['ffill', 'bfill'])
def fillna_method(request):
    return request.param


@pytest.fixture(params=[True, False])
def as_array(request):
    return request.param


@pytest.fixture
def invalid_scalar(data):
    return object.__new__(object)


@pytest.fixture(params=[True, False])
def using_nan_is_na(request):
    """Mirrors pandas' own session fixture (pandas/conftest.py)."""
    with pd.option_context('future.distinguish_nan_and_na', not request.param):
        yield request.param


class TestDtype(base.BaseDtypeTests):
    pass


class TestInterface(base.BaseInterfaceTests):
    pass


class TestConstructors(base.BaseConstructorsTests):
    def test_from_dtype(self, data):
        """Concrete dtype construction works without global string registration."""
        expected = pd.Series(data)
        result = pd.Series(list(data), dtype=data.dtype)
        pd.testing.assert_series_equal(result, expected)

        with pytest.raises(TypeError):
            pd.Series(list(data), dtype=str(data.dtype))


class TestGetitem(base.BaseGetitemTests):
    @pytest.mark.parametrize(
        'indexer',
        [[0, 1, 2, pd.NA], pd.array([0, 1, 2, pd.NA], dtype='Int64')],
        ids=['list', 'integer-array'],
    )
    def test_getitem_series_integer_with_missing_raises(self, data, indexer):
        """A string-indexed Series treats integers as labels, so NA never matches.

        pandas' base test (xfailed upstream for every extension array) expects
        the integer-NA `ValueError`; pandas resolves this lookup by label and
        raises `KeyError` before the array is consulted.
        """
        series = pd.Series(data, index=[chr(100 + i) for i in range(len(data))])
        with pytest.raises(KeyError, match='are in the'):
            series[indexer]


class TestIndex(base.BaseIndexTests):
    pass


class TestMissing(base.BaseMissingTests):
    pass


class TestCasting(base.BaseCastingTests):
    pass


class TestPrinting(base.BasePrintingTests):
    pass


class TestReshaping(base.BaseReshapingTests):
    @pytest.mark.parametrize(
        'index',
        [
            pd.MultiIndex.from_product([['A', 'B'], ['a', 'b']], names=['a', 'b']),
            pd.MultiIndex.from_tuples([('A', 'a'), ('A', 'b'), ('B', 'b')]),
            pd.MultiIndex.from_product([('A', 'B'), ('a', 'b', 'c'), (0, 1, 2)]),
            pd.MultiIndex.from_tuples([
                ('A', 'a', 1),
                ('A', 'b', 0),
                ('A', 'a', 0),
                ('B', 'a', 0),
                ('B', 'c', 1),
            ]),
        ],
    )
    @pytest.mark.parametrize('obj', ['series', 'frame'])
    def test_unstack(self, data, index, obj):
        """Unstack with `None` as the missing geometry.

        The base test passes `fill_value=data.dtype.na_value` to the object
        reference; for `na_value=None` pandas reads that as "no fill value" and
        fills with NaN, so its expectation names the wrong missing marker.
        Everything else is pandas' own assertion sequence.
        """
        final_length = min(len(index), len(data))
        index = index[:final_length]
        data = data[:final_length]
        if obj == 'series':
            ser = pd.Series(data, index=index)
        else:
            ser = pd.DataFrame({'A': data, 'B': data}, index=index)

        levels = range(index.nlevels)
        combinations = itertools.chain.from_iterable(
            itertools.permutations(levels, size) for size in range(1, index.nlevels)
        )
        for level in combinations:
            result = ser.unstack(level=list(level))
            assert all(
                isinstance(result[column].array, type(data))
                for column in result.columns
            )
            if obj == 'series':
                alternative = ser.to_frame().unstack(level=list(level))
                pd.testing.assert_frame_equal(result, alternative.droplevel(0, axis=1))

            expected = ser.astype(object).unstack(level=list(level))
            expected = expected.where(expected.notna(), None)
            if obj == 'series':
                assert expected.dtypes.eq(np.dtype(object)).all()
            pd.testing.assert_frame_equal(result.astype(object), expected)


class TestGroupby(base.BaseGroupbyTests):
    pass


class TestMethods(base.BaseMethodsTests):
    def test_combine_le(self, data_repeated):
        pytest.skip('geometries are unordered; `<=` is a TypeError by design')
