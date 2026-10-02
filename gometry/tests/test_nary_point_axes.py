"""N-ary point axes follow partition reconciliation before assembly."""

import gometry as gm
import pytest


@pytest.mark.parametrize('operation', [gm.union_all, gm.symmetric_difference_all])
@pytest.mark.parametrize('include_empty', [False, True])
def test_mixed_axis_point_overlay_matches_partition(operation, include_empty):
    values = [gm.Point(0, 0), gm.from_wkt('POINT Z (1 0 7)')]
    if include_empty:
        values.append(gm.from_wkt('POINT EMPTY'))
    expected = gm.from_wkt('MULTIPOINT ((0 0), (1 0))')

    assert operation(values).to_wkb() == expected.to_wkb()


@pytest.mark.parametrize('operation', [gm.union_all, gm.symmetric_difference_all])
def test_point_overlay_visits_shared_collection_support_once(operation):
    point = gm.from_wkt('POINT ZM (1 2 3 4)')
    value = point
    for _ in range(128):
        value = gm.GeometryCollection([value, value])

    assert operation([value]).to_wkb() == point.to_wkb()


@pytest.mark.parametrize('shared_prefix', [False, True])
def test_point_source_order_skips_equal_shared_collection_prefixes(shared_prefix):
    left_prefix = gm.Point(1, 2)
    right_prefix = gm.Point(1, 2)
    for _ in range(127):
        left_prefix = gm.GeometryCollection([left_prefix, left_prefix])
        right_prefix = gm.GeometryCollection([right_prefix, right_prefix])
    if shared_prefix:
        right_prefix = left_prefix
    positive = gm.GeometryCollection([left_prefix, gm.Point(0.0, 0.0)])
    negative = gm.GeometryCollection([right_prefix, gm.Point(-0.0, -0.0)])
    expected = gm.from_wkt('MULTIPOINT ((-0 -0), (1 2))')

    assert gm.union_all([positive, negative]).to_wkb() == expected.to_wkb()
    assert (
        gm.symmetric_difference_all([positive, negative, negative]).to_wkb()
        == expected.to_wkb()
    )
