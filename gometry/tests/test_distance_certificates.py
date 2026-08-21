"""R20-A necessary-condition certificates must not route as sufficient."""

from __future__ import annotations

from fractions import Fraction

import gometry as gm
import pytest


def _hausdorff_case(
    exponent: int, swap_axes: bool
) -> tuple[gm.Geometry, gm.Geometry, float]:
    length = 10.0**exponent
    height = 10.0**-exponent
    baseline = [(0.0, 0.0), (length, 0.0)]
    bent = [(0.0, height), (length / 2.0, 2.0 * height), (length, height)]
    if swap_axes:
        baseline = [(y, x) for x, y in baseline]
        bent = [(y, x) for x, y in bent]
    # The oracle is the exact rational value of the stored apex ordinate, not
    # the decimal literal used to construct it.
    expected = float(Fraction(abs(bent[1][0 if swap_axes else 1])))
    return gm.LineString(baseline), gm.LineString(bent), expected


@pytest.mark.parametrize(
    'exponent', [153, 154, 155, 156, 157, 160, 161, 162, 163, 164, 200, 300]
)
@pytest.mark.parametrize('swap_axes', [False, True])
def test_continuous_hausdorff_preserves_reciprocal_axis_metric(
    exponent: int, swap_axes: bool
) -> None:
    baseline, bent, expected = _hausdorff_case(exponent, swap_axes)
    for left, right in ((baseline, bent), (bent, baseline)):
        assert gm.hausdorff_distance(left, right) == expected
        assert gm.hausdorff_distance(
            gm.GeometryArray([left]), gm.GeometryArray([right])
        ).tolist() == [expected]


@pytest.mark.parametrize('exponent', [154, 155, 161, 162, 200])
@pytest.mark.parametrize('swap_axes', [False, True])
@pytest.mark.parametrize('carrier', ['multi', 'collection'])
def test_continuous_hausdorff_reciprocal_axis_carriers(
    exponent: int, swap_axes: bool, carrier: str
) -> None:
    baseline, bent, expected = _hausdorff_case(exponent, swap_axes)
    wrap = (
        (lambda geom: gm.MultiLineString([geom.coords]))
        if carrier == 'multi'
        else (lambda geom: gm.GeometryCollection([geom]))
    )
    left, right = wrap(baseline), wrap(bent)
    assert gm.hausdorff_distance(left, right) == expected
    assert gm.hausdorff_distance(right, left) == expected


_ZERO_AREA_DEGENERATE_SHELLS = [
    [(0, 0), (1, 0), (0, 0), (0, 1), (0, 0)],
    [(0, 0), (0, 1), (0, 0), (1, 0), (0, 0)],
    [(0, 0), (1, 0), (2, 0), (3, 0), (0, 0)],
]


@pytest.mark.parametrize('shell', _ZERO_AREA_DEGENERATE_SHELLS)
def test_degenerate_four_edge_shells_do_not_gain_rectangle_area(
    shell: list[tuple[int, int]],
) -> None:
    bad = gm.Polygon(shell)
    clip = gm.box(0.5, 0.5, 2.0, 2.0)
    for left, right in ((bad, clip), (clip, bad)):
        result = gm.intersection(left, right)
        assert result.area == 0.0, result.to_wkt()
    packed = gm.intersection(gm.GeometryArray([bad]), clip)
    assert packed[0].area == 0.0, packed[0].to_wkt()
    reverse_packed = gm.intersection(clip, gm.GeometryArray([bad]))
    assert reverse_packed[0].area == 0.0, reverse_packed[0].to_wkt()
