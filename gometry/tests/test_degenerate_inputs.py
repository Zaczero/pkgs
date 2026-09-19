"""Degenerate input: empties of an accepted kind, rings that collapse under a
transform, zero plateaus, and amplification parameters past every budget.

Each case answers or raises the operation's typed error; none may panic,
hang, or surface an internal size overflow.
"""

from __future__ import annotations

import gometry as gm
import pytest

W = gm.from_wkt


def test_affine_transforms_keep_mixed_axes_rows() -> None:
    rows = gm.GeometryArray([W('POINT (1 2)'), W('POINT Z (1 2 3)')])
    assert rows.translate(1, 1).to_wkt() == ['POINT (2 3)', 'POINT Z (2 3 3)']
    assert rows.affine_transform([2, 0, 0, 2, 0, 0]).to_wkt() == [
        'POINT (2 4)',
        'POINT Z (2 4 3)',
    ]


@pytest.mark.parametrize(
    ('call', 'parameter'),
    [
        (lambda: gm.LineString([(0, 0), (1, 1)]).segmentize(fraction=5e-324), 'fraction'),
        (lambda: gm.LineString([(0, 0), (1, 1)]).segmentize(5e-324), 'max_length'),
        (
            lambda: gm.frechet_distance(
                gm.LineString([(0, 0), (1, 1)]),
                gm.LineString([(0, 1), (1, 0)]),
                densify=5e-324,
            ),
            'densify',
        ),
    ],
    ids=['segmentize-fraction', 'segmentize-max-length', 'frechet-densify'],
)
def test_amplification_past_every_count_names_its_parameter(
    call: object, parameter: str
) -> None:
    with pytest.raises(gm.GeometryError, match=f'reduce {parameter}$') as info:
        call()  # type: ignore[operator]
    assert info.value.parameter == parameter
    assert info.value.produced > info.value.limit > 0
