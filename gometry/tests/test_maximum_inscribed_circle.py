"""Maximum-inscribed-circle output containment and representation failures."""

import gometry as gm
import pytest


def test_maximum_inscribed_circle_covers_axis_aligned_boxes_scalar_and_array() -> None:
    sources = [gm.box(0, 0, 2, 2), gm.box(0, 0, 4, 2)]
    outputs = [source.maximum_inscribed_circle(tolerance=1e-12) for source in sources]
    assert [
        gm.covers(source, output) for source, output in zip(sources, outputs, strict=True)
    ] == [True, True]

    rows = gm.GeometryArray(sources)
    batch = rows.maximum_inscribed_circle(tolerance=[1e-12, 1e-12])
    assert [
        gm.covers(source, output) for source, output in zip(rows, batch, strict=True)
    ] == [True, True]
    assert [gm.equals_exact(output, batch[index]) for index, output in enumerate(outputs)] == [
        True,
        True,
    ]


def test_maximum_inscribed_radius_uses_the_stored_coordinate_clearance() -> None:
    square = gm.box(0.1, 0.1, 0.3, 0.3)
    center = square.polylabel(tolerance=1e-12)
    assert square.maximum_inscribed_radius(tolerance=1e-12) == 0.09999999999999998
    assert square.maximum_inscribed_radius(tolerance=1e-12) == gm.distance(
        center, square.boundary()
    )
    assert gm.covers(square, square.maximum_inscribed_circle(tolerance=1e-12))


def test_maximum_inscribed_circle_respects_an_enclosed_hole() -> None:
    source = gm.Polygon(
        [(0, 0), (10, 0), (10, 10), (0, 10)],
        holes=[[(4.5, 4.5), (5.5, 4.5), (5.5, 5.5), (4.5, 5.5)]],
    )
    circle = source.maximum_inscribed_circle(tolerance=1e-12)
    assert gm.covers(source, circle)
    assert not gm.covers(circle, gm.Point(5, 5))


def test_maximum_inscribed_circle_rejects_unrepresentable_diamond() -> None:
    diamond = gm.Polygon([(0, 1), (-1, 0), (0, -1), (1, 0)])
    with pytest.raises(gm.GeometryError) as raised:
        diamond.maximum_inscribed_circle(tolerance=1e-12)
    assert raised.value.operation == "maximum_inscribed_circle"


def test_maximum_inscribed_circle_rejects_nearest_emission_at_rotated_boundary() -> None:
    rotated = gm.Polygon([
        (0.00000000000000008659560562354934, 1.4142135623730951),
        (-1.4142135623730951, 0.00000000000000017319121124709868),
        (-0.000000000000000259786816870648, -1.4142135623730951),
        (1.4142135623730951, -0.00000000000000034638242249419736),
    ])
    with pytest.raises(gm.GeometryError) as raised:
        rotated.maximum_inscribed_circle(tolerance=1e-12)
    assert raised.value.operation == "maximum_inscribed_circle"
