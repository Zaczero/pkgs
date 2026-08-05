"""Packed Lines hausdorff/frechet -- column-direct array x array fast path."""

from __future__ import annotations

import gometry as gm
import pytest

from tests._support import canon, floats, line_storage_twins


def test_packed_lines_hausdorff_array_matches_mixed_storage() -> None:
    packed, mixed = line_storage_twins()
    assert packed.to_arrow().type.extension_name == 'geoarrow.linestring'
    assert floats(gm.hausdorff_distance(packed, mixed)) == pytest.approx(
        floats(gm.hausdorff_distance(mixed, mixed))
    )


def test_packed_lines_frechet_array_matches_mixed_storage() -> None:
    packed, mixed = line_storage_twins()
    assert floats(gm.frechet_distance(packed, mixed)) == pytest.approx(
        floats(gm.frechet_distance(mixed, mixed))
    )


def test_packed_lines_concat_matches_mixed_storage() -> None:
    packed, mixed = line_storage_twins()
    assert canon(packed.concat(packed)) == canon(mixed.concat(mixed))


def test_packed_lines_hausdorff_self_matches_per_row_scalar() -> None:
    packed, mixed = line_storage_twins()
    expected = [
        gm.hausdorff_distance(left, right)
        for left, right in zip(mixed, mixed, strict=True)
    ]
    assert floats(gm.hausdorff_distance(packed, packed)) == pytest.approx(expected)
    assert floats(gm.hausdorff_distance(mixed, packed)) == pytest.approx(expected)


def test_packed_lines_frechet_self_matches_per_row_scalar() -> None:
    packed, mixed = line_storage_twins()
    expected = [
        gm.frechet_distance(left, right)
        for left, right in zip(mixed, mixed, strict=True)
    ]
    assert floats(gm.frechet_distance(packed, packed)) == pytest.approx(expected)
    assert floats(gm.frechet_distance(mixed, packed)) == pytest.approx(expected)


def test_packed_lines_frechet_cross_take_shift_matches_oracle() -> None:
    import math

    count = 20
    packed = gm.GeometryArray([
        gm.LineString(
            [
                (float(i), 0.0),
                (float(i) + 0.25, 0.5 * math.sin(i * 0.3)),
                (float(i) + 0.5, 0.0),
                (float(i) + 0.75, -0.5 * math.sin(i * 0.3)),
            ],
            crs=3857,
        )
        for i in range(count)
    ])
    shift = [*list(range(1, count)), 0]
    shifted = packed[shift]
    assert floats(gm.frechet_distance(packed, packed)) == pytest.approx([0.0] * count)
    expected = [
        gm.frechet_distance(left, right)
        for left, right in zip(packed, shifted, strict=True)
    ]
    assert floats(gm.frechet_distance(packed, shifted)) == pytest.approx(expected)


def test_packed_lines_hausdorff_cross_take_shift_matches_oracle() -> None:
    import math

    count = 20
    packed = gm.GeometryArray([
        gm.LineString(
            [
                (float(i), 0.0),
                (float(i) + 0.25, 0.5 * math.sin(i * 0.3)),
                (float(i) + 0.5, 0.0),
                (float(i) + 0.75, -0.5 * math.sin(i * 0.3)),
            ],
            crs=3857,
        )
        for i in range(count)
    ])
    shift = [*list(range(1, count)), 0]
    shifted = packed[shift]
    assert floats(gm.hausdorff_distance(packed, packed)) == pytest.approx([0.0] * count)
    expected = [
        gm.hausdorff_distance(left, right)
        for left, right in zip(packed, shifted, strict=True)
    ]
    assert floats(gm.hausdorff_distance(packed, shifted)) == pytest.approx(expected)


def test_packed_geographic_hausdorff_self_is_zero() -> None:
    import math

    import gometry as gm

    lines = gm.GeometryArray([
        gm.LineString(
            [
                (0.0, float(i) * 0.001),
                (1.0, float(i) * 0.001 + 0.1 * math.sin(i * 0.017)),
                (2.0, float(i) * 0.001),
            ],
            crs=4326,
        )
        for i in range(50)
    ])
    assert floats(gm.hausdorff_distance(lines, lines)) == pytest.approx([0.0] * 50)
    assert gm.hausdorff_distance(lines[0], lines[0]) == pytest.approx(0.0)
