"""Public geographic quotient topology regression paths."""

import math

import gometry as gm

from tests._support import ids, pair_rows


def seam_box() -> gm.Polygon:
    return gm.Polygon(
        [(170, -10), (-170, -10), (-170, 10), (170, 10), (170, -10)], crs=4326
    )


def test_quotient_seam_membership_reaches_every_topology_consumer() -> None:
    source = seam_box()
    point = gm.Point(180, 0, crs=4326)
    outside = gm.Point(0, 0, crs=4326)

    assert gm.intersects(source, point)
    assert gm.intersects(point, source)
    assert gm.intersects(gm.GeometryArray([source, source]), point).tolist() == [
        True,
        True,
    ]
    assert gm.intersects(source, gm.GeometryArray([point, outside])).tolist() == [
        True,
        False,
    ]
    assert gm.relate_pattern(source, point, 'T*****FF*')

    index = gm.SpatialIndex([source])
    assert ids(index.query(point, predicate='intersects')) == [0]
    assert [
        row.tolist()
        for row in index.query(
            gm.GeometryArray([point, outside]), predicate='intersects'
        )
    ] == [
        [0],
        [],
    ]
    assert pair_rows(index.join(gm.GeometryArray([point]), predicate='intersects')) == [
        (0, 0)
    ]
    assert pair_rows(
        gm.join(gm.GeometryArray([point]), [source], predicate='intersects')
    ) == [(0, 0)]
    assert pair_rows(
        gm.SpatialIndex([source, point]).self_join(predicate='intersects')
    ) == [(0, 1)]

    assert gm.intersection(source, point).to_wkt() == point.to_wkt()
    assert len(gm.h3_cover(source, resolution=2, cell_rule='center')) > 0
    assert gm.intersects(source, gm.box(179, -1, 180, 1, crs=4326))


def test_subnormal_seam_crossing_keeps_exact_predicate_and_nary_contacts() -> None:
    """S2's lifted edge reaches the seam at t=1/4 and exact latitude δ/4."""
    point = gm.Point(180, 0, crs=4326)
    delta = math.ulp(0.0)
    cases = (
        (
            gm.LineString([(170, 0), (-160, delta)], crs=4326),
            False,
            'FF1FF00F2',
        ),
        (
            gm.LineString([(170, 0), (-150, delta)], crs=4326),
            False,
            'FF1FF00F2',
        ),
        (
            gm.LineString([(170, 0), (-160, 0)], crs=4326),
            True,
            '0F1FF0FF2',
        ),
    )
    for line, intersects, matrix in cases:
        assert gm.intersects(line, point) is intersects
        assert gm.contains(line, point) is intersects
        assert gm.within(point, line) is intersects
        assert gm.relate(line, point) == matrix
        result = gm.intersection_all([line, point])
        assert result.is_empty is (not intersects)
        if intersects:
            assert result.geometry_type == 'Point'
            assert gm.equals(result, point)


def test_ordinary_geographic_polygons_relate_like_their_planar_twins() -> None:
    """Away from the seam and the poles a chart-linear polygon pair is its planar pair.

    The full nine-byte matrix is asserted, both ways, because a wrong boundary
    row once passed every named predicate this file checks.
    """
    from shapely import from_wkt, relate

    hexagon = gm.Polygon(
        [(math.cos(t), math.sin(t)) for t in (i * math.pi / 3 for i in range(6))]
        + [(1.0, 0.0)],
        crs=4326,
    )
    whole = gm.box(-10, -10, 10, 10, crs=4326)
    partial = gm.box(-10, -10, 0.25, 10, crs=4326)
    apart = gm.box(5, 5, 10, 10, crs=4326)
    for left, right in ((hexagon, whole), (hexagon, partial), (hexagon, apart)):
        expected = relate(from_wkt(left.to_wkt()), from_wkt(right.to_wkt()))
        assert gm.relate(left, right) == expected
        assert gm.relate(right, left) == relate(
            from_wkt(right.to_wkt()), from_wkt(left.to_wkt())
        )
    assert gm.relate(hexagon, whole) == '2FF1FF212'
    assert gm.relate(hexagon, partial) == '212101212'
    assert gm.relate(hexagon, hexagon) == '2FFF1FFF2'
    boundary_vertex = gm.Point(math.cos(math.pi / 3), math.sin(math.pi / 3), crs=4326)
    assert gm.relate(hexagon, boundary_vertex) == 'FF20F1FF2'

    index = gm.SpatialIndex([hexagon])
    assert ids(index.query(partial, predicate='intersects')) == [0]
    assert ids(index.query(partial, predicate='within')) == []
    assert ids(index.query(whole, predicate='contains')) == [0]
    assert pair_rows(
        gm.join(gm.GeometryArray([partial, apart]), [hexagon], predicate='intersects')
    ) == [(0, 0)]


def test_geographic_overlay_uses_pole_equivalence_for_binary_and_aggregates() -> None:
    """A pole has one location even when its source vertices spell it differently."""
    left = gm.Polygon([(0, 90), (10, 80), (-10, 80), (0, 90)], crs=4326)
    right = gm.Polygon([(100, 90), (90, 80), (110, 80), (100, 90)], crs=4326)
    pole = gm.Point(-120, 90, crs=4326)

    # Predicates already use the quotient. Overlay must agree for both its
    # binary dispatcher and shared all-input assembly.
    assert gm.intersects(left, right)
    for result in (
        gm.intersection(left, right),
        gm.intersection_all([left, right]),
        gm.intersection_all(gm.GeometryArray([left, right])),
    ):
        assert result.geometry_type == 'Point'
        assert gm.equals(result, pole)


def test_geographic_overlay_keeps_seam_and_polar_area_membership() -> None:
    """The quotient graph augments, rather than replaces, the seam chart."""
    seam_left = gm.Polygon(
        [(170, 0), (-170, 0), (-170, 20), (170, 20), (170, 0)], crs=4326
    )
    seam_right = gm.Polygon(
        [(175, 10), (-175, 10), (-175, 30), (175, 30), (175, 10)], crs=4326
    )
    cap = gm.Polygon(
        [(-180, 80), (-90, 80), (0, 80), (90, 80), (180, 80), (-180, 80)],
        crs=4326,
    )
    polar_hole = gm.Polygon(
        [(-180, 70), (-90, 70), (0, 70), (90, 70), (180, 70), (-180, 70)],
        holes=[[(-180, 80), (-90, 80), (0, 80), (90, 80), (180, 80), (-180, 80)]],
        crs=4326,
    )
    polar_triangle = gm.Polygon([(120, 90), (110, 85), (130, 85), (120, 90)], crs=4326)
    full_band = gm.Polygon(
        [(-180, -10), (180, -10), (180, 10), (-180, 10), (-180, -10)], crs=4326
    )
    band_window = gm.box(20, -5, 40, 5, crs=4326)

    seam_intersection = gm.intersection_all([seam_left, seam_right])
    assert gm.covers(seam_intersection, gm.Point(180, 15, crs=4326))
    assert gm.equals(gm.intersection(cap, polar_triangle), polar_triangle)
    assert gm.intersection(polar_hole, polar_triangle).is_empty
    assert gm.equals(gm.intersection(full_band, band_window), band_window)


def test_south_full_turn_ring_emits_its_polar_shell() -> None:
    cap = gm.Polygon(
        [(0, -80), (90, -80), (180, -80), (-90, -80), (0, -80)], crs=4326
    )

    result = gm.union_all([cap])

    assert result.geometry_type == 'Polygon'
    assert result.is_valid
    assert gm.contains(result, gm.Point(0, -85, crs=4326))
    assert not gm.contains(result, gm.Point(0, -70, crs=4326))


def test_intersection_emits_a_new_shell_at_the_north_pole() -> None:
    cap = gm.Polygon(
        [(0, 80), (90, 80), (180, 80), (-90, 80), (0, 80)], crs=4326
    )
    wedge = gm.Polygon([(120, 90), (110, 75), (130, 75), (120, 90)], crs=4326)

    result = gm.intersection(cap, wedge)

    assert result.geometry_type == 'Polygon'
    assert result.is_valid
    assert gm.contains(result, gm.Point(120, 85, crs=4326))
    assert not gm.contains(result, gm.Point(120, 78, crs=4326))
    assert not gm.equals(result, cap)
    assert not gm.equals(result, wedge)


def test_difference_emits_a_new_shell_across_the_seam() -> None:
    source = gm.Polygon(
        [(170, 0), (-170, 0), (-170, 20), (170, 20), (170, 0)], crs=4326
    )
    cutter = gm.Polygon(
        [(175, 10), (-175, 10), (-175, 30), (175, 30), (175, 10)], crs=4326
    )

    result = gm.difference(source, cutter)

    assert result.geometry_type == 'Polygon'
    assert result.is_valid
    assert gm.contains(result, gm.Point(172, 15, crs=4326))
    assert gm.contains(result, gm.Point(180, 5, crs=4326))
    assert not gm.contains(result, gm.Point(180, 15, crs=4326))
    assert not gm.equals(result, source)
    assert not gm.equals(result, cutter)


def test_union_merges_seam_adjacent_source_rings_into_one_shell() -> None:
    east = gm.Polygon(
        [(170, 0), (180, 0), (180, 10), (170, 10), (170, 0)], crs=4326
    )
    west = gm.Polygon(
        [(-180, 0), (-170, 0), (-170, 10), (-180, 10), (-180, 0)], crs=4326
    )

    result = gm.union_all([east, west])

    assert result.geometry_type == 'Polygon'
    assert result.is_valid
    assert gm.contains(result, gm.Point(175, 5, crs=4326))
    assert gm.contains(result, gm.Point(-175, 5, crs=4326))
    assert not gm.contains(result, gm.Point(0, 5, crs=4326))
    assert not gm.equals(result, east)
    assert not gm.equals(result, west)


def test_geographic_array_candidates_keep_periodic_envelope_selectivity() -> None:
    source = seam_box()
    distant = gm.box(-10, -1, 10, 1, crs=4326)
    index = gm.SpatialIndex([source, distant])
    queries = gm.GeometryArray([source])

    assert [row.tolist() for row in index.candidates(queries)] == [[0]]


def test_quotient_topology_ignores_z_and_m_ordinates() -> None:
    xy = seam_box()
    xyzm = gm.from_wkt(
        'POLYGON ZM ((170 -10 1 2, -170 -10 3 4, -170 10 5 6, '
        '170 10 7 8, 170 -10 1 2))',
        crs=4326,
    )
    point = gm.Point(180, 0, crs=4326)
    assert gm.relate(xy, point) == gm.relate(xyzm, point)


def test_epsg_4807_pole_index_candidate_reaches_exact_refinement() -> None:
    pole = 100.0
    for _ in range(7):
        pole = math.nextafter(pole, math.inf)
    north = gm.Point(0, pole, crs=4807)
    alternate_spelling = gm.Point(200, pole, crs=4807)
    index = gm.SpatialIndex([north])
    assert ids(index.candidates(alternate_spelling)) == [0]
    assert ids(index.query(alternate_spelling, predicate='intersects')) == [0]
