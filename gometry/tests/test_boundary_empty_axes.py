"""Boundary results preserve declared axes when their coordinate set is empty."""

import gometry as gm
import pytest

AXES = (
    ('XY', ''),
    ('XYZ', ' Z'),
    ('XYM', ' M'),
    ('XYZM', ' ZM'),
)


def coordinate(axes: str, x: int, y: int, value: int) -> str:
    if axes == 'XY':
        return f'{x} {y}'
    if axes == 'XYZ':
        return f'{x} {y} {value}'
    if axes == 'XYM':
        return f'{x} {y} {value}'
    return f'{x} {y} {value} {value + 10}'


@pytest.mark.parametrize(('axes', 'tag'), AXES, ids=[axes for axes, _ in AXES])
def test_closed_line_boundary_preserves_declared_axes(axes: str, tag: str) -> None:
    points = ', '.join([
        coordinate(axes, 0, 0, 1),
        coordinate(axes, 1, 0, 2),
        coordinate(axes, 0, 0, 1),
    ])
    boundary = gm.from_wkt(f'LINESTRING{tag} ({points})').boundary()

    assert boundary.geometry_type == 'MultiPoint'
    assert boundary.coordinate_axes == axes
    assert boundary.is_empty
    assert len(boundary.coords) == 0


@pytest.mark.parametrize(('axes', 'tag'), AXES, ids=[axes for axes, _ in AXES])
def test_cancelled_multiline_boundary_preserves_declared_axes(
    axes: str, tag: str
) -> None:
    forward = ', '.join([coordinate(axes, 0, 0, 1), coordinate(axes, 1, 0, 2)])
    reverse = ', '.join([coordinate(axes, 1, 0, 2), coordinate(axes, 0, 0, 1)])
    boundary = gm.from_wkt(
        f'MULTILINESTRING{tag} (({forward}), ({reverse}))'
    ).boundary()

    assert boundary.geometry_type == 'MultiPoint'
    assert boundary.coordinate_axes == axes
    assert boundary.is_empty
    assert len(boundary.coords) == 0


@pytest.mark.parametrize(('axes', 'tag'), AXES, ids=[axes for axes, _ in AXES])
@pytest.mark.parametrize(
    ('kind', 'boundary_kind'),
    [('MULTILINESTRING', 'MultiPoint'), ('MULTIPOLYGON', 'MultiLineString')],
)
def test_sparse_empty_multipart_matches_typed_empty_boundary(
    axes: str,
    tag: str,
    kind: str,
    boundary_kind: str,
) -> None:
    typed_boundary = gm.from_wkt(f'{kind}{tag} EMPTY').boundary()
    for members in ('EMPTY', 'EMPTY, EMPTY'):
        sparse_boundary = gm.from_wkt(f'{kind}{tag} ({members})').boundary()

        assert sparse_boundary.geometry_type == boundary_kind
        assert sparse_boundary.geometry_type == typed_boundary.geometry_type
        assert sparse_boundary.coordinate_axes == axes
        assert sparse_boundary.coordinate_axes == typed_boundary.coordinate_axes
        assert sparse_boundary.is_empty
        assert len(sparse_boundary.coords) == 0


@pytest.mark.parametrize(('axes', 'tag'), AXES, ids=[axes for axes, _ in AXES])
def test_mixed_empty_and_nonempty_multipart_keeps_output_structure(
    axes: str, tag: str
) -> None:
    line = ', '.join([coordinate(axes, 0, 0, 1), coordinate(axes, 1, 0, 2)])
    multiline_boundary = gm.from_wkt(
        f'MULTILINESTRING{tag} (EMPTY, ({line}))'
    ).boundary()
    assert multiline_boundary.geometry_type == 'MultiPoint'
    assert multiline_boundary.coordinate_axes == axes
    assert not multiline_boundary.is_empty
    assert len(multiline_boundary.coords) == 2

    ring = ', '.join([
        coordinate(axes, 0, 0, 1),
        coordinate(axes, 1, 0, 2),
        coordinate(axes, 1, 1, 3),
        coordinate(axes, 0, 0, 1),
    ])
    multipolygon_boundary = gm.from_wkt(
        f'MULTIPOLYGON{tag} (EMPTY, (({ring})))'
    ).boundary()
    assert multipolygon_boundary.geometry_type == 'LineString'
    assert multipolygon_boundary.coordinate_axes == axes
    assert not multipolygon_boundary.is_empty
    assert len(multipolygon_boundary.coords) == 4


@pytest.mark.parametrize(('axes', 'tag'), AXES, ids=[axes for axes, _ in AXES])
def test_open_line_and_ring_boundaries_preserve_emitted_axes(
    axes: str, tag: str
) -> None:
    line = ', '.join([coordinate(axes, 0, 0, 1), coordinate(axes, 1, 0, 2)])
    line_boundary = gm.from_wkt(f'LINESTRING{tag} ({line})').boundary()
    assert line_boundary.geometry_type == 'MultiPoint'
    assert line_boundary.coordinate_axes == axes
    assert not line_boundary.is_empty
    assert len(line_boundary.coords) == 2

    ring = ', '.join([
        coordinate(axes, 0, 0, 1),
        coordinate(axes, 1, 0, 2),
        coordinate(axes, 1, 1, 3),
        coordinate(axes, 0, 0, 1),
    ])
    ring_boundary = gm.from_wkt(f'POLYGON{tag} (({ring}))').boundary()
    assert ring_boundary.geometry_type == 'LineString'
    assert ring_boundary.coordinate_axes == axes
    assert not ring_boundary.is_empty
    assert len(ring_boundary.coords) == 4


@pytest.mark.parametrize(('axes', 'tag'), AXES, ids=[axes for axes, _ in AXES])
def test_array_boundary_preserves_axes_and_missing_rows(axes: str, tag: str) -> None:
    empty = gm.from_wkt(f'MULTILINESTRING{tag} (EMPTY)')
    points = ', '.join([
        coordinate(axes, 0, 0, 1),
        coordinate(axes, 1, 0, 2),
        coordinate(axes, 0, 0, 1),
    ])
    closed = gm.from_wkt(f'MULTILINESTRING{tag} (({points}))')
    result = gm.GeometryArray([empty, None, closed]).boundary()

    assert result.coordinate_axes == [axes, None, axes]
    assert result.is_missing.tolist() == [False, True, False]
    assert result[0].geometry_type == 'MultiPoint'
    assert result[0].is_empty
    assert result[2].geometry_type == 'MultiPoint'
    assert result[2].coordinate_axes == axes
    assert result[2].is_empty


@pytest.mark.parametrize(('axes', 'tag'), AXES, ids=[axes for axes, _ in AXES])
def test_geographic_boundary_preserves_crs_and_declared_axes(
    axes: str, tag: str
) -> None:
    source = gm.from_wkt(f'MULTILINESTRING{tag} (EMPTY)', crs=4326)
    boundary = source.boundary()

    assert boundary.crs == source.crs
    assert boundary.geometry_type == 'MultiPoint'
    assert boundary.coordinate_axes == axes
    assert boundary.is_empty

    array = gm.GeometryArray([source, None], crs=4326)
    result = array.boundary()
    assert result.crs == array.crs
    assert result.coordinate_axes == [axes, None]
    assert result.is_missing.tolist() == [False, True]


def test_point_replace_preserves_typed_empty_axes() -> None:
    empty_z = gm.from_wkt('POINT Z EMPTY')
    assert empty_z.to_wkt() == 'POINT Z EMPTY'
    assert empty_z.__replace__().to_wkt() == 'POINT Z EMPTY'
    assert empty_z.__replace__(crs=4326).to_wkt() == 'POINT Z EMPTY'
    assert empty_z.__replace__(crs=4326).crs == 'EPSG:4326'

    empty_m = gm.from_wkt('POINT M EMPTY')
    assert empty_m.__replace__().to_wkt() == 'POINT M EMPTY'
    empty_zm = gm.from_wkt('POINT ZM EMPTY')
    assert empty_zm.__replace__().to_wkt() == 'POINT ZM EMPTY'

    # Axis-aware Z/M edits on empties.
    cleared = empty_z.__replace__(z=None)
    assert cleared.to_wkt() == 'POINT EMPTY'
    promoted = gm.from_wkt('POINT EMPTY').__replace__(z=1.0)
    assert promoted.to_wkt() == 'POINT Z EMPTY'

    # Materialization that lacks required ordinates raises rather than flattens.
    with pytest.raises(gm.InvalidGeometryError, match='requires z'):
        empty_z.__replace__(x=1.0, y=2.0)
    material = empty_z.__replace__(x=1.0, y=2.0, z=3.0)
    assert material.to_wkt() == 'POINT Z (1 2 3)'
