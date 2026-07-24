"""Characterization tests for SVG/HTML reprs and docstring exposure."""

import re

import gometry as gm
import pytest

GREEN = '#22c55e'
RED = '#ef4444'


def _viewbox(svg: str) -> tuple[float, float, float, float]:
    match = re.search('viewBox="([^"]+)"', svg)
    assert match, f'no viewBox in {svg!r}'
    x, y, w, h = (float(v) for v in match.group(1).split())
    return (x, y, w, h)


def test_valid_geometry_renders_green():
    svg = gm.box(0, 0, 1, 1)._repr_svg_()
    assert svg.startswith('<svg')
    assert 'gometry-geom' in svg
    assert GREEN in svg
    assert RED not in svg


def test_invalid_self_intersecting_polygon_renders_red():
    bowtie = gm.from_wkt('POLYGON((0 0,1 1,1 0,0 1,0 0))')
    svg = bowtie._repr_svg_()
    assert RED in svg
    assert GREEN not in svg


def test_viewbox_derives_from_bounds():
    svg = gm.box(10, 20, 12, 24)._repr_svg_()
    minx, miny, w, h = _viewbox(svg)
    assert minx == pytest.approx(9.9)
    assert miny == pytest.approx(19.8)
    assert w == pytest.approx(2.2)
    assert h == pytest.approx(4.4)


def test_flipped_content_stays_inside_the_viewbox():
    svg = gm.box(20.85, 52.1, 21.25, 52.35)._repr_svg_()
    minx, miny, w, h = _viewbox(svg)
    flip_match = re.search('matrix\\(1 0 0 -1 0 ([-\\d.]+)\\)', svg)
    assert flip_match is not None
    flip = float(flip_match.group(1))
    for x, y in re.findall('[ML]([-\\d.]+) ([-\\d.]+)', svg):
        tx, ty = (float(x), flip - float(y))
        assert minx <= tx <= minx + w
        assert miny <= ty <= miny + h


@pytest.mark.parametrize(
    'wkt',
    [
        'POINT(0 0)',
        'MULTIPOINT(0 0,1 1)',
        'LINESTRING(0 0,1 1,2 0)',
        'MULTILINESTRING((0 0,1 1),(2 2,3 3))',
        'POLYGON((0 0,2 0,2 2,0 2,0 0),(0.5 0.5,1.5 0.5,1.5 1.5,0.5 1.5,0.5 0.5))',
        'MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)),((2 2,3 2,3 3,2 2)))',
        'GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(0 0,1 1))',
    ],
)
def test_each_family_renders(wkt):
    svg = gm.from_wkt(wkt)._repr_svg_()
    assert svg.startswith('<svg')
    assert 'gometry-geom' in svg


def test_point_uses_circle():
    assert '<circle' in gm.from_wkt('POINT(3 4)')._repr_svg_()


def test_polygon_uses_evenodd_path():
    svg = gm.box(0, 0, 1, 1)._repr_svg_()
    assert '<path' in svg
    assert 'fill-rule="evenodd"' in svg


def test_empty_geometry_does_not_raise():
    svg = gm.from_wkt('GEOMETRYCOLLECTION EMPTY')._repr_svg_()
    assert svg.startswith('<svg')
    assert 'gometry-geom' in svg


def test_repr_html_contains_svg_and_header():
    html = gm.box(0, 0, 1, 1)._repr_html_()
    assert '<svg' in html
    assert '&lt;POLYGON' in html or '<POLYGON' in html


def test_array_repr_html_grids_and_caption():
    arr = gm.GeometryArray([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)])
    html = arr._repr_html_()
    assert '<svg' in html
    assert '2 geometries' in html


def test_array_repr_html_is_bounded():
    arr = gm.GeometryArray([gm.box(i, 0, i + 1, 1) for i in range(50)])
    html = arr._repr_html_()
    assert html.count('<svg') <= 12
    assert '50 geometries' in html
    assert 'showing first 12' in html


def test_docstrings_are_populated():
    assert gm.Point.__doc__
    assert gm.box.__doc__
    assert gm.intersects.__doc__
    assert gm.area.__doc__
    assert gm.Geometry.buffer.__doc__
    assert gm.from_wkb.__doc__
    assert gm.crs_transform.__doc__
    assert gm.Geometry.area.__doc__
    assert gm.contains.__doc__


def test_repr_shows_wkt_and_frame() -> None:
    assert repr(gm.Point(1, 2)) == '<POINT (1 2)>'
    assert repr(gm.Point(1, 2, crs=4326)) == '<POINT (1 2) EPSG:4326>'
    assert (
        repr(gm.Point(1, 2, crs=4326, epoch=2020.5))
        == '<POINT (1 2) EPSG:4326 @2020.5>'
    )
    long = gm.LineString([(i, i % 7) for i in range(60)])
    assert repr(long).endswith('...>') and len(repr(long)) < 140
    points = gm.GeometryArray([gm.Point(1, 2, crs=4326), gm.Point(3, 4, crs=4326)])
    assert repr(points) == '<GeometryArray[Point] len=2 EPSG:4326>'
    missing = gm.GeometryArray([
        gm.Point(1, 2, crs=4326),
        None,
        gm.Point(3, 4, crs=4326),
    ])
    assert repr(missing) == '<GeometryArray[Point] len=3 missing=1 EPSG:4326>'
    mixed = gm.GeometryArray([gm.Point(1, 2), gm.box(0, 0, 1, 1)])
    assert repr(mixed) == '<GeometryArray len=2>'


def test_groups_repr_uses_public_row_reprs_not_rust_debug() -> None:
    """Geometry/cell Groups preview each row via Python __repr__, never Debug."""
    cell_groups = gm.h3_cover(gm.points([-0.1], [51.5], crs=4326), 7)
    cell_text = repr(cell_groups)
    assert '{' not in cell_text
    assert 'PyCellArray' not in cell_text
    assert 'CellStorage' not in cell_text
    assert cell_text.startswith('Groups([')
    assert '<CellArray[' in cell_text
    assert 'len=' in cell_text

    geom_groups = gm.GeometryArray([gm.MultiPoint([(0, 0), (1, 1)])]).parts
    geom_text = repr(geom_groups)
    assert '{' not in geom_text
    assert 'PyGeometryArray' not in geom_text
    assert 'CoordSeq' not in geom_text
    assert geom_text.startswith('Groups([')
    assert '<GeometryArray[' in geom_text
    assert geom_text.endswith(', len=1)')

    # Int64 Groups keep nested-list preview style.
    idx = gm.SpatialIndex(gm.points([0.0, 1.0], [0.0, 1.0]))
    int_groups = idx.query(gm.points([0.0], [0.0]), predicate='intersects')
    int_text = repr(int_groups)
    assert int_text.startswith('Groups([[')
    assert '{' not in int_text
