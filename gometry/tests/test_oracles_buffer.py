import math

import gometry as gm
import pytest
from gometry._types import CapStyle, JoinStyle

shapely = pytest.importorskip('shapely')


def _buffer_oracle_fixtures() -> list[tuple[str, str]]:
    """(name, WKT) buffer inputs spanning the buffer engine's caseload:
    convex/concave/holed/self-touching polygons, multipolygons, and lines
    with folds and self-crossings. Each is differentially checked against
    GEOS across distances, resolutions, and join/cap styles.
    """

    def ring(
        n: int,
        r: float,
        cx: float = 0.0,
        cy: float = 0.0,
        amp: float = 0.0,
        waves: int = 7,
    ) -> str:
        pts = []
        for i in range(n):
            t = 2 * math.pi * i / n
            rr = r + amp * math.sin(waves * t)
            pts.append(f'{cx + rr * math.cos(t)} {cy + rr * math.sin(t)}')
        pts.append(pts[0])
        return '(' + ', '.join(pts) + ')'

    fixtures = [
        ('convex_square', 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'),
        ('convex_octagon', f'POLYGON ({ring(8, 10.0)})'),
        ('concave_L', 'POLYGON ((0 0, 10 0, 10 4, 4 4, 4 10, 0 10, 0 0))'),
        (
            'concave_star',
            'POLYGON ((0 10, 2 2, 10 0, 2 -2, 0 -10, -2 -2, -10 0, -2 2, 0 10))',
        ),
        ('concave_wavy', f'POLYGON ({ring(120, 10.0, amp=3.0)})'),
        (
            'holed',
            'POLYGON ((0 0, 30 0, 30 30, 0 30, 0 0), (5 5, 5 12, 12 12, 12 5, 5 5), (18 18, 18 25, 25 25, 25 18, 18 18))',
        ),
        ('nested_holes', f'POLYGON ({ring(60, 30.0)}, {ring(40, 18.0)})'),
        ('self_touching_shell', 'POLYGON ((0 0, 10 0, 10 10, 5 5, 0 10, 0 0))'),
        (
            'multipolygon',
            'MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0)), ((20 0, 30 0, 30 10, 20 10, 20 0)))',
        ),
        (
            'multipolygon_touching_when_buffered',
            'MULTIPOLYGON (((0 0, 8 0, 8 8, 0 8, 0 0)), ((9 0, 17 0, 17 8, 9 8, 9 0)))',
        ),
        ('line_open', 'LINESTRING (0 0, 10 0, 10 10, 20 10)'),
        ('line_sharp_fold', 'LINESTRING (0 0, 10 0, 0 0.5)'),
        ('line_self_crossing', 'LINESTRING (0 0, 10 10, 10 0, 0 10)'),
        (
            'line_smooth_arc',
            f'LINESTRING ({", ".join(ring(80, 10.0)[1:-1].split(", ")[:41])})',
        ),
        ('multilinestring', 'MULTILINESTRING ((0 0, 10 0, 10 10), (0 5, 10 5))'),
        ('point', 'POINT (5 5)'),
        ('multipoint', 'MULTIPOINT ((0 0), (5 0), (2.5 4))'),
    ]
    return fixtures


def _buffer_oracle_cases() -> list[object]:
    cases = []
    style_pairs: list[tuple[CapStyle, JoinStyle]] = [
        ('round', 'round'),
        ('flat', 'miter'),
        ('square', 'bevel'),
    ]
    for name, wkt in _buffer_oracle_fixtures():
        is_puntal = name in {'point', 'multipoint'}
        for cap_style, join_style in style_pairs:
            if is_puntal and (cap_style, join_style) != ('round', 'round'):
                continue
            cases.append(
                pytest.param(
                    name,
                    wkt,
                    cap_style,
                    join_style,
                    id=f'{name}-{cap_style}-{join_style}',
                )
            )
    return cases


@pytest.mark.parametrize(
    ('name', 'wkt', 'cap_style', 'join_style'), _buffer_oracle_cases()
)
@pytest.mark.parametrize('quadrant_segments', [1, 2, 8, 16, 32, 64])
def test_buffer_matches_geos_oracle(
    name: str,
    wkt: str,
    quadrant_segments: int,
    cap_style: CapStyle,
    join_style: JoinStyle,
) -> None:
    """Differential oracle: gometry's buffer must match GEOS in area, validity,
    and coverage (low symmetric-difference) across the whole caseload —
    convex/concave/holed/self-touching polygons and folding/crossing lines, at
    every resolution and join/cap style. Robust comparison (area + symmetric
    difference), not exact coordinates, since noding order legitimately differs.
    """
    from shapely import buffer as shapely_buffer
    from shapely import is_valid
    from shapely.wkt import loads as shapely_loads

    geometry = gm.from_wkt(wkt)
    reference = shapely_loads(wkt)
    is_areal = reference.geom_type in ('Polygon', 'MultiPolygon')
    distances = [0.5, 2.0, 5.0]
    if is_areal:
        distances += [-1.0, -3.0]
    sagitta = 1.0 - math.cos(math.pi / (2.0 * quadrant_segments))
    area_tol = max(3.0 * sagitta, 0.0015)
    sym_tol = max(6.0 * sagitta, 0.004)
    for distance in distances:
        gometry_result = geometry.buffer(
            distance,
            cap_style=cap_style,
            join_style=join_style,
            quadrant_segments=quadrant_segments,
        )
        geos_join_style = 'mi' + 'tre' if join_style == 'miter' else join_style
        geos_kwargs = {
            'quad_' + 'segs': quadrant_segments,
            'cap_style': cap_style,
            'join_style': geos_join_style,
        }
        geos_result = shapely_buffer(reference, distance, **geos_kwargs)
        gometry_area = gometry_result.area
        geos_area = geos_result.area
        sliver = distance * distance * 0.05
        if gometry_area <= sliver and geos_area <= sliver:
            continue
        if geos_area == 0.0:
            assert gometry_area == pytest.approx(0.0, abs=1e-09), (
                f'{name} d={distance} q={quadrant_segments} {cap_style}/{join_style}: gometry non-empty ({gometry_area}) where GEOS empty'
            )
            continue
        gometry_geos = shapely_loads(gometry_result.to_wkt())
        assert is_valid(gometry_geos), (
            f'{name} d={distance} q={quadrant_segments} {cap_style}/{join_style}: gometry output invalid'
        )
        rel = abs(gometry_area - geos_area) / geos_area
        assert rel < area_tol, (
            f'{name} d={distance} q={quadrant_segments} {cap_style}/{join_style}: area gometry={gometry_area} geos={geos_area} rel={rel}'
        )
        sym = shapely.symmetric_difference(gometry_geos, geos_result).area
        sym_rel = sym / geos_area
        assert sym_rel < sym_tol, (
            f'{name} d={distance} q={quadrant_segments} {cap_style}/{join_style}: symmetric-difference rel={sym_rel} (sym={sym}, geos_area={geos_area})'
        )


def test_buffer_high_resolution_no_panic_on_extreme_coordinates() -> None:
    """The FFI no-panic invariant for the buffer engine under huge finite
    coordinates and high resolution — overflow/degenerate paths must fall
    through gracefully, never panic.
    """
    huge = 1e150
    for wkt in (
        f'POLYGON ((0 0, {huge} 0, {huge} {huge}, 0 {huge}, 0 0))',
        f'LINESTRING (0 0, {huge} {huge}, {huge} 0)',
        'POLYGON ((0 0, 1e-12 0, 1e-12 1e-12, 0 1e-12, 0 0))',
    ):
        geometry = gm.from_wkt(wkt)
        for quadrant_segments in (8, 64):
            result = geometry.buffer(1.0, quadrant_segments=quadrant_segments)
            assert result is not None
