"""Kernel-parity golden snapshot for the public dispatch surface.

Captures every public kernel output on a fixed input corpus so later plumbing
slices can prove behavior is unchanged within tolerance. Re-capture with:

    RECAPTURE_KERNEL_GOLDEN=1 .venv/bin/python -m pytest tests/test_kernel_parity_snapshot.py -q
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

import gometry as gm

if TYPE_CHECKING:
    from collections.abc import Callable
from tests._oracles_support import (
    error_snapshot,
    serialize_snapshot_value,
    snapshot_values_match,
)

GOLDEN_PATH = Path(__file__).resolve().parent / 'data' / 'kernel_parity_golden.json'
RECAPTURE = os.environ.get('RECAPTURE_KERNEL_GOLDEN') == '1'


def _staircase(
    x0: float, y0: float, steps: int, *, crs: int | None = 3857
) -> gm.Geometry:
    pts = [(x0, y0)]
    x, y = (x0, y0)
    for _ in range(steps):
        x += 1.0
        pts.append((x, y))
        y += 1.0
        pts.append((x, y))
    pts += [(x0, y), (x0, y0)]
    wkt = ', '.join((f'{px} {py}' for px, py in pts))
    return gm.from_wkt(f'POLYGON (({wkt}))', crs=crs)


def _empty_point() -> gm.Geometry:
    return gm.Point()


INPUT_BUILDERS: dict[str, Callable[[], gm.Geometry]] = {
    'empty_point': _empty_point,
    'point': lambda: gm.Point(1.0, 2.0, crs=3857),
    'two_vertex_line': lambda: gm.LineString([(0.0, 0.0), (3.0, 4.0)], crs=3857),
    'collinear_line': lambda: gm.LineString(
        [(0.0, 0.0), (2.0, 0.0), (5.0, 0.0)], crs=3857
    ),
    'polygon_holes': lambda: gm.Polygon(
        [(0, 0), (4, 0), (4, 4), (0, 4), (0, 0)],
        holes=[[(1, 1), (2, 1), (2, 2), (1, 2), (1, 1)]],
        crs=3857,
    ),
    'antimeridian_line': lambda: gm.LineString(
        [(170.0, -10.0), (-170.0, 10.0)], crs=4326
    ),
    'geo_polygon': lambda: gm.box(20.0, 51.0, 21.0, 52.0, crs=4326),
    'projected_polygon': lambda: gm.box(0.0, 0.0, 2.0, 3.0, crs=3857),
    'z_line': lambda: gm.LineString([(0.0, 0.0, 1.0), (3.0, 4.0, 5.0)], crs=3857),
    'zm_polygon': lambda: gm.from_wkt(
        'POLYGON ZM ((0 0 1 10, 4 0 2 20, 4 4 3 30, 0 0 1 10))'
    ),
    'multipart': lambda: gm.MultiPolygon(
        [gm.box(0, 0, 1, 1, crs=3857), gm.box(2, 2, 3, 3, crs=3857)], crs=3857
    ),
    'staircase_polygon': lambda: _staircase(0.0, 0.0, 6),
    'random_polygon_0': lambda: gm.Polygon(
        [
            (2.444326621317076, -4.74989244777333),
            (1.9192973029479568, -3.8405149928947604),
            (0.8692386662097182, -3.84051499289476),
            (0.34420934784059876, -4.74989244777333),
            (0.8692386662097176, -5.6592699026519),
            (1.9192973029479568, -5.659269902651901),
        ],
        crs=3857,
    ),
    'random_line_1': lambda: gm.LineString(
        [
            *[
                (-1.6232852254132093, -3.9750482382849253),
                (-2.0868296731887934, -2.701470335107945),
                (-3.260565417678661, -2.023813687069234),
                (-4.595288459416237, -2.2591613711008387),
                (-5.466467053378228, -3.297391590246214),
                (-5.466467053378228, -4.652704886323636),
                (-4.5952884594162375, -5.690935105469011),
                (-3.260565417678662, -5.9262827895006165),
                (-2.0868296731887943, -5.248626141461906),
            ],
            (-1.6232852254132093, -3.9750482382849253),
        ],
        crs=3857,
    ),
    'random_polygon_2': lambda: gm.Polygon(
        [
            (-3.5710172348296982, -0.7807818031472955),
            (-3.8508144542677685, -0.2961588032640594),
            (-4.410408893143909, -0.29615880326405936),
            (-4.690206112581979, -0.7807818031472954),
            (-4.410408893143909, -1.2654048030305316),
            (-3.8508144542677685, -1.2654048030305316),
        ],
        crs=3857,
    ),
    'random_line_3': lambda: gm.LineString(
        [
            *[
                (-1.050900940215378, 1.0201872904998037),
                (-1.4304915211489437, 2.063103840252438),
                (-2.391648612451435, 2.618028145649449),
                (-3.4846361290312164, 2.425304956898893),
                (-4.198033064677433, 1.5751115958968152),
                (-4.198033064677433, 0.46526298510279274),
                (-3.4846361290312178, -0.38493037589928525),
                (-2.391648612451436, -0.577653564649842),
                (-1.4304915211489442, -0.02272925925283098),
            ],
            (-1.050900940215378, 1.0201872904998037),
        ],
        crs=3857,
    ),
    'random_polygon_4': lambda: gm.Polygon(
        [
            (0.25157962607791307, -0.5079095371614635),
            (-0.47836198015498393, 0.4967688926571845),
            (-1.6594323088425034, 0.11301588023033693),
            (-1.6594323088425036, -1.1288349545532637),
            (-0.4783619801549842, -1.5125879669801114),
        ],
        crs=3857,
    ),
}


def _matching_crs(geom: gm.Geometry) -> Any:
    return geom.crs if geom.crs is not None else 3857


def _secondary(geom: gm.Geometry) -> gm.Geometry:
    crs = _matching_crs(geom)
    if _is_geo(crs):
        return gm.box(20.5, 51.5, 21.5, 52.5, crs=4326)
    return gm.box(1.0, 1.0, 3.0, 4.0, crs=crs)


def _probe(geom: gm.Geometry) -> gm.Geometry:
    crs = _matching_crs(geom)
    if _is_geo(crs):
        return gm.Point(20.75, 51.75, crs=4326)
    return gm.Point(1.0, 1.0, crs=crs)


def _linework(geom: gm.Geometry) -> gm.Geometry:
    if geom.geometry_type in {'LineString', 'MultiLineString'}:
        return geom
    crs = _matching_crs(geom)
    if _is_geo(crs):
        return gm.LineString([(20.0, 51.0), (21.0, 52.0)], crs=4326)
    return gm.LineString([(0.0, 0.0), (3.0, 4.0), (6.0, 4.0)], crs=crs)


def _sites(geom: gm.Geometry) -> gm.Geometry:
    crs = _matching_crs(geom)
    if _is_geo(crs):
        return gm.MultiPoint(
            [(20.0, 51.0), (21.0, 51.0), (20.0, 52.0), (21.0, 52.0)], crs=4326
        )
    return gm.MultiPoint([(0.0, 0.0), (4.0, 0.0), (0.0, 4.0), (4.0, 4.0)], crs=crs)


def _distance_scale(geom: gm.Geometry) -> float:
    crs = _matching_crs(geom)
    return 500.0 if _is_geo(crs) else 1.0


def _is_geo(crs: Any) -> bool:
    return crs in (4326, 'EPSG:4326')


def _segmentize_length(geom: gm.Geometry) -> float:
    """A max segment length in the units ``segmentize`` actually measures in.

    ``segmentize`` is CRS-aware: on a geographic CRS the argument is a
    real-world distance measured along the ellipsoid, exactly like ``length``,
    not a span in degrees. A bare ``0.5`` therefore means half a METRE on the
    ~131 km geographic fixture — 262k vertices instead of the 3 this snapshot
    wants. 50 km is the geographic counterpart of the 0.5 planar units used
    for the projected fixtures.
    """
    return 50_000.0 if _is_geo(_matching_crs(geom)) else 0.5


def _binary(
    op: str, *, distance: float | None = None
) -> Callable[[gm.Geometry], object]:
    fn = getattr(gm, op)

    def runner(geom: gm.Geometry) -> object:
        other = _secondary(geom)
        if distance is None:
            return fn(geom, other)
        return fn(geom, other, distance)

    return runner


OP_RUNNERS: dict[str, Callable[[gm.Geometry], object]] = {
    'intersects': _binary('intersects'),
    'contains': _binary('contains'),
    'within': _binary('within'),
    'covers': _binary('covers'),
    'disjoint': _binary('disjoint'),
    'touches': _binary('touches'),
    'crosses': _binary('crosses'),
    'overlaps': _binary('overlaps'),
    'equals': _binary('equals'),
    'relate': _binary('relate'),
    'dwithin': lambda geom: gm.dwithin(geom, _secondary(geom), _distance_scale(geom)),
    'area': lambda geom: geom.area,
    'length': lambda geom: geom.length,
    'distance': lambda geom: gm.distance(geom, _probe(geom)),
    'distance_3d': lambda geom: gm.distance_3d(geom, _probe(geom).force_3d(z=0.0)),
    'hausdorff_distance': _binary('hausdorff_distance'),
    'frechet_distance': _binary('frechet_distance'),
    'minimum_clearance': lambda geom: geom.minimum_clearance(),
    'minimum_bounding_radius': lambda geom: geom.minimum_bounding_radius(),
    'buffer': lambda geom: geom.buffer(_distance_scale(geom)),
    'centroid': lambda geom: geom.centroid(),
    'envelope': lambda geom: geom.envelope(),
    'convex_hull': lambda geom: geom.convex_hull(),
    'concave_hull': lambda geom: _sites(geom).concave_hull(concavity=1.0),
    'boundary': lambda geom: geom.boundary(),
    'point_on_surface': lambda geom: geom.point_on_surface(),
    'simplify': lambda geom: geom.simplify(0.5),
    'smooth': lambda geom: _linework(geom).smooth(iterations=1, method='chaikin'),
    'offset_curve': lambda geom: _linework(geom).offset_curve(
        _distance_scale(geom) * 0.1, join_style='round'
    ),
    'voronoi_polygons': lambda geom: _sites(geom).voronoi_polygons(clip='envelope'),
    'triangulate': lambda geom: _sites(geom).triangulate(method='delaunay'),
    'polylabel': lambda geom: geom.polylabel(tolerance=0.01),
    'maximum_inscribed_circle': lambda geom: geom.maximum_inscribed_circle(
        tolerance=0.01
    ),
    'maximum_inscribed_radius': lambda geom: geom.maximum_inscribed_radius(
        tolerance=0.01
    ),
    'minimum_bounding_circle': lambda geom: geom.minimum_bounding_circle(),
    'minimum_rotated_rectangle': lambda geom: geom.minimum_rotated_rectangle(),
    'union': _binary('union'),
    'intersection': _binary('intersection'),
    'difference': _binary('difference'),
    'symmetric_difference': _binary('symmetric_difference'),
    'union_all': lambda geom: gm.union_all([geom, _secondary(geom)]),
    'affine_transform': lambda geom: geom.affine_transform((
        1.0,
        0.2,
        0.0,
        0.9,
        1.0,
        2.0,
    )),
    'translate': lambda geom: geom.translate(1.0, 2.0),
    'rotate': lambda geom: geom.rotate(45.0, origin=(0.0, 0.0)),
    'scale': lambda geom: geom.scale(1.5, 0.75, origin=(0.0, 0.0)),
    'snap': lambda geom: gm.snap(geom, _probe(geom), 0.5),
    'segmentize': lambda geom: _linework(geom).segmentize(_segmentize_length(geom)),
    'reverse': lambda geom: _linework(geom).reverse(),
    'normalize': lambda geom: geom.normalize(),
    'orient_polygons': lambda geom: geom.orient_polygons(),
    'force_2d': lambda geom: geom.force_2d(),
    'force_3d': lambda geom: geom.force_3d(z=0.0),
    'set_z': lambda geom: geom.set_z(7.0),
    'set_m': lambda geom: geom.set_m(8.0),
    'line_interpolate': lambda geom: _linework(geom).line_interpolate(1.0),
    'line_locate': lambda geom: _linework(geom).line_locate(_probe(geom)),
    'line_substring': lambda geom: _linework(geom).line_substring(0.5, 2.5),
    'line_merge': lambda geom: _linework(geom).line_merge(),
    'shared_paths': lambda geom: gm.shared_paths(_linework(geom), _secondary(geom)),
    'split': lambda geom: gm.split(
        _linework(geom), gm.MultiPoint([_probe(geom)], crs=_matching_crs(geom))
    ),
    'is_valid': lambda geom: geom.is_valid,
    'is_simple': lambda geom: geom.is_simple,
    'repair': lambda geom: geom.repair(),
    'get_coordinates': gm.get_coordinates,
    'parts': gm.parts,
    'rings': gm.rings,
    'to_wkt': lambda geom: geom.to_wkt(),
    'to_wkb': lambda geom: geom.to_wkb(),
    'to_geojson': lambda geom: geom.to_geojson(),
    'from_wkt': lambda geom: gm.from_wkt(geom.to_wkt(), crs=geom.crs),
    'from_wkb': lambda geom: gm.from_wkb(geom.to_wkb()),
    'h3_cell': lambda geom: gm.H3Cell(geom, resolution=7),
    'h3_parent': lambda geom: gm.H3Cell(geom, resolution=7).parent(6),
    'h3_children': lambda geom: gm.H3Cell(geom, resolution=7).children(8),
    'h3_boundary': lambda geom: gm.H3Cell(geom, resolution=7).polygon,
    'h3_cover': lambda geom: gm.h3_cover(geom, resolution=7),
    's2_cell': lambda geom: gm.S2Cell(geom, level=12),
    's2_parent': lambda geom: gm.S2Cell(geom, level=12).parent(10),
    's2_children': lambda geom: gm.S2Cell(geom, level=12).children(13),
    's2_boundary': lambda geom: gm.S2Cell(geom, level=12).polygon,
    's2_cover': lambda geom: gm.s2_cover(geom, target_cells=8),
    'geohash_cell': lambda geom: gm.GeohashCell(geom, precision=5),
    'geohash_parent': lambda geom: gm.GeohashCell(geom, precision=5).parent(4),
    'geohash_children': lambda geom: gm.GeohashCell(geom, precision=5).children(6),
    'geohash_boundary': lambda geom: gm.GeohashCell(geom, precision=5).polygon,
    'geohash_cover': lambda geom: gm.geohash_cover(geom, precision=5),
    'tiles_tile': lambda geom: gm.Tile(geom, zoom=10),
    'tiles_parent': lambda geom: gm.Tile(geom, zoom=10).parent(9),
    'tiles_children': lambda geom: gm.Tile(geom, zoom=10).children(11),
    'tiles_boundary': lambda geom: gm.Tile(geom, zoom=10).polygon,
    'tiles_cover': lambda geom: gm.tile_cover(geom, zoom=10),
}
OP_NAMES: tuple[str, ...] = (
    'intersects',
    'contains',
    'within',
    'covers',
    'disjoint',
    'touches',
    'crosses',
    'overlaps',
    'equals',
    'relate',
    'dwithin',
    'area',
    'length',
    'distance',
    'distance_3d',
    'hausdorff_distance',
    'frechet_distance',
    'minimum_clearance',
    'minimum_bounding_radius',
    'buffer',
    'centroid',
    'envelope',
    'convex_hull',
    'concave_hull',
    'boundary',
    'point_on_surface',
    'simplify',
    'smooth',
    'offset_curve',
    'voronoi_polygons',
    'triangulate',
    'polylabel',
    'maximum_inscribed_circle',
    'maximum_inscribed_radius',
    'minimum_bounding_circle',
    'minimum_rotated_rectangle',
    'union',
    'intersection',
    'difference',
    'symmetric_difference',
    'union_all',
    'affine_transform',
    'translate',
    'rotate',
    'scale',
    'snap',
    'segmentize',
    'reverse',
    'normalize',
    'orient_polygons',
    'force_2d',
    'force_3d',
    'set_z',
    'set_m',
    'line_interpolate',
    'line_locate',
    'line_substring',
    'line_merge',
    'shared_paths',
    'split',
    'is_valid',
    'is_simple',
    'repair',
    'get_coordinates',
    'parts',
    'rings',
    'to_wkt',
    'to_wkb',
    'to_geojson',
    'from_wkt',
    'from_wkb',
    'h3_cell',
    'h3_parent',
    'h3_children',
    'h3_boundary',
    'h3_cover',
    's2_cell',
    's2_parent',
    's2_children',
    's2_boundary',
    's2_cover',
    'geohash_cell',
    'geohash_parent',
    'geohash_children',
    'geohash_boundary',
    'geohash_cover',
    'tiles_tile',
    'tiles_parent',
    'tiles_children',
    'tiles_boundary',
    'tiles_cover',
)
assert tuple(OP_RUNNERS.keys()) == OP_NAMES
OP_COUNT = len(OP_NAMES)


def _entry_key(op: str, input_name: str) -> str:
    return f'{op}:{input_name}'


def _run_case(op: str, input_name: str) -> object:
    geom = INPUT_BUILDERS[input_name]()
    return OP_RUNNERS[op](geom)


def _capture_entry(op: str, input_name: str) -> object:
    try:
        return serialize_snapshot_value(_run_case(op, input_name))
    except Exception as exc:
        return error_snapshot(exc)


def build_golden_payload() -> dict[str, object]:
    entries: dict[str, object] = {}
    for op in OP_NAMES:
        for input_name in INPUT_BUILDERS:
            entries[_entry_key(op, input_name)] = _capture_entry(op, input_name)
    return {
        'version': 1,
        'op_count': OP_COUNT,
        'ops': list(OP_NAMES),
        'inputs': list(INPUT_BUILDERS),
        'entries': entries,
    }


def write_golden(path: Path = GOLDEN_PATH) -> dict[str, object]:
    payload = build_golden_payload()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n', encoding='utf-8'
    )
    return payload


def load_golden(path: Path = GOLDEN_PATH) -> dict[str, object]:
    return json.loads(path.read_text(encoding='utf-8'))


def test_kernel_parity_op_registry_is_auditable() -> None:
    """The explicit registry stays in sync with the runner table."""
    assert OP_COUNT == len(OP_RUNNERS) == len(OP_NAMES)
    print(f'kernel parity ops: {OP_COUNT}')


def test_kernel_parity_snapshot() -> None:
    if RECAPTURE:
        payload = write_golden()
    else:
        if not GOLDEN_PATH.is_file():
            raise AssertionError(
                f'missing kernel golden {GOLDEN_PATH}; set RECAPTURE_KERNEL_GOLDEN=1 '
                'to recapture intentionally (silent recapture is forbidden)'
            )
        payload = load_golden()
    assert payload['op_count'] == OP_COUNT
    assert payload['ops'] == list(OP_NAMES)
    assert payload['inputs'] == list(INPUT_BUILDERS)
    entries = payload['entries']
    assert isinstance(entries, dict)
    expected_keys = {_entry_key(op, name) for op in OP_NAMES for name in INPUT_BUILDERS}
    assert set(entries) == expected_keys
    mismatches: list[str] = []
    for key in sorted(expected_keys):
        op, input_name = key.split(':', 1)
        actual = _capture_entry(op, input_name)
        try:
            snapshot_values_match(entries[key], actual)
        except AssertionError as exc:
            mismatches.append(f'{key}: {exc}')
    if mismatches:
        sample = '\n'.join(mismatches[:20])
        raise AssertionError(
            f'{len(mismatches)} kernel parity mismatches (first 20):\n{sample}'
        )
