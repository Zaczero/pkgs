"""R14-D grids + CRS correctness regression suite (deterministic fixtures)."""

from __future__ import annotations

import json
import math

import gometry as gm
import pytest

# ---------------------------------------------------------------------------
# A1 - S2 within/overlap refine true cells (rectangles prune only)
# ---------------------------------------------------------------------------


def test_a1_s2_overlap_no_unrelated_face_cells():
    """Lon/lat rect proxy must not emit unrelated cells (audit: a7c must not pull 72c)."""
    # Cell a7c center is near the Pacific cube edge; a tiny box there used to
    # also emit the unrelated face-neighbour 72c under pure rect classification.
    a7c = gm.S2Cell('a7c')
    ctr = a7c.center
    box = gm.box(ctr.x - 0.01, ctr.y - 0.01, ctr.x + 0.01, ctr.y + 0.01, crs=4326)
    tokens = {c.token for c in gm.s2_cover(box, level=3, cell_rule='overlap')}
    assert 'a7c' in tokens
    assert '72c' not in tokens
    assert not any(t.startswith('72') for t in tokens)


def test_a1_s2_within_subset_of_overlap():
    box = gm.box(10.0, 10.0, 20.0, 20.0, crs=4326)
    within = {c.token for c in gm.s2_cover(box, level=6, cell_rule='within')}
    overlap = {c.token for c in gm.s2_cover(box, level=6, cell_rule='overlap')}
    assert within <= overlap


# ---------------------------------------------------------------------------
# A2 - S2 bbox covers antimeridian windows separately
# ---------------------------------------------------------------------------


def test_a2_s2_bbox_antimeridian_not_full_equator():
    b1 = gm.box(179.5, -1.0, 180.0, 1.0, crs=4326)
    b2 = gm.box(-180.0, -1.0, -179.5, 1.0, crs=4326)
    band = gm.union(b1, b2)
    n_overlap = len(list(gm.s2_cover(band, level=8, cell_rule='overlap')))
    n_bbox = len(list(gm.s2_cover(band, level=8, cell_rule='bbox')))
    # Pre-fix: bbox ballooned to ~8720 cells (~270x overlap). After fix the
    # bbox cover is a small multiple of the overlap cover, not two orders larger.
    assert n_overlap > 0
    assert n_bbox <= max(4 * n_overlap, n_overlap + 64)


# ---------------------------------------------------------------------------
# A3 - H3 non-areal cell_rule refinement
# ---------------------------------------------------------------------------


def test_a3_h3_exact_center_under_center_rule():
    cov = list(
        gm.h3_cover(
            gm.box(-122.42, 37.77, -122.41, 37.78, crs=4326),
            resolution=9,
            cell_rule='overlap',
        )
    )
    cell = cov[0]
    pt = gm.Point(cell.center.x, cell.center.y, crs=4326)
    center_cov = list(gm.h3_cover(pt, resolution=cell.resolution, cell_rule='center'))
    assert len(center_cov) == 1
    assert center_cov[0].id == cell.id


def test_a3_h3_shared_vertex_overlap_covers_multiple_cells():
    cov = list(
        gm.h3_cover(
            gm.box(-122.42, 37.77, -122.41, 37.78, crs=4326),
            resolution=9,
            cell_rule='overlap',
        )
    )
    cell = cov[0]
    poly = cell.polygon
    if poly.crs is None:
        poly = poly.set_crs(4326)
    vertex = next(iter(poly.exterior.coords))
    vpt = gm.Point(vertex[0], vertex[1], crs=4326)
    tokens = [
        c.token
        for c in gm.h3_cover(vpt, resolution=cell.resolution, cell_rule='overlap')
    ]
    # Three hexes meet at a non-pentagon vertex under closed-set overlap.
    assert len(tokens) >= 2, tokens


# ---------------------------------------------------------------------------
# A4 - H3/S2 bounding cell seam
# ---------------------------------------------------------------------------


def test_a4_h3_bounding_cell_seam_line():
    line = gm.LineString([(179.9, 0.0), (-179.9, 0.0)], crs=4326)
    cell = gm.h3_bounding_cell(line)
    assert cell is not None
    poly = cell.polygon
    if poly.crs is None:
        poly = poly.set_crs(4326)
    assert gm.covers(poly, gm.Point(179.9, 0.0, crs=4326))
    assert gm.covers(poly, gm.Point(-179.9, 0.0, crs=4326))


def test_a4_s2_bounding_cell_seam_line():
    line = gm.LineString([(179.9, 0.0), (-179.9, 0.0)], crs=4326)
    cell = gm.s2_bounding_cell(line)
    assert cell is not None
    # Contains both endpoints hierarchically.
    e = gm.s2_bounding_cell(gm.Point(179.9, 0.0, crs=4326))
    w = gm.s2_bounding_cell(gm.Point(-179.9, 0.0, crs=4326))
    assert cell.contains(e) or cell.id == e.id or (e.parent() and cell.contains(e))
    # At least the face-level ancestor relationship holds.
    cur = e
    found = False
    for _ in range(31):
        if cur.id == cell.id:
            found = True
            break
        if cur.level == 0:
            break
        cur = cur.parent()
    assert found, (cell.token, e.token, w.token)


def test_a4_h3_bounding_cell_near_antimeridian_line():
    line = gm.LineString([(179.5, 0.0), (179.9, 0.0)], crs=4326)
    cell = gm.h3_bounding_cell(line)
    assert cell.resolution >= 0


# ---------------------------------------------------------------------------
# A5 - signed-zero center cover identity
# ---------------------------------------------------------------------------


def test_a5_signed_zero_point_identity_in_covers():
    """-0.0 and +0.0 must match for discrete point center membership."""
    # Two points that are IEEE signed zeros - covers_point identity path.
    pt_neg = gm.Point(math.copysign(0.0, -1.0), math.copysign(0.0, -1.0), crs=4326)
    pt_pos = gm.Point(0.0, 0.0, crs=4326)
    assert pt_neg.x == 0.0 and math.copysign(1.0, pt_neg.x) < 0
    # Overlap cover must agree (same containing cells).
    for kwargs, name in [
        ({'precision': 4}, 'geohash'),
        ({'zoom': 4}, 'tile'),
        ({'level': 8}, 's2'),
        ({'resolution': 5}, 'h3'),
    ]:
        fn = getattr(gm, f'{name}_cover')
        a = sorted(
            getattr(c, 'token', str(c))
            for c in fn(pt_neg, cell_rule='overlap', **kwargs)
        )
        b = sorted(
            getattr(c, 'token', str(c))
            for c in fn(pt_pos, cell_rule='overlap', **kwargs)
        )
        assert a == b, (name, a, b)


# ---------------------------------------------------------------------------
# A6 - tile rejects out-of-domain latitude (no silent clamp)
# ---------------------------------------------------------------------------


def test_a6_tile_rejects_out_of_web_mercator_latitude():
    with pytest.raises(gm.InvalidGeometryError, match='Web Mercator'):
        gm.Tile(lon=0.0, lat=86.0, zoom=5)
    with pytest.raises(gm.InvalidGeometryError, match='Web Mercator'):
        gm.tile_cells([0.0], [89.9], zoom=3)
    # Domain edge is still accepted.
    edge = gm.Tile(lon=0.0, lat=85.0511287798, zoom=5)
    assert edge.zoom == 5


# ---------------------------------------------------------------------------
# B2/B3 - dynamic CRS discovery + epoch fixtures
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ('code', 'preserve'),
    [
        (4326, True),  # WGS 84 dynamic
        (3857, True),  # Web Mercator (derived from WGS 84)
        (4258, False),  # ETRS89 static - clears epoch
        (2180, False),  # ETRF2000-PL static
        (7844, False),  # GDA2020 static
        (9000, True),  # ITRF2014 dynamic
        (9707, True),  # WGS 84 + EGM96 height (compound dynamic horizontal)
    ],
)
def test_b3_epoch_policy_to_crs(code, preserve):
    pt = gm.Point(10.0, 50.0, crs=4326, epoch=2010.0)
    out = pt.to_crs(code)
    if preserve:
        assert out.epoch == 2010.0, f'EPSG:{code} should preserve epoch'
    else:
        assert out.epoch is None, f'EPSG:{code} should clear epoch'


def test_b2_compound_9707_is_dynamic_for_epoch():
    pt = gm.Point(10.0, 50.0, crs=4326, epoch=2020.0)
    out = pt.to_crs(9707)
    assert out.epoch == 2020.0


def _cell_keys(values):
    keys = []
    for value in values:
        token = getattr(value, 'token', None)
        keys.append(token if token is not None else getattr(value, 'id', str(value)))
    return keys


def _dynamic_grid_source() -> tuple[gm.Geometry, gm.Geometry]:
    # Build the installed EPSG:7912 -> EPSG:7665 time-dependent operation into
    # a BoundCRS whose target is WGS 84 (G1762). PROJ then follows that bound
    # operation before the final explicit to_crs(4326), rather than selecting
    # the epoch-insensitive WGS 84 ensemble shortcut. The source latitude is a
    # z29 tile boundary witness: epochs 2000 and 2020 must choose different
    # emitted cells.
    from pyproj.transformer import TransformerGroup

    operation = TransformerGroup(
        7912,
        7665,
        always_xy=True,
        allow_ballpark=False,
    ).transformers[0].to_json_dict()
    crs = json.dumps({
        'type': 'BoundCRS',
        'source_crs': json.loads(gm.CRS(7912).to_projjson()),
        'target_crs': json.loads(gm.CRS(7665).to_projjson()),
        'transformation': operation['steps'][2],
    })
    boundary_latitude = 49.99008996175559
    return (
        gm.Point(10.0, boundary_latitude, z=100.0, crs=crs, epoch=2020.0),
        gm.Point(10.0, boundary_latitude, z=100.0, crs=crs, epoch=2000.0),
    )


@pytest.mark.parametrize(
    ('cover', 'kwargs'),
    [
        (gm.h3_cover, {'resolution': 7}),
        (gm.s2_cover, {'level': 12}),
        (gm.geohash_cover, {'precision': 8}),
        (gm.tile_cover, {'zoom': 29}),
    ],
    ids=['h3', 's2', 'geohash', 'tile'],
)
def test_grid_cover_admits_dynamic_source_frame_once(cover, kwargs) -> None:
    source, _ = _dynamic_grid_source()
    explicit = source.to_crs(4326)
    assert _cell_keys(cover(source, max_cells=None, **kwargs)) == _cell_keys(
        cover(explicit, max_cells=None, **kwargs)
    )


@pytest.mark.parametrize(
    ('factory', 'kwargs'),
    [
        (gm.h3_cells, {'resolution': 7}),
        (gm.s2_cells, {'level': 12}),
        (gm.geohash_cells, {'precision': 8}),
        (gm.tile_cells, {'zoom': 29}),
    ],
    ids=['h3', 's2', 'geohash', 'tile'],
)
def test_point_cell_array_admits_dynamic_source_frame_once(factory, kwargs) -> None:
    source, _ = _dynamic_grid_source()
    values = gm.GeometryArray([source, None])
    explicit = values.to_crs(4326)
    assert _cell_keys(factory(values, **kwargs)) == _cell_keys(factory(explicit, **kwargs))


@pytest.mark.parametrize(
    ('factory', 'kwargs'),
    [
        (gm.h3_bounding_cell, {}),
        (gm.s2_bounding_cell, {}),
        (gm.geohash_bounding_cell, {}),
        (gm.tile_bounding_cell, {}),
    ],
    ids=['h3', 's2', 'geohash', 'tile'],
)
def test_bounding_cell_admits_dynamic_source_frame_once(factory, kwargs) -> None:
    source, _ = _dynamic_grid_source()
    explicit = source.to_crs(4326)
    assert factory(source, **kwargs) == factory(explicit, **kwargs)


def test_dynamic_source_epoch_changes_a_boundary_cell() -> None:
    current, old = _dynamic_grid_source()
    current_lon = current.to_crs(4326)
    old_lon = old.to_crs(4326)
    assert abs(current_lon.x - old_lon.x) > 1e-9 or abs(current_lon.y - old_lon.y) > 1e-9
    assert _cell_keys(gm.tile_cover(current, zoom=29)) != _cell_keys(
        gm.tile_cover(old, zoom=29)
    )


# ---------------------------------------------------------------------------
# B1 - compound geographic not planar
# ---------------------------------------------------------------------------


def test_b1_compound_geographic_metric_matches_horizontal():
    # EPSG:9707 is WGS 84 + height; geodesic metrics must match 4326.
    a9707 = gm.box(0, 0, 1, 1, crs=9707).area
    a4326 = gm.box(0, 0, 1, 1, crs=4326).area
    assert a9707 == pytest.approx(a4326, rel=1e-12)
    d9707 = gm.distance(gm.Point(0, 0, crs=9707), gm.Point(1, 0, crs=9707))
    d4326 = gm.distance(gm.Point(0, 0, crs=4326), gm.Point(1, 0, crs=4326))
    assert d9707 == pytest.approx(d4326, rel=1e-12)


# ---------------------------------------------------------------------------
# X1 - MultiPolygon DE-9IM boundary parity with Polygon
# ---------------------------------------------------------------------------


def test_x1_singleton_multipolygon_shell_line_matches_polygon():
    shell = [(0.0, 0.0), (0.0, 2.0), (2.0, 2.0), (2.0, 0.0), (0.0, 0.0)]
    poly = gm.Polygon(shell)
    mp = gm.MultiPolygon([poly])
    line = gm.LineString([(0.0, 1.0), (0.0, 1.5)])
    assert gm.relate(poly, line) == 'FF2101FF2'
    assert gm.relate(mp, line) == 'FF2101FF2'
    assert gm.contains(poly, line) is False
    assert gm.contains(mp, line) is False
    assert gm.touches(poly, line) is True
    assert gm.touches(mp, line) is True


# ---------------------------------------------------------------------------
# C1 - compound to_cf rejects rather than truncating
# ---------------------------------------------------------------------------


def test_c1_compound_to_cf_rejects():
    with pytest.raises(gm.CRSError, match='compound'):
        gm.CRS(9707).to_cf()


# ---------------------------------------------------------------------------
# C4 - identity transform validates geographic domain
# ---------------------------------------------------------------------------


def test_c4_identity_transform_rejects_out_of_domain_latitude():
    with pytest.raises((gm.CRSError, gm.InvalidGeometryError, gm.GeometryError)):
        gm.crs_transform('EPSG:4326', 'EPSG:4326', [10.0], [100.0])
    # Valid identity still works.
    out = gm.crs_transform('EPSG:4326', 'EPSG:4326', [10.0], [50.0])
    assert out[0][0] == pytest.approx(10.0)
    assert out[0][1] == pytest.approx(50.0)


# ---------------------------------------------------------------------------
# D4/D5 - geocode domain / north pole
# ---------------------------------------------------------------------------


def test_d4_pluscode_rejects_out_of_domain():
    with pytest.raises(gm.InvalidGeometryError):
        gm.pluscode_encode(1e20, 1e20)
    with pytest.raises(gm.InvalidGeometryError):
        gm.pluscode_encode(200.0, 0.0)


def test_d5_shortlink_north_pole_distinct_from_south():
    north = gm.osm_shortlink_encode(0.0, 90.0, zoom=10)
    south = gm.osm_shortlink_encode(0.0, -90.0, zoom=10)
    assert north != south


# ---------------------------------------------------------------------------
# C2 - polar stereographic variant A by defining parameters
# ---------------------------------------------------------------------------


def test_c2_polar_stereo_variant_a_uses_scale_factor():
    """Variant A CF must honor scale_factor_at_projection_origin (+k_0)."""
    cf = {
        'grid_mapping_name': 'polar_stereographic',
        'latitude_of_projection_origin': 90.0,
        'straight_vertical_longitude_from_pole': 0.0,
        'scale_factor_at_projection_origin': 0.994,
        'false_easting': 2_000_000.0,
        'false_northing': 2_000_000.0,
        'semi_major_axis': 6_378_137.0,
        'inverse_flattening': 298.257223563,
    }
    crs = gm.CRS(cf)
    text = crs.to_proj()
    assert 'k_0=0.994' in text or 'k=0.994' in text
    # Must not invent a default standard parallel lat_ts when scale is defining.
    assert 'lat_ts=90' not in text


def test_c2_polar_stereo_variant_b_uses_standard_parallel():
    cf = {
        'grid_mapping_name': 'polar_stereographic',
        'latitude_of_projection_origin': -90.0,
        'straight_vertical_longitude_from_pole': 0.0,
        'standard_parallel': -71.0,
        'false_easting': 0.0,
        'false_northing': 0.0,
        'semi_major_axis': 6_378_137.0,
        'inverse_flattening': 298.257223563,
    }
    crs = gm.CRS(cf)
    text = crs.to_proj()
    assert 'lat_ts=-71' in text
    assert 'k_0=' not in text and '+k=' not in text


def test_c8_mercator_rejects_conflicting_defining_parameters():
    with pytest.raises(gm.CRSError):
        gm.CRS({
            'grid_mapping_name': 'mercator',
            'longitude_of_projection_origin': 0.0,
            'standard_parallel': 30.0,
            'scale_factor_at_projection_origin': 0.8,
            'false_easting': 1234.0,
            'false_northing': -567.0,
            'semi_major_axis': 6_378_137.0,
            'inverse_flattening': 298.257223563,
        })


def test_c8_mercator_single_key_controls_match_independent_proj_definitions():
    common = {
        'grid_mapping_name': 'mercator',
        'longitude_of_projection_origin': 0.0,
        'false_easting': 1234.0,
        'false_northing': -567.0,
        'semi_major_axis': 6_378_137.0,
        'inverse_flattening': 298.257223563,
    }
    standard_parallel = {**common, 'standard_parallel': 30.0}
    scale_factor = {**common, 'scale_factor_at_projection_origin': 0.8}
    standard_proj = (
        '+proj=merc +lat_0=0 +lon_0=0 +lat_ts=30 '
        '+x_0=1234 +y_0=-567 +a=6378137 +rf=298.257223563 '
        '+units=m +type=crs'
    )
    scale_proj = (
        '+proj=merc +lat_0=0 +lon_0=0 +k_0=0.8 '
        '+x_0=1234 +y_0=-567 +a=6378137 +rf=298.257223563 '
        '+units=m +type=crs'
    )
    x, y = 1_000_000.0, 2_000_000.0
    assert gm.crs_transform(standard_parallel, 4326, x, y) == pytest.approx(
        gm.crs_transform(standard_proj, 4326, x, y)
    )
    assert gm.crs_transform(scale_factor, 4326, x, y) == pytest.approx(
        gm.crs_transform(scale_proj, 4326, x, y)
    )


def test_c8_mercator_wkt_precedence_survives_parameter_conflict():
    cf = {
        'grid_mapping_name': 'mercator',
        'longitude_of_projection_origin': 0.0,
        'standard_parallel': 30.0,
        'scale_factor_at_projection_origin': 0.8,
        'semi_major_axis': 6_378_137.0,
        'inverse_flattening': 298.257223563,
        'crs_wkt': gm.CRS(3395).to_wkt(),
    }
    assert gm.CRS(cf).to_epsg() == 3395


_DEG = math.pi / 180.0
_GRAD = math.pi / 200.0
# International foot (EPSG:9002), exact.
_INTL_FOOT = 0.3048


def _polar_a_wkt(
    *,
    lat: float,
    lon: float,
    k: float,
    false_easting: float,
    false_northing: float,
    angle_unit: str,
    angle_factor: float,
    length_unit: str,
    length_factor: float,
    axis_unit: str | None = None,
    axis_factor: float | None = None,
) -> str:
    axis_unit = axis_unit or length_unit
    axis_factor = length_factor if axis_factor is None else axis_factor
    return f"""PROJCRS["probe polar A",
  BASEGEOGCRS["WGS 84",
    DATUM["World Geodetic System 1984",
      ELLIPSOID["WGS 84",6378137,298.257223563]],
    PRIMEM["Greenwich",0,ANGLEUNIT["degree",{_DEG}]],
    UNIT["degree",{_DEG}]],
  CONVERSION["probe",
    METHOD["Polar Stereographic (variant A)",ID["EPSG",9810]],
    PARAMETER["Latitude of natural origin",{lat},ANGLEUNIT["{angle_unit}",{angle_factor}],ID["EPSG",8801]],
    PARAMETER["Longitude of natural origin",{lon},ANGLEUNIT["{angle_unit}",{angle_factor}],ID["EPSG",8802]],
    PARAMETER["Scale factor at natural origin",{k},SCALEUNIT["unity",1],ID["EPSG",8805]],
    PARAMETER["False easting",{false_easting},LENGTHUNIT["{length_unit}",{length_factor}],ID["EPSG",8806]],
    PARAMETER["False northing",{false_northing},LENGTHUNIT["{length_unit}",{length_factor}],ID["EPSG",8807]]],
  CS[Cartesian,2],
    AXIS["easting (E)",east,ORDER[1],LENGTHUNIT["{axis_unit}",{axis_factor}]],
    AXIS["northing (N)",north,ORDER[2],LENGTHUNIT["{axis_unit}",{axis_factor}]]]"""


def _cf_projection_keys(crs: object) -> dict[str, object]:
    cf = dict(gm.CRS(crs).to_cf())
    cf.pop('crs_wkt', None)
    return cf


def test_c2_polar_a_custom_degree_and_grad_export_same_cf_degrees():
    """100 gon / 50 gon must export as 90° / 45°, not as stored grads."""
    degree = _polar_a_wkt(
        lat=90.0,
        lon=45.0,
        k=0.999,
        false_easting=1000.0,
        false_northing=2000.0,
        angle_unit='degree',
        angle_factor=_DEG,
        length_unit='metre',
        length_factor=1.0,
    )
    grad = _polar_a_wkt(
        lat=100.0,
        lon=50.0,
        k=0.999,
        false_easting=1000.0,
        false_northing=2000.0,
        angle_unit='grad',
        angle_factor=_GRAD,
        length_unit='metre',
        length_factor=1.0,
    )
    radian = _polar_a_wkt(
        lat=math.pi / 2.0,
        lon=math.pi / 4.0,
        k=0.999,
        false_easting=1000.0,
        false_northing=2000.0,
        angle_unit='radian',
        angle_factor=1.0,
        length_unit='metre',
        length_factor=1.0,
    )
    keys = (
        'latitude_of_projection_origin',
        'straight_vertical_longitude_from_pole',
        'scale_factor_at_projection_origin',
        'false_easting',
        'false_northing',
    )
    expected = {
        'latitude_of_projection_origin': 90.0,
        'straight_vertical_longitude_from_pole': 45.0,
        'scale_factor_at_projection_origin': 0.999,
        'false_easting': 1000.0,
        'false_northing': 2000.0,
    }
    for source in (degree, grad, radian):
        cf = _cf_projection_keys(source)
        assert cf['grid_mapping_name'] == 'polar_stereographic'
        for key, value in expected.items():
            assert cf[key] == pytest.approx(value)
        rebuilt = {k: cf[k] for k in ('grid_mapping_name', *keys)}
        rebuilt['semi_major_axis'] = 6_378_137.0
        rebuilt['inverse_flattening'] = 298.257223563
        assert gm.crs_transform(rebuilt, 4326, 1000.0, 2000.0) == pytest.approx(
            gm.crs_transform(source, 4326, 1000.0, 2000.0)
        )


def test_c2_polar_a_metre_offsets_on_foot_axes_use_axis_units():
    """False easting/northing follow the projected axis, not metres.

    Parameter offsets are 1000 m and 2500 m; axes are the international foot
    (0.3048 m). Expected CF values are 1000/0.3048 and 2500/0.3048, written as
    independent literals so a swapped pair or a metres leftover fails.
    """
    wkt = _polar_a_wkt(
        lat=90.0,
        lon=45.0,
        k=0.994,
        false_easting=1000.0,
        false_northing=2500.0,
        angle_unit='degree',
        angle_factor=_DEG,
        length_unit='metre',
        length_factor=1.0,
        axis_unit='foot',
        axis_factor=_INTL_FOOT,
    )
    cf = _cf_projection_keys(wkt)
    assert cf['units'] == 'ft'
    assert cf['false_easting'] == pytest.approx(3280.839895013123)
    assert cf['false_northing'] == pytest.approx(8202.099737532808)
    rebuilt = dict(cf)
    east, north = cf['false_easting'] + 100.0, cf['false_northing'] + 200.0
    assert gm.crs_transform(rebuilt, 4326, east, north) == pytest.approx(
        gm.crs_transform(wkt, 4326, east, north)
    )


def test_c2_paris_prime_meridian_exports_degrees():
    """EPSG:4807 stores 2.5969213 gon; CF longitude_of_prime_meridian is degrees."""
    cf = gm.CRS(4807).to_cf()
    assert cf['longitude_of_prime_meridian'] == pytest.approx(2.33722917)


# ---------------------------------------------------------------------------
# C7 - standard_parallel exact arity
# ---------------------------------------------------------------------------


def test_c7_standard_parallel_empty_raises():
    with pytest.raises(gm.CRSError, match='standard_parallel'):
        gm.CRS({
            'grid_mapping_name': 'lambert_conformal_conic',
            'standard_parallel': [],
            'longitude_of_central_meridian': 0.0,
            'semi_major_axis': 6_378_137.0,
            'inverse_flattening': 298.257223563,
        })


def test_c7_standard_parallel_extra_raises():
    with pytest.raises(gm.CRSError, match='1 or 2'):
        gm.CRS({
            'grid_mapping_name': 'lambert_conformal_conic',
            'standard_parallel': [30.0, 60.0, 45.0],
            'longitude_of_central_meridian': 0.0,
            'semi_major_axis': 6_378_137.0,
            'inverse_flattening': 298.257223563,
        })


def test_c7_polar_empty_standard_parallel_raises():
    with pytest.raises(gm.CRSError, match='standard_parallel'):
        gm.CRS({
            'grid_mapping_name': 'polar_stereographic',
            'standard_parallel': [],
            'straight_vertical_longitude_from_pole': 0.0,
            'semi_major_axis': 6_378_137.0,
            'inverse_flattening': 298.257223563,
        })


# ---------------------------------------------------------------------------
# C9 - projected CF native units
# ---------------------------------------------------------------------------


def test_c9_cf_projected_units_from_metadata():
    """US-survey-foot CRS must reparse pure CF (no WKT) with us-ft units.

    CF false_easting is in native feet; PROJ +x_0 must be metres with
    +units=us-ft (EPSG pattern). A metres misread shifts lon/lat by tens of
    degrees. PROJ may identify the rebuilt definition (e.g. EPSG:10913).
    """
    cf = dict(gm.CRS(2227).to_cf())
    assert cf.get('units') == 'us-ft'
    del cf['crs_wkt']
    rebuilt = gm.CRS(cf)
    proj = rebuilt.to_proj()
    axes = rebuilt.axes
    unit_ok = (
        'us-ft' in proj
        or 'to_meter' in proj
        or any(
            'survey' in (a.get('unit_name') or '').lower()
            or abs((a.get('unit_conversion_factor') or 1.0) - 0.304800609601219) < 1e-12
            for a in axes
        )
    )
    assert unit_ok, (rebuilt.canonical, proj, axes)
    # 1000 native units east of false origin must not match a metres misread.
    origin_e, origin_n = cf['false_easting'], cf['false_northing']
    p_native = gm.Point(origin_e + 1000.0, origin_n, crs=2227)
    p_rebuilt = gm.Point(origin_e + 1000.0, origin_n, crs=rebuilt)
    g_native = p_native.to_crs(4326)
    g_rebuilt = p_rebuilt.to_crs(4326)
    assert abs(g_native.x - g_rebuilt.x) < 1e-8
    assert abs(g_native.y - g_rebuilt.y) < 1e-8


# ---------------------------------------------------------------------------
# C6 - CrsCfInfo includes runtime keys
# ---------------------------------------------------------------------------


def test_c6_crscfinfo_covers_runtime_keys():
    from gometry._types import CrsCfInfo

    keys = set(CrsCfInfo.__annotations__)
    # Sample native keys across projected variants must be typed.
    for required in (
        'grid_mapping_name',
        'standard_parallel',
        'scale_factor_at_projection_origin',
        'straight_vertical_longitude_from_pole',
        'units',
        'false_easting',
    ):
        assert required in keys, required


# ---------------------------------------------------------------------------
# D2 - bulk cell factories carry missing rows
# ---------------------------------------------------------------------------


def test_d2_h3_cells_geometry_array_missing_rows():
    arr = gm.GeometryArray([
        gm.Point(0.0, 0.0, crs=4326),
        None,
        gm.Point(1.0, 1.0, crs=4326),
    ])
    cells = gm.h3_cells(arr, resolution=5)
    assert len(cells) == 3
    assert list(cells.is_missing) == [False, True, False]
    assert cells[0] is not None
    assert cells[1] is None
    assert cells[2] is not None


@pytest.mark.parametrize(
    ('name', 'kwargs'),
    [
        ('h3', {'resolution': 2}),
        ('s2', {'level': 3}),
        ('geohash', {'precision': 3}),
        ('tile', {'zoom': 3}),
    ],
)
def test_grid_cover_normalizes_projected_and_geographic_seam_sources(name, kwargs):
    """Every coverer sees the same normalized WGS84 seam source."""
    source = gm.LineString([(179.0, -10.0), (-179.0, 10.0)], crs=4326)
    variants = [
        source.set_crs(None),
        source,
        source.set_crs('OGC:CRS84', overwrite=True),
        source.to_crs(3857),
    ]
    cover = getattr(gm, f'{name}_cover')
    results = [
        {
            str(cell)
            for cell in cover(
                variant,
                cell_rule='overlap',
                max_cells=None,
                **kwargs,
            )
        }
        for variant in variants
    ]
    assert results[1:] == [results[0]] * (len(results) - 1)


@pytest.mark.parametrize(
    ('name', 'kwargs'),
    [
        ('h3', {'resolution': 2}),
        ('s2', {'level': 3}),
        ('geohash', {'precision': 3}),
        ('tile', {'zoom': 3}),
    ],
)
def test_grid_cover_array_empty_and_missing_rows_are_empty_groups(name, kwargs):
    """Present empty and missing rows preserve source-row association."""
    source = gm.box(-10.0, -10.0, 10.0, 10.0, crs=4326).to_crs(3857)
    array = gm.GeometryArray(
        [
            source,
            gm.from_wkt('POLYGON EMPTY', crs=3857),
            None,
        ]
    )
    cover = getattr(gm, f'{name}_cover')
    groups = cover(array, cell_rule='overlap', max_cells=None, **kwargs)
    assert [len(group) for group in groups] == [len(groups[0]), 0, 0]
    assert len(groups[0]) > 0


# ---------------------------------------------------------------------------
# D3 - CellArray equality uses visible values
# ---------------------------------------------------------------------------


def test_d3_cellarray_equality_uses_visible_values():
    box = gm.box(0.0, 0.0, 1.0, 1.0, crs=4326)
    a = gm.h3_cover(box, resolution=3, cell_rule='overlap', max_cells=100)
    b = gm.h3_cover(box, resolution=3, cell_rule='overlap', max_cells=50)
    # Factory budgets are not part of the CellArray value.
    assert list(a) == list(b)
    assert a == b


# ---------------------------------------------------------------------------
# B4 — geodesic cap cache keys CRS runtime generation
# ---------------------------------------------------------------------------


def test_b4_spatial_index_geodesic_caps_survive_crs_clear_cache():
    """Warm caps, clear CRS caches, nearest must stay correct (no mixed ellipsoid)."""
    rows = [
        gm.LineString([(lon, lat), (lon + 1.0, lat + 1.0)], crs=4326)
        for lon, lat in ((-10.0, 40.0), (0.0, 50.0), (20.0, 0.0), (-170.0, -40.0))
    ]
    idx = gm.SpatialIndex(rows)
    query = gm.Point(-169.0, -39.5, crs=4326)
    warm = int(idx.nearest(query)[0])
    gm.crs_clear_cache()
    after = int(idx.nearest(query)[0])
    expected = min(range(len(rows)), key=lambda i: (gm.distance(query, rows[i]), i))
    assert warm == after == expected


# ---------------------------------------------------------------------------
# C5 — crs_codes kind tokens admit the typed inventory
# ---------------------------------------------------------------------------


def test_c5_crs_codes_accepts_typed_database_kinds():
    """Every CrsDatabaseKind token is admitted; unknown tokens are rejected."""
    # Representative tokens from the typed inventory (not PROJ empty kinds).
    for kind in (
        'projected',
        'geographic_2d',
        'ellipsoid',
        'conversion',
        'geodetic_reference_frame',
    ):
        codes = gm.crs_codes('EPSG', kind=kind)
        assert isinstance(codes, list)
        assert len(codes) > 0
    with pytest.raises(gm.CRSError, match='unknown PROJ database kind'):
        gm.crs_codes('EPSG', kind='derived_projected')  # missing _crs suffix


# ---------------------------------------------------------------------------
# D1 — max_cells charges realized unique cells
# ---------------------------------------------------------------------------


def test_d1_h3_max_cells_charges_realized_cells_not_raw_candidates():
    """A tight max_cells that fits the realized cover must succeed.

    Pre-fix, charging repeated edge candidates rejected legitimate covers
    well below the visible cell count.
    """
    box = gm.box(-122.42, 37.77, -122.40, 37.79, crs=4326)
    full = gm.h3_cover(box, resolution=9, cell_rule='overlap')
    n = len(list(full))
    assert n > 1
    # Budget equal to realized size must succeed (not charge raw duplicates).
    tight = gm.h3_cover(box, resolution=9, cell_rule='overlap', max_cells=n)
    assert len(list(tight)) == n
    with pytest.raises(gm.GeometryError, match='max_cells'):
        gm.h3_cover(box, resolution=9, cell_rule='overlap', max_cells=max(1, n // 4))


# ---------------------------------------------------------------------------
# D6 — geometry output documents planar chord proxy
# ---------------------------------------------------------------------------


def test_d6_h3_cell_polygon_doc_mentions_chord_proxy():
    """Public cell.polygon prose must disclose planar-chord proxy semantics."""
    cell = gm.H3Cell(0x85283473FFFFFFF)
    text = (type(cell).polygon.__doc__ or '').lower()
    assert 'chord' in text or 'planar' in text, text
    assert 'cell-algebra' in text or 'contains' in text or 'hierarchical' in text
    poly = cell.polygon
    assert poly.geometry_type in ('Polygon', 'MultiPolygon')


# ---------------------------------------------------------------------------
# E6 — CellArray compact/uncompact/to_polygon docs parse and run
# ---------------------------------------------------------------------------


def test_e6_cellarray_compact_uncompact_to_polygon_examples():
    p = gm.Point(-122.4194, 37.7749, crs=4326)
    cell = gm.h3_cover(p, resolution=7)[0]
    cells = gm.CellArray([cell, next(iter(cell.neighbors))])
    assert len(cells.compact(5)) == 2
    assert len(gm.CellArray([cell]).uncompact(8)) == 7
    assert gm.CellArray([cell]).to_polygon().geometry_type == 'Polygon'
