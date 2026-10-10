"""37 public PairCase builders + timed callables for RELEASE.

Registers into ``PUBLIC_CASE_BUILDERS`` (keyed by gometry row name). Timed
callables perform no validation; all checks live in compare/preconditions.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import numpy as np

from _bench_chart_edge_reference import (
    country_exterior_chart_length,
    polygonal_chart_areas,
)

if TYPE_CHECKING:
    from collections.abc import Callable

from _bench_oracles import (
    OracleContext,
    OracleMismatch,
    PairCase,
    _as_shapely,
    areal_result_equivalent,
    exact_coordinates,
    exact_mask,
    linework_result_equivalent,
    metric_allclose,
    non_unique_points_contract,
    normalized_index_pairs,
    normalized_tile_set,
    normalized_uint64_set,
    parsed_geometry_exact,
    register_public_builders,
    wrap_longitude_allclose,
)
from _bench_public_fixtures import (
    arrow_mixed_100k,
    webmerc_transform_10k,
    buildings_10k_gometry,
    buildings_10k_shapely,
    clip_linework_10k,
    destination_inputs_10k,
    dwithin_pairs_10k,
    geodesic_pairs_10k,
    index_build_probe_boxes,
    index_nearest_queries_1k,
    index_query_boxes_1k,
    intersection_pairs_1k,
    intersects_polygon_and_points,
    geohash_encode_inputs,
    masked_crs_200k,
    mixed_10k_gometry,
    mixed_10k_shapely,
    mixed_ewkb_10k,
    mixed_wkt_10k,
    parcels_10k,
    points_10k_gometry,
    points_10k_shapely,
    points_xy_numpy,
    prepared_polygon_and_probes,
    repair_1k_gometry,
    repair_1k_shapely,
    roads_10k_gometry,
    roads_10k_shapely,
    service_areas_1024,
    validity_10k_gometry,
    validity_10k_shapely,
)
from _bench_real_world_layers import (
    BRAZIL_BOUNDS,
    brazil_bbox_polygon,
    brazil_geometry,
    country_exteriors_multilinestring,
    country_exteriors_shapely,
    country_parts_shapely,
    country_pois_10k,
    country_pois_10k_shapely,
    load_country_geojson_text,
    load_country_parts,
)


# ---------------------------------------------------------------------------
# Case wrapper: kind + unit for the oracle driver display / helpers
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class PublicCase:
    """PairCase + display kind/unit (PairCase itself has no kind field)."""

    pair: PairCase
    kind: str
    unit: str | None = None
    useful_work: dict[str, object] = field(
        default_factory=lambda: {'count': 1, 'unit': 'equivalent-case-invocation'}
    )
    output_validator: Callable[[object], None] | None = None

    def verify(self, context: OracleContext) -> None:
        ctx = OracleContext(
            operation=context.operation,
            kind=self.kind,
            unit=self.unit,
            oracle_identity=context.oracle_identity,
        )
        self.pair.verify(ctx)


def _case(
    gometry_call: Callable[[], object],
    competitor_call: Callable[[], object] | None,
    compare: Callable[[object, object | None, OracleContext], None],
    *,
    kind: str,
    unit: str | None = None,
    preconditions: tuple[Callable[[], None], ...] = (),
    useful_work: dict[str, object] | None = None,
    output_validator: Callable[[object], None] | None = None,
) -> PublicCase:
    return PublicCase(
        pair=PairCase(
            gometry_call=gometry_call,
            competitor_call=competitor_call,
            compare=compare,
            preconditions=preconditions,
        ),
        kind=kind,
        unit=unit,
        useful_work=useful_work or {'count': 1, 'unit': 'equivalent-case-invocation'},
        output_validator=output_validator,
    )


# Timed callables keyed by raw benchmark row name (both sides)
PUBLIC_TIMED: dict[str, Callable[[], object]] = {}


def _reg(name: str, fn: Callable[[], object]) -> Callable[[], object]:
    PUBLIC_TIMED[name] = fn
    return fn


# ---------------------------------------------------------------------------
# Compare helpers specialized for public ops
# ---------------------------------------------------------------------------


def _compare_ewkb_roundtrip(
    left: object, right: object | None, context: OracleContext
) -> None:
    """Exact parsed structure plus EWKB SRID."""
    import shapely

    if right is None:
        raise OracleMismatch('from_wkb requires competitor')
    left_rows = list(left)  # type: ignore[arg-type]
    right_rows = list(right)  # type: ignore[arg-type]
    if len(left_rows) != len(right_rows):
        raise OracleMismatch(
            'from_wkb count mismatch',
            details=f'{len(left_rows)} vs {len(right_rows)}',
        )
    for i, (lg, rg) in enumerate(zip(left_rows, right_rows, strict=True)):
        if lg is None or rg is None:
            if lg is not rg:
                raise OracleMismatch(f'from_wkb missing mismatch row={i}')
            continue
        crs = getattr(lg, 'crs', None)
        l_srid = crs.to_epsg() if crs is not None and hasattr(crs, 'to_epsg') else None
        r_srid = int(shapely.get_srid(rg))
        if l_srid != r_srid and not (l_srid is None and r_srid == 0):
            raise OracleMismatch(
                'from_wkb SRID mismatch',
                details=f'row={i} {l_srid!r} vs {r_srid!r}',
            )
    parsed_geometry_exact(left, right, context)


def _compare_from_arrow(
    left: object, right: object | None, context: OracleContext
) -> None:
    import shapely

    if right is None:
        raise OracleMismatch('from_arrow requires competitor')
    gm_arr = left
    gpd_s = right
    if len(gm_arr) != len(gpd_s):  # type: ignore[arg-type]
        raise OracleMismatch(
            'from_arrow length mismatch',
            details=f'{len(gm_arr)} vs {len(gpd_s)}',  # type: ignore[arg-type]
        )
    gm_miss = np.asarray(gm_arr.is_missing)  # type: ignore[union-attr]
    sh_miss = np.asarray(gpd_s.isna())  # type: ignore[union-attr]
    if not np.array_equal(gm_miss, sh_miss):
        raise OracleMismatch(
            'from_arrow missing mask mismatch',
            details=f'gm missing={int(gm_miss.sum())} gpd={int(sh_miss.sum())}',
        )
    if int(gm_miss.sum()) != 10_000:
        raise OracleMismatch(
            'from_arrow missing count', details=str(int(gm_miss.sum()))
        )
    crs = getattr(gm_arr, 'crs', None)
    epsg = crs.to_epsg() if crs is not None and hasattr(crs, 'to_epsg') else None
    if epsg != 32634:
        raise OracleMismatch('from_arrow CRS not 32634', details=repr(epsg))
    # Full-column topological equality outside any timed region (oracle is untimed).
    checked = 0
    for i, (g, s) in enumerate(zip(gm_arr, gpd_s, strict=True)):  # type: ignore[arg-type]
        if g is None:
            continue
        checked += 1
        ga = g.set_crs(None) if getattr(g, 'crs', None) is not None else g
        if not bool(shapely.equals(_as_shapely(ga), _as_shapely(s))):
            raise OracleMismatch(f'from_arrow geometry mismatch row={i}')
    if checked != 90_000:
        raise OracleMismatch('from_arrow non-missing count', details=str(checked))


def _compare_to_wkb(left: object, right: object | None, context: OracleContext) -> None:
    """Element-for-element EWKB bytes (spec §3: same include_srid/flavor path)."""
    if right is None:
        raise OracleMismatch('to_wkb requires competitor')
    a = list(left)  # type: ignore[arg-type]
    b = list(right)  # type: ignore[arg-type]
    if len(a) != len(b):
        raise OracleMismatch('to_wkb length mismatch', details=f'{len(a)} vs {len(b)}')
    for i, (ba, bb) in enumerate(zip(a, b, strict=True)):
        ba_b = bytes(ba)
        bb_b = bytes(bb)
        if ba_b != bb_b:
            raise OracleMismatch(
                'to_wkb exact bytes mismatch',
                details=(
                    f'row={i} gometry={ba_b[:12].hex()}… '
                    f'competitor={bb_b[:12].hex()}… '
                    f'lens={len(ba_b)}/{len(bb_b)}'
                ),
            )


def _coordinate_values_and_indices(
    value: object, operation: str
) -> tuple[object, np.ndarray]:
    if not isinstance(value, tuple) or len(value) != 2:
        raise OracleMismatch(f'{operation} result must be (coordinates, indexes)')
    coordinates, indexes = value
    return coordinates, np.asarray(indexes)


def _compare_coords_matrix_and_row_index(
    left: object, right: object | None, context: OracleContext
) -> None:
    if right is None:
        raise OracleMismatch('coords matrix and row_index require competitor')
    l_coords, l_idx = _coordinate_values_and_indices(
        left, 'coords matrix and row_index'
    )
    r_coords, r_idx = right  # type: ignore[misc]
    exact_coordinates(
        l_coords,
        r_coords,
        OracleContext(
            operation=context.operation,
            kind=context.kind,
            unit='m',
            oracle_identity=context.oracle_identity,
        ),
    )
    if not np.array_equal(np.asarray(l_idx), np.asarray(r_idx)):
        raise OracleMismatch(
            'coords row_index mismatch',
            details=f'shapes {np.asarray(l_idx).shape} vs {np.asarray(r_idx).shape}',
        )


def _compare_points(left: object, right: object | None, context: OracleContext) -> None:
    if right is None:
        raise OracleMismatch('points requires competitor')

    if len(left) != len(right):  # type: ignore[arg-type]
        raise OracleMismatch(
            'points count mismatch',
            details=f'{len(left)} vs {len(right)}',  # type: ignore[arg-type]
        )
    # exact XY
    lx = np.asarray([g.x for g in left], dtype=np.float64)  # type: ignore[union-attr]
    ly = np.asarray([g.y for g in left], dtype=np.float64)  # type: ignore[union-attr]
    import shapely

    rx = shapely.get_coordinates(right)[:, 0]
    ry = shapely.get_coordinates(right)[:, 1]
    if not np.array_equal(lx, rx) or not np.array_equal(ly, ry):
        raise OracleMismatch('points XY mismatch')


def _precondition_prepared_hit_rate() -> None:
    import gometry as gm

    data = prepared_polygon_and_probes()
    mask = gm.contains_xy(data['gm_poly'], data['xs'], data['ys'])
    rate = float(np.asarray(mask).mean())
    if not (0.40 <= rate <= 0.60):
        raise OracleMismatch(
            'prepared hit rate outside 40-60%',
            details=f'rate={rate:.4%}',
        )
    if data['n_coords'] != 1316:
        raise OracleMismatch(
            'prepared polygon coordinate count',
            details=f'n_coords={data["n_coords"]}',
        )


def _precondition_dwithin_count() -> None:
    import shapely

    d = dwithin_pairs_10k()
    mask = shapely.dwithin(d['sh_a'], d['sh_b'], 100.0)
    n = int(np.asarray(mask).sum())
    if n != 5000:
        raise OracleMismatch('dwithin match count', details=f'got {n}, want 5000')


def _precondition_validity_dist() -> None:
    arr = validity_10k_gometry()
    n_valid = int(np.asarray(arr.is_valid).sum())
    if n_valid != 8000:
        raise OracleMismatch(
            'validity distribution', details=f'valid={n_valid}, want 8000'
        )


def _compare_simplify(
    left: object, right: object | None, context: OracleContext
) -> None:
    import shapely

    if right is None:
        raise OracleMismatch('simplify requires competitor')
    view = left.coords
    l_coordinates = (np.asarray(view, dtype=np.float64), view.row_index)
    l_coords, l_idx = _coordinate_values_and_indices(l_coordinates, 'simplify')
    r_coords, r_idx = shapely.get_coordinates(right, include_z=False, return_index=True)
    if len(l_coords) != len(r_coords):
        raise OracleMismatch(
            'simplify vertex count mismatch',
            details=f'{len(l_coords)} vs {len(r_coords)}',
        )
    if not np.array_equal(np.asarray(l_idx), np.asarray(r_idx)):
        raise OracleMismatch('simplify row-index mismatch')
    # endpoints preserved per row: check first/last of each run
    exact_coordinates(
        l_coords,
        r_coords,
        OracleContext(
            operation=context.operation,
            kind=context.kind,
            unit='m',
            oracle_identity=context.oracle_identity,
        ),
        atol=1e-9,
        rtol=0.0,
    )


def _as_geom_rows(value: object) -> list[object]:
    """Normalize scalar geometry / array / sequence to a row list."""
    if value is None:
        return []
    # numpy object array of shapely geoms
    if isinstance(value, np.ndarray):
        return list(value)
    # GeometryArray (has is_missing)
    if hasattr(value, 'is_missing'):
        return list(value)  # type: ignore[arg-type]
    # scalar gometry geometry (has geometry_type, no is_missing)
    if hasattr(value, 'geometry_type'):
        return [value]
    # scalar shapely (has geom_type)
    if hasattr(value, 'geom_type') and not isinstance(value, (list, tuple)):
        return [value]
    return list(value)  # type: ignore[arg-type]


def _compare_areal_result(
    left: object, right: object | None, context: OracleContext
) -> None:
    """Compare pure areal overlay output with its explicit area budget."""
    areal_result_equivalent(left, right, context, rtol=1e-6, atol=1e-3)


def _compare_repair(left: object, right: object | None, context: OracleContext) -> None:
    import shapely

    if right is None:
        raise OracleMismatch('repair requires competitor')
    left_rows = _as_geom_rows(left)
    right_rows = _as_geom_rows(right)
    if len(left_rows) != len(right_rows):
        raise OracleMismatch(
            'repair count mismatch',
            details=f'{len(left_rows)} vs {len(right_rows)}',
        )
    for i, (lg, rg) in enumerate(zip(left_rows, right_rows, strict=True)):
        if lg is None or rg is None:
            if lg is not rg:
                raise OracleMismatch(f'repair missing mismatch row={i}')
            continue
        try:
            left_valid = bool(shapely.is_valid(_as_shapely(lg)))
            right_valid = bool(shapely.is_valid(_as_shapely(rg)))
        except Exception as exc:
            raise OracleMismatch(
                'repair Shapely validity check failed', details=f'row={i}: {exc}'
            ) from exc
        if not left_valid:
            raise OracleMismatch(f'repair gometry output invalid row={i}')
        if not right_valid:
            raise OracleMismatch('repair competitor output has invalids')
    areal_result_equivalent(
        left,
        right,
        context,
        rtol=1e-6,
        atol=1e-3,
        allow_collapsed=True,
    )


def _compare_to_crs(left: object, right: object | None, context: OracleContext) -> None:

    if right is None:
        raise OracleMismatch('to_crs requires competitor')
    gm_arr = left
    gpd_s = right
    gm_rows = list(gm_arr)  # type: ignore[call-overload]
    gm_miss = np.array([g is None for g in gm_rows], dtype=bool)
    sh_miss = np.asarray(gpd_s.isna())  # type: ignore[union-attr]
    if not np.array_equal(gm_miss, sh_miss):
        raise OracleMismatch('to_crs missing mask mismatch')
    crs = getattr(gm_arr, 'crs', None)
    epsg = crs.to_epsg() if crs is not None else None
    if epsg != 3857:
        raise OracleMismatch('to_crs result CRS not 3857', details=repr(epsg))
    # coordinates
    lxy = []
    rxy = []
    for g, s in zip(gm_rows, gpd_s, strict=True):  # type: ignore[arg-type]
        if g is None:
            continue
        lxy.append([g.x, g.y])
        rxy.append([s.x, s.y])
    exact_coordinates(
        np.asarray(lxy, dtype=np.float64),
        np.asarray(rxy, dtype=np.float64),
        OracleContext(
            operation=context.operation,
            kind=context.kind,
            unit='m_3857',
            oracle_identity=context.oracle_identity,
        ),
    )


def _compare_crs_transform(
    left: object, right: object | None, context: OracleContext
) -> None:
    exact_coordinates(
        left,
        right,
        OracleContext(
            operation=context.operation,
            kind=context.kind,
            unit='m_3857',
            oracle_identity=context.oracle_identity,
        ),
        rtol=5e-12,
        atol=1e-8,
    )


def _compare_destination(
    left: object, right: object | None, context: OracleContext
) -> None:

    if right is None:
        raise OracleMismatch('destination requires competitor')
    # left GeometryArray; right GeoSeries
    gm_rows = list(left)  # type: ignore[call-overload]
    gpd_s = right
    if len(gm_rows) != len(gpd_s):  # type: ignore[arg-type]
        raise OracleMismatch('destination length mismatch')
    lxy = np.array([[g.x, g.y] for g in gm_rows], dtype=np.float64)
    rxy = np.array([[p.x, p.y] for p in gpd_s], dtype=np.float64)  # type: ignore[union-attr]
    wrap_longitude_allclose(
        lxy,
        rxy,
        OracleContext(
            operation=context.operation,
            kind=context.kind,
            unit='deg_destination',
            oracle_identity=context.oracle_identity,
        ),
    )
    # CRS check
    crs = getattr(left, 'crs', None)
    if crs is not None and hasattr(crs, 'to_epsg') and crs.to_epsg() != 4326:
        raise OracleMismatch('destination CRS not 4326')


def _compare_index_build(
    left: object, right: object | None, context: OracleContext
) -> None:
    import shapely

    if right is None:
        raise OracleMismatch('index build requires competitor')
    gm_idx = left
    sh_tree = right
    if len(gm_idx) != 10_000:  # type: ignore[arg-type]
        raise OracleMismatch(
            'gometry index length',
            details=str(len(gm_idx)),  # type: ignore[arg-type]
        )
    if len(sh_tree) != 10_000:  # type: ignore[arg-type]
        raise OracleMismatch(
            'shapely tree length',
            details=str(len(sh_tree)),  # type: ignore[arg-type]
        )
    probes = index_build_probe_boxes()
    for box in probes:
        gm_c = set(np.asarray(gm_idx.candidates(box)).tolist())  # type: ignore[union-attr]
        sh_c = set(
            np.asarray(sh_tree.query(shapely.box(*box.bounds))).tolist()  # type: ignore[union-attr]
        )
        if gm_c != sh_c:
            raise OracleMismatch(
                'index build probe candidates mismatch',
                details=f'gm={sorted(gm_c)[:8]} sh={sorted(sh_c)[:8]}',
            )


def _compare_nearest(
    left: object, right: object | None, context: OracleContext
) -> None:
    if right is None:
        raise OracleMismatch('nearest requires competitor')
    # left: (query_ids, tree_ids, dist)
    lq, lt, ld = left  # type: ignore[misc]
    rq, rt, rd = right  # type: ignore[misc]
    normalized_index_pairs(
        (lq, lt),
        (rq, rt),
        context,
    )
    metric_allclose(
        ld,
        rd,
        OracleContext(
            operation=context.operation,
            kind=context.kind,
            unit='m',
            oracle_identity=context.oracle_identity,
        ),
    )
    # uniqueness: each query once
    lq_a = np.asarray(lq)
    if lq_a.size != np.unique(lq_a).size:
        raise OracleMismatch('nearest query ids not unique')


def _compare_from_geojson(
    left: object, right: object | None, context: OracleContext
) -> None:
    import shapely

    if right is None:
        raise OracleMismatch('from_geojson requires competitor')
    if len(left) != 217:  # type: ignore[arg-type]
        raise OracleMismatch(
            'from_geojson country count',
            details=str(len(left)),  # type: ignore[arg-type]
        )
    if len(right) != 217:  # type: ignore[arg-type]
        raise OracleMismatch(
            'from_geojson competitor count',
            details=str(len(right)),  # type: ignore[arg-type]
        )
    # coordinate count
    n_coords = 0
    for g in left:  # type: ignore[union-attr]
        n_coords += len(g.coords)
    if n_coords != 16_604:
        raise OracleMismatch('from_geojson coordinate count', details=str(n_coords))
    # Rowwise geometric equality is required. Equal bounds alone must NOT pass
    # unequal shapes; when exact OGC equals fails on presentation noise, accept
    # only a tiny Hausdorff distance (never bounds-as-equality).
    for i, (g, s) in enumerate(zip(left, right, strict=True)):  # type: ignore[arg-type]
        g_xy = g.set_crs(None) if getattr(g, 'crs', None) is not None else g
        if bool(shapely.equals(_as_shapely(g_xy), _as_shapely(s))):
            continue
        try:
            hausdorff = float(
                shapely.hausdorff_distance(_as_shapely(g_xy), _as_shapely(s))
            )
        except Exception as exc:
            raise OracleMismatch(
                'from_geojson geometry mismatch',
                details=(
                    f'row={i} equals=False hausdorff_error={exc!r} '
                    f'bounds_gm={g.bounds!r} bounds_sh={s.bounds!r}'
                ),
            ) from exc
        if hausdorff > 1e-9:
            raise OracleMismatch(
                'from_geojson geometry mismatch',
                details=(
                    f'row={i} equals=False hausdorff={hausdorff!r} '
                    f'bounds_gm={g.bounds!r} bounds_sh={s.bounds!r}'
                ),
            )


def _compare_join(left: object, right: object | None, context: OracleContext) -> None:
    if right is None:
        raise OracleMismatch('join requires competitor')
    normalized_index_pairs(left, right, context)
    q, _t = left  # type: ignore[misc]
    n = len(np.asarray(q))
    if n != 10_046:
        raise OracleMismatch('join pair count', details=f'got {n}, want 10046')
    mult = Counter(np.asarray(q).tolist())
    ones = sum(1 for v in mult.values() if v == 1)
    twos = sum(1 for v in mult.values() if v == 2)
    if ones != 9_954 or twos != 46:
        raise OracleMismatch(
            'join multiplicities',
            details=f'ones={ones} twos={twos}',
        )


def _compare_h3_compact(
    left: object, right: object | None, context: OracleContext
) -> None:
    normalized_uint64_set(left, right, context)
    # uncompact recovers source — checked in precondition with shared ids


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def build_from_wkb() -> PublicCase:
    ewkb = mixed_ewkb_10k()

    def gometry_call():
        import gometry as gm

        return gm.from_wkb(ewkb)

    def competitor_call():
        import shapely

        return shapely.from_wkb(ewkb, on_invalid='raise')

    _reg('gometry.from_wkb.batch/10k_mixed_ewkb', gometry_call)
    _reg('shapely.from_wkb.batch/10k_mixed_ewkb', competitor_call)

    def pre() -> None:
        # same object identity
        assert ewkb is mixed_ewkb_10k()

    return _case(
        gometry_call,
        competitor_call,
        _compare_ewkb_roundtrip,
        kind='parsed_geometry_exact',
        preconditions=(pre,),
    )


def _compare_from_wkt(
    left: object, right: object | None, context: OracleContext
) -> None:
    """Check every parsed row's recursive WKB structure exactly."""
    if right is None:
        raise OracleMismatch('from_wkt requires competitor')
    left_rows = _as_geom_rows(left)
    right_rows = _as_geom_rows(right)
    if len(left_rows) != 10_000 or len(right_rows) != 10_000:
        raise OracleMismatch(
            'from_wkt row count mismatch',
            details=f'{len(left_rows)} vs {len(right_rows)} (want 10000)',
        )
    parsed_geometry_exact(left_rows, right_rows, context)


def build_from_wkt() -> PublicCase:
    wkts = mixed_wkt_10k()

    def gometry_call():
        import gometry as gm

        return gm.from_wkt(wkts)

    def competitor_call():
        import shapely

        return shapely.from_wkt(wkts)

    _reg('gometry.from_wkt.batch/10k_realistic_mixed_wkt', gometry_call)
    _reg('shapely.from_wkt.batch/10k_realistic_mixed_wkt', competitor_call)
    return _case(
        gometry_call,
        competitor_call,
        _compare_from_wkt,
        kind='parsed_geometry_exact',
        preconditions=(lambda: len(wkts) == 10_000,),
    )


def build_from_arrow() -> PublicCase:
    arrow = arrow_mixed_100k()

    def gometry_call():
        import gometry as gm

        return gm.from_arrow(arrow)

    def competitor_call():
        import geopandas as gpd

        return gpd.GeoSeries.from_arrow(arrow)

    _reg('gometry.from_arrow/100k_mixed_10pct_missing', gometry_call)
    _reg(
        'geopandas.GeoSeries.from_arrow/100k_mixed_10pct_missing',
        competitor_call,
    )
    return _case(
        gometry_call,
        competitor_call,
        _compare_from_arrow,
        kind='rowwise_geometry_topological',
    )


def build_to_wkb() -> PublicCase:
    gm_mixed = mixed_10k_gometry()
    sh_mixed = mixed_10k_shapely()

    def gometry_call():
        return gm_mixed.to_wkb(include_srid=True)

    def competitor_call():
        import shapely

        return shapely.to_wkb(
            sh_mixed,
            hex=False,
            output_dimension=2,
            byte_order=1,
            include_srid=True,
            flavor='extended',
        )

    _reg('gometry.to_wkb.batch/10k_mixed_ewkb', gometry_call)
    _reg('shapely.to_wkb.batch/10k_mixed_ewkb', competitor_call)
    return _case(gometry_call, competitor_call, _compare_to_wkb, kind='exact_bytes')


def build_coords_matrix_and_index() -> PublicCase:
    gm_roads = roads_10k_gometry()
    sh_roads = roads_10k_shapely()

    def gometry_call():

        view = gm_roads.coords
        return np.asarray(view, dtype=np.float64), view.row_index

    def competitor_call():
        import shapely

        return shapely.get_coordinates(sh_roads, include_z=False, return_index=True)

    _reg('gometry.coords.matrix_and_index/100k_vertices', gometry_call)
    _reg('shapely.get_coordinates/100k_vertices_with_index', competitor_call)
    return _case(
        gometry_call,
        competitor_call,
        _compare_coords_matrix_and_row_index,
        kind='exact_coordinates',
        unit='m',
    )


def build_points() -> PublicCase:
    x, y = points_xy_numpy()

    def gometry_call():
        import gometry as gm

        return gm.points(x, y)

    def competitor_call():
        import shapely

        return shapely.points(x, y)

    _reg('gometry.points/10k_numpy_xy', gometry_call)
    _reg('shapely.points/10k_numpy_xy', competitor_call)
    return _case(
        gometry_call, competitor_call, _compare_points, kind='exact_coordinates'
    )


def build_contains_xy() -> PublicCase:
    data = prepared_polygon_and_probes()
    polygon = data['gm_poly']
    sh_poly = data['sh_poly']
    xs, ys = data['xs'], data['ys']

    def gometry_call():
        import gometry as gm

        return gm.contains_xy(polygon, xs, ys)

    def competitor_call():
        import shapely

        return shapely.contains_xy(sh_poly, xs, ys)

    _reg(
        'gometry.contains_xy/prepared_100k_probes_1316_vertex_polygon',
        gometry_call,
    )
    _reg(
        'shapely.contains_xy.prepared/100k_probes_1316_vertex_polygon',
        competitor_call,
    )
    return _case(
        gometry_call,
        competitor_call,
        exact_mask,
        kind='exact_mask',
        preconditions=(_precondition_prepared_hit_rate,),
    )


def build_intersects_polygon_points() -> PublicCase:
    data = intersects_polygon_and_points()

    def gometry_call():
        import gometry as gm

        return gm.intersects(data['gm_polygon'], data['gm_points'])

    def competitor_call():
        import shapely

        return shapely.intersects(data['sh_polygon'], data['sh_points'])

    def precondition() -> None:
        result = np.asarray(gometry_call())
        for category, expected in (
            ('interior', True),
            ('boundary', True),
            ('exterior', False),
        ):
            values = result[np.asarray([label == category for label in data['labels']])]
            if values.size == 0 or not np.all(values == expected):
                raise OracleMismatch('intersects category coverage', details=category)

    _reg('gometry.intersects/12k_irregular_polygon_point_array', gometry_call)
    _reg('shapely.intersects/12k_irregular_polygon_point_array', competitor_call)
    return _case(
        gometry_call,
        competitor_call,
        exact_mask,
        kind='exact_mask',
        preconditions=(precondition,),
    )


def build_dwithin() -> PublicCase:
    d = dwithin_pairs_10k()

    def gometry_call():
        import gometry as gm

        return gm.dwithin(d['gm_a'], d['gm_b'], 100.0)

    def competitor_call():
        import shapely

        return shapely.dwithin(d['sh_a'], d['sh_b'], 100.0)

    _reg('gometry.dwithin/pairwise_10k_50pct_matches', gometry_call)
    _reg('shapely.dwithin/pairwise_10k_50pct_matches', competitor_call)
    return _case(
        gometry_call,
        competitor_call,
        exact_mask,
        kind='exact_mask',
        preconditions=(_precondition_dwithin_count,),
    )


def build_area() -> PublicCase:
    gm_b = buildings_10k_gometry()
    sh_b = buildings_10k_shapely()

    def gometry_call():
        return gm_b.area

    def competitor_call():
        import shapely

        return shapely.area(sh_b)

    def compare(left: object, right: object | None, context: OracleContext) -> None:
        # gometry's area kernel origin-shifts each ring (src/geometry/area.rs), so
        # it matches GEOS to machine precision even at UTM-scale coordinates
        # (verified max ~6e-16 relative on this fixture). Strict tolerance holds.
        metric_allclose(left, right, context, rtol=1e-12, atol=1e-6)

    _reg('gometry.area/10k_projected_buildings', gometry_call)
    _reg('shapely.area/10k_projected_buildings', competitor_call)
    return _case(
        gometry_call,
        competitor_call,
        compare,
        kind='metric_allclose',
        unit='m2',
    )


def build_bounds() -> PublicCase:
    gm_buildings = buildings_10k_gometry()
    sh_buildings = buildings_10k_shapely()

    def gometry_call():
        return gm_buildings.bounds

    def competitor_call():
        import shapely

        return shapely.bounds(sh_buildings)

    def compare(left: object, right: object | None, context: OracleContext) -> None:
        if right is None:
            raise OracleMismatch('bounds requires competitor')
        a = np.asarray(left)
        b = np.asarray(right)
        if a.dtype != np.float64 or b.dtype != np.float64:
            raise OracleMismatch(
                'bounds requires float64 output', details=f'{a.dtype} vs {b.dtype}'
            )
        if a.shape != (10_000, 4) or b.shape != (10_000, 4):
            raise OracleMismatch(
                'bounds shape mismatch', details=f'{a.shape} vs {b.shape}'
            )
        if np.isnan(a).any() or np.isnan(b).any():
            raise OracleMismatch('bounds projected buildings unexpectedly masked')
        exact_coordinates(a, b, context, rtol=0.0, atol=0.0)

    _reg('gometry.bounds/10k_projected_buildings', gometry_call)
    _reg('shapely.bounds/10k_projected_buildings', competitor_call)
    return _case(
        gometry_call,
        competitor_call,
        compare,
        kind='float64_bounds',
        unit='m',
    )


def build_length() -> PublicCase:
    gm_r = roads_10k_gometry()
    sh_r = roads_10k_shapely()

    def gometry_call():
        return gm_r.length

    def competitor_call():
        import shapely

        return shapely.length(sh_r)

    _reg('gometry.length/10k_roads_100k_vertices', gometry_call)
    _reg('shapely.length/10k_roads_100k_vertices', competitor_call)
    return _case(
        gometry_call,
        competitor_call,
        metric_allclose,
        kind='metric_allclose',
        unit='m',
    )


def build_simplify() -> PublicCase:
    gm_r = roads_10k_gometry()
    sh_r = roads_10k_shapely()

    def gometry_call():
        return gm_r.simplify(2.0, method='dp', preserve_topology=False)

    def competitor_call():
        import shapely

        return shapely.simplify(sh_r, 2.0, preserve_topology=False)

    _reg('gometry.simplify.dp/10k_roads_100k_vertices', gometry_call)
    _reg('shapely.simplify.dp/10k_roads_100k_vertices', competitor_call)
    return _case(
        gometry_call,
        competitor_call,
        _compare_simplify,
        kind='exact_coordinates',
        unit='m',
    )


def build_buffer() -> PublicCase:
    gm_p = points_10k_gometry()
    sh_p = points_10k_shapely()

    def gometry_call():
        return gm_p.buffer(
            25,
            cap_style='round',
            join_style='round',
            quadrant_segments=8,
            miter_limit=5,
            side='both',
        )

    def competitor_call():
        import shapely

        return shapely.buffer(
            sh_p,
            25,
            quad_segs=8,
            cap_style='round',
            join_style='round',
            mitre_limit=5,
            single_sided=False,
        )

    _reg('gometry.buffer/10k_projected_points', gometry_call)
    _reg('shapely.buffer/10k_projected_points', competitor_call)
    return _case(
        gometry_call,
        competitor_call,
        _compare_areal_result,
        kind='areal_result',
    )


def build_intersection() -> PublicCase:
    d = intersection_pairs_1k()

    def gometry_call():
        import gometry as gm

        return gm.intersection(d['gm_a'], d['gm_b'])

    def competitor_call():
        import shapely

        return shapely.intersection(d['sh_a'], d['sh_b'])

    _reg('gometry.intersection/pairwise_1k_irregular_polygons', gometry_call)
    _reg('shapely.intersection/pairwise_1k_irregular_polygons', competitor_call)
    return _case(
        gometry_call,
        competitor_call,
        _compare_areal_result,
        kind='areal_result',
    )


def build_intersects_pairs() -> PublicCase:
    d = intersection_pairs_1k()

    def gometry_call():
        import gometry as gm

        return gm.intersects(d['gm_a'], d['gm_b'])

    def competitor_call():
        import shapely

        return shapely.intersects(d['sh_a'], d['sh_b'])

    _reg('gometry.intersects/pairwise_1k_irregular_polygons', gometry_call)
    _reg('shapely.intersects/pairwise_1k_irregular_polygons', competitor_call)
    return _case(gometry_call, competitor_call, exact_mask, kind='exact_mask')


def build_union_all() -> PublicCase:
    d = service_areas_1024()

    def gometry_call():
        import gometry as gm

        return gm.union_all(d['gm'])

    def competitor_call():
        import shapely

        return shapely.union_all(d['sh'])

    def output_validator(value: object) -> None:
        if value is None or bool(value.is_empty):  # type: ignore[union-attr]
            raise ValueError('union_all output is empty')

    _reg('gometry.union_all/1024_service_areas', gometry_call)
    _reg('shapely.union_all/1024_service_areas', competitor_call)
    return _case(
        gometry_call,
        competitor_call,
        _compare_areal_result,
        kind='areal_result',
        useful_work={'count': 1_024, 'unit': 'input-geometries'},
        output_validator=output_validator,
    )


def build_coverage_union() -> PublicCase:
    d = parcels_10k()

    def gometry_call():
        import gometry as gm

        return gm.coverage_union_all(d['gm'])

    def competitor_call():
        import shapely

        return shapely.coverage_union_all(d['sh'])

    def pre() -> None:
        import shapely

        if not hasattr(shapely, 'coverage_is_valid'):
            raise OracleMismatch('coverage validity unavailable on Shapely competitor')
        if not bool(np.all(shapely.coverage_is_valid(d['sh']))):
            raise OracleMismatch('parcels not coverage-valid on shapely')

    _reg('gometry.coverage_union_all/10k_edge_matched_parcels', gometry_call)
    _reg('shapely.coverage_union_all/10k_edge_matched_parcels', competitor_call)
    return _case(
        gometry_call,
        competitor_call,
        _compare_areal_result,
        kind='areal_result',
        preconditions=(pre,),
    )


def build_is_valid() -> PublicCase:
    gm_v = validity_10k_gometry()
    sh_v = validity_10k_shapely()

    def gometry_call():
        return gm_v.is_valid

    def competitor_call():
        import shapely

        return shapely.is_valid(sh_v)

    _reg('gometry.is_valid/10k_mixed_polygons_20pct_invalid', gometry_call)
    _reg('shapely.is_valid/10k_mixed_polygons_20pct_invalid', competitor_call)
    return _case(
        gometry_call,
        competitor_call,
        exact_mask,
        kind='exact_mask',
        preconditions=(_precondition_validity_dist,),
    )


def build_repair() -> PublicCase:
    gm_r = repair_1k_gometry()
    sh_r = repair_1k_shapely()

    def gometry_call():
        return gm_r.repair(method='linework')

    def competitor_call():
        import shapely

        return shapely.make_valid(sh_r, method='linework', keep_collapsed=True)

    _reg('gometry.repair.linework/1k_invalid_polygons', gometry_call)
    _reg('shapely.make_valid.linework/1k_invalid_polygons', competitor_call)
    return _case(
        gometry_call,
        competitor_call,
        _compare_repair,
        kind='repair_result',
    )


def _compare_clip_by_rect(
    left: object, right: object | None, context: OracleContext
) -> None:
    if right is None:
        raise OracleMismatch('clip_by_rect requires competitor')
    left_rows = _as_geom_rows(left)
    right_rows = _as_geom_rows(right)
    if len(left_rows) != 10_000 or len(right_rows) != 10_000:
        raise OracleMismatch(
            'clip_by_rect row alignment mismatch',
            details=f'{len(left_rows)} vs {len(right_rows)}',
        )
    for i, (lg, rg) in enumerate(zip(left_rows, right_rows, strict=True)):
        if lg is None or rg is None:
            raise OracleMismatch(f'clip_by_rect unexpected missing row={i}')
    linework_result_equivalent(left_rows, right_rows, context)


def build_clip_by_rect() -> PublicCase:
    data = clip_linework_10k()

    def gometry_call():
        return data['gm'].clip_by_rect(0.0, 0.0, 1.0, 1.0)

    def competitor_call():
        import shapely

        return shapely.clip_by_rect(data['sh'], 0.0, 0.0, 1.0, 1.0)

    def precondition() -> None:
        result = _as_geom_rows(gometry_call())
        if len(result) != 10_000:
            raise OracleMismatch('clip_by_rect precondition lost row alignment')
        categories = data['categories']
        counts = dict.fromkeys(('crossing', 'inside', 'outside'), 0)
        for i, (category, source, clipped) in enumerate(
            zip(categories, data['gm'], result, strict=True)
        ):
            counts[category] += 1
            if clipped is None:
                raise OracleMismatch(f'clip_by_rect returned missing row={i}')
            source_sh = _as_shapely(source)
            clipped_sh = _as_shapely(clipped)
            import shapely

            if category == 'crossing':
                if bool(shapely.is_empty(clipped_sh)) or bool(
                    shapely.equals(source_sh, clipped_sh)
                ):
                    raise OracleMismatch(
                        'clip_by_rect crossing category was not clipped',
                        details=f'row={i}',
                    )
            elif category == 'inside':
                if bool(shapely.is_empty(clipped_sh)):
                    raise OracleMismatch(
                        'clip_by_rect inside category became empty', details=f'row={i}'
                    )
            elif not bool(shapely.is_empty(clipped_sh)):
                raise OracleMismatch(
                    'clip_by_rect outside category was not empty', details=f'row={i}'
                )
        if counts != {'crossing': 4_000, 'inside': 4_000, 'outside': 2_000}:
            raise OracleMismatch(
                'clip_by_rect category distribution drifted', details=repr(counts)
            )

    _reg('gometry.clip_by_rect/10k_realistic_linework_categories', gometry_call)
    _reg('shapely.clip_by_rect/10k_realistic_linework_categories', competitor_call)
    return _case(
        gometry_call,
        competitor_call,
        _compare_clip_by_rect,
        kind='linework_result',
        preconditions=(precondition,),
    )


def build_point_on_surface() -> PublicCase:
    gm_buildings = buildings_10k_gometry()
    sh_buildings = buildings_10k_shapely()

    def gometry_call():
        return gm_buildings.point_on_surface()

    def competitor_call():
        import shapely

        return shapely.point_on_surface(sh_buildings)

    def precondition() -> None:
        holes = sum(1 for geometry in gm_buildings if len(geometry.interiors) > 0)
        if holes != 2_000:
            raise OracleMismatch(
                'point_on_surface holed source count', details=str(holes)
            )
        if len(gm_buildings) != 10_000:
            raise OracleMismatch(
                'point_on_surface source count', details=str(len(gm_buildings))
            )
        concave = False
        for geometry in gm_buildings:
            coords = list(
                zip(geometry.exterior.coords.x, geometry.exterior.coords.y, strict=True)
            )
            turns = []
            for first, second, third in zip(
                coords, coords[1:], coords[2:], strict=False
            ):
                turns.append(
                    (second[0] - first[0]) * (third[1] - second[1])
                    - (second[1] - first[1]) * (third[0] - second[0])
                )
            if any(turn > 0.0 for turn in turns) and any(turn < 0.0 for turn in turns):
                concave = True
                break
        if not concave:
            raise OracleMismatch('point_on_surface fixture has no concave sources')

    def compare(left: object, right: object | None, context: OracleContext) -> None:
        non_unique_points_contract(left, right, context, sources=gm_buildings)

    _reg('gometry.point_on_surface/10k_concave_holed_buildings', gometry_call)
    _reg('shapely.point_on_surface/10k_concave_holed_buildings', competitor_call)
    return _case(
        gometry_call,
        competitor_call,
        compare,
        kind='non_unique_points_contract',
        preconditions=(precondition,),
    )


def build_to_crs() -> PublicCase:
    d = masked_crs_200k()

    def gometry_call():
        return d['gm'].to_crs(3857)

    def competitor_call():
        return d['gpd'].to_crs(3857)

    _reg('gometry.to_crs.masked/200k_points_10pct_missing', gometry_call)
    _reg('geopandas.GeoSeries.to_crs/200k_points_10pct_missing', competitor_call)
    return _case(
        gometry_call,
        competitor_call,
        _compare_to_crs,
        kind='exact_coordinates',
        unit='m_3857',
    )


def build_crs_transform() -> PublicCase:
    lon, lat = webmerc_transform_10k()
    from pyproj import Transformer

    transformer = Transformer.from_crs(4326, 3857, always_xy=True)

    def gometry_call():
        import gometry as gm

        return gm.crs_transform(4326, 3857, lon, lat)

    def competitor_call():
        x, y = transformer.transform(lon, lat)
        return np.column_stack([x, y])

    _reg('gometry.crs_transform/10k_epsg4326_to3857', gometry_call)
    _reg('pyproj.Transformer.transform/10k_epsg4326_to3857', competitor_call)
    return _case(
        gometry_call,
        competitor_call,
        _compare_crs_transform,
        kind='exact_coordinates',
        unit='m_3857',
    )


def build_distance_geodesic() -> PublicCase:
    d = geodesic_pairs_10k()
    from pyproj import Geod

    geod = Geod(ellps='WGS84')

    def gometry_call():
        import gometry as gm

        return gm.distance(d['gm_a'], d['gm_b'], unit='meters')

    def competitor_call():
        _az12, _az21, dist = geod.inv(d['lon1'], d['lat1'], d['lon2'], d['lat2'])
        return np.asarray(dist, dtype=np.float64)

    _reg('gometry.distance.geodesic/10k_wgs84_pairs', gometry_call)
    _reg('pyproj.Geod.inv/10k_wgs84_pairs', competitor_call)
    return _case(
        gometry_call,
        competitor_call,
        metric_allclose,
        kind='metric_allclose',
        unit='m_geodesic',
    )


def build_destination() -> PublicCase:
    d = destination_inputs_10k()
    from pyproj import Geod

    geod = Geod(ellps='WGS84')

    def gometry_call():
        return d['starts'].destination(
            d['az'], d['dist'], path='geodesic', unit='meters'
        )

    def competitor_call():
        import geopandas as gpd
        import shapely

        lon, lat, _back = geod.fwd(
            d['lon'],
            d['lat'],
            d['az'],
            d['dist'],
            return_back_azimuth=False,
        )
        pts = shapely.points(lon, lat)
        return gpd.GeoSeries(pts, crs=4326)

    _reg('gometry.destination.geodesic/10k_wgs84', gometry_call)
    _reg(
        'pyproj.Geod.fwd+Shapely.points+GeoPandas.GeoSeries/10k_wgs84',
        competitor_call,
    )
    return _case(
        gometry_call,
        competitor_call,
        _compare_destination,
        kind='wrap_longitude_allclose',
        unit='deg_destination',
    )


def build_h3_cover() -> PublicCase:
    br = brazil_geometry()
    import h3
    from h3.api import numpy_int

    h3_shape = h3.geo_to_h3shape(br.__geo_interface__)

    def gometry_call():
        import gometry as gm

        return gm.h3_cover(br, 5, cell_rule='center').to_numpy()

    def competitor_call():
        return np.asarray(numpy_int.h3shape_to_cells(h3_shape, 5), dtype=np.uint64)

    def pre() -> None:
        ids = gometry_call()
        n = len(np.asarray(ids))
        if n != 32_260:
            raise OracleMismatch('h3 cover cell count', details=f'got {n}, want 32260')

    _reg('gometry.h3_cover.center/BR_res5_32260_cells', gometry_call)
    _reg('h3.numpy_int.h3shape_to_cells/BR_res5_32260_cells', competitor_call)
    return _case(
        gometry_call,
        competitor_call,
        normalized_uint64_set,
        kind='normalized_uint64_set',
        preconditions=(pre,),
    )


def build_h3_compact() -> PublicCase:
    br = brazil_geometry()
    import gometry as gm
    from h3.api import numpy_int

    # Common sorted resolution-5 IDs (built outside timing)
    h3_ids = np.sort(
        gm.h3_cover(br, 5, cell_rule='center').to_numpy().astype(np.uint64)
    )
    gm_cells = gm.CellArray(h3_ids, type=gm.H3Cell)

    def gometry_call():
        return gm_cells.compact().to_numpy()

    def competitor_call():
        return np.asarray(numpy_int.compact_cells(h3_ids), dtype=np.uint64)

    def pre() -> None:
        compact = np.asarray(gometry_call(), dtype=np.uint64)
        if len(compact) != 1_732:
            raise OracleMismatch(
                'h3 compact count', details=f'got {len(compact)}, want 1732'
            )
        # uncompact recovers source
        recovered = np.sort(
            gm_cells.compact().uncompact(5).to_numpy().astype(np.uint64)
        )
        if not np.array_equal(recovered, h3_ids):
            raise OracleMismatch('h3 uncompact did not recover source ids')

    _reg('gometry.h3_compact/32260_to_1732_cells', gometry_call)
    _reg('h3.numpy_int.compact_cells/32260_to_1732_cells', competitor_call)
    return _case(
        gometry_call,
        competitor_call,
        normalized_uint64_set,
        kind='normalized_uint64_set',
        preconditions=(pre,),
    )


def build_tile_cover() -> PublicCase:
    bbox = brazil_bbox_polygon()
    w, s, e, n = BRAZIL_BOUNDS

    def gometry_call():
        import gometry as gm

        return gm.tile_cover(bbox, zoom=10, cell_rule='bbox', max_cells=None)

    def competitor_call():
        import mercantile

        return tuple(mercantile.tiles(w, s, e, n, [10], truncate=False))

    def pre() -> None:
        tiles = gometry_call()
        n_tiles = len(tiles)
        if n_tiles != 15_340:
            raise OracleMismatch(
                'tile cover count', details=f'got {n_tiles}, want 15340'
            )

    def output_validator(value: object) -> None:
        if value is None or len(value) != 15_340:  # type: ignore[arg-type]
            raise ValueError('expected 15340 output tiles')

    _reg('gometry.tile_cover.bbox/BR_z10_15340_tiles', gometry_call)
    _reg('mercantile.tiles/BR_z10_15340_tiles', competitor_call)
    return _case(
        gometry_call,
        competitor_call,
        normalized_tile_set,
        kind='normalized_tile_set',
        preconditions=(pre,),
        useful_work={'count': 15_340, 'unit': 'output-tiles'},
        output_validator=output_validator,
    )


def build_index_join() -> PublicCase:
    countries = load_country_parts()
    pois_gm = country_pois_10k()
    pois_sh = country_pois_10k_shapely()
    import gometry as gm
    import shapely

    country_index = gm.SpatialIndex(countries)
    sh_parts = [shapely.from_wkb(g.to_wkb()) for g in countries]
    tree = shapely.STRtree(sh_parts)

    def gometry_call():
        return country_index.join(pois_gm, predicate='within')

    def competitor_call():
        pairs = tree.query(pois_sh, predicate='within')
        # Shapely 2 array query: pairs[0]=input/query indices, pairs[1]=tree
        return pairs[0], pairs[1]

    _reg('gometry.index.join.within/10k_pois_217_countries', gometry_call)
    _reg('shapely.STRtree.query.within/10k_pois_217_countries', competitor_call)
    return _case(
        gometry_call,
        competitor_call,
        _compare_join,
        kind='normalized_index_pairs',
    )


def build_join_within() -> PublicCase:
    countries = load_country_parts()
    pois_gm = country_pois_10k()
    pois_sh = country_pois_10k_shapely()
    import shapely

    sh_parts = [shapely.from_wkb(g.to_wkb()) for g in countries]

    def gometry_call():
        import gometry as gm

        return gm.join(pois_gm, countries, predicate='within')

    def competitor_call():
        tree = shapely.STRtree(sh_parts)
        pairs = tree.query(pois_sh, predicate='within')
        return pairs[0], pairs[1]

    _reg('gometry.join.within/10k_pois_217_countries', gometry_call)
    _reg(
        'shapely.STRtree.query.one_shot.within/10k_pois_217_countries', competitor_call
    )
    return _case(
        gometry_call, competitor_call, _compare_join, kind='normalized_index_pairs'
    )


def build_geohash_encode() -> PublicCase:
    data = geohash_encode_inputs()

    def gometry_tokens(lon, lat, precision):
        import gometry as gm

        return list(gm.geohash_cells(lon, lat, precision=precision).token)

    def gometry_call():
        return gometry_tokens(data['lon'], data['lat'], 6)

    def competitor_call():
        import pygeohash

        return [
            pygeohash.encode(float(lat), float(lon), precision=6)
            for lon, lat in zip(data['lon'], data['lat'], strict=True)
        ]

    def precondition() -> None:
        import pygeohash

        for precision in (1, 6, 12):
            # gometry canonicalizes longitude +180 to the same meridian at -180.
            expected = [
                pygeohash.encode(
                    float(lat),
                    -180.0 if lon == 180.0 else float(lon),
                    precision=precision,
                )
                for lon, lat in zip(data['edge_lon'], data['edge_lat'], strict=True)
            ]
            if (
                gometry_tokens(data['edge_lon'], data['edge_lat'], precision)
                != expected
            ):
                raise OracleMismatch(
                    'geohash edge probe mismatch', details=str(precision)
                )

    def compare(left, right, context):
        if list(left) != list(right):
            raise OracleMismatch('geohash token list mismatch')

    _reg('gometry.geohash_encode/10k_precision6', gometry_call)
    _reg('pygeohash.encode/10k_precision6', competitor_call)
    return _case(
        gometry_call,
        competitor_call,
        compare,
        kind='exact_token_list',
        preconditions=(precondition,),
    )


def build_index_candidates() -> PublicCase:
    buildings_gm = buildings_10k_gometry()
    buildings_sh = buildings_10k_shapely()
    boxes = index_query_boxes_1k()
    import gometry as gm
    import shapely

    index = gm.SpatialIndex(buildings_gm)
    tree = shapely.STRtree(buildings_sh)

    def gometry_call():
        return index.candidates(boxes['gm']).to_pairs()

    def competitor_call():
        pairs = tree.query(boxes['sh'])
        # pairs[0]=query indices, pairs[1]=tree indices
        return pairs[0], pairs[1]

    _reg('gometry.index.candidates/1k_queries_10k_polygons', gometry_call)
    _reg('shapely.STRtree.query/1k_queries_10k_polygons', competitor_call)
    return _case(
        gometry_call,
        competitor_call,
        normalized_index_pairs,
        kind='normalized_index_pairs',
    )


def build_index_nearest() -> PublicCase:
    buildings_gm = buildings_10k_gometry()
    buildings_sh = buildings_10k_shapely()
    queries = index_nearest_queries_1k()
    import gometry as gm
    import shapely

    index = gm.SpatialIndex(buildings_gm)
    tree = shapely.STRtree(buildings_sh)

    def gometry_call():
        groups, dist = index.nearest(
            queries['gm'], k=1, return_distance=True, ties=False
        )
        q, t = groups.to_pairs()
        return q, t, dist

    def competitor_call():
        idx, dist = tree.query_nearest(
            queries['sh'],
            return_distance=True,
            all_matches=False,
            exclusive=False,
        )
        # idx[0]=query indices, idx[1]=tree indices (same layout as query)
        return idx[0], idx[1], dist

    def precondition() -> None:
        _q, _t, distances = gometry_call()
        values = np.asarray(distances, dtype=np.float64)
        if values.shape != (1_000,) or not np.all(np.isfinite(values)):
            raise OracleMismatch('nearest distance shape/finite precondition failed')
        if not np.all(values > 0.1):
            raise OracleMismatch(
                'nearest probes must be outside buildings with meaningful distances',
                details=f'min={float(values.min())}',
            )

    _reg('gometry.index.nearest/1k_queries_10k_polygons_k1', gometry_call)
    _reg(
        'shapely.STRtree.query_nearest/1k_queries_10k_polygons_k1',
        competitor_call,
    )
    return _case(
        gometry_call,
        competitor_call,
        _compare_nearest,
        kind='normalized_index_pairs+metric',
        unit='m',
        preconditions=(precondition,),
    )


def build_index_build() -> PublicCase:
    buildings_gm = buildings_10k_gometry()
    buildings_sh = buildings_10k_shapely()

    def gometry_call():
        import gometry as gm

        return gm.SpatialIndex(buildings_gm)

    def competitor_call():
        import shapely

        return shapely.STRtree(buildings_sh)

    _reg('gometry.index.build/10k_polygons', gometry_call)
    _reg('shapely.STRtree/10k_polygons', competitor_call)
    return _case(
        gometry_call,
        competitor_call,
        _compare_index_build,
        kind='index_build_probe',
    )


def build_from_geojson() -> PublicCase:
    text = load_country_geojson_text()

    def gometry_call():
        import gometry as gm

        return gm.from_geojson(text)

    def competitor_call():
        import shapely

        return shapely.get_parts(shapely.from_geojson(text))

    _reg('gometry.from_geojson/217_countries_2_26mb', gometry_call)
    _reg('shapely.from_geojson/217_countries_2_26mb', competitor_call)
    return _case(
        gometry_call,
        competitor_call,
        _compare_from_geojson,
        kind='rowwise_geometry_hausdorff',
    )


def build_chart_edge_area() -> PublicCase:
    parts_gm = load_country_parts()
    parts_sh = country_parts_shapely()
    from pyproj import Geod

    geod = Geod(ellps='WGS84')

    def gometry_call():
        return parts_gm.area

    def competitor_call():
        return polygonal_chart_areas(parts_sh, geod)

    _reg('gometry.chart_edge_area/217_countries', gometry_call)
    _reg('pyproj.chart_edge_area_reference/217_countries', competitor_call)
    return _case(
        gometry_call,
        competitor_call,
        metric_allclose,
        kind='metric_allclose',
        unit='m2_ellipsoidal',
    )


def build_chart_edge_length() -> PublicCase:
    ext_gm = country_exteriors_multilinestring()
    ext_sh = country_exteriors_shapely()
    from pyproj import Geod

    geod = Geod(ellps='WGS84')

    def gometry_call():
        return ext_gm.length

    def competitor_call():
        return country_exterior_chart_length(ext_sh.geoms, geod)

    _reg(
        'gometry.chart_edge_length/1034_exteriors_16135_vertices',
        gometry_call,
    )
    _reg(
        'pyproj.chart_edge_length_reference/1034_exteriors_16135_vertices',
        competitor_call,
    )
    return _case(
        gometry_call,
        competitor_call,
        metric_allclose,
        kind='metric_allclose',
        unit='m_ellipsoidal',
    )


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

BUILDERS: dict[str, Callable[[], PublicCase]] = {
    'gometry.from_wkb.batch/10k_mixed_ewkb': build_from_wkb,
    'gometry.from_arrow/100k_mixed_10pct_missing': build_from_arrow,
    'gometry.to_wkb.batch/10k_mixed_ewkb': build_to_wkb,
    'gometry.coords.matrix_and_index/100k_vertices': build_coords_matrix_and_index,
    'gometry.points/10k_numpy_xy': build_points,
    'gometry.from_wkt.batch/10k_realistic_mixed_wkt': build_from_wkt,
    'gometry.contains_xy/prepared_100k_probes_1316_vertex_polygon': build_contains_xy,
    'gometry.intersects/12k_irregular_polygon_point_array': build_intersects_polygon_points,
    'gometry.dwithin/pairwise_10k_50pct_matches': build_dwithin,
    'gometry.area/10k_projected_buildings': build_area,
    'gometry.bounds/10k_projected_buildings': build_bounds,
    'gometry.length/10k_roads_100k_vertices': build_length,
    'gometry.simplify.dp/10k_roads_100k_vertices': build_simplify,
    'gometry.buffer/10k_projected_points': build_buffer,
    'gometry.intersection/pairwise_1k_irregular_polygons': build_intersection,
    'gometry.intersects/pairwise_1k_irregular_polygons': build_intersects_pairs,
    'gometry.union_all/1024_service_areas': build_union_all,
    'gometry.coverage_union_all/10k_edge_matched_parcels': build_coverage_union,
    'gometry.is_valid/10k_mixed_polygons_20pct_invalid': build_is_valid,
    'gometry.repair.linework/1k_invalid_polygons': build_repair,
    'gometry.clip_by_rect/10k_realistic_linework_categories': build_clip_by_rect,
    'gometry.point_on_surface/10k_concave_holed_buildings': build_point_on_surface,
    'gometry.to_crs.masked/200k_points_10pct_missing': build_to_crs,
    'gometry.crs_transform/10k_epsg4326_to3857': build_crs_transform,
    'gometry.distance.geodesic/10k_wgs84_pairs': build_distance_geodesic,
    'gometry.destination.geodesic/10k_wgs84': build_destination,
    'gometry.h3_cover.center/BR_res5_32260_cells': build_h3_cover,
    'gometry.h3_compact/32260_to_1732_cells': build_h3_compact,
    'gometry.tile_cover.bbox/BR_z10_15340_tiles': build_tile_cover,
    'gometry.index.join.within/10k_pois_217_countries': build_index_join,
    'gometry.join.within/10k_pois_217_countries': build_join_within,
    'gometry.geohash_encode/10k_precision6': build_geohash_encode,
    'gometry.index.candidates/1k_queries_10k_polygons': build_index_candidates,
    'gometry.index.nearest/1k_queries_10k_polygons_k1': build_index_nearest,
    'gometry.index.build/10k_polygons': build_index_build,
    'gometry.from_geojson/217_countries_2_26mb': build_from_geojson,
    'gometry.chart_edge_area/217_countries': build_chart_edge_area,
    'gometry.chart_edge_length/1034_exteriors_16135_vertices': build_chart_edge_length,
}


def ensure_registered() -> None:
    """Idempotently register all public builders into PUBLIC_CASE_BUILDERS."""
    register_public_builders(BUILDERS)  # type: ignore[arg-type]


ensure_registered()
