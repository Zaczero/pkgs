"""Cross-library oracle: data model, frozen tolerances, and equivalence helpers.

Per-op case builders live in ``PUBLIC_CASE_BUILDERS`` (keyed by gometry row name).
The public case module registers one builder for each of the 37 operations.
An empty map fails closed at the oracle CLI (``validate_builders``), never
succeeds with zero verified ops.
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np

_SUPPORT = Path(__file__).resolve().parents[1] / 'support'
if str(_SUPPORT) not in sys.path:
    sys.path.insert(0, str(_SUPPORT))

if TYPE_CHECKING:
    from _bench_registry import ReleaseOperation

# ---------------------------------------------------------------------------
# Frozen numeric tolerances (spec §1)
# ---------------------------------------------------------------------------

# (rtol, atol) keyed by unit token used in OracleContext.unit
TOLERANCES: dict[str, tuple[float, float]] = {
    'm2': (1e-12, 1e-6),  # projected area
    'm': (1e-12, 1e-9),  # projected length / index distance
    'm_3857': (2e-12, 1e-7),  # EPSG:3857 coordinates
    'deg': (1e-12, 1e-10),  # geographic transformed coordinates
    'm_geodesic': (1e-12, 1e-6),  # geodesic distances
    'deg_destination': (0.0, 1e-11),  # destination coords (lon wrapped)
    'm2_ellipsoidal': (2e-12, 1e-3),  # 217-country area
    'm_ellipsoidal': (2e-12, 1e-6),  # country boundary length
}

# ``clip_by_rect`` already admits this coordinate budget in its public
# benchmark contract.  It is a distance budget, not an area budget.
_LINEWORK_COORD_RTOL = 1e-9
_LINEWORK_COORD_ATOL = 1e-8


@dataclass(frozen=True, slots=True)
class OracleContext:
    operation: ReleaseOperation
    kind: str
    unit: str | None = None
    oracle_identity: str | None = None


def public_oracle_identity(operation: ReleaseOperation) -> str:
    """Stable identity for the untimed oracle attached to one public operation."""
    return f'public:{operation.gometry}:oracle'


@dataclass(frozen=True, slots=True)
class PairCase:
    gometry_call: Callable[[], object]
    competitor_call: Callable[[], object] | None
    compare: Callable[[object, object | None, OracleContext], None]
    preconditions: tuple[Callable[[], None], ...] = ()

    def verify(self, context: OracleContext) -> None:
        for precondition in self.preconditions:
            precondition()
        gometry_result = self.gometry_call()
        competitor_result = (
            None if self.competitor_call is None else self.competitor_call()
        )
        self.compare(gometry_result, competitor_result, context)


class OracleMismatch(Exception):  # noqa: N818 — spec name; not an Error suffix
    """Raised when a public competitive pair fails equivalence."""

    def __init__(
        self,
        message: str,
        *,
        gometry_name: str | None = None,
        competitor_name: str | None = None,
        kind: str | None = None,
        unit: str | None = None,
        details: str | None = None,
    ) -> None:
        parts = [message]
        if gometry_name is not None or competitor_name is not None:
            parts.append(
                f'rows: gometry={gometry_name!r} competitor={competitor_name!r}'
            )
        if kind is not None:
            parts.append(f'kind={kind!r}')
        if unit is not None:
            parts.append(f'unit={unit!r}')
        if details:
            parts.append(details)
        super().__init__('; '.join(parts))
        self.gometry_name = gometry_name
        self.competitor_name = competitor_name
        self.kind = kind
        self.unit = unit
        self.details = details


# Register one builder per gometry row name on this dict (or call
# ``register_public_builders``). Keys must be RELEASE_OPERATIONS gometry names.
PUBLIC_CASE_BUILDERS: dict[str, Callable[[], PairCase]] = {}
PUBLIC_SCALING_CASE_BUILDERS: dict[str, Callable[[], PairCase]] = {}


def register_public_builders(
    builders: dict[str, Callable[[], PairCase]],
) -> None:
    """Merge public suite builders into the shared map."""
    PUBLIC_CASE_BUILDERS.update(builders)


def register_public_scaling_builders(
    builders: dict[str, Callable[[], PairCase]],
) -> None:
    """Merge the derived sparse scaling builders into the shared map."""
    PUBLIC_SCALING_CASE_BUILDERS.update(builders)


def _ctx_names(context: OracleContext) -> tuple[str, str | None]:
    return context.operation.gometry, context.operation.competitor


def _shape_dtype_detail(value: object, label: str) -> str:
    arr = np.asarray(value)
    return f'{label}: shape={arr.shape} dtype={arr.dtype}'


def _first_diff_mask(a: np.ndarray, b: np.ndarray) -> str:
    if a.shape != b.shape:
        return f'shape mismatch {a.shape} vs {b.shape}'
    unequal = a != b
    if not np.any(unequal):
        return 'no differing elements'
    flat = unequal.reshape(-1)
    idx = int(np.flatnonzero(flat)[0])
    multi = np.unravel_index(idx, a.shape) if a.ndim else (0,)
    return (
        f'first differing index={multi} '
        f'left={a.reshape(-1)[idx]!r} right={b.reshape(-1)[idx]!r}'
    )


def _error_stats(a: np.ndarray, b: np.ndarray) -> str:
    af = np.asarray(a, dtype=np.float64)
    bf = np.asarray(b, dtype=np.float64)
    if af.shape != bf.shape or af.size == 0:
        return 'max_abs=n/a max_rel=n/a'
    both_finite = np.isfinite(af) & np.isfinite(bf)
    if not np.any(both_finite):
        return 'max_abs=n/a max_rel=n/a'
    diff = np.abs(af[both_finite] - bf[both_finite])
    max_abs = float(np.max(diff))
    denom = np.maximum(np.abs(af[both_finite]), np.abs(bf[both_finite]))
    with np.errstate(divide='ignore', invalid='ignore'):
        rel = np.where(denom > 0, diff / denom, 0.0)
    max_rel = float(np.max(rel)) if rel.size else 0.0
    return f'max_abs={max_abs:.6g} max_rel={max_rel:.6g}'


def _raise(
    message: str,
    context: OracleContext,
    *,
    details: str | None = None,
) -> None:
    g_name, c_name = _ctx_names(context)
    raise OracleMismatch(
        message,
        gometry_name=g_name,
        competitor_name=c_name,
        kind=context.kind,
        unit=context.unit,
        details=details,
    )


# ---------------------------------------------------------------------------
# Equivalence helpers
# ---------------------------------------------------------------------------


def exact_mask(
    left: object,
    right: object | None,
    context: OracleContext,
) -> None:
    """Identical shape, boolean dtype, and values."""
    if right is None:
        _raise('exact_mask requires a competitor result', context)
    a = np.asarray(left)
    b = np.asarray(right)
    if a.dtype != np.bool_ or b.dtype != np.bool_:
        _raise(
            'exact_mask requires boolean dtype on both sides',
            context,
            details=(
                f'{_shape_dtype_detail(a, "gometry")}; '
                f'{_shape_dtype_detail(b, "competitor")}'
            ),
        )
    if a.shape != b.shape or not np.array_equal(a, b):
        _raise(
            'exact_mask mismatch',
            context,
            details=(
                f'{_shape_dtype_detail(a, "gometry")}; '
                f'{_shape_dtype_detail(b, "competitor")}; '
                f'{_first_diff_mask(a, b)}'
            ),
        )


def exact_coordinates(
    left: object,
    right: object | None,
    context: OracleContext,
    *,
    rtol: float | None = None,
    atol: float | None = None,
) -> None:
    """Identical shape/missingness and allclose; unit is mandatory."""
    if right is None:
        _raise('exact_coordinates requires a competitor result', context)
    unit = context.unit
    if not unit:
        _raise(
            'exact_coordinates requires a named unit on OracleContext',
            context,
        )
    a = np.asarray(left, dtype=np.float64)
    b = np.asarray(right, dtype=np.float64)
    if a.shape != b.shape:
        _raise(
            'exact_coordinates shape mismatch',
            context,
            details=(
                f'{_shape_dtype_detail(a, "gometry")}; '
                f'{_shape_dtype_detail(b, "competitor")}'
            ),
        )
    a_nan = np.isnan(a)
    b_nan = np.isnan(b)
    if not np.array_equal(a_nan, b_nan):
        _raise(
            'exact_coordinates missingness mismatch',
            context,
            details=_first_diff_mask(a_nan, b_nan),
        )
    tr, ta = TOLERANCES.get(unit, (1e-12, 1e-9))
    use_rtol = tr if rtol is None else rtol
    use_atol = ta if atol is None else atol
    finite = ~a_nan
    if not np.any(finite):
        return
    if not np.allclose(
        a[finite], b[finite], rtol=use_rtol, atol=use_atol, equal_nan=False
    ):
        _raise(
            'exact_coordinates allclose failed',
            context,
            details=(
                f'{_shape_dtype_detail(a, "gometry")}; '
                f'{_shape_dtype_detail(b, "competitor")}; '
                f'rtol={use_rtol} atol={use_atol}; '
                f'{_error_stats(a[finite], b[finite])}'
            ),
        )


def metric_allclose(
    left: object,
    right: object | None,
    context: OracleContext,
    *,
    rtol: float | None = None,
    atol: float | None = None,
) -> None:
    """Numeric allclose for metrics; refuses an empty/missing unit string."""
    if right is None:
        _raise('metric_allclose requires a competitor result', context)
    unit = context.unit
    if unit is None or unit == '':
        _raise(
            'metric_allclose refuses empty/missing unit '
            '(square-degree vs square-metre bug class)',
            context,
        )
    a = np.asarray(left, dtype=np.float64)
    b = np.asarray(right, dtype=np.float64)
    if a.shape != b.shape:
        _raise(
            'metric_allclose shape mismatch',
            context,
            details=(
                f'{_shape_dtype_detail(a, "gometry")}; '
                f'{_shape_dtype_detail(b, "competitor")}'
            ),
        )
    tr, ta = TOLERANCES.get(unit, (1e-12, 1e-9))
    use_rtol = tr if rtol is None else rtol
    use_atol = ta if atol is None else atol
    if not np.allclose(a, b, rtol=use_rtol, atol=use_atol, equal_nan=True):
        _raise(
            'metric_allclose failed',
            context,
            details=(
                f'{_shape_dtype_detail(a, "gometry")}; '
                f'{_shape_dtype_detail(b, "competitor")}; '
                f'rtol={use_rtol} atol={use_atol}; '
                f'{_error_stats(a, b)}; '
                f'{_first_diff_mask(a, b)}'
            ),
        )


def _as_uint64_1d(value: object) -> np.ndarray:
    arr = np.asarray(value)
    if arr.ndim == 0:
        arr = arr.reshape(1)
    return np.asarray(arr, dtype=np.uint64).reshape(-1)


def normalized_uint64_set(
    left: object,
    right: object | None,
    context: OracleContext,
) -> None:
    """Normalize, verify uniqueness, sort, compare exactly."""
    if right is None:
        _raise('normalized_uint64_set requires a competitor result', context)
    a = _as_uint64_1d(left)
    b = _as_uint64_1d(right)
    if a.size != np.unique(a).size:
        _raise(
            'normalized_uint64_set: gometry side has duplicate ids',
            context,
            details=f'count={a.size} unique={np.unique(a).size}',
        )
    if b.size != np.unique(b).size:
        _raise(
            'normalized_uint64_set: competitor side has duplicate ids',
            context,
            details=f'count={b.size} unique={np.unique(b).size}',
        )
    a_s = np.sort(a)
    b_s = np.sort(b)
    if a_s.shape != b_s.shape or not np.array_equal(a_s, b_s):
        length_note = (
            _first_diff_mask(a_s, b_s) if a_s.shape == b_s.shape else 'length differs'
        )
        _raise(
            'normalized_uint64_set mismatch',
            context,
            details=(
                f'gometry count={a_s.size} competitor count={b_s.size}; {length_note}'
            ),
        )


def _normalize_tiles(value: object) -> list[tuple[int, int, int]]:
    items: list[tuple[int, int, int]] = []
    if value is None:
        return items
    for item in value:  # type: ignore[union-attr]
        if hasattr(item, 'zoom') and hasattr(item, 'x') and hasattr(item, 'y'):
            items.append((int(item.zoom), int(item.x), int(item.y)))
        elif hasattr(item, 'z') and hasattr(item, 'x') and hasattr(item, 'y'):
            items.append((int(item.z), int(item.x), int(item.y)))
        else:
            z, x, y = item  # type: ignore[misc]
            items.append((int(z), int(x), int(y)))
    items.sort()
    return items


def normalized_tile_set(
    left: object,
    right: object | None,
    context: OracleContext,
) -> None:
    """Normalize to sorted (z, x, y) and compare exactly."""
    if right is None:
        _raise('normalized_tile_set requires a competitor result', context)
    a = _normalize_tiles(left)
    b = _normalize_tiles(right)
    if a != b:
        first = None
        for i, (la, lb) in enumerate(zip(a, b, strict=False)):
            if la != lb:
                first = f'index={i} left={la} right={lb}'
                break
        if first is None:
            first = f'length left={len(a)} right={len(b)}'
        _raise(
            'normalized_tile_set mismatch',
            context,
            details=first,
        )


def _normalize_pairs(value: object) -> list[tuple[int, int]]:
    """Accept (query_ids, tree_ids), Nx2 array, or iterable of pairs."""
    if value is None:
        return []
    if isinstance(value, tuple) and len(value) == 2:
        q, t = value
        q_arr = np.asarray(q, dtype=np.int64).reshape(-1)
        t_arr = np.asarray(t, dtype=np.int64).reshape(-1)
        if q_arr.shape != t_arr.shape:
            raise ValueError('pair id vectors must have equal length')
        pairs = list(zip(q_arr.tolist(), t_arr.tolist(), strict=True))
    else:
        arr = np.asarray(value)
        if arr.ndim == 2 and arr.shape[1] == 2:
            pairs = [(int(r[0]), int(r[1])) for r in arr]
        else:
            pairs = [(int(a), int(b)) for a, b in value]  # type: ignore[misc]
    pairs.sort()
    return pairs


def normalized_index_pairs(
    left: object,
    right: object | None,
    context: OracleContext,
) -> None:
    """Normalize to sorted (query_id, tree_id) and compare exactly."""
    if right is None:
        _raise('normalized_index_pairs requires a competitor result', context)
    try:
        a = _normalize_pairs(left)
        b = _normalize_pairs(right)
    except (TypeError, ValueError) as exc:
        _raise(f'normalized_index_pairs normalize failed: {exc}', context)
    if a != b:
        first = None
        for i, (la, lb) in enumerate(zip(a, b, strict=False)):
            if la != lb:
                first = f'index={i} left={la} right={lb}'
                break
        if first is None:
            first = f'length left={len(a)} right={len(b)}'
        _raise(
            'normalized_index_pairs mismatch',
            context,
            details=f'counts left={len(a)} right={len(b)}; {first}',
        )


def _geom_missing(g: object) -> bool:
    return g is None


def _as_sequence(value: object) -> Sequence[object]:
    if value is None:
        return ()
    if isinstance(value, (str, bytes)):
        return (value,)
    if isinstance(value, Sequence) and not hasattr(value, 'coords'):
        return value
    try:
        return list(value)  # type: ignore[arg-type]
    except TypeError:
        return (value,)


def _is_gometry_module(obj: object) -> bool:
    return type(obj).__module__.startswith('gometry')


def _as_shapely(geom: object):
    """Convert one result through WKB before the competitor-side judgment.

    The gometry result is data under test, not an oracle.  Both it and the
    competitor result take the same representation-only WKB in/out path so
    Shapely owns every subsequent geometry decision.
    """
    import shapely

    if _is_gometry_module(geom):
        payload = geom.to_wkb()  # type: ignore[union-attr]
    else:
        payload = shapely.to_wkb(geom)
    return shapely.from_wkb(bytes(payload))


def _coordinate_bits(geometry: object) -> tuple[tuple[int, ...], ...]:
    import shapely

    coordinates = np.asarray(
        shapely.get_coordinates(geometry, include_z=True, include_m=True),
        dtype=np.float64,
    )
    bits = coordinates.view(np.uint64).reshape(coordinates.shape)
    return tuple(tuple(int(value) for value in row) for row in bits)


def _structure_signature(geometry: object) -> tuple[object, ...]:
    import shapely

    kind = str(geometry.geom_type)
    axes = (bool(shapely.has_z(geometry)), bool(shapely.has_m(geometry)))
    empty = bool(shapely.is_empty(geometry))
    if kind == 'Polygon':
        children = (
            _structure_signature(geometry.exterior),
            *(_structure_signature(ring) for ring in geometry.interiors),
        )
        coordinates: tuple[tuple[int, ...], ...] = ()
    elif kind in {
        'MultiPoint',
        'MultiLineString',
        'MultiPolygon',
        'GeometryCollection',
    }:
        children = tuple(_structure_signature(part) for part in geometry.geoms)
        coordinates = ()
    else:
        children = ()
        coordinates = _coordinate_bits(geometry)
    return kind, axes, empty, coordinates, children


def _structure_difference(
    left: tuple[object, ...],
    right: tuple[object, ...],
    path: str = 'root',
) -> str | None:
    left_kind, left_axes, left_empty, left_coords, left_children = left
    right_kind, right_axes, right_empty, right_coords, right_children = right
    if left_kind != right_kind:
        return f'{path} kind {left_kind!r} vs {right_kind!r}'
    if left_axes != right_axes:
        return f'{path} axes {left_axes!r} vs {right_axes!r}'
    if left_empty != right_empty:
        return f'{path} empty={left_empty} vs {right_empty}'
    if left_coords != right_coords:
        return f'{path} ordinate binary64 values differ'
    if len(left_children) != len(right_children):
        return f'{path} child count {len(left_children)} vs {len(right_children)}'
    for index, (left_child, right_child) in enumerate(
        zip(left_children, right_children, strict=True)
    ):
        difference = _structure_difference(
            left_child,
            right_child,
            f'{path}.child[{index}]',
        )
        if difference is not None:
            return difference
    return None


def parsed_geometry_exact(
    left: object,
    right: object | None,
    context: OracleContext,
) -> None:
    """Compare parsed rows through exact Shapely/WKB structure.

    Both results cross the same representation-only WKB boundary.  Geometry
    kind, every collection/part and polygon-ring boundary, ordinate axes, row
    missingness, and the exact binary64 bits are then compared recursively.
    Topological equality is deliberately not used as parser evidence.
    """
    if right is None:
        _raise('parsed_geometry_exact requires a competitor result', context)
    left_rows = _as_sequence(left)
    right_rows = _as_sequence(right)
    if len(left_rows) != len(right_rows):
        _raise(
            'parsed_geometry_exact count mismatch',
            context,
            details=f'left={len(left_rows)} right={len(right_rows)}',
        )
    for i, (lg, rg) in enumerate(zip(left_rows, right_rows, strict=True)):
        left_missing = _geom_missing(lg)
        right_missing = _geom_missing(rg)
        if left_missing and right_missing:
            continue
        if left_missing or right_missing:
            _raise(
                'parsed_geometry_exact missingness mismatch',
                context,
                details=f'row={i}',
            )
        try:
            left_structure = _structure_signature(_as_shapely(lg))
            right_structure = _structure_signature(_as_shapely(rg))
        except Exception as exc:
            _raise(
                'parsed_geometry_exact WKB conversion failed',
                context,
                details=f'row={i}: {exc}',
            )
        difference = _structure_difference(left_structure, right_structure)
        if difference is not None:
            _raise(
                'parsed_geometry_exact structure mismatch',
                context,
                details=f'row={i}: {difference}',
            )


def _support_dimensions(geometry: object) -> frozenset[str]:
    import shapely

    if bool(shapely.is_empty(geometry)):
        return frozenset()
    kind = str(geometry.geom_type)
    if kind == 'Polygon':
        return frozenset({'areal'})
    if kind == 'MultiPolygon':
        dimensions: set[str] = set()
        for part in geometry.geoms:
            dimensions.update(_support_dimensions(part))
        return frozenset(dimensions)
    if kind in {'LineString', 'LinearRing'}:
        return frozenset({'line'})
    if kind == 'MultiLineString':
        dimensions = set()
        for part in geometry.geoms:
            dimensions.update(_support_dimensions(part))
        return frozenset(dimensions)
    if kind == 'Point':
        return frozenset({'point'})
    if kind == 'MultiPoint':
        dimensions = set()
        for part in geometry.geoms:
            dimensions.update(_support_dimensions(part))
        return frozenset(dimensions)
    if kind == 'GeometryCollection':
        dimensions = set()
        for part in geometry.geoms:
            dimensions.update(_support_dimensions(part))
        return frozenset(dimensions)
    return frozenset({'unknown'})


def _format_dimensions(dimensions: frozenset[str]) -> str:
    return ','.join(sorted(dimensions)) or 'empty'


def areal_result_equivalent(
    left: object,
    right: object | None,
    context: OracleContext,
    *,
    rtol: float = 1e-6,
    atol: float = 1e-3,
    allow_collapsed: bool = False,
) -> None:
    """Compare areal results with a bounded, support-aware area fallback.

    Non-repair callers must produce pure areal support.  Repair may produce a
    collapsed point or line, but only an exact topological match may admit that
    lower-dimensional result; the area fallback itself never admits one.
    """
    import shapely

    if right is None:
        _raise('areal_result_equivalent requires a competitor result', context)
    left_rows = _as_sequence(left)
    right_rows = _as_sequence(right)
    if len(left_rows) != len(right_rows):
        _raise(
            'areal_result_equivalent count mismatch',
            context,
            details=f'left={len(left_rows)} right={len(right_rows)}',
        )
    for i, (lg, rg) in enumerate(zip(left_rows, right_rows, strict=True)):
        left_missing = _geom_missing(lg)
        right_missing = _geom_missing(rg)
        if left_missing and right_missing:
            continue
        if left_missing or right_missing:
            _raise(
                'areal_result_equivalent missingness mismatch',
                context,
                details=f'row={i}',
            )
        try:
            left_shape = _as_shapely(lg)
            right_shape = _as_shapely(rg)
        except Exception as exc:
            _raise(
                'areal_result_equivalent WKB conversion failed',
                context,
                details=f'row={i}: {exc}',
            )
        left_empty = bool(shapely.is_empty(left_shape))
        right_empty = bool(shapely.is_empty(right_shape))
        if not allow_collapsed and str(left_shape.geom_type) != str(
            right_shape.geom_type
        ):
            _raise(
                'areal_result_equivalent kind mismatch',
                context,
                details=f'row={i}',
            )
        if left_empty and right_empty:
            continue
        left_dimensions = _support_dimensions(left_shape)
        right_dimensions = _support_dimensions(right_shape)
        if not allow_collapsed and (
            left_dimensions != frozenset({'areal'})
            or right_dimensions != frozenset({'areal'})
        ):
            _raise(
                'areal_result_equivalent lower-dimensional result',
                context,
                details=(
                    f'row={i} left={_format_dimensions(left_dimensions)} '
                    f'right={_format_dimensions(right_dimensions)}'
                ),
            )
        try:
            if bool(shapely.equals(left_shape, right_shape)):
                continue
            if allow_collapsed and left_dimensions != right_dimensions:
                _raise(
                    'areal_result_equivalent dimensionality mismatch',
                    context,
                    details=(
                        f'row={i} left={_format_dimensions(left_dimensions)} '
                        f'right={_format_dimensions(right_dimensions)}'
                    ),
                )
            symmetric_difference = shapely.symmetric_difference(left_shape, right_shape)
            residual_dimensions = _support_dimensions(symmetric_difference)
            if residual_dimensions and residual_dimensions != frozenset({'areal'}):
                _raise(
                    'areal_result_equivalent lower-dimensional residual',
                    context,
                    details=(
                        f'row={i} support={_format_dimensions(residual_dimensions)}'
                    ),
                )
            symmetric_difference_area = float(shapely.area(symmetric_difference))
            union_area = float(shapely.area(shapely.union(left_shape, right_shape)))
        except OracleMismatch:
            raise
        except Exception as exc:
            _raise(
                'areal_result_equivalent Shapely comparison failed',
                context,
                details=f'row={i}: {exc}',
            )
        budget = atol + rtol * abs(union_area)
        if (
            np.isfinite(symmetric_difference_area)
            and symmetric_difference_area <= budget
        ):
            continue
        _raise(
            'areal_result_equivalent failed',
            context,
            details=(
                f'row={i} symdiff_area={symmetric_difference_area:.6g} '
                f'union_area={union_area:.6g} budget={budget:.6g} '
                f'rtol={rtol} atol={atol}'
            ),
        )


def _linework_signature(geometry: object) -> tuple[object, ...]:
    kind = str(geometry.geom_type)
    if kind in {'MultiLineString', 'GeometryCollection'}:
        return kind, tuple(_linework_signature(part) for part in geometry.geoms)
    return (kind,)


def _linework_scale(left: object, right: object) -> float:
    import shapely

    bounds = np.concatenate((
        np.asarray(shapely.bounds(left), dtype=np.float64).reshape(-1),
        np.asarray(shapely.bounds(right), dtype=np.float64).reshape(-1),
    ))
    finite = bounds[np.isfinite(bounds)]
    return max(1.0, float(np.max(np.abs(finite), initial=0.0)))


def linework_result_equivalent(
    left: object,
    right: object | None,
    context: OracleContext,
    *,
    rtol: float = _LINEWORK_COORD_RTOL,
    atol: float = _LINEWORK_COORD_ATOL,
) -> None:
    """Compare clipped linework by structure, Hausdorff distance, and length."""
    import shapely

    if right is None:
        _raise('linework_result_equivalent requires a competitor result', context)
    left_rows = _as_sequence(left)
    right_rows = _as_sequence(right)
    if len(left_rows) != len(right_rows):
        _raise(
            'linework_result_equivalent count mismatch',
            context,
            details=f'left={len(left_rows)} right={len(right_rows)}',
        )
    for i, (lg, rg) in enumerate(zip(left_rows, right_rows, strict=True)):
        if _geom_missing(lg) or _geom_missing(rg):
            _raise(
                'linework_result_equivalent missingness mismatch',
                context,
                details=f'row={i}',
            )
        try:
            left_shape = _as_shapely(lg)
            right_shape = _as_shapely(rg)
        except Exception as exc:
            _raise(
                'linework_result_equivalent WKB conversion failed',
                context,
                details=f'row={i}: {exc}',
            )
        left_empty = bool(shapely.is_empty(left_shape))
        right_empty = bool(shapely.is_empty(right_shape))
        if left_empty or right_empty:
            if not left_empty or not right_empty:
                _raise(
                    'linework_result_equivalent empty mismatch',
                    context,
                    details=f'row={i}',
                )
            continue
        left_dimensions = _support_dimensions(left_shape)
        right_dimensions = _support_dimensions(right_shape)
        if left_dimensions != frozenset({'line'}) or (
            right_dimensions != frozenset({'line'})
        ):
            _raise(
                'linework_result_equivalent non-line support',
                context,
                details=(
                    f'row={i} left={_format_dimensions(left_dimensions)} '
                    f'right={_format_dimensions(right_dimensions)}'
                ),
            )
        if _linework_signature(left_shape) != _linework_signature(right_shape):
            _raise(
                'linework_result_equivalent component structure mismatch',
                context,
                details=f'row={i}',
            )
        left_length = float(shapely.length(left_shape))
        right_length = float(shapely.length(right_shape))
        if not np.isclose(
            left_length, right_length, rtol=rtol, atol=atol, equal_nan=False
        ):
            _raise(
                'linework_result_equivalent length mismatch',
                context,
                details=(
                    f'row={i} left={left_length:.6g} right={right_length:.6g} '
                    f'rtol={rtol} atol={atol}'
                ),
            )
        try:
            if bool(shapely.equals(left_shape, right_shape)):
                continue
            separation = max(
                float(shapely.hausdorff_distance(left_shape, right_shape)),
                float(shapely.hausdorff_distance(right_shape, left_shape)),
            )
        except Exception as exc:
            _raise(
                'linework_result_equivalent Shapely comparison failed',
                context,
                details=f'row={i}: {exc}',
            )
        budget = atol + rtol * _linework_scale(left_shape, right_shape)
        if np.isfinite(separation) and separation <= budget:
            continue
        _raise(
            'linework_result_equivalent Hausdorff mismatch',
            context,
            details=(
                f'row={i} separation={separation:.6g} budget={budget:.6g} '
                f'rtol={rtol} atol={atol}'
            ),
        )


def non_unique_points_contract(
    left: object,
    right: object | None,
    context: OracleContext,
    *,
    sources: Iterable[object] | None = None,
) -> None:
    """Same count; every result is a nonempty Point covered by its source.

    Do not compare coordinates. ``sources`` is the parallel source geometry
    sequence used for the coverage check; when omitted, only count and Point
    kind/non-empty are verified on both sides.
    """
    left_rows = _as_sequence(left)
    if right is not None:
        right_rows = _as_sequence(right)
        if len(left_rows) != len(right_rows):
            _raise(
                'non_unique_points_contract count mismatch',
                context,
                details=f'left={len(left_rows)} right={len(right_rows)}',
            )
    else:
        right_rows = left_rows

    source_list: Sequence[object] | None
    if sources is None:
        source_list = None
    else:
        source_list = list(sources)
        if len(source_list) != len(left_rows):
            _raise(
                'non_unique_points_contract sources length mismatch',
                context,
                details=f'sources={len(source_list)} results={len(left_rows)}',
            )

    def _is_nonempty_point(g: object) -> bool:
        if g is None:
            return False
        name = getattr(g, 'geom_type', None) or type(g).__name__
        if str(name).lower().replace(' ', '') != 'point':
            return False
        is_empty = getattr(g, 'is_empty', False)
        if callable(is_empty):
            is_empty = is_empty()
        return not bool(is_empty)

    def _covers(source: object, point: object) -> bool:
        try:
            import shapely

            # This is a contract oracle for gometry's representative point,
            # so evaluating it with gometry's own predicate would let the
            # producer and checker share one defect. Both values cross the
            # representation-only WKB boundary; Shapely owns containment.
            return bool(shapely.covers(_as_shapely(source), _as_shapely(point)))
        except Exception:
            return False

    for i, g in enumerate(left_rows):
        if not _is_nonempty_point(g):
            _raise(
                'non_unique_points_contract: gometry result not a nonempty Point',
                context,
                details=f'row={i} type={type(g).__name__}',
            )
        if source_list is not None and not _covers(source_list[i], g):
            _raise(
                'non_unique_points_contract: point not covered by source',
                context,
                details=f'row={i}',
            )
    if right is not None:
        for i, g in enumerate(right_rows):
            if not _is_nonempty_point(g):
                _raise(
                    'non_unique_points_contract: competitor result not a nonempty Point',
                    context,
                    details=f'row={i} type={type(g).__name__}',
                )
            if source_list is not None and not _covers(source_list[i], g):
                _raise(
                    'non_unique_points_contract: competitor point not covered by source',
                    context,
                    details=f'row={i}',
                )


def wrap_longitude_allclose(
    left: object,
    right: object | None,
    context: OracleContext,
    *,
    atol: float | None = None,
) -> None:
    """Destination-coordinate helper: wrap longitude to (-180, 180], then allclose."""
    if right is None:
        _raise('wrap_longitude_allclose requires a competitor result', context)
    a = np.asarray(left, dtype=np.float64)
    b = np.asarray(right, dtype=np.float64)
    if a.shape != b.shape:
        _raise(
            'wrap_longitude_allclose shape mismatch',
            context,
            details=f'{a.shape} vs {b.shape}',
        )
    use_atol = TOLERANCES['deg_destination'][1] if atol is None else atol

    def _wrap_lon(arr: np.ndarray) -> np.ndarray:
        out = arr.copy()
        if out.ndim >= 1 and out.shape[-1] >= 1:
            lon = out[..., 0]
            lon = ((lon + 180.0) % 360.0) - 180.0
            lon = np.where(lon == -180.0, 180.0, lon)
            out[..., 0] = lon
        return out

    aw = _wrap_lon(a)
    bw = _wrap_lon(b)
    if not np.allclose(aw, bw, rtol=0.0, atol=use_atol, equal_nan=True):
        _raise(
            'wrap_longitude_allclose failed',
            context,
            details=_error_stats(aw, bw),
        )
