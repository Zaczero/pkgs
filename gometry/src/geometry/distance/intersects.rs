use super::*;
/// Coordinate magnitude below which squared-space distance kernels cannot
/// overflow: gaps are at most `2 × 1e150`, whose square stays below
/// `f64::MAX`.
pub(crate) const SQUARED_SPACE_MAX_MAGNITUDE: f64 = 1e150;

/// Whether one probe coordinate keeps squared-space kernels finite.
pub(crate) fn coordinate_squared_safe(point: Point) -> bool {
    point.x.abs() <= SQUARED_SPACE_MAX_MAGNITUDE && point.y.abs() <= SQUARED_SPACE_MAX_MAGNITUDE
}

/// Whether a bounding box keeps squared-space distance kernels finite. Every
/// coordinate of the operand lies within these corners, so the corner check
/// gates the whole operand without a per-vertex scan — used to keep the
/// in-place puntal distance off extreme-coordinate inputs (where the squared
/// distance would overflow and only the hypot-space sweep stays exact).
pub(crate) fn bounds_squared_safe(bounds: Bounds) -> bool {
    bounds.minx().abs() <= SQUARED_SPACE_MAX_MAGNITUDE
        && bounds.maxx().abs() <= SQUARED_SPACE_MAX_MAGNITUDE
        && bounds.miny().abs() <= SQUARED_SPACE_MAX_MAGNITUDE
        && bounds.maxy().abs() <= SQUARED_SPACE_MAX_MAGNITUDE
}

/// Whether every vertex of `parts` keeps squared-space kernels finite —
/// computed (two column scans) on first distance use and cached.
pub(crate) fn squared_space_safe(parts: &DistanceParts) -> bool {
    *parts.squared_safe.get_or_init(|| {
        let safe_column = |column: &[f64]| {
            column
                .iter()
                .all(|v| v.abs() <= SQUARED_SPACE_MAX_MAGNITUDE)
        };
        let mut linework_safe = true;
        parts.linework.for_each_chain_xy_columns(|xs, ys| {
            linework_safe &= safe_column(xs) && safe_column(ys);
        });
        linework_safe
            && parts
                .point_only
                .iter()
                .all(|point| coordinate_squared_safe(*point))
    })
}

/// Boolean facade over the shared pre-parts area witness oracle. Needs only the
/// `Shape`s (raycast over rings), so callers can answer containment before
/// paying to build lazy linework; a negative answer still needs the boundary
/// crossing test.
pub(crate) fn area_overlap_probe(left: &Shape, right: &Shape) -> bool {
    quick_area_overlap(left, right).is_some()
}

/// The isolated-point and segment-crossing halves of the parts oracle —
/// for the distance/dwithin/nearest entry points whose representative
/// probes already ran inside `quick_area_overlap` (re-running them per
/// pair was measurable in batch metrics over overlapping operands).
pub(crate) fn parts_boundary_contact(
    left: &Shape,
    left_parts: &DistanceParts,
    right: &Shape,
    right_parts: &DistanceParts,
) -> bool {
    left_parts
        .point_only
        .iter()
        .any(|&point| parts_covers_point(right, right_parts, point))
        || right_parts
            .point_only
            .iter()
            .any(|&point| parts_covers_point(left, left_parts, point))
        || parts_segments_cross(left_parts, right_parts)
}
