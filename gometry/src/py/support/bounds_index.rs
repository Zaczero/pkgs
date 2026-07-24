use super::*;

pub(crate) fn bounds_envelope(bounds: Bounds) -> AABB<[f64; 2]> {
    // Bounds are ordered by construction — skip from_corners' min/max pass.
    AABB::from_bounds([bounds.minx(), bounds.miny()], [
        bounds.maxx(),
        bounds.maxy(),
    ])
}

pub(crate) fn global_geographic_candidate_envelope() -> AABB<[f64; 2]> {
    AABB::from_bounds([-f64::MAX, -f64::MAX], [f64::MAX, f64::MAX])
}

/// The R-tree index/query envelope for an antimeridian-crossing geographic
/// geometry, whose true extent a single planar box cannot represent. The planar
/// min/max is the spurious *false-middle* box that excludes the real region, so
/// widen longitude to the full band; and a pole-enclosing ring's vertices stop
/// short of the pole it actually contains, so extend latitude to ±90 for any
/// pole it encloses. Conservative (never misses a candidate); the gated refine
/// restores exactness.
pub(crate) fn crossing_index_bounds(shape: &Shape, bounds: Bounds) -> Bounds {
    // A planar polygon box only reads its shell, but a polar hole can be the
    // far latitude edge of the represented annulus.  Index envelopes are a
    // conservative trust boundary, so include every ring vertex before the
    // exact predicate refines candidates.
    let mut miny = bounds.miny();
    let mut maxy = bounds.maxy();
    shape.for_each_point(|point| {
        miny = miny.min(point.y);
        maxy = maxy.max(point.y);
    });
    Bounds::new_unchecked(
        -180.0,
        if crate::geometry::shape_encloses_pole(shape, false) {
            -90.0
        } else {
            miny
        },
        180.0,
        if crate::geometry::shape_encloses_pole(shape, true) {
            90.0
        } else {
            maxy
        },
    )
}

/// [`crossing_index_bounds`] as an `AABB` for the R-tree.
pub(crate) fn crossing_index_envelope(shape: &Shape, bounds: Bounds) -> AABB<[f64; 2]> {
    bounds_envelope(crossing_index_bounds(shape, bounds))
}

pub(crate) fn point_from_bounds(bounds: Bounds) -> Option<Point> {
    (bounds.minx().total_cmp(&bounds.maxx()).is_eq()
        && bounds.miny().total_cmp(&bounds.maxy()).is_eq())
    .then_some(Point::new_unchecked_xy(bounds.minx(), bounds.miny()))
}
