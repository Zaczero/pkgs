use crate::boundary::geographic::normalize_accepted_latitude;
use crate::py::support::{AABB, Bounds, Point, Shape};

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

/// Conservative envelope for any geometry with a coordinate at a physical
/// pole.  Its longitude is undefined after geographic admission, so retaining
/// a raw coordinate longitude would let the planar R-tree reject an exact
/// geographic candidate.  Normalize the accepted exterior ULP before deriving
/// latitude extents as well: a point at ``90.next_up()`` is the north pole,
/// not a point above every exact-pole entry.
fn pole_reaching_index_bounds(shape: &Shape) -> Bounds {
    let mut miny = f64::INFINITY;
    let mut maxy = f64::NEG_INFINITY;
    shape.for_each_point(|point| {
        let latitude = normalize_accepted_latitude(point.y);
        miny = miny.min(latitude);
        maxy = maxy.max(latitude);
    });
    Bounds::new_unchecked(-180.0, miny, 180.0, maxy)
}

/// One conservative envelope policy for every geographic index entry and
/// query. A physical pole has no unique longitude, so it needs the same
/// full-longitude treatment as an antimeridian-crossing shape: a planar point
/// envelope could otherwise make the R-tree establish a false negative before
/// the exact geographic predicate has a chance to run.
pub(crate) fn index_bounds(shape: &Shape, bounds: Bounds, geographic: bool) -> Bounds {
    if !geographic {
        return bounds;
    }
    if crate::geometry::shape_reaches_geographic_pole(shape) {
        return pole_reaching_index_bounds(shape);
    }
    if shape.crosses_antimeridian() {
        return crossing_index_bounds(shape, bounds);
    }
    bounds
}

/// [`index_bounds`] as an R-tree envelope.
pub(crate) fn index_envelope(shape: &Shape, bounds: Bounds, geographic: bool) -> AABB<[f64; 2]> {
    bounds_envelope(index_bounds(shape, bounds, geographic))
}

/// The point-specialized form avoids building a transient [`Shape`] in packed
/// point index/query lanes.
pub(crate) fn point_index_envelope(point: Point, geographic: bool) -> AABB<[f64; 2]> {
    if geographic && crate::geometry::point_is_geographic_pole(point).is_some() {
        let latitude = normalize_accepted_latitude(point.y);
        return bounds_envelope(Bounds::new_unchecked(-180.0, latitude, 180.0, latitude));
    }
    AABB::from_point([point.x, point.y])
}

pub(crate) fn point_from_bounds(bounds: Bounds) -> Option<Point> {
    (bounds.minx().total_cmp(&bounds.maxx()).is_eq()
        && bounds.miny().total_cmp(&bounds.maxy()).is_eq())
    .then_some(Point::new_unchecked_xy(bounds.minx(), bounds.miny()))
}
