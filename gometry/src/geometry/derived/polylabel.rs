use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::num::NonZeroU32;

use crate::error::Result;
#[cfg(test)]
use crate::geometry::tessellation::exact::compare_ratios;
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational as Rational};
use crate::geometry::{
    AxisFrame, BVH_MIN_INDEXED_SEGMENTS, Bounds, CoordSeq, CoordSeqBuilder, Coordinates as _,
    ExpansionBudget, FacetBvh, Orientation, Point, Polygon, PreparedLinework, PreparedSupport,
    Ring, Segment, Shape, orientation, predicates,
};

mod incidence;
mod inscribed;
mod search;

pub(super) use self::incidence::exact_difference;
use self::incidence::{
    candidate_center_is_strictly_inside, framed_metric_segment_distance, has_interior,
    label_carrier_enclosure, representative_point,
};
#[cfg(test)]
pub(in crate::geometry::derived::polylabel) use self::incidence::{
    exact_boundary_radius, exact_boundary_squared, segment_enters_candidate,
    source_boundary_enters_candidate, source_hole_is_enclosed, source_vertex_outside_candidate,
};
#[cfg(test)]
pub(crate) use self::inscribed::build_inscribed_circle;
#[cfg(test)]
pub(in crate::geometry::derived::polylabel) use self::inscribed::build_inscribed_circle_once;
pub(crate) use self::inscribed::{build_inscribed_circle_budgeted, emit_zero_radius_point};
use self::search::polylabel_search;
pub(super) use super::minimum_circle::next_realization_radius;

pub(crate) fn polylabel_point(shape: &Shape, tolerance: Option<f64>) -> Result<Point> {
    let Some(bounds) = shape.bounds() else {
        return Ok(Point::new_raw_xy(0.0, 0.0));
    };
    if !has_interior(shape) {
        return Ok(degenerate_polygon_center(bounds));
    }
    let tolerance = tolerance.unwrap_or_else(|| scale_aware_default_tolerance(shape));
    let center = degenerate_polygon_center(bounds);
    let hx = bounds.maxx() / 2.0 - bounds.minx() / 2.0;
    let hy = bounds.maxy() / 2.0 - bounds.miny() / 2.0;
    // Ordinary coordinates use the existing squared SIMD distance kernel
    // directly. Only arithmetic outside its range needs a local-frame copy.
    let magnitude = bounds
        .minx()
        .abs()
        .max(bounds.maxx().abs())
        .max(bounds.miny().abs())
        .max(bounds.maxy().abs());
    if (magnitude * magnitude).is_finite() && hx * hx > 0.0 && hy * hy > 0.0 {
        return polylabel_search(shape, bounds, None, tolerance);
    }
    let Some(frame) = AxisFrame::from_origin_extents(
        center,
        bounds.minx().abs().max(bounds.maxx().abs()).max(hx.abs()),
        bounds.miny().abs().max(bounds.maxy().abs()).max(hy.abs()),
    ) else {
        return Ok(center);
    };
    let normalized = shape.try_map_coordseqs(
        |seq| {
            let mut builder = CoordSeqBuilder::with_capacity(seq.axes(), seq.len());
            for point in seq.iter_coords() {
                let xy = frame.frame_xy(point.x, point.y);
                builder.push(point.with_xy(xy.x, xy.y)?);
            }
            builder.finish()
        },
        |point| {
            let xy = frame.frame_xy(point.x, point.y);
            point.with_xy(xy.x, xy.y)
        },
    )?;
    let normalized_bounds = normalized.bounds().expect("nonempty framed polygon");
    let point = polylabel_search(&normalized, normalized_bounds, Some(frame), tolerance)?;
    let world = frame.unframe_xy(point.xy());
    Ok(Point::new_raw_xy(world.x, world.y))
}

/// The omitted search tolerance used by both polylabel and the certified
/// maximum-inscribed-circle owner. The frame is only used to form stable
/// extent differences; the returned value is in the caller's Euclidean units.
pub(crate) fn scale_aware_default_tolerance(shape: &Shape) -> f64 {
    let Some(bounds) = shape.bounds() else {
        return f64::from_bits(1);
    };
    let midpoint = degenerate_polygon_center(bounds);
    let half_width = bounds.maxx() / 2.0 - bounds.minx() / 2.0;
    let half_height = bounds.maxy() / 2.0 - bounds.miny() / 2.0;
    let Some(frame) = AxisFrame::from_origin_extents(
        midpoint,
        half_width
            .abs()
            .max(bounds.minx().abs())
            .max(bounds.maxx().abs()),
        half_height
            .abs()
            .max(bounds.miny().abs())
            .max(bounds.maxy().abs()),
    ) else {
        return f64::from_bits(1);
    };
    let min = frame.frame_xy(bounds.minx(), bounds.miny());
    let max = frame.frame_xy(bounds.maxx(), bounds.maxy());
    (((max.x - min.x).abs() / frame.scale_x()).min((max.y - min.y).abs() / frame.scale_y()) / 500.0)
        .max(f64::from_bits(1))
}

pub(crate) const fn degenerate_polygon_center(bounds: Bounds) -> Point {
    Point::new_raw_xy(
        f64::midpoint(bounds.minx(), bounds.maxx()),
        f64::midpoint(bounds.miny(), bounds.maxy()),
    )
}

#[cfg(test)]
mod tests;
