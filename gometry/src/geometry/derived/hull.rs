use std::cmp::Ordering;

use crate::error::Result;
use crate::geometry::{
    CoordSeq, LineSeq, Orientation, Point, Polygon, Ring, Shape, XY, axis_pow2_scale, orientation,
    same_topological_coordinate, scaled_residual,
};
/// Reduce an open monotone-chain hull (0–2 vertices degenerate to
/// point/line; 3+ close into a polygon shell).
pub(crate) fn shape_from_open_hull(
    hull: &[Point],
    empty: impl FnOnce() -> Shape,
) -> crate::error::Result<Shape> {
    match hull {
        [] => Ok(empty()),
        [point] => Ok(Shape::Point(*point)),
        [first, second] => Ok(Shape::LineString(LineSeq::segment(*first, *second)?)),
        _ => {
            let mut ring = hull.to_vec();
            ring.push(ring[0]);
            Ok(Shape::Polygon(Polygon::new(
                Ring::try_from_closed(CoordSeq::try_from_points(&ring)?)?,
                Vec::new(),
            )))
        },
    }
}

/// Assemble a hull whose vertices are selected copies of source points.
/// Selected axes must be homogeneous; unselected source vertices are ignored.
pub(crate) fn shape_from_selected_hull(
    hull: &[Point],
    empty: impl FnOnce() -> Shape,
) -> Result<Shape> {
    match hull {
        [] => Ok(empty()),
        [point] => Ok(Shape::Point(*point)),
        [_, _] => Ok(Shape::LineString(LineSeq::from_proven(
            CoordSeq::try_from_points(hull)?,
        ))),
        _ => {
            let mut ring = hull.to_vec();
            let first = ring[0];
            ring.push(first);
            Ok(Shape::Polygon(Polygon::new(
                Ring::try_from_closed(CoordSeq::try_from_points(&ring)?)?,
                Vec::new(),
            )))
        },
    }
}

/// Lexicographic XY order that treats ±0.0 as the same ordinate. Stored
/// spellings are not rewritten; only the comparison collapses them.
pub(crate) fn geometric_xy_cmp(left: impl Into<XY>, right: impl Into<XY>) -> Ordering {
    let left = left.into();
    let right = right.into();
    geometric_ordinate_cmp(left.x, right.x).then(geometric_ordinate_cmp(left.y, right.y))
}

fn geometric_ordinate_cmp(left: f64, right: f64) -> Ordering {
    if same_topological_coordinate(left, right) {
        Ordering::Equal
    } else {
        left.total_cmp(&right)
    }
}

/// Andrew's monotone chain over the input vertices: lexicographic XY sort,
/// exact XY dedup, then lower+upper chains with ROBUST orientation turns
/// (collinear vertices pop). Returns open CCW hull indices into `points`;
/// 1-2 distinct points pass through for the degenerate reductions.
pub(crate) fn monotone_chain_hull(points: &[Point]) -> Vec<usize> {
    let mut ids: Vec<usize> = (0..points.len()).collect();
    akl_toussaint_filter(points, &mut ids);
    ids.sort_unstable_by(|&left, &right| geometric_xy_cmp(points[left], points[right]));
    ids.dedup_by(|right, left| {
        same_topological_coordinate(points[*left].x, points[*right].x)
            && same_topological_coordinate(points[*left].y, points[*right].y)
    });
    if ids.len() <= 2 {
        return ids;
    }
    let mut hull: Vec<usize> = Vec::with_capacity(64);
    let keeps_turn = |hull: &[usize], point: usize| {
        orientation(
            points[hull[hull.len() - 2]],
            points[hull[hull.len() - 1]],
            points[point],
        ) == Orientation::CounterClockwise
    };
    for &point in &ids {
        while hull.len() >= 2 && !keeps_turn(&hull, point) {
            hull.pop();
        }
        hull.push(point);
    }
    let lower = hull.len() + 1;
    for &point in ids.iter().rev().skip(1) {
        while hull.len() >= lower && !keeps_turn(&hull, point) {
            hull.pop();
        }
        hull.push(point);
    }
    hull.pop();
    hull
}

/// Akl–Toussaint pre-filter: points strictly inside the convex quad of the
/// four axis extremes can never be hull vertices — discard them before the
/// sort (the hull cost at scale). The inside test is plain-float crosses
/// with a conservative margin that over-covers the floating error, so a
/// borderline point is always KEPT for the exact chain; only clearly
/// interior points drop.
fn akl_toussaint_filter(points: &[Point], ids: &mut Vec<usize>) {
    if ids.len() < 256 {
        return;
    }
    let mut quad = [points[ids[0]].xy(); 4]; // min-x, min-y, max-x, max-y extremes
    let mut extent = 0.0_f64;
    for &id in ids.as_slice() {
        let point = points[id].xy();
        if point.x < quad[0].x {
            quad[0] = point;
        }
        if point.y < quad[1].y {
            quad[1] = point;
        }
        if point.x > quad[2].x {
            quad[2] = point;
        }
        if point.y > quad[3].y {
            quad[3] = point;
        }
        extent = extent.max(point.x.abs()).max(point.y.abs());
    }
    // One power-of-two frame keeps the filter arithmetic finite even when
    // stored coordinates are near f64::MAX. A non-finite intermediate is not
    // a negative certificate: fail open into the robust monotone chain.
    if !extent.is_finite() {
        return;
    }
    let origin = XY::new(
        f64::midpoint(quad[0].x, quad[2].x),
        f64::midpoint(quad[1].y, quad[3].y),
    );
    let scale = axis_pow2_scale(extent);
    let normalize = |point: XY| {
        XY::new(
            scaled_residual(point.x, origin.x, scale),
            scaled_residual(point.y, origin.y, scale),
        )
    };
    quad = quad.map(normalize);
    let margin = 1e-10;
    ids.retain(|&id| {
        let point = normalize(points[id].xy());
        for edge in 0..4 {
            let a = quad[edge];
            let b = quad[(edge + 1) % 4];
            let cross = (b.x - a.x) * (point.y - a.y) - (b.y - a.y) * (point.x - a.x);
            if !cross.is_finite() || cross <= margin {
                return true; // on, outside, or too close to call: keep
            }
        }
        false // clearly interior on every edge
    });
}
