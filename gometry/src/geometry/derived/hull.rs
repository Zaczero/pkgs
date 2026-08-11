use crate::geometry::{
    CoordSeq, LineSeq, Orientation, Point, Polygon, Ring, Shape, XY, axis_pow2_scale, orientation,
    same_topological_coordinate, scaled_residual,
};
/// Reduce an open monotone-chain hull (0–2 vertices degenerate to
/// point/line; 3+ close into a polygon shell).
pub(crate) fn shape_from_open_hull(hull: &[Point], empty: impl FnOnce() -> Shape) -> Shape {
    match hull {
        [] => empty(),
        [point] => Shape::Point(*point),
        [first, second] => Shape::LineString(
            LineSeq::try_new(CoordSeq::from(vec![*first, *second]))
                .expect("two hull vertices form a line"),
        ),
        _ => {
            let mut ring = hull.to_vec();
            ring.push(ring[0]);
            Shape::Polygon(Polygon::new(Ring::from_trusted_closed(ring), Vec::new()))
        },
    }
}

/// Andrew's monotone chain over the input vertices: lexicographic sort,
/// exact dedup, then lower+upper chains with ROBUST orientation turns
/// (collinear vertices pop — the strict-hull convention GEOS and geo
/// share). Output is the open CCW hull ring; 1-2 distinct points pass
/// through for the degenerate reductions.
pub(crate) fn monotone_chain_hull(points: &[Point]) -> Vec<Point> {
    // Sort 16-byte XY pairs, not 40-byte Points — the sort IS the cost at
    // scale; hull vertices map back to full Points at the end (Z/M are
    // re-carried by the caller anyway).
    let mut xy: Vec<XY> = points
        .iter()
        .map(|point| XY::new(point.x, point.y))
        .collect();
    akl_toussaint_filter(&mut xy);
    xy.sort_unstable_by(|a, b| a.x.total_cmp(&b.x).then(a.y.total_cmp(&b.y)));
    xy.dedup_by(|a, b| {
        same_topological_coordinate(a.x, b.x) && same_topological_coordinate(a.y, b.y)
    });
    if xy.len() <= 2 {
        return xy
            .into_iter()
            .map(|point| Point::new_unchecked_xy(point.x, point.y))
            .collect();
    }
    let mut hull: Vec<XY> = Vec::with_capacity(64);
    let keeps_turn = |hull: &[XY], p: XY| {
        orientation(hull[hull.len() - 2], hull[hull.len() - 1], p) == Orientation::CounterClockwise
    };
    for &point in &xy {
        while hull.len() >= 2 && !keeps_turn(&hull, point) {
            hull.pop();
        }
        hull.push(point);
    }
    let lower = hull.len() + 1;
    for &point in xy.iter().rev().skip(1) {
        while hull.len() >= lower && !keeps_turn(&hull, point) {
            hull.pop();
        }
        hull.push(point);
    }
    hull.pop();
    hull.into_iter()
        .map(|point| Point::new_unchecked_xy(point.x, point.y))
        .collect()
}

/// Akl–Toussaint pre-filter: points strictly inside the convex quad of the
/// four axis extremes can never be hull vertices — discard them before the
/// sort (the hull cost at scale). The inside test is plain-float crosses
/// with a conservative margin that over-covers the floating error, so a
/// borderline point is always KEPT for the exact chain; only clearly
/// interior points drop.
fn akl_toussaint_filter(points: &mut Vec<XY>) {
    if points.len() < 256 {
        return;
    }
    let mut quad = [points[0]; 4]; // min-x, min-y, max-x, max-y extremes
    let mut extent = 0.0_f64;
    for &point in points.iter() {
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
    points.retain(|&point| {
        let point = normalize(point);
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
