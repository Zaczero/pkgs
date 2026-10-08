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
        [first, ..] => {
            let mut ring = hull.to_vec();
            ring.push(*first);
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
        [first, ..] => {
            let mut ring = hull.to_vec();
            ring.push(*first);
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
    geometric_ordinate_cmp(left.x, right.x).then_with(|| geometric_ordinate_cmp(left.y, right.y))
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
#[expect(
    clippy::indexing_slicing,
    reason = "hull and sort keys are source ordinals constructed from this points slice"
)]
pub(crate) fn monotone_chain_hull(points: &[Point]) -> Vec<usize> {
    let mut ids = akl_toussaint_filter(points);
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
        let Some((&last, prefix)) = hull.split_last() else {
            return false;
        };
        let Some((&previous, _)) = prefix.split_last() else {
            return false;
        };
        orientation(points[previous], points[last], points[point]) == Orientation::CounterClockwise
    };
    for &point in &ids {
        while hull.len() >= 2 && !keeps_turn(&hull, point) {
            hull.pop();
        }
        hull.push(point);
    }
    let lower_len = hull.len();
    for &point in ids.iter().rev().skip(1) {
        while hull.len() > lower_len && !keeps_turn(&hull, point) {
            hull.pop();
        }
        hull.push(point);
    }
    hull.pop();
    hull
}

/// Monotone chain over point sites in one retained periodic chart.
///
/// Lexicographic X comparison uses the exact native window/lift pair and
/// turn decisions evaluate those same retained expressions. Rounded chart X
/// is never used to order or merge sites.
#[expect(
    clippy::indexing_slicing,
    reason = "monotone-chain indices are constructed from this admitted site slice"
)]
pub(crate) fn monotone_chain_chart_hull(
    sites: &[crate::geographic::ChartPointSite],
    frame: crate::geographic::GeographicFrame,
) -> Result<Vec<usize>> {
    let mut indices = (0..sites.len()).collect::<Vec<_>>();
    indices.sort_unstable_by(|&left, &right| sites[left].compare_chart_xy(sites[right]));
    if indices.len() <= 2 {
        return Ok(indices);
    }

    let mut hull: Vec<usize> = Vec::with_capacity(indices.len());
    for &site in &indices {
        while hull.len() >= 2 {
            let previous = *hull.get(hull.len() - 2).expect("two-site suffix");
            let current = *hull.last().expect("nonempty hull");
            let turn =
                crate::geometry::exact_arith::retained_site_orientation(frame.longitude(), [
                    (sites[previous].longitude(), sites[previous].source_y()),
                    (sites[current].longitude(), sites[current].source_y()),
                    (sites[site].longitude(), sites[site].source_y()),
                ])?;
            if turn == crate::geometry::exact_arith::ExactSign::Positive {
                break;
            }
            hull.pop();
        }
        hull.push(site);
    }
    let lower_len = hull.len();
    for &site in indices.iter().rev().skip(1) {
        while hull.len() > lower_len {
            let previous = *hull.get(hull.len() - 2).expect("two-site suffix");
            let current = *hull.last().expect("nonempty hull");
            let turn =
                crate::geometry::exact_arith::retained_site_orientation(frame.longitude(), [
                    (sites[previous].longitude(), sites[previous].source_y()),
                    (sites[current].longitude(), sites[current].source_y()),
                    (sites[site].longitude(), sites[site].source_y()),
                ])?;
            if turn == crate::geometry::exact_arith::ExactSign::Positive {
                break;
            }
            hull.pop();
        }
        hull.push(site);
    }
    hull.pop();
    Ok(hull)
}

/// Akl–Toussaint pre-filter: points strictly inside the convex quad of the
/// four axis extremes can never be hull vertices — discard them before the
/// sort (the hull cost at scale). The inside test is plain-float crosses
/// with a conservative margin that over-covers the floating error, so a
/// borderline point is always KEPT for the exact chain; only clearly
/// interior points drop.
fn akl_toussaint_filter(points: &[Point]) -> Vec<usize> {
    let mut ids = Vec::with_capacity(points.len());
    if points.len() < 256 {
        ids.extend(0..points.len());
        return ids;
    }
    let first = *points.first().expect("large point input is non-empty");
    let (mut west, mut south, mut east, mut north) =
        (first.xy(), first.xy(), first.xy(), first.xy());
    let mut extent = 0.0_f64;
    for point in points {
        let point = point.xy();
        if point.x < west.x {
            west = point;
        }
        if point.y < south.y {
            south = point;
        }
        if point.x > east.x {
            east = point;
        }
        if point.y > north.y {
            north = point;
        }
        extent = extent.max(point.x.abs()).max(point.y.abs());
    }
    // One power-of-two frame keeps the filter arithmetic finite even when
    // stored coordinates are near f64::MAX. A non-finite intermediate is not
    // a negative certificate: fail open into the robust monotone chain.
    if !extent.is_finite() {
        ids.extend(0..points.len());
        return ids;
    }
    let origin = XY::new(
        f64::midpoint(west.x, east.x),
        f64::midpoint(south.y, north.y),
    );
    let scale = axis_pow2_scale(extent);
    let normalize = |point: XY| {
        XY::new(
            scaled_residual(point.x, origin.x, scale),
            scaled_residual(point.y, origin.y, scale),
        )
    };
    let [west, south, east, north] = [west, south, east, north].map(normalize);
    let edges = [(west, south), (south, east), (east, north), (north, west)];
    let margin = 1e-10;
    for (id, point) in points.iter().enumerate() {
        let point = normalize(point.xy());
        let mut keep = false;
        for (a, b) in edges {
            let cross = (b.x - a.x) * (point.y - a.y) - (b.y - a.y) * (point.x - a.x);
            if !cross.is_finite() || cross <= margin {
                keep = true;
                break;
            }
        }
        if keep {
            ids.push(id);
        }
    }
    ids
}
