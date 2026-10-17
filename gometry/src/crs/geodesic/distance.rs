use geographiclib_rs::Geodesic;

use crate::crs::RuntimeState;
use crate::crs::geodesic::{
    EllipsoidMetric, GeodesicCalculation, Result, finite, with_ellipsoid_metric_at,
    with_geodesic_at,
};
use crate::geographic::GeographicFrame;
use crate::geometry::{Coordinates, GeodesicMetric as _, Point, Shape};

/// Element-wise geodesic distances between packed lon/lat columns (array ×
/// array point pairs). Domain validation runs over both columns; the caller
/// must resolve the geodesic once and pass it in (no per-pair CRS/cache work).
pub(crate) fn geodesic_point_pair_distances(
    geodesic: &Geodesic,
    left_xs: &[f64],
    left_ys: &[f64],
    right_xs: &[f64],
    right_ys: &[f64],
    frame: GeographicFrame,
) -> Result<Vec<f64>> {
    crate::crs::ensure_geographic_columns_in_frame(frame, left_xs, left_ys)?;
    crate::crs::ensure_geographic_columns_in_frame(frame, right_xs, right_ys)?;
    let output_len = left_xs
        .len()
        .min(left_ys.len())
        .min(right_xs.len())
        .min(right_ys.len());
    let metric = EllipsoidMetric::for_geodesic_at_frame(geodesic, 0, frame);
    let mut values = Vec::with_capacity(output_len);
    for ((&lx, &ly), (&rx, &ry)) in std::iter::zip(
        std::iter::zip(left_xs, left_ys),
        std::iter::zip(right_xs, right_ys),
    ) {
        let value = finite(
            metric.segment_length(Point::new_raw_xy(lx, ly), Point::new_raw_xy(rx, ry)),
            GeodesicCalculation::Distance,
        )?;
        values.push(value);
    }
    Ok(values)
}

/// Direct distance for one geographic point pair. This is the point arm of
/// the prepared-pair path without constructing transient prepared plans.
pub(crate) fn geodesic_point_pair_distance(
    geodesic: &Geodesic,
    left: Point,
    right: Point,
    frame: GeographicFrame,
    runtime_generation: u64,
) -> Result<f64> {
    crate::crs::ensure_geographic_lonlat_in_frame(frame, left.x, left.y)?;
    crate::crs::ensure_geographic_lonlat_in_frame(frame, right.x, right.y)?;
    let metric = EllipsoidMetric::for_geodesic_at_frame(geodesic, runtime_generation, frame);
    finite(
        metric.segment_length(left, right),
        GeodesicCalculation::Distance,
    )
}

/// Element-wise geodesic `dwithin` over packed lon/lat columns (array × array
/// point pairs). Compares each pair's geodesic distance against `distance`
/// (meters); domain validation matches [`geodesic_point_pair_distances`].
pub(crate) fn geodesic_point_pair_dwithin(
    geodesic: &Geodesic,
    left_xs: &[f64],
    left_ys: &[f64],
    right_xs: &[f64],
    right_ys: &[f64],
    distance: f64,
    frame: GeographicFrame,
) -> Result<Vec<bool>> {
    crate::crs::ensure_geographic_columns_in_frame(frame, left_xs, left_ys)?;
    crate::crs::ensure_geographic_columns_in_frame(frame, right_xs, right_ys)?;
    let output_len = left_xs
        .len()
        .min(left_ys.len())
        .min(right_xs.len())
        .min(right_ys.len());
    if distance.is_infinite() && distance.is_sign_positive() {
        return Ok(vec![true; output_len]);
    }
    let metric = EllipsoidMetric::for_geodesic_at_frame(geodesic, 0, frame);
    let mut values = Vec::with_capacity(output_len);
    for ((&lx, &ly), (&rx, &ry)) in std::iter::zip(
        std::iter::zip(left_xs, left_ys),
        std::iter::zip(right_xs, right_ys),
    ) {
        values.push(
            metric.segment_length(Point::new_raw_xy(lx, ly), Point::new_raw_xy(rx, ry)) <= distance,
        );
    }
    Ok(values)
}

/// Direct `dwithin` for one geographic point pair, matching the prepared
/// point-pair comparison at the threshold.
pub(crate) fn geodesic_point_pair_dwithin_one(
    geodesic: &Geodesic,
    left: Point,
    right: Point,
    distance: f64,
    frame: GeographicFrame,
    runtime_generation: u64,
) -> Result<bool> {
    crate::crs::ensure_geographic_lonlat_in_frame(frame, left.x, left.y)?;
    crate::crs::ensure_geographic_lonlat_in_frame(frame, right.x, right.y)?;
    let metric = EllipsoidMetric::for_geodesic_at_frame(geodesic, runtime_generation, frame);
    if distance.is_infinite() && distance.is_sign_positive() {
        return Ok(true);
    }
    Ok(metric.segment_length(left, right) <= distance)
}

pub(crate) fn geodesic_point_distances(
    crs: &str,
    xs: &[f64],
    ys: &[f64],
    target: Point,
    frame: GeographicFrame,
    state: &RuntimeState,
) -> Result<Vec<f64>> {
    crate::crs::ensure_geographic_lonlat_in_frame(frame, target.x, target.y)?;
    crate::crs::ensure_geographic_columns_in_frame(frame, xs, ys)?;
    let output_len = xs.len().min(ys.len());
    with_geodesic_at(crs, state, |geodesic| {
        let metric = EllipsoidMetric::for_geodesic_at_frame(geodesic, state.generation, frame);
        let mut values = Vec::with_capacity(output_len);
        for (&x, &y) in std::iter::zip(xs, ys) {
            let value = finite(
                metric.segment_length(Point::new_raw_xy(x, y), target),
                GeodesicCalculation::Distance,
            )?;
            values.push(value);
        }
        Ok(values)
    })
}

/// Geographic Hausdorff distance (meters) between chart-edge supports.
pub(crate) fn geodesic_hausdorff_at(
    crs: &str,
    a: &Shape,
    b: &Shape,
    state: &RuntimeState,
) -> Result<f64> {
    with_ellipsoid_metric_at(crs, &[a, b], state, |metric| {
        finite(
            a.chart_hausdorff_distance(b, metric)?,
            GeodesicCalculation::HausdorffDistance,
        )
    })
}

/// Discrete geodesic Fréchet distance (meters) on `crs`'s ellipsoid.
///
/// The geodesic sibling of `Shape::frechet_distance`. The coupling DP mirrors
/// the planar discrete-Fréchet kernel exactly but couples on true Karney
/// distances, with band pruning around a greedy coupling upper bound.
pub(crate) fn geodesic_frechet_at_with_scratch(
    crs: &str,
    a: &Shape,
    b: &Shape,
    state: &RuntimeState,
    scratch: &mut crate::geometry::FrechetDpScratch,
) -> Result<f64> {
    // Public metric dispatch handles empty shapes before selecting this arm,
    // so the geodesic and planar public lanes share the infinity sentinel.
    // Direct callers here still pass through `single_linework`'s checks.
    let left = a.single_linework()?;
    let right = b.single_linework()?;
    with_ellipsoid_metric_at(crs, &[a, b], state, |metric| {
        let distance = if left.coord_count() <= right.coord_count() {
            geodesic_frechet_dp_with_scratch(metric, left, right, scratch)
        } else {
            geodesic_frechet_dp_with_scratch(metric, right, left, scratch)
        };
        finite(distance, GeodesicCalculation::FrechetDistance)
    })
}

/// Greedy diagonal-walk upper bound on the discrete geodesic Fréchet
/// bottleneck. `O(L + W)`, tight when the curves run close — exactly when the
/// band pays off.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the DP calls this only for nonempty admitted rows and each index advances only while below its row length"
)]
pub(crate) fn geodesic_frechet_greedy_ub<S: Coordinates + ?Sized, L: Coordinates + ?Sized>(
    metric: &EllipsoidMetric<'_>,
    short: &S,
    long: &L,
) -> f64 {
    let (width, length) = (short.coord_count(), long.coord_count());
    let distance =
        |i: usize, j: usize| metric.segment_length(long.nth_coord(i), short.nth_coord(j));
    let (mut i, mut j) = (0_usize, 0_usize);
    let mut bottleneck = distance(0, 0);
    while i + 1 < length || j + 1 < width {
        let (down, right) = (i + 1 < length, j + 1 < width);
        (i, j) = if down && right {
            let (diag, dn, dr) = (
                distance(i + 1, j + 1),
                distance(i + 1, j),
                distance(i, j + 1),
            );
            if diag <= dn && diag <= dr {
                (i + 1, j + 1)
            } else if dn <= dr {
                (i + 1, j)
            } else {
                (i, j + 1)
            }
        } else if down {
            (i + 1, j)
        } else {
            (i, j + 1)
        };
        bottleneck = bottleneck.max(distance(i, j));
    }
    bottleneck
}

/// The discrete-Fréchet coupling DP with `short` as the rolling-row dimension,
/// over geodesic edge distances (no squaring — the geodesic metric is already
/// the distance). Mirrors [`frechet_dp_columns`] in the geometry layer: band
/// pruning around [`geodesic_frechet_greedy_ub`], within the backend's
/// evaluation allowance of the full table.
#[cfg(test)]
pub(crate) fn geodesic_frechet_dp<S: Coordinates + ?Sized, L: Coordinates + ?Sized>(
    metric: &EllipsoidMetric<'_>,
    short: &S,
    long: &L,
) -> f64 {
    let mut scratch = crate::geometry::FrechetDpScratch::default();
    geodesic_frechet_dp_with_scratch(metric, short, long, &mut scratch)
}

#[expect(
    clippy::indexing_slicing,
    reason = "rolling rows are sized to width and each cell access is bounded by the live Fréchet predecessor band"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "width is nonzero after the early return and recurrence indices remain within the admitted rows"
)]
pub(crate) fn geodesic_frechet_dp_with_scratch<S: Coordinates + ?Sized, L: Coordinates + ?Sized>(
    metric: &EllipsoidMetric<'_>,
    short: &S,
    long: &L,
    scratch: &mut crate::geometry::FrechetDpScratch,
) -> f64 {
    const INF: f64 = f64::INFINITY;
    let width = short.coord_count();
    if width == 0 || long.coord_count() == 0 {
        return INF;
    }
    let distance = |p: Point, q: Point| metric.segment_length(p, q);
    // Greedy and DP evaluate the same pairs independently. Transcendental
    // roundoff must not prune the coupling that supplied the upper bound.
    let allowance = metric.geodesic_numerical_policy().evaluation_allowance(2.0);
    let ub = (geodesic_frechet_greedy_ub(metric, short, long) + allowance).next_up();
    let (previous, current) = scratch.geodesic_buffers(width);
    previous[..width].fill(INF);
    current[..width].fill(INF);

    let first_long = long.nth_coord(0);
    let mut running = 0.0_f64;
    let (mut prev_lo, mut prev_hi) = (0_usize, 0_usize);
    for (index, short_point) in short.iter_coords().enumerate() {
        running = running.max(distance(first_long, short_point));
        if running <= ub {
            previous[index] = running;
            prev_hi = index;
        } else {
            break;
        }
    }

    for long_point in long.iter_coords().skip(1) {
        let lo = prev_lo;
        let (mut cur_lo, mut cur_hi) = (usize::MAX, 0_usize);
        let mut left = INF;
        for index in lo..width {
            let prev_j = if index <= prev_hi {
                previous[index]
            } else {
                INF
            };
            let prev_j1 = if index > lo && index - 1 <= prev_hi {
                previous[index - 1]
            } else {
                INF
            };
            let reach = prev_j.min(prev_j1).min(left);
            let value = if reach.is_finite() {
                distance(long_point, short.nth_coord(index)).max(reach)
            } else {
                INF
            };
            if value <= ub {
                current[index] = value;
                if cur_lo == usize::MAX {
                    cur_lo = index;
                }
                cur_hi = index;
                left = value;
            } else {
                current[index] = INF;
                left = INF;
                if index > prev_hi {
                    break;
                }
            }
        }
        debug_assert_ne!(
            cur_lo,
            usize::MAX,
            "the optimal coupling keeps each row live"
        );
        std::mem::swap(previous, current);
        (prev_lo, prev_hi) = (cur_lo, cur_hi);
    }
    debug_assert_eq!(
        prev_hi,
        width - 1,
        "the coupling reaches its final endpoint"
    );
    previous[width - 1]
}
