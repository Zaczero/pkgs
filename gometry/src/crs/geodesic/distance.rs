#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use geographiclib_rs::Geodesic;

use super::*;
use crate::geometry::{Coordinates, GeodesicMetric, Point, Shape};

/// Batch geodesic distances from packed lon/lat columns to one fixed
/// point: CRS/domain validation and the geodesic cache resolve happen
/// ONCE, then every pair runs the same point kernel the shape path uses
/// (bit-identical to the per-row lane, minus its per-pair cache lookups).
/// Element-wise geodesic distances between packed lon/lat columns (array ×
/// array point pairs). Domain validation runs over both columns; the caller
/// must resolve the geodesic once and pass it in (no per-pair CRS/cache work).
pub(crate) fn geodesic_point_pair_distances(
    geodesic: &Geodesic,
    left_xs: &[f64],
    left_ys: &[f64],
    right_xs: &[f64],
    right_ys: &[f64],
) -> Result<Vec<f64>> {
    for (&x, &y) in std::iter::zip(left_xs, left_ys) {
        ensure_geographic_lonlat(x, y)?;
    }
    for (&x, &y) in std::iter::zip(right_xs, right_ys) {
        ensure_geographic_lonlat(x, y)?;
    }
    let metric = EllipsoidMetric::for_geodesic(geodesic);
    std::iter::zip(
        std::iter::zip(left_xs, left_ys),
        std::iter::zip(right_xs, right_ys),
    )
    .map(|((&lx, &ly), (&rx, &ry))| {
        finite(
            metric.segment_length(
                Point::new_unchecked_xy(lx, ly),
                Point::new_unchecked_xy(rx, ry),
            ),
            "geodesic distance",
        )
    })
    .collect()
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
) -> Result<Vec<bool>> {
    for (&x, &y) in std::iter::zip(left_xs, left_ys) {
        ensure_geographic_lonlat(x, y)?;
    }
    for (&x, &y) in std::iter::zip(right_xs, right_ys) {
        ensure_geographic_lonlat(x, y)?;
    }
    let metric = EllipsoidMetric::for_geodesic(geodesic);
    Ok(std::iter::zip(
        std::iter::zip(left_xs, left_ys),
        std::iter::zip(right_xs, right_ys),
    )
    .map(|((&lx, &ly), (&rx, &ry))| {
        metric.segment_length(
            Point::new_unchecked_xy(lx, ly),
            Point::new_unchecked_xy(rx, ry),
        ) <= distance
    })
    .collect())
}

pub(crate) fn geodesic_point_distances(
    crs: &str,
    xs: &[f64],
    ys: &[f64],
    target: Point,
) -> Result<Vec<f64>> {
    ensure_geodesic_lonlat_crs(crs)?;
    ensure_geographic_lonlat(target.x, target.y)?;
    for (&x, &y) in std::iter::zip(xs, ys) {
        ensure_geographic_lonlat(x, y)?;
    }
    let crs = normalize(crs)?;
    with_geodesic(&crs, |geodesic| {
        let metric = EllipsoidMetric::for_geodesic(geodesic);
        std::iter::zip(xs, ys)
            .map(|(&x, &y)| {
                finite(
                    metric.segment_length(Point::new_unchecked_xy(x, y), target),
                    "geodesic distance",
                )
            })
            .collect()
    })
}

/// Segment-aware geodesic Hausdorff distance (meters) on `crs`'s ellipsoid.
///
/// The geodesic sibling of `Shape::hausdorff_distance`. Each directed sweep
/// is the linework max-min over source vertices and source segment interiors,
/// with target pruning via auxiliary-sphere bounds and exact
/// point-to-segment distances.
pub(crate) fn geodesic_hausdorff(crs: &str, a: &Shape, b: &Shape) -> Result<f64> {
    if a.is_empty() || b.is_empty() {
        return Ok(f64::INFINITY);
    }
    with_ellipsoid_metric(crs, &[a, b], |metric| {
        finite(
            a.geodesic_hausdorff_distance(b, metric),
            "geodesic Hausdorff distance",
        )
    })
}

/// Discrete geodesic Fréchet distance (meters) on `crs`'s ellipsoid.
///
/// The geodesic sibling of `Shape::frechet_distance`. The coupling DP mirrors
/// the planar discrete-Fréchet kernel exactly but couples on true Karney
/// distances, with band pruning around a greedy coupling upper bound.
pub(crate) fn geodesic_frechet(crs: &str, a: &Shape, b: &Shape) -> Result<f64> {
    // Fréchet requires non-empty linework on both sides; `single_linework`
    // raises `EmptyLinework` for empty input, exactly like the planar path
    // (do NOT short-circuit empty to inf — that is the distance/Hausdorff
    // convention, not Fréchet's).
    let left = a.single_linework()?;
    let right = b.single_linework()?;
    with_ellipsoid_metric(crs, &[a, b], |metric| {
        let distance = if left.coord_count() <= right.coord_count() {
            geodesic_frechet_dp(metric, left, right)
        } else {
            geodesic_frechet_dp(metric, right, left)
        };
        finite(distance, "geodesic Fréchet distance")
    })
}

/// Greedy diagonal-walk upper bound on the discrete geodesic Fréchet
/// bottleneck. `O(L + W)`, tight when the curves run close — exactly when the
/// band pays off.
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
/// pruning around [`geodesic_frechet_greedy_ub`], bit-identical to the full
/// table.
pub(crate) fn geodesic_frechet_dp<S: Coordinates + ?Sized, L: Coordinates + ?Sized>(
    metric: &EllipsoidMetric<'_>,
    short: &S,
    long: &L,
) -> f64 {
    const INF: f64 = f64::INFINITY;
    let width = short.coord_count();
    if width == 0 || long.coord_count() == 0 {
        return INF;
    }
    let distance = |p: Point, q: Point| metric.segment_length(p, q);
    let ub = geodesic_frechet_greedy_ub(metric, short, long);
    let mut previous = vec![INF; width];
    let mut current = vec![INF; width];

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
        std::mem::swap(&mut previous, &mut current);
        (prev_lo, prev_hi) = (cur_lo, cur_hi);
    }
    if prev_hi == width - 1 {
        previous[width - 1]
    } else {
        ub
    }
}
