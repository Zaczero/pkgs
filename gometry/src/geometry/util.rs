#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Point/coordinate utility helpers shared across the geometry kernels:
//! ring closure, exact/tolerant coordinate equality, consecutive-vertex
//! dedup, and the topology-coordinate bit canonicalization.

use std::simd::Select;
use std::simd::cmp::SimdPartialOrd;
use std::simd::num::SimdFloat;

use super::*;
use crate::error::Result;

pub(crate) fn closed_ring(mut points: Vec<Point>) -> Result<Vec<Point>> {
    if points.len() < Ring::MIN_VERTICES_OPEN {
        return Err(GeometryErrorKind::RingTooShort(points.len()).into());
    }
    if let (Some(first), Some(last)) = (points.first(), points.last())
        && !same_point(*first, *last)
    {
        points.push(*first);
    }
    Ok(points)
}

pub(crate) const fn empty_geometry() -> Shape {
    Shape::GeometryCollection(Vec::new())
}

pub(crate) fn bounds_to_shape(bounds: Bounds) -> Shape {
    if same_topological_coordinate(bounds.minx(), bounds.maxx())
        && same_topological_coordinate(bounds.miny(), bounds.maxy())
    {
        return Shape::Point(Point::new_unchecked_xy(bounds.minx(), bounds.miny()));
    }
    if same_topological_coordinate(bounds.minx(), bounds.maxx())
        || same_topological_coordinate(bounds.miny(), bounds.maxy())
    {
        return Shape::LineString(
            LineSeq::try_new(CoordSeq::from(vec![
                Point::new_unchecked_xy(bounds.minx(), bounds.miny()),
                Point::new_unchecked_xy(bounds.maxx(), bounds.maxy()),
            ]))
            .expect("degenerate rectangle line has two vertices"),
        );
    }
    Shape::Polygon(rect_polygon(bounds))
}

impl Shape {
    /// XY-unique vertices in first-seen order, pre-sized from
    /// [`coord_count`](Self::coord_count) and gathered without the boxed
    /// point iterator.
    pub(crate) fn unique_xy_points(&self) -> Vec<Point> {
        let capacity = self.coord_count();
        let mut seen = HashSet::with_capacity(capacity);
        let mut unique = Vec::with_capacity(capacity);
        self.for_each_point(|point| {
            if seen.insert(PointKey::new(point)) {
                unique.push(point.to_xy());
            }
        });
        unique
    }
}

pub(crate) fn unique_xy_points<I>(points: I) -> Vec<Point>
where
    I: IntoIterator,
    I::Item: Borrow<Point>,
{
    let iter = points.into_iter();
    let (lower, upper) = iter.size_hint();
    let capacity = upper.unwrap_or(lower);
    let mut seen = HashSet::with_capacity(capacity);
    let mut unique = Vec::with_capacity(capacity);
    for point in iter {
        let point = *point.borrow();
        if seen.insert(PointKey::new(point)) {
            unique.push(point);
        }
    }
    unique
}

pub(crate) fn same_point(left: impl Into<XY>, right: impl Into<XY>) -> bool {
    let (left, right) = (left.into(), right.into());
    same_topological_coordinate(left.x, right.x) && same_topological_coordinate(left.y, right.y)
}

/// Whether two positions match on every active ordinate (X/Y plus Z/M when
/// declared). Used by GeoJSON ring-closure admission and pack_admission so a
/// ring whose first/last differ only in Z is not treated as closed.
///
/// Axes must agree (homogeneous sequences already guarantee this). Inactive
/// Z/M sentinels are not compared. Topological ±0.0 equality matches
/// [`same_topological_coordinate`] / planar [`same_point`].
pub(crate) fn same_active_position(left: Point, right: Point) -> bool {
    if left.axes != right.axes {
        return false;
    }
    if !same_topological_coordinate(left.x, right.x)
        || !same_topological_coordinate(left.y, right.y)
    {
        return false;
    }
    if left.axes.has_z() {
        let (Some(lz), Some(rz)) = (left.z(), right.z()) else {
            return false;
        };
        if !same_topological_coordinate(lz, rz) {
            return false;
        }
    }
    if left.axes.has_m() {
        let (Some(lm), Some(rm)) = (left.m(), right.m()) else {
            return false;
        };
        if !same_topological_coordinate(lm, rm) {
            return false;
        }
    }
    true
}

/// Collapse consecutive duplicate vertices (order-preserving, first kept).
pub(crate) fn dedup_consecutive_points(v: &mut Vec<Point>) {
    v.dedup_by(|right, left| same_point(*left, *right));
}

/// Collapse consecutive duplicate planar coordinates (order-preserving).
pub(crate) fn dedup_consecutive_xy(v: &mut Vec<XY>) {
    v.dedup_by(|right, left| same_point(*left, *right));
}

/// Cyclic ring index `base + offset` wrapped into `0..len`, branchlessly.
/// Identical to `(base + offset) % len` whenever `offset <= len` (i.e. the
/// value is below `2 * len`) — which holds for every cyclic neighbour access
/// here (`+1`, `+2`, or `+ len - 1` predecessor). The compiler lowers the
/// single compare to a `cmov`, replacing the per-iteration integer `div` that
/// `% len` emits (which also blocks bounds-check elision and vectorization).
pub(crate) const fn wrap_index(value: usize, len: usize) -> usize {
    if value >= len { value - len } else { value }
}

pub(crate) const fn same_topological_coordinate(left: f64, right: f64) -> bool {
    topology_coordinate_bits(left) == topology_coordinate_bits(right)
}

pub(crate) const fn topology_coordinate_bits(value: f64) -> u64 {
    let bits = value.to_bits();
    if bits.trailing_zeros() >= 63 { 0 } else { bits }
}

/// SIMD twin of [`topology_coordinate_bits`]: canonicalize ``±0.0`` to zero
/// bits, preserve every other payload (including NaN) bit-exactly.
pub(crate) fn topology_coordinate_bits_simd(
    values: ReduceSimd,
) -> std::simd::Simd<u64, REDUCE_LANES> {
    let bits = values.to_bits();
    let zeroish =
        (bits & std::simd::Simd::splat(0x7FFF_FFFF_FFFF_FFFF)).simd_eq(std::simd::Simd::splat(0));
    zeroish.select(std::simd::Simd::splat(0), bits)
}

pub(crate) fn points_equal_exact<
    const Z: bool,
    const M: bool,
    A: Coordinates + ?Sized,
    B: Coordinates + ?Sized,
>(
    left: &A,
    right: &B,
    tolerance: f64,
) -> bool {
    if left.coord_count() != right.coord_count() {
        return false;
    }
    // Column fast path: compare the SoA ordinate runs directly (SIMD lanes)
    // instead of gathering per-vertex `Point`s. Falls back to the per-point
    // loop for non-column representations.
    if let (Some((left_xs, left_ys)), Some((right_xs, right_ys))) =
        (left.xy_columns(), right.xy_columns())
    {
        let optional_columns_within =
            |left: Option<&[f64]>, right: Option<&[f64]>| match (left, right) {
                (Some(left), Some(right)) => columns_within(left, right, tolerance),
                (None, None) => true,
                _ => false,
            };
        return columns_within(left_xs, right_xs, tolerance)
            && columns_within(left_ys, right_ys, tolerance)
            && (!Z || optional_columns_within(left.z_column(), right.z_column()))
            && (!M || optional_columns_within(left.m_column(), right.m_column()));
    }
    std::iter::zip(left.iter_coords(), right.iter_coords())
        .all(|(left, right)| point_equals_exact::<Z, M>(left, right, tolerance))
}

/// Column form of [`coord_within`]: every paired ordinate equal within
/// `tolerance`, in 8-wide SIMD lanes with a scalar tail. The lane test
/// `|l − r| ≤ tol || bits(l) == bits(r)` is exactly the scalar semantics:
/// the difference form covers ±0 at any tolerance, and the bit comparison
/// preserves the non-finite matching (`inf == inf`, `NaN` by payload) of
/// `same_topological_coordinate`.
pub(crate) fn columns_within(left: &[f64], right: &[f64], tolerance: f64) -> bool {
    debug_assert_eq!(left.len(), right.len());
    let tolerances = ReduceSimd::splat(tolerance);
    let (left_chunks, left_remainder) = left.as_chunks::<REDUCE_LANES>();
    let (right_chunks, right_remainder) = right.as_chunks::<REDUCE_LANES>();
    for (left_chunk, right_chunk) in std::iter::zip(left_chunks, right_chunks) {
        let left_lane = ReduceSimd::from_slice(left_chunk);
        let right_lane = ReduceSimd::from_slice(right_chunk);
        let within = (left_lane - right_lane).abs().simd_le(tolerances);
        let identical = left_lane.to_bits().simd_eq(right_lane.to_bits());
        if !(within | identical.cast()).all() {
            return false;
        }
    }
    std::iter::zip(left_remainder, right_remainder)
        .all(|(&left, &right)| coord_within(left, right, tolerance))
}

pub(crate) fn point_equals_exact<const Z: bool, const M: bool>(
    left: Point,
    right: Point,
    tolerance: f64,
) -> bool {
    coord_within(left.x, right.x, tolerance)
        && coord_within(left.y, right.y, tolerance)
        && (!Z || optional_coord_within(left.z(), right.z(), tolerance))
        && (!M || optional_coord_within(left.m(), right.m(), tolerance))
}

/// Per-ordinate equality within `tolerance` (JTS `Coordinate.equals2D` /
/// Shapely `equals_exact` semantics: `|left - right| <= tolerance`). At the
/// `tolerance == 0.0` default this reduces to exact equality while still
/// treating `+0.0`/`-0.0` as equal and matching non-finite ordinates by bits
/// (so `inf == inf`, which the difference form would turn into `NaN`).
pub(crate) fn coord_within(left: f64, right: f64, tolerance: f64) -> bool {
    same_topological_coordinate(left, right) || (left - right).abs() <= tolerance
}

pub(crate) fn optional_coord_within(left: Option<f64>, right: Option<f64>, tolerance: f64) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => coord_within(left, right, tolerance),
        (None, None) => true,
        _ => false,
    }
}
