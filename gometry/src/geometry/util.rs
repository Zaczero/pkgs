//! Point/coordinate utility helpers shared across the geometry kernels:
//! exact/tolerant coordinate equality, consecutive-vertex dedup, and the
//! topology-coordinate bit canonicalization. Ring admission lives on
//! [`crate::geometry::Ring`] (single owner for all untrusted ingresses).

use std::simd::Select as _;
use std::simd::cmp::{SimdPartialEq as _, SimdPartialOrd as _};
use std::simd::num::SimdFloat as _;

use crate::geometry::{
    Borrow, Bounds, CoordSeq, CoordinateAxes, Coordinates, GeometryKind, LineSeq, Point, PointKey,
    Polygon, REDUCE_LANES, ReduceSimd, Ring, Shape, ShapeCountQuery, XY, rect_polygon,
};

pub(crate) const fn empty_geometry() -> Shape {
    Shape::typed_empty(GeometryKind::GeometryCollection, CoordinateAxes::XY)
}

pub(crate) fn bounds_to_shape(bounds: Bounds) -> crate::error::Result<Shape> {
    if same_topological_coordinate(bounds.minx(), bounds.maxx())
        && same_topological_coordinate(bounds.miny(), bounds.maxy())
    {
        return Ok(Shape::Point(Point::new_raw_xy(
            bounds.minx(),
            bounds.miny(),
        )));
    }
    if same_topological_coordinate(bounds.minx(), bounds.maxx())
        || same_topological_coordinate(bounds.miny(), bounds.maxy())
    {
        return Ok(Shape::LineString(LineSeq::from_proven(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(bounds.minx(), bounds.miny()),
                Point::new_raw_xy(bounds.maxx(), bounds.maxy()),
            ])?,
        )));
    }
    Ok(Shape::Polygon(rect_polygon(bounds)?))
}

/// Build a geographic rectangle whose horizontal sides approximate parallels.
pub(crate) fn geographic_bounds_to_shape(
    bounds: Bounds,
    frame: crate::geographic::GeographicFrame,
) -> crate::error::Result<Shape> {
    // Geographic box parallels use equal-longitude geodesic chords at this
    // angular spacing; the resulting metric fidelity is documented in the
    // geographic CRS guide.
    const MAX_CHORD_RADIANS: f64 = 0.247;
    if same_topological_coordinate(bounds.minx(), bounds.maxx()) {
        return bounds_to_shape(bounds);
    }
    let segments = ((bounds.maxx() - bounds.minx()) * frame.longitude().unit().radians_per_unit()
        / MAX_CHORD_RADIANS)
        .ceil()
        .max(1.0) as usize;
    let capacity = if same_topological_coordinate(bounds.miny(), bounds.maxy()) {
        segments.checked_add(1)
    } else {
        segments
            .checked_mul(2)
            .and_then(|value| value.checked_add(3))
    }
    .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let mut points = Vec::new();
    points.reserve_exact(capacity);
    let span = bounds.maxx() - bounds.minx();
    points.push(Point::new_raw_xy(bounds.minx(), bounds.miny()));
    for index in 1..segments {
        points.push(Point::new_raw_xy(
            bounds.minx() + span * index as f64 / segments as f64,
            bounds.miny(),
        ));
    }
    points.push(Point::new_raw_xy(bounds.maxx(), bounds.miny()));
    if same_topological_coordinate(bounds.miny(), bounds.maxy()) {
        return Ok(Shape::LineString(LineSeq::from_proven(
            CoordSeq::try_from_points(&points)?,
        )));
    }
    points.push(Point::new_raw_xy(bounds.maxx(), bounds.maxy()));
    for index in (1..segments).rev() {
        points.push(Point::new_raw_xy(
            bounds.minx() + span * index as f64 / segments as f64,
            bounds.maxy(),
        ));
    }
    points.push(Point::new_raw_xy(bounds.minx(), bounds.maxy()));
    points.push(Point::new_raw_xy(bounds.minx(), bounds.miny()));
    Ok(Shape::Polygon(Polygon::new(
        Ring::try_from_closed(CoordSeq::try_from_points(&points)?)?,
        Vec::new(),
    )))
}

impl Shape {
    /// XY-unique vertices in first-seen order.
    pub(crate) fn try_unique_xy_points(&self) -> crate::error::Result<Vec<Point>> {
        let capacity = ShapeCountQuery::default().coordinates(self).ok();
        let mut seen = crate::collections::try_hash_set()?;
        let mut unique = Vec::new();
        if let Some(capacity) = capacity {
            let _seen_reservation = seen.try_reserve(capacity);
            let _unique_reservation = unique.try_reserve(capacity);
        }
        self.for_each_point(|point| {
            if seen.insert(PointKey::new(point)) {
                unique.push(point.to_xy());
            }
        });
        Ok(unique)
    }

    /// XY-unique vertices in first-seen order, keeping each survivor's full
    /// coordinates (Z/M and signed-zero spelling).
    pub(crate) fn first_seen_xy_vertices(&self) -> crate::error::Result<Vec<Point>> {
        let capacity = ShapeCountQuery::default().coordinates(self).ok();
        let mut seen = crate::collections::try_hash_set()?;
        let mut unique = Vec::new();
        if let Some(capacity) = capacity {
            let _seen_reservation = seen.try_reserve(capacity);
            let _unique_reservation = unique.try_reserve(capacity);
        }
        self.for_each_point(|point| {
            if seen.insert(PointKey::new(point)) {
                unique.push(point);
            }
        });
        Ok(unique)
    }
}

pub(crate) fn unique_xy_points<I>(points: I) -> crate::error::Result<Vec<Point>>
where
    I: IntoIterator,
    I::Item: Borrow<Point>,
{
    let iter = points.into_iter();
    let (lower, upper) = iter.size_hint();
    let capacity = upper.unwrap_or(lower);
    let mut seen = crate::collections::try_hash_set()?;
    let mut unique = Vec::new();
    let _seen_reservation = seen.try_reserve(capacity);
    let _unique_reservation = unique.try_reserve(capacity);
    for point in iter {
        let point = *point.borrow();
        if seen.insert(PointKey::new(point)) {
            unique.push(point);
        }
    }
    Ok(unique)
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

/// Per-ordinate equality within `tolerance` (`|left - right| <= tolerance`).
/// At the `tolerance == 0.0` default this reduces to exact equality while
/// still treating `+0.0`/`-0.0` as equal and matching non-finite ordinates by
/// bits (so `inf == inf`, which the difference form would turn into `NaN`).
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

#[cfg(test)]
mod unique_xy_capacity_tests {
    use super::*;

    struct HugeSizeHint<I>(I);

    impl<I: Iterator> Iterator for HugeSizeHint<I> {
        type Item = I::Item;

        fn next(&mut self) -> Option<Self::Item> {
            self.0.next()
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            (0, Some(usize::MAX))
        }
    }

    #[test]
    fn huge_advisory_size_hint_preserves_first_seen_xy_order() {
        let first = Point::new_raw_xy(1.0, 2.0);
        let second = Point::new_raw_xy(3.0, 4.0);
        let last = Point::new_raw_xy(5.0, 6.0);

        let unique =
            unique_xy_points(HugeSizeHint([first, second, first, last].into_iter())).unwrap();

        assert_eq!(unique, [first, second, last]);
    }
}
