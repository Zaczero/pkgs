//! Point/coordinate utility helpers shared across the geometry kernels:
//! exact/tolerant coordinate equality, consecutive-vertex dedup, and the
//! topology-coordinate bit canonicalization. Ring admission lives on
//! [`crate::geometry::Ring`] (single owner for all untrusted ingresses).

use std::convert::Infallible;
use std::ops::ControlFlow;
use std::simd::Select as _;
use std::simd::cmp::{SimdPartialEq as _, SimdPartialOrd as _};
use std::simd::num::SimdFloat as _;

use crate::geometry::{
    Borrow, Bounds, CoordSeq, CoordinateAxes, Coordinates, GeometryKind, LineSeq, Point, PointKey,
    Polygon, REDUCE_LANES, ReduceSimd, Ring, RunIdentity, Shape, XY, rect_polygon,
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

/// Build a geographic rectangle whose sides follow chart parallels and meridians.
pub(crate) fn geographic_bounds_to_shape(
    bounds: Bounds,
    frame: crate::geographic::GeographicFrame,
) -> crate::error::Result<Shape> {
    let shape = geographic_bounds_chart(bounds, frame)?;
    crate::geometry::publish_stored_computed_shape(&shape, frame, "geographic bounds")
}

pub(crate) fn geographic_bounds_chart(
    bounds: Bounds,
    frame: crate::geographic::GeographicFrame,
) -> crate::error::Result<Shape> {
    if same_topological_coordinate(bounds.minx(), bounds.maxx()) {
        return bounds_to_shape(bounds);
    }
    let span = bounds.maxx() - bounds.minx();
    // Each step must stay strictly below the topology's half-period so its
    // short-way longitude direction is unambiguous.
    let steps = ((span / frame.longitude().half_period()).floor() as usize)
        .checked_add(1)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let capacity = if same_topological_coordinate(bounds.miny(), bounds.maxy()) {
        steps.checked_add(1)
    } else {
        steps.checked_mul(2).and_then(|value| value.checked_add(3))
    }
    .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let mut points = Vec::new();
    points.reserve_exact(capacity);
    points.push(Point::new_raw_xy(bounds.minx(), bounds.miny()));
    for index in 1..steps {
        points.push(Point::new_raw_xy(
            bounds.minx() + span * index as f64 / steps as f64,
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
    for index in (1..steps).rev() {
        points.push(Point::new_raw_xy(
            bounds.minx() + span * index as f64 / steps as f64,
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
    #[expect(
        clippy::disallowed_methods,
        reason = "The source-coordinate upper bound may greatly exceed the deduplicated output."
    )]
    pub(crate) fn try_unique_xy_points(&self) -> crate::error::Result<Vec<Point>> {
        let mut unique = Vec::new();
        if let Some(capacity) = self.point_set_capacity_hint(RunIdentity::XY) {
            let _reservation = unique.try_reserve(capacity);
        }
        let ControlFlow::Continue(()): ControlFlow<Infallible> = self
            .try_for_each_distinct_xy_point(|point| {
                unique.push(point.to_xy());
                ControlFlow::Continue(())
            })?;
        Ok(unique)
    }

    /// XY-unique vertices in first-seen order, keeping each survivor's full
    /// coordinates (Z/M and signed-zero spelling).
    #[expect(
        clippy::disallowed_methods,
        reason = "The source-coordinate upper bound may greatly exceed the deduplicated output."
    )]
    pub(crate) fn first_seen_xy_vertices(&self) -> crate::error::Result<Vec<Point>> {
        let mut unique = Vec::new();
        if let Some(capacity) = self.point_set_capacity_hint(RunIdentity::XY) {
            let _reservation = unique.try_reserve(capacity);
        }
        let ControlFlow::Continue(()): ControlFlow<Infallible> = self
            .try_for_each_distinct_xy_point(|point| {
                unique.push(point);
                ControlFlow::Continue(())
            })?;
        Ok(unique)
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "Iterator size_hint is advisory and can greatly exceed the deduplicated output."
)]
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
/// declared). Used by GeoJSON ring-closure admission and storage::admission so a
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

pub(crate) fn push_distinct_active_position(points: &mut Vec<Point>, point: Point) {
    if points
        .last()
        .is_none_or(|last| !same_active_position(*last, point))
    {
        points.push(point);
    }
}

pub(crate) fn push_distinct_xy_position(points: &mut Vec<Point>, point: Point) {
    if points.last().is_none_or(|last| !same_point(*last, point)) {
        points.push(point);
    }
}

/// Collapse consecutive duplicate vertices (order-preserving, first kept).
pub(crate) fn dedup_consecutive_points(v: &mut Vec<Point>) {
    v.dedup_by(|right, left| same_point(*left, *right));
}

/// Wrap a precomputed cyclic index once into `0..len`.
///
/// The caller supplies a nonzero length and a value below twice that length;
/// one conditional subtraction then produces the wrapped index without a
/// remainder.
pub(crate) const fn wrap_index(value: usize, len: usize) -> usize {
    if value >= len {
        value.wrapping_sub(len)
    } else {
        value
    }
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

/// Column form of [`coord_within`]: every paired ordinate equal within the
/// admitted finite, nonnegative `tolerance`, in 8-wide SIMD lanes with a scalar
/// tail. The lane test `|l − r| ≤ tol || bits(l) == bits(r)` preserves the
/// scalar semantics in that domain: the difference form covers ±0, and the
/// bit comparison preserves the non-finite matching (`inf == inf`, `NaN` by
/// payload) of `same_topological_coordinate`.
pub(crate) fn columns_within(left: &[f64], right: &[f64], tolerance: f64) -> bool {
    debug_assert_eq!(left.len(), right.len());
    let tolerances = ReduceSimd::splat(tolerance);
    let (left_chunks, left_remainder) = left.as_chunks::<REDUCE_LANES>();
    let (right_chunks, right_remainder) = right.as_chunks::<REDUCE_LANES>();
    for (left_chunk, right_chunk) in std::iter::zip(left_chunks, right_chunks) {
        let left_lane = ReduceSimd::from_slice(left_chunk);
        let right_lane = ReduceSimd::from_slice(right_chunk);
        #[expect(
            clippy::arithmetic_side_effects,
            reason = "IEEE subtraction intentionally permits finite overflow and NaN before the tolerance and bit-identity comparisons"
        )]
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

#[cfg(test)]
mod cyclic_and_nearness_tests {
    use super::{columns_within, wrap_index};

    #[test]
    fn wrap_index_retains_single_wrap_boundaries_and_extrema() {
        for (value, len, expected) in [
            (0_usize, 1, 0),
            (1, 1, 0),
            (0, 2, 0),
            (1, 2, 1),
            (2, 2, 0),
            (3, 2, 1),
            (4, 5, 4),
            (5, 5, 0),
            (9, 5, 4),
        ] {
            assert_eq!(wrap_index(value, len), expected);
        }

        let maximum_half = usize::MAX.wrapping_div(2);
        assert_eq!(
            wrap_index(usize::MAX.wrapping_sub(1), usize::MAX),
            usize::MAX.wrapping_sub(1)
        );
        assert_eq!(wrap_index(usize::MAX, usize::MAX), 0);
        assert_eq!(
            wrap_index(usize::MAX, maximum_half.wrapping_add(1)),
            maximum_half
        );
    }

    #[test]
    fn columns_within_keeps_tolerance_boundaries_across_chunks_and_tail() {
        for len in [0_usize, 1, 7, 8, 9, 16, 17] {
            let equal = vec![0.0; len];
            assert!(columns_within(&equal, &equal, 0.0), "equal length {len}");
        }

        let distinct = [
            2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0,
            18.0,
        ];
        for (len, position) in [(1_usize, 0_usize), (8, 7), (9, 8), (17, 16)] {
            let mut left: Vec<_> = distinct.iter().copied().take(len).collect();
            let mut right = left.clone();
            *left
                .get_mut(position)
                .expect("test position is in its column") = 0.0;
            *right
                .get_mut(position)
                .expect("test position is in its column") = 1.0;

            assert!(
                columns_within(&left, &right, 1.0),
                "inclusive tolerance at position {position}"
            );
            assert!(
                !columns_within(&left, &right, 1.0_f64.next_down()),
                "value above tolerance at position {position}"
            );
            assert!(
                columns_within(&left, &right, 1.0_f64.next_up()),
                "value below tolerance at position {position}"
            );
        }

        let negative_zero = [-0.0; 9];
        let positive_zero = [0.0; 9];
        assert!(columns_within(&negative_zero, &positive_zero, 0.0));
        assert!(columns_within(&negative_zero, &positive_zero, -0.0));
    }

    #[rstest::rstest]
    #[case::equal_positive_infinity(
        "equal positive infinity",
        f64::INFINITY,
        f64::INFINITY,
        0.0,
        true
    )]
    #[case::equal_negative_infinity(
        "equal negative infinity",
        f64::NEG_INFINITY,
        f64::NEG_INFINITY,
        0.0,
        true
    )]
    #[case::opposite_infinities(
        "opposite infinities",
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::MAX,
        false
    )]
    #[case::finite_and_infinity("finite and infinity", f64::MAX, f64::INFINITY, f64::MAX, false)]
    #[case::same_nan_payload(
        "same NaN payload",
        f64::from_bits(0x7FF8_0000_0000_0001),
        f64::from_bits(0x7FF8_0000_0000_0001),
        0.0,
        true
    )]
    #[case::different_nan_payload(
        "different NaN payload",
        f64::from_bits(0x7FF8_0000_0000_0001),
        f64::from_bits(0x7FF8_0000_0000_0002),
        f64::MAX,
        false
    )]
    #[case::finite_subtraction_overflow("finite subtraction overflow", f64::MAX, -f64::MAX, f64::MAX, false)]
    #[case::subnormal_above_zero_tolerance(
        "subnormal above zero tolerance",
        f64::from_bits(1),
        0.0,
        0.0,
        false
    )]
    #[case::subnormal_at_tolerance(
        "subnormal at tolerance",
        f64::from_bits(1),
        0.0,
        f64::from_bits(1),
        true
    )]
    fn columns_within_preserves_exceptional_ordinate_decisions_in_chunks_and_tail(
        #[case] name: &'static str,
        #[case] left_value: f64,
        #[case] right_value: f64,
        #[case] tolerance: f64,
        #[case] expected: bool,
    ) {
        for position in [0_usize, 8] {
            let mut left = [0.0; 9];
            let mut right = [0.0; 9];
            *left
                .get_mut(position)
                .expect("test position is in its column") = left_value;
            *right
                .get_mut(position)
                .expect("test position is in its column") = right_value;
            assert_eq!(
                columns_within(&left, &right, tolerance),
                expected,
                "{name} at position {position}"
            );
        }
    }
}
