use std::sync::Arc;

use crate::geometry::types::*;
use crate::test_support::allocation_count;

/// Test fixtures only. These panic on an empty payload, which is exactly what
/// a test wants and exactly what production must not have: keeping them in the
/// test tree means a production caller reaching for `.into()` fails to compile
/// and is pushed to `Shape::multi_point`/`narrow_points`, which admit the empty
/// case as `Shape::Empty`.
mod fixture_conversions {
    use super::*;

    impl From<CoordSeq> for MultiPoint {
        fn from(coords: CoordSeq) -> Self {
            Self::from_coordseq(coords).expect("test multipoint payload is nonempty")
        }
    }

    #[expect(
        clippy::fallible_impl_from,
        reason = "test fixtures panic on empty; TryFrom would reintroduce a production-shaped empty path"
    )]
    impl From<Vec<Point>> for MultiPoint {
        fn from(points: Vec<Point>) -> Self {
            Self::from_coordseq(CoordSeq::try_from_points(&points).unwrap())
                .expect("test multipoint payload is nonempty")
        }
    }

    #[expect(
        clippy::fallible_impl_from,
        reason = "test fixtures panic on empty; TryFrom would reintroduce a production-shaped empty path"
    )]
    impl From<Vec<XY>> for MultiPoint {
        fn from(points: Vec<XY>) -> Self {
            Self::from_coordseq(CoordSeq::try_from_xy(&points).unwrap())
                .expect("test multipoint payload is nonempty")
        }
    }

    impl FromIterator<Point> for MultiPoint {
        fn from_iter<T: IntoIterator<Item = Point>>(iter: T) -> Self {
            iter.into_iter().collect::<Vec<_>>().into()
        }
    }

    impl From<Vec<LineSeq>> for MultiLineString {
        fn from(lines: Vec<LineSeq>) -> Self {
            Self::from_dense(lines).expect("test multilinestring payload is nonempty")
        }
    }

    impl FromIterator<LineSeq> for MultiLineString {
        fn from_iter<T: IntoIterator<Item = LineSeq>>(iter: T) -> Self {
            iter.into_iter().collect::<Vec<_>>().into()
        }
    }

    impl From<Vec<Polygon>> for MultiPolygon {
        fn from(polygons: Vec<Polygon>) -> Self {
            Self::from_dense(polygons).expect("test multipolygon payload is nonempty")
        }
    }

    impl FromIterator<Polygon> for MultiPolygon {
        fn from_iter<T: IntoIterator<Item = Polygon>>(iter: T) -> Self {
            iter.into_iter().collect::<Vec<_>>().into()
        }
    }
}

mod multipoint_iteration_tests {
    use super::*;

    fn point(x: f64, y: f64, z: Option<f64>, m: Option<f64>) -> Point {
        Point::new_axes(x, y, ZOrdinate(z), MOrdinate(m)).expect("finite point")
    }

    #[test]
    fn borrowed_iteration_filters_empty_members_in_order_and_preserves_axes() {
        let points = MultiPoint::try_new(CoordinateAxes::XYZM, vec![
            MultiMember::Empty,
            MultiMember::Value(point(1.0, 2.0, Some(3.0), Some(4.0))),
            MultiMember::Empty,
            MultiMember::Value(point(5.0, 6.0, Some(7.0), Some(8.0))),
            MultiMember::Empty,
        ])
        .expect("homogeneous members");

        let mut iter = (&points).into_iter();
        assert_eq!(iter.len(), 2);
        assert_eq!(
            iter.next_back(),
            Some(point(5.0, 6.0, Some(7.0), Some(8.0)))
        );
        assert_eq!(iter.len(), 1);
        assert_eq!(iter.next(), Some(point(1.0, 2.0, Some(3.0), Some(4.0))));
        assert_eq!(iter.len(), 0);
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next_back(), None);

        let empty = MultiPoint::try_new(CoordinateAxes::XYZM, vec![
            MultiMember::Empty,
            MultiMember::Empty,
        ])
        .expect("embedded empties are valid members");
        assert_eq!((&empty).into_iter().len(), 0);
        assert_eq!((&empty).into_iter().next(), None);
    }

    #[test]
    fn column_iteration_and_segment_pairs_preserve_each_coordinate_layout() {
        for (axes, first, second) in [
            (
                CoordinateAxes::XY,
                point(1.0, 2.0, None, None),
                point(5.0, 6.0, None, None),
            ),
            (
                CoordinateAxes::XYZ,
                point(1.0, 2.0, Some(3.0), None),
                point(5.0, 6.0, Some(7.0), None),
            ),
            (
                CoordinateAxes::XYM,
                point(1.0, 2.0, None, Some(4.0)),
                point(5.0, 6.0, None, Some(8.0)),
            ),
            (
                CoordinateAxes::XYZM,
                point(1.0, 2.0, Some(3.0), Some(4.0)),
                point(5.0, 6.0, Some(7.0), Some(8.0)),
            ),
        ] {
            let expected = vec![first, second];
            let coords = CoordSeq::try_from_points(&expected).expect("matching axes");
            assert_eq!(coords.axes(), axes);
            assert_eq!(coords.iter_coords().collect::<Vec<_>>(), expected);
            assert_eq!(expected.iter_coords().collect::<Vec<_>>(), expected);

            let empty = CoordSeq::try_from_vecs(
                Vec::new(),
                Vec::new(),
                axes.has_z().then(Vec::new),
                axes.has_m().then(Vec::new),
            )
            .expect("empty columns have matching axes");
            assert_eq!(empty.segment_pairs().next(), None);
            let singleton = CoordSeq::try_from_points(&[first]).expect("one coordinate");
            assert_eq!(singleton.segment_pairs().next(), None);

            let line = LineSeq::from_proven(coords.clone());
            assert_eq!(line.iter_coords().collect::<Vec<_>>(), expected);
            assert_eq!(line.iter_coords().rev().collect::<Vec<_>>(), vec![
                second, first
            ],);
            assert_eq!(line.segment_pairs().collect::<Vec<_>>(), vec![[
                first, second
            ]]);

            let forwarded: &LineSeq = &line;
            assert_eq!(
                Coordinates::iter_coords(&forwarded).collect::<Vec<_>>(),
                expected,
            );

            let multipoint = MultiPoint::from_coordseq(coords).expect("nonempty sequence");
            assert_eq!(multipoint.iter_coords().collect::<Vec<_>>(), expected);
        }

        let xy_points = vec![XY::new(1.0, 2.0), XY::new(5.0, 6.0)];
        assert_eq!(xy_points.iter_coords().collect::<Vec<_>>(), vec![
            Point::new_raw_xy(1.0, 2.0),
            Point::new_raw_xy(5.0, 6.0)
        ],);
    }

    #[test]
    fn column_iterator_alternates_ends_without_crossing_xyzm_lanes() {
        let [first, second, middle, fourth, last] = [
            point(1.0, 11.0, Some(21.0), Some(31.0)),
            point(2.0, 12.0, Some(22.0), Some(32.0)),
            point(3.0, 13.0, Some(23.0), Some(33.0)),
            point(4.0, 14.0, Some(24.0), Some(34.0)),
            point(5.0, 15.0, Some(25.0), Some(35.0)),
        ];
        let coords = CoordSeq::try_from_points(&[first, second, middle, fourth, last])
            .expect("matching XYZM coordinates");
        let mut iter = coords.iter();

        assert_eq!(iter.len(), 5);
        assert_eq!(iter.size_hint(), (5, Some(5)));
        assert_eq!(iter.next(), Some(first));
        assert_eq!(iter.len(), 4);
        assert_eq!(iter.size_hint(), (4, Some(4)));
        assert_eq!(iter.next_back(), Some(last));
        assert_eq!(iter.len(), 3);
        assert_eq!(iter.size_hint(), (3, Some(3)));
        assert_eq!(iter.next(), Some(second));
        assert_eq!(iter.len(), 2);
        assert_eq!(iter.size_hint(), (2, Some(2)));
        assert_eq!(iter.next_back(), Some(fourth));
        assert_eq!(iter.len(), 1);
        assert_eq!(iter.size_hint(), (1, Some(1)));
        assert_eq!(iter.next(), Some(middle));
        assert_eq!(iter.len(), 0);
        assert_eq!(iter.size_hint(), (0, Some(0)));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next_back(), None);
        assert_eq!(iter.next_back(), None);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn borrowed_iteration_and_shape_scans_do_not_allocate() {
        let multipoint = MultiPoint::from_coordseq(
            CoordSeq::try_from_points(&[Point::new_raw_xy(1.0, 2.0), Point::new_raw_xy(3.0, 4.0)])
                .unwrap(),
        )
        .expect("nonempty");
        let (sum, allocations) =
            allocation_count(|| (&multipoint).into_iter().map(|p| p.x).sum::<f64>());
        assert_eq!(sum, 4.0);
        assert_eq!(allocations, 0);

        let shape = Shape::MultiPoint(multipoint);
        let (sum, allocations) = allocation_count(|| {
            let mut sum = 0.0;
            shape.for_each_point(|point| sum += point.x);
            sum
        });
        assert_eq!(sum, 4.0);
        assert_eq!(allocations, 0);

        let (issue, allocations) = allocation_count(|| shape.validate().unwrap());
        assert_eq!(issue, None);
        assert_eq!(allocations, 0);
    }

    #[test]
    fn borrowed_iteration_short_circuits_and_owned_collection_remains_available() {
        let multipoint = MultiPoint::from_coordseq(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(1.0, 2.0),
                Point::new_raw_xy(3.0, 4.0),
                Point::new_raw_xy(5.0, 6.0),
            ])
            .unwrap(),
        )
        .expect("nonempty");
        let mut visited = 0;
        let result: Result<(), ()> =
            Shape::MultiPoint(multipoint.clone()).try_for_each_point(|_| {
                visited += 1;
                Err(())
            });
        assert_eq!(result, Err(()));
        assert_eq!(visited, 1);
        assert_eq!(multipoint.into_iter().count(), 3);
    }
}

mod csr_offset_column_tests {
    use super::*;

    #[test]
    fn csr_offset_column_validates_monotonic_zero_start() {
        let column = CsrOffsetColumn::<()>::try_new(vec![0, 2, 5], 5).expect("valid column");
        assert_eq!(column.shared_offsets().as_ref(), &[0, 2, 5]);
    }

    #[test]
    fn csr_offset_builder_finish_matches_try_new() {
        let mut builder = CsrOffsetBuilder::new();
        builder.push_end(3, 10).expect("in-range end");
        builder.push_end(7, 10).expect("in-range end");
        let built = builder.finish(7).expect("finish");
        let direct = CsrOffsetColumn::<()>::try_new(vec![0, 3, 7], 7).expect("direct");
        assert_eq!(
            built.shared_offsets().as_ref(),
            direct.shared_offsets().as_ref()
        );
    }

    #[test]
    fn csr_offset_builder_rejects_invalid_final_capacity() {
        let mut builder = CsrOffsetBuilder::new();
        builder.push_end(7, 7).unwrap();
        let error = builder.finish(6).unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::OffsetCapacityExceeded)
        ));
    }

    #[test]
    fn csr_offset_rejects_overflow_at_freeze() {
        let overflow = (i32::MAX as usize).saturating_add(1);
        drop(CsrOffsetColumn::<()>::try_new(vec![0, overflow], overflow).unwrap_err());
    }

    #[test]
    fn csr_offset_rejects_malformed_non_monotonic_offsets() {
        use crate::error::ErrorKind;

        let error = CsrOffsetColumn::<()>::try_new(vec![0, 5, 3], 5).expect_err("non-monotonic");
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::MalformedCsrOffsets)
        ));
        assert!(error.to_string().contains("malformed"));
    }

    #[test]
    fn csr_offset_try_from_shared_i32_roundtrips_shared_offsets() {
        let column = CsrOffsetColumn::<()>::try_new(vec![0, 3, 7], 7).expect("valid");
        let arc = column.shared_offsets();
        let roundtrip = CsrOffsetColumn::<()>::try_from_shared_i32(arc, 7).expect("roundtrip");
        assert_eq!(roundtrip.as_slice(), &[0, 3, 7]);
    }

    #[test]
    fn csr_offset_try_from_shared_i32_rejects_malformed_offsets() {
        use crate::error::ErrorKind;

        for offsets in [vec![], vec![1, 2], vec![0, -1, 3], vec![0, 2, 1]] {
            let error = CsrOffsetColumn::<()>::try_from_shared_i32(
                crate::geometry::SharedSlice::from_vec(offsets),
                3,
            )
            .expect_err("malformed external offsets");
            assert!(matches!(
                error.kind(),
                ErrorKind::Geometry(GeometryErrorKind::MalformedCsrOffsets)
            ));
        }
    }

    #[test]
    fn csr_offset_try_from_shared_i32_rejects_terminal_outside_parent() {
        use crate::error::ErrorKind;

        let error = CsrOffsetColumn::<()>::try_from_shared_i32(
            crate::geometry::SharedSlice::from_vec(vec![0, 3]),
            2,
        )
        .expect_err("terminal exceeds the admitted parent");
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::OffsetCapacityExceeded)
        ));
    }
}

#[cfg(test)]
mod coordseq_builder_tests {
    use super::*;
    use crate::error::ErrorKind;

    #[test]
    fn finish_rejects_mismatched_point_axes() {
        let mut builder = CoordSeqBuilder::with_capacity(CoordinateAxes::XY, 2);
        builder.push(Point::new_raw_xy(0.0, 0.0));
        builder.push(Point::new_raw_axes(
            1.0,
            1.0,
            ZOrdinate(Some(0.0)),
            MOrdinate(None),
        ));
        let err = builder.finish().unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::Geometry(GeometryErrorKind::CoordinateAxesMismatch {
                declared: CoordinateAxes::XY,
                got: CoordinateAxes::XYZ,
            })
        ));
    }

    #[test]
    fn finish_rejects_push_at_from_xyz_source_into_xy_builder() {
        let source = CoordSeq::try_from_points(&[Point::new_raw_axes(
            1.0,
            1.0,
            ZOrdinate(Some(2.0)),
            MOrdinate(None),
        )])
        .unwrap();
        let mut builder = CoordSeqBuilder::with_capacity(CoordinateAxes::XY, 1);
        builder.push_at(&source, 0);
        let err = builder.finish().unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::Geometry(GeometryErrorKind::CoordinateAxesMismatch {
                declared: CoordinateAxes::XY,
                got: CoordinateAxes::XYZ,
            })
        ));
    }

    #[test]
    fn try_from_points_rejects_mixed_axes_without_panicking() {
        let points = [
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_axes(1.0, 1.0, ZOrdinate(Some(2.0)), MOrdinate(None)),
        ];
        let err = CoordSeq::try_from_points(&points).unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::Geometry(GeometryErrorKind::CoordinateAxesMismatch {
                declared: CoordinateAxes::XY,
                got: CoordinateAxes::XYZ,
            })
        ));
    }

    #[test]
    fn try_from_points_preserves_ordinate_bits_and_order() {
        assert_eq!(
            CoordSeq::try_from_points(&[]).unwrap().axes(),
            CoordinateAxes::XY
        );
        for axes in [
            CoordinateAxes::XY,
            CoordinateAxes::XYZ,
            CoordinateAxes::XYM,
            CoordinateAxes::XYZM,
        ] {
            let points: Vec<_> = (0..256)
                .map(|index| {
                    let zero = if index % 2 == 0 { -0.0 } else { 0.0 };
                    Point::new_raw_axes(
                        zero,
                        f64::from(index / 3),
                        ZOrdinate(axes.has_z().then_some(zero)),
                        MOrdinate(axes.has_m().then_some(f64::from(index))),
                    )
                })
                .collect();
            let (coords, allocations) =
                allocation_count(|| CoordSeq::try_from_points(&points).unwrap());
            assert_eq!(coords.axes(), axes);
            assert_eq!(coords.len(), points.len());
            assert_eq!(
                allocations,
                2 * (2 + usize::from(axes.has_z()) + usize::from(axes.has_m()))
            );
            for (actual, expected) in coords.iter().zip(points) {
                assert_eq!(actual.x.to_bits(), expected.x.to_bits());
                assert_eq!(actual.y.to_bits(), expected.y.to_bits());
                assert_eq!(actual.z().map(f64::to_bits), expected.z().map(f64::to_bits));
                assert_eq!(actual.m().map(f64::to_bits), expected.m().map(f64::to_bits));
            }
        }
    }

    #[rstest::rstest]
    #[case::f64_nan(f64::NAN)]
    #[case::f64_infinity(f64::INFINITY)]
    #[case::f64_neg_infinity(f64::NEG_INFINITY)]
    fn try_from_points_rejects_nonfinite_active_ordinates(#[case] value: f64) {
        for point in [
            Point::new_raw_xy(value, 0.0),
            Point::new_raw_xy(0.0, value),
            Point::new_raw_axes(0.0, 0.0, ZOrdinate(Some(value)), MOrdinate(None)),
            Point::new_raw_axes(0.0, 0.0, ZOrdinate(None), MOrdinate(Some(value))),
        ] {
            let error = CoordSeq::try_from_points(&[point, point]).unwrap_err();
            assert!(matches!(
                error.kind(),
                ErrorKind::Geometry(GeometryErrorKind::NonFiniteCoordinate)
            ));
        }
    }

    #[test]
    fn try_from_points_first_axes_mismatch_precedes_finiteness() {
        let points = [
            Point::new_raw_xy(f64::NAN, 0.0),
            Point::new_raw_axes(1.0, 1.0, ZOrdinate(Some(2.0)), MOrdinate(None)),
            Point::new_raw_axes(1.0, 1.0, ZOrdinate(None), MOrdinate(Some(2.0))),
        ];
        let error = CoordSeq::try_from_points(&points).unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::CoordinateAxesMismatch {
                declared: CoordinateAxes::XY,
                got: CoordinateAxes::XYZ,
            })
        ));
    }

    #[test]
    fn point_mapping_publishes_directly_into_coordinate_columns() {
        let source = CoordSeq::try_from_points(
            &(0..256)
                .map(|index| Point::new_raw_xy(f64::from(index), 0.0))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let (mapped, allocations) =
            allocation_count(|| source.try_map_points(Ok::<_, crate::error::Error>).unwrap());
        assert_eq!(mapped, source);
        assert_eq!(allocations, 4, "two columns and their shared owners");
    }

    #[test]
    fn point_mapping_callback_error_precedes_axes_validation() {
        let source =
            CoordSeq::try_from_points(&[Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(1.0, 1.0)])
                .unwrap();
        let mut first = true;
        let error = source
            .try_map_points(|point| {
                if std::mem::take(&mut first) {
                    Ok(Point::new_raw_axes(
                        point.x,
                        point.y,
                        ZOrdinate(Some(1.0)),
                        MOrdinate(None),
                    ))
                } else {
                    Err(GeometryErrorKind::message("second callback"))
                }
            })
            .unwrap_err();
        assert!(error.to_string().contains("second callback"));
    }

    #[test]
    fn coordinate_axes_from_bits_accepts_only_canonical_layouts() {
        assert_eq!(CoordinateAxes::from_bits(0), Some(CoordinateAxes::XY));
        assert_eq!(CoordinateAxes::from_bits(1), Some(CoordinateAxes::XYZ));
        assert_eq!(CoordinateAxes::from_bits(2), Some(CoordinateAxes::XYM));
        assert_eq!(CoordinateAxes::from_bits(3), Some(CoordinateAxes::XYZM));
        assert_eq!(CoordinateAxes::from_bits(4), None);
    }

    #[test]
    fn finish_rejects_mismatched_push_xyzm_ordinals() {
        let mut builder = CoordSeqBuilder::with_capacity(CoordinateAxes::XY, 1);
        builder.push_xyzm(0.0, 0.0, Some(1.0), None);
        let err = builder.finish().unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::Geometry(GeometryErrorKind::CoordinateAxesMismatch {
                declared: CoordinateAxes::XY,
                got: CoordinateAxes::XYZ,
            })
        ));
    }

    #[test]
    fn finish_validates_column_lengths() {
        let builder = CoordSeqBuilder::from_mismatched_xy_columns(vec![0.0], vec![0.0, 1.0]);
        let err = builder.finish().unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::Geometry(GeometryErrorKind::CoordinateLength(1, 2))
        ));
    }

    #[test]
    fn finish_seals_homogeneous_points() {
        let mut builder = CoordSeqBuilder::with_capacity(CoordinateAxes::XYZ, 2);
        builder.push(Point::new_raw_axes(
            0.0,
            0.0,
            ZOrdinate(Some(1.0)),
            MOrdinate(None),
        ));
        builder.push(Point::new_raw_axes(
            2.0,
            3.0,
            ZOrdinate(Some(4.0)),
            MOrdinate(None),
        ));
        let seq = builder.finish().expect("homogeneous XYZ sequence");
        assert_eq!(seq.len(), 2);
        assert_eq!(seq.axes(), CoordinateAxes::XYZ);
        assert_eq!(seq.zs().expect("Z column").to_vec(), [1.0, 4.0]);
    }

    #[test]
    fn empty_builder_finishes_empty_sequence() {
        let builder = CoordSeqBuilder::with_capacity(CoordinateAxes::XY, 0);
        let seq = builder.finish().unwrap();
        assert_eq!(seq.len(), 0);
        assert_eq!(seq.axes(), CoordinateAxes::XY);
    }

    #[rstest::rstest]
    #[case::coordinateaxes_xy(CoordinateAxes::XY, None, None)]
    #[case::coordinateaxes_xyz(CoordinateAxes::XYZ, Some(17.0), None)]
    #[case::coordinateaxes_xym(CoordinateAxes::XYM, None, Some(23.0))]
    #[case::coordinateaxes_xyzm(CoordinateAxes::XYZM, Some(17.0), Some(23.0))]
    fn coordinate_growth_preserves_every_active_column(
        #[case] axes: CoordinateAxes,
        #[case] z: Option<f64>,
        #[case] m: Option<f64>,
    ) {
        let mut builder = CoordSeqBuilder::with_capacity(axes, 0);
        for index in 0..32 {
            builder.push(Point::new_raw_axes(
                f64::from(index),
                5.0,
                ZOrdinate(z),
                MOrdinate(m),
            ));
        }
        let coords = builder.finish().unwrap();
        assert_eq!(coords.len(), 32);
        assert_eq!(coords.axes(), axes);
        for (index, point) in coords.iter().enumerate() {
            assert_eq!(point.x, index as f64);
            assert_eq!(point.y, 5.0);
            assert_eq!(point.z(), z);
            assert_eq!(point.m(), m);
        }
    }
}

#[cfg(test)]
mod coordseq_random_access_tests {
    use super::*;

    #[test]
    fn point_at_panics_at_the_visible_length() {
        let coords =
            CoordSeq::try_from_points(&[Point::new_raw_xy(3.0, 7.0)]).expect("one coordinate");
        let _panic = std::panic::catch_unwind(|| coords.point_at(coords.len()))
            .expect_err("point_at must panic at the visible length");
    }
}

#[cfg(test)]
mod coordseq_select_rows_tests {
    use super::*;

    fn sample_seq() -> CoordSeq {
        CoordSeq::try_from_vecs(
            vec![0.0, 1.0, 2.0, 3.0, 4.0],
            vec![10.0, 11.0, 12.0, 13.0, 14.0],
            None,
            None,
        )
        .unwrap()
    }

    #[test]
    fn select_rows_contiguous_view_shares_columns() {
        let seq = sample_seq();
        let parent = seq.column_arcs();
        let viewed = seq.select_rows(&[1, 2, 3]).unwrap();
        let child = viewed.column_arcs();
        assert!(Arc::ptr_eq(
            &parent.xs.owner().unwrap(),
            &child.xs.owner().unwrap()
        ));
        assert!(Arc::ptr_eq(
            &parent.ys.owner().unwrap(),
            &child.ys.owner().unwrap()
        ));
        assert_eq!(viewed.xs().to_vec(), [1.0, 2.0, 3.0]);
    }

    #[test]
    fn select_rows_scatter_copies_columns() {
        let seq = sample_seq();
        let parent = seq.column_arcs();
        let gathered = seq.select_rows(&[4, 0, 2]).unwrap();
        let child = gathered.column_arcs();
        assert!(!Arc::ptr_eq(
            &parent.xs.owner().unwrap(),
            &child.xs.owner().unwrap()
        ));
        assert_eq!(gathered.xs().to_vec(), [4.0, 0.0, 2.0]);
    }

    #[test]
    fn select_rows_scatter_preserves_duplicate_and_reversed_xyzm_order() {
        let seq = CoordSeq::try_from_vecs(
            vec![10.0, 20.0, 30.0, 40.0, 50.0],
            vec![11.0, 21.0, 31.0, 41.0, 51.0],
            Some(vec![12.0, 22.0, 32.0, 42.0, 52.0]),
            Some(vec![13.0, 23.0, 33.0, 43.0, 53.0]),
        )
        .unwrap();

        let gathered = seq.select_rows(&[4, 1, 4, 0]).unwrap();
        assert_eq!(gathered.axes(), CoordinateAxes::XYZM);
        assert_eq!(gathered.xs(), &[50.0, 20.0, 50.0, 10.0]);
        assert_eq!(gathered.ys(), &[51.0, 21.0, 51.0, 11.0]);
        assert_eq!(gathered.zs(), Some(&[52.0, 22.0, 52.0, 12.0][..]));
        assert_eq!(gathered.ms(), Some(&[53.0, 23.0, 53.0, 13.0][..]));

        let reversed = seq.select_rows(&[4, 3, 2, 1, 0]).unwrap();
        assert_eq!(reversed.xs(), &[50.0, 40.0, 30.0, 20.0, 10.0]);
        assert_eq!(reversed.ys(), &[51.0, 41.0, 31.0, 21.0, 11.0]);
        assert_eq!(reversed.zs(), Some(&[52.0, 42.0, 32.0, 22.0, 12.0][..]));
        assert_eq!(reversed.ms(), Some(&[53.0, 43.0, 33.0, 23.0, 13.0][..]));
    }

    #[test]
    fn contiguous_positive_slice_detects_step_one_windows() {
        assert_eq!(CoordSeq::contiguous_positive_slice(1, 4, 1), Some(1..4),);
        assert_eq!(CoordSeq::contiguous_positive_slice(0, 0, 1), Some(0..0));
        assert!(CoordSeq::contiguous_positive_slice(0, 5, -1).is_none());
        assert!(CoordSeq::contiguous_positive_slice(0, 5, 2).is_none());
    }
}

#[cfg(test)]
mod coordseq_xy_transform_carry_tests {
    use super::*;

    #[test]
    fn full_window_affine_carries_zm_unchanged_and_shares_arcs() {
        let seq = CoordSeq::try_from_vecs(
            vec![1.0, 2.0, 3.0],
            vec![4.0, 5.0, 6.0],
            Some(vec![9.0, 8.0, 7.0]),
            Some(vec![0.1, 0.2, 0.3]),
        )
        .unwrap();
        let mapped = seq
            .try_affine(&[0.0, 1.0, 1.0, 0.0, 0.0, 0.0])
            .expect("swap XY affine");
        assert_eq!(mapped.xs().to_vec(), [4.0, 5.0, 6.0]);
        assert_eq!(mapped.ys().to_vec(), [1.0, 2.0, 3.0]);
        assert_eq!(mapped.zs().expect("Z carried").to_vec(), [9.0, 8.0, 7.0]);
        assert_eq!(mapped.ms().expect("M carried").to_vec(), [0.1, 0.2, 0.3]);
    }

    #[test]
    fn windowed_affine_copies_zm_subrange() {
        let seq = CoordSeq::try_from_vecs(
            vec![1.0, 2.0, 3.0, 4.0],
            vec![5.0, 6.0, 7.0, 8.0],
            Some(vec![10.0, 11.0, 12.0, 13.0]),
            None,
        )
        .unwrap()
        .view(CoordWindow::from_range(1..3, 4));
        let mapped = seq.map_xy(|x, y| (x + 1.0, y + 1.0)).unwrap();
        assert_eq!(mapped.zs().expect("Z carried").to_vec(), [11.0, 12.0]);
    }
}

#[cfg(test)]
mod coordseq_m_storage_tests {

    use super::*;

    #[test]
    fn local_m_storage_copies_only_the_requested_visible_window() {
        let m = SharedSlice::from_vec(vec![50.0, 60.0]);
        let seq = CoordSeq::from_shared_columns_with_window(
            SharedSlice::from_vec(vec![0.0, 1.0, 2.0, 3.0]),
            SharedSlice::from_vec(vec![10.0, 11.0, 12.0, 13.0]),
            None,
            Some(MColumn::Local {
                owner: m.clone(),
                offset: 0,
            }),
            CoordWindow::from_range(1..3, 4),
        )
        .unwrap();
        assert_eq!(seq.ms(), Some([50.0, 60.0].as_slice()));
        assert_eq!(seq.column_arcs().m_parts().unwrap().1, 0..2);
        assert!(Arc::ptr_eq(
            &seq.column_arcs().m_parts().unwrap().0.owner().unwrap(),
            &m.owner().unwrap()
        ));

        let child = seq.view(CoordWindow::from_range(1..2, seq.len()));
        assert_eq!(child.ms(), Some([60.0].as_slice()));
        assert_eq!(child.column_arcs().m_parts().unwrap().1, 1..2);
        assert!(Arc::ptr_eq(
            &child.column_arcs().m_parts().unwrap().0.owner().unwrap(),
            &m.owner().unwrap()
        ));
    }

    #[test]
    fn shared_m_storage_keeps_the_parent_window() {
        let m = SharedSlice::from_vec(vec![50.0, 60.0, 70.0, 80.0]);
        let seq = CoordSeq::try_from_vecs(
            vec![0.0, 1.0, 2.0, 3.0],
            vec![10.0, 11.0, 12.0, 13.0],
            None,
            Some(m.as_slice().to_vec()),
        )
        .unwrap()
        .view(CoordWindow::from_range(1..3, 4));
        assert_eq!(seq.ms(), Some([60.0, 70.0].as_slice()));
        assert_eq!(seq.column_arcs().m_parts().unwrap().1, 1..3);
    }

    #[test]
    fn nested_middle_local_m_keeps_view_offsets_and_compacts_computed_output() {
        let nested = {
            let m = SharedSlice::from_vec((0..10).map(|value| f64::from(value) + 50.0).collect());
            let seq = CoordSeq::from_shared_columns_with_window(
                SharedSlice::from_vec((0..6).map(f64::from).collect()),
                SharedSlice::from_vec(vec![10.0; 6]),
                None,
                Some(MColumn::Local {
                    owner: m.clone(),
                    offset: 4,
                }),
                CoordWindow::from_range(2..5, 6),
            )
            .unwrap();
            assert_eq!(seq.xs(), &[2.0, 3.0, 4.0]);
            assert_eq!(seq.ms(), Some(&[54.0, 55.0, 56.0][..]));
            let columns = seq.column_arcs();
            assert_eq!(columns.window, 2..5);
            assert_eq!(columns.m_parts().unwrap().1, 4..7);
            assert_ne!(columns.window.start, columns.m_parts().unwrap().1.start);

            let nested = seq.view(CoordWindow::from_range(1..3, seq.len()));
            assert_eq!(nested.xs(), &[3.0, 4.0]);
            assert_eq!(nested.ms(), Some(&[55.0, 56.0][..]));
            let nested_columns = nested.column_arcs();
            assert_eq!(nested_columns.window, 3..5);
            assert_eq!(nested_columns.m_parts().unwrap().1, 5..7);

            let inner = nested.view(CoordWindow::from_range(1..2, nested.len()));
            assert_eq!(inner.xs(), &[4.0]);
            assert_eq!(inner.ms(), Some(&[56.0][..]));

            let mapped = nested.map_xy(|x, y| (x + 1.0, y + 1.0)).unwrap();
            assert_eq!(mapped.xs(), &[4.0, 5.0]);
            assert_eq!(mapped.ms(), Some(&[55.0, 56.0][..]));
            let columns = mapped.column_arcs();
            let (owner, window) = columns.m_parts().unwrap();
            assert_eq!(owner.len(), mapped.len());
            assert_eq!(window, 0..mapped.len());
            assert!(Arc::ptr_eq(
                &inner.column_arcs().m_parts().unwrap().0.owner().unwrap(),
                &m.owner().unwrap(),
            ));
            nested
        };

        assert_eq!(nested.axes(), CoordinateAxes::XYM);
        assert_eq!(nested.ms(), Some(&[55.0, 56.0][..]));
        assert_eq!(
            nested
                .iter()
                .map(|point| (point.x, point.y, point.axes, point.m))
                .collect::<Vec<_>>(),
            vec![
                (3.0, 10.0, CoordinateAxes::XYM, 55.0),
                (4.0, 10.0, CoordinateAxes::XYM, 56.0),
            ],
        );
    }

    #[test]
    fn local_m_admission_preserves_real_range_errors_and_exact_suffix_coverage() {
        let make = |owner: Vec<f64>, offset: usize, visible: usize| {
            CoordSeq::from_shared_columns_with_window(
                SharedSlice::from_vec(vec![1.0; visible]),
                SharedSlice::from_vec(vec![2.0; visible]),
                None,
                Some(MColumn::Local {
                    owner: SharedSlice::from_vec(owner),
                    offset,
                }),
                CoordWindow::from_range(0..visible, visible),
            )
        };

        let offset_past_owner =
            make(vec![50.0, 60.0], 3, 1).expect_err("an M offset past its owner is rejected");
        assert!(matches!(
            offset_past_owner.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::CoordinateRange {
                start: 3,
                end: 3,
                length: 2,
            })
        ));

        let visible_past_suffix = make(vec![50.0, 60.0, 70.0], 1, 3)
            .expect_err("M coverage must include the visible coordinate window");
        assert!(matches!(
            visible_past_suffix.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::CoordinateRange {
                start: 1,
                end: 4,
                length: 3,
            })
        ));

        let exact_suffix = make(vec![50.0, 60.0, 70.0], 1, 2)
            .expect("the visible M interval exactly covers the owner's suffix");
        assert_eq!(exact_suffix.ms(), Some(&[60.0, 70.0][..]));
    }

    #[test]
    fn visible_owner_keys_include_xy_windows_axes_and_local_m_offsets() {
        let xs = SharedSlice::from_vec(vec![0.0, 1.0, 2.0]);
        let ys = SharedSlice::from_vec(vec![10.0, 11.0, 12.0]);
        let ms = SharedSlice::from_vec(vec![20.0, 21.0, 22.0]);
        let visible = CoordWindow::from_range(0..2, xs.len());
        let make = |m_offset| {
            CoordSeq::from_shared_columns_with_window(
                xs.clone(),
                ys.clone(),
                None,
                Some(MColumn::Local {
                    owner: ms.clone(),
                    offset: m_offset,
                }),
                visible,
            )
            .unwrap()
        };
        let first = make(0);
        let clone = first.clone();
        let different_m = make(1);
        let different_xy = CoordSeq::from_shared_columns_with_window(
            xs.clone(),
            ys.clone(),
            None,
            Some(MColumn::Local {
                owner: ms.clone(),
                offset: 1,
            }),
            CoordWindow::from_range(1..3, xs.len()),
        )
        .unwrap();

        assert_eq!(first.shared_xy_key(), clone.shared_xy_key());
        assert_eq!(first.shared_point_run_key(), clone.shared_point_run_key());
        assert_eq!(first.shared_xy_key(), different_m.shared_xy_key());
        assert_ne!(
            first.shared_point_run_key(),
            different_m.shared_point_run_key(),
            "the visible M slice carries its independent local offset",
        );
        assert_ne!(
            first.shared_xy_key(),
            different_xy.shared_xy_key(),
            "the visible XY start is part of the key",
        );

        let measured = Shape::geometry_collection(vec![
            Shape::LineString(LineSeq::from_proven(first)),
            Shape::LineString(LineSeq::from_proven(different_m)),
        ])
        .unwrap()
        .unique_points()
        .unwrap();
        let measured_points = measured.points_vec().unwrap();
        assert_eq!(measured_points.len(), 4);
        assert_eq!(
            measured_points
                .iter()
                .map(|point| point.m().unwrap())
                .collect::<Vec<_>>(),
            [20.0, 21.0, 21.0, 22.0]
        );
    }

    #[test]
    fn full_m_carry_shares_while_middle_m_output_compacts() {
        use crate::test_support::allocation_count;

        let seq = CoordSeq::try_from_vecs(
            vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0],
            vec![10.0; 6],
            None,
            Some(vec![50.0, 51.0, 52.0, 53.0, 54.0, 55.0]),
        )
        .unwrap();
        let source_m = seq.column_arcs().m_parts().unwrap().0.owner().unwrap();
        let (carried, allocations) = allocation_count(|| seq.carried_ms());
        assert_eq!(allocations, 0);
        let MColumn::Shared(carried) = carried.unwrap() else {
            panic!("a full shared M column stays shared");
        };
        assert!(Arc::ptr_eq(&carried.owner().unwrap(), &source_m));
        let seq = seq.view(CoordWindow::from_range(1..4, 6));
        assert_eq!(seq.ms(), Some(&[51.0, 52.0, 53.0][..]));

        let mapped = seq.map_xy(|x, y| (x + 1.0, y)).unwrap();
        assert_eq!(mapped.xs(), &[2.0, 3.0, 4.0]);
        assert_eq!(mapped.ms(), Some(&[51.0, 52.0, 53.0][..]));
        assert!(!Arc::ptr_eq(
            &mapped.column_arcs().m_parts().unwrap().0.owner().unwrap(),
            &source_m,
        ));
        let columns = mapped.column_arcs();
        let (owner, window) = columns.m_parts().unwrap();
        assert_eq!(owner.len(), mapped.len());
        assert_eq!(window, 0..mapped.len());
    }
}

#[cfg(test)]
mod point_gather_tests {
    use super::*;

    fn repeated_xy_line() -> Shape {
        Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(0.0, 0.0),
                    Point::new_raw_xy(1.0, 0.0),
                    Point::new_raw_xy(1.0, 0.0),
                    Point::new_raw_xy(0.0, 0.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        )
    }

    #[test]
    fn collect_points_into_preserves_prefix_order_and_repeated_vertices() {
        let shape = repeated_xy_line();
        let mut out = vec![Point::new_raw_xy(-1.0, -1.0)];
        shape.collect_points_into(&mut out);
        assert_eq!(out, [
            Point::new_raw_xy(-1.0, -1.0),
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(1.0, 0.0),
            Point::new_raw_xy(1.0, 0.0),
            Point::new_raw_xy(0.0, 0.0),
        ]);
    }

    #[test]
    fn unique_points_preserves_first_seen_xyzm_identity() {
        use crate::geometry::{MOrdinate, ZOrdinate};

        let a = Point::new_raw_axes(4.0, 2.0, ZOrdinate(Some(7.0)), MOrdinate(Some(11.0)));
        let b = Point::new_raw_axes(1.0, 3.0, ZOrdinate(Some(5.0)), MOrdinate(Some(13.0)));
        let c = Point::new_raw_axes(4.0, 2.0, ZOrdinate(Some(17.0)), MOrdinate(Some(11.0)));
        let d = Point::new_raw_axes(4.0, 2.0, ZOrdinate(Some(7.0)), MOrdinate(Some(19.0)));
        let shape = Shape::multi_point(
            CoordinateAxes::XYZM,
            CoordSeq::try_from_points(&[a, b, a, c, b, d, c]).unwrap(),
        );
        let unique = shape.unique_points().unwrap();
        assert_eq!(unique.axes(), CoordinateAxes::XYZM);
        assert_eq!(unique.points_vec().unwrap(), [a, b, c, d]);
    }
}

#[cfg(test)]
mod shape_heap_memo_tests {
    use super::*;

    fn point(x: f64) -> Shape {
        Shape::Point(Point::new_raw_xy(x, 0.0))
    }

    fn empty_point() -> Shape {
        Shape::Empty(GeometryKind::Point, CoordinateAxes::XY)
    }

    fn collection(members: Vec<Shape>) -> Shape {
        Shape::geometry_collection(members).expect("nonempty test collection")
    }

    fn heap_bytes(shape: &Shape) -> usize {
        crate::HeapSize::heap_bytes(shape)
    }

    #[test]
    fn unique_collection_size_is_allocation_free_and_shared_dag_counts_each_occurrence() {
        let point_bytes = heap_bytes(&point(0.0));
        let empty_pair_bytes = heap_bytes(&collection(vec![empty_point(), empty_point()]));
        assert!(point_bytes > 0);
        assert!(empty_pair_bytes > 0);
        let two_point_bytes = empty_pair_bytes.saturating_add(point_bytes.saturating_mul(2));

        let single = collection(vec![point(0.0), point(1.0)]);
        let (single_bytes, single_allocations) = allocation_count(|| heap_bytes(&single));
        assert_eq!(single_bytes, two_point_bytes);
        assert_eq!(single_allocations, 0);

        let unique_children: Vec<_> = (0..5_000)
            .map(|index| collection(vec![point(f64::from(index)), point(f64::from(index) + 1.0)]))
            .collect();
        let unique = collection(unique_children);
        let Shape::GeometryCollection(unique_members) = &unique else {
            unreachable!("collection constructor returns a collection");
        };
        assert!(!unique_members.members_are_shared());
        assert!(unique_members.iter().all(|member| {
            let Shape::GeometryCollection(child) = member else {
                return false;
            };
            !child.members_are_shared()
        }));

        let empty_root = collection(vec![empty_point(); 5_000]);
        let expected_unique_bytes =
            heap_bytes(&empty_root).saturating_add(two_point_bytes.saturating_mul(5_000));
        let (unique_bytes, unique_allocations) = allocation_count(|| heap_bytes(&unique));
        assert_eq!(unique_bytes, expected_unique_bytes);
        assert_eq!(unique_allocations, 0);

        let mut shared = point(0.0);
        for _ in 0..20 {
            shared = collection(vec![shared.clone(), shared.clone()]);
        }
        let Shape::GeometryCollection(root) = &shared else {
            unreachable!("depth-20 fixture ends in a collection");
        };
        assert!(!root.members_are_shared());
        let Shape::GeometryCollection(shared_child) = root
            .as_slice()
            .first()
            .expect("collection has two repeated children")
        else {
            unreachable!("depth-20 fixture child is a collection");
        };
        assert!(shared_child.members_are_shared());

        let expected_shared_bytes = (0..20).fold(point_bytes, |bytes, _| {
            empty_pair_bytes.saturating_add(bytes.saturating_mul(2))
        });
        assert_eq!(heap_bytes(&shared), expected_shared_bytes);
    }
}

#[cfg(test)]
mod shape_count_query_tests {
    use super::*;
    use crate::error::{ErrorKind, ResourceError};

    fn double(shape: Shape, depth: usize) -> Shape {
        (0..depth).fold(shape, |child, _| {
            Shape::geometry_collection(vec![child.clone(), child])
                .expect("shared child stays within collection depth")
        })
    }

    fn square_ring(offset: f64) -> Ring {
        Ring::try_from_closed(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(offset, 0.0),
                Point::new_raw_xy(offset + 1.0, 0.0),
                Point::new_raw_xy(offset + 1.0, 1.0),
                Point::new_raw_xy(offset, 0.0),
            ])
            .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn shared_collection_counts_multiply_every_logical_member() {
        let point = Shape::Point(Point::new_raw_xy(0.0, 0.0));
        let points = double(point, 10);
        let mut counts = ShapeCountQuery::default();
        assert_eq!(counts.coordinates(&points).unwrap(), 1_024);
        assert_eq!(counts.segments(&points).unwrap(), 0);
        assert!(counts.coordinates_at_most(&points, 1_024));
        assert!(!counts.coordinates_at_most(&points, 1_023));

        let line = Shape::LineString(
            LineSeq::segment(Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(1.0, 0.0)).unwrap(),
        );
        let lines = double(line, 10);
        assert_eq!(counts.coordinates(&lines).unwrap(), 2_048);
        assert_eq!(counts.segments(&lines).unwrap(), 1_024);
        assert!(counts.has_segments(&lines));
    }

    #[test]
    fn shared_multipart_and_polygon_owners_keep_multiplicity() {
        let line =
            LineSeq::segment(Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(1.0, 0.0)).unwrap();
        let multiline = MultiLineString::try_new(CoordinateAxes::XY, vec![
            MultiMember::Empty,
            MultiMember::Value(line),
            MultiMember::Empty,
        ])
        .unwrap();
        let repeated_multiline = Shape::geometry_collection(vec![
            Shape::MultiLineString(multiline.clone()),
            Shape::MultiLineString(multiline),
        ])
        .unwrap();

        let polygon = Polygon::new(square_ring(0.0), vec![square_ring(2.0)]);
        let repeated_polygon = Shape::geometry_collection(vec![
            Shape::Polygon(polygon.clone()),
            Shape::Polygon(polygon.clone()),
        ])
        .unwrap();
        let multi_polygon = MultiPolygon::try_new(CoordinateAxes::XY, vec![
            MultiMember::Empty,
            MultiMember::Value(polygon),
            MultiMember::Empty,
        ])
        .unwrap();
        let repeated_multi_polygon = Shape::geometry_collection(vec![
            Shape::MultiPolygon(multi_polygon.clone()),
            Shape::MultiPolygon(multi_polygon),
        ])
        .unwrap();

        let mut counts = ShapeCountQuery::default();
        assert_eq!(counts.coordinates(&repeated_multiline).unwrap(), 4);
        assert_eq!(counts.segments(&repeated_multiline).unwrap(), 2);
        assert_eq!(counts.coordinates(&repeated_polygon).unwrap(), 16);
        assert_eq!(counts.segments(&repeated_polygon).unwrap(), 12);
        assert_eq!(counts.coordinates(&repeated_multi_polygon).unwrap(), 16);
        assert_eq!(counts.segments(&repeated_multi_polygon).unwrap(), 12);
    }

    #[test]
    fn coordinate_overflow_does_not_overflow_the_independent_segment_count() {
        let points = double(Shape::Point(Point::new_raw_xy(0.0, 0.0)), MAX_NESTING);
        let mut counts = ShapeCountQuery::default();
        assert!(matches!(
            counts.coordinates(&points).unwrap_err().kind(),
            ErrorKind::Resource(ResourceError::SizeOverflow)
        ));
        assert!(matches!(
            points.points_vec().unwrap_err().kind(),
            ErrorKind::Resource(ResourceError::SizeOverflow)
        ));
        assert_eq!(counts.segments(&points).unwrap(), 0);
        assert!(!counts.coordinates_at_most(&points, usize::MAX));
        assert!(!counts.has_segments(&points));
    }

    #[test]
    fn ordinary_scalar_count_query_allocates_nothing() {
        let point = Shape::Point(Point::new_raw_xy(1.0, 2.0));
        let (count, allocations) =
            allocation_count(|| ShapeCountQuery::default().coordinates(&point).unwrap());
        assert_eq!(count, 1);
        assert_eq!(allocations, 0);
    }
}
