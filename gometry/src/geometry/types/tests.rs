use std::sync::Arc;

use crate::geometry::types::*;
use crate::test_support::allocation_count;

// Test fixtures only. These panic on an empty payload, which is exactly what a
// test wants and exactly what production must not have: gating them on
// `cfg(test)` means a production caller reaching for `.into()` fails to
// compile and is pushed to `Shape::multi_point`/`narrow_points`, which admit
// the empty case as `Shape::Empty`.
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
            Self::from_dense(lines)
                .expect("test multilinestring allocation")
                .expect("test multilinestring payload is nonempty")
        }
    }

    impl FromIterator<LineSeq> for MultiLineString {
        fn from_iter<T: IntoIterator<Item = LineSeq>>(iter: T) -> Self {
            iter.into_iter().collect::<Vec<_>>().into()
        }
    }

    impl From<Vec<Polygon>> for MultiPolygon {
        fn from(polygons: Vec<Polygon>) -> Self {
            Self::from_dense(polygons)
                .expect("test multipolygon allocation")
                .expect("test multipolygon payload is nonempty")
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
    fn borrowed_iteration_preserves_each_coordinate_layout() {
        for (axes, point) in [
            (CoordinateAxes::XY, point(1.0, 2.0, None, None)),
            (CoordinateAxes::XYZ, point(1.0, 2.0, Some(3.0), None)),
            (CoordinateAxes::XYM, point(1.0, 2.0, None, Some(4.0))),
            (CoordinateAxes::XYZM, point(1.0, 2.0, Some(3.0), Some(4.0))),
        ] {
            let multipoint =
                MultiPoint::try_new(axes, vec![MultiMember::Value(point)]).expect("matching axes");
            assert_eq!((&multipoint).into_iter().next(), Some(point));
        }
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
        for (end, cap) in [(7, 6), (0x8000_0000, 0x8000_0000)] {
            let mut builder = CsrOffsetBuilder::new();
            builder.push_end(end, end).unwrap();
            let error = builder.finish(cap).unwrap_err();
            assert!(matches!(
                error.kind(),
                crate::error::ErrorKind::Geometry(GeometryErrorKind::OffsetCapacityExceeded)
            ));
        }
    }

    #[test]
    fn csr_offset_rejects_overflow_at_freeze() {
        let overflow = (i32::MAX as usize).saturating_add(1);
        let _ = CsrOffsetColumn::<()>::try_new(vec![0, overflow], overflow).unwrap_err();
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
    fn csr_offset_try_from_shared_i32_rejects_negative_as_malformed() {
        use crate::error::ErrorKind;

        let error = CsrOffsetColumn::<()>::try_from_shared_i32(
            crate::geometry::SharedSlice::from_vec(vec![0, -1, 3]),
            3,
        )
        .expect_err("negative offset");
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::MalformedCsrOffsets)
        ));
    }
}

#[cfg(test)]
mod coordseq_builder_tests {
    use super::*;
    use crate::error::{ErrorKind, ResourceError};

    #[test]
    fn finish_rejects_mismatched_point_axes() {
        let mut builder = CoordSeqBuilder::try_with_capacity(CoordinateAxes::XY, 2).unwrap();
        builder.push(Point::new_raw_xy(0.0, 0.0)).unwrap();
        builder
            .push(Point::new_raw_axes(
                1.0,
                1.0,
                ZOrdinate(Some(0.0)),
                MOrdinate(None),
            ))
            .unwrap();
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
        let mut builder = CoordSeqBuilder::try_with_capacity(CoordinateAxes::XY, 1).unwrap();
        builder.push_at(&source, 0).unwrap();
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
    fn coordinate_axes_from_bits_accepts_only_canonical_layouts() {
        assert_eq!(CoordinateAxes::from_bits(0), Some(CoordinateAxes::XY));
        assert_eq!(CoordinateAxes::from_bits(1), Some(CoordinateAxes::XYZ));
        assert_eq!(CoordinateAxes::from_bits(2), Some(CoordinateAxes::XYM));
        assert_eq!(CoordinateAxes::from_bits(3), Some(CoordinateAxes::XYZM));
        assert_eq!(CoordinateAxes::from_bits(4), None);
    }

    #[test]
    fn finish_rejects_mismatched_push_xyzm_ordinals() {
        let mut builder = CoordSeqBuilder::try_with_capacity(CoordinateAxes::XY, 1).unwrap();
        builder.push_xyzm(0.0, 0.0, Some(1.0), None).unwrap();
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
        let mut builder = CoordSeqBuilder::try_with_capacity(CoordinateAxes::XYZ, 2).unwrap();
        builder
            .push(Point::new_raw_axes(
                0.0,
                0.0,
                ZOrdinate(Some(1.0)),
                MOrdinate(None),
            ))
            .unwrap();
        builder
            .push(Point::new_raw_axes(
                2.0,
                3.0,
                ZOrdinate(Some(4.0)),
                MOrdinate(None),
            ))
            .unwrap();
        let seq = builder.finish().expect("homogeneous XYZ sequence");
        assert_eq!(seq.len(), 2);
        assert_eq!(seq.axes(), CoordinateAxes::XYZ);
        assert_eq!(seq.zs().expect("Z column").to_vec(), [1.0, 4.0]);
    }

    #[test]
    fn empty_builder_finishes_empty_sequence() {
        let builder = CoordSeqBuilder::try_with_capacity(CoordinateAxes::XY, 0).unwrap();
        let seq = builder.finish().unwrap();
        assert_eq!(seq.len(), 0);
        assert_eq!(seq.axes(), CoordinateAxes::XY);
    }

    #[test]
    fn coordinate_capacity_rejects_unrepresentable_layouts() {
        for axes in [
            CoordinateAxes::XY,
            CoordinateAxes::XYZ,
            CoordinateAxes::XYM,
            CoordinateAxes::XYZM,
        ] {
            let Err(error) = CoordSeqBuilder::try_with_capacity(axes, usize::MAX) else {
                panic!("unrepresentable coordinate layout was accepted");
            };
            assert!(matches!(
                error.kind(),
                ErrorKind::Resource(ResourceError::SizeOverflow)
            ));
        }
    }

    #[test]
    fn coordinate_growth_preserves_every_active_column() {
        for (axes, z, m) in [
            (CoordinateAxes::XY, None, None),
            (CoordinateAxes::XYZ, Some(17.0), None),
            (CoordinateAxes::XYM, None, Some(23.0)),
            (CoordinateAxes::XYZM, Some(17.0), Some(23.0)),
        ] {
            let mut builder = CoordSeqBuilder::try_with_capacity(axes, 0).unwrap();
            for index in 0..32 {
                builder
                    .push(Point::new_raw_axes(
                        f64::from(index),
                        5.0,
                        ZOrdinate(z),
                        MOrdinate(m),
                    ))
                    .unwrap();
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
    use std::sync::Arc;

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
    fn nested_middle_local_m_keeps_distinct_offset_and_carries_without_allocation() {
        use crate::test_support::allocation_count;

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

        let (carried, allocations) = allocation_count(|| nested.carried_ms().unwrap());
        assert_eq!(allocations, 0);

        let mapped = nested.map_xy(|x, y| (x + 1.0, y + 1.0)).unwrap();
        assert_eq!(mapped.xs(), &[4.0, 5.0]);
        assert_eq!(mapped.ms(), Some(&[55.0, 56.0][..]));
        assert!(Arc::ptr_eq(
            &mapped.column_arcs().m_parts().unwrap().0.owner().unwrap(),
            &m.owner().unwrap(),
        ));
        let _ = carried;
    }

    #[test]
    fn shared_middle_m_carry_reuses_owner_as_local_without_allocation() {
        use crate::test_support::allocation_count;

        let seq = CoordSeq::try_from_vecs(
            vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0],
            vec![10.0; 6],
            None,
            Some(vec![50.0, 51.0, 52.0, 53.0, 54.0, 55.0]),
        )
        .unwrap()
        .view(CoordWindow::from_range(1..4, 6));
        let source_m = seq.column_arcs().m_parts().unwrap().0.owner().unwrap();
        assert_eq!(seq.ms(), Some(&[51.0, 52.0, 53.0][..]));
        let (carried, allocations) = allocation_count(|| seq.carried_ms().unwrap());
        assert_eq!(allocations, 0);

        let mapped = seq.map_xy(|x, y| (x + 1.0, y)).unwrap();
        assert_eq!(mapped.xs(), &[2.0, 3.0, 4.0]);
        assert_eq!(mapped.ms(), Some(&[51.0, 52.0, 53.0][..]));
        assert!(Arc::ptr_eq(
            &mapped.column_arcs().m_parts().unwrap().0.owner().unwrap(),
            &source_m,
        ));
        let _ = carried;
    }
}

#[cfg(test)]
mod point_gather_tests {
    use super::*;
    use crate::geometry::{LineSeq, Shape};

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
        shape.collect_points_into(&mut out).unwrap();
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
