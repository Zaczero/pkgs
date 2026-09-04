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

    impl From<Vec<Point>> for MultiPoint {
        fn from(points: Vec<Point>) -> Self {
            CoordSeq::from_points(&points).into()
        }
    }

    impl From<Vec<XY>> for MultiPoint {
        fn from(points: Vec<XY>) -> Self {
            CoordSeq::from_xy(&points).into()
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
        let multipoint = MultiPoint::from_coordseq(CoordSeq::from_points(&[
            Point::new_raw_xy(1.0, 2.0),
            Point::new_raw_xy(3.0, 4.0),
        ]))
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

        let (issue, allocations) = allocation_count(|| shape.validate());
        assert_eq!(issue, None);
        assert_eq!(allocations, 0);
    }

    #[test]
    fn borrowed_iteration_short_circuits_and_owned_collection_remains_available() {
        let multipoint = MultiPoint::from_coordseq(CoordSeq::from_points(&[
            Point::new_raw_xy(1.0, 2.0),
            Point::new_raw_xy(3.0, 4.0),
            Point::new_raw_xy(5.0, 6.0),
        ]))
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
        assert_eq!(column.as_arc_i32().as_ref(), &[0, 2, 5]);
    }

    #[test]
    fn csr_offset_builder_finish_matches_try_new() {
        let mut builder = CsrOffsetBuilder::new();
        builder.push_end(3, 10).expect("in-range end");
        builder.push_end(7, 10).expect("in-range end");
        let built = builder.finish(7).expect("finish");
        let direct = CsrOffsetColumn::<()>::try_new(vec![0, 3, 7], 7).expect("direct");
        assert_eq!(built.as_arc_i32().as_ref(), direct.as_arc_i32().as_ref());
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
    fn csr_offset_rebase_concat_matches_checked_add() {
        let prefix = &[0, 3, 7];
        let tail = &[0, 2, 5];
        let column = CsrOffsetColumn::<()>::rebase_concat(prefix, tail, 12).expect("rebase");
        assert_eq!(column.as_arc_i32().as_ref(), &[0, 3, 7, 9, 12]);
    }

    #[test]
    fn csr_offset_try_from_arc_i32_roundtrips_as_arc_i32() {
        let column = CsrOffsetColumn::<()>::try_new(vec![0, 3, 7], 7).expect("valid");
        let arc = column.as_arc_i32();
        let roundtrip = CsrOffsetColumn::<()>::try_from_arc_i32(arc, 7).expect("roundtrip");
        assert_eq!(roundtrip.as_slice(), &[0, 3, 7]);
    }

    #[test]
    fn csr_offset_try_from_arc_i32_rejects_negative_as_malformed() {
        use crate::error::ErrorKind;

        let error = CsrOffsetColumn::<()>::try_from_arc_i32(Arc::from([0, -1, 3]), 3)
            .expect_err("negative offset");
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::MalformedCsrOffsets)
        ));
    }

    #[test]
    fn csr_offset_rebase_concat_polygon_ring_level() {
        let left_rings = &[0, 4, 8];
        let right_rings = &[0, 3];
        let rings =
            CsrOffsetColumn::<()>::rebase_concat(left_rings, right_rings, 11).expect("rings");
        assert_eq!(rings.as_slice(), &[0, 4, 8, 11]);
        let left_polygons = &[0, 2];
        let right_polygons = &[0, 1];
        let polygons = CsrOffsetColumn::<()>::rebase_concat(left_polygons, right_polygons, 3)
            .expect("polygons");
        assert_eq!(polygons.as_slice(), &[0, 2, 3]);
    }

    #[test]
    fn csr_offset_rebase_concat_validated_matches_rebase_concat() {
        let cases: &[(&[i32], &[i32], usize)] = &[
            (&[0, 3, 7], &[0, 2, 5], 12),
            (&[0, 4, 8], &[0, 3], 11),
            (&[0, 2], &[0, 1], 3),
        ];
        for &(prefix, tail, end_cap) in cases {
            let prefix = CsrOffsetColumn::<()>::try_new(
                prefix.iter().map(|&offset| offset as usize).collect(),
                end_cap,
            )
            .expect("valid prefix");
            let tail = CsrOffsetColumn::<()>::try_new(
                tail.iter().map(|&offset| offset as usize).collect(),
                end_cap,
            )
            .expect("valid tail");
            let trusted = CsrOffsetColumn::<()>::rebase_concat_validated(&prefix, &tail, end_cap)
                .expect("trusted");
            let checked =
                CsrOffsetColumn::<()>::rebase_concat(prefix.as_slice(), tail.as_slice(), end_cap)
                    .expect("checked");
            assert_eq!(trusted.as_slice(), checked.as_slice());
        }
    }

    /// Valid near-`i32::MAX` slices must reject a rebased end that cannot be
    /// represented by the output offsets.
    #[test]
    fn csr_offset_rebase_concat_rejects_overflow() {
        let err = CsrOffsetColumn::<()>::rebase_concat(&[0, 1], &[0, i32::MAX], usize::MAX)
            .expect_err("rebased tail must exceed i32 capacity");
        assert!(matches!(
            err.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::OffsetCapacityExceeded)
        ));
    }

    /// Negative raw offsets must error before rebase arithmetic.
    #[test]
    fn csr_offset_rebase_concat_rejects_negative_offsets() {
        let err = CsrOffsetColumn::<()>::rebase_concat(&[0, 1], &[0, -1], usize::MAX)
            .expect_err("negative tail offset must error");
        assert!(matches!(
            err.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::MalformedCsrOffsets)
        ));
    }

    /// Non-monotonic raw ends must error rather than produce a false CSR.
    #[test]
    fn csr_offset_rebase_concat_rejects_non_monotonic_offsets() {
        let err = CsrOffsetColumn::<()>::rebase_concat(&[0, 5, 3], &[0], 100)
            .expect_err("non-monotonic prefix must error");
        assert!(matches!(
            err.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::MalformedCsrOffsets)
        ));
        let err = CsrOffsetColumn::<()>::rebase_concat(&[0, 5], &[0, 3, 1], 100)
            .expect_err("non-monotonic tail must error");
        assert!(matches!(
            err.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::MalformedCsrOffsets)
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
}

#[cfg(test)]
mod coordseq_select_rows_tests {
    use super::*;

    fn sample_seq() -> CoordSeq {
        CoordSeq::from_columns(
            [0.0, 1.0, 2.0, 3.0, 4.0].into(),
            [10.0, 11.0, 12.0, 13.0, 14.0].into(),
            None,
            None,
        )
    }

    #[test]
    fn select_rows_contiguous_view_shares_columns() {
        let seq = sample_seq();
        let parent = seq.column_arcs();
        let viewed = seq.select_rows(&[1, 2, 3]);
        let child = viewed.column_arcs();
        assert!(Arc::ptr_eq(&parent.xs, &child.xs));
        assert!(Arc::ptr_eq(&parent.ys, &child.ys));
        assert_eq!(viewed.xs().to_vec(), [1.0, 2.0, 3.0]);
    }

    #[test]
    fn select_rows_scatter_copies_columns() {
        let seq = sample_seq();
        let parent = seq.column_arcs();
        let gathered = seq.select_rows(&[4, 0, 2]);
        let child = gathered.column_arcs();
        assert!(!Arc::ptr_eq(&parent.xs, &child.xs));
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
    use std::sync::Arc;

    use super::*;

    #[test]
    fn full_window_affine_carries_zm_unchanged_and_shares_arcs() {
        let seq = CoordSeq::from_columns(
            [1.0, 2.0, 3.0].into(),
            [4.0, 5.0, 6.0].into(),
            Some([9.0, 8.0, 7.0].into()),
            Some([0.1, 0.2, 0.3].into()),
        );
        let parent_z = seq.column_arcs().zs.expect("Z column");
        let parent_m = seq.column_arcs().m_parts().expect("M column").0;
        let mapped = seq
            .try_affine(&[0.0, 1.0, 1.0, 0.0, 0.0, 0.0])
            .expect("swap XY affine");
        assert_eq!(mapped.xs().to_vec(), [4.0, 5.0, 6.0]);
        assert_eq!(mapped.ys().to_vec(), [1.0, 2.0, 3.0]);
        assert_eq!(mapped.zs().expect("Z carried").to_vec(), [9.0, 8.0, 7.0]);
        assert_eq!(mapped.ms().expect("M carried").to_vec(), [0.1, 0.2, 0.3]);
        let child = mapped.column_arcs();
        assert!(Arc::ptr_eq(
            &parent_z,
            child.zs.as_ref().expect("shared Z arc")
        ));
        assert!(Arc::ptr_eq(
            &parent_m,
            &child.m_parts().expect("shared M arc").0
        ));
    }

    #[test]
    fn windowed_affine_copies_zm_subrange() {
        let seq = CoordSeq::from_columns(
            [1.0, 2.0, 3.0, 4.0].into(),
            [5.0, 6.0, 7.0, 8.0].into(),
            Some([10.0, 11.0, 12.0, 13.0].into()),
            None,
        )
        .view(CoordWindow::from_range(1..3, 4));
        let parent_z = seq.column_arcs().zs.expect("Z column");
        let mapped = seq.map_xy(|x, y| (x + 1.0, y + 1.0));
        assert_eq!(mapped.zs().expect("Z carried").to_vec(), [11.0, 12.0]);
        let child_z = mapped.column_arcs().zs.expect("Z column");
        assert!(!Arc::ptr_eq(&parent_z, &child_z));
    }
}

#[cfg(test)]
mod coordseq_m_storage_tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn local_m_storage_copies_only_the_requested_visible_window() {
        let m: Arc<[f64]> = [50.0, 60.0].into();
        let seq = CoordSeq::from_columns_with_window(
            [0.0, 1.0, 2.0, 3.0].into(),
            [10.0, 11.0, 12.0, 13.0].into(),
            None,
            Some(Arc::clone(&m)),
            CoordWindow::from_range(1..3, 4),
        );
        assert_eq!(seq.ms(), Some([50.0, 60.0].as_slice()));
        assert_eq!(seq.column_arcs().m_parts().unwrap().1, 0..2);
        assert!(Arc::ptr_eq(&seq.column_arcs().m_parts().unwrap().0, &m));

        let child = seq.view(CoordWindow::from_range(1..2, seq.len()));
        assert_eq!(child.ms(), Some([60.0].as_slice()));
        assert_eq!(child.column_arcs().m_parts().unwrap().1, 0..1);
        assert!(!Arc::ptr_eq(&child.column_arcs().m_parts().unwrap().0, &m));
    }

    #[test]
    fn shared_m_storage_keeps_the_parent_window() {
        let m: Arc<[f64]> = [50.0, 60.0, 70.0, 80.0].into();
        let seq = CoordSeq::from_columns(
            [0.0, 1.0, 2.0, 3.0].into(),
            [10.0, 11.0, 12.0, 13.0].into(),
            None,
            Some(Arc::clone(&m)),
        )
        .view(CoordWindow::from_range(1..3, 4));
        assert_eq!(seq.ms(), Some([60.0, 70.0].as_slice()));
        assert_eq!(seq.column_arcs().m_parts().unwrap().1, 1..3);
        assert!(Arc::ptr_eq(&seq.column_arcs().m_parts().unwrap().0, &m));
    }
}

#[cfg(test)]
mod size_probe_tests {
    use super::*;

    /// Documents the engine's per-segment memory footprint (the density
    /// lever evidence): a `Segment` carries two full `Point`s.
    #[test]
    fn record_engine_type_sizes() {
        println!("Point = {}", std::mem::size_of::<Point>());
        println!("Segment = {}", std::mem::size_of::<Segment>());
        println!(
            "SweepEntry-equivalent = {}",
            std::mem::size_of::<[f64; 4]>() + 8
        );
    }
}
