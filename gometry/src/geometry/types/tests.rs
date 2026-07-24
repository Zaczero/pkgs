use std::sync::Arc;

use super::*;

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
    fn csr_offset_rebase_concat_trusted_matches_rebase_concat() {
        let cases: &[(&[i32], &[i32], usize)] = &[
            (&[0, 3, 7], &[0, 2, 5], 12),
            (&[0, 4, 8], &[0, 3], 11),
            (&[0, 2], &[0, 1], 3),
        ];
        for &(prefix, tail, end_cap) in cases {
            let trusted = CsrOffsetColumn::<()>::rebase_concat_trusted(prefix, tail, end_cap)
                .expect("trusted");
            let checked =
                CsrOffsetColumn::<()>::rebase_concat(prefix, tail, end_cap).expect("checked");
            assert_eq!(trusted.as_slice(), checked.as_slice());
        }
    }
}

#[cfg(test)]
mod coordseq_builder_tests {
    use super::*;
    use crate::error::ErrorKind;

    #[test]
    fn finish_rejects_mismatched_point_axes() {
        let mut builder = CoordSeqBuilder::with_capacity(CoordinateAxes::XY, 2);
        builder.push(Point::new_unchecked_xy(0.0, 0.0));
        builder.push(Point::new_unchecked_axes(
            1.0,
            1.0,
            ZOrdinate(Some(0.0)),
            MOrdinate(None),
        ));
        let err = builder.finish().unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::Geometry(GeometryErrorKind::CoordinateAxesMismatch)
        ));
    }

    #[test]
    fn try_from_points_rejects_mixed_axes_without_panicking() {
        let points = [
            Point::new_unchecked_xy(0.0, 0.0),
            Point::new_unchecked_axes(1.0, 1.0, ZOrdinate(Some(2.0)), MOrdinate(None)),
        ];
        let err = CoordSeq::try_from_points(&points).unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::Geometry(GeometryErrorKind::CoordinateAxesMismatch)
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
            ErrorKind::Geometry(GeometryErrorKind::CoordinateAxesMismatch)
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
        builder.push(Point::new_unchecked_axes(
            0.0,
            0.0,
            ZOrdinate(Some(1.0)),
            MOrdinate(None),
        ));
        builder.push(Point::new_unchecked_axes(
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
        let parent_m = seq.column_arcs().ms.expect("M column");
        let mapped = seq
            .try_affine(&[0.0, 1.0, 1.0, 0.0, 0.0, 0.0])
            .expect("swap XY affine");
        assert_eq!(mapped.xs().to_vec(), [4.0, 5.0, 6.0]);
        assert_eq!(mapped.ys().to_vec(), [1.0, 2.0, 3.0]);
        assert_eq!(mapped.zs().expect("Z carried").to_vec(), [9.0, 8.0, 7.0]);
        assert_eq!(mapped.ms().expect("M carried").to_vec(), [0.1, 0.2, 0.3]);
        let child = mapped.column_arcs();
        assert!(Arc::ptr_eq(&parent_z, &child.zs.expect("shared Z arc")));
        assert!(Arc::ptr_eq(&parent_m, &child.ms.expect("shared M arc")));
    }

    #[test]
    fn windowed_affine_copies_zm_subrange() {
        let seq = CoordSeq::from_columns(
            [1.0, 2.0, 3.0, 4.0].into(),
            [5.0, 6.0, 7.0, 8.0].into(),
            Some([10.0, 11.0, 12.0, 13.0].into()),
            None,
        )
        .view(CoordWindow::trusted(1..3, 4));
        let parent_z = seq.column_arcs().zs.expect("Z column");
        let mapped = seq.map_xy(|x, y| (x + 1.0, y + 1.0));
        assert_eq!(mapped.zs().expect("Z carried").to_vec(), [11.0, 12.0]);
        let child_z = mapped.column_arcs().zs.expect("Z column");
        assert!(!Arc::ptr_eq(&parent_z, &child_z));
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
