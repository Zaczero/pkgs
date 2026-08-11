#[cfg(test)]
mod csr_offset_tests {
    use crate::error::ErrorKind;
    use crate::geometry::{CsrOffsetColumn, GeometryErrorKind};

    #[test]
    fn csr_vertex_count_rejects_i32_overflow() {
        let overflow = (i32::MAX as usize).saturating_add(1);
        i32::try_from(overflow).unwrap_err();
        // `as i32` would silently wrap past `i32::MAX` (negative offset).
        assert!((overflow as i32).is_negative());
        let error = CsrOffsetColumn::<()>::try_new(vec![0, overflow], overflow)
            .expect_err("overflow must fail at freeze");
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::OffsetCapacityExceeded)
        ));
        assert!(error.to_string().contains("i32 offset capacity"));
    }
}

#[cfg(test)]
mod linref_array_degrade_tests {
    use crate::broadcast::{
        CollectRows as _, degrade_linref_float, degrade_linref_linestring, degrade_linref_point,
        degrade_linref_point_between, is_degradable_line_row,
    };
    use crate::crs::MetricModel;
    use crate::error::ErrorKind;
    use crate::geometry::{
        CoordSeq, CoordinateAxes, EmptyKind, GeometryErrorKind, LineSeq, MeasureRange, Point,
        Polygon, Ring, Shape,
    };
    use crate::{line_interpolate_coordseq, line_locate_coordseq, line_substring_coordseq};

    fn line_coordseq(points: &[(f64, f64)]) -> CoordSeq {
        CoordSeq::from_points(
            &points
                .iter()
                .map(|&(x, y)| Point::new_unchecked_xy(x, y))
                .collect::<Vec<_>>(),
        )
    }

    fn line_xy(points: &[(f64, f64)]) -> Shape {
        Shape::LineString(LineSeq::try_new(line_coordseq(points)).expect("test line is valid"))
    }

    fn line_with_m(vertices: &[(f64, f64, f64)]) -> Shape {
        Shape::LineString(
            LineSeq::try_new(CoordSeq::from_columns(
                vertices.iter().map(|&(x, ..)| x).collect::<Vec<_>>().into(),
                vertices
                    .iter()
                    .map(|&(_, y, _)| y)
                    .collect::<Vec<_>>()
                    .into(),
                None,
                Some(
                    vertices
                        .iter()
                        .map(|&(_, _, m)| m)
                        .collect::<Vec<_>>()
                        .into(),
                ),
            ))
            .expect("test line is valid"),
        )
    }

    fn empty_line() -> Shape {
        Shape::LineString(LineSeq::empty(CoordinateAxes::XY))
    }

    fn planar_model() -> MetricModel {
        MetricModel::COORDINATE
    }

    fn query_point() -> Point {
        Point::new_unchecked_xy(5.0, 0.0)
    }

    #[test]
    fn is_degradable_line_row_recognizes_data_failures_only() {
        for kind in [
            GeometryErrorKind::EmptyLinework,
            GeometryErrorKind::MissingMeasure,
            GeometryErrorKind::NonMonotonicMeasure,
        ] {
            assert!(is_degradable_line_row(&kind.into()));
        }
        assert!(!is_degradable_line_row(
            &GeometryErrorKind::LineStringRequired.into()
        ));
        assert!(!is_degradable_line_row(&GeometryErrorKind::finite(
            "distance",
            f64::NAN
        )));
    }

    #[test]
    fn line_locate_point_degrades_empty_row_to_nan() {
        let model = planar_model();
        let query = query_point();
        let lines = [
            line_coordseq(&[(0.0, 0.0), (10.0, 0.0)]),
            CoordSeq::empty(CoordinateAxes::XY),
        ];
        let results = lines
            .iter()
            .map(|line| degrade_linref_float(line_locate_coordseq(&model, line, query, false)))
            .collect_rows()
            .expect("degradable rows must not abort");
        assert!((results[0] - 5.0).abs() < 1e-12);
        assert!(results[1].is_nan());
    }

    #[test]
    fn line_locate_point_m_degrades_empty_missing_and_non_monotonic_rows() {
        let query = query_point();
        let lines = [
            line_with_m(&[(0.0, 0.0, 0.0), (10.0, 0.0, 100.0)]),
            empty_line(),
            line_xy(&[(0.0, 0.0), (10.0, 0.0)]),
            line_with_m(&[(0.0, 0.0, 0.0), (10.0, 0.0, 100.0), (5.0, 0.0, 50.0)]),
        ];
        let results = lines
            .iter()
            .map(|line| degrade_linref_float(line.line_locate_point_m(query)))
            .collect_rows()
            .expect("degradable rows must not abort");
        assert!((results[0] - 50.0).abs() < 1e-12);
        assert!(results[1].is_nan());
        assert!(results[2].is_nan());
        assert!(results[3].is_nan());
    }

    #[test]
    fn line_interpolate_point_degrades_empty_row_to_point_empty() {
        let model = planar_model();
        let lines = [
            line_coordseq(&[(0.0, 0.0), (10.0, 0.0)]),
            CoordSeq::empty(CoordinateAxes::XY),
        ];
        let results = lines
            .iter()
            .map(|line| {
                degrade_linref_point_between(line_interpolate_coordseq(&model, line, 4.0, false))
            })
            .collect_rows()
            .expect("degradable rows must not abort");
        assert!(matches!(results[0], Shape::Point(p) if (p.x - 4.0).abs() < 1e-12));
        assert!(matches!(results[1], Shape::Empty(EmptyKind::Point, _)));
    }

    #[test]
    fn line_interpolate_point_m_degrades_measured_data_failures() {
        let lines = [
            line_with_m(&[(0.0, 0.0, 0.0), (10.0, 0.0, 100.0)]),
            empty_line(),
            line_xy(&[(0.0, 0.0), (10.0, 0.0)]),
            line_with_m(&[(0.0, 0.0, 0.0), (10.0, 0.0, 100.0), (5.0, 0.0, 50.0)]),
        ];
        let results = lines
            .iter()
            .map(|line| degrade_linref_point(line.line_interpolate_point_m(50.0)))
            .collect_rows()
            .expect("degradable rows must not abort");
        assert!(matches!(results[0], Shape::Point(p) if (p.x - 5.0).abs() < 1e-12));
        for bad in &results[1..] {
            assert!(matches!(bad, Shape::Empty(EmptyKind::Point, _)));
        }
    }

    #[test]
    fn line_substring_degrades_empty_row_to_linestring_empty() {
        let model = planar_model();
        let range = MeasureRange::substring_distance(2.0, 6.0).expect("ordered distances");
        let lines = [
            line_coordseq(&[(0.0, 0.0), (10.0, 0.0)]),
            CoordSeq::empty(CoordinateAxes::XY),
        ];
        let results = lines
            .iter()
            .map(|line| {
                degrade_linref_linestring(line_substring_coordseq(&model, line, range, false))
            })
            .collect_rows()
            .expect("degradable rows must not abort");
        assert!(matches!(&results[0], Shape::LineString(seq) if !seq.is_empty()));
        assert!(matches!(&results[1], Shape::LineString(seq) if seq.is_empty()));
    }

    #[test]
    fn line_substring_m_degrades_measured_data_failures() {
        let range = MeasureRange::substring_measure(25.0, 75.0).expect("ordered measures");
        let lines = [
            line_with_m(&[(0.0, 0.0, 0.0), (10.0, 0.0, 100.0)]),
            empty_line(),
            line_xy(&[(0.0, 0.0), (10.0, 0.0)]),
        ];
        let results = lines
            .iter()
            .map(|line| degrade_linref_linestring(line.line_substring_m(range)))
            .collect_rows()
            .expect("degradable rows must not abort");
        assert!(matches!(&results[0], Shape::LineString(seq) if !seq.is_empty()));
        for bad in &results[1..] {
            assert!(matches!(bad, Shape::LineString(seq) if seq.is_empty()));
        }
    }

    #[test]
    fn linref_array_non_degradable_failures_still_abort() {
        let polygon = Shape::Polygon(Polygon::new(
            Ring::from_trusted_closed(line_coordseq(&[
                (0.0, 0.0),
                (1.0, 0.0),
                (1.0, 1.0),
                (0.0, 0.0),
            ])),
            vec![],
        ));
        let query = query_point();
        let wrong_kind = polygon
            .line_locate_point(query, false)
            .expect_err("wrong kind");
        assert!(matches!(
            wrong_kind.kind(),
            ErrorKind::Geometry(GeometryErrorKind::LineStringRequired)
        ));
        assert!(!is_degradable_line_row(&wrong_kind));

        let out_of_order = MeasureRange::substring_measure(6.0, 2.0).expect_err("ordered");
        assert!(!is_degradable_line_row(&out_of_order));

        let lines = [line_xy(&[(0.0, 0.0), (10.0, 0.0)]), polygon];
        let batch = lines
            .iter()
            .map(|line| degrade_linref_float(line.line_locate_point(query, false)))
            .collect_rows();
        assert!(batch.is_err(), "wrong-kind rows must abort the batch");
    }
}
