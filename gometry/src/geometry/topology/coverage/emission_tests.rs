use super::analyze_geographic_coverage;
use crate::error::{ErrorKind, Result};
use crate::geographic::GeographicFrame;
use crate::geometry::coverage::{CoverageOverlapRule, coverage_clean};
use crate::geometry::publication::StorageTarget;
use crate::geometry::{CoordinateAxes, GeometryErrorKind, Shape, SourceArcContext, TopologyDomain};
use crate::work::Unmetered;

#[test]
fn geographic_clean_never_collapses_a_rational_sliver() -> Result<()> {
    let rows = [
        crate::io::parse_wkt(
            "POLYGON ((1 0, 2 0, 2 1, 1.0000000000000002 1, 1 0))",
        )?,
        crate::io::parse_wkt(
            "POLYGON ((1 0.25, 1.5 0.25, 1.5 0.5, 1 0.5, 1 0.25))",
        )?,
    ];
    let context = SourceArcContext::new(
        TopologyDomain::Geographic(GeographicFrame::crs84()),
        None,
    );
    match coverage_clean(&rows, 0.0, 0.0, CoverageOverlapRule::MinIndex, &context) {
        Ok(output) => {
            let sliver = output.get(1).expect("second coverage row");
            assert!(!sliver.is_empty());
            assert!(sliver.area() > 0.0, "nonempty exact face cannot collapse");
            assert_eq!(sliver.validate()?, None);
        },
        Err(error) => assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::UnrepresentableOutput { .. })
        )),
    }
    Ok(())
}

#[test]
fn geographic_clean_attributes_belong_to_the_output_occurrence() -> Result<()> {
    let rows = [
        crate::io::parse_wkt(
            "POLYGON ZM ((0 0 10 7, 1 0 10 7, 1 1 10 7, 0 1 10 7, 0 0 10 7))",
        )?,
        crate::io::parse_wkt(
            "POLYGON ZM ((1 0 20 7, 2 0 20 7, 2 1 20 7, 1 1 20 7, 1 0 20 7))",
        )?,
        crate::io::parse_wkt(
            "POLYGON ZM ((10 0 30 7, 12 0 30 7, 12 1 30 7, 10 1 30 7, 10 0 30 7))",
        )?,
        crate::io::parse_wkt(
            "POLYGON ZM ((11 0 30 7, 13 0 30 7, 13 1 30 7, 11 1 30 7, 11 0 30 7))",
        )?,
    ];
    let model = crate::crs::metric_model(Some("OGC:CRS84"))?;
    let metric = crate::crs::GeodesicMetric::from_model(&model)?;
    let metric_access = || Ok(&metric);
    let context = SourceArcContext::new(
        TopologyDomain::Geographic(GeographicFrame::crs84()),
        Some(&metric_access),
    );
    for reverse in [false, true] {
        for permute in [false, true] {
            let mut input = rows.to_vec();
            if reverse {
                input = input.into_iter().map(|row| row.reverse()).collect();
            }
            if permute {
                input.swap(2, 3);
            }
            for selected in [&input[..2], &input[..]] {
                let output = coverage_clean(
                    selected,
                    0.0,
                    0.0,
                    CoverageOverlapRule::MinIndex,
                    &context,
                )?;
                for (row, z) in output.iter().take(2).zip([10.0_f64, 20.0]) {
                    assert_eq!(row.axes(), CoordinateAxes::XYZM);
                    row.try_for_each_point(|point| {
                        assert_eq!(point.z().map(f64::to_bits), Some(z.to_bits()));
                        assert_eq!(point.m().map(f64::to_bits), Some(7.0_f64.to_bits()));
                        Ok::<_, crate::error::Error>(())
                    })?;
                }
            }
        }
    }
    Ok(())
}

#[test]
fn geographic_clean_empty_row_preserves_source_axes() -> Result<()> {
    let row = crate::io::parse_wkt(
        "POLYGON ZM ((0 0 10 7, 1 0 10 7, 1 1 10 7, 0 1 10 7, 0 0 10 7))",
    )?;
    let context = SourceArcContext::new(
        TopologyDomain::Geographic(GeographicFrame::crs84()),
        None,
    );
    let output = coverage_clean(
        &[row.clone(), row],
        0.0,
        0.0,
        CoverageOverlapRule::MinIndex,
        &context,
    )?;
    let empty = output.get(1).expect("second coverage row");
    assert!(empty.is_empty());
    assert_eq!(empty.axes(), CoordinateAxes::XYZM);
    Ok(())
}

#[test]
fn geographic_scalar_coverage_receipt_rejects_two_live_rows() -> Result<()> {
    let rows = [
        crate::io::parse_wkt("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))")?,
        crate::io::parse_wkt("POLYGON ((3 0, 4 0, 4 1, 3 1, 3 0))")?,
    ];
    let frame = GeographicFrame::crs84();
    let analysis = analyze_geographic_coverage(&rows, frame, 0.0, "coverage_clean")?;
    let mut work = Unmetered;
    let assignment = analysis.assign_cleaning_faces(
        &rows.iter().map(Shape::area).collect::<Vec<_>>(),
        0.0,
        CoverageOverlapRule::MinIndex,
        &mut work,
    )?;
    let context: SourceArcContext<'_, '_> =
        SourceArcContext::new(TopologyDomain::Geographic(frame), None);
    match analysis.coverage_drawing_receipt(
        &assignment,
        CoordinateAxes::XY,
        &context,
        StorageTarget::Scalar,
        &mut work,
    ) {
        Err(error) => assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
        )),
        Ok(_) => panic!("scalar coverage silently discarded a live output row"),
    }
    Ok(())
}
