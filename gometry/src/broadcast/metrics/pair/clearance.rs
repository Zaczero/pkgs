use super::{
    CoordinateAxes, GeometryKind, LineSeq, LocalInputPurpose, PreparedLocalInput, Shape, crs,
    dispatch_metric_model, geodesic_by_identity,
};
use crate::geometry::{
    GeographicProvenancePlan, emit_from_original_with_source_roots, emit_from_original_xy,
};

/// CRS-aware minimum-clearance witness line: the realizing two-point
/// `LineString` in the geometry's own coordinates. On a geographic CRS the
/// witness is found in the geometry's best local projection (the same frame
/// `metric_minimum_clearance` measures in), but the witness coordinates are
/// emitted from source-frame vertex provenance instead of round-tripping the
/// projected line.
pub(crate) fn metric_minimum_clearance_line(
    model: &crs::MetricModel,
    crs: Option<&str>,
    shape: &Shape,
) -> crate::error::Result<Shape> {
    metric_minimum_clearance_line_impl(model, crs, shape, true)
}

fn metric_minimum_clearance_line_impl(
    model: &crs::MetricModel,
    crs: Option<&str>,
    shape: &Shape,
    resolve_source_ordinates: bool,
) -> crate::error::Result<Shape> {
    dispatch_metric_model(
        model,
        |_| shape.minimum_clearance_line(),
        |_, _, state| {
            let mut budget =
                crate::geometry::ExpansionBudget::new("minimum_clearance", "input_refinement");
            geodesic_by_identity(
                shape,
                crs,
                state,
                &mut budget,
                if resolve_source_ordinates {
                    LocalInputPurpose::ClearanceWitness
                } else {
                    LocalInputPurpose::BoundarySupport
                },
                |sites, support, refined_chains| {
                    let Some(witness) = sites.minimum_clearance_plan_with_support(
                        support,
                        refined_chains,
                        resolve_source_ordinates,
                    )?
                    else {
                        return Ok((
                            Vec::new(),
                            Shape::typed_empty(GeometryKind::LineString, CoordinateAxes::XY),
                        ));
                    };
                    let plan = witness.plan;
                    let projected_points = emit_from_original_xy(&witness.points, &plan)?;
                    let [a, b] = projected_points.as_slice() else {
                        unreachable!("minimum-clearance witness plan has two vertices");
                    };
                    Ok((
                        Vec::from(plan),
                        Shape::LineString(LineSeq::segment(*a, *b)?),
                    ))
                },
                |source_points, original_roots, plan, axes, source_frame| {
                    if !resolve_source_ordinates {
                        return emit_from_original_xy(source_points, plan);
                    }
                    let plan = GeographicProvenancePlan::new(source_points, plan, axes)?;
                    let source_metric = std::cell::OnceCell::new();
                    let metric_access = || {
                        source_metric
                            .get_or_try_init(|| crate::crs::GeodesicMetric::from_model(model))
                    };
                    emit_from_original_with_source_roots(
                        &plan,
                        original_roots,
                        source_frame,
                        Some(&metric_access),
                        &mut crate::work::Unmetered,
                    )
                },
                |points| {
                    if points.is_empty() {
                        Ok(Shape::typed_empty(
                            GeometryKind::LineString,
                            CoordinateAxes::XY,
                        ))
                    } else {
                        let [a, b] = points.as_slice() else {
                            unreachable!("minimum_clearance witness plan has two vertices");
                        };
                        Ok(Shape::LineString(LineSeq::segment(*a, *b)?))
                    }
                },
            )
        },
    )
}

/// CRS-aware minimum clearance: the planar clearance in the active planar unit,
/// or — on a geographic CRS — the geodesic length of the source-frame
/// provenance witness selected in the geometry's best local projection.
pub(crate) fn metric_minimum_clearance(
    model: &crs::MetricModel,
    crs: Option<&str>,
    shape: &Shape,
) -> crate::error::Result<f64> {
    dispatch_metric_model(
        model,
        |to_metre| Ok(shape.minimum_clearance()? * to_metre),
        |geodesic_crs, _, state| {
            let line = metric_minimum_clearance_line_impl(model, crs, shape, false)?;
            crs::geodesic_witness_distance_at(geodesic_crs, &line, state)
        },
    )
}

#[cfg(test)]
mod minimum_clearance_computed_edge_tests {
    use geographiclib_rs::InverseGeodesic as _;

    use super::metric_minimum_clearance_line;
    use crate::geographic::GeographicFrame;
    use crate::geometry::{CoordSeq, LineSeq, MultiPoint, Point, Shape};

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn minimum_clearance_line_across_the_seam_uses_a_short_connector() {
        let points = [
            Point::new_raw_xy(179.8, 0.0),
            Point::new_raw_xy(-179.8, 0.0),
        ];
        let shape = Shape::MultiPoint(
            MultiPoint::from_coordseq(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
        );
        let model = crate::crs::metric_model(Some("EPSG:4326")).unwrap();
        let output = metric_minimum_clearance_line(&model, Some("EPSG:4326"), &shape).unwrap();
        let Shape::LineString(line) = output else {
            panic!("minimum clearance publishes a line");
        };
        let coordinates = line.as_coords().iter().collect::<Vec<_>>();
        assert_eq!(coordinates.len(), 2);
        assert!(
            coordinates
                .iter()
                .any(|point| point.x.to_bits() == 179.8_f64.to_bits())
        );
        assert!(
            coordinates
                .iter()
                .any(|point| point.x.to_bits() == (-179.8_f64).to_bits())
        );
        let axis = GeographicFrame::crs84().longitude();
        let step = axis
            .chart_step(coordinates[0].x, coordinates[1].x)
            .unwrap()
            .delta(axis);
        assert!(step.abs() < 1.0, "seam connector step is {step}");
    }
    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn minimum_clearance_line_refined_support_uses_original_root_ordinates() {
        fn chart_length(
            geodesic: &geographiclib_rs::Geodesic,
            start: Point,
            end: Point,
            stop: f64,
        ) -> f64 {
            let pieces = 4096_u32;
            let mut length = 0.0;
            let mut previous = start;
            for step in 1..=pieces {
                let parameter = stop * f64::from(step) / f64::from(pieces);
                let next = Point::new_raw_xy(
                    start.x + parameter * (end.x - start.x),
                    start.y + parameter * (end.y - start.y),
                );
                let segment_length: f64 = geodesic.inverse(previous.y, previous.x, next.y, next.x);
                length += segment_length;
                previous = next;
            }
            length
        }

        let root_start = Point::new_raw_axes(
            0.0,
            0.0,
            crate::geometry::ZOrdinate(Some(10.0)),
            crate::geometry::MOrdinate(Some(100.0)),
        );
        let root_end = Point::new_raw_axes(
            1.0,
            1.0,
            crate::geometry::ZOrdinate(Some(210.0)),
            crate::geometry::MOrdinate(Some(500.0)),
        );
        let near_start = Point::new_raw_axes(
            0.5,
            0.5001,
            crate::geometry::ZOrdinate(Some(110.0)),
            crate::geometry::MOrdinate(Some(300.0)),
        );
        let near_end = Point::new_raw_axes(
            0.5002,
            0.5003,
            crate::geometry::ZOrdinate(Some(111.0)),
            crate::geometry::MOrdinate(Some(301.0)),
        );
        let line = |points: [Point; 2]| {
            Shape::LineString(
                LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
            )
        };
        let shape = Shape::geometry_collection(vec![
            line([root_start, root_end]),
            line([near_start, near_end]),
        ])
        .unwrap();
        let model = crate::crs::metric_model(Some("OGC:CRS84")).unwrap();
        let output = metric_minimum_clearance_line(&model, Some("OGC:CRS84"), &shape).unwrap();
        let Shape::LineString(witness) = output else {
            panic!("minimum clearance publishes a line");
        };
        let coordinates = witness.as_coords().iter().collect::<Vec<_>>();
        let foot = coordinates[1];
        let parameter = (foot.x - root_start.x) / (root_end.x - root_start.x);
        let geodesic = geographiclib_rs::Geodesic::wgs84();
        let fraction = chart_length(&geodesic, root_start, root_end, parameter)
            / chart_length(&geodesic, root_start, root_end, 1.0);

        assert!((foot.z().unwrap() - (10.0 + 200.0 * fraction)).abs() < 1.0e-8);
        assert!((foot.m().unwrap() - (100.0 + 400.0 * fraction)).abs() < 1.0e-8);
    }
}
