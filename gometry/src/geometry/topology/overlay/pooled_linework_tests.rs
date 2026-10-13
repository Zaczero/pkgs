#![expect(
    clippy::panic_in_result_fn,
    reason = "Result-returning tests use assertions for their behavior checks"
)]

use super::{
    DiagnosticKind, ExactGeographicTopology, QEmissionPlan, QLineOrigin, QProducer, SourceRowId,
};
use crate::error::Result;
use crate::geographic::{GeographicFrame, GeographicInputs};
use crate::geometry::{GeometryErrorKind, Shape};
use crate::work::Unmetered;

fn topology(shapes: &[Shape]) -> Result<ExactGeographicTopology> {
    let frame = GeographicFrame::crs84();
    let inputs = GeographicInputs::try_from_shapes(frame, shapes.iter())
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    ExactGeographicTopology::from_views(frame, inputs.views().iter().copied())
}

#[test]
fn polygonize_full_keeps_a_dangle_on_the_joint_polygon_contact_graph() -> Result<()> {
    let boundary = crate::io::parse_wkt("LINESTRING (0 0, 10 0, 10 10, 0 10, 0 0)")?;
    let dangle = crate::io::parse_wkt("LINESTRING (5 0, 5 -3)")?;
    let topology = topology(&[boundary, dangle])?;
    let mut work = Unmetered;
    let plan = QEmissionPlan::build_polygonize(&topology, true, &mut work)?;

    let diagnostic = plan
        .line_paths
        .iter()
        .find(|line| line.origin == QLineOrigin::Diagnostic(DiagnosticKind::Dangle))
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("the joined dangle diagnostic is missing".into())
        })?;
    assert_eq!(diagnostic.source_row, Some(SourceRowId(1)));
    let step = diagnostic
        .path
        .first()
        .ok_or_else(|| GeometryErrorKind::Invalid("the dangle diagnostic has no span".into()))?;
    let span = topology
        .q
        .spans
        .get(step.span)
        .ok_or_else(|| GeometryErrorKind::Invalid("the dangle span is unknown".into()))?;
    let source_rows = span
        .owners
        .iter()
        .map(|owner| {
            topology
                .sources
                .edges
                .get(owner.source_edge.0 as usize)
                .map(|edge| edge.row)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("the dangle owner has no source edge".into()).into()
                })
        })
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(source_rows, [SourceRowId(1)]);
    let shared_node_count = span
        .nodes
        .iter()
        .copied()
        .filter(|node| {
            plan.cycles
                .iter()
                .flat_map(|cycle| &cycle.darts)
                .any(|dart_id| {
                    topology
                        .q
                        .darts
                        .get(dart_id.0 as usize)
                        .and_then(|dart| topology.q.spans.get(dart.span.0 as usize))
                        .is_some_and(|boundary_span| boundary_span.nodes.contains(node))
                })
        })
        .count();
    assert_eq!(shared_node_count, 1);
    Ok(())
}

#[test]
fn polygonize_full_does_not_diagnose_a_span_used_by_a_selected_face() -> Result<()> {
    let boundary = crate::io::parse_wkt("LINESTRING (0 0, 10 0, 10 10, 0 10, 0 0)")?;
    let shared_boundary = crate::io::parse_wkt("LINESTRING (0 0, 10 0)")?;
    let topology = topology(&[boundary, shared_boundary])?;
    let mut work = Unmetered;
    let plan = QEmissionPlan::build_polygonize(&topology, true, &mut work)?;

    assert!(
        !plan.cycles.is_empty(),
        "the shared boundary encloses a face"
    );
    assert!(plan.line_paths.is_empty());
    Ok(())
}

#[test]
fn geographic_polygonize_full_source_dangle_keeps_its_own_axes() -> Result<()> {
    let boundary = crate::io::parse_wkt("LINESTRING (0 0, 10 0, 10 10, 0 10, 0 0)")?;
    let dangle = crate::io::parse_wkt("LINESTRING ZM (20 0 1 10, 20 1 2 20)")?;
    let frame = GeographicFrame::crs84();
    let context: crate::geometry::SourceArcContext<'_, '_, crate::crs::GeodesicMetric> =
        crate::geometry::SourceArcContext::new(
            crate::geometry::TopologyDomain::Geographic(frame),
            None,
        );

    let full = super::exact_geographic_polygonize_full_from_shapes(
        &[&boundary, &dangle],
        false,
        &context,
    )?;

    assert_eq!(full.polygons.len(), 1);
    assert_eq!(full.dangles.len(), 2);
    assert!(full.dangles[0].is_empty());
    let Some(Shape::LineString(line)) = full.dangles[1].first() else {
        return Err(
            GeometryErrorKind::Invalid("Q geographic source dangle is missing".into()).into(),
        );
    };
    let coordinates = line.as_coords();
    assert!(coordinates.axes().has_z());
    assert!(coordinates.axes().has_m());
    let mut points = coordinates
        .iter()
        .map(|point| {
            (
                point.x.to_bits(),
                point.y.to_bits(),
                point.z().map(f64::to_bits),
                point.m().map(f64::to_bits),
            )
        })
        .collect::<Vec<_>>();
    points.sort_unstable();
    assert_eq!(points, [
        (
            20.0_f64.to_bits(),
            0.0_f64.to_bits(),
            Some(1.0_f64.to_bits()),
            Some(10.0_f64.to_bits()),
        ),
        (
            20.0_f64.to_bits(),
            1.0_f64.to_bits(),
            Some(2.0_f64.to_bits()),
            Some(20.0_f64.to_bits()),
        ),
    ]);
    Ok(())
}

#[test]
fn geographic_q_node_and_build_area_bind_through_producer_drawing() -> Result<()> {
    let frame = GeographicFrame::crs84();
    let crossing = crate::io::parse_wkt("MULTILINESTRING ((170 0, -170 0), (180 -1, 180 1))")?;
    let boundary = crate::io::parse_wkt("LINESTRING (0 0, 10 0, 10 10, 0 10, 0 0)")?;
    let context: crate::geometry::SourceArcContext<'_, '_, crate::crs::GeodesicMetric> =
        crate::geometry::SourceArcContext::new(
            crate::geometry::TopologyDomain::Geographic(frame),
            None,
        );

    let noded = super::exact_geographic_linework_from_shapes(
        &[&crossing],
        QProducer::Node,
        false,
        false,
        &context,
        crate::Frame::None,
    )?;
    assert!(matches!(noded, Shape::MultiLineString(_)));
    assert!(!noded.is_empty());
    assert!(noded.any_point(|point| {
        point.y.to_bits() == 0.0_f64.to_bits()
            && (point.x.to_bits() == 180.0_f64.to_bits()
                || point.x.to_bits() == (-180.0_f64).to_bits())
    }));

    let area = super::exact_geographic_linework_from_shapes(
        &[&boundary],
        QProducer::BuildArea,
        false,
        false,
        &context,
        crate::Frame::None,
    )?;
    assert!(matches!(area, Shape::Polygon(_) | Shape::MultiPolygon(_)));
    assert!(!area.is_empty());
    assert!(area.any_point(|point| {
        point.x.to_bits() == 0.0_f64.to_bits() && point.y.to_bits() == 0.0_f64.to_bits()
    }));
    Ok(())
}
