#![expect(
    clippy::panic_in_result_fn,
    reason = "known-answer acceptance checks use assertions alongside fallible setup"
)]

use super::{
    EvaluationBudget, ExactGeographicTopology, ExactRational, GeographicFrame, GeometryErrorKind,
    NodeOrdinates, QCoordinates, QEmbeddingCertification, QEmbeddingOutput, QEmissionPlan,
    SearchOutcome, certify_and_search_q_embedding,
};
use crate::error::Result;
use crate::geometry::OverlayOp;

fn topology(shapes: &[crate::geometry::Shape]) -> Result<ExactGeographicTopology> {
    let frame = GeographicFrame::crs84();
    let inputs = crate::geographic::GeographicInputs::try_from_shapes(frame, shapes.iter())
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    ExactGeographicTopology::from_views(frame, inputs.views().iter().copied())
}

fn published_sample_contact_fixture() -> Result<[crate::geometry::Shape; 2]> {
    let line = crate::io::parse_wkt("LINESTRING (0 1, 360 1.0000000000000004)")?;
    let point = crate::io::parse_wkt("POINT (90 1)")?;
    let input = crate::geometry::Shape::geometry_collection([line, point].into_iter().collect())?;
    let clip = crate::io::parse_wkt("POLYGON ((0 0, 180 0, 180 2, 0 2, 0 0))")?;
    Ok([input, clip])
}

fn sealed_shape(output: QEmbeddingOutput) -> Result<crate::geometry::Shape> {
    let finished = match output.sealed {
        super::super::emission::SealedQDrawing::Source(source) => source.sealed,
        super::super::emission::SealedQDrawing::Native(native) => native.sealed,
    };
    Ok(finished.into_scalar()?.0)
}

#[test]
fn partial_circle_publication_sample_repairs_contact_without_losing_either_leaf() -> Result<()> {
    let shapes = published_sample_contact_fixture()?;
    let topology = topology(&shapes)?;
    let mut budget = EvaluationBudget::new(OverlayOp::Intersection.name(), 0)?;
    let plan = QEmissionPlan::build(&topology, OverlayOp::Intersection, &mut budget)?;
    let nearest = QCoordinates::build_with_work(&topology, &mut budget)?;
    let publication = plan.fragment_recipes.publish(
        &topology,
        &nearest,
        OverlayOp::Intersection.name(),
        &mut budget,
    )?;

    let expected_sample_y =
        ExactRational::from_f64(1.0).try_add(&ExactRational::from_f64(2.0_f64.powi(-53)))?;
    let expected_sample_x = ExactRational::from_f64(90.0);
    let mut sample_index = None;
    for (index, sample) in publication.samples.iter().enumerate() {
        if sample.point.x.to_bits() == 90.0_f64.to_bits()
            && sample.point.y.to_bits() == 1.0_f64.to_bits()
            && sample.exact_point.x.try_cmp(&expected_sample_x)?.is_eq()
            && sample.exact_point.y.try_cmp(&expected_sample_y)?.is_eq()
        {
            sample_index = Some(index);
            break;
        }
    }
    let sample_index = sample_index.ok_or_else(|| {
        GeometryErrorKind::Invalid(
            "the clipped Circle fragment did not retain its exact midpoint sample".into(),
        )
    })?;
    let sample = publication.samples.get(sample_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("the retained Circle midpoint sample is unknown".into())
    })?;
    assert_eq!(sample.parameter, ExactRational::from_f64(0.5));
    assert_eq!(
        sample.published_chart_point.y,
        ExactRational::from_f64(1.0),
        "the exact midpoint ties to the source point's nearest-even Y"
    );

    let source_points = vec![None; topology.q.nodes.len()];
    let drawing = plan.drawing(&topology, &nearest, &source_points, &mut budget)?;
    let live_keys = drawing.live_node_keys(topology.sources.frame, &mut budget)?;
    let emission = plan.embedding_emission(&topology, &source_points, &publication, &mut budget)?;
    let ordinates = vec![NodeOrdinates::default(); topology.q.nodes.len()];
    let certification = QEmbeddingCertification::<crate::crs::GeodesicMetric> {
        topology: &topology,
        emission: &emission,
        nearest: &nearest,
        ordinates: &ordinates,
        declared_axes: crate::geometry::CoordinateAxes::XY,
        strict_axes: false,
        operation: OverlayOp::Intersection.name(),
        metric_access: None,
    };
    let nearest_problem = certification.build_problem(&mut budget)?;
    let sample_node = *nearest_problem
        .publication_sample_nodes
        .get(sample_index)
        .ok_or_else(|| GeometryErrorKind::Invalid("sample node is missing".into()))?;
    let nearest_projection = nearest_problem.certify_assigned_producer(
        &nearest,
        &nearest_problem.publication_nearest.clone(),
        None,
        &mut budget,
    )?;
    let contact_rejections =
        nearest_problem.publication_graph_rejections(&nearest_projection, &mut budget)?;
    let point_port = *topology.q.source_point_ports.first().ok_or_else(|| {
        GeometryErrorKind::Invalid("the nonincident point has no retained Q port".into())
    })?;
    let point_node = topology
        .q
        .ports
        .get(point_port.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("the nonincident point port is unknown".into()))?
        .node;
    assert!(
        contact_rejections.contains(&super::QVariableNode::Source(point_node))
            && contact_rejections.contains(&super::QVariableNode::Published(sample_node)),
        "the actual source-point/publication-leaf contact must depend on both identities"
    );
    let pass =
        match certify_and_search_q_embedding(&certification, &live_keys, drawing, &mut budget)? {
            SearchOutcome::Pass(pass) => pass,
            SearchOutcome::Exhausted => {
                return Err(GeometryErrorKind::unrepresentable_output(
                    OverlayOp::Intersection.name(),
                ));
            },
        };
    assert!(
        pass.searched,
        "the colliding nearest sample must be repaired"
    );

    let result = sealed_shape(pass.output)?;
    assert!(
        crate::geometry::validate_shape_in_frame(
            &result,
            crate::geometry::TopologyDomain::Geographic(GeographicFrame::crs84()),
        )?
        .is_none(),
        "the repaired complete producer output must remain valid"
    );
    let mut line_points = Vec::new();
    let mut sample_incident_leaves = 0;
    let mut point_records = Vec::new();
    result.try_for_each_component(&mut |component| -> Result<()> {
        match component {
            crate::geometry::Component::LineString(line) => {
                let points = line.points().collect::<Vec<_>>();
                sample_incident_leaves += points
                    .windows(2)
                    .filter(|pair| {
                        pair.iter().any(|point| {
                            point.x.to_bits() == 90.0_f64.to_bits()
                                && point.y.to_bits() == 1.0_f64.next_up().to_bits()
                        })
                    })
                    .count();
                line_points.extend(points);
            },
            crate::geometry::Component::Point(point) => point_records.push(point),
            crate::geometry::Component::Polygon(_) => {},
        }
        Ok(())
    })?;
    assert_eq!(
        sample_incident_leaves, 2,
        "the checked midpoint must remain shared by both published leaves"
    );
    assert!(
        line_points.iter().any(|point| {
            point.x.to_bits() == 90.0_f64.to_bits()
                && point.y.to_bits() == 1.0_f64.next_up().to_bits()
        }),
        "the midpoint must use the admitted nearest Y neighbor after the collision"
    );
    assert!(
        point_records.iter().any(|point| {
            point.x.to_bits() == 90.0_f64.to_bits() && point.y.to_bits() == 1.0_f64.to_bits()
        }),
        "repair must preserve the selected nonincident source point record"
    );
    Ok(())
}
