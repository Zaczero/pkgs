use super::{
    EvaluationBudget, ExactGeographicTopology, ExactRational, GeographicFrame, GeometryErrorKind,
    NodeOrdinates, QCoordinates, QEmbeddingCertification, QEmbeddingOutput, QEmissionPlan,
    SearchOutcome, certify_and_search_q_embedding,
};
use crate::error::Result;
use crate::geometry::OverlayOp;
use crate::geometry::publication::{
    ProducerDrawingReceipt, StorageTarget, finish_producer_drawing,
};

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

fn assigned_leaf_interval_fixture() -> Result<[crate::geometry::Shape; 3]> {
    Ok([
        crate::io::parse_wkt("LINESTRING (0 0, 1 0)")?,
        crate::io::parse_wkt("LINESTRING (0 1, 1 -2)")?,
        crate::io::parse_wkt("POINT (0.3333333333333333 0)")?,
    ])
}

fn line_shape(points: &[(f64, f64)]) -> Result<crate::geometry::Shape> {
    let points = points
        .iter()
        .map(|&(x, y)| crate::geometry::Point::new_raw_xy(x, y))
        .collect::<Vec<_>>();
    let coordinates = crate::geometry::CoordSeq::try_from_points(&points)?;
    Ok(crate::geometry::Shape::LineString(
        crate::geometry::LineSeq::from_proven(coordinates),
    ))
}

fn sealed_shape(output: QEmbeddingOutput) -> Result<crate::geometry::Shape> {
    let finished = match output.drawing {
        super::super::emission::CompletedQDrawing::Source(source) => source.sealed,
        super::super::emission::CompletedQDrawing::Native(native) => native.sealed,
        super::super::emission::CompletedQDrawing::Drawing { shape, frame, .. } => {
            finish_producer_drawing(
                ProducerDrawingReceipt::drawing(shape)?,
                frame,
                StorageTarget::Scalar,
            )?
        },
    };
    Ok(finished.into_scalar()?.0)
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions define the test oracle while Result propagates setup errors"
)]
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

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions define the test oracle while Result propagates setup errors"
)]
fn assigned_leaf_uses_the_repaired_one_third_endpoint_and_keeps_its_inventory() -> Result<()> {
    let shapes = assigned_leaf_interval_fixture()?;
    let topology = topology(&shapes)?;
    let mut budget = EvaluationBudget::new(OverlayOp::Union.name(), 0)?;
    let plan = QEmissionPlan::build(&topology, OverlayOp::Union, &mut budget)?;
    let nearest = QCoordinates::build_with_work(&topology, &mut budget)?;
    let publication =
        plan.fragment_recipes
            .publish(&topology, &nearest, OverlayOp::Union.name(), &mut budget)?;

    let exact_third = ExactRational::from_f64(1.0).try_quotient(&ExactRational::from_f64(3.0))?;
    let written_third = 1.0_f64 / 3.0;
    let fragment_index = publication
        .fragments
        .iter()
        .position(|fragment| {
            fragment
                .exact_endpoints
                .iter()
                .any(|point| point.x == exact_third && point.y.is_zero())
        })
        .ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "the exact one-third crossing has no published producer leaf".into(),
            )
        })?;
    let fragment = publication.fragments.get(fragment_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("the one-third producer leaf index is unknown".into())
    })?;
    let frozen_points = fragment.points.clone();
    let frozen_exact_endpoints = fragment.exact_endpoints.clone();
    assert!(
        frozen_points
            .iter()
            .any(|point| point.x.to_bits() == written_third.to_bits()
                && point.y.to_bits() == 0.0_f64.to_bits()),
        "the frozen producer leaf keeps the original nearest endpoint"
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
        operation: OverlayOp::Union.name(),
        metric_access: None,
    };
    let problem = certification.build_problem(&mut budget)?;
    let target = problem
        .exact_positions
        .iter()
        .position(|exact| {
            exact
                .as_ref()
                .is_some_and(|exact| exact.longitude == exact_third && exact.latitude.is_zero())
        })
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("the one-third crossing has no Q source node".into())
        })?;
    let target = super::QNodeId(
        u32::try_from(target).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
    );
    assert_eq!(nearest.xy(target)?.x.to_bits(), written_third.to_bits());

    let pass =
        match certify_and_search_q_embedding(&certification, &live_keys, drawing, &mut budget)? {
            SearchOutcome::Pass(pass) => pass,
            SearchOutcome::Exhausted => {
                return Err(GeometryErrorKind::unrepresentable_output(
                    OverlayOp::Union.name(),
                ));
            },
        };
    assert!(
        pass.searched,
        "the nearest crossing collides with its fixed source vertex"
    );
    assert_eq!(
        pass.coordinates.xy(target)?.x.to_bits(),
        written_third.next_up().to_bits(),
        "the assigned producer chooses the admitted next-up crossing"
    );
    let publication_coordinates = problem.publication_nearest.clone();
    let assigned_projection = problem.certify_assigned_producer(
        &pass.coordinates,
        &publication_coordinates,
        None,
        &mut budget,
    )?;
    assert!(
        assigned_projection.rejections.is_empty(),
        "the accepted assignment must preserve producer-leaf correspondence"
    );
    let chord_index = problem
        .publication_chords
        .iter()
        .position(|chord| {
            chord.recipe == fragment.recipe
                && chord
                    .exact
                    .iter()
                    .any(|point| point.x == exact_third && point.y.is_zero())
        })
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("the assigned one-third chord is missing".into())
        })?;
    let chord = problem.publication_chords.get(chord_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("the assigned one-third chord index is unknown".into())
    })?;
    let endpoint_index = chord
        .exact
        .iter()
        .position(|point| point.x == exact_third && point.y.is_zero())
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("the assigned one-third endpoint is missing".into())
        })?;
    let assigned_leaf = assigned_projection.leaves.get(chord_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("the assigned one-third leaf is missing".into())
    })?;
    let assigned_endpoint = assigned_leaf.points.get(endpoint_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("the assigned one-third leaf endpoint is missing".into())
    })?;
    assert_eq!(
        assigned_endpoint.x,
        ExactRational::from_f64(written_third.next_up()),
        "the producer leaf must follow the assigned endpoint"
    );

    // Counterfactual publisher-rerun control: the original nearest authority
    // leaves B at 1/3 and cannot satisfy the assigned-leaf assertion above.
    let stale_projection =
        problem.certify_assigned_producer(&nearest, &publication_coordinates, None, &mut budget)?;
    let stale_leaf = stale_projection
        .leaves
        .get(chord_index)
        .ok_or_else(|| GeometryErrorKind::Invalid("the stale one-third leaf is missing".into()))?;
    let stale_endpoint = stale_leaf.points.get(endpoint_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("the stale one-third leaf endpoint is missing".into())
    })?;
    assert_eq!(stale_endpoint.x, ExactRational::from_f64(written_third));
    assert_ne!(stale_endpoint.x, assigned_endpoint.x);

    let republished = plan.fragment_recipes.publish(
        &topology,
        &pass.coordinates,
        OverlayOp::Union.name(),
        &mut budget,
    )?;
    let republished_fragment = republished
        .fragments
        .iter()
        .find(|candidate| candidate.recipe == fragment.recipe)
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("candidate-specific publisher lost the producer leaf".into())
        })?;
    assert_eq!(
        republished_fragment.exact_endpoints, frozen_exact_endpoints,
        "candidate-specific publication still names the frozen exact interval"
    );
    assert_eq!(
        republished_fragment.points, frozen_points,
        "rerunning the publisher against the frozen interval omits the assigned leaf"
    );
    assert!(
        !republished_fragment
            .points
            .iter()
            .any(|point| point.x.to_bits() == written_third.next_up().to_bits()),
        "candidate-specific republishing must not stand in for assigned leaf projection"
    );

    let result = sealed_shape(pass.output)?;
    assert!(
        crate::geometry::validate_shape_in_frame(
            &result,
            crate::geometry::TopologyDomain::Geographic(GeographicFrame::crs84()),
        )?
        .is_none(),
        "the complete union must retain its assigned embedding"
    );
    let mut line_vertices = Vec::new();
    result.try_for_each_component(&mut |component| -> Result<()> {
        if let crate::geometry::Component::LineString(line) = component {
            line_vertices.extend(line.points());
        }
        Ok(())
    })?;
    assert!(
        line_vertices.iter().any(
            |point| point.x.to_bits() == written_third.next_up().to_bits()
                && point.y.to_bits() == 0.0_f64.to_bits()
        ),
        "final producer leaves must follow the assigned next-up crossing"
    );
    assert!(
        line_vertices
            .iter()
            .any(|point| point.x.to_bits() == written_third.to_bits()
                && point.y.to_bits() == 0.0_f64.to_bits()),
        "the frozen source vertex at the nearest one-third image must remain"
    );
    assert_eq!(
        fragment.exact_endpoints, frozen_exact_endpoints,
        "candidate repair must not replace the frozen publication interval"
    );
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions define the test oracle while Result propagates setup errors"
)]
fn assigned_endpoint_publisher_shrinks_away_the_midpoint_sample() -> Result<()> {
    use crate::geographic::{
        ComputedEdgeIntent, ComputedSampleOrdinates, ComputedVertexIntent,
        publish_computed_chain_with_vertex_intents_and_samples,
    };
    use crate::geometry::{ExpansionBudget, MOrdinate, ZOrdinate};

    let u = 2.0_f64.powi(-45);
    let shapes = [
        line_shape(&[(0.0, 1.0), (180.0, 1.0)])?,
        line_shape(&[(180.0 - u, 0.0), (180.0, 2.0)])?,
        line_shape(&[(180.0 - u, 2.0), (180.0, 0.0)])?,
    ];
    let topology = topology(&shapes)?;
    let operation = OverlayOp::Union;
    let mut budget = EvaluationBudget::new(operation.name(), 0)?;
    let plan = QEmissionPlan::build(&topology, operation, &mut budget)?;
    let nearest = QCoordinates::build_with_work(&topology, &mut budget)?;
    let publication =
        plan.fragment_recipes
            .publish(&topology, &nearest, operation.name(), &mut budget)?;

    let exact_a = super::super::exact::ExactChartPoint::from_rationals(
        ExactRational::zero(),
        ExactRational::from_f64(1.0),
    );
    let exact_b_x =
        ExactRational::from_f64(180.0).try_subtract(&ExactRational::from_f64(u / 2.0))?;
    let exact_b = super::super::exact::ExactChartPoint::from_rationals(
        exact_b_x.clone(),
        ExactRational::from_f64(1.0),
    );
    let exact_midpoint = super::super::exact::ExactChartPoint::from_rationals(
        exact_b_x.try_quotient(&ExactRational::from_f64(2.0))?,
        ExactRational::from_f64(1.0),
    );
    let fragment = publication
        .fragments
        .iter()
        .find(|fragment| {
            fragment.exact_endpoints.contains(&exact_a)
                && fragment.exact_endpoints.contains(&exact_b)
        })
        .ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "the source edge A→S has no actual initial A→B producer recipe".into(),
            )
        })?;
    let sample_index = fragment
        .sample_range
        .clone()
        .find(|&index| {
            publication.samples.get(index).is_some_and(|sample| {
                sample.exact_point == exact_midpoint
                    && sample.parameter == ExactRational::from_f64(0.5)
            })
        })
        .ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "the actual initial A→B publication recipe did not create midpoint M".into(),
            )
        })?;
    let sample = publication.samples.get(sample_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("the initial publication sample M is missing".into())
    })?;
    assert_eq!(
        sample.family.parent,
        super::super::emission::FeatureFamilyParent::Fragment(fragment.recipe)
    );
    assert_eq!(sample.exact_point, exact_midpoint);
    assert_eq!(fragment.sample_range.len(), 1);

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
        operation: operation.name(),
        metric_access: None,
    };
    let problem = certification.build_problem(&mut budget)?;
    let target_index = problem
        .exact_positions
        .iter()
        .position(|exact| {
            exact
                .as_ref()
                .is_some_and(|exact| exact.longitude == exact_b.x && exact.latitude == exact_b.y)
        })
        .ok_or_else(|| GeometryErrorKind::Invalid("constructed endpoint B has no Q node".into()))?;
    let target = super::QNodeId(
        u32::try_from(target_index).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
    );
    let fixed_s = topology
        .q
        .ports
        .iter()
        .find(|port| {
            !port.cut
                && port.point.x == ExactRational::from_f64(180.0)
                && port.point.y == ExactRational::from_f64(1.0)
        })
        .map(|port| port.node)
        .ok_or_else(|| GeometryErrorKind::Invalid("fixed source endpoint S is missing".into()))?;
    assert!(
        problem
            .node_location
            .get(fixed_s.0 as usize)
            .is_some_and(Option::is_none),
        "S remains a fixed source endpoint"
    );
    assert_eq!(
        super::emitted_node_key(topology.sources.frame, nearest.xy(target)?)?,
        super::emitted_node_key(topology.sources.frame, nearest.xy(fixed_s)?)?,
        "nearest-even B and fixed S share one quotient seam identity"
    );

    let pass =
        match certify_and_search_q_embedding(&certification, &live_keys, drawing, &mut budget)? {
            SearchOutcome::Pass(pass) => pass,
            SearchOutcome::Exhausted => {
                return Err(GeometryErrorKind::unrepresentable_output(operation.name()));
            },
        };
    assert!(pass.searched, "fixed S must force a real endpoint repair");
    let assigned_b = pass.coordinates.xy(target)?;
    assert_ne!(assigned_b, nearest.xy(target)?);
    assert_eq!(
        assigned_b.x.to_bits(),
        180.0_f64.next_down().to_bits(),
        "the accepted endpoint is the admitted left neighbor of 180"
    );
    assert_eq!(assigned_b.y.to_bits(), 1.0_f64.to_bits());

    let publication_coordinates = problem.publication_nearest.clone();
    let assigned = problem.certify_assigned_producer(
        &pass.coordinates,
        &publication_coordinates,
        None,
        &mut budget,
    )?;
    assert!(assigned.rejections.is_empty());
    let sample_node = *problem
        .publication_sample_nodes
        .get(sample_index)
        .ok_or_else(|| GeometryErrorKind::Invalid("M has no frozen identity".into()))?;
    let adjacent_chords = problem
        .publication_chords
        .iter()
        .enumerate()
        .filter(|(_, chord)| {
            chord.recipe == fragment.recipe && chord.exact.contains(&sample.exact_point)
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    assert_eq!(
        adjacent_chords.len(),
        2,
        "the frozen M identity belongs to both adjacent producer chords"
    );
    assert!(
        adjacent_chords
            .iter()
            .all(|&index| problem
                .publication_chords
                .get(index)
                .is_some_and(|chord| chord
                    .nodes
                    .contains(&super::QGraphNode::Published(sample_node)))),
        "both frozen chords name M's stable publication identity"
    );
    let assigned_images = problem.checked_publication_sample_images(&assigned, &mut budget)?;
    let assigned_m = assigned_images
        .get(sample_index)
        .ok_or_else(|| GeometryErrorKind::Invalid("M has no checked assigned image".into()))?;
    for &chord_index in &adjacent_chords {
        let chord = problem
            .publication_chords
            .get(chord_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("a frozen M chord is missing".into()))?;
        let endpoint_index = chord
            .exact
            .iter()
            .position(|point| point == &sample.exact_point)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("a frozen chord lost M's reference".into())
            })?;
        let leaf = assigned
            .leaves
            .get(chord_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("an assigned M leaf is missing".into()))?;
        assert_eq!(
            leaf.points.get(endpoint_index),
            Some(assigned_m),
            "adjacent assigned leaves share the frozen M image"
        );
    }

    let assigned_reference = [ExactRational::zero(), ExactRational::from_f64(assigned_b.x)];
    let assigned_line = crate::geometry::CoordSeq::try_from_points(&[
        crate::geometry::Point::new_raw_xy(0.0, 1.0),
        crate::geometry::Point::new_raw_xy(assigned_b.x, assigned_b.y),
    ])?;
    let mut assigned_samples = Vec::new();
    let mut expansion = ExpansionBudget::new(operation.name(), "computed edges");
    let assigned_output = publish_computed_chain_with_vertex_intents_and_samples(
        topology.sources.frame,
        &assigned_line,
        false,
        &mut expansion,
        |_, _, _| {
            Ok(ComputedEdgeIntent::exact_lifted([
                &assigned_reference[0],
                &assigned_reference[1],
            ]))
        },
        |_, _| ComputedVertexIntent::Sampled,
        |_, _, _, _, _| {
            Ok(ComputedSampleOrdinates {
                y: assigned_b.y,
                z: ZOrdinate(None),
                m: MOrdinate(None),
            })
        },
        |_, _, _, parameter, _, point| {
            if parameter > 0.0 && parameter < 1.0 {
                assigned_samples.push(point);
            }
            Ok(())
        },
    )?;
    assert!(
        assigned_samples.is_empty(),
        "candidate-specific A/B publication is one strict-minor edge and removes M"
    );
    assert_eq!(
        assigned_output.map_or(0, |points| points.len()),
        2,
        "the candidate-specific assigned publisher emits only A and B"
    );
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions define the test oracle while Result propagates setup errors"
)]
fn publication_sample_survives_constructed_endpoint_repair() -> Result<()> {
    let u = 2.0_f64.powi(-45);
    let shapes = [
        line_shape(&[(0.0, 1.0), (180.0, 1.0)])?,
        line_shape(&[(180.0 - u, 0.0), (180.0, 2.0)])?,
        line_shape(&[(180.0 - u, 2.0), (180.0, 0.0)])?,
    ];
    let topology = topology(&shapes)?;
    let operation = OverlayOp::Union;
    let mut budget = EvaluationBudget::new(operation.name(), 0)?;
    let plan = QEmissionPlan::build(&topology, operation, &mut budget)?;
    let nearest = QCoordinates::build_with_work(&topology, &mut budget)?;
    let publication =
        plan.fragment_recipes
            .publish(&topology, &nearest, operation.name(), &mut budget)?;

    let exact_a = super::super::exact::ExactChartPoint::from_rationals(
        ExactRational::zero(),
        ExactRational::from_f64(1.0),
    );
    let exact_b_x =
        ExactRational::from_f64(180.0).try_subtract(&ExactRational::from_f64(u / 2.0))?;
    let exact_b = super::super::exact::ExactChartPoint::from_rationals(
        exact_b_x.clone(),
        ExactRational::from_f64(1.0),
    );
    let exact_midpoint = super::super::exact::ExactChartPoint::from_rationals(
        exact_b_x.try_quotient(&ExactRational::from_f64(2.0))?,
        ExactRational::from_f64(1.0),
    );
    let a_to_b = publication
        .fragments
        .iter()
        .find(|fragment| {
            fragment.exact_endpoints.contains(&exact_a)
                && fragment.exact_endpoints.contains(&exact_b)
        })
        .ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "the source edge A→S has no actual initial A→B producer recipe".into(),
            )
        })?;
    let sample_index = a_to_b
        .sample_range
        .clone()
        .find(|&index| {
            publication.samples.get(index).is_some_and(|sample| {
                sample.exact_point == exact_midpoint
                    && sample.parameter == ExactRational::from_f64(0.5)
            })
        })
        .ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "the actual initial A→B publication recipe did not create midpoint M".into(),
            )
        })?;
    let sample = publication.samples.get(sample_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("the initial publication sample M is missing".into())
    })?;
    let sample_recipe = a_to_b.recipe;
    assert_eq!(
        sample.family.parent,
        super::super::emission::FeatureFamilyParent::Fragment(sample_recipe)
    );
    assert_eq!(sample.exact_point, exact_midpoint);
    assert!(
        a_to_b.sample_range.contains(&sample_index),
        "M belongs to the actual initial A→B publication recipe"
    );
    assert_eq!(
        publication
            .fragments
            .get(sample_recipe.0 as usize)
            .map(|fragment| fragment.sample_range.len()),
        Some(1),
        "the initial A→B recipe retains its one midpoint publication sample"
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
        operation: operation.name(),
        metric_access: None,
    };
    let problem = certification.build_problem(&mut budget)?;
    let target_index = problem
        .exact_positions
        .iter()
        .position(|exact| {
            exact
                .as_ref()
                .is_some_and(|exact| exact.longitude == exact_b.x && exact.latitude == exact_b.y)
        })
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("constructed endpoint B has no Q source node".into())
        })?;
    let target = super::QNodeId(
        u32::try_from(target_index).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
    );
    let fixed_s = topology
        .q
        .ports
        .iter()
        .find(|port| {
            !port.cut
                && port.point.x == ExactRational::from_f64(180.0)
                && port.point.y == ExactRational::from_f64(1.0)
        })
        .map(|port| port.node)
        .ok_or_else(|| GeometryErrorKind::Invalid("fixed source endpoint S is missing".into()))?;
    assert!(
        problem
            .node_location
            .get(fixed_s.0 as usize)
            .is_some_and(Option::is_none),
        "S remains a fixed source endpoint"
    );
    assert_eq!(
        super::emitted_node_key(topology.sources.frame, nearest.xy(target)?)?,
        super::emitted_node_key(topology.sources.frame, nearest.xy(fixed_s)?)?,
        "nearest-even B and fixed S share one quotient seam identity"
    );
    assert_eq!(
        nearest.xy(target)?.y.to_bits(),
        1.0_f64.to_bits(),
        "nearest-even B and S share the same latitude"
    );
    let fixed_s_key = super::emitted_node_key(topology.sources.frame, nearest.xy(fixed_s)?)?;

    let pass =
        match certify_and_search_q_embedding(&certification, &live_keys, drawing, &mut budget)? {
            SearchOutcome::Pass(pass) => pass,
            SearchOutcome::Exhausted => {
                return Err(GeometryErrorKind::unrepresentable_output(operation.name()));
            },
        };
    assert!(pass.searched, "fixed S must force a real endpoint repair");
    let assigned_b = pass.coordinates.xy(target)?;
    assert!(
        assigned_b != nearest.xy(target)?,
        "the assigned endpoint must move away from S"
    );
    assert_eq!(
        assigned_b.x.to_bits(),
        180.0_f64.next_down().to_bits(),
        "the accepted endpoint is the admitted left neighbor of 180"
    );
    assert_eq!(assigned_b.y.to_bits(), 1.0_f64.to_bits());

    let sample_node = *problem
        .publication_sample_nodes
        .get(sample_index)
        .ok_or_else(|| GeometryErrorKind::Invalid("M has no frozen identity".into()))?;
    let adjacent_chords = problem
        .publication_chords
        .iter()
        .enumerate()
        .filter(|(_, chord)| {
            chord.recipe == sample_recipe && chord.exact.contains(&sample.exact_point)
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    assert_eq!(
        adjacent_chords.len(),
        2,
        "M remains the shared endpoint of both frozen publication chords"
    );
    assert!(
        adjacent_chords
            .iter()
            .all(|&index| problem
                .publication_chords
                .get(index)
                .is_some_and(|chord| chord
                    .nodes
                    .contains(&super::QGraphNode::Published(sample_node)))),
        "both frozen chords retain M's stable publication-node identity"
    );
    let result = sealed_shape(pass.output)?;
    assert!(
        crate::geometry::validate_shape_in_frame(
            &result,
            crate::geometry::TopologyDomain::Geographic(GeographicFrame::crs84()),
        )?
        .is_none(),
        "the repaired union remains valid"
    );
    let mut output_lines = Vec::new();
    result.try_for_each_component(&mut |component| -> Result<()> {
        if let crate::geometry::Component::LineString(line) = component {
            output_lines.push(line.points().collect::<Vec<_>>());
        }
        Ok(())
    })?;
    let accepted_line = output_lines
        .iter()
        .find(|points| {
            points.len() == 3
                && points.first().is_some_and(|point| {
                    point.x.to_bits() == 0.0_f64.to_bits() && point.y.to_bits() == 1.0_f64.to_bits()
                })
                && points.last().is_some_and(|point| {
                    point.x.to_bits() == assigned_b.x.to_bits()
                        && point.y.to_bits() == assigned_b.y.to_bits()
                })
        })
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("the final assigned A→B line is missing its midpoint".into())
        })?;
    let stored_m = *accepted_line
        .get(1)
        .ok_or_else(|| GeometryErrorKind::Invalid("final line has no M vertex".into()))?;
    let stored_m_key = super::emitted_node_key(topology.sources.frame, stored_m.xy())?;
    let sample_domain = problem
        .publication_domains
        .get(sample_node.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("M has no admitted output domain".into()))?;
    let mut stored_m_is_admitted = false;
    for candidate in sample_domain {
        if super::emitted_node_key(topology.sources.frame, candidate.native)? == stored_m_key {
            stored_m_is_admitted = true;
            break;
        }
    }
    assert!(
        stored_m_is_admitted,
        "final storage must retain M at an admitted image of its frozen identity"
    );
    assert_eq!(
        accepted_line
            .windows(2)
            .filter(|pair| pair.iter().any(|point| point.xy() == stored_m.xy()))
            .count(),
        2,
        "the stored midpoint remains shared by both adjacent output leaves"
    );
    let mut fixed_s_preserved = false;
    for point in output_lines.iter().flatten() {
        fixed_s_preserved |=
            super::emitted_node_key(topology.sources.frame, point.xy())? == fixed_s_key;
    }
    assert!(
        fixed_s_preserved,
        "the fixed source endpoint S remains stored"
    );
    Ok(())
}
