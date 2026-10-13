#[test]
fn source_ring_rotation_keeps_publication_samples_with_their_recipes() -> Result<()> {
    use super::super::emission::{
        FeatureFamily, FeatureFamilyParent, FragmentRecipeId, PublishedFragment,
        PublishedSample, SampleProducerKind, SourceTrace,
    };
    use crate::geometry::ordinates::RequestRowId;

    let shape = crate::io::parse_wkt("POLYGON ((0 0, 10 0, 10 10, 0 0))")?;
    let topology = test_topology(&[shape]);
    let source_ring =
        topology.sources.rings.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("sampled source ring is missing".into())
        })?;
    let mut vertices = Vec::with_capacity(source_ring.source_edges.len().saturating_add(1));
    for edge_index in source_ring.source_edges.clone() {
        let edge = topology.sources.edges.get(edge_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("sampled source ring edge is unknown".into())
        })?;
        let port_id = topology
            .q
            .source_edge_ports
            .get(edge.id.0 as usize)
            .and_then(|ports| ports.first())
            .copied()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("sampled source ring start port is missing".into())
            })?;
        let port = topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("sampled source ring start port is unknown".into())
        })?;
        vertices.push(EmittedVertex {
            port,
            point: edge.source[0],
            literal_image: QSourceImage::Port(port_id),
        });
    }
    let last_edge = topology
        .sources
        .edges
        .get(source_ring.source_edges.end.checked_sub(1).ok_or_else(|| {
            GeometryErrorKind::Invalid("sampled source ring has no closing edge".into())
        })?)
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("sampled source ring closing edge is unknown".into())
        })?;
    let end_port_id = topology
        .q
        .source_edge_ports
        .get(last_edge.id.0 as usize)
        .and_then(|ports| ports.get(1))
        .copied()
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("sampled source ring end port is missing".into())
        })?;
    let end_port = topology
        .q
        .ports
        .get(end_port_id.0 as usize)
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("sampled source ring end port is unknown".into())
        })?;
    vertices.push(EmittedVertex {
        port: end_port,
        point: last_edge.source[1],
        literal_image: QSourceImage::Port(end_port_id),
    });
    let edge_count = source_ring.source_edges.len();
    if edge_count != 3 {
        return Err(GeometryErrorKind::Invalid(
            "sampled source-ring fixture has an unexpected edge count".into(),
        )
        .into());
    }
    let start_port = vertices
        .get(2)
        .ok_or_else(|| GeometryErrorKind::Invalid("rotated ring start is missing".into()))?
        .port
        .id;
    let mut work = crate::work::Unmetered;
    let (rotated_vertices, recipe_rotation) =
        rotate_area_ring_vertices(&vertices, Some(start_port), &mut work)?.ok_or_else(
            || GeometryErrorKind::Invalid("ring rotation was not applied".into()),
        )?;
    assert_eq!(recipe_rotation, 2);

    let recipes = (0..edge_count)
        .map(|index| FragmentRecipeId(u32::try_from(index).expect("three recipe IDs")))
        .collect::<Vec<_>>();
    let owner = RecipeWalkOwner::SourceRing(source_ring.id.0 as usize);
    let walk_rows = [RecipeWalk {
        owner,
        fragments: recipes.clone(),
    }];
    let walks = index_recipe_walks(&walk_rows, &mut work)?;
    let mut fragments = Vec::with_capacity(edge_count);
    let mut samples = Vec::with_capacity(edge_count);
    let mut assigned_samples = Vec::with_capacity(edge_count);
    for (index, recipe) in recipes.iter().copied().enumerate() {
        let start = vertices.get(index).ok_or_else(|| {
            GeometryErrorKind::Invalid("sampled source recipe start is missing".into())
        })?;
        let end = vertices.get(index + 1).ok_or_else(|| {
            GeometryErrorKind::Invalid("sampled source recipe end is missing".into())
        })?;
        let source_sample = Point::new_raw_xy(
            f64::midpoint(start.point.x, end.point.x),
            f64::midpoint(start.point.y, end.point.y),
        );
        let assigned = XY::new(source_sample.x + 0.125, source_sample.y + 0.125);
        let exact_start = ExactChartPoint::from_rationals(
            ExactRational::from_f64(start.point.x),
            ExactRational::from_f64(start.point.y),
        );
        let exact_end = ExactChartPoint::from_rationals(
            ExactRational::from_f64(end.point.x),
            ExactRational::from_f64(end.point.y),
        );
        let exact_sample = ExactChartPoint::from_rationals(
            ExactRational::from_f64(source_sample.x),
            ExactRational::from_f64(source_sample.y),
        );
        fragments.push(PublishedFragment {
            recipe,
            points: vec![start.point, source_sample, end.point],
            trace: SourceTrace::from_edge_rows([Vec::new()], &mut work)?,
            sample_range: index..index + 1,
            source_records: [None, None],
            exact_endpoints: [exact_start, exact_end],
        });
        samples.push(PublishedSample {
            request_row: RequestRowId::from_index(index)?,
            family: FeatureFamily {
                parent: FeatureFamilyParent::Fragment(recipe),
                producer: SampleProducerKind::Publication,
                occurrence: u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?,
            },
            parameter: ExactRational::from_f64(0.5),
            written_vertex: 1,
            point: source_sample,
            exact_point: exact_sample.clone(),
            published_chart_point: exact_sample,
            source_trace: None,
            endpoint_only: false,
        });
        assigned_samples.push(assigned);
    }
    let sample_ordinates = vec![NodeOrdinates::default(); samples.len()];
    let publication = PublicationInventory {
        fragments,
        samples,
        counts: Vec::new(),
        ordinates: sample_ordinates.clone(),
    };
    let source_ordinates = vec![NodeOrdinates::default(); topology.q.nodes.len()];
    let source_publication = SourcePublication {
        inventory: &publication,
        points: &assigned_samples,
        ordinates: &sample_ordinates,
        live_samples: &[true; 3],
    };
    let mut cursors = BTreeMap::new();
    let path_context = SourcePathContext {
        ordinates: &source_ordinates,
        source_ports: None,
        publication: Some(source_publication),
        walks: Some(&walks),
        axes: CoordinateAxes::XY,
        diagnostic_axes: None,
    };

    let (ring, locations) = write_ring(
        &rotated_vertices,
        0,
        owner,
        recipe_rotation,
        &path_context,
        &mut cursors,
        &mut work,
    )?;

    let ring_points = ring.coords().iter().collect::<Vec<_>>();
    assert_eq!(ring_points.len(), 7);
    let written_samples = locations
        .images
        .iter()
        .zip(&ring_points)
        .filter_map(|(image, point)| match image {
            QSourceImage::PublicationSample(sample) => {
                Some((*sample as usize, point.x, point.y))
            },
            QSourceImage::Port(_)
            | QSourceImage::SourcePoint(_)
            | QSourceImage::SourceRingStart(_)
            | QSourceImage::SourceRingEnd(_)
            | QSourceImage::CollapsedStart(_)
            | QSourceImage::CollapsedEnd(_) => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(written_samples, vec![
        (2, assigned_samples[2].x, assigned_samples[2].y),
        (0, assigned_samples[0].x, assigned_samples[0].y),
        (1, assigned_samples[1].x, assigned_samples[1].y),
    ]);
    Ok(())
}
