use super::*;

pub(in crate::geometry) fn publish_polygon_with_edge_roles(
    polygon: &Polygon,
    computed_edges: &[Vec<bool>],
    frame: GeographicFrame,
    budget: &mut ExpansionBudget,
) -> Result<Polygon> {
    publish_polygon_with_edge_roles_inner(polygon, computed_edges, frame, budget, None)
}

pub(crate) fn publish_polygon_with_edge_roles_and_context(
    polygon: &Polygon,
    computed_edges: &[Vec<bool>],
    frame: GeographicFrame,
    source_context: &SourceArcContext<'_, '_, crate::crs::GeodesicMetric>,
    budget: &mut ExpansionBudget,
) -> Result<Polygon> {
    publish_polygon_with_edge_roles_inner(
        polygon,
        computed_edges,
        frame,
        budget,
        Some(source_context),
    )
}

fn publish_polygon_with_edge_roles_inner(
    polygon: &Polygon,
    computed_edges: &[Vec<bool>],
    frame: GeographicFrame,
    budget: &mut ExpansionBudget,
    source_context: Option<&SourceArcContext<'_, '_, crate::crs::GeodesicMetric>>,
) -> Result<Polygon> {
    if polygon.rings().count() != computed_edges.len() {
        return Err(GeometryErrorKind::message(
            "computed edge roles do not match the polygon rings",
        ));
    }
    let mut published_rings = Vec::with_capacity(computed_edges.len());
    for (ring, computed_edges) in polygon.rings().zip(computed_edges) {
        let points = ring.coords();
        if computed_edges.len() != points.coord_count().saturating_sub(1) {
            return Err(GeometryErrorKind::message(
                "computed edge roles do not match a closed ring",
            ));
        }
        if computed_edges.is_empty() {
            published_rings.push(ring.clone());
            continue;
        }
        let edge_count = computed_edges.len();
        let intent = |index, start: crate::geometry::Point, end: crate::geometry::Point| {
            if *computed_edges
                .get(index)
                .expect("publication edge belongs to its polygon role inventory")
            {
                ComputedEdgeIntent::retained_binary64_pair(frame.longitude(), [start.x, end.x])
            } else {
                let delta = frame
                    .longitude()
                    .chart_step(start.x, end.x)
                    .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?
                    .delta(frame.longitude());
                Ok(ComputedEdgeIntent::preserved(delta == 0.0))
            }
        };
        let vertex_intent = |index: usize, _: crate::geometry::Point| {
            let touches_preserved_edge = if index == 0 || index == edge_count {
                !*computed_edges.first().expect("nonempty role inventory")
                    || !*computed_edges.last().expect("nonempty role inventory")
            } else {
                !*computed_edges
                    .get(index.wrapping_sub(1))
                    .expect("incoming vertex edge belongs to its polygon role inventory")
                    || !*computed_edges
                        .get(index)
                        .expect("outgoing vertex edge belongs to its polygon role inventory")
            };
            if touches_preserved_edge {
                ComputedVertexIntent::Preserved
            } else {
                ComputedVertexIntent::Canonical
            }
        };
        let published = match source_context {
            Some(source_context) => publish_native_computed_chain(
                frame,
                points,
                true,
                budget,
                source_context,
                intent,
                vertex_intent,
            )?,
            None => publish_computed_chain_with_vertex_intents(
                frame,
                points,
                true,
                budget,
                intent,
                vertex_intent,
            )?,
        };
        published_rings.push(match published {
            Some(points) => Ring::try_from_closed(CoordSeq::try_from_points(&points)?)?,
            None => ring.clone(),
        });
    }
    let mut rings = published_rings.into_iter();
    let shell = rings
        .next()
        .ok_or_else(|| GeometryErrorKind::message("a polygon has an exterior ring"))?;
    Ok(Polygon::new(shell, rings.collect()))
}

pub(crate) fn publish_polygon_parts_with_edge_roles(
    polygons: &[Polygon],
    edge_roles: &[Vec<Vec<bool>>],
    frame: GeographicFrame,
    budget: &mut ExpansionBudget,
) -> Result<Vec<Polygon>> {
    if polygons.len() != edge_roles.len() {
        return Err(GeometryErrorKind::message(
            "computed edge roles do not match the polygon parts",
        ));
    }
    polygons
        .iter()
        .zip(edge_roles)
        .map(|(polygon, roles)| publish_polygon_with_edge_roles(polygon, roles, frame, budget))
        .collect()
}
