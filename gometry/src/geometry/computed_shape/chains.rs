use super::*;

/// Rebuild shape structure while mapping each line or ring coordinate chain.
///
/// `source` carries producer-side data (for example the projected segment
/// chain), while `output` carries the published coordinate sequence. They
/// must have matching geometry and chain cardinality.
pub(crate) fn map_computed_shape_chains(
    source: &Shape,
    output: &Shape,
    mut map: impl FnMut(&CoordSeq, &CoordSeq, bool) -> Result<Option<Vec<crate::geometry::Point>>>,
) -> Result<Shape> {
    map_shape_chains(source, output, &mut map)
}

pub(crate) fn publish_native_computed_chain<'intent, 'metric, P, M>(
    frame: GeographicFrame,
    points: &P,
    closed_ring: bool,
    budget: &mut ExpansionBudget,
    source_context: &SourceArcContext<'_, 'metric, M>,
    intent: impl Fn(
        usize,
        crate::geometry::Point,
        crate::geometry::Point,
    ) -> Result<ComputedEdgeIntent<'intent>>,
    vertex_intent: impl Fn(usize, crate::geometry::Point) -> ComputedVertexIntent,
) -> Result<Option<Vec<crate::geometry::Point>>>
where
    P: crate::geometry::Coordinates + ?Sized,
    M: ChartMetric + 'metric,
{
    publish_native_computed_chain_inner(
        frame,
        points,
        closed_ring,
        budget,
        source_context,
        intent,
        vertex_intent,
    )
}

fn publish_native_computed_chain_inner<'intent, 'metric, P, M>(
    frame: GeographicFrame,
    points: &P,
    closed_ring: bool,
    budget: &mut ExpansionBudget,
    source_context: &SourceArcContext<'_, 'metric, M>,
    intent: impl Fn(
        usize,
        crate::geometry::Point,
        crate::geometry::Point,
    ) -> Result<ComputedEdgeIntent<'intent>>,
    vertex_intent: impl Fn(usize, crate::geometry::Point) -> ComputedVertexIntent,
) -> Result<Option<Vec<crate::geometry::Point>>>
where
    P: crate::geometry::Coordinates + ?Sized,
    M: ChartMetric + 'metric,
{
    #[cfg(test)]
    crate::geometry::record_root_work(crate::geometry::RootWorkEvent::NativeChainEntry);
    let metric_access = source_context.metric_access();
    let axes = points
        .first_coord()
        .map_or(crate::geometry::CoordinateAxes::XY, |point| point.axes);
    let mut active_root = None;
    let mut root_metric_state = RootMetricState::default();
    let mut work = Unmetered;
    publish_computed_chain_with_vertex_intents_and_samples(
        frame,
        points,
        closed_ring,
        budget,
        intent,
        vertex_intent,
        |root, endpoints, relation, parameter, _retained| {
            if active_root != Some(root) {
                active_root = Some(root);
                root_metric_state = RootMetricState::default();
            }
            let ComputedLongitudeRelation::Native(longitude) = *relation else {
                return Err(GeometryErrorKind::message(
                    "computed Native output requires a retained Native chart edge",
                ));
            };
            computed_native_sample_ordinates(
                frame,
                endpoints,
                longitude,
                parameter,
                axes,
                metric_access,
                &mut root_metric_state,
                &mut work,
            )
        },
        |_, _, _, _, _, _| Ok(()),
    )
}

fn map_shape_chains(
    source: &Shape,
    output: &Shape,
    map: &mut impl FnMut(&CoordSeq, &CoordSeq, bool) -> Result<Option<Vec<crate::geometry::Point>>>,
) -> Result<Shape> {
    Ok(match (source, output) {
        (Shape::LineString(source), Shape::LineString(output)) => Shape::LineString(map_line(
            source,
            output,
            source.len() > 2 && source.first() == source.last(),
            map,
        )?),
        (Shape::MultiLineString(source), Shape::MultiLineString(output)) => {
            let mut source_lines = source.iter();
            let mapped = output.try_map_values(output.axes(), |line| {
                let source = source_lines.next().ok_or_else(|| {
                    GeometryErrorKind::message(
                        "computed output changed a multiline coordinate structure",
                    )
                })?;
                map_line(
                    source,
                    line,
                    source.len() > 2 && source.first() == source.last(),
                    map,
                )
            })?;
            if source_lines.next().is_some() {
                return Err(GeometryErrorKind::message(
                    "computed output changed a multiline coordinate structure",
                ));
            }
            Shape::MultiLineString(mapped)
        },
        (Shape::Polygon(source), Shape::Polygon(output)) => {
            Shape::Polygon(map_polygon(source, output, map)?)
        },
        (Shape::MultiPolygon(source), Shape::MultiPolygon(output)) => {
            let mut source_polygons = source.iter();
            let mapped = output.try_map_values(output.axes(), |polygon| {
                let source = source_polygons.next().ok_or_else(|| {
                    GeometryErrorKind::message(
                        "computed output changed a multipolygon coordinate structure",
                    )
                })?;
                map_polygon(source, polygon, map)
            })?;
            if source_polygons.next().is_some() {
                return Err(GeometryErrorKind::message(
                    "computed output changed a multipolygon coordinate structure",
                ));
            }
            Shape::MultiPolygon(mapped)
        },
        (Shape::GeometryCollection(source), Shape::GeometryCollection(output)) => {
            let mut source_members = source.iter();
            let mapped = output.try_map_members(|member| {
                let source = source_members.next().ok_or_else(|| {
                    GeometryErrorKind::message(
                        "computed output changed a geometry-collection structure",
                    )
                })?;
                map_shape_chains(source, member, map)
            })?;
            if source_members.next().is_some() {
                return Err(GeometryErrorKind::message(
                    "computed output changed a geometry-collection structure",
                ));
            }
            Shape::GeometryCollection(mapped)
        },
        (Shape::Point(_), Shape::Point(_))
        | (Shape::MultiPoint(_), Shape::MultiPoint(_))
        | (Shape::Empty(..), Shape::Empty(..)) => output.clone(),
        _ => {
            return Err(GeometryErrorKind::message(
                "computed output changed geometry structure",
            ));
        },
    })
}

fn map_line(
    source: &LineSeq,
    output: &LineSeq,
    closed: bool,
    map: &mut impl FnMut(&CoordSeq, &CoordSeq, bool) -> Result<Option<Vec<crate::geometry::Point>>>,
) -> Result<LineSeq> {
    let coords = map(source.as_coords(), output.as_coords(), closed)?;
    coords.map_or_else(
        || Ok(output.clone()),
        |points| Ok(LineSeq::from_proven(CoordSeq::try_from_points(&points)?)),
    )
}

fn map_polygon(
    source: &Polygon,
    output: &Polygon,
    map: &mut impl FnMut(&CoordSeq, &CoordSeq, bool) -> Result<Option<Vec<crate::geometry::Point>>>,
) -> Result<Polygon> {
    let mut source_rings = source.rings();
    let mut rings = Vec::with_capacity(output.rings().count());
    for ring in output.rings() {
        let source_ring = source_rings.next().ok_or_else(|| {
            GeometryErrorKind::message("computed output changed a polygon ring structure")
        })?;
        rings.push(map_ring(source_ring, ring, map)?);
    }
    if source_rings.next().is_some() {
        return Err(GeometryErrorKind::message(
            "computed output changed a polygon ring structure",
        ));
    }
    let mut rings = rings.into_iter();
    let shell = rings
        .next()
        .ok_or_else(|| GeometryErrorKind::message("a polygon has an exterior ring"))?;
    Ok(Polygon::new(shell, rings.collect()))
}

fn map_ring(
    source: &Ring,
    output: &Ring,
    map: &mut impl FnMut(&CoordSeq, &CoordSeq, bool) -> Result<Option<Vec<crate::geometry::Point>>>,
) -> Result<Ring> {
    map(source.coords(), output.coords(), true)?.map_or_else(
        || Ok(output.clone()),
        |points| Ring::try_from_closed(CoordSeq::try_from_points(&points)?),
    )
}
