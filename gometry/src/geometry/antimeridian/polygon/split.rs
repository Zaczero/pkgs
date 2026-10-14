use super::super::publication::restriction_for_edge;
use super::assemble::{
    KeptHole, SeamFragment, assemble_fragments, extend_over_poles_fragments, polygons_from_shape,
};
use super::*;
use crate::error::ResourceError;
use crate::geometry::antimeridian::linestring::{SeamPiece, SeamVertexSource};
use crate::geometry::overlay::OverlayPolygonPartWithSources;
use crate::work::{Unmetered, Work};

/// A pole-closure cap fabricates ±90 vertices with no source segment, so a
/// measured/3D input cannot honestly keep its ordinates there. Reject Z/M only
/// when such a cap vertex ACTUALLY survived into the assembled output — a plain
/// seam crossing (no pole) fabricates none and its seam vertices interpolate
/// fine, so it must not be rejected. (The check is post-assembly because the
/// split speculatively explores both pole closures before discarding the cap.)
#[expect(
    clippy::float_cmp,
    reason = "cap vertices are the exact literals ±90; any other latitude is real data"
)]
fn reject_fabricated_pole_ordinates(source: &Shape, polygons: &[Polygon]) -> Result<()> {
    if source.axes() == CoordinateAxes::XY {
        return Ok(());
    }
    let fabricated_pole_vertex = polygons.iter().any(|polygon| {
        std::iter::once(polygon.shell.coords())
            .chain(polygon.holes.iter().map(Ring::coords))
            .any(|ring| {
                ring.iter_coords()
                    .any(|coord| coord.y == 90.0 || coord.y == -90.0)
            })
    });
    if fabricated_pole_vertex {
        Err(GeometryErrorKind::AntimeridianPoleOrdinates.into())
    } else {
        Ok(())
    }
}

pub(super) fn ring_is_ccw(coords: &[Point]) -> bool {
    if coords.len() >= 2
        && coords
            .first()
            .zip(coords.last())
            .is_some_and(|(first, last)| same_point(*first, *last))
    {
        ring_winding(coords).is_ccw()
    } else {
        open_point_cycle_winding(coords).is_ccw()
    }
}

/// Split one polygon into its seam-following parts with silent winding fixes.
/// `preferred` forces the pole-closure direction (the caller knows which pole
/// the ring encircles); `Auto` infers it and retries the other direction only
/// if the inference is invalid. Z/M is rejected only when a pole cap actually
/// survives into the output (see [`reject_fabricated_pole_ordinates`]).
pub(super) fn split_polygon<M: ChartMetric>(
    source: &Shape,
    polygon: &Polygon,
    preferred: PoleClosure,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<Vec<Polygon>> {
    let parts = split_polygon_core(
        source,
        polygon,
        None,
        preferred,
        HoleOverlayMode::Select,
        GeographicFrame::crs84(),
        metric_access,
        None,
    )?;
    let polygons = parts
        .into_iter()
        .map(|(polygon, _)| polygon)
        .collect::<Vec<_>>();
    reject_fabricated_pole_ordinates(source, &polygons)?;
    Ok(polygons)
}

pub(super) fn seam_piece_lineage(
    piece: &SeamPiece,
    source: &super::super::lineage::LineageSequence,
    source_closed: bool,
    catalog: &mut super::super::lineage::LineageCatalog,
) -> Result<super::super::lineage::LineageSequence> {
    fn source_parameters(
        sources: &[SeamVertexSource],
        edge_count: usize,
        source_closed: bool,
        work: &mut impl Work,
    ) -> Result<Vec<(usize, ExactRational)>> {
        let mut parameters = Vec::new();
        for source in sources {
            match source {
                SeamVertexSource::InputVertex(index) if source_closed && *index == edge_count => {
                    let last_edge = edge_count
                        .checked_sub(1)
                        .ok_or(ResourceError::SizeOverflow)?;
                    parameters.push((last_edge, ExactRational::one()));
                    parameters.push((0, ExactRational::zero()));
                },
                SeamVertexSource::InputVertex(index) => {
                    if *index < edge_count {
                        parameters.push((*index, ExactRational::zero()));
                    }
                    if *index > 0 {
                        let previous_edge =
                            index.checked_sub(1).ok_or(ResourceError::SizeOverflow)?;
                        parameters.push((previous_edge, ExactRational::one()));
                    }
                },
                SeamVertexSource::InputEdge { edge, parameter } => {
                    parameters.push((*edge, parameter.try_clone_with_work(work)?));
                },
            }
        }
        Ok(parameters)
    }

    fn append_event(
        output: &mut Vec<super::super::lineage::EventId>,
        event: super::super::lineage::EventId,
    ) {
        if !output.contains(&event) {
            output.push(event);
        }
    }

    let member = source.member;
    let edge_count = source.edges.len();
    let mut vertices = Vec::with_capacity(piece.points.len());
    for sources in &piece.vertices {
        let mut events = Vec::new();
        for source_use in sources {
            match source_use {
                SeamVertexSource::InputVertex(index) => {
                    let source_events = source.vertices.get(*index).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "seam fragment references an unknown source vertex".into(),
                        )
                    })?;
                    for &event in source_events {
                        append_event(&mut events, event);
                    }
                },
                SeamVertexSource::InputEdge { edge, parameter } => {
                    let source_edge = source.edges.get(*edge).copied().ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "seam fragment references an unknown source edge".into(),
                        )
                    })?;
                    let event =
                        catalog.event_from_edge_parameters(member, [(source_edge, parameter)])?;
                    append_event(&mut events, event);
                },
            }
        }
        if events.is_empty() {
            events.push(catalog.event(member, Vec::new())?);
        } else if events.len() > 1 {
            events = vec![catalog.combine_events(member, &events)?];
        }
        vertices.push(events);
    }

    let mut edges = Vec::with_capacity(piece.points.len().saturating_sub(1));
    let mut work = Unmetered;
    for (index, &[start_point, end_point]) in piece.points.array_windows::<2>().enumerate() {
        let end_index = index.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        let starts = source_parameters(
            piece
                .vertices
                .get(index)
                .ok_or_else(|| GeometryErrorKind::Invalid("seam vertex is unavailable".into()))?,
            edge_count,
            source_closed,
            &mut work,
        );
        let ends = source_parameters(
            piece.vertices.get(end_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("seam edge end vertex is unavailable".into())
            })?,
            edge_count,
            source_closed,
            &mut work,
        )?;
        let starts = starts?;
        let mut source_uses = Vec::new();
        for (edge, start) in &starts {
            for (end_edge, end) in &ends {
                if edge == end_edge && !start.try_eq_with_work(end, &mut work)? {
                    let use_id = source.edges.get(*edge).copied().ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "seam edge references an unknown source interval".into(),
                        )
                    })?;
                    source_uses.push((
                        use_id,
                        start.try_clone_with_work(&mut work)?,
                        end.try_clone_with_work(&mut work)?,
                    ));
                }
            }
        }
        let restriction = if source_uses.is_empty() {
            restriction_for_edge(start_point, end_point)
        } else {
            None
        };
        edges.push(
            catalog.compose_edge_uses(
                member,
                source_uses
                    .iter()
                    .map(|(edge, start, end)| (*edge, start, end)),
                restriction,
            )?,
        );
    }
    Ok(super::super::lineage::LineageSequence {
        member,
        vertices,
        edges,
    })
}

pub(super) fn polygon_parts_shape(
    parts: Vec<(Polygon, Option<super::super::lineage::LineagePolygon>)>,
    axes: CoordinateAxes,
    captured: bool,
) -> Result<(Shape, Option<super::super::lineage::ShapeLineage>)> {
    let mut polygons = Vec::with_capacity(parts.len());
    let mut lineages = Vec::with_capacity(parts.len());
    for (polygon, lineage) in parts {
        polygons.push(polygon);
        lineages.push(lineage);
    }
    let shape = Shape::narrow_polygons(axes, polygons);
    if !captured {
        return Ok((shape, None));
    }
    let lineage = match lineages.len() {
        0 => ShapeLineage::Empty,
        1 => ShapeLineage::Polygon(lineages.pop().flatten().ok_or_else(|| {
            GeometryErrorKind::Invalid("split polygon part has no member lineage".into())
        })?),
        _ => ShapeLineage::MultiPolygon {
            axes,
            members: lineages,
        },
    };
    Ok((shape, Some(lineage)))
}

pub(super) fn split_polygon_member<M: ChartMetric>(
    source: &Shape,
    polygon: &Polygon,
    lineage: Option<&super::super::lineage::LineagePolygon>,
    source_frame: GeographicFrame,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    catalog: Option<&mut super::super::lineage::LineageCatalog>,
) -> Result<Vec<(Polygon, Option<super::super::lineage::LineagePolygon>)>> {
    match lineage {
        Some(lineage) => {
            let catalog = catalog.ok_or_else(|| {
                GeometryErrorKind::Invalid("captured polygon lineage has no event catalog".into())
            })?;
            split_polygon_with_lineage(
                source,
                polygon,
                lineage,
                PoleClosure::Auto,
                HoleOverlayMode::Select,
                source_frame,
                metric_access,
                catalog,
            )
            .map(|parts| {
                parts
                    .into_iter()
                    .map(|part| (part.polygon, Some(part.lineage)))
                    .collect()
            })
        },
        None => split_polygon(source, polygon, PoleClosure::Auto, metric_access)
            .map(|parts| parts.into_iter().map(|polygon| (polygon, None)).collect()),
    }
}

fn append_polygon_edge_uses(
    lineage: &super::super::lineage::LineagePolygon,
    output: &mut Vec<super::super::lineage::EdgeUseId>,
) {
    output.extend(lineage.shell.edges.iter().copied());
    for hole in &lineage.holes {
        output.extend(hole.edges.iter().copied());
    }
}

fn append_shape_edge_uses(
    lineage: &super::super::lineage::ShapeLineage,
    output: &mut Vec<super::super::lineage::EdgeUseId>,
) -> Result<()> {
    match lineage {
        ShapeLineage::Polygon(polygon) => append_polygon_edge_uses(polygon, output),
        ShapeLineage::MultiPolygon { members, .. } => {
            for polygon in members.iter().flatten() {
                append_polygon_edge_uses(polygon, output);
            }
        },
        ShapeLineage::GeometryCollection(members) => {
            for member in members.iter().flatten() {
                append_shape_edge_uses(member, output)?;
            }
        },
        ShapeLineage::Point { .. }
        | ShapeLineage::MultiPoint { .. }
        | ShapeLineage::LineString(_)
        | ShapeLineage::MultiLineString { .. }
        | ShapeLineage::Empty => {
            return Err(GeometryErrorKind::Invalid(
                "lineage-aware polygon overlay received non-polygonal input".into(),
            )
            .into());
        },
    }
    Ok(())
}

fn overlay_ring_lineage<M: ChartMetric>(
    part: &OverlayPolygonPartWithSources,
    ring_index: usize,
    axes: CoordinateAxes,
    source_edges: &[super::super::lineage::EdgeUseId],
    member: super::super::lineage::MemberId,
    source_frame: GeographicFrame,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    catalog: &mut super::super::lineage::LineageCatalog,
) -> Result<(Ring, super::super::lineage::LineageSequence)> {
    let vertices = part.vertices.get(ring_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("overlay polygon has no source-node ring".into())
    })?;
    let edge_sources = part.edge_sources.get(ring_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("overlay polygon has no source-edge ring".into())
    })?;
    if vertices.len() != edge_sources.len() {
        return Err(GeometryErrorKind::Invalid(
            "overlay polygon source nodes and edges disagree".into(),
        )
        .into());
    }
    let coordinates = if ring_index == 0 {
        part.polygon.shell.coords()
    } else {
        let hole_index = ring_index
            .checked_sub(1)
            .ok_or(ResourceError::SizeOverflow)?;
        part.polygon
            .holes
            .get(hole_index)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("overlay polygon source hole is unknown".into())
            })?
            .coords()
    };
    let points = coordinates.iter_coords().collect::<Vec<_>>();
    let vertex_count = vertices.len();
    if points.len() != vertex_count.saturating_add(1) {
        return Err(GeometryErrorKind::Invalid(
            "overlay polygon source-node count does not match its ring".into(),
        )
        .into());
    }
    let mut lineage_vertices = Vec::with_capacity(vertex_count);
    let mut output_points = Vec::with_capacity(vertex_count.saturating_add(1));
    for (index, point) in points.iter().copied().take(vertex_count).enumerate() {
        let previous = if index == 0 {
            vertex_count
                .checked_sub(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?
        } else {
            index
                .checked_sub(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?
        };
        let mut uses = Vec::new();
        for source in edge_sources
            .get(previous)
            .ok_or_else(|| GeometryErrorKind::Invalid("overlay edge use is unknown".into()))?
        {
            uses.push((
                *source_edges.get(source.source_id as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "overlay edge use references an unknown split edge".into(),
                    )
                })?,
                source.end_parameter.try_clone_with_work(&mut Unmetered)?,
            ));
        }
        for source in edge_sources
            .get(index)
            .ok_or_else(|| GeometryErrorKind::Invalid("overlay edge use is unknown".into()))?
        {
            uses.push((
                *source_edges.get(source.source_id as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "overlay edge use references an unknown split edge".into(),
                    )
                })?,
                source.start_parameter.try_clone_with_work(&mut Unmetered)?,
            ));
        }
        let events = vec![catalog.event_from_edge_parameters(
            member,
            uses.iter().map(|(edge, parameter)| (*edge, parameter)),
        )?];
        let ordinates =
            catalog.resolve_vertex_ordinates(&events, axes, source_frame, metric_access)?;
        let z = if axes.has_z() {
            ordinates.z.value()
        } else {
            None
        };
        let m = if axes.has_m() {
            ordinates.m.value()
        } else {
            None
        };
        if (axes.has_z() && z.is_none()) || (axes.has_m() && m.is_none()) {
            return Err(
                GeometryErrorKind::OrdinateDropped("antimeridian split overlay".into()).into(),
            );
        }
        output_points.push(Point::new_axes(
            point.x,
            point.y,
            ZOrdinate(z),
            MOrdinate(m),
        )?);
        lineage_vertices.push(events);
    }
    if let Some(first) = output_points.first().copied() {
        output_points.push(first);
    }
    if let Some(first) = lineage_vertices.first().cloned() {
        lineage_vertices.push(first);
    }
    let output_ring = Ring::try_from_closed(CoordSeq::try_from_points(&output_points)?)?;

    let mut lineage_edges = Vec::with_capacity(vertex_count);
    for (index, uses) in edge_sources.iter().enumerate() {
        let mut sources = Vec::with_capacity(uses.len());
        for source in uses {
            sources.push((
                *source_edges.get(source.source_id as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "overlay edge use references an unknown split edge".into(),
                    )
                })?,
                source.start_parameter.try_clone_with_work(&mut Unmetered)?,
                source.end_parameter.try_clone_with_work(&mut Unmetered)?,
            ));
        }
        let next = if index
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?
            == vertex_count
        {
            0
        } else {
            index
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?
        };
        let restriction = if sources.is_empty() {
            let start = points.get(index).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("overlay output ring edge start is unavailable".into())
            })?;
            let end = points.get(next).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("overlay output ring edge end is unavailable".into())
            })?;
            restriction_for_edge(start, end)
        } else {
            None
        };
        lineage_edges.push(catalog.compose_edge_uses(
            member,
            sources.iter().map(|(edge, start, end)| (*edge, start, end)),
            restriction,
        )?);
    }
    Ok((output_ring, super::super::lineage::LineageSequence {
        member,
        vertices: lineage_vertices,
        edges: lineage_edges,
    }))
}

fn overlay_parts_shape<M: ChartMetric>(
    parts: Vec<OverlayPolygonPartWithSources>,
    axes: CoordinateAxes,
    member: super::super::lineage::MemberId,
    source_edges: &[super::super::lineage::EdgeUseId],
    source_frame: GeographicFrame,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    catalog: &mut super::super::lineage::LineageCatalog,
) -> Result<(Shape, super::super::lineage::ShapeLineage)> {
    let mut polygons = Vec::with_capacity(parts.len());
    let mut lineages = Vec::with_capacity(parts.len());
    for part in parts {
        let mut rings = Vec::with_capacity(part.vertices.len());
        for index in 0..part.vertices.len() {
            rings.push(overlay_ring_lineage(
                &part,
                index,
                axes,
                source_edges,
                member,
                source_frame,
                metric_access,
                catalog,
            )?);
        }
        let mut rings = rings.into_iter();
        let (shell, shell_lineage) = rings
            .next()
            .ok_or_else(|| GeometryErrorKind::Invalid("overlay polygon has no shell".into()))?;
        let mut holes = Vec::new();
        let mut hole_lineages = Vec::new();
        for (hole, lineage) in rings {
            holes.push(hole);
            hole_lineages.push(lineage);
        }
        polygons.push(Polygon::new(shell, holes));
        lineages.push(Some(super::super::lineage::LineagePolygon {
            member,
            shell: shell_lineage,
            holes: hole_lineages,
        }));
    }
    match polygons.len() {
        0 => Ok((
            Shape::narrow_polygons(axes, polygons),
            super::super::lineage::ShapeLineage::Empty,
        )),
        1 => Ok((
            Shape::Polygon(
                polygons
                    .pop()
                    .ok_or(crate::error::ResourceError::SizeOverflow)?,
            ),
            super::super::lineage::ShapeLineage::Polygon(lineages.pop().flatten().ok_or_else(
                || GeometryErrorKind::Invalid("overlay polygon has no output lineage".into()),
            )?),
        )),
        _ => Ok((
            Shape::MultiPolygon(MultiPolygon::try_new(
                axes,
                polygons.into_iter().map(MultiMember::Value).collect(),
            )?),
            super::super::lineage::ShapeLineage::MultiPolygon {
                axes,
                members: lineages,
            },
        )),
    }
}

fn polygon_lineages(
    lineage: super::super::lineage::ShapeLineage,
    polygon_count: usize,
) -> Result<Vec<super::super::lineage::LineagePolygon>> {
    match lineage {
        super::super::lineage::ShapeLineage::Polygon(lineage) if polygon_count == 1 => {
            Ok(vec![lineage])
        },
        super::super::lineage::ShapeLineage::MultiPolygon { members, .. } => {
            let lineages = members.into_iter().flatten().collect::<Vec<_>>();
            if lineages.len() == polygon_count {
                Ok(lineages)
            } else {
                Err(GeometryErrorKind::Invalid(
                    "overlay polygon outputs and lineages disagree".into(),
                )
                .into())
            }
        },
        super::super::lineage::ShapeLineage::Empty if polygon_count == 0 => Ok(Vec::new()),
        super::super::lineage::ShapeLineage::Point { .. }
        | super::super::lineage::ShapeLineage::MultiPoint { .. }
        | super::super::lineage::ShapeLineage::LineString(_)
        | super::super::lineage::ShapeLineage::MultiLineString { .. }
        | super::super::lineage::ShapeLineage::Polygon(_)
        | super::super::lineage::ShapeLineage::GeometryCollection(_)
        | super::super::lineage::ShapeLineage::Empty => Err(GeometryErrorKind::Invalid(
            "overlay polygon output has the wrong lineage kind".into(),
        )
        .into()),
    }
}

fn overlay_hole_difference<M: ChartMetric>(
    operands: &[&Shape],
    quotient: bool,
    frame: GeographicFrame,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<Shape> {
    if quotient {
        match metric_access {
            Some(metric_access) => crate::geometry::overlay_all_with_metric(
                operands,
                TopologyDomain::Geographic(frame),
                crate::geometry::OverlayOp::Difference,
                Strictness::Strict,
                metric_access,
            ),
            None => crate::geometry::overlay_all(
                operands,
                TopologyDomain::Geographic(frame),
                crate::geometry::OverlayOp::Difference,
                Strictness::Strict,
            ),
        }
    } else {
        crate::geometry::overlay_all_geographic_chart(
            operands,
            frame,
            crate::geometry::OverlayOp::Difference,
            Strictness::Strict,
            metric_access,
        )
    }
}

fn split_polygon_core<M: ChartMetric>(
    source: &Shape,
    polygon: &Polygon,
    source_lineage: Option<&super::super::lineage::LineagePolygon>,
    preferred: PoleClosure,
    overlay_mode: HoleOverlayMode,
    source_frame: GeographicFrame,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    catalog: Option<&mut super::super::lineage::LineageCatalog>,
) -> Result<Vec<(Polygon, Option<super::super::lineage::LineagePolygon>)>> {
    let parts = split_polygon_parts_inner(
        source,
        polygon,
        source_lineage,
        preferred,
        overlay_mode,
        source_frame,
        metric_access,
        catalog,
    )?;
    if source_lineage.is_some() {
        let polygons = parts
            .iter()
            .map(|(polygon, _)| polygon.clone())
            .collect::<Vec<_>>();
        reject_fabricated_pole_ordinates(source, &polygons)?;
    }
    Ok(parts)
}

fn split_polygon_parts_inner<M: ChartMetric>(
    source: &Shape,
    polygon: &Polygon,
    source_lineage: Option<&super::super::lineage::LineagePolygon>,
    preferred: PoleClosure,
    mut overlay_mode: HoleOverlayMode,
    source_frame: GeographicFrame,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    mut catalog: Option<&mut super::super::lineage::LineageCatalog>,
) -> Result<Vec<(Polygon, Option<super::super::lineage::LineagePolygon>)>> {
    let preferred = match preferred {
        PoleClosure::Auto => pole_closure_for_ring(&polygon.shell),
        closure @ PoleClosure::Chart(_) => closure,
    };
    if source_lineage.is_some_and(|lineage| lineage.holes.len() != polygon.holes.len()) {
        return Err(GeometryErrorKind::Invalid(
            "antimeridian polygon holes and lineage disagree".into(),
        )
        .into());
    }
    let mut crossing_holes = Vec::new();
    let mut stationary_holes = Vec::new();
    for (index, hole) in polygon.holes.iter().enumerate() {
        let points = collect_points(hole.coords());
        let hole_lineage = source_lineage.and_then(|lineage| lineage.holes.get(index));
        match split_ring_at_seam_with_metric(&points, GeographicFrame::crs84(), metric_access)? {
            SeamRing::Whole(spelled) if !ring_follows_seam(&points) => {
                let lineage = match hole_lineage {
                    Some(lineage) => Some(seam_piece_lineage(
                        &spelled,
                        lineage,
                        true,
                        catalog.as_deref_mut().ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "captured polygon lineage has no event catalog".into(),
                            )
                        })?,
                    )?),
                    None => None,
                };
                stationary_holes.push((
                    Ring::try_from_closed(CoordSeq::try_from_points(&spelled.points)?)?,
                    lineage,
                ));
            },
            SeamRing::Pieces(_) | SeamRing::Whole(_) => {
                crossing_holes.push((hole.clone(), hole_lineage.cloned()));
            },
        }
    }

    if !crossing_holes.is_empty() {
        let axes = polygon.shell.coords().axes();
        let shell_only = Polygon::new(
            polygon.shell.clone(),
            stationary_holes
                .iter()
                .map(|(hole, _)| hole.clone())
                .collect(),
        );
        let shell_only_lineage = if let Some(source_lineage) = source_lineage {
            let holes = stationary_holes
                .iter()
                .map(|(_, lineage)| lineage.clone())
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("captured stationary hole has no lineage".into())
                })?;
            Some(super::super::lineage::LineagePolygon {
                member: source_lineage.member,
                shell: source_lineage.shell.clone(),
                holes,
            })
        } else {
            None
        };
        let shell_parts = split_polygon_core(
            source,
            &shell_only,
            shell_only_lineage.as_ref(),
            preferred,
            overlay_mode,
            source_frame,
            metric_access,
            catalog.as_deref_mut(),
        )?;
        let (mut result, mut result_lineage) =
            polygon_parts_shape(shell_parts, axes, source_lineage.is_some())?;
        let frame = GeographicFrame::crs84();
        for (hole, hole_sequence) in crossing_holes {
            let hole_polygon = Polygon::new(hole.clone(), Vec::new());
            let hole_shape = Shape::Polygon(hole_polygon.clone());
            let hole_lineage = match (hole_sequence, source_lineage) {
                (Some(shell), Some(source_lineage)) => {
                    Some(super::super::lineage::LineagePolygon {
                        member: source_lineage.member,
                        shell,
                        holes: Vec::new(),
                    })
                },
                (None, None) => None,
                _ => {
                    return Err(GeometryErrorKind::Invalid(
                        "crossing hole and source lineage disagree".into(),
                    )
                    .into());
                },
            };
            let hole_parts = split_polygon_core(
                &hole_shape,
                &hole_polygon,
                hole_lineage.as_ref(),
                pole_closure_for_ring(&hole),
                overlay_mode,
                source_frame,
                metric_access,
                catalog.as_deref_mut(),
            )?;
            let (hole_shape, hole_result_lineage) =
                polygon_parts_shape(hole_parts, hole.coords().axes(), source_lineage.is_some())?;
            let operands = [&result, &hole_shape];
            let use_quotient = match overlay_mode {
                HoleOverlayMode::Select => {
                    shape_has_longitude_turn_in_frame(&result, frame)?
                        || shape_has_longitude_turn_in_frame(&hole_shape, frame)?
                },
                HoleOverlayMode::Quotient => true,
                HoleOverlayMode::Chart => false,
            };
            if use_quotient {
                overlay_mode = HoleOverlayMode::Quotient;
            }
            if source_lineage.is_some() {
                let mut source_edges = Vec::new();
                append_shape_edge_uses(
                    result_lineage.as_ref().ok_or_else(|| {
                        GeometryErrorKind::Invalid("captured shell overlay has no lineage".into())
                    })?,
                    &mut source_edges,
                )?;
                append_shape_edge_uses(
                    hole_result_lineage.as_ref().ok_or_else(|| {
                        GeometryErrorKind::Invalid("captured hole overlay has no lineage".into())
                    })?,
                    &mut source_edges,
                )?;
                let parts = crate::geometry::overlay_all_geographic_chart_with_sources(
                    &operands,
                    crate::geometry::OverlayOp::Difference,
                )?;
                let axes = result.axes().union(hole_shape.axes());
                let (shape, lineage) = overlay_parts_shape(
                    parts,
                    axes,
                    source_lineage
                        .expect("captured split has source lineage")
                        .member,
                    &source_edges,
                    source_frame,
                    metric_access,
                    catalog.as_deref_mut().ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "captured polygon lineage has no event catalog".into(),
                        )
                    })?,
                )?;
                result = shape;
                result_lineage = Some(lineage);
            } else {
                result = overlay_hole_difference(&operands, use_quotient, frame, metric_access)?;
            }
        }

        let polygons = polygons_from_shape(result)?;
        let lineages = match result_lineage {
            Some(lineage) => polygon_lineages(lineage, polygons.len())?
                .into_iter()
                .map(Some)
                .collect(),
            None => vec![None; polygons.len()],
        };
        let parts = polygons.into_iter().zip(lineages).collect::<Vec<_>>();
        if overlay_mode != HoleOverlayMode::Quotient {
            return Ok(parts);
        }
        let mut charted = Vec::new();
        for (polygon, lineage) in parts {
            charted.extend(split_polygon_core(
                source,
                &polygon,
                lineage.as_ref(),
                pole_closure_for_ring(&polygon.shell),
                HoleOverlayMode::Chart,
                source_frame,
                metric_access,
                catalog.as_deref_mut(),
            )?);
        }
        return Ok(charted);
    }

    let shell_points = collect_points(polygon.shell.coords());
    match split_ring_at_seam_with_metric(&shell_points, GeographicFrame::crs84(), metric_access)? {
        SeamRing::Whole(shell) => {
            let shell_lineage = match source_lineage {
                Some(source_lineage) => Some(seam_piece_lineage(
                    &shell,
                    &source_lineage.shell,
                    true,
                    catalog.as_deref_mut().ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "captured polygon lineage has no event catalog".into(),
                        )
                    })?,
                )?),
                None => None,
            };
            let shell = Ring::try_from_closed(CoordSeq::try_from_points(&shell.points)?)?;
            let mut holes = Vec::with_capacity(stationary_holes.len());
            let mut hole_lineages = Vec::with_capacity(stationary_holes.len());
            for (hole, lineage) in stationary_holes {
                holes.push(hole);
                if let Some(lineage) = lineage {
                    hole_lineages.push(lineage);
                }
            }
            let polygon = Polygon::new(shell, holes);
            let lineage = match (source_lineage, shell_lineage) {
                (Some(source_lineage), Some(shell))
                    if hole_lineages.len() == polygon.holes.len() =>
                {
                    Some(super::super::lineage::LineagePolygon {
                        member: source_lineage.member,
                        shell,
                        holes: hole_lineages,
                    })
                },
                (None, None) => None,
                _ => {
                    return Err(GeometryErrorKind::Invalid(
                        "whole shell and source lineage disagree".into(),
                    )
                    .into());
                },
            };
            Ok(vec![(polygon, lineage)])
        },
        SeamRing::Pieces(pieces) => {
            let mut segments = Vec::with_capacity(pieces.len());
            for piece in pieces {
                let lineage = match source_lineage {
                    Some(source_lineage) => Some(seam_piece_lineage(
                        &piece,
                        &source_lineage.shell,
                        true,
                        catalog.as_deref_mut().ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "captured polygon lineage has no event catalog".into(),
                            )
                        })?,
                    )?),
                    None => None,
                };
                segments.push(SeamFragment {
                    points: piece.points,
                    lineage,
                });
            }
            let original_segments = segments.clone();
            let (segments, capped) =
                extend_over_poles_fragments(segments, preferred, catalog.as_deref_mut())?;
            let kept_holes = stationary_holes
                .iter()
                .map(|(ring, lineage)| KeptHole {
                    ring,
                    lineage: lineage.as_ref(),
                })
                .collect::<Vec<_>>();
            let mut polygons = assemble_fragments(segments, &kept_holes, catalog.as_deref_mut())?;
            if capped
                && preferred == PoleClosure::Auto
                && !polygons.iter().try_fold(true, |valid, (polygon, _)| {
                    Ok::<_, crate::error::Error>(valid && polygon_is_valid(polygon)?)
                })?
            {
                let mut candidates = Vec::new();
                for closure in [
                    PoleClosure::Chart(ChartPole::North),
                    PoleClosure::Chart(ChartPole::South),
                ] {
                    let (forced, _) = extend_over_poles_fragments(
                        original_segments.clone(),
                        closure,
                        catalog.as_deref_mut(),
                    )?;
                    if let Ok(candidate) =
                        assemble_fragments(forced, &kept_holes, catalog.as_deref_mut())
                        && candidate.iter().try_fold(true, |valid, (polygon, _)| {
                            Ok::<_, crate::error::Error>(valid && polygon_is_valid(polygon)?)
                        })?
                    {
                        candidates.push(candidate);
                    }
                }
                polygons = match <[Vec<_>; 1]>::try_from(candidates) {
                    Ok([winner]) => winner,
                    Err(candidates) if candidates.is_empty() => {
                        return Err(GeometryErrorKind::antimeridian_split_failed(
                            "neither pole closure produces valid polygon rings",
                        ));
                    },
                    Err(_) => {
                        return Err(GeometryErrorKind::antimeridian_split_failed(
                            "pole closure is ambiguous: both directions produce valid rings",
                        ));
                    },
                };
            }
            Ok(polygons)
        },
    }
}

pub(super) fn split_polygon_with_lineage<M: ChartMetric>(
    source: &Shape,
    polygon: &Polygon,
    source_lineage: &super::super::lineage::LineagePolygon,
    preferred: PoleClosure,
    overlay_mode: HoleOverlayMode,
    source_frame: GeographicFrame,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    catalog: &mut super::super::lineage::LineageCatalog,
) -> Result<Vec<super::super::lineage::PolygonWithLineage>> {
    let parts = split_polygon_core(
        source,
        polygon,
        Some(source_lineage),
        preferred,
        overlay_mode,
        source_frame,
        metric_access,
        Some(catalog),
    )?;
    parts
        .into_iter()
        .map(|(polygon, lineage)| {
            Ok(super::super::lineage::PolygonWithLineage {
                polygon,
                lineage: lineage.ok_or_else(|| {
                    GeometryErrorKind::Invalid("captured split polygon has no lineage".into())
                })?,
            })
        })
        .collect()
}

fn shape_has_longitude_turn_in_frame(shape: &Shape, frame: GeographicFrame) -> Result<bool> {
    fn ring_has_longitude_turn(ring: &Ring, frame: GeographicFrame) -> Result<bool> {
        let summary = RingSummary::try_new(frame, &GeographicRing::from_ring(ring))
            .map_err(|error| split_view_error(&error))?;
        Ok(summary.chart_turn != 0)
    }

    fn polygon_has_longitude_turn(polygon: &Polygon, frame: GeographicFrame) -> Result<bool> {
        if ring_has_longitude_turn(&polygon.shell, frame)? {
            return Ok(true);
        }
        for hole in polygon.holes.iter() {
            if ring_has_longitude_turn(hole, frame)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    match shape {
        Shape::Polygon(polygon) => polygon_has_longitude_turn(polygon, frame),
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons.iter() {
                if polygon_has_longitude_turn(polygon, frame)? {
                    return Ok(true);
                }
            }
            Ok(false)
        },
        Shape::GeometryCollection(geometries) => {
            for geometry in geometries {
                if shape_has_longitude_turn_in_frame(geometry, frame)? {
                    return Ok(true);
                }
            }
            Ok(false)
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Empty(..) => Ok(false),
    }
}

/// Resolve pole ownership from the admitted source ring before chart stitching.
fn pole_closure_for_ring(ring: &Ring) -> PoleClosure {
    match (
        ring_encloses_pole(ring, ChartPole::North),
        ring_encloses_pole(ring, ChartPole::South),
    ) {
        (true, false) => PoleClosure::Chart(ChartPole::North),
        (false, true) => PoleClosure::Chart(ChartPole::South),
        _ => PoleClosure::Auto,
    }
}
