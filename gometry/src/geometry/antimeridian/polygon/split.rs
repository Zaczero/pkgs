use super::assemble::{
    assemble_polygons, assemble_polygons_with_lineage, extend_over_poles, polygons_from_shape,
};
use super::*;
use crate::geometry::antimeridian::linestring::{SeamPiece, SeamVertexSource};
use crate::work::Unmetered;

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
    let preferred = match preferred {
        PoleClosure::Auto => pole_closure_for_ring(&polygon.shell),
        closure @ PoleClosure::Chart(_) => closure,
    };
    let polygons = split_polygon_inner(polygon, preferred, metric_access)?;
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
        work: &mut impl crate::work::Work,
    ) -> Result<Vec<(usize, ExactRational)>> {
        let mut parameters = Vec::new();
        for source in sources {
            match source {
                SeamVertexSource::InputVertex(index) if source_closed && *index == edge_count => {
                    parameters.push((edge_count - 1, ExactRational::one()));
                    parameters.push((0, ExactRational::zero()));
                },
                SeamVertexSource::InputVertex(index) => {
                    if *index < edge_count {
                        parameters.push((*index, ExactRational::zero()));
                    }
                    if *index > 0 {
                        parameters.push((*index - 1, ExactRational::one()));
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
                    let event = catalog.event_on_edge(member, source_edge, parameter)?;
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
    for index in 0..piece.points.len().saturating_sub(1) {
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
            piece.vertices.get(index + 1).ok_or_else(|| {
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
            crate::geometry::antimeridian::publication::restriction_for_edge(
                piece.points[index],
                piece.points[index + 1],
            )
        } else {
            None
        };
        edges.push(catalog.compose_edge_uses(member, &source_uses, restriction)?);
    }
    Ok(super::super::lineage::LineageSequence {
        member,
        vertices,
        edges,
    })
}

fn polygon_parts_shape(
    parts: Vec<super::super::lineage::PolygonWithLineage>,
    axes: CoordinateAxes,
) -> Result<(Shape, super::super::lineage::ShapeLineage)> {
    let mut polygons = Vec::with_capacity(parts.len());
    let mut lineages = Vec::with_capacity(parts.len());
    for part in parts {
        polygons.push(part.polygon);
        lineages.push(Some(part.lineage));
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
                || GeometryErrorKind::Invalid("split polygon part has no member lineage".into()),
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
    part: &crate::geometry::overlay::OverlayPolygonPartWithSources,
    ring_index: usize,
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
        part.polygon
            .holes
            .get(ring_index - 1)
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
    for index in 0..vertex_count {
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
        let events = vec![catalog.event_from_edge_parameters(member, &uses)?];
        let ordinates = catalog.resolve_vertex_ordinates(
            &events,
            coordinates.axes(),
            source_frame,
            metric_access,
        )?;
        let z = if coordinates.axes().has_z() {
            ordinates.z.value()
        } else {
            None
        };
        let m = if coordinates.axes().has_m() {
            ordinates.m.value()
        } else {
            None
        };
        if (coordinates.axes().has_z() && z.is_none())
            || (coordinates.axes().has_m() && m.is_none())
        {
            return Err(
                GeometryErrorKind::OrdinateDropped("antimeridian split overlay".into()).into(),
            );
        }
        output_points.push(Point::new_axes(
            points[index].x,
            points[index].y,
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
            crate::geometry::antimeridian::publication::restriction_for_edge(
                points[index],
                points[next],
            )
        } else {
            None
        };
        lineage_edges.push(catalog.compose_edge_uses(member, &sources, restriction)?);
    }
    Ok((output_ring, super::super::lineage::LineageSequence {
        member,
        vertices: lineage_vertices,
        edges: lineage_edges,
    }))
}

fn overlay_parts_shape<M: ChartMetric>(
    parts: Vec<crate::geometry::overlay::OverlayPolygonPartWithSources>,
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
        _ => Err(GeometryErrorKind::Invalid(
            "overlay polygon output has the wrong lineage kind".into(),
        )
        .into()),
    }
}

pub(super) fn split_polygon_with_lineage<M: ChartMetric>(
    source: &Shape,
    polygon: &Polygon,
    source_lineage: &super::super::lineage::LineagePolygon,
    preferred: PoleClosure,
    mut overlay_mode: HoleOverlayMode,
    source_frame: GeographicFrame,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    catalog: &mut super::super::lineage::LineageCatalog,
) -> Result<Vec<super::super::lineage::PolygonWithLineage>> {
    let preferred = match preferred {
        PoleClosure::Auto => pole_closure_for_ring(&polygon.shell),
        closure @ PoleClosure::Chart(_) => closure,
    };
    let mut crossing_holes = Vec::new();
    let mut stationary_holes = Vec::new();
    for (hole, lineage) in polygon.holes.iter().zip(&source_lineage.holes) {
        let points = collect_points(hole.coords());
        match split_ring_at_seam_with_metric(&points, GeographicFrame::crs84(), metric_access)? {
            SeamRing::Whole(spelled) if !ring_follows_seam(&points) => {
                let hole_lineage = seam_piece_lineage(&spelled, lineage, true, catalog)?;
                stationary_holes.push((
                    Ring::try_from_closed(CoordSeq::try_from_points(&spelled.points)?)?,
                    hole_lineage,
                ));
            },
            SeamRing::Pieces(_) | SeamRing::Whole(_) => {
                crossing_holes.push((hole.clone(), lineage.clone()));
            },
        }
    }
    if !crossing_holes.is_empty() {
        let shell_only = Polygon::new(
            polygon.shell.clone(),
            stationary_holes
                .iter()
                .map(|(hole, _)| hole.clone())
                .collect(),
        );
        let shell_only_lineage = super::super::lineage::LineagePolygon {
            member: source_lineage.member,
            shell: source_lineage.shell.clone(),
            holes: stationary_holes
                .iter()
                .map(|(_, lineage)| lineage.clone())
                .collect(),
        };
        let shell_parts = split_polygon_with_lineage(
            source,
            &shell_only,
            &shell_only_lineage,
            preferred,
            overlay_mode,
            source_frame,
            metric_access,
            catalog,
        )?;
        let (mut result, mut result_lineage) = polygon_parts_shape(shell_parts, polygon.axes())?;
        let frame = GeographicFrame::crs84();
        for (hole, hole_lineage) in crossing_holes {
            let hole_polygon = Polygon::new(hole.clone(), Vec::new());
            let hole_shape = Shape::Polygon(hole_polygon.clone());
            let hole_lineage = super::super::lineage::LineagePolygon {
                member: source_lineage.member,
                shell: hole_lineage,
                holes: Vec::new(),
            };
            let hole_parts = split_polygon_with_lineage(
                &hole_shape,
                &hole_polygon,
                &hole_lineage,
                pole_closure_for_ring(&hole),
                overlay_mode,
                source_frame,
                metric_access,
                catalog,
            )?;
            let (hole_shape, hole_result_lineage) =
                polygon_parts_shape(hole_parts, hole.coords().axes())?;
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
            let mut source_edges = Vec::new();
            append_shape_edge_uses(&result_lineage, &mut source_edges)?;
            append_shape_edge_uses(&hole_result_lineage, &mut source_edges)?;
            let parts = crate::geometry::overlay_all_geographic_chart_with_sources(
                &operands,
                frame,
                crate::geometry::OverlayOp::Difference,
                Strictness::Strict,
                metric_access,
            )?;
            let axes = result.axes().union(hole_shape.axes());
            (result, result_lineage) = overlay_parts_shape(
                parts,
                axes,
                source_lineage.member,
                &source_edges,
                source_frame,
                metric_access,
                catalog,
            )?;
        }
        let polygons = polygons_from_shape(result)?;
        if overlay_mode != HoleOverlayMode::Quotient {
            let lineages = polygon_lineages(result_lineage, polygons.len())?;
            reject_fabricated_pole_ordinates(source, &polygons)?;
            return Ok(polygons
                .into_iter()
                .zip(lineages)
                .map(
                    |(polygon, lineage)| super::super::lineage::PolygonWithLineage {
                        polygon,
                        lineage,
                    },
                )
                .collect());
        }
        let lineages = polygon_lineages(result_lineage, polygons.len())?;
        let mut charted = Vec::new();
        for (polygon, lineage) in polygons.into_iter().zip(lineages) {
            let closure = pole_closure_for_ring(&polygon.shell);
            charted.extend(split_polygon_with_lineage(
                source,
                &polygon,
                &lineage,
                closure,
                HoleOverlayMode::Chart,
                source_frame,
                metric_access,
                catalog,
            )?);
        }
        reject_fabricated_pole_ordinates(
            source,
            &charted
                .iter()
                .map(|part| part.polygon.clone())
                .collect::<Vec<_>>(),
        )?;
        return Ok(charted);
    }

    let shell_points = collect_points(polygon.shell.coords());
    match split_ring_at_seam_with_metric(&shell_points, GeographicFrame::crs84(), metric_access)? {
        SeamRing::Whole(shell) => {
            let shell_lineage = seam_piece_lineage(&shell, &source_lineage.shell, true, catalog)?;
            let shell = Ring::try_from_closed(CoordSeq::try_from_points(&shell.points)?)?;
            let (holes, hole_lineages): (Vec<_>, Vec<_>) = stationary_holes.into_iter().unzip();
            let polygon = Polygon::new(shell, holes);
            reject_fabricated_pole_ordinates(source, std::slice::from_ref(&polygon))?;
            Ok(vec![super::super::lineage::PolygonWithLineage {
                polygon,
                lineage: super::super::lineage::LineagePolygon {
                    member: source_lineage.member,
                    shell: shell_lineage,
                    holes: hole_lineages,
                },
            }])
        },
        SeamRing::Pieces(pieces) => {
            let mut segments = pieces
                .iter()
                .map(|piece| {
                    Ok((
                        piece.points.clone(),
                        seam_piece_lineage(piece, &source_lineage.shell, true, catalog)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            let (segments, capped) =
                super::assemble::extend_over_poles_with_lineage(segments, preferred, catalog)?;
            if capped {
                let polygons = split_polygon(source, polygon, preferred, metric_access)?;
                return Ok(polygons
                    .into_iter()
                    .map(|polygon| {
                        let member = source_lineage.member;
                        let empty_sequence = |ring: &Ring| super::super::lineage::LineageSequence {
                            member,
                            vertices: vec![Vec::new(); ring.coords().len()],
                            edges: Vec::new(),
                        };
                        super::super::lineage::PolygonWithLineage {
                            lineage: super::super::lineage::LineagePolygon {
                                member,
                                shell: empty_sequence(&polygon.shell),
                                holes: polygon.holes.iter().map(empty_sequence).collect(),
                            },
                            polygon,
                        }
                    })
                    .collect());
            }
            let kept_holes = stationary_holes;
            assemble_polygons_with_lineage(segments, &kept_holes, catalog)
        },
    }
}

fn split_polygon_inner<M: ChartMetric>(
    polygon: &Polygon,
    preferred: PoleClosure,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<Vec<Polygon>> {
    split_polygon_inner_in_mode(polygon, preferred, HoleOverlayMode::Select, metric_access)
}

fn split_polygon_inner_in_mode<M: ChartMetric>(
    polygon: &Polygon,
    preferred: PoleClosure,
    mut overlay_mode: HoleOverlayMode,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<Vec<Polygon>> {
    // Split crossing holes as independent areal operands, then subtract them
    // from the split shell.  Treating shell and hole seam fragments as one
    // undifferentiated stitching pool works for ordinary crossings, but it
    // cannot represent nested polar caps: both rings independently require a
    // pole closure, and the pooled closest-start walk joins them into invalid
    // self-intersecting rings. Set difference is the structural operation the
    // source polygon already expresses. Already-cut chart parts use Cartesian
    // coordinates, including zero-turn world bands with their interior
    // longitude witnesses. A nonzero longitude turn remains quotient support
    // and needs Q; its result is cut back into chart pieces before publication.
    // A hole tracing the seam is split with the crossing holes: the polygon's
    // interior lies across the seam from it.
    let mut crossing_holes = Vec::new();
    let mut stationary_holes = Vec::new();
    for hole in polygon.holes.iter() {
        let points = collect_points(hole.coords());
        match split_ring_at_seam_with_metric(&points, GeographicFrame::crs84(), metric_access)? {
            SeamRing::Whole(spelled) if !ring_follows_seam(&points) => {
                stationary_holes.push(Ring::try_from_closed(CoordSeq::try_from_points(
                    &spelled.points,
                )?)?);
            },
            SeamRing::Pieces(_) | SeamRing::Whole(_) => crossing_holes.push(hole.clone()),
        }
    }
    if !crossing_holes.is_empty() {
        let axes = polygon.shell.coords().axes();
        let shell_only = Polygon::new(polygon.shell.clone(), stationary_holes);
        let shell_parts =
            split_polygon_inner_in_mode(&shell_only, preferred, overlay_mode, metric_access)?;
        let mut result = Shape::narrow_polygons(axes, shell_parts);
        for hole in crossing_holes {
            let hole_axes = hole.coords().axes();
            let hole_closure = pole_closure_for_ring(&hole);
            let hole_parts = split_polygon_inner_in_mode(
                &Polygon::new(hole, Vec::new()),
                hole_closure,
                overlay_mode,
                metric_access,
            )?;
            let hole_shape = Shape::narrow_polygons(hole_axes, hole_parts);
            let frame = GeographicFrame::crs84();
            let operands = [&result, &hole_shape];
            let use_quotient = match overlay_mode {
                HoleOverlayMode::Select => {
                    shape_has_longitude_turn_in_frame(&result, frame)?
                        || shape_has_longitude_turn_in_frame(&hole_shape, frame)?
                },
                HoleOverlayMode::Quotient => true,
                HoleOverlayMode::Chart => false,
            };
            result = if use_quotient {
                overlay_mode = HoleOverlayMode::Quotient;
                match metric_access {
                    Some(metric_access) => crate::geometry::overlay_all_with_metric(
                        &operands,
                        TopologyDomain::Geographic(frame),
                        crate::geometry::OverlayOp::Difference,
                        Strictness::Strict,
                        metric_access,
                    )?,
                    None => crate::geometry::overlay_all(
                        &operands,
                        TopologyDomain::Geographic(frame),
                        crate::geometry::OverlayOp::Difference,
                        Strictness::Strict,
                    )?,
                }
            } else {
                crate::geometry::overlay_all_geographic_chart(
                    &operands,
                    frame,
                    crate::geometry::OverlayOp::Difference,
                    Strictness::Strict,
                    metric_access,
                )?
            };
        }
        let polygons = polygons_from_shape(result)?;
        if overlay_mode != HoleOverlayMode::Quotient {
            return Ok(polygons);
        }
        // Q selects the correct quotient set, but this API publishes cut-chart geometry.
        let mut charted = Vec::new();
        for polygon in polygons {
            let closure = pole_closure_for_ring(&polygon.shell);
            charted.extend(split_polygon_inner_in_mode(
                &polygon,
                closure,
                HoleOverlayMode::Chart,
                metric_access,
            )?);
        }
        return Ok(charted);
    }

    let segments = match split_ring_at_seam_with_metric(
        &collect_points(polygon.shell.coords()),
        GeographicFrame::crs84(),
        metric_access,
    )? {
        SeamRing::Whole(shell) => {
            let shell = Ring::try_from_closed(CoordSeq::try_from_points(&shell.points)?)?;
            return Ok(vec![Polygon::new(shell, stationary_holes)]);
        },
        SeamRing::Pieces(pieces) => pieces.into_iter().map(|piece| piece.points).collect(),
    };
    let kept_holes: Vec<&Ring> = stationary_holes.iter().collect();
    if preferred != PoleClosure::Auto {
        // Caller knows the pole: close over it directly, no inference/retry.
        let (stitched, _) = extend_over_poles(segments, preferred)?;
        return assemble_polygons(stitched, &kept_holes);
    }
    let (stitched, capped) = extend_over_poles(segments.clone(), PoleClosure::Auto)?;
    let polygons = assemble_polygons(stitched, &kept_holes)?;
    if !capped
        || polygons.iter().try_fold(true, |ok, polygon| {
            Ok::<_, crate::error::Error>(ok && polygon_is_valid(polygon)?)
        })?
    {
        return Ok(polygons);
    }
    // The automatic closure picked a pole that self-intersects the ring.
    // Try each direction; a unique valid candidate wins, a tie is genuinely
    // ambiguous, and when nothing is valid the input itself was broken. Never
    // return the known-invalid automatic candidate as a plausible geometry.
    let mut valid = Vec::new();
    for closure in [
        PoleClosure::Chart(ChartPole::North),
        PoleClosure::Chart(ChartPole::South),
    ] {
        let (forced, _) = extend_over_poles(segments.clone(), closure)?;
        if let Ok(candidate) = assemble_polygons(forced, &kept_holes)
            && candidate.iter().try_fold(true, |ok, polygon| {
                Ok::<_, crate::error::Error>(ok && polygon_is_valid(polygon)?)
            })?
        {
            valid.push(candidate);
        }
    }
    match <[Vec<Polygon>; 1]>::try_from(valid) {
        Ok([winner]) => Ok(winner),
        Err(candidates) if candidates.is_empty() => {
            Err(GeometryErrorKind::antimeridian_split_failed(
                "neither pole closure produces valid polygon rings",
            ))
        },
        Err(_) => Err(GeometryErrorKind::antimeridian_split_failed(
            "pole closure is ambiguous: both directions produce valid rings",
        )),
    }
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
