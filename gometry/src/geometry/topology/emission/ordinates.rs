use super::sites::{EmittedVertex, with_vertex_axes};
use super::{
    AssignedSite, ChartMetric, ChartSide, CoordinateAxes, EmissionChordId, EmissionPortId,
    ExactChartPoint, ExactGeographicTopology, ExpandedEmissionPlan, FragmentRecipeId,
    GeometryErrorKind, MOrdinate, NativeChordPoints, NodeOrdinates, Point, PortPosition,
    PortSpelling, PublicationInventory, PublishedFragment, QSourceImage, RequestRowInput,
    ResourceError, Result, RootCatalog, RootId, RootRange, RootRequest, SourceArcMetricAccess,
    TraceRequestLocation, VertexOrigin, ZOrdinate, resolve_request_rows,
};
use crate::geometry::XY;
use crate::geometry::tessellation::exact::ExactRational;
use crate::work::Work;

pub(in crate::geometry::topology) fn with_publication_axes(
    point: XY,
    ordinates: NodeOrdinates,
    axes: CoordinateAxes,
) -> Result<Point> {
    let z = if axes.has_z() {
        Some(ordinates.z.value().ok_or_else(|| {
            GeometryErrorKind::Invalid("source publication Z ordinate is missing".into())
        })?)
    } else {
        None
    };
    let m = if axes.has_m() {
        Some(ordinates.m.value().ok_or_else(|| {
            GeometryErrorKind::Invalid("source publication M ordinate is missing".into())
        })?)
    } else {
        None
    };
    Ok(Point::new_raw_axes(
        point.x,
        point.y,
        ZOrdinate(z),
        MOrdinate(m),
    ))
}

pub(in crate::geometry::topology) fn with_assigned_source_vertex_axes(
    vertex: EmittedVertex<'_>,
    ordinates: &[NodeOrdinates],
    source_ports: Option<&[Option<ExactChartPoint>]>,
    axes: CoordinateAxes,
    work: &mut impl Work,
) -> Result<Point> {
    let point = match (vertex.port.cut, source_ports) {
        (true, Some(source_ports)) => {
            let image = source_ports
                .get(vertex.port.id.0 as usize)
                .and_then(Option::as_ref)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "assigned Q source cut has no checked chart image".into(),
                    )
                })?;
            work.charge_into_error(1)?;
            Point::new_raw_xy(image.x.round()?, image.y.round()?)
        },
        _ => vertex.point,
    };
    with_vertex_axes(
        EmittedVertex {
            port: vertex.port,
            point,
            literal_image: vertex.literal_image,
        },
        ordinates,
        axes,
    )
}

pub(in crate::geometry::topology) const fn source_literal_image(
    vertex: EmittedVertex<'_>,
    source_ports: Option<&[Option<ExactChartPoint>]>,
) -> QSourceImage {
    if vertex.port.cut && source_ports.is_some() {
        QSourceImage::Port(vertex.port.id)
    } else {
        vertex.literal_image
    }
}

pub(in crate::geometry::topology) fn node_ordinates<M: crate::geometry::ChartMetric>(
    topology: &ExactGeographicTopology,
    declared_axes: CoordinateAxes,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    work: &mut impl Work,
) -> Result<Vec<NodeOrdinates>> {
    let q = &topology.q;
    if q.source_edge_ports.len() != topology.sources.edges.len() {
        return Err(GeometryErrorKind::Invalid(
            "Q source-edge ports do not match their source intervals".into(),
        )
        .into());
    }
    if q.source_point_ports.len() != topology.sources.points.len() {
        return Err(GeometryErrorKind::Invalid(
            "Q source-point ports do not match their source points".into(),
        )
        .into());
    }
    work.charge_into_error(u64::try_from(q.nodes.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut output = vec![NodeOrdinates::default(); q.nodes.len()];
    if !declared_axes.has_z() && !declared_axes.has_m() {
        return Ok(output);
    }
    for (edge, ports) in topology
        .sources
        .edges
        .iter()
        .zip(q.source_edge_ports.iter())
    {
        work.charge_into_error(1)?;
        for (point, &port) in edge.source.iter().zip(ports) {
            work.charge_into_error(1)?;
            let node = q
                .ports
                .get(port.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q source endpoint port is unknown".into())
                })?
                .node;
            let values = output.get_mut(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source endpoint node is unknown".into())
            })?;
            values.z.observe(point.z());
            values.m.observe(point.m());
        }
    }
    let mut request_rows = Vec::new();
    let mut request_nodes = Vec::new();
    for span in &q.spans {
        work.charge_into_error(1)?;
        for owner in &span.owners {
            work.charge_into_error(1)?;
            let start_parameter = if owner.reversed {
                owner.source_range.end.try_clone_with_work(work)?
            } else {
                owner.source_range.start.try_clone_with_work(work)?
            };
            let end_parameter = if owner.reversed {
                owner.source_range.start.try_clone_with_work(work)?
            } else {
                owner.source_range.end.try_clone_with_work(work)?
            };
            for (node, parameter) in [
                (span.nodes[0], start_parameter),
                (span.nodes[1], end_parameter),
            ] {
                work.charge_into_error(3)?;
                request_rows.push(RequestRowInput {
                    axes: declared_axes,
                    requests: vec![RootRequest::new_with_work(
                        RootId::from_index(owner.source_edge.0),
                        parameter,
                        work,
                    )?],
                });
                request_nodes.push(node);
            }
        }
    }
    let resolved = resolve_request_rows(&request_rows, topology, metric_access, work)?;
    for (node, ordinates) in request_nodes.into_iter().zip(resolved) {
        work.charge_into_error(1)?;
        output
            .get_mut(node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q span endpoint node is unknown".into()))?
            .merge(ordinates);
    }
    for (source, &port) in topology
        .sources
        .points
        .iter()
        .zip(q.source_point_ports.iter())
    {
        work.charge_into_error(1)?;
        let node = q
            .ports
            .get(port.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q source point port is unknown".into()))?
            .node;
        let values = output
            .get_mut(node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q source point node is unknown".into()))?;
        values.z.observe(source.point.z());
        values.m.observe(source.point.m());
    }
    Ok(output)
}

pub(in crate::geometry::topology) fn resolve_emission_ordinates<M: ChartMetric, C: RootCatalog>(
    plan: &ExpandedEmissionPlan<'_>,
    publication: &PublicationInventory,
    source_ordinates: &[NodeOrdinates],
    roots: &C,
    declared_axes: CoordinateAxes,
    strict: bool,
    operation: &'static str,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    work: &mut impl Work,
) -> Result<(Vec<NodeOrdinates>, CoordinateAxes)> {
    if !declared_axes.has_z() && !declared_axes.has_m() {
        work.charge_into_error(
            u64::try_from(plan.ports.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        return Ok((
            vec![NodeOrdinates::default(); plan.ports.len()],
            CoordinateAxes::XY,
        ));
    }
    work.charge_into_error(
        u64::try_from(plan.occurrences.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut request_rows = Vec::with_capacity(plan.occurrences.len());
    for occurrence in &plan.occurrences {
        work.charge_into_error(1)?;
        let requests = match &occurrence.source_trace {
            Some(location) => source_requests_at_location(publication, location, work)?,
            None => Vec::new(),
        };
        request_rows.push(RequestRowInput {
            axes: declared_axes,
            requests,
        });
    }
    let resolved_requests = resolve_request_rows(&request_rows, roots, metric_access, work)?;

    work.charge_into_error(
        u64::try_from(plan.ports.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut output = Vec::with_capacity(plan.ports.len());
    let mut has_z = declared_axes.has_z();
    let mut has_m = declared_axes.has_m();
    for port in &plan.ports {
        work.charge_into_error(1)?;
        let mut ordinates = NodeOrdinates::default();
        let mut has_live_presentation = false;
        for occurrence_index in port.occurrence_range.clone() {
            work.charge_into_error(1)?;
            let occurrence = plan.occurrences.get(occurrence_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("native port occurrence row is unknown".into())
            })?;
            if occurrence.request_row.index() != occurrence_index {
                return Err(GeometryErrorKind::Invalid(
                    "native port occurrence request row is misaligned".into(),
                )
                .into());
            }
            let mut occurrence_ordinates = match &occurrence.origin {
                VertexOrigin::SourceNode(node) => {
                    *source_ordinates.get(node.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("native source ordinate node is unknown".into())
                    })?
                },
                VertexOrigin::SourceSample => NodeOrdinates::default(),
                VertexOrigin::Presentation => {
                    has_live_presentation = true;
                    NodeOrdinates::default()
                },
            };
            occurrence_ordinates.merge(*resolved_requests.get(occurrence_index).ok_or_else(
                || {
                    GeometryErrorKind::Invalid(
                        "native occurrence has no resolved source request row".into(),
                    )
                },
            )?);
            ordinates.merge(occurrence_ordinates);
        }
        has_z &= !has_live_presentation && ordinates.z.value().is_some();
        has_m &= !has_live_presentation && ordinates.m.value().is_some();
        output.push(ordinates);
    }
    let axes = CoordinateAxes::new(crate::geometry::HasZ(has_z), crate::geometry::HasM(has_m));
    if strict && axes != declared_axes {
        return Err(GeometryErrorKind::ordinate_dropped(operation));
    }
    Ok((output, axes))
}

pub(super) fn source_requests_at_location(
    publication: &PublicationInventory,
    location: &TraceRequestLocation,
    work: &mut impl Work,
) -> Result<Vec<RootRequest>> {
    match location {
        TraceRequestLocation::Root { root, parameter } => {
            work.charge_into_error(1)?;
            Ok(vec![RootRequest::new_with_work(
                *root,
                parameter.try_clone_with_work(work)?,
                work,
            )?])
        },
        TraceRequestLocation::Edge {
            recipe,
            edge,
            parameter,
        } => {
            let fragment = source_trace_fragment(publication, *recipe)?;
            fragment.trace.rows.requests_at(*edge, parameter, work)
        },
        TraceRequestLocation::Sample { recipe, row } => {
            let fragment = source_trace_fragment(publication, *recipe)?;
            fragment.trace.rows.sample_requests_at(row.index(), work)
        },
    }
}

pub(in crate::geometry::topology) fn source_root_ranges_at_location(
    publication: &PublicationInventory,
    location: &TraceRequestLocation,
    work: &mut impl Work,
) -> Result<Vec<RootRange>> {
    let requests = source_requests_at_location(publication, location, work)?;
    let mut ranges = Vec::with_capacity(requests.len());
    for request in requests {
        work.charge_into_error(1)?;
        ranges.push(RootRange::point_with_work(
            request.root(),
            request.parameter().try_clone_with_work(work)?,
            ExactRational::zero(),
            work,
        )?);
    }
    Ok(ranges)
}

pub(super) fn source_trace_fragment(
    publication: &PublicationInventory,
    recipe: FragmentRecipeId,
) -> Result<&PublishedFragment> {
    publication
        .fragments
        .get(recipe.0 as usize)
        .filter(|fragment| fragment.recipe == recipe)
        .ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "written occurrence references an unknown source trace".into(),
            )
            .into()
        })
}

pub(super) fn native_chord_points(
    plan: &ExpandedEmissionPlan<'_>,
    sites: &[AssignedSite],
    ordinates: &[NodeOrdinates],
    chords: &[EmissionChordId],
    axes: CoordinateAxes,
    closed: bool,
    budget: &mut impl Work,
) -> Result<NativeChordPoints> {
    let Some(&first_id) = chords.first() else {
        return Err(GeometryErrorKind::Invalid("native emission walk is empty".into()).into());
    };
    let first_chord = plan.chords.get(first_id.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("native emission walk starts with an unknown chord".into())
    })?;
    let first_port = first_chord.ports[0];
    let point_count = chords
        .len()
        .checked_add(1)
        .ok_or(ResourceError::SizeOverflow)?;
    budget.charge_into_error(
        u64::try_from(
            point_count
                .checked_mul(2)
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut points = Vec::with_capacity(point_count);
    let mut ports = Vec::with_capacity(point_count);
    budget.charge_into_error(1)?;
    points.push(native_port_point(plan, sites, ordinates, first_port, axes)?);
    ports.push(first_port);
    let mut previous = first_port;
    for &chord_id in chords {
        budget.charge_into_error(1)?;
        let chord = plan.chords.get(chord_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("native emission walk references an unknown chord".into())
        })?;
        if chord.ports[0] != previous {
            return Err(GeometryErrorKind::Invalid(
                "native emission chords are not directed-contiguous".into(),
            )
            .into());
        }
        let end = chord.ports[1];
        points.push(native_port_point(plan, sites, ordinates, end, axes)?);
        ports.push(end);
        previous = end;
    }
    if closed {
        if previous != first_port {
            return Err(GeometryErrorKind::Invalid(
                "native polygon part does not close through its indexed ports".into(),
            )
            .into());
        }
        budget.charge_into_error(1)?;
        let first = *points.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("native polygon part has no first record".into())
        })?;
        let last = points.last_mut().ok_or_else(|| {
            GeometryErrorKind::Invalid("native polygon part has no closing record".into())
        })?;
        *last = first;
    }
    Ok(NativeChordPoints { points, ports })
}

pub(in crate::geometry::topology) fn native_port_point(
    plan: &ExpandedEmissionPlan<'_>,
    sites: &[AssignedSite],
    ordinates: &[NodeOrdinates],
    port_id: EmissionPortId,
    axes: CoordinateAxes,
) -> Result<Point> {
    let port = plan.ports.get(port_id.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("native drawing references an unknown port".into())
    })?;
    let site = sites.get(port.node.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("native drawing node has no assigned site".into())
    })?;
    let (longitude, latitude) = port.source_record.map_or_else(
        || {
            let spelling = match &port.position {
                PortPosition::Source { spelling } | PortPosition::Projected { spelling } => {
                    spelling
                },
            };
            let longitude = match spelling {
                PortSpelling::Interior { .. } => site.longitude.native().get(),
                PortSpelling::Seam(ChartSide::East) => {
                    plan.topology.sources.frame.longitude().half_period()
                },
                PortSpelling::Seam(ChartSide::West) => {
                    -plan.topology.sources.frame.longitude().half_period()
                },
                PortSpelling::Pole { native_longitude } => *native_longitude,
            };
            (longitude, site.latitude)
        },
        |record| (record.x, record.y),
    );
    let ordinates = ordinates.get(port_id.0 as usize).copied().ok_or_else(|| {
        GeometryErrorKind::Invalid("native drawing port has no resolved ordinates".into())
    })?;
    let z = if axes.has_z() {
        Some(ordinates.z.value().ok_or_else(|| {
            GeometryErrorKind::Invalid("sealed native Z ordinate is missing".into())
        })?)
    } else {
        None
    };
    let m = if axes.has_m() {
        Some(ordinates.m.value().ok_or_else(|| {
            GeometryErrorKind::Invalid("sealed native M ordinate is missing".into())
        })?)
    } else {
        None
    };
    Ok(Point::new_raw_axes(
        longitude,
        latitude,
        ZOrdinate(z),
        MOrdinate(m),
    ))
}

pub(in crate::geometry::topology) fn resolve_publication_ordinates<M: ChartMetric>(
    topology: &ExactGeographicTopology,
    publication: &PublicationInventory,
    live_samples: &[bool],
    axes: CoordinateAxes,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    work: &mut impl Work,
) -> Result<Vec<NodeOrdinates>> {
    if live_samples.len() != publication.samples.len() {
        return Err(GeometryErrorKind::Invalid(
            "publication liveness rows do not match their sample inventory".into(),
        )
        .into());
    }
    if publication.samples.is_empty() {
        return Ok(Vec::new());
    }
    if !axes.has_z() && !axes.has_m() {
        work.charge_into_error(
            u64::try_from(publication.samples.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        return Ok(vec![NodeOrdinates::default(); publication.samples.len()]);
    }
    work.charge_into_error(
        u64::try_from(publication.samples.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut request_rows = Vec::with_capacity(publication.samples.len());
    for fragment in &publication.fragments {
        if fragment.trace.samples.len() != fragment.sample_range.len() {
            return Err(GeometryErrorKind::Invalid(
                "publication source ordinates do not match their sample rows".into(),
            )
            .into());
        }
        for (offset, trace_sample) in fragment.trace.samples.iter().enumerate() {
            work.charge_into_error(1)?;
            let sample_index = fragment
                .sample_range
                .start
                .checked_add(offset)
                .ok_or(ResourceError::SizeOverflow)?;
            let sample = publication.samples.get(sample_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("publication sample row is unknown".into())
            })?;
            if sample.request_row.index() != sample_index
                || sample.written_vertex != trace_sample.written_vertex
                || sample.source_trace.is_some() != trace_sample.has_source
                || sample.endpoint_only != trace_sample.endpoint_only
            {
                return Err(GeometryErrorKind::Invalid(
                    "publication sample source rows are misaligned".into(),
                )
                .into());
            }
            let source_trace = if let Some(source_trace) = &sample.source_trace {
                let TraceRequestLocation::Sample { recipe, row } = source_trace else {
                    return Err(GeometryErrorKind::Invalid(
                        "publication sample does not own a sample request row".into(),
                    )
                    .into());
                };
                if *recipe != fragment.recipe || *row != trace_sample.request_row {
                    return Err(GeometryErrorKind::Invalid(
                        "publication sample source trace is misaligned".into(),
                    )
                    .into());
                }
                Some(source_trace)
            } else {
                None
            };
            let live = *live_samples.get(sample_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("publication sample liveness row is unknown".into())
            })?;
            let requests = if live {
                source_trace
                    .map(|location| source_requests_at_location(publication, location, work))
                    .transpose()?
                    .unwrap_or_default()
            } else {
                Vec::new()
            };
            request_rows.push(RequestRowInput {
                axes: if live { axes } else { CoordinateAxes::XY },
                requests,
            });
        }
    }
    if request_rows.len() != publication.samples.len() {
        return Err(GeometryErrorKind::Invalid(
            "publication source ordinate rows are incomplete".into(),
        )
        .into());
    }
    resolve_request_rows(&request_rows, topology, metric_access, work)
}
