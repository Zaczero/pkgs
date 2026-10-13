//! Preserve source-root traces while exact partitions emit subdivision children.

use super::*;

#[derive(Clone, Copy)]
enum LineSource<'a> {
    LineString(&'a LineSeq),
    MultiLineString(&'a MultiLineString),
}

impl<'a> LineSource<'a> {
    const fn from_shape(shape: &'a Shape) -> Option<Self> {
        match shape {
            Shape::LineString(line) => Some(Self::LineString(line)),
            Shape::MultiLineString(lines) => Some(Self::MultiLineString(lines)),
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::Polygon(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(..) => None,
        }
    }

    const fn kind(self) -> GeometryKind {
        match self {
            Self::LineString(_) => GeometryKind::LineString,
            Self::MultiLineString(_) => GeometryKind::MultiLineString,
        }
    }
}

fn source_line_ranges(source: LineSource<'_>) -> Result<Vec<Range<usize>>> {
    let mut ranges = Vec::with_capacity(match source {
        LineSource::LineString(_) => 1,
        LineSource::MultiLineString(lines) => lines.len(),
    });
    let mut offset = 0_usize;
    let mut push = |length: usize| -> Result<()> {
        let end = offset
            .checked_add(length.saturating_sub(1))
            .ok_or(ResourceError::SizeOverflow)?;
        ranges.push(offset..end);
        offset = end;
        Ok(())
    };
    match source {
        LineSource::LineString(line) => push(line.len())?,
        LineSource::MultiLineString(lines) => {
            for line in lines.iter() {
                push(line.len())?;
            }
        },
    }
    Ok(ranges)
}

fn polygon_source_ring_ranges(shape: &Shape) -> Result<Vec<Range<usize>>> {
    let polygons: Vec<&Polygon> = match shape {
        Shape::Polygon(polygon) => vec![polygon],
        Shape::MultiPolygon(polygons) => polygons.iter().collect(),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => {
            return Err(GeometryErrorKind::Invalid(
                "subdivision ring traces require a polygonal source shape".into(),
            )
            .into());
        },
    };
    let mut ranges = Vec::new();
    let mut offset = 0_usize;
    for polygon in polygons {
        for ring in polygon.rings() {
            let start = offset;
            for _ in ring.segment_pairs() {
                offset = offset.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            }
            ranges.push(start..offset);
        }
    }
    Ok(ranges)
}

fn collect_polygon_parts<'a>(shape: &'a Shape, polygons: &mut Vec<&'a Polygon>) {
    match shape {
        Shape::Polygon(polygon) => polygons.push(polygon),
        Shape::MultiPolygon(multipolygon) => polygons.extend(multipolygon.iter()),
        Shape::GeometryCollection(collection) => {
            for member in collection {
                collect_polygon_parts(member, polygons);
            }
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Empty(..) => {},
    }
}

fn same_published_polygon_xy(frame: GeographicFrame, left: XY, right: XY) -> Result<bool> {
    let longitude = frame.longitude();
    let left_x = longitude
        .canonicalize(left.x)
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?
        .get();
    let right_x = longitude
        .canonicalize(right.x)
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?
        .get();
    Ok(same_point(
        XY::new(left_x, left.y),
        XY::new(right_x, right.y),
    ))
}

pub(in crate::geometry) struct SubdivisionRootCatalog {
    inventory: ExactSourceInventory,
    frame: GeographicFrame,
    root_xy_positions_needed: bool,
}

impl SubdivisionRootCatalog {
    pub(in crate::geometry) fn needs_root_xy_positions(
        shape: &Shape,
        bounds: Option<Bounds>,
        frame: GeographicFrame,
    ) -> bool {
        // The envelope certifies the common local case without another vertex
        // walk; wider envelopes need an edge-level chart check before rooting.
        if bounds.is_some_and(|bounds| {
            frame
                .longitude()
                .ordinary_chart_edge(bounds.minx(), bounds.maxx())
        }) {
            return false;
        }
        let mut needed = false;
        let Ok(()) = shape.try_for_each_segment_chain(|chain| {
            for segment in line_segments(chain) {
                needed |= !frame
                    .longitude()
                    .ordinary_chart_edge(segment.start.x, segment.end.x);
            }
            Ok::<_, std::convert::Infallible>(())
        });
        needed
    }

    pub(in crate::geometry) fn from_line_shape(
        shape: &Shape,
        frame: GeographicFrame,
        root_xy_positions_needed: bool,
    ) -> Result<(Self, Vec<SourceTrace>)> {
        let source = LineSource::from_shape(shape).ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "ordinary subdivision roots require a line source shape".into(),
            )
        })?;
        let inventory = ExactSourceInventory::from_shapes(&[shape])?;
        let mut line_traces = Vec::with_capacity(match source {
            LineSource::LineString(_) => 1,
            LineSource::MultiLineString(lines) => lines.len(),
        });
        let mut source_edge = 0_usize;
        let mut work = Unmetered;
        let mut push_line = |line: &LineSeq| -> Result<()> {
            let mut edge_rows = Vec::with_capacity(line.len().saturating_sub(1));
            for _ in line.segment_pairs() {
                let root_index =
                    u32::try_from(source_edge).map_err(|_| ResourceError::SizeOverflow)?;
                source_edge = source_edge
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
                let zero = ExactRational::zero();
                let one = ExactRational::one();
                edge_rows.push(vec![RootRange::from_mapped_with_work(
                    RootId::from_index(root_index),
                    zero.clone(),
                    one.clone(),
                    zero,
                    one,
                    &mut work,
                )?]);
            }
            line_traces.push(SourceTrace::from_edge_rows(edge_rows, &mut work)?);
            Ok(())
        };
        match source {
            LineSource::LineString(line) => push_line(line)?,
            LineSource::MultiLineString(lines) => {
                for line in lines.iter() {
                    push_line(line)?;
                }
            },
        }
        if source_edge != inventory.source_segments.len() {
            return Err(GeometryErrorKind::Invalid(
                "subdivision root traces do not cover the original line inventory".into(),
            )
            .into());
        }
        Ok((
            Self {
                inventory,
                frame,
                root_xy_positions_needed,
            },
            line_traces,
        ))
    }
    pub(in crate::geometry) fn from_polygon_shape(
        shape: &Shape,
        frame: GeographicFrame,
        root_xy_positions_needed: bool,
    ) -> Result<(Self, Vec<SourceTrace>)> {
        let ring_ranges = polygon_source_ring_ranges(shape)?;
        let inventory = ExactSourceInventory::from_shapes(&[shape])?;
        let source_edge_count = ring_ranges.iter().try_fold(0_usize, |count, range| {
            count
                .checked_add(range.len())
                .ok_or(ResourceError::SizeOverflow)
        })?;
        if source_edge_count != inventory.source_segments.len() {
            return Err(GeometryErrorKind::Invalid(
                "polygon root traces do not cover the original ring inventory".into(),
            )
            .into());
        }
        let mut traces = Vec::with_capacity(ring_ranges.len());
        let mut source_edge = 0_usize;
        let mut work = Unmetered;
        for range in ring_ranges {
            let mut edge_rows = Vec::with_capacity(range.len());
            while source_edge < range.end {
                let root = RootId::from_index(
                    u32::try_from(source_edge).map_err(|_| ResourceError::SizeOverflow)?,
                );
                let zero = ExactRational::zero();
                let one = ExactRational::one();
                edge_rows.push(vec![RootRange::from_mapped_with_work(
                    root,
                    zero.clone(),
                    one.clone(),
                    zero,
                    one,
                    &mut work,
                )?]);
                source_edge = source_edge
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
            }
            traces.push(SourceTrace::from_edge_rows(edge_rows, &mut work)?);
        }
        Ok((
            Self {
                inventory,
                frame,
                root_xy_positions_needed,
            },
            traces,
        ))
    }

    pub(in crate::geometry) const fn root_xy_positions_needed(&self) -> bool {
        self.root_xy_positions_needed
    }

    pub(in crate::geometry) fn resolve_output_requests<M: ChartMetric>(
        &self,
        rows: &[RequestRowInput],
        metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
        work: &mut impl Work,
    ) -> Result<Vec<NodeOrdinates>> {
        let mut output = Vec::with_capacity(rows.len());
        #[cfg(test)]
        let mut request_count = 0_usize;
        #[cfg(test)]
        let mut resolved_count = 0_usize;
        for row in rows {
            work.charge_into_error(1)?;
            let mut ordinates = NodeOrdinates::default();
            let mut state = RootMetricState::default();
            let mut previous: Option<&RootRequest> = None;
            for request in &row.requests {
                work.charge_into_error(1)?;
                #[cfg(test)]
                {
                    request_count = request_count.saturating_add(1);
                }
                let duplicate = if let Some(previous) = previous {
                    previous.root() == request.root()
                        && previous
                            .parameter()
                            .try_cmp_with_work(request.parameter(), work)?
                            .is_eq()
                } else {
                    false
                };
                previous = Some(request);
                if duplicate {
                    continue;
                }
                let mut fraction = None;
                ordinates.merge(self.resolve(
                    request.root(),
                    request.parameter(),
                    row.axes,
                    metric_access,
                    &mut state,
                    &mut fraction,
                    work,
                )?);
                #[cfg(test)]
                {
                    resolved_count = resolved_count.saturating_add(1);
                }
            }
            output.push(ordinates);
        }
        #[cfg(test)]
        SUBDIVISION_RESOLUTION_BATCHES.with(|batches| {
            batches
                .borrow_mut()
                .push((rows.len(), request_count, resolved_count));
        });
        Ok(output)
    }

    fn source_pair(&self, root: RootId) -> Result<[Point; 2]> {
        if let Some(points) = self.inventory.source_points.as_ref() {
            return points
                .get(root.index())
                .map(|record| record.points)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "ordinary subdivision trace references an unknown original source edge"
                            .into(),
                    )
                    .into()
                });
        }
        let segment = self
            .inventory
            .source_segments
            .get(root.index())
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "ordinary subdivision trace references an unknown original source edge".into(),
                )
            })?;
        Ok([segment.start.point(), segment.end.point()])
    }

    fn source_displacement(
        &self,
        source: &[Point; 2],
    ) -> Result<(crate::geographic::RetainedChartDisplacement, f64)> {
        let longitude = self.frame.longitude();
        let connection = longitude
            .chart_connection(source[0].x, source[1].x)
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        let displacement = connection.retained();
        Ok((displacement, displacement.delta_native(longitude)))
    }

    fn source_position(
        &self,
        root: RootId,
        parameter: &ExactRational,
        work: &mut impl Work,
    ) -> Result<Option<XY>> {
        if parameter.is_zero() || parameter.is_one() {
            return Ok(None);
        }
        let source = self.source_pair(root)?;
        let [start, end] = source;
        let (_, delta) = self.source_displacement(&source)?;
        let parameter = parameter.round_with_work(work)?;
        let lifted_x =
            crate::geometry::interpolate_subdivision_ordinate(start.x, start.x + delta, parameter);
        let y = crate::geometry::interpolate_subdivision_ordinate(start.y, end.y, parameter);
        let x = self
            .frame
            .longitude()
            .canonicalize(lifted_x)
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?
            .get();
        Ok(Some(XY::new(x, y)))
    }

    fn source_node_position(
        &self,
        root: RootId,
        parameter: &ExactRational,
        work: &mut impl Work,
    ) -> Result<Option<XY>> {
        #[cfg(test)]
        SUBDIVISION_ROOT_XY_POSITION_CALLS.with(|calls| {
            calls.set(calls.get().saturating_add(1));
        });
        if parameter.is_zero() || parameter.is_one() {
            return Ok(None);
        }
        let source = self.source_pair(root)?;
        if self
            .frame
            .longitude()
            .ordinary_chart_edge(source[0].x, source[1].x)
        {
            return Ok(None);
        }
        let raw_delta = source[1].x - source[0].x;
        let (_, delta) = self.source_displacement(&source)?;
        if delta.to_bits() == raw_delta.to_bits() {
            return Ok(None);
        }
        self.source_position(root, parameter, work)
    }

    fn seam_parameter(&self, root: RootId, work: &mut impl Work) -> Result<Option<ExactRational>> {
        let source = self.source_pair(root)?;
        let start = source[0].x;
        if self
            .frame
            .longitude()
            .ordinary_chart_edge(source[0].x, source[1].x)
        {
            return Ok(None);
        }
        let (_, delta) = self.source_displacement(&source)?;
        if delta == 0.0 {
            return Ok(None);
        }
        let end = start + delta;
        let (low, high) = if start < end {
            (start, end)
        } else {
            (end, start)
        };
        let longitude = self.frame.longitude();
        let seam_index = ((low - longitude.half_period()) / longitude.period()).floor();
        let seam = longitude.half_period() + (seam_index + 1.0) * longitude.period();
        if seam <= low || seam >= high {
            return Ok(None);
        }
        let numerator = ExactRational::from_f64(seam)
            .try_subtract_with_work(&ExactRational::from_f64(start), work)?;
        let parameter = numerator.try_quotient_with_work(&ExactRational::from_f64(delta), work)?;
        Ok(Some(parameter))
    }
}

impl RootCatalog for SubdivisionRootCatalog {
    fn resolve<M: ChartMetric, W: Work>(
        &self,
        root: RootId,
        parameter: &ExactRational,
        axes: CoordinateAxes,
        metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
        state: &mut RootMetricState,
        fraction: &mut Option<f64>,
        work: &mut W,
    ) -> Result<NodeOrdinates> {
        if !axes.has_z() && !axes.has_m() {
            return Ok(NodeOrdinates::default());
        }
        let record = self
            .inventory
            .source_points
            .as_ref()
            .and_then(|points| points.get(root.index()))
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "ordinary subdivision trace references an unknown original source edge".into(),
                )
            })?;
        let source = record.points;
        let (displacement, _) = self.source_displacement(&source)?;
        let mut metric_state = record.metric.get();
        let result = resolve_source_root(
            self.frame,
            &source,
            displacement,
            parameter,
            axes,
            metric_access,
            &mut metric_state,
            fraction,
            work,
        );
        record.metric.set(metric_state);
        *state = metric_state;
        result
    }
}

struct SourceSpanTraceView {
    span: u32,
    reversed: bool,
    source_start: ExactRational,
    source_end: ExactRational,
}

struct SourceLineTraceViews {
    source_ids: Vec<usize>,
    views: Vec<Vec<SourceSpanTraceView>>,
}

pub(in crate::geometry) struct SubdivisionTraceRequest<'a, 'access, 'metric, M: ChartMetric, C> {
    pub(in crate::geometry) source_shape: &'a Shape,
    pub(in crate::geometry) source_traces: Option<&'a [SourceTrace]>,
    pub(in crate::geometry) roots: &'a C,
    pub(in crate::geometry) root_positions: Option<&'a SubdivisionRootCatalog>,
    pub(in crate::geometry) declared_axes: CoordinateAxes,
    pub(in crate::geometry) strict: bool,
    pub(in crate::geometry) clip_operands: [usize; 2],
    pub(in crate::geometry) context: &'a SourceArcContext<'access, 'metric, M>,
    pub(in crate::geometry) fallback_inventory: Option<&'a ExactSourceInventory>,
    pub(in crate::geometry) reuse_existing_source_vertices: bool,
}

struct PolygonNodeRootRequests {
    by_node: Vec<Vec<RootRequest>>,
    root_xy_nodes: Option<Vec<bool>>,
}

struct SourceLineSeamCut {
    parameter: ExactRational,
    coordinate: XY,
    requests: Vec<RootRequest>,
}

struct SourceLineSeamSample {
    coordinate: XY,
    requests: Vec<RootRequest>,
}

#[derive(Clone, Copy)]
enum TracedSourceLineVertex {
    Node(NodeId),
    Seam(usize),
}

struct TracedSourceLineRun {
    member: usize,
    vertices: Vec<TracedSourceLineVertex>,
    trace_slices: Vec<(usize, ExactRational, ExactRational)>,
}

fn mapped_root_ranges_between_requests<W: Work>(
    start_requests: &[RootRequest],
    end_requests: &[RootRequest],
    work: &mut W,
) -> Result<Vec<RootRange>> {
    let zero = ExactRational::zero();
    let one = ExactRational::one();
    let mut ranges = Vec::new();
    for start in start_requests {
        work.charge_into_error(1)?;
        let Some(end) = end_requests
            .iter()
            .find(|request| request.root() == start.root())
        else {
            continue;
        };
        ranges.push(RootRange::from_mapped_with_work(
            start.root(),
            start.parameter().clone(),
            end.parameter().clone(),
            zero.clone(),
            one.clone(),
            work,
        )?);
    }
    Ok(ranges)
}

struct SourceLineRun {
    member: usize,
    nodes: Vec<NodeId>,
    trace_slices: Vec<(usize, ExactRational, ExactRational)>,
}

fn push_source_line_run<W: Work + ?Sized>(
    runs: &mut Vec<SourceLineRun>,
    member: usize,
    nodes: &mut Vec<NodeId>,
    trace_slices: &mut Vec<(usize, ExactRational, ExactRational)>,
    traced: bool,
    work: &mut W,
) -> Result<()> {
    if nodes.len() < 2 {
        nodes.clear();
        trace_slices.clear();
        return Ok(());
    }
    if traced {
        let expected_edges = nodes
            .len()
            .checked_sub(1)
            .ok_or(ResourceError::SizeOverflow)?;
        if trace_slices.len() != expected_edges {
            return Err(GeometryErrorKind::Invalid(
                "source line run vertices do not match their trace slices".into(),
            )
            .into());
        }
        work.charge_into_error(1)?;
    } else if !trace_slices.is_empty() {
        return Err(
            GeometryErrorKind::Invalid("untraced source line run has trace slices".into()).into(),
        );
    }
    runs.push(SourceLineRun {
        member,
        nodes: std::mem::take(nodes),
        trace_slices: std::mem::take(trace_slices),
    });
    Ok(())
}

pub(in crate::geometry) struct SubdivisionPart {
    pub shape: Shape,
    pub traces: Option<Vec<SourceTrace>>,
    pub polygon_traces: Option<Vec<SourceTrace>>,
}

fn resolve_traced_node_requests<'e, 'p, M: ChartMetric, C: RootCatalog, W: Work, F>(
    partition: &'p ExactPartition<'_>,
    embedding: &'e embedding::Embedding<'p>,
    nodes: &[NodeId],
    mut requests_by_node: Vec<Vec<RootRequest>>,
    request: &SubdivisionTraceRequest<'_, '_, '_, M, C>,
    root_xy_nodes: Option<&[bool]>,
    work: &mut W,
    resolve_rows: &mut F,
) -> Result<ResolvedNodePoints<'e, 'p>>
where
    F: FnMut(&[RequestRowInput], &mut W) -> Result<Vec<NodeOrdinates>>,
{
    if request.root_positions.is_some() != root_xy_nodes.is_some() {
        return Err(GeometryErrorKind::Invalid(
            "subdivision root positions and node selection disagree".into(),
        )
        .into());
    }
    let has_root_xy_nodes = if let Some(root_xy_nodes) = root_xy_nodes {
        if root_xy_nodes.len() != partition.node_count() {
            return Err(GeometryErrorKind::Invalid(
                "subdivision root-position node selection has the wrong length".into(),
            )
            .into());
        }
        let mut any = false;
        for &node in nodes {
            any |= *root_xy_nodes.get(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "subdivision root-position node is outside its partition".into(),
                )
            })?;
        }
        any
    } else {
        false
    };
    if !request.declared_axes.has_z() && !request.declared_axes.has_m() && !has_root_xy_nodes {
        return Ok(ResolvedNodePoints::from_embedding(embedding, None));
    }
    work.charge_into_error(
        u64::try_from(partition.node_count()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let rooted_xy = if let Some(root_positions) = request.root_positions
        && has_root_xy_nodes
    {
        let root_xy_nodes = root_xy_nodes.ok_or_else(|| {
            GeometryErrorKind::Invalid("subdivision root-position node selection is missing".into())
        })?;
        let mut rooted_xy = vec![None; partition.node_count()];
        for &node in nodes {
            if !root_xy_nodes.get(node.0 as usize).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "subdivision root-position node is outside its partition".into(),
                )
            })? {
                continue;
            }
            let requests = requests_by_node.get(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("subdivision emission node is unknown".into())
            })?;
            let mut position = None;
            let mut agrees = true;
            for request in requests {
                let Some(candidate) = root_positions.source_node_position(
                    request.root(),
                    request.parameter(),
                    work,
                )?
                else {
                    continue;
                };
                if position.is_some_and(|previous| !same_point(previous, candidate)) {
                    agrees = false;
                    break;
                }
                position = Some(candidate);
            }
            if agrees {
                let slot = rooted_xy.get_mut(node.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "subdivision source position node is outside its partition".into(),
                    )
                })?;
                *slot = position;
            }
        }
        Some(rooted_xy)
    } else {
        None
    };
    if !request.declared_axes.has_z() && !request.declared_axes.has_m() {
        return Ok(rooted_xy.map_or_else(
            || ResolvedNodePoints::from_embedding(embedding, None),
            |rooted_xy| ResolvedNodePoints::from_embedding_with_xy(embedding, None, rooted_xy),
        ));
    }
    let mut by_node = vec![None; partition.node_count()];
    let mut fallback_nodes = Vec::new();
    let mut request_rows = Vec::with_capacity(nodes.len());
    let mut request_row_nodes = Vec::with_capacity(nodes.len());
    for &node in nodes {
        work.charge_into_error(1)?;
        if request.reuse_existing_source_vertices
            && let Some(fallback_inventory) = request.fallback_inventory
            && let Some(ordinates) = fallback_inventory.existing_source_vertex_ordinates(
                partition,
                node,
                request.declared_axes,
                0,
            )?
            && (!request.declared_axes.has_z() || ordinates.z.value().is_some())
            && (!request.declared_axes.has_m() || ordinates.m.value().is_some())
        {
            let slot = by_node.get_mut(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "existing subdivision vertex is outside the partition".into(),
                )
            })?;
            *slot = Some((
                ZOrdinate(ordinates.z.value()),
                MOrdinate(ordinates.m.value()),
            ));
            continue;
        }
        let requests = requests_by_node.get_mut(node.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("subdivision emission node is unknown".into())
        })?;
        if requests.is_empty() {
            if request.fallback_inventory.is_some() {
                fallback_nodes.push(node);
            }
            continue;
        }
        request_rows.push(RequestRowInput {
            axes: request.declared_axes,
            requests: std::mem::take(requests),
        });
        request_row_nodes.push(node);
    }
    let ordinates = resolve_rows(&request_rows, work)?;
    if ordinates.len() != request_rows.len() {
        return Err(GeometryErrorKind::Invalid(
            "original-root resolution returned incomplete traced-node ordinates".into(),
        )
        .into());
    }
    for (node, ordinate) in request_row_nodes.into_iter().zip(ordinates) {
        let slot = by_node.get_mut(node.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("subdivision output node is outside its partition".into())
        })?;
        *slot = Some((ZOrdinate(ordinate.z.value()), MOrdinate(ordinate.m.value())));
    }
    if let Some(fallback_inventory) = request.fallback_inventory
        && !fallback_nodes.is_empty()
    {
        work.charge_into_error(
            u64::try_from(fallback_nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let fallback = fallback_inventory.resolve_node_points(
            partition,
            embedding,
            fallback_nodes.iter().copied(),
            request.declared_axes,
            request.context,
            None,
        )?;
        for node in fallback_nodes {
            if let Some(ordinates) = fallback.ordinates(node) {
                let slot = by_node.get_mut(node.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "fallback subdivision node is outside the partition".into(),
                    )
                })?;
                *slot = Some(ordinates);
            }
        }
    }
    Ok(match rooted_xy {
        Some(rooted_xy) => {
            ResolvedNodePoints::from_embedding_with_xy(embedding, Some(by_node), rooted_xy)
        },
        None => ResolvedNodePoints::from_embedding(embedding, Some(by_node)),
    })
}

fn mark_root_xy_node(
    root_xy_nodes: &mut [bool],
    node: NodeId,
    local_parameter: &ExactRational,
    appended_requests: usize,
) -> Result<()> {
    if appended_requests == 0 || local_parameter.is_zero() || local_parameter.is_one() {
        return Ok(());
    }
    let slot = root_xy_nodes.get_mut(node.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("subdivision root-position node is outside its partition".into())
    })?;
    *slot = true;
    Ok(())
}

impl ExactSourceInventory {
    fn existing_source_vertex_ordinates(
        &self,
        partition: &ExactPartition<'_>,
        node: NodeId,
        axes: CoordinateAxes,
        source_scope: u32,
    ) -> Result<Option<NodeOrdinates>> {
        if !axes.has_z() && !axes.has_m() {
            return Ok(None);
        }
        let Some(node_key) = partition.points.stored_key(node.0) else {
            return Ok(None);
        };
        let mut ordinates = NodeOrdinates::default();
        let mut found = false;
        for &unique in self.node_bindings(node, partition) {
            for &(source, _) in self.provenance.owners(unique as usize) {
                if self.source_operands.get(source as usize).copied() != Some(source_scope) {
                    continue;
                }
                let Some(points) = self.source_ordinate_points(source)? else {
                    continue;
                };
                let [first, _] = points;
                if axes.has_z() && !first.axes().has_z() || axes.has_m() && !first.axes().has_m() {
                    continue;
                }
                for point in points {
                    if PointKey::new(*point) != node_key {
                        continue;
                    }
                    found = true;
                    if axes.has_z() {
                        ordinates.z.observe(point.z());
                    }
                    if axes.has_m() {
                        ordinates.m.observe(point.m());
                    }
                }
            }
        }
        for &(bound, source) in &partition.point_sources {
            if bound != node
                || self.source_operands.get(source as usize).copied() != Some(source_scope)
            {
                continue;
            }
            let Some(points) = self.source_ordinate_points(source)? else {
                continue;
            };
            let [first, _] = points;
            if axes.has_z() && !first.axes().has_z() || axes.has_m() && !first.axes().has_m() {
                continue;
            }
            for point in points {
                if PointKey::new(*point) != node_key {
                    continue;
                }
                found = true;
                if axes.has_z() {
                    ordinates.z.observe(point.z());
                }
                if axes.has_m() {
                    ordinates.m.observe(point.m());
                }
            }
        }
        for (bound, point, operand) in self.isolated_point_occurrences(partition) {
            if bound != node
                || operand != source_scope
                || PointKey::new(point) != node_key
                || axes.has_z() && !point.axes().has_z()
                || axes.has_m() && !point.axes().has_m()
            {
                continue;
            }
            found = true;
            if axes.has_z() {
                ordinates.z.observe(point.z());
            }
            if axes.has_m() {
                ordinates.m.observe(point.m());
            }
        }
        Ok(found.then_some(ordinates))
    }

    pub(in crate::geometry) fn subdivision_parts<M: ChartMetric>(
        &self,
        partition: &ExactPartition<'_>,
        source_shape: &Shape,
        declared_axes: crate::geometry::CoordinateAxes,
        strict: bool,
        clip_operands: [usize; 2],
        budget: &mut ExpansionBudget,
        context: &SourceArcContext<'_, '_, M>,
    ) -> Result<[Shape; 2]> {
        let geographic_frame = match context.domain() {
            crate::geometry::TopologyDomain::Cartesian => None,
            crate::geometry::TopologyDomain::Geographic(frame) => Some(frame),
        };
        let source_operand = 0;
        let operands = self.operand_count();
        if clip_operands
            .iter()
            .any(|&operand| operand >= operands || operand == source_operand)
        {
            return Err(GeometryErrorKind::Invalid(
                "exact subdivision clip operand is out of range".into(),
            )
            .into());
        }

        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let winding_rings = self.winding_source_rings();
        let facts =
            self.build_cell_facts(partition, &topology, &edges, &global, &winding_rings, false)?;
        if let Some(line_source) = LineSource::from_shape(source_shape) {
            return self.subdivision_line_parts(
                partition,
                line_source,
                &topology,
                &global,
                &facts,
                declared_axes,
                strict,
                clip_operands,
                context,
            );
        }
        let ordinate_protected = self.ordinate_protected_nodes(partition, declared_axes)?;
        let preserved_spans = geographic_frame.is_some().then(|| {
            let mut roles = Vec::with_capacity(partition.spans.len());
            for span in 0..partition.spans.len() {
                roles.push(
                    partition
                        .provenance
                        .owners(span)
                        .iter()
                        .any(|&(source, _)| {
                            self.source_operands.get(source as usize).copied()
                                == Some(source_operand as u32)
                        }),
                );
            }
            roles
        });

        let mut plans = Vec::with_capacity(clip_operands.len());
        for &clip_operand in &clip_operands {
            let cells = self.select_subdivision_cells(
                partition,
                &topology,
                &global,
                &facts,
                source_operand,
                clip_operand,
            );
            plans.push(cells.emission_plan_with_protected(
                partition,
                &topology,
                &edges,
                &global,
                ordinate_protected.as_deref(),
                preserved_spans.as_deref(),
            )?);
        }
        let polygon_edge_roles = preserved_spans
            .as_ref()
            .map(|preserved_spans| {
                plans
                    .iter()
                    .map(|plan| {
                        plan.polygon_edge_roles(partition, &topology, &edges, preserved_spans)
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?;

        let emissions: Vec<_> = plans
            .iter()
            .map(|plan| plan.emission(&topology, &edges))
            .collect();
        let emission_refs: Vec<_> = emissions.iter().collect();
        let embedding = partition.embed(&emission_refs, "subdivide")?;

        let mut node_parts = Vec::with_capacity(plans.len());
        for (plan, emission) in plans.iter().zip(&emissions) {
            node_parts.push(plan.build_node_parts(partition, &topology, emission)?);
        }

        let by_node = self.resolve_node_points(
            partition,
            &embedding,
            emissions
                .iter()
                .flat_map(|emission| emission.items.iter().flatten().copied()),
            declared_axes,
            context,
            None,
        )?;
        let mut parts = node_parts.into_iter();
        let mut polygon_edge_roles = polygon_edge_roles.unwrap_or_default().into_iter();
        let first_roles = polygon_edge_roles.next();
        let first_publication = match (first_roles, geographic_frame) {
            (Some(roles), Some(frame)) => Some((roles, frame, &mut *budget)),
            _ => None,
        };
        let first = self.node_parts_to_shape_resolved(
            parts
                .next()
                .expect("two subdivision plans produce two outputs"),
            &embedding,
            &by_node,
            declared_axes,
            strict,
            "subdivide",
            first_publication,
        )?;
        let second_roles = polygon_edge_roles.next();
        let second_publication = match (second_roles, geographic_frame) {
            (Some(roles), Some(frame)) => Some((roles, frame, &mut *budget)),
            _ => None,
        };
        let second = self.node_parts_to_shape_resolved(
            parts
                .next()
                .expect("two subdivision plans produce two outputs"),
            &embedding,
            &by_node,
            declared_axes,
            strict,
            "subdivide",
            second_publication,
        )?;
        Ok([first, second])
    }

    pub(in crate::geometry) fn subdivision_parts_with_roots<M: ChartMetric>(
        &self,
        partition: &ExactPartition<'_>,
        request: &SubdivisionTraceRequest<'_, '_, '_, M, SubdivisionRootCatalog>,
        budget: &mut ExpansionBudget,
        trace_work: &mut Unmetered,
    ) -> Result<[SubdivisionPart; 2]> {
        let roots = request.roots;
        let context = request.context;
        let mut resolve_rows = |rows: &[RequestRowInput], work: &mut Unmetered| {
            roots.resolve_output_requests(rows, context.metric_access(), work)
        };
        if request.source_traces.is_none() {
            return self.subdivision_parts_traced(
                partition,
                request,
                budget,
                trace_work,
                &mut resolve_rows,
            );
        }
        if LineSource::from_shape(request.source_shape).is_some() {
            return self.subdivision_parts_traced(
                partition,
                request,
                budget,
                trace_work,
                &mut resolve_rows,
            );
        }
        self.subdivision_polygon_parts_with_roots(partition, request, budget, trace_work)
    }

    pub(in crate::geometry) fn subdivision_polygon_parts_with_roots<M: ChartMetric>(
        &self,
        partition: &ExactPartition<'_>,
        request: &SubdivisionTraceRequest<'_, '_, '_, M, SubdivisionRootCatalog>,
        budget: &mut ExpansionBudget,
        trace_work: &mut Unmetered,
    ) -> Result<[SubdivisionPart; 2]> {
        let source_shape = request.source_shape;
        let source_traces = request.source_traces.ok_or_else(|| {
            GeometryErrorKind::Invalid("polygon subdivision source traces are missing".into())
        })?;
        let roots = request.roots;
        let declared_axes = request.declared_axes;
        let strict = request.strict;
        let clip_operands = request.clip_operands;
        let context = request.context;
        let ring_ranges = polygon_source_ring_ranges(source_shape)?;
        if source_traces.len() != ring_ranges.len() {
            return Err(GeometryErrorKind::Invalid(
                "polygon source traces do not match the shell and hole rings".into(),
            )
            .into());
        }
        for (range, trace) in ring_ranges.iter().zip(source_traces) {
            if trace.edge_count() != range.len() {
                return Err(GeometryErrorKind::Invalid(
                    "polygon source trace edge count differs from its ring".into(),
                )
                .into());
            }
        }
        let operands = self.operand_count();
        if clip_operands
            .iter()
            .any(|&operand| operand >= operands || operand == 0)
        {
            return Err(GeometryErrorKind::Invalid(
                "exact subdivision clip operand is out of range".into(),
            )
            .into());
        }

        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let winding_rings = self.winding_source_rings();
        let facts =
            self.build_cell_facts(partition, &topology, &edges, &global, &winding_rings, false)?;
        let protected = self.ordinate_protected_nodes(partition, declared_axes)?;
        let preserved_spans = {
            let mut roles = Vec::with_capacity(partition.spans.len());
            for span in 0..partition.spans.len() {
                roles.push(
                    partition
                        .provenance
                        .owners(span)
                        .iter()
                        .any(|&(source, _)| {
                            self.source_operands.get(source as usize).copied() == Some(0)
                        }),
                );
            }
            roles
        };
        let mut plans = Vec::with_capacity(clip_operands.len());
        for &clip_operand in &clip_operands {
            let cells = self.select_subdivision_cells(
                partition,
                &topology,
                &global,
                &facts,
                0,
                clip_operand,
            );
            plans.push(cells.emission_plan_with_protected(
                partition,
                &topology,
                &edges,
                &global,
                protected.as_deref(),
                Some(&preserved_spans),
            )?);
        }
        let polygon_edge_roles = plans
            .iter()
            .map(|plan| plan.polygon_edge_roles(partition, &topology, &edges, &preserved_spans))
            .collect::<Result<Vec<_>>>()?;
        let emissions: Vec<_> = plans
            .iter()
            .map(|plan| plan.emission(&topology, &edges))
            .collect();
        let emission_refs: Vec<_> = emissions.iter().collect();
        let embedding = partition.embed(&emission_refs, "subdivide")?;
        let mut node_parts = Vec::with_capacity(plans.len());
        for (plan, emission) in plans.iter().zip(&emissions) {
            node_parts.push(plan.build_node_parts(partition, &topology, emission)?);
        }
        trace_work.charge_into_error(
            u64::try_from(partition.node_count()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut nodes = Vec::new();
        let mut seen = vec![false; partition.node_count()];
        for emission in &emissions {
            for &[start, end] in &emission.items {
                for node in [start, end] {
                    trace_work.charge_into_error(1)?;
                    let admitted = seen.get_mut(node.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "subdivision emission references an unknown node".into(),
                        )
                    })?;
                    if !*admitted {
                        *admitted = true;
                        nodes.push(node);
                    }
                }
            }
        }
        let views = self.source_line_trace_views(partition, trace_work)?;
        let root_positions = request.root_positions;
        let polygon_requests = self.polygon_node_root_requests(
            partition,
            &nodes,
            &ring_ranges,
            source_traces,
            &views,
            root_positions.is_some(),
            trace_work,
        )?;
        let mut polygon_traces = Vec::with_capacity(plans.len());
        for (parts, roles) in node_parts.iter().zip(&polygon_edge_roles) {
            polygon_traces.push(self.polygon_ring_traces_from_nodes(
                &parts.polygons,
                roles,
                &polygon_requests.by_node,
                trace_work,
            )?);
        }
        let mut resolve_rows = |rows: &[RequestRowInput], work: &mut Unmetered| {
            roots.resolve_output_requests(rows, context.metric_access(), work)
        };
        let by_node = resolve_traced_node_requests(
            partition,
            &embedding,
            &nodes,
            polygon_requests.by_node,
            request,
            polygon_requests.root_xy_nodes.as_deref(),
            trace_work,
            &mut resolve_rows,
        )?;
        let old_polygon_xy = node_parts
            .iter()
            .map(|parts| self.polygon_node_xy(&parts.polygons, &by_node))
            .collect::<Result<Vec<_>>>()?;
        let first_old_polygon_xy = old_polygon_xy.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("first subdivision polygon trace is unknown".into())
        })?;
        let second_old_polygon_xy = old_polygon_xy.get(1).ok_or_else(|| {
            GeometryErrorKind::Invalid("second subdivision polygon trace is unknown".into())
        })?;
        let mut node_parts = node_parts.into_iter();
        let mut polygon_edge_roles = polygon_edge_roles.into_iter();
        let first_roles = polygon_edge_roles.next().ok_or_else(|| {
            GeometryErrorKind::Invalid("first polygon edge-role plan is unknown".into())
        })?;
        let first_publication = Some((first_roles.clone(), roots.frame, &mut *budget));
        let first = self.node_parts_to_shape_resolved(
            node_parts.next().ok_or_else(|| {
                GeometryErrorKind::Invalid("first subdivision polygon part is unknown".into())
            })?,
            &embedding,
            &by_node,
            declared_axes,
            strict,
            "subdivide",
            first_publication,
        )?;
        let first_traces = self.align_published_polygon_traces(
            &first,
            first_old_polygon_xy,
            &first_roles,
            polygon_traces.remove(0),
            roots,
            roots.frame,
            trace_work,
        )?;
        let first_traces = (!first_traces.is_empty()).then_some(first_traces);
        let second_roles = polygon_edge_roles.next().ok_or_else(|| {
            GeometryErrorKind::Invalid("second polygon edge-role plan is unknown".into())
        })?;
        let second_publication = Some((second_roles.clone(), roots.frame, &mut *budget));
        let second = self.node_parts_to_shape_resolved(
            node_parts.next().ok_or_else(|| {
                GeometryErrorKind::Invalid("second subdivision polygon part is unknown".into())
            })?,
            &embedding,
            &by_node,
            declared_axes,
            strict,
            "subdivide",
            second_publication,
        )?;
        let second_traces = self.align_published_polygon_traces(
            &second,
            second_old_polygon_xy,
            &second_roles,
            polygon_traces.remove(0),
            roots,
            roots.frame,
            trace_work,
        )?;
        let second_traces = (!second_traces.is_empty()).then_some(second_traces);
        Ok([
            SubdivisionPart {
                shape: first,
                traces: None,
                polygon_traces: first_traces,
            },
            SubdivisionPart {
                shape: second,
                traces: None,
                polygon_traces: second_traces,
            },
        ])
    }

    fn subdivision_parts_traced<
        M: ChartMetric,
        C: RootCatalog,
        F: FnMut(&[RequestRowInput], &mut Unmetered) -> Result<Vec<NodeOrdinates>>,
    >(
        &self,
        partition: &ExactPartition<'_>,
        request: &SubdivisionTraceRequest<'_, '_, '_, M, C>,
        budget: &mut ExpansionBudget,
        trace_work: &mut Unmetered,
        resolve_rows: &mut F,
    ) -> Result<[SubdivisionPart; 2]> {
        let Some(source_traces) = request.source_traces else {
            let [left, right] = self.subdivision_parts(
                partition,
                request.source_shape,
                request.declared_axes,
                request.strict,
                request.clip_operands,
                budget,
                request.context,
            )?;
            return Ok([
                SubdivisionPart {
                    shape: left,
                    traces: None,
                    polygon_traces: None,
                },
                SubdivisionPart {
                    shape: right,
                    traces: None,
                    polygon_traces: None,
                },
            ]);
        };
        let source = LineSource::from_shape(request.source_shape).ok_or_else(|| {
            GeometryErrorKind::Invalid("source trace was attached to a non-line shape".into())
        })?;
        let ranges = source_line_ranges(source)?;
        if source_traces.len() != ranges.len() {
            return Err(GeometryErrorKind::Invalid(
                "source traces do not match the subdivision line members".into(),
            )
            .into());
        }
        let operands = self.operand_count();
        if request
            .clip_operands
            .iter()
            .any(|&operand| operand >= operands || operand == 0)
        {
            return Err(GeometryErrorKind::Invalid(
                "exact subdivision clip operand is out of range".into(),
            )
            .into());
        }
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let winding_rings = self.winding_source_rings();
        let facts =
            self.build_cell_facts(partition, &topology, &edges, &global, &winding_rings, false)?;
        let mut selected = Vec::with_capacity(request.clip_operands.len());
        for &clip_operand in &request.clip_operands {
            let cells = self.select_subdivision_cells(
                partition,
                &topology,
                &global,
                &facts,
                0,
                clip_operand,
            );
            selected.push(cells.spans);
        }
        let emissions: Vec<_> = selected
            .iter()
            .map(|spans| {
                partition.span_emission(
                    spans
                        .iter()
                        .enumerate()
                        .filter_map(|(span, &keep)| keep.then_some(span as u32)),
                )
            })
            .collect();
        let emission_refs: Vec<_> = emissions.iter().collect();
        trace_work.charge_into_error(
            u64::try_from(partition.node_count()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut nodes = Vec::new();
        let mut seen = vec![false; partition.node_count()];
        for emission in &emissions {
            for &[start, end] in &emission.items {
                for node in [start, end] {
                    trace_work.charge_into_error(1)?;
                    let admitted = seen.get_mut(node.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "subdivision emission references an unknown node".into(),
                        )
                    })?;
                    if !*admitted {
                        *admitted = true;
                        nodes.push(node);
                    }
                }
            }
        }
        let embedding = partition.embed(&emission_refs, "subdivide")?;
        let views = self.source_line_trace_views(partition, trace_work)?;
        let mut run_plans = Vec::with_capacity(selected.len());
        for spans in &selected {
            run_plans.push(self.source_line_runs(
                partition,
                &ranges,
                &views,
                spans,
                source_traces,
                trace_work,
            )?);
        }
        let by_node = self.resolve_traced_source_line_points(
            partition,
            &embedding,
            &nodes,
            &run_plans,
            source_traces,
            request,
            trace_work,
            resolve_rows,
        )?;
        let mut parts = Vec::with_capacity(run_plans.len());
        for runs in run_plans {
            parts.push(self.emit_traced_source_line_shape(
                source.kind(),
                runs,
                source_traces,
                &by_node,
                request,
                trace_work,
            )?);
        }
        let mut parts = parts.into_iter();
        Ok([
            parts.next().expect("two subdivision selections"),
            parts.next().expect("two subdivision selections"),
        ])
    }

    fn source_line_trace_views<W: Work + ?Sized>(
        &self,
        partition: &ExactPartition<'_>,
        work: &mut W,
    ) -> Result<SourceLineTraceViews> {
        let source_count = self
            .source_operands
            .iter()
            .filter(|&&operand| operand == 0)
            .count();
        let mut source_ids = Vec::with_capacity(source_count);
        for (source, &operand) in self.source_operands.iter().enumerate() {
            work.charge_into_error(1)?;
            if operand == 0 {
                source_ids.push(source);
            }
        }
        let views = partition.source_span_trace_views(&self.source_segments, &source_ids, work)?;
        if views.len() != source_ids.len() {
            return Err(GeometryErrorKind::Invalid(
                "ordinary source trace views do not match the source rows".into(),
            )
            .into());
        }
        Ok(SourceLineTraceViews { source_ids, views })
    }

    fn polygon_node_root_requests<W: Work>(
        &self,
        partition: &ExactPartition<'_>,
        nodes: &[NodeId],
        ring_ranges: &[Range<usize>],
        ring_traces: &[SourceTrace],
        views: &SourceLineTraceViews,
        track_root_xy_nodes: bool,
        work: &mut W,
    ) -> Result<PolygonNodeRootRequests> {
        if ring_ranges.len() != ring_traces.len() {
            return Err(GeometryErrorKind::Invalid(
                "polygon source ring ranges do not match their root traces".into(),
            )
            .into());
        }
        let edge_count = ring_ranges.iter().try_fold(0_usize, |count, range| {
            count
                .checked_add(range.len())
                .ok_or(ResourceError::SizeOverflow)
        })?;
        if edge_count != views.source_ids.len() || views.source_ids.len() != views.views.len() {
            return Err(GeometryErrorKind::Invalid(
                "polygon operand-0 source rows do not match the ring trace inventory".into(),
            )
            .into());
        }
        let mut ring_of_source = vec![None; self.source_segments.len()];
        for (ring, range) in ring_ranges.iter().enumerate() {
            for source in range.clone() {
                let slot = ring_of_source.get_mut(source).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "polygon ring ranges are outside the source segment inventory".into(),
                    )
                })?;
                *slot = Some((
                    ring,
                    source
                        .checked_sub(range.start)
                        .ok_or(ResourceError::SizeOverflow)?,
                ));
            }
        }
        let mut visible_nodes = vec![false; partition.node_count()];
        for &node in nodes {
            let slot = visible_nodes.get_mut(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("polygon emission node is unknown".into())
            })?;
            *slot = true;
        }
        let mut requests_by_node = Vec::new();
        requests_by_node.resize_with(partition.node_count(), Vec::new);
        let mut root_xy_nodes = track_root_xy_nodes.then(|| vec![false; partition.node_count()]);
        for (&full_source, source_views) in views.source_ids.iter().zip(&views.views) {
            work.charge_into_error(1)?;
            if self.source_operands.get(full_source).copied() != Some(0) {
                return Err(GeometryErrorKind::Invalid(
                    "polygon trace remap includes a clip-operand source".into(),
                )
                .into());
            }
            let (ring, edge) = ring_of_source
                .get(full_source)
                .copied()
                .flatten()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "polygon source remap has no shell or hole trace".into(),
                    )
                })?;
            let trace = ring_traces.get(ring).ok_or_else(|| {
                GeometryErrorKind::Invalid("polygon source ring trace is unknown".into())
            })?;
            let ring_range = ring_ranges.get(ring).ok_or_else(|| {
                GeometryErrorKind::Invalid("polygon ring source range is unknown".into())
            })?;
            if trace.edge_count() != ring_range.len() {
                return Err(GeometryErrorKind::Invalid(
                    "polygon source trace edge count differs from its ring".into(),
                )
                .into());
            }
            for view in source_views {
                work.charge_into_error(1)?;
                let span = *partition.spans.get(view.span as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("polygon source trace span is unknown".into())
                })?;
                let endpoints = if view.reversed {
                    [
                        (span.start, &view.source_end),
                        (span.end, &view.source_start),
                    ]
                } else {
                    [
                        (span.start, &view.source_start),
                        (span.end, &view.source_end),
                    ]
                };
                for (node, parameter) in endpoints {
                    if !visible_nodes.get(node.0 as usize).copied().unwrap_or(false) {
                        continue;
                    }
                    let row = requests_by_node.get_mut(node.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "polygon source trace node is outside the partition".into(),
                        )
                    })?;
                    let added = trace.append_requests_at(edge, parameter, row, work)?;
                    work.charge_into_error(
                        u64::try_from(added).map_err(|_| ResourceError::SizeOverflow)?,
                    )?;
                    if let Some(root_xy_nodes) = root_xy_nodes.as_mut() {
                        mark_root_xy_node(root_xy_nodes, node, parameter, added)?;
                    }
                }
            }
        }
        Ok(PolygonNodeRootRequests {
            by_node: requests_by_node,
            root_xy_nodes,
        })
    }

    fn polygon_ring_traces_from_nodes<W: Work>(
        &self,
        polygons: &[Vec<Vec<NodeId>>],
        computed_edges: &[Vec<Vec<bool>>],
        requests_by_node: &[Vec<RootRequest>],
        work: &mut W,
    ) -> Result<Vec<SourceTrace>> {
        if polygons.len() != computed_edges.len() {
            return Err(GeometryErrorKind::Invalid(
                "polygon ring traces do not match the emitted polygon parts".into(),
            )
            .into());
        }
        let ring_count = polygons.iter().try_fold(0_usize, |count, polygon| {
            count
                .checked_add(polygon.len())
                .ok_or(ResourceError::SizeOverflow)
        })?;
        let mut traces = Vec::with_capacity(ring_count);
        for (polygon_index, polygon) in polygons.iter().enumerate() {
            let roles = computed_edges.get(polygon_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("polygon edge-role part is unknown".into())
            })?;
            if roles.len() != polygon.len() {
                return Err(GeometryErrorKind::Invalid(
                    "polygon edge roles do not match shell and hole rings".into(),
                )
                .into());
            }
            for (ring, roles) in polygon.iter().zip(roles) {
                let edge_count = ring.len().saturating_sub(1);
                if roles.len() != edge_count {
                    return Err(GeometryErrorKind::Invalid(
                        "polygon edge roles do not match a closed ring".into(),
                    )
                    .into());
                }
                let mut edge_rows = Vec::with_capacity(edge_count);
                for (edge, &computed) in roles.iter().enumerate() {
                    work.charge_into_error(1)?;
                    if computed {
                        edge_rows.push(Vec::new());
                        continue;
                    }
                    let start = *ring.get(edge).ok_or_else(|| {
                        GeometryErrorKind::Invalid("polygon trace start node is unknown".into())
                    })?;
                    let end = *ring.get(edge.wrapping_add(1)).ok_or_else(|| {
                        GeometryErrorKind::Invalid("polygon trace end node is unknown".into())
                    })?;
                    let start_requests =
                        requests_by_node.get(start.0 as usize).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "polygon trace start node has no source request row".into(),
                            )
                        })?;
                    let end_requests = requests_by_node.get(end.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "polygon trace end node has no source request row".into(),
                        )
                    })?;
                    edge_rows.push(mapped_root_ranges_between_requests(
                        start_requests,
                        end_requests,
                        work,
                    )?);
                }
                traces.push(SourceTrace::from_edge_rows(edge_rows, work)?);
            }
        }
        Ok(traces)
    }

    fn polygon_node_xy(
        &self,
        polygons: &[Vec<Vec<NodeId>>],
        by_node: &ResolvedNodePoints<'_, '_>,
    ) -> Result<Vec<Vec<Vec<XY>>>> {
        let coordinates = by_node.coordinates();
        polygons
            .iter()
            .map(|polygon| {
                polygon
                    .iter()
                    .map(|ring| {
                        ring.iter()
                            .map(|&node| {
                                Ok(by_node
                                    .point(node, embedding::Embedding::xy_at(coordinates, node))
                                    .xy())
                            })
                            .collect()
                    })
                    .collect()
            })
            .collect()
    }

    fn align_published_polygon_traces<W: Work>(
        &self,
        shape: &Shape,
        old_xy: &[Vec<Vec<XY>>],
        computed_edges: &[Vec<Vec<bool>>],
        source_traces: Vec<SourceTrace>,
        roots: &SubdivisionRootCatalog,
        frame: GeographicFrame,
        work: &mut W,
    ) -> Result<Vec<SourceTrace>> {
        let mut polygons = Vec::new();
        collect_polygon_parts(shape, &mut polygons);
        if polygons.len() != old_xy.len() || polygons.len() != computed_edges.len() {
            return Err(GeometryErrorKind::Invalid(
                "published polygons do not match their subdivision trace rows".into(),
            )
            .into());
        }
        let trace_count = source_traces.len();
        let mut source_traces = source_traces.into_iter();
        let mut traces = Vec::with_capacity(trace_count);
        for (polygon_index, polygon) in polygons.into_iter().enumerate() {
            let old_rings = old_xy.get(polygon_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("unpublished polygon trace rows are unknown".into())
            })?;
            let edge_roles = computed_edges.get(polygon_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("published polygon edge roles are unknown".into())
            })?;
            let published_rings = polygon.rings().collect::<Vec<_>>();
            if old_rings.len() != edge_roles.len() || published_rings.len() != old_rings.len() {
                return Err(GeometryErrorKind::Invalid(
                    "published shell and hole rows do not match their source roles".into(),
                )
                .into());
            }
            for ((old_ring, roles), published_ring) in
                old_rings.iter().zip(edge_roles).zip(published_rings)
            {
                let source_trace = source_traces.next().ok_or_else(|| {
                    GeometryErrorKind::Invalid("polygon ring source trace is unknown".into())
                })?;
                let old_edge_count = old_ring.len().saturating_sub(1);
                if roles.len() != old_edge_count || source_trace.edge_count() != old_edge_count {
                    return Err(GeometryErrorKind::Invalid(
                        "polygon trace rows do not match their stored ring edges".into(),
                    )
                    .into());
                }
                let published = published_ring.coords();
                let published_edge_count = published.coord_count().saturating_sub(1);
                if published_edge_count == 0 {
                    return Err(GeometryErrorKind::Invalid(
                        "published polygon ring has no edges".into(),
                    )
                    .into());
                }
                let source_start = *old_ring.first().ok_or_else(|| {
                    GeometryErrorKind::Invalid("published polygon source ring is empty".into())
                })?;
                let mut published_start = None;
                for index in 0..published_edge_count {
                    if same_published_polygon_xy(
                        frame,
                        published.nth_coord(index).xy(),
                        source_start,
                    )? {
                        published_start = Some(index);
                        break;
                    }
                }
                let Some(published_start) = published_start else {
                    return Err(GeometryErrorKind::Invalid(
                        "published polygon ring lost its source start vertex".into(),
                    )
                    .into());
                };

                let mut slices = Vec::with_capacity(published_edge_count);
                let zero = ExactRational::zero();
                let one = ExactRational::one();
                let mut output_edge = 0_usize;
                for (edge, &computed) in roles.iter().enumerate() {
                    let target = *old_ring.get(edge.wrapping_add(1)).ok_or_else(|| {
                        GeometryErrorKind::Invalid("polygon trace endpoint is unknown".into())
                    })?;
                    let mut intermediate = Vec::new();
                    loop {
                        if output_edge >= published_edge_count {
                            let published_vertices = (0..published_edge_count)
                                .map(|index| published.nth_coord(index).xy())
                                .collect::<Vec<_>>();
                            return Err(GeometryErrorKind::Invalid(
                                format!(
                                    "polygon publication lost ring endpoint at source edge {edge}; start={published_start}, output_edge={output_edge}/{published_edge_count}, old_ring={old_ring:?}, published={published_vertices:?}"
                                )
                                .into(),
                            )
                            .into());
                        }
                        let endpoint_index = published_start
                            .checked_add(output_edge)
                            .and_then(|index| index.checked_add(1))
                            .ok_or(ResourceError::SizeOverflow)?
                            .checked_rem(published_edge_count)
                            .ok_or(ResourceError::SizeOverflow)?;
                        let endpoint = published.nth_coord(endpoint_index).xy();
                        output_edge = output_edge
                            .checked_add(1)
                            .ok_or(ResourceError::SizeOverflow)?;
                        if same_published_polygon_xy(frame, endpoint, target)? {
                            break;
                        }
                        intermediate.push(endpoint);
                    }
                    if computed {
                        let slice_count = intermediate
                            .len()
                            .checked_add(1)
                            .ok_or(ResourceError::SizeOverflow)?;
                        slices.extend(
                            std::iter::repeat_with(|| (edge, zero.clone(), one.clone()))
                                .take(slice_count),
                        );
                        continue;
                    }
                    if intermediate.is_empty() {
                        slices.push((edge, zero.clone(), one.clone()));
                        continue;
                    }
                    let cuts =
                        self.source_line_seam_cuts(&source_trace, edge, &zero, &one, roots, work)?;
                    if cuts.len() != intermediate.len() {
                        return Err(GeometryErrorKind::Invalid(
                            "published polygon seam vertices do not match their source trace"
                                .into(),
                        )
                        .into());
                    }
                    for (cut, &endpoint) in cuts.iter().zip(&intermediate) {
                        if !same_published_polygon_xy(frame, endpoint, cut.coordinate)? {
                            return Err(GeometryErrorKind::Invalid(
                                "published polygon seam vertex differs from its source trace"
                                    .into(),
                            )
                            .into());
                        }
                    }
                    let mut start = zero.clone();
                    for cut in cuts {
                        let end = cut.parameter;
                        slices.push((edge, start, end.clone()));
                        start = end;
                    }
                    slices.push((edge, start, one.clone()));
                }
                if output_edge != published_edge_count || slices.len() != published_edge_count {
                    return Err(GeometryErrorKind::Invalid(
                        "polygon publication added an untraced edge".into(),
                    )
                    .into());
                }
                slices.rotate_right(published_start);
                traces.push(source_trace.compose_edges(&slices, work)?);
            }
        }
        if source_traces.next().is_some() {
            return Err(GeometryErrorKind::Invalid(
                "polygon source traces include an unassigned ring".into(),
            )
            .into());
        }
        Ok(traces)
    }

    fn resolve_traced_source_line_points<
        'e,
        'p,
        M: ChartMetric,
        C: RootCatalog,
        W: Work,
        F: FnMut(&[RequestRowInput], &mut W) -> Result<Vec<NodeOrdinates>>,
    >(
        &self,
        partition: &'p ExactPartition<'_>,
        embedding: &'e embedding::Embedding<'p>,
        nodes: &[NodeId],
        run_plans: &[Vec<SourceLineRun>],
        source_traces: &[SourceTrace],
        request: &SubdivisionTraceRequest<'_, '_, '_, M, C>,
        work: &mut W,
        resolve_rows: &mut F,
    ) -> Result<ResolvedNodePoints<'e, 'p>> {
        if !request.declared_axes.has_z()
            && !request.declared_axes.has_m()
            && request.root_positions.is_none()
        {
            return Ok(ResolvedNodePoints::from_embedding(embedding, None));
        }
        work.charge_into_error(
            u64::try_from(partition.node_count()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut requests_by_node = Vec::new();
        requests_by_node.resize_with(partition.node_count(), Vec::new);
        let mut root_xy_nodes = request
            .root_positions
            .map(|_| vec![false; partition.node_count()]);
        for runs in run_plans {
            for run in runs {
                if run.nodes.len() != run.trace_slices.len().saturating_add(1) {
                    return Err(GeometryErrorKind::Invalid(
                        "traced source line run vertices do not match its edge slices".into(),
                    )
                    .into());
                }
                let trace = source_traces.get(run.member).ok_or_else(|| {
                    GeometryErrorKind::Invalid("source line trace member is unknown".into())
                })?;
                for (edge_index, (source_edge, start, end)) in run.trace_slices.iter().enumerate() {
                    let start_node = *run.nodes.get(edge_index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("source line trace start node is unknown".into())
                    })?;
                    let end_node = *run.nodes.get(edge_index.wrapping_add(1)).ok_or_else(|| {
                        GeometryErrorKind::Invalid("source line trace end node is unknown".into())
                    })?;
                    let start_row =
                        requests_by_node
                            .get_mut(start_node.0 as usize)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "source line trace start node is outside the partition".into(),
                                )
                            })?;
                    let start_added =
                        trace.append_requests_at(*source_edge, start, start_row, work)?;
                    work.charge_into_error(
                        u64::try_from(start_added).map_err(|_| ResourceError::SizeOverflow)?,
                    )?;
                    if let Some(root_xy_nodes) = root_xy_nodes.as_mut() {
                        mark_root_xy_node(root_xy_nodes, start_node, start, start_added)?;
                    }
                    let end_row =
                        requests_by_node
                            .get_mut(end_node.0 as usize)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "source line trace end node is outside the partition".into(),
                                )
                            })?;
                    let end_added = trace.append_requests_at(*source_edge, end, end_row, work)?;
                    work.charge_into_error(
                        u64::try_from(end_added).map_err(|_| ResourceError::SizeOverflow)?,
                    )?;
                    if let Some(root_xy_nodes) = root_xy_nodes.as_mut() {
                        mark_root_xy_node(root_xy_nodes, end_node, end, end_added)?;
                    }
                }
            }
        }
        resolve_traced_node_requests(
            partition,
            embedding,
            nodes,
            requests_by_node,
            request,
            root_xy_nodes.as_deref(),
            work,
            resolve_rows,
        )
    }

    fn source_line_seam_cuts<W: Work>(
        &self,
        trace: &SourceTrace,
        source_edge: usize,
        start: &ExactRational,
        end: &ExactRational,
        roots: &SubdivisionRootCatalog,
        work: &mut W,
    ) -> Result<Vec<SourceLineSeamCut>> {
        let start_requests = trace.requests_at(source_edge, start, work)?;
        let end_requests = trace.requests_at(source_edge, end, work)?;
        let mut cuts = Vec::<SourceLineSeamCut>::new();
        for start_request in start_requests {
            work.charge_into_error(1)?;
            let Some(end_request) = end_requests
                .iter()
                .find(|request| request.root() == start_request.root())
            else {
                continue;
            };
            let Some(root_parameter) = roots.seam_parameter(start_request.root(), work)? else {
                continue;
            };
            let root_order = start_request
                .parameter()
                .try_cmp_with_work(end_request.parameter(), work)?;
            if root_order.is_eq() {
                continue;
            }
            let before = start_request
                .parameter()
                .try_cmp_with_work(&root_parameter, work)?;
            let after = end_request
                .parameter()
                .try_cmp_with_work(&root_parameter, work)?;
            let crosses = if root_order.is_lt() {
                before.is_lt() && after.is_gt()
            } else {
                before.is_gt() && after.is_lt()
            };
            if !crosses {
                continue;
            }
            let root_width = end_request
                .parameter()
                .try_subtract_with_work(start_request.parameter(), work)?;
            let root_offset =
                root_parameter.try_subtract_with_work(start_request.parameter(), work)?;
            let edge_fraction = root_offset.try_quotient_with_work(&root_width, work)?;
            let edge_width = end.try_subtract_with_work(start, work)?;
            let parameter = start.try_add_with_work(
                &edge_width.try_product_with_work(&edge_fraction, work)?,
                work,
            )?;
            if parameter.try_cmp_with_work(start, work)?.is_le()
                || parameter.try_cmp_with_work(end, work)?.is_ge()
            {
                continue;
            }
            let coordinate = roots.source_position(start_request.root(), &root_parameter, work)?;
            let Some(coordinate) = coordinate else {
                return Err(GeometryErrorKind::Invalid(
                    "an original seam crossing has no source-chart position".into(),
                )
                .into());
            };
            let requests = trace.requests_at(source_edge, &parameter, work)?;
            let mut insert_at = cuts.len();
            let mut duplicate = false;
            for (index, existing) in cuts.iter().enumerate() {
                match parameter.try_cmp_with_work(&existing.parameter, work)? {
                    Ordering::Less => {
                        insert_at = index;
                        break;
                    },
                    Ordering::Equal => {
                        duplicate = true;
                        break;
                    },
                    Ordering::Greater => {},
                }
            }
            if !duplicate {
                cuts.insert(insert_at, SourceLineSeamCut {
                    parameter,
                    coordinate,
                    requests,
                });
            }
        }
        Ok(cuts)
    }

    fn subdivision_line_parts<M: ChartMetric>(
        &self,
        partition: &ExactPartition<'_>,
        source: LineSource<'_>,
        topology: &IndexedTopology,
        global: &GlobalFaces,
        facts: &CellFacts,
        declared_axes: crate::geometry::CoordinateAxes,
        strict: bool,
        clip_operands: [usize; 2],
        context: &SourceArcContext<'_, '_, M>,
    ) -> Result<[Shape; 2]> {
        let mut selected = Vec::with_capacity(clip_operands.len());
        for &clip_operand in &clip_operands {
            let cells =
                self.select_subdivision_cells(partition, topology, global, facts, 0, clip_operand);
            selected.push(cells.spans);
        }

        let emissions: Vec<_> = selected
            .iter()
            .map(|spans| {
                partition.span_emission(
                    spans
                        .iter()
                        .enumerate()
                        .filter_map(|(span, &selected)| selected.then_some(span as u32)),
                )
            })
            .collect();
        let emission_refs: Vec<_> = emissions.iter().collect();
        let embedding = partition.embed(&emission_refs, "subdivide")?;
        let by_node = self.resolve_node_points(
            partition,
            &embedding,
            emissions
                .iter()
                .flat_map(|emission| emission.items.iter().flatten().copied()),
            declared_axes,
            context,
            None,
        )?;
        let views = partition.source_span_views(&self.source_segments)?;
        let ranges = source_line_ranges(source)?;
        let mut outputs = Vec::with_capacity(selected.len());
        for spans in selected {
            outputs.push(self.emit_source_line_shape(
                partition,
                source.kind(),
                &ranges,
                &views,
                &spans,
                &by_node,
                declared_axes,
                strict,
            )?);
        }
        let mut outputs = outputs.into_iter();
        Ok([
            outputs.next().expect("two subdivision line selections"),
            outputs.next().expect("two subdivision line selections"),
        ])
    }

    fn source_line_runs<W: Work + ?Sized>(
        &self,
        partition: &ExactPartition<'_>,
        ranges: &[Range<usize>],
        views: &SourceLineTraceViews,
        selected: &[bool],
        source_traces: &[SourceTrace],
        work: &mut W,
    ) -> Result<Vec<SourceLineRun>> {
        #[cfg(test)]
        SOURCE_LINE_RUNS_CALLS.with(|calls| {
            calls.set(calls.get().saturating_add(1));
        });
        if source_traces.len() != ranges.len() {
            return Err(GeometryErrorKind::Invalid(
                "source trace rows do not match the line members".into(),
            )
            .into());
        }
        if views.source_ids.len() != views.views.len() {
            return Err(GeometryErrorKind::Invalid(
                "exact subdivision line view remap does not match its source rows".into(),
            )
            .into());
        }
        work.charge_into_error(
            u64::try_from(selected.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut runs = Vec::new();
        let mut source_offset = 0_usize;
        for (member, range) in ranges.iter().enumerate() {
            work.charge_into_error(1)?;
            if range.start != source_offset {
                return Err(GeometryErrorKind::Invalid(
                    "line member source ranges are not contiguous".into(),
                )
                .into());
            }
            source_offset = range.end;
            let trace = source_traces.get(member).ok_or_else(|| {
                GeometryErrorKind::Invalid("source trace member is unknown".into())
            })?;
            if trace.edge_count() != range.len() {
                return Err(GeometryErrorKind::Invalid(
                    "source trace edge count differs from its line member".into(),
                )
                .into());
            }
            let mut run = Vec::new();
            let mut trace_slices = Vec::new();
            for source in range.clone() {
                let (Some(full_source), Some(source_views)) = (
                    views.source_ids.get(source).copied(),
                    views.views.get(source),
                ) else {
                    continue;
                };
                if self.source_operands.get(full_source).copied() != Some(0) {
                    return Err(GeometryErrorKind::Invalid(
                        "exact subdivision line remap includes a clip-operand source".into(),
                    )
                    .into());
                }
                for view in source_views {
                    work.charge_into_error(1)?;
                    let is_selected = *selected.get(view.span as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "source trace span is outside the selected span table".into(),
                        )
                    })?;
                    if !is_selected {
                        push_source_line_run(
                            &mut runs,
                            member,
                            &mut run,
                            &mut trace_slices,
                            true,
                            work,
                        )?;
                        continue;
                    }
                    let span = *partition.spans.get(view.span as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("source trace span is unknown".into())
                    })?;
                    let (start, end) = if view.reversed {
                        (span.end, span.start)
                    } else {
                        (span.start, span.end)
                    };
                    if run.last().is_some_and(|&last| last != start) {
                        push_source_line_run(
                            &mut runs,
                            member,
                            &mut run,
                            &mut trace_slices,
                            true,
                            work,
                        )?;
                    }
                    if run.is_empty() {
                        run.reserve(2);
                        run.push(start);
                    }
                    if run.last().copied() != Some(end) {
                        run.push(end);
                        work.charge_into_error(1)?;
                        trace_slices.push((
                            source
                                .checked_sub(range.start)
                                .ok_or(ResourceError::SizeOverflow)?,
                            view.source_start.clone(),
                            view.source_end.clone(),
                        ));
                    }
                }
            }
            push_source_line_run(&mut runs, member, &mut run, &mut trace_slices, true, work)?;
        }
        if source_offset != views.source_ids.len() {
            return Err(GeometryErrorKind::Invalid(
                "line member source ranges omit partition source segments".into(),
            )
            .into());
        }
        Ok(runs)
    }

    fn emit_traced_source_line_shape<M: ChartMetric, C: RootCatalog, W: Work>(
        &self,
        source_kind: GeometryKind,
        runs: Vec<SourceLineRun>,
        source_traces: &[SourceTrace],
        by_node: &ResolvedNodePoints,
        request: &SubdivisionTraceRequest<'_, '_, '_, M, C>,
        work: &mut W,
    ) -> Result<SubdivisionPart> {
        let roots = request.roots;
        let root_positions = request.root_positions;
        let declared_axes = request.declared_axes;
        let strict = request.strict;
        let context = request.context;
        if root_positions.is_none() {
            let node_count = runs.iter().try_fold(0_usize, |count, run| {
                count
                    .checked_add(run.nodes.len())
                    .ok_or(ResourceError::SizeOverflow)
            })?;
            work.charge_into_error(
                u64::try_from(node_count).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let mut line_nodes = Vec::with_capacity(node_count);
            for run in &runs {
                line_nodes.extend(run.nodes.iter().copied());
            }
            let line_axes = bucket_axes(
                line_nodes.iter().copied(),
                by_node,
                declared_axes,
                strict,
                "subdivide",
            )?;
            let coordinates = by_node.coordinates();
            let mut lines = Vec::with_capacity(runs.len());
            for run in &runs {
                let mut points = Vec::with_capacity(run.nodes.len());
                for &node in &run.nodes {
                    work.charge_into_error(1)?;
                    points.push(with_axes(
                        by_node.point(node, embedding::Embedding::xy_at(coordinates, node)),
                        line_axes,
                    ));
                }
                lines.push(LineSeq::from_proven(CoordSeq::try_from_points(&points)?));
            }
            if lines.is_empty() {
                return Ok(SubdivisionPart {
                    shape: Shape::typed_empty(source_kind, declared_axes),
                    traces: Some(Vec::new()),
                    polygon_traces: None,
                });
            }
            work.charge_into_error(
                u64::try_from(runs.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let mut traces = Vec::with_capacity(runs.len());
            for run in runs {
                let source_trace = source_traces.get(run.member).ok_or_else(|| {
                    GeometryErrorKind::Invalid("source line trace member is unknown".into())
                })?;
                traces.push(source_trace.compose_edges(&run.trace_slices, work)?);
            }
            return Ok(SubdivisionPart {
                shape: Shape::narrow_lines(line_axes, lines),
                traces: Some(traces),
                polygon_traces: None,
            });
        }

        let root_positions = root_positions.expect("ordinary trace path has its root catalog");
        let mut seam_samples = Vec::new();
        let mut traced_runs = Vec::with_capacity(runs.len());
        for run in runs {
            if run.nodes.len() != run.trace_slices.len().saturating_add(1) {
                return Err(GeometryErrorKind::Invalid(
                    "traced source line run vertices do not match its edge slices".into(),
                )
                .into());
            }
            let trace = source_traces.get(run.member).ok_or_else(|| {
                GeometryErrorKind::Invalid("source line trace member is unknown".into())
            })?;
            let mut vertices =
                Vec::with_capacity(run.nodes.len().saturating_add(run.trace_slices.len()));
            let first = *run.nodes.first().ok_or_else(|| {
                GeometryErrorKind::Invalid("traced source line run has no first vertex".into())
            })?;
            vertices.push(TracedSourceLineVertex::Node(first));
            let mut trace_slices = Vec::new();
            for (edge_index, (source_edge, start, end)) in run.trace_slices.iter().enumerate() {
                let cuts = self.source_line_seam_cuts(
                    trace,
                    *source_edge,
                    start,
                    end,
                    root_positions,
                    work,
                )?;
                let mut slice_start = start.clone();
                for cut in cuts {
                    trace_slices.push((
                        *source_edge,
                        slice_start,
                        cut.parameter.try_clone_with_work(work)?,
                    ));
                    let sample_index = seam_samples.len();
                    seam_samples.push(SourceLineSeamSample {
                        coordinate: cut.coordinate,
                        requests: cut.requests,
                    });
                    vertices.push(TracedSourceLineVertex::Seam(sample_index));
                    slice_start = cut.parameter;
                }
                trace_slices.push((*source_edge, slice_start, end.clone()));
                let node = *run.nodes.get(edge_index.wrapping_add(1)).ok_or_else(|| {
                    GeometryErrorKind::Invalid("source line trace end node is unknown".into())
                })?;
                vertices.push(TracedSourceLineVertex::Node(node));
            }
            traced_runs.push(TracedSourceLineRun {
                member: run.member,
                vertices,
                trace_slices,
            });
        }

        let seam_ordinates = if declared_axes.has_z() || declared_axes.has_m() {
            let mut request_rows = Vec::with_capacity(seam_samples.len());
            for sample in &mut seam_samples {
                request_rows.push(RequestRowInput {
                    axes: declared_axes,
                    requests: std::mem::take(&mut sample.requests),
                });
            }
            resolve_request_rows(&request_rows, roots, context.metric_access(), work)?
        } else {
            vec![NodeOrdinates::default(); seam_samples.len()]
        };
        if seam_ordinates.len() != seam_samples.len() {
            return Err(GeometryErrorKind::Invalid(
                "original-root seam resolution returned incomplete source ordinates".into(),
            )
            .into());
        }
        let node_count = traced_runs.iter().try_fold(0_usize, |count, run| {
            count
                .checked_add(
                    run.vertices
                        .iter()
                        .filter(|vertex| matches!(vertex, TracedSourceLineVertex::Node(_)))
                        .count(),
                )
                .ok_or(ResourceError::SizeOverflow)
        })?;
        work.charge_into_error(
            u64::try_from(node_count).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut line_nodes = Vec::with_capacity(node_count);
        for run in &traced_runs {
            for vertex in &run.vertices {
                if let TracedSourceLineVertex::Node(node) = vertex {
                    line_nodes.push(*node);
                }
            }
        }
        let mut line_axes = bucket_axes(
            line_nodes.iter().copied(),
            by_node,
            declared_axes,
            strict,
            "subdivide",
        )?;
        let mut has_z = line_axes.has_z();
        let mut has_m = line_axes.has_m();
        for ordinates in &seam_ordinates {
            has_z &= ordinates.z.value().is_some();
            has_m &= ordinates.m.value().is_some();
        }
        if strict && (has_z != declared_axes.has_z() || has_m != declared_axes.has_m()) {
            return Err(GeometryErrorKind::ordinate_dropped("subdivide"));
        }
        line_axes = CoordinateAxes::new(crate::geometry::HasZ(has_z), crate::geometry::HasM(has_m));

        let coordinates = by_node.coordinates();
        let mut lines = Vec::with_capacity(traced_runs.len());
        for run in &traced_runs {
            let mut points = Vec::with_capacity(run.vertices.len());
            for vertex in &run.vertices {
                work.charge_into_error(1)?;
                points.push(match *vertex {
                    TracedSourceLineVertex::Node(node) => with_axes(
                        by_node.point(node, embedding::Embedding::xy_at(coordinates, node)),
                        line_axes,
                    ),
                    TracedSourceLineVertex::Seam(sample_index) => {
                        let sample = seam_samples.get(sample_index).ok_or_else(|| {
                            GeometryErrorKind::Invalid("source seam sample is unknown".into())
                        })?;
                        let ordinates =
                            seam_ordinates.get(sample_index).copied().ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "source seam ordinate row is unknown".into(),
                                )
                            })?;
                        Point::new_raw_axes(
                            sample.coordinate.x,
                            sample.coordinate.y,
                            crate::geometry::ZOrdinate(
                                line_axes.has_z().then(|| ordinates.z.value()).flatten(),
                            ),
                            crate::geometry::MOrdinate(
                                line_axes.has_m().then(|| ordinates.m.value()).flatten(),
                            ),
                        )
                    },
                });
            }
            lines.push(LineSeq::from_proven(CoordSeq::try_from_points(&points)?));
        }
        if lines.is_empty() {
            return Ok(SubdivisionPart {
                shape: Shape::typed_empty(source_kind, declared_axes),
                traces: Some(Vec::new()),
                polygon_traces: None,
            });
        }
        work.charge_into_error(
            u64::try_from(traced_runs.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut traces = Vec::with_capacity(traced_runs.len());
        for run in traced_runs {
            let source_trace = source_traces.get(run.member).ok_or_else(|| {
                GeometryErrorKind::Invalid("source line trace member is unknown".into())
            })?;
            traces.push(source_trace.compose_edges(&run.trace_slices, work)?);
        }
        Ok(SubdivisionPart {
            shape: Shape::narrow_lines(line_axes, lines),
            traces: Some(traces),
            polygon_traces: None,
        })
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "span IDs and their selection bits are produced from the same ExactPartition, so both indexed columns share its span domain"
    )]
    fn emit_source_line_shape(
        &self,
        partition: &ExactPartition<'_>,
        source_kind: GeometryKind,
        ranges: &[Range<usize>],
        views: &[Vec<(u32, bool)>],
        selected: &[bool],
        by_node: &ResolvedNodePoints,
        declared_axes: crate::geometry::CoordinateAxes,
        strict: bool,
    ) -> Result<Shape> {
        let mut node_runs = Vec::with_capacity(selected.len());
        let mut source_offset = 0;
        for range in ranges {
            debug_assert_eq!(range.start, source_offset);
            source_offset = range.end;
            let mut run = Vec::new();
            for source in range.clone() {
                for &(span_id, reversed) in views.get(source).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "exact subdivision line provenance lost a source segment".into(),
                    )
                })? {
                    if !selected[span_id as usize] {
                        if run.len() >= 2 {
                            node_runs.push(std::mem::take(&mut run));
                        } else {
                            run.clear();
                        }
                        continue;
                    }
                    let span = partition.spans[span_id as usize];
                    let (start, end) = if reversed {
                        (span.end, span.start)
                    } else {
                        (span.start, span.end)
                    };
                    if run.last().copied() != Some(start) && run.len() >= 2 {
                        node_runs.push(std::mem::take(&mut run));
                    }
                    if run.is_empty() {
                        run.reserve(2);
                        run.push(start);
                    }
                    if run.last().copied() != Some(end) {
                        run.push(end);
                    }
                }
            }
            if run.len() >= 2 {
                node_runs.push(run);
            }
        }

        let line_axes = bucket_axes(
            node_runs.iter().flatten().copied(),
            by_node,
            declared_axes,
            strict,
            "subdivide",
        )?;
        let coordinates = by_node.coordinates();
        let mut lines = Vec::with_capacity(node_runs.len());
        for run in node_runs {
            let mut points = Vec::with_capacity(run.len());
            for node in run {
                points.push(with_axes(
                    by_node.point(node, embedding::Embedding::xy_at(coordinates, node)),
                    line_axes,
                ));
            }
            lines.push(LineSeq::from_proven(CoordSeq::try_from_points(&points)?));
        }
        if lines.is_empty() {
            return Ok(Shape::typed_empty(source_kind, declared_axes));
        }
        Ok(Shape::narrow_lines(line_axes, lines))
    }

    /// Select both closed children from one exact arrangement.
    ///
    /// The cut rectangles are separate polygon operands, but their seam and
    /// source crossings share node recipes, topology, rounding, and ordinates.
    /// The embedding pins the exactly representable cut ordinate, so dissolving
    /// a straight seam cannot move a vertex retained by the other child.
    #[expect(
        clippy::indexing_slicing,
        reason = "topology IDs and CSR rows come from this partition; CellFacts admission checks row-major products and operand IDs are validated before selection"
    )]
    pub(super) fn select_subdivision_cells(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        global: &GlobalFaces,
        facts: &CellFacts,
        source_operand: usize,
        clip_operand: usize,
    ) -> ArealCells {
        let operands = self.operand_count();
        let mut cells = ArealCells {
            faces: Vec::new(),
            spans: Vec::new(),
            nodes: Vec::new(),
        };
        cells.faces.reserve(global.count as usize);
        let source_faces = facts
            .face_membership
            .iter()
            .skip(source_operand)
            .step_by(operands);
        let clip_faces = facts
            .face_membership
            .iter()
            .skip(clip_operand)
            .step_by(operands);
        cells.faces.extend(
            source_faces
                .zip(clip_faces)
                .map(|(&source, &clip)| source && clip),
        );
        cells.spans.reserve(partition.spans.len());
        let source_spans = facts
            .span_support
            .iter()
            .skip(source_operand)
            .step_by(operands);
        let clip_spans = facts
            .span_support
            .iter()
            .skip(clip_operand)
            .step_by(operands);
        cells.spans.extend(
            source_spans
                .zip(clip_spans)
                .map(|(&source, &clip)| source && clip),
        );
        cells.nodes.reserve(partition.node_count());
        cells.nodes.resize(partition.node_count(), false);
        for (span, &selected) in partition.spans.iter().zip(&cells.spans) {
            if selected {
                cells.nodes[span.start.0 as usize] = true;
                cells.nodes[span.end.0 as usize] = true;
            }
        }

        let source_points = facts
            .point_membership
            .iter()
            .skip(source_operand)
            .step_by(operands);
        let clip_points = facts
            .point_membership
            .iter()
            .skip(clip_operand)
            .step_by(operands);
        let face_membership = |face: u32, operand: usize| {
            let offset = (face as usize).wrapping_mul(operands).wrapping_add(operand);
            facts.face_membership[offset]
        };
        for (((selected, starts), &component), (&source_point, &clip_point)) in cells
            .nodes
            .iter_mut()
            .zip(topology.starts.array_windows::<2>())
            .zip(&topology.component_of)
            .zip(source_points.zip(clip_points))
        {
            let &[start, end] = starts;
            let range = start as usize..end as usize;
            let memberships = |operand: usize, point_membership: bool| {
                let face_membership = if range.is_empty() {
                    let host = global.component_hosts[component as usize];
                    face_membership(host, operand)
                } else {
                    range.clone().any(|slot| {
                        let face = global.walk_faces[topology.face_of[slot] as usize];
                        face_membership(face, operand)
                    })
                };
                face_membership || point_membership
            };
            *selected |=
                memberships(source_operand, source_point) && memberships(clip_operand, clip_point);
        }
        cells
    }
}

impl ExactPartition<'_> {
    fn source_span_trace_views<W: Work + ?Sized>(
        &self,
        source_segments: &[Segment],
        source_ids: &[usize],
        work: &mut W,
    ) -> Result<Vec<Vec<SourceSpanTraceView>>> {
        let views = self.source_span_views(source_segments)?;
        let mut total = 0_usize;
        for &source in source_ids {
            let row = views.get(source).ok_or_else(|| {
                GeometryErrorKind::Invalid("source span trace row is unknown".into())
            })?;
            total = total
                .checked_add(row.len())
                .ok_or(ResourceError::SizeOverflow)?;
        }
        work.charge_into_error(u64::try_from(total).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut traced = Vec::with_capacity(source_ids.len());
        for &source in source_ids {
            work.charge_into_error(1)?;
            let segment = *source_segments.get(source).ok_or_else(|| {
                GeometryErrorKind::Invalid("source span trace row is unknown".into())
            })?;
            let source_start = PointKey::new(segment.start);
            let source_end = PointKey::new(segment.end);
            let source_parameter = |node: NodeId| -> Result<ExactRational> {
                if let Some(key) = self.stored_key(node.0 as usize) {
                    if key == source_start {
                        #[cfg(test)]
                        SOURCE_SPAN_ENDPOINT_PARAMETERS
                            .with(|count| count.set(count.get().saturating_add(1)));
                        return Ok(ExactRational::zero());
                    }
                    if key == source_end {
                        #[cfg(test)]
                        SOURCE_SPAN_ENDPOINT_PARAMETERS
                            .with(|count| count.set(count.get().saturating_add(1)));
                        return Ok(ExactRational::one());
                    }
                }
                #[cfg(test)]
                SOURCE_SPAN_INTERIOR_PARAMETERS
                    .with(|count| count.set(count.get().saturating_add(1)));
                let point = self.exact(node)?;
                let [numerator, denominator] = parameter_along(segment, &point)?;
                ExactRational::new(numerator, denominator)
            };
            let row = views.get(source).ok_or_else(|| {
                GeometryErrorKind::Invalid("source span view row is unknown".into())
            })?;
            let mut output = Vec::with_capacity(row.len());
            for &(span, reversed) in row {
                work.charge_into_error(1)?;
                let span_data = self.spans.get(span as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "source span trace references an unknown exact span".into(),
                    )
                })?;
                let [start, end] = if reversed {
                    [span_data.end, span_data.start]
                } else {
                    [span_data.start, span_data.end]
                };
                let start_parameter = source_parameter(start)?;
                let end_parameter = source_parameter(end)?;
                if start_parameter.try_cmp(&end_parameter)?.is_ge() {
                    return Err(GeometryErrorKind::Invalid(
                        "source span trace does not advance along its edge".into(),
                    )
                    .into());
                }
                output.push(SourceSpanTraceView {
                    span,
                    reversed,
                    source_start: start_parameter,
                    source_end: end_parameter,
                });
            }
            traced.push(output);
        }
        Ok(traced)
    }
}

#[cfg(test)]
mod tests;
