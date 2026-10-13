use super::super::{
    AdmittedGraph, ChartSpanId, Deref, EmissionPort, ExactChartPoint, ExactDyadic, ExactRational,
    ExactSourceRange, ExpandedEmissionPlan, FallibleAvl, GeographicFrame, GeometryErrorKind,
    LogicalArealWalk, NativeAreaEdge, NativeAreaEdgeKind, NativeChartAtom, NativeChartAtomIndex,
    NativeChartContributor, NativeChartSpellingKey, NativeChartVertexIndex, NativeChartVertexKey,
    NativeGraphCycle, NativeGraphEdgeOwner, NativeGraphFaceSourceTable, NativeGraphPart,
    NativeGraphSewing, NativePortData, NativeProducerEdgeRow, NativeProducerUseKey,
    NativeProducerUseTable, NativeReferencePoints, NativeWrittenChartEdge, Ordering, PortPosition,
    PortSpelling, QPortId, ResourceError, Result, XY, admit_known_graph,
    build_native_producer_use_table, native_graph_sewing, vector_cross_with_work,
    vector_difference_with_work,
};
use crate::geometry::graph::{
    GlobalFaces, RayAxis, RayPoints, RayPointsWithWork, graph_global_faces_with_work,
};
use crate::geometry::numerics::product::ExactSign;
use crate::work::{Unmetered, Work};

#[derive(Clone, Copy)]
pub(in crate::geometry::topology::q_embedding) enum NativeRayTransform {
    Identity,
    ReflectX,
    SwapAxes,
}
pub(in crate::geometry::topology::q_embedding) struct NativeReferenceRayPoints<'a> {
    pub(in crate::geometry::topology::q_embedding) base: &'a NativeReferencePoints,
    pub(in crate::geometry::topology::q_embedding) exact_queries: &'a [ExactChartPoint],
    pub(in crate::geometry::topology::q_embedding) rounded_queries: &'a [XY],
    pub(in crate::geometry::topology::q_embedding) transform: NativeRayTransform,
}
impl NativeReferenceRayPoints<'_> {
    pub(in crate::geometry::topology::q_embedding) fn exact_point(
        &self,
        index: u32,
    ) -> Result<&ExactChartPoint> {
        let base_len = self.base.exact.len();
        let index = index as usize;
        if index < base_len {
            self.base.exact.get(index).ok_or_else(|| {
                GeometryErrorKind::Invalid("native graph point ID is unknown".into()).into()
            })
        } else {
            self.exact_queries
                .get(
                    index
                        .checked_sub(base_len)
                        .ok_or(ResourceError::SizeOverflow)?,
                )
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("native graph query ID is unknown".into()).into()
                })
        }
    }
}
impl RayPoints for NativeReferenceRayPoints<'_> {
    fn rounded(&self, index: u32) -> XY {
        let index = index as usize;
        let point = self
            .base
            .rounded
            .get(index)
            .or_else(|| {
                index
                    .checked_sub(self.base.rounded.len())
                    .and_then(|query_index| self.rounded_queries.get(query_index))
            })
            .copied()
            .expect("ray point IDs belong to the paired graph and query point tables");
        match self.transform {
            NativeRayTransform::Identity => point,
            NativeRayTransform::ReflectX => XY::new(-point.x, point.y),
            NativeRayTransform::SwapAxes => XY::new(point.y, point.x),
        }
    }

    fn compare_axis(&self, left: u32, right: u32, axis: RayAxis) -> Result<Ordering> {
        self.compare_axis_with_work(left, right, axis, &mut Unmetered)
    }

    fn orientation(&self, a: u32, b: u32, c: u32) -> Result<ExactSign> {
        self.orientation_with_work(a, b, c, &mut Unmetered)
    }
}
impl RayPointsWithWork for NativeReferenceRayPoints<'_> {
    fn compare_axis_with_work<W: Work>(
        &self,
        left: u32,
        right: u32,
        axis: RayAxis,
        work: &mut W,
    ) -> Result<Ordering> {
        let left = self.exact_point(left)?;
        let right = self.exact_point(right)?;
        let source_axis = match (self.transform, axis) {
            (NativeRayTransform::SwapAxes, RayAxis::X) => RayAxis::Y,
            (NativeRayTransform::SwapAxes, RayAxis::Y) => RayAxis::X,
            (NativeRayTransform::Identity | NativeRayTransform::ReflectX, axis) => axis,
        };
        let order = match source_axis {
            RayAxis::X => left.x.try_cmp_with_work(&right.x, work)?,
            RayAxis::Y => left.y.try_cmp_with_work(&right.y, work)?,
        };
        Ok(
            if matches!(
                (self.transform, axis),
                (NativeRayTransform::ReflectX, RayAxis::X)
            ) {
                order.reverse()
            } else {
                order
            },
        )
    }

    fn orientation_with_work<W: Work>(
        &self,
        a: u32,
        b: u32,
        c: u32,
        work: &mut W,
    ) -> Result<ExactSign> {
        let a = self.exact_point(a)?;
        let b = self.exact_point(b)?;
        let c = self.exact_point(c)?;
        let left = vector_difference_with_work(a, b, work)?;
        let right = vector_difference_with_work(a, c, work)?;
        let sign = vector_cross_with_work(&left, &right, work)?.sign();
        Ok(
            if matches!(
                self.transform,
                NativeRayTransform::ReflectX | NativeRayTransform::SwapAxes
            ) {
                match sign {
                    ExactSign::Positive => ExactSign::Negative,
                    ExactSign::Negative => ExactSign::Positive,
                    ExactSign::Zero => ExactSign::Zero,
                }
            } else {
                sign
            },
        )
    }
}
pub(in crate::geometry::topology::q_embedding) struct NativeReferenceGraph {
    pub(in crate::geometry::topology::q_embedding) common: AdmittedGraph,
    pub(in crate::geometry::topology::q_embedding) faces: GlobalFaces,
    pub(in crate::geometry::topology::q_embedding) vertex_keys: Vec<NativeChartVertexKey>,
    pub(in crate::geometry::topology::q_embedding) vertices: FallibleAvl<NativeChartVertexIndex>,
    pub(in crate::geometry::topology::q_embedding) atoms: Vec<NativeChartAtom>,
    pub(in crate::geometry::topology::q_embedding) written_edges: Vec<NativeWrittenChartEdge>,
    pub(in crate::geometry::topology::q_embedding) producer_uses: NativeProducerUseTable,
    pub(in crate::geometry::topology::q_embedding) face_source_uses: NativeGraphFaceSourceTable,
    pub(in crate::geometry::topology::q_embedding) sewing: Vec<NativeGraphSewing>,
    pub(in crate::geometry::topology::q_embedding) face_regions: Vec<Option<usize>>,
    pub(in crate::geometry::topology::q_embedding) face_groups: Vec<u32>,
    pub(in crate::geometry::topology::q_embedding) face_cells: Vec<Option<u8>>,
    pub(in crate::geometry::topology::q_embedding) cycles: Vec<NativeGraphCycle>,
    pub(in crate::geometry::topology::q_embedding) parts: Vec<NativeGraphPart>,
}
impl Deref for NativeReferenceGraph {
    type Target = AdmittedGraph;

    fn deref(&self) -> &Self::Target {
        &self.common
    }
}
pub(in crate::geometry::topology::q_embedding) fn logical_layout_for_plan(
    plan: &ExpandedEmissionPlan<'_>,
    port_vertices: &[u32],
    work: &mut impl Work,
) -> Result<Vec<LogicalArealWalk>> {
    let walk_count = plan.parts.iter().try_fold(0_usize, |count, part| {
        count
            .checked_add(1)
            .and_then(|count| count.checked_add(part.holes.len()))
            .ok_or(ResourceError::SizeOverflow)
    })?;
    work.charge_into_error(u64::try_from(walk_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut areal_walks = Vec::with_capacity(walk_count);
    for (output, part) in plan.parts.iter().enumerate() {
        for (walk, chords) in std::iter::once(&part.shell).chain(&part.holes).enumerate() {
            work.charge_into_error(1)?;
            let expected_count = chords
                .len()
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            work.charge_into_error(
                u64::try_from(expected_count).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let mut vertices = Vec::with_capacity(expected_count);
            for &chord_id in chords {
                let chord = plan.chords.get(chord_id.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q areal walk references an unknown chord".into())
                })?;
                let [start_port, end_port] = chord.ports;
                let start = *port_vertices.get(start_port.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q areal walk start port has no graph vertex".into())
                })?;
                let end = *port_vertices.get(end_port.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q areal walk end port has no graph vertex".into())
                })?;
                if let Some(&previous) = vertices.last()
                    && previous != start
                {
                    return Err(GeometryErrorKind::Invalid(
                        "Q areal logical walk is not directed-contiguous".into(),
                    )
                    .into());
                }
                if vertices.is_empty() {
                    vertices.push(start);
                }
                vertices.push(end);
            }
            if vertices.len() < 4 || vertices.first() != vertices.last() {
                return Err(GeometryErrorKind::Invalid(
                    "Q areal logical walk is not a closed ring".into(),
                )
                .into());
            }
            areal_walks.push(LogicalArealWalk {
                output: u32::try_from(output).map_err(|_| ResourceError::SizeOverflow)?,
                walk: u32::try_from(walk).map_err(|_| ResourceError::SizeOverflow)?,
                vertices,
            });
        }
    }
    Ok(areal_walks)
}
pub(in crate::geometry::topology::q_embedding) fn native_point_contact_vertices(
    plan: &ExpandedEmissionPlan<'_>,
    port_vertices: &[u32],
    work: &mut impl Work,
) -> Result<Vec<u32>> {
    work.charge_into_error(
        u64::try_from(plan.points.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut vertices = Vec::with_capacity(plan.points.len());
    for &port in &plan.points {
        work.charge_into_error(1)?;
        vertices.push(*port_vertices.get(port.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("native output point has no graph vertex".into())
        })?);
    }
    Ok(vertices)
}
impl NativeReferenceGraph {
    pub(in crate::geometry::topology::q_embedding) fn local_vertex(
        &self,
        key: NativeChartVertexKey,
        budget: &mut impl Work,
    ) -> Result<u32> {
        let probe = NativeChartVertexIndex { key, index: 0 };
        self.vertices
            .get_with_work(&probe, budget, &mut |left, right, work| {
                work.charge_into_error(1)?;
                Ok(left.key.cmp(&right.key))
            })?
            .map(|vertex| vertex.index)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("native provider key has no chart-local vertex".into())
                    .into()
            })
    }
}
pub(in crate::geometry::topology::q_embedding) struct NativeReferenceGraphBuilder {
    pub(in crate::geometry::topology::q_embedding) points: NativeReferencePoints,
    pub(in crate::geometry::topology::q_embedding) vertex_keys: Vec<NativeChartVertexKey>,
    pub(in crate::geometry::topology::q_embedding) vertices: FallibleAvl<NativeChartVertexIndex>,
    pub(in crate::geometry::topology::q_embedding) atoms: Vec<NativeChartAtom>,
    pub(in crate::geometry::topology::q_embedding) edge_nodes: Vec<[u32; 2]>,
    pub(in crate::geometry::topology::q_embedding) atom_index: FallibleAvl<NativeChartAtomIndex>,
    pub(in crate::geometry::topology::q_embedding) written_edges: Vec<NativeWrittenChartEdge>,
    pub(in crate::geometry::topology::q_embedding) producer_edges: Vec<NativeProducerEdgeRow>,
}
pub(in crate::geometry::topology::q_embedding) struct NativeKnownSpanBoundary {
    pub(in crate::geometry::topology::q_embedding) span: ChartSpanId,
    pub(in crate::geometry::topology::q_embedding) parameter: ExactRational,
    pub(in crate::geometry::topology::q_embedding) port: QPortId,
    pub(in crate::geometry::topology::q_embedding) data: NativePortData,
}
pub(in crate::geometry::topology::q_embedding) struct NativeSplitPoint {
    pub(in crate::geometry::topology::q_embedding) parameter: ExactRational,
    pub(in crate::geometry::topology::q_embedding) data: NativePortData,
}
pub(in crate::geometry::topology::q_embedding) const fn native_chart_spelling_key(
    spelling: &PortSpelling,
) -> NativeChartSpellingKey {
    match spelling {
        PortSpelling::Interior { .. } => NativeChartSpellingKey::Interior(0),
        PortSpelling::Seam(super::super::super::emission::ChartSide::East) => {
            NativeChartSpellingKey::SeamEast
        },
        PortSpelling::Seam(super::super::super::emission::ChartSide::West) => {
            NativeChartSpellingKey::SeamWest
        },
        PortSpelling::Pole { native_longitude } => {
            NativeChartSpellingKey::Pole(native_longitude.to_bits())
        },
    }
}
pub(in crate::geometry::topology::q_embedding) fn normalize_native_chart_lift(
    port: &mut NativePortData,
    frame: GeographicFrame,
    budget: &mut impl Work,
) -> Result<()> {
    budget.charge_into_error(1)?;
    let PortSpelling::Interior { periods } = &port.spelling else {
        return Ok(());
    };
    if *periods == 0 {
        return Ok(());
    }
    let lift = ExactRational::from_dyadic(ExactDyadic::from_i64(*periods));
    let shift =
        ExactRational::from_f64(frame.longitude().period()).try_product_with_work(&lift, budget)?;
    port.point.x = port.point.x.try_subtract_with_work(&shift, budget)?;
    port.spelling = PortSpelling::Interior { periods: 0 };
    Ok(())
}
pub(in crate::geometry::topology::q_embedding) fn normalize_native_chart_lifts(
    area_edges: &mut [NativeAreaEdge],
    line_edges: &mut [NativeAreaEdge],
    reference_ports: &mut [NativePortData],
    frame: GeographicFrame,
    budget: &mut impl Work,
) -> Result<()> {
    for edge in area_edges.iter_mut().chain(line_edges) {
        for port in &mut edge.vertices {
            normalize_native_chart_lift(port, frame, budget)?;
        }
    }
    for port in reference_ports {
        normalize_native_chart_lift(port, frame, budget)?;
    }
    Ok(())
}
pub(in crate::geometry::topology::q_embedding) const fn native_chart_vertex_key(
    data: &NativePortData,
) -> NativeChartVertexKey {
    NativeChartVertexKey {
        node: data.node,
        spelling: native_chart_spelling_key(&data.spelling),
    }
}
pub(in crate::geometry::topology::q_embedding) const fn native_emission_port_key(
    port: &EmissionPort,
) -> NativeChartVertexKey {
    let spelling = match &port.position {
        PortPosition::Source { spelling } | PortPosition::Projected { spelling } => spelling,
    };
    NativeChartVertexKey {
        node: port.node,
        spelling: native_chart_spelling_key(spelling),
    }
}
pub(in crate::geometry::topology::q_embedding) fn intern_native_chart_vertex(
    data: &NativePortData,
    points: &mut Vec<ExactChartPoint>,
    vertex_keys: &mut Vec<NativeChartVertexKey>,
    index: &mut FallibleAvl<NativeChartVertexIndex>,
    budget: &mut impl Work,
) -> Result<u32> {
    budget.charge_into_error(1)?;
    let key = native_chart_vertex_key(data);
    let probe = NativeChartVertexIndex { key, index: 0 };
    if let Some(previous) = index.get_with_work(&probe, budget, &mut |left, right, budget| {
        budget.charge_into_error(1)?;
        Ok(left.key.cmp(&right.key))
    })? {
        let point = points.get(previous.index as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("native chart vertex index is unknown".into())
        })?;
        if point != &data.point {
            return Err(GeometryErrorKind::Invalid(
                "native chart vertex spelling maps to two reference points".into(),
            )
            .into());
        }
        if vertex_keys.get(previous.index as usize) != Some(&key) {
            return Err(GeometryErrorKind::Invalid(
                "native chart vertex provider map disagrees with its lookup index".into(),
            )
            .into());
        }
        return Ok(previous.index);
    }
    budget.charge_into_error(3)?;
    let vertex = u32::try_from(points.len()).map_err(|_| ResourceError::SizeOverflow)?;
    points.push(data.point.try_clone_with_work(budget)?);
    vertex_keys.push(key);
    index.insert_with_work(
        NativeChartVertexIndex { key, index: vertex },
        budget,
        &mut |left, right, budget| {
            budget.charge_into_error(1)?;
            Ok(left.key.cmp(&right.key))
        },
    )?;
    Ok(vertex)
}
pub(in crate::geometry::topology::q_embedding) fn native_chart_atom(
    left: u32,
    right: u32,
    atoms: &mut Vec<NativeChartAtom>,
    edge_nodes: &mut Vec<[u32; 2]>,
    index: &mut FallibleAvl<NativeChartAtomIndex>,
    written_edges: &mut Vec<NativeWrittenChartEdge>,
    written_edge: usize,
    region: Option<usize>,
    budget: &mut impl Work,
) -> Result<()> {
    if left == right {
        return Err(GeometryErrorKind::Invalid(
            "native reference contains a zero-length chart chord".into(),
        )
        .into());
    }
    let reversed = left > right;
    let endpoints = if reversed {
        [right, left]
    } else {
        [left, right]
    };
    let probe = NativeChartAtomIndex {
        endpoints,
        index: 0,
    };
    let atom = if let Some(previous) =
        index.get_with_work(&probe, budget, &mut |left, right, budget| {
            budget.charge_into_error(1)?;
            Ok(left.endpoints.cmp(&right.endpoints))
        })? {
        previous.index
    } else {
        budget.charge_into_error(2)?;
        let atom = u32::try_from(atoms.len()).map_err(|_| ResourceError::SizeOverflow)?;
        atoms.push(NativeChartAtom {
            contributors: Vec::new(),
        });
        edge_nodes.push(endpoints);
        index.insert_with_work(
            NativeChartAtomIndex {
                endpoints,
                index: atom,
            },
            budget,
            &mut |left, right, budget| {
                budget.charge_into_error(1)?;
                Ok(left.endpoints.cmp(&right.endpoints))
            },
        )?;
        atom
    };
    budget.charge_into_error(1)?;
    atoms
        .get_mut(atom as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("native chart atom is unknown".into()))?
        .contributors
        .push(NativeChartContributor {
            written_edge,
            region,
            reversed,
        });
    written_edges.push(NativeWrittenChartEdge { atom, reversed });
    Ok(())
}
pub(in crate::geometry::topology::q_embedding) fn new_native_reference_graph_builder(
    area_edges: &[NativeAreaEdge],
    reference_ports: &[NativePortData],
    budget: &mut impl Work,
) -> Result<NativeReferenceGraphBuilder> {
    let mut graph = NativeReferenceGraphBuilder {
        points: NativeReferencePoints {
            exact: Vec::new(),
            rounded: Vec::new(),
        },
        vertex_keys: Vec::new(),
        vertices: FallibleAvl::with_capacity(0),
        atoms: Vec::new(),
        edge_nodes: Vec::new(),
        atom_index: FallibleAvl::with_capacity(0),
        written_edges: Vec::new(),
        producer_edges: Vec::new(),
    };
    graph.append_edges(area_edges, budget)?;
    graph.append_ports(reference_ports, budget)?;
    Ok(graph)
}
impl NativeReferenceGraphBuilder {
    pub(in crate::geometry::topology::q_embedding) fn has_area_edge<W: Work>(
        &self,
        region: usize,
        start: &NativePortData,
        end: &NativePortData,
        budget: &mut W,
    ) -> Result<bool> {
        let lookup_vertex = |port: &NativePortData, budget: &mut W| -> Result<Option<u32>> {
            let key = native_chart_vertex_key(port);
            let probe = NativeChartVertexIndex { key, index: 0 };
            let Some(vertex) =
                self.vertices
                    .get_with_work(&probe, budget, &mut |left, right, work| {
                        work.charge_into_error(1)?;
                        Ok(left.key.cmp(&right.key))
                    })?
            else {
                return Ok(None);
            };
            let index = vertex.index as usize;
            if self.vertex_keys.get(index) != Some(&key)
                || self.points.exact.get(index) != Some(&port.point)
            {
                return Err(GeometryErrorKind::Invalid(
                    "native boundary lookup disagrees with its exact provider vertex".into(),
                )
                .into());
            }
            Ok(Some(vertex.index))
        };
        let Some(start) = lookup_vertex(start, budget)? else {
            return Ok(false);
        };
        let Some(end) = lookup_vertex(end, budget)? else {
            return Ok(false);
        };
        if start == end {
            return Err(GeometryErrorKind::Invalid(
                "native boundary lookup contains a zero-length edge".into(),
            )
            .into());
        }
        let reversed = start > end;
        let endpoints = if reversed { [end, start] } else { [start, end] };
        let probe = NativeChartAtomIndex {
            endpoints,
            index: 0,
        };
        let Some(atom) =
            self.atom_index
                .get_with_work(&probe, budget, &mut |left, right, work| {
                    work.charge_into_error(1)?;
                    Ok(left.endpoints.cmp(&right.endpoints))
                })?
        else {
            return Ok(false);
        };
        let atom = self.atoms.get(atom.index as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("native boundary lookup references an unknown atom".into())
        })?;
        let mut same_direction = false;
        let mut opposite_direction = false;
        for contributor in &atom.contributors {
            budget.charge_into_error(1)?;
            if contributor.region != Some(region) {
                continue;
            }
            if contributor.reversed == reversed {
                same_direction = true;
            } else {
                opposite_direction = true;
            }
        }
        if same_direction && opposite_direction {
            return Err(GeometryErrorKind::Invalid(
                "native selected boundary atom has conflicting directed producers".into(),
            )
            .into());
        }
        if opposite_direction {
            return Err(GeometryErrorKind::Invalid(
                "native seam closure opposes an existing selected boundary edge".into(),
            )
            .into());
        }
        Ok(same_direction)
    }

    pub(in crate::geometry::topology::q_embedding) fn append_edges(
        &mut self,
        area_edges: &[NativeAreaEdge],
        budget: &mut impl Work,
    ) -> Result<()> {
        let first_edge = self.written_edges.len();
        let additional_edges = area_edges.len().checked_sub(first_edge).ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "native graph edge table shrank after reference construction".into(),
            )
        })?;
        let additional_vertices = additional_edges
            .checked_mul(2)
            .ok_or(ResourceError::SizeOverflow)?;
        budget.charge_into_error(
            u64::try_from(additional_vertices).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        self.points.exact.reserve(additional_vertices);
        budget.charge_into_error(
            u64::try_from(additional_vertices).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        self.points.rounded.reserve(additional_vertices);
        budget.charge_into_error(
            u64::try_from(additional_vertices).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        self.vertex_keys.reserve(additional_vertices);
        budget.charge_into_error(
            u64::try_from(additional_edges).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        self.atoms.reserve(additional_edges);
        budget.charge_into_error(
            u64::try_from(additional_edges).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        self.edge_nodes.reserve(additional_edges);
        budget.charge_into_error(
            u64::try_from(additional_edges).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        self.written_edges.reserve(additional_edges);
        budget.charge_into_error(
            u64::try_from(additional_edges).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        self.producer_edges.reserve(additional_edges);
        for (written_edge, edge) in area_edges.iter().enumerate().skip(first_edge) {
            if !matches!(edge.owner, NativeGraphEdgeOwner::Area { .. }) {
                return Err(GeometryErrorKind::Invalid(
                    "native area edge list contains a line producer".into(),
                )
                .into());
            }
            self.append_graph_edge(edge, written_edge, budget)?;
        }
        self.refresh_rounded(budget)
    }

    pub(in crate::geometry::topology::q_embedding) fn append_line_edges(
        &mut self,
        area_edge_count: usize,
        line_edges: &[NativeAreaEdge],
        budget: &mut impl Work,
    ) -> Result<()> {
        let existing_lines = self
            .written_edges
            .len()
            .checked_sub(area_edge_count)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("native line graph precedes its area edges".into())
            })?;
        if existing_lines > line_edges.len() {
            return Err(GeometryErrorKind::Invalid(
                "native line graph edge table shrank after reference construction".into(),
            )
            .into());
        }
        let additional_edges = line_edges
            .len()
            .checked_sub(existing_lines)
            .ok_or(ResourceError::SizeOverflow)?;
        let additional_vertices = additional_edges
            .checked_mul(2)
            .ok_or(ResourceError::SizeOverflow)?;
        budget.charge_into_error(
            u64::try_from(additional_vertices).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        self.points.exact.reserve(additional_vertices);
        budget.charge_into_error(
            u64::try_from(additional_vertices).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        self.points.rounded.reserve(additional_vertices);
        budget.charge_into_error(
            u64::try_from(additional_vertices).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        self.vertex_keys.reserve(additional_vertices);
        budget.charge_into_error(
            u64::try_from(additional_edges).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        self.atoms.reserve(additional_edges);
        budget.charge_into_error(
            u64::try_from(additional_edges).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        self.edge_nodes.reserve(additional_edges);
        budget.charge_into_error(
            u64::try_from(additional_edges).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        self.written_edges.reserve(additional_edges);
        budget.charge_into_error(
            u64::try_from(additional_edges).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        self.producer_edges.reserve(additional_edges);
        for (line_index, edge) in line_edges.iter().enumerate().skip(existing_lines) {
            if !matches!(edge.owner, NativeGraphEdgeOwner::Line) {
                return Err(GeometryErrorKind::Invalid(
                    "native line edge list contains an area edge".into(),
                )
                .into());
            }
            let written_edge = area_edge_count
                .checked_add(line_index)
                .ok_or(ResourceError::SizeOverflow)?;
            self.append_graph_edge(edge, written_edge, budget)?;
        }
        self.refresh_rounded(budget)
    }

    pub(in crate::geometry::topology::q_embedding) fn append_graph_edge(
        &mut self,
        edge: &NativeAreaEdge,
        written_edge: usize,
        budget: &mut impl Work,
    ) -> Result<()> {
        if self.written_edges.len() != written_edge
            || (matches!(edge.owner, NativeGraphEdgeOwner::Line)
                && !matches!(&edge.kind, NativeAreaEdgeKind::Producer { .. }))
        {
            return Err(GeometryErrorKind::Invalid(
                "native line graph edge is not an ordered producer row".into(),
            )
            .into());
        }
        budget.charge_into_error(1)?;
        let [start, end] = &edge.vertices;
        let start = intern_native_chart_vertex(
            start,
            &mut self.points.exact,
            &mut self.vertex_keys,
            &mut self.vertices,
            budget,
        )?;
        let end = intern_native_chart_vertex(
            end,
            &mut self.points.exact,
            &mut self.vertex_keys,
            &mut self.vertices,
            budget,
        )?;
        native_chart_atom(
            start,
            end,
            &mut self.atoms,
            &mut self.edge_nodes,
            &mut self.atom_index,
            &mut self.written_edges,
            written_edge,
            match edge.owner {
                NativeGraphEdgeOwner::Area { region } => Some(region),
                NativeGraphEdgeOwner::Line => None,
            },
            budget,
        )?;
        if let NativeAreaEdgeKind::Producer {
            recipe,
            trace_edge,
            producer_start,
            producer_end,
            ..
        } = &edge.kind
        {
            let written = self
                .written_edges
                .get(written_edge)
                .copied()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "native producer edge has no written-edge back-map".into(),
                    )
                })?;
            self.producer_edges.push(NativeProducerEdgeRow {
                key: NativeProducerUseKey {
                    recipe: *recipe,
                    trace_edge: *trace_edge,
                },
                written_edge,
                atom: written.atom,
                reversed: written.reversed,
                producer_range: ExactSourceRange {
                    start: producer_start.as_ref().clone(),
                    end: producer_end.clone(),
                },
            });
        }
        Ok(())
    }

    pub(in crate::geometry::topology::q_embedding) fn append_ports(
        &mut self,
        ports: &[NativePortData],
        budget: &mut impl Work,
    ) -> Result<()> {
        for port in ports {
            intern_native_chart_vertex(
                port,
                &mut self.points.exact,
                &mut self.vertex_keys,
                &mut self.vertices,
                budget,
            )?;
        }
        self.refresh_rounded(budget)
    }

    pub(in crate::geometry::topology::q_embedding) fn refresh_rounded(
        &mut self,
        budget: &mut impl Work,
    ) -> Result<()> {
        let first = self.points.rounded.len();
        let added = self.points.exact.len().checked_sub(first).ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "native graph rounded-point table exceeds its exact-point table".into(),
            )
        })?;
        budget.charge_into_error(u64::try_from(added).map_err(|_| ResourceError::SizeOverflow)?)?;
        self.points.rounded.reserve(added);
        for point in self.points.exact.iter().skip(first) {
            budget.charge_into_error(1)?;
            self.points.rounded.push(XY::new(
                point.x.round_with_work(budget)?,
                point.y.round_with_work(budget)?,
            ));
        }
        Ok(())
    }

    // Q source-germ, selected-region, and face-use construction needs the full
    // producer graph; shared dimension acceptance builds its own wall view later.
    pub(in crate::geometry::topology::q_embedding) fn planar_faces(
        graph: &AdmittedGraph,
        budget: &mut impl Work,
    ) -> Result<GlobalFaces> {
        graph_global_faces_with_work(
            &graph.points,
            &graph.topology,
            graph.edge_nodes.len(),
            |edge| {
                graph.edge_nodes.get(edge).copied().ok_or_else(|| {
                    GeometryErrorKind::Invalid("native planar edge is unknown".into()).into()
                })
            },
            &graph.edge_of_slot,
            budget,
        )
    }

    pub(in crate::geometry::topology::q_embedding) fn finish(
        mut self,
        frame: GeographicFrame,
        budget: &mut impl Work,
    ) -> Result<NativeReferenceGraph> {
        self.refresh_rounded(budget)?;
        budget.charge_into_error(
            u64::try_from(self.vertex_keys.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let point_identities = self
            .vertex_keys
            .iter()
            .map(|key| key.node.0)
            .collect::<Vec<_>>();
        let common = admit_known_graph(self.points, self.edge_nodes, point_identities, budget)?;
        let faces = Self::planar_faces(&common, budget)?;
        let face_count = usize::try_from(faces.count).map_err(|_| ResourceError::SizeOverflow)?;
        budget.charge_into_error(
            u64::try_from(face_count).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        budget.charge_into_error(
            u64::try_from(face_count).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut face_groups = Vec::with_capacity(face_count);
        for face in 0..face_count {
            budget.charge_into_error(1)?;
            face_groups.push(u32::try_from(face).map_err(|_| ResourceError::SizeOverflow)?);
        }
        budget.charge_into_error(
            u64::try_from(face_count).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let sewing = native_graph_sewing(&self.vertex_keys, &common.points.exact, frame, budget)?;
        let producer_uses = build_native_producer_use_table(self.producer_edges, budget)?;
        Ok(NativeReferenceGraph {
            common,
            faces,
            vertex_keys: self.vertex_keys,
            vertices: self.vertices,
            atoms: self.atoms,
            written_edges: self.written_edges,
            producer_uses,
            face_source_uses: NativeGraphFaceSourceTable {
                offsets: Vec::new(),
                uses: Vec::new(),
            },
            sewing,
            face_regions: vec![None; face_count],
            face_groups,
            face_cells: vec![None; face_count],
            cycles: Vec::new(),
            parts: Vec::new(),
        })
    }
}

pub(in crate::geometry::topology::q_embedding) fn align_producer_fan_target(
    target: &ExactChartPoint,
    source_span_target: &ExactChartPoint,
    base_lift: ExactDyadic,
    frame: GeographicFrame,
    work: &mut impl Work,
) -> Result<ExactChartPoint> {
    let period = ExactRational::from_f64(frame.longitude().period());
    // Resolve seam aliases against the retained source-span target so they
    // preserve the fan ray rather than defaulting to the opposite chart bank.
    let base_shift = period.try_product_with_work(&ExactRational::from_dyadic(base_lift), work)?;
    let base_x = target.x.try_add_with_work(&base_shift, work)?;
    let mut best_offset = 0_i64;
    let mut best_distance = None;
    // Split-site normalization has already selected the base image; only an
    // adjacent image can be nearer after resolving the seam occurrence.
    for offset in [0_i64, -1, 1] {
        work.charge_into_error(1)?;
        let shifted_x = if offset == 0 {
            base_x.try_clone_with_work(work)?
        } else {
            let shift = period.try_product_with_work(
                &ExactRational::from_dyadic(ExactDyadic::from_i64(offset)),
                work,
            )?;
            base_x.try_add_with_work(&shift, work)?
        };
        let difference = shifted_x.try_subtract_with_work(&source_span_target.x, work)?;
        let distance = difference.try_product_with_work(&difference, work)?;
        let improves = match &best_distance {
            Some(best) => distance.try_cmp_with_work(best, work)?.is_lt(),
            None => true,
        };
        if improves {
            best_offset = offset;
            best_distance = Some(distance);
        }
    }
    let x = if best_offset == 0 {
        base_x
    } else {
        let shift = period.try_product_with_work(
            &ExactRational::from_dyadic(ExactDyadic::from_i64(best_offset)),
            work,
        )?;
        base_x.try_add_with_work(&shift, work)?
    };
    Ok(ExactChartPoint::from_rationals(
        x,
        target.y.try_clone_with_work(work)?,
    ))
}
