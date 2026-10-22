#[cfg(test)]
use std::cell::Cell;

use super::exact_partition::ordinates::ResolvedNodePoints;
use crate::error::Result;
use crate::geometry::ordinates::{
    NodeOrdinates, RootMetricState, SourceArcContext, resolve_source_root,
};
use crate::geometry::overlay::{
    ExactLineEmission, ExactSourceInventory, NodeId, OverlayOp, Strictness, merge_chains,
    merge_chains_with_origins, overlay_all,
};
use crate::geometry::segments::projection::segment_projection;
use crate::geometry::topology::overlay::{QProducer, exact_geographic_linework_from_shapes};
use crate::geometry::{
    Bounds, CoordSeq, CoordinateAxes, GeometryErrorKind, GeometryKind, HashMap, HashSet, LineSeq,
    Lineal, Point, PointKey, Segment, Shape, TopologyDomain, carry_ordinates,
    for_each_overlapping_bounds_cross_pair_with_work, point_on_segment,
    undirected_segment_edge_key,
};

#[cfg(test)]
std::thread_local! {
    static LINE_MERGE_SOURCE_CANDIDATES: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
pub(in crate::geometry) fn reset_line_merge_source_candidates() {
    LINE_MERGE_SOURCE_CANDIDATES.with(|count| count.set(0));
}

#[cfg(test)]
pub(in crate::geometry) fn line_merge_source_candidates() -> usize {
    LINE_MERGE_SOURCE_CANDIDATES.with(Cell::get)
}

#[cfg(test)]
fn record_line_merge_source_candidate() {
    LINE_MERGE_SOURCE_CANDIDATES.with(|count| count.set(count.get().saturating_add(1)));
}

impl Shape {
    pub(crate) fn intersection(&self, other: &Self, strictness: Strictness) -> Result<Self> {
        self.overlay(other, OverlayOp::Intersection, strictness)
    }

    pub(crate) fn union(&self, other: &Self, strictness: Strictness) -> Result<Self> {
        self.overlay(other, OverlayOp::Union, strictness)
    }

    pub(crate) fn difference(&self, other: &Self, strictness: Strictness) -> Result<Self> {
        self.overlay(other, OverlayOp::Difference, strictness)
    }

    pub(crate) fn symmetric_difference(
        &self,
        other: &Self,
        strictness: Strictness,
    ) -> Result<Self> {
        self.overlay(other, OverlayOp::SymmetricDifference, strictness)
    }

    /// Dispatch an overlay op. `overlay_all` owns the operation's
    /// dimension-typed empty normalization for binary and n-ary inputs, so a
    /// vanishing result remains `POLYGON EMPTY` for a canceled polygon
    /// difference rather than an untyped collection among array siblings.
    pub(crate) fn overlay(
        &self,
        other: &Self,
        op: OverlayOp,
        strictness: Strictness,
    ) -> Result<Self> {
        self.overlay_inner(other, op, strictness)
    }

    /// Mixed-dimension set overlay over X/Y. Binary puntal operations,
    /// line/polygon intersections, and line-minus-polygon differences use
    /// dimension-specific lanes; other inputs use the exact partition. New
    /// vertices restore Z/M from source provenance, or drop an unavailable axis
    /// when `strict` is false.
    pub(crate) fn overlay_inner(
        &self,
        other: &Self,
        op: OverlayOp,
        strictness: Strictness,
    ) -> Result<Self> {
        overlay_all(&[self, other], TopologyDomain::Cartesian, op, strictness)
    }

    /// Dissolve every geometry into one mixed-dimension union.
    ///
    /// Cartesian inputs use the exact partition except for binary puntal unions,
    /// which can be resolved directly. Points and lines survive only where the
    /// selected operation does not absorb them. Z/M is restored afterwards
    /// unless `strict`.
    ///
    /// Planar-only internal path (no frame). Prefer
    /// [`Self::union_all_topo`] when a geographic frame is known so
    /// antimeridian-crossing operands are split first (same gate as binary
    /// overlay).
    pub(crate) fn union_all<S: std::borrow::Borrow<Self>>(
        geometries: &[S],
        strictness: Strictness,
    ) -> Result<Self> {
        Self::union_all_topo(geometries, TopologyDomain::Cartesian, strictness)
    }

    /// Frame-aware union. Geographic inputs use their frame-aware overlay
    /// route; Cartesian inputs use the exact partition except for binary
    /// puntal unions, which can be resolved directly.
    pub(crate) fn union_all_topo<S: std::borrow::Borrow<Self>>(
        geometries: &[S],
        domain: TopologyDomain,
        strictness: Strictness,
    ) -> Result<Self> {
        if geometries.is_empty() {
            return Err(GeometryErrorKind::EmptyGeometrySequence {
                operation: "union_all",
            }
            .into());
        }
        overlay_all(geometries, domain, OverlayOp::Union, strictness)
    }

    /// Frame-aware n-ary intersection. Geographic inputs use their frame-aware
    /// overlay route; Cartesian inputs use the exact partition except for
    /// binary puntal and line/polygon combinations, which can be resolved
    /// directly.
    #[cfg(test)]
    pub(crate) fn intersection_all_topo<S: std::borrow::Borrow<Self>>(
        geometries: &[S],
        domain: TopologyDomain,
        strictness: Strictness,
    ) -> Result<Self> {
        if geometries.is_empty() {
            return Err(GeometryErrorKind::EmptyGeometrySequence {
                operation: "intersection_all",
            }
            .into());
        }
        overlay_all(geometries, domain, OverlayOp::Intersection, strictness)
    }

    /// Frame-aware n-ary symmetric difference. Geographic inputs use their
    /// frame-aware overlay route; binary Cartesian puntal combinations can be
    /// resolved directly.
    #[cfg(test)]
    pub(crate) fn symmetric_difference_all_topo<S: std::borrow::Borrow<Self>>(
        geometries: &[S],
        domain: TopologyDomain,
        strictness: Strictness,
    ) -> Result<Self> {
        if geometries.is_empty() {
            return Err(GeometryErrorKind::EmptyGeometrySequence {
                operation: "symmetric_difference_all",
            }
            .into());
        }
        overlay_all(
            geometries,
            domain,
            OverlayOp::SymmetricDifference,
            strictness,
        )
    }

    /// Node this geometry's linework: split every edge at all intersections
    /// (crossings and shared points) and return the noded edges as a
    /// `MultiLineString` (GEOS/PostGIS `ST_Node`). Polygon boundaries are
    /// included; point-only input yields `MULTILINESTRING EMPTY`. Z/M is
    #[cfg(test)]
    pub(crate) fn node_with_context<M: crate::geometry::ChartMetric>(
        &self,
        strictness: Strictness,
        context: &SourceArcContext<'_, '_, M>,
    ) -> Result<Self> {
        self.node_with_publication_frame(strictness, context, crate::Frame::None)
    }

    pub(crate) fn node_with_publication_frame<M: crate::geometry::ChartMetric>(
        &self,
        strictness: Strictness,
        context: &SourceArcContext<'_, '_, M>,
        publication_frame: crate::Frame,
    ) -> Result<Self> {
        if context.domain().is_geographic() {
            return exact_geographic_linework_from_shapes(
                &[self],
                QProducer::Node,
                false,
                strictness.is_strict(),
                context,
                publication_frame,
            );
        }
        let inventory = ExactSourceInventory::from_shapes(&[self])?;
        let mut budget = crate::geometry::ExpansionBudget::new("node", "exact partition");
        let partition = inventory.partition(&mut budget)?;
        inventory.node_shape(&partition, self.axes(), strictness.is_strict(), context)
    }

    pub fn line_merge(&self) -> Result<Self> {
        self.line_merge_with_context(
            &crate::geometry::ordinates::SourceArcContext::cartesian(),
            false,
        )
    }

    pub(crate) fn line_merge_with_context(
        &self,
        context: &SourceArcContext<'_, '_>,
        directed: bool,
    ) -> Result<Self> {
        let lineal = self.lineal().ok_or(GeometryErrorKind::LinealRequired)?;
        if let TopologyDomain::Geographic(frame) = context.domain() {
            return match lineal {
                Lineal::MultiLineString(lines) => {
                    line_merge_geographic(self, lines, frame, context, directed)
                },
                Lineal::LineString(_) | Lineal::Empty => Ok(self.clone()),
            };
        }
        let merged = match lineal {
            Lineal::LineString(_) | Lineal::Empty => self.clone(),
            Lineal::MultiLineString(lines) => {
                // Dissolve the component lines into maximal chains at degree-2
                // nodes, splitting every Y/T/X junction (degree `>= 3`). The
                // overlay line sink uses the same [`merge_chains`] rule.
                let chains = super::merge_line_chains(lines, directed)?;
                // A merge that produced nothing keeps the lineal type
                // (`MULTILINESTRING EMPTY`), unlike the general narrowing.
                if chains.is_empty() {
                    Self::typed_empty(GeometryKind::MultiLineString, lines.axes())
                } else {
                    let mut proven = Vec::with_capacity(chains.len());
                    for line in chains {
                        proven.push(LineSeq::from_proven(line));
                    }
                    Self::narrow_lines(lines.axes(), proven)
                }
            },
        };
        if directed {
            Ok(merged)
        } else {
            carry_ordinates(merged, &[self], "line_merge", false, context)
        }
    }

    pub fn shared_paths(&self, other: &Self) -> Result<Self> {
        self.shared_paths_with_context(
            other,
            &crate::geometry::ordinates::SourceArcContext::cartesian(),
        )
    }

    pub(crate) fn shared_paths_with_context(
        &self,
        other: &Self,
        context: &SourceArcContext<'_, '_>,
    ) -> Result<Self> {
        self.linework()
            .map_err(|_| GeometryErrorKind::LinealRequired)?;
        other
            .linework()
            .map_err(|_| GeometryErrorKind::LinealRequired)?;
        let inventory = ExactSourceInventory::from_shapes(&[self, other])?;
        let mut budget = crate::geometry::ExpansionBudget::new("shared_paths", "exact partition");
        let mut partition = inventory.partition(&mut budget)?;
        if let TopologyDomain::Geographic(frame) = context.domain() {
            partition.set_geographic_frame(frame);
        }
        let emission = inventory.line_emission(&partition, "shared_paths")?;
        let declared_axes = self.axes().union(other.axes());
        let (same_segments, same_directions, opposite_segments, opposite_directions) =
            shared_path_segments_from_exact(&emission)?;
        let resolved = inventory.resolve_line_emission_points(
            &partition,
            &emission,
            same_segments
                .iter()
                .chain(&opposite_segments)
                .flat_map(|edge| edge.nodes),
            declared_axes,
            context,
            None,
        )?;
        let (same_axes, same) =
            merge_shared_path_segments(&same_segments, &resolved, declared_axes, &same_directions)?;
        let (opposite_axes, opposite) = merge_shared_path_segments(
            &opposite_segments,
            &resolved,
            declared_axes,
            &opposite_directions,
        )?;
        let result_parts = vec![
            Self::multi_line_string(
                same_axes,
                same.into_iter().map(LineSeq::from_proven).collect(),
            ),
            Self::multi_line_string(
                opposite_axes,
                opposite.into_iter().map(LineSeq::from_proven).collect(),
            ),
        ];
        Self::geometry_collection(result_parts)
    }
}

fn line_merge_geographic(
    input: &Shape,
    lines: &crate::geometry::MultiLineString,
    frame: crate::geographic::GeographicFrame,
    context: &SourceArcContext<'_, '_>,
    directed: bool,
) -> Result<Shape> {
    if directed {
        return super::line_merge::directed_geographic_line_merge(input, lines, frame, context);
    }
    if !input.has_z() && !input.has_m() {
        let chains = merge_chains(lines)?;
        return Ok(if chains.is_empty() {
            Shape::typed_empty(GeometryKind::MultiLineString, input.axes())
        } else {
            Shape::narrow_lines(
                lines.axes(),
                chains.into_iter().map(LineSeq::from_proven).collect(),
            )
        });
    }

    let (chains, origins) = merge_chains_with_origins(lines)?;
    if chains.is_empty() {
        return Ok(Shape::typed_empty(
            GeometryKind::MultiLineString,
            input.axes(),
        ));
    }
    if chains.len() != origins.len()
        || chains
            .iter()
            .zip(&origins)
            .any(|(chain, chain_origins)| chain.len() != chain_origins.len())
    {
        return Err(GeometryErrorKind::Invalid(
            "line-merge output coordinates lost their source occurrences".into(),
        )
        .into());
    }

    let mut source_segment_count = 0_usize;
    for line in lines {
        source_segment_count = source_segment_count
            .checked_add(line.len().saturating_sub(1))
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
    }
    let hash_state = crate::collections::collection_hash_state()?.clone();
    let output_occurrences = origins.iter().try_fold(0_usize, |count, chain| {
        count
            .checked_add(chain.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)
    })?;
    let mut output_node_of = HashMap::<PointKey, usize>::with_hasher(hash_state);
    output_node_of.reserve(output_occurrences);
    let mut nodes = Vec::with_capacity(output_occurrences);
    let mut chain_nodes = Vec::with_capacity(chains.len());
    for (chain, chain_origins) in chains.iter().zip(&origins) {
        let mut node_ids = Vec::with_capacity(chain.len());
        for (point, &(line_index, vertex_index)) in chain.iter().zip(chain_origins) {
            // The origin selects the source occurrence; the key only joins its exact node incidences.
            let source_line = lines.get(line_index).ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "line-merge occurrence references an unknown line".into(),
                )
            })?;
            if vertex_index >= source_line.len() {
                return Err(GeometryErrorKind::Invalid(
                    "line-merge occurrence references an unknown vertex".into(),
                )
                .into());
            }
            let origin = source_line.point_at(vertex_index);
            let key = PointKey::new(origin);
            if PointKey::new(point) != key {
                return Err(GeometryErrorKind::Invalid(
                    "line-merge output coordinate differs from its source occurrence".into(),
                )
                .into());
            }
            let node = match output_node_of.entry(key) {
                std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let index = nodes.len();
                    nodes.push(LineMergeNode {
                        point: origin,
                        ordinates: NodeOrdinates::default(),
                    });
                    entry.insert(index);
                    index
                },
            };
            node_ids.push(node);
        }
        chain_nodes.push(node_ids);
    }

    let mut source_segments = Vec::with_capacity(source_segment_count);
    for line in lines {
        for point in line {
            let Some(&node_index) = output_node_of.get(&PointKey::new(point)) else {
                continue;
            };
            let node = nodes
                .get_mut(node_index)
                .expect("source endpoint incidence belongs to its output node table");
            if input.axes().has_z() {
                node.ordinates.z.observe(point.z());
            }
            if input.axes().has_m() {
                node.ordinates.m.observe(point.m());
            }
        }
        for vertex in 0..line.len().saturating_sub(1) {
            let start = line.point_at(vertex);
            let end = line.point_at(vertex.wrapping_add(1));
            if PointKey::new(start) == PointKey::new(end) {
                continue;
            }
            let axes = CoordinateAxes::new(
                crate::geometry::HasZ(
                    input.axes().has_z() && start.z().is_some() && end.z().is_some(),
                ),
                crate::geometry::HasM(
                    input.axes().has_m() && start.m().is_some() && end.m().is_some(),
                ),
            );
            if axes.has_z() || axes.has_m() {
                source_segments.push(LineMergeSource {
                    points: [start, end],
                    metric_state: RootMetricState::default(),
                });
            }
        }
    }
    drop(output_node_of);

    resolve_line_merge_source_incidence(
        &mut nodes,
        &mut source_segments,
        input.axes(),
        frame,
        context,
    )?;
    drop(source_segments);
    let mut axes = input.axes();
    for node in &nodes {
        axes = CoordinateAxes::new(
            crate::geometry::HasZ(axes.has_z() && node.ordinates.z_value().is_some()),
            crate::geometry::HasM(axes.has_m() && node.ordinates.m_value().is_some()),
        );
    }

    let mut output = Vec::with_capacity(chains.len());
    for (chain, node_ids) in chains.into_iter().zip(chain_nodes) {
        let mut points = Vec::with_capacity(chain.len());
        for (point, node_id) in chain.iter().zip(node_ids) {
            let ordinates = nodes
                .get(node_id)
                .expect("line-merge output node belongs to its node table")
                .ordinates;
            points.push(Point::new_raw_axes(
                point.x,
                point.y,
                crate::geometry::ZOrdinate(
                    axes.has_z()
                        .then(|| ordinates.z_value().expect("retained Z is resolved")),
                ),
                crate::geometry::MOrdinate(
                    axes.has_m()
                        .then(|| ordinates.m_value().expect("retained M is resolved")),
                ),
            ));
        }
        output.push(LineSeq::from_proven(CoordSeq::try_from_points(&points)?));
    }
    Ok(Shape::narrow_lines(axes, output))
}

struct LineMergeNode {
    point: Point,
    ordinates: NodeOrdinates,
}

struct LineMergeSource {
    points: [Point; 2],
    metric_state: RootMetricState,
}

fn resolve_line_merge_source_incidence<M: crate::geometry::ChartMetric>(
    nodes: &mut [LineMergeNode],
    sources: &mut [LineMergeSource],
    input_axes: CoordinateAxes,
    frame: crate::geographic::GeographicFrame,
    context: &SourceArcContext<'_, '_, M>,
) -> Result<()> {
    // Only retained merge nodes query source interiors; unrelated crossings are not merge incidences.
    let node_bounds = nodes
        .iter()
        .map(|node| Bounds::new_raw(node.point.x, node.point.y, node.point.x, node.point.y))
        .collect::<Vec<_>>();
    let source_bounds = sources
        .iter()
        .map(|source| {
            let [start, end] = source.points;
            Bounds::new_raw(
                start.x.min(end.x),
                start.y.min(end.y),
                start.x.max(end.x),
                start.y.max(end.y),
            )
        })
        .collect::<Vec<_>>();
    let mut work = crate::work::Unmetered;
    let _flow: std::ops::ControlFlow<()> = for_each_overlapping_bounds_cross_pair_with_work(
        &node_bounds,
        &source_bounds,
        &mut work,
        |node_index, source_index, work| {
            #[cfg(test)]
            record_line_merge_source_candidate();

            let node = nodes
                .get_mut(node_index)
                .expect("candidate node belongs to retained line-merge nodes");
            let source = sources
                .get_mut(source_index)
                .expect("candidate source belongs to line-merge source edges");
            let [start, end] = source.points;
            let key = PointKey::new(node.point);
            if key == PointKey::new(start) || key == PointKey::new(end) {
                return Ok(std::ops::ControlFlow::<()>::Continue(()));
            }

            let segment = Segment {
                start: start.into(),
                end: end.into(),
            };
            if !point_on_segment(node.point, segment.start, segment.end) {
                return Ok(std::ops::ControlFlow::<()>::Continue(()));
            }

            let axes = CoordinateAxes::new(
                crate::geometry::HasZ(
                    input_axes.has_z() && start.z().is_some() && end.z().is_some(),
                ),
                crate::geometry::HasM(
                    input_axes.has_m() && start.m().is_some() && end.m().is_some(),
                ),
            );
            if !axes.has_z() && !axes.has_m() {
                return Ok(std::ops::ControlFlow::<()>::Continue(()));
            }

            let parameter = segment_projection(node.point, segment)?.exact_fraction()?;
            let longitude = frame
                .longitude()
                .chart_connection(start.x, end.x)
                .map(crate::geographic::ChartConnection::retained)
                .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
            let mut fraction = None;
            let mut metric_state = source.metric_state;
            let ordinates = resolve_source_root(
                frame,
                &source.points,
                longitude,
                &parameter,
                axes,
                context.metric_access(),
                &mut metric_state,
                &mut fraction,
                work,
            )?;
            source.metric_state = metric_state;
            node.ordinates.merge(ordinates);
            Ok(std::ops::ControlFlow::<()>::Continue(()))
        },
    )?;
    Ok(())
}

type SharedPathExtraction = (
    Vec<SharedPathSegment>,
    HashMap<(PointKey, PointKey), Segment>,
    Vec<SharedPathSegment>,
    HashMap<(PointKey, PointKey), Segment>,
);

fn shared_path_segments_from_exact(
    emission: &ExactLineEmission<'_>,
) -> Result<SharedPathExtraction> {
    let mut same = Vec::new();
    let mut opposite = Vec::new();
    let mut same_path_direction =
        HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    let mut opposite_path_direction =
        HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    let mut same_keys = HashSet::with_hasher(crate::collections::collection_hash_state()?.clone());
    let mut opposite_keys =
        HashSet::with_hasher(crate::collections::collection_hash_state()?.clone());
    same.reserve(emission.len());
    opposite.reserve(emission.len());
    for map in [&mut same_path_direction, &mut opposite_path_direction] {
        map.reserve(emission.len());
    }
    for set in [&mut same_keys, &mut opposite_keys] {
        set.reserve(emission.len());
    }

    for (index, (nodes, segment)) in emission.segments_with_nodes().enumerate() {
        let mut left_direction = None;
        let mut left_mask = 0_u8;
        let mut right_mask = 0_u8;
        for source in emission.sources(index) {
            let direction = !source.reversed;
            match source.operand {
                0 => {
                    left_direction.get_or_insert(direction);
                    left_mask |= if direction { 1 } else { 2 };
                },
                1 => {
                    right_mask |= if direction { 1 } else { 2 };
                },
                _ => {},
            }
        }
        let Some(left_direction) = left_direction else {
            continue;
        };
        let has_same_direction = left_mask & right_mask != 0;
        let has_opposite_direction = (left_mask & 1 != 0 && right_mask & 2 != 0)
            || (left_mask & 2 != 0 && right_mask & 1 != 0);
        if !has_same_direction && !has_opposite_direction {
            continue;
        }
        let key = undirected_segment_edge_key(segment);
        let (nodes, segment) = if left_direction {
            (nodes, segment)
        } else {
            ([nodes[1], nodes[0]], Segment {
                start: segment.end,
                end: segment.start,
            })
        };
        let edge = SharedPathSegment { nodes, segment };
        if has_opposite_direction {
            opposite_keys.insert(key);
            opposite_path_direction.insert(key, segment);
            opposite.push(edge);
        } else {
            same_keys.insert(key);
            same_path_direction.insert(key, segment);
            same.push(edge);
        }
    }
    same.retain(|edge| !opposite_keys.contains(&undirected_segment_edge_key(edge.segment)));
    same_keys.retain(|key| !opposite_keys.contains(key));
    same_path_direction.retain(|key, _| same_keys.contains(key));

    Ok((same, same_path_direction, opposite, opposite_path_direction))
}

#[derive(Clone, Copy)]
struct SharedPathSegment {
    nodes: [NodeId; 2],
    segment: Segment,
}

fn merge_shared_path_segments(
    segments: &[SharedPathSegment],
    resolved: &ResolvedNodePoints<'_, '_>,
    declared_axes: CoordinateAxes,
    directions: &HashMap<(PointKey, PointKey), Segment>,
) -> Result<(CoordinateAxes, Vec<CoordSeq>)> {
    if segments.is_empty() {
        return Ok((declared_axes, Vec::new()));
    }
    let mut has_z = declared_axes.has_z();
    let mut has_m = declared_axes.has_m();
    for edge in segments {
        for node in edge.nodes {
            let (z, m) = resolved
                .ordinates(node)
                .expect("shared path node has resolved source ordinates");
            has_z &= z.0.is_some();
            has_m &= m.0.is_some();
        }
    }
    let axes = CoordinateAxes::new(crate::geometry::HasZ(has_z), crate::geometry::HasM(has_m));
    let mut lines = Vec::with_capacity(segments.len());
    for edge in segments {
        let point = |node: NodeId, xy: crate::geometry::XY| {
            let point = resolved.point(node, xy);
            Point::new_raw_axes(
                point.x,
                point.y,
                crate::geometry::ZOrdinate(axes.has_z().then(|| {
                    point
                        .z()
                        .expect("shared path Z is present in every selected node")
                })),
                crate::geometry::MOrdinate(axes.has_m().then(|| {
                    point
                        .m()
                        .expect("shared path M is present in every selected node")
                })),
            )
        };
        lines.push(CoordSeq::try_from_points(&[
            point(edge.nodes[0], edge.segment.start),
            point(edge.nodes[1], edge.segment.end),
        ])?);
    }
    Ok((
        axes,
        orient_shared_paths(merge_chains(&lines)?, directions)?,
    ))
}

fn orient_shared_paths(
    paths: Vec<CoordSeq>,
    directions: &HashMap<(PointKey, PointKey), Segment>,
) -> Result<Vec<CoordSeq>> {
    let mut oriented = Vec::with_capacity(paths.len());
    for path in paths {
        let start = path.first().expect("shared path has a start").xy();
        let end = path.point_at(1).xy();
        let key = undirected_segment_edge_key(Segment { start, end });
        let Some(expected) = directions.get(&key) else {
            oriented.push(path);
            continue;
        };
        if start == expected.start && end == expected.end {
            oriented.push(path);
        } else {
            let mut points = Vec::with_capacity(path.len());
            points.extend(path.iter().rev());
            oriented.push(CoordSeq::try_from_points(&points)?);
        }
    }
    Ok(oriented)
}
