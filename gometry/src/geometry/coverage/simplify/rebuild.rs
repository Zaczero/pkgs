use super::*;

type ChainReplacement = (Vec<(NodeId, XY)>, Option<Vec<CoverageEdgeIntent>>);
type ResolvedNodeOrdinates = (crate::geometry::ZOrdinate, crate::geometry::MOrdinate);

#[derive(Clone, Copy)]
struct SourceAtom {
    span: u32,
    reversed: bool,
}

pub(super) struct RebuildRoots<'inventory, 'access, 'metric> {
    inventory: &'inventory ExactSourceInventory,
    context: SourceArcContext<'access, 'metric, crate::crs::GeodesicMetric>,
    axes: crate::geometry::CoordinateAxes,
    by_node: Vec<Option<ResolvedNodeOrdinates>>,
}

impl<'inventory, 'access, 'metric> RebuildRoots<'inventory, 'access, 'metric> {
    pub(super) fn new(
        inventory: &'inventory ExactSourceInventory,
        context: &SourceArcContext<'access, 'metric, crate::crs::GeodesicMetric>,
        axes: crate::geometry::CoordinateAxes,
        node_count: usize,
    ) -> Self {
        Self {
            inventory,
            context: *context,
            axes,
            by_node: vec![None; node_count],
        }
    }

    fn resolve(&mut self, partition: &ExactPartition<'_>, node: NodeId) -> Result<()> {
        let slot = self.by_node.get_mut(node.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("coverage rebuild references an unknown source node".into())
        })?;
        if slot.is_none() {
            *slot = Some(self.inventory.resolve_node_ordinates(
                partition,
                node,
                self.axes,
                &self.context,
                None,
            )?);
        }
        Ok(())
    }

    fn ordinates(&self, node: NodeId) -> Result<ResolvedNodeOrdinates> {
        self.by_node
            .get(node.0 as usize)
            .copied()
            .flatten()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "coverage rebuild lost a retained source-node occurrence".into(),
                )
                .into()
            })
    }
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "NonZeroUsize proves the cyclic remainder divisor cannot be zero"
)]
const fn cyclic_index(index: usize, count: NonZeroUsize) -> usize {
    index.wrapping_rem(count.get())
}

fn source_ordinals_for_ring(
    ring: &CoordSeq,
    source_segments: &[Segment],
    source_range: &std::ops::Range<usize>,
) -> Result<Vec<usize>> {
    if source_range.len() != ring.len().saturating_sub(1) {
        return Err(GeometryErrorKind::Invalid(
            "coverage source ring range does not match its coordinate count".into(),
        )
        .into());
    }
    let mut ordinals = Vec::with_capacity(ring.len().saturating_sub(1));
    for ordinal in source_range.clone() {
        let source = source_segments.get(ordinal).ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "coverage source ring range exceeds the exact inventory".into(),
            )
        })?;
        if !same_point(source.start, source.end) {
            ordinals.push(ordinal);
        }
    }
    Ok(ordinals)
}

fn source_edge_intents(source_range: &std::ops::Range<usize>) -> Vec<CoverageEdgeIntent> {
    vec![CoverageEdgeIntent::Source; source_range.len()]
}

fn source_run_nodes(partition: &ExactPartition<'_>, run: &[SourceAtom]) -> Result<Vec<NodeId>> {
    let Some(&first) = run.first() else {
        return Ok(Vec::new());
    };
    let first_span = *partition
        .spans
        .get(first.span as usize)
        .expect("coverage source span is within the exact partition");
    let first_node = if first.reversed {
        first_span.end
    } else {
        first_span.start
    };
    let mut nodes = Vec::with_capacity(run.len().wrapping_add(1));
    nodes.push(first_node);
    let mut current = first_node;
    for &atom in run {
        let span = *partition
            .spans
            .get(atom.span as usize)
            .expect("coverage source span is within the exact partition");
        let (start, end) = if atom.reversed {
            (span.end, span.start)
        } else {
            (span.start, span.end)
        };
        if start != current {
            return Err(GeometryErrorKind::Invalid(
                "coverage source atoms are not contiguous in ring order".into(),
            )
            .into());
        }
        nodes.push(end);
        current = end;
    }
    Ok(nodes)
}

fn chain_run_indices(
    chain: &Chain,
    run: &[SourceAtom],
    run_nodes: &[NodeId],
) -> Option<Vec<usize>> {
    let count = chain.spans.len();
    let count_nonzero = NonZeroUsize::new(count)?;
    if run.is_empty() || run.len() > count {
        return None;
    }
    let cyclic = chain.nodes.first() == chain.nodes.last();
    let matches = |start: usize, forward: bool| {
        for (index, atom) in run.iter().enumerate() {
            let (span_index, node_index) = if cyclic {
                if forward {
                    let index = cyclic_index(start.wrapping_add(index), count_nonzero);
                    (index, index)
                } else {
                    (
                        cyclic_index(
                            start
                                .wrapping_add(count)
                                .wrapping_sub(1)
                                .wrapping_sub(index),
                            count_nonzero,
                        ),
                        cyclic_index(start.wrapping_add(count).wrapping_sub(index), count_nonzero),
                    )
                }
            } else if forward {
                let index = start.wrapping_add(index);
                (index, index)
            } else {
                (
                    start.wrapping_sub(1).wrapping_sub(index),
                    start.wrapping_sub(index),
                )
            };
            if *chain
                .spans
                .get(span_index)
                .expect("matched span index is within the exact chain")
                != atom.span
                || *chain
                    .nodes
                    .get(node_index)
                    .expect("matched node index is within the exact chain")
                    != *run_nodes
                        .get(index)
                        .expect("source run has one node for every span")
            {
                return None;
            }
        }
        let mut indices = Vec::with_capacity(run.len().wrapping_add(1));
        for index in 0..=run.len() {
            let node_index = if cyclic {
                if forward {
                    cyclic_index(start.wrapping_add(index), count_nonzero)
                } else {
                    cyclic_index(start.wrapping_add(count).wrapping_sub(index), count_nonzero)
                }
            } else if forward {
                start.wrapping_add(index)
            } else {
                start.wrapping_sub(index)
            };
            indices.push(node_index);
        }
        Some(indices)
    };

    if cyclic {
        for start in 0..count {
            if let Some(indices) = matches(start, true) {
                return Some(indices);
            }
            if let Some(indices) = matches(start, false) {
                return Some(indices);
            }
        }
    } else {
        for start in 0..count.wrapping_sub(run.len().wrapping_sub(1)) {
            if let Some(indices) = matches(start, true) {
                return Some(indices);
            }
        }
        for start in run.len()..=count {
            if let Some(indices) = matches(start, false) {
                return Some(indices);
            }
        }
    }
    None
}

fn replacement_for_chain_run(
    partition: &ExactPartition<'_>,
    chain: &Chain,
    simplified: &SimplifiedChain,
    run: &[SourceAtom],
    capture_edge_roles: bool,
) -> Result<ChainReplacement> {
    let run_nodes = source_run_nodes(partition, run)?;
    let indices = chain_run_indices(chain, run, &run_nodes).ok_or_else(|| {
        GeometryErrorKind::Invalid("coverage source run is absent from its exact chain".into())
    })?;
    let mut replacement = Vec::with_capacity(indices.len());
    let mut retained_positions = capture_edge_roles.then(|| Vec::with_capacity(indices.len()));
    for (position, &index) in indices.iter().enumerate() {
        if position == indices.len().wrapping_sub(1) || simplified.kept.contains(&index) {
            replacement.push((
                *chain
                    .nodes
                    .get(index)
                    .expect("simplified chain node index is within its node sequence"),
                *chain
                    .points
                    .get(index)
                    .expect("simplified chain index is within its point sequence"),
            ));
            if let Some(retained_positions) = &mut retained_positions {
                retained_positions.push(position);
            }
        }
    }
    let edge_roles = retained_positions.map(|positions| {
        positions
            .windows(2)
            .map(|pair| match pair {
                [left, right] if left.checked_add(1) == Some(*right) => CoverageEdgeIntent::Source,
                _ => CoverageEdgeIntent::Constructed,
            })
            .collect()
    });
    Ok((replacement, edge_roles))
}

#[derive(Clone, Copy)]
struct RebuiltVertex {
    point: Point,
    node: Option<NodeId>,
}

type RebuiltRing = (Vec<RebuiltVertex>, Option<Vec<CoverageEdgeIntent>>);

struct RebuiltPolygon {
    rings: Vec<Vec<RebuiltVertex>>,
}

/// Rebuild one row's polygons by splicing simplified chains into each ring.
pub(super) fn rebuild_row(
    shape: &Shape,
    source_ring_cursor: &mut usize,
    source_ring_ranges: &[std::ops::Range<usize>],
    source_segments: &[Segment],
    source_spans: &[Vec<(u32, bool)>],
    chain_of_span: &[u32],
    chains: &[Chain],
    simplified: &[SimplifiedChain],
    partition: &ExactPartition<'_>,
    mut roots: Option<&mut RebuildRoots<'_, '_, '_>>,
) -> Result<(Shape, Option<CoverageEdgeRoles>)> {
    let capture_edge_roles = roots.is_some();
    let mut rebuild_ring = |ring: &CoordSeq| -> Result<RebuiltRing> {
        let original: Vec<Point> = ring.iter_coords().collect();
        let source_range = source_ring_ranges.get(*source_ring_cursor).ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "coverage source ring is absent from the admitted inventory".into(),
            )
        })?;
        *source_ring_cursor = source_ring_cursor
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let source_ordinals = source_ordinals_for_ring(ring, source_segments, source_range)?;
        if original.len() < 4 {
            let vertices = original
                .into_iter()
                .map(|point| RebuiltVertex { point, node: None })
                .collect();
            let roles = capture_edge_roles.then(|| source_edge_intents(source_range));
            return Ok((vertices, roles));
        }
        // Match collect_polygon_segments: elide stutter vertices so chain
        // lookups never probe zero-length edges absent from the exact partition.
        let (_, open_ring) = original
            .split_last()
            .expect("ring with at least four coordinates is non-empty");
        let mut open = open_ring.to_vec();
        dedup_consecutive_points(&mut open);
        if open.len() < 3 {
            let vertices = original
                .into_iter()
                .map(|point| RebuiltVertex { point, node: None })
                .collect();
            let roles = capture_edge_roles.then(|| source_edge_intents(source_range));
            return Ok((vertices, roles));
        }
        // Geographic rebuilds retain node roots; coordinate-key recovery is
        // permitted only for the Cartesian carry path.
        let source_point_by_key: Option<HashMap<PointKey, Point>> =
            (!capture_edge_roles).then(|| {
                open.iter()
                    .copied()
                    .map(|point| (PointKey::new(point), point))
                    .collect()
            });
        let source_point = |point: XY| {
            source_point_by_key
                .as_ref()
                .and_then(|source| source.get(&PointKey::new(point)))
                .copied()
                .unwrap_or_else(|| Point::new_raw_xy(point.x, point.y))
        };
        let mut atoms = Vec::new();
        for &ordinal in &source_ordinals {
            let source = source_spans.get(ordinal).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage source ordinal is out of range".into())
            })?;
            if source.is_empty() {
                return Err(GeometryErrorKind::Invalid(
                    "coverage source segment has no exact atom".into(),
                )
                .into());
            }
            atoms.extend(
                source
                    .iter()
                    .map(|&(span, reversed)| SourceAtom { span, reversed }),
            );
        }
        let chain_at = |index: usize| {
            let atom = atoms
                .get(index)
                .expect("cyclic coverage atom index is within the ring");
            *chain_of_span
                .get(atom.span as usize)
                .expect("coverage span has an assigned chain") as usize
        };
        let count = atoms.len();
        let mut out: Vec<RebuiltVertex> = Vec::with_capacity(count.wrapping_add(1));
        let mut edge_roles = capture_edge_roles.then(|| Vec::with_capacity(count));
        let Some(atom_count) = NonZeroUsize::new(count) else {
            return Ok((out, edge_roles));
        };
        let start = (0..count)
            .find(|&index| {
                chain_at(cyclic_index(
                    index.wrapping_add(count).wrapping_sub(1),
                    atom_count,
                )) != chain_at(index)
            })
            .unwrap_or(0);
        let mut consumed = 0;
        while consumed < count {
            let here = cyclic_index(start.wrapping_add(consumed), atom_count);
            let id = chain_at(here);
            let mut len = 1;
            while len < count
                && chain_at(cyclic_index(
                    start.wrapping_add(consumed).wrapping_add(len),
                    atom_count,
                )) == id
            {
                len = len.wrapping_add(1);
            }
            let mut run = Vec::with_capacity(len);
            for offset in 0..len {
                run.push(
                    *atoms
                        .get(cyclic_index(
                            start.wrapping_add(consumed).wrapping_add(offset),
                            atom_count,
                        ))
                        .expect("coverage atom index is within the ring"),
                );
            }
            let (replacement, run_edge_roles) = replacement_for_chain_run(
                partition,
                chains
                    .get(id)
                    .expect("coverage span chain is in the chain table"),
                simplified
                    .get(id)
                    .expect("coverage span chain has a simplification result"),
                &run,
                capture_edge_roles,
            )?;
            for &(node, point) in replacement.iter().take(replacement.len().saturating_sub(1)) {
                if capture_edge_roles {
                    roots
                        .as_deref_mut()
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "geographic coverage rebuild has no retained source roots".into(),
                            )
                        })?
                        .resolve(partition, node)?;
                    out.push(RebuiltVertex {
                        point: Point::new_raw_xy(point.x, point.y),
                        node: Some(node),
                    });
                } else {
                    let point = source_point(point);
                    out.push(RebuiltVertex { point, node: None });
                }
            }
            match (&mut edge_roles, run_edge_roles) {
                (Some(edge_roles), Some(run_edge_roles)) => edge_roles.extend(run_edge_roles),
                (None, None) => {},
                _ => {
                    return Err(GeometryErrorKind::Invalid(
                        "coverage edge roles do not match the rebuild mode".into(),
                    )
                    .into());
                },
            }
            consumed = consumed.wrapping_add(len);
        }
        if let Some(&first) = out.first() {
            out.push(first);
        }
        if edge_roles
            .as_ref()
            .is_some_and(|roles| roles.len() != out.len().saturating_sub(1))
        {
            return Err(GeometryErrorKind::Invalid(
                "coverage edge roles do not match a rebuilt ring".into(),
            )
            .into());
        }
        Ok((out, edge_roles))
    };

    let (rebuilt, edge_roles, is_multi) = match shape {
        Shape::Polygon(polygon) => {
            let (rebuilt, roles) = rebuild_polygon(polygon, &mut rebuild_ring)?;
            (vec![rebuilt], roles.map(|roles| vec![roles]), false)
        },
        Shape::MultiPolygon(polygons) => {
            let mut rebuilt = Vec::with_capacity(polygons.len());
            let mut polygon_roles = capture_edge_roles.then(|| Vec::with_capacity(polygons.len()));
            for polygon in polygons {
                let (polygon, roles) = rebuild_polygon(polygon, &mut rebuild_ring)?;
                match (&mut polygon_roles, roles) {
                    (Some(polygon_roles), Some(roles)) => polygon_roles.push(roles),
                    (None, None) => {},
                    _ => {
                        return Err(GeometryErrorKind::Invalid(
                            "coverage edge roles do not match the rebuild mode".into(),
                        )
                        .into());
                    },
                }
                rebuilt.push(polygon);
            }
            (rebuilt, polygon_roles, true)
        },
        Shape::Empty(..)
        | Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::GeometryCollection(_) => {
            return Ok((shape.clone(), capture_edge_roles.then(Vec::new)));
        },
    };
    if !capture_edge_roles {
        let axes = shape.axes();
        let mut polygons = Vec::with_capacity(rebuilt.len());
        for polygon in rebuilt {
            let mut rings = polygon.rings.into_iter();
            let shell =
                build_ring_cartesian(rings.next().expect("rebuilt polygon has an exterior ring"))?;
            let holes = rings
                .map(build_ring_cartesian)
                .collect::<Result<Vec<_>>>()?;
            polygons.push(Polygon::new(shell, holes));
        }
        let shape = if is_multi {
            Shape::multi_polygon(axes, polygons)
        } else {
            Shape::Polygon(
                polygons
                    .into_iter()
                    .next()
                    .expect("rebuilt polygon source has one polygon"),
            )
        };
        return Ok((shape, edge_roles));
    }
    let mut axes = shape.axes();
    for vertex in rebuilt
        .iter()
        .flat_map(|polygon| polygon.rings.iter().flatten())
    {
        let (z, m) = rebuilt_vertex_ordinates(vertex, roots.as_deref())?;
        axes = crate::geometry::CoordinateAxes::new(
            crate::geometry::HasZ(axes.has_z() && z.is_some()),
            crate::geometry::HasM(axes.has_m() && m.is_some()),
        );
    }
    let mut polygons = Vec::with_capacity(rebuilt.len());
    for polygon in rebuilt {
        let mut rings = polygon.rings.into_iter();
        let shell = build_ring(
            rings.next().expect("rebuilt polygon has an exterior ring"),
            axes,
            roots.as_deref(),
        )?;
        let holes = rings
            .map(|ring| build_ring(ring, axes, roots.as_deref()))
            .collect::<Result<Vec<_>>>()?;
        polygons.push(Polygon::new(shell, holes));
    }
    let shape = if is_multi {
        Shape::multi_polygon(axes, polygons)
    } else {
        Shape::Polygon(
            polygons
                .into_iter()
                .next()
                .expect("rebuilt polygon source has one polygon"),
        )
    };
    Ok((shape, edge_roles))
}

fn build_ring_cartesian(vertices: Vec<RebuiltVertex>) -> Result<Ring> {
    let points: Vec<_> = vertices.into_iter().map(|vertex| vertex.point).collect();
    Ring::try_from_closed(CoordSeq::try_from_points(&points)?)
}

fn rebuilt_vertex_ordinates(
    vertex: &RebuiltVertex,
    roots: Option<&RebuildRoots<'_, '_, '_>>,
) -> Result<(Option<f64>, Option<f64>)> {
    if let Some(node) = vertex.node {
        let roots = roots.ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "coverage rebuild lost its retained source-root catalog".into(),
            )
        })?;
        let ordinates = roots.ordinates(node)?;
        Ok((ordinates.0.0, ordinates.1.0))
    } else {
        let point = vertex.point;
        Ok((point.z(), point.m()))
    }
}

fn build_ring(
    vertices: Vec<RebuiltVertex>,
    axes: crate::geometry::CoordinateAxes,
    roots: Option<&RebuildRoots<'_, '_, '_>>,
) -> Result<Ring> {
    let mut coordinates = crate::geometry::CoordSeqBuilder::with_capacity(axes, vertices.len());
    for vertex in vertices {
        let (z, m) = rebuilt_vertex_ordinates(&vertex, roots)?;
        let point = vertex.point;
        coordinates.push(if vertex.node.is_some() || point.axes() != axes {
            Point::new_raw_axes(
                point.x,
                point.y,
                crate::geometry::ZOrdinate(
                    axes.has_z()
                        .then(|| z.expect("retained Z is present on every output vertex")),
                ),
                crate::geometry::MOrdinate(
                    axes.has_m()
                        .then(|| m.expect("retained M is present on every output vertex")),
                ),
            )
        } else {
            point
        });
    }
    Ring::try_from_closed(coordinates.finish()?)
}

fn rebuild_polygon(
    polygon: &Polygon,
    rebuild_ring: &mut impl FnMut(&CoordSeq) -> Result<RebuiltRing>,
) -> Result<(RebuiltPolygon, Option<Vec<Vec<CoverageEdgeIntent>>>)> {
    let (shell, shell_roles) = rebuild_ring(polygon.shell.coords())?;
    let capture_edge_roles = shell_roles.is_some();
    let mut rings = Vec::with_capacity(polygon.holes.len().wrapping_add(1));
    rings.push(shell);
    let mut roles = Vec::with_capacity(polygon.holes.len().wrapping_add(1));
    if let Some(shell_roles) = shell_roles {
        roles.push(shell_roles);
    }
    for hole in polygon.holes.iter() {
        let (vertices, hole_roles) = rebuild_ring(hole.coords())?;
        rings.push(vertices);
        match (capture_edge_roles, hole_roles) {
            (true, Some(hole_roles)) => roles.push(hole_roles),
            (false, None) => {},
            _ => {
                return Err(GeometryErrorKind::Invalid(
                    "coverage ring roles do not match the polygon rebuild mode".into(),
                )
                .into());
            },
        }
    }
    Ok((
        RebuiltPolygon { rings },
        capture_edge_roles.then_some(roles),
    ))
}
