use super::*;

#[derive(Clone, Copy)]
struct SourceAtom {
    span: u32,
    reversed: bool,
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
    cursor: &mut usize,
) -> Result<Vec<u32>> {
    let mut ordinals = Vec::with_capacity(ring.len().saturating_sub(1));
    for segment in line_segments(ring).filter(|segment| !same_point(segment.start, segment.end)) {
        let ordinal = (*cursor..source_segments.len()).find(|&index| {
            let source = *source_segments
                .get(index)
                .expect("source ordinal search stays within the inventory");
            same_point(source.start, segment.start) && same_point(source.end, segment.end)
        });
        let Some(ordinal) = ordinal else {
            return Err(GeometryErrorKind::Invalid(
                "coverage source ring is absent from the exact inventory".into(),
            )
            .into());
        };
        *cursor = ordinal.wrapping_add(1);
        ordinals.push(ordinal as u32);
    }
    Ok(ordinals)
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
) -> Result<(Vec<(NodeId, XY)>, Option<Vec<bool>>)> {
    let run_nodes = source_run_nodes(partition, run)?;
    let indices = chain_run_indices(chain, run, &run_nodes).ok_or_else(|| {
        GeometryErrorKind::Invalid("coverage source run is absent from its exact chain".into())
    })?;
    let mut replacement = Vec::with_capacity(indices.len());
    let mut retained_indices = capture_edge_roles.then(|| Vec::with_capacity(indices.len()));
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
            if let Some(retained_indices) = &mut retained_indices {
                retained_indices.push(index);
            }
        }
    }
    let edge_roles = retained_indices
        .map(|indices| {
            let span_count = NonZeroUsize::new(chain.spans.len()).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage chain has no source spans".into())
            })?;
            let cyclic = chain.nodes.first() == chain.nodes.last();
            Ok::<_, crate::error::Error>(
                indices
                    .array_windows::<2>()
                    .map(|[left, right]| {
                        let adjacent = if cyclic {
                            cyclic_index(left.wrapping_add(1), span_count) == *right
                                || cyclic_index(right.wrapping_add(1), span_count) == *left
                        } else {
                            left.abs_diff(*right) == 1
                        };
                        !adjacent
                    })
                    .collect(),
            )
        })
        .transpose()?;
    Ok((replacement, edge_roles))
}

#[derive(Clone, Copy)]
struct RebuiltVertex {
    xy: XY,
    point: Option<Point>,
    node: Option<NodeId>,
}

struct RebuiltPolygon {
    rings: Vec<Vec<RebuiltVertex>>,
}

/// Rebuild one row's polygons by splicing simplified chains into each ring.
pub(super) fn rebuild_row(
    shape: &Shape,
    source_cursor: &mut usize,
    source_segments: &[Segment],
    source_spans: &[Vec<(u32, bool)>],
    chain_of_span: &[u32],
    chains: &[Chain],
    simplified: &[SimplifiedChain],
    inventory: &ExactSourceInventory,
    partition: &ExactPartition<'_>,
    source_context: &super::SourceArcContext<'_, '_, crate::crs::GeodesicMetric>,
    root_axes: crate::geometry::CoordinateAxes,
    node_ordinates: &mut [Option<crate::geometry::NodeOrdinates>],
    capture_edge_roles: bool,
) -> Result<(Shape, Option<crate::geometry::PolygonPartEdgeRoles>)> {
    let mut rebuild_ring = |ring: &CoordSeq| -> Result<(Vec<RebuiltVertex>, Option<Vec<bool>>)> {
        let original: Vec<Point> = ring.iter_coords().collect();
        let source_ordinals = source_ordinals_for_ring(ring, source_segments, source_cursor)?;
        if original.len() < 4 {
            let vertices = original
                .into_iter()
                .map(|point| RebuiltVertex {
                    xy: point.xy(),
                    point: Some(point),
                    node: None,
                })
                .collect();
            let roles = capture_edge_roles.then(|| vec![false; ring.len().saturating_sub(1)]);
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
                .map(|point| RebuiltVertex {
                    xy: point.xy(),
                    point: Some(point),
                    node: None,
                })
                .collect();
            let roles = capture_edge_roles.then(|| vec![false; ring.len().saturating_sub(1)]);
            return Ok((vertices, roles));
        }
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
            let source = source_spans.get(ordinal as usize).ok_or_else(|| {
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
                    resolve_node(
                        inventory,
                        partition,
                        node,
                        root_axes,
                        source_context,
                        node_ordinates,
                    )?;
                    out.push(RebuiltVertex {
                        xy: point,
                        point: None,
                        node: Some(node),
                    });
                } else {
                    let point = source_point(point);
                    out.push(RebuiltVertex {
                        xy: point.xy(),
                        point: Some(point),
                        node: None,
                    });
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
    let mut axes = shape.axes();
    for vertex in rebuilt
        .iter()
        .flat_map(|polygon| polygon.rings.iter().flatten())
    {
        let (z, m) = rebuilt_vertex_ordinates(vertex, node_ordinates)?;
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
            node_ordinates,
        )?;
        let holes = rings
            .map(|ring| build_ring(ring, axes, node_ordinates))
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

fn resolve_node(
    inventory: &ExactSourceInventory,
    partition: &ExactPartition<'_>,
    node: NodeId,
    axes: crate::geometry::CoordinateAxes,
    context: &super::SourceArcContext<'_, '_, crate::crs::GeodesicMetric>,
    resolved: &mut [Option<crate::geometry::NodeOrdinates>],
) -> Result<()> {
    let slot = resolved.get_mut(node.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("coverage rebuild references an unknown source node".into())
    })?;
    if slot.is_none() {
        *slot = Some(inventory.resolve_node_ordinates(partition, node, axes, context, None)?);
    }
    Ok(())
}

fn rebuilt_vertex_ordinates(
    vertex: &RebuiltVertex,
    node_ordinates: &[Option<crate::geometry::NodeOrdinates>],
) -> Result<(Option<f64>, Option<f64>)> {
    if let Some(node) = vertex.node {
        let ordinates = node_ordinates
            .get(node.0 as usize)
            .copied()
            .flatten()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "coverage rebuild lost a retained source-node occurrence".into(),
                )
            })?;
        Ok((ordinates.z.value(), ordinates.m.value()))
    } else {
        let point = vertex
            .point
            .expect("non-root rebuilt vertex retains its original point");
        Ok((point.z(), point.m()))
    }
}

fn build_ring(
    vertices: Vec<RebuiltVertex>,
    axes: crate::geometry::CoordinateAxes,
    node_ordinates: &[Option<crate::geometry::NodeOrdinates>],
) -> Result<Ring> {
    let mut points = Vec::with_capacity(vertices.len());
    for vertex in vertices {
        let (z, m) = rebuilt_vertex_ordinates(&vertex, node_ordinates)?;
        points.push(Point::new_raw_axes(
            vertex.xy.x,
            vertex.xy.y,
            crate::geometry::ZOrdinate(
                axes.has_z()
                    .then(|| z.expect("retained Z is present on every output vertex")),
            ),
            crate::geometry::MOrdinate(
                axes.has_m()
                    .then(|| m.expect("retained M is present on every output vertex")),
            ),
        ));
    }
    Ring::try_from_closed(CoordSeq::try_from_points(&points)?)
}

fn rebuild_polygon(
    polygon: &Polygon,
    rebuild_ring: &mut impl FnMut(&CoordSeq) -> Result<(Vec<RebuiltVertex>, Option<Vec<bool>>)>,
) -> Result<(RebuiltPolygon, Option<Vec<Vec<bool>>>)> {
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
