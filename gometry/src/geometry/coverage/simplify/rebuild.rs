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
) -> Result<(Vec<XY>, Option<Vec<bool>>)> {
    let run_nodes = source_run_nodes(partition, run)?;
    let indices = chain_run_indices(chain, run, &run_nodes).ok_or_else(|| {
        GeometryErrorKind::Invalid("coverage source run is absent from its exact chain".into())
    })?;
    let mut replacement = Vec::with_capacity(indices.len());
    let mut retained_indices = capture_edge_roles.then(|| Vec::with_capacity(indices.len()));
    for (position, &index) in indices.iter().enumerate() {
        if position == indices.len().wrapping_sub(1) || simplified.kept.contains(&index) {
            replacement.push(
                *chain
                    .points
                    .get(index)
                    .expect("simplified chain index is within its point sequence"),
            );
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

/// Rebuild one row's polygons by splicing simplified chains into each ring.
pub(super) fn rebuild_row(
    shape: &Shape,
    source_cursor: &mut usize,
    source_segments: &[Segment],
    source_spans: &[Vec<(u32, bool)>],
    chain_of_span: &[u32],
    chains: &[Chain],
    simplified: &[SimplifiedChain],
    partition: &ExactPartition<'_>,
    capture_edge_roles: bool,
) -> Result<(Shape, Option<crate::geometry::PolygonPartEdgeRoles>)> {
    let mut rebuild_ring = |ring: &CoordSeq| -> Result<(Vec<Point>, Option<Vec<bool>>)> {
        let original: Vec<Point> = ring.iter_coords().collect();
        let source_ordinals = source_ordinals_for_ring(ring, source_segments, source_cursor)?;
        if original.len() < 4 {
            let roles = capture_edge_roles.then(|| vec![false; original.len().saturating_sub(1)]);
            return Ok((original, roles));
        }
        // Match collect_polygon_segments: elide stutter vertices so chain
        // lookups never probe zero-length edges absent from the exact partition.
        let (_, open_ring) = original
            .split_last()
            .expect("ring with at least four coordinates is non-empty");
        let mut open = open_ring.to_vec();
        dedup_consecutive_points(&mut open);
        if open.len() < 3 {
            let roles = capture_edge_roles.then(|| vec![false; original.len().saturating_sub(1)]);
            return Ok((original, roles));
        }
        let source_point_by_key: HashMap<PointKey, Point> = open
            .iter()
            .copied()
            .map(|point| (PointKey::new(point), point))
            .collect();
        let source_point = |point: XY| {
            source_point_by_key
                .get(&PointKey::new(point))
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
        let mut out: Vec<Point> = Vec::with_capacity(count.wrapping_add(1));
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
            for &point in replacement.iter().take(replacement.len().saturating_sub(1)) {
                out.push(source_point(point));
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
    match shape {
        Shape::Polygon(polygon) => {
            let (polygon, roles) = rebuild_polygon(polygon, &mut rebuild_ring)?;
            Ok((Shape::Polygon(polygon), roles.map(|roles| vec![roles])))
        },
        Shape::MultiPolygon(polygons) => {
            let mut rebuilt = Vec::with_capacity(polygons.len());
            let mut polygon_roles = capture_edge_roles.then(|| Vec::with_capacity(polygons.len()));
            for polygon in polygons {
                let (polygon, roles) = rebuild_polygon(polygon, &mut rebuild_ring)?;
                rebuilt.push(polygon);
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
            }
            Ok((
                Shape::multi_polygon(polygons.axes(), rebuilt),
                polygon_roles,
            ))
        },
        Shape::Empty(..)
        | Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::GeometryCollection(_) => Ok((shape.clone(), capture_edge_roles.then(Vec::new))),
    }
}

fn rebuild_polygon(
    polygon: &Polygon,
    rebuild_ring: &mut impl FnMut(&CoordSeq) -> Result<(Vec<Point>, Option<Vec<bool>>)>,
) -> Result<(Polygon, Option<Vec<Vec<bool>>>)> {
    let (shell, shell_roles) = rebuild_ring(polygon.shell.coords())?;
    let capture_edge_roles = shell_roles.is_some();
    let shell = Ring::try_from_closed(CoordSeq::try_from_points(&shell)?)?;
    let mut holes = Vec::with_capacity(polygon.holes.len());
    let mut roles = Vec::with_capacity(polygon.holes.len().wrapping_add(1));
    if let Some(shell_roles) = shell_roles {
        roles.push(shell_roles);
    }
    for hole in polygon.holes.iter() {
        let (points, hole_roles) = rebuild_ring(hole.coords())?;
        holes.push(Ring::try_from_closed(CoordSeq::try_from_points(&points)?)?);
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
    let edge_roles = capture_edge_roles.then_some(roles);
    Ok((Polygon::new(shell, holes), edge_roles))
}
