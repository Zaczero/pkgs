use crate::HeapSize;
use crate::error::Result;
use crate::geometry::distance::ChartEdgePart;
use crate::geometry::{BvhCode, BvhNode, ChartBox, ChartMetric, Point};

const BVH_LEAF_SIZE: usize = 8;

fn balanced_leaf_count(entry_count: usize) -> usize {
    if entry_count <= BVH_LEAF_SIZE {
        return 1;
    }
    let quotient = entry_count / BVH_LEAF_SIZE;
    let base = 1_usize << quotient.ilog2();
    let complete_level = base.wrapping_mul(2);
    let partial_level = base
        .wrapping_add(entry_count)
        .wrapping_sub(BVH_LEAF_SIZE.wrapping_mul(base));
    complete_level.min(partial_level)
}

#[derive(Clone, Copy)]
struct ChartEdgeBuild {
    index: u32,
    center: [f64; 2],
}

#[derive(Clone, Copy)]
struct ChartEdgeLeaf {
    first: u32,
    len: u8,
}
crate::heapless!(ChartEdgeLeaf);

/// Flat BVH over the chart boxes of one prepared geometry.
pub(crate) struct ChartEdgeBvh {
    boxes: Box<[ChartBox]>,
    codes: Box<[u32]>,
    leaves: Box<[ChartEdgeLeaf]>,
    edge_ids: Box<[u32]>,
}

impl ChartEdgeBvh {
    pub(crate) fn build(edges: &[ChartEdgePart]) -> Option<Self> {
        let edge_count = u32::try_from(edges.len()).ok()?;
        if edge_count == 0 || edge_count > BvhCode::LEAF_BIT / 2 {
            return None;
        }
        let leaf_count = balanced_leaf_count(edges.len());
        let node_count = leaf_count.checked_mul(2)?.checked_sub(1)?;
        let world_box = ChartBox {
            west: -180.0,
            south: -90.0,
            east: 180.0,
            north: 90.0,
        };
        let mut tree = Self {
            boxes: vec![world_box; node_count].into_boxed_slice(),
            codes: vec![0; node_count].into_boxed_slice(),
            leaves: vec![ChartEdgeLeaf { first: 0, len: 0 }; leaf_count].into_boxed_slice(),
            edge_ids: vec![0; edges.len()].into_boxed_slice(),
        };
        let mut entries = edges
            .iter()
            .enumerate()
            .map(|(index, edge)| ChartEdgeBuild {
                index: index as u32,
                center: chart_box_center(edge.bounds),
            })
            .collect::<Vec<_>>();
        let mut next_node = 1;
        let mut next_leaf = 0;
        let mut next_edge = 0;
        tree.fill_node(
            0,
            &mut next_node,
            &mut next_leaf,
            &mut next_edge,
            edges,
            &mut entries,
        );
        debug_assert_eq!(next_node, node_count);
        debug_assert_eq!(next_leaf, leaf_count);
        debug_assert_eq!(next_edge, edges.len());
        Some(tree)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "node, leaf, and edge indexes are generated together within the admitted u32-sized chart BVH"
    )]
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "the admitted node count bounds adjacent child slots and cursor increments"
    )]
    fn fill_node(
        &mut self,
        node: usize,
        next_node: &mut usize,
        next_leaf: &mut usize,
        next_edge: &mut usize,
        edges: &[ChartEdgePart],
        entries: &mut [ChartEdgeBuild],
    ) {
        self.boxes[node] = chart_box_union(
            entries
                .iter()
                .map(|entry| edges[entry.index as usize].bounds),
        );
        if entries.len() <= BVH_LEAF_SIZE {
            entries.sort_unstable_by_key(|entry| entry.index);
            let first = *next_edge as u32;
            for entry in entries.iter() {
                self.edge_ids[*next_edge] = entry.index;
                *next_edge = (*next_edge).wrapping_add(1);
            }
            let leaf_index = *next_leaf as u32;
            self.leaves[*next_leaf] = ChartEdgeLeaf {
                first,
                len: entries.len() as u8,
            };
            *next_leaf = (*next_leaf).wrapping_add(1);
            self.codes[node] = BvhCode::leaf(leaf_index as usize).raw();
            return;
        }

        let axis = chart_split_axis(self.boxes[node]);
        let middle = entries.len() / 2;
        entries.select_nth_unstable_by(middle, |left, right| {
            left.center[axis]
                .total_cmp(&right.center[axis])
                .then_with(|| left.index.cmp(&right.index))
        });
        let (lower, upper) = entries.split_at_mut(middle);
        let first_child = *next_node;
        *next_node = (*next_node).wrapping_add(2);
        self.codes[node] = BvhCode::internal(first_child as u32).raw();
        self.fill_node(first_child, next_node, next_leaf, next_edge, edges, lower);
        self.fill_node(
            first_child + 1,
            next_node,
            next_leaf,
            next_edge,
            edges,
            upper,
        );
    }

    /// Visit candidate edges from the nearer chart boxes first. `limit` is
    /// inclusive for threshold queries and an improving upper bound for
    /// minima; the visitor may lower `best` or stop with a value.
    #[expect(
        clippy::indexing_slicing,
        reason = "Traversal IDs and leaf ranges are encoded by this immutable chart BVH builder"
    )]
    pub(crate) fn visit_point_candidates<T>(
        &self,
        edges: &[ChartEdgePart],
        point: Point,
        metric: &impl ChartMetric,
        best: &mut f64,
        limit: Option<f64>,
        stack: &mut Vec<(u32, f64)>,
        mut visit: impl FnMut(usize, f64, &mut f64) -> Result<Option<T>>,
    ) -> Result<Option<T>> {
        stack.clear();
        let bound = metric.point_envelope_bound(point);
        let root_bound = bound(self.boxes[0]);
        if chart_bound_prunes(root_bound, *best, limit) {
            return Ok(None);
        }
        stack.push((0, root_bound));
        while let Some((node, lower_bound)) = stack.pop() {
            if chart_bound_prunes(lower_bound, *best, limit) {
                continue;
            }
            match BvhCode::new(self.codes[node as usize]).decode() {
                BvhNode::Leaf(leaf_index) => {
                    let leaf = self.leaves[leaf_index];
                    let Some(edge_ids) = self
                        .edge_ids
                        .get(leaf.first as usize..)
                        .and_then(|remaining| remaining.get(..leaf.len as usize))
                    else {
                        unreachable!("chart BVH leaf range belongs to its edge-id table")
                    };
                    for &edge_id in edge_ids {
                        let edge_index = edge_id as usize;
                        let edge_bound = bound(edges[edge_index].bounds);
                        if chart_bound_prunes(edge_bound, *best, limit) {
                            continue;
                        }
                        if let Some(result) = visit(edge_index, edge_bound, best)? {
                            return Ok(Some(result));
                        }
                    }
                },
                BvhNode::Internal(first_child) => {
                    let lower_child = first_child as u32;
                    let upper_child = lower_child.wrapping_add(1);
                    let lower_bound = bound(self.boxes[lower_child as usize]);
                    let upper_bound = bound(self.boxes[upper_child as usize]);
                    let lower_admitted = !chart_bound_prunes(lower_bound, *best, limit);
                    let upper_admitted = !chart_bound_prunes(upper_bound, *best, limit);
                    match (lower_admitted, upper_admitted) {
                        (true, true) if lower_bound <= upper_bound => {
                            stack.push((upper_child, upper_bound));
                            stack.push((lower_child, lower_bound));
                        },
                        (true, true) => {
                            stack.push((lower_child, lower_bound));
                            stack.push((upper_child, upper_bound));
                        },
                        (true, false) => stack.push((lower_child, lower_bound)),
                        (false, true) => stack.push((upper_child, upper_bound)),
                        (false, false) => {},
                    }
                },
            }
        }
        Ok(None)
    }
}

impl HeapSize for ChartEdgeBvh {
    fn heap_bytes(&self) -> usize {
        self.boxes
            .heap_bytes()
            .saturating_add(self.codes.heap_bytes())
            .saturating_add(self.leaves.heap_bytes())
            .saturating_add(self.edge_ids.heap_bytes())
    }
}

fn chart_bound_prunes(lower_bound: f64, best: f64, limit: Option<f64>) -> bool {
    limit.map_or_else(|| lower_bound > best.next_up(), |limit| lower_bound > limit)
}

#[expect(
    clippy::float_cmp,
    reason = "the exact (-180, 180) endpoints encode the canonical full-longitude sentinel"
)]
fn chart_box_union(boxes: impl Iterator<Item = ChartBox>) -> ChartBox {
    let mut south = f64::INFINITY;
    let mut north = f64::NEG_INFINITY;
    let mut west = 180.0_f64;
    let mut east = -180.0_f64;
    let mut full_longitude = false;
    for bounds in boxes {
        south = south.min(bounds.south);
        north = north.max(bounds.north);
        if bounds.west > bounds.east || (bounds.west == -180.0 && bounds.east == 180.0) {
            full_longitude = true;
        } else {
            west = west.min(bounds.west);
            east = east.max(bounds.east);
        }
    }
    if full_longitude {
        ChartBox {
            west: -180.0,
            south,
            east: 180.0,
            north,
        }
    } else {
        ChartBox {
            west,
            south,
            east,
            north,
        }
    }
}

#[expect(
    clippy::float_cmp,
    reason = "the exact (-180, 180) endpoints encode the canonical full-longitude sentinel"
)]
fn chart_box_center(bounds: ChartBox) -> [f64; 2] {
    let longitude = if bounds.west == -180.0 && bounds.east == 180.0 {
        0.0
    } else if bounds.west <= bounds.east {
        f64::midpoint(bounds.west, bounds.east)
    } else {
        let sweep = (180.0 - bounds.west) + (bounds.east + 180.0);
        let unwrapped = bounds.west + sweep / 2.0;
        if unwrapped > 180.0 {
            unwrapped - 360.0
        } else {
            unwrapped
        }
    };
    [longitude, f64::midpoint(bounds.south, bounds.north)]
}

#[expect(
    clippy::float_cmp,
    reason = "the exact (-180, 180) endpoints encode the canonical full-longitude sentinel"
)]
fn chart_longitude_span(bounds: ChartBox) -> f64 {
    if bounds.west == -180.0 && bounds.east == 180.0 {
        360.0
    } else if bounds.west <= bounds.east {
        bounds.east - bounds.west
    } else {
        (180.0 - bounds.west) + (bounds.east + 180.0)
    }
}

fn chart_split_axis(bounds: ChartBox) -> usize {
    usize::from(chart_longitude_span(bounds) < bounds.north - bounds.south)
}

#[cfg(test)]
mod tests {
    use super::balanced_leaf_count;

    #[test]
    fn balanced_leaf_count_matches_split_boundaries() {
        for (segment_count, expected_leaves) in [
            (1, 1),
            (8, 1),
            (9, 2),
            (16, 2),
            (17, 3),
            (18, 4),
            (23, 4),
            (32, 4),
            (33, 5),
            (34, 6),
            (35, 7),
            (36, 8),
            (64, 8),
            (65, 9),
            (71, 15),
            (72, 16),
            (95, 16),
            (128, 16),
            (129, 17),
            (144, 32),
            (256, 32),
            (257, 33),
            (512, 64),
            (513, 65),
        ] {
            let leaves = balanced_leaf_count(segment_count);
            assert_eq!(leaves, expected_leaves, "segments={segment_count}");
        }
    }
}
