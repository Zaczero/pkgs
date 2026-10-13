use super::{
    BvhNode, Facet, FacetBvh, HeapSize, Point, PreparedLinework, Segment, SquaredDistanceKey,
    aabb_distance_squared_lower, axis_pow2_scale, clearance_aabb_filter, clearance_strip_exceeds,
    facet_for_leaf, point_aabb, point_pair_distance_key, point_segment_exact_distance_key,
    scaled_residual, segment_aabb, source_dot_cmp,
};
#[cfg(test)]
use super::{
    CLEARANCE_BVH_FACETS, CLEARANCE_BVH_NODES, CLEARANCE_BVH_SEGMENTS,
    CLEARANCE_STRIP_BUILD_VERTICES, CLEARANCE_STRIP_TESTS, NEAREST_WORK,
};

impl ClearanceFacetBvh {
    /// Nearest positive eligible source segment by exact source distance.
    /// AABBs reject first; source-supported strips resolve radial box slack.
    pub(crate) fn nearest_clearance_segment_with_stack_and_ordinal(
        &self,
        linework: &PreparedLinework,
        probe: Point,
        keep: &impl Fn(Point, usize, Segment) -> bool,
        incumbent: Option<&SquaredDistanceKey>,
        stack: &mut Vec<u32>,
    ) -> crate::error::Result<Option<(usize, SquaredDistanceKey)>> {
        let mut best: Option<(usize, SquaredDistanceKey)> = None;
        let probe_aabb = point_aabb(probe.x, probe.y);
        let lower_bound = |aabb: [f64; 4]| aabb_distance_squared_lower(aabb, probe_aabb);
        stack.clear();
        stack.push(0);
        while let Some(index) = stack.pop() {
            #[cfg(test)]
            {
                NEAREST_WORK.with(|work| work.set(work.get().saturating_add(1)));
                CLEARANCE_BVH_NODES.with(|work| work.set(work.get().saturating_add(1)));
            }
            let local = best.as_ref().map(|(_, key)| key);
            let limit = clearance_segment_limit(local, incumbent)?;
            if let Some(limit) = limit {
                let (witness, filter) =
                    clearance_aabb_filter(probe.xy(), self.tree.node_aabb(index));
                if filter.cmp_witness_key(witness, limit)?.is_gt() {
                    continue;
                }
            }
            let node = self.tree.decode_node(index);
            if let Some(limit) = limit {
                #[cfg(test)]
                CLEARANCE_STRIP_TESTS.with(|work| work.set(work.get().saturating_add(1)));
                let strip = *self
                    .strips
                    .get(index as usize)
                    .expect("clearance BVH node has its source-supported strip");
                if clearance_strip_exceeds(
                    probe.xy(),
                    strip.normal,
                    linework
                        .vertex_at_chain_local(strip.low_support.chain, strip.low_support.local)
                        .xy(),
                    linework
                        .vertex_at_chain_local(strip.high_support.chain, strip.high_support.local)
                        .xy(),
                    limit,
                )? {
                    continue;
                }
            }
            match node {
                BvhNode::Leaf(facet_index) => {
                    let facet = facet_for_leaf(linework, facet_index);
                    #[cfg(test)]
                    CLEARANCE_BVH_FACETS.with(|work| work.set(work.get().saturating_add(1)));
                    for offset in 0..facet.segment_count as usize {
                        #[cfg(test)]
                        {
                            NEAREST_WORK.with(|work| work.set(work.get().saturating_add(1)));
                            CLEARANCE_BVH_SEGMENTS
                                .with(|work| work.set(work.get().saturating_add(1)));
                        }
                        let segment = linework.segment(facet, offset);
                        let ordinal = linework.segment_ordinal(facet, offset);
                        if !keep(probe, ordinal, segment) {
                            continue;
                        }
                        if let Some(limit) =
                            clearance_segment_limit(best.as_ref().map(|(_, key)| key), incumbent)?
                        {
                            let (witness, filter) =
                                clearance_aabb_filter(probe.xy(), segment_aabb(segment));
                            if filter.cmp_witness_key(witness, limit)?.is_gt() {
                                continue;
                            }
                        }
                        let key = point_segment_exact_distance_key(probe.xy(), segment);
                        if key.is_exact_zero()? {
                            continue;
                        }
                        let replace = match best.as_ref() {
                            None => true,
                            Some((best_ordinal, best_key)) => {
                                let order = key.cmp_distance(best_key)?;
                                order.is_lt() || order.is_eq() && ordinal < *best_ordinal
                            },
                        };
                        if replace {
                            best = Some((ordinal, key));
                        }
                    }
                },
                BvhNode::Internal(first_child) => {
                    self.tree
                        .push_nearer_last(stack, first_child as u32, &lower_bound);
                },
            }
        }
        Ok(best)
    }
}

#[derive(Clone, Copy)]
pub(super) struct ClearanceStrip {
    pub(super) normal: [f64; 2],
    pub(super) low_support: ClearanceSupport,
    pub(super) high_support: ClearanceSupport,
}

#[derive(Clone, Copy)]
pub(super) struct ClearanceSupport {
    pub(super) chain: u32,
    pub(super) local: u32,
}

pub(crate) struct ClearanceFacetBvh {
    pub(super) tree: FacetBvh,
    pub(super) strips: Box<[ClearanceStrip]>,
}

fn clearance_normal_for_facet(
    linework: &PreparedLinework,
    facet: Facet,
) -> crate::error::Result<[f64; 2]> {
    // The normal selects a projection direction; source supports, not an exact
    // perpendicular or a unit-length assumption, define the bound.
    let mut longest: Option<(Segment, SquaredDistanceKey)> = None;
    for offset in 0..facet.segment_count as usize {
        let segment = linework.segment(facet, offset);
        let key = point_pair_distance_key(segment.start, segment.end);
        let replace = match longest.as_ref() {
            None => true,
            Some((_, incumbent)) => key.cmp_distance(incumbent)?.is_gt(),
        };
        if replace {
            longest = Some((segment, key));
        }
    }
    let Some((segment, _)) = longest else {
        return Ok([1.0, 0.0]);
    };
    let mut dx = segment.end.x - segment.start.x;
    let mut dy = segment.end.y - segment.start.y;
    if !dx.is_finite() || !dy.is_finite() {
        let scale = axis_pow2_scale(
            segment
                .start
                .x
                .abs()
                .max(segment.start.y.abs())
                .max(segment.end.x.abs())
                .max(segment.end.y.abs()),
        );
        dx = scaled_residual(segment.end.x, segment.start.x, scale);
        dy = scaled_residual(segment.end.y, segment.start.y, scale);
    }
    let magnitude = dx.abs().max(dy.abs());
    if magnitude == 0.0 || !magnitude.is_finite() {
        return Ok([1.0, 0.0]);
    }
    let [mut normal_x, mut normal_y] = [-dy / magnitude, dx / magnitude];
    if normal_y.total_cmp(&0.0).is_lt()
        || normal_y.total_cmp(&0.0).is_eq() && normal_x.total_cmp(&0.0).is_lt()
    {
        normal_x = -normal_x;
        normal_y = -normal_y;
    }
    Ok([normal_x, normal_y])
}

pub(super) fn clearance_strip_for_facets(
    linework: &PreparedLinework,
    ids: &[u32],
) -> crate::error::Result<ClearanceStrip> {
    let representative = *ids
        .get(ids.len() / 2)
        .expect("clearance node facet range is nonempty");
    let normal =
        clearance_normal_for_facet(linework, facet_for_leaf(linework, representative as usize))?;
    let mut low: Option<(ClearanceSupport, Point)> = None;
    let mut high: Option<(ClearanceSupport, Point)> = None;
    for &id in ids {
        let facet = facet_for_leaf(linework, id as usize);
        for local in 0..=facet.segment_count as usize {
            #[cfg(test)]
            CLEARANCE_STRIP_BUILD_VERTICES.with(|count| {
                count.set(count.get().saturating_add(1));
            });
            let local = facet
                .first_vertex
                .checked_add(u32::try_from(local).expect("facet-local vertex index fits u32"))
                .expect("facet-local support vertex belongs to its u32 chain");
            let support = ClearanceSupport {
                chain: facet.chain,
                local,
            };
            let point = linework.vertex_at_chain_local(support.chain, support.local);
            match low {
                None => low = Some((support, point)),
                Some((_, source)) => {
                    if source_dot_cmp(normal, point.xy(), source.xy())?.is_lt() {
                        low = Some((support, point));
                    }
                },
            }
            match high {
                None => high = Some((support, point)),
                Some((_, source)) => {
                    if source_dot_cmp(normal, point.xy(), source.xy())?.is_gt() {
                        high = Some((support, point));
                    }
                },
            }
        }
    }
    let (low_support, _) = low.expect("clearance node contains a facet vertex");
    let (high_support, _) = high.expect("clearance node contains a facet vertex");
    Ok(ClearanceStrip {
        normal,
        low_support,
        high_support,
    })
}

fn clearance_segment_limit<'a>(
    local: Option<&'a SquaredDistanceKey>,
    global: Option<&'a SquaredDistanceKey>,
) -> crate::error::Result<Option<&'a SquaredDistanceKey>> {
    match (local, global) {
        (None, None) => Ok(None),
        (Some(local), None) => Ok(Some(local)),
        (None, Some(global)) => Ok(Some(global)),
        (Some(local), Some(global)) => {
            if local.cmp_distance(global)?.is_lt() {
                Ok(Some(local))
            } else {
                Ok(Some(global))
            }
        },
    }
}

impl HeapSize for ClearanceFacetBvh {
    fn heap_bytes(&self) -> usize {
        self.tree.heap_bytes().saturating_add(
            self.strips
                .len()
                .saturating_mul(std::mem::size_of::<ClearanceStrip>()),
        )
    }
}
