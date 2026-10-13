use std::ops::ControlFlow;

use crate::HeapSize;
use crate::geometry::facet_bvh::{
    Facet, PreparedLinework, aabb_distance, aabb_distance_squared, aabb_distance_squared_lower,
    aabb_max_distance_squared, aabbs_overlap, point_aabb, segment_aabb, union_aabb,
};
use crate::geometry::segments::projection::segment_projection;
use crate::geometry::{
    BvhCode, BvhNode, CoordinateAxes, Point, Segment, SegmentProjection, SquaredDistanceKey,
    axis_pow2_scale, clearance_aabb_filter, clearance_strip_exceeds, point_on_segment,
    point_pair_distance_key, point_segment_distance_squared, point_segment_exact_distance_key,
    point_segment_selection, scaled_residual, segments_intersect, source_dot_cmp,
};

#[cfg(test)]
std::thread_local! {
    pub(crate) static NEAREST_WORK: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static CLEARANCE_BVH_NODES: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static CLEARANCE_BVH_SEGMENTS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static CLEARANCE_BVH_FACETS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static CLEARANCE_STRIP_TESTS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static CLEARANCE_STRIP_BUILD_VERTICES: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

/// One nearest-pair candidate. Source segments retain their ordinal until
/// the final witness is emitted with full source ordinates.
pub(crate) struct NearestCandidate {
    pub(in crate::geometry) probe: Point,
    pub(in crate::geometry) target: NearestTarget,
    pub(in crate::geometry) distance_key: SquaredDistanceKey,
}

/// The exact selected projection and its directed source edge, retained until
/// the witness ordinates are resolved.
pub(crate) struct NearestSourceProjection {
    pub(crate) start: Point,
    pub(crate) end: Point,
    pub(crate) projection: SegmentProjection,
}

pub(crate) enum NearestTarget {
    Segment(usize),
    Point(Point),
}

/// Expand the shared exact candidate rule in facet and streaming loops, avoiding
/// a call per segment. Ordinal mapping is deferred until a tie or a win; `?`
/// propagates exact-selection errors in the caller.
macro_rules! update_nearest_candidate {
    ($selection:expr, $probe:expr, $segment:expr, $ordinal:expr, $best:expr $(,)?) => {{
        let selection = $selection;
        let probe = $probe;
        let segment = $segment;
        let segment_ordinal = $ordinal;
        let best = &mut *($best);
        let closer = match best.as_ref() {
            None => true,
            Some(candidate) => {
                selection.precedes(probe.xy(), segment, &candidate.distance_key, || {
                    candidate
                        .segment_ordinal()
                        .is_some_and(|best| segment_ordinal() < best)
                })?
            },
        };
        if closer {
            *best = Some($crate::geometry::NearestCandidate {
                probe,
                target: $crate::geometry::NearestTarget::Segment(segment_ordinal()),
                distance_key: selection.distance_key(probe.xy(), segment),
            });
        }
        $crate::error::Result::<bool>::Ok(closer)
    }};
}
pub(crate) use update_nearest_candidate;

impl NearestCandidate {
    pub(crate) const fn segment_ordinal(&self) -> Option<usize> {
        match self.target {
            NearestTarget::Segment(ordinal) => Some(ordinal),
            NearestTarget::Point(_) => None,
        }
    }

    pub(crate) fn target_point<const SOURCE_CONTEXT: bool>(
        &self,
        linework: &PreparedLinework,
        source_axes: CoordinateAxes,
    ) -> crate::error::Result<(Point, Option<NearestSourceProjection>)> {
        match self.target {
            NearestTarget::Point(point) => Ok((point, None)),
            NearestTarget::Segment(ordinal) => {
                let (start, end) = linework.vertex_pair_at_segment(ordinal);
                let segment = Segment {
                    start: start.xy(),
                    end: end.xy(),
                };
                let projection = segment_projection(self.probe, segment)?;
                let point = projection.interpolate_point(start, end)?;
                let source = if SOURCE_CONTEXT {
                    let has_source_ordinates =
                        (source_axes.has_z() && start.z().is_some() && end.z().is_some())
                            || (source_axes.has_m() && start.m().is_some() && end.m().is_some());
                    (has_source_ordinates && !projection.is_start() && !projection.is_end())
                        .then_some(NearestSourceProjection {
                            start,
                            end,
                            projection,
                        })
                } else {
                    None
                };
                Ok((point, source))
            },
        }
    }
}

/// Scalar-refine one facet's exact selection keys, deferring projection
/// and ordinate interpolation until the selected witness is emitted.
pub(super) fn refine_facet_nearest<const BOUNDED: bool>(
    linework: &PreparedLinework,
    facet: Facet,
    probe: Point,
    upper_bound: f64,
    tolerance: f64,
    keep: &impl Fn(Point, Segment) -> bool,
    best: &mut Option<NearestCandidate>,
) -> crate::error::Result<bool> {
    refine_facet_nearest_with_ordinal::<BOUNDED>(
        linework,
        facet,
        probe,
        upper_bound,
        tolerance,
        &|probe, _, segment| keep(probe, segment),
        best,
    )
}

pub(super) fn refine_facet_nearest_with_ordinal<const BOUNDED: bool>(
    linework: &PreparedLinework,
    facet: Facet,
    probe: Point,
    upper_bound: f64,
    tolerance: f64,
    keep: &impl Fn(Point, usize, Segment) -> bool,
    best: &mut Option<NearestCandidate>,
) -> crate::error::Result<bool> {
    let mut changed = false;
    for index in 0..facet.segment_count as usize {
        #[cfg(test)]
        NEAREST_WORK.with(|work| work.set(work.get().saturating_add(1)));
        let segment = linework.segment(facet, index);
        let ordinal = linework.segment_ordinal(facet, index);
        if !keep(probe, ordinal, segment) {
            continue;
        }
        let selection = point_segment_selection(probe.xy(), segment)?;
        if BOUNDED && selection.distance_squared > upper_bound {
            continue;
        }
        if BOUNDED
            && (selection.distance() > tolerance
                || tolerance == 0.0
                    && !selection
                        .distance_key(probe.xy(), segment)
                        .is_exact_zero()?)
        {
            continue;
        }
        changed |= update_nearest_candidate!(&selection, probe, segment, || ordinal, best,)?;
    }
    Ok(changed)
}

/// Flat binary AABB tree over a [`PreparedLinework`]'s facets,
/// longest-axis-median built. `2F − 1` nodes for `F` facets, root at 0.
///
/// Layout is traversal-driven: the AABBs live in their own dense array
/// (32 B each — the only bytes the pruned majority of node visits touch,
/// two per cache line with no straddle), the per-node code words in a
/// second; and a node's two children are ALLOCATED ADJACENTLY, so the
/// nearer-child ordering reads both child bounds from neighboring memory
/// instead of two distant tree regions.
pub(crate) struct FacetBvh {
    aabbs: Box<[[f64; 4]]>,
    codes: Box<[u32]>,
}

#[derive(Clone, Copy)]
struct ClearanceStrip {
    normal: [f64; 2],
    low_support: ClearanceSupport,
    high_support: ClearanceSupport,
}

#[derive(Clone, Copy)]
struct ClearanceSupport {
    chain: u32,
    local: u32,
}

pub(crate) struct ClearanceFacetBvh {
    tree: FacetBvh,
    strips: Box<[ClearanceStrip]>,
}

#[expect(
    clippy::indexing_slicing,
    reason = "facet IDs were admitted from the same facet_aabbs slice"
)]
fn partition_aabb(facet_aabbs: &[[f64; 4]], ids: &[u32]) -> [f64; 4] {
    ids.iter()
        .map(|&id| facet_aabbs[id as usize])
        .reduce(union_aabb)
        .expect("partition contains at least one facet")
}

fn finite_span(lower: f64, upper: f64) -> Option<f64> {
    if !lower.is_finite() || !upper.is_finite() || lower > upper {
        return None;
    }
    let span = upper - lower;
    Some(if span.is_finite() {
        span
    } else {
        upper * 0.5 - lower * 0.5
    })
}

fn finite_midpoint(lower: f64, upper: f64) -> f64 {
    if lower.is_finite()
        && upper.is_finite()
        && lower.is_sign_negative() == upper.is_sign_negative()
    {
        lower + (upper - lower) * 0.5
    } else {
        lower * 0.5 + upper * 0.5
    }
}

fn relative_span(child: f64, parent: f64) -> Option<f64> {
    if parent == 0.0 {
        (child == 0.0).then_some(0.0)
    } else {
        Some(child / parent)
    }
}

fn partition_cost(parent: [f64; 4], lower: [f64; 4], upper: [f64; 4]) -> Option<f64> {
    let [parent_min_x, parent_min_y, parent_max_x, parent_max_y] = parent;
    let [lower_min_x, lower_min_y, lower_max_x, lower_max_y] = lower;
    let [upper_min_x, upper_min_y, upper_max_x, upper_max_y] = upper;
    let parent_x = finite_span(parent_min_x, parent_max_x)?;
    let parent_y = finite_span(parent_min_y, parent_max_y)?;
    let lower_x = relative_span(finite_span(lower_min_x, lower_max_x)?, parent_x)?;
    let lower_y = relative_span(finite_span(lower_min_y, lower_max_y)?, parent_y)?;
    let upper_x = relative_span(finite_span(upper_min_x, upper_max_x)?, parent_x)?;
    let upper_y = relative_span(finite_span(upper_min_y, upper_max_y)?, parent_y)?;
    Some(lower_x + lower_y + upper_x + upper_y)
}

#[expect(
    clippy::indexing_slicing,
    reason = "facet IDs were admitted from the same facet_aabbs slice"
)]
fn partition_facets(
    aabb: [f64; 4],
    facet_aabbs: &[[f64; 4]],
    ids: &mut [u32],
    prefer_tighter_source_order: bool,
) -> usize {
    let [min_x, min_y, max_x, max_y] = aabb;
    let width = finite_span(min_x, max_x).unwrap_or(0.0);
    let height = finite_span(min_y, max_y).unwrap_or(0.0);
    let axis = usize::from(height > width);
    let center = |id: u32| {
        let [min_x, min_y, max_x, max_y] = facet_aabbs[id as usize];
        let (minimum, maximum) = if axis == 0 {
            (min_x, max_x)
        } else {
            (min_y, max_y)
        };
        finite_midpoint(minimum, maximum)
    };
    let mid = ids.len() / 2;
    ids.select_nth_unstable_by(mid, |&a, &b| {
        center(a).total_cmp(&center(b)).then_with(|| a.cmp(&b))
    });
    if !prefer_tighter_source_order {
        return mid;
    }
    // For clearance, source order is only a layout candidate: use it when its
    // normalized child extents tighten bounds more than the spatial median.
    let (lower, upper) = ids.split_at(mid);
    let spatial_cost = partition_cost(
        aabb,
        partition_aabb(facet_aabbs, lower),
        partition_aabb(facet_aabbs, upper),
    );
    ids.select_nth_unstable_by(mid, u32::cmp);
    let (lower, upper) = ids.split_at(mid);
    let source_cost = partition_cost(
        aabb,
        partition_aabb(facet_aabbs, lower),
        partition_aabb(facet_aabbs, upper),
    );
    if source_cost
        .zip(spatial_cost)
        .is_some_and(|(source, spatial)| source < spatial)
    {
        return mid;
    }
    ids.select_nth_unstable_by(mid, |&a, &b| {
        center(a).total_cmp(&center(b)).then_with(|| a.cmp(&b))
    });
    mid
}

fn facet_for_leaf(linework: &PreparedLinework, index: usize) -> Facet {
    *linework
        .facets
        .get(index)
        .expect("BVH leaf facet index is within its prepared linework")
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

fn clearance_strip_for_facets(
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

impl FacetBvh {
    /// Build over `linework`'s facets; `None` when it has none — or so many
    /// that a NODE index (`2F − 1` of them) could reach [`BvhCode::LEAF_BIT`] and a
    /// first-child code would read as a leaf (those astronomically large
    /// shapes keep the flat facet scans, which are index-width-agnostic).
    pub(crate) fn build(linework: &PreparedLinework) -> Option<Self> {
        let (mut tree, facet_aabbs, mut ids) = Self::build_parts(linework)?;
        let mut next = 1;
        tree.fill_node(0, &mut next, &facet_aabbs, &mut ids);
        Some(tree)
    }

    pub(crate) fn build_for_clearance(
        linework: &PreparedLinework,
    ) -> crate::error::Result<Option<ClearanceFacetBvh>> {
        let Some((mut tree, facet_aabbs, mut ids)) = Self::build_parts(linework) else {
            return Ok(None);
        };
        let mut strips = vec![
            ClearanceStrip {
                normal: [1.0, 0.0],
                low_support: ClearanceSupport { chain: 0, local: 0 },
                high_support: ClearanceSupport { chain: 0, local: 0 },
            };
            tree.aabbs.len()
        ];
        let mut next = 1;
        tree.fill_clearance_node(0, &mut next, &facet_aabbs, &mut ids, linework, &mut strips)?;
        debug_assert_eq!(next, tree.aabbs.len());
        Ok(Some(ClearanceFacetBvh {
            tree,
            strips: strips.into_boxed_slice(),
        }))
    }

    fn build_parts(linework: &PreparedLinework) -> Option<(Self, Vec<[f64; 4]>, Vec<u32>)> {
        let facet_count = u32::try_from(linework.facets.len()).ok()?;
        if facet_count == 0 || facet_count > BvhCode::LEAF_BIT / 2 {
            return None;
        }
        let facet_aabbs: Vec<[f64; 4]> = linework
            .facets
            .iter()
            .map(|&facet| linework.facet_aabb(facet))
            .collect();
        let ids: Vec<u32> = (0..facet_count).collect();
        let node_count = (facet_count as usize).wrapping_mul(2).wrapping_sub(1);
        let tree = Self {
            aabbs: vec![[0.0; 4]; node_count].into_boxed_slice(),
            codes: vec![0; node_count].into_boxed_slice(),
        };
        Some((tree, facet_aabbs, ids))
    }

    /// Median split of facet centers into the node slot `node`.
    #[expect(
        clippy::indexing_slicing,
        reason = "Facet ids are below facet_aabbs.len(); recursive node ids are builder-produced and below the allocated 2F-1 aabbs/codes lengths"
    )]
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "facet_count <= LEAF_BIT/2 keeps 2F-1 in usize; next advances only across sibling pairs within the allocated node_count"
    )]
    fn fill_node(
        &mut self,
        node: usize,
        next: &mut usize,
        facet_aabbs: &[[f64; 4]],
        ids: &mut [u32],
    ) {
        if let [id] = ids {
            self.aabbs[node] = facet_aabbs[*id as usize];
            self.codes[node] = BvhCode::leaf(*id as usize).raw();
            return;
        }
        let aabb = partition_aabb(facet_aabbs, ids);
        let mid = partition_facets(aabb, facet_aabbs, ids, false);
        let (lower, upper) = ids.split_at_mut(mid);
        let first_child = *next;
        *next += 2;
        self.aabbs[node] = aabb;
        self.codes[node] = BvhCode::internal(first_child as u32).raw();
        self.fill_node(first_child, next, facet_aabbs, lower);
        self.fill_node(first_child + 1, next, facet_aabbs, upper);
    }

    /// Spatial median split, preferring a tighter source-ordinal median where available.
    #[expect(
        clippy::indexing_slicing,
        reason = "Facet ids and recursive node ids are builder-produced and stay below the allocated bounds/codes"
    )]
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "facet_count <= LEAF_BIT/2 keeps 2F-1 in usize; next advances only across allocated sibling pairs"
    )]
    fn fill_clearance_node(
        &mut self,
        node: usize,
        next: &mut usize,
        facet_aabbs: &[[f64; 4]],
        ids: &mut [u32],
        linework: &PreparedLinework,
        strips: &mut [ClearanceStrip],
    ) -> crate::error::Result<()> {
        let aabb = partition_aabb(facet_aabbs, ids);
        self.aabbs[node] = aabb;
        strips[node] = clearance_strip_for_facets(linework, ids)?;
        if let [id] = ids {
            self.codes[node] = BvhCode::leaf(*id as usize).raw();
            return Ok(());
        }

        let mid = partition_facets(aabb, facet_aabbs, ids, true);
        let (lower, upper) = ids.split_at_mut(mid);
        let first_child = *next;
        *next += 2;
        self.codes[node] = BvhCode::internal(first_child as u32).raw();
        self.fill_clearance_node(first_child, next, facet_aabbs, lower, linework, strips)?;
        self.fill_clearance_node(first_child + 1, next, facet_aabbs, upper, linework, strips)
    }

    fn node_aabb(&self, index: u32) -> [f64; 4] {
        *self
            .aabbs
            .get(index as usize)
            .expect("BVH traversal node index is within the built tree")
    }

    fn decode_node(&self, index: u32) -> BvhNode {
        BvhCode::new(
            *self
                .codes
                .get(index as usize)
                .expect("BVH traversal node index is within the built tree"),
        )
        .decode()
    }

    /// Minimum squared (or `hypot`) distance from a probe vertex set to the
    /// linework — one branch-and-bound descent per probe over a single
    /// shared traversal stack, carrying `best` across the whole sweep so
    /// every later probe prunes on what earlier ones learned. `SQUARED`
    /// selects squared-space bounds + the SIMD facet kernel; `false` runs
    /// overflow-safe `hypot` space throughout.
    pub(crate) fn min_points_distance<const SQUARED: bool>(
        &self,
        linework: &PreparedLinework,
        probes: impl Iterator<Item = (f64, f64)>,
        mut best: f64,
    ) -> crate::error::Result<f64> {
        let mut stack: Vec<u32> = Vec::new();
        for (x, y) in probes {
            best =
                self.min_point_distance_with_stack::<SQUARED>(linework, x, y, &mut stack, best)?;
        }
        Ok(best)
    }

    /// One probe's branch-and-bound descent over a caller-provided (reused)
    /// stack — the per-row engine behind
    /// [`min_points_distance`](Self::min_points_distance) and the packed
    /// point-array lanes (which need per-row results, not one fold). Returns
    /// `best` folded with this probe's minimum.
    #[expect(
        clippy::indexing_slicing,
        reason = "Stack ids start at root 0 and follow builder-encoded children below aabbs/codes.len(); leaf ids are below linework.facets.len()"
    )]
    fn min_point_distance_with_stack_by(
        &self,
        linework: &PreparedLinework,
        stack: &mut Vec<u32>,
        mut best: f64,
        bound: impl Fn([f64; 4]) -> f64,
        mut refine: impl FnMut(Facet) -> crate::error::Result<f64>,
    ) -> crate::error::Result<f64> {
        stack.clear();
        stack.push(0);
        while let Some(index) = stack.pop() {
            if bound(self.aabbs[index as usize]) >= best {
                continue;
            }
            match BvhCode::new(self.codes[index as usize]).decode() {
                BvhNode::Leaf(facet_index) => {
                    let candidate = refine(linework.facets[facet_index])?;
                    best = best.min(candidate);
                },
                BvhNode::Internal(first_child) => {
                    self.push_nearer_last(stack, first_child as u32, &bound);
                },
            }
        }
        Ok(best)
    }

    pub(crate) fn min_point_distance_with_stack<const SQUARED: bool>(
        &self,
        linework: &PreparedLinework,
        x: f64,
        y: f64,
        stack: &mut Vec<u32>,
        best: f64,
    ) -> crate::error::Result<f64> {
        let bound = |aabb: [f64; 4]| {
            if SQUARED {
                aabb_distance_squared(aabb, point_aabb(x, y))
            } else {
                aabb_distance(aabb, point_aabb(x, y))
            }
        };
        self.min_point_distance_with_stack_by(linework, stack, best, bound, |facet| {
            if SQUARED {
                linework.facet_point_distance_squared(facet, x, y)
            } else {
                linework.facet_point_distance(facet, Point::new_raw_xy(x, y))
            }
        })
    }

    /// Whether any probe vertex lies within `limit` (squared, inclusive) of
    /// the linework, over a single shared traversal stack. The AABB upper
    /// bound answers TRUE without exact tests when a whole subtree sits
    /// inside the limit. `simd` enables the vector facet kernel (callers
    /// gate it on squared-space-safe coordinates; the scalar kernel carries
    /// its own extreme-coordinate rescue).
    pub(crate) fn any_points_within(
        &self,
        linework: &PreparedLinework,
        probes: impl Iterator<Item = (f64, f64)>,
        limit: f64,
        simd: bool,
    ) -> crate::error::Result<bool> {
        let mut stack: Vec<u32> = Vec::new();
        for (x, y) in probes {
            if self.point_within_with_stack(linework, x, y, limit, simd, &mut stack)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// One probe's within-limit descent over a caller-provided (reused) stack
    /// — the per-row engine behind
    /// [`any_points_within`](Self::any_points_within) and the packed
    /// point-array lanes.
    #[expect(
        clippy::indexing_slicing,
        reason = "Stack ids start at root 0 and follow builder-encoded children below self.aabbs/codes.len(); leaf ids are below linework.facets.len()"
    )]
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "A decoded internal node owns an allocated adjacent child pair, so first_child + 1 is below self.aabbs.len()"
    )]
    pub(crate) fn point_within_with_stack(
        &self,
        linework: &PreparedLinework,
        x: f64,
        y: f64,
        limit: f64,
        simd: bool,
        stack: &mut Vec<u32>,
    ) -> crate::error::Result<bool> {
        let probe = point_aabb(x, y);
        stack.clear();
        stack.push(0);
        while let Some(index) = stack.pop() {
            let aabb = self.aabbs[index as usize];
            if aabb_distance_squared(aabb, probe) > limit {
                continue;
            }
            // Upper-bound early-TRUE: every point of the node's box
            // (hence every segment vertex beneath it) is within limit.
            if aabb_max_distance_squared(aabb, probe) <= limit {
                return Ok(true);
            }
            match BvhCode::new(self.codes[index as usize]).decode() {
                BvhNode::Leaf(facet_index) => {
                    let facet = linework.facets[facet_index];
                    let candidate = if simd {
                        linework.facet_point_distance_squared(facet, x, y)?
                    } else {
                        let point = Point::new_raw_xy(x, y);
                        let mut best = f64::INFINITY;
                        for i in 0..facet.segment_count as usize {
                            best = best.min(point_segment_distance_squared(
                                point,
                                linework.segment(facet, i),
                            )?);
                        }
                        best
                    };
                    if candidate <= limit {
                        return Ok(true);
                    }
                },
                BvhNode::Internal(first_child) => {
                    let first_child = first_child as u32;
                    stack.push(first_child);
                    stack.push(first_child + 1);
                },
            }
        }
        Ok(false)
    }

    /// Visit every box-overlapping segment pair between the two trees'
    /// linework — dual descent with box-OVERLAP pruning (disjoint boxes
    /// cannot contain touching segments), stopping at the visitor's first
    /// `Break`. The crossing and contact-classifying oracles share this
    /// skeleton.
    pub(crate) fn for_each_overlapping_segment_pair<B>(
        &self,
        linework: &PreparedLinework,
        other: &Self,
        other_linework: &PreparedLinework,
        visit: impl FnMut(Segment, Segment) -> ControlFlow<B>,
    ) -> Option<B> {
        let mut stack = Vec::new();
        self.for_each_overlapping_segment_pair_with_stack(
            linework,
            other,
            other_linework,
            &mut stack,
            visit,
        )
    }

    /// [`for_each_overlapping_segment_pair`] with a caller-owned stack so an
    /// outer probe loop can allocate once and `clear` per call.
    #[expect(
        clippy::indexing_slicing,
        reason = "Root/child ids are below each tree's aabbs/codes.len(); leaf facet ids are below each linework.facets.len()"
    )]
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "Internal nodes encode allocated sibling pairs, so first_child + 1 stays below the corresponding aabbs.len()"
    )]
    pub(crate) fn for_each_overlapping_segment_pair_with_stack<B>(
        &self,
        linework: &PreparedLinework,
        other: &Self,
        other_linework: &PreparedLinework,
        stack: &mut Vec<(u32, u32)>,
        mut visit: impl FnMut(Segment, Segment) -> ControlFlow<B>,
    ) -> Option<B> {
        stack.clear();
        stack.push((0, 0));
        while let Some((left_index, right_index)) = stack.pop() {
            if !aabbs_overlap(
                self.aabbs[left_index as usize],
                other.aabbs[right_index as usize],
            ) {
                continue;
            }
            let left = BvhCode::new(self.codes[left_index as usize]).decode();
            let right = BvhCode::new(other.codes[right_index as usize]).decode();
            match (left, right) {
                (BvhNode::Leaf(left_index), BvhNode::Leaf(right_index)) => {
                    let left_facet = linework.facets[left_index];
                    let right_facet = other_linework.facets[right_index];
                    for i in 0..left_facet.segment_count as usize {
                        let probe = linework.segment(left_facet, i);
                        for j in 0..right_facet.segment_count as usize {
                            if let ControlFlow::Break(value) =
                                visit(probe, other_linework.segment(right_facet, j))
                            {
                                return Some(value);
                            }
                        }
                    }
                },
                (BvhNode::Internal(left_first), _) => {
                    let left_first = left_first as u32;
                    stack.push((left_first, right_index));
                    stack.push((left_first + 1, right_index));
                },
                (BvhNode::Leaf(_), BvhNode::Internal(right_first)) => {
                    let right_first = right_first as u32;
                    stack.push((left_index, right_first));
                    stack.push((left_index, right_first + 1));
                },
            }
        }
        None
    }

    /// Whether any segment of this tree's linework touches or crosses any
    /// segment of `other`'s — the crossing half of the distance/dwithin
    /// disjointness check.
    pub(crate) fn any_segments_intersect(
        &self,
        linework: &PreparedLinework,
        other: &Self,
        other_linework: &PreparedLinework,
    ) -> bool {
        self.for_each_overlapping_segment_pair(linework, other, other_linework, |left, right| {
            if segments_intersect(left, right) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })
        .is_some()
    }

    /// Whether `point` lies ON any linework segment (boundary-inclusive,
    /// exact `point_on_segment` at leaves) — the tree-accelerated
    /// `covers_point` for 1D linework: a point can only lie on a segment
    /// whose box contains it, so the descent prunes by box containment.
    pub(crate) fn covers_point(&self, linework: &PreparedLinework, point: Point) -> bool {
        let mut stack = Vec::new();
        self.covers_point_with_stack(linework, point, &mut stack)
    }

    /// [`covers_point`] with a caller-owned stack for batch probes.
    pub(crate) fn covers_point_with_stack(
        &self,
        linework: &PreparedLinework,
        point: Point,
        stack: &mut Vec<u32>,
    ) -> bool {
        stack.clear();
        stack.push(0);
        while let Some(index) = stack.pop() {
            let aabb = self.node_aabb(index);
            if point.x < aabb[0] || point.x > aabb[2] || point.y < aabb[1] || point.y > aabb[3] {
                continue;
            }
            match self.decode_node(index) {
                BvhNode::Leaf(facet_index) => {
                    let facet = facet_for_leaf(linework, facet_index);
                    for segment in linework.facet_segments(facet) {
                        if point_on_segment(point, segment.start, segment.end) {
                            return true;
                        }
                    }
                },
                BvhNode::Internal(first_child) => {
                    let first_child = first_child as u32;
                    stack.push(first_child);
                    stack.push(first_child.wrapping_add(1));
                },
            }
        }
        false
    }

    /// Visit every linework segment whose box overlaps `probe`'s — the
    /// single-tree sibling of [`for_each_overlapping_segment_pair`] for a
    /// side below the tree crossover.
    pub(crate) fn for_each_segment_overlapping<B>(
        &self,
        linework: &PreparedLinework,
        probe: Segment,
        visit: impl FnMut(Segment) -> ControlFlow<B>,
    ) -> Option<B> {
        let mut stack = Vec::new();
        self.for_each_segment_overlapping_with_stack(linework, probe, &mut stack, visit)
    }

    /// [`for_each_segment_overlapping`] with a caller-owned stack — the one-
    /// sided probe loop in `parts_segments_cross` allocates once and clears.
    pub(crate) fn for_each_segment_overlapping_with_stack<B>(
        &self,
        linework: &PreparedLinework,
        probe: Segment,
        stack: &mut Vec<u32>,
        mut visit: impl FnMut(Segment) -> ControlFlow<B>,
    ) -> Option<B> {
        let probe_aabb = segment_aabb(probe);
        stack.clear();
        stack.push(0);
        while let Some(index) = stack.pop() {
            if !aabbs_overlap(self.node_aabb(index), probe_aabb) {
                continue;
            }
            match self.decode_node(index) {
                BvhNode::Leaf(facet_index) => {
                    let facet = facet_for_leaf(linework, facet_index);
                    for i in 0..facet.segment_count as usize {
                        if let ControlFlow::Break(value) = visit(linework.segment(facet, i)) {
                            return Some(value);
                        }
                    }
                },
                BvhNode::Internal(first_child) => {
                    let first_child = first_child as u32;
                    stack.push(first_child);
                    stack.push(first_child.wrapping_add(1));
                },
            }
        }
        None
    }

    /// Whether `probe` touches or crosses any linework segment, reusing a
    /// caller-owned traversal stack across probes.
    pub(crate) fn any_segment_intersecting_with_stack(
        &self,
        linework: &PreparedLinework,
        probe: Segment,
        stack: &mut Vec<u32>,
    ) -> bool {
        self.for_each_segment_overlapping_with_stack(linework, probe, stack, |candidate| {
            if segments_intersect(probe, candidate) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })
        .is_some()
    }

    /// Argmin sibling of the min sweep: the nearest `(probe vertex,
    /// projection)` pair from a full-point probe stream to the linework.
    /// Safe squares use SIMD facet bounds; other exponents use distance space.
    /// Winning segments retain their full source ordinates.
    pub(crate) fn nearest_to_points<const SQUARED: bool>(
        &self,
        linework: &PreparedLinework,
        probes: impl Iterator<Item = Point>,
        keep: &impl Fn(Point, Segment) -> bool,
        best: &mut Option<NearestCandidate>,
    ) -> crate::error::Result<bool> {
        let mut stack: Vec<u32> = Vec::new();
        let mut changed = false;
        for probe in probes {
            changed |= self.nearest_point_with_stack::<SQUARED, false>(
                linework,
                probe,
                f64::INFINITY,
                f64::INFINITY,
                keep,
                &mut stack,
                best,
            )?;
        }
        Ok(changed)
    }

    pub(crate) fn nearest_point_with_stack<const SQUARED: bool, const BOUNDED: bool>(
        &self,
        linework: &PreparedLinework,
        probe: Point,
        upper_bound: f64,
        tolerance: f64,
        keep: &impl Fn(Point, Segment) -> bool,
        stack: &mut Vec<u32>,
        best: &mut Option<NearestCandidate>,
    ) -> crate::error::Result<bool> {
        self.nearest_point_with_stack_and_ordinal::<SQUARED, BOUNDED>(
            linework,
            probe,
            upper_bound,
            tolerance,
            &|point, _, segment| keep(point, segment),
            stack,
            best,
        )
    }

    pub(crate) fn nearest_point_with_stack_and_ordinal<const SQUARED: bool, const BOUNDED: bool>(
        &self,
        linework: &PreparedLinework,
        probe: Point,
        upper_bound: f64,
        tolerance: f64,
        keep: &impl Fn(Point, usize, Segment) -> bool,
        stack: &mut Vec<u32>,
        best: &mut Option<NearestCandidate>,
    ) -> crate::error::Result<bool> {
        let mut changed_any = false;
        let probe_aabb = point_aabb(probe.x, probe.y);
        let bound = |aabb: [f64; 4]| {
            if SQUARED {
                aabb_distance_squared(aabb, probe_aabb)
            } else {
                aabb_distance(aabb, probe_aabb)
            }
        };
        let mut distance_limit = if SQUARED {
            upper_bound
        } else {
            best.as_ref()
                .map(|best| best.distance_key.distance_upper_bound())
                .transpose()?
                .unwrap_or(upper_bound)
        };
        stack.clear();
        stack.push(0);
        while let Some(index) = stack.pop() {
            #[cfg(test)]
            NEAREST_WORK.with(|work| work.set(work.get().saturating_add(1)));
            let limit = if SQUARED {
                best.as_ref().map_or(upper_bound, |b| {
                    if BOUNDED {
                        upper_bound.min(b.distance_key.upper_bound())
                    } else {
                        b.distance_key.upper_bound()
                    }
                })
            } else {
                distance_limit
            };
            if bound(self.node_aabb(index)) > limit {
                continue;
            }
            match self.decode_node(index) {
                BvhNode::Leaf(facet_index) => {
                    let facet = facet_for_leaf(linework, facet_index);
                    let facet_distance = if SQUARED {
                        linework.facet_point_distance_squared(facet, probe.x, probe.y)?
                    } else {
                        linework.facet_point_distance(facet, probe)?
                    };
                    if facet_distance <= limit {
                        let changed = refine_facet_nearest_with_ordinal::<BOUNDED>(
                            linework,
                            facet,
                            probe,
                            upper_bound,
                            tolerance,
                            keep,
                            best,
                        )?;
                        changed_any |= changed;
                        if !SQUARED && changed {
                            distance_limit = best
                                .as_ref()
                                .expect("refinement improved the nearest candidate")
                                .distance_key
                                .distance_upper_bound()?;
                        }
                    }
                },
                BvhNode::Internal(first_child) => {
                    if BOUNDED {
                        stack.push(first_child as u32);
                        stack.push((first_child as u32).wrapping_add(1));
                    } else {
                        self.push_nearer_last(stack, first_child as u32, &bound);
                    }
                },
            }
        }
        Ok(changed_any)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "first_child is builder-produced and first_child + 1 is below self.aabbs.len()"
    )]
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "first_child names an allocated sibling pair, so right = left + 1 and stack child + 1 stay below self.aabbs.len()"
    )]
    fn push_nearer_last(
        &self,
        stack: &mut Vec<u32>,
        first_child: u32,
        bound: &impl Fn([f64; 4]) -> f64,
    ) {
        let left = first_child as usize;
        let right = left + 1;
        let left_bound = bound(self.aabbs[left]);
        let right_bound = bound(self.aabbs[right]);
        if left_bound <= right_bound {
            stack.push(first_child + 1);
            stack.push(first_child);
        } else {
            stack.push(first_child);
            stack.push(first_child + 1);
        }
    }

    /// First touching/crossing segment pair between two trees' lineworks
    /// (full ordinates) — the witness extractor for intersecting operands.
    /// Rides the shared dual descent
    /// ([`for_each_overlapping_segment_pair`]), returning
    /// the first crossing pair instead of a boolean.
    pub(crate) fn find_intersecting_pair(
        &self,
        linework: &PreparedLinework,
        other: &Self,
        other_linework: &PreparedLinework,
    ) -> Option<(Segment, Segment)> {
        self.for_each_overlapping_segment_pair(linework, other, other_linework, |left, right| {
            if segments_intersect(left, right) {
                ControlFlow::Break((left, right))
            } else {
                ControlFlow::Continue(())
            }
        })
    }

    /// One-sided sibling of [`find_intersecting_pair`]: the first linework
    /// segment touching `probe` (full ordinates), or `None`. Rides the shared
    /// single-tree descent ([`for_each_segment_overlapping`]).
    pub(crate) fn find_intersecting_segment(
        &self,
        linework: &PreparedLinework,
        probe: Segment,
    ) -> Option<Segment> {
        self.for_each_segment_overlapping(linework, probe, |candidate| {
            if segments_intersect(probe, candidate) {
                ControlFlow::Break(candidate)
            } else {
                ControlFlow::Continue(())
            }
        })
    }
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

impl HeapSize for FacetBvh {
    fn heap_bytes(&self) -> usize {
        self.aabbs
            .heap_bytes()
            .saturating_add(self.codes.heap_bytes())
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

#[cfg(test)]
mod public_nearest_work_tests {
    use pyo3::prelude::*;
    use pyo3::types::PyList;

    use super::{
        CLEARANCE_BVH_FACETS, CLEARANCE_BVH_NODES, CLEARANCE_BVH_SEGMENTS,
        CLEARANCE_STRIP_BUILD_VERTICES, CLEARANCE_STRIP_TESTS, NEAREST_WORK,
    };
    use crate::geometry::CLEARANCE_STRIP_SUPPORT_FALLBACKS;

    #[test]
    fn public_clearance_bounds_radial_candidate_work() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let module = PyModule::new(py, "gometry._lib").unwrap();
            crate::lib(&module).unwrap();
            let mut previous = None;
            let mut previous_count = None;
            for count in [128_usize, 512, 1024, 4096] {
                let coords = PyList::empty(py);
                for index in 0..count {
                    let radius = 0.5 + (index.wrapping_mul(17) % 31) as f64 / 31.0;
                    let angle = std::f64::consts::TAU * index as f64 / count as f64;
                    coords
                        .append((radius * angle.cos(), radius * angle.sin()))
                        .unwrap();
                }
                let polygon = module
                    .getattr("Polygon")
                    .unwrap()
                    .call1((&coords,))
                    .unwrap();
                for operation in ["minimum_clearance", "minimum_clearance_line"] {
                    NEAREST_WORK.with(|work| work.set(0));
                    CLEARANCE_BVH_NODES.with(|work| work.set(0));
                    CLEARANCE_BVH_SEGMENTS.with(|work| work.set(0));
                    CLEARANCE_BVH_FACETS.with(|work| work.set(0));
                    CLEARANCE_STRIP_TESTS.with(|work| work.set(0));
                    CLEARANCE_STRIP_BUILD_VERTICES.with(|work| work.set(0));
                    CLEARANCE_STRIP_SUPPORT_FALLBACKS.with(|work| work.set(0));
                    let result = polygon.call_method0(operation).unwrap();
                    if operation == "minimum_clearance" {
                        assert!(result.extract::<f64>().unwrap() > 0.0);
                    } else {
                        assert!(
                            !result
                                .getattr("is_empty")
                                .unwrap()
                                .extract::<bool>()
                                .unwrap()
                        );
                    }
                    let work = NEAREST_WORK.with(std::cell::Cell::get);
                    let nodes = CLEARANCE_BVH_NODES.with(std::cell::Cell::get);
                    let segments = CLEARANCE_BVH_SEGMENTS.with(std::cell::Cell::get);
                    let facets = CLEARANCE_BVH_FACETS.with(std::cell::Cell::get);
                    let strips = CLEARANCE_STRIP_TESTS.with(std::cell::Cell::get);
                    let build_vertices = CLEARANCE_STRIP_BUILD_VERTICES.with(std::cell::Cell::get);
                    let support_fallbacks =
                        CLEARANCE_STRIP_SUPPORT_FALLBACKS.with(std::cell::Cell::get);
                    assert_eq!(work, nodes.saturating_add(segments));
                    assert!(facets > 0, "clearance BVH did not reach a facet");
                    assert!(strips > 0, "clearance BVH did not test a source strip");
                    assert!(build_vertices > 0);
                    eprintln!(
                        "{operation}: {count} vertices, {nodes} BVH nodes, {facets} facets, {segments} segments, {strips} strip tests, {build_vertices} support-vertex visits, {support_fallbacks} support exact fallbacks",
                    );
                    assert!(
                        work > count,
                        "the public call did not reach nearest queries"
                    );
                    if let Some(previous) = previous
                        && ((previous_count == Some(128) && count == 512)
                            || (previous_count == Some(1024) && count == 4096))
                    {
                        assert!(
                            work < previous * 6,
                            "{count} radial vertices took {work} visits versus {previous} at the previous fourfold checkpoint",
                        );
                    }
                    if operation == "minimum_clearance_line" {
                        previous = Some(work);
                        previous_count = Some(count);
                    }
                }
            }
        });
    }

    #[test]
    fn public_clearance_bit_reversal_work_scales_at_extreme_magnitudes() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let module = PyModule::new(py, "gometry._lib").unwrap();
            crate::lib(&module).unwrap();
            for scale in [1.0, 2.0_f64.powi(-700), 2.0_f64.powi(600)] {
                let mut previous = None;
                for count in [64_usize, 256] {
                    let lines = PyList::empty(py);
                    let bits = count.trailing_zeros();
                    for index in 0..count {
                        let source_index = index.reverse_bits() >> (usize::BITS - bits);
                        let start = (10 * source_index) as f64 * scale;
                        let end = (10 * source_index + 1) as f64 * scale;
                        lines
                            .append(
                                PyList::new(py, [(start, 0.0), (end, 0.0)])
                                    .expect("two-point line coordinates are accepted"),
                            )
                            .unwrap();
                    }
                    let shape = module
                        .getattr("MultiLineString")
                        .unwrap()
                        .call1((&lines,))
                        .unwrap();

                    CLEARANCE_BVH_NODES.with(|work| work.set(0));
                    CLEARANCE_BVH_SEGMENTS.with(|work| work.set(0));
                    let answer = shape
                        .call_method0("minimum_clearance")
                        .unwrap()
                        .extract::<f64>()
                        .unwrap();
                    assert_eq!(answer, scale);
                    let nodes = CLEARANCE_BVH_NODES.with(std::cell::Cell::get);
                    let segments = CLEARANCE_BVH_SEGMENTS.with(std::cell::Cell::get);
                    let work = nodes.saturating_add(segments);
                    eprintln!(
                        "bit-reversal clearance: scale={scale:e}, {count} segments, answer={answer:e}, {nodes} BVH nodes, {segments} segment visits, {work} total",
                    );

                    if let Some(previous) = previous {
                        assert!(
                            work < previous * 7,
                            "fourfold bit-reversal input at scale {scale:e} took {work} events versus {previous}",
                        );
                    }
                    previous = Some(work);
                }
            }
        });
    }

    #[test]
    fn public_split_bounds_point_projection_work() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let module = PyModule::new(py, "gometry._lib").unwrap();
            crate::lib(&module).unwrap();
            let mut previous = None;
            for count in [128_usize, 512] {
                let coords = PyList::new(
                    py,
                    (0..=count).map(|index| (index as f64, (index % 2) as f64)),
                )
                .unwrap();
                let points =
                    PyList::new(py, (0..count).map(|index| (index as f64 + 0.5, 0.5))).unwrap();
                let line = module
                    .getattr("LineString")
                    .unwrap()
                    .call1((&coords,))
                    .unwrap();
                let points = module
                    .getattr("MultiPoint")
                    .unwrap()
                    .call1((&points,))
                    .unwrap();
                NEAREST_WORK.with(|work| work.set(0));
                let pieces = module
                    .getattr("split")
                    .unwrap()
                    .call1((&line, &points))
                    .unwrap();
                assert_eq!(pieces.len().unwrap(), count + 1);
                let work = NEAREST_WORK.with(std::cell::Cell::get);
                eprintln!("split: {count} segments/points, {work} candidate visits");
                assert!(
                    work > count,
                    "the public call did not reach nearest queries"
                );
                if let Some(previous) = previous {
                    assert!(
                        work < previous * 6,
                        "{count} split segments/points took {work} visits versus {previous} at one quarter the size",
                    );
                }
                previous = Some(work);
            }
        });
    }
}

#[cfg(test)]
mod clearance_strip_tests {
    use super::{BvhNode, ClearanceFacetBvh, FacetBvh, finite_midpoint};
    use crate::geometry::{CoordSeq, Point, PreparedLinework, source_dot_cmp};

    fn collect_descendant_facets(tree: &ClearanceFacetBvh, node: usize, facets: &mut Vec<usize>) {
        match tree
            .tree
            .decode_node(u32::try_from(node).expect("clearance node IDs fit u32"))
        {
            BvhNode::Leaf(facet) => facets.push(facet),
            BvhNode::Internal(first_child) => {
                collect_descendant_facets(tree, first_child, facets);
                collect_descendant_facets(tree, first_child.wrapping_add(1), facets);
            },
        }
    }

    #[test]
    fn facet_center_midpoints_stay_finite_at_coordinate_extremes() {
        let maximum = f64::MAX;
        assert_eq!(finite_midpoint(maximum, maximum), maximum);
        assert_eq!(finite_midpoint(-maximum, maximum), 0.0);
        assert_eq!(
            finite_midpoint(0.0, 2.0 * f64::from_bits(1)),
            f64::from_bits(1),
        );
    }

    #[test]
    fn clearance_strip_supports_enclose_every_source_vertex() {
        let maximum = f64::MAX;
        let points = [
            Point::new_raw_xy(-maximum, -maximum),
            Point::new_raw_xy(maximum, maximum),
            Point::new_raw_xy(maximum, -maximum),
            Point::new_raw_xy(-maximum, maximum),
            Point::new_raw_xy(-(2.0_f64.powi(600)), 2.0_f64.powi(600)),
            Point::new_raw_xy(2.0_f64.powi(600), -(2.0_f64.powi(600))),
            Point::new_raw_xy(-maximum, -maximum),
            Point::new_raw_xy(maximum, maximum),
            Point::new_raw_xy(maximum, -maximum),
            Point::new_raw_xy(-maximum, maximum),
            Point::new_raw_xy(-(2.0_f64.powi(600)), 2.0_f64.powi(600)),
            Point::new_raw_xy(2.0_f64.powi(600), -(2.0_f64.powi(600))),
            Point::new_raw_xy(-maximum, -maximum),
            Point::new_raw_xy(maximum, maximum),
            Point::new_raw_xy(maximum, -maximum),
            Point::new_raw_xy(-maximum, maximum),
            Point::new_raw_xy(-(2.0_f64.powi(600)), 2.0_f64.powi(600)),
        ];
        let first_chain =
            CoordSeq::try_from_points(points.get(..9).expect("first facet chain exists")).unwrap();
        let second_chain =
            CoordSeq::try_from_points(points.get(9..).expect("second facet chain exists")).unwrap();
        let linework = PreparedLinework::from_chains(vec![first_chain, second_chain]).unwrap();
        let clearance = FacetBvh::build_for_clearance(&linework)
            .unwrap()
            .expect("linework has facets");

        for (node, strip) in clearance.strips.iter().enumerate() {
            let low = linework
                .vertex_at_chain_local(strip.low_support.chain, strip.low_support.local)
                .xy();
            let high = linework
                .vertex_at_chain_local(strip.high_support.chain, strip.high_support.local)
                .xy();
            let mut facets = Vec::new();
            collect_descendant_facets(&clearance, node, &mut facets);
            for facet_index in facets {
                let facet = *linework
                    .facets
                    .get(facet_index)
                    .expect("clearance leaf refers to prepared linework");
                for local in 0..=facet.segment_count as usize {
                    let vertex_index = linework.facet_vertex_index(facet, local);
                    let vertex = linework.vertex_at(vertex_index).xy();
                    assert!(
                        !source_dot_cmp(strip.normal, low, vertex).unwrap().is_gt(),
                        "node {node} low support excludes source vertex {vertex_index}"
                    );
                    assert!(
                        !source_dot_cmp(strip.normal, vertex, high).unwrap().is_gt(),
                        "node {node} high support excludes source vertex {vertex_index}"
                    );
                }
            }
        }
    }
}
