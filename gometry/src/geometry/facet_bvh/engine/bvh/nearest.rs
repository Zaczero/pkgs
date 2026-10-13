#[cfg(test)]
use super::NEAREST_WORK;
use super::{
    BvhCode, BvhNode, Facet, FacetBvh, NearestCandidate, Point, PreparedLinework, Segment,
    aabb_distance, aabb_distance_squared, aabb_max_distance_squared, facet_for_leaf, point_aabb,
    point_segment_distance_squared, point_segment_selection,
};

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

impl FacetBvh {
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
    pub(super) fn push_nearer_last(
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
}

/// Scalar-refine one facet's exact selection keys, deferring projection
/// and ordinate interpolation until the selected witness is emitted.
pub(in crate::geometry::facet_bvh::engine) fn refine_facet_nearest<const BOUNDED: bool>(
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

fn refine_facet_nearest_with_ordinal<const BOUNDED: bool>(
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
