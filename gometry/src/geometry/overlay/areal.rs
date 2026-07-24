#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;
use crate::geometry::*;
pub(in crate::geometry) fn binary_areal_overlay<P: AsRef<Polygon>, Q: AsRef<Polygon>>(
    left: &[P],
    right: &[Q],
    op: OverlayOp,
) -> Vec<Polygon> {
    // The cascade's hot MERGE step: mirror `Shape::union`'s clean fast lane so a
    // dissolve merge of two simple blobs reassembles straight from its
    // result-boundary arcs instead of paying a full DCEL/winding arrangement at
    // every level of the recursion. Without this the cascade was several × the
    // equivalent binary union (which already takes this path). Bails to the
    // arrangement oracle (below) on any degeneracy — same contract as the entry.
    if matches!(
        op,
        OverlayOp::Union | OverlayOp::Difference | OverlayOp::SymmetricDifference
    ) && let Some(shape) = crate::geometry::clean_union::clean_overlay(left, right, op)
    {
        return match shape {
            Shape::Polygon(polygon) => vec![polygon],
            Shape::MultiPolygon(polygons) => polygons,
            _ => Vec::new(),
        };
    }
    build_areal_arrangement(left, right).overlay_polygons(op)
}

impl ArealArrangement {
    /// The `op` region of this joint arrangement, assembled into polygons —
    /// [`binary_areal_overlay`] minus the build (the overlay entry reuses ONE
    /// arrangement for both the region and the boundary contact).
    pub(in crate::geometry) fn overlay_polygons(&self, op: OverlayOp) -> Vec<Polygon> {
        let interior = |winding: [i32; 2]| -> bool {
            let (a, b) = (winding[0] >= 1, winding[1] >= 1);
            match op {
                OverlayOp::Union => a || b,
                OverlayOp::Intersection => a && b,
                OverlayOp::Difference => a && !b,
                OverlayOp::SymmetricDifference => a != b,
            }
        };
        assemble_region_polygons(self.arrangement.region_rings(&self.windings, interior))
    }
}

/// The joint per-operand winding arrangement behind [`binary_areal_overlay`]
/// and the native areal relate: both operands' rings noded into ONE
/// arrangement whose faces carry `[left, right]` winding numbers.
pub(in crate::geometry) struct ArealArrangement {
    pub arrangement: Arrangement<[i32; 2]>,
    pub windings: Vec<[i32; 2]>,
    /// Whether each operand contributed at least one non-degenerate ring.
    pub operand_present: [bool; 2],
}

impl ArealArrangement {
    /// Boundary∩boundary contact straight from the joint arrangement —
    /// the noding already split every crossing and vertex-on-edge touch,
    /// so one edge-piece pass answers both verdicts with NO second
    /// boundary scan: a noded edge with nonzero net multiplicity for
    /// BOTH operands is a shared collinear run, and a vertex incident to
    /// both operands' edges but on no shared run is an isolated touch.
    /// Net-zero pieces bound nothing (the winding doctrine, exactly as
    /// the native relate reads them).
    pub(in crate::geometry) fn boundary_contact(&self) -> (Vec<XY>, bool) {
        let vertices = self.arrangement.vertex_count();
        let mut incident_left = vec![false; vertices];
        let mut incident_right = vec![false; vertices];
        let mut on_shared_run = vec![false; vertices];
        let mut shares_boundary = false;
        self.arrangement
            .for_each_edge_piece(|multiplicity, from, to, _, _| {
                let left_edge = multiplicity[0] != 0;
                let right_edge = multiplicity[1] != 0;
                if left_edge {
                    incident_left[from as usize] = true;
                    incident_left[to as usize] = true;
                }
                if right_edge {
                    incident_right[from as usize] = true;
                    incident_right[to as usize] = true;
                }
                if left_edge && right_edge {
                    shares_boundary = true;
                    on_shared_run[from as usize] = true;
                    on_shared_run[to as usize] = true;
                }
            });
        let touch_points = (0..vertices)
            .filter(|&vertex| {
                incident_left[vertex] && incident_right[vertex] && !on_shared_run[vertex]
            })
            .map(|vertex| self.arrangement.vertex_point(vertex as u32))
            .collect();
        (touch_points, shares_boundary)
    }
}

fn areal_operand_inventory<P: AsRef<Polygon>>(polygons: &[P]) -> (usize, usize) {
    polygons
        .iter()
        .flat_map(|polygon| polygon.as_ref().rings())
        .fold((0, 0), |(segments, rings), ring| {
            (segments + ring.len(), rings + 1)
        })
}

fn stage_areal_operand<P: AsRef<Polygon>>(
    operand: usize,
    polygons: &[P],
    segments: &mut Vec<Segment>,
    tags: &mut Vec<u32>,
    segment_loop: &mut Vec<u32>,
    loop_anchor: &mut Vec<XY>,
    operand_present: &mut [bool; 2],
) {
    for polygon in polygons {
        for (ring_index, ring) in polygon.as_ref().rings().enumerate() {
            // Columnar canonical walk (shells CCW, holes CW): the
            // orientation reads the same shoelace the Point-staged
            // `canonical_ring_walk` read, and segments emit straight
            // off the coordinate columns in forward or reversed cyclic
            // order — no 40-byte `Point` per vertex.
            let (xs, ys) = (ring.xs(), ring.ys());
            let mut count = xs.len();
            if count >= 2
                && same_point(XY::new(xs[0], ys[0]), XY::new(xs[count - 1], ys[count - 1]))
            {
                count -= 1;
            }
            if count < 3 {
                continue;
            }
            let reverse = (ring_winding(ring) == RingWinding::Clockwise) != (ring_index > 0);
            let before = segments.len();
            for step in 0..count {
                let (from, to) = if reverse {
                    (wrap_index(count - step, count), count - step - 1)
                } else {
                    (step, wrap_index(step + 1, count))
                };
                let start = XY::new(xs[from], ys[from]);
                let end = XY::new(xs[to], ys[to]);
                if !same_point(start, end) {
                    segments.push(Segment { start, end });
                    tags.push(operand as u32);
                    segment_loop.push(loop_anchor.len() as u32);
                }
            }
            if segments.len() > before {
                loop_anchor.push(XY::new(xs[0], ys[0]));
                operand_present[operand] = true;
            }
        }
    }
}

pub(in crate::geometry) fn build_areal_arrangement<P: AsRef<Polygon>, Q: AsRef<Polygon>>(
    left: &[P],
    right: &[Q],
) -> ArealArrangement {
    // Tight upper bounds from the ring inventory: every ring of `n` points
    // yields at most `n` segments and exactly one anchor.
    let (left_segments, left_rings) = areal_operand_inventory(left);
    let (right_segments, right_rings) = areal_operand_inventory(right);
    let mut segments: Vec<Segment> =
        Vec::with_capacity(left_segments.saturating_add(right_segments));
    let mut tags: Vec<u32> = Vec::with_capacity(left_segments.saturating_add(right_segments));
    let mut segment_loop: Vec<u32> =
        Vec::with_capacity(left_segments.saturating_add(right_segments));
    // One anchor vertex per loop — locates the loop's component after
    // noding.
    let mut loop_anchor: Vec<XY> = Vec::with_capacity(left_rings.saturating_add(right_rings));
    let mut operand_present = [false, false];
    stage_areal_operand(
        0,
        left,
        &mut segments,
        &mut tags,
        &mut segment_loop,
        &mut loop_anchor,
        &mut operand_present,
    );
    stage_areal_operand(
        1,
        right,
        &mut segments,
        &mut tags,
        &mut segment_loop,
        &mut loop_anchor,
        &mut operand_present,
    );
    // Per-loop contiguous ranges + operand (segment_loop is non-decreasing
    // by construction) for the positional fast path.
    let mut loop_ranges: Vec<(u32, u32)> =
        Vec::with_capacity(left_rings.saturating_add(right_rings));
    let mut loop_operand: Vec<u32> = Vec::new();
    for (index, &loop_id) in segment_loop.iter().enumerate() {
        if loop_id as usize == loop_ranges.len() {
            loop_ranges.push((index as u32, index as u32 + 1));
            loop_operand.push(tags[index]);
        } else {
            loop_ranges[loop_id as usize].1 = index as u32 + 1;
        }
    }
    // POSITIONAL multi-loop build (clean rings, the common case): per-loop
    // positional identity skips the atomic soup, the vertex hash-dedup,
    // and the global edge sort the general path pays. `None` (repeats,
    // T-junctions, collinear overlaps, cross-loop vertex coincidence)
    // falls back to the exact general oracle.
    let arrangement = Arrangement::<[i32; 2]>::from_loops(&segments, &loop_ranges, |loop_id| {
        if loop_operand[loop_id] == 0 {
            [1, 0]
        } else {
            [0, 1]
        }
    })
    .unwrap_or_else(|| {
        let (atomic, sources) = self_node_segments_sourced(&segments);
        Arrangement::<[i32; 2]>::weighted(&atomic, |piece| {
            if tags[sources[piece] as usize] == 0 {
                [1, 0]
            } else {
                [0, 1]
            }
        })
    });
    // Outside-winding seed per component and per operand: every loop
    // lives in exactly one component, and loops of other components
    // cannot pass through it (see `winding_region`).
    let loop_component: Vec<u32> = loop_anchor
        .iter()
        .map(|&anchor| arrangement.component_of_point(anchor))
        .collect();
    let seeds = outside_winding_seeds(
        &segments,
        &tags,
        &segment_loop,
        &loop_component,
        &arrangement.component_probes(),
    );
    let windings = arrangement.face_windings(&seeds);
    ArealArrangement {
        arrangement,
        windings,
        operand_present,
    }
}

/// Outside-winding seeds for every arrangement component: per operand,
/// the winding of all loops OUTSIDE the probe's component, by ONE
/// rightward-ray R-tree query over the input segment soup. The crossing
/// rule is additive per segment over closed loops, so the soup answers
/// every loop at once and the box-pruned per-loop scans disappear — the
/// dense dissolve cascade was raycasting its merged blob's full shell
/// for THOUSANDS of component probes (the profiled 59% of an 8k-star
/// union). Own-component segments are skipped: the probe is one of the
/// component's own vertices, where its own winding is undefined (and
/// other components' loops cannot pass through it, so the seed is
/// constant across the component).
pub(in crate::geometry) fn outside_winding_seeds(
    segments: &[Segment],
    segment_operand: &[u32],
    segment_loop: &[u32],
    loop_component: &[u32],
    probes: &[XY],
) -> Vec<[i32; 2]> {
    // Loops are contiguous runs of the soup: recover each loop's segment
    // range and bbox in one pass (the arm choice needs the longest run).
    let loop_count = loop_component.len();
    let mut ranges = vec![(u32::MAX, 0_u32); loop_count];
    let mut boxes = vec![Bounds::from_xy_iter(std::iter::empty()); loop_count];
    for (ordinal, (&segment, &index)) in segments.iter().zip(segment_loop).enumerate() {
        let range = &mut ranges[index as usize];
        range.0 = range.0.min(ordinal as u32);
        range.1 = ordinal as u32 + 1;
        let bounds = &mut boxes[index as usize];
        bounds.include_xy(segment.start);
        bounds.include_xy(segment.end);
    }
    // Two arms by TIME MODEL, not a flat probe count. The loop-granular
    // scan costs one box check per probe-loop pair plus the crossing
    // rule inside containing boxes — its degenerate term is probes ×
    // longest loop (the dense cascade's merged blob shell raycast in
    // full for thousands of hole-component probes: the profiled 59% of
    // an 8k-star union). The R-tree arm replaces that with one
    // rightward-ray query per probe (the crossing rule is additive per
    // segment over closed loops, so the soup answers every loop at
    // once), but its bulk load costs tens of box checks per segment —
    // sparse calls (thousands of probes over SHORT loops) stay brute.
    let longest = ranges
        .iter()
        .map(|&(start, end)| end.saturating_sub(start))
        .max()
        .unwrap_or(0) as usize;
    let brute = probes.len() * longest <= 64 * segments.len() + 256 * probes.len();
    if brute {
        return probes
            .iter()
            .enumerate()
            .map(|(component, &probe)| {
                let mut seed = [0, 0];
                for index in 0..loop_count {
                    let bounds = boxes[index];
                    if loop_component[index] != component as u32 && bounds.contains_xy(probe) {
                        let (start, end) = ranges[index];
                        seed[segment_operand[start as usize] as usize] += segments
                            [start as usize..end as usize]
                            .iter()
                            .map(|&segment| ray_crossing(segment, probe))
                            .sum::<i32>();
                    }
                }
                seed
            })
            .collect();
    }
    let index = SegmentIndex::build(segments);
    let ray_end = segments.iter().fold(f64::NEG_INFINITY, |end, segment| {
        end.max(segment.start.x).max(segment.end.x)
    }) + 1.0;
    probes
        .iter()
        .enumerate()
        .map(|(component, &probe)| {
            let ray = Segment {
                start: probe,
                end: XY::new(ray_end, probe.y),
            };
            let mut seed = [0, 0];
            for entry in index.intersecting_candidates(ray) {
                if loop_component[segment_loop[entry.ordinal] as usize] != component as u32 {
                    seed[segment_operand[entry.ordinal] as usize] +=
                        ray_crossing(entry.segment, probe);
                }
            }
            seed
        })
        .collect()
}

/// One segment's signed contribution to the rightward-ray winding number
/// at `probe` — the per-edge body of the classic crossing rule (closed
/// loops sum these to their winding, so segment soups compose).
pub(crate) fn ray_crossing(segment: Segment, probe: XY) -> i32 {
    let (ay, by) = (segment.start.y, segment.end.y);
    // Sign-form crossing decision (exact; the cross-multiplied form overflowed
    // to `inf` vs `inf` at extreme finite coordinates and dropped crossings).
    if (ay > probe.y) != (by > probe.y)
        && ray_crossing_is_right(segment.start.x, ay, segment.end.x, by, probe.x, probe.y)
    {
        return if by > ay { 1 } else { -1 };
    }
    0
}

/// Canonical interior-left walk of one ring (shells CCW, holes CW):
/// oriented bit-exactly (reversal only), closing duplicate dropped —
/// `None` for degenerate rings.
pub(crate) fn canonical_ring_walk<C: Coordinates + ?Sized>(
    coords: &C,
    hole: bool,
) -> Option<Vec<Point>> {
    let mut walk = orient_ring(coords, hole);
    if walk.len() > 1 && same_point(walk[0], walk[walk.len() - 1]) {
        walk.pop();
    }
    (walk.len() >= 3).then_some(walk)
}

/// Dissolve (n-ary union) of polygons. Small batches resolve in ONE joint
/// arrangement (the union of valid polygons IS the `winding >= 1` region);
/// larger batches recurse through a kd-style split on the wider bbox axis
/// and merge with the binary winding overlay — the cascaded-union shape
/// (each half dissolves to far fewer segments before the merges above it,
/// keeping every noding pass small).
pub(in crate::geometry) fn dissolve_polygons(polygons: Vec<Polygon>) -> Vec<Polygon> {
    // Disjoint clusters dissolve independently — and SINGLETONS (the
    // sparse caseload) cost one tiny single-loop arrangement instead of
    // riding every cascade merge above them. Boxes that do not overlap
    // cannot interact, so cross-cluster results concatenate exactly.
    if polygons.len() > 2
        && let Some(roots) = bbox_overlap_clusters(&polygons)
    {
        // Slots in first-occurrence order keep the output deterministic.
        let mut slot_of: crate::collections::HashMap<usize, usize> =
            crate::collections::HashMap::new();
        let mut groups: Vec<Vec<Polygon>> = Vec::new();
        for (polygon, root) in polygons.into_iter().zip(roots) {
            let slot = *slot_of.entry(root).or_insert_with(|| {
                groups.push(Vec::new());
                groups.len() - 1
            });
            groups[slot].push(polygon);
        }
        let mut out = Vec::new();
        for group in groups {
            out.extend(dissolve_connected(group));
        }
        return out;
    }
    dissolve_connected(polygons)
}

/// Union-find clustering by bbox overlap via a sweep over `minx`, with a
/// pair-test budget: `None` for interaction-dense scenes where clustering
/// cannot pay (one cluster — the cascade handles it). Correctness never
/// depends on the verdict, only the work shape does.
pub(crate) fn bbox_overlap_clusters(polygons: &[Polygon]) -> Option<Vec<usize>> {
    let bounds: Vec<_> = polygons.iter().map(Polygon::bounds).collect();
    bbox_overlap_clusters_for_bounds(&bounds)
}

pub(crate) fn bbox_overlap_clusters_for_bounds(bounds: &[Option<Bounds>]) -> Option<Vec<usize>> {
    let mut boxes: Vec<(Bounds, usize)> = bounds
        .iter()
        .enumerate()
        .map(|(index, bounds)| bounds.map(|bounds| (bounds, index)))
        .collect::<Option<_>>()?;
    boxes.sort_unstable_by(|a, b| a.0.minx().total_cmp(&b.0.minx()));
    let mut components = crate::collections::UnionFind::new(bounds.len());
    let mut budget = 8 * bounds.len();
    let mut active: Vec<(Bounds, usize)> = Vec::new();
    let mut clusters = bounds.len();
    for &(bounds, index) in &boxes {
        active.retain(|(candidate, _)| candidate.maxx() >= bounds.minx());
        budget = budget.checked_sub(active.len())?;
        for &(candidate, other) in &active {
            if candidate.maxy() >= bounds.miny()
                && candidate.miny() <= bounds.maxy()
                && !components.union(index, other)
            {
                clusters -= 1;
            }
        }
        active.push((bounds, index));
    }
    (clusters > 1).then(|| {
        (0..bounds.len())
            .map(|index| components.find(index))
            .collect()
    })
}

/// One bbox-connected cluster's dissolve: the joint-arrangement leaf or
/// the kd-style cascade.
pub(crate) fn dissolve_connected(polygons: Vec<Polygon>) -> Vec<Polygon> {
    /// Joint-arrangement budget: below this many segments the single
    /// global pass beats the recursion overhead.
    const LEAF_SEGMENTS: usize = 256;
    // A two-blob leaf is exactly a binary union — take the clean fast lane
    // (arrangement fallback on degeneracy) rather than building a joint
    // winding arrangement, which for large blobs costs several × the clean
    // reassembly. The joint winding pass still owns 3+-way small leaves, where
    // one shared arrangement beats pairwise merges.
    if polygons.len() == 2 {
        return binary_areal_overlay(&polygons[..1], &polygons[1..], OverlayOp::Union);
    }
    let segments: usize = polygons.iter().map(Polygon::segment_count).sum();
    if segments <= LEAF_SEGMENTS || polygons.len() <= 1 {
        return winding_region(&polygon_winding_loops(&polygons), |winding| winding >= 1);
    }
    // Median split by bbox center along the wider joint axis. Centers are
    // computed ONCE and carried next to each polygon — `bounds()` is a
    // full vertex scan, far too hot for a selection comparator.
    let mut keyed: Vec<((f64, f64), Polygon)> = polygons
        .into_iter()
        .map(|polygon| {
            let center = polygon.bounds().map_or((0.0, 0.0), |bounds| {
                (
                    f64::midpoint(bounds.minx(), bounds.maxx()),
                    f64::midpoint(bounds.miny(), bounds.maxy()),
                )
            });
            (center, polygon)
        })
        .collect();
    let (minx, maxx, miny, maxy) = keyed.iter().fold(
        (
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ),
        |(minx, maxx, miny, maxy), (c, _)| {
            (minx.min(c.0), maxx.max(c.0), miny.min(c.1), maxy.max(c.1))
        },
    );
    let split_x = (maxx - minx) >= (maxy - miny);
    let component = |center: &(f64, f64)| if split_x { center.0 } else { center.1 };
    let mid = keyed.len() / 2;
    keyed.select_nth_unstable_by(mid, |a, b| component(&a.0).total_cmp(&component(&b.0)));
    let right = keyed.split_off(mid).into_iter().map(|(_, p)| p).collect();
    let left = dissolve_connected(keyed.into_iter().map(|(_, p)| p).collect());
    let right = dissolve_connected(right);
    binary_areal_overlay(&left, &right, OverlayOp::Union)
}

/// The canonical winding loops of polygons (shells CCW, holes CW) — the
/// single-winding dissolve input: the union of valid polygons IS the
/// `winding >= 1` region of their joint arrangement.
pub(in crate::geometry) fn polygon_winding_loops(
    polygons: &[Polygon],
) -> Vec<(Vec<f64>, Vec<f64>)> {
    let mut loops = Vec::new();
    for polygon in polygons {
        for (ring_index, ring) in polygon.rings().enumerate() {
            let Some(walk) = canonical_ring_walk(&ring, ring_index > 0) else {
                continue;
            };
            loops.push(walk.iter().map(|point| (point.x, point.y)).unzip());
        }
    }
    loops
}
