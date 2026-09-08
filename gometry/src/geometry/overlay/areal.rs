#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::error::Result;
use crate::geometry::overlay::{ExactSourceInventory, OverlayOp, self_node_segments_sourced};
use crate::geometry::{
    Arrangement, Bounds, CoordSeq, Point, Polygon, RingWinding, Segment, SegmentIndex, XY,
    assemble_region_polygons, ray_crossing_is_right, ring_winding, same_point, wrap_index,
};
use crate::scratch_pool::{RetainScratch, ScratchPool, flat_vec_retained_bytes};

pub(in crate::geometry) fn binary_areal_overlay<P: AsRef<Polygon>, Q: AsRef<Polygon>>(
    left: &[P],
    right: &[Q],
    op: OverlayOp,
) -> Result<Vec<Polygon>> {
    let inventory = ExactSourceInventory::from_polygon_operands(left, right)?;
    let mut budget = crate::geometry::ExpansionBudget::new(op.name(), "exact partition");
    let partition = inventory.partition(&mut budget)?;
    let (_, _, polygons) = inventory.overlay_parts(&partition, op)?;
    Ok(polygons)
}

impl ArealArrangement {
    /// The `op` region of this joint arrangement, assembled into polygons —
    /// [`binary_areal_overlay`] minus the build (the overlay entry reuses ONE
    /// arrangement for both the region and the boundary contact).
    pub(in crate::geometry) fn overlay_polygons(&self, op: OverlayOp) -> Result<Vec<Polygon>> {
        let interior = |winding: [i32; 2]| -> bool {
            let (a, b) = (winding[0] != 0, winding[1] != 0);
            match op {
                OverlayOp::Union => a || b,
                OverlayOp::Intersection => a && b,
                OverlayOp::Difference => a && !b,
                OverlayOp::SymmetricDifference => a != b,
            }
        };
        assemble_region_polygons(self.arrangement.region_rings(&self.windings, interior))
    }

    /// The intersection of the two operands as the mixed-dimension parts a
    /// graph overlay reports: the region where both windings are nonzero,
    /// every boundary run both operands share that bounds no kept face (a
    /// line), and every isolated meeting of the two boundaries (a point).
    /// A vertex with a kept face around it, or on an emitted line, is
    /// already represented.
    pub(in crate::geometry) fn intersection_parts(
        &self,
    ) -> Result<(Vec<Point>, Vec<CoordSeq>, Vec<Polygon>)> {
        let polygons = self.overlay_polygons(OverlayOp::Intersection)?;
        let winding = |face: u32| self.windings[face as usize];
        let kept = |face: u32| {
            let [left, right] = winding(face);
            left != 0 && right != 0
        };
        let arrangement = &self.arrangement;
        let mut lines = Vec::new();
        let mut on_line = vec![false; arrangement.points.len()];
        arrangement.for_each_edge_piece(|_, from, to, left_face, right_face| {
            if kept(left_face) || kept(right_face) {
                return;
            }
            let (left, right) = (winding(left_face), winding(right_face));
            let shared = (left[0] != 0) != (right[0] != 0) && (left[1] != 0) != (right[1] != 0);
            if shared {
                lines.push(CoordSeq::try_from_xy(&[
                    arrangement.points[from as usize],
                    arrangement.points[to as usize],
                ]));
                on_line[from as usize] = true;
                on_line[to as usize] = true;
            }
        });
        let mut points = Vec::new();
        for vertex in 0..arrangement.points.len() {
            if on_line[vertex] {
                continue;
            }
            let slots =
                arrangement.starts[vertex] as usize..arrangement.starts[vertex + 1] as usize;
            let (mut any_kept, mut left_any, mut left_all, mut right_any, mut right_all) =
                (false, false, true, false, true);
            for slot in slots {
                let face = arrangement.face_of[slot];
                any_kept |= kept(face);
                let [left, right] = winding(face);
                left_any |= left != 0;
                left_all &= left != 0;
                right_any |= right != 0;
                right_all &= right != 0;
            }
            if !any_kept && left_any && !left_all && right_any && !right_all {
                let point = arrangement.points[vertex];
                points.push(Point::new_raw_xy(point.x, point.y));
            }
        }
        Ok((
            points,
            lines.into_iter().collect::<Result<Vec<_>>>()?,
            polygons,
        ))
    }
}

/// The joint per-operand winding arrangement behind [`binary_areal_overlay`]
/// and the native areal relate: both operands' rings noded into ONE
/// arrangement whose faces carry `[left, right]` winding numbers.
pub(in crate::geometry) struct ArealArrangement {
    pub arrangement: Arrangement<[i32; 2]>,
    pub windings: Vec<[i32; 2]>,
}

#[derive(Default)]
struct ArealScratch {
    segments: Vec<Segment>,
    tags: Vec<u32>,
    segment_loop: Vec<u32>,
    loop_anchor: Vec<XY>,
    loop_ranges: Vec<(u32, u32)>,
    loop_operand: Vec<u32>,
    loop_component: Vec<u32>,
    probes: Vec<XY>,
    probe_slots: Vec<Option<XY>>,
    ranges: Vec<(u32, u32)>,
    boxes: Vec<Bounds>,
}

impl RetainScratch for ArealScratch {
    fn clear_for_reuse(&mut self) {
        self.segments.clear();
        self.tags.clear();
        self.segment_loop.clear();
        self.loop_anchor.clear();
        self.loop_ranges.clear();
        self.loop_operand.clear();
        self.loop_component.clear();
        self.probes.clear();
        self.probe_slots.clear();
        self.ranges.clear();
        self.boxes.clear();
    }

    fn retained_bytes(&self) -> usize {
        flat_vec_retained_bytes(&self.segments)
            .saturating_add(flat_vec_retained_bytes(&self.tags))
            .saturating_add(flat_vec_retained_bytes(&self.segment_loop))
            .saturating_add(flat_vec_retained_bytes(&self.loop_anchor))
            .saturating_add(flat_vec_retained_bytes(&self.loop_ranges))
            .saturating_add(flat_vec_retained_bytes(&self.loop_operand))
            .saturating_add(flat_vec_retained_bytes(&self.loop_component))
            .saturating_add(flat_vec_retained_bytes(&self.probes))
            .saturating_add(flat_vec_retained_bytes(&self.probe_slots))
            .saturating_add(flat_vec_retained_bytes(&self.ranges))
            .saturating_add(flat_vec_retained_bytes(&self.boxes))
    }
}

thread_local! {
    static AREAL_SCRATCH: ScratchPool<ArealScratch> = const { ScratchPool::new(ArealScratch {
        segments: Vec::new(), tags: Vec::new(), segment_loop: Vec::new(), loop_anchor: Vec::new(),
        loop_ranges: Vec::new(), loop_operand: Vec::new(), loop_component: Vec::new(),
        probes: Vec::new(), probe_slots: Vec::new(), ranges: Vec::new(), boxes: Vec::new(),
    }) };
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
            }
        }
    }
}

pub(in crate::geometry) fn build_areal_arrangement<P: AsRef<Polygon>, Q: AsRef<Polygon>>(
    left: &[P],
    right: &[Q],
) -> Result<ArealArrangement> {
    // Tight upper bounds from the ring inventory: every ring of `n` points
    // yields at most `n` segments and exactly one anchor.
    let (left_segments, left_rings) = areal_operand_inventory(left);
    let (right_segments, right_rings) = areal_operand_inventory(right);
    let mut scratch = ScratchPool::checkout(&AREAL_SCRATCH);
    let segment_capacity = left_segments.saturating_add(right_segments);
    let ring_capacity = left_rings.saturating_add(right_rings);
    scratch.segments.reserve(segment_capacity);
    scratch.tags.reserve(segment_capacity);
    scratch.segment_loop.reserve(segment_capacity);
    scratch.loop_anchor.reserve(ring_capacity);
    let mut segments = std::mem::take(&mut scratch.segments);
    let mut tags = std::mem::take(&mut scratch.tags);
    let mut segment_loop = std::mem::take(&mut scratch.segment_loop);
    // One anchor vertex per loop — locates the loop's component after
    // noding.
    let mut loop_anchor = std::mem::take(&mut scratch.loop_anchor);
    stage_areal_operand(
        0,
        left,
        &mut segments,
        &mut tags,
        &mut segment_loop,
        &mut loop_anchor,
    );
    stage_areal_operand(
        1,
        right,
        &mut segments,
        &mut tags,
        &mut segment_loop,
        &mut loop_anchor,
    );
    // Per-loop contiguous ranges + operand (segment_loop is non-decreasing
    // by construction) for the positional fast path.
    let mut loop_ranges = std::mem::take(&mut scratch.loop_ranges);
    let mut loop_operand = std::mem::take(&mut scratch.loop_operand);
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
    let mut arrangement =
        match Arrangement::<[i32; 2]>::from_loops(&segments, &loop_ranges, |loop_id| {
            if loop_operand[loop_id] == 0 {
                [1, 0]
            } else {
                [0, 1]
            }
        })? {
            Some(arrangement) => arrangement,
            None => {
                let (atomic, sources) = self_node_segments_sourced(segments.iter().copied())?;
                // Multi-source provenance: sum every original ordinal's unit weight,
                // flipping sign when that ordinal owned the reverse of the noded
                // representative. Exact undirected dedup keeps `atomic` unique-sized
                // so stacked multiparts never re-expand into O(E) weighted pieces.
                Arrangement::<[i32; 2]>::weighted(&atomic, |piece| {
                    let mut weight = [0_i32, 0_i32];
                    for &(ordinal, reversed) in &sources[piece] {
                        let unit = if tags[ordinal as usize] == 0 {
                            [1, 0]
                        } else {
                            [0, 1]
                        };
                        if reversed {
                            weight[0] -= unit[0];
                            weight[1] -= unit[1];
                        } else {
                            weight[0] += unit[0];
                            weight[1] += unit[1];
                        }
                    }
                    weight
                })
            },
        };
    // Outside-winding seed per component and per operand: every loop
    // lives in exactly one component, and loops of other components
    // cannot pass through it (see `winding_region`).
    let mut loop_component = std::mem::take(&mut scratch.loop_component);
    loop_component.extend(
        loop_anchor
            .iter()
            .map(|&anchor| arrangement.component_of_point(anchor)),
    );
    let mut probes = std::mem::take(&mut scratch.probes);
    arrangement.component_probes_from_slots(&mut probes, &mut scratch.probe_slots);
    let mut ranges = std::mem::take(&mut scratch.ranges);
    let mut boxes = std::mem::take(&mut scratch.boxes);
    let seeds = outside_winding_seeds_with_scratch(
        &segments,
        &tags,
        &segment_loop,
        &loop_component,
        &probes,
        &mut ranges,
        &mut boxes,
    )?;
    let windings = arrangement.face_windings(&seeds);
    scratch.segments = segments;
    scratch.tags = tags;
    scratch.segment_loop = segment_loop;
    scratch.loop_anchor = loop_anchor;
    scratch.loop_ranges = loop_ranges;
    scratch.loop_operand = loop_operand;
    scratch.loop_component = loop_component;
    scratch.probes = probes;
    scratch.ranges = ranges;
    scratch.boxes = boxes;
    Ok(ArealArrangement {
        arrangement,
        windings,
    })
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
) -> Result<Vec<[i32; 2]>> {
    let mut ranges = Vec::new();
    let mut boxes = Vec::new();
    outside_winding_seeds_with_scratch(
        segments,
        segment_operand,
        segment_loop,
        loop_component,
        probes,
        &mut ranges,
        &mut boxes,
    )
}

fn outside_winding_seeds_with_scratch(
    segments: &[Segment],
    segment_operand: &[u32],
    segment_loop: &[u32],
    loop_component: &[u32],
    probes: &[XY],
    ranges: &mut Vec<(u32, u32)>,
    boxes: &mut Vec<Bounds>,
) -> Result<Vec<[i32; 2]>> {
    // Loops are contiguous runs of the soup: recover each loop's segment
    // range and bbox in one pass (the arm choice needs the longest run).
    let loop_count = loop_component.len();
    ranges.clear();
    ranges.resize(loop_count, (u32::MAX, 0_u32));
    boxes.clear();
    boxes.resize(loop_count, Bounds::from_xy_iter(std::iter::empty()));
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
        return Ok(probes
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
            .collect());
    }
    let index = SegmentIndex::build(segments)?;
    let ray_end = segments.iter().fold(f64::NEG_INFINITY, |end, segment| {
        end.max(segment.start.x).max(segment.end.x)
    }) + 1.0;
    Ok(probes
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
        .collect())
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
