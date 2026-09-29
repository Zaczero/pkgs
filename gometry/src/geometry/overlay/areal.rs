#[cfg(test)]
use crate::error::Result;
#[cfg(test)]
use crate::geometry::Polygon;
#[cfg(test)]
use crate::geometry::overlay::{ExactSourceInventory, OverlayOp};
use crate::geometry::{Segment, XY, ray_crossing_is_right};

/// One segment's signed contribution to the rightward-ray winding number at
/// `probe`; closed loops sum these contributions to their winding.
pub(crate) fn ray_crossing(segment: Segment, probe: XY) -> i32 {
    let (ay, by) = (segment.start.y, segment.end.y);
    if (ay > probe.y) != (by > probe.y)
        && ray_crossing_is_right(segment.start.x, ay, segment.end.x, by, probe.x, probe.y)
    {
        return if by > ay { 1 } else { -1 };
    }
    0
}

#[cfg(test)]
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

#[cfg(test)]
pub(in crate::geometry) use reference::build_areal_arrangement;

#[cfg(test)]
mod reference {
    use super::ray_crossing;
    use crate::error::{ResourceError, Result};
    use crate::geometry::overlay::self_node_segments_sourced;
    use crate::geometry::{
        Arrangement, Bounds, Polygon, Ring, RingWinding, Segment, SegmentIndex, XY, ring_winding,
        same_point,
    };
    use crate::scratch_pool::{RetainScratch, ScratchPool, flat_vec_retained_bytes};

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
                (segments.saturating_add(ring.len()), rings.saturating_add(1))
            })
    }

    fn staged_ring_segments(ring: &Ring, ring_index: usize) -> impl Iterator<Item = Segment> + '_ {
        let (xs, ys) = (ring.xs(), ring.ys());
        let mut count = xs.len();
        if count >= 2 {
            let first = xs
                .first()
                .zip(ys.first())
                .map(|(&x, &y)| XY::new(x, y))
                .expect("a ring with at least two coordinates has a first point");
            let last = xs
                .last()
                .zip(ys.last())
                .map(|(&x, &y)| XY::new(x, y))
                .expect("a ring with at least two coordinates has a last point");
            if same_point(first, last) {
                count = count
                    .checked_sub(1)
                    .expect("a closed ring with at least two coordinates has one to remove");
            }
        }
        // Shells follow CCW winding and holes follow CW winding in the source
        // chain so coincident rings can cancel by their signed direction.
        let has_cycle = count >= 3;
        let reverse =
            has_cycle && ((ring_winding(ring) == RingWinding::Clockwise) != (ring_index > 0));
        let mut remaining = if has_cycle { count } else { 0 };
        let mut from = 0_usize;
        std::iter::from_fn(move || {
            while remaining > 0 {
                let to = if reverse {
                    from.checked_sub(1).unwrap_or_else(|| {
                        count
                            .checked_sub(1)
                            .expect("a staged ring cycle has at least three coordinates")
                    })
                } else {
                    from.checked_add(1)
                        .filter(|&next| next < count)
                        .unwrap_or(0)
                };
                let start = XY::new(
                    *xs.get(from)
                        .expect("staged source index is in the open ring"),
                    *ys.get(from)
                        .expect("staged source index is in the open ring"),
                );
                let end = XY::new(
                    *xs.get(to).expect("staged target index is in the open ring"),
                    *ys.get(to).expect("staged target index is in the open ring"),
                );
                from = to;
                remaining = remaining
                    .checked_sub(1)
                    .expect("a staged edge consumes one remaining ring edge");
                if !same_point(start, end) {
                    return Some(Segment { start, end });
                }
            }
            None
        })
    }

    fn next_areal_segment_count(count: usize) -> Result<usize> {
        let next = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        u32::try_from(next)
            .map(|_| next)
            .map_err(|_| ResourceError::SizeOverflow.into())
    }

    fn count_staged_operand_segments<P: AsRef<Polygon>>(
        polygons: &[P],
        mut count: usize,
    ) -> Result<usize> {
        for polygon in polygons {
            for (ring_index, ring) in polygon.as_ref().rings().enumerate() {
                for _ in staged_ring_segments(ring, ring_index) {
                    count = next_areal_segment_count(count)?;
                }
            }
        }
        Ok(count)
    }

    fn areal_segment_capacity<P: AsRef<Polygon>, Q: AsRef<Polygon>>(
        left: &[P],
        right: &[Q],
        upper_bound: usize,
    ) -> Result<usize> {
        // Ring lengths are a cheap reserve upper bound. If it exceeds the stored
        // ordinal width, count the exact staged edges using the same ring walk.
        if u32::try_from(upper_bound).is_ok() {
            return Ok(upper_bound);
        }
        let exact_count = count_staged_operand_segments(left, 0)
            .and_then(|count| count_staged_operand_segments(right, count));
        match exact_count {
            Ok(count) => Ok(count),
            Err(error) => {
                // The sourced-noding fallback acquires this state before reporting
                // its width error. Preserve entropy-error precedence while
                // rejecting before staging the columns that cannot be indexed.
                crate::collections::collection_hash_state()?;
                Err(error)
            },
        }
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
                let before = segments.len();
                for segment in staged_ring_segments(ring, ring_index) {
                    segments.push(segment);
                    tags.push(operand as u32);
                    segment_loop.push(loop_anchor.len() as u32);
                }
                if segments.len() > before {
                    let anchor = ring
                        .xs()
                        .first()
                        .zip(ring.ys().first())
                        .map(|(&x, &y)| XY::new(x, y))
                        .expect("a ring with staged segments has a first point");
                    loop_anchor.push(anchor);
                }
            }
        }
    }

    pub(in crate::geometry) fn build_areal_arrangement<P: AsRef<Polygon>, Q: AsRef<Polygon>>(
        left: &[P],
        right: &[Q],
    ) -> Result<Vec<[i32; 2]>> {
        // Ring lengths and ring count provide cheap upper bounds for staged data.
        let (left_segments, left_rings) = areal_operand_inventory(left);
        let (right_segments, right_rings) = areal_operand_inventory(right);
        let segment_upper = left_segments.saturating_add(right_segments);
        let segment_capacity = areal_segment_capacity(left, right, segment_upper)?;
        let mut scratch = ScratchPool::checkout(&AREAL_SCRATCH);
        // Each retained loop anchor belongs to a ring that staged at least one edge.
        let ring_capacity = left_rings.saturating_add(right_rings).min(segment_capacity);
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
        let mut arrangement = if let Some(arrangement) =
            Arrangement::<[i32; 2]>::from_loops(&segments, &loop_ranges, |loop_id| {
                if loop_operand[loop_id] == 0 {
                    [1, 0]
                } else {
                    [0, 1]
                }
            })? {
            arrangement
        } else {
            let (atomic, sources) = self_node_segments_sourced(segments.iter().copied())?;
            // Multi-source provenance: sum every original ordinal's unit weight,
            // flipping sign when that ordinal owned the reverse of the noded
            // representative. Exact undirected dedup keeps `atomic` unique-sized
            // so stacked multiparts never re-expand into O(E) weighted pieces.
            Arrangement::<[i32; 2]>::weighted(atomic.iter().copied(), |piece| {
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
            })?
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
        );
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
        Ok(windings)
    }

    fn outside_winding_seeds_with_scratch(
        segments: &[Segment],
        segment_operand: &[u32],
        segment_loop: &[u32],
        loop_component: &[u32],
        probes: &[XY],
        ranges: &mut Vec<(u32, u32)>,
        boxes: &mut Vec<Bounds>,
    ) -> Vec<[i32; 2]> {
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

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::error::ErrorKind;
        use crate::geometry::CoordSeq;

        #[test]
        fn areal_segment_count_admits_u32_boundary_and_rejects_next_segment() {
            let maximum = usize::try_from(u32::MAX).unwrap();
            assert_eq!(
                next_areal_segment_count(maximum.checked_sub(1).unwrap()).unwrap(),
                maximum
            );
            assert!(matches!(
                next_areal_segment_count(maximum).unwrap_err().kind(),
                ErrorKind::Resource(ResourceError::SizeOverflow)
            ));
        }

        #[test]
        fn loose_segment_upper_bound_uses_the_staged_cyclic_edges() {
            let points = [
                XY::new(0.0, 0.0),
                XY::new(2.0, 0.0),
                XY::new(2.0, 0.0),
                XY::new(2.0, 2.0),
                XY::new(0.0, 2.0),
                XY::new(0.0, 0.0),
            ];
            let shell = Ring::from_closed(CoordSeq::try_from_xy(&points).unwrap()).unwrap();
            let collapsed_points = [XY::new(1.0, 1.0); 4];
            let collapsed_ring =
                Ring::from_closed(CoordSeq::try_from_xy(&collapsed_points).unwrap()).unwrap();
            let polygons = [Polygon::new(shell, vec![collapsed_ring])];

            let right: [Polygon; 0] = [];
            let loose_upper = usize::try_from(u32::MAX).unwrap().checked_add(1).unwrap();
            let segment_capacity = areal_segment_capacity(&polygons, &right, loose_upper).unwrap();

            let mut segments = Vec::new();
            let mut tags = Vec::new();
            let mut segment_loop = Vec::new();
            let mut loop_anchor = Vec::new();
            stage_areal_operand(
                0,
                &polygons,
                &mut segments,
                &mut tags,
                &mut segment_loop,
                &mut loop_anchor,
            );
            assert_eq!(segment_capacity, 4);
            assert_eq!(segments.len(), segment_capacity);
            assert_eq!(segments.len(), tags.len());
            assert_eq!(segments.len(), segment_loop.len());
            assert_eq!(loop_anchor, [XY::new(0.0, 0.0)]);
            assert!(
                segments
                    .iter()
                    .all(|segment| !same_point(segment.start, segment.end))
            );
        }
    }
}
