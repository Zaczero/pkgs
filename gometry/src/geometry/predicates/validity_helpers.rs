#[cfg(test)]
use ahash::HashMapExt as _;

#[cfg(test)]
use crate::geometry::HashMap;
use crate::geometry::predicates::{LineworkChains, indexed_segments_are_simple, validate_line};
use crate::geometry::{
    Component, CoordSeq, Coordinates, FacetBvh, Ordering, Point, PointKey, PointSetIndex, Polygon,
    PreparedLinework, RefinedChainProvenance, SEGMENT_INDEX_MIN_PAIRS, Segment, Shape,
    ShapeCountQuery, VertexProvenance, XY, point_distance, point_distance_squared,
    point_in_ccw_triangle, point_segment_distance, ring_winding, same_point, segment_projection,
    squared_norm_is_trustworthy, try_point_distance_squared, try_point_segment_distance_squared,
};

/// Iteratively remove segments with a degree-1 endpoint (dangles) from the
/// reference linework used by the validity tests.
#[cfg(test)]
pub(crate) fn prune_dangles(segments: &mut Vec<Segment>) {
    if segments.is_empty() {
        return;
    }

    let count = segments.len();
    let map_capacity = count.saturating_mul(2);
    let mut degree: HashMap<PointKey, usize> = HashMap::with_capacity(map_capacity);
    let mut adjacency: HashMap<PointKey, Vec<usize>> = HashMap::with_capacity(map_capacity);
    for (index, segment) in segments.iter().enumerate() {
        let start = PointKey::new(segment.start);
        let end = PointKey::new(segment.end);
        let start_degree = degree.entry(start).or_insert(0);
        *start_degree = start_degree.saturating_add(1);
        let end_degree = degree.entry(end).or_insert(0);
        *end_degree = end_degree.saturating_add(1);
        adjacency.entry(start).or_default().push(index);
        adjacency.entry(end).or_default().push(index);
    }

    let mut removed = vec![false; count];
    let mut worklist: Vec<PointKey> = degree
        .iter()
        .filter_map(|(key, &value)| (value == 1).then_some(*key))
        .collect();
    worklist.sort_unstable();

    while let Some(vertex) = worklist.pop() {
        if degree.get(&vertex).copied().unwrap_or(0) != 1 {
            continue;
        }
        let Some(incident) = adjacency.get(&vertex) else {
            continue;
        };
        for &index in incident {
            if removed[index] {
                continue;
            }
            removed[index] = true;
            let segment = &segments[index];
            for endpoint in [PointKey::new(segment.start), PointKey::new(segment.end)] {
                if let Some(value) = degree.get_mut(&endpoint) {
                    *value = value
                        .checked_sub(1)
                        .expect("each live segment contributes to endpoint degree");
                    if *value == 1 {
                        worklist.push(endpoint);
                    }
                }
            }
        }
    }

    let mut index = 0;
    segments.retain(|_| {
        let keep = !removed[index];
        index = index.wrapping_add(1);
        keep
    });
}

/// A point strictly inside a simple counter-clockwise closed ring.
///
/// Takes the lowest-then-leftmost vertex (necessarily convex), and probes the
/// centroid of its corner triangle; when other boundary vertices intrude into
/// that triangle, the midpoint towards the intruder closest to the corner is
/// interior instead (the classical interior-point construction).
#[expect(
    clippy::indexing_slicing,
    reason = "the admitted face is a closed ring and the selected corner and cyclic neighbors come from its open slice"
)]
pub(in crate::geometry) fn face_interior_point(face: &[XY]) -> XY {
    let (_, open) = face
        .split_last()
        .expect("closed face has a final coordinate");
    let corner = open
        .iter()
        .enumerate()
        .min_by(|(_, left), (_, right)| {
            (left.y, left.x)
                .partial_cmp(&(right.y, right.x))
                .unwrap_or(Ordering::Equal)
        })
        .map_or(0, |(index, _)| index);
    let vertex = open[corner];
    let previous_index = if corner == 0 {
        open.len().wrapping_sub(1)
    } else {
        corner.wrapping_sub(1)
    };
    let next_index = if corner.wrapping_add(1) == open.len() {
        0
    } else {
        corner.wrapping_add(1)
    };
    let previous = open[previous_index];
    let next = open[next_index];

    let intruder = open
        .iter()
        .copied()
        .filter(|point| {
            !same_point(*point, previous)
                && !same_point(*point, vertex)
                && !same_point(*point, next)
                && point_in_ccw_triangle(*point, previous, vertex, next)
        })
        .min_by(|left, right| {
            point_distance_squared(*left, vertex)
                .partial_cmp(&point_distance_squared(*right, vertex))
                .unwrap_or(Ordering::Equal)
        });
    // Midpoints of finite coordinates are finite (`f64::midpoint` cannot
    // overflow), so construction cannot fail. The no-intruder probe is the
    // midpoint of the corner and the opposite edge's midpoint — strictly
    // inside the corner triangle, like the classical centroid.
    let (x, y) = intruder.map_or_else(
        || {
            (
                f64::midpoint(f64::midpoint(previous.x, next.x), vertex.x),
                f64::midpoint(f64::midpoint(previous.y, next.y), vertex.y),
            )
        },
        |intruder| {
            (
                f64::midpoint(vertex.x, intruder.x),
                f64::midpoint(vertex.y, intruder.y),
            )
        },
    );
    XY::new(x, y)
}

/// The realizing pair of the minimum clearance — `(vertex, nearest other
/// vertex or segment projection, squared distance)` — or `None` when the
/// clearance is infinite. Both `minimum_clearance` (the scalar) and
/// `minimum_clearance_line` (the witness line) fold this one scan, so the
/// two surfaces can never disagree.
#[derive(Clone, Debug)]
pub(crate) struct MinimumClearanceWitness {
    pub(crate) plan: [VertexProvenance; 2],
    pub(crate) distance_squared: f64,
    /// The vertices the plan indexes. Emitting the witness pair needs them,
    /// and the scan above already transposed the geometry to build them, so
    /// the witness carries them rather than letting each caller transpose it
    /// a second time.
    pub(crate) points: Vec<Point>,
}

pub(crate) fn minimum_clearance_witness(
    shape: &Shape,
) -> crate::error::Result<Option<MinimumClearanceWitness>> {
    let points = shape.points_vec()?;
    if points.len() < 2 {
        return Ok(None);
    }
    let segments = indexed_segments(shape)?;
    let keep = |point: Point, _: usize, segment: Segment| {
        !same_point(point, segment.start) && !same_point(point, segment.end)
    };
    let witness = minimum_clearance_candidates(shape, &points, &segments, &keep)?;
    Ok(
        witness.map(|(plan, distance_squared)| MinimumClearanceWitness {
            plan,
            distance_squared,
            points,
        }),
    )
}

pub(crate) fn minimum_clearance_witness_with_support(
    sites: &Shape,
    support: &Shape,
    refined_chains: &[RefinedChainProvenance],
) -> crate::error::Result<Option<MinimumClearanceWitness>> {
    let site_points = sites.points_vec()?;
    if site_points.len() < 2 {
        return Ok(None);
    }
    let segments = indexed_segments(support)?;
    let mut lineages: Vec<SupportChainLineage> = Vec::new();
    let mut refined_index = 0_usize;
    let mut segment_cursor = 0_usize;
    let mut mismatch = false;
    let stop = sites.any_paired_chain(
        support,
        &mut |chain_index, site_start, site_chain, support_chain| {
            let refined_lineage_index = if let Some(lineage) = refined_chains
                .get(refined_index)
                .filter(|lineage| lineage.chain_index == chain_index)
            {
                refined_index = refined_index
                    .checked_add(1)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
                let piece_ends = &lineage.piece_ends;
                if piece_ends.len() != site_chain.len().saturating_sub(1)
                    || piece_ends.last().copied().unwrap_or(0)
                        != support_chain.len().saturating_sub(1)
                {
                    mismatch = true;
                    return Ok(true);
                }
                Some(refined_index.wrapping_sub(1))
            } else {
                if site_chain.len() != support_chain.len() {
                    mismatch = true;
                    return Ok(true);
                }
                None
            };
            let support_segment_count = support_chain.len().saturating_sub(1);
            if support_segment_count > 0 {
                lineages.push(SupportChainLineage {
                    first_segment: segment_cursor,
                    segment_count: support_segment_count,
                    site_start,
                    refined_lineage_index,
                });
            }
            segment_cursor = segment_cursor
                .checked_add(support_segment_count)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            Ok(false)
        },
    )?;
    if stop || mismatch || segment_cursor != segments.len() || refined_index != refined_chains.len()
    {
        return Err(crate::geometry::GeometryErrorKind::message(
            "prepared minimum-clearance support lost its source-chain correspondence",
        ));
    }

    let keep = |point: Point, segment_index: usize, _segment: Segment| {
        let chain_index = lineages
            .partition_point(|chain| chain.first_segment <= segment_index)
            .saturating_sub(1);
        let Some(chain) = lineages.get(chain_index) else {
            return true;
        };
        let local_segment = segment_index.wrapping_sub(chain.first_segment);
        if local_segment >= chain.segment_count {
            return true;
        }
        let original_edge = chain.refined_lineage_index.map_or(local_segment, |index| {
            refined_chains
                .get(index)
                .expect("refined-chain lineage index was admitted")
                .piece_ends
                .partition_point(|&piece_end| piece_end <= local_segment)
        });
        let start_index = chain.site_start.wrapping_add(original_edge);
        let end_index = start_index.wrapping_add(1);
        let start = *site_points
            .get(start_index)
            .expect("paired line-chain site start belongs to the site coordinate slice");
        let end = *site_points
            .get(end_index)
            .expect("paired line-chain site end belongs to the site coordinate slice");
        !same_point(point, start) && !same_point(point, end)
    };
    let witness = minimum_clearance_candidates(support, &site_points, &segments, &keep)?;
    let Some((mut plan, distance_squared)) = witness else {
        return Ok(None);
    };
    let has_segment_witness = plan
        .iter()
        .any(|vertex| matches!(vertex, VertexProvenance::OnSegment { .. }));
    let points = if has_segment_witness {
        let support_points = support.points_vec()?;
        let offset = site_points.len();
        for vertex in &mut plan {
            if let VertexProvenance::OnSegment { i, j, .. } = vertex {
                *i = i
                    .checked_add(offset)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
                *j = j
                    .checked_add(offset)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
            }
        }
        let total = offset.wrapping_add(support_points.len());
        let mut points = Vec::with_capacity(total);
        points.extend(site_points);
        points.extend(support_points);
        points
    } else {
        site_points
    };
    Ok(Some(MinimumClearanceWitness {
        plan,
        distance_squared,
        points,
    }))
}

struct SupportChainLineage {
    first_segment: usize,
    segment_count: usize,
    site_start: usize,
    refined_lineage_index: Option<usize>,
}

fn minimum_clearance_candidates(
    shape: &Shape,
    points: &[Point],
    segments: &[IndexedSegment],
    keep: &impl Fn(Point, usize, Segment) -> bool,
) -> crate::error::Result<Option<([VertexProvenance; 2], f64)>> {
    let witness = if points
        .len()
        .saturating_mul(points.len().max(segments.len()))
        >= SEGMENT_INDEX_MIN_PAIRS
    {
        minimum_clearance_witness_indexed(shape, points, segments, keep)?
    } else {
        minimum_clearance_witness_brute(points, segments, keep)?
    };
    Ok(witness)
}

/// Indexed nearest-query path (large multipoints / dense linework).
fn minimum_clearance_witness_indexed(
    shape: &Shape,
    points: &[Point],
    segments: &[IndexedSegment],
    keep: &impl Fn(Point, usize, Segment) -> bool,
) -> crate::error::Result<Option<([VertexProvenance; 2], f64)>> {
    let vertex_index = PointSetIndex::build(points.iter().copied());
    let mut vertex_witness = None;
    let mut clearance_squared = f64::INFINITY;
    for (point_index, point) in points.iter().enumerate() {
        if let Some((other, raw_sq)) = vertex_index.nearest_other(*point)
            && let Some(distance_squared) =
                admit_clearance_squared(raw_sq, clearance_squared, || {
                    Ok(point_distance(*point, other))
                })?
        {
            clearance_squared = distance_squared;
            vertex_witness = Some((point_index, other, distance_squared));
        }
    }
    let mut witness = vertex_witness.map(|(point_index, other, distance_squared)| {
        let key = PointKey::new(other);
        let other_index = points
            .iter()
            .position(|point| PointKey::new(*point) == key)
            .expect("the selected indexed vertex belongs to the source points");
        (
            [
                VertexProvenance::Input(point_index),
                VertexProvenance::Input(other_index),
            ],
            distance_squared,
        )
    });
    let linework = PreparedLinework::build_all(shape)?;
    let bvh = FacetBvh::build_for_clearance(&linework);
    if bvh.is_none() && !segments.is_empty() {
        return Err(crate::error::ResourceError::SizeOverflow.into());
    }
    let mut stack = Vec::new();
    for (point_index, point) in points.iter().enumerate() {
        // Express the clearance fold's strict bound as an inclusive predecessor.
        // An untrustworthy square cannot prune candidate distances.
        let probe_best = (if clearance_squared.is_normal() {
            clearance_squared
        } else {
            f64::INFINITY
        })
        .next_down();
        let mut nearest = None;
        if let Some(bvh) = &bvh {
            if bvh.has_slabs() {
                bvh.nearest_point_with_stack_and_ordinal::<true, true, true>(
                    &linework,
                    *point,
                    probe_best,
                    f64::INFINITY,
                    keep,
                    &mut stack,
                    &mut nearest,
                )?;
            } else {
                bvh.nearest_point_with_stack_and_ordinal::<false, true, true>(
                    &linework,
                    *point,
                    probe_best,
                    f64::INFINITY,
                    keep,
                    &mut stack,
                    &mut nearest,
                )?;
            }
        }
        if let Some(nearest) = nearest
            && let Some(seg_i) = nearest.segment_ordinal()
            && let Some(distance_squared) =
                admit_clearance_squared(nearest.distance_squared, clearance_squared, || {
                    point_segment_distance(
                        *point,
                        segments
                            .get(seg_i)
                            .expect("segment ordinals come from the indexed segment slice")
                            .xy,
                    )
                })?
        {
            clearance_squared = distance_squared;
            let segment = *segments
                .get(seg_i)
                .expect("segment ordinals come from the indexed segment slice");
            witness = Some((
                [
                    VertexProvenance::Input(point_index),
                    VertexProvenance::OnSegment {
                        i: segment.start,
                        j: segment.end,
                        projection: segment_projection(*point, segment.xy)?,
                    },
                ],
                distance_squared,
            ));
        }
    }
    Ok(witness)
}

/// Brute O(n²) path for small multipoints / sparse linework.
fn minimum_clearance_witness_brute(
    points: &[Point],
    segments: &[IndexedSegment],
    keep: &impl Fn(Point, usize, Segment) -> bool,
) -> crate::error::Result<Option<([VertexProvenance; 2], f64)>> {
    let mut witness = None;
    let mut clearance_squared = f64::INFINITY;
    for (left_index, left) in points.iter().enumerate() {
        let later_points = points
            .get(left_index.wrapping_add(1)..)
            .expect("an enumerated point precedes the end of its source slice");
        for (offset, right) in later_points.iter().enumerate() {
            if same_point(*left, *right) {
                continue;
            }
            // Normal squares compare in squared space; false-zero / subnormal
            // residuals recompute in distance space (1.5e-162 not dropped,
            // 2.2e-162 not quantized).
            let Some(distance_squared) = admit_clearance_from_try(
                Ok(try_point_distance_squared(*left, *right)),
                clearance_squared,
                || Ok(point_distance(*left, *right)),
            )?
            else {
                continue;
            };
            clearance_squared = distance_squared;
            witness = Some((
                [
                    VertexProvenance::Input(left_index),
                    VertexProvenance::Input(left_index.wrapping_add(1).wrapping_add(offset)),
                ],
                distance_squared,
            ));
        }
    }
    for (point_index, point) in points.iter().enumerate() {
        for (segment_index, segment) in segments.iter().enumerate() {
            if !keep(*point, segment_index, segment.xy) {
                continue;
            }
            let Some(distance_squared) = admit_clearance_from_try(
                try_point_segment_distance_squared(*point, segment.xy),
                clearance_squared,
                || point_segment_distance(*point, segment.xy),
            )?
            else {
                continue;
            };
            clearance_squared = distance_squared;
            witness = Some((
                [
                    VertexProvenance::Input(point_index),
                    VertexProvenance::OnSegment {
                        i: segment.start,
                        j: segment.end,
                        projection: segment_projection(*point, segment.xy)?,
                    },
                ],
                distance_squared,
            ));
        }
    }
    Ok(witness)
}

/// True when candidate distance `d` is strictly closer than the current
/// clearance tracked as a (possibly untrustworthy) squared residual.
fn clearance_beats_distance(clearance_squared: f64, d: f64) -> bool {
    if clearance_squared.is_normal() {
        d < clearance_squared.sqrt()
    } else if !clearance_squared.is_finite() {
        // Still at +inf sentinel.
        true
    } else {
        // Prior best was zero/subnormal placeholder — compare via sqrt of the
        // placeholder when it is positive finite.
        clearance_squared <= 0.0 || d < clearance_squared.sqrt()
    }
}

/// Admit a candidate clearance residual: normal positive squares compare in
/// squared space; false-zero / subnormal residuals recompute via `honest_d`.
fn admit_clearance_squared(
    raw_sq: f64,
    clearance_squared: f64,
    honest_d: impl FnOnce() -> crate::error::Result<f64>,
) -> crate::error::Result<Option<f64>> {
    if squared_norm_is_trustworthy(raw_sq, false) {
        Ok((raw_sq > 0.0 && raw_sq < clearance_squared).then_some(raw_sq))
    } else {
        let d = honest_d()?;
        Ok((d > 0.0 && clearance_beats_distance(clearance_squared, d)).then_some(d * d))
    }
}

/// Brute-path twin of [`admit_clearance_squared`] over a `try_*` Option square.
fn admit_clearance_from_try(
    tried: crate::error::Result<Option<f64>>,
    clearance_squared: f64,
    honest_d: impl FnOnce() -> crate::error::Result<f64>,
) -> crate::error::Result<Option<f64>> {
    if let Some(sq) = tried? {
        admit_clearance_squared(sq, clearance_squared, honest_d)
    } else {
        let d = honest_d()?;
        Ok((d > 0.0 && clearance_beats_distance(clearance_squared, d)).then_some(d * d))
    }
}

#[derive(Clone, Copy)]
struct IndexedSegment {
    xy: Segment,
    start: usize,
    end: usize,
}

fn indexed_segments(shape: &Shape) -> crate::error::Result<Vec<IndexedSegment>> {
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "segment and coordinate ordinals remain within the admitted source shape's usize-sized coordinate inventory"
    )]
    fn line_segments_with_indices<C: Coordinates + ?Sized>(
        coords: &C,
        cursor: &mut usize,
        out: &mut Vec<IndexedSegment>,
    ) {
        let base = *cursor;
        for index in 0..coords.coord_count().saturating_sub(1) {
            let start = coords.nth_coord(index);
            let end = coords.nth_coord(index + 1);
            out.push(IndexedSegment {
                xy: Segment {
                    start: start.xy(),
                    end: end.xy(),
                },
                start: base + index,
                end: base + index + 1,
            });
        }
        *cursor += coords.coord_count();
    }

    let segment_count = ShapeCountQuery::default().segments(shape)?;
    let mut out = Vec::with_capacity(segment_count);
    let mut cursor = 0_usize;
    shape.for_each_component(&mut |component| match component {
        // A puntal member owns one coordinate index and no segment.
        Component::Point(_) => {
            cursor = cursor.wrapping_add(1);
        },
        Component::LineString(line) => line_segments_with_indices(line, &mut cursor, &mut out),
        Component::Polygon(polygon) => {
            for ring in polygon.rings() {
                line_segments_with_indices(ring, &mut cursor, &mut out);
            }
        },
    });
    Ok(out)
}

pub(crate) fn line_is_closed<C: Coordinates + ?Sized>(points: &C) -> bool {
    line_is_closed_in_domain(points, crate::geometry::TopologyDomain::Cartesian)
}

pub(crate) fn line_is_closed_in_domain<C: Coordinates + ?Sized>(
    points: &C,
    domain: crate::geometry::TopologyDomain,
) -> bool {
    let len = points.coord_count();
    len >= 2
        && crate::geometry::VertexKey::new(points.nth_coord(0), domain)
            == crate::geometry::VertexKey::new(points.nth_coord(len.wrapping_sub(1)), domain)
}

/// `LineString` CCW test (the `Shape::is_ccw` line arm): a closed ring
/// (≥4 vertices, coincident endpoints) with positive signed area. Column-native
/// so the packed-`Lines` array path runs it per window with no per-row `Shape`.
pub(crate) fn line_is_ccw<C: Coordinates + ?Sized>(points: &C) -> bool {
    points.coord_count() >= 4
        && points
            .first_coord()
            .zip(points.last_coord())
            .is_some_and(|(first, last)| same_point(first, last))
        && ring_winding(points).is_ccw()
}

/// Column-native validity for one `LineString` window (matches
/// `validate_line`).
pub(crate) fn line_is_valid(coords: &CoordSeq) -> bool {
    validate_line(coords, "line string").is_none()
}

/// Column-native validity for one packed `Polygon` row (matches
/// `Polygon::validate`).
pub(crate) fn polygon_is_valid(polygon: &Polygon) -> crate::error::Result<bool> {
    Ok(polygon.validate("polygon", "$")?.is_none())
}

pub(crate) fn line_is_simple(points: &CoordSeq) -> crate::error::Result<bool> {
    // Strictly monotone ordinates are an O(n) simplicity certificate:
    // non-adjacent segments occupy disjoint open extents along the axis,
    // and adjacent segments cannot fold back into a collinear overlap —
    // no self-contact is possible. Smooth sampled tracks (the common
    // simple case) certify here without staging the candidate engine;
    // non-monotone input fails within a few comparisons.
    if strictly_monotone(points.xs()) || strictly_monotone(points.ys()) {
        return Ok(true);
    }
    let mut chains = LineworkChains::default();
    let _ = chains.push_line(points)?;
    indexed_segments_are_simple(&chains)
}

/// Strictly increasing or strictly decreasing (`NaN` fails both — the
/// caller's general path owns malformed input).
pub(crate) fn strictly_monotone(values: &[f64]) -> bool {
    values.is_sorted_by(|left, right| left < right)
        || values.is_sorted_by(|left, right| left > right)
}

pub(crate) fn multiline_is_simple<L: AsRef<CoordSeq>>(lines: &[L]) -> crate::error::Result<bool> {
    let mut chains = LineworkChains::default();
    for line in lines {
        let _ = chains.push_line(line.as_ref())?;
    }
    indexed_segments_are_simple(&chains)
}

#[cfg(test)]
mod prune_dangles_tests {
    use super::*;

    fn prune_dangles_reference(segments: &mut Vec<Segment>) {
        loop {
            let mut degree: HashMap<PointKey, usize> =
                HashMap::with_capacity(segments.len().saturating_mul(2));
            for segment in segments.iter() {
                let start_degree = degree.entry(PointKey::new(segment.start)).or_insert(0);
                *start_degree = start_degree.saturating_add(1);
                let end_degree = degree.entry(PointKey::new(segment.end)).or_insert(0);
                *end_degree = end_degree.saturating_add(1);
            }
            let before = segments.len();
            segments.retain(|segment| {
                degree[&PointKey::new(segment.start)] > 1 && degree[&PointKey::new(segment.end)] > 1
            });
            if segments.len() == before {
                return;
            }
        }
    }

    fn segment_key(segment: Segment) -> (u64, u64, u64, u64) {
        let start = PointKey::new(segment.start);
        let end = PointKey::new(segment.end);
        (start.x, start.y, end.x, end.y)
    }

    fn assert_same_pruned_set(input: Vec<Segment>) {
        let mut fast = input.clone();
        let mut reference = input;
        prune_dangles(&mut fast);
        prune_dangles_reference(&mut reference);
        let mut fast_keys: Vec<_> = fast.into_iter().map(segment_key).collect();
        let mut reference_keys: Vec<_> = reference.into_iter().map(segment_key).collect();
        fast_keys.sort_unstable();
        reference_keys.sort_unstable();
        assert_eq!(fast_keys, reference_keys);
    }

    #[test]
    #[expect(clippy::many_single_char_names, reason = "standard math notation")]
    fn prune_dangles_matches_iterative_rebuild() {
        let a = XY::new(0.0, 0.0);
        let b = XY::new(1.0, 0.0);
        let c = XY::new(2.0, 0.0);
        let d = XY::new(3.0, 0.0);
        let e = XY::new(4.0, 0.0);
        let f = XY::new(5.0, 0.0);
        let spur = vec![
            Segment { start: a, end: b },
            Segment { start: b, end: c },
            Segment { start: c, end: d },
            Segment { start: d, end: e },
            Segment { start: e, end: f },
            Segment {
                start: f,
                end: XY::new(6.0, 0.0),
            },
        ];
        assert_same_pruned_set(spur);

        let hub = vec![
            Segment { start: a, end: b },
            Segment { start: b, end: c },
            Segment { start: c, end: d },
            Segment {
                start: b,
                end: XY::new(1.0, 1.0),
            },
            Segment {
                start: b,
                end: XY::new(1.0, -1.0),
            },
            Segment {
                start: c,
                end: XY::new(2.0, 1.0),
            },
        ];
        assert_same_pruned_set(hub);

        let ring = vec![
            Segment { start: a, end: b },
            Segment { start: b, end: c },
            Segment { start: c, end: d },
            Segment { start: d, end: a },
            Segment {
                start: b,
                end: XY::new(1.0, 2.0),
            },
        ];
        assert_same_pruned_set(ring);
    }
}

#[cfg(test)]
mod clearance_index_tests {
    use super::*;
    use crate::geometry::LineSeq;

    fn line(points: &[Point]) -> Shape {
        Shape::LineString(
            LineSeq::try_new(CoordSeq::try_from_points(points).unwrap())
                .expect("test line is valid"),
        )
    }

    fn indexed_line() -> Shape {
        let points: Vec<Point> = (0_u8..64)
            .map(|index| Point::new_raw_xy(f64::from(index), 0.0))
            .collect();
        line(&points)
    }

    #[test]
    fn indexed_clearance_matches_brute_force() {
        let shape = indexed_line();
        let points = shape.points_vec().unwrap();
        let segments = indexed_segments(&shape).unwrap();
        let keep = |point: Point, _: usize, segment: Segment| {
            !same_point(point, segment.start) && !same_point(point, segment.end)
        };
        assert!(points.len() * points.len().max(segments.len()) >= SEGMENT_INDEX_MIN_PAIRS);
        let (indexed_plan, indexed_distance) =
            minimum_clearance_witness_indexed(&shape, &points, &segments, &keep)
                .unwrap()
                .expect("finite indexed clearance");
        let (brute_plan, brute_distance) =
            minimum_clearance_witness_brute(&points, &segments, &keep)
                .unwrap()
                .expect("finite brute-force clearance");
        assert_eq!(indexed_distance, brute_distance);
        assert_eq!(
            crate::geometry::emit_from_original(&points, &indexed_plan).unwrap(),
            crate::geometry::emit_from_original(&points, &brute_plan).unwrap()
        );
    }

    #[test]
    fn refined_support_keeps_inserted_samples_out_of_clearance_site_candidates() {
        let sites = Shape::geometry_collection(vec![
            line(&[Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(10.0, 0.0)]),
            Shape::Point(Point::new_raw_xy(5.0, 1.0)),
        ])
        .unwrap();
        let support = Shape::geometry_collection(vec![
            line(&[
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(5.0, 0.0),
                Point::new_raw_xy(10.0, 0.0),
            ]),
            Shape::Point(Point::new_raw_xy(5.0, 1.0)),
        ])
        .unwrap();
        let refined_chains = [RefinedChainProvenance {
            chain_index: 0,
            piece_ends: vec![2],
        }];

        let witness = minimum_clearance_witness_with_support(&sites, &support, &refined_chains)
            .unwrap()
            .expect("source line has a finite clearance");
        assert_eq!(witness.distance_squared, 1.0);
        assert!(matches!(witness.plan[0], VertexProvenance::Input(2)));
        assert!(matches!(
            witness.plan[1],
            VertexProvenance::OnSegment { .. }
        ));
        assert_eq!(
            crate::geometry::emit_from_original(&witness.points, &witness.plan).unwrap(),
            [Point::new_raw_xy(5.0, 1.0), Point::new_raw_xy(5.0, 0.0)]
        );

        let subdivided_site_witness = minimum_clearance_witness(&support)
            .unwrap()
            .expect("subdivided line has a finite clearance");
        assert!(matches!(
            subdivided_site_witness.plan[0],
            VertexProvenance::Input(_)
        ));
        assert!(matches!(
            subdivided_site_witness.plan[1],
            VertexProvenance::Input(_)
        ));
    }

    #[test]
    fn indexed_refined_support_preserves_original_clearance_sites() {
        let mut source_line = Vec::with_capacity(64);
        let mut support_line = Vec::with_capacity(65);
        for x in 0..64 {
            let point = Point::new_raw_xy(f64::from(x), 0.0);
            source_line.push(point);
            support_line.push(point);
            if x == 31 {
                support_line.push(Point::new_raw_xy(31.5, 0.0));
            }
        }
        let sites = Shape::geometry_collection(vec![
            line(&source_line),
            Shape::Point(Point::new_raw_xy(31.5, 0.25)),
        ])
        .unwrap();
        let support = Shape::geometry_collection(vec![
            line(&support_line),
            Shape::Point(Point::new_raw_xy(31.5, 0.25)),
        ])
        .unwrap();
        let mut piece_ends = Vec::with_capacity(source_line.len() - 1);
        let mut end = 0_usize;
        for edge in 0..source_line.len() - 1 {
            end += if edge == 31 { 2 } else { 1 };
            piece_ends.push(end);
        }
        let refined_chains = [RefinedChainProvenance {
            chain_index: 0,
            piece_ends,
        }];

        let witness = minimum_clearance_witness_with_support(&sites, &support, &refined_chains)
            .unwrap()
            .expect("the point and line have a finite clearance");
        assert_eq!(witness.distance_squared, 0.0625);
        assert!(matches!(witness.plan[0], VertexProvenance::Input(64)));
        assert!(matches!(
            witness.plan[1],
            VertexProvenance::OnSegment { .. }
        ));

        let inserted_site_witness = minimum_clearance_witness(&support)
            .unwrap()
            .expect("the subdivided line has a finite clearance");
        assert!(matches!(
            inserted_site_witness.plan[0],
            VertexProvenance::Input(_)
        ));
        assert!(matches!(
            inserted_site_witness.plan[1],
            VertexProvenance::Input(_)
        ));
    }
}
