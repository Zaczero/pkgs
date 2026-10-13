#[cfg(test)]
use ahash::HashMapExt as _;

#[cfg(test)]
use crate::geometry::HashMap;
#[cfg(test)]
use crate::geometry::PointKey;
use crate::geometry::ordinates::{RootId, RootRange, RootRequest};
use crate::geometry::predicates::{LineworkChains, indexed_segments_are_simple, validate_line};
use crate::geometry::segments::projection::segment_projection;
use crate::geometry::{
    ClearanceFacetBvh, ClearancePointQuery, Component, CoordSeq, CoordinateAxes, Coordinates,
    ExactRational, FacetBvh, Ordering, Point, PointSetIndex, Polygon, PreparedLinework,
    RefinedChainProvenance, SEGMENT_INDEX_MIN_PAIRS, Segment, Shape, ShapeCountQuery,
    SquaredDistanceKey, VertexProvenance, XY, point_distance, point_distance_squared,
    point_in_ccw_triangle, point_pair_distance_key, point_segment_distance,
    point_segment_exact_distance_key, ring_winding, same_point,
};
use crate::work::Unmetered;

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
/// vertex or segment projection)` — or `None` when the clearance is infinite.
/// Both scalar and witness-line surfaces finish this same selected source.
#[derive(Clone, Debug)]
pub(crate) struct MinimumClearanceWitness {
    pub(crate) plan: [VertexProvenance; 2],
    /// The vertices the plan indexes; witness emission and source-continuum
    /// measurement both read them.
    pub(crate) points: Vec<Point>,
}

impl MinimumClearanceWitness {
    pub(crate) fn planar_distance(&self) -> crate::error::Result<f64> {
        match self.plan.as_slice() {
            [
                VertexProvenance::Input(left),
                VertexProvenance::Input(right),
            ] => Ok(point_distance(
                *self
                    .points
                    .get(*left)
                    .expect("clearance point provenance belongs to the source points"),
                *self
                    .points
                    .get(*right)
                    .expect("clearance point provenance belongs to the source points"),
            )),
            [
                VertexProvenance::Input(point),
                VertexProvenance::OnSegment { i, j, .. },
            ] => {
                let point = *self
                    .points
                    .get(*point)
                    .expect("clearance point provenance belongs to the source points");
                let start = *self
                    .points
                    .get(*i)
                    .expect("clearance segment provenance belongs to the source points");
                let end = *self
                    .points
                    .get(*j)
                    .expect("clearance segment provenance belongs to the source points");
                point_segment_distance(point, Segment {
                    start: start.xy(),
                    end: end.xy(),
                })
            },
            _ => unreachable!("clearance witnesses start at an input point"),
        }
    }
}

#[derive(Clone, Copy)]
struct MinimumClearanceCandidate {
    source: ClearanceSource,
    distance_key: SquaredDistanceKey,
}

#[derive(Clone, Copy)]
enum ClearanceSource {
    PointPair { first: usize, second: usize },
    PointSegment { point: usize, segment: usize },
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
    let Some(candidate) = minimum_clearance_candidates(shape, &points, &segments, &keep)? else {
        return Ok(None);
    };
    let plan = finish_clearance_candidate(&candidate, &points, &segments)?;
    Ok(Some(MinimumClearanceWitness { plan, points }))
}

pub(crate) fn minimum_clearance_witness_with_support(
    sites: &Shape,
    support: &Shape,
    refined_chains: &[RefinedChainProvenance],
    resolve_source_ordinates: bool,
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
        let original_edge = chain.original_edge(local_segment, refined_chains);
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
    let Some(candidate) = minimum_clearance_candidates(support, &site_points, &segments, &keep)?
    else {
        return Ok(None);
    };
    let mut plan = finish_clearance_candidate(&candidate, &site_points, &segments)?;
    if resolve_source_ordinates
        && let ClearanceSource::PointSegment { segment, .. } = candidate.source
    {
        let [_, second] = &mut plan;
        let VertexProvenance::OnSegment {
            projection,
            root_request,
            ..
        } = second
        else {
            return Err(crate::geometry::GeometryErrorKind::Invalid(
                "clearance point-segment candidate lost its support projection".into(),
            )
            .into());
        };
        *root_request = root_request_for_support_segment(
            segment,
            projection,
            sites.axes(),
            &site_points,
            &lineages,
            refined_chains,
        )?;
    }
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
    Ok(Some(MinimumClearanceWitness { plan, points }))
}

struct SupportChainLineage {
    first_segment: usize,
    segment_count: usize,
    site_start: usize,
    refined_lineage_index: Option<usize>,
}

impl SupportChainLineage {
    fn original_edge(
        &self,
        local_segment: usize,
        refined_chains: &[RefinedChainProvenance],
    ) -> usize {
        self.refined_lineage_index.map_or(local_segment, |index| {
            refined_chains
                .get(index)
                .expect("refined-chain lineage index was admitted")
                .piece_ends
                .partition_point(|&piece_end| piece_end <= local_segment)
        })
    }
}

fn root_request_for_support_segment(
    segment_index: usize,
    projection: &crate::geometry::SegmentProjection,
    axes: CoordinateAxes,
    site_points: &[Point],
    lineages: &[SupportChainLineage],
    refined_chains: &[RefinedChainProvenance],
) -> crate::error::Result<Option<RootRequest>> {
    let chain_index = lineages
        .partition_point(|chain| chain.first_segment <= segment_index)
        .saturating_sub(1);
    let Some(chain) = lineages.get(chain_index) else {
        return Ok(None);
    };
    let local_segment = segment_index.wrapping_sub(chain.first_segment);
    if local_segment >= chain.segment_count {
        return Ok(None);
    }
    let original_edge = chain.original_edge(local_segment, refined_chains);
    let root_start_index = chain
        .site_start
        .checked_add(original_edge)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let root_end_index = root_start_index
        .checked_add(1)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let root_start = *site_points.get(root_start_index).ok_or_else(|| {
        crate::geometry::GeometryErrorKind::Invalid(
            "clearance support root start is outside the source points".into(),
        )
    })?;
    let root_end = *site_points.get(root_end_index).ok_or_else(|| {
        crate::geometry::GeometryErrorKind::Invalid(
            "clearance support root end is outside the source points".into(),
        )
    })?;
    let has_eligible_axis = (axes.has_z() && root_start.z().is_some() && root_end.z().is_some())
        || (axes.has_m() && root_start.m().is_some() && root_end.m().is_some());
    if !has_eligible_axis {
        return Ok(None);
    }

    let root = RootId::from_index(
        u32::try_from(root_start_index).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
    );
    let (source_start, source_end) = if let Some(refined_index) = chain.refined_lineage_index {
        let refined = refined_chains
            .get(refined_index)
            .expect("refined-chain lineage index was admitted");
        let (start, end) = refined
            .source_interval(original_edge, local_segment)
            .ok_or_else(|| {
                crate::geometry::GeometryErrorKind::Invalid(
                    "clearance support segment has no exact source parameter interval".into(),
                )
            })?;
        (start, end)
    } else {
        (0.0, 1.0)
    };
    let mut work = Unmetered;
    let range = RootRange::from_mapped_with_work(
        root,
        ExactRational::from_f64(source_start),
        ExactRational::from_f64(source_end),
        ExactRational::zero(),
        ExactRational::one(),
        &mut work,
    )?;
    let parameter = projection.exact_fraction()?;
    range
        .request_at_with_work(&parameter, &mut work)?
        .map_or_else(
            || {
                Err(crate::geometry::GeometryErrorKind::Invalid(
                    "clearance support projection is outside its exact source interval".into(),
                )
                .into())
            },
            |request| Ok(Some(request)),
        )
}

fn minimum_clearance_candidates(
    shape: &Shape,
    points: &[Point],
    segments: &[IndexedSegment],
    keep: &impl Fn(Point, usize, Segment) -> bool,
) -> crate::error::Result<Option<MinimumClearanceCandidate>> {
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
) -> crate::error::Result<Option<MinimumClearanceCandidate>> {
    let mut best: Option<MinimumClearanceCandidate> = None;
    {
        let vertex_index = PointSetIndex::build_clearance_with_ordinals(points);
        let mut vertex_query: ClearancePointQuery<'_> = vertex_index.clearance_query();
        for (point_index, point) in points.iter().enumerate() {
            let incumbent = best.as_ref().map(|candidate| &candidate.distance_key);
            if let Some((other, distance_key)) =
                vertex_query.nearest_clearance_neighbor(*point, incumbent)?
            {
                let (first, second) = if point_index < other {
                    (point_index, other)
                } else {
                    (other, point_index)
                };
                consider_clearance_candidate(&mut best, &MinimumClearanceCandidate {
                    source: ClearanceSource::PointPair { first, second },
                    distance_key,
                })?;
            }
        }
    }
    if segments.is_empty() {
        return Ok(best);
    }
    let linework = PreparedLinework::build_all(shape)?;
    let bvh: Option<ClearanceFacetBvh> = FacetBvh::build_for_clearance(&linework)?;
    if bvh.is_none() {
        return Err(crate::error::ResourceError::SizeOverflow.into());
    }
    let mut stack = Vec::new();
    for (point_index, point) in points.iter().enumerate() {
        let incumbent = best.as_ref().map(|candidate| &candidate.distance_key);
        if let Some(bvh) = &bvh
            && let Some((segment, distance_key)) = bvh
                .nearest_clearance_segment_with_stack_and_ordinal(
                    &linework, *point, keep, incumbent, &mut stack,
                )?
        {
            consider_clearance_candidate(&mut best, &MinimumClearanceCandidate {
                source: ClearanceSource::PointSegment {
                    point: point_index,
                    segment,
                },
                distance_key,
            })?;
        }
    }
    Ok(best)
}

/// Exhaustive source-key path for small multipoints / sparse linework.
fn minimum_clearance_witness_brute(
    points: &[Point],
    segments: &[IndexedSegment],
    keep: &impl Fn(Point, usize, Segment) -> bool,
) -> crate::error::Result<Option<MinimumClearanceCandidate>> {
    let mut best = None;
    for (left_index, left) in points.iter().enumerate() {
        let later_points = points
            .get(left_index.wrapping_add(1)..)
            .expect("an enumerated point precedes the end of its source slice");
        for (offset, right) in later_points.iter().enumerate() {
            if same_point(*left, *right) {
                continue;
            }
            let right_index = left_index.wrapping_add(1).wrapping_add(offset);
            consider_clearance_candidate(&mut best, &MinimumClearanceCandidate {
                source: ClearanceSource::PointPair {
                    first: left_index,
                    second: right_index,
                },
                distance_key: point_pair_distance_key(left.xy(), right.xy()),
            })?;
        }
    }
    for (point_index, point) in points.iter().enumerate() {
        for (segment_index, segment) in segments.iter().enumerate() {
            if !keep(*point, segment_index, segment.xy) {
                continue;
            }
            let distance_key = point_segment_exact_distance_key(point.xy(), segment.xy);
            if distance_key.is_exact_zero()? {
                continue;
            }
            consider_clearance_candidate(&mut best, &MinimumClearanceCandidate {
                source: ClearanceSource::PointSegment {
                    point: point_index,
                    segment: segment_index,
                },
                distance_key,
            })?;
        }
    }
    Ok(best)
}

fn consider_clearance_candidate(
    best: &mut Option<MinimumClearanceCandidate>,
    candidate: &MinimumClearanceCandidate,
) -> crate::error::Result<()> {
    let replace = match best.as_ref() {
        None => true,
        Some(best) => candidate
            .distance_key
            .cmp_distance(&best.distance_key)?
            .is_lt(),
    };
    if replace {
        *best = Some(*candidate);
    }
    Ok(())
}

fn finish_clearance_candidate(
    candidate: &MinimumClearanceCandidate,
    points: &[Point],
    segments: &[IndexedSegment],
) -> crate::error::Result<[VertexProvenance; 2]> {
    match candidate.source {
        ClearanceSource::PointPair { first, second } => Ok([
            VertexProvenance::Input(first),
            VertexProvenance::Input(second),
        ]),
        ClearanceSource::PointSegment { point, segment } => {
            let segment = *segments
                .get(segment)
                .expect("clearance segment ordinal belongs to the indexed source segments");
            Ok([
                VertexProvenance::Input(point),
                VertexProvenance::OnSegment {
                    i: segment.start,
                    j: segment.end,
                    projection: segment_projection(
                        *points
                            .get(point)
                            .expect("clearance point ordinal belongs to the source points"),
                        segment.xy,
                    )?,
                    root_request: None,
                },
            ])
        },
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
    use pyo3::prelude::{Bound, PyModule, Python};
    use pyo3::types::{PyAnyMethods as _, PyList, PyListMethods as _};

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

    fn assert_indexed_matches_exact_exhaustive(shape: &Shape) -> MinimumClearanceCandidate {
        let points = shape.points_vec().unwrap();
        let segments = indexed_segments(shape).unwrap();
        let keep = |point: Point, _: usize, segment: Segment| {
            !same_point(point, segment.start) && !same_point(point, segment.end)
        };
        let estimated_pairs = points
            .len()
            .saturating_mul(points.len().max(segments.len()));
        assert!(estimated_pairs >= SEGMENT_INDEX_MIN_PAIRS);
        let indexed = minimum_clearance_witness_indexed(shape, &points, &segments, &keep)
            .unwrap()
            .expect("finite indexed clearance");
        let exhaustive = minimum_clearance_witness_brute(&points, &segments, &keep)
            .unwrap()
            .expect("finite exhaustive clearance");
        match (indexed.source, exhaustive.source) {
            (
                ClearanceSource::PointPair {
                    first: indexed_first,
                    second: indexed_second,
                },
                ClearanceSource::PointPair {
                    first: exhaustive_first,
                    second: exhaustive_second,
                },
            ) => assert_eq!(
                (indexed_first, indexed_second),
                (exhaustive_first, exhaustive_second),
            ),
            (
                ClearanceSource::PointSegment {
                    point: indexed_point,
                    segment: indexed_segment,
                },
                ClearanceSource::PointSegment {
                    point: exhaustive_point,
                    segment: exhaustive_segment,
                },
            ) => assert_eq!(
                (indexed_point, indexed_segment),
                (exhaustive_point, exhaustive_segment),
            ),
            _ => panic!("indexed and exhaustive clearance chose different source kinds"),
        }
        assert!(
            indexed
                .distance_key
                .cmp_distance(&exhaustive.distance_key)
                .unwrap()
                .is_eq()
        );
        let indexed_plan = finish_clearance_candidate(&indexed, &points, &segments).unwrap();
        let exhaustive_plan = finish_clearance_candidate(&exhaustive, &points, &segments).unwrap();
        assert_eq!(
            crate::geometry::emit_from_original(&points, &indexed_plan).unwrap(),
            crate::geometry::emit_from_original(&points, &exhaustive_plan).unwrap(),
        );
        indexed
    }

    #[test]
    fn indexed_clearance_matches_exact_exhaustive_oracle() {
        let shape = indexed_line();
        let candidate = assert_indexed_matches_exact_exhaustive(&shape);
        assert!(matches!(candidate.source, ClearanceSource::PointPair {
            first: 0,
            second: 1
        }));
    }

    #[test]
    fn indexed_clearance_returns_none_when_all_xy_candidates_are_aliases() {
        let points = vec![Point::new_raw_xy(-0.0, 0.0); 64];
        let shape = line(&points);
        let segments = indexed_segments(&shape).unwrap();
        let keep = |point: Point, _: usize, segment: Segment| {
            !same_point(point, segment.start) && !same_point(point, segment.end)
        };

        assert!(
            minimum_clearance_witness_indexed(&shape, &points, &segments, &keep)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn indexed_tiny_and_huge_clearance_keep_exact_point_pairs() {
        for spacing in [2.0_f64.powi(-700), 2.0_f64.powi(600)] {
            for count in [64_u16, 128, 256] {
                let points: Vec<_> = (0..count)
                    .map(|index| Point::new_raw_xy(f64::from(index) * spacing, 0.0))
                    .collect();
                let shape = line(&points);
                crate::geometry::CLEARANCE_POINT_BUILD_SITES.with(|work| work.set(0));
                crate::geometry::CLEARANCE_POINT_NODES.with(|work| work.set(0));
                crate::geometry::CLEARANCE_POINT_LEAVES.with(|work| work.set(0));
                crate::geometry::CLEARANCE_POINT_RECHECKS.with(|work| work.set(0));
                let candidate = assert_indexed_matches_exact_exhaustive(&shape);
                assert!(matches!(candidate.source, ClearanceSource::PointPair {
                    first: 0,
                    second: 1
                }));
                let build_sites =
                    crate::geometry::CLEARANCE_POINT_BUILD_SITES.with(std::cell::Cell::get);
                let nodes = crate::geometry::CLEARANCE_POINT_NODES.with(std::cell::Cell::get);
                let leaves = crate::geometry::CLEARANCE_POINT_LEAVES.with(std::cell::Cell::get);
                let rechecks = crate::geometry::CLEARANCE_POINT_RECHECKS.with(std::cell::Cell::get);
                eprintln!(
                    "point clearance: spacing={spacing:e}, {count} sites ({build_sites} build visits), {nodes} node bounds, {leaves} leaf bounds, {rechecks} rechecks",
                );
                assert!(
                    nodes > 0 && leaves > 0,
                    "point query did not visit the source tree"
                );
            }
        }
    }

    #[test]
    fn indexed_near_tie_keeps_exactly_closer_segment_over_rounded_point_pair() {
        let mut parts = vec![
            Shape::Point(Point::new_raw_xy(0.0, 0.0)),
            Shape::Point(Point::new_raw_xy(1.0, 2.0_f64.powi(-28))),
            line(&[
                Point::new_raw_xy(-10.0, -1.0),
                Point::new_raw_xy(10.0, -1.0),
            ]),
        ];
        parts.extend(
            (0..64).map(|index| {
                Shape::Point(Point::new_raw_xy(100.0 + 10.0 * f64::from(index), 100.0))
            }),
        );
        let shape = Shape::geometry_collection(parts).unwrap();

        let candidate = assert_indexed_matches_exact_exhaustive(&shape);

        assert!(matches!(candidate.source, ClearanceSource::PointSegment {
            point: 0,
            segment: 0
        }));
    }

    #[test]
    fn indexed_oblique_strip_bound_preserves_exact_exhaustive_winner() {
        let mut parts = vec![
            Shape::Point(Point::new_raw_xy(0.625, 0.625)),
            line(&[Point::new_raw_xy(-1.0, 1.0), Point::new_raw_xy(1.0, -1.0)]),
            Shape::Point(Point::new_raw_xy(100.0, 100.0)),
            Shape::Point(Point::new_raw_xy(101.0, 100.0)),
        ];
        parts.extend((0..60).map(|index| {
            Shape::Point(Point::new_raw_xy(1000.0 + 10.0 * f64::from(index), 1000.0))
        }));
        let shape = Shape::geometry_collection(parts).unwrap();
        let candidate = assert_indexed_matches_exact_exhaustive(&shape);
        let point_pair = point_pair_distance_key(XY::new(100.0, 100.0), XY::new(101.0, 100.0));

        assert!(matches!(candidate.source, ClearanceSource::PointSegment {
            point: 0,
            segment: 0
        }));
        assert!(
            candidate
                .distance_key
                .cmp_distance(&point_pair)
                .unwrap()
                .is_lt()
        );
    }

    #[test]
    fn indexed_zero_segment_candidate_does_not_hide_positive_clearance() {
        let mut parts = vec![
            Shape::Point(Point::new_raw_xy(0.0, 0.0)),
            line(&[Point::new_raw_xy(-10.0, 0.0), Point::new_raw_xy(10.0, 0.0)]),
            line(&[Point::new_raw_xy(-1.0, 1.0), Point::new_raw_xy(1.0, 1.0)]),
        ];
        parts.extend(
            (0..64).map(|index| {
                Shape::Point(Point::new_raw_xy(100.0 + 10.0 * f64::from(index), 100.0))
            }),
        );
        let shape = Shape::geometry_collection(parts).unwrap();

        let candidate = assert_indexed_matches_exact_exhaustive(&shape);

        assert!(matches!(candidate.source, ClearanceSource::PointSegment {
            point: 0,
            segment: 1
        }));
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
            source_parameters: None,
        }];

        let witness =
            minimum_clearance_witness_with_support(&sites, &support, &refined_chains, false)
                .unwrap()
                .expect("source line has a finite clearance");
        assert_eq!(witness.planar_distance().unwrap(), 1.0);
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
            source_parameters: None,
        }];

        let witness =
            minimum_clearance_witness_with_support(&sites, &support, &refined_chains, false)
                .unwrap()
                .expect("the point and line have a finite clearance");
        assert_eq!(witness.planar_distance().unwrap(), 0.25);
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

    fn public_clearance_module(py: Python<'_>) -> Bound<'_, PyModule> {
        let module = PyModule::new(py, "gometry._lib").unwrap();
        crate::lib(&module).unwrap();
        module
    }

    fn public_line_wkb(
        py: Python<'_>,
        module: &Bound<'_, PyModule>,
        coordinates: [(f64, f64); 2],
    ) -> Vec<u8> {
        let coordinates = PyList::new(py, coordinates).unwrap();
        module
            .getattr("LineString")
            .unwrap()
            .call1((&coordinates,))
            .unwrap()
            .call_method0("to_wkb")
            .unwrap()
            .extract()
            .unwrap()
    }

    #[test]
    fn compiled_public_tiny_clearance_uses_exact_source_selection() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let module = public_clearance_module(py);
            let spacing = 2.0_f64.powi(-700);
            let coordinates =
                PyList::new(py, [(0.0, 0.0), (5.0 * spacing, 0.0), (spacing, 0.0)]).unwrap();
            let shape = module
                .getattr("MultiPoint")
                .unwrap()
                .call1((&coordinates,))
                .unwrap();

            assert_eq!(
                shape
                    .call_method0("minimum_clearance")
                    .unwrap()
                    .extract::<f64>()
                    .unwrap(),
                spacing,
            );
            let witness = shape
                .call_method0("minimum_clearance_line")
                .unwrap()
                .call_method0("to_wkb")
                .unwrap()
                .extract::<Vec<u8>>()
                .unwrap();
            assert_eq!(
                witness,
                public_line_wkb(py, &module, [(0.0, 0.0), (spacing, 0.0)]),
            );
        });
    }

    #[test]
    fn compiled_public_huge_clearance_uses_exact_source_selection() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let module = public_clearance_module(py);
            let spacing = 2.0_f64.powi(600);
            let coordinates =
                PyList::new(py, [(0.0, 0.0), (5.0 * spacing, 0.0), (spacing, 0.0)]).unwrap();
            let shape = module
                .getattr("MultiPoint")
                .unwrap()
                .call1((&coordinates,))
                .unwrap();

            assert_eq!(
                shape
                    .call_method0("minimum_clearance")
                    .unwrap()
                    .extract::<f64>()
                    .unwrap(),
                spacing,
            );
            let witness = shape
                .call_method0("minimum_clearance_line")
                .unwrap()
                .call_method0("to_wkb")
                .unwrap()
                .extract::<Vec<u8>>()
                .unwrap();
            assert_eq!(
                witness,
                public_line_wkb(py, &module, [(0.0, 0.0), (spacing, 0.0)]),
            );
        });
    }

    #[test]
    fn compiled_public_near_tie_uses_exact_source_selection() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let module = public_clearance_module(py);
            let points = PyList::new(py, [(0.0, 0.0), (1.0, 2.0_f64.powi(-28))]).unwrap();
            let multipoint = module
                .getattr("MultiPoint")
                .unwrap()
                .call1((&points,))
                .unwrap();
            let line_coordinates = PyList::new(py, [(-10.0, -1.0), (10.0, -1.0)]).unwrap();
            let line = module
                .getattr("LineString")
                .unwrap()
                .call1((&line_coordinates,))
                .unwrap();
            let parts = PyList::empty(py);
            parts.append(multipoint).unwrap();
            parts.append(line).unwrap();
            for index in 0..64 {
                parts
                    .append(
                        module
                            .getattr("Point")
                            .unwrap()
                            .call1((100.0 + 10.0 * f64::from(index), 100.0))
                            .unwrap(),
                    )
                    .unwrap();
            }
            let shape = module
                .getattr("GeometryCollection")
                .unwrap()
                .call1((&parts,))
                .unwrap();

            assert_eq!(
                shape
                    .call_method0("minimum_clearance")
                    .unwrap()
                    .extract::<f64>()
                    .unwrap(),
                1.0,
            );
            let witness = shape
                .call_method0("minimum_clearance_line")
                .unwrap()
                .call_method0("to_wkb")
                .unwrap()
                .extract::<Vec<u8>>()
                .unwrap();
            assert_eq!(
                witness,
                public_line_wkb(py, &module, [(0.0, 0.0), (0.0, -1.0)]),
            );
        });
    }

    #[test]
    fn compiled_public_positive_clearance_skips_zero_segment_incidence() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let module = public_clearance_module(py);
            let parts = PyList::empty(py);
            parts
                .append(module.getattr("Point").unwrap().call1((0.0, 0.0)).unwrap())
                .unwrap();
            for coordinates in [[(-10.0, 0.0), (10.0, 0.0)], [(-1.0, 1.0), (1.0, 1.0)]] {
                let coordinates = PyList::new(py, coordinates).unwrap();
                parts
                    .append(
                        module
                            .getattr("LineString")
                            .unwrap()
                            .call1((&coordinates,))
                            .unwrap(),
                    )
                    .unwrap();
            }
            for index in 0..64 {
                parts
                    .append(
                        module
                            .getattr("Point")
                            .unwrap()
                            .call1((100.0 + 10.0 * f64::from(index), 100.0))
                            .unwrap(),
                    )
                    .unwrap();
            }
            let shape = module
                .getattr("GeometryCollection")
                .unwrap()
                .call1((&parts,))
                .unwrap();

            assert_eq!(
                shape
                    .call_method0("minimum_clearance")
                    .unwrap()
                    .extract::<f64>()
                    .unwrap(),
                1.0,
            );
            let witness = shape
                .call_method0("minimum_clearance_line")
                .unwrap()
                .call_method0("to_wkb")
                .unwrap()
                .extract::<Vec<u8>>()
                .unwrap();
            assert_eq!(
                witness,
                public_line_wkb(py, &module, [(0.0, 0.0), (0.0, 1.0)]),
            );
        });
    }
}
