use std::simd::cmp::SimdPartialOrd as _;
use std::simd::num::SimdFloat as _;

use crate::geometry::predicates::{LineworkChains, indexed_segments_are_simple, validate_line};
use crate::geometry::{
    CoordSeq, Coordinates, HashMap, HashMapExt as _, Ordering, Point, PointKey, PointSetIndex,
    Polygon, REDUCE_LANES, ReduceSimd, SEGMENT_INDEX_MIN_PAIRS, Segment, SegmentIndex, Shape,
    VertexProvenance, XY, point_distance, point_distance_squared, point_in_ccw_triangle,
    point_segment_distance, ring_winding, same_point, segment_projection, simd_mask_any,
    squared_norm_is_trustworthy, try_point_distance_squared, try_point_segment_distance_squared,
    wrap_index,
};

/// Iteratively remove segments with a degree-1 endpoint (dangles): the
/// closed remainder is the linework whose even-odd parity is well-defined.
pub(crate) fn prune_dangles(segments: &mut Vec<Segment>) {
    if segments.is_empty() {
        return;
    }

    let count = segments.len();
    let mut degree: HashMap<PointKey, u32> = HashMap::with_capacity(count * 2);
    let mut adjacency: HashMap<PointKey, Vec<usize>> = HashMap::with_capacity(count * 2);
    for (index, segment) in segments.iter().enumerate() {
        let start = PointKey::new(segment.start);
        let end = PointKey::new(segment.end);
        *degree.entry(start).or_insert(0) += 1;
        *degree.entry(end).or_insert(0) += 1;
        adjacency.entry(start).or_default().push(index);
        adjacency.entry(end).or_default().push(index);
    }

    let mut removed = vec![false; count];
    let mut worklist: Vec<PointKey> = degree
        .iter()
        .filter_map(|(key, &value)| (value == 1).then_some(*key))
        .collect();

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
                    *value -= 1;
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
        index += 1;
        keep
    });
}

/// A point strictly inside a simple counter-clockwise closed ring.
///
/// Takes the lowest-then-leftmost vertex (necessarily convex), and probes the
/// centroid of its corner triangle; when other boundary vertices intrude into
/// that triangle, the midpoint towards the intruder closest to the corner is
/// interior instead (the classical interior-point construction).
pub(in crate::geometry) fn face_interior_point(face: &[XY]) -> XY {
    let open = &face[..face.len() - 1];
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
    let previous = open[wrap_index(corner + open.len() - 1, open.len())];
    let next = open[wrap_index(corner + 1, open.len())];

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
}

pub(crate) fn minimum_clearance_witness(
    shape: &Shape,
) -> crate::error::Result<Option<MinimumClearanceWitness>> {
    let points = shape.points_vec()?;
    if points.len() < 2 {
        return Ok(None);
    }
    let segments = indexed_segments(shape)?;
    if points.len() * points.len().max(segments.len()) >= SEGMENT_INDEX_MIN_PAIRS {
        minimum_clearance_witness_indexed(&points, &segments)
    } else {
        minimum_clearance_witness_brute(&points, &segments)
    }
}

/// Indexed nearest-query path (large multipoints / dense linework).
fn minimum_clearance_witness_indexed(
    points: &[Point],
    segments: &[IndexedSegment],
) -> crate::error::Result<Option<MinimumClearanceWitness>> {
    let first_point_index_by_xy = first_point_index_by_xy(points)?;
    let vertex_index = PointSetIndex::build(points.iter().copied())?;
    let mut witness = None;
    let mut clearance_squared = f64::INFINITY;
    for (point_index, point) in points.iter().enumerate() {
        if let Some((other, raw_sq)) = vertex_index.nearest_other(*point)
            && let Some(distance_squared) =
                admit_clearance_squared(raw_sq, clearance_squared, || {
                    Ok(point_distance(*point, other))
                })?
        {
            clearance_squared = distance_squared;
            let other_index = first_point_index_by_xy[&PointKey::new(other)];
            witness = Some(MinimumClearanceWitness {
                plan: [
                    VertexProvenance::Input(point_index),
                    VertexProvenance::Input(other_index),
                ],
                distance_squared,
            });
        }
    }
    let segment_index = SegmentIndex::build_from_iter(segments.iter().map(|segment| segment.xy))?;
    for (point_index, point) in points.iter().enumerate() {
        // Untrustworthy best must not prune the R-tree (would hide nearer segments).
        let probe_best = if clearance_squared.is_normal() {
            clearance_squared
        } else {
            f64::INFINITY
        };
        if let Some((seg_i, _segment, raw_sq)) =
            segment_index.nearest_segment_ordinal_if(*point, probe_best, |segment, _distance| {
                !same_point(*point, segment.start) && !same_point(*point, segment.end)
            })?
            && let Some(distance_squared) =
                admit_clearance_squared(raw_sq, clearance_squared, || {
                    point_segment_distance(*point, segments[seg_i].xy)
                })?
        {
            clearance_squared = distance_squared;
            let segment = segments[seg_i];
            witness = Some(MinimumClearanceWitness {
                plan: [
                    VertexProvenance::Input(point_index),
                    VertexProvenance::OnSegment {
                        i: segment.start,
                        j: segment.end,
                        projection: segment_projection(*point, segment.xy)?,
                    },
                ],
                distance_squared,
            });
        }
    }
    Ok(witness)
}

/// Brute O(n²) path for small multipoints / sparse linework.
fn minimum_clearance_witness_brute(
    points: &[Point],
    segments: &[IndexedSegment],
) -> crate::error::Result<Option<MinimumClearanceWitness>> {
    let mut witness = None;
    let mut clearance_squared = f64::INFINITY;
    for (left_index, left) in points.iter().enumerate() {
        for (offset, right) in points[(left_index + 1)..].iter().enumerate() {
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
            witness = Some(MinimumClearanceWitness {
                plan: [
                    VertexProvenance::Input(left_index),
                    VertexProvenance::Input(left_index + 1 + offset),
                ],
                distance_squared,
            });
        }
    }
    for (point_index, point) in points.iter().enumerate() {
        for segment in segments {
            if same_point(*point, segment.xy.start) || same_point(*point, segment.xy.end) {
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
            witness = Some(MinimumClearanceWitness {
                plan: [
                    VertexProvenance::Input(point_index),
                    VertexProvenance::OnSegment {
                        i: segment.start,
                        j: segment.end,
                        projection: segment_projection(*point, segment.xy)?,
                    },
                ],
                distance_squared,
            });
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

fn first_point_index_by_xy(points: &[Point]) -> crate::error::Result<HashMap<PointKey, usize>> {
    let mut indices = HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    indices
        .try_reserve(points.len())
        .map_err(crate::error::Error::from)?;
    for (index, &point) in points.iter().enumerate() {
        indices.entry(PointKey::new(point)).or_insert(index);
    }
    Ok(indices)
}

fn indexed_segments(shape: &Shape) -> crate::error::Result<Vec<IndexedSegment>> {
    fn line_segments_with_indices<C: Coordinates + ?Sized>(
        coords: &C,
        cursor: &mut usize,
        out: &mut Vec<IndexedSegment>,
    ) -> crate::error::Result<()> {
        let base = *cursor;
        for index in 0..coords.coord_count().saturating_sub(1) {
            let start = coords.nth_coord(index);
            let end = coords.nth_coord(index + 1);
            crate::try_push_vec(out, IndexedSegment {
                xy: Segment {
                    start: start.xy(),
                    end: end.xy(),
                },
                start: base + index,
                end: base + index + 1,
            })?;
        }
        *cursor += coords.coord_count();
        Ok(())
    }

    fn walk(
        shape: &Shape,
        cursor: &mut usize,
        out: &mut Vec<IndexedSegment>,
    ) -> crate::error::Result<()> {
        match shape {
            Shape::Point(_) => *cursor += 1,
            Shape::MultiPoint(points) => *cursor += points.coord_count(),
            Shape::LineString(points) => line_segments_with_indices(points, cursor, out)?,
            Shape::MultiLineString(lines) => {
                for line in lines {
                    line_segments_with_indices(line, cursor, out)?;
                }
            },
            Shape::Polygon(polygon) => {
                for ring in polygon.rings() {
                    line_segments_with_indices(ring, cursor, out)?;
                }
            },
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons {
                    for ring in polygon.rings() {
                        line_segments_with_indices(ring, cursor, out)?;
                    }
                }
            },
            Shape::GeometryCollection(geometries) => {
                for geometry in geometries {
                    walk(geometry, cursor, out)?;
                }
            },
            Shape::Empty(..) => {},
        }
        Ok(())
    }

    let mut out = Vec::new();
    crate::try_reserve_vec(&mut out, shape.segment_count())?;
    let mut cursor = 0;
    walk(shape, &mut cursor, &mut out)?;
    Ok(out)
}

pub(crate) fn line_is_closed<C: Coordinates + ?Sized>(points: &C) -> bool {
    let len = points.coord_count();
    len >= 2 && same_point(points.nth_coord(0), points.nth_coord(len - 1))
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
    let _ = chains.push_line(points);
    indexed_segments_are_simple(&chains)
}

/// Strictly increasing or strictly decreasing (`NaN` fails both — the
/// caller's general path owns malformed input).
pub(crate) fn strictly_monotone(values: &[f64]) -> bool {
    values.is_sorted_by(|left, right| left < right)
        || values.is_sorted_by(|left, right| left > right)
}

pub(crate) fn line_crosses_antimeridian<C: Coordinates + ?Sized>(points: &C) -> bool {
    if let Some((xs, _)) = points.xy_columns() {
        return antimeridian_crosses_x_columns(xs);
    }
    points.segment_pairs().any(|[start, end]| {
        let delta = (end.x - start.x).abs();
        delta > 180.0 && delta < 360.0
    })
}

fn antimeridian_crosses_x_columns(xs: &[f64]) -> bool {
    let segments = xs.len().saturating_sub(1);
    if segments == 0 {
        return false;
    }
    let (x0, _) = xs[..segments].as_chunks::<REDUCE_LANES>();
    let (x1, _) = xs[1..=segments].as_chunks::<REDUCE_LANES>();
    simd_mask_any(
        segments,
        |index| {
            let delta = (xs[index + 1] - xs[index]).abs();
            delta > 180.0 && delta < 360.0
        },
        |start| {
            let chunk = start / REDUCE_LANES;
            let delta =
                (ReduceSimd::from_array(x1[chunk]) - ReduceSimd::from_array(x0[chunk])).abs();
            delta.simd_gt(ReduceSimd::splat(180.0)) & delta.simd_lt(ReduceSimd::splat(360.0))
        },
    )
}

pub(crate) fn multiline_is_simple<L: AsRef<CoordSeq>>(lines: &[L]) -> crate::error::Result<bool> {
    let mut chains = LineworkChains::default();
    for line in lines {
        let _ = chains.push_line(line.as_ref());
    }
    indexed_segments_are_simple(&chains)
}

#[cfg(test)]
mod prune_dangles_tests {
    use super::*;

    fn prune_dangles_reference(segments: &mut Vec<Segment>) {
        loop {
            let mut degree: HashMap<PointKey, u32> = HashMap::with_capacity(segments.len() * 2);
            for segment in segments.iter() {
                *degree.entry(PointKey::new(segment.start)).or_insert(0) += 1;
                *degree.entry(PointKey::new(segment.end)).or_insert(0) += 1;
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
mod clearance_allocation_tests {
    use super::*;
    use crate::error::{ErrorKind, ResourceError};
    use crate::geometry::LineSeq;
    use crate::test_support::{allocation_count, fail_allocations_after};

    fn assert_allocation(error: &crate::error::Error) {
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
    }

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
    fn indexed_segments_reject_each_cold_allocation() {
        let shape = line(&[
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(1.0, 0.0),
            Point::new_raw_xy(0.0, 1.0),
        ]);
        let (segments, count) = allocation_count(|| indexed_segments(&shape).unwrap());
        assert_eq!(segments.len(), 2);
        assert!(count > 0);
        for allowed in 0..count {
            let Err(error) = fail_allocations_after(allowed, || indexed_segments(&shape)) else {
                panic!("each cold construction allocation must propagate");
            };
            assert_allocation(&error);
        }
        assert_eq!(indexed_segments(&shape).unwrap().len(), 2);
    }

    #[test]
    fn first_point_index_failure_is_retryable() {
        let points = [
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(1.0, 0.0),
            Point::new_raw_xy(0.0, 0.0),
        ];
        let Err(error) = fail_allocations_after(0, || first_point_index_by_xy(&points)) else {
            panic!("table reservation must propagate");
        };
        assert_allocation(&error);
        let indices = first_point_index_by_xy(&points).unwrap();
        assert_eq!(indices[&PointKey::new(points[0])], 0);
        assert_eq!(indices[&PointKey::new(points[1])], 1);
        assert_eq!(indices.len(), 2);
    }

    #[test]
    fn indexed_clearance_producer_rejects_package_owned_staging() {
        let shape = indexed_line();
        let points = shape.points_vec().unwrap();
        let segments = indexed_segments(&shape).unwrap();
        assert!(points.len() * points.len().max(segments.len()) >= SEGMENT_INDEX_MIN_PAIRS);
        let Err(error) =
            fail_allocations_after(0, || minimum_clearance_witness_indexed(&points, &segments))
        else {
            panic!("indexed staging reservation must propagate");
        };
        assert_allocation(&error);
        let indexed = minimum_clearance_witness_indexed(&points, &segments).unwrap();
        let brute = minimum_clearance_witness_brute(&points, &segments);
        assert_eq!(format!("{brute:?}"), format!("{indexed:?}"));
    }
}
