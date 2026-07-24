#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;
use crate::geometry::*;

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

/// Even-odd raycast parity of `point` against a bag of segments (the
/// deduplicated noded linework) — the segment-set sibling of
/// [`ring_contains_interior`].
pub(crate) fn segments_contain_interior(segments: &[Segment], point: XY) -> bool {
    let mut inside = false;
    for segment in segments {
        let (a, b) = (segment.start, segment.end);
        if (a.y > point.y) != (b.y > point.y)
            && ray_crossing_is_right(a.x, a.y, b.x, b.y, point.x, point.y)
        {
            inside = !inside;
        }
    }
    inside
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
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct MinimumClearanceWitness {
    pub(crate) plan: [VertexProvenance; 2],
    pub(crate) distance_squared: f64,
}

pub(crate) fn minimum_clearance_witness(shape: &Shape) -> Option<MinimumClearanceWitness> {
    let points = shape.points_vec();
    if points.len() < 2 {
        return None;
    }

    let segments = indexed_segments(shape);
    let mut witness: Option<MinimumClearanceWitness> = None;
    let mut clearance_squared = f64::INFINITY;
    let indexed = points.len() * points.len().max(segments.len()) >= SEGMENT_INDEX_MIN_PAIRS;
    let first_point_index_by_xy = first_point_index_by_xy(&points);
    if indexed {
        // Both quadratic phases become pruned nearest queries; min over
        // positive distances is evaluation-order independent, so the result
        // matches the brute scans exactly.
        let vertex_index = PointSetIndex::build(points.iter().copied());
        for (point_index, point) in points.iter().enumerate() {
            if let Some((other, distance_squared)) = vertex_index.nearest_other(*point)
                && distance_squared < clearance_squared
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
        let segment_index =
            SegmentIndex::build_from_iter(segments.iter().map(|segment| segment.xy));
        for (point_index, point) in points.iter().enumerate() {
            if let Some((segment_index, _segment, distance_squared)) = segment_index
                .nearest_segment_ordinal_if(*point, clearance_squared, |segment, distance| {
                    distance > 0.0
                        && !same_point(*point, segment.start)
                        && !same_point(*point, segment.end)
                })
            {
                clearance_squared = distance_squared;
                let segment = segments[segment_index];
                witness = Some(MinimumClearanceWitness {
                    plan: [
                        VertexProvenance::Input(point_index),
                        VertexProvenance::OnSegment {
                            i: segment.start,
                            j: segment.end,
                            fraction: segment_projection_fraction(*point, segment.xy),
                        },
                    ],
                    distance_squared,
                });
            }
        }
        return witness;
    }
    for (left_index, left) in points.iter().enumerate() {
        for (offset, right) in points[(left_index + 1)..].iter().enumerate() {
            let distance_squared = point_distance_squared(*left, *right);
            if distance_squared > 0.0 && distance_squared < clearance_squared {
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
    }
    for (point_index, point) in points.iter().enumerate() {
        for segment in &segments {
            if same_point(*point, segment.xy.start) || same_point(*point, segment.xy.end) {
                continue;
            }
            let distance_squared = point_segment_distance_squared(*point, segment.xy);
            if distance_squared > 0.0 && distance_squared < clearance_squared {
                clearance_squared = distance_squared;
                witness = Some(MinimumClearanceWitness {
                    plan: [
                        VertexProvenance::Input(point_index),
                        VertexProvenance::OnSegment {
                            i: segment.start,
                            j: segment.end,
                            fraction: segment_projection_fraction(*point, segment.xy),
                        },
                    ],
                    distance_squared,
                });
            }
        }
    }
    witness
}

#[derive(Clone, Copy)]
struct IndexedSegment {
    xy: Segment,
    start: usize,
    end: usize,
}

fn first_point_index_by_xy(points: &[Point]) -> HashMap<PointKey, usize> {
    let mut indices = HashMap::with_capacity(points.len());
    for (index, &point) in points.iter().enumerate() {
        indices.entry(PointKey::new(point)).or_insert(index);
    }
    indices
}

fn indexed_segments(shape: &Shape) -> Vec<IndexedSegment> {
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

    fn walk(shape: &Shape, cursor: &mut usize, out: &mut Vec<IndexedSegment>) {
        match shape {
            Shape::Point(_) => *cursor += 1,
            Shape::MultiPoint(points) => *cursor += points.len(),
            Shape::LineString(points) => line_segments_with_indices(points, cursor, out),
            Shape::MultiLineString(lines) => {
                for line in lines {
                    line_segments_with_indices(line, cursor, out);
                }
            },
            Shape::Polygon(polygon) => {
                for ring in polygon.rings() {
                    line_segments_with_indices(ring, cursor, out);
                }
            },
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons {
                    for ring in polygon.rings() {
                        line_segments_with_indices(ring, cursor, out);
                    }
                }
            },
            Shape::GeometryCollection(geometries) => {
                for geometry in geometries {
                    walk(geometry, cursor, out);
                }
            },
            Shape::Empty(..) => {},
        }
    }

    let mut out = Vec::with_capacity(shape.segment_count());
    let mut cursor = 0;
    walk(shape, &mut cursor, &mut out);
    out
}

/// The DE-9IM matrix for two BBOX-DISJOINT operands, from interior + boundary
/// dimensions alone: `II IB IE / BI BB BE / EI EB EE = F F iA / F F bA / iB bB
/// 2`. Each operand's interior lies wholly in the other's exterior (dim = its
/// content dim) and likewise its boundary (dim = its boundary dim, `F` when the
/// boundary is empty — points, closed lines). `None` for an empty operand or a
/// `GeometryCollection` (whose boundary is undefined) — those keep the general
/// relate path. Verified bit-exact against the engine across dimension combos.
pub(in crate::geometry) fn disjoint_de9im(left: &Shape, right: &Shape) -> Option<De9im> {
    let interior = |shape: &Shape| -> Option<u8> {
        Some(match effective_dimension(shape)? {
            Dimension::Point => b'0',
            Dimension::Curve => b'1',
            Dimension::Surface => b'2',
        })
    };
    let boundary = |shape: &Shape| -> Option<u8> {
        if effective_dimension(shape) == Some(Dimension::Point) {
            return Some(b'F');
        }
        Some(match shape {
            Shape::Point(_) | Shape::MultiPoint(_) => b'F',
            Shape::Polygon(_) | Shape::MultiPolygon(_) => b'1',
            // A line's boundary is its mod-2 endpoints — empty when closed.
            Shape::LineString(_) | Shape::MultiLineString(_) => {
                if shape.boundary().is_empty() {
                    b'F'
                } else {
                    b'0'
                }
            },
            Shape::GeometryCollection(_) | Shape::Empty(..) => return None,
        })
    };
    Some(De9im([
        b'F',
        b'F',
        interior(left)?,
        b'F',
        b'F',
        boundary(left)?,
        interior(right)?,
        boundary(right)?,
        b'2',
    ]))
}

pub(crate) const fn has_collection_operand(left: &Shape, right: &Shape) -> bool {
    matches!(
        left,
        Shape::GeometryCollection(_) | Shape::MultiPolygon(_) | Shape::MultiLineString(_)
    ) || matches!(
        right,
        Shape::GeometryCollection(_) | Shape::MultiPolygon(_) | Shape::MultiLineString(_)
    )
}

pub(crate) fn line_is_closed<C: Coordinates + ?Sized>(points: &C) -> bool {
    let len = points.coord_count();
    len >= 2 && same_point(points.nth_coord(0), points.nth_coord(len - 1))
}

/// `LineString` CCW test (the `Shape::is_ccw` line arm): a closed ring
/// (≥4 vertices, coincident endpoints) with positive signed area. Column-native
/// so the packed-`Lines` array path runs it per window with no per-row `Shape`.
pub(crate) fn line_is_ccw(points: &CoordSeq) -> bool {
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
pub(crate) fn polygon_is_valid(polygon: &Polygon) -> bool {
    polygon.validate("polygon", "$").is_none()
}

pub(crate) fn line_is_simple(points: &CoordSeq) -> bool {
    // Strictly monotone ordinates are an O(n) simplicity certificate:
    // non-adjacent segments occupy disjoint open extents along the axis,
    // and adjacent segments cannot fold back into a collinear overlap —
    // no self-contact is possible. Smooth sampled tracks (the common
    // simple case) certify here without staging the candidate engine;
    // non-monotone input fails within a few comparisons.
    if strictly_monotone(points.xs()) || strictly_monotone(points.ys()) {
        return true;
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

pub(crate) fn multiline_is_simple<L: AsRef<CoordSeq>>(lines: &[L]) -> bool {
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
