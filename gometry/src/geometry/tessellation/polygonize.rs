use ahash::HashSetExt as _;
use rstar::AABB;
use rstar::primitives::{GeomWithData, Rectangle};

use crate::geometry::{
    Arrangement, Bounds, BulkRTree, CoordSeq, HashMap, HashMapExt as _, HashSet, LineSeq, PointKey,
    Polygon, PolygonizeFull, Ring, Segment, Shape, XY, compare_point_slices, line_segments,
    open_xy_cycle_winding, ordered_edge, orient_ring, ring_contains_interior, ring_decision_area,
    same_point,
};

type Face = (Vec<XY>, f64);

pub(crate) fn polygonize_segments(lines: &[Vec<XY>]) -> Vec<Segment> {
    lines.iter().flat_map(line_segments).collect()
}

/// Positive-area face rings from a built arrangement — CCW walks only, rings
/// `take`n to avoid cloning.
pub(crate) fn take_positive_face_rings(arrangement: &mut Arrangement) -> Vec<Vec<XY>> {
    arrangement
        .faces_mut()
        .iter_mut()
        .filter(|face| open_xy_cycle_winding(&face.ring).is_ccw())
        .map(|face| std::mem::take(&mut face.ring))
        .collect()
}

/// Minimal CCW faces of noded segment linework — the standard planar
/// polygonizer's positive-area rings only.
pub(crate) fn minimal_positive_face_rings(segments: &[Segment]) -> Vec<Vec<XY>> {
    take_positive_face_rings(&mut Arrangement::new_with_rings(segments))
}

/// Minimal CCW faces from a built arrangement — positive [`Arrangement::Face`]
/// walks only, rings `take`n to avoid cloning.
pub(crate) fn arrangement_minimal_faces(arrangement: &mut Arrangement) -> Vec<Face> {
    take_positive_face_rings(arrangement)
        .into_iter()
        .map(|ring| {
            let area = ring_decision_area(&ring).magnitude().get();
            (ring, area)
        })
        .collect()
}

/// Noded linework → its minimal faces (ring + `|area|`, sorted area-descending)
/// and each face's parent: the smallest-area face that strictly contains it
/// (its immediate container), or `None` for a top-level face. Parents always
/// point to an earlier (larger-area) position, so depth folds in one forward
/// pass. Shared by [`polygonize_lines`] (all-faces) and [`build_area_lines`]
/// (even-odd fill) — the only difference between the two ops is how they
/// assemble shells and holes from this forest.
pub(crate) fn polygonize_faces_from_arrangement(
    arrangement: &mut Arrangement,
) -> (Vec<Face>, Vec<Option<usize>>) {
    // Faces carry their |area| from here on: the size sort and the parent
    // query below compare areas constantly, and the shoelace over a Point-slice
    // ring gathers columns per call — at 1.5k faces the recomputation was 60%
    // of a polygonize profile.
    let mut faces = arrangement_minimal_faces(arrangement);
    faces.sort_by(|(left_ring, left_area), (right_ring, right_area)| {
        right_area
            .total_cmp(left_area)
            .then_with(|| compare_point_slices(left_ring, right_ring))
    });
    // Envelope per face: the nesting scan rejects nearly every candidate on
    // the box before the exact interior test.
    let boxes: Vec<Bounds> = faces
        .iter()
        .map(|(ring, _)| Bounds::from_xy_iter(ring.iter().copied()))
        .collect();

    // Index the face envelopes so each face's parent query is O(log F + hits)
    // instead of an O(F) prior-face scan (the whole pass was O(F²) — the
    // dominant polygonize cost, growing unboundedly with face count). The
    // point-in-rectangle query yields every box-containing candidate; the exact
    // `ring_contains_interior` test and the smallest-area/earliest-tie parent
    // rule below reproduce the prior scan's verdict exactly.
    let tree: BulkRTree<GeomWithData<Rectangle<[f64; 2]>, usize>> =
        BulkRTree::bulk_load_with_params(
            boxes
                .iter()
                .enumerate()
                .map(|(position, bounds)| {
                    GeomWithData::new(
                        Rectangle::from_corners([bounds.minx(), bounds.miny()], [
                            bounds.maxx(),
                            bounds.maxy(),
                        ]),
                        position,
                    )
                })
                .collect(),
        );

    // Each face's parent is the smallest-area face that strictly contains it
    // (its immediate container); a face with no container is a top-level shell.
    let mut parents = vec![None; faces.len()];
    for index in 0..faces.len() {
        let Some(test_point) = faces[index].0.first().copied() else {
            continue;
        };
        let point_envelope =
            AABB::from_corners([test_point.x, test_point.y], [test_point.x, test_point.y]);
        // Parent = the smallest-area face that strictly contains the point,
        // ties broken by earliest sort position (faces are area-descending, so
        // this matches the prior `candidate < index` forward scan).
        let mut best: Option<(f64, usize)> = None;
        for entry in tree.locate_in_envelope_intersecting(point_envelope) {
            let candidate = entry.data;
            if candidate >= index {
                continue;
            }
            let (ring, area) = &faces[candidate];
            if ring_contains_interior(ring, test_point)
                && best.is_none_or(|(best_area, best_pos)| {
                    area.total_cmp(&best_area)
                        .then(candidate.cmp(&best_pos))
                        .is_lt()
                })
            {
                best = Some((*area, candidate));
            }
        }
        parents[index] = best.map(|(_, parent)| parent);
    }

    (faces, parents)
}

pub(crate) fn polygonize_faces(lines: &[Vec<XY>]) -> (Vec<Face>, Vec<Option<usize>>) {
    let segments = polygonize_segments(lines);
    let mut arrangement = Arrangement::new_with_rings(&segments);
    polygonize_faces_from_arrangement(&mut arrangement)
}

/// Polygonize noded linework: every minimal cycle becomes its own polygon (the
/// JTS/GEOS `Polygonizer` contract). A face's holes are its DIRECT child faces,
/// so a nested ring is BOTH a hole of its container and a shell in its own
/// right — exactly like GEOS (a donut yields the holed annulus AND the inner
/// disk).
pub(crate) fn polygonize_lines(lines: &[Vec<XY>]) -> Vec<Polygon> {
    let (faces, parents) = polygonize_faces(lines);
    let children = children_of(&parents);
    (0..faces.len())
        .map(|index| {
            let shell = Ring::from_trusted_closed(orient_ring(&faces[index].0, false));
            let holes = children[index]
                .iter()
                .map(|&child| Ring::from_trusted_closed(orient_ring(&faces[child].0, true)))
                .collect();
            Polygon::new(shell, holes)
        })
        .collect()
}

/// Invert the parent forest into a direct-children adjacency in one O(faces)
/// pass, so hole assembly reads `children[shell]` instead of rescanning every
/// face (which made the assembly O(faces²)).
pub(crate) fn children_of(parents: &[Option<usize>]) -> Vec<Vec<usize>> {
    let mut children = vec![Vec::new(); parents.len()];
    for (child, parent) in parents.iter().enumerate() {
        if let Some(parent) = parent {
            children[*parent].push(child);
        }
    }
    children
}

/// Assemble noded linework with GEOS `BuildArea` semantics: nested rings
/// alternate solid/hole (even-odd fill), so a ring enclosing a hole yields ONE
/// holed polygon rather than the ring-plus-inner-disk that [`polygonize_lines`]
/// produces. Only even-depth faces are shells; their holes are the odd-depth
/// faces immediately nested within. The right model for dissolving a cell-grid
/// coverage into its outline.
pub(crate) fn build_area_lines(lines: &[Vec<XY>]) -> Vec<Polygon> {
    let (faces, parents) = polygonize_faces(lines);
    let children = children_of(&parents);
    // Parents point to earlier (larger-area) positions, so depth folds forward.
    let mut depths = vec![0_usize; faces.len()];
    for index in 0..faces.len() {
        if let Some(parent) = parents[index] {
            depths[index] = depths[parent] + 1;
        }
    }
    // Even-depth faces are solid (shells); their holes are exactly their direct
    // children (odd-depth, immediately nested). Odd-depth faces are holes, not
    // shells — their own children become separate even-depth shells.
    (0..faces.len())
        .filter(|&index| depths[index].is_multiple_of(2))
        .map(|index| {
            let shell = Ring::from_trusted_closed(orient_ring(&faces[index].0, false));
            let holes = children[index]
                .iter()
                .map(|&hole| Ring::from_trusted_closed(orient_ring(&faces[hole].0, true)))
                .collect();
            Polygon::new(shell, holes)
        })
        .collect()
}

pub(crate) fn polygonize_full(lines: &[Vec<XY>]) -> PolygonizeFull {
    let segments = polygonize_segments(lines);
    let mut arrangement = Arrangement::new_with_rings(&segments);
    let (faces, parents) = polygonize_faces_from_arrangement(&mut arrangement);
    let children = children_of(&parents);
    let polygons: Vec<Polygon> = (0..faces.len())
        .map(|index| {
            let shell = Ring::from_trusted_closed(orient_ring(&faces[index].0, false));
            let holes = children[index]
                .iter()
                .map(|&child| Ring::from_trusted_closed(orient_ring(&faces[child].0, true)))
                .collect();
            Polygon::new(shell, holes)
        })
        .collect();
    let used_segments = polygon_segment_keys(&polygons);
    let mut counted_segments = counted_line_segments(lines);
    let mut cuts = Vec::new();
    let mut dangles = Vec::new();
    let mut invalid_rings = Vec::new();

    // Classify each unique edge at its first appearance in the input, not in
    // hash-map order — cuts/dangles/invalid-rings come out in a deterministic,
    // input-aligned order.
    for line in lines {
        for input_segment in line_segments(line) {
            if same_point(input_segment.start, input_segment.end) {
                continue;
            }
            let edge = ordered_edge(
                PointKey::new(input_segment.start),
                PointKey::new(input_segment.end),
            );
            let Some((count, segment)) = counted_segments.remove(&edge) else {
                continue;
            };
            if count > 1 {
                invalid_rings.push(Shape::LineString(
                    LineSeq::try_new(CoordSeq::from(vec![
                        segment.start,
                        segment.end,
                        segment.start,
                    ]))
                    .expect("invalid-ring diagnostic line has three vertices"),
                ));
                continue;
            }
            if used_segments.contains(&edge) {
                continue;
            }
            let shape = Shape::LineString(
                LineSeq::try_new(CoordSeq::from(vec![segment.start, segment.end]))
                    .expect("dangle/cut diagnostic line has two vertices"),
            );
            if arrangement.vertex_degree(segment.start) <= 1
                || arrangement.vertex_degree(segment.end) <= 1
            {
                dangles.push(shape);
            } else {
                cuts.push(shape);
            }
        }
    }

    PolygonizeFull {
        polygons: polygons.into_iter().map(Shape::Polygon).collect(),
        cuts,
        dangles,
        invalid_rings,
    }
}

pub(crate) fn polygon_segment_keys(polygons: &[Polygon]) -> HashSet<(PointKey, PointKey)> {
    let mut keys = HashSet::new();
    for polygon in polygons {
        for ring in polygon.rings() {
            for segment in line_segments(&ring) {
                keys.insert(ordered_edge(
                    PointKey::new(segment.start),
                    PointKey::new(segment.end),
                ));
            }
        }
    }
    keys
}

pub(crate) fn counted_line_segments(
    lines: &[Vec<XY>],
) -> HashMap<(PointKey, PointKey), (usize, Segment)> {
    let mut counts = HashMap::new();
    for line in lines {
        for segment in line_segments(line) {
            if same_point(segment.start, segment.end) {
                continue;
            }
            let edge = ordered_edge(PointKey::new(segment.start), PointKey::new(segment.end));
            counts
                .entry(edge)
                .and_modify(|(count, _)| *count += 1)
                .or_insert((1, segment));
        }
    }
    counts
}
