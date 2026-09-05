use ahash::HashSetExt as _;
use rstar::AABB;
use rstar::primitives::{GeomWithData, Rectangle};

use crate::geometry::{
    Arrangement, Bounds, BulkRTree, CoordSeq, HashMap, HashMapExt as _, HashSet, LineSeq, PointKey,
    Polygon, PolygonizeFull, Ring, Segment, Shape, TopologyGraph, XY, compare_point_slices,
    line_segments, open_xy_cycle_winding, orient_ring, point_on_segment, ring_contains_interior,
    ring_decision_area, segment_projection,
};

type Face = (Vec<XY>, f64);

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
    // Faces keep one rounded |area| key for the descending sort and parent
    // scan; equal keys break on the original-coordinate ExactSum order.
    let mut faces = arrangement_minimal_faces(arrangement);
    faces.sort_by(|(left_ring, left_area), (right_ring, right_area)| {
        crate::geometry::cmp_area_keys_then_closed(*right_area, *left_area, right_ring, left_ring)
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
        if faces[index].0.is_empty() {
            continue;
        }
        let child_box = Bounds::from_xy_iter(faces[index].0.iter().copied());
        let point_envelope = AABB::from_corners([child_box.minx(), child_box.miny()], [
            child_box.maxx(),
            child_box.maxy(),
        ]);
        // Parent = the smallest-area face that strictly contains at least one
        // child vertex, ignoring shared boundary vertices. Ties are broken by
        // earliest sort position (faces are area-descending).
        let mut best: Option<(f64, usize)> = None;
        for entry in tree.locate_in_envelope_intersecting(point_envelope) {
            let candidate = entry.data;
            if candidate >= index {
                continue;
            }
            let (ring, area) = &faces[candidate];
            if faces[index].0.iter().copied().any(|point| {
                !ring
                    .array_windows::<2>()
                    .any(|[start, end]| point_on_segment(point, *start, *end))
                    && ring_contains_interior(ring, point)
            }) && best.is_none_or(|(best_area, best_pos)| {
                area.total_cmp(&best_area)
                    .then(candidate.cmp(&best_pos))
                    .is_lt()
            }) {
                best = Some((*area, candidate));
            }
        }
        parents[index] = best.map(|(_, parent)| parent);
    }

    (faces, parents)
}

pub(crate) fn polygonize_faces(graph: &TopologyGraph) -> (Vec<Face>, Vec<Option<usize>>) {
    let mut arrangement = arrangement_from_graph(graph);
    polygonize_faces_from_arrangement(&mut arrangement)
}

/// Polygonize noded linework: every minimal cycle becomes its own polygon (the
/// JTS/GEOS `Polygonizer` contract). A face's holes are its DIRECT child faces,
/// so a nested ring is BOTH a hole of its container and a shell in its own
/// right — exactly like GEOS (a donut yields the holed annulus AND the inner
/// disk).
pub(crate) fn polygonize_lines(graph: &TopologyGraph) -> Vec<Polygon> {
    let (faces, parents) = polygonize_faces(graph);
    let children = children_of(&parents);
    (0..faces.len())
        .map(|index| {
            let shell = Ring::from_closed(orient_ring(&faces[index].0, false));
            let holes = children[index]
                .iter()
                .map(|&child| Ring::from_closed(orient_ring(&faces[child].0, true)))
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
pub(crate) fn build_area_lines(graph: &TopologyGraph) -> Vec<Polygon> {
    let (faces, parents) = polygonize_faces(graph);
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
            let shell = Ring::from_closed(orient_ring(&faces[index].0, false));
            let holes = children[index]
                .iter()
                .map(|&hole| Ring::from_closed(orient_ring(&faces[hole].0, true)))
                .collect();
            Polygon::new(shell, holes)
        })
        .collect()
}

pub(crate) fn polygonize_full(graph: &TopologyGraph, source_row_count: usize) -> PolygonizeFull {
    let mut arrangement = arrangement_from_graph(graph);
    let (faces, parents) = polygonize_faces_from_arrangement(&mut arrangement);
    let children = children_of(&parents);
    let polygons: Vec<Polygon> = (0..faces.len())
        .map(|index| {
            let shell = Ring::from_closed(orient_ring(&faces[index].0, false));
            let holes = children[index]
                .iter()
                .map(|&child| Ring::from_closed(orient_ring(&faces[child].0, true)))
                .collect();
            Polygon::new(shell, holes)
        })
        .collect();
    let used_segments = polygon_segment_keys(&polygons);
    let mut cuts = vec![Vec::new(); source_row_count];
    let mut dangles = vec![Vec::new(); source_row_count];
    let mut invalid_rings = vec![Vec::new(); source_row_count];
    let mut degree = HashMap::with_capacity(graph.nodes.len());
    for edge in &graph.edges {
        *degree
            .entry(PointKey::new(edge.segment.start))
            .or_insert(0_usize) += 1;
        *degree
            .entry(PointKey::new(edge.segment.end))
            .or_insert(0_usize) += 1;
    }

    // Each noded edge keeps every source owner. Emit one diagnostic per source
    // row and atomic piece, so pooled inputs retain ownership without replaying
    // the raw, pre-noding segment list.
    for edge in &graph.edges {
        let key = undirected_edge_key(edge.segment.start, edge.segment.end);
        let mut rows = Vec::with_capacity(edge.sources.len());
        for &source_index in &edge.sources {
            let row = graph.source_edges[source_index as usize].source_row as usize;
            if !rows.contains(&row) {
                rows.push(row);
            }
        }
        for row in rows {
            if row >= source_row_count {
                continue;
            }
            if edge.sources.len() > 1 {
                invalid_rings[row].push(diagnostic_line(edge.segment, true));
                continue;
            }
            if used_segments.contains(&key) {
                continue;
            }
            if degree
                .get(&PointKey::new(edge.segment.start))
                .copied()
                .unwrap_or(0)
                <= 1
                || degree
                    .get(&PointKey::new(edge.segment.end))
                    .copied()
                    .unwrap_or(0)
                    <= 1
            {
                dangles[row].push(diagnostic_line(edge.segment, false));
            } else {
                cuts[row].push(diagnostic_line(edge.segment, false));
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

fn arrangement_from_graph(graph: &TopologyGraph) -> Arrangement {
    let segments: Vec<_> = graph.edges.iter().map(|edge| edge.segment).collect();
    let weights: Vec<_> = graph
        .edges
        .iter()
        .map(|edge| {
            edge.sources
                .iter()
                .map(|&source_index| {
                    source_direction(
                        edge.segment,
                        graph.source_edges[source_index as usize].segment,
                    )
                })
                .sum()
        })
        .collect();
    Arrangement::build::<true>(&segments, |index| weights[index])
}

fn source_direction(piece: Segment, source: Segment) -> i32 {
    let start = segment_projection(piece.start, source);
    let end = segment_projection(piece.end, source);
    if end.cmp_along(&start).is_ge() { 1 } else { -1 }
}

fn undirected_edge_key(
    start: XY,
    end: XY,
) -> (crate::geometry::PointKey, crate::geometry::PointKey) {
    let left = crate::geometry::PointKey::new(start);
    let right = crate::geometry::PointKey::new(end);
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
}

fn diagnostic_line(segment: crate::geometry::Segment, closed: bool) -> Shape {
    let mut points = vec![segment.start, segment.end];
    if closed {
        points.push(segment.start);
    }
    Shape::LineString(LineSeq::from_proven(CoordSeq::from(points)))
}

pub(crate) fn polygon_segment_keys(
    polygons: &[Polygon],
) -> HashSet<(crate::geometry::PointKey, crate::geometry::PointKey)> {
    let mut keys = HashSet::new();
    for polygon in polygons {
        for ring in polygon.rings() {
            for segment in line_segments(&ring) {
                keys.insert(undirected_edge_key(segment.start, segment.end));
            }
        }
    }
    keys
}
