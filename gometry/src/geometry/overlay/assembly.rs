use crate::geometry::overlay::{
    DimensionalParts, OverlayOp, dedup_points, empty_shape_for_dimension,
    for_each_overlapping_pair, for_each_segment_overlap_point,
};
use crate::geometry::{
    Bounds, CoordSeq, Coordinates as _, HashMap, HashMapExt as _, LineSeq, Point, PointKey,
    Polygon, Segment, Shape, XY, point_on_segment,
};
pub(crate) fn line_line_cross_points(left: &[Segment], right: &[Segment]) -> Vec<Point> {
    let mut candidates: Vec<XY> = Vec::new();
    for_each_overlapping_pair::<false>(left, right, |l, r| {
        for_each_segment_overlap_point(l, r, |point| candidates.push(point));
    });
    candidates.into_iter().map(XY::point).collect()
}

/// Canonicalize overlay buckets into the narrowest representable `Shape`.
///
/// Points and lines covered by the polygon output are absorbed (a piece inside
/// or on a polygon is not an independent component). Each surviving bucket
/// narrows to a leaf or multi-geometry; mixed dimensions yield a
/// `GeometryCollection` ordered by ascending dimension (point, line, polygon).
/// Empty output is an empty `GeometryCollection`.
/// The dimension-typed empty for an overlay op whose result came out empty.
/// Typed to the MAXIMUM dimension the op's result could carry — intersection
/// `min(da, db)`, difference `da` (the minuend), union/symdiff `max(da, db)` —
/// so an empty overlay is `POINT`/`LINESTRING`/`POLYGON EMPTY` consistent with
/// gometry's typed-empty model and Shapely, instead of an untyped collection.
/// An empty geometry carries no ordinates, so the result is plain XY.
pub(crate) fn empty_overlay_shape(a: &Shape, b: &Shape, op: OverlayOp) -> Shape {
    let (da, db) = (a.topological_dimension(), b.topological_dimension());
    let dimension = match op {
        OverlayOp::Intersection => da.min(db),
        OverlayOp::Difference => da,
        OverlayOp::Union | OverlayOp::SymmetricDifference => da.max(db),
    };
    empty_shape_for_dimension(dimension)
}

/// Dissolve polyline `edges` into maximal connected chains: concatenate
/// through every shared endpoint of degree exactly 2 and split at chain termini
/// and degree-`>= 3` junctions, preserving each edge's interior vertices.
/// Endpoint identity uses [`PointKey`] (canonical signed-zero), so a
/// `+0.0`/`-0.0` joint still merges. This is the noded `LineMerger` shared by
/// [`Shape::line_merge`] (edges are the input `LineString`s) and the overlay
/// line sink (edges are atomic noded segments), so one degree-aware dissolve
/// drives both — a Y/T junction splits all its arms instead of fusing two
/// through it. Deterministic and hash-order independent: open chains seed from
/// barrier endpoints in first-seen edge order, then any remaining all-degree-2
/// cycles are walked from their lowest unused edge.
pub(crate) fn merge_chains(edges: Vec<CoordSeq>) -> Vec<CoordSeq> {
    // Drop degenerate edges with no extent (fewer than two DISTINCT vertices) —
    // a zero-length stroke is not linework and must not leak out as an awkward
    // zero-length `LineString`; it simply vanishes from the result.
    let edges: Vec<CoordSeq> = edges
        .into_iter()
        .filter(|edge| {
            let mut points = edge.iter();
            let Some(first) = points.next().map(PointKey::new) else {
                return false;
            };
            points.any(|point| PointKey::new(point) != first)
        })
        .collect();
    if edges.len() <= 1 {
        return edges;
    }
    let endpoints: Vec<[PointKey; 2]> = edges
        .iter()
        .map(|edge| {
            [
                PointKey::new(edge.first().expect("edge has >= 2 vertices")),
                PointKey::new(edge.last().expect("edge has >= 2 vertices")),
            ]
        })
        .collect();
    // Incident edge-ends per node, in first-seen edge order — the walk picks the
    // lowest unused continuation, so output is independent of hash iteration.
    let mut incident: HashMap<PointKey, Vec<usize>> = HashMap::new();
    for (index, [start, end]) in endpoints.iter().enumerate() {
        incident.entry(*start).or_default().push(index);
        incident.entry(*end).or_default().push(index);
    }

    let mut used = vec![false; edges.len()];
    let mut chains: Vec<CoordSeq> = Vec::new();
    // Open chains: every edge with a barrier endpoint (degree != 2) starts a
    // chain that runs until the next barrier. Edges interior to such a chain are
    // consumed mid-walk and skipped when their own seed turn comes.
    for seed in 0..edges.len() {
        if used[seed] {
            continue;
        }
        if let Some(end) = (0..2).find(|&end| incident[&endpoints[seed][end]].len() != 2) {
            let chain = walk_chain(
                seed,
                endpoints[seed][end],
                &edges,
                &endpoints,
                &incident,
                &mut used,
            );
            chains.push(chain);
        }
    }
    // Whatever remains is an all-degree-2 cycle (a closed ring with no
    // junction); walk each from its lowest unused edge.
    for seed in 0..edges.len() {
        if !used[seed] {
            let chain = walk_chain(
                seed,
                endpoints[seed][0],
                &edges,
                &endpoints,
                &incident,
                &mut used,
            );
            chains.push(chain);
        }
    }
    chains
}

/// Walk one maximal chain from `start` departing along `seed`, following
/// degree-2 nodes (reversing edges as needed) until a barrier or a closed loop,
/// marking edges used. Helper of [`merge_chains`].
pub(crate) fn walk_chain(
    seed: usize,
    start: PointKey,
    edges: &[CoordSeq],
    endpoints: &[[PointKey; 2]],
    incident: &HashMap<PointKey, Vec<usize>>,
    used: &mut [bool],
) -> CoordSeq {
    let mut coords: Vec<Point> = Vec::new();
    let mut edge = seed;
    let mut from = start;
    loop {
        used[edge] = true;
        let [a, b] = endpoints[edge];
        // Orient the edge to depart `from`, streaming its points straight into
        // `coords` (forward or reversed) — no per-edge `Vec` allocation. The
        // shared join vertex is dropped via `skip(1)` on every edge after the
        // first. `far` is the arrival node.
        let skip = usize::from(!coords.is_empty());
        let far = if a == from {
            coords.extend(edges[edge].iter().skip(skip));
            b
        } else {
            coords.extend(edges[edge].iter().rev().skip(skip));
            a
        };
        if incident.get(&far).map_or(0, Vec::len) != 2 {
            break;
        }
        match incident[&far]
            .iter()
            .copied()
            .find(|&index| index != edge && !used[index])
        {
            Some(index) => {
                edge = index;
                from = far;
            },
            None => break,
        }
    }
    coords.into()
}

/// The bounds-disjoint union / symmetric-difference of two shapes: simply both
/// side by side (no noding — disjoint operands never interact), canonicalized
/// into the lowest enclosing multi/collection type. For disjoint operands
/// `union` and `symmetric_difference` coincide (no overlap to subtract), so one
/// helper serves both. The array overlay broadcast calls this directly off the
/// borrowed row shapes, skipping the transient per-pair `ShapeData` handles.
pub(crate) fn disjoint_overlay_combine(left: &Shape, right: &Shape) -> Shape {
    let mut parts = DimensionalParts::default();
    parts.push_shape(left);
    parts.push_shape(right);
    build_overlay_shape(
        parts.points,
        parts.lines.iter().map(|line| (*line).clone()).collect(),
        parts
            .polygons
            .iter()
            .map(|polygon| (*polygon).clone())
            .collect(),
    )
}

#[expect(
    clippy::redundant_closure_for_method_calls,
    reason = "Polygon::bounds lives in a private module; closure keeps the call site readable"
)]
pub(crate) fn build_overlay_shape(
    points: Vec<Point>,
    lines: Vec<CoordSeq>,
    polygons: Vec<Polygon>,
) -> Shape {
    let polygon_bounds: Vec<Option<Bounds>> =
        polygons.iter().map(|polygon| polygon.bounds()).collect();
    let mut surviving: Vec<Point> = points
        .into_iter()
        .filter(|point| {
            !polygons
                .iter()
                .zip(&polygon_bounds)
                .any(|(polygon, bounds)| {
                    bounds.is_none_or(|bounds| bounds.contains_xy(point.xy()))
                        && polygon.covers_point(*point)
                })
                && !lines.iter().any(|line| {
                    line.segment_pairs()
                        .any(|[start, end]| point_on_segment(*point, start, end))
                })
        })
        .collect();
    dedup_points(&mut surviving);

    // Overlays node linework into atomic per-vertex segments; dissolve them back
    // into maximal chains (split only at genuine degree-`>= 3` junctions), so a
    // result reads as the cleanest lines representing its point set rather than
    // a `MultiLineString` of one segment per span.
    let lines = merge_chains(lines);
    let line_part = match lines.len() {
        0 => None,
        1 => Some(Shape::LineString(
            LineSeq::try_new(lines.into_iter().next().expect("len == 1"))
                .expect("assembled overlay chain is lineal"),
        )),
        _ => Some(Shape::MultiLineString(
            lines
                .into_iter()
                .map(|line| LineSeq::try_new(line).expect("assembled overlay chain is lineal"))
                .collect(),
        )),
    };

    let point_part = match surviving.len() {
        0 => None,
        1 => Some(Shape::Point(surviving[0])),
        _ => Some(Shape::MultiPoint(surviving.into())),
    };
    let polygon_part = match polygons.len() {
        0 => None,
        1 => Some(Shape::Polygon(
            polygons.into_iter().next().expect("len checked == 1"),
        )),
        _ => Some(Shape::MultiPolygon(polygons)),
    };

    let parts: Vec<Shape> = [point_part, line_part, polygon_part]
        .into_iter()
        .flatten()
        .collect();
    match <[Shape; 1]>::try_from(parts) {
        Ok([single]) => single,
        Err(parts) => Shape::GeometryCollection(parts),
    }
}
