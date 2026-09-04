use ahash::HashSetExt as _;

use crate::geometry::overlay::{
    OverlayOp, empty_shape_for_dimension_with_axes, overlay_declared_axes,
};
use crate::geometry::{
    Bounds, CoordSeq, CoordinateAxes, Coordinates as _, GeometryKind, HashMap, HashMapExt as _,
    HashSet, LineSeq, Point, PointKey, Polygon, Ring, Segment, Shape, point_on_segment,
};

/// Order-preserving topological dedup of overlay output points.
pub(crate) fn dedup_points(points: &mut Vec<Point>) {
    let mut seen: HashSet<PointKey> = HashSet::with_capacity(points.len());
    points.retain(|point| seen.insert(PointKey::new(*point)));
}

/// Overlay nodes rings where boundaries meet; a node on a straight run does
/// not define the output region and is removed before the result is emitted.
fn drop_collinear_ring_vertices(polygon: &Polygon) -> Polygon {
    let simplify = |ring: &Ring| Ring::from_closed(drop_collinear_vertices(ring.coords().iter_coords().collect()));
    Polygon::new(simplify(&polygon.shell), polygon.holes.iter().map(simplify).collect())
}

/// One stack pass plus the seam removes vertices between collinear neighbours.
pub(crate) fn drop_collinear_vertices(mut vertices: Vec<Point>) -> Vec<Point> {
    if vertices.len() <= 4 {
        return vertices;
    }
    vertices.pop();
    let mut kept = Vec::with_capacity(vertices.len());
    for vertex in vertices {
        while kept.len() >= 2 && point_on_segment(kept[kept.len() - 1], kept[kept.len() - 2], vertex) {
            kept.pop();
        }
        kept.push(vertex);
    }
    loop {
        let count = kept.len();
        if count <= 3 {
            break;
        }
        if point_on_segment(kept[count - 1], kept[count - 2], kept[0]) {
            kept.pop();
        } else if point_on_segment(kept[0], kept[count - 1], kept[1]) {
            kept.remove(0);
        } else {
            break;
        }
    }
    kept.push(kept[0]);
    kept
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
/// The empty keeps the operation's declared axes even though it has no
/// coordinates from which to resolve them.
pub(crate) fn empty_overlay_shape(a: &Shape, b: &Shape, op: OverlayOp) -> Shape {
    let (da, db) = (a.topological_dimension(), b.topological_dimension());
    let dimension = match op {
        OverlayOp::Intersection => da.min(db),
        OverlayOp::Difference => da,
        OverlayOp::Union | OverlayOp::SymmetricDifference => da.max(db),
    };
    empty_shape_for_dimension_with_axes(dimension, overlay_declared_axes(&[a, b], op))
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
/// One edge of a linework arrangement as the dissolve sees it: its two end
/// keys, and its points streamed in either direction.
pub(crate) trait ChainEdge {
    /// `None` for an edge with no extent (fewer than two distinct vertices),
    /// which is not linework and vanishes from the result.
    fn ends(&self) -> Option<[PointKey; 2]>;
    /// Append the edge's points departing from `from`, dropping the first
    /// `skip` (the join vertex the chain already holds).
    fn extend_from(&self, from: PointKey, skip: usize, out: &mut Vec<Point>);
}

impl ChainEdge for CoordSeq {
    fn ends(&self) -> Option<[PointKey; 2]> {
        let mut points = self.iter();
        let first = PointKey::new(points.next()?);
        points.any(|point| PointKey::new(point) != first).then(|| {
            [
                first,
                PointKey::new(self.last().expect("edge has >= 2 vertices")),
            ]
        })
    }

    fn extend_from(&self, from: PointKey, skip: usize, out: &mut Vec<Point>) {
        if PointKey::new(self.first().expect("edge has >= 2 vertices")) == from {
            out.extend(self.iter().skip(skip));
        } else {
            out.extend(self.iter().rev().skip(skip));
        }
    }
}

impl ChainEdge for Segment {
    fn ends(&self) -> Option<[PointKey; 2]> {
        let ends = [PointKey::new(self.start), PointKey::new(self.end)];
        (ends[0] != ends[1]).then_some(ends)
    }

    fn extend_from(&self, from: PointKey, skip: usize, out: &mut Vec<Point>) {
        let (near, far) = if PointKey::new(self.start) == from {
            (self.start, self.end)
        } else {
            (self.end, self.start)
        };
        if skip == 0 {
            out.push(near.point());
        }
        out.push(far.point());
    }
}

/// Dissolve linework edges into maximal chains, split only at genuine
/// junctions (nodes of degree other than two). Every edge is consumed once;
/// output order follows the lowest unused seed, so it is independent of
/// hash iteration. Node incidence is a CSR over the distinct end keys: one
/// map lookup per edge end and no per-node allocation, which is what an
/// overlay of thousands of atomic segments spends its time on otherwise.
pub(crate) fn merge_chains(edges: Vec<CoordSeq>) -> Vec<CoordSeq> {
    merge_chain_edges(&edges)
}

pub(crate) fn merge_chain_edges<E: ChainEdge>(edges: &[E]) -> Vec<CoordSeq> {
    let mut ends: Vec<[PointKey; 2]> = Vec::with_capacity(edges.len());
    let mut live: Vec<usize> = Vec::with_capacity(edges.len());
    for (index, edge) in edges.iter().enumerate() {
        if let Some(pair) = edge.ends() {
            ends.push(pair);
            live.push(index);
        }
    }
    if live.len() <= 1 {
        return live
            .into_iter()
            .map(|index| {
                let mut coords = Vec::new();
                edges[index].extend_from(ends[0][0], 0, &mut coords);
                coords.into()
            })
            .collect();
    }
    let mut node_of: HashMap<PointKey, u32> = HashMap::with_capacity(ends.len());
    let mut degree: Vec<u32> = Vec::new();
    let mut node_ends: Vec<[u32; 2]> = Vec::with_capacity(ends.len());
    for pair in &ends {
        let mut ids = [0_u32; 2];
        for (slot, key) in pair.iter().enumerate() {
            let next = degree.len() as u32;
            let id = *node_of.entry(*key).or_insert(next);
            if id == next {
                degree.push(0);
            }
            degree[id as usize] += 1;
            ids[slot] = id;
        }
        node_ends.push(ids);
    }
    let mut offsets: Vec<u32> = Vec::with_capacity(degree.len() + 1);
    offsets.push(0);
    for &count in &degree {
        offsets.push(offsets[offsets.len() - 1] + count);
    }
    let mut cursor = offsets.clone();
    let mut slots = vec![0_u32; offsets[degree.len()] as usize];
    for (edge, ids) in node_ends.iter().enumerate() {
        for &id in ids {
            slots[cursor[id as usize] as usize] = edge as u32;
            cursor[id as usize] += 1;
        }
    }
    let incident = |node: u32| &slots[offsets[node as usize] as usize..offsets[node as usize + 1] as usize];

    let mut used = vec![false; ends.len()];
    let mut chains: Vec<CoordSeq> = Vec::new();
    let mut walk = |seed: usize, start_slot: usize, used: &mut [bool]| -> CoordSeq {
        let mut coords: Vec<Point> = Vec::new();
        let mut edge = seed;
        let mut from = node_ends[seed][start_slot];
        loop {
            used[edge] = true;
            let [a, b] = node_ends[edge];
            let from_key = ends[edge][usize::from(a != from)];
            edges[live[edge]].extend_from(from_key, usize::from(!coords.is_empty()), &mut coords);
            let far = if a == from { b } else { a };
            if degree[far as usize] != 2 {
                break;
            }
            match incident(far).iter().copied().find(|&next| next as usize != edge && !used[next as usize]) {
                Some(next) => {
                    edge = next as usize;
                    from = far;
                },
                None => break,
            }
        }
        coords.into()
    };
    // Open chains first: every edge with a barrier end (degree != 2) starts
    // a chain that runs until the next barrier; edges interior to such a
    // chain are consumed mid-walk and skipped when their own seed turn comes.
    for seed in 0..ends.len() {
        if used[seed] {
            continue;
        }
        if let Some(slot) = (0..2).find(|&slot| degree[node_ends[seed][slot] as usize] != 2) {
            chains.push(walk(seed, slot, &mut used));
        }
    }
    // Whatever remains is an all-degree-2 cycle; walk each from its lowest
    // unused edge.
    for seed in 0..ends.len() {
        if !used[seed] {
            chains.push(walk(seed, 0, &mut used));
        }
    }
    chains
}

#[expect(
    clippy::redundant_closure_for_method_calls,
    reason = "Polygon::bounds lives in a private module; closure keeps the call site readable"
)]
pub(crate) fn build_overlay_shape(
    points: Vec<Point>,
    lines: Vec<CoordSeq>,
    polygons: Vec<Polygon>,
) -> crate::error::Result<Shape> {
    // Overlays node linework into atomic per-vertex segments; dissolve them back
    // into maximal chains (split only at genuine degree-`>= 3` junctions), so a
    // result reads as the cleanest lines representing its point set rather than
    // a `MultiLineString` of one segment per span.
    build_overlay_shape_from_chains(points, merge_chains(lines), polygons)
}

/// [`build_overlay_shape`] for a lane that dissolved its linework itself.
pub(crate) fn build_overlay_shape_from_chains(
    points: Vec<Point>,
    lines: Vec<CoordSeq>,
    polygons: Vec<Polygon>,
) -> crate::error::Result<Shape> {
    let polygons: Vec<Polygon> = polygons.iter().map(drop_collinear_ring_vertices).collect();
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

    let line_part = (!lines.is_empty()).then(|| {
        Shape::narrow_lines(
            CoordinateAxes::XY,
            lines.into_iter().map(LineSeq::from_proven).collect(),
        )
    });

    let point_part =
        (!surviving.is_empty()).then(|| Shape::narrow_points(CoordinateAxes::XY, surviving));
    let polygon_part =
        (!polygons.is_empty()).then(|| Shape::narrow_polygons(CoordinateAxes::XY, polygons));

    let parts: Vec<Shape> = [point_part, line_part, polygon_part]
        .into_iter()
        .flatten()
        .collect();
    match <[Shape; 1]>::try_from(parts) {
        Ok([single]) => Ok(single),
        Err(parts) if parts.is_empty() => Ok(Shape::typed_empty(
            GeometryKind::GeometryCollection,
            CoordinateAxes::XY,
        )),
        Err(parts) => Shape::geometry_collection(parts),
    }
}
