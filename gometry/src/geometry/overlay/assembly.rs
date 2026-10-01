use crate::geometry::{
    CoordSeq, CoordSeqBuilder, CoordinateAxes, Coordinates as _, GeometryKind, HashMap, HashSet,
    LineSeq, Point, PointKey, Polygon, Ring, Shape, point_on_segment,
};

/// Order-preserving topological dedup of overlay output points.
pub(crate) fn dedup_points(points: &mut Vec<Point>) -> crate::error::Result<()> {
    let mut seen: HashSet<PointKey> =
        HashSet::with_hasher(crate::collections::collection_hash_state()?.clone());
    seen.reserve(points.len());
    points.retain(|point| seen.insert(PointKey::new(*point)));
    Ok(())
}
/// An overlay nodes rings wherever operands meet, and a node on a straight
/// run says nothing about the region: every ring leaves with each vertex
/// that lies on the segment between its neighbours removed.
fn drop_collinear_ring_vertices(polygon: &Polygon) -> crate::error::Result<Polygon> {
    let simplify = |ring: &Ring| {
        let mut vertices = Vec::with_capacity(ring.coord_count());
        for point in ring.coords().iter_coords() {
            vertices.push(point);
        }
        Ring::try_from_closed(CoordSeq::try_from_points(&drop_collinear_vertices(
            vertices,
        ))?)
    };
    let shell = simplify(&polygon.shell)?;
    let mut holes = Vec::with_capacity(polygon.holes.len());
    for hole in polygon.holes.iter() {
        holes.push(simplify(hole)?);
    }
    Ok(Polygon::new(shell, holes))
}

/// One pass with a stack, then the seam: a vertex on the segment between
/// its neighbours goes, and removing it re-examines the neighbour it exposed.
/// A ring of three vertices or fewer is left alone, since a collinear whole
/// ring is a degeneracy for the caller to see, not to hide.
pub(crate) fn drop_collinear_vertices(mut vertices: Vec<Point>) -> Vec<Point> {
    if vertices.len() <= 4 {
        return vertices;
    }
    vertices.pop();
    let mut kept = Vec::with_capacity(vertices.len());
    for vertex in vertices {
        while kept.len() >= 2
            && point_on_segment(kept[kept.len() - 1], kept[kept.len() - 2], vertex)
        {
            kept.pop();
        }
        kept.push(vertex);
    }
    // The seam: the last vertices against the first, and the first against
    // the last, until neither is collinear with its neighbours.
    loop {
        let n = kept.len();
        if n <= 3 {
            break;
        }
        if point_on_segment(kept[n - 1], kept[n - 2], kept[0]) {
            kept.pop();
        } else if point_on_segment(kept[0], kept[n - 1], kept[1]) {
            kept.remove(0);
        } else {
            break;
        }
    }
    let first = kept[0];
    kept.push(first);
    kept
}

fn extend_chain(edge: &CoordSeq, from: PointKey, skip: usize, out: &mut CoordSeqBuilder) {
    if PointKey::new(edge.first().expect("edge has >= 2 vertices")) == from {
        out.extend_window(edge, skip..edge.len());
    } else {
        for point in edge.iter().rev().skip(skip) {
            out.push(point);
        }
    }
}

/// Dissolve linework edges into maximal chains, split only at genuine
/// junctions (nodes of degree other than two). Every edge is consumed once;
/// output order follows the lowest unused seed, so it is independent of
/// hash iteration. Node incidence is a CSR over the distinct end keys: one
/// map lookup per edge end and no per-node allocation, which is what an
/// overlay of thousands of atomic segments spends its time on otherwise.
pub(crate) fn merge_chains<E: AsRef<CoordSeq>>(edges: &[E]) -> crate::error::Result<Vec<CoordSeq>> {
    let mut ends: Vec<[PointKey; 2]> = Vec::with_capacity(edges.len());
    let mut live: Vec<usize> = Vec::with_capacity(edges.len());
    for (index, edge) in edges.iter().enumerate() {
        let edge = edge.as_ref();
        let mut points = edge.iter();
        let Some(first) = points.next().map(PointKey::new) else {
            continue;
        };
        if points.any(|point| PointKey::new(point) != first) {
            ends.push([
                first,
                PointKey::new(edge.last().expect("edge has >= 2 vertices")),
            ]);
            live.push(index);
        }
    }
    if live.len() <= 1 {
        let mut result = Vec::with_capacity(live.len());
        for index in live {
            let edge = edges
                .get(index)
                .expect("live edge ordinal comes from the input edges")
                .as_ref();
            let mut coords = CoordSeqBuilder::with_capacity(edge.axes(), edge.len());
            extend_chain(edge, ends[0][0], 0, &mut coords);
            result.push(coords.finish()?);
        }
        return Ok(result);
    }
    let mut node_of: HashMap<PointKey, u32> =
        HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    node_of.reserve(ends.len());
    let mut degree: Vec<u32> = Vec::new();
    let node_capacity = ends
        .len()
        .checked_mul(2)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    degree.reserve(node_capacity);
    let mut node_ends: Vec<[u32; 2]> = Vec::with_capacity(ends.len());
    for pair in &ends {
        let mut ids = [0_u32; 2];
        for (slot, key) in pair.iter().enumerate() {
            let next = u32::try_from(degree.len())
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
            let id = *node_of.entry(*key).or_insert(next);
            if id == next {
                degree.push(0);
            }
            degree[id as usize] = degree[id as usize]
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            ids[slot] = id;
        }
        node_ends.push(ids);
    }
    drop(node_of);
    let offset_capacity = degree
        .len()
        .checked_add(1)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let mut offsets: Vec<u32> = Vec::with_capacity(offset_capacity);
    offsets.push(0);
    for &count in &degree {
        let next = offsets[offsets.len() - 1]
            .checked_add(count)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        offsets.push(next);
    }
    let mut cursor = Vec::with_capacity(offsets.len());
    cursor.extend_from_slice(&offsets);
    let slot_count = usize::try_from(*offsets.last().expect("offsets has an initial zero"))
        .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
    let mut slots = vec![0_u32; slot_count];
    for (edge, ids) in node_ends.iter().enumerate() {
        for &id in ids {
            slots[cursor[id as usize] as usize] = edge as u32;
            cursor[id as usize] += 1;
        }
    }
    drop(cursor);
    let incident =
        |node: u32| &slots[offsets[node as usize] as usize..offsets[node as usize + 1] as usize];

    let mut used = Vec::with_capacity(ends.len());
    used.resize(ends.len(), false);
    let mut chains: Vec<CoordSeq> = Vec::with_capacity(ends.len());
    let walk =
        |seed: usize, start_slot: usize, used: &mut [bool]| -> crate::error::Result<CoordSeq> {
            let first = edges
                .get(
                    *live
                        .get(seed)
                        .expect("chain seed belongs to the live edges"),
                )
                .expect("live edge ordinal comes from the input edges")
                .as_ref();
            let mut coords = CoordSeqBuilder::with_capacity(first.axes(), first.len());
            let mut edge = seed;
            let mut from = node_ends[seed][start_slot];
            loop {
                used[edge] = true;
                let [a, b] = node_ends[edge];
                let from_key = ends[edge][usize::from(a != from)];
                extend_chain(
                    edges[live[edge]].as_ref(),
                    from_key,
                    usize::from(coords.len() != 0),
                    &mut coords,
                );
                let far = if a == from { b } else { a };
                if degree[far as usize] != 2 {
                    break;
                }
                match incident(far)
                    .iter()
                    .copied()
                    .find(|&next| next as usize != edge && !used[next as usize])
                {
                    Some(next) => {
                        edge = next as usize;
                        from = far;
                    },
                    None => break,
                }
            }
            coords.finish()
        };
    // Open chains first: every edge with a barrier end (degree != 2) starts
    // a chain that runs until the next barrier; edges interior to such a
    // chain are consumed mid-walk and skipped when their own seed turn comes.
    for seed in 0..ends.len() {
        if used[seed] {
            continue;
        }
        if let Some(slot) = (0..2).find(|&slot| degree[node_ends[seed][slot] as usize] != 2) {
            chains.push(walk(seed, slot, &mut used)?);
        }
    }
    // Whatever remains is an all-degree-2 cycle; walk each from its lowest
    // unused edge.
    for seed in 0..ends.len() {
        if !used[seed] {
            chains.push(walk(seed, 0, &mut used)?);
        }
    }
    Ok(chains)
}

pub(crate) fn build_overlay_shape(
    points: Vec<Point>,
    lines: &[CoordSeq],
    polygons: &[Polygon],
) -> crate::error::Result<Shape> {
    // Overlays node linework into atomic per-vertex segments; dissolve them back
    // into maximal chains (split only at genuine degree-`>= 3` junctions), so a
    // result reads as the cleanest lines representing its point set rather than
    // a `MultiLineString` of one segment per span.
    build_overlay_shape_from_chains(points, merge_chains(lines)?, polygons)
}

/// Serialize an already certified exact-partition plan without reclassifying
/// points or lines against rounded polygon coordinates. The exact selector has
/// already performed dimensional absorption, so any further coverage test here
/// would be a second topology owner.
pub(crate) fn build_certified_overlay_shape(
    points: Vec<Point>,
    lines: Vec<CoordSeq>,
    polygons: Vec<Polygon>,
) -> crate::error::Result<Shape> {
    build_certified_overlay_shape_with_axes(points, lines, polygons, CoordinateAxes::XY)
}

pub(crate) fn build_certified_overlay_shape_with_axes(
    mut points: Vec<Point>,
    mut lines: Vec<CoordSeq>,
    mut polygons: Vec<Polygon>,
    axes: CoordinateAxes,
) -> crate::error::Result<Shape> {
    points.sort_unstable_by(crate::geometry::compare_points);
    lines.sort_unstable_by(crate::geometry::compare_point_slices);
    polygons.sort_unstable_by(crate::geometry::compare_polygons);
    let point_part = (!points.is_empty())
        .then(|| Shape::narrow_points(axes, points))
        .transpose()?;
    let line_part = if lines.is_empty() {
        None
    } else {
        let mut proven = Vec::with_capacity(lines.len());
        for line in lines {
            proven.push(LineSeq::from_proven(line));
        }
        Some(Shape::narrow_lines(axes, proven))
    };
    let polygon_part = (!polygons.is_empty()).then(|| Shape::narrow_polygons(axes, polygons));
    let mut parts = Vec::with_capacity(3);
    parts.extend([point_part, line_part, polygon_part].into_iter().flatten());
    match <[Shape; 1]>::try_from(parts) {
        Ok([single]) => Ok(single),
        Err(parts) => Shape::geometry_collection(parts),
    }
}

/// Canonicalize overlay buckets into the narrowest representable `Shape`.
///
/// Points and lines covered by the polygon output are absorbed (a piece inside
/// or on a polygon is not an independent component). Each surviving bucket
/// narrows to a leaf or multi-geometry; mixed dimensions yield a
/// `GeometryCollection` ordered by ascending dimension (point, line, polygon).
/// The n-ary overlay owner applies operation-specific typed-empty normalization
/// after this bucket assembly returns an empty result.
///
/// [`build_overlay_shape`] for a lane that dissolved its linework itself.
pub(crate) fn build_overlay_shape_from_chains(
    points: Vec<Point>,
    lines: Vec<CoordSeq>,
    input_polygons: &[Polygon],
) -> crate::error::Result<Shape> {
    let mut polygons = Vec::with_capacity(input_polygons.len());
    for polygon in input_polygons {
        polygons.push(drop_collinear_ring_vertices(polygon)?);
    }
    let mut polygon_bounds = Vec::with_capacity(polygons.len());
    for polygon in &polygons {
        polygon_bounds.push(polygon.bounds());
    }
    let mut surviving = Vec::with_capacity(points.len());
    for point in points {
        let covered_by_polygon = polygons
            .iter()
            .zip(&polygon_bounds)
            .any(|(polygon, bounds)| {
                bounds.is_none_or(|bounds| bounds.contains_xy(point.xy()))
                    && polygon.covers_point(point)
            });
        let covered_by_line = lines.iter().any(|line| {
            line.segment_pairs()
                .any(|[start, end]| point_on_segment(point, start, end))
        });
        if !covered_by_polygon && !covered_by_line {
            surviving.push(point);
        }
    }
    dedup_points(&mut surviving)?;
    let line_part = if lines.is_empty() {
        None
    } else {
        let mut proven = Vec::with_capacity(lines.len());
        for line in lines {
            proven.push(LineSeq::from_proven(line));
        }
        Some(Shape::narrow_lines(CoordinateAxes::XY, proven))
    };

    let point_part = (!surviving.is_empty())
        .then(|| Shape::narrow_points(CoordinateAxes::XY, surviving))
        .transpose()?;
    let polygon_part =
        (!polygons.is_empty()).then(|| Shape::narrow_polygons(CoordinateAxes::XY, polygons));

    let mut parts = Vec::with_capacity(3);
    parts.extend([point_part, line_part, polygon_part].into_iter().flatten());
    match <[Shape; 1]>::try_from(parts) {
        Ok([single]) => Ok(single),
        Err(parts) if parts.is_empty() => Ok(Shape::typed_empty(
            GeometryKind::GeometryCollection,
            CoordinateAxes::XY,
        )),
        Err(parts) => Shape::geometry_collection(parts),
    }
}
