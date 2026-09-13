use crate::geometry::{
    CoordSeq, CoordinateAxes, Coordinates as _, GeometryKind, HashMap, HashSet, LineSeq, Point,
    PointKey, Polygon, Ring, Segment, Shape, point_on_segment,
};
use crate::{try_push_vec, try_reserve_vec};

/// Order-preserving topological dedup of overlay output points.
pub(crate) fn dedup_points(points: &mut Vec<Point>) -> crate::error::Result<()> {
    let mut seen: HashSet<PointKey> =
        HashSet::with_hasher(crate::collections::collection_hash_state()?.clone());
    seen.try_reserve(points.len())
        .map_err(crate::error::Error::from)?;
    points.retain(|point| seen.insert(PointKey::new(*point)));
    Ok(())
}
/// An overlay nodes rings wherever operands meet, and a node on a straight
/// run says nothing about the region: every ring leaves with each vertex
/// that lies on the segment between its neighbours removed.
fn drop_collinear_ring_vertices(polygon: &Polygon) -> crate::error::Result<Polygon> {
    let simplify = |ring: &Ring| {
        let mut vertices = Vec::new();
        try_reserve_vec(&mut vertices, ring.coord_count())?;
        for point in ring.coords().iter_coords() {
            try_push_vec(&mut vertices, point)?;
        }
        drop_collinear_vertices(vertices)
            .and_then(|vertices| CoordSeq::try_from_points(&vertices).map(Ring::from_closed))
    };
    let shell = simplify(&polygon.shell)?;
    let mut holes = Vec::new();
    try_reserve_vec(&mut holes, polygon.holes.len())?;
    for hole in polygon.holes.iter() {
        try_push_vec(&mut holes, simplify(hole)?)?;
    }
    Polygon::new(shell, holes)
}

/// One pass with a stack, then the seam: a vertex on the segment between
/// its neighbours goes, and removing it re-examines the neighbour it exposed.
/// A ring of three vertices or fewer is left alone, since a collinear whole
/// ring is a degeneracy for the caller to see, not to hide.
pub(crate) fn drop_collinear_vertices(
    mut vertices: Vec<Point>,
) -> crate::error::Result<Vec<Point>> {
    if vertices.len() <= 4 {
        return Ok(vertices);
    }
    vertices.pop();
    let mut kept = Vec::new();
    try_reserve_vec(&mut kept, vertices.len())?;
    for vertex in vertices {
        while kept.len() >= 2
            && point_on_segment(kept[kept.len() - 1], kept[kept.len() - 2], vertex)
        {
            kept.pop();
        }
        try_push_vec(&mut kept, vertex)?;
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
    try_push_vec(&mut kept, first)?;
    Ok(kept)
}

/// One edge of a linework arrangement as the dissolve sees it: its two end
/// keys, and its points streamed in either direction.
pub(crate) trait ChainEdge {
    /// `None` for an edge with no extent (fewer than two distinct vertices),
    /// which is not linework and vanishes from the result.
    fn ends(&self) -> Option<[PointKey; 2]>;
    /// Append the edge's points departing from `from`, dropping the first
    /// `skip` (the join vertex the chain already holds).
    fn extend_from(
        &self,
        from: PointKey,
        skip: usize,
        out: &mut Vec<Point>,
    ) -> crate::error::Result<()>;
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

    fn extend_from(
        &self,
        from: PointKey,
        skip: usize,
        out: &mut Vec<Point>,
    ) -> crate::error::Result<()> {
        try_reserve_vec(out, self.coord_count().saturating_sub(skip))?;
        if PointKey::new(self.first().expect("edge has >= 2 vertices")) == from {
            out.extend(self.iter().skip(skip));
        } else {
            out.extend(self.iter().rev().skip(skip));
        }
        Ok(())
    }
}

impl ChainEdge for Segment {
    fn ends(&self) -> Option<[PointKey; 2]> {
        let ends = [PointKey::new(self.start), PointKey::new(self.end)];
        (ends[0] != ends[1]).then_some(ends)
    }

    fn extend_from(
        &self,
        from: PointKey,
        skip: usize,
        out: &mut Vec<Point>,
    ) -> crate::error::Result<()> {
        let (near, far) = if PointKey::new(self.start) == from {
            (self.start, self.end)
        } else {
            (self.end, self.start)
        };
        if skip == 0 {
            try_push_vec(out, near.point())?;
        }
        try_push_vec(out, far.point())?;
        Ok(())
    }
}

/// Dissolve linework edges into maximal chains, split only at genuine
/// junctions (nodes of degree other than two). Every edge is consumed once;
/// output order follows the lowest unused seed, so it is independent of
/// hash iteration. Node incidence is a CSR over the distinct end keys: one
/// map lookup per edge end and no per-node allocation, which is what an
/// overlay of thousands of atomic segments spends its time on otherwise.
pub(crate) fn merge_chains(edges: &[CoordSeq]) -> crate::error::Result<Vec<CoordSeq>> {
    merge_chain_edges(edges)
}

pub(crate) fn merge_chain_edges<E: ChainEdge>(edges: &[E]) -> crate::error::Result<Vec<CoordSeq>> {
    let mut ends: Vec<[PointKey; 2]> = Vec::new();
    try_reserve_vec(&mut ends, edges.len())?;
    let mut live: Vec<usize> = Vec::new();
    try_reserve_vec(&mut live, edges.len())?;
    for (index, edge) in edges.iter().enumerate() {
        if let Some(pair) = edge.ends() {
            try_push_vec(&mut ends, pair)?;
            try_push_vec(&mut live, index)?;
        }
    }
    if live.len() <= 1 {
        let mut result = Vec::new();
        try_reserve_vec(&mut result, live.len())?;
        for index in live {
            let mut coords = Vec::new();
            edges[index].extend_from(ends[0][0], 0, &mut coords)?;
            try_push_vec(&mut result, CoordSeq::try_from_points(&coords)?)?;
        }
        return Ok(result);
    }
    let mut node_of: HashMap<PointKey, u32> =
        HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    node_of
        .try_reserve(ends.len())
        .map_err(crate::error::Error::from)?;
    let mut degree: Vec<u32> = Vec::new();
    let node_capacity = ends
        .len()
        .checked_mul(2)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    try_reserve_vec(&mut degree, node_capacity)?;
    let mut node_ends: Vec<[u32; 2]> = Vec::new();
    try_reserve_vec(&mut node_ends, ends.len())?;
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
    let offset_capacity = degree
        .len()
        .checked_add(1)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let mut offsets: Vec<u32> = Vec::new();
    try_reserve_vec(&mut offsets, offset_capacity)?;
    offsets.push(0);
    for &count in &degree {
        let next = offsets[offsets.len() - 1]
            .checked_add(count)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        offsets.push(next);
    }
    let mut cursor = Vec::new();
    try_reserve_vec(&mut cursor, offsets.len())?;
    cursor.extend_from_slice(&offsets);
    let slot_count = usize::try_from(*offsets.last().expect("offsets has an initial zero"))
        .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
    let mut slots = Vec::new();
    try_reserve_vec(&mut slots, slot_count)?;
    slots.resize(slot_count, 0_u32);
    for (edge, ids) in node_ends.iter().enumerate() {
        for &id in ids {
            slots[cursor[id as usize] as usize] = edge as u32;
            cursor[id as usize] += 1;
        }
    }
    let incident =
        |node: u32| &slots[offsets[node as usize] as usize..offsets[node as usize + 1] as usize];

    let mut used = Vec::new();
    try_reserve_vec(&mut used, ends.len())?;
    used.resize(ends.len(), false);
    let mut chains: Vec<CoordSeq> = Vec::new();
    try_reserve_vec(&mut chains, ends.len())?;
    let walk =
        |seed: usize, start_slot: usize, used: &mut [bool]| -> crate::error::Result<CoordSeq> {
            let mut coords: Vec<Point> = Vec::new();
            let mut edge = seed;
            let mut from = node_ends[seed][start_slot];
            loop {
                used[edge] = true;
                let [a, b] = node_ends[edge];
                let from_key = ends[edge][usize::from(a != from)];
                edges[live[edge]].extend_from(
                    from_key,
                    usize::from(!coords.is_empty()),
                    &mut coords,
                )?;
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
            CoordSeq::try_from_points(&coords)
        };
    // Open chains first: every edge with a barrier end (degree != 2) starts
    // a chain that runs until the next barrier; edges interior to such a
    // chain are consumed mid-walk and skipped when their own seed turn comes.
    for seed in 0..ends.len() {
        if used[seed] {
            continue;
        }
        if let Some(slot) = (0..2).find(|&slot| degree[node_ends[seed][slot] as usize] != 2) {
            try_push_vec(&mut chains, walk(seed, slot, &mut used)?)?;
        }
    }
    // Whatever remains is an all-degree-2 cycle; walk each from its lowest
    // unused edge.
    for seed in 0..ends.len() {
        if !used[seed] {
            try_push_vec(&mut chains, walk(seed, 0, &mut used)?)?;
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
    points.sort_by(crate::geometry::compare_points);
    lines.sort_by(crate::geometry::compare_point_slices);
    polygons.sort_by(crate::geometry::compare_polygons);
    let point_part = (!points.is_empty())
        .then(|| Shape::narrow_points(axes, points))
        .transpose()?;
    let line_part = if lines.is_empty() {
        None
    } else {
        let mut proven = Vec::new();
        try_reserve_vec(&mut proven, lines.len())?;
        for line in lines {
            try_push_vec(&mut proven, LineSeq::from_proven(line))?;
        }
        Some(Shape::narrow_lines(axes, proven)?)
    };
    let polygon_part = (!polygons.is_empty())
        .then(|| Shape::narrow_polygons(axes, polygons))
        .transpose()?;
    let mut parts = Vec::new();
    try_reserve_vec(&mut parts, 3)?;
    for part in [point_part, line_part, polygon_part].into_iter().flatten() {
        try_push_vec(&mut parts, part)?;
    }
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
    let mut polygons = Vec::new();
    try_reserve_vec(&mut polygons, input_polygons.len())?;
    for polygon in input_polygons {
        try_push_vec(&mut polygons, drop_collinear_ring_vertices(polygon)?)?;
    }
    let mut polygon_bounds = Vec::new();
    try_reserve_vec(&mut polygon_bounds, polygons.len())?;
    for polygon in &polygons {
        try_push_vec(&mut polygon_bounds, polygon.bounds())?;
    }
    let mut surviving = Vec::new();
    try_reserve_vec(&mut surviving, points.len())?;
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
            try_push_vec(&mut surviving, point)?;
        }
    }
    dedup_points(&mut surviving)?;

    let line_part = if lines.is_empty() {
        None
    } else {
        let mut proven = Vec::new();
        try_reserve_vec(&mut proven, lines.len())?;
        for line in lines {
            try_push_vec(&mut proven, LineSeq::from_proven(line))?;
        }
        Some(Shape::narrow_lines(CoordinateAxes::XY, proven)?)
    };

    let point_part = (!surviving.is_empty())
        .then(|| Shape::narrow_points(CoordinateAxes::XY, surviving))
        .transpose()?;
    let polygon_part = (!polygons.is_empty())
        .then(|| Shape::narrow_polygons(CoordinateAxes::XY, polygons))
        .transpose()?;

    let mut parts = Vec::new();
    try_reserve_vec(&mut parts, 3)?;
    for part in [point_part, line_part, polygon_part].into_iter().flatten() {
        try_push_vec(&mut parts, part)?;
    }
    match <[Shape; 1]>::try_from(parts) {
        Ok([single]) => Ok(single),
        Err(parts) if parts.is_empty() => Ok(Shape::typed_empty(
            GeometryKind::GeometryCollection,
            CoordinateAxes::XY,
        )),
        Err(parts) => Shape::geometry_collection(parts),
    }
}
