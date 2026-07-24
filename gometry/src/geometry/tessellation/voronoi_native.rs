use super::*;

/// Minimum-area enclosing rectangle of a CCW convex ring (open, >= 3
/// vertices) by rotating calipers: one side of the optimum is collinear
/// with a hull edge, and the three support vertices (along-max, outward-max,
/// along-min) only ever advance forward around the hull, so the sweep is
/// O(h) after the hull.
///
/// `hull` must be an open CCW ring from [`monotone_chain_hull`].
pub(crate) fn minimum_area_rectangle(hull: &[Point]) -> CoordSeq {
    let count = hull.len();
    let point = |index: usize| hull[index % count];
    let dot = |direction: (f64, f64), index: usize| {
        let p = point(index);
        direction.0 * p.x + direction.1 * p.y
    };
    // Bounded advance: projections under a fixed direction are unimodal
    // around a convex ring, so the support stops within one lap — the
    // explicit bound also hard-caps any degenerate input.
    let advance = |direction: (f64, f64), mut index: usize| {
        index %= count;
        for _ in 0..count {
            let next = wrap_index(index + 1, count);
            if dot(direction, next) > dot(direction, index) {
                index = next;
            } else {
                break;
            }
        }
        index
    };
    // CCW angular order seen from any edge: edge start, then the along-max
    // support, then the outward-max, then the along-min — each pointer is
    // clamped to never trail the previous one as the edge rotates.
    let (mut along_max, mut outward_max, mut along_min) = (1, 1, 1);
    let mut best: Option<(f64, [Point; 5])> = None;
    for edge in 0..count {
        let start = point(edge);
        let end = point(edge + 1);
        let length = point_distance(start, end);
        if length == 0.0 {
            continue;
        }
        let along = ((end.x - start.x) / length, (end.y - start.y) / length);
        let outward = (-along.1, along.0);
        along_max = advance(along, along_max.max(edge + 1)) % count;
        outward_max = advance(outward, outward_max.max(along_max)) % count;
        along_min = advance((-along.0, -along.1), along_min.max(outward_max)) % count;
        let base = dot(outward, edge);
        let height = dot(outward, outward_max) - base;
        let max_along = dot(along, along_max);
        let min_along = dot(along, along_min);
        let area = (max_along - min_along) * height;
        if best.as_ref().is_none_or(|(smallest, _)| area < *smallest) {
            let corner = |a: f64, o: f64| {
                Point::new_unchecked_xy(along.0 * a + outward.0 * o, along.1 * a + outward.1 * o)
            };
            best = Some((area, [
                corner(min_along, base),
                corner(max_along, base),
                corner(max_along, base + height),
                corner(min_along, base + height),
                corner(min_along, base),
            ]));
        }
    }
    let (_, corners) = best.expect("non-degenerate hull has a non-zero edge");
    CoordSeq::from_points(&corners)
}

/// The Voronoi cells of `points` (deduplicated sites), each one carved from
/// the boundary rectangle by successive half-plane clips against the site's
/// Delaunay neighbors' perpendicular bisectors. The rect bounds every cell,
/// so hull sites need no infinite-ray construction; the rect is the sites'
/// envelope (`Envelope`) or that envelope padded by half its larger span on
/// every side (`Padded` — the geo engine's convention, which this lane
/// matches). `None` for degenerate (collinear) site sets — the caller keeps
/// the geo engine for those.
#[expect(
    clippy::too_many_lines,
    reason = "cohesive kernel; splitting obscures the algorithm"
)]
pub(crate) fn native_voronoi_cells(
    points: &[Point],
    boundary: &VoronoiBoundary<'_>,
) -> Option<Vec<Polygon>> {
    let sites: Vec<delaunator::Point> = points
        .iter()
        .map(|point| delaunator::Point {
            x: point.x,
            y: point.y,
        })
        .collect();
    let triangulation = delaunator::triangulate(&sites);
    if triangulation.triangles.is_empty() {
        return None;
    }
    // Neighbor lists: both endpoints of every triangle edge, deduplicated
    // (interior edges arrive from two triangles).
    let site_count = points.len();
    let mut neighbor_counts = vec![0_usize; site_count];
    for edge in 0..triangulation.triangles.len() {
        let from = triangulation.triangles[edge];
        let to = triangulation.triangles[delaunator::next_halfedge(edge)];
        neighbor_counts[from] += 1;
        neighbor_counts[to] += 1;
    }
    let mut neighbor_offsets = Vec::with_capacity(site_count + 1);
    neighbor_offsets.push(0);
    for count in neighbor_counts {
        neighbor_offsets.push(neighbor_offsets.last().copied().unwrap_or(0) + count);
    }
    let mut neighbors = vec![0_u32; *neighbor_offsets.last().unwrap_or(&0)];
    let mut write_heads = neighbor_offsets.clone();
    for edge in 0..triangulation.triangles.len() {
        let from = triangulation.triangles[edge];
        let to = triangulation.triangles[delaunator::next_halfedge(edge)];
        let from_head = &mut write_heads[from];
        neighbors[*from_head] = to as u32;
        *from_head += 1;
        let to_head = &mut write_heads[to];
        neighbors[*to_head] = from as u32;
        *to_head += 1;
    }
    let mut compact_offsets = vec![0_usize];
    let mut compact_write = 0_usize;
    for site in 0..site_count {
        let start = neighbor_offsets[site];
        let end = neighbor_offsets[site + 1];
        if start < end {
            neighbors[start..end].sort_unstable();
            let mut dedup_end = start;
            for read in (start + 1)..end {
                if neighbors[read] != neighbors[dedup_end] {
                    dedup_end += 1;
                    neighbors[dedup_end] = neighbors[read];
                }
            }
            dedup_end += 1;
            let dedup_len = dedup_end - start;
            if compact_write != start {
                neighbors.copy_within(start..dedup_end, compact_write);
            }
            compact_write += dedup_len;
        }
        compact_offsets.push(compact_write);
    }
    neighbors.truncate(compact_write);
    let neighbor_offsets = compact_offsets;
    let bounds = voronoi_rect(points, boundary);
    let rect = [
        XY::new(bounds[0], bounds[1]),
        XY::new(bounds[2], bounds[1]),
        XY::new(bounds[2], bounds[3]),
        XY::new(bounds[0], bounds[3]),
    ];
    let mut ring: Vec<XY> = Vec::with_capacity(16);
    let mut clipped: Vec<XY> = Vec::with_capacity(16);
    let mut cells = Vec::with_capacity(points.len());
    for site in 0..site_count {
        let start = neighbor_offsets[site];
        let end = neighbor_offsets[site + 1];
        ring.clear();
        ring.extend_from_slice(&rect);
        let origin = XY::new(points[site].x, points[site].y);
        for &neighbor in &neighbors[start..end] {
            let other = points[neighbor as usize];
            // Keep the half-plane closer to `site`: f(P) = d . (P - mid) <= 0.
            let (dx, dy) = (other.x - origin.x, other.y - origin.y);
            let (mx, my) = (
                f64::midpoint(other.x, origin.x),
                f64::midpoint(other.y, origin.y),
            );
            clipped.clear();
            for index in 0..ring.len() {
                let current = ring[index];
                let next = ring[wrap_index(index + 1, ring.len())];
                let f_current = dx * (current.x - mx) + dy * (current.y - my);
                let f_next = dx * (next.x - mx) + dy * (next.y - my);
                if f_current <= 0.0 {
                    clipped.push(current);
                }
                // Sign change: the denominator cannot vanish (one side is
                // strictly negative, the other is not).
                if (f_current < 0.0) != (f_next < 0.0) {
                    let t = f_current / (f_current - f_next);
                    clipped.push(XY::new(
                        current.x + t * (next.x - current.x),
                        current.y + t * (next.y - current.y),
                    ));
                }
            }
            std::mem::swap(&mut ring, &mut clipped);
            if ring.len() < 3 {
                break;
            }
        }
        if ring.len() < 3 {
            // A site's cell can only degenerate through floating collapse;
            // surrender the whole diagram to the geo engine.
            return None;
        }
        let mut shell: Vec<Point> = ring
            .iter()
            .map(|point| Point::new_unchecked_xy(point.x, point.y))
            .collect();
        shell.push(shell[0]);
        cells.push(Polygon::new(Ring::from_trusted_closed(shell), Vec::new()));
    }
    Some(cells)
}

/// The diagram rectangle per boundary mode: the sites' envelope
/// (`Envelope`), padded by half its larger span per side (`Padded` — the
/// geo convention).
pub(crate) fn voronoi_rect(points: &[Point], boundary: &VoronoiBoundary<'_>) -> [f64; 4] {
    let bounds = Bounds::from_points(points.iter().copied());
    if matches!(boundary, VoronoiBoundary::Envelope) {
        bounds.into_array()
    } else {
        bounds.pad_by_span(0.5).into_array()
    }
}
