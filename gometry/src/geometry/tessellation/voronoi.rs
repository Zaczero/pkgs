use spade::handles::VoronoiVertex::{Inner, Outer};

use super::*;
use crate::error::Result;

pub(crate) fn voronoi_triangulation(
    sites: &[Point],
    tolerance: f64,
) -> Result<DelaunayTriangulation<Point2<f64>>> {
    // tolerance>0 snaps near-coincident sites before insertion (geo configures
    // spade's snap_radius for the same effect). A uniform grid of cell-size
    // `tolerance` buckets the candidates, so each snap probes only the 3×3
    // neighbourhood — O(n) overall, versus geo's O(n²) all-pairs scan.
    // Single collect into spade points: tolerance==0 maps XY in one pass;
    // tolerance>0 still needs the snap staging buffer first.
    let sites = if tolerance > 0.0 {
        snap_sites(sites, tolerance)
            .into_iter()
            .map(spade_point)
            .collect()
    } else {
        sites.iter().map(|point| spade_point(point.xy())).collect()
    };
    DelaunayTriangulation::<Point2<f64>>::bulk_load(sites)
        .map_err(|error| GeometryErrorKind::voronoi(error.to_string()))
}

/// Snap sites within `radius` to a shared representative via a uniform grid
/// (cell size = `radius`): each point probes its 3×3 neighbour cells for an
/// existing representative, snapping to the nearest within range or seeding a
/// new one. First-seen order is preserved, matching geo's snap semantics at
/// O(n) instead of O(n²).
pub(crate) fn snap_sites(sites: &[Point], radius: f64) -> Vec<XY> {
    let inverse = 1.0 / radius;
    let cell = |value: f64| (value * inverse).floor() as i64;
    let mut buckets: HashMap<(i64, i64), Vec<XY>> = HashMap::with_capacity(sites.len());
    let mut snapped = Vec::with_capacity(sites.len());
    for site in sites {
        let point = site.xy();
        let (cx, cy) = (cell(point.x), cell(point.y));
        let mut best: Option<(f64, XY)> = None;
        for dx in -1..=1 {
            for dy in -1..=1 {
                let Some(entries) = buckets.get(&(cx + dx, cy + dy)) else {
                    continue;
                };
                for &existing in entries {
                    let distance = point_distance(existing, point);
                    if distance < radius && best.is_none_or(|(closest, _)| distance < closest) {
                        best = Some((distance, existing));
                    }
                }
            }
        }
        let representative = if let Some((_, existing)) = best {
            existing
        } else {
            buckets.entry((cx, cy)).or_default().push(point);
            point
        };
        snapped.push(representative);
    }
    snapped
}

/// The tight envelope of a triangulation's vertices.
pub(crate) fn triangulation_bounds(triangulation: &DelaunayTriangulation<Point2<f64>>) -> Bounds {
    Bounds::from_xy_iter(triangulation.vertices().map(|vertex| {
        let position = vertex.position();
        XY::new(position.x, position.y)
    }))
}

/// Padded bounds: each side grown by `factor * max(width, height)`. A factor of
/// 0.5 is the `PostGIS` / geo default for the unbounded-cell clip.
pub(crate) fn padded_bounds(base: Bounds, factor: f64) -> Bounds {
    base.pad_by_span(factor)
}

/// Raw Voronoi cells from the Delaunay dual: per site, the circumcenters of its
/// incident faces plus, for hull sites, a point far along each unbounded edge's
/// ray, sorted by angle around the site. Callers clip these oversized cells.
/// Returns `(cells, base_bounds)`; `None`-equivalent collinear input yields a
/// `Voronoi` error (no 2-D cells exist). Ported from geo's
/// `build_raw_voronoi_cells`.
pub(crate) fn build_raw_voronoi_cells(
    sites: &[Point],
    tolerance: f64,
) -> Result<(Vec<Polygon>, [f64; 4])> {
    let triangulation = voronoi_triangulation(sites, tolerance)?;
    if triangulation.num_vertices() < 2 {
        return Ok((Vec::new(), [0.0; 4]));
    }
    let base = triangulation_bounds(&triangulation);
    let padded = padded_bounds(base, 0.5);
    let extension = ((padded.maxx() - padded.minx()) + (padded.maxy() - padded.miny())) * 2.0;

    let mut cells = Vec::new();
    for face in triangulation.voronoi_faces() {
        let site = face.as_delaunay_vertex().position();
        let mut vertices: Vec<XY> = Vec::new();
        let mut rays: Vec<(XY, XY)> = Vec::new();
        for edge in face.adjacent_edges() {
            for vertex in [edge.from(), edge.to()] {
                if let Inner(inner) = vertex {
                    let center = inner.circumcenter();
                    let point = XY::new(center.x, center.y);
                    if !vertices.contains(&point) {
                        vertices.push(point);
                    }
                }
            }
            // An inner→outer (or outer→inner) edge is an unbounded ray from the
            // inner circumcenter along the outer edge's direction.
            let ray = match (edge.from(), edge.to()) {
                (Inner(inner), Outer(outer)) | (Outer(outer), Inner(inner)) => {
                    let origin = inner.circumcenter();
                    let direction = outer.direction_vector();
                    Some((
                        XY::new(origin.x, origin.y),
                        XY::new(direction.x, direction.y),
                    ))
                },
                _ => None,
            };
            if let Some(ray) = ray {
                rays.push(ray);
            }
        }
        for (origin, direction) in &rays {
            let length = (direction.x * direction.x + direction.y * direction.y).sqrt();
            if length == 0.0 || !length.is_finite() {
                continue;
            }
            vertices.push(XY::new(
                origin.x + direction.x / length * extension,
                origin.y + direction.y / length * extension,
            ));
        }
        if vertices.len() < 3 {
            continue;
        }
        vertices.sort_by(|left, right| {
            pseudo_angle(left.x - site.x, left.y - site.y)
                .total_cmp(&pseudo_angle(right.x - site.x, right.y - site.y))
        });
        let mut shell: Vec<Point> = Vec::with_capacity(vertices.len() + 1);
        shell.extend(
            vertices
                .iter()
                .map(|vertex| Point::new_unchecked_xy(vertex.x, vertex.y)),
        );
        shell.push(shell[0]);
        cells.push(Polygon::new(Ring::from_trusted_closed(shell), Vec::new()));
    }

    // Collinear input has no 2-D cells (only bisector lines) — surface it as a
    // Voronoi error, mirroring geo's `CollinearInput`.
    if cells.is_empty() {
        return Err(GeometryErrorKind::voronoi(
            "input points are collinear; Voronoi cells cannot be computed",
        ));
    }
    Ok((cells, base.into_array()))
}

/// Clip raw (oversized) Voronoi cells to the boundary. Rect modes use the
/// Sutherland-Hodgman rectangle clip; the polygon mode intersects each cell
/// against the clip polygon. Cells already inside the clip rectangle skip the
/// clip entirely (geo's interior-cell fast path).
pub(crate) fn clip_voronoi_cells(
    raw_cells: Vec<Polygon>,
    base: [f64; 4],
    boundary: VoronoiBoundary<'_>,
) -> Vec<Polygon> {
    let base_bounds = Bounds::new_unchecked(base[0], base[1], base[2], base[3]);
    let rect = match boundary {
        VoronoiBoundary::Padded => padded_bounds(base_bounds, 0.5),
        VoronoiBoundary::Envelope => base_bounds,
        VoronoiBoundary::Polygon(polygon) => polygon.bounds().unwrap_or(base_bounds),
    };
    // Clip each cell INDIVIDUALLY — adjacent cells share edges, so a batched
    // overlay would dissolve them and lose the 1:1 site→cell correspondence
    // (the reason geo intersects cell-by-cell). Cells already inside the clip
    // rectangle skip clipping entirely (geo's interior-cell fast path).
    match boundary {
        VoronoiBoundary::Padded | VoronoiBoundary::Envelope => raw_cells
            .into_iter()
            .flat_map(|cell| {
                if cell_inside_rect(&cell, rect) {
                    vec![cell]
                } else {
                    clip_polygonal_parts(std::slice::from_ref(&cell), rect)
                }
            })
            .collect(),
        VoronoiBoundary::Polygon(clip) => {
            let clip = std::slice::from_ref(clip);
            raw_cells
                .into_iter()
                .flat_map(|cell| {
                    binary_areal_overlay(std::slice::from_ref(&cell), clip, OverlayOp::Intersection)
                })
                .collect()
        },
    }
}

/// Whether a cell's envelope is fully within the clip rectangle — then the
/// clip is a no-op and is skipped.
pub(crate) fn cell_inside_rect(cell: &Polygon, rect: Bounds) -> bool {
    cell.bounds().is_some_and(|bounds| {
        bounds.minx() >= rect.minx()
            && bounds.maxx() <= rect.maxx()
            && bounds.miny() >= rect.miny()
            && bounds.maxy() <= rect.maxy()
    })
}

/// Voronoi edges as segment endpoints, clipped to the 50%-padded bounds
/// (`PostGIS` `ST_VoronoiLines`). Ported from geo's
/// `voronoi_edges_with_params`: inner-inner edges are emitted directly,
/// inner-outer rays are clipped to the box, and outer-outer edges (collinear
/// input) become perpendicular bisectors.
pub(crate) fn voronoi_edge_segments(sites: &[Point], tolerance: f64) -> Result<Vec<[XY; 2]>> {
    let triangulation = voronoi_triangulation(sites, tolerance)?;
    let base = triangulation_bounds(&triangulation);
    let bounds = padded_bounds(base, 0.5);
    let width = base.maxx() - base.minx();
    let height = base.maxy() - base.miny();
    let reach = width + height;

    let mut sorted_sites: Vec<XY> = triangulation
        .vertices()
        .map(|vertex| {
            let position = vertex.position();
            XY::new(position.x, position.y)
        })
        .collect();
    sorted_sites.sort_by(|left, right| {
        left.x
            .total_cmp(&right.x)
            .then_with(|| left.y.total_cmp(&right.y))
    });

    let mut edges: Vec<[XY; 2]> = Vec::new();
    let mut outer_edge_counter = 0;
    for edge in triangulation.undirected_voronoi_edges() {
        match edge.vertices() {
            [Inner(from), Inner(to)] => {
                let from = from.circumcenter();
                let to = to.circumcenter();
                edges.push([XY::new(from.x, from.y), XY::new(to.x, to.y)]);
            },
            [Inner(from), Outer(outer)] | [Outer(outer), Inner(from)] => {
                let start = from.circumcenter();
                let direction = outer.direction_vector();
                let extended =
                    XY::new(start.x + direction.x * reach, start.y + direction.y * reach);
                let start = XY::new(start.x, start.y);
                if let Some(hit) = closest_rect_intersection(start, extended, bounds) {
                    edges.push([start, hit]);
                }
            },
            [Outer(first), Outer(_)] => {
                // Collinear input: N-1 perpendicular bisectors between sorted
                // sites; `outer_edge_counter` indexes the next consecutive pair.
                if outer_edge_counter + 1 >= sorted_sites.len() {
                    continue;
                }
                let mid = XY::new(
                    f64::midpoint(
                        sorted_sites[outer_edge_counter].x,
                        sorted_sites[outer_edge_counter + 1].x,
                    ),
                    f64::midpoint(
                        sorted_sites[outer_edge_counter].y,
                        sorted_sites[outer_edge_counter + 1].y,
                    ),
                );
                let direction = first.direction_vector();
                edges.push([
                    XY::new(mid.x - direction.x * reach, mid.y - direction.y * reach),
                    XY::new(mid.x + direction.x * reach, mid.y + direction.y * reach),
                ]);
                outer_edge_counter += 1;
            },
        }
    }
    Ok(edges)
}

/// Monotone pseudo-angle around the origin — same cyclic order as `atan2` for
/// convex Voronoi cells, without a libm call.
pub(crate) fn pseudo_angle(dx: f64, dy: f64) -> f64 {
    let ratio = dy / (dx.abs() + dy.abs());
    if dx < 0.0 {
        if dy < 0.0 { ratio - 2.0 } else { ratio + 2.0 }
    } else {
        ratio
    }
}

/// The intersection of segment `start`→`extended` with the boundary rectangle
/// closest to `start` — the clip point for an unbounded Voronoi ray. The ray is
/// extended well past the box, so a crossing always exists for finite input.
pub(crate) fn closest_rect_intersection(start: XY, extended: XY, bounds: Bounds) -> Option<XY> {
    let corners = bounds.corners().map(|corner| XY::new(corner[0], corner[1]));
    let ray = Segment {
        start,
        end: extended,
    };
    (0..4)
        .filter_map(|index| {
            let side = Segment {
                start: corners[index],
                end: corners[(index + 1) % 4],
            };
            // Only proper crossings (not collinear overlaps), matching geo's
            // SinglePoint-only filter.
            (segment_contact(ray, side) != SegmentContact::None)
                .then(|| line_intersection(ray, side))
                .flatten()
        })
        .min_by(|left, right| {
            point_distance_squared(start, *left).total_cmp(&point_distance_squared(start, *right))
        })
}
