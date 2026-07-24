#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;

/// Optional CDT mesh-refinement controls. Active when either quality constraint
/// is set. Ruppert-style refinement is bounded; incomplete meshes are rejected
/// rather than returned as if the requested quality had been satisfied.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) enum CdtRefinement {
    #[default]
    Off,
    On {
        min_angle: Option<crate::Positive>,
        max_area: Option<crate::Positive>,
    },
}

impl CdtRefinement {
    pub(crate) const fn active(self) -> bool {
        matches!(self, Self::On { .. })
    }
}

fn refinement_parameters(
    refinement: CdtRefinement,
    max_additional_vertices: usize,
) -> RefinementParameters<f64> {
    let mut params = RefinementParameters::new()
        .exclude_outer_faces(true)
        // `max_area` alone must not silently opt into Spade's default 30°
        // angle target; each public constraint controls only its own quality.
        .with_angle_limit(AngleLimit::from_deg(0.0))
        .with_max_additional_vertices(max_additional_vertices);
    let CdtRefinement::On {
        min_angle,
        max_area,
    } = refinement
    else {
        return params;
    };
    if let Some(angle) = min_angle {
        params = params.with_angle_limit(AngleLimit::from_deg(angle.get()));
    }
    if let Some(area) = max_area {
        params = params.with_max_allowed_area(area.get());
    }
    params
}

fn triangle_min_angle_degrees(a: Point2<f64>, b: Point2<f64>, c: Point2<f64>) -> f64 {
    fn angle(a: Point2<f64>, center: Point2<f64>, b: Point2<f64>) -> f64 {
        let (ax, ay) = (a.x - center.x, a.y - center.y);
        let (bx, by) = (b.x - center.x, b.y - center.y);
        let denominator = ax.hypot(ay) * bx.hypot(by);
        if denominator == 0.0 || !denominator.is_finite() {
            return 0.0;
        }
        ((ax * bx + ay * by) / denominator)
            .clamp(-1.0, 1.0)
            .acos()
            .to_degrees()
    }
    angle(b, a, c).min(angle(a, b, c)).min(angle(a, c, b))
}

fn triangle_area(a: Point2<f64>, b: Point2<f64>, c: Point2<f64>) -> f64 {
    ((b.x - a.x) * (c.y - a.y) - (b.y - a.y) * (c.x - a.x)).abs() / 2.0
}

fn validate_refinement_request(rings: &[&CoordSeq], refinement: CdtRefinement) -> Result<()> {
    if let CdtRefinement::On {
        min_angle: Some(angle),
        ..
    } = refinement
        && angle.get() > 30.0
    {
        return Err(GeometryErrorKind::triangulation(format!(
            "min_angle must be at most 30 degrees for terminating constrained refinement, got {}",
            angle.get()
        )));
    }
    if let CdtRefinement::On {
        max_area: Some(max_area),
        ..
    } = refinement
    {
        let area = rings
            .iter()
            .map(|ring| ring_area_measure(*ring).get())
            .sum::<f64>();
        let minimum_faces = (area / max_area.get()).ceil() as usize;
        ExpansionBudget::product("triangulate", "max_area", minimum_faces, 4)?;
    }
    Ok(())
}

fn apply_refinement(
    cdt: &mut ConstrainedDelaunayTriangulation<Point2<f64>>,
    refinement: CdtRefinement,
) -> Result<()> {
    if !refinement.active() {
        return Ok(());
    }
    let max_additional_vertices = GENERATED_ITEM_LIMIT.saturating_sub(cdt.num_vertices());
    let result = cdt.refine(refinement_parameters(refinement, max_additional_vertices));
    if result.refinement_complete {
        Ok(())
    } else {
        Err(GeometryErrorKind::triangulation(format!(
            "constrained refinement could not satisfy the requested quality within the limit of {GENERATED_ITEM_LIMIT} generated vertices; relax min_angle/max_area"
        )))
    }
}

fn validate_refined_triangle(
    a: Point2<f64>,
    b: Point2<f64>,
    c: Point2<f64>,
    refinement: CdtRefinement,
) -> Result<()> {
    let CdtRefinement::On {
        min_angle,
        max_area,
    } = refinement
    else {
        return Ok(());
    };
    if let Some(limit) = min_angle
        && triangle_min_angle_degrees(a, b, c) + 1.0e-9 < limit.get()
    {
        return Err(GeometryErrorKind::triangulation(format!(
            "constrained refinement did not satisfy min_angle={} degrees",
            limit.get()
        )));
    }
    if let Some(limit) = max_area
        && triangle_area(a, b, c) > limit.get() * (1.0 + 1.0e-12)
    {
        return Err(GeometryErrorKind::triangulation(format!(
            "constrained refinement did not satisfy max_area={}",
            limit.get()
        )));
    }
    Ok(())
}

pub(crate) fn delaunay_triangulation(points: &[Point]) -> delaunator::Triangulation {
    let sites: Vec<delaunator::Point> = points
        .iter()
        .map(|point| delaunator::Point {
            x: point.x,
            y: point.y,
        })
        .collect();
    delaunator::triangulate(&sites)
}

/// Delaunay triangulation via `spade`, emitted in `delaunator`'s
/// `(triangles, halfedges)` layout so the existing half-edge consumers (the
/// chi-shape peeling) run unchanged. `delaunator`'s sweep-circle `legalize`
/// cascades to ~O(n²·⁹) on near-cocircular inputs (dense points on a smooth
/// curve); `spade`'s bulk loader stays far closer to linear there, so this is
/// the engine `concave_hull` uses. Falls back to `delaunator` if loading is
/// rejected (keeps the point-index ↔ vertex-index mapping exact).
pub(crate) fn delaunay_triangulation_spade(points: &[Point]) -> delaunator::Triangulation {
    let spade_points: Vec<_> = points.iter().map(|point| spade_point(point.xy())).collect();
    let Ok(triangulation) = DelaunayTriangulation::<Point2<f64>>::bulk_load_stable(spade_points)
    else {
        return delaunay_triangulation(points);
    };
    if triangulation.num_vertices() != points.len() {
        return delaunay_triangulation(points);
    }

    // spade inner faces are CCW (gometry's shell canon, matching delaunator).
    let mut triangles: Vec<usize> = Vec::with_capacity(triangulation.num_inner_faces() * 3);
    let mut face_to_dense: HashMap<FixedFaceHandle<InnerTag>, usize> =
        HashMap::with_capacity(triangulation.num_inner_faces());
    for (face_index, face) in triangulation.inner_faces().enumerate() {
        face_to_dense.insert(face.fix(), face_index);
        let [a, b, c] = face.vertices();
        triangles.push(a.index());
        triangles.push(b.index());
        triangles.push(c.index());
    }

    let mut halfedges = Vec::with_capacity(triangles.len());
    for face in triangulation.inner_faces() {
        for edge in face.adjacent_edges() {
            let rev_edge = edge.rev();
            let Some(rev_face) = rev_edge.face().as_inner() else {
                halfedges.push(delaunator::EMPTY);
                continue;
            };
            let rev_face_index = face_to_dense[&rev_face.fix()];
            let rev_edge_handle = rev_edge.fix();
            let rev_local_index = rev_face
                .adjacent_edges()
                .iter()
                .position(|candidate| candidate.fix() == rev_edge_handle)
                .expect("reversed edge must be adjacent to its inner face");
            halfedges.push(rev_face_index * 3 + rev_local_index);
        }
    }
    delaunator::Triangulation {
        triangles,
        halfedges,
        hull: Vec::new(),
    }
}

/// Constrained Delaunay triangulation of an areal geometry (`rings` =
/// every shell and hole as a closed coordinate sequence): the ring edges
/// become CDT constraints, then inner faces whose centroid lies inside the
/// geometry (`contains`) are kept — the triangles strictly within the input.
///
/// This is spade used directly (the engine geo wrapped): collect the
/// constraint segments, snap/dedup near-coincident endpoints, build the CDT,
/// then filter by centroid membership. Valid polygon boundaries are already
/// simple; any crossing constraints fall back to spade's split insertion.
/// Constrained Delaunay triangulation of the polygon interior as a flat
/// `[a, b, c, a]`-per-kept-face vertex stream (XY, since spade's CDT is 2D).
/// `contains` keeps interior faces. The packed array surface builds a
/// `Polygons` layout from this in one pass; the `Vec<Shape>` form wraps it
/// below.
pub(crate) fn constrained_triangle_vertices<'a>(
    rings: impl Iterator<Item = &'a CoordSeq>,
    contains: impl Fn(Point) -> bool,
    refinement: CdtRefinement,
) -> Result<Vec<Point>> {
    let rings: Vec<&CoordSeq> = rings.collect();
    validate_refinement_request(&rings, refinement)?;
    let mut lines: Vec<[XY; 2]> = Vec::new();
    for ring in rings {
        let count = ring.coord_count();
        for index in 0..count.saturating_sub(1) {
            lines.push([ring.nth_coord(index).xy(), ring.nth_coord(index + 1).xy()]);
        }
    }
    let lines = cleanup_constraint_lines(lines);

    let mut vertices = Vec::new();
    let mut vertex_indices: HashMap<PointKey, usize> = HashMap::with_capacity(lines.len() * 2);
    let mut edges = Vec::with_capacity(lines.len());
    for [start, end] in lines {
        let start = constraint_vertex_index(start, &mut vertices, &mut vertex_indices);
        let end = constraint_vertex_index(end, &mut vertices, &mut vertex_indices);
        edges.push([start, end]);
    }

    let mut cdt = bulk_load_constraints(vertices, edges)?;

    apply_refinement(&mut cdt, refinement)?;

    let capacity = cdt
        .num_inner_faces()
        .checked_mul(4)
        .ok_or_else(|| GeometryErrorKind::triangulation("triangle output size overflow"))?;
    if refinement.active() {
        ExpansionBudget::check("triangulate", "min_angle/max_area", capacity)?;
    }
    let mut vertices = Vec::new();
    vertices.try_reserve_exact(capacity).map_err(|_| {
        GeometryErrorKind::triangulation(format!(
            "could not allocate {capacity} constrained-triangulation coordinates"
        ))
    })?;
    for face in cdt.inner_faces() {
        let [a, b, c] = face.positions();
        let centroid = Point::new_unchecked_xy((a.x + b.x + c.x) / 3.0, (a.y + b.y + c.y) / 3.0);
        if contains(centroid) {
            validate_refined_triangle(a, b, c, refinement)?;
            // Spade emits inner faces counter-clockwise — gometry's shell canon —
            // so the closed ring is `[a, b, c, a]` (no reversal).
            let a = Point::new_unchecked_xy(a.x, a.y);
            let b = Point::new_unchecked_xy(b.x, b.y);
            let c = Point::new_unchecked_xy(c.x, c.y);
            vertices.extend_from_slice(&[a, b, c, a]);
        }
    }
    Ok(vertices)
}

/// [`constrained_triangle_vertices`] chunked into one triangle `Polygon` each —
/// the `Vec<Shape>` form (Z/M resolved by the caller's `carry_each`).
pub(crate) fn constrained_triangles<'a>(
    rings: impl Iterator<Item = &'a CoordSeq>,
    contains: impl Fn(Point) -> bool,
    refinement: CdtRefinement,
) -> Result<Vec<Shape>> {
    Ok(constrained_triangle_vertices(rings, contains, refinement)?
        .as_chunks::<4>()
        .0
        .iter()
        .map(|ring| {
            Shape::Polygon(Polygon::new(
                Ring::from_trusted_closed(CoordSeq::from_points(ring)),
                Vec::new(),
            ))
        })
        .collect())
}

pub(crate) const fn spade_point(point: XY) -> Point2<f64> {
    Point2::new(point.x, point.y)
}

pub(crate) fn insertion_error(error: InsertionError) -> Error {
    GeometryErrorKind::triangulation(error.to_string())
}

fn constraint_vertex_index(
    point: XY,
    vertices: &mut Vec<Point2<f64>>,
    vertex_indices: &mut HashMap<PointKey, usize>,
) -> usize {
    let next_index = vertices.len();
    *vertex_indices
        .entry(PointKey::new(point))
        .or_insert_with(|| {
            vertices.push(spade_point(point));
            next_index
        })
}

fn bulk_load_constraints(
    vertices: Vec<Point2<f64>>,
    edges: Vec<[usize; 2]>,
) -> Result<ConstrainedDelaunayTriangulation<Point2<f64>>> {
    let mut constraint_conflict = false;
    let cdt = ConstrainedDelaunayTriangulation::<Point2<f64>>::try_bulk_load_cdt(
        vertices.clone(),
        edges.clone(),
        |_| {
            constraint_conflict = true;
        },
    )
    .map_err(insertion_error)?;
    if !constraint_conflict {
        return Ok(cdt);
    }

    let mut cdt =
        ConstrainedDelaunayTriangulation::<Point2<f64>>::bulk_load_cdt(vertices, Vec::new())
            .map_err(insertion_error)?;
    for [start, end] in edges {
        cdt.add_constraint_and_split(
            FixedVertexHandle::from_index(start),
            FixedVertexHandle::from_index(end),
            |point| point,
        );
    }
    Ok(cdt)
}

/// Prepare constraint segments for spade: snap near-coincident endpoints to a
/// shared vertex, then drop degenerate and duplicate edges. Any remaining
/// constraint crossings are delegated to spade's `add_constraint_and_split`
/// fallback so split vertices match the CDT engine.
pub(crate) struct ConstraintSnapRegistry {
    buckets: HashMap<(i64, i64), Vec<XY>>,
}

impl ConstraintSnapRegistry {
    pub(crate) fn new() -> Self {
        Self {
            buckets: HashMap::new(),
        }
    }
}

pub(crate) fn cleanup_constraint_lines(lines: Vec<[XY; 2]>) -> Vec<[XY; 2]> {
    // Undirected edge key on the snapped endpoints (canonical (min,max) order).
    let edge_key = |a: XY, b: XY| -> (PointKey, PointKey) {
        let (ka, kb) = (PointKey::new(a), PointKey::new(b));
        if ka <= kb { (ka, kb) } else { (kb, ka) }
    };
    let mut known = ConstraintSnapRegistry::new();
    let mut prepared: Vec<[XY; 2]> = Vec::with_capacity(lines.len());
    let mut seen: HashSet<(PointKey, PointKey)> = HashSet::with_capacity(lines.len());
    for [start, end] in lines {
        let start = snap_or_register(start, &mut known);
        let end = snap_or_register(end, &mut known);
        if !same_point(start, end) && seen.insert(edge_key(start, end)) {
            prepared.push([start, end]);
        }
    }
    prepared
}

/// Snap to the nearest registered vertex within the snap radius, else register
/// and return the input — geo's `snap_or_register_point`.
pub(crate) fn snap_or_register(point: XY, known: &mut ConstraintSnapRegistry) -> XY {
    let radius = CONSTRAINT_SNAP_RADIUS;
    let radius_sq = radius * radius;
    let inverse = 1.0 / radius;
    let cell = |value: f64| (value * inverse).floor() as i64;
    let (cx, cy) = (cell(point.x), cell(point.y));
    let mut best: Option<(f64, XY)> = None;
    for dx in -1..=1 {
        for dy in -1..=1 {
            let Some(entries) = known.buckets.get(&(cx + dx, cy + dy)) else {
                continue;
            };
            for &existing in entries {
                let distance_sq = point_distance_squared(existing, point);
                if distance_sq < radius_sq && best.is_none_or(|(closest, _)| distance_sq < closest)
                {
                    best = Some((distance_sq, existing));
                }
            }
        }
    }
    if let Some((_, existing)) = best {
        return existing;
    }
    known.buckets.entry((cx, cy)).or_default().push(point);
    point
}
