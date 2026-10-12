//! Direct dual construction from the certified Delaunay mesh.

use super::{
    CertifiedDelaunay, CoordSeq, DelaunayComplex, DirectedDualEdge, DualClipUse, DualEdgeDraft,
    DualRings, DualVertex, EMPTY, EdgeTag, EmbeddedEdgeData, EmbeddedEdges, EmbeddedPolygon,
    EmbeddedVoronoi, ExactSign, ExpansionBudget, FrameSide, IncidenceOutput, IncidentFaceRow,
    IncidentFaces, NormalizedClip, Ordering, OverlayNodeReceipt, Point, PointKey,
    PointSelectionChart, Polygon, Result, Ring, Site, SiteId, Tagged, VoronoiAssignmentReceipt,
    VoronoiBoundary, VoronoiCellReceipt, VoronoiClipAtomReceipt, VoronoiDartReceipt,
    VoronoiDirectedEdgeUse, VoronoiEdgeOrigin, VoronoiEdgeReceipt, VoronoiEdgeSourceUse,
    VoronoiProducer, VoronoiProduct, VoronoiRingEdgeRole, VoronoiSeparatorReceipt,
    VoronoiVertexOrigin, VoronoiVertexReceipt, XY, axis_aligned_rectangle, between,
    build_product_with_chart, chart_vertex_origins, clip_segment_receipts, delaunay_complex,
    ensure_coordseq_vertex_capacity, exact, exact_bisector_frame_intersections,
    exact_segment_parameter, exact_xy_coordinates, lex_xy, normalize_clip_with_chart,
    polygon_is_valid, polygon_output_count, receipt_frame_side, site_at, site_by_id, site_receipts,
    stored_xy_cycle_matches_orientation, try_hash_map_with_capacity, voronoi_error,
};

#[derive(Debug)]
pub(super) enum DualEmbeddingError {
    Unrepresentable,
    Other(crate::error::Error),
}

impl From<crate::error::Error> for DualEmbeddingError {
    fn from(error: crate::error::Error) -> Self {
        Self::Other(error)
    }
}

impl From<crate::error::ResourceError> for DualEmbeddingError {
    fn from(error: crate::error::ResourceError) -> Self {
        Self::Other(error.into())
    }
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "edge is an admitted ordinal in a three-slot Delaunay face, so its side is 0, 1, or 2 and the wrapped face-local successor stays in range"
)]
const fn dual_next_halfedge(edge: usize) -> usize {
    if edge % 3 == 2 { edge - 2 } else { edge + 1 }
}

fn same_undirected_dual_edge(left: DirectedDualEdge, right: DirectedDualEdge) -> bool {
    left.low == right.low && left.high == right.high
}

fn dual_clip_segment_for_edge(
    clip: &NormalizedClip<'_>,
    start: &exact::ExactPoint,
    end: &exact::ExactPoint,
) -> Result<Option<DualClipUse>> {
    let zero = exact::ExactRational::zero();
    let one = exact::ExactRational::one();
    let mut segment = 0_usize;
    for ring in &clip.rings {
        for (clip_start, clip_end) in ring.iter().zip(ring.iter().cycle().skip(1)) {
            if exact::orient_points(clip_start, clip_end, start)? == ExactSign::Zero
                && exact::orient_points(clip_start, clip_end, end)? == ExactSign::Zero
            {
                let start_parameter = exact_segment_parameter(clip_start, clip_end, start)?;
                let end_parameter = exact_segment_parameter(clip_start, clip_end, end)?;
                let in_range = |parameter: &exact::ExactRational| -> Result<bool> {
                    Ok(parameter.try_cmp(&zero)? != Ordering::Less
                        && parameter.try_cmp(&one)? != Ordering::Greater)
                };
                if in_range(&start_parameter)? && in_range(&end_parameter)? {
                    return Ok(Some(DualClipUse {
                        segment: u32::try_from(segment)
                            .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                        start_parameter,
                        end_parameter,
                    }));
                }
            }
            segment = segment
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
        }
    }
    Ok(None)
}

impl Tagged {
    fn clone_tagged(&self) -> Self {
        Self {
            vertex: match &self.vertex {
                DualVertex::Center(component) => DualVertex::Center(*component),
                DualVertex::Generated(point) => DualVertex::Generated(Box::new((**point).clone())),
            },
            leaving: self.leaving,
        }
    }
}

/// The ring's vertices, without their edge tags.
fn untag(ring: Vec<Tagged>) -> Vec<DualVertex> {
    ring.into_iter().map(|tagged| tagged.vertex).collect()
}

pub(super) fn reverse_tagged(ring: &mut [Tagged]) {
    // Reversal turns each edge around: the edge leaving a vertex afterwards
    // is the one that used to arrive at it.
    ring.reverse();
    if let Some(first) = ring.first().map(|tagged| tagged.leaving) {
        let mut next = first;
        for tagged in ring.iter_mut().rev() {
            std::mem::swap(&mut tagged.leaving, &mut next);
        }
    }
}

fn dedup_tagged(ring: &mut Vec<Tagged>, centers: &[exact::ExactPoint]) -> Result<()> {
    let same = |left: &Tagged, right: &Tagged| {
        dual_vertex_point(&left.vertex, centers)
            .same_position(dual_vertex_point(&right.vertex, centers))
    };
    let mut failure = None;
    ring.dedup_by(|later, earlier| {
        let same = failure.is_none()
            && same(later, earlier).unwrap_or_else(|error| {
                failure = Some(error);
                false
            });
        if same {
            earlier.leaving = later.leaving;
        }
        same
    });
    if let Some(error) = failure {
        return Err(error);
    }
    while let (Some(first), Some(last)) = (ring.first(), ring.last()) {
        if ring.len() <= 1 || !same(first, last)? {
            break;
        }
        ring.pop();
    }
    Ok(())
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "half-edge and site ordinals come from the same certified mesh and admitted site slice; the count pass bounds each row and its total slots"
)]
fn dual_incident_faces(mesh: &CertifiedDelaunay, site_count: usize) -> IncidentFaces {
    let triangles = mesh.triangles();
    let halfedges = mesh.halfedges();
    let flat = |edge: usize| dual_site_at_halfedge(triangles, edge);
    let mut start_edge = Vec::with_capacity(site_count);
    start_edge.resize(site_count, usize::MAX);
    for edge in 0..halfedges.len() {
        let endpoint = flat(dual_next_halfedge(edge));
        if start_edge[endpoint] == usize::MAX || halfedges[edge] == EMPTY {
            start_edge[endpoint] = edge;
        }
    }
    let mut offsets: Vec<usize> = Vec::new();
    let mut boundaries = Vec::new();
    offsets.reserve(site_count + 1);
    boundaries.reserve(site_count);
    offsets.push(0);
    for site in 0..site_count {
        let start = start_edge[site];
        let (length, boundary) = incident_faces_for_site(start, halfedges, None);
        offsets.push(offsets.last().copied().unwrap_or(0) + length);
        boundaries.push(boundary);
    }
    let total = offsets.last().copied().unwrap_or(0);
    let mut faces = Vec::new();
    let mut neighbors = Vec::new();
    faces.reserve(total);
    neighbors.reserve(total);
    faces.resize(total, 0);
    neighbors.resize(total, SiteId(0));
    for (&start, &[row_start, row_end]) in start_edge.iter().zip(offsets.array_windows::<2>()) {
        incident_faces_for_site(
            start,
            halfedges,
            Some(IncidenceOutput {
                faces: &mut faces[row_start..row_end],
                neighbors: &mut neighbors[row_start..row_end],
                flat: &flat,
            }),
        );
    }
    IncidentFaces {
        offsets,
        faces,
        neighbors,
        boundaries,
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "each certified Delaunay face owns exactly three consecutive half-edge origins"
)]
fn dual_site_at_halfedge(triangles: &[[usize; 3]], halfedge: usize) -> usize {
    triangles[halfedge / 3][halfedge % 3]
}

impl IncidentFaces {
    #[expect(
        clippy::indexing_slicing,
        reason = "offsets, face rows, neighbor rows, and boundary records are built in the same dense site order"
    )]
    fn rows(&self) -> impl Iterator<Item = IncidentFaceRow<'_>> + '_ {
        self.offsets
            .array_windows::<2>()
            .zip(self.boundaries.iter())
            .map(move |(&[start, end], &boundary)| IncidentFaceRow {
                faces: &self.faces[start..end],
                neighbors: &self.neighbors[start..end],
                boundary,
            })
    }
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "the certified half-edge walk's count pass reserves this site's face/neighbor row and every next-halfedge ordinal belongs to its mesh"
)]
fn incident_faces_for_site(
    start: usize,
    halfedges: &[usize],
    mut output: Option<IncidenceOutput<'_>>,
) -> (usize, Option<(usize, usize)>) {
    if start == usize::MAX {
        return (0, None);
    }
    let mut length = 0;
    let mut incoming = start;
    loop {
        let outgoing = dual_next_halfedge(incoming);
        if let Some(output) = output.as_mut() {
            output.faces[length] = incoming / 3;
            // `outgoing` runs from the site to the neighbour shared with the
            // next face around it.
            output.neighbors[length] = SiteId((output.flat)(dual_next_halfedge(outgoing)) as u32);
        }
        length += 1;
        let next = halfedges[outgoing];
        if next == EMPTY {
            // `start` was a hull half-edge (q -> site) with no twin and
            // `outgoing` is the hull half-edge (site -> r).
            return (length, Some((start, outgoing)));
        }
        incoming = next;
        if incoming == start {
            return (length, None);
        }
    }
}

/// Cap one unbounded dual ray on the private frame. Identical construction to
/// the shipped hull-edge lane in `emit_clipped_separators`.
fn dual_ray_hit(
    sites: &[Site],
    pair: [usize; 2],
    opposite: usize,
    frame: &[exact::ExactPoint; 4],
) -> Result<(FrameSide, exact::ExactPoint)> {
    let [left_site, right_site] = pair;
    let [left, right] = [
        &site_at(sites, left_site).exact_point,
        &site_at(sites, right_site).exact_point,
    ];
    let opposite_sign = exact::orient_points(left, right, &site_at(sites, opposite).exact_point)?;
    let line = exact::ExactLine::perpendicular_bisector_points(left, right)?;
    for hit in exact_bisector_frame_intersections([left, right], &line, frame)? {
        if exact::orient_points(left, right, &hit.1)? != opposite_sign {
            return Ok(hit);
        }
    }
    Err(voronoi_error(
        "certified Voronoi hull ray misses exact frame",
    ))
}

fn dual_close_hull_chain(
    faces: &[usize],
    neighbors: &[SiteId],
    entry_site: SiteId,
    complex: &DelaunayComplex,
    center_points: &[exact::ExactPoint],
    entry_ray: &(FrameSide, exact::ExactPoint),
    exit_ray: &(FrameSide, exact::ExactPoint),
    frame: &[exact::ExactPoint; 4],
    exterior: bool,
) -> Result<Vec<Tagged>> {
    let generated = |point: &exact::ExactPoint, leaving| -> Result<Tagged> {
        Ok(Tagged {
            vertex: DualVertex::Generated(Box::new(point.clone())),
            leaving,
        })
    };
    let entry_side = entry_ray.0;
    let exit_side = exit_ray.0;
    let mut ring = Vec::with_capacity(faces.len().saturating_add(6));
    ring.push(generated(&entry_ray.1, EdgeTag::Bisector(entry_site))?);
    ring.extend(
        faces
            .iter()
            .zip(neighbors)
            .map(|(&face, &neighbor)| Tagged {
                vertex: DualVertex::Center(complex.component_of_face(face)),
                leaving: EdgeTag::Bisector(neighbor),
            }),
    );
    ring.push(generated(&exit_ray.1, EdgeTag::Frame(exit_side))?);
    if exterior {
        let steps = match exit_side.steps_clockwise_to(entry_side) {
            0 => FrameSide::ALL.len(),
            steps => steps,
        };
        let mut side = exit_side;
        for _ in 0..steps {
            ring.push(generated(
                side.start(frame),
                EdgeTag::Frame(side.previous()),
            )?);
            side = side.previous();
        }
    } else {
        let steps = exit_side.steps_counterclockwise_to(entry_side);
        let mut side = exit_side;
        for _ in 0..steps {
            side = side.next();
            ring.push(generated(side.start(frame), EdgeTag::Frame(side))?);
        }
    }
    dedup_tagged(&mut ring, center_points)?;
    if dual_cycle_orientation(&ring, center_points)? == ExactSign::Negative {
        reverse_tagged(&mut ring);
    }
    Ok(ring)
}

#[expect(
    clippy::indexing_slicing,
    reason = "center component IDs come from the same DelaunayComplex whose exact centers are passed to every dual traversal"
)]
pub(super) fn dual_vertex_point<'a>(
    vertex: &'a DualVertex,
    centers: &'a [exact::ExactPoint],
) -> &'a exact::ExactPoint {
    match vertex {
        DualVertex::Center(component) => &centers[*component],
        DualVertex::Generated(point) => point,
    }
}

fn dual_cycle_orientation(ring: &[Tagged], centers: &[exact::ExactPoint]) -> Result<ExactSign> {
    if ring.len() < 3 {
        return Err(voronoi_error("exact cycle has fewer than three vertices"));
    }
    let mut points = ring
        .iter()
        .chain(ring.iter().take(2))
        .map(|tagged| dual_vertex_point(&tagged.vertex, centers));
    let mut start = points
        .next()
        .expect("an exact cycle has at least three vertices");
    let mut middle = points
        .next()
        .expect("an exact cycle has at least three vertices");
    for end in points {
        let sign = exact::orient_points(start, middle, end)?;
        if sign != ExactSign::Zero {
            return Ok(sign);
        }
        start = middle;
        middle = end;
    }
    Err(voronoi_error("exact cycle has zero orientation"))
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "an admitted simple Voronoi cell has exact winding 0, 1, or -1 for this point-in-cycle query"
)]
fn dual_point_in_cycle(
    cycle: &[Tagged],
    centers: &[exact::ExactPoint],
    point: &exact::ExactPoint,
) -> Result<exact::PointInCycle> {
    let mut winding = 0_i32;
    for (start, end) in cycle.iter().zip(cycle.iter().cycle().skip(1)) {
        let start = dual_vertex_point(&start.vertex, centers);
        let end = dual_vertex_point(&end.vertex, centers);
        let orientation = exact::orient_points(start, end, point)?;
        if orientation == ExactSign::Zero && between(point, start, end)? {
            return Ok(exact::PointInCycle::Boundary);
        }
        let start_y = start.compare_y(point)?;
        let end_y = end.compare_y(point)?;
        if start_y != Ordering::Greater
            && end_y == Ordering::Greater
            && orientation == ExactSign::Positive
        {
            winding += 1;
        } else if end_y != Ordering::Greater
            && start_y == Ordering::Greater
            && orientation == ExactSign::Negative
        {
            winding -= 1;
        }
    }
    Ok(if winding == 0 {
        exact::PointInCycle::Outside
    } else {
        exact::PointInCycle::Inside
    })
}

/// Sutherland-Hodgman against a CCW convex rectangle, in exact arithmetic.
/// "Inside" is `orient_points(side_start, side_end, p) != Negative`, i.e. the
/// same exact predicate the rest of the module uses; a crossing vertex is the
/// exact intersection of two support lines, never a rounded one.
/// Sutherland-Hodgman against the clip rectangle, in exact arithmetic.
///
/// The clip is axis-aligned, so the half-plane test collapses from a general
/// orientation determinant to ONE coordinate comparison. For the bottom side
/// `a = (minx, miny) -> b = (maxx, miny)` the determinant is
/// `(maxx - minx) * (py - miny)`, and `maxx > minx` is checked once by the
/// caller, so its sign IS the sign of `py - miny`. The same reduction holds on
/// the other three sides. Identical verdict, one filtered comparison instead of
/// six interval operations. A degenerate rectangle falls back to the general
/// orientation test.
fn dual_clip_convex(
    ring: Vec<Tagged>,
    site: &Site,
    sites: &[Site],
    rect: &[exact::ExactPoint; 4],
    axis_aligned: bool,
    centers: &[exact::ExactPoint],
) -> Result<Vec<Tagged>> {
    // Certified crossings need the clip sides as doubles.
    let corners = if sites.iter().all(|site| site.retained_chart_site.is_none()) {
        match rect.each_ref().map(exact::ExactPoint::as_double) {
            [Some(a), Some(b), Some(c), Some(d)] if axis_aligned => Some([a, b, c, d]),
            _ => None,
        }
    } else {
        None
    };
    let mut current = ring;
    for side in FrameSide::ALL {
        if current.is_empty() {
            return Ok(current);
        }
        let (a, b) = (side.start(rect), side.end(rect));
        let inside = |point: &exact::ExactPoint| -> Result<bool> {
            if !axis_aligned {
                return Ok(exact::orient_points(a, b, point)? != ExactSign::Negative);
            }
            Ok(match side {
                FrameSide::Bottom => point.compare_y(a)? != Ordering::Less,
                FrameSide::Right => point.compare_x(a)? != Ordering::Greater,
                FrameSide::Top => point.compare_y(a)? != Ordering::Greater,
                FrameSide::Left => point.compare_x(a)? != Ordering::Less,
            })
        };
        // The crossing of the edge `from -> to` with this side. A bisector
        // edge meets the side at a certified rounding; an axis-aligned edge
        // meets it at an exact double; anything the certificate declines is
        // the exact intersection of the two support lines, as before.
        let crossing = |from: &exact::ExactPoint,
                        tag: EdgeTag,
                        to: &exact::ExactPoint|
         -> Result<Option<exact::ExactPoint>> {
            if let Some(corners) = corners {
                let [corner_start, corner_end] = side.endpoints_xy(&corners);
                match tag {
                    EdgeTag::Bisector(neighbor) => {
                        let neighbor = site_by_id(sites, neighbor);
                        if let (Some(site), Some(neighbor)) = (
                            site.exact_point.as_double(),
                            neighbor.exact_point.as_double(),
                        ) && let exact::FrameHit::Point(hit) =
                            exact::ExactPoint::certified_bisector_hit([site, neighbor], [
                                corner_start,
                                corner_end,
                            ])
                        {
                            return Ok(Some(hit));
                        }
                    },
                    EdgeTag::Frame(_) => {
                        // An edge along a frame or clip side shares one exact
                        // coordinate: a ray hit's side coordinate is exact in
                        // its bounds, a corner is a double. The crossing with
                        // a perpendicular side is then an exact double.
                        let [from_x, from_y] = from.bounds();
                        let [to_x, to_y] = to.bounds();
                        let shared = |from: exact::Interval, to: exact::Interval| {
                            (from.lo == from.hi && to.lo == to.hi && from.lo == to.lo)
                                .then_some(from.lo)
                        };
                        let hit = match side {
                            FrameSide::Bottom | FrameSide::Top => {
                                shared(from_x, to_x).map(|x| XY::new(x, corner_start.y))
                            },
                            FrameSide::Right | FrameSide::Left => {
                                shared(from_y, to_y).map(|y| XY::new(corner_start.x, y))
                            },
                        };
                        if let Some(hit) = hit
                            && hit.x.is_finite()
                            && hit.y.is_finite()
                        {
                            return Ok(Some(exact::ExactPoint::from_xy(hit)));
                        }
                    },
                }
            }
            let boundary = exact::ExactLine::through_points(a, b)?;
            let edge = exact::ExactLine::through_points(from, to)?;
            let hit = exact::line_intersection(&boundary, &edge)?;
            Ok(hit.is_finite().then_some(hit))
        };
        let mut next = Vec::with_capacity(current.len().saturating_add(1));
        let Some(mut previous) = current.last() else {
            return Ok(current);
        };
        let mut previous_in = inside(dual_vertex_point(&previous.vertex, centers))?;
        for this in &current {
            let this_point = dual_vertex_point(&this.vertex, centers);
            let this_in = inside(this_point)?;
            if this_in != previous_in {
                let tag = previous.leaving;
                let previous_point = dual_vertex_point(&previous.vertex, centers);
                if let Some(hit) = crossing(previous_point, tag, this_point)? {
                    next.push(Tagged {
                        vertex: DualVertex::Generated(Box::new(hit)),
                        // Entering: the rest of the original edge follows.
                        // Leaving: the new edge runs along this side.
                        leaving: if this_in { tag } else { EdgeTag::Frame(side) },
                    });
                }
            }
            if this_in {
                next.push(this.clone_tagged());
            }
            previous = this;
            previous_in = this_in;
        }
        dedup_tagged(&mut next, centers)?;
        current = next;
    }
    Ok(current)
}

pub(super) fn dual_build_rings(
    sites: &[Site],
    mesh: &CertifiedDelaunay,
    complex: DelaunayComplex,
    clip: &NormalizedClip<'_>,
) -> Result<DualRings> {
    let centers = &complex.centers;
    let mut site_points = Vec::with_capacity(sites.len());
    site_points.extend(sites.iter().map(|site| site.exact_point.clone()));
    let frame_inputs = || {
        site_points
            .iter()
            .chain(clip.rings.iter().flatten())
            .chain(centers.iter())
    };
    let frame = if let Some(frame) = exact::enclosing_frame_binary64(frame_inputs())? {
        frame
    } else {
        let mut inputs = Vec::with_capacity(frame_inputs().count());
        inputs.extend(frame_inputs().cloned());
        exact::enclosing_frame(&inputs)?
    };
    let [clip_outer] = clip.rings.as_slice() else {
        return Err(voronoi_error(
            "certified rectangular clip requires one outer ring",
        ));
    };
    let [a, b, c, d] = clip_outer.as_slice() else {
        return Err(voronoi_error(
            "certified rectangular clip requires four corners",
        ));
    };
    let rect = [a.clone(), b.clone(), c.clone(), d.clone()];
    // Certified once: the clip really is a positively-oriented axis-aligned
    // rectangle with strictly positive extent. Only then may the half-plane
    // test collapse to a coordinate comparison.
    let axis_aligned = axis_aligned_rectangle(&rect)?;

    // A box certified to lie inside the rectangle, for the interior fast
    // lane: the inner edges of the corners' certified intervals. A padded
    // corner is a dyadic that need not be a double, and this stays exact for
    // it; an unbounded interval fails every strict comparison and keeps the
    // cell on the exact lane.
    let interior_bounds = axis_aligned.then(|| {
        let [bottom_left, _, top_right, _] = &rect;
        let [low_x, low_y] = bottom_left.bounds();
        let [high_x, high_y] = top_right.bounds();
        [low_x.hi, high_x.lo, low_y.hi, high_y.lo]
    });
    // The incidence walk has one rotational sense for the whole mesh; the
    // first interior cell settles it exactly and every later one reuses it.
    let mut walk_clockwise: Option<bool> = None;

    let triangles = mesh.triangles();
    let flat = |edge: usize| dual_site_at_halfedge(triangles, edge);
    let incidence = dual_incident_faces(mesh, sites.len());
    let mut rings = Vec::with_capacity(sites.len());
    let mut owners = Vec::with_capacity(sites.len());
    let mut edge_tags = Vec::with_capacity(sites.len());

    for (site, incidence_row) in sites.iter().zip(incidence.rows()) {
        let faces = incidence_row.faces;
        let boundary = incidence_row.boundary;
        let neighbors = incidence_row.neighbors;
        let ring = if let Some((first_hull, last_hull)) = boundary {
            // (q -> site) and (site -> r): the two hull edges bounding the cell.
            let entry_site = flat(first_hull);
            let entry_neighbor = flat(dual_next_halfedge(first_hull));
            let entry_opposite = flat(dual_next_halfedge(dual_next_halfedge(first_hull)));
            let exit_site = flat(last_hull);
            let exit_neighbor = flat(dual_next_halfedge(last_hull));
            let exit_opposite = flat(dual_next_halfedge(dual_next_halfedge(last_hull)));
            let entry_pair = [entry_site, entry_neighbor];
            let exit_pair = [exit_site, exit_neighbor];
            let entry_ray = dual_ray_hit(sites, entry_pair, entry_opposite, &frame)?;
            let exit_ray = dual_ray_hit(sites, exit_pair, exit_opposite, &frame)?;
            let site_id = SiteId(entry_site as u32);
            let site_point = &site.exact_point;
            // Positive topology fixes the open center chain. The two frame
            // paths are complementary clipped faces; exact ownership decides
            // only which one contains this site, including the zero-separator
            // isolated-ring case. Ring handedness is normalization, not a
            // second candidate.
            let interior = dual_close_hull_chain(
                faces, neighbors, site_id, &complex, centers, &entry_ray, &exit_ray, &frame, false,
            )?;
            if dual_point_in_cycle(&interior, centers, site_point)? == exact::PointInCycle::Inside {
                interior
            } else {
                let exterior = dual_close_hull_chain(
                    faces, neighbors, site_id, &complex, centers, &entry_ray, &exit_ray, &frame,
                    true,
                )?;
                if dual_point_in_cycle(&exterior, centers, site_point)?
                    != exact::PointInCycle::Inside
                {
                    return Err(voronoi_error(
                        "dual hull cell has no admissible frame closure",
                    ));
                }
                exterior
            }
        } else {
            // Interior fast lane. Two distinct components around one site have
            // distinct exact centers (both circles pass through the site, so a
            // shared center would be one circle, hence one component), which
            // makes consecutive-id dedup exact. The certified mesh orders the
            // faces counter-clockwise, and every center strictly inside the
            // rectangle means the clip is the identity; the embedding's
            // certificate still proves the rounded cell.
            let mut ring: Vec<Tagged> = Vec::with_capacity(faces.len().saturating_add(6));
            let mut interior = interior_bounds.is_some();
            for (&face, &neighbor) in faces.iter().zip(neighbors) {
                let component = complex.component_of_face(face);
                match ring.last_mut() {
                    // Cocircular faces share the vertex; the edge leaving it
                    // is the last face's.
                    Some(last) if matches!(last.vertex, DualVertex::Center(id) if id == component) =>
                    {
                        last.leaving = EdgeTag::Bisector(neighbor);
                    },
                    _ => ring.push(Tagged {
                        vertex: DualVertex::Center(component),
                        leaving: EdgeTag::Bisector(neighbor),
                    }),
                }
                interior = interior
                    && interior_bounds
                        .is_some_and(|bounds| complex.center(component).strictly_inside(bounds));
            }
            if ring.len() > 1 {
                let first = ring.first().map(|tagged| &tagged.vertex);
                let last = ring.last().map(|tagged| &tagged.vertex);
                if let (Some(DualVertex::Center(first)), Some(DualVertex::Center(last))) =
                    (first, last)
                    && first == last
                {
                    let leaving = ring
                        .last()
                        .expect("a repeated closing center has a final edge")
                        .leaving;
                    ring.pop();
                    ring.first_mut()
                        .expect("a repeated closing center has a first edge")
                        .leaving = leaving;
                }
            }
            if interior && ring.len() >= 3 {
                let clockwise = if let Some(clockwise) = walk_clockwise {
                    clockwise
                } else {
                    let clockwise = dual_cycle_orientation(&ring, centers)? == ExactSign::Negative;
                    walk_clockwise = Some(clockwise);
                    clockwise
                };
                if clockwise {
                    reverse_tagged(&mut ring);
                }
                edge_tags.push(ring.iter().map(|tagged| tagged.leaving).collect());
                rings.push(untag(ring));
                owners.push(SiteId(
                    u32::try_from(site.id)
                        .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                ));
                continue;
            }
            dedup_tagged(&mut ring, centers)?;
            if ring.len() >= 3 && dual_cycle_orientation(&ring, centers)? == ExactSign::Negative {
                reverse_tagged(&mut ring);
            }
            ring
        };
        let clipped = dual_clip_convex(ring, site, sites, &rect, axis_aligned, centers)?;
        if clipped.len() < 3 {
            return Err(voronoi_error(
                "dual Voronoi cell degenerated under the clip",
            ));
        }
        edge_tags.push(clipped.iter().map(|tagged| tagged.leaving).collect());
        rings.push(untag(clipped));
        owners.push(SiteId(
            u32::try_from(site.id).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        ));
    }
    Ok(DualRings {
        rings,
        centers: complex.centers,
        center_sites: complex.center_sites,
        owners,
        edge_tags,
    })
}

/// Accept a rounded cell on its doubles alone: a positively oriented, convex,
/// simple ring that covers `site`. Every turn is a left turn or a forward
/// collinear step (a clipped hull cell keeps the points where its rays meet a
/// rectangle side, so consecutive vertices on one side are collinear), never
/// a right turn, a repeated vertex, or a reversal; and the lexicographic edge
/// direction changes exactly twice around the ring, which pins the turning
/// number to one: a pentagram turns left at every vertex and changes direction
/// four times. `site` may lie on an edge. Each sign comes from the exact
/// predicate, so a declined float filter costs one dyadic evaluation, never a
/// whole exact cell, and a refusal here only routes the cell to the exact
/// certificates.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "direction_changes is bounded by the number of vertices in this admitted ring"
)]
pub(super) fn certify_convex_cell(ring: &[XY], site: XY) -> bool {
    let count = ring.len();
    if count < 3 {
        return false;
    }
    let mut direction_changes = 0_usize;
    let mut cyclic = ring.iter().cycle();
    let mut a = *cyclic.next().expect("ring has at least three vertices");
    let mut b = *cyclic.next().expect("ring has at least three vertices");
    for &c in cyclic.take(count) {
        let (ahead, onward) = (lex_xy(a, b), lex_xy(b, c));
        // Three vertices on one axis-aligned side (a clipped hull cell's
        // frame or clip edge) are exactly collinear; no filter can certify
        // a zero, so decide it on the shared coordinate.
        let turn = if (a.x == b.x && b.x == c.x) || (a.y == b.y && b.y == c.y) {
            ExactSign::Zero
        } else {
            exact::orient2d(a, b, c)
        };
        match turn {
            ExactSign::Positive => {},
            ExactSign::Zero if ahead == onward && ahead != Ordering::Equal => {},
            ExactSign::Zero | ExactSign::Negative => return false,
        }
        if ahead != onward {
            direction_changes += 1;
        }
        if exact::orient2d(a, b, site) == ExactSign::Negative {
            return false;
        }
        (a, b) = (b, c);
    }
    direction_changes == 2
}

/// Round the dual once and run the certificates that survive the redesign.
///
/// What is UNCHANGED from `embed`: the sole nearest-even rounding, the global
/// injectivity check over distinct exact vertices, the per-ring orientation
/// check against the rounded ring, and `polygon_is_valid` per cell.
///
/// What is GONE, because the structure it certified no longer exists: the
/// arrangement's rounded no-crossing scan and the per-vertex exact-vs-rounded
/// fan-order comparison. The dual never builds that arrangement. Instead,
/// rounded edge cancellation proves that the valid cells have exactly the
/// rectangular clip boundary, and exact point membership checks each canonical
/// site's rounded cell. Polygon clips remain on the exact arrangement lane.
pub(super) fn dual_embed(
    rings: &DualRings,
    clip: &NormalizedClip<'_>,
    sites: &[Site],
    product: VoronoiProduct,
    budget: &mut ExpansionBudget,
    chart: Option<&PointSelectionChart>,
) -> std::result::Result<EmbeddedVoronoi, DualEmbeddingError> {
    if rings.rings.len() != sites.len() {
        return Err(DualEmbeddingError::Unrepresentable);
    }
    // Vertex identity WITHOUT an exact positional sort.
    //
    // A `Center` vertex IS its component, so its rounding is memoised once per
    // component. Everything else is hashed on its ROUNDED key, and a key
    // collision is then resolved EXACTLY: two exact points that share a rounded
    // key are the same vertex only if `same_position` says so, and otherwise the
    // rounding is not injective and the diagram is refused. That is the same
    // verdict the sort produced -- rounding is a function, so equal exact points
    // always land in the same bucket -- reached in O(V) instead of O(V log V)
    // exact comparisons.
    // Every distinct exact vertex gets one dense id. Center component IDs
    // remain separate producer identities and map to these interned vertices;
    // a clip hit may be the first occurrence of a center's exact position.
    if let Some(last) = rings.centers.len().checked_sub(1) {
        u32::try_from(last).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
    }
    let total = rings.rings.iter().try_fold(0_usize, |count, ring| {
        count
            .checked_add(ring.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)
    })?;
    let mut center_rounded: Vec<Option<(XY, u32)>> = Vec::with_capacity(rings.centers.len());
    center_rounded.resize(rings.centers.len(), None);
    let mut exact_by_id = Vec::new();
    // Distinct vertices are the centers plus a boundary's worth of generated
    // points; sizing for every ring reference would triple the table.
    let mut interned = try_hash_map_with_capacity(rings.centers.len())?;
    let mut vertex_xy: Vec<XY> = Vec::with_capacity(rings.centers.len());
    #[expect(
        clippy::collapsible_if,
        reason = "keep the center-only memo lookup on its measured base instruction path"
    )]
    #[expect(
        clippy::indexing_slicing,
        reason = "center components index the admitted center map; dense IDs occupy its prefix or are assigned immediately before their coordinate is published, and the intern table only returns those IDs"
    )]
    fn resolve<'r>(
        vertex: &'r DualVertex,
        centers: &'r [exact::ExactPoint],
        center_rounded: &mut [Option<(XY, u32)>],
        exact_by_id: &mut Vec<exact::ExactPoint>,
        interned: &mut crate::collections::HashMap<PointKey, (&'r exact::ExactPoint, u32)>,
        vertex_xy: &mut Vec<XY>,
    ) -> std::result::Result<(XY, u32), DualEmbeddingError> {
        if let DualVertex::Center(component) = *vertex {
            if let Some(memo) = center_rounded[component] {
                return Ok(memo);
            }
        }
        let point = dual_vertex_point(vertex, centers);
        let mut xy = point.round_nearest_even()?;
        let key = PointKey::new(xy);
        let id = match interned.entry(key) {
            std::collections::hash_map::Entry::Occupied(existing) => {
                let &(representative, id) = existing.get();
                if !representative.same_position(point)? {
                    return Err(DualEmbeddingError::Unrepresentable);
                }
                // Equal exact generated vertices can reach signed zero through
                // different algebraic expressions. Reuse the first admitted
                // representative so shared binary64 vertices are bit-identical.
                xy = representative.round_nearest_even()?;
                id
            },
            std::collections::hash_map::Entry::Vacant(slot) => {
                let id = u32::try_from(vertex_xy.len())
                    .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
                vertex_xy.push(xy);
                exact_by_id.push(point.clone());
                slot.insert((point, id));
                id
            },
        };
        if let DualVertex::Center(component) = *vertex {
            center_rounded[component] = Some((xy, id));
        }
        Ok((xy, id))
    }
    let mut offsets = Vec::new();
    let mut rounded_all: Vec<XY> = Vec::new();
    let mut ids_all: Vec<u32> = Vec::new();
    let mut edge_tags = rings.edge_tags.clone();
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "the offset column has one terminal sentinel in addition to one row per existing non-zero-sized ring"
    )]
    offsets.reserve(rings.rings.len() + 1);
    rounded_all.reserve(total);
    ids_all.reserve(total);
    offsets.push(0);
    for ring in &rings.rings {
        for vertex in ring {
            let (xy, id) = resolve(
                vertex,
                &rings.centers,
                &mut center_rounded,
                &mut exact_by_id,
                &mut interned,
                &mut vertex_xy,
            )?;
            rounded_all.push(xy);
            ids_all.push(id);
        }
        offsets.push(rounded_all.len());
    }
    for (ring_index, (window, site)) in offsets.array_windows::<2>().zip(sites).enumerate() {
        let [row_start, row_end] = *window;
        let rounded = rounded_all
            .get_mut(row_start..row_end)
            .ok_or(DualEmbeddingError::Unrepresentable)?;
        // The ring starts at its lexicographically smallest rounded vertex,
        // the canonical form both lanes publish; rounding is injective here,
        // so the minimum is unique.
        let rotation = rounded
            .iter()
            .enumerate()
            .min_by(|(_, left), (_, right)| lex_xy(**left, **right))
            .map_or(0, |(index, _)| index);
        rounded.rotate_left(rotation);
        ids_all
            .get_mut(row_start..row_end)
            .ok_or(DualEmbeddingError::Unrepresentable)?
            .rotate_left(rotation);
        let tags = edge_tags
            .get_mut(ring_index)
            .ok_or(DualEmbeddingError::Unrepresentable)?;
        if tags.len() != rounded.len() {
            return Err(DualEmbeddingError::Unrepresentable);
        }
        tags.rotate_left(rotation);
        let site_xy = site.rounded_xy()?;
        let first = rounded
            .first()
            .expect("dual_build_rings admits only cells with at least three vertices");
        let closed = rounded.iter().chain(std::iter::once(first));
        if !certify_convex_cell(rounded, site_xy) {
            if !stored_xy_cycle_matches_orientation(rounded, ExactSign::Positive) {
                return Err(DualEmbeddingError::Unrepresentable);
            }
            let mut rounded_ring = Vec::with_capacity(rounded.len());
            rounded_ring.extend(rounded.iter().map(|&xy| exact::ExactPoint::from_xy(xy)));
            let capacity = rounded
                .len()
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            ensure_coordseq_vertex_capacity(capacity)?;
            let mut points = Vec::with_capacity(capacity);
            points.extend(closed.clone().map(|xy| Point::new_raw_xy(xy.x, xy.y)));
            if !polygon_is_valid(&Polygon::new(Ring::closed(points)?, Vec::new()))? {
                return Err(DualEmbeddingError::Unrepresentable);
            }
            if exact::point_in_cycle(&rounded_ring, &site.exact_point)?
                == exact::PointInCycle::Outside
            {
                return Err(DualEmbeddingError::Unrepresentable);
            }
        }
    }
    // Completeness by EDGE CANCELLATION rather than by a full overlay union.
    //
    // Every rounded cell edge is entered directed. An edge interior to the
    // partition is walked once in each direction by its two owners and cancels;
    // an edge on the outer boundary is walked exactly once. So: if every
    // undirected edge occurs either twice with opposite directions or once, and
    // the once-occurring edges tile the clip rectangle's four sides exactly with
    // no gap and no overlap, then the union of the cells IS the clip rectangle.
    // Combined with positive simple-cell validation and exact site ownership,
    // this is the same partition statement the union/difference pair made,
    // computed on stored binary64 with no overlay. Winding-number membership
    // alone cannot prove this: a gap and an overlap can leave every tested site
    // inside its own cell, while cancellation and directed boundary tiling
    // reject both.
    // Sort by the undirected endpoint pair, then direction, so the two owners
    // of each interior edge become an adjacent pair.
    let mut directed: Vec<DirectedDualEdge> = Vec::with_capacity(total);
    let mut by_dart: Vec<Option<DirectedDualEdge>> = Vec::with_capacity(total);
    by_dart.resize(total, None);
    let mut clip_uses_by_dart = Vec::with_capacity(total);
    let mut next_dart = 0_u32;
    for (ring_index, window) in offsets.array_windows::<2>().enumerate() {
        let [start, end] = *window;
        #[expect(
            clippy::indexing_slicing,
            reason = "each row offset was pushed around appending this same ring's dense vertex IDs"
        )]
        let ids = &ids_all[start..end];
        let tags = edge_tags
            .get(ring_index)
            .ok_or(DualEmbeddingError::Unrepresentable)?;
        let owner = rings
            .owners
            .get(ring_index)
            .ok_or(DualEmbeddingError::Unrepresentable)?
            .0;
        if ids.len() != tags.len() {
            return Err(DualEmbeddingError::Unrepresentable);
        }
        if ids.is_empty() {
            return Err(DualEmbeddingError::Unrepresentable);
        }
        for (edge_index, (&from, &to)) in ids.iter().zip(ids.iter().cycle().skip(1)).enumerate() {
            let from_xy = *vertex_xy
                .get(from as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            let to_xy = *vertex_xy
                .get(to as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            let (endpoints, forward) = if lex_xy(from_xy, to_xy).is_lt() {
                ([from, to], true)
            } else {
                ([to, from], false)
            };
            let (low, high) = if from <= to { (from, to) } else { (to, from) };
            let exact_from = exact_by_id
                .get(from as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            let exact_to = exact_by_id
                .get(to as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            let clip_use = dual_clip_segment_for_edge(clip, exact_from, exact_to)?;
            let clip_segment = clip_use.as_ref().map(|use_| use_.segment);
            let tag = *tags
                .get(edge_index)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            let origin = match tag {
                EdgeTag::Bisector(neighbor) => {
                    let mut pair = [owner, neighbor.0];
                    pair.sort_unstable();
                    VoronoiEdgeOrigin::Bisector { sites: pair }
                },
                EdgeTag::Frame(_) if clip.source.is_some() => {
                    let segment = clip_segment.ok_or(DualEmbeddingError::Unrepresentable)?;
                    VoronoiEdgeOrigin::SourceClip { segment }
                },
                EdgeTag::Frame(side) => VoronoiEdgeOrigin::Frame {
                    side: receipt_frame_side(side),
                },
            };
            let edge = DirectedDualEdge {
                low,
                high,
                endpoints,
                from,
                to,
                forward,
                owner,
                origin,
                clip_segment,
                dart: next_dart,
            };
            next_dart = next_dart
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            *by_dart
                .get_mut(edge.dart as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)? = Some(edge);
            clip_uses_by_dart.push(clip_use);
            directed.push(edge);
        }
    }
    if next_dart as usize != total {
        return Err(DualEmbeddingError::Unrepresentable);
    }
    directed.sort_unstable();
    let [min_x, max_x, min_y, max_y] = clip.frame_bounds();
    let mut sides: [Vec<(f64, f64)>; 4] = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
    let [bottom, right, top, left] = &mut sides;
    // Each interior edge takes two directed entries.
    let mut internal_edge_count = 0_usize;
    let mut edge_drafts = Vec::with_capacity(directed.len());
    for run in directed.chunk_by(|left, right| same_undirected_dual_edge(*left, *right)) {
        let (first, second) = match run {
            [left, right] => {
                if left.forward == right.forward
                    || left.owner == right.owner
                    || left.origin != right.origin
                    || left.clip_segment != right.clip_segment
                {
                    return Err(DualEmbeddingError::Unrepresentable);
                }
                internal_edge_count = internal_edge_count
                    .checked_add(1)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
                (*left, Some(*right))
            },
            [edge] => (*edge, None),
            _ => return Err(DualEmbeddingError::Unrepresentable),
        };
        let coordinates = [
            *vertex_xy
                .get(first.endpoints[0] as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?,
            *vertex_xy
                .get(first.endpoints[1] as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?,
        ];
        edge_drafts.push(DualEdgeDraft {
            endpoints: first.endpoints,
            coordinates,
            first,
            second,
        });
        if second.is_some() {
            continue;
        }
        let [a, b] = coordinates;
        let (a, b) = if first.forward { (a, b) } else { (b, a) };
        let side = if a.y == min_y && b.y == min_y {
            FrameSide::Bottom
        } else if a.x == max_x && b.x == max_x {
            FrameSide::Right
        } else if a.y == max_y && b.y == max_y {
            FrameSide::Top
        } else if a.x == min_x && b.x == min_x {
            FrameSide::Left
        } else {
            return Err(DualEmbeddingError::Unrepresentable);
        };
        let correctly_directed = match side {
            FrameSide::Bottom => a.x < b.x,
            FrameSide::Right => a.y < b.y,
            FrameSide::Top => a.x > b.x,
            FrameSide::Left => a.y > b.y,
        };
        if !correctly_directed {
            return Err(DualEmbeddingError::Unrepresentable);
        }
        let (lo, hi) = match side {
            FrameSide::Bottom | FrameSide::Top => (a.x.min(b.x), a.x.max(b.x)),
            FrameSide::Right | FrameSide::Left => (a.y.min(b.y), a.y.max(b.y)),
        };
        match side {
            FrameSide::Bottom => &mut *bottom,
            FrameSide::Right => &mut *right,
            FrameSide::Top => &mut *top,
            FrameSide::Left => &mut *left,
        }
        .push((lo, hi));
    }
    for (frame_side, side) in FrameSide::ALL.into_iter().zip([bottom, right, top, left]) {
        side.sort_unstable_by(|left, right| left.0.total_cmp(&right.0));
        let (start, end) = match frame_side {
            FrameSide::Bottom | FrameSide::Top => (min_x, max_x),
            FrameSide::Right | FrameSide::Left => (min_y, max_y),
        };
        let mut cursor = start;
        for &(lo, hi) in side.iter() {
            if lo != cursor {
                return Err(DualEmbeddingError::Unrepresentable);
            }
            cursor = hi;
        }
        if cursor != end {
            return Err(DualEmbeddingError::Unrepresentable);
        }
    }
    edge_drafts.sort_unstable_by(|left, right| {
        lex_xy(left.coordinates[0], right.coordinates[0])
            .then(lex_xy(left.coordinates[1], right.coordinates[1]))
    });
    let mut edge_id_by_pair = try_hash_map_with_capacity(edge_drafts.len())?;
    for (index, draft) in edge_drafts.iter().enumerate() {
        let edge = u32::try_from(index).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        if edge_id_by_pair
            .insert((draft.first.low, draft.first.high), edge)
            .is_some()
        {
            return Err(DualEmbeddingError::Unrepresentable);
        }
    }
    let mut published = 0_usize;
    if product.includes_polygons() {
        for ring in &rings.rings {
            let positions = ring
                .len()
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            let cell_count = polygon_output_count(positions, std::iter::empty())?;
            published = published
                .checked_add(cell_count)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
        }
    }
    if product.includes_edges() {
        let edge_positions = internal_edge_count
            .checked_mul(2)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        published = published
            .checked_add(edge_positions)
            .and_then(|count| count.checked_add(internal_edge_count))
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
    }
    let baseline = if product.includes_polygons() {
        clip.polygon_baseline.unwrap_or(0)
    } else {
        0
    };
    budget.add(published.saturating_sub(baseline))?;

    let mut receipt = site_receipts(VoronoiProducer::Dual, sites)?;
    receipt.clip_segments = clip_segment_receipts(clip, None)?;
    let mut vertex_origins: Vec<Vec<VoronoiVertexOrigin>> = std::iter::repeat_with(Vec::new)
        .take(vertex_xy.len())
        .collect();
    for (component, rounded) in center_rounded.iter().enumerate() {
        if let Some((_, id)) = rounded {
            let sites = *rings
                .center_sites
                .get(component)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            vertex_origins
                .get_mut(*id as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?
                .push(VoronoiVertexOrigin::Circumcenter {
                    component: u32::try_from(component)
                        .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                    sites,
                });
        }
    }
    let mut twin_by_dart = Vec::with_capacity(directed.len());
    twin_by_dart.resize(directed.len(), None);
    let mut internal_edges = Vec::with_capacity(if product.includes_edges() {
        internal_edge_count
    } else {
        0
    });
    for (index, draft) in edge_drafts.iter().enumerate() {
        let edge_id =
            u32::try_from(index).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        if let Some(second) = draft.second {
            *twin_by_dart
                .get_mut(draft.first.dart as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)? = Some(second.dart);
            *twin_by_dart
                .get_mut(second.dart as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)? = Some(draft.first.dart);
        }
        let [start_id, end_id] = draft.endpoints;
        let [start, end] = draft.coordinates;
        let mut origins = vec![draft.first.origin];
        let source_segment = if clip.source.is_some() {
            draft.first.clip_segment
        } else {
            None
        };
        if let Some(segment) = source_segment {
            let source_origin = VoronoiEdgeOrigin::SourceClip { segment };
            if !origins.contains(&source_origin) {
                origins.push(source_origin);
            }
        }
        let separator = match draft.first.origin {
            VoronoiEdgeOrigin::Bisector { sites } => Some(sites),
            VoronoiEdgeOrigin::SourceClip { .. } | VoronoiEdgeOrigin::Frame { .. } => None,
        };
        let frame_side = match draft.first.origin {
            VoronoiEdgeOrigin::Frame { side } => Some(side),
            VoronoiEdgeOrigin::SourceClip { .. } | VoronoiEdgeOrigin::Bisector { .. } => None,
        };
        let mut owner_transition = [None, None];
        for use_edge in [Some(draft.first), draft.second].into_iter().flatten() {
            let [left_owner, right_owner] = &mut owner_transition;
            if use_edge.forward {
                *left_owner = Some(use_edge.owner);
            } else {
                *right_owner = Some(use_edge.owner);
            }
        }
        receipt.edges.push(VoronoiEdgeReceipt {
            id: edge_id,
            endpoints: [start_id, end_id],
            separator,
            clip_sources: source_segment.into_iter().collect(),
            frame_side,
            owner_transition,
            origins,
        });
        if let Some(sites) = separator {
            receipt.separators.push(VoronoiSeparatorReceipt {
                sites,
                vertices: [start_id, end_id].to_vec(),
                edges: vec![edge_id],
            });
            if product.includes_edges() {
                internal_edges.push([start, end]);
            }
        }
        if let Some(segment) = draft.first.clip_segment {
            let clip_use = clip_uses_by_dart
                .get(draft.first.dart as usize)
                .and_then(Option::as_ref)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            if clip_use.segment != segment {
                return Err(DualEmbeddingError::Unrepresentable);
            }
            let (start_parameter, end_parameter) = if draft.first.forward {
                (
                    clip_use.start_parameter.clone(),
                    clip_use.end_parameter.clone(),
                )
            } else {
                (
                    clip_use.end_parameter.clone(),
                    clip_use.start_parameter.clone(),
                )
            };
            let reversed = start_parameter.try_cmp(&end_parameter)? == Ordering::Greater;
            let (low_parameter, high_parameter) = if reversed {
                (end_parameter.clone(), start_parameter.clone())
            } else {
                (start_parameter.clone(), end_parameter.clone())
            };
            let clip_segment = receipt
                .clip_segments
                .get_mut(segment as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            clip_segment.atoms.push(VoronoiClipAtomReceipt {
                edge: edge_id,
                reversed,
                start_parameter: low_parameter,
                end_parameter: high_parameter,
            });
            if clip.source.is_some() {
                for (vertex_id, parameter) in
                    [(start_id, &start_parameter), (end_id, &end_parameter)]
                {
                    for root in &clip_segment.source_roots {
                        let source_parameter = if root.reversed {
                            exact::ExactRational::one().try_subtract(parameter)?
                        } else {
                            parameter.clone()
                        };
                        vertex_origins
                            .get_mut(vertex_id as usize)
                            .ok_or(DualEmbeddingError::Unrepresentable)?
                            .push(VoronoiVertexOrigin::SourceClip {
                                segment,
                                parameter: source_parameter,
                                source_ring: Some(root.ring),
                                source_edge: Some(root.edge),
                            });
                    }
                }
            }
        }
        for vertex_id in [start_id, end_id] {
            if let Some(sites) = separator {
                vertex_origins
                    .get_mut(vertex_id as usize)
                    .ok_or(DualEmbeddingError::Unrepresentable)?
                    .push(VoronoiVertexOrigin::Bisector { sites });
            }
            if let Some(side) = frame_side {
                vertex_origins
                    .get_mut(vertex_id as usize)
                    .ok_or(DualEmbeddingError::Unrepresentable)?
                    .push(VoronoiVertexOrigin::Frame { side });
            }
        }
    }
    for site in sites {
        let site_id =
            u32::try_from(site.id).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        let key = PointKey::new(site.rounded_xy()?);
        if let Some((representative, vertex_id)) = interned.get(&key)
            && representative.same_position(&site.exact_point)?
        {
            vertex_origins
                .get_mut(*vertex_id as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?
                .push(VoronoiVertexOrigin::SourceSite { site: site_id });
        }
    }
    for (id, exact_point) in exact_by_id.iter().enumerate() {
        vertex_origins
            .get_mut(id)
            .ok_or(DualEmbeddingError::Unrepresentable)?
            .extend(chart_vertex_origins(chart, exact_point)?);
        let embedded = *vertex_xy
            .get(id)
            .ok_or(DualEmbeddingError::Unrepresentable)?;
        let origins = std::mem::take(
            vertex_origins
                .get_mut(id)
                .ok_or(DualEmbeddingError::Unrepresentable)?,
        );
        receipt.vertices.push(VoronoiVertexReceipt {
            id: u32::try_from(id).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
            exact_xy: exact_xy_coordinates(exact_point)?,
            origins,
            assignment: Some(embedded),
        });
        receipt.assignments.push(VoronoiAssignmentReceipt {
            vertex: u32::try_from(id).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
            embedded: u32::try_from(id).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
            point: embedded,
        });
    }
    let mut edge_roles_by_ring = Vec::with_capacity(rings.rings.len());
    let mut dart_receipts = Vec::with_capacity(directed.len());
    for (ring_index, window) in offsets.array_windows::<2>().enumerate() {
        let [start, end] = *window;
        let ids = ids_all
            .get(start..end)
            .ok_or(DualEmbeddingError::Unrepresentable)?;
        let owner = rings
            .owners
            .get(ring_index)
            .ok_or(DualEmbeddingError::Unrepresentable)?
            .0;
        let mut cell_uses = Vec::with_capacity(ids.len());
        let mut role_row = Vec::with_capacity(ids.len());
        for (&from, &to) in ids.iter().zip(ids.iter().cycle().skip(1)) {
            let (low, high) = if from <= to { (from, to) } else { (to, from) };
            let edge_id = *edge_id_by_pair
                .get(&(low, high))
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            let dart_id = u32::try_from(dart_receipts.len())
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
            let directed_edge = by_dart
                .get(dart_id as usize)
                .and_then(|edge| *edge)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            if directed_edge.from != from || directed_edge.to != to || directed_edge.owner != owner
            {
                return Err(DualEmbeddingError::Unrepresentable);
            }
            let forward = directed_edge.forward;
            let edge_record = receipt
                .edges
                .get(edge_id as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            let mut source_uses = Vec::new();
            if let Some(segment) = directed_edge.clip_segment
                && clip.source.is_some()
            {
                let clip_use = clip_uses_by_dart
                    .get(dart_id as usize)
                    .and_then(Option::as_ref)
                    .ok_or(DualEmbeddingError::Unrepresentable)?;
                if clip_use.segment != segment {
                    return Err(DualEmbeddingError::Unrepresentable);
                }
                let start_parameter = &clip_use.start_parameter;
                let end_parameter = &clip_use.end_parameter;
                let clip_segment = receipt
                    .clip_segments
                    .get(segment as usize)
                    .ok_or(DualEmbeddingError::Unrepresentable)?;
                for root in &clip_segment.source_roots {
                    let source_start = if root.reversed {
                        exact::ExactRational::one().try_subtract(start_parameter)?
                    } else {
                        start_parameter.clone()
                    };
                    let source_end = if root.reversed {
                        exact::ExactRational::one().try_subtract(end_parameter)?
                    } else {
                        end_parameter.clone()
                    };
                    source_uses.push(VoronoiEdgeSourceUse {
                        source_segment: segment,
                        source_operand: None,
                        source_ring: Some(
                            u32::try_from(root.ring)
                                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                        ),
                        source_edge: Some(
                            u32::try_from(root.edge)
                                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                        ),
                        forward: source_start.try_cmp(&source_end)? != Ordering::Greater,
                        start_parameter: source_start,
                        end_parameter: source_end,
                    });
                }
            }
            role_row.push(VoronoiRingEdgeRole {
                edge: Some(edge_id),
                forward,
                origins: edge_record.origins.clone(),
                source_uses,
            });
            cell_uses.push(VoronoiDirectedEdgeUse {
                edge: edge_id,
                forward,
            });
            dart_receipts.push(VoronoiDartReceipt {
                id: dart_id,
                origin: from,
                target: to,
                twin: twin_by_dart.get(dart_id as usize).copied().flatten(),
                edge: edge_id,
                face: None,
                owner: Some(owner),
            });
        }
        receipt.cells.push(VoronoiCellReceipt {
            owner,
            face: None,
            shell: cell_uses,
            holes: Vec::new(),
        });
        edge_roles_by_ring.push(role_row);
    }
    receipt.darts = dart_receipts;
    let edge_data = EmbeddedEdgeData::Segments(internal_edges);
    let mut polygons = Vec::new();
    if product.includes_polygons() {
        polygons.reserve(rings.rings.len());
        for (ring_index, window) in offsets.array_windows::<2>().enumerate() {
            let [start, end] = *window;
            let rounded = rounded_all
                .get(start..end)
                .expect("row offsets bracket appends to this rounded vertex column");
            let first = rounded
                .first()
                .expect("dual_build_rings admits only cells with at least three vertices");
            let capacity = rounded
                .len()
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            ensure_coordseq_vertex_capacity(capacity)?;
            let mut xs = Vec::with_capacity(capacity);
            let mut ys = Vec::with_capacity(capacity);
            xs.extend(rounded.iter().map(|xy| xy.x));
            ys.extend(rounded.iter().map(|xy| xy.y));
            xs.push(first.x);
            ys.push(first.y);
            let ring = Ring::closed_coordseq(CoordSeq::try_from_vecs(xs, ys, None, None)?)?;
            let edge_roles = vec![
                edge_roles_by_ring
                    .get(ring_index)
                    .cloned()
                    .ok_or(DualEmbeddingError::Unrepresentable)?,
            ];
            let producer_ids = ids_all
                .get(start..end)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            let producer_vertices = vec![
                producer_ids
                    .iter()
                    .map(|&id| {
                        Ok(OverlayNodeReceipt {
                            id,
                            exact_point: exact_by_id
                                .get(id as usize)
                                .ok_or(DualEmbeddingError::Unrepresentable)?
                                .clone(),
                            assignment: *vertex_xy
                                .get(id as usize)
                                .ok_or(DualEmbeddingError::Unrepresentable)?,
                        })
                    })
                    .collect::<std::result::Result<Vec<_>, DualEmbeddingError>>()?,
            ];
            let owner = rings
                .owners
                .get(ring_index)
                .ok_or(DualEmbeddingError::Unrepresentable)?
                .0;
            polygons.push(EmbeddedPolygon {
                polygon: Polygon::new(ring, Vec::new()),
                owner: Some(owner),
                edge_roles,
                producer_vertices,
            });
        }
    }
    let receipt = std::sync::Arc::new(receipt);
    Ok(EmbeddedVoronoi {
        polygons,
        edges: EmbeddedEdges {
            data: edge_data,
            receipt: std::sync::Arc::clone(&receipt),
        },
        receipt,
    })
}

#[cfg(test)]
pub(super) fn dual_build(
    sites: &[Site],
    boundary: VoronoiBoundary<'_>,
    budget: &mut ExpansionBudget,
    mesh: &CertifiedDelaunay,
) -> Result<EmbeddedVoronoi> {
    dual_build_product(sites, boundary, budget, mesh, VoronoiProduct::Both, None)
}

pub(super) fn dual_build_product(
    sites: &[Site],
    boundary: VoronoiBoundary<'_>,
    budget: &mut ExpansionBudget,
    mesh: &CertifiedDelaunay,
    product: VoronoiProduct,
    chart: Option<&PointSelectionChart>,
) -> Result<EmbeddedVoronoi> {
    let clip = normalize_clip_with_chart(sites, boundary, product.includes_polygons(), chart)?;
    if mesh.collinear_order().is_some() {
        return Err(voronoi_error(
            "collinear sites require the exact one-dimensional lane",
        ));
    }
    let complex = delaunay_complex(mesh, sites)?;
    let rings = dual_build_rings(sites, mesh, complex, &clip)?;
    match dual_embed(&rings, &clip, sites, product, budget, chart) {
        Ok(result) => Ok(result),
        Err(DualEmbeddingError::Unrepresentable) => {
            // The optimized lane remains byte-identical when its nearest
            // embedding passes. Only a binary64 certificate failure uses the
            // general DCEL lane, where constructed vertices can move to
            // adjacent floats under the complete cell/face certificate.
            build_product_with_chart(sites, boundary, budget, false, mesh, product, chart)
        },
        Err(DualEmbeddingError::Other(error)) => Err(error),
    }
}
