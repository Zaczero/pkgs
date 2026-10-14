//! Direct dual construction from the certified Delaunay mesh.

use super::{
    CertifiedDelaunay, CoordSeq, DelaunayComplex, DirectedDualEdge, DualClipUse, DualEdgeDraft,
    DualRings, DualVertex, EMPTY, EdgeTag, EmbeddedEdgeData, EmbeddedEdges, EmbeddedPolygon,
    EmbeddedVoronoi, ExactSign, ExpansionBudget, FrameSide, IncidenceOutput, IncidentFaceRow,
    IncidentFaces, NormalizedClip, Ordering, OverlayNodeReceipt, Point, PointKey,
    PointSelectionChart, Polygon, Result, Ring, Site, SiteId, Tagged, VoronoiAssignmentReceipt,
    VoronoiBoundary, VoronoiCellReceipt, VoronoiDartReceipt, VoronoiDirectedEdgeUse,
    VoronoiEdgeOrigin, VoronoiEdgeReceipt, VoronoiProducer, VoronoiProduct, VoronoiRingEdgeRole,
    VoronoiSeparatorReceipt, VoronoiVertexOrigin, VoronoiVertexReceipt, XY, axis_aligned_rectangle,
    between, build_product_with_chart, clip_segment_receipts, delaunay_complex,
    ensure_coordseq_vertex_capacity, exact, exact_bisector_frame_intersections,
    exact_segment_parameter, exact_xy_coordinates, lex_xy, normalize_clip_with_chart,
    polygon_is_valid, polygon_output_count, receipt_frame_side, site_at, site_by_id, site_receipts,
    stored_xy_cycle_matches_orientation, try_hash_map_with_capacity, voronoi_error,
};

mod embed;

pub(super) use embed::dual_embed;

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

const fn same_undirected_dual_edge(left: DirectedDualEdge, right: DirectedDualEdge) -> bool {
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
            if exact::incidence::orient_points(clip_start, clip_end, start)? == ExactSign::Zero
                && exact::incidence::orient_points(clip_start, clip_end, end)? == ExactSign::Zero
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
    for (edge, &halfedge) in halfedges.iter().enumerate() {
        let endpoint = flat(dual_next_halfedge(edge));
        if start_edge[endpoint] == usize::MAX || halfedge == EMPTY {
            start_edge[endpoint] = edge;
        }
    }
    let mut offsets: Vec<usize> = Vec::new();
    let mut boundaries = Vec::new();
    offsets.reserve(site_count + 1);
    boundaries.reserve(site_count);
    offsets.push(0);
    for &start in &start_edge {
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
const fn dual_site_at_halfedge(triangles: &[[usize; 3]], halfedge: usize) -> usize {
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
    let opposite_sign =
        exact::incidence::orient_points(left, right, &site_at(sites, opposite).exact_point)?;
    let line = exact::ExactLine::perpendicular_bisector_points(left, right)?;
    for hit in exact_bisector_frame_intersections([left, right], &line, frame)? {
        if exact::incidence::orient_points(left, right, &hit.1)? != opposite_sign {
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
        let sign = exact::incidence::orient_points(start, middle, end)?;
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
        let orientation = exact::incidence::orient_points(start, end, point)?;
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
#[expect(
    clippy::float_cmp,
    reason = "certified Voronoi topology classifies exact frame and vertex identity before rounding"
)]
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
                return Ok(exact::incidence::orient_points(a, b, point)? != ExactSign::Negative);
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
            let hit = exact::incidence::line_intersection(&boundary, &edge)?;
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
    let frame = if let Some(frame) = exact::frame::enclosing_frame_binary64(frame_inputs())? {
        frame
    } else {
        let mut inputs = Vec::with_capacity(frame_inputs().count());
        inputs.extend(frame_inputs().cloned());
        exact::frame::enclosing_frame(&inputs)?
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
#[expect(
    clippy::float_cmp,
    reason = "certified Voronoi topology classifies exact frame and vertex identity before rounding"
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
    match dual_embed(&rings, &clip, sites, product, budget) {
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
