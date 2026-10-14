//! Analytic Voronoi construction for collinear sites.

use super::{
    AtomicEdge, BTreeSet, CertifiedDelaunay, ClipSegmentId, CoordSeq, EdgeId, EmbeddedEdgeData,
    EmbeddedEdges, EmbeddedPolygon, EmbeddedVoronoi, ExactEdgeChain, ExactSign, ExactSubdivision,
    ExactVertex, ExpansionBudget, FrameSide, IndexedTopology, NormalizedClip, Ordering,
    OverlayNodeReceipt, OverlaySourceEdgeUse, OwnerTransition, PointSelectionChart, Polygon,
    Result, Ring, Shape, Site, SiteId, Transition, VertexId, VoronoiBoundary, VoronoiEdgeOrigin,
    VoronoiProducer, VoronoiProduct, VoronoiRingEdgeRole, XY,
    binary_cartesian_intersection_with_edge_roles, collinear_polygon_receipt, embed,
    empty_packed_edges, exact, exact_cycle_error, fixed_coordinates, lex_position,
    normalize_clip_with_chart, orient_separator, polygon_output_count, receipt_frame_side, site_at,
    site_receipts, sort_unique_along, try_dedup_by, try_sort_by, voronoi_error,
};

mod edges;

#[cfg(test)]
pub(super) use edges::build_collinear_edges;
pub(in crate::geometry::tessellation) use edges::build_collinear_edges_with_chart;

struct CollinearClipEdge<'a> {
    start: &'a exact::ExactPoint,
    end: &'a exact::ExactPoint,
}

struct CollinearProjectionEvent {
    level: exact::ExactRational,
    edge: usize,
}

struct CollinearBisector {
    level: exact::ExactRational,
    sites: [usize; 2],
}

struct CollinearLineEvent {
    point: exact::ExactPoint,
    crossing: bool,
    boundary_starts: usize,
    boundary_ends: usize,
}

struct CollinearClipEvents {
    separators: Vec<([usize; 2], Vec<exact::ExactPoint>)>,
    edge_events: Vec<Vec<exact::ExactPoint>>,
}

/// Voronoi cells of collinear sites: the strips between consecutive
/// perpendicular bisectors. Each strip is cut from the clip frame by at most
/// two exact half-planes and rounded once; neighbouring cells round the same
/// exact bisector crossings, so they share their vertices. A polygon clip then
/// intersects each strip with the polygon.
pub(in crate::geometry::tessellation) fn build_collinear_polygons_with_chart(
    sites: &[Site],
    boundary: VoronoiBoundary<'_>,
    budget: &mut ExpansionBudget,
    mesh: &CertifiedDelaunay,
    chart: Option<&PointSelectionChart>,
) -> Result<EmbeddedVoronoi> {
    let order = mesh
        .collinear_order()
        .ok_or_else(|| voronoi_error("non-collinear sites entered the collinear Voronoi lane"))?;
    if matches!(boundary, VoronoiBoundary::Envelope)
        && let Some(first) = sites.first()
    {
        let mut same_x = true;
        let mut same_y = true;
        for site in sites.iter().skip(1) {
            same_x &= site.exact_point.compare_x(&first.exact_point)?.is_eq();
            same_y &= site.exact_point.compare_y(&first.exact_point)?.is_eq();
        }
        if same_x || same_y {
            let receipt = std::sync::Arc::new(site_receipts(VoronoiProducer::Collinear, sites)?);
            return Ok(EmbeddedVoronoi {
                polygons: Vec::new(),
                edges: EmbeddedEdges {
                    data: EmbeddedEdgeData::Segments(Vec::new()),
                    receipt: std::sync::Arc::clone(&receipt),
                },
                receipt,
            });
        }
    }
    let clip = normalize_clip_with_chart(sites, boundary, true, chart)?;
    let frame = if clip.source.is_none() {
        clip.rings
            .first()
            .cloned()
            .expect("generated Voronoi clips contain their frame ring")
    } else {
        let outer = clip
            .rings
            .first()
            .ok_or_else(|| voronoi_error("Voronoi clip has no outer ring"))?;
        exact::frame::axis_aligned_bounds(outer.iter())?.to_vec()
    };
    let mut cells = Vec::new();
    let baseline = clip.polygon_baseline.unwrap_or(0);
    let mut published = 0_usize;
    let mut ordered_sites = order.iter().copied().peekable();
    let mut previous = None;
    while let Some(site_index) = ordered_sites.next() {
        let next = ordered_sites.peek().copied();
        let here = &site_at(sites, site_index).exact_point;
        let mut ring = frame.clone();
        let mut ring_origins = FrameSide::ALL
            .into_iter()
            .map(|side| {
                vec![VoronoiEdgeOrigin::Frame {
                    side: receipt_frame_side(side),
                }]
            })
            .collect::<Vec<_>>();
        for neighbor in previous.into_iter().chain(next) {
            let line = exact::ExactLine::perpendicular_bisector_points(
                here,
                &site_at(sites, neighbor).exact_point,
            )?;
            let keep = exact::incidence::signed_line_product(&line, here)?;
            let [left, right] = [
                u32::try_from(site_index).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                u32::try_from(neighbor).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
            ];
            let pair = [left.min(right), left.max(right)];
            (ring, ring_origins) = clip_to_half_plane_with_origins(
                &ring,
                &ring_origins,
                &line,
                keep,
                VoronoiEdgeOrigin::Bisector { sites: pair },
            )?;
        }
        let (mut vertices, mut edge_origins, mut exact_vertices) =
            rounded_ring_with_origins(&ring, &ring_origins)?;
        if vertices.len() < 3 {
            continue;
        }
        if clip.source.is_none() {
            let positions = vertices
                .len()
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            let count = polygon_output_count(positions, std::iter::empty())?;
            let next_published = published
                .checked_add(count)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            budget.check_additional(next_published.saturating_sub(baseline))?;
            published = next_published;
        }
        // Start at the least vertex, as every other emitted cell does.
        let (least, _) = vertices
            .iter()
            .enumerate()
            .min_by(|(_, left), (_, right)| {
                left.x.total_cmp(&right.x).then(left.y.total_cmp(&right.y))
            })
            .expect("a nondegenerate Voronoi strip has a least vertex");
        vertices.rotate_left(least);
        edge_origins.rotate_left(least);
        exact_vertices.rotate_left(least);
        let first = *vertices
            .first()
            .expect("a nondegenerate Voronoi strip has a first vertex");
        vertices.push(first);
        let strip = Polygon::new(
            Ring::try_from_closed(CoordSeq::try_from_xy(&vertices)?)?,
            Vec::new(),
        );
        if clip.source.is_none() {
            let edge_roles = vec![
                edge_origins
                    .into_iter()
                    .map(|origins| VoronoiRingEdgeRole {
                        edge: None,
                        forward: true,
                        origins,
                    })
                    .collect::<Vec<_>>(),
            ];
            let producer_vertices = exact_vertices
                .into_iter()
                .zip(vertices.iter().copied())
                .enumerate()
                .map(|(index, (exact_point, assignment))| {
                    Ok(OverlayNodeReceipt {
                        id: u32::try_from(index)
                            .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                        exact_point,
                        assignment,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            cells.push(EmbeddedPolygon {
                polygon: strip,
                owner: Some(
                    u32::try_from(site_index)
                        .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                ),
                edge_roles,
                producer_vertices: vec![producer_vertices],
            });
        } else {
            let strip = Shape::Polygon(strip);
            let clip_shape = Shape::Polygon(
                clip.geometry
                    .as_ref()
                    .expect("polygon clips retain their chart-space polygon")
                    .clone(),
            );
            let intersection = binary_cartesian_intersection_with_edge_roles(&strip, &clip_shape)?;
            for part in intersection {
                let count = polygon_output_count(
                    part.polygon.shell.len(),
                    part.polygon.holes.iter().map(|hole| hole.len()),
                )?;
                let next_published = published
                    .checked_add(count)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
                budget.check_additional(next_published.saturating_sub(baseline))?;
                published = next_published;
                let edge_roles =
                    collinear_overlay_edge_roles(part.edge_sources, &edge_origins, &clip)?;
                cells.push(EmbeddedPolygon {
                    polygon: part.polygon,
                    owner: Some(
                        u32::try_from(site_index)
                            .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                    ),
                    edge_roles,
                    producer_vertices: part.vertices,
                });
            }
        }
        previous = Some(site_index);
    }
    budget.add(published.saturating_sub(baseline))?;
    let receipt = std::sync::Arc::new(collinear_polygon_receipt(sites, &mut cells)?);
    Ok(EmbeddedVoronoi {
        polygons: cells,
        edges: EmbeddedEdges {
            data: EmbeddedEdgeData::Segments(Vec::new()),
            receipt: std::sync::Arc::clone(&receipt),
        },
        receipt,
    })
}

fn clip_to_half_plane_with_origins(
    ring: &[exact::ExactPoint],
    edge_origins: &[Vec<VoronoiEdgeOrigin>],
    line: &exact::ExactLine,
    keep: ExactSign,
    cut_origin: VoronoiEdgeOrigin,
) -> Result<(Vec<exact::ExactPoint>, Vec<Vec<VoronoiEdgeOrigin>>)> {
    struct Piece {
        start: exact::ExactPoint,
        end: exact::ExactPoint,
        origins: Vec<VoronoiEdgeOrigin>,
    }

    if ring.len() != edge_origins.len() {
        return Err(voronoi_error(
            "collinear strip edge origins do not match the exact ring",
        ));
    }
    let mut pieces = Vec::new();
    for (index, (start, end)) in ring.iter().zip(ring.iter().cycle().skip(1)).enumerate() {
        let start_side = exact::incidence::signed_line_product(line, start)?;
        let end_side = exact::incidence::signed_line_product(line, end)?;
        let start_inside = start_side == keep || start_side == ExactSign::Zero;
        let end_inside = end_side == keep || end_side == ExactSign::Zero;
        let crossing = if start_inside == end_inside {
            None
        } else {
            Some(if start_side == ExactSign::Zero {
                start.clone()
            } else if end_side == ExactSign::Zero {
                end.clone()
            } else {
                let support = exact::ExactLine::through_points(start, end)?;
                exact::incidence::line_intersection(line, &support)?
            })
        };
        let (piece_start, piece_end) = match (start_inside, end_inside) {
            (true, true) => (Some(start.clone()), Some(end.clone())),
            (true, false) => (Some(start.clone()), crossing),
            (false, true) => (crossing, Some(end.clone())),
            (false, false) => (None, None),
        };
        if let (Some(piece_start), Some(piece_end)) = (piece_start, piece_end)
            && !piece_start.same_position(&piece_end)?
        {
            pieces.push(Piece {
                start: piece_start,
                end: piece_end,
                origins: edge_origins
                    .get(index)
                    .ok_or_else(|| voronoi_error("collinear strip edge origins are absent"))?
                    .clone(),
            });
        }
    }
    if pieces.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let mut edges = Vec::with_capacity(pieces.len().saturating_add(1));
    for (piece, next) in pieces.iter().zip(pieces.iter().cycle().skip(1)) {
        edges.push((
            piece.start.clone(),
            piece.end.clone(),
            piece.origins.clone(),
        ));
        if !piece.end.same_position(&next.start)? {
            edges.push((piece.end.clone(), next.start.clone(), vec![cut_origin]));
        }
    }
    let vertices = edges.iter().map(|(start, ..)| start.clone()).collect();
    let origins = edges.into_iter().map(|(_, _, origins)| origins).collect();
    Ok((vertices, origins))
}

type RoundedRingWithOrigins = (Vec<XY>, Vec<Vec<VoronoiEdgeOrigin>>, Vec<exact::ExactPoint>);

fn rounded_ring_with_origins(
    ring: &[exact::ExactPoint],
    edge_origins: &[Vec<VoronoiEdgeOrigin>],
) -> Result<RoundedRingWithOrigins> {
    if ring.len() != edge_origins.len() {
        return Err(voronoi_error(
            "collinear strip edge origins do not match the exact ring",
        ));
    }
    let Some(first) = ring.first() else {
        return Ok((Vec::new(), Vec::new(), Vec::new()));
    };
    let mut points = vec![first.round_nearest_even()?];
    let mut exact_points = vec![first.clone()];
    let mut origins = Vec::with_capacity(ring.len());
    let mut pending = Vec::new();
    for (index, (edge_origin, next_vertex)) in edge_origins
        .iter()
        .zip(ring.iter().cycle().skip(1))
        .enumerate()
    {
        for origin in edge_origin {
            if !pending.contains(origin) {
                pending.push(*origin);
            }
        }
        let next = next_vertex.round_nearest_even()?;
        let current = *points
            .last()
            .expect("the exact strip ring begins with a point");
        if next != current {
            origins.push(std::mem::take(&mut pending));
            if index.saturating_add(1) < ring.len() {
                points.push(next);
                exact_points.push(next_vertex.clone());
            }
        }
    }
    if points.len() > 1 && points.first() == points.last() {
        points.pop();
        exact_points.pop();
        let last_edge = origins
            .last_mut()
            .ok_or_else(|| voronoi_error("rounded collinear strip has no closing edge"))?;
        for origin in std::mem::take(&mut pending) {
            if !last_edge.contains(&origin) {
                last_edge.push(origin);
            }
        }
    }
    if !pending.is_empty() {
        return Err(voronoi_error(
            "rounded collinear strip leaves an unassigned edge origin",
        ));
    }
    if origins.len() != points.len() {
        return Err(voronoi_error("rounded collinear strip lost an edge origin"));
    }
    Ok((points, origins, exact_points))
}

fn clip_segment_offsets(clip: &NormalizedClip<'_>) -> Result<Vec<usize>> {
    let offset_count = clip
        .rings
        .len()
        .checked_add(1)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let mut offsets: Vec<usize> = Vec::with_capacity(offset_count);
    offsets.push(0);
    for ring in &clip.rings {
        let current = offsets
            .last()
            .copied()
            .expect("clip segment offsets begin at zero");
        offsets.push(
            current
                .checked_add(ring.len())
                .ok_or(crate::error::ResourceError::SizeOverflow)?,
        );
    }
    Ok(offsets)
}

fn collinear_overlay_edge_roles(
    source_edges: Vec<Vec<Vec<OverlaySourceEdgeUse>>>,
    strip_origins: &[Vec<VoronoiEdgeOrigin>],
    clip: &NormalizedClip<'_>,
) -> Result<Vec<Vec<VoronoiRingEdgeRole>>> {
    let clip_offsets = clip_segment_offsets(clip)?;
    let mut rings = Vec::with_capacity(source_edges.len());
    for ring in source_edges {
        let mut roles = Vec::with_capacity(ring.len());
        for edge_sources in ring {
            let mut origins = Vec::new();
            for source in edge_sources {
                let location = source.location;
                match location.operand {
                    0 => {
                        if location.ring != 0 {
                            return Err(voronoi_error(
                                "collinear strip source has a non-shell ring ID",
                            ));
                        }
                        let strip_edge = usize::try_from(location.edge)
                            .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
                        let strip_origins = strip_origins
                            .get(strip_edge)
                            .ok_or_else(|| voronoi_error("collinear strip edge role is unknown"))?;
                        for origin in strip_origins {
                            if !origins.contains(origin) {
                                origins.push(*origin);
                            }
                        }
                    },
                    1 => {
                        let ring = usize::try_from(location.ring)
                            .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
                        let edge = usize::try_from(location.edge)
                            .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
                        let segment = clip_offsets
                            .get(ring)
                            .copied()
                            .and_then(|offset| offset.checked_add(edge))
                            .ok_or_else(|| voronoi_error("clip source edge is unknown"))?;
                        let segment = u32::try_from(segment)
                            .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
                        let origin = VoronoiEdgeOrigin::SourceClip { segment };
                        if !origins.contains(&origin) {
                            origins.push(origin);
                        }
                    },
                    _ => {
                        return Err(voronoi_error(
                            "collinear overlay returned an unknown operand",
                        ));
                    },
                }
            }
            if origins.is_empty() {
                return Err(voronoi_error(
                    "collinear output edge has no producer origin",
                ));
            }
            roles.push(VoronoiRingEdgeRole {
                edge: None,
                forward: true,
                origins,
            });
        }
        rings.push(roles);
    }
    Ok(rings)
}

fn collinear_clip_events(
    sites: &[Site],
    order: &[usize],
    clip: &NormalizedClip<'_>,
) -> Result<CollinearClipEvents> {
    let (Some(&first_site), Some(&last_site)) = (order.first(), order.last()) else {
        return Err(voronoi_error("collinear Voronoi site order is empty"));
    };
    let first = sites
        .get(first_site)
        .ok_or_else(|| voronoi_error("collinear site order is out of range"))?
        .exact_point
        .clone();
    let last = sites
        .get(last_site)
        .ok_or_else(|| voronoi_error("collinear site order is out of range"))?
        .exact_point
        .clone();
    let one = exact::ExactDyadic::one();
    let zero = exact::ExactDyadic::zero();
    let x = |point: &exact::ExactPoint| point.projected_coordinate(&one, &zero);
    let y = |point: &exact::ExactPoint| point.projected_coordinate(&zero, &one);
    let direction_x = x(&last)?.try_subtract(&x(&first)?)?;
    let direction_y = y(&last)?.try_subtract(&y(&first)?)?;
    let projection = |point: &exact::ExactPoint| {
        x(point)?
            .try_product(&direction_x)?
            .try_add(&y(point)?.try_product(&direction_y)?)
    };
    let mut site_projections = Vec::with_capacity(order.len());
    for &site in order {
        let point = &sites
            .get(site)
            .ok_or_else(|| voronoi_error("collinear site order is out of range"))?
            .exact_point;
        site_projections.push(projection(point)?);
    }
    let mut bisectors = Vec::with_capacity(order.len().saturating_sub(1));
    for (site_pair, projection_pair) in order
        .array_windows::<2>()
        .zip(site_projections.array_windows::<2>())
    {
        let [left, right] = *site_pair;
        let [left_projection, right_projection] = projection_pair;
        bisectors.push(CollinearBisector {
            level: left_projection.try_add(right_projection)?,
            sites: [left, right],
        });
    }
    try_sort_by(&mut bisectors, |left, right| {
        Ok(left
            .level
            .try_cmp(&right.level)?
            .then(left.sites.cmp(&right.sites)))
    })?;
    for [left, right] in bisectors.array_windows::<2>() {
        if left.level.try_cmp(&right.level)? == Ordering::Equal {
            return Err(voronoi_error(
                "collinear Voronoi bisectors are not distinct",
            ));
        }
    }

    let clip_segment_count = clip.rings.iter().try_fold(0_usize, |count, ring| {
        count
            .checked_add(ring.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)
    })?;
    let mut edges = Vec::with_capacity(clip_segment_count);
    let mut low_events = Vec::with_capacity(clip_segment_count);
    let mut high_events = Vec::with_capacity(clip_segment_count);
    let twice = exact::ExactRational::from_f64(2.0);
    for ring in &clip.rings {
        for (start, end) in ring.iter().zip(ring.iter().cycle().skip(1)) {
            let start_level = projection(start)?.try_product(&twice)?;
            let end_level = projection(end)?.try_product(&twice)?;
            let edge_index = edges.len();
            edges.push(CollinearClipEdge { start, end });
            let (low, high) = if start_level.try_cmp(&end_level)?.is_le() {
                (start_level, end_level)
            } else {
                (end_level, start_level)
            };
            low_events.push(CollinearProjectionEvent {
                level: low,
                edge: edge_index,
            });
            high_events.push(CollinearProjectionEvent {
                level: high,
                edge: edge_index,
            });
        }
    }
    let sort_projection_events = |events: &mut Vec<CollinearProjectionEvent>| {
        try_sort_by(events, |left, right| {
            Ok(left
                .level
                .try_cmp(&right.level)?
                .then(left.edge.cmp(&right.edge)))
        })
    };
    sort_projection_events(&mut low_events)?;
    sort_projection_events(&mut high_events)?;

    let mut active = BTreeSet::new();
    let (mut low_cursor, mut high_cursor) = (0, 0);
    let mut edge_events: Vec<Vec<exact::ExactPoint>> = std::iter::repeat_with(Vec::new)
        .take(clip_segment_count)
        .collect();
    let mut chains = Vec::new();
    for bisector in bisectors {
        while let Some(event) = low_events.get(low_cursor) {
            if event.level.try_cmp(&bisector.level)?.is_gt() {
                break;
            }
            active.insert(event.edge);
            low_cursor = low_cursor
                .checked_add(1)
                .expect("cursor is bounded by the low-event vector length");
        }
        while let Some(event) = high_events.get(high_cursor) {
            if !event.level.try_cmp(&bisector.level)?.is_lt() {
                break;
            }
            active.remove(&event.edge);
            high_cursor = high_cursor
                .checked_add(1)
                .expect("cursor is bounded by the high-event vector length");
        }

        let [left_site, right_site] = bisector.sites;
        let line = exact::ExactLine::perpendicular_bisector_points(
            &sites
                .get(left_site)
                .ok_or_else(|| voronoi_error("collinear bisector site is out of range"))?
                .exact_point,
            &sites
                .get(right_site)
                .ok_or_else(|| voronoi_error("collinear bisector site is out of range"))?
                .exact_point,
        )?;
        let mut events = Vec::with_capacity(active.len().saturating_mul(2));
        for &edge_index in &active {
            let edge = edges
                .get(edge_index)
                .ok_or_else(|| voronoi_error("active clip edge is out of range"))?;
            let start_sign = exact::incidence::signed_line_product(&line, edge.start)?;
            let end_sign = exact::incidence::signed_line_product(&line, edge.end)?;
            if start_sign == ExactSign::Zero && end_sign == ExactSign::Zero {
                let (start, end) = if edge.start.compare_lex(edge.end)?.is_lt() {
                    (edge.start, edge.end)
                } else {
                    (edge.end, edge.start)
                };
                events.push(CollinearLineEvent {
                    point: start.clone(),
                    crossing: false,
                    boundary_starts: 1,
                    boundary_ends: 0,
                });
                events.push(CollinearLineEvent {
                    point: end.clone(),
                    crossing: false,
                    boundary_starts: 0,
                    boundary_ends: 1,
                });
                edge_events
                    .get_mut(edge_index)
                    .ok_or_else(|| voronoi_error("clip edge has no event row"))?
                    .extend([edge.start.clone(), edge.end.clone()]);
                continue;
            }

            let crossing = (start_sign == ExactSign::Negative && end_sign != ExactSign::Negative)
                || (start_sign != ExactSign::Negative && end_sign == ExactSign::Negative);
            let point = if start_sign == ExactSign::Zero {
                Some(edge.start.clone())
            } else if end_sign == ExactSign::Zero {
                Some(edge.end.clone())
            } else if start_sign != end_sign {
                let support = exact::ExactLine::through_points(edge.start, edge.end)?;
                let point = exact::incidence::line_intersection(&line, &support)?;
                matches!(
                    exact::incidence::segment_intersection(edge.start, edge.end, &point, &point)?,
                    exact::SegmentIntersection::Point(_)
                )
                .then_some(())
                .expect(
                    "opposite exact endpoint signs put the support intersection on the segment",
                );
                Some(point)
            } else {
                None
            }
            .expect("an active clip edge brackets the exact bisector projection level");
            edge_events
                .get_mut(edge_index)
                .ok_or_else(|| voronoi_error("clip edge has no event row"))?
                .push(point.clone());
            events.push(CollinearLineEvent {
                point,
                crossing,
                boundary_starts: 0,
                boundary_ends: 0,
            });
        }
        try_sort_by(&mut events, |left, right| {
            left.point.compare_lex(&right.point)
        })?;
        let mut grouped: Vec<CollinearLineEvent> = Vec::with_capacity(events.len());
        for event in events {
            if let Some(earlier) = grouped.last_mut()
                && earlier.point.same_position(&event.point)?
            {
                earlier.crossing ^= event.crossing;
                earlier.boundary_starts = earlier
                    .boundary_starts
                    .checked_add(event.boundary_starts)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
                earlier.boundary_ends = earlier
                    .boundary_ends
                    .checked_add(event.boundary_ends)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
                continue;
            }
            grouped.push(event);
        }
        let events = grouped;

        let mut inside = false;
        let mut boundary_active = 0_usize;
        let mut current = Vec::new();
        for [event, next_event] in events.array_windows::<2>() {
            inside ^= event.crossing;
            boundary_active = boundary_active
                .checked_sub(event.boundary_ends)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            boundary_active = boundary_active
                .checked_add(event.boundary_starts)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            let from = &event.point;
            let to = &next_event.point;
            if inside || boundary_active != 0 {
                if current.is_empty() {
                    current.push(from.clone());
                }
                current.push(to.clone());
            } else if current.len() >= 2 {
                chains.push((bisector.sites, std::mem::take(&mut current)));
            } else {
                current.clear();
            }
        }
        if let Some(event) = events.last() {
            inside ^= event.crossing;
            boundary_active = boundary_active
                .checked_sub(event.boundary_ends)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            boundary_active = boundary_active
                .checked_add(event.boundary_starts)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
        }
        (!inside && boundary_active == 0)
            .then_some(())
            .expect("closed clip rings balance exact crossings and boundary interval endpoints");
        if current.len() >= 2 {
            chains.push((bisector.sites, current));
        }
    }
    Ok(CollinearClipEvents {
        separators: chains,
        edge_events,
    })
}
