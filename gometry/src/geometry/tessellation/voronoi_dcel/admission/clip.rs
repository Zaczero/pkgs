//! Clip normalization and rounded-boundary certification.

use super::{
    ClipBoundaryFailure, ClipCertificate, ClipCertificateEdge, ClipRingParity, ClipTrace,
    ControlFlow, CoordSeq, ExactCycleContact, ExactPairTouch, ExactSign, ExactSubdivision,
    MappedSourceEdge, NormalizedClip, NormalizedSourceRing, OpenClipRing, Ordering, Point,
    PointKey, PointSelectionChart, Polygon, PolygonPointIndex, Result, Ring, SearchAccount,
    Segment, SegmentParameter, Site, VertexId, VoronoiBoundary, VoronoiClipSourceRoot, XY,
    YStabbingIndex, clip_source_trace_error, ensure_coordseq_vertex_capacity, exact,
    exact_cycle_error, polygon_output_baseline, sort_unique_along, try_dedup_by,
    try_hash_map_with_capacity, try_sort_by, validate_exact_contact_graph,
    validate_exact_rectangle, voronoi_error,
};
use crate::geographic::retained_exact::sites::retained_site_longitude;

pub(in crate::geometry::tessellation::voronoi_dcel) fn exact_chart_point(
    chart: Option<&PointSelectionChart>,
    point: XY,
) -> Result<exact::ExactPoint> {
    let Some(chart) = chart else {
        return Ok(exact::ExactPoint::from_xy(point));
    };
    let frame = chart.frame();
    let longitude = chart.longitude_site(point.x)?;
    Ok(exact::ExactPoint::from_dyadics(
        retained_site_longitude(frame.longitude(), longitude)?,
        exact::ExactDyadic::from_f64(point.y),
    ))
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "Ring guarantees at least four closed coordinates, so subtracting its explicit closing point is safe"
)]
pub(super) fn open_ring(ring: &Ring, chart: Option<&PointSelectionChart>) -> Result<OpenClipRing> {
    let open = ring.len() - 1;
    let mut exact_points = Vec::new();
    let mut fixed = Vec::new();
    let mut chart_fixed = Vec::new();
    let mut vertex_indices = Vec::with_capacity(open);
    exact_points.reserve(open);
    fixed.reserve(open);
    for point in ring.iter().take(open) {
        let point = point.xy();
        let exact = exact_chart_point(chart, point)?;
        if let Some(previous) = exact_points.last()
            && exact.same_position(previous)?
        {
            vertex_indices.push(
                exact_points
                    .len()
                    .checked_sub(1)
                    .expect("a repeated point follows an admitted ring vertex"),
            );
            continue;
        }
        vertex_indices.push(exact_points.len());
        chart_fixed.push(exact.round_nearest_even()?);
        exact_points.push(exact);
        fixed.push(point);
    }
    if exact_points.len() < 3 {
        return Err(voronoi_error(
            "exact Voronoi clip ring has fewer than three vertices",
        ));
    }
    let source_edges = vertex_indices
        .iter()
        .copied()
        .zip(vertex_indices.iter().copied().cycle().skip(1))
        .take(open)
        .map(|(start, end)| (start != end).then_some(start))
        .collect();
    Ok((exact_points, fixed, chart_fixed, source_edges))
}

const fn reverse_ring(points: &mut [exact::ExactPoint], fixed: &mut [XY], chart_fixed: &mut [XY]) {
    points.reverse();
    fixed.reverse();
    chart_fixed.reverse();
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "least cyclic rotation compares random offsets modulo this admitted nonempty ring; direct access preserves constant-time candidate steps without a linear nth lookup"
)]
pub(super) fn rotate_ring(
    points: &mut [exact::ExactPoint],
    fixed: &mut [XY],
    chart_fixed: &mut [XY],
) -> Result<usize> {
    let count = points.len();
    let mut least = 0;
    for candidate in 1..count {
        for offset in 0..count {
            match points[(candidate + offset) % count]
                .compare_lex(&points[(least + offset) % count])?
            {
                Ordering::Less => least = candidate,
                Ordering::Greater => {},
                Ordering::Equal => continue,
            }
            break;
        }
    }
    points.rotate_left(least);
    fixed.rotate_left(least);
    chart_fixed.rotate_left(least);
    Ok(least)
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "source edge and rotation are admitted ring ordinals modulo its nonzero length"
)]
const fn normalized_source_edge(
    edge: usize,
    count: usize,
    rotation: usize,
    reversed: bool,
) -> usize {
    // Source edges are ordinals modulo the admitted nonempty ring length.
    let oriented = if reversed {
        count
            .wrapping_sub(1)
            .wrapping_sub(edge.wrapping_add(1) % count)
    } else {
        edge
    };
    oriented.wrapping_add(count).wrapping_sub(rotation) % count
}

fn map_source_polygon_contacts(
    source_contacts: &[crate::geometry::PolygonContact],
    source_edges: &[Vec<Option<usize>>],
    source_rings: &[Option<NormalizedSourceRing>],
    rings: &[Vec<exact::ExactPoint>],
    fixed: &[Vec<XY>],
    chart: Option<&PointSelectionChart>,
) -> Result<(Vec<ExactCycleContact>, Vec<ExactPairTouch>)> {
    let mut contacts = Vec::with_capacity(source_contacts.len());
    let mut pair_touches = Vec::new();
    let mut touch_index = try_hash_map_with_capacity(source_contacts.len())?;
    for source in source_contacts {
        let [first_ring, second_ring] = source.rings;
        let [first_edge, second_edge] = source.edges;
        let [first_parameter, second_parameter] = &source.parameters;
        let first = map_source_polygon_edge(
            first_ring,
            first_edge,
            first_parameter,
            source.point,
            source_edges,
            source_rings,
            rings,
            fixed,
        )?;
        let second = map_source_polygon_edge(
            second_ring,
            second_edge,
            second_parameter,
            source.point,
            source_edges,
            source_rings,
            rings,
            fixed,
        )?;
        if first.ring == second.ring || (first.endpoint.is_none() && second.endpoint.is_none()) {
            return Err(exact_cycle_error());
        }
        let [first, second] = if first.ring > second.ring {
            [second, first]
        } else {
            [first, second]
        };
        let ring_pair = [first.ring, second.ring];
        let segments = [first.segment, second.segment];
        let point = exact_chart_point(chart, source.point)?;
        contacts.push(ExactCycleContact {
            segments,
            point: point.clone(),
        });
        let key = (ring_pair, PointKey::new(source.point));
        let touch = match touch_index.entry(key) {
            std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let index = pair_touches.len();
                pair_touches.push(ExactPairTouch {
                    rings: ring_pair,
                    point,
                    directions: [Vec::new(), Vec::new()],
                });
                entry.insert(index);
                index
            },
        };
        let pair_touch = pair_touches.get_mut(touch).ok_or_else(exact_cycle_error)?;
        let [first_directions, second_directions] = &mut pair_touch.directions;
        for (edge, directions) in [(first, first_directions), (second, second_directions)] {
            let cycle = rings.get(edge.ring).ok_or_else(exact_cycle_error)?;
            let start = cycle.get(edge.edge).ok_or_else(exact_cycle_error)?;
            let next_edge = edge
                .edge
                .checked_add(1)
                .filter(|&next| next < cycle.len())
                .unwrap_or(0);
            let end = cycle.get(next_edge).ok_or_else(exact_cycle_error)?;
            for (index, direction) in [start, end].into_iter().enumerate() {
                if edge.endpoint == Some(index) {
                    continue;
                }
                directions.push(direction.clone());
            }
        }
    }
    for touch in &mut pair_touches {
        for directions in &mut touch.directions {
            try_sort_by(directions, exact::ExactPoint::compare_lex)?;
            try_dedup_by(directions, exact::ExactPoint::same_position)?;
        }
    }
    try_sort_by(&mut contacts, |left, right| {
        Ok(match left.segments.cmp(&right.segments) {
            Ordering::Equal => left.point.compare_lex(&right.point)?,
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
        })
    })?;
    try_dedup_by(&mut contacts, |later, earlier| {
        Ok(later.segments == earlier.segments && later.point.same_position(&earlier.point)?)
    })?;
    Ok((contacts, pair_touches))
}

fn map_source_polygon_edge(
    source_ring: usize,
    source_edge: usize,
    source_parameter: &SegmentParameter,
    source_point: XY,
    source_edges: &[Vec<Option<usize>>],
    source_rings: &[Option<NormalizedSourceRing>],
    rings: &[Vec<exact::ExactPoint>],
    fixed: &[Vec<XY>],
) -> Result<MappedSourceEdge> {
    let normalization = source_rings
        .get(source_ring)
        .copied()
        .flatten()
        .ok_or_else(exact_cycle_error)?;
    let edge = source_edges
        .get(source_ring)
        .and_then(|edges| edges.get(source_edge))
        .copied()
        .flatten()
        .ok_or_else(exact_cycle_error)?;
    let parameter = exact_source_parameter(source_parameter)?;
    let parameter = if normalization.reversed {
        exact::ExactRational::one().try_subtract(&parameter)?
    } else {
        parameter
    };
    let normalized_ring = rings
        .get(normalization.ring)
        .ok_or_else(exact_cycle_error)?;
    let normalized_coordinates = fixed
        .get(normalization.ring)
        .ok_or_else(exact_cycle_error)?;
    if normalized_ring.len() != normalized_coordinates.len() || normalized_ring.is_empty() {
        return Err(exact_cycle_error());
    }
    let start = *normalized_coordinates
        .get(edge)
        .ok_or_else(exact_cycle_error)?;
    let next_edge = edge
        .checked_add(1)
        .filter(|&next| next < normalized_coordinates.len())
        .unwrap_or(0);
    let end = *normalized_coordinates
        .get(next_edge)
        .ok_or_else(exact_cycle_error)?;
    let rounded = parameter.round_segment(start, end)?;
    if !crate::geometry::same_point(rounded, source_point) {
        return Err(exact_cycle_error());
    }
    let endpoint = if parameter.is_zero() {
        Some(0)
    } else if parameter.is_one() {
        Some(1)
    } else {
        None
    };
    let segment = normalization
        .segment_offset
        .checked_add(edge)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    Ok(MappedSourceEdge {
        ring: normalization.ring,
        edge,
        segment,
        endpoint,
    })
}

fn exact_source_parameter(parameter: &SegmentParameter) -> Result<exact::ExactRational> {
    match parameter {
        SegmentParameter::Start => Ok(exact::ExactRational::zero()),
        SegmentParameter::End => Ok(exact::ExactRational::one()),
        SegmentParameter::Ratio { num, den } => exact::ExactRational::new(num.clone(), den.clone()),
    }
}

pub(in crate::geometry::tessellation::voronoi_dcel) fn exact_segment_parameter(
    start: &exact::ExactPoint,
    end: &exact::ExactPoint,
    point: &exact::ExactPoint,
) -> Result<exact::ExactRational> {
    let one = exact::ExactDyadic::one();
    let zero = exact::ExactDyadic::zero();
    let x = |point: &exact::ExactPoint| point.projected_coordinate(&one, &zero);
    let y = |point: &exact::ExactPoint| point.projected_coordinate(&zero, &one);
    let start_x = x(start)?;
    let end_x = x(end)?;
    let point_x = x(point)?;
    let denominator_x = end_x.try_subtract(&start_x)?;
    if !denominator_x.is_zero() {
        return point_x.try_subtract(&start_x)?.try_quotient(&denominator_x);
    }
    let start_y = y(start)?;
    let end_y = y(end)?;
    let point_y = y(point)?;
    let denominator_y = end_y.try_subtract(&start_y)?;
    if denominator_y.is_zero() {
        return Err(clip_source_trace_error());
    }
    point_y.try_subtract(&start_y)?.try_quotient(&denominator_y)
}

fn polygon_from_open_xy_rings(rings: &[Vec<XY>]) -> Result<Polygon> {
    let mut rings = rings.iter();
    let make_ring = |open: &[XY]| -> Result<Ring> {
        let first = *open.first().ok_or_else(exact_cycle_error)?;
        let mut closed = Vec::with_capacity(open.len().saturating_add(1));
        closed.extend_from_slice(open);
        closed.push(first);
        Ring::try_from_closed(CoordSeq::try_from_xy(&closed)?)
    };
    let shell = make_ring(rings.next().ok_or_else(exact_cycle_error)?)?;
    let holes = rings
        .map(|ring| make_ring(ring))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(Polygon::new(shell, holes))
}

#[cfg(test)]
pub(in crate::geometry::tessellation::voronoi_dcel) fn normalize_clip<'a>(
    sites: &[Site],
    boundary: VoronoiBoundary<'a>,
    polygon_output: bool,
) -> Result<NormalizedClip<'a>> {
    normalize_clip_with_chart(sites, boundary, polygon_output, None)
}

pub(in crate::geometry::tessellation::voronoi_dcel) fn normalize_clip_with_chart<'a>(
    sites: &[Site],
    boundary: VoronoiBoundary<'a>,
    polygon_output: bool,
    chart: Option<&PointSelectionChart>,
) -> Result<NormalizedClip<'a>> {
    let mut rings = Vec::new();
    let mut fixed = Vec::new();
    let mut chart_fixed = Vec::new();
    let mut source_roots;
    let mut contacts = Vec::new();
    let mut polygon_baseline = None;
    let source = match boundary {
        VoronoiBoundary::Polygon(polygon) => {
            let mut polygon_contacts = Vec::new();
            if polygon
                .validate_with_contacts("polygon", "$", &mut polygon_contacts)?
                .is_some()
            {
                return Err(voronoi_error("Voronoi clip polygon is invalid"));
            }
            polygon_baseline = polygon_output
                .then(|| polygon_output_baseline(polygon))
                .transpose()?;
            let clip_ring_count = polygon
                .holes
                .len()
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            let source_point_rings: Vec<Vec<Point>> = std::iter::once(&polygon.shell)
                .chain(polygon.holes.iter())
                .map(|ring| ring.iter().collect())
                .collect();
            let mut clip_rings = Vec::with_capacity(clip_ring_count);
            for (ring_index, ring) in std::iter::once(&polygon.shell)
                .chain(polygon.holes.iter())
                .enumerate()
            {
                let (mut points, mut coordinates, mut chart_coordinates, mut source_edges) =
                    open_ring(ring, chart)?;
                let orientation = exact::incidence::cycle_orientation(&points)?;
                let wanted = if ring_index == 0 {
                    ExactSign::Positive
                } else {
                    ExactSign::Negative
                };
                let reversed = orientation != wanted;
                if reversed {
                    reverse_ring(&mut points, &mut coordinates, &mut chart_coordinates);
                }
                let rotation = rotate_ring(&mut points, &mut coordinates, &mut chart_coordinates)?;
                let count = points.len();
                for edge in source_edges.iter_mut().flatten() {
                    *edge = normalized_source_edge(*edge, count, rotation, reversed);
                }
                clip_rings.push((
                    ring_index,
                    points,
                    coordinates,
                    chart_coordinates,
                    source_edges,
                    reversed,
                ));
            }
            let (_, holes) = clip_rings
                .split_first_mut()
                .expect("the normalized clip ring sequence begins with its shell");
            try_sort_by(holes, |(_, left, ..), (_, right, ..)| {
                compare_exact_sequences(left, right)
            })?;
            rings.reserve(clip_rings.len());
            fixed.reserve(clip_rings.len());
            chart_fixed.reserve(clip_rings.len());
            let mut source_edge_map = vec![Vec::new(); clip_rings.len()];
            let mut source_ring_map = vec![None; clip_rings.len()];
            let mut segment_offset = 0_usize;
            for (source_ring, points, coordinates, chart_coordinates, edges, reversed) in clip_rings
            {
                let normalized_ring = rings.len();
                *source_edge_map
                    .get_mut(source_ring)
                    .ok_or_else(exact_cycle_error)? = edges;
                *source_ring_map
                    .get_mut(source_ring)
                    .ok_or_else(exact_cycle_error)? = Some(NormalizedSourceRing {
                    ring: normalized_ring,
                    segment_offset,
                    reversed,
                });
                segment_offset = segment_offset
                    .checked_add(points.len())
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
                rings.push(points);
                fixed.push(coordinates);
                chart_fixed.push(chart_coordinates);
            }
            source_roots = vec![Vec::new(); segment_offset];
            for (source_ring, edges) in source_edge_map.iter().enumerate() {
                let normalization = source_ring_map
                    .get(source_ring)
                    .copied()
                    .flatten()
                    .ok_or_else(exact_cycle_error)?;
                let source_points = source_point_rings
                    .get(source_ring)
                    .ok_or_else(exact_cycle_error)?;
                for (source_edge, &normalized_edge) in edges.iter().enumerate() {
                    let Some(normalized_edge) = normalized_edge else {
                        continue;
                    };
                    let source_end = source_edge
                        .checked_add(1)
                        .ok_or(crate::error::ResourceError::SizeOverflow)?;
                    let start = *source_points
                        .get(source_edge)
                        .ok_or_else(exact_cycle_error)?;
                    let end = *source_points
                        .get(source_end)
                        .ok_or_else(exact_cycle_error)?;
                    let segment = normalization
                        .segment_offset
                        .checked_add(normalized_edge)
                        .ok_or(crate::error::ResourceError::SizeOverflow)?;
                    source_roots
                        .get_mut(segment)
                        .ok_or_else(exact_cycle_error)?
                        .push(VoronoiClipSourceRoot {
                            ring: source_ring,
                            edge: source_edge,
                            reversed: normalization.reversed,
                            start,
                            end,
                        });
                }
            }
            let (source_cycle_contacts, pair_touches) = map_source_polygon_contacts(
                &polygon_contacts,
                &source_edge_map,
                &source_ring_map,
                &rings,
                &fixed,
                chart,
            )?;
            let mut open = Vec::with_capacity(rings.len());
            open.extend(rings.iter().map(Vec::as_slice));
            validate_exact_contact_graph(&open, 0, &pair_touches)?;
            contacts = source_cycle_contacts;
            Some((polygon, polygon_from_open_xy_rings(&chart_fixed)?))
        },
        VoronoiBoundary::Envelope | VoronoiBoundary::Padded => {
            let points: Vec<_> = sites.iter().map(|site| &site.exact_point).collect();
            let rectangle = exact::frame::rectangular_boundary_for_exact(
                &points,
                matches!(boundary, VoronoiBoundary::Padded),
            )?;
            let mut corners = Vec::with_capacity(rectangle.len());
            for corner in &rectangle {
                corners.push(corner.round_nearest_even()?);
            }
            validate_exact_rectangle(&rectangle)?;
            let mut ring = Vec::with_capacity(rectangle.len());
            ring.extend(rectangle);
            rings.reserve(1);
            fixed.reserve(1);
            chart_fixed.reserve(1);
            source_roots = vec![Vec::new(); ring.len()];
            rings.push(ring);
            fixed.push(corners.clone());
            chart_fixed.push(corners);
            None
        },
    };
    let (source, geometry) = source.map_or((None, None), |(source, geometry)| {
        (Some(source), Some(geometry))
    });
    Ok(NormalizedClip {
        rings,
        fixed,
        chart_fixed,
        source_roots,
        source,
        geometry,
        polygon_baseline,
        contacts,
    })
}

fn compare_exact_sequences(
    left: &[exact::ExactPoint],
    right: &[exact::ExactPoint],
) -> Result<Ordering> {
    for (left, right) in left.iter().zip(right) {
        let order = left.compare_lex(right)?;
        if order.is_ne() {
            return Ok(order);
        }
    }
    Ok(left.len().cmp(&right.len()))
}

fn point_on_segment_xy(point: XY, start: XY, end: XY) -> bool {
    point.x >= start.x.min(end.x)
        && point.x <= start.x.max(end.x)
        && point.y >= start.y.min(end.y)
        && point.y <= start.y.max(end.y)
        && exact::orient2d(start, end, point) == ExactSign::Zero
}

fn exact_point_covered_by_clip_index<A: SearchAccount>(
    point: &exact::ExactPoint,
    edges: &[ClipCertificateEdge],
    index: &YStabbingIndex<usize>,
    parity: &mut ClipRingParity,
    account: &mut A,
) -> Result<bool> {
    account.charge_evaluations(1)?;
    let query_y = point.round_nearest_even()?.y;
    parity.begin_query();
    let mut boundary = false;
    let mut failure: Result<()> = Ok(());
    let _ = index.for_each_at_y(query_y, |&edge_index| {
        if boundary || failure.is_err() {
            return ControlFlow::Break(());
        }
        let Some(edge) = edges.get(edge_index) else {
            failure = Err(exact_cycle_error());
            return ControlFlow::Break(());
        };
        failure = (|| {
            account.charge_evaluations(1)?;
            let (start_y, end_y) = (edge.start.compare_y(point)?, edge.end.compare_y(point)?);
            let (low_y, high_y) = if edge.start.compare_y(&edge.end)?.is_le() {
                (&edge.start, &edge.end)
            } else {
                (&edge.end, &edge.start)
            };
            if point.compare_y(low_y)?.is_lt() || point.compare_y(high_y)?.is_gt() {
                return Ok(());
            }
            let (low_x, high_x) = if edge.start.compare_x(&edge.end)?.is_le() {
                (&edge.start, &edge.end)
            } else {
                (&edge.end, &edge.start)
            };
            let in_x_range = point.compare_x(low_x)?.is_ge() && point.compare_x(high_x)?.is_le();
            let crosses_ray = start_y.is_gt() != end_y.is_gt();
            if !in_x_range && !crosses_ray {
                return Ok(());
            }
            let orientation = exact::incidence::orient_points(&edge.start, &edge.end, point)?;
            if in_x_range && orientation == ExactSign::Zero {
                boundary = true;
                return Ok(());
            }
            if crosses_ray
                && (orientation == ExactSign::Positive) == edge.end.compare_y(&edge.start)?.is_gt()
            {
                parity.toggle(edge.ring)?;
            }
            Ok(())
        })();
        if failure.is_err() || boundary {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    });
    failure?;
    Ok(boundary || parity.covers())
}

pub(in crate::geometry::tessellation::voronoi_dcel) fn rounded_segment_covered_by_clip<
    A: SearchAccount,
>(
    segment: Segment,
    clip_edges: &[ClipCertificateEdge],
    candidates: &[usize],
    point_index: &PolygonPointIndex,
    y_index: &YStabbingIndex<usize>,
    events: &mut Vec<exact::ExactPoint>,
    parity: &mut ClipRingParity,
    account: &mut A,
) -> Result<Option<ClipBoundaryFailure>> {
    let mut interior_contact = false;
    let mut start_on_boundary = false;
    let mut end_on_boundary = false;
    for &edge_index in candidates {
        account.charge_evaluations(1)?;
        let edge = clip_edges.get(edge_index).ok_or_else(exact_cycle_error)?;
        let [start, end] = edge.rounded;
        start_on_boundary |= point_on_segment_xy(segment.start, start, end);
        end_on_boundary |= point_on_segment_xy(segment.end, start, end);
        let segment_start = exact::orient2d(segment.start, segment.end, start);
        let segment_end = exact::orient2d(segment.start, segment.end, end);
        let edge_start = exact::orient2d(start, end, segment.start);
        let edge_end = exact::orient2d(start, end, segment.end);
        if segment_start != ExactSign::Zero
            && segment_end != ExactSign::Zero
            && edge_start != ExactSign::Zero
            && edge_end != ExactSign::Zero
            && segment_start != segment_end
            && edge_start != edge_end
        {
            return Ok(Some(ClipBoundaryFailure {
                ring: edge.ring,
                atom: Some(edge.atom),
            }));
        }
        if (point_on_segment_xy(start, segment.start, segment.end)
            && start != segment.start
            && start != segment.end)
            || (point_on_segment_xy(end, segment.start, segment.end)
                && end != segment.start
                && end != segment.end)
        {
            interior_contact = true;
        }
    }
    for (point, on_boundary) in [
        (segment.start, start_on_boundary),
        (segment.end, end_on_boundary),
    ] {
        if !on_boundary {
            account.charge_evaluations(1)?;
            if !point_index.covers_point(Point::new_raw_xy(point.x, point.y)) {
                let point = exact::ExactPoint::from_xy(point);
                if exact_point_covered_by_clip_index(&point, clip_edges, y_index, parity, account)?
                {
                    return Err(voronoi_error(
                        "assigned clip point index disagrees with exact ring membership",
                    ));
                }
                return Ok(Some(ClipBoundaryFailure {
                    ring: parity.outside_ring().ok_or_else(clip_source_trace_error)?,
                    atom: None,
                }));
            }
        }
    }
    let start = exact::ExactPoint::from_xy(segment.start);
    let end = exact::ExactPoint::from_xy(segment.end);
    if !interior_contact {
        let midpoint = start.midpoint(&end)?;
        if exact_point_covered_by_clip_index(&midpoint, clip_edges, y_index, parity, account)? {
            return Ok(None);
        }
        return Ok(Some(ClipBoundaryFailure {
            ring: parity.outside_ring().ok_or_else(clip_source_trace_error)?,
            atom: None,
        }));
    }
    events.clear();
    events.reserve(2);
    events.extend([start.clone(), end.clone()]);
    for &edge_index in candidates {
        account.charge_evaluations(1)?;
        let edge = clip_edges.get(edge_index).ok_or_else(exact_cycle_error)?;
        match exact::incidence::segment_intersection(&start, &end, &edge.start, &edge.end)? {
            exact::SegmentIntersection::None => {},
            exact::SegmentIntersection::Point(point) => events.push(point),
            exact::SegmentIntersection::Overlap { start, end } => {
                events.reserve(2);
                events.extend([start, end]);
            },
        }
    }
    sort_unique_along(&start, &end, events)?;
    for [left, right] in events.array_windows::<2>() {
        if exact::incidence::compare_along(&start, &end, left, right)? != Ordering::Less {
            continue;
        }
        let midpoint = left.midpoint(right)?;
        if !exact_point_covered_by_clip_index(&midpoint, clip_edges, y_index, parity, account)? {
            return Ok(Some(ClipBoundaryFailure {
                ring: parity.outside_ring().ok_or_else(clip_source_trace_error)?,
                atom: None,
            }));
        }
    }
    Ok(None)
}

pub(in crate::geometry::tessellation::voronoi_dcel) fn clip_certificate_data<A: SearchAccount>(
    subdivision: &ExactSubdivision,
    clip: &NormalizedClip<'_>,
    trace: &ClipTrace,
    xy_of: impl Fn(VertexId) -> Result<XY>,
    account: &mut A,
) -> Result<ClipCertificate> {
    if trace.rings.len() != clip.rings.len() {
        return Err(clip_source_trace_error());
    }
    account.charge_evaluations(
        u64::try_from(trace.atom_count.div_ceil(16))
            .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
    )?;

    let mut edges = Vec::with_capacity(trace.atom_count);
    let mut output_rings = Vec::with_capacity(trace.rings.len());
    for (ring_index, (source_ring, trace_ring)) in clip.rings.iter().zip(&trace.rings).enumerate() {
        if source_ring.len() < 3 || trace_ring.atoms.len() < source_ring.len() {
            return Err(clip_source_trace_error());
        }
        let first_atom = trace_ring
            .atoms
            .first()
            .ok_or_else(clip_source_trace_error)?;
        let [first_left, first_right] = subdivision.edge(first_atom.edge).endpoints;
        let first_vertex = if first_atom.reversed {
            first_right
        } else {
            first_left
        };
        let mut previous_end = None;
        let mut rounded_ring = Vec::with_capacity(trace_ring.atoms.len());
        let mut exact_ring = Vec::with_capacity(trace_ring.atoms.len());
        let mut output_points = Vec::with_capacity(trace_ring.atoms.len());
        for atom in &trace_ring.atoms {
            let [left, right] = subdivision.edge(atom.edge).endpoints;
            let (start, end) = if atom.reversed {
                (right, left)
            } else {
                (left, right)
            };
            if previous_end.is_some_and(|previous| previous != start) {
                return Err(clip_source_trace_error());
            }
            let rounded = xy_of(start)?;
            rounded_ring.push(rounded);
            exact_ring.push(exact::ExactPoint::from_xy(rounded));
            output_points.push(Point::new_raw_xy(rounded.x, rounded.y));
            previous_end = Some(end);
        }
        if previous_end != Some(first_vertex) {
            return Err(clip_source_trace_error());
        }
        let atom_pairs = trace_ring
            .atoms
            .iter()
            .zip(exact_ring.iter().zip(exact_ring.iter().cycle().skip(1)))
            .zip(rounded_ring.iter().zip(rounded_ring.iter().cycle().skip(1)));
        for ((atom, (start, end)), (rounded_start, rounded_end)) in atom_pairs {
            edges.push(ClipCertificateEdge {
                ring: ring_index,
                atom: atom.edge,
                start: start.clone(),
                end: end.clone(),
                rounded: [*rounded_start, *rounded_end],
            });
        }
        ensure_coordseq_vertex_capacity(
            output_points
                .len()
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?,
        )?;
        output_rings.push(Ring::closed(output_points)?);
    }
    let mut output_rings = output_rings.into_iter();
    let shell = output_rings.next().ok_or_else(clip_source_trace_error)?;
    Ok(ClipCertificate {
        edges,
        polygon: Polygon::new(shell, output_rings.collect()),
    })
}
