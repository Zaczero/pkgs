//! Clipping separators and rays to the normalised frame.
//!
//! Every bisector reaches the frame or leaves through it; these routines
//! answer where, in exact arithmetic, so the scaffold below never invents a
//! vertex the clip did not produce.

use super::{
    ExactSign, FrameSide, NormalizedClip, Ordering, Result, Site, SiteId, XY, exact, lex_xy,
    site_at, site_by_id, voronoi_error,
};
use crate::geometry::segments::ordering::{try_dedup_by, try_sort_by};

pub(super) fn orient_separator(
    mut start: exact::ExactPoint,
    mut end: exact::ExactPoint,
    sites: [SiteId; 2],
    input: &[Site],
) -> Result<(exact::ExactPoint, exact::ExactPoint)> {
    let [left_site, _] = sites;
    match exact::incidence::orient_points(&start, &end, &site_by_id(input, left_site).exact_point)?
    {
        ExactSign::Positive => {},
        ExactSign::Negative => std::mem::swap(&mut start, &mut end),
        ExactSign::Zero => {
            return Err(voronoi_error("Voronoi separator contains its source site"));
        },
    }
    Ok((start, end))
}

/// The frame hits of the perpendicular bisector of two sites: certified lazy
/// points on a frame of stored doubles, the exact construction otherwise.
pub(super) fn bisector_frame_intersections(
    sites: [XY; 2],
    frame: &[exact::ExactPoint; 4],
) -> Result<Vec<(FrameSide, exact::ExactPoint)>> {
    let [left, right] = sites;
    if let [Some(a), Some(b), Some(c), Some(d)] = frame.each_ref().map(exact::ExactPoint::as_double)
    {
        let corners = [a, b, c, d];
        let mut hits = Vec::new();
        let mut certified = true;
        for (side, ((&start, &end), (frame_start, frame_end))) in FrameSide::ALL.into_iter().zip(
            corners
                .iter()
                .zip(corners.iter().cycle().skip(1))
                .zip(frame.iter().zip(frame.iter().cycle().skip(1))),
        ) {
            match exact::ExactPoint::certified_bisector_hit([left, right], [start, end]) {
                exact::FrameHit::Declined => {
                    certified = false;
                    break;
                },
                exact::FrameHit::Parallel => {},
                exact::FrameHit::Point(hit) => {
                    if between(&hit, frame_start, frame_end)? {
                        hits.push((side, hit));
                    }
                },
            }
        }
        if certified {
            // Adjacent for the corner dedup below; the order itself carries
            // nothing, since a bisector meets a convex frame at most twice and
            // the caller keeps the one on its side. Rounded keys keep this off
            // the exact comparator.
            try_sort_by(&mut hits, |left, right| {
                Ok(
                    lex_xy(left.1.round_nearest_even()?, right.1.round_nearest_even()?)
                        .then(left.0.cmp(&right.0)),
                )
            })?;
            try_dedup_by(&mut hits, |later, earlier| {
                later.1.same_position(&earlier.1)
            })?;
            return Ok(hits);
        }
    }
    frame_intersections(
        &exact::ExactLine::perpendicular_bisector(left, right)?,
        frame,
    )
}

/// Whether `point` lies in the closed box spanned by `low` and `high`.
pub(super) fn between(
    point: &exact::ExactPoint,
    low: &exact::ExactPoint,
    high: &exact::ExactPoint,
) -> Result<bool> {
    let within = |low: Ordering, high: Ordering| {
        (low != Ordering::Less && high != Ordering::Greater)
            || (high != Ordering::Less && low != Ordering::Greater)
    };
    Ok(within(point.compare_x(low)?, point.compare_x(high)?)
        && within(point.compare_y(low)?, point.compare_y(high)?))
}

pub(super) fn frame_intersections(
    line: &exact::ExactLine,
    frame: &[exact::ExactPoint; 4],
) -> Result<Vec<(FrameSide, exact::ExactPoint)>> {
    let mut hits = Vec::new();
    for (side, (start, end)) in FrameSide::ALL
        .into_iter()
        .zip(frame.iter().zip(frame.iter().cycle().skip(1)))
    {
        let edge = exact::ExactLine::through_points(start, end)?;
        let hit = exact::incidence::line_intersection(line, &edge)?;
        if exact::incidence::signed_line_product(line, &hit)? == ExactSign::Zero
            && exact::incidence::point_in_cycle(frame, &hit)? != exact::PointInCycle::Outside
            && exact::incidence::segment_intersection(start, end, &hit, &hit)?.is_point()
        {
            hits.push((side, hit));
        }
    }
    try_sort_by(&mut hits, |left, right| {
        Ok(left.1.compare_lex(&right.1)?.then(left.0.cmp(&right.0)))
    })?;
    try_dedup_by(&mut hits, |later, earlier| {
        later.1.same_position(&earlier.1)
    })?;
    Ok(hits)
}

pub(super) trait SegmentIntersectionExt {
    fn is_point(&self) -> bool;
}

impl SegmentIntersectionExt for exact::SegmentIntersection {
    fn is_point(&self) -> bool {
        matches!(self, Self::Point(_))
    }
}

pub(super) fn clip_rectangle(clip: &NormalizedClip<'_>) -> Result<[exact::ExactPoint; 4]> {
    if let [ring] = clip.rings.as_slice()
        && let [a, b, c, d] = ring.as_slice()
    {
        let rect = [a.clone(), b.clone(), c.clone(), d.clone()];
        if axis_aligned_rectangle(&rect)? {
            return Ok(rect);
        }
    }
    exact::frame::axis_aligned_bounds(clip.rings.iter().flatten())
}

/// Whether the four corners form a counter-clockwise axis-aligned rectangle
/// of positive extent, starting at its lower-left corner.
pub(super) fn axis_aligned_rectangle(rect: &[exact::ExactPoint; 4]) -> Result<bool> {
    let [bottom_left, bottom_right, top_right, top_left] = rect;
    Ok(bottom_left.compare_y(bottom_right)?.is_eq()
        && top_right.compare_y(top_left)?.is_eq()
        && bottom_right.compare_x(top_right)?.is_eq()
        && top_left.compare_x(bottom_left)?.is_eq()
        && bottom_left.compare_x(bottom_right)?.is_lt()
        && bottom_right.compare_y(top_right)?.is_lt())
}

pub(super) fn in_rectangle(
    rect: &[exact::ExactPoint; 4],
    point: &exact::ExactPoint,
) -> Result<bool> {
    Ok(exact::incidence::point_in_cycle(rect, point)? != exact::PointInCycle::Outside)
}

/// Sort points along `origin -> hint` and drop repeated positions.
pub(super) fn sort_unique_along(
    origin: &exact::ExactPoint,
    hint: &exact::ExactPoint,
    points: &mut Vec<exact::ExactPoint>,
) -> Result<()> {
    try_sort_by(points, |left, right| {
        exact::incidence::compare_along(origin, hint, left, right)
    })?;
    try_dedup_by(points, exact::ExactPoint::same_position)
}

pub(super) fn span_from_points(
    origin: &exact::ExactPoint,
    hint: &exact::ExactPoint,
    mut points: Vec<exact::ExactPoint>,
) -> Result<Option<(exact::ExactPoint, exact::ExactPoint)>> {
    sort_unique_along(origin, hint, &mut points)?;
    Ok(match points.as_slice() {
        [first, .., last] => Some((first.clone(), last.clone())),
        _ => None,
    })
}

pub(super) fn separator_rectangle_hits(
    pair: [&exact::ExactPoint; 2],
    line: &exact::ExactLine,
    rect: &[exact::ExactPoint; 4],
) -> Result<Vec<exact::ExactPoint>> {
    let certified = match pair.map(exact::ExactPoint::as_double) {
        [Some(left), Some(right)] => bisector_frame_intersections([left, right], rect)?,
        _ => exact_bisector_frame_intersections(pair, line, rect)?,
    };
    let mut hits = Vec::with_capacity(certified.len().saturating_add(8));
    hits.extend(certified.into_iter().map(|(_, point)| point));
    for (start, end) in rect.iter().zip(rect.iter().cycle().skip(1)) {
        if exact::incidence::signed_line_product(line, start)? == ExactSign::Zero
            && exact::incidence::signed_line_product(line, end)? == ExactSign::Zero
        {
            hits.push(start.clone());
            hits.push(end.clone());
        }
    }
    Ok(hits)
}

pub(super) fn exact_bisector_frame_intersections(
    _pair: [&exact::ExactPoint; 2],
    line: &exact::ExactLine,
    rect: &[exact::ExactPoint; 4],
) -> Result<Vec<(FrameSide, exact::ExactPoint)>> {
    let mut hits = Vec::new();
    for (side, (start, end)) in FrameSide::ALL
        .into_iter()
        .zip(rect.iter().zip(rect.iter().cycle().skip(1)))
    {
        let start_sign = exact::incidence::signed_line_product(line, start)?;
        let end_sign = exact::incidence::signed_line_product(line, end)?;
        let hit = match (start_sign, end_sign) {
            (ExactSign::Zero, ExactSign::Zero) => {
                hits.push((side, start.clone()));
                Some(end.clone())
            },
            (ExactSign::Zero, _) => Some(start.clone()),
            (_, ExactSign::Zero) => Some(end.clone()),
            (left, right) if left != right => {
                let support = exact::ExactLine::through_points(start, end)?;
                Some(exact::incidence::line_intersection(line, &support)?)
            },
            _ => None,
        };
        if let Some(hit) = hit {
            hits.push((side, hit));
        }
    }
    Ok(hits)
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "fallible segment filtering uses swap compaction: kept never exceeds the current index, and both remain below the original points length"
)]
pub(super) fn clip_segment_to_rectangle(
    start: &exact::ExactPoint,
    end: &exact::ExactPoint,
    pair: [&exact::ExactPoint; 2],
    line: &exact::ExactLine,
    rect: &[exact::ExactPoint; 4],
) -> Result<Option<(exact::ExactPoint, exact::ExactPoint)>> {
    let mut points = separator_rectangle_hits(pair, line, rect)?;
    points.reserve(2);
    if in_rectangle(rect, start)? {
        points.push(start.clone());
    }
    if in_rectangle(rect, end)? {
        points.push(end.clone());
    }
    let mut kept = 0;
    for index in 0..points.len() {
        if exact::incidence::segment_intersection(start, end, &points[index], &points[index])?
            .is_point()
        {
            points.swap(kept, index);
            kept += 1;
        }
    }
    points.truncate(kept);
    span_from_points(start, end, points)
}

pub(super) fn hull_point_on_ray(
    center: &exact::ExactPoint,
    mid: &exact::ExactPoint,
    sites: [&exact::ExactPoint; 2],
    opposite_sign: ExactSign,
    point: &exact::ExactPoint,
) -> Result<bool> {
    if point.same_position(center)? {
        return Ok(true);
    }
    let [left, right] = sites;
    if center.same_position(mid)? {
        return Ok(exact::incidence::orient_points(left, right, point)? != opposite_sign);
    }
    Ok(
        if exact::incidence::orient_points(left, right, center)? == opposite_sign {
            exact::incidence::compare_along(center, mid, center, point)? != Ordering::Greater
        } else {
            exact::incidence::compare_along(mid, center, center, point)? != Ordering::Greater
        },
    )
}

pub(super) fn clip_hull_ray_to_rectangle(
    center: &exact::ExactPoint,
    pair: [usize; 2],
    opposite: usize,
    line: &exact::ExactLine,
    rect: &[exact::ExactPoint; 4],
    sites: &[Site],
) -> Result<Option<(exact::ExactPoint, exact::ExactPoint)>> {
    let [left_site, right_site] = pair;
    let left = &site_at(sites, left_site).exact_point;
    let right = &site_at(sites, right_site).exact_point;
    let hits = separator_rectangle_hits([left, right], line, rect)?;
    let mid = left.midpoint(right)?;
    let opposite_sign =
        exact::incidence::orient_points(left, right, &site_at(sites, opposite).exact_point)?;
    let mut points = Vec::with_capacity(hits.len().saturating_add(1));
    if in_rectangle(rect, center)? {
        points.push(center.clone());
    }
    for hit in hits {
        if hull_point_on_ray(center, &mid, [left, right], opposite_sign, &hit)? {
            points.push(hit);
        }
    }
    if points.len() < 2 {
        return Ok(None);
    }
    let (origin, hint) = if center.same_position(&mid)? {
        let mut hint = None;
        for point in &points {
            if !point.same_position(center)? {
                hint = Some(point.clone());
                break;
            }
        }
        let Some(hint) = hint else {
            return Ok(None);
        };
        (center.clone(), hint)
    } else if exact::incidence::orient_points(left, right, center)? == opposite_sign {
        (center.clone(), mid)
    } else {
        (mid, center.clone())
    };
    span_from_points(&origin, &hint, points)
}
