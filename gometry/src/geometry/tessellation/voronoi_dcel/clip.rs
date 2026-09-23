//! Clipping separators and rays to the normalised frame.
//!
//! Every bisector reaches the frame or leaves through it; these routines
//! answer where, in exact arithmetic, so the scaffold below never invents a
//! vertex the clip did not produce.

use super::{
    ExactSign, ExpansionBudget, NormalizedClip, Ordering, Result, Site, SiteId, XY, exact, lex_xy,
    try_dedup_by, try_sort_by, voronoi_error,
};

pub(super) fn orient_separator(
    mut start: exact::ExactPoint,
    mut end: exact::ExactPoint,
    sites: [SiteId; 2],
    input: &[Site],
) -> Result<(exact::ExactPoint, exact::ExactPoint)> {
    match exact::orient_points(
        &start,
        &end,
        &exact::ExactPoint::from_xy(input[sites[0].0 as usize].point.xy()),
    )? {
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
) -> Result<Vec<(u8, exact::ExactPoint)>> {
    if let [Some(a), Some(b), Some(c), Some(d)] = frame.each_ref().map(exact::ExactPoint::as_double)
    {
        let corners = [a, b, c, d];
        let mut hits = Vec::new();
        let mut certified = true;
        for side in 0..4 {
            let ends = [corners[side], corners[(side + 1) % 4]];
            match exact::ExactPoint::certified_bisector_hit(sites, ends) {
                exact::FrameHit::Declined => {
                    certified = false;
                    break;
                },
                exact::FrameHit::Parallel => {},
                exact::FrameHit::Point(hit) => {
                    if between(&hit, &frame[side], &frame[(side + 1) % 4])? {
                        hits.push((side as u8, hit));
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
        &exact::ExactLine::perpendicular_bisector(sites[0], sites[1])?,
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
) -> Result<Vec<(u8, exact::ExactPoint)>> {
    let mut hits = Vec::new();
    for side in 0..4 {
        let edge = exact::ExactLine::through_points(&frame[side], &frame[(side + 1) % 4])?;
        let hit = exact::line_intersection(line, &edge)?;
        if exact::signed_line_product(line, &hit)? == ExactSign::Zero
            && exact::point_in_cycle(frame, &hit)? != exact::PointInCycle::Outside
            && exact::segment_intersection(&frame[side], &frame[(side + 1) % 4], &hit, &hit)?
                .is_point()
        {
            hits.push((side as u8, hit));
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
    exact::axis_aligned_bounds(clip.rings.iter().flatten())
}

/// Whether the four corners form a counter-clockwise axis-aligned rectangle
/// of positive extent, starting at its lower-left corner.
pub(super) fn axis_aligned_rectangle(rect: &[exact::ExactPoint; 4]) -> Result<bool> {
    Ok(rect[0].compare_y(&rect[1])?.is_eq()
        && rect[2].compare_y(&rect[3])?.is_eq()
        && rect[1].compare_x(&rect[2])?.is_eq()
        && rect[3].compare_x(&rect[0])?.is_eq()
        && rect[0].compare_x(&rect[1])?.is_lt()
        && rect[1].compare_y(&rect[2])?.is_lt())
}

pub(super) fn in_rectangle(
    rect: &[exact::ExactPoint; 4],
    point: &exact::ExactPoint,
) -> Result<bool> {
    Ok(exact::point_in_cycle(rect, point)? != exact::PointInCycle::Outside)
}

/// Sort points along `origin -> hint` and drop repeated positions.
pub(super) fn sort_unique_along(
    origin: &exact::ExactPoint,
    hint: &exact::ExactPoint,
    points: &mut Vec<exact::ExactPoint>,
) -> Result<()> {
    try_sort_by(points, |left, right| {
        exact::compare_along(origin, hint, left, right)
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
    pair: [XY; 2],
    line: &exact::ExactLine,
    rect: &[exact::ExactPoint; 4],
    budget: &mut ExpansionBudget,
) -> Result<Vec<exact::ExactPoint>> {
    let certified = bisector_frame_intersections(pair, rect)?;
    budget.add(certified.len().saturating_add(8))?;
    let mut hits = Vec::with_capacity(certified.len().saturating_add(8));
    hits.extend(certified.into_iter().map(|(_, point)| point));
    for side in 0..4 {
        let start = &rect[side];
        let end = &rect[(side + 1) % 4];
        if exact::signed_line_product(line, start)? == ExactSign::Zero
            && exact::signed_line_product(line, end)? == ExactSign::Zero
        {
            hits.push(start.clone());
            hits.push(end.clone());
        }
    }
    Ok(hits)
}

pub(super) fn clip_segment_to_rectangle(
    start: &exact::ExactPoint,
    end: &exact::ExactPoint,
    pair: [XY; 2],
    line: &exact::ExactLine,
    rect: &[exact::ExactPoint; 4],
    budget: &mut ExpansionBudget,
) -> Result<Option<(exact::ExactPoint, exact::ExactPoint)>> {
    budget.add(4)?;
    let mut points = separator_rectangle_hits(pair, line, rect, budget)?;
    budget.add(2)?;
    points.reserve(2);
    if in_rectangle(rect, start)? {
        points.push(start.clone());
    }
    if in_rectangle(rect, end)? {
        points.push(end.clone());
    }
    let mut kept = 0;
    for index in 0..points.len() {
        if exact::segment_intersection(start, end, &points[index], &points[index])?.is_point() {
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
    if center.same_position(mid)? {
        return Ok(exact::orient_points(sites[0], sites[1], point)? != opposite_sign);
    }
    Ok(
        if exact::orient_points(sites[0], sites[1], center)? == opposite_sign {
            exact::compare_along(center, mid, center, point)? != Ordering::Greater
        } else {
            exact::compare_along(mid, center, center, point)? != Ordering::Greater
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
    budget: &mut ExpansionBudget,
) -> Result<Option<(exact::ExactPoint, exact::ExactPoint)>> {
    budget.add(4)?;
    let hits = separator_rectangle_hits(
        [sites[pair[0]].point.xy(), sites[pair[1]].point.xy()],
        line,
        rect,
        budget,
    )?;
    let left = exact::ExactPoint::from_xy(sites[pair[0]].point.xy());
    let right = exact::ExactPoint::from_xy(sites[pair[1]].point.xy());
    let mid = left.midpoint(&right)?;
    let opposite_sign = exact::orient_points(
        &left,
        &right,
        &exact::ExactPoint::from_xy(sites[opposite].point.xy()),
    )?;
    budget.add(hits.len().saturating_add(1))?;
    let mut points = Vec::with_capacity(hits.len().saturating_add(1));
    if in_rectangle(rect, center)? {
        points.push(center.clone());
    }
    for hit in hits {
        if hull_point_on_ray(center, &mid, [&left, &right], opposite_sign, &hit)? {
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
    } else if exact::orient_points(&left, &right, center)? == opposite_sign {
        (center.clone(), mid)
    } else {
        (mid, center.clone())
    };
    span_from_points(&origin, &hint, points)
}
