//! Exact shared-grid rounding and hot-pixel routing.

use std::cmp::Ordering;
use std::collections::HashSet;

use rstar::{AABB, RTree, RTreeObject};

#[cfg(test)]
use crate::error::Error;
use crate::error::{ResourceError, Result};
use crate::geometry::coverage::coverage_simplify_protected;
use crate::geometry::interval::Interval;
use crate::geometry::overlay::{clip_polygonal_rectangle_exact_paths, polygonize_grid};
use crate::geometry::segments::clip::SegmentParameter;
use crate::geometry::segments::ordering::try_sort_by;
use crate::geometry::{
    CoordSeq, CoordinateAxes, ExactDyadic, ExactRational, GeometryErrorKind, LineSeq, Point,
    Polygon, Ring, Shape, SharedSlice,
};

mod coordinate;
mod simplify;

pub(in crate::geometry) use coordinate::{GridCoordinate, GridPoint};

fn round_float(value: f64) -> Option<i64> {
    // The upper endpoint cannot be represented as f64, so exclude it.
    if !value.is_finite() || value < i64::MIN as f64 || value >= -(i64::MIN as f64) {
        return None;
    }
    let lower = value.floor();
    let integer = lower as i64;
    integer.checked_add(i64::from(value - lower >= 0.5))
}

fn certified_round(
    coordinate: f64,
    lower: f64,
    upper: f64,
    grid_span: i64,
    local_origin: i64,
) -> Option<i64> {
    let numerator = Interval::exact(coordinate).sub(Interval::exact(lower));
    let denominator = Interval::exact(upper).sub(Interval::exact(lower));
    if denominator.lower() <= 0.0 {
        return None;
    }
    let span = grid_span as f64;
    let origin = local_origin as f64;
    let scaled = numerator.mul(Interval::from_bounds(span.next_down(), span.next_up()));
    let placed = scaled
        .div(denominator)
        .sub(Interval::from_bounds(origin.next_down(), origin.next_up()));
    let lo = round_float(placed.lower())?;
    let hi = round_float(placed.upper())?;
    (lo == hi).then_some(lo)
}

fn integer(value: i128) -> Result<ExactRational> {
    Ok(ExactRational::from_dyadic(ExactDyadic::try_from_u128(
        value < 0,
        value.unsigned_abs(),
        0,
    )?))
}

#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the binary search is confined to the i64 range represented in i128"
)]
fn floor_i64(value: &ExactRational) -> Result<i64> {
    let min = i128::from(i64::MIN);
    let upper = i128::from(i64::MAX) + 1;
    if value.try_cmp(&integer(min)?)? == Ordering::Less
        || value.try_cmp(&integer(upper)?)? != Ordering::Less
    {
        return Err(Error::from(ResourceError::SizeOverflow));
    }
    let (mut low, mut high) = (min, upper);
    while high - low > 1 {
        let mid = low + (high - low) / 2;
        if value.try_cmp(&integer(mid)?)? == Ordering::Less {
            high = mid;
        } else {
            low = mid;
        }
    }
    i64::try_from(low).map_err(|_| Error::from(ResourceError::SizeOverflow))
}

pub(crate) fn floor_exact(value: &ExactRational) -> Result<ExactDyadic> {
    let (numerator, denominator) = value.parts();
    let nearest = numerator.nearest_integer_quotient_ties_to_zero(denominator)?;
    if ExactRational::from_dyadic(nearest.clone()).try_cmp(value)? == Ordering::Greater {
        nearest.try_subtract(ExactDyadic::one())
    } else {
        Ok(nearest)
    }
}

fn ceil_exact(value: &ExactRational) -> Result<ExactDyadic> {
    let floor = floor_exact(value)?;
    if ExactRational::from_dyadic(floor.clone()).try_cmp(value)? == Ordering::Equal {
        Ok(floor)
    } else {
        floor.try_add(ExactDyadic::one())
    }
}

#[derive(Clone)]
struct ExactPoint([ExactRational; 2]);

impl ExactPoint {
    #[cfg(test)]
    fn from_f64(x: f64, y: f64) -> Self {
        Self([ExactRational::from_f64(x), ExactRational::from_f64(y)])
    }

    fn subtract(&self, other: &Self) -> Result<Self> {
        let [x, y] = &self.0;
        let [other_x, other_y] = &other.0;
        Ok(Self([x.try_subtract(other_x)?, y.try_subtract(other_y)?]))
    }

    fn pixel(&self) -> Result<GridPoint> {
        let [x, y] = &self.0;
        let half = ExactRational::from_f64(0.5);
        Ok([
            GridCoordinate::from_exact(floor_exact(&x.try_add(&half)?)?),
            GridCoordinate::from_exact(floor_exact(&y.try_add(&half)?)?),
        ])
    }

    fn bounds(&self) -> Result<[(f64, f64); 2]> {
        let [x, y] = &self.0;
        let bounds = |value: &ExactRational| -> Result<(f64, f64)> {
            if value.try_cmp(&ExactRational::from_f64(f64::MAX))?.is_gt() {
                Ok((f64::MAX, f64::MAX))
            } else if value.try_cmp(&ExactRational::from_f64(-f64::MAX))?.is_lt() {
                Ok((-f64::MAX, -f64::MAX))
            } else {
                value.outward_bounds()
            }
        };
        Ok([bounds(x)?, bounds(y)?])
    }
}

fn cross(a: &ExactPoint, b: &ExactPoint) -> Result<ExactRational> {
    let [ax, ay] = &a.0;
    let [bx, by] = &b.0;
    ax.try_product(by)?.try_subtract(&ay.try_product(bx)?)
}

fn segment_intersection(
    a: &ExactPoint,
    b: &ExactPoint,
    c: &ExactPoint,
    d: &ExactPoint,
) -> Result<Option<ExactPoint>> {
    let u = b.subtract(a)?;
    let v = d.subtract(c)?;
    let w = c.subtract(a)?;
    let denominator = cross(&u, &v)?;
    let zero = integer(0)?;
    if denominator.try_cmp(&zero)? == Ordering::Equal {
        return Ok(None);
    }
    let t = cross(&w, &v)?.try_quotient(&denominator)?;
    let s = cross(&w, &u)?.try_quotient(&denominator)?;
    let one = integer(1)?;
    if t.try_cmp(&zero)? == Ordering::Less
        || t.try_cmp(&one)? == Ordering::Greater
        || s.try_cmp(&zero)? == Ordering::Less
        || s.try_cmp(&one)? == Ordering::Greater
    {
        return Ok(None);
    }
    let [ax, ay] = &a.0;
    let [ux, uy] = &u.0;
    Ok(Some(ExactPoint([
        ax.try_add(&t.try_product(ux)?)?,
        ay.try_add(&t.try_product(uy)?)?,
    ])))
}

struct GridRectangle {
    minimum: [ExactRational; 2],
    maximum: [ExactRational; 2],
}

struct ClippedSegment {
    start: ExactPoint,
    end: ExactPoint,
    starts_at_source: bool,
    ends_at_source: bool,
}

fn interpolate(
    a: &ExactPoint,
    delta: &ExactPoint,
    parameter: &ExactRational,
) -> Result<ExactPoint> {
    let [x, y] = &a.0;
    let [dx, dy] = &delta.0;
    Ok(ExactPoint([
        x.try_add(&parameter.try_product(dx)?)?,
        y.try_add(&parameter.try_product(dy)?)?,
    ]))
}

fn clip_segment(
    a: &ExactPoint,
    b: &ExactPoint,
    rectangle: &GridRectangle,
) -> Result<Option<ClippedSegment>> {
    let zero = integer(0)?;
    let one = integer(1)?;
    let delta = b.subtract(a)?;
    let mut low = zero.clone();
    let mut high = one.clone();
    for ((((start, end), change), minimum), maximum) in
        a.0.iter()
            .zip(&b.0)
            .zip(&delta.0)
            .zip(&rectangle.minimum)
            .zip(&rectangle.maximum)
    {
        let start_below = start.try_cmp(minimum)?.is_lt();
        let end_below = end.try_cmp(minimum)?.is_lt();
        let start_above = start.try_cmp(maximum)?.is_gt();
        let end_above = end.try_cmp(maximum)?.is_gt();
        if (start_below && end_below) || (start_above && end_above) {
            return Ok(None);
        }
        if !(start_below || end_below || start_above || end_above) {
            continue;
        }
        let sign = change.try_cmp(&zero)?;
        let first = minimum.try_subtract(start)?.try_quotient(change)?;
        let last = maximum.try_subtract(start)?.try_quotient(change)?;
        let (first, last) = if sign == Ordering::Less {
            (last, first)
        } else {
            (first, last)
        };
        if first.try_cmp(&low)? == Ordering::Greater {
            low = first;
        }
        if last.try_cmp(&high)? == Ordering::Less {
            high = last;
        }
        if low.try_cmp(&high)? == Ordering::Greater {
            return Ok(None);
        }
    }
    let starts_at_source = low.try_cmp(&zero)? == Ordering::Equal;
    let ends_at_source = high.try_cmp(&one)? == Ordering::Equal;
    Ok(Some(ClippedSegment {
        start: if starts_at_source {
            a.clone()
        } else {
            interpolate(a, &delta, &low)?
        },
        end: if ends_at_source {
            b.clone()
        } else {
            interpolate(a, &delta, &high)?
        },
        starts_at_source,
        ends_at_source,
    }))
}

fn clip_line_path(path: &[ExactPoint], rectangle: &GridRectangle) -> Result<Vec<Vec<ExactPoint>>> {
    let mut result: Vec<Vec<ExactPoint>> = Vec::new();
    let mut previous_ended_at_source = false;
    for pair in path.windows(2) {
        let [start, end] = pair else {
            unreachable!("a two-point window has two entries");
        };
        let Some(clipped) = clip_segment(start, end, rectangle)? else {
            previous_ended_at_source = false;
            continue;
        };
        if previous_ended_at_source && clipped.starts_at_source {
            result
                .last_mut()
                .expect("a continued path has a previous component")
                .push(clipped.end);
        } else {
            result.push(vec![clipped.start, clipped.end]);
        }
        previous_ended_at_source = clipped.ends_at_source;
    }
    Ok(result)
}

fn same_exact_point(left: &ExactPoint, right: &ExactPoint) -> Result<bool> {
    let [lx, ly] = &left.0;
    let [rx, ry] = &right.0;
    Ok(lx.try_cmp(rx)? == Ordering::Equal && ly.try_cmp(ry)? == Ordering::Equal)
}

fn ring_has_area_in_rectangle(ring: &[ExactPoint], rectangle: &GridRectangle) -> Result<bool> {
    for pair in ring.windows(2) {
        let [a, b] = pair else {
            unreachable!("a two-point window has two entries");
        };
        if let Some(segment) = clip_segment(a, b, rectangle)?
            && !same_exact_point(&segment.start, &segment.end)?
        {
            let [ax, ay] = &segment.start.0;
            let [bx, by] = &segment.end.0;
            let midpoint = [ax.try_midpoint(bx)?, ay.try_midpoint(by)?];
            let mut inside = true;
            for ((value, minimum), maximum) in midpoint
                .iter()
                .zip(&rectangle.minimum)
                .zip(&rectangle.maximum)
            {
                if value.try_cmp(minimum)? != Ordering::Greater
                    || value.try_cmp(maximum)? != Ordering::Less
                {
                    inside = false;
                    break;
                }
            }
            if inside {
                return Ok(true);
            }
        }
    }
    let [minx, miny] = &rectangle.minimum;
    let [maxx, maxy] = &rectangle.maximum;
    let center = [minx.try_midpoint(maxx)?, miny.try_midpoint(maxy)?];
    let [cx, cy] = &center;
    let mut inside = false;
    for pair in ring.windows(2) {
        let [a, b] = pair else {
            unreachable!("a two-point window has two entries");
        };
        let [ax, ay] = &a.0;
        let [bx, by] = &b.0;
        let above_a = ay.try_cmp(cy)? == Ordering::Greater;
        let above_b = by.try_cmp(cy)? == Ordering::Greater;
        if above_a != above_b {
            let parameter = cy.try_subtract(ay)?.try_quotient(&by.try_subtract(ay)?)?;
            let crossing = ax.try_add(&parameter.try_product(&bx.try_subtract(ax)?)?)?;
            if crossing.try_cmp(cx)? == Ordering::Greater {
                inside = !inside;
            }
        }
    }
    Ok(inside)
}

fn grid_integer(value: &ExactRational) -> Result<GridCoordinate> {
    let floor = floor_exact(value)?;
    if value.try_cmp(&ExactRational::from_dyadic(floor.clone()))? != Ordering::Equal {
        return Err(GeometryErrorKind::message(
            "tile boundary clipping produced a fractional grid vertex",
        ));
    }
    Ok(GridCoordinate::from_exact(floor))
}

fn integer_point(point: &ExactPoint) -> Result<GridPoint> {
    let [x, y] = &point.0;
    Ok([grid_integer(x)?, grid_integer(y)?])
}

fn exact_integer_point([x, y]: GridPoint) -> ExactPoint {
    ExactPoint([x.rational(), y.rational()])
}

fn grid_lines(
    paths: &[Vec<ExactPoint>],
    extent: &ExactRational,
    buffer: &ExactRational,
    clip: bool,
    track_collapse: bool,
) -> Result<(Vec<Vec<GridPoint>>, bool)> {
    let zero = integer(0)?;
    let minimum = zero.try_subtract(buffer)?;
    let maximum = extent.try_add(buffer)?;
    let tile = GridRectangle {
        minimum: [minimum.clone(), minimum.clone()],
        maximum: [maximum.clone(), maximum.clone()],
    };
    let work_minimum = minimum.try_subtract(&integer(1)?)?;
    let work_maximum = maximum.try_add(&integer(1)?)?;
    let work = GridRectangle {
        minimum: [work_minimum.clone(), work_minimum],
        maximum: [work_maximum.clone(), work_maximum],
    };
    let boundaries = if clip {
        vec![zero, extent.clone(), minimum, maximum]
    } else {
        vec![zero, extent.clone()]
    };
    let mut output = Vec::new();
    let mut collapsed = false;
    for path in paths {
        let originally_visible = if !track_collapse {
            false
        } else if clip {
            let visible = clip_line_path(path, &tile)?;
            let mut has_segment = false;
            for part in visible {
                for pair in part.windows(2) {
                    let [a, b] = pair else {
                        unreachable!("a two-point window has two entries");
                    };
                    if !same_exact_point(a, b)? {
                        has_segment = true;
                        break;
                    }
                }
                if has_segment {
                    break;
                }
            }
            has_segment
        } else {
            true
        };
        let work_paths = if clip {
            clip_line_path(path, &work)?
        } else {
            vec![path.clone()]
        };
        let anchored = insert_anchors(&work_paths, &boundaries, &boundaries)?;
        let routed = route_paths(&anchored)?;
        let before = output.len();
        for part in routed {
            let final_paths = if clip {
                let exact = part
                    .into_iter()
                    .map(exact_integer_point)
                    .collect::<Vec<_>>();
                clip_line_path(&exact, &tile)?
            } else {
                vec![
                    part.into_iter()
                        .map(exact_integer_point)
                        .collect::<Vec<_>>(),
                ]
            };
            for final_path in final_paths {
                let mut points = Vec::new();
                for point in final_path {
                    let point = integer_point(&point)?;
                    if points.last() != Some(&point) {
                        points.push(point);
                    }
                }
                if points.len() >= 2 {
                    output.push(points);
                }
            }
        }
        if originally_visible && output.len() == before {
            collapsed = true;
        }
    }
    Ok((output, collapsed))
}

struct PixelInterval {
    lower: SegmentParameter,
    upper: SegmentParameter,
    lower_closed: bool,
    upper_closed: bool,
}

fn intersects_pixel(a: &ExactPoint, b: &ExactPoint, pixel: &GridPoint) -> Result<bool> {
    let mut interval = PixelInterval {
        lower: SegmentParameter::Start,
        upper: SegmentParameter::End,
        lower_closed: true,
        upper_closed: true,
    };
    let zero = integer(0)?;
    let parameter_zero = ExactDyadic::zero();
    let parameter_one = ExactDyadic::one();
    let half = ExactRational::from_f64(0.5);
    for ((start, end), center) in a.0.iter().zip(&b.0).zip(pixel) {
        let center = center.rational();
        let minimum = center.try_subtract(&half)?;
        let maximum = center.try_add(&half)?;
        let delta = end.try_subtract(start)?;
        let sign = delta.try_cmp(&zero)?;
        if sign == Ordering::Equal {
            if start.try_cmp(&minimum)? == Ordering::Less
                || start.try_cmp(&maximum)? != Ordering::Less
            {
                return Ok(false);
            }
            continue;
        }
        let parameter = |boundary: &ExactRational| -> Result<SegmentParameter> {
            let offset = boundary.try_subtract(start)?;
            let (offset_num, offset_den) = offset.components();
            let (delta_num, delta_den) = delta.components();
            Ok(SegmentParameter::Ratio {
                num: offset_num.try_product(delta_den)?,
                den: offset_den.try_product(delta_num)?,
            })
        };
        let low = parameter(&minimum)?;
        let high = parameter(&maximum)?;
        let (low, high, low_closed, high_closed) = if sign == Ordering::Less {
            (high, low, false, true)
        } else {
            (low, high, true, false)
        };
        match low.cmp(&interval.lower, &parameter_zero, &parameter_one)? {
            Ordering::Greater => {
                interval.lower = low;
                interval.lower_closed = low_closed;
            },
            Ordering::Equal => interval.lower_closed &= low_closed,
            Ordering::Less => {},
        }
        match high.cmp(&interval.upper, &parameter_zero, &parameter_one)? {
            Ordering::Less => {
                interval.upper = high;
                interval.upper_closed = high_closed;
            },
            Ordering::Equal => interval.upper_closed &= high_closed,
            Ordering::Greater => {},
        }
        let order = interval
            .lower
            .cmp(&interval.upper, &parameter_zero, &parameter_one)?;
        if order == Ordering::Greater
            || (order == Ordering::Equal && !(interval.lower_closed && interval.upper_closed))
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn insert_anchors(
    paths: &[Vec<ExactPoint>],
    x_lines: &[ExactRational],
    y_lines: &[ExactRational],
) -> Result<Vec<Vec<ExactPoint>>> {
    let mut output = Vec::with_capacity(paths.len());
    for path in paths {
        let mut anchored = Vec::new();
        if let Some(first) = path.first() {
            anchored.push(first.clone());
        }
        for pair in path.windows(2) {
            let [a, b] = pair else {
                unreachable!("a two-point window has two entries");
            };
            let delta = b.subtract(a)?;
            let mut parameters = Vec::new();
            for (((start, end), change), lines) in
                a.0.iter().zip(&b.0).zip(&delta.0).zip([x_lines, y_lines])
            {
                for line in lines {
                    let start_side = start.try_cmp(line)?;
                    let end_side = end.try_cmp(line)?;
                    if !matches!(
                        (start_side, end_side),
                        (Ordering::Less, Ordering::Greater) | (Ordering::Greater, Ordering::Less)
                    ) {
                        continue;
                    }
                    let parameter = line.try_subtract(start)?.try_quotient(change)?;
                    parameters.push(parameter);
                }
            }
            try_sort_by(&mut parameters, ExactRational::try_cmp)?;
            let mut previous: Option<ExactRational> = None;
            for parameter in parameters {
                if let Some(previous) = &previous
                    && parameter.try_cmp(previous)? == Ordering::Equal
                {
                    continue;
                }
                anchored.push(interpolate(a, &delta, &parameter)?);
                previous = Some(parameter);
            }
            anchored.push(b.clone());
        }
        output.push(anchored);
    }
    Ok(output)
}

#[derive(Clone, Copy)]
struct IndexedEnvelope {
    index: usize,
    envelope: AABB<[f64; 2]>,
}

impl RTreeObject for IndexedEnvelope {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        self.envelope
    }
}

fn segment_envelope(a: &ExactPoint, b: &ExactPoint) -> Result<AABB<[f64; 2]>> {
    let [ax, ay] = a.bounds()?;
    let [bx, by] = b.bounds()?;
    Ok(AABB::from_corners(
        [
            ax.0.min(bx.0).next_down().max(-f64::MAX),
            ay.0.min(by.0).next_down().max(-f64::MAX),
        ],
        [
            ax.1.max(bx.1).next_up().min(f64::MAX),
            ay.1.max(by.1).next_up().min(f64::MAX),
        ],
    ))
}

fn route_paths(paths: &[Vec<ExactPoint>]) -> Result<Vec<Vec<GridPoint>>> {
    let mut pixels = HashSet::new();
    let mut segments = Vec::new();
    for path in paths {
        for point in path {
            pixels.insert(point.pixel()?);
        }
        for pair in path.windows(2) {
            let [a, b] = pair else {
                unreachable!("a two-point window has two entries");
            };
            segments.push((a, b));
        }
    }
    let envelopes = segments
        .iter()
        .enumerate()
        .map(|(index, (a, b))| {
            Ok(IndexedEnvelope {
                index,
                envelope: segment_envelope(a, b)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let segment_index = RTree::bulk_load(envelopes.clone());
    for (index, (a, b)) in segments.iter().enumerate() {
        let envelope = envelopes.get(index).expect("each segment has an envelope");
        for candidate in segment_index.locate_in_envelope_intersecting(envelope.envelope) {
            if candidate.index <= index {
                continue;
            }
            let (c, d) = segments
                .get(candidate.index)
                .expect("indexed segment belongs to the staged input");
            if let Some(point) = segment_intersection(a, b, c, d)? {
                pixels.insert(point.pixel()?);
            }
        }
    }
    let pixels = pixels.into_iter().collect::<Vec<_>>();
    let pixel_index = RTree::bulk_load(
        pixels
            .iter()
            .enumerate()
            .map(|(index, [x, y])| {
                Ok(IndexedEnvelope {
                    index,
                    envelope: AABB::from_point([x.rounded()?, y.rounded()?]),
                })
            })
            .collect::<Result<Vec<_>>>()?,
    );
    let mut output = Vec::with_capacity(paths.len());
    let mut envelopes = envelopes.iter();
    for path in paths {
        let mut routed = Vec::new();
        for pair in path.windows(2) {
            let [a, b] = pair else {
                unreachable!("a two-point window has two entries");
            };
            let mut visits = Vec::new();
            let envelope = envelopes
                .next()
                .expect("each path segment has an envelope")
                .envelope;
            let envelope = AABB::from_corners(
                envelope
                    .lower()
                    .map(|value| (value - 1.0).next_down().max(-f64::MAX)),
                envelope
                    .upper()
                    .map(|value| (value + 1.0).next_up().min(f64::MAX)),
            );
            for candidate in pixel_index.locate_in_envelope_intersecting(envelope) {
                let pixel = pixels
                    .get(candidate.index)
                    .expect("indexed pixel belongs to the staged input");
                if intersects_pixel(a, b, pixel)? {
                    visits.push(pixel);
                }
            }
            let [ax, ay] = &a.0;
            let [bx, by] = &b.0;
            let reverse_x = ax.try_cmp(bx)?.is_gt();
            let reverse_y = ay.try_cmp(by)?.is_gt();
            // Disjoint half-open grid cells are visited monotonically in x,
            // then in y within a column; no rational parameter is needed to order them.
            visits.sort_unstable_by(|left, right| {
                let [lx, ly] = *left;
                let [rx, ry] = *right;
                let x = if reverse_x { rx.cmp(lx) } else { lx.cmp(rx) };
                x.then_with(|| if reverse_y { ry.cmp(ly) } else { ly.cmp(ry) })
            });
            for point in visits {
                if routed.last() != Some(point) {
                    routed.push(point.clone());
                }
            }
        }
        output.push(routed);
    }
    Ok(output)
}

/// Round a represented coordinate on the shared grid, with ties toward +infinity.
#[cfg(test)]
pub(super) fn round(
    coordinate: f64,
    lower: f64,
    upper: f64,
    grid_span: i64,
    local_origin: i64,
) -> Result<i64> {
    if !coordinate.is_finite() {
        return Err(GeometryErrorKind::NonFiniteCoordinate.into());
    }
    if !lower.is_finite() || !upper.is_finite() || lower >= upper || grid_span <= 0 {
        return Err(GeometryErrorKind::InvalidRectangle.into());
    }
    if let Some(rounded) = certified_round(coordinate, lower, upper, grid_span, local_origin) {
        return Ok(rounded);
    }
    let value = exact_grid_position(coordinate, lower, upper, grid_span, local_origin)?
        .try_add(&ExactRational::from_f64(0.5))?;
    floor_i64(&value)
}

#[cfg(test)]
fn exact_grid_position(
    coordinate: f64,
    lower: f64,
    upper: f64,
    grid_span: i64,
    local_origin: i64,
) -> Result<ExactRational> {
    GridAxis::new(lower, upper, grid_span, local_origin)?.position(coordinate)
}

#[derive(Clone)]
struct GridAxis {
    source_lower: f64,
    source_upper: f64,
    grid_span: i64,
    local_origin: Option<i64>,
    lower: ExactRational,
    scale: ExactRational,
    origin: ExactRational,
}

#[derive(Clone)]
pub(crate) struct TileGrid {
    x: GridAxis,
    y: GridAxis,
    extent: ExactRational,
    world_span: Option<i64>,
}

impl TileGrid {
    pub(crate) fn simplify_shape(
        &self,
        shape: Shape,
        buffer: &ExactRational,
        clip: bool,
        tolerance: f64,
        method: crate::geometry::SimplifyMethod,
    ) -> Result<Shape> {
        let zero = integer(0)?;
        let mut boundaries = vec![zero.clone(), self.extent.clone()];
        if clip {
            boundaries.push(zero.try_subtract(buffer)?);
            boundaries.push(self.extent.try_add(buffer)?);
        }
        simplify::simplify_tile_shape(shape, &boundaries, tolerance, method)
    }

    pub(crate) fn simplify_coverage(
        &self,
        shapes: &[Shape],
        buffer: &ExactRational,
        clip: bool,
        tolerance: f64,
        method: crate::geometry::SimplifyMethod,
    ) -> Result<Vec<Shape>> {
        let zero = integer(0)?;
        let mut boundaries = vec![zero.clone(), self.extent.clone()];
        if clip {
            boundaries.push(zero.try_subtract(buffer)?);
            boundaries.push(self.extent.try_add(buffer)?);
        }
        let protected = |point: crate::geometry::XY| -> Result<bool> {
            let x = ExactRational::from_f64(point.x);
            let y = ExactRational::from_f64(point.y);
            for boundary in &boundaries {
                if x.try_cmp(boundary)?.is_eq() || y.try_cmp(boundary)?.is_eq() {
                    return Ok(true);
                }
            }
            Ok(false)
        };
        coverage_simplify_protected(
            shapes,
            tolerance,
            method,
            true,
            &crate::geometry::SourceArcContext::cartesian(),
            Some(&protected),
        )
    }

    pub(crate) fn custom(bounds: [f64; 4], extent: i64) -> Result<Self> {
        let [minx, miny, maxx, maxy] = bounds;
        Ok(Self {
            x: GridAxis::new(minx, maxx, extent, 0)?,
            y: GridAxis::new(-maxy, -miny, extent, 0)?,
            extent: integer(i128::from(extent))?,
            world_span: None,
        })
    }

    pub(crate) fn xyz(zoom: u32, column: u32, row: u32, extent: i64) -> Result<Self> {
        if zoom > 29
            || extent <= 0
            || extent > i64::from(u32::MAX)
            || u64::from(column) >= (1_u64 << zoom)
            || u64::from(row) >= (1_u64 << zoom)
        {
            return Err(GeometryErrorKind::InvalidRectangle.into());
        }
        let span = extent
            .checked_mul(1_i64 << zoom)
            .ok_or(ResourceError::SizeOverflow)?;
        let x_origin = i64::from(column)
            .checked_mul(extent)
            .ok_or(ResourceError::SizeOverflow)?;
        let y_origin = i64::from(row)
            .checked_mul(extent)
            .ok_or(ResourceError::SizeOverflow)?;
        let world = std::f64::consts::PI * 6_378_137.0;
        Ok(Self {
            x: GridAxis::new(-world, world, span, x_origin)?,
            y: GridAxis::new(-world, world, span, y_origin)?,
            extent: integer(i128::from(extent))?,
            world_span: Some(span),
        })
    }

    fn point(&self, x: f64, y: f64) -> Result<ExactPoint> {
        Ok(ExactPoint([self.x.position(x)?, self.y.position(-y)?]))
    }

    pub(crate) fn locate_shape(&self, shape: &Shape) -> Result<Shape> {
        shape.map_xy_with(|x, y| {
            let x = self
                .x
                .source_coordinate(&ExactRational::from_f64(x))?
                .round()?;
            let y = -self
                .y
                .source_coordinate(&ExactRational::from_f64(y))?
                .round()?;
            Ok((x, y))
        })
    }

    fn exact_point(&self, x: &ExactRational, y: &ExactRational) -> Result<ExactPoint> {
        Ok(ExactPoint([
            self.x.position_exact(x)?,
            self.y.position_exact(&integer(0)?.try_subtract(y)?)?,
        ]))
    }

    fn work_rectangle(&self, buffer: &ExactRational) -> Result<GridRectangle> {
        let lower = integer(-1)?.try_subtract(buffer)?;
        let upper = self.extent.try_add(buffer)?.try_add(&integer(1)?)?;
        Ok(GridRectangle {
            minimum: [lower.clone(), lower],
            maximum: [upper.clone(), upper],
        })
    }

    fn images(&self, shape: &Shape, buffer: &ExactRational, clip: bool) -> Result<Vec<Self>> {
        let Some(span) = self.world_span.filter(|_| clip) else {
            return Ok(vec![self.clone()]);
        };
        let Some(bounds) = shape.bounds() else {
            return Ok(Vec::new());
        };
        let work = self.work_rectangle(buffer)?;
        let [xmin, ymin] = &work.minimum;
        let [xmax, ymax] = &work.maximum;
        let shape_ymin = self.y.position(-bounds.maxy())?;
        let shape_ymax = self.y.position(-bounds.miny())?;
        if shape_ymax.try_cmp(ymin)? == Ordering::Less
            || shape_ymin.try_cmp(ymax)? == Ordering::Greater
        {
            return Ok(Vec::new());
        }
        let shape_xmin = self.x.position(bounds.minx())?;
        let shape_xmax = self.x.position(bounds.maxx())?;
        let span = integer(i128::from(span))?;
        let low = xmin.try_subtract(&shape_xmax)?.try_quotient(&span)?;
        let high = xmax.try_subtract(&shape_xmin)?.try_quotient(&span)?;
        let mut image = ceil_exact(&low)?;
        let last = floor_exact(&high)?;
        let mut output = Vec::new();
        while image.cmp(&last) != Ordering::Greater {
            let mut grid = self.clone();
            let shift = ExactRational::from_dyadic(image.clone()).try_product(&span)?;
            grid.x.origin = grid.x.origin.try_subtract(&shift)?;
            if !image.is_zero() {
                grid.x.local_origin = None;
            }
            output.push(grid);
            image = image.try_add(ExactDyadic::one())?;
        }
        Ok(output)
    }

    fn source_bounds(&self, rectangle: &GridRectangle) -> Result<crate::geometry::Bounds> {
        fn lower(value: &ExactRational) -> Result<f64> {
            if value.try_cmp(&ExactRational::from_f64(-f64::MAX))? == Ordering::Less {
                Ok(-f64::MAX)
            } else {
                Ok(value.outward_bounds()?.0)
            }
        }
        fn upper(value: &ExactRational) -> Result<f64> {
            if value.try_cmp(&ExactRational::from_f64(f64::MAX))? == Ordering::Greater {
                Ok(f64::MAX)
            } else {
                Ok(value.outward_bounds()?.1)
            }
        }
        let [xmin, ymin] = &rectangle.minimum;
        let [xmax, ymax] = &rectangle.maximum;
        let minx = lower(&self.x.source_coordinate(xmin)?)?;
        let maxx = upper(&self.x.source_coordinate(xmax)?)?;
        let miny = -upper(&self.y.source_coordinate(ymax)?)?;
        let maxy = -lower(&self.y.source_coordinate(ymin)?)?;
        Ok(crate::geometry::Bounds::new_raw(minx, miny, maxx, maxy))
    }

    fn pixel(&self, point: Point) -> Result<GridPoint> {
        match (self.x.certified(point.x), self.y.certified(-point.y)) {
            (Some(x), Some(y)) => Ok([GridCoordinate::Small(x), GridCoordinate::Small(y)]),
            _ => self.point(point.x, point.y)?.pixel(),
        }
    }

    pub(crate) fn prepare_shape(
        &self,
        shape: &Shape,
        buffer: &ExactRational,
        clip: bool,
        track_collapse: bool,
    ) -> Result<(Option<Shape>, bool)> {
        match shape {
            Shape::Empty(..) => Ok((None, false)),
            Shape::Point(point) => self.prepare_points(std::iter::once(*point), buffer, clip),
            Shape::MultiPoint(points) => self.prepare_points(points.points(), buffer, clip),
            Shape::LineString(line) => {
                self.prepare_line_images(std::iter::once(line), buffer, clip, track_collapse)
            },
            Shape::MultiLineString(lines) => {
                self.prepare_line_images(lines.iter(), buffer, clip, track_collapse)
            },
            Shape::Polygon(polygon) => {
                self.prepare_polygon_images(std::iter::once(polygon), buffer, clip, track_collapse)
            },
            Shape::MultiPolygon(polygons) => {
                self.prepare_polygon_images(polygons.iter(), buffer, clip, track_collapse)
            },
            Shape::GeometryCollection(_) => Err(GeometryErrorKind::message(
                "tile preparation does not accept GeometryCollection",
            )),
        }
    }

    fn prepare_points(
        &self,
        points: impl Iterator<Item = Point>,
        buffer: &ExactRational,
        clip: bool,
    ) -> Result<(Option<Shape>, bool)> {
        let mut output = Vec::with_capacity(points.size_hint().0);
        let limits = if clip {
            Some((
                GridCoordinate::from_exact(ceil_exact(&integer(0)?.try_subtract(buffer)?)?),
                GridCoordinate::from_exact(floor_exact(&self.extent.try_add(buffer)?)?),
            ))
        } else {
            None
        };
        for point in points {
            let [x, y] = self.pixel(point)?;
            let Some((lower, upper)) = &limits else {
                output.push(grid_point([x, y])?);
                continue;
            };
            if y < *lower || y > *upper {
                continue;
            }
            if let Some(span) = self.world_span {
                // Integer translations commute with grid rounding; clipping the
                // pixels avoids transforming the same point for every world image.
                let span = integer(i128::from(span))?;
                let position = x.rational();
                let mut image = ceil_exact(
                    &lower
                        .rational()
                        .try_subtract(&position)?
                        .try_quotient(&span)?,
                )?;
                let last = floor_exact(
                    &upper
                        .rational()
                        .try_subtract(&position)?
                        .try_quotient(&span)?,
                )?;
                while image.cmp(&last) != Ordering::Greater {
                    let shifted = position
                        .try_add(&ExactRational::from_dyadic(image.clone()).try_product(&span)?)?;
                    output.push(grid_point([
                        GridCoordinate::from_exact(floor_exact(&shifted)?),
                        y.clone(),
                    ])?);
                    image = image.try_add(ExactDyadic::one())?;
                }
            } else if x >= *lower && x <= *upper {
                output.push(grid_point([x, y])?);
            }
        }
        if output.is_empty() {
            Ok((None, false))
        } else {
            Ok((
                Some(Shape::narrow_points(CoordinateAxes::XY, output)?),
                false,
            ))
        }
    }

    fn prepare_line_images<'a>(
        &self,
        lines: impl Iterator<Item = &'a LineSeq>,
        buffer: &ExactRational,
        clip: bool,
        track_collapse: bool,
    ) -> Result<(Option<Shape>, bool)> {
        let mut output = Vec::new();
        let mut collapsed = false;
        for line in lines {
            if self.world_span.is_none() || !clip {
                let (shape, line_collapsed) =
                    self.prepare_lines(std::iter::once(line), buffer, clip, track_collapse)?;
                collapsed |= line_collapsed;
                collect_line_parts(shape, &mut output);
                continue;
            }
            let source = Shape::LineString(line.clone());
            for grid in self.images(&source, buffer, clip)? {
                let (shape, image_collapsed) =
                    grid.prepare_lines(std::iter::once(line), buffer, clip, track_collapse)?;
                collapsed |= image_collapsed;
                collect_line_parts(shape, &mut output);
            }
        }
        if output.is_empty() {
            Ok((None, collapsed))
        } else {
            Ok((
                Some(Shape::narrow_lines(CoordinateAxes::XY, output)),
                collapsed,
            ))
        }
    }

    fn prepare_polygon_images<'a>(
        &self,
        polygons: impl Iterator<Item = &'a Polygon>,
        buffer: &ExactRational,
        clip: bool,
        track_collapse: bool,
    ) -> Result<(Option<Shape>, bool)> {
        let mut inputs = Vec::new();
        for polygon in polygons {
            if self.world_span.is_none() || !clip {
                inputs.push((0, polygon.clone(), self.clone()));
                continue;
            }
            let source = Shape::Polygon(polygon.clone());
            for grid in self.images(&source, buffer, clip)? {
                inputs.push((0, polygon.clone(), grid));
            }
        }
        Ok(self
            .prepare_polygon_groups(&inputs, 1, buffer, clip, track_collapse)?
            .pop()
            .expect("one source row has one result"))
    }

    pub(crate) fn prepare_coverage(
        &self,
        shapes: &[Shape],
        buffer: &ExactRational,
        clip: bool,
        track_collapse: bool,
    ) -> Result<Vec<(Option<Shape>, bool)>> {
        let mut inputs = Vec::new();
        for (row, shape) in shapes.iter().enumerate() {
            let polygons: Vec<&Polygon> = match shape {
                Shape::Polygon(polygon) => vec![polygon],
                Shape::MultiPolygon(polygons) => polygons.iter().collect(),
                Shape::Empty(..) => Vec::new(),
                Shape::Point(_)
                | Shape::MultiPoint(_)
                | Shape::LineString(_)
                | Shape::MultiLineString(_)
                | Shape::GeometryCollection(_) => {
                    return Err(GeometryErrorKind::message(
                        "coverage requires polygonal geometry",
                    ));
                },
            };
            for polygon in polygons {
                if self.world_span.is_none() || !clip {
                    inputs.push((row, polygon.clone(), self.clone()));
                } else {
                    let source = Shape::Polygon(polygon.clone());
                    for grid in self.images(&source, buffer, clip)? {
                        inputs.push((row, polygon.clone(), grid));
                    }
                }
            }
        }
        self.prepare_polygon_groups(&inputs, shapes.len(), buffer, clip, track_collapse)
    }

    fn prepare_lines<'a>(
        &self,
        lines: impl Iterator<Item = &'a LineSeq>,
        buffer: &ExactRational,
        clip: bool,
        track_collapse: bool,
    ) -> Result<(Option<Shape>, bool)> {
        let mut paths = Vec::new();
        for line in lines {
            paths.push(
                line.iter()
                    .map(|point| self.point(point.x, point.y))
                    .collect::<Result<Vec<_>>>()?,
            );
        }
        let (paths, collapsed) = grid_lines(&paths, &self.extent, buffer, clip, track_collapse)?;
        let mut lines = Vec::with_capacity(paths.len());
        for path in paths {
            let points = path
                .into_iter()
                .map(grid_point)
                .collect::<Result<Vec<_>>>()?;
            lines.push(LineSeq::try_new(CoordSeq::try_from_points(&points)?)?);
        }
        if lines.is_empty() {
            Ok((None, collapsed))
        } else {
            Ok((
                Some(Shape::narrow_lines(CoordinateAxes::XY, lines)),
                collapsed,
            ))
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "axis indices are 0 or 1; polygon and visibility rows are allocated from the input row count"
    )]
    fn prepare_polygon_groups(
        &self,
        polygons: &[(usize, Polygon, Self)],
        row_count: usize,
        buffer: &ExactRational,
        clip: bool,
        track_collapse: bool,
    ) -> Result<Vec<(Option<Shape>, bool)>> {
        let mut groups = Vec::new();
        let mut paths = Vec::new();
        for (row, polygon, grid) in polygons {
            let clipping_bounds = if clip {
                let bounds = grid.source_bounds(&grid.work_rectangle(buffer)?)?;
                let inside = polygon.rings().all(|ring| {
                    ring.iter().all(|point| {
                        point.x >= bounds.minx()
                            && point.x <= bounds.maxx()
                            && point.y >= bounds.miny()
                            && point.y <= bounds.maxy()
                    })
                });
                (!inside).then_some(bounds)
            } else {
                None
            };
            let source = if let Some(bounds) = clipping_bounds {
                let rectangle = rectangle_polygon(bounds)?;
                clip_polygonal_rectangle_exact_paths(
                    std::slice::from_ref(polygon),
                    &rectangle,
                    bounds,
                )?
            } else {
                vec![
                    polygon
                        .rings()
                        .map(|ring| {
                            ring.iter()
                                .map(|point| {
                                    [
                                        ExactRational::from_f64(point.x),
                                        ExactRational::from_f64(point.y),
                                    ]
                                })
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>(),
                ]
            };
            for polygon in source {
                groups.push((*row, polygon.len()));
                for ring in polygon {
                    paths.push(
                        ring.into_iter()
                            .map(|[x, y]| grid.exact_point(&x, &y))
                            .collect::<Result<Vec<_>>>()?,
                    );
                }
            }
        }
        let zero = integer(0)?;
        let boundaries = if clip {
            vec![
                zero.clone(),
                self.extent.clone(),
                zero.try_subtract(buffer)?,
                self.extent.try_add(buffer)?,
            ]
        } else {
            vec![zero, self.extent.clone()]
        };
        if paths.is_empty() {
            return Ok(vec![(None, false); row_count]);
        }
        let mut anchored = insert_anchors(&paths, &boundaries, &boundaries)?;
        if clip {
            let lower = integer(0)?.try_subtract(buffer)?;
            let upper = self.extent.try_add(buffer)?;
            let mut minimum: [Option<ExactRational>; 2] = [None, None];
            let mut maximum: [Option<ExactRational>; 2] = [None, None];
            for point in paths.iter().flatten() {
                for (axis, coordinate) in point.0.iter().enumerate() {
                    if match &minimum[axis] {
                        Some(value) => coordinate.try_cmp(value)?.is_lt(),
                        None => true,
                    } {
                        minimum[axis] = Some(coordinate.clone());
                    }
                    if match &maximum[axis] {
                        Some(value) => coordinate.try_cmp(value)?.is_gt(),
                        None => true,
                    } {
                        maximum[axis] = Some(coordinate.clone());
                    }
                }
            }
            let mut limits = [(lower.clone(), upper.clone()), (lower, upper)];
            for (axis, (lo, hi)) in limits.iter_mut().enumerate() {
                if let (Some(minimum), Some(maximum)) = (&minimum[axis], &maximum[axis]) {
                    let bounded_lo = ExactRational::from_dyadic(
                        floor_exact(minimum)?.try_subtract(ExactDyadic::one())?,
                    );
                    let bounded_hi = ExactRational::from_dyadic(
                        ceil_exact(maximum)?.try_add(ExactDyadic::one())?,
                    );
                    if bounded_lo.try_cmp(lo)? == Ordering::Greater {
                        *lo = bounded_lo;
                    }
                    if bounded_hi.try_cmp(hi)? == Ordering::Less {
                        *hi = bounded_hi;
                    }
                }
            }
            let [(left, right), (bottom, top)] = limits;
            anchored.push(vec![
                ExactPoint([left.clone(), bottom.clone()]),
                ExactPoint([right.clone(), bottom.clone()]),
                ExactPoint([right, top.clone()]),
                ExactPoint([left.clone(), top]),
                ExactPoint([left, bottom]),
            ]);
        }
        let mut routed = route_paths(&anchored)?;
        let rectangle = clip.then(|| routed.pop().expect("the clipping rectangle was routed"));
        let mut routed = routed.into_iter();
        let mut original = paths.iter();
        let visible = if clip && track_collapse {
            let lower = integer(0)?.try_subtract(buffer)?;
            let upper = self.extent.try_add(buffer)?;
            Some(GridRectangle {
                minimum: [lower.clone(), lower],
                maximum: [upper.clone(), upper],
            })
        } else {
            None
        };
        let mut polygons = vec![Vec::new(); row_count];
        let mut visibility = vec![Vec::new(); row_count];
        for (row, count) in groups {
            let mut rings = Vec::with_capacity(count);
            for _ in 0..count {
                let path = routed
                    .next()
                    .expect("each admitted ring has one routed path");
                let source = original
                    .next()
                    .expect("each routed ring has an admitted source");
                let is_visible = if !track_collapse {
                    false
                } else if let Some(visible) = &visible {
                    ring_has_area_in_rectangle(source, visible)?
                } else {
                    true
                };
                visibility[row].push(is_visible);
                rings.push(path);
            }
            polygons[row].push(rings);
        }
        polygons
            .into_iter()
            .zip(visibility)
            .map(|(polygons, visibility)| {
                if polygons.is_empty() {
                    return Ok((None, false));
                }
                let (polygons, occupied) =
                    polygonize_grid(&polygons, rectangle.as_ref(), track_collapse)?;
                let collapsed = visibility
                    .into_iter()
                    .zip(occupied)
                    .any(|(visible, occupied)| visible && !occupied);
                let mut output = Vec::with_capacity(polygons.len());
                for polygon in polygons {
                    let mut rings = polygon.into_iter();
                    let Some(shell) = rings.next() else {
                        continue;
                    };
                    let convert = |points: Vec<GridPoint>| -> Result<Ring> {
                        let points = points
                            .into_iter()
                            .map(grid_point)
                            .collect::<Result<Vec<_>>>()?;
                        Ring::try_from_closed(CoordSeq::try_from_points(&points)?)
                    };
                    output.push(Polygon {
                        shell: convert(shell)?,
                        holes: SharedSlice::from_vec(
                            rings.map(convert).collect::<Result<Vec<_>>>()?,
                        ),
                    });
                }
                Ok(if output.is_empty() {
                    (None, collapsed)
                } else {
                    (
                        Some(Shape::narrow_polygons(CoordinateAxes::XY, output)),
                        collapsed,
                    )
                })
            })
            .collect()
    }
}

fn rectangle_polygon(bounds: crate::geometry::Bounds) -> Result<Polygon> {
    let (minx, miny, maxx, maxy) = (bounds.minx(), bounds.miny(), bounds.maxx(), bounds.maxy());
    let points = [
        Point::new_raw_xy(minx, miny),
        Point::new_raw_xy(maxx, miny),
        Point::new_raw_xy(maxx, maxy),
        Point::new_raw_xy(minx, maxy),
        Point::new_raw_xy(minx, miny),
    ];
    Ok(Polygon {
        shell: Ring::try_from_closed(CoordSeq::try_from_points(&points)?)?,
        holes: SharedSlice::from_vec(Vec::new()),
    })
}

fn collect_line_parts(shape: Option<Shape>, output: &mut Vec<LineSeq>) {
    match shape {
        Some(Shape::LineString(line)) => output.push(line),
        Some(Shape::MultiLineString(lines)) => output.extend(lines.iter().cloned()),
        None => {},
        _ => unreachable!("line preparation returns lineal geometry"),
    }
}

pub(crate) fn buffer_from_bytes(bytes: &[u8]) -> Result<ExactRational> {
    let limbs = bytes
        .chunks(8)
        .map(|chunk| {
            let mut bytes = [0_u8; 8];
            bytes
                .get_mut(..chunk.len())
                .expect("a limb chunk contains at most eight bytes")
                .copy_from_slice(chunk);
            u64::from_le_bytes(bytes)
        })
        .collect();
    Ok(ExactRational::from_dyadic(
        ExactDyadic::try_from_unsigned_limbs(limbs)?,
    ))
}

fn grid_point([x, y]: GridPoint) -> Result<Point> {
    let x = x.exact_f64().ok_or_else(|| {
        GeometryErrorKind::message("tile coordinate is not exactly representable as binary64")
    })?;
    let y = y.exact_f64().ok_or_else(|| {
        GeometryErrorKind::message("tile coordinate is not exactly representable as binary64")
    })?;
    Ok(Point::new_raw_xy(x, y))
}

impl GridAxis {
    fn new(lower: f64, upper: f64, grid_span: i64, local_origin: i64) -> Result<Self> {
        if !lower.is_finite() || !upper.is_finite() || lower >= upper || grid_span <= 0 {
            return Err(GeometryErrorKind::InvalidRectangle.into());
        }
        let lower_value = lower;
        let lower = ExactRational::from_f64(lower);
        let width = ExactRational::from_f64(upper).try_subtract(&lower)?;
        let scale =
            ExactRational::from_dyadic(ExactDyadic::from_i64(grid_span)).try_quotient(&width)?;
        Ok(Self {
            source_lower: lower_value,
            source_upper: upper,
            grid_span,
            local_origin: Some(local_origin),
            lower,
            scale,
            origin: ExactRational::from_dyadic(ExactDyadic::from_i64(local_origin)),
        })
    }

    fn position(&self, coordinate: f64) -> Result<ExactRational> {
        if !coordinate.is_finite() {
            return Err(GeometryErrorKind::NonFiniteCoordinate.into());
        }
        self.position_exact(&ExactRational::from_f64(coordinate))
    }

    fn certified(&self, coordinate: f64) -> Option<i64> {
        self.local_origin.and_then(|local_origin| {
            certified_round(
                coordinate,
                self.source_lower,
                self.source_upper,
                self.grid_span,
                local_origin,
            )
        })
    }

    fn position_exact(&self, coordinate: &ExactRational) -> Result<ExactRational> {
        coordinate
            .try_subtract(&self.lower)?
            .try_product(&self.scale)?
            .try_subtract(&self.origin)
    }

    fn source_coordinate(&self, coordinate: &ExactRational) -> Result<ExactRational> {
        coordinate
            .try_add(&self.origin)?
            .try_quotient(&self.scale)?
            .try_add(&self.lower)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_shared_grid_rounding_and_neighbor_translation() {
        let world = std::f64::consts::PI * 6_378_137.0;
        let cases = [
            (0, 4095_i64, 0, 0.0, 2048),
            (1, 4096, 0, -10_018_754.171_394_622, 2048),
            (12, 4095, 2074, 259_274.399_943_317_84, 2047),
            (29, 4096, 0x0FFF_FFFF, -0.037_322_767_717_371_22, 2048),
            (
                29,
                0xFFFF_FFFF,
                0x1FFF_FFFE,
                20_037_508.230_820_94,
                2_147_483_543,
            ),
        ];
        for (zoom, extent, column, coordinate, expected) in cases {
            let span = (1_i64 << zoom) * extent;
            let origin = column * extent;
            assert_eq!(
                round(coordinate, -world, world, span, origin).unwrap(),
                expected
            );
            assert_eq!(
                round(coordinate, -world, world, span, origin + extent).unwrap() + extent,
                expected
            );
        }
    }

    #[test]
    fn ties_extreme_spans_and_subnormal_bounds() {
        assert_eq!(round(0.5, 0.0, 1.0, 1, 0).unwrap(), 1);
        assert_eq!(round(-0.5, -1.0, 0.0, 1, 1).unwrap(), 0);
        assert_eq!(round(0.0, -1e308, 1e308, 4096, 0).unwrap(), 2048);
        let tiny = f64::from_bits(1);
        assert_eq!(round(tiny, 0.0, 2.0 * tiny, 4096, 0).unwrap(), 2048);
        round(f64::NAN, 0.0, 1.0, 1, 0).unwrap_err();
        round(0.0, 0.0, 0.0, 1, 0).unwrap_err();
    }

    #[test]
    fn routed_points_use_exact_unrounded_shared_grid_coordinates() {
        let world = std::f64::consts::PI * 6_378_137.0;
        let x = exact_grid_position(
            259_274.399_943_317_84,
            -world,
            world,
            4095 * (1 << 12),
            2074 * 4095,
        )
        .unwrap();
        let point = ExactPoint([x, ExactRational::from_f64(0.0)]);
        assert_eq!(small_point(point.pixel().unwrap()), [2047, 0]);
        let axis = GridAxis::new(-world, world, 4095 * (1 << 12), 2074 * 4095).unwrap();
        let source = axis.source_coordinate(point.0.first().unwrap()).unwrap();
        assert!(
            source
                .try_cmp(&ExactRational::from_f64(259_274.399_943_317_84))
                .unwrap()
                .is_eq()
        );
    }

    #[test]
    fn grid_frames_map_exact_points_and_conservative_work_bounds() {
        let grid = TileGrid::custom([0.0, 0.0, 9.0, 9.0], 9).unwrap();
        let point = grid.point(3.0, 8.5).unwrap();
        assert_eq!(small_point(point.pixel().unwrap()), [3, 1]);
        let exact = grid
            .exact_point(&ExactRational::from_f64(3.0), &ExactRational::from_f64(8.5))
            .unwrap();
        assert_eq!(exact.pixel().unwrap(), point.pixel().unwrap());
        let work = grid.work_rectangle(&integer(0).unwrap()).unwrap();
        let bounds = grid.source_bounds(&work).unwrap();
        assert_eq!(
            (bounds.minx(), bounds.miny(), bounds.maxx(), bounds.maxy()),
            (-1.0, -1.0, 10.0, 10.0)
        );
        let grid = TileGrid::xyz(12, 2074, 2048, 4095).unwrap();
        let [x, _] = grid
            .point(259_274.399_943_317_84, 0.0)
            .unwrap()
            .pixel()
            .unwrap();
        assert_eq!(small_coordinate(x), 2047);
        assert!(TileGrid::xyz(30, 0, 0, 4096).is_err());
        let wide = TileGrid::custom([-1e308, -1.0, 1e308, 1.0], 9).unwrap();
        let work = wide.work_rectangle(&integer(4096).unwrap()).unwrap();
        let bounds = wide.source_bounds(&work).unwrap();
        assert_eq!(bounds.minx(), -f64::MAX);
        assert_eq!(bounds.maxx(), f64::MAX);
    }

    #[test]
    fn xyz_periodic_images_use_exact_translation_even_for_large_source_x() {
        let grid = TileGrid::xyz(1, 0, 0, 4096).unwrap();
        let world = std::f64::consts::PI * 6_378_137.0;
        let shape = Shape::Point(Point::new_raw_xy(world - 10_000.0, 0.0));
        let images = grid.images(&shape, &integer(64).unwrap(), true).unwrap();
        assert_eq!(images.len(), 1);
        assert_eq!(
            small_point(
                images
                    .first()
                    .unwrap()
                    .point(world - 10_000.0, 0.0)
                    .unwrap()
                    .pixel()
                    .unwrap()
            ),
            [-2, 4096]
        );

        let world_tile = TileGrid::xyz(0, 0, 0, 4096).unwrap();
        let far = Shape::Point(Point::new_raw_xy(1e300, 0.0));
        let images = world_tile.images(&far, &integer(0).unwrap(), true).unwrap();
        assert!(!images.is_empty());
        let [x, _] = images
            .first()
            .unwrap()
            .point(1e300, 0.0)
            .unwrap()
            .pixel()
            .unwrap();
        assert!((-1..=4097).contains(&small_coordinate(x)));
    }

    #[expect(
        clippy::needless_pass_by_value,
        reason = "the test helper consumes coordinates extracted from an owned two-element array"
    )]
    fn small_coordinate(coordinate: GridCoordinate) -> i64 {
        let GridCoordinate::Small(value) = coordinate else {
            panic!("test coordinate fits in i64");
        };
        value
    }

    fn small_point([x, y]: GridPoint) -> [i64; 2] {
        [small_coordinate(x), small_coordinate(y)]
    }

    fn small_paths(paths: Vec<Vec<GridPoint>>) -> Vec<Vec<[i64; 2]>> {
        paths
            .into_iter()
            .map(|path| path.into_iter().map(small_point).collect())
            .collect()
    }

    fn path(points: &[(f64, f64)]) -> Vec<ExactPoint> {
        points
            .iter()
            .map(|&(x, y)| ExactPoint::from_f64(x, y))
            .collect()
    }

    #[test]
    fn routing_nodes_crossings_and_preserves_path_direction() {
        let paths = [
            path(&[(0.0, 0.0), (4.0, 4.0)]),
            path(&[(0.0, 4.0), (4.0, 0.0)]),
            path(&[(4.0, 4.0), (0.0, 0.0)]),
        ];
        assert_eq!(small_paths(route_paths(&paths).unwrap()), [
            vec![[0, 0], [2, 2], [4, 4]],
            vec![[0, 4], [2, 2], [4, 0]],
            vec![[4, 4], [2, 2], [0, 0]],
        ]);
    }

    #[test]
    fn routing_orders_multiple_pixels_in_one_column_in_both_directions() {
        let forward = path(&[(0.0, 0.0), (4.0, 2.0)]);
        let mut reverse = forward.clone();
        reverse.reverse();
        let mut paths = vec![forward, reverse];
        for point in [(1.0, 0.0), (1.0, 1.0), (2.0, 1.0), (3.0, 1.0), (3.0, 2.0)] {
            paths.push(path(&[point]));
        }
        let routed = small_paths(route_paths(&paths).unwrap());
        assert_eq!(routed[0], [
            [0, 0],
            [1, 0],
            [1, 1],
            [2, 1],
            [3, 1],
            [3, 2],
            [4, 2]
        ]);
        assert_eq!(routed[1], [
            [4, 2],
            [3, 2],
            [3, 1],
            [2, 1],
            [1, 1],
            [1, 0],
            [0, 0]
        ]);
    }

    #[test]
    fn routing_respects_half_open_pixel_edges() {
        let paths = [
            path(&[(0.0, 0.5), (4.0, 0.5)]),
            path(&[(2.0, 0.0)]),
            path(&[(2.0, 1.0)]),
        ];
        let routed = small_paths(route_paths(&paths).unwrap());
        assert_eq!(routed[0], [[0, 1], [2, 1], [4, 1]]);
    }

    #[test]
    fn routing_vertical_half_open_edge_preserves_reverse_order() {
        let paths = [
            path(&[(0.5, 4.0), (0.5, -2.0)]),
            path(&[(0.0, 2.0)]),
            path(&[(1.0, 2.0)]),
            path(&[(1.0, 0.0)]),
        ];
        let routed = small_paths(route_paths(&paths).unwrap());
        assert_eq!(routed[0], [[1, 4], [1, 2], [1, 0], [1, -2]]);
    }

    #[test]
    fn supporting_line_anchors_survive_grid_routing() {
        let lines = [path(&[(12.0, 2.4), (20.0, 4.8)])];
        let anchored = insert_anchors(&lines, &[integer(16).unwrap()], &[]).unwrap();
        assert_eq!(small_paths(route_paths(&anchored).unwrap()), [vec![
            [12, 2],
            [16, 4],
            [20, 5]
        ]]);
    }

    #[test]
    fn line_clipping_keeps_direction_and_separates_outside_excursions() {
        let rectangle = GridRectangle {
            minimum: [integer(0).unwrap(), integer(0).unwrap()],
            maximum: [integer(4).unwrap(), integer(4).unwrap()],
        };
        let original = path(&[(10.0, 2.0), (-10.0, 2.0), (10.0, 2.0)]);
        let clipped = clip_line_path(&original, &rectangle).unwrap();
        assert_eq!(small_paths(route_paths(&clipped).unwrap()), [
            vec![[4, 2], [0, 2]],
            vec![[0, 2], [4, 2]]
        ]);
    }

    #[test]
    fn buffered_neighbors_share_line_anchor_and_clip_at_integer_vertices() {
        let extent = integer(16).unwrap();
        let buffer = integer(2).unwrap();
        let west = [path(&[(12.0, 2.4), (20.0, 4.8)])];
        let east = [path(&[(-4.0, 2.4), (4.0, 4.8)])];
        let (west, west_collapsed) = grid_lines(&west, &extent, &buffer, true, true).unwrap();
        let (east, east_collapsed) = grid_lines(&east, &extent, &buffer, true, true).unwrap();
        assert!(!west_collapsed && !east_collapsed);
        assert_eq!(small_paths(west), [vec![[12, 2], [16, 4], [18, 4]]]);
        assert_eq!(small_paths(east), [vec![[-2, 3], [0, 4], [4, 5]]]);
    }

    #[test]
    fn line_collapse_excludes_ordinary_clipping() {
        let extent = integer(16).unwrap();
        let buffer = integer(0).unwrap();
        let (_, collapsed) = grid_lines(
            &[path(&[(1.1, 1.0), (1.4, 1.0)])],
            &extent,
            &buffer,
            true,
            true,
        )
        .unwrap();
        assert!(collapsed);
        let (outside, collapsed) = grid_lines(
            &[path(&[(20.1, 1.0), (20.4, 1.0)])],
            &extent,
            &buffer,
            true,
            true,
        )
        .unwrap();
        assert!(outside.is_empty());
        assert!(!collapsed);
        let (touch, collapsed) = grid_lines(
            &[path(&[(16.0, 0.0), (18.0, 0.0)])],
            &extent,
            &buffer,
            true,
            true,
        )
        .unwrap();
        assert!(touch.is_empty());
        assert!(!collapsed);
    }

    #[test]
    fn exact_clip_anchor_and_grid_reconstruction_preserve_diagonal_hole() {
        use crate::geometry::Shape;

        let Shape::Polygon(subject) =
            crate::io::parse_wkt("POLYGON ((0 9,9 9,9 6,0 9),(3 8.5,4.6 7.49,6 8.5,3 8.5))")
                .unwrap()
        else {
            panic!("expected polygon");
        };
        let Shape::Polygon(rectangle) =
            crate::io::parse_wkt("POLYGON ((-1 -1,10 -1,10 10,-1 10,-1 -1))").unwrap()
        else {
            panic!("expected rectangle");
        };
        let clipped = clip_polygonal_rectangle_exact_paths(
            &[subject],
            &rectangle,
            crate::geometry::Bounds::new_raw(-1.0, -1.0, 10.0, 10.0),
        )
        .unwrap();
        let nine = integer(9).unwrap();
        let paths = clipped
            .into_iter()
            .flatten()
            .map(|ring| {
                ring.into_iter()
                    .map(|[x, y]| Ok(ExactPoint([x, nine.try_subtract(&y)?])))
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let boundaries = [integer(0).unwrap(), nine];
        let anchored = insert_anchors(&paths, &boundaries, &boundaries).unwrap();
        let routed = small_paths(route_paths(&anchored).unwrap());
        let body = routed
            .iter()
            .map(|ring| {
                format!(
                    "({})",
                    ring.iter()
                        .map(|[x, y]| format!("{x} {y}"))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let shape = crate::io::parse_wkt(&format!("POLYGON({body})")).unwrap();
        let shape = if shape.validate().unwrap().is_some() {
            shape
                .repair_preserving_source_vertices(
                    crate::geometry::RepairMethod::Structure,
                    true,
                    &crate::geometry::SourceArcContext::cartesian(),
                )
                .unwrap()
        } else {
            shape
        };
        assert!(shape.validate().unwrap().is_none());
        assert_eq!(shape.area(), 13.0);
        for point in shape.points_vec().unwrap() {
            assert_eq!(point.x, point.x.round());
            assert_eq!(point.y, point.y.round());
        }
    }

    #[test]
    fn hole_near_diagonal_routes_through_shared_grid_events() {
        let rings = [
            path(&[(0.0, 0.0), (9.0, 0.0), (9.0, 3.0), (0.0, 0.0)]),
            path(&[(3.0, 0.5), (4.6, 1.51), (6.0, 0.5), (3.0, 0.5)]),
        ];
        assert_eq!(small_paths(route_paths(&rings).unwrap()), [
            vec![[0, 0], [9, 0], [9, 3], [5, 2], [3, 1], [0, 0]],
            vec![[3, 1], [5, 2], [6, 1], [3, 1]],
        ]);
    }

    #[test]
    fn coverage_routes_shared_border_with_neighbor_hole_events() {
        let rings = [
            path(&[(0.0, 0.0), (8.0, 0.0), (8.0, 4.0), (0.0, 0.0)]),
            path(&[(0.0, 0.0), (8.0, 4.0), (0.0, 4.0), (0.0, 0.0)]),
            path(&[(3.0, 1.6), (3.0, 2.5), (4.0, 2.5), (3.0, 1.6)]),
        ];
        assert_eq!(small_paths(route_paths(&rings).unwrap()), [
            vec![[0, 0], [8, 0], [8, 4], [3, 2], [0, 0]],
            vec![[0, 0], [3, 2], [8, 4], [0, 4], [0, 0]],
            vec![[3, 2], [3, 3], [4, 3], [3, 2]],
        ]);
    }

    #[test]
    fn routed_polygons_reconstruct_valid_integer_areal_results() {
        type PolygonCase = (Vec<Vec<(f64, f64)>>, f64);
        let cases: Vec<PolygonCase> = vec![
            (
                vec![vec![
                    (0.1, 0.1),
                    (4.1, 0.1),
                    (4.1, 0.3),
                    (0.1, 0.3),
                    (0.1, 0.1),
                ]],
                0.0,
            ),
            (
                vec![
                    vec![(0.0, 0.0), (5.0, 0.0), (5.0, 5.0), (0.0, 5.0), (0.0, 0.0)],
                    vec![(0.2, 1.0), (0.2, 2.0), (2.0, 2.0), (2.0, 1.0), (0.2, 1.0)],
                ],
                23.0,
            ),
            (
                vec![
                    vec![(0.0, 0.0), (8.0, 0.0), (8.0, 8.0), (0.0, 8.0), (0.0, 0.0)],
                    vec![(1.0, 1.0), (1.0, 4.0), (3.4, 4.0), (3.4, 1.0), (1.0, 1.0)],
                    vec![
                        (3.49, 1.0),
                        (3.49, 4.0),
                        (6.0, 4.0),
                        (6.0, 1.0),
                        (3.49, 1.0),
                    ],
                ],
                49.0,
            ),
            (
                vec![
                    vec![(0.0, 0.0), (5.0, 0.0), (5.0, 5.0), (0.0, 5.0), (0.0, 0.0)],
                    vec![(1.1, 1.1), (1.1, 1.4), (1.4, 1.4), (1.4, 1.1), (1.1, 1.1)],
                ],
                25.0,
            ),
            (
                vec![vec![(0.0, 0.0), (9.0, 0.0), (9.0, 3.0), (0.0, 0.0)], vec![
                    (3.0, 0.5),
                    (4.6, 1.51),
                    (6.0, 0.5),
                    (3.0, 0.5),
                ]],
                13.0,
            ),
            (
                vec![vec![
                    (0.0, 0.0),
                    (2.0, 0.0),
                    (2.0, 0.8),
                    (4.0, 0.8),
                    (4.0, 0.0),
                    (6.0, 0.0),
                    (6.0, 2.0),
                    (4.0, 2.0),
                    (4.0, 1.2),
                    (2.0, 1.2),
                    (2.0, 2.0),
                    (0.0, 2.0),
                    (0.0, 0.0),
                ]],
                8.0,
            ),
        ];
        for (rings, expected_area) in cases {
            let input = rings.iter().map(|ring| path(ring)).collect::<Vec<_>>();
            let routed = small_paths(route_paths(&input).unwrap());
            let shell = routed.first().unwrap();
            if shell.len() < 4 {
                assert_eq!(expected_area, 0.0);
                continue;
            }
            let body = routed
                .iter()
                .filter(|ring| ring.len() >= 4)
                .map(|ring| {
                    format!(
                        "({})",
                        ring.iter()
                            .map(|[x, y]| format!("{x} {y}"))
                            .collect::<Vec<_>>()
                            .join(",")
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            let shape = crate::io::parse_wkt(&format!("POLYGON({body})")).unwrap();
            let shape = if shape.validate().unwrap().is_some() {
                shape
                    .repair_preserving_source_vertices(
                        crate::geometry::RepairMethod::Structure,
                        true,
                        &crate::geometry::SourceArcContext::cartesian(),
                    )
                    .unwrap()
            } else {
                shape
            };
            assert!(shape.validate().unwrap().is_none());
            assert_eq!(shape.area(), expected_area);
            for point in shape.points_vec().unwrap() {
                assert_eq!(point.x, point.x.round());
                assert_eq!(point.y, point.y.round());
            }
        }
    }
}
