#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
#![expect(
    clippy::self_named_module_files,
    reason = "the cohesive segment kernel predates its exact-arithmetic child module; a mechanical file move adds no ownership boundary"
)]
//! The shared segment-geometry kernel: nearest-point, segment intersection /
//! projection, and interpolation primitives used across distance, overlay,
//! linear-referencing, and predicate modules.

use crate::error::{ErrorKind, Result};
use crate::geometry::exact_arith::{ExactSign, two_sum};
use crate::geometry::tessellation::exact::{
    ExactDyadic, ExactRational, compare_ratios, round_ratio, round_sqrt_ratio,
};
use crate::geometry::{
    Bounds, Coordinates, MOrdinate, Ordering, Point, PointKey, Segment, Unmetered, Work, XY,
    ZOrdinate, axis_pow2_scale, same_point, same_topological_coordinate, scaled_residual,
};

mod exact;

pub(crate) use exact::orientation;
pub(super) use exact::{CCW_ERRBOUND_A, orientation_xy, ray_crossing_is_right};
pub(in crate::geometry) use exact::{
    DdInterval, crossing_homogeneous, crossing_homogeneous_stored, crossing_point,
    crossing_point_with, dd_strict_cmp, stored_cross_sign, stored_orientation_sign,
};
use exact::{certified_projection, dd_interpolate};

fn projection_difference(left: f64, right: f64) -> Result<ExactDyadic> {
    ExactDyadic::from_f64(left).try_subtract(ExactDyadic::from_f64(right))
}

/// Rounded source expression with an outward error radius. The radius covers
/// the source-coordinate differences, products, and final two-term sum.
#[derive(Clone, Copy)]
struct SourcePair {
    value: f64,
    radius: f64,
    left: f64,
    right: f64,
    subtract: bool,
}

impl SourcePair {
    fn new(left: f64, right: f64, subtract: bool) -> Option<Self> {
        if !(left.is_normal() || left == 0.0) || !(right.is_normal() || right == 0.0) {
            return None;
        }
        let value = if subtract { left - right } else { left + right };
        let magnitude = left.abs() + right.abs();
        let radius = (magnitude * (8.0 * (f64::EPSILON / 2.0))).next_up();
        (value.is_finite() && radius.is_finite()).then_some(Self {
            value,
            radius,
            left,
            right,
            subtract,
        })
    }

    fn lower(self) -> f64 {
        (self.value - self.radius).next_down()
    }

    fn upper(self) -> f64 {
        (self.value + self.radius).next_up()
    }

    const fn terms_do_not_cancel(self) -> bool {
        self.left == 0.0
            || self.right == 0.0
            || if self.subtract {
                self.left.is_sign_positive() != self.right.is_sign_positive()
            } else {
                self.left.is_sign_positive() == self.right.is_sign_positive()
            }
    }
}

fn source_difference(left: f64, right: f64) -> Option<f64> {
    let difference = left - right;
    (difference.is_normal()
        || left.to_bits() == right.to_bits()
        || (left.to_bits() << 1 == 0 && right.to_bits() << 1 == 0))
        .then_some(difference)
}

fn source_product(left: f64, right: f64) -> Option<f64> {
    let product = left * right;
    (product.is_normal() || (product == 0.0 && (left == 0.0 || right == 0.0))).then_some(product)
}

#[derive(Clone, Copy)]
struct FilteredInteriorProjection {
    fraction: f64,
    terms_do_not_cancel: bool,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SourceInteriorProjection {
    point: XY,
    segment: Segment,
    fraction: f64,
}

#[derive(Clone, Copy)]
enum ClassifiedSegmentProjection {
    Start,
    FilteredInterior(FilteredInteriorProjection),
    End,
    Declined,
}

enum ExactClassifiedSegmentProjection {
    Start,
    Interior(ExactProjectionTerms),
    End,
}

#[derive(Clone, Debug)]
pub(crate) struct ExactSegmentProjection {
    numerator: ExactDyadic,
    denominator: ExactDyadic,
    fraction: f64,
    // This is an emitted-witness override only; source calculations use N/D.
    emitted_foot: Option<XY>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CertifiedSegmentProjection {
    point: XY,
    segment: Segment,
    fraction: f64,
    foot: XY,
}

impl CertifiedSegmentProjection {
    fn exact(self) -> Result<ExactSegmentProjection> {
        let mut exact = exact_interior_projection(self.point, self.segment)?;
        exact.emitted_foot = Some(self.foot);
        Ok(exact)
    }
}

impl ExactSegmentProjection {
    fn interpolate(&self, start: f64, end: f64) -> Result<f64> {
        let numerator = ExactDyadic::from_f64(start)
            .try_product(&self.denominator)?
            .try_add(projection_difference(end, start)?.try_product(&self.numerator)?)?;
        round_ratio(&numerator, &self.denominator)
    }

    fn scale(&self, value: f64) -> Result<f64> {
        let numerator = ExactDyadic::from_f64(value).try_product(&self.numerator)?;
        round_ratio(&numerator, &self.denominator)
    }

    fn squared_distance(&self, point: XY, segment: Segment) -> Result<ExactSquaredDistance> {
        ExactSquaredDistance::from_projection(point, segment, self)
    }

    fn distance(&self, point: XY, segment: Segment) -> Result<f64> {
        let squared = self.squared_distance(point, segment)?;
        round_sqrt_ratio(&squared.numerator, &squared.denominator)
    }

    #[cfg(test)]
    fn emitted_xy(&self, segment: Segment) -> Result<XY> {
        Ok(match self.emitted_foot {
            Some(foot) => foot,
            None => XY::new(
                self.interpolate(segment.start.x, segment.end.x)?,
                self.interpolate(segment.start.y, segment.end.y)?,
            ),
        })
    }
}

/// The source primitive governing a witness, independent of rounded emission.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum WitnessRole {
    Interior,
    Point,
}

impl WitnessRole {
    pub(crate) fn cmp_tie(
        self,
        incumbent: Self,
        source_order: impl FnOnce() -> Ordering,
    ) -> Ordering {
        self.cmp(&incumbent).then_with(source_order)
    }
}

/// One certified clamped projection. The provenance-carrying variants retain
/// their source ratio until coordinate, distance, and along-distance
/// reconstruction; a rounded `t == 0` or `t == 1` therefore cannot turn a
/// genuine interior foot into an endpoint.
#[derive(Clone, Debug)]
pub(crate) enum SegmentProjection {
    Start,
    Interior(SourceInteriorProjection),
    CertifiedInterior(CertifiedSegmentProjection),
    ExactInterior(Box<ExactSegmentProjection>),
    End,
}

impl SegmentProjection {
    pub(super) const fn is_start(&self) -> bool {
        matches!(self, Self::Start)
    }

    pub(super) const fn is_end(&self) -> bool {
        matches!(self, Self::End)
    }

    #[cfg(test)]
    pub(super) const fn uses_exact_ratio(&self) -> bool {
        matches!(self, Self::CertifiedInterior(_) | Self::ExactInterior(_))
    }

    pub(crate) fn fraction(&self) -> f64 {
        match self {
            Self::Start => 0.0,
            Self::Interior(projection) => projection.fraction,
            Self::CertifiedInterior(projection) => projection.fraction,
            Self::ExactInterior(projection) => projection.fraction,
            Self::End => 1.0,
        }
    }

    pub(in crate::geometry) fn exact_fraction(&self) -> Result<ExactRational> {
        match self {
            Self::Start => Ok(ExactRational::zero()),
            Self::End => Ok(ExactRational::one()),
            Self::Interior(_) | Self::CertifiedInterior(_) | Self::ExactInterior(_) => {
                let (numerator, denominator) = self.exact_ratio()?;
                ExactRational::new(numerator, denominator)
            },
        }
    }

    fn continuum_distance(&self, point: XY, segment: Segment) -> Result<f64> {
        match self {
            Self::Start => Ok(point_distance(point, segment.start)),
            Self::Interior(projection) => {
                source_projection_distance(projection.point, projection.segment)
            },
            Self::CertifiedInterior(projection) => projection.exact()?.distance(point, segment),
            Self::ExactInterior(projection) => projection.distance(point, segment),
            Self::End => Ok(point_distance(point, segment.end)),
        }
    }

    fn continuum_squared(&self, point: XY, segment: Segment) -> Result<f64> {
        match self {
            Self::Start => Ok(point_distance(point, segment.start).powi(2)),
            Self::Interior(projection) => {
                source_projection_squared(projection.point, projection.segment)
            },
            Self::CertifiedInterior(projection) => projection
                .exact()?
                .squared_distance(point, segment)?
                .rounded(),
            Self::ExactInterior(projection) => {
                projection.squared_distance(point, segment)?.rounded()
            },
            Self::End => Ok(point_distance(point, segment.end).powi(2)),
        }
    }

    pub(super) fn cmp_along(&self, other: &Self) -> Result<Ordering> {
        match (self, other) {
            (Self::Start, Self::Start) | (Self::End, Self::End) => Ok(Ordering::Equal),
            (Self::Start, _) | (_, Self::End) => Ok(Ordering::Less),
            (Self::End, _) | (_, Self::Start) => Ok(Ordering::Greater),
            _ => {
                let (left_numerator, left_denominator) = self.exact_ratio()?;
                let (right_numerator, right_denominator) = other.exact_ratio()?;
                compare_ratios(
                    &left_numerator,
                    &left_denominator,
                    &right_numerator,
                    &right_denominator,
                )
            },
        }
    }

    #[cfg(test)]
    pub(super) fn interpolate_xy(&self, segment: Segment) -> Result<XY> {
        Ok(match self {
            Self::Start => segment.start,
            Self::End => segment.end,
            Self::Interior(projection) => {
                exact_interior_projection(projection.point, projection.segment)?
                    .emitted_xy(segment)?
            },
            Self::CertifiedInterior(projection) => projection.foot,
            Self::ExactInterior(projection) => projection.emitted_xy(segment)?,
        })
    }

    fn exact_ratio(&self) -> Result<(ExactDyadic, ExactDyadic)> {
        let projection = match self {
            Self::Interior(projection) => {
                return exact_interior_projection(projection.point, projection.segment)
                    .map(|projection| (projection.numerator, projection.denominator));
            },
            Self::CertifiedInterior(projection) => projection.exact()?,
            Self::ExactInterior(projection) => {
                return Ok((projection.numerator.clone(), projection.denominator.clone()));
            },
            Self::Start | Self::End => unreachable!("endpoint ratio has no interior recipe"),
        };
        Ok((projection.numerator, projection.denominator))
    }

    pub(super) fn interpolate_point(&self, start: Point, end: Point) -> Result<Point> {
        let exact;
        let projection = match self {
            Self::Start => return Ok(start),
            Self::End => return Ok(end),
            Self::Interior(projection) => {
                exact = exact_interior_projection(projection.point, projection.segment)?;
                &exact
            },
            Self::ExactInterior(projection) => projection.as_ref(),
            Self::CertifiedInterior(certified) => {
                exact = certified.exact()?;
                &exact
            },
        };
        let interpolate_optional = |start: Option<f64>, end: Option<f64>| -> Result<Option<f64>> {
            match (start, end) {
                (Some(start), Some(end)) => Ok(Some(projection.interpolate(start, end)?)),
                _ => Ok(None),
            }
        };
        Ok(Point::new_axes(
            projection.interpolate(start.x, end.x)?,
            projection.interpolate(start.y, end.y)?,
            ZOrdinate(interpolate_optional(start.z(), end.z())?),
            MOrdinate(interpolate_optional(start.m(), end.m())?),
        )
        .unwrap_or(if projection.fraction <= 0.5 {
            start
        } else {
            end
        }))
    }

    pub(super) fn scale(&self, value: f64) -> Result<f64> {
        Ok(match self {
            Self::Start => 0.0,
            Self::Interior(projection) => {
                exact_interior_projection(projection.point, projection.segment)?.scale(value)?
            },
            Self::CertifiedInterior(projection) => projection.exact()?.scale(value)?,
            Self::ExactInterior(projection) => projection.scale(value)?,
            Self::End => value,
        })
    }
}

/// Stable sort by a fallible comparison; the first error ends it. Runs of
/// `RUN` items sort by insertion in place. Longer inputs merge those runs as
/// a permutation through two index buffers, then apply it in place, so no item
/// is cloned.
pub(in crate::geometry) fn try_sort_by<T>(
    items: &mut [T],
    mut compare: impl FnMut(&T, &T) -> Result<Ordering>,
) -> Result<()> {
    try_sort_by_with_work(items, &mut Unmetered, |left, right, _work| {
        compare(left, right)
    })
}

/// Stable fallible sort that shares one raw-work account with its comparator.
pub(in crate::geometry) fn try_sort_by_with_work<T, W: Work>(
    items: &mut [T],
    work: &mut W,
    mut compare: impl FnMut(&T, &T, &mut W) -> Result<Ordering>,
) -> Result<()> {
    const RUN: usize = 16;
    for run in items.chunks_mut(RUN) {
        for index in 1..run.len() {
            let mut cursor = index;
            while cursor > 0 {
                let previous = cursor.wrapping_sub(1);
                work.charge(1)?;
                #[expect(
                    clippy::indexing_slicing,
                    reason = "previous and cursor are adjacent indices within this admitted RUN16 slice"
                )]
                let ordering = compare(&run[previous], &run[cursor], work)?;
                if ordering != Ordering::Greater {
                    break;
                }
                work.charge(2)?;
                run.swap(previous, cursor);
                cursor = previous;
            }
        }
    }
    let len = items.len();
    if len <= RUN {
        return Ok(());
    }
    work.charge_product(len, 2)?;
    let mut order = Vec::with_capacity(len);
    let mut merged = Vec::with_capacity(len);
    order.extend(0..len);
    merged.resize(len, 0);
    let mut width = RUN;
    while width < len {
        // Successful allocation of both usize buffers bounds each doubling,
        // including the final width update, below usize::MAX.
        let stride = width.wrapping_mul(2);
        for (source_run, output_run) in order.chunks(stride).zip(merged.chunks_mut(stride)) {
            let left_len = width.min(source_run.len());
            let (mut left, mut right) = source_run.split_at(left_len);
            for slot in output_run {
                // Only a strictly smaller right item overtakes: ties keep
                // their input order. Suffixes also handle the final short run
                // without offset arithmetic.
                if let Some((left_id, left_rest)) = left.split_first() {
                    if let Some((right_id, right_rest)) = right.split_first() {
                        work.charge(1)?;
                        let left_item = items
                            .get(*left_id)
                            .expect("merge ordinal belongs to the admitted item slice");
                        let right_item = items
                            .get(*right_id)
                            .expect("merge ordinal belongs to the admitted item slice");
                        let ordering = compare(left_item, right_item, work)?;
                        if ordering == Ordering::Greater {
                            work.charge(1)?;
                            right = right_rest;
                            *slot = *right_id;
                        } else {
                            work.charge(1)?;
                            left = left_rest;
                            *slot = *left_id;
                        }
                    } else {
                        work.charge(1)?;
                        left = left_rest;
                        *slot = *left_id;
                    }
                } else {
                    let (right_id, right_rest) = right
                        .split_first()
                        .expect("each merge output slot has a remaining run head");
                    work.charge(1)?;
                    right = right_rest;
                    *slot = *right_id;
                }
            }
        }
        std::mem::swap(&mut order, &mut merged);
        width = width.wrapping_mul(2);
    }
    // Position `k` takes the item at `order[k]`; each cycle is walked once.
    for start in 0..len {
        let mut current = start;
        loop {
            #[expect(
                clippy::indexing_slicing,
                reason = "current is admitted by the cycle walk over the same initialized order permutation"
            )]
            let entry = &mut order[current];
            let source = *entry;
            if source == current {
                break;
            }
            work.charge(1)?;
            *entry = current;
            if source == start {
                break;
            }
            work.charge(2)?;
            items.swap(current, source);
            current = source;
        }
    }
    Ok(())
}

/// `Vec::dedup_by` with a fallible sameness test of `(later, earlier)`; the
/// first error ends it.
pub(in crate::geometry) fn try_dedup_by<T>(
    items: &mut Vec<T>,
    mut same: impl FnMut(&T, &T) -> Result<bool>,
) -> Result<()> {
    let mut failure = None;
    items.dedup_by(|later, earlier| {
        failure.is_none()
            && same(later, earlier).unwrap_or_else(|error| {
                failure = Some(error);
                false
            })
    });
    failure.map_or(Ok(()), Err)
}

#[cfg(test)]
#[derive(Clone, Debug)]
enum SegmentWitness {
    Start,
    Projection(SegmentProjection),
    End,
    RoundedIntersection(XY),
}

#[cfg(test)]
impl SegmentWitness {
    fn xy(&self, segment: Segment) -> XY {
        match self {
            Self::Start => segment.start,
            Self::Projection(projection) => projection.interpolate_xy(segment).unwrap(),
            Self::End => segment.end,
            Self::RoundedIntersection(point) => *point,
        }
    }
}

#[cfg(test)]
struct SegmentPairNearest {
    left: SegmentWitness,
    right: SegmentWitness,
    distance_key: SquaredDistanceKey,
}

/// Exact endpoints of a segment's closed intersection with a rectangle.
///
/// `exact` is absent only for an original stored endpoint. Constructed
/// crossings retain their exact homogeneous coordinate; consumers can node the
/// local span without treating its rounded endpoints as new source geometry.
#[derive(Clone, Debug)]
pub(crate) struct SegmentClipPoint {
    pub(crate) point: XY,
    pub(crate) exact: Option<Box<[ExactDyadic; 3]>>,
}

#[derive(Clone, Debug)]
pub(crate) struct SegmentClipInterval {
    pub(crate) start: SegmentClipPoint,
    pub(crate) end: SegmentClipPoint,
}

/// Clip a stored source carrier to a closed rectangle without replacing that
/// carrier by its rounded endpoint chord.
#[expect(
    clippy::indexing_slicing,
    reason = "hit indices are bounded by the fixed six-slot buffer populated from the segment and four rectangle sides"
)]
pub(in crate::geometry) fn clip_segment_rect_interval(
    segment: Segment,
    rect: Bounds,
) -> Result<Option<SegmentClipInterval>> {
    let (start, end) = (segment.start, segment.end);
    if same_point(start, end) {
        return Ok(rect.contains_xy(start).then_some(SegmentClipInterval {
            start: SegmentClipPoint {
                point: start,
                exact: None,
            },
            end: SegmentClipPoint {
                point: start,
                exact: None,
            },
        }));
    }
    if rect.contains_xy(start) && rect.contains_xy(end) {
        return Ok(Some(SegmentClipInterval {
            start: SegmentClipPoint {
                point: start,
                exact: None,
            },
            end: SegmentClipPoint {
                point: end,
                exact: None,
            },
        }));
    }
    let corners = [
        XY::new(rect.minx(), rect.miny()),
        XY::new(rect.maxx(), rect.miny()),
        XY::new(rect.maxx(), rect.maxy()),
        XY::new(rect.minx(), rect.maxy()),
    ];
    let edges = [
        (corners[0], corners[1]),
        (corners[1], corners[2]),
        (corners[2], corners[3]),
        (corners[3], corners[0]),
    ];
    let segment = Segment { start, end };
    let zero = ExactDyadic::from_f64(0.0);
    let one = ExactDyadic::from_f64(1.0);
    let mut hits: [Option<ClipHit>; 6] = [const { None }; 6];
    let mut n_hits = 0_usize;
    let point_in = |p: XY| {
        p.x >= rect.minx() && p.x <= rect.maxx() && p.y >= rect.miny() && p.y <= rect.maxy()
    };
    if point_in(start) {
        push_clip_hit(
            ClipHit {
                parameter: SegmentParameter::Start,
                point: start,
                exact: None,
            },
            &mut hits,
            &mut n_hits,
        )?;
    }
    if point_in(end) {
        push_clip_hit(
            ClipHit {
                parameter: SegmentParameter::End,
                point: end,
                exact: None,
            },
            &mut hits,
            &mut n_hits,
        )?;
    }
    for (edge_start, edge_end) in edges {
        let edge = Segment {
            start: edge_start,
            end: edge_end,
        };
        let orientations = PairOrientations::new(segment, edge);
        if orientations.all_collinear() {
            if let Some(span) = collinear_shared_span_exact(segment, edge) {
                push_clip_hit(
                    ClipHit {
                        parameter: clip_segment_parameter(segment, span.start)?,
                        point: span.start,
                        exact: None,
                    },
                    &mut hits,
                    &mut n_hits,
                )?;
                push_clip_hit(
                    ClipHit {
                        parameter: clip_segment_parameter(segment, span.end)?,
                        point: span.end,
                        exact: None,
                    },
                    &mut hits,
                    &mut n_hits,
                )?;
            }
        } else if orientations.straddles_or_touches() {
            if orientations.has_collinear() {
                if let Some(point) = touch_endpoint(segment, edge, orientations) {
                    push_clip_hit(
                        ClipHit {
                            parameter: clip_segment_parameter(segment, point)?,
                            point,
                            exact: None,
                        },
                        &mut hits,
                        &mut n_hits,
                    )?;
                }
            } else {
                push_clip_hit(
                    clip_segment_edge_cross_exact(segment, edge_start, edge_end)?,
                    &mut hits,
                    &mut n_hits,
                )?;
            }
        }
    }
    if n_hits == 0 {
        return Ok(None);
    }
    let mut i_min = 0_usize;
    let mut i_max = 0_usize;
    for i in 1..n_hits {
        let left = hits[i].as_ref().expect("stored clip hit");
        let min = hits[i_min].as_ref().expect("stored clip hit");
        if left.parameter.cmp(&min.parameter, &zero, &one)?.is_lt() {
            i_min = i;
        }
        let max = hits[i_max].as_ref().expect("stored clip hit");
        if left.parameter.cmp(&max.parameter, &zero, &one)?.is_gt() {
            i_max = i;
        }
    }
    let first = hits[i_min].take().expect("stored clip hit");
    let last = if i_min == i_max {
        first.clone()
    } else {
        hits[i_max].take().expect("stored clip hit")
    };
    Ok(Some(SegmentClipInterval {
        start: SegmentClipPoint {
            point: first.point,
            exact: first.exact.map(Box::new),
        },
        end: SegmentClipPoint {
            point: last.point,
            exact: last.exact.map(Box::new),
        },
    }))
}

#[derive(Clone)]
struct ClipHit {
    parameter: SegmentParameter,
    point: XY,
    exact: Option<[ExactDyadic; 3]>,
}

/// A position on a segment in exact arithmetic: an endpoint, or the interior
/// ratio `num / den` of the way from start to end.
#[derive(Clone, Debug)]
pub(in crate::geometry) enum SegmentParameter {
    Start,
    Ratio { num: ExactDyadic, den: ExactDyadic },
    End,
}

impl SegmentParameter {
    pub(in crate::geometry) fn cmp(
        &self,
        other: &Self,
        zero: &ExactDyadic,
        one: &ExactDyadic,
    ) -> Result<Ordering> {
        match (self, other) {
            (Self::Start, Self::Start) | (Self::End, Self::End) => Ok(Ordering::Equal),
            (Self::Start, Self::End) => Ok(Ordering::Less),
            (Self::End, Self::Start) => Ok(Ordering::Greater),
            (Self::Start, Self::Ratio { num, den }) => compare_ratios(zero, one, num, den),
            (Self::End, Self::Ratio { num, den }) => compare_ratios(one, one, num, den),
            (Self::Ratio { num, den }, Self::Start) => compare_ratios(num, den, zero, one),
            (Self::Ratio { num, den }, Self::End) => compare_ratios(num, den, one, one),
            (
                Self::Ratio {
                    num: left_num,
                    den: left_den,
                },
                Self::Ratio {
                    num: right_num,
                    den: right_den,
                },
            ) => compare_ratios(left_num, left_den, right_num, right_den),
        }
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "hit count is bounded by distinct endpoints of one segment's convex rectangle intersection"
)]
fn push_clip_hit(hit: ClipHit, output: &mut [Option<ClipHit>; 6], count: &mut usize) -> Result<()> {
    let (zero, one) = (ExactDyadic::zero(), ExactDyadic::one());
    for existing in output[..*count].iter().flatten() {
        if existing.parameter.cmp(&hit.parameter, &zero, &one)? == Ordering::Equal {
            return Ok(());
        }
    }
    output[*count] = Some(hit);
    *count = (*count).wrapping_add(1);
    Ok(())
}

pub(in crate::geometry) fn clip_segment_parameter(
    segment: Segment,
    point: XY,
) -> Result<SegmentParameter> {
    if same_point(segment.start, point) {
        return Ok(SegmentParameter::Start);
    }
    if same_point(segment.end, point) {
        return Ok(SegmentParameter::End);
    }
    let use_x = !same_topological_coordinate(segment.start.x, segment.end.x);
    let (point, start, end) = if use_x {
        (point.x, segment.start.x, segment.end.x)
    } else {
        (point.y, segment.start.y, segment.end.y)
    };
    let numerator = ExactDyadic::from_f64(point).try_subtract(ExactDyadic::from_f64(start))?;
    let denominator = ExactDyadic::from_f64(end).try_subtract(ExactDyadic::from_f64(start))?;
    Ok(SegmentParameter::Ratio {
        num: numerator,
        den: denominator,
    })
}

/// Exact source parameter of a proper crossing, without first rounding the
/// crossing coordinates. Linework consumers that order constructed contacts
/// must not derive that order from the emitted binary64 witness.
pub(in crate::geometry) fn exact_crossing_parameter(
    segment: Segment,
    other: Segment,
) -> Result<SegmentParameter> {
    let [x, y, weight] = crossing_homogeneous(segment, other)?;
    let use_x = !same_topological_coordinate(segment.start.x, segment.end.x);
    let (coordinate, start, end) = if use_x {
        (x, segment.start.x, segment.end.x)
    } else {
        (y, segment.start.y, segment.end.y)
    };
    let start = ExactDyadic::from_f64(start);
    let denominator =
        weight.try_product(&ExactDyadic::from_f64(end).try_subtract(start.clone())?)?;
    let numerator = coordinate.try_subtract(weight.try_product(&start)?)?;
    Ok(SegmentParameter::Ratio {
        num: numerator,
        den: denominator,
    })
}

fn clip_segment_edge_cross_exact(
    segment: Segment,
    edge_start: XY,
    edge_end: XY,
) -> Result<ClipHit> {
    debug_assert!(
        same_topological_coordinate(edge_start.x, edge_end.x)
            || same_topological_coordinate(edge_start.y, edge_end.y),
        "rectangle edge must be axis aligned"
    );
    let start_x = ExactDyadic::from_f64(segment.start.x);
    let end_x = ExactDyadic::from_f64(segment.end.x);
    let start_y = ExactDyadic::from_f64(segment.start.y);
    let end_y = ExactDyadic::from_f64(segment.end.y);
    let delta_x = end_x.try_subtract(start_x.clone())?;
    let delta_y = end_y.try_subtract(start_y.clone())?;
    if same_topological_coordinate(edge_start.x, edge_end.x) {
        let edge_x = ExactDyadic::from_f64(edge_start.x);
        let offset = edge_x.clone().try_subtract(start_x)?;
        let mut denominator = delta_x.clone();
        let mut numerator = start_y
            .try_product(&delta_x)?
            .try_add(delta_y.try_product(&offset)?)?;
        if denominator.sign() == ExactSign::Negative {
            denominator = denominator.neg();
            numerator = numerator.neg();
        }
        let x = edge_x.try_product(&denominator)?;
        let free = round_ratio(&numerator, &denominator)?;
        Ok(ClipHit {
            parameter: SegmentParameter::Ratio {
                num: offset,
                den: delta_x,
            },
            point: XY::new(edge_start.x, free),
            exact: Some([x, numerator, denominator]),
        })
    } else {
        let edge_y = ExactDyadic::from_f64(edge_start.y);
        let offset = edge_y.clone().try_subtract(start_y)?;
        let mut denominator = delta_y.clone();
        let mut numerator = start_x
            .try_product(&delta_y)?
            .try_add(delta_x.try_product(&offset)?)?;
        if denominator.sign() == ExactSign::Negative {
            denominator = denominator.neg();
            numerator = numerator.neg();
        }
        let y = edge_y.try_product(&denominator)?;
        let free = round_ratio(&numerator, &denominator)?;
        Ok(ClipHit {
            parameter: SegmentParameter::Ratio {
                num: offset,
                den: delta_y,
            },
            point: XY::new(free, edge_start.y),
            exact: Some([numerator, y, denominator]),
        })
    }
}

#[cfg(test)]
pub(super) fn segment_nearest_points(left: Segment, right: Segment) -> Result<(XY, XY)> {
    let nearest = segment_pair_nearest(left, right)?;
    Ok((nearest.left.xy(left), nearest.right.xy(right)))
}

#[cfg(test)]
fn segment_pair_nearest(left: Segment, right: Segment) -> Result<SegmentPairNearest> {
    // One contact computation, not an `intersects` probe followed by a
    // separate placement. When the pair meets, the shared point IS the
    // nearest pair; otherwise fall through to the projection feet below.
    if let Some(point) = segment_contact_point(left, right)? {
        return Ok(SegmentPairNearest {
            left: SegmentWitness::RoundedIntersection(point),
            right: SegmentWitness::RoundedIntersection(point),
            distance_key: point_pair_distance_key(point, point),
        });
    }

    let left_start = segment_projection(left.start, right)?;
    let left_end = segment_projection(left.end, right)?;
    let right_start = segment_projection(right.start, left)?;
    let right_end = segment_projection(right.end, left)?;
    let candidates = [
        SegmentPairNearest {
            left: SegmentWitness::Start,
            right: SegmentWitness::Projection(left_start),
            distance_key: point_segment_distance_key(left.start, right)?,
        },
        SegmentPairNearest {
            left: SegmentWitness::End,
            right: SegmentWitness::Projection(left_end),
            distance_key: point_segment_distance_key(left.end, right)?,
        },
        SegmentPairNearest {
            left: SegmentWitness::Projection(right_start),
            right: SegmentWitness::Start,
            distance_key: point_segment_distance_key(right.start, left)?,
        },
        SegmentPairNearest {
            left: SegmentWitness::Projection(right_end),
            right: SegmentWitness::End,
            distance_key: point_segment_distance_key(right.end, left)?,
        },
    ];
    let mut candidates = candidates.into_iter();
    let mut best = candidates.next().expect("four segment-pair candidates");
    for candidate in candidates {
        if candidate.distance_key.cmp(&best.distance_key)?.is_lt() {
            best = candidate;
        }
    }
    Ok(best)
}

/// Return a binary64 witness when the stored segments contact, or `None` when
/// they are disjoint. Contact classification is exact; touches return stored
/// endpoints and collinear overlaps return a stored endpoint of the shared
/// interval. Proper crossings are rounded to nearest-even, so the emitted
/// coordinates need not lie exactly on either carrier.
pub(super) fn segment_contact_point(left: Segment, right: Segment) -> Result<Option<XY>> {
    let contact = segment_contact_exact(left, right);
    Ok(match contact {
        Contact::Cross => Some(crossing_point(left, right)?.point),
        Contact::Touch(endpoint) => Some(endpoint.point(left, right)),
        // A positive-length overlap has no single crossing; its near end is a
        // real point of both segments and is the conventional witness.
        Contact::Collinear => contact.shared_span(left, right).map(|span| span.start),
        Contact::Disjoint => None,
    })
}

/// Certified clamped point-to-segment projection over the complete finite
/// binary64 domain. The outward interval is an allocation-free proof for the
/// common case. Any unresolved overlap, underflow, overflow, or cancellation
/// falls through to the exact stored-dyadic numerator/denominator owner.
pub(crate) fn segment_projection(
    point: impl Into<XY>,
    segment: Segment,
) -> Result<SegmentProjection> {
    let point = point.into();
    projection_from_classification(point, segment, classify_segment_projection(point, segment))
}

// The compact decide/decline filter handles ordinary projections; unresolved
// cases continue through the exact fallback.
fn classify_segment_projection(point: XY, segment: Segment) -> ClassifiedSegmentProjection {
    let Some(dx) = source_difference(segment.end.x, segment.start.x) else {
        return ClassifiedSegmentProjection::Declined;
    };
    let Some(dy) = source_difference(segment.end.y, segment.start.y) else {
        return ClassifiedSegmentProjection::Declined;
    };
    if dx == 0.0 && dy == 0.0 {
        return ClassifiedSegmentProjection::Start;
    }
    let Some(qx) = source_difference(point.x, segment.start.x) else {
        return ClassifiedSegmentProjection::Declined;
    };
    let Some(qy) = source_difference(point.y, segment.start.y) else {
        return ClassifiedSegmentProjection::Declined;
    };
    let Some(numerator_left) = source_product(qx, dx) else {
        return ClassifiedSegmentProjection::Declined;
    };
    let Some(numerator_right) = source_product(qy, dy) else {
        return ClassifiedSegmentProjection::Declined;
    };
    let Some(denominator_left) = source_product(dx, dx) else {
        return ClassifiedSegmentProjection::Declined;
    };
    let Some(denominator_right) = source_product(dy, dy) else {
        return ClassifiedSegmentProjection::Declined;
    };
    let Some(numerator) = SourcePair::new(numerator_left, numerator_right, false) else {
        return ClassifiedSegmentProjection::Declined;
    };
    let Some(denominator) = SourcePair::new(denominator_left, denominator_right, false) else {
        return ClassifiedSegmentProjection::Declined;
    };

    if numerator.upper() < 0.0 {
        return ClassifiedSegmentProjection::Start;
    }
    if numerator.lower() > denominator.upper() {
        return ClassifiedSegmentProjection::End;
    }
    if numerator.lower() > 0.0 && numerator.upper() < denominator.lower() {
        return classify_filtered_interior(segment, numerator, denominator);
    }
    ClassifiedSegmentProjection::Declined
}

/// Ratio and witness certification is reached only after both endpoint signs
/// have been decided as strictly interior.
fn classify_filtered_interior(
    segment: Segment,
    numerator: SourcePair,
    denominator: SourcePair,
) -> ClassifiedSegmentProjection {
    let fraction = numerator.value / denominator.value;
    if fraction.is_finite() && fraction > 0.0 && fraction < 1.0 && fraction.is_normal() {
        let foot = interpolate_segment_point(segment.start, segment.end, fraction);
        if !same_point(foot, segment.start) && !same_point(foot, segment.end) {
            return ClassifiedSegmentProjection::FilteredInterior(FilteredInteriorProjection {
                fraction,
                terms_do_not_cancel: numerator.terms_do_not_cancel(),
            });
        }
    }
    ClassifiedSegmentProjection::Declined
}

/// The interval-filter decline uses exact stored-dyadic arithmetic.
fn classify_segment_projection_exact(
    point: XY,
    segment: Segment,
) -> Result<ExactClassifiedSegmentProjection> {
    let terms = exact_projection_terms(point, segment)?;
    if terms.denominator.is_zero() {
        return Ok(ExactClassifiedSegmentProjection::Start);
    }
    if terms.numerator.sign() != ExactSign::Positive {
        return Ok(ExactClassifiedSegmentProjection::Start);
    }
    if terms.numerator.cmp(&terms.denominator) != Ordering::Less {
        return Ok(ExactClassifiedSegmentProjection::End);
    }
    Ok(ExactClassifiedSegmentProjection::Interior(terms))
}

fn projection_from_classification(
    point: XY,
    segment: Segment,
    classification: ClassifiedSegmentProjection,
) -> Result<SegmentProjection> {
    let (terms, historical_fraction) = match classification {
        ClassifiedSegmentProjection::Start => return Ok(SegmentProjection::Start),
        ClassifiedSegmentProjection::End => return Ok(SegmentProjection::End),
        ClassifiedSegmentProjection::FilteredInterior(filtered) if filtered.terms_do_not_cancel => {
            return Ok(SegmentProjection::Interior(SourceInteriorProjection {
                point,
                segment,
                fraction: filtered.fraction,
            }));
        },
        ClassifiedSegmentProjection::FilteredInterior(filtered) => {
            if let Some((fraction, foot)) = certified_projection(point, segment, filtered.fraction)
            {
                return Ok(SegmentProjection::CertifiedInterior(
                    CertifiedSegmentProjection {
                        point,
                        segment,
                        fraction,
                        foot,
                    },
                ));
            }
            (
                exact_projection_terms(point, segment)?,
                Some(filtered.fraction),
            )
        },
        ClassifiedSegmentProjection::Declined => {
            match classify_segment_projection_exact(point, segment)? {
                ExactClassifiedSegmentProjection::Start => return Ok(SegmentProjection::Start),
                ExactClassifiedSegmentProjection::End => return Ok(SegmentProjection::End),
                ExactClassifiedSegmentProjection::Interior(terms) => (terms, None),
            }
        },
    };
    let fraction = round_ratio(&terms.numerator, &terms.denominator)?;
    let emitted_foot = historical_fraction
        .filter(|historical| historical.to_bits() == fraction.to_bits())
        .map(|historical| interpolate_segment_point(segment.start, segment.end, historical));
    Ok(SegmentProjection::ExactInterior(Box::new(
        ExactSegmentProjection {
            numerator: terms.numerator,
            denominator: terms.denominator,
            fraction,
            emitted_foot,
        },
    )))
}

fn exact_interior_projection(point: XY, segment: Segment) -> Result<ExactSegmentProjection> {
    let terms = exact_projection_terms(point, segment)?;
    debug_assert!(!terms.denominator.is_zero());
    debug_assert_eq!(terms.numerator.sign(), ExactSign::Positive);
    debug_assert!(terms.numerator.cmp(&terms.denominator).is_lt());
    let fraction = round_ratio(&terms.numerator, &terms.denominator)?;
    Ok(ExactSegmentProjection {
        numerator: terms.numerator,
        denominator: terms.denominator,
        fraction,
        emitted_foot: None,
    })
}

struct ExactProjectionTerms {
    numerator: ExactDyadic,
    denominator: ExactDyadic,
    determinant: ExactDyadic,
}

fn exact_projection_terms(point: XY, segment: Segment) -> Result<ExactProjectionTerms> {
    let dx = projection_difference(segment.end.x, segment.start.x)?;
    let dy = projection_difference(segment.end.y, segment.start.y)?;
    let qx = projection_difference(point.x, segment.start.x)?;
    let qy = projection_difference(point.y, segment.start.y)?;
    let numerator = qx.try_product(&dx)?.try_add(qy.try_product(&dy)?)?;
    let denominator = dx.try_product(&dx)?.try_add(dy.try_product(&dy)?)?;
    let determinant = dx.try_product(&qy)?.try_subtract(dy.try_product(&qx)?)?;
    Ok(ExactProjectionTerms {
        numerator,
        denominator,
        determinant,
    })
}

struct ExactSquaredDistance {
    numerator: ExactDyadic,
    denominator: ExactDyadic,
}

impl ExactSquaredDistance {
    fn from_operands(determinant: &ExactDyadic, denominator: &ExactDyadic) -> Result<Self> {
        Ok(Self {
            numerator: determinant.try_product(determinant)?,
            denominator: denominator.clone(),
        })
    }

    fn interior(terms: &ExactProjectionTerms) -> Result<Self> {
        Self::from_operands(&terms.determinant, &terms.denominator)
    }

    fn from_projection(
        point: XY,
        segment: Segment,
        _projection: &ExactSegmentProjection,
    ) -> Result<Self> {
        let terms = exact_projection_terms(point, segment)?;
        Self::from_operands(&terms.determinant, &terms.denominator)
    }

    fn rounded(&self) -> Result<f64> {
        match round_ratio(&self.numerator, &self.denominator) {
            Ok(rounded) => Ok(rounded),
            Err(error) if matches!(error.kind(), ErrorKind::Resource(_)) => Err(error),
            Err(_) => Ok(f64::INFINITY),
        }
    }

    fn point_pair(left: XY, right: XY) -> Result<Self> {
        let dx = projection_difference(left.x, right.x)?;
        let dy = projection_difference(left.y, right.y)?;
        Ok(Self {
            numerator: dx.try_product(&dx)?.try_add(dy.try_product(&dy)?)?,
            denominator: ExactDyadic::from_f64(1.0),
        })
    }

    fn cmp(&self, other: &Self) -> Result<Ordering> {
        compare_ratios(
            &self.numerator,
            &self.denominator,
            &other.numerator,
            &other.denominator,
        )
    }

    fn rounded_trustworthy(&self) -> Result<Option<f64>> {
        match round_ratio(&self.numerator, &self.denominator) {
            Ok(rounded) => Ok(if self.numerator.is_zero() {
                Some(0.0)
            } else {
                rounded.is_normal().then_some(rounded)
            }),
            Err(error) if matches!(error.kind(), ErrorKind::Resource(_)) => Err(error),
            Err(_) => Ok(None),
        }
    }
}

fn source_dd_terms(point: XY, segment: Segment) -> Option<(DdInterval, DdInterval, DdInterval)> {
    let dx = DdInterval::difference(segment.end.x, segment.start.x)?;
    let dy = DdInterval::difference(segment.end.y, segment.start.y)?;
    let qx = DdInterval::difference(point.x, segment.start.x)?;
    let qy = DdInterval::difference(point.y, segment.start.y)?;
    let denominator = dx.square()?.add(dy.square()?)?;
    let numerator = qx.mul(dx)?.add(qy.mul(dy)?)?;
    let determinant = dx.mul(qy)?.sub(dy.mul(qx)?)?;
    Some((numerator, denominator, determinant))
}

fn exact_source_squared(point: XY, segment: Segment) -> Result<ExactSquaredDistance> {
    ExactSquaredDistance::interior(&exact_projection_terms(point, segment)?)
}

fn source_fast_interior_measurement(
    point: XY,
    segment: Segment,
) -> Option<(f64, SquaredDistanceFilter)> {
    let dx = source_difference(segment.end.x, segment.start.x)?;
    let dy = source_difference(segment.end.y, segment.start.y)?;
    let qx = source_difference(point.x, segment.start.x)?;
    let qy = source_difference(point.y, segment.start.y)?;
    let determinant_left = source_product(dx, qy)?;
    let determinant_right = source_product(dy, qx)?;
    let denominator_left = source_product(dx, dx)?;
    let denominator_right = source_product(dy, dy)?;
    let determinant = SourcePair::new(determinant_left, determinant_right, true)?;
    let denominator = SourcePair::new(denominator_left, denominator_right, false)?;
    // Equal-sign determinant products cancel. Let the DD tier retain their
    // original subtraction tails; opposite signs and exact zero terms are
    // the cheap source-aware path.
    if !(determinant.terms_do_not_cancel() || determinant.left == 0.0 && determinant.right == 0.0) {
        return None;
    }
    if determinant.value == 0.0 && !(determinant.left == 0.0 && determinant.right == 0.0) {
        return None;
    }
    if denominator.lower() <= 0.0 {
        return None;
    }
    let distance = determinant.value.abs() / denominator.value.sqrt();
    let squared = distance * distance;
    squared_norm_is_trustworthy(squared, determinant.value == 0.0).then_some((
        squared,
        SquaredDistanceFilter::source_squared(squared, 16.0 * (f64::EPSILON / 2.0)),
    ))
}

fn source_interior_measurement(
    point: XY,
    segment: Segment,
) -> Option<(f64, SquaredDistanceFilter)> {
    if let Some(measurement) = source_fast_interior_measurement(point, segment) {
        return Some(measurement);
    }
    let (_numerator, denominator, determinant) = source_dd_terms(point, segment)?;
    if denominator.is_exact_zero() {
        return None;
    }
    if determinant.is_exact_zero() {
        return Some((
            0.0,
            SquaredDistanceFilter::source_squared(0.0, 16.0 * (f64::EPSILON / 2.0)),
        ));
    }
    let determinant = determinant.rounded_common()?;
    let denominator = denominator.rounded_common()?;
    if determinant == 0.0 || !denominator.is_sign_positive() {
        return None;
    }
    let distance = determinant.abs() / denominator.sqrt();
    let squared = distance * distance;
    if !squared_norm_is_trustworthy(squared, false) {
        return None;
    }
    Some((
        squared,
        SquaredDistanceFilter::source_squared(squared, 8.0 * (f64::EPSILON / 2.0)),
    ))
}

fn source_projection_distance(point: XY, segment: Segment) -> Result<f64> {
    if let Some((squared, _)) = source_interior_measurement(point, segment) {
        return Ok(squared.sqrt());
    }
    let squared = exact_source_squared(point, segment)?;
    round_sqrt_ratio(&squared.numerator, &squared.denominator)
}

fn source_projection_squared(point: XY, segment: Segment) -> Result<f64> {
    if let Some((squared, _)) = source_interior_measurement(point, segment) {
        return Ok(squared);
    }
    exact_source_squared(point, segment)?.rounded()
}

enum PointSegmentMetric {
    Squared {
        measurement_squared: f64,
        filter: SquaredDistanceFilter,
        witness_kind: PointSegmentWitnessKind,
        projection: SelectionProjectionSource,
    },
    Projection(SegmentProjection),
}

#[derive(Clone)]
enum SelectionProjectionSource {
    Classified(ClassifiedSegmentProjection),
    Ready(SegmentProjection),
}

pub(crate) struct PointSegmentSelection {
    distance: f64,
    pub(super) distance_squared: f64,
    filter: SquaredDistanceFilter,
    witness_kind: PointSegmentWitnessKind,
    projection: SelectionProjectionSource,
}

#[derive(Clone, Copy)]
enum PointSegmentWitnessKind {
    Start,
    Interior,
    End,
}

impl PointSegmentSelection {
    pub(crate) const fn distance(&self) -> f64 {
        self.distance
    }

    pub(crate) fn projection(&self, point: XY, segment: Segment) -> Result<SegmentProjection> {
        match &self.projection {
            SelectionProjectionSource::Classified(classification) => {
                projection_from_classification(point, segment, *classification)
            },
            SelectionProjectionSource::Ready(projection) => match projection {
                SegmentProjection::ExactInterior(inner) => Ok(SegmentProjection::ExactInterior(
                    Box::new(ExactSegmentProjection {
                        numerator: inner.numerator.clone(),
                        denominator: inner.denominator.clone(),
                        fraction: inner.fraction,
                        emitted_foot: inner.emitted_foot,
                    }),
                )),
                other @ (SegmentProjection::Start
                | SegmentProjection::Interior(_)
                | SegmentProjection::CertifiedInterior(_)
                | SegmentProjection::End) => Ok(other.clone()),
            },
        }
    }

    const fn witness(&self, point: XY, segment: Segment) -> ExactDistanceWitness {
        match self.witness_kind {
            PointSegmentWitnessKind::Start => ExactDistanceWitness::Points(point, segment.start),
            PointSegmentWitnessKind::Interior => ExactDistanceWitness::Projected(point, segment),
            PointSegmentWitnessKind::End => ExactDistanceWitness::Points(point, segment.end),
        }
    }

    pub(crate) const fn distance_key(&self, point: XY, segment: Segment) -> SquaredDistanceKey {
        self.filter.bind(self.witness(point, segment))
    }

    pub(crate) fn precedes(
        &self,
        point: XY,
        segment: Segment,
        incumbent: &SquaredDistanceKey,
        tie: impl FnOnce() -> bool,
    ) -> Result<bool> {
        self.filter.disjoint_cmp(incumbent.filter).map_or_else(
            || {
                let ordering = exact_witness_cmp_with_identity(
                    self.witness(point, segment),
                    incumbent.witness,
                )?;
                Ok(ordering.is_lt() || ordering.is_eq() && tie())
            },
            |ordering| Ok(ordering.is_lt()),
        )
    }
}

/// The single projection owner classifies every clamped parameter before a
/// metric consumes it. The measurement and selection key both use the source
/// determinant; projection emission is a later witness operation.
fn point_segment_metric(point: XY, segment: Segment) -> Result<PointSegmentMetric> {
    let classification = classify_segment_projection(point, segment);
    if matches!(&classification, ClassifiedSegmentProjection::Start) {
        return Ok(try_point_distance_squared(point, segment.start).map_or(
            PointSegmentMetric::Projection(SegmentProjection::Start),
            |squared| PointSegmentMetric::Squared {
                measurement_squared: squared,
                filter: SquaredDistanceFilter::point_pair(squared),
                witness_kind: PointSegmentWitnessKind::Start,
                projection: SelectionProjectionSource::Classified(classification),
            },
        ));
    }
    if matches!(&classification, ClassifiedSegmentProjection::End) {
        return Ok(try_point_distance_squared(point, segment.end).map_or(
            PointSegmentMetric::Projection(SegmentProjection::End),
            |squared| PointSegmentMetric::Squared {
                measurement_squared: squared,
                filter: SquaredDistanceFilter::point_pair(squared),
                witness_kind: PointSegmentWitnessKind::End,
                projection: SelectionProjectionSource::Classified(classification),
            },
        ));
    }
    Ok(match classification {
        ClassifiedSegmentProjection::FilteredInterior(filtered) => {
            let projection = projection_from_classification(
                point,
                segment,
                ClassifiedSegmentProjection::FilteredInterior(filtered),
            )?;
            if let Some((measurement_squared, filter)) = source_interior_measurement(point, segment)
            {
                PointSegmentMetric::Squared {
                    measurement_squared,
                    filter,
                    witness_kind: PointSegmentWitnessKind::Interior,
                    projection: SelectionProjectionSource::Ready(projection),
                }
            } else {
                PointSegmentMetric::Projection(projection)
            }
        },
        ClassifiedSegmentProjection::Declined => return point_segment_metric_exact(point, segment),
        ClassifiedSegmentProjection::Start | ClassifiedSegmentProjection::End => unreachable!(),
    })
}

pub(in crate::geometry) fn point_segment_distance_and_projection(
    point: XY,
    segment: Segment,
) -> Result<(f64, SegmentProjection)> {
    match point_segment_metric(point, segment)? {
        PointSegmentMetric::Squared {
            measurement_squared,
            projection,
            ..
        } => {
            let projection = match projection {
                SelectionProjectionSource::Classified(classification) => {
                    projection_from_classification(point, segment, classification)?
                },
                SelectionProjectionSource::Ready(projection) => projection,
            };
            Ok((measurement_squared.sqrt(), projection))
        },
        PointSegmentMetric::Projection(projection) => {
            let distance = projection.continuum_distance(point, segment)?;
            Ok((distance, projection))
        },
    }
}

/// Exact classification and squared-ratio rounding handle filter declines.
fn point_segment_metric_exact(point: XY, segment: Segment) -> Result<PointSegmentMetric> {
    Ok(match classify_segment_projection_exact(point, segment)? {
        ExactClassifiedSegmentProjection::Start => try_point_distance_squared(point, segment.start)
            .map_or(
                PointSegmentMetric::Projection(SegmentProjection::Start),
                |squared| PointSegmentMetric::Squared {
                    measurement_squared: squared,
                    filter: SquaredDistanceFilter::unbounded(),
                    witness_kind: PointSegmentWitnessKind::Start,
                    projection: SelectionProjectionSource::Ready(SegmentProjection::Start),
                },
            ),
        ExactClassifiedSegmentProjection::End => try_point_distance_squared(point, segment.end)
            .map_or(
                PointSegmentMetric::Projection(SegmentProjection::End),
                |squared| PointSegmentMetric::Squared {
                    measurement_squared: squared,
                    filter: SquaredDistanceFilter::unbounded(),
                    witness_kind: PointSegmentWitnessKind::End,
                    projection: SelectionProjectionSource::Ready(SegmentProjection::End),
                },
            ),
        ExactClassifiedSegmentProjection::Interior(terms) => {
            let squared = ExactSquaredDistance::interior(&terms)?;
            let rounded = squared.rounded_trustworthy()?;
            let fraction = round_ratio(&terms.numerator, &terms.denominator)?;
            let projection = SegmentProjection::ExactInterior(Box::new(ExactSegmentProjection {
                numerator: terms.numerator,
                denominator: terms.denominator,
                fraction,
                emitted_foot: None,
            }));
            match rounded {
                Some(measurement_squared) => PointSegmentMetric::Squared {
                    measurement_squared,
                    filter: SquaredDistanceFilter::source_squared(
                        measurement_squared,
                        2.0 * (f64::EPSILON / 2.0),
                    ),
                    witness_kind: PointSegmentWitnessKind::Interior,
                    projection: SelectionProjectionSource::Ready(projection),
                },
                None => PointSegmentMetric::Projection(projection),
            }
        },
    })
}

#[derive(Clone, Copy)]
pub(super) enum ExactDistanceWitness {
    Points(XY, XY),
    Projected(XY, Segment),
    Clamped(XY, Segment),
}

/// Order-faithful key for one nearest-witness candidate.
///
/// Distance measures the source continuum, while emitted nearest/shortest-line
/// points are faithful stored witnesses. A projected key retains the original
/// point and segment, so an emitted foot can never change its ordering.
/// Conservative source-distance intervals decide disjoint candidates cheaply;
/// every overlap goes to the stored-dyadic owner, never to a bare float `<`.
#[derive(Clone, Copy)]
pub(crate) struct SquaredDistanceKey {
    filter: SquaredDistanceFilter,
    witness: ExactDistanceWitness,
}

#[derive(Clone, Copy)]
struct SquaredDistanceFilter {
    lo: f64,
    hi: f64,
}

impl SquaredDistanceFilter {
    /// A point-pair key has four error-bearing stages (coordinate residuals,
    /// their squares, and the sum). `gamma(4)` bounds their normal relative
    /// error; the final one-ulp widening absorbs the bound arithmetic itself.
    fn point_pair(squared: f64) -> Self {
        const UNIT_ROUNDOFF: f64 = f64::EPSILON / 2.0;
        const GAMMA_4: f64 = 4.0 * UNIT_ROUNDOFF / (1.0 - 4.0 * UNIT_ROUNDOFF);
        Self::source_squared(squared, GAMMA_4)
    }

    fn source_squared(squared: f64, relative_error: f64) -> Self {
        if !squared.is_finite() {
            return Self::unbounded();
        }
        let error = squared * relative_error;
        let lo = (squared - error).next_down().max(0.0);
        let hi = (squared + error).next_up();
        if hi.is_finite() {
            Self { lo, hi }
        } else {
            Self::unbounded()
        }
    }

    const fn unbounded() -> Self {
        Self {
            lo: f64::NEG_INFINITY,
            hi: f64::INFINITY,
        }
    }

    const fn bind(self, witness: ExactDistanceWitness) -> SquaredDistanceKey {
        SquaredDistanceKey {
            filter: self,
            witness,
        }
    }

    fn disjoint_cmp(self, other: Self) -> Option<Ordering> {
        if self.hi < other.lo {
            Some(Ordering::Less)
        } else if other.hi < self.lo {
            Some(Ordering::Greater)
        } else {
            None
        }
    }
}

impl SquaredDistanceKey {
    pub(in crate::geometry) fn cmp_distance(&self, other: &Self) -> Result<Ordering> {
        if self.witness.same_distance_as(other.witness) {
            return Ok(Ordering::Equal);
        }
        if let Some(order) = self.filter.disjoint_cmp(other.filter) {
            return Ok(order);
        }
        exact_selection_distance_cmp(self.witness, other.witness)
    }

    pub(super) fn is_exact_zero(&self) -> Result<bool> {
        // The source-distance filter's positive lower endpoint certifies
        // nonzero without materializing its exact dyadic witness.
        if self.filter.lo > 0.0 {
            return Ok(false);
        }
        Ok(self.witness.squared()?.numerator.is_zero())
    }

    pub(super) fn cmp(&self, other: &Self) -> Result<Ordering> {
        if self.filter.hi < other.filter.lo {
            return Ok(Ordering::Less);
        }
        if other.filter.hi < self.filter.lo {
            return Ok(Ordering::Greater);
        }
        exact_witness_cmp_with_identity(self.witness, other.witness)
    }

    pub(super) const fn upper_bound(&self) -> f64 {
        self.filter.hi
    }

    pub(super) fn distance_upper_bound(&self) -> Result<f64> {
        let value = if self.filter.hi.is_finite() {
            self.filter.hi.sqrt()
        } else {
            let exact = self.witness.squared()?;
            match round_sqrt_ratio(&exact.numerator, &exact.denominator) {
                Ok(value) => value,
                Err(error) if matches!(error.kind(), ErrorKind::Resource(_)) => return Err(error),
                Err(_) => f64::INFINITY,
            }
        };
        Ok((value * (1.0 + 16.0 * f64::EPSILON) + f64::from_bits(8)).next_up())
    }

    #[cfg(test)]
    fn contains_exact(self) -> bool {
        if !self.filter.lo.is_finite() || !self.filter.hi.is_finite() {
            return true;
        }
        let exact = self.witness.squared().unwrap();
        let denominator = ExactDyadic::from_f64(1.0);
        let lo = ExactSquaredDistance {
            numerator: ExactDyadic::from_f64(self.filter.lo),
            denominator: denominator.clone(),
        };
        let hi = ExactSquaredDistance {
            numerator: ExactDyadic::from_f64(self.filter.hi),
            denominator,
        };
        !lo.cmp(&exact).unwrap().is_gt() && !hi.cmp(&exact).unwrap().is_lt()
    }

    #[cfg(test)]
    const fn is_bounded(self) -> bool {
        self.filter.lo.is_finite() && self.filter.hi.is_finite()
    }
}

fn source_dot_difference_interval(normal: [f64; 2], left: XY, right: XY) -> Option<DdInterval> {
    let [nx, ny] = normal;
    let dx = DdInterval::difference(left.x, right.x)?;
    let dy = DdInterval::difference(left.y, right.y)?;
    DdInterval::exact(nx)
        .mul(dx)?
        .add(DdInterval::exact(ny).mul(dy)?)
}

fn source_dot_difference_exact(normal: [f64; 2], left: XY, right: XY) -> Result<ExactDyadic> {
    let [nx, ny] = normal;
    let dx = projection_difference(left.x, right.x)?;
    let dy = projection_difference(left.y, right.y)?;
    ExactDyadic::from_f64(nx)
        .try_product(&dx)?
        .try_add(ExactDyadic::from_f64(ny).try_product(&dy)?)
}

pub(crate) fn source_dot_cmp(normal: [f64; 2], left: XY, right: XY) -> Result<Ordering> {
    if let Some(difference) = source_dot_difference_interval(normal, left, right) {
        if difference.is_exact_zero() {
            return Ok(Ordering::Equal);
        }
        if let Some(order) = dd_strict_cmp(difference, DdInterval::exact(0.0)) {
            return Ok(order);
        }
    }
    #[cfg(test)]
    CLEARANCE_STRIP_SUPPORT_FALLBACKS.with(|count| count.set(count.get().saturating_add(1)));
    Ok(source_dot_difference_exact(normal, left, right)?.cmp(&ExactDyadic::zero()))
}

/// `Some(true)` certifies pruning, `Some(false)` certifies retaining the node,
/// and `None` requires the exact source-ratio comparison.
fn clearance_strip_filter_result(
    point: XY,
    normal: [f64; 2],
    low: XY,
    high: XY,
    incumbent: &SquaredDistanceKey,
) -> Option<bool> {
    let low_gap = source_dot_difference_interval(normal, low, point)?;
    let high_gap = source_dot_difference_interval(normal, point, high)?;
    let (low_lower, low_upper) = low_gap.outward_bounds();
    let (high_lower, high_upper) = high_gap.outward_bounds();
    if low_upper.is_finite() && high_upper.is_finite() && low_upper <= 0.0 && high_upper <= 0.0 {
        return Some(false);
    }
    let gap_lower = low_lower.max(high_lower).max(0.0);
    let gap_upper = low_upper.max(high_upper).max(0.0);
    if !gap_lower.is_finite() || !gap_upper.is_finite() {
        return None;
    }

    let [nx, ny] = normal;
    let norm_squared = DdInterval::exact(nx)
        .square()
        .and_then(|x| DdInterval::exact(ny).square().and_then(|y| x.add(y)))?;
    let (norm_lower, norm_upper) = norm_squared.outward_bounds();
    if !norm_lower.is_finite() || !norm_upper.is_finite() || norm_upper <= 0.0 {
        return None;
    }
    let key_lower = incumbent.filter.lo;
    let key_upper = incumbent.filter.hi;
    // A lower strip bound above the key's upper enclosure proves pruning.
    if gap_lower > 0.0
        && key_upper.is_finite()
        && key_upper >= 0.0
        && let (Some(bound_numerator), Some(incumbent_upper_product)) = (
            DdInterval::exact(gap_lower).square(),
            DdInterval::exact(key_upper).mul(DdInterval::exact(norm_upper)),
        )
        && dd_strict_cmp(bound_numerator, incumbent_upper_product).is_some_and(Ordering::is_gt)
    {
        return Some(true);
    }
    // An upper strip bound below the key's lower enclosure proves no-prune.
    if key_lower.is_finite()
        && key_lower >= 0.0
        && norm_lower > 0.0
        && DdInterval::exact(gap_upper)
            .square()
            .zip(DdInterval::exact(key_lower).mul(DdInterval::exact(norm_lower)))
            .is_some_and(|(bound_numerator, incumbent_lower_product)| {
                let (_, bound_upper) = bound_numerator.outward_bounds();
                let (incumbent_lower, _) = incumbent_lower_product.outward_bounds();
                bound_upper.is_finite()
                    && incumbent_lower.is_finite()
                    && bound_upper <= incumbent_lower
            })
    {
        return Some(false);
    }
    None
}

pub(crate) fn clearance_strip_exceeds(
    point: XY,
    normal: [f64; 2],
    low: XY,
    high: XY,
    incumbent: &SquaredDistanceKey,
) -> Result<bool> {
    if let Some(prunes) = clearance_strip_filter_result(point, normal, low, high, incumbent) {
        return Ok(prunes);
    }
    #[cfg(test)]
    CLEARANCE_STRIP_EXACT_FALLBACKS.with(|count| count.set(count.get().saturating_add(1)));

    let low_gap = source_dot_difference_exact(normal, low, point)?;
    let high_gap = source_dot_difference_exact(normal, point, high)?;
    let gap = match low_gap.cmp(&high_gap) {
        Ordering::Greater => low_gap,
        Ordering::Less | Ordering::Equal => high_gap,
    };
    let zero = ExactDyadic::zero();
    if !gap.cmp(&zero).is_gt() {
        return Ok(false);
    }
    let [nx, ny] = normal;
    let x = ExactDyadic::from_f64(nx);
    let y = ExactDyadic::from_f64(ny);
    let norm_squared = x.try_product(&x)?.try_add(y.try_product(&y)?)?;
    if norm_squared.cmp(&zero).is_eq() {
        return Ok(false);
    }
    let bound_numerator = gap.try_product(&gap)?;
    let incumbent = incumbent.witness.squared()?;
    Ok(compare_ratios(
        &bound_numerator,
        &norm_squared,
        &incumbent.numerator,
        &incumbent.denominator,
    )?
    .is_gt())
}

pub(super) fn point_pair_distance_key(left: XY, right: XY) -> SquaredDistanceKey {
    let witness = ExactDistanceWitness::Points(left, right);
    let dx = left.x - right.x;
    let dy = left.y - right.y;
    let squared = dx * dx + dy * dy;
    if squared_norm_is_trustworthy(squared, dx == 0.0 && dy == 0.0) {
        SquaredDistanceFilter::point_pair(squared).bind(witness)
    } else {
        SquaredDistanceFilter::unbounded().bind(witness)
    }
}

#[expect(
    clippy::float_cmp,
    reason = "axis-aligned carriers are exact identities over stored coordinates"
)]
pub(super) fn point_segment_exact_distance_key(point: XY, segment: Segment) -> SquaredDistanceKey {
    if segment.start.y == segment.end.y
        && point.x >= segment.start.x.min(segment.end.x)
        && point.x <= segment.start.x.max(segment.end.x)
    {
        return point_pair_distance_key(point, XY::new(point.x, segment.start.y));
    }
    if segment.start.x == segment.end.x
        && point.y >= segment.start.y.min(segment.end.y)
        && point.y <= segment.start.y.max(segment.end.y)
    {
        return point_pair_distance_key(point, XY::new(segment.start.x, point.y));
    }
    let filtered = (|| {
        let dx = source_difference(segment.end.x, segment.start.x)?;
        let dy = source_difference(segment.end.y, segment.start.y)?;
        let qx = source_difference(point.x, segment.start.x)?;
        let qy = source_difference(point.y, segment.start.y)?;
        let denominator = SourcePair::new(source_product(dx, dx)?, source_product(dy, dy)?, false)?;
        let numerator = SourcePair::new(source_product(qx, dx)?, source_product(qy, dy)?, false)?;
        if numerator.upper() < 0.0 {
            return Some(point_pair_distance_key(point, segment.start));
        }
        if numerator.lower() > denominator.upper() {
            return Some(point_pair_distance_key(point, segment.end));
        }
        if numerator.lower() <= 0.0 || numerator.upper() >= denominator.lower() {
            return None;
        }
        let determinant = SourcePair::new(source_product(dx, qy)?, source_product(dy, qx)?, true)?;
        let (low, high) = (determinant.lower(), determinant.upper());
        let minimum = if low <= 0.0 && high >= 0.0 {
            0.0
        } else {
            low.abs().min(high.abs())
        };
        let maximum = low.abs().max(high.abs());
        let lower_square = (minimum * minimum).next_down().max(0.0);
        let upper_square = (maximum * maximum).next_up();
        let lower = (lower_square / denominator.upper()).next_down().max(0.0);
        let upper = (upper_square / denominator.lower()).next_up();
        upper.is_finite().then(|| {
            SquaredDistanceFilter {
                lo: lower,
                hi: upper,
            }
            .bind(ExactDistanceWitness::Projected(point, segment))
        })
    })();
    if let Some(key) = filtered {
        return key;
    }
    SquaredDistanceFilter::unbounded().bind(ExactDistanceWitness::Clamped(point, segment))
}

#[cfg(test)]
pub(super) fn point_segment_distance_key(
    point: XY,
    segment: Segment,
) -> Result<SquaredDistanceKey> {
    Ok(point_segment_selection(point, segment)?.distance_key(point, segment))
}

pub(crate) fn point_segment_selection(
    point: XY,
    segment: Segment,
) -> Result<PointSegmentSelection> {
    Ok(match point_segment_metric(point, segment)? {
        PointSegmentMetric::Squared {
            measurement_squared,
            filter,
            witness_kind,
            projection,
            ..
        } => PointSegmentSelection {
            distance: measurement_squared.sqrt(),
            distance_squared: measurement_squared,
            filter,
            witness_kind,
            projection,
        },
        PointSegmentMetric::Projection(projection) => {
            let distance = projection.continuum_distance(point, segment)?;
            let squared = match &projection {
                SegmentProjection::Start => {
                    ExactSquaredDistance::point_pair(point, segment.start)?.rounded()?
                },
                SegmentProjection::End => {
                    ExactSquaredDistance::point_pair(point, segment.end)?.rounded()?
                },
                SegmentProjection::Interior(_)
                | SegmentProjection::CertifiedInterior(_)
                | SegmentProjection::ExactInterior(_) => source_projection_squared(point, segment)?,
            };
            let witness_kind = if projection.is_start() {
                PointSegmentWitnessKind::Start
            } else if projection.is_end() {
                PointSegmentWitnessKind::End
            } else {
                PointSegmentWitnessKind::Interior
            };
            PointSegmentSelection {
                distance,
                distance_squared: if squared.is_finite() {
                    squared
                } else {
                    f64::INFINITY
                },
                filter: if squared_norm_is_trustworthy(squared, false) {
                    SquaredDistanceFilter::source_squared(squared, 16.0 * (f64::EPSILON / 2.0))
                } else {
                    SquaredDistanceFilter::unbounded()
                },
                witness_kind,
                projection: SelectionProjectionSource::Ready(projection),
            }
        },
    })
}

#[cfg(test)]
std::thread_local! {
    pub(crate) static EXACT_SELECTION_FALLBACKS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    pub(crate) static CLEARANCE_STRIP_EXACT_FALLBACKS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    pub(crate) static CLEARANCE_STRIP_SUPPORT_FALLBACKS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

impl ExactDistanceWitness {
    fn witness_role(self) -> Result<WitnessRole> {
        Ok(match self {
            Self::Projected(..) => WitnessRole::Interior,
            Self::Points(..) => WitnessRole::Point,
            Self::Clamped(point, segment) => {
                match classify_segment_projection_exact(point, segment)? {
                    ExactClassifiedSegmentProjection::Interior(_) => WitnessRole::Interior,
                    ExactClassifiedSegmentProjection::Start
                    | ExactClassifiedSegmentProjection::End => WitnessRole::Point,
                }
            },
        })
    }

    #[expect(
        clippy::float_cmp,
        reason = "equal axis-aligned offsets certify the same exact source distance"
    )]
    fn same_distance_as(self, other: Self) -> bool {
        let equal_sum = |a, b, c, d| {
            let (left, left_tail) = two_sum(a, b);
            let (right, right_tail) = two_sum(c, d);
            left.is_finite() && right.is_finite() && left == right && left_tail == right_tail
        };
        match (self, other) {
            (Self::Points(left_a, left_b), Self::Points(right_a, right_b)) => {
                (same_point(left_a, right_a) && same_point(left_b, right_b))
                    || (same_point(left_a, right_b) && same_point(left_b, right_a))
                    || (left_a.x == left_b.x
                        && right_a.x == right_b.x
                        && ((left_a.y == right_a.y && left_b.y == right_b.y)
                            || (left_a.y == right_b.y && left_b.y == right_a.y)))
                    || (left_a.y == left_b.y
                        && right_a.y == right_b.y
                        && ((left_a.x == right_a.x && left_b.x == right_b.x)
                            || (left_a.x == right_b.x && left_b.x == right_a.x)))
            },
            (Self::Projected(left, a), Self::Projected(right, b)) => {
                ((same_point(a.start, b.start) && same_point(a.end, b.end))
                    || (same_point(a.start, b.end) && same_point(a.end, b.start)))
                    && equal_sum(left.x, right.x, a.start.x, a.end.x)
                    && equal_sum(left.y, right.y, a.start.y, a.end.y)
            },
            _ => false,
        }
    }

    fn squared(self) -> Result<ExactSquaredDistance> {
        match self {
            Self::Points(left, right) => ExactSquaredDistance::point_pair(left, right),
            Self::Projected(point, segment) => exact_source_squared(point, segment),
            Self::Clamped(point, segment) => {
                match classify_segment_projection_exact(point, segment)? {
                    ExactClassifiedSegmentProjection::Start => {
                        ExactSquaredDistance::point_pair(point, segment.start)
                    },
                    ExactClassifiedSegmentProjection::End => {
                        ExactSquaredDistance::point_pair(point, segment.end)
                    },
                    ExactClassifiedSegmentProjection::Interior(terms) => {
                        ExactSquaredDistance::interior(&terms)
                    },
                }
            },
        }
    }
}

pub(super) fn exact_distance_witness_cmp(
    left: ExactDistanceWitness,
    right: ExactDistanceWitness,
) -> Result<Ordering> {
    left.squared()?.cmp(&right.squared()?)
}

fn exact_selection_distance_cmp(
    left: ExactDistanceWitness,
    right: ExactDistanceWitness,
) -> Result<Ordering> {
    #[cfg(test)]
    EXACT_SELECTION_FALLBACKS.with(|count| count.set(count.get().saturating_add(1)));
    exact_distance_witness_cmp(left, right)
}

fn exact_witness_cmp_with_identity(
    left: ExactDistanceWitness,
    right: ExactDistanceWitness,
) -> Result<Ordering> {
    if left.same_distance_as(right) {
        return Ok(left
            .witness_role()?
            .cmp_tie(right.witness_role()?, || Ordering::Equal));
    }
    let distance = exact_selection_distance_cmp(left, right)?;
    if distance.is_eq() {
        Ok(left
            .witness_role()?
            .cmp_tie(right.witness_role()?, || Ordering::Equal))
    } else {
        Ok(distance)
    }
}

#[cfg(test)]
mod witness_role_tests {
    use super::*;

    #[test]
    fn chart_witness_role_clamped_recipe_classifies_only_equal_source_distances() {
        let point = XY::new(0.0, 0.0);
        let endpoint = ExactDistanceWitness::Clamped(point, Segment {
            start: XY::new(0.0, 1.0),
            end: XY::new(1.0, 2.0),
        });
        let interior = ExactDistanceWitness::Clamped(point, Segment {
            start: XY::new(-1.0, 1.0),
            end: XY::new(1.0, 1.0),
        });
        let point_pair = ExactDistanceWitness::Points(point, XY::new(0.0, 1.0));
        assert_eq!(
            exact_witness_cmp_with_identity(endpoint, point_pair).unwrap(),
            Ordering::Equal
        );
        assert_eq!(
            exact_witness_cmp_with_identity(interior, point_pair).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            exact_distance_witness_cmp(interior, point_pair).unwrap(),
            Ordering::Equal
        );
        let closer = ExactDistanceWitness::Points(point, XY::new(0.0, 0.5));
        assert_eq!(
            exact_witness_cmp_with_identity(interior, closer).unwrap(),
            Ordering::Greater
        );
    }
}

/// Affinely interpolate between two finite points. A convex `fraction` in
/// `[0, 1]` always yields a finite point; far extrapolation (e.g. a
/// near-parallel `line_intersection`) can overflow, in which case we fall back
/// to the nearer endpoint rather than panic across the FFI boundary.
pub(super) fn interpolate_segment_point(
    start: impl Into<XY>,
    end: impl Into<XY>,
    fraction: f64,
) -> XY {
    let (start, end) = (start.into(), end.into());
    let x = interpolate_f64(start.x, end.x, fraction);
    let y = interpolate_f64(start.y, end.y, fraction);
    if x.is_finite() && y.is_finite() {
        XY::new(x, y)
    } else if fraction <= 0.5 {
        start
    } else {
        end
    }
}

/// Ordinate-carrying interpolation for the LRS lanes: every present
/// ordinate (X/Y/Z/M) interpolates; the engine's planar form is
/// [`interpolate_segment_point`].
pub(crate) fn lerp_point(start: Point, end: Point, fraction: f64) -> Point {
    Point::new_axes(
        interpolate_f64(start.x, end.x, fraction),
        interpolate_f64(start.y, end.y, fraction),
        ZOrdinate(interpolate_optional(start.z(), end.z(), fraction)),
        MOrdinate(interpolate_optional(start.m(), end.m(), fraction)),
    )
    .unwrap_or(if fraction <= 0.5 { start } else { end })
}

/// Affine interpolation `start*(1 - fraction) + end*fraction`, valid for any
/// `fraction` (callers such as `line_intersection` extrapolate past `[0, 1]`).
/// Return exact endpoint and constant-ordinate identities before evaluating the
/// convex form, whose separate products can underflow an equal subnormal value.
/// Otherwise the convex form keeps the result bounded by
/// `max(|start|, |end|)` for `fraction` in `[0, 1]`, avoiding the
/// `end - start` overflow class.
#[expect(
    clippy::float_cmp,
    reason = "zero and one are exact affine parameter identities; equal finite ordinates are preserved"
)]
pub(crate) fn interpolate_f64(start: f64, end: f64, fraction: f64) -> f64 {
    if fraction == 0.0 {
        return start;
    }
    if fraction == 1.0 {
        return end;
    }
    if fraction.is_finite() && start.is_finite() && start == end {
        return start;
    }
    let value = start * (1.0 - fraction) + end * fraction;
    if value.is_finite() || !start.is_finite() || !end.is_finite() {
        return value;
    }
    // The same convex reconstruction in a power-of-two frame: `±1e308`
    // endpoints can have a perfectly finite interior point even though the
    // direct two products overflow with opposite signs.
    let scale = axis_pow2_scale(start.abs().max(end.abs()));
    (start * scale * (1.0 - fraction) + end * scale * fraction) / scale
}

/// [`interpolate_f64`] for an optional Z or M ordinate, present only when both
/// endpoints carry it.
pub(crate) fn interpolate_optional_ordinate(
    start: Option<f64>,
    end: Option<f64>,
    fraction: f64,
) -> Option<f64> {
    Some(interpolate_f64(start?, end?, fraction))
}

/// Interpolation for emitted subdivision ordinates. The error-free two-sum
/// difference identifies the common case where one FMA avoids an extra
/// product/add rounding; an existing double-double path handles an inexact
/// difference. Both retain the convex finite-range fallback for extremes.
#[expect(
    clippy::float_cmp,
    reason = "zero and one endpoints and an exact two-sum residual are deliberate"
)]
pub(crate) fn interpolate_subdivision_ordinate(start: f64, end: f64, fraction: f64) -> f64 {
    if fraction == 0.0 {
        return start;
    }
    if fraction == 1.0 {
        return end;
    }
    let (difference, residual) = two_sum(end, -start);
    if difference.is_finite() && residual == 0.0 {
        let value = fraction.mul_add(difference, start);
        if value.is_finite() {
            return value;
        }
    }
    dd_interpolate(start, end, (fraction, 0.0))
}

/// Fraction for a finite interpolation interval. Keep the established
/// arithmetic on the ordinary path; exact stored-binary64 ratio arithmetic is
/// only needed when the source interval or target residual overflows.
pub(crate) fn interpolate_fraction(start: f64, end: f64, target: f64) -> Result<f64> {
    let numerator = target - start;
    let denominator = end - start;
    if denominator == 0.0 {
        return Ok(0.0);
    }
    if numerator.is_finite() && denominator.is_finite() {
        return Ok(numerator / denominator);
    }
    let numerator = ExactDyadic::from_f64(target).try_subtract(ExactDyadic::from_f64(start))?;
    let denominator = ExactDyadic::from_f64(end).try_subtract(ExactDyadic::from_f64(start))?;
    round_ratio(&numerator, &denominator)
}

/// Reconstruct the free axis at a known held-axis crossing without forming
/// `(at - start) / (end - start)` in world scale. `None` is deliberate: an
/// unresolved crossing must make a caller fall back, never manufacture a
/// non-finite vertex or an empty clipped result.
#[expect(
    clippy::float_cmp,
    reason = "a held axis exactly selects an already-stored endpoint before any interpolation"
)]
pub(crate) fn held_axis_interpolate(
    start_held: f64,
    start_free: f64,
    end_held: f64,
    end_free: f64,
    held: f64,
) -> Option<f64> {
    if held == start_held {
        return Some(start_free);
    }
    if held == end_held {
        return Some(end_free);
    }
    // Preserve the established ordinary finite arithmetic exactly.  The
    // normalized reconstruction below exists for an overflowed delta or a
    // non-finite raw interpolation, not to perturb unrelated stored-double
    // crossings by changing their rounding path.
    let numerator = held - start_held;
    let denominator = end_held - start_held;
    if numerator.is_finite() && denominator.is_finite() {
        let fraction = numerator / denominator;
        let value = start_free + fraction * (end_free - start_free);
        if fraction.is_finite() && (0.0..=1.0).contains(&fraction) && value.is_finite() {
            return Some(value);
        }
    }
    let scale = axis_pow2_scale(start_held.abs().max(end_held.abs()).max(held.abs()));
    let numerator = scaled_residual(held, start_held, scale);
    let denominator = scaled_residual(end_held, start_held, scale);
    let fraction = numerator / denominator;
    if !fraction.is_finite() || !(0.0..=1.0).contains(&fraction) {
        return None;
    }
    let value = interpolate_f64(start_free, end_free, fraction);
    value.is_finite().then_some(value)
}

pub(super) fn interpolate_optional(
    start: Option<f64>,
    end: Option<f64>,
    fraction: f64,
) -> Option<f64> {
    match (start, end) {
        (Some(start), Some(end)) => Some(interpolate_f64(start, end, fraction)),
        _ => None,
    }
}

/// Tolerance for deciding an output vertex lies on an input segment when
/// restoring Z/M (relative to the segment length; see `segment_ordinate_at`).
pub(super) const ON_SEGMENT_EPSILON: f64 = 1e-9;

/// Transversal crossing point of two segments, or `None` when they do not
/// meet (or are collinear — overlaps are the endpoint-overlap path's job).
///
/// **Existence is decided exactly** by the robust orientation predicates, so
/// floating-point conditioning can never drop a true crossing (nor invent
/// one). A touching endpoint is returned bit-exactly and a proper crossing as
/// [`crossing_point`], the nearest-even rounding of the exact point. The
/// returned point is the plain X/Y crossing; Z/M is restored later.
pub(super) fn segment_cross_point(a: Segment, b: Segment) -> Result<Option<XY>> {
    Ok(match segment_contact_exact(a, b) {
        Contact::Cross => Some(crossing_point(a, b)?.point),
        Contact::Touch(endpoint) => Some(endpoint.point(a, b)),
        Contact::Disjoint | Contact::Collinear => None,
    })
}

/// The endpoint at which a pair of segments touches without crossing:
/// a straight segment meets the other's carrier once, so an endpoint on it is
/// the contact, bit-exactly. `None` when neither straddles on an endpoint.
fn touch_endpoint(a: Segment, b: Segment, orientations: PairOrientations) -> Option<XY> {
    if orientations.right_start == Orientation::Collinear {
        return Some(b.start);
    }
    if orientations.right_end == Orientation::Collinear {
        return Some(b.end);
    }
    if orientations.left_start == Orientation::Collinear {
        return Some(a.start);
    }
    if orientations.left_end == Orientation::Collinear {
        return Some(a.end);
    }
    None
}

/// The single point shared by a pair of segments that are not collinear:
/// a touching endpoint bit-exactly, or the nearest-even rounding of a
/// proper crossing; `None` when they do not meet.
#[cfg(test)]
pub(crate) fn segment_cross_point_with_orientations(
    a: Segment,
    b: Segment,
    orientations: PairOrientations,
) -> Result<Option<XY>> {
    // Same strict side: no crossing. Both collinear: parallel overlap,
    // handled by the endpoint-overlap path.
    if orientations.right_start == orientations.right_end
        || orientations.left_start == orientations.left_end
    {
        return Ok(None);
    }
    if let Some(point) = touch_endpoint(a, b, orientations) {
        return Ok(Some(point));
    }
    Ok(Some(crossing_point(a, b)?.point))
}

pub(crate) fn point_on_segment(
    point: impl Into<XY>,
    start: impl Into<XY>,
    end: impl Into<XY>,
) -> bool {
    let (point, start, end) = (point.into(), start.into(), end.into());
    // Cheap closed-envelope reject first: orientation() is the robust adaptive
    // determinant and the compiler cannot reorder it past the && short-circuit.
    if point.x < start.x.min(end.x)
        || point.x > start.x.max(end.x)
        || point.y < start.y.min(end.y)
        || point.y > start.y.max(end.y)
    {
        return false;
    }
    orientation(start, end, point) == Orientation::Collinear
}

pub(crate) fn line_segments<C: Coordinates + ?Sized>(
    points: &C,
) -> impl Iterator<Item = Segment> + '_ {
    let mut columns = points
        .xy_columns()
        .map(|(xs, ys)| (xs.array_windows::<2>(), ys.array_windows::<2>()));
    let mut pairs = points.segment_pairs();
    std::iter::from_fn(move || {
        if let Some((xs, ys)) = &mut columns {
            let x = xs.next()?;
            let y = ys.next()?;
            return Some(Segment {
                start: XY::new(x[0], y[0]),
                end: XY::new(x[1], y[1]),
            });
        }
        pairs.next().map(|[start, end]| Segment {
            start: start.into(),
            end: end.into(),
        })
    })
}

/// Orientation-independent undirected key for two segment endpoints.
pub(super) fn undirected_edge_key(left: PointKey, right: PointKey) -> (PointKey, PointKey) {
    if (left.x, left.y) <= (right.x, right.y) {
        (left, right)
    } else {
        (right, left)
    }
}

/// Orientation-independent undirected key for an atomic segment.
pub(super) fn undirected_segment_edge_key(segment: Segment) -> (PointKey, PointKey) {
    undirected_edge_key(PointKey::new(segment.start), PointKey::new(segment.end))
}

pub(super) fn segments_intersect(left: Segment, right: Segment) -> bool {
    segment_contact(left, right) != SegmentContact::None
}

/// How a segment meets a rectangle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RectContact {
    /// Enters the rectangle's interior.
    Open,
    /// Touches the closed rectangle (edge/corner contact only).
    Closed,
    /// No contact.
    None,
}

/// Exact segment-vs-rectangle contact via the separating-axis test on robust
/// orientations. The closed contact result is kept distinct from entering the
/// open rectangle so a source boundary coincident with a covered cell edge
/// does not demote that cell to a partial result.
pub(crate) fn segment_vs_rect(segment: Segment, rect: Bounds) -> RectContact {
    let (sx, ex) = (segment.start.x, segment.end.x);
    let (sy, ey) = (segment.start.y, segment.end.y);
    let (lo_x, hi_x) = (sx.min(ex), sx.max(ex));
    let (lo_y, hi_y) = (sy.min(ey), sy.max(ey));
    if hi_x < rect.minx() || lo_x > rect.maxx() || hi_y < rect.miny() || lo_y > rect.maxy() {
        return RectContact::None;
    }
    #[expect(
        clippy::float_cmp,
        reason = "exact coordinate identity is required to classify a repeated segment endpoint"
    )]
    let degenerate = sx == ex && sy == ey;
    if degenerate {
        return if sx > rect.minx() && sx < rect.maxx() && sy > rect.miny() && sy < rect.maxy() {
            RectContact::Open
        } else {
            RectContact::Closed
        };
    }
    let corners = [
        XY::new(rect.minx(), rect.miny()),
        XY::new(rect.maxx(), rect.miny()),
        XY::new(rect.maxx(), rect.maxy()),
        XY::new(rect.minx(), rect.maxy()),
    ];
    let mut strict_positive = false;
    let mut strict_negative = false;
    let mut collinear = false;
    for corner in corners {
        match orientation(segment.start, segment.end, corner) {
            Orientation::CounterClockwise => strict_positive = true,
            Orientation::Clockwise => strict_negative = true,
            Orientation::Collinear => collinear = true,
        }
    }
    if !(strict_positive && strict_negative) {
        return if collinear {
            RectContact::Closed
        } else {
            RectContact::None
        };
    }
    if hi_x > rect.minx() && lo_x < rect.maxx() && hi_y > rect.miny() && lo_y < rect.maxy() {
        RectContact::Open
    } else {
        RectContact::Closed
    }
}

/// Closed-envelope disjointness — the cheap reject before the robust
/// orientation work in brute-force pair scans (the indexed paths already
/// prune by envelope).
pub(super) fn segment_envelopes_disjoint(left: Segment, right: Segment) -> bool {
    left.start.x.max(left.end.x) < right.start.x.min(right.end.x)
        || right.start.x.max(right.end.x) < left.start.x.min(left.end.x)
        || left.start.y.max(left.end.y) < right.start.y.min(right.end.y)
        || right.start.y.max(right.end.y) < left.start.y.min(left.end.y)
}

/// How a segment pair meets, strongest last: `Cross` is a TRANSVERSAL
/// crossing strictly interior to both segments (every endpoint strictly off
/// the other's carrier, both straddling), `Touch` is any other intersection
/// (endpoint contact or collinear overlap). The split is exact: with any
/// endpoint on the other's carrier, the carrier meets that segment only at
/// the endpoint (or the segments are fully collinear), so the intersection
/// — if it exists — involves an endpoint and the boundary-inclusive
/// membership checks decide it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum SegmentContact {
    None,
    Touch,
    Cross,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PairOrientations {
    pub(crate) right_start: Orientation,
    pub(crate) right_end: Orientation,
    pub(crate) left_start: Orientation,
    pub(crate) left_end: Orientation,
}

impl PairOrientations {
    pub(crate) fn new(left: Segment, right: Segment) -> Self {
        Self {
            right_start: orientation(left.start, left.end, right.start),
            right_end: orientation(left.start, left.end, right.end),
            left_start: orientation(right.start, right.end, left.start),
            left_end: orientation(right.start, right.end, left.end),
        }
    }

    fn all_collinear(self) -> bool {
        self.right_start == Orientation::Collinear
            && self.right_end == Orientation::Collinear
            && self.left_start == Orientation::Collinear
            && self.left_end == Orientation::Collinear
    }

    fn has_collinear(self) -> bool {
        self.right_start == Orientation::Collinear
            || self.right_end == Orientation::Collinear
            || self.left_start == Orientation::Collinear
            || self.left_end == Orientation::Collinear
    }

    fn straddles_or_touches(self) -> bool {
        self.right_start != self.right_end && self.left_start != self.left_end
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct SharedSpan {
    pub same_direction: bool,
    pub start: XY,
    pub end: XY,
}

/// Exact touches are stored endpoints; defer copying their coordinate until a
/// consumer needs it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TouchEndpoint {
    LeftStart,
    RightStart,
    RightEnd,
}

impl TouchEndpoint {
    pub(super) const fn point(self, left: Segment, right: Segment) -> XY {
        match self {
            Self::LeftStart => left.start,
            Self::RightStart => right.start,
            Self::RightEnd => right.end,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SegmentEndpoint {
    Start,
    End,
}

impl SegmentEndpoint {
    const fn point(self, segment: Segment) -> XY {
        match self {
            Self::Start => segment.start,
            Self::End => segment.end,
        }
    }
}

/// Which segment contains a touch in its interior, or which stored endpoints
/// are shared.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TouchLocation {
    Shared(TouchEndpoint),
    LeftInterior(SegmentEndpoint),
    RightInterior(SegmentEndpoint),
}

impl TouchLocation {
    pub(super) const fn point(self, left: Segment, right: Segment) -> XY {
        match self {
            Self::Shared(endpoint) => endpoint.point(left, right),
            Self::LeftInterior(endpoint) => endpoint.point(right),
            Self::RightInterior(endpoint) => endpoint.point(left),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Contact {
    Disjoint,
    /// A proper crossing, interior to both segments; [`crossing_point`]
    /// places it.
    Cross,
    Touch(TouchLocation),
    /// The carriers coincide; consumers that need run endpoints ask for them.
    Collinear,
}

impl Contact {
    /// Construct the shared run only for consumers that need its endpoints.
    pub(super) fn shared_span(self, left: Segment, right: Segment) -> Option<SharedSpan> {
        match self {
            Self::Collinear => collinear_shared_span_exact(left, right),
            Self::Disjoint | Self::Cross | Self::Touch(_) => None,
        }
    }
}

// The partition expands the same exact decisions as ordinary callers.
macro_rules! segment_contact_exact_impl {
    (@collinear_cuts $left:expr, $right:expr; |$location:ident| $effect:block) => {{
        use $crate::geometry::segments::{SegmentEndpoint, TouchLocation, strictly_inside};

        let left = $left;
        let right = $right;
        if strictly_inside(left, right.start) {
            let $location = TouchLocation::LeftInterior(SegmentEndpoint::Start);
            $effect
        }
        if strictly_inside(left, right.end) {
            let $location = TouchLocation::LeftInterior(SegmentEndpoint::End);
            $effect
        }
        if strictly_inside(right, left.start) {
            let $location = TouchLocation::RightInterior(SegmentEndpoint::Start);
            $effect
        }
        if strictly_inside(right, left.end) {
            let $location = TouchLocation::RightInterior(SegmentEndpoint::End);
            $effect
        }
    }};
    ($left:expr, $right:expr) => {{
        use $crate::geometry::exact_arith::ExactSign;
        use $crate::geometry::same_point;
        use $crate::geometry::segments::{
            Contact, SegmentEndpoint, TouchEndpoint, TouchLocation, stored_orientation_sign,
            strictly_inside,
        };

        let left = $left;
        let right = $right;
        'contact: {
            'decide: {
                if same_point(left.start, left.end) {
                    if same_point(right.start, right.end) {
                        if same_point(left.start, right.start) {
                            break 'contact Contact::Touch(TouchLocation::Shared(
                                TouchEndpoint::LeftStart,
                            ));
                        }
                        break 'contact Contact::Disjoint;
                    }
                    if !$crate::geometry::segments::point_in_segment_envelope(left.start, right) {
                        break 'contact Contact::Disjoint;
                    }
                    if stored_orientation_sign(
                        right.start.x,
                        right.start.y,
                        right.end.x,
                        right.end.y,
                        left.start.x,
                        left.start.y,
                    ) != ExactSign::Zero
                    {
                        break 'contact Contact::Disjoint;
                    }
                    if same_point(left.start, right.start) || same_point(left.start, right.end) {
                        break 'contact Contact::Touch(TouchLocation::Shared(
                            TouchEndpoint::LeftStart,
                        ));
                    }
                    break 'contact Contact::Touch(TouchLocation::RightInterior(
                        SegmentEndpoint::Start,
                    ));
                }
                if same_point(right.start, right.end) {
                    if !$crate::geometry::segments::point_in_segment_envelope(right.start, left) {
                        break 'contact Contact::Disjoint;
                    }
                    if stored_orientation_sign(
                        left.start.x,
                        left.start.y,
                        left.end.x,
                        left.end.y,
                        right.start.x,
                        right.start.y,
                    ) != ExactSign::Zero
                    {
                        break 'contact Contact::Disjoint;
                    }
                    if same_point(right.start, left.start) || same_point(right.start, left.end) {
                        break 'contact Contact::Touch(TouchLocation::Shared(
                            TouchEndpoint::RightStart,
                        ));
                    }
                    break 'contact Contact::Touch(TouchLocation::LeftInterior(
                        SegmentEndpoint::Start,
                    ));
                }

                if $crate::geometry::segments::segment_envelopes_disjoint(left, right) {
                    break 'contact Contact::Disjoint;
                }

                let right_start_shared =
                    same_point(right.start, left.start) || same_point(right.start, left.end);
                let right_end_shared =
                    same_point(right.end, left.start) || same_point(right.end, left.end);
                let shared = match (right_start_shared, right_end_shared) {
                    (true, true) => break 'decide,
                    (true, false) => Some((TouchEndpoint::RightStart, right.end)),
                    (false, true) => Some((TouchEndpoint::RightEnd, right.start)),
                    (false, false) => None,
                };
                if let Some((shared, other)) = shared {
                    if stored_orientation_sign(
                        left.start.x,
                        left.start.y,
                        left.end.x,
                        left.end.y,
                        other.x,
                        other.y,
                    ) == ExactSign::Zero
                    {
                        break 'decide;
                    }
                    break 'contact Contact::Touch(TouchLocation::Shared(shared));
                }

                let right_start = stored_orientation_sign(
                    left.start.x,
                    left.start.y,
                    left.end.x,
                    left.end.y,
                    right.start.x,
                    right.start.y,
                );
                let right_end = stored_orientation_sign(
                    left.start.x,
                    left.start.y,
                    left.end.x,
                    left.end.y,
                    right.end.x,
                    right.end.y,
                );
                if $crate::geometry::segments::strictly_same_side(right_start, right_end) {
                    break 'contact Contact::Disjoint;
                }
                if right_start == ExactSign::Zero && right_end == ExactSign::Zero {
                    break 'decide;
                }
                if right_start == ExactSign::Zero {
                    if strictly_inside(left, right.start) {
                        break 'contact Contact::Touch(TouchLocation::LeftInterior(
                            SegmentEndpoint::Start,
                        ));
                    }
                    break 'contact Contact::Disjoint;
                }
                if right_end == ExactSign::Zero {
                    if strictly_inside(left, right.end) {
                        break 'contact Contact::Touch(TouchLocation::LeftInterior(
                            SegmentEndpoint::End,
                        ));
                    }
                    break 'contact Contact::Disjoint;
                }

                let left_start = stored_orientation_sign(
                    right.start.x,
                    right.start.y,
                    right.end.x,
                    right.end.y,
                    left.start.x,
                    left.start.y,
                );
                let left_end = stored_orientation_sign(
                    right.start.x,
                    right.start.y,
                    right.end.x,
                    right.end.y,
                    left.end.x,
                    left.end.y,
                );
                if $crate::geometry::segments::strictly_same_side(left_start, left_end) {
                    break 'contact Contact::Disjoint;
                }
                if left_start == ExactSign::Zero && left_end == ExactSign::Zero {
                    break 'decide;
                }
                if left_start == ExactSign::Zero {
                    if strictly_inside(right, left.start) {
                        break 'contact Contact::Touch(TouchLocation::RightInterior(
                            SegmentEndpoint::Start,
                        ));
                    }
                    break 'contact Contact::Disjoint;
                }
                if left_end == ExactSign::Zero {
                    if strictly_inside(right, left.end) {
                        break 'contact Contact::Touch(TouchLocation::RightInterior(
                            SegmentEndpoint::End,
                        ));
                    }
                    break 'contact Contact::Disjoint;
                }

                if left_start != left_end {
                    break 'contact Contact::Cross;
                }
                break 'contact Contact::Disjoint;
            }

            Contact::Collinear
        }
    }};
}

pub(super) use segment_contact_exact_impl;

pub(super) fn segment_contact_exact(left: Segment, right: Segment) -> Contact {
    segment_contact_exact_impl!(left, right)
}

pub(super) fn strictly_same_side(left: ExactSign, right: ExactSign) -> bool {
    left == right && left != ExactSign::Zero
}

pub(super) fn segment_contact(left: Segment, right: Segment) -> SegmentContact {
    match segment_contact_exact(left, right) {
        Contact::Disjoint => SegmentContact::None,
        Contact::Cross => SegmentContact::Cross,
        Contact::Touch(_) | Contact::Collinear => SegmentContact::Touch,
    }
}

fn collinear_shared_span_exact(left: Segment, right: Segment) -> Option<SharedSpan> {
    let left_degenerate = same_point(left.start, left.end);
    let right_degenerate = same_point(right.start, right.end);

    if left_degenerate && right_degenerate {
        // A zero-length span carries no meaningful direction; the endpoint
        // deltas are exactly zero, so the dot product is a well-defined `0.0`
        // here (no underflow) — reuse it for the degenerate agreement flag.
        let same_direction = segment_direction_dot(left, right) > 0.0;
        return same_point(left.start, right.start).then_some(SharedSpan {
            same_direction,
            start: left.start,
            end: left.start,
        });
    }
    if left_degenerate {
        let same_direction = segment_direction_dot(left, right) > 0.0;
        return point_in_segment_envelope(left.start, right).then_some(SharedSpan {
            same_direction,
            start: left.start,
            end: left.start,
        });
    }
    if right_degenerate {
        let same_direction = segment_direction_dot(left, right) > 0.0;
        return point_in_segment_envelope(right.start, left).then_some(SharedSpan {
            same_direction,
            start: right.start,
            end: right.start,
        });
    }

    let axis = if (left.end.x - left.start.x).abs() >= (left.end.y - left.start.y).abs() {
        Axis::X
    } else {
        Axis::Y
    };
    let left_forward = axis.value(left.start) <= axis.value(left.end);
    // Shared direction from endpoint ORDER on the dominant axis, NOT a
    // direction-vector dot product: for tiny (~1e-162) collinear segments the
    // products underflow to `0.0`, misreading two identical segments as
    // opposite-direction. Both segments are non-degenerate and collinear here,
    // so each has a strictly nonzero extent on the larger-extent axis and the
    // ordering comparison is exact.
    let same_direction = left_forward == (axis.value(right.start) <= axis.value(right.end));
    let along_left = |point: XY| {
        let value = axis.value(point);
        if left_forward { value } else { -value }
    };

    let left_start = along_left(left.start);
    let left_end = along_left(left.end);
    let right_start = along_left(right.start);
    let right_end = along_left(right.end);
    let right_min = right_start.min(right_end);
    let right_max = right_start.max(right_end);
    let overlap_start = left_start.max(right_min);
    let overlap_end = left_end.min(right_max);
    if overlap_start > overlap_end {
        return None;
    }

    Some(SharedSpan {
        same_direction,
        start: endpoint_at_collinear_order(left, right, along_left, overlap_start),
        end: endpoint_at_collinear_order(left, right, along_left, overlap_end),
    })
}

#[derive(Clone, Copy)]
enum Axis {
    X,
    Y,
}

impl Axis {
    const fn value(self, point: XY) -> f64 {
        match self {
            Self::X => point.x,
            Self::Y => point.y,
        }
    }
}

/// Whether `point`, already known to lie on the carrier of this segment, is
/// strictly between its endpoints.
pub(super) fn strictly_inside(segment: Segment, point: XY) -> bool {
    let axis = if (segment.end.x - segment.start.x).abs() >= (segment.end.y - segment.start.y).abs()
    {
        Axis::X
    } else {
        Axis::Y
    };
    let start = axis.value(segment.start);
    let end = axis.value(segment.end);
    let value = axis.value(point);
    start.min(end) < value && value < start.max(end)
}

fn endpoint_at_collinear_order(
    left: Segment,
    right: Segment,
    along_left: impl Fn(XY) -> f64,
    target: f64,
) -> XY {
    [left.start, left.end, right.start, right.end]
        .into_iter()
        .find(|point| endpoint_order_key_matches(along_left(*point), target))
        .expect("collinear overlap boundary is one of the input endpoints")
}

#[expect(
    clippy::float_cmp,
    reason = "target is selected from the same exact endpoint keys"
)]
fn endpoint_order_key_matches(candidate: f64, target: f64) -> bool {
    // Exact key match: the target was selected from these endpoint keys.
    candidate == target
}

pub(crate) fn point_in_segment_envelope(point: XY, segment: Segment) -> bool {
    point.x >= segment.start.x.min(segment.end.x)
        && point.x <= segment.start.x.max(segment.end.x)
        && point.y >= segment.start.y.min(segment.end.y)
        && point.y <= segment.start.y.max(segment.end.y)
}

fn segment_direction_dot(left: Segment, right: Segment) -> f64 {
    let left_dx = left.end.x - left.start.x;
    let left_dy = left.end.y - left.start.y;
    let right_dx = right.end.x - right.start.x;
    let right_dy = right.end.y - right.start.y;
    left_dx * right_dx + left_dy * right_dy
}

/// Return the positive-length collinear overlap in source coordinates.
pub(super) fn shared_segment_part(left: Segment, right: Segment) -> Option<SharedSpan> {
    // The fused kernel already computes the EXACT collinear overlap span (its
    // endpoints are actual input coordinates, ordered along the segment). Reuse
    // it directly — a positive-length shared run is exactly an overlap whose two
    // endpoints differ by coordinate identity. No scale-dependent `1e-12` cutoff
    // (which misgraded a tiny-but-real shared edge as a point on large
    // coordinates) and no interpolation jitter.
    let span = segment_contact_exact(left, right).shared_span(left, right)?;
    if same_point(span.start, span.end) {
        return None;
    }
    Some(span)
}

pub(crate) fn point_segment_distance(point: impl Into<XY>, segment: Segment) -> Result<f64> {
    let point = point.into();
    Ok(match point_segment_metric(point, segment)? {
        PointSegmentMetric::Squared {
            measurement_squared,
            ..
        } => measurement_squared.sqrt(),
        PointSegmentMetric::Projection(projection) => {
            projection.continuum_distance(point, segment)?
        },
    })
}

/// A squared Euclidean residual is trustworthy only when it is a **normal**
/// positive number, or exactly zero with all-exact-zero deltas (true contact).
///
/// Positive subnormals are **not** faithful norms: `sqrt` of a subnormal square
/// quantizes the distance (e.g. separation `2.2e-162` squares to a subnormal
/// whose `sqrt` is ~`2.22e-162`, a +1% error that also flips `dwithin` and
/// breaks monotonicity). Full underflow to zero with nonzero deltas is the
/// sibling false-zero case. One rule for scalar, SIMD, and packed lanes.
pub(crate) fn squared_norm_is_trustworthy(squared: f64, all_deltas_zero: bool) -> bool {
    squared.is_normal() || (squared == 0.0 && all_deltas_zero)
}

/// Finalize a planar **distance** answer accumulated in squared space.
///
/// - `best_sq` is a normal positive → `sqrt` (ordinary mid-range, bit-stable)
/// - `best_sq` is zero or a positive subnormal → recompute in distance space
///   (true contact still returns 0; subnormal/false-zero cannot masquerade)
/// - non-finite → propagate (`+inf` = no candidate / overflow class)
///
/// Every planar distance answer that min-reduces squared candidates must
/// finish through this helper — never bare `squared.sqrt()`.
pub(crate) fn finish_planar_squared_min(
    best_sq: f64,
    recompute_distance: impl FnOnce() -> Result<f64>,
) -> Result<f64> {
    if best_sq.is_normal() {
        Ok(best_sq.sqrt())
    } else if best_sq >= 0.0 && best_sq.is_finite() {
        // Zero or positive subnormal: recompute with an honest hypot form.
        recompute_distance()
    } else {
        Ok(best_sq)
    }
}

/// Squared distance from `point` to `segment` — self-contained MEASUREMENT.
/// Ordering / compare-min callers may use this; a returned `0.0` is **not**
/// a contact certificate under underflow (use [`point_segment_distance`] or
/// [`finish_planar_squared_min`] for magnitude answers).
pub(super) fn point_segment_distance_squared(
    point: impl Into<XY>,
    segment: Segment,
) -> Result<f64> {
    let point = point.into();
    Ok(match point_segment_metric(point, segment)? {
        PointSegmentMetric::Squared {
            measurement_squared,
            ..
        } => measurement_squared,
        PointSegmentMetric::Projection(projection) => {
            projection.continuum_squared(point, segment)?
        },
    })
}

/// Guarded sqrt-form planar distance — ONE point-distance for the whole
/// project: one inline `sqrt` instead of a libm `hypot` call (~3× cheaper, the
/// dominant cost of prefix-length builds). The rescue covers both failure
/// classes — overflow to non-finite, and full underflow to `0.0` with a nonzero
/// delta — by falling back to `hypot`, so every input class gets a result
/// matching the overflow-safe form (lane rounding may differ by an ulp; lengths
/// are measurement arithmetic per the float policy). This is the scalar sibling
/// of the SIMD `line_length` kernel's sqrt-with-rescue lanes.
pub(crate) fn point_distance(left: impl Into<XY>, right: impl Into<XY>) -> f64 {
    let (left, right) = (left.into(), right.into());
    let dx = left.x - right.x;
    let dy = left.y - right.y;
    // Plain mul+add, NOT `mul_add`: scalar `mul_add` on the portable
    // baseline (no FMA target feature) is a libm `fma` CALL — the very cost
    // class this function exists to remove. Fused rounding buys nothing a
    // measurement needs.
    let squared = dx * dx + dy * dy;
    if squared_norm_is_trustworthy(squared, dx == 0.0 && dy == 0.0) {
        squared.sqrt()
    } else {
        dx.hypot(dy)
    }
}

/// Honest squared Euclidean distance when squared space is faithful.
/// Returns `None` on overflow, positive-subnormal squares, or false-zero
/// underflow (distinct points whose `dx²+dy²` collapses to 0 or a subnormal)
/// — callers that need a correct magnitude or zero-test must use
/// [`point_distance`] / [`points_dwithin`].
pub(crate) fn try_point_distance_squared(left: impl Into<XY>, right: impl Into<XY>) -> Option<f64> {
    let (left, right) = (left.into(), right.into());
    let dx = left.x - right.x;
    let dy = left.y - right.y;
    let squared = dx * dx + dy * dy;
    squared_norm_is_trustworthy(squared, dx == 0.0 && dy == 0.0).then_some(squared)
}

pub(crate) fn point_distance_squared(left: impl Into<XY>, right: impl Into<XY>) -> f64 {
    let (left, right) = (left.into(), right.into());
    try_point_distance_squared(left, right).unwrap_or_else(|| {
        // Overflow: hypot then square when representable; underflow of
        // d*d for tiny d still yields 0 — ordering-only callers should
        // prefer nearest_pair_key-style compare helpers; magnitude
        // callers must use [`point_distance`] / [`points_dwithin`].
        let d = point_distance(left, right);
        let d2 = d * d;
        if d2.is_finite() { d2 } else { f64::INFINITY }
    })
}

/// Planar dwithin in distance space when squared space collapses.
/// Ordinary mid-range pairs keep the squared compare (bit-stable at the
/// boundary for honest squares); tiny separations and tiny limits use
/// [`point_distance`] so `dwithin(a,b,0)` is false for distinct points and
/// `dwithin(a,b,5e-201)` is false when distance is 1e-200.
pub(crate) fn points_dwithin(left: impl Into<XY>, right: impl Into<XY>, limit: f64) -> bool {
    let (left, right) = (left.into(), right.into());
    if !limit.is_finite() {
        // +inf admits everything finite; NaN / -inf never.
        return limit == f64::INFINITY;
    }
    if limit < 0.0 {
        return false;
    }
    let dx = left.x - right.x;
    let dy = left.y - right.y;
    if dx == 0.0 && dy == 0.0 {
        return true;
    }
    if limit == 0.0 {
        // Distinct points are never within a hard zero radius.
        return false;
    }
    let d2 = dx * dx + dy * dy;
    let lim2 = limit * limit;
    // Both sides of the compare must be trustworthy normals — a false-zero
    // or positive-subnormal d²/limit² would quantize the boundary (e.g.
    // d=2.2e-162, limit=1.6e-162 wrongly returns true via subnormal squares).
    if d2.is_normal() && lim2.is_normal() {
        d2 <= lim2
    } else {
        point_distance(left, right) <= limit
    }
}

/// Order cuts from `segment.start` to `segment.end` without measuring a
/// distance. A nonconstant source coordinate is monotone along an exact
/// straight segment, so X suffices unless it is constant. The other axis
/// makes rounded off-support cuts deterministic and keeps equal point keys
/// adjacent for deduplication.
#[cfg(test)]
#[expect(
    clippy::float_cmp,
    reason = "cut order uses exact coordinate identity along a stored segment; a tolerance would reorder distinct cuts"
)]
pub(crate) fn compare_along_segment(segment: Segment, left: XY, right: XY) -> std::cmp::Ordering {
    let x_primary = segment.start.x != segment.end.x;
    let compare_axis = |left: f64, right: f64, start: f64, end: f64| {
        if left == right {
            std::cmp::Ordering::Equal
        } else if start <= end {
            left.total_cmp(&right)
        } else {
            right.total_cmp(&left)
        }
    };
    if x_primary {
        compare_axis(left.x, right.x, segment.start.x, segment.end.x)
            .then_with(|| compare_axis(left.y, right.y, segment.start.y, segment.end.y))
    } else {
        compare_axis(left.y, right.y, segment.start.y, segment.end.y)
            .then_with(|| compare_axis(left.x, right.x, segment.start.x, segment.end.x))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Orientation {
    Collinear,
    Clockwise,
    CounterClockwise,
}

#[cfg(test)]
#[path = "segments_tests.rs"]
mod tests;
