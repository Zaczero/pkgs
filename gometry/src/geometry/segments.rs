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
    ExactDyadic, compare_ratios, round_ratio, round_sqrt_ratio,
};
use crate::geometry::{
    Bounds, Coordinates, MOrdinate, Ordering, Point, PointKey, Segment, Shape, XY, ZOrdinate,
    axis_pow2_scale, same_point, same_topological_coordinate, scaled_residual,
};

mod exact;

pub(crate) use exact::orientation;
pub(super) use exact::{CCW_ERRBOUND_A, orientation_xy, ray_crossing_is_right};
pub(in crate::geometry) use exact::{
    DdInterval, crossing_homogeneous, crossing_point, dd_strict_cmp, stored_cross_sign,
    stored_orientation_sign,
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

    pub(super) fn fraction(&self) -> f64 {
        match self {
            Self::Start => 0.0,
            Self::Interior(projection) => projection.fraction,
            Self::CertifiedInterior(projection) => projection.fraction,
            Self::ExactInterior(projection) => projection.fraction,
            Self::End => 1.0,
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
                return Ok((
                    projection.numerator.try_clone()?,
                    projection.denominator.try_clone()?,
                ));
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
/// a permutation through two fallibly reserved index buffers, then apply it
/// in place, so no item is cloned and nothing allocates infallibly.
pub(in crate::geometry) fn try_sort_by<T>(
    items: &mut [T],
    mut compare: impl FnMut(&T, &T) -> Result<Ordering>,
) -> Result<()> {
    const RUN: usize = 16;
    for run in items.chunks_mut(RUN) {
        for index in 1..run.len() {
            let mut cursor = index;
            while cursor > 0 && compare(&run[cursor - 1], &run[cursor])? == Ordering::Greater {
                run.swap(cursor - 1, cursor);
                cursor -= 1;
            }
        }
    }
    let len = items.len();
    if len <= RUN {
        return Ok(());
    }
    let mut order = Vec::new();
    let mut merged = Vec::new();
    crate::try_reserve_vec(&mut order, len)?;
    crate::try_reserve_vec(&mut merged, len)?;
    order.extend(0..len);
    merged.resize(len, 0);
    let mut width = RUN;
    while width < len {
        for start in (0..len).step_by(2 * width) {
            let middle = (start + width).min(len);
            let end = (start + 2 * width).min(len);
            let (mut left, mut right) = (start, middle);
            for slot in &mut merged[start..end] {
                // Only a strictly smaller right item overtakes: ties keep
                // their input order.
                let take_right = left == middle
                    || (right < end
                        && compare(&items[order[left]], &items[order[right]])?
                            == Ordering::Greater);
                *slot = if take_right {
                    right += 1;
                    order[right - 1]
                } else {
                    left += 1;
                    order[left - 1]
                };
            }
        }
        std::mem::swap(&mut order, &mut merged);
        width *= 2;
    }
    // Position `k` takes the item at `order[k]`; each cycle is walked once.
    for start in 0..len {
        let mut current = start;
        while order[current] != current {
            let source = order[current];
            order[current] = current;
            if source == start {
                break;
            }
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

pub(super) fn nearest_points(left: &Shape, right: &Shape) -> Result<Option<(Point, Point)>> {
    let mut nearest = NearestPoints::default();
    // Vertex-pair lists are hoisted once per side; this lane carries FULL
    // ordinates so witness feet interpolate Z/M like the vertices they
    // sit between (the planar `Segment` is XY-only by design).
    let mut left_pairs = Vec::new();
    crate::try_reserve_vec(&mut left_pairs, left.segment_count())?;
    left.for_each_vertex_pair(|start, end| left_pairs.push((start, end)));
    let mut right_pairs = Vec::new();
    crate::try_reserve_vec(&mut right_pairs, right.segment_count())?;
    right.for_each_vertex_pair(|start, end| right_pairs.push((start, end)));

    let mut missing = false;
    left.try_for_each_point(|point| {
        match nearest_point_with_segments(point, right, &right_pairs)? {
            Some(candidate) => nearest.add(point, candidate.point, &candidate.distance_key)?,
            None => missing = true,
        }
        Ok::<_, crate::error::Error>(())
    })?;
    right.try_for_each_point(|point| {
        match nearest_point_with_segments(point, left, &left_pairs)? {
            Some(candidate) => nearest.add(candidate.point, point, &candidate.distance_key)?,
            None => missing = true,
        }
        Ok::<_, crate::error::Error>(())
    })?;
    if missing {
        return Ok(None);
    }
    for left_pair in &left_pairs {
        for right_pair in &right_pairs {
            let candidate = pair_nearest_points(left_pair, right_pair)?;
            nearest.add(candidate.left, candidate.right, &candidate.distance_key)?;
        }
    }

    Ok(nearest.points)
}

/// [`segment_nearest_points`] over full-ordinate vertex pairs: the planar
/// kernel finds the XY witnesses, and each foot lifts back through its
/// host pair so Z/M interpolate.
fn pair_nearest_points(left: &(Point, Point), right: &(Point, Point)) -> Result<FullPairNearest> {
    let left_xy = Segment {
        start: left.0.xy(),
        end: left.1.xy(),
    };
    let right_xy = Segment {
        start: right.0.xy(),
        end: right.1.xy(),
    };
    let nearest = segment_pair_nearest(left_xy, right_xy)?;
    Ok(FullPairNearest {
        left: nearest.left.interpolate_point(left, left_xy)?,
        right: nearest.right.interpolate_point(right, right_xy)?,
        distance_key: nearest.distance_key,
    })
}

#[derive(Clone, Debug)]
enum SegmentWitness {
    Start,
    Projection(SegmentProjection),
    End,
    RoundedIntersection(XY),
}

impl SegmentWitness {
    #[cfg(test)]
    fn xy(&self, segment: Segment) -> XY {
        match self {
            Self::Start => segment.start,
            Self::Projection(projection) => projection.interpolate_xy(segment).unwrap(),
            Self::End => segment.end,
            Self::RoundedIntersection(point) => *point,
        }
    }

    fn interpolate_point(&self, pair: &(Point, Point), segment: Segment) -> Result<Point> {
        Ok(match self {
            Self::Start => pair.0,
            Self::Projection(projection) => projection.interpolate_point(pair.0, pair.1)?,
            Self::End => pair.1,
            Self::RoundedIntersection(point) => {
                segment_projection(*point, segment)?.interpolate_point(pair.0, pair.1)?
            },
        })
    }
}

struct SegmentPairNearest {
    left: SegmentWitness,
    right: SegmentWitness,
    distance_key: SquaredDistanceKey,
}

struct FullPairNearest {
    left: Point,
    right: Point,
    distance_key: SquaredDistanceKey,
}

#[derive(Default)]
pub(super) struct NearestPoints {
    points: Option<(Point, Point)>,
    distance_key: Option<SquaredDistanceKey>,
}

impl NearestPoints {
    fn add(&mut self, left: Point, right: Point, distance_key: &SquaredDistanceKey) -> Result<()> {
        let closer = match self.distance_key {
            None => true,
            Some(incumbent) => distance_key.cmp(&incumbent)?.is_lt(),
        };
        if closer {
            self.points = Some((left, right));
            self.distance_key = Some(*distance_key);
        }
        Ok(())
    }
}

struct NearestPointResult {
    point: Point,
    distance_key: SquaredDistanceKey,
}

/// Nearest point on `geometry` to `point`, with the geometry's segments
/// precomputed so batch callers hoist the segment gather out of their
/// per-vertex loop.
fn nearest_point_with_segments(
    point: Point,
    geometry: &Shape,
    pairs: &[(Point, Point)],
) -> Result<Option<NearestPointResult>> {
    if geometry.contains_point(point)? {
        return Ok(Some(NearestPointResult {
            point,
            distance_key: point_pair_distance_key(point.xy(), point.xy()),
        }));
    }

    let mut nearest = None;
    let mut best_key = None;
    for pair in pairs {
        let segment = Segment {
            start: pair.0.xy(),
            end: pair.1.xy(),
        };
        let selection = point_segment_selection(point.xy(), segment)?;
        let closer = match best_key {
            None => true,
            Some(incumbent) => selection.cmp(point.xy(), segment, &incumbent)?.is_lt(),
        };
        if closer {
            nearest = Some(
                selection
                    .projection(point.xy(), segment)?
                    .interpolate_point(pair.0, pair.1)?,
            );
            best_key = Some(selection.distance_key(point.xy(), segment));
        }
    }
    geometry.try_for_each_point(|candidate| {
        let distance_key = point_pair_distance_key(point.xy(), candidate.xy());
        let closer = match best_key {
            None => true,
            Some(incumbent) => distance_key.cmp(&incumbent)?.is_lt(),
        };
        if closer {
            nearest = Some(candidate);
            best_key = Some(distance_key);
        }
        Ok::<_, crate::error::Error>(())
    })?;
    Ok(nearest
        .zip(best_key)
        .map(|(point, distance_key)| NearestPointResult {
            point,
            distance_key,
        }))
}

pub(crate) fn clip_segment(
    start: impl Into<XY>,
    end: impl Into<XY>,
    rect: Bounds,
) -> Result<Option<(XY, XY)>> {
    let (start, end) = (start.into(), end.into());
    match clip_segment_axis_aligned(start, end, rect) {
        ClipVerdict::Declined => {},
        decided => return Ok(decided.into_clip()),
    }
    match clip_segment_liang_barsky(start, end, rect) {
        ClipVerdict::Declined => {},
        decided => return Ok(decided.into_clip()),
    }
    clip_segment_exact(start, end, rect)
}

/// What a fast clip lane says about a segment: nothing (the next authority
/// decides), a proved miss, or the clipped piece.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum ClipVerdict {
    Declined,
    Disjoint,
    Clipped(XY, XY),
}

impl ClipVerdict {
    /// The clip a decided verdict names; a declined one has none to give.
    pub(super) const fn into_clip(self) -> Option<(XY, XY)> {
        match self {
            Self::Clipped(start, end) => Some((start, end)),
            Self::Disjoint | Self::Declined => None,
        }
    }
}

#[cfg(test)]
fn clip_segment_authority(start: XY, end: XY, rect: Bounds) -> Option<(XY, XY)> {
    match clip_segment_axis_aligned(start, end, rect) {
        ClipVerdict::Declined => {
            clip_segment_exact(start, end, rect).expect("exact clip authority")
        },
        decided => decided.into_clip(),
    }
}

fn clip_segment_axis_aligned(start: XY, end: XY, rect: Bounds) -> ClipVerdict {
    // Axis-aligned clamp: when the free span dwarfs the rect (±1e20 vs unit
    // box), parametric t for both edges collapses to the same value and
    // edge-hit reconstruction degenerates. Clamping the free ordinate keeps
    // exact window coordinates. Bit-exact axis equality (not epsilon).
    if same_topological_coordinate(start.y, end.y) {
        let y = start.y;
        if y < rect.miny() || y > rect.maxy() {
            return ClipVerdict::Disjoint;
        }
        let (lo, hi) = (start.x.min(end.x), start.x.max(end.x));
        if hi < rect.minx() || lo > rect.maxx() {
            return ClipVerdict::Disjoint;
        }
        let x0 = lo.max(rect.minx());
        let x1 = hi.min(rect.maxx());
        if same_topological_coordinate(x0, x1) {
            return ClipVerdict::Disjoint;
        }
        return if start.x <= end.x {
            ClipVerdict::Clipped(XY::new(x0, y), XY::new(x1, y))
        } else {
            ClipVerdict::Clipped(XY::new(x1, y), XY::new(x0, y))
        };
    }
    if same_topological_coordinate(start.x, end.x) {
        let x = start.x;
        if x < rect.minx() || x > rect.maxx() {
            return ClipVerdict::Disjoint;
        }
        let (lo, hi) = (start.y.min(end.y), start.y.max(end.y));
        if hi < rect.miny() || lo > rect.maxy() {
            return ClipVerdict::Disjoint;
        }
        let y0 = lo.max(rect.miny());
        let y1 = hi.min(rect.maxy());
        if same_topological_coordinate(y0, y1) {
            return ClipVerdict::Disjoint;
        }
        return if start.y <= end.y {
            ClipVerdict::Clipped(XY::new(x, y0), XY::new(x, y1))
        } else {
            ClipVerdict::Clipped(XY::new(x, y1), XY::new(x, y0))
        };
    }
    ClipVerdict::Declined
}

fn clip_segment_exact(start: XY, end: XY, rect: Bounds) -> Result<Option<(XY, XY)>> {
    // Exact orientations decide which closed rectangle edges meet the stored
    // segment. Strict crossings use the axis-specific rational placement below;
    // the generic DD crossing witness is a rounded approximation, not an
    // authority for clipping.
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
            },
            &mut hits,
            &mut n_hits,
        );
    }
    if point_in(end) {
        push_clip_hit(
            ClipHit {
                parameter: SegmentParameter::End,
                point: end,
            },
            &mut hits,
            &mut n_hits,
        );
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
                    },
                    &mut hits,
                    &mut n_hits,
                );
                push_clip_hit(
                    ClipHit {
                        parameter: clip_segment_parameter(segment, span.end)?,
                        point: span.end,
                    },
                    &mut hits,
                    &mut n_hits,
                );
            }
        } else if orientations.straddles_or_touches() {
            if orientations.has_collinear() {
                if let Some(point) = touch_endpoint(segment, edge, orientations) {
                    push_clip_hit(
                        ClipHit {
                            parameter: clip_segment_parameter(segment, point)?,
                            point,
                        },
                        &mut hits,
                        &mut n_hits,
                    );
                }
            } else {
                push_clip_hit(
                    clip_segment_edge_cross_exact(segment, edge_start, edge_end)?,
                    &mut hits,
                    &mut n_hits,
                );
            }
        }
    }
    if n_hits < 2 {
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
    let p_min = hits[i_min].as_ref().expect("stored clip hit").point;
    let p_max = hits[i_max].as_ref().expect("stored clip hit").point;
    if same_point(p_min, p_max) {
        return Ok(None);
    }
    Ok(Some((p_min, p_max)))
}

struct ClipHit {
    parameter: SegmentParameter,
    point: XY,
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

fn push_clip_hit(hits: ClipHit, output: &mut [Option<ClipHit>; 6], count: &mut usize) {
    if *count >= output.len()
        || output[..*count]
            .iter()
            .flatten()
            .any(|existing| same_point(existing.point, hits.point))
    {
        return;
    }
    output[*count] = Some(hits);
    *count += 1;
}

fn clip_segment_parameter(segment: Segment, point: XY) -> Result<SegmentParameter> {
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
    let delta_x = end_x.try_subtract(start_x.try_clone()?)?;
    let delta_y = end_y.try_subtract(start_y.try_clone()?)?;
    if same_topological_coordinate(edge_start.x, edge_end.x) {
        let edge_x = ExactDyadic::from_f64(edge_start.x);
        let offset = edge_x.try_subtract(start_x)?;
        let numerator = start_y
            .try_product(&delta_x)?
            .try_add(delta_y.try_product(&offset)?)?;
        let free = round_ratio(&numerator, &delta_x)?;
        Ok(ClipHit {
            parameter: SegmentParameter::Ratio {
                num: offset,
                den: delta_x,
            },
            point: XY::new(edge_start.x, free),
        })
    } else {
        let edge_y = ExactDyadic::from_f64(edge_start.y);
        let offset = edge_y.try_subtract(start_y)?;
        let numerator = start_x
            .try_product(&delta_y)?
            .try_add(delta_x.try_product(&offset)?)?;
        let free = round_ratio(&numerator, &delta_y)?;
        Ok(ClipHit {
            parameter: SegmentParameter::Ratio {
                num: offset,
                den: delta_y,
            },
            point: XY::new(free, edge_start.y),
        })
    }
}

/// Liang-Barsky lane admitted only when every subtraction, ratio, and free-axis
/// reconstruction has a finite, separated rounding cell. `None` declines to
/// the exact rectangle-edge authority; `Some(None)` is a proved disjoint segment.
fn clip_segment_liang_barsky(start: XY, end: XY, rect: Bounds) -> ClipVerdict {
    liang_barsky_decided(start, end, rect).unwrap_or(ClipVerdict::Declined)
}

/// The lane's own answer, where `None` declines to the next authority so the
/// error-bounded arithmetic can decline with `?`.
#[expect(
    clippy::float_cmp,
    reason = "an exact predicate's degeneracy test: these operands are compared for identity, not proximity, and a tolerance would make the predicate inexact"
)]
fn liang_barsky_decided(start: XY, end: XY, rect: Bounds) -> Option<ClipVerdict> {
    #[derive(Clone, Copy)]
    enum Edge {
        MinX,
        MaxX,
        MinY,
        MaxY,
    }

    let (dx, dx_error) = subtraction_with_ulp_error(end.x, start.x)?;
    let (dy, dy_error) = subtraction_with_ulp_error(end.y, start.y)?;
    if !dx.is_normal() || !dy.is_normal() {
        return None;
    }
    let width = rect.maxx() - rect.minx();
    let height = rect.maxy() - rect.miny();
    if !width.is_normal() || !height.is_normal() {
        return None;
    }
    let mut lower: f64 = 0.0;
    let mut upper: f64 = 1.0;
    let mut lower_error: f64 = 0.0;
    let mut upper_error: f64 = 0.0;
    let mut lower_edge = None;
    let mut upper_edge = None;
    for (p, p_error, q, q_error, edge) in [
        (
            -dx,
            dx_error,
            start.x - rect.minx(),
            subtraction_ulp_error(start.x, rect.minx()),
            Edge::MinX,
        ),
        (
            dx,
            dx_error,
            rect.maxx() - start.x,
            subtraction_ulp_error(rect.maxx(), start.x),
            Edge::MaxX,
        ),
        (
            -dy,
            dy_error,
            start.y - rect.miny(),
            subtraction_ulp_error(start.y, rect.miny()),
            Edge::MinY,
        ),
        (
            dy,
            dy_error,
            rect.maxy() - start.y,
            subtraction_ulp_error(rect.maxy(), start.y),
            Edge::MaxY,
        ),
    ] {
        let q_error = q_error?;
        if p == 0.0 {
            match compare_with_error(q, q_error, 0.0, 0.0) {
                Some(std::cmp::Ordering::Less) => return Some(ClipVerdict::Disjoint),
                Some(std::cmp::Ordering::Equal | std::cmp::Ordering::Greater) => continue,
                None => return None,
            }
        }
        let (ratio, ratio_error) = ratio_with_ulp_error(q, q_error, p, p_error)?;
        if p < 0.0 {
            match compare_with_error(ratio, ratio_error, upper, upper_error) {
                Some(std::cmp::Ordering::Greater) => return Some(ClipVerdict::Disjoint),
                Some(std::cmp::Ordering::Equal | std::cmp::Ordering::Less) => {},
                None => return None,
            }
            match compare_with_error(ratio, ratio_error, lower, lower_error) {
                Some(std::cmp::Ordering::Greater) => {
                    lower = ratio;
                    lower_error = ratio_error;
                    lower_edge = Some(edge);
                },
                Some(std::cmp::Ordering::Equal | std::cmp::Ordering::Less) => {},
                None => return None,
            }
        } else {
            match compare_with_error(ratio, ratio_error, lower, lower_error) {
                Some(std::cmp::Ordering::Less) => return Some(ClipVerdict::Disjoint),
                Some(std::cmp::Ordering::Equal | std::cmp::Ordering::Greater) => {},
                None => return None,
            }
            match compare_with_error(ratio, ratio_error, upper, upper_error) {
                Some(std::cmp::Ordering::Less) => {
                    upper = ratio;
                    upper_error = ratio_error;
                    upper_edge = Some(edge);
                },
                Some(std::cmp::Ordering::Equal | std::cmp::Ordering::Greater) => {},
                None => return None,
            }
        }
    }
    if lower > upper {
        return (compare_with_error(lower, lower_error, upper, upper_error)
            == Some(std::cmp::Ordering::Greater))
        .then_some(ClipVerdict::Disjoint);
    }
    if lower == upper {
        return (lower_error == 0.0 && upper_error == 0.0).then_some(ClipVerdict::Disjoint);
    }
    if lower_edge.is_some() && !(lower_error < lower && lower_error < 1.0 - lower) {
        return None;
    }
    if upper_edge.is_some() && !(upper_error < upper && upper_error < 1.0 - upper) {
        return None;
    }
    let origin = start;
    let point_at = |fraction, fraction_error, edge| -> Option<XY> {
        let (free, fixed_value, x_fixed) = match edge {
            Some(Edge::MinX) => (
                reconstruct_free_axis(origin.y, dy, dy_error, fraction, fraction_error)?,
                rect.minx(),
                true,
            ),
            Some(Edge::MaxX) => (
                reconstruct_free_axis(origin.y, dy, dy_error, fraction, fraction_error)?,
                rect.maxx(),
                true,
            ),
            Some(Edge::MinY) => (
                reconstruct_free_axis(origin.x, dx, dx_error, fraction, fraction_error)?,
                rect.miny(),
                false,
            ),
            Some(Edge::MaxY) => (
                reconstruct_free_axis(origin.x, dx, dx_error, fraction, fraction_error)?,
                rect.maxy(),
                false,
            ),
            None => {
                return Some(XY::new(
                    dx.mul_add(fraction, origin.x),
                    dy.mul_add(fraction, origin.y),
                ));
            },
        };
        let (free_value, free_error) = free;
        let (min, max) = if x_fixed {
            (rect.miny(), rect.maxy())
        } else {
            (rect.minx(), rect.maxx())
        };
        let margin = (free_value - min).min(max - free_value);
        if margin.partial_cmp(&free_error) != Some(std::cmp::Ordering::Greater) {
            return None;
        }
        Some(if x_fixed {
            XY::new(fixed_value, free_value)
        } else {
            XY::new(free_value, fixed_value)
        })
    };
    let clipped_start = (lower == 0.0)
        .then_some(origin)
        .map_or_else(|| point_at(lower, lower_error, lower_edge), Some)?;
    let clipped_end = (upper == 1.0)
        .then_some(end)
        .map_or_else(|| point_at(upper, upper_error, upper_edge), Some)?;
    Some(ClipVerdict::Clipped(clipped_start, clipped_end))
}

fn ulp_error(value: f64) -> Option<f64> {
    if !value.is_finite() {
        return None;
    }
    let upward = value.next_up() - value;
    let downward = value - value.next_down();
    let error = upward.max(downward);
    (error.is_finite() && error > 0.0).then_some(error)
}

fn subtraction_ulp_error(left: f64, right: f64) -> Option<f64> {
    let value = left - right;
    value.is_finite().then(|| ulp_error(value)).flatten()
}

fn subtraction_with_ulp_error(left: f64, right: f64) -> Option<(f64, f64)> {
    let value = left - right;
    Some((value, ulp_error(value)?))
}

#[expect(
    clippy::float_cmp,
    reason = "an exact predicate's degeneracy test: these operands are compared for identity, not proximity, and a tolerance would make the predicate inexact"
)]
fn compare_with_error(
    left: f64,
    left_error: f64,
    right: f64,
    right_error: f64,
) -> Option<std::cmp::Ordering> {
    let left_lo = left - left_error;
    let left_hi = left + left_error;
    let right_lo = right - right_error;
    let right_hi = right + right_error;
    if !left_lo.is_finite()
        || !left_hi.is_finite()
        || !right_lo.is_finite()
        || !right_hi.is_finite()
    {
        return None;
    }
    if left_lo > right_hi {
        Some(std::cmp::Ordering::Greater)
    } else if left_hi < right_lo {
        Some(std::cmp::Ordering::Less)
    } else if left_error == 0.0 && right_error == 0.0 && left == right {
        Some(std::cmp::Ordering::Equal)
    } else {
        None
    }
}

#[expect(
    clippy::nonminimal_bool,
    reason = "NaN must fall into the declined branch; the negated comparisons are what make it"
)]
fn ratio_with_ulp_error(
    numerator: f64,
    numerator_error: f64,
    denominator: f64,
    denominator_error: f64,
) -> Option<(f64, f64)> {
    let ratio = numerator / denominator;
    if !ratio.is_normal() {
        return None;
    }
    let numerator_abs = numerator.abs();
    let denominator_abs = denominator.abs();
    let numerator_lo = numerator_abs - numerator_error;
    let numerator_hi = numerator_abs + numerator_error;
    let denominator_lo = denominator_abs - denominator_error;
    let denominator_hi = denominator_abs + denominator_error;
    if !(numerator_lo > 0.0 && denominator_lo > 0.0)
        || !numerator_hi.is_finite()
        || !denominator_hi.is_finite()
    {
        return None;
    }
    let ratio_lo = numerator_lo / denominator_hi;
    let ratio_hi = numerator_hi / denominator_lo;
    let rounding = ulp_error(ratio)?;
    let error = (ratio.abs() - ratio_lo)
        .max(ratio_hi - ratio.abs())
        .max(rounding);
    error.is_finite().then_some((ratio, error))
}

fn reconstruct_free_axis(
    origin: f64,
    delta: f64,
    delta_error: f64,
    fraction: f64,
    fraction_error: f64,
) -> Option<(f64, f64)> {
    let value = delta.mul_add(fraction, origin);
    let propagated =
        delta.abs() * fraction_error + fraction.abs() * delta_error + delta_error * fraction_error;
    let error = propagated + ulp_error(value)?;
    value.is_finite().then_some((value, error))
}

#[cfg(test)]
pub(super) fn segment_nearest_points(left: Segment, right: Segment) -> Result<(XY, XY)> {
    let nearest = segment_pair_nearest(left, right)?;
    Ok((nearest.left.xy(left), nearest.right.xy(right)))
}

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

/// A point lying on BOTH segments, or `None` when they do not meet.
///
/// The one witness primitive. Existence and placement both come from
/// [`segment_contact_exact`], so every returned point is a real shared point:
/// a transversal crossing placed in double-double arithmetic, a touching
/// endpoint returned bit-exactly, or the near end of a collinear overlap run.
///
/// It returns `None` rather than fabricating. The predecessor
/// (`segment_intersection_point`) answered `left.start` for a zero
/// denominator and had no finiteness guard, so a caller could receive a point
/// on neither segment — or a non-finite one — with no way to tell. It backed
/// the public `nearest_points`/`shortest_line` witness.
pub(super) fn segment_contact_point(left: Segment, right: Segment) -> Result<Option<XY>> {
    Ok(match segment_contact_exact(left, right) {
        Contact::Cross => Some(crossing_point(left, right)?.point),
        Contact::Touch { point } => Some(point),
        // A positive-length overlap has no single crossing; its near end is a
        // real point of both segments and is the conventional witness.
        Contact::Collinear { span } => Some(span.start),
        Contact::Disjoint => None,
    })
}

/// Certified clamped point-to-segment projection over the complete finite
/// binary64 domain. The outward interval is an allocation-free proof for the
/// common case. Any unresolved overlap, underflow, overflow, or cancellation
/// falls through to the exact stored-dyadic numerator/denominator owner.
pub(super) fn segment_projection(
    point: impl Into<XY>,
    segment: Segment,
) -> Result<SegmentProjection> {
    let point = point.into();
    projection_from_classification(point, segment, classify_segment_projection(point, segment))
}

// This compact decide/decline filter is the projection hot path. Keeping its
// consumer-specific return handling in the caller removes the otherwise
// dominant per-segment enum call ABI; the exact fallback remains cold and
// out-of-line below.
#[expect(
    clippy::inline_always,
    reason = "measured specialization reduced the ordinary distance ABBA ratio from 1.3043x to 1.224192x"
)]
#[inline(always)]
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
/// have been decided as strictly interior. Keeping it out of the endpoint
/// classifier avoids carrying this larger proof through the distance loop's
/// overwhelmingly common start/end decisions.
#[inline(never)]
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

/// The interval-filter decline is intentionally out of line: keeping limb
/// construction and exact ratio rounding out of the ordinary projection's
/// codegen is material in the distance inner loop.
#[cold]
#[inline(never)]
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
            denominator: denominator.try_clone()?,
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
                        numerator: inner.numerator.try_clone()?,
                        denominator: inner.denominator.try_clone()?,
                        fraction: inner.fraction,
                        emitted_foot: inner.emitted_foot,
                    }),
                )),
                other => Ok(other.clone()),
            },
        }
    }

    const fn witness(&self, point: XY, segment: Segment) -> ExactDistanceWitness {
        match self.witness_kind {
            PointSegmentWitnessKind::Start => ExactDistanceWitness::PointPair(point, segment.start),
            PointSegmentWitnessKind::Interior => {
                ExactDistanceWitness::ProjectedPointPair(point, segment)
            },
            PointSegmentWitnessKind::End => ExactDistanceWitness::PointPair(point, segment.end),
        }
    }

    pub(crate) const fn distance_key(&self, point: XY, segment: Segment) -> SquaredDistanceKey {
        self.filter.bind(self.witness(point, segment))
    }

    pub(crate) fn cmp(
        &self,
        point: XY,
        segment: Segment,
        incumbent: &SquaredDistanceKey,
    ) -> Result<Ordering> {
        self.filter.disjoint_cmp(incumbent.filter).map_or_else(
            || exact_witness_cmp_with_identity(self.witness(point, segment), incumbent.witness),
            Ok,
        )
    }
}

/// The single projection owner classifies every clamped parameter before a
/// metric consumes it. The measurement and selection key both use the source
/// determinant; projection emission is a later witness operation.
#[expect(
    clippy::inline_always,
    reason = "measured specialization reduced the ordinary distance ABBA ratio from 1.3043x to 1.224192x"
)]
#[inline(always)]
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

/// Exact classification and squared-ratio rounding are a decline path. They
/// stay out of the ordinary metric's frame so a certified endpoint/interior
/// does not carry heap-backed dyadic state through the distance inner loop.
#[cold]
#[inline(never)]
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
    PointPair(XY, XY),
    ProjectedPointPair(XY, Segment),
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
    pub(super) fn is_exact_zero(&self) -> Result<bool> {
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

    #[cfg(test)]
    fn contains_exact(self) -> bool {
        if !self.filter.lo.is_finite() || !self.filter.hi.is_finite() {
            return true;
        }
        let exact = self.witness.squared().unwrap();
        let denominator = ExactDyadic::from_f64(1.0);
        let lo = ExactSquaredDistance {
            numerator: ExactDyadic::from_f64(self.filter.lo),
            denominator: denominator.try_clone().unwrap(),
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

pub(super) fn point_pair_distance_key(left: XY, right: XY) -> SquaredDistanceKey {
    let witness = ExactDistanceWitness::PointPair(left, right);
    let dx = left.x - right.x;
    let dy = left.y - right.y;
    let squared = dx * dx + dy * dy;
    if squared_norm_is_trustworthy(squared, dx == 0.0 && dy == 0.0) {
        SquaredDistanceFilter::point_pair(squared).bind(witness)
    } else {
        SquaredDistanceFilter::unbounded().bind(witness)
    }
}

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
    static EXACT_SELECTION_FALLBACKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

impl ExactDistanceWitness {
    const fn tie_rank(self) -> u8 {
        match self {
            // On an exact source-distance tie, retain an interior projection's
            // ordinate provenance rather than collapsing it to an endpoint.
            Self::ProjectedPointPair(..) => 0,
            Self::PointPair(..) => 1,
        }
    }

    fn same_distance_as(self, other: Self) -> bool {
        match (self, other) {
            (Self::PointPair(left_a, left_b), Self::PointPair(right_a, right_b)) => {
                (same_point(left_a, right_a) && same_point(left_b, right_b))
                    || (same_point(left_a, right_b) && same_point(left_b, right_a))
            },
            _ => false,
        }
    }

    fn squared(self) -> Result<ExactSquaredDistance> {
        match self {
            Self::PointPair(left, right) => ExactSquaredDistance::point_pair(left, right),
            Self::ProjectedPointPair(point, segment) => exact_source_squared(point, segment),
        }
    }
}

pub(super) fn exact_distance_witness_cmp(
    left: ExactDistanceWitness,
    right: ExactDistanceWitness,
) -> Result<Ordering> {
    left.squared()?.cmp(&right.squared()?)
}

fn exact_witness_cmp_with_identity(
    left: ExactDistanceWitness,
    right: ExactDistanceWitness,
) -> Result<Ordering> {
    if left.same_distance_as(right) {
        return Ok(left.tie_rank().cmp(&right.tie_rank()));
    }
    #[cfg(test)]
    EXACT_SELECTION_FALLBACKS.with(|count| count.set(count.get() + 1));
    Ok(exact_distance_witness_cmp(left, right)?.then(left.tie_rank().cmp(&right.tie_rank())))
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
/// The convex form keeps the result bounded by `max(|start|, |end|)` for
/// `fraction` in `[0, 1]`, avoiding the `end - start` overflow class, and is
/// exact at the endpoints (`fraction` 0 → `start`, 1 → `end`).
pub(crate) fn interpolate_f64(start: f64, end: f64, fraction: f64) -> f64 {
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
        Contact::Touch { point } => Some(point),
        Contact::Disjoint | Contact::Collinear { .. } => None,
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

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum Contact {
    Disjoint,
    /// A proper crossing, interior to both segments; [`crossing_point`]
    /// places it.
    Cross,
    Touch {
        point: XY,
    },
    Collinear {
        span: SharedSpan,
    },
}

pub(super) fn segment_contact_with_orientations(
    left: Segment,
    right: Segment,
) -> (Contact, PairOrientations) {
    let orientations = PairOrientations::new(left, right);
    let contact = if orientations.all_collinear() {
        collinear_shared_span_exact(left, right)
            .map_or(Contact::Disjoint, |span| Contact::Collinear { span })
    } else if orientations.straddles_or_touches() {
        if orientations.has_collinear() {
            touch_endpoint(left, right, orientations)
                .map_or(Contact::Disjoint, |point| Contact::Touch { point })
        } else {
            Contact::Cross
        }
    } else {
        Contact::Disjoint
    };
    (contact, orientations)
}

pub(super) fn segment_contact_exact(left: Segment, right: Segment) -> Contact {
    segment_contact_with_orientations(left, right).0
}

pub(super) fn segment_contact(left: Segment, right: Segment) -> SegmentContact {
    match segment_contact_exact(left, right) {
        Contact::Disjoint => SegmentContact::None,
        Contact::Cross => SegmentContact::Cross,
        Contact::Touch { .. } | Contact::Collinear { .. } => SegmentContact::Touch,
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
    let Contact::Collinear { span } = segment_contact_exact(left, right) else {
        return None;
    };
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

/// Honest squared distance point→segment. `None` on overflow or false-zero
/// underflow (representable positive separation whose `ex²+ey²` collapses to
/// 0). Magnitude / zero-tests must not treat a bare `0.0` from
/// [`point_segment_distance_squared`] as contact — use this, or finish via
/// [`finish_planar_squared_min`].
pub(super) fn try_point_segment_distance_squared(
    point: impl Into<XY>,
    segment: Segment,
) -> Result<Option<f64>> {
    let point = point.into();
    Ok(match point_segment_metric(point, segment)? {
        PointSegmentMetric::Squared {
            measurement_squared,
            ..
        } => Some(measurement_squared),
        PointSegmentMetric::Projection(_) => None,
    })
}

/// Squared distance from `point` to `segment` — self-contained MEASUREMENT.
/// Ordering / compare-min callers may use this; a returned `0.0` is **not**
/// a contact certificate under underflow (use [`try_point_segment_distance_squared`]
/// or [`finish_planar_squared_min`] for magnitude answers).
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
