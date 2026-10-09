//! Original-source geographic chart inventory and exact noding.
//!
//! Binary64 coordinates and the frame period are admitted values, not rounded
//! construction results. Chart cuts and contacts retain exact source
//! parameters; their binary64 coordinates are derived witnesses only.

use std::cmp::Ordering;
use std::ops::Range;

use crate::error::{ResourceError, Result};
#[cfg(test)]
use crate::geographic::GeographicInputs;
use crate::geographic::{
    GeographicFrame, Latitude, RetainedChartDisplacement, RingDomain, RingSummary,
    ScalarGeographicView, SeamIdentity,
};
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::segments::try_sort_by_with_work;
use crate::geometry::tessellation::exact::{CompactLimbs, ExactDyadic, ExactRational};
use crate::geometry::{
    Bounds, Component, Coordinates as _, ExpansionBudget, GeometryErrorKind, Point, Polygon,
    RingKind, Segment, Shape, Unmetered, Work, XY, for_each_candidate_pair,
    for_each_overlapping_bounds_cross_pair,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct SourceRowId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct SourcePolygonId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct SourceRingId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct SourceLineId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct SourceEdgeId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct ChartPieceId(pub u32);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::geometry) struct ExactChartPoint {
    pub(in crate::geometry) x: ExactRational,
    pub(in crate::geometry) y: ExactRational,
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "HeapSize reports capacities of already-allocated dyadic limbs"
)]
pub(super) const fn dyadic_heap_bytes(value: &ExactDyadic) -> usize {
    match &value.limbs {
        CompactLimbs::Inline { .. } => 0,
        CompactLimbs::Heap(limbs) => limbs.capacity() * std::mem::size_of::<u64>(),
    }
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "HeapSize sums the two already-owned rational limbs"
)]
pub(super) fn rational_heap_bytes(value: &ExactRational) -> usize {
    let (numerator, denominator) = value.parts();
    dyadic_heap_bytes(numerator) + dyadic_heap_bytes(denominator)
}

fn quotient_key_heap_bytes(value: &ExactQuotientKey) -> usize {
    match value {
        ExactQuotientKey::Coordinate(point) => point.heap_bytes(),
        ExactQuotientKey::Seam(latitude) => rational_heap_bytes(latitude),
        ExactQuotientKey::NorthPole | ExactQuotientKey::SouthPole => 0,
    }
}

impl ExactChartPoint {
    #[cfg(test)]
    fn from_xy(point: XY) -> Self {
        Self {
            x: ExactRational::from_f64(point.x),
            y: ExactRational::from_f64(point.y),
        }
    }

    fn from_dyadics(x: ExactDyadic, y: ExactDyadic) -> Self {
        Self {
            x: ExactRational::from_dyadic(x),
            y: ExactRational::from_dyadic(y),
        }
    }

    pub(in crate::geometry) const fn from_rationals(x: ExactRational, y: ExactRational) -> Self {
        Self { x, y }
    }

    pub(in crate::geometry) fn try_clone_with_work(&self, work: &mut impl Work) -> Result<Self> {
        Ok(Self {
            x: self.x.try_clone_with_work(work)?,
            y: self.y.try_clone_with_work(work)?,
        })
    }

    pub(in crate::geometry) fn same_position(&self, other: &Self) -> Result<bool> {
        Ok(self.x.try_cmp(&other.x)?.is_eq() && self.y.try_cmp(&other.y)?.is_eq())
    }

    pub(in crate::geometry) fn try_eq_with_work(
        &self,
        other: &Self,
        work: &mut impl Work,
    ) -> Result<bool> {
        Ok(self.x.try_eq_with_work(&other.x, work)? && self.y.try_eq_with_work(&other.y, work)?)
    }

    pub(in crate::geometry) fn compare_lex(&self, other: &Self) -> Result<Ordering> {
        let mut work = Unmetered;
        self.compare_lex_with_work(other, &mut work)
    }

    pub(in crate::geometry) fn compare_lex_with_work(
        &self,
        other: &Self,
        work: &mut impl Work,
    ) -> Result<Ordering> {
        let x = self.x.try_cmp_with_work(&other.x, work)?;
        Ok(if x.is_eq() {
            self.y.try_cmp_with_work(&other.y, work)?
        } else {
            x
        })
    }

    pub(in crate::geometry) fn compare_x(&self, other: &Self) -> Result<Ordering> {
        self.x.try_cmp(&other.x)
    }

    pub(in crate::geometry) fn compare_y(&self, other: &Self) -> Result<Ordering> {
        self.y.try_cmp(&other.y)
    }

    pub(in crate::geometry) fn outward_bounds(&self) -> Result<[f64; 4]> {
        let (min_x, max_x) = self.x.outward_bounds()?;
        let (min_y, max_y) = self.y.outward_bounds()?;
        Ok([min_x, min_y, max_x, max_y])
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "HeapSize sums the two already-owned chart-coordinate rationals"
    )]
    pub(super) fn heap_bytes(&self) -> usize {
        rational_heap_bytes(&self.x) + rational_heap_bytes(&self.y)
    }
}

impl ExactChartPiece {
    fn outward_proxy(&self) -> Result<Option<Segment>> {
        let [start_min_x, start_min_y, start_max_x, start_max_y] = self.start.outward_bounds()?;
        let [end_min_x, end_min_y, end_max_x, end_max_y] = self.end.outward_bounds()?;
        let bounds = [
            start_min_x.min(end_min_x),
            start_min_y.min(end_min_y),
            start_max_x.max(end_max_x),
            start_max_y.max(end_max_y),
        ];
        if !bounds.into_iter().all(f64::is_finite) {
            return Ok(None);
        }
        let [min_x, min_y, max_x, max_y] = bounds;
        Ok(Some(Segment {
            start: XY::new(min_x, min_y),
            end: XY::new(max_x, max_y),
        }))
    }

    fn outward_bounds(&self) -> Result<Option<Bounds>> {
        let Some(proxy) = self.outward_proxy()? else {
            return Ok(None);
        };
        Ok(Some(Bounds::new_raw(
            proxy.start.x,
            proxy.start.y,
            proxy.end.x,
            proxy.end.y,
        )))
    }

    fn shifted_x(&self, shift: &ExactRational) -> Result<Self> {
        let shift_point = |point: &ExactChartPoint| -> Result<ExactChartPoint> {
            Ok(ExactChartPoint::from_rationals(
                point.x.try_add(shift)?,
                point.y.clone(),
            ))
        };
        Ok(Self {
            id: self.id,
            source_edge: self.source_edge,
            start: shift_point(&self.start)?,
            end: shift_point(&self.end)?,
            source_range: self.source_range.clone(),
            lift: self.lift,
        })
    }
}

pub(super) fn quotient_key(
    frame: GeographicFrame,
    point: &ExactChartPoint,
) -> Result<ExactQuotientKey> {
    let mut work = Unmetered;
    quotient_key_with_work(frame, point, &mut work)
}

pub(super) fn quotient_key_with_work(
    frame: GeographicFrame,
    point: &ExactChartPoint,
    work: &mut impl Work,
) -> Result<ExactQuotientKey> {
    let north =
        ExactRational::from_f64(frame.chart_pole_latitude(crate::geographic::ChartPole::North));
    let south =
        ExactRational::from_f64(frame.chart_pole_latitude(crate::geographic::ChartPole::South));
    if point.y.try_cmp_with_work(&north, work)?.is_eq() {
        return Ok(ExactQuotientKey::NorthPole);
    }
    if point.y.try_cmp_with_work(&south, work)?.is_eq() {
        return Ok(ExactQuotientKey::SouthPole);
    }
    let half = ExactRational::from_f64(frame.longitude().half_period());
    if point.x.try_cmp_with_work(&half, work)?.is_eq()
        || point
            .x
            .try_cmp_with_work(
                &half.try_subtract_with_work(
                    &ExactRational::from_f64(frame.longitude().period()),
                    work,
                )?,
                work,
            )?
            .is_eq()
    {
        return Ok(ExactQuotientKey::Seam(point.y.try_clone_with_work(work)?));
    }
    Ok(ExactQuotientKey::Coordinate(
        point.try_clone_with_work(work)?,
    ))
}

pub(super) fn quotient_key_for_chart_position(
    frame: GeographicFrame,
    point: &ExactChartPoint,
) -> Result<ExactQuotientKey> {
    let mut work = Unmetered;
    quotient_key_for_chart_position_with_work(frame, point, &mut work)
}

pub(super) fn quotient_key_for_chart_position_with_work(
    frame: GeographicFrame,
    point: &ExactChartPoint,
    work: &mut impl Work,
) -> Result<ExactQuotientKey> {
    let (native, _) = crate::geometry::exact_arith::split_rational_native_site_with_work(
        &point.x,
        frame.longitude().period(),
        frame.longitude().half_period(),
        work,
    )?;
    quotient_key_with_work(
        frame,
        &ExactChartPoint::from_rationals(native.into_owned(), point.y.try_clone_with_work(work)?),
        work,
    )
}

pub(super) fn compare_quotient_keys(
    left: &ExactQuotientKey,
    right: &ExactQuotientKey,
) -> Result<Ordering> {
    let mut work = Unmetered;
    compare_quotient_keys_with_work(left, right, &mut work)
}

pub(super) fn compare_quotient_keys_with_work(
    left: &ExactQuotientKey,
    right: &ExactQuotientKey,
    work: &mut impl Work,
) -> Result<Ordering> {
    use ExactQuotientKey::{Coordinate, NorthPole, Seam, SouthPole};
    let rank = |key: &ExactQuotientKey| match key {
        Coordinate(_) => 0_u8,
        Seam(_) => 1,
        NorthPole => 2,
        SouthPole => 3,
    };
    let rank_order = rank(left).cmp(&rank(right));
    if !rank_order.is_eq() {
        return Ok(rank_order);
    }
    match (left, right) {
        (Coordinate(left), Coordinate(right)) => left.compare_lex_with_work(right, work),
        (Seam(left), Seam(right)) => left.try_cmp_with_work(right, work),
        (NorthPole, NorthPole) | (SouthPole, SouthPole) => Ok(Ordering::Equal),
        _ => unreachable!("equal quotient-key ranks have the same variant"),
    }
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "binary-search bounds remain within the sorted exact chart-port slice"
)]
#[cfg(test)]
fn find_chart_port(ports: &[ChartPort], point: &ExactChartPoint) -> Result<ChartPortId> {
    let mut work = Unmetered;
    find_chart_port_with_work(ports, point, &mut work)
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "binary-search bounds remain within the sorted exact chart-port slice"
)]
fn find_chart_port_with_work<W: Work + ?Sized>(
    ports: &[ChartPort],
    point: &ExactChartPoint,
    work: &mut W,
) -> Result<ChartPortId> {
    let mut low = 0_usize;
    let mut high = ports.len();
    while low < high {
        work.charge(1)?;
        let middle = low + (high - low) / 2;
        match ports[middle].point.compare_lex(point)? {
            Ordering::Less => low = middle + 1,
            Ordering::Greater => high = middle,
            Ordering::Equal => return Ok(ports[middle].id),
        }
    }
    Err(GeometryErrorKind::Invalid("exact chart port was not interned".into()).into())
}

pub(super) fn source_chart_point(frame: GeographicFrame, point: Point) -> ExactChartPoint {
    let canonical = frame
        .longitude()
        .canonicalize(point.x)
        .expect("source longitude was admitted");
    ExactChartPoint::from_dyadics(
        ExactDyadic::from_f64(seam_start_spelling(frame.longitude(), point.x, canonical)),
        ExactDyadic::from_f64(Latitude::from_admitted(frame, point.y).get()),
    )
}

pub(in crate::geometry) fn vector_difference(
    start: &ExactChartPoint,
    end: &ExactChartPoint,
) -> Result<(ExactRational, ExactRational)> {
    let mut work = Unmetered;
    vector_difference_with_work(start, end, &mut work)
}

pub(in crate::geometry) fn vector_difference_with_work(
    start: &ExactChartPoint,
    end: &ExactChartPoint,
    work: &mut impl Work,
) -> Result<(ExactRational, ExactRational)> {
    Ok((
        end.x.try_subtract_with_work(&start.x, work)?,
        end.y.try_subtract_with_work(&start.y, work)?,
    ))
}

pub(in crate::geometry) fn vector_cross(
    left: &(ExactRational, ExactRational),
    right: &(ExactRational, ExactRational),
) -> Result<ExactRational> {
    let mut work = Unmetered;
    vector_cross_with_work(left, right, &mut work)
}

pub(in crate::geometry) fn vector_cross_with_work(
    left: &(ExactRational, ExactRational),
    right: &(ExactRational, ExactRational),
    work: &mut impl Work,
) -> Result<ExactRational> {
    left.0
        .try_product_with_work(&right.1, work)?
        .try_subtract_with_work(&left.1.try_product_with_work(&right.0, work)?, work)
}

pub(in crate::geometry) fn point_on_exact_segment(
    point: &ExactChartPoint,
    start: &ExactChartPoint,
    end: &ExactChartPoint,
) -> Result<bool> {
    let mut work = Unmetered;
    point_on_exact_segment_with_work(point, start, end, &mut work)
}

pub(in crate::geometry) fn point_on_exact_segment_with_work(
    point: &ExactChartPoint,
    start: &ExactChartPoint,
    end: &ExactChartPoint,
    work: &mut impl Work,
) -> Result<bool> {
    let line = vector_difference_with_work(start, end, work)?;
    let offset = vector_difference_with_work(start, point, work)?;
    if !vector_cross_with_work(&line, &offset, work)?.is_zero() {
        return Ok(false);
    }
    let (min_x, max_x) = if start.x.try_cmp_with_work(&end.x, work)?.is_le() {
        (&start.x, &end.x)
    } else {
        (&end.x, &start.x)
    };
    let (min_y, max_y) = if start.y.try_cmp_with_work(&end.y, work)?.is_le() {
        (&start.y, &end.y)
    } else {
        (&end.y, &start.y)
    };
    Ok(point.x.try_cmp_with_work(min_x, work)?.is_ge()
        && point.x.try_cmp_with_work(max_x, work)?.is_le()
        && point.y.try_cmp_with_work(min_y, work)?.is_ge()
        && point.y.try_cmp_with_work(max_y, work)?.is_le())
}

pub(super) fn parameter_on_exact_segment(
    point: &ExactChartPoint,
    start: &ExactChartPoint,
    end: &ExactChartPoint,
) -> Result<ExactRational> {
    if !start.x.try_cmp(&end.x)?.is_eq() {
        return point
            .x
            .try_subtract(&start.x)?
            .try_quotient(&end.x.try_subtract(&start.x)?);
    }
    if !start.y.try_cmp(&end.y)?.is_eq() {
        return point
            .y
            .try_subtract(&start.y)?
            .try_quotient(&end.y.try_subtract(&start.y)?);
    }
    Err(
        GeometryErrorKind::Invalid("a point parameter requires a nondegenerate exact span".into())
            .into(),
    )
}

pub(super) fn interpolate_exact_segment(
    start: &ExactChartPoint,
    end: &ExactChartPoint,
    parameter: &ExactRational,
) -> Result<ExactChartPoint> {
    let (dx, dy) = vector_difference(start, end)?;
    Ok(ExactChartPoint::from_rationals(
        start.x.try_add(&dx.try_product(parameter)?)?,
        start.y.try_add(&dy.try_product(parameter)?)?,
    ))
}

pub(super) fn in_unit_interval(parameter: &ExactRational) -> Result<bool> {
    let mut work = Unmetered;
    in_unit_interval_with_work(parameter, &mut work)
}

pub(in crate::geometry) fn in_unit_interval_with_work(
    parameter: &ExactRational,
    work: &mut impl Work,
) -> Result<bool> {
    Ok(parameter.sign() != ExactSign::Negative
        && parameter
            .try_cmp_with_work(&ExactRational::one(), work)?
            .is_le())
}

fn compare_cut_events(left: &CutEvent, right: &CutEvent) -> Result<Ordering> {
    let piece = left.piece.cmp(&right.piece);
    Ok(if piece.is_eq() {
        left.parameter.try_cmp(&right.parameter)?
    } else {
        piece
    })
}

fn insert_cut_event<W: Work + ?Sized>(
    cuts: &mut FallibleAvl<CutEvent>,
    piece: &ExactChartPiece,
    parameter: ExactRational,
    point: ExactChartPoint,
    work: &mut W,
    compare: &mut impl FnMut(&CutEvent, &CutEvent, &mut W) -> Result<Ordering>,
) -> Result<()> {
    work.charge(1)?;
    if parameter.is_zero() {
        work.charge(1)?;
        if !point.same_position(&piece.start)? {
            return Err(GeometryErrorKind::Invalid(
                "an exact source-start parameter produced a different chart point".into(),
            )
            .into());
        }
        return Ok(());
    }
    if parameter.is_one() {
        work.charge(1)?;
        if !point.same_position(&piece.end)? {
            return Err(GeometryErrorKind::Invalid(
                "an exact source-end parameter produced a different chart point".into(),
            )
            .into());
        }
        return Ok(());
    }
    let event = CutEvent {
        piece: piece.id,
        parameter,
        point,
    };
    work.charge(1)?;
    if !cuts.insert_with_work(event.clone(), work, compare)?
        && !cuts
            .get_with_work(&event, work, compare)?
            .expect("a duplicate exact cut remains in its owner tree")
            .point
            .same_position(&event.point)?
    {
        return Err(GeometryErrorKind::Invalid(
            "one exact chart parameter produced two contact points".into(),
        )
        .into());
    }
    Ok(())
}

fn exact_piece_contacts<W: Work + ?Sized>(
    left: &ExactChartPiece,
    right: &ExactChartPiece,
    work: &mut W,
    mut contact: impl FnMut(ExactChartPoint, ExactRational, ExactRational, &mut W) -> Result<()>,
) -> Result<()> {
    work.charge(1)?;
    let r = vector_difference(&left.start, &left.end)?;
    work.charge(1)?;
    let s = vector_difference(&right.start, &right.end)?;
    work.charge(1)?;
    let q_minus_p = vector_difference(&left.start, &right.start)?;
    work.charge(1)?;
    let denominator = vector_cross(&r, &s)?;
    if denominator.is_zero() {
        work.charge(1)?;
        if !vector_cross(&q_minus_p, &r)?.is_zero() {
            return Ok(());
        }
        for point in [&left.start, &left.end, &right.start, &right.end] {
            work.charge(1)?;
            if point_on_exact_segment(point, &left.start, &left.end)?
                && point_on_exact_segment(point, &right.start, &right.end)?
            {
                work.charge(2)?;
                contact(
                    (*point).clone(),
                    parameter_on_exact_segment(point, &left.start, &left.end)?,
                    parameter_on_exact_segment(point, &right.start, &right.end)?,
                    work,
                )?;
            }
        }
        return Ok(());
    }

    work.charge(1)?;
    let left_parameter = vector_cross(&q_minus_p, &s)?.try_quotient(&denominator)?;
    work.charge(1)?;
    if !in_unit_interval(&left_parameter)? {
        return Ok(());
    }
    work.charge(1)?;
    let right_parameter = vector_cross(&q_minus_p, &r)?.try_quotient(&denominator)?;
    work.charge(1)?;
    if !in_unit_interval(&right_parameter)? {
        return Ok(());
    }
    work.charge(1)?;
    let point = interpolate_exact_segment(&left.start, &left.end, &left_parameter)?;
    contact(point, left_parameter, right_parameter, work)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ExactSourceRange {
    pub start: ExactRational,
    pub end: ExactRational,
}

impl ExactSourceRange {
    pub(super) fn compose_with_work(
        &self,
        local_start: &ExactRational,
        local_end: &ExactRational,
        work: &mut impl Work,
    ) -> Result<Self> {
        let width = self.end.try_subtract_with_work(&self.start, work)?;
        Ok(Self {
            start: self
                .start
                .try_add_with_work(&width.try_product_with_work(local_start, work)?, work)?,
            end: self
                .start
                .try_add_with_work(&width.try_product_with_work(local_end, work)?, work)?,
        })
    }

    pub(super) fn try_eq_with_work(&self, other: &Self, work: &mut impl Work) -> Result<bool> {
        Ok(self.start.try_eq_with_work(&other.start, work)?
            && self.end.try_eq_with_work(&other.end, work)?)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SourceEdgeKind {
    Line {
        line: SourceLineId,
    },
    Ring {
        polygon: SourcePolygonId,
        ring: SourceRingId,
        hole: bool,
        kind: RingKind,
    },
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ExactSourceEdge {
    pub id: SourceEdgeId,
    pub source: [Point; 2],
    longitude: RetainedChartDisplacement,
    pub row: SourceRowId,
    pub kind: SourceEdgeKind,
}

impl ExactSourceEdge {
    pub(super) const fn retained_longitude(&self) -> RetainedChartDisplacement {
        self.longitude
    }

    pub(super) fn source_interval_reads_as(
        &self,
        frame: GeographicFrame,
        reversed: bool,
        start: Point,
        end: Point,
    ) -> Result<bool> {
        let [source_start, source_end] = if reversed {
            [self.source[1], self.source[0]]
        } else {
            self.source
        };
        let longitude = if reversed {
            let [first, second] = *self.longitude.endpoints();
            RetainedChartDisplacement::from_native_endpoints(
                [second, first],
                self.longitude
                    .periods()
                    .checked_neg()
                    .ok_or(ResourceError::SizeOverflow)?,
            )
        } else {
            self.longitude
        };
        let readback = frame
            .longitude()
            .chart_step(start.x, end.x)
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        crate::geometry::exact_arith::retained_source_interval_reads_as(
            longitude,
            frame,
            [source_start.y, source_end.y],
            0.0,
            1.0,
            readback,
            start,
            end,
        )
    }
}

#[derive(Clone, Debug)]
pub(super) struct ExactChartPiece {
    pub id: ChartPieceId,
    pub source_edge: SourceEdgeId,
    pub start: ExactChartPoint,
    pub end: ExactChartPoint,
    pub source_range: ExactSourceRange,
    /// Periods from this chart piece to the source ring's continuous lift.
    pub lift: i64,
}

#[derive(Clone, Debug)]
struct CutEvent {
    piece: ChartPieceId,
    parameter: ExactRational,
    point: ExactChartPoint,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct ChartPortId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct QuotientNodeId(pub u32);

#[derive(Clone, Debug)]
pub(super) struct ChartPort {
    pub id: ChartPortId,
    pub point: ExactChartPoint,
    pub node: QuotientNodeId,
}

#[derive(Clone, Debug)]
pub(super) struct QuotientNode {
    pub id: QuotientNodeId,
    pub ports: Range<usize>,
    pub key: ExactQuotientKey,
}

#[derive(Clone, Debug)]
pub(super) enum ExactQuotientKey {
    Coordinate(ExactChartPoint),
    Seam(ExactRational),
    NorthPole,
    SouthPole,
}

#[derive(Clone, Debug)]
pub(super) struct ExactSpanOwner {
    pub source_edge: SourceEdgeId,
    pub source_range: ExactSourceRange,
    pub reversed: bool,
    pub lift: i64,
}

#[derive(Clone, Debug)]
pub(super) struct ExactChartAtom {
    pub ports: [ChartPortId; 2],
    pub owners: Box<[ExactSpanOwner]>,
}

#[derive(Debug)]
pub(super) struct NodedExactChart {
    pub ports: Box<[ChartPort]>,
    pub nodes: Box<[QuotientNode]>,
    pub node_ports: Box<[ChartPortId]>,
    pub atoms: Box<[ExactChartAtom]>,
    pub source_edge_ports: Box<[[ChartPortId; 2]]>,
    pub source_point_ports: Box<[ChartPortId]>,
}

struct AtomOccurrence {
    start: ExactChartPoint,
    end: ExactChartPoint,
    source_edge: SourceEdgeId,
    source_range: ExactSourceRange,
    lift: i64,
}

struct AvlNode<T> {
    value: T,
    left: Option<usize>,
    right: Option<usize>,
    height: usize,
}

pub(super) struct FallibleAvl<T> {
    nodes: Vec<AvlNode<T>>,
    root: Option<usize>,
}

#[expect(
    clippy::indexing_slicing,
    reason = "AVL child links are private indices into the node vector and are maintained by rotations"
)]
impl<T> FallibleAvl<T> {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self {
            nodes: Vec::with_capacity(capacity),
            root: None,
        }
    }

    pub(super) const fn len(&self) -> usize {
        self.nodes.len()
    }

    #[cfg(test)]
    pub(super) fn tree_height(&self) -> usize {
        self.height(self.root)
    }

    fn height(&self, node: Option<usize>) -> usize {
        node.map_or(0, |node| self.nodes[node].height)
    }

    fn refresh(&mut self, node: usize) {
        let child_height = self
            .height(self.nodes[node].left)
            .max(self.height(self.nodes[node].right));
        self.nodes[node].height = child_height
            .checked_add(1)
            .expect("tree height fits the allocated node vector");
    }

    fn rotate_left(&mut self, root: usize) -> usize {
        let top = self.nodes[root]
            .right
            .expect("a left rotation has a right child");
        self.nodes[root].right = self.nodes[top].left;
        self.nodes[top].left = Some(root);
        self.refresh(root);
        self.refresh(top);
        top
    }

    fn rotate_right(&mut self, root: usize) -> usize {
        let top = self.nodes[root]
            .left
            .expect("a right rotation has a left child");
        self.nodes[root].left = self.nodes[top].right;
        self.nodes[top].right = Some(root);
        self.refresh(root);
        self.refresh(top);
        top
    }

    fn balance(&mut self, root: usize) -> usize {
        self.refresh(root);
        let left_height = self.height(self.nodes[root].left);
        let right_height = self.height(self.nodes[root].right);
        if left_height > right_height && left_height.abs_diff(right_height) > 1 {
            let left = self.nodes[root]
                .left
                .expect("a left-heavy node has a left child");
            if self.height(self.nodes[left].left) < self.height(self.nodes[left].right) {
                self.nodes[root].left = Some(self.rotate_left(left));
            }
            return self.rotate_right(root);
        }
        if right_height > left_height && right_height.abs_diff(left_height) > 1 {
            let right = self.nodes[root]
                .right
                .expect("a right-heavy node has a right child");
            if self.height(self.nodes[right].right) < self.height(self.nodes[right].left) {
                self.nodes[root].right = Some(self.rotate_right(right));
            }
            return self.rotate_left(root);
        }
        root
    }

    pub(super) fn insert(
        &mut self,
        value: T,
        compare: &mut impl FnMut(&T, &T) -> Result<Ordering>,
    ) -> Result<bool> {
        let mut work = Unmetered;
        self.insert_with_work(value, &mut work, |left, right, _work| compare(left, right))
    }

    pub(super) fn insert_with_work<W: Work + ?Sized>(
        &mut self,
        value: T,
        work: &mut W,
        compare: &mut impl FnMut(&T, &T, &mut W) -> Result<Ordering>,
    ) -> Result<bool> {
        fn descend<T, W: Work + ?Sized>(
            tree: &mut FallibleAvl<T>,
            root: Option<usize>,
            value: T,
            work: &mut W,
            compare: &mut impl FnMut(&T, &T, &mut W) -> Result<Ordering>,
        ) -> Result<(usize, bool)> {
            work.charge(1)?;
            let Some(root) = root else {
                work.charge(1)?;
                let index = tree.nodes.len();
                tree.nodes.push(AvlNode {
                    value,
                    left: None,
                    right: None,
                    height: 1,
                });
                return Ok((index, true));
            };
            work.charge(1)?;
            match compare(&value, &tree.nodes[root].value, work)? {
                Ordering::Equal => Ok((root, false)),
                Ordering::Less => {
                    let (left, inserted) =
                        descend(tree, tree.nodes[root].left, value, work, compare)?;
                    work.charge(1)?;
                    tree.nodes[root].left = Some(left);
                    Ok((tree.balance(root), inserted))
                },
                Ordering::Greater => {
                    let (right, inserted) =
                        descend(tree, tree.nodes[root].right, value, work, compare)?;
                    work.charge(1)?;
                    tree.nodes[root].right = Some(right);
                    Ok((tree.balance(root), inserted))
                },
            }
        }

        let (root, inserted) = descend(self, self.root, value, work, compare)?;
        work.charge(1)?;
        self.root = Some(root);
        Ok(inserted)
    }

    pub(super) fn get(
        &self,
        value: &T,
        compare: &mut impl FnMut(&T, &T) -> Result<Ordering>,
    ) -> Result<Option<&T>> {
        let mut work = Unmetered;
        self.get_with_work(value, &mut work, &mut |left, right, _work| {
            compare(left, right)
        })
    }

    pub(super) fn get_with_work<W: Work + ?Sized>(
        &self,
        value: &T,
        work: &mut W,
        compare: &mut impl FnMut(&T, &T, &mut W) -> Result<Ordering>,
    ) -> Result<Option<&T>> {
        let mut cursor = self.root;
        while let Some(index) = cursor {
            work.charge(1)?;
            match compare(value, &self.nodes[index].value, work)? {
                Ordering::Equal => return Ok(Some(&self.nodes[index].value)),
                Ordering::Less => cursor = self.nodes[index].left,
                Ordering::Greater => cursor = self.nodes[index].right,
            }
        }
        Ok(None)
    }

    pub(super) fn in_order_indices_with_work<W: Work + ?Sized>(
        &self,
        work: &mut W,
    ) -> Result<Vec<usize>> {
        work.charge(u64::try_from(self.nodes.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut ordered = Vec::with_capacity(self.nodes.len());
        let mut stack = Vec::with_capacity(self.height(self.root));
        let mut current = self.root;
        loop {
            while let Some(node) = current {
                work.charge(1)?;
                stack.push(node);
                current = self.nodes[node].left;
            }
            work.charge(1)?;
            let Some(node) = stack.pop() else {
                break;
            };
            work.charge(1)?;
            ordered.push(node);
            current = self.nodes[node].right;
        }
        Ok(ordered)
    }

    pub(super) fn remove(
        &mut self,
        value: &T,
        compare: &mut impl FnMut(&T, &T) -> Result<Ordering>,
    ) -> Result<bool>
    where
        T: Clone,
    {
        fn remove_min<T: Clone>(tree: &mut FallibleAvl<T>, root: usize) -> (Option<usize>, usize) {
            let Some(left) = tree.nodes[root].left else {
                return (tree.nodes[root].right, root);
            };
            let (left, minimum) = remove_min(tree, left);
            tree.nodes[root].left = left;
            (Some(tree.balance(root)), minimum)
        }

        fn descend<T: Clone>(
            tree: &mut FallibleAvl<T>,
            root: Option<usize>,
            value: &T,
            compare: &mut impl FnMut(&T, &T) -> Result<Ordering>,
        ) -> Result<(Option<usize>, bool)> {
            let Some(root) = root else {
                return Ok((None, false));
            };
            match compare(value, &tree.nodes[root].value)? {
                Ordering::Less => {
                    let (left, removed) = descend(tree, tree.nodes[root].left, value, compare)?;
                    if removed {
                        tree.nodes[root].left = left;
                        Ok((Some(tree.balance(root)), true))
                    } else {
                        Ok((Some(root), false))
                    }
                },
                Ordering::Greater => {
                    let (right, removed) = descend(tree, tree.nodes[root].right, value, compare)?;
                    if removed {
                        tree.nodes[root].right = right;
                        Ok((Some(tree.balance(root)), true))
                    } else {
                        Ok((Some(root), false))
                    }
                },
                Ordering::Equal => {
                    let (Some(left), Some(right)) = (tree.nodes[root].left, tree.nodes[root].right)
                    else {
                        return Ok((tree.nodes[root].left.or(tree.nodes[root].right), true));
                    };
                    let (right, successor) = remove_min(tree, right);
                    tree.nodes[root].value = tree.nodes[successor].value.clone();
                    tree.nodes[root].left = Some(left);
                    tree.nodes[root].right = right;
                    Ok((Some(tree.balance(root)), true))
                },
            }
        }

        let (root, removed) = descend(self, self.root, value, compare)?;
        if removed {
            self.root = root;
        }
        Ok(removed)
    }

    pub(super) fn predecessor<Q>(
        &self,
        query: &Q,
        mut compare: impl FnMut(&T, &Q) -> Result<Ordering>,
    ) -> Result<Option<&T>> {
        let mut cursor = self.root;
        let mut predecessor = None;
        while let Some(index) = cursor {
            match compare(&self.nodes[index].value, query)? {
                Ordering::Less => {
                    predecessor = Some(&self.nodes[index].value);
                    cursor = self.nodes[index].right;
                },
                Ordering::Equal | Ordering::Greater => cursor = self.nodes[index].left,
            }
        }
        Ok(predecessor)
    }

    pub(super) fn for_each(&self, visit: &mut impl FnMut(&T) -> Result<()>) -> Result<()> {
        fn walk<T>(
            tree: &FallibleAvl<T>,
            root: Option<usize>,
            visit: &mut impl FnMut(&T) -> Result<()>,
        ) -> Result<()> {
            let Some(root) = root else {
                return Ok(());
            };
            walk(tree, tree.nodes[root].left, visit)?;
            visit(&tree.nodes[root].value)?;
            walk(tree, tree.nodes[root].right, visit)
        }

        walk(self, self.root, visit)
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ExactSourcePoint {
    pub point: Point,
    pub row: SourceRowId,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ExactLineBoundary {
    pub row: SourceRowId,
    pub line: SourceLineId,
    pub edge: SourceEdgeId,
    pub at_end: bool,
}

#[derive(Clone, Debug)]
pub(super) struct ExactSourceLine {
    pub id: SourceLineId,
    pub row: SourceRowId,
    pub source_edges: Range<usize>,
}

#[derive(Clone, Debug)]
pub(super) struct ExactSourceRing {
    pub id: SourceRingId,
    pub polygon: SourcePolygonId,
    pub hole: bool,
    pub kind: RingKind,
    pub summary: RingSummary,
    pub source_edges: Range<usize>,
}

impl ExactSourceRing {
    /// Full-longitude traversals and explicit axis bands can enclose spherical
    /// area even when their longitude/latitude projection has zero shoelace.
    pub(super) const fn has_areal_winding(&self) -> bool {
        !matches!(self.summary.domain, RingDomain::SeamOnly)
            && (matches!(self.kind, RingKind::Winding)
                || self.summary.chart_turn != 0
                || self.summary.exact_axis_band)
    }
}

#[derive(Clone, Debug)]
pub(super) struct ExactSourcePolygon {
    pub row: SourceRowId,
    pub rings: Range<usize>,
}

/// All source identities needed by predicates and Q are captured before any
/// chart cut, noding, or binary64 emission witness is constructed.
#[derive(Debug)]
pub(super) struct ExactGeographicChartInventory {
    pub frame: GeographicFrame,
    pub row_count: usize,
    pub edges: Vec<ExactSourceEdge>,
    pub pieces: Vec<ExactChartPiece>,
    pub lines: Vec<ExactSourceLine>,
    pub rings: Vec<ExactSourceRing>,
    pub polygons: Vec<ExactSourcePolygon>,
    pub points: Vec<ExactSourcePoint>,
    pub line_boundaries: Vec<ExactLineBoundary>,
}

impl crate::heap_size::HeapSize for ExactGeographicChartInventory {
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "HeapSize estimates retained bytes from already-allocated inventory buffers"
    )]
    fn heap_bytes(&self) -> usize {
        self.edges.capacity() * std::mem::size_of::<ExactSourceEdge>()
            + self.pieces.capacity() * std::mem::size_of::<ExactChartPiece>()
            + self
                .pieces
                .iter()
                .map(|piece| {
                    piece.start.heap_bytes()
                        + piece.end.heap_bytes()
                        + piece.source_range.heap_bytes()
                })
                .sum::<usize>()
            + self.lines.capacity() * std::mem::size_of::<ExactSourceLine>()
            + self.rings.capacity() * std::mem::size_of::<ExactSourceRing>()
            + self.polygons.capacity() * std::mem::size_of::<ExactSourcePolygon>()
            + self.points.capacity() * std::mem::size_of::<ExactSourcePoint>()
            + self.line_boundaries.capacity() * std::mem::size_of::<ExactLineBoundary>()
    }
}

impl ExactSourceRange {
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "HeapSize sums the two rational parameters owned by this range"
    )]
    pub(super) fn heap_bytes(&self) -> usize {
        rational_heap_bytes(&self.start) + rational_heap_bytes(&self.end)
    }
}

impl crate::heap_size::HeapSize for NodedExactChart {
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "HeapSize estimates retained bytes from already-allocated exact chart buffers"
    )]
    fn heap_bytes(&self) -> usize {
        self.ports.len() * std::mem::size_of::<ChartPort>()
            + self
                .ports
                .iter()
                .map(|port| port.point.heap_bytes())
                .sum::<usize>()
            + self.nodes.len() * std::mem::size_of::<QuotientNode>()
            + self
                .nodes
                .iter()
                .map(|node| quotient_key_heap_bytes(&node.key))
                .sum::<usize>()
            + self.node_ports.len() * std::mem::size_of::<ChartPortId>()
            + self.atoms.len() * std::mem::size_of::<ExactChartAtom>()
            + self
                .atoms
                .iter()
                .map(|atom| {
                    atom.owners.len() * std::mem::size_of::<ExactSpanOwner>()
                        + atom
                            .owners
                            .iter()
                            .map(|owner| owner.source_range.heap_bytes())
                            .sum::<usize>()
                })
                .sum::<usize>()
            + self.source_edge_ports.len() * std::mem::size_of::<[ChartPortId; 2]>()
            + self.source_point_ports.len() * std::mem::size_of::<ChartPortId>()
    }
}

impl ExactGeographicChartInventory {
    #[cfg(test)]
    pub(super) fn collect(inputs: &GeographicInputs<'_>) -> Result<Self> {
        Self::collect_views(inputs.frame(), inputs.views().iter().copied())
    }

    pub(super) fn collect_views<'a>(
        frame: GeographicFrame,
        views: impl ExactSizeIterator<Item = ScalarGeographicView<'a>>,
    ) -> Result<Self> {
        let mut work = Unmetered;
        Self::collect_views_with_work(frame, views, &mut work)
    }

    pub(super) fn collect_views_with_work<'a>(
        frame: GeographicFrame,
        views: impl ExactSizeIterator<Item = ScalarGeographicView<'a>>,
        work: &mut impl Work,
    ) -> Result<Self> {
        Self::collect_views_inner(frame, views, false, work)
    }

    pub(super) fn collect_views_as_set<'a>(
        frame: GeographicFrame,
        views: impl ExactSizeIterator<Item = ScalarGeographicView<'a>>,
    ) -> Result<Self> {
        let mut work = Unmetered;
        Self::collect_views_as_set_with_work(frame, views, &mut work)
    }

    pub(super) fn collect_views_as_set_with_work<'a>(
        frame: GeographicFrame,
        views: impl ExactSizeIterator<Item = ScalarGeographicView<'a>>,
        work: &mut impl Work,
    ) -> Result<Self> {
        Self::collect_views_inner(frame, views, true, work)
    }

    fn collect_views_inner<'a>(
        frame: GeographicFrame,
        views: impl ExactSizeIterator<Item = ScalarGeographicView<'a>>,
        as_set: bool,
        work: &mut impl Work,
    ) -> Result<Self> {
        work.charge(u64::try_from(views.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut inventory = Self::new(frame, views.len());
        for (row, view) in views.enumerate() {
            work.charge(1)?;
            if view.frame() != frame {
                return Err(GeometryErrorKind::Invalid(
                    "exact geographic source views do not share one frame".into(),
                )
                .into());
            }
            let row = SourceRowId(u32::try_from(row).map_err(|_| ResourceError::SizeOverflow)?);
            if as_set {
                inventory.append_shape_as_set(view.shape(), row, work)?;
            } else {
                inventory.append_shape(view.shape(), row, work)?;
            }
        }
        Ok(inventory)
    }

    const fn new(frame: GeographicFrame, row_count: usize) -> Self {
        Self {
            frame,
            row_count,
            edges: Vec::new(),
            pieces: Vec::new(),
            lines: Vec::new(),
            rings: Vec::new(),
            polygons: Vec::new(),
            points: Vec::new(),
            line_boundaries: Vec::new(),
        }
    }

    fn append_shape(
        &mut self,
        shape: &Shape,
        row: SourceRowId,
        work: &mut impl Work,
    ) -> Result<()> {
        shape.try_for_each_component(&mut |source| {
            work.charge(1)?;
            self.append_component(source, row, work)
        })
    }

    fn append_shape_as_set(
        &mut self,
        shape: &Shape,
        row: SourceRowId,
        work: &mut impl Work,
    ) -> Result<()> {
        shape.try_for_each_first_component(&mut |source| {
            work.charge(1)?;
            self.append_component(source, row, work)
        })
    }

    fn append_component(
        &mut self,
        source: Component<'_>,
        row: SourceRowId,
        work: &mut impl Work,
    ) -> Result<()> {
        match source {
            Component::Point(point) => {
                work.charge(1)?;
                self.points.push(ExactSourcePoint { point, row });
                Ok(())
            },
            Component::LineString(line) => self.push_line(line.as_coords(), row, work),
            Component::Polygon(polygon) => self.push_polygon_with_work(polygon, row, work),
        }
    }

    fn push_line(
        &mut self,
        coordinates: &crate::geometry::CoordSeq,
        row: SourceRowId,
        work: &mut impl Work,
    ) -> Result<()> {
        let segment_count = coordinates.coord_count().saturating_sub(1);
        work.charge(u64::try_from(segment_count).map_err(|_| ResourceError::SizeOverflow)?)?;
        self.reserve_source_edges(segment_count, work)?;
        let line =
            SourceLineId(u32::try_from(self.lines.len()).map_err(|_| ResourceError::SizeOverflow)?);
        let edge_start = self.edges.len();
        let piece_start = self.pieces.len();
        let mut first_edge = None;
        let mut last_edge = None;
        for pair in coordinates.segment_pairs() {
            work.charge(1)?;
            let [start, end] = pair;
            let before = self.pieces.len();
            let (source, _) = self.append_source_edge_with_work(
                start,
                end,
                row,
                SourceEdgeKind::Line { line },
                0,
                work,
            )?;
            if self.pieces.len() != before {
                first_edge.get_or_insert(source);
                last_edge = Some(source);
            }
        }
        if self.pieces.len() == piece_start {
            work.charge(
                u64::try_from(coordinates.coord_count())
                    .map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            self.points.reserve(coordinates.coord_count());
            for point in coordinates.iter_coords() {
                self.points.push(ExactSourcePoint { point, row });
            }
        } else if let (Some(first), Some(last)) = (first_edge, last_edge) {
            work.charge(2)?;
            self.line_boundaries.reserve(2);
            self.line_boundaries.push(ExactLineBoundary {
                row,
                line,
                edge: first,
                at_end: false,
            });
            self.line_boundaries.push(ExactLineBoundary {
                row,
                line,
                edge: last,
                at_end: true,
            });
        }
        work.charge(1)?;
        self.lines.push(ExactSourceLine {
            id: line,
            row,
            source_edges: edge_start..self.edges.len(),
        });
        Ok(())
    }

    fn push_polygon(&mut self, polygon: &Polygon, row: SourceRowId) -> Result<()> {
        let mut work = Unmetered;
        self.push_polygon_with_work(polygon, row, &mut work)
    }

    fn push_polygon_with_work(
        &mut self,
        polygon: &Polygon,
        row: SourceRowId,
        work: &mut impl Work,
    ) -> Result<()> {
        let polygon_id = SourcePolygonId(
            u32::try_from(self.polygons.len()).map_err(|_| ResourceError::SizeOverflow)?,
        );
        work.charge(
            u64::try_from(polygon.rings().len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let edge_count = polygon.rings().try_fold(0_usize, |count, ring| {
            count
                .checked_add(ring.coord_count().saturating_sub(1))
                .ok_or(ResourceError::SizeOverflow)
        })?;
        work.charge(u64::try_from(edge_count).map_err(|_| ResourceError::SizeOverflow)?)?;
        self.reserve_source_edges(edge_count, work)?;
        work.charge(1)?;
        self.polygons.reserve(1);
        self.rings.reserve(polygon.holes.len().saturating_add(1));
        let first_ring = self.rings.len();
        self.polygons.push(ExactSourcePolygon {
            row,
            rings: first_ring..first_ring,
        });
        for (ring_index, ring) in polygon.rings().enumerate() {
            work.charge(1)?;
            work.charge(
                u64::try_from(ring.coord_count()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let ring_id = SourceRingId(
                u32::try_from(self.rings.len()).map_err(|_| ResourceError::SizeOverflow)?,
            );
            let stored_kind =
                crate::geometry::ring_kind_columns(ring.coords().xs(), ring.coords().ys());
            let summary = RingSummary::try_new(
                self.frame,
                &crate::geographic::GeographicRing::from_ring(ring),
            )
            .expect("the source ring was admitted in this frame");
            let kind = if summary.domain == RingDomain::SeamOnly {
                RingKind::Collapsed
            } else {
                stored_kind
            };
            let first_edge = self.edges.len();
            let mut lift = 0_i64;
            for [start, end] in ring.segment_pairs() {
                work.charge(1)?;
                let (_, transit) = self.append_source_edge_with_work(
                    start,
                    end,
                    row,
                    SourceEdgeKind::Ring {
                        polygon: polygon_id,
                        ring: ring_id,
                        hole: ring_index != 0,
                        kind,
                    },
                    lift,
                    work,
                )?;
                lift = lift
                    .checked_add(i64::from(transit))
                    .ok_or(ResourceError::SizeOverflow)?;
            }
            debug_assert_eq!(lift, summary.chart_turn);
            self.rings.push(ExactSourceRing {
                id: ring_id,
                polygon: polygon_id,
                hole: ring_index != 0,
                kind,
                summary,
                source_edges: first_edge..self.edges.len(),
            });
        }
        let target = self
            .polygons
            .get_mut(polygon_id.0 as usize)
            .expect("the polygon record was reserved and appended");
        target.rings = first_ring..self.rings.len();
        Ok(())
    }

    fn reserve_source_edges(&mut self, count: usize, work: &mut impl Work) -> Result<()> {
        let edge_total = self
            .edges
            .len()
            .checked_add(count)
            .ok_or(ResourceError::SizeOverflow)?;
        u32::try_from(edge_total).map_err(|_| ResourceError::SizeOverflow)?;
        let piece_capacity = count
            .checked_mul(2)
            .and_then(|additional| self.pieces.len().checked_add(additional))
            .ok_or(ResourceError::SizeOverflow)?;
        let additional_pieces = piece_capacity
            .checked_sub(self.pieces.len())
            .expect("piece capacity includes the existing chart pieces");
        let reservation_work = count
            .checked_add(additional_pieces)
            .ok_or(ResourceError::SizeOverflow)?;
        work.charge(u64::try_from(reservation_work).map_err(|_| ResourceError::SizeOverflow)?)?;
        self.edges.reserve(count);
        self.pieces.reserve(additional_pieces);
        Ok(())
    }

    fn append_source_edge(
        &mut self,
        start: Point,
        end: Point,
        row: SourceRowId,
        kind: SourceEdgeKind,
        ring_lift: i64,
    ) -> Result<(SourceEdgeId, i8)> {
        let mut work = Unmetered;
        self.append_source_edge_with_work(start, end, row, kind, ring_lift, &mut work)
    }

    fn append_source_edge_with_work(
        &mut self,
        start: Point,
        end: Point,
        row: SourceRowId,
        kind: SourceEdgeKind,
        ring_lift: i64,
        work: &mut impl Work,
    ) -> Result<(SourceEdgeId, i8)> {
        work.charge(1)?;
        let id =
            SourceEdgeId(u32::try_from(self.edges.len()).map_err(|_| ResourceError::SizeOverflow)?);
        let connection = self
            .frame
            .longitude()
            .chart_connection(start.x, end.x)
            .expect("source longitude was admitted");
        self.edges.push(ExactSourceEdge {
            id,
            source: [start, end],
            longitude: connection.retained(),
            row,
            kind,
        });
        self.push_chart_pieces_with_work(id, ring_lift, work)?;
        Ok((id, connection.seam_transit()))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "SourceEdgeId is issued by append_source_edge for this inventory's edge vector"
    )]
    fn push_chart_pieces_with_work(
        &mut self,
        source: SourceEdgeId,
        ring_lift: i64,
        work: &mut impl Work,
    ) -> Result<()> {
        work.charge(1)?;
        let edge = self.edges[source.0 as usize];
        let [start, end] = edge.source;
        let local_edge = local_longitude_edge(self.frame, start.x, edge.retained_longitude());
        let start_x = local_edge.start_x;
        let delta_x = local_edge.delta_x(self.frame.longitude().period())?;
        let start_alias = local_edge.start_turn_adjust;
        let piece_lift = ring_lift
            .checked_sub(start_alias)
            .ok_or(ResourceError::SizeOverflow)?;
        let start_y = ExactDyadic::from_f64(Latitude::from_admitted(self.frame, start.y).get());
        let end_y = ExactDyadic::from_f64(Latitude::from_admitted(self.frame, end.y).get());
        let start_y_rational = ExactRational::from_dyadic(start_y.clone());
        let delta_y = ExactRational::from_dyadic(end_y.clone()).try_subtract(&start_y_rational)?;
        let start_x = ExactDyadic::from_f64(start_x);
        let end_x = start_x.clone().try_add(delta_x.clone())?;
        let half = ExactDyadic::from_f64(self.frame.longitude().half_period());
        let negative_half = half.clone().neg();
        let zero = ExactRational::zero();
        let one = ExactRational::one();

        if end_x.cmp(&half).is_gt() {
            let parameter =
                ExactRational::new(ExactDyadic::try_subtract_refs(&half, &start_x)?, delta_x)?;
            let seam_y = interpolate_rational(&start_y_rational, &delta_y, &parameter)?;
            let seam =
                ExactChartPoint::from_rationals(ExactRational::from_dyadic(half), seam_y.clone());
            self.push_chart_piece_with_work(
                source,
                ExactChartPoint::from_dyadics(start_x, start_y),
                seam,
                zero,
                parameter.clone(),
                piece_lift,
                work,
            )?;
            let end_x =
                end_x.try_subtract(ExactDyadic::from_f64(self.frame.longitude().period()))?;
            self.push_chart_piece_with_work(
                source,
                ExactChartPoint::from_rationals(ExactRational::from_dyadic(negative_half), seam_y),
                ExactChartPoint::from_dyadics(end_x, end_y),
                parameter,
                one,
                piece_lift
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?,
                work,
            )?;
        } else if end_x.cmp(&negative_half).is_lt() {
            let parameter = ExactRational::new(
                ExactDyadic::try_subtract_refs(&negative_half, &start_x)?,
                delta_x,
            )?;
            let seam_y = interpolate_rational(&start_y_rational, &delta_y, &parameter)?;
            let seam = ExactChartPoint::from_rationals(
                ExactRational::from_dyadic(negative_half),
                seam_y.clone(),
            );
            self.push_chart_piece_with_work(
                source,
                ExactChartPoint::from_dyadics(start_x, start_y),
                seam,
                zero,
                parameter.clone(),
                piece_lift,
                work,
            )?;
            let end_x = end_x.try_add(ExactDyadic::from_f64(self.frame.longitude().period()))?;
            self.push_chart_piece_with_work(
                source,
                ExactChartPoint::from_rationals(ExactRational::from_dyadic(half), seam_y),
                ExactChartPoint::from_dyadics(end_x, end_y),
                parameter,
                one,
                piece_lift
                    .checked_sub(1)
                    .ok_or(ResourceError::SizeOverflow)?,
                work,
            )?;
        } else {
            self.push_chart_piece_with_work(
                source,
                ExactChartPoint::from_dyadics(start_x, start_y),
                ExactChartPoint::from_dyadics(end_x, end_y),
                zero,
                one,
                piece_lift,
                work,
            )?;
        }
        Ok(())
    }

    fn push_chart_piece_with_work(
        &mut self,
        source_edge: SourceEdgeId,
        start: ExactChartPoint,
        end: ExactChartPoint,
        source_start: ExactRational,
        source_end: ExactRational,
        lift: i64,
        work: &mut impl Work,
    ) -> Result<()> {
        work.charge(1)?;
        if start.same_position(&end)? {
            return Ok(());
        }
        let id = ChartPieceId(
            u32::try_from(self.pieces.len()).map_err(|_| ResourceError::SizeOverflow)?,
        );
        self.pieces.push(ExactChartPiece {
            id,
            source_edge,
            start,
            end,
            source_range: ExactSourceRange {
                start: source_start,
                end: source_end,
            },
            lift,
        });
        Ok(())
    }

    #[expect(
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        reason = "piece, port, quotient-node, and atom IDs index dense columns emitted by this noder"
    )]
    pub(super) fn node(&self, budget: &mut ExpansionBudget) -> Result<NodedExactChart> {
        let mut work = Unmetered;
        self.node_with_work(budget, &mut work)
    }

    #[expect(
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        reason = "piece, port, quotient-node, and atom IDs index dense columns emitted by this noder"
    )]
    pub(super) fn node_with_work<W: Work + ?Sized>(
        &self,
        budget: &mut ExpansionBudget,
        work: &mut W,
    ) -> Result<NodedExactChart> {
        let cut_capacity = self
            .pieces
            .len()
            .checked_mul(2)
            .ok_or(ResourceError::SizeOverflow)?;
        work.charge(u64::try_from(cut_capacity).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut cuts = FallibleAvl::with_capacity(cut_capacity);
        let mut compare_cuts = |left: &CutEvent, right: &CutEvent, work: &mut W| {
            work.charge(1)?;
            compare_cut_events(left, right)
        };
        for piece in &self.pieces {
            work.charge(1)?;
            cuts.insert_with_work(
                CutEvent {
                    piece: piece.id,
                    parameter: ExactRational::zero(),
                    point: piece.start.clone(),
                },
                work,
                &mut compare_cuts,
            )?;
            cuts.insert_with_work(
                CutEvent {
                    piece: piece.id,
                    parameter: ExactRational::one(),
                    point: piece.end.clone(),
                },
                work,
                &mut compare_cuts,
            )?;
        }
        let source_keys = self.source_quotient_keys_with_work(work)?;
        let node_capacity = source_keys
            .len()
            .checked_add(self.pieces.len())
            .ok_or(ResourceError::SizeOverflow)?;
        work.charge(u64::try_from(node_capacity).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut constructed_nodes = FallibleAvl::with_capacity(node_capacity);
        let mut compare_keys = |left: &ExactQuotientKey, right: &ExactQuotientKey, work: &mut W| {
            work.charge(1)?;
            compare_quotient_keys(left, right)
        };
        for key in source_keys {
            constructed_nodes.insert_with_work(key, work, &mut compare_keys)?;
        }
        for piece in &self.pieces {
            work.charge(1)?;
            if !piece.source_range.start.is_zero() {
                let key = quotient_key(self.frame, &piece.start)?;
                if constructed_nodes.insert_with_work(key, work, &mut compare_keys)? {
                    budget.add(1)?;
                }
            }
            if !piece.source_range.end.is_one() {
                let key = quotient_key(self.frame, &piece.end)?;
                if constructed_nodes.insert_with_work(key, work, &mut compare_keys)? {
                    budget.add(1)?;
                }
            }
        }
        let mut proxy_piece_ids = Vec::with_capacity(self.pieces.len());
        let mut proxies = Vec::with_capacity(self.pieces.len());
        let mut proxyable = Vec::with_capacity(self.pieces.len());
        for piece in &self.pieces {
            work.charge(1)?;
            if let Some(proxy) = piece.outward_proxy()? {
                proxy_piece_ids.push(piece.id.0 as usize);
                proxies.push(proxy);
                proxyable.push(true);
            } else {
                proxyable.push(false);
            }
        }
        u32::try_from(proxies.len()).map_err(|_| ResourceError::SizeOverflow)?;

        let mut failure = None;
        let candidates = for_each_candidate_pair(
            &proxies,
            |index| index as u32,
            |left, right| {
                if let Err(error) = work.charge(1) {
                    failure = Some(error);
                    return std::ops::ControlFlow::Break(());
                }
                let left_piece = proxy_piece_ids[left];
                let right_piece = proxy_piece_ids[right];
                match self.refine_piece_pair(
                    left_piece,
                    right_piece,
                    &mut cuts,
                    &mut constructed_nodes,
                    &mut compare_cuts,
                    &mut compare_keys,
                    budget,
                    work,
                ) {
                    Ok(()) => std::ops::ControlFlow::Continue(()),
                    Err(error) => {
                        failure = Some(error);
                        std::ops::ControlFlow::Break(())
                    },
                }
            },
        )?;
        if candidates.is_break() {
            return Err(failure.expect("a failed exact pair stores its first error"));
        }

        // Proxy-declined pieces have no conservative broadphase interval.
        // Enumerate every pair involving one, without imposing a candidate cap.
        for left in 0..self.pieces.len() {
            work.charge(1)?;
            if proxyable[left] {
                continue;
            }
            for (right, &is_proxyable) in proxyable.iter().enumerate().take(left) {
                work.charge(1)?;
                if is_proxyable {
                    self.refine_piece_pair(
                        right,
                        left,
                        &mut cuts,
                        &mut constructed_nodes,
                        &mut compare_cuts,
                        &mut compare_keys,
                        budget,
                        work,
                    )?;
                }
            }
            for right in left + 1..self.pieces.len() {
                work.charge(1)?;
                self.refine_piece_pair(
                    left,
                    right,
                    &mut cuts,
                    &mut constructed_nodes,
                    &mut compare_cuts,
                    &mut compare_keys,
                    budget,
                    work,
                )?;
            }
        }
        self.refine_seam_aliases(
            &mut cuts,
            &mut constructed_nodes,
            &mut compare_cuts,
            &mut compare_keys,
            budget,
            work,
        )?;
        self.refine_source_points(
            &mut cuts,
            &mut compare_cuts,
            &proxy_piece_ids,
            &proxies,
            &proxyable,
            work,
        )?;

        let cut_order = cuts.in_order_indices_with_work(work)?;
        work.charge(u64::try_from(self.pieces.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut occurrences = Vec::with_capacity(self.pieces.len());
        let mut previous_piece = None;
        let mut previous_cut: Option<usize> = None;
        for cut_index in cut_order {
            work.charge(1)?;
            let cut = &cuts.nodes[cut_index].value;
            if previous_piece != Some(cut.piece) {
                previous_piece = Some(cut.piece);
                previous_cut = None;
            }
            if let Some(start_index) = previous_cut {
                let start = &cuts.nodes[start_index].value;
                work.charge(1)?;
                if !start.point.same_position(&cut.point)? {
                    let piece = &self.pieces[cut.piece.0 as usize];
                    work.charge(1)?;
                    occurrences.push(AtomOccurrence {
                        start: start.point.clone(),
                        end: cut.point.clone(),
                        source_edge: piece.source_edge,
                        source_range: piece.source_range.compose_with_work(
                            &start.parameter,
                            &cut.parameter,
                            work,
                        )?,
                        lift: piece.lift,
                    });
                }
            }
            previous_cut = Some(cut_index);
        }

        let port_capacity = occurrences
            .len()
            .checked_mul(2)
            .and_then(|count| self.edges.len().checked_mul(2)?.checked_add(count))
            .and_then(|count| count.checked_add(self.points.len()))
            .ok_or(ResourceError::SizeOverflow)?;
        work.charge(u64::try_from(port_capacity).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut port_points = Vec::with_capacity(port_capacity);
        for occurrence in &occurrences {
            work.charge(2)?;
            port_points.extend([occurrence.start.clone(), occurrence.end.clone()]);
        }
        for edge in &self.edges {
            for point in edge.source {
                work.charge(1)?;
                port_points.push(source_chart_point(self.frame, point));
            }
        }
        for source in &self.points {
            work.charge(1)?;
            port_points.push(source_chart_point(self.frame, source.point));
        }
        try_sort_by_with_work(&mut port_points, work, |left, right, work| {
            work.charge(1)?;
            left.compare_lex(right)
        })?;
        work.charge(u64::try_from(port_points.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut unique_ports: Vec<ExactChartPoint> = Vec::with_capacity(port_points.len());
        for point in port_points {
            work.charge(1)?;
            let duplicate = if let Some(previous) = unique_ports.last() {
                work.charge(1)?;
                previous.compare_lex(&point)?.is_eq()
            } else {
                false
            };
            if duplicate {
                continue;
            }
            unique_ports.push(point);
        }
        work.charge(u64::try_from(unique_ports.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut ports = Vec::with_capacity(unique_ports.len());
        for (index, point) in unique_ports.into_iter().enumerate() {
            work.charge(1)?;
            ports.push(ChartPort {
                id: ChartPortId(u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?),
                point,
                node: QuotientNodeId(0),
            });
        }

        work.charge(u64::try_from(self.edges.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut source_edge_ports = Vec::with_capacity(self.edges.len());
        for edge in &self.edges {
            work.charge(1)?;
            source_edge_ports.push([
                find_chart_port_with_work(
                    &ports,
                    &source_chart_point(self.frame, edge.source[0]),
                    work,
                )?,
                find_chart_port_with_work(
                    &ports,
                    &source_chart_point(self.frame, edge.source[1]),
                    work,
                )?,
            ]);
        }
        work.charge(u64::try_from(self.points.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut source_point_ports = Vec::with_capacity(self.points.len());
        for point in &self.points {
            work.charge(1)?;
            source_point_ports.push(find_chart_port_with_work(
                &ports,
                &source_chart_point(self.frame, point.point),
                work,
            )?);
        }

        work.charge(u64::try_from(ports.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut quotient_keys = Vec::with_capacity(ports.len());
        for port in &ports {
            work.charge(1)?;
            quotient_keys.push((quotient_key(self.frame, &port.point)?, port.id));
        }
        try_sort_by_with_work(&mut quotient_keys, work, |left, right, work| {
            work.charge(1)?;
            compare_quotient_keys(&left.0, &right.0)
                .map(|order| order.then_with(|| left.1.cmp(&right.1)))
        })?;
        work.charge(u64::try_from(quotient_keys.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut nodes = Vec::with_capacity(quotient_keys.len());
        work.charge(u64::try_from(ports.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut node_ports = Vec::with_capacity(ports.len());
        let mut port_cursor = 0;
        while port_cursor < quotient_keys.len() {
            work.charge(1)?;
            let node_id = QuotientNodeId(
                u32::try_from(nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
            );
            let start = node_ports.len();
            let key = quotient_keys[port_cursor].0.clone();
            while port_cursor < quotient_keys.len() {
                work.charge(1)?;
                if !compare_quotient_keys(&key, &quotient_keys[port_cursor].0)?.is_eq() {
                    break;
                }
                let port = quotient_keys[port_cursor].1;
                ports[port.0 as usize].node = node_id;
                node_ports.push(port);
                port_cursor += 1;
            }
            nodes.push(QuotientNode {
                id: node_id,
                ports: start..node_ports.len(),
                key,
            });
        }

        work.charge(u64::try_from(occurrences.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut atom_occurrences = Vec::with_capacity(occurrences.len());
        for occurrence in occurrences {
            work.charge(1)?;
            let start = find_chart_port_with_work(&ports, &occurrence.start, work)?;
            let end = find_chart_port_with_work(&ports, &occurrence.end, work)?;
            let (ports, reversed) = if start <= end {
                ([start, end], false)
            } else {
                ([end, start], true)
            };
            atom_occurrences.push((
                ports,
                occurrence.source_edge,
                occurrence.source_range,
                occurrence.lift,
                reversed,
            ));
        }
        try_sort_by_with_work(&mut atom_occurrences, work, |left, right, work| {
            work.charge(1)?;
            let endpoints = left.0.cmp(&right.0);
            if !endpoints.is_eq() {
                return Ok(endpoints);
            }
            let source = left.1.cmp(&right.1);
            if !source.is_eq() {
                return Ok(source);
            }
            let start = left.2.start.try_cmp(&right.2.start)?;
            if !start.is_eq() {
                return Ok(start);
            }
            let end = left.2.end.try_cmp(&right.2.end)?;
            Ok(if end.is_eq() {
                left.3.cmp(&right.3).then_with(|| left.4.cmp(&right.4))
            } else {
                end
            })
        })?;
        work.charge(
            u64::try_from(atom_occurrences.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut atoms = Vec::with_capacity(atom_occurrences.len());
        let mut cursor = 0;
        while cursor < atom_occurrences.len() {
            work.charge(1)?;
            let endpoints = atom_occurrences[cursor].0;
            let mut owners = Vec::new();
            while cursor < atom_occurrences.len() && atom_occurrences[cursor].0 == endpoints {
                work.charge(1)?;
                let (_, source_edge, source_range, lift, reversed) = &atom_occurrences[cursor];
                owners.push(ExactSpanOwner {
                    source_edge: *source_edge,
                    source_range: source_range.clone(),
                    reversed: *reversed,
                    lift: *lift,
                });
                cursor += 1;
            }
            atoms.push(ExactChartAtom {
                ports: endpoints,
                owners: owners.into_boxed_slice(),
            });
        }

        let distinct_sources = self.distinct_source_piece_count_with_work(work)?;
        let net_atom_growth = atoms.len().saturating_sub(distinct_sources);
        budget.add(net_atom_growth)?;

        Ok(NodedExactChart {
            ports: ports.into_boxed_slice(),
            nodes: nodes.into_boxed_slice(),
            node_ports: node_ports.into_boxed_slice(),
            atoms: atoms.into_boxed_slice(),
            source_edge_ports: source_edge_ports.into_boxed_slice(),
            source_point_ports: source_point_ports.into_boxed_slice(),
        })
    }

    fn source_quotient_keys_with_work<W: Work + ?Sized>(
        &self,
        work: &mut W,
    ) -> Result<Vec<ExactQuotientKey>> {
        let source_endpoints = self
            .edges
            .len()
            .checked_mul(2)
            .and_then(|count| count.checked_add(self.points.len()))
            .ok_or(ResourceError::SizeOverflow)?;
        work.charge(u64::try_from(source_endpoints).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut keys = Vec::with_capacity(source_endpoints);
        for edge in &self.edges {
            for point in edge.source {
                work.charge(1)?;
                keys.push(quotient_key(
                    self.frame,
                    &source_chart_point(self.frame, point),
                )?);
            }
        }
        for point in &self.points {
            work.charge(1)?;
            keys.push(quotient_key(
                self.frame,
                &source_chart_point(self.frame, point.point),
            )?);
        }
        try_sort_by_with_work(&mut keys, work, |left, right, work| {
            work.charge(1)?;
            compare_quotient_keys(left, right)
        })?;
        work.charge(u64::try_from(keys.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut unique = Vec::with_capacity(keys.len());
        for key in keys {
            work.charge(1)?;
            let duplicate = if let Some(previous) = unique.last() {
                work.charge(1)?;
                compare_quotient_keys(previous, &key)?.is_eq()
            } else {
                false
            };
            if duplicate {
                continue;
            }
            unique.push(key);
        }
        Ok(unique)
    }

    fn distinct_source_piece_count_with_work<W: Work + ?Sized>(
        &self,
        work: &mut W,
    ) -> Result<usize> {
        work.charge(u64::try_from(self.pieces.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut endpoints = Vec::with_capacity(self.pieces.len());
        for piece in &self.pieces {
            work.charge(1)?;
            let (start, end) = if piece.start.compare_lex(&piece.end)?.is_le() {
                (piece.start.clone(), piece.end.clone())
            } else {
                (piece.end.clone(), piece.start.clone())
            };
            endpoints.push((start, end));
        }
        try_sort_by_with_work(&mut endpoints, work, |left, right, work| {
            work.charge(1)?;
            let start = left.0.compare_lex(&right.0)?;
            Ok(if start.is_eq() {
                left.1.compare_lex(&right.1)?
            } else {
                start
            })
        })?;
        let mut unique = 0_usize;
        let mut previous: Option<(ExactChartPoint, ExactChartPoint)> = None;
        for piece in endpoints {
            work.charge(1)?;
            let same = if let Some(previous) = &previous {
                previous.0.same_position(&piece.0)? && previous.1.same_position(&piece.1)?
            } else {
                false
            };
            if same {
                continue;
            }
            unique = unique.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            previous = Some(piece);
        }
        Ok(unique)
    }

    /// Noding is planar inside each chart strip; only pieces incident to
    /// opposite seam lifts can acquire new contacts after quotienting.
    #[expect(
        clippy::indexing_slicing,
        reason = "seam candidate ordinals index the parallel proxy-ID columns built by this sweep"
    )]
    fn refine_seam_aliases<W: Work + ?Sized>(
        &self,
        cuts: &mut FallibleAvl<CutEvent>,
        constructed_nodes: &mut FallibleAvl<ExactQuotientKey>,
        compare_cuts: &mut impl FnMut(&CutEvent, &CutEvent, &mut W) -> Result<Ordering>,
        compare_keys: &mut impl FnMut(&ExactQuotientKey, &ExactQuotientKey, &mut W) -> Result<Ordering>,
        budget: &mut ExpansionBudget,
        work: &mut W,
    ) -> Result<()> {
        let axis = self.frame.longitude();
        let half = ExactRational::from_f64(axis.half_period());
        let negative_half = ExactRational::from_f64(-axis.half_period());
        let period = ExactRational::from_f64(axis.period());
        let touches = |piece: &ExactChartPiece, seam: &ExactRational| -> Result<bool> {
            Ok(piece.start.x.try_cmp(seam)?.is_eq() || piece.end.x.try_cmp(seam)?.is_eq())
        };
        let mut positive_proxy_ids = Vec::new();
        let mut positive_bounds = Vec::new();
        let mut positive_unproxyable = Vec::new();
        let mut negative_ids = Vec::new();
        let mut negative_proxy_ids = Vec::new();
        let mut negative_bounds = Vec::new();
        let mut negative_unproxyable = Vec::new();
        for (index, piece) in self.pieces.iter().enumerate() {
            work.charge(1)?;
            let positive = touches(piece, &half)?;
            let negative = touches(piece, &negative_half)?;
            if positive {
                if let Some(bounds) = piece.outward_bounds()? {
                    positive_proxy_ids.push(index);
                    positive_bounds.push(bounds);
                } else {
                    positive_unproxyable.push(index);
                }
            }
            if negative {
                negative_ids.push(index);
                if let Some(bounds) = piece.shifted_x(&period)?.outward_bounds()? {
                    negative_proxy_ids.push(index);
                    negative_bounds.push(bounds);
                } else {
                    negative_unproxyable.push(index);
                }
            }
        }
        if !positive_bounds.is_empty() && !negative_bounds.is_empty() {
            let mut failure = None;
            let candidates = for_each_overlapping_bounds_cross_pair(
                &positive_bounds,
                &negative_bounds,
                |left, right| {
                    if let Err(error) = work.charge(1) {
                        failure = Some(error);
                        return std::ops::ControlFlow::Break(());
                    }
                    let left_piece = positive_proxy_ids[left];
                    let right_piece = negative_proxy_ids[right];
                    if left_piece == right_piece {
                        return std::ops::ControlFlow::Continue(());
                    }
                    match self.refine_piece_pair_shifted(
                        left_piece,
                        right_piece,
                        &period,
                        cuts,
                        constructed_nodes,
                        compare_cuts,
                        compare_keys,
                        budget,
                        work,
                    ) {
                        Ok(()) => std::ops::ControlFlow::Continue(()),
                        Err(error) => {
                            failure = Some(error);
                            std::ops::ControlFlow::Break(())
                        },
                    }
                },
            )?;
            if candidates.is_break() {
                return Err(failure.expect("a failed seam pair stores its first error"));
            }
        }
        for &left in &positive_unproxyable {
            for &right in &negative_ids {
                work.charge(1)?;
                if left != right {
                    self.refine_piece_pair_shifted(
                        left,
                        right,
                        &period,
                        cuts,
                        constructed_nodes,
                        compare_cuts,
                        compare_keys,
                        budget,
                        work,
                    )?;
                }
            }
        }
        for &left in &positive_proxy_ids {
            for &right in &negative_unproxyable {
                work.charge(1)?;
                if left != right {
                    self.refine_piece_pair_shifted(
                        left,
                        right,
                        &period,
                        cuts,
                        constructed_nodes,
                        compare_cuts,
                        compare_keys,
                        budget,
                        work,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn refine_piece_pair_shifted<W: Work + ?Sized>(
        &self,
        left: usize,
        right: usize,
        shift: &ExactRational,
        cuts: &mut FallibleAvl<CutEvent>,
        constructed_nodes: &mut FallibleAvl<ExactQuotientKey>,
        compare_cuts: &mut impl FnMut(&CutEvent, &CutEvent, &mut W) -> Result<Ordering>,
        compare_keys: &mut impl FnMut(&ExactQuotientKey, &ExactQuotientKey, &mut W) -> Result<Ordering>,
        budget: &mut ExpansionBudget,
        work: &mut W,
    ) -> Result<()> {
        let left_piece = self.pieces.get(left).ok_or_else(|| {
            GeometryErrorKind::Invalid("seam pair references an unknown left piece".into())
        })?;
        let right_piece = self.pieces.get(right).ok_or_else(|| {
            GeometryErrorKind::Invalid("seam pair references an unknown right piece".into())
        })?;
        let shifted_right = right_piece.shifted_x(shift)?;
        exact_piece_contacts(
            left_piece,
            &shifted_right,
            work,
            |point, left_t, right_t, work| {
                let right_point =
                    ExactChartPoint::from_rationals(point.x.try_subtract(shift)?, point.y.clone());
                let key = quotient_key(self.frame, &point)?;
                if constructed_nodes.insert_with_work(key, work, compare_keys)? {
                    budget.add(1)?;
                }
                insert_cut_event(cuts, left_piece, left_t, point, work, compare_cuts)?;
                insert_cut_event(cuts, right_piece, right_t, right_point, work, compare_cuts)
            },
        )
    }

    fn refine_source_points<W: Work + ?Sized>(
        &self,
        cuts: &mut FallibleAvl<CutEvent>,
        compare_cuts: &mut impl FnMut(&CutEvent, &CutEvent, &mut W) -> Result<Ordering>,
        proxy_piece_ids: &[usize],
        proxies: &[Segment],
        proxyable: &[bool],
        work: &mut W,
    ) -> Result<()> {
        if self.points.is_empty() || self.pieces.is_empty() {
            return Ok(());
        }
        let axis = self.frame.longitude();
        let half = ExactRational::from_f64(axis.half_period());
        let negative_half = ExactRational::from_f64(-axis.half_period());
        let point_capacity = self
            .points
            .len()
            .checked_mul(2)
            .ok_or(ResourceError::SizeOverflow)?;
        work.charge(u64::try_from(point_capacity).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut point_coordinates = Vec::with_capacity(point_capacity);
        for source in &self.points {
            work.charge(1)?;
            let point = source_chart_point(self.frame, source.point);
            if point.x.try_cmp(&half)?.is_eq() || point.x.try_cmp(&negative_half)?.is_eq() {
                work.charge(2)?;
                let y = point.y;
                point_coordinates.push(ExactChartPoint::from_rationals(
                    negative_half.clone(),
                    y.clone(),
                ));
                point_coordinates.push(ExactChartPoint::from_rationals(half.clone(), y));
            } else {
                point_coordinates.push(point);
            }
        }
        try_sort_by_with_work(&mut point_coordinates, work, |left, right, work| {
            work.charge(1)?;
            left.compare_lex(right)
        })?;
        work.charge(
            u64::try_from(point_coordinates.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut unique_points: Vec<ExactChartPoint> = Vec::with_capacity(point_coordinates.len());
        for point in point_coordinates {
            work.charge(1)?;
            if let Some(previous) = unique_points.last()
                && previous.same_position(&point)?
            {
                continue;
            }
            unique_points.push(point);
        }

        work.charge(
            u64::try_from(
                unique_points
                    .len()
                    .checked_mul(2)
                    .ok_or(ResourceError::SizeOverflow)?,
            )
            .map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut point_bounds = Vec::with_capacity(unique_points.len());
        let mut proxy_points = Vec::with_capacity(unique_points.len());
        let mut unproxyable_points = Vec::new();
        for point in unique_points {
            work.charge(1)?;
            let [min_x, min_y, max_x, max_y] = point.outward_bounds()?;
            if [min_x, min_y, max_x, max_y].into_iter().all(f64::is_finite) {
                point_bounds.push(Bounds::new_raw(min_x, min_y, max_x, max_y));
                proxy_points.push(point);
            } else {
                unproxyable_points.push(point);
            }
        }

        work.charge(u64::try_from(proxies.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut piece_bounds = Vec::with_capacity(proxies.len());
        for proxy in proxies {
            work.charge(1)?;
            piece_bounds.push(Bounds::new_raw(
                proxy.start.x,
                proxy.start.y,
                proxy.end.x,
                proxy.end.y,
            ));
        }
        if !piece_bounds.is_empty() && !point_bounds.is_empty() {
            let mut failure = None;
            let candidates = for_each_overlapping_bounds_cross_pair(
                &piece_bounds,
                &point_bounds,
                |piece_index, point_index| {
                    if let Err(error) = work.charge(1) {
                        failure = Some(error);
                        return std::ops::ControlFlow::Break(());
                    }
                    let Some(&piece_id) = proxy_piece_ids.get(piece_index) else {
                        failure = Some(
                            GeometryErrorKind::Invalid(
                                "Q point sweep has an unknown chart piece".into(),
                            )
                            .into(),
                        );
                        return std::ops::ControlFlow::Break(());
                    };
                    let Some(point) = proxy_points.get(point_index) else {
                        failure = Some(
                            GeometryErrorKind::Invalid(
                                "Q point sweep has an unknown source point".into(),
                            )
                            .into(),
                        );
                        return std::ops::ControlFlow::Break(());
                    };
                    match self.refine_piece_point(piece_id, point, cuts, compare_cuts, work) {
                        Ok(()) => std::ops::ControlFlow::Continue(()),
                        Err(error) => {
                            failure = Some(error);
                            std::ops::ControlFlow::Break(())
                        },
                    }
                },
            )?;
            if candidates.is_break() {
                return Err(failure.expect("a failed exact point contact stores its error"));
            }
        }
        for point in &unproxyable_points {
            for piece_index in 0..self.pieces.len() {
                work.charge(1)?;
                self.refine_piece_point(piece_index, point, cuts, compare_cuts, work)?;
            }
        }
        for (piece_index, &is_proxyable) in proxyable.iter().enumerate() {
            if !is_proxyable {
                for point in &proxy_points {
                    work.charge(1)?;
                    self.refine_piece_point(piece_index, point, cuts, compare_cuts, work)?;
                }
            }
        }
        Ok(())
    }

    fn refine_piece_point<W: Work + ?Sized>(
        &self,
        piece_index: usize,
        point: &ExactChartPoint,
        cuts: &mut FallibleAvl<CutEvent>,
        compare_cuts: &mut impl FnMut(&CutEvent, &CutEvent, &mut W) -> Result<Ordering>,
        work: &mut W,
    ) -> Result<()> {
        work.charge(1)?;
        let piece = self.pieces.get(piece_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q point contact has an unknown chart piece".into())
        })?;
        if point_on_exact_segment(point, &piece.start, &piece.end)? {
            work.charge(1)?;
            let parameter = parameter_on_exact_segment(point, &piece.start, &piece.end)?;
            insert_cut_event(cuts, piece, parameter, point.clone(), work, compare_cuts)?;
        }
        Ok(())
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "piece indices are generated from the admitted exact chart-piece slice"
    )]
    fn refine_piece_pair<W: Work + ?Sized>(
        &self,
        left: usize,
        right: usize,
        cuts: &mut FallibleAvl<CutEvent>,
        constructed_nodes: &mut FallibleAvl<ExactQuotientKey>,
        compare_cuts: &mut impl FnMut(&CutEvent, &CutEvent, &mut W) -> Result<Ordering>,
        compare_keys: &mut impl FnMut(&ExactQuotientKey, &ExactQuotientKey, &mut W) -> Result<Ordering>,
        budget: &mut ExpansionBudget,
        work: &mut W,
    ) -> Result<()> {
        let a = &self.pieces[left];
        let b = &self.pieces[right];
        work.charge(1)?;
        exact_piece_contacts(a, b, work, |point, left_t, right_t, work| {
            let key = quotient_key(self.frame, &point)?;
            if constructed_nodes.insert_with_work(key, work, compare_keys)? {
                budget.add(1)?;
            }
            insert_cut_event(cuts, a, left_t, point.clone(), work, compare_cuts)?;
            insert_cut_event(cuts, b, right_t, point, work, compare_cuts)?;
            Ok(())
        })
    }
}

fn interpolate_rational(
    start: &ExactRational,
    delta: &ExactRational,
    parameter: &ExactRational,
) -> Result<ExactRational> {
    start.try_add(&delta.try_product(parameter)?)
}

pub(super) struct LocalLongitudeEdge {
    pub(super) start_x: f64,
    pub(super) start_turn_adjust: i64,
    retained: RetainedChartDisplacement,
}

impl LocalLongitudeEdge {
    pub(super) fn delta_x(&self, period: f64) -> Result<ExactDyadic> {
        let [start, end] = self.retained.endpoints();
        let period_delta = ExactDyadic::from_f64(period)
            .try_product(&ExactDyadic::from_i64(self.retained.periods()))?;
        ExactDyadic::from_f64(end.get())
            .try_subtract_ref(&ExactDyadic::from_f64(start.get()))?
            .try_add(period_delta)
    }
}

pub(super) fn local_longitude_edge(
    frame: GeographicFrame,
    from: f64,
    retained: RetainedChartDisplacement,
) -> LocalLongitudeEdge {
    let axis = frame.longitude();
    let canonical_start = retained.endpoints()[0];
    let start_x = seam_start_spelling(axis, from, canonical_start);
    let start_turn_adjust = i64::from(start_x.to_bits() != canonical_start.get().to_bits());
    LocalLongitudeEdge {
        start_x,
        start_turn_adjust,
        retained,
    }
}

fn seam_start_spelling(
    axis: crate::geographic::PeriodicAxis,
    source: f64,
    canonical: crate::geographic::CanonicalLongitude,
) -> f64 {
    let identity = canonical.identity(axis);
    if identity == SeamIdentity::Seam && source.is_sign_positive() {
        axis.half_period()
    } else {
        canonical.get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geographic::{AngularFactor, GeographicAxisOrientation, source_admission_counts};
    use crate::geometry::{CoordSeq, LineSeq, MOrdinate, Ring, ZOrdinate};

    fn degrees() -> GeographicFrame {
        let unit = AngularFactor::try_new("test degrees", std::f64::consts::PI / 180.0)
            .expect("degrees are a valid angular factor");
        GeographicFrame::try_new(unit, unit, 0.0).expect("degree frame is valid")
    }

    fn line(points: &[(f64, f64)]) -> Shape {
        let points = points
            .iter()
            .map(|&(x, y)| Point::new_raw_xy(x, y))
            .collect::<Vec<_>>();
        Shape::LineString(
            LineSeq::try_new(CoordSeq::try_from_points(&points).expect("coordinates"))
                .expect("a line has two coordinates"),
        )
    }

    fn ring(points: &[(f64, f64)]) -> Ring {
        Ring::from_closed(
            CoordSeq::try_from_points(
                &points
                    .iter()
                    .map(|&(x, y)| Point::new_raw_xy(x, y))
                    .collect::<Vec<_>>(),
            )
            .unwrap(),
        )
        .unwrap()
    }

    fn chart_step_identity_holds(frame: GeographicFrame, from: f64, to: f64) -> bool {
        let axis = frame.longitude();
        let transit = axis.chart_transit(from, to).unwrap();
        let longitude = axis.chart_connection(from, to).unwrap().retained();
        let delta = local_longitude_edge(frame, from, longitude)
            .delta_x(frame.longitude().period())
            .unwrap();
        let canonical = |value: f64| ExactDyadic::from_f64(axis.canonicalize(value).unwrap().get());
        let period = ExactDyadic::from_f64(axis.period());
        let turn = match transit {
            0 => ExactDyadic::zero(),
            1 => period,
            -1 => period.neg(),
            other => panic!("an edge transits the seam at most once, got {other}"),
        };
        let expected = canonical(to)
            .try_subtract_ref(&canonical(from))
            .unwrap()
            .try_add(turn)
            .unwrap();
        delta.cmp(&expected).is_eq()
    }

    #[test]
    #[expect(
        clippy::float_cmp,
        reason = "exact half-period equality selects the source seam spelling without rounding"
    )]
    fn admitted_source_producer_preserves_raw_branch_and_piece_correspondence() {
        let degree = degrees();
        let grad_unit = AngularFactor::try_new("grads", std::f64::consts::TAU / 400.0).unwrap();
        let grad = GeographicFrame::try_new(grad_unit, grad_unit, 0.0).unwrap();
        let rad = GeographicFrame::try_new(
            AngularFactor::try_new("radians", 1.0).unwrap(),
            AngularFactor::try_new("radians", 1.0).unwrap(),
            0.0,
        )
        .unwrap();
        let tiny = f64::from_bits(1);
        let cases = [
            (degree, 0.0, 360.0, 0.0, 0.0, 1),
            (degree, 0.0, -360.0, 0.0, 0.0, -1),
            (degree, 0.0, 180.0, 0.0, -180.0, 1),
            (degree, 0.0, -180.0, 0.0, -180.0, 0),
            (degree, 180.0, 175.0, -180.0, 175.0, -1),
            (degree, 175.0, 180.0, 175.0, -180.0, 1),
            (degree, 530.0, 190.0, 170.0, -170.0, 1),
            (degree, 190.0, 530.0, -170.0, 170.0, -1),
            (degree, 2_f64.powi(60), 2_f64.powi(60), 136.0, 136.0, 0),
            (degree, -0.0, tiny, 0.0, tiny, 0),
            (degree, tiny, -tiny, tiny, -tiny, 0),
            (degree, 1.0e-20, 360.0, 1.0e-20, 0.0, 0),
            (grad, 195.0, -195.0, 195.0, -195.0, 1),
            (grad, -195.0, 195.0, -195.0, 195.0, -1),
            (rad, 3.0, -3.0, 3.0, -3.0, 1),
            (rad, -3.0, 3.0, -3.0, 3.0, -1),
        ];
        for (frame, from, to, native_from, native_to, periods) in cases {
            let source = [Point::new_raw_xy(from, 1.0), Point::new_raw_xy(to, 2.0)];
            let mut inventory = ExactGeographicChartInventory::new(frame, 1);
            let before = source_admission_counts::snapshot();
            let _ = inventory
                .append_source_edge(
                    source[0],
                    source[1],
                    SourceRowId(0),
                    SourceEdgeKind::Line {
                        line: SourceLineId(0),
                    },
                    0,
                )
                .unwrap();
            let after = source_admission_counts::snapshot();
            assert_eq!(after.0 - before.0, 1, "{from} -> {to}");
            let edge = &inventory.edges[0];
            for (actual, original) in edge.source.iter().zip(source) {
                assert_eq!(actual.x.to_bits(), original.x.to_bits());
                assert_eq!(actual.y.to_bits(), original.y.to_bits());
            }
            let retained = edge.retained_longitude();
            assert_eq!(retained.periods(), periods, "{from} -> {to}");
            assert_eq!(
                retained.endpoints()[0].get().to_bits(),
                native_from.to_bits()
            );
            assert_eq!(retained.endpoints()[1].get().to_bits(), native_to.to_bits());
            let delta = ExactDyadic::from_f64(native_to)
                .try_subtract_ref(&ExactDyadic::from_f64(native_from))
                .unwrap()
                .try_add(
                    ExactDyadic::from_f64(frame.longitude().period())
                        .try_product(&ExactDyadic::from_i64(periods))
                        .unwrap(),
                )
                .unwrap();
            let chart_start =
                if native_from == -frame.longitude().half_period() && from.is_sign_positive() {
                    frame.longitude().half_period()
                } else {
                    native_from
                };
            let alias_adjust = i64::from(chart_start.to_bits() != native_from.to_bits());
            for piece in &inventory.pieces {
                for (point, parameter) in [
                    (&piece.start, &piece.source_range.start),
                    (&piece.end, &piece.source_range.end),
                ] {
                    let original_x = ExactRational::from_f64(chart_start)
                        .try_add(
                            &ExactRational::from_dyadic(delta.clone())
                                .try_product(parameter)
                                .unwrap(),
                        )
                        .unwrap();
                    let lifted_x = point
                        .x
                        .try_add(
                            &ExactRational::from_f64(frame.longitude().period())
                                .try_product(&ExactRational::from_dyadic(ExactDyadic::from_i64(
                                    piece.lift + alias_adjust,
                                )))
                                .unwrap(),
                        )
                        .unwrap();
                    assert_eq!(
                        lifted_x.try_cmp(&original_x).unwrap(),
                        Ordering::Equal,
                        "{from} -> {to}"
                    );
                    let expected_y = ExactRational::one().try_add(parameter).unwrap();
                    assert_eq!(point.y.try_cmp(&expected_y).unwrap(), Ordering::Equal);
                }
            }
            let actual_delta = local_longitude_edge(frame, from, edge.retained_longitude())
                .delta_x(frame.longitude().period())
                .unwrap();
            assert_eq!(actual_delta, delta);
            assert_eq!(source_admission_counts::snapshot(), after);
        }
    }

    #[test]
    fn admitted_ring_producer_classifies_once_beyond_legitimate_summary() {
        let frame = degrees();
        let ring = ring(&[
            (175.0, 10.0),
            (-175.0, 10.0),
            (-175.0, 20.0),
            (175.0, 20.0),
            (175.0, 10.0),
        ]);
        let before = source_admission_counts::snapshot().0;
        RingSummary::try_new(frame, &crate::geographic::GeographicRing::from_ring(&ring)).unwrap();
        let summary_calls = source_admission_counts::snapshot().0 - before;
        let polygon = Polygon::new(ring, Vec::new());
        let mut inventory = ExactGeographicChartInventory::new(frame, 1);
        let before = source_admission_counts::snapshot().0;
        inventory.push_polygon(&polygon, SourceRowId(0)).unwrap();
        let actual = source_admission_counts::snapshot().0 - before;
        assert_eq!(actual, summary_calls + inventory.edges.len());
    }

    #[test]
    fn chart_transit_is_the_integer_quotient_of_the_exact_chart_step() {
        let radians = GeographicFrame::try_new(
            AngularFactor::try_new("radians", 1.0).unwrap(),
            AngularFactor::try_new("radians", 1.0).unwrap(),
            0.0,
        )
        .unwrap();
        let grad_unit = AngularFactor::try_new("grads", std::f64::consts::TAU / 400.0).unwrap();
        let grads = GeographicFrame::try_new(grad_unit, grad_unit, 0.0).unwrap();
        let tau = std::f64::consts::TAU;
        let pi = std::f64::consts::PI;
        let samples: [(&str, GeographicFrame, Vec<f64>); 3] = [
            ("degrees", degrees(), vec![
                -720.0, -540.0, -360.0, -300.0, -180.0, -90.0, 0.0, 90.0, 180.0, 270.0, 360.0,
                540.0, 720.0, 179.5, 359.5, -179.5, 0.25,
            ]),
            ("radians", radians, vec![
                -2.0 * tau,
                -tau,
                -pi,
                -pi / 2.0,
                0.0,
                pi / 2.0,
                pi,
                tau,
                2.0 * tau,
                1.0,
            ]),
            ("grads", grads, vec![
                -400.0, -200.0, -100.0, 0.0, 50.0, 100.0, 200.0, 300.0, 400.0, 600.0,
            ]),
        ];
        let mut checked = 0_usize;
        let mut failures = Vec::new();
        for (name, frame, values) in &samples {
            for &from in values {
                for &to in values {
                    if !chart_step_identity_holds(*frame, from, to) {
                        failures.push(format!("{name}: {from} -> {to}"));
                    }
                    checked += 1;
                }
            }
        }
        assert!(checked > 400);
        // Grads uses a rounded factor, so its nominal 200-grad tie differs
        // from the frame's exact derived half-period.
        assert!(failures.is_empty(), "{failures:?}");
    }

    /// A rounded raw difference of one period is not a circle unless the
    /// represented source endpoints differ by exactly that period.
    #[test]
    fn rounded_period_differences_do_not_create_false_circles() {
        let grad_unit = AngularFactor::try_new("grads", std::f64::consts::TAU / 400.0).unwrap();
        let grads = GeographicFrame::try_new(grad_unit, grad_unit, 0.0).unwrap();
        assert!(chart_step_identity_holds(degrees(), 1.0e-20, 360.0));
        assert!(chart_step_identity_holds(degrees(), 360.0, 1.0e-20));
        assert!(chart_step_identity_holds(grads, 40.5, 440.5));
    }

    fn inventory(frame: GeographicFrame, shapes: &[Shape]) -> ExactGeographicChartInventory {
        let inputs = GeographicInputs::try_from_shapes(frame, shapes.iter())
            .expect("source coordinates are admitted");
        ExactGeographicChartInventory::collect(&inputs).unwrap()
    }

    fn seam_node(chart: &NodedExactChart, latitude: &ExactRational) -> QuotientNodeId {
        chart
            .nodes
            .iter()
            .find_map(|node| match &node.key {
                ExactQuotientKey::Seam(y)
                    if y.try_cmp(latitude).expect("exact seam comparison").is_eq() =>
                {
                    Some(node.id)
                },
                ExactQuotientKey::Coordinate(_)
                | ExactQuotientKey::Seam(_)
                | ExactQuotientKey::NorthPole
                | ExactQuotientKey::SouthPole => None,
            })
            .expect("the exact seam node exists")
    }

    #[test]
    fn source_points_share_exact_seam_and_pole_nodes() {
        let source = Shape::geometry_collection(vec![
            Shape::Point(Point::new_raw_xy(-180.0, 5.0)),
            Shape::Point(Point::new_raw_xy(180.0, 5.0)),
            Shape::Point(Point::new_raw_xy(-30.0, 90.0)),
            Shape::Point(Point::new_raw_xy(120.0, 90.0)),
            Shape::Point(Point::new_raw_xy(-180.0, -90.0)),
            Shape::Point(Point::new_raw_xy(47.0, -90.0)),
            Shape::Point(Point::new_raw_xy(-180.0, 6.0)),
        ])
        .unwrap();
        let sources = inventory(degrees(), &[source]);
        let mut budget = ExpansionBudget::new("test", "exact chart");
        let chart = sources.node(&mut budget).unwrap();
        let point_node = |index: usize| {
            let port = chart
                .source_point_ports
                .get(index)
                .expect("source point port exists");
            chart
                .ports
                .get(port.0 as usize)
                .expect("source point port is admitted")
                .node
        };

        assert_eq!(chart.source_point_ports.len(), 7);
        assert_eq!(point_node(0), point_node(1));
        assert_eq!(point_node(2), point_node(3));
        assert_eq!(point_node(4), point_node(5));
        assert_ne!(point_node(2), point_node(4));
        assert_ne!(point_node(0), point_node(2));
        assert_ne!(point_node(0), point_node(6));
    }

    #[test]
    fn stored_vertex_identity_matches_exact_geographic_quotient() {
        let degrees = degrees();
        let degree_unit = AngularFactor::try_new("test degree", std::f64::consts::PI / 180.0)
            .expect("degree unit is valid");
        let grads_unit = AngularFactor::try_new("test grad", std::f64::consts::PI / 200.0)
            .expect("grad unit is valid");
        let grads = GeographicFrame::try_new(grads_unit, grads_unit, 0.0).unwrap();
        let prime_meridian = GeographicFrame::try_new(degree_unit, degree_unit, 0.1).unwrap();

        for (frame, points) in [
            (degrees, vec![
                XY::new(0.0, 0.0),
                XY::new(360.0, -0.0),
                XY::new(-0.0, 0.0),
                XY::new(180.0, 15.0),
                XY::new(-180.0, 15.0),
                XY::new(180.0, -15.0),
                XY::new(-180.0, -15.0),
                XY::new(0.0, 90.0),
                XY::new(123.0, 90.0),
                XY::new(-180.0, -90.0),
                XY::new(47.0, -90.0),
                XY::new(180.0_f64.next_down(), 15.0),
            ]),
            (grads, vec![
                XY::new(0.0, 0.0),
                XY::new(400.0, -0.0),
                XY::new(200.0, 25.0),
                XY::new(-200.0, 25.0),
                XY::new(200.0, -25.0),
                XY::new(-200.0, -25.0),
                XY::new(0.0, 100.0),
                XY::new(137.0, 100.0),
                XY::new(0.0, -100.0),
                XY::new(-91.0, -100.0),
            ]),
            (prime_meridian, vec![
                XY::new(0.0, 0.0),
                XY::new(360.0, -0.0),
                XY::new(180.0, 12.0),
                XY::new(-180.0, 12.0),
                XY::new(0.0, 90.0),
                XY::new(91.0, 90.0),
            ]),
        ] {
            let exact = points
                .iter()
                .copied()
                .map(|point| {
                    quotient_key(
                        frame,
                        &source_chart_point(frame, Point::new_raw_xy(point.x, point.y)),
                    )
                    .expect("stored coordinates form exact quotient keys")
                })
                .collect::<Vec<_>>();
            let stored = points
                .iter()
                .copied()
                .map(|point| {
                    crate::geometry::VertexKey::new(
                        Point::new_raw_xy(point.x, point.y),
                        crate::geometry::TopologyDomain::Geographic(frame),
                    )
                })
                .collect::<Vec<_>>();

            for ((left_point, left_exact), left_stored) in points.iter().zip(&exact).zip(&stored) {
                for ((right_point, right_exact), right_stored) in
                    points.iter().zip(&exact).zip(&stored)
                {
                    assert_eq!(
                        left_stored == right_stored,
                        compare_quotient_keys(left_exact, right_exact)
                            .expect("exact quotient keys compare")
                            .is_eq(),
                        "frame={frame:?}, left={left_point:?}, right={right_point:?}",
                    );
                }
            }
        }
    }

    fn source_uses_node(
        chart: &NodedExactChart,
        source: SourceEdgeId,
        node: QuotientNodeId,
    ) -> bool {
        chart.atoms.iter().any(|atom| {
            atom_touches_node(chart, atom, node)
                && atom.owners.iter().any(|owner| owner.source_edge == source)
        })
    }

    fn atom_touches_node(
        chart: &NodedExactChart,
        atom: &ExactChartAtom,
        node: QuotientNodeId,
    ) -> bool {
        atom.ports.iter().any(|&port_id| {
            chart
                .ports
                .get(port_id.0 as usize)
                .is_some_and(|port| port.node == node)
        })
    }

    #[test]
    fn seam_contacts_keep_exact_parameters_and_distinct_quotient_nodes() {
        let delta = f64::from_bits(1);
        let shapes = [
            line(&[(170.0, 0.0), (-160.0, delta)]),
            line(&[(150.0, 0.0), (-90.0, delta)]),
            line(&[(180.0, 0.0), (180.0, delta)]),
        ];
        let inventory = inventory(degrees(), &shapes);
        assert_eq!(inventory.pieces.len(), 5);
        let third =
            ExactRational::new(ExactDyadic::from_f64(delta), ExactDyadic::from_f64(3.0)).unwrap();
        let quarter =
            ExactRational::new(ExactDyadic::from_f64(delta), ExactDyadic::from_f64(4.0)).unwrap();
        let source_third =
            ExactRational::new(ExactDyadic::one(), ExactDyadic::from_f64(3.0)).unwrap();
        let source_quarter =
            ExactRational::new(ExactDyadic::one(), ExactDyadic::from_f64(4.0)).unwrap();
        let first_source = inventory.edges[0].id;
        let second_source = inventory.edges[1].id;
        assert_eq!(
            inventory.pieces[0]
                .source_range
                .end
                .try_cmp(&source_third)
                .unwrap(),
            Ordering::Equal
        );
        assert_eq!(
            inventory.pieces[2]
                .source_range
                .end
                .try_cmp(&source_quarter)
                .unwrap(),
            Ordering::Equal
        );
        assert_eq!(
            inventory.pieces[0].end.y.try_cmp(&third).unwrap(),
            Ordering::Equal
        );
        assert_eq!(
            inventory.pieces[2].end.y.try_cmp(&quarter).unwrap(),
            Ordering::Equal
        );

        let mut budget = ExpansionBudget::new("test", "exact chart");
        let chart = inventory.node(&mut budget).unwrap();
        let third_node = seam_node(&chart, &third);
        let quarter_node = seam_node(&chart, &quarter);
        let zero_node = seam_node(&chart, &ExactRational::zero());
        assert_ne!(third_node, quarter_node);
        assert_ne!(third_node, zero_node);
        assert_ne!(quarter_node, zero_node);
        assert!(source_uses_node(&chart, first_source, third_node));
        assert!(!source_uses_node(&chart, first_source, zero_node));
        assert!(source_uses_node(&chart, second_source, quarter_node));
        assert!(!source_uses_node(&chart, second_source, zero_node));
        let crossing = ExactChartPoint::from_rationals(
            ExactRational::new(ExactDyadic::from_f64(530.0), ExactDyadic::from_f64(3.0)).unwrap(),
            ExactRational::new(
                ExactDyadic::from_f64(delta)
                    .try_product(&ExactDyadic::from_f64(2.0))
                    .unwrap(),
                ExactDyadic::from_f64(9.0),
            )
            .unwrap(),
        );
        let crossing_port = find_chart_port(&chart.ports, &crossing).unwrap();
        let crossing_node = chart.ports[crossing_port.0 as usize].node;
        assert!(source_uses_node(&chart, first_source, crossing_node));
        assert!(source_uses_node(&chart, second_source, crossing_node));
        assert_eq!(budget.used(), 7);
    }

    #[test]
    fn opposite_seam_spans_node_overlaps_and_keep_each_source_parameter() {
        let shapes = [
            line(&[(180.0, 0.0), (180.0, 10.0)]),
            line(&[(-180.0, 5.0), (-180.0, 15.0)]),
        ];
        let inventory = inventory(degrees(), &shapes);
        let mut budget = ExpansionBudget::new("test", "exact chart");
        let chart = inventory.node(&mut budget).unwrap();
        let five = ExactRational::from_f64(5.0);
        let ten = ExactRational::from_f64(10.0);
        let five_node = seam_node(&chart, &five);
        let ten_node = seam_node(&chart, &ten);
        assert!(source_uses_node(&chart, SourceEdgeId(0), five_node));
        assert!(source_uses_node(&chart, SourceEdgeId(0), ten_node));
        assert!(source_uses_node(&chart, SourceEdgeId(1), five_node));
        assert!(source_uses_node(&chart, SourceEdgeId(1), ten_node));

        for source in [SourceEdgeId(0), SourceEdgeId(1)] {
            let mut ranges = chart
                .atoms
                .iter()
                .flat_map(|atom| atom.owners.iter())
                .filter(|owner| owner.source_edge == source)
                .map(|owner| owner.source_range.clone())
                .collect::<Vec<_>>();
            ranges.sort_unstable_by(|left, right| {
                left.start
                    .try_cmp(&right.start)
                    .expect("source parameters compare")
            });
            assert_eq!(ranges.len(), 2);
            assert!(
                ranges[0]
                    .start
                    .try_cmp(&ExactRational::zero())
                    .unwrap()
                    .is_eq()
            );
            assert!(
                ranges[0]
                    .end
                    .try_cmp(&ExactRational::from_f64(0.5))
                    .unwrap()
                    .is_eq()
            );
            assert!(
                ranges[1]
                    .start
                    .try_cmp(&ExactRational::from_f64(0.5))
                    .unwrap()
                    .is_eq()
            );
            assert!(
                ranges[1]
                    .end
                    .try_cmp(&ExactRational::one())
                    .unwrap()
                    .is_eq()
            );
        }
        assert_eq!(budget.used(), 2);
    }

    #[test]
    fn proper_multiway_crossings_share_one_exact_node() {
        let shapes = [
            line(&[(0.0, 0.0), (4.0, 4.0)]),
            line(&[(0.0, 4.0), (4.0, 0.0)]),
            line(&[(2.0, -1.0), (2.0, 5.0)]),
        ];
        let inventory = inventory(degrees(), &shapes);
        let mut budget = ExpansionBudget::new("test", "exact chart");
        let chart = inventory.node(&mut budget).unwrap();
        let center = ExactChartPoint::from_xy(XY::new(2.0, 2.0));
        let center_port = find_chart_port(&chart.ports, &center).unwrap();
        let center_node = chart.ports[center_port.0 as usize].node;
        let incident_sources = chart
            .atoms
            .iter()
            .filter(|atom| atom_touches_node(&chart, atom, center_node))
            .flat_map(|atom| atom.owners.iter().map(|owner| owner.source_edge))
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(incident_sources.len(), 3);
        assert_eq!(budget.used(), 4);
    }

    #[test]
    fn crossing_lines_keep_each_exact_half_source_range() {
        let shapes = [
            line(&[(0.0, 0.0), (4.0, 4.0)]),
            line(&[(0.0, 4.0), (4.0, 0.0)]),
        ];
        let inventory = inventory(degrees(), &shapes);
        let mut budget = ExpansionBudget::new("test", "exact chart");
        let chart = inventory.node(&mut budget).unwrap();
        let half = ExactRational::from_f64(0.5);

        assert_eq!(chart.atoms.len(), 4);
        assert!(chart.atoms.iter().all(|atom| atom.owners.len() == 1));
        for source in [SourceEdgeId(0), SourceEdgeId(1)] {
            let mut ranges = chart
                .atoms
                .iter()
                .flat_map(|atom| atom.owners.iter())
                .filter(|owner| owner.source_edge == source)
                .map(|owner| owner.source_range.clone())
                .collect::<Vec<_>>();
            ranges.sort_unstable_by(|left, right| {
                left.start
                    .try_cmp(&right.start)
                    .expect("source parameters compare")
            });
            assert_eq!(ranges.len(), 2);
            assert_eq!(
                ranges[0].start.try_cmp(&ExactRational::zero()).unwrap(),
                Ordering::Equal
            );
            assert_eq!(ranges[0].end.try_cmp(&half).unwrap(), Ordering::Equal);
            assert_eq!(ranges[1].start.try_cmp(&half).unwrap(), Ordering::Equal);
            assert_eq!(
                ranges[1].end.try_cmp(&ExactRational::one()).unwrap(),
                Ordering::Equal
            );
        }
    }

    #[test]
    fn seam_split_ring_edges_keep_exact_parameters_and_quotient_owners() {
        let shape = Shape::Polygon(Polygon::new(
            ring(&[
                (170.0, 0.0),
                (-170.0, 0.0),
                (-170.0, 10.0),
                (170.0, 10.0),
                (170.0, 0.0),
            ]),
            vec![],
        ));
        let inventory = inventory(degrees(), &[shape]);
        let mut budget = ExpansionBudget::new("test", "exact chart");
        let chart = inventory.node(&mut budget).unwrap();
        let half = ExactRational::from_f64(0.5);

        for source in [SourceEdgeId(0), SourceEdgeId(2)] {
            let mut ranges = chart
                .atoms
                .iter()
                .flat_map(|atom| atom.owners.iter())
                .filter(|owner| owner.source_edge == source)
                .map(|owner| owner.source_range.clone())
                .collect::<Vec<_>>();
            ranges.sort_unstable_by(|left, right| {
                left.start
                    .try_cmp(&right.start)
                    .expect("source parameters compare")
            });
            assert_eq!(ranges.len(), 2);
            assert_eq!(
                ranges[0].start.try_cmp(&ExactRational::zero()).unwrap(),
                Ordering::Equal
            );
            assert_eq!(ranges[0].end.try_cmp(&half).unwrap(), Ordering::Equal);
            assert_eq!(ranges[1].start.try_cmp(&half).unwrap(), Ordering::Equal);
            assert_eq!(
                ranges[1].end.try_cmp(&ExactRational::one()).unwrap(),
                Ordering::Equal
            );
        }

        for (latitude, source) in [
            (ExactRational::zero(), SourceEdgeId(0)),
            (ExactRational::from_f64(10.0), SourceEdgeId(2)),
        ] {
            let node = seam_node(&chart, &latitude);
            assert!(source_uses_node(&chart, source, node));
        }
    }

    #[test]
    fn reversed_native_axis_edges_keep_exact_seam_parameters() {
        let frame = GeographicFrame::try_new_with_orientation(
            AngularFactor::try_new("west longitude unit", std::f64::consts::TAU / 8.0).unwrap(),
            AngularFactor::try_new("south latitude radian", 1.0).unwrap(),
            0.0,
            GeographicAxisOrientation::from_axis_directions(false, false),
        )
        .unwrap();
        let sources = inventory(frame, &[
            line(&[(-3.75, 0.0), (3.25, -1.0)]),
            line(&[(3.25, -1.0), (-3.75, 0.0)]),
        ]);
        let quarter = ExactRational::from_f64(0.25);
        let three_quarters = ExactRational::from_f64(0.75);
        let seam_latitude = ExactRational::from_f64(-0.25);

        for (source, parameter) in [
            (SourceEdgeId(0), &quarter),
            (SourceEdgeId(1), &three_quarters),
        ] {
            let pieces = sources
                .pieces
                .iter()
                .filter(|piece| piece.source_edge == source)
                .collect::<Vec<_>>();
            assert_eq!(pieces.len(), 2);
            let crossing = pieces
                .iter()
                .flat_map(|piece| [&piece.start, &piece.end])
                .find(|point| {
                    point
                        .x
                        .try_cmp(&ExactRational::from_f64(-4.0))
                        .unwrap()
                        .is_eq()
                        || point
                            .x
                            .try_cmp(&ExactRational::from_f64(4.0))
                            .unwrap()
                            .is_eq()
                })
                .expect("source edge crosses the native longitude seam");
            assert_eq!(crossing.y.try_cmp(&seam_latitude).unwrap(), Ordering::Equal);
            assert!(
                pieces
                    .iter()
                    .any(|piece| { piece.source_range.end.try_cmp(parameter).unwrap().is_eq() })
                    || pieces.iter().any(|piece| {
                        piece.source_range.start.try_cmp(parameter).unwrap().is_eq()
                    })
            );
        }
        let mut budget = ExpansionBudget::new("test", "exact chart");
        let chart = sources.node(&mut budget).unwrap();
        let seam = seam_node(&chart, &seam_latitude);
        assert!(source_uses_node(&chart, SourceEdgeId(0), seam));
        assert!(source_uses_node(&chart, SourceEdgeId(1), seam));
    }

    #[test]
    fn one_ulp_source_endpoint_is_shared_by_adjacent_exact_edges() {
        let endpoint = (-170.0_f64).next_down();
        let shape = Shape::Polygon(Polygon::new(
            ring(&[
                (170.0, 0.0),
                (endpoint, 0.0),
                (endpoint, 10.0),
                (170.0, 10.0),
                (170.0, 0.0),
            ]),
            vec![],
        ));
        let inventory = inventory(degrees(), &[shape]);
        let mut budget = ExpansionBudget::new("test", "exact chart");
        let chart = inventory.node(&mut budget).unwrap();
        let first_edge = chart
            .source_edge_ports
            .first()
            .expect("first ring edge exists");
        let second_edge = chart
            .source_edge_ports
            .get(1)
            .expect("adjacent ring edge exists");
        let first_endpoint = chart
            .ports
            .get(first_edge[1].0 as usize)
            .expect("source endpoint port exists");
        let second_start = chart
            .ports
            .get(second_edge[0].0 as usize)
            .expect("adjacent source port exists");

        assert_eq!(first_endpoint.node, second_start.node);
        assert_eq!(
            first_endpoint
                .point
                .x
                .try_cmp(&ExactRational::from_f64(endpoint))
                .unwrap(),
            Ordering::Equal,
        );
        assert_eq!(
            first_endpoint
                .point
                .y
                .try_cmp(&ExactRational::zero())
                .unwrap(),
            Ordering::Equal,
        );
    }

    #[test]
    fn collinear_overlap_atoms_retain_each_source_parameter_range() {
        let shapes = [
            line(&[(0.0, 0.0), (4.0, 0.0)]),
            line(&[(2.0, 0.0), (6.0, 0.0)]),
        ];
        let inventory = inventory(degrees(), &shapes);
        let mut budget = ExpansionBudget::new("test", "exact chart");
        let chart = inventory.node(&mut budget).unwrap();
        let overlaps = chart
            .atoms
            .iter()
            .filter(|atom| atom.owners.len() == 2)
            .collect::<Vec<_>>();
        assert_eq!(overlaps.len(), 1);
        let mut owners = overlaps[0].owners.iter().collect::<Vec<_>>();
        owners.sort_unstable_by_key(|owner| owner.source_edge);
        let half = ExactRational::from_f64(0.5);
        assert_eq!(owners[0].source_edge, SourceEdgeId(0));
        assert_eq!(
            owners[0].source_range.start.try_cmp(&half).unwrap(),
            Ordering::Equal
        );
        assert_eq!(
            owners[0]
                .source_range
                .end
                .try_cmp(&ExactRational::one())
                .unwrap(),
            Ordering::Equal
        );
        assert_eq!(owners[1].source_edge, SourceEdgeId(1));
        assert_eq!(
            owners[1]
                .source_range
                .start
                .try_cmp(&ExactRational::zero())
                .unwrap(),
            Ordering::Equal
        );
        assert_eq!(
            owners[1].source_range.end.try_cmp(&half).unwrap(),
            Ordering::Equal
        );
        assert_eq!(chart.atoms.len(), 3);
    }

    #[test]
    fn endpoint_contact_splits_the_interior_span_and_keeps_both_owners() {
        let shapes = [
            line(&[(0.0, 0.0), (4.0, 0.0)]),
            line(&[(2.0, -1.0), (2.0, 0.0)]),
        ];
        let inventory = inventory(degrees(), &shapes);
        let mut budget = ExpansionBudget::new("test", "exact chart");
        let chart = inventory.node(&mut budget).unwrap();
        let contact = ExactChartPoint::from_xy(XY::new(2.0, 0.0));
        let contact_port = find_chart_port(&chart.ports, &contact).unwrap();
        let contact_node = chart.ports[contact_port.0 as usize].node;
        let mut ranges = chart
            .atoms
            .iter()
            .filter(|atom| atom_touches_node(&chart, atom, contact_node))
            .flat_map(|atom| {
                atom.owners
                    .iter()
                    .map(|owner| (owner.source_edge, owner.source_range.clone()))
            })
            .collect::<Vec<_>>();
        ranges.sort_unstable_by_key(|(source, _)| *source);
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].0, SourceEdgeId(0));
        assert!(
            ranges[0]
                .1
                .start
                .try_cmp(&ExactRational::zero())
                .unwrap()
                .is_eq()
        );
        assert!(
            ranges[0]
                .1
                .end
                .try_cmp(&ExactRational::from_f64(0.5))
                .unwrap()
                .is_eq()
        );
        assert_eq!(ranges[1].0, SourceEdgeId(0));
        assert!(
            ranges[1]
                .1
                .start
                .try_cmp(&ExactRational::from_f64(0.5))
                .unwrap()
                .is_eq()
        );
        assert!(
            ranges[1]
                .1
                .end
                .try_cmp(&ExactRational::one())
                .unwrap()
                .is_eq()
        );
        assert_eq!(ranges[2].0, SourceEdgeId(1));
        assert!(
            ranges[2]
                .1
                .start
                .try_cmp(&ExactRational::zero())
                .unwrap()
                .is_eq()
        );
        assert!(
            ranges[2]
                .1
                .end
                .try_cmp(&ExactRational::one())
                .unwrap()
                .is_eq()
        );
        assert_eq!(budget.used(), 1);
    }

    #[test]
    fn distinct_chart_ports_at_a_pole_share_one_quotient_node() {
        let shapes = [
            line(&[(0.0, 80.0), (0.0, 90.0)]),
            line(&[(120.0, 90.0), (120.0, 80.0)]),
        ];
        let inventory = inventory(degrees(), &shapes);
        let mut budget = ExpansionBudget::new("test", "exact chart");
        let chart = inventory.node(&mut budget).unwrap();
        let north_pole = chart
            .nodes
            .iter()
            .find(|node| matches!(node.key, ExactQuotientKey::NorthPole))
            .expect("the source edges reach the north pole");
        let first_port =
            find_chart_port(&chart.ports, &ExactChartPoint::from_xy(XY::new(0.0, 90.0))).unwrap();
        let second_port = find_chart_port(
            &chart.ports,
            &ExactChartPoint::from_xy(XY::new(120.0, 90.0)),
        )
        .unwrap();
        assert_ne!(first_port, second_port);
        assert_eq!(chart.ports[first_port.0 as usize].node, north_pole.id);
        assert_eq!(chart.ports[second_port.0 as usize].node, north_pole.id);
        let incident_sources = chart
            .atoms
            .iter()
            .filter(|atom| atom_touches_node(&chart, atom, north_pole.id))
            .flat_map(|atom| atom.owners.iter().map(|owner| owner.source_edge))
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(
            incident_sources,
            [SourceEdgeId(0), SourceEdgeId(1)].into_iter().collect()
        );
    }

    #[test]
    fn inventory_retains_full_turn_ring_and_source_ownership() {
        let shell = ring(&[
            (0.0, 10.0),
            (90.0, 20.0),
            (180.0, 30.0),
            (-90.0, 20.0),
            (0.0, 10.0),
        ]);
        let hole = ring(&[(10.0, 2.0), (20.0, 3.0), (15.0, 4.0), (10.0, 2.0)]);
        let polygon = Polygon::new(shell, vec![hole]);
        let shapes = [Shape::Polygon(polygon), line(&[(1.0, 1.0), (2.0, 2.0)])];
        let inventory = inventory(degrees(), &shapes);
        let mut budget = ExpansionBudget::new("test", "exact chart");
        let chart = inventory.node(&mut budget).unwrap();
        assert_eq!(inventory.row_count, 2);
        assert_eq!(inventory.polygons.len(), 1);
        assert_eq!(inventory.polygons[0].row, SourceRowId(0));
        assert_eq!(inventory.rings.len(), 2);
        assert_eq!(inventory.rings[0].id, SourceRingId(0));
        assert_eq!(inventory.rings[0].polygon, SourcePolygonId(0));
        assert!(!inventory.rings[0].hole);
        assert_eq!(
            inventory.rings[0].summary.turn(),
            crate::geographic::RingTurn::Full(crate::geographic::TurnDirection::Positive,)
        );
        assert!(!inventory.rings[0].summary.exact_axis_band);
        assert!(matches!(
            inventory.rings[0].summary.domain,
            crate::geographic::RingDomain::FullLongitude(_)
        ));
        assert_eq!(inventory.rings[0].source_edges, 0..4);
        assert_eq!(inventory.rings[1].id, SourceRingId(1));
        assert!(inventory.rings[1].hole);
        assert_eq!(inventory.rings[1].polygon, SourcePolygonId(0));
        assert_eq!(inventory.rings[1].source_edges, 4..7);
        assert_eq!(inventory.edges[0].row, SourceRowId(0));
        assert!(matches!(inventory.edges[0].kind, SourceEdgeKind::Ring {
            polygon: SourcePolygonId(0),
            ring: SourceRingId(0),
            hole: false,
            ..
        }));
        assert_eq!(inventory.edges[7].row, SourceRowId(1));
        for edge in &inventory.edges[..4] {
            let mut ranges = chart
                .atoms
                .iter()
                .flat_map(|atom| atom.owners.iter())
                .filter(|owner| owner.source_edge == edge.id)
                .map(|owner| owner.source_range.clone())
                .collect::<Vec<_>>();
            ranges.sort_unstable_by(|left, right| {
                left.start
                    .try_cmp(&right.start)
                    .expect("source parameters compare")
            });
            assert!(!ranges.is_empty());
            assert!(
                ranges[0]
                    .start
                    .try_cmp(&ExactRational::zero())
                    .unwrap()
                    .is_eq()
            );
            for pair in ranges.windows(2) {
                let [left, right] = pair else {
                    unreachable!("windows(2) yields adjacent source parameter ranges");
                };
                assert!(left.end.try_cmp(&right.start).unwrap().is_eq());
            }
            assert!(
                ranges
                    .last()
                    .unwrap()
                    .end
                    .try_cmp(&ExactRational::one())
                    .unwrap()
                    .is_eq()
            );
        }
    }

    #[test]
    fn inventory_retains_multi_turn_ring_count_beyond_summary_class() {
        let shell = ring(&[
            (0.0, 10.0),
            (120.0, 10.0),
            (240.0, 10.0),
            (360.0, 10.0),
            (480.0, 10.0),
            (600.0, 10.0),
            (720.0, 10.0),
            (0.0, 10.0),
        ]);
        let polygon = Polygon::new(shell, Vec::new());
        let inventory = inventory(degrees(), &[Shape::Polygon(polygon)]);
        assert_eq!(
            inventory.rings[0].summary.turn(),
            crate::geographic::RingTurn::Other
        );
        assert_eq!(inventory.rings[0].summary.chart_turn, 2);
    }

    #[test]
    fn exact_chart_split_stays_finite_at_the_maximum_period() {
        let longitude =
            AngularFactor::try_new("maximum-period longitude", std::f64::consts::TAU / f64::MAX)
                .unwrap();
        let latitude =
            AngularFactor::try_new("degree latitude", std::f64::consts::PI / 180.0).unwrap();
        let frame = GeographicFrame::try_new(longitude, latitude, 0.0).unwrap();
        let half = frame.longitude().half_period();
        let shape = line(&[(-half, 0.0), (half.next_down(), 1.0)]);
        let inventory = inventory(frame, &[shape]);
        let mut budget = ExpansionBudget::new("test", "exact chart");
        let chart = inventory.node(&mut budget).unwrap();
        assert!(!chart.atoms.is_empty());
        assert!(
            chart
                .ports
                .iter()
                .flat_map(|port| port.point.outward_bounds().unwrap())
                .all(f64::is_finite)
        );
    }

    #[test]
    fn exact_inventory_retains_original_zm_endpoints() {
        let start = Point::new_raw_axes(170.0, 0.0, ZOrdinate(Some(10.0)), MOrdinate(Some(100.0)));
        let end = Point::new_raw_axes(-160.0, 1.0, ZOrdinate(Some(20.0)), MOrdinate(Some(200.0)));
        let shape = Shape::LineString(
            LineSeq::try_new(CoordSeq::try_from_points(&[start, end]).unwrap()).unwrap(),
        );
        let inventory = inventory(degrees(), &[shape]);
        assert_eq!(inventory.edges[0].source, [start, end]);
        assert!(
            inventory.pieces[0]
                .source_range
                .start
                .try_cmp(&ExactRational::zero())
                .unwrap()
                .is_eq()
        );
        assert!(
            inventory.pieces[1]
                .source_range
                .end
                .try_cmp(&ExactRational::one())
                .unwrap()
                .is_eq()
        );
    }
}
