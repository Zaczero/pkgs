//! Original-source geographic chart inventory and exact noding.
//!
//! Binary64 coordinates and the frame period are admitted values, not rounded
//! construction results. Chart cuts and contacts retain exact source
//! parameters; their binary64 coordinates are derived witnesses only.

use std::cmp::Ordering;
use std::ops::Range;

use crate::error::{ResourceError, Result};
use crate::geographic::retained_exact::edges::retained_source_interval_reads_as;
use crate::geographic::retained_exact::sites::split_rational_native_site_with_work;
use crate::geographic::{
    GeographicFrame, Latitude, RetainedChartDisplacement, RingDomain, RingSummary, SeamIdentity,
};
#[cfg(test)]
use crate::geometry::XY;
use crate::geometry::numerics::product::ExactSign;
use crate::geometry::tessellation::exact::{CompactLimbs, ExactDyadic, ExactRational};
use crate::geometry::{GeometryErrorKind, Point, RingKind};
use crate::work::{Unmetered, Work};

mod collect;
mod events;
mod fallible_avl;
mod keys;
mod node;
mod refine;

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
pub(super) const fn rational_heap_bytes(value: &ExactRational) -> usize {
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

    pub(in crate::geometry) fn outward_bounds(&self) -> Result<[f64; 4]> {
        self.outward_bounds_with_work(&mut Unmetered)
    }

    pub(in crate::geometry) fn outward_bounds_with_work(
        &self,
        work: &mut impl Work,
    ) -> Result<[f64; 4]> {
        let (min_x, max_x) = self.x.outward_bounds_with_work(work)?;
        let (min_y, max_y) = self.y.outward_bounds_with_work(work)?;
        Ok([min_x, min_y, max_x, max_y])
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "HeapSize sums the two already-owned chart-coordinate rationals"
    )]
    pub(super) const fn heap_bytes(&self) -> usize {
        rational_heap_bytes(&self.x) + rational_heap_bytes(&self.y)
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
    let (native, _) = split_rational_native_site_with_work(
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
        retained_source_interval_reads_as(
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
    pub(super) const fn heap_bytes(&self) -> usize {
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
mod tests;
