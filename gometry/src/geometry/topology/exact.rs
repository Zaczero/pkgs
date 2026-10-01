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
    EdgeDirection, GeographicFrame, Latitude, RingDomain, RingSummary, ScalarGeographicView,
    SeamIdentity, TurnDirection,
};
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::segments::try_sort_by;
use crate::geometry::tessellation::exact::{CompactLimbs, ExactDyadic, ExactRational};
use crate::geometry::{
    Bounds, Component, Coordinates as _, ExpansionBudget, GeometryErrorKind, Point, Polygon,
    RingKind, RingWinding, Segment, Shape, XY, for_each_candidate_pair,
    for_each_overlapping_bounds_cross_pair,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct SourceRowId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct SourcePolygonId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct SourceRingId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct SourceEdgeId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct ChartPieceId(pub u32);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ExactChartPoint {
    pub(super) x: ExactRational,
    pub(super) y: ExactRational,
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

    pub(super) const fn from_rationals(x: ExactRational, y: ExactRational) -> Self {
        Self { x, y }
    }

    pub(super) fn same_position(&self, other: &Self) -> Result<bool> {
        Ok(self.x.try_cmp(&other.x)?.is_eq() && self.y.try_cmp(&other.y)?.is_eq())
    }

    pub(super) fn compare_lex(&self, other: &Self) -> Result<Ordering> {
        let x = self.x.try_cmp(&other.x)?;
        Ok(if x.is_eq() {
            self.y.try_cmp(&other.y)?
        } else {
            x
        })
    }

    pub(super) fn compare_x(&self, other: &Self) -> Result<Ordering> {
        self.x.try_cmp(&other.x)
    }

    pub(super) fn compare_y(&self, other: &Self) -> Result<Ordering> {
        self.y.try_cmp(&other.y)
    }

    pub(super) fn outward_bounds(&self) -> Result<[f64; 4]> {
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
        })
    }
}

pub(super) fn quotient_key(
    frame: GeographicFrame,
    point: &ExactChartPoint,
) -> Result<ExactQuotientKey> {
    let north = ExactRational::from_f64(frame.pole());
    let south = ExactRational::from_f64(-frame.pole());
    if point.y.try_cmp(&north)?.is_eq() {
        return Ok(ExactQuotientKey::NorthPole);
    }
    if point.y.try_cmp(&south)?.is_eq() {
        return Ok(ExactQuotientKey::SouthPole);
    }
    let half = ExactRational::from_f64(frame.longitude().half_period());
    if point.x.try_cmp(&half)?.is_eq()
        || point
            .x
            .try_cmp(&half.try_subtract(&ExactRational::from_f64(frame.longitude().period()))?)?
            .is_eq()
    {
        return Ok(ExactQuotientKey::Seam(point.y.clone()));
    }
    Ok(ExactQuotientKey::Coordinate(point.clone()))
}

pub(super) fn compare_quotient_keys(
    left: &ExactQuotientKey,
    right: &ExactQuotientKey,
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
        (Coordinate(left), Coordinate(right)) => left.compare_lex(right),
        (Seam(left), Seam(right)) => left.try_cmp(right),
        (NorthPole, NorthPole) | (SouthPole, SouthPole) => Ok(Ordering::Equal),
        _ => unreachable!("equal quotient-key ranks have the same variant"),
    }
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "binary-search bounds remain within the sorted exact chart-port slice"
)]
fn find_chart_port(ports: &[ChartPort], point: &ExactChartPoint) -> Result<ChartPortId> {
    let mut low = 0_usize;
    let mut high = ports.len();
    while low < high {
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
        .expect("source longitude was admitted")
        .get();
    ExactChartPoint::from_dyadics(
        ExactDyadic::from_f64(seam_start_spelling(frame.longitude(), point.x, canonical)),
        ExactDyadic::from_f64(Latitude::from_admitted(frame, point.y).get()),
    )
}

pub(super) fn vector_difference(
    start: &ExactChartPoint,
    end: &ExactChartPoint,
) -> Result<(ExactRational, ExactRational)> {
    Ok((end.x.try_subtract(&start.x)?, end.y.try_subtract(&start.y)?))
}

pub(super) fn vector_cross(
    left: &(ExactRational, ExactRational),
    right: &(ExactRational, ExactRational),
) -> Result<ExactRational> {
    left.0
        .try_product(&right.1)?
        .try_subtract(&left.1.try_product(&right.0)?)
}

pub(super) fn point_on_exact_segment(
    point: &ExactChartPoint,
    start: &ExactChartPoint,
    end: &ExactChartPoint,
) -> Result<bool> {
    let line = vector_difference(start, end)?;
    let offset = vector_difference(start, point)?;
    if !vector_cross(&line, &offset)?.is_zero() {
        return Ok(false);
    }
    let (min_x, max_x) = if start.compare_x(end)?.is_le() {
        (&start.x, &end.x)
    } else {
        (&end.x, &start.x)
    };
    let (min_y, max_y) = if start.compare_y(end)?.is_le() {
        (&start.y, &end.y)
    } else {
        (&end.y, &start.y)
    };
    Ok(point.x.try_cmp(min_x)?.is_ge()
        && point.x.try_cmp(max_x)?.is_le()
        && point.y.try_cmp(min_y)?.is_ge()
        && point.y.try_cmp(max_y)?.is_le())
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
    Ok(
        parameter.sign() != ExactSign::Negative
            && parameter.try_cmp(&ExactRational::one())?.is_le(),
    )
}

fn compare_cut_events(left: &CutEvent, right: &CutEvent) -> Result<Ordering> {
    let piece = left.piece.cmp(&right.piece);
    Ok(if piece.is_eq() {
        left.parameter.try_cmp(&right.parameter)?
    } else {
        piece
    })
}

fn insert_cut_event(
    cuts: &mut FallibleAvl<CutEvent>,
    piece: &ExactChartPiece,
    parameter: ExactRational,
    point: ExactChartPoint,
    compare: &mut impl FnMut(&CutEvent, &CutEvent) -> Result<Ordering>,
) -> Result<()> {
    if parameter.is_zero() {
        if !point.same_position(&piece.start)? {
            return Err(GeometryErrorKind::Invalid(
                "an exact source-start parameter produced a different chart point".into(),
            )
            .into());
        }
        return Ok(());
    }
    if parameter.is_one() {
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
    if !cuts.insert(event.clone(), compare)?
        && !cuts
            .get(&event, compare)?
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

fn exact_piece_contacts(
    left: &ExactChartPiece,
    right: &ExactChartPiece,
    mut contact: impl FnMut(ExactChartPoint, ExactRational, ExactRational) -> Result<()>,
) -> Result<()> {
    let r = vector_difference(&left.start, &left.end)?;
    let s = vector_difference(&right.start, &right.end)?;
    let q_minus_p = vector_difference(&left.start, &right.start)?;
    let denominator = vector_cross(&r, &s)?;
    if denominator.is_zero() {
        if !vector_cross(&q_minus_p, &r)?.is_zero() {
            return Ok(());
        }
        for point in [
            left.start.clone(),
            left.end.clone(),
            right.start.clone(),
            right.end.clone(),
        ] {
            if point_on_exact_segment(&point, &left.start, &left.end)?
                && point_on_exact_segment(&point, &right.start, &right.end)?
            {
                contact(
                    point.clone(),
                    parameter_on_exact_segment(&point, &left.start, &left.end)?,
                    parameter_on_exact_segment(&point, &right.start, &right.end)?,
                )?;
            }
        }
        return Ok(());
    }

    let left_parameter = vector_cross(&q_minus_p, &s)?.try_quotient(&denominator)?;
    if !in_unit_interval(&left_parameter)? {
        return Ok(());
    }
    let right_parameter = vector_cross(&q_minus_p, &r)?.try_quotient(&denominator)?;
    if !in_unit_interval(&right_parameter)? {
        return Ok(());
    }
    let point = interpolate_exact_segment(&left.start, &left.end, &left_parameter)?;
    contact(point, left_parameter, right_parameter)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ExactSourceRange {
    pub start: ExactRational,
    pub end: ExactRational,
}

impl ExactSourceRange {
    pub(super) fn compose(
        &self,
        local_start: &ExactRational,
        local_end: &ExactRational,
    ) -> Result<Self> {
        let width = self.end.try_subtract(&self.start)?;
        Ok(Self {
            start: self.start.try_add(&width.try_product(local_start)?)?,
            end: self.start.try_add(&width.try_product(local_end)?)?,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SourceEdgeKind {
    Line,
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
    pub row: SourceRowId,
    pub kind: SourceEdgeKind,
}

#[derive(Clone, Debug)]
pub(super) struct ExactChartPiece {
    pub id: ChartPieceId,
    pub source_edge: SourceEdgeId,
    pub start: ExactChartPoint,
    pub end: ExactChartPoint,
    pub source_range: ExactSourceRange,
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
        fn descend<T>(
            tree: &mut FallibleAvl<T>,
            root: Option<usize>,
            value: T,
            compare: &mut impl FnMut(&T, &T) -> Result<Ordering>,
        ) -> Result<(usize, bool)> {
            let Some(root) = root else {
                let index = tree.nodes.len();
                tree.nodes.push(AvlNode {
                    value,
                    left: None,
                    right: None,
                    height: 1,
                });
                return Ok((index, true));
            };
            match compare(&value, &tree.nodes[root].value)? {
                Ordering::Equal => Ok((root, false)),
                Ordering::Less => {
                    let (left, inserted) = descend(tree, tree.nodes[root].left, value, compare)?;
                    tree.nodes[root].left = Some(left);
                    Ok((tree.balance(root), inserted))
                },
                Ordering::Greater => {
                    let (right, inserted) = descend(tree, tree.nodes[root].right, value, compare)?;
                    tree.nodes[root].right = Some(right);
                    Ok((tree.balance(root), inserted))
                },
            }
        }

        let (root, inserted) = descend(self, self.root, value, compare)?;
        self.root = Some(root);
        Ok(inserted)
    }

    pub(super) fn get(
        &self,
        value: &T,
        compare: &mut impl FnMut(&T, &T) -> Result<Ordering>,
    ) -> Result<Option<&T>> {
        let mut cursor = self.root;
        while let Some(index) = cursor {
            match compare(value, &self.nodes[index].value)? {
                Ordering::Equal => return Ok(Some(&self.nodes[index].value)),
                Ordering::Less => cursor = self.nodes[index].left,
                Ordering::Greater => cursor = self.nodes[index].right,
            }
        }
        Ok(None)
    }

    pub(super) fn in_order_indices(&self) -> Vec<usize> {
        let mut ordered = Vec::with_capacity(self.nodes.len());
        let mut stack = Vec::with_capacity(self.height(self.root));
        let mut current = self.root;
        loop {
            while let Some(node) = current {
                stack.push(node);
                current = self.nodes[node].left;
            }
            let Some(node) = stack.pop() else {
                break;
            };
            ordered.push(node);
            current = self.nodes[node].right;
        }
        ordered
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
    pub edge: SourceEdgeId,
    pub at_end: bool,
}

#[derive(Clone, Debug)]
pub(super) struct ExactSourceRing {
    pub id: SourceRingId,
    pub polygon: SourcePolygonId,
    pub hole: bool,
    pub kind: RingKind,
    pub summary: RingSummary,
    pub axis_band_winding: Option<RingWinding>,
    pub source_edges: Range<usize>,
}

impl ExactSourceRing {
    /// Full-longitude traversals and explicit axis bands can enclose spherical
    /// area even when their longitude/latitude projection has zero shoelace.
    pub(super) const fn has_areal_winding(&self) -> bool {
        !matches!(self.summary.domain, RingDomain::SeamOnly)
            && (matches!(self.kind, RingKind::Winding)
                || self.summary.seam_transits != 0
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
        let mut inventory = Self::new(frame, views.len());
        for (row, view) in views.enumerate() {
            if view.frame() != frame {
                return Err(GeometryErrorKind::Invalid(
                    "exact geographic source views do not share one frame".into(),
                )
                .into());
            }
            let row = SourceRowId(u32::try_from(row).map_err(|_| ResourceError::SizeOverflow)?);
            inventory.append_shape(view.shape(), row)?;
        }
        Ok(inventory)
    }

    const fn new(frame: GeographicFrame, row_count: usize) -> Self {
        Self {
            frame,
            row_count,
            edges: Vec::new(),
            pieces: Vec::new(),
            rings: Vec::new(),
            polygons: Vec::new(),
            points: Vec::new(),
            line_boundaries: Vec::new(),
        }
    }

    fn append_shape(&mut self, shape: &Shape, row: SourceRowId) -> Result<()> {
        shape.try_for_each_component(&mut |source| match source {
            Component::Point(point) => {
                self.points.push(ExactSourcePoint { point, row });
                Ok(())
            },
            Component::LineString(line) => self.push_line(line.as_coords(), row),
            Component::Polygon(polygon) => self.push_polygon(polygon, row),
        })
    }

    fn push_line(
        &mut self,
        coordinates: &crate::geometry::CoordSeq,
        row: SourceRowId,
    ) -> Result<()> {
        self.reserve_source_edges(coordinates.coord_count().saturating_sub(1))?;
        let piece_start = self.pieces.len();
        let mut first_edge = None;
        let mut last_edge = None;
        for pair in coordinates.segment_pairs() {
            let [start, end] = pair;
            let source = self.push_source_edge(start, end, row, SourceEdgeKind::Line)?;
            let before = self.pieces.len();
            self.push_chart_pieces(source)?;
            if self.pieces.len() != before {
                first_edge.get_or_insert(source);
                last_edge = Some(source);
            }
        }
        if self.pieces.len() == piece_start {
            self.points.reserve(coordinates.coord_count());
            for point in coordinates.iter_coords() {
                self.points.push(ExactSourcePoint { point, row });
            }
        } else if let (Some(first), Some(last)) = (first_edge, last_edge) {
            self.line_boundaries.reserve(2);
            self.line_boundaries.push(ExactLineBoundary {
                row,
                edge: first,
                at_end: false,
            });
            self.line_boundaries.push(ExactLineBoundary {
                row,
                edge: last,
                at_end: true,
            });
        }
        Ok(())
    }

    fn push_polygon(&mut self, polygon: &Polygon, row: SourceRowId) -> Result<()> {
        let polygon_id = SourcePolygonId(
            u32::try_from(self.polygons.len()).map_err(|_| ResourceError::SizeOverflow)?,
        );
        let edge_count = polygon.rings().try_fold(0_usize, |count, ring| {
            count
                .checked_add(ring.coord_count().saturating_sub(1))
                .ok_or(ResourceError::SizeOverflow)
        })?;
        self.reserve_source_edges(edge_count)?;
        self.polygons.reserve(1);
        self.rings.reserve(polygon.holes.len().saturating_add(1));
        let first_ring = self.rings.len();
        self.polygons.push(ExactSourcePolygon {
            row,
            rings: first_ring..first_ring,
        });
        for (ring_index, ring) in polygon.rings().enumerate() {
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
            let axis_band_winding = summary
                .exact_axis_band
                .then(|| crate::geometry::ring_winding(ring));
            let first_edge = self.edges.len();
            for [start, end] in ring.segment_pairs() {
                let source = self.push_source_edge(start, end, row, SourceEdgeKind::Ring {
                    polygon: polygon_id,
                    ring: ring_id,
                    hole: ring_index != 0,
                    kind,
                })?;
                self.push_chart_pieces(source)?;
            }
            self.rings.push(ExactSourceRing {
                id: ring_id,
                polygon: polygon_id,
                hole: ring_index != 0,
                kind,
                summary,
                axis_band_winding,
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

    fn reserve_source_edges(&mut self, count: usize) -> Result<()> {
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
        self.edges.reserve(count);
        let additional_pieces = piece_capacity
            .checked_sub(self.pieces.len())
            .expect("piece capacity includes the existing chart pieces");
        self.pieces.reserve(additional_pieces);
        Ok(())
    }

    fn push_source_edge(
        &mut self,
        start: Point,
        end: Point,
        row: SourceRowId,
        kind: SourceEdgeKind,
    ) -> Result<SourceEdgeId> {
        let id =
            SourceEdgeId(u32::try_from(self.edges.len()).map_err(|_| ResourceError::SizeOverflow)?);
        self.edges.push(ExactSourceEdge {
            id,
            source: [start, end],
            row,
            kind,
        });
        Ok(id)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "SourceEdgeId is issued by push_source_edge for this inventory's edge vector"
    )]
    fn push_chart_pieces(&mut self, source: SourceEdgeId) -> Result<()> {
        let edge = self.edges[source.0 as usize];
        let [start, end] = edge.source;
        let (start_x, delta_x) = local_longitude_edge(self.frame, start.x, end.x)?;
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
            self.push_chart_piece(
                source,
                ExactChartPoint::from_dyadics(start_x, start_y),
                seam,
                zero,
                parameter.clone(),
            )?;
            let end_x =
                end_x.try_subtract(ExactDyadic::from_f64(self.frame.longitude().period()))?;
            self.push_chart_piece(
                source,
                ExactChartPoint::from_rationals(ExactRational::from_dyadic(negative_half), seam_y),
                ExactChartPoint::from_dyadics(end_x, end_y),
                parameter,
                one,
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
            self.push_chart_piece(
                source,
                ExactChartPoint::from_dyadics(start_x, start_y),
                seam,
                zero,
                parameter.clone(),
            )?;
            let end_x = end_x.try_add(ExactDyadic::from_f64(self.frame.longitude().period()))?;
            self.push_chart_piece(
                source,
                ExactChartPoint::from_rationals(ExactRational::from_dyadic(half), seam_y),
                ExactChartPoint::from_dyadics(end_x, end_y),
                parameter,
                one,
            )?;
        } else {
            self.push_chart_piece(
                source,
                ExactChartPoint::from_dyadics(start_x, start_y),
                ExactChartPoint::from_dyadics(end_x, end_y),
                zero,
                one,
            )?;
        }
        Ok(())
    }

    fn push_chart_piece(
        &mut self,
        source_edge: SourceEdgeId,
        start: ExactChartPoint,
        end: ExactChartPoint,
        source_start: ExactRational,
        source_end: ExactRational,
    ) -> Result<()> {
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
        });
        Ok(())
    }

    #[expect(
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        reason = "piece, port, quotient-node, and atom IDs index dense columns emitted by this noder"
    )]
    pub(super) fn node(&self, budget: &mut ExpansionBudget) -> Result<NodedExactChart> {
        let cut_capacity = self
            .pieces
            .len()
            .checked_mul(2)
            .ok_or(ResourceError::SizeOverflow)?;
        let mut cuts = FallibleAvl::with_capacity(cut_capacity);
        let mut compare_cuts = compare_cut_events;
        for piece in &self.pieces {
            cuts.insert(
                CutEvent {
                    piece: piece.id,
                    parameter: ExactRational::zero(),
                    point: piece.start.clone(),
                },
                &mut compare_cuts,
            )?;
            cuts.insert(
                CutEvent {
                    piece: piece.id,
                    parameter: ExactRational::one(),
                    point: piece.end.clone(),
                },
                &mut compare_cuts,
            )?;
        }
        let source_keys = self.source_quotient_keys()?;
        let node_capacity = source_keys
            .len()
            .checked_add(self.pieces.len())
            .ok_or(ResourceError::SizeOverflow)?;
        let mut constructed_nodes = FallibleAvl::with_capacity(node_capacity);
        let mut compare_keys = compare_quotient_keys;
        for key in source_keys {
            constructed_nodes.insert(key, &mut compare_keys)?;
        }
        for piece in &self.pieces {
            if !piece.source_range.start.is_zero() {
                let key = quotient_key(self.frame, &piece.start)?;
                if constructed_nodes.insert(key, &mut compare_keys)? {
                    budget.add(1)?;
                }
            }
            if !piece.source_range.end.is_one() {
                let key = quotient_key(self.frame, &piece.end)?;
                if constructed_nodes.insert(key, &mut compare_keys)? {
                    budget.add(1)?;
                }
            }
        }
        let mut proxy_piece_ids = Vec::with_capacity(self.pieces.len());
        let mut proxies = Vec::with_capacity(self.pieces.len());
        let mut proxyable = Vec::with_capacity(self.pieces.len());
        for piece in &self.pieces {
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
            if proxyable[left] {
                continue;
            }
            for (right, &is_proxyable) in proxyable.iter().enumerate().take(left) {
                if is_proxyable {
                    self.refine_piece_pair(
                        right,
                        left,
                        &mut cuts,
                        &mut constructed_nodes,
                        &mut compare_cuts,
                        &mut compare_keys,
                        budget,
                    )?;
                }
            }
            for right in left + 1..self.pieces.len() {
                self.refine_piece_pair(
                    left,
                    right,
                    &mut cuts,
                    &mut constructed_nodes,
                    &mut compare_cuts,
                    &mut compare_keys,
                    budget,
                )?;
            }
        }
        self.refine_seam_aliases(
            &mut cuts,
            &mut constructed_nodes,
            &mut compare_cuts,
            &mut compare_keys,
            budget,
        )?;
        self.refine_source_points(
            &mut cuts,
            &mut compare_cuts,
            &proxy_piece_ids,
            &proxies,
            &proxyable,
        )?;

        let cut_order = cuts.in_order_indices();
        let mut occurrences = Vec::with_capacity(self.pieces.len());
        let mut previous_piece = None;
        let mut previous_cut: Option<usize> = None;
        for cut_index in cut_order {
            let cut = &cuts.nodes[cut_index].value;
            if previous_piece != Some(cut.piece) {
                previous_piece = Some(cut.piece);
                previous_cut = None;
            }
            if let Some(start_index) = previous_cut {
                let start = &cuts.nodes[start_index].value;
                if !start.point.same_position(&cut.point)? {
                    let piece = &self.pieces[cut.piece.0 as usize];
                    occurrences.push(AtomOccurrence {
                        start: start.point.clone(),
                        end: cut.point.clone(),
                        source_edge: piece.source_edge,
                        source_range: piece
                            .source_range
                            .compose(&start.parameter, &cut.parameter)?,
                    });
                }
            }
            previous_cut = Some(cut_index);
        }

        let mut port_points = Vec::with_capacity(occurrences.len().saturating_mul(2));
        port_points.extend(
            occurrences
                .iter()
                .flat_map(|occurrence| [occurrence.start.clone(), occurrence.end.clone()]),
        );
        for edge in &self.edges {
            for point in edge.source {
                port_points.push(source_chart_point(self.frame, point));
            }
        }
        for source in &self.points {
            port_points.push(source_chart_point(self.frame, source.point));
        }
        try_sort_by(&mut port_points, ExactChartPoint::compare_lex)?;
        let mut unique_ports: Vec<ExactChartPoint> = Vec::with_capacity(port_points.len());
        for point in port_points {
            if let Some(previous) = unique_ports.last()
                && previous.compare_lex(&point)?.is_eq()
            {
                continue;
            }
            unique_ports.push(point);
        }
        let mut ports: Vec<_> = unique_ports
            .into_iter()
            .enumerate()
            .map(|(index, point)| {
                Ok(ChartPort {
                    id: ChartPortId(u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?),
                    point,
                    node: QuotientNodeId(0),
                })
            })
            .collect::<Result<_>>()?;

        let mut source_edge_ports = Vec::with_capacity(self.edges.len());
        for edge in &self.edges {
            source_edge_ports.push([
                find_chart_port(&ports, &source_chart_point(self.frame, edge.source[0]))?,
                find_chart_port(&ports, &source_chart_point(self.frame, edge.source[1]))?,
            ]);
        }
        let mut source_point_ports = Vec::with_capacity(self.points.len());
        for point in &self.points {
            source_point_ports.push(find_chart_port(
                &ports,
                &source_chart_point(self.frame, point.point),
            )?);
        }

        let mut quotient_keys = Vec::with_capacity(ports.len());
        for port in &ports {
            quotient_keys.push((quotient_key(self.frame, &port.point)?, port.id));
        }
        try_sort_by(&mut quotient_keys, |left, right| {
            compare_quotient_keys(&left.0, &right.0)
                .map(|order| order.then_with(|| left.1.cmp(&right.1)))
        })?;
        let mut nodes = Vec::new();
        let mut node_ports = Vec::with_capacity(ports.len());
        let mut port_cursor = 0;
        while port_cursor < quotient_keys.len() {
            let node_id = QuotientNodeId(
                u32::try_from(nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
            );
            let start = node_ports.len();
            let key = quotient_keys[port_cursor].0.clone();
            while port_cursor < quotient_keys.len()
                && compare_quotient_keys(&key, &quotient_keys[port_cursor].0)?.is_eq()
            {
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

        let mut atom_occurrences = Vec::with_capacity(occurrences.len());
        for occurrence in occurrences {
            let start = find_chart_port(&ports, &occurrence.start)?;
            let end = find_chart_port(&ports, &occurrence.end)?;
            let (ports, reversed) = if start <= end {
                ([start, end], false)
            } else {
                ([end, start], true)
            };
            atom_occurrences.push((
                ports,
                occurrence.source_edge,
                occurrence.source_range,
                reversed,
            ));
        }
        try_sort_by(&mut atom_occurrences, |left, right| {
            let endpoints = left.0.cmp(&right.0);
            if !endpoints.is_eq() {
                return Ok(endpoints);
            }
            let source = left.1.cmp(&right.1);
            if !source.is_eq() {
                return Ok(source);
            }
            let start = left.2.start.try_cmp(&right.2.start)?;
            Ok(if start.is_eq() {
                left.2.end.try_cmp(&right.2.end)?
            } else {
                start
            })
        })?;
        let mut atoms = Vec::new();
        let mut cursor = 0;
        while cursor < atom_occurrences.len() {
            let endpoints = atom_occurrences[cursor].0;
            let mut owners = Vec::new();
            while cursor < atom_occurrences.len() && atom_occurrences[cursor].0 == endpoints {
                let (_, source_edge, source_range, reversed) = &atom_occurrences[cursor];
                owners.push(ExactSpanOwner {
                    source_edge: *source_edge,
                    source_range: source_range.clone(),
                    reversed: *reversed,
                });
                cursor += 1;
            }
            atoms.push(ExactChartAtom {
                ports: endpoints,
                owners: owners.into_boxed_slice(),
            });
        }

        let distinct_sources = self.distinct_source_piece_count()?;
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

    fn source_quotient_keys(&self) -> Result<Vec<ExactQuotientKey>> {
        let source_endpoints = self
            .edges
            .len()
            .checked_mul(2)
            .and_then(|count| count.checked_add(self.points.len()))
            .ok_or(ResourceError::SizeOverflow)?;
        let mut keys = Vec::with_capacity(source_endpoints);
        for edge in &self.edges {
            for point in edge.source {
                keys.push(quotient_key(
                    self.frame,
                    &source_chart_point(self.frame, point),
                )?);
            }
        }
        for point in &self.points {
            keys.push(quotient_key(
                self.frame,
                &source_chart_point(self.frame, point.point),
            )?);
        }
        try_sort_by(&mut keys, compare_quotient_keys)?;
        let mut unique = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(previous) = unique.last()
                && compare_quotient_keys(previous, &key)?.is_eq()
            {
                continue;
            }
            unique.push(key);
        }
        Ok(unique)
    }

    fn distinct_source_piece_count(&self) -> Result<usize> {
        let mut endpoints = Vec::with_capacity(self.pieces.len());
        for piece in &self.pieces {
            let (start, end) = if piece.start.compare_lex(&piece.end)?.is_le() {
                (piece.start.clone(), piece.end.clone())
            } else {
                (piece.end.clone(), piece.start.clone())
            };
            endpoints.push((start, end));
        }
        try_sort_by(&mut endpoints, |left, right| {
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
    fn refine_seam_aliases(
        &self,
        cuts: &mut FallibleAvl<CutEvent>,
        constructed_nodes: &mut FallibleAvl<ExactQuotientKey>,
        compare_cuts: &mut impl FnMut(&CutEvent, &CutEvent) -> Result<Ordering>,
        compare_keys: &mut impl FnMut(&ExactQuotientKey, &ExactQuotientKey) -> Result<Ordering>,
        budget: &mut ExpansionBudget,
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
                    )?;
                }
            }
        }
        for &left in &positive_proxy_ids {
            for &right in &negative_unproxyable {
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
                    )?;
                }
            }
        }
        Ok(())
    }

    fn refine_piece_pair_shifted(
        &self,
        left: usize,
        right: usize,
        shift: &ExactRational,
        cuts: &mut FallibleAvl<CutEvent>,
        constructed_nodes: &mut FallibleAvl<ExactQuotientKey>,
        compare_cuts: &mut impl FnMut(&CutEvent, &CutEvent) -> Result<Ordering>,
        compare_keys: &mut impl FnMut(&ExactQuotientKey, &ExactQuotientKey) -> Result<Ordering>,
        budget: &mut ExpansionBudget,
    ) -> Result<()> {
        let left_piece = self.pieces.get(left).ok_or_else(|| {
            GeometryErrorKind::Invalid("seam pair references an unknown left piece".into())
        })?;
        let right_piece = self.pieces.get(right).ok_or_else(|| {
            GeometryErrorKind::Invalid("seam pair references an unknown right piece".into())
        })?;
        let shifted_right = right_piece.shifted_x(shift)?;
        exact_piece_contacts(left_piece, &shifted_right, |point, left_t, right_t| {
            let right_point =
                ExactChartPoint::from_rationals(point.x.try_subtract(shift)?, point.y.clone());
            let key = quotient_key(self.frame, &point)?;
            if constructed_nodes.insert(key, compare_keys)? {
                budget.add(1)?;
            }
            insert_cut_event(cuts, left_piece, left_t, point, compare_cuts)?;
            insert_cut_event(cuts, right_piece, right_t, right_point, compare_cuts)
        })
    }

    fn refine_source_points(
        &self,
        cuts: &mut FallibleAvl<CutEvent>,
        compare_cuts: &mut impl FnMut(&CutEvent, &CutEvent) -> Result<Ordering>,
        proxy_piece_ids: &[usize],
        proxies: &[Segment],
        proxyable: &[bool],
    ) -> Result<()> {
        if self.points.is_empty() || self.pieces.is_empty() {
            return Ok(());
        }
        let axis = self.frame.longitude();
        let half = ExactRational::from_f64(axis.half_period());
        let negative_half = ExactRational::from_f64(-axis.half_period());
        let mut point_coordinates = Vec::with_capacity(self.points.len());
        for source in &self.points {
            let point = source_chart_point(self.frame, source.point);
            if point.x.try_cmp(&half)?.is_eq() || point.x.try_cmp(&negative_half)?.is_eq() {
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
        try_sort_by(&mut point_coordinates, ExactChartPoint::compare_lex)?;
        let mut unique_points: Vec<ExactChartPoint> = Vec::with_capacity(point_coordinates.len());
        for point in point_coordinates {
            if let Some(previous) = unique_points.last()
                && previous.same_position(&point)?
            {
                continue;
            }
            unique_points.push(point);
        }

        let mut point_bounds = Vec::with_capacity(unique_points.len());
        let mut proxy_points = Vec::with_capacity(unique_points.len());
        let mut unproxyable_points = Vec::new();
        for point in unique_points {
            let [min_x, min_y, max_x, max_y] = point.outward_bounds()?;
            if [min_x, min_y, max_x, max_y].into_iter().all(f64::is_finite) {
                point_bounds.push(Bounds::new_raw(min_x, min_y, max_x, max_y));
                proxy_points.push(point);
            } else {
                unproxyable_points.push(point);
            }
        }

        let mut piece_bounds = Vec::with_capacity(proxies.len());
        for proxy in proxies {
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
                    match self.refine_piece_point(piece_id, point, cuts, compare_cuts) {
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
                self.refine_piece_point(piece_index, point, cuts, compare_cuts)?;
            }
        }
        for (piece_index, &is_proxyable) in proxyable.iter().enumerate() {
            if !is_proxyable {
                for point in &proxy_points {
                    self.refine_piece_point(piece_index, point, cuts, compare_cuts)?;
                }
            }
        }
        Ok(())
    }

    fn refine_piece_point(
        &self,
        piece_index: usize,
        point: &ExactChartPoint,
        cuts: &mut FallibleAvl<CutEvent>,
        compare_cuts: &mut impl FnMut(&CutEvent, &CutEvent) -> Result<Ordering>,
    ) -> Result<()> {
        let piece = self.pieces.get(piece_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q point contact has an unknown chart piece".into())
        })?;
        if point_on_exact_segment(point, &piece.start, &piece.end)? {
            let parameter = parameter_on_exact_segment(point, &piece.start, &piece.end)?;
            insert_cut_event(cuts, piece, parameter, point.clone(), compare_cuts)?;
        }
        Ok(())
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "piece indices are generated from the admitted exact chart-piece slice"
    )]
    fn refine_piece_pair(
        &self,
        left: usize,
        right: usize,
        cuts: &mut FallibleAvl<CutEvent>,
        constructed_nodes: &mut FallibleAvl<ExactQuotientKey>,
        compare_cuts: &mut impl FnMut(&CutEvent, &CutEvent) -> Result<Ordering>,
        compare_keys: &mut impl FnMut(&ExactQuotientKey, &ExactQuotientKey) -> Result<Ordering>,
        budget: &mut ExpansionBudget,
    ) -> Result<()> {
        let a = &self.pieces[left];
        let b = &self.pieces[right];
        exact_piece_contacts(a, b, |point, left_t, right_t| {
            let key = quotient_key(self.frame, &point)?;
            if constructed_nodes.insert(key, compare_keys)? {
                budget.add(1)?;
            }
            insert_cut_event(cuts, a, left_t, point.clone(), compare_cuts)?;
            insert_cut_event(cuts, b, right_t, point, compare_cuts)?;
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

fn local_longitude_edge(frame: GeographicFrame, from: f64, to: f64) -> Result<(f64, ExactDyadic)> {
    let axis = frame.longitude();
    let from_canonical = axis
        .canonicalize(from)
        .expect("source longitude was admitted")
        .get();
    let to_canonical = axis
        .canonicalize(to)
        .expect("source longitude was admitted")
        .get();
    if let Some(full_turn) = axis.full_turn_delta(from, to) {
        return Ok((
            seam_start_spelling(axis, from, from_canonical),
            ExactDyadic::from_f64(full_turn),
        ));
    }
    let direction = axis
        .shortest_edge(from, to)
        .expect("source longitude was admitted");
    let from_exact = ExactDyadic::from_f64(from_canonical);
    let to_exact = ExactDyadic::from_f64(to_canonical);
    let period = ExactDyadic::from_f64(axis.period());
    let half = ExactDyadic::from_f64(axis.half_period());
    let raw_delta = to_exact.try_subtract_ref(&from_exact)?;
    let negative_half = half.clone().neg();
    let delta = match direction {
        EdgeDirection::HalfTurn(TurnDirection::Negative) => negative_half,
        EdgeDirection::HalfTurn(TurnDirection::Positive) => half,
        EdgeDirection::Minor(_) if raw_delta.cmp(&half).is_gt() => {
            raw_delta.try_subtract(period)?
        },
        EdgeDirection::Minor(_) if raw_delta.cmp(&negative_half).is_lt() => {
            raw_delta.try_add(period)?
        },
        EdgeDirection::Minor(_) => raw_delta,
    };
    Ok((seam_start_spelling(axis, from, from_canonical), delta))
}

fn seam_start_spelling(axis: crate::geographic::PeriodicAxis, source: f64, canonical: f64) -> f64 {
    let identity = axis
        .canonicalize(source)
        .expect("source longitude was admitted")
        .identity(axis);
    if identity == SeamIdentity::Seam && source.is_sign_positive() {
        axis.half_period()
    } else {
        canonical
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geographic::AngularFactor;
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
            inventory.rings[0].summary.turn,
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
            inventory.rings[0].summary.turn,
            crate::geographic::RingTurn::Other
        );
        assert_eq!(inventory.rings[0].summary.seam_transits, 2);
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
