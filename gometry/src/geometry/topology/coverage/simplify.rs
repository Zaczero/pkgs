#![expect(
    dead_code,
    reason = "the geographic coverage output stage consumes these guarded chain plans"
)]

use std::collections::BTreeSet;
use std::ops::Range;

use rstar::primitives::{GeomWithData, Rectangle};
use rstar::{AABB, RTree};

use super::super::ExactChartPoint;
use super::super::exact::{
    ExactSourceRange, SourceEdgeId, SourceRowId, point_on_exact_segment_with_work,
    vector_cross_with_work, vector_difference_with_work,
};
use super::super::q::{ExactGeographicTopology, QNodeId, QPortId, QSpatialItem};
use super::GeographicCoverageAnalysis;
use super::chains::{
    GeographicSharedChainInventory, GeographicSharedChainRootUse, GeographicSharedChainUse,
};
use crate::collections::{HashMap, try_hash_map_with_capacity};
use crate::error::{ResourceError, Result};
use crate::geographic::{ChartPole, ChartSpanGuard};
use crate::geometry::embedding_search::Work;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::segments::try_sort_by_with_work;
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};
use crate::geometry::{GeometryErrorKind, SimplifyMethod, XY, simplify_chain_kept_with_try_remove};

type GeneratedEdgeIndex = RTree<GeomWithData<Rectangle<[f64; 2]>, (usize, i8)>>;

pub(in crate::geometry::topology) struct GeographicCoverageChainSimplification<'q> {
    pub(in crate::geometry::topology) inventory: GeographicSharedChainInventory<'q>,
    pub(in crate::geometry::topology) chains: Vec<GeographicSimplifiedChain>,
}

pub(in crate::geometry::topology) struct GeographicSimplifiedChain {
    pub(in crate::geometry::topology) closed: bool,
    pub(in crate::geometry::topology) vertices: Vec<GeographicSimplifiedVertex>,
    pub(in crate::geometry::topology) edges: Vec<GeographicSimplifiedEdge>,
}

pub(in crate::geometry::topology) struct GeographicSimplifiedVertex {
    pub(in crate::geometry::topology) source_vertex_index: usize,
    pub(in crate::geometry::topology) node: QNodeId,
    pub(in crate::geometry::topology) port: QPortId,
    pub(in crate::geometry::topology) point: ExactChartPoint,
}

pub(in crate::geometry::topology) struct GeographicSimplifiedEdge {
    pub(in crate::geometry::topology) from_vertex_index: usize,
    pub(in crate::geometry::topology) to_vertex_index: usize,
    /// The original Q spans covered by this retained edge or generated chord.
    pub(in crate::geometry::topology) source_span_range: Range<usize>,
    pub(in crate::geometry::topology) generated: bool,
    /// One exact source-root composition for each D1 row-ring use.
    pub(in crate::geometry::topology) row_uses: Vec<GeographicSimplifiedEdgeUse>,
}

pub(in crate::geometry::topology) struct GeographicSimplifiedEdgeUse {
    pub(in crate::geometry::topology) row_use_index: usize,
    pub(in crate::geometry::topology) roots: Vec<GeographicComposedRoot>,
}

pub(in crate::geometry::topology) struct GeographicComposedRoot {
    pub(in crate::geometry::topology) source_edge: SourceEdgeId,
    pub(in crate::geometry::topology) source_range: ExactSourceRange,
    /// Period shift placing this original root in the chain's continuous lift.
    pub(in crate::geometry::topology) lift: i64,
    pub(in crate::geometry::topology) owner_refs: Vec<GeographicSharedChainRootUse>,
}

struct GeographicChainSequence {
    points: Vec<ExactChartPoint>,
    nodes: Vec<QNodeId>,
    ports: Vec<QPortId>,
    lifts: Vec<i64>,
    span_lifts: Vec<i64>,
}

struct GeographicRemovalCandidate<'a> {
    chain_index: usize,
    before: usize,
    vertex: usize,
    after: usize,
    sequence: &'a GeographicChainSequence,
    rows: &'a [SourceRowId],
}

struct RootFragment {
    source_edge: SourceEdgeId,
    source_range: ExactSourceRange,
    lift: i64,
    chain_span_index: usize,
    owner_ref: (usize, usize),
}

impl GeographicCoverageAnalysis {
    pub(in crate::geometry::topology) fn simplify_geographic_chains(
        &self,
        tolerance: f64,
        method: SimplifyMethod,
        simplify_boundary: bool,
        work: &mut impl Work,
    ) -> Result<GeographicCoverageChainSimplification<'_>> {
        if !(tolerance.is_finite() && tolerance >= 0.0) {
            return Err(GeometryErrorKind::NonNegativeFinite("tolerance", tolerance).into());
        }
        let inventory = self.shared_chain_inventory(work)?;
        let topology = inventory.topology;
        let mut guard = GeographicQGuard::new(self, &inventory, work)?;
        let mut simplified = Vec::with_capacity(inventory.chains.len());
        let period = ExactRational::from_f64(topology.sources.frame.longitude().period());

        for (chain_index, chain) in inventory.chains.iter().enumerate() {
            work.charge_into_error(1)?;
            guard.begin_chain(chain_index, work)?;
            let mut canonical_sequence: Option<GeographicChainSequence> = None;
            let mut row_use_span_lifts = Vec::with_capacity(chain.row_uses.len());
            let mut longitude_columns = Vec::with_capacity(chain.row_uses.len());
            for use_index in 0..chain.row_uses.len() {
                let mut sequence = build_chain_sequence(&inventory, chain_index, use_index, work)?;
                if let Some(canonical) = &canonical_sequence {
                    align_chain_sequence(&mut sequence, canonical, &period, work)?;
                    chain_sequences_match(canonical, &sequence, work)?;
                }
                work.charge_into_error(
                    u64::try_from(sequence.span_lifts.len())
                        .map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                row_use_span_lifts.push(sequence.span_lifts.clone());
                let mut longitudes = Vec::with_capacity(sequence.points.len());
                for point in &sequence.points {
                    work.charge_into_error(1)?;
                    longitudes.push(point.x.round_with_work(work)?);
                }
                longitude_columns.push(longitudes);
                if canonical_sequence.is_none() {
                    canonical_sequence = Some(sequence);
                }
            }
            let canonical = canonical_sequence.as_ref().ok_or_else(|| {
                GeometryErrorKind::Invalid("geographic shared chain has no row use".into())
            })?;
            let xy = exact_points_to_xy(&canonical.points, work)?;
            let rows = chain_rows(&chain.row_uses, work)?;
            let shared = rows.len() > 1;
            let chart_guards = longitude_columns
                .iter()
                .map(|longitudes| {
                    ChartSpanGuard::new(longitudes, topology.sources.frame.longitude())
                })
                .collect::<Vec<_>>();

            let kept = if !simplify_boundary && !shared {
                work.charge_into_error(
                    u64::try_from(xy.len()).map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                (0..xy.len()).collect()
            } else {
                let mut generated_edges = vec![None; xy.len()];
                simplify_chain_kept_with_try_remove(
                    xy.len(),
                    |index| {
                        *xy.get(index)
                            .expect("the chain simplifier queries an admitted vertex index")
                    },
                    chain.closed,
                    tolerance,
                    method,
                    |before, vertex, after, _previous, _point, _next| {
                        if chart_guards
                            .iter()
                            .any(|chart_guard| chart_guard.rejects_span(before, after))
                        {
                            return Ok(false);
                        }
                        guard.try_remove(
                            &GeographicRemovalCandidate {
                                chain_index,
                                before,
                                vertex,
                                after,
                                sequence: canonical,
                                rows: &rows,
                            },
                            &mut generated_edges,
                            work,
                        )
                    },
                )?
            };
            simplified.push(build_simplified_chain(
                &inventory,
                chain_index,
                chain.closed,
                canonical,
                &kept,
                &row_use_span_lifts,
                work,
            )?);
        }

        Ok(GeographicCoverageChainSimplification {
            inventory,
            chains: simplified,
        })
    }
}

fn exact_lifted_port(
    topology: &ExactGeographicTopology,
    port: QPortId,
    lift: i64,
    period: &ExactRational,
    work: &mut impl Work,
) -> Result<ExactChartPoint> {
    let port = topology.q.ports.get(port.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("geographic chain references an unknown Q port".into())
    })?;
    let lift = ExactRational::from_dyadic(ExactDyadic::from_i64(lift));
    let shift = period.try_product_with_work(&lift, work)?;
    Ok(ExactChartPoint::from_rationals(
        port.point.x.try_add_with_work(&shift, work)?,
        port.point.y.try_clone_with_work(work)?,
    ))
}

fn representative_root_indices(
    row_use: &GeographicSharedChainUse,
    span_count: usize,
    work: &mut impl Work,
) -> Result<Vec<usize>> {
    let mut indices = Vec::with_capacity(span_count);
    let mut cursor = 0_usize;
    for span_index in 0..span_count {
        work.charge_into_error(1)?;
        let root_index = cursor;
        let root = row_use.root_uses.get(cursor).ok_or_else(|| {
            GeometryErrorKind::Invalid("geographic row use omits a chain span".into())
        })?;
        if root.chain_span_index != span_index {
            return Err(GeometryErrorKind::Invalid(
                "geographic row use span roots are not complete and ordered".into(),
            )
            .into());
        }
        while row_use
            .root_uses
            .get(cursor)
            .is_some_and(|root| root.chain_span_index == span_index)
        {
            work.charge_into_error(1)?;
            cursor = cursor.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        }
        indices.push(root_index);
    }
    if cursor != row_use.root_uses.len() {
        return Err(GeometryErrorKind::Invalid(
            "geographic row use has roots outside its chain".into(),
        )
        .into());
    }
    Ok(indices)
}

fn build_chain_sequence(
    inventory: &GeographicSharedChainInventory<'_>,
    chain_index: usize,
    use_index: usize,
    work: &mut impl Work,
) -> Result<GeographicChainSequence> {
    let chain = inventory.chains.get(chain_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("geographic chain index is out of range".into())
    })?;
    let row_use = chain.row_uses.get(use_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("geographic chain row use is out of range".into())
    })?;
    let roots = representative_root_indices(row_use, chain.spans.len(), work)?;
    let period = ExactRational::from_f64(inventory.topology.sources.frame.longitude().period());
    let mut points: Vec<ExactChartPoint> = Vec::with_capacity(chain.spans.len().wrapping_add(1));
    let mut nodes = Vec::with_capacity(chain.spans.len().wrapping_add(1));
    let mut ports = Vec::with_capacity(chain.spans.len().wrapping_add(1));
    let mut lifts = Vec::with_capacity(chain.spans.len().wrapping_add(1));
    let mut span_lifts = Vec::with_capacity(chain.spans.len());

    for (span_index, span) in chain.spans.iter().enumerate() {
        work.charge_into_error(1)?;
        let root_index = *roots.get(span_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("geographic chain has no representative source root".into())
        })?;
        let owner = inventory.source_owner(chain_index, use_index, root_index)?;
        let mut lift = owner.lift;
        let mut from = exact_lifted_port(inventory.topology, span.from_port, lift, &period, work)?;
        if let Some(previous) = points.last() {
            let shift = exact_longitude_shift(&from, previous, &period, work)?;
            lift = lift.checked_add(shift).ok_or(ResourceError::SizeOverflow)?;
            from = exact_lifted_port(inventory.topology, span.from_port, lift, &period, work)?;
            if !previous.same_position(&from)? {
                return Err(GeometryErrorKind::Invalid(
                    "source-root lifts do not join adjacent Q chain spans".into(),
                )
                .into());
            }
        }
        let to = exact_lifted_port(inventory.topology, span.to_port, lift, &period, work)?;
        if points.is_empty() {
            points.push(from);
            nodes.push(span.from_node);
            ports.push(span.from_port);
            lifts.push(lift);
        } else if nodes.last().copied() != Some(span.from_node) {
            return Err(GeometryErrorKind::Invalid(
                "geographic chain Q nodes are not contiguous".into(),
            )
            .into());
        }
        points.push(to);
        nodes.push(span.to_node);
        ports.push(span.to_port);
        lifts.push(lift);
        span_lifts.push(lift);
    }
    Ok(GeographicChainSequence {
        points,
        nodes,
        ports,
        lifts,
        span_lifts,
    })
}

fn exact_longitude_shift(
    source: &ExactChartPoint,
    target: &ExactChartPoint,
    period: &ExactRational,
    work: &mut impl Work,
) -> Result<i64> {
    if !source.y.try_eq_with_work(&target.y, work)? {
        return Err(GeometryErrorKind::Invalid(
            "adjacent geographic chain ports have different latitudes".into(),
        )
        .into());
    }
    let difference = target.x.try_subtract_with_work(&source.x, work)?;
    let periods = difference.try_quotient_with_work(period, work)?;
    if !periods.is_integer() {
        return Err(GeometryErrorKind::Invalid(
            "adjacent geographic chain ports do not share an exact longitude lift".into(),
        )
        .into());
    }
    let rounded = periods.round_with_work(work)?;
    if !rounded.is_finite() || rounded < i64::MIN as f64 || rounded >= -(i64::MIN as f64) {
        return Err(ResourceError::SizeOverflow.into());
    }
    let shift = rounded as i64;
    let exact_shift = ExactRational::from_dyadic(ExactDyadic::from_i64(shift));
    if !periods.try_eq_with_work(&exact_shift, work)? {
        return Err(ResourceError::SizeOverflow.into());
    }
    Ok(shift)
}

fn align_chain_sequence(
    sequence: &mut GeographicChainSequence,
    canonical: &GeographicChainSequence,
    period: &ExactRational,
    work: &mut impl Work,
) -> Result<()> {
    let first = sequence.points.first().ok_or_else(|| {
        GeometryErrorKind::Invalid("geographic chain sequence has no first point".into())
    })?;
    let canonical_first = canonical.points.first().ok_or_else(|| {
        GeometryErrorKind::Invalid("canonical geographic chain has no first point".into())
    })?;
    let shift = exact_longitude_shift(first, canonical_first, period, work)?;
    for point in &mut sequence.points {
        *point = shift_exact_point(point, shift, period, work)?;
    }
    for lift in sequence.lifts.iter_mut().chain(&mut sequence.span_lifts) {
        *lift = lift.checked_add(shift).ok_or(ResourceError::SizeOverflow)?;
    }
    Ok(())
}

fn chain_sequences_match(
    canonical: &GeographicChainSequence,
    other: &GeographicChainSequence,
    work: &mut impl Work,
) -> Result<()> {
    if canonical.nodes != other.nodes
        || canonical.points.len() != other.points.len()
        || canonical.ports.len() != other.ports.len()
    {
        return Err(GeometryErrorKind::Invalid(
            "signed row uses disagree on geographic chain vertices".into(),
        )
        .into());
    }
    for (left, right) in canonical.points.iter().zip(&other.points) {
        work.charge_into_error(1)?;
        if !left.same_position(right)? {
            return Err(GeometryErrorKind::Invalid(
                "signed row uses disagree on the lifted geographic chain".into(),
            )
            .into());
        }
    }
    Ok(())
}

fn exact_points_to_xy(points: &[ExactChartPoint], work: &mut impl Work) -> Result<Vec<XY>> {
    let mut xy = Vec::with_capacity(points.len());
    for point in points {
        work.charge_into_error(1)?;
        xy.push(XY::new(
            point.x.round_with_work(work)?,
            point.y.round_with_work(work)?,
        ));
    }
    Ok(xy)
}

fn chain_rows(uses: &[GeographicSharedChainUse], work: &mut impl Work) -> Result<Vec<SourceRowId>> {
    let mut rows = BTreeSet::new();
    for use_ in uses {
        work.charge_into_error(1)?;
        rows.insert(use_.row);
    }
    Ok(rows.into_iter().collect())
}

fn compose_edge_uses(
    inventory: &GeographicSharedChainInventory<'_>,
    chain_index: usize,
    span_range: Range<usize>,
    row_use_span_lifts: &[Vec<i64>],
    work: &mut impl Work,
) -> Result<Vec<GeographicSimplifiedEdgeUse>> {
    let chain = inventory.chains.get(chain_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("geographic edge chain is out of range".into())
    })?;
    let mut output = Vec::with_capacity(chain.row_uses.len());
    for (use_index, row_use) in chain.row_uses.iter().enumerate() {
        work.charge_into_error(1)?;
        let span_lifts = row_use_span_lifts.get(use_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("geographic row-use lifts are incomplete".into())
        })?;
        let mut fragments = Vec::new();
        for (root_index, root_use) in row_use.root_uses.iter().enumerate() {
            work.charge_into_error(1)?;
            if !span_range.contains(&root_use.chain_span_index) {
                continue;
            }
            let owner = inventory.source_owner(chain_index, use_index, root_index)?;
            let lift = *span_lifts.get(root_use.chain_span_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("geographic row-use lift is out of range".into())
            })?;
            fragments.push(RootFragment {
                source_edge: owner.source_edge,
                source_range: owner.source_range.clone(),
                lift,
                chain_span_index: root_use.chain_span_index,
                owner_ref: (root_use.chain_span_index, root_use.owner_index),
            });
        }
        try_sort_by_with_work(&mut fragments, work, |left, right, _work| {
            Ok(left
                .source_edge
                .cmp(&right.source_edge)
                .then(left.lift.cmp(&right.lift))
                .then(left.chain_span_index.cmp(&right.chain_span_index))
                .then(left.owner_ref.1.cmp(&right.owner_ref.1)))
        })?;
        let source_forward = row_use.source_forward == row_use.signed_forward;
        let mut roots: Vec<GeographicComposedRoot> = Vec::new();
        let mut previous_span_by_root: Vec<usize> = Vec::new();
        for fragment in fragments {
            work.charge_into_error(1)?;
            let previous = roots.last_mut();
            let previous_span = previous_span_by_root.last_mut();
            let mut merged = false;
            if let (Some(previous), Some(previous_span)) = (previous, previous_span)
                && previous.source_edge == fragment.source_edge
                && previous.lift == fragment.lift
                && previous_span
                    .checked_add(1)
                    .is_some_and(|next| next == fragment.chain_span_index)
            {
                let adjacent = if source_forward {
                    previous
                        .source_range
                        .end
                        .try_eq_with_work(&fragment.source_range.start, work)?
                } else {
                    previous
                        .source_range
                        .start
                        .try_eq_with_work(&fragment.source_range.end, work)?
                };
                if adjacent {
                    if source_forward {
                        previous.source_range.end =
                            fragment.source_range.end.try_clone_with_work(work)?;
                    } else {
                        previous.source_range.start =
                            fragment.source_range.start.try_clone_with_work(work)?;
                    }
                    previous.owner_refs.push(GeographicSharedChainRootUse {
                        chain_span_index: fragment.owner_ref.0,
                        owner_index: fragment.owner_ref.1,
                    });
                    *previous_span = fragment.chain_span_index;
                    merged = true;
                }
            }
            if !merged {
                roots.push(GeographicComposedRoot {
                    source_edge: fragment.source_edge,
                    source_range: fragment.source_range,
                    lift: fragment.lift,
                    owner_refs: vec![GeographicSharedChainRootUse {
                        chain_span_index: fragment.owner_ref.0,
                        owner_index: fragment.owner_ref.1,
                    }],
                });
                previous_span_by_root.push(fragment.chain_span_index);
            }
        }
        output.push(GeographicSimplifiedEdgeUse {
            row_use_index: use_index,
            roots,
        });
    }
    Ok(output)
}

fn build_simplified_chain(
    inventory: &GeographicSharedChainInventory<'_>,
    chain_index: usize,
    closed: bool,
    canonical: &GeographicChainSequence,
    kept: &[usize],
    row_use_span_lifts: &[Vec<i64>],
    work: &mut impl Work,
) -> Result<GeographicSimplifiedChain> {
    let source_chain = inventory.chains.get(chain_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("geographic simplified chain is out of range".into())
    })?;
    let mut vertices = Vec::with_capacity(kept.len());
    for &source_vertex_index in kept {
        work.charge_into_error(1)?;
        vertices.push(GeographicSimplifiedVertex {
            source_vertex_index,
            node: *canonical.nodes.get(source_vertex_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("kept geographic chain node is out of range".into())
            })?,
            port: *canonical.ports.get(source_vertex_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("kept geographic chain port is out of range".into())
            })?,
            point: canonical
                .points
                .get(source_vertex_index)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("kept geographic chain point is out of range".into())
                })?
                .try_clone_with_work(work)?,
        });
    }
    let mut edges = Vec::with_capacity(kept.len().saturating_sub(1));
    for pair in kept.windows(2) {
        work.charge_into_error(1)?;
        let [from_vertex_index, to_vertex_index] = pair else {
            return Err(GeometryErrorKind::Invalid(
                "simplified geographic edge has incomplete endpoints".into(),
            )
            .into());
        };
        let source_span_range = *from_vertex_index..*to_vertex_index;
        let generated = source_span_range.len() != 1;
        let row_uses = compose_edge_uses(
            inventory,
            chain_index,
            source_span_range.clone(),
            row_use_span_lifts,
            work,
        )?;
        edges.push(GeographicSimplifiedEdge {
            from_vertex_index: *from_vertex_index,
            to_vertex_index: *to_vertex_index,
            source_span_range,
            generated,
            row_uses,
        });
    }
    if closed && kept.last().copied() != Some(source_chain.spans.len()) {
        return Err(GeometryErrorKind::Invalid(
            "closed geographic chain lost its pinned closure".into(),
        )
        .into());
    }
    Ok(GeographicSimplifiedChain {
        closed,
        vertices,
        edges,
    })
}

struct LiveGeographicChord {
    start_node: QNodeId,
    end_node: QNodeId,
    start: ExactChartPoint,
    end: ExactChartPoint,
    active: bool,
}

struct GeographicQGuard<'a, 'q> {
    analysis: &'a GeographicCoverageAnalysis,
    inventory: &'a GeographicSharedChainInventory<'q>,
    period: ExactRational,
    live_q_spans: Vec<bool>,
    generated: Vec<LiveGeographicChord>,
    generated_index: GeneratedEdgeIndex,
    current_chain_positions: HashMap<usize, usize>,
    current_chain_index: Option<usize>,
    occupied_poles: [bool; 2],
}

impl<'a, 'q> GeographicQGuard<'a, 'q> {
    fn new(
        analysis: &'a GeographicCoverageAnalysis,
        inventory: &'a GeographicSharedChainInventory<'q>,
        work: &mut impl Work,
    ) -> Result<Self> {
        let topology = inventory.topology;
        let frame = topology.sources.frame;
        let period = ExactRational::from_f64(frame.longitude().period());
        let mut occupied_poles = [false; 2];
        for (pole, occupied) in [ChartPole::North, ChartPole::South]
            .into_iter()
            .zip(&mut occupied_poles)
        {
            work.charge_into_error(1)?;
            let point = ExactChartPoint::from_rationals(
                topology.q.cut.try_clone_with_work(work)?,
                ExactRational::from_f64(frame.chart_pole_latitude(pole)),
            );
            *occupied = !topology
                .face_rows_at_chart_point(&point, &analysis.face_rows, work)?
                .is_empty();
        }
        let current_chain_positions = try_hash_map_with_capacity(
            inventory
                .chains
                .iter()
                .map(|chain| chain.spans.len())
                .max()
                .unwrap_or(0),
        )?;
        Ok(Self {
            analysis,
            inventory,
            period,
            live_q_spans: vec![true; topology.q.spans.len()],
            generated: Vec::new(),
            generated_index: RTree::new(),
            current_chain_positions,
            current_chain_index: None,
            occupied_poles,
        })
    }

    fn begin_chain(&mut self, chain_index: usize, work: &mut impl Work) -> Result<()> {
        let chain = self.inventory.chains.get(chain_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("geographic chain index is out of range".into())
        })?;
        self.current_chain_positions.clear();
        self.current_chain_index = Some(chain_index);
        for (position, span) in chain.spans.iter().enumerate() {
            work.charge_into_error(1)?;
            if self
                .current_chain_positions
                .insert(span.span.0 as usize, position)
                .is_some()
            {
                return Err(GeometryErrorKind::Invalid(
                    "a Q span occurs twice in one geographic chain".into(),
                )
                .into());
            }
        }
        Ok(())
    }

    fn try_remove(
        &mut self,
        candidate: &GeographicRemovalCandidate<'_>,
        generated_edges: &mut [Option<usize>],
        work: &mut impl Work,
    ) -> Result<bool> {
        let chain_index = candidate.chain_index;
        let before = candidate.before;
        let vertex = candidate.vertex;
        let after = candidate.after;
        let sequence = candidate.sequence;
        let rows = candidate.rows;
        let points = &sequence.points;
        let nodes = &sequence.nodes;
        let lifts = &sequence.lifts;
        let chain = self.inventory.chains.get(chain_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("geographic chain index is out of range".into())
        })?;
        if before >= vertex || vertex >= after || after >= points.len() {
            return Err(GeometryErrorKind::Invalid(
                "guarded geographic removal has an invalid chain interval".into(),
            )
            .into());
        }
        let start_node = *nodes.get(before).ok_or_else(|| {
            GeometryErrorKind::Invalid("geographic chain start node is missing".into())
        })?;
        let removed_node = *nodes.get(vertex).ok_or_else(|| {
            GeometryErrorKind::Invalid("geographic chain removed node is missing".into())
        })?;
        let end_node = *nodes.get(after).ok_or_else(|| {
            GeometryErrorKind::Invalid("geographic chain end node is missing".into())
        })?;
        let start_point = points.get(before).ok_or_else(|| {
            GeometryErrorKind::Invalid("geographic chain start point is missing".into())
        })?;
        let removed_point = points.get(vertex).ok_or_else(|| {
            GeometryErrorKind::Invalid("geographic chain removed point is missing".into())
        })?;
        let end_point = points.get(after).ok_or_else(|| {
            GeometryErrorKind::Invalid("geographic chain end point is missing".into())
        })?;
        let start_lift = *lifts.get(before).ok_or_else(|| {
            GeometryErrorKind::Invalid("geographic chain start lift is missing".into())
        })?;
        if self.triangle_touches_occupied_pole(start_point, removed_point, end_point, work)? {
            return Ok(false);
        }

        let left_generated = generated_edges.get(before).copied().flatten();
        let right_generated = generated_edges.get(vertex).copied().flatten();
        if left_generated.is_some() && left_generated == right_generated {
            return Err(GeometryErrorKind::Invalid(
                "one live geographic chord occupies both sides of a removable vertex".into(),
            )
            .into());
        }
        let source_lift = start_lift
            .checked_neg()
            .ok_or(ResourceError::SizeOverflow)?;
        let candidate_start = shift_exact_point(start_point, source_lift, &self.period, work)?;
        let candidate_end = shift_exact_point(end_point, source_lift, &self.period, work)?;

        for copy in [-1_i64, 0, 1] {
            let candidate_start_copy =
                shift_exact_point(&candidate_start, copy, &self.period, work)?;
            let candidate_end_copy = shift_exact_point(&candidate_end, copy, &self.period, work)?;
            if !self.q_spans_allow_chord(
                chain_index,
                before,
                after,
                [start_node, end_node],
                &candidate_start_copy,
                &candidate_end_copy,
                work,
            )? {
                return Ok(false);
            }
            if !self.generated_chords_allow_segment(
                [start_node, end_node],
                left_generated,
                right_generated,
                &candidate_start_copy,
                &candidate_end_copy,
                work,
            )? {
                return Ok(false);
            }
            let shift = source_lift
                .checked_add(copy)
                .ok_or(ResourceError::SizeOverflow)?;
            let triangle = [
                shift_exact_point(start_point, shift, &self.period, work)?,
                shift_exact_point(removed_point, shift, &self.period, work)?,
                shift_exact_point(end_point, shift, &self.period, work)?,
            ];
            if !self.q_vertices_allow_triangle(
                chain_index,
                before,
                after,
                [start_node, removed_node, end_node],
                &triangle,
                work,
            )? {
                return Ok(false);
            }
            if !self.generated_chords_allow_triangle(
                left_generated,
                right_generated,
                [start_node, removed_node, end_node],
                &triangle,
                work,
            )? {
                return Ok(false);
            }
        }
        if !self.row_regions_allow_chord(start_point, end_point, rows, start_lift, work)? {
            return Ok(false);
        }

        let chain_spans = chain.spans.get(before..after).ok_or_else(|| {
            GeometryErrorKind::Invalid("geographic chain source-span range is incomplete".into())
        })?;
        let mut rectangles = Vec::with_capacity(3);
        for shift in [-1_i64, 0, 1] {
            let start = shift_exact_point(&candidate_start, shift, &self.period, work)?;
            let end = shift_exact_point(&candidate_end, shift, &self.period, work)?;
            rectangles.push(exact_rectangle(&start, &end)?);
        }
        let charged_spans =
            u64::try_from(chain_spans.len()).map_err(|_| ResourceError::SizeOverflow)?;
        work.charge_into_error(
            charged_spans
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?,
        )?;
        let generated_index = self.generated.len();
        self.generated.push(LiveGeographicChord {
            start_node,
            end_node,
            start: candidate_start,
            end: candidate_end,
            active: true,
        });
        for (copy, rectangle) in [-1_i8, 0, 1].into_iter().zip(rectangles) {
            self.generated_index
                .insert(GeomWithData::new(rectangle, (generated_index, copy)));
        }
        if let Some(edge) = left_generated
            && let Some(edge) = self.generated.get_mut(edge)
        {
            edge.active = false;
        }
        if let Some(edge) = right_generated
            && let Some(edge) = self.generated.get_mut(edge)
        {
            edge.active = false;
        }
        for span in chain_spans {
            let slot = self
                .live_q_spans
                .get_mut(span.span.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("geographic Q live-span table is incomplete".into())
                })?;
            *slot = false;
        }
        let left_slot = generated_edges.get_mut(before).ok_or_else(|| {
            GeometryErrorKind::Invalid("geographic chain edge table is incomplete".into())
        })?;
        *left_slot = Some(generated_index);
        let removed_slot = generated_edges.get_mut(vertex).ok_or_else(|| {
            GeometryErrorKind::Invalid("geographic chain edge table is incomplete".into())
        })?;
        *removed_slot = None;
        Ok(true)
    }

    fn is_replaced_chain_span(
        &self,
        chain_index: usize,
        span_index: usize,
        before: usize,
        after: usize,
    ) -> bool {
        self.current_chain_positions
            .get(&span_index)
            .is_some_and(|&position| position >= before && position < after)
            && self.current_chain_index == Some(chain_index)
    }

    fn q_spans_allow_chord(
        &self,
        chain_index: usize,
        before: usize,
        after: usize,
        endpoints: [QNodeId; 2],
        start: &ExactChartPoint,
        end: &ExactChartPoint,
        work: &mut impl Work,
    ) -> Result<bool> {
        let completed = self
            .inventory
            .topology
            .q
            .for_each_spatial_candidate_with_work(start, end, work, |item, work| {
                let QSpatialItem::Span(span_index) = item else {
                    return Ok(true);
                };
                let is_live = self.live_q_spans.get(span_index).copied().ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q spatial index has an unknown span".into())
                })?;
                if !is_live || self.is_replaced_chain_span(chain_index, span_index, before, after) {
                    return Ok(true);
                }
                let span = self
                    .inventory
                    .topology
                    .q
                    .spans
                    .get(span_index)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q spatial index has an unknown span".into())
                    })?;
                let [start_port, end_port] = span.ports;
                let start_port = self
                    .inventory
                    .topology
                    .q
                    .ports
                    .get(start_port.0 as usize)
                    .ok_or_else(|| GeometryErrorKind::Invalid("Q span has no start port".into()))?;
                let end_port = self
                    .inventory
                    .topology
                    .q
                    .ports
                    .get(end_port.0 as usize)
                    .ok_or_else(|| GeometryErrorKind::Invalid("Q span has no end port".into()))?;
                let intersects = super::super::q::exact_segments_intersect_with_work(
                    start,
                    end,
                    &start_port.point,
                    &end_port.point,
                    work,
                )?;
                if intersects {
                    return intersection_is_endpoint_only(
                        [start, end],
                        endpoints,
                        [&start_port.point, &end_port.point],
                        span.nodes,
                        work,
                    );
                }
                Ok(true)
            })?;
        Ok(completed)
    }

    fn generated_chords_allow_segment(
        &self,
        endpoints: [QNodeId; 2],
        left_generated: Option<usize>,
        right_generated: Option<usize>,
        start: &ExactChartPoint,
        end: &ExactChartPoint,
        work: &mut impl Work,
    ) -> Result<bool> {
        let envelope = exact_aabb(&[start, end])?;
        for entry in self
            .generated_index
            .locate_in_envelope_intersecting(envelope)
        {
            work.charge_into_error(1)?;
            let (edge_index, shift) = entry.data;
            if Some(edge_index) == left_generated || Some(edge_index) == right_generated {
                continue;
            }
            let Some(edge) = self.generated.get(edge_index).filter(|edge| edge.active) else {
                continue;
            };
            let shift = i64::from(shift);
            let other_start = shift_exact_point(&edge.start, shift, &self.period, work)?;
            let other_end = shift_exact_point(&edge.end, shift, &self.period, work)?;
            if super::super::q::exact_segments_intersect_with_work(
                start,
                end,
                &other_start,
                &other_end,
                work,
            )? && !intersection_is_endpoint_only(
                [start, end],
                endpoints,
                [&other_start, &other_end],
                [edge.start_node, edge.end_node],
                work,
            )? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn q_vertices_allow_triangle(
        &self,
        chain_index: usize,
        before: usize,
        after: usize,
        ignored_nodes: [QNodeId; 3],
        triangle: &[ExactChartPoint; 3],
        work: &mut impl Work,
    ) -> Result<bool> {
        let [minimum, maximum] = exact_triangle_bounds(triangle, work)?;
        let completed = self
            .inventory
            .topology
            .q
            .for_each_spatial_candidate_with_work(&minimum, &maximum, work, |item, work| {
                match item {
                    QSpatialItem::Span(span_index) => {
                        let is_live =
                            self.live_q_spans.get(span_index).copied().ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "Q spatial index has an unknown span".into(),
                                )
                            })?;
                        if !is_live
                            || self.is_replaced_chain_span(chain_index, span_index, before, after)
                        {
                            return Ok(true);
                        }
                        let span =
                            self.inventory
                                .topology
                                .q
                                .spans
                                .get(span_index)
                                .ok_or_else(|| {
                                    GeometryErrorKind::Invalid(
                                        "Q spatial index has an unknown span".into(),
                                    )
                                })?;
                        let [q_start_node, q_end_node] = span.nodes;
                        let [q_start_port, q_end_port] = span.ports;
                        let q_start_port = self
                            .inventory
                            .topology
                            .q
                            .ports
                            .get(q_start_port.0 as usize)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "Q span start endpoint port is unknown".into(),
                                )
                            })?;
                        let q_end_port = self
                            .inventory
                            .topology
                            .q
                            .ports
                            .get(q_end_port.0 as usize)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "Q span end endpoint port is unknown".into(),
                                )
                            })?;
                        let [first, second, third] = triangle;
                        let [start_node, middle_node, end_node] = ignored_nodes;
                        for (triangle_edge, triangle_nodes) in [
                            ([first, second], [start_node, middle_node]),
                            ([second, third], [middle_node, end_node]),
                            ([third, first], [end_node, start_node]),
                        ] {
                            let [edge_start, edge_end] = triangle_edge;
                            if super::super::q::exact_segments_intersect_with_work(
                                edge_start,
                                edge_end,
                                &q_start_port.point,
                                &q_end_port.point,
                                work,
                            )? && !intersection_is_endpoint_only(
                                triangle_edge,
                                triangle_nodes,
                                [&q_start_port.point, &q_end_port.point],
                                [q_start_node, q_end_node],
                                work,
                            )? {
                                return Ok(false);
                            }
                        }
                        for (point, node) in [
                            (&q_start_port.point, q_start_node),
                            (&q_end_port.point, q_end_node),
                        ] {
                            if !ignored_nodes.contains(&node)
                                && point_in_exact_triangle(point, triangle, work)?
                            {
                                return Ok(false);
                            }
                        }
                    },
                    QSpatialItem::Point(port_id) => {
                        let port = self
                            .inventory
                            .topology
                            .q
                            .ports
                            .get(port_id.0 as usize)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid("Q spatial point port is unknown".into())
                            })?;
                        if !ignored_nodes.contains(&port.node)
                            && point_in_exact_triangle(&port.point, triangle, work)?
                        {
                            return Ok(false);
                        }
                    },
                }
                Ok(true)
            })?;
        Ok(completed)
    }

    fn generated_chords_allow_triangle(
        &self,
        left_generated: Option<usize>,
        right_generated: Option<usize>,
        ignored_nodes: [QNodeId; 3],
        triangle: &[ExactChartPoint; 3],
        work: &mut impl Work,
    ) -> Result<bool> {
        let [minimum, maximum] = exact_triangle_bounds(triangle, work)?;
        let envelope = exact_aabb(&[&minimum, &maximum])?;
        for entry in self
            .generated_index
            .locate_in_envelope_intersecting(envelope)
        {
            work.charge_into_error(1)?;
            let (edge_index, shift) = entry.data;
            if Some(edge_index) == left_generated || Some(edge_index) == right_generated {
                continue;
            }
            let Some(edge) = self.generated.get(edge_index).filter(|edge| edge.active) else {
                continue;
            };
            let shift = i64::from(shift);
            let other_start = shift_exact_point(&edge.start, shift, &self.period, work)?;
            let other_end = shift_exact_point(&edge.end, shift, &self.period, work)?;
            let [first, second, third] = triangle;
            let [start_node, middle_node, end_node] = ignored_nodes;
            for (triangle_edge, triangle_nodes) in [
                ([first, second], [start_node, middle_node]),
                ([second, third], [middle_node, end_node]),
                ([third, first], [end_node, start_node]),
            ] {
                let [edge_start, edge_end] = triangle_edge;
                if super::super::q::exact_segments_intersect_with_work(
                    edge_start,
                    edge_end,
                    &other_start,
                    &other_end,
                    work,
                )? && !intersection_is_endpoint_only(
                    triangle_edge,
                    triangle_nodes,
                    [&other_start, &other_end],
                    [edge.start_node, edge.end_node],
                    work,
                )? {
                    return Ok(false);
                }
            }
            for (point, node) in [(&other_start, edge.start_node), (&other_end, edge.end_node)] {
                if !ignored_nodes.contains(&node) && point_in_exact_triangle(point, triangle, work)?
                {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    fn row_regions_allow_chord(
        &self,
        start: &ExactChartPoint,
        end: &ExactChartPoint,
        rows: &[SourceRowId],
        base_lift: i64,
        work: &mut impl Work,
    ) -> Result<bool> {
        let midpoint = ExactChartPoint::from_rationals(
            start.x.try_midpoint_with_work(&end.x, work)?,
            start.y.try_midpoint_with_work(&end.y, work)?,
        );
        let base = shift_exact_point(
            &midpoint,
            base_lift.checked_neg().ok_or(ResourceError::SizeOverflow)?,
            &self.period,
            work,
        )?;
        let occupied = self.inventory.topology.face_rows_at_chart_point(
            &base,
            &self.analysis.face_rows,
            work,
        )?;
        let mut chain_row_occupied = false;
        for row in occupied {
            let row = SourceRowId(u32::try_from(row).map_err(|_| ResourceError::SizeOverflow)?);
            if rows.binary_search(&row).is_ok() {
                chain_row_occupied = true;
            } else {
                return Ok(false);
            }
        }
        Ok(rows.len() <= 1 || chain_row_occupied)
    }

    fn triangle_touches_occupied_pole(
        &self,
        first: &ExactChartPoint,
        second: &ExactChartPoint,
        third: &ExactChartPoint,
        work: &mut impl Work,
    ) -> Result<bool> {
        for (pole, occupied) in [ChartPole::North, ChartPole::South]
            .into_iter()
            .zip(self.occupied_poles)
        {
            if !occupied {
                continue;
            }
            work.charge_into_error(1)?;
            let pole_y = ExactRational::from_f64(
                self.inventory
                    .topology
                    .sources
                    .frame
                    .chart_pole_latitude(pole),
            );
            let low = if first.y.try_cmp_with_work(&second.y, work)?.is_le() {
                if first.y.try_cmp_with_work(&third.y, work)?.is_le() {
                    &first.y
                } else {
                    &third.y
                }
            } else if second.y.try_cmp_with_work(&third.y, work)?.is_le() {
                &second.y
            } else {
                &third.y
            };
            let high = if first.y.try_cmp_with_work(&second.y, work)?.is_ge() {
                if first.y.try_cmp_with_work(&third.y, work)?.is_ge() {
                    &first.y
                } else {
                    &third.y
                }
            } else if second.y.try_cmp_with_work(&third.y, work)?.is_ge() {
                &second.y
            } else {
                &third.y
            };
            if low.try_cmp_with_work(&pole_y, work)?.is_le()
                && high.try_cmp_with_work(&pole_y, work)?.is_ge()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
}
fn shift_exact_point(
    point: &ExactChartPoint,
    periods: i64,
    period: &ExactRational,
    work: &mut impl Work,
) -> Result<ExactChartPoint> {
    let multiplier = ExactRational::from_dyadic(ExactDyadic::from_i64(periods));
    let shift = period.try_product_with_work(&multiplier, work)?;
    Ok(ExactChartPoint::from_rationals(
        point.x.try_add_with_work(&shift, work)?,
        point.y.try_clone_with_work(work)?,
    ))
}

fn exact_rectangle(start: &ExactChartPoint, end: &ExactChartPoint) -> Result<Rectangle<[f64; 2]>> {
    let [start_min_x, start_min_y, start_max_x, start_max_y] = start.outward_bounds()?;
    let [end_min_x, end_min_y, end_max_x, end_max_y] = end.outward_bounds()?;
    let minimum = [start_min_x.min(end_min_x), start_min_y.min(end_min_y)];
    let maximum = [start_max_x.max(end_max_x), start_max_y.max(end_max_y)];
    if !minimum.into_iter().chain(maximum).all(f64::is_finite) {
        return Err(GeometryErrorKind::Invalid(
            "generated geographic chord has a non-finite Q envelope".into(),
        )
        .into());
    }
    Ok(Rectangle::from_corners(minimum, maximum))
}

fn exact_aabb(points: &[&ExactChartPoint]) -> Result<AABB<[f64; 2]>> {
    let mut minimum_x = f64::INFINITY;
    let mut minimum_y = f64::INFINITY;
    let mut maximum_x = f64::NEG_INFINITY;
    let mut maximum_y = f64::NEG_INFINITY;
    for point in points {
        let [min_x, min_y, max_x, max_y] = point.outward_bounds()?;
        minimum_x = minimum_x.min(min_x);
        minimum_y = minimum_y.min(min_y);
        maximum_x = maximum_x.max(max_x);
        maximum_y = maximum_y.max(max_y);
    }
    if ![minimum_x, minimum_y, maximum_x, maximum_y]
        .into_iter()
        .all(f64::is_finite)
    {
        return Err(GeometryErrorKind::Invalid(
            "geographic Q query has a non-finite envelope".into(),
        )
        .into());
    }
    Ok(AABB::from_corners([minimum_x, minimum_y], [
        maximum_x, maximum_y,
    ]))
}

fn intersection_is_endpoint_only(
    candidate: [&ExactChartPoint; 2],
    candidate_nodes: [QNodeId; 2],
    other: [&ExactChartPoint; 2],
    other_nodes: [QNodeId; 2],
    work: &mut impl Work,
) -> Result<bool> {
    for candidate_index in 0..2 {
        for other_index in 0..2 {
            work.charge_into_error(1)?;
            if candidate_nodes.get(candidate_index) != other_nodes.get(other_index)
                || !candidate
                    .get(candidate_index)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("candidate endpoint is incomplete".into())
                    })?
                    .try_eq_with_work(
                        other.get(other_index).ok_or_else(|| {
                            GeometryErrorKind::Invalid("Q span endpoint is incomplete".into())
                        })?,
                        work,
                    )?
            {
                continue;
            }
            let candidate_other = candidate
                .get(1_usize.wrapping_sub(candidate_index))
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("candidate opposite endpoint is incomplete".into())
                })?;
            let other_other = other
                .get(1_usize.wrapping_sub(other_index))
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q span opposite endpoint is incomplete".into())
                })?;
            let overlap = point_on_exact_segment_with_work(
                candidate_other,
                candidate.get(candidate_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("candidate endpoint is incomplete".into())
                })?,
                other_other,
                work,
            )? || point_on_exact_segment_with_work(
                other_other,
                candidate.get(candidate_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("candidate endpoint is incomplete".into())
                })?,
                candidate_other,
                work,
            )?;
            if !overlap {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn point_in_exact_triangle(
    point: &ExactChartPoint,
    triangle: &[ExactChartPoint; 3],
    work: &mut impl Work,
) -> Result<bool> {
    let first = exact_orientation(
        triangle.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("geographic guard triangle has no first vertex".into())
        })?,
        triangle.get(1).ok_or_else(|| {
            GeometryErrorKind::Invalid("geographic guard triangle has no second vertex".into())
        })?,
        point,
        work,
    )?;
    let second = exact_orientation(
        triangle.get(1).ok_or_else(|| {
            GeometryErrorKind::Invalid("geographic guard triangle has no second vertex".into())
        })?,
        triangle.get(2).ok_or_else(|| {
            GeometryErrorKind::Invalid("geographic guard triangle has no third vertex".into())
        })?,
        point,
        work,
    )?;
    let third = exact_orientation(
        triangle.get(2).ok_or_else(|| {
            GeometryErrorKind::Invalid("geographic guard triangle has no third vertex".into())
        })?,
        triangle.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("geographic guard triangle has no first vertex".into())
        })?,
        point,
        work,
    )?;
    if first == ExactSign::Zero && second == ExactSign::Zero && third == ExactSign::Zero {
        let [minimum, maximum] = exact_triangle_bounds(triangle, work)?;
        return Ok(point.x.try_cmp_with_work(&minimum.x, work)?.is_ge()
            && point.x.try_cmp_with_work(&maximum.x, work)?.is_le()
            && point.y.try_cmp_with_work(&minimum.y, work)?.is_ge()
            && point.y.try_cmp_with_work(&maximum.y, work)?.is_le());
    }
    let clockwise = [first, second, third]
        .into_iter()
        .all(|sign| sign != ExactSign::Positive);
    let counterclockwise = [first, second, third]
        .into_iter()
        .all(|sign| sign != ExactSign::Negative);
    Ok(clockwise || counterclockwise)
}

fn exact_orientation(
    start: &ExactChartPoint,
    end: &ExactChartPoint,
    point: &ExactChartPoint,
    work: &mut impl Work,
) -> Result<ExactSign> {
    let direction = vector_difference_with_work(start, end, work)?;
    let offset = vector_difference_with_work(start, point, work)?;
    Ok(vector_cross_with_work(&direction, &offset, work)?.sign())
}

fn exact_triangle_bounds(
    triangle: &[ExactChartPoint; 3],
    work: &mut impl Work,
) -> Result<[ExactChartPoint; 2]> {
    let first = triangle.first().ok_or_else(|| {
        GeometryErrorKind::Invalid("geographic guard triangle has no first vertex".into())
    })?;
    let mut minimum_x = first.x.try_clone_with_work(work)?;
    let mut maximum_x = first.x.try_clone_with_work(work)?;
    let mut minimum_y = first.y.try_clone_with_work(work)?;
    let mut maximum_y = first.y.try_clone_with_work(work)?;
    for point in triangle.iter().skip(1) {
        work.charge_into_error(1)?;
        if point.x.try_cmp_with_work(&minimum_x, work)?.is_lt() {
            minimum_x = point.x.try_clone_with_work(work)?;
        }
        if point.x.try_cmp_with_work(&maximum_x, work)?.is_gt() {
            maximum_x = point.x.try_clone_with_work(work)?;
        }
        if point.y.try_cmp_with_work(&minimum_y, work)?.is_lt() {
            minimum_y = point.y.try_clone_with_work(work)?;
        }
        if point.y.try_cmp_with_work(&maximum_y, work)?.is_gt() {
            maximum_y = point.y.try_clone_with_work(work)?;
        }
    }
    Ok([
        ExactChartPoint::from_rationals(minimum_x, minimum_y),
        ExactChartPoint::from_rationals(maximum_x, maximum_y),
    ])
}

#[cfg(test)]
mod tests {
    use super::super::analyze_geographic_coverage;
    use super::*;
    use crate::geographic::GeographicFrame;
    use crate::geometry::Unmetered;

    fn source_endpoint_port(
        analysis: &GeographicCoverageAnalysis,
        row: SourceRowId,
        x: f64,
        y: f64,
    ) -> QPortId {
        let (edge_index, endpoint_index) = analysis
            .topology
            .sources
            .edges
            .iter()
            .enumerate()
            .find_map(|(edge_index, edge)| {
                if edge.row != row {
                    return None;
                }
                edge.source
                    .iter()
                    .position(|point| {
                        point.x.to_bits() == x.to_bits() && point.y.to_bits() == y.to_bits()
                    })
                    .map(|endpoint_index| (edge_index, endpoint_index))
            })
            .expect("source coordinate has an exact source edge endpoint");
        *analysis
            .topology
            .q
            .source_edge_ports
            .get(edge_index)
            .and_then(|ports| ports.get(endpoint_index))
            .expect("source edge endpoint has an exact Q port")
    }

    fn source_endpoint_node(
        analysis: &GeographicCoverageAnalysis,
        row: SourceRowId,
        x: f64,
        y: f64,
    ) -> QNodeId {
        let port_id = source_endpoint_port(analysis, row, x, y);
        analysis
            .topology
            .q
            .ports
            .get(port_id.0 as usize)
            .expect("source endpoint Q port remains present")
            .node
    }

    fn source_endpoint_point(
        analysis: &GeographicCoverageAnalysis,
        row: SourceRowId,
        x: f64,
        y: f64,
    ) -> ExactChartPoint {
        let port_id = source_endpoint_port(analysis, row, x, y);
        analysis
            .topology
            .q
            .ports
            .get(port_id.0 as usize)
            .expect("source endpoint Q port remains present")
            .point
            .clone()
    }

    #[test]
    fn shared_chain_simplifies_once_and_composes_exact_row_roots() {
        let rows = [
            crate::io::parse_wkt("POLYGON ((0 0, 2 0, 2 1, 2 2, 0 2, 0 0))")
                .expect("first coverage polygon"),
            crate::io::parse_wkt("POLYGON ((2 0, 3 0, 3 2, 2 2, 2 0))")
                .expect("second coverage polygon"),
        ];
        let analysis =
            analyze_geographic_coverage(&rows, GeographicFrame::crs84(), 0.0, "coverage_simplify")
                .expect("geographic coverage admits");
        let mut work = Unmetered;
        let plan = analysis
            .simplify_geographic_chains(0.1, SimplifyMethod::Dp, true, &mut work)
            .expect("geographic Q chains simplify");
        let (chain_index, source_chain) = plan
            .inventory
            .chains
            .iter()
            .enumerate()
            .find(|(_, chain)| {
                chain.spans.len() == 2
                    && chain.row_uses.iter().any(|use_| use_.row == SourceRowId(0))
                    && chain.row_uses.iter().any(|use_| use_.row == SourceRowId(1))
            })
            .expect("subdivided interface is one canonical Q chain");
        let chain = plan
            .chains
            .get(chain_index)
            .expect("simplified chain aligns with its source chain");
        assert_eq!(chain.vertices.len(), 2);
        assert_eq!(chain.edges.len(), 1);
        let edge = chain.edges.first().expect("shared chain has its chord");
        assert!(edge.generated);
        assert_eq!(edge.source_span_range, 0..2);
        assert_eq!(edge.row_uses.len(), source_chain.row_uses.len());

        let first_row_use = source_chain
            .row_uses
            .iter()
            .position(|use_| use_.row == SourceRowId(0))
            .expect("first row has a signed shared-chain use");
        let second_row_use = source_chain
            .row_uses
            .iter()
            .position(|use_| use_.row == SourceRowId(1))
            .expect("second row has a signed shared-chain use");
        let first_roots = edge
            .row_uses
            .get(first_row_use)
            .expect("first row roots are replayed")
            .roots
            .as_slice();
        let second_roots = edge
            .row_uses
            .get(second_row_use)
            .expect("second row roots are replayed")
            .roots
            .as_slice();
        let zero = ExactRational::zero();
        let one = ExactRational::one();
        assert_eq!(first_roots.len(), 2);
        assert!(
            first_roots.iter().all(|root| {
                root.source_range.start.is_zero()
                    && root
                        .source_range
                        .end
                        .try_cmp(&one)
                        .expect("exact source-root comparison succeeds")
                        .is_eq()
            }),
            "the two original source edges retain their full exact intervals",
        );
        assert_eq!(second_roots.len(), 1);
        let composed = second_roots.first().expect("split edge roots compose");
        assert!(
            composed
                .source_range
                .start
                .try_cmp(&zero)
                .expect("exact source-root comparison succeeds")
                .is_eq()
        );
        assert!(
            composed
                .source_range
                .end
                .try_cmp(&one)
                .expect("exact source-root comparison succeeds")
                .is_eq()
        );
        assert_eq!(composed.owner_refs.len(), 2);
    }

    #[test]
    fn q_guard_keeps_a_kink_that_contains_an_occupied_neighbor() {
        let notch = crate::io::parse_wkt("POLYGON ((0 0, 4 0, 4 1, 3 2, 4 3, 4 4, 0 4, 0 0))")
            .expect("notched coverage row");
        let neighbor =
            crate::io::parse_wkt("POLYGON ((3.5 1.7, 3.9 1.7, 3.9 2.3, 3.5 2.3, 3.5 1.7))")
                .expect("disjoint coverage neighbor");

        let alone = [notch.clone()];
        let alone_analysis =
            analyze_geographic_coverage(&alone, GeographicFrame::crs84(), 0.0, "coverage_simplify")
                .expect("notched row admits alone");
        let alone_kink = source_endpoint_node(&alone_analysis, SourceRowId(0), 3.0, 2.0);
        let mut work = Unmetered;
        let alone_plan = alone_analysis
            .simplify_geographic_chains(1.1, SimplifyMethod::Dp, true, &mut work)
            .expect("notched row simplifies without a neighbor");
        assert!(
            alone_plan
                .chains
                .iter()
                .flat_map(|chain| &chain.vertices)
                .all(|vertex| vertex.node != alone_kink),
            "the kink is removable when its swept region contains no other row",
        );

        let rows = [notch, neighbor];
        let analysis =
            analyze_geographic_coverage(&rows, GeographicFrame::crs84(), 0.0, "coverage_simplify")
                .expect("notch and neighbor form a valid coverage");
        let kink = source_endpoint_node(&analysis, SourceRowId(0), 3.0, 2.0);
        let mut work = Unmetered;
        let plan = analysis
            .simplify_geographic_chains(1.1, SimplifyMethod::Dp, true, &mut work)
            .expect("Q guard retains the occupied neighbor");
        assert!(
            plan.chains
                .iter()
                .flat_map(|chain| &chain.vertices)
                .any(|vertex| vertex.node == kink),
            "a foreign Q vertex inside the shortcut's swept region stays covered",
        );
    }

    #[test]
    fn q_guard_rejects_a_chord_crossing_a_live_generated_chain() {
        let rows = [
            crate::io::parse_wkt("POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))").expect("coverage square")
        ];
        let analysis =
            analyze_geographic_coverage(&rows, GeographicFrame::crs84(), 0.0, "coverage_simplify")
                .expect("coverage square admits");
        let mut work = Unmetered;
        let inventory = analysis
            .shared_chain_inventory(&mut work)
            .expect("coverage Q inventory builds");
        let mut guard =
            GeographicQGuard::new(&analysis, &inventory, &mut work).expect("Q guard builds");
        let start_node = source_endpoint_node(&analysis, SourceRowId(0), 0.0, 0.0);
        let end_node = source_endpoint_node(&analysis, SourceRowId(0), 4.0, 4.0);
        let candidate_start_node = source_endpoint_node(&analysis, SourceRowId(0), 4.0, 0.0);
        let candidate_end_node = source_endpoint_node(&analysis, SourceRowId(0), 0.0, 4.0);
        let start = source_endpoint_point(&analysis, SourceRowId(0), 0.0, 0.0);
        let end = source_endpoint_point(&analysis, SourceRowId(0), 4.0, 4.0);
        let candidate_start = source_endpoint_point(&analysis, SourceRowId(0), 4.0, 0.0);
        let candidate_end = source_endpoint_point(&analysis, SourceRowId(0), 0.0, 4.0);
        let generated_id = guard.generated.len();
        guard.generated.push(LiveGeographicChord {
            start_node,
            end_node,
            start: start.clone(),
            end: end.clone(),
            active: true,
        });
        for shift in [-1_i8, 0, 1] {
            let start = shift_exact_point(&start, i64::from(shift), &guard.period, &mut work)
                .expect("generated chord shift is exact");
            let end = shift_exact_point(&end, i64::from(shift), &guard.period, &mut work)
                .expect("generated chord shift is exact");
            guard.generated_index.insert(GeomWithData::new(
                exact_rectangle(&start, &end).expect("generated chord envelope is finite"),
                (generated_id, shift),
            ));
        }
        assert!(
            !guard
                .generated_chords_allow_segment(
                    [candidate_start_node, candidate_end_node],
                    None,
                    None,
                    &candidate_start,
                    &candidate_end,
                    &mut work,
                )
                .expect("live-chord query succeeds"),
            "crossing a previously generated chord is unsafe",
        );
    }

    #[test]
    fn q_guard_rejects_a_shortcut_through_another_row_at_a_junction() {
        let rows = [
            crate::io::parse_wkt("POLYGON ((0 0, 4 0, 4 1, 3 2, 4 3, 4 4, 0 4, 0 0))")
                .expect("notched polygon"),
            crate::io::parse_wkt("POLYGON ((3.5 1.7, 3.9 1.7, 3.9 2.3, 3.5 2.3, 3.5 1.7))")
                .expect("polygon in the notch"),
        ];
        let analysis =
            analyze_geographic_coverage(&rows, GeographicFrame::crs84(), 0.0, "coverage_simplify")
                .expect("notched rows form a valid coverage");
        let mut work = Unmetered;
        let plan = analysis
            .simplify_geographic_chains(1.1, SimplifyMethod::Dp, true, &mut work)
            .expect("guarded geographic chains simplify");
        let kink = source_endpoint_node(&analysis, SourceRowId(0), 3.0, 2.0);
        assert!(
            plan.chains
                .iter()
                .flat_map(|chain| &chain.vertices)
                .any(|vertex| vertex.node == kink),
            "the notch vertex cannot be removed through the neighboring row",
        );
    }

    #[test]
    fn q_guard_uses_exact_occupied_face_labels_for_shortcut_midpoints() {
        let rows = [
            crate::io::parse_wkt("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))")
                .expect("first coverage polygon"),
            crate::io::parse_wkt("POLYGON ((3 0, 4 0, 4 1, 3 1, 3 0))")
                .expect("second coverage polygon"),
        ];
        let analysis =
            analyze_geographic_coverage(&rows, GeographicFrame::crs84(), 0.0, "coverage_simplify")
                .expect("disjoint coverage admits");
        let mut work = Unmetered;
        let inventory = analysis
            .shared_chain_inventory(&mut work)
            .expect("coverage Q inventory builds");
        let guard =
            GeographicQGuard::new(&analysis, &inventory, &mut work).expect("Q guard builds");
        let start = source_endpoint_point(&analysis, SourceRowId(1), 3.0, 0.0);
        let end = source_endpoint_point(&analysis, SourceRowId(1), 4.0, 1.0);
        assert!(
            !guard
                .row_regions_allow_chord(&start, &end, &[SourceRowId(0)], 0, &mut work,)
                .expect("Q row-side lookup succeeds"),
            "a row-0 chain cannot shortcut through row 1's occupied Q face",
        );
    }

    #[test]
    fn q_guard_rejects_triangles_that_reach_an_occupied_pole() {
        let rows = [
            crate::io::parse_wkt("POLYGON ((-10 80, 10 80, 10 90, -10 90, -10 80))")
                .expect("polar coverage cap"),
        ];
        let analysis =
            analyze_geographic_coverage(&rows, GeographicFrame::crs84(), 0.0, "coverage_simplify")
                .expect("polar coverage admits");
        let mut work = Unmetered;
        let inventory = analysis
            .shared_chain_inventory(&mut work)
            .expect("polar Q inventory builds");
        let guard =
            GeographicQGuard::new(&analysis, &inventory, &mut work).expect("polar Q guard builds");
        assert!(guard.occupied_poles[0]);
        let triangle = [
            ExactChartPoint::from_rationals(
                ExactRational::from_f64(-5.0),
                ExactRational::from_f64(80.0),
            ),
            ExactChartPoint::from_rationals(
                ExactRational::from_f64(5.0),
                ExactRational::from_f64(90.0),
            ),
            ExactChartPoint::from_rationals(
                ExactRational::from_f64(0.0),
                ExactRational::from_f64(82.0),
            ),
        ];
        assert!(
            guard
                .triangle_touches_occupied_pole(
                    triangle.first().expect("triangle has first point"),
                    triangle.get(1).expect("triangle has second point"),
                    triangle.get(2).expect("triangle has third point"),
                    &mut work,
                )
                .expect("pole guard query succeeds")
        );
    }

    #[test]
    fn shared_chain_sequence_keeps_cut_aliases_in_one_exact_lift() {
        let rows = [crate::io::parse_wkt(
            "POLYGON ((-180 20, -90 20, 0 20, 90 20, 180 20, \
             180 30, 90 30, 0 30, -90 30, -180 30, -180 20))",
        )
        .expect("full-longitude band")];
        let analysis =
            analyze_geographic_coverage(&rows, GeographicFrame::crs84(), 0.0, "coverage_simplify")
                .expect("full-longitude coverage admits");
        let mut work = Unmetered;
        let plan = analysis
            .simplify_geographic_chains(1.0, SimplifyMethod::Vw, true, &mut work)
            .expect("lifted Q chain sequences simplify");
        assert!(
            plan.inventory
                .chains
                .iter()
                .enumerate()
                .any(|(chain_index, source_chain)| {
                    let crosses_cut_alias = source_chain.spans.windows(2).any(|pair| {
                        let Some((left, right)) = pair.first().zip(pair.get(1)) else {
                            return false;
                        };
                        let left_cut = analysis
                            .topology
                            .q
                            .ports
                            .get(left.to_port.0 as usize)
                            .is_some_and(|port| port.cut);
                        let right_cut = analysis
                            .topology
                            .q
                            .ports
                            .get(right.from_port.0 as usize)
                            .is_some_and(|port| port.cut);
                        left.to_node == right.from_node
                            && left.to_port != right.from_port
                            && (left_cut || right_cut)
                    });
                    crosses_cut_alias
                        && plan
                            .chains
                            .get(chain_index)
                            .is_some_and(|chain| !chain.vertices.is_empty())
                })
        );
        let period = ExactRational::from_f64(GeographicFrame::crs84().longitude().period());
        for chain in &plan.chains {
            for pair in chain.vertices.windows(2) {
                let [start, end] = pair else {
                    continue;
                };
                let delta = end
                    .point
                    .x
                    .try_subtract(&start.point.x)
                    .expect("exact lifted longitude subtraction succeeds");
                let periods = delta
                    .try_quotient(&period)
                    .expect("exact longitude-period quotient succeeds");
                assert!(
                    !periods.is_integer() || periods.is_zero(),
                    "adjacent simplified vertices do not jump between cut aliases",
                );
            }
        }
        let mut composed_lift_accounts_for_cut_alias = false;
        for (chain_index, chain) in plan.chains.iter().enumerate() {
            let source_chain = plan
                .inventory
                .chains
                .get(chain_index)
                .expect("simplified chains remain aligned with the inventory");
            for edge in &chain.edges {
                for row_use in &edge.row_uses {
                    for root in &row_use.roots {
                        for owner_ref in &root.owner_refs {
                            let source_span = source_chain
                                .spans
                                .get(owner_ref.chain_span_index)
                                .expect("composed root references a source Q span");
                            let owner = plan
                                .inventory
                                .topology
                                .q
                                .spans
                                .get(source_span.span.0 as usize)
                                .and_then(|span| span.owners.get(owner_ref.owner_index))
                                .expect("composed root retains its original Q owner");
                            composed_lift_accounts_for_cut_alias |= root.lift != owner.lift;
                        }
                    }
                }
            }
        }
        assert!(
            composed_lift_accounts_for_cut_alias,
            "composed roots carry the continuous lift used by their simplified chain",
        );
    }
}
