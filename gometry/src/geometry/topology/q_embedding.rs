//! Certified binary64 assignments for geographic quotient emissions.
//!
//! Q charts lift canonical node coordinates to exact chart ports. The shared
//! embedding search operates on node candidates; this module owns those lifts,
//! quotient-specific certificates, and their Q problem implementation.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::{ControlFlow, Deref};

use self::cut_reconstruction::reconstruct_cut_ports;
use self::native::assignment::*;
use self::native::cut_search::*;
use self::native::cycles::{
    NativeGraphCycle, NativeGraphPart, align_native_cycle_starts, native_graph_cycle_recipe,
    native_graph_cycle_vertex, native_reference_cycles_and_parts,
};
use self::native::linework::*;
use self::native::model::*;
use self::native::ports::*;
use self::native::producer_tables::*;
use self::native::reference_builder::*;
use self::native::refinement::*;
use self::native::seam_bands::*;
use self::native::source_ranges::*;
use self::native::transport::*;
use self::native::whole_sphere::*;
use super::emission::cut_ports::CutNodePorts;
use super::emission::plan::{
    DrawnAreaOrigin, DrawnPolygons, PlannedAreaRing, PolygonPlan, QEmbeddingEmission,
    QEmissionDrawing, QEmissionPlan,
};
use super::emission::sites::{
    EmittedNodeKey, EmittedVertex, QCandidate, QCoordinates, canonical_bits,
    canonicalize_ring_vertices, emitted_node_key, exact_coordinate_matches_with_work,
};
use super::emission::source_reuse::{source_line_reuse_is_complete, source_ring_reuse_matches};
use super::emission::{
    AssignedSite, EmissionChordId, EmissionNodeId, EmissionNodeRegistry, EmissionPort,
    ExpandedEmissionPlan, FeatureFamilyBounds, FeatureFamilyRegistry, FragmentRecipe,
    FragmentRecipeId, NativeAreaEdge, NativeAreaEdgeKind, NativeCutFeature, NativeFaceId,
    NativeGraphEdgeOwner, NativePortData, NativeSeamEvent, NativeSeamVertices,
    NativeSourceFaceGerm, NativeSourceSpanUse, NodeAuthority, OccurrenceSource, PortPosition,
    PortSpelling, PublicationInventory, RecipeWalk, RecipeWalkOwner, SealedSourceDrawing,
    SelectedNativeSeamBand, SelectedUse, SourceFrozenDrawing, SourceSampleOccurrence, SourceTrace,
    TraceRequestLocation, TracedLine, merge_native_port_data, native_pole_edges, native_seam_edges,
    native_source_sample_occurrences,
};
use super::exact::{
    ExactChartPoint, ExactQuotientKey, ExactSourceEdge, ExactSourceRange, FallibleAvl,
    SourceEdgeId, SourceEdgeKind, SourceRowId, compare_quotient_keys,
    compare_quotient_keys_with_work, point_on_exact_segment_with_work,
    quotient_key_for_chart_position_with_work, vector_cross_with_work, vector_difference_with_work,
};
use super::q::{
    ChartSpan, ChartSpanId, CutChart, ExactGeographicQ, ExactGeographicTopology, GlobalFaceId,
    GlobalFaceStructure, QDartId, QFaceChart, QNode, QNodeId, QNodeKey, QPort, QPortId,
    QuotientFaces, compare_q_darts_with_points, compare_qnode_keys, compare_qnode_keys_with_work,
    global_face_structure,
};
use super::{EmissionPortId, QSourceImage};
use crate::collections::{HashMap, HashSet, UnionFind, collection_hash_state};
use crate::error::{ResourceError, Result};
use crate::geographic::{
    ChartPole, GeographicFrame, GeographicRing, Latitude, RingSummary, SeamIdentity,
};
use crate::geometry::embedding_completion::{
    AdmittedGraph, AdmittedLayout, CheckedAssignment, CheckedImages,
    ExactPoints as NativeReferencePoints, FrozenLiteralBuffers, LiteralChart, LiteralCheck,
    LogicalArealWalk, SealedOutput, ShapeLiteralLayout, admit_candidate, admit_known_graph,
    admit_logical_layout, bind_literals, exact_chord_fraction,
};
use crate::geometry::embedding_search::{
    ClassAssignment, EvaluationBudget, GlobalCheck, Problem, SearchAccount, SearchOutcome,
    proximity_limit, run_with_budget, swept_bounds,
};
use crate::geometry::exact_arith::{
    ExactSign, RoundedNativeSite, retained_exact_native_site_with_work,
    round_native_longitude_with_work, rounded_native_neighbors_with_work,
    split_rational_native_site_with_work,
};
#[cfg(test)]
use crate::geometry::ordinates::RequestRowId;
use crate::geometry::ordinates::{
    NodeOrdinates, RootId, RootRange, RootRequest, SourceArcMetricAccess,
};
use crate::geometry::overlay::{
    EmbeddingDomain, GlobalFaces, RayAxis, RayPoints, RayPointsWithWork,
    compare_departures_with_work, graph_global_faces_with_work, segments_within_chebyshev,
    shoot_left_with_work, walk_is_shell_with_work,
};
use crate::geometry::publication::{
    ChartRestriction, ProducerReceipt, PublicationCall, PublicationOutcome, PublicationRejection,
    StorageTarget, finish_geographic,
};
use crate::geometry::segments::try_sort_by_with_work;
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational, Interval};
use crate::geometry::{
    Bounds, ChartMetric, CoordSeq, GeometryErrorKind, Point, Ring, RingWinding, Segment, Unmetered,
    Work, XY, for_each_overlapping_bounds_cross_pair_with_work, ring_winding,
};

mod cut_reconstruction;
mod dependencies;
mod domains;
pub(in crate::geometry::topology) mod emission;
mod endpoint_point_cache;
mod face_structure;
mod native;
mod problem;
mod source_binding;
use self::endpoint_point_cache::{NativeEndpointPointCache, NativeEndpointPointKey};
pub(in crate::geometry) mod publication;
use publication::{NativeCandidatePublication, SourceOnlyPublication};

use self::dependencies::chords::*;
use self::dependencies::classes::*;
use self::dependencies::proximity::*;
use self::domains::*;
use self::emission::ring_roles::*;
use self::face_structure::*;
use self::publication::chords::*;
use self::publication::fans::*;
use self::publication::model::*;
use self::publication::tracing::*;
use self::publication::witness::*;
use self::source_binding::*;

type NativeCutPeriodRange = (i64, i64);
type NativeChordCutRanges = Vec<NativeCutPeriodRange>;
type NativeRecipeCutRanges = Vec<NativeChordCutRanges>;
type NativeCutFamilyDomains = Vec<NativeRecipeCutRanges>;

/// Compare the role used for publication with the role of its exact Q ports.
/// `None` means the emitted chart has no classifiable ring role and must be
/// rejected as a candidate; failures while classifying the exact ring remain
/// internal errors.
#[cfg(test)]
pub(super) fn compare_q_ring_role<'a, I>(
    frame: GeographicFrame,
    emitted_chart: &Ring,
    published: &Ring,
    exact_ports: I,
    quotient_closure: bool,
) -> Result<Option<QRingRole>>
where
    I: IntoIterator<Item = &'a ExactChartPoint>,
    I::IntoIter: Clone + ExactSizeIterator + DoubleEndedIterator,
{
    let mut work = Unmetered;
    compare_q_ring_role_with_work(
        frame,
        emitted_chart,
        published,
        exact_ports,
        quotient_closure,
        &mut work,
    )
}

fn compare_q_ring_role_with_work<'a, I>(
    frame: GeographicFrame,
    emitted_chart: &Ring,
    published: &Ring,
    exact_ports: I,
    quotient_closure: bool,
    work: &mut impl Work,
) -> Result<Option<QRingRole>>
where
    I: IntoIterator<Item = &'a ExactChartPoint>,
    I::IntoIter: Clone + ExactSizeIterator + DoubleEndedIterator,
{
    let exact_ports = exact_ports.into_iter();
    work.charge_into_error(
        u64::try_from(exact_ports.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let exact_is_hole = exact_q_ring_role_with_work(frame, exact_ports, work)?;
    work.charge_into_error(
        u64::try_from(
            emitted_chart
                .coords()
                .len()
                .saturating_add(published.coords().len()),
        )
        .map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let Ok(emitted_is_hole) = ring_role_from_winding(
        frame,
        ring_winding(emitted_chart),
        published,
        quotient_closure,
    ) else {
        return Ok(None);
    };
    let published_matches_exact = if ring_winding(published) == RingWinding::Degenerate {
        let Ok(published_is_hole) =
            ring_role_from_winding(frame, RingWinding::Degenerate, published, true)
        else {
            return Ok(None);
        };
        published_is_hole == exact_is_hole
    } else {
        true
    };
    Ok(Some(QRingRole {
        emitted_is_hole,
        exact_is_hole,
        published_matches_exact,
    }))
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) enum QVariableNode {
    Source(QNodeId),
    Published(EmissionNodeId),
}

impl PublishedEndpoint {
    fn try_clone_with_work(&self, work: &mut impl Work) -> Result<Self> {
        Ok(match self {
            Self::Source {
                port,
                source_record,
                sample,
            } => Self::Source {
                port: *port,
                source_record: *source_record,
                sample: sample
                    .as_ref()
                    .map(|sample| sample.try_clone_with_work(work))
                    .transpose()?,
            },
            Self::Published { node, sample } => Self::Published {
                node: *node,
                sample: *sample,
            },
        })
    }
}

#[derive(Clone, Copy)]
struct UnifiedQClassInputs<'a> {
    topology: &'a ExactGeographicTopology,
    source_classes: &'a [QSourceClass],
    publication_variables: &'a [EmissionNodeId],
    publication_nodes: &'a EmissionNodeRegistry,
    source_chords: &'a [QChord],
    publication_chords: &'a [QPublishedChord],
    proximity_pairs: &'a [QProximityPair],
    cut_rows: &'a [QCutRow],
    fan_rows: &'a [QFanRow],
    publication_pairs: &'a [QPublicationPair],
    source_domains: &'a [Vec<QCandidate>],
    source_movable_axes: &'a [[bool; 2]],
    publication_domains: &'a [Vec<QCandidate>],
    producer_witness: &'a PreparedProducerWitness,
}

fn exact_orientation_with_work(
    start: &ExactChartPoint,
    end: &ExactChartPoint,
    point: &ExactChartPoint,
    work: &mut impl Work,
) -> Result<ExactSign> {
    let direction = vector_difference_with_work(start, end, work)?;
    let offset = vector_difference_with_work(start, point, work)?;
    Ok(vector_cross_with_work(&direction, &offset, work)?.sign())
}

fn exact_segments_properly_cross_with_work(
    left: &[ExactChartPoint; 2],
    right: &[ExactChartPoint; 2],
    work: &mut impl Work,
) -> Result<bool> {
    let left_start = exact_orientation_with_work(&left[0], &left[1], &right[0], work)?;
    let left_end = exact_orientation_with_work(&left[0], &left[1], &right[1], work)?;
    let right_start = exact_orientation_with_work(&right[0], &right[1], &left[0], work)?;
    let right_end = exact_orientation_with_work(&right[0], &right[1], &left[1], work)?;
    Ok(matches!(
        (left_start, left_end, right_start, right_end),
        (
            ExactSign::Positive,
            ExactSign::Negative,
            ExactSign::Positive,
            ExactSign::Negative
        ) | (
            ExactSign::Positive,
            ExactSign::Negative,
            ExactSign::Negative,
            ExactSign::Positive
        ) | (
            ExactSign::Negative,
            ExactSign::Positive,
            ExactSign::Positive,
            ExactSign::Negative
        ) | (
            ExactSign::Negative,
            ExactSign::Positive,
            ExactSign::Negative,
            ExactSign::Positive
        )
    ))
}

#[cfg(test)]
pub(super) fn pair_preserves_incidence<N: Copy + Eq>(
    left_nodes: [N; 2],
    left_exact: &[ExactChartPoint; 2],
    left_candidate: &[ExactChartPoint; 2],
    right_nodes: [N; 2],
    right_exact: &[ExactChartPoint; 2],
    right_candidate: &[ExactChartPoint; 2],
) -> Result<bool> {
    let mut work = Unmetered;
    pair_preserves_incidence_with_work(
        left_nodes,
        left_exact,
        left_candidate,
        right_nodes,
        right_exact,
        right_candidate,
        &mut work,
    )
}

fn pair_preserves_incidence_with_work<N: Copy + Eq>(
    left_nodes: [N; 2],
    left_exact: &[ExactChartPoint; 2],
    left_candidate: &[ExactChartPoint; 2],
    right_nodes: [N; 2],
    right_exact: &[ExactChartPoint; 2],
    right_candidate: &[ExactChartPoint; 2],
    work: &mut impl Work,
) -> Result<bool> {
    let shared_node = left_nodes.iter().any(|node| right_nodes.contains(node));
    if !shared_node {
        return Ok(super::q::exact_segments_intersect_with_work(
            &left_exact[0],
            &left_exact[1],
            &right_exact[0],
            &right_exact[1],
            work,
        )? == super::q::exact_segments_intersect_with_work(
            &left_candidate[0],
            &left_candidate[1],
            &right_candidate[0],
            &right_candidate[1],
            work,
        )?);
    }
    if exact_segments_properly_cross_with_work(left_exact, right_exact, work)?
        != exact_segments_properly_cross_with_work(left_candidate, right_candidate, work)?
    {
        return Ok(false);
    }
    for (nodes, exact, candidate, other_nodes, other_exact, other_candidate) in [
        (
            left_nodes,
            left_exact,
            left_candidate,
            right_nodes,
            right_exact,
            right_candidate,
        ),
        (
            right_nodes,
            right_exact,
            right_candidate,
            left_nodes,
            left_exact,
            left_candidate,
        ),
    ] {
        let [first_node, second_node] = nodes;
        let [first_exact, second_exact] = exact;
        let [first_candidate, second_candidate] = candidate;
        let [other_start, other_end] = other_exact;
        let [other_candidate_start, other_candidate_end] = other_candidate;
        for (node, exact_point, candidate_point) in [
            (first_node, first_exact, first_candidate),
            (second_node, second_exact, second_candidate),
        ] {
            if other_nodes.contains(&node) {
                continue;
            }
            if point_on_exact_segment_with_work(exact_point, other_start, other_end, work)?
                != point_on_exact_segment_with_work(
                    candidate_point,
                    other_candidate_start,
                    other_candidate_end,
                    work,
                )?
            {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

fn translated_exact_point_with_work(
    point: &ExactChartPoint,
    shift: i8,
    period: &ExactRational,
    work: &mut impl Work,
) -> Result<ExactChartPoint> {
    if shift == 0 {
        return point.try_clone_with_work(work);
    }
    let offset = period.try_product_with_work(&ExactRational::from_f64(f64::from(shift)), work)?;
    Ok(ExactChartPoint::from_rationals(
        point.x.try_add_with_work(&offset, work)?,
        point.y.try_clone_with_work(work)?,
    ))
}

const fn candidate_from_emission_site(site: super::emission::AssignedSite) -> QCandidate {
    QCandidate {
        native: XY::new(site.longitude.native().get(), site.latitude),
        periods: site.longitude.periods(),
    }
}

struct QEmbeddingProblem<'topology, 'emission, 'access, 'metric, M: ChartMetric> {
    topology: &'topology ExactGeographicTopology,
    emission: &'emission QEmbeddingEmission<'topology, 'emission>,
    family_state: QFamilyState,
    ordinates: Vec<NodeOrdinates>,
    metric_access: Option<&'access SourceArcMetricAccess<'access, 'metric, M>>,
    declared_axes: crate::geometry::CoordinateAxes,
    strict_axes: bool,
    operation: &'static str,
    nearest: QCoordinates,
    exact_positions: Vec<Option<ExactNodePosition>>,
    domains: Vec<Vec<QCandidate>>,
    cut_east: ExactRational,
    cut_index_by_node: Vec<Option<usize>>,
    cut_rows: Vec<QCutRow>,
    chords: Vec<QChord>,
    publication_nodes: EmissionNodeRegistry,
    publication_sample_nodes: Vec<EmissionNodeId>,
    // Source-authority samples reuse the admitted Q-port ordinal, never a
    // coordinate-key lookup in the assigned-output writer.
    publication_sample_source_ports: Vec<Option<QPortId>>,
    publication_chords: Vec<QPublishedChord>,
    publication_chord_offsets: Vec<usize>,
    publication_pairs: Vec<QPublicationPair>,
    prepared_producer: PreparedProducerWitness,
    publication_exact_positions: Vec<Option<ExactNodePosition>>,
    publication_nearest: Vec<Option<QCandidate>>,
    publication_domains: Vec<Vec<QCandidate>>,
    publication_location: Vec<Option<QNodeLocation>>,
    fan_rows: Vec<QFanRow>,
    proximity_pairs: Vec<QProximityPair>,
    classes: Vec<Vec<QVariableNode>>,
    node_location: Vec<Option<(usize, usize)>>,
    class_states: Vec<QClassState>,
    dependency_index: QDependencyIndex,
    live_nodes: &'emission [QNodeId],
    source_inventory: Vec<QNodeId>,
    exact_faces: Option<QFaceStructure>,
}

enum QFamilyState {
    Shared { original_source_base: usize },
    Native(FeatureFamilyRegistry),
}

pub(super) enum NearestQCertificate {
    Pass(Box<QEmbeddingOutput>),
    SearchRequired,
    Unrepresentable,
}

pub(super) struct QEmbeddingOutput {
    pub(super) sealed: super::emission::SealedQDrawing,
    pub(super) family_state: Option<FeatureFamilyRegistry>,
}

pub(in crate::geometry::topology::q_embedding) struct QEmbeddingPass {
    pub(in crate::geometry::topology::q_embedding) output: QEmbeddingOutput,
    #[cfg(test)]
    pub(in crate::geometry::topology::q_embedding) coordinates: QCoordinates,
    #[cfg(test)]
    pub(in crate::geometry::topology::q_embedding) searched: bool,
}

#[derive(Clone, Copy)]
pub(super) struct QEmbeddingCertification<'topology, 'emission, 'access, 'metric, M: ChartMetric> {
    pub(super) topology: &'topology ExactGeographicTopology,
    pub(super) emission: &'emission QEmbeddingEmission<'topology, 'emission>,
    pub(super) nearest: &'emission QCoordinates,
    pub(super) ordinates: &'emission [NodeOrdinates],
    pub(super) declared_axes: crate::geometry::CoordinateAxes,
    pub(super) strict_axes: bool,
    pub(super) operation: &'static str,
    pub(super) metric_access: Option<&'access SourceArcMetricAccess<'access, 'metric, M>>,
}

impl<'topology, 'emission, 'access, 'metric, M: ChartMetric>
    QEmbeddingCertification<'topology, 'emission, 'access, 'metric, M>
{
    fn build_problem<A: SearchAccount>(
        &self,
        budget: &mut A,
    ) -> Result<QEmbeddingProblem<'topology, 'emission, 'access, 'metric, M>> {
        QEmbeddingProblem::new(
            self.topology,
            self.emission,
            self.nearest,
            self.ordinates,
            self.declared_axes,
            self.strict_axes,
            self.operation,
            self.metric_access,
            budget,
        )
    }
}

impl<'topology, 'emission, 'access, 'metric, M: ChartMetric>
    QEmbeddingProblem<'topology, 'emission, 'access, 'metric, M>
{
    fn new(
        topology: &'topology ExactGeographicTopology,
        emission: &'emission QEmbeddingEmission<'topology, 'emission>,
        nearest: &QCoordinates,
        ordinates: &'emission [NodeOrdinates],
        declared_axes: crate::geometry::CoordinateAxes,
        strict_axes: bool,
        operation: &'static str,
        metric_access: Option<&'access SourceArcMetricAccess<'access, 'metric, M>>,
        budget: &mut impl SearchAccount,
    ) -> Result<Self> {
        let q = &topology.q;
        let frame = topology.sources.frame;
        if emission.publication.fragments.len() != emission.fragment_recipes.len()
            || emission.publication.counts.len() != emission.fragment_recipes.len()
            || emission.publication.ordinates.len() != emission.publication.samples.len()
        {
            return Err(GeometryErrorKind::Invalid(
                "Q publication inventory does not match its fragment recipes".into(),
            )
            .into());
        }
        budget.charge_into_error(
            u64::try_from(emission.fragment_recipes.len())
                .map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut publication_counts = vec![0_usize; emission.fragment_recipes.len()];
        let mut publication_chord_offsets: Vec<usize> = Vec::with_capacity(
            emission
                .fragment_recipes
                .len()
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?,
        );
        publication_chord_offsets.push(0);
        for (index, fragment) in emission.publication.fragments.iter().enumerate() {
            budget.charge_into_error(1)?;
            if fragment.recipe.0 as usize != index
                || fragment.trace.rows.edge_count().checked_add(1) != Some(fragment.points.len())
            {
                return Err(GeometryErrorKind::Invalid(
                    "Q publication fragment trace is not indexed by its recipe".into(),
                )
                .into());
            }
            *publication_counts.get_mut(index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication count row is unknown".into())
            })? = fragment.trace.samples.len();
            let previous = *publication_chord_offsets.last().ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication chord prefix is empty".into())
            })?;
            let edge_count = fragment.points.len().checked_sub(1).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication fragment has no endpoints".into())
            })?;
            publication_chord_offsets.push(
                previous
                    .checked_add(edge_count)
                    .ok_or(ResourceError::SizeOverflow)?,
            );
        }
        budget.charge_into_error(
            u64::try_from(publication_counts.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        if publication_counts != emission.publication.counts {
            return Err(GeometryErrorKind::Invalid(
                "Q publication sample counts disagree with their traces".into(),
            )
            .into());
        }
        budget.charge_into_error(
            u64::try_from(publication_counts.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut family_occurrences = vec![0_u32; publication_counts.len()];
        for sample in &emission.publication.samples {
            budget.charge_into_error(1)?;
            let family_recipe = sample.family.fragment_recipe().ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication sample has no fragment family".into())
            })?;
            let parent = family_recipe.0 as usize;
            let occurrence = family_occurrences.get_mut(parent).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication sample parent is unknown".into())
            })?;
            let source_trace_valid = match &sample.source_trace {
                None => true,
                Some(TraceRequestLocation::Edge { .. } | TraceRequestLocation::Root { .. }) => {
                    false
                },
                Some(TraceRequestLocation::Sample { recipe, row }) => {
                    let fragment = emission
                        .publication
                        .fragments
                        .get(recipe.0 as usize)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Q publication sample source trace is unknown".into(),
                            )
                        })?;
                    let trace_sample = fragment.trace.samples.get(row.index());
                    *recipe == family_recipe
                        && fragment.recipe == *recipe
                        && trace_sample.is_some_and(|trace_sample| {
                            trace_sample.request_row == *row
                                && trace_sample.written_vertex == sample.written_vertex
                                && trace_sample.parameter == sample.parameter
                                && trace_sample.has_source
                                && trace_sample.endpoint_only == sample.endpoint_only
                        })
                },
            };
            if sample.family.producer != super::emission::SampleProducerKind::Publication
                || sample.family.occurrence != *occurrence
                || !super::exact::in_unit_interval_with_work(&sample.parameter, budget)?
                || !source_trace_valid
            {
                return Err(GeometryErrorKind::Invalid(
                    "Q publication sample references an unknown producer or source ramp".into(),
                )
                .into());
            }
            *occurrence = occurrence
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
        }
        for (&occurrences, &count) in family_occurrences.iter().zip(&publication_counts) {
            budget.charge_into_error(1)?;
            if usize::try_from(occurrences).map_err(|_| ResourceError::SizeOverflow)? != count {
                return Err(GeometryErrorKind::Invalid(
                    "Q publication families do not cover their materialized samples".into(),
                )
                .into());
            }
        }
        budget.charge_into_error(
            u64::try_from(emission.fragment_recipes.len())
                .map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut seen_fragments = vec![false; emission.fragment_recipes.len()];
        for walk in emission.fragment_walks {
            budget.charge_into_error(1)?;
            match walk.owner {
                RecipeWalkOwner::Cycle(cycle) => {
                    emission.plan.cycles.get(cycle).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "Q recipe walk references an unknown cycle".into(),
                        )
                    })?;
                },
                RecipeWalkOwner::Line(line) => {
                    emission.plan.line_paths.get(line).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "Q recipe walk references an unknown line".into(),
                        )
                    })?;
                },
                RecipeWalkOwner::SourceRing(ring) => {
                    topology.sources.rings.get(ring).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "Q recipe walk references an unknown source ring".into(),
                        )
                    })?;
                },
            }
            for &recipe_id in &walk.fragments {
                budget.charge_into_error(1)?;
                let seen = seen_fragments
                    .get_mut(recipe_id.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q recipe walk has an unknown fragment".into())
                    })?;
                if std::mem::replace(seen, true) {
                    return Err(GeometryErrorKind::Invalid(
                        "Q fragment appears in more than one publication walk".into(),
                    )
                    .into());
                }
            }
        }
        for &seen in &seen_fragments {
            budget.charge_into_error(1)?;
            if !seen {
                return Err(GeometryErrorKind::Invalid(
                    "Q fragment is missing its publication walk".into(),
                )
                .into());
            }
        }
        for (index, recipe) in emission.fragment_recipes.iter().enumerate() {
            budget.charge_into_error(1)?;
            if recipe.id.0 as usize != index {
                return Err(GeometryErrorKind::Invalid(
                    "Q fragment recipes are not indexed by FragmentRecipeId".into(),
                )
                .into());
            }
            let [from_id, to_id] = recipe.endpoints;
            let from = q.ports.get(from_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q fragment recipe start is unknown".into())
            })?;
            let to = q.ports.get(to_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q fragment recipe end is unknown".into())
            })?;
            emission.source_nodes.source_node(from.node)?;
            emission.source_nodes.source_node(to.node)?;
            let reference_endpoints = recipe.reference_endpoints(topology, budget)?;
            let publication_fragment =
                emission.publication.fragments.get(index).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "Q fragment has no frozen publication reference".into(),
                    )
                })?;
            if !reference_endpoints[0]
                .try_eq_with_work(&publication_fragment.exact_endpoints[0], budget)?
                || !reference_endpoints[1]
                    .try_eq_with_work(&publication_fragment.exact_endpoints[1], budget)?
            {
                return Err(GeometryErrorKind::Invalid(
                    "Q publication reference differs from its exact recipe endpoints".into(),
                )
                .into());
            }
            let mut has_areal_owner = false;
            for range in &recipe.ranges {
                budget.charge_into_error(1)?;
                let edge = topology
                    .sources
                    .edges
                    .get(range.source_edge.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "Q fragment recipe source edge is unknown".into(),
                        )
                    })?;
                has_areal_owner |= matches!(edge.kind, SourceEdgeKind::Ring { .. });
                if edge.id != range.source_edge
                    || !super::exact::in_unit_interval_with_work(&range.range.start, budget)?
                    || !super::exact::in_unit_interval_with_work(&range.range.end, budget)?
                {
                    return Err(GeometryErrorKind::Invalid(
                        "Q fragment recipe has an invalid directed source range".into(),
                    )
                    .into());
                }
                for producer_parameter in [&range.producer_start, &range.producer_end] {
                    budget.charge_into_error(1)?;
                    if !super::exact::in_unit_interval_with_work(producer_parameter, budget)? {
                        return Err(GeometryErrorKind::Invalid(
                            "Q source range has an invalid affine producer parameter".into(),
                        )
                        .into());
                    }
                }
            }
            if matches!(
                recipe.use_kind,
                super::emission::SelectedUse::AreaBoundary
                    | super::emission::SelectedUse::ArealBoundaryLine
            ) && !has_areal_owner
            {
                return Err(GeometryErrorKind::Invalid(
                    "areal Q recipe lost its source-face ownership".into(),
                )
                .into());
            }
        }
        for planned in planned_area_rings(emission.plan) {
            budget.charge_into_error(1)?;
            let region = match *planned {
                PlannedAreaRing::Source { region, .. } | PlannedAreaRing::Cycle { region, .. } => {
                    region
                },
            };
            let mut low = 0_usize;
            let mut high = emission.selected_regions.len();
            let mut found = false;
            while low < high {
                budget.charge_into_error(1)?;
                let middle = low
                    .checked_add(high.checked_sub(low).ok_or(ResourceError::SizeOverflow)? / 2)
                    .ok_or(ResourceError::SizeOverflow)?;
                let fact = emission.selected_regions.get(middle).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q selected region index is unknown".into())
                })?;
                match fact.root.cmp(&region) {
                    Ordering::Less => {
                        low = middle.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
                    },
                    Ordering::Greater => high = middle,
                    Ordering::Equal => {
                        found = true;
                        break;
                    },
                }
            }
            if !found {
                return Err(GeometryErrorKind::Invalid(
                    "Q area emission references a region without retained pole facts".into(),
                )
                .into());
            }
        }
        budget.charge_into_error(
            u64::try_from(
                q.nodes
                    .len()
                    .checked_mul(3)
                    .ok_or(ResourceError::SizeOverflow)?,
            )
            .map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut exact_positions = Vec::with_capacity(q.nodes.len());
        let mut movable_axes = Vec::with_capacity(q.nodes.len());
        let mut stored_nodes = vec![false; q.nodes.len()];
        for &port_id in q
            .source_edge_ports
            .iter()
            .flatten()
            .chain(&q.source_point_ports)
        {
            budget.charge_into_error(1)?;
            let port = q.ports.get(port_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q stored-node port is unknown".into())
            })?;
            *stored_nodes
                .get_mut(port.node.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q stored node is unknown".into()))? =
                true;
        }
        for node in &q.nodes {
            budget.charge_into_error(1)?;
            let nearest_xy = nearest.xy(node.id)?;
            let is_stored = stored_nodes
                .get(node.id.0 as usize)
                .copied()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q node has no stored-node state".into())
                })?;
            let exact = if is_stored {
                None
            } else {
                match topology.canonical_node_key(node.id)? {
                    super::q::CanonicalQNodeKey::Chart(key) => {
                        exact_node_position_from_key_with_work(frame, key, budget)?
                    },
                    super::q::CanonicalQNodeKey::Cut { .. } => None,
                }
            };
            let axes = match &exact {
                Some(position) => inexact_axes_with_work(position, nearest_xy, budget)?,
                None => [false, false],
            };
            exact_positions.push(exact);
            movable_axes.push(axes);
        }
        let domains = candidate_domains(
            topology,
            emission,
            nearest,
            &exact_positions,
            &movable_axes,
            budget,
        )?;
        let cut_rows = build_q_cut_rows(topology, emission, budget)?;
        let chords = build_q_chords(topology, emission, nearest, &domains, budget)?;
        let fan_rows = build_q_fan_rows(topology, emission, budget)?;
        let period = ExactRational::from_f64(frame.longitude().period());
        let cut_east = q.cut.try_add_with_work(&period, budget)?;
        budget.charge_into_error(
            u64::try_from(q.nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut cut_index_by_node = vec![None; q.nodes.len()];
        for (cut_index, cut) in emission.cut_nodes.iter().enumerate() {
            budget.charge_into_error(1)?;
            let slot = cut_index_by_node
                .get_mut(cut.node.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q cut node is unknown".into()))?;
            if slot.replace(cut_index).is_some() {
                return Err(GeometryErrorKind::Invalid(
                    "Q embedding emission repeats a cut node".into(),
                )
                .into());
            }
        }
        let exact_faces = if emission.areal_spans.is_empty() {
            None
        } else {
            let exact_chart = subgraph_chart(topology, emission, nearest, None, budget)?;
            Some(QFaceStructure::build(topology, emission, exact_chart)?)
        };
        let source_spans = source_span_range_index(topology, budget)?;
        let BuiltQPublication {
            nodes: publication_nodes,
            sample_nodes: publication_sample_nodes,
            sample_source_ports: publication_sample_source_ports,
            chords: mut publication_chords,
            source_inventory,
            domains: published,
        } = build_q_publication_chords(
            topology,
            emission,
            nearest,
            &domains,
            &source_spans,
            budget,
        )?;
        if publication_sample_nodes.len() != emission.publication.samples.len() {
            return Err(GeometryErrorKind::Invalid(
                "publication sample-to-node mapping is incomplete".into(),
            )
            .into());
        }
        if publication_chord_offsets.last().copied() != Some(publication_chords.len()) {
            return Err(GeometryErrorKind::Invalid(
                "Q publication chord offsets do not cover their fragments".into(),
            )
            .into());
        }
        budget.charge_into_error(
            u64::try_from(ordinates.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut merged_ordinates = ordinates.to_vec();
        for (sample_index, &node_id) in publication_sample_nodes.iter().enumerate() {
            budget.charge_into_error(1)?;
            let node = publication_nodes
                .nodes()
                .get(node_id.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication ordinate node is unknown".into())
                })?;
            if let NodeAuthority::Source(source_node) = &node.authority {
                let sample_ordinates = emission
                    .publication
                    .ordinates
                    .get(sample_index)
                    .copied()
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q publication ordinate row is unknown".into())
                    })?;
                let source_ordinates = merged_ordinates
                    .get_mut(source_node.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "Q publication source ordinate node is unknown".into(),
                        )
                    })?;
                source_ordinates.merge(sample_ordinates);
            }
        }
        for chord in &mut publication_chords {
            budget.charge_into_error(1)?;
            for endpoint in &chord.endpoints {
                budget.charge_into_error(1)?;
                let PublishedEndpoint::Published { node, .. } = endpoint else {
                    continue;
                };
                let Some(exact) = published
                    .exact_positions
                    .get(node.0 as usize)
                    .and_then(Option::as_ref)
                else {
                    continue;
                };
                let candidates = published.domains.get(node.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "publication chord endpoint has no candidate domain".into(),
                    )
                })?;
                chord.radius = chord.radius.max(published_candidate_radius(
                    frame, exact, candidates, budget,
                )?);
            }
            let (boxes, image_radii) = chord_boxes(
                chord.nearest,
                chord.radius,
                frame.longitude().period(),
                budget,
            )?;
            chord.boxes = boxes;
            chord.image_radii = image_radii;
        }
        let publication_pairs = build_q_publication_pairs(
            &chords,
            &publication_chords,
            frame.longitude().period(),
            budget,
        )?;
        let prepared_producer =
            PreparedProducerWitness::build(topology, emission, &publication_chords, budget)?;
        let producer_source_dependencies = prepared_producer.source_dependencies(budget)?;
        budget.charge_into_error(
            u64::try_from(emission.live_nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut source_seeds = emission.live_nodes.clone();
        for chord in &chords {
            budget.charge_into_error(1)?;
            for &node in &chord.dependencies {
                budget.charge_into_error(1)?;
                source_seeds.push(node);
            }
        }
        for cut in &cut_rows {
            budget.charge_into_error(1)?;
            for &node in &cut.dependencies {
                budget.charge_into_error(1)?;
                source_seeds.push(node);
            }
        }
        for &node in &producer_source_dependencies {
            budget.charge_into_error(1)?;
            source_seeds.push(node);
        }
        for fan in &fan_rows {
            budget.charge_into_error(1)?;
            for &node in &fan.dependencies {
                budget.charge_into_error(1)?;
                source_seeds.push(node);
            }
        }
        for chord in &publication_chords {
            budget.charge_into_error(1)?;
            if !chord.generated {
                continue;
            }
            for &node in &chord.dependencies {
                budget.charge_into_error(1)?;
                source_seeds.push(node);
            }
            for node in chord.nodes {
                budget.charge_into_error(1)?;
                if let QGraphNode::Source(node) = node {
                    source_seeds.push(node);
                }
            }
        }
        try_sort_by_with_work(&mut source_seeds, budget, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        budget.charge_into_error(
            u64::try_from(source_seeds.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        source_seeds.dedup();
        let mut source_constructed = 0_usize;
        for &node in &source_seeds {
            budget.charge_into_error(1)?;
            let axes = movable_axes.get(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("prepared Q source node has no movability row".into())
            })?;
            if axes.iter().any(|&movable| movable) {
                source_constructed = source_constructed
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
            }
        }
        budget
            .budget_mut()
            .include_constructed(source_constructed)?;
        let QClosure {
            classes: source_classes,
            proximity_pairs,
            dependency_index,
        } = build_q_proximity_closure(
            topology,
            &chords,
            &domains,
            &cut_rows,
            &fan_rows,
            &source_seeds,
            &movable_axes,
            budget,
        )?;
        let UnifiedQClassBuild {
            source_variable_count: source_base,
            classes,
            node_location,
            publication_location,
            states: class_states,
        } = build_unified_q_classes(
            &UnifiedQClassInputs {
                topology,
                source_classes: &source_classes,
                publication_variables: &published.variables,
                publication_nodes: &publication_nodes,
                source_chords: &chords,
                publication_chords: &publication_chords,
                proximity_pairs: &proximity_pairs,
                cut_rows: &cut_rows,
                fan_rows: &fan_rows,
                publication_pairs: &publication_pairs,
                source_domains: &domains,
                source_movable_axes: &movable_axes,
                publication_domains: &published.domains,
                producer_witness: &prepared_producer,
            },
            budget,
        )?;
        budget.budget_mut().include_constructed(source_base)?;
        Ok(Self {
            topology,
            emission,
            family_state: QFamilyState::Shared {
                original_source_base: source_base,
            },
            ordinates: merged_ordinates,
            metric_access,
            declared_axes,
            strict_axes,
            operation,
            nearest: nearest.clone(),
            exact_positions,
            domains,
            cut_east,
            cut_index_by_node,
            cut_rows,
            chords,
            publication_nodes,
            publication_sample_nodes,
            publication_sample_source_ports,
            publication_chords,
            publication_chord_offsets,
            publication_pairs,
            prepared_producer,
            publication_exact_positions: published.exact_positions,
            publication_nearest: published.nearest,
            publication_domains: published.domains,
            publication_location,
            fan_rows,
            proximity_pairs,
            classes,
            node_location,
            class_states,
            dependency_index,
            live_nodes: &emission.live_nodes,
            source_inventory,
            exact_faces,
        })
    }

    fn certify_nearest(
        &mut self,
        live_keys: &[(EmittedNodeKey, QNodeId)],
        drawing: QEmissionDrawing<'topology>,
        budget: &mut impl SearchAccount,
    ) -> Result<NearestQCertificate> {
        budget.charge_into_error(
            u64::try_from(self.classes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut failed_classes = vec![false; self.classes.len()];
        let mut unrepresentable = false;
        let (publication_conflicts, publication_unrepresentable) = self
            .publication_identity_conflicts(
                &self.nearest,
                live_keys,
                &self.publication_nearest,
                budget,
            )?;
        unrepresentable |= publication_unrepresentable;
        for node in publication_conflicts {
            budget.charge_into_error(1)?;
            let location = match node {
                QVariableNode::Source(node) => {
                    self.node_location.get(node.0 as usize).copied().flatten()
                },
                QVariableNode::Published(node) => self
                    .publication_location
                    .get(node.0 as usize)
                    .copied()
                    .flatten(),
            };
            if let Some((class, _)) = location {
                *failed_classes.get_mut(class).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication conflict class is unknown".into())
                })? = true;
            } else {
                unrepresentable = true;
            }
        }
        let mut group_start = 0_usize;
        while group_start < live_keys.len() {
            budget.charge_into_error(1)?;
            let key = live_keys
                .get(group_start)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q live-key group is unknown".into()))?
                .0;
            let mut group_end = group_start
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            while let Some((candidate, _)) = live_keys.get(group_end) {
                budget.charge_into_error(1)?;
                if *candidate != key {
                    break;
                }
                group_end = group_end
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
            }
            let mut group_nodes = live_keys
                .get(group_start..group_end)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q live-key group is invalid".into()))?
                .iter()
                .map(|(_, node)| *node)
                .collect::<Vec<_>>();
            budget.charge_into_error(
                u64::try_from(group_nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            try_sort_by_with_work(&mut group_nodes, budget, |left, right, _work| {
                Ok(left.cmp(right))
            })?;
            group_nodes.dedup();
            if group_nodes.len() > 1 {
                let mut movable = false;
                for node in group_nodes {
                    budget.charge_into_error(1)?;
                    if let Some((class, _)) =
                        self.node_location.get(node.0 as usize).copied().flatten()
                    {
                        *failed_classes.get_mut(class).ok_or_else(|| {
                            GeometryErrorKind::Invalid("Q identity class is unknown".into())
                        })? = true;
                        movable = true;
                    }
                }
                if !movable {
                    unrepresentable = true;
                }
            }
            group_start = group_end;
        }
        for &node in &self.emission.live_nodes {
            budget.charge_into_error(1)?;
            let nearest_candidate = self.nearest.candidate(node)?;
            let domain = self.domains.get(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q live node has no candidate domain".into())
            })?;
            let mut contains_nearest = false;
            for &candidate in domain {
                budget.charge_into_error(1)?;
                if candidate == nearest_candidate {
                    contains_nearest = true;
                    break;
                }
            }
            if contains_nearest {
                continue;
            }
            if let Some((class, _)) = self.node_location.get(node.0 as usize).copied().flatten() {
                *failed_classes.get_mut(class).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q domain class is unknown".into())
                })? = true;
            } else {
                unrepresentable = true;
            }
        }
        for (node_index, node) in self.publication_nodes.nodes().iter().enumerate() {
            budget.charge_into_error(1)?;
            if !matches!(&node.authority, NodeAuthority::Computed(_)) {
                continue;
            }
            let nearest_candidate = self
                .publication_nearest
                .get(node_index)
                .copied()
                .flatten()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication node has no nearest candidate".into())
                })?;
            let domain = self.publication_domains.get(node_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication node has no candidate domain".into())
            })?;
            let mut contains_nearest = false;
            for &candidate in domain {
                budget.charge_into_error(1)?;
                if candidate == nearest_candidate {
                    contains_nearest = true;
                    break;
                }
            }
            if contains_nearest {
                continue;
            }
            if let Some((class, _)) = self.publication_location.get(node_index).copied().flatten() {
                *failed_classes.get_mut(class).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication domain class is unknown".into())
                })? = true;
            } else {
                unrepresentable = true;
            }
        }
        if unrepresentable {
            return Ok(NearestQCertificate::Unrepresentable);
        }

        for (class_index, variables) in self.classes.iter().enumerate() {
            budget.charge_into_error(
                u64::try_from(variables.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let values = variables
                .iter()
                .map(|&node| self.nearest_candidate(node))
                .collect::<Result<Vec<_>>>()?;
            if !failed_classes
                .get(class_index)
                .copied()
                .ok_or_else(|| GeometryErrorKind::Invalid("Q nearest class is unknown".into()))?
            {
                let state = self.class_states.get(class_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q nearest class state is unknown".into())
                })?;
                budget.charge_into_error(
                    u64::try_from(values.len()).map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                let assigned = values.iter().copied().map(Some).collect::<Vec<_>>();
                for rank in 0..variables.len() {
                    budget.charge_evaluations(1)?;
                    let prefix = assigned.get(..=rank).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q nearest prefix is out of range".into())
                    })?;
                    if !<Self as Problem>::check_local(
                        self,
                        state,
                        class_index,
                        variables,
                        rank,
                        prefix,
                        budget,
                    )? {
                        *failed_classes.get_mut(class_index).ok_or_else(|| {
                            GeometryErrorKind::Invalid("Q nearest class is unknown".into())
                        })? = true;
                        break;
                    }
                }
            }
        }

        let mut repair_needed = false;
        for failed in failed_classes {
            budget.charge_into_error(1)?;
            if failed {
                repair_needed = true;
            }
        }
        if repair_needed {
            return Ok(NearestQCertificate::SearchRequired);
        }

        self.charge_global_work(budget)?;
        let clone_entries = [
            self.nearest.node.len(),
            self.nearest.longitude_periods.len(),
            self.nearest.pole_port_longitude.len(),
            self.nearest.pole_port_periods.len(),
            self.publication_nearest.len(),
        ]
        .into_iter()
        .try_fold(0_usize, |count, entries| {
            count
                .checked_add(entries)
                .ok_or(ResourceError::SizeOverflow)
        })?;
        budget.charge_into_error(
            u64::try_from(clone_entries).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let nearest = self.nearest.clone();
        let publication_nearest = self.publication_nearest.clone();
        match self.check_global_candidate(&nearest, &publication_nearest, drawing, budget)? {
            GlobalCheck::Pass(mut output) => {
                output.family_state = self.take_native_families();
                Ok(NearestQCertificate::Pass(Box::new(output)))
            },
            GlobalCheck::Reject(classes) => {
                if classes.is_empty() {
                    Ok(NearestQCertificate::Unrepresentable)
                } else {
                    Ok(NearestQCertificate::SearchRequired)
                }
            },
        }
    }

    fn certify_and_search<A: SearchAccount>(
        mut self,
        live_keys: &[(EmittedNodeKey, QNodeId)],
        drawing: QEmissionDrawing<'topology>,
        budget: &mut A,
    ) -> Result<SearchOutcome<QEmbeddingPass>> {
        match self.certify_nearest(live_keys, drawing, budget)? {
            NearestQCertificate::Pass(output) => {
                let output = *output;
                #[cfg(test)]
                {
                    Ok(SearchOutcome::Pass(QEmbeddingPass {
                        output,
                        coordinates: self.nearest.clone(),
                        searched: false,
                    }))
                }
                #[cfg(not(test))]
                {
                    Ok(SearchOutcome::Pass(QEmbeddingPass { output }))
                }
            },
            NearestQCertificate::SearchRequired => search_q_embedding(self, budget),
            NearestQCertificate::Unrepresentable => Ok(SearchOutcome::Exhausted),
        }
    }

    fn publication_identity_conflicts(
        &self,
        coordinates: &QCoordinates,
        live_keys: &[(EmittedNodeKey, QNodeId)],
        publication_coordinates: &[Option<QCandidate>],
        budget: &mut impl Work,
    ) -> Result<(Vec<QVariableNode>, bool)> {
        if self.emission.publication.samples.is_empty() {
            return Ok((Vec::new(), false));
        }
        let frame = self.topology.sources.frame;
        let nodes = &self.publication_nodes;
        if self.publication_sample_nodes.len() != self.emission.publication.samples.len() {
            return Err(GeometryErrorKind::Invalid(
                "Q publication sample node table is incomplete".into(),
            )
            .into());
        }
        let live_count = live_keys
            .len()
            .checked_add(self.emission.publication.samples.len())
            .ok_or(ResourceError::SizeOverflow)?;
        budget.charge_into_error(
            u64::try_from(live_count).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut live = Vec::with_capacity(live_count);
        for &(key, node) in live_keys {
            budget.charge_into_error(1)?;
            live.push((key, nodes.source_node(node)?));
        }
        for sample_index in 0..self.emission.publication.samples.len() {
            budget.charge_into_error(1)?;
            let node = *self
                .publication_sample_nodes
                .get(sample_index)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication sample node is unknown".into())
                })?;
            let key = match &nodes
                .nodes()
                .get(node.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication node identity is unknown".into())
                })?
                .authority
            {
                NodeAuthority::Source(source_node) => {
                    emitted_node_key(frame, coordinates.xy(*source_node)?)?
                },
                NodeAuthority::Computed(_) => {
                    let candidate = publication_coordinates
                        .get(node.0 as usize)
                        .copied()
                        .flatten()
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Q publication identity has no assigned native site".into(),
                            )
                        })?;
                    emitted_node_key(frame, candidate.native)?
                },
            };
            live.push((key, node));
        }

        let mut conflicting_nodes = Vec::new();
        let mut unrepresentable = false;
        try_sort_by_with_work(&mut live, budget, |left, right, _work| {
            Ok(left.1.cmp(&right.1).then(left.0.cmp(&right.0)))
        })?;
        for pair in live.windows(2) {
            budget.charge_into_error(1)?;
            let [(left_key, left_node), (right_key, right_node)] = pair else {
                unreachable!("windows(2) contains two publication node rows");
            };
            if left_node != right_node || left_key == right_key {
                continue;
            }
            match &nodes
                .nodes()
                .get(left_node.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication node identity is unknown".into())
                })?
                .authority
            {
                NodeAuthority::Source(source) => {
                    conflicting_nodes.push(QVariableNode::Source(*source));
                },
                NodeAuthority::Computed(_) => {
                    conflicting_nodes.push(QVariableNode::Published(*left_node));
                },
            }
        }

        try_sort_by_with_work(&mut live, budget, |left, right, _work| Ok(left.cmp(right)))?;
        let mut start = 0_usize;
        while start < live.len() {
            budget.charge_into_error(1)?;
            let key = live
                .get(start)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q publication key is unknown".into()))?
                .0;
            let mut end = start.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            while let Some((candidate, _)) = live.get(end) {
                budget.charge_into_error(1)?;
                if *candidate != key {
                    break;
                }
                end = end.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            }
            let group = live.get(start..end).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication key group is invalid".into())
            })?;
            budget.charge_into_error(
                u64::try_from(group.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let mut distinct = group.iter().map(|(_, node)| *node).collect::<Vec<_>>();
            try_sort_by_with_work(&mut distinct, budget, |left, right, _work| {
                Ok(left.cmp(right))
            })?;
            budget.charge_into_error(
                u64::try_from(distinct.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            distinct.dedup();
            if distinct.len() > 1 {
                let mut has_source = false;
                let mut computed_count = 0_usize;
                for node in distinct {
                    budget.charge_into_error(1)?;
                    match &nodes
                        .nodes()
                        .get(node.0 as usize)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Q publication collision identity is unknown".into(),
                            )
                        })?
                        .authority
                    {
                        NodeAuthority::Source(source) => {
                            has_source = true;
                            conflicting_nodes.push(QVariableNode::Source(*source));
                        },
                        NodeAuthority::Computed(_) => {
                            conflicting_nodes.push(QVariableNode::Published(node));
                            computed_count = computed_count
                                .checked_add(1)
                                .ok_or(ResourceError::SizeOverflow)?;
                        },
                    }
                }
                if !has_source || computed_count > 1 {
                    unrepresentable = true;
                }
            }
            start = end;
        }
        try_sort_by_with_work(&mut conflicting_nodes, budget, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        budget.charge_into_error(
            u64::try_from(conflicting_nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        conflicting_nodes.dedup();
        Ok((conflicting_nodes, unrepresentable))
    }

    fn nearest_candidate(&self, node: QVariableNode) -> Result<QCandidate> {
        match node {
            QVariableNode::Source(node) => self.nearest.candidate(node),
            QVariableNode::Published(node) => self
                .publication_nearest
                .get(node.0 as usize)
                .copied()
                .flatten()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "Q publication variable has no nearest candidate".into(),
                    )
                    .into()
                }),
        }
    }

    fn charge_global_work(&self, budget: &mut impl SearchAccount) -> Result<()> {
        if self.exact_faces.is_some() {
            let item_count = self
                .emission
                .spans
                .len()
                .checked_add(self.emission.point_nodes.len())
                .ok_or(ResourceError::SizeOverflow)?;
            budget.charge_evaluations(
                u64::try_from(item_count.div_ceil(16)).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
        }
        let mut ring_edge_count = 0_usize;
        for ring in planned_area_rings(self.emission.plan) {
            let edge_count = match ring {
                PlannedAreaRing::Cycle { cycle, .. } => self
                    .emission
                    .plan
                    .cycles
                    .get(*cycle)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q area ring references an unknown cycle".into())
                    })?
                    .darts
                    .len(),
                PlannedAreaRing::Source { ring, .. } => self
                    .topology
                    .sources
                    .rings
                    .get(*ring)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q source ring role is unknown".into())
                    })?
                    .source_edges
                    .len(),
            };
            ring_edge_count = ring_edge_count
                .checked_add(edge_count)
                .ok_or(ResourceError::SizeOverflow)?;
        }
        let total = ring_edge_count
            .checked_add(self.publication_pairs.len())
            .ok_or(ResourceError::SizeOverflow)?;
        budget.charge_evaluations(u64::try_from(total).map_err(|_| ResourceError::SizeOverflow)?)
    }

    fn global_q_port_points(
        &self,
        coordinates: &QCoordinates,
        budget: &mut impl Work,
    ) -> Result<Vec<Option<ExactChartPoint>>> {
        let q = &self.topology.q;
        budget.charge_into_error(
            u64::try_from(q.ports.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut port_points = vec![None; q.ports.len()];
        for &(_, port_id) in &self.emission.selected_ports {
            budget.charge_into_error(1)?;
            let port = q
                .ports
                .get(port_id.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q selected port is unknown".into()))?;
            let point = if port.cut {
                port.point.clone()
            } else {
                let assigned = q_port_assigned_point(
                    self.topology,
                    &self.nearest,
                    coordinates,
                    port_id,
                    budget,
                )?;
                align_native_chart_lift(
                    self.topology.sources.frame,
                    &assigned,
                    &port.point,
                    budget,
                )?
            };
            *port_points.get_mut(port_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q selected port index is invalid".into())
            })? = Some(point);
        }
        reconstruct_cut_ports(
            self.topology.sources.frame,
            q,
            &self.emission.cut_nodes,
            &mut port_points,
            &self.cut_east,
            budget,
        )?;
        Ok(port_points)
    }

    fn publication_candidate_point(
        &self,
        endpoint: &PublishedEndpoint,
        reference: &ExactChartPoint,
        reference_offset: &ExactRational,
        port_points: &[Option<ExactChartPoint>],
        publication_coordinates: &[Option<QCandidate>],
        budget: &mut impl Work,
    ) -> Result<ExactChartPoint> {
        budget.charge_into_error(1)?;
        let assigned = match endpoint {
            PublishedEndpoint::Source { port, sample, .. } => {
                if let Some(sample) = sample
                    && let Some(sample_index) = sample.publication_sample
                    && self
                        .publication_sample_source_ports
                        .get(sample_index)
                        .copied()
                        .flatten()
                        != Some(*port)
                {
                    return Err(GeometryErrorKind::Invalid(
                        "source publication occurrence changed its Q-port authority".into(),
                    )
                    .into());
                }
                port_points
                    .get(port.0 as usize)
                    .and_then(Option::as_ref)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "Q publication source port has no assigned chart point".into(),
                        )
                    })?
                    .try_clone_with_work(budget)?
            },
            PublishedEndpoint::Published { node, sample } => {
                if self.publication_sample_nodes.get(*sample) != Some(node) {
                    return Err(GeometryErrorKind::Invalid(
                        "computed publication occurrence changed its node authority".into(),
                    )
                    .into());
                }
                let candidate = publication_coordinates
                    .get(node.0 as usize)
                    .copied()
                    .flatten()
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "computed publication node has no assigned candidate".into(),
                        )
                    })?;
                candidate_chart_point_with_work(self.topology.sources.frame, candidate, budget)?
            },
        };
        self.assigned_publication_endpoint_point(
            endpoint,
            reference,
            reference_offset,
            &assigned,
            budget,
        )
    }

    fn assigned_publication_endpoint_point(
        &self,
        endpoint: &PublishedEndpoint,
        reference: &ExactChartPoint,
        reference_offset: &ExactRational,
        assigned: &ExactChartPoint,
        budget: &mut impl Work,
    ) -> Result<ExactChartPoint> {
        let sample_index = match endpoint {
            PublishedEndpoint::Source { sample, .. } => {
                sample.as_ref().and_then(|sample| sample.publication_sample)
            },
            PublishedEndpoint::Published { sample, .. } => Some(*sample),
        };
        if let Some(sample_index) = sample_index {
            let sample = self
                .emission
                .publication
                .samples
                .get(sample_index)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("publication endpoint sample is unknown".into())
                })?;
            if !sample.exact_point.try_eq_with_work(reference, budget)? {
                return Err(GeometryErrorKind::Invalid(
                    "publication endpoint changed its frozen sample reference".into(),
                )
                .into());
            }
        }
        lift_assigned_publication_point(assigned, reference_offset, budget)
    }

    fn publication_graph_rejections(
        &self,
        producer: &AssignedProducerProjection,
        budget: &mut impl Work,
    ) -> Result<Vec<QVariableNode>> {
        if self.publication_pairs.is_empty() {
            return Ok(Vec::new());
        }
        let q = &self.topology.q;
        let port_points = &producer.source_ports;
        let period = ExactRational::from_f64(self.topology.sources.frame.longitude().period());
        let mut rejected = Vec::<QVariableNode>::new();
        for pair in &self.publication_pairs {
            budget.charge_into_error(1)?;
            let (left_nodes, left_exact, left_candidate, left_dependencies) = match pair.left {
                QPublicationChordId::Source(index) => {
                    let chord = self.chords.get(index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q publication source chord is unknown".into())
                    })?;
                    budget.charge_into_error(
                        u64::try_from(chord.dependencies.len())
                            .map_err(|_| ResourceError::SizeOverflow)?,
                    )?;
                    let [start_port, end_port] = chord.ports;
                    let exact = [
                        q.ports
                            .get(start_port.0 as usize)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid("Q source chord port is unknown".into())
                            })?
                            .point
                            .clone(),
                        q.ports
                            .get(end_port.0 as usize)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid("Q source chord port is unknown".into())
                            })?
                            .point
                            .clone(),
                    ];
                    let candidate = [
                        port_points
                            .get(start_port.0 as usize)
                            .and_then(Option::as_ref)
                            .cloned()
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "Q source chord has no assigned start".into(),
                                )
                            })?,
                        port_points
                            .get(end_port.0 as usize)
                            .and_then(Option::as_ref)
                            .cloned()
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "Q source chord has no assigned end".into(),
                                )
                            })?,
                    ];
                    (
                        chord.nodes.map(QGraphNode::Source),
                        exact,
                        candidate,
                        chord.dependencies.clone(),
                    )
                },
                QPublicationChordId::Published(index) => {
                    let chord = self.publication_chords.get(index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q publication chord is unknown".into())
                    })?;
                    let leaf = producer.leaves.get(index).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "Q publication pair has no assigned producer leaf".into(),
                        )
                    })?;
                    if leaf.recipe != chord.recipe || leaf.trace_edge != chord.trace_edge {
                        return Err(GeometryErrorKind::Invalid(
                            "Q publication pair changed its assigned leaf identity".into(),
                        )
                        .into());
                    }
                    budget.charge_into_error(
                        u64::try_from(chord.dependencies.len())
                            .map_err(|_| ResourceError::SizeOverflow)?,
                    )?;
                    (
                        chord.nodes,
                        chord.exact.clone(),
                        leaf.points.clone(),
                        chord.dependencies.clone(),
                    )
                },
            };
            let (right_nodes, right_exact, right_candidate, right_dependencies) = match pair.right {
                QPublicationChordId::Source(index) => {
                    let chord = self.chords.get(index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q publication source chord is unknown".into())
                    })?;
                    budget.charge_into_error(
                        u64::try_from(chord.dependencies.len())
                            .map_err(|_| ResourceError::SizeOverflow)?,
                    )?;
                    let [start_port, end_port] = chord.ports;
                    let exact = [
                        q.ports
                            .get(start_port.0 as usize)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid("Q source chord port is unknown".into())
                            })?
                            .point
                            .clone(),
                        q.ports
                            .get(end_port.0 as usize)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid("Q source chord port is unknown".into())
                            })?
                            .point
                            .clone(),
                    ];
                    let candidate = [
                        port_points
                            .get(start_port.0 as usize)
                            .and_then(Option::as_ref)
                            .cloned()
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "Q source chord has no assigned start".into(),
                                )
                            })?,
                        port_points
                            .get(end_port.0 as usize)
                            .and_then(Option::as_ref)
                            .cloned()
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "Q source chord has no assigned end".into(),
                                )
                            })?,
                    ];
                    (
                        chord.nodes.map(QGraphNode::Source),
                        exact,
                        candidate,
                        chord.dependencies.clone(),
                    )
                },
                QPublicationChordId::Published(index) => {
                    let chord = self.publication_chords.get(index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q publication chord is unknown".into())
                    })?;
                    let leaf = producer.leaves.get(index).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "Q publication pair has no assigned producer leaf".into(),
                        )
                    })?;
                    if leaf.recipe != chord.recipe || leaf.trace_edge != chord.trace_edge {
                        return Err(GeometryErrorKind::Invalid(
                            "Q publication pair changed its assigned leaf identity".into(),
                        )
                        .into());
                    }
                    budget.charge_into_error(
                        u64::try_from(chord.dependencies.len())
                            .map_err(|_| ResourceError::SizeOverflow)?,
                    )?;
                    (
                        chord.nodes,
                        chord.exact.clone(),
                        leaf.points.clone(),
                        chord.dependencies.clone(),
                    )
                },
            };
            let right_exact = [
                translated_exact_point_with_work(
                    &right_exact[0],
                    pair.right_shift,
                    &period,
                    budget,
                )?,
                translated_exact_point_with_work(
                    &right_exact[1],
                    pair.right_shift,
                    &period,
                    budget,
                )?,
            ];
            let right_candidate = [
                translated_exact_point_with_work(
                    &right_candidate[0],
                    pair.right_shift,
                    &period,
                    budget,
                )?,
                translated_exact_point_with_work(
                    &right_candidate[1],
                    pair.right_shift,
                    &period,
                    budget,
                )?,
            ];
            if !pair_preserves_incidence_with_work(
                left_nodes,
                &left_exact,
                &left_candidate,
                right_nodes,
                &right_exact,
                &right_candidate,
                budget,
            )? {
                budget.charge_into_error(
                    u64::try_from(
                        left_dependencies
                            .len()
                            .checked_add(right_dependencies.len())
                            .ok_or(ResourceError::SizeOverflow)?
                            .checked_add(4)
                            .ok_or(ResourceError::SizeOverflow)?,
                    )
                    .map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                rejected.extend(left_dependencies.into_iter().map(QVariableNode::Source));
                rejected.extend(right_dependencies.into_iter().map(QVariableNode::Source));
                rejected.extend(left_nodes.map(|node| match node {
                    QGraphNode::Source(node) => QVariableNode::Source(node),
                    QGraphNode::Published(node) => QVariableNode::Published(node),
                }));
                rejected.extend(right_nodes.map(|node| match node {
                    QGraphNode::Source(node) => QVariableNode::Source(node),
                    QGraphNode::Published(node) => QVariableNode::Published(node),
                }));
            }
        }
        try_sort_by_with_work(&mut rejected, budget, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        budget.charge_into_error(
            u64::try_from(rejected.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        rejected.dedup();
        Ok(rejected)
    }

    fn certify_assigned_producer(
        &self,
        coordinates: &QCoordinates,
        publication_coordinates: &[Option<QCandidate>],
        source_faces: Option<&QFaceStructure>,
        budget: &mut impl Work,
    ) -> Result<AssignedProducerProjection> {
        let q = &self.topology.q;
        budget.charge_into_error(
            u64::try_from(self.publication_chords.len())
                .map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let port_points = self.global_q_port_points(coordinates, budget)?;
        let mut leaves = Vec::with_capacity(self.publication_chords.len());
        let mut rejected = Vec::new();
        for chord in &self.publication_chords {
            budget.charge_into_error(1)?;
            let points = [
                self.publication_candidate_point(
                    &chord.endpoints[0],
                    &chord.exact[0],
                    &chord.reference_offsets[0],
                    &port_points,
                    publication_coordinates,
                    budget,
                )?,
                self.publication_candidate_point(
                    &chord.endpoints[1],
                    &chord.exact[1],
                    &chord.reference_offsets[1],
                    &port_points,
                    publication_coordinates,
                    budget,
                )?,
            ];
            if (points[0] == points[1]) != (chord.exact[0] == chord.exact[1]) {
                budget.charge_into_error(
                    u64::try_from(
                        chord
                            .dependencies
                            .len()
                            .checked_add(2)
                            .ok_or(ResourceError::SizeOverflow)?,
                    )
                    .map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                rejected.extend(
                    chord
                        .dependencies
                        .iter()
                        .copied()
                        .map(QVariableNode::Source),
                );
                for node in chord.nodes {
                    rejected.push(match node {
                        QGraphNode::Source(node) => QVariableNode::Source(node),
                        QGraphNode::Published(node) => QVariableNode::Published(node),
                    });
                }
            }
            leaves.push(AssignedProducerLeaf {
                recipe: chord.recipe,
                trace_edge: chord.trace_edge,
                points,
            });
        }
        if let Some(source_faces) = source_faces {
            budget.charge_into_error(
                u64::try_from(source_faces.outgoing_nodes.len())
                    .map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let mut local_by_node = BTreeMap::new();
            for (local, &node) in source_faces.outgoing_nodes.iter().enumerate() {
                budget.charge_into_error(1)?;
                if local_by_node.insert(node, local).is_some() {
                    return Err(GeometryErrorKind::Invalid(
                        "assigned producer face projection repeats a source node".into(),
                    )
                    .into());
                }
            }
            for point in &self.prepared_producer.points {
                budget.charge_into_error(1)?;
                let Some(&local) = local_by_node.get(&point.node) else {
                    if point.host.is_some() {
                        budget.charge_into_error(
                            u64::try_from(point.dependencies.len())
                                .map_err(|_| ResourceError::SizeOverflow)?,
                        )?;
                        rejected.extend_from_slice(&point.dependencies);
                    }
                    continue;
                };
                let assigned_host = source_faces
                    .global
                    .source_node_faces
                    .get(local)
                    .copied()
                    .flatten();
                if point.host.is_some() && assigned_host.is_none() {
                    budget.charge_into_error(
                        u64::try_from(point.dependencies.len())
                            .map_err(|_| ResourceError::SizeOverflow)?,
                    )?;
                    rejected.extend_from_slice(&point.dependencies);
                }
            }
            for junction in &self.prepared_producer.junctions {
                let Some(_host) = junction.isolated_host else {
                    continue;
                };
                budget.charge_into_error(1)?;
                let assigned_host = local_by_node
                    .get(&junction.node)
                    .and_then(|local| source_faces.global.source_node_faces.get(*local))
                    .copied()
                    .flatten();
                if assigned_host.is_none() {
                    budget.charge_into_error(
                        u64::try_from(junction.dependencies.len())
                            .map_err(|_| ResourceError::SizeOverflow)?,
                    )?;
                    rejected.extend_from_slice(&junction.dependencies);
                }
            }
        } else {
            for linework in &self.prepared_producer.linework {
                budget.charge_into_error(1)?;
                if linework.use_kind == SelectedUse::ArealBoundaryLine {
                    return Err(GeometryErrorKind::Invalid(
                        "assigned areal-line producer has no source-face projection".into(),
                    )
                    .into());
                }
            }
        }
        for junction in &self.prepared_producer.junctions {
            budget.charge_into_error(1)?;
            if junction.producer_germs.is_empty() {
                continue;
            }
            let mut rays_by_port =
                BTreeMap::<QPortId, BTreeMap<QDartId, AssignedProducerFanRayBuilder>>::new();
            let mut candidate_mismatch = false;
            for germ in &junction.producer_germs {
                budget.charge_into_error(1)?;
                let chord = self.publication_chords.get(germ.chord).ok_or_else(|| {
                    GeometryErrorKind::Invalid("assigned producer fan chord is unknown".into())
                })?;
                let leaf = leaves.get(germ.chord).ok_or_else(|| {
                    GeometryErrorKind::Invalid("assigned producer fan leaf is unknown".into())
                })?;
                if leaf.recipe != chord.recipe || leaf.trace_edge != chord.trace_edge {
                    return Err(GeometryErrorKind::Invalid(
                        "assigned producer leaf projection lost its trace identity".into(),
                    )
                    .into());
                }
                let other_endpoint = match germ.endpoint {
                    0 => 1,
                    1 => 0,
                    _ => {
                        return Err(GeometryErrorKind::Invalid(
                            "assigned producer fan endpoint is outside its chord".into(),
                        )
                        .into());
                    },
                };
                let origin = leaf.points.get(germ.endpoint).cloned().ok_or_else(|| {
                    GeometryErrorKind::Invalid("assigned producer fan endpoint is unknown".into())
                })?;
                let target = leaf.points.get(other_endpoint).cloned().ok_or_else(|| {
                    GeometryErrorKind::Invalid("assigned producer fan target is unknown".into())
                })?;
                if origin == target {
                    let chord_exact = chord.exact.get(germ.endpoint).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "assigned producer reference endpoint is unknown".into(),
                        )
                    })?;
                    let target_exact = chord.exact.get(other_endpoint).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "assigned producer reference target is unknown".into(),
                        )
                    })?;
                    if chord_exact != target_exact {
                        candidate_mismatch = true;
                        break;
                    }
                    continue;
                }
                let producer_parameter = if germ.endpoint == 0 {
                    ExactRational::zero()
                } else {
                    ExactRational::one()
                };
                for &range_index in &germ.source_ranges {
                    budget.charge_into_error(1)?;
                    let range = chord.source_ranges.get(range_index).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "assigned producer fan source range is unknown".into(),
                        )
                    })?;
                    if !producer_range_touches_node(
                        range,
                        &producer_parameter,
                        junction.node,
                        budget,
                    )? {
                        return Err(GeometryErrorKind::Invalid(
                            "assigned producer fan lost its original junction range".into(),
                        )
                        .into());
                    }
                    let Some((port, dart)) = producer_range_fan_dart(
                        q,
                        range,
                        &producer_parameter,
                        junction.node,
                        budget,
                    )?
                    else {
                        return Err(GeometryErrorKind::Invalid(
                            "assigned producer fan range has no directed source germ".into(),
                        )
                        .into());
                    };
                    let selected_root = junction
                        .face_germs
                        .iter()
                        .find(|face_germ| face_germ.dart == dart)
                        .map(|face_germ| face_germ.selected_root)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "assigned producer fan source dart is absent from its node fan"
                                    .into(),
                            )
                        })?;
                    let source_point = port_points
                        .get(port.0 as usize)
                        .and_then(Option::as_ref)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "assigned producer fan source port has no candidate point".into(),
                            )
                        })?;
                    let source_key = quotient_key_for_chart_position_with_work(
                        self.topology.sources.frame,
                        source_point,
                        budget,
                    )?;
                    let origin_key = quotient_key_for_chart_position_with_work(
                        self.topology.sources.frame,
                        &origin,
                        budget,
                    )?;
                    if !compare_quotient_keys_with_work(&source_key, &origin_key, budget)?.is_eq() {
                        candidate_mismatch = true;
                        break;
                    }
                    let target_dart = q.darts.get(dart.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "assigned producer fan has an unknown target dart".into(),
                        )
                    })?;
                    let target_source_point = port_points
                        .get(target_dart.to_port.0 as usize)
                        .and_then(Option::as_ref)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "assigned producer fan target port has no candidate point".into(),
                            )
                        })?;
                    let longitude = self.topology.sources.frame.longitude();
                    let (_, source_lift) = split_rational_native_site_with_work(
                        &target_source_point.x,
                        longitude.period(),
                        longitude.half_period(),
                        budget,
                    )?;
                    let (_, output_lift) = split_rational_native_site_with_work(
                        &target.x,
                        longitude.period(),
                        longitude.half_period(),
                        budget,
                    )?;
                    let lift_delta = source_lift.try_subtract_with_work(output_lift, budget)?;
                    let longitude_shift = ExactRational::from_f64(longitude.period())
                        .try_product_with_work(&ExactRational::from_dyadic(lift_delta), budget)?;
                    let mut aligned_target = target.clone();
                    aligned_target.x = aligned_target
                        .x
                        .try_add_with_work(&longitude_shift, budget)?;
                    budget.charge_into_error(1)?;
                    let ray = rays_by_port
                        .entry(port)
                        .or_default()
                        .entry(dart)
                        .or_insert_with(|| AssignedProducerFanRayBuilder {
                            selected_root,
                            targets: Vec::new(),
                        });
                    if ray.selected_root != selected_root {
                        return Err(GeometryErrorKind::Invalid(
                            "assigned producer fan changed a selected source root".into(),
                        )
                        .into());
                    }
                    ray.targets.push(aligned_target);
                }
                if candidate_mismatch {
                    break;
                }
            }
            if candidate_mismatch {
                budget.charge_into_error(
                    u64::try_from(junction.dependencies.len())
                        .map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                rejected.extend_from_slice(&junction.dependencies);
                continue;
            }

            for (port, darts) in rays_by_port {
                budget.charge_into_error(1)?;
                let origin = port_points
                    .get(port.0 as usize)
                    .and_then(Option::as_ref)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "assigned producer fan port has no candidate point".into(),
                        )
                    })?;
                let represented_darts = darts.keys().copied().collect::<BTreeSet<_>>();
                budget.charge_into_error(
                    u64::try_from(
                        represented_darts
                            .len()
                            .checked_add(1)
                            .ok_or(ResourceError::SizeOverflow)?,
                    )
                    .map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                let mut points = NativeReferencePoints {
                    exact: Vec::with_capacity(
                        represented_darts
                            .len()
                            .checked_add(1)
                            .ok_or(ResourceError::SizeOverflow)?,
                    ),
                    rounded: Vec::with_capacity(
                        represented_darts
                            .len()
                            .checked_add(1)
                            .ok_or(ResourceError::SizeOverflow)?,
                    ),
                };
                points.exact.push(origin.clone());
                points.rounded.push(XY::new(
                    origin.x.round_with_work(budget)?,
                    origin.y.round_with_work(budget)?,
                ));
                let mut candidate_rays = Vec::with_capacity(represented_darts.len());
                for (dart, ray) in darts {
                    budget.charge_into_error(1)?;
                    let target = ray.targets.first().ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "assigned producer fan dart has no written leaf".into(),
                        )
                    })?;
                    if target == origin {
                        candidate_mismatch = true;
                        break;
                    }
                    for other in ray.targets.iter().skip(1) {
                        budget.charge_into_error(1)?;
                        if !exact_departures_share_forward_ray(origin, target, other, budget)? {
                            candidate_mismatch = true;
                            break;
                        }
                    }
                    if candidate_mismatch {
                        break;
                    }
                    let point_index = u32::try_from(points.exact.len())
                        .map_err(|_| ResourceError::SizeOverflow)?;
                    points.exact.push(target.clone());
                    points.rounded.push(XY::new(
                        target.x.round_with_work(budget)?,
                        target.y.round_with_work(budget)?,
                    ));
                    candidate_rays.push((dart, point_index, target.clone()));
                }
                if candidate_mismatch {
                    break;
                }
                let mut expected_darts = Vec::new();
                for germ in &junction.face_germs {
                    budget.charge_into_error(1)?;
                    let dart = q.darts.get(germ.dart.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "assigned producer fan has an unknown source dart".into(),
                        )
                    })?;
                    if dart.from_port == port && represented_darts.contains(&germ.dart) {
                        expected_darts.push(germ.dart);
                    }
                }
                if expected_darts.len() != candidate_rays.len() {
                    return Err(GeometryErrorKind::Invalid(
                        "assigned producer fan projection is not reciprocal with the source fan"
                            .into(),
                    )
                    .into());
                }
                if !assigned_producer_fan_order_matches(
                    &points,
                    origin,
                    &mut candidate_rays,
                    &expected_darts,
                    budget,
                )? {
                    candidate_mismatch = true;
                    break;
                }
            }
            if candidate_mismatch {
                budget.charge_into_error(
                    u64::try_from(junction.dependencies.len())
                        .map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                rejected.extend_from_slice(&junction.dependencies);
            }
        }
        try_sort_by_with_work(&mut rejected, budget, |left, right, work| {
            work.charge_into_error(1)?;
            Ok(left.cmp(right))
        })?;
        budget.charge_into_error(
            u64::try_from(rejected.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        rejected.dedup();
        Ok(AssignedProducerProjection {
            source_ports: port_points,
            leaves,
            rejections: rejected,
        })
    }

    fn source_reused_recipe_ids(
        &self,
        drawing: &QEmissionDrawing<'topology>,
        ring_roles: &[bool],
        coordinates: &QCoordinates,
        publication_coordinates: &[Option<QCandidate>],
        budget: &mut impl Work,
    ) -> Result<BTreeSet<FragmentRecipeId>> {
        let walks = super::emission::index_recipe_walks(self.emission.fragment_walks, budget)?;
        let mut cursors = BTreeMap::new();
        let mut reused = BTreeSet::new();
        match &drawing.polygons {
            DrawnPolygons::WholeSphere(polygons) => {
                for polygon in polygons {
                    for ring in polygon {
                        budget.charge_into_error(1)?;
                        let walk = super::emission::next_recipe_walk(
                            &walks,
                            &mut cursors,
                            ring.recipe_walk,
                            budget,
                        )?;
                        let RecipeWalkOwner::SourceRing(ring_id) = ring.recipe_walk else {
                            return Err(GeometryErrorKind::Invalid(
                                "whole-sphere source polygon has a non-source recipe owner".into(),
                            )
                            .into());
                        };
                        let source_ring =
                            self.topology.sources.rings.get(ring_id).ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "whole-sphere source reuse ring is unknown".into(),
                                )
                            })?;
                        if source_ring.hole != ring.hole {
                            return Err(GeometryErrorKind::Invalid(
                                "whole-sphere source reuse changed its shell/hole role".into(),
                            )
                            .into());
                        }
                        if source_ring_reuse_matches(
                            self.topology,
                            source_ring,
                            false,
                            &ring.vertices,
                            self.emission.fragment_recipes,
                            walk,
                        )? && self.source_q_vertices_are_unchanged(
                            &ring.vertices,
                            coordinates,
                            budget,
                        )? && self.source_walk_publication_is_unchanged(
                            walk,
                            coordinates,
                            publication_coordinates,
                            budget,
                        )? {
                            add_source_reused_recipe_walk(
                                walk,
                                ring.vertices.len().saturating_sub(1),
                                self.emission.fragment_recipes,
                                self.emission.publication,
                                &mut reused,
                                budget,
                            )?;
                        }
                    }
                }
            },
            DrawnPolygons::Regions { rings, .. } => {
                for (ring_index, ring) in rings.iter().enumerate() {
                    budget.charge_into_error(1)?;
                    let walk = super::emission::next_recipe_walk(
                        &walks,
                        &mut cursors,
                        ring.recipe_walk,
                        budget,
                    )?;
                    if !matches!(ring.origin, DrawnAreaOrigin::Source) {
                        continue;
                    }
                    let RecipeWalkOwner::SourceRing(ring_id) = ring.recipe_walk else {
                        return Err(GeometryErrorKind::Invalid(
                            "source area ring has a non-source recipe owner".into(),
                        )
                        .into());
                    };
                    let source_ring =
                        self.topology.sources.rings.get(ring_id).ok_or_else(|| {
                            GeometryErrorKind::Invalid("source area reuse ring is unknown".into())
                        })?;
                    let is_hole = *ring_roles.get(ring_index).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "source area reuse has no certified shell/hole role".into(),
                        )
                    })?;
                    let planned = match &self.emission.plan.polygons {
                        PolygonPlan::Regions { rings, .. } => rings.get(ring_index).copied(),
                        PolygonPlan::WholeSphere(_) => None,
                    };
                    let Some(PlannedAreaRing::Source {
                        ring: planned_ring,
                        region: planned_region,
                        reverse,
                        ..
                    }) = planned
                    else {
                        return Err(GeometryErrorKind::Invalid(
                            "source drawing has no matching source area plan".into(),
                        )
                        .into());
                    };
                    if source_ring.hole ^ reverse != is_hole {
                        return Err(GeometryErrorKind::Invalid(
                            "source area reuse role disagrees with its planned traversal".into(),
                        )
                        .into());
                    }
                    if planned_ring != ring_id || planned_region != ring.region {
                        return Err(GeometryErrorKind::Invalid(
                            "source drawing changed its planned source area identity".into(),
                        )
                        .into());
                    }
                    if source_ring_reuse_matches(
                        self.topology,
                        source_ring,
                        reverse,
                        &ring.vertices,
                        self.emission.fragment_recipes,
                        walk,
                    )? && self.source_q_vertices_are_unchanged(
                        &ring.vertices,
                        coordinates,
                        budget,
                    )? && self.source_walk_publication_is_unchanged(
                        walk,
                        coordinates,
                        publication_coordinates,
                        budget,
                    )? {
                        add_source_reused_recipe_walk(
                            walk,
                            ring.vertices.len().saturating_sub(1),
                            self.emission.fragment_recipes,
                            self.emission.publication,
                            &mut reused,
                            budget,
                        )?;
                    }
                }
            },
        }
        for line in &drawing.lines {
            budget.charge_into_error(1)?;
            let walk =
                super::emission::next_recipe_walk(&walks, &mut cursors, line.recipe_walk, budget)?;
            if source_line_reuse_is_complete(
                self.topology,
                self.emission.fragment_recipes,
                Some(self.emission.publication),
                walk,
                &line.path,
                budget,
            )? && self.source_q_vertices_are_unchanged(&line.path, coordinates, budget)?
                && self.source_walk_publication_is_unchanged(
                    walk,
                    coordinates,
                    publication_coordinates,
                    budget,
                )?
            {
                budget.charge_into_error(
                    u64::try_from(walk.fragments.len()).map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                reused.extend(walk.fragments.iter().copied());
            }
        }
        for (owner, owner_walks) in &walks {
            budget.charge_into_error(1)?;
            if cursors.get(owner).copied().unwrap_or_default() != owner_walks.len() {
                return Err(GeometryErrorKind::Invalid(
                    "certified drawing did not consume each recipe walk exactly once".into(),
                )
                .into());
            }
        }
        Ok(reused)
    }

    fn source_q_vertices_are_unchanged(
        &self,
        vertices: &[EmittedVertex<'_>],
        coordinates: &QCoordinates,
        budget: &mut impl Work,
    ) -> Result<bool> {
        for vertex in vertices {
            budget.charge_into_error(1)?;
            if coordinates.candidate(vertex.port.node)?
                != self.nearest.candidate(vertex.port.node)?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn source_walk_publication_is_unchanged(
        &self,
        walk: &RecipeWalk,
        coordinates: &QCoordinates,
        publication_coordinates: &[Option<QCandidate>],
        budget: &mut impl Work,
    ) -> Result<bool> {
        for &recipe_id in &walk.fragments {
            let fragment = self
                .emission
                .publication
                .fragments
                .get(recipe_id.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "source-reuse walk has no publication fragment".into(),
                    )
                })?;
            for sample_index in fragment.sample_range.clone() {
                budget.charge_into_error(1)?;
                let node_id =
                    *self
                        .publication_sample_nodes
                        .get(sample_index)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "source-reuse sample has no emission node".into(),
                            )
                        })?;
                let node = self
                    .publication_nodes
                    .nodes()
                    .get(node_id.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "source-reuse sample emission node is unknown".into(),
                        )
                    })?;
                let (current, nearest) = match &node.authority {
                    NodeAuthority::Source(source) => (
                        coordinates.candidate(*source)?,
                        self.nearest.candidate(*source)?,
                    ),
                    NodeAuthority::Computed(_) => {
                        let current = publication_coordinates
                            .get(node_id.0 as usize)
                            .copied()
                            .flatten()
                            .or_else(|| {
                                self.publication_nearest
                                    .get(node_id.0 as usize)
                                    .copied()
                                    .flatten()
                            })
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "source-reuse sample has no assigned candidate".into(),
                                )
                            })?;
                        let nearest = self
                            .publication_nearest
                            .get(node_id.0 as usize)
                            .copied()
                            .flatten()
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "source-reuse sample has no nearest candidate".into(),
                                )
                            })?;
                        (current, nearest)
                    },
                };
                if current != nearest {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    fn input_port_has_rounding_residual(
        &self,
        port_id: QPortId,
        source_nodes: &[bool],
        producer: &AssignedProducerProjection,
        budget: &mut impl Work,
    ) -> Result<bool> {
        budget.charge_into_error(1)?;
        let port = self
            .topology
            .q
            .ports
            .get(port_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Native-demand Q port is unknown".into()))?;
        if port.cut {
            let assigned = producer
                .source_ports
                .get(port_id.0 as usize)
                .and_then(Option::as_ref)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "Native-demand Q cut port has no assigned image".into(),
                    )
                })?;
            if assigned == &port.point {
                return Ok(false);
            }
            let native_longitude = exact_canonical_longitude_with_work(
                self.topology.sources.frame,
                &assigned.x,
                budget,
            )?;
            return Ok(
                !exact_coordinate_has_binary64_value_with_work(&native_longitude, budget)?
                    || !exact_coordinate_has_binary64_value_with_work(&assigned.y, budget)?,
            );
        }
        let is_source = *source_nodes
            .get(port.node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Native-demand Q node is unknown".into()))?;
        if is_source {
            return Ok(false);
        }
        let Some(position) = self
            .exact_positions
            .get(port.node.0 as usize)
            .and_then(Option::as_ref)
        else {
            return Ok(false);
        };
        Ok(!exact_node_position_has_binary64_value_with_work(
            position, budget,
        )?)
    }

    fn drawing_has_rounding_residual(
        &self,
        drawing: &QEmissionDrawing<'topology>,
        producer: &AssignedProducerProjection,
        budget: &mut impl Work,
    ) -> Result<bool> {
        let q = &self.topology.q;
        let source_endpoint_count = q
            .source_edge_ports
            .len()
            .checked_mul(2)
            .and_then(|count| count.checked_add(q.source_point_ports.len()))
            .and_then(|count| count.checked_add(q.nodes.len()))
            .ok_or(ResourceError::SizeOverflow)?;
        budget.charge_into_error(
            u64::try_from(source_endpoint_count).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut source_nodes = vec![false; q.nodes.len()];
        for &[start, end] in &q.source_edge_ports {
            for port_id in [start, end] {
                let port = q.ports.get(port_id.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "Native-demand input edge references an unknown Q port".into(),
                    )
                })?;
                *source_nodes.get_mut(port.node.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "Native-demand input edge references an unknown Q node".into(),
                    )
                })? = true;
            }
        }
        for &port_id in &q.source_point_ports {
            let port = q.ports.get(port_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "Native-demand input point references an unknown Q port".into(),
                )
            })?;
            *source_nodes.get_mut(port.node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "Native-demand input point references an unknown Q node".into(),
                )
            })? = true;
        }
        match &drawing.polygons {
            DrawnPolygons::WholeSphere(polygons) => {
                for polygon in polygons {
                    for ring in polygon {
                        for vertex in &ring.vertices {
                            if self.input_port_has_rounding_residual(
                                vertex.port.id,
                                &source_nodes,
                                producer,
                                budget,
                            )? {
                                return Ok(true);
                            }
                        }
                    }
                }
            },
            DrawnPolygons::Regions { rings, .. } => {
                for ring in rings {
                    for vertex in &ring.vertices {
                        if self.input_port_has_rounding_residual(
                            vertex.port.id,
                            &source_nodes,
                            producer,
                            budget,
                        )? {
                            return Ok(true);
                        }
                    }
                }
            },
        }
        for line in &drawing.lines {
            for vertex in &line.path {
                if self.input_port_has_rounding_residual(
                    vertex.port.id,
                    &source_nodes,
                    producer,
                    budget,
                )? {
                    return Ok(true);
                }
            }
        }
        for vertex in &drawing.points {
            if self.input_port_has_rounding_residual(
                vertex.port.id,
                &source_nodes,
                producer,
                budget,
            )? {
                return Ok(true);
            }
        }
        for chord in &self.publication_chords {
            budget.charge_into_error(1)?;
            for endpoint in &chord.endpoints {
                let port_id = match endpoint {
                    PublishedEndpoint::Source { port, .. } => *port,
                    PublishedEndpoint::Published { node, sample } => {
                        let emission_node = self
                            .publication_nodes
                            .nodes()
                            .get(node.0 as usize)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "Native-demand publication node is unknown".into(),
                                )
                            })?;
                        let NodeAuthority::Source(source_node) = &emission_node.authority else {
                            let position = self
                                .publication_exact_positions
                                .get(node.0 as usize)
                                .and_then(Option::as_ref);
                            if let Some(position) = position
                                && !exact_node_position_has_binary64_value_with_work(
                                    position, budget,
                                )?
                            {
                                return Ok(true);
                            }
                            continue;
                        };
                        let Some(port_id) = self
                            .publication_sample_source_ports
                            .get(*sample)
                            .copied()
                            .flatten()
                        else {
                            return Ok(false);
                        };
                        let port = q.ports.get(port_id.0 as usize).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Native-demand publication sample has an unknown Q port".into(),
                            )
                        })?;
                        if port.node != *source_node {
                            return Err(GeometryErrorKind::Invalid(
                                "Native-demand publication sample changed its Q node".into(),
                            )
                            .into());
                        }
                        port_id
                    },
                };
                if self.input_port_has_rounding_residual(
                    port_id,
                    &source_nodes,
                    producer,
                    budget,
                )? {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn native_demand_after_input_port_check(
        &self,
        drawing: &QEmissionDrawing<'topology>,
        producer: &AssignedProducerProjection,
        budget: &mut impl Work,
    ) -> Result<bool> {
        self.drawing_has_rounding_residual(drawing, producer, budget)
    }

    fn actual_native_demand(
        &self,
        drawing: &QEmissionDrawing<'topology>,
        ring_roles: &[bool],
        producer: &AssignedProducerProjection,
        coordinates: &QCoordinates,
        publication_coordinates: &[Option<QCandidate>],
        budget: &mut impl Work,
    ) -> Result<bool> {
        if self.emission.force_native {
            return Ok(true);
        }
        if producer.leaves.len() != self.publication_chords.len() {
            return Err(GeometryErrorKind::Invalid(
                "assigned producer projection does not cover every published leaf".into(),
            )
            .into());
        }
        let source_reused = self.source_reused_recipe_ids(
            drawing,
            ring_roles,
            coordinates,
            publication_coordinates,
            budget,
        )?;
        let axis = self.topology.sources.frame.longitude();
        for leaf in &producer.leaves {
            budget.charge_into_error(1)?;
            if source_reused.contains(&leaf.recipe) {
                continue;
            }
            let [start, end] = &leaf.points;
            if strict_native_bank_event(axis, &start.x, &end.x, budget)? {
                return self.native_demand_after_input_port_check(drawing, producer, budget);
            }
        }
        if let DrawnPolygons::Regions { rings, .. } = &drawing.polygons {
            for ring in rings {
                budget.charge_into_error(1)?;
                if !matches!(ring.origin, DrawnAreaOrigin::Cycle) {
                    continue;
                }
                let selected_regions = self.emission.selected_regions;
                let mut low = 0_usize;
                let mut high = selected_regions.len();
                while low < high {
                    budget.charge_into_error(1)?;
                    let middle = low.midpoint(high);
                    let candidate = selected_regions.get(middle).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "generated area use has no selected-region authority".into(),
                        )
                    })?;
                    if candidate.root < ring.region {
                        low = middle.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
                    } else {
                        high = middle;
                    }
                }
                let region = selected_regions
                    .get(low)
                    .filter(|region| region.root == ring.region)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "generated area use has no selected-region authority".into(),
                        )
                    })?;
                if region.poles.includes(ChartPole::North)
                    || region.poles.includes(ChartPole::South)
                {
                    return self.native_demand_after_input_port_check(drawing, producer, budget);
                }
                for vertex in &ring.vertices {
                    budget.charge_into_error(1)?;
                    if vertex.port.cut
                        || axis
                            .canonicalize(vertex.point.x)
                            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?
                            .identity(axis)
                            == SeamIdentity::Seam
                    {
                        return self
                            .native_demand_after_input_port_check(drawing, producer, budget);
                    }
                }
            }
        }
        Ok(false)
    }

    fn materialize_native_cut_features(
        &mut self,
        producer: &AssignedProducerProjection,
        budget: &mut impl SearchAccount,
    ) -> Result<Vec<NativeCutFeature>> {
        if producer.leaves.len() != self.publication_chords.len() {
            return Err(GeometryErrorKind::Invalid(
                "native cut materialization lost its assigned producer leaves".into(),
            )
            .into());
        }
        let mut output = Vec::new();
        for (chord_index, chord) in self.publication_chords.iter().enumerate() {
            budget.charge_into_error(1)?;
            let leaf = producer.leaves.get(chord_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut producer leaf is unknown".into())
            })?;
            if leaf.recipe != chord.recipe || leaf.trace_edge != chord.trace_edge {
                return Err(GeometryErrorKind::Invalid(
                    "native cut producer projection changed its leaf identity".into(),
                )
                .into());
            }
            let [start, end] = &leaf.points;
            let displacement = end.x.try_subtract_with_work(&start.x, budget)?;
            let recipe = self
                .emission
                .fragment_recipes
                .get(chord.recipe.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("native producer recipe is unknown".into())
                })?;
            let fragment = self
                .emission
                .publication
                .fragments
                .get(chord.recipe.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("native producer trace is unknown".into())
                })?;
            let features = super::emission::native_cut_features(
                self.topology,
                recipe,
                &fragment.trace,
                chord.trace_edge,
                start,
                end,
                &displacement,
                budget,
            )?;
            output.extend(features);
        }
        let QFamilyState::Native(families) = &mut self.family_state else {
            return Err(GeometryErrorKind::Invalid(
                "native cuts were materialized before Native family activation".into(),
            )
            .into());
        };
        for feature in &mut output {
            budget.charge_into_error(1)?;
            let parent = feature.family.fragment_recipe().ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut has no fragment family".into())
            })?;
            feature.family.occurrence = families.native_cut_occurrence(
                parent,
                feature.trace_edge,
                feature.seam_period_index,
                budget,
            )?;
            families.register(feature.family, budget)?;
        }
        families.synchronize_budget(budget)?;
        Ok(output)
    }

    fn all_outer_repair_classes(&self, budget: &mut impl Work) -> Result<Vec<usize>> {
        let variable_count = self.classes.iter().try_fold(0_usize, |count, class| {
            count
                .checked_add(class.len())
                .ok_or(ResourceError::SizeOverflow)
        })?;
        budget.charge_into_error(
            u64::try_from(variable_count).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut variables = Vec::with_capacity(variable_count);
        for class in &self.classes {
            variables.extend_from_slice(class);
        }
        self.classes_for_variables(variables, budget)
    }

    fn search_native_cut_candidates(
        &self,
        reference: &NativeReference<'topology>,
        features: &[NativeCutFeature],
        coordinates: &QCoordinates,
        publication_coordinates: &[Option<QCandidate>],
        budget: &mut EvaluationBudget,
    ) -> Result<NativeCutSearchOutcome> {
        let frame = self.topology.sources.frame;
        if reference.publication_sample_nodes.len() != self.emission.publication.samples.len()
            || reference.feature_nodes.len() != features.len()
        {
            return Err(GeometryErrorKind::Invalid(
                "native reference nodes do not match its publication and cut rows".into(),
            )
            .into());
        }
        let nodes = &reference.plan.node_registry;
        if reference.sites.len() != nodes.nodes().len() {
            return Err(GeometryErrorKind::Invalid(
                "native reference sites do not cover every emission node".into(),
            )
            .into());
        }
        budget.charge_into_error(
            u64::try_from(reference.sites.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut fixed_sites = Vec::with_capacity(reference.sites.len());
        for site in &reference.sites {
            budget.charge_into_error(1)?;
            fixed_sites.push(
                site.as_ref()
                    .map(|site| candidate_from_emission_site(*site)),
            );
        }
        budget.charge_into_error(
            u64::try_from(nodes.nodes().len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut has_publication_sample_by_node = vec![false; nodes.nodes().len()];
        for &node in &reference.publication_sample_nodes {
            budget.charge_into_error(1)?;
            if matches!(
                nodes
                    .nodes()
                    .get(node.0 as usize)
                    .map(|value| &value.authority),
                Some(NodeAuthority::Computed(_))
            ) {
                let slot = has_publication_sample_by_node
                    .get_mut(node.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "native publication sample node is outside its registry".into(),
                        )
                    })?;
                *slot = true;
            }
        }

        let mut variable_for_node = vec![None; nodes.nodes().len()];
        let mut points = Vec::new();
        let mut domains = Vec::new();
        let mut feature_sites = Vec::with_capacity(features.len());
        for (&node_id, feature) in reference.feature_nodes.iter().zip(features) {
            let node_index = node_id.0 as usize;
            let node = nodes.nodes().get(node_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut emission node is unknown".into())
            })?;
            match &node.authority {
                NodeAuthority::Source(source_node) => {
                    let index = source_node.0 as usize;
                    let candidate = QCandidate {
                        native: coordinates.xy(*source_node)?,
                        periods: *coordinates.longitude_periods.get(index).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "native source cut node has no retained lift".into(),
                            )
                        })?,
                    };
                    feature_sites.push(NativeCutOrderSite::Fixed(candidate));
                },
                NodeAuthority::Computed(_) => {
                    let has_publication_sample = *has_publication_sample_by_node
                        .get(node_index)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "native cut publication node has no sample state".into(),
                            )
                        })?;
                    if has_publication_sample {
                        let candidate = publication_coordinates
                            .get(node_index)
                            .copied()
                            .flatten()
                            .or_else(|| self.publication_nearest.get(node_index).copied().flatten())
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "native cut matching a publication sample has no assigned site"
                                        .into(),
                                )
                            })?;
                        feature_sites.push(NativeCutOrderSite::Fixed(candidate));
                    } else {
                        let variable = if let Some(variable) =
                            variable_for_node.get(node_index).copied().flatten()
                        {
                            variable
                        } else {
                            let variable = points.len();
                            let point = feature.point.clone();
                            budget.charge_into_error(3)?;
                            let domain = native_cut_candidate_domain(frame, &point, budget)?;
                            if domain.is_empty() {
                                return Ok(NativeCutSearchOutcome::Reject(
                                    self.all_outer_repair_classes(budget)?,
                                ));
                            }
                            *variable_for_node.get_mut(node_index).ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "native computed cut node has no variable slot".into(),
                                )
                            })? = Some(variable);
                            points.push(point);
                            domains.push(domain);
                            variable
                        };
                        feature_sites.push(NativeCutOrderSite::Variable(variable));
                    }
                },
            }
        }

        let mut event_order = (0..features.len()).collect::<Vec<_>>();
        budget.charge_into_error(
            u64::try_from(event_order.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        try_sort_by_with_work(&mut event_order, budget, |&left, &right, budget| {
            budget.charge_into_error(1)?;
            let left = features.get(left).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut ordering has an unknown feature".into())
            })?;
            let right = features.get(right).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut ordering has an unknown feature".into())
            })?;
            let latitude = left.point.y.try_cmp_with_work(&right.point.y, budget)?;
            Ok(latitude.then_with(|| left.family.cmp(&right.family)))
        })?;
        let mut unique_nodes = BTreeSet::new();
        let mut unique_event_order = Vec::with_capacity(event_order.len());
        for event in event_order {
            let node = *reference.feature_nodes.get(event).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut ordering has an unknown node".into())
            })?;
            if unique_nodes.insert(node) {
                unique_event_order.push(event);
            }
        }
        let mut orders = Vec::new();
        let mut previous_event = None;
        for event in unique_event_order {
            let Some(previous) = previous_event else {
                previous_event = Some(event);
                continue;
            };
            let before_node = *reference.feature_nodes.get(previous).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut order has no previous node".into())
            })?;
            let after_node = *reference.feature_nodes.get(event).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut order has no next node".into())
            })?;
            if before_node == after_node {
                continue;
            }
            let before_feature = features.get(previous).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut order has no previous feature".into())
            })?;
            let after_feature = features.get(event).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut order has no next feature".into())
            })?;
            let ascending = before_feature
                .point
                .y
                .try_cmp_with_work(&after_feature.point.y, budget)?
                .is_lt();
            if before_feature
                .point
                .y
                .try_cmp_with_work(&after_feature.point.y, budget)?
                .is_eq()
            {
                return Ok(NativeCutSearchOutcome::Reject(
                    self.all_outer_repair_classes(budget)?,
                ));
            }
            let before_site = *feature_sites.get(previous).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut order has no previous site".into())
            })?;
            let after_site = *feature_sites.get(event).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut order has no next site".into())
            })?;
            if let (NativeCutOrderSite::Fixed(before), NativeCutOrderSite::Fixed(after)) =
                (before_site, after_site)
            {
                budget.charge_into_error(1)?;
                if !native_cut_order_holds(ascending, before, after) {
                    return Ok(NativeCutSearchOutcome::Reject(
                        self.all_outer_repair_classes(budget)?,
                    ));
                }
            } else {
                orders.push(NativeCutOrder {
                    before: before_site,
                    after: after_site,
                    ascending,
                });
            }
            previous_event = Some(event);
        }

        budget.charge_into_error(
            u64::try_from(nodes.nodes().len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut fixed_keys = Vec::new();
        for node_index in 0..nodes.nodes().len() {
            if variable_for_node
                .get(node_index)
                .copied()
                .flatten()
                .is_some()
            {
                continue;
            }
            let candidate = fixed_sites
                .get(node_index)
                .copied()
                .flatten()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "fixed native reference node has no assigned site".into(),
                    )
                })?;
            let key = emitted_node_key(frame, candidate.native)?;
            fixed_keys.push(key);
        }

        let variable_count = points.len();
        let no_variables = variable_count == 0;
        let complete = |candidates: &[QCandidate],
                        budget: &mut EvaluationBudget|
         -> Result<GlobalCheck<super::emission::CompletedNativeDrawing>> {
            if candidates.len() != variable_count {
                return Err(GeometryErrorKind::Invalid(
                    "native cut completion candidates do not match their variable table".into(),
                )
                .into());
            }
            let mut feature_candidates = Vec::with_capacity(feature_sites.len());
            for &site in &feature_sites {
                budget.charge_into_error(1)?;
                let candidate = match site {
                    NativeCutOrderSite::Variable(variable) => {
                        *candidates.get(variable).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "native cut completion omitted a variable".into(),
                            )
                        })?
                    },
                    NativeCutOrderSite::Fixed(candidate) => candidate,
                };
                feature_candidates.push(candidate);
            }
            match self.complete_native_drawing(reference, features, &feature_candidates, budget)? {
                Some(drawing) => Ok(GlobalCheck::Pass(drawing)),
                None if no_variables => Ok(GlobalCheck::Reject(Vec::new())),
                None => Ok(GlobalCheck::Reject(vec![0])),
            }
        };
        if no_variables {
            return match complete(&[], budget)? {
                GlobalCheck::Pass(drawing) => Ok(NativeCutSearchOutcome::Pass(drawing)),
                GlobalCheck::Reject(_) => Ok(NativeCutSearchOutcome::Reject(
                    self.all_outer_repair_classes(budget)?,
                )),
            };
        }
        let variables = (0..points.len()).collect::<Vec<_>>();
        let mut problem = NativeCutProblem {
            frame,
            points,
            domains,
            classes: vec![variables],
            fixed_keys,
            orders,
            complete,
        };
        match run_with_budget(&mut problem, budget)? {
            SearchOutcome::Pass(assignment) => Ok(NativeCutSearchOutcome::Pass(assignment.output)),
            SearchOutcome::Exhausted => Ok(NativeCutSearchOutcome::Reject(
                self.all_outer_repair_classes(budget)?,
            )),
        }
    }

    fn native_endpoint_data(
        &self,
        builder: &super::emission::EmissionBuilder<'topology>,
        endpoint: &PublishedEndpoint,
        assigned_point: &ExactChartPoint,
        source_requests: &[RootRequest],
        sites: &[Option<AssignedSite>],
        bank: Option<super::emission::ChartSide>,
        budget: &mut impl Work,
    ) -> Result<NativePortData> {
        let (node, candidate, source_record, occurrences) = match endpoint {
            PublishedEndpoint::Source {
                port,
                source_record,
                sample,
            } => {
                let q_port = self.topology.q.ports.get(port.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("native source endpoint port is unknown".into())
                })?;
                let node = builder.source_node(q_port.node)?;
                let site = sites
                    .get(node.0 as usize)
                    .and_then(Option::as_ref)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "native source endpoint has no assigned site".into(),
                        )
                    })?;
                let candidate = candidate_from_emission_site(*site);
                if let Some(record) = source_record {
                    super::emission::native_source_record_quotient_key(
                        self.topology.sources.frame,
                        *record,
                        assigned_point,
                        budget,
                    )?;
                }
                let occurrences = if let Some(sample) = sample {
                    if let Some(sample_index) = sample.publication_sample
                        && self
                            .publication_sample_source_ports
                            .get(sample_index)
                            .copied()
                            .flatten()
                            != Some(*port)
                    {
                        return Err(GeometryErrorKind::Invalid(
                            "Native source sample changed its Q-port authority".into(),
                        )
                        .into());
                    }
                    if q_port.cut {
                        native_source_sample_occurrences(
                            sample
                                .trace
                                .as_ref()
                                .map(|trace| trace.try_clone_with_work(budget))
                                .transpose()?,
                            source_requests,
                            budget,
                        )?
                    } else {
                        budget.charge_into_error(2)?;
                        let mut occurrences = Vec::with_capacity(2);
                        if sample.trace.is_some() && !sample.endpoint_only {
                            occurrences.push(OccurrenceSource {
                                origin: super::emission::VertexOrigin::SourceNode(q_port.node),
                                source_trace: None,
                            });
                        }
                        occurrences.push(OccurrenceSource {
                            origin: sample
                                .trace
                                .as_ref()
                                .map_or(super::emission::VertexOrigin::Presentation, |_| {
                                    super::emission::VertexOrigin::SourceSample
                                }),
                            source_trace: sample
                                .trace
                                .as_ref()
                                .map(|trace| trace.try_clone_with_work(budget))
                                .transpose()?,
                        });
                        occurrences
                    }
                } else if q_port.cut {
                    return Err(GeometryErrorKind::Invalid(
                        "cut publication endpoint has no authenticated source row".into(),
                    )
                    .into());
                } else {
                    vec![OccurrenceSource {
                        origin: super::emission::VertexOrigin::SourceNode(q_port.node),
                        source_trace: None,
                    }]
                };
                (node, candidate, *source_record, occurrences)
            },
            PublishedEndpoint::Published { node, sample } => {
                if self.publication_sample_nodes.get(*sample) != Some(node) {
                    return Err(GeometryErrorKind::Invalid(
                        "Native publication sample changed its node authority".into(),
                    )
                    .into());
                }
                let sample = self
                    .emission
                    .publication
                    .samples
                    .get(*sample)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "native endpoint publication sample is unknown".into(),
                        )
                    })?;
                let site = sites
                    .get(node.0 as usize)
                    .and_then(Option::as_ref)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "native publication endpoint has no assigned site".into(),
                        )
                    })?;
                let candidate = candidate_from_emission_site(*site);
                let occurrence = OccurrenceSource {
                    origin: sample
                        .source_trace
                        .as_ref()
                        .map_or(super::emission::VertexOrigin::Presentation, |_| {
                            super::emission::VertexOrigin::SourceSample
                        }),
                    source_trace: sample
                        .source_trace
                        .as_ref()
                        .map(|trace| trace.try_clone_with_work(budget))
                        .transpose()?,
                };
                (*node, candidate, None, vec![occurrence])
            },
        };
        native_port_data(
            self.topology.sources.frame,
            node,
            candidate,
            assigned_point,
            source_record,
            occurrences,
            bank,
            budget,
        )
    }

    fn native_endpoint_root_requests(
        &self,
        endpoint: &PublishedEndpoint,
        source_trace: &SourceTrace,
        trace_edge: usize,
        edge_parameter: &ExactRational,
        budget: &mut impl Work,
    ) -> Result<Vec<RootRequest>> {
        let PublishedEndpoint::Source { port, .. } = endpoint else {
            return Ok(Vec::new());
        };
        let q_port = self.topology.q.ports.get(port.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("native source endpoint port is unknown".into())
        })?;
        if q_port.cut {
            source_trace.requests_at(trace_edge, edge_parameter, budget)
        } else {
            Ok(Vec::new())
        }
    }

    fn native_cut_port_data(
        &self,
        builder: &mut super::emission::EmissionBuilder<'topology>,
        feature: &NativeCutFeature,
        side: super::emission::ChartSide,
        source_requests: &[RootRequest],
        budget: &mut impl Work,
    ) -> Result<NativePortData> {
        let node = builder.intern_point(&feature.point, budget)?;
        let occurrences = native_source_sample_occurrences(
            feature
                .source_trace
                .as_ref()
                .map(|trace| trace.try_clone_with_work(budget))
                .transpose()?,
            source_requests,
            budget,
        )?;
        let longitude = match side {
            super::emission::ChartSide::East => {
                ExactRational::from_f64(self.topology.sources.frame.longitude().half_period())
            },
            super::emission::ChartSide::West => ExactRational::zero().try_subtract_with_work(
                &ExactRational::from_f64(self.topology.sources.frame.longitude().half_period()),
                budget,
            )?,
        };
        Ok(NativePortData {
            node,
            point: ExactChartPoint::from_rationals(
                longitude,
                feature.point.y.try_clone_with_work(budget)?,
            ),
            spelling: super::emission::PortSpelling::Seam(side),
            source_record: None,
            occurrences,
        })
    }

    fn native_chord_pieces(
        &self,
        builder: &mut super::emission::EmissionBuilder<'topology>,
        chord: &QPublishedChord,
        assigned_endpoints: &[ExactChartPoint; 2],
        source_trace: &SourceTrace,
        region: Option<usize>,
        feature_indices: &[usize],
        features: &[NativeCutFeature],
        sites: &[Option<AssignedSite>],
        budget: &mut impl Work,
    ) -> Result<NativeChordPieces> {
        budget.charge_into_error(1)?;
        let recipe = chord.recipe;
        let graph_owner = region.map_or(NativeGraphEdgeOwner::Line, |region| {
            NativeGraphEdgeOwner::Area { region }
        });
        let [start_chart, end_chart] = (*assigned_endpoints).clone();
        let displacement = end_chart.x.try_subtract_with_work(&start_chart.x, budget)?;
        let zero = ExactRational::zero();
        let one = ExactRational::one();
        let direction = displacement.try_cmp_with_work(&zero, budget)?;
        let start_is_seam = matches!(
            quotient_key_for_chart_position_with_work(
                self.topology.sources.frame,
                &start_chart,
                budget,
            )?,
            ExactQuotientKey::Seam(_)
        );
        let end_is_seam = matches!(
            quotient_key_for_chart_position_with_work(
                self.topology.sources.frame,
                &end_chart,
                budget,
            )?,
            ExactQuotientKey::Seam(_)
        );
        if direction.is_eq() && !feature_indices.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "zero-longitude producer chord has a native cut".into(),
            )
            .into());
        }
        let mut seam_wall_source_spans = None;
        let seam_wall_bank = if let Some(region) = region
            && direction.is_eq()
            && start_is_seam
            && end_is_seam
        {
            let vertical = start_chart.y.try_cmp_with_work(&end_chart.y, budget)?;
            if vertical.is_eq() {
                None
            } else {
                let source_spans = native_known_source_spans_for_slice(
                    self.topology,
                    self.emission.selected_face_roots,
                    Some(region),
                    chord,
                    &zero,
                    &one,
                    budget,
                )?;
                if source_spans.selected_left_region != Some(region) {
                    return Err(GeometryErrorKind::Invalid(
                        "native seam wall has no authenticated selected-left source germ".into(),
                    )
                    .into());
                }
                seam_wall_source_spans = Some(source_spans);
                Some(if vertical.is_gt() {
                    super::emission::ChartSide::West
                } else {
                    super::emission::ChartSide::East
                })
            }
        } else {
            None
        };
        let start_bank = seam_wall_bank.map_or_else(
            || {
                if !start_is_seam {
                    None
                } else if direction.is_gt() {
                    Some(super::emission::ChartSide::West)
                } else if direction.is_lt() {
                    Some(super::emission::ChartSide::East)
                } else {
                    None
                }
            },
            Some,
        );
        let end_bank = seam_wall_bank.map_or_else(
            || {
                if !end_is_seam {
                    None
                } else if direction.is_gt() {
                    Some(super::emission::ChartSide::East)
                } else if direction.is_lt() {
                    Some(super::emission::ChartSide::West)
                } else {
                    None
                }
            },
            Some,
        );
        let start_requests = self.native_endpoint_root_requests(
            &chord.endpoints[0],
            source_trace,
            chord.trace_edge,
            &zero,
            budget,
        )?;
        let end_requests = self.native_endpoint_root_requests(
            &chord.endpoints[1],
            source_trace,
            chord.trace_edge,
            &one,
            budget,
        )?;
        let mut current = self.native_endpoint_data(
            builder,
            &chord.endpoints[0],
            &start_chart,
            &start_requests,
            sites,
            start_bank,
            budget,
        )?;
        let mut end = self.native_endpoint_data(
            builder,
            &chord.endpoints[1],
            &end_chart,
            &end_requests,
            sites,
            end_bank,
            budget,
        )?;
        let current_key = quotient_key_for_chart_position_with_work(
            self.topology.sources.frame,
            &current.point,
            budget,
        )?;
        let start_key = quotient_key_for_chart_position_with_work(
            self.topology.sources.frame,
            &start_chart,
            budget,
        )?;
        let end_key = quotient_key_for_chart_position_with_work(
            self.topology.sources.frame,
            &end.point,
            budget,
        )?;
        let assigned_end_key = quotient_key_for_chart_position_with_work(
            self.topology.sources.frame,
            &end_chart,
            budget,
        )?;
        if !compare_quotient_keys_with_work(&current_key, &start_key, budget)?.is_eq()
            || !compare_quotient_keys_with_work(&end_key, &assigned_end_key, budget)?.is_eq()
        {
            return Err(GeometryErrorKind::Invalid(
                "native producer ports differ from their assigned quotient projection".into(),
            )
            .into());
        }
        let mut output = NativeChordPieces {
            edges: Vec::with_capacity(feature_indices.len().saturating_add(1)),
            break_after: Vec::with_capacity(feature_indices.len().saturating_add(1)),
            seam_events: Vec::new(),
        };
        for (is_seam, chart, endpoint, source_requests) in [
            (
                start_is_seam,
                &start_chart,
                &chord.endpoints[0],
                &start_requests,
            ),
            (end_is_seam, &end_chart, &chord.endpoints[1], &end_requests),
        ] {
            budget.charge_into_error(1)?;
            if !is_seam {
                continue;
            }
            let east = self.native_endpoint_data(
                builder,
                endpoint,
                chart,
                source_requests,
                sites,
                Some(super::emission::ChartSide::East),
                budget,
            )?;
            let west = self.native_endpoint_data(
                builder,
                endpoint,
                chart,
                source_requests,
                sites,
                Some(super::emission::ChartSide::West),
                budget,
            )?;
            output.seam_events.push(NativeSeamEventData {
                event: NativeSeamEvent {
                    latitude: chart.y.clone(),
                    output_vertex: true,
                },
                vertices: NativeSeamVertices {
                    latitude: chart.y.clone(),
                    east,
                    west,
                },
            });
        }
        let mut previous_parameter = ExactRational::zero();
        for &feature_index in feature_indices {
            budget.charge_into_error(1)?;
            let feature = features.get(feature_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("native chord references an unknown cut feature".into())
            })?;
            if feature.family.fragment_recipe() != Some(chord.recipe)
                || feature.trace_edge != chord.trace_edge
            {
                return Err(GeometryErrorKind::Invalid(
                    "native cut feature is attached to another producer leaf".into(),
                )
                .into());
            }
            let (entry_side, exit_side) = if direction.is_gt() {
                (
                    super::emission::ChartSide::East,
                    super::emission::ChartSide::West,
                )
            } else {
                (
                    super::emission::ChartSide::West,
                    super::emission::ChartSide::East,
                )
            };
            let source_requests =
                source_trace.requests_at(chord.trace_edge, &feature.edge_parameter, budget)?;
            let entry =
                self.native_cut_port_data(builder, feature, entry_side, &source_requests, budget)?;
            let exit =
                self.native_cut_port_data(builder, feature, exit_side, &source_requests, budget)?;
            let east = self.native_cut_port_data(
                builder,
                feature,
                super::emission::ChartSide::East,
                &source_requests,
                budget,
            )?;
            let west = self.native_cut_port_data(
                builder,
                feature,
                super::emission::ChartSide::West,
                &source_requests,
                budget,
            )?;
            output.seam_events.push(NativeSeamEventData {
                event: NativeSeamEvent {
                    latitude: feature.point.y.clone(),
                    output_vertex: true,
                },
                vertices: NativeSeamVertices {
                    latitude: feature.point.y.clone(),
                    east,
                    west,
                },
            });
            align_native_port_to_seam(self.topology.sources.frame, &mut current, &entry, budget)?;
            output.edges.push(NativeAreaEdge {
                vertices: [current, entry],
                owner: graph_owner,
                kind: NativeAreaEdgeKind::Producer {
                    recipe,
                    trace_edge: chord.trace_edge,
                    producer_start: Box::new(previous_parameter.clone()),
                    producer_end: feature.edge_parameter.clone(),
                    trace_ranges: trace_ranges_for_slice(
                        source_trace,
                        chord.trace_edge,
                        &previous_parameter,
                        &feature.edge_parameter,
                        budget,
                    )?,
                    known_spans: native_known_source_spans_for_slice(
                        self.topology,
                        self.emission.selected_face_roots,
                        region,
                        chord,
                        &previous_parameter,
                        &feature.edge_parameter,
                        budget,
                    )?
                    .uses,
                },
            });
            output.break_after.push(true);
            current = exit;
            previous_parameter = feature.edge_parameter.clone();
        }
        if !matches!(&current.spelling, super::emission::PortSpelling::Seam(_))
            && matches!(&end.spelling, super::emission::PortSpelling::Seam(_))
        {
            align_native_port_to_seam(self.topology.sources.frame, &mut current, &end, budget)?;
        } else if matches!(&current.spelling, super::emission::PortSpelling::Seam(_))
            && !matches!(&end.spelling, super::emission::PortSpelling::Seam(_))
        {
            align_native_port_to_seam(self.topology.sources.frame, &mut end, &current, budget)?;
        }
        output.edges.push(NativeAreaEdge {
            vertices: [current, end],
            owner: graph_owner,
            kind: NativeAreaEdgeKind::Producer {
                recipe,
                trace_edge: chord.trace_edge,
                producer_start: Box::new(previous_parameter.clone()),
                producer_end: one.clone(),
                trace_ranges: trace_ranges_for_slice(
                    source_trace,
                    chord.trace_edge,
                    &previous_parameter,
                    &one,
                    budget,
                )?,
                known_spans: match seam_wall_source_spans.take() {
                    Some(source_spans) => source_spans.uses,
                    None => {
                        native_known_source_spans_for_slice(
                            self.topology,
                            self.emission.selected_face_roots,
                            region,
                            chord,
                            &previous_parameter,
                            &one,
                            budget,
                        )?
                        .uses
                    },
                },
            },
        });
        output.break_after.push(end_is_seam);
        Ok(output)
    }

    fn register_native_pole_families(
        &mut self,
        pole_closures: &[NativePoleClosure],
        budget: &mut impl SearchAccount,
    ) -> Result<()> {
        if pole_closures.is_empty() {
            return Ok(());
        }
        budget.charge_into_error(
            u64::try_from(pole_closures.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut families = Vec::with_capacity(pole_closures.len());
        for &(region, pole) in pole_closures {
            budget.charge_into_error(1)?;
            families.push(super::emission::FeatureFamily {
                parent: super::emission::FeatureFamilyParent::Region(region),
                producer: super::emission::SampleProducerKind::PolePresentation(pole),
                occurrence: 0,
            });
        }
        let QFamilyState::Native(registry) = &mut self.family_state else {
            return Err(GeometryErrorKind::Invalid(
                "native pole families were registered before Native activation".into(),
            )
            .into());
        };
        for family in families {
            registry.register(family, budget)?;
        }
        registry.synchronize_budget(budget)
    }

    fn native_region_family_universe(
        &self,
        budget: &mut impl Work,
    ) -> Result<Vec<super::emission::FeatureFamily>> {
        let whole_sphere_region = if self.emission.plan.is_whole_sphere() {
            Some(
                self.emission
                    .selected_face_roots
                    .iter()
                    .flatten()
                    .copied()
                    .next()
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "whole-sphere family universe has no selected root".into(),
                        )
                    })?,
            )
        } else {
            None
        };
        budget.charge_into_error(
            u64::try_from(self.emission.selected_regions.len())
                .map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let possible_poles =
            self.emission
                .selected_regions
                .iter()
                .try_fold(0_usize, |count, region| {
                    let poles = usize::from(region.poles.includes(ChartPole::North))
                        .checked_add(usize::from(region.poles.includes(ChartPole::South)))
                        .ok_or(ResourceError::SizeOverflow)?;
                    count.checked_add(poles).ok_or(ResourceError::SizeOverflow)
                })?;
        let cover_families = if whole_sphere_region.is_some() { 10 } else { 0 };
        let capacity = possible_poles
            .checked_add(cover_families)
            .ok_or(ResourceError::SizeOverflow)?;
        budget
            .charge_into_error(u64::try_from(capacity).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut families = Vec::with_capacity(capacity);
        if let Some(region) = whole_sphere_region {
            for occurrence in 0..10_u32 {
                budget.charge_into_error(1)?;
                families.push(super::emission::FeatureFamily {
                    parent: super::emission::FeatureFamilyParent::Region(region),
                    producer: super::emission::SampleProducerKind::WholeSphereCover,
                    occurrence,
                });
            }
        } else {
            for region in self.emission.selected_regions {
                for pole in [ChartPole::South, ChartPole::North] {
                    budget.charge_into_error(1)?;
                    if region.poles.includes(pole) {
                        families.push(super::emission::FeatureFamily {
                            parent: super::emission::FeatureFamilyParent::Region(region.root),
                            producer: super::emission::SampleProducerKind::PolePresentation(pole),
                            occurrence: 0,
                        });
                    }
                }
            }
        }
        Ok(families)
    }

    fn freeze_native_families(&mut self, budget: &mut EvaluationBudget) -> Result<()> {
        let QFamilyState::Shared {
            original_source_base,
        } = &self.family_state
        else {
            return Ok(());
        };
        let source_base = *original_source_base;
        let native_cut_ranges = self.native_cut_family_domains(budget)?;
        let region_families = self.native_region_family_universe(budget)?;
        let bounds = FeatureFamilyBounds::from_prepared_domains(
            self.emission.fragment_recipes,
            self.emission.publication,
            &native_cut_ranges,
            region_families,
            budget,
        )?;
        let mut registry = bounds.into_registry(source_base);
        for sample in &self.emission.publication.samples {
            registry.register(sample.family, budget)?;
        }
        registry.synchronize_budget(budget)?;
        self.family_state = QFamilyState::Native(registry);
        Ok(())
    }

    fn take_native_families(&mut self) -> Option<FeatureFamilyRegistry> {
        let QFamilyState::Native(_) = &self.family_state else {
            return None;
        };
        match std::mem::replace(&mut self.family_state, QFamilyState::Shared {
            original_source_base: 0,
        }) {
            QFamilyState::Native(registry) => Some(registry),
            QFamilyState::Shared {
                original_source_base,
            } => {
                self.family_state = QFamilyState::Shared {
                    original_source_base,
                };
                None
            },
        }
    }

    fn native_family_endpoint_domain(
        &self,
        chord: &QPublishedChord,
        endpoint_index: usize,
        source_points: &mut NativeEndpointPointCache,
        budget: &mut impl Work,
    ) -> Result<Vec<(QCandidate, ExactChartPoint)>> {
        let endpoint = chord.endpoints.get(endpoint_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("native family endpoint index is unknown".into())
        })?;
        let reference = chord.exact.get(endpoint_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("native family endpoint reference is unknown".into())
        })?;
        let reference_offset = chord.reference_offsets.get(endpoint_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("native family endpoint lift is unknown".into())
        })?;
        let mut output = Vec::new();
        match endpoint {
            PublishedEndpoint::Source { port, .. } => {
                let q_port = self.topology.q.ports.get(port.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("native family source port is unknown".into())
                })?;
                let nearest = self.nearest.candidate(q_port.node)?;
                if q_port.cut {
                    budget.charge_into_error(1)?;
                    output.push((
                        nearest,
                        self.assigned_publication_endpoint_point(
                            endpoint,
                            reference,
                            reference_offset,
                            &q_port.point,
                            budget,
                        )?,
                    ));
                } else {
                    let candidates = self.domains.get(q_port.node.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("native family source domain is unknown".into())
                    })?;
                    budget.charge_into_error(
                        u64::try_from(candidates.len()).map_err(|_| ResourceError::SizeOverflow)?,
                    )?;
                    output.reserve(candidates.len());
                    for &candidate in candidates {
                        budget.charge_into_error(1)?;
                        let point = source_points.get_or_compute(
                            NativeEndpointPointKey::new(*port, candidate),
                            budget,
                            |budget| {
                                assigned_port_point(
                                    self.topology.sources.frame,
                                    &self.topology.q,
                                    *port,
                                    self.nearest.xy(q_port.node)?,
                                    candidate,
                                    self.nearest
                                        .pole_port_longitude
                                        .get(port.0 as usize)
                                        .copied()
                                        .flatten(),
                                    self.nearest
                                        .pole_port_periods
                                        .get(port.0 as usize)
                                        .copied()
                                        .flatten(),
                                    &self.cut_east,
                                    budget,
                                )
                            },
                        )?;
                        if let Some(point) = point {
                            output.push((
                                candidate,
                                self.assigned_publication_endpoint_point(
                                    endpoint,
                                    reference,
                                    reference_offset,
                                    point,
                                    budget,
                                )?,
                            ));
                        }
                    }
                    if output.is_empty() {
                        return Err(GeometryErrorKind::Invalid(
                            "native family endpoint has no admitted source-site image".into(),
                        )
                        .into());
                    }
                }
            },
            PublishedEndpoint::Published { node, .. } => {
                let node_index = node.0 as usize;
                let candidates = self.publication_domains.get(node_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("native family publication domain is unknown".into())
                })?;
                budget.charge_into_error(
                    u64::try_from(candidates.len()).map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                output.reserve(candidates.len());
                for &candidate in candidates {
                    budget.charge_into_error(1)?;
                    output.push((
                        candidate,
                        self.assigned_publication_endpoint_point(
                            endpoint,
                            reference,
                            reference_offset,
                            &candidate_chart_point_with_work(
                                self.topology.sources.frame,
                                candidate,
                                budget,
                            )?,
                            budget,
                        )?,
                    ));
                }
                if output.is_empty() {
                    return Err(GeometryErrorKind::Invalid(
                        "native family publication domain is empty".into(),
                    )
                    .into());
                }
            },
        }
        Ok(output)
    }

    fn native_cut_family_domains(&self, budget: &mut impl Work) -> Result<NativeCutFamilyDomains> {
        let recipe_count = self.emission.fragment_recipes.len();
        if self.publication_chord_offsets.len()
            != recipe_count
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?
        {
            return Err(GeometryErrorKind::Invalid(
                "native family chord offsets do not cover every recipe".into(),
            )
            .into());
        }
        budget.charge_into_error(
            u64::try_from(recipe_count).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        budget.charge_into_error(1)?;
        let mut source_points = NativeEndpointPointCache::new(collection_hash_state()?.clone());
        let mut recipe_ranges: NativeCutFamilyDomains = Vec::with_capacity(recipe_count);
        for recipe_index in 0..recipe_count {
            budget.charge_into_error(1)?;
            let start = *self
                .publication_chord_offsets
                .get(recipe_index)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("native family recipe start is unknown".into())
                })?;
            let next_recipe = recipe_index
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            let end = *self
                .publication_chord_offsets
                .get(next_recipe)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("native family recipe end is unknown".into())
                })?;
            let leaf_count = end.checked_sub(start).ok_or_else(|| {
                GeometryErrorKind::Invalid("native family chord offsets are reversed".into())
            })?;
            budget.charge_into_error(
                u64::try_from(leaf_count).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let mut leaf_ranges: NativeRecipeCutRanges = Vec::with_capacity(leaf_count);
            for chord_index in start..end {
                budget.charge_into_error(1)?;
                let chord = self.publication_chords.get(chord_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("native family chord is unknown".into())
                })?;
                let left =
                    self.native_family_endpoint_domain(chord, 0, &mut source_points, budget)?;
                let right =
                    self.native_family_endpoint_domain(chord, 1, &mut source_points, budget)?;
                let mut ranges: NativeChordCutRanges = Vec::new();
                for (left_candidate, left_point) in &left {
                    for (right_candidate, right_point) in &right {
                        budget.charge_into_error(1)?;
                        if chord.nodes[0] == chord.nodes[1] && left_candidate != right_candidate {
                            continue;
                        }
                        let left_site = retained_exact_native_site_with_work(
                            self.topology.sources.frame.longitude(),
                            &left_point.x,
                            budget,
                        )?;
                        let right_site = retained_exact_native_site_with_work(
                            self.topology.sources.frame.longitude(),
                            &right_point.x,
                            budget,
                        )?;
                        if left_site.periods() == right_site.periods() {
                            continue;
                        }
                        let (low, high) = if left_site.periods() < right_site.periods() {
                            (
                                left_site.periods(),
                                right_site
                                    .periods()
                                    .checked_sub(1)
                                    .ok_or(ResourceError::SizeOverflow)?,
                            )
                        } else {
                            (
                                right_site.periods(),
                                left_site
                                    .periods()
                                    .checked_sub(1)
                                    .ok_or(ResourceError::SizeOverflow)?,
                            )
                        };
                        if low <= high {
                            ranges.push((low, high));
                        }
                    }
                }
                leaf_ranges.push(ranges);
            }
            recipe_ranges.push(leaf_ranges);
        }
        Ok(recipe_ranges)
    }

    fn prepare_native_reference(
        &mut self,
        drawing: &QEmissionDrawing<'topology>,
        ring_roles: &[bool],
        producer: &AssignedProducerProjection,
        coordinates: &QCoordinates,
        publication_coordinates: &[Option<QCandidate>],
        features: &[NativeCutFeature],
        budget: &mut EvaluationBudget,
    ) -> Result<NativeReference<'topology>> {
        if producer.leaves.len() != self.publication_chords.len() {
            return Err(GeometryErrorKind::Invalid(
                "native reference does not match its assigned producer projection".into(),
            )
            .into());
        }
        let area_facts = native_area_facts(drawing, ring_roles, self.emission.selected_face_roots)?;
        let whole_sphere = matches!(&drawing.polygons, DrawnPolygons::WholeSphere(_));
        let has_area = whole_sphere || !area_facts.is_empty();
        let whole_sphere_region = if whole_sphere {
            Some(
                self.emission
                    .selected_face_roots
                    .iter()
                    .flatten()
                    .copied()
                    .next()
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "whole-sphere reference has no selected region root".into(),
                        )
                    })?,
            )
        } else {
            None
        };
        let selected_poles = native_selected_pole_sectors(
            &self.topology.q,
            self.emission.selected_face_roots,
            budget,
        )?;
        let pole_closure_sectors = native_pole_closure_regions(&selected_poles.sectors, budget)?;
        let mut pole_closures = Vec::new();
        if has_area {
            for closure in selected_poles.closures {
                budget.charge_into_error(1)?;
                let (region, _) = closure;
                // A source fan already supplies its pole port and sector bounds;
                // terminal and isolated sectors need a synthetic closure.
                if !pole_closure_sectors.contains(&closure) {
                    continue;
                }
                let mut area_region = whole_sphere;
                for &(candidate, _) in area_facts.values() {
                    budget.charge_into_error(1)?;
                    area_region |= candidate == region;
                    if area_region {
                        break;
                    }
                }
                if area_region && !pole_closures.contains(&closure) {
                    budget.charge_into_error(1)?;
                    pole_closures.push(closure);
                }
            }
        }
        let mut builder = super::emission::EmissionBuilder::new(
            self.topology,
            self.emission.plan,
            &self.source_inventory,
            whole_sphere,
            budget,
        )?;
        let sample_nodes =
            builder.reserve_publication_nodes(&self.emission.publication.samples, budget)?;
        if sample_nodes != self.publication_sample_nodes {
            return Err(GeometryErrorKind::Invalid(
                "native emission node order differs from publication search".into(),
            )
            .into());
        }
        let mut cut_nodes = Vec::with_capacity(features.len());
        for feature in features {
            budget.charge_into_error(1)?;
            cut_nodes.push(builder.intern_point(&feature.point, budget)?);
        }

        let mut presentation_candidates = {
            let node_count = builder.nodes().nodes().len();
            budget.charge_into_error(
                u64::try_from(node_count).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            vec![None; node_count]
        };
        let whole_sphere_edges = if let Some(region) = whole_sphere_region {
            let QFamilyState::Native(families) = &mut self.family_state else {
                return Err(GeometryErrorKind::Invalid(
                    "whole-sphere presentation preceded Native family activation".into(),
                )
                .into());
            };
            native_whole_sphere_cell_edges(
                &mut builder,
                self.topology.sources.frame,
                region,
                &mut presentation_candidates,
                families,
                budget,
            )?
        } else {
            Vec::new()
        };

        let node_count = builder.nodes().nodes().len();
        let mut has_publication_sample_by_node = vec![false; node_count];
        for &node in &sample_nodes {
            let authority = &builder
                .nodes()
                .nodes()
                .get(node.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("native publication node is unknown".into())
                })?
                .authority;
            if matches!(authority, NodeAuthority::Computed(_)) {
                let slot = has_publication_sample_by_node
                    .get_mut(node.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("native sample node slot is unknown".into())
                    })?;
                *slot = true;
            }
        }
        let mut cut_node = vec![false; node_count];
        for &node in &cut_nodes {
            let slot = cut_node.get_mut(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut node slot is unknown".into())
            })?;
            *slot = true;
        }
        let mut sites = Vec::with_capacity(node_count);
        for (node_index, node) in builder.nodes().nodes().iter().enumerate() {
            let site = match &node.authority {
                NodeAuthority::Source(source) => {
                    let index = source.0 as usize;
                    let candidate = QCandidate {
                        native: coordinates.xy(*source)?,
                        periods: *coordinates.longitude_periods.get(index).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "native source node has no assigned longitude lift".into(),
                            )
                        })?,
                    };
                    Some(assigned_emission_site(
                        self.topology.sources.frame,
                        candidate,
                    )?)
                },
                NodeAuthority::Computed(_) => {
                    let has_publication_sample = *has_publication_sample_by_node
                        .get(node_index)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "native publication node has no sample state".into(),
                            )
                        })?;
                    let publication_candidate = if has_publication_sample {
                        publication_coordinates
                            .get(node_index)
                            .copied()
                            .flatten()
                            .or_else(|| self.publication_nearest.get(node_index).copied().flatten())
                    } else {
                        None
                    };
                    if let Some(candidate) = publication_candidate {
                        Some(assigned_emission_site(
                            self.topology.sources.frame,
                            candidate,
                        )?)
                    } else if let Some(candidate) =
                        presentation_candidates.get(node_index).copied().flatten()
                    {
                        Some(assigned_emission_site(
                            self.topology.sources.frame,
                            candidate,
                        )?)
                    } else if cut_node.get(node_index).copied().unwrap_or(false) {
                        None
                    } else {
                        return Err(GeometryErrorKind::Invalid(
                            "native computed node has no source, publication, or cut owner".into(),
                        )
                        .into());
                    }
                },
            };
            sites.push(site);
        }

        let mut chords_by_recipe = vec![Vec::<usize>::new(); self.emission.fragment_recipes.len()];
        for (chord_index, chord) in self.publication_chords.iter().enumerate() {
            chords_by_recipe
                .get_mut(chord.recipe.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("native producer chord has an unknown recipe".into())
                })?
                .push(chord_index);
        }
        let mut cuts_by_leaf = BTreeMap::<(FragmentRecipeId, usize), Vec<usize>>::new();
        for (feature_index, feature) in features.iter().enumerate() {
            let recipe = feature.family.fragment_recipe().ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut has no fragment family".into())
            })?;
            cuts_by_leaf
                .entry((recipe, feature.trace_edge))
                .or_default()
                .push(feature_index);
        }
        for indices in cuts_by_leaf.values_mut() {
            try_sort_by_with_work(indices, budget, |left, right, budget| {
                budget.charge_into_error(1)?;
                features
                    .get(*left)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("native cut trace parameter is unknown".into())
                    })?
                    .edge_parameter
                    .try_cmp_with_work(
                        &features
                            .get(*right)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "native cut trace parameter is unknown".into(),
                                )
                            })?
                            .edge_parameter,
                        budget,
                    )
            })?;
        }

        let mut expanded_traces = Vec::with_capacity(self.emission.publication.fragments.len());
        let zero = ExactRational::zero();
        let one = ExactRational::one();
        for (recipe_index, fragment) in self.emission.publication.fragments.iter().enumerate() {
            budget.charge_into_error(1)?;
            if fragment.recipe.0 as usize != recipe_index {
                return Err(GeometryErrorKind::Invalid(
                    "native source traces are not recipe-aligned".into(),
                )
                .into());
            }
            let edge_count = fragment.trace.rows.edge_count();
            let mut slices = Vec::with_capacity(edge_count);
            for edge in 0..edge_count {
                budget.charge_into_error(1)?;
                slices.push((edge, zero.clone(), one.clone()));
            }
            expanded_traces.push(fragment.trace.compose_edges(&slices, budget)?);
        }

        let port_points = self.global_q_port_points(coordinates, budget)?;
        let mut area_edges = whole_sphere_edges;
        let mut expected_area_leaves = Vec::new();
        let mut expected_line_leaves = Vec::new();
        let mut seam_events = Vec::<NativeSeamEventData>::new();
        let mut line_traces = Vec::new();
        let mut literal_chord_uses = Vec::new();
        let mut line_graph_edges = Vec::new();
        let mut reference_ports = Vec::new();
        for walk in self.emission.fragment_walks {
            if whole_sphere && matches!(walk.owner, RecipeWalkOwner::SourceRing(_)) {
                continue;
            }
            let area_region = match walk.owner {
                RecipeWalkOwner::Line(_) => None,
                owner @ (RecipeWalkOwner::Cycle(_) | RecipeWalkOwner::SourceRing(_)) => Some(
                    area_facts
                        .get(&owner)
                        .map(|(region, _)| *region)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "native area walk has no selected-region fact".into(),
                            )
                        })?,
                ),
            };
            let line_member = match walk.owner {
                RecipeWalkOwner::Line(member) => Some(member),
                RecipeWalkOwner::Cycle(_) | RecipeWalkOwner::SourceRing(_) => None,
            };
            let mut line_run = NativeLineRun {
                member: line_member,
                ..NativeLineRun::default()
            };
            for &recipe_id in &walk.fragments {
                let fragment = self
                    .emission
                    .publication
                    .fragments
                    .get(recipe_id.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "native recipe walk has no published fragment".into(),
                        )
                    })?;
                let recipe = self
                    .emission
                    .fragment_recipes
                    .get(recipe_id.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("native recipe walk is unknown".into())
                    })?;
                if area_region.is_some()
                    != matches!(recipe.use_kind, super::emission::SelectedUse::AreaBoundary)
                {
                    return Err(GeometryErrorKind::Invalid(
                        "native recipe use does not match its selected walk".into(),
                    )
                    .into());
                }
                let chord_indices =
                    chords_by_recipe.get(recipe_id.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "native recipe has no assigned producer chords".into(),
                        )
                    })?;
                let edge_count = fragment.points.len().checked_sub(1).ok_or_else(|| {
                    GeometryErrorKind::Invalid("native producer fragment is empty".into())
                })?;
                if chord_indices.len() != edge_count {
                    return Err(GeometryErrorKind::Invalid(
                        "native producer chord count differs from its trace leaves".into(),
                    )
                    .into());
                }
                for (trace_edge, &chord_index) in chord_indices.iter().enumerate() {
                    let chord = self.publication_chords.get(chord_index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("native producer chord is unknown".into())
                    })?;
                    let assigned_leaf = producer.leaves.get(chord_index).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "native producer leaf projection is unknown".into(),
                        )
                    })?;
                    if assigned_leaf.recipe != chord.recipe
                        || assigned_leaf.trace_edge != chord.trace_edge
                    {
                        return Err(GeometryErrorKind::Invalid(
                            "native producer leaf projection changed its writer identity".into(),
                        )
                        .into());
                    }
                    if chord.recipe != recipe_id || chord.trace_edge != trace_edge {
                        return Err(GeometryErrorKind::Invalid(
                            "native producer leaves are not in recipe order".into(),
                        )
                        .into());
                    }
                    if area_region.is_some() {
                        budget.charge_into_error(1)?;
                        expected_area_leaves.push((recipe_id, trace_edge));
                    } else {
                        budget.charge_into_error(1)?;
                        expected_line_leaves.push((recipe_id, trace_edge));
                    }
                    let no_cuts = Vec::new();
                    let feature_indices = cuts_by_leaf
                        .get(&(recipe_id, trace_edge))
                        .unwrap_or(&no_cuts);
                    let expanded_trace =
                        expanded_traces.get(recipe_id.0 as usize).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "native producer has no expanded source trace".into(),
                            )
                        })?;
                    let pieces = self.native_chord_pieces(
                        &mut builder,
                        chord,
                        &assigned_leaf.points,
                        expanded_trace,
                        area_region,
                        feature_indices,
                        features,
                        &sites,
                        budget,
                    )?;
                    let NativeChordPieces {
                        edges,
                        break_after,
                        seam_events: piece_seam_events,
                    } = pieces;
                    seam_events.extend(piece_seam_events);
                    if area_region.is_some() {
                        area_edges.extend(edges);
                    } else {
                        for (edge, break_after) in edges.into_iter().zip(break_after) {
                            append_native_line_edge(
                                self.topology.sources.frame,
                                &mut line_run,
                                edge,
                                &mut line_graph_edges,
                                budget,
                            )?;
                            if break_after && !line_run.chords.is_empty() {
                                let trace_line = line_traces.len();
                                line_traces.push(finish_native_line_run(
                                    &mut builder,
                                    std::mem::take(&mut line_run),
                                    &mut reference_ports,
                                    trace_line,
                                    &mut literal_chord_uses,
                                    budget,
                                )?);
                                line_run.member = line_member;
                            }
                        }
                    }
                }
            }
            if area_region.is_none() && !line_run.chords.is_empty() {
                let trace_line = line_traces.len();
                line_traces.push(finish_native_line_run(
                    &mut builder,
                    line_run,
                    &mut reference_ports,
                    trace_line,
                    &mut literal_chord_uses,
                    budget,
                )?);
            }
        }

        for vertex in &drawing.points {
            let node = builder.source_node(vertex.port.node)?;
            let site = sites
                .get(node.0 as usize)
                .and_then(Option::as_ref)
                .copied()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("native isolated point has no assigned site".into())
                })?;
            let chart_point = port_points
                .get(vertex.port.id.0 as usize)
                .and_then(Option::as_ref)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("native isolated point has no chart spelling".into())
                })?;
            let frame = self.topology.sources.frame;
            let half = ExactRational::from_f64(frame.longitude().half_period());
            let negative_half = ExactRational::zero().try_subtract_with_work(&half, budget)?;
            let bank = if chart_point.x.try_cmp_with_work(&half, budget)?.is_eq() {
                Some(super::emission::ChartSide::East)
            } else if chart_point
                .x
                .try_cmp_with_work(&negative_half, budget)?
                .is_eq()
            {
                Some(super::emission::ChartSide::West)
            } else {
                None
            };
            budget.charge_into_error(1)?;
            let port_data = native_port_data(
                frame,
                node,
                candidate_from_emission_site(site),
                chart_point,
                None,
                vec![OccurrenceSource {
                    origin: super::emission::VertexOrigin::SourceNode(vertex.port.node),
                    source_trace: None,
                }],
                bank,
                budget,
            )?;
            let spelling = port_data.spelling.clone();
            reference_ports.push(port_data);
            let port = builder.source_port(
                vertex.port.id,
                super::emission::VertexOrigin::SourceNode(vertex.port.node),
                Some(vertex.point),
                spelling,
            )?;
            builder.point(port)?;
        }

        let known_span_boundaries = native_known_span_boundaries(
            self.topology,
            &area_edges,
            &line_graph_edges,
            &builder,
            &port_points,
            &sites,
            budget,
        )?;
        area_edges = common_refine_native_edges(
            area_edges,
            &known_span_boundaries,
            self.topology.sources.frame,
            budget,
        )?;
        line_graph_edges = common_refine_native_edges(
            line_graph_edges,
            &known_span_boundaries,
            self.topology.sources.frame,
            budget,
        )?;
        validate_native_source_ranges(
            self.topology,
            self.emission.selected_face_roots,
            &area_edges,
            &expected_area_leaves,
            &expanded_traces,
            true,
            budget,
        )?;
        validate_native_source_ranges(
            self.topology,
            self.emission.selected_face_roots,
            &line_graph_edges,
            &expected_line_leaves,
            &expanded_traces,
            false,
            budget,
        )?;
        normalize_native_chart_lifts(
            &mut area_edges,
            &mut line_graph_edges,
            &mut reference_ports,
            self.topology.sources.frame,
            budget,
        )?;
        let mut graph_builder =
            new_native_reference_graph_builder(&area_edges, &reference_ports, budget)?;
        let mut graph = if has_area {
            if !whole_sphere {
                let mut band_events = seam_events
                    .iter()
                    .map(|event| event.event.clone())
                    .collect::<Vec<_>>();
                let bands = native_seam_bands_from_assigned_graph(
                    &graph_builder,
                    &area_edges,
                    self.topology.sources.frame,
                    &mut band_events,
                    budget,
                )?;
                for (pole, band) in [
                    (ChartPole::South, bands.first()),
                    (ChartPole::North, bands.last()),
                ] {
                    if let Some(band) = band {
                        for region in [band.west_region, band.east_region].into_iter().flatten() {
                            if !pole_closures.contains(&(region, pole)) {
                                return Err(GeometryErrorKind::Invalid(
                                    "assigned seam pole has no selected source-sector germ".into(),
                                )
                                .into());
                            }
                        }
                    }
                }
                for (pole, latitude) in [
                    (
                        ChartPole::South,
                        self.topology
                            .sources
                            .frame
                            .chart_pole_latitude(ChartPole::South),
                    ),
                    (
                        ChartPole::North,
                        self.topology
                            .sources
                            .frame
                            .chart_pole_latitude(ChartPole::North),
                    ),
                ] {
                    if !pole_closures.iter().any(|&(_, selected)| selected == pole) {
                        continue;
                    }
                    let latitude = ExactRational::from_f64(latitude);
                    let chart_point =
                        ExactChartPoint::from_rationals(ExactRational::zero(), latitude.clone());
                    let node = builder.intern_point(&chart_point, budget)?;
                    let node_index = node.0 as usize;
                    if node_index == sites.len() {
                        let [_, longitude, _] = rounded_native_neighbors_with_work(
                            self.topology.sources.frame.longitude(),
                            &chart_point.x,
                            budget,
                        )?;
                        sites.push(Some(assigned_emission_site(
                            self.topology.sources.frame,
                            QCandidate {
                                native: XY::new(
                                    longitude.native,
                                    latitude.round_with_work(budget)?,
                                ),
                                periods: longitude.periods,
                            },
                        )?));
                    } else if node_index > sites.len() {
                        return Err(GeometryErrorKind::Invalid(
                            "native pole node is outside its assigned site table".into(),
                        )
                        .into());
                    } else if sites
                        .get(node_index)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "native pole node has no assigned site slot".into(),
                            )
                        })?
                        .is_none()
                    {
                        let [_, longitude, _] = rounded_native_neighbors_with_work(
                            self.topology.sources.frame.longitude(),
                            &chart_point.x,
                            budget,
                        )?;
                        *sites.get_mut(node_index).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "native pole node has no assigned site slot".into(),
                            )
                        })? = Some(assigned_emission_site(
                            self.topology.sources.frame,
                            QCandidate {
                                native: XY::new(
                                    longitude.native,
                                    latitude.round_with_work(budget)?,
                                ),
                                periods: longitude.periods,
                            },
                        )?);
                    }
                    let site = sites
                        .get(node_index)
                        .and_then(Option::as_ref)
                        .copied()
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid("native pole site is unknown".into())
                        })?;
                    let candidate = candidate_from_emission_site(site);
                    let east = native_port_data(
                        self.topology.sources.frame,
                        node,
                        candidate,
                        &chart_point,
                        None,
                        vec![OccurrenceSource {
                            origin: super::emission::VertexOrigin::Presentation,
                            source_trace: None,
                        }],
                        Some(super::emission::ChartSide::East),
                        budget,
                    )?;
                    let west = native_port_data(
                        self.topology.sources.frame,
                        node,
                        candidate,
                        &chart_point,
                        None,
                        vec![OccurrenceSource {
                            origin: super::emission::VertexOrigin::Presentation,
                            source_trace: None,
                        }],
                        Some(super::emission::ChartSide::West),
                        budget,
                    )?;
                    seam_events.push(NativeSeamEventData {
                        event: NativeSeamEvent {
                            latitude: latitude.clone(),
                            output_vertex: true,
                        },
                        vertices: NativeSeamVertices {
                            latitude,
                            east,
                            west,
                        },
                    });
                }

                let normalized_events =
                    normalize_native_seam_events(self.topology.sources.frame, seam_events, budget)?;
                budget.charge_into_error(
                    u64::try_from(normalized_events.len())
                        .map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                let mut native_seam_vertices = Vec::with_capacity(normalized_events.len());
                for event in &normalized_events {
                    budget.charge_into_error(1)?;
                    native_seam_vertices.push(NativeSeamVertices {
                        latitude: event.vertices.latitude.try_clone_with_work(budget)?,
                        east: event.vertices.east.try_clone_with_work(budget)?,
                        west: event.vertices.west.try_clone_with_work(budget)?,
                    });
                }
                let seam_edges = native_seam_edges(
                    &bands,
                    &native_seam_vertices,
                    &mut |region, start, end, budget| {
                        graph_builder.has_area_edge(region, start, end, budget)
                    },
                    budget,
                )?;
                area_edges.extend(seam_edges);
                graph_builder.append_edges(&area_edges, budget)?;
                let pole_edges = native_pole_edges(
                    self.topology.sources.frame,
                    &native_seam_vertices,
                    &pole_closures,
                    &area_edges,
                    &line_graph_edges,
                    &reference_ports,
                    &mut |region, start, end, budget| {
                        graph_builder.has_area_edge(region, start, end, budget)
                    },
                    budget,
                )?;
                budget.charge_into_error(
                    u64::try_from(pole_edges.len()).map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                let mut materialized_poles = Vec::with_capacity(pole_edges.len());
                for edge in &pole_edges {
                    budget.charge_into_error(1)?;
                    let NativeAreaEdgeKind::Restriction(restriction) = &edge.kind else {
                        return Err(GeometryErrorKind::Invalid(
                            "native pole constructor returned a non-pole edge".into(),
                        )
                        .into());
                    };
                    let ChartRestriction::PolarCap { pole } = restriction.restriction else {
                        return Err(GeometryErrorKind::Invalid(
                            "native pole constructor returned a non-polar restriction".into(),
                        )
                        .into());
                    };
                    let NativeGraphEdgeOwner::Area { region } = edge.owner else {
                        return Err(GeometryErrorKind::Invalid(
                            "native pole presentation has no selected-region owner".into(),
                        )
                        .into());
                    };
                    materialized_poles.push((region, pole));
                }
                try_sort_by_with_work(&mut materialized_poles, budget, |left, right, budget| {
                    budget.charge_into_error(1)?;
                    Ok(left.cmp(right))
                })?;
                budget.charge_into_error(
                    u64::try_from(materialized_poles.len())
                        .map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                materialized_poles.dedup();
                self.register_native_pole_families(&materialized_poles, budget)?;
                area_edges.extend(pole_edges);
            }

            graph_builder.append_edges(&area_edges, budget)?;
            graph_builder.append_line_edges(area_edges.len(), &line_graph_edges, budget)?;
            let mut graph = graph_builder.finish(self.topology.sources.frame, budget)?;
            graph.face_source_uses =
                native_graph_face_source_table(&graph, &area_edges, &line_graph_edges, budget)?;
            let NativeReferenceGraphResult {
                mut cycles,
                parts,
                face_regions,
                face_groups,
                face_cells,
            } = native_reference_cycles_and_parts(
                &graph,
                &area_edges,
                &line_graph_edges,
                whole_sphere,
                true,
                budget,
            )?;
            align_native_cycle_starts(
                self.topology.sources.frame,
                self.topology,
                drawing,
                ring_roles,
                builder.nodes(),
                &graph,
                &mut cycles,
                budget,
            )?;
            graph.cycles = cycles;
            graph.parts = parts;
            graph.face_regions = face_regions;
            graph.face_groups = face_groups;
            graph.face_cells = face_cells;
            let mut cycle_chords = Vec::with_capacity(graph.cycles.len());
            for cycle in &graph.cycles {
                let edge_count = cycle.edges.len();
                let mut ports = Vec::with_capacity(edge_count);
                for vertex_index in 0..edge_count {
                    let previous_index = if vertex_index == 0 {
                        edge_count.checked_sub(1).ok_or_else(|| {
                            GeometryErrorKind::Invalid("native area cycle is empty".into())
                        })?
                    } else {
                        vertex_index
                            .checked_sub(1)
                            .ok_or(ResourceError::SizeOverflow)?
                    };
                    let incoming = cycle.edges.get(previous_index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("native graph cycle has no incoming atom".into())
                    })?;
                    let outgoing = cycle.edges.get(vertex_index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("native graph cycle has no outgoing atom".into())
                    })?;
                    let incoming_vertex = native_graph_cycle_vertex(
                        self.topology.sources.frame,
                        &graph,
                        incoming,
                        1,
                        &area_edges,
                        budget,
                    )?;
                    let outgoing_vertex = native_graph_cycle_vertex(
                        self.topology.sources.frame,
                        &graph,
                        outgoing,
                        0,
                        &area_edges,
                        budget,
                    )?;
                    let vertex = merge_native_port_data(
                        self.topology.sources.frame,
                        &incoming_vertex,
                        &outgoing_vertex,
                        budget,
                    )?;
                    ports.push(builder.port(vertex)?);
                }
                let mut chords = Vec::with_capacity(edge_count);
                for (index, cycle_edge) in cycle.edges.iter().enumerate() {
                    let next = index
                        .checked_add(1)
                        .ok_or(ResourceError::SizeOverflow)?
                        .checked_rem(edge_count)
                        .ok_or(ResourceError::SizeOverflow)?;
                    let start = *ports.get(index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("native area start port is unknown".into())
                    })?;
                    let end = *ports.get(next).ok_or_else(|| {
                        GeometryErrorKind::Invalid("native area end port is unknown".into())
                    })?;
                    let recipe = native_graph_cycle_recipe(&graph, cycle_edge, &area_edges)?;
                    let chord = builder.chord([start, end], recipe)?;
                    if chord.0 as usize != literal_chord_uses.len() {
                        return Err(GeometryErrorKind::Invalid(
                            "native area chord is out of order in the literal-use map".into(),
                        )
                        .into());
                    }
                    budget.charge_into_error(
                        u64::try_from(cycle_edge.contributors.len())
                            .map_err(|_| ResourceError::SizeOverflow)?,
                    )?;
                    literal_chord_uses.push(NativeLiteralChordUse {
                        chord,
                        authority: NativeLiteralChordAuthority::ReferenceAtom {
                            atom: cycle_edge.atom,
                            reversed: cycle_edge.reversed,
                            writer_edges: cycle_edge.contributors.clone(),
                        },
                    });
                    chords.push(chord);
                }
                cycle_chords.push(chords);
            }
            for part in &graph.parts {
                let shell = cycle_chords.get(part.shell).cloned().ok_or_else(|| {
                    GeometryErrorKind::Invalid("native part shell cycle is unknown".into())
                })?;
                let holes = part
                    .holes
                    .iter()
                    .map(|&hole| {
                        cycle_chords.get(hole).cloned().ok_or_else(|| {
                            GeometryErrorKind::Invalid("native part hole cycle is unknown".into())
                                .into()
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                builder.part(part.face, part.region, shell, holes)?;
            }
            graph
        } else {
            graph_builder.append_line_edges(area_edges.len(), &line_graph_edges, budget)?;
            let mut graph = graph_builder.finish(self.topology.sources.frame, budget)?;
            graph.face_source_uses =
                native_graph_face_source_table(&graph, &area_edges, &line_graph_edges, budget)?;
            let NativeReferenceGraphResult {
                cycles,
                parts,
                face_regions,
                face_groups,
                face_cells,
            } = native_reference_cycles_and_parts(
                &graph,
                &area_edges,
                &line_graph_edges,
                whole_sphere,
                false,
                budget,
            )?;
            graph.cycles = cycles;
            graph.parts = parts;
            graph.face_regions = face_regions;
            graph.face_groups = face_groups;
            graph.face_cells = face_cells;
            graph
        };

        let plan = builder.finish();
        if literal_chord_uses.len() != plan.chords.len() {
            return Err(GeometryErrorKind::Invalid(
                "native literal chord-use rows do not cover the emission plan".into(),
            )
            .into());
        }
        for (index, use_row) in literal_chord_uses.iter().enumerate() {
            budget.charge_into_error(1)?;
            if use_row.chord.0 as usize != index {
                return Err(GeometryErrorKind::Invalid(
                    "native literal chord-use rows are not chord-indexed".into(),
                )
                .into());
            }
        }
        budget.charge_into_error(
            u64::try_from(plan.ports.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut port_vertices = Vec::with_capacity(plan.ports.len());
        for port in &plan.ports {
            budget.charge_into_error(1)?;
            port_vertices.push(graph.local_vertex(native_emission_port_key(port), budget)?);
        }
        let point_contact_vertices = native_point_contact_vertices(&plan, &port_vertices, budget)?;
        graph.common = graph
            .common
            .with_output_point_contacts(&point_contact_vertices, budget)?;
        let logical_walks = logical_layout_for_plan(&plan, &port_vertices, budget)?;
        let dimension =
            admit_logical_layout(&graph.common, logical_walks, Some(&graph.faces), budget)?;
        let reference = NativeReference {
            plan,
            dimension,
            port_vertices,
            sites,
            feature_nodes: cut_nodes,
            publication_sample_nodes: sample_nodes,
            line_traces,
            literal_chord_uses,
            line_graph_edges,
            area_edges,
            pole_sectors: selected_poles.sectors,
            graph,
        };
        validate_native_reference(&reference, budget)?;
        Ok(reference)
    }

    fn complete_native_drawing(
        &self,
        reference: &NativeReference<'topology>,
        features: &[NativeCutFeature],
        feature_candidates: &[QCandidate],
        budget: &mut EvaluationBudget,
    ) -> Result<Option<super::emission::CompletedNativeDrawing>> {
        if reference.feature_nodes.len() != feature_candidates.len()
            || features.len() != feature_candidates.len()
        {
            return Err(GeometryErrorKind::Invalid(
                "native cut feature nodes and candidates are misaligned".into(),
            )
            .into());
        }
        budget.charge_into_error(
            u64::try_from(reference.sites.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut sites = reference.sites.clone();
        let mut assigned_by_node = vec![None; reference.sites.len()];
        for (&node, &candidate) in reference.feature_nodes.iter().zip(feature_candidates) {
            budget.charge_into_error(1)?;
            let slot = assigned_by_node.get_mut(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut node is outside its reference".into())
            })?;
            if slot.is_some_and(|previous| previous != candidate) {
                return Err(GeometryErrorKind::Invalid(
                    "native cut aliases have conflicting assigned sites".into(),
                )
                .into());
            }
            *slot = Some(candidate);
        }
        for (node_index, candidate) in assigned_by_node.into_iter().enumerate() {
            let Some(candidate) = candidate else {
                continue;
            };
            let site = assigned_emission_site(self.topology.sources.frame, candidate)?;
            let reference_site = sites.get_mut(node_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut node site is unknown".into())
            })?;
            if let Some(previous) = reference_site
                && *previous != site
            {
                return Err(GeometryErrorKind::Invalid(
                    "native cut assignment conflicts with its fixed reference node".into(),
                )
                .into());
            }
            *reference_site = Some(site);
        }
        let mut assigned_sites = Vec::with_capacity(sites.len());
        for site in sites {
            budget.charge_into_error(1)?;
            assigned_sites.push(site.ok_or_else(|| {
                GeometryErrorKind::Invalid("native reference node has no assigned site".into())
            })?);
        }
        let (ordinates, axes) = super::emission::resolve_emission_ordinates(
            &reference.plan,
            self.emission.publication,
            &self.ordinates,
            self.topology,
            self.declared_axes,
            self.strict_axes,
            self.operation,
            self.metric_access,
            budget,
        )?;
        let parts = super::emission::materialize_native_parts(
            &reference.plan,
            &assigned_sites,
            &ordinates,
            axes,
            &reference.line_traces,
            budget,
        )?;
        let Some(mut candidate) = validate_native_materialized_candidate(
            reference,
            &parts,
            &assigned_sites,
            &ordinates,
            budget,
        )?
        else {
            return Ok(None);
        };
        let frozen = self
            .emission
            .plan
            .seal_native_parts(parts, self.declared_axes, budget)?;
        if !std::ptr::eq(candidate.assignment.reference, reference) {
            return Err(GeometryErrorKind::Invalid(
                "native completion assignment belongs to another reference".into(),
            )
            .into());
        }
        let line_traces = frozen.line_traces;
        let receipt = NativeCandidatePublication {
            assignment: &candidate.assignment,
            buffers: frozen.buffers,
            sites: &assigned_sites,
            ordinates: &ordinates,
            port_records: &mut candidate.port_records,
        };
        match finish_geographic(
            PublicationCall::new(
                ProducerReceipt::QNative(receipt),
                self.topology.publication_frame().clone(),
                budget,
            ),
            StorageTarget::Scalar,
        )? {
            PublicationOutcome::Pass(sealed) => Ok(Some(super::emission::CompletedNativeDrawing {
                sealed,
                line_traces,
            })),
            PublicationOutcome::Reject(PublicationRejection::Native) => Ok(None),
            PublicationOutcome::Reject(
                PublicationRejection::SourceOnly(_)
                | PublicationRejection::Batch(_)
                | PublicationRejection::ProducerDrawing(_),
            ) => Err(GeometryErrorKind::Invalid(
                "native Q finisher returned a non-native rejection".into(),
            )
            .into()),
        }
    }

    fn assigned_publication_chart_points(
        &self,
        producer: &AssignedProducerProjection,
        budget: &mut impl Work,
    ) -> Result<Vec<ExactChartPoint>> {
        self.checked_publication_sample_images(producer, budget)
    }

    fn checked_publication_sample_images(
        &self,
        producer: &AssignedProducerProjection,
        work: &mut impl Work,
    ) -> Result<Vec<ExactChartPoint>> {
        let sample_count = self.emission.publication.samples.len();
        if self.publication_sample_nodes.len() != sample_count
            || self.publication_sample_source_ports.len() != sample_count
            || producer.leaves.len() != self.publication_chords.len()
        {
            return Err(GeometryErrorKind::Invalid(
                "publication sample receipts do not match their checked producer leaves".into(),
            )
            .into());
        }
        work.charge_into_error(
            u64::try_from(sample_count).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut images = vec![None; sample_count];
        for (chord, leaf) in self.publication_chords.iter().zip(&producer.leaves) {
            work.charge_into_error(1)?;
            if chord.recipe != leaf.recipe || chord.trace_edge != leaf.trace_edge {
                return Err(GeometryErrorKind::Invalid(
                    "publication leaf projection lost its recipe/trace-edge identity".into(),
                )
                .into());
            }
            for side in 0..2 {
                work.charge_into_error(1)?;
                let endpoint = chord.endpoints.get(side).ok_or_else(|| {
                    GeometryErrorKind::Invalid("checked publication endpoint is unknown".into())
                })?;
                let Some(sample_index) = publication_sample_index(endpoint) else {
                    continue;
                };
                let sample = self
                    .emission
                    .publication
                    .samples
                    .get(sample_index)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("checked publication sample is unknown".into())
                    })?;
                let reference = chord.exact.get(side).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "checked publication endpoint lost its exact reference".into(),
                    )
                })?;
                if !reference.try_eq_with_work(&sample.exact_point, work)? {
                    return Err(GeometryErrorKind::Invalid(
                        "checked publication endpoint changed its frozen sample reference".into(),
                    )
                    .into());
                }
                let point = leaf.points.get(side).cloned().ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "checked publication leaf has no endpoint image".into(),
                    )
                })?;
                let slot = images.get_mut(sample_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "publication sample maps outside the image table".into(),
                    )
                })?;
                if slot.as_ref().is_some_and(|previous| previous != &point) {
                    return Err(GeometryErrorKind::Invalid(
                        "adjacent checked leaves disagree on one sample image".into(),
                    )
                    .into());
                }
                *slot = Some(point);
            }
        }
        let mut quotient_images = BTreeMap::<EmissionNodeId, ExactQuotientKey>::new();
        let mut checked = Vec::with_capacity(sample_count);
        for (sample_index, image) in images.into_iter().enumerate() {
            work.charge_into_error(1)?;
            let image = image.ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "publication sample has no checked source-port or leaf image".into(),
                )
            })?;
            let node = *self
                .publication_sample_nodes
                .get(sample_index)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "publication sample has no checked node identity".into(),
                    )
                })?;
            let key = quotient_key_for_chart_position_with_work(
                self.topology.sources.frame,
                &image,
                work,
            )?;
            if let Some(previous) = quotient_images.get(&node) {
                if !compare_quotient_keys_with_work(previous, &key, work)?.is_eq() {
                    return Err(GeometryErrorKind::Invalid(
                        "publication occurrences of one node have different assigned quotient images"
                            .into(),
                    )
                    .into());
                }
            } else {
                quotient_images.insert(node, key);
            }
            checked.push(image);
        }
        Ok(checked)
    }

    fn check_global_candidate(
        &mut self,
        coordinates: &QCoordinates,
        publication_coordinates: &[Option<QCandidate>],
        drawing: QEmissionDrawing<'topology>,
        budget: &mut impl SearchAccount,
    ) -> Result<GlobalCheck<QEmbeddingOutput>> {
        let live_keys = drawing.live_node_keys(self.topology.sources.frame, budget)?;
        let (identity_conflicts, identity_unrepresentable) = self.publication_identity_conflicts(
            coordinates,
            &live_keys,
            publication_coordinates,
            budget,
        )?;
        if identity_unrepresentable {
            return Ok(GlobalCheck::Reject(Vec::new()));
        }
        if !identity_conflicts.is_empty() {
            return Ok(GlobalCheck::Reject(
                self.classes_for_variables(identity_conflicts, budget)?,
            ));
        }
        let source_faces = if let Some(exact_faces) = &self.exact_faces {
            let chart = subgraph_chart(
                self.topology,
                self.emission,
                &self.nearest,
                Some(coordinates),
                budget,
            )?;
            let emitted_faces = QFaceStructure::build(self.topology, self.emission, chart)?;
            if let Some(nodes) = face_structure_mismatch_nodes(
                exact_faces,
                &emitted_faces,
                &self.emission.selected_nodes,
            ) {
                return Ok(GlobalCheck::Reject(self.classes_for_nodes(nodes, budget)?));
            }
            Some(emitted_faces)
        } else {
            None
        };
        let mut producer_projection = {
            let source_faces_for_certification = source_faces;
            self.certify_assigned_producer(
                coordinates,
                publication_coordinates,
                source_faces_for_certification.as_ref(),
                budget,
            )?
        };
        let publication_rejections =
            self.publication_graph_rejections(&producer_projection, budget)?;
        if !publication_rejections.is_empty() {
            return Ok(GlobalCheck::Reject(
                self.classes_for_variables(publication_rejections, budget)?,
            ));
        }
        match check_q_ring_roles(self.topology, &drawing, budget)? {
            QRingRoleCheck::Pass(ring_roles) => {
                if !producer_projection.rejections.is_empty() {
                    let rejections = std::mem::take(&mut producer_projection.rejections);
                    return Ok(GlobalCheck::Reject(
                        self.classes_for_variables(rejections, budget)?,
                    ));
                }
                let native_required = self.actual_native_demand(
                    &drawing,
                    &ring_roles,
                    &producer_projection,
                    coordinates,
                    publication_coordinates,
                    budget,
                )?;
                let sealed = if native_required {
                    let budget = budget.budget_mut();
                    self.freeze_native_families(budget)?;
                    let native_cut_features =
                        self.materialize_native_cut_features(&producer_projection, budget)?;
                    let prepared_reference = self.prepare_native_reference(
                        &drawing,
                        &ring_roles,
                        &producer_projection,
                        coordinates,
                        publication_coordinates,
                        &native_cut_features,
                        budget,
                    );
                    let reference = match native_reference_global_check(prepared_reference, || {
                        self.all_outer_repair_classes(budget)
                    })? {
                        GlobalCheck::Pass(reference) => reference,
                        GlobalCheck::Reject(classes) => {
                            return Ok(GlobalCheck::Reject(classes));
                        },
                    };
                    let native = match self.search_native_cut_candidates(
                        &reference,
                        &native_cut_features,
                        coordinates,
                        publication_coordinates,
                        budget,
                    )? {
                        NativeCutSearchOutcome::Pass(drawing) => drawing,
                        NativeCutSearchOutcome::Reject(classes) => {
                            return Ok(GlobalCheck::Reject(classes));
                        },
                    };
                    super::emission::SealedQDrawing::Native(native)
                } else {
                    let live_publication_samples =
                        super::emission::source_publication::source_live_publication_samples(
                            self.emission.plan,
                            self.emission.publication,
                            budget,
                        )?;
                    let publication_chart_points =
                        self.assigned_publication_chart_points(&producer_projection, budget)?;
                    let publication_points = publication_chart_points
                        .iter()
                        .map(rounded_chart_point)
                        .collect::<Result<Vec<_>>>()?;
                    let publication_ordinates = super::emission::resolve_publication_ordinates(
                        self.topology,
                        self.emission.publication,
                        &live_publication_samples,
                        self.declared_axes,
                        self.metric_access,
                        budget,
                    )?;
                    let source_publication =
                        super::emission::source_publication::SourcePublication {
                            inventory: self.emission.publication,
                            points: &publication_points,
                            ordinates: &publication_ordinates,
                            live_samples: &live_publication_samples,
                        };
                    let source = self.emission.plan.seal_source_drawing(
                        drawing,
                        &ring_roles,
                        &self.ordinates,
                        Some(source_publication),
                        Some(&producer_projection.source_ports),
                        self.declared_axes,
                        self.strict_axes,
                        self.operation,
                        budget,
                    )?;
                    let receipt = SourceOnlyPublication {
                        source,
                        topology: self.topology,
                        source_ports: Some(&producer_projection.source_ports),
                        source_ordinates: &self.ordinates,
                        publication: Some(self.emission.publication),
                        publication_images: &publication_chart_points,
                        publication_points: &publication_points,
                        publication_ordinates: &publication_ordinates,
                    };
                    let sealed = match finish_geographic(
                        PublicationCall::new(
                            ProducerReceipt::QSourceOnly(receipt),
                            self.topology.publication_frame().clone(),
                            budget,
                        ),
                        StorageTarget::Scalar,
                    )? {
                        PublicationOutcome::Pass(finished) => finished,
                        PublicationOutcome::Reject(PublicationRejection::SourceOnly(images)) => {
                            let mut variables = Vec::with_capacity(images.len());
                            for image in images {
                                budget.charge_into_error(1)?;
                                variables.push(match image {
                                    image @ (QSourceImage::Port(_)
                                    | QSourceImage::SourcePoint(_)
                                    | QSourceImage::SourceRingStart(_)
                                    | QSourceImage::SourceRingEnd(_)
                                    | QSourceImage::CollapsedStart(_)
                                    | QSourceImage::CollapsedEnd(_)) => {
                                        let port = q_source_image_port(self.topology, image)?;
                                        let node = self
                                            .topology
                                            .q
                                            .ports
                                            .get(port.0 as usize)
                                            .map(|port| port.node)
                                            .ok_or_else(|| {
                                                GeometryErrorKind::Invalid(
                                                    "rejected source literal port is unknown"
                                                        .into(),
                                                )
                                            })?;
                                        QVariableNode::Source(node)
                                    },
                                    QSourceImage::PublicationSample(sample) => {
                                        let node = self
                                            .publication_sample_nodes
                                            .get(sample as usize)
                                            .copied()
                                            .ok_or_else(|| {
                                                GeometryErrorKind::Invalid(
                                                    "rejected publication literal has no node"
                                                        .into(),
                                                )
                                            })?;
                                        QVariableNode::Published(node)
                                    },
                                });
                            }
                            return Ok(GlobalCheck::Reject(
                                self.classes_for_variables(variables, budget)?,
                            ));
                        },
                        PublicationOutcome::Reject(PublicationRejection::Native) => {
                            return Err(GeometryErrorKind::Invalid(
                                "source-only Q finisher returned native rejection".into(),
                            )
                            .into());
                        },
                        PublicationOutcome::Reject(PublicationRejection::Batch(_)) => {
                            return Err(GeometryErrorKind::Invalid(
                                "source-only Q finisher returned array batch rejection".into(),
                            )
                            .into());
                        },
                        PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(_)) => {
                            return Err(GeometryErrorKind::Invalid(
                                "source-only Q finisher returned producer-drawn rejection".into(),
                            )
                            .into());
                        },
                    };
                    super::emission::SealedQDrawing::Source(SealedSourceDrawing {
                        sealed,
                        line_traces: Vec::new(),
                    })
                };
                Ok(GlobalCheck::Pass(QEmbeddingOutput {
                    sealed,
                    family_state: None,
                }))
            },
            QRingRoleCheck::Reject { nodes, .. } => {
                Ok(GlobalCheck::Reject(self.classes_for_nodes(nodes, budget)?))
            },
        }
    }

    fn local_candidate(
        &self,
        node: QNodeId,
        class: usize,
        assigned: &[Option<QCandidate>],
    ) -> Result<QCandidate> {
        match self.node_location.get(node.0 as usize).copied().flatten() {
            Some((owner, rank)) if owner == class => {
                assigned.get(rank).copied().flatten().ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "Q local check reached an unassigned dependency".into(),
                    )
                    .into()
                })
            },
            _ => {
                let index = node.0 as usize;
                Ok(QCandidate {
                    native: self.nearest.xy(node)?,
                    periods: *self.nearest.longitude_periods.get(index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q node has no retained longitude lift".into())
                    })?,
                })
            },
        }
    }

    fn candidate_cut_latitude(
        &self,
        cut_index: usize,
        class: usize,
        assigned: &[Option<QCandidate>],
        budget: &mut impl Work,
    ) -> Result<Option<ExactRational>> {
        let cut_row = self.cut_rows.get(cut_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q cut certificate index is unknown".into())
        })?;
        let cut =
            self.emission.cut_nodes.get(cut_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q cut certificate row is unknown".into())
            })?;
        if cut.node != cut_row.node {
            return Err(
                GeometryErrorKind::Invalid("Q cut certificate rows are misaligned".into()).into(),
            );
        }
        let frame = self.topology.sources.frame;
        let q = &self.topology.q;
        let period = ExactRational::from_f64(frame.longitude().period());
        let mut latitude: Option<ExactRational> = None;
        for chord in &cut.chords {
            budget.charge_into_error(1)?;
            let west_port = q.ports.get(chord.west_far_port.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q west cut endpoint is unknown".into())
            })?;
            let east_port = q.ports.get(chord.east_far_port.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q east cut endpoint is unknown".into())
            })?;
            if west_port.cut || east_port.cut {
                return Ok(None);
            }
            let west_nearest = self.nearest.xy(west_port.node)?;
            let east_nearest = self.nearest.xy(east_port.node)?;
            let west_candidate = self.local_candidate(west_port.node, class, assigned)?;
            let east_candidate = self.local_candidate(east_port.node, class, assigned)?;
            let west_longitude = self
                .nearest
                .pole_port_longitude
                .get(chord.west_far_port.0 as usize)
                .copied()
                .flatten();
            let east_longitude = self
                .nearest
                .pole_port_longitude
                .get(chord.east_far_port.0 as usize)
                .copied()
                .flatten();
            let west_periods = self
                .nearest
                .pole_port_periods
                .get(chord.west_far_port.0 as usize)
                .copied()
                .flatten();
            let east_periods = self
                .nearest
                .pole_port_periods
                .get(chord.east_far_port.0 as usize)
                .copied()
                .flatten();
            let Some(west_point) = assigned_port_point(
                frame,
                q,
                chord.west_far_port,
                west_nearest,
                west_candidate,
                west_longitude,
                west_periods,
                &self.cut_east,
                budget,
            )?
            else {
                return Ok(None);
            };
            let Some(east_point) = assigned_port_point(
                frame,
                q,
                chord.east_far_port,
                east_nearest,
                east_candidate,
                east_longitude,
                east_periods,
                &self.cut_east,
                budget,
            )?
            else {
                return Ok(None);
            };
            let Some(candidate_y) =
                cut_intersection_y(q, &period, &west_point, &east_point, budget)?
            else {
                return Ok(None);
            };
            if let Some(previous) = &latitude {
                if !previous.try_cmp_with_work(&candidate_y, budget)?.is_eq() {
                    return Ok(None);
                }
            } else {
                latitude = Some(candidate_y);
            }
        }
        let Some(latitude) = latitude else {
            return Ok(None);
        };
        let node = q
            .nodes
            .get(cut.node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q cut node is unknown".into()))?;
        if node.pole.is_some() {
            let exact = q
                .ports
                .get(cut.west.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q pole cut has no west port".into()))?;
            if !latitude.try_cmp_with_work(&exact.point.y, budget)?.is_eq() {
                return Ok(None);
            }
        }
        Ok(Some(latitude))
    }

    fn candidate_port_point(
        &self,
        port_id: QPortId,
        class: usize,
        assigned: &[Option<QCandidate>],
        cut_cache: &mut BTreeMap<QNodeId, ExactRational>,
        budget: &mut impl Work,
    ) -> Result<Option<ExactChartPoint>> {
        budget.charge_into_error(1)?;
        let frame = self.topology.sources.frame;
        let q = &self.topology.q;
        let port = q
            .ports
            .get(port_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q candidate port is unknown".into()))?;
        if port.cut {
            let latitude = if let Some(latitude) = cut_cache.get(&port.node) {
                latitude.clone()
            } else {
                let Some(cut_index) = self
                    .cut_index_by_node
                    .get(port.node.0 as usize)
                    .copied()
                    .flatten()
                else {
                    return Ok(None);
                };
                let Some(latitude) =
                    self.candidate_cut_latitude(cut_index, class, assigned, budget)?
                else {
                    return Ok(None);
                };
                cut_cache.insert(port.node, latitude.clone());
                latitude
            };
            return Ok(Some(ExactChartPoint::from_rationals(
                port.point.x.clone(),
                latitude,
            )));
        }
        let nearest_xy = self.nearest.xy(port.node)?;
        let candidate = self.local_candidate(port.node, class, assigned)?;
        let pole_longitude = self
            .nearest
            .pole_port_longitude
            .get(port_id.0 as usize)
            .copied()
            .flatten();
        assigned_port_point(
            frame,
            q,
            port_id,
            nearest_xy,
            candidate,
            pole_longitude,
            self.nearest
                .pole_port_periods
                .get(port_id.0 as usize)
                .copied()
                .flatten(),
            &self.cut_east,
            budget,
        )
    }

    fn local_publication_endpoint(
        &self,
        endpoint: &PublishedEndpoint,
        reference: &ExactChartPoint,
        reference_offset: &ExactRational,
        class: usize,
        assigned: &[Option<QCandidate>],
        cut_cache: &mut BTreeMap<QNodeId, ExactRational>,
        budget: &mut impl Work,
    ) -> Result<Option<ExactChartPoint>> {
        budget.charge_into_error(1)?;
        let assigned_point = match endpoint {
            PublishedEndpoint::Source { port, .. } => {
                self.candidate_port_point(*port, class, assigned, cut_cache, budget)?
            },
            PublishedEndpoint::Published { node, sample } => {
                if self.publication_sample_nodes.get(*sample) != Some(node) {
                    return Err(GeometryErrorKind::Invalid(
                        "local publication occurrence changed its node authority".into(),
                    )
                    .into());
                }
                let candidate = match self
                    .publication_location
                    .get(node.0 as usize)
                    .copied()
                    .flatten()
                {
                    Some((owner, rank)) if owner == class => assigned.get(rank).copied().flatten(),
                    Some(_) => {
                        return Err(GeometryErrorKind::Invalid(
                            "publication predicate crosses variable classes".into(),
                        )
                        .into());
                    },
                    None => self
                        .publication_nearest
                        .get(node.0 as usize)
                        .copied()
                        .flatten(),
                };
                candidate
                    .map(|candidate| {
                        candidate_chart_point_with_work(
                            self.topology.sources.frame,
                            candidate,
                            budget,
                        )
                    })
                    .transpose()?
            },
        };
        assigned_point
            .as_ref()
            .map(|assigned| {
                self.assigned_publication_endpoint_point(
                    endpoint,
                    reference,
                    reference_offset,
                    assigned,
                    budget,
                )
            })
            .transpose()
    }

    fn local_publication_chord(
        &self,
        chord_id: QPublicationChordId,
        class: usize,
        assigned: &[Option<QCandidate>],
        cut_cache: &mut BTreeMap<QNodeId, ExactRational>,
        budget: &mut impl Work,
    ) -> Result<Option<LocalPublicationChord>> {
        budget.charge_into_error(1)?;
        match chord_id {
            QPublicationChordId::Source(index) => {
                let chord = self.chords.get(index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication source chord is unknown".into())
                })?;
                let [start_port, end_port] = chord.ports;
                let exact = [
                    self.exact_port_point(start_port)?.clone(),
                    self.exact_port_point(end_port)?.clone(),
                ];
                let [Some(start), Some(end)] = [
                    self.candidate_port_point(start_port, class, assigned, cut_cache, budget)?,
                    self.candidate_port_point(end_port, class, assigned, cut_cache, budget)?,
                ] else {
                    return Ok(None);
                };
                Ok(Some(LocalPublicationChord {
                    nodes: chord.nodes.map(QGraphNode::Source),
                    exact,
                    candidate: [start, end],
                }))
            },
            QPublicationChordId::Published(index) => {
                let chord = self.publication_chords.get(index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication chord is unknown".into())
                })?;
                let [Some(start), Some(end)] = [
                    self.local_publication_endpoint(
                        &chord.endpoints[0],
                        &chord.exact[0],
                        &chord.reference_offsets[0],
                        class,
                        assigned,
                        cut_cache,
                        budget,
                    )?,
                    self.local_publication_endpoint(
                        &chord.endpoints[1],
                        &chord.exact[1],
                        &chord.reference_offsets[1],
                        class,
                        assigned,
                        cut_cache,
                        budget,
                    )?,
                ] else {
                    return Ok(None);
                };
                Ok(Some(LocalPublicationChord {
                    nodes: chord.nodes,
                    exact: chord.exact.clone(),
                    candidate: [start, end],
                }))
            },
        }
    }

    fn publication_pair_matches_local(
        &self,
        pair: QPublicationPair,
        class: usize,
        assigned: &[Option<QCandidate>],
        cut_cache: &mut BTreeMap<QNodeId, ExactRational>,
        budget: &mut impl Work,
    ) -> Result<bool> {
        budget.charge_into_error(1)?;
        let (Some(left), Some(right)) = (
            self.local_publication_chord(pair.left, class, assigned, cut_cache, budget)?,
            self.local_publication_chord(pair.right, class, assigned, cut_cache, budget)?,
        ) else {
            return Ok(false);
        };
        let period = ExactRational::from_f64(self.topology.sources.frame.longitude().period());
        let right_exact = [
            translated_exact_point_with_work(&right.exact[0], pair.right_shift, &period, budget)?,
            translated_exact_point_with_work(&right.exact[1], pair.right_shift, &period, budget)?,
        ];
        let right_candidate = [
            translated_exact_point_with_work(
                &right.candidate[0],
                pair.right_shift,
                &period,
                budget,
            )?,
            translated_exact_point_with_work(
                &right.candidate[1],
                pair.right_shift,
                &period,
                budget,
            )?,
        ];
        pair_preserves_incidence_with_work(
            left.nodes,
            &left.exact,
            &left.candidate,
            right.nodes,
            &right_exact,
            &right_candidate,
            budget,
        )
    }

    fn exact_port_point(&self, port_id: QPortId) -> Result<&ExactChartPoint> {
        self.topology
            .q
            .ports
            .get(port_id.0 as usize)
            .map(|port| &port.point)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q exact port is unknown".into()).into())
    }

    fn fan_matches(
        &self,
        fan: &QFanRow,
        class: usize,
        assigned: &[Option<QCandidate>],
        cut_cache: &mut BTreeMap<QNodeId, ExactRational>,
        budget: &mut impl Work,
    ) -> Result<bool> {
        let q = &self.topology.q;
        budget.charge_into_error(
            u64::try_from(fan.darts.len())
                .map_err(|_| ResourceError::SizeOverflow)?
                .checked_mul(2)
                .ok_or(ResourceError::SizeOverflow)?,
        )?;
        let mut ports = Vec::with_capacity(fan.darts.len().saturating_mul(2));
        for &dart_id in &fan.darts {
            budget.charge_into_error(1)?;
            let dart = q.darts.get(dart_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q fan certificate dart is unknown".into())
            })?;
            ports.extend([dart.from_port, dart.to_port]);
        }
        try_sort_by_with_work(&mut ports, budget, |left, right, _work| Ok(left.cmp(right)))?;
        budget.charge_into_error(
            u64::try_from(ports.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        ports.dedup();
        let mut points = Vec::<(QPortId, ExactChartPoint)>::with_capacity(ports.len());
        for port_id in ports {
            budget.charge_into_error(1)?;
            let Some(point) =
                self.candidate_port_point(port_id, class, assigned, cut_cache, budget)?
            else {
                return Ok(false);
            };
            points.push((port_id, point));
        }
        let chart = QFaceChart {
            cut: &q.cut,
            cut_east: &self.cut_east,
            ports: &q.ports,
            nodes: &q.nodes,
        };
        budget.charge_into_error(
            u64::try_from(fan.darts.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut candidate_order = fan.darts.clone();
        try_sort_by_with_work(&mut candidate_order, budget, |left, right, work| {
            work.charge_into_error(1)?;
            compare_q_darts_with_points(chart, &q.darts, *left, *right, |port| {
                work.charge_into_error(1)?;
                let index = points
                    .binary_search_by_key(&port, |(candidate, _)| *candidate)
                    .map_err(|_| {
                        GeometryErrorKind::Invalid("Q fan point cache is incomplete".into())
                    })?;
                points.get(index).map(|(_, point)| point).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q fan point cache index is unknown".into()).into()
                })
            })
        })?;
        for adjacent in candidate_order.windows(2) {
            budget.charge_into_error(1)?;
            let [left, right] = adjacent else {
                unreachable!("windows(2) contains two Q fan darts");
            };
            if compare_q_darts_with_points(chart, &q.darts, *left, *right, |port| {
                budget.charge_into_error(1)?;
                let index = points
                    .binary_search_by_key(&port, |(candidate, _)| *candidate)
                    .map_err(|_| {
                        GeometryErrorKind::Invalid("Q fan point cache is incomplete".into())
                    })?;
                points.get(index).map(|(_, point)| point).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q fan point cache index is unknown".into()).into()
                })
            })?
            .is_eq()
            {
                return Ok(false);
            }
        }
        if let (Some(&first), Some(&last)) = (candidate_order.first(), candidate_order.last())
            && compare_q_darts_with_points(chart, &q.darts, last, first, |port| {
                budget.charge_into_error(1)?;
                let index = points
                    .binary_search_by_key(&port, |(candidate, _)| *candidate)
                    .map_err(|_| {
                        GeometryErrorKind::Invalid("Q fan point cache is incomplete".into())
                    })?;
                points.get(index).map(|(_, point)| point).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q fan point cache index is unknown".into()).into()
                })
            })?
            .is_eq()
        {
            return Ok(false);
        }
        budget.charge_into_error(
            u64::try_from(fan.darts.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        Ok(rotation_row_matches(&fan.darts, &candidate_order))
    }

    fn source_variable_location(
        &self,
        node: QNodeId,
        budget: &mut impl Work,
    ) -> Result<Option<QNodeLocation>> {
        budget.charge_into_error(1)?;
        let index = node.0 as usize;
        let location = self
            .node_location
            .get(index)
            .copied()
            .ok_or_else(|| GeometryErrorKind::Invalid("Q dependency node is unknown".into()))?;
        let exact = self
            .exact_positions
            .get(index)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q dependency domain is unknown".into()))?;
        let movable = match exact {
            Some(exact) => inexact_axes_with_work(exact, self.nearest.xy(node)?, budget)?
                .into_iter()
                .any(|axis| axis),
            None => false,
        };
        if location.is_some() != movable {
            return Err(GeometryErrorKind::Invalid(
                "Q dependency is movable but absent from the frozen source inventory".into(),
            )
            .into());
        }
        Ok(location)
    }

    fn publication_variable_location(
        &self,
        node: EmissionNodeId,
        budget: &mut impl Work,
    ) -> Result<Option<QNodeLocation>> {
        budget.charge_into_error(1)?;
        let index = node.0 as usize;
        let location = self
            .publication_location
            .get(index)
            .copied()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication dependency is unknown".into())
            })?;
        let authority = &self
            .publication_nodes
            .nodes()
            .get(index)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication dependency authority is unknown".into())
            })?
            .authority;
        let nearest = self
            .publication_nearest
            .get(index)
            .copied()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication dependency has no nearest row".into())
            })?;
        let domain = self.publication_domains.get(index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q publication dependency has no domain".into())
        })?;
        let is_computed = matches!(authority, NodeAuthority::Computed(_));
        if is_computed != nearest.is_some() {
            return Err(GeometryErrorKind::Invalid(
                "Q publication authority and nearest inventory disagree".into(),
            )
            .into());
        }
        let movable = if let Some(nearest) = nearest {
            if domain.is_empty() {
                return Err(GeometryErrorKind::Invalid(
                    "Q publication dependency has an invalid admitted domain".into(),
                )
                .into());
            }
            let mut movable = false;
            let mut contains_nearest = false;
            for &candidate in domain {
                budget.charge_into_error(1)?;
                contains_nearest |= candidate == nearest;
                movable |= candidate != nearest;
            }
            if !contains_nearest {
                return Err(GeometryErrorKind::Invalid(
                    "Q publication dependency has an invalid admitted domain".into(),
                )
                .into());
            }
            movable
        } else {
            if !domain.is_empty() {
                return Err(GeometryErrorKind::Invalid(
                    "fixed Q publication dependency has an admitted domain".into(),
                )
                .into());
            }
            false
        };
        if location.is_some() != movable {
            return Err(GeometryErrorKind::Invalid(
                "Q publication dependency is movable but absent from the frozen inventory".into(),
            )
            .into());
        }
        Ok(location)
    }

    fn classes_for_nodes(
        &self,
        nodes: impl IntoIterator<Item = QNodeId>,
        budget: &mut impl Work,
    ) -> Result<Vec<usize>> {
        budget.charge_into_error(
            u64::try_from(self.node_location.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut seen = vec![false; self.node_location.len()];
        let mut pending = Vec::new();
        for node in nodes {
            budget.charge_into_error(1)?;
            pending.push(node);
        }
        let mut classes = Vec::new();
        let mut next = 0;
        while let Some(node) = pending.get(next).copied() {
            next = next.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            budget.charge_into_error(1)?;
            let seen_node = seen.get_mut(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q rejection references an unknown node".into())
            })?;
            if std::mem::replace(seen_node, true) {
                continue;
            }
            if let Some((class, _)) = self.source_variable_location(node, budget)? {
                budget.charge_into_error(1)?;
                classes.push(class);
            }
            let cuts = self
                .dependency_index
                .cuts
                .get(node.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q rejection node has no cut dependency row".into())
                })?;
            for &cut_index in cuts {
                budget.charge_into_error(1)?;
                let cut = self.cut_rows.get(cut_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q cut dependency row is unknown".into())
                })?;
                for &dependency in &cut.dependencies {
                    budget.charge_into_error(1)?;
                    pending.push(dependency);
                }
            }
            let fans = self
                .dependency_index
                .fans
                .get(node.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q rejection node has no fan dependency row".into())
                })?;
            for &fan_index in fans {
                budget.charge_into_error(1)?;
                let fan = self.fan_rows.get(fan_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q fan dependency row is unknown".into())
                })?;
                for &dependency in &fan.dependencies {
                    budget.charge_into_error(1)?;
                    pending.push(dependency);
                }
            }
            let pairs = self
                .dependency_index
                .pairs
                .get(node.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q rejection node has no pair dependency row".into())
                })?;
            for &pair_index in pairs {
                budget.charge_into_error(1)?;
                let pair = self.proximity_pairs.get(pair_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q proximity dependency pair is unknown".into())
                })?;
                for chord_index in [pair.left, pair.right] {
                    let chord = self.chords.get(chord_index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q proximity dependency chord is unknown".into())
                    })?;
                    for &dependency in &chord.dependencies {
                        budget.charge_into_error(1)?;
                        pending.push(dependency);
                    }
                }
            }
        }
        try_sort_by_with_work(&mut classes, budget, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        budget.charge_into_error(
            u64::try_from(classes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        classes.dedup();
        Ok(classes)
    }

    fn classes_for_variables(
        &self,
        variables: impl IntoIterator<Item = QVariableNode>,
        budget: &mut impl Work,
    ) -> Result<Vec<usize>> {
        let mut source_nodes = Vec::new();
        let mut classes = Vec::new();
        for variable in variables {
            budget.charge_into_error(1)?;
            match variable {
                QVariableNode::Source(node) => source_nodes.push(node),
                QVariableNode::Published(node) => {
                    if let Some((class, _)) = self.publication_variable_location(node, budget)? {
                        classes.push(class);
                    }
                },
            }
        }
        for class in self.classes_for_nodes(source_nodes, budget)? {
            budget.charge_into_error(1)?;
            classes.push(class);
        }
        try_sort_by_with_work(&mut classes, budget, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        budget.charge_into_error(
            u64::try_from(classes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        classes.dedup();
        Ok(classes)
    }
}

#[cfg(test)]
mod canonical_coordinate_tests {
    #![expect(
        clippy::panic_in_result_fn,
        reason = "exact coordinate controls use assertions for independently computed bit expectations"
    )]

    use super::*;
    use crate::Frame;
    use crate::geometry::embedding_completion::{LiteralPoint, LiteralRowLayout};
    use crate::geometry::publication::FinishedGeometry;
    use crate::geometry::storage::{
        GeometryArrayStorage, LogicalRow, PackedStorage, RowSelection, StorageLayout,
    };
    use crate::geometry::{CoordinateAxes, OverlayOp, Shape};

    fn topology(shapes: &[Shape]) -> Result<ExactGeographicTopology> {
        let frame = GeographicFrame::crs84();
        let inputs = crate::geographic::GeographicInputs::try_from_shapes(frame, shapes.iter())
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        ExactGeographicTopology::from_views(frame, inputs.views().iter().copied())
    }

    fn native_union_for_shapes(
        shapes: &[crate::geometry::Shape],
    ) -> Result<crate::geometry::Shape> {
        let topology = topology(shapes)?;
        let mut budget = EvaluationBudget::new(OverlayOp::Union.name(), 0)?;
        let plan = QEmissionPlan::build(&topology, OverlayOp::Union, &mut budget)?;
        let nearest = QCoordinates::build_with_work(&topology, &mut budget)?;
        let publication = plan.fragment_recipes.publish(
            &topology,
            &nearest,
            OverlayOp::Union.name(),
            &mut budget,
        )?;
        let source_points = vec![None; topology.q.nodes.len()];
        let drawing = plan.drawing(&topology, &nearest, &source_points, &mut budget)?;
        let live_keys = drawing.live_node_keys(topology.sources.frame, &mut budget)?;
        let emission =
            plan.embedding_emission(&topology, &source_points, &publication, &mut budget)?;
        let ordinates = vec![NodeOrdinates::default(); topology.q.nodes.len()];
        let certification = QEmbeddingCertification::<crate::crs::GeodesicMetric> {
            topology: &topology,
            emission: &emission,
            nearest: &nearest,
            ordinates: &ordinates,
            declared_axes: crate::geometry::CoordinateAxes::XY,
            strict_axes: false,
            operation: OverlayOp::Union.name(),
            metric_access: None,
        };
        let outcome =
            certify_and_search_q_embedding(&certification, &live_keys, drawing, &mut budget)?;
        let SearchOutcome::Pass(QEmbeddingPass { output, .. }) = outcome else {
            return Err(GeometryErrorKind::Invalid(
                "reduced Native union did not complete its Q embedding".into(),
            )
            .into());
        };
        let super::super::emission::SealedQDrawing::Native(native) = output.sealed else {
            return Err(GeometryErrorKind::Invalid(
                "reduced Native union used source-only publication".into(),
            )
            .into());
        };
        let (shape, _frame) = native.sealed.into_scalar()?;
        Ok(shape)
    }

    fn source_drawing_for_storage(
        point: Point,
        target: StorageTarget,
        work: &mut impl Work,
    ) -> Result<SourceFrozenDrawing> {
        let layout = ShapeLiteralLayout {
            points: vec![LiteralPoint {
                output: 0,
                axes: CoordinateAxes::XYZM,
                image: QSourceImage::PublicationSample(0),
            }],
            lines: Vec::new(),
            polygons: Vec::new(),
        };
        let buffers = match target {
            StorageTarget::Scalar => FrozenLiteralBuffers::shapes(Shape::Point(point), layout),
            StorageTarget::Array | StorageTarget::Groups => {
                let storage =
                    std::sync::Arc::new(GeometryArrayStorage::Packed(PackedStorage::Points {
                        coords: std::sync::Arc::new(CoordSeq::try_from_points(&[point])?),
                        row_map: RowSelection::Identity,
                    }));
                let row = LiteralRowLayout {
                    operation: 0,
                    logical_row: 0,
                    group_row: matches!(target, StorageTarget::Groups).then_some(0),
                    value_row: matches!(target, StorageTarget::Groups).then_some(0),
                    layout,
                };
                match target {
                    StorageTarget::Array => FrozenLiteralBuffers::rows(
                        storage,
                        None,
                        StorageLayout::LogicalRows,
                        vec![Some(row)],
                        work,
                    )?,
                    StorageTarget::Groups => FrozenLiteralBuffers::groups(
                        storage,
                        None,
                        StorageLayout::LogicalRows,
                        vec![Some(row)],
                        vec![0, 1],
                        None,
                        work,
                    )?,
                    StorageTarget::Scalar => unreachable!(),
                }
            },
        };
        Ok(SourceFrozenDrawing { buffers })
    }

    fn read_finished_point(
        finished: FinishedGeometry,
        target: StorageTarget,
    ) -> Result<(Option<Point>, Frame)> {
        Ok(match target {
            StorageTarget::Scalar => {
                let (shape, frame) = finished.into_scalar()?;
                let point = match shape {
                    Shape::Point(point) => Some(point),
                    Shape::MultiPoint(_)
                    | Shape::LineString(_)
                    | Shape::MultiLineString(_)
                    | Shape::Polygon(_)
                    | Shape::MultiPolygon(_)
                    | Shape::GeometryCollection(_)
                    | Shape::Empty(..) => None,
                };
                (point, frame)
            },
            StorageTarget::Array => {
                let (rows, frame) = finished.into_array()?;
                let point = match rows.logical_row(0) {
                    Some(LogicalRow::Present { row, .. }) => row.point(),
                    Some(LogicalRow::Missing { .. }) | None => None,
                };
                (point, frame)
            },
            StorageTarget::Groups => {
                let (groups, frame) = finished.into_groups()?;
                let point = match groups.values.logical_row(0) {
                    Some(LogicalRow::Present { row, .. }) => row.point(),
                    Some(LogicalRow::Missing { .. }) | None => None,
                };
                (point, frame)
            },
        })
    }

    fn native_demand_for_shapes(shapes: &[Shape], operation: OverlayOp) -> Result<(bool, bool)> {
        let topology = topology(shapes)?;
        let mut budget = EvaluationBudget::new(operation.name(), 0)?;
        let plan = QEmissionPlan::build(&topology, operation, &mut budget)?;
        let nearest = QCoordinates::build_with_work(&topology, &mut budget)?;
        let publication =
            plan.fragment_recipes
                .publish(&topology, &nearest, operation.name(), &mut budget)?;
        let source_points = vec![None; topology.q.nodes.len()];
        let drawing = plan.drawing(&topology, &nearest, &source_points, &mut budget)?;
        let ring_roles = match check_q_ring_roles(&topology, &drawing, &mut budget)? {
            QRingRoleCheck::Pass(roles) => roles,
            QRingRoleCheck::Reject { .. } => {
                return Err(GeometryErrorKind::Invalid(
                    "Native-demand fixture has an invalid selected ring role".into(),
                )
                .into());
            },
        };
        let emission =
            plan.embedding_emission(&topology, &source_points, &publication, &mut budget)?;
        let ordinates = vec![NodeOrdinates::default(); topology.q.nodes.len()];
        let problem: QEmbeddingProblem<'_, '_, '_, '_, crate::crs::GeodesicMetric> =
            QEmbeddingProblem::new(
                &topology,
                &emission,
                &nearest,
                &ordinates,
                CoordinateAxes::XY,
                false,
                operation.name(),
                None,
                &mut budget,
            )?;
        let publication_coordinates = vec![None; problem.publication_nodes.nodes().len()];
        let source_ports = topology
            .q
            .ports
            .iter()
            .map(|port| Some(port.point.clone()))
            .collect();
        let leaves = problem
            .publication_chords
            .iter()
            .map(|chord| AssignedProducerLeaf {
                recipe: chord.recipe,
                trace_edge: chord.trace_edge,
                points: chord.exact.clone(),
            })
            .collect();
        let producer = AssignedProducerProjection {
            source_ports,
            leaves,
            rejections: Vec::new(),
        };
        let demand = problem.actual_native_demand(
            &drawing,
            &ring_roles,
            &producer,
            &nearest,
            &publication_coordinates,
            &mut budget,
        )?;

        let mut source_nodes = vec![false; topology.q.nodes.len()];
        for ports in &topology.q.source_edge_ports {
            for &port_id in ports {
                let port = topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("fixture source edge has an unknown Q port".into())
                })?;
                *source_nodes.get_mut(port.node.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("fixture source edge has an unknown Q node".into())
                })? = true;
            }
        }
        for &port_id in &topology.q.source_point_ports {
            let port = topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("fixture source point has an unknown Q port".into())
            })?;
            *source_nodes.get_mut(port.node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("fixture source point has an unknown Q node".into())
            })? = true;
        }
        let mut constructed_vertex = false;
        let mut inspect = |vertex: &EmittedVertex<'_>| -> Result<()> {
            let source = *source_nodes
                .get(vertex.port.node.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("fixture drawing has an unknown Q node".into())
                })?;
            if !source && !vertex.port.cut {
                constructed_vertex = true;
            }
            Ok(())
        };
        match &drawing.polygons {
            DrawnPolygons::WholeSphere(polygons) => {
                for polygon in polygons {
                    for ring in polygon {
                        for vertex in &ring.vertices {
                            inspect(vertex)?;
                        }
                    }
                }
            },
            DrawnPolygons::Regions { rings, .. } => {
                for ring in rings {
                    for vertex in &ring.vertices {
                        inspect(vertex)?;
                    }
                }
            },
        }
        Ok((demand, constructed_vertex))
    }

    fn reversed_lift_native_refinement_fixture(
        event: NativePortData,
        event_port: QPortId,
    ) -> (NativeAreaEdge, NativeKnownSpanBoundary) {
        let producer_port = |node, longitude| NativePortData {
            node: EmissionNodeId(node),
            point: ExactChartPoint::from_rationals(
                ExactRational::from_f64(longitude),
                ExactRational::from_f64(-80.0),
            ),
            spelling: PortSpelling::Interior { periods: 0 },
            source_record: None,
            occurrences: Vec::new(),
        };
        let source = NativeSourceSpanUse {
            span: ChartSpanId(0),
            owner: 0,
            source_edge: SourceEdgeId(0),
            source_lift: -1,
            source_range: ExactSourceRange {
                start: ExactRational::from_f64(0.5),
                end: ExactRational::zero(),
            },
            producer_range: ExactSourceRange {
                start: ExactRational::from_f64(0.5),
                end: ExactRational::one(),
            },
            span_range: ExactSourceRange {
                start: ExactRational::zero(),
                end: ExactRational::one(),
            },
            nodes: [QNodeId(0), QNodeId(1)],
            ports: [QPortId(0), QPortId(1)],
            face_germs: None,
        };
        let edge = NativeAreaEdge {
            vertices: [producer_port(0, -180.0), producer_port(1, -120.0)],
            owner: NativeGraphEdgeOwner::Area { region: 0 },
            kind: NativeAreaEdgeKind::Producer {
                recipe: FragmentRecipeId(4),
                trace_edge: 0,
                producer_start: Box::new(ExactRational::zero()),
                producer_end: ExactRational::one(),
                trace_ranges: Vec::new(),
                known_spans: vec![source],
            },
        };
        let boundary = NativeKnownSpanBoundary {
            span: ChartSpanId(0),
            parameter: ExactRational::zero(),
            port: event_port,
            data: event,
        };
        (edge, boundary)
    }

    struct NativeRefinementSearch {
        classes: Vec<Vec<u8>>,
        visits: Vec<u8>,
    }

    impl Problem for NativeRefinementSearch {
        type Node = u8;
        type Candidate = u8;
        type Key = u8;
        type ClassState = ();
        type Output = u8;

        fn classes(&self) -> &[Vec<Self::Node>] {
            &self.classes
        }

        fn fixed_keys<W: Work>(
            &self,
            _class: usize,
            _variables: &[Self::Node],
            work: &mut W,
        ) -> Result<Vec<Self::Key>> {
            work.charge_into_error(1)?;
            Ok(Vec::new())
        }

        fn compare_nodes<W: Work>(
            &self,
            left: Self::Node,
            right: Self::Node,
            work: &mut W,
        ) -> Result<std::cmp::Ordering> {
            work.charge_into_error(1)?;
            Ok(left.cmp(&right))
        }

        fn fill_candidates<W: Work>(
            &self,
            _node: Self::Node,
            candidates: &mut Vec<Self::Candidate>,
            work: &mut W,
        ) -> Result<()> {
            work.charge_into_error(2)?;
            candidates.extend([0, 1]);
            Ok(())
        }

        fn compare_candidates<W: Work>(
            &self,
            _node: Self::Node,
            left: Self::Candidate,
            right: Self::Candidate,
            work: &mut W,
        ) -> Result<std::cmp::Ordering> {
            work.charge_into_error(1)?;
            Ok(left.cmp(&right))
        }

        fn candidate_key(&self, candidate: Self::Candidate) -> Self::Key {
            candidate
        }

        fn prepare_class<A: SearchAccount>(
            &self,
            _class: usize,
            _variables: &[Self::Node],
            account: &mut A,
        ) -> Result<Self::ClassState> {
            account.charge_evaluations(1)?;
            Ok(())
        }

        fn check_local<A: SearchAccount>(
            &self,
            _class_state: &Self::ClassState,
            _class: usize,
            _variables: &[Self::Node],
            _rank: usize,
            _assigned: &[Option<Self::Candidate>],
            account: &mut A,
        ) -> Result<bool> {
            account.charge_evaluations(1)?;
            Ok(true)
        }

        fn check_global<A: SearchAccount>(
            &mut self,
            assignments: &[ClassAssignment<'_, Self::Node, Self::Candidate>],
            account: &mut A,
        ) -> Result<GlobalCheck<Self::Output>> {
            account.charge_evaluations(1)?;
            let candidate = assignments
                .first()
                .and_then(|assignment| assignment.values.first())
                .copied()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("native refinement search has no assignment".into())
                })?;
            self.visits.push(candidate);
            let prepared = if candidate == 0 {
                Err(GeometryErrorKind::NativeRefinementMismatch.into())
            } else {
                Ok(candidate)
            };
            native_reference_global_check(prepared, || Ok(vec![0]))
        }
    }

    #[test]
    fn native_refinement_mismatch_rejects_first_candidate_and_searches_next() -> Result<()> {
        let mut problem = NativeRefinementSearch {
            classes: vec![vec![0]],
            visits: Vec::new(),
        };
        let mut budget = EvaluationBudget::new("native incidence search control", 0)?;

        let outcome = run_with_budget(&mut problem, &mut budget)?;

        let SearchOutcome::Pass(assignment) = outcome else {
            return Err(GeometryErrorKind::Invalid(
                "native incidence search exhausted after a repairable rejection".into(),
            )
            .into());
        };
        assert_eq!(assignment.output, 1);
        assert_eq!(problem.visits, [0, 1]);
        Ok(())
    }

    #[test]
    fn corrupt_native_refinement_source_remains_a_hard_error() -> Result<()> {
        let result: Result<u8> =
            Err(GeometryErrorKind::Invalid("corrupt source-span port map".into()).into());

        match native_reference_global_check(result, || Ok(vec![0])) {
            Err(error) => assert!(matches!(
                error.kind(),
                crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
            )),
            Ok(_) => {
                return Err(GeometryErrorKind::Invalid(
                    "corrupt source-span data was treated as a candidate rejection".into(),
                )
                .into());
            },
        }
        Ok(())
    }

    #[test]
    fn native_refinement_aligns_a_reversed_source_lift_to_the_assigned_leaf() -> Result<()> {
        let event = NativePortData {
            node: EmissionNodeId(2),
            point: ExactChartPoint::from_rationals(
                ExactRational::from_f64(-150.0),
                ExactRational::from_f64(-80.0),
            ),
            spelling: PortSpelling::Interior { periods: 0 },
            source_record: None,
            occurrences: Vec::new(),
        };
        let (edge, boundary) = reversed_lift_native_refinement_fixture(event, QPortId(0));
        let refined = common_refine_native_edges(
            vec![edge],
            &[boundary],
            GeographicFrame::crs84(),
            &mut Unmetered,
        )?;

        assert_eq!(refined.len(), 2);
        let split = refined
            .first()
            .and_then(|edge| edge.vertices.get(1))
            .ok_or_else(|| GeometryErrorKind::Invalid("refined split port is missing".into()))?;
        assert_eq!(
            split.point,
            ExactChartPoint::from_rationals(
                ExactRational::from_f64(-150.0),
                ExactRational::from_f64(-80.0),
            )
        );
        assert_eq!(split.node, EmissionNodeId(2));
        let NativeAreaEdgeKind::Producer { known_spans, .. } = &refined
            .get(1)
            .ok_or_else(|| GeometryErrorKind::Invalid("second refined edge is missing".into()))?
            .kind
        else {
            return Err(GeometryErrorKind::Invalid(
                "refinement changed the producer edge kind".into(),
            )
            .into());
        };
        let known = known_spans
            .first()
            .ok_or_else(|| GeometryErrorKind::Invalid("reversed source span was lost".into()))?;
        assert_eq!(known.source_range.start, ExactRational::from_f64(0.5));
        assert_eq!(known.source_range.end, ExactRational::zero());
        assert_eq!(known.source_lift, -1);
        Ok(())
    }

    #[test]
    fn noncoincident_native_refinement_junctions_are_typed_candidate_rejections() -> Result<()> {
        for (longitude, latitude) in [(-149.5, -80.0), (-150.0, (-80.0_f64).next_up())] {
            let event = NativePortData {
                node: EmissionNodeId(2),
                point: ExactChartPoint::from_rationals(
                    ExactRational::from_f64(longitude),
                    ExactRational::from_f64(latitude),
                ),
                spelling: PortSpelling::Interior { periods: 0 },
                source_record: None,
                occurrences: Vec::new(),
            };
            let (edge, boundary) = reversed_lift_native_refinement_fixture(event, QPortId(0));
            let Err(error) = common_refine_native_edges(
                vec![edge],
                &[boundary],
                GeographicFrame::crs84(),
                &mut Unmetered,
            ) else {
                return Err(GeometryErrorKind::Invalid(
                    "noncoincident native refinement event was accepted".into(),
                )
                .into());
            };
            assert!(matches!(
                error.kind(),
                crate::error::ErrorKind::Geometry(GeometryErrorKind::NativeRefinementMismatch)
            ));
        }
        Ok(())
    }

    #[test]
    fn corrupt_native_span_port_map_propagates_through_candidate_classification() -> Result<()> {
        let event = NativePortData {
            node: EmissionNodeId(2),
            point: ExactChartPoint::from_rationals(
                ExactRational::from_f64(-150.0),
                ExactRational::from_f64(-80.0),
            ),
            spelling: PortSpelling::Interior { periods: 0 },
            source_record: None,
            occurrences: Vec::new(),
        };
        let (edge, boundary) = reversed_lift_native_refinement_fixture(event, QPortId(1));
        let result = common_refine_native_edges(
            vec![edge],
            &[boundary],
            GeographicFrame::crs84(),
            &mut Unmetered,
        )
        .map(|_| ());

        match native_reference_global_check(result, || Ok(vec![0])) {
            Err(error) => assert!(matches!(
                error.kind(),
                crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
            )),
            Ok(_) => {
                return Err(GeometryErrorKind::Invalid(
                    "corrupt Q-span port identity was treated as a candidate rejection".into(),
                )
                .into());
            },
        }
        Ok(())
    }

    #[test]
    fn full_source_line_walk_proves_literal_reuse() -> Result<()> {
        let line = crate::io::parse_wkt("LINESTRING Z (0 0 1, 1 1 2)")?;
        let topology = topology(std::slice::from_ref(&line))?;
        let mut work = Unmetered;
        let plan = QEmissionPlan::build(&topology, OverlayOp::Union, &mut work)?;
        let coordinates = QCoordinates::build_with_work(&topology, &mut work)?;
        let source_points = vec![None; topology.q.nodes.len()];
        let drawing = plan.drawing(&topology, &coordinates, &source_points, &mut work)?;
        let DrawnPolygons::Regions { rings, .. } = &drawing.polygons else {
            return Err(GeometryErrorKind::Invalid(
                "line-only source reuse unexpectedly selected a whole sphere".into(),
            )
            .into());
        };
        assert!(rings.is_empty());
        let drawn = drawing.lines.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("source line reuse drawing is missing".into())
        })?;
        let walk = plan
            .fragment_recipes
            .walks()
            .iter()
            .find(|walk| walk.owner == RecipeWalkOwner::Line(0))
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("source line reuse recipe walk is missing".into())
            })?;
        assert!(source_line_reuse_is_complete(
            &topology,
            plan.fragment_recipes.as_slice(),
            None,
            walk,
            &drawn.path,
            &mut work,
        )?);
        Ok(())
    }

    #[test]
    fn reversed_source_shell_can_be_reused_as_a_selected_hole() -> Result<()> {
        let source = crate::io::parse_wkt("POLYGON ((170 40, -170 40, -170 50, 170 50, 170 40))")?;
        let cutter = crate::io::parse_wkt("POLYGON ((172 42, 178 42, 178 48, 172 48, 172 42))")?;
        let topology = topology(&[source, cutter])?;
        let operation = OverlayOp::Difference;
        let mut budget = EvaluationBudget::new(operation.name(), 0)?;
        let plan = QEmissionPlan::build(&topology, operation, &mut budget)?;
        let nearest = QCoordinates::build_with_work(&topology, &mut budget)?;
        let publication =
            plan.fragment_recipes
                .publish(&topology, &nearest, operation.name(), &mut budget)?;
        let source_points = vec![None; topology.q.nodes.len()];
        let drawing = plan.drawing(&topology, &nearest, &source_points, &mut budget)?;
        let ring_roles = match check_q_ring_roles(&topology, &drawing, &mut budget)? {
            QRingRoleCheck::Pass(roles) => roles,
            QRingRoleCheck::Reject { .. } => {
                return Err(GeometryErrorKind::Invalid(
                    "difference source rings did not retain their selected roles".into(),
                )
                .into());
            },
        };
        let emission =
            plan.embedding_emission(&topology, &source_points, &publication, &mut budget)?;
        let ordinates = vec![NodeOrdinates::default(); topology.q.nodes.len()];
        let problem: QEmbeddingProblem<'_, '_, '_, '_, crate::crs::GeodesicMetric> =
            QEmbeddingProblem::new(
                &topology,
                &emission,
                &nearest,
                &ordinates,
                crate::geometry::CoordinateAxes::XY,
                false,
                operation.name(),
                None,
                &mut budget,
            )?;
        let publication_coordinates = vec![None; problem.publication_nodes.nodes().len()];
        let reused = problem.source_reused_recipe_ids(
            &drawing,
            &ring_roles,
            &nearest,
            &publication_coordinates,
            &mut budget,
        )?;

        let source_polygon = topology.sources.polygons.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("difference source polygon is missing".into())
        })?;
        let cutter_polygon = topology.sources.polygons.get(1).ok_or_else(|| {
            GeometryErrorKind::Invalid("difference cutter polygon is missing".into())
        })?;
        let source_ring = source_polygon.rings.start;
        let cutter_ring = cutter_polygon.rings.start;
        let DrawnPolygons::Regions { rings, .. } = &drawing.polygons else {
            return Err(GeometryErrorKind::Invalid(
                "difference unexpectedly selected a whole sphere".into(),
            )
            .into());
        };
        let planned_rings = match &plan.polygons {
            PolygonPlan::Regions { rings, .. } => rings,
            PolygonPlan::WholeSphere(_) => {
                return Err(GeometryErrorKind::Invalid(
                    "difference unexpectedly planned a whole sphere".into(),
                )
                .into());
            },
        };
        let (cutter_index, reverse) = rings
            .iter()
            .enumerate()
            .find_map(|(index, ring)| {
                let RecipeWalkOwner::SourceRing(ring_id) = ring.recipe_walk else {
                    return None;
                };
                let Some(PlannedAreaRing::Source {
                    ring: planned_ring,
                    reverse,
                    ..
                }) = planned_rings.get(index).copied()
                else {
                    return None;
                };
                (ring_id == cutter_ring && planned_ring == cutter_ring).then_some((index, reverse))
            })
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("difference did not reuse the cutter shell".into())
            })?;
        let source_ring_row = topology
            .sources
            .rings
            .get(source_ring)
            .ok_or_else(|| GeometryErrorKind::Invalid("source shell row is missing".into()))?;
        let cutter_ring_row = topology
            .sources
            .rings
            .get(cutter_ring)
            .ok_or_else(|| GeometryErrorKind::Invalid("cutter shell row is missing".into()))?;
        assert!(!source_ring_row.hole);
        assert!(!cutter_ring_row.hole);
        assert!(ring_roles[cutter_index]);
        assert!(reverse);
        let source_walks = plan.fragment_recipes.walks();
        let source_recipe_ids = source_walks
            .iter()
            .find(|walk| walk.owner == RecipeWalkOwner::SourceRing(source_ring))
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("difference source shell recipe is missing".into())
            })?;
        let cutter_recipe_ids = source_walks
            .iter()
            .find(|walk| walk.owner == RecipeWalkOwner::SourceRing(cutter_ring))
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("difference cutter shell recipe is missing".into())
            })?;
        assert!(
            source_recipe_ids
                .fragments
                .iter()
                .all(|recipe| reused.contains(recipe)),
            "the unchanged source shell keeps its raw reuse path"
        );
        assert!(
            cutter_recipe_ids
                .fragments
                .iter()
                .all(|recipe| reused.contains(recipe)),
            "the reversed cutter shell keeps its certified planned reuse path"
        );

        let mut wrong_roles = ring_roles;
        wrong_roles[cutter_index] = false;
        assert!(
            problem
                .source_reused_recipe_ids(
                    &drawing,
                    &wrong_roles,
                    &nearest,
                    &publication_coordinates,
                    &mut budget,
                )
                .is_err(),
            "a selected shell role without its planned traversal reversal must reject"
        );
        Ok(())
    }

    #[test]
    fn native_demand_counts_only_strict_interior_bank_events() -> Result<()> {
        let axis = GeographicFrame::crs84().longitude();
        let exact = |value| ExactRational::from_f64(value);
        let mut budget = EvaluationBudget::new("Native demand", 0)?;
        for (start, end, expected) in [
            (170.0, 180.0, false),
            (180.0, 190.0, false),
            (170.0, 190.0, true),
            (190.0, 170.0, true),
            (-180.0, -170.0, false),
            (-190.0, -180.0, false),
            (-190.0, -170.0, true),
            (539.0, 541.0, true),
            (540.0, 550.0, false),
        ] {
            assert_eq!(
                strict_native_bank_event(axis, &exact(start), &exact(end), &mut budget)?,
                expected,
                "unexpected strict-bank result for {start} -> {end}",
            );
        }
        Ok(())
    }

    #[test]
    fn source_only_seam_union_stays_on_quotient_publication() -> Result<()> {
        let east = crate::io::parse_wkt("POLYGON ((170 0, 180 0, 180 10, 170 10, 170 0))")?;
        let west = crate::io::parse_wkt("POLYGON ((-180 0, -170 0, -170 10, -180 10, -180 0))")?;

        assert_eq!(
            native_demand_for_shapes(&[east, west], OverlayOp::Union)?,
            (false, false),
            "a connected seam union uses only unchanged source/cut ports"
        );
        Ok(())
    }

    #[test]
    fn source_only_receipt_binds_the_current_publication_buffers() -> Result<()> {
        use super::super::emission::{
            FeatureFamily, FeatureFamilyParent, FragmentRecipeId, PublicationInventory,
            PublishedSample, SampleProducerKind,
        };
        let source_shape = crate::io::parse_wkt("POINT (0 0)")?;
        let geographic_frame = GeographicFrame::crs84();
        let inputs = crate::geographic::GeographicInputs::try_from_shapes(
            geographic_frame,
            std::iter::once(&source_shape),
        )
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        let output_frame = crate::Frame::CrsEpoch("OGC:CRS84".into(), 2024.5);
        let topology = ExactGeographicTopology::from_views_as_set_with_publication_frame(
            geographic_frame,
            output_frame.clone(),
            inputs.views().iter().copied(),
        )?;
        let point = Point::new_raw_axes(
            7.0,
            6.0,
            crate::geometry::ZOrdinate(Some(70.0)),
            crate::geometry::MOrdinate(Some(700.0)),
        );
        let corrupted_point = Point::new_raw_axes(
            7.0,
            6.0,
            crate::geometry::ZOrdinate(Some(70.0)),
            crate::geometry::MOrdinate(Some(701.0)),
        );
        let mut sample_ordinates = NodeOrdinates::default();
        sample_ordinates.z.observe(Some(70.0));
        sample_ordinates.m.observe(Some(700.0));
        let exact = ExactChartPoint::from_rationals(
            ExactRational::from_f64(7.0),
            ExactRational::from_f64(6.0),
        );
        let publication = PublicationInventory {
            fragments: Vec::new(),
            samples: vec![PublishedSample {
                request_row: RequestRowId::from_index(0)?,
                family: FeatureFamily {
                    parent: FeatureFamilyParent::Fragment(FragmentRecipeId(0)),
                    producer: SampleProducerKind::Publication,
                    occurrence: 0,
                },
                parameter: ExactRational::from_f64(0.5),
                written_vertex: 0,
                point,
                exact_point: exact.clone(),
                published_chart_point: exact.clone(),
                source_trace: None,
                endpoint_only: false,
            }],
            counts: vec![1],
            ordinates: vec![sample_ordinates],
        };
        let source_ordinates = [];
        let publication_images = [exact];
        let publication_points = [XY::new(7.0, 6.0)];
        let publication_ordinates = [sample_ordinates];
        let mut work = Unmetered;

        for target in [
            StorageTarget::Scalar,
            StorageTarget::Array,
            StorageTarget::Groups,
        ] {
            let source = source_drawing_for_storage(corrupted_point, target, &mut work)?;
            let receipt = SourceOnlyPublication::new(
                source,
                &topology,
                None,
                &source_ordinates,
                Some(&publication),
                &publication_images,
                &publication_points,
                &publication_ordinates,
            );
            let call = PublicationCall::new(
                ProducerReceipt::QSourceOnly(receipt),
                topology.publication_frame().clone(),
                &mut work,
            );
            match finish_geographic(call, target)? {
                PublicationOutcome::Reject(PublicationRejection::SourceOnly(images)) => {
                    assert_eq!(images, [QSourceImage::PublicationSample(0)]);
                },
                PublicationOutcome::Reject(PublicationRejection::Native) => {
                    return Err(GeometryErrorKind::Invalid(
                        "source-only receipt returned native rejection".into(),
                    )
                    .into());
                },
                PublicationOutcome::Reject(PublicationRejection::Batch(_)) => {
                    return Err(GeometryErrorKind::Invalid(
                        "scalar source-only receipt returned a batch rejection".into(),
                    )
                    .into());
                },
                PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(_)) => {
                    return Err(GeometryErrorKind::Invalid(
                        "scalar source-only receipt returned a producer-drawn rejection".into(),
                    )
                    .into());
                },
                PublicationOutcome::Pass(finished) => {
                    let (actual, _) = read_finished_point(finished, target)?;
                    assert_eq!(
                        actual,
                        Some(point),
                        "omitting the binder must fail final-storage readback"
                    );
                },
            }

            let source = source_drawing_for_storage(point, target, &mut work)?;
            let receipt = SourceOnlyPublication::new(
                source,
                &topology,
                None,
                &source_ordinates,
                Some(&publication),
                &publication_images,
                &publication_points,
                &publication_ordinates,
            );
            let call = PublicationCall::new(
                ProducerReceipt::QSourceOnly(receipt),
                topology.publication_frame().clone(),
                &mut work,
            );
            let PublicationOutcome::Pass(finished) = finish_geographic(call, target)? else {
                return Err(GeometryErrorKind::Invalid(
                    "valid source-only output was rejected".into(),
                )
                .into());
            };
            let (actual, finished_frame) = read_finished_point(finished, target)?;
            assert_eq!(
                actual,
                Some(point),
                "final storage readback must retain the checked full record"
            );
            assert_eq!(finished_frame, output_frame);
        }
        Ok(())
    }

    #[test]
    fn exactly_representable_constructed_seam_vertex_stays_on_quotient_publication() -> Result<()> {
        let source = crate::io::parse_wkt("POLYGON ((170 0, -170 10, 170 10, 170 0))")?;
        let cutter = crate::io::parse_wkt("POLYGON ((180 2, 185 2, 185 8, 180 8, 180 2))")?;

        assert_eq!(
            native_demand_for_shapes(&[source, cutter], OverlayOp::Intersection)?,
            (false, true),
            "an exact constructed seam vertex has no binary64 rounding residual"
        );
        Ok(())
    }

    #[test]
    fn exactly_representable_seam_difference_stays_on_quotient_publication() -> Result<()> {
        let source = crate::io::parse_wkt("POLYGON ((170 0, -170 0, -170 20, 170 20, 170 0))")?;
        let cutter = crate::io::parse_wkt("POLYGON ((175 10, -175 10, -175 30, 175 30, 175 10))")?;

        assert_eq!(
            native_demand_for_shapes(&[source, cutter], OverlayOp::Difference)?,
            (false, true),
            "constructed (175,20) and (-175,20) have exact binary64 coordinates"
        );
        Ok(())
    }

    #[test]
    fn nonrepresentable_constructed_seam_vertex_requires_native_completion() -> Result<()> {
        let source = crate::io::parse_wkt("POLYGON ((179 0, -178 1, 179 1, 179 0))")?;
        let cutter = crate::io::parse_wkt("POLYGON ((180 0, 181 0, 181 1, 180 1, 180 0))")?;

        let (demand, constructed_vertex) =
            native_demand_for_shapes(&[source, cutter], OverlayOp::Intersection)?;
        assert!(constructed_vertex);
        assert!(demand);
        Ok(())
    }

    #[test]
    fn seam_wall_reductions_complete_through_the_native_reference() -> Result<()> {
        let west = vec![
            crate::io::parse_wkt("POLYGON ((170 -1, -170 -1, -170 2, 170 2, 170 -1))")?,
            crate::io::parse_wkt("POLYGON ((177 0, -174 1, -174 3, 177 2, 177 0))")?,
            crate::io::parse_wkt("POLYGON ((-180 60, -179 61, -180 62, -180 60))")?,
            crate::io::parse_wkt("POLYGON ((-180 50, -179 51, -180 52, -180 50))")?,
        ];
        let east = vec![
            crate::io::parse_wkt("POLYGON ((170 -1, -170 -1, -170 2, 170 2, 170 -1))")?,
            crate::io::parse_wkt("POLYGON ((177 0, -174 1, -174 3, 177 2, 177 0))")?,
            crate::io::parse_wkt("POLYGON ((180 60, 180 62, 179 61, 180 60))")?,
        ];

        for (bank, shapes) in [("West", west), ("East", east)] {
            let output = native_union_for_shapes(&shapes)?;
            assert!(
                output.validate()?.is_none(),
                "{bank} seam-wall reduction did not produce a valid Native result"
            );
        }
        Ok(())
    }

    #[test]
    fn polar_q_cut_midpoint_rounds_to_its_native_site() -> Result<()> {
        let upper = 80.0_f64.next_up();
        let polar_shell = crate::io::parse_wkt(&format!(
            "POLYGON ((-180 80, -120 80, 20 {upper}, 90 80, 180 80, -180 80))"
        ))?;
        let overlap_left =
            crate::io::parse_wkt("POLYGON ((170 -1, -170 -1, -170 2, 170 2, 170 -1))")?;
        let overlap_right =
            crate::io::parse_wkt("POLYGON ((177 0, -174 1, -174 3, 177 2, 177 0))")?;
        let output = native_union_for_shapes(&[polar_shell, overlap_left, overlap_right])?;

        assert!(output.validate()?.is_none());
        Ok(())
    }

    #[test]
    fn selected_pole_wedge_is_a_fan_not_a_terminal_chart_closure() -> Result<()> {
        let cap = crate::io::parse_wkt("POLYGON ((0 80, 90 80, 180 80, -90 80, 0 80))")?;
        let wedge = crate::io::parse_wkt("POLYGON ((120 90, 110 75, 130 75, 120 90))")?;
        let topology = topology(&[cap, wedge])?;
        let mut work = Unmetered;
        let plan = QEmissionPlan::build(&topology, OverlayOp::Intersection, &mut work)?;
        let NativeSelectedPoleSectors { sectors, closures } =
            native_selected_pole_sectors(&topology.q, &plan.selected_face_roots, &mut work)?;
        let (region, clockwise, counterclockwise) = sectors
            .iter()
            .find_map(|selected| match selected.sector {
                super::super::q::PoleSector::Fan {
                    pole: ChartPole::North,
                    clockwise,
                    counterclockwise,
                    ..
                } => Some((selected.region, clockwise, counterclockwise)),
                super::super::q::PoleSector::Fan { .. }
                | super::super::q::PoleSector::Isolated { .. }
                | super::super::q::PoleSector::Terminal { .. } => None,
            })
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("north wedge has no selected pole fan".into())
            })?;

        assert_ne!(clockwise, counterclockwise);
        assert!(closures.contains(&(region, ChartPole::North)));
        assert!(!sectors.iter().any(|selected| {
            selected.region == region
                && matches!(selected.sector, super::super::q::PoleSector::Terminal {
                    pole: ChartPole::North,
                    ..
                })
        }));
        Ok(())
    }

    #[test]
    fn native_pole_closures_keep_terminal_isolated_and_full_turn_sectors() -> Result<()> {
        let sectors = [
            NativeSelectedPoleSector {
                region: 1,
                sector: super::super::q::PoleSector::Fan {
                    pole: ChartPole::North,
                    clockwise: QDartId(0),
                    counterclockwise: QDartId(1),
                    face: GlobalFaceId(0),
                },
            },
            NativeSelectedPoleSector {
                region: 4,
                sector: super::super::q::PoleSector::Fan {
                    pole: ChartPole::North,
                    clockwise: QDartId(2),
                    counterclockwise: QDartId(2),
                    face: GlobalFaceId(3),
                },
            },
            NativeSelectedPoleSector {
                region: 2,
                sector: super::super::q::PoleSector::Terminal {
                    pole: ChartPole::North,
                    face: GlobalFaceId(1),
                },
            },
            NativeSelectedPoleSector {
                region: 3,
                sector: super::super::q::PoleSector::Isolated {
                    pole: ChartPole::South,
                    node: QNodeId(0),
                    face: GlobalFaceId(2),
                },
            },
        ];
        let closures = native_pole_closure_regions(&sectors, &mut Unmetered)?;

        assert_eq!(
            closures,
            BTreeSet::from([
                (2, ChartPole::North),
                (3, ChartPole::South),
                (4, ChartPole::North),
            ])
        );
        Ok(())
    }

    #[test]
    fn exact_source_producer_identity_distinguishes_sub_ulp_latitudes() -> Result<()> {
        let frame = GeographicFrame::crs84();
        let one = ExactRational::one();
        let quarter_ulp = ExactRational::from_f64(2.0_f64.powi(-54));
        let half_ulp = ExactRational::from_f64(2.0_f64.powi(-53));
        let left = ExactChartPoint::from_rationals(
            ExactRational::from_f64(25.0),
            one.try_add(&quarter_ulp)?,
        );
        let right =
            ExactChartPoint::from_rationals(ExactRational::from_f64(25.0), one.try_add(&half_ulp)?);

        assert_eq!(rounded_chart_point(&left)?, rounded_chart_point(&right)?);
        let mut work = Unmetered;
        let left_key = quotient_key_for_chart_position_with_work(frame, &left, &mut work)?;
        let right_key = quotient_key_for_chart_position_with_work(frame, &right, &mut work)?;
        assert!(
            !compare_quotient_keys_with_work(&left_key, &right_key, &mut work)?.is_eq(),
            "producer correspondence must not collapse exact origins that round alike"
        );
        Ok(())
    }

    #[test]
    fn assigned_expanded_producer_fan_rejects_a_ray_swap() -> Result<()> {
        let point = |x, y| {
            ExactChartPoint::from_rationals(ExactRational::from_f64(x), ExactRational::from_f64(y))
        };
        let origin = point(0.0, 0.0);
        let east = point(1.0, 0.0);
        let north = point(0.0, 1.0);
        let west = point(-1.0, 0.0);
        let exact = vec![origin.clone(), east.clone(), north.clone(), west.clone()];
        let points = NativeReferencePoints {
            exact,
            rounded: vec![
                XY::new(0.0, 0.0),
                XY::new(1.0, 0.0),
                XY::new(0.0, 1.0),
                XY::new(-1.0, 0.0),
            ],
        };
        let expected = [QDartId(1), QDartId(2), QDartId(3)];
        let mut ordered = [
            (QDartId(1), 1, east.clone()),
            (QDartId(2), 2, north.clone()),
            (QDartId(3), 3, west.clone()),
        ];
        let mut work = Unmetered;
        assert!(assigned_producer_fan_order_matches(
            &points,
            &origin,
            &mut ordered,
            &expected,
            &mut work,
        )?);
        let mut swapped = [
            (QDartId(1), 1, east),
            (QDartId(2), 3, west),
            (QDartId(3), 2, north),
        ];
        let origin = point(0.0, 0.0);
        for (left_nodes, left_exact, left_candidate, right_nodes, right_exact, right_candidate) in [
            (
                [
                    QGraphNode::Source(QNodeId(0)),
                    QGraphNode::Published(EmissionNodeId(1)),
                ],
                [origin.clone(), point(1.0, 0.0)],
                [origin.clone(), point(1.0, 0.0)],
                [
                    QGraphNode::Source(QNodeId(0)),
                    QGraphNode::Published(EmissionNodeId(2)),
                ],
                [origin.clone(), point(0.0, 1.0)],
                [origin.clone(), point(-1.0, 0.0)],
            ),
            (
                [
                    QGraphNode::Source(QNodeId(0)),
                    QGraphNode::Published(EmissionNodeId(1)),
                ],
                [origin.clone(), point(1.0, 0.0)],
                [origin.clone(), point(1.0, 0.0)],
                [
                    QGraphNode::Source(QNodeId(0)),
                    QGraphNode::Published(EmissionNodeId(3)),
                ],
                [origin.clone(), point(-1.0, 0.0)],
                [origin.clone(), point(0.0, 1.0)],
            ),
            (
                [
                    QGraphNode::Source(QNodeId(0)),
                    QGraphNode::Published(EmissionNodeId(2)),
                ],
                [origin.clone(), point(0.0, 1.0)],
                [origin.clone(), point(-1.0, 0.0)],
                [
                    QGraphNode::Source(QNodeId(0)),
                    QGraphNode::Published(EmissionNodeId(3)),
                ],
                [origin.clone(), point(-1.0, 0.0)],
                [origin.clone(), point(0.0, 1.0)],
            ),
        ] {
            assert!(pair_preserves_incidence(
                left_nodes,
                &left_exact,
                &left_candidate,
                right_nodes,
                &right_exact,
                &right_candidate,
            )?);
        }
        assert!(!assigned_producer_fan_order_matches(
            &points,
            &origin,
            &mut swapped,
            &expected,
            &mut work,
        )?);
        Ok(())
    }

    #[test]
    fn periodic_interior_aliases_share_one_canonical_graph_vertex() -> Result<()> {
        let frame = GeographicFrame::crs84();
        let y = ExactRational::from_f64(20.0);
        let canonical_point =
            ExactChartPoint::from_rationals(ExactRational::from_f64(-179.8), y.clone());
        let periodic_point = ExactChartPoint::from_rationals(ExactRational::from_f64(180.2), y);
        let port = |point, periods| NativePortData {
            node: EmissionNodeId(7),
            point,
            spelling: PortSpelling::Interior { periods },
            source_record: None,
            occurrences: Vec::new(),
        };
        let mut area_edges = Vec::new();
        let mut line_edges = Vec::new();
        let mut ports = vec![port(canonical_point.clone(), 0), port(periodic_point, 1)];
        let mut work = Unmetered;

        normalize_native_chart_lifts(
            &mut area_edges,
            &mut line_edges,
            &mut ports,
            frame,
            &mut work,
        )?;
        let graph = new_native_reference_graph_builder(&area_edges, &ports, &mut work)?;

        assert_eq!(ports[1].point, canonical_point);
        assert!(matches!(&ports[1].spelling, PortSpelling::Interior {
            periods: 0
        }));
        assert_eq!(graph.points.exact.len(), 1);
        assert_eq!(
            native_chart_vertex_key(&ports[0]),
            native_chart_vertex_key(&ports[1])
        );
        Ok(())
    }

    #[test]
    fn native_seam_port_without_a_record_uses_its_assigned_bank() -> Result<()> {
        let frame = GeographicFrame::crs84();
        let mut work = Unmetered;
        for (longitude, side) in [
            (180.0, super::super::emission::ChartSide::East),
            (-180.0, super::super::emission::ChartSide::West),
        ] {
            let point = ExactChartPoint::from_rationals(
                ExactRational::from_f64(longitude),
                ExactRational::from_f64(-1.0),
            );
            let port = native_port_data(
                frame,
                EmissionNodeId(0),
                QCandidate {
                    native: XY::new(longitude, -1.0),
                    periods: 0,
                },
                &point,
                None,
                Vec::new(),
                None,
                &mut work,
            )?;

            assert_eq!(port.point, point);
            assert!(matches!(&port.spelling, PortSpelling::Seam(actual) if *actual == side));
        }
        Ok(())
    }

    #[test]
    fn native_seam_source_record_keeps_its_chart_bank() -> Result<()> {
        let frame = GeographicFrame::crs84();
        let mut work = Unmetered;
        for (longitude, side) in [
            (180.0, super::super::emission::ChartSide::East),
            (-180.0, super::super::emission::ChartSide::West),
        ] {
            let point = ExactChartPoint::from_rationals(
                ExactRational::from_f64(longitude),
                ExactRational::from_f64(-1.0),
            );
            let record = Point::new_raw_xy(longitude, -1.0);
            let port = native_port_data(
                frame,
                EmissionNodeId(0),
                QCandidate {
                    native: XY::new(longitude, -1.0),
                    periods: 0,
                },
                &point,
                Some(record),
                Vec::new(),
                None,
                &mut work,
            )?;

            assert_eq!(port.point, point);
            assert_eq!(
                port.source_record.map(|record| record.x.to_bits()),
                Some(longitude.to_bits())
            );
            assert!(matches!(&port.spelling, PortSpelling::Seam(actual) if *actual == side));
        }
        Ok(())
    }

    #[test]
    fn equatorial_east_west_aliases_are_seam_banks_not_pole_longitudes() -> Result<()> {
        let frame = GeographicFrame::crs84();
        let node = EmissionNodeId(0);
        let half = ExactRational::from_f64(frame.longitude().half_period());
        let west = ExactRational::zero().try_subtract(&half)?;
        let keys = [
            NativeChartVertexKey {
                node,
                spelling: NativeChartSpellingKey::SeamEast,
            },
            NativeChartVertexKey {
                node,
                spelling: NativeChartSpellingKey::SeamWest,
            },
        ];
        let points = [
            ExactChartPoint::from_rationals(half, ExactRational::zero()),
            ExactChartPoint::from_rationals(west, ExactRational::zero()),
        ];
        let mut work = Unmetered;
        let sewing = native_graph_sewing(&keys, &points, frame, &mut work)?;
        assert_eq!(sewing.as_slice(), &[NativeGraphSewing {
            vertices: [0, 1],
            kind: NativeGraphSewingKind::SeamBank,
        }]);
        Ok(())
    }

    #[test]
    fn duplicate_east_bank_provider_is_rejected() -> Result<()> {
        let frame = GeographicFrame::crs84();
        let node = EmissionNodeId(0);
        let keys = [
            NativeChartVertexKey {
                node,
                spelling: NativeChartSpellingKey::SeamEast,
            },
            NativeChartVertexKey {
                node,
                spelling: NativeChartSpellingKey::SeamWest,
            },
            NativeChartVertexKey {
                node,
                spelling: NativeChartSpellingKey::SeamEast,
            },
        ];
        let east = ExactRational::from_f64(frame.longitude().half_period());
        let west = ExactRational::zero()
            .try_subtract(&ExactRational::from_f64(frame.longitude().half_period()))?;
        let points = [
            ExactChartPoint::from_rationals(east.clone(), ExactRational::zero()),
            ExactChartPoint::from_rationals(west, ExactRational::zero()),
            ExactChartPoint::from_rationals(east, ExactRational::zero()),
        ];
        let mut work = Unmetered;
        native_graph_sewing(&keys, &points, frame, &mut work).unwrap_err();
        Ok(())
    }

    #[test]
    fn duplicate_west_bank_provider_is_rejected() -> Result<()> {
        let frame = GeographicFrame::crs84();
        let node = EmissionNodeId(0);
        let half = ExactRational::from_f64(frame.longitude().half_period());
        let west = ExactRational::zero().try_subtract(&half)?;
        let keys = [
            NativeChartVertexKey {
                node,
                spelling: NativeChartSpellingKey::SeamWest,
            },
            NativeChartVertexKey {
                node,
                spelling: NativeChartSpellingKey::SeamEast,
            },
            NativeChartVertexKey {
                node,
                spelling: NativeChartSpellingKey::SeamWest,
            },
        ];
        let points = [
            ExactChartPoint::from_rationals(west.clone(), ExactRational::zero()),
            ExactChartPoint::from_rationals(half, ExactRational::zero()),
            ExactChartPoint::from_rationals(west, ExactRational::zero()),
        ];
        let mut work = Unmetered;
        native_graph_sewing(&keys, &points, frame, &mut work).unwrap_err();
        Ok(())
    }

    #[test]
    fn opposite_seam_banks_must_have_the_same_latitude() -> Result<()> {
        let frame = GeographicFrame::crs84();
        let node = EmissionNodeId(0);
        let half = ExactRational::from_f64(frame.longitude().half_period());
        let west = ExactRational::zero().try_subtract(&half)?;
        let keys = [
            NativeChartVertexKey {
                node,
                spelling: NativeChartSpellingKey::SeamEast,
            },
            NativeChartVertexKey {
                node,
                spelling: NativeChartSpellingKey::SeamWest,
            },
        ];
        let points = [
            ExactChartPoint::from_rationals(half, ExactRational::zero()),
            ExactChartPoint::from_rationals(west, ExactRational::from_f64(1.0)),
        ];
        let mut work = Unmetered;
        native_graph_sewing(&keys, &points, frame, &mut work).unwrap_err();
        Ok(())
    }

    #[test]
    fn nonpolar_pole_spelling_is_rejected() {
        let frame = GeographicFrame::crs84();
        let keys = [NativeChartVertexKey {
            node: EmissionNodeId(0),
            spelling: NativeChartSpellingKey::Pole(0.0_f64.to_bits()),
        }];
        let points = [ExactChartPoint::from_rationals(
            ExactRational::zero(),
            ExactRational::zero(),
        )];
        let mut work = Unmetered;
        native_graph_sewing(&keys, &points, frame, &mut work).unwrap_err();
    }

    #[test]
    fn interior_spelling_at_a_pole_is_rejected() {
        let frame = GeographicFrame::crs84();
        let keys = [NativeChartVertexKey {
            node: EmissionNodeId(0),
            spelling: NativeChartSpellingKey::Interior(0),
        }];
        let points = [ExactChartPoint::from_rationals(
            ExactRational::zero(),
            ExactRational::from_f64(frame.chart_pole_latitude(ChartPole::North)),
        )];
        let mut work = Unmetered;
        native_graph_sewing(&keys, &points, frame, &mut work).unwrap_err();
    }

    #[test]
    fn native_whole_sphere_reference_covers_and_sews_two_chart_strips() -> Result<()> {
        let source = crate::geometry::Shape::Polygon(crate::geometry::Polygon::new(
            Ring::try_from_closed(CoordSeq::try_from_points(&[
                Point::new_raw_xy(-180.0, -90.0),
                Point::new_raw_xy(-90.0, -90.0),
                Point::new_raw_xy(0.0, -90.0),
                Point::new_raw_xy(90.0, -90.0),
                Point::new_raw_xy(180.0, -90.0),
                Point::new_raw_xy(180.0, 90.0),
                Point::new_raw_xy(90.0, 90.0),
                Point::new_raw_xy(0.0, 90.0),
                Point::new_raw_xy(-90.0, 90.0),
                Point::new_raw_xy(-180.0, 90.0),
                Point::new_raw_xy(-180.0, -90.0),
            ])?)?,
            Vec::new(),
        ));
        let topology = topology(&[source])?;
        let operation = OverlayOp::Union;
        let mut budget = EvaluationBudget::new("native whole-sphere cover", 0)?;
        let plan =
            super::super::emission::plan::QEmissionPlan::build(&topology, operation, &mut budget)?;
        assert!(matches!(&plan.polygons, PolygonPlan::WholeSphere(_)));
        let nearest = QCoordinates::build_with_work(&topology, &mut budget)?;
        let publication =
            plan.fragment_recipes
                .publish(&topology, &nearest, operation.name(), &mut budget)?;
        let region = plan
            .selected_face_roots
            .iter()
            .flatten()
            .copied()
            .next()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("whole-sphere plan has no selected region".into())
            })?;
        let native_cut_ranges = publication
            .fragments
            .iter()
            .map(|fragment| vec![Vec::new(); fragment.trace.rows.edge_count()])
            .collect::<Vec<_>>();
        let region_families = (0..10_u32)
            .map(|occurrence| super::super::emission::FeatureFamily {
                parent: super::super::emission::FeatureFamilyParent::Region(region),
                producer: super::super::emission::SampleProducerKind::WholeSphereCover,
                occurrence,
            })
            .collect();
        let feature_bounds = FeatureFamilyBounds::from_prepared_domains(
            plan.fragment_recipes.as_slice(),
            &publication,
            &native_cut_ranges,
            region_families,
            &mut budget,
        )?;
        let mut families = feature_bounds.into_registry(0);
        let mut builder =
            super::super::emission::EmissionBuilder::new(&topology, &plan, &[], true, &mut budget)?;
        let mut presentation_candidates = Vec::new();
        let edges = native_whole_sphere_cell_edges(
            &mut builder,
            topology.sources.frame,
            region,
            &mut presentation_candidates,
            &mut families,
            &mut budget,
        )?;
        let graph_builder = new_native_reference_graph_builder(&edges, &[], &mut budget)?;
        let boundary = edges.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("whole-sphere cover has no boundary edge".into())
        })?;
        assert!(graph_builder.has_area_edge(
            region,
            &boundary.vertices[0],
            &boundary.vertices[1],
            &mut budget,
        )?);
        graph_builder
            .has_area_edge(
                region,
                &boundary.vertices[1],
                &boundary.vertices[0],
                &mut budget,
            )
            .expect_err("opposite selected boundary orientation must be rejected");
        let mut graph = graph_builder.finish(topology.sources.frame, &mut budget)?;
        let NativeReferenceGraphResult {
            cycles,
            parts,
            face_regions,
            face_groups,
            face_cells,
        } = native_reference_cycles_and_parts(&graph, &edges, &[], true, true, &mut budget)?;
        graph.cycles = cycles;
        graph.parts = parts;
        graph.face_regions = face_regions;
        graph.face_groups = face_groups;
        graph.face_cells = face_cells;

        assert_eq!(edges.len(), 12);
        assert_eq!(graph.points.exact.len(), 10);
        assert_eq!(graph.written_edges.len(), 12);
        assert_eq!(graph.atoms.len(), 11);
        assert_eq!(graph.faces.count, 3);
        assert_eq!(graph.cycles.len(), 2);
        assert_eq!(graph.parts.len(), 2);
        assert!(graph.parts.iter().all(|part| part.holes.is_empty()));
        assert!(
            graph
                .cycles
                .iter()
                .all(|cycle| cycle.edges.len() == 6 && cycle.shell)
        );
        let point = |x, y| {
            ExactChartPoint::from_rationals(ExactRational::from_f64(x), ExactRational::from_f64(y))
        };
        let expected_cells = [
            [
                point(-180.0, -90.0),
                point(-90.0, -90.0),
                point(0.0, -90.0),
                point(0.0, 90.0),
                point(-90.0, 90.0),
                point(-180.0, 90.0),
            ],
            [
                point(0.0, -90.0),
                point(90.0, -90.0),
                point(180.0, -90.0),
                point(180.0, 90.0),
                point(90.0, 90.0),
                point(0.0, 90.0),
            ],
        ];
        let mut seen_cells = [false; 2];
        let mut seam_vertices = [None, None];
        for part in &graph.parts {
            let cycle = graph.cycles.get(part.shell).ok_or_else(|| {
                GeometryErrorKind::Invalid("whole-sphere test shell is unknown".into())
            })?;
            let mut cell = None;
            for cycle_edge in &cycle.edges {
                for &written_edge in &cycle_edge.contributors {
                    let written = edges.get(written_edge).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "whole-sphere test contributor is unknown".into(),
                        )
                    })?;
                    let NativeAreaEdgeKind::WholeSphereCover { cell: edge_cell } = &written.kind
                    else {
                        return Err(GeometryErrorKind::Invalid(
                            "whole-sphere test shell contains a non-cover edge".into(),
                        )
                        .into());
                    };
                    if cell.is_some_and(|previous| previous != *edge_cell) {
                        return Err(GeometryErrorKind::Invalid(
                            "whole-sphere test shell mixes cell owners".into(),
                        )
                        .into());
                    }
                    cell = Some(*edge_cell);
                }
            }
            let cell = cell.ok_or_else(|| {
                GeometryErrorKind::Invalid("whole-sphere test shell has no cell owner".into())
            })?;
            let expected = expected_cells.get(cell as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("whole-sphere test has an unknown cell".into())
            })?;
            let mut seen_edges = [false; 6];
            for cycle_edge in &cycle.edges {
                let start = native_graph_cycle_vertex(
                    topology.sources.frame,
                    &graph,
                    cycle_edge,
                    0,
                    &edges,
                    &mut budget,
                )?;
                let end = native_graph_cycle_vertex(
                    topology.sources.frame,
                    &graph,
                    cycle_edge,
                    1,
                    &edges,
                    &mut budget,
                )?;
                let mut matched = None;
                for start_index in 0..expected.len() {
                    let end_index = start_index
                        .checked_add(1)
                        .ok_or(ResourceError::SizeOverflow)?
                        .checked_rem(expected.len())
                        .ok_or(ResourceError::SizeOverflow)?;
                    if expected.get(start_index) == Some(&start.point)
                        && expected.get(end_index) == Some(&end.point)
                    {
                        matched = Some(start_index);
                        break;
                    }
                }
                let matched = matched.ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "whole-sphere test shell differs from its expected longitude strip".into(),
                    )
                })?;
                let seen = seen_edges.get_mut(matched).ok_or_else(|| {
                    GeometryErrorKind::Invalid("whole-sphere test edge is unknown".into())
                })?;
                assert!(!std::mem::replace(seen, true));
                let seam_edge = if cell == 0 { 5 } else { 2 };
                if matched == seam_edge {
                    let seam = seam_vertices.get_mut(cell as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("whole-sphere test seam cell is unknown".into())
                    })?;
                    *seam = Some([
                        graph.local_vertex(native_chart_vertex_key(&start), &mut budget)?,
                        graph.local_vertex(native_chart_vertex_key(&end), &mut budget)?,
                    ]);
                }
            }
            assert!(seen_edges.into_iter().all(|seen| seen));
            *seen_cells.get_mut(cell as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("whole-sphere test cell is unknown".into())
            })? = true;
        }
        assert_eq!(seen_cells, [true, true]);
        let [Some(west), Some(east)] = seam_vertices else {
            return Err(GeometryErrorKind::Invalid(
                "whole-sphere test lost a sewn seam edge".into(),
            )
            .into());
        };
        let mut pole_aliases = UnionFind::new(graph.points.exact.len());
        for sewing in &graph.sewing {
            if sewing.kind == NativeGraphSewingKind::PoleLongitude {
                pole_aliases.union(sewing.vertices[0] as usize, sewing.vertices[1] as usize);
            }
        }
        for (west_vertex, east_vertex) in [(west[0], east[1]), (west[1], east[0])] {
            let west_key = graph.vertex_keys.get(west_vertex as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("whole-sphere test west pole is unknown".into())
            })?;
            let east_key = graph.vertex_keys.get(east_vertex as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("whole-sphere test east pole is unknown".into())
            })?;
            assert_eq!(west_key.node, east_key.node);
            assert!(matches!(west_key.spelling, NativeChartSpellingKey::Pole(_)));
            assert!(matches!(east_key.spelling, NativeChartSpellingKey::Pole(_)));
            assert_eq!(
                graph.topology.component_of.get(west_vertex as usize),
                graph.topology.component_of.get(east_vertex as usize),
            );
            assert_eq!(
                pole_aliases.find_with_work(west_vertex as usize, &mut budget)?,
                pole_aliases.find_with_work(east_vertex as usize, &mut budget)?,
            );
        }
        assert!(native_graph_sewing_matches(
            &graph.sewing,
            &graph.vertex_keys,
            &graph.points.exact,
            topology.sources.frame,
            &mut budget,
        )?);
        let mut omitted_pole_aliases = graph.sewing.clone();
        omitted_pole_aliases.retain(|sewing| sewing.kind != NativeGraphSewingKind::PoleLongitude);
        assert!(omitted_pole_aliases.len() < graph.sewing.len());
        assert!(
            !native_graph_sewing_matches(
                &omitted_pole_aliases,
                &graph.vertex_keys,
                &graph.points.exact,
                topology.sources.frame,
                &mut budget,
            )?,
            "required pole sewing must not be waived by connected ordinary chart atoms"
        );
        let shared = graph
            .atoms
            .iter()
            .filter(|atom| atom.contributors.len() == 2)
            .collect::<Vec<_>>();
        assert_eq!(shared.len(), 1);
        let shared_atom = shared.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("whole-sphere shared edge is unknown".into())
        })?;
        let left = shared_atom.contributors.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("whole-sphere shared edge has no left owner".into())
        })?;
        let right = shared_atom.contributors.get(1).ok_or_else(|| {
            GeometryErrorKind::Invalid("whole-sphere shared edge has no right owner".into())
        })?;
        assert_ne!(left.reversed, right.reversed);
        let left_edge = edges.get(left.written_edge).ok_or_else(|| {
            GeometryErrorKind::Invalid("whole-sphere shared edge owner is unknown".into())
        })?;
        let right_edge = edges.get(right.written_edge).ok_or_else(|| {
            GeometryErrorKind::Invalid("whole-sphere shared edge owner is unknown".into())
        })?;
        assert!(matches!(
            (&left_edge.kind, &right_edge.kind),
            (
                NativeAreaEdgeKind::WholeSphereCover { cell: 0 },
                NativeAreaEdgeKind::WholeSphereCover { cell: 1 },
            ) | (
                NativeAreaEdgeKind::WholeSphereCover { cell: 1 },
                NativeAreaEdgeKind::WholeSphereCover { cell: 0 },
            )
        ));
        assert_eq!(families.additional_constructed(), 10);
        Ok(())
    }

    #[test]
    fn published_sample_has_its_own_native_neighbor_domain() -> Result<()> {
        let topology = topology(&[crate::io::parse_wkt("LINESTRING (0 0, 10 10)")?])?;
        let frame = topology.sources.frame;
        let half_ulp = ExactRational::from_f64(2.0_f64.powi(-54));
        let exact = ExactChartPoint::from_rationals(
            ExactRational::from_f64(10.0).try_add(&half_ulp)?,
            ExactRational::from_f64(1.0).try_add(&half_ulp)?,
        );
        let mut work = crate::geometry::Unmetered;
        let mut nodes = EmissionNodeRegistry::with_sources(&topology, &[], &mut work)?;
        let node = nodes.intern_computed(frame, &exact, &mut work)?;
        let mut publication = PublicationInventory {
            fragments: Vec::new(),
            samples: vec![super::super::emission::PublishedSample {
                request_row: RequestRowId::from_index(0)?,
                family: super::super::emission::FeatureFamily {
                    parent: super::super::emission::FeatureFamilyParent::Fragment(
                        super::super::emission::FragmentRecipeId(0),
                    ),
                    producer: super::super::emission::SampleProducerKind::Publication,
                    occurrence: 0,
                },
                parameter: ExactRational::from_f64(0.5),
                written_vertex: 1,
                point: Point::new_raw_xy(10.0, 1.0),
                exact_point: exact,
                published_chart_point: ExactChartPoint::from_rationals(
                    ExactRational::from_f64(10.0),
                    ExactRational::from_f64(1.0),
                ),
                source_trace: None,
                endpoint_only: false,
            }],
            counts: Vec::new(),
            ordinates: vec![NodeOrdinates::default()],
        };
        let mut budget = EvaluationBudget::new("Q publication domain", 0)?;
        let domains = published_domains(&topology, &publication, &nodes, &[node], &mut budget)?;

        assert_eq!(domains.variables, [node]);
        let nearest = domains
            .nearest
            .get(node.0 as usize)
            .copied()
            .flatten()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("published nearest site is missing".into())
            })?;
        let candidates = domains
            .domains
            .get(node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("published domain is missing".into()))?;
        assert_eq!(candidates.len(), 9);
        assert!(candidates.contains(&nearest));
        let fixed_source_axes = vec![[false, false]; topology.q.nodes.len()];
        let producer_witness = PreparedProducerWitness::default();
        let UnifiedQClassBuild {
            source_variable_count: source_base,
            classes,
            node_location: source_location,
            publication_location,
            states,
        } = build_unified_q_classes(
            &UnifiedQClassInputs {
                topology: &topology,
                source_classes: &[],
                publication_variables: &domains.variables,
                publication_nodes: &nodes,
                source_chords: &[],
                publication_chords: &[],
                proximity_pairs: &[],
                cut_rows: &[],
                fan_rows: &[],
                publication_pairs: &[],
                source_domains: &[],
                source_movable_axes: &fixed_source_axes,
                publication_domains: &domains.domains,
                producer_witness: &producer_witness,
            },
            &mut budget,
        )?;
        assert_eq!(source_base, 0);
        assert_eq!(classes, vec![vec![QVariableNode::Published(node)]]);
        assert!(source_location.iter().all(Option::is_none));
        assert_eq!(
            publication_location.get(node.0 as usize).copied().flatten(),
            Some((0, 0))
        );
        assert_eq!(states.len(), 1);

        publication
            .samples
            .get_mut(0)
            .ok_or_else(|| GeometryErrorKind::Invalid("published sample is missing".into()))?
            .published_chart_point
            .y = ExactRational::from_f64(1.0_f64.next_up());
        let error = published_domains(&topology, &publication, &nodes, &[node], &mut budget)
            .err()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "one-ULP wrong assigned Y must reject its frozen publication image".into(),
                )
            })?;
        assert!(
            error
                .to_string()
                .contains("published nearest image differs from its exact source reference"),
            "wrong assigned-Y rejection came from an unexpected owner: {error}"
        );
        Ok(())
    }

    #[test]
    fn checked_publication_samples_reject_an_omitted_leaf_occurrence() -> Result<()> {
        let line = crate::io::parse_wkt("LINESTRING (0 0, 360 10)")?;
        let topology = topology(&[line])?;
        let mut budget = EvaluationBudget::new("omitted publication sample", 0)?;
        let plan = QEmissionPlan::build(&topology, OverlayOp::Union, &mut budget)?;
        let nearest = QCoordinates::build_with_work(&topology, &mut budget)?;
        let publication = plan.fragment_recipes.publish(
            &topology,
            &nearest,
            OverlayOp::Union.name(),
            &mut budget,
        )?;
        assert!(
            !publication.samples.is_empty(),
            "the full-turn fixture must publish at least one checked sample"
        );
        let source_points = vec![None; topology.sources.points.len()];
        let emission =
            plan.embedding_emission(&topology, &source_points, &publication, &mut budget)?;
        let ordinates = vec![NodeOrdinates::default(); topology.q.nodes.len()];
        let metric_access: Option<&SourceArcMetricAccess<'_, '_, crate::crs::GeodesicMetric>> =
            None;
        let mut problem = QEmbeddingProblem::new(
            &topology,
            &emission,
            &nearest,
            &ordinates,
            crate::geometry::CoordinateAxes::XY,
            false,
            "omitted publication sample",
            metric_access,
            &mut budget,
        )?;

        let omitted_sample = 0;
        let original_chord_count = problem.publication_chords.len();
        problem.publication_chords.retain(|chord| {
            !chord
                .endpoints
                .iter()
                .any(|endpoint| publication_sample_index(endpoint) == Some(omitted_sample))
        });
        assert!(
            problem.publication_chords.len() < original_chord_count,
            "the omitted sample must have producer-leaf occurrences"
        );
        let producer = AssignedProducerProjection {
            source_ports: Vec::new(),
            leaves: problem
                .publication_chords
                .iter()
                .map(|chord| AssignedProducerLeaf {
                    recipe: chord.recipe,
                    trace_edge: chord.trace_edge,
                    points: chord.exact.clone(),
                })
                .collect(),
            rejections: Vec::new(),
        };
        let error = problem
            .checked_publication_sample_images(&producer, &mut budget)
            .err()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "every publication occurrence must have a checked leaf image".into(),
                )
            })?;
        assert!(
            error
                .to_string()
                .contains("publication sample has no checked source-port or leaf image"),
            "omitted-sample rejection came from an unexpected owner: {error}"
        );
        Ok(())
    }

    fn single_sample_native_reference<'topology>(
        topology: &'topology ExactGeographicTopology,
        budget: &mut EvaluationBudget,
        assigned: QCandidate,
    ) -> Result<(NativeReference<'topology>, EmissionPortId)> {
        use super::super::emission::{
            EmissionBuilder, FeatureFamily, FeatureFamilyParent, PublishedSample,
            SampleProducerKind, VertexOrigin,
        };

        let selected = QEmissionPlan::build(topology, OverlayOp::Union, budget)?;
        let exact = ExactChartPoint::from_rationals(
            ExactRational::from_f64(10.0).try_add(&ExactRational::from_f64(2.0_f64.powi(-54)))?,
            ExactRational::from_f64(1.0).try_add(&ExactRational::from_f64(2.0_f64.powi(-54)))?,
        );
        let sample = PublishedSample {
            request_row: RequestRowId::from_index(0)?,
            family: FeatureFamily {
                parent: FeatureFamilyParent::Fragment(FragmentRecipeId(0)),
                producer: SampleProducerKind::Publication,
                occurrence: 0,
            },
            parameter: ExactRational::from_f64(0.5),
            written_vertex: 1,
            point: Point::new_raw_xy(10.0, 1.0),
            exact_point: exact,
            published_chart_point: ExactChartPoint::from_rationals(
                ExactRational::from_f64(10.0),
                ExactRational::from_f64(1.0),
            ),
            source_trace: None,
            endpoint_only: false,
        };
        let mut builder = EmissionBuilder::new(topology, &selected, &[], false, budget)?;
        let sample_nodes = builder.reserve_publication_nodes(&[sample], budget)?;
        let sample_node = *sample_nodes.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("Native literal fixture has no sample node".into())
        })?;
        let assigned_point =
            candidate_chart_point_with_work(topology.sources.frame, assigned, budget)?;
        let port_data = NativePortData {
            node: sample_node,
            point: assigned_point,
            spelling: PortSpelling::Interior { periods: 0 },
            source_record: None,
            occurrences: vec![OccurrenceSource {
                origin: VertexOrigin::SourceSample,
                source_trace: None,
            }],
        };
        let port_id = builder.port(port_data.try_clone_with_work(budget)?)?;
        builder.point(port_id)?;
        let plan = builder.finish();
        let graph_builder = new_native_reference_graph_builder(&[], &[port_data], budget)?;
        let graph = graph_builder.finish(topology.sources.frame, budget)?;
        let dimension =
            admit_logical_layout(&graph.common, Vec::new(), Some(&graph.faces), budget)?;
        let vertex = graph.local_vertex(
            native_emission_port_key(
                plan.ports
                    .get(port_id.0 as usize)
                    .ok_or_else(|| GeometryErrorKind::Invalid("sample port is unknown".into()))?,
            ),
            budget,
        )?;
        let reference = NativeReference {
            plan,
            dimension,
            port_vertices: vec![vertex],
            sites: vec![Some(assigned_emission_site(
                topology.sources.frame,
                assigned,
            )?)],
            feature_nodes: Vec::new(),
            publication_sample_nodes: vec![sample_node],
            line_traces: Vec::new(),
            literal_chord_uses: Vec::new(),
            line_graph_edges: Vec::new(),
            area_edges: Vec::new(),
            pole_sectors: Vec::new(),
            graph,
        };
        Ok((reference, port_id))
    }

    #[test]
    fn native_literal_readback_rejects_a_post_pass_published_port_mutation() -> Result<()> {
        use super::super::emission::native_port_point;
        use crate::geometry::Shape;

        let topology = topology(&[crate::io::parse_wkt("POINT (0 0)")?])?;
        let mut budget = EvaluationBudget::new("native literal readback", 0)?;
        let assigned = QCandidate {
            native: XY::new(10.0, 1.0),
            periods: 0,
        };
        let (reference, port_id) =
            single_sample_native_reference(&topology, &mut budget, assigned)?;
        let vertex = *reference
            .port_vertices
            .get(port_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("sample graph vertex is unknown".into()))?;
        let sites = [assigned_emission_site(topology.sources.frame, assigned)?];
        let ordinates = [NodeOrdinates::default()];
        let mut record = native_port_point(
            &reference.plan,
            &sites,
            &ordinates,
            port_id,
            crate::geometry::CoordinateAxes::XY,
        )?;
        record.y = record.y.next_up();
        let mut exact_points = reference.graph.points.exact.clone();
        exact_points
            .get_mut(vertex as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("published vertex is unknown".into()))?
            .y = ExactRational::from_f64(record.y);
        let candidate_points = NativeReferencePoints::from_exact(exact_points, &mut budget)?;
        let checked = admit_candidate(
            &reference.graph.common,
            &reference.dimension,
            candidate_points,
            &mut budget,
        )?
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("mutated native test assignment was not admitted".into())
        })?;
        let assignment = NativeCandidateAssignment::issue(&reference, checked)?;
        let mut port_records = vec![None; reference.plan.ports.len()];
        let buffers = FrozenLiteralBuffers::shapes(Shape::Point(record), ShapeLiteralLayout {
            points: vec![LiteralPoint {
                output: 0,
                axes: crate::geometry::CoordinateAxes::XY,
                image: port_id,
            }],
            lines: Vec::new(),
            polygons: Vec::new(),
        });
        let receipt = NativeCandidatePublication {
            assignment: &assignment,
            buffers,
            sites: &sites,
            ordinates: &ordinates,
            port_records: &mut port_records,
        };
        assert!(
            matches!(
                finish_geographic(
                    PublicationCall::new(
                        ProducerReceipt::QNative(receipt),
                        topology.publication_frame().clone(),
                        &mut budget,
                    ),
                    StorageTarget::Scalar,
                )?,
                PublicationOutcome::Reject(PublicationRejection::Native)
            ),
            "the common Native binder rejects a post-admission endpoint mutation"
        );
        Ok(())
    }

    #[test]
    fn native_assignment_factory_rejects_crosswired_graph_layout_and_images() -> Result<()> {
        let topology = topology(&[crate::io::parse_wkt("POINT (0 0)")?])?;
        let mut budget = EvaluationBudget::new("crosswired native receiver", 0)?;
        let (reference, _) = single_sample_native_reference(&topology, &mut budget, QCandidate {
            native: XY::new(10.0, 1.0),
            periods: 0,
        })?;
        let (other_reference, _) =
            single_sample_native_reference(&topology, &mut budget, QCandidate {
                native: XY::new(11.0, 1.0),
                periods: 0,
            })?;
        let foreign_assignment = admit_candidate(
            &other_reference.graph.common,
            &other_reference.dimension,
            other_reference.graph.points.clone(),
            &mut budget,
        )?
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("foreign reference assignment was not admitted".into())
        })?;

        assert!(
            NativeCandidateAssignment::issue(&reference, foreign_assignment).is_err(),
            "a graph, layout, and image from another native reference must not issue a receiver"
        );
        Ok(())
    }

    #[test]
    fn native_receiver_rejects_missing_producer_literal_rows() -> Result<()> {
        let topology = topology(&[crate::io::parse_wkt("POINT (0 0)")?])?;
        let mut budget = EvaluationBudget::new("native literal row binding", 0)?;
        let assigned = QCandidate {
            native: XY::new(10.0, 1.0),
            periods: 0,
        };
        let (reference, _) = single_sample_native_reference(&topology, &mut budget, assigned)?;
        let checked = admit_candidate(
            &reference.graph.common,
            &reference.dimension,
            reference.graph.points.clone(),
            &mut budget,
        )?
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("unchanged Native reference assignment was rejected".into())
        })?;
        let assignment = NativeCandidateAssignment::issue(&reference, checked)?;
        let sites = [assigned_emission_site(topology.sources.frame, assigned)?];
        let ordinates = [NodeOrdinates::default()];
        let mut port_records = vec![None; reference.plan.ports.len()];
        let missing_rows = FrozenLiteralBuffers::shapes(
            crate::geometry::Shape::Empty(
                crate::geometry::GeometryKind::Point,
                crate::geometry::CoordinateAxes::XY,
            ),
            ShapeLiteralLayout {
                points: Vec::<LiteralPoint<EmissionPortId>>::new(),
                lines: Vec::new(),
                polygons: Vec::new(),
            },
        );

        let receipt = NativeCandidatePublication {
            assignment: &assignment,
            buffers: missing_rows,
            sites: &sites,
            ordinates: &ordinates,
            port_records: &mut port_records,
        };
        assert!(
            matches!(
                finish_geographic(
                    PublicationCall::new(
                        ProducerReceipt::QNative(receipt),
                        topology.publication_frame().clone(),
                        &mut budget,
                    ),
                    StorageTarget::Scalar,
                )?,
                PublicationOutcome::Reject(PublicationRejection::Native)
            ),
            "the consuming Q sink must reject a sealed output that omits its point row"
        );
        Ok(())
    }

    fn fixtures() -> Result<Vec<(&'static str, Vec<crate::geometry::Shape>)>> {
        Ok(vec![
            ("stored_alias", vec![crate::io::parse_wkt(
                "LINESTRING (122.427194 10, 123.427194 11)",
            )?]),
            ("exact_crossing", vec![
                crate::io::parse_wkt("LINESTRING (0 0, 2 2)")?,
                crate::io::parse_wkt("LINESTRING (0 2, 2 0)")?,
            ]),
            ("inexact_crossing_with_pinned_latitude", vec![
                crate::io::parse_wkt("LINESTRING (0 0, 2 0)")?,
                crate::io::parse_wkt("LINESTRING (0 1, 1 -2)")?,
            ]),
            ("seam_cut_and_pole", vec![
                crate::io::parse_wkt(
                    "POLYGON ((-180 10, -90 10, 0 10, 90 10, 180 10, 180 20, 90 20, 0 20, -90 20, -180 20, -180 10))",
                )?,
                crate::io::parse_wkt("POINT (0 90)")?,
            ]),
        ])
    }

    #[test]
    fn canonical_coordinate_owner_preserves_all_nearest_and_pole_port_bits() -> Result<()> {
        let frame = GeographicFrame::crs84();
        for (name, shapes) in fixtures()? {
            let topology = topology(&shapes)?;
            let actual = QCoordinates::build(&topology)?;
            for node in &topology.q.nodes {
                let first_port_id =
                    *topology.q.node_ports.get(node.ports.start).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q node has no first port".into())
                    })?;
                let first_port = topology
                    .q
                    .ports
                    .get(first_port_id.0 as usize)
                    .ok_or_else(|| GeometryErrorKind::Invalid("Q first port is unknown".into()))?;
                let expected = XY::new(
                    round_canonical_longitude(frame, &first_port.point.x)?,
                    Latitude::try_new(frame, first_port.point.y.round()?)
                        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?
                        .get(),
                );
                let actual_xy = actual.xy(node.id)?;
                assert_eq!(
                    [actual_xy.x.to_bits(), actual_xy.y.to_bits()],
                    [expected.x.to_bits(), expected.y.to_bits()],
                    "{name}/node {:?}",
                    node.id
                );
                if node.pole.is_some() {
                    for &port_id in
                        topology
                            .q
                            .node_ports
                            .get(node.ports.clone())
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid("Q pole ports are unknown".into())
                            })?
                    {
                        let port = topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
                            GeometryErrorKind::Invalid("Q pole port is unknown".into())
                        })?;
                        assert_eq!(
                            actual.pole_port_longitude[port_id.0 as usize]
                                .expect("every pole port has its longitude")
                                .to_bits(),
                            round_canonical_longitude(frame, &port.point.x)?.to_bits(),
                            "{name}/pole port {port_id:?}",
                        );
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn canonical_inexact_axes_match_the_previous_exact_port_alias_rule() -> Result<()> {
        let mut stored = 0_usize;
        let mut constructed_exact = 0_usize;
        let mut constructed_inexact = 0_usize;
        let mut cut = 0_usize;
        let mut pole = 0_usize;
        let mut pinned_latitude = 0_usize;

        for (name, shapes) in fixtures()? {
            let topology = topology(&shapes)?;
            let frame = topology.sources.frame;
            let coordinates = QCoordinates::build(&topology)?;
            let mut stored_nodes = std::collections::BTreeSet::new();
            for &port_id in topology
                .q
                .source_edge_ports
                .iter()
                .flatten()
                .chain(&topology.q.source_point_ports)
            {
                stored_nodes.insert(
                    topology
                        .q
                        .ports
                        .get(port_id.0 as usize)
                        .ok_or_else(|| GeometryErrorKind::Invalid("stored port is unknown".into()))?
                        .node,
                );
            }

            for node in &topology.q.nodes {
                let nearest = coordinates.xy(node.id)?;
                let old_axes = if node.pole.is_some() || matches!(&node.key, QNodeKey::Cut(_)) {
                    [false, false]
                } else {
                    let port_id = topology
                        .q
                        .node_ports
                        .get(node.ports.clone())
                        .and_then(|ports| {
                            ports.iter().copied().find(|port_id| {
                                topology
                                    .q
                                    .ports
                                    .get(port_id.0 as usize)
                                    .is_some_and(|port| !port.cut)
                            })
                        })
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid("source Q node has no non-cut port".into())
                        })?;
                    let port = topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("source Q port is unknown".into())
                    })?;
                    let exact = ExactNodePosition {
                        longitude: nearest_exact_longitude_alias(
                            frame,
                            &port.point.x,
                            &ExactRational::from_f64(nearest.x),
                        )?,
                        latitude: port.point.y.clone(),
                    };
                    inexact_axes(&exact, nearest)?
                };
                let old_inexact = old_axes.into_iter().any(|axis| axis);
                let actual_inexact = coordinates.has_inexact_nodes(&topology, [node.id])?;
                assert_eq!(actual_inexact, old_inexact, "{name}/node {:?}", node.id);

                if node.pole.is_some() {
                    pole += 1;
                    assert!(!actual_inexact);
                } else if matches!(&node.key, QNodeKey::Cut(_)) {
                    cut += 1;
                    assert!(!actual_inexact);
                } else if stored_nodes.contains(&node.id) {
                    stored += 1;
                    assert!(!actual_inexact, "stored node moved");
                } else if old_inexact {
                    constructed_inexact += 1;
                    if name == "inexact_crossing_with_pinned_latitude" {
                        assert_eq!(old_axes, [true, false]);
                        pinned_latitude += 1;
                    }
                } else {
                    constructed_exact += 1;
                }
            }
        }

        assert!(stored > 0);
        assert!(constructed_exact > 0);
        assert!(constructed_inexact > 0);
        assert!(cut > 0);
        assert!(pole > 0);
        assert!(pinned_latitude > 0);
        Ok(())
    }

    #[test]
    fn canonical_node_table_matches_port_rounding_bits() -> Result<()> {
        let frame = GeographicFrame::crs84();
        for (name, shapes) in fixtures()? {
            let topology = topology(&shapes)?;
            let canonical = QCoordinates::build(&topology)?;
            for node in &topology.q.nodes {
                let port_id =
                    *topology.q.node_ports.get(node.ports.start).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q node has no first port".into())
                    })?;
                let port =
                    topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q first port is unknown".into())
                    })?;
                let old = XY::new(
                    round_canonical_longitude(frame, &port.point.x)?,
                    Latitude::try_new(frame, port.point.y.round()?)
                        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?
                        .get(),
                );
                let actual = canonical.xy(node.id)?;
                assert_eq!(
                    (actual.x.to_bits(), actual.y.to_bits()),
                    (old.x.to_bits(), old.y.to_bits()),
                    "{name}/QNodeId({})",
                    node.id.0
                );
                if node.pole.is_some() {
                    for &port_id in
                        topology
                            .q
                            .node_ports
                            .get(node.ports.clone())
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid("Q pole ports are unknown".into())
                            })?
                    {
                        let pole_port = &topology.q.ports[port_id.0 as usize];
                        assert_eq!(
                            canonical.pole_port_longitude[port_id.0 as usize]
                                .expect("pole has a per-port longitude")
                                .to_bits(),
                            round_canonical_longitude(frame, &pole_port.point.x)?.to_bits(),
                            "{name}/QPortId({})",
                            port_id.0
                        );
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn q_domain_keeps_both_native_neighbors_and_orders_the_true_left_site() -> Result<()> {
        let frame = GeographicFrame::crs84();
        let half = frame.longitude().half_period();
        let ulp = 2.0_f64.powi(-45);
        let line = |start: Point, end: Point| -> Result<crate::geometry::Shape> {
            let coordinates = CoordSeq::try_from_points(&[start, end])?;
            Ok(crate::geometry::Shape::LineString(
                crate::geometry::LineSeq::from_proven(coordinates),
            ))
        };
        let shapes = [
            line(
                Point::new_raw_xy(half - ulp, 0.0),
                Point::new_raw_xy(half, 2.0),
            )?,
            line(
                Point::new_raw_xy(half - ulp, 2.0),
                Point::new_raw_xy(half, 0.0),
            )?,
            crate::geometry::Shape::Point(Point::new_raw_xy(half - ulp, 1.0)),
        ];
        let topology = topology(&shapes)?;
        let mut budget = EvaluationBudget::new("q native domain", 0)?;
        let plan = super::super::emission::plan::QEmissionPlan::build(
            &topology,
            OverlayOp::Union,
            &mut budget,
        )?;
        let source_points = vec![None; topology.sources.points.len()];
        let nearest = QCoordinates::build_with_work(&topology, &mut budget)?;
        let publication = plan.fragment_recipes.publish(
            &topology,
            &nearest,
            OverlayOp::Union.name(),
            &mut budget,
        )?;
        let emission =
            plan.embedding_emission(&topology, &source_points, &publication, &mut budget)?;
        let ordinates = vec![NodeOrdinates::default(); topology.q.nodes.len()];
        let metric_access: Option<&SourceArcMetricAccess<'_, '_, crate::crs::GeodesicMetric>> =
            None;
        let problem = QEmbeddingProblem::new(
            &topology,
            &emission,
            &nearest,
            &ordinates,
            crate::geometry::CoordinateAxes::XY,
            false,
            "q native domain",
            metric_access,
            &mut budget,
        )?;

        let exact_longitude =
            ExactRational::from_f64(half).try_subtract(&ExactRational::from_f64(ulp / 2.0))?;
        let exact_latitude = ExactRational::one();
        let (target_index, _) = problem
            .exact_positions
            .iter()
            .enumerate()
            .find(|(_, exact)| {
                exact.as_ref().is_some_and(|exact| {
                    exact.longitude == exact_longitude && exact.latitude == exact_latitude
                })
            })
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("the crossing has no constructed Q node".into())
            })?;
        let target = QNodeId(u32::try_from(target_index).map_err(|_| ResourceError::SizeOverflow)?);
        let nearest_target = nearest.candidate(target)?;
        assert_eq!(nearest_target.native.x, -half);
        assert_eq!(nearest_target.periods, 1);

        let left = QCandidate {
            native: XY::new(half - ulp, 1.0),
            periods: 0,
        };
        let right = QCandidate {
            native: XY::new(-half + ulp, 1.0),
            periods: 1,
        };
        let seam = QCandidate {
            native: XY::new(-half, 1.0),
            periods: 1,
        };
        let domain = problem
            .domains
            .get(target.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("crossing node has no domain".into()))?;
        assert!(domain.contains(&left));
        assert!(domain.contains(&right));
        assert!(!domain.contains(&seam));
        let mut compare_work = Unmetered;
        assert_eq!(
            problem.compare_candidates(
                QVariableNode::Source(target),
                left,
                right,
                &mut compare_work,
            )?,
            Ordering::Less,
            "native+lift distances are u²/4 on the left and 9u²/4 on the right",
        );

        let point_port =
            *topology.q.source_point_ports.first().ok_or_else(|| {
                GeometryErrorKind::Invalid("fixed witness point has no port".into())
            })?;
        let fixed_node = topology
            .q
            .ports
            .get(point_port.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("fixed witness port is unknown".into()))?
            .node;
        let (class, _) = problem
            .node_location
            .get(target.0 as usize)
            .copied()
            .flatten()
            .ok_or_else(|| GeometryErrorKind::Invalid("crossing node is not movable".into()))?;
        assert!(
            problem.classes.get(class).is_some_and(|variables| {
                !variables.contains(&QVariableNode::Source(fixed_node))
            })
        );
        let variables = problem
            .classes
            .get(class)
            .ok_or_else(|| GeometryErrorKind::Invalid("crossing class is unknown".into()))?;
        let mut fixed_work = Unmetered;
        let fixed_keys =
            <QEmbeddingProblem<'_, '_, '_, '_, crate::crs::GeodesicMetric> as Problem>::fixed_keys(
                &problem,
                class,
                variables,
                &mut fixed_work,
            )?;
        assert!(fixed_keys.contains(&emitted_node_key(frame, XY::new(half - ulp, 1.0),)?));
        Ok(())
    }
}

#[cfg(test)]
mod ring_filter_tests {
    #![expect(
        clippy::panic_in_result_fn,
        reason = "numerical filter controls assert independently known exact windings"
    )]

    use super::*;

    fn exact_points(values: &[(f64, f64)]) -> Vec<ExactChartPoint> {
        values
            .iter()
            .map(|&(x, y)| {
                ExactChartPoint::from_rationals(
                    ExactRational::from_f64(x),
                    ExactRational::from_f64(y),
                )
            })
            .collect()
    }

    #[test]
    fn quotient_circle_roles_keep_stored_period_edges_and_reject_retraces() -> Result<()> {
        use crate::geographic::RingDomain;

        let frame = GeographicFrame::crs84();
        for coordinates in [[(0.0, 60.0), (360.0, 60.0), (315.0, 65.0), (0.0, 60.0)], [
            (0.0, -60.0),
            (-360.0, -60.0),
            (-315.0, -65.0),
            (0.0, -60.0),
        ]] {
            let ring = rounded_exact_ring(&exact_points(&coordinates))?;
            let summary = RingSummary::try_new(frame, &GeographicRing::from_ring(&ring))
                .expect("stored full-period circle is admitted");
            assert!(matches!(summary.domain, RingDomain::FullLongitude(_)));
            assert_eq!(quotient_ring_role(frame, &ring)?, Some(false));
            let reversed = coordinates.into_iter().rev().collect::<Vec<_>>();
            let reversed_ring = rounded_exact_ring(&exact_points(&reversed))?;
            assert_eq!(quotient_ring_role(frame, &reversed_ring)?, Some(true));
        }
        let retrace = rounded_exact_ring(&exact_points(&[
            (0.0, 60.0),
            (360.0, 60.0),
            (0.0, 60.0),
            (0.0, 60.0),
        ]))?;
        let summary = RingSummary::try_new(frame, &GeographicRing::from_ring(&retrace))
            .expect("zero-turn retrace has a summary");
        assert_eq!(summary.chart_turn, 0);
        assert_eq!(summary.domain, RingDomain::Ordinary);
        assert_eq!(quotient_ring_role(frame, &retrace)?, None);
        Ok(())
    }

    #[test]
    fn quotient_closure_roles_use_chart_turn_instead_of_folded_winding() -> Result<()> {
        let frame = GeographicFrame::crs84();
        for (name, coordinates) in [
            ("north", [
                (0.0, 80.0),
                (90.0, 90.0),
                (180.0, 80.0),
                (-90.0, 80.0),
                (0.0, 80.0),
            ]),
            ("south", [
                (0.0, -80.0),
                (-90.0, -90.0),
                (-180.0, -80.0),
                (90.0, -80.0),
                (0.0, -80.0),
            ]),
            ("equator-touching north", [
                (0.0, 0.0),
                (90.0, 10.0),
                (180.0, 0.0),
                (-90.0, 0.0),
                (0.0, 0.0),
            ]),
        ] {
            let ports = exact_points(&coordinates);
            let published = rounded_exact_ring(&ports)?;
            let role = compare_q_ring_role(frame, &published, &published, &ports, true)?
                .expect("bounded quotient closure has a role");
            assert!(!role.exact_is_hole, "{name}");
            assert!(!role.emitted_is_hole, "{name}");
            assert!(role.matches(), "{name}");
            let reversed = ports.iter().rev().cloned().collect::<Vec<_>>();
            let reversed_ring = rounded_exact_ring(&reversed)?;
            let complement =
                compare_q_ring_role(frame, &reversed_ring, &reversed_ring, &ports, true)?
                    .expect("reversed bounded closure has a role");
            assert!(complement.emitted_is_hole, "{name}");
            assert!(!complement.matches(), "{name}");
        }
        Ok(())
    }

    #[test]
    fn interval_ring_filter_certifies_fixed_known_windings() {
        let frame = GeographicFrame::crs84();
        let unit = f64::from_bits(0x3D10_0000_0000_0000);
        let mut rational_box =
            exact_points(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]);
        for point in &mut rational_box {
            point.x = point
                .x
                .try_quotient(&ExactRational::from_f64(3.0))
                .expect("divide rational longitude");
            point.y = point
                .y
                .try_quotient(&ExactRational::from_f64(7.0))
                .expect("divide rational latitude");
        }
        let mut translated_box = exact_points(&[
            (122.427_194, 10.0),
            (123.427_194, 10.0),
            (123.427_194, 11.0),
            (122.427_194, 11.0),
            (122.427_194, 10.0),
        ]);
        for point in &mut translated_box {
            point.x = point
                .x
                .try_add(&ExactRational::from_f64(360.0))
                .expect("translate exact longitude by a period");
        }

        let cases = [
            (
                "ordinary counterclockwise ring",
                exact_points(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]),
                RingWinding::CounterClockwise,
            ),
            (
                "rational thirds and sevenths",
                rational_box,
                RingWinding::CounterClockwise,
            ),
            (
                "one-ulp narrow ring",
                exact_points(&[
                    (100.0, 80.0),
                    (100.0 + unit, 80.0),
                    (100.0, 80.0 + unit),
                    (100.0, 80.0),
                ]),
                RingWinding::CounterClockwise,
            ),
            (
                "seam-crossing box",
                exact_points(&[
                    (179.0, 0.0),
                    (-179.0, 0.0),
                    (-179.0, 1.0),
                    (179.0, 1.0),
                    (179.0, 0.0),
                ]),
                RingWinding::CounterClockwise,
            ),
            (
                "pole ring",
                exact_points(&[
                    (0.0, 80.0),
                    (90.0, 90.0),
                    (180.0, 80.0),
                    (-90.0, 80.0),
                    (0.0, 80.0),
                ]),
                RingWinding::Clockwise,
            ),
            (
                "low-bit longitude plus a full period",
                translated_box,
                RingWinding::CounterClockwise,
            ),
        ];
        for (name, ports, expected) in cases {
            assert_eq!(
                exact_q_ring_winding_filter(frame, &ports).map(|proof| proof.winding),
                Some(expected),
                "{name}"
            );
        }
    }

    #[test]
    fn interval_ring_filter_declines_uncertain_signs_and_exact_fallback_preserves_errors()
    -> Result<()> {
        let frame = GeographicFrame::crs84();
        let epsilon = ExactRational::from_f64(2.0_f64.powi(-54));
        let mut counterclockwise = exact_points(&[(0.0, 0.0), (1.0, 1.0), (2.0, 2.0), (0.0, 0.0)]);
        counterclockwise[2].y = ExactRational::from_f64(2.0).try_add(&epsilon)?;
        let mut clockwise = counterclockwise.clone();
        clockwise[2].y = ExactRational::from_f64(2.0).try_subtract(&epsilon)?;
        for (ports, expected) in [
            (&counterclockwise, RingWinding::CounterClockwise),
            (&clockwise, RingWinding::Clockwise),
        ] {
            assert_eq!(exact_q_ring_winding_filter(frame, ports), None);
            let lifted = continuously_lift_exact_ring(frame, ports)?;
            assert_eq!(exact_ring_winding(&lifted)?, expected);
        }

        let flat_full_turn = exact_points(&[
            (0.0, 10.0),
            (120.0, 10.0),
            (240.0, 10.0),
            (360.0, 10.0),
            (0.0, 10.0),
        ]);
        assert_eq!(exact_q_ring_winding_filter(frame, &flat_full_turn), None);

        let half_turn_tie = exact_points(&[
            (0.0, 0.0),
            (180.0, 0.0),
            (180.0, 1.0),
            (0.0, 1.0),
            (0.0, 0.0),
        ]);
        assert_eq!(exact_q_ring_winding_filter(frame, &half_turn_tie), None);

        let subnormal = f64::from_bits(1);
        let subnormal_triangle =
            exact_points(&[(0.0, 0.0), (subnormal, 0.0), (0.0, subnormal), (0.0, 0.0)]);
        assert_eq!(
            exact_q_ring_winding_filter(frame, &subnormal_triangle),
            None
        );

        let twice_max =
            ExactRational::from_f64(f64::MAX).try_product(&ExactRational::from_f64(2.0))?;
        let outside_binary64 = vec![
            ExactChartPoint::from_rationals(ExactRational::zero(), ExactRational::zero()),
            ExactChartPoint::from_rationals(twice_max.clone(), ExactRational::zero()),
            ExactChartPoint::from_rationals(twice_max, ExactRational::one()),
            ExactChartPoint::from_rationals(ExactRational::zero(), ExactRational::zero()),
        ];
        assert_eq!(exact_q_ring_winding_filter(frame, &outside_binary64), None);
        let error = exact_q_ring_role(frame, &outside_binary64)
            .expect_err("exact fallback retains binary64 conversion failure");
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactCoordinateOutOfRange)
        ));
        Ok(())
    }

    #[test]
    fn interval_ring_filter_rejects_uncertain_clockwise_cancellation() -> Result<()> {
        let frame = GeographicFrame::crs84();
        let epsilon = ExactRational::from_f64(2.0_f64.powi(-54));
        let mut clockwise = exact_points(&[(0.0, 0.0), (1.0, 1.0), (2.0, 2.0), (0.0, 0.0)]);
        clockwise[2].y = ExactRational::from_f64(2.0).try_subtract(&epsilon)?;

        assert_eq!(exact_q_ring_winding_filter(frame, &clockwise), None);
        let lifted = continuously_lift_exact_ring(frame, &clockwise)?;
        assert_eq!(exact_ring_winding(&lifted)?, RingWinding::Clockwise);
        Ok(())
    }
}

#[cfg(test)]
mod native_ray_work_tests {
    #![expect(
        clippy::panic_in_result_fn,
        reason = "ray Work controls assert observed refusal and exact predicate results"
    )]

    use std::cmp::Ordering;

    use super::{NativeRayTransform, NativeReferencePoints, NativeReferenceRayPoints, RayAxis};
    use crate::error::{ErrorKind, Result};
    use crate::geometry::embedding_search::{EvaluationBudget, EvaluationOnly};
    use crate::geometry::exact_arith::ExactSign;
    use crate::geometry::overlay::RayPointsWithWork;
    use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};
    use crate::geometry::topology::ExactChartPoint;
    use crate::geometry::{GeometryErrorKind, Unmetered, Work, XY};

    #[derive(Clone, Copy)]
    struct ChargeEvent {
        spent_before: u128,
        ticks: u128,
    }

    struct RecordingWork<'a> {
        budget: &'a mut EvaluationBudget,
        spent: u128,
        charges: Vec<ChargeEvent>,
        refused: Option<ChargeEvent>,
    }

    impl<'a> RecordingWork<'a> {
        fn new(budget: &'a mut EvaluationBudget) -> Self {
            Self {
                budget,
                spent: 0,
                charges: Vec::new(),
                refused: None,
            }
        }

        fn record(&mut self, ticks: u128, result: &Result<()>) {
            let event = ChargeEvent {
                spent_before: self.spent,
                ticks,
            };
            if result.is_ok() {
                self.charges.push(event);
                self.spent = self.spent.wrapping_add(ticks);
            } else {
                self.refused = Some(event);
            }
        }
    }

    impl Work for RecordingWork<'_> {
        type Error = crate::error::Error;

        fn charge(&mut self, ticks: u64) -> Result<()> {
            let result = <EvaluationBudget as Work>::charge(self.budget, ticks);
            self.record(u128::from(ticks), &result);
            result
        }

        fn charge_product(&mut self, count: usize, ticks_per_item: usize) -> Result<()> {
            let ticks = (count as u128).wrapping_mul(ticks_per_item as u128);
            let result =
                <EvaluationBudget as Work>::charge_product(self.budget, count, ticks_per_item);
            self.record(ticks, &result);
            result
        }
    }

    fn budget_refusing_before(
        operation: &'static str,
        event: ChargeEvent,
    ) -> Result<EvaluationBudget> {
        let remaining = event
            .spent_before
            .checked_add(event.ticks)
            .and_then(|ticks| ticks.checked_sub(1))
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("invalid native-ray charge calibration".into())
            })?;
        EvaluationBudget::with_remaining_ticks_for_test(operation, 1, remaining)
    }

    fn assert_limit_error(error: &crate::error::Error, operation: &'static str) {
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: actual,
                ..
            }) if *actual == operation
        ));
    }

    fn nudge_one(offset: f64) -> Result<ExactRational> {
        let numerator = ExactDyadic::from_f64(1.0)
            .try_add_with_work(ExactDyadic::from_f64(offset), &mut Unmetered)?;
        Ok(ExactRational::from_dyadic(numerator))
    }

    fn chart_point(x: ExactRational, y: ExactRational) -> ExactChartPoint {
        ExactChartPoint::from_rationals(x, y)
    }

    fn native_reference_points(points: Vec<ExactChartPoint>) -> Result<NativeReferencePoints> {
        NativeReferencePoints::from_exact(points, &mut Unmetered)
    }

    fn compare_axis<W: Work>(
        points: &NativeReferenceRayPoints<'_>,
        work: &mut W,
    ) -> Result<Ordering> {
        work.charge_into_error(1)?;
        RayPointsWithWork::compare_axis_with_work(points, 0, 1, RayAxis::X, work)
    }

    fn orientation<W: Work>(
        points: &NativeReferenceRayPoints<'_>,
        work: &mut W,
    ) -> Result<ExactSign> {
        work.charge_into_error(1)?;
        RayPointsWithWork::orientation_with_work(points, 0, 1, 2, work)
    }

    #[test]
    fn native_reference_ray_axis_refuses_inside_exact_comparison() -> Result<()> {
        const OPERATION: &str = "native_reference_ray_axis";
        let base = native_reference_points(vec![chart_point(
            ExactRational::from_f64(1.0),
            ExactRational::from_f64(1.0),
        )])?;
        let exact_queries = [chart_point(
            nudge_one(2.0_f64.powi(-256))?,
            ExactRational::from_f64(1.0),
        )];
        let rounded_queries = [XY::new(1.0, 1.0)];
        let points = NativeReferenceRayPoints {
            base: &base,
            exact_queries: &exact_queries,
            rounded_queries: &rounded_queries,
            transform: NativeRayTransform::Identity,
        };
        let (order, charges) = {
            let mut budget = EvaluationBudget::new(OPERATION, 1)?;
            let mut work = RecordingWork::new(&mut budget);
            let order = compare_axis(&points, &mut work)?;
            (order, work.charges)
        };
        assert_eq!(order, Ordering::Less);
        let target = charges
            .iter()
            .copied()
            .find(|event| event.spent_before > 0 && event.ticks > 0)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Native ray axis comparison did not charge Work".into())
            })?;

        let mut budget = budget_refusing_before(OPERATION, target)?;
        let error = {
            let mut work = RecordingWork::new(&mut budget);
            let error = compare_axis(&points, &mut work)
                .expect_err("native ray refuses before returning exact axis order");
            assert_eq!(
                work.refused.map(|event| event.spent_before),
                Some(target.spent_before)
            );
            error
        };
        assert_limit_error(&error, OPERATION);

        let mut unmetered = Unmetered;
        assert_eq!(
            RayPointsWithWork::compare_axis_with_work(&points, 0, 1, RayAxis::X, &mut unmetered,)?,
            order
        );
        let mut ordinary_budget = EvaluationBudget::new(OPERATION, 1)?;
        let ordinary_spend = ordinary_budget.spent_ticks_for_test();
        {
            let mut work = EvaluationOnly::new(&mut ordinary_budget);
            assert_eq!(
                RayPointsWithWork::compare_axis_with_work(&points, 0, 1, RayAxis::X, &mut work,)?,
                order
            );
        }
        assert_eq!(ordinary_budget.spent_ticks_for_test(), ordinary_spend);
        Ok(())
    }

    #[test]
    fn native_reference_ray_orientation_refuses_inside_exact_determinant() -> Result<()> {
        const OPERATION: &str = "native_reference_ray_orientation";
        let base = native_reference_points(vec![
            chart_point(ExactRational::from_f64(1.0), ExactRational::from_f64(1.0)),
            chart_point(
                nudge_one(2.0_f64.powi(-256))?,
                nudge_one(2.0_f64.powi(-256))?,
            ),
            chart_point(
                nudge_one(2.0_f64.powi(-255))?,
                nudge_one(2.0_f64.powi(-256))?,
            ),
        ])?;
        let points = NativeReferenceRayPoints {
            base: &base,
            exact_queries: &[],
            rounded_queries: &[],
            transform: NativeRayTransform::Identity,
        };
        let (sign, charges) = {
            let mut budget = EvaluationBudget::new(OPERATION, 1)?;
            let mut work = RecordingWork::new(&mut budget);
            let sign = orientation(&points, &mut work)?;
            (sign, work.charges)
        };
        assert_eq!(sign, ExactSign::Negative);
        let target = charges
            .iter()
            .copied()
            .find(|event| event.spent_before > 0 && event.ticks > 0)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Native ray orientation did not charge Work".into())
            })?;

        let mut budget = budget_refusing_before(OPERATION, target)?;
        let error = {
            let mut work = RecordingWork::new(&mut budget);
            let error = orientation(&points, &mut work)
                .expect_err("native ray refuses before returning determinant sign");
            assert_eq!(
                work.refused.map(|event| event.spent_before),
                Some(target.spent_before)
            );
            error
        };
        assert_limit_error(&error, OPERATION);

        let mut unmetered = Unmetered;
        assert_eq!(
            RayPointsWithWork::orientation_with_work(&points, 0, 1, 2, &mut unmetered)?,
            sign
        );
        let mut ordinary_budget = EvaluationBudget::new(OPERATION, 1)?;
        let ordinary_spend = ordinary_budget.spent_ticks_for_test();
        {
            let mut work = EvaluationOnly::new(&mut ordinary_budget);
            assert_eq!(
                RayPointsWithWork::orientation_with_work(&points, 0, 1, 2, &mut work)?,
                sign
            );
        }
        assert_eq!(ordinary_budget.spent_ticks_for_test(), ordinary_spend);
        Ok(())
    }
}

#[cfg(test)]
mod native_ray_transform_work_tests {
    #![expect(
        clippy::panic_in_result_fn,
        reason = "ray Work controls assert transformed refusal and exact predicate results"
    )]

    use std::cmp::Ordering;

    use super::{NativeRayTransform, NativeReferencePoints, NativeReferenceRayPoints, RayAxis};
    use crate::error::{ErrorKind, Result};
    use crate::geometry::embedding_search::{EvaluationBudget, EvaluationOnly};
    use crate::geometry::exact_arith::ExactSign;
    use crate::geometry::overlay::RayPointsWithWork;
    use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};
    use crate::geometry::topology::ExactChartPoint;
    use crate::geometry::{Unmetered, Work, XY};

    #[derive(Clone, Copy)]
    struct ChargeEvent {
        spent_before: u128,
        ticks: u128,
    }

    struct RecordingWork<'a> {
        budget: &'a mut EvaluationBudget,
        spent: u128,
        charges: Vec<ChargeEvent>,
        refused: Option<ChargeEvent>,
    }

    impl<'a> RecordingWork<'a> {
        fn new(budget: &'a mut EvaluationBudget) -> Self {
            Self {
                budget,
                spent: 0,
                charges: Vec::new(),
                refused: None,
            }
        }

        fn record(&mut self, ticks: u128, result: &Result<()>) {
            let event = ChargeEvent {
                spent_before: self.spent,
                ticks,
            };
            if result.is_ok() {
                self.charges.push(event);
                self.spent = self.spent.wrapping_add(ticks);
            } else {
                self.refused = Some(event);
            }
        }
    }

    impl Work for RecordingWork<'_> {
        type Error = crate::error::Error;

        fn charge(&mut self, ticks: u64) -> Result<()> {
            let result = <EvaluationBudget as Work>::charge(self.budget, ticks);
            self.record(u128::from(ticks), &result);
            result
        }

        fn charge_product(&mut self, count: usize, ticks_per_item: usize) -> Result<()> {
            let ticks = (count as u128).wrapping_mul(ticks_per_item as u128);
            let result =
                <EvaluationBudget as Work>::charge_product(self.budget, count, ticks_per_item);
            self.record(ticks, &result);
            result
        }
    }

    fn budget_refusing_before(operation: &'static str, event: ChargeEvent) -> EvaluationBudget {
        let remaining = event
            .spent_before
            .checked_add(event.ticks)
            .and_then(|ticks| ticks.checked_sub(1))
            .expect("the target native-ray charge is positive");
        EvaluationBudget::with_remaining_ticks_for_test(operation, 1, remaining)
            .expect("calibrated native-ray budget is representable")
    }

    fn assert_limit_error(error: &crate::error::Error, operation: &'static str) {
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(crate::geometry::GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: actual,
                ..
            }) if *actual == operation
        ));
    }

    fn nudge_one(offset: f64) -> ExactRational {
        let numerator = ExactDyadic::from_f64(1.0)
            .try_add_with_work(ExactDyadic::from_f64(offset), &mut Unmetered)
            .expect("test dyadic is representable");
        ExactRational::from_dyadic(numerator)
    }

    fn chart_point(x: ExactRational, y: ExactRational) -> ExactChartPoint {
        ExactChartPoint::from_rationals(x, y)
    }

    fn native_reference_points(points: Vec<ExactChartPoint>) -> Result<NativeReferencePoints> {
        NativeReferencePoints::from_exact(points, &mut Unmetered)
    }

    fn compare_axis<W: Work>(
        points: &NativeReferenceRayPoints<'_>,
        work: &mut W,
    ) -> Result<Ordering> {
        work.charge_into_error(1)?;
        RayPointsWithWork::compare_axis_with_work(points, 0, 1, RayAxis::X, work)
    }

    fn orientation<W: Work>(
        points: &NativeReferenceRayPoints<'_>,
        work: &mut W,
    ) -> Result<ExactSign> {
        work.charge_into_error(1)?;
        RayPointsWithWork::orientation_with_work(points, 0, 1, 2, work)
    }

    #[test]
    fn native_reference_ray_reflect_x_axis_funds_exact_comparison() -> Result<()> {
        const OPERATION: &str = "native_reference_ray_axis";
        let base = native_reference_points(vec![chart_point(
            ExactRational::from_f64(1.0),
            ExactRational::from_f64(1.0),
        )])?;
        let exact_queries = [chart_point(
            nudge_one(2.0_f64.powi(-256)),
            ExactRational::from_f64(1.0),
        )];
        let rounded_queries = [XY::new(1.0, 1.0)];
        let points = NativeReferenceRayPoints {
            base: &base,
            exact_queries: &exact_queries,
            rounded_queries: &rounded_queries,
            transform: NativeRayTransform::ReflectX,
        };
        let (order, charges) = {
            let mut budget = EvaluationBudget::new(OPERATION, 1)?;
            let mut work = RecordingWork::new(&mut budget);
            let order = compare_axis(&points, &mut work)?;
            (order, work.charges)
        };
        assert_eq!(order, Ordering::Greater);
        let target = *charges
            .iter()
            .find(|event| event.spent_before > 0 && event.ticks > 0)
            .expect("NativeReferenceRayPoints forwards exact-axis Work");

        let mut budget = budget_refusing_before(OPERATION, target);
        let error = {
            let mut work = RecordingWork::new(&mut budget);
            let error = compare_axis(&points, &mut work)
                .expect_err("native ray refuses before returning exact axis order");
            assert_eq!(
                work.refused.map(|event| event.spent_before),
                Some(target.spent_before)
            );
            error
        };
        assert_limit_error(&error, OPERATION);

        let mut unmetered = Unmetered;
        assert_eq!(
            RayPointsWithWork::compare_axis_with_work(&points, 0, 1, RayAxis::X, &mut unmetered,)?,
            order
        );
        let mut ordinary_budget = EvaluationBudget::new(OPERATION, 1)?;
        let ordinary_spend = ordinary_budget.spent_ticks_for_test();
        {
            let mut work = EvaluationOnly::new(&mut ordinary_budget);
            assert_eq!(
                RayPointsWithWork::compare_axis_with_work(&points, 0, 1, RayAxis::X, &mut work,)?,
                order
            );
        }
        assert_eq!(ordinary_budget.spent_ticks_for_test(), ordinary_spend);
        Ok(())
    }

    #[test]
    fn native_reference_ray_swap_axes_funds_exact_determinant() -> Result<()> {
        const OPERATION: &str = "native_reference_ray_orientation";
        let base = native_reference_points(vec![
            chart_point(ExactRational::from_f64(1.0), ExactRational::from_f64(1.0)),
            chart_point(nudge_one(2.0_f64.powi(-256)), nudge_one(2.0_f64.powi(-256))),
            chart_point(nudge_one(2.0_f64.powi(-255)), nudge_one(2.0_f64.powi(-256))),
        ])?;
        let points = NativeReferenceRayPoints {
            base: &base,
            exact_queries: &[],
            rounded_queries: &[],
            transform: NativeRayTransform::SwapAxes,
        };
        let (sign, charges) = {
            let mut budget = EvaluationBudget::new(OPERATION, 1)?;
            let mut work = RecordingWork::new(&mut budget);
            let sign = orientation(&points, &mut work)?;
            (sign, work.charges)
        };
        assert_eq!(sign, ExactSign::Positive);
        let target = *charges
            .iter()
            .find(|event| event.spent_before > 0 && event.ticks > 0)
            .expect("NativeReferenceRayPoints forwards determinant Work");

        let mut budget = budget_refusing_before(OPERATION, target);
        let error = {
            let mut work = RecordingWork::new(&mut budget);
            let error = orientation(&points, &mut work)
                .expect_err("native ray refuses before returning determinant sign");
            assert_eq!(
                work.refused.map(|event| event.spent_before),
                Some(target.spent_before)
            );
            error
        };
        assert_limit_error(&error, OPERATION);

        let mut unmetered = Unmetered;
        assert_eq!(
            RayPointsWithWork::orientation_with_work(&points, 0, 1, 2, &mut unmetered)?,
            sign
        );
        let mut ordinary_budget = EvaluationBudget::new(OPERATION, 1)?;
        let ordinary_spend = ordinary_budget.spent_ticks_for_test();
        {
            let mut work = EvaluationOnly::new(&mut ordinary_budget);
            assert_eq!(
                RayPointsWithWork::orientation_with_work(&points, 0, 1, 2, &mut work)?,
                sign
            );
        }
        assert_eq!(ordinary_budget.spent_ticks_for_test(), ordinary_spend);
        Ok(())
    }
}

#[cfg(test)]
mod native_face_region_tests {
    use super::*;

    #[test]
    fn distinct_selected_roots_cannot_share_one_native_face() -> Result<()> {
        let mut face_regions = vec![None; 1];
        let mut known = vec![false; 1];
        let mut work = Unmetered;
        assign_native_face_region(&mut face_regions, &mut known, 0, Some(1256), &mut work)?;

        if assign_native_face_region(&mut face_regions, &mut known, 0, Some(1251), &mut work)
            .is_ok()
        {
            return Err(GeometryErrorKind::Invalid(
                "one Native face accepted two distinct selected-region roots".into(),
            )
            .into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod native_ordinate_provenance_tests {
    #![expect(
        clippy::panic_in_result_fn,
        reason = "assertions are exact request-row and refinement provenance checks"
    )]

    use super::*;

    #[test]
    fn common_refinement_merges_source_requests_at_one_split_parameter() -> Result<()> {
        let mut work = Unmetered;
        let left_request = RootRequest::new_with_work(
            RootId::from_index(2),
            ExactRational::from_f64(0.25),
            &mut work,
        )?;
        let right_request = RootRequest::new_with_work(
            RootId::from_index(7),
            ExactRational::from_f64(0.75),
            &mut work,
        )?;
        let point = ExactChartPoint::from_rationals(
            ExactRational::from_f64(-150.0),
            ExactRational::from_f64(-80.0),
        );
        let source_port_data =
            |request: &RootRequest, work: &mut Unmetered| -> Result<NativePortData> {
                Ok(NativePortData {
                    node: EmissionNodeId(2),
                    point: point.clone(),
                    spelling: PortSpelling::Interior { periods: 0 },
                    source_record: None,
                    occurrences: native_source_sample_occurrences(
                        None,
                        std::slice::from_ref(request),
                        work,
                    )?,
                })
            };
        let event =
            |request: &RootRequest, work: &mut Unmetered| -> Result<NativeKnownSpanBoundary> {
                Ok(NativeKnownSpanBoundary {
                    span: ChartSpanId(0),
                    parameter: ExactRational::zero(),
                    port: QPortId(0),
                    data: source_port_data(request, work)?,
                })
            };
        let producer_port = |node, longitude| NativePortData {
            node: EmissionNodeId(node),
            point: ExactChartPoint::from_rationals(
                ExactRational::from_f64(longitude),
                ExactRational::from_f64(-80.0),
            ),
            spelling: PortSpelling::Interior { periods: 0 },
            source_record: None,
            occurrences: Vec::new(),
        };
        let source = NativeSourceSpanUse {
            span: ChartSpanId(0),
            owner: 0,
            source_edge: SourceEdgeId(0),
            source_lift: -1,
            source_range: ExactSourceRange {
                start: ExactRational::from_f64(0.5),
                end: ExactRational::zero(),
            },
            producer_range: ExactSourceRange {
                start: ExactRational::from_f64(0.5),
                end: ExactRational::one(),
            },
            span_range: ExactSourceRange {
                start: ExactRational::zero(),
                end: ExactRational::one(),
            },
            nodes: [QNodeId(0), QNodeId(1)],
            ports: [QPortId(0), QPortId(1)],
            face_germs: None,
        };
        let edge = NativeAreaEdge {
            vertices: [producer_port(0, -180.0), producer_port(1, -120.0)],
            owner: NativeGraphEdgeOwner::Area { region: 0 },
            kind: NativeAreaEdgeKind::Producer {
                recipe: FragmentRecipeId(4),
                trace_edge: 0,
                producer_start: Box::new(ExactRational::zero()),
                producer_end: ExactRational::one(),
                trace_ranges: Vec::new(),
                known_spans: vec![source.clone(), source],
            },
        };
        let boundaries = [
            event(&left_request, &mut work)?,
            event(&right_request, &mut work)?,
        ];

        let refined = common_refine_native_edges(
            vec![edge],
            &boundaries,
            GeographicFrame::crs84(),
            &mut work,
        )?;

        let split = refined
            .first()
            .and_then(|edge| edge.vertices.get(1))
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("common-refinement source split is missing".into())
            })?;
        assert!(
            split.occurrences.len() >= 2,
            "both directed source-root requests must survive coincident refinement events"
        );
        assert!(split.occurrences.iter().any(|occurrence| matches!(
            &occurrence.source_trace,
            Some(TraceRequestLocation::Root { root, parameter })
                if *root == RootId::from_index(2)
                    && *parameter == ExactRational::from_f64(0.25)
        )));
        assert!(split.occurrences.iter().any(|occurrence| matches!(
            &occurrence.source_trace,
            Some(TraceRequestLocation::Root { root, parameter })
                if *root == RootId::from_index(7)
                    && *parameter == ExactRational::from_f64(0.75)
        )));
        Ok(())
    }

    #[test]
    fn coincident_known_span_boundaries_merge_distinct_source_requests() -> Result<()> {
        let mut work = Unmetered;
        let left_request = RootRequest::new_with_work(
            RootId::from_index(2),
            ExactRational::from_f64(0.25),
            &mut work,
        )?;
        let right_request = RootRequest::new_with_work(
            RootId::from_index(7),
            ExactRational::from_f64(0.75),
            &mut work,
        )?;
        let point = ExactChartPoint::from_rationals(
            ExactRational::from_f64(10.0),
            ExactRational::from_f64(20.0),
        );
        let port_data =
            |requests: &[RootRequest], work: &mut Unmetered| -> Result<NativePortData> {
                Ok(NativePortData {
                    node: EmissionNodeId(3),
                    point: point.clone(),
                    spelling: PortSpelling::Interior { periods: 0 },
                    source_record: None,
                    occurrences: native_source_sample_occurrences(None, requests, work)?,
                })
            };
        let mut unique = Vec::new();
        for request in [&left_request, &right_request] {
            let boundary = NativeKnownSpanBoundary {
                span: ChartSpanId(0),
                parameter: ExactRational::zero(),
                port: QPortId(5),
                data: port_data(std::slice::from_ref(request), &mut work)?,
            };
            append_native_known_span_boundary(
                &mut unique,
                boundary,
                GeographicFrame::crs84(),
                &mut work,
            )?;
        }

        assert_eq!(unique.len(), 1);
        let boundary = unique
            .first()
            .ok_or_else(|| GeometryErrorKind::Invalid("merged known boundary is missing".into()))?;
        assert_eq!(boundary.data.occurrences.len(), 2);
        let first = boundary
            .data
            .occurrences
            .first()
            .and_then(|occurrence| occurrence.source_trace.as_ref())
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("first known-boundary root request is missing".into())
            })?;
        let second = boundary
            .data
            .occurrences
            .get(1)
            .and_then(|occurrence| occurrence.source_trace.as_ref())
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("second known-boundary root request is missing".into())
            })?;
        let TraceRequestLocation::Root {
            root: first_root,
            parameter: first_parameter,
        } = first
        else {
            return Err(GeometryErrorKind::Invalid(
                "first known-boundary occurrence lost its direct root".into(),
            )
            .into());
        };
        let TraceRequestLocation::Root {
            root: second_root,
            parameter: second_parameter,
        } = second
        else {
            return Err(GeometryErrorKind::Invalid(
                "second known-boundary occurrence lost its direct root".into(),
            )
            .into());
        };
        assert_eq!(*first_root, RootId::from_index(2));
        assert_eq!(first_parameter, &ExactRational::from_f64(0.25));
        assert_eq!(*second_root, RootId::from_index(7));
        assert_eq!(second_parameter, &ExactRational::from_f64(0.75));
        Ok(())
    }

    #[test]
    fn known_cut_boundary_keeps_the_directed_source_root_parameter() -> Result<()> {
        let source = NativeSourceSpanUse {
            span: ChartSpanId(0),
            owner: 0,
            source_edge: SourceEdgeId(7),
            source_lift: -1,
            source_range: ExactSourceRange {
                start: ExactRational::from_f64(0.75),
                end: ExactRational::from_f64(0.25),
            },
            producer_range: ExactSourceRange {
                start: ExactRational::zero(),
                end: ExactRational::one(),
            },
            span_range: ExactSourceRange {
                start: ExactRational::one(),
                end: ExactRational::zero(),
            },
            nodes: [QNodeId(3), QNodeId(4)],
            ports: [QPortId(5), QPortId(6)],
            face_germs: None,
        };
        let mut work = Unmetered;

        let at_span_start =
            native_source_request_at_known_boundary(&source, &source.span_range.start, &mut work)?;
        let at_span_end =
            native_source_request_at_known_boundary(&source, &source.span_range.end, &mut work)?;

        assert_eq!(at_span_start.root(), RootId::from_index(7));
        assert_eq!(at_span_start.parameter(), &source.source_range.start);
        assert_eq!(at_span_end.root(), RootId::from_index(7));
        assert_eq!(at_span_end.parameter(), &source.source_range.end);
        Ok(())
    }
}

#[cfg(test)]
mod native_cycle_start_tests {
    #![expect(
        clippy::panic_in_result_fn,
        reason = "cycle-start controls assert indexed behavior against the exhaustive oracle"
    )]

    use super::*;

    #[derive(Clone, Copy)]
    struct StartSpec {
        node: EmissionNodeId,
        spelling: NativeChartSpellingKey,
        reversed: bool,
    }

    impl StartSpec {
        fn forward(node: EmissionNodeId, spelling: NativeChartSpellingKey) -> Self {
            Self {
                node,
                spelling,
                reversed: false,
            }
        }
    }

    struct CycleSpec {
        region: usize,
        shell: bool,
        starts: Vec<StartSpec>,
    }

    struct CountingWork(u64);

    impl Work for CountingWork {
        type Error = crate::error::Error;

        fn charge(&mut self, ticks: u64) -> Result<()> {
            self.0 = self
                .0
                .checked_add(ticks)
                .ok_or(ResourceError::SizeOverflow)?;
            Ok(())
        }

        fn charge_product(&mut self, count: usize, ticks_per_item: usize) -> Result<()> {
            let ticks = count
                .checked_mul(ticks_per_item)
                .ok_or(ResourceError::SizeOverflow)?;
            self.charge(u64::try_from(ticks).map_err(|_| ResourceError::SizeOverflow)?)
        }
    }

    struct SeededInputs(u64);

    impl SeededInputs {
        fn next(&mut self) -> u32 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (self.0 >> 32) as u32
        }

        fn spelling(&mut self) -> NativeChartSpellingKey {
            match self.next() & 7 {
                0 => NativeChartSpellingKey::Interior(-1),
                1 => NativeChartSpellingKey::Interior(0),
                2 => NativeChartSpellingKey::Interior(1),
                3 => NativeChartSpellingKey::SeamEast,
                4 => NativeChartSpellingKey::SeamWest,
                _ => NativeChartSpellingKey::Pole(u64::from(self.next())),
            }
        }
    }

    fn synthetic_graph(
        specifications: Vec<CycleSpec>,
    ) -> (
        Vec<NativeGraphCycle>,
        Vec<[u32; 2]>,
        Vec<NativeChartVertexKey>,
    ) {
        let mut cycles = Vec::with_capacity(specifications.len());
        let mut edge_nodes = Vec::new();
        let mut vertex_keys = Vec::new();
        for specification in specifications {
            let mut edges = Vec::with_capacity(specification.starts.len());
            for start in specification.starts {
                let vertex = u32::try_from(vertex_keys.len()).expect("small test graph vertex");
                let other_vertex = vertex.checked_add(1).expect("small test graph endpoint");
                let atom = u32::try_from(edge_nodes.len()).expect("small test graph edge");
                vertex_keys.push(NativeChartVertexKey {
                    node: start.node,
                    spelling: start.spelling,
                });
                vertex_keys.push(NativeChartVertexKey {
                    node: EmissionNodeId(u32::MAX),
                    spelling: NativeChartSpellingKey::Pole(u64::MAX),
                });
                edge_nodes.push(if start.reversed {
                    [other_vertex, vertex]
                } else {
                    [vertex, other_vertex]
                });
                edges.push(NativeGraphCycleEdge {
                    atom,
                    reversed: start.reversed,
                    contributors: Vec::new(),
                });
            }
            cycles.push(NativeGraphCycle {
                face: 0,
                region: specification.region,
                edges,
                shell: specification.shell,
            });
        }
        (cycles, edge_nodes, vertex_keys)
    }

    fn query(
        region: usize,
        shell: bool,
        node: u32,
        spelling: Option<NativeChartSpellingKey>,
    ) -> NativeCycleStartQuery {
        NativeCycleStartQuery {
            region,
            shell,
            node: EmissionNodeId(node),
            spelling,
        }
    }

    fn exhaustive_rotations_for_queries(
        queries: &[NativeCycleStartQuery],
        cycles: &[NativeGraphCycle],
        edge_nodes: &[[u32; 2]],
        vertex_keys: &[NativeChartVertexKey],
        budget: &mut impl Work,
    ) -> Result<Vec<Option<usize>>> {
        let mut rotations = vec![None; cycles.len()];
        for &query in queries {
            let mut matching_cycle = None;
            let mut ambiguous = false;
            for (cycle_index, cycle) in cycles.iter().enumerate() {
                budget.charge_into_error(1)?;
                if cycle.region != query.region || cycle.shell != query.shell {
                    continue;
                }
                let Some(start) = exhaustive_native_cycle_start_vertex_index(
                    edge_nodes,
                    vertex_keys,
                    cycle,
                    query.node,
                    query.spelling,
                    budget,
                )?
                else {
                    continue;
                };
                if matching_cycle
                    .replace(NativeCycleStartCandidate { cycle_index, start })
                    .is_some()
                {
                    ambiguous = true;
                    break;
                }
            }
            if ambiguous {
                continue;
            }
            if let Some(candidate) = matching_cycle {
                let slot = rotations.get_mut(candidate.cycle_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Native cycle rotation row is unknown".into())
                })?;
                if slot.is_some_and(|previous| previous != candidate.start) {
                    return Err(GeometryErrorKind::Invalid(
                        "Native Q rings request conflicting starts on one graph cycle".into(),
                    )
                    .into());
                }
                *slot = Some(candidate.start);
            }
        }
        Ok(rotations)
    }

    fn assert_same_differential_result(
        indexed: &Result<Vec<Option<usize>>>,
        exhaustive: &Result<Vec<Option<usize>>>,
        context: &str,
    ) {
        match (indexed, exhaustive) {
            (Ok(indexed), Ok(exhaustive)) => assert_eq!(indexed, exhaustive, "{context}"),
            (Err(indexed), Err(exhaustive)) => {
                assert!(
                    matches!(
                        indexed.kind(),
                        crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
                    ),
                    "{context}: indexed result has the wrong error kind"
                );
                assert!(
                    matches!(
                        exhaustive.kind(),
                        crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
                    ),
                    "{context}: exhaustive result has the wrong error kind"
                );
            },
            (indexed, exhaustive) => panic!(
                "{context}: indexed/exhaustive error disagreement: indexed={indexed:?}, exhaustive={exhaustive:?}"
            ),
        }
    }

    fn differential_rotations(
        queries: &[NativeCycleStartQuery],
        cycles: &[NativeGraphCycle],
        edge_nodes: &[[u32; 2]],
        vertex_keys: &[NativeChartVertexKey],
        context: &str,
    ) -> Result<Vec<Option<usize>>> {
        let indexed = native_cycle_rotations_for_queries(
            queries,
            cycles,
            edge_nodes,
            vertex_keys,
            &mut Unmetered,
        );
        let exhaustive = exhaustive_rotations_for_queries(
            queries,
            cycles,
            edge_nodes,
            vertex_keys,
            &mut Unmetered,
        );
        assert_same_differential_result(&indexed, &exhaustive, context);
        indexed
    }

    #[test]
    fn native_cycle_start_index_matches_exhaustive_scan_on_seeded_small_graphs() {
        let mut inputs = SeededInputs(0x6E61_7469_7665_6379);
        for case in 0..96 {
            let cycle_count = inputs.next() & 7;
            let mut specifications = Vec::new();
            for _ in 0..cycle_count {
                let region = usize::try_from(inputs.next() & 3).expect("small test region");
                let shell = inputs.next() & 1 == 0;
                let start_count = inputs.next() & 7;
                let mut starts = Vec::new();
                for _ in 0..start_count {
                    starts.push(StartSpec {
                        node: EmissionNodeId(inputs.next() & 7),
                        spelling: inputs.spelling(),
                        reversed: inputs.next() & 1 != 0,
                    });
                }
                specifications.push(CycleSpec {
                    region,
                    shell,
                    starts,
                });
            }
            let query_count = (inputs.next() & 7).max(1);
            let mut queries = Vec::new();
            for _ in 0..query_count {
                let spelling = if inputs.next().trailing_zeros() >= 2 {
                    None
                } else {
                    Some(inputs.spelling())
                };
                queries.push(query(
                    usize::try_from(inputs.next() & 3).expect("small test region"),
                    inputs.next() & 1 == 0,
                    inputs.next() & 7,
                    spelling,
                ));
            }
            let (cycles, edge_nodes, vertex_keys) = synthetic_graph(specifications);

            let indexed = native_cycle_rotations_for_queries(
                &queries,
                &cycles,
                &edge_nodes,
                &vertex_keys,
                &mut Unmetered,
            );
            let exhaustive = exhaustive_rotations_for_queries(
                &queries,
                &cycles,
                &edge_nodes,
                &vertex_keys,
                &mut Unmetered,
            );
            assert_same_differential_result(
                &indexed,
                &exhaustive,
                &format!("seeded graph case {case}"),
            );
        }
    }

    #[test]
    fn native_cycle_start_lookup_preserves_first_match_fallback_and_ambiguity() -> Result<()> {
        let interior_zero = NativeChartSpellingKey::Interior(0);
        let interior_one = NativeChartSpellingKey::Interior(1);
        let interior_two = NativeChartSpellingKey::Interior(2);
        let (cycles, edge_nodes, vertex_keys) = synthetic_graph(vec![
            CycleSpec {
                region: 2,
                shell: true,
                starts: vec![
                    StartSpec::forward(EmissionNodeId(5), interior_zero),
                    StartSpec::forward(EmissionNodeId(5), interior_zero),
                    StartSpec::forward(EmissionNodeId(2), interior_one),
                    StartSpec::forward(EmissionNodeId(3), interior_one),
                    StartSpec::forward(EmissionNodeId(3), interior_two),
                ],
            },
            CycleSpec {
                region: 2,
                shell: true,
                starts: vec![StartSpec::forward(EmissionNodeId(5), interior_zero)],
            },
        ]);

        let first_exact = differential_rotations(
            &[query(2, true, 5, Some(interior_zero))],
            &cycles[..1],
            &edge_nodes[..5],
            &vertex_keys,
            "repeated exact spelling in one cycle",
        )?;
        assert_eq!(first_exact, [Some(0)]);

        let unique_node_fallback = differential_rotations(
            &[query(2, true, 2, Some(interior_zero))],
            &cycles[..1],
            &edge_nodes[..5],
            &vertex_keys,
            "unique-node fallback",
        )?;
        assert_eq!(unique_node_fallback, [Some(2)]);

        let repeated_node_without_exact_spelling = differential_rotations(
            &[query(2, true, 3, Some(interior_zero))],
            &cycles[..1],
            &edge_nodes[..5],
            &vertex_keys,
            "repeated node without exact spelling",
        )?;
        assert_eq!(repeated_node_without_exact_spelling, [None]);

        let exact_spelling_precedes_nonmatching_occurrences = differential_rotations(
            &[query(2, true, 3, Some(interior_one))],
            &cycles[..1],
            &edge_nodes[..5],
            &vertex_keys,
            "exact spelling beats other node occurrences",
        )?;
        assert_eq!(exact_spelling_precedes_nonmatching_occurrences, [Some(3)]);

        let unspecified_spelling_chooses_the_first_node_start = differential_rotations(
            &[query(2, true, 3, None)],
            &cycles[..1],
            &edge_nodes[..5],
            &vertex_keys,
            "unspecified spelling chooses first node start",
        )?;
        assert_eq!(unspecified_spelling_chooses_the_first_node_start, [Some(3)]);

        let cross_cycle_ambiguity = differential_rotations(
            &[query(2, true, 5, Some(interior_zero))],
            &cycles,
            &edge_nodes,
            &vertex_keys,
            "multiple matching cycles are ambiguous",
        )?;
        assert_eq!(cross_cycle_ambiguity, [None, None]);

        let (fallback_ambiguity_cycles, fallback_edge_nodes, fallback_vertex_keys) =
            synthetic_graph(vec![
                CycleSpec {
                    region: 3,
                    shell: true,
                    starts: vec![StartSpec::forward(EmissionNodeId(7), interior_zero)],
                },
                CycleSpec {
                    region: 3,
                    shell: true,
                    starts: vec![StartSpec::forward(EmissionNodeId(7), interior_one)],
                },
            ]);
        let exact_and_unique_fallback_are_ambiguous = differential_rotations(
            &[query(3, true, 7, Some(interior_zero))],
            &fallback_ambiguity_cycles,
            &fallback_edge_nodes,
            &fallback_vertex_keys,
            "exact match plus another cycle's unique fallback is ambiguous",
        )?;
        assert_eq!(exact_and_unique_fallback_are_ambiguous, [None, None]);
        Ok(())
    }

    #[test]
    fn native_cycle_start_conflicting_requests_keep_the_existing_error() {
        let spelling = NativeChartSpellingKey::Interior(0);
        let (cycles, edge_nodes, vertex_keys) = synthetic_graph(vec![CycleSpec {
            region: 4,
            shell: true,
            starts: vec![
                StartSpec::forward(EmissionNodeId(1), spelling),
                StartSpec::forward(EmissionNodeId(2), spelling),
            ],
        }]);
        let queries = [
            query(4, true, 1, Some(spelling)),
            query(4, true, 2, Some(spelling)),
        ];
        let indexed = native_cycle_rotations_for_queries(
            &queries,
            &cycles,
            &edge_nodes,
            &vertex_keys,
            &mut Unmetered,
        );
        let exhaustive = exhaustive_rotations_for_queries(
            &queries,
            &cycles,
            &edge_nodes,
            &vertex_keys,
            &mut Unmetered,
        );
        assert_same_differential_result(&indexed, &exhaustive, "conflicting ring starts");
        let error = indexed.expect_err("conflicting starts on one cycle must remain an error");
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
        ));
    }

    #[test]
    fn native_cycle_start_work_scales_with_rings_and_cycles() -> Result<()> {
        fn measured_ticks(ring_count: u32, cycle_count: u32) -> Result<u64> {
            let mut specifications = Vec::new();
            for _ in 0..cycle_count {
                specifications.push(CycleSpec {
                    region: 0,
                    shell: true,
                    starts: vec![StartSpec::forward(
                        EmissionNodeId(0),
                        NativeChartSpellingKey::Interior(0),
                    )],
                });
            }
            let (cycles, edge_nodes, vertex_keys) = synthetic_graph(specifications);
            let queries = (0..ring_count)
                .map(|ring| {
                    query(
                        0,
                        true,
                        ring + 100,
                        Some(NativeChartSpellingKey::Interior(0)),
                    )
                })
                .collect::<Vec<_>>();
            let mut work = CountingWork(0);
            let rotations = native_cycle_rotations_for_queries(
                &queries,
                &cycles,
                &edge_nodes,
                &vertex_keys,
                &mut work,
            )?;
            assert!(rotations.iter().all(Option::is_none));
            Ok(work.0)
        }

        let measured = [(8, 8), (16, 16), (32, 32)]
            .map(|(rings, cycles)| measured_ticks(rings, cycles))
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        for (&ticks, (rings, cycles)) in measured.iter().zip([(8_u64, 8_u64), (16, 16), (32, 32)]) {
            assert!(
                ticks <= 5 * (rings + cycles),
                "cycle-start work must stay linear in K + M: K={rings}, M={cycles}, ticks={ticks}"
            );
        }
        assert!(
            measured[1] <= measured[0] * 3 && measured[2] <= measured[1] * 3,
            "doubling K and M must not approach the fourfold growth of a K×M scan: {measured:?}"
        );
        Ok(())
    }
}
