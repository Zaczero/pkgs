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
use super::emission::ring_roles::*;

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

#[cfg(test)]
mod canonical_coordinate_tests {
    include!("q_embedding/native/tests/support.rs");
    include!("q_embedding/native/refinement/tests.rs");
    include!("q_embedding/emission/source_reuse/tests.rs");
    include!("q_embedding/native/linework/tests.rs");
    include!("q_embedding/source_binding/tests.rs");
    include!("q_embedding/emission/sites/tests.rs");
}

#[cfg(test)]
mod ring_filter_tests {
    include!("emission/ring_roles/tests.rs");
}

#[cfg(test)]
mod native_ray_work_tests {
    include!("q_embedding/native/reference_builder/tests/work.rs");
}

#[cfg(test)]
mod native_ray_transform_work_tests {
    include!("q_embedding/native/reference_builder/tests/transforms.rs");
}

#[cfg(test)]
mod native_face_region_tests {
    include!("q_embedding/native/seam_bands/tests.rs");
}

#[cfg(test)]
mod native_ordinate_provenance_tests {
    include!("q_embedding/native/refinement/tests/provenance.rs");
}

#[cfg(test)]
mod native_cycle_start_tests {
    include!("q_embedding/native/cycles/tests.rs");
}

use self::dependencies::chords::*;
use self::dependencies::classes::*;
use self::dependencies::proximity::*;
use self::publication::chords::*;
use self::publication::fans::*;
use self::publication::model::*;
use self::publication::tracing::*;
use self::publication::witness::*;
