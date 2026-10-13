//! Exact n-ary geographic set selection and output emission over global Q.

use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use self::plan_boundary::*;
use self::plan_linework::*;
use self::plan_overlay::*;
use self::recipes::*;
use self::select_boundaries::*;
use self::select_faces::*;
use self::select_lines::*;
use super::emission::draw::*;
use super::emission::line_paths::*;
use super::emission::ordinates::{
    node_ordinates, source_literal_image, with_assigned_source_vertex_axes, with_publication_axes,
};
use super::emission::plan::*;
#[cfg(test)]
use super::emission::sites::canonicalize_ring_vertices;
use super::emission::sites::{
    EmittedNodeKey, EmittedVertex, QCoordinates, emitted_node_key, for_each_q_cycle_vertex,
    node_injectivity_collisions_with_work, source_ring_emitted_vertices,
};
#[cfg(test)]
use super::emission::sites::{certify_node_injectivity, node_injectivity_collisions};
use super::emission::source_publication::SourcePublication;
#[cfg(test)]
use super::emission::source_reuse::cycle_belongs_to_source_ring;
use super::emission::source_reuse::{
    source_cycle_start_port, source_cycles_by_ring, source_edge_at,
    source_ring_cycles_cover_boundary, source_ring_cycles_follow_source,
};
use super::emission::write::*;
use super::emission::{
    DirectedSpan, FeatureFamilyRegistry, FragmentRecipes, LiteralPoint, LiteralPolygon,
    LiteralSequence, NativeMaterializedParts, PreparedNativeSource, QSourceImage, RecipeWalkIndex,
    RecipeWalkOwner, SealedQDrawing, SealedSourceDrawing, SelectedUse, SourceFrozenDrawing,
    index_recipe_walks, next_recipe_walk, write_completed_native,
};
#[cfg(test)]
use super::emission::{EmissionNodeRegistry, PublicationInventory, RecipeWalk};
use super::exact::{
    ExactChartPoint, ExactSourceRange, ExactSourceRing, SourceEdgeId, SourceEdgeKind, SourceRowId,
};
#[cfg(test)]
use super::exact::{ExactSourceEdge, SourceRingId};
use super::linework::{
    DiagnosticSpan, even_odd_face_selection, face_cycle_hole_roles, minimal_face_boundaries,
    minimal_face_selection, noded_spans,
};
use super::q::{ExactGeographicTopology, GlobalFaceId, QDartId, QNodeId, QPortId, row_locations};
#[cfg(test)]
use super::q_embedding::{NearestQCertificate, certify_q_embedding_nearest};
use super::q_embedding::{
    QEmbeddingCertification, certify_and_search_q_embedding, certify_nearest_ring_roles,
};
use crate::error::{ResourceError, Result};
use crate::geographic::{ChartPole, SeamIdentity};
#[cfg(test)]
use crate::geometry::embedding_completion::LiteralCheck;
use crate::geometry::embedding_completion::{FrozenLiteralBuffers, ShapeLiteralLayout};
use crate::geometry::embedding_search::{
    EvaluationBudget, EvaluationOnly, SearchAccount, SearchOutcome,
};
use crate::geometry::ordinates::{
    NodeOrdinates, RequestRowInput, RootId, RootRequest, SourceArcMetricAccess,
    resolve_request_rows,
};
use crate::geometry::overlay::{
    CertifiedOverlayOrder, OverlayOp, build_certified_overlay_shape_with_axes,
    build_certified_overlay_shape_with_axes_and_order,
};
use crate::geometry::publication::{PendingPublicationRow, finish_geographic};
use crate::geometry::segments::ordering::try_sort_by_with_work;
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};
use crate::geometry::{
    CoordSeq, CoordSeqBuilder, CoordinateAxes, ExpansionBudget, GeometryErrorKind, GeometryKind,
    HasM, HasZ, LineSeq, Loc, Point, Polygon, Ring, Shape, Work,
};
#[cfg(test)]
use crate::geometry::{MOrdinate, XY, ZOrdinate};

mod publish;
use self::publish::publish_q_overlay;
#[cfg(test)]
use self::publish::{
    CertifiedQEmission, QEmbeddingChoice, resolve_q_embedding, write_certified_q_emission,
};
mod plan_boundary;
mod plan_linework;
mod plan_overlay;
mod recipes;
pub(in crate::geometry::topology) mod select_boundaries;
pub(in crate::geometry::topology) mod select_faces;
pub(in crate::geometry::topology) mod select_lines;
pub(in crate::geometry::topology) use self::select_boundaries::coverage_area_boundaries;
pub(in crate::geometry::topology) use self::select_faces::{
    coverage_face_regions, selected_face_roots, selected_region_facts,
};

/// Select all dimensions from the one exact source chart and its global Q.
/// Rounding happens only after face, span, and node selection is complete.
pub(in crate::geometry) fn exact_geographic_overlay<M: crate::geometry::ChartMetric>(
    topology: &ExactGeographicTopology,
    operation: OverlayOp,
    declared_axes: CoordinateAxes,
    strict: bool,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<Shape> {
    publish::exact_geographic_overlay(topology, operation, declared_axes, strict, metric_access)
}

pub(in crate::geometry) fn exact_geographic_overlay_pending_row<M: crate::geometry::ChartMetric>(
    topology: &ExactGeographicTopology,
    operation: OverlayOp,
    declared_axes: CoordinateAxes,
    strict: bool,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    logical_row: usize,
) -> Result<PendingPublicationRow> {
    publish::exact_geographic_overlay_pending_row(
        topology,
        operation,
        declared_axes,
        strict,
        metric_access,
        logical_row,
    )
}

pub(in crate::geometry) fn prepare_native_source_lines<M: crate::geometry::ChartMetric>(
    topology: &ExactGeographicTopology,
    line_members: &[usize],
    declared_axes: CoordinateAxes,
    strict: bool,
    operation: &'static str,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    expansion_budget: &mut ExpansionBudget,
    budget: &mut EvaluationBudget,
) -> Result<PreparedNativeSource> {
    let plan = QEmissionPlan::build_source_lines(topology, line_members, budget)?;
    if plan.line_paths.is_empty() {
        return Ok(PreparedNativeSource {
            shape: Shape::typed_empty(GeometryKind::MultiLineString, declared_axes),
            line_traces: Vec::new(),
            source_members: Vec::new(),
            family_state: None,
        });
    }
    let mut source_members = Vec::with_capacity(plan.line_paths.len());
    for line in &plan.line_paths {
        let RecipeWalkOwner::Line(member) = line.recipe_walk else {
            return Err(GeometryErrorKind::Invalid(
                "native source-line plan contains a non-line producer".into(),
            )
            .into());
        };
        source_members.push(member);
    }
    let nearest = QCoordinates::build_with_work(topology, budget)?;
    let source_points = source_point_coordinates_with_work(topology, budget)?;
    let embedding = resolve_q_embedding(
        topology,
        &plan,
        &source_points,
        &nearest,
        declared_axes,
        strict,
        operation,
        metric_access,
        expansion_budget,
        budget,
    )?;
    let written = write_certified_q_emission(embedding)?;
    let line_count = match &written.shape {
        Shape::LineString(_) => 1,
        Shape::MultiLineString(lines) => lines.len(),
        Shape::Empty(GeometryKind::LineString | GeometryKind::MultiLineString, _) => 0,
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => {
            return Err(GeometryErrorKind::Invalid(
                "native line preparation emitted a non-lineal shape".into(),
            )
            .into());
        },
    };
    if line_count != written.line_traces.len() {
        return Err(GeometryErrorKind::Invalid(
            "native line preparation lost its written trace rows".into(),
        )
        .into());
    }
    Ok(PreparedNativeSource {
        shape: written.shape,
        line_traces: written.line_traces,
        source_members,
        family_state: written.family_state,
    })
}

pub(in crate::geometry) fn exact_geographic_boundary<M: crate::geometry::ChartMetric>(
    topology: &ExactGeographicTopology,
    declared_axes: CoordinateAxes,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<Shape> {
    if topology.sources.row_count != 1 {
        return Err(GeometryErrorKind::Invalid(
            "an exact geographic boundary requires one source row".into(),
        )
        .into());
    }
    let row = SourceRowId(0);
    let mut budget = EvaluationBudget::new("geographic boundary", 0)?;
    let embedding = {
        let mut preparation_work = EvaluationOnly::new(&mut budget);
        let mut expansion_budget = ExpansionBudget::new("geographic boundary", "computed edges");
        let (plan, _support) = QEmissionPlan::build_boundary(topology, &mut preparation_work)?;
        let source_points = boundary_source_points_with_work(topology, row, &mut preparation_work)?;
        let nearest = QCoordinates::build_with_work(topology, &mut preparation_work)?;
        resolve_q_embedding(
            topology,
            &plan,
            &source_points,
            &nearest,
            declared_axes,
            false,
            "geographic boundary",
            metric_access,
            &mut expansion_budget,
            &mut preparation_work,
        )?
    };
    publish_q_overlay(embedding)
}

const fn selects_support(
    count: usize,
    first: bool,
    row_count: usize,
    operation: OverlayOp,
) -> bool {
    match operation {
        OverlayOp::Union => count != 0,
        OverlayOp::Intersection => count == row_count,
        OverlayOp::Difference => first && count == 1,
        OverlayOp::SymmetricDifference => !count.is_multiple_of(2),
    }
}

#[cfg(test)]
mod tests {
    include!("overlay/tests/support.rs");
    include!("overlay/plan_overlay/tests.rs");
    include!("overlay/recipes/tests.rs");
    include!("overlay/plan_linework/tests.rs");
    include!("emission/source_reuse/tests.rs");
    include!("overlay/q_embedding_tests.rs");
    include!("overlay/tests/ordinates.rs");
    include!("overlay/tests/source_publication.rs");
    include!("overlay/select_faces/tests.rs");
    include!("overlay/tests/ring_roles.rs");
    include!("emission/sites/tests.rs");
    include!("overlay/select_boundaries/tests.rs");
}
