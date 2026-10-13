use std::sync::Arc;

use crate::crs::TransformMetricScratch;
use crate::error::Result;
use crate::geographic::retained_exact::transform::select_transformed_chart_displacement;
use crate::geographic::{
    ComputedEdgeIntent, ComputedLongitudeRelation, ComputedVertexIntent, GeographicFrame,
    RetainedChartDisplacement, publish_computed_chain, publish_computed_chain_with_vertex_intents,
    publish_computed_chain_with_vertex_intents_and_samples,
    publish_computed_chain_with_vertex_intents_and_samples_with_edge_ranges,
};
use crate::geometry::tessellation::{
    ConstrainedMeshPlan, VoronoiDiagramReceipt, VoronoiEdgeColumn, VoronoiEdgeOrigin,
    VoronoiPolygonOutput, VoronoiPolygonOutputs, publish_constrained_plan,
};
use crate::geometry::{
    ChartMetric, CoordSeq, CoordSeqBuilder, CoordWindow, Coordinates as _, CsrOffsetBuilder,
    CsrOffsetColumn, ExpansionBudget, GeometryErrorKind, LineSeq, Polygon, Ring, RootMetricState,
    Shape, ShapeCountQuery, SourceArcContext, computed_native_sample_ordinates,
};
use crate::work::Unmetered;

pub(crate) type PolygonPartEdgeRoles = Vec<Vec<Vec<bool>>>;

mod chains;
mod polygons;
mod raw;
mod tessellation;
mod transformed;

pub(crate) use chains::{map_computed_shape_chains, publish_native_computed_chain};
pub(super) use polygons::publish_polygon_with_edge_roles;
pub(crate) use polygons::{
    publish_polygon_parts_with_edge_roles, publish_polygon_with_edge_roles_and_context,
};
pub(crate) use raw::{
    publish_changed_raw_computed_shape, publish_computed_point_or_raw_shape,
    publish_raw_computed_shape, publish_raw_computed_shape_with_budget,
    publish_raw_computed_shape_with_budget_and_context, publish_raw_computed_shape_with_capture,
    publish_raw_computed_shape_with_context, publish_raw_computed_shapes_with_budget_and_context,
    publish_raw_computed_shapes_with_context, publish_stored_computed_shape,
};
pub(crate) use tessellation::{
    PublishedTriangleVertices, PublishedVoronoiEdgeColumn, PublishedVoronoiPolygonOutputs,
    VoronoiReceiptHold, publish_cdt_triangle_vertex_stream_budgeted,
    publish_earcut_mesh_publication_for_domain, publish_mesh_publication_for_domain,
    publish_triangle_vertex_stream_budgeted, publish_voronoi_edge_column_with_budget,
    publish_voronoi_polygon_outputs,
};
pub(in crate::geometry) use tessellation::{
    publish_voronoi_edge_column, publish_voronoi_polygon_shape,
};
pub(crate) use transformed::{publish_transformed_chain, publish_transformed_shape};

#[cfg(test)]
mod tests;
