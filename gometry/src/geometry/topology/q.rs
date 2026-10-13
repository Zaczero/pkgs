//! Exact geographic quotient faces and source-owned face occupancy.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::ops::Range;

use super::exact::{
    ExactChartPoint, ExactGeographicChartInventory, ExactQuotientKey, ExactSourceRange,
    FallibleAvl, NodedExactChart, QuotientNodeId, SourceEdgeId, SourceEdgeKind, SourcePolygonId,
    SourceRingId, SourceRowId, point_on_exact_segment, point_on_exact_segment_with_work,
    vector_cross, vector_difference,
};
use crate::error::{ResourceError, Result};
use crate::geographic::{ChartPole, GeographicFrame};
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::ordinates::{
    NodeOrdinates, RootCatalog, RootId, RootMetricState, SourceArcMetricAccess, resolve_source_root,
};
use crate::geometry::segments::{try_sort_by, try_sort_by_with_work};
use crate::geometry::source_support::RingKind;
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::{
    Bounds, ChartMetric, De9im, Dimension, ExpansionBudget, GeometryErrorKind, Loc, Point, Segment,
    SegmentIndex, Shape, ValidationIssue, Work,
};
use crate::heap_size::HeapSize;

mod build;
mod collapsed_support;
mod cut_chart;
mod cut_index;
mod cut_selection;
mod face_validity;
mod global_faces;
mod host_sweep;
mod location;
mod model;
mod occupancy;
mod rectangle;
mod rotation;
mod row_support;
mod spatial_index;
mod validity;

use self::collapsed_support::{
    append_explicit_node_point_support, collapsed_point_candidates,
    collect_collapsed_shell_candidates, span_has_collapsed_shell, trim_collapsed_points_on_face,
};
pub(super) use self::cut_chart::{compare_qnode_keys, compare_qnode_keys_with_work};
use self::cut_chart::{make_cut_chart, q_port_node};
use self::cut_index::{
    apply_cut_event_winding, apply_ring_transition, for_each_span_ring_transition,
    source_ring_transition, span_has_ring_transition,
};
use self::cut_selection::select_cut;
#[cfg(test)]
use self::cut_selection::{
    chart_edge_longitude_intervals, chart_has_cut_port, select_edge_free_cut,
};
pub(super) use self::face_validity::QFaceValidity;
use self::face_validity::{FaceArc, FaceOccupancyState, FaceWalkFrame};
pub(super) use self::global_faces::global_face_structure;
#[cfg(test)]
use self::global_faces::north_pole_ring_windings;
use self::global_faces::{SeedFaceGerm, build_global_face_topology};
use self::host_sweep::{compare_span_to_query, compare_spans_at, resolve_host_queries};
pub(super) use self::location::row_locations;
use self::location::{
    lift_query_point, locate_q_node, locate_q_span, location_in_area_face, location_in_global_face,
    q_face_has_row, span_crosses_north_ray,
};
pub(in crate::geometry) use self::model::PoleSector;
pub(super) use self::model::{
    CanonicalQNodeKey, ChartSpan, ChartSpanId, CutChart, CutIntervalId, ExactGeographicQ,
    GlobalFaceId, GlobalFaceStructure, QComponentId, QCutIndex, QDart, QDartId, QFaceChart,
    QLocalFace, QLocalFaceId, QNode, QNodeId, QNodeKey, QPort, QPortId, QRowLocations, QSpanOwner,
    QuotientFaces,
};
pub(crate) use self::model::{ExactGeographicPointLocation, ExactGeographicTopology};
use self::model::{
    GlobalFaceTopology, HostGerm, HostQuery, HostQueryTarget, HostRoot, PortCandidate,
    QComponentCutRuns, QCutEvent, QCutInterval, ROOT_CUT_FACE_SIDE, RingTransition,
    SeamBoundaryOwner, SeamOwnerSide, SeedRaySpan, SpanOccurrence, SweepEvent, SweepEventKind,
    point_heap_bytes, rational_heap_bytes, source_range_heap_bytes,
};
pub(super) use self::occupancy::GlobalFaceRowOccupancy;
use self::occupancy::GlobalFaceRowProducts;
pub(crate) use self::rectangle::ExactGeographicRectangleLocation;
use self::rectangle::ExactQRectangleWindows;
#[cfg(test)]
use self::rotation::compare_q_darts;
pub(super) use self::rotation::compare_q_darts_with_points;
use self::rotation::{UnionFind, face_at_pole_longitude, face_in_direction};
use self::row_support::{
    CollapsedPointFaces, GlobalNodeRowSupport, GlobalSpanRowSupport, build_node_line_boundary_rows,
    build_span_line_rows,
};
pub(super) use self::spatial_index::QSpatialItem;
use self::spatial_index::{QSpatialIndex, outward_segment_proxy};
pub(in crate::geometry) use crate::geometry::exact_point::{
    exact_segments_intersect, exact_segments_intersect_with_work,
};

#[cfg(test)]
mod tests;
#[cfg(test)]
pub(super) use tests::subgraph_oracle::{B3SubgraphFaceStructure, b3_subgraph_face_structure};
