//! Exact source partition of unique stored segments.
//!
//! Borrows unique undirected representatives and existing CSR provenance.
//! A node is a stored endpoint or the crossing of two unique segments. Every
//! node carries its nearest-even binary64, certified in double-double for a
//! crossing; a crossing's exact homogeneous point is built only when a
//! comparison ties on the rounded coordinates, since rounding is monotone.
//! Exact cyclic walks and leftward ray contacts define global faces; stored-ring
//! winding selects cells and an emission plan. Emission checks rounded
//! incidence, cyclic order, and global-face identity.
//!
//! Candidates come from the shared fallible sweep over unique segments plus
//! extra isolated points as zero-length items. Contact-derived cuts, compact
//! nodes, and canonical atoms are generated items. Sweep scratch is
//! [`ResourceError`], not the item cap.

use std::borrow::{Borrow, Cow};
#[cfg(test)]
use std::cell::Cell;
use std::cmp::Ordering;
#[cfg(test)]
use std::ops::ControlFlow;
use std::ops::Range;

#[cfg(test)]
use crate::collections::HashMap;
use crate::error::ResourceError;
use crate::geographic::GeographicFrame;
use crate::geometry::arrangement::{IndexedTopology, IndexedTopologyScratch};
use crate::geometry::ordinates::{
    NodeOrdinates, RequestRowInput, RootCatalog, RootEvaluationContext, RootEvaluationStates,
    RootId, RootMetricState, RootRange, RootRequest, RootSourceData, SourceArcContext,
    resolve_source_root,
};
use crate::geometry::overlay::noding::{
    SegmentProvenance, SegmentSource, dedup_undirected_segments_sourced, placement_offsets,
};
use crate::geometry::segments::clip::{SegmentClipInterval, clip_segment_rect_interval};
use crate::geometry::segments::contact::{
    Contact, RectContact, segment_contact_exact, segment_vs_rect,
};
use crate::geometry::segments::{stored_cross_sign, stored_orientation_sign};
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};
use crate::geometry::topology::SourceTrace;
use crate::geometry::{
    Bounds, ChartMetric, CoordSeq, CoordinateAxes, Coordinates as _, ExpansionBudget,
    GeometryErrorKind, GeometryKind, LineSeq, Loc, MOrdinate, MultiLineString, Point, PointKey,
    Polygon, PolygonizeFull, RepairMethod, Result, Ring, RingKind, Segment, Shape,
    SharedShapeOwners, XY, ZOrdinate, line_segments, remove_antenna_vertices, same_point, storage,
};
#[cfg(test)]
use crate::geometry::{for_each_candidate_pair, single_chain};

mod areal_sources;
mod chain_walk;
mod construction;
mod coverage;
mod embedding;
mod emission;
mod incidence;
mod ingress;
mod linework;
mod location;
mod metric_support;
#[cfg(test)]
mod metrics;
pub(in crate::geometry) mod model;
pub(in crate::geometry) mod operations;
pub(in crate::geometry) mod ordinates;
mod partition;
mod rays;
mod relate;
pub(in crate::geometry) mod serialize;
pub(in crate::geometry) mod subdivision;
pub(in crate::geometry) mod support_queries;
mod topology;
mod views;
pub(in crate::geometry) mod winding;

#[cfg(test)]
use areal_sources::ArealSources;
use areal_sources::{ArealCells, BoundarySelection};
use chain_walk::{node_source_range, walk_exact_chain};
#[cfg(test)]
use incidence::certify_emitted_incidence;
use incidence::cycle_contains_node;
use model::{
    ExactChainView, ExactChainViews, ExactLineSource, ExactMetricNode, ExactMetricSpan,
    ExactMetricSupport, ExactPartition, ExactSourceInventory, ExactSpan, KeptNodes, NodeId,
    NodeRecipe, OrdinateBindings, SourceArcRecord, StagedExactSources, StagedRelationItem,
    StagedSourceComponent,
};
use ordinates::ResolvedNodePoints;
use serialize::{
    ExactLineEmission, assemble_node_parts, bucket_axes, polygons_from_xy_nodes, with_axes,
    xy_sequence_for_nodes,
};
#[cfg(test)]
use support_queries::{
    cartesian_support_nonempty, exact_intersects_bounds, occupied_cartesian_area,
    occupied_cartesian_support,
};
use topology::remove_node_antennas;
use winding::{
    PolygonOccupancy, homogeneous_midpoint, rectangle_side_outward, ring_occupied, rings_occupied,
    select_operation, symbolic_ray_crossing,
};

#[cfg(test)]
thread_local! {
    static SOURCE_LINE_RUNS_CALLS: Cell<usize> = const { Cell::new(0) };
    static SUBDIVISION_ROOT_XY_POSITION_CALLS: Cell<usize> = const { Cell::new(0) };
    static SOURCE_SPAN_VIEW_SORTS: Cell<usize> = const { Cell::new(0) };
    static SOURCE_SPAN_VIEW_ORDERED: Cell<usize> = const { Cell::new(0) };
    static SOURCE_SPAN_ENDPOINT_PARAMETERS: Cell<usize> = const { Cell::new(0) };
    static SOURCE_SPAN_INTERIOR_PARAMETERS: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
fn reset_source_line_runs_calls() {
    SOURCE_LINE_RUNS_CALLS.with(|calls| calls.set(0));
}

#[cfg(test)]
fn source_line_runs_calls() -> usize {
    SOURCE_LINE_RUNS_CALLS.with(Cell::get)
}

#[cfg(test)]
pub(crate) fn reset_subdivision_root_xy_position_calls() {
    SUBDIVISION_ROOT_XY_POSITION_CALLS.with(|calls| calls.set(0));
}

#[cfg(test)]
pub(crate) fn subdivision_root_xy_position_calls() -> usize {
    SUBDIVISION_ROOT_XY_POSITION_CALLS.with(Cell::get)
}

#[cfg(test)]
pub(crate) fn reset_source_span_view_order_counts() {
    SOURCE_SPAN_VIEW_SORTS.with(|count| count.set(0));
    SOURCE_SPAN_VIEW_ORDERED.with(|count| count.set(0));
    SOURCE_SPAN_ENDPOINT_PARAMETERS.with(|count| count.set(0));
    SOURCE_SPAN_INTERIOR_PARAMETERS.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn source_span_view_order_counts() -> (usize, usize, usize, usize) {
    (
        SOURCE_SPAN_VIEW_SORTS.with(Cell::get),
        SOURCE_SPAN_VIEW_ORDERED.with(Cell::get),
        SOURCE_SPAN_ENDPOINT_PARAMETERS.with(Cell::get),
        SOURCE_SPAN_INTERIOR_PARAMETERS.with(Cell::get),
    )
}

pub(in crate::geometry) use construction::Forest as ExactPointForest;
use construction::{
    AxisValue, PointArena, interpolate_ordinate, parameter_along, partition_unique_segments,
    partition_unique_segments_in_window, partition_unique_segments_unbudgeted,
};
pub(in crate::geometry) use embedding::{EmbeddingDomain, segments_within_chebyshev};
use emission::{
    BoundaryPlan, CellFacts, CoverageFaceState, NodeParts, OverlaySourceLocation,
    polygonize_emission, polygonize_full_emission,
};
pub(in crate::geometry) use emission::{
    OverlayNodeReceipt, OverlayPolygonPartWithSources, OverlaySourceEdgeUse,
    emitted_pair_preserves_incidence, walk_is_shell_with_work,
};
use location::AreaLocationCells;
pub(in crate::geometry) use location::AreaLocationStrategy;
#[cfg(test)]
pub(in crate::geometry) use metrics::{
    partition_builds, partition_metrics, reset_partition_builds,
};
#[cfg(test)]
pub(crate) use metrics::{partition_calls, reset_partition_calls};

#[cfg(test)]
use crate::geometry::segments::crossing_homogeneous;
#[cfg(test)]
use crate::geometry::tessellation::exact::{compare_ratios, round_ratio};

const fn location(occupied: bool) -> Loc {
    if occupied {
        Loc::Interior
    } else {
        Loc::Exterior
    }
}

#[cfg(test)]
mod tests;
