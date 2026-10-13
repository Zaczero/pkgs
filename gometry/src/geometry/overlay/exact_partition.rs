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
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::ordinates::{
    NodeOrdinates, RequestRowInput, RootCatalog, RootId, RootMetricState, RootRange, RootRequest,
    SourceArcContext, SourceArcMetricAccess, SourceArcRequest, resolve_request_rows,
    resolve_source_root, source_arc_ordinates,
};
use crate::geometry::overlay::noding::{
    SegmentProvenance, SegmentSource, dedup_undirected_segments_sourced, placement_offsets,
};
use crate::geometry::overlay::{build_certified_overlay_shape, ray_crossing};
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
    SharedShapeOwners, Unmetered, Work, XY, ZOrdinate, line_segments, remove_antenna_vertices,
    same_point, storage,
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
    static SUBDIVISION_RESOLUTION_BATCHES: std::cell::RefCell<Vec<(usize, usize, usize)>> =
        const { std::cell::RefCell::new(Vec::new()) };
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
pub(crate) fn reset_subdivision_resolution_batches() {
    SUBDIVISION_RESOLUTION_BATCHES.with(|batches| batches.borrow_mut().clear());
}

#[cfg(test)]
pub(crate) fn take_subdivision_resolution_batches() -> Vec<(usize, usize, usize)> {
    SUBDIVISION_RESOLUTION_BATCHES.with(|batches| std::mem::take(&mut *batches.borrow_mut()))
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
    GlobalFaces, OverlayNodeReceipt, OverlayPolygonPartWithSources, OverlaySourceEdgeUse,
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
pub(in crate::geometry) use rays::{
    RayPoints, RayPointsWithWork, compare_departures_with_work, shoot_left, shoot_left_with_work,
    visit_component_faces,
};

pub(in crate::geometry) use self::Axis as RayAxis;
#[cfg(test)]
use crate::geometry::segments::crossing_homogeneous;
#[cfg(test)]
use crate::geometry::tessellation::exact::{compare_ratios, round_ratio};

#[derive(Clone, Copy)]
pub(in crate::geometry) enum Axis {
    X,
    Y,
}

impl Axis {
    const fn of(self, point: XY) -> f64 {
        match self {
            Self::X => point.x,
            Self::Y => point.y,
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::X => 0,
            Self::Y => 1,
        }
    }
}

const fn location(occupied: bool) -> Loc {
    if occupied {
        Loc::Interior
    } else {
        Loc::Exterior
    }
}

fn graph_edge_departure<E, W>(edge_nodes: &E, edge: usize, origin: u32, work: &mut W) -> Result<u32>
where
    E: Fn(usize) -> Result<[u32; 2]> + ?Sized,
    W: Work,
{
    work.charge_into_error(1)?;
    let [start, end] = edge_nodes(edge)?;
    if start == origin {
        Ok(end)
    } else if end == origin {
        Ok(start)
    } else {
        Err(
            GeometryErrorKind::Invalid("graph departure edge is not incident to its origin".into())
                .into(),
        )
    }
}

fn graph_contact_walk<R, E, W>(
    points: &R,
    topology: &IndexedTopology,
    edge_of_slot: &[u32],
    edge_nodes: &E,
    edge: usize,
    work: &mut W,
) -> Result<u32>
where
    R: RayPointsWithWork + ?Sized,
    E: Fn(usize) -> Result<[u32; 2]> + ?Sized,
    W: Work,
{
    work.charge_into_error(1)?;
    let [start, end] = edge_nodes(edge)?;
    let upper = match points.compare_axis_with_work(start, end, Axis::Y, work)? {
        Ordering::Less => end,
        Ordering::Greater => start,
        Ordering::Equal => {
            return Err(
                GeometryErrorKind::Invalid("leftward ray contact is horizontal".into()).into(),
            );
        },
    };
    let start = *topology.starts.get(upper as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("leftward ray contact node has no departure row".into())
    })? as usize;
    let end = *topology
        .starts
        .get(
            (upper as usize)
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("leftward ray contact row is incomplete".into())
        })? as usize;
    for slot in start..end {
        work.charge_into_error(1)?;
        if edge_of_slot.get(slot).copied()
            == Some(u32::try_from(edge).map_err(|_| ResourceError::SizeOverflow)?)
        {
            let face = *topology.face_of.get(slot).ok_or_else(|| {
                GeometryErrorKind::Invalid("leftward ray contact has no face walk".into())
            })?;
            return face
                .checked_add(1)
                .ok_or_else(|| ResourceError::SizeOverflow.into());
        }
    }
    Err(
        GeometryErrorKind::Invalid("leftward ray contact edge has no upper-end departure".into())
            .into(),
    )
}

fn graph_exterior_walk<R, E, W>(
    points: &R,
    topology: &IndexedTopology,
    edge_of_slot: &[u32],
    edge_nodes: &E,
    root: u32,
    work: &mut W,
) -> Result<Option<u32>>
where
    R: RayPointsWithWork + ?Sized,
    E: Fn(usize) -> Result<[u32; 2]> + ?Sized,
    W: Work,
{
    let start = *topology.starts.get(root as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("graph component root has no departure row".into())
    })? as usize;
    let end = *topology
        .starts
        .get(
            (root as usize)
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("graph component root row is incomplete".into())
        })? as usize;
    let mut chosen = None;
    let mut has_upper = false;
    for slot in start..end {
        work.charge_into_error(1)?;
        let target = *topology.targets.get(slot).ok_or_else(|| {
            GeometryErrorKind::Invalid("graph component departure has no target".into())
        })?;
        work.charge_into_error(1)?;
        let upper = !points
            .compare_axis_with_work(target, root, Axis::Y, work)?
            .is_lt();
        if chosen.is_none() || (upper && !has_upper) {
            chosen = Some(slot);
            has_upper = upper;
            continue;
        }
        if upper != has_upper {
            continue;
        }
        let previous = chosen.ok_or_else(|| {
            GeometryErrorKind::Invalid("graph exterior fan lost its chosen departure".into())
        })?;
        let previous_edge = *edge_of_slot.get(previous).ok_or_else(|| {
            GeometryErrorKind::Invalid("graph exterior fan references an unknown edge".into())
        })? as usize;
        let current_edge = *edge_of_slot.get(slot).ok_or_else(|| {
            GeometryErrorKind::Invalid("graph exterior fan references an unknown edge".into())
        })? as usize;
        let previous_target = graph_edge_departure(edge_nodes, previous_edge, root, work)?;
        let current_target = graph_edge_departure(edge_nodes, current_edge, root, work)?;
        if compare_departures_with_work(points, root, previous_target, current_target, work)?
            == Ordering::Less
        {
            chosen = Some(slot);
        }
    }
    chosen
        .map(|slot| {
            topology.face_of.get(slot).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("graph exterior departure has no face".into()).into()
            })
        })
        .transpose()
}

pub(in crate::geometry) fn graph_global_faces_with_work<R, E, W>(
    points: &R,
    topology: &IndexedTopology,
    edge_count: usize,
    edge_nodes: E,
    edge_of_slot: &[u32],
    work: &mut W,
) -> Result<GlobalFaces>
where
    R: RayPointsWithWork + ?Sized,
    E: Fn(usize) -> Result<[u32; 2]>,
    W: Work,
{
    let node_count = topology.component_of.len();
    let slots = edge_count
        .checked_mul(2)
        .ok_or(ResourceError::SizeOverflow)?;
    if edge_of_slot.len() != slots
        || topology.targets.len() != slots
        || topology.face_of.len() != slots
        || topology.starts.len()
            != node_count
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?
    {
        return Err(GeometryErrorKind::Invalid(
            "graph face inputs do not share one indexed topology".into(),
        )
        .into());
    }
    work.charge_into_error(u64::try_from(node_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut component_count = 0_usize;
    for &component in &topology.component_of {
        work.charge_into_error(1)?;
        component_count = component_count.max(
            usize::try_from(component)
                .map_err(|_| ResourceError::SizeOverflow)?
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?,
        );
    }
    work.charge_into_error(
        u64::try_from(component_count).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut minima = vec![None; component_count];
    for (node, &component) in topology.component_of.iter().enumerate() {
        work.charge_into_error(1)?;
        let component = usize::try_from(component).map_err(|_| ResourceError::SizeOverflow)?;
        let minimum = minima.get_mut(component).ok_or_else(|| {
            GeometryErrorKind::Invalid("indexed graph component ID is unknown".into())
        })?;
        let node = u32::try_from(node).map_err(|_| ResourceError::SizeOverflow)?;
        if let Some(previous) = *minimum {
            work.charge_into_error(1)?;
            let x = points.compare_axis_with_work(node, previous, Axis::X, work)?;
            let order = if x.is_eq() {
                work.charge_into_error(1)?;
                points.compare_axis_with_work(node, previous, Axis::Y, work)?
            } else {
                x
            };
            if order.is_lt() {
                *minimum = Some(node);
            }
        } else {
            *minimum = Some(node);
        }
    }
    work.charge_into_error(
        u64::try_from(component_count).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut roots = Vec::with_capacity(component_count);
    for minimum in minima {
        work.charge_into_error(1)?;
        roots.push(minimum.ok_or_else(|| {
            GeometryErrorKind::Invalid("indexed graph component has no root node".into())
        })?);
    }

    let walks = topology.face_starts.len().checked_sub(1).ok_or_else(|| {
        GeometryErrorKind::Invalid("indexed graph face starts have no initial sentinel".into())
    })?;
    let size = walks.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
    let limit = u32::try_from(size).map_err(|_| ResourceError::SizeOverflow)?;
    work.charge_into_error(u64::try_from(size).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut links: Vec<u32> = (0..limit).collect();
    work.charge_into_error(
        u64::try_from(component_count).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut component_hosts = Vec::with_capacity(component_count);
    let hits = if roots.len() == 1 {
        work.charge_into_error(1)?;
        vec![None]
    } else {
        shoot_left_with_work(points, edge_count, &edge_nodes, &roots, work)?
    };
    if hits.len() != roots.len() {
        return Err(GeometryErrorKind::Invalid(
            "leftward ray sweep returned an incomplete component-host table".into(),
        )
        .into());
    }
    for (&root, hit) in roots.iter().zip(hits) {
        work.charge_into_error(1)?;
        let host = if let Some(edge) = hit {
            graph_contact_walk(points, topology, edge_of_slot, &edge_nodes, edge, work)?
        } else {
            0
        };
        component_hosts.push(host);
        if let Some(exterior) =
            graph_exterior_walk(points, topology, edge_of_slot, &edge_nodes, root, work)?
        {
            let link = usize::try_from(exterior)
                .map_err(|_| ResourceError::SizeOverflow)?
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            *links.get_mut(link).ok_or_else(|| {
                GeometryErrorKind::Invalid("graph exterior face link is unknown".into())
            })? = host;
        }
    }

    for start in 1..size {
        work.charge_into_error(1)?;
        let mut root = u32::try_from(start).map_err(|_| ResourceError::SizeOverflow)?;
        let mut steps = 0_usize;
        loop {
            work.charge_into_error(1)?;
            let parent = *links.get(root as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("graph face link is outside its table".into())
            })?;
            if parent == root {
                break;
            }
            root = parent;
            steps = steps.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            if steps >= size {
                return Err(GeometryErrorKind::Invalid(
                    "graph component exterior links contain a cycle".into(),
                )
                .into());
            }
        }
        let mut node = u32::try_from(start).map_err(|_| ResourceError::SizeOverflow)?;
        let mut steps = 0_usize;
        loop {
            work.charge_into_error(1)?;
            let parent = *links.get(node as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("graph face link is outside its table".into())
            })?;
            if parent == root {
                break;
            }
            *links.get_mut(node as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("graph face link is outside its table".into())
            })? = root;
            node = parent;
            steps = steps.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            if steps >= size {
                return Err(GeometryErrorKind::Invalid(
                    "graph component exterior links contain a cycle".into(),
                )
                .into());
            }
        }
    }

    work.charge_into_error(u64::try_from(size).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut labels = vec![0_u32; size];
    let mut count = 1_u32;
    for node in 1..size {
        work.charge_into_error(1)?;
        let link = *links.get(node).ok_or_else(|| {
            GeometryErrorKind::Invalid("graph face link is outside its table".into())
        })?;
        if link == node as u32 {
            *labels.get_mut(node).ok_or_else(|| {
                GeometryErrorKind::Invalid("graph face label is outside its table".into())
            })? = count;
            count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        }
    }
    for host in &mut component_hosts {
        work.charge_into_error(1)?;
        let root = *links.get(*host as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("graph component host is outside its link table".into())
        })?;
        *host = *labels.get(root as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("graph component host has no global label".into())
        })?;
    }
    work.charge_into_error(u64::try_from(walks).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut walk_faces = Vec::with_capacity(walks);
    for &root in links.iter().skip(1) {
        work.charge_into_error(1)?;
        walk_faces.push(*labels.get(root as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("graph local face has no global label".into())
        })?);
    }
    Ok(GlobalFaces {
        walk_faces,
        component_hosts,
        count,
    })
}

#[cfg(test)]
mod tests;
