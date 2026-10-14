use std::collections::BinaryHeap;
use std::num::NonZeroUsize;

use crate::error::Result;
mod chain;
#[cfg(test)]
mod computed_edge_publication_tests;
mod guard;
#[cfg(test)]
mod guarded_scratch_tests;
mod publication;
mod rebuild;

pub(crate) use chain::vw_area_tolerance;
use chain::{
    Chain, CoverageEdgeIntent, CoverageEdgeRoles, SimplifiedChain, exact_chain, simplify_chain_kept,
};
pub(in crate::geometry) use chain::{simplify_chain_guarded, triangle_area};
pub(in crate::geometry) use guard::TopologyGuard;
use publication::{publish_coverage_edge_roles, simplify_unshared_geographic_rows};
use rebuild::{RebuildRoots, rebuild_row};

use crate::geometry::coverage::{
    AABB, HashMap, Reverse, coverage_rows, valid_coverage_rows, validate_polygonal_rows,
};
use crate::geometry::overlay::{ExactChainView, ExactPartition, ExactSourceInventory, NodeId};
use crate::geometry::segment_index::ChordIndex;
use crate::geometry::{
    BulkRTree, CoordSeq, Coordinates as _, GeometryErrorKind, Orientation, Point, PointKey,
    Polygon, Ring, Segment, Shape, SimplifyMethod, SourceArcContext, TopologyDomain, XY,
    dedup_consecutive_points, orientation, point_segment_distance, same_point,
    same_topological_coordinate, segment_contact_exact, undirected_segment_edge_key,
};

// --- Simplification ---------------------------------------------------------

/// Simplify a valid polygonal coverage's boundaries, preserving topology.
///
/// Shared interfaces are derived from exact partition incidence, simplified
/// once, and replayed to every source owner. `simplify_boundary` controls
/// whether exterior (unshared) chains simplify too.
pub(crate) fn coverage_simplify_with_context<S: std::borrow::Borrow<Shape>>(
    rows: &[S],
    tolerance: f64,
    method: SimplifyMethod,
    simplify_boundary: bool,
    source_context: &SourceArcContext<'_, '_, crate::crs::GeodesicMetric>,
) -> Result<Vec<Shape>> {
    if !(tolerance.is_finite() && tolerance >= 0.0) {
        return Err(GeometryErrorKind::NonNegativeFinite("tolerance", tolerance).into());
    }
    let domain = source_context.domain();
    let identity = same_topological_coordinate(tolerance, 0.0);
    if identity {
        let prepared = match domain {
            TopologyDomain::Cartesian => valid_coverage_rows(rows, "coverage_simplify")?.rows,
            TopologyDomain::Geographic(_) => {
                if !crate::geometry::coverage_is_valid_with_domain(rows, 0.0, domain)? {
                    return Err(GeometryErrorKind::InvalidCoverage {
                        operation: "coverage_simplify",
                    }
                    .into());
                }
                coverage_rows(rows, 0.0)?.rows
            },
        };
        return Ok(prepared.iter().map(|row| row.shape.clone()).collect());
    }
    let geographic_frame = match domain {
        TopologyDomain::Geographic(frame) => Some(frame),
        TopologyDomain::Cartesian => None,
    };
    if let TopologyDomain::Geographic(frame) = domain {
        validate_polygonal_rows(rows)?;
        let analysis =
            crate::geometry::analyze_geographic_coverage(rows, frame, 0.0, "coverage_simplify")?;
        if !analysis.is_valid() {
            return Err(GeometryErrorKind::InvalidCoverage {
                operation: "coverage_simplify",
            }
            .into());
        }
        let has_shared_boundary = analysis.has_shared_boundary();
        let ordinary_source_rings = analysis.source_rings_are_ordinary();
        // A no-shared full-turn ring can use its source-index plan to retain
        // directed edges. Ordinary short-chart rows keep the independent path;
        // ordinary long edges stay on the exact-chain path.
        if !has_shared_boundary
            && (!ordinary_source_rings
                || (rows.len() > 1 && analysis.source_edges_match_shortest_chart()?))
        {
            return simplify_unshared_geographic_rows(
                rows,
                tolerance,
                method,
                simplify_boundary,
                frame,
                source_context,
            );
        }
    }

    let prepared = valid_coverage_rows(rows, "coverage_simplify")?.rows;

    let mut all_segments = Vec::with_capacity(prepared.iter().map(|row| row.segments.len()).sum());
    for row in &prepared {
        all_segments.extend_from_slice(&row.segments);
    }
    let source_refs: Vec<&Shape> = prepared.iter().map(|row| row.shape).collect();
    let inventory = ExactSourceInventory::from_shapes(&source_refs)?;
    let mut budget = crate::geometry::ExpansionBudget::new("coverage_simplify", "exact partition");
    let partition = inventory.partition(&mut budget)?;
    let mut rebuild_roots = geographic_frame.map(|_| {
        let root_axes = source_refs
            .iter()
            .fold(crate::geometry::CoordinateAxes::XY, |axes, shape| {
                axes.union(shape.axes())
            });
        RebuildRoots::new(
            &inventory,
            source_context,
            root_axes,
            partition.node_count(),
        )
    });
    let views = partition.coverage_chain_views(inventory.source_operands())?;
    let source_spans = partition.source_span_views(inventory.source_segments())?;
    let mut chain_sources = vec![Vec::new(); views.chains.len()];
    for (source, spans) in source_spans.iter().enumerate() {
        for &(span, _) in spans {
            let chain = *views
                .chain_of_span
                .get(span as usize)
                .expect("source span belongs to the coverage chain inventory");
            let sources = chain_sources
                .get_mut(chain as usize)
                .expect("coverage span has a chain owner");
            if sources.last() != Some(&source) {
                sources.push(source);
            }
        }
    }
    let mut chains = Vec::with_capacity(views.chains.len());
    for (view, sources) in views.chains.into_iter().zip(&chain_sources) {
        chains.push(exact_chain(
            &partition,
            view,
            sources,
            inventory.source_segments(),
        ));
    }
    let guard = TopologyGuard::new(
        &all_segments,
        all_segments
            .iter()
            .flat_map(|segment| {
                [[segment.start.x, segment.start.y], [
                    segment.end.x,
                    segment.end.y,
                ]]
            })
            .collect(),
    );
    let simplified: Vec<SimplifiedChain> = chains
        .iter()
        .map(|chain| {
            if !simplify_boundary && !chain.shared {
                return Ok(SimplifiedChain {
                    kept: (0..chain.points.len()).collect(),
                });
            }
            let cyclic = chain.points.len() > 1
                && chain
                    .points
                    .first()
                    .zip(chain.points.last())
                    .is_some_and(|(&first, &last)| same_point(first, last));
            let kept = simplify_chain_kept(
                chain.points.len(),
                |index| {
                    *chain
                        .points
                        .get(index)
                        .expect("simplification index is within its chain")
                },
                cyclic,
                tolerance,
                method,
                &guard,
                &chain.source_segments,
            )?;
            Ok(SimplifiedChain { kept })
        })
        .collect::<Result<Vec<_>>>()?;
    drop(guard);
    drop(all_segments);

    let source_ring_ranges = inventory.source_ring_segment_ranges()?;
    let mut source_ring_cursor = 0;
    let output = prepared
        .iter()
        .map(|row| {
            let mut computed_edge_budget =
                crate::geometry::ExpansionBudget::new("coverage_simplify", "computed edges");
            let (shape, edge_roles) = rebuild_row(
                row.shape,
                &mut source_ring_cursor,
                &source_ring_ranges,
                inventory.source_segments(),
                &source_spans,
                &views.chain_of_span,
                &chains,
                &simplified,
                &partition,
                rebuild_roots.as_mut(),
            )?;
            match (geographic_frame, edge_roles) {
                (Some(frame), Some(edge_roles)) => publish_coverage_edge_roles(
                    shape,
                    edge_roles,
                    frame,
                    source_context,
                    &mut computed_edge_budget,
                ),
                (None, None) => Ok(shape),
                _ => Err(GeometryErrorKind::Invalid(
                    "coverage edge roles do not match the publication frame".into(),
                )
                .into()),
            }
        })
        .collect::<Result<Vec<_>>>()?;
    if source_ring_cursor != source_ring_ranges.len() {
        return Err(GeometryErrorKind::Invalid(
            "coverage rebuild left source ring ranges unused".into(),
        )
        .into());
    }
    Ok(output)
}
