use super::super::lineage::{LineageCatalog, LineageSequence, ShapeLineage};
use crate::error::{ResourceError, Result};
use crate::geographic::{ChartPole, GeographicFrame};
use crate::geometry::publication::{
    ChartCutSide, ChartRestriction, DirectedChartRestrictionUse, SyntheticChartVertex,
};
use crate::geometry::{Coordinates as _, GeometryErrorKind, MultiMember, Point, Ring, Shape};

pub(super) fn chart_restrictions(
    shape: &Shape,
    lineage: &ShapeLineage,
    catalog: &LineageCatalog,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
    node_start: u32,
) -> Result<(Vec<DirectedChartRestrictionUse>, u32)> {
    let mut restrictions = Vec::new();
    let mut next_node_id = node_start;
    collect_restrictions(
        shape,
        lineage,
        catalog,
        frame,
        longitude_scale,
        latitude_scale,
        &mut next_node_id,
        &mut restrictions,
    )?;
    Ok((restrictions, next_node_id))
}

const fn chart_pole(point: Point) -> Option<ChartPole> {
    if point.y.to_bits() == 90.0_f64.to_bits() {
        Some(ChartPole::North)
    } else if point.y.to_bits() == (-90.0_f64).to_bits() {
        Some(ChartPole::South)
    } else {
        None
    }
}

fn chart_side(point: Point) -> ChartCutSide {
    if point.x > 0.0 {
        ChartCutSide::East
    } else {
        ChartCutSide::West
    }
}

#[expect(
    clippy::float_cmp,
    reason = "this operation classifies exact antimeridian or pole coordinates before chart mapping"
)]
fn synthetic_vertex(
    native_point: Point,
    chart_point: Point,
    node: u32,
) -> Option<SyntheticChartVertex> {
    if let Some(pole) = chart_pole(chart_point) {
        return Some(SyntheticChartVertex::Pole {
            node,
            pole,
            native_longitude_bits: native_point.x.to_bits(),
        });
    }
    if chart_point.x.abs() == 180.0 {
        return Some(SyntheticChartVertex::Cut {
            node,
            side: chart_side(chart_point),
        });
    }
    None
}

#[expect(
    clippy::float_cmp,
    reason = "this operation classifies exact antimeridian or pole coordinates before chart mapping"
)]
pub(in crate::geometry::antimeridian) fn restriction_for_edge(
    start: Point,
    end: Point,
) -> Option<ChartRestriction> {
    let start_cut = start.x.abs() == 180.0;
    let end_cut = end.x.abs() == 180.0;
    let start_pole = chart_pole(start);
    let end_pole = chart_pole(end);
    match (start_pole, end_pole, start_cut, end_cut) {
        (Some(left), Some(right), ..) if left == right && start.x != end.x => {
            Some(ChartRestriction::PolarCap { pole: left })
        },
        (Some(pole), None, _, true) | (None, Some(pole), true, _) => {
            Some(ChartRestriction::PolarCap { pole })
        },
        (Some(_), Some(_), true, true) if start.x == end.x => Some(ChartRestriction::CutChord),
        (_, _, true, true) => Some(ChartRestriction::CutChord),
        _ => None,
    }
}

fn next_node(node: &mut u32) -> Result<u32> {
    let current = *node;
    *node = node.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
    Ok(current)
}

fn ring_vertex_node(
    vertex_index: usize,
    last_index: usize,
    closed_duplicate: bool,
    nodes: &mut [Option<u32>],
    next_node_id: &mut u32,
) -> Result<u32> {
    let node_index = if closed_duplicate && vertex_index == last_index {
        0
    } else {
        vertex_index
    };
    let slot = nodes.get_mut(node_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("restriction vertex lies outside its ring".into())
    })?;
    if let Some(node) = *slot {
        return Ok(node);
    }
    let node = next_node(next_node_id)?;
    *slot = Some(node);
    Ok(node)
}

fn ring_restrictions(
    ring: &Ring,
    lineage: &LineageSequence,
    catalog: &LineageCatalog,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
    next_node_id: &mut u32,
    output: &mut Vec<DirectedChartRestrictionUse>,
) -> Result<()> {
    let native_points = ring.iter_coords().collect::<Vec<_>>();
    if native_points.len() < 2 {
        return Ok(());
    }
    if native_points.len() != lineage.vertices.len()
        || native_points.len().saturating_sub(1) != lineage.edges.len()
    {
        return Err(GeometryErrorKind::Invalid(
            "antimeridian ring coordinates and edge lineage disagree".into(),
        )
        .into());
    }
    let last_index = native_points
        .len()
        .checked_sub(1)
        .ok_or(ResourceError::SizeOverflow)?;
    let closed_duplicate =
        native_points
            .first()
            .zip(native_points.last())
            .is_some_and(|(first, last)| {
                first.x.to_bits() == last.x.to_bits()
                    && first.y.to_bits() == last.y.to_bits()
                    && first.z().map(f64::to_bits) == last.z().map(f64::to_bits)
                    && first.m().map(f64::to_bits) == last.m().map(f64::to_bits)
            });
    let chart_points = native_points
        .iter()
        .copied()
        .map(|point| {
            super::super::polygon::chart_point(point, frame, longitude_scale, latitude_scale)
        })
        .collect::<Result<Vec<_>>>()?;
    let mut nodes = vec![None; native_points.len()];
    for (index, (native_pair, chart_pair)) in native_points
        .array_windows::<2>()
        .zip(chart_points.array_windows::<2>())
        .enumerate()
    {
        let [start_native, end_native] = *native_pair;
        let [start_chart, end_chart] = *chart_pair;
        let edge = lineage.edges.get(index).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("antimeridian ring edge lineage is unavailable".into())
        })?;
        let edge_use = catalog.edge_use_at(edge);
        if edge_use.member != lineage.member {
            return Err(GeometryErrorKind::Invalid(
                "antimeridian chart restriction belongs to a different member".into(),
            )
            .into());
        }
        let Some(restriction) = edge_use.restriction else {
            continue;
        };
        let end_index = index.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        let start_node = ring_vertex_node(
            index,
            last_index,
            closed_duplicate,
            &mut nodes,
            next_node_id,
        )?;
        let end_node = ring_vertex_node(
            end_index,
            last_index,
            closed_duplicate,
            &mut nodes,
            next_node_id,
        )?;
        let (Some(start), Some(end)) = (
            synthetic_vertex(start_native, start_chart, start_node),
            synthetic_vertex(end_native, end_chart, end_node),
        ) else {
            return Err(GeometryErrorKind::Invalid(
                "antimeridian chart restriction has a nonsynthetic endpoint".into(),
            )
            .into());
        };
        output.push(DirectedChartRestrictionUse {
            restriction,
            start,
            end,
        });
    }
    Ok(())
}

fn collect_restrictions(
    shape: &Shape,
    lineage: &ShapeLineage,
    catalog: &LineageCatalog,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
    next_node_id: &mut u32,
    output: &mut Vec<DirectedChartRestrictionUse>,
) -> Result<()> {
    match (shape, lineage) {
        (Shape::Polygon(polygon), ShapeLineage::Polygon(polygon_lineage)) => {
            ring_restrictions(
                &polygon.shell,
                &polygon_lineage.shell,
                catalog,
                frame,
                longitude_scale,
                latitude_scale,
                next_node_id,
                output,
            )?;
            for (hole, hole_lineage) in polygon.holes.iter().zip(&polygon_lineage.holes) {
                ring_restrictions(
                    hole,
                    hole_lineage,
                    catalog,
                    frame,
                    longitude_scale,
                    latitude_scale,
                    next_node_id,
                    output,
                )?;
            }
            Ok(())
        },
        (
            Shape::MultiPolygon(polygons),
            ShapeLineage::MultiPolygon {
                members: lineages, ..
            },
        ) => {
            if polygons.member_count() != lineages.len() {
                return Err(GeometryErrorKind::Invalid(
                    "antimeridian multipolygon members and lineage disagree".into(),
                )
                .into());
            }
            for (polygon, lineage) in polygons.members_with_values().zip(lineages) {
                match (polygon, lineage) {
                    (MultiMember::Empty, None) => {},
                    (MultiMember::Value(polygon), Some(lineage)) => {
                        ring_restrictions(
                            &polygon.shell,
                            &lineage.shell,
                            catalog,
                            frame,
                            longitude_scale,
                            latitude_scale,
                            next_node_id,
                            output,
                        )?;
                        for (hole, hole_lineage) in polygon.holes.iter().zip(&lineage.holes) {
                            ring_restrictions(
                                hole,
                                hole_lineage,
                                catalog,
                                frame,
                                longitude_scale,
                                latitude_scale,
                                next_node_id,
                                output,
                            )?;
                        }
                    },
                    _ => {
                        return Err(GeometryErrorKind::Invalid(
                            "antimeridian multipolygon members and lineage disagree".into(),
                        )
                        .into());
                    },
                }
            }
            Ok(())
        },
        (Shape::GeometryCollection(collection), ShapeLineage::GeometryCollection(lineages)) => {
            if collection.len() != lineages.len() {
                return Err(GeometryErrorKind::Invalid(
                    "antimeridian collection members and lineage disagree".into(),
                )
                .into());
            }
            for (member, lineage) in collection.iter().zip(lineages) {
                let lineage = lineage.as_ref().ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "antimeridian collection member has no lineage".into(),
                    )
                })?;
                collect_restrictions(
                    member,
                    lineage,
                    catalog,
                    frame,
                    longitude_scale,
                    latitude_scale,
                    next_node_id,
                    output,
                )?;
            }
            Ok(())
        },
        (Shape::Point(_), ShapeLineage::Point { .. })
        | (Shape::MultiPoint(_), ShapeLineage::MultiPoint { .. })
        | (Shape::LineString(_), ShapeLineage::LineString(_))
        | (Shape::MultiLineString(_), ShapeLineage::MultiLineString { .. })
        | (Shape::Empty(..), ShapeLineage::Empty) => Ok(()),
        _ => Err(GeometryErrorKind::Invalid(
            "antimeridian shape and lineage disagree during chart restriction collection".into(),
        )
        .into()),
    }
}
