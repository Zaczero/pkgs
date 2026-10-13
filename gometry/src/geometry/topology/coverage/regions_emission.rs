use std::sync::Arc;

use super::super::emission::plan::SelectedRegionPoles;
use super::super::exact::SourceEdgeKind;
use super::super::q::{ExactGeographicTopology, QNodeId, QPortId};
use super::regions::{GeographicCoverageBoundaryCycle, GeographicCoverageRootUse};
use super::{GeographicCoverageAnalysis, GeographicCoverageFaceAssignment};
use crate::array::StreamingShapes;
use crate::error::{ResourceError, Result};
use crate::geographic::{ChartPole, FullTurnPole, GeographicFrame, Latitude};
use crate::geometry::area::{AreaSign, exact_ring_area_sign_columns};
use crate::geometry::embedding_completion::literals::{
    LiteralAddress, LiteralPolygon, LiteralRowLayout, LiteralSequence, ShapeLiteralLayout,
};
use crate::geometry::embedding_completion::storage::FrozenLiteralBuffers;
use crate::geometry::ordinates::{
    NodeOrdinates, RequestRowInput, RootCatalog, RootId, RootMetricState, RootRange, RootRequest,
    SourceArcMetricAccess, resolve_request_rows, resolve_source_root,
};
use crate::geometry::predicates::ring_contains_interior;
use crate::geometry::publication::{
    ProducerDrawingReceipt, ProducerImageId, ProducerLiteral, ProducerSourceRootUse, StorageTarget,
};
use crate::geometry::storage::StorageLayout;
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::{
    ChartMetric, CoordSeq, CoordinateAxes, GeometryErrorKind, MOrdinate, Point, Polygon, Ring,
    Shape, ZOrdinate, same_topological_coordinate,
};
use crate::work::Work;

struct RootParameter {
    root: RootId,
    parameter: ExactRational,
    emitted: bool,
}

struct CoverageRoots<'a> {
    topology: &'a ExactGeographicTopology,
    frame: GeographicFrame,
}

impl RootCatalog for CoverageRoots<'_> {
    fn resolve<M: ChartMetric, W: Work>(
        &self,
        root: RootId,
        parameter: &ExactRational,
        axes: CoordinateAxes,
        metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
        state: &mut RootMetricState,
        fraction: &mut Option<f64>,
        work: &mut W,
    ) -> Result<NodeOrdinates> {
        let edge = self
            .topology
            .sources
            .edges
            .get(root.index())
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "coverage output source root is outside the admitted edge table".into(),
                )
            })?;
        resolve_source_root(
            self.frame,
            &edge.source,
            edge.retained_longitude(),
            parameter,
            axes,
            metric_access,
            state,
            fraction,
            work,
        )
    }
}

#[derive(Clone, Copy)]
struct OutputVertex {
    point: Point,
    node: Option<usize>,
    request_row: usize,
}

struct OutputPolygon {
    shell: Vec<OutputVertex>,
    holes: Vec<Vec<OutputVertex>>,
}

struct OutputRow {
    logical_row: usize,
    polygons: Vec<OutputPolygon>,
}

struct OutputRing {
    vertices: Vec<OutputVertex>,
    hole: bool,
}

impl GeographicCoverageAnalysis {
    pub(in crate::geometry) fn coverage_drawing_receipt<M: ChartMetric>(
        &self,
        assignment: &GeographicCoverageFaceAssignment,
        declared_axes: CoordinateAxes,
        context: &crate::geometry::SourceArcContext<'_, '_, M>,
        target: StorageTarget,
        work: &mut impl Work,
    ) -> Result<ProducerDrawingReceipt> {
        let crate::geometry::TopologyDomain::Geographic(frame) = context.domain() else {
            return Err(GeometryErrorKind::Invalid(
                "geographic coverage cleaning requires its admitted chart frame".into(),
            )
            .into());
        };
        if frame != self.topology.sources.frame {
            return Err(GeometryErrorKind::Invalid(
                "geographic coverage cleaning changed its admitted chart frame".into(),
            )
            .into());
        }

        let plans = self.cleaning_boundary_plans(assignment, work)?;
        for (owner, region) in assignment.owners.iter().zip(&plans.face_regions) {
            if owner != &region.map(|region| region.output_row) {
                return Err(GeometryErrorKind::Invalid(
                    "coverage output regions changed their assigned owner row".into(),
                )
                .into());
            }
        }

        let mut node_roots: Vec<Option<Vec<RootParameter>>> = std::iter::repeat_with(|| None)
            .take(plans.topology.q.nodes.len())
            .collect();
        for &node in &plans.live_nodes {
            *node_roots.get_mut(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage live node is outside the Q table".into())
            })? = Some(Vec::new());
        }
        for row in &plans.rows {
            for region in &row.regions {
                for cycle in &region.cycles {
                    for use_ in &cycle.uses {
                        if plans.live_spans.binary_search(&use_.span).is_err() {
                            return Err(GeometryErrorKind::Invalid(
                                "coverage output boundary uses a span outside its live plan".into(),
                            )
                            .into());
                        }
                        add_endpoint_roots(
                            &mut node_roots,
                            use_.from_presentation.node,
                            &use_.root_uses,
                            true,
                            work,
                        )?;
                        add_endpoint_roots(
                            &mut node_roots,
                            use_.to_presentation.node,
                            &use_.root_uses,
                            false,
                            work,
                        )?;
                    }
                }
            }
        }

        let mut live_spans = vec![false; plans.topology.q.spans.len()];
        for &span in &plans.live_spans {
            *live_spans.get_mut(span.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage live span is outside the Q table".into())
            })? = true;
        }
        let mut port_points = vec![None; plans.topology.q.ports.len()];
        for &port_id in &plans.live_ports {
            *port_points.get_mut(port_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage live port is outside the Q table".into())
            })? = Some(port_point(plans.topology, port_id, frame, work)?);
        }

        let mut output_rows = Vec::with_capacity(plans.rows.len());
        for (expected_row, row) in plans.rows.iter().enumerate() {
            work.charge_into_error(1)?;
            if row.output_row != expected_row {
                return Err(GeometryErrorKind::Invalid(
                    "coverage output row plans are not in logical row order".into(),
                )
                .into());
            }
            let mut polygons = Vec::new();
            if row.whole_sphere {
                polygons.push(OutputPolygon {
                    shell: whole_sphere_vertices(frame),
                    holes: Vec::new(),
                });
            } else {
                for region in &row.regions {
                    let mut shells = Vec::<OutputRing>::new();
                    let mut holes = Vec::<OutputRing>::new();
                    let mut cycles = region.cycles.iter().collect::<Vec<_>>();
                    cycles.sort_unstable_by_key(|cycle| cycle.cycle_index);
                    for cycle in cycles {
                        let ring = output_ring(
                            plans.topology,
                            cycle,
                            region.poles,
                            frame,
                            &port_points,
                            &live_spans,
                            work,
                        )?;
                        if ring.hole {
                            holes.push(ring);
                        } else {
                            shells.push(ring);
                        }
                    }
                    let mut region_polygons = shells
                        .into_iter()
                        .map(|shell| OutputPolygon {
                            shell: shell.vertices,
                            holes: Vec::new(),
                        })
                        .collect::<Vec<_>>();
                    for hole in holes {
                        let probe = hole
                            .vertices
                            .first()
                            .map(|vertex| vertex.point)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "coverage hole boundary cycle has no vertices".into(),
                                )
                            })?;
                        let mut containing = None;
                        for (index, polygon) in region_polygons.iter().enumerate() {
                            work.charge_into_error(1)?;
                            let shell = polygon
                                .shell
                                .iter()
                                .map(|vertex| vertex.point)
                                .collect::<Vec<_>>();
                            if ring_contains_interior(&shell, probe) {
                                containing = Some(index);
                                break;
                            }
                        }
                        let polygon_index = match containing {
                            Some(index) => index,
                            None if region_polygons.len() == 1 => 0,
                            None => {
                                return Err(GeometryErrorKind::Invalid(
                                    "coverage hole has no containing output shell".into(),
                                )
                                .into());
                            },
                        };
                        region_polygons
                            .get_mut(polygon_index)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "coverage hole shell index is unknown".into(),
                                )
                            })?
                            .holes
                            .push(hole.vertices);
                    }
                    polygons.extend(region_polygons);
                }
            }
            output_rows.push(OutputRow {
                logical_row: row.output_row,
                polygons,
            });
        }
        if target == StorageTarget::Scalar {
            output_rows.truncate(1);
        }

        let mut request_rows = Vec::new();
        for row in &mut output_rows {
            for polygon in &mut row.polygons {
                for ring in std::iter::once(&mut polygon.shell).chain(&mut polygon.holes) {
                    for vertex in ring {
                        work.charge_into_error(1)?;
                        vertex.request_row = request_rows.len();
                        let requests = match vertex.node {
                            Some(node) => node_roots
                                .get(node)
                                .and_then(Option::as_ref)
                                .ok_or_else(|| {
                                    GeometryErrorKind::Invalid(
                                        "coverage output references an unknown Q node".into(),
                                    )
                                })?
                                .iter()
                                .filter(|root| root.emitted)
                                .map(|root| {
                                    RootRequest::new_with_work(
                                        root.root,
                                        root.parameter.try_clone_with_work(work)?,
                                        work,
                                    )
                                })
                                .collect::<Result<Vec<_>>>()?,
                            None => Vec::new(),
                        };
                        request_rows.push(RequestRowInput {
                            axes: declared_axes,
                            requests,
                        });
                    }
                }
            }
        }
        let resolved = resolve_request_rows(
            &request_rows,
            &CoverageRoots {
                topology: plans.topology,
                frame,
            },
            context.metric_access(),
            work,
        )?;

        let mut output_shapes = Vec::with_capacity(output_rows.len());
        let mut row_map = Vec::with_capacity(output_rows.len());
        let mut literals = Vec::new();
        let mut source_root_uses = Vec::new();
        for row in output_rows {
            let logical_row = row.logical_row;
            let mut axes = declared_axes;
            let mut has_vertices = false;
            for polygon in &row.polygons {
                for ring in std::iter::once(&polygon.shell).chain(&polygon.holes) {
                    for vertex in ring {
                        has_vertices = true;
                        let ordinates = resolved.get(vertex.request_row).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "coverage output vertex has no resolved source ordinates".into(),
                            )
                        })?;
                        axes = CoordinateAxes::new(
                            crate::geometry::HasZ(axes.has_z() && ordinates.z.value().is_some()),
                            crate::geometry::HasM(axes.has_m() && ordinates.m.value().is_some()),
                        );
                    }
                }
            }
            if !has_vertices {
                axes = CoordinateAxes::XY;
            }

            let mut output_polygons = Vec::with_capacity(row.polygons.len());
            let mut literal_polygons = Vec::with_capacity(row.polygons.len());
            for (output, polygon) in row.polygons.into_iter().enumerate() {
                let output = u32::try_from(output).map_err(|_| ResourceError::SizeOverflow)?;
                let (shell, shell_layout) = write_ring(
                    polygon.shell,
                    axes,
                    output,
                    logical_row,
                    &node_roots,
                    &resolved,
                    &mut literals,
                    &mut source_root_uses,
                    work,
                )?;
                let mut holes = Vec::with_capacity(polygon.holes.len());
                let mut hole_layouts = Vec::with_capacity(polygon.holes.len());
                for hole in polygon.holes {
                    let (hole, layout) = write_ring(
                        hole,
                        axes,
                        output,
                        logical_row,
                        &node_roots,
                        &resolved,
                        &mut literals,
                        &mut source_root_uses,
                        work,
                    )?;
                    holes.push(hole);
                    hole_layouts.push(layout);
                }
                output_polygons.push(Polygon::new(shell, holes));
                literal_polygons.push(LiteralPolygon {
                    output,
                    axes,
                    shell: shell_layout,
                    holes: hole_layouts,
                });
            }

            let shape = Shape::narrow_polygons(axes, output_polygons);
            row_map.push(Some(LiteralRowLayout {
                operation: 0,
                logical_row,
                group_row: None,
                value_row: None,
                layout: ShapeLiteralLayout {
                    points: Vec::new(),
                    lines: Vec::new(),
                    polygons: literal_polygons,
                },
            }));
            output_shapes.push(shape);
        }

        let buffers = match target {
            StorageTarget::Array => {
                let mut writer = StreamingShapes::with_capacity(0, output_shapes.len());
                for shape in output_shapes {
                    writer.try_push(shape)?;
                }
                let (storage, ..) = writer.finish_parts()?;
                FrozenLiteralBuffers::rows(
                    Arc::new(storage),
                    None,
                    StorageLayout::PresentOnly,
                    row_map,
                    work,
                )?
            },
            StorageTarget::Scalar => {
                if output_shapes.len() > 1 || row_map.len() > 1 {
                    return Err(GeometryErrorKind::Invalid(
                        "scalar coverage boundary receipt has more than one output row".into(),
                    )
                    .into());
                }
                let shape = output_shapes
                    .into_iter()
                    .next()
                    .unwrap_or_else(Shape::empty_polygon);
                let layout = row_map.into_iter().next().flatten().map_or(
                    ShapeLiteralLayout {
                        points: Vec::new(),
                        lines: Vec::new(),
                        polygons: Vec::new(),
                    },
                    |row| row.layout,
                );
                FrozenLiteralBuffers::shapes(shape, layout)
            },
            StorageTarget::Groups => {
                return Err(GeometryErrorKind::Invalid(
                    "coverage boundary receipts do not publish grouped rows".into(),
                )
                .into());
            },
        };
        Ok(ProducerDrawingReceipt::new(
            buffers,
            literals,
            source_root_uses,
            Vec::new(),
        ))
    }
}

fn output_ring(
    topology: &ExactGeographicTopology,
    cycle: &GeographicCoverageBoundaryCycle,
    poles: SelectedRegionPoles,
    frame: GeographicFrame,
    port_points: &[Option<Point>],
    live_spans: &[bool],
    work: &mut impl Work,
) -> Result<OutputRing> {
    let first = cycle.uses.first().ok_or_else(|| {
        GeometryErrorKind::Invalid("coverage output boundary cycle has no spans".into())
    })?;
    let first_port = first.from;
    let first_presentation = first.from_presentation;
    let mut ports = Vec::with_capacity(cycle.uses.len().saturating_add(2));
    ports.push((first_port, first_presentation));
    let mut last_port = first_port;
    let mut has_shell_source = false;
    let mut has_hole_source = false;
    let mut source_north_pole = false;
    let mut source_south_pole = false;
    for use_ in &cycle.uses {
        if !live_spans
            .get(use_.span.0 as usize)
            .copied()
            .unwrap_or(false)
        {
            return Err(GeometryErrorKind::Invalid(
                "coverage boundary cycle references a non-live span".into(),
            )
            .into());
        }
        if use_.from != last_port {
            let previous = topology.q.ports.get(last_port.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage cycle has an unknown previous port".into())
            })?;
            let next = topology.q.ports.get(use_.from.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage cycle has an unknown next port".into())
            })?;
            if previous.node != next.node {
                return Err(GeometryErrorKind::Invalid(
                    "coverage boundary cycle changes Q nodes between spans".into(),
                )
                .into());
            }
            ports.push((use_.from, use_.from_presentation));
            last_port = use_.from;
        }
        if use_.to != last_port {
            ports.push((use_.to, use_.to_presentation));
            last_port = use_.to;
        }
        for root_use in &use_.root_uses {
            work.charge_into_error(1)?;
            let edge = topology
                .sources
                .edges
                .get(root_use.source_edge.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "coverage boundary source root is outside its inventory".into(),
                    )
                })?;
            if let SourceEdgeKind::Ring { ring, hole, .. } = edge.kind {
                if hole {
                    has_hole_source = true;
                } else {
                    has_shell_source = true;
                }
                if root_use.emitted
                    && let Some(FullTurnPole::Bounded(pole)) = topology
                        .sources
                        .rings
                        .get(ring.0 as usize)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "coverage boundary source ring is outside its inventory".into(),
                            )
                        })?
                        .summary
                        .full_turn_pole()
                {
                    match pole {
                        ChartPole::North => source_north_pole = true,
                        ChartPole::South => source_south_pole = true,
                    }
                }
            }
        }
    }
    if last_port != first_port {
        let previous = topology.q.ports.get(last_port.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("coverage cycle has an unknown closing port".into())
        })?;
        let first_port_record = topology.q.ports.get(first_port.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("coverage cycle has an unknown first port".into())
        })?;
        if previous.node != first_port_record.node {
            return Err(GeometryErrorKind::Invalid(
                "coverage boundary cycle does not close in Q".into(),
            )
            .into());
        }
        ports.push((first_port, first_presentation));
    }

    let mut vertices = Vec::with_capacity(ports.len());
    let mut xs = Vec::with_capacity(ports.len());
    let mut ys = Vec::with_capacity(ports.len());
    for (port_id, presentation) in ports {
        work.charge_into_error(1)?;
        let port =
            topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage output port is unknown".into())
            })?;
        if presentation.port != port_id
            || presentation.node != port.node
            || presentation.seam_cut != port.cut
        {
            return Err(GeometryErrorKind::Invalid(
                "coverage boundary port presentation changed its Q identity".into(),
            )
            .into());
        }
        let node = topology.q.nodes.get(port.node.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("coverage output port has an unknown node".into())
        })?;
        if presentation.pole != node.pole {
            return Err(GeometryErrorKind::Invalid(
                "coverage boundary port presentation changed its pole role".into(),
            )
            .into());
        }
        let mut point = port_points
            .get(port_id.0 as usize)
            .copied()
            .flatten()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage output port was not admitted as live".into())
            })?;
        if let Some(pole) = presentation.pole {
            point.y = frame.chart_pole_latitude(pole);
        }
        let point = Point::new_raw_xy(point.x, point.y);
        xs.push(point.x);
        ys.push(point.y);
        vertices.push(OutputVertex {
            point,
            node: Some(presentation.node.0 as usize),
            request_row: 0,
        });
    }
    if vertices.len() < 4
        || vertices
            .first()
            .zip(vertices.last())
            .is_none_or(|(first, last)| {
                first.point.x.to_bits() != last.point.x.to_bits()
                    || first.point.y.to_bits() != last.point.y.to_bits()
            })
    {
        return Err(GeometryErrorKind::Invalid(
            "coverage output boundary is not a closed ring".into(),
        )
        .into());
    }
    let closure_pole = match (source_north_pole, source_south_pole) {
        (true, false) => Some(ChartPole::North),
        (false, true) => Some(ChartPole::South),
        (false, false) | (true, true) => [ChartPole::North, ChartPole::South]
            .into_iter()
            .find(|&pole| poles.includes(pole)),
    };
    if let Some(pole) = closure_pole {
        close_full_turn_at_pole(&mut vertices, &mut xs, &mut ys, pole, frame);
    }
    let pole_boundary = poles.includes(ChartPole::North) || poles.includes(ChartPole::South);
    let hole = match (
        exact_ring_area_sign_columns(&xs, &ys, vertices.len().saturating_sub(1)),
        frame.latitude_orientation_sign().is_sign_positive(),
        has_shell_source,
        has_hole_source,
        pole_boundary,
    ) {
        (AreaSign::Positive, true, ..)
        | (AreaSign::Negative, false, ..)
        | (AreaSign::Zero, _, true, false, _) => false,
        (AreaSign::Negative, true, ..)
        | (AreaSign::Positive, false, ..)
        | (AreaSign::Zero, _, false, true, _) => true,
        (AreaSign::Zero, _, _, _, true) => {
            return Err(GeometryErrorKind::Invalid(
                "polar coverage boundary has no source shell/hole role".into(),
            )
            .into());
        },
        (AreaSign::Zero, _, false, false, false) | (AreaSign::Zero, _, true, true, false) => {
            return Err(GeometryErrorKind::Invalid(
                "coverage boundary has no shell/hole winding".into(),
            )
            .into());
        },
    };
    Ok(OutputRing { vertices, hole })
}

fn whole_sphere_vertices(frame: GeographicFrame) -> Vec<OutputVertex> {
    let half = frame.longitude().half_period();
    let south = frame.chart_pole_latitude(crate::geographic::ChartPole::South);
    let north = frame.chart_pole_latitude(crate::geographic::ChartPole::North);
    [
        Point::new_raw_xy(-half, south),
        Point::new_raw_xy(-half / 2.0, south),
        Point::new_raw_xy(0.0, south),
        Point::new_raw_xy(half / 2.0, south),
        Point::new_raw_xy(half, south),
        Point::new_raw_xy(half, north),
        Point::new_raw_xy(half / 2.0, north),
        Point::new_raw_xy(0.0, north),
        Point::new_raw_xy(-half / 2.0, north),
        Point::new_raw_xy(-half, north),
        Point::new_raw_xy(-half, 0.0),
        Point::new_raw_xy(-half, south),
    ]
    .into_iter()
    .map(|point| OutputVertex {
        point,
        node: None,
        request_row: 0,
    })
    .collect()
}

fn close_full_turn_at_pole(
    vertices: &mut Vec<OutputVertex>,
    xs: &mut Vec<f64>,
    ys: &mut Vec<f64>,
    pole: ChartPole,
    frame: GeographicFrame,
) {
    let period = frame.longitude().period();
    let Some(first) = vertices.first().copied() else {
        return;
    };
    let Some(last) = vertices.get(vertices.len().saturating_sub(2)).copied() else {
        return;
    };
    if !same_topological_coordinate((last.point.x - first.point.x).abs(), period) {
        let ring_len = vertices.len().saturating_sub(1);
        let seam_start = (1..ring_len).find(|&start| {
            let previous = start.saturating_sub(1);
            vertices
                .get(previous)
                .zip(vertices.get(start))
                .is_some_and(|(before, after)| {
                    before.node.is_some()
                        && before.node == after.node
                        && before.point.y.to_bits() == after.point.y.to_bits()
                        && same_topological_coordinate(
                            (after.point.x - before.point.x).abs(),
                            period,
                        )
                })
        });
        let Some(seam_start) = seam_start else {
            return;
        };
        vertices.truncate(ring_len);
        xs.truncate(ring_len);
        ys.truncate(ring_len);
        vertices.rotate_left(seam_start);
        xs.rotate_left(seam_start);
        ys.rotate_left(seam_start);
        let Some(first) = vertices.first().copied() else {
            return;
        };
        vertices.push(first);
        xs.push(first.point.x);
        ys.push(first.point.y);
    }

    let Some(first) = vertices.first().copied() else {
        return;
    };
    let Some(last) = vertices.get(vertices.len().saturating_sub(2)).copied() else {
        return;
    };
    if !same_topological_coordinate((last.point.x - first.point.x).abs(), period) {
        return;
    }
    let pole_y = frame.chart_pole_latitude(pole);
    // A closed source ring can wind a full longitude turn while its stored
    // endpoint aliases the start. The pole edge makes that winding explicit
    // to later geometry admission without assigning invented source ordinates.
    vertices.pop();
    xs.pop();
    ys.pop();
    vertices.push(OutputVertex {
        point: Point::new_raw_xy(last.point.x, pole_y),
        node: None,
        request_row: 0,
    });
    xs.push(last.point.x);
    ys.push(pole_y);
    vertices.push(OutputVertex {
        point: Point::new_raw_xy(first.point.x, pole_y),
        node: None,
        request_row: 0,
    });
    xs.push(first.point.x);
    ys.push(pole_y);
    vertices.push(first);
    xs.push(first.point.x);
    ys.push(first.point.y);
}

fn port_point(
    topology: &ExactGeographicTopology,
    port: QPortId,
    frame: GeographicFrame,
    work: &mut impl Work,
) -> Result<Point> {
    let port = topology
        .q
        .ports
        .get(port.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("coverage live port is unknown".into()))?;
    let x = port.point.x.round_with_work(work)?;
    let y = port.point.y.round_with_work(work)?;
    let latitude = Latitude::try_new(frame, y)
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    Ok(Point::new_raw_xy(x, latitude.get()))
}

fn write_ring(
    vertices: Vec<OutputVertex>,
    axes: CoordinateAxes,
    output: u32,
    logical_row: usize,
    node_roots: &[Option<Vec<RootParameter>>],
    resolved: &[NodeOrdinates],
    literals: &mut Vec<ProducerLiteral>,
    source_root_uses: &mut Vec<ProducerSourceRootUse>,
    work: &mut impl Work,
) -> Result<(Ring, LiteralSequence<ProducerImageId>)> {
    let mut points = Vec::with_capacity(vertices.len());
    let mut images = Vec::with_capacity(vertices.len());
    for vertex in vertices {
        work.charge_into_error(1)?;
        let ordinates = resolved.get(vertex.request_row).ok_or_else(|| {
            GeometryErrorKind::Invalid("coverage output lost a vertex's ordinates".into())
        })?;
        let z = if axes.has_z() {
            Some(ordinates.z.value().ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "coverage retained a Z axis without a source value".into(),
                )
            })?)
        } else {
            None
        };
        let m = if axes.has_m() {
            Some(ordinates.m.value().ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "coverage retained an M axis without a source value".into(),
                )
            })?)
        } else {
            None
        };
        let point = Point::new_raw_axes(vertex.point.x, vertex.point.y, ZOrdinate(z), MOrdinate(m));
        let image = ProducerImageId::new(literals.len());
        let address = LiteralAddress {
            operation: 0,
            logical_row,
            group_row: None,
            value_row: None,
            output,
        };
        literals.push(ProducerLiteral::from_point(address, image, point));
        if let Some(node) = vertex.node {
            let roots = node_roots
                .get(node)
                .and_then(Option::as_ref)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("coverage literal has an inactive Q node".into())
                })?;
            for root in roots {
                work.charge_into_error(1)?;
                source_root_uses.push(ProducerSourceRootUse {
                    address,
                    image,
                    range: RootRange::point_with_work(
                        root.root,
                        root.parameter.try_clone_with_work(work)?,
                        ExactRational::zero(),
                        work,
                    )?,
                });
            }
        }
        points.push(point);
        images.push(image);
    }
    Ok((
        Ring::try_from_closed(CoordSeq::try_from_points(&points)?)?,
        LiteralSequence {
            output,
            axes,
            images,
        },
    ))
}

fn add_endpoint_roots(
    node_roots: &mut [Option<Vec<RootParameter>>],
    node: QNodeId,
    root_uses: &[GeographicCoverageRootUse],
    start: bool,
    work: &mut impl Work,
) -> Result<()> {
    let roots = node_roots
        .get_mut(node.0 as usize)
        .and_then(Option::as_mut)
        .ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "coverage boundary root references an inactive Q node".into(),
            )
        })?;
    for root_use in root_uses {
        work.charge_into_error(1)?;
        roots.push(RootParameter {
            root: RootId::from_index(root_use.source_edge.0),
            parameter: if start {
                root_use.source_range.start.try_clone_with_work(work)?
            } else {
                root_use.source_range.end.try_clone_with_work(work)?
            },
            emitted: root_use.emitted,
        });
    }
    Ok(())
}
