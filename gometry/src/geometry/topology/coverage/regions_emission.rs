use std::sync::Arc;

use super::super::exact::SourceEdgeKind;
use super::super::q::{ExactGeographicTopology, QNodeId};
use super::regions::{GeographicCoverageBoundaryCycle, GeographicCoverageRootUse};
use super::{GeographicCoverageAnalysis, GeographicCoverageFaceAssignment};
use crate::array::StreamingShapes;
use crate::error::{ResourceError, Result};
use crate::geographic::{GeographicFrame, Latitude};
use crate::geometry::embedding_completion::{
    FrozenLiteralBuffers, LiteralAddress, LiteralPolygon, LiteralRowLayout, LiteralSequence,
    ShapeLiteralLayout,
};
use crate::geometry::ordinates::{
    NodeOrdinates, RequestRowInput, RootCatalog, RootId, RootMetricState, RootRange, RootRequest,
    SourceArcMetricAccess, resolve_request_rows, resolve_source_root,
};
use crate::geometry::publication::{
    ProducerDrawingReceipt, ProducerImageId, ProducerLiteral, ProducerSourceRootUse,
};
use crate::geometry::storage::StorageLayout;
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::{
    ChartMetric, CoordSeq, CoordinateAxes, GeometryErrorKind, MOrdinate, Point, Polygon, Ring,
    Shape, Work, ZOrdinate,
};

struct RootParameter {
    root: RootId,
    parameter: ExactRational,
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
    polygons: Vec<OutputPolygon>,
}

struct OutputRing {
    vertices: Vec<OutputVertex>,
    hole: bool,
}

impl GeographicCoverageAnalysis {
    pub(in crate::geometry) fn cleaning_drawing_receipt<M: ChartMetric>(
        &self,
        assignment: &GeographicCoverageFaceAssignment,
        declared_axes: CoordinateAxes,
        context: &crate::geometry::SourceArcContext<'_, '_, M>,
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
        let mut node_roots: Vec<Vec<RootParameter>> = std::iter::repeat_with(Vec::new)
            .take(plans.topology.q.nodes.len())
            .collect();
        for row in &plans.rows {
            for region in &row.regions {
                for cycle in &region.cycles {
                    for use_ in &cycle.uses {
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

        let mut output_rows = Vec::with_capacity(plans.rows.len());
        for row in &plans.rows {
            work.charge_into_error(1)?;
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
                    for cycle in &region.cycles {
                        let ring = output_ring(plans.topology, cycle, frame, work)?;
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
                            if crate::geometry::predicates::ring_contains_interior(&shell, probe) {
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
            output_rows.push(OutputRow { polygons });
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
                                .ok_or_else(|| {
                                    GeometryErrorKind::Invalid(
                                        "coverage output references an unknown Q node".into(),
                                    )
                                })?
                                .iter()
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

        let mut writer = StreamingShapes::with_capacity(0, output_rows.len());
        let mut row_map = Vec::with_capacity(output_rows.len());
        let mut literals = Vec::new();
        let mut source_root_uses = Vec::new();
        for (logical_row, row) in output_rows.into_iter().enumerate() {
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
            writer.try_push(shape)?;
        }

        let (storage, ..) = writer.finish_parts()?;
        let buffers = FrozenLiteralBuffers::rows(
            Arc::new(storage),
            None,
            StorageLayout::PresentOnly,
            row_map,
            work,
        )?;
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
    frame: GeographicFrame,
    work: &mut impl Work,
) -> Result<OutputRing> {
    let first = cycle.uses.first().ok_or_else(|| {
        GeometryErrorKind::Invalid("coverage output boundary cycle has no spans".into())
    })?;
    let first_port = first.from;
    let mut ports = Vec::with_capacity(cycle.uses.len().saturating_add(2));
    ports.push(first_port);
    let mut last_port = first_port;
    let mut has_shell_source = false;
    let mut has_hole_source = false;
    for use_ in &cycle.uses {
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
            ports.push(use_.from);
            last_port = use_.from;
        }
        if use_.to != last_port {
            ports.push(use_.to);
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
            if let SourceEdgeKind::Ring { hole, .. } = edge.kind {
                if hole {
                    has_hole_source = true;
                } else {
                    has_shell_source = true;
                }
            }
        }
    }
    if last_port != first_port {
        let previous = topology.q.ports.get(last_port.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("coverage cycle has an unknown closing port".into())
        })?;
        let first = topology.q.ports.get(first_port.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("coverage cycle has an unknown first port".into())
        })?;
        if previous.node != first.node {
            return Err(GeometryErrorKind::Invalid(
                "coverage boundary cycle does not close in Q".into(),
            )
            .into());
        }
        ports.push(first_port);
    }

    let mut vertices = Vec::with_capacity(ports.len());
    let mut xs = Vec::with_capacity(ports.len());
    let mut ys = Vec::with_capacity(ports.len());
    for port_id in ports {
        work.charge_into_error(1)?;
        let port =
            topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage output port is unknown".into())
            })?;
        let x = port.point.x.round_with_work(work)?;
        let y = port.point.y.round_with_work(work)?;
        let latitude = Latitude::try_new(frame, y)
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        let point = Point::new_raw_xy(x, latitude.get());
        xs.push(point.x);
        ys.push(point.y);
        vertices.push(OutputVertex {
            point,
            node: Some(port.node.0 as usize),
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
    let sign = crate::geometry::area::exact_ring_area_sign_columns(
        &xs,
        &ys,
        vertices.len().saturating_sub(1),
    );
    let hole = match sign {
        crate::geometry::area::AreaSign::Positive
            if frame.latitude_orientation_sign().is_sign_positive() =>
        {
            false
        },
        crate::geometry::area::AreaSign::Negative
            if frame.latitude_orientation_sign().is_sign_positive() =>
        {
            true
        },
        crate::geometry::area::AreaSign::Negative
            if frame.latitude_orientation_sign().is_sign_negative() =>
        {
            false
        },
        crate::geometry::area::AreaSign::Positive
            if frame.latitude_orientation_sign().is_sign_negative() =>
        {
            true
        },
        crate::geometry::area::AreaSign::Zero if has_hole_source != has_shell_source => {
            has_hole_source
        },
        _ => {
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

fn write_ring(
    vertices: Vec<OutputVertex>,
    axes: CoordinateAxes,
    output: u32,
    logical_row: usize,
    node_roots: &[Vec<RootParameter>],
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
            let roots = node_roots.get(node).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage literal has an unknown Q node".into())
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
    node_roots: &mut [Vec<RootParameter>],
    node: QNodeId,
    root_uses: &[GeographicCoverageRootUse],
    start: bool,
    work: &mut impl Work,
) -> Result<()> {
    let roots = node_roots.get_mut(node.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("coverage boundary root references an unknown Q node".into())
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
        });
    }
    Ok(())
}
