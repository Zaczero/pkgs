use super::sites::{EmittedVertex, with_vertex_axes};
use super::*;
use crate::geometry::XY;

pub(in crate::geometry::topology) fn with_publication_axes(
    point: XY,
    ordinates: NodeOrdinates,
    axes: CoordinateAxes,
) -> Result<Point> {
    let z = if axes.has_z() {
        Some(ordinates.z.value().ok_or_else(|| {
            GeometryErrorKind::Invalid("source publication Z ordinate is missing".into())
        })?)
    } else {
        None
    };
    let m = if axes.has_m() {
        Some(ordinates.m.value().ok_or_else(|| {
            GeometryErrorKind::Invalid("source publication M ordinate is missing".into())
        })?)
    } else {
        None
    };
    Ok(Point::new_raw_axes(
        point.x,
        point.y,
        ZOrdinate(z),
        MOrdinate(m),
    ))
}

pub(in crate::geometry::topology) fn with_assigned_source_vertex_axes(
    vertex: EmittedVertex<'_>,
    ordinates: &[NodeOrdinates],
    source_ports: Option<&[Option<ExactChartPoint>]>,
    axes: CoordinateAxes,
    work: &mut impl Work,
) -> Result<Point> {
    let point = match (vertex.port.cut, source_ports) {
        (true, Some(source_ports)) => {
            let image = source_ports
                .get(vertex.port.id.0 as usize)
                .and_then(Option::as_ref)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "assigned Q source cut has no checked chart image".into(),
                    )
                })?;
            work.charge_into_error(1)?;
            Point::new_raw_xy(image.x.round()?, image.y.round()?)
        },
        _ => vertex.point,
    };
    with_vertex_axes(
        EmittedVertex {
            port: vertex.port,
            point,
            literal_image: vertex.literal_image,
        },
        ordinates,
        axes,
    )
}

pub(in crate::geometry::topology) const fn source_literal_image(
    vertex: EmittedVertex<'_>,
    source_ports: Option<&[Option<ExactChartPoint>]>,
) -> QSourceImage {
    if vertex.port.cut && source_ports.is_some() {
        QSourceImage::Port(vertex.port.id)
    } else {
        vertex.literal_image
    }
}
