use crate::error::Result;
use crate::geometry::{GeodesicMetric, Shape, SharedShapeOwners};

/// Validate linework operands used by continuous geodesic Hausdorff.
pub(crate) fn ensure_geodesic_linework(shape: &Shape, metric: &impl GeodesicMetric) -> Result<()> {
    ensure_geodesic_linework_inner(shape, metric, &mut SharedShapeOwners::for_shape(shape))
}

fn ensure_geodesic_linework_inner(
    shape: &Shape,
    metric: &impl GeodesicMetric,
    owners: &mut Option<SharedShapeOwners>,
) -> Result<()> {
    match shape {
        Shape::Polygon(_) | Shape::MultiPolygon(_) => Err(crate::crs::CrsError::message(
            "geodesic distance for polygon interiors is unsupported",
        )),
        Shape::GeometryCollection(geometries) => {
            if owners.as_mut().is_some_and(|owners| {
                !owners.first_collection(geometries.as_slice(), geometries.members_are_shared())
            }) {
                return Ok(());
            }
            for geometry in geometries {
                ensure_geodesic_linework_inner(geometry, metric, owners)?;
            }
            Ok(())
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Empty(..) => metric.ensure_shape(shape),
    }
}
