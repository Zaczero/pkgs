use super::{
    CrsError, GeographicError, GeographicFrame, GeometryErrorKind, Result, TopologyDomain,
    TransformFailure, transform_in_core_xy_batch, transform_in_core_xy_batch_indexed,
};

pub(in crate::crs) fn transform_in_core_xy_with_domains(
    operation: &crate::crs::InCoreTransform,
    source_domain: TopologyDomain,
    target_domain: TopologyDomain,
    x: &mut [f64],
    y: &mut [f64],
) -> Result<()> {
    if let TopologyDomain::Geographic(frame) = source_domain
        && frame != GeographicFrame::crs84()
    {
        frame
            .to_geodesic_lanes(x, y)
            .map_err(map_geographic_conversion_error)?;
    }
    transform_in_core_xy_batch(operation, x, y)?;
    if let TopologyDomain::Geographic(frame) = target_domain {
        frame
            .lanes_from_geodesic(x, y)
            .map_err(map_geographic_conversion_error)?;
    }
    Ok(())
}

pub(in crate::crs) fn transform_in_core_xy_with_domains_indexed(
    operation: &crate::crs::InCoreTransform,
    source_domain: TopologyDomain,
    target_domain: TopologyDomain,
    x: &mut [f64],
    y: &mut [f64],
) -> std::result::Result<(), TransformFailure> {
    transform_in_core_xy_with_domains_indexed_mode(
        operation,
        source_domain,
        target_domain,
        x,
        y,
        TargetLongitudeOutput::Canonical,
    )
}

pub(in crate::crs) fn transform_in_core_xy_with_domains_indexed_for_shape(
    operation: &crate::crs::InCoreTransform,
    source_domain: TopologyDomain,
    target_domain: TopologyDomain,
    x: &mut [f64],
    y: &mut [f64],
) -> std::result::Result<(), TransformFailure> {
    transform_in_core_xy_with_domains_indexed_mode(
        operation,
        source_domain,
        target_domain,
        x,
        y,
        TargetLongitudeOutput::Continuous,
    )
}

#[derive(Clone, Copy)]
enum TargetLongitudeOutput {
    Canonical,
    Continuous,
}

fn transform_in_core_xy_with_domains_indexed_mode(
    operation: &crate::crs::InCoreTransform,
    source_domain: TopologyDomain,
    target_domain: TopologyDomain,
    x: &mut [f64],
    y: &mut [f64],
    target_longitude_output: TargetLongitudeOutput,
) -> std::result::Result<(), TransformFailure> {
    if let TopologyDomain::Geographic(frame) = source_domain
        && frame != GeographicFrame::crs84()
    {
        frame
            .to_geodesic_lanes_indexed(x, y)
            .map_err(|(coordinate, error)| {
                TransformFailure::at(map_geographic_conversion_error(error), coordinate)
            })?;
    }
    transform_in_core_xy_batch_indexed(operation, x, y)?;
    if let TopologyDomain::Geographic(frame) = target_domain {
        let result = match target_longitude_output {
            TargetLongitudeOutput::Canonical => frame.lanes_indexed_from_geodesic(x, y),
            TargetLongitudeOutput::Continuous => frame.lanes_unwrapped_from_geodesic_indexed(x, y),
        };
        result.map_err(|(coordinate, error)| {
            TransformFailure::at(map_geographic_conversion_error(error), coordinate)
        })?;
    }
    Ok(())
}

pub(in crate::crs) fn map_geographic_conversion_error(
    error: GeographicError,
) -> crate::error::Error {
    match error {
        GeographicError::LatitudeOutOfRange { value } => GeometryErrorKind::projection(format!(
            "latitude {value:e} is outside the geographic coordinate domain"
        )),
        GeographicError::NonFinite { .. } => GeometryErrorKind::NonFiniteCoordinate.into(),
        other @ (GeographicError::InvalidAngularFactor { .. }
        | GeographicError::ChartTurnOverflow
        | GeographicError::AmbiguousFullTurnRing { .. }) => CrsError::invalid(other.to_string()),
    }
}

pub(crate) fn validate_coordinate_slices(
    x: &[f64],
    y: &[f64],
    z: Option<&[f64]>,
    t: Option<&[f64]>,
) -> Result<()> {
    if let Some(other) = [Some(y.len()), z.map(<[f64]>::len), t.map(<[f64]>::len)]
        .into_iter()
        .flatten()
        .find(|&len| len != x.len())
    {
        return Err(GeometryErrorKind::CoordinateLength(x.len(), other).into());
    }
    if crate::geometry::column_all_finite(x)
        && crate::geometry::column_all_finite(y)
        && z.is_none_or(crate::geometry::column_all_finite)
        && t.is_none_or(crate::geometry::column_all_finite)
    {
        Ok(())
    } else {
        Err(GeometryErrorKind::NonFiniteCoordinate.into())
    }
}

pub(crate) fn validate_coordinate_lanes(
    x: &[f64],
    y: &[f64],
    zt: crate::ZtLaneRefs<'_>,
) -> Result<()> {
    match zt {
        crate::Zt::None => validate_coordinate_slices(x, y, None, None),
        crate::Zt::Z(z) => validate_coordinate_slices(x, y, Some(z), None),
        crate::Zt::T(t) => validate_coordinate_slices(x, y, None, Some(t)),
        crate::Zt::Zt { z, t } => validate_coordinate_slices(x, y, Some(z), Some(t)),
    }
}

pub(crate) fn coordinates_are_finite(
    x: &[f64],
    y: &[f64],
    z: Option<&[f64]>,
    t: Option<&[f64]>,
) -> bool {
    crate::geometry::column_all_finite(x)
        && crate::geometry::column_all_finite(y)
        && z.is_none_or(crate::geometry::column_all_finite)
        && t.is_none_or(crate::geometry::column_all_finite)
}
