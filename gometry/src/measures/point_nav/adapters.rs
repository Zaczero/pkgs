use super::{
    CRSError, Crs, DistanceUnit, GeometryError, GeometryInput, MetricOp, Point, PyErr, PyGeometry,
    PyResult, Shape, ValidityMask, crs,
};

pub(super) fn geometry_point(geometry: &PyGeometry) -> Option<Point> {
    let Shape::Point(point) = geometry.shape.shape() else {
        return None;
    };
    Some(*point)
}

pub(super) fn row_zero(err: PyErr) -> PyErr {
    crate::note_array_row(err, 0)
}

pub(super) fn require_geographic_model(
    crs: Option<&str>,
    operation: MetricOp,
) -> PyResult<crs::MetricModel> {
    let operation = operation.name();
    match crs::metric_model(crs)? {
        model @ crs::MetricModel::Geodesic { .. } => Ok(model),
        crs::MetricModel::Planar { .. } => Err(CRSError::new_err(format!(
            "{operation} requires a geographic CRS; use with_crs(...) or to_crs(...) to attach one"
        ))),
    }
}

/// The point operand of a navigation call; `None` for POINT EMPTY, which
/// every navigation answers (NaN, infinity or POINT EMPTY) instead of raising.
pub(super) fn point_operand(geometry: &PyGeometry, operation: MetricOp) -> PyResult<Option<Point>> {
    Ok(geometry.shape.shape().point_operand(operation.name())?)
}

pub(super) fn input_len(input: GeometryInput<'_>) -> Option<usize> {
    match input {
        GeometryInput::One(_) => None,
        GeometryInput::Many(array) => Some(array.len()),
    }
}

pub(super) const fn input_frame<'a>(
    input: &'a GeometryInput<'a>,
) -> (Option<&'a Crs>, Option<f64>) {
    match input {
        GeometryInput::One(geometry) => (geometry.crs_ref(), geometry.epoch()),
        GeometryInput::Many(array) => (array.crs_ref(), array.epoch()),
    }
}

pub(super) fn input_crs_str<'a>(input: &'a GeometryInput<'a>) -> Option<&'a str> {
    match input {
        GeometryInput::One(geometry) => geometry.crs_str(),
        GeometryInput::Many(array) => array.crs_str(),
    }
}

pub(super) fn is_missing(mask: Option<&ValidityMask>, row: usize) -> bool {
    mask.is_some_and(|mask| mask.is_missing(row))
}

pub(crate) fn reject_rhumb_unit(unit: Option<DistanceUnit>, operation: MetricOp) -> PyResult<()> {
    if unit.is_some() {
        return Err(GeometryError::new_err(format!(
            "{} does not accept unit when path='rhumb'; rhumb distances are meters",
            operation.name()
        )));
    }
    Ok(())
}
