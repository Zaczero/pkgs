use pyo3::IntoPyObjectExt as _;

use super::adapters::{
    geometry_point, input_crs_str, input_frame, input_len, is_missing, point_operand,
    require_geographic_model, row_zero,
};
use super::kernels::{cross_track_endpoints, to_geodesic_point};
use super::rows::{
    PointOperand, PreparedPointOperand, point_operand_input, row_missing, union_missing_masks,
};
use super::{
    Bound, Frame, GeographicFrame, GeometryInput, MetricOp, Point, Py, PyAny, PyErr, PyGeometry,
    PyResult, Python, ValidityMask, classify_required, crs, ensure_same_len,
    expected_geometry_or_array, float64_array, pyfunction,
};

fn point_cross_track_distance(
    geometry: &PyGeometry,
    start: &PyGeometry,
    end: &PyGeometry,
) -> PyResult<f64> {
    const OP: MetricOp = MetricOp::CrossTrackDistance;
    let probe = point_operand(geometry, OP)?;
    let from = point_operand(start, OP)?;
    let to = point_operand(end, OP)?;
    geometry.frame.compatible(&start.frame, OP.name())?;
    geometry.frame.compatible(&end.frame, OP.name())?;
    let crs::MetricModel::Geodesic { crs, frame, state } =
        require_geographic_model(geometry.crs_str(), OP)?
    else {
        unreachable!("a geographic model is geodesic");
    };
    let (Some(probe), Some(from), Some(to)) = (probe, from, to) else {
        return Ok(f64::NAN);
    };
    cross_track_points(&crs, frame, probe, from, to, &state)
}

fn cross_track_points(
    crs: &str,
    frame: GeographicFrame,
    probe: Point,
    from: Point,
    to: Point,
    state: &crs::RuntimeState,
) -> PyResult<f64> {
    let (from, to) = cross_track_endpoints(frame, from, to)?;
    let probe = to_geodesic_point(frame, probe)?;
    Ok(crs::geodesic_cross_track_crs_at(
        crs,
        (probe.x, probe.y),
        (from.x, from.y),
        (to.x, to.y),
        state,
    )?)
}

fn cross_track_points_with_radius(
    radius: f64,
    frame: GeographicFrame,
    probe: Point,
    from: Point,
    to: Point,
) -> PyResult<f64> {
    let (from, to) = cross_track_endpoints(frame, from, to)?;
    let probe = to_geodesic_point(frame, probe)?;
    Ok(crs::geodesic_cross_track_with_radius(
        radius,
        (probe.x, probe.y),
        (from.x, from.y),
        (to.x, to.y),
    )?)
}

fn cross_track_point_rows<const HAS_MISSING: bool>(
    radius: f64,
    frame: GeographicFrame,
    points: &PreparedPointOperand<'_>,
    starts: &PreparedPointOperand<'_>,
    ends: &PreparedPointOperand<'_>,
    len: usize,
    missing: Option<&ValidityMask>,
) -> PyResult<Vec<f64>> {
    (0..len)
        .map(|row| {
            if row_missing::<HAS_MISSING>(missing, row) {
                return Ok(f64::NAN);
            }
            let (p, s, e) = (points.get(row), starts.get(row), ends.get(row));
            cross_track_points_with_radius(radius, frame, p, s, e)
                .map_err(|err| crate::note_array_row(err, row))
        })
        .collect()
}

fn cross_track_point_rows_dispatch(
    radius: f64,
    frame: GeographicFrame,
    points: PointOperand<'_>,
    starts: PointOperand<'_>,
    ends: PointOperand<'_>,
    len: usize,
    missing: Option<&ValidityMask>,
) -> PyResult<Vec<f64>> {
    let points = points.prepare();
    let starts = starts.prepare();
    let ends = ends.prepare();
    missing.map_or_else(
        || cross_track_point_rows::<false>(radius, frame, &points, &starts, &ends, len, None),
        |mask| {
            cross_track_point_rows::<true>(radius, frame, &points, &starts, &ends, len, Some(mask))
        },
    )
}

fn cross_track_broadcast(
    py: Python<'_>,
    point: &Bound<'_, PyAny>,
    start: &Bound<'_, PyAny>,
    end: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    const OP: MetricOp = MetricOp::CrossTrackDistance;
    use GeometryInput::{Many, One};
    let op = point_cross_track_distance;
    let point_in = classify_required(point)?;
    let start_in = classify_required(start)?;
    let end_in = classify_required(end)?;

    let mut broadcast_len = None;
    for len in [&point_in, &start_in, &end_in]
        .into_iter()
        .filter_map(|input| input_len(*input))
    {
        if let Some(previous) = broadcast_len {
            ensure_same_len(previous, len)?;
        } else {
            broadcast_len = Some(len);
        }
    }
    let row_at = |input: &GeometryInput<'_>, row: usize| -> PyResult<PyGeometry> {
        Ok(match input {
            One(geometry) => (*geometry).clone(),
            Many(array) => array.geometry_at(row).map_err(crate::PyErr::from)?,
        })
    };
    match (&point_in, &start_in, &end_in) {
        (One(p), One(s), One(e)) if broadcast_len.is_none() => {
            p.frame.compatible(&s.frame, OP.name())?;
            p.frame.compatible(&e.frame, OP.name())?;
            match (geometry_point(p), geometry_point(s), geometry_point(e)) {
                (Some(probe), Some(start), Some(end)) => {
                    let model = require_geographic_model(p.crs_str(), OP)?;
                    let metric = crs::GeodesicMetric::from_model(&model)?;
                    cross_track_points(&metric.crs, metric.frame, probe, start, end, &metric.state)?
                        .into_py_any(py)
                },
                _ => op(p, s, e)?.into_py_any(py),
            }
        },
        _ if broadcast_len.is_some() => {
            let len = broadcast_len.expect("checked above");
            let (point_crs, point_epoch) = input_frame(&point_in);
            let (start_crs, start_epoch) = input_frame(&start_in);
            let (end_crs, end_epoch) = input_frame(&end_in);
            Frame::compatible_parts(point_crs, point_epoch, start_crs, start_epoch, OP.name())?;
            Frame::compatible_parts(point_crs, point_epoch, end_crs, end_epoch, OP.name())?;
            let missing = union_missing_masks([point_in, start_in, end_in]);
            if let (Some(points), Some(starts), Some(ends)) = (
                point_operand_input(point_in),
                point_operand_input(start_in),
                point_operand_input(end_in),
            ) {
                let model =
                    require_geographic_model(input_crs_str(&point_in), OP).map_err(row_zero)?;
                let metric = crs::GeodesicMetric::from_model(&model)
                    .map_err(PyErr::from)
                    .map_err(row_zero)?;
                let radius = crs::geodesic_cross_track_radius_crs_at(&metric.crs, &metric.state)
                    .map_err(PyErr::from)
                    .map_err(row_zero)?;
                return float64_array(
                    py,
                    py.detach(move || {
                        cross_track_point_rows_dispatch(
                            radius,
                            metric.frame,
                            points,
                            starts,
                            ends,
                            len,
                            missing.as_ref(),
                        )
                    })?,
                );
            }
            let model = require_geographic_model(point_crs.map(smol_str::SmolStr::as_str), OP)?;
            let metric = crs::GeodesicMetric::from_model(&model)?;
            float64_array(
                py,
                py.detach(move || {
                    (0..len)
                        .map(|row| {
                            if is_missing(missing.as_ref(), row) {
                                return Ok(f64::NAN);
                            }
                            let (p, s, e) = (
                                row_at(&point_in, row)?,
                                row_at(&start_in, row)?,
                                row_at(&end_in, row)?,
                            );
                            let (Some(probe), Some(start), Some(end)) = (
                                point_operand(&p, OP)?,
                                point_operand(&s, OP)?,
                                point_operand(&e, OP)?,
                            ) else {
                                return Ok(f64::NAN);
                            };
                            cross_track_points(
                                &metric.crs,
                                metric.frame,
                                probe,
                                start,
                                end,
                                &metric.state,
                            )
                            .map_err(|err| crate::note_array_row(err, row))
                        })
                        .collect::<PyResult<Vec<_>>>()
                })?,
            )
        },
        _ => Err(expected_geometry_or_array()),
    }
}

/// Signed distance from a point to the great circle through ``start`` and
/// ``end``, in meters.
///
/// Positive when the point lies left of the directed path ``start -> end``,
/// negative right, zero on it. Spherical on the CRS ellipsoid's mean radius.
/// Geographic CRS only. An empty point yields ``nan``.
///
/// Parameters
/// ----------
/// point, start, end : Point or GeometryArray
///     The probe point and two distinct endpoints defining the directed path;
///     all must share a CRS.
///
/// Returns
/// -------
/// float or numpy.ndarray
///     Signed meters off-path.
///
/// Raises
/// ------
/// GeometryTypeError
///     If any operand is not a `Point`.
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
/// CRSError
///     If the CRS is missing or not geographic.
/// InvalidGeometryError
///     If ``start`` and ``end`` are coincident or antipodal and therefore do
///     not define a unique great circle.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> a, b = gm.Point(0, 0, crs=4326), gm.Point(1, 0, crs=4326)
/// >>> probe = gm.Point(0.5, 0.1, crs=4326)
/// >>> round(gm.cross_track_distance(probe, a, b) / 1000, 1)
/// 11.1
#[pyfunction]
pub(crate) fn cross_track_distance(
    py: Python<'_>,
    point: &Bound<'_, PyAny>,
    start: &Bound<'_, PyAny>,
    end: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    cross_track_broadcast(py, point, start, end)
}
