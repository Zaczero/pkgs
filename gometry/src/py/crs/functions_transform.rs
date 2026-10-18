use pyo3::exceptions::PyTypeError;

use crate::boundary::FloatInput;
use crate::check_i32_min;
use crate::py::crs::{
    Bound, CrsCoordinateArgs, IntoPyObject as _, Py, PyAny, PyAnyMethods as _, PyErr, PyResult,
    Python, TransformOptionArgs, crs, crs_normalize, finite_coordinate_required, pyfunction,
};
use crate::py::errors;
use crate::py::errors::{AccuracyWarning, GeometryError};
use crate::py::numpy::{float64_array, float64_matrix, reject_masked_array};

/// Drain the one diagnostic published by the shared PROJ pipeline seam.
///
/// Python-visible transform boundaries call this after their native batch has
/// completed, so a warning is emitted once per operation rather than once per
/// coordinate or from the `extern "C"` PROJ callback.
pub(crate) fn drain_accuracy_warning(
    py: Python<'_>,
    execution: &crs::TransformExecution,
) -> PyResult<()> {
    if let Some(message) = execution.take_diagnostic() {
        let message = std::ffi::CString::new(message)
            .map_err(|_| GeometryError::new_err("PROJ diagnostic contained a NUL byte"))?;
        PyErr::warn(py, &py.get_type::<AccuracyWarning>(), &message, 1)?;
    }
    Ok(())
}

/// Information about a transformation grid.
///
/// Parameters
/// ----------
/// name : str
///     Grid short name (e.g. ``'us_noaa_g2018u0.tif'``).
///
/// Returns
/// -------
/// dict
///     Local grid metadata and availability.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.crs_grid('null')['available']
/// True
#[pyfunction]
pub(crate) fn crs_grid(py: Python<'_>, name: &str) -> PyResult<crs::GridDatabaseInfo> {
    Ok(py.detach(|| crs::grid_info(name))?)
}

/// The selected coordinate operation between two CRS.
///
/// Parameters
/// ----------
/// source, target : CRS-like
///     Source and destination CRS-like inputs.
///
/// Returns
/// -------
/// dict
pub(crate) fn crs_operation<'py>(
    py: Python<'py>,
    source: &Bound<'_, PyAny>,
    target: &Bound<'_, PyAny>,
    opts: TransformOptionArgs<'_>,
) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
    let source = crs_normalize(source)?;
    let target = crs_normalize(target)?;
    let options = opts.parse()?;
    super::list_cache::crs_operation_py_dict(py, &source, &target, &options)
}

/// The coordinate operation between two CRS at a location.
///
/// Parameters
/// ----------
/// source, target : CRS-like
///     Source and destination CRS-like inputs.
/// coord : coordinate
///     A coordinate as ``(x, y)``.
///
/// Returns
/// -------
/// dict
pub(crate) fn crs_operation_at(
    py: Python<'_>,
    source: &Bound<'_, PyAny>,
    target: &Bound<'_, PyAny>,
    x: &Bound<'_, PyAny>,
    y: &Bound<'_, PyAny>,
    z: Option<&Bound<'_, PyAny>>,
    t: Option<&Bound<'_, PyAny>>,
    opts: TransformOptionArgs<'_>,
) -> PyResult<crs::OperationInfo> {
    let source = crs_normalize(source)?;
    let target = crs_normalize(target)?;
    let options = opts.parse()?;
    let x = finite_coordinate_required("x", x)?;
    let y = finite_coordinate_required("y", y)?;
    let z = z
        .filter(|value| !value.is_none())
        .map(|value| finite_coordinate_required("z", value))
        .transpose()?;
    let t = t
        .filter(|value| !value.is_none())
        .map(|value| finite_coordinate_required("t", value))
        .transpose()?;
    let zt = match (z, t) {
        (None, None) => crate::Zt::None,
        (Some(z), None) => crate::Zt::Z(z),
        (None, Some(t)) => crate::Zt::T(t),
        (Some(z), Some(t)) => crate::Zt::Zt { z, t },
    };
    // PROJ's operation selection is a database search: run it detached, like
    // the bulk transform paths in this file.
    Ok(py.detach(|| crs::operation_info_at(&source, &target, x, y, zt, &options))?)
}

/// Candidate coordinate operations between two CRS.
///
/// Parameters
/// ----------
/// source, target : CRS-like
///     Source and destination CRS-like inputs.
///
/// Returns
/// -------
/// list
pub(crate) fn crs_operations<'py>(
    py: Python<'py>,
    source: &Bound<'_, PyAny>,
    target: &Bound<'_, PyAny>,
    opts: TransformOptionArgs<'_>,
) -> PyResult<Bound<'py, pyo3::types::PyList>> {
    let source = crs_normalize(source)?;
    let target = crs_normalize(target)?;
    let options = opts.parse()?;
    super::list_cache::crs_operations_py_list(py, &source, &target, &options)
}

#[pyfunction(signature = (
    source,
    target,
    x,
    y,
    z = None,
    *,
    t = None,
    area_of_interest = None,
    source_epoch = None,
    target_epoch = None,
    authority = None,
    accuracy = None,
    allow_ballpark = None,
    only_best = None,
    force_over = false
))]
/// Reproject raw coordinates from one CRS to another.
///
/// For geometries use ``to_crs``; this is
/// the lower-level coordinate-column form.
///
/// Parameters
/// ----------
/// source, target : CRS-like
///     Source and target CRS-like inputs.
/// x, y : float or sequence of float
///     Coordinate columns (scalars transform a single point).
/// z : float or sequence of float, optional
///     Vertical column for 3D transforms.
/// t : float or sequence of float, optional
///     Coordinate epoch column.
/// area_of_interest : sequence of float, dict, or object, optional
///     Area of interest guiding operation selection, as
///     ``(west, south, east, north)`` in DEGREES, an area mapping, or an
///     AreaOfInterest-like object.
/// source_epoch, target_epoch : numeric scalar or 0-D numeric array, optional
///     Coordinate epochs for dynamic CRS.
/// authority : str, optional
///     Restrict candidate coordinate operations to this authority
///     (e.g. ``'EPSG'``).
///
/// accuracy : float, optional
///     Maximum acceptable operation accuracy, in meters.
///
/// allow_ballpark : bool, optional
///     Allow low-accuracy ballpark operations when no precise one exists.
///
/// only_best : bool, optional
///     Require PROJ's best operation. If a required transformation grid is
///     unavailable, raise ``TransformError`` instead of using a less accurate
///     fallback operation.
///
/// force_over : bool, optional
///     Keep coordinates on the source side of the antimeridian instead of
///     wrapping into ``[-180, 180]`` (PROJ ``FORCE_OVER=YES``). Like
///     ``only_best``, this also collapses operation selection to a single
///     candidate, so enumerating surfaces return exactly one operation.
///
/// Returns
/// -------
/// tuple or numpy.ndarray
///     Scalars in, scalars out: ``(x, y)`` or ``(x, y, z)``. Lane input returns
///     a read-only ``(N, 2)``/``(N, 3)`` ``float64`` matrix (interleaved, the
///     same shape as a NumPy matrix exported from ``geometry.coords``), so
///     ``result[:, 0]`` / ``result[:, 1]``
///     read the transformed columns directly. The input epoch ``t`` is not a
///     transformed spatial ordinate and is not returned (use `apply` if you
///     need the raw columns echoed back).
///
/// Raises
/// ------
/// CRSError
///     If ``source``/``target`` are unrecognized.
/// TransformError
///     If the transform is undefined or fails to apply.
/// InvalidGeometryError
///     If coordinate columns are non-finite or differ in length.
/// GeometryError
///     If an epoch option is invalid.
///
/// See Also
/// --------
/// Geometry.to_crs : Reproject a geometry.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> import numpy as np
/// >>> np.round(np.asarray(gm.crs_transform(4326, 3857, -122.4, 37.8)), 1).tolist()
/// [-13625505.7, 4551210.9]
#[expect(
    clippy::too_many_arguments,
    reason = "the Python API keeps CRS endpoints, coordinate columns, epochs, and independent PROJ selection options explicit"
)]
pub(crate) fn crs_transform(
    py: Python<'_>,
    source: &Bound<'_, PyAny>,
    target: &Bound<'_, PyAny>,
    x: &Bound<'_, PyAny>,
    y: &Bound<'_, PyAny>,
    z: Option<&Bound<'_, PyAny>>,
    t: Option<&Bound<'_, PyAny>>,
    area_of_interest: Option<&Bound<'_, PyAny>>,
    source_epoch: Option<&Bound<'_, PyAny>>,
    target_epoch: Option<&Bound<'_, PyAny>>,
    authority: Option<String>,
    accuracy: Option<&Bound<'_, PyAny>>,
    allow_ballpark: Option<bool>,
    only_best: Option<bool>,
    force_over: bool,
) -> PyResult<Py<PyAny>> {
    let source = crs_normalize(source)?;
    let target = crs_normalize(target)?;
    let mut coordinates = CrsCoordinateArgs::parse(py, x, y, z, t)?;
    let options = TransformOptionArgs {
        area_of_interest,
        source_epoch,
        target_epoch,
        authority,
        accuracy,
        allow_ballpark,
        only_best,
        force_over,
    }
    .parse()?;
    let (execution, output) = py.detach(|| -> crate::error::Result<_> {
        let (x, y, zt) = coordinates.columns_mut();
        let execution = crs::transform_coordinates(&source, &target, x, y, zt, options)?;
        // Warning-as-error takes precedence over native output assembly errors.
        Ok((execution, TransformOutput::from_coordinates(coordinates)))
    })?;
    drain_accuracy_warning(py, &execution)?;
    let output = output.map_err(|error| match error {
        TransformOutputError::SizeOverflow => {
            crate::error::Error::from(crate::error::ResourceError::SizeOverflowContext(
                "coordinate matrix size overflows usize",
            ))
            .into()
        },
        TransformOutputError::Allocation(error) => {
            pyo3::exceptions::PyMemoryError::new_err(error.to_string())
        },
    })?;
    match output {
        TransformOutput::Xy(x, y) => Ok((x, y).into_pyobject(py)?.unbind().into()),
        TransformOutput::Xyz(x, y, z) => Ok((x, y, z).into_pyobject(py)?.unbind().into()),
        TransformOutput::Matrix {
            values,
            rows,
            columns,
        } => float64_matrix(py, values, rows, columns),
    }
}

/// Apply a PROJ pipeline/operation definition to coordinates.
///
/// Runs an explicit operation (e.g. a
/// ``+proj=pipeline`` string) rather than resolving one from a CRS pair.
///
/// Parameters
/// ----------
/// operation : str
///     PROJ operation or pipeline definition.
/// x, y : float or sequence of float
///     Coordinate columns (scalars transform a single point).
/// z : float or sequence of float, optional
///     Vertical column for 3D operations.
/// t : float or sequence of float, optional
///     Coordinate epoch column.
/// direction : {'forward', 'inverse'}, default 'forward'
///     Operation direction.
///
/// Returns
/// -------
/// tuple
///     The transformed columns — ``(x, y)``, ``(x, y, z)``, ``(x, y, t)``
///     when T is supplied without Z, or ``(x, y, z, t)``. Scalars in, scalars
///     out; lane inputs return
///     read-only ``float64`` ``numpy.ndarray`` columns (``np.asarray(column)``
///     reads the values directly, ``list(column)`` materializes them).
///
/// Raises
/// ------
/// TransformError
///     If no transform exists between the frames or it fails to apply.
/// CRSError
///     If the operation definition is invalid.
/// InvalidGeometryError
///     If coordinate columns are non-finite or differ in length.
/// GeometryError
///     If ``direction`` is invalid.
///
/// Notes
/// -----
/// Supply a literal operation definition. A definition returned by
/// ``CRS.operation`` may be ``None`` when selected angular semantics cannot be
/// represented faithfully as an executable PROJ string.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> op = '+proj=affine +xoff=1 +yoff=2'
/// >>> gm.crs_apply(op, -122.4, 37.8)
/// (-121.4, 39.8)
#[pyfunction(
    signature = (operation, x, y, z = None, *, t = None, direction = crs::ProjDirection::Forward),
    text_signature = "(operation, x, y, z=None, *, t=None, direction='forward')"
)]
pub(crate) fn crs_apply(
    py: Python<'_>,
    operation: &str,
    x: &Bound<'_, PyAny>,
    y: &Bound<'_, PyAny>,
    z: Option<&Bound<'_, PyAny>>,
    t: Option<&Bound<'_, PyAny>>,
    direction: crs::ProjDirection,
) -> PyResult<Py<PyAny>> {
    let mut coordinates = CrsCoordinateArgs::parse(py, x, y, z, t)?;
    let execution = py.detach(|| {
        let (x, y, zt) = coordinates.columns_mut();
        crs::apply_operation(operation, direction, x, y, zt)
    })?;
    drain_accuracy_warning(py, &execution)?;
    coordinates_to_py(py, coordinates)
}

enum TransformOutput {
    Xy(f64, f64),
    Xyz(f64, f64, f64),
    Matrix {
        values: Vec<f64>,
        rows: usize,
        columns: usize,
    },
}

enum TransformOutputError {
    SizeOverflow,
    Allocation(std::collections::TryReserveError),
}

impl TransformOutput {
    /// The epoch is an input to transformation, not a spatial output ordinate.
    #[expect(
        clippy::indexing_slicing,
        reason = "scalar CrsCoordinateArgs columns contain exactly one ordinate"
    )]
    fn from_coordinates(coordinates: CrsCoordinateArgs) -> Result<Self, TransformOutputError> {
        let CrsCoordinateArgs { x, y, zt, scalar } = coordinates;
        let (x, y) = (x.values, y.values);
        let z = match zt {
            crate::Zt::None | crate::Zt::T(_) => None,
            crate::Zt::Z(z) | crate::Zt::Zt { z, .. } => Some(z.values),
        };
        if scalar {
            return Ok(z.map_or_else(|| Self::Xy(x[0], y[0]), |z| Self::Xyz(x[0], y[0], z[0])));
        }
        let rows = x.len();
        let columns = if z.is_some() { 3 } else { 2 };
        let capacity = rows
            .checked_mul(columns)
            .ok_or(TransformOutputError::SizeOverflow)?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(capacity)
            .map_err(TransformOutputError::Allocation)?;
        match z {
            None => {
                for pair in x.into_iter().zip(y) {
                    values.extend_from_slice(&<[f64; 2]>::from(pair));
                }
            },
            Some(z) => {
                for ((x, y), z) in x.into_iter().zip(y).zip(z) {
                    values.extend_from_slice(&[x, y, z]);
                }
            },
        }
        Ok(Self::Matrix {
            values,
            rows,
            columns,
        })
    }
}

/// Rebuild the Python result tuple — `(x, y)`, `(x, y, z)`, or
/// `(x, y, z, t)`; scalars in, scalars out.
#[expect(
    clippy::indexing_slicing,
    reason = "CrsCoordinateArgs records scalar input before broadcasting and scalar columns contain one ordinate"
)]
pub(crate) fn coordinates_to_py(
    py: Python<'_>,
    coordinates: CrsCoordinateArgs,
) -> PyResult<Py<PyAny>> {
    let CrsCoordinateArgs { x, y, zt, scalar } = coordinates;
    let (x, y) = (x.values, y.values);
    if scalar {
        return match zt {
            crate::Zt::None => Ok((x[0], y[0]).into_pyobject(py)?.unbind().into()),
            crate::Zt::Z(z) => Ok((x[0], y[0], z.values[0]).into_pyobject(py)?.unbind().into()),
            crate::Zt::T(t) => Ok((x[0], y[0], t.values[0]).into_pyobject(py)?.unbind().into()),
            crate::Zt::Zt { z, t } => Ok((x[0], y[0], z.values[0], t.values[0])
                .into_pyobject(py)?
                .unbind()
                .into()),
        };
    }
    // Bulk outputs are zero-copy buffer columns, not 500k boxed floats:
    // `np.asarray(column)` reads the f64 lane directly, `list(column)`
    // still works, and the type mirrors `array.coords` (measured: the
    // list-of-PyFloat build was ~2/3 of a 500k transform's wall time).
    let column = |values: Vec<f64>| float64_array(py, values);
    match zt {
        crate::Zt::None => Ok((column(x)?, column(y)?).into_pyobject(py)?.unbind().into()),
        crate::Zt::Z(z) => Ok((column(x)?, column(y)?, column(z.values)?)
            .into_pyobject(py)?
            .unbind()
            .into()),
        crate::Zt::T(t) => Ok((column(x)?, column(y)?, column(t.values)?)
            .into_pyobject(py)?
            .unbind()
            .into()),
        crate::Zt::Zt { z, t } => {
            Ok(
                (column(x)?, column(y)?, column(z.values)?, column(t.values)?)
                    .into_pyobject(py)?
                    .unbind()
                    .into(),
            )
        },
    }
}

#[pyfunction(signature = (
    source,
    target,
    bounds,
    *,
    densify = 21,
    area_of_interest = None,
    source_epoch = None,
    target_epoch = None,
    authority = None,
    accuracy = None,
    allow_ballpark = None,
    only_best = None,
    force_over = false
), text_signature = "(source, target, bounds, *, densify=21, area_of_interest=None, source_epoch=None, target_epoch=None, authority=None, accuracy=None, allow_ballpark=None, only_best=None, force_over=False)")]
/// Reproject a bounding box, densifying edges for accuracy.
///
/// Parameters
/// ----------
/// source, target : CRS-like
///     Source and destination CRS-like inputs.
/// bounds : sequence of float or iterable of sequences of float
///     A scalar box is a 4-value ``(minx, miny, maxx, maxy)`` sequence or a
///     6-value ``(minx, miny, minz, maxx, maxy, maxz)`` sequence in the source
///     CRS. A batch is a nonempty iterable of 4- or 6-value rows; every row
///     must have the same dimensionality. One-dimensional NumPy arrays supply
///     scalar bounds and two-dimensional NumPy arrays supply batch rows.
///     Batch row order is preserved.
/// densify : int, default 21
///     Points added per edge before transforming, to track curved edges.
///     Must be a non-boolean, nonnegative integer that fits in ``u32``. For a
///     non-identity transform it must be at most 10,000; if the target CRS is
///     geographic, it must also be at least 2. Identity transforms do not
///     sample bounds and skip these transform-specific limits.
/// area_of_interest : sequence of float, dict, or object, optional
///     Area of interest guiding operation selection, as
///     ``(west, south, east, north)`` in DEGREES, an area mapping, or an
///     AreaOfInterest-like object.
/// source_epoch, target_epoch : numeric scalar or 0-D numeric array, optional
///     Coordinate epochs for dynamic CRS.
/// authority : str, optional
///     Restrict candidate coordinate operations to this authority
///     (e.g. ``'EPSG'``).
///
/// accuracy : float, optional
///     Maximum acceptable operation accuracy, in meters.
///
/// allow_ballpark : bool, optional
///     Allow low-accuracy ballpark operations when no precise one exists.
///
/// only_best : bool, optional
///     Require PROJ's best operation. If a required transformation grid is
///     unavailable, raise ``TransformError`` instead of using a less accurate
///     fallback operation.
///
/// force_over : bool, optional
///     Keep coordinates on the source side of the antimeridian instead of
///     wrapping into ``[-180, 180]`` (PROJ ``FORCE_OVER=YES``). Like
///     ``only_best``, this also collapses operation selection to a single
///     candidate, so enumerating surfaces return exactly one operation.
///     Bounds use literal longitude values in this mode: geographic source
///     bounds sample the closed numeric segment between west and east, and
///     geographic target bounds return numeric longitude extrema rather than
///     a periodic cover. Literal identity bounds retain out-of-period values;
///     descending source longitudes use the segment between their sorted
///     endpoints.
///
/// Returns
/// -------
/// tuple or numpy.ndarray
///     The reprojected box in the target CRS. Scalar input returns a 4- or
///     6-value tuple. Batch input returns a ``float64`` array of shape
///     ``(n, 4)`` or ``(n, 6)`` in input row order.
///
/// Raises
/// ------
/// TransformError
///     If no transform exists between the frames or it fails to apply.
/// CRSError
///     If ``source``/``target`` are unrecognized, or a non-identity transform
///     has ``densify`` above 10,000 or below 2 for a geographic target.
/// GeometryError
///     If a bounds box does not contain four or six finite values, a batch is
///     empty, batch rows have inconsistent dimensions, or ``densify`` is
///     outside the nonnegative ``u32`` range.
/// TypeError
///     If ``bounds`` is not iterable or ``densify`` is not an integer index
///     value (``bool`` is not accepted).
/// OverflowError
///     If ``densify`` is outside the signed 64-bit input range.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> import numpy as np
/// >>> np.round(np.asarray(gm.crs_transform_bounds(
/// ...     4326, 3857, (-123, 37, -122, 38))), 0).tolist()
/// [-13692297.0, 4439107.0, -13580978.0, 4579426.0]
#[expect(
    clippy::too_many_arguments,
    reason = "the bounds API exposes its bounds, densification, epochs, and independent PROJ selection options"
)]
pub(crate) fn crs_transform_bounds(
    py: Python<'_>,
    source: &Bound<'_, PyAny>,
    target: &Bound<'_, PyAny>,
    bounds: &Bound<'_, PyAny>,
    #[pyo3(from_py_with = crate::strict_i64_overflow)] densify: i64,
    area_of_interest: Option<&Bound<'_, PyAny>>,
    source_epoch: Option<&Bound<'_, PyAny>>,
    target_epoch: Option<&Bound<'_, PyAny>>,
    authority: Option<String>,
    accuracy: Option<&Bound<'_, PyAny>>,
    allow_ballpark: Option<bool>,
    only_best: Option<bool>,
    force_over: bool,
) -> PyResult<Py<PyAny>> {
    reject_masked_array(bounds)?;
    let densify = u32::try_from(densify).map_err(|_| {
        errors::integer_parameter_error(
            format!("bounds densify must be between 0 and 10000, got {densify}"),
            "densify",
            densify,
        )
    })?;
    // Acquire once and classify from the first item. A scalar bounds tuple is
    // fixed-arity and therefore reads at most six ordinates plus one excess
    // check; a row stream remains an ordinary materialized batch.
    let mut iter = crate::py_iter_or_error(bounds, || {
        PyTypeError::new_err("bounds must be a 4-tuple or 6-tuple of finite floats")
    })?;
    let Some(first) = iter.next() else {
        return Err(GeometryError::new_err(
            "bounds must be a 4-tuple or 6-tuple of finite floats",
        ));
    };
    let first = first?;
    let first_scalar = crate::f64_scalar_named("bounds", &first)?;
    let items = if first_scalar.is_some() {
        let rest = crate::collect_py_iter_at_most_from_iter(bounds, iter, 5, Ok, |_| {
            GeometryError::new_err("bounds must be a 4-tuple or 6-tuple of finite floats")
        })?;
        let mut items = Vec::new();
        items.push(first);
        items.reserve(rest.len());
        items.extend(rest);
        items
    } else {
        let rest = crate::collect_py_iter_from_iter(bounds, iter, Ok)?;
        let mut items = Vec::new();
        items.push(first);
        items.reserve(rest.len());
        items.extend(rest);
        items
    };
    let source = crs_normalize(source)?;
    let target = crs_normalize(target)?;
    let options = TransformOptionArgs {
        area_of_interest,
        source_epoch,
        target_epoch,
        authority,
        accuracy,
        allow_ballpark,
        only_best,
        force_over,
    }
    .parse()?;
    let (result, execution) = (match classify_bounds_shape(py, &items, first_scalar)? {
        BoundsShape::One(values) => match values.as_slice() {
            [minx, miny, maxx, maxy] => {
                let (result, execution) = crs::transform_bounds(
                    &source,
                    &target,
                    (*minx, *miny, *maxx, *maxy),
                    densify,
                    options,
                )?;
                Ok((result.into_pyobject(py)?.unbind().into(), execution))
            },
            [minx, miny, minz, maxx, maxy, maxz] => {
                let (result, execution) = crs::transform_bounds_3d(
                    &source,
                    &target,
                    (*minx, *miny, *minz, *maxx, *maxy, *maxz),
                    densify,
                    options,
                )?;
                Ok((result.into_pyobject(py)?.unbind().into(), execution))
            },
            _ => Err(GeometryError::new_err(
                "bounds must be a 4-tuple or 6-tuple of finite floats",
            )),
        },
        BoundsShape::Many(rows) => {
            let (len, columns) = match &rows {
                BoundsRows::TwoD(rows) => (rows.len(), 4),
                BoundsRows::ThreeD(rows) => (rows.len(), 6),
            };
            let (out, execution) = py.detach(move || {
                let capacity = len
                    .checked_mul(columns)
                    .ok_or_else(|| GeometryError::new_err("bounds matrix size overflows usize"))?;
                let mut out = Vec::with_capacity(capacity);
                let execution = match rows {
                    BoundsRows::TwoD(input_rows) => {
                        let (transformed, execution) = crs::transform_bounds_many(
                            &source,
                            &target,
                            &input_rows,
                            densify,
                            options,
                        )?;
                        for transformed in transformed {
                            out.extend_from_slice(&<[f64; 4]>::from(transformed));
                        }
                        execution
                    },
                    BoundsRows::ThreeD(input_rows) => {
                        let (transformed, execution) = crs::transform_bounds_3d_many(
                            &source,
                            &target,
                            &input_rows,
                            densify,
                            options,
                        )?;
                        for transformed in transformed {
                            out.extend_from_slice(&<[f64; 6]>::from(transformed));
                        }
                        execution
                    },
                };
                Ok::<_, PyErr>((out, execution))
            })?;
            Ok((float64_matrix(py, out, len, columns)?, execution))
        },
    })?;
    drain_accuracy_warning(py, &execution)?;
    Ok(result)
}

enum BoundsRows {
    TwoD(Vec<(f64, f64, f64, f64)>),
    ThreeD(Vec<(f64, f64, f64, f64, f64, f64)>),
}

enum BoundsShape {
    One(Vec<f64>),
    Many(BoundsRows),
}

/// Structural One|Many classification over already-materialized bounds items.
///
/// A leading finite float means a scalar box (every element must be a finite
/// float; length must be 4 or 6). Otherwise each item is a row box — a
/// malformed scalar is never re-routed into the batch error lane.
fn classify_bounds_shape(
    py: Python<'_>,
    items: &[Bound<'_, PyAny>],
    first_scalar: Option<f64>,
) -> PyResult<BoundsShape> {
    // Empty input is not a batch of zero rows — it fails the scalar contract
    // (same diagnostic as a wrong-length box).
    if items.is_empty() {
        return Err(GeometryError::new_err(
            "bounds must be a 4-tuple or 6-tuple of finite floats",
        ));
    }
    if let Some(first) = first_scalar {
        if !first.is_finite() {
            return Err(GeometryError::new_err("bounds must be finite"));
        }
        let mut values = Vec::with_capacity(items.len());
        values.push(first);
        for item in items.iter().skip(1) {
            // Match float_values' element contract: f64-extractable only.
            let value = crate::finite_f64_required("bounds", item)?;
            values.push(value);
        }
        if !matches!(values.len(), 4 | 6) {
            return Err(GeometryError::new_err(
                "bounds must be a 4-tuple or 6-tuple of finite floats",
            ));
        }
        return Ok(BoundsShape::One(values));
    }
    let mut rows = None;
    let mut width = None;
    for item in items {
        let values =
            crate::float_values_at_most(py, item, "bounds", FloatInput::Parameter, 6, |got| {
                GeometryError::new_err(format!(
                    "bounds rows must be 4-tuples or 6-tuples of finite floats; got {got} values"
                ))
            })?;
        let row_width = values.len();
        if !matches!(row_width, 4 | 6) {
            return Err(GeometryError::new_err(
                "bounds rows must be 4-tuples or 6-tuples of finite floats",
            ));
        }
        if let Some(width) = width {
            if width != row_width {
                return Err(GeometryError::new_err(
                    "bounds rows must all have the same dimensionality",
                ));
            }
        } else {
            width = Some(row_width);
            rows = Some(match row_width {
                4 => BoundsRows::TwoD(Vec::with_capacity(items.len())),
                6 => BoundsRows::ThreeD(Vec::with_capacity(items.len())),
                _ => unreachable!("checked row width above"),
            });
        }
        match (
            rows.as_mut()
                .expect("the first valid row creates its output"),
            values.as_slice(),
        ) {
            (BoundsRows::TwoD(output), [minx, miny, maxx, maxy]) => {
                output.push((*minx, *miny, *maxx, *maxy));
            },
            (BoundsRows::ThreeD(output), [minx, miny, minz, maxx, maxy, maxz]) => {
                output.push((*minx, *miny, *minz, *maxx, *maxy, *maxz));
            },
            _ => unreachable!("checked homogeneous bounds width"),
        }
    }
    Ok(BoundsShape::Many(
        rows.expect("nonempty batch contains at least one valid row"),
    ))
}

#[pyfunction(signature = (
    source,
    target,
    x,
    y,
    z = None,
    *,
    t = None,
    iterations = 1,
    direction = crs::ProjDirection::Forward,
    area_of_interest = None,
    source_epoch = None,
    target_epoch = None,
    authority = None,
    accuracy = None,
    allow_ballpark = None,
    only_best = None,
    force_over = false
), text_signature = "(source, target, x, y, z=None, *, t=None, iterations=1, direction='forward', area_of_interest=None, source_epoch=None, target_epoch=None, authority=None, accuracy=None, allow_ballpark=None, only_best=None, force_over=False)")]
/// Round-trip coordinates through a CRS pair to measure error.
///
/// Parameters
/// ----------
/// source, target : CRS-like
///     Source and destination CRS-like inputs.
/// x, y : float or sequence of float
///     Coordinates, scalar or batch.
/// z, t : float or sequence of float, optional
///     Height and time ordinates when provided.
/// iterations : int, default 1
///     How many forward+inverse passes to apply.
/// direction : {'forward', 'inverse'}, default 'forward'
///     Which leg runs first.
/// area_of_interest : sequence of float, dict, or object, optional
///     Area of interest guiding operation selection, as
///     ``(west, south, east, north)`` in DEGREES, an area mapping, or an
///     AreaOfInterest-like object.
/// source_epoch, target_epoch : numeric scalar or 0-D numeric array, optional
///     Coordinate epochs for dynamic CRS.
/// authority : str, optional
///     Restrict candidate coordinate operations to this authority
///     (e.g. ``'EPSG'``).
///
/// accuracy : float, optional
///     Maximum acceptable operation accuracy, in meters.
///
/// allow_ballpark : bool, optional
///     Allow low-accuracy ballpark operations when no precise one exists.
///
/// only_best : bool, optional
///     Require PROJ's best operation. If a required transformation grid is
///     unavailable, raise ``TransformError`` instead of using a less accurate
///     fallback operation.
///
/// force_over : bool, optional
///     Keep coordinates on the source side of the antimeridian instead of
///     wrapping into ``[-180, 180]`` (PROJ ``FORCE_OVER=YES``). Like
///     ``only_best``, this also collapses operation selection to a single
///     candidate, so enumerating surfaces return exactly one operation.
///
/// Returns
/// -------
/// float or numpy.ndarray
///     Euclidean residual of the raw input columns in their original units
///     after the out-and-back legs. When Z or T is present the hypot mixes
///     those column units with X and Y. There is no geodesic distance and no
///     circular-longitude normalization. Scalar in, scalar out, and lane
///     inputs return a read-only ``float64`` ``numpy.ndarray``.
///
/// Raises
/// ------
/// TransformError
///     If no transform exists between the frames or it fails to apply.
/// CRSError
///     If ``source``/``target`` are unrecognized.
/// InvalidGeometryError
///     If coordinate columns are non-finite or differ in length.
/// GeometryError
///     If an epoch option is invalid.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.crs_roundtrip(4326, 3857, -122.4, 37.8) < 1e-9
/// True
#[expect(
    clippy::too_many_arguments,
    reason = "round-trip iterations, direction, coordinate columns, epochs, and PROJ selection are distinct Python controls"
)]
pub(crate) fn crs_roundtrip(
    py: Python<'_>,
    source: &Bound<'_, PyAny>,
    target: &Bound<'_, PyAny>,
    x: &Bound<'_, PyAny>,
    y: &Bound<'_, PyAny>,
    z: Option<&Bound<'_, PyAny>>,
    t: Option<&Bound<'_, PyAny>>,
    #[pyo3(from_py_with = crate::strict_i32_overflow)] iterations: i32,
    direction: crs::ProjDirection,
    area_of_interest: Option<&Bound<'_, PyAny>>,
    source_epoch: Option<&Bound<'_, PyAny>>,
    target_epoch: Option<&Bound<'_, PyAny>>,
    authority: Option<String>,
    accuracy: Option<&Bound<'_, PyAny>>,
    allow_ballpark: Option<bool>,
    only_best: Option<bool>,
    force_over: bool,
) -> PyResult<Py<PyAny>> {
    let iterations = check_i32_min("roundtrip iterations", i64::from(iterations), 1)?;
    let coordinates = CrsCoordinateArgs::parse(py, x, y, z, t)?;
    let source = crs_normalize(source)?;
    let target = crs_normalize(target)?;
    let options = TransformOptionArgs {
        area_of_interest,
        source_epoch,
        target_epoch,
        authority,
        accuracy,
        allow_ballpark,
        only_best,
        force_over,
    }
    .parse()?;
    let (x, y, zt) = coordinates.columns();
    // Bulk PROJ work runs detached, like `transform`/`apply`.
    let (errors, execution) = py.detach(|| {
        crs::roundtrip_errors(&source, &target, x, y, zt, iterations, direction, &options)
    })?;
    drain_accuracy_warning(py, &execution)?;
    if coordinates.scalar {
        Ok(errors
            .first()
            .expect("scalar coordinates produce one round-trip error")
            .into_pyobject(py)?
            .unbind()
            .into())
    } else {
        Ok(float64_array(py, errors)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case::xy(false, false, false)]
    #[case::xyz(true, false, false)]
    #[case::xyt(false, true, false)]
    #[case::xyzt(true, true, false)]
    #[case::empty_xy(false, false, true)]
    #[case::empty_xyz(true, false, true)]
    #[cfg_attr(miri, ignore = "Miri cannot execute Python/NumPy and native PROJ FFI")]
    fn transform_matrix_preserves_spatial_columns_and_omits_epoch(
        #[case] with_z: bool,
        #[case] with_t: bool,
        #[case] empty: bool,
    ) {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let crs = 4326_i32.into_pyobject(py).expect("EPSG input").into_any();
            let x = if empty { vec![] } else { vec![0.0, -0.0] }
                .into_pyobject(py)
                .expect("x input");
            let y = if empty { vec![] } else { vec![1.0, 2.0] }
                .into_pyobject(py)
                .expect("y input");
            let z = if empty { vec![] } else { vec![3.0, 4.0] }
                .into_pyobject(py)
                .expect("z input");
            let t = if empty { vec![] } else { vec![2020.0, 2021.0] }
                .into_pyobject(py)
                .expect("t input");
            let output = crs_transform(
                py,
                &crs,
                &crs,
                x.as_any(),
                y.as_any(),
                with_z.then_some(z.as_any()),
                with_t.then_some(t.as_any()),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                false,
            )
            .expect("identity transform");
            let output = output.bind(py);
            let columns = if with_z { 3 } else { 2 };
            assert_eq!(
                output
                    .getattr("shape")
                    .expect("matrix shape")
                    .extract::<(usize, usize)>()
                    .expect("shape pair"),
                (if empty { 0 } else { 2 }, columns)
            );
            assert!(
                !output
                    .getattr("flags")
                    .expect("matrix flags")
                    .getattr("writeable")
                    .expect("writable flag")
                    .extract::<bool>()
                    .expect("boolean flag")
            );
            let values = output
                .call_method0("tolist")
                .expect("matrix rows")
                .extract::<Vec<Vec<f64>>>()
                .expect("numeric rows");
            let expected = if empty {
                vec![]
            } else if with_z {
                vec![vec![0.0, 1.0, 3.0], vec![-0.0, 2.0, 4.0]]
            } else {
                vec![vec![0.0, 1.0], vec![-0.0, 2.0]]
            };
            assert_eq!(
                values
                    .iter()
                    .flatten()
                    .map(|value| value.to_bits())
                    .collect::<Vec<_>>(),
                expected
                    .iter()
                    .flatten()
                    .map(|value: &f64| value.to_bits())
                    .collect::<Vec<_>>()
            );
        });
    }
}
