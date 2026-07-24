#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::check_i32_min;
use crate::py::crs::*;

pub(crate) struct TransformOptionArgs<'py> {
    pub area_of_interest: Option<&'py Bound<'py, PyAny>>,
    pub source_epoch: Option<&'py Bound<'py, PyAny>>,
    pub target_epoch: Option<&'py Bound<'py, PyAny>>,
    pub authority: Option<String>,
    pub accuracy: Option<&'py Bound<'py, PyAny>>,
    pub allow_ballpark: Option<bool>,
    pub only_best: Option<bool>,
    pub force_over: bool,
}

impl TransformOptionArgs<'_> {
    pub(crate) fn parse(self) -> PyResult<crs::TransformOptions> {
        Ok(crs::TransformOptions {
            area_of_interest: parse_area(self.area_of_interest, "area_of_interest")?,
            source_epoch: coordinate_epoch_option("source_epoch", self.source_epoch)?,
            target_epoch: coordinate_epoch_option("target_epoch", self.target_epoch)?,
            authority: self.authority,
            accuracy: accuracy_option(self.accuracy)?,
            allow_ballpark: self.allow_ballpark,
            only_best: self.only_best,
            force_over: self.force_over,
        })
    }
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
pub(crate) fn crs_grid(name: &str) -> PyResult<crs::GridDatabaseInfo> {
    Ok(crs::grid_info(name)?)
}

/// The selected coordinate operation between two CRS.
///
/// Parameters
/// ----------
/// source, target : CRS-like
///     Source and destination CRS (EPSG code or authority/WKT string).
///
/// Returns
/// -------
/// dict
pub(crate) fn crs_operation(
    source: &Bound<'_, PyAny>,
    target: &Bound<'_, PyAny>,
    opts: TransformOptionArgs<'_>,
) -> PyResult<crs::OperationInfo> {
    let source = crs_normalize(source)?;
    let target = crs_normalize(target)?;
    let options = opts.parse()?;
    Ok(crs::operation_info(&source, &target, &options)?)
}

/// The coordinate operation between two CRS at a location.
///
/// Parameters
/// ----------
/// source, target : CRS-like
///     Source and destination CRS (EPSG code or authority/WKT string).
/// coord : coordinate
///     A coordinate as ``(x, y)``.
///
/// Returns
/// -------
/// dict
pub(crate) fn crs_operation_at(
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
    Ok(crs::operation_info_at(
        &source, &target, x, y, zt, &options,
    )?)
}

/// Candidate coordinate operations between two CRS.
///
/// Parameters
/// ----------
/// source, target : CRS-like
///     Source and destination CRS (EPSG code or authority/WKT string).
///
/// Returns
/// -------
/// list
pub(crate) fn crs_operations(
    source: &Bound<'_, PyAny>,
    target: &Bound<'_, PyAny>,
    opts: TransformOptionArgs<'_>,
) -> PyResult<Vec<crs::OperationInfo>> {
    let source = crs_normalize(source)?;
    let target = crs_normalize(target)?;
    let options = opts.parse()?;
    Ok(crs::operations_info(&source, &target, &options)?)
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
///     Source and target CRS (EPSG code or authority/WKT string).
/// x, y : float or sequence of float
///     Coordinate columns (scalars transform a single point).
/// z : float or sequence of float, optional
///     Vertical column for 3D transforms.
/// t : float or sequence of float, optional
///     Coordinate epoch column.
/// area_of_interest : dict or object, optional
///     Area of interest guiding operation selection.
/// source_epoch, target_epoch : float, optional
///     Coordinate epochs for dynamic CRS.
/// authority, accuracy, allow_ballpark, only_best, force_over : optional
///     PROJ operation-selection options.
///
/// Returns
/// -------
/// tuple or numpy.ndarray
///     Scalars in, scalars out: ``(x, y)`` or ``(x, y, z)``. Lane input returns
///     a read-only ``(N, 2)``/``(N, 3)`` ``float64`` matrix (interleaved, the
///     same shape as `get_coordinates`), so ``result[:, 0]`` / ``result[:, 1]``
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
    let source = crs_canonical(source)?;
    let target = crs_canonical(target)?;
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
    py.detach(|| {
        let (x, y, zt) = coordinates.columns_mut();
        crs::transform_coordinates(&source, &target, x, y, zt, options)
    })?;
    coordinates_to_matrix_py(py, coordinates)
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
///     The transformed columns — ``(x, y)``, ``(x, y, z)``, or
///     ``(x, y, z, t)``. Scalars in, scalars out; lane inputs return
///     read-only ``float64`` ``numpy.ndarray`` columns (``np.asarray(column)``
///     reads the values directly, ``list(column)`` materializes them).
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
///     If ``direction`` is invalid.
#[pyfunction(
    signature = (operation, x, y, z = None, *, t = None, direction = crs::ProjDirection::Forward),
    text_signature = "(operation, x, y, z=None, *, t=None, direction='forward')"
)]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> import numpy as np
/// >>> op = gm.CRS(4326).operation(3857).get('definition') or ''
/// >>> np.round(np.asarray(gm.crs_apply(op, -122.4, 37.8)), 1).tolist()
/// [-13625505.7, 4551210.9]
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
    py.detach(|| {
        let (x, y, zt) = coordinates.columns_mut();
        crs::apply_operation(operation, direction, x, y, zt)
    })?;
    coordinates_to_py(py, coordinates)
}

/// Rebuild the `crs_transform` result — scalars in, a scalar tuple out
/// (`(x, y)` or `(x, y, z)`); lane input returns a frozen `(N, 2)`/`(N, 3)`
/// `float64` matrix (interleaved, consistent with `get_coordinates` and
/// `transform_bounds_many`). The input epoch `t` is never a transformed
/// spatial ordinate, so it is not echoed (unlike `crs_apply`, which returns
/// the raw columns it ran on).
pub(crate) fn coordinates_to_matrix_py(
    py: Python<'_>,
    coordinates: CrsCoordinateArgs,
) -> PyResult<Py<PyAny>> {
    let CrsCoordinateArgs { x, y, zt, scalar } = coordinates;
    let (x, y) = (x.values, y.values);
    let z = match zt {
        crate::Zt::None | crate::Zt::T(_) => None,
        crate::Zt::Z(z) | crate::Zt::Zt { z, .. } => Some(z.values),
    };
    if scalar {
        return match z {
            None => Ok((x[0], y[0]).into_pyobject(py)?.unbind().into()),
            Some(z) => Ok((x[0], y[0], z[0]).into_pyobject(py)?.unbind().into()),
        };
    }
    let rows = x.len();
    let columns = if z.is_some() { 3 } else { 2 };
    let mut flat = Vec::with_capacity(rows * columns);
    match z {
        None => {
            for i in 0..rows {
                flat.push(x[i]);
                flat.push(y[i]);
            }
        },
        Some(z) => {
            for i in 0..rows {
                flat.push(x[i]);
                flat.push(y[i]);
                flat.push(z[i]);
            }
        },
    }
    crate::py::numpy::float64_matrix(py, flat, rows, columns)
}

/// Rebuild the Python result tuple — `(x, y)`, `(x, y, z)`, or
/// `(x, y, z, t)`; scalars in, scalars out.
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
    let column = |values: Vec<f64>| crate::py::numpy::float64_array(py, values);
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
///     Source and destination CRS (EPSG code or authority/WKT string).
/// bounds : tuple
///     ``(minx, miny, maxx, maxy)`` (or a 3D ``(minx, miny, minz, maxx,
///     maxy, maxz)``) box in the source CRS.
/// densify : int, default 21
///     Points added per edge before transforming, to track curved edges.
/// area_of_interest : dict or object, optional
///     Area of interest guiding operation selection.
/// source_epoch, target_epoch : float, optional
///     Coordinate epochs for dynamic CRS.
/// authority, accuracy, allow_ballpark, only_best, force_over : optional
///     PROJ operation-selection options (see ``crs_transform``).
///
/// Returns
/// -------
/// tuple
///     The reprojected box in the target CRS.
///
/// Raises
/// ------
/// TransformError
///     If no transform exists between the frames or it fails to apply.
/// CRSError
///     If ``source``/``target`` are unrecognized.
/// GeometryError
///     If ``bounds`` is not a 4- or 6-value sequence of finite floats.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> import numpy as np
/// >>> np.round(np.asarray(gm.crs_transform_bounds(
/// ...     4326, 3857, (-123, 37, -122, 38))), 0).tolist()
/// [-13692297.0, 4439107.0, -13580978.0, 4579426.0]
pub(crate) fn crs_transform_bounds(
    py: Python<'_>,
    source: &Bound<'_, PyAny>,
    target: &Bound<'_, PyAny>,
    bounds: &Bound<'_, PyAny>,
    densify: i64,
    area_of_interest: Option<&Bound<'_, PyAny>>,
    source_epoch: Option<&Bound<'_, PyAny>>,
    target_epoch: Option<&Bound<'_, PyAny>>,
    authority: Option<String>,
    accuracy: Option<&Bound<'_, PyAny>>,
    allow_ballpark: Option<bool>,
    only_best: Option<bool>,
    force_over: bool,
) -> PyResult<Py<PyAny>> {
    let densify = u32::try_from(densify).map_err(|_| {
        crate::py::errors::integer_parameter_error(
            format!("bounds densify must be between 0 and 10000, got {densify}"),
            "densify",
            densify,
        )
    })?;
    // Materialize once (one-shot generators + D10 MemoryError on unbounded
    // streams), then classify structurally as One | Many — never treat a
    // scalar parse failure as evidence of batch input.
    let items = crate::collect_py_iter(bounds, Ok)?;
    let source = crs_canonical(source)?;
    let target = crs_canonical(target)?;
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
    match classify_bounds_shape(py, &items)? {
        BoundsShape::One(values) => match values.as_slice() {
            [minx, miny, maxx, maxy] => Ok(crs::transform_bounds(
                &source,
                &target,
                (*minx, *miny, *maxx, *maxy),
                densify,
                options,
            )?
            .into_pyobject(py)?
            .unbind()
            .into()),
            [minx, miny, minz, maxx, maxy, maxz] => Ok(crs::transform_bounds_3d(
                &source,
                &target,
                (*minx, *miny, *minz, *maxx, *maxy, *maxz),
                densify,
                options,
            )?
            .into_pyobject(py)?
            .unbind()
            .into()),
            _ => Err(crate::py::errors::GeometryError::new_err(
                "bounds must be a 4-tuple or 6-tuple of finite floats",
            )),
        },
        BoundsShape::Many(rows) => {
            let columns = rows.first().map_or(4, BoundsRow::width);
            let len = rows.len();
            let out = py.detach(move || {
                let mut out = Vec::with_capacity(len * columns);
                if columns == 4 {
                    let rows: Vec<_> = rows
                        .into_iter()
                        .map(|row| match row {
                            BoundsRow::TwoD(bounds) => bounds,
                            BoundsRow::ThreeD(_) => {
                                unreachable!("checked homogeneous bounds width")
                            },
                        })
                        .collect();
                    for transformed in
                        crs::transform_bounds_many(&source, &target, &rows, densify, options)?
                    {
                        out.extend_from_slice(&[
                            transformed.0,
                            transformed.1,
                            transformed.2,
                            transformed.3,
                        ]);
                    }
                } else {
                    let rows: Vec<_> = rows
                        .into_iter()
                        .map(|row| match row {
                            BoundsRow::ThreeD(bounds) => bounds,
                            BoundsRow::TwoD(_) => {
                                unreachable!("checked homogeneous bounds width")
                            },
                        })
                        .collect();
                    for transformed in
                        crs::transform_bounds_3d_many(&source, &target, &rows, densify, options)?
                    {
                        out.extend_from_slice(&[
                            transformed.0,
                            transformed.1,
                            transformed.2,
                            transformed.3,
                            transformed.4,
                            transformed.5,
                        ]);
                    }
                }
                Ok::<_, PyErr>(out)
            })?;
            crate::py::numpy::float64_matrix(py, out, len, columns)
        },
    }
}

enum BoundsRow {
    TwoD((f64, f64, f64, f64)),
    ThreeD((f64, f64, f64, f64, f64, f64)),
}

impl BoundsRow {
    const fn width(&self) -> usize {
        match self {
            Self::TwoD(_) => 4,
            Self::ThreeD(_) => 6,
        }
    }
}

enum BoundsShape {
    One(Vec<f64>),
    Many(Vec<BoundsRow>),
}

/// Structural One|Many classification over already-materialized bounds items.
///
/// A leading finite float means a scalar box (every element must be a finite
/// float; length must be 4 or 6). Otherwise each item is a row box — a
/// malformed scalar is never re-routed into the batch error lane.
fn classify_bounds_shape(py: Python<'_>, items: &[Bound<'_, PyAny>]) -> PyResult<BoundsShape> {
    // Empty input is not a batch of zero rows — it fails the scalar contract
    // (same diagnostic as a wrong-length box).
    if items.is_empty() {
        return Err(crate::py::errors::GeometryError::new_err(
            "bounds must be a 4-tuple or 6-tuple of finite floats",
        ));
    }
    let is_scalar = items[0].extract::<f64>().is_ok();
    if is_scalar {
        let mut values = Vec::with_capacity(items.len());
        for item in items {
            // Match coordinate_values' element contract: f64-extractable only.
            let value = item.extract::<f64>().map_err(|_| {
                pyo3::exceptions::PyTypeError::new_err(
                    "bounds must be a float or an iterable of finite floats",
                )
            })?;
            values.push(value);
        }
        if !matches!(values.len(), 4 | 6) {
            return Err(crate::py::errors::GeometryError::new_err(
                "bounds must be a 4-tuple or 6-tuple of finite floats",
            ));
        }
        return Ok(BoundsShape::One(values));
    }
    let mut rows = Vec::new();
    crate::try_reserve_hint(&mut rows, items.len())?;
    let mut width = None;
    for item in items {
        let values = coordinate_values(py, item, "bounds")?;
        let row_width = values.len();
        if !matches!(row_width, 4 | 6) {
            return Err(crate::py::errors::GeometryError::new_err(
                "bounds rows must be 4-tuples or 6-tuples of finite floats",
            ));
        }
        if let Some(width) = width {
            if width != row_width {
                return Err(crate::py::errors::GeometryError::new_err(
                    "bounds rows must all have the same dimensionality",
                ));
            }
        } else {
            width = Some(row_width);
        }
        crate::try_push(&mut rows, match values.as_slice() {
            [minx, miny, maxx, maxy] => BoundsRow::TwoD((*minx, *miny, *maxx, *maxy)),
            [minx, miny, minz, maxx, maxy, maxz] => {
                BoundsRow::ThreeD((*minx, *miny, *minz, *maxx, *maxy, *maxz))
            },
            _ => unreachable!("checked row width above"),
        })?;
    }
    Ok(BoundsShape::Many(rows))
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
///     Source and destination CRS (EPSG code or authority/WKT string).
/// x, y : float or sequence of float
///     Coordinates, scalar or batch.
/// z, t : float or sequence of float, optional
///     Height and time ordinates when provided.
/// iterations : int, default 1
///     How many forward+inverse passes to apply.
/// direction : {'forward', 'inverse'}, default 'forward'
///     Which leg runs first.
/// area_of_interest : dict or object, optional
///     Area of interest guiding operation selection.
/// source_epoch, target_epoch : float, optional
///     Coordinate epochs for dynamic CRS.
/// authority, accuracy, allow_ballpark, only_best, force_over : optional
///     PROJ operation-selection options (see ``crs_transform``).
///
/// Returns
/// -------
/// float or numpy.ndarray
///     The round-trip error per coordinate; scalar in, scalar out, and
///     lane inputs return a read-only ``float64`` ``numpy.ndarray``.
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
pub(crate) fn crs_roundtrip(
    py: Python<'_>,
    source: &Bound<'_, PyAny>,
    target: &Bound<'_, PyAny>,
    x: &Bound<'_, PyAny>,
    y: &Bound<'_, PyAny>,
    z: Option<&Bound<'_, PyAny>>,
    t: Option<&Bound<'_, PyAny>>,
    iterations: i32,
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
    let errors = py.detach(|| {
        crs::roundtrip_errors(&source, &target, x, y, zt, iterations, direction, &options)
    })?;
    if coordinates.scalar {
        Ok(errors[0].into_pyobject(py)?.unbind().into())
    } else {
        Ok(crate::py::numpy::float64_array(py, errors)?)
    }
}
