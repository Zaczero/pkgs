use pyo3::types::{PyDict, PyTuple};

use crate::boundary::coordinate_input::coordinate_column_values;
use crate::geometry::SharedSlice;
use crate::geometry::storage::replacement::{CoordinateReplacement, ReplacementAxis};
use crate::py::classes::coordinate_methods::{
    Bound, CoordinateAxes, CoordinateAxis, PyAny, PyAnyMethods as _, PyCoordinates,
    PyDictMethods as _, PyErr, PyRef, PyResult, PyTupleMethods as _, Python, coordinate_axis_order,
    coordinates,
};
use crate::py::errors::{GeometryError, InvalidGeometryError};
use crate::py::numpy::numpy_module;

fn replacement_len_error(expected: usize, got: usize) -> PyErr {
    InvalidGeometryError::new_err(format!(
        "coordinates must have length {expected}; got length {got}"
    ))
}

fn replacement_width_error(expected: usize, got: usize) -> PyErr {
    InvalidGeometryError::new_err(format!(
        "coordinates must have width {expected}; got width {got}"
    ))
}

fn axis_length_error(name: &str, expected: usize, got: usize) -> PyErr {
    InvalidGeometryError::new_err(format!(
        "{name} must have length {expected}; got length {got}"
    ))
}

fn explicit_none_error(name: &str) -> PyErr {
    InvalidGeometryError::new_err(format!(
        "{name}=None is not allowed; X and Y ordinates cannot be removed"
    ))
}

fn axis_count(axes: CoordinateAxes) -> usize {
    2_usize
        .wrapping_add(usize::from(axes.has_z()))
        .wrapping_add(usize::from(axes.has_m()))
}

fn replacement_axis_values(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    name: &str,
    expected_len: usize,
) -> PyResult<SharedSlice<f64>> {
    let is_scalar = match value.getattr_opt(pyo3::intern!(py, "ndim"))? {
        Some(ndim) => ndim.extract::<usize>()? == 0,
        None => value.getattr_opt(pyo3::intern!(py, "__float__"))?.is_some(),
    };
    let values = if is_scalar {
        vec![crate::finite_coordinate_required(name, value)?; expected_len]
    } else {
        crate::coordinate_column_values_exact(py, value, name, expected_len, |got| {
            axis_length_error(name, expected_len, got)
        })?
    };
    if !crate::geometry::column_all_finite(&values) {
        return Err(InvalidGeometryError::new_err(format!(
            "{name} must be finite"
        )));
    }
    Ok(SharedSlice::from_vec(values))
}

fn optional_xy_axis(
    py: Python<'_>,
    kwargs: &Bound<'_, PyDict>,
    name: &str,
    expected_len: usize,
) -> PyResult<Option<SharedSlice<f64>>> {
    let Some(value) = kwargs.get_item(name)? else {
        return Ok(None);
    };
    if value.is_none() {
        return Err(explicit_none_error(name));
    }
    Ok(Some(replacement_axis_values(
        py,
        &value,
        name,
        expected_len,
    )?))
}

fn optional_replace_axis(
    py: Python<'_>,
    kwargs: &Bound<'_, PyDict>,
    name: &str,
    expected_len: usize,
) -> PyResult<ReplacementAxis> {
    let Some(value) = kwargs.get_item(name)? else {
        return Ok(ReplacementAxis::Carry);
    };
    if value.is_none() {
        return Ok(ReplacementAxis::Drop);
    }
    Ok(ReplacementAxis::Replace(replacement_axis_values(
        py,
        &value,
        name,
        expected_len,
    )?))
}

fn replacement_from_kwargs(
    py: Python<'_>,
    kwargs: &Bound<'_, PyDict>,
    axes: CoordinateAxes,
    expected_len: usize,
) -> PyResult<CoordinateReplacement> {
    for key in kwargs.keys() {
        let name = key.extract::<&str>()?;
        if !matches!(name, "x" | "y" | "z" | "m") {
            return Err(pyo3::exceptions::PyTypeError::new_err(format!(
                "with_coordinates got an unexpected keyword argument {name:?}"
            )));
        }
    }
    Ok(CoordinateReplacement {
        xs: optional_xy_axis(py, kwargs, "x", expected_len)?,
        ys: optional_xy_axis(py, kwargs, "y", expected_len)?,
        zs: optional_replace_axis(py, kwargs, "z", expected_len)?,
        ms: optional_replace_axis(py, kwargs, "m", expected_len)?,
        len: expected_len,
        axes,
        positional: false,
    })
}

fn replacement_from_coordinates_view(
    py: Python<'_>,
    coords: &PyCoordinates,
    axes: CoordinateAxes,
    expected_len: usize,
) -> PyResult<CoordinateReplacement> {
    let layout = match coords.layout {
        Some(layout) => layout,
        None => coords.view.axes_for_python(py)?,
    };
    if layout != axes {
        return Err(InvalidGeometryError::new_err(format!(
            "coordinates axes must be {}; got {}",
            axes.as_str(),
            layout.as_str()
        )));
    }
    let rows = coords.view.len_for_python(py)?;
    if rows != expected_len {
        return Err(replacement_len_error(expected_len, rows));
    }
    coordinates::checked_vector_len::<f64>(expected_len, "coordinate replacement")?;
    if let Some(seq) = coords.view.single_seq()
        && seq.axes() == axes
        && seq.len() == expected_len
    {
        return Ok(CoordinateReplacement {
            xs: Some(seq.carried_xs()),
            ys: Some(seq.carried_ys()),
            zs: seq
                .carried_zs()
                .map_or(ReplacementAxis::Carry, ReplacementAxis::Replace),
            ms: seq.ms().map_or(ReplacementAxis::Carry, |values| {
                ReplacementAxis::Replace(SharedSlice::from_vec(values.to_vec()))
            }),
            len: expected_len,
            axes,
            positional: true,
        });
    }
    let mut xs = Vec::with_capacity(expected_len);
    let mut ys = Vec::with_capacity(expected_len);
    let mut zs = axes.has_z().then(|| Vec::with_capacity(expected_len));
    let mut ms = axes.has_m().then(|| Vec::with_capacity(expected_len));
    coords.view.for_each_point(|coord| {
        xs.push(coord.x);
        ys.push(coord.y);
        if let Some(out) = zs.as_mut() {
            out.push(coord.z().unwrap_or(f64::NAN));
        }
        if let Some(out) = ms.as_mut() {
            out.push(coord.m().unwrap_or(f64::NAN));
        }
    });
    Ok(CoordinateReplacement {
        xs: Some(SharedSlice::from_vec(xs)),
        ys: Some(SharedSlice::from_vec(ys)),
        zs: zs.map_or(ReplacementAxis::Carry, |values| {
            ReplacementAxis::Replace(SharedSlice::from_vec(values))
        }),
        ms: ms.map_or(ReplacementAxis::Carry, |values| {
            ReplacementAxis::Replace(SharedSlice::from_vec(values))
        }),
        len: expected_len,
        axes,
        positional: true,
    })
}

fn replacement_from_matrix(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    axes: CoordinateAxes,
    expected_len: usize,
) -> PyResult<CoordinateReplacement> {
    if let Ok(coords) = value.extract::<PyRef<PyCoordinates>>() {
        return replacement_from_coordinates_view(py, &coords, axes, expected_len);
    }

    let numpy = numpy_module(py)?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("dtype", numpy.getattr("float64")?)?;
    let array = numpy.getattr("asarray")?.call((value,), Some(&kwargs))?;
    let shape = array.getattr("shape")?.extract::<Vec<usize>>()?;
    let [rows, width] = shape.as_slice() else {
        return Err(InvalidGeometryError::new_err(
            "coordinates must be a two-dimensional matrix",
        ));
    };
    let (rows, width) = (*rows, *width);
    let expected_width = axis_count(axes);
    if rows != expected_len {
        return Err(replacement_len_error(expected_len, rows));
    }
    if width != expected_width {
        return Err(replacement_width_error(expected_width, width));
    }
    let cells = rows.checked_mul(width).ok_or_else(|| {
        pyo3::exceptions::PyOverflowError::new_err("coordinate replacement matrix is too large")
    })?;
    coordinates::checked_vector_len::<f64>(cells, "coordinate replacement matrix")?;
    coordinates::checked_vector_len::<f64>(rows, "coordinate replacement")?;
    let flat_obj = array.call_method0("ravel")?;
    let flat = coordinate_column_values(py, &flat_obj, "coordinates")?;
    let mut xs = Vec::with_capacity(rows);
    let mut ys = Vec::with_capacity(rows);
    let mut zs = axes.has_z().then(|| Vec::with_capacity(rows));
    let mut ms = axes.has_m().then(|| Vec::with_capacity(rows));
    let (order, n) = coordinate_axis_order(axes);
    let order = order
        .get(..n)
        .expect("axis count fits its fixed order array");
    for row in flat.chunks_exact(width) {
        for (&value, &axis) in row.iter().zip(order) {
            match axis {
                CoordinateAxis::X => xs.push(value),
                CoordinateAxis::Y => ys.push(value),
                CoordinateAxis::Z => {
                    zs.as_mut().expect("Z axis present").push(value);
                },
                CoordinateAxis::M => {
                    ms.as_mut().expect("M axis present").push(value);
                },
            }
        }
    }
    // X/Y must always be finite. Z/M may carry NaN padding for members that
    // lack those axes under the geometry's union layout; those cells are
    // ignored per-member at apply time, and members that own Z/M re-validate
    // finiteness via CoordSeq/Point constructors.
    if !crate::geometry::column_all_finite(&xs) || !crate::geometry::column_all_finite(&ys) {
        return Err(InvalidGeometryError::new_err("coordinates must be finite"));
    }
    Ok(CoordinateReplacement {
        xs: Some(SharedSlice::from_vec(xs)),
        ys: Some(SharedSlice::from_vec(ys)),
        zs: zs.map_or(ReplacementAxis::Carry, |values| {
            ReplacementAxis::Replace(SharedSlice::from_vec(values))
        }),
        ms: ms.map_or(ReplacementAxis::Carry, |values| {
            ReplacementAxis::Replace(SharedSlice::from_vec(values))
        }),
        len: expected_len,
        axes,
        positional: true,
    })
}

pub(crate) fn parse_coordinate_replacement(
    py: Python<'_>,
    args: &Bound<'_, PyTuple>,
    kwargs: Option<&Bound<'_, PyDict>>,
    axes: CoordinateAxes,
    expected_len: usize,
) -> PyResult<CoordinateReplacement> {
    let has_kwargs = kwargs.is_some_and(|kwargs| !kwargs.is_empty());
    match (args.len(), has_kwargs) {
        (0, true) => {
            coordinates::checked_vector_len::<f64>(expected_len, "coordinate replacement")?;
            replacement_from_kwargs(py, kwargs.expect("checked"), axes, expected_len)
        },
        (1, false) => replacement_from_matrix(py, &args.get_item(0)?, axes, expected_len),
        (0, false) => Err(GeometryError::new_err(
            "with_coordinates requires a coordinate matrix or at least one ordinate",
        )),
        (1, true) => {
            let coordinates = args.get_item(0)?;
            if coordinates.is_none() {
                replacement_from_kwargs(py, kwargs.expect("checked"), axes, expected_len)
            } else {
                Err(GeometryError::new_err(
                    "with_coordinates cannot mix a positional coordinate matrix with ordinate columns",
                ))
            }
        },
        _ => Err(GeometryError::new_err(
            "with_coordinates accepts at most one positional coordinate matrix",
        )),
    }
}

pub(crate) fn map_coordinates_callback(
    py: Python<'_>,
    coords: &PyCoordinates,
    func: &Bound<'_, PyAny>,
) -> PyResult<CoordinateReplacement> {
    let matrix = coords.__array__(py, None, None)?;
    let result = func.call1((matrix.bind(py),))?;
    let axes = coords.view.axes_for_python(py)?;
    let len = coords.view.len_for_python(py)?;
    replacement_from_matrix(py, &result, axes, len)
}
