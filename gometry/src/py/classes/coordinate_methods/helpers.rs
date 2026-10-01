use pyo3::prelude::*;
use pyo3::types::PyTuple;

use crate::broadcast::{GeometryInput, classify_required};
use crate::py::classes::coordinate_methods::{
    Arc, CoordinateAxes, CoordinateAxis, Point, PyCoordinates, PyGeometryArray, coordinates,
    point_tuple,
};
use crate::py::errors::parameter_error;
use crate::py::numpy::{
    float64_array, float64_matrix, float64_slice_array, int64_array, optional_float64_array,
};

/// Resolve an axis layout token to [`CoordinateAxes`]; `concept` names the
/// rejected parameter in the canonical error template.
pub(crate) fn parse_coordinate_axes(value: &str, concept: &str) -> PyResult<CoordinateAxes> {
    match value {
        "XY" => Ok(CoordinateAxes::XY),
        "XYZ" => Ok(CoordinateAxes::XYZ),
        "XYM" => Ok(CoordinateAxes::XYM),
        "XYZM" => Ok(CoordinateAxes::XYZM),
        other => Err(parameter_error(
            crate::tokens::unknown_token_message(concept, other, &["XY", "XYZ", "XYM", "XYZM"]),
            concept,
        )),
    }
}

/// The ordered axes of a layout: ``X``, ``Y``, then ``Z``/``M`` when present.
pub(crate) const fn coordinate_axis_order(axes: CoordinateAxes) -> ([CoordinateAxis; 4], usize) {
    match (axes.has_z(), axes.has_m()) {
        (false, false) => (
            [
                CoordinateAxis::X,
                CoordinateAxis::Y,
                CoordinateAxis::X,
                CoordinateAxis::X,
            ],
            2,
        ),
        (true, false) => (
            [
                CoordinateAxis::X,
                CoordinateAxis::Y,
                CoordinateAxis::Z,
                CoordinateAxis::X,
            ],
            3,
        ),
        (false, true) => (
            [
                CoordinateAxis::X,
                CoordinateAxis::Y,
                CoordinateAxis::M,
                CoordinateAxis::X,
            ],
            3,
        ),
        (true, true) => (
            [
                CoordinateAxis::X,
                CoordinateAxis::Y,
                CoordinateAxis::Z,
                CoordinateAxis::M,
            ],
            4,
        ),
    }
}

pub(crate) fn coordinate_ordinate(point: Point, axis: CoordinateAxis) -> Option<f64> {
    match axis {
        CoordinateAxis::X => Some(point.x),
        CoordinateAxis::Y => Some(point.y),
        CoordinateAxis::Z => point.z(),
        CoordinateAxis::M => point.m(),
    }
}

/// Materialize a row-major matrix, interleaving contiguous columns directly.
pub(crate) fn coordinate_matrix_data(
    py: Python<'_>,
    view: &coordinates::CoordinateView,
    axes: CoordinateAxes,
    missing: f64,
) -> PyResult<(usize, Vec<f64>)> {
    let (order, dims) = coordinate_axis_order(axes);
    let order = order
        .get(..dims)
        .expect("axis count fits its fixed order array");
    let rows = view.len_for_python(py)?;
    let count = rows.checked_mul(dims).ok_or_else(|| {
        pyo3::exceptions::PyOverflowError::new_err("coordinate matrix is too large")
    })?;
    let capacity = coordinates::checked_vector_len::<f64>(count, "coordinate matrix")?;
    let mut data = vec![0.0; capacity];
    let mut output = data.as_mut_slice();
    view.for_each(&mut |_, run| {
        let count = match run {
            crate::geometry::CoordRun::Seq(seq) => seq.len(),
            crate::geometry::CoordRun::Point(_) => 1,
        }
        .wrapping_mul(dims);
        let (current, remaining) = std::mem::take(&mut output).split_at_mut(count);
        output = remaining;
        match run {
            crate::geometry::CoordRun::Seq(seq) => {
                interleave_coordinate_columns(seq, axes, order, missing, current);
            },
            crate::geometry::CoordRun::Point(point) => {
                for (value, &axis) in current.iter_mut().zip(order) {
                    *value = coordinate_ordinate(point, axis).unwrap_or(missing);
                }
            },
        }
    });
    Ok((rows, data))
}

fn interleave_coordinate_columns(
    seq: &crate::geometry::CoordSeq,
    axes: CoordinateAxes,
    order: &[CoordinateAxis],
    missing: f64,
    data: &mut [f64],
) {
    let xs = seq.xs();
    let ys = seq.ys();
    let zs = seq.zs();
    let ms = seq.ms();
    if axes == CoordinateAxes::XY {
        for (output, (&x, &y)) in data
            .as_chunks_mut::<2>()
            .0
            .iter_mut()
            .zip(xs.iter().zip(ys))
        {
            *output = [x, y];
        }
    } else {
        for (i, ((output, &x), &y)) in data
            .chunks_exact_mut(order.len())
            .zip(xs)
            .zip(ys)
            .enumerate()
        {
            for (value, &axis) in output.iter_mut().zip(order) {
                *value = match axis {
                    CoordinateAxis::X => x,
                    CoordinateAxis::Y => y,
                    CoordinateAxis::Z => zs.map_or(missing, |column| {
                        *column
                            .get(i)
                            .expect("Z coordinates match the admitted row count")
                    }),
                    CoordinateAxis::M => ms.map_or(missing, |column| {
                        *column
                            .get(i)
                            .expect("M coordinates match the admitted row count")
                    }),
                };
            }
        }
    }
}

/// Format one coordinate as a Python tuple, honoring a fixed `select` layout
/// (absent Z/M become `None`) or the coordinate's native axes.
pub(crate) fn coordinate_tuple(
    py: Python<'_>,
    point: Point,
    layout: Option<CoordinateAxes>,
) -> PyResult<Py<PyAny>> {
    let Some(axes) = layout else {
        return point_tuple(py, point);
    };
    let (order, n) = coordinate_axis_order(axes);
    let order = order
        .get(..n)
        .expect("axis count fits its fixed order array");
    let values = order
        .iter()
        .map(|&axis| Ok(coordinate_ordinate(point, axis).into_pyobject(py)?.unbind()))
        .collect::<PyResult<Vec<_>>>()?;
    Ok(PyTuple::new(py, values)?.into_any().unbind())
}

impl<'a, 'py> FromPyObject<'a, 'py> for CoordinateAxes {
    type Error = PyErr;

    fn extract(value: pyo3::Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        parse_coordinate_axes(value.extract::<&str>()?, "axes")
    }
}

/// One axis → frozen float64 ndarray of view length (NaN when the axis is
/// absent). Shared by `.x`/`.y`/`.z`/`.m` and `to_dict`.
pub(crate) fn column_axis_to_py(
    py: Python<'_>,
    slf: Bound<'_, PyCoordinates>,
    axis: CoordinateAxis,
) -> PyResult<Py<PyAny>> {
    let owner = slf.clone().into_any();
    let this = slf.borrow();
    match this.view.try_column(py, axis)? {
        coordinates::CoordinateColumnRef::Missing => {
            let count = this.view.len_for_python(py)?;
            let capacity = coordinates::checked_vector_len::<f64>(count, "coordinate column")?;
            float64_array(py, vec![f64::NAN; capacity])
        },
        column @ (coordinates::CoordinateColumnRef::Borrowed(_)
        | coordinates::CoordinateColumnRef::Dense(_)
        | coordinates::CoordinateColumnRef::Nullable(_)) => column_ref_to_py(py, owner, column),
    }
}

pub(crate) fn column_ref_to_py(
    py: Python<'_>,
    owner: Bound<'_, PyAny>,
    column: coordinates::CoordinateColumnRef<'_>,
) -> PyResult<Py<PyAny>> {
    use coordinates::CoordinateColumnRef::{Borrowed, Dense, Missing, Nullable};
    match column {
        Borrowed(slice) => {
            // SAFETY: `owner` pins the immutable `Coordinates` object; the
            // borrowed slice points into its stable backing storage.
            unsafe { float64_slice_array(owner, slice) }
        },
        Dense(values) => float64_array(py, values),
        Nullable(values) => optional_float64_array(py, values),
        Missing => unreachable!("column_axis_to_py handles Missing"),
    }
}

pub(crate) fn coordinate_view_for_input(input: GeometryInput<'_>) -> coordinates::CoordinateView {
    match input {
        GeometryInput::One(geometry) => {
            coordinates::CoordinateView::from_shape(Arc::clone(&geometry.shape))
        },
        GeometryInput::Many(array) => array.coordinate_view(),
    }
}

/// Extract coordinates as one flat dense matrix.
///
/// Parameters
/// ----------
/// values : Geometry, GeometryArray, or iterable of Geometry/None
///     Values are flattened depth-first in source-row order. Missing and empty
///     geometries contribute no rows, but array and iterable source indexes
///     remain available through `return_index=True`.
///     The matrix includes every axis carried by the coordinates; heterogeneous
///     rows use ``NaN`` where a coordinate lacks an axis.
/// return_index : bool, default False
///     Return a source-row index alongside the matrix. Scalar geometries use
///     index zero for every coordinate.
///
/// Returns
/// -------
/// numpy.ndarray or tuple of numpy.ndarray
///     A dense matrix, or `(matrix, index)` when `return_index=True`.
///
/// Raises
/// ------
/// TypeError
///     If ``values`` is not geometry-like.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.get_coordinates(gm.Point(1, 2)).tolist()
/// [[1.0, 2.0]]
/// >>> matrix, indexes = gm.get_coordinates(
/// ...     [gm.Point(1, 2), None], return_index=True
/// ... )
/// >>> matrix.tolist(), indexes.tolist()
/// ([[1.0, 2.0]], [0])
#[pyfunction]
#[pyo3(signature = (values, *, return_index = false))]
pub(crate) fn get_coordinates(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    return_index: bool,
) -> PyResult<Py<PyAny>> {
    let array;
    let input = if let Ok(input) = classify_required(values) {
        input
    } else {
        array = PyGeometryArray::new(values, None, None)?;
        GeometryInput::Many(&array)
    };
    let view = coordinate_view_for_input(input);
    let axes = view.axes_for_python(py)?;
    let (_, dims) = coordinate_axis_order(axes);
    let (rows, data) = coordinate_matrix_data(py, &view, axes, f64::NAN)?;
    let matrix = float64_matrix(py, data, rows, dims)?;
    if return_index {
        let indexes = int64_array(py, view.try_row_index_i64(py)?)?;
        Ok((matrix, indexes).into_pyobject(py)?.unbind().into())
    } else {
        Ok(matrix)
    }
}
