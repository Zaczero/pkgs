#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyTuple};

use super::*;
use crate::broadcast::{GeometryInput, classify_required};
use crate::geometry::{MOrdinate, ZOrdinate};
use crate::py::errors::parameter_error;
use crate::py::numpy::{
    float64_array, float64_matrix, float64_slice_array, optional_float64_array,
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
    let mut order = [
        CoordinateAxis::X,
        CoordinateAxis::Y,
        CoordinateAxis::X,
        CoordinateAxis::X,
    ];
    let mut n = 2;
    if axes.has_z() {
        order[n] = CoordinateAxis::Z;
        n += 1;
    }
    if axes.has_m() {
        order[n] = CoordinateAxis::M;
        n += 1;
    }
    (order, n)
}

pub(crate) fn coordinate_ordinate(point: Point, axis: CoordinateAxis) -> Option<f64> {
    match axis {
        CoordinateAxis::X => Some(point.x),
        CoordinateAxis::Y => Some(point.y),
        CoordinateAxis::Z => point.z(),
        CoordinateAxis::M => point.m(),
    }
}

/// Row-major `(rows, dims)` interleave from one contiguous [`CoordSeq`].
pub(crate) fn interleave_coordseq_ordered(
    seq: &CoordSeq,
    order: &[CoordinateAxis],
    missing: f64,
) -> Vec<f64> {
    let rows = seq.len();
    let dims = order.len();
    let mut data = vec![0.0_f64; rows * dims];
    let xs = seq.xs();
    let ys = seq.ys();
    let zs = seq.zs();
    let ms = seq.ms();
    if dims == 2
        && order.len() == 2
        && order[0] == CoordinateAxis::X
        && order[1] == CoordinateAxis::Y
    {
        for i in 0..rows {
            let base = i * 2;
            data[base] = xs[i];
            data[base + 1] = ys[i];
        }
    } else {
        for i in 0..rows {
            let base = i * dims;
            for (d, &axis) in order.iter().enumerate() {
                data[base + d] = match axis {
                    CoordinateAxis::X => xs[i],
                    CoordinateAxis::Y => ys[i],
                    CoordinateAxis::Z => zs.map_or(missing, |column| column[i]),
                    CoordinateAxis::M => ms.map_or(missing, |column| column[i]),
                };
            }
        }
    }
    data
}

/// Format one coordinate as a Python tuple, honoring a fixed `select` layout
/// (absent Z/M become `None`) or the coordinate's native axes. Shared by
/// `Coordinates` indexing/iteration and the lazy iterator.
pub(crate) fn parse_coordinate_member(item: &Bound<'_, PyAny>) -> Option<Point> {
    let values = coordinate_values(item.py(), item, "coordinate").ok()?;
    if !(2..=4).contains(&values.len()) || values.iter().any(|value| !value.is_finite()) {
        return None;
    }
    Point::new_axes(
        values[0],
        values[1],
        ZOrdinate(values.get(2).copied()),
        MOrdinate(values.get(3).copied()),
    )
    .ok()
}

pub(crate) fn coordinate_tuple(
    py: Python<'_>,
    point: Point,
    layout: Option<CoordinateAxes>,
) -> PyResult<Py<PyAny>> {
    let Some(axes) = layout else {
        return point_tuple(py, point);
    };
    let (order, n) = coordinate_axis_order(axes);
    let values = order[..n]
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
    match this.view.column(axis) {
        coordinates::CoordinateColumnRef::Missing => {
            float64_array(py, vec![f64::NAN; this.view.len()])
        },
        column => column_ref_to_py(py, owner, column),
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
            coordinates::CoordinateView::from_shape(geometry.shape.clone())
        },
        GeometryInput::Many(array) => array.coordinate_view(),
    }
}

/// Extract coordinates as a dense ``(N, k)`` ``float64`` matrix (Shapely
/// ``get_coordinates`` parity).
///
/// Parameters
/// ----------
/// values : Geometry, GeometryArray, or iterable of Geometry/None
///     The geometry values whose coordinates are flattened depth-first.
/// axes : {'XY', 'XYZ', 'XYM', 'XYZM'}, optional
///     Output coordinate columns. ``None`` means ``'XY'``.
/// return_index : bool, default False
///     Also return the per-coordinate source-geometry row index.
///
/// Returns
/// -------
/// numpy.ndarray or tuple of numpy.ndarray
///     The ``(N, k)`` coordinate matrix, or ``(matrix, index)`` when
///     ``return_index=True``. Missing
///     `GeometryArray` rows contribute no coordinates; the matrix is a
///     flattened vertex stream, not row-aligned. Use ``return_index=True`` to
///     recover source-row alignment, or call ``drop_missing()`` before
///     extraction for an explicit dense path.
///
/// Raises
/// ------
/// TypeError
///     If ``values`` is not geometry-like.
#[pyfunction]
#[pyo3(signature = (values, *, axes = None, return_index = false))]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.get_coordinates(gm.Point(1, 2)).tolist()
/// [[1.0, 2.0]]
pub(crate) fn get_coordinates(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    axes: Option<CoordinateAxes>,
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
    let axes = axes.unwrap_or(CoordinateAxes::XY);
    let (order, dims) = coordinate_axis_order(axes);
    let order = &order[..dims];
    let rows = view.len();
    let mut indexes = return_index.then(|| Vec::with_capacity(rows));
    let data = if !return_index
        && let Some(seq) = view.single_seq()
        && seq.len() == rows
    {
        interleave_coordseq_ordered(seq, order, f64::NAN)
    } else {
        let mut data = Vec::with_capacity(rows * dims);
        view.for_each_point(|coord| {
            for &axis in order {
                data.push(coordinate_ordinate(coord.point, axis).unwrap_or(f64::NAN));
            }
            if let Some(indexes) = indexes.as_mut() {
                indexes.push(coord.path.geometry.unwrap_or(0) as i64);
            }
        });
        data
    };
    let matrix = float64_matrix(py, data, rows, dims)?;
    if let Some(indexes) = indexes {
        Ok((matrix, crate::py::numpy::int64_array(py, indexes)?)
            .into_pyobject(py)?
            .unbind()
            .into())
    } else {
        Ok(matrix)
    }
}
