#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::IntoPyObjectExt as _;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyTuple};

use crate::broadcast::{GeometryInput, classify_required};
use crate::py::classes::coordinate_methods::{
    Arc, CoordSeq, CoordinateAxes, CoordinateAxis, Point, PyCoordinates, PyGeometryArray,
    coordinates, point_tuple,
};
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
) -> PyResult<Vec<f64>> {
    let rows = seq.len();
    let dims = order.len();
    let count = rows.checked_mul(dims).ok_or_else(|| {
        pyo3::exceptions::PyOverflowError::new_err("coordinate matrix size overflows usize")
    })?;
    let mut data = crate::try_vec_with_capacity(count)?;
    data.resize(count, 0.0);
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
    Ok(data)
}

/// Format one coordinate as a Python tuple, honoring a fixed `select` layout
/// (absent Z/M become `None`) or the coordinate's native axes. Shared by
/// `Coordinates` indexing/iteration, membership, and the lazy iterator.
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
    match this.view.column(axis)? {
        coordinates::CoordinateColumnRef::Missing => {
            let mut values = crate::try_vec_with_capacity(this.view.len())?;
            for _ in 0..this.view.len() {
                crate::try_push(&mut values, f64::NAN)?;
            }
            float64_array(py, values)
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
            coordinates::CoordinateView::from_shape(Arc::clone(&geometry.shape))
        },
        GeometryInput::Many(array) => array.coordinate_view(),
    }
}

fn coordinate_groups(
    array: &PyGeometryArray,
    order: &[CoordinateAxis],
) -> crate::error::Result<crate::CsrBuilder<[f64; 4]>> {
    let view = array.coordinate_view();
    let mut builder = crate::CsrBuilder::<[f64; 4]>::new();
    let mut active_row = None;
    let mut next_row = 0_usize;
    let result = view.try_for_each_point(&mut |coord| {
        let row = coord.path.geometry.expect("array view tags every point");
        if active_row != Some(row) {
            if active_row.is_some() {
                let end = builder.len();
                if let Err(error) = builder.push_end(end) {
                    return std::ops::ControlFlow::Break(error);
                }
                next_row += 1;
            }
            while next_row < row {
                let end = builder.len();
                if let Err(error) = builder.push_end(end) {
                    return std::ops::ControlFlow::Break(error);
                }
                next_row += 1;
            }
            active_row = Some(row);
        }
        let mut values = [0.0; 4];
        for (index, &axis) in order.iter().enumerate() {
            values[index] = coordinate_ordinate(coord.point, axis).unwrap_or(f64::NAN);
        }
        match builder.push(values) {
            Ok(()) => std::ops::ControlFlow::Continue(()),
            Err(error) => std::ops::ControlFlow::Break(error),
        }
    });
    if let std::ops::ControlFlow::Break(error) = result {
        return Err(error);
    }
    if active_row.is_some() {
        let end = builder.len();
        builder.push_end(end)?;
        next_row += 1;
    }
    while next_row < array.len() {
        let end = builder.len();
        builder.push_end(end)?;
        next_row += 1;
    }
    Ok(builder)
}

#[cfg(test)]
mod coordinate_groups_tests {
    use super::*;
    use crate::boundary::Frame;
    use crate::error::{ErrorKind, ResourceError};
    use crate::geometry::Shape;

    #[test]
    fn mixed_empty_rows_preserve_coordinate_spans() {
        let array = PyGeometryArray::from_shapes(
            vec![
                Shape::Point(Point::new_raw_xy(1.0, 2.0)),
                Shape::empty_polygon(),
                Shape::Point(Point::new_raw_xy(3.0, 4.0)),
                Shape::empty_polygon(),
            ],
            Frame::None,
        )
        .unwrap();
        let (values, offsets) = coordinate_groups(&array, &[CoordinateAxis::X, CoordinateAxis::Y])
            .unwrap()
            .finish()
            .unwrap();
        assert_eq!(offsets, [0, 1, 1, 2, 2]);
        assert_eq!(values, [[1.0, 2.0, 0.0, 0.0], [3.0, 4.0, 0.0, 0.0]]);
    }

    #[test]
    fn csr_reserve_failure_after_array_setup_is_native() {
        let array = PyGeometryArray::packed_points(
            CoordSeq::try_from_vecs(vec![1.0, 3.0], vec![2.0, 4.0], None, None).unwrap(),
            Frame::None,
        )
        .unwrap();
        let error = crate::test_support::fail_allocations_after(0, || {
            coordinate_groups(&array, &[CoordinateAxis::X, CoordinateAxis::Y])
                .err()
                .unwrap()
        });
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));

        let view = array.coordinate_view();
        let mut builder = crate::CsrBuilder::new();
        let mut visited = 0;
        let result = crate::test_support::fail_allocations_after(0, || {
            view.try_for_each_point(&mut |coord| {
                visited += 1;
                match builder.push(coord.point) {
                    Ok(()) => std::ops::ControlFlow::Continue(()),
                    Err(error) => std::ops::ControlFlow::Break(error),
                }
            })
        });
        assert_eq!(visited, 1);
        assert!(matches!(result, std::ops::ControlFlow::Break(error)
            if matches!(error.kind(), ErrorKind::Resource(ResourceError::Allocation))));
    }
}

/// Extract scalar coordinates as a dense matrix, or array coordinates as
/// row-preserving `Groups` of matrices.
///
/// Parameters
/// ----------
/// values : Geometry, GeometryArray, or iterable of Geometry/None
///     Scalar values are flattened depth-first; a `GeometryArray` keeps one
///     matrix per source row.
/// axes : {'XY', 'XYZ', 'XYM', 'XYZM'}, optional
///     Output coordinate columns. ``None`` means ``'XY'``.
/// return_index : bool, default False
///     Return a scalar source-row index alongside the matrix. For arrays,
///     `Groups.offsets` already carries the row map and this flag is ignored.
///
/// Returns
/// -------
/// numpy.ndarray, tuple of numpy.ndarray, or Groups
///     Scalar dense output, scalar `(matrix, index)` output, or one dense
///     matrix per array row. Empty and missing rows remain empty groups.
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
    let (input, grouped) = if let Ok(input) = classify_required(values) {
        (input, matches!(input, GeometryInput::Many(_)))
    } else {
        array = PyGeometryArray::new(values, None, None)?;
        (GeometryInput::Many(&array), false)
    };
    let axes = axes.unwrap_or(CoordinateAxes::XY);
    let (order, dims) = coordinate_axis_order(axes);
    let order = &order[..dims];
    if grouped && let GeometryInput::Many(array) = input {
        let builder = coordinate_groups(array, order)?;
        return crate::py::vectors::Groups::from_coordinate_chunks(dims, builder)?.into_py_any(py);
    }
    let view = coordinate_view_for_input(input);
    let rows = view.len();
    // Coordinate matrix: always ride the SoA interleave when the view is one
    // contiguous packed column, independent of `return_index`. The index
    // column is built separately at coordinate-run altitude (`row_index_i64`).
    let data = if let Some(seq) = view.single_seq()
        && seq.len() == rows
    {
        interleave_coordseq_ordered(seq, order, f64::NAN)?
    } else {
        let mut data = crate::try_vec_with_capacity(rows.checked_mul(dims).ok_or_else(|| {
            pyo3::exceptions::PyOverflowError::new_err("coordinate matrix size overflows usize")
        })?)?;
        match view.try_for_each_point(&mut |coord| {
            for &axis in order {
                match crate::try_push(
                    &mut data,
                    coordinate_ordinate(coord.point, axis).unwrap_or(f64::NAN),
                ) {
                    Ok(()) => {},
                    Err(error) => return std::ops::ControlFlow::Break(error),
                }
            }
            std::ops::ControlFlow::Continue(())
        }) {
            std::ops::ControlFlow::Continue(()) => data,
            std::ops::ControlFlow::Break(error) => return Err(error),
        }
    };
    let matrix = float64_matrix(py, data, rows, dims)?;
    if return_index {
        let indexes = crate::py::numpy::int64_array(py, view.row_index_i64()?)?;
        Ok((matrix, indexes).into_pyobject(py)?.unbind().into())
    } else {
        Ok(matrix)
    }
}
