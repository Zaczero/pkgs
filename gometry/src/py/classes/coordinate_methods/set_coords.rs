use std::cell::Cell;

use pyo3::types::{PyDict, PyTuple};

use crate::boundary::coordinate_input::coordinate_column_values;
use crate::geometry::{MOrdinate, SharedSlice, ZOrdinate};
use crate::py::classes::coordinate_methods::{
    Bound, CoordSeq, CoordinateAxes, CoordinateAxis, CoordinateReplacement, Point, PyAny,
    PyAnyMethods as _, PyCoordinates, PyDictMethods as _, PyErr, PyRef, PyResult,
    PyTupleMethods as _, Python, ReplacementAxis, Shape, coordinate_axis_order,
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
        "{name}=None is not allowed in set_coordinates; use set_z(None) or set_m(None) to clear ordinates"
    ))
}

fn axis_count(axes: CoordinateAxes) -> usize {
    2 + usize::from(axes.has_z()) + usize::from(axes.has_m())
}

fn reject_axis_fabrication(axes: CoordinateAxes, kwargs: &Bound<'_, PyDict>) -> PyResult<()> {
    if !axes.has_z() && kwargs.contains("z")? {
        return Err(InvalidGeometryError::new_err(
            "z coordinates require an existing Z axis; use force_3d() first",
        ));
    }
    if !axes.has_m() && kwargs.contains("m")? {
        return Err(InvalidGeometryError::new_err(
            "m coordinates require an existing M axis; use set_m() first",
        ));
    }
    Ok(())
}

fn required_axis(
    py: Python<'_>,
    kwargs: &Bound<'_, PyDict>,
    name: &str,
    expected_len: usize,
) -> PyResult<SharedSlice<f64>> {
    let value = kwargs
        .get_item(name)?
        .ok_or_else(|| InvalidGeometryError::new_err(format!("{name} is required")))?;
    if value.is_none() {
        return Err(explicit_none_error(name));
    }
    // Receiver length is known: exact collect so `itertools.repeat` cannot hang (D11).
    let values = crate::coordinate_column_values_exact(py, &value, name, expected_len, |got| {
        axis_length_error(name, expected_len, got)
    })?;
    Ok(SharedSlice::from_vec(values))
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
        return Err(explicit_none_error(name));
    }
    let values = crate::coordinate_column_values_exact(py, &value, name, expected_len, |got| {
        axis_length_error(name, expected_len, got)
    })?;
    if !crate::geometry::column_all_finite(&values) {
        return Err(InvalidGeometryError::new_err(format!(
            "{name} must be finite"
        )));
    }
    Ok(ReplacementAxis::Replace(SharedSlice::from_vec(values)))
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
            return Err(GeometryError::new_err(format!(
                "set_coordinates got an unexpected keyword argument {name:?}"
            )));
        }
    }
    reject_axis_fabrication(axes, kwargs)?;
    let xs = required_axis(py, kwargs, "x", expected_len)?;
    let ys = required_axis(py, kwargs, "y", expected_len)?;
    // Validate XY finiteness directly — no validation-only CoordSeq.
    if !crate::geometry::column_all_finite(&xs) || !crate::geometry::column_all_finite(&ys) {
        let error: crate::error::Error =
            crate::geometry::GeometryErrorKind::NonFiniteCoordinate.into();
        return Err(error.into());
    }
    Ok(CoordinateReplacement {
        xs,
        ys,
        zs: optional_replace_axis(py, kwargs, "z", expected_len)?,
        ms: optional_replace_axis(py, kwargs, "m", expected_len)?,
        len: expected_len,
        axes,
        positional: false,
    })
}

fn replacement_from_coordinates_view(
    coords: &PyCoordinates,
    axes: CoordinateAxes,
    expected_len: usize,
) -> PyResult<CoordinateReplacement> {
    let layout = coords.layout.unwrap_or_else(|| coords.view.axes());
    if layout != axes {
        return Err(InvalidGeometryError::new_err(format!(
            "coordinates axes must be {}; got {}",
            axes.as_str(),
            layout.as_str()
        )));
    }
    let rows = coords.view.len();
    if rows != expected_len {
        return Err(replacement_len_error(expected_len, rows));
    }
    if let Some(seq) = coords.view.single_seq()
        && seq.axes() == axes
        && seq.len() == expected_len
    {
        return Ok(CoordinateReplacement {
            xs: seq.carried_xs(),
            ys: seq.carried_ys(),
            zs: seq
                .carried_zs()
                .map_or(ReplacementAxis::Carry, ReplacementAxis::Replace),
            ms: seq
                .ms()
                .map(|values| ReplacementAxis::Replace(SharedSlice::from_vec(values.to_vec())))
                .unwrap_or(ReplacementAxis::Carry),
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
        xs: SharedSlice::from_vec(xs),
        ys: SharedSlice::from_vec(ys),
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
        return replacement_from_coordinates_view(&coords, axes, expected_len);
    }

    let numpy = numpy_module(py)?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("dtype", numpy.getattr("float64")?)?;
    let array = numpy.getattr("asarray")?.call((value,), Some(&kwargs))?;
    let shape = array.getattr("shape")?.extract::<Vec<usize>>()?;
    if shape.len() != 2 {
        return Err(InvalidGeometryError::new_err(
            "coordinates must be a two-dimensional matrix",
        ));
    }
    let rows = shape[0];
    let width = shape[1];
    let expected_width = axis_count(axes);
    if rows != expected_len {
        return Err(replacement_len_error(expected_len, rows));
    }
    if width != expected_width {
        return Err(replacement_width_error(expected_width, width));
    }
    let flat_obj = array.call_method0("ravel")?;
    let flat = coordinate_column_values(py, &flat_obj, "coordinates")?;
    let mut xs = Vec::with_capacity(rows);
    let mut ys = Vec::with_capacity(rows);
    let mut zs = axes.has_z().then(|| Vec::with_capacity(rows));
    let mut ms = axes.has_m().then(|| Vec::with_capacity(rows));
    let (order, n) = coordinate_axis_order(axes);
    for row in flat.chunks_exact(width) {
        for (&value, axis) in row.iter().zip(&order[..n]) {
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
        xs: SharedSlice::from_vec(xs),
        ys: SharedSlice::from_vec(ys),
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
        (0, true) => replacement_from_kwargs(py, kwargs.expect("checked"), axes, expected_len),
        (1, false) => replacement_from_matrix(py, &args.get_item(0)?, axes, expected_len),
        (0, false) => Err(GeometryError::new_err(
            "set_coordinates requires a coordinate matrix or x= and y= columns",
        )),
        (1, true) => Err(GeometryError::new_err(
            "set_coordinates cannot mix a positional coordinate matrix with x/y/z/m columns",
        )),
        _ => Err(GeometryError::new_err(
            "set_coordinates accepts at most one positional coordinate matrix",
        )),
    }
}

fn replacement_seq(
    old: &CoordSeq,
    replacement: &CoordinateReplacement,
    cursor: &Cell<usize>,
) -> PyResult<CoordSeq> {
    // Positional matrices use the geometry's *union* axes. Each independent
    // sequence keeps its own axes (XY member stays XY; padded union Z/M is
    // ignored at apply). Reject only demotion (member has an axis the matrix
    // lacks). Keyword `z=`/`m=` still go through z_column_for_seq, which
    // rejects fabricating axes on members that lack them.
    if replacement.positional
        && ((old.axes().has_z() && !replacement.axes.has_z())
            || (old.axes().has_m() && !replacement.axes.has_m()))
    {
        return Err(InvalidGeometryError::new_err(
            "coordinates must preserve each coordinate sequence axes",
        ));
    }
    let start = cursor.get();
    let end = start + old.len();
    cursor.set(end);
    CoordSeq::try_from_vecs(
        replacement.xs[start..end].to_vec(),
        replacement.ys[start..end].to_vec(),
        replacement
            .z_column_for_seq(old, start..end)?
            .map(|values| values.as_slice().to_vec()),
        replacement
            .m_column_for_seq(old, start..end)?
            .map(|values| values.as_slice().to_vec()),
    )
    .map_err(PyErr::from)
}

fn replacement_point(
    old: &Point,
    replacement: &CoordinateReplacement,
    cursor: &Cell<usize>,
) -> PyResult<Point> {
    if replacement.positional
        && ((old.axes.has_z() && !replacement.axes.has_z())
            || (old.axes.has_m() && !replacement.axes.has_m()))
    {
        return Err(InvalidGeometryError::new_err(
            "coordinates must preserve each coordinate sequence axes",
        ));
    }
    let index = cursor.get();
    cursor.set(index + 1);
    Point::new_axes(
        replacement.xs[index],
        replacement.ys[index],
        ZOrdinate(replacement.z_at(*old, index)?),
        MOrdinate(replacement.m_at(*old, index)?),
    )
    .map_err(PyErr::from)
}

fn validate_ring(seq: &CoordSeq) -> PyResult<()> {
    match crate::array::CheckedRingWindow::checked(seq, 0..seq.len()) {
        Ok(_) => Ok(()),
        Err(crate::array::RingWindowError::TooShort(len)) => {
            let error: crate::error::Error =
                crate::geometry::GeometryErrorKind::RingTooShort(len).into();
            Err(error.into())
        },
        Err(crate::array::RingWindowError::Open) => {
            Err(InvalidGeometryError::new_err("polygon ring must be closed"))
        },
        Err(
            crate::array::RingWindowError::Inverted | crate::array::RingWindowError::OutOfRange,
        ) => Err(InvalidGeometryError::new_err(
            "polygon ring range is invalid",
        )),
    }
}

pub(crate) fn validate_shape_rings(shape: &Shape) -> PyResult<()> {
    match shape {
        Shape::Polygon(polygon) => {
            for ring in polygon.rings() {
                validate_ring(ring)?;
            }
        },
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons {
                for ring in polygon.rings() {
                    validate_ring(ring)?;
                }
            }
        },
        Shape::GeometryCollection(items) => {
            for item in items {
                validate_shape_rings(item)?;
            }
        },
        _ => {},
    }
    Ok(())
}

pub(crate) fn replace_shape_coordinates(
    shape: &Shape,
    replacement: &CoordinateReplacement,
) -> PyResult<Shape> {
    let cursor = Cell::new(0);
    let replaced = shape.try_map_coordseqs(
        |seq| replacement_seq(seq, replacement, &cursor),
        |point| replacement_point(point, replacement, &cursor),
    )?;
    if cursor.get() != replacement.len {
        return Err(replacement_len_error(replacement.len, cursor.get()));
    }
    validate_shape_rings(&replaced)?;
    Ok(replaced)
}

pub(crate) fn slice_replacement_for_shape(
    replacement: &CoordinateReplacement,
    start: usize,
    len: usize,
) -> CoordinateReplacement {
    let end = start + len;
    CoordinateReplacement {
        xs: SharedSlice::from_vec(replacement.xs[start..end].to_vec()),
        ys: SharedSlice::from_vec(replacement.ys[start..end].to_vec()),
        zs: match &replacement.zs {
            ReplacementAxis::Replace(values) => {
                ReplacementAxis::Replace(SharedSlice::from_vec(values[start..end].to_vec()))
            },
            ReplacementAxis::Carry => ReplacementAxis::Carry,
        },
        ms: match &replacement.ms {
            ReplacementAxis::Replace(values) => {
                ReplacementAxis::Replace(SharedSlice::from_vec(values[start..end].to_vec()))
            },
            ReplacementAxis::Carry => ReplacementAxis::Carry,
        },
        len,
        axes: replacement.axes,
        positional: replacement.positional,
    }
}

pub(crate) fn map_coordinates_callback(
    py: Python<'_>,
    coords: PyCoordinates,
    func: &Bound<'_, PyAny>,
) -> PyResult<CoordinateReplacement> {
    let matrix = coords.__array__(py, None, None)?;
    let result = func.call1((matrix.bind(py),))?;
    let axes = coords.view.axes();
    replacement_from_matrix(py, &result, axes, coords.view.len())
}
