use pyo3::prelude::{Py, PyAny, PyResult, Python};

use crate::PyGeometryArray;
use crate::py::arrow::ArrowEncoding;
use crate::py::arrow_c;
use crate::py::arrow_c::PreparedArrowExportPlan;

fn materialize_pyarrow(py: Python<'_>, plan: PreparedArrowExportPlan) -> PyResult<Py<PyAny>> {
    let export = arrow_c::materialize_export_plan(plan)?;
    arrow_c::pyarrow_array_from_export(py, export)
}

/// Build and pack an array export without holding Python state in the native
/// planning path. Capsule import is the attached egress for PyArrow.
pub(crate) fn array_to_arrow(
    py: Python<'_>,
    array: &PyGeometryArray,
    encoding: ArrowEncoding,
) -> PyResult<Py<PyAny>> {
    let array = array.clone();
    let plan = py
        .detach(move || {
            let plan = arrow_c::plan_geometry_array(&array, encoding)?;
            arrow_c::prepare_export_plan(plan)
        })
        .map_err(|error| error.into_py_err(py))?;
    materialize_pyarrow(py, plan)
}

pub(crate) fn shapes_to_arrow(
    py: Python<'_>,
    shapes: &[crate::geometry::Shape],
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<Py<PyAny>> {
    let shapes = shapes.to_owned();
    let crs = crs.map(str::to_owned);
    let plan = py
        .detach(move || {
            let plan = arrow_c::plan_shapes(&shapes, crs, epoch)?;
            arrow_c::prepare_export_plan(plan)
        })
        .map_err(|error| error.into_py_err(py))?;
    materialize_pyarrow(py, plan)
}

pub(crate) fn shapes_to_wkb_arrow(
    py: Python<'_>,
    shapes: &[crate::geometry::Shape],
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<Py<PyAny>> {
    let shapes = shapes.to_owned();
    let crs = crs.map(str::to_owned);
    let plan = py
        .detach(move || {
            let plan = arrow_c::plan_wkb_shapes(&shapes, crs, epoch)?;
            arrow_c::prepare_export_plan(plan)
        })
        .map_err(|error| error.into_py_err(py))?;
    materialize_pyarrow(py, plan)
}
