//! Owned Arrow schema exposed independently of Python Arrow providers.

use std::sync::Arc;

use pyo3::IntoPyObjectExt as _;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyCapsule, PyDict};

use super::admitted::{AdmittedArrowSchema, classify_admitted_geometry_schema};
use super::values::export_schema;

#[pyclass(name = "_ArrowSchema", module = "gometry._lib", frozen, immutable_type)]
pub(crate) struct OwnedArrowSchema {
    pub(super) schema: Arc<AdmittedArrowSchema>,
}

#[pymethods]
impl OwnedArrowSchema {
    #[getter]
    fn names(&self) -> Vec<String> {
        self.schema
            .children
            .iter()
            .map(|child| child.name().to_owned())
            .collect()
    }

    #[getter]
    fn metadata(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let result = PyDict::new(py);
        for (key, value) in self.schema.metadata.iter() {
            result.set_item(PyBytes::new(py, key), PyBytes::new(py, value))?;
        }
        Ok(result.unbind())
    }

    #[getter]
    fn geometry_columns(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let mut result = Vec::new();
        for child in self.schema.children.iter() {
            if child.format() == "+s" && child.extension_name()?.is_none() {
                continue;
            }
            match classify_admitted_geometry_schema(child) {
                Ok(_) => result.push(child.name().to_owned()),
                Err(error) if error.is_instance_of::<PyTypeError>(py) => {},
                Err(error) => return Err(error),
            }
        }
        Ok(result)
    }

    #[getter]
    fn geoarrow_columns(&self) -> PyResult<Vec<String>> {
        let mut result = Vec::new();
        for child in self.schema.children.iter() {
            if child
                .extension_name()?
                .is_some_and(|name| name.starts_with("geoarrow."))
            {
                result.push(child.name().to_owned());
            }
        }
        Ok(result)
    }

    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        PyCapsule::new_with_value(py, export_schema(&self.schema, true)?, c"arrow_schema")
    }

    fn __reduce__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let schema = super::state::schema_to_python(py, &self.schema)?;
        let restore = py
            .import("gometry._lib")?
            .getattr("_restore_arrow_schema")?;
        (restore, (schema,)).into_py_any(py)
    }
}

#[pyfunction(name = "_restore_arrow_schema")]
fn restore_arrow_schema(
    py: Python<'_>,
    schema: &Bound<'_, PyAny>,
) -> PyResult<Py<OwnedArrowSchema>> {
    Py::new(py, OwnedArrowSchema {
        schema: Arc::new(super::state::schema_from_python(schema)?),
    })
}

pub(super) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<OwnedArrowSchema>()?;
    module.add_function(wrap_pyfunction!(restore_arrow_schema, module)?)?;
    Ok(())
}
