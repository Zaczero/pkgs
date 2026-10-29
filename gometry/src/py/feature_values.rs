//! Admission of ordinary feature property values into owned snapshots.

use std::collections::HashSet;

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple,
};

use crate::GeometryError;
use crate::py::support::is_mapping_like;

fn atomic(value: &Bound<'_, PyAny>) -> bool {
    value.is_none()
        || value.cast_exact::<PyBool>().is_ok()
        || value.cast_exact::<PyInt>().is_ok()
        || value.cast_exact::<PyFloat>().is_ok()
        || value.cast_exact::<PyString>().is_ok()
        || value.cast_exact::<PyBytes>().is_ok()
}

fn snapshot(
    value: &Bound<'_, PyAny>,
    path: &str,
    active: &mut HashSet<usize>,
) -> PyResult<Py<PyAny>> {
    let py = value.py();
    if atomic(value) {
        return Ok(value.clone().unbind());
    }
    if let Ok(mapping) = value.cast_exact::<PyDict>() {
        let identity = value.as_ptr() as usize;
        if !active.insert(identity) {
            return Err(PyValueError::new_err(format!(
                "{path}: cyclic property value"
            )));
        }
        let result = (|| {
            let owned = mapping.copy()?;
            let mut nested = Vec::new();
            for (key, child) in owned.iter() {
                if !key.is_instance_of::<PyString>() {
                    return Err(PyTypeError::new_err(format!(
                        "{path}: property keys must be strings"
                    )));
                }
                if !atomic(&child) {
                    nested.push((key.unbind(), child.unbind()));
                }
            }
            for (key, child) in nested {
                let key = key.bind(py);
                let child_path = format!("{path}.{}", key.cast::<PyString>()?.to_string_lossy());
                let child = snapshot(child.bind(py), &child_path, active)?;
                owned.set_item(key, child)?;
            }
            Ok(owned.into_any().unbind())
        })();
        active.remove(&identity);
        return result;
    }
    let elements = value
        .cast_exact::<PyList>()
        .map(|list| list.iter().map(Bound::unbind).collect::<Vec<_>>())
        .or_else(|_| {
            value
                .cast_exact::<PyTuple>()
                .map(|tuple| tuple.iter().map(Bound::unbind).collect::<Vec<_>>())
        })
        .ok();
    if let Some(elements) = elements {
        let identity = value.as_ptr() as usize;
        if !active.insert(identity) {
            return Err(PyValueError::new_err(format!(
                "{path}: cyclic property value"
            )));
        }
        let result = (|| {
            let mut output = Vec::with_capacity(elements.len());
            for (index, child) in elements.iter().enumerate() {
                let child = child.bind(py);
                if atomic(child) {
                    output.push(child.clone().unbind());
                } else {
                    output.push(snapshot(child, &format!("{path}[{index}]"), active)?);
                }
            }
            Ok(PyTuple::new(py, output)?.into_any().unbind())
        })();
        active.remove(&identity);
        return result;
    }

    if let Some(keys) = value.getattr_opt(pyo3::intern!(py, "keys"))?
        && keys.is_callable()
    {
        let identity = value.as_ptr() as usize;
        if !active.insert(identity) {
            return Err(PyValueError::new_err(format!(
                "{path}: cyclic property value"
            )));
        }
        let result = (|| {
            let owned = PyDict::new(py);
            let keys = keys.call0()?;
            if keys.is_instance_of::<PyString>()
                || keys.is_instance_of::<PyBytes>()
                || keys.is_instance_of::<PyByteArray>()
            {
                return Err(PyTypeError::new_err(
                    "mapping keys() must return an iterable of keys",
                ));
            }
            for (index, key) in keys.try_iter()?.enumerate() {
                crate::check_python_signals(py, index)?;
                let key = key?;
                if !key.is_instance_of::<PyString>() {
                    return Err(PyTypeError::new_err(format!(
                        "{path}: property keys must be strings"
                    )));
                }
                if owned.contains(&key)? {
                    return Err(GeometryError::new_err(format!(
                        "{path}: duplicate property key"
                    )));
                }
                let child = value.get_item(&key)?;
                if atomic(&child) {
                    owned.set_item(key, child)?;
                } else {
                    let child_path =
                        format!("{path}.{}", key.cast::<PyString>()?.to_string_lossy());
                    let child = snapshot(&child, &child_path, active)?;
                    owned.set_item(key, child)?;
                }
            }
            Ok(owned.into_any().unbind())
        })();
        active.remove(&identity);
        return result;
    }

    if value.cast::<PyList>().is_ok() || value.cast::<PyTuple>().is_ok() {
        let identity = value.as_ptr() as usize;
        if !active.insert(identity) {
            return Err(PyValueError::new_err(format!(
                "{path}: cyclic property value"
            )));
        }
        let result = (|| {
            let mut output = Vec::new();
            for (index, child) in value.try_iter()?.enumerate() {
                crate::check_python_signals(py, index)?;
                let child = child?;
                if atomic(&child) {
                    output.push(child.unbind());
                } else {
                    output.push(snapshot(&child, &format!("{path}[{index}]"), active)?);
                }
            }
            Ok(PyTuple::new(py, output)?.into_any().unbind())
        })();
        active.remove(&identity);
        return result;
    }

    let (normalized, complete): (Py<PyAny>, bool) = py
        .import("gometry._feature_values")?
        .getattr("normalize_scalar")?
        .call1((value, path))?
        .extract()?;
    if complete {
        Ok(normalized)
    } else {
        snapshot(normalized.bind(py), path, active)
    }
}

#[pyfunction(name = "_snapshot_feature_value")]
pub(crate) fn snapshot_feature_value(value: &Bound<'_, PyAny>, path: &str) -> PyResult<Py<PyAny>> {
    snapshot(value, path, &mut HashSet::new())
}

#[pyfunction(name = "_snapshot_feature_rows")]
pub(crate) fn snapshot_feature_rows(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    expected: usize,
) -> PyResult<Py<PyTuple>> {
    let mut index = 0_usize;
    let mut active = HashSet::new();
    let rows = crate::collect_py_iter_exact(
        values,
        expected,
        |value| {
            let row = if value.is_none() {
                py.None()
            } else {
                if !is_mapping_like(&value)? {
                    return Err(PyTypeError::new_err(
                        "properties rows must be mappings or None",
                    ));
                }
                snapshot(&value, &format!("properties[{index}]"), &mut active)?
            };
            index = index.saturating_add(1);
            Ok(row)
        },
        |got| {
            GeometryError::new_err(format!(
                "properties length {} does not match geometries length {expected}",
                if got > expected {
                    format!(">{expected}")
                } else {
                    got.to_string()
                }
            ))
        },
    )?;
    Ok(PyTuple::new(py, rows)?.unbind())
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(snapshot_feature_value, module)?)?;
    module.add_function(wrap_pyfunction!(snapshot_feature_rows, module)?)?;
    Ok(())
}
