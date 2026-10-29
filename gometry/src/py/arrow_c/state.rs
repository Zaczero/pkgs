//! Pickle state for provider-independent Arrow schema and owned buffers.

use std::sync::Arc;

use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow_data::ArrayData;
use arrow_schema::DataType;
use pyo3::IntoPyObjectExt as _;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};

use super::admitted::{AdmittedArrowSchema, AdmittedDictionary};
use super::column::OwnedArrowColumn;
use super::values::{ArrowValue, arrow_error, export_schema};

pub(super) fn schema_to_python(
    py: Python<'_>,
    schema: &AdmittedArrowSchema,
) -> PyResult<Py<PyAny>> {
    let metadata = schema
        .metadata
        .iter()
        .map(|(key, value)| {
            PyTuple::new(py, [PyBytes::new(py, key), PyBytes::new(py, value)]).map(Bound::unbind)
        })
        .collect::<PyResult<Vec<_>>>()?;
    let children = schema
        .children
        .iter()
        .map(|child| schema_to_python(py, child))
        .collect::<PyResult<Vec<_>>>()?;
    let dictionary = match &schema.dictionary {
        AdmittedDictionary::Absent => py.None(),
        AdmittedDictionary::Captured(child) => schema_to_python(py, child)?,
        AdmittedDictionary::Unselected => {
            return Err(PyValueError::new_err("incomplete Arrow dictionary schema"));
        },
    };
    Ok(PyTuple::new(py, [
        schema.format.as_str().into_py_any(py)?,
        schema.name.as_str().into_py_any(py)?,
        schema.flags.into_py_any(py)?,
        PyTuple::new(py, metadata)?.into_any().unbind(),
        PyTuple::new(py, children)?.into_any().unbind(),
        dictionary,
    ])?
    .into_any()
    .unbind())
}

pub(super) fn schema_from_python(state: &Bound<'_, PyAny>) -> PyResult<AdmittedArrowSchema> {
    let state = state.cast::<PyTuple>()?;
    if state.len() != 6 {
        return Err(PyValueError::new_err("invalid Arrow schema pickle"));
    }
    let metadata = state
        .get_item(3)?
        .cast::<PyTuple>()?
        .iter()
        .map(|pair| {
            let pair = pair.cast::<PyTuple>()?;
            if pair.len() != 2 {
                return Err(PyValueError::new_err("invalid Arrow metadata pickle"));
            }
            Ok((
                Box::<[u8]>::from(pair.get_item(0)?.cast::<PyBytes>()?.as_bytes()),
                Box::<[u8]>::from(pair.get_item(1)?.cast::<PyBytes>()?.as_bytes()),
            ))
        })
        .collect::<PyResult<Vec<_>>>()?;
    let children = state
        .get_item(4)?
        .cast::<PyTuple>()?
        .iter()
        .map(|child| schema_from_python(&child))
        .collect::<PyResult<Vec<_>>>()?;
    let dictionary = state.get_item(5)?;
    let dictionary = if dictionary.is_none() {
        AdmittedDictionary::Absent
    } else {
        AdmittedDictionary::Captured(Box::new(schema_from_python(&dictionary)?))
    };
    Ok(AdmittedArrowSchema {
        format: Arc::new(state.get_item(0)?.extract()?),
        name: Arc::new(state.get_item(1)?.extract()?),
        metadata: Arc::new(metadata),
        flags: state.get_item(2)?.extract()?,
        dictionary,
        children: Arc::new(children),
    })
}

pub(super) fn data_to_python(py: Python<'_>, data: &ArrayData) -> PyResult<Py<PyAny>> {
    let nulls = match data.nulls() {
        Some(nulls) => {
            let bits = nulls.inner();
            (PyBytes::new(py, bits.values()), bits.offset(), bits.len()).into_py_any(py)?
        },
        None => py.None(),
    };
    let buffers = data
        .buffers()
        .iter()
        .map(|buffer| PyBytes::new(py, buffer.as_slice()).unbind())
        .collect::<Vec<_>>();
    let children = data
        .child_data()
        .iter()
        .map(|child| data_to_python(py, child))
        .collect::<PyResult<Vec<_>>>()?;
    (
        data.len(),
        data.offset(),
        nulls,
        PyTuple::new(py, buffers)?,
        PyTuple::new(py, children)?,
    )
        .into_py_any(py)
}

fn data_from_python(state: &Bound<'_, PyAny>, schema: &AdmittedArrowSchema) -> PyResult<ArrayData> {
    let state = state.cast::<PyTuple>()?;
    if state.len() != 5 {
        return Err(PyValueError::new_err("invalid Arrow array pickle"));
    }
    let nulls = state.get_item(2)?;
    let nulls = if nulls.is_none() {
        None
    } else {
        let nulls = nulls.cast::<PyTuple>()?;
        if nulls.len() != 3 {
            return Err(PyValueError::new_err("invalid Arrow null bitmap pickle"));
        }
        let bytes = nulls.get_item(0)?;
        let bytes = bytes.cast::<PyBytes>()?;
        let offset: usize = nulls.get_item(1)?.extract()?;
        let len: usize = nulls.get_item(2)?.extract()?;
        let end = offset
            .checked_add(len)
            .ok_or_else(|| PyValueError::new_err("Arrow null bitmap length overflow"))?;
        if end > bytes.as_bytes().len().saturating_mul(8) {
            return Err(PyValueError::new_err("Arrow null bitmap is truncated"));
        }
        Some(NullBuffer::new(BooleanBuffer::new(
            Buffer::from_slice_ref(bytes.as_bytes()),
            offset,
            len,
        )))
    };
    let buffers = state
        .get_item(3)?
        .cast::<PyTuple>()?
        .iter()
        .map(|buffer| {
            let buffer = buffer.cast::<PyBytes>()?;
            Ok(Buffer::from_slice_ref(buffer.as_bytes()))
        })
        .collect::<PyResult<Vec<_>>>()?;
    let child_states = state.get_item(4)?;
    let child_states = child_states.cast::<PyTuple>()?;
    let schemas: Vec<&AdmittedArrowSchema> = match &schema.dictionary {
        AdmittedDictionary::Absent => schema.children.iter().collect(),
        AdmittedDictionary::Captured(child) => vec![child],
        AdmittedDictionary::Unselected => {
            return Err(PyValueError::new_err("incomplete Arrow dictionary schema"));
        },
    };
    if child_states.len() != schemas.len() {
        return Err(PyValueError::new_err(
            "Arrow child count does not match schema",
        ));
    }
    let children = child_states
        .iter()
        .zip(schemas)
        .map(|(child, schema)| data_from_python(&child, schema))
        .collect::<PyResult<Vec<_>>>()?;
    let data_type =
        DataType::try_from(&export_schema(schema, false)?).map_err(|error| arrow_error(&error))?;
    ArrayData::builder(data_type)
        .len(state.get_item(0)?.extract()?)
        .offset(state.get_item(1)?.extract()?)
        .nulls(nulls)
        .buffers(buffers)
        .child_data(children)
        .build()
        .map_err(|error| arrow_error(&error))
}

#[pyfunction(name = "_restore_arrow_value")]
fn restore_arrow_value(
    py: Python<'_>,
    schema: &Bound<'_, PyAny>,
    data: &Bound<'_, PyAny>,
) -> PyResult<Py<ArrowValue>> {
    let schema = Arc::new(schema_from_python(schema)?);
    let data = data_from_python(data, &schema)?;
    if data.len() != 1 || data.is_null(0) {
        return Err(PyValueError::new_err(
            "Arrow value pickle must contain one value",
        ));
    }
    Py::new(py, ArrowValue { schema, data })
}

#[pyfunction(name = "_restore_arrow_column")]
fn restore_arrow_column(
    py: Python<'_>,
    schema: &Bound<'_, PyAny>,
    chunks: &Bound<'_, PyTuple>,
) -> PyResult<Py<OwnedArrowColumn>> {
    let schema = Arc::new(schema_from_python(schema)?);
    let chunks = chunks
        .iter()
        .map(|data| data_from_python(&data, &schema))
        .collect::<PyResult<Vec<_>>>()?;
    if chunks.is_empty() {
        return Err(PyValueError::new_err(
            "Arrow column pickle must contain a chunk",
        ));
    }
    Py::new(py, OwnedArrowColumn::from_chunks(schema, chunks)?)
}

pub(super) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(restore_arrow_value, module)?)?;
    module.add_function(wrap_pyfunction!(restore_arrow_column, module)?)?;
    Ok(())
}
