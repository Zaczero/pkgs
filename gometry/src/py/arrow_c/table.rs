//! Snapshot Arrow C arrays and streams into shared native columns.

use std::sync::{Arc, Mutex};

use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_data::ArrayData;
use arrow_data::transform::MutableArrayData;
use arrow_schema::DataType;
use pyo3::exceptions::{PyIndexError, PyRuntimeError, PyTypeError};
use pyo3::gc::{PyTraverseError, PyVisit};
use pyo3::prelude::*;
use pyo3::sync::MutexExt as _;
use pyo3::types::PyTuple;

use super::admitted::{AdmittedArrowSchema, ArrowReleaseGuard, MovedArrowShell};
use super::column::OwnedArrowColumn;
use super::import::{empty_schema, stream_callback_error};
use super::schema::OwnedArrowSchema;
use super::values::{arrow_error, export_schema, normalize_offset, snapshot};
use super::{
    ArrowArray, ArrowArrayStream, ArrowSchema, array_capsule_name, capsule_pointer, empty_array,
    schema_capsule_name, stream_capsule_name,
};
use crate::py::arrow::arrow_import_error;

const _: () = {
    assert!(size_of::<ArrowArrayStream>() == size_of::<FFI_ArrowArrayStream>());
    assert!(align_of::<ArrowArrayStream>() == align_of::<FFI_ArrowArrayStream>());
};

struct ArrowStream {
    stream: FFI_ArrowArrayStream,
    schema: Arc<AdmittedArrowSchema>,
    data_type: DataType,
}

impl ArrowStream {
    fn open(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        let capsule = value.call_method1("__arrow_c_stream__", (py.None(),))?;
        let source = capsule_pointer::<ArrowArrayStream>(&capsule, stream_capsule_name())?;
        // SAFETY: the named capsule pins a live, writable Arrow stream shell.
        if unsafe { (*source).release.is_none() } {
            return Err(PyTypeError::new_err("Arrow stream is already released"));
        }
        // SAFETY: Arrow's FFI type has the same C layout and takes the live
        // shell, clearing the capsule's release slot. Its Send implementation
        // permits ownership transfer; callbacks remain serialized by PyO3.
        let mut stream = unsafe { FFI_ArrowArrayStream::from_raw(source.cast()) };
        let raw = std::ptr::from_mut(&mut stream).cast::<ArrowArrayStream>();
        // SAFETY: `stream` remains live while its schema callback executes.
        unsafe {
            let get_schema = (*raw)
                .get_schema
                .ok_or_else(|| PyTypeError::new_err("Arrow stream has no schema callback"))?;
            if (*raw).get_next.is_none() {
                return Err(PyTypeError::new_err("Arrow stream has no next callback"));
            }
            let mut output = empty_schema();
            let status = get_schema(raw, &raw mut output);
            let guard = ArrowReleaseGuard::from_raw(&raw mut output)?;
            if status != 0 {
                return Err(stream_callback_error(
                    py,
                    raw,
                    status,
                    "Arrow stream failed to export its schema",
                ));
            }
            let shell = MovedArrowShell::take(guard.as_ptr())?;
            let (schema, data_type) = capture_schema(&shell)?;
            drop(shell);
            drop(guard);
            Ok(Self {
                stream,
                schema,
                data_type,
            })
        }
    }

    fn next(&mut self, py: Python<'_>) -> PyResult<Option<ArrayData>> {
        let raw = std::ptr::from_mut(&mut self.stream).cast::<ArrowArrayStream>();
        // SAFETY: the stream is live and this exclusive borrow serializes calls.
        unsafe {
            let get_next = (*raw)
                .get_next
                .expect("stream callback admitted at construction");
            let mut output = empty_array();
            let status = get_next(raw, &raw mut output);
            let guard = ArrowReleaseGuard::from_raw(&raw mut output)?;
            if status != 0 {
                return Err(stream_callback_error(
                    py,
                    raw,
                    status,
                    "Arrow stream failed to export its next batch",
                ));
            }
            if output.release.is_none() {
                return Ok(None);
            }
            capture_array(guard.as_ptr(), &self.data_type).map(Some)
        }
    }
}

#[pyclass(name = "_ArrowStreamReader", module = "gometry._lib", immutable_type)]
struct OwnedArrowStreamReader {
    stream: Mutex<StreamState>,
    source: Option<Py<PyAny>>,
}

enum StreamState {
    Ready(ArrowStream),
    Busy,
    Closed,
}

#[pymethods]
impl OwnedArrowStreamReader {
    const fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let mut guard = self
            .stream
            .lock_py_attached(py)
            .map_err(|_| PyRuntimeError::new_err("Arrow stream lock is poisoned"))?;
        let mut stream = match std::mem::replace(&mut *guard, StreamState::Busy) {
            StreamState::Ready(stream) => stream,
            StreamState::Busy => {
                *guard = StreamState::Busy;
                return Err(PyRuntimeError::new_err(
                    "Arrow stream is already being consumed",
                ));
            },
            StreamState::Closed => {
                *guard = StreamState::Closed;
                return Ok(None);
            },
        };
        drop(guard);
        let result = stream.next(py).and_then(|data| match data {
            Some(data) => finish(py, Arc::clone(&stream.schema), &stream.data_type, vec![
                data,
            ])
            .map(Some),
            None => Ok(None),
        });
        let mut guard = self
            .stream
            .lock_py_attached(py)
            .map_err(|_| PyRuntimeError::new_err("Arrow stream lock is poisoned"))?;
        match result {
            Ok(Some(result)) => {
                *guard = StreamState::Ready(stream);
                Ok(Some(result))
            },
            other => {
                *guard = StreamState::Closed;
                drop(guard);
                drop(stream);
                other
            },
        }
    }

    fn close(slf: &Bound<'_, Self>) -> PyResult<()> {
        let (source, stream) = {
            let mut borrowed = slf.try_borrow_mut()?;
            let stream = borrowed
                .stream
                .get_mut()
                .map_err(|_| PyRuntimeError::new_err("Arrow stream lock is poisoned"))?;
            let stream = std::mem::replace(stream, StreamState::Closed);
            (borrowed.source.take(), stream)
        };
        drop(stream);
        drop(source);
        Ok(())
    }

    #[expect(
        clippy::same_name_method,
        reason = "PyO3 requires this name for cyclic-GC traversal"
    )]
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        visit.call(&self.source)
    }

    fn __clear__(slf: &Bound<'_, Self>) {
        let (source, stream) = {
            let mut borrowed = slf.borrow_mut();
            let stream = borrowed
                .stream
                .get_mut()
                .unwrap_or_else(|error| error.into_inner());
            let stream = std::mem::replace(stream, StreamState::Closed);
            (borrowed.source.take(), stream)
        };
        drop(stream);
        drop(source);
    }
}

#[pyclass(name = "_ArrowTable", module = "gometry._lib", frozen, immutable_type)]
struct OwnedArrowTable {
    schema: Arc<AdmittedArrowSchema>,
    columns: Vec<OwnedArrowColumn>,
    len: usize,
}

#[pymethods]
impl OwnedArrowTable {
    const fn __len__(&self) -> usize {
        self.len
    }

    #[getter]
    fn schema(&self, py: Python<'_>) -> PyResult<Py<OwnedArrowSchema>> {
        Py::new(py, OwnedArrowSchema {
            schema: Arc::clone(&self.schema),
        })
    }

    fn column(&self, py: Python<'_>, index: usize) -> PyResult<Py<OwnedArrowColumn>> {
        let column = self
            .columns
            .get(index)
            .ok_or_else(|| PyIndexError::new_err("Arrow column index out of range"))?;
        Py::new(py, column.clone())
    }
}

fn capture_schema(
    shell: &MovedArrowShell<ArrowSchema>,
) -> PyResult<(Arc<AdmittedArrowSchema>, DataType)> {
    // SAFETY: the moved shell keeps the complete producer schema live.
    let schema = unsafe { AdmittedArrowSchema::capture_values(shell) }?;
    let data_type =
        DataType::try_from(&export_schema(&schema, false)?).map_err(|error| arrow_error(&error))?;
    if matches!(&data_type, DataType::Struct(fields) if fields.is_empty())
        && schema.extension_name()?.is_none()
    {
        return Err(PyTypeError::new_err(
            "Arrow table has no geometry-like column",
        ));
    }
    Ok((Arc::new(schema), data_type))
}

/// # Safety
///
/// `source` is a live Arrow array whose data agrees with `data_type`. Its
/// producer keeps all buffers quiescent until this function returns.
unsafe fn capture_array(source: *mut ArrowArray, data_type: &DataType) -> PyResult<ArrayData> {
    // SAFETY: the caller supplies a live writable output shell.
    let _guard = unsafe { ArrowReleaseGuard::from_raw(source) }?;
    // SAFETY: the guard owns the live producer shell until the move.
    let shell = unsafe { MovedArrowShell::take(source) }?;
    // SAFETY: the shell retains the producer's live, quiescent array tree.
    let data = unsafe { super::snapshot_admission::snapshot_array(shell.as_ptr(), data_type) }
        .map_err(arrow_import_error)?;
    drop(shell);
    Ok(data)
}

fn table_child(root: &ArrayData, child_index: usize) -> PyResult<ArrayData> {
    let row_view = root.slice(0, root.len());
    let child = row_view
        .child_data()
        .get(child_index)
        .ok_or_else(|| PyIndexError::new_err("Arrow table child is missing"))?;
    let Some(nulls) = row_view.nulls().filter(|nulls| nulls.null_count() != 0) else {
        return Ok(child.clone());
    };
    let child = normalize_offset(child).map_err(|error| arrow_error(&error))?;
    let mut output = MutableArrayData::try_new(vec![&child], true, root.len())
        .map_err(|error| arrow_error(&error))?;
    let mut start = 0_usize;
    while start < root.len() {
        let valid = nulls.is_valid(start);
        let mut end = start.saturating_add(1);
        while end < root.len() && nulls.is_valid(end) == valid {
            end = end.saturating_add(1);
        }
        if valid {
            output
                .try_extend(0, start, end)
                .map_err(|error| arrow_error(&error))?;
        } else {
            output
                .try_extend_nulls(end.wrapping_sub(start))
                .map_err(|error| arrow_error(&error))?;
        }
        start = end;
    }
    snapshot(&output.freeze()).map_err(|error| arrow_error(&error))
}

fn finish(
    py: Python<'_>,
    schema: Arc<AdmittedArrowSchema>,
    data_type: &DataType,
    batches: Vec<ArrayData>,
) -> PyResult<Py<PyAny>> {
    let is_table = schema.format() == "+s" && schema.extension_name()?.is_none();
    if !is_table {
        let chunks = if batches.is_empty() {
            vec![ArrayData::new_empty(data_type)]
        } else {
            batches
        };
        return Ok(Py::new(py, OwnedArrowColumn::from_chunks(schema, chunks)?)?.into_any());
    }
    let DataType::Struct(fields) = data_type else {
        return Err(PyTypeError::new_err("Arrow table schema must be a struct"));
    };
    if fields.len() != schema.children.len() {
        return Err(PyTypeError::new_err(
            "Arrow table fields do not match schema",
        ));
    }
    let mut len = 0_usize;
    for batch in &batches {
        len = len
            .checked_add(batch.len())
            .ok_or_else(|| PyTypeError::new_err("Arrow table length overflow"))?;
    }
    let mut columns = Vec::with_capacity(fields.len());
    for (index, field) in fields.iter().enumerate() {
        let child_schema = schema.children.get(index).expect("admitted table field");
        let chunks = if batches.is_empty() {
            vec![ArrayData::new_empty(field.data_type())]
        } else {
            batches
                .iter()
                .map(|batch| table_child(batch, index))
                .collect::<PyResult<Vec<_>>>()?
        };
        columns.push(OwnedArrowColumn::from_chunks(
            Arc::new(child_schema.clone()),
            chunks,
        )?);
    }
    Ok(Py::new(py, OwnedArrowTable {
        schema,
        columns,
        len,
    })?
    .into_any())
}

#[pyfunction(name = "_snapshot_arrow_c")]
fn snapshot_arrow_c(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    if value.hasattr("__arrow_c_array__")? {
        let capsules = value.call_method1("__arrow_c_array__", (py.None(),))?;
        let capsules = capsules.cast::<PyTuple>()?;
        if capsules.len() != 2 {
            return Err(PyTypeError::new_err(
                "__arrow_c_array__ must return (schema_capsule, array_capsule)",
            ));
        }
        let schema_capsule = capsules.get_item(0)?;
        let array_capsule = capsules.get_item(1)?;
        let schema_ptr = capsule_pointer::<ArrowSchema>(&schema_capsule, schema_capsule_name())?;
        let array_ptr = capsule_pointer::<ArrowArray>(&array_capsule, array_capsule_name())?;
        // SAFETY: named capsules carry live one-shot producer shells.
        let schema_shell = unsafe { MovedArrowShell::take(schema_ptr) }?;
        let (schema, data_type) = capture_schema(&schema_shell)?;
        drop(schema_shell);
        // SAFETY: named array capsule carries the matching live Arrow data.
        let data = unsafe { capture_array(array_ptr, &data_type) }?;
        return finish(py, schema, &data_type, vec![data]);
    }
    if !value.hasattr("__arrow_c_stream__")? {
        return Ok(py.None());
    }
    let mut stream = ArrowStream::open(py, value)?;
    let mut batches = Vec::new();
    loop {
        crate::check_python_signals(py, batches.len())?;
        let Some(data) = stream.next(py)? else {
            break;
        };
        batches.push(data);
    }
    finish(py, Arc::clone(&stream.schema), &stream.data_type, batches)
}

#[pyfunction(name = "_arrow_stream_reader")]
fn arrow_stream_reader(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<Py<OwnedArrowStreamReader>> {
    Py::new(py, OwnedArrowStreamReader {
        stream: Mutex::new(StreamState::Ready(ArrowStream::open(py, value)?)),
        source: Some(value.clone().unbind()),
    })
}

pub(super) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<OwnedArrowTable>()?;
    module.add_class::<OwnedArrowStreamReader>()?;
    module.add_function(wrap_pyfunction!(snapshot_arrow_c, module)?)?;
    module.add_function(wrap_pyfunction!(arrow_stream_reader, module)?)?;
    Ok(())
}
