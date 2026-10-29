//! Immutable chunked Arrow columns and composed row selections.

use std::sync::Arc;

use arrow_array::types::{ArrowPrimitiveType, Float16Type};
use arrow_data::ArrayData;
use arrow_data::transform::MutableArrayData;
use arrow_schema::DataType;
use pyo3::IntoPyObjectExt as _;
use pyo3::exceptions::{PyIndexError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use super::admitted::{AdmittedArrowSchema, AdmittedDictionary};
use super::attribute_values;
use super::values::{OwnedArrowArray, arrow_error, normalize_offset, snapshot};
use crate::geometry::storage::{RowSelection, RowSelectionRef, row_selection_from_logical_rows};
use crate::py::errors::GeometryError;
use crate::py::row::{RowIndexOrSlice, parse_row_index_or_slice, slice_row_indices};

#[pyclass(
    name = "_ArrowColumn",
    module = "gometry._lib",
    frozen,
    sequence,
    immutable_type
)]
#[derive(Clone)]
pub(crate) struct OwnedArrowColumn {
    schema: Arc<AdmittedArrowSchema>,
    chunks: Arc<[ArrayData]>,
    ends: Arc<[usize]>,
    selection: RowSelection,
}

impl OwnedArrowColumn {
    pub(super) fn from_chunks(
        schema: Arc<AdmittedArrowSchema>,
        chunks: Vec<ArrayData>,
    ) -> PyResult<Self> {
        let mut length = 0_usize;
        let mut ends = Vec::with_capacity(chunks.len());
        for chunk in &chunks {
            length = length
                .checked_add(chunk.len())
                .ok_or_else(|| PyValueError::new_err("Arrow column length overflow"))?;
            ends.push(length);
        }
        Ok(Self {
            schema,
            chunks: chunks.into(),
            ends: ends.into(),
            selection: RowSelection::Identity,
        })
    }

    fn physical_len(&self) -> usize {
        self.ends.last().copied().unwrap_or(0)
    }

    fn selected(&self, rows: impl IntoIterator<Item = usize>) -> Self {
        Self {
            schema: Arc::clone(&self.schema),
            chunks: Arc::clone(&self.chunks),
            ends: Arc::clone(&self.ends),
            selection: row_selection_from_logical_rows(
                self.selection.as_deref(),
                self.physical_len(),
                rows,
            ),
        }
    }

    pub(crate) fn select(&self, selection: &RowSelection) -> Self {
        let selection = if selection.is_identity() {
            self.selection.clone()
        } else if self.selection.is_identity() {
            selection.clone()
        } else {
            row_selection_from_logical_rows(
                self.selection.as_deref(),
                self.physical_len(),
                (0..selection.as_deref().len(self.__len__()))
                    .map(|index| selection.as_deref().physical(index)),
            )
        };
        Self {
            schema: Arc::clone(&self.schema),
            chunks: Arc::clone(&self.chunks),
            ends: Arc::clone(&self.ends),
            selection,
        }
    }

    #[expect(
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        reason = "selected physical rows belong to the admitted chunk ranges and run endpoints are ordered"
    )]
    pub(crate) fn materialize(&self) -> PyResult<Self> {
        let mut runs: Vec<(usize, usize, usize)> = Vec::new();
        for row in 0..self.__len__() {
            let physical = self.selection.as_deref().physical(row);
            let chunk = self.ends.partition_point(|end| *end <= physical);
            let start = if chunk == 0 {
                0
            } else {
                *self.ends.get(chunk - 1).expect("previous Arrow chunk end")
            };
            let index = physical - start;
            if let Some((previous_chunk, _, previous_end)) = runs.last_mut()
                && *previous_chunk == chunk
                && *previous_end == index
            {
                *previous_end = previous_end.saturating_add(1);
            } else {
                runs.push((chunk, index, index.saturating_add(1)));
            }
        }
        if runs.is_empty() {
            let empty = self
                .chunks
                .first()
                .expect("Arrow column has a schema")
                .slice(0, 0);
            let data = super::values::compact(&empty).map_err(|error| arrow_error(&error))?;
            return Self::from_chunks(Arc::clone(&self.schema), vec![data]);
        }
        let has_dictionary = schema_has_dictionary(&self.schema);
        let mut output = Vec::new();
        if has_dictionary {
            for group in runs.chunk_by(|left, right| left.0 == right.0) {
                let &(chunk, ..) = group.first().expect("a chunk group is not empty");
                let source =
                    normalize_offset(self.chunks.get(chunk).expect("selected Arrow chunk"))
                        .map_err(|error| arrow_error(&error))?;
                let len = group.iter().map(|(_, start, end)| end - start).sum();
                let mut data = MutableArrayData::try_new(vec![&source], true, len)
                    .map_err(|error| arrow_error(&error))?;
                for &(_, start, end) in group {
                    data.try_extend(0, start, end)
                        .map_err(|error| arrow_error(&error))?;
                }
                output.push(snapshot(&data.freeze()).map_err(|error| arrow_error(&error))?);
            }
        } else {
            let mut used = Vec::new();
            let mut lookup = vec![None; self.chunks.len()];
            for &(chunk, ..) in &runs {
                if lookup[chunk].is_none() {
                    lookup[chunk] = Some(used.len());
                    used.push(
                        normalize_offset(&self.chunks[chunk])
                            .map_err(|error| arrow_error(&error))?,
                    );
                }
            }
            let references: Vec<&ArrayData> = used.iter().collect();
            let mut data = MutableArrayData::try_new(references, true, self.__len__())
                .map_err(|error| arrow_error(&error))?;
            for (chunk, start, end) in runs {
                data.try_extend(
                    lookup[chunk].expect("selected Arrow chunk is admitted"),
                    start,
                    end,
                )
                .map_err(|error| arrow_error(&error))?;
            }
            output.push(snapshot(&data.freeze()).map_err(|error| arrow_error(&error))?);
        }
        Self::from_chunks(Arc::clone(&self.schema), output)
    }

    #[expect(
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        reason = "the admitted selection maps the checked logical index into a nonempty chunk range"
    )]
    fn cell(&self, py: Python<'_>, index: usize) -> PyResult<Py<PyAny>> {
        let physical = self.selection.as_deref().physical(index);
        let chunk_index = self.ends.partition_point(|end| *end <= physical);
        let start = if chunk_index == 0 {
            0
        } else {
            self.ends[chunk_index - 1]
        };
        let data = self
            .chunks
            .get(chunk_index)
            .expect("selected Arrow row has an admitted chunk");
        attribute_values::cell(py, data, &self.schema, physical - start, true)
    }
}

fn schema_has_dictionary(schema: &AdmittedArrowSchema) -> bool {
    !matches!(schema.dictionary, AdmittedDictionary::Absent)
        || schema.children.iter().any(schema_has_dictionary)
}

#[pymethods]
impl OwnedArrowColumn {
    fn __len__(&self) -> usize {
        self.selection.as_deref().len(self.physical_len())
    }

    fn __getitem__(&self, py: Python<'_>, index: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        match parse_row_index_or_slice(index, self.__len__(), "Arrow column")? {
            RowIndexOrSlice::Index(index) => self.cell(py, index),
            RowIndexOrSlice::Slice {
                start, step, count, ..
            } => Ok(Py::new(py, self.selected(slice_row_indices(start, step, count)))?.into_any()),
        }
    }

    #[pyo3(name = "_take")]
    fn take(&self, indices: &Bound<'_, PyTuple>) -> PyResult<Self> {
        let len = self.__len__();
        let rows = indices
            .iter()
            .map(|value| {
                let index = value.extract::<usize>()?;
                if index >= len {
                    return Err(PyIndexError::new_err("Arrow column index out of range"));
                }
                Ok(index)
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(self.selected(rows))
    }

    #[pyo3(name = "_materialize")]
    fn materialize_python(slf: &Bound<'_, Self>) -> PyResult<Py<Self>> {
        let owner = slf.clone().unbind();
        slf.py().detach(|| {
            let output = owner.get().materialize()?;
            Python::attach(|py| Py::new(py, output))
        })
    }

    fn __reduce__(slf: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let detached = Self::materialize_python(slf)?;
        let detached = detached.get();
        let schema = super::state::schema_to_python(py, &detached.schema)?;
        let chunks = detached
            .chunks
            .iter()
            .map(|data| super::state::data_to_python(py, data))
            .collect::<PyResult<Vec<_>>>()?;
        let restore = py
            .import("gometry._lib")?
            .getattr("_restore_arrow_column")?;
        (restore, (schema, PyTuple::new(py, chunks)?)).into_py_any(py)
    }

    #[pyo3(name = "_chunks")]
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "the selected intersection is ordered and lies within the current chunk"
    )]
    fn chunks(&self, py: Python<'_>) -> PyResult<Vec<Py<OwnedArrowArray>>> {
        let range = match self.selection.as_deref() {
            RowSelectionRef::Identity | RowSelectionRef::Gather(_) => None,
            RowSelectionRef::Window { start, len } => Some(start..start.saturating_add(len)),
        };
        let mut output = Vec::new();
        let mut start = 0_usize;
        for (data, end) in self.chunks.iter().zip(self.ends.iter().copied()) {
            let selected = if let Some(range) = &range {
                let low = range.start.max(start);
                let high = range.end.min(end);
                if low >= high {
                    start = end;
                    continue;
                }
                data.slice(low - start, high - low)
            } else {
                data.clone()
            };
            output.push(Py::new(py, OwnedArrowArray {
                schema: Arc::clone(&self.schema),
                data: selected,
            })?);
            start = end;
        }
        if output.is_empty()
            && let Some(first) = self.chunks.first()
        {
            output.push(Py::new(py, OwnedArrowArray {
                schema: Arc::clone(&self.schema),
                data: first.slice(0, 0),
            })?);
        }
        Ok(output)
    }

    #[pyo3(name = "_indices")]
    fn indices(&self) -> Option<Vec<usize>> {
        match self.selection.as_deref() {
            RowSelectionRef::Identity | RowSelectionRef::Window { .. } => None,
            RowSelectionRef::Gather(rows) => Some(rows.to_vec()),
        }
    }

    #[pyo3(name = "_validate_feature_type")]
    fn validate_feature_type(&self, path: &str) -> PyResult<()> {
        validate_feature_schema(&self.schema, path)
    }

    #[pyo3(name = "_validate_ids")]
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "the physical row is inside the selected chunk's ordered range"
    )]
    fn validate_ids(&self) -> PyResult<()> {
        validate_feature_schema(&self.schema, "ids")?;
        let data_type = self
            .chunks
            .first()
            .expect("Arrow column has an admitted schema")
            .data_type();
        if !id_type(data_type) {
            return Err(PyTypeError::new_err(
                "Arrow feature IDs must be strings, numbers, or null",
            ));
        }
        if id_float_type(data_type) {
            for row in 0..self.__len__() {
                let physical = self.selection.as_deref().physical(row);
                let chunk = self.ends.partition_point(|end| *end <= physical);
                let start = if chunk == 0 {
                    0
                } else {
                    *self.ends.get(chunk - 1).expect("previous Arrow chunk end")
                };
                let data = self.chunks.get(chunk).expect("selected Arrow chunk");
                if !finite_id(data, physical - start) {
                    return Err(GeometryError::new_err("numeric feature IDs must be finite"));
                }
            }
        }
        Ok(())
    }
}

fn validate_feature_schema(schema: &AdmittedArrowSchema, path: &str) -> PyResult<()> {
    if schema.extension_name()?.is_some() {
        return Err(PyTypeError::new_err(format!(
            "{path}: unsupported Arrow extension value"
        )));
    }
    if let AdmittedDictionary::Captured(dictionary) = &schema.dictionary {
        validate_feature_schema(dictionary, path)?;
    }
    for child in schema.children.iter() {
        validate_feature_schema(child, &format!("{path}.{}", child.name()))?;
    }
    Ok(())
}

#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "only the enumerated scalar Arrow families can represent feature IDs"
)]
fn id_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Null
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View => true,
        DataType::Dictionary(_, values) => id_type(values),
        _ => false,
    }
}

#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "non-floating Arrow types need no finite-value scan"
)]
fn id_float_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Float16 | DataType::Float32 | DataType::Float64 => true,
        DataType::Dictionary(_, values) => id_float_type(values),
        _ => false,
    }
}

#[expect(
    clippy::indexing_slicing,
    clippy::wildcard_enum_match_arm,
    reason = "bounds are checked before reading validated buffers; only floating and dictionary values need further inspection"
)]
fn finite_id(data: &ArrayData, index: usize) -> bool {
    if index >= data.len() {
        return false;
    }
    if data.is_null(index) {
        return true;
    }
    match data.data_type() {
        DataType::Float16 => data.buffer::<<Float16Type as ArrowPrimitiveType>::Native>(0)[index]
            .to_f32()
            .is_finite(),
        DataType::Float32 => data.buffer::<f32>(0)[index].is_finite(),
        DataType::Float64 => data.buffer::<f64>(0)[index].is_finite(),
        DataType::Dictionary(key, _) => {
            let index = match key.as_ref() {
                DataType::Int8 => usize::try_from(data.buffer::<i8>(0)[index]).ok(),
                DataType::Int16 => usize::try_from(data.buffer::<i16>(0)[index]).ok(),
                DataType::Int32 => usize::try_from(data.buffer::<i32>(0)[index]).ok(),
                DataType::Int64 => usize::try_from(data.buffer::<i64>(0)[index]).ok(),
                DataType::UInt8 => Some(usize::from(data.buffer::<u8>(0)[index])),
                DataType::UInt16 => Some(usize::from(data.buffer::<u16>(0)[index])),
                DataType::UInt32 => usize::try_from(data.buffer::<u32>(0)[index]).ok(),
                DataType::UInt64 => usize::try_from(data.buffer::<u64>(0)[index]).ok(),
                _ => None,
            };
            index.is_some_and(|index| {
                data.child_data()
                    .first()
                    .is_some_and(|values| finite_id(values, index))
            })
        },
        _ => true,
    }
}

#[pyfunction(name = "_arrow_column")]
fn arrow_column(py: Python<'_>, chunks: &Bound<'_, PyTuple>) -> PyResult<Py<OwnedArrowColumn>> {
    let mut arrays = Vec::with_capacity(chunks.len());
    let mut schema = None;
    let mut data_type = None;
    for chunk in chunks.iter() {
        let chunk = chunk.cast::<OwnedArrowArray>()?;
        let chunk = chunk.get();
        if let Some(expected) = &data_type {
            if expected != chunk.data.data_type() {
                return Err(PyValueError::new_err("Arrow chunk types do not match"));
            }
        } else {
            data_type = Some(chunk.data.data_type().clone());
            schema = Some(Arc::clone(&chunk.schema));
        }
        arrays.push(chunk.data.clone());
    }
    let schema = schema.ok_or_else(|| PyValueError::new_err("Arrow column requires a schema"))?;
    Py::new(py, OwnedArrowColumn::from_chunks(schema, arrays)?)
}

pub(super) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<OwnedArrowColumn>()?;
    module.add_function(wrap_pyfunction!(arrow_column, module)?)?;
    Ok(())
}
