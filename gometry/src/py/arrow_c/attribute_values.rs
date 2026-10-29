//! Conversion of admitted Arrow cells to immutable Python values.

use std::sync::Arc;

use arrow_array::{BinaryViewArray, Float16Array, StringViewArray};
use arrow_data::ArrayData;
use arrow_schema::{DataType, TimeUnit, UnionMode};
use pyo3::IntoPyObjectExt as _;
use pyo3::exceptions::{PyIndexError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyTuple};

use super::admitted::{AdmittedArrowSchema, AdmittedDictionary};
use super::values::ArrowValue;

#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "non-temporal Arrow types have no temporal parts"
)]
pub(super) fn temporal_parts(
    data: &ArrayData,
    index: usize,
) -> Option<(&'static str, i64, Option<&str>, &'static str)> {
    let unit = |unit: &TimeUnit| match unit {
        TimeUnit::Second => "s",
        TimeUnit::Millisecond => "ms",
        TimeUnit::Microsecond => "us",
        TimeUnit::Nanosecond => "ns",
    };
    match data.data_type() {
        DataType::Timestamp(resolution, zone) => Some((
            "timestamp",
            *data.buffer::<i64>(0).get(index)?,
            zone.as_deref(),
            unit(resolution),
        )),
        DataType::Duration(resolution) => Some((
            "duration",
            *data.buffer::<i64>(0).get(index)?,
            None,
            unit(resolution),
        )),
        DataType::Time32(resolution) => Some((
            "time",
            i64::from(*data.buffer::<i32>(0).get(index)?),
            None,
            unit(resolution),
        )),
        DataType::Time64(resolution) => Some((
            "time",
            *data.buffer::<i64>(0).get(index)?,
            None,
            unit(resolution),
        )),
        DataType::Date32 => Some((
            "date32",
            i64::from(*data.buffer::<i32>(0).get(index)?),
            None,
            "s",
        )),
        DataType::Date64 => Some(("date64", *data.buffer::<i64>(0).get(index)?, None, "ms")),
        _ => None,
    }
}

pub(super) fn temporal_as_py(
    py: Python<'_>,
    kind: &str,
    value: i64,
    timezone: Option<&str>,
    unit: &str,
) -> PyResult<Py<PyAny>> {
    Ok(py
        .import("gometry._feature_values")?
        .getattr("temporal_as_py")?
        .call1((kind, value, timezone, unit))?
        .unbind())
}

macro_rules! primitive {
    ($data:expr, $index:expr, $ty:ty, $py:expr) => {
        $data
            .buffer::<$ty>(0)
            .get($index)
            .copied()
            .expect("validated Arrow buffer contains each selected value")
            .into_py_any($py)
    };
}

fn offset<T: arrow_buffer::ArrowNativeType + TryInto<usize>>(
    data: &ArrayData,
    buffer: usize,
    index: usize,
) -> PyResult<usize> {
    data.buffer::<T>(buffer)
        .get(index)
        .copied()
        .and_then(|value| value.try_into().ok())
        .ok_or_else(|| PyIndexError::new_err("invalid Arrow offset"))
}

fn binary_slice<T: arrow_buffer::ArrowNativeType + TryInto<usize>>(
    data: &ArrayData,
    index: usize,
) -> PyResult<&[u8]> {
    let start = offset::<T>(data, 0, index)?;
    let end = offset::<T>(data, 0, index.saturating_add(1))?;
    data.buffers()
        .get(1)
        .and_then(|buffer| buffer.as_slice().get(start..end))
        .ok_or_else(|| PyIndexError::new_err("invalid Arrow value span"))
}

fn list_values(
    py: Python<'_>,
    data: &ArrayData,
    schema: &AdmittedArrowSchema,
    start: usize,
    end: usize,
    allow_view: bool,
) -> PyResult<Py<PyAny>> {
    let child = data.child_data().first().expect("validated list values");
    let child_schema = schema.children.first().expect("admitted list value schema");
    let values = (start..end)
        .map(|index| cell(py, child, child_schema, index, allow_view))
        .collect::<PyResult<Vec<_>>>()?;
    Ok(PyTuple::new(py, values)?.into_any().unbind())
}

fn map_values(
    py: Python<'_>,
    data: &ArrayData,
    schema: &AdmittedArrowSchema,
    index: usize,
    allow_view: bool,
) -> PyResult<Py<PyAny>> {
    let start = offset::<i32>(data, 0, index)?;
    let end = offset::<i32>(data, 0, index.saturating_add(1))?;
    let entries = data.child_data().first().expect("validated map entries");
    let entry_schema = schema.children.first().expect("admitted map entry schema");
    let key_schema = entry_schema
        .children
        .first()
        .expect("admitted map key schema");
    let value_schema = entry_schema
        .children
        .get(1)
        .expect("admitted map value schema");
    let mut pairs = Vec::with_capacity(end.saturating_sub(start));
    for position in start..end {
        let entry = entries.slice(position, 1);
        let [key_data, value_data] = entry.child_data() else {
            unreachable!("validated Arrow map entries contain key and value children");
        };
        let key = cell(py, key_data, key_schema, 0, allow_view)?;
        let value = cell(py, value_data, value_schema, 0, allow_view)?;
        pairs.push(PyTuple::new(py, [key, value])?.unbind());
    }
    Ok(PyTuple::new(py, pairs)?.into_any().unbind())
}

#[expect(
    clippy::indexing_slicing,
    clippy::wildcard_enum_match_arm,
    reason = "admitted run-end buffers contain their declared rows; all other Arrow types are rejected as run ends"
)]
fn run_index(data: &ArrayData, index: usize) -> PyResult<usize> {
    let runs = data.child_data().first().expect("validated run ends");
    let position = i64::try_from(data.offset().saturating_add(index))
        .map_err(|_| PyIndexError::new_err("Arrow run position is too large"))?;
    let result =
        match runs.data_type() {
            DataType::Int16 => runs.buffer::<i16>(0)[..runs.len()]
                .partition_point(|run| i64::from(*run) <= position),
            DataType::Int32 => runs.buffer::<i32>(0)[..runs.len()]
                .partition_point(|run| i64::from(*run) <= position),
            DataType::Int64 => {
                runs.buffer::<i64>(0)[..runs.len()].partition_point(|run| *run <= position)
            },
            _ => return Err(PyTypeError::new_err("unsupported Arrow run-end type")),
        };
    Ok(result)
}

#[expect(
    clippy::indexing_slicing,
    clippy::wildcard_enum_match_arm,
    reason = "row bounds are checked before indexing validated Arrow buffers; types without a Python equivalent use the exact Arrow view"
)]
pub(super) fn cell(
    py: Python<'_>,
    data: &ArrayData,
    schema: &AdmittedArrowSchema,
    index: usize,
    allow_view: bool,
) -> PyResult<Py<PyAny>> {
    if index >= data.len() {
        return Err(PyIndexError::new_err("Arrow value index out of range"));
    }
    if data.is_null(index) || matches!(data.data_type(), DataType::Null) {
        return Ok(py.None());
    }
    match data.data_type() {
        DataType::Boolean => {
            let bit = data.offset().saturating_add(index);
            let byte = *data.buffers()[0]
                .as_slice()
                .get(bit / 8)
                .expect("validated Arrow boolean buffer contains each selected value");
            (byte & (1 << (bit % 8)) != 0).into_py_any(py)
        },
        DataType::Int8 => primitive!(data, index, i8, py),
        DataType::Int16 => primitive!(data, index, i16, py),
        DataType::Int32 => primitive!(data, index, i32, py),
        DataType::Int64 => primitive!(data, index, i64, py),
        DataType::UInt8 => primitive!(data, index, u8, py),
        DataType::UInt16 => primitive!(data, index, u16, py),
        DataType::UInt32 => primitive!(data, index, u32, py),
        DataType::UInt64 => primitive!(data, index, u64, py),
        DataType::Float16 => Float16Array::from(data.clone())
            .value(index)
            .to_f32()
            .into_py_any(py),
        DataType::Float32 => primitive!(data, index, f32, py),
        DataType::Float64 => primitive!(data, index, f64, py),
        DataType::Utf8 | DataType::LargeUtf8 => {
            let bytes = if matches!(data.data_type(), DataType::Utf8) {
                binary_slice::<i32>(data, index)?
            } else {
                binary_slice::<i64>(data, index)?
            };
            std::str::from_utf8(bytes)
                .map_err(|error| PyTypeError::new_err(error.to_string()))?
                .into_py_any(py)
        },
        DataType::Binary | DataType::LargeBinary => {
            let bytes = if matches!(data.data_type(), DataType::Binary) {
                binary_slice::<i32>(data, index)?
            } else {
                binary_slice::<i64>(data, index)?
            };
            Ok(PyBytes::new(py, bytes).into_any().unbind())
        },
        DataType::Utf8View => StringViewArray::from(data.clone())
            .value(index)
            .into_py_any(py),
        DataType::BinaryView => {
            let array = BinaryViewArray::from(data.clone());
            Ok(PyBytes::new(py, array.value(index)).into_any().unbind())
        },
        DataType::FixedSizeBinary(width) => {
            let width = usize::try_from(*width)
                .map_err(|_| PyTypeError::new_err("invalid fixed-size binary width"))?;
            let start = data.offset().saturating_add(index).saturating_mul(width);
            let end = start.saturating_add(width);
            let bytes = data.buffers()[0]
                .as_slice()
                .get(start..end)
                .ok_or_else(|| PyIndexError::new_err("invalid fixed-size binary span"))?;
            Ok(PyBytes::new(py, bytes).into_any().unbind())
        },
        DataType::Decimal32(_, scale) => {
            decimal(py, &data.buffer::<i32>(0)[index].to_string(), *scale)
        },
        DataType::Decimal64(_, scale) => {
            decimal(py, &data.buffer::<i64>(0)[index].to_string(), *scale)
        },
        DataType::Decimal128(_, scale) => {
            decimal(py, &data.buffer::<i128>(0)[index].to_string(), *scale)
        },
        DataType::Decimal256(_, scale) => decimal(
            py,
            &data.buffer::<arrow_buffer::i256>(0)[index].to_string(),
            *scale,
        ),
        DataType::List(_) => list_values(
            py,
            data,
            schema,
            offset::<i32>(data, 0, index)?,
            offset::<i32>(data, 0, index.saturating_add(1))?,
            allow_view,
        ),
        DataType::LargeList(_) => list_values(
            py,
            data,
            schema,
            offset::<i64>(data, 0, index)?,
            offset::<i64>(data, 0, index.saturating_add(1))?,
            allow_view,
        ),
        DataType::ListView(_) => {
            let start = offset::<i32>(data, 0, index)?;
            let size = offset::<i32>(data, 1, index)?;
            list_values(
                py,
                data,
                schema,
                start,
                start.saturating_add(size),
                allow_view,
            )
        },
        DataType::LargeListView(_) => {
            let start = offset::<i64>(data, 0, index)?;
            let size = offset::<i64>(data, 1, index)?;
            list_values(
                py,
                data,
                schema,
                start,
                start.saturating_add(size),
                allow_view,
            )
        },
        DataType::FixedSizeList(_, size) => {
            let size = usize::try_from(*size)
                .map_err(|_| PyTypeError::new_err("invalid fixed-size list width"))?;
            let start = data.offset().saturating_add(index).saturating_mul(size);
            list_values(
                py,
                data,
                schema,
                start,
                start.saturating_add(size),
                allow_view,
            )
        },
        DataType::Map(..) => map_values(py, data, schema, index, allow_view),
        DataType::Struct(fields) => {
            let row = data.slice(index, 1);
            let output = PyDict::new(py);
            for (child_index, field) in fields.iter().enumerate() {
                let child = row
                    .child_data()
                    .get(child_index)
                    .expect("validated struct child");
                let child_schema = schema
                    .children
                    .get(child_index)
                    .expect("admitted struct field");
                output.set_item(field.name(), cell(py, child, child_schema, 0, allow_view)?)?;
            }
            Ok(output.into_any().unbind())
        },
        DataType::Dictionary(key, _) => {
            let key = match key.as_ref() {
                DataType::Int8 => usize::try_from(data.buffer::<i8>(0)[index]).ok(),
                DataType::Int16 => usize::try_from(data.buffer::<i16>(0)[index]).ok(),
                DataType::Int32 => usize::try_from(data.buffer::<i32>(0)[index]).ok(),
                DataType::Int64 => usize::try_from(data.buffer::<i64>(0)[index]).ok(),
                DataType::UInt8 => Some(usize::from(data.buffer::<u8>(0)[index])),
                DataType::UInt16 => Some(usize::from(data.buffer::<u16>(0)[index])),
                DataType::UInt32 => usize::try_from(data.buffer::<u32>(0)[index]).ok(),
                DataType::UInt64 => usize::try_from(data.buffer::<u64>(0)[index]).ok(),
                _ => return Err(PyTypeError::new_err("unsupported Arrow dictionary key")),
            }
            .ok_or_else(|| PyIndexError::new_err("invalid Arrow dictionary key"))?;
            let AdmittedDictionary::Captured(child_schema) = &schema.dictionary else {
                unreachable!("admitted dictionary carries its value schema");
            };
            let child = data
                .child_data()
                .first()
                .expect("validated dictionary values");
            cell(py, child, child_schema, key, allow_view)
        },
        DataType::Union(fields, mode) => {
            let type_id = *data
                .buffer::<i8>(0)
                .get(index)
                .expect("validated union type IDs");
            let child_index = fields
                .iter()
                .position(|(candidate, _)| candidate == type_id)
                .ok_or_else(|| PyTypeError::new_err("invalid Arrow union type ID"))?;
            let child_row = match mode {
                UnionMode::Sparse => data.offset().saturating_add(index),
                UnionMode::Dense => offset::<i32>(data, 1, index)?,
            };
            let child = data
                .child_data()
                .get(child_index)
                .expect("validated union child");
            let child_schema = schema
                .children
                .get(child_index)
                .expect("admitted union field");
            cell(py, child, child_schema, child_row, allow_view)
        },
        DataType::RunEndEncoded(..) => {
            let child = data.child_data().get(1).expect("validated run values");
            let child_schema = schema.children.get(1).expect("admitted run value schema");
            cell(py, child, child_schema, run_index(data, index)?, allow_view)
        },
        DataType::Timestamp(..)
        | DataType::Duration(..)
        | DataType::Time32(..)
        | DataType::Time64(..)
        | DataType::Date32
        | DataType::Date64 => {
            let (kind, value, zone, unit) =
                temporal_parts(data, index).expect("validated temporal value");
            if allow_view && (unit == "ns" || zone.is_some()) {
                return view(py, data, schema, index);
            }
            match temporal_as_py(py, kind, value, zone, unit) {
                Ok(value) => Ok(value),
                Err(error)
                    if allow_view
                        && (error.is_instance_of::<pyo3::exceptions::PyOverflowError>(py)
                            || error.is_instance_of::<pyo3::exceptions::PyValueError>(py)) =>
                {
                    view(py, data, schema, index)
                },
                Err(error) => Err(error),
            }
        },
        _ if allow_view => view(py, data, schema, index),
        _ => Err(PyTypeError::new_err(format!(
            "Arrow value of type {} has no exact Python conversion",
            data.data_type()
        ))),
    }
}

fn view(
    py: Python<'_>,
    data: &ArrayData,
    schema: &AdmittedArrowSchema,
    index: usize,
) -> PyResult<Py<PyAny>> {
    Ok(Py::new(py, ArrowValue {
        schema: Arc::new(schema.clone()),
        data: data.slice(index, 1),
    })?
    .into_any())
}

fn decimal(py: Python<'_>, value: &str, scale: i8) -> PyResult<Py<PyAny>> {
    Ok(py
        .import("decimal")?
        .getattr("Decimal")?
        .call1((format!("{value}e{}", i16::from(scale).wrapping_neg()),))?
        .unbind())
}
