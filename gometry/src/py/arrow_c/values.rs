//! Provider-independent immutable Arrow values and their capsule ownership.

use std::sync::Arc;

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow_data::ArrayData;
use arrow_data::transform::MutableArrayData;
use arrow_schema::ffi::Flags;
use arrow_schema::{ArrowError, DataType, UnionMode};
use pyo3::IntoPyObjectExt as _;
use pyo3::basic::CompareOp;
use pyo3::exceptions::{PyIndexError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};

use super::admitted::{AdmittedArrowSchema, AdmittedDictionary, MovedArrowShell};
use super::{ArrowArray, ArrowSchema, array_capsule_name, capsule_pointer, schema_capsule_name};
use crate::py::arrow::arrow_import_error;

const _: () = {
    assert!(size_of::<ArrowArray>() == size_of::<FFI_ArrowArray>());
    assert!(align_of::<ArrowArray>() == align_of::<FFI_ArrowArray>());
    assert!(size_of::<ArrowSchema>() == size_of::<FFI_ArrowSchema>());
    assert!(align_of::<ArrowSchema>() == align_of::<FFI_ArrowSchema>());
};

pub(super) fn arrow_error(error: &ArrowError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

struct MetadataSchema {
    schema: FFI_ArrowSchema,
    metadata: Vec<u8>,
}

unsafe extern "C" fn release_metadata(schema: *mut ArrowSchema) {
    // SAFETY: this callback is installed only on shells backed by MetadataSchema.
    // Its inner library shell owns the children and has its original callback.
    unsafe {
        let schema = &mut *schema;
        let owner = schema.private_data.cast::<MetadataSchema>();
        schema.release = None;
        schema.private_data = std::ptr::null_mut();
        drop(Box::from_raw(owner));
    }
}

pub(super) fn export_schema(
    schema: &AdmittedArrowSchema,
    metadata: bool,
) -> PyResult<FFI_ArrowSchema> {
    let children = schema
        .children
        .iter()
        .map(|child| export_schema(child, metadata))
        .collect::<PyResult<Vec<_>>>()?;
    let dictionary = match &schema.dictionary {
        AdmittedDictionary::Absent => None,
        AdmittedDictionary::Captured(dictionary) => Some(export_schema(dictionary, metadata)?),
        AdmittedDictionary::Unselected => unreachable!("attribute schemas capture dictionaries"),
    };
    let exported = FFI_ArrowSchema::try_new(schema.format(), children, dictionary)
        .and_then(|value| value.with_name(schema.name()))
        .and_then(|value| value.with_flags(Flags::from_bits_retain(schema.flags)))
        .map_err(|error| arrow_error(&error))?;
    if !metadata || schema.metadata.is_empty() {
        return Ok(exported);
    }
    let mut bytes = Vec::new();
    bytes.extend(i32::try_from(schema.metadata.len())?.to_ne_bytes());
    for (key, value) in schema.metadata.iter() {
        bytes.extend(i32::try_from(key.len())?.to_ne_bytes());
        bytes.extend_from_slice(key);
        bytes.extend(i32::try_from(value.len())?.to_ne_bytes());
        bytes.extend_from_slice(value);
    }
    // Arrow's Rust Field metadata requires UTF-8; the C interface permits binary
    // metadata. Retain those bytes beside the library-owned physical schema.
    let owner = Box::new(MetadataSchema {
        schema: exported,
        metadata: bytes,
    });
    // SAFETY: the copied header borrows the inner shell's children. Its replacement
    // callback owns that inner shell and the metadata, so only the inner callback
    // releases children. No allocation or fallible operation occurs after copying.
    let mut exported = unsafe { std::ptr::read(&raw const owner.schema) };
    // SAFETY: both schema structures implement the Arrow C ABI.
    unsafe {
        let raw = &mut *std::ptr::from_mut(&mut exported).cast::<ArrowSchema>();
        raw.metadata = owner.metadata.as_ptr().cast();
        raw.private_data = Box::into_raw(owner).cast();
        raw.release = Some(release_metadata);
    }
    Ok(exported)
}

pub(super) fn snapshot(data: &ArrayData) -> Result<ArrayData, ArrowError> {
    if data.offset() != 0 {
        return compact(data);
    }
    let nulls = data.nulls().map(|nulls| {
        let bits = nulls.inner();
        NullBuffer::new(BooleanBuffer::new(
            Buffer::from_slice_ref(bits.values()),
            bits.offset(),
            bits.len(),
        ))
    });
    ArrayData::builder(data.data_type().clone())
        .len(data.len())
        .offset(data.offset())
        .nulls(nulls)
        .buffers(data.buffers().iter().map(Buffer::from_slice_ref).collect())
        .child_data(
            data.child_data()
                .iter()
                .map(snapshot)
                .collect::<Result<Vec<_>, _>>()?,
        )
        .build()
}

#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "only fixed-size lists and sparse unions need child-window normalization"
)]
pub(super) fn normalize_offset(data: &ArrayData) -> Result<ArrayData, ArrowError> {
    let children = data
        .child_data()
        .iter()
        .map(normalize_offset)
        .collect::<Result<Vec<_>, _>>()?;
    match data.data_type() {
        DataType::FixedSizeList(_, width) if data.offset() != 0 => {
            let width = usize::try_from(*width)
                .map_err(|_| ArrowError::InvalidArgumentError("invalid list width".into()))?;
            let start = data.offset().checked_mul(width).ok_or_else(|| {
                ArrowError::InvalidArgumentError("fixed-size list offset overflow".into())
            })?;
            let len = data.len().checked_mul(width).ok_or_else(|| {
                ArrowError::InvalidArgumentError("fixed-size list length overflow".into())
            })?;
            let child = children.first().expect("validated fixed-size list child");
            ArrayData::builder(data.data_type().clone())
                .len(data.len())
                .nulls(data.nulls().cloned())
                .child_data(vec![child.slice(start, len)])
                .build()
        },
        DataType::Union(_, UnionMode::Sparse) if data.offset() != 0 => {
            let children = children
                .iter()
                .map(|child| child.slice(data.offset(), data.len()))
                .collect();
            let buffer = data.buffers().first().expect("validated union type IDs");
            ArrayData::builder(data.data_type().clone())
                .len(data.len())
                .buffers(vec![buffer.slice(data.offset())])
                .child_data(children)
                .build()
        },
        _ => data.clone().into_builder().child_data(children).build(),
    }
}

pub(super) fn compact(data: &ArrayData) -> Result<ArrayData, ArrowError> {
    let normalized = normalize_offset(data)?;
    let mut output = MutableArrayData::try_new(vec![&normalized], true, data.len())?;
    output.try_extend(0, 0, data.len())?;
    snapshot(&output.freeze())
}

#[pyclass(name = "_ArrowArray", module = "gometry._lib", frozen, immutable_type)]
pub(crate) struct OwnedArrowArray {
    pub(super) schema: Arc<AdmittedArrowSchema>,
    pub(super) data: ArrayData,
}

/// An immutable Arrow-backed property value.
///
/// Values retain their Arrow type and can be converted to a Python value or
/// detached from the parent Arrow storage.
#[pyclass(name = "ArrowValue", module = "gometry", frozen, immutable_type)]
pub(crate) struct ArrowValue {
    pub(super) schema: Arc<AdmittedArrowSchema>,
    pub(super) data: ArrayData,
}

#[pymethods]
impl ArrowValue {
    #[classattr]
    const __hash__: Option<Py<PyAny>> = None;

    #[getter]
    /// The Arrow data type.
    ///
    /// Returns
    /// -------
    /// str
    ///     The Arrow data type as a string.
    fn r#type(&self) -> String {
        self.data.data_type().to_string()
    }

    #[getter]
    /// The integer value of a temporal Arrow scalar.
    ///
    /// Returns
    /// -------
    /// int
    ///     The value in the Arrow type's native units.
    ///
    /// Raises
    /// ------
    /// TypeError
    ///     If the Arrow value is not temporal.
    fn value(&self) -> PyResult<i64> {
        super::attribute_values::temporal_parts(&self.data, 0)
            .map(|(_, value, ..)| value)
            .ok_or_else(|| PyTypeError::new_err("this Arrow value is not temporal"))
    }

    fn __repr__(&self) -> String {
        format!("ArrowValue({})", self.data.data_type())
    }

    fn __richcmp__(
        &self,
        py: Python<'_>,
        other: &Bound<'_, PyAny>,
        op: CompareOp,
    ) -> PyResult<Py<PyAny>> {
        if !matches!(op, CompareOp::Eq | CompareOp::Ne) {
            return Ok(py.NotImplemented());
        }
        let Ok(other) = other.cast::<Self>() else {
            return Ok(py.NotImplemented());
        };
        let other = other.get();
        let equal = self.data == other.data
            && self.schema.metadata_value(b"ARROW:extension:name")
                == other.schema.metadata_value(b"ARROW:extension:name")
            && self.schema.metadata_value(b"ARROW:extension:metadata")
                == other.schema.metadata_value(b"ARROW:extension:metadata");
        (if matches!(op, CompareOp::Eq) {
            equal
        } else {
            !equal
        })
        .into_py_any(py)
    }

    /// Convert this value to its Python representation.
    ///
    /// Returns
    /// -------
    /// object
    ///     The converted value.
    fn as_py(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if let Some((kind, value, timezone, unit)) =
            super::attribute_values::temporal_parts(&self.data, 0)
        {
            return super::attribute_values::temporal_as_py(py, kind, value, timezone, unit);
        }
        super::attribute_values::cell(py, &self.data, &self.schema, 0, false)
    }

    /// Detach this value from its parent Arrow storage.
    ///
    /// Returns
    /// -------
    /// ArrowValue
    ///     An independently owned value with the same Arrow type.
    fn materialize(slf: &Bound<'_, Self>) -> PyResult<Py<Self>> {
        let owner = slf.clone().unbind();
        slf.py().detach(|| {
            let value = owner.get();
            let data = if matches!(
                value.data.data_type(),
                DataType::Timestamp(..) | DataType::Duration(..) | DataType::Time64(..)
            ) {
                let raw = value.value()?;
                ArrayData::builder(value.data.data_type().clone())
                    .len(1)
                    .buffers(vec![Buffer::from_slice_ref([raw])])
                    .build()
            } else {
                compact(&value.data)
            }
            .map_err(|error| arrow_error(&error))?;
            Python::attach(|py| {
                Py::new(py, Self {
                    schema: Arc::clone(&value.schema),
                    data,
                })
            })
        })
    }

    fn __reduce__(slf: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let detached = Self::materialize(slf)?;
        let detached = detached.get();
        let schema = super::state::schema_to_python(py, &detached.schema)?;
        let data = super::state::data_to_python(py, &detached.data)?;
        let restore = py.import("gometry._lib")?.getattr("_restore_arrow_value")?;
        (restore, (schema, data)).into_py_any(py)
    }

    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        PyCapsule::new_with_value(py, export_schema(&self.schema, true)?, c"arrow_schema")
    }

    #[pyo3(signature = (requested_schema = None))]
    fn __arrow_c_array__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<(Bound<'py, PyCapsule>, Bound<'py, PyCapsule>)> {
        let _ = requested_schema;
        Ok((
            self.__arrow_c_schema__(py)?,
            PyCapsule::new_with_value(py, FFI_ArrowArray::new(&self.data), c"arrow_array")?,
        ))
    }
}

#[pyfunction(name = "_arrow_value")]
fn arrow_value(
    py: Python<'_>,
    array: &Bound<'_, OwnedArrowArray>,
    index: usize,
) -> PyResult<Py<ArrowValue>> {
    let array = array.get();
    if index >= array.data.len() {
        return Err(PyIndexError::new_err("Arrow value index out of range"));
    }
    if array.data.is_null(index) {
        return Err(PyValueError::new_err(
            "a null Arrow value has no typed view",
        ));
    }
    Py::new(py, ArrowValue {
        schema: Arc::clone(&array.schema),
        data: array.data.slice(index, 1),
    })
}

#[pymethods]
impl OwnedArrowArray {
    const fn __len__(&self) -> usize {
        self.data.len()
    }

    fn __getitem__(&self, py: Python<'_>, index: usize) -> PyResult<Py<PyAny>> {
        super::attribute_values::cell(py, &self.data, &self.schema, index, true)
    }

    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        PyCapsule::new_with_value(py, export_schema(&self.schema, true)?, c"arrow_schema")
    }

    #[pyo3(signature = (requested_schema = None))]
    fn __arrow_c_array__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<(Bound<'py, PyCapsule>, Bound<'py, PyCapsule>)> {
        // Requested schemas are best-effort in the capsule protocol. This owner
        // exports its exact stored schema; the consumer owns any requested cast.
        let _ = requested_schema;
        Ok((
            self.__arrow_c_schema__(py)?,
            PyCapsule::new_with_value(py, FFI_ArrowArray::new(&self.data), c"arrow_array")?,
        ))
    }
}

#[pyfunction(name = "_snapshot_arrow_array")]
pub(crate) fn snapshot_arrow_array(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<Py<OwnedArrowArray>> {
    if let Ok(owned) = value.cast::<OwnedArrowArray>() {
        return Ok(owned.clone().unbind());
    }
    let capsules = value.call_method1("__arrow_c_array__", (py.None(),))?;
    let capsules = capsules.cast::<PyTuple>()?;
    if capsules.len() != 2 {
        return Err(PyTypeError::new_err(
            "__arrow_c_array__ must return (schema_capsule, array_capsule)",
        ));
    }
    let schema_capsule = capsules.get_item(0)?;
    let array_capsule = capsules.get_item(1)?;
    let schema = capsule_pointer::<ArrowSchema>(&schema_capsule, schema_capsule_name())?;
    let array = capsule_pointer::<ArrowArray>(&array_capsule, array_capsule_name())?;
    // SAFETY: named live capsules transfer each shell exactly once. The guards
    // remain attached and non-Send until all foreign buffers have been copied.
    let schema = unsafe { MovedArrowShell::take(schema) }?;
    // SAFETY: the array capsule is live and has not been consumed.
    let array = unsafe { MovedArrowShell::take(array) }?;
    // SAFETY: the moved shell pins the complete schema during owned capture.
    let admitted = unsafe { AdmittedArrowSchema::capture_values(&schema) }?;
    let data_type = DataType::try_from(&export_schema(&admitted, false)?)
        .map_err(|error| arrow_error(&error))?;
    // SAFETY: the moved shell keeps all producer buffers live and quiescent.
    let data = unsafe { super::snapshot_admission::snapshot_array(array.as_ptr(), &data_type) }
        .map_err(arrow_import_error)?;
    drop(array);
    drop(schema);
    Py::new(py, OwnedArrowArray {
        schema: Arc::new(admitted),
        data,
    })
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<ArrowValue>()?;
    module.add_class::<OwnedArrowArray>()?;
    super::column::register(module)?;
    super::schema::register(module)?;
    super::state::register(module)?;
    super::table::register(module)?;
    module.add_function(wrap_pyfunction!(arrow_value, module)?)?;
    module.add_function(wrap_pyfunction!(snapshot_arrow_array, module)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema(format: &str, name: &str) -> AdmittedArrowSchema {
        AdmittedArrowSchema {
            format: Arc::new(format.to_owned()),
            name: Arc::new(name.to_owned()),
            metadata: Arc::new(vec![(
                Box::from(&b"\xffkey"[..]),
                Box::from(&b"\xfe\0value"[..]),
            )]),
            flags: 2,
            dictionary: AdmittedDictionary::Absent,
            children: Arc::new(Vec::new()),
        }
    }

    #[test]
    fn binary_schema_metadata_survives_moved_release_lifetimes() {
        let mut column = schema("c", "region");
        column.dictionary = AdmittedDictionary::Captured(Box::new(schema("u", "")));
        let mut root = schema("+s", "features");
        root.children = Arc::new(vec![column]);
        for _ in 0..3 {
            let mut exported = export_schema(&root, true).expect("export binary schema");
            // SAFETY: this moves a live library-compatible schema shell into the
            // same owner used by capsule admission, clearing the original release.
            let moved = unsafe {
                MovedArrowShell::take(std::ptr::from_mut(&mut exported).cast::<ArrowSchema>())
            }
            .expect("move exported schema");
            assert!(exported.release().is_none());
            // SAFETY: moved owns the complete live schema and dictionary tree.
            let captured = unsafe { AdmittedArrowSchema::capture_values(&moved) }
                .expect("capture binary metadata");
            drop(moved);
            drop(exported);
            assert_eq!(captured.name(), "features");
            assert_eq!(
                captured.metadata_value(b"\xffkey"),
                Some(&b"\xfe\0value"[..])
            );
            let column = captured.children.first().expect("region column");
            assert_eq!(column.name(), "region");
            let AdmittedDictionary::Captured(dictionary) = &column.dictionary else {
                panic!("dictionary schema was not retained");
            };
            assert_eq!(dictionary.format(), "u");
            assert_eq!(
                dictionary.metadata_value(b"\xffkey"),
                Some(&b"\xfe\0value"[..])
            );
        }
    }

    #[test]
    fn dictionary_snapshot_owns_children_and_exports_after_owner_drop() {
        let values = ArrayData::builder(DataType::Utf8)
            .len(2)
            .buffers(vec![
                Buffer::from_slice_ref([0_i32, 5, 10]),
                Buffer::from_slice_ref(b"northsouth"),
            ])
            .build()
            .expect("dictionary values");
        let source = ArrayData::builder(DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::Utf8),
        ))
        .len(3)
        .buffers(vec![Buffer::from_slice_ref([0_i8, 1, 0])])
        .child_data(vec![values])
        .build()
        .expect("dictionary");
        let owned = snapshot(&source.slice(1, 2)).expect("snapshot");
        assert_ne!(
            source.buffer::<i8>(0).as_ptr(),
            owned.buffer::<i8>(0).as_ptr()
        );
        let owned_values = owned.child_data().first().expect("owned dictionary");
        let source_values = source.child_data().first().expect("source dictionary");
        assert_ne!(
            owned_values.buffer::<u8>(1).as_ptr(),
            source_values.buffer::<u8>(1).as_ptr(),
        );
        let data_type = owned.data_type().clone();
        let first = FFI_ArrowArray::new(&owned);
        let second = FFI_ArrowArray::new(&owned);
        drop(owned);
        drop(source);
        for array in [first, second] {
            // SAFETY: these shells were exported from fully owned validated data.
            let roundtrip =
                unsafe { arrow_array::ffi::from_ffi_and_data_type(array, data_type.clone()) }
                    .expect("import surviving export");
            assert_eq!(roundtrip.buffer::<i8>(0).get(..2), Some(&[1, 0][..]));
            assert_eq!(
                roundtrip
                    .child_data()
                    .first()
                    .expect("dictionary values")
                    .buffer::<u8>(1),
                b"northsouth",
            );
        }
    }
}
