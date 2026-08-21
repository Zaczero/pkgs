//! One boundary contract for H3 cell, vertex, and directed-edge indexes.
//!
//! The foreign `h3o` index types deliberately do not share a public trait.
//! This sealed local contract unifies Python scalar parsing and bulk id
//! validation without exposing an extension point or erasing the concrete
//! index type.

use h3o::{CellIndex, DirectedEdgeIndex, VertexIndex};
use numpy::{PyReadonlyArrayDyn, PyUntypedArray, PyUntypedArrayMethods as _};
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyInt};

use crate::py::errors::{ParseFormat, parse_error};

mod sealed {
    pub(in crate::py::cells) trait Sealed {}
}

/// Closed conversion vocabulary shared by the three `h3o` index types.
pub(in crate::py::cells) trait H3Index: sealed::Sealed + Copy {
    const ENTITY: &'static str;
    const ARRAY_ENTITY: &'static str;
    const ARRAY_TYPE_ERROR: &'static str;
    const TYPE_ERROR: &'static str;
    const STRING_ERROR_KIND: &'static str;

    fn from_id(id: u64) -> Option<Self>;
    fn from_token(token: &str) -> Option<Self>;
    fn id(self) -> u64;
}

macro_rules! impl_h3_index {
    (
        $ty:ty,
        entity: $entity:literal,
        array_entity: $array_entity:literal,
        array_type_error: $array_type_error:literal,
        type_error: $type_error:literal,
        string_error_kind: $string_error_kind:literal
    ) => {
        impl sealed::Sealed for $ty {}

        impl H3Index for $ty {
            const ENTITY: &'static str = $entity;
            const ARRAY_ENTITY: &'static str = $array_entity;
            const ARRAY_TYPE_ERROR: &'static str = $array_type_error;
            const TYPE_ERROR: &'static str = $type_error;
            const STRING_ERROR_KIND: &'static str = $string_error_kind;

            fn from_id(id: u64) -> Option<Self> {
                Self::try_from(id).ok()
            }

            fn from_token(token: &str) -> Option<Self> {
                token.parse().ok()
            }

            fn id(self) -> u64 {
                self.into()
            }
        }
    };
}

impl_h3_index!(
    CellIndex,
    entity: "H3 cell",
    array_entity: "H3 cell",
    array_type_error: "cell array ids must be a uint64 numpy array or an iterable of cell objects or integer ids",
    type_error: "H3 cell must be an H3Cell, integer id, or string token",
    string_error_kind: "token"
);
impl_h3_index!(
    VertexIndex,
    entity: "H3 vertex",
    array_entity: "H3 vertex",
    array_type_error: "H3 vertex array ids must be an iterable",
    type_error: "H3 vertex must be an H3Vertex, integer id, or string token",
    string_error_kind: "id"
);
impl_h3_index!(
    DirectedEdgeIndex,
    entity: "H3 edge",
    array_entity: "H3 edge",
    array_type_error: "H3 edge array ids must be an iterable",
    type_error: "H3 edge must be an H3Edge, integer id, or string token",
    string_error_kind: "id"
);

fn invalid_id<I: H3Index>(id: u64) -> PyErr {
    parse_error(format!("invalid {} id {id}", I::ENTITY), ParseFormat::H3)
}

fn invalid_token<I: H3Index>(token: &str) -> PyErr {
    parse_error(
        format!("invalid {} {} {token:?}", I::ENTITY, I::STRING_ERROR_KIND),
        ParseFormat::H3,
    )
}

pub(in crate::py::cells) fn validate_h3_index_id<I: H3Index>(id: u64) -> PyResult<I> {
    I::from_id(id).ok_or_else(|| invalid_id::<I>(id))
}

pub(in crate::py::cells) fn validate_h3_index_ids<I: H3Index>(ids: &[u64]) -> PyResult<()> {
    ids.iter()
        .try_for_each(|&id| validate_h3_index_id::<I>(id).map(|_| ()))
}

/// Parse one scalar after the concrete wrapper supplies its exact-class fast
/// path. Integer and token behavior is otherwise identical across all H3
/// index kinds.
pub(super) fn parse_h3_index<I: H3Index>(
    value: &Bound<'_, PyAny>,
    native: impl FnOnce(&Bound<'_, PyAny>) -> Option<I>,
) -> PyResult<I> {
    if let Some(index) = native(value) {
        return Ok(index);
    }
    if value.cast_exact::<PyBool>().is_err() && value.cast::<PyInt>().is_ok() {
        let id = value.extract::<u64>().map_err(|_| {
            parse_error(
                format!("{} id must be a non-negative 64-bit integer", I::ENTITY),
                ParseFormat::H3,
            )
        })?;
        return validate_h3_index_id::<I>(id);
    }
    let token: String = value
        .extract()
        .map_err(|_| PyTypeError::new_err(I::TYPE_ERROR))?;
    I::from_token(&token).ok_or_else(|| invalid_token::<I>(&token))
}

fn owned_h3_ids_from_tobytes<I: H3Index, T>(ids: &Bound<'_, PyAny>) -> PyResult<Vec<u64>>
where
    T: Copy + TryInto<u64>,
{
    use pyo3::types::PyBytes;
    let py_bytes = ids.call_method0("tobytes")?;
    let bytes = py_bytes.cast::<PyBytes>()?.as_bytes();
    let width = std::mem::size_of::<T>();
    if width == 0 || !bytes.len().is_multiple_of(width) {
        return Err(PyTypeError::new_err(format!(
            "{} ids buffer length is invalid",
            I::ARRAY_ENTITY
        )));
    }
    let mut out = Vec::with_capacity(bytes.len() / width);
    for chunk in bytes.chunks_exact(width) {
        // SAFETY: dtype matched by extract; tobytes is native-endian for T.
        let value = unsafe { std::ptr::read_unaligned(chunk.as_ptr().cast::<T>()) };
        let id = value.try_into().map_err(|_| {
            PyTypeError::new_err(format!(
                "{} ids must be unsigned 64-bit integers",
                I::ARRAY_ENTITY
            ))
        })?;
        out.push(id);
    }
    Ok(out)
}

fn parse_uint64_ids<I: H3Index>(ids: &Bound<'_, PyAny>) -> PyResult<Option<Vec<u64>>> {
    let Ok(array) = ids.cast::<PyUntypedArray>() else {
        return Ok(None);
    };
    if array.ndim() > 1 {
        return Err(PyTypeError::new_err(format!(
            "{} ids must be zero- or one-dimensional",
            I::ARRAY_ENTITY
        )));
    }
    macro_rules! try_array {
        ($($ty:ty),* $(,)?) => {
            $(
                if ids.extract::<PyReadonlyArrayDyn<'_, $ty>>().is_ok() {
                    // Owned capture via `tobytes()` — never an ArrayView over
                    // writable NumPy memory, so no borrow outlives this call.
                    // The free-threaded exporter allowlist that
                    // `boundary::coordinate_input` needs for its raw pointer
                    // loads has nothing to protect here, and applying it would
                    // reject every NumPy array the GIL build accepts.
                    return Ok(Some(owned_h3_ids_from_tobytes::<I, $ty>(ids)?));
                }
            )*
        };
    }
    try_array!(u8, u16, u32, u64, usize);
    try_array!(i8, i16, i32, i64, isize);
    // Object / string / other dtypes are not packed uint64 columns — fall
    // through so the collector walks them as an ordinary iterable.
    Ok(None)
}

pub(super) fn collect_h3_index_ids<I: H3Index>(
    ids: &Bound<'_, PyAny>,
    parse: impl Fn(&Bound<'_, PyAny>) -> PyResult<I>,
) -> PyResult<Vec<u64>> {
    if ids.is_instance_of::<pyo3::types::PyBytes>()
        || ids.is_instance_of::<pyo3::types::PyByteArray>()
        || (ids.is_instance_of::<pyo3::types::PyMemoryView>()
            && matches!(
                ids.getattr("format")?.extract::<String>()?.as_str(),
                "B" | "b" | "c"
            ))
    {
        return Err(PyTypeError::new_err(
            "byte payloads are not H3 collections; use payload.decode() for one textual token or a list/uint64 array for numeric ids",
        ));
    }
    if let Some(values) = parse_uint64_ids::<I>(ids)? {
        validate_h3_index_ids::<I>(&values)?;
        return Ok(values);
    }
    if let Ok(buffer) = PyBuffer::<u64>::get(ids) {
        // m07: same rank rule as ndarray — 0/1-D only (no silent flatten).
        if buffer.dimensions() > 1 {
            return Err(PyTypeError::new_err(format!(
                "{} ids must be zero- or one-dimensional",
                I::ARRAY_ENTITY
            )));
        }
        // Normalize non-native endian (`>u8`) — raw to_vec reinterprets bytes
        // and yields invalid-looking ids that then over-reject at validate.
        let values = crate::buffer_to_vec_u64(ids.py(), &buffer)?;
        validate_h3_index_ids::<I>(&values)?;
        return Ok(values);
    }
    // Fallible growth (D10): `H3VertexArray(itertools.repeat(...))` etc. must
    // MemoryError rather than capacity-overflow abort / hang.
    crate::collect_py_iter(ids, |item| parse(&item).map(H3Index::id)).map_err(|err| {
        if err.is_instance_of::<pyo3::exceptions::PyMemoryError>(ids.py()) {
            err
        } else if err.is_instance_of::<PyTypeError>(ids.py())
            && err.to_string().contains("not iterable")
        {
            PyTypeError::new_err(I::ARRAY_TYPE_ERROR)
        } else {
            err
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generic_validation_preserves_concrete_index_types() {
        crate::test_support::initialize_python();

        let cell: CellIndex = "8928308280fffff".parse().unwrap();
        let vertex = cell.vertexes().next().unwrap();
        let edge = cell.edges().next().unwrap();

        assert_eq!(
            validate_h3_index_id::<CellIndex>(cell.into()).unwrap(),
            cell
        );
        assert_eq!(
            validate_h3_index_id::<VertexIndex>(vertex.into()).unwrap(),
            vertex
        );
        assert_eq!(
            validate_h3_index_id::<DirectedEdgeIndex>(edge.into()).unwrap(),
            edge
        );
        let _cell_error = validate_h3_index_id::<CellIndex>(0).unwrap_err();
        let _vertex_error = validate_h3_index_id::<VertexIndex>(0).unwrap_err();
        let _edge_error = validate_h3_index_id::<DirectedEdgeIndex>(0).unwrap_err();
    }
}
