//! Coordinate-vertex argument parsing and broadcasting for the column-form
//! constructors (`line_string(x=, y=, …)`, `multi_point(...)`, etc.).
//!
//! `CoordinateInput` holds parsed `x`/`y`/`z`/`m` columns; the helpers coerce
//! Python sequences/scalars, broadcast scalar lanes to a common length, and
//! validate equal column lengths. Re-exported at the crate root via `use
//! super::*`.
use numpy::{PyUntypedArray, PyUntypedArrayMethods as _};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyFloat, PyInt};

use crate::boundary::buffer_endian;
use crate::boundary::buffer_endian::{
    buffer_item_count, buffer_to_column_f64, buffer_to_vec_f64, try_typed_buffer,
};
use crate::crs::{Zt, ZtLaneRefs, ZtLanes};
use crate::geometry::{CoordWindow, column_all_finite};
use crate::py::errors;
use crate::py::errors::{GeometryError, InvalidGeometryError};
use crate::py::numpy::reject_masked_array;
use crate::{
    CoordSeq, PyBuffer, collect_py_iter_at_most_from_iter, collect_py_iter_exact,
    collect_py_iter_exact_from_iter, collect_py_iter_from_iter, f64_required, f64_scalar_named,
    finite_coordinate_required, finite_f64_required, py_iter_or_error,
};

mod model;

mod parameters;

mod buffer;

mod broadcast;

pub(crate) use broadcast::{
    CrsCoordinateArgs, broadcast_coordinate_group, broadcast_coordinate_input,
    broadcast_crs_coordinate_inputs, broadcast_optional_coordinate_input,
    coordinate_input_with_expected, coordinate_inputs_are_scalar, coordinate_sequence_len_hint,
    optional_coordinate_input_with_expected, parameter_input_with_expected,
};
pub(crate) use buffer::{
    coordinate_column_values, coordinate_column_values_exact, coordinate_column_values_from_buffer,
    optional_coordinate_column_values, optional_coordinate_column_values_from_buffer,
    try_coordseq_from_nd_buffer,
};
pub(crate) use model::{
    CoordSeqBuffer, CoordSeqBufferMode, CoordSeqShape, CoordSeqStructure, CoordinateInput,
};
pub(crate) use parameters::{
    DefaultedF64Input, DefaultedI64Input, F64Param, FloatInput, I64Param, float_values,
    float_values_at_most, float_values_exact, reject_multidimensional_coordinate_array,
};
use parameters::{coordinate_scalar_named, reject_coordinate_buffer_rank};
