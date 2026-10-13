use pyo3::types::{PyBytes, PyTuple, PyTupleMethods as _};
use pyo3::{IntoPyObject as _, pyclass, pyfunction, pymethods};
use serde_json::Value;

use crate::crs::frame::{optional_crs_labels_operationally_equal, optional_epochs_equal};
use crate::py::arrow::{
    Bound, GeometryEncoding, Py, PyAny, PyAnyMethods as _, PyErr, PyModule, PyResult, PyTypeError,
    PyTypeMethods as _, Python, WkbOffsetWidth,
};
use crate::py::arrow_c::NativeArrowClassification;
use crate::py::errors::{GeometryError, ParseFormat, parse_error};
use crate::py::support::JsonValueError;
use crate::py::{arrow, errors, support};

#[path = "metadata/geoarrow.rs"]
mod geoarrow_metadata;

mod physical;

mod geoparquet;

#[cfg(test)]
mod tests;

pub(crate) use geoarrow_metadata::{
    decode_extension_name, geoarrow_parse_error, parse_geoarrow_extension_metadata,
    parse_geoarrow_extension_metadata_for, py_parse_geoarrow_extension_metadata,
    reconcile_extension_sources,
};
use geoarrow_metadata::{extension_name_present, field_extension_source, type_extension_source};
#[cfg(test)]
use geoparquet::GeoParquetAdmissionError;
pub(crate) use geoparquet::{
    GeoParquetColumnFrame, GeoParquetPhysicalLayout, py_admit_geoparquet_geometry_storage,
    py_parse_geoparquet_column_frame,
};
pub(crate) use physical::{arrow_classification_for_value, arrow_type_is_large_binary};
use physical::{
    arrow_type_is_binary_family, arrow_type_is_binary_view, geoarrow_native_layout,
    unwrap_extension_then_dictionary, wkb_offset_width,
};
