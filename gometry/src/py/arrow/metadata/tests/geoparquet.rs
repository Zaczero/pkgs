use pyo3::exceptions::{PyMemoryError, PyOverflowError};
use pyo3::types::PyAnyMethods as _;

use super::GeoParquetAdmissionError;
use crate::py::errors::{GeometryError, ParseError};

#[test]
fn geoparquet_error_categories_keep_python_projections_distinct() {
    crate::test_support::initialize_python();
    pyo3::Python::attach(|py| {
        let layout = GeoParquetAdmissionError::Layout("wrong storage".to_owned()).into_py_err();
        assert!(layout.is_instance_of::<GeometryError>(py));
        assert_eq!(
            layout
                .value(py)
                .getattr("category")
                .expect("layout category")
                .extract::<&str>()
                .expect("layout category string"),
            "layout"
        );

        let metadata =
            GeoParquetAdmissionError::Metadata("bad declaration".to_owned()).into_py_err();
        assert!(metadata.is_instance_of::<ParseError>(py));
        assert_eq!(
            metadata
                .value(py)
                .getattr("category")
                .expect("metadata category")
                .extract::<&str>()
                .expect("metadata category string"),
            "metadata"
        );
        assert_eq!(
            metadata
                .value(py)
                .getattr("format")
                .expect("metadata format")
                .extract::<&str>()
                .expect("metadata format string"),
            "geoparquet"
        );

        let resource =
            GeoParquetAdmissionError::Resource(crate::error::ResourceError::SizeOverflow.into())
                .into_py_err();
        assert!(resource.is_instance_of::<PyOverflowError>(py));
        assert_eq!(
            resource
                .value(py)
                .getattr("category")
                .expect("resource category")
                .extract::<&str>()
                .expect("resource category string"),
            "resource"
        );
    });
}

#[test]
fn geoparquet_python_memory_errors_preserve_exception_identity() {
    crate::test_support::initialize_python();
    pyo3::Python::attach(|py| {
        let original = PyMemoryError::new_err("provider allocation failed");
        for admitted in [
            GeoParquetAdmissionError::layout_from_py(original.clone_ref(py)),
            GeoParquetAdmissionError::from(original.clone_ref(py)),
        ] {
            let projected = admitted.into_py_err();
            assert!(projected.value(py).is(original.value(py)));
            assert_eq!(
                projected
                    .value(py)
                    .getattr("category")
                    .unwrap()
                    .extract::<&str>()
                    .unwrap(),
                "resource"
            );
        }
    });
}
