// Fixture Rust source for the source-scanning gates (never compiled).

/// A runtime-unsubclassable class.
#[pyclass(name = "Sealed", frozen)]
pub struct PySealed {
    inner: u64,
}

#[pymethods]
impl PySealed {
    #[getter]
    fn maybe(&self) -> Option<i64> {
        None
    }
}

/// A subclassable class.
#[pyclass(name = "Open", subclass)]
pub struct PyOpen;

/// An unnamed pyclass: exports under its Rust identifier.
#[pyclass]
#[derive(Clone)]
pub struct Orphan {
    value: u8,
}

#[pyfunction]
#[pyo3(signature = (a, b = "x"), text_signature = "(a, b='x')")]
fn documented(a: i64, b: &str) -> String {
    b.repeat(a as usize)
}
