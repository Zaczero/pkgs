use pyo3::exceptions::PyOverflowError;
use pyo3::types::{PyAny, PyAnyMethods as _};
use pyo3::{Bound, PyResult, Python};

/// Report Python's allocation base plus the type's owned native heap estimate.
pub(crate) fn python_sizeof(
    py: Python<'_>,
    instance: &Bound<'_, PyAny>,
    heap_bytes: usize,
) -> PyResult<isize> {
    let base_bytes = py
        .import("builtins")?
        .getattr("object")?
        .getattr("__sizeof__")?
        .call1((instance,))?
        .extract::<isize>()?;
    checked_sizeof(base_bytes, heap_bytes)
}

fn checked_sizeof(base_bytes: isize, heap_bytes: usize) -> PyResult<isize> {
    let heap_bytes = isize::try_from(heap_bytes).map_err(|_| size_overflow())?;
    base_bytes.checked_add(heap_bytes).ok_or_else(size_overflow)
}

fn size_overflow() -> pyo3::PyErr {
    PyOverflowError::new_err("__sizeof__ result exceeds Py_ssize_t")
}

#[cfg(test)]
mod tests {
    use pyo3::Python;
    use pyo3::exceptions::PyOverflowError;

    use super::checked_sizeof;

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn python_sizeof_accepts_signed_limit_and_rejects_overflow() {
        let limit = usize::try_from(isize::MAX).expect("isize maximum fits usize");
        let one_below = limit.checked_sub(1).expect("isize maximum is nonzero");
        assert_eq!(checked_sizeof(1, one_below).unwrap(), isize::MAX);

        let python_limit_plus_one = limit
            .checked_add(1)
            .expect("isize maximum plus one fits usize");
        let over_limit = checked_sizeof(0, python_limit_plus_one)
            .expect_err("Py_ssize_t overflow must be reported");
        Python::initialize();
        assert!(Python::attach(
            |py| over_limit.is_instance_of::<PyOverflowError>(py)
        ));

        let addition_overflow = checked_sizeof(isize::MAX, 1)
            .expect_err("the inline and heap total must be checked before addition");
        assert!(Python::attach(
            |py| addition_overflow.is_instance_of::<PyOverflowError>(py)
        ));
    }
}
