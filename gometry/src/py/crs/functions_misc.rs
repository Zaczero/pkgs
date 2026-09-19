use crate::py::crs::{Bound, CRSError, Crs, PyAny, PyResult, crs, parse_crs_inner, pyfunction};
use crate::py::errors::native_memory_error;
/// Name/version of the underlying CRS engine (PROJ).
///
/// Returns
/// -------
/// dict
///     Engine metadata. ``paths`` is the effective per-context grid search
///     path configured through :func:`crs_configure`.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.crs_engine()['version']
/// '9.6.2'
#[pyfunction]
pub(crate) fn crs_engine() -> PyResult<crs::EngineInfo> {
    let state = crs::runtime_state_snapshot()?;
    Ok(crs::engine_info_with_state(&state)?)
}

/// One required-CRS parser: borrowed `Crs`/text through the shared
/// [`parse_crs_inner`] path (geometry `to_crs`, transforms, and normalize).
pub(crate) fn crs_parse_required(value: &Bound<'_, PyAny>) -> PyResult<Crs> {
    parse_crs_inner(value, 0)?.ok_or_else(|| CRSError::new_err("CRS is required"))
}

// Normalized CRS definition: the same parser as `crs_parse_required` /
// geometry `to_crs`, so syntax-only spellings ("Epsg:4326", "EPSG:4326") share
// one stored authority spelling without rewriting authored definitions.
pub(crate) fn crs_normalize(value: &Bound<'_, PyAny>) -> PyResult<String> {
    let crs = crs_parse_required(value)?;
    crate::boundary::try_string_from_str(crs.as_str()).map_or_else(
        |()| {
            drop(crs);
            Err(native_memory_error())
        },
        Ok,
    )
}

#[cfg(test)]
mod allocation_tests {
    use pyo3::exceptions::PyMemoryError;
    use pyo3::prelude::*;
    use pyo3::types::PyString;

    use super::*;
    use crate::crs::RUNTIME_STATE_TEST_LOCK;
    use crate::test_support::{allocation_count, fail_allocations_after};

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        clippy::needless_borrows_for_generic_args,
        reason = "the test classifies every injected normalization failure and retries the same closure"
    )]
    fn crs_normalization_allocation_failures_return_memory_error() -> PyResult<()> {
        Python::initialize();
        Python::attach(|py| {
            let _guard = RUNTIME_STATE_TEST_LOCK.lock().unwrap();
            let value = PyString::new(py, "EPSG:4326");
            let run = || crs_normalize(value.as_any());
            drop(run()?);
            let (_, allocations) = allocation_count(&run);
            assert!(allocations > 0, "CRS normalization did not allocate");
            for allowed in 0..allocations {
                let result = fail_allocations_after(allowed, &run);
                let Err(error) = result else {
                    panic!("CRS normalization prefix {allowed} unexpectedly succeeded");
                };
                assert!(error.is_instance_of::<PyMemoryError>(py));
                drop(error);
                drop(run()?);
            }
            Ok(())
        })
    }
}
