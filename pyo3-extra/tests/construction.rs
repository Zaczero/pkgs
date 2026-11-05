use std::sync::{Arc, Barrier};

use pyo3::types::{
    PyAnyMethods as _, PyBool, PyBytes, PyBytesMethods as _, PyDictMethods as _, PyString,
};
use pyo3::{Bound, IntoPyObjectExt as _, PyResult, Python};
use pyo3_extra::{cached_bytes, dict};

fn init_python() {
    Python::initialize();
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn py_dict_interns_static_string_literal_values() {
    init_python();
    Python::attach(|py| -> PyResult<()> {
        let dict = dict!(py, {
            "type" => "http.request",
        });
        let value = dict.get_item("type")?.unwrap().cast_into::<PyString>()?;
        let interned = PyString::intern(py, "http.request");

        assert!(value.is(&interned));
        Ok(())
    })
    .unwrap();
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn py_match_cached_bytes_list_form_caches_each_literal() {
    init_python();
    Python::attach(|py| -> PyResult<()> {
        let cached = |value: &str| cached_bytes!(py, value, ["", "/known"]);
        let root = cached("/known");
        let repeated = cached("/known");
        let empty = cached("");
        let other = cached("/other");
        assert_eq!(root.as_bytes(), b"/known");
        assert!(root.is(&repeated));
        assert_eq!(empty.as_bytes(), b"");
        assert_eq!(other.as_bytes(), b"/other");
        Ok(())
    })
    .unwrap();
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn py_dict_caches_static_bytes_literal_values() {
    init_python();
    Python::attach(|py| -> PyResult<()> {
        let build = || -> PyResult<_> {
            Ok(dict!(py, {
                "body" => b"fixed payload",
            }))
        };
        let first = build()?;
        let second = build()?;
        let first_value = first.get_item("body")?.unwrap().cast_into::<PyBytes>()?;
        let second_value = second.get_item("body")?.unwrap().cast_into::<PyBytes>()?;

        assert!(first_value.is(&second_value));
        Ok(())
    })
    .unwrap();
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn py_dict_uses_python_bool_singletons_for_bool_literals() {
    init_python();
    Python::attach(|py| -> PyResult<()> {
        let dict = dict!(py, {
            "more_body" => true,
        });
        let value = dict.get_item("more_body")?.unwrap().cast_into::<PyBool>()?;
        let expected = true.into_bound_py_any(py)?.cast_into::<PyBool>()?;

        assert!(value.is(&expected));
        Ok(())
    })
    .unwrap();
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn bulk_dictionary_retains_borrowed_temporaries_and_duplicate_values() {
    init_python();
    Python::attach(|py| -> PyResult<()> {
        let dict = dict!(py, {
            "value" => &PyString::new(py, "temporary"),
            "other" => &pyo3::types::PyList::empty(py),
            "value" => 42,
            "false" => false,
        });
        assert_eq!(dict.len(), 3);
        assert_eq!(dict.get_item("value")?.unwrap().extract::<i32>()?, 42);
        assert_eq!(dict.get_item("other")?.unwrap().len()?, 0);
        assert!(!dict.get_item("false")?.unwrap().is_truthy()?);
        assert!(dict!(py, {}).is_empty());
        Ok(())
    })
    .unwrap();
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn bulk_dictionary_evaluates_values_in_order_once() {
    init_python();
    Python::attach(|py| -> PyResult<()> {
        let mut seen = Vec::new();
        let dict = dict!(py, {
            "first" => { seen.push(1); 1 },
            "second" => { seen.push(2); 2 },
            "third" => { seen.push(3); 3 },
        });
        assert_eq!(seen, [1, 2, 3]);
        assert_eq!(dict.len(), 3);
        Ok(())
    })
    .unwrap();
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn py_dict_omits_false_conditional_fields() {
    init_python();
    Python::attach(|py| -> PyResult<()> {
        let include = false;
        let dict = dict!(py, {
            "first" => 1,
            if include => {
                "omitted" => 2,
            },
            "last" => 3,
        });
        assert_eq!(dict.len(), 2);
        assert_eq!(dict.get_item("first")?.unwrap().extract::<usize>()?, 1);
        assert!(dict.get_item("omitted")?.is_none());
        assert_eq!(dict.get_item("last")?.unwrap().extract::<usize>()?, 3);
        Ok(())
    })
    .unwrap();
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn dictionary_expressions_are_evaluated_once() {
    init_python();
    Python::attach(|py| -> PyResult<()> {
        let tokens = std::cell::Cell::new(0_u32);
        let values = std::cell::Cell::new(0_u32);
        let conditions = std::cell::Cell::new(0_u32);
        let dict = dict!({
            tokens.set(tokens.get().checked_add(1).unwrap());
            py
        }, {
            "value" => {
                values.set(values.get().checked_add(1).unwrap());
                31
            },
            if {
                conditions.set(conditions.get().checked_add(1).unwrap());
                false
            } => {
                "omitted" => {
                    values.set(99);
                    0
                },
            },
            if let Some(value) = Some(47) => {
                "optional" => value,
            },
        });
        assert_eq!(tokens.get(), 1);
        assert_eq!(values.get(), 1);
        assert_eq!(conditions.get(), 1);
        assert_eq!(dict.len(), 2);
        assert_eq!(dict.get_item("value")?.unwrap().extract::<i32>()?, 31);
        assert_eq!(dict.get_item("optional")?.unwrap().extract::<i32>()?, 47);
        Ok(())
    })
    .unwrap();
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn failed_dictionary_construction_releases_previous_values() {
    struct FailingValue(pyo3::PyErr);

    impl<'py> pyo3::IntoPyObject<'py> for FailingValue {
        type Target = pyo3::PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = pyo3::PyErr;

        fn into_pyobject(self, _py: Python<'py>) -> Result<Self::Output, Self::Error> {
            Err(self.0)
        }
    }

    init_python();
    Python::attach(|py| -> PyResult<()> {
        let payload = py.eval(c"type('Payload', (), {})()", None, None)?;
        let weak = py.import("weakref")?.getattr("ref")?.call1((&payload,))?;
        let failure = pyo3::exceptions::PyValueError::new_err("conversion sentinel");
        let expected = failure.value(py).clone();
        let result = (move || -> PyResult<Bound<'_, pyo3::types::PyDict>> {
            Ok(dict!(py, {
                "first" => payload,
                "second" => FailingValue(failure),
            }))
        })();
        let error = result.expect_err("second expression fails");
        assert!(error.value(py).is(&expected));
        py.import("gc")?.call_method0("collect")?;
        assert!(weak.call0()?.is_none());
        Ok(())
    })
    .unwrap();
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn concurrent_dictionary_construction_keeps_values_local() {
    const WORKERS: usize = 4;
    init_python();
    let barrier = Arc::new(Barrier::new(WORKERS));
    let workers = std::array::from_fn::<_, WORKERS, _>(|worker| {
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || -> PyResult<()> {
            barrier.wait();
            Python::attach(|py| {
                #[cfg(Py_GIL_DISABLED)]
                assert!(
                    !py.import("sys")?
                        .call_method0("_is_gil_enabled")?
                        .extract::<bool>()?
                );
                for value in 17..49 {
                    let dict = dict!(py, {
                        "worker" => worker,
                        "value" => value,
                        "kind" => "concurrent",
                    });
                    assert_eq!(
                        dict.get_item("worker")?.unwrap().extract::<usize>()?,
                        worker
                    );
                    assert_eq!(dict.get_item("value")?.unwrap().extract::<i32>()?, value);
                    assert!(
                        dict.get_item("kind")?
                            .unwrap()
                            .is(pyo3::intern!(py, "concurrent"))
                    );
                }
                Ok(())
            })
        })
    });
    for worker in workers {
        worker
            .join()
            .expect("worker thread does not panic")
            .unwrap();
    }
}
