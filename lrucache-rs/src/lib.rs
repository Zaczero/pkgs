#![feature(likely_unlikely)]

use core::hint::{likely, unlikely};
use nohash_hasher::{BuildNoHashHasher, IsEnabled};
use ordered_hash_map::OrderedHashMap;
use parking_lot::Mutex;
use pyo3::{
    exceptions::{PyKeyError, PyValueError},
    prelude::*,
    types::{PyIterator, PyTuple, PyType},
};
use std::hash::{Hash, Hasher};

struct PyObjectWrapper {
    hash: isize,
    obj: Py<PyAny>,
}

impl Hash for PyObjectWrapper {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_isize(self.hash);
    }
}

impl PartialEq for PyObjectWrapper {
    fn eq(&self, other: &Self) -> bool {
        if self.hash != other.hash {
            return false;
        }
        if self.obj.as_ptr() == other.obj.as_ptr() {
            return true;
        }
        Python::attach(|py| {
            self.obj
                .bind(py)
                .eq(other.obj.bind(py))
                .expect("LRUCache: key comparison (__eq__) raised")
        })
    }
}

impl Eq for PyObjectWrapper {}
impl IsEnabled for PyObjectWrapper {}

#[pyclass]
struct LRUCache {
    maxsize: usize,
    cache: Mutex<OrderedHashMap<PyObjectWrapper, Py<PyAny>, BuildNoHashHasher<PyObjectWrapper>>>,
}

impl LRUCache {
    fn wrap_key(py: Python<'_>, key: Py<PyAny>) -> PyResult<PyObjectWrapper> {
        Ok(PyObjectWrapper {
            hash: key.bind(py).hash()?,
            obj: key,
        })
    }

    fn key_repr_or_fallback(py: Python<'_>, key: &Py<PyAny>) -> String {
        key.bind(py)
            .repr()
            .map_or_else(|_| String::from("key not found"), |s| s.to_string())
    }
}

#[pymethods]
impl LRUCache {
    #[classmethod]
    fn __class_getitem__(
        cls: &Bound<'_, PyType>,
        _item: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyType>> {
        Ok(cls.clone().unbind())
    }

    #[new]
    fn new(maxsize: usize) -> PyResult<Self> {
        if unlikely(maxsize == 0) {
            Err(PyValueError::new_err("maxsize must be positive"))
        } else {
            Ok(Self {
                maxsize,
                cache: Mutex::new(OrderedHashMap::with_capacity_and_hasher(
                    maxsize,
                    BuildNoHashHasher::default(),
                )),
            })
        }
    }

    fn __len__(&self) -> usize {
        self.cache.lock().len()
    }

    fn __contains__(&self, py: Python, key: Py<PyAny>) -> PyResult<bool> {
        let key = Self::wrap_key(py, key)?;
        Ok(self.cache.lock().contains_key(&key))
    }

    fn __iter__<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyIterator>> {
        let cache = self.cache.lock();
        let tuple = PyTuple::new(py, cache.keys().map(|key| key.obj.clone_ref(py)))?;
        PyIterator::from_object(tuple.as_any())
    }

    fn __setitem__(&self, py: Python, key: Py<PyAny>, value: Py<PyAny>) -> PyResult<()> {
        let key = Self::wrap_key(py, key)?;
        let mut cache = self.cache.lock();
        cache.insert(key, value);
        if cache.len() > self.maxsize {
            cache.pop_front();
        }
        Ok(())
    }

    fn __getitem__(&self, py: Python, key: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let cache_key = Self::wrap_key(py, key)?;
        let mut cache = self.cache.lock();
        let value = cache.get(&cache_key);
        if likely(value.is_some()) {
            let result = value.unwrap().clone_ref(py);
            cache.move_to_back(&cache_key);
            drop(cache);
            Ok(result)
        } else {
            drop(cache);
            Err(PyKeyError::new_err(Self::key_repr_or_fallback(
                py,
                &cache_key.obj,
            )))
        }
    }

    fn __delitem__(&self, py: Python, key: Py<PyAny>) -> PyResult<()> {
        let cache_key = Self::wrap_key(py, key)?;
        if likely(self.cache.lock().remove(&cache_key).is_some()) {
            Ok(())
        } else {
            Err(PyKeyError::new_err(Self::key_repr_or_fallback(
                py,
                &cache_key.obj,
            )))
        }
    }

    #[pyo3(signature = (key, /, default=None))]
    fn get(&self, py: Python, key: Py<PyAny>, default: Option<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        let cache_key = Self::wrap_key(py, key)?;
        let mut cache = self.cache.lock();
        let value = cache.get(&cache_key);
        if likely(value.is_some()) {
            let result = value.unwrap().clone_ref(py);
            cache.move_to_back(&cache_key);
            drop(cache);
            Ok(result)
        } else {
            Ok(default.unwrap_or_else(|| py.None()))
        }
    }
}

#[pymodule(gil_used = false)]
#[pyo3(name = "_lib")]
fn lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<LRUCache>()?;
    Ok(())
}
