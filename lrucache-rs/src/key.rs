use nohash_hasher::IsEnabled;
use pyo3::prelude::*;
use std::hash::{Hash, Hasher};

pub(crate) struct PyObjectWrapper {
    pub(crate) hash: isize,
    pub(crate) obj: Py<PyAny>,
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
