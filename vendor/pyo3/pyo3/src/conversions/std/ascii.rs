//! Typed ASCII scalars and sequences map to Python strings without UTF-8 admission.

use core::ascii::Char;

use crate::conversion::private::{Reference, Token};
use crate::types::PyString;
use crate::{Bound, IntoPyObject, PyAny, PyErr, Python};
#[cfg(feature = "experimental-inspect")]
use crate::{inspect::PyStaticExpr, type_object::PyTypeInfo};

impl<'py> IntoPyObject<'py> for Char {
    type Target = PyString;
    type Output = Bound<'py, PyString>;
    type Error = PyErr;

    #[cfg(feature = "experimental-inspect")]
    const OUTPUT_TYPE: PyStaticExpr = PyString::TYPE_HINT;
    #[cfg(feature = "experimental-inspect")]
    const SEQUENCE_OUTPUT_TYPE: PyStaticExpr = PyString::TYPE_HINT;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyString::from_ascii(py, core::slice::from_ref(&self))
    }

    fn owned_sequence_into_pyobject<I>(
        characters: I,
        py: Python<'py>,
        _: Token,
    ) -> Result<Bound<'py, PyAny>, PyErr>
    where
        I: AsRef<[Char]>,
    {
        PyString::from_ascii(py, characters.as_ref()).map(Bound::into_any)
    }
}

impl<'py> IntoPyObject<'py> for &Char {
    type Target = PyString;
    type Output = Bound<'py, PyString>;
    type Error = PyErr;

    #[cfg(feature = "experimental-inspect")]
    const OUTPUT_TYPE: PyStaticExpr = PyString::TYPE_HINT;
    #[cfg(feature = "experimental-inspect")]
    const SEQUENCE_OUTPUT_TYPE: PyStaticExpr = PyString::TYPE_HINT;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyString::from_ascii(py, core::slice::from_ref(self))
    }

    fn borrowed_sequence_into_pyobject<I>(
        characters: I,
        py: Python<'py>,
        _: Token,
    ) -> Result<Bound<'py, PyAny>, PyErr>
    where
        I: AsRef<[<Self as Reference>::BaseType]>,
    {
        PyString::from_ascii(py, characters.as_ref()).map(Bound::into_any)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{PyAnyMethods, PyStringMethods};

    #[test]
    fn ascii_sequences_are_strings_not_lists() {
        Python::attach(|py| {
            let text = b"a\0z".as_ascii().unwrap();
            for value in [
                text.into_pyobject(py).unwrap(),
                text.to_vec().into_pyobject(py).unwrap(),
                (*text).into_pyobject(py).unwrap(),
            ] {
                assert!(value.is_instance_of::<PyString>());
                assert_eq!(value.cast::<PyString>().unwrap().to_str().unwrap(), "a\0z");
            }
            assert_eq!(text[0].into_pyobject(py).unwrap().to_str().unwrap(), "a");
        });
    }
}
