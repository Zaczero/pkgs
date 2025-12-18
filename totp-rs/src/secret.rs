use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyBytesMethods, PyString, PyStringMethods};
use std::borrow::Cow;

use crate::base32::decode_base32_secret;
use crate::errors::Error;

pub(crate) type SecretBytes<'a> = Cow<'a, [u8]>;

pub(crate) fn parse_secret_from_py<'a>(secret: &'a Bound<'_, PyAny>) -> PyResult<SecretBytes<'a>> {
    if let Ok(value) = secret.cast::<PyBytes>() {
        return Ok(Cow::Borrowed(value.as_bytes()));
    }

    if let Ok(value) = secret.cast::<PyString>() {
        let decoded = decode_base32_secret(value.to_str()?)
            .map_err(|err| PyValueError::new_err(err.message()))?;
        return Ok(Cow::Owned(decoded));
    }

    Err(PyValueError::new_err(Error::InvalidSecretType.message()))
}
