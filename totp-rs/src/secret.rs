use std::borrow::Cow;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyBytesMethods, PyString, PyStringMethods};

use crate::base32::decode_base32_secret;
use crate::errors::Error;

pub type SecretBytes<'a> = Cow<'a, [u8]>;

pub fn parse_secret_from_py<'a>(secret: &'a Bound<'_, PyAny>) -> PyResult<SecretBytes<'a>> {
    if let Ok(value) = secret.cast::<PyBytes>() {
        return Ok(Cow::Borrowed(value.as_bytes()));
    }

    if let Ok(value) = secret.cast::<PyString>() {
        let decoded = decode_base32_secret(value.to_str()?).map_err(Error::into_pyerr)?;
        return Ok(Cow::Owned(decoded));
    }

    Err(Error::InvalidSecretType.into_pyerr())
}
