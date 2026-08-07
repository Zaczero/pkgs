use std::borrow::Cow;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};

use crate::base32::decode_base32_secret;
use crate::errors::TotpError;

pub(crate) type SecretBytes<'a> = Cow<'a, [u8]>;

pub(crate) fn parse_secret_from_py<'a>(secret: &'a Bound<'_, PyAny>) -> PyResult<SecretBytes<'a>> {
    if let Ok(value) = secret.cast::<PyBytes>() {
        return Ok(Cow::Borrowed(value.as_bytes()));
    }

    if let Ok(value) = secret.cast::<PyString>() {
        let decoded = decode_base32_secret(value.to_str()?).map_err(TotpError::into_pyerr)?;
        return Ok(Cow::Owned(decoded));
    }

    Err(TotpError::InvalidSecretType.into_pyerr())
}
