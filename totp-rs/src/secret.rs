use std::borrow::Cow;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};

use crate::base32::decode_base32_secret;
use crate::errors::TotpError;

pub(crate) enum Secret<'a> {
    Raw(&'a [u8]),
    Base32(&'a str),
}

impl<'a> Secret<'a> {
    pub(crate) fn from_py(secret: &'a Bound<'_, PyAny>) -> PyResult<Self> {
        if let Ok(value) = secret.cast::<PyBytes>() {
            return Ok(Self::Raw(value.as_bytes()));
        }
        if let Ok(value) = secret.cast::<PyString>() {
            return Ok(Self::Base32(value.to_str()?));
        }
        Err(TotpError::InvalidSecretType.into_pyerr())
    }

    pub(crate) fn decode(self) -> Result<Cow<'a, [u8]>, TotpError> {
        match self {
            Self::Raw(bytes) => Ok(Cow::Borrowed(bytes)),
            Self::Base32(text) => decode_base32_secret(text).map(Cow::Owned),
        }
    }
}
