use std::borrow::Cow;

use crate::base32::decode_base32_secret;
use crate::errors::TotpError;

pub(crate) enum Secret<'a> {
    Raw(&'a [u8]),
    Base32(&'a str),
}

impl<'a> Secret<'a> {
    pub(crate) fn decode(self) -> Result<Cow<'a, [u8]>, TotpError> {
        match self {
            Self::Raw(bytes) => Ok(Cow::Borrowed(bytes)),
            Self::Base32(text) => decode_base32_secret(text).map(Cow::Owned),
        }
    }
}
