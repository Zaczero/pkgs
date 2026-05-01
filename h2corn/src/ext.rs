use std::str;

use bytes::Bytes;

use crate::hpack::BytesStr;

/// Represents the `:protocol` pseudo-header used by
/// the [Extended CONNECT Protocol].
///
/// [Extended CONNECT Protocol]: https://datatracker.ietf.org/doc/html/rfc8441#section-4
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Protocol(BytesStr);

impl Protocol {
    /// Returns a str representation of the header.
    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl From<&str> for Protocol {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

impl TryFrom<Bytes> for Protocol {
    type Error = str::Utf8Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        Ok(Self(BytesStr::try_from(bytes)?))
    }
}

impl AsRef<[u8]> for Protocol {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl AsRef<str> for Protocol {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}
