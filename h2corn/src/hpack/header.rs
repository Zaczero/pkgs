use std::{fmt, ops, str};

use bytes::Bytes;
use http::{Method, StatusCode};

use crate::ext::Protocol;
use crate::header_value::header_value_is_valid;
use crate::http::header::lowercase_header_name_is_valid;
use crate::http::types::parse_request_method;

use super::{DecoderError, NeedMore};

/// HTTP/2 Header
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Header {
    Field { name: BytesStr, value: Bytes },
    Authority(BytesStr),
    Method(Method),
    Scheme(BytesStr),
    Path(BytesStr),
    Protocol(Protocol),
    Status(StatusCode),
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum OwnedName {
    Field(BytesStr),
    Authority,
    Method,
    Scheme,
    Path,
    Protocol,
    Status,
}

#[doc(hidden)]
#[derive(Clone, Eq, PartialEq, Hash, Default)]
pub struct BytesStr(Bytes);

pub fn len(name: &BytesStr, value: &Bytes) -> usize {
    32 + name.len() + value.len()
}

impl Header {
    pub fn new(name: Bytes, value: Bytes) -> Result<Self, DecoderError> {
        if name.is_empty() {
            return Err(DecoderError::NeedMore(NeedMore::UnexpectedEndOfStream));
        }
        if let Some(name) = name.as_ref().strip_prefix(b":") {
            match name {
                b"authority" => {
                    let value = BytesStr::try_from(value)?;
                    Ok(Self::Authority(value))
                }
                b"method" => {
                    let method = parse_request_method(&value)?;
                    Ok(Self::Method(method))
                }
                b"scheme" => {
                    let value = BytesStr::try_from(value)?;
                    Ok(Self::Scheme(value))
                }
                b"path" => {
                    let value = BytesStr::try_from(value)?;
                    Ok(Self::Path(value))
                }
                b"protocol" => {
                    let value = Protocol::try_from(value)?;
                    Ok(Self::Protocol(value))
                }
                b"status" => {
                    let status = StatusCode::from_bytes(&value)?;
                    Ok(Self::Status(status))
                }
                _ => Err(DecoderError::InvalidPseudoheader),
            }
        } else {
            // HTTP/2 requires lower case header names
            if !lowercase_header_name_is_valid(name.as_ref())
                || !header_value_is_valid(value.as_ref())
            {
                return Err(DecoderError::InvalidUtf8);
            }

            Ok(Self::Field {
                // SAFETY: `lowercase_header_name_is_valid` accepted `name`, and
                // HTTP header name bytes are restricted to ASCII token bytes.
                name: unsafe { BytesStr::from_validated_ascii(name) },
                value,
            })
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Field { name, value } => len(name, value),
            Self::Authority(v) => 32 + 10 + v.len(),
            Self::Method(v) => 32 + 7 + v.as_ref().len(),
            Self::Scheme(v) => 32 + 7 + v.len(),
            Self::Path(v) => 32 + 5 + v.len(),
            Self::Protocol(v) => 32 + 9 + v.as_str().len(),
            Self::Status(_) => 32 + 7 + 3,
        }
    }

    pub fn owned_name(&self) -> OwnedName {
        match self {
            Self::Field { name, .. } => OwnedName::Field(name.clone()),
            Self::Authority(..) => OwnedName::Authority,
            Self::Method(..) => OwnedName::Method,
            Self::Scheme(..) => OwnedName::Scheme,
            Self::Path(..) => OwnedName::Path,
            Self::Protocol(..) => OwnedName::Protocol,
            Self::Status(..) => OwnedName::Status,
        }
    }
}

impl OwnedName {
    pub fn into_entry(self, value: Bytes) -> Result<Header, DecoderError> {
        match self {
            Self::Field(name) => {
                if !header_value_is_valid(value.as_ref()) {
                    return Err(DecoderError::InvalidUtf8);
                }
                Ok(Header::Field { name, value })
            }
            Self::Authority => Ok(Header::Authority(BytesStr::try_from(value)?)),
            Self::Method => Ok(Header::Method(parse_request_method(&value)?)),
            Self::Scheme => Ok(Header::Scheme(BytesStr::try_from(value)?)),
            Self::Path => Ok(Header::Path(BytesStr::try_from(value)?)),
            Self::Protocol => Ok(Header::Protocol(Protocol::try_from(value)?)),
            Self::Status => StatusCode::from_bytes(&value)
                .map(Header::Status)
                .map_err(|_| DecoderError::InvalidStatusCode),
        }
    }
}

impl BytesStr {
    pub(crate) const fn from_static_bytes(value: &'static [u8]) -> Self {
        Self(Bytes::from_static(value))
    }

    pub(crate) const fn from_static(value: &'static str) -> Self {
        Self::from_static_bytes(value.as_bytes())
    }

    pub(crate) unsafe fn from_validated_ascii(value: Bytes) -> Self {
        debug_assert!(value.iter().all(u8::is_ascii));
        Self(value)
    }

    pub(crate) unsafe fn from_validated_utf8(value: Bytes) -> Self {
        debug_assert!(str::from_utf8(value.as_ref()).is_ok());
        Self(value)
    }

    pub(crate) fn as_str(&self) -> &str {
        // SAFETY: `BytesStr` is only constructed through validated UTF-8
        // conversion paths or from string literals, so its backing bytes are
        // always valid UTF-8.
        unsafe { str::from_utf8_unchecked(self.0.as_ref()) }
    }

    pub(crate) fn into_inner(self) -> Bytes {
        self.0
    }
}

impl ops::Deref for BytesStr {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<[u8]> for BytesStr {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl AsRef<str> for BytesStr {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl From<&str> for BytesStr {
    fn from(value: &str) -> Self {
        Self(Bytes::copy_from_slice(value.as_bytes()))
    }
}

impl From<String> for BytesStr {
    fn from(value: String) -> Self {
        Self(Bytes::from(value))
    }
}

impl TryFrom<Bytes> for BytesStr {
    type Error = str::Utf8Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        str::from_utf8(bytes.as_ref())?;
        Ok(Self(bytes))
    }
}

impl fmt::Debug for BytesStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
