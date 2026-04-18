use std::fmt;
use std::ops;
use std::str;

use bytes::Bytes;
use http::{Method, StatusCode};

use crate::ext::Protocol;

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

const HEADER_NAME_VALID_H2: [u8; 256] = {
    let mut table = [0; 256];

    let mut byte = b'0';
    while byte <= b'9' {
        table[byte as usize] = 1;
        byte += 1;
    }

    let mut byte = b'a';
    while byte <= b'z' {
        table[byte as usize] = 1;
        byte += 1;
    }

    let bytes = b"!#$%&'*+-.^_`|~";
    let mut index = 0;
    while index < bytes.len() {
        table[bytes[index] as usize] = 1;
        index += 1;
    }

    table
};

pub fn len(name: &BytesStr, value: &Bytes) -> usize {
    32 + name.len() + value.len()
}

impl Header {
    pub fn new(name: Bytes, value: Bytes) -> Result<Header, DecoderError> {
        if name.is_empty() {
            return Err(DecoderError::NeedMore(NeedMore::UnexpectedEndOfStream));
        }
        if let Some(name) = name.as_ref().strip_prefix(b":") {
            match name {
                b"authority" => {
                    let value = BytesStr::try_from(value)?;
                    Ok(Header::Authority(value))
                }
                b"method" => {
                    let method = Method::from_bytes(&value)?;
                    Ok(Header::Method(method))
                }
                b"scheme" => {
                    let value = BytesStr::try_from(value)?;
                    Ok(Header::Scheme(value))
                }
                b"path" => {
                    let value = BytesStr::try_from(value)?;
                    Ok(Header::Path(value))
                }
                b"protocol" => {
                    let value = Protocol::try_from(value)?;
                    Ok(Header::Protocol(value))
                }
                b"status" => {
                    let status = StatusCode::from_bytes(&value)?;
                    Ok(Header::Status(status))
                }
                _ => Err(DecoderError::InvalidPseudoheader),
            }
        } else {
            // HTTP/2 requires lower case header names
            if !header_name_is_valid_h2(name.as_ref()) || !header_value_is_valid(value.as_ref()) {
                return Err(DecoderError::InvalidUtf8);
            }

            Ok(Header::Field {
                name: BytesStr::try_from(name).expect("validated lower-case names are UTF-8"),
                value,
            })
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Header::Field { name, value } => len(name, value),
            Header::Authority(v) => 32 + 10 + v.len(),
            Header::Method(v) => 32 + 7 + v.as_ref().len(),
            Header::Scheme(v) => 32 + 7 + v.len(),
            Header::Path(v) => 32 + 5 + v.len(),
            Header::Protocol(v) => 32 + 9 + v.as_str().len(),
            Header::Status(_) => 32 + 7 + 3,
        }
    }

    pub fn owned_name(&self) -> OwnedName {
        match self {
            Header::Field { name, .. } => OwnedName::Field(name.clone()),
            Header::Authority(..) => OwnedName::Authority,
            Header::Method(..) => OwnedName::Method,
            Header::Scheme(..) => OwnedName::Scheme,
            Header::Path(..) => OwnedName::Path,
            Header::Protocol(..) => OwnedName::Protocol,
            Header::Status(..) => OwnedName::Status,
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
            Self::Method => Ok(Header::Method(Method::from_bytes(&value)?)),
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
        BytesStr(Bytes::from_static(value))
    }

    pub(crate) const fn from_static(value: &'static str) -> Self {
        Self::from_static_bytes(value.as_bytes())
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
        BytesStr(Bytes::copy_from_slice(value.as_bytes()))
    }
}

impl From<String> for BytesStr {
    fn from(value: String) -> Self {
        BytesStr(Bytes::from(value))
    }
}

impl TryFrom<Bytes> for BytesStr {
    type Error = str::Utf8Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        str::from_utf8(bytes.as_ref())?;
        Ok(BytesStr(bytes))
    }
}

impl fmt::Debug for BytesStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

fn header_name_is_valid_h2(name: &[u8]) -> bool {
    !name.is_empty()
        && name
            .iter()
            .all(|byte| HEADER_NAME_VALID_H2[usize::from(*byte)] != 0)
}

fn header_value_is_valid(value: &[u8]) -> bool {
    value
        .iter()
        .all(|byte| *byte == b'\t' || (*byte >= 32 && *byte != 127))
}
