use bytes::Bytes;
use http::{Method, StatusCode};

use super::{
    Header,
    header::{BytesStr, OwnedName},
};

pub const STATIC_TABLE_LEN: usize = 61;
pub const DYNAMIC_INDEX_OFFSET: usize = STATIC_TABLE_LEN + 1;
const EMPTY_VALUE: &[u8] = b"";

#[derive(Clone, Copy)]
pub struct StaticFieldEntry {
    pub index: usize,
    pub exact_value: &'static [u8],
    pub skip_value_index: bool,
    pub never_index: bool,
}

impl StaticFieldEntry {
    const fn new(index: usize) -> Self {
        Self {
            index,
            exact_value: EMPTY_VALUE,
            skip_value_index: false,
            never_index: false,
        }
    }

    const fn with_value(mut self, exact_value: &'static [u8]) -> Self {
        self.exact_value = exact_value;
        self
    }

    const fn skip_value_index(mut self) -> Self {
        self.skip_value_index = true;
        self
    }

    const fn never_index(mut self) -> Self {
        self.never_index = true;
        self
    }
}

macro_rules! static_field_data {
    ($($first:literal => { $(($index:literal, $name:literal, $value:literal $(, $flag:ident)*),)+ }),+ $(,)?) => {
        const _: () = {
            $($(
                assert!(!$name.is_empty());
                assert!($name[0] == $first);
            )+)+
        };

        fn get_field(index: usize) -> Header {
            match index {
                $($(
                    $index => Header::Field {
                        name: BytesStr::from_static_bytes($name),
                        value: Bytes::from_static($value),
                    },
                )+)+
                _ => unreachable!(),
            }
        }

        pub fn field_index_entry(name: &[u8]) -> Option<StaticFieldEntry> {
            match name.first().copied() {
                $(
                Some($first) => match name {
                    $($name => Some(StaticFieldEntry::new($index).with_value($value)$(.$flag())*),)+
                    _ => None,
                },
                )+
                _ => None,
            }
        }

        fn field_name(index: usize) -> OwnedName {
            match index {
                $($($index => OwnedName::Field(BytesStr::from_static_bytes($name)),)+)+
                _ => unreachable!(),
            }
        }
    };
}

static_field_data! {
    b'a' => {
        (15, b"accept-charset", b""),
        (16, b"accept-encoding", b"gzip, deflate"),
        (17, b"accept-language", b""),
        (18, b"accept-ranges", b""),
        (19, b"accept", b""),
        (20, b"access-control-allow-origin", b""),
        (21, b"age", b"", skip_value_index),
        (22, b"allow", b""),
        (23, b"authorization", b"", skip_value_index, never_index),
    },
    b'c' => {
        (24, b"cache-control", b""),
        (25, b"content-disposition", b""),
        (26, b"content-encoding", b""),
        (27, b"content-language", b""),
        (28, b"content-length", b"", skip_value_index),
        (29, b"content-location", b""),
        (30, b"content-range", b""),
        (31, b"content-type", b""),
        (32, b"cookie", b"", skip_value_index),
    },
    b'd' => {
        (33, b"date", b""),
    },
    b'e' => {
        (34, b"etag", b"", skip_value_index),
        (35, b"expect", b""),
        (36, b"expires", b""),
    },
    b'f' => {
        (37, b"from", b""),
    },
    b'h' => {
        (38, b"host", b""),
    },
    b'i' => {
        (39, b"if-match", b""),
        (40, b"if-modified-since", b"", skip_value_index),
        (41, b"if-none-match", b"", skip_value_index),
        (42, b"if-range", b""),
        (43, b"if-unmodified-since", b""),
    },
    b'l' => {
        (44, b"last-modified", b""),
        (45, b"link", b""),
        (46, b"location", b"", skip_value_index),
    },
    b'm' => {
        (47, b"max-forwards", b""),
    },
    b'p' => {
        (48, b"proxy-authenticate", b""),
        (49, b"proxy-authorization", b"", never_index),
    },
    b'r' => {
        (50, b"range", b""),
        (51, b"referer", b""),
        (52, b"refresh", b""),
        (53, b"retry-after", b""),
    },
    b's' => {
        (54, b"server", b""),
        (55, b"set-cookie", b"", skip_value_index, never_index),
        (56, b"strict-transport-security", b""),
    },
    b't' => {
        (57, b"transfer-encoding", b""),
    },
    b'u' => {
        (58, b"user-agent", b""),
    },
    b'v' => {
        (59, b"vary", b""),
        (60, b"via", b""),
    },
    b'w' => {
        (61, b"www-authenticate", b""),
    },
}

pub fn get(index: usize) -> Header {
    match index {
        1 => Header::Authority(BytesStr::from_static_bytes(b"")),
        2 => Header::Method(Method::GET),
        3 => Header::Method(Method::POST),
        4 => Header::Path(BytesStr::from_static_bytes(b"/")),
        5 => Header::Path(BytesStr::from_static_bytes(b"/index.html")),
        6 => Header::Scheme(BytesStr::from_static_bytes(b"http")),
        7 => Header::Scheme(BytesStr::from_static_bytes(b"https")),
        8 => Header::Status(StatusCode::OK),
        9 => Header::Status(StatusCode::NO_CONTENT),
        10 => Header::Status(StatusCode::PARTIAL_CONTENT),
        11 => Header::Status(StatusCode::NOT_MODIFIED),
        12 => Header::Status(StatusCode::BAD_REQUEST),
        13 => Header::Status(StatusCode::NOT_FOUND),
        14 => Header::Status(StatusCode::INTERNAL_SERVER_ERROR),
        15..=STATIC_TABLE_LEN => get_field(index),
        _ => unreachable!(),
    }
}

pub fn name(index: usize) -> OwnedName {
    match index {
        1 => OwnedName::Authority,
        2 | 3 => OwnedName::Method,
        4 | 5 => OwnedName::Path,
        6 | 7 => OwnedName::Scheme,
        8..=14 => OwnedName::Status,
        15..=STATIC_TABLE_LEN => field_name(index),
        _ => unreachable!(),
    }
}

#[cfg(test)]
pub const fn status_index(status: u16) -> Option<usize> {
    match status {
        200 => Some(8),
        204 => Some(9),
        206 => Some(10),
        304 => Some(11),
        400 => Some(12),
        404 => Some(13),
        500 => Some(14),
        _ => None,
    }
}

#[cfg(test)]
pub fn exact_index(header: &Header) -> Option<usize> {
    match header {
        Header::Field { name, value } => {
            field_exact_index_bytes(name.as_str().as_bytes(), value.as_ref())
        }
        Header::Authority(value) if value.as_str().is_empty() => Some(1),
        Header::Method(Method::GET) => Some(2),
        Header::Method(Method::POST) => Some(3),
        Header::Scheme(value) => match value.as_str() {
            "http" => Some(6),
            "https" => Some(7),
            _ => None,
        },
        Header::Path(value) => match value.as_str() {
            "/" => Some(4),
            "/index.html" => Some(5),
            _ => None,
        },
        Header::Status(status) => status_index(status.as_u16()),
        Header::Protocol(_) | Header::Authority(_) | Header::Method(_) => None,
    }
}

#[cfg(test)]
pub fn field_exact_index_bytes(name: &[u8], value: &[u8]) -> Option<usize> {
    let entry = field_index_entry(name)?;
    (entry.exact_value == value).then_some(entry.index)
}

#[cfg(test)]
mod test {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn field_exact_lookup_matches_static_entry() {
        assert_eq!(
            field_exact_index_bytes(b"accept-encoding", b"gzip, deflate"),
            Some(16)
        );
        assert_eq!(
            field_exact_index_bytes(b"content-type", b"text/plain"),
            None
        );
    }

    #[test]
    fn generic_exact_lookup_matches_pseudo_headers() {
        assert_eq!(exact_index(&Header::Method(Method::GET)), Some(2));
        assert_eq!(
            exact_index(&Header::Status(StatusCode::NOT_FOUND)),
            Some(13)
        );
        assert_eq!(
            exact_index(&Header::Path(
                BytesStr::try_from(Bytes::from_static(b"/")).unwrap()
            )),
            Some(4)
        );
    }
}
