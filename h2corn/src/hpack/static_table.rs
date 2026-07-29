use bytes::Bytes;

use super::HpackField;

macro_rules! static_field_data {
    ($($first:literal => { $(($index:literal, $name:literal, $value:literal $(, $flag:ident)*),)+ }),+ $(,)?) => {
        const _: () = {
            $($(
                assert!(!$name.is_empty());
                assert!($name[0] == $first);
            )+)+
        };

        fn get_field(index: usize) -> HpackField {
            match index {
                $($(
                    $index => HpackField::from_static($name, $value),
                )+)+
                _ => unreachable!(),
            }
        }

        pub(crate) const fn field_index_entry(name: &[u8]) -> Option<StaticFieldEntry> {
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

        fn field_name(index: usize) -> Bytes {
            match index {
                $($($index => Bytes::from_static($name),)+)+
                _ => unreachable!(),
            }
        }

    };
}

pub(super) const STATIC_TABLE_LEN: usize = 61;
pub(super) const DYNAMIC_INDEX_OFFSET: usize = STATIC_TABLE_LEN + 1;
const EMPTY_VALUE: &[u8] = b"";

#[derive(Clone, Copy)]
pub(super) struct StaticFieldEntry {
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

pub(super) fn get(index: usize) -> HpackField {
    match index {
        1 => HpackField::from_static(b":authority", b""),
        2 => HpackField::from_static(b":method", b"GET"),
        3 => HpackField::from_static(b":method", b"POST"),
        4 => HpackField::from_static(b":path", b"/"),
        5 => HpackField::from_static(b":path", b"/index.html"),
        6 => HpackField::from_static(b":scheme", b"http"),
        7 => HpackField::from_static(b":scheme", b"https"),
        8 => HpackField::from_static(b":status", b"200"),
        9 => HpackField::from_static(b":status", b"204"),
        10 => HpackField::from_static(b":status", b"206"),
        11 => HpackField::from_static(b":status", b"304"),
        12 => HpackField::from_static(b":status", b"400"),
        13 => HpackField::from_static(b":status", b"404"),
        14 => HpackField::from_static(b":status", b"500"),
        15..=STATIC_TABLE_LEN => get_field(index),
        _ => unreachable!(),
    }
}

pub(super) fn name(index: usize) -> Bytes {
    match index {
        1 => Bytes::from_static(b":authority"),
        2 | 3 => Bytes::from_static(b":method"),
        4 | 5 => Bytes::from_static(b":path"),
        6 | 7 => Bytes::from_static(b":scheme"),
        8..=14 => Bytes::from_static(b":status"),
        15..=STATIC_TABLE_LEN => field_name(index),
        _ => unreachable!(),
    }
}

#[cfg(test)]
pub(super) fn field_exact_index_bytes(name: &[u8], value: &[u8]) -> Option<usize> {
    let entry = field_index_entry(name)?;
    (entry.exact_value == value).then_some(entry.index)
}

#[cfg(test)]
mod tests {
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
    fn static_pseudo_headers_are_raw_fields() {
        assert_eq!(get(2).name(), b":method");
        assert_eq!(get(2).value(), b"GET");
        assert_eq!(get(13).name(), b":status");
        assert_eq!(get(13).value(), b"404");
        assert_eq!(get(4).name(), b":path");
        assert_eq!(get(4).value(), b"/");
    }
}
