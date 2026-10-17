use std::fmt;

pub(crate) fn hex_lower(bytes: &[u8]) -> impl fmt::Display + '_ {
    fmt::from_fn(move |formatter| {
        for byte in bytes {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    })
}
