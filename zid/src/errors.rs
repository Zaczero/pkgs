use std::borrow::Cow;
use std::fmt;

use crate::constants::MAX_ZIDS_AT_ONCE;

#[derive(Debug)]
pub(crate) enum Error {
    TooManyZidsAtOnce { attempted: usize },
}

impl Error {
    pub(crate) fn message(&self) -> Cow<'static, str> {
        match self {
            Self::TooManyZidsAtOnce { attempted } => Cow::Owned(format!(
                "Up to {MAX_ZIDS_AT_ONCE} ZIDs can be generated at once (attempted {attempted})"
            )),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message().as_ref())
    }
}
