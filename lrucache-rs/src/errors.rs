use std::borrow::Cow;
use std::fmt;

#[derive(Debug)]
pub(crate) enum Error {
    MaxsizeMustBePositive,
}

impl Error {
    pub(crate) fn message(&self) -> Cow<'static, str> {
        Cow::Borrowed("Invalid maxsize: must be positive")
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message().as_ref())
    }
}
