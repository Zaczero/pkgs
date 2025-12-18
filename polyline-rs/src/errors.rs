use std::borrow::Cow;
use std::fmt;

#[derive(Debug)]
pub(crate) enum Error {
    CoordinateMustContain2Values { index: usize },
}

impl Error {
    pub(crate) fn message(&self) -> Cow<'static, str> {
        match self {
            Self::CoordinateMustContain2Values { index } => Cow::Owned(format!(
                "Invalid coordinate: expected 2 values at index {index}"
            )),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message().as_ref())
    }
}
