use pyo3::PyErr;

use crate::py::errors::{GeometryError, InvalidGeometryError, integer_parameter_error};

#[derive(Debug)]
pub(crate) enum CellArrayError {
    DepthOutOfRange {
        noun: &'static str,
        parameter: &'static str,
        min: i64,
        max: i64,
        value: i64,
    },
    Geometry(String),
    InvalidGeometry(String),
    Native(crate::error::Error),
}

impl CellArrayError {
    pub(crate) fn depth(
        value: i64,
        noun: &'static str,
        parameter: &'static str,
        min: i64,
        max: i64,
    ) -> Result<u8, Self> {
        if (min..=max).contains(&value) {
            return Ok(u8::try_from(value).expect("cell depth bounds fit in u8"));
        }
        Err(Self::DepthOutOfRange {
            noun,
            parameter,
            min,
            max,
            value,
        })
    }
}

impl From<crate::error::Error> for CellArrayError {
    fn from(error: crate::error::Error) -> Self {
        Self::Native(error)
    }
}

impl From<crate::error::ResourceError> for CellArrayError {
    fn from(error: crate::error::ResourceError) -> Self {
        Self::Native(error.into())
    }
}

impl From<CellArrayError> for PyErr {
    fn from(error: CellArrayError) -> Self {
        match error {
            CellArrayError::DepthOutOfRange {
                noun,
                parameter,
                min,
                max,
                value,
            } => integer_parameter_error(
                format!("{noun} must be between {min} and {max}, got {value}"),
                parameter,
                value,
            ),
            CellArrayError::Geometry(message) => GeometryError::new_err(message),
            CellArrayError::InvalidGeometry(message) => InvalidGeometryError::new_err(message),
            CellArrayError::Native(error) => error.into(),
        }
    }
}
