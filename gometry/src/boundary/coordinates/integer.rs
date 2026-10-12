use pyo3::types::{PyAnyMethods as _, PyInt};
use pyo3::{Bound, PyResult, Python};

#[derive(Clone)]
pub(super) enum ExactUnsigned<'py> {
    Small(u128),
    Big(Bound<'py, PyInt>),
}

impl<'py> ExactUnsigned<'py> {
    pub(super) const ZERO: Self = Self::Small(0);

    pub(super) fn add(&self, py: Python<'py>, other: &Self) -> PyResult<Self> {
        match (self, other) {
            (Self::Small(left), Self::Small(right)) => left.checked_add(*right).map_or_else(
                || {
                    let left = PyInt::new(py, *left);
                    let right = PyInt::new(py, *right);
                    Self::add_python_ints(&left, &right)
                },
                |sum| Ok(Self::Small(sum)),
            ),
            (Self::Big(left), Self::Small(right)) => {
                let right = PyInt::new(py, *right);
                Self::add_python_ints(left, &right)
            },
            (Self::Small(left), Self::Big(right)) => {
                let left = PyInt::new(py, *left);
                Self::add_python_ints(&left, right)
            },
            (Self::Big(left), Self::Big(right)) => Self::add_python_ints(left, right),
        }
    }

    fn add_python_ints(left: &Bound<'py, PyInt>, right: &Bound<'py, PyInt>) -> PyResult<Self> {
        add_python_integers(left, right).map(Self::Big)
    }

    pub(super) fn into_pyint(self, py: Python<'py>) -> Bound<'py, PyInt> {
        match self {
            Self::Small(value) => usize::try_from(value)
                .map_or_else(|_| PyInt::new(py, value), |value| PyInt::new(py, value)),
            Self::Big(value) => value,
        }
    }

    pub(super) const fn fits_py_ssize(&self) -> Option<usize> {
        match self {
            Self::Small(value) if *value <= isize::MAX as u128 => Some(*value as usize),
            Self::Small(_) | Self::Big(_) => None,
        }
    }

    pub(super) fn equals(&self, py: Python<'py>, other: &Self) -> PyResult<bool> {
        match (self, other) {
            (Self::Small(left), Self::Small(right)) => Ok(left == right),
            (Self::Big(left), Self::Big(right)) => left.as_any().eq(right.as_any()),
            (Self::Big(left), Self::Small(right)) => left.as_any().eq(PyInt::new(py, *right)),
            (Self::Small(left), Self::Big(right)) => right.as_any().eq(PyInt::new(py, *left)),
        }
    }
}

pub(super) fn add_python_integers<'py>(
    left: &Bound<'py, PyInt>,
    right: &Bound<'py, PyInt>,
) -> PyResult<Bound<'py, PyInt>> {
    Ok(left.as_any().add(right.as_any())?.cast_into::<PyInt>()?)
}
