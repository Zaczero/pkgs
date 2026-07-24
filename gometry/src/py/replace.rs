//! Helpers for ``__replace__`` (``copy.replace`` on Python 3.13+).

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

pub(crate) enum ReplacePresence<T> {
    Unset,
    Set(T),
}

pub(crate) fn replace_presence(
    kwargs: Option<&Bound<'_, PyDict>>,
    key: &str,
) -> PyResult<ReplacePresence<f64>> {
    let Some(kwargs) = kwargs else {
        return Ok(ReplacePresence::Unset);
    };
    match kwargs.get_item(key)? {
        Some(value) => Ok(ReplacePresence::Set(crate::finite_coordinate_required(
            key, &value,
        )?)),
        None => Ok(ReplacePresence::Unset),
    }
}

pub(crate) fn reject_unknown_kwargs(
    kwargs: Option<&Bound<'_, PyDict>>,
    allowed: &[&str],
    cls_method: &str,
) -> PyResult<()> {
    let Some(kwargs) = kwargs else {
        return Ok(());
    };
    for (key, _) in kwargs.iter() {
        let key_str = key.extract::<&str>()?;
        if !allowed.contains(&key_str) {
            return Err(PyTypeError::new_err(format!(
                "{cls_method}() got an unexpected keyword argument '{key_str}'"
            )));
        }
    }
    Ok(())
}

pub(crate) fn replace_optional_f64(
    kwargs: Option<&Bound<'_, PyDict>>,
    key: &str,
    name: &str,
) -> PyResult<ReplacePresence<Option<f64>>> {
    let Some(kwargs) = kwargs else {
        return Ok(ReplacePresence::Unset);
    };
    match kwargs.get_item(key)? {
        None => Ok(ReplacePresence::Unset),
        Some(value) if value.is_none() => Ok(ReplacePresence::Set(None)),
        Some(value) => Ok(ReplacePresence::Set(Some(
            crate::finite_coordinate_required(name, &value)?,
        ))),
    }
}

pub(crate) fn replace_crs(
    kwargs: Option<&Bound<'_, PyDict>>,
    key: &str,
) -> PyResult<ReplacePresence<Option<crate::Crs>>> {
    let Some(kwargs) = kwargs else {
        return Ok(ReplacePresence::Unset);
    };
    match kwargs.get_item(key)? {
        None => Ok(ReplacePresence::Unset),
        Some(value) if value.is_none() => Ok(ReplacePresence::Set(None)),
        Some(value) => Ok(ReplacePresence::Set(crate::parse_crs(Some(&value))?)),
    }
}

pub(crate) fn replace_epoch(
    kwargs: Option<&Bound<'_, PyDict>>,
    key: &str,
) -> PyResult<ReplacePresence<Option<f64>>> {
    let Some(kwargs) = kwargs else {
        return Ok(ReplacePresence::Unset);
    };
    match kwargs.get_item(key)? {
        None => Ok(ReplacePresence::Unset),
        Some(value) if value.is_none() => Ok(ReplacePresence::Set(None)),
        Some(value) => Ok(ReplacePresence::Set(crate::coordinate_epoch_option(
            "epoch",
            Some(&value),
        )?)),
    }
}
