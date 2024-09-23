use pyo3::{exceptions::PyValueError, prelude::*};
use std::{
    fs::{self},
    path::PathBuf,
    str::FromStr,
};

#[pyfunction]
#[pyo3(signature = (dir_ = PathBuf::from(".git")))]
fn githead(dir_: PathBuf) -> PyResult<String> {
    let head_path: PathBuf = dir_.join("HEAD");
    let head_raw = fs::read_to_string(head_path)?;
    let head = head_raw.trim_end();

    let is_symbolic = head.starts_with("ref: ");
    if !is_symbolic {
        return Ok(String::from_str(head)?);
    }

    let dir_abs = fs::canonicalize(dir_)?;
    let ref_abs = fs::canonicalize(dir_abs.join(&head[5..]))?;
    if !ref_abs.ancestors().any(|p| p == dir_abs) {
        return Err(PyValueError::new_err(
            "HEAD references outside of .git directory",
        ));
    }

    let ref_raw = fs::read_to_string(ref_abs)?;
    let ref_ = ref_raw.trim_end();
    Ok(String::from_str(ref_)?)
}

#[pymodule]
#[pyo3(name = "_lib")]
fn lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(githead, m)?)?;
    Ok(())
}
