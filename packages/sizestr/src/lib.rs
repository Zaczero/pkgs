use pyo3::prelude::*;

const BASE: f64 = 1024.0;
const SUFFIXES: [&str; 7] = [" KiB", " MiB", " GiB", " TiB", " PiB", " EiB", " ZiB"];

#[pyfunction]
fn sizestr(mut size: f64) -> String {
    if !size.is_finite() {
        return format!("({size})").to_string();
    }

    let prefix = if size.is_sign_negative() {
        size = -size;
        "-"
    } else {
        ""
    };

    if size < BASE {
        return format!("{prefix}{size:.0} B").to_string();
    }

    for suffix in SUFFIXES {
        size /= BASE;
        if size < BASE {
            let precision = if size < 10.0 { 2 } else { 1 };
            return format!("{prefix}{size:.precision$}{suffix}").to_string();
        }
    }

    "(too large to display)".to_string()
}

#[pymodule]
#[pyo3(name = "_lib")]
fn lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sizestr, m)?)?;
    Ok(())
}
