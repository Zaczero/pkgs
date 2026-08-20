use pyo3::prelude::*;

pub(crate) fn initialize_python() {
    Python::initialize();
    Python::attach(|py| {
        // Embedded interpreters do not inherit the launcher-configured module
        // search path, so install it before importing NumPy.
        if let Ok(paths) = std::env::var("PYTHONPATH") {
            let sys = PyModule::import(py, "sys").expect("import sys in embedded Python");
            let path = sys
                .getattr("path")
                .expect("read sys.path in embedded Python");
            for entry in paths.split(':').filter(|entry| !entry.is_empty()) {
                path.call_method1("append", (entry,))
                    .expect("append PYTHONPATH entry to embedded sys.path");
            }
        }
        py.import("numpy").unwrap_or_else(|error| {
            panic!(
                "gometry's Rust tests require NumPy in the embedded Python interpreter. \
                 Install NumPy into the interpreter selected by PYO3_PYTHON, for example: \
                 uv pip install --python \"$PYO3_PYTHON\" numpy. \
                 Original Python error: {error}"
            )
        });
    });
}
