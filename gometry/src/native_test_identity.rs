use std::path::PathBuf;
use std::{env, fs};

use pyo3::prelude::*;
use pyo3::types::PyModule;
use sha2::{Digest, Sha256};

fn embedded_python_identity() -> serde_json::Value {
    Python::initialize();
    Python::attach(|py| -> PyResult<serde_json::Value> {
        let sys = PyModule::import(py, "sys")?;
        let platform = PyModule::import(py, "platform")?;
        let sysconfig = PyModule::import(py, "sysconfig")?;
        let builtins = PyModule::import(py, "builtins")?;
        let gil_disabled = builtins
            .getattr("bool")?
            .call1((sysconfig.call_method1("get_config_var", ("Py_GIL_DISABLED",))?,))?
            .extract::<bool>()?;
        let gil_enabled = if sys.hasattr("_is_gil_enabled")? {
            Some(sys.call_method0("_is_gil_enabled")?.extract::<bool>()?)
        } else {
            None
        };

        Ok(serde_json::json!({
            "version": sys.getattr("version")?.extract::<String>()?,
            "implementation": platform
                .call_method0("python_implementation")?
                .extract::<String>()?,
            "cache_tag": sys
                .getattr("implementation")?
                .getattr("cache_tag")?
                .extract::<String>()?,
            "ext_suffix": sysconfig
                .call_method1("get_config_var", ("EXT_SUFFIX",))?
                .extract::<String>()?,
            "gil_disabled": gil_disabled,
            "gil_enabled": gil_enabled,
        }))
    })
    .expect("embedded Python identity is readable")
}

#[test]
fn records_the_running_nextest_binary_when_requested() {
    let Ok(receipt_path) = env::var("GOMETRY_NATIVE_TEST_RECEIPT") else {
        return;
    };
    let invocation_run_id = env::var("GOMETRY_NATIVE_TEST_RUN_ID")
        .expect("native test receipt requires its invocation UUID");
    let nextest_run_id =
        env::var("NEXTEST_RUN_ID").expect("native test receipt requires the Nextest run UUID");
    let selected_python = env::var("PYO3_PYTHON")
        .expect("native test receipt requires the selected PyO3 interpreter");
    let executable = env::current_exe().expect("current test executable is available");
    let bytes = fs::read(&executable).expect("current test executable is readable");
    assert!(
        bytes.starts_with(b"\x7fELF"),
        "native test runner must execute ELF"
    );
    let embedded_python = embedded_python_identity();
    let gil_disabled = embedded_python["gil_disabled"]
        .as_bool()
        .expect("embedded Python reports its free-threaded build mode");
    if gil_disabled {
        assert_eq!(
            embedded_python["gil_enabled"].as_bool(),
            Some(false),
            "embedded free-threaded Python must keep the GIL disabled"
        );
    }

    let value = serde_json::json!({
        "schema": 2,
        "kind": "gometry-native-test-binary",
        "invocation_run_id": invocation_run_id,
        "nextest_run_id": nextest_run_id,
        "test_name": "records_the_running_nextest_binary_when_requested",
        "selected_python": selected_python,
        "embedded_python": embedded_python,
        "pid": std::process::id(),
        "executable": executable,
        "size_bytes": bytes.len(),
        "sha256": format!("{:x}", Sha256::digest(&bytes)),
        "source_inputs_format": env!("GOMETRY_BUILD_SOURCE_INPUTS_FORMAT"),
        "source_inputs_sha256": env!("GOMETRY_BUILD_SOURCE_INPUTS_SHA256"),
        "native_source_inputs_format": env!("GOMETRY_BUILD_NATIVE_SOURCE_INPUTS_FORMAT"),
        "native_source_inputs_sha256": env!("GOMETRY_BUILD_NATIVE_SOURCE_INPUTS_SHA256"),
        "profile_family": env!("GOMETRY_BUILD_PROFILE_FAMILY"),
        "opt_level": env!("GOMETRY_BUILD_OPT_LEVEL"),
        "debug_assertions": env!("GOMETRY_BUILD_DEBUG_ASSERTIONS") == "true",
        "target": env!("GOMETRY_BUILD_TARGET"),
        "target_arch": env!("GOMETRY_BUILD_TARGET_ARCH"),
        "target_features": env!("GOMETRY_BUILD_TARGET_FEATURES"),
    });

    let destination = PathBuf::from(receipt_path);
    fs::create_dir_all(
        destination
            .parent()
            .expect("native test receipt has a parent directory"),
    )
    .expect("native test receipt directory can be created");
    let temporary = destination.with_extension(format!("{}.tmp", std::process::id()));
    fs::write(
        &temporary,
        serde_json::to_vec(&value).expect("receipt is JSON"),
    )
    .expect("native test receipt can be written");
    fs::rename(&temporary, &destination).expect("native test receipt can be published");
}
