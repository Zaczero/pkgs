//! Compiler flags shared by the workspace's CMake-built native dependencies.

use std::env;
use std::error::Error;

/// Give every object CMake compiles the canonical paths Cargo gives rustc.
///
/// Under `trim-paths`, Cargo hands build scripts the source and build
/// directory remaps it applies to rustc. C and C++ objects embed `__FILE__`
/// and debug paths just as Rust objects do, so with the same mapping the
/// archives two checkouts produce are identical, and the compiler cache can
/// share them.
pub fn configure_path_remapping(config: &mut cmake::Config) {
    if env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("msvc") {
        return;
    }
    if let Some(remap) = env::var_os("CARGO_TRIM_PATHS_REMAP") {
        for pair in env::split_paths(&remap).filter(|pair| !pair.as_os_str().is_empty()) {
            add_file_prefix_map(config, pair.display());
        }
    }
}

/// Reconfigure from the current compiler and native build inputs.
pub fn configure_fresh_build(config: &mut cmake::Config) {
    // cmake-rs records its CMake and compiler environment reads as build inputs.
    // Clear CMake's cache as well so a rerun uses the newly selected tools.
    config.configure_arg("--fresh");
}

pub fn add_file_prefix_map<M: std::fmt::Display>(config: &mut cmake::Config, mapping: M) {
    let flag = format!("-ffile-prefix-map={mapping}");
    config.cflag(&flag).cxxflag(&flag);
}

/// Apply the release flags the workspace declares through
/// `GOMETRY_RELEASE_CFLAGS` and `GOMETRY_RELEASE_CXXFLAGS`, unless the caller
/// set the ambient variables cmake-rs reads itself.
pub fn configure_release_native_flags(config: &mut cmake::Config) -> Result<(), Box<dyn Error>> {
    if let Some(flags) = release_native_flags("CFLAGS")?
        && env::var_os("CFLAGS").is_none()
        && !flags.is_empty()
    {
        config.cflag(&flags);
    }
    if let Some(flags) = release_native_flags("CXXFLAGS")?
        && env::var_os("CXXFLAGS").is_none()
        && !flags.is_empty()
    {
        config.cxxflag(&flags);
    }
    Ok(())
}

fn release_native_flags(name: &str) -> Result<Option<String>, Box<dyn Error>> {
    let declared_name = format!("GOMETRY_RELEASE_{name}");
    println!("cargo:rerun-if-env-changed={declared_name}");
    let target_suffix = env::var("TARGET").unwrap_or_default().replace('-', "_");
    let target_name = format!("{name}_{target_suffix}");
    let Some(declared) = env::var_os(&declared_name) else {
        return Ok(None);
    };
    let declared = declared.to_string_lossy().into_owned();
    for ambient_name in [name.to_owned(), target_name] {
        if let Some(ambient) = env::var_os(&ambient_name) {
            let ambient = ambient.to_string_lossy();
            if ambient != declared {
                return Err(format!(
                    "release native flags disagree: {declared_name}={declared:?}, \
                     {ambient_name}={ambient:?}"
                )
                .into());
            }
        }
    }
    Ok(Some(declared))
}
