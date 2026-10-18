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
    let target = env::var("TARGET").expect("Cargo provides TARGET to build scripts");
    let host = env::var("HOST").expect("Cargo provides HOST to build scripts");
    let build_kind = if host == target { "HOST" } else { "TARGET" };
    let cmake_target_suffix = target.replace('-', "_");
    let cc_target_suffix = target.replace(['-', '.'], "_");

    // Both cmake-rs and cc-rs use PATH to discover their tools; cc-rs omits it
    // from Cargo's rerun metadata even though it can change the selected tools.
    println!("cargo:rerun-if-env-changed=PATH");
    for variable in [
        "CMAKE",
        "CMAKE_GENERATOR",
        "CMAKE_PREFIX_PATH",
        "CMAKE_TOOLCHAIN_FILE",
    ] {
        watch_target_env(variable, &target, &cmake_target_suffix, build_kind);
    }
    for variable in [
        "CMAKE_GENERATOR_INSTANCE",
        "CMAKE_GENERATOR_PLATFORM",
        "CMAKE_GENERATOR_TOOLSET",
    ] {
        println!("cargo:rerun-if-env-changed={variable}");
    }
    if env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("msvc") {
        for variable in [
            "INCLUDE",
            "LIB",
            "ProgramFiles",
            "VCINSTALLDIR",
            "VCToolsVersion",
            "VSCMD_ARG_TGT_ARCH",
            "VSINSTALLDIR",
            "VSTEL_MSBuildProjectFullPath",
            "WindowsSdkDir",
        ] {
            println!("cargo:rerun-if-env-changed={variable}");
        }
    }
    if env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        for variable in ["MACOSX_DEPLOYMENT_TARGET", "SDKROOT"] {
            println!("cargo:rerun-if-env-changed={variable}");
        }
    }

    for variable in ["CC", "CXX", "CFLAGS", "CXXFLAGS"] {
        watch_target_env(variable, &target, &cc_target_suffix, build_kind);
    }
    for variable in [
        "CC_FORCE_DISABLE",
        "CC_KNOWN_WRAPPER_CUSTOM",
        "CC_SHELL_ESCAPED_FLAGS",
        "CRATE_CC_NO_DEFAULTS",
        "CROSS_COMPILE",
    ] {
        println!("cargo:rerun-if-env-changed={variable}");
    }

    // cmake-rs disables cc-rs Cargo metadata; Cargo must observe these inputs,
    // and CMake must admit them after discarding its previous compiler cache.
    config.configure_arg("--fresh");
}

fn watch_target_env(variable: &str, target: &str, suffix: &str, build_kind: &str) {
    println!("cargo:rerun-if-env-changed={variable}_{target}");
    println!("cargo:rerun-if-env-changed={variable}_{suffix}");
    println!("cargo:rerun-if-env-changed={build_kind}_{variable}");
    println!("cargo:rerun-if-env-changed={variable}");
}

pub fn add_file_prefix_map(config: &mut cmake::Config, mapping: impl std::fmt::Display) {
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
