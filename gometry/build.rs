use std::path::{Path, PathBuf};
use std::{env, fs};

use sha2::{Digest as _, Sha256};

#[path = "native/digest_encoding.rs"]
mod digest_encoding;

const SOURCE_INPUTS_FORMAT: &str = "sha256-path-nul-content-nul-v1";
// The requested PEP 517 profile is not a Cargo-observed fact. Cargo reports
// `PROFILE=release` for release-dist because it inherits the release profile.
const RELEASE_ENV_VARS: &[&str] = &[
    "GOMETRY_RELEASE_TARGET",
    "GOMETRY_RELEASE_TARGET_POLICY",
    "GOMETRY_RELEASE_RUSTFLAGS",
    "GOMETRY_RELEASE_CFLAGS",
    "GOMETRY_RELEASE_CXXFLAGS",
];

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(
        env::var_os("CARGO_MANIFEST_DIR")
            .ok_or_else(|| std::io::Error::other("Cargo did not provide CARGO_MANIFEST_DIR"))?,
    );
    let (source_inputs_sha256, native_source_inputs_sha256) = source_input_digests(&manifest_dir)?;
    let profile = env::var("PROFILE").unwrap_or_else(|_| "unknown".to_owned());
    // Keep only Cargo's coarse profile family; release-dist cannot be observed
    // here and its artifact consequence is checked by the wheel verifier.
    let profile_family = match profile.as_str() {
        "release" | "release-dist" => "release".to_owned(),
        profile => profile.to_owned(),
    };
    let opt_level = env::var("OPT_LEVEL").unwrap_or_else(|_| "unknown".to_owned());
    // Cargo exports a set flag cfg such as `debug_assertions` with an empty
    // value, so presence alone carries the answer.
    let debug_assertions = env::var_os("CARGO_CFG_DEBUG_ASSERTIONS").is_some();
    let target = env::var("TARGET").unwrap_or_else(|_| "unknown".to_owned());
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_else(|_| "unknown".to_owned());
    let target_features = {
        let value = env::var("CARGO_CFG_TARGET_FEATURE").unwrap_or_default();
        let mut features: Vec<_> = value
            .split(',')
            .filter(|feature| !feature.is_empty())
            .collect();
        features.sort_unstable();
        features.join(",")
    };
    validate_release_identity(&target, &profile_family, &opt_level, debug_assertions)?;

    // Exactly the digested inputs: Cargo's own fingerprints already rebuild
    // on dependency and profile changes, so watching the workspace lock only
    // re-ran this script whenever a sibling package touched it.
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=native");
    for variable in RELEASE_ENV_VARS {
        println!("cargo:rerun-if-env-changed={variable}");
    }
    for prefix in ["CFLAGS", "CXXFLAGS"] {
        println!(
            "cargo:rerun-if-env-changed={}",
            target_flag_variable(prefix, &target)
        );
    }
    println!("cargo::rustc-env=GOMETRY_BUILD_SOURCE_INPUTS_FORMAT={SOURCE_INPUTS_FORMAT}");
    println!("cargo::rustc-env=GOMETRY_BUILD_SOURCE_INPUTS_SHA256={source_inputs_sha256}");
    println!("cargo::rustc-env=GOMETRY_BUILD_NATIVE_SOURCE_INPUTS_FORMAT={SOURCE_INPUTS_FORMAT}");
    println!(
        "cargo::rustc-env=GOMETRY_BUILD_NATIVE_SOURCE_INPUTS_SHA256={native_source_inputs_sha256}"
    );
    println!("cargo::rustc-env=GOMETRY_BUILD_PROFILE_FAMILY={profile_family}");
    println!("cargo::rustc-env=GOMETRY_BUILD_OPT_LEVEL={opt_level}");
    println!("cargo::rustc-env=GOMETRY_BUILD_DEBUG_ASSERTIONS={debug_assertions}");
    println!("cargo::rustc-env=GOMETRY_BUILD_TARGET={target}");
    println!("cargo::rustc-env=GOMETRY_BUILD_TARGET_ARCH={target_arch}");
    println!("cargo::rustc-env=GOMETRY_BUILD_TARGET_FEATURES={target_features}");

    pyo3_build_config::use_pyo3_cfgs();
    if env::var_os("CARGO_CFG_WINDOWS").is_some() {
        println!("cargo::rustc-link-lib=ole32");
        println!("cargo::rustc-link-lib=shell32");
    }
    Ok(())
}

fn validate_release_identity(
    target: &str,
    profile_family: &str,
    opt_level: &str,
    debug_assertions: bool,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let present = RELEASE_ENV_VARS
        .iter()
        .filter(|variable| env::var_os(variable).is_some())
        .count();
    if present == 0 {
        return Ok(());
    }
    if present != RELEASE_ENV_VARS.len() {
        return Err(std::io::Error::other(format!(
            "release identity environment is partial: {present}/{} variables are set",
            RELEASE_ENV_VARS.len()
        ))
        .into());
    }
    let release_target = env::var("GOMETRY_RELEASE_TARGET").map_err(|error| {
        std::io::Error::other(format!("cannot read GOMETRY_RELEASE_TARGET: {error}"))
    })?;
    if release_target != target {
        return Err(std::io::Error::other(format!(
            "release target disagrees with Cargo TARGET: {release_target:?} != {target:?}"
        ))
        .into());
    }
    let policy = env::var("GOMETRY_RELEASE_TARGET_POLICY").map_err(|error| {
        std::io::Error::other(format!(
            "cannot read GOMETRY_RELEASE_TARGET_POLICY: {error}"
        ))
    })?;
    if policy.is_empty() || policy == "unconfigured" {
        return Err(std::io::Error::other("release target policy is empty or unconfigured").into());
    }
    if profile_family != "release" || opt_level != "3" || debug_assertions {
        return Err(std::io::Error::other(format!(
            "release identity does not describe an optimized release: \
             profile={profile_family:?}, opt-level={opt_level:?}, \
             debug-assertions={debug_assertions}"
        ))
        .into());
    }
    validate_release_compiler_flags(target)?;
    Ok(())
}

fn validate_release_compiler_flags(
    target: &str,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    // tools/_artifact_common.py maps targets to flags; verify Cargo received
    // its exported values without duplicating that policy table here.
    let policy_rustflags = env::var("GOMETRY_RELEASE_RUSTFLAGS")
        .map_err(|error| {
            std::io::Error::other(format!("cannot read GOMETRY_RELEASE_RUSTFLAGS: {error}"))
        })?
        .split_whitespace()
        .collect::<Vec<_>>()
        .join("\u{1f}");
    let actual_flag_variables = [
        (
            "GOMETRY_RELEASE_CFLAGS",
            target_flag_variable("CFLAGS", target),
        ),
        (
            "GOMETRY_RELEASE_CXXFLAGS",
            target_flag_variable("CXXFLAGS", target),
        ),
    ];
    let actual_rustflags = env::var("CARGO_ENCODED_RUSTFLAGS").unwrap_or_default();
    if actual_rustflags != policy_rustflags {
        return Err(std::io::Error::other(
            "Cargo rust flags do not match GOMETRY_RELEASE_RUSTFLAGS",
        )
        .into());
    }
    for (policy_variable, actual_variable) in actual_flag_variables {
        let policy_flags = env::var(policy_variable).map_err(|error| {
            std::io::Error::other(format!("cannot read {policy_variable}: {error}"))
        })?;
        let actual_flags = env::var(&actual_variable).unwrap_or_default();
        if actual_flags != policy_flags {
            return Err(std::io::Error::other(format!(
                "{actual_variable} does not match exported release flags {policy_variable}"
            ))
            .into());
        }
    }
    Ok(())
}

fn target_flag_variable(prefix: &str, target: &str) -> String {
    format!("{prefix}_{}", target.replace('-', "_"))
}

fn source_input_digests(manifest_dir: &Path) -> std::io::Result<(String, String)> {
    // The compiler is identified by the release manifest's `rustc --version`,
    // not by the rolling monorepo toolchain channel.
    let mut files = vec![("gometry/build.rs".to_owned(), manifest_dir.join("build.rs"))];
    // Maturin normalizes Cargo manifests and pyproject.toml, omits the
    // checkout-only Cargo config, and prunes unrelated workspace packages from
    // Cargo.lock when it builds from an sdist. Hash only inputs preserved
    // byte-for-byte across both checkout and sdist builds; dependency
    // resolution is checked separately against the archived lock.
    let mut source_files = Vec::new();
    collect_files(
        &manifest_dir.join("src"),
        "gometry/src",
        &mut source_files,
        false,
    )?;
    let mut native_files = Vec::new();
    collect_files(
        &manifest_dir.join("native"),
        "gometry/native",
        &mut native_files,
        true,
    )?;
    files.extend(source_files);
    files.extend(native_files.clone());
    Ok((digest_files(&files)?, digest_files(&native_files)?))
}

fn digest_files(files: &[(String, PathBuf)]) -> std::io::Result<String> {
    let mut files = files.to_vec();
    files.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    let mut digest = Sha256::new();
    for (logical, path) in files {
        let payload = fs::read(&path).map_err(|error| {
            std::io::Error::new(
                error.kind(),
                format!("cannot read build input {}: {error}", path.display()),
            )
        })?;
        digest.update(logical.as_bytes());
        digest.update([0]);
        digest.update(&payload);
        digest.update([0]);
    }
    Ok(digest_encoding::hex_lower(&digest.finalize()).to_string())
}

fn collect_files(
    root: &Path,
    logical_root: &str,
    files: &mut Vec<(String, PathBuf)>,
    skip_hidden_native: bool,
) -> std::io::Result<()> {
    let mut entries = fs::read_dir(root)
        .map_err(|error| {
            std::io::Error::new(
                error.kind(),
                format!(
                    "cannot read build input directory {}: {error}",
                    root.display()
                ),
            )
        })?
        .map(|entry| {
            entry.map_err(|error| {
                std::io::Error::new(
                    error.kind(),
                    format!(
                        "cannot read build input directory entry under {}: {error}",
                        root.display()
                    ),
                )
            })
        })
        .collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_unstable_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let path = entry.path();
        if path.file_name().is_some_and(|name| name == "Cargo.toml") {
            continue;
        }
        let logical = format!("{logical_root}/{}", entry.file_name().to_string_lossy());
        if skip_hidden_native
            && logical
                .split('/')
                .skip(2)
                .any(|component| component.starts_with('.'))
        {
            continue;
        }
        if path.is_dir() {
            collect_files(&path, &logical, files, skip_hidden_native)?;
        } else if path.is_file() {
            files.push((logical, path));
        }
    }
    Ok(())
}
