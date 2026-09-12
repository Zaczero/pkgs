#![allow(
    clippy::panic,
    reason = "a build script reports a missing input by panicking"
)]
use std::path::{Path, PathBuf};
use std::{env, fs};

use sha2::{Digest as _, Sha256};

fn main() {
    let manifest_dir = PathBuf::from(
        env::var_os("CARGO_MANIFEST_DIR").expect("Cargo did not provide CARGO_MANIFEST_DIR"),
    );
    let source_inputs_sha256 = source_inputs_sha256(&manifest_dir);
    let profile_family = env::var("PROFILE").unwrap_or_else(|_| "unknown".to_owned());
    let opt_level = env::var("OPT_LEVEL").unwrap_or_else(|_| "unknown".to_owned());
    let debug = env::var("DEBUG").unwrap_or_else(|_| "unknown".to_owned());
    let debug_assertions =
        env::var("CARGO_CFG_DEBUG_ASSERTIONS").is_ok_and(|value| !value.is_empty());
    let target = env::var("TARGET").unwrap_or_else(|_| "unknown".to_owned());
    let target_features = env::var("CARGO_CFG_TARGET_FEATURE")
        .map(|value| {
            let mut features: Vec<_> = value
                .split(',')
                .filter(|feature| !feature.is_empty())
                .collect();
            features.sort_unstable();
            features.join(",")
        })
        .unwrap_or_default();

    println!("cargo:rerun-if-changed=../Cargo.toml");
    println!("cargo:rerun-if-changed=../Cargo.lock");
    println!("cargo:rerun-if-changed=../rust-toolchain.toml");
    println!("cargo:rerun-if-changed=../.cargo/config.toml");
    println!("cargo:rerun-if-changed=../pyproject.toml");
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=pyproject.toml");
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=native");
    println!("cargo::rustc-env=GOMETRY_BUILD_SOURCE_INPUTS_SHA256={source_inputs_sha256}");
    println!("cargo::rustc-env=GOMETRY_BUILD_PROFILE_FAMILY={profile_family}");
    println!("cargo::rustc-env=GOMETRY_BUILD_OPT_LEVEL={opt_level}");
    println!("cargo::rustc-env=GOMETRY_BUILD_DEBUG={debug}");
    println!("cargo::rustc-env=GOMETRY_BUILD_DEBUG_ASSERTIONS={debug_assertions}");
    println!("cargo::rustc-env=GOMETRY_BUILD_TARGET={target}");
    println!("cargo::rustc-env=GOMETRY_BUILD_TARGET_FEATURES={target_features}");

    pyo3_build_config::use_pyo3_cfgs();
    if env::var_os("CARGO_CFG_WINDOWS").is_some() {
        println!("cargo::rustc-link-lib=ole32");
        println!("cargo::rustc-link-lib=shell32");
    }
}

fn source_inputs_sha256(manifest_dir: &Path) -> String {
    let workspace_dir = manifest_dir
        .parent()
        .expect("gometry manifest has no workspace directory");
    let mut files = Vec::new();
    for (logical, path) in [
        (
            "rust-toolchain.toml",
            workspace_dir.join("rust-toolchain.toml"),
        ),
        ("gometry/Cargo.toml", manifest_dir.join("Cargo.toml")),
        ("gometry/build.rs", manifest_dir.join("build.rs")),
    ] {
        files.push((logical.to_owned(), path));
    }
    // Maturin normalizes the workspace manifest and pyproject.toml, omits the
    // checkout-only Cargo config, and prunes unrelated workspace packages from
    // Cargo.lock when it builds from an sdist. Hash only inputs preserved
    // byte-for-byte across both checkout and sdist builds; dependency
    // resolution is checked separately against the archived lock.
    for directory in ["src", "native"] {
        collect_files(
            &manifest_dir.join(directory),
            &format!("gometry/{directory}"),
            &mut files,
        );
    }
    files.sort_unstable_by(|left, right| left.0.cmp(&right.0));

    let mut digest = Sha256::new();
    for (logical, path) in files {
        let payload = fs::read(&path)
            .unwrap_or_else(|error| panic!("cannot read build input {}: {error}", path.display()));
        digest.update(logical.as_bytes());
        digest.update([0]);
        digest.update(&payload);
        digest.update([0]);
    }
    format!("{:x}", digest.finalize())
}

fn collect_files(root: &Path, logical_root: &str, files: &mut Vec<(String, PathBuf)>) {
    let mut entries: Vec<_> = fs::read_dir(root)
        .unwrap_or_else(|error| {
            panic!(
                "cannot read build input directory {}: {error}",
                root.display()
            )
        })
        .map(|entry| entry.expect("cannot read build input directory entry"))
        .collect();
    entries.sort_unstable_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let path = entry.path();
        if entry.file_name() == ".git" {
            continue;
        }
        if path.is_dir() {
            collect_files(
                &path,
                &format!("{logical_root}/{}", entry.file_name().to_string_lossy()),
                files,
            );
        } else if path.is_file() {
            files.push((
                format!("{logical_root}/{}", entry.file_name().to_string_lossy()),
                path,
            ));
        }
    }
}
