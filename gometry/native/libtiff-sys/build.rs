use std::path::PathBuf;
use std::process::Command;
use std::{env, fs};

const LIBTIFF_VERSION: &str = "4.7.2";
const LIBTIFF_PATH: &str = "libtiff";
const LIBTIFF_COMMIT: &str = "d01a94be176f5f6a87f7ee1c0b32e65416aa2b4d";

fn main() {
    println!("cargo:rerun-if-changed=libtiff");
    println!("cargo:rerun-if-changed=libtiff.pin");

    let source = PathBuf::from(LIBTIFF_PATH);
    let cmake = source.join("CMakeLists.txt");
    let version = source.join("VERSION");
    let hint = "run `git submodule update --init gometry/native/libtiff-sys/libtiff`";
    if !cmake.is_file() || !version.is_file() {
        panic!("libtiff source is not initialized (missing CMakeLists.txt or VERSION); {hint}");
    }
    let expected_pin = format!(
        "url = https://gitlab.com/libtiff/libtiff.git\ncommit = d01a94be176f5f6a87f7ee1c0b32e65416aa2b4d\ntag = v{LIBTIFF_VERSION}\nversion = {LIBTIFF_VERSION}\n"
    );
    let pin = fs::read_to_string("libtiff.pin")
        .unwrap_or_else(|error| panic!("cannot read libtiff.pin: {error}"));
    if pin != expected_pin {
        panic!("libtiff.pin does not describe the pinned libtiff source");
    }
    let actual_version = fs::read_to_string(version)
        .unwrap_or_else(|error| panic!("cannot read libtiff VERSION: {error}"));
    if actual_version.trim() != LIBTIFF_VERSION {
        panic!(
            "unsupported libtiff VERSION {:?}; expected {LIBTIFF_VERSION}",
            actual_version.trim()
        );
    }
    let cmake_text = fs::read_to_string(cmake)
        .unwrap_or_else(|error| panic!("cannot read libtiff CMakeLists.txt: {error}"));
    if !cmake_text.contains("add_subdirectory(build)") {
        panic!("libtiff CMakeLists.txt is not the unmodified upstream build; {hint}");
    }
    if source.join(".git").exists() {
        let git = |args: &[&str]| {
            Command::new("git")
                .args(["-C", LIBTIFF_PATH])
                .args(args)
                .output()
                .unwrap_or_else(|error| panic!("cannot inspect libtiff submodule: {error}"))
        };
        let head = git(&["rev-parse", "HEAD"]);
        if !head.status.success() || String::from_utf8_lossy(&head.stdout).trim() != LIBTIFF_COMMIT
        {
            panic!("libtiff submodule is not pinned to {LIBTIFF_COMMIT}");
        }
        if !git(&["diff", "--exit-code"]).status.success()
            || !git(&["diff", "--cached", "--exit-code"]).status.success()
        {
            panic!("libtiff submodule has tracked changes; restore the pinned upstream tree");
        }
        let status = git(&["status", "--porcelain", "--untracked-files=all"]);
        if !status.status.success() || !status.stdout.is_empty() {
            panic!("libtiff submodule has untracked files; restore the pinned upstream tree");
        }
    }
    let zlib_root = PathBuf::from(
        env::var_os("DEP_Z_ROOT").expect("libz-sys must provide its bundled zlib root"),
    );
    let zlib_library = if env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("msvc") {
        zlib_root.join("lib/z.lib")
    } else {
        zlib_root.join("lib/libz.a")
    };
    let mut config = cmake::Config::new("libtiff");
    config
        .profile("Release")
        .define("BUILD_SHARED_LIBS", "OFF")
        .define("CMAKE_POSITION_INDEPENDENT_CODE", "ON")
        .define("tiff-tools", "OFF")
        .define("tiff-tests", "OFF")
        .define("tiff-contrib", "OFF")
        .define("tiff-docs", "OFF")
        .define("tiff-deprecated", "OFF")
        .define("tiff-cxx", "OFF")
        .define("zlib", "ON")
        .define("deflate", "OFF")
        .define("jpeg", "OFF")
        .define("jbig", "OFF")
        .define("lerc", "OFF")
        .define("lzma", "OFF")
        .define("zstd", "OFF")
        .define("webp", "OFF")
        .define("ZLIB_ROOT", &zlib_root)
        .define("ZLIB_LIBRARY", &zlib_library)
        .define("ZLIB_INCLUDE_DIR", zlib_root.join("include"));
    if env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux") {
        config.define("CMAKE_REQUIRED_LIBRARIES", "m");
    }

    let dst = config.build();
    let lib_dir = if dst.join("lib/libtiff.a").exists() || dst.join("lib/tiff.lib").exists() {
        dst.join("lib")
    } else {
        dst.join("lib64")
    };
    let library = if env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("msvc") {
        lib_dir.join("tiff.lib")
    } else {
        lib_dir.join("libtiff.a")
    };

    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=static=tiff");
    println!("cargo:include={}", dst.join("include").display());
    println!("cargo:library={}", library.display());
    println!("cargo:z_library={}", zlib_library.display());
}
