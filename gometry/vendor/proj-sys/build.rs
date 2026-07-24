use std::env;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use tar::Archive;

const MINIMUM_PROJ_VERSION: &str = "9.8.1";

#[cfg(feature = "nobuild")]
fn main() {} // Skip the build script on docs.rs

#[cfg(not(feature = "nobuild"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let include_path = build_from_source()?;

    #[cfg(feature = "buildtime_bindgen")]
    generate_bindings(include_path)?;
    #[cfg(not(feature = "buildtime_bindgen"))]
    let _ = include_path;

    Ok(())
}

#[cfg(feature = "buildtime_bindgen")]
fn generate_bindings(include_path: std::path::PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    // If you update the configuration here you also
    // need to update the corresponding bindgen command in
    // `DEVELOPMENT.md`
    let bindings = bindgen::Builder::default()
        .clang_arg(format!("-I{}", include_path.to_string_lossy()))
        .size_t_is_usize(true)
        .blocklist_type("max_align_t")
        // The input header we would like to generate
        // bindings for.
        .header("wrapper.h")
        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings.write_to_file(out_path.join("bindings.rs"))?;

    Ok(())
}

// returns the path of "include" for the built proj
fn build_from_source() -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    eprintln!("building libproj from source");
    println!("cargo:rustc-cfg=bundled_build");
    if let Ok(val) = &env::var("_PROJ_SYS_TEST_EXPECT_BUILD_FROM_SRC") {
        if val == "0" {
            panic!(
                "for testing purposes: package was building from source but should not have been"
            );
        }
    }

    let path = format!("PROJSRC/proj-{MINIMUM_PROJ_VERSION}.tar.gz");
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    let tar_gz = File::open(path)?;
    let tar = GzDecoder::new(tar_gz);
    let mut archive = Archive::new(tar);
    archive.unpack(out_path.join("PROJSRC/proj"))?;
    let source_path = out_path.join(format!("PROJSRC/proj/proj-{MINIMUM_PROJ_VERSION}"));
    patch_bundled_geodesic(&source_path)?;
    let mut config = cmake::Config::new(source_path);
    config.define("BUILD_SHARED_LIBS", "OFF");
    config.define("BUILD_TESTING", "OFF");
    config.define("BUILD_CCT", "OFF");
    config.define("BUILD_CS2CS", "OFF");
    config.define("BUILD_GEOD", "OFF");
    config.define("BUILD_GIE", "OFF");
    config.define("BUILD_PROJ", "OFF");
    config.define("BUILD_PROJINFO", "OFF");
    config.define("BUILD_PROJSYNC", "OFF");
    config.define("ENABLE_CURL", "OFF");
    // PROJ resources are compiled into this static build. Embedding the CMake
    // install data path as a fallback leaks the build host's absolute target
    // directory into every wheel and is both unnecessary and unreproducible.
    config.define("EMBED_PROJ_DATA_PATH", "OFF");

    // we check here whether or not these variables are set by cargo
    // if they are set, `libsqlite3-sys` was built with the bundled feature
    // enabled, which in turn allows us to rely on the built libsqlite3 version
    // and link it statically
    //
    // If these are not set, it's necessary that libsqlite3 exists on the build system
    // in a location accessible by cmake
    if let Ok(sqlite_include) = std::env::var("DEP_SQLITE3_INCLUDE") {
        config.define("SQLITE3_INCLUDE_DIR", sqlite_include);
    }
    if let Ok(sqlite_lib_dir) = std::env::var("DEP_SQLITE3_LIB_DIR") {
        config.define("SQLITE3_LIBRARY", format!("{sqlite_lib_dir}/libsqlite3.a",));
    }

    if cfg!(feature = "tiff") {
        eprintln!("enabling tiff support");
        config.define("ENABLE_TIFF", "ON");
    } else {
        eprintln!("disabling tiff support");
        config.define("ENABLE_TIFF", "OFF");
    }

    if cfg!(target_env = "msvc") {
        // rust links the release MVSC runtime
        // also for debug builds. If we let
        // cmake choose debug/release builds
        // based on the underlying cargo build
        // version that results in linker errors
        config.profile("Release");
    }

    let proj = config.build();
    // Tell cargo to tell rustc to link libproj, and where to find it
    // libproj will be built in $OUT_DIR/lib

    //proj likes to create proj_d when configured as debug and on MSVC, so link to that one if it exists
    if proj.join("lib").join("proj_d.lib").exists() {
        println!("cargo:rustc-link-lib=static=proj_d");
    } else {
        println!("cargo:rustc-link-lib=static=proj");
    }
    println!(
        "cargo:rustc-link-search=native={}",
        proj.join("lib").display()
    );

    // This is producing a warning - this directory doesn't exist (on aarch64 anyway)
    println!(
        "cargo:rustc-link-search={}",
        &out_path.join("lib64").display()
    );
    println!(
        "cargo:rustc-link-search={}",
        &out_path.join("build/lib").display()
    );

    if cfg!(feature = "tiff") {
        // On platforms like apples aarch64, users are likely to have installed libtiff with homebrew,
        // which isn't in the default search path, so try to determine path from pkg-config
        match pkg_config::Config::new()
            .atleast_version("4.0")
            .probe("libtiff-4")
        {
            Ok(pk) => {
                eprintln!(
                    "found acceptable libtiff installed at: {:?}",
                    pk.link_paths[0]
                );
                println!("cargo:rustc-link-search=native={:?}", pk.link_paths[0]);
            },
            Err(err) => {
                // pkg-config might not even be installed. Let's try to stumble forward
                // to see if the build succeeds regardless, e.g. if libtiff is installed
                // in some default search path.
                eprintln!("Failed to find libtiff with pkg-config: {err}");
            },
        }
        println!("cargo:rustc-link-lib=dylib=tiff");
    }

    Ok(proj.join("include"))
}

/// Keep bundled PROJ's antipodal astroid solver on Gometry's fixed x86-64-v2
/// baseline. Its C `cbrt` reference otherwise binds to Rust compiler-builtins
/// during static linking, pulling in runtime FMA/FMA4 dispatch. A `pow` seed
/// plus two Newton steps is sufficient for this internal seed; the stable
/// astroid reconstruction remains unchanged. Exact marker counts make an
/// upstream source drift fail the build rather than silently skip the patch.
fn patch_bundled_geodesic(source_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    const ASTROID_HEADER: &str = "double Astroid(double x, double y) {";
    const ASTROID_PATCH: &str = r#"static double cbrt_unfused(double value) {
  if (value == 0 || !isfinite(value))
    return value;
  double magnitude = fabs(value);
  double root = pow(magnitude, 1.0 / 3.0);
  root = (2.0 * root + magnitude / (root * root)) / 3.0;
  root = (2.0 * root + magnitude / (root * root)) / 3.0;
  return copysign(root, value);
}

double Astroid(double x, double y) {"#;
    const CBRT_CALL: &str = "T = cbrt(T3);            /* T = r * t */";
    const CBRT_PATCH: &str = "T = cbrt_unfused(T3);    /* T = r * t */";

    let geodesic_path = source_path.join("src/geodesic.c");
    let source = fs::read_to_string(&geodesic_path)?;
    if source.matches(ASTROID_HEADER).count() != 1 || source.matches(CBRT_CALL).count() != 1 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "bundled PROJ geodesic.c no longer matches the x86-64-v2 cbrt patch",
        )
        .into());
    }
    let patched = source
        .replacen(ASTROID_HEADER, ASTROID_PATCH, 1)
        .replacen(CBRT_CALL, CBRT_PATCH, 1);
    fs::write(geodesic_path, patched)?;
    Ok(())
}
