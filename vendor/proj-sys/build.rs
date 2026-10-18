#[cfg(not(feature = "nobuild"))]
#[path = "../../patches/proj/apply.rs"]
mod proj_patches;

#[cfg(not(feature = "nobuild"))]
use std::env;
#[cfg(not(feature = "nobuild"))]
use std::fs::{self, File};
#[cfg(not(feature = "nobuild"))]
use std::path::{Path, PathBuf};

#[cfg(not(feature = "nobuild"))]
use flate2::read::GzDecoder;
#[cfg(not(feature = "nobuild"))]
use proj_patches::MINIMUM_PROJ_VERSION;
#[cfg(not(feature = "nobuild"))]
use tar::Archive;

#[cfg(not(feature = "nobuild"))]
const PROJ_ARCHIVE_SIZE: usize = 6_048_023;
#[cfg(not(feature = "nobuild"))]
const PROJ_ARCHIVE_FNV1A64: u64 = 0xCB0ADD31BCB48CC1;
#[cfg(not(feature = "nobuild"))]
const PROJ_SOURCE_OVERRIDES: [(&str, &str); 3] = [
    ("aea.cpp", "src/projections/aea.cpp"),
    ("merc.cpp", "src/projections/merc.cpp"),
    ("gometry_proj_context.cpp", "src/gometry_proj_context.cpp"),
];

#[cfg(feature = "nobuild")]
fn main() {} // Skip the build script on docs.rs

#[cfg(not(feature = "nobuild"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    let workspace_root = PathBuf::from(
        env::var_os("CARGO_MANIFEST_DIR").expect("Cargo provides CARGO_MANIFEST_DIR"),
    )
    .join("../..")
    .canonicalize()?;
    let proj_archive =
        workspace_root.join(format!("vendor/proj/proj-{MINIMUM_PROJ_VERSION}.tar.gz"));
    let patch_dir = workspace_root.join(format!("patches/proj/{MINIMUM_PROJ_VERSION}"));
    println!("cargo:rerun-if-changed={}", proj_archive.display());
    println!(
        "cargo:rerun-if-changed={}",
        workspace_root.join("patches/proj/apply.rs").display()
    );
    for &(source, _) in &PROJ_SOURCE_OVERRIDES {
        println!(
            "cargo:rerun-if-changed={}",
            patch_dir.join(source).display()
        );
    }
    let include_path = if cfg!(feature = "bundled_proj") {
        eprintln!("feature flags specified source build");
        build_from_source(&proj_archive, &patch_dir)?
    } else {
        pkg_config::Config::new()
            .atleast_version(MINIMUM_PROJ_VERSION)
            .probe("proj")
            .map(|pk| {
                eprintln!(
                    "found acceptable libproj already installed at: {:?}",
                    pk.link_paths
                );
                if cfg!(feature = "network") {
                    // Generally, system proj installations have been built with tiff support
                    // allowing for network grid interaction. If this proves to be untrue
                    // could we try to determine some kind of runtime check and fall back
                    // to building from source?
                    eprintln!(
                        "assuming existing system libproj installation has network (tiff) support"
                    );
                }
                if let Ok(val) = &env::var("_PROJ_SYS_TEST_EXPECT_BUILD_FROM_SRC")
                    && val != "0"
                {
                    panic!(
                        "for testing purposes: existing package was found, but should not have been"
                    );
                }

                // Tell cargo to tell rustc to link the system proj
                // shared library.
                for path in &pk.link_paths {
                    println!("cargo:rustc-link-search=native={}", path.display());
                }
                println!("cargo:rustc-link-lib=proj");

                pk.include_paths
                    .into_iter()
                    .next()
                    .expect("pkg-config reported a PROJ include directory")
            })
            .or_else(|err| {
                eprintln!("pkg-config unable to find existing libproj installation: {err}");
                build_from_source(&proj_archive, &patch_dir)
            })?
    };

    #[cfg(feature = "buildtime_bindgen")]
    generate_bindings(include_path)?;
    #[cfg(not(feature = "buildtime_bindgen"))]
    drop(include_path);

    Ok(())
}

#[cfg(feature = "buildtime_bindgen")]
fn generate_bindings(include_path: std::path::PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    // If you update the configuration here you also
    // need to update the corresponding bindgen command in
    // `gometry/AGENTS.md`
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
#[cfg(not(feature = "nobuild"))]
fn build_from_source(
    archive_path: &Path,
    patch_dir: &Path,
) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    eprintln!("building libproj from source");
    println!("cargo:rustc-cfg=bundled_build");
    if let Ok(val) = &env::var("_PROJ_SYS_TEST_EXPECT_BUILD_FROM_SRC")
        && val == "0"
    {
        panic!("for testing purposes: package was building from source but should not have been");
    }

    verify_pinned_proj_archive(archive_path)?;
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    let tar_gz = File::open(archive_path)?;
    let tar = GzDecoder::new(tar_gz);
    let mut archive = Archive::new(tar);
    let source_root = out_path.join("PROJSRC/proj");
    if source_root.exists() {
        fs::remove_dir_all(&source_root)?;
    }
    archive.unpack(&source_root)?;
    let source_dir = source_root.join(format!("proj-{MINIMUM_PROJ_VERSION}"));
    for &(source, destination) in &PROJ_SOURCE_OVERRIDES {
        let patch_path = patch_dir.join(source);
        if !patch_path.is_file() {
            return Err(format!(
                "bundled PROJ {MINIMUM_PROJ_VERSION} requires a reviewed source override at {}",
                patch_path.display()
            )
            .into());
        }
        fs::copy(patch_path, source_dir.join(destination))?;
    }
    proj_patches::apply_pinned_semantic_patches(&source_dir)?;
    let source_list = source_dir.join("src/lib_proj.cmake");
    let source_list_contents = fs::read_to_string(&source_list)?;
    let source_list_marker = "  ctx.cpp\n";
    let source_list_replacement = "  ctx.cpp\n  gometry_proj_context.cpp\n";
    if source_list_contents.matches(source_list_marker).count() != 1 {
        return Err(format!(
            "bundled PROJ {MINIMUM_PROJ_VERSION} source list has an unexpected ctx.cpp entry"
        )
        .into());
    }
    fs::write(
        source_list,
        source_list_contents.replacen(source_list_marker, source_list_replacement, 1),
    )?;
    let mut config = cmake::Config::new(source_dir);
    cmake_flags::configure_fresh_build(&mut config);
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
    // CMake invokes the native compilers itself, so pass Cargo's canonical
    // source/build remaps through to the C and C++ objects it produces.
    configure_path_remapping(&mut config);
    // The static build embeds PROJ's resources. Do not retain the temporary
    // install prefix as a dead filesystem fallback in the extension.
    config.define("EMBED_PROJ_DATA_PATH", "OFF");

    // we check here whether or not these variables are set by cargo
    // if they are set, `libsqlite3-sys` was built with the bundled feature
    // enabled, which in turn allows us to rely on the built libsqlite3 version
    // and link it statically
    //
    // If these are not set, it's necessary that libsqlite3 exists on the build system
    // in a location accessible by cmake
    if let Ok(sqlite_include) = std::env::var("DEP_SQLITE3_INCLUDE") {
        config.define("SQLite3_INCLUDE_DIR", sqlite_include);
    }
    if let Ok(sqlite_lib_dir) = std::env::var("DEP_SQLITE3_LIB_DIR") {
        let sqlite_library = if env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("msvc") {
            "sqlite3.lib"
        } else {
            "libsqlite3.a"
        };
        config.define(
            "SQLite3_LIBRARY",
            PathBuf::from(sqlite_lib_dir).join(sqlite_library),
        );
    }

    if cfg!(feature = "tiff") {
        eprintln!("enabling statically linked tiff support");
        config.define("ENABLE_TIFF", "ON");
        config.define(
            "TIFF_INCLUDE_DIR",
            std::env::var("DEP_TIFF_INCLUDE").expect("bundled libtiff include path"),
        );
        config.define(
            "TIFF_LIBRARY",
            std::env::var("DEP_TIFF_LIBRARY").expect("bundled libtiff archive path"),
        );
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
    cmake_flags::configure_release_native_flags(&mut config)?;

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
        out_path.join("lib64").display()
    );
    println!(
        "cargo:rustc-link-search={}",
        out_path.join("build/lib").display()
    );

    if cfg!(feature = "tiff") {
        let tiff =
            PathBuf::from(std::env::var("DEP_TIFF_LIBRARY").expect("bundled libtiff archive path"));
        let zlib =
            PathBuf::from(std::env::var("DEP_TIFF_Z_LIBRARY").expect("bundled zlib archive path"));
        println!(
            "cargo:rustc-link-search=native={}",
            tiff.parent().expect("libtiff archive directory").display()
        );
        println!("cargo:rustc-link-lib=static=tiff");
        println!(
            "cargo:rustc-link-search=native={}",
            zlib.parent().expect("zlib archive directory").display()
        );
        println!("cargo:rustc-link-lib=static=z");
    }

    Ok(proj.join("include"))
}

#[cfg(not(feature = "nobuild"))]
fn verify_pinned_proj_archive(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let bytes = fs::read(path)?;
    if bytes.len() != PROJ_ARCHIVE_SIZE {
        return Err(format!(
            "bundled PROJ {MINIMUM_PROJ_VERSION} archive size changed: expected {}, got {}",
            PROJ_ARCHIVE_SIZE,
            bytes.len()
        )
        .into());
    }
    let hash = bytes.iter().fold(0xCBF29CE484222325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001B3)
    });
    if hash != PROJ_ARCHIVE_FNV1A64 {
        return Err(format!(
            "bundled PROJ {MINIMUM_PROJ_VERSION} archive hash changed: expected {PROJ_ARCHIVE_FNV1A64:016x}, got {hash:016x}"
        )
        .into());
    }
    Ok(())
}

#[cfg(not(feature = "nobuild"))]
fn configure_path_remapping(config: &mut cmake::Config) {
    cmake_flags::configure_path_remapping(config);
    if env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("msvc") {
        return;
    }
    if let Some(sqlite_include) = env::var_os("DEP_SQLITE3_INCLUDE") {
        cmake_flags::add_file_prefix_map(
            config,
            format!(
                "{}=/cargo/libsqlite3",
                PathBuf::from(sqlite_include).display()
            ),
        );
    }
}
