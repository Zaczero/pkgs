#[cfg(not(feature = "nobuild"))]
#[path = "../cmake_flags.rs"]
mod cmake_flags;

use std::env;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use tar::Archive;

const MINIMUM_PROJ_VERSION: &str = "9.6.2";
const PROJ_ARCHIVE_SIZE: usize = 6_394_614;
const PROJ_ARCHIVE_FNV1A64: u64 = 0x0F8C94421730578E;
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
    println!("cargo:rerun-if-changed=PROJSRC/proj-9.6.2.tar.gz");
    for &(source, _) in &PROJ_SOURCE_OVERRIDES {
        println!("cargo:rerun-if-changed=PROJSRC/patches/proj-9.6.2/{source}");
    }
    let include_path = if cfg!(feature = "bundled_proj") {
        eprintln!("feature flags specified source build");
        build_from_source()?
    } else {
        pkg_config::Config::new()
        .atleast_version(MINIMUM_PROJ_VERSION)
        .probe("proj")
        .map(|pk| {
            eprintln!("found acceptable libproj already installed at: {:?}", pk.link_paths);
            if cfg!(feature = "network") {
                // Generally, system proj installations have been built with tiff support
                // allowing for network grid interaction. If this proves to be untrue
                // could we try to determine some kind of runtime check and fall back
                // to building from source?
                eprintln!("assuming existing system libproj installation has network (tiff) support");
            }
            if let Ok(val) = &env::var("_PROJ_SYS_TEST_EXPECT_BUILD_FROM_SRC") {
                if val != "0" {
                    panic!("for testing purposes: existing package was found, but should not have been");
                }
            }

            // Tell cargo to tell rustc to link the system proj
            // shared library.
            for path in &pk.link_paths {
                println!("cargo:rustc-link-search=native={}", path.display());
            }
            println!("cargo:rustc-link-lib=proj");

            pk.include_paths.into_iter().next().expect("pkg-config reported a PROJ include directory")
        })
        .or_else(|err| {
            eprintln!("pkg-config unable to find existing libproj installation: {err}");
            build_from_source()
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

    let path = PathBuf::from(format!("PROJSRC/proj-{MINIMUM_PROJ_VERSION}.tar.gz"));
    verify_pinned_proj_archive(&path)?;
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    let tar_gz = File::open(&path)?;
    let tar = GzDecoder::new(tar_gz);
    let mut archive = Archive::new(tar);
    let source_root = out_path.join("PROJSRC/proj");
    if source_root.exists() {
        fs::remove_dir_all(&source_root)?;
    }
    archive.unpack(&source_root)?;
    let source_dir = source_root.join(format!("proj-{MINIMUM_PROJ_VERSION}"));
    let patch_dir = PathBuf::from(format!("PROJSRC/patches/proj-{MINIMUM_PROJ_VERSION}"));
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
    apply_pinned_semantic_patches(&source_dir)?;
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
        config.define("SQLITE3_INCLUDE_DIR", sqlite_include);
    }
    if let Ok(sqlite_lib_dir) = std::env::var("DEP_SQLITE3_LIB_DIR") {
        config.define("SQLITE3_LIBRARY", format!("{sqlite_lib_dir}/libsqlite3.a",));
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

fn replace_pinned_source(
    source_dir: &Path,
    relative: &str,
    before: &str,
    after: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = source_dir.join(relative);
    let text = fs::read_to_string(&path)?;
    let count = text.matches(before).count();
    if count != 1 {
        return Err(format!(
            "bundled PROJ {MINIMUM_PROJ_VERSION} source anchor is not unique in {relative}: expected 1, got {count}"
        )
        .into());
    }
    fs::write(path, text.replacen(before, after, 1))?;
    Ok(())
}

fn replace_pinned_source_section(
    source_dir: &Path,
    relative: &str,
    start: &str,
    end: &str,
    replacement: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = source_dir.join(relative);
    let text = fs::read_to_string(&path)?;
    if text.matches(start).count() != 1 {
        return Err(format!(
            "bundled PROJ {MINIMUM_PROJ_VERSION} section start anchor is not unique in {relative}"
        )
        .into());
    }
    let start_index = text.find(start).expect("checked unique section start");
    let (prefix, suffix) = text.split_at(start_index);
    if suffix.matches(end).count() != 1 {
        return Err(format!(
            "bundled PROJ {MINIMUM_PROJ_VERSION} section end anchor is not unique after its start in {relative}"
        )
        .into());
    }
    let end_index = suffix.find(end).expect("checked unique section end");
    let (_, suffix) = suffix.split_at(end_index);
    fs::write(path, format!("{prefix}{replacement}{suffix}"))?;
    Ok(())
}

fn remove_pinned_source_section(
    source_dir: &Path,
    relative: &str,
    start: &str,
    end: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    replace_pinned_source_section(source_dir, relative, start, end, "")
}

/// Apply the exact-meaning CRS fixes to the pinned PROJ source.
///
/// These edits intentionally stay as small source anchors rather than copying
/// generated upstream files. A changed archive, version, or anchor fails the
/// build instead of silently producing a different CRS engine.
fn apply_pinned_semantic_patches(source_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    replace_pinned_source(
        source_dir,
        "src/conversions/unitconvert.cpp",
        "    double xy_factor; /* unit conversion factor for horizontal components */",
        "    double x_factor;\n    double y_factor; /* unit conversion factor for horizontal components */",
    )?;

    replace_pinned_source(
        source_dir,
        "src/proj_internal.h",
        "    bool forceOver = false;\n    int epsg_file_exists = -1;",
        "    bool forceOver = false;\n    bool gometryDefaultAngularUnits = false;\n    int epsg_file_exists = -1;",
    )?;
    replace_pinned_source(
        source_dir,
        "src/proj_internal.h",
        "    int iCurCoordOp = -1;",
        "    int iCurCoordOp = -1;\n    bool gometryIdentityPhase = false;\n    double gometryIdentityPhaseHigh = 0.0;\n    double gometryIdentityPhaseLow = 0.0;",
    )?;
    replace_pinned_source(
        source_dir,
        "src/ctx.cpp",
        "      forceOver(other.forceOver), epsg_file_exists(other.epsg_file_exists),",
        "      forceOver(other.forceOver),\n      gometryDefaultAngularUnits(other.gometryDefaultAngularUnits),\n      epsg_file_exists(other.epsg_file_exists),",
    )?;
    replace_pinned_source(
        source_dir,
        "src/proj_internal.h",
        "#ifndef DO_NOT_DEFINE_PROJ_HEAD",
        "extern \"C\" double gometry_proj_conventional_period(double) noexcept;\nextern \"C\" double gometry_adjust_longitude_rotation_degrees(double, double, double, double, double, int) noexcept;\nextern \"C\" void gometry_proj_configure_default_noop_phase(PJ_CONTEXT *, PJ *, const PJ *, const PJ *) noexcept;\n\n#ifndef DO_NOT_DEFINE_PROJ_HEAD",
    )?;
    replace_pinned_source(
        source_dir,
        "src/iso19111/crs.cpp",
        "using namespace NS_PROJ::internal;",
        "extern \"C\" double gometry_proj_conventional_period(double) noexcept;\n\nusing namespace NS_PROJ::internal;",
    )?;
    replace_pinned_source(
        source_dir,
        "src/malloc.cpp",
        "    skipNonInstantiable = other.skipNonInstantiable;",
        "    skipNonInstantiable = other.skipNonInstantiable;\n    gometryIdentityPhase = other.gometryIdentityPhase;\n    gometryIdentityPhaseHigh = other.gometryIdentityPhaseHigh;\n    gometryIdentityPhaseLow = other.gometryIdentityPhaseLow;",
    )?;
    replace_pinned_source(
        source_dir,
        "src/trans.cpp",
        "PJ_COORD proj_trans(PJ *P, PJ_DIRECTION direction, PJ_COORD coord) {",
        r#"static double gometry_add_phase(double value, double phaseHigh,
                                 double phaseLow) {
    if (!std::isfinite(value))
        return value;
    const auto twoSum = [](double a, double b) {
        const double sum = a + b;
        const double bVirtual = sum - a;
        const double error = (a - (sum - bVirtual)) + (b - bVirtual);
        return std::pair<double, double>(sum, error);
    };
    const auto first = twoSum(value, phaseHigh);
    const auto second = twoSum(first.second, phaseLow);
    const auto third = twoSum(first.first, second.first);
    return third.first + (third.second + second.second);
}

PJ_COORD proj_trans(PJ *P, PJ_DIRECTION direction, PJ_COORD coord) {"#,
    )?;
    replace_pinned_source(
        source_dir,
        "src/trans.cpp",
        "#include <limits>",
        "#include <limits>\n#include <utility>",
    )?;
    replace_pinned_source(
        source_dir,
        "src/trans.cpp",
        "    if (P->inverted)\n        direction = pj_opposite_direction(direction);\n\n    if (P->iso_obj != nullptr && !P->iso_obj_is_coordinate_operation) {",
        "    if (P->inverted)\n        direction = pj_opposite_direction(direction);\n    if (P->gometryIdentityPhase) {\n        const double sign = direction == PJ_FWD ? 1.0 : -1.0;\n        coord.xyzt.x = gometry_add_phase(\n            coord.xyzt.x, sign * P->gometryIdentityPhaseHigh,\n            sign * P->gometryIdentityPhaseLow);\n    }\n\n    if (P->iso_obj != nullptr && !P->iso_obj_is_coordinate_operation) {",
    )?;
    replace_pinned_source(
        source_dir,
        "src/info.cpp",
        "#include \"filemanager.hpp\"",
        "#include \"filemanager.hpp\"\n#include <cstring>",
    )?;
    replace_pinned_source(
        source_dir,
        "src/info.cpp",
        "    /* coordinate operation description */",
        "    /* coordinate operation description */\n    bool gometryDefinitionUnavailable = P->gometryIdentityPhase;",
    )?;
    replace_pinned_source(
        source_dir,
        "src/info.cpp",
        "    /* projection id */",
        "    gometryDefinitionUnavailable =\n        gometryDefinitionUnavailable || P->gometryIdentityPhase;\n\n    /* projection id */",
    )?;
    replace_pinned_source(
        source_dir,
        "src/info.cpp",
        "    /* projection definition */\n    if (P->def_full)\n        def = P->def_full;\n    else\n        def = pj_get_def(P, 0); /* pj_get_def takes a non-const PJ pointer */\n    if (nullptr == def)\n        pjinfo.definition = empty;\n    else\n        pjinfo.definition = pj_shrink(def);\n    /* Make proj_destroy clean this up eventually */\n    P->def_full = def;",
        "    /* projection definition */\n    if (gometryDefinitionUnavailable) {\n        pjinfo.definition = nullptr;\n    } else {\n        if (P->def_full)\n            def = P->def_full;\n        else\n            def = pj_get_def(P, 0); /* pj_get_def takes a non-const PJ pointer */\n        if (nullptr == def)\n            pjinfo.definition = empty;\n        else if (std::strstr(def, \"gometry_ang_\") != nullptr)\n            pjinfo.definition = nullptr;\n        else\n            pjinfo.definition = pj_shrink(def);\n        /* Make proj_destroy clean this up eventually */\n        P->def_full = def;\n    }",
    )?;
    replace_pinned_source(
        source_dir,
        "include/proj/io.hpp",
        "    PROJ_INTERNAL Convention convention() const;",
        "    PROJ_INTERNAL Convention convention() const;\n    PROJ_INTERNAL PROJStringFormatter &setGometryDefaultAngularUnits(bool enabled);\n    PROJ_INTERNAL bool gometryDefaultAngularUnits() const;",
    )?;
    replace_pinned_source(
        source_dir,
        "src/iso19111/io.cpp",
        "    bool legacyCRSToCRSContext_ = false;",
        "    bool legacyCRSToCRSContext_ = false;\n    bool gometryDefaultAngularUnits_ = false;",
    )?;
    replace_pinned_source(
        source_dir,
        "src/iso19111/io.cpp",
        "bool PROJStringFormatter::getCRSExport() const { return d->crsExport_; }",
        "bool PROJStringFormatter::getCRSExport() const { return d->crsExport_; }\n\nPROJStringFormatter &PROJStringFormatter::setGometryDefaultAngularUnits(bool enabled) {\n    d->gometryDefaultAngularUnits_ = enabled;\n    return *this;\n}\n\nbool PROJStringFormatter::gometryDefaultAngularUnits() const {\n    return d->gometryDefaultAngularUnits_;\n}",
    )?;
    replace_pinned_source(
        source_dir,
        "src/iso19111/c_api.cpp",
        "                auto projString = coordop->exportToPROJString(formatter.get());",
        "                formatter->setGometryDefaultAngularUnits(ctx->gometryDefaultAngularUnits);\n                auto projString = coordop->exportToPROJString(formatter.get());",
    )?;
    replace_pinned_source(
        source_dir,
        "src/iso19111/operation/singleoperation.cpp",
        "        bool done = false;\n        if (offsetDeg != 0.0) {",
        r#"        if (formatter->gometryDefaultAngularUnits()) {
            const auto &sourcePM = sourceCRSGeog->primeMeridian()->longitude();
            const auto &targetPM = targetCRSGeog->primeMeridian()->longitude();
            offsetDeg = gometry_adjust_longitude_rotation_degrees(
                offsetDeg, sourcePM.value(),
                sourcePM.unit().conversionToSI(), targetPM.value(),
                targetPM.unit().conversionToSI(),
                sourcePM.unit() == targetPM.unit());
        }
        bool done = false;
        if (offsetDeg != 0.0) {"#,
    )?;
    replace_pinned_source(
        source_dir,
        "src/conversions/unitconvert.cpp",
        "    point.xy.x *= Q->xy_factor;\n    point.xy.y *= Q->xy_factor;",
        r#"    point.xy.x = Q->x_gometry_mode == 0
                      ? point.xy.x * Q->x_factor
                      : gometry_forward_angular_value(
                            point.xy.x, Q->x_quantum_in,
                            Q->x_quantum_out, Q->x_factor,
                            Q->x_gometry_mode);
    point.xy.y = Q->y_gometry_mode == 0
                     ? point.xy.y * Q->y_factor
                     : gometry_forward_angular_value(
                           point.xy.y, Q->y_quantum_in,
                           Q->y_quantum_out, Q->y_factor,
                           Q->y_gometry_mode);"#,
    )?;
    replace_pinned_source(
        source_dir,
        "src/conversions/unitconvert.cpp",
        "    point.xy.x /= Q->xy_factor;\n    point.xy.y /= Q->xy_factor;",
        r#"    point.xy.x = Q->x_gometry_mode == 0
                      ? point.xy.x / Q->x_factor
                      : gometry_reverse_angular_value(
                            point.xy.x, Q->x_quantum_in,
                            Q->x_quantum_out, Q->x_factor,
                            Q->x_gometry_mode);
    point.xy.y = Q->y_gometry_mode == 0
                     ? point.xy.y / Q->y_factor
                     : gometry_reverse_angular_value(
                           point.xy.y, Q->y_quantum_in,
                           Q->y_quantum_out, Q->y_factor,
                           Q->y_gometry_mode);"#,
    )?;
    replace_pinned_source(
        source_dir,
        "src/conversions/unitconvert.cpp",
        "    Q->xy_factor = 1.0;",
        "    Q->x_factor = 1.0;",
    )?;
    replace_pinned_source(
        source_dir,
        "src/conversions/unitconvert.cpp",
        "        Q->xy_factor = f;",
        "        Q->x_factor = f;",
    )?;
    replace_pinned_source(
        source_dir,
        "src/conversions/unitconvert.cpp",
        "        Q->xy_factor /= f;",
        "        Q->x_factor /= f;",
    )?;
    replace_pinned_source(
        source_dir,
        "src/conversions/unitconvert.cpp",
        "    if ((name = pj_param(P->ctx, P->params, \"sz_in\").s) != nullptr) {",
        r#"    Q->y_factor = Q->x_factor;
     auto axisFactor = [P](const char *key, const char *numericKey, double &factor, bool inverse) {
         const char *name = pj_param(P->ctx, P->params, key).s;
         if (!name) return;
         double value = get_unit_conversion_factor(P, name, nullptr, nullptr);
         if (value == 0) value = pj_param(P->ctx, P->params, numericKey).f;
         factor = inverse ? factor / value : factor * value;
     };
    axisFactor("sx_in", "dx_in", Q->x_factor, false);
    axisFactor("sy_in", "dy_in", Q->y_factor, false);
    axisFactor("sx_out", "dx_out", Q->x_factor, true);
    axisFactor("sy_out", "dy_out", Q->y_factor, true);
    if ((name = pj_param(P->ctx, P->params, "sz_in").s) != nullptr) {"#,
    )?;

    replace_pinned_source(
        source_dir,
        "src/conversions/unitconvert.cpp",
        "    double y_factor; /* unit conversion factor for horizontal components */\n    double z_factor;",
        r#"    double y_factor; /* unit conversion factor for horizontal components */
    double x_quantum_in = 0;
    double x_quantum_out = 0;
    double y_quantum_in = 0;
    double y_quantum_out = 0;
    int x_gometry_mode = 0;
    int y_gometry_mode = 0;
    double z_factor;"#,
    )?;

    replace_pinned_source(
        source_dir,
        "src/conversions/unitconvert.cpp",
        "#include <errno.h>",
        "#include <errno.h>\n#include <cstring>\n#include <cstdlib>",
    )?;
    replace_pinned_source(
        source_dir,
        "src/conversions/unitconvert.cpp",
        "};\n} // anonymous namespace\n\n/***********************************************************************/\nstatic int is_leap_year(long year) {",
        r#"};

static double gometry_scaled_angular_value(double value, double inputQuantum,
                                           double outputQuantum) {
    if (value == 0.0)
        return value;
    if (value == inputQuantum)
        return outputQuantum;
    if (value == -inputQuantum)
        return -outputQuantum;
    int valueExponent = 0;
    int inputExponent = 0;
    int outputExponent = 0;
    const double valueMantissa = std::frexp(value, &valueExponent);
    const double inputMantissa = std::frexp(inputQuantum, &inputExponent);
    const double outputMantissa = std::frexp(outputQuantum, &outputExponent);
    const double mantissa = (valueMantissa / inputMantissa) * outputMantissa;
    return std::scalbn(mantissa,
                       valueExponent - inputExponent + outputExponent);
}

static double gometry_forward_angular_value(double value, double inputQuantum,
                                            double outputQuantum, double factor,
                                            int mode) {
    if (mode == 1) {
        if (value == inputQuantum)
            return outputQuantum;
        if (value == -inputQuantum)
            return -outputQuantum;
        return value * factor;
    }
    if (mode == 2) {
        if (value == inputQuantum)
            return outputQuantum;
        if (value == -inputQuantum)
            return -outputQuantum;
        return value / factor;
    }
    return gometry_scaled_angular_value(value, inputQuantum, outputQuantum);
}

static double gometry_reverse_angular_value(double value, double inputQuantum,
                                            double outputQuantum, double factor,
                                            int mode) {
    if (mode == 1) {
        if (value == outputQuantum)
            return inputQuantum;
        if (value == -outputQuantum)
            return -inputQuantum;
        return value / factor;
    }
    if (mode == 2) {
        if (value == outputQuantum)
            return inputQuantum;
        if (value == -outputQuantum)
            return -inputQuantum;
        return value * factor;
    }
    return gometry_scaled_angular_value(value, outputQuantum, inputQuantum);
}

} // anonymous namespace

/***********************************************************************/
static int is_leap_year(long year) {"#,
    )?;
    replace_pinned_source(
        source_dir,
        "src/conversions/unitconvert.cpp",
        "    return 0.0;\n}\n\n/***********************************************************************/\nPJ *PJ_CONVERSION(unitconvert, 0) {",
        r#"    /***********************************************************************/
    return 0.0;
}

static bool gometry_private_angular_unit(PJ *P, const char *name,
                                         double &factor, double &period) {
    if (!P->ctx->gometryDefaultAngularUnits || name == nullptr)
        return false;
    constexpr const char *periodPrefix = "gometry_ang_p_";
    constexpr const char *factorPrefix = "gometry_ang_f_";
    const char *number = nullptr;
    bool hasPeriod = false;
    if (std::strncmp(name, periodPrefix, std::strlen(periodPrefix)) == 0) {
        number = name + std::strlen(periodPrefix);
        hasPeriod = true;
    } else if (std::strncmp(name, factorPrefix, std::strlen(factorPrefix)) == 0) {
        number = name + std::strlen(factorPrefix);
    } else {
        return false;
    }
    char *end = nullptr;
    const double value = std::strtod(number, &end);
    if (end == number || *end != '\0' || !std::isfinite(value) || value <= 0.0)
        return false;
    period = hasPeriod ? value : 0.0;
    factor = hasPeriod ? (2.0 * M_PI) / value : value;
    return std::isfinite(factor) && factor > 0.0;
}

static double get_unit_conversion_factor(PJ *P, const char *name,
                                         int *p_is_linear,
                                         const char **p_normalized_name) {
    double factor = 0.0;
    double period = 0.0;
    int local_is_linear = -1;
    int *is_linear = p_is_linear ? p_is_linear : &local_is_linear;
    if (gometry_private_angular_unit(P, name, factor, period)) {
        if (p_normalized_name)
            *p_normalized_name = "GometryAngular";
        *is_linear = 0;
        return factor;
    }
    factor = get_unit_conversion_factor(name, is_linear, p_normalized_name);
    if (factor != 0.0 && P->ctx->gometryDefaultAngularUnits &&
        *is_linear == 0) {
        period = gometry_proj_conventional_period(factor);
        if (period > 0.0)
            factor = (2.0 * M_PI) / period;
    }
    return factor;
}

static bool gometry_angular_quantum(PJ *P, const char *name, bool longitude,
                                    double &quantum) {
    if (!P->ctx->gometryDefaultAngularUnits || name == nullptr)
        return false;
    double factor = 0.0;
    double period = 0.0;
    int isLinear = -1;
    if (!gometry_private_angular_unit(P, name, factor, period)) {
        factor = get_unit_conversion_factor(name, &isLinear, nullptr);
        if (factor == 0.0 || isLinear != 0)
            return false;
        period = gometry_proj_conventional_period(factor);
    }
    if (period > 0.0) {
        quantum = longitude ? period / 2.0 : period / 4.0;
    } else {
        quantum = (longitude ? M_PI : M_HALFPI) / factor;
    }
    return std::isfinite(quantum) && quantum > 0.0;
}

static int gometry_angular_mode(double inputQuantum, double outputQuantum,
                                double &factor) {
    const double multiply = outputQuantum / inputQuantum;
    if (std::isfinite(multiply) && multiply > 0.0 &&
        inputQuantum * multiply == outputQuantum &&
        outputQuantum / multiply == inputQuantum) {
        factor = multiply;
        return 1;
    }
    const double divide = inputQuantum / outputQuantum;
    if (std::isfinite(divide) && divide > 0.0 &&
        inputQuantum / divide == outputQuantum &&
        outputQuantum * divide == inputQuantum) {
        factor = divide;
        return 2;
    }
    return 3;
}

static const char *gometry_axis_unit(PJ *P, const char *axisKey,
                                    const char *xyKey) {
    const char *axis = pj_param(P->ctx, P->params, axisKey).s;
    return axis ? axis : pj_param(P->ctx, P->params, xyKey).s;
}

static void gometry_prepare_unitconvert(PJ *P,
                                        struct pj_opaque_unitconvert *Q) {
    const char *xIn = gometry_axis_unit(P, "sx_in", "sxy_in");
    const char *yIn = gometry_axis_unit(P, "sy_in", "sxy_in");
    const char *xOut = gometry_axis_unit(P, "sx_out", "sxy_out");
    const char *yOut = gometry_axis_unit(P, "sy_out", "sxy_out");
    if (gometry_angular_quantum(P, xIn, true, Q->x_quantum_in) &&
        gometry_angular_quantum(P, xOut, true, Q->x_quantum_out)) {
        Q->x_gometry_mode =
            gometry_angular_mode(Q->x_quantum_in, Q->x_quantum_out, Q->x_factor);
    }
     if (gometry_angular_quantum(P, yIn, false, Q->y_quantum_in) &&
         gometry_angular_quantum(P, yOut, false, Q->y_quantum_out)) {
         Q->y_gometry_mode =
             gometry_angular_mode(Q->y_quantum_in, Q->y_quantum_out, Q->y_factor);
     }
 }

/***********************************************************************/
PJ *PJ_CONVERSION(unitconvert, 0) {"#,
    )?;
    replace_pinned_source(
        source_dir,
        "src/conversions/unitconvert.cpp",
        "f = get_unit_conversion_factor(name, &xy_in_is_linear,",
        "f = get_unit_conversion_factor(P, name, &xy_in_is_linear,",
    )?;
    replace_pinned_source(
        source_dir,
        "src/conversions/unitconvert.cpp",
        "f = get_unit_conversion_factor(name, &xy_out_is_linear,",
        "f = get_unit_conversion_factor(P, name, &xy_out_is_linear,",
    )?;
    replace_pinned_source(
        source_dir,
        "src/conversions/unitconvert.cpp",
        "f = get_unit_conversion_factor(name, &z_in_is_linear,",
        "f = get_unit_conversion_factor(P, name, &z_in_is_linear,",
    )?;
    replace_pinned_source(
        source_dir,
        "src/conversions/unitconvert.cpp",
        "f = get_unit_conversion_factor(name, &z_out_is_linear,",
        "f = get_unit_conversion_factor(P, name, &z_out_is_linear,",
    )?;
    replace_pinned_source(
        source_dir,
        "src/conversions/unitconvert.cpp",
        "    return P;\n}",
        "    gometry_prepare_unitconvert(P, Q);\n    return P;\n}",
    )?;
    replace_pinned_source(
        source_dir,
        "src/iso19111/common.cpp",
        "    return name() == other.name();",
        "    return name() == other.name() && type() == other.type() && conversionToSI() == other.conversionToSI();",
    )?;
    replace_pinned_source(
        source_dir,
        "src/iso19111/common.cpp",
        "    return name() != other.name();",
        "    return !operator==(other);",
    )?;
    replace_pinned_source(
        source_dir,
        "src/iso19111/common.cpp",
        r#"            if (::fabs(proj_units[i].factor - conversionToSI()) <
                1e-10 * conversionToSI()) {"#,
        "            if (proj_units[i].factor == conversionToSI()) {",
    )?;
    replace_pinned_source(
        source_dir,
        "src/iso19111/common.cpp",
        r#"            if (::fabs(proj_angular_units[i].factor - conversionToSI()) <
                1e-10 * conversionToSI()) {"#,
        "            if (proj_angular_units[i].factor == conversionToSI()) {",
    )?;
    replace_pinned_source(
        source_dir,
        "src/iso19111/common.cpp",
        r#"    return std::fabs(conversionToSI() - other.conversionToSI()) <=
           1e-10 * std::fabs(conversionToSI());"#,
        "    return type() == other.type() && conversionToSI() == other.conversionToSI();",
    )?;
    replace_pinned_source(
        source_dir,
        "src/iso19111/common.cpp",
        "           maxRelativeError * std::fabs(SIValue);",
        "           0.0 * std::fabs(SIValue);",
    )?;

    replace_pinned_source(
        source_dir,
        "src/iso19111/coordinatesystem.cpp",
        r#"    // For approximate comparison, only care about axis direction and unit.
    if (!(*(d->direction) == *(otherCSA->d->direction) &&
          d->unit._isEquivalentTo(otherCSA->d->unit, criterion))) {"#,
        r#"    const auto &m1 = meridian();
    const auto &m2 = otherCSA->meridian();
    if (bool(m1) != bool(m2) ||
        (m1 && m1->longitude().getSIValue() != m2->longitude().getSIValue()))
        return false;
    // Direction and scale define the coordinate mapping.
    if (!(*(d->direction) == *(otherCSA->d->direction) &&
          d->unit._isEquivalentTo(otherCSA->d->unit, criterion))) {"#,
    )?;

    replace_pinned_source(
        source_dir,
        "src/iso19111/crs.cpp",
        r#"    if (&dir0 == &cs::AxisDirection::NORTH &&
        &dir1 == &cs::AxisDirection::EAST) {"#,
        r#"    if ((&dir0 == &cs::AxisDirection::NORTH || &dir0 == &cs::AxisDirection::SOUTH) &&
        (&dir1 == &cs::AxisDirection::EAST || &dir1 == &cs::AxisDirection::WEST)) {"#,
    )?;
    replace_pinned_source_section(
        source_dir,
        "src/iso19111/crs.cpp",
        "        const auto &unitHoriz = axisList[0]->unit();",
        "    if (axisList.size() == 3 && !formatter->omitZUnitConversion())",
        r#"        const auto &unit0 = axisList[0]->unit();
        const auto &unit1 = axisList[1]->unit();
        const auto outputToken = [formatter](const common::UnitOfMeasure &unit) {
            if (formatter->gometryDefaultAngularUnits() &&
                !formatter->getCRSExport()) {
                const double period =
                    gometry_proj_conventional_period(unit.conversionToSI());
                if (period > 0.0)
                    return std::string("gometry_ang_p_") +
                           internal::toString(period);
                return std::string("gometry_ang_f_") +
                       internal::toString(unit.conversionToSI());
            }
            const auto token = unit.exportToPROJString();
            return token.empty() ? internal::toString(unit.conversionToSI())
                                 : token;
        };
        const auto &direction0 = axisList[0]->direction();
        const bool latitudeFirst = &direction0 == &cs::AxisDirection::NORTH ||
                                   &direction0 == &cs::AxisDirection::SOUTH;
        const auto &longitudeUnit = latitudeFirst ? unit1 : unit0;
        const auto &latitudeUnit = latitudeFirst ? unit0 : unit1;
        const auto longitudeToken = outputToken(longitudeUnit);
        const auto latitudeToken = outputToken(latitudeUnit);
        if (longitudeToken == latitudeToken) {
            formatter->addParam("xy_out", longitudeToken);
        } else {
            formatter->addParam("x_out", longitudeToken);
            formatter->addParam("y_out", latitudeToken);
        }
    }
"#,
    )?;
    replace_pinned_source_section(
        source_dir,
        "src/iso19111/crs.cpp",
        "    const auto &unit = axisListIn[0]->unit();",
        "\n    if (!axisSpecFound &&",
        r#"    const auto &unit0 = axisListIn[0]->unit();
    const auto &unit1 = axisListIn[1]->unit();
    const auto *zUnit = axisListIn.size() == 3 ? &axisListIn[2]->unit() : nullptr;
    if (unit0.conversionToSI() != 1.0 || unit1.conversionToSI() != 1.0 ||
        (zUnit && zUnit->conversionToSI() != 1.0)) {
        if (formatter->getCRSExport()) {
            const auto token = unit0.exportToPROJString();
            if (token.empty()) formatter->addParam("to_meter", unit0.conversionToSI());
            else formatter->addParam("units", token);
        } else {
            formatter->addStep("unitconvert");
            formatter->addParam("xy_in", "m");
            if (unit0.conversionToSI() == unit1.conversionToSI()) {
                const auto token = unit0.exportToPROJString();
                if (token.empty()) formatter->addParam("xy_out", unit0.conversionToSI());
                else formatter->addParam("xy_out", token);
            } else {
                const auto &dir0 = axisListIn[0]->direction();
                const auto &dir1 = axisListIn[1]->direction();
                const bool swap = (&dir0 == &cs::AxisDirection::NORTH ||
                                   &dir0 == &cs::AxisDirection::SOUTH) &&
                                  (&dir1 == &cs::AxisDirection::EAST ||
                                   &dir1 == &cs::AxisDirection::WEST);
                formatter->addParam("x_out", (swap ? unit1 : unit0).conversionToSI());
                formatter->addParam("y_out", (swap ? unit0 : unit1).conversionToSI());
            }
            if (zUnit) {
                formatter->addParam("z_in", "m");
                formatter->addParam("z_out", zUnit->conversionToSI());
            }
        }
    } else if (formatter->getCRSExport() && !formatter->getLegacyCRSToCRSContext()) {
        formatter->addParam("units", "m");
    }
"#,
    )?;

    replace_pinned_source(
        source_dir,
        "src/iso19111/datum.cpp",
        "        if (::fabs(valRad - valRefRad) < 1e-10) {",
        "        if (valRad == valRefRad) {",
    )?;
    replace_pinned_source(
        source_dir,
        "src/iso19111/datum.cpp",
        "        if (::fabs(a - a_iter) < 1e-10 * a_iter) {",
        "        if (a == a_iter) {",
    )?;
    replace_pinned_source(
        source_dir,
        "src/iso19111/datum.cpp",
        "                if (::fabs(b - b_iter) < 1e-10 * b_iter) {",
        "                if (b == b_iter) {",
    )?;
    replace_pinned_source(
        source_dir,
        "src/iso19111/datum.cpp",
        "                if (::fabs(rf - rf_iter) < 1e-10 * rf_iter) {",
        "                if (rf == rf_iter) {",
    )?;
    remove_pinned_source_section(
        source_dir,
        "src/iso19111/datum.cpp",
        r#"    // PROJ "clrk80" name is "Clarke 1880 mod.""#,
        "    if (!semiMajorAxis()._isEquivalentTo",
    )?;
    replace_pinned_source(
        source_dir,
        "src/iso19111/datum.cpp",
        r#"        if (l_primeMeridian->nameStr() != "Greenwich") {"#,
        "        if (l_primeMeridian->longitude().getSIValue() != 0.0) {",
    )?;
    replace_pinned_source(
        source_dir,
        "src/iso19111/datum.cpp",
        r#"    if (l_primeMeridian->nameStr() != "Greenwich") {"#,
        "    if (l_primeMeridian->longitude().getSIValue() != 0.0) {",
    )?;
    remove_pinned_source_section(
        source_dir,
        "src/iso19111/datum.cpp",
        "    if (dbContext) {\n        if (!l_identifiers.empty()) {",
        "    std::string l_name(nameStr());\n    if (grf) {",
    )?;

    replace_pinned_source(
        source_dir,
        "src/iso19111/internal.cpp",
        "/******************************************************************************\n",
        "#include <charconv>\n/******************************************************************************\n",
    )?;
    replace_pinned_source(
        source_dir,
        "src/iso19111/internal.cpp",
        r#"std::string toString(double val, int precision) {
    std::ostringstream buffer;
    buffer.imbue(std::locale::classic());
    buffer << std::setprecision(precision);
    buffer << val;
    auto str = buffer.str();
    if (precision == 15 && str.find("9999999999") != std::string::npos) {
        buffer.str("");
        buffer.clear();
        buffer << std::setprecision(14);
        buffer << val;
        return buffer.str();
    }
    return str;
}"#,
        r#"std::string toString(double val, int) {
    char buffer[64];
    const auto result = std::to_chars(buffer, buffer + sizeof(buffer), val);
    return std::string(buffer, result.ptr);
}"#,
    )?;
    replace_pinned_source(
        source_dir,
        "src/iso19111/internal.cpp",
        r#"std::string toString(double val, int precision) {
    // use sqlite3 API that is slightly faster than std::ostringstream
    // with forcing the C locale. sqlite3_snprintf() emulates a C locale.
    constexpr int BUF_SIZE = 32;
    char szBuffer[BUF_SIZE];
    sqlite3_snprintf(BUF_SIZE, szBuffer, "%.*g", precision, val);
    if (precision == 15 && strstr(szBuffer, "9999999999")) {
        sqlite3_snprintf(BUF_SIZE, szBuffer, "%.14g", val);
    }
    return szBuffer;
}"#,
        r#"std::string toString(double val, int) {
    char buffer[64];
    const auto result = std::to_chars(buffer, buffer + sizeof(buffer), val);
    return std::string(buffer, result.ptr);
}"#,
    )?;

    replace_pinned_source(
        source_dir,
        "src/iso19111/io.cpp",
        r#"        constexpr double US_FOOT_CONV_FACTOR = 12.0 / 39.37;
        constexpr double REL_ERROR = 1e-10;
        // Fix common rounding errors
        if (std::fabs(convFactor - UnitOfMeasure::DEGREE.conversionToSI()) <
            REL_ERROR * convFactor) {
            convFactor = UnitOfMeasure::DEGREE.conversionToSI();
        } else if (std::fabs(convFactor - US_FOOT_CONV_FACTOR) <
                   REL_ERROR * convFactor) {
            convFactor = US_FOOT_CONV_FACTOR;
        }
"#,
        r#"        // WKT1/ESRI records carry rounded legacy unit factors. Normalize only
        // that parser dialect; authored WKT2 and PROJJSON factors remain exact.
        if (esriStyle_ && dbContext_ &&
            (ci_equal(unitName, "degree") || code == "9002" || code == "9003")) {
            constexpr double US_FOOT_CONV_FACTOR = 12.0 / 39.37;
            constexpr double REL_ERROR = 1e-10;
            if (std::fabs(convFactor - UnitOfMeasure::DEGREE.conversionToSI()) <
                REL_ERROR * convFactor) {
                convFactor = UnitOfMeasure::DEGREE.conversionToSI();
            } else if (std::fabs(convFactor - US_FOOT_CONV_FACTOR) <
                       REL_ERROR * convFactor) {
                convFactor = US_FOOT_CONV_FACTOR;
            }
        }
"#,
    )?;
    remove_pinned_source_section(
        source_dir,
        "src/iso19111/io.cpp",
        "        // Correct for GDAL WKT1 and WKT1-ESRI departure",
        "        auto &properties = buildProperties(node);",
    )?;
    replace_pinned_source(
        source_dir,
        "src/iso19111/io.cpp",
        "                } else if (dbCRS) {",
        "                } else if (dbCRS && isNull(csNode) &&\n                           node->countChildrenOfName(WKTConstants::AXIS) == 0) {",
    )?;
    remove_pinned_source_section(
        source_dir,
        "src/iso19111/io.cpp",
        "        if (dbContext_ && memberJ.contains(\"id\")) {",
        "        if (!datumAdded) {",
    )?;
    remove_pinned_source_section(
        source_dir,
        "src/iso19111/io.cpp",
        "    if (std::abs(val * 10 - std::round(val * 10)) < 1e-8) {",
        "    return normalizeSerializedString(internal::toString(val));",
    )?;

    replace_pinned_source(
        source_dir,
        "src/iso19111/operation/conversion.cpp",
        r#"                } else if (param->unit_type ==
                           common::UnitOfMeasure::Type::ANGULAR) {
                    valueConverted =
                        value.convertToUnit(common::UnitOfMeasure::DEGREE);
                } else {"#,
        r#"                } else if (param->unit_type ==
                           common::UnitOfMeasure::Type::ANGULAR) {
                    valueConverted =
                        value.value() *
                        (value.unit().conversionToSI() /
                         common::UnitOfMeasure::DEGREE.conversionToSI());
                } else {"#,
    )?;
    replace_pinned_source(
        source_dir,
        "src/iso19111/operation/conversion.cpp",
        r#"std::fabs(measure.value() -
                                  UTM_LATITUDE_OF_NATURAL_ORIGIN) < 1e-10"#,
        "measure.value() == UTM_LATITUDE_OF_NATURAL_ORIGIN",
    )?;
    replace_pinned_source(
        source_dir,
        "src/iso19111/operation/conversion.cpp",
        "std::abs(dfZone - std::round(dfZone)) < 1e-10",
        "measure.value() == 6.0 * std::round(dfZone) - 183.0",
    )?;
    replace_pinned_source(
        source_dir,
        "src/iso19111/operation/conversion.cpp",
        "std::fabs(measure.value() - UTM_SCALE_FACTOR) < 1e-10",
        "measure.value() == UTM_SCALE_FACTOR",
    )?;
    replace_pinned_source(
        source_dir,
        "src/iso19111/operation/conversion.cpp",
        r#"std::fabs(measure.value() -
                                      UTM_NORTH_FALSE_NORTHING) < 1e-10"#,
        "measure.value() == UTM_NORTH_FALSE_NORTHING",
    )?;
    replace_pinned_source(
        source_dir,
        "src/iso19111/operation/conversion.cpp",
        r#"std::fabs(measure.value() -
                                             UTM_SOUTH_FALSE_NORTHING) <
                                   1e-10"#,
        "measure.value() == UTM_SOUTH_FALSE_NORTHING",
    )?;

    remove_pinned_source_section(
        source_dir,
        "src/iso19111/operation/coordinateoperationfactory.cpp",
        "        std::map<std::pair<io::AuthorityFactory::ObjectType, std::string>,",
        "        // Normally there should be at most one element",
    )?;
    remove_pinned_source_section(
        source_dir,
        "src/iso19111/operation/coordinateoperationfactory.cpp",
        "            const std::pair<io::AuthorityFactory::ObjectType, std::string> key(",
        "            const auto &authFactoryName",
    )?;
    replace_pinned_source(
        source_dir,
        "src/iso19111/operation/coordinateoperationfactory.cpp",
        "            context.cacheNameToCRS[key] = ids;\n",
        "",
    )?;
    replace_pinned_source(
        source_dir,
        "src/iso19111/operation/coordinateoperationfactory.cpp",
        "        if (offset_pm.value() == 0 && !axisReversal2D && !axisReversal3D) {",
        "        if (offset_pm.value() == 0 && !axisReversal2D && !axisReversal3D &&\n            srcAxisList[0]->_isEquivalentTo(dstAxisList[0].get(), util::IComparable::Criterion::EQUIVALENT) &&\n            srcAxisList[1]->_isEquivalentTo(dstAxisList[1].get(), util::IComparable::Criterion::EQUIVALENT)) {",
    )?;
    replace_pinned_source(
        source_dir,
        "src/iso19111/operation/coordinateoperationfactory.cpp",
        "    if (sameDatum && (axisReversal2D || axisReversal3D)) {",
        "    if (sameDatum && (axisReversal2D || axisReversal3D) &&\n        offset_pm.value() == 0 &&\n        srcAxisList[0]->unit().conversionToSI() == dstAxisList[1]->unit().conversionToSI() &&\n        srcAxisList[1]->unit().conversionToSI() == dstAxisList[0]->unit().conversionToSI()) {",
    )?;
    replace_pinned_source_section(
        source_dir,
        "src/iso19111/operation/singleoperation.cpp",
        "    if (isAxisOrderReversal(methodEPSGCode)) {\n        formatter->addStep(\"axisswap\");\n        formatter->addParam(\"order\", \"2,1\");\n        auto sourceCRSGeog =",
        "\n        return true;\n    }\n\n    if (methodEPSGCode == EPSG_CODE_METHOD_GEOGRAPHIC_GEOCENTRIC)",
        "    if (isAxisOrderReversal(methodEPSGCode)) {\n        formatter->addStep(\"axisswap\");\n        formatter->addParam(\"order\", \"2,1\");",
    )?;

    // Upstream PROJ 9.8 (PR #4674) zeroes northing when cos(phi) rounds to 1;
    // this patch uses the equivalent atan2 form, which keeps the true northing
    // in that band and removes the acos conditioning step.
    replace_pinned_source(
        source_dir,
        "src/projections/tmerc.cpp",
        "    xy.y = cosphi * cos(lp.lam) / sqrt(1. - b * b);\n\n    b = fabs(xy.y);\n    if (cosphi == 1 && (lp.lam < -M_HALFPI || lp.lam > M_HALFPI)) {\n        /* Helps to be able to roundtrip |longitudes| > 90 at lat=0 */\n        /* We could also map to -M_PI ... */\n        xy.y = M_PI;\n    } else if (b >= 1.) {\n        if ((b - 1.) > EPS10) {\n            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);\n            return xy;\n        } else\n            xy.y = 0.;\n    } else\n        xy.y = acos(xy.y);",
        "    xy.y = atan2(fabs(sin(lp.phi)), cosphi * cos(lp.lam));",
    )?;
    replace_pinned_source(
        source_dir,
        "src/projections/tmerc.cpp",
        "    h = cos(D);\n    lp.phi = asin(sqrt((1. - h * h) / (1. + g * g)));",
        "    h = cos(D);\n    lp.phi = atan2(fabs(sin(D)), hypot(g, h));",
    )?;

    replace_pinned_source(
        source_dir,
        "src/proj_json_streaming_writer.cpp",
        "/******************************************************************************\n",
        "#define FROM_PROJ_CPP\n#include <proj/internal/internal.hpp>\n/******************************************************************************\n",
    )?;
    replace_pinned_source_section(
        source_dir,
        "src/proj_json_streaming_writer.cpp",
        "    } else if (dfVal >= std::numeric_limits<int>::min() &&",
        "\n    }\n}",
        "    } else {\n        Print(internal::toString(dfVal, 17));",
    )?;
    Ok(())
}

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
