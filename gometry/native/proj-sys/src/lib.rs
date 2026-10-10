#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![doc(html_logo_url = "https://raw.githubusercontent.com/georust/meta/master/logo/logo.png")]
//! # Low-level bindings for PROJ v9.6.x
//!
//! **This is a
//! [`*-sys`](https://doc.rust-lang.org/cargo/reference/build-scripts.html#-sys-packages)
//! crate; you shouldn't use its API directly.** See the
//! [`proj`](https://github.com/georust/proj) crate for general use.
//!
//! A guide to the functions can be found here:
//! <https://proj.org/development/reference/functions.html>.
//!
//! By default, the crate will search for an acceptable existing `libproj`
//! installation on your system using
//! [pkg-config](https://www.freedesktop.org/wiki/Software/pkg-config/).
//!
//! If an acceptable installation is not found, proj-sys will attempt to build
//! libproj from source bundled in the crate.
//!
//! ## Features
//!
//! `bundled_proj` - forces building libproj from source even if an acceptable
//! version could be found on your system. It also enables bundled SQLite and
//! builds `libproj` **without** its native network functionality; you will
//! have to implement your own set of callbacks if you wish to make use of them
//! (see the [`proj`](https://crates.io/crates/proj) crate for an example).
//!
//! The `tiff` feature links the path dependency's statically bundled libtiff and
//! zlib. It does not use system libtiff or fetch source or transformation grids
//! from the network.

#[cfg(bundled_build)]
extern crate libsqlite3_sys;
#[cfg(bundled_build)]
extern crate link_cplusplus;

#[allow(
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    reason = "bindgen's const ABI assertions intentionally fail compilation through subtraction or indexing when C layouts differ"
)]
mod bindings {
    #[cfg(all(not(feature = "nobuild"), feature = "buildtime_bindgen"))]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

    #[cfg(not(feature = "buildtime_bindgen"))]
    include!("bundled_bindings.rs");
}

pub use bindings::*;

#[cfg(bundled_build)]
unsafe extern "C" {
    /// Create a fresh context with the bundled PROJ defaults.
    ///
    /// This private entry point is available only for the bundled build. It
    /// intentionally bypasses PROJ's process default context.
    ///
    /// # Safety
    ///
    /// The returned pointer is uniquely owned when non-null and must be
    /// released with `proj_context_destroy` exactly once.
    pub fn gometry_proj_context_create() -> *mut PJ_CONTEXT;
    /// Enable the immutable default angular-unit policy for this fresh context.
    ///
    /// This must be called before resolving a CRS-to-CRS operation. Contexts
    /// used for metadata and literal operation definitions leave it disabled.
    ///
    /// # Safety
    ///
    /// `context` is a live context created by this crate and is used only on
    /// its creating thread.
    pub fn gometry_proj_context_enable_default_angular_units(
        context: *mut PJ_CONTEXT,
    ) -> std::ffi::c_int;
    /// Resolve a recognized conventional angular period, or return zero.
    pub fn gometry_proj_conventional_period(radians_per_unit: f64) -> f64;
    /// Bind the original endpoint prime-meridian phase to a normalized no-op PJ.
    ///
    /// # Safety
    ///
    /// All PJ arguments are live on `context`'s creating thread. `operation`
    /// is uniquely owned and remains so after the metadata bind.
    pub fn gometry_proj_configure_default_noop_phase(
        context: *mut PJ_CONTEXT,
        operation: *mut PJ,
        source_crs: *const PJ,
        target_crs: *const PJ,
    );
}

/// The pinned PROJ and Rust CRS admission share this recognition rule.
pub fn gometry_conventional_period(radians_per_unit: f64) -> Option<f64> {
    #[cfg(bundled_build)]
    // SAFETY: the helper is pure and accepts a binary64 value by value.
    let period = unsafe { gometry_proj_conventional_period(radians_per_unit) };

    #[cfg(not(bundled_build))]
    let period = {
        let turns = std::f64::consts::TAU / radians_per_unit;
        let nearest = turns.round();
        if radians_per_unit.is_finite()
            && radians_per_unit > 0.0
            && nearest > 0.0
            && (turns - nearest).abs() <= 8.0 * f64::EPSILON * nearest.abs()
        {
            nearest
        } else {
            0.0
        }
    };

    (period > 0.0).then_some(period)
}

/// Set default angular semantics on a fresh, uniquely owned bundled context.
///
/// # Safety
///
/// `context` must be a live context created by this crate and used on its
/// creating thread.
pub unsafe fn gometry_enable_default_angular_units(context: *mut PJ_CONTEXT) -> bool {
    #[cfg(bundled_build)]
    {
        // SAFETY: caller owns this live context on the creating thread.
        unsafe { gometry_proj_context_enable_default_angular_units(context) != 0 }
    }

    #[cfg(not(bundled_build))]
    {
        let _ = context;
        false
    }
}
