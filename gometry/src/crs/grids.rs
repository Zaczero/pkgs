//! PROJ grid database introspection (`grid_info`).
//!
//! The public entry point and its typed-context helper stay private in the
//! parent `crs` module, reached via `use super::*`; re-exported at `crs`.

use crate::crs::{
    CString, CrsError, GridDatabaseInfo, ProjContext, cstring, ptr, with_proj_context,
};
use crate::error::Result;

pub(crate) fn grid_info(name: &str) -> Result<GridDatabaseInfo> {
    let name = name.trim();
    if name.is_empty() {
        return Err(CrsError::invalid("grid name is required".to_owned()));
    }
    let grid_name = cstring(name)?;
    with_proj_context(|context| grid_info_for_context(context, name, &grid_name))?
}

pub(super) fn grid_info_for_context(
    context: &ProjContext,
    name: &str,
    grid_name: &CString,
) -> Result<GridDatabaseInfo> {
    let mut full_name = ptr::null();
    let mut package_name = ptr::null();
    let mut url = ptr::null();
    let mut direct_download = 0;
    let mut open_license = 0;
    let mut available = 0;
    // SAFETY: DOC-H. Typed live context; grid_name is a live CString; OUT slots
    // are exclusive locals; returned strings are copied immediately.
    let found = unsafe {
        proj_sys::proj_grid_get_info_from_database(
            context.as_ptr(),
            grid_name.as_ptr(),
            &raw mut full_name,
            &raw mut package_name,
            &raw mut url,
            &raw mut direct_download,
            &raw mut open_license,
            &raw mut available,
        )
    };
    if found == 0 {
        return Err(CrsError::message(format!("unknown PROJ grid {name:?}")));
    }
    // The database flag only proves that the configured filename exists. Probe
    // the grid itself so malformed caller-supplied files do not advertise an
    // executable transformation. Creating a shift operation is the narrow
    // context-bound probe: it uses the caller's search paths and works for both
    // horizontal and vertical legacy grid formats.
    let grid_is_usable = if available != 0 {
        let definition = cstring(format!("+proj=hgridshift +grids={name} +ellps=WGS84"))?;
        // SAFETY: typed live context and live definition; each non-null PJ is
        // destroyed immediately after the probe.
        let horizontal = unsafe { proj_sys::proj_create(context.as_ptr(), definition.as_ptr()) };
        if horizontal.is_null() {
            let definition = cstring(format!("+proj=vgridshift +grids={name} +ellps=WGS84"))?;
            // SAFETY: typed live context and live definition; each non-null PJ
            // is destroyed immediately after the probe.
            let vertical = unsafe { proj_sys::proj_create(context.as_ptr(), definition.as_ptr()) };
            if vertical.is_null() {
                false
            } else {
                // SAFETY: `vertical` is a live PROJ operation and the
                // coordinate is a by-value input.
                let coordinate = unsafe {
                    proj_sys::proj_coord((-104.0_f64).to_radians(), 40.0_f64.to_radians(), 0.0, 0.0)
                };
                // SAFETY: `vertical` remains live for this synchronous probe;
                // PROJ writes no borrowed data into the coordinate.
                let usable = unsafe {
                    proj_sys::proj_trans(vertical, proj_sys::PJ_DIRECTION_PJ_FWD, coordinate);
                    proj_sys::proj_errno(vertical) == 0
                };
                // SAFETY: uniquely owned probe object.
                unsafe {
                    proj_sys::proj_destroy(vertical);
                }
                usable
            }
        } else {
            // SAFETY: `horizontal` is a live PROJ operation and the
            // coordinate is a by-value input.
            let coordinate = unsafe {
                proj_sys::proj_coord((-104.0_f64).to_radians(), 40.0_f64.to_radians(), 0.0, 0.0)
            };
            // Creation is lazy for grid-backed operations. Execute one
            // coordinate so availability means the grid can actually open.
            // SAFETY: `horizontal` remains live for this synchronous probe;
            // PROJ writes no borrowed data into the coordinate.
            let usable = unsafe {
                proj_sys::proj_trans(horizontal, proj_sys::PJ_DIRECTION_PJ_FWD, coordinate);
                proj_sys::proj_errno(horizontal) == 0
            };
            // SAFETY: uniquely owned probe object.
            unsafe {
                proj_sys::proj_destroy(horizontal);
            }
            usable
        }
    } else {
        false
    };
    Ok(GridDatabaseInfo {
        name: name.to_owned(),
        full_name: proj_c_string!(full_name),
        package_name: proj_c_string!(package_name),
        url: proj_c_string!(url),
        direct_download: direct_download != 0,
        available: grid_is_usable,
    })
}
