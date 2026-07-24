//! PROJ grid database introspection (`grid_info`).
//!
//! The public entry point and its raw-pointer context helper stay private in the
//! parent `crs` module, reached via `use super::*`; re-exported at `crs`.

use super::*;
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
    context: *mut proj_sys::PJ_CONTEXT,
    name: &str,
    grid_name: &CString,
) -> Result<GridDatabaseInfo> {
    let mut full_name = ptr::null();
    let mut package_name = ptr::null();
    let mut url = ptr::null();
    let mut direct_download = 0;
    let mut open_license = 0;
    let mut available = 0;
    // SAFETY: context and grid_name are valid for the call; output pointers
    // reference initialized local storage and returned strings are copied.
    let found = unsafe {
        proj_sys::proj_grid_get_info_from_database(
            context,
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
    Ok(GridDatabaseInfo {
        name: name.to_owned(),
        full_name: string_from_ptr(full_name),
        package_name: string_from_ptr(package_name),
        available: available != 0,
    })
}
