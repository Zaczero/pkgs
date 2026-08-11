//! libPROJ FFI handle types — RAII `Drop` and small constructors for
//! `ProjContext`/`ProjArea`/`ProjOperationFactoryContext`/
//! `ProjCrsListParameters`/`ProjTransformOptions` plus typed list owners.
//!
//! Handle structs live here; reached via `use super::*`.
//!
//! # Ownership design (R13-L3)
//!
//! Safe crate-internal APIs never accept raw PROJ pointers. Provenance is
//! encoded in typed owners (`ProjContext`, `OwnedPj`, list guards). `as_ptr()`
//! is for the final documented FFI expression only. Ownership adoption is
//! `unsafe` after a checked `NonNull::new`, with `# Safety` naming provenance,
//! thread confinement, and exactly-once destruction.

use std::os::raw::c_int;
use std::ptr::NonNull;

use crate::crs::{
    AreaOfInterest, AreaOfUse, CStr, CString, CelestialBodyInfo, CrsCatalogInfo, CrsCatalogOptions,
    CrsError, CrsObjectKind, ProjContext, TransformOptions, UnitInfo, c_char, crs_type_name,
    cstring, ptr,
};
use crate::error::Result;

/// Provenance-owning C-string copy from a raw PROJ pointer.
///
/// # Safety (call-site obligation)
///
/// The expression must evaluate to null or a NUL-terminated C string that
/// remains live for the evaluation (object / list-entry / process-static).
/// This macro expands to an `unsafe` call of [`copy_proj_c_string`]; it is
/// **not** a safe API — the raw-pointer precondition is documented here and
/// at every expansion via the fixed SAFETY proof. Prefer RAII owners first.
macro_rules! proj_c_string {
    ($ptr:expr) => {{
        // SAFETY: `$ptr` is a PROJ-returned C string (or null) that remains
        // live for this evaluation — object-lifetime, list-entry, or
        // process-static. Call sites of this macro are only at those FFI
        // read boundaries.
        unsafe { $crate::crs::proj::copy_proj_c_string($ptr) }
    }};
}

/// Owned `PJ` handle — destroyed exactly once on drop.
pub(super) struct OwnedPj {
    raw: NonNull<proj_sys::PJ>,
}

/// Owned `PJ_OBJ_LIST` handle. Objects fetched with [`ProjObjList::get`] are
/// independent owned references and must be destroyed separately.
pub(super) struct ProjObjList {
    raw: NonNull<proj_sys::PJ_OBJ_LIST>,
}

/// Owned PROJ int list (e.g. identify confidence values).
pub(super) struct ProjIntList {
    raw: Option<NonNull<c_int>>,
    len: usize,
}

/// Caller-owned PROJ null-terminated string list (`proj_*_from_database`).
pub(super) struct OwnedProjStringList {
    raw: NonNull<*mut c_char>,
}

/// Caller-owned CRS info list from `proj_get_crs_info_list_from_database`.
pub(super) struct OwnedCrsInfoList {
    raw: NonNull<*mut proj_sys::PROJ_CRS_INFO>,
    len: i32,
}

/// Caller-owned celestial-body list from the PROJ database.
pub(super) struct OwnedCelestialBodyList {
    raw: NonNull<*mut proj_sys::PROJ_CELESTIAL_BODY_INFO>,
    len: i32,
}

/// Caller-owned unit list from `proj_get_units_from_database`.
pub(super) struct OwnedUnitList {
    raw: NonNull<*mut proj_sys::PROJ_UNIT_INFO>,
    len: i32,
}

const _: () = {
    assert!(std::mem::size_of::<OwnedPj>() == std::mem::size_of::<*mut proj_sys::PJ>());
};

macro_rules! impl_proj_single_drop {
    ($ty:ty, $destroy:path) => {
        impl Drop for $ty {
            fn drop(&mut self) {
                // SAFETY: DOC-H. `raw` is the uniquely owned libPROJ allocation
                // transferred into this wrapper on the creating OS thread (the
                // guard is !Send via NonNull; CRS caches are TLS). Destroyed
                // exactly once here; PROJ invokes no Python callback.
                unsafe {
                    $destroy(self.raw.as_ptr());
                }
            }
        }
    };
}

// ProjContext Drop lives in context.rs (paired with path-includable owner).
impl_proj_single_drop!(ProjArea, proj_sys::proj_area_destroy);
impl_proj_single_drop!(
    ProjOperationFactoryContext,
    proj_sys::proj_operation_factory_context_destroy
);
impl_proj_single_drop!(
    ProjCrsListParameters,
    proj_sys::proj_get_crs_list_parameters_destroy
);

impl OwnedPj {
    /// Adopt a uniquely owned non-null `PJ` returned by PROJ.
    ///
    /// # Safety
    ///
    /// - `raw` is a non-null `PJ*` freshly returned by a PROJ create/get API
    ///   that transfers ownership to the caller.
    /// - It has not been destroyed and is not aliased by any other owner.
    /// - It remains on its creating OS thread until this wrapper drops
    ///   (NonNull makes the guard `!Send`).
    /// - Drop calls `proj_destroy` exactly once.
    pub(super) const unsafe fn from_owned(raw: NonNull<proj_sys::PJ>) -> Self {
        Self { raw }
    }

    /// Null-check then adopt. See [`Self::from_owned`] for the full contract.
    ///
    /// # Safety
    ///
    /// When `raw` is non-null, the same ownership/provenance/thread contract
    /// as [`Self::from_owned`] must hold.
    pub(super) unsafe fn try_from_owned(raw: *mut proj_sys::PJ) -> Option<Self> {
        NonNull::new(raw).map(|raw| {
            // SAFETY: non-null branch; caller upholds from_owned contract.
            unsafe { Self::from_owned(raw) }
        })
    }

    pub(super) const fn as_ptr(&self) -> *mut proj_sys::PJ {
        self.raw.as_ptr()
    }
}

impl Drop for OwnedPj {
    fn drop(&mut self) {
        // SAFETY: DOC-H. Unique ownership of a live PJ on the creating thread
        // (!Send); destroyed exactly once; PROJ invokes no Python callback.
        unsafe {
            proj_sys::proj_destroy(self.raw.as_ptr());
        }
    }
}

impl ProjObjList {
    /// Adopt a uniquely owned non-null object list returned by PROJ.
    ///
    /// # Safety
    ///
    /// `raw` is a non-null `PJ_OBJ_LIST*` returned by PROJ with ownership
    /// transfer, not yet destroyed, confined to the creating thread. Drop
    /// calls `proj_list_destroy` exactly once. Items from [`Self::get`] are
    /// independent owned references.
    pub(super) const unsafe fn from_owned(raw: NonNull<proj_sys::PJ_OBJ_LIST>) -> Self {
        Self { raw }
    }

    /// # Safety
    /// When non-null, same contract as [`Self::from_owned`].
    pub(super) unsafe fn try_from_owned(raw: *mut proj_sys::PJ_OBJ_LIST) -> Option<Self> {
        NonNull::new(raw).map(|raw| {
            // SAFETY: non-null; caller upholds from_owned.
            unsafe { Self::from_owned(raw) }
        })
    }

    pub(super) fn count(&self) -> i32 {
        // SAFETY: DOC-H. Self owns a live list on the creating thread; PROJ
        // invokes no Python callback.
        unsafe { proj_sys::proj_list_get_count(self.raw.as_ptr()) }
    }

    /// Fetch item `index` as an independently owned `PJ`.
    ///
    /// Returns `None` when `index` is out of range or PROJ returns null.
    pub(super) fn get(&self, context: &ProjContext, index: i32) -> Option<OwnedPj> {
        let count = self.count();
        if index < 0 || index >= count {
            return None;
        }
        // SAFETY: DOC-H. Context and list are live typed owners on this thread;
        // index is in `0..count`. PROJ returns a new owned reference (or null).
        let raw = unsafe { proj_sys::proj_list_get(context.as_ptr(), self.raw.as_ptr(), index) };
        // SAFETY: non-null returns are uniquely owned by the caller per PROJ.
        unsafe { OwnedPj::try_from_owned(raw) }
    }
}

impl Drop for ProjObjList {
    fn drop(&mut self) {
        // SAFETY: DOC-H. Unique list ownership on creating thread; destroy once.
        unsafe {
            proj_sys::proj_list_destroy(self.raw.as_ptr());
        }
    }
}

impl ProjIntList {
    pub(super) const fn empty() -> Self {
        Self { raw: None, len: 0 }
    }

    /// Adopt a PROJ-owned integer list of exactly `len` initialized entries.
    ///
    /// # Safety
    ///
    /// `raw` is a non-null list from e.g. `proj_identify` with at least `len`
    /// initialized `c_int` entries, not yet destroyed, creating-thread confined.
    /// Drop calls `proj_int_list_destroy` exactly once.
    pub(super) const unsafe fn from_owned(raw: NonNull<c_int>, len: usize) -> Self {
        Self {
            raw: Some(raw),
            len,
        }
    }

    /// # Safety
    /// When non-null, same contract as [`Self::from_owned`].
    pub(super) const unsafe fn try_from_owned(raw: *mut c_int, len: usize) -> Self {
        match NonNull::new(raw) {
            // SAFETY: non-null branch upholds from_owned.
            Some(raw) => unsafe { Self::from_owned(raw, len) },
            None => Self::empty(),
        }
    }

    pub(super) fn get(&self, index: usize) -> Option<c_int> {
        if index >= self.len {
            return None;
        }
        self.raw.map(|raw| {
            // SAFETY: construction guarantees `len` initialized entries and
            // the bounds check above proves `index < len`. Thread-confined
            // (!Send); PROJ invokes no Python callback.
            unsafe { *raw.as_ptr().add(index) }
        })
    }
}

impl Drop for ProjIntList {
    fn drop(&mut self) {
        if let Some(raw) = self.raw {
            // SAFETY: DOC-H. Unique ownership of the int list on creating thread.
            unsafe {
                proj_sys::proj_int_list_destroy(raw.as_ptr());
            }
        }
    }
}

impl ProjContext {
    pub(super) fn errno(&self) -> i32 {
        // SAFETY: DOC-H. Self is a live context on the creating thread.
        unsafe { proj_sys::proj_context_errno(self.as_ptr()) }
    }
}

impl ProjArea {
    pub(super) fn new(area: AreaOfInterest) -> Result<Self> {
        area.validate()?;
        // SAFETY: DOC-H. No pointer inputs; returns uniquely owned area or null.
        let raw = NonNull::new(unsafe { proj_sys::proj_area_create() }).ok_or_else(|| {
            CrsError::invalid("PROJ area-of-interest creation returned null".to_owned())
        })?;
        // SAFETY: DOC-H. Freshly owned area on creating thread; coordinates are
        // by-value; PROJ invokes no Python callback.
        unsafe {
            proj_sys::proj_area_set_bbox(
                raw.as_ptr(),
                area.west,
                area.south,
                area.east,
                area.north,
            );
        }
        Ok(Self { raw })
    }

    pub(super) const fn as_ptr(&self) -> *mut proj_sys::PJ_AREA {
        self.raw.as_ptr()
    }
}

impl ProjOperationFactoryContext {
    pub(super) fn new(context: &ProjContext, options: &TransformOptions) -> Result<Self> {
        options.validate()?;
        let authority = options.authority.as_deref().map(cstring).transpose()?;
        // SAFETY: DOC-H. Typed live context; authority CString lives for the
        // call; returns uniquely owned factory context or null.
        let raw = unsafe {
            proj_sys::proj_create_operation_factory_context(
                context.as_ptr(),
                authority
                    .as_ref()
                    .map_or(ptr::null(), |value| value.as_ptr()),
            )
        };
        let raw = NonNull::new(raw).ok_or_else(|| {
            CrsError::invalid("PROJ operation factory context creation returned null".to_owned())
        })?;
        let raw_ptr = raw.as_ptr();
        // SAFETY: DOC-H. Typed context + freshly owned factory on creating
        // thread; configuration setters take by-value / live factory only.
        unsafe {
            if let Some(area) = options.area_of_interest {
                proj_sys::proj_operation_factory_context_set_area_of_interest(
                    context.as_ptr(),
                    raw_ptr,
                    area.west,
                    area.south,
                    area.east,
                    area.north,
                );
            }
            if let Some(accuracy) = options.accuracy {
                proj_sys::proj_operation_factory_context_set_desired_accuracy(
                    context.as_ptr(),
                    raw_ptr,
                    accuracy.get(),
                );
            }
            if let Some(allow_ballpark) = options.allow_ballpark {
                proj_sys::proj_operation_factory_context_set_allow_ballpark_transformations(
                    context.as_ptr(),
                    raw_ptr,
                    i32::from(allow_ballpark),
                );
            }
            proj_sys::proj_operation_factory_context_set_discard_superseded(
                context.as_ptr(),
                raw_ptr,
                1,
            );
            proj_sys::proj_operation_factory_context_set_grid_availability_use(
                context.as_ptr(),
                raw_ptr,
                proj_sys::PROJ_GRID_AVAILABILITY_USE_PROJ_GRID_AVAILABILITY_IGNORED,
            );
            proj_sys::proj_operation_factory_context_set_spatial_criterion(
                context.as_ptr(),
                raw_ptr,
                proj_sys::PROJ_SPATIAL_CRITERION_PROJ_SPATIAL_CRITERION_PARTIAL_INTERSECTION,
            );
        }
        Ok(Self { raw })
    }

    pub(super) const fn as_ptr(&self) -> *const proj_sys::PJ_OPERATION_FACTORY_CONTEXT {
        self.raw.as_ptr()
    }
}

impl ProjCrsListParameters {
    pub(super) fn new(options: &CrsCatalogOptions) -> Result<Self> {
        options.validate()?;
        let kind = options
            .kind
            .map(CrsObjectKind::to_proj)
            .into_iter()
            .collect::<Vec<_>>();
        let celestial_body = options.celestial_body.as_deref().map(cstring).transpose()?;
        // SAFETY: DOC-H. No pointer inputs; uniquely owned params or null.
        let raw = NonNull::new(unsafe { proj_sys::proj_get_crs_list_parameters_create() })
            .ok_or_else(|| {
                CrsError::invalid("PROJ CRS list parameter creation returned null".to_owned())
            })?;
        let raw_ptr = raw.as_ptr();
        // SAFETY: DOC-H. Freshly owned exclusive struct on creating thread.
        // `_types` / `_celestial_body` retain every borrowed buffer until after
        // params destruction (field drop order: buffers after `raw` would be
        // wrong — they are declared after `raw` so Drop destroys raw first
        // while buffers still live; wait — Drop order is field declaration
        // order, first field first. We declare `raw` first so it drops first
        // while `_types`/`_celestial_body` still live — correct).
        unsafe {
            if kind.is_empty() {
                (*raw_ptr).types = ptr::null();
                (*raw_ptr).typesCount = 0;
            } else {
                (*raw_ptr).types = kind.as_ptr();
                (*raw_ptr).typesCount = kind.len();
            }
            if let Some(area) = options.area {
                (*raw_ptr).bbox_valid = 1;
                (*raw_ptr).crs_area_of_use_contains_bbox = i32::from(options.contains_area);
                (*raw_ptr).west_lon_degree = area.west;
                (*raw_ptr).south_lat_degree = area.south;
                (*raw_ptr).east_lon_degree = area.east;
                (*raw_ptr).north_lat_degree = area.north;
            }
            (*raw_ptr).allow_deprecated = i32::from(options.allow_deprecated);
            (*raw_ptr).celestial_body_name = celestial_body
                .as_ref()
                .map_or(ptr::null(), |value| value.as_ptr());
        }
        Ok(Self {
            raw,
            _types: kind,
            _celestial_body: celestial_body,
        })
    }

    pub(super) const fn as_ptr(&self) -> *const proj_sys::PROJ_CRS_LIST_PARAMETERS {
        self.raw.as_ptr()
    }
}

impl ProjTransformOptions {
    pub(super) fn new(options: &TransformOptions) -> Result<Self> {
        options.validate()?;
        let mut values = Vec::new();
        if let Some(authority) = &options.authority {
            values.push(cstring(format!("AUTHORITY={authority}"))?);
        }
        if let Some(accuracy) = options.accuracy {
            values.push(cstring(format!("ACCURACY={}", accuracy.get()))?);
        }
        if let Some(allow_ballpark) = options.allow_ballpark {
            values.push(cstring(format!(
                "ALLOW_BALLPARK={}",
                yes_no(allow_ballpark)
            ))?);
        }
        if let Some(only_best) = options.only_best {
            values.push(cstring(format!("ONLY_BEST={}", yes_no(only_best)))?);
        }
        if options.force_over {
            values.push(cstring("FORCE_OVER=YES")?);
        }

        let mut pointers = values
            .iter()
            .map(|value| value.as_ptr())
            .collect::<Vec<_>>();
        pointers.push(ptr::null());
        Ok(Self {
            _values: values,
            pointers,
        })
    }

    pub(super) const fn as_ptr(&self) -> *const *const c_char {
        if self.pointers.len() == 1 {
            ptr::null()
        } else {
            self.pointers.as_ptr()
        }
    }
}

impl OwnedProjStringList {
    /// # Safety
    /// `raw` is a non-null PROJ-owned null-terminated string list not yet
    /// destroyed; creating-thread confined; Drop calls
    /// `proj_string_list_destroy` exactly once.
    pub(super) const unsafe fn from_owned(raw: NonNull<*mut c_char>) -> Self {
        Self { raw }
    }

    /// # Safety
    /// When non-null, same contract as [`Self::from_owned`].
    pub(super) unsafe fn try_from_owned(raw: proj_sys::PROJ_STRING_LIST) -> Option<Self> {
        NonNull::new(raw).map(|raw| {
            // SAFETY: non-null; caller upholds from_owned.
            unsafe { Self::from_owned(raw) }
        })
    }

    pub(super) fn to_strings(&self) -> Vec<String> {
        let mut values = Vec::new();
        let mut index = 0_usize;
        loop {
            // SAFETY: DOC-H + LIST. PROJ string lists are null-terminated arrays
            // of C strings owned by this guard until Drop. Each entry is copied
            // before return. Thread-confined; no Python callback.
            let value = unsafe { *self.raw.as_ptr().add(index) };
            if value.is_null() {
                break;
            }
            if let Some(value) = proj_c_string!(value) {
                values.push(value);
            }
            index += 1;
        }
        values
    }
}

impl Drop for OwnedProjStringList {
    fn drop(&mut self) {
        // SAFETY: DOC-H. Unique ownership; destroy exactly once on creating thread.
        unsafe {
            proj_sys::proj_string_list_destroy(self.raw.as_ptr());
        }
    }
}

impl OwnedCrsInfoList {
    /// # Safety
    /// `raw` points to `len` PROJ-owned `PROJ_CRS_INFO*` entries (null entries
    /// allowed); not yet destroyed; creating-thread confined.
    pub(super) const unsafe fn from_owned(
        raw: NonNull<*mut proj_sys::PROJ_CRS_INFO>,
        len: i32,
    ) -> Self {
        Self { raw, len }
    }

    pub(super) fn into_catalog_infos(self) -> Vec<CrsCatalogInfo> {
        let mut items = Vec::with_capacity(self.len.max(0) as usize);
        for index in 0..self.len {
            // SAFETY: LIST(n). Self owns `len` entries until Drop.
            let info = unsafe { *self.raw.as_ptr().add(index as usize) };
            if info.is_null() {
                continue;
            }
            // SAFETY: non-null entry is a live PROJ_CRS_INFO owned by the list.
            let info = unsafe { &*info };
            let authority = proj_c_string!(info.auth_name);
            let code = proj_c_string!(info.code);
            let crs = match (&authority, &code) {
                (Some(authority), Some(code)) => format!("{authority}:{code}"),
                _ => proj_c_string!(info.name).unwrap_or_else(|| "unknown".to_owned()),
            };
            items.push(CrsCatalogInfo {
                crs,
                authority,
                code,
                name: proj_c_string!(info.name),
                kind: crs_type_name(info.type_),
                deprecated: info.deprecated != 0,
                area_of_use: (info.bbox_valid != 0).then(|| AreaOfUse {
                    west: info.west_lon_degree,
                    south: info.south_lat_degree,
                    east: info.east_lon_degree,
                    north: info.north_lat_degree,
                    name: proj_c_string!(info.area_name),
                }),
                projection_method_name: proj_c_string!(info.projection_method_name),
                celestial_body: proj_c_string!(info.celestial_body_name),
            });
        }
        items
    }
}

impl Drop for OwnedCrsInfoList {
    fn drop(&mut self) {
        // SAFETY: DOC-H. Unique CRS-info list ownership; destroy once.
        unsafe {
            proj_sys::proj_crs_info_list_destroy(self.raw.as_ptr());
        }
    }
}

impl OwnedCelestialBodyList {
    /// # Safety
    /// `raw` points to `len` PROJ-owned celestial-body entries; not destroyed;
    /// creating-thread confined.
    pub(super) const unsafe fn from_owned(
        raw: NonNull<*mut proj_sys::PROJ_CELESTIAL_BODY_INFO>,
        len: i32,
    ) -> Self {
        Self { raw, len }
    }

    pub(super) fn into_infos(self) -> Vec<CelestialBodyInfo> {
        let mut items = Vec::with_capacity(self.len.max(0) as usize);
        for index in 0..self.len {
            // SAFETY: LIST(n). Self owns `len` entries until Drop.
            let info = unsafe { *self.raw.as_ptr().add(index as usize) };
            if info.is_null() {
                continue;
            }
            // SAFETY: live PROJ_CELESTIAL_BODY_INFO owned by the list.
            let info = unsafe { &*info };
            items.push(CelestialBodyInfo {
                authority: proj_c_string!(info.auth_name),
                name: proj_c_string!(info.name),
            });
        }
        items
    }
}

impl Drop for OwnedCelestialBodyList {
    fn drop(&mut self) {
        // SAFETY: DOC-H. Unique list ownership; destroy once.
        unsafe {
            proj_sys::proj_celestial_body_list_destroy(self.raw.as_ptr());
        }
    }
}

impl OwnedUnitList {
    /// # Safety
    /// `raw` points to `len` PROJ-owned unit-info entries; not destroyed;
    /// creating-thread confined.
    pub(super) const unsafe fn from_owned(
        raw: NonNull<*mut proj_sys::PROJ_UNIT_INFO>,
        len: i32,
    ) -> Self {
        Self { raw, len }
    }

    pub(super) fn into_units(self) -> Vec<UnitInfo> {
        let mut units = Vec::with_capacity(self.len.max(0) as usize);
        for index in 0..self.len {
            // SAFETY: LIST(n). Self owns `len` entries until Drop.
            let info = unsafe { *self.raw.as_ptr().add(index as usize) };
            if info.is_null() {
                continue;
            }
            // SAFETY: live PROJ_UNIT_INFO owned by the list.
            let info = unsafe { &*info };
            units.push(UnitInfo {
                authority: proj_c_string!(info.auth_name),
                code: proj_c_string!(info.code),
                name: proj_c_string!(info.name),
                category: proj_c_string!(info.category),
                conversion_factor: info.conv_factor,
                proj_short_name: proj_c_string!(info.proj_short_name),
            });
        }
        units
    }
}

impl Drop for OwnedUnitList {
    fn drop(&mut self) {
        // SAFETY: DOC-H. Unique unit-list ownership; destroy once.
        unsafe {
            proj_sys::proj_unit_list_destroy(self.raw.as_ptr());
        }
    }
}

pub(crate) struct ProjPipeline {
    pub(super) transform: OwnedPj,
    pub(super) context: ProjContext,
}

// `ProjContext` lives in `context.rs` (path-includable type-level keystone).

pub(super) struct ProjArea {
    raw: NonNull<proj_sys::PJ_AREA>,
}

pub(super) struct ProjTransformOptions {
    pub _values: Vec<CString>,
    pub pointers: Vec<*const c_char>,
}

pub(super) struct ProjOperationFactoryContext {
    raw: NonNull<proj_sys::PJ_OPERATION_FACTORY_CONTEXT>,
}

pub(super) struct ProjCrsListParameters {
    raw: NonNull<proj_sys::PROJ_CRS_LIST_PARAMETERS>,
    pub _types: Vec<proj_sys::PJ_TYPE>,
    pub _celestial_body: Option<CString>,
}

pub(super) struct ProjObject {
    pub(super) object: OwnedPj,
    pub(super) context: ProjContext,
}

pub(super) const fn yes_no(value: bool) -> &'static str {
    if value { "YES" } else { "NO" }
}

// `with_proj_context` lives in `context.rs` (same module as `ProjContext`).

/// Collect a PROJ database string list into owned Rust strings.
///
/// `list` is the RAII guard from the FFI acquisition site (or `None` when PROJ
/// returned null). On `None`, inspects context errno (empty vs error). A safe
/// caller cannot supply a dangling or double-owned raw `PROJ_STRING_LIST`.
pub(super) fn take_proj_string_list(
    context: &ProjContext,
    list: Option<OwnedProjStringList>,
) -> Result<Vec<String>> {
    let Some(list) = list else {
        return if context.errno() == 0 {
            Ok(Vec::new())
        } else {
            Err(CrsError::invalid(proj_context_error_message(context)))
        };
    };
    Ok(list.to_strings())
}

pub(super) fn proj_error_message(error: i32) -> String {
    if error == 0 {
        return "PROJ transformed fewer coordinates than requested".to_owned();
    }
    // SAFETY: DOC-H / STATIC. `proj_errno_string` returns null or immutable
    // process-static error text; copied immediately. No thread-local handle;
    // PROJ invokes no Python callback.
    unsafe {
        let message = proj_sys::proj_errno_string(error);
        if message.is_null() {
            return format!("PROJ error code {error}");
        }
        CStr::from_ptr(message).to_string_lossy().into_owned()
    }
}

pub(super) fn proj_context_error_message(context: &ProjContext) -> String {
    // SAFETY: DOC-H. Typed live context on creating thread; PROJ returns
    // static null-terminated error text for known codes, copied immediately.
    unsafe {
        let error = proj_sys::proj_context_errno(context.as_ptr());
        if error == 0 {
            return "PROJ returned null".to_owned();
        }
        let message = proj_sys::proj_context_errno_string(context.as_ptr(), error);
        if message.is_null() {
            return format!("PROJ error code {error}");
        }
        let message = CStr::from_ptr(message).to_string_lossy().into_owned();
        if message.starts_with("Unknown error") {
            return "PROJ could not resolve CRS".to_owned();
        }
        message
    }
}

/// Copy a PROJ-returned C string into owned Rust storage.
///
/// # Safety
///
/// `value` is null or a NUL-terminated C string that remains live for this
/// call (object lifetime, list-entry lifetime, or process-static catalog).
/// Call only at a provenance-owning FFI boundary; never with a forgeable
/// arbitrary pointer.
pub(crate) unsafe fn copy_proj_c_string(value: *const std::os::raw::c_char) -> Option<String> {
    if value.is_null() {
        return None;
    }
    // SAFETY: caller guarantees a live NUL-terminated C string for this call.
    Some(
        unsafe { CStr::from_ptr(value) }
            .to_string_lossy()
            .into_owned(),
    )
}

pub(super) fn optional_c_string(value: Option<&str>) -> Result<Option<CString>> {
    value.map(cstring).transpose()
}

/// First entry of a process-static `char**` descriptor (operation catalog).
///
/// # Safety
///
/// `value` is null or points into PROJ's process-static operation metadata
/// whose first entry is a display description string (or null).
pub(super) unsafe fn first_static_string_from_ptr(value: *const *const c_char) -> Option<String> {
    if value.is_null() {
        return None;
    }
    proj_c_string!(*value)
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use super::*;
    use crate::crs::context::with_proj_context;

    #[test]
    fn mandatory_handles_are_non_null_and_pointer_sized() {
        assert_eq!(
            std::mem::size_of::<OwnedPj>(),
            std::mem::size_of::<*mut proj_sys::PJ>()
        );
        assert_eq!(
            std::mem::size_of::<ProjContext>(),
            std::mem::size_of::<*mut proj_sys::PJ_CONTEXT>()
        );
        let context = ProjContext::new().expect("bundled PROJ creates a context");
        assert!(!context.as_ptr().is_null());
    }

    #[test]
    fn nullable_int_list_is_an_explicit_empty_option() {
        let list = ProjIntList::empty();
        assert!(list.raw.is_none());
        assert_eq!(list.get(0), None);
    }

    #[test]
    fn int_list_get_checks_length_before_pointer_arithmetic() {
        let list = std::mem::ManuallyDrop::new(ProjIntList {
            raw: Some(NonNull::dangling()),
            len: 0,
        });
        assert_eq!(list.get(0), None);
    }

    #[test]
    fn object_list_get_takes_typed_context() {
        let get: fn(&ProjObjList, &ProjContext, i32) -> Option<OwnedPj> = ProjObjList::get;
        let _ = get;
    }

    #[test]
    fn with_proj_context_lends_typed_borrow() {
        let value = with_proj_context(|ctx| {
            assert!(!ctx.as_ptr().is_null());
            7
        })
        .expect("context");
        assert_eq!(value, 7);
    }

    /// Holding an owned PROJ string list across a panic must Drop (destroy)
    /// it; afterwards PROJ catalog queries still succeed (no double-free /
    /// corruption from a leaked or double-destroyed list).
    #[test]
    fn owned_string_list_releases_on_panic() {
        let panicked = catch_unwind(AssertUnwindSafe(|| {
            let context = ProjContext::new().expect("context");
            // SAFETY: DOC-H. Live context; PROJ returns caller-owned string list.
            let raw = unsafe { proj_sys::proj_get_authorities_from_database(context.as_ptr()) };
            // SAFETY: non-null path transfers unique ownership to the guard.
            let list = unsafe { OwnedProjStringList::try_from_owned(raw) }
                .expect("authorities list from bundled PROJ");
            let _hold = list;
            panic!("force unwind while list guard is live");
        }));
        assert!(panicked.is_err(), "panic must propagate for Drop to run");
        // Post-unwind: a fresh list still works → prior Drop destroyed cleanly.
        let authorities = with_proj_context(|ctx| {
            // SAFETY: DOC-H. Live typed context; uniquely owned list adopted at
            // the FFI boundary before the safe consumer runs.
            let list = unsafe {
                OwnedProjStringList::try_from_owned(proj_sys::proj_get_authorities_from_database(
                    ctx.as_ptr(),
                ))
            };
            take_proj_string_list(ctx, list)
        })
        .expect("context")
        .expect("authorities after panic cleanup");
        assert!(
            authorities.iter().any(|a| a == "EPSG"),
            "EPSG must remain after unwind cleanup, got {authorities:?}"
        );
    }
}
