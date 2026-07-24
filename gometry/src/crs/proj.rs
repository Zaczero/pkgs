#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! libPROJ FFI handle types impls — RAII `Drop` and small constructors for
//! `ProjContext`/`ProjArea`/`ProjOperationFactoryContext`/
//! `ProjCrsListParameters`/ `ProjTransformOptions`.
//!
//! Handle structs live in the parent `crs` module; reached via `use super::*`.

use std::marker::PhantomData;
use std::os::raw::c_int;
use std::ptr::NonNull;

use super::*;
use crate::error::Result;

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

/// Non-owning `PJ` reference. There is intentionally no `Drop` implementation:
/// the owner remains responsible for destroying the referenced object.
pub(super) struct BorrowedPj<'owner> {
    raw: NonNull<proj_sys::PJ>,
    _owner: PhantomData<&'owner proj_sys::PJ>,
}

const _: () = {
    assert!(std::mem::size_of::<OwnedPj>() == std::mem::size_of::<*mut proj_sys::PJ>());
};

macro_rules! impl_proj_single_drop {
    ($ty:ty, $destroy:path) => {
        impl Drop for $ty {
            fn drop(&mut self) {
                // SAFETY: raw is owned by this wrapper and destroyed exactly once
                // here.
                unsafe {
                    $destroy(self.raw.as_ptr());
                }
            }
        }
    };
}

impl_proj_single_drop!(ProjContext, proj_sys::proj_context_destroy);
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
    /// # Safety
    ///
    /// `raw` must be a non-null `PJ` pointer returned by PROJ and not yet
    /// destroyed.
    pub(super) const unsafe fn from_owned(raw: *mut proj_sys::PJ) -> Self {
        Self {
            raw: {
                // SAFETY: upheld by the caller contract above.
                unsafe { NonNull::new_unchecked(raw) }
            },
        }
    }

    pub(super) const fn as_ptr(&self) -> *mut proj_sys::PJ {
        self.raw.as_ptr()
    }
}

impl Drop for OwnedPj {
    fn drop(&mut self) {
        // SAFETY: raw is owned by this wrapper and destroyed exactly once here.
        unsafe {
            proj_sys::proj_destroy(self.raw.as_ptr());
        }
    }
}

impl ProjObjList {
    /// # Safety
    ///
    /// `raw` must be a non-null list pointer returned by PROJ and not yet
    /// destroyed.
    pub(super) const unsafe fn from_owned(raw: *mut proj_sys::PJ_OBJ_LIST) -> Self {
        Self {
            raw: {
                // SAFETY: upheld by the caller contract above.
                unsafe { NonNull::new_unchecked(raw) }
            },
        }
    }

    pub(super) fn count(&self) -> i32 {
        // SAFETY: self owns a valid PROJ object list for the duration of `self`.
        unsafe { proj_sys::proj_list_get_count(self.raw.as_ptr()) }
    }

    pub(super) fn get(&self, context: *mut proj_sys::PJ_CONTEXT, index: i32) -> Option<OwnedPj> {
        // SAFETY: self owns the list and index is within the count reported by
        // PROJ. The returned object is a new owned reference which PROJ requires
        // the caller to release with `proj_destroy`.
        let raw = unsafe { proj_sys::proj_list_get(context, self.raw.as_ptr(), index) };
        NonNull::new(raw).map(|raw| OwnedPj { raw })
    }
}

impl Drop for ProjObjList {
    fn drop(&mut self) {
        // SAFETY: raw is owned by this wrapper and destroyed exactly once here.
        unsafe {
            proj_sys::proj_list_destroy(self.raw.as_ptr());
        }
    }
}

impl ProjIntList {
    pub(super) const fn empty() -> Self {
        Self { raw: None, len: 0 }
    }

    /// # Safety
    ///
    /// `raw` must be a non-null PROJ-owned integer list that has not been
    /// destroyed and contains at least `len` initialized entries. Ownership is
    /// transferred to this wrapper; use [`Self::empty`] for a null result.
    pub(super) const unsafe fn from_owned(raw: *mut c_int, len: usize) -> Self {
        Self {
            raw: Some({
                // SAFETY: upheld by the caller contract above.
                unsafe { NonNull::new_unchecked(raw) }
            }),
            len,
        }
    }

    pub(super) fn get(&self, index: usize) -> Option<c_int> {
        if index >= self.len {
            return None;
        }
        self.raw.map(|raw| {
            // SAFETY: construction guarantees `len` initialized entries and
            // the explicit bounds check above proves `index < len`.
            unsafe { *raw.as_ptr().add(index) }
        })
    }
}

impl Drop for ProjIntList {
    fn drop(&mut self) {
        if let Some(raw) = self.raw {
            // SAFETY: raw is owned by this wrapper and destroyed exactly once here.
            unsafe {
                proj_sys::proj_int_list_destroy(raw.as_ptr());
            }
        }
    }
}

impl BorrowedPj<'_> {
    /// # Safety
    ///
    /// `raw` must be either null or a valid PJ pointer whose owner outlives the
    /// returned borrow.
    pub(super) unsafe fn from_borrowed(raw: *mut proj_sys::PJ) -> Option<Self> {
        NonNull::new(raw).map(|raw| Self {
            raw,
            _owner: PhantomData,
        })
    }

    pub(super) const fn as_ptr(&self) -> *mut proj_sys::PJ {
        self.raw.as_ptr()
    }
}

impl ProjContext {
    pub(super) fn new() -> Result<Self> {
        Ok(Self {
            raw: create_proj_context()?,
        })
    }

    pub(super) const fn as_ptr(&self) -> *mut proj_sys::PJ_CONTEXT {
        self.raw.as_ptr()
    }
}

impl ProjArea {
    pub(super) fn new(area: AreaOfInterest) -> Result<Self> {
        area.validate()?;
        // SAFETY: PROJ returns an owned area pointer or null.
        let raw = NonNull::new(unsafe { proj_sys::proj_area_create() }).ok_or_else(|| {
            CrsError::invalid("PROJ area-of-interest creation returned null".to_owned())
        })?;
        // SAFETY: raw is a valid PROJ area pointer created above.
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
    pub(super) fn new(
        context: *mut proj_sys::PJ_CONTEXT,
        options: &TransformOptions,
    ) -> Result<Self> {
        options.validate()?;
        let authority = options.authority.as_deref().map(cstring).transpose()?;
        // SAFETY: context is valid and authority, when present, is a valid C string
        // for the duration of the call. PROJ returns an owned factory context.
        let raw = unsafe {
            proj_sys::proj_create_operation_factory_context(
                context,
                authority
                    .as_ref()
                    .map_or(ptr::null(), |value| value.as_ptr()),
            )
        };
        let raw = NonNull::new(raw).ok_or_else(|| {
            CrsError::invalid("PROJ operation factory context creation returned null".to_owned())
        })?;
        let raw_ptr = raw.as_ptr();
        // SAFETY: raw/context are valid for these factory configuration calls.
        unsafe {
            if let Some(area) = options.area_of_interest {
                proj_sys::proj_operation_factory_context_set_area_of_interest(
                    context, raw_ptr, area.west, area.south, area.east, area.north,
                );
            }
            if let Some(accuracy) = options.accuracy {
                proj_sys::proj_operation_factory_context_set_desired_accuracy(
                    context,
                    raw_ptr,
                    accuracy.get(),
                );
            }
            if let Some(allow_ballpark) = options.allow_ballpark {
                proj_sys::proj_operation_factory_context_set_allow_ballpark_transformations(
                    context,
                    raw_ptr,
                    i32::from(allow_ballpark),
                );
            }
            proj_sys::proj_operation_factory_context_set_discard_superseded(context, raw_ptr, 1);
            proj_sys::proj_operation_factory_context_set_grid_availability_use(
                context,
                raw_ptr,
                proj_sys::PROJ_GRID_AVAILABILITY_USE_PROJ_GRID_AVAILABILITY_IGNORED,
            );
            proj_sys::proj_operation_factory_context_set_spatial_criterion(
                context,
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
        // SAFETY: PROJ returns owned list parameters or null.
        let raw = NonNull::new(unsafe { proj_sys::proj_get_crs_list_parameters_create() })
            .ok_or_else(|| {
                CrsError::invalid("PROJ CRS list parameter creation returned null".to_owned())
            })?;
        let raw_ptr = raw.as_ptr();
        // SAFETY: raw is valid and owned by this wrapper. The type and celestial
        // body buffers are stored in the wrapper so their pointers stay valid for
        // the list call.
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

pub(crate) struct ProjPipeline {
    pub(super) transform: OwnedPj,
    pub(super) context: ProjContext,
}

pub(super) struct ProjContext {
    pub raw: NonNull<proj_sys::PJ_CONTEXT>,
}

pub(super) struct ProjArea {
    pub raw: NonNull<proj_sys::PJ_AREA>,
}

pub(super) struct ProjTransformOptions {
    pub _values: Vec<CString>,
    pub pointers: Vec<*const c_char>,
}

pub(super) struct ProjOperationFactoryContext {
    pub raw: NonNull<proj_sys::PJ_OPERATION_FACTORY_CONTEXT>,
}

pub(super) struct ProjCrsListParameters {
    pub raw: NonNull<proj_sys::PROJ_CRS_LIST_PARAMETERS>,
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

pub(super) fn with_proj_context<R>(
    operation: impl FnOnce(*mut proj_sys::PJ_CONTEXT) -> R,
) -> Result<R> {
    ensure_thread_caches_current();
    let context = ProjContext::new()?;
    Ok(operation(context.as_ptr()))
}

pub(super) fn proj_error_message(error: i32) -> String {
    if error == 0 {
        return "PROJ transformed fewer coordinates than requested".to_owned();
    }
    // SAFETY: proj_errno_string returns a null-terminated static string for known
    // PROJ errors, or null for unknown error codes.
    unsafe {
        let message = proj_sys::proj_errno_string(error);
        if message.is_null() {
            return format!("PROJ error code {error}");
        }
        CStr::from_ptr(message).to_string_lossy().into_owned()
    }
}

pub(super) fn proj_context_error_message(context: *mut proj_sys::PJ_CONTEXT) -> String {
    // SAFETY: context is a valid PROJ context for this call path and PROJ returns
    // static null-terminated error text for known error codes.
    unsafe {
        let error = proj_sys::proj_context_errno(context);
        if error == 0 {
            return "PROJ returned null".to_owned();
        }
        let message = proj_sys::proj_context_errno_string(context, error);
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

pub(super) fn string_from_ptr(value: *const std::os::raw::c_char) -> Option<String> {
    if value.is_null() {
        return None;
    }
    // SAFETY: PROJ metadata/export functions return null-terminated strings that
    // remain valid for the lifetime documented by PROJ.
    Some(
        unsafe { CStr::from_ptr(value) }
            .to_string_lossy()
            .into_owned(),
    )
}

pub(super) fn optional_c_string(value: Option<&str>) -> Result<Option<CString>> {
    value.map(cstring).transpose()
}

pub(super) fn first_static_string_from_ptr(value: *const *const c_char) -> Option<String> {
    if value.is_null() {
        return None;
    }
    // SAFETY: PROJ operation metadata exposes a descriptor pointer whose first
    // entry is the display description for this operation.
    string_from_ptr(unsafe { *value })
}

pub(super) fn string_list_from_ptr(
    context: *mut proj_sys::PJ_CONTEXT,
    list: proj_sys::PROJ_STRING_LIST,
) -> Result<Vec<String>> {
    if list.is_null() {
        // SAFETY: context is valid. PROJ returns a nonzero context errno for
        // database failures; unknown empty authority lookups return null without
        // setting an error.
        let error = unsafe { proj_sys::proj_context_errno(context) };
        return if error == 0 {
            Ok(Vec::new())
        } else {
            Err(CrsError::invalid(proj_context_error_message(context)))
        };
    }
    let mut values = Vec::new();
    let mut index = 0;
    loop {
        // SAFETY: PROJ string lists are null-terminated arrays. Each non-null
        // entry is copied immediately, and the list is destroyed before return.
        let value = unsafe { *list.add(index) };
        if value.is_null() {
            break;
        }
        if let Some(value) = string_from_ptr(value) {
            values.push(value);
        }
        index += 1;
    }
    // SAFETY: list was allocated by PROJ and is destroyed exactly once here.
    unsafe {
        proj_sys::proj_string_list_destroy(list);
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn object_list_items_are_independent_owned_guards() {
        // PROJ's `proj_list_get` transfers a new reference that must be released
        // with `proj_destroy`; keep that ownership visible in the wrapper type.
        let get: fn(&ProjObjList, *mut proj_sys::PJ_CONTEXT, i32) -> Option<OwnedPj> =
            ProjObjList::get;
        let _ = get;
    }
}
