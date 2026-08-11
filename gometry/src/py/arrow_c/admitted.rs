//! Design-1 owned Arrow admission: move shells → snapshot → release → decode only owned.
//!
//! No `ArrowSchemaPtr` / `ArrowArrayPtr` / `ValidatedArrowArray` / `ImportedCapsules`
//! and no producer-backed `Send`/`Sync` buffer wrappers. Intermediate shells are
//! non-`Send` RAII (`MovedArrowShell`); only owned trees escape admission.
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::too_many_lines,
    clippy::use_self,
    clippy::type_complexity,
    clippy::missing_const_for_fn,
    reason = "owned admission capture is intentionally explicit; helpers land ahead of full native migration"
)]

use std::ffi::CStr;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::Arc;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

use crate::py::arrow_c::import::schema_metadata_pairs_owned;
use crate::py::arrow_c::native::{drop_moved_arrow, move_arrow_shell};
use crate::py::arrow_c::{ArrowReleaseSlot, ArrowSchema};

/// Geometry decoding selected from an owned Arrow schema tree.
#[derive(Clone, Debug)]
pub(crate) struct ClassifiedGeometrySchema {
    pub(crate) encoding: crate::py::geoarrow::GeometryEncoding,
    pub(crate) wkb_offset_width: crate::py::arrow::WkbOffsetWidth,
    pub(crate) crs: Option<String>,
    pub(crate) epoch: Option<f64>,
    /// The geometry child when the root is a table/record-batch struct.
    pub(crate) struct_child: Option<usize>,
}

/// Maximum selected Arrow schema nesting admitted by the recursive snapshot.
///
/// Geometry storage itself is shallow (at most the GeoArrow list nesting plus
/// its coordinate struct); this bound protects the native stack from an
/// arbitrarily nested selected executable-provider schema. Non-selected table
/// siblings remain shallow-captured and never consume this budget.
const MAX_SELECTED_SCHEMA_CAPTURE_DEPTH: usize = 128;

/// One-shot consumer-owned Arrow base shell. Neither `Send` nor `Sync`.
pub(crate) struct MovedArrowShell<T: ArrowReleaseSlot> {
    ptr: NonNull<T>,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl<T: ArrowReleaseSlot> MovedArrowShell<T> {
    /// Move a live producer shell into a non-`Send` consumer owner.
    ///
    /// # Safety
    ///
    /// `source` must be a live ABI-conforming base structure. After return the
    /// producer source has a null release slot; this owner releases exactly once
    /// on drop. No nested provider call or Python detach until drop.
    pub(crate) unsafe fn take(source: *mut T) -> PyResult<Self> {
        if source.is_null() {
            return Err(PyTypeError::new_err("Arrow base structure pointer is null"));
        }
        // SAFETY: probe release slot before moving; already-released shells
        // must not be re-admitted (double-move / use-after-release).
        let release = unsafe { *T::release_slot(source) };
        if release.is_none() {
            return Err(PyTypeError::new_err(
                "Arrow base structure is already released",
            ));
        }
        // SAFETY: caller guarantees a live base structure for the move.
        let moved = unsafe { move_arrow_shell(source) };
        if moved.is_null() {
            return Err(PyTypeError::new_err("Arrow move produced a null shell"));
        }
        // SAFETY: move_arrow_shell returns a non-null owned allocation.
        let ptr = unsafe { NonNull::new_unchecked(moved) };
        Ok(Self {
            ptr,
            _not_send_sync: PhantomData,
        })
    }

    pub(crate) const fn as_ptr(&self) -> *const T {
        self.ptr.as_ptr()
    }
}

impl<T: ArrowReleaseSlot> Drop for MovedArrowShell<T> {
    fn drop(&mut self) {
        // SAFETY: unique ownership of the moved shell.
        unsafe { drop_moved_arrow(self.ptr.as_ptr()) }
    }
}

/// Owned schema tree — no producer pointers after capture.
#[derive(Clone, Debug)]
pub(crate) struct AdmittedArrowSchema {
    pub(crate) format: Arc<str>,
    pub(crate) name: Arc<str>,
    pub(crate) metadata: Arc<[(Box<[u8]>, Box<[u8]>)]>,
    pub(crate) dictionary_present: bool,
    pub(crate) children: Arc<[AdmittedArrowSchema]>,
}

impl AdmittedArrowSchema {
    /// Snapshot the schema needed for geometry admission from a moved shell.
    ///
    /// For a bare geometry array this is a full recursive capture of that tree.
    /// For a struct/table root, only the **selected geometry child's** subtree is
    /// captured deeply; sibling columns are shallow (name/format/metadata only).
    /// That keeps an irrelevant 50k-deep Struct sibling off the consumer stack
    /// (C12) while still allowing column selection by name/extension/format.
    ///
    /// # Safety
    ///
    /// See module contract: shell pins producer allocation; no writers during
    /// capture; no nested provider calls; only owned result escapes.
    pub(crate) unsafe fn capture(owner: &MovedArrowShell<ArrowSchema>) -> PyResult<Self> {
        // SAFETY: shell is live for this call; owner witnesses the root pointer.
        unsafe { Self::capture_for_geometry_admission(owner, owner.as_ptr()) }
    }

    /// Root-aware capture: deep only along the geometry selection path.
    ///
    /// # Safety
    ///
    /// Same as [`Self::capture`].
    unsafe fn capture_for_geometry_admission(
        owner: &MovedArrowShell<ArrowSchema>,
        schema: *const ArrowSchema,
    ) -> PyResult<Self> {
        if schema.is_null() {
            return Err(PyTypeError::new_err("Arrow schema pointer is null"));
        }
        // SAFETY: shell pins root for the format/metadata probe.
        let raw = unsafe { &*schema };
        let format = owned_c_str(raw.format, "Arrow schema format")?;
        // SAFETY: metadata live under shell for this copy only.
        let root_meta = unsafe { owned_schema_metadata(raw) }?;
        let root_has_extension = root_meta
            .iter()
            .any(|(k, _)| k.as_ref() == b"ARROW:extension:name");
        // Geometry array first (same order as classify): extension-tagged roots
        // include GeoArrow struct points (`+s` of x/y) and nested list types —
        // those are NOT table wrappers and must deep-capture wholly. Binary WKB
        // formats are also bare geometry.
        let is_table_wrapper = format.as_ref() == "+s" && !root_has_extension;
        if !is_table_wrapper {
            // SAFETY: shell pins this geometry root for deep capture.
            return unsafe { Self::capture_node(owner, schema, CaptureDepth::Deep, 0) };
        }
        // Table/struct root without extension: shallow-capture every top-level
        // child for selection, then deep-capture only the selected geometry.
        if raw.n_children < 0 {
            return Err(PyTypeError::new_err(
                "Arrow schema n_children is negative or too large",
            ));
        }
        let n = usize::try_from(raw.n_children).map_err(|_| {
            PyTypeError::new_err("Arrow schema n_children is negative or too large")
        })?;
        if n == 0 {
            return Err(PyTypeError::new_err(
                "Arrow table or record batch has no columns",
            ));
        }
        if raw.children.is_null() {
            return Err(PyTypeError::new_err(
                "Arrow schema children pointer is null while n_children > 0",
            ));
        }
        let mut shallow_children = Vec::new();
        shallow_children.try_reserve(n).map_err(|_| {
            PyTypeError::new_err("Arrow schema child count is too large to allocate")
        })?;
        for i in 0..n {
            // SAFETY: index in range; shell pins child table.
            let child_ptr = unsafe { *raw.children.add(i) };
            if child_ptr.is_null() {
                return Err(PyTypeError::new_err("Arrow child schema is null"));
            }
            // SAFETY: child_ptr live under shell; shallow capture only.
            shallow_children
                .push(unsafe { Self::capture_node(owner, child_ptr, CaptureDepth::Shallow, 0) }?);
        }
        let selected = select_geometry_child_index_from_shallow(&shallow_children)?;
        let mut children = Vec::with_capacity(n);
        for (i, shallow) in shallow_children.into_iter().enumerate() {
            if i == selected {
                // SAFETY: same child table as above; deep-capture geometry only.
                let child_ptr = unsafe { *raw.children.add(i) };
                // SAFETY: child_ptr live under shell; deep geometry subtree.
                children
                    .push(unsafe { Self::capture_node(owner, child_ptr, CaptureDepth::Deep, 0) }?);
            } else {
                // Already-built shallow sibling (no nested payload).
                children.push(shallow);
            }
        }
        let name = if raw.name.is_null() {
            Arc::from("")
        } else {
            owned_c_str(raw.name, "Arrow field name")?
        };
        // SAFETY: metadata live under shell for this copy only.
        let metadata = unsafe { owned_schema_metadata(raw) }?;
        Ok(Self {
            format,
            name,
            metadata: Arc::from(metadata),
            dictionary_present: !raw.dictionary.is_null(),
            children: Arc::from(children),
        })
    }

    /// Owner-witnessed schema snapshot with explicit recursion depth.
    ///
    /// # Safety
    ///
    /// Same as [`Self::capture`] for the duration of this call only. `owner`
    /// must pin the root schema tree that contains `schema` (or `schema` itself).
    unsafe fn capture_node(
        owner: &MovedArrowShell<ArrowSchema>,
        schema: *const ArrowSchema,
        depth: CaptureDepth,
        nesting: usize,
    ) -> PyResult<Self> {
        if nesting >= MAX_SELECTED_SCHEMA_CAPTURE_DEPTH {
            return Err(PyTypeError::new_err(format!(
                "Arrow selected schema nesting exceeds maximum depth of {MAX_SELECTED_SCHEMA_CAPTURE_DEPTH}"
            )));
        }
        if schema.is_null() {
            return Err(PyTypeError::new_err("Arrow schema pointer is null"));
        }
        // Owner witness: shell must still be live for this pointer's tree.
        debug_assert!(
            !owner.as_ptr().is_null(),
            "MovedArrowShell always holds a non-null base"
        );
        // SAFETY: caller + shell pin this schema for the capture.
        let schema = unsafe { &*schema };
        // Capture sibling formats verbatim, including LargeList `+L` so its
        // checked i64 offsets remain available to selected-geometry admission.
        let format = owned_c_str(schema.format, "Arrow schema format")?;
        let name = if schema.name.is_null() {
            Arc::from("")
        } else {
            owned_c_str(schema.name, "Arrow field name")?
        };
        // SAFETY: metadata blob is live under the owner shell for this copy only.
        let metadata = unsafe { owned_schema_metadata(schema) }?;
        let dictionary_present = !schema.dictionary.is_null();
        if schema.n_children < 0 {
            return Err(PyTypeError::new_err(
                "Arrow schema n_children is negative or too large",
            ));
        }
        let n = usize::try_from(schema.n_children).map_err(|_| {
            PyTypeError::new_err("Arrow schema n_children is negative or too large")
        })?;
        let mut children = Vec::new();
        match depth {
            CaptureDepth::Shallow => {
                // No grandchildren — enough for name/extension/format selection.
            },
            CaptureDepth::Deep => {
                children.try_reserve(n).map_err(|_| {
                    PyTypeError::new_err("Arrow schema child count is too large to allocate")
                })?;
                if n > 0 {
                    if schema.children.is_null() {
                        return Err(PyTypeError::new_err(
                            "Arrow schema children pointer is null while n_children > 0",
                        ));
                    }
                    // SAFETY: children table live under shell for capture.
                    unsafe {
                        for i in 0..n {
                            let child = *schema.children.add(i);
                            if child.is_null() {
                                return Err(PyTypeError::new_err("Arrow child schema is null"));
                            }
                            children.push(Self::capture_node(
                                owner,
                                child,
                                CaptureDepth::Deep,
                                nesting + 1,
                            )?);
                        }
                    }
                }
            },
        }
        // Silence unused `n` on the shallow path (count was validated above).
        let _ = n;
        Ok(Self {
            format,
            name,
            metadata: Arc::from(metadata),
            dictionary_present,
            children: Arc::from(children),
        })
    }

    pub(crate) fn format(&self) -> &str {
        self.format.as_ref()
    }

    pub(crate) fn name(&self) -> &str {
        self.name.as_ref()
    }

    pub(crate) fn metadata_value(&self, key: &[u8]) -> Option<&[u8]> {
        self.metadata
            .iter()
            .find(|(k, _)| k.as_ref() == key)
            .map(|(_, v)| v.as_ref())
    }

    pub(crate) fn extension_name(&self) -> PyResult<Option<String>> {
        let Some(raw) = self.metadata_value(b"ARROW:extension:name") else {
            return Ok(None);
        };
        crate::py::arrow::decode_extension_name(raw.to_vec()).map(Some)
    }

    pub(crate) fn extension_metadata_bytes(&self) -> Vec<u8> {
        self.metadata_value(b"ARROW:extension:metadata")
            .map(<[u8]>::to_vec)
            .unwrap_or_default()
    }
}

/// How deep to walk child schema nodes during admission capture.
enum CaptureDepth {
    /// Name/format/metadata only — no grandchildren. Used for non-selected
    /// table siblings so a 50k-deep unused Struct cannot stack-overflow.
    Shallow,
    /// Full recursive capture of the geometry subtree.
    Deep,
}

/// Select a geometry column index from shallow-captured top-level children.
///
/// Uses name `"geometry"`, extension metadata, or binary/WKB storage formats —
/// the same preference order as [`select_admitted_geometry_child`], without
/// requiring deep nested structure on non-geometry siblings.
fn select_geometry_child_index_from_shallow(children: &[AdmittedArrowSchema]) -> PyResult<usize> {
    if children.is_empty() {
        return Err(PyTypeError::new_err(
            "Arrow table or record batch has no columns",
        ));
    }
    let mut geometry_name = None;
    for (index, child) in children.iter().enumerate() {
        if child.name() == "geometry" {
            if geometry_name.is_some() {
                return Err(PyTypeError::new_err(
                    "Arrow table or record batch has multiple columns named 'geometry'; select one column explicitly",
                ));
            }
            geometry_name = Some(index);
        }
    }
    if let Some(index) = geometry_name {
        return Ok(index);
    }
    let mut found = None;
    for (index, child) in children.iter().enumerate() {
        if shallow_child_looks_like_geometry(child)? {
            if found.is_some() {
                return Err(PyTypeError::new_err(
                    "Arrow table or record batch has multiple geometry-like columns; use table['geometry'] or select one column explicitly",
                ));
            }
            found = Some(index);
        }
    }
    found.ok_or_else(|| {
        PyTypeError::new_err(
            "expected an Arrow geometry array, chunked array, or table/record batch with a 'geometry' column or exactly one geometry-like column",
        )
    })
}

/// Whether a shallow child is a geometry candidate (extension name or WKB wire).
fn shallow_child_looks_like_geometry(child: &AdmittedArrowSchema) -> PyResult<bool> {
    if child.extension_name()?.is_some() {
        return Ok(true);
    }
    Ok(matches!(child.format(), "z" | "Z" | "vz"))
}

fn owned_c_str(ptr: *const std::ffi::c_char, what: &str) -> PyResult<Arc<str>> {
    if ptr.is_null() {
        return Err(PyTypeError::new_err(format!("{what} is null")));
    }
    // SAFETY: caller pins NUL-terminated C string for capture.
    let s = unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map_err(|_| PyTypeError::new_err(format!("{what} is not valid UTF-8")))?;
    Ok(Arc::from(s))
}

type OwnedMetadataPairs = Vec<(Box<[u8]>, Box<[u8]>)>;

/// # Safety
/// `schema` metadata blob is live and quiescent for the full copy.
unsafe fn owned_schema_metadata(schema: &ArrowSchema) -> PyResult<OwnedMetadataPairs> {
    // SAFETY: forwarded from capture_raw under the owner shell.
    unsafe { schema_metadata_pairs_owned(schema) }
}

/// Classify against an owned schema tree (no raw `ArrowSchema`).
pub(crate) fn classify_admitted_geometry_schema(
    schema: &AdmittedArrowSchema,
) -> PyResult<ClassifiedGeometrySchema> {
    classify_stream_geometry_schema_admitted(schema)
}

/// Adapter: classify stream schema that was already admitted.
fn classify_stream_geometry_schema_admitted(
    schema: &AdmittedArrowSchema,
) -> PyResult<ClassifiedGeometrySchema> {
    // Temporary bridge: reconstruct a minimal walk over owned fields using the
    // same rules as classify_stream_geometry_schema without raw pointers.
    if schema.dictionary_present {
        return Err(PyTypeError::new_err(
            "Arrow dictionary-encoded arrays are not supported for geometry import",
        ));
    }

    let extension = schema.extension_name()?;
    let format = schema.format();

    // Geometry array first.
    if extension.is_some() || matches!(format, "z" | "Z" | "vz") {
        let (encoding, width, crs, epoch) = classify_admitted_geometry_array(schema)?;
        return Ok(ClassifiedGeometrySchema {
            encoding,
            wkb_offset_width: width,
            crs,
            epoch,
            struct_child: None,
        });
    }

    if format == "+s" {
        let (child, index) = select_admitted_geometry_child(schema)?;
        let (encoding, width, crs, epoch) = classify_admitted_geometry_array(child)?;
        return Ok(ClassifiedGeometrySchema {
            encoding,
            wkb_offset_width: width,
            crs,
            epoch,
            struct_child: Some(index),
        });
    }

    let (encoding, width, crs, epoch) = classify_admitted_geometry_array(schema)?;
    Ok(ClassifiedGeometrySchema {
        encoding,
        wkb_offset_width: width,
        crs,
        epoch,
        struct_child: None,
    })
}

fn classify_admitted_geometry_array(
    schema: &AdmittedArrowSchema,
) -> PyResult<(
    crate::py::geoarrow::GeometryEncoding,
    crate::py::arrow::WkbOffsetWidth,
    Option<String>,
    Option<f64>,
)> {
    use crate::py::arrow::WkbOffsetWidth;
    use crate::py::geoarrow::GeometryEncoding;

    if schema.dictionary_present {
        return Err(PyTypeError::new_err(
            "Arrow dictionary-encoded arrays are not supported for geometry import",
        ));
    }
    let format = schema.format();
    if let Some(name) = schema.extension_name()? {
        let metadata = schema.extension_metadata_bytes();
        let (crs, epoch) = crate::py::arrow::parse_geoarrow_extension_metadata(&metadata)?;
        let encoding = GeometryEncoding::from_extension_name(&name)
            .ok_or_else(|| PyTypeError::new_err(GeometryEncoding::EXPECTED_EXTENSION))?;
        validate_admitted_encoding_storage(schema, encoding)?;
        let width = if matches!(encoding, GeometryEncoding::Wkb) {
            wkb_width_from_format(format)?
        } else {
            WkbOffsetWidth::Int32
        };
        return Ok((encoding, width, crs, epoch));
    }
    if matches!(format, "z" | "Z" | "vz") {
        if !schema.children.is_empty() {
            return Err(PyTypeError::new_err(
                "Arrow binary schema requires 0 children",
            ));
        }
        return Ok((
            GeometryEncoding::Wkb,
            wkb_width_from_format(format)?,
            None,
            None,
        ));
    }
    Err(PyTypeError::new_err(
        "expected a geoarrow point, multipoint, linestring, multilinestring, polygon, multipolygon, WKB, binary, or large_binary Arrow array",
    ))
}

fn wkb_width_from_format(format: &str) -> PyResult<crate::py::arrow::WkbOffsetWidth> {
    use crate::py::arrow::WkbOffsetWidth;
    Ok(match format {
        "vz" => WkbOffsetWidth::View,
        "Z" => WkbOffsetWidth::Int64,
        "z" => WkbOffsetWidth::Int32,
        _ => {
            return Err(PyTypeError::new_err(
                "geoarrow.wkb storage must be binary, large_binary, or binary_view",
            ));
        },
    })
}

fn select_admitted_geometry_child(
    schema: &AdmittedArrowSchema,
) -> PyResult<(&AdmittedArrowSchema, usize)> {
    if schema.children.is_empty() {
        return Err(PyTypeError::new_err(
            "Arrow table or record batch has no columns",
        ));
    }
    let mut geometry_name = None;
    for (index, child) in schema.children.iter().enumerate() {
        if child.name() == "geometry" {
            if geometry_name.is_some() {
                return Err(PyTypeError::new_err(
                    "Arrow table or record batch has multiple columns named 'geometry'; select one column explicitly",
                ));
            }
            geometry_name = Some((child, index));
        }
    }
    if let Some(found) = geometry_name {
        return Ok(found);
    }
    let mut found = None;
    for (index, child) in schema.children.iter().enumerate() {
        if classify_admitted_geometry_array(child).is_ok() {
            if found.is_some() {
                return Err(PyTypeError::new_err(
                    "Arrow table or record batch has multiple geometry-like columns; use table['geometry'] or select one column explicitly",
                ));
            }
            found = Some((child, index));
        }
    }
    found.ok_or_else(|| {
        PyTypeError::new_err(
            "expected an Arrow geometry array, chunked array, or table/record batch with a 'geometry' column or exactly one geometry-like column",
        )
    })
}

fn validate_admitted_encoding_storage(
    schema: &AdmittedArrowSchema,
    encoding: crate::py::geoarrow::GeometryEncoding,
) -> PyResult<()> {
    use crate::py::geoarrow::GeometryEncoding;
    match encoding {
        GeometryEncoding::Point => validate_admitted_coordinate_storage(schema),
        GeometryEncoding::MultiPoint | GeometryEncoding::LineString => {
            require_admitted_list(schema)?;
            if schema.children.len() != 1 {
                return Err(PyTypeError::new_err(
                    "Arrow list geometry requires exactly one child",
                ));
            }
            validate_admitted_coordinate_storage(&schema.children[0])
        },
        GeometryEncoding::MultiLineString | GeometryEncoding::Polygon => {
            require_admitted_list(schema)?;
            if schema.children.len() != 1 {
                return Err(PyTypeError::new_err(
                    "Arrow list geometry requires exactly one child",
                ));
            }
            require_admitted_list(&schema.children[0])?;
            if schema.children[0].children.len() != 1 {
                return Err(PyTypeError::new_err(
                    "Arrow nested list geometry requires exactly one child",
                ));
            }
            validate_admitted_coordinate_storage(&schema.children[0].children[0])
        },
        GeometryEncoding::MultiPolygon => {
            require_admitted_list(schema)?;
            let c0 = &schema.children;
            if c0.len() != 1 {
                return Err(PyTypeError::new_err(
                    "Arrow list geometry requires exactly one child",
                ));
            }
            require_admitted_list(&c0[0])?;
            if c0[0].children.len() != 1 {
                return Err(PyTypeError::new_err(
                    "Arrow nested list geometry requires exactly one child",
                ));
            }
            require_admitted_list(&c0[0].children[0])?;
            if c0[0].children[0].children.len() != 1 {
                return Err(PyTypeError::new_err(
                    "Arrow nested list geometry requires exactly one child",
                ));
            }
            validate_admitted_coordinate_storage(&c0[0].children[0].children[0])
        },
        GeometryEncoding::Wkb => {
            if !matches!(schema.format(), "z" | "Z" | "vz") {
                return Err(PyTypeError::new_err(
                    "geoarrow.wkb storage must be binary, large_binary, or binary_view",
                ));
            }
            if !schema.children.is_empty() {
                return Err(PyTypeError::new_err(
                    "Arrow binary schema requires 0 children",
                ));
            }
            Ok(())
        },
    }
}

/// Validate GeoArrow's two coordinate carriers: separated struct fields or
/// one interleaved fixed-size-list child.  The native capsule lane must use
/// the same accepted storage as direct PyArrow admission.
fn validate_admitted_coordinate_storage(schema: &AdmittedArrowSchema) -> PyResult<()> {
    if let Some(size) = schema
        .format()
        .strip_prefix("+w:")
        .and_then(|size| size.parse::<usize>().ok())
    {
        return validate_admitted_interleaved_coordinates(schema, size);
    }
    require_admitted_format(schema, "+s")?;
    let mut fields = Vec::with_capacity(schema.children.len());
    for child in schema.children.iter() {
        if child.dictionary_present {
            return Err(PyTypeError::new_err(
                "Arrow dictionary-encoded arrays are not supported for geometry import",
            ));
        }
        if !child.children.is_empty() {
            return Err(PyTypeError::new_err(
                "geoarrow point ordinate children must be leaves",
            ));
        }
        fields.push((child.name().to_owned(), child.format() == "g"));
    }
    crate::py::geoarrow::classify_geoarrow_ordinates(fields).map_err(PyTypeError::new_err)?;
    Ok(())
}

/// Validate `FixedSizeList<float64>[2|3|4]` coordinates from owned schema
/// data. The child name carries the optional GeoArrow dimension spelling.
fn validate_admitted_interleaved_coordinates(
    schema: &AdmittedArrowSchema,
    size: usize,
) -> PyResult<()> {
    if !(2..=4).contains(&size) {
        return Err(PyTypeError::new_err(
            "geoarrow interleaved coordinates require fixed_size_list of length 2, 3, or 4",
        ));
    }
    if schema.children.len() != 1 {
        return Err(PyTypeError::new_err(
            "Arrow fixed-size-list coordinates require exactly one value child",
        ));
    }
    let values = &schema.children[0];
    if values.dictionary_present || !values.children.is_empty() || values.format() != "g" {
        return Err(PyTypeError::new_err(
            "geoarrow interleaved coordinates require a float64 value leaf",
        ));
    }
    let name = values.name();
    if name.is_empty() || name == "item" {
        return Ok(());
    }
    let allowed = match size {
        2 => &["xy"][..],
        3 => &["xyz", "xym"][..],
        4 => &["xyzm"][..],
        _ => unreachable!("size is checked above"),
    };
    if !allowed.contains(&name) {
        return Err(PyTypeError::new_err(format!(
            "geoarrow interleaved fixed_size_list[{size}] field name must be one of {allowed:?} (or default 'item'), got {name:?}"
        )));
    }
    Ok(())
}

fn require_admitted_format(schema: &AdmittedArrowSchema, expected: &str) -> PyResult<()> {
    if schema.format() != expected {
        return Err(PyTypeError::new_err(format!(
            "unsupported Arrow schema format '{}' for geometry storage (expected {expected})",
            schema.format()
        )));
    }
    Ok(())
}

fn require_admitted_list(schema: &AdmittedArrowSchema) -> PyResult<()> {
    if !matches!(schema.format(), "+l" | "+L") {
        return Err(PyTypeError::new_err(format!(
            "unsupported Arrow schema format '{}' for geometry storage (expected +l or +L)",
            schema.format()
        )));
    }
    Ok(())
}

/// Capture a live producer schema into owned storage, release the shell, and
/// classify. Direct-capsule paths that do not need the raw schema after admit.
pub(crate) fn admit_and_classify_raw_schema(
    schema: *mut ArrowSchema,
) -> PyResult<(AdmittedArrowSchema, ClassifiedGeometrySchema)> {
    // SAFETY: caller guarantees a live schema base structure.
    let shell = unsafe { MovedArrowShell::take(schema) }?;
    // SAFETY: shell pins the moved allocation for capture only.
    let admitted = unsafe { AdmittedArrowSchema::capture(&shell) }?;
    drop(shell);
    let classified = classify_admitted_geometry_schema(&admitted)?;
    Ok((admitted, classified))
}
