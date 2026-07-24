//! Shared geometry encoding/storage classifier for empty and non-empty
//! Arrow-C (and the schema half of PyArrow) ingress.
//!
//! Empty/zero-chunk streams never build arrays, so they must run the **same**
//! encoding admission as non-empty import: extension name + storage format,
//! dictionary rejection, leaf cardinality checks, and table field selection.
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]

use std::ffi::CStr;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

use super::{ArrowSchema, reject_large_list_format, schema_format_str, schema_metadata_value};
use crate::py::arrow::{WkbOffsetWidth, decode_extension_name, parse_geoarrow_extension_metadata};
use crate::py::geoarrow::{GeometryEncoding, classify_geoarrow_ordinates};

/// Result of classifying a stream/array schema as a supported geometry column.
#[derive(Clone, Debug)]
pub(crate) struct ClassifiedGeometrySchema {
    pub(crate) encoding: GeometryEncoding,
    pub(crate) wkb_offset_width: WkbOffsetWidth,
    pub(crate) crs: Option<String>,
    pub(crate) epoch: Option<f64>,
    /// When the root is a struct/table, the child index of the geometry field.
    /// `None` when the root itself is the geometry array.
    pub(crate) struct_child: Option<usize>,
}

/// Classify a stream or array root schema: select the geometry field (named
/// `geometry` or unique geometry-like child), validate only that subtree, and
/// return encoding + frame metadata.
pub(crate) fn classify_stream_geometry_schema(
    schema: &ArrowSchema,
) -> PyResult<ClassifiedGeometrySchema> {
    let format = schema_format_str(schema)?;
    reject_large_list_format(format)?;
    reject_dictionary(schema)?;

    // Geometry array first (extension name or bare binary). A geoarrow.point
    // storage IS `+s` (x/y struct) — that must not be mistaken for a table.
    if extension_name(schema)?.is_some() || matches!(format, "z" | "Z" | "vz") {
        let (encoding, width, crs, epoch) = classify_geometry_array_schema(schema)?;
        return Ok(ClassifiedGeometrySchema {
            encoding,
            wkb_offset_width: width,
            crs,
            epoch,
            struct_child: None,
        });
    }

    // Table / record-batch: bare struct of columns — select geometry field only.
    if format == "+s" {
        let (child, index) = select_geometry_struct_child(schema)?;
        let classified = classify_geometry_array_schema(child)?;
        return Ok(ClassifiedGeometrySchema {
            encoding: classified.0,
            wkb_offset_width: classified.1,
            crs: classified.2,
            epoch: classified.3,
            struct_child: Some(index),
        });
    }

    // Remaining formats (list, float64, int64, …) as a root array: only admit
    // if they carry geometry extension metadata (already handled) or are bare
    // binary (handled). Everything else rejects with the shared message.
    let (encoding, width, crs, epoch) = classify_geometry_array_schema(schema)?;
    Ok(ClassifiedGeometrySchema {
        encoding,
        wkb_offset_width: width,
        crs,
        epoch,
        struct_child: None,
    })
}

/// Classify a single geometry array schema (not a multi-column struct root).
pub(crate) fn classify_geometry_array_schema(
    schema: &ArrowSchema,
) -> PyResult<(
    GeometryEncoding,
    WkbOffsetWidth,
    Option<String>,
    Option<f64>,
)> {
    reject_dictionary(schema)?;
    let format = schema_format_str(schema)?;
    reject_large_list_format(format)?;

    // Extension metadata is meaningful only when an extension NAME is present.
    // Bare binary WKB with junk `ARROW:extension:metadata` must still import.
    let extension = extension_name(schema)?;
    if let Some(name) = extension {
        let metadata =
            schema_metadata_value(schema, b"ARROW:extension:metadata")?.unwrap_or_default();
        let (crs, epoch) = parse_geoarrow_extension_metadata(&metadata)?;
        // A present name must classify successfully — never fall through to bare WKB.
        let encoding = GeometryEncoding::from_extension_name(&name)
            .ok_or_else(|| PyTypeError::new_err(GeometryEncoding::EXPECTED_EXTENSION))?;
        validate_encoding_storage(schema, encoding)?;
        let width = if matches!(encoding, GeometryEncoding::Wkb) {
            wkb_width_from_format(format)?
        } else {
            WkbOffsetWidth::Int32
        };
        return Ok((encoding, width, crs, epoch));
    }

    // Bare binary → WKB (matches non-empty native_arrow_storage_array).
    if matches!(format, "z" | "Z" | "vz") {
        validate_wkb_leaf_schema(schema)?;
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

fn wkb_width_from_format(format: &str) -> PyResult<WkbOffsetWidth> {
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

fn extension_name(schema: &ArrowSchema) -> PyResult<Option<String>> {
    let Some(raw) = schema_metadata_value(schema, b"ARROW:extension:name")? else {
        return Ok(None);
    };
    decode_extension_name(raw).map(Some)
}

fn reject_dictionary(schema: &ArrowSchema) -> PyResult<()> {
    if !schema.dictionary.is_null() {
        return Err(PyTypeError::new_err(
            "Arrow dictionary-encoded arrays are not supported for geometry import",
        ));
    }
    Ok(())
}

/// Select the geometry field from a struct schema using the same rules as
/// [`crate::py::arrow::arrow_geometry_input`]: named `geometry` first, else
/// unique geometry-like child.
fn select_geometry_struct_child(schema: &ArrowSchema) -> PyResult<(&ArrowSchema, usize)> {
    let children = schema_children(schema)?;
    if children.is_empty() {
        return Err(PyTypeError::new_err(
            "Arrow table or record batch has no columns",
        ));
    }

    // Named "geometry" wins — but more than one exact name is ambiguous.
    let mut geometry_name = None;
    for (index, child) in children.iter().enumerate() {
        if schema_field_name(child)? == "geometry" {
            if geometry_name.is_some() {
                return Err(PyTypeError::new_err(
                    "Arrow table or record batch has multiple columns named 'geometry'; select one column explicitly",
                ));
            }
            // Validate only this subtree (siblings may be large_list, etc.).
            geometry_name = Some((child, index));
        }
    }
    if let Some((child, index)) = geometry_name {
        return Ok((child, index));
    }

    // Unique geometry-like column.
    let mut found = None;
    for (index, child) in children.iter().enumerate() {
        if schema_looks_like_geometry(child) {
            if found.is_some() {
                return Err(PyTypeError::new_err(
                    "Arrow table or record batch has multiple geometry-like columns; use table['geometry'] or select one column explicitly",
                ));
            }
            found = Some((child, index));
        }
    }
    found
        .map(|(child, index)| (*child, index))
        .ok_or_else(|| {
            PyTypeError::new_err(
                "expected an Arrow geometry array, chunked array, or table/record batch with a 'geometry' column or exactly one geometry-like column",
            )
        })
}

fn schema_looks_like_geometry(schema: &ArrowSchema) -> bool {
    classify_geometry_array_schema(schema).is_ok()
}

fn schema_field_name(schema: &ArrowSchema) -> PyResult<String> {
    if schema.name.is_null() {
        return Ok(String::new());
    }
    // SAFETY: producer-owned NUL-terminated field name for the schema lifetime.
    let name = unsafe { CStr::from_ptr(schema.name) }
        .to_str()
        .map_err(|_| PyTypeError::new_err("Arrow field name is not valid UTF-8"))?;
    Ok(name.to_owned())
}

fn schema_children(schema: &ArrowSchema) -> PyResult<Vec<&ArrowSchema>> {
    if schema.n_children < 0 {
        return Err(PyTypeError::new_err(
            "Arrow schema n_children is negative or too large",
        ));
    }
    if schema.n_children == 0 {
        return Ok(Vec::new());
    }
    if schema.children.is_null() {
        return Err(PyTypeError::new_err(
            "Arrow schema children pointer is null while n_children > 0",
        ));
    }
    let n = usize::try_from(schema.n_children)
        .map_err(|_| PyTypeError::new_err("Arrow schema n_children is negative or too large"))?;
    let mut out = Vec::with_capacity(n);
    // SAFETY: children table is producer-owned for the schema lifetime.
    unsafe {
        for index in 0..n {
            let child = *schema.children.add(index);
            if child.is_null() {
                return Err(PyTypeError::new_err("Arrow child schema is null"));
            }
            out.push(&*child);
        }
    }
    Ok(out)
}

/// Validate that storage format nesting matches the GeoArrow encoding.
///
/// Shared choke-point for stream, direct Arrow-C, and any native path that
/// already knows the encoding — must run before coordinate buffers are read.
pub(crate) fn validate_encoding_storage(
    schema: &ArrowSchema,
    encoding: GeometryEncoding,
) -> PyResult<()> {
    match encoding {
        GeometryEncoding::Point => validate_point_struct(schema),
        GeometryEncoding::MultiPoint | GeometryEncoding::LineString => {
            // list<point>
            require_format(schema, "+l")?;
            let children = schema_children(schema)?;
            if children.len() != 1 {
                return Err(PyTypeError::new_err(
                    "Arrow list geometry requires exactly one child",
                ));
            }
            validate_point_struct(children[0])
        },
        GeometryEncoding::MultiLineString | GeometryEncoding::Polygon => {
            // list<list<point>>
            require_format(schema, "+l")?;
            let children = schema_children(schema)?;
            if children.len() != 1 {
                return Err(PyTypeError::new_err(
                    "Arrow list geometry requires exactly one child",
                ));
            }
            require_format(children[0], "+l")?;
            let inner = schema_children(children[0])?;
            if inner.len() != 1 {
                return Err(PyTypeError::new_err(
                    "Arrow nested list geometry requires exactly one child",
                ));
            }
            validate_point_struct(inner[0])
        },
        GeometryEncoding::MultiPolygon => {
            // list<list<list<point>>>
            require_format(schema, "+l")?;
            let c0 = schema_children(schema)?;
            if c0.len() != 1 {
                return Err(PyTypeError::new_err(
                    "Arrow list geometry requires exactly one child",
                ));
            }
            require_format(c0[0], "+l")?;
            let c1 = schema_children(c0[0])?;
            if c1.len() != 1 {
                return Err(PyTypeError::new_err(
                    "Arrow nested list geometry requires exactly one child",
                ));
            }
            require_format(c1[0], "+l")?;
            let c2 = schema_children(c1[0])?;
            if c2.len() != 1 {
                return Err(PyTypeError::new_err(
                    "Arrow nested list geometry requires exactly one child",
                ));
            }
            validate_point_struct(c2[0])
        },
        GeometryEncoding::Wkb => {
            let format = schema_format_str(schema)?;
            if !matches!(format, "z" | "Z" | "vz") {
                return Err(PyTypeError::new_err(
                    "geoarrow.wkb storage must be binary, large_binary, or binary_view",
                ));
            }
            validate_wkb_leaf_schema(schema)
        },
    }
}

fn validate_point_struct(schema: &ArrowSchema) -> PyResult<()> {
    require_format(schema, "+s")?;
    let children = schema_children(schema)?;
    let mut fields = Vec::with_capacity(children.len());
    for child in children {
        reject_dictionary(child)?;
        let format = schema_format_str(child)?;
        reject_large_list_format(format)?;
        // Leaves: no further children.
        let grand = schema_children(child)?;
        if !grand.is_empty() {
            return Err(PyTypeError::new_err(
                "geoarrow point ordinate children must be leaves",
            ));
        }
        let name = schema_field_name(child)?;
        // Exact float64 only — never reinterpret int64 (or other) bytes as f64.
        fields.push((name, format == "g"));
    }
    classify_geoarrow_ordinates(fields).map_err(PyTypeError::new_err)?;
    Ok(())
}

fn require_format(schema: &ArrowSchema, expected: &str) -> PyResult<()> {
    let format = schema_format_str(schema)?;
    reject_large_list_format(format)?;
    if format != expected {
        return Err(PyTypeError::new_err(format!(
            "unsupported Arrow schema format '{format}' for geometry storage (expected {expected})"
        )));
    }
    Ok(())
}

/// Validate WKB storage directly. Classification has already established that
/// it is a binary leaf, so never dereference its declared child table.
fn validate_wkb_leaf_schema(schema: &ArrowSchema) -> PyResult<()> {
    reject_dictionary(schema)?;
    let format = schema_format_str(schema)?;
    if !matches!(format, "z" | "Z" | "vz") {
        return Err(PyTypeError::new_err(
            "geoarrow.wkb storage must be binary, large_binary, or binary_view",
        ));
    }
    if schema.n_children != 0 {
        return Err(PyTypeError::new_err(
            "Arrow binary schema requires 0 children",
        ));
    }
    Ok(())
}

/// Public keystone used by empty streams: full geometry classification
/// (replaces the +L-only walker).
pub(crate) fn validate_empty_stream_schema(
    schema: &ArrowSchema,
) -> PyResult<ClassifiedGeometrySchema> {
    classify_stream_geometry_schema(schema)
}
