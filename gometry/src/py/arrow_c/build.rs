#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::ptr;
use std::sync::Arc;

use crate::py::arrow_c::*;

impl SchemaNode {
    pub(crate) fn into_schema(self) -> Box<ArrowSchema> {
        let mut private = Box::new(SchemaPrivate {
            format: CString::new(self.format).expect("schema format has no nul"),
            name: CString::new(self.name).expect("schema name has no nul"),
            metadata: self.metadata.map(encode_schema_metadata),
            children: self.children.into_iter().map(Self::into_schema).collect(),
            child_ptrs: Vec::new(),
        });
        private.child_ptrs = private
            .children
            .iter_mut()
            .map(|child| ptr::from_mut(child.as_mut()))
            .collect();
        let schema = ArrowSchema {
            format: private.format.as_ptr(),
            name: private.name.as_ptr(),
            metadata: private
                .metadata
                .as_ref()
                .map_or(ptr::null(), |metadata| metadata.as_ptr().cast()),
            flags: 0,
            n_children: private.children.len() as i64,
            children: if private.child_ptrs.is_empty() {
                ptr::null_mut()
            } else {
                private.child_ptrs.as_mut_ptr()
            },
            dictionary: ptr::null_mut(),
            release: Some(release_schema),
            private_data: Box::into_raw(private).cast(),
        };
        Box::new(schema)
    }
}

pub(crate) fn encode_schema_metadata(pairs: Vec<(String, String)>) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(pairs.len() as i32).to_ne_bytes());
    for (key, value) in pairs {
        out.extend_from_slice(&(key.len() as i32).to_ne_bytes());
        out.extend_from_slice(key.as_bytes());
        out.extend_from_slice(&(value.len() as i32).to_ne_bytes());
        out.extend_from_slice(value.as_bytes());
    }
    out
}

pub(crate) fn make_array(
    length: usize,
    n_buffers: usize,
    buffers: Vec<*const c_void>,
    children: Vec<Box<ArrowArray>>,
    f64_buffers: Vec<Arc<[f64]>>,
    i32_buffers: Vec<Arc<[i32]>>,
    u8_buffers: Vec<Arc<[u8]>>,
) -> Box<ArrowArray> {
    let mut private = Box::new(ArrayPrivate {
        _f64_buffers: f64_buffers,
        _i32_buffers: i32_buffers,
        u8_buffers,
        buffers,
        children,
        child_ptrs: Vec::new(),
    });
    debug_assert_eq!(private.buffers.len(), n_buffers);
    private.child_ptrs = private
        .children
        .iter_mut()
        .map(|child| ptr::from_mut(child.as_mut()))
        .collect();
    let array = ArrowArray {
        length: length as i64,
        null_count: 0,
        offset: 0,
        n_buffers: n_buffers as i64,
        n_children: private.children.len() as i64,
        buffers: private.buffers.as_ptr(),
        children: if private.child_ptrs.is_empty() {
            ptr::null_mut()
        } else {
            private.child_ptrs.as_mut_ptr()
        },
        dictionary: ptr::null_mut(),
        release: Some(release_array),
        private_data: Box::into_raw(private).cast(),
    };
    Box::new(array)
}

pub(crate) fn apply_top_level_validity(array: &mut ArrowArray, mask: &[bool]) -> PyResult<()> {
    let null_count = mask.iter().filter(|&&missing| missing).count();
    if null_count == 0 {
        return Ok(());
    }
    if array.n_buffers < 1 {
        return Err(GeometryError::new_err(
            "Arrow geometry export has no top-level validity buffer slot",
        ));
    }
    let validity: Arc<[u8]> = crate::py::arrow::validity_bitmap_from_missing(mask).into();
    let private = array.private_data.cast::<ArrayPrivate>();
    if private.is_null() {
        return Err(GeometryError::new_err(
            "Arrow geometry export is missing private buffer ownership",
        ));
    }
    // SAFETY: gometry-built Arrow arrays always carry an `ArrayPrivate` box
    // whose `buffers` vector backs `array.buffers`. Appending the validity
    // Arc may move that vector, so the exported buffer pointer is refreshed.
    unsafe {
        let private = &mut *private;
        if private.buffers.is_empty() {
            return Err(GeometryError::new_err(
                "Arrow geometry export has no top-level validity buffer slot",
            ));
        }
        private.u8_buffers.push(validity);
        private.buffers[0] = private
            .u8_buffers
            .last()
            .expect("validity buffer was just pushed")
            .as_ptr()
            .cast();
        array.buffers = private.buffers.as_ptr();
    }
    array.null_count = i64::try_from(null_count)
        .map_err(|_| GeometryError::new_err("Arrow geometry export null count exceeds i64"))?;
    Ok(())
}

pub(crate) fn empty_array() -> ArrowArray {
    ArrowArray {
        length: 0,
        null_count: 0,
        offset: 0,
        n_buffers: 0,
        n_children: 0,
        buffers: ptr::null(),
        children: ptr::null_mut(),
        dictionary: ptr::null_mut(),
        release: None,
        private_data: ptr::null_mut(),
    }
}

pub(crate) fn primitive_f64_array(values: Arc<[f64]>) -> Box<ArrowArray> {
    make_array(
        values.len(),
        2,
        vec![ptr::null(), values.as_ptr().cast()],
        Vec::new(),
        vec![values],
        Vec::new(),
        Vec::new(),
    )
}

pub(crate) fn coordinate_schema(axes: CoordinateAxes) -> SchemaNode {
    let mut children = vec![
        SchemaNode {
            format: "g",
            name: "x",
            metadata: None,
            children: Vec::new(),
        },
        SchemaNode {
            format: "g",
            name: "y",
            metadata: None,
            children: Vec::new(),
        },
    ];
    if axes.has_z() {
        children.push(SchemaNode {
            format: "g",
            name: "z",
            metadata: None,
            children: Vec::new(),
        });
    }
    if axes.has_m() {
        children.push(SchemaNode {
            format: "g",
            name: "m",
            metadata: None,
            children: Vec::new(),
        });
    }
    SchemaNode {
        format: "+s",
        name: "",
        metadata: None,
        children,
    }
}

pub(crate) fn coordinate_array(seq: &CoordSeq) -> Box<ArrowArray> {
    // Export each column through the one blessed carry primitive: zero copy on
    // the full-window case (the gather path always produces full windows),
    // copying only a windowed sub-range otherwise.
    let xs = seq.carried_xs();
    let ys = seq.carried_ys();
    let zs = seq.carried_zs();
    let ms = seq.carried_ms();
    let mut children = vec![
        primitive_f64_array(Arc::clone(&xs)),
        primitive_f64_array(Arc::clone(&ys)),
    ];
    if let Some(zs) = &zs {
        children.push(primitive_f64_array(Arc::clone(zs)));
    }
    if let Some(ms) = &ms {
        children.push(primitive_f64_array(Arc::clone(ms)));
    }
    make_array(
        seq.len(),
        1,
        vec![ptr::null()],
        children,
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
}

pub(crate) fn list_array(offsets: Arc<[i32]>, child: Box<ArrowArray>) -> Box<ArrowArray> {
    let length = offsets.len().saturating_sub(1);
    make_array(
        length,
        2,
        vec![ptr::null(), offsets.as_ptr().cast()],
        vec![child],
        Vec::new(),
        vec![offsets],
        Vec::new(),
    )
}

pub(crate) fn binary_array(offsets: Arc<[i32]>, data: Arc<[u8]>) -> Box<ArrowArray> {
    let length = offsets.len().saturating_sub(1);
    make_array(
        length,
        3,
        vec![ptr::null(), offsets.as_ptr().cast(), data.as_ptr().cast()],
        Vec::new(),
        Vec::new(),
        vec![offsets],
        vec![data],
    )
}

pub(crate) fn extension_schema(
    encoding: GeometryEncoding,
    storage: SchemaNode,
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<SchemaNode> {
    let extension_name = encoding.extension_name();
    let mut metadata = vec![("ARROW:extension:name".to_owned(), extension_name.to_owned())];
    if let Some(value) = extension_metadata_json(crs, epoch)? {
        metadata.push(("ARROW:extension:metadata".to_owned(), value));
    }
    Ok(SchemaNode {
        metadata: Some(metadata),
        ..storage
    })
}

pub(crate) fn extension_metadata_json(
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<Option<String>> {
    let mut metadata = Map::new();
    if let Some(crs) = crs {
        let projjson = crate::crs::to_projjson(crs)?;
        let projjson = serde_json::from_str::<Value>(&projjson).map_err(|error| {
            ParseError::new_err(format!("invalid PROJJSON generated by PROJ: {error}"))
        })?;
        metadata.insert("crs".to_owned(), projjson);
        metadata.insert("crs_type".to_owned(), Value::String("projjson".to_owned()));
    }
    if let Some(epoch) = epoch {
        let value = Number::from_f64(epoch)
            .ok_or_else(|| GeometryError::new_err("coordinate epoch must be finite"))?;
        metadata.insert("epoch".to_owned(), Value::Number(value));
    }
    if metadata.is_empty() {
        Ok(None)
    } else {
        serde_json::to_string(&metadata).map(Some).map_err(|error| {
            GeometryError::new_err(format!("failed to encode GeoArrow metadata: {error}"))
        })
    }
}

pub(crate) fn list_schema(child: SchemaNode) -> SchemaNode {
    SchemaNode {
        format: "+l",
        name: "",
        metadata: None,
        children: vec![SchemaNode {
            name: "item",
            ..child
        }],
    }
}

pub(crate) const fn wkb_schema() -> SchemaNode {
    SchemaNode {
        format: "z",
        name: "",
        metadata: None,
        children: Vec::new(),
    }
}
