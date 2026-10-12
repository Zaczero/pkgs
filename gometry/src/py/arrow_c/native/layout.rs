use super::*;

/// Arrow C Data Interface `null_count`: `-1` means unknown; known counts must
/// not exceed the logical length.
///
/// Spec note (Arrow C Data Interface): a **null validity bitmap** means there
/// are no null values. When `null_count == -1` (unknown) and the bitmap pointer
/// is also null, treating the array as all-valid is the correct interpretation
/// — there is no bitmap to invent nulls from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum NullCount {
    Unknown,
    Known(usize),
}

impl NullCount {
    pub(super) fn parse(raw: i64, logical_length: usize) -> PyResult<Self> {
        if raw == -1 {
            return Ok(Self::Unknown);
        }
        if raw < -1 {
            return Err(PyTypeError::new_err("Arrow array null_count is below -1"));
        }
        let known = usize::try_from(raw)
            .map_err(|_| PyTypeError::new_err("Arrow array null_count is negative or too large"))?;
        if known > logical_length {
            return Err(PyTypeError::new_err(
                "Arrow array null_count exceeds logical length",
            ));
        }
        Ok(Self::Known(known))
    }
}

/// Schema format once, so buffer layout is schema-driven not position-inferred.
/// Only formats decoded end-to-end are admitted — no size-guessing `Other` arm.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ArrowFormat {
    /// `g` — float64 primitive values.
    Float64,
    /// `z` — binary with i32 offsets.
    Binary,
    /// `Z` — large binary with i64 offsets.
    LargeBinary,
    /// `vz` — binary view.
    BinaryView,
    /// `+l` — list with i32 offsets.
    List,
    /// `+L` — large list with i64 offsets.
    LargeList,
    /// `+w:N` — fixed-size list with exactly `N` values per row.
    FixedSizeList(usize),
    /// `+s` — struct.
    Struct,
}

impl ArrowFormat {
    /// Fallible parse: admit only formats decoded end-to-end.
    pub(super) fn parse(format: &str) -> PyResult<Self> {
        match format {
            "g" => Ok(Self::Float64),
            "z" => Ok(Self::Binary),
            "Z" => Ok(Self::LargeBinary),
            "vz" => Ok(Self::BinaryView),
            "+l" => Ok(Self::List),
            "+L" => Ok(Self::LargeList),
            "+s" => Ok(Self::Struct),
            fixed if let Some(size) = fixed.strip_prefix("+w:") => {
                let size = size.parse::<usize>().map_err(|_| {
                    PyTypeError::new_err(format!("invalid Arrow fixed-size-list format '{format}'"))
                })?;
                if size == 0 {
                    return Err(PyTypeError::new_err(
                        "Arrow fixed-size-list size must be greater than zero",
                    ));
                }
                Ok(Self::FixedSizeList(size))
            },
            _ => Err(PyTypeError::new_err(format!(
                "unsupported Arrow schema format '{format}'"
            ))),
        }
    }

    pub(super) const fn offset_width(self) -> Option<usize> {
        match self {
            Self::Binary | Self::List => Some(4),
            Self::LargeBinary | Self::LargeList => Some(8),
            Self::Float64 | Self::BinaryView | Self::FixedSizeList(_) | Self::Struct => None,
        }
    }

    /// Exact buffer/child layout for this format. BinaryView admits
    /// `n_buffers >= 3` (validity + views + mandatory variadic-sizes table,
    /// with optional variadic data buffers before that table).
    pub(super) fn validate_layout(
        self,
        n_buffers: usize,
        n_children: usize,
        schema_n_children: usize,
    ) -> PyResult<()> {
        if n_children != schema_n_children {
            return Err(PyTypeError::new_err(
                "Arrow array n_children does not match schema n_children",
            ));
        }
        match self {
            Self::Float64 => {
                if n_buffers != 2 || n_children != 0 {
                    return Err(PyTypeError::new_err(
                        "Arrow float64 array requires exactly 2 buffers and 0 children",
                    ));
                }
            },
            Self::Binary | Self::LargeBinary => {
                if n_buffers != 3 || n_children != 0 {
                    return Err(PyTypeError::new_err(
                        "Arrow binary array requires exactly 3 buffers and 0 children",
                    ));
                }
            },
            Self::List | Self::LargeList => {
                if n_buffers != 2 || n_children != 1 {
                    return Err(PyTypeError::new_err(
                        "Arrow list array requires exactly 2 buffers and 1 child",
                    ));
                }
            },
            Self::FixedSizeList(_) => {
                if n_buffers != 1 || n_children != 1 {
                    return Err(PyTypeError::new_err(
                        "Arrow fixed-size-list array requires exactly 1 buffer and 1 child",
                    ));
                }
            },
            Self::Struct => {
                if n_buffers != 1 {
                    return Err(PyTypeError::new_err(
                        "Arrow struct array requires exactly 1 buffer",
                    ));
                }
            },
            Self::BinaryView => {
                if n_buffers < 3 || n_children != 0 {
                    return Err(PyTypeError::new_err(
                        "Arrow binary-view array requires at least 3 buffers and 0 children",
                    ));
                }
            },
        }
        Ok(())
    }
}

impl<'shell> CaptureLayout<'shell> {
    /// Root / non-struct child: visible range equals the array's own offset+length.
    ///
    /// # Safety
    ///
    /// - `shell` pins the producer array tree for every use of the returned
    ///   layout (including buffer table walks during snapshot).
    /// - `ptr` is either the shell root or a live child under that shell.
    /// - Producer buffers/children stay readable and quiescent for the full
    ///   capture; no nested provider call or Python detach until snapshot ends.
    pub(super) unsafe fn new(
        shell: &'shell MovedArrowShell<ArrowArray>,
        ptr: *const ArrowArray,
        format: ArrowFormat,
        schema_n_children: usize,
    ) -> PyResult<Self> {
        // SAFETY: forwarded from this function's caller.
        unsafe { Self::from_raw(shell, ptr, format, None, schema_n_children) }
    }

    /// Struct field: visible length is the parent's, offset inherits parent
    /// offset; reject when the visible end exceeds the child's raw allocation.
    ///
    /// # Safety
    ///
    /// Same as [`Self::new`]: `ptr` is a live child of the owner-backed tree.
    pub(super) unsafe fn struct_child(
        shell: &'shell MovedArrowShell<ArrowArray>,
        ptr: *const ArrowArray,
        format: ArrowFormat,
        parent_offset: usize,
        parent_length: usize,
        schema_n_children: usize,
    ) -> PyResult<Self> {
        // SAFETY: forwarded from this function's caller.
        unsafe {
            Self::from_raw(
                shell,
                ptr,
                format,
                Some((parent_offset, parent_length)),
                schema_n_children,
            )
        }
    }

    /// # Safety
    ///
    /// Same as [`Self::new`].
    unsafe fn from_raw(
        shell: &'shell MovedArrowShell<ArrowArray>,
        ptr: *const ArrowArray,
        format: ArrowFormat,
        parent_slice: Option<(usize, usize)>,
        schema_n_children: usize,
    ) -> PyResult<Self> {
        if ptr.is_null() {
            return Err(PyTypeError::new_err("Arrow array pointer is null"));
        }
        // SAFETY: caller guarantees live shell-backed array for this pointer.
        let raw = unsafe { &*ptr };
        let raw_length = usize::try_from(raw.length)
            .map_err(|_| PyTypeError::new_err("Arrow array length is negative or too large"))?;
        let raw_offset = usize::try_from(raw.offset)
            .map_err(|_| PyTypeError::new_err("Arrow array offset is negative or too large"))?;
        let raw_end = raw_offset
            .checked_add(raw_length)
            .ok_or_else(|| PyTypeError::new_err("Arrow array offset+length overflows"))?;

        let (offset, length) = if let Some((parent_offset, parent_length)) = parent_slice {
            let offset = raw_offset
                .checked_add(parent_offset)
                .ok_or_else(|| PyTypeError::new_err("Arrow struct child offset overflows"))?;
            let end = offset.checked_add(parent_length).ok_or_else(|| {
                PyTypeError::new_err("Arrow struct child offset+length overflows")
            })?;
            if end > raw_end {
                return Err(PyTypeError::new_err(
                    "Arrow struct child visible range exceeds child allocation",
                ));
            }
            (offset, parent_length)
        } else {
            (raw_offset, raw_length)
        };
        offset
            .checked_add(length)
            .ok_or_else(|| PyTypeError::new_err("Arrow array offset+length overflows"))?;
        // Struct-child null_count is validated against the RAW child length;
        // a projected view preserves Known(0), preserves the raw count when the
        // view equals the full raw range, else exposes Unknown (inspect bitmap).
        let null_count = if parent_slice.is_some() {
            let raw_count = NullCount::parse(raw.null_count, raw_length)?;
            match raw_count {
                NullCount::Known(0) => NullCount::Known(0),
                count if offset == raw_offset && length == raw_length => count,
                NullCount::Known(_) | NullCount::Unknown => NullCount::Unknown,
            }
        } else {
            NullCount::parse(raw.null_count, length)?
        };
        let n_buffers = usize::try_from(raw.n_buffers)
            .map_err(|_| PyTypeError::new_err("Arrow array n_buffers is negative or too large"))?;
        let n_children = usize::try_from(raw.n_children)
            .map_err(|_| PyTypeError::new_err("Arrow array n_children is negative or too large"))?;
        format.validate_layout(n_buffers, n_children, schema_n_children)?;
        if n_buffers > 0 && raw.buffers.is_null() {
            return Err(PyTypeError::new_err("Arrow array buffers pointer is null"));
        }
        if n_children > 0 && raw.children.is_null() {
            return Err(PyTypeError::new_err("Arrow array children pointer is null"));
        }
        // Design-1: ONLY scalar/layout checks here. Content validation
        // (null_count vs bitmap, BinaryView descriptor scan) runs on OWNED
        // buffers after `snapshot_native_buffers` — never via producer
        // `from_raw_parts` or long-lived foreign slices.
        Ok(Self {
            shell,
            length,
            offset,
            null_count,
            n_buffers,
            n_children,
            format,
            buffers: raw.buffers,
            children: raw.children,
            child_window: None,
        })
    }

    /// Buffer pointer at `index`, or null when absent.
    ///
    /// # Safety
    ///
    /// Capture shell must still be live (buffer table valid).
    pub(super) const unsafe fn buffer_ptr(&self, index: usize) -> *const c_void {
        if index >= self.n_buffers || self.buffers.is_null() {
            return ptr::null();
        }
        // SAFETY: layout validated n_buffers and non-null table when n_buffers > 0.
        unsafe { *self.buffers.add(index) }
    }

    /// Child array pointer at `index`.
    ///
    /// # Safety
    ///
    /// Capture shell must still be live.
    pub(super) unsafe fn child_ptr(&self, index: usize) -> PyResult<*mut ArrowArray> {
        if index >= self.n_children {
            return Err(errors::parse_error(
                format!(
                    "Arrow child index {index} is out of range for n_children={}",
                    self.n_children
                ),
                crate::error::ParseFormat::GeoArrow,
            ));
        }
        if self.children.is_null() {
            return Err(errors::parse_error(
                "Arrow array children pointer is null",
                crate::error::ParseFormat::GeoArrow,
            ));
        }
        // SAFETY: layout-validated children table under live shell.
        let child = unsafe { *self.children.add(index) };
        if child.is_null() {
            return Err(errors::parse_error(
                "Arrow child array is null",
                crate::error::ParseFormat::GeoArrow,
            ));
        }
        Ok(child)
    }
}
