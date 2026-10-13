/// No-op release for stack-owned test arrays moved into [`MovedArrowShell`].
#[cfg(test)]
unsafe extern "C" fn test_stack_array_release(array: *mut ArrowArray) {
    // SAFETY: test owns the stack shell; null the slot so Drop is idempotent.
    unsafe {
        if !array.is_null() {
            *ArrowArray::release_slot(array) = None;
        }
    }
}

/// Run `f` with a capture layout whose lifetime is bound to a temporary shell
/// over a stack-owned test array (design-1 type-closure for unit tests).
#[cfg(test)]
fn with_stack_layout<R>(
    array: &mut ArrowArray,
    format: ArrowFormat,
    schema_n_children: usize,
    f: impl FnOnce(&mut CaptureLayout<'_>) -> R,
) -> R {
    if array.release.is_none() {
        array.release = Some(test_stack_array_release);
    }
    // SAFETY: stack array is live; release was just ensured non-null.
    let shell = unsafe { MovedArrowShell::take(array) }.expect("test shell take");
    // SAFETY: shell pins the moved stack array for the layout window.
    let mut layout =
        unsafe { CaptureLayout::new(&shell, shell.as_ptr(), format, schema_n_children) }
            .expect("test layout");
    f(&mut layout)
}

/// Like [`with_stack_layout`] but returns `PyResult` from the body.
#[cfg(test)]
fn try_with_stack_layout<R>(
    array: &mut ArrowArray,
    format: ArrowFormat,
    schema_n_children: usize,
    f: impl FnOnce(&mut CaptureLayout<'_>) -> PyResult<R>,
) -> PyResult<R> {
    if array.release.is_none() {
        array.release = Some(test_stack_array_release);
    }
    // SAFETY: stack array is live; release was just ensured non-null.
    let shell = unsafe { MovedArrowShell::take(array) }?;
    // SAFETY: shell pins the moved stack array for the layout window.
    let mut layout =
        unsafe { CaptureLayout::new(&shell, shell.as_ptr(), format, schema_n_children) }?;
    f(&mut layout)
}

/// Run capture + owned content validation (null_count / BinaryView ranges).
/// Unit tests that previously expected content errors from `new` use this.
#[cfg(test)]
fn layout_with_owned_content(
    array: &mut ArrowArray,
    format: ArrowFormat,
    schema_n_children: usize,
) -> PyResult<()> {
    try_with_stack_layout(array, format, schema_n_children, |layout| {
        drop(snapshot_native_buffers(layout)?);
        Ok(())
    })
}

/// Content-validated layout for tests that need lengths after BinaryView scan.
#[cfg(test)]
fn with_owned_content_layout<R>(
    array: &mut ArrowArray,
    format: ArrowFormat,
    schema_n_children: usize,
    f: impl FnOnce(&mut CaptureLayout<'_>, &OwnedBufferSlots) -> R,
) -> PyResult<R> {
    try_with_stack_layout(array, format, schema_n_children, |layout| {
        let buffers = snapshot_native_buffers(layout)?;
        Ok(f(layout, &buffers))
    })
}

// Global release counters are shared across the two capsule lifecycle
// tests. Parallel nextest interleaves `before + N` assertions and flakes
// (passes alone, fails with -j2). Serialize both tests on this mutex so
// each `before + N` window is exclusive; counters stay process-global for
// the release callbacks (which cannot carry per-test state).
static CAPSULE_RELEASE_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
static MOVED_SCHEMA_RELEASES: AtomicUsize = AtomicUsize::new(0);
static MOVED_ARRAY_RELEASES: AtomicUsize = AtomicUsize::new(0);

unsafe extern "C" fn count_moved_schema_release(schema: *mut ArrowSchema) {
    MOVED_SCHEMA_RELEASES.fetch_add(1, Ordering::Relaxed);
    // SAFETY: the test callback receives the consumer-owned moved shell.
    unsafe {
        (*schema).release = None;
    }
}

unsafe extern "C" fn stack_schema_release(schema: *mut ArrowSchema) {
    // SAFETY: the test owns this stack shell and only clears the release
    // slot so `MovedArrowShell` can finish its idempotent drop.
    unsafe {
        if !schema.is_null() {
            (*schema).release = None;
        }
    }
}

unsafe extern "C" fn count_moved_array_release(array: *mut ArrowArray) {
    MOVED_ARRAY_RELEASES.fetch_add(1, Ordering::Relaxed);
    // SAFETY: the test callback receives the consumer-owned moved shell.
    unsafe {
        (*array).release = None;
    }
}

fn ensure_python() {
    // `NullCount::parse` and friends construct `PyErr` on failure paths.
    crate::test_support::initialize_python();
}

fn stack_array(
    length: i64,
    offset: i64,
    null_count: i64,
    n_buffers: i64,
    n_children: i64,
    buffers: *const *const c_void,
    children: *mut *mut ArrowArray,
) -> ArrowArray {
    ArrowArray {
        length,
        null_count,
        offset,
        n_buffers,
        n_children,
        buffers,
        children,
        dictionary: ptr::null_mut(),
        release: None,
        private_data: ptr::null_mut(),
    }
}

/// Dummy non-null buffer pointer table so `n_buffers > 0` arrays pass the
/// null-buffers gate when the test only exercises schema-driven lengths.
fn dummy_buffers(n: usize) -> Vec<*const c_void> {
    vec![ptr::null(); n]
}
