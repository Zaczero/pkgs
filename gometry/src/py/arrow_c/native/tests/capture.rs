#[test]
fn imported_capsules_move_once_and_release_the_owned_shells() {
    ensure_python();
    let _guard = CAPSULE_RELEASE_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let schema_before = MOVED_SCHEMA_RELEASES.load(Ordering::Relaxed);
    let array_before = MOVED_ARRAY_RELEASES.load(Ordering::Relaxed);
    Python::attach(|py| {
        // Bare binary WKB admits without extension metadata.
        let mut schema = ArrowSchema {
            format: c"z".as_ptr(),
            name: ptr::null(),
            metadata: ptr::null(),
            flags: 0,
            n_children: 0,
            children: ptr::null_mut(),
            dictionary: ptr::null_mut(),
            release: Some(count_moved_schema_release),
            private_data: ptr::null_mut(),
        };
        let buffers = [ptr::null(), ptr::null(), ptr::null()];
        let mut array = ArrowArray {
            length: 0,
            null_count: 0,
            offset: 0,
            n_buffers: 3,
            n_children: 0,
            buffers: buffers.as_ptr(),
            children: ptr::null_mut(),
            dictionary: ptr::null_mut(),
            release: Some(count_moved_array_release),
            private_data: ptr::null_mut(),
        };
        // SAFETY: these capsules borrow stack-owned shells for this test;
        // their no-op destructors never free those shells.
        let schema_capsule = unsafe {
            Bound::<PyAny>::from_owned_ptr(
                py,
                ffi::PyCapsule_New(
                    ptr::from_mut(&mut schema).cast(),
                    schema_capsule_name(),
                    None,
                ),
            )
        };
        // SAFETY: see the schema capsule immediately above.
        let array_capsule = unsafe {
            Bound::<PyAny>::from_owned_ptr(
                py,
                ffi::PyCapsule_New(ptr::from_mut(&mut array).cast(), array_capsule_name(), None),
            )
        };
        // Design-1: admit moves+releases both shells; no ImportedCapsules.
        let (batch, _) = native_arrow_batch_from_capsules(&schema_capsule, &array_capsule).unwrap();
        assert!(schema.release.is_none());
        assert!(array.release.is_none());
        // Second admit on already-released capsules must fail.
        assert!(native_arrow_batch_from_capsules(&schema_capsule, &array_capsule).is_err());
        drop(batch);
    });
    assert_eq!(
        MOVED_SCHEMA_RELEASES.load(Ordering::Relaxed),
        schema_before + 1
    );
    assert_eq!(
        MOVED_ARRAY_RELEASES.load(Ordering::Relaxed),
        array_before + 1
    );
}

#[test]
fn admit_releases_array_shell_per_batch_with_owned_schema() {
    ensure_python();
    let _guard = CAPSULE_RELEASE_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let schema_before = MOVED_SCHEMA_RELEASES.load(Ordering::Relaxed);
    let array_before = MOVED_ARRAY_RELEASES.load(Ordering::Relaxed);
    Python::attach(|_py| {
        let mut schema = ArrowSchema {
            format: c"g".as_ptr(),
            name: ptr::null(),
            metadata: ptr::null(),
            flags: 0,
            n_children: 0,
            children: ptr::null_mut(),
            dictionary: ptr::null_mut(),
            release: Some(count_moved_schema_release),
            private_data: ptr::null_mut(),
        };
        // Capture schema once, release immediately (stream design-1).
        // SAFETY: stack-owned live schema.
        // SAFETY: stack-owned live schema moved into shell for capture.
        let shell = unsafe { MovedArrowShell::take(&raw mut schema) }.unwrap();
        let admitted = unsafe { AdmittedArrowSchema::capture(&shell) }.unwrap();
        // Release after owned schema snapshot.
        drop(shell);
        let buffers = [ptr::null(), ptr::null()];
        let mut first_array = ArrowArray {
            length: 0,
            null_count: 0,
            offset: 0,
            n_buffers: 2,
            n_children: 0,
            buffers: buffers.as_ptr(),
            children: ptr::null_mut(),
            dictionary: ptr::null_mut(),
            release: Some(count_moved_array_release),
            private_data: ptr::null_mut(),
        };
        let mut second_array = ArrowArray {
            length: 0,
            null_count: 0,
            offset: 0,
            n_buffers: 2,
            n_children: 0,
            buffers: buffers.as_ptr(),
            children: ptr::null_mut(),
            dictionary: ptr::null_mut(),
            release: Some(count_moved_array_release),
            private_data: ptr::null_mut(),
        };
        let classification = NativeArrowClassification {
            encoding: GeometryEncoding::Point,
            wkb_offset_width: WkbOffsetWidth::Int32,
            crs: None,
            epoch: None,
        };
        let n1 = unsafe {
            native_arrow_batch_from_raw_array(
                &raw mut first_array,
                &admitted,
                classification.clone(),
                None,
            )
        }
        .unwrap();
        assert!(first_array.release.is_none());
        drop(n1);
        let n2 = unsafe {
            native_arrow_batch_from_raw_array(
                &raw mut second_array,
                &admitted,
                classification,
                None,
            )
        }
        .unwrap();
        assert!(second_array.release.is_none());
        drop(n2);
    });
    assert_eq!(
        MOVED_SCHEMA_RELEASES.load(Ordering::Relaxed),
        schema_before + 1
    );
    assert_eq!(
        MOVED_ARRAY_RELEASES.load(Ordering::Relaxed),
        array_before + 2
    );
}
