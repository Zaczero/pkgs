#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn owned_batch_decoder_runs_detached() {
    ensure_python();
    let leaf = |value: f64| NativeNode {
        length: 1,
        validity_offset: 0,
        n_buffers: 2,
        format: ArrowFormat::Float64,
        buffers: OwnedBufferSlots::Dense(Arc::new(vec![
            Some(OwnedBuffer::Bytes(Arc::new(Vec::new()))),
            Some(OwnedBuffer::F64(Arc::new(vec![value]))),
        ])),
        children: Arc::new(Vec::new()),
        schema_name: Arc::new(String::new()),
        field_names: Arc::new(Vec::new()),
        effective_missing: None,
    };
    let node = NativeNode {
        length: 1,
        validity_offset: 0,
        n_buffers: 1,
        format: ArrowFormat::Struct,
        buffers: OwnedBufferSlots::Dense(Arc::new(vec![Some(OwnedBuffer::Bytes(Arc::new(
            Vec::new(),
        )))])),
        children: Arc::new(vec![leaf(1.0), leaf(2.0)]),
        schema_name: Arc::new(String::new()),
        field_names: Arc::new(vec![String::from("x"), String::from("y")]),
        effective_missing: None,
    };
    let batch = NativeArrowBatch {
        node,
        classification: NativeArrowClassification {
            encoding: GeometryEncoding::Point,
            wkb_offset_width: WkbOffsetWidth::Int32,
            crs: None,
            epoch: None,
        },
    };
    Python::attach(|py| {
        let attached = py.detach(move || {
            // PyO3 0.29 has no public `Python::is_attached`; the C API
            // check is the same observation without attaching again.
            let detached = unsafe { ffi::PyGILState_Check() != 0 };
            let decoded = batch.decode();
            (detached, decoded)
        });
        let (detached, decoded) = attached;
        let decoded = decoded.expect("owned batch decoder");
        assert!(!detached);
        assert_eq!(decoded.length, 1);
        assert_eq!(decoded.storage.len(), 1);
        assert!(matches!(
            decoded.storage,
            NativeDecodedStorage::Packed(storage::GeometryArrayStorage::Packed(
                storage::PackedStorage::Points { .. }
            ))
        ));
    });
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn native_c_struct_decode_publishes_packed_points_without_python_import() {
    ensure_python();
    let mut x_schema = ArrowSchema {
        format: c"g".as_ptr(),
        name: c"x".as_ptr(),
        metadata: ptr::null(),
        flags: 0,
        n_children: 0,
        children: ptr::null_mut(),
        dictionary: ptr::null_mut(),
        release: None,
        private_data: ptr::null_mut(),
    };
    let mut y_schema = ArrowSchema {
        format: c"g".as_ptr(),
        name: c"y".as_ptr(),
        metadata: ptr::null(),
        flags: 0,
        n_children: 0,
        children: ptr::null_mut(),
        dictionary: ptr::null_mut(),
        release: None,
        private_data: ptr::null_mut(),
    };
    let mut schema_children = [ptr::from_mut(&mut x_schema), ptr::from_mut(&mut y_schema)];
    let mut schema = ArrowSchema {
        format: c"+s".as_ptr(),
        name: c"".as_ptr(),
        metadata: ptr::null(),
        flags: 0,
        n_children: 2,
        children: schema_children.as_mut_ptr(),
        dictionary: ptr::null_mut(),
        release: Some(stack_schema_release),
        private_data: ptr::null_mut(),
    };
    // SAFETY: this test owns the stack schema and keeps its child tree live
    // until the admitted owned schema has been captured.
    let schema_shell = unsafe { MovedArrowShell::take(&raw mut schema) }.unwrap();
    let admitted = unsafe { AdmittedArrowSchema::capture_geometry(&schema_shell) }.unwrap();
    drop(schema_shell);

    let x_values = [1.0_f64, 3.0];
    let y_values = [2.0_f64, 4.0];
    let x_buffers: [*const c_void; 2] = [ptr::null(), x_values.as_ptr().cast()];
    let y_buffers: [*const c_void; 2] = [ptr::null(), y_values.as_ptr().cast()];
    let mut x_array = stack_array(2, 0, 0, 2, 0, x_buffers.as_ptr(), ptr::null_mut());
    let mut y_array = stack_array(2, 0, 0, 2, 0, y_buffers.as_ptr(), ptr::null_mut());
    let mut array_children = [ptr::from_mut(&mut x_array), ptr::from_mut(&mut y_array)];
    let root_buffers = [ptr::null()];
    let mut array = stack_array(
        2,
        0,
        0,
        1,
        2,
        root_buffers.as_ptr(),
        array_children.as_mut_ptr(),
    );
    array.release = Some(test_stack_array_release);
    // SAFETY: the stack array and all children remain live through owned
    // admission; only the copied NativeNode enters the detached decoder.
    let batch = unsafe {
        native_arrow_batch_from_raw_array(
            &raw mut array,
            &admitted,
            NativeArrowClassification {
                encoding: GeometryEncoding::Point,
                wkb_offset_width: WkbOffsetWidth::Int32,
                crs: None,
                epoch: None,
            },
            None,
        )
    }
    .unwrap();
    assert!(array.release.is_none());

    Python::attach(|py| {
        let (decoded, allocations) =
            py.detach(move || crate::test_support::allocation_count(|| batch.decode()));
        let decoded = decoded.expect("native C struct point decode");
        assert_eq!(
            allocations, 1,
            "only the final packed storage Arc allocates"
        );
        assert!(matches!(
            decoded.storage,
            NativeDecodedStorage::Packed(storage::GeometryArrayStorage::Packed(
                storage::PackedStorage::Points { .. }
            ))
        ));
    });
}
