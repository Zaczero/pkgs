#[test]
fn null_count_accepts_unknown_minus_one_and_known_in_range() {
    ensure_python();
    assert_eq!(NullCount::parse(-1, 10).unwrap(), NullCount::Unknown);
    assert_eq!(NullCount::parse(-1, 0).unwrap(), NullCount::Unknown);
    assert_eq!(NullCount::parse(0, 10).unwrap(), NullCount::Known(0));
    assert_eq!(NullCount::parse(3, 10).unwrap(), NullCount::Known(3));
    assert_eq!(NullCount::parse(10, 10).unwrap(), NullCount::Known(10));
    assert_eq!(NullCount::parse(0, 0).unwrap(), NullCount::Known(0));
}

#[test]
fn null_count_rejects_below_minus_one() {
    ensure_python();
    assert!(NullCount::parse(-2, 10).is_err());
    assert!(NullCount::parse(-100, 1).is_err());
    assert!(NullCount::parse(i64::MIN, 0).is_err());
}

#[test]
fn null_count_rejects_known_exceeding_logical_length() {
    ensure_python();
    assert!(NullCount::parse(1, 0).is_err());
    assert!(NullCount::parse(11, 10).is_err());
    assert!(NullCount::parse(i64::MAX, 1).is_err());
}

#[test]
fn arrow_format_parse_is_fallible_rejects_other_and_accepts_large_list() {
    ensure_python();
    assert_eq!(ArrowFormat::parse("g").unwrap(), ArrowFormat::Float64);
    assert_eq!(ArrowFormat::parse("z").unwrap(), ArrowFormat::Binary);
    assert_eq!(ArrowFormat::parse("Z").unwrap(), ArrowFormat::LargeBinary);
    assert_eq!(ArrowFormat::parse("vz").unwrap(), ArrowFormat::BinaryView);
    assert_eq!(ArrowFormat::parse("+l").unwrap(), ArrowFormat::List);
    assert_eq!(ArrowFormat::parse("+L").unwrap(), ArrowFormat::LargeList);
    assert_eq!(ArrowFormat::parse("+s").unwrap(), ArrowFormat::Struct);
    // Defect: int8 "c" tagged geoarrow.wkb used to size-guess as Other.
    assert!(ArrowFormat::parse("c").is_err());
    assert!(ArrowFormat::parse("i").is_err());
    assert_eq!(ArrowFormat::Binary.offset_width(), Some(4));
    assert_eq!(ArrowFormat::LargeBinary.offset_width(), Some(8));
    assert_eq!(ArrowFormat::List.offset_width(), Some(4));
    assert_eq!(ArrowFormat::LargeList.offset_width(), Some(8));
    assert_eq!(ArrowFormat::Float64.offset_width(), None);
}

#[test]
fn validity_snapshot_uses_offset_plus_length_not_null_count() {
    ensure_python();
    // Visible rows [5, 10) straddle bitmap bytes 0 and 1.  This enters
    // `snapshot_native_buffers`, the only shipping owner of the visible
    // validity window.  Mutating `visible_validity_range` to `(0, 0)`
    // makes the first assertion fail with an empty snapshot.
    let validity = [0b1110_0000_u8, 0b0000_0011];
    let values: [f64; 10] = std::array::from_fn(|index| index as f64);
    let buffers: [*const c_void; 2] = [validity.as_ptr().cast(), values.as_ptr().cast()];
    let mut array = stack_array(5, 5, 0, 2, 0, buffers.as_ptr(), ptr::null_mut());
    try_with_stack_layout(&mut array, ArrowFormat::Float64, 0, |layout| {
        let slots = snapshot_native_buffers(layout)?;
        let OwnedBuffer::Bytes(bitmap) = slots.get(0).expect("owned validity") else {
            panic!("validity is a byte bitmap")
        };
        assert_eq!(bitmap.as_slice(), validity);
        let OwnedBuffer::F64(visible_values) = slots.get(1).expect("owned values") else {
            panic!("float64 values are decoded at admission")
        };
        assert_eq!(visible_values.as_slice(), &values[5..]);
        Ok(())
    })
    .unwrap();
}

#[test]
fn validated_array_rejects_excessive_null_count_at_construction() {
    ensure_python();
    let bufs = dummy_buffers(2);
    let mut array = stack_array(3, 0, 4, 2, 0, bufs.as_ptr(), ptr::null_mut());
    assert!(try_with_stack_layout(&mut array, ArrowFormat::Float64, 0, |_| Ok(())).is_err());
    let mut array = stack_array(3, 0, -2, 2, 0, bufs.as_ptr(), ptr::null_mut());
    assert!(try_with_stack_layout(&mut array, ArrowFormat::Float64, 0, |_| Ok(())).is_err());
}

#[test]
fn known_null_count_must_match_visible_validity_bitmap() {
    ensure_python();
    // null_count=1 but validity bit set (all valid) — P02 mismatch.
    let validity: [u8; 1] = [0b0000_0001];
    let data = [0_u8; 8];
    let buffers: [*const c_void; 2] = [validity.as_ptr().cast(), data.as_ptr().cast()];
    let mut array = stack_array(1, 0, 1, 2, 0, buffers.as_ptr(), ptr::null_mut());
    let err = layout_with_owned_content(&mut array, ArrowFormat::Float64, 0);
    let message = match err {
        Ok(()) => panic!("expected null_count/bitmap mismatch to be rejected"),
        Err(error) => error.to_string(),
    };
    assert!(
        message.contains("null_count") && message.contains("validity bitmap"),
        "unexpected error: {message}"
    );
    // Matching pair: null_count=1, bit0 clear.
    let validity_null: [u8; 1] = [0];
    let buffers: [*const c_void; 2] = [validity_null.as_ptr().cast(), data.as_ptr().cast()];
    let mut array = stack_array(1, 0, 1, 2, 0, buffers.as_ptr(), ptr::null_mut());
    assert!(layout_with_owned_content(&mut array, ArrowFormat::Float64, 0).is_ok());
}

#[test]
fn validated_array_accepts_unknown_null_count() {
    ensure_python();
    let bufs = dummy_buffers(3);
    let mut array = stack_array(4, 0, -1, 3, 0, bufs.as_ptr(), ptr::null_mut());
    with_stack_layout(&mut array, ArrowFormat::Binary, 0, |validated| {
        assert_eq!(validated.null_count, NullCount::Unknown);
        assert_eq!(validated.null_count, NullCount::Unknown);
    });
}

#[test]
fn layout_rejects_wrong_buffer_and_child_cardinalities() {
    ensure_python();
    // Float64 with excess n_buffers (pointer-table OOB path).
    let bufs = dummy_buffers(3);
    let mut array = stack_array(1, 0, 0, 3, 0, bufs.as_ptr(), ptr::null_mut());
    assert!(try_with_stack_layout(&mut array, ArrowFormat::Float64, 0, |_| Ok(())).is_err());
    // Binary with too few buffers.
    let bufs = dummy_buffers(2);
    let mut array = stack_array(1, 0, 0, 2, 0, bufs.as_ptr(), ptr::null_mut());
    assert!(try_with_stack_layout(&mut array, ArrowFormat::Binary, 0, |_| Ok(())).is_err());
    // Schema/array child-count mismatch.
    let bufs = dummy_buffers(2);
    let mut array = stack_array(1, 0, 0, 2, 0, bufs.as_ptr(), ptr::null_mut());
    assert!(try_with_stack_layout(&mut array, ArrowFormat::Float64, 1, |_| Ok(())).is_err());
    // BinaryView now requires validity + views + mandatory sizes table.
    assert!(layout_with_owned_content(&mut array, ArrowFormat::BinaryView, 0).is_err());
}

#[test]
fn struct_child_rejects_visible_end_beyond_raw_allocation() {
    ensure_python();
    let bufs = dummy_buffers(2);
    let mut child = stack_array(4, 0, 0, 2, 0, bufs.as_ptr(), ptr::null_mut());
    if child.release.is_none() {
        child.release = Some(test_stack_array_release);
    }

    let shell = unsafe { MovedArrowShell::take(&raw mut child) }.unwrap();

    let err = unsafe {
        CaptureLayout::struct_child(&shell, shell.as_ptr(), ArrowFormat::Float64, 2, 3, 0)
    };
    let message = match err {
        Ok(_) => panic!("expected struct child range to be rejected"),
        Err(error) => error.to_string(),
    };
    assert!(
        message.contains("exceeds child allocation") || message.contains("overflow"),
        "unexpected error: {message}"
    );
}

#[test]
fn struct_child_null_count_validated_against_raw_not_projected() {
    ensure_python();
    // Parent offset=2, length=3; child raw length=10, null_count=6.
    // Old code rejected 6 > 3; fixed code accepts and exposes Unknown.
    let bufs = dummy_buffers(2);
    let mut child = stack_array(10, 0, 6, 2, 0, bufs.as_ptr(), ptr::null_mut());
    if child.release.is_none() {
        child.release = Some(test_stack_array_release);
    }

    let shell = unsafe { MovedArrowShell::take(&raw mut child) }.unwrap();

    let validated = unsafe {
        CaptureLayout::struct_child(&shell, shell.as_ptr(), ArrowFormat::Float64, 2, 3, 0)
    }
    .unwrap();
    assert_eq!(validated.length, 3);
    assert_eq!(validated.null_count, NullCount::Unknown);
    // Known(0) is preserved under projection.
    let mut child = stack_array(10, 0, 0, 2, 0, bufs.as_ptr(), ptr::null_mut());
    if child.release.is_none() {
        child.release = Some(test_stack_array_release);
    }

    let shell = unsafe { MovedArrowShell::take(&raw mut child) }.unwrap();

    let validated = unsafe {
        CaptureLayout::struct_child(&shell, shell.as_ptr(), ArrowFormat::Float64, 2, 3, 0)
    }
    .unwrap();
    assert_eq!(validated.null_count, NullCount::Known(0));
    // Full-view (parent covers raw range) preserves the known count.
    // Bitmap must match: 6 nulls of 10 rows (bits 0..5 clear, 6..9 set).
    let validity: [u8; 2] = [0b1100_0000, 0b0000_0011];
    let data = [0_u8; 80];
    let buffers: [*const c_void; 2] = [validity.as_ptr().cast(), data.as_ptr().cast()];
    let mut child = stack_array(10, 0, 6, 2, 0, buffers.as_ptr(), ptr::null_mut());
    if child.release.is_none() {
        child.release = Some(test_stack_array_release);
    }

    let shell = unsafe { MovedArrowShell::take(&raw mut child) }.unwrap();

    let validated = unsafe {
        CaptureLayout::struct_child(&shell, shell.as_ptr(), ArrowFormat::Float64, 0, 10, 0)
    }
    .unwrap();
    assert_eq!(validated.null_count, NullCount::Known(6));
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn struct_child_exposes_only_the_parent_slice() {
    ensure_python();
    // This fixture uses the same process-global release callbacks as the
    // exact-once lifecycle fixtures below. Keep their observed counters
    // exclusive under libtest's parallel scheduler.
    let _guard = CAPSULE_RELEASE_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Python::attach(|py| {
        let mut child_schema = ArrowSchema {
            format: c"g".as_ptr(),
            name: c"geometry".as_ptr(),
            metadata: ptr::null(),
            flags: 0,
            n_children: 0,
            children: ptr::null_mut(),
            dictionary: ptr::null_mut(),
            release: None,
            private_data: ptr::null_mut(),
        };
        let mut schema_children = [ptr::from_mut(&mut child_schema)];
        let schema = ArrowSchema {
            format: c"+s".as_ptr(),
            name: ptr::null(),
            metadata: ptr::null(),
            flags: 0,
            n_children: 1,
            children: schema_children.as_mut_ptr(),
            dictionary: ptr::null_mut(),
            release: Some(count_moved_schema_release),
            private_data: ptr::null_mut(),
        };
        let values = [0.0_f64, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let child_buffers: [*const c_void; 2] = [ptr::null(), values.as_ptr().cast()];
        let mut child = stack_array(10, 1, 0, 2, 0, child_buffers.as_ptr(), ptr::null_mut());
        let mut array_children = [ptr::from_mut(&mut child)];
        let root_buffers = [ptr::null()];
        let mut array = stack_array(
            3,
            2,
            0,
            1,
            1,
            root_buffers.as_ptr(),
            array_children.as_mut_ptr(),
        );
        array.release = Some(count_moved_array_release);
        // SAFETY: array capsule borrows a stack-owned shell for this test.
        let array_capsule = unsafe {
            Bound::<PyAny>::from_owned_ptr(
                py,
                ffi::PyCapsule_New(ptr::from_mut(&mut array).cast(), array_capsule_name(), None),
            )
        };
        // Selected-span: re-root the geometry child (index 0) at admission.
        let mut schema_for_admit = schema;
        // SAFETY: stack schema moved into shell for owned capture.
        let shell = unsafe { MovedArrowShell::take(&raw mut schema_for_admit) }.unwrap();
        let admitted = unsafe { AdmittedArrowSchema::capture(&shell) }.unwrap();
        drop(shell);
        let array_ptr =
            capsule_pointer::<ArrowArray>(&array_capsule, array_capsule_name()).unwrap();
        // SAFETY: one-shot array capsule.
        let batch = unsafe {
            native_arrow_batch_from_raw_array(
                array_ptr,
                &admitted,
                NativeArrowClassification {
                    encoding: GeometryEncoding::Point,
                    wkb_offset_width: WkbOffsetWidth::Int32,
                    crs: None,
                    epoch: None,
                },
                Some(0),
            )
        }
        .unwrap();
        let output = batch.node;
        assert_eq!(output.length, 3);
        // Admission re-roots the selected child span: its three values
        // are [3, 4, 5], not a retained physical prefix plus offset 3.
        let OwnedBuffer::F64(values) = output.buffers.get(1).unwrap() else {
            panic!("expected f64 values");
        };
        assert_eq!(values.len(), 3);
        assert_eq!(values[0].to_bits(), 3.0_f64.to_bits());
        assert_eq!(values[2].to_bits(), 5.0_f64.to_bits());
    });
}
