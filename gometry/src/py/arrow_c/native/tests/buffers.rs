#[test]
fn sliced_list_snapshot_rebases_offsets_and_projects_child() {
    // Capture owns only a list slice's bitmap, offsets, and child window.
    // This is intentionally below the Python decode layer: decoded geometry
    // cannot observe a temporary physical-parent copy. Restoring a full
    // parent snapshot makes the three exact byte/value assertions fail.
    ensure_python();
    let validity = [0_u8, 0b0000_0100, 0_u8];
    let offsets: [i32; 12] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 16];
    let values: [f64; 16] = std::array::from_fn(|index| index as f64);
    let child_buffers: [*const c_void; 2] = [ptr::null(), values.as_ptr().cast()];
    let mut child = stack_array(16, 0, 0, 2, 0, child_buffers.as_ptr(), ptr::null_mut());
    child.release = Some(test_stack_array_release);
    let mut child_ptrs = [ptr::from_mut(&mut child)];
    let parent_buffers: [*const c_void; 2] = [validity.as_ptr().cast(), offsets.as_ptr().cast()];
    // The producer's one visible parent row is physical row 10 and selects
    // child values [10, 16).  Its allocation deliberately has no public
    // length metadata beyond the Arrow offset window.
    let mut parent = stack_array(
        1,
        10,
        0,
        2,
        1,
        parent_buffers.as_ptr(),
        child_ptrs.as_mut_ptr(),
    );
    with_stack_layout(&mut parent, ArrowFormat::List, 1, |layout| {
        let slots = snapshot_native_buffers(layout).unwrap();
        let OwnedBuffer::Bytes(bitmap) = slots.get(0).unwrap() else {
            panic!("expected copied validity byte");
        };
        assert_eq!(bitmap.as_slice(), &validity[1..2]);
        let OwnedBuffer::Bytes(local_offsets) = slots.get(1).unwrap() else {
            panic!("expected copied list offsets");
        };
        assert_eq!(local_offsets.len(), 2 * std::mem::size_of::<i32>());
        let (offsets, remainder) = local_offsets.as_slice().as_chunks::<4>();
        assert!(remainder.is_empty());
        assert_eq!(
            offsets
                .iter()
                .map(|bytes| i32::from_le_bytes(*bytes))
                .collect::<Vec<_>>(),
            vec![0, 6]
        );
        assert_eq!(layout.child_window, Some((10, 6)));

        let child_ptr = unsafe { layout.child_ptr(0).unwrap() };
        let mut child_layout = unsafe {
            CaptureLayout::struct_child(layout.shell, child_ptr, ArrowFormat::Float64, 10, 6, 0)
        }
        .unwrap();
        let child_slots = snapshot_native_buffers(&mut child_layout).unwrap();
        let OwnedBuffer::F64(child_values) = child_slots.get(1).unwrap() else {
            panic!("expected owned f64 child window");
        };
        assert_eq!(child_values.as_slice(), &values[10..16]);
    });
}

#[test]
fn binary_data_len_reads_terminal_offset_at_matching_width() {
    ensure_python();
    let offsets: [i32; 3] = [0, 3, 7];
    // Minimal non-null data buffer so snapshot of index 2 is non-empty span.
    let data = [0_u8; 7];
    let buffers: [*const c_void; 3] = [ptr::null(), offsets.as_ptr().cast(), data.as_ptr().cast()];
    let mut array = stack_array(2, 0, 0, 3, 0, buffers.as_ptr(), ptr::null_mut());
    with_stack_layout(&mut array, ArrowFormat::Binary, 0, |validated| {
        let slots = snapshot_native_buffers(validated).unwrap();
        let OwnedBuffer::Bytes(data_slot) = slots.get(2).unwrap() else {
            panic!("expected bytes data buffer");
        };
        assert_eq!(data_slot.len(), 7);
    });

    let large_offsets: [i64; 3] = [0, 10, 25];
    let large_data = [0_u8; 25];
    let large_buffers: [*const c_void; 3] = [
        ptr::null(),
        large_offsets.as_ptr().cast(),
        large_data.as_ptr().cast(),
    ];
    let mut large = stack_array(2, 0, 0, 3, 0, large_buffers.as_ptr(), ptr::null_mut());
    with_stack_layout(&mut large, ArrowFormat::LargeBinary, 0, |validated| {
        let slots = snapshot_native_buffers(validated).unwrap();
        let OwnedBuffer::Bytes(data_slot) = slots.get(2).unwrap() else {
            panic!("expected bytes data buffer");
        };
        assert_eq!(data_slot.len(), 25);
    });
}

#[test]
fn binary_data_len_rejects_negative_terminal_offset() {
    ensure_python();
    let offsets: [i32; 2] = [0, -1];
    let buffers: [*const c_void; 3] = [ptr::null(), offsets.as_ptr().cast(), ptr::null()];
    let mut array = stack_array(1, 0, 0, 3, 0, buffers.as_ptr(), ptr::null_mut());
    with_stack_layout(&mut array, ArrowFormat::Binary, 0, |validated| {
        assert!(snapshot_native_buffers(validated).is_err());
    });
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn checked_byte_span_rejects_overflow_and_isize_max() {
    ensure_python();
    Python::attach(|py| {
        for (count, width) in [
            (usize::MAX, 2),
            ((usize::MAX / 8) + 1, 8),
            (usize::MAX / 4 + 1, 4),
            (1_usize << 60, 8),
        ] {
            let error = checked_byte_span(count, width, "overflow")
                .expect_err("unrepresentable byte span must be rejected");
            assert!(
                error.is_instance_of::<pyo3::exceptions::PyOverflowError>(py),
                "{error}"
            );
        }
        assert_eq!(checked_byte_span(4, 8, "overflow").unwrap(), 32);
        assert_eq!(
            checked_byte_span(isize::MAX as usize, 1, "overflow").unwrap(),
            isize::MAX as usize
        );
    });
}
