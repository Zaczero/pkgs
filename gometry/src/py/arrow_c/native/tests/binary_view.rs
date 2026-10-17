#[test]
fn binary_view_rejects_out_of_range_buffer_index() {
    // One non-inline view: length=13, buffer_index=5, but only validity,
    // views, and the mandatory sizes table (no variadic data) → reject.
    let mut view = [0_u8; 16];
    view[0..4].copy_from_slice(&13_i32.to_le_bytes());
    view[8..12].copy_from_slice(&5_i32.to_le_bytes());
    let buffers: [*const c_void; 3] = [ptr::null(), view.as_ptr().cast(), ptr::null()];
    let mut array = stack_array(1, 0, 0, 3, 0, buffers.as_ptr(), ptr::null_mut());
    assert!(layout_with_owned_content(&mut array, ArrowFormat::BinaryView, 0).is_err());
}

#[test]
fn zero_length_skips_binary_view_descriptor_scan() {
    let bufs = dummy_buffers(3);
    let mut array = stack_array(0, 0, -1, 3, 0, bufs.as_ptr(), ptr::null_mut());
    with_owned_content_layout(
        &mut array,
        ArrowFormat::BinaryView,
        0,
        |validated, slots| {
            assert_eq!(validated.length, 0);
            assert_eq!(validated.null_count, NullCount::Unknown);
            let OwnedBufferSlots::BinaryView { data, .. } = slots else {
                panic!("expected sparse BinaryView buffers");
            };
            assert!(data.is_empty(), "zero rows retain no data fragments");
        },
    )
    .unwrap();
}

#[test]
fn binary_view_huge_n_buffers_length_zero_is_sparse_not_oom() {
    // Release blocker: format "vz", length=0, n_buffers huge must not
    // allocate `vec![0; n_buffers-3]` (capacity overflow / multi-GB).
    let n_buffers = 1_000_000;
    let sizes = vec![0_i64; n_buffers - 3];
    let mut bufs = dummy_buffers(n_buffers);
    bufs[n_buffers - 1] = sizes.as_ptr().cast();
    // The mandatory sizes table has one entry per variadic buffer, but the
    // selected payload owner remains sparse instead of mirroring that table.
    let mut array = stack_array(0, 0, 0, n_buffers as i64, 0, bufs.as_ptr(), ptr::null_mut());
    with_owned_content_layout(&mut array, ArrowFormat::BinaryView, 0, |_, slots| {
        let OwnedBufferSlots::BinaryView {
            data,
            n_buffers: captured_buffers,
            ..
        } = slots
        else {
            panic!("expected sparse BinaryView buffers");
        };
        assert_eq!(*captured_buffers, n_buffers);
        assert!(data.is_empty());
    })
    .unwrap();
}

#[test]
fn binary_view_selected_fragment_does_not_retain_unreferenced_size_entries() {
    // One high-index payload has a 5,000-entry producer size table. The
    // imported node owns only that descriptor's 8-byte declaration and
    // selected payload, not all 40 KiB of unrelated advisory metadata.
    let data_index = 4_999_usize;
    let n_buffers = data_index + 4;
    let mut view = [0_u8; 16];
    view[0..4].copy_from_slice(&20_i32.to_le_bytes());
    view[4..8].copy_from_slice(&[7, 7, 7, 7]);
    view[8..12].copy_from_slice(&(data_index as i32).to_le_bytes());
    let data = [7_u8; 20];
    let mut sizes = vec![0_i64; n_buffers - 3];
    sizes[data_index] = data.len() as i64;
    let mut buffers = dummy_buffers(n_buffers);
    buffers[1] = view.as_ptr().cast();
    buffers[data_index + 2] = data.as_ptr().cast();
    buffers[n_buffers - 1] = sizes.as_ptr().cast();
    let mut array = stack_array(
        1,
        0,
        0,
        n_buffers as i64,
        0,
        buffers.as_ptr(),
        ptr::null_mut(),
    );
    try_with_stack_layout(&mut array, ArrowFormat::BinaryView, 0, |layout| {
        let owned = snapshot_native_buffers(layout)?;
        assert!(matches!(
            owned.get(data_index + 2),
            Some(OwnedBuffer::Bytes(bytes)) if bytes.as_slice() == &data[..]
        ));
        assert!(owned.get(2).is_none(), "unreferenced low slot was retained");
        assert!(
            owned.get(data_index + 3).is_none(),
            "the high-index sizes slot was exposed as payload"
        );
        Ok(())
    })
    .unwrap();
}

#[test]
fn binary_view_sparse_high_buffer_index_rejects_not_dense_grow() {
    // Present non-inline: buffer_index=5 with no variadic data buffers
    // (only validity, views, and sizes) → reject. No dense growth to 5.
    let mut view = [0_u8; 16];
    view[0..4].copy_from_slice(&20_i32.to_le_bytes());
    view[8..12].copy_from_slice(&5_i32.to_le_bytes());
    view[12..16].copy_from_slice(&0_i32.to_le_bytes());
    let buffers: [*const c_void; 3] = [ptr::null(), view.as_ptr().cast(), ptr::null()];
    let mut array = stack_array(1, 0, 0, 3, 0, buffers.as_ptr(), ptr::null_mut());
    assert!(layout_with_owned_content(&mut array, ArrowFormat::BinaryView, 0).is_err());
}

#[test]
fn binary_view_multi_buffer_endpoints_are_sparse_and_correct() {
    // Two non-inline rows on distinct data buffers: ends keyed sparsely.
    let mut views = [0_u8; 32];
    let data0 = *b"0123456789abcdefghijklmnopqrstuv";
    let data1 = [0_u8; 1]; // unused index 1
    let data2 = *b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef";
    // Row 0: length=20, buffer_index=0, byte_offset=0 → end=20
    views[0..4].copy_from_slice(&20_i32.to_le_bytes());
    views[4..8].copy_from_slice(&data0[0..4]);
    views[8..12].copy_from_slice(&0_i32.to_le_bytes());
    views[12..16].copy_from_slice(&0_i32.to_le_bytes());
    // Row 1: length=15, buffer_index=2, byte_offset=4 → end=19
    views[16..20].copy_from_slice(&15_i32.to_le_bytes());
    views[20..24].copy_from_slice(&data2[4..8]);
    views[24..28].copy_from_slice(&2_i32.to_le_bytes());
    views[28..32].copy_from_slice(&4_i32.to_le_bytes());
    let sizes = [32_i64, 1, 32];
    let buffers: [*const c_void; 6] = [
        ptr::null(),
        views.as_ptr().cast(),
        data0.as_ptr().cast(),
        data1.as_ptr().cast(),
        data2.as_ptr().cast(),
        sizes.as_ptr().cast(),
    ];
    let mut array = stack_array(2, 0, 0, 6, 0, buffers.as_ptr(), ptr::null_mut());
    with_owned_content_layout(&mut array, ArrowFormat::BinaryView, 0, |_, slots| {
        let Some(OwnedBuffer::Bytes(first)) = slots.get(2) else {
            panic!("selected first BinaryView payload was not retained");
        };
        let Some(OwnedBuffer::Bytes(second)) = slots.get(4) else {
            panic!("selected second BinaryView payload was not retained");
        };
        assert_eq!(first.as_slice(), &data0[0..20]);
        assert_eq!(second.as_slice(), &data2[4..19]);
        assert!(
            slots.get(3).is_none(),
            "unreferenced middle slot was retained"
        );
        assert!(slots.get(5).is_none(), "sizes table was exposed as payload");
    })
    .unwrap();
}

#[test]
fn binary_view_null_row_descriptor_is_ignored_not_oob() {
    // Conforming null row: validity bit0=0, ignored descriptor claims a
    // 1_000_000-byte span over a 1-byte data buffer. Pre-fix code treated
    // the descriptor as live and OOB-read the allocation.
    let mut view = [0_u8; 16];
    view[0..4].copy_from_slice(&1_000_000_i32.to_le_bytes());
    view[8..12].copy_from_slice(&0_i32.to_le_bytes());
    view[12..16].copy_from_slice(&0_i32.to_le_bytes());
    let validity: [u8; 1] = [0]; // bit0 = null
    let data: [u8; 1] = [0xAB];
    let sizes = [1_i64];
    let buffers: [*const c_void; 4] = [
        validity.as_ptr().cast(),
        view.as_ptr().cast(),
        data.as_ptr().cast(),
        sizes.as_ptr().cast(),
    ];
    let mut array = stack_array(1, 0, 1, 4, 0, buffers.as_ptr(), ptr::null_mut());
    with_owned_content_layout(&mut array, ArrowFormat::BinaryView, 0, |_, slots| {
        let OwnedBufferSlots::BinaryView { data, .. } = slots else {
            panic!("expected sparse BinaryView buffers");
        };
        assert!(data.is_empty(), "null descriptors must not select payloads");
        assert!(slots.get(2).is_none());
    })
    .unwrap();
}

#[test]
fn binary_view_present_descriptor_snapshots_selected_payload() {
    // Present non-inline: length=20, buffer_index=0, byte_offset=2 → end=22.
    let mut view = [0_u8; 16];
    view[0..4].copy_from_slice(&20_i32.to_le_bytes());
    view[4..8].copy_from_slice(&[0, 0, 0, 0]);
    view[8..12].copy_from_slice(&0_i32.to_le_bytes());
    view[12..16].copy_from_slice(&2_i32.to_le_bytes());
    let data = [0_u8; 32];
    let sizes = [32_i64];
    let buffers: [*const c_void; 4] = [
        ptr::null(),
        view.as_ptr().cast(),
        data.as_ptr().cast(),
        sizes.as_ptr().cast(),
    ];
    let mut array = stack_array(1, 0, 0, 4, 0, buffers.as_ptr(), ptr::null_mut());
    try_with_stack_layout(&mut array, ArrowFormat::BinaryView, 0, |layout| {
        let owned = snapshot_native_buffers(layout)?;
        let Some(OwnedBuffer::Bytes(payload)) = owned.get(2) else {
            panic!("selected payload slot was not retained");
        };
        assert_eq!(payload.as_slice(), &data[2..22]);
        Ok(())
    })
    .unwrap();
}

#[test]
fn binary_view_rebases_nonzero_selected_payload_origin() {
    // One selected non-inline descriptor starts at physical byte 2. Native
    // admission must copy only [2, 22) and rewrite the owned descriptor
    // to offset 0; otherwise downstream BinaryView decode indexes the
    // rebased payload with a stale physical offset. This is deliberately a
    // mutation guard for `rebase_owned_binary_view_offsets`.
    let mut view = [0_u8; 16];
    view[0..4].copy_from_slice(&20_i32.to_le_bytes());
    view[4..8].copy_from_slice(&[2, 3, 4, 5]);
    view[8..12].copy_from_slice(&0_i32.to_le_bytes());
    view[12..16].copy_from_slice(&2_i32.to_le_bytes());
    let data: [u8; 32] = std::array::from_fn(|index| index as u8);
    let sizes = [32_i64];
    let buffers: [*const c_void; 4] = [
        ptr::null(),
        view.as_ptr().cast(),
        data.as_ptr().cast(),
        sizes.as_ptr().cast(),
    ];
    let mut array = stack_array(1, 0, 0, 4, 0, buffers.as_ptr(), ptr::null_mut());
    try_with_stack_layout(&mut array, ArrowFormat::BinaryView, 0, |layout| {
        let owned = snapshot_native_buffers(layout)?;
        let view = owned
            .get(1)
            .and_then(|buffer| match buffer {
                OwnedBuffer::Bytes(bytes) => Some(bytes.as_slice()),
                OwnedBuffer::F64(_) => None,
            })
            .expect("owned BinaryView descriptors");
        let offset = i32::from_le_bytes(view[12..16].try_into().expect("view offset"));
        assert_eq!(offset, 0, "selected payload origin must be rebased");
        let payload = owned
            .get(2)
            .and_then(|buffer| match buffer {
                OwnedBuffer::Bytes(bytes) => Some(bytes.as_slice()),
                OwnedBuffer::F64(_) => None,
            })
            .expect("owned BinaryView payload");
        assert_eq!(payload, &data[2..22]);
        Ok(())
    })
    .unwrap();
}

#[test]
fn binary_view_rejects_span_beyond_declared_variadic_buffer_size() {
    // The backing allocation is 21 bytes, but the mandatory sizes table
    // declares only one byte. Old code read the external prefix anyway.
    let mut view = [0_u8; 16];
    view[0..4].copy_from_slice(&13_i32.to_le_bytes());
    view[4..8].copy_from_slice(&[0, 0, 0, 0]);
    view[8..12].copy_from_slice(&0_i32.to_le_bytes());
    view[12..16].copy_from_slice(&0_i32.to_le_bytes());
    let data = [0_u8; 21];
    let sizes = [1_i64];
    let buffers: [*const c_void; 4] = [
        ptr::null(),
        view.as_ptr().cast(),
        data.as_ptr().cast(),
        sizes.as_ptr().cast(),
    ];
    let mut array = stack_array(1, 0, 0, 4, 0, buffers.as_ptr(), ptr::null_mut());
    assert!(layout_with_owned_content(&mut array, ArrowFormat::BinaryView, 0).is_err());
}
