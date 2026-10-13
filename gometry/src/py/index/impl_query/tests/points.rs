    #[test]
fn compact_nullable_point_index_resolves_sparse_handles_without_span_rank() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let sys = py.import("sys").expect("sys module");
            let copy_module = py.import("copy").expect("copy module");
            let mut retained_deltas = Vec::new();
            let mut allocation_counts = Vec::new();

            for logical_len in [1_000_usize, 1_000_000, 100_000_000] {
                let handles = [7_usize, 63, logical_len.wrapping_sub(1)];
                let mut bitmap = vec![0_u8; logical_len.div_ceil(8)];
                for &handle in &handles {
                    *bitmap
                        .get_mut(handle / 8)
                        .expect("present handle is within the bitmap") |= 1 << (handle % 8);
                }
                let mask = ValidityMask::from_bitmap(SharedSlice::from_vec(bitmap), 0, logical_len)
                    .expect("the fixture contains missing rows");
                let present = PyGeometryArray::packed_points(
                    CoordSeq::try_from_vecs(
                        vec![10.0, 20.0, 30.0],
                        vec![1.0, 2.0, 3.0],
                        None,
                        None,
                    )
                    .expect("three present point rows"),
                    Frame::None,
                )
                .expect("packed present points");
                let source_array = PyGeometryArray::scatter_present_rows(&present, mask)
                    .expect("compact nullable point source");
                assert_eq!(source_array.storage().len(), handles.len());
                assert_eq!(
                    source_array.missing().unwrap().present_count(),
                    handles.len()
                );
                let source = Bound::new(py, source_array)
                    .expect("source array binds")
                    .into_any();
                let native_index = build_spatial_index(py, &source)
                    .expect("public spatial-index construction path");
                assert_eq!(native_index.rows.next_handle, logical_len);
                let packed = native_index
                    .rows
                    .packed
                    .as_ref()
                    .expect("compact points remain packed");
                let directory = match &packed.addressing {
                    super::super::PackedRowAddressing::PresentOrdinals(directory) => directory,
                    super::super::PackedRowAddressing::Logical => {
                        panic!("compact nullable points require original-present ordinals")
                    },
                };
                assert_eq!(&**directory, &handles);
                assert_eq!(directory.len(), handles.len());
                assert_eq!(native_index.live_handles_sorted(), handles);
                let index = Bound::new(py, native_index).expect("index binds");
                let builtins = py.import("builtins").expect("builtins module");
                reset_count_valid_bytes();
                let listed = builtins
                    .getattr("list")
                    .unwrap()
                    .call1((&index,))
                    .expect("public index iteration");
                assert_eq!(
                    listed.extract::<Vec<usize>>().unwrap(),
                    handles,
                    "iteration preserves original present IDs at M={logical_len}"
                );
                assert_no_compact_point_rank(logical_len, "iteration");

                reset_count_valid_bytes();
                let source_size = sys
                    .call_method1("getsizeof", (&source,))
                    .expect("source size")
                    .extract::<usize>()
                    .expect("source size is an integer");
                let index_size = sys
                    .call_method1("getsizeof", (&index,))
                    .expect("index size")
                    .extract::<usize>()
                    .expect("index size is an integer");
                let retained_delta = index_size
                    .checked_sub(source_size)
                    .expect("the index includes its retained source");
                assert_no_compact_point_rank(logical_len, "size");
                retained_deltas.push(retained_delta);

                let rows = [
                    (7, 10.0, 1.0),
                    (63, 20.0, 2.0),
                    (logical_len - 1, 30.0, 3.0),
                ];
                let queries: Vec<_> = rows
                    .iter()
                    .map(|&(_, x, y)| public_point(py, x, y))
                    .collect();
                let dwithin = PyDict::new(py);
                dwithin
                    .set_item("predicate", "dwithin")
                    .expect("dwithin predicate");
                dwithin.set_item("distance", 0.0).expect("zero distance");

                // The first public lookup is deliberately cold: it must not
                // construct the source-wide compact validity rank directory.
                reset_count_valid_bytes();
                let first = index
                    .call_method1("__getitem__", (7_usize,))
                    .expect("cold public index lookup");
                assert_no_compact_point_rank(logical_len, "cold lookup");
                assert_eq!(first.getattr("x").unwrap().extract::<f64>().unwrap(), 10.0);
                assert_eq!(first.getattr("y").unwrap().extract::<f64>().unwrap(), 1.0);

                let mut validity_counts = Vec::new();
                for ((handle, x, y), query) in rows.iter().zip(&queries) {
                    reset_count_valid_bytes();
                    let item = index
                        .call_method1("__getitem__", (*handle,))
                        .expect("public index lookup");
                    assert_no_compact_point_rank(logical_len, "lookup");
                    assert_eq!(item.getattr("x").unwrap().extract::<f64>().unwrap(), *x);
                    assert_eq!(item.getattr("y").unwrap().extract::<f64>().unwrap(), *y);
                    validity_counts.push(count_valid_bytes());

                    reset_count_valid_bytes();
                    let matched = index
                        .call_method1("query", (query,))
                        .expect("public exact query");
                    assert_eq!(public_ids(&matched), [i64::try_from(*handle).unwrap()]);
                    assert_no_compact_point_rank(logical_len, "query");
                    validity_counts.push(count_valid_bytes());

                    reset_count_valid_bytes();
                    let nearest = index
                        .call_method1("nearest", (query,))
                        .expect("public nearest");
                    assert_eq!(public_ids(&nearest), [i64::try_from(*handle).unwrap()]);
                    assert_no_compact_point_rank(logical_len, "nearest");
                    validity_counts.push(count_valid_bytes());

                    reset_count_valid_bytes();
                    let within = index
                        .call_method("query", (query,), Some(&dwithin))
                        .expect("public exact dwithin");
                    assert_eq!(public_ids(&within), [i64::try_from(*handle).unwrap()]);
                    assert_no_compact_point_rank(logical_len, "dwithin");
                    validity_counts.push(count_valid_bytes());
                }

                let (looked_up, lookup_allocations) = crate::test_support::allocation_count(|| {
                    index
                        .call_method1("__getitem__", (7_usize,))
                        .expect("warmed public lookup")
                });
                assert_no_compact_point_rank(logical_len, "measured lookup");
                drop(looked_up);
                let (matched, query_allocations) = crate::test_support::allocation_count(|| {
                    index
                        .call_method1("query", (&queries[0],))
                        .expect("warmed public query")
                });
                assert_eq!(public_ids(&matched), [7]);
                assert_no_compact_point_rank(logical_len, "measured query");
                let (nearest, nearest_allocations) = crate::test_support::allocation_count(|| {
                    index
                        .call_method1("nearest", (&queries[0],))
                        .expect("warmed public nearest")
                });
                assert_eq!(public_ids(&nearest), [7]);
                assert_no_compact_point_rank(logical_len, "measured nearest");
                let (within, dwithin_allocations) = crate::test_support::allocation_count(|| {
                    index
                        .call_method("query", (&queries[0],), Some(&dwithin))
                        .expect("warmed public dwithin")
                });
                assert_eq!(public_ids(&within), [7]);
                assert_no_compact_point_rank(logical_len, "measured dwithin");
                allocation_counts.push([
                    lookup_allocations,
                    query_allocations,
                    nearest_allocations,
                    dwithin_allocations,
                    0,
                    0,
                    0,
                ]);

                reset_count_valid_bytes();
                let (copied, copy_allocations) = crate::test_support::allocation_count(|| {
                    copy_module
                        .call_method1("copy", (&index,))
                        .expect("public copy")
                });
                assert_no_compact_point_rank(logical_len, "copy");
                allocation_counts.last_mut().unwrap()[4] = copy_allocations;
                reset_count_valid_bytes();
                assert!(
                    copied
                        .call_method1("remove", (7_usize,))
                        .expect("remove from copy")
                        .extract::<bool>()
                        .expect("remove returns bool")
                );
                assert_no_compact_point_rank(logical_len, "copy removal");
                reset_count_valid_bytes();
                assert_eq!(
                    public_ids(&index.call_method1("query", (&queries[0],)).unwrap()),
                    [7]
                );
                assert_no_compact_point_rank(logical_len, "query after copy removal");

                reset_count_valid_bytes();
                let (payload, pickle_write_allocations) =
                    crate::test_support::allocation_count(|| {
                        let index = index.borrow();
                        let (values, handles) = index
                            .rows
                            .pickle_live_rows(index.metadata.clone().unwrap_or_default())
                            .expect("index pickle row payload");
                        (
                            values,
                            handles,
                            index.rows.next_handle,
                            index.metadata.is_some(),
                        )
                    });
                assert_no_compact_point_rank(logical_len, "pickle payload");
                allocation_counts.last_mut().unwrap()[5] = pickle_write_allocations;
                reset_count_valid_bytes();
                let (restored, pickle_restore_allocations) =
                    crate::test_support::allocation_count(|| {
                        restore_spatial_index_v2(
                            &payload.0,
                            &payload.1,
                            payload.2,
                            payload.3,
                        )
                        .expect("direct index reconstruction")
                    });
                assert_no_compact_point_rank(logical_len, "pickle reconstruction");
                allocation_counts.last_mut().unwrap()[6] = pickle_restore_allocations;
                let restored = Bound::new(py, restored).expect("restored index binds");
                let restored_low = restored.call_method1("__getitem__", (7_usize,)).unwrap();
                assert_eq!(
                    public_ids(&restored.call_method1("query", (&restored_low,)).unwrap()),
                    [7]
                );

                let restored_high = restored
                    .call_method1("__getitem__", (logical_len - 1,))
                    .unwrap();
                assert_eq!(
                    public_ids(&restored.call_method1("query", (&restored_high,)).unwrap()),
                    [i64::try_from(logical_len - 1).unwrap()]
                );
                reset_count_valid_bytes();
                let restored_next = restored
                    .call_method1("insert", (&restored_high,))
                    .expect("restored index insert")
                    .extract::<usize>()
                    .expect("insert returns a handle");
                assert_eq!(restored_next, logical_len);
                assert_no_compact_point_rank(logical_len, "restored insertion");
                reset_count_valid_bytes();
                assert_eq!(
                    public_ids(&restored.call_method1("query", (&restored_high,)).unwrap()),
                    [
                        i64::try_from(logical_len - 1).unwrap(),
                        i64::try_from(logical_len).unwrap(),
                    ]
                );
                assert_no_compact_point_rank(logical_len, "restored boxed query");

                for handle in [7_usize, 63] {
                    reset_count_valid_bytes();
                    assert!(
                        index
                            .call_method1("remove", (handle,))
                            .expect("public removal")
                            .extract::<bool>()
                            .expect("remove returns bool")
                    );
                    assert_no_compact_point_rank(logical_len, "remove");
                }
                reset_count_valid_bytes();
                let last = index
                    .call_method1("__getitem__", (logical_len - 1,))
                    .expect("last sparse point remains addressable");
                assert_eq!(last.getattr("x").unwrap().extract::<f64>().unwrap(), 30.0);
                assert_no_compact_point_rank(logical_len, "lookup after removals");
                reset_count_valid_bytes();
                let last_nearest = index
                    .call_method1("nearest", (&queries[2],))
                    .expect("last point remains nearest");
                assert_eq!(public_ids(&last_nearest), [
                    i64::try_from(logical_len - 1).unwrap()
                ]);
                assert_no_compact_point_rank(logical_len, "nearest after removals");

                let inserted_point = public_point(py, 40.0, 4.0);
                reset_count_valid_bytes();
                let inserted = index
                    .call_method1("insert", (&inserted_point,))
                    .expect("boxed insertion")
                    .extract::<usize>()
                    .expect("insert returns a handle");
                assert_eq!(inserted, logical_len);
                assert_no_compact_point_rank(logical_len, "boxed insertion");
                reset_count_valid_bytes();
                let inserted_query = index
                    .call_method1("query", (&inserted_point,))
                    .expect("boxed query after packed rows");
                assert_eq!(public_ids(&inserted_query), [
                    i64::try_from(logical_len).unwrap()
                ]);
                assert_no_compact_point_rank(logical_len, "boxed query");

                eprintln!(
                    "compact_point_span M={logical_len} retained_delta={retained_delta} validity_counts={validity_counts:?} allocation_requests={:?}",
                    allocation_counts.last().expect("measured operations")
                );
            }

            assert!(
                retained_deltas
                    .array_windows::<2>()
                    .all(|[previous, next]| previous == next),
                "index-minus-source retained heap must not grow with M: {retained_deltas:?}"
            );
            assert!(
                allocation_counts
                    .array_windows::<2>()
                    .all(|[previous, next]| previous == next),
                "warmed public operation allocation requests must not grow with M: {allocation_counts:?}"
            );
        });
    }
