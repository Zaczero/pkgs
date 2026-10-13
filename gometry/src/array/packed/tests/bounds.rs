use super::*;

#[cfg(test)]
mod element_bounds_cache_tests {

    use super::*;
    use crate::array::BoundsSeed;
    use crate::heap_size::HeapSize as _;
    use crate::test_support::allocation_count;

    fn lines() -> PyGeometryArray {
        PyGeometryArray::packed_lines(
            CoordSeq::try_from_vecs(
                vec![0.0, 1.0, 10.0, 11.0],
                vec![0.0, 0.0, 10.0, 10.0],
                None,
                None,
            )
            .unwrap(),
            CsrOffsetColumn::try_new(vec![0, 2, 4], 4).unwrap(),
            Frame::None,
        )
        .unwrap()
    }

    fn single_line() -> PyGeometryArray {
        PyGeometryArray::packed_lines(
            CoordSeq::try_from_vecs(vec![0.0, 1.0], vec![0.0, 0.0], None, None).unwrap(),
            CsrOffsetColumn::try_new(vec![0, 2], 2).unwrap(),
            Frame::None,
        )
        .unwrap()
    }

    #[test]
    fn mixed_element_bounds_decline_without_allocating() {
        let array = PyGeometryArray::from_shapes(
            vec![
                Shape::Point(Point::new_raw_xy(0.0, 0.0)),
                Shape::empty_polygon(),
            ],
            Frame::None,
        )
        .unwrap();
        let (bounds, allocations) = allocation_count(|| array.element_bounds());
        assert!(bounds.is_none());
        assert_eq!(allocations, 0);
    }

    #[test]
    fn cold_row_seed_does_not_materialize_element_bounds() {
        let array = lines();
        let (seed, allocations) = allocation_count(|| array.row_bounds_seed(0));

        assert!(matches!(seed, BoundsSeed::Unset));
        assert_eq!(allocations, 0);
        assert!(array.existing_element_bounds().is_none());
    }

    #[test]
    fn warm_lookup_returns_the_published_element_bounds() {
        let array = lines();
        let published = array
            .element_bounds()
            .expect("packed lines publish element bounds");
        let (lookup, allocations) = allocation_count(|| {
            array
                .existing_element_bounds()
                .expect("published bounds remain available")
        });

        assert_eq!(allocations, 0);
        assert!(Arc::ptr_eq(&published, &lookup));
        assert_eq!(lookup.len(), 2);
        assert_eq!(lookup[0], Some(Bounds::new_raw(0.0, 0.0, 1.0, 0.0)));
        assert_eq!(lookup[1], Some(Bounds::new_raw(10.0, 10.0, 11.0, 10.0)));
    }

    #[test]
    fn retained_size_includes_published_element_bounds() {
        let array = lines();
        let cold = array.heap_bytes();
        array
            .element_bounds()
            .expect("non-empty lines publish bounds");
        assert!(array.heap_bytes() > cold);
    }

    #[test]
    fn required_bounds_keep_compact_nullable_rows_logically_aligned() {
        let present = single_line();
        let array = PyGeometryArray::scatter_present_rows(
            &present,
            ValidityMask::from_vec(2, vec![false, true]).unwrap(),
        )
        .unwrap();
        let bounds = array
            .element_bounds()
            .expect("compact lines publish element bounds");

        assert_eq!(bounds.len(), 2);
        assert_eq!(bounds[0], Some(Bounds::new_raw(0.0, 0.0, 1.0, 0.0)));
        assert_eq!(bounds[1], None);
    }
}
