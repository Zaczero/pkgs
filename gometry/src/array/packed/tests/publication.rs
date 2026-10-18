use super::*;

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn compact_bulk_publication_and_validation_use_the_cursor() {
    let present = PyGeometryArray::packed_points(
        CoordSeq::try_from_vecs(
            (0..10_000).map(f64::from).collect(),
            (0..10_000).map(|row| f64::from(row * 2)).collect(),
            None,
            None,
        )
        .unwrap(),
        Frame::None,
    )
    .unwrap();
    let array = PyGeometryArray::scatter_present_rows(
        &present,
        ValidityMask::from_vec(10_001, (0..10_001).map(|row| row == 9_999).collect()).unwrap(),
    )
    .unwrap();

    missing::reset_count_valid_bytes();
    pyo3::Python::initialize();
    pyo3::Python::attach(|py| {
        drop(array.to_numpy(py).unwrap());
    });
    assert_eq!(missing::count_valid_bytes(), 0);

    missing::reset_count_valid_bytes();
    pyo3::Python::attach(|py| array.validate_impl(py)).unwrap();
    assert_eq!(missing::count_valid_bytes(), 0);
}

#[test]
fn logical_nullable_point_publication_compacts_hidden_payload() {
    let storage = Arc::new(GeometryArrayStorage::Packed(PackedStorage::Points {
        coords: Arc::new(
            CoordSeq::try_from_vecs(
                vec![901.0, 10.0, 802.0, 20.0],
                vec![91.0, 1.0, 82.0, 2.0],
                None,
                None,
            )
            .unwrap(),
        ),
        row_map: RowSelection::Identity,
    }));
    let source = PyGeometryArray::from_result_storage(
        storage,
        Frame::None,
        Some(ValidityMask::from_vec(4, vec![true, false, true, false]).unwrap()),
        StorageLayout::LogicalRows,
        ComputedCaches::fresh(),
    )
    .unwrap();
    assert_eq!(logical_point_xs(&source), [
        None,
        Some(10.0),
        None,
        Some(20.0)
    ]);

    let gathered = source.gather_logical_rows(&[3, 0, 1, 3]).unwrap();
    assert_eq!(logical_point_xs(&gathered), [
        Some(20.0),
        None,
        Some(10.0),
        Some(20.0)
    ]);

    let columns = source.packed_columns().expect("admitted point view");
    let PackedColumnOutput::Points(mapped) =
        map_selected_points(columns, CoordinateAxes::XY, |point| {
            Ok(Point::new_raw_xy(point.x + 3.0, point.y))
        })
        .unwrap()
    else {
        panic!("point mapping preserves packed points");
    };
    assert_eq!(mapped.xs(), &[13.0, 23.0]);
    let mapped = PyGeometryArray::from_result_storage(
        Arc::new(GeometryArrayStorage::Packed(PackedStorage::Points {
            coords: Arc::new(mapped),
            row_map: RowSelection::Identity,
        })),
        Frame::None,
        source.missing().cloned(),
        StorageLayout::PresentOnly,
        ComputedCaches::fresh(),
    )
    .unwrap();
    assert_eq!(logical_point_xs(&mapped), [
        None,
        Some(13.0),
        None,
        Some(23.0)
    ]);
}

#[cfg(test)]
mod computed_triangle_publication_tests {
    use super::*;
    use crate::crs::frame::crs_arc_static;

    #[test]
    fn geographic_packed_triangles_publish_variable_ring_offsets() {
        let vertices = [
            Point::new_raw_xy(10.0, 20.0),
            Point::new_raw_xy(260.0, 40.0),
            Point::new_raw_xy(260.0, 60.0),
            Point::new_raw_xy(10.0, 20.0),
        ];
        let domain = crate::geometry::TopologyDomain::Geographic(
            crate::geographic::GeographicFrame::crs84(),
        );
        let context = crate::geometry::SourceArcContext::new(domain, None);
        let mut budget = crate::geometry::ExpansionBudget::new("triangulate", "method");
        let published = crate::geometry::publish_triangle_vertex_stream_budgeted(
            &vertices,
            crate::geographic::GeographicFrame::crs84(),
            &context,
            &mut budget,
        )
        .unwrap();
        let array = PyGeometryArray::packed_triangles_with_ring_ends(
            &published.points,
            &published.ring_ends,
            Frame::Crs(crs_arc_static("EPSG:4326")),
        )
        .unwrap();
        let GeometryArrayStorage::Packed(PackedStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            ..
        }) = array.storage()
        else {
            panic!("triangle output remains packed polygons");
        };
        assert_eq!(ring_offsets.as_slice(), &[0, 6]);
        assert_eq!(polygon_offsets.as_slice(), &[0, 1]);
        assert_eq!(coords.iter().collect::<Vec<_>>(), [
            Point::new_raw_xy(10.0, 20.0),
            Point::new_raw_xy(135.0, 30.0),
            Point::new_raw_xy(-100.0, 40.0),
            Point::new_raw_xy(-100.0, 60.0),
            Point::new_raw_xy(135.0, 40.0),
            Point::new_raw_xy(10.0, 20.0),
        ]);
    }
}
