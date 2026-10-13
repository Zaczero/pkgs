use super::*;

fn raw_nested(depth: usize) -> Shape {
    let mut shape = Shape::Point(Point::new_raw_xy(0.0, 0.0));
    for _ in 0..depth {
        shape = Shape::geometry_collection(vec![shape]).expect("test depth is within limit");
    }
    shape
}

fn nested_around(mut shape: Shape, depth: usize) -> Shape {
    for _ in 0..depth {
        shape = Shape::geometry_collection(vec![shape]).expect("test depth is within limit");
    }
    shape
}

#[test]
fn collection_admission_has_one_inclusive_depth_boundary() {
    assert_eq!(raw_nested(MAX_NESTING).nesting_depth(), MAX_NESTING);
    Shape::geometry_collection(vec![raw_nested(MAX_NESTING - 1)]).unwrap();
    Shape::geometry_collection(vec![raw_nested(MAX_NESTING)]).unwrap_err();
}

#[test]
fn collection_depth_uses_the_deepest_direct_child() {
    let shallow = raw_nested(1);
    let deep = raw_nested(3);
    let collection = Shape::geometry_collection(vec![shallow, deep]).unwrap();
    assert_eq!(collection.nesting_depth(), 4);
}

#[test]
fn collection_topological_dimension_tracks_members_and_rebuilds() {
    let curve = Shape::geometry_collection(vec![Shape::typed_empty(
        GeometryKind::LineString,
        CoordinateAxes::XY,
    )])
    .unwrap();
    let source = Shape::geometry_collection(vec![
        Shape::empty_point(),
        curve.clone(),
        Shape::typed_empty(GeometryKind::Polygon, CoordinateAxes::XY),
    ])
    .unwrap();
    assert_eq!(source.topological_dimension(), Dimension::Surface);

    let Shape::GeometryCollection(collection) = source else {
        unreachable!("collection fixture")
    };
    let rebuilt = collection.try_rebuild_members(vec![curve]).unwrap();
    assert_eq!(
        Shape::GeometryCollection(rebuilt).topological_dimension(),
        Dimension::Curve
    );
}

#[test]
fn structure_preserving_rebuild_recomputes_after_filtering() {
    let source = Shape::geometry_collection(vec![raw_nested(3), Shape::empty_point()]).unwrap();
    let Shape::GeometryCollection(collection) = &source else {
        unreachable!("collection fixture")
    };
    let rebuilt = collection
        .try_rebuild_members(vec![Shape::empty_point()])
        .unwrap();
    assert_eq!(Shape::GeometryCollection(rebuilt).nesting_depth(), 1);
}

#[test]
fn structure_preserving_rebuild_rejects_depth_growth() {
    let source = nested_around(Shape::Point(Point::new_raw_xy(0.0, 0.0)), 2);
    let Shape::GeometryCollection(collection) = &source else {
        unreachable!("collection fixture")
    };
    collection
        .try_map_members(|_| Ok::<_, crate::error::Error>(raw_nested(2)))
        .unwrap_err();
}

#[cfg(test)]
mod mapping_error_tests {
    use super::*;

    fn two_point_collection() -> GeometryCollection {
        let Shape::GeometryCollection(collection) = Shape::geometry_collection(vec![
            Shape::Point(Point::new_raw_xy(0.0, 0.0)),
            Shape::Point(Point::new_raw_xy(1.0, 0.0)),
        ])
        .unwrap() else {
            unreachable!("collection fixture")
        };
        collection
    }

    #[test]
    #[expect(
        clippy::float_cmp,
        reason = "fixture identity selects the second stored point, not a nearby ordinate"
    )]
    fn try_map_members_reports_callback_error_in_member_order() {
        let collection = two_point_collection();
        let mut seen = 0;
        let error = collection
            .try_map_members(|shape| {
                seen += 1;
                if let Shape::Point(point) = shape
                    && point.x == 1.0
                {
                    Err(GeometryErrorKind::message("second member"))
                } else {
                    Ok(shape.clone())
                }
            })
            .unwrap_err();
        assert_eq!(seen, 2);
        assert!(error.to_string().contains("second member"));
    }

    #[test]
    fn try_filter_map_owned_members_empty_is_typed_empty_collection() {
        let collection = two_point_collection();
        let filtered = collection
            .try_filter_map_owned_members(|_| Ok(None))
            .unwrap();
        assert!(matches!(
            filtered,
            Shape::Empty(GeometryKind::GeometryCollection, CoordinateAxes::XY)
        ));
    }

    #[test]
    #[expect(
        clippy::float_cmp,
        reason = "fixture identity selects the second stored point, not a nearby ordinate"
    )]
    fn try_filter_map_owned_members_preserves_kept_order() {
        let collection = two_point_collection();
        let filtered = collection
            .try_filter_map_owned_members(|shape| {
                Ok(matches!(&shape, Shape::Point(point) if point.x == 1.0).then_some(shape))
            })
            .unwrap();
        let Shape::GeometryCollection(collection) = filtered else {
            unreachable!("kept one member")
        };
        assert_eq!(collection.len(), 1);
        assert_eq!(collection.as_slice(), &[Shape::Point(Point::new_raw_xy(
            1.0, 0.0
        ))]);
    }
}
