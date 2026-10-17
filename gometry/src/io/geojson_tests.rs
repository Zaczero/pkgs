//! RFC 7946 conformance tests for the GeoJSON writer/reader.
//! RFC 7946 conformance: right-hand ring winding on output, the WGS84
//! lon/lat domain on read and write, and antimeridian cutting on output.

use serde_json::{Value, json};

use super::to_geojson_string_with_limit;
use crate::error::{ErrorKind, ResourceError};
use crate::io::*;
use crate::test_support::allocation_count;

fn point(x: f64, y: f64) -> Point {
    Point::new(x, y).expect("finite test coordinate")
}

fn ring(coords: &[(f64, f64)]) -> Ring {
    Ring::closed(coords.iter().map(|&(x, y)| point(x, y)).collect()).expect("closed test ring")
}

fn xyz_ring(coords: &[(f64, f64)], z: f64) -> Ring {
    Ring::closed(
        coords
            .iter()
            .map(|&(x, y)| {
                Point::new_axes(
                    x,
                    y,
                    crate::geometry::ZOrdinate(Some(z)),
                    crate::geometry::MOrdinate(None),
                )
                .expect("finite XYZ test coordinate")
            })
            .collect(),
    )
    .expect("closed XYZ test ring")
}

fn mixed_ring_axes_polygon() -> Polygon {
    Polygon::new(
        xyz_ring(
            &[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)],
            3.0,
        ),
        vec![ring(&[
            (1.0, 1.0),
            (1.0, 2.0),
            (2.0, 2.0),
            (2.0, 1.0),
            (1.0, 1.0),
        ])],
    )
}

fn assert_mixed_ring_axes_error(shape: &Shape) {
    let error = to_geojson_string(shape, crate::geometry::TopologyDomain::Cartesian)
        .expect_err("mixed polygon ring axes must be rejected by the writer");
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(crate::geometry::GeometryErrorKind::Invalid(_))
    ));
}

#[test]
fn checked_writer_runs_when_geojson_upper_bound_exceeds_fitting_lower_bound() {
    let shape = Shape::Point(Point::new_raw_xy(f64::MAX, 0.0));
    let limit = 50;
    let summary = geojson_text_summary(&shape);
    assert!(matches!(
        summary.geojson_lower_bound(limit),
        TextWidthBound::Within { lower_bound, .. } if lower_bound <= limit
    ));
    assert!(
        summary
            .geojson_upper_bound()
            .is_some_and(|upper_bound| upper_bound > limit)
    );

    let error =
        to_geojson_string_with_limit(&shape, crate::geometry::TopologyDomain::Cartesian, limit)
            .expect_err("GeoJSON position exceeds the output limit");
    assert!(matches!(
        error.kind(),
        ErrorKind::Resource(ResourceError::SizeOverflow)
    ));
}

#[test]
fn checked_geojson_accepts_exact_byte_limit_and_rejects_one_byte_less() {
    let shape = Shape::Point(Point::new_raw_xy(1.5, 2.5));
    let expected = r#"{"type":"Point","coordinates":[1.5,2.5]}"#;
    assert_eq!(
        to_geojson_string_with_limit(
            &shape,
            crate::geometry::TopologyDomain::Cartesian,
            expected.len(),
        )
        .expect("exact output width fits"),
        expected
    );
    let error = to_geojson_string_with_limit(
        &shape,
        crate::geometry::TopologyDomain::Cartesian,
        expected.len() - 1,
    )
    .expect_err("one byte less than the output width fails");
    assert!(matches!(
        error.kind(),
        ErrorKind::Resource(ResourceError::SizeOverflow)
    ));
}

#[test]
fn checked_geojson_charges_leading_comma_with_the_complete_position() {
    let expected = "[,[-0.0,2.5,3.5]";
    let mut too_small = super::CheckedGeoJson::with_capacity_hint(0, expected.len() - 1);
    too_small.text.push_str("[").expect("prefix fits");
    let error = super::GeoJsonSink::position(&mut too_small, -0.0, 2.5, Some(3.5), true)
        .expect_err("incomplete position does not fit");
    assert!(matches!(
        error.kind(),
        ErrorKind::Resource(ResourceError::SizeOverflow)
    ));
    assert_eq!(too_small.text.as_str(), "[");

    let mut exact = super::CheckedGeoJson::with_capacity_hint(0, expected.len());
    exact.text.push_str("[").expect("prefix fits");
    super::GeoJsonSink::position(&mut exact, -0.0, 2.5, Some(3.5), true)
        .expect("complete comma and position fit");
    assert_eq!(exact.text.as_str(), expected);
}

#[test]
fn geojson_upper_bound_covers_each_geometry_kind() {
    for source in [
        "POINT (0 0)",
        "POINT EMPTY",
        "MULTIPOINT ((0 0), (1 1))",
        "LINESTRING (0 0, 1 1)",
        "MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))",
        "POLYGON ((0 0, 2 0, 2 2, 0 0), (0.5 0.5, 1 0.5, 1 1, 0.5 0.5))",
        "MULTIPOLYGON (((0 0, 2 0, 2 2, 0 0)), ((3 3, 4 3, 4 4, 3 3)))",
        "MULTIPOLYGON EMPTY",
        "GEOMETRYCOLLECTION (POINT (0 0), GEOMETRYCOLLECTION (POINT EMPTY))",
        "GEOMETRYCOLLECTION EMPTY",
    ] {
        let shape = parse_wkt(source).expect("fixture WKT parses");
        let output = to_geojson_string(&shape, crate::geometry::TopologyDomain::Cartesian)
            .expect("fixture GeoJSON serializes");
        let upper_bound = geojson_text_summary(&shape)
            .geojson_upper_bound()
            .expect("fixture upper bound is representable");
        assert!(
            output.len() <= upper_bound,
            "upper bound {upper_bound} is below output length {} for {source}",
            output.len()
        );
    }
}

#[test]
fn checked_geojson_writer_runs_when_upper_bound_exceeds_fitting_lower_bound() {
    let small_shape = Shape::Point(Point::new_raw_xy(0.0, 0.0));
    let small_limit = 40;
    let small_summary = geojson_text_summary(&small_shape);
    assert!(matches!(
        small_summary.geojson_lower_bound(small_limit),
        TextWidthBound::Within { lower_bound, .. } if lower_bound <= small_limit
    ));
    assert!(
        small_summary
            .geojson_upper_bound()
            .is_some_and(|upper_bound| upper_bound > small_limit)
    );
    assert_eq!(
        to_geojson_string_with_limit(
            &small_shape,
            crate::geometry::TopologyDomain::Cartesian,
            small_limit,
        )
        .expect("checked fallback emits a Point below the limit"),
        to_geojson_string(&small_shape, crate::geometry::TopologyDomain::Cartesian)
            .expect("base writer emits a Point")
    );

    let shape = Shape::Point(Point::new_raw_xy(f64::MAX, 0.0));
    let limit = 50;
    let summary = geojson_text_summary(&shape);
    assert!(matches!(
        summary.geojson_lower_bound(limit),
        TextWidthBound::Within { lower_bound, .. } if lower_bound <= limit
    ));
    assert!(
        summary
            .geojson_upper_bound()
            .is_some_and(|upper_bound| upper_bound > limit)
    );

    let error =
        to_geojson_string_with_limit(&shape, crate::geometry::TopologyDomain::Cartesian, limit)
            .expect_err("GeoJSON position exceeds the output limit");
    assert!(matches!(
        error.kind(),
        ErrorKind::Resource(ResourceError::SizeOverflow)
    ));
}

fn line(coords: &[(f64, f64)]) -> Shape {
    Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(
                &coords.iter().map(|&(x, y)| point(x, y)).collect::<Vec<_>>(),
            )
            .unwrap(),
        )
        .expect("test line has two vertices"),
    )
}

fn geographic_domain() -> crate::geometry::TopologyDomain {
    crate::geometry::TopologyDomain::Geographic(
        crate::crs::info("EPSG:4326")
            .unwrap()
            .geographic_frame()
            .unwrap(),
    )
}

fn written(shape: &Shape, domain: crate::geometry::TopologyDomain) -> Value {
    let text = to_geojson_string(shape, domain).expect("serializes");
    serde_json::from_str(&text).expect("emitted GeoJSON parses")
}

/// Signed area of a coordinate ring (positive == counterclockwise).
fn ring_signed_area(ring: &Value) -> f64 {
    let pts: Vec<(f64, f64)> = ring
        .as_array()
        .unwrap()
        .iter()
        .map(|p| {
            let a = p.as_array().unwrap();
            (a[0].as_f64().unwrap(), a[1].as_f64().unwrap())
        })
        .collect();
    pts.array_windows::<2>()
        .map(|[a, b]| a.0 * b.1 - b.0 * a.1)
        .sum::<f64>()
        / 2.0
}

#[test]
fn clockwise_shell_serializes_counterclockwise() {
    // A clockwise unit square exterior (negative signed area).
    let cw_shell = Shape::Polygon(Polygon::new(
        ring(&[(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0), (0.0, 0.0)]),
        vec![],
    ));
    let out = written(&cw_shell, crate::geometry::TopologyDomain::Cartesian);
    assert_eq!(out["type"], "Polygon");
    let shell = &out["coordinates"][0];
    assert!(
        ring_signed_area(shell) > 0.0,
        "RFC 7946 §3.1.6 requires a counterclockwise exterior ring, got {shell}"
    );
}

#[test]
fn holes_serialize_clockwise() {
    // Both rings authored counterclockwise; the hole must flip to CW.
    let poly = Shape::Polygon(Polygon::new(
        ring(&[
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ]),
        vec![ring(&[
            (2.0, 2.0),
            (4.0, 2.0),
            (4.0, 4.0),
            (2.0, 4.0),
            (2.0, 2.0),
        ])],
    ));
    let out = written(&poly, crate::geometry::TopologyDomain::Cartesian);
    assert!(ring_signed_area(&out["coordinates"][0]) > 0.0, "shell CCW");
    assert!(
        ring_signed_area(&out["coordinates"][1]) < 0.0,
        "RFC 7946 §3.1.6 requires clockwise interior rings"
    );
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn geographic_geojson_orientation_keeps_full_records_and_closure() {
    let xyz_ring = |coordinates: &[(f64, f64, f64)]| {
        Ring::closed(
            coordinates
                .iter()
                .map(|&(x, y, z)| {
                    Point::new_axes(
                        x,
                        y,
                        crate::geometry::ZOrdinate(Some(z)),
                        crate::geometry::MOrdinate(None),
                    )
                    .expect("finite XYZ test coordinate")
                })
                .collect(),
        )
        .expect("closed XYZ test ring")
    };
    let source = Shape::Polygon(Polygon::new(
        xyz_ring(&[
            (-0.0, 0.0, -0.0),
            (0.0, 2.0, 20.0),
            (2.0, 2.0, 30.0),
            (2.0, 0.0, 40.0),
            (-0.0, 0.0, -0.0),
        ]),
        vec![xyz_ring(&[
            (0.5, 0.5, 101.0),
            (1.5, 0.5, 102.0),
            (1.5, 1.5, 103.0),
            (0.5, 1.5, 104.0),
            (0.5, 0.5, 101.0),
        ])],
    ));
    let expected = Shape::Polygon(Polygon::new(
        xyz_ring(&[
            (-0.0, 0.0, -0.0),
            (2.0, 0.0, 40.0),
            (2.0, 2.0, 30.0),
            (0.0, 2.0, 20.0),
            (-0.0, 0.0, -0.0),
        ]),
        vec![xyz_ring(&[
            (0.5, 0.5, 101.0),
            (0.5, 1.5, 104.0),
            (1.5, 1.5, 103.0),
            (1.5, 0.5, 102.0),
            (0.5, 0.5, 101.0),
        ])],
    ));

    let text = super::to_geojson_string(&source, geographic_domain())
        .expect("oriented polygon serializes");
    assert!(text.contains("-0.0"));
    let reparsed = parse_geojson(&serde_json::from_str::<Value>(&text).unwrap())
        .expect("oriented text reparses");
    assert_eq!(
        crate::io::to_wkb(&reparsed, None, false).unwrap(),
        crate::io::to_wkb(&expected, None, false).unwrap()
    );
}

#[test]
fn multipolygon_and_collection_rings_are_right_handed() {
    let cw = ring(&[(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0), (0.0, 0.0)]);
    let multi = Shape::MultiPolygon(vec![Polygon::new(cw.clone(), vec![])].into());
    let out = written(&multi, crate::geometry::TopologyDomain::Cartesian);
    assert!(ring_signed_area(&out["coordinates"][0][0]) > 0.0);

    let collection = Shape::geometry_collection(vec![Shape::Polygon(Polygon::new(cw, vec![]))])
        .expect("test collection is valid");
    let out = written(&collection, crate::geometry::TopologyDomain::Cartesian);
    assert!(ring_signed_area(&out["geometries"][0]["coordinates"][0]) > 0.0);
}

#[test]
fn writer_rejects_mixed_polygon_ring_axes_at_the_serializer() {
    let shape = Shape::Polygon(mixed_ring_axes_polygon());
    assert_mixed_ring_axes_error(&shape);
}

#[test]
fn writer_rejects_mixed_polygon_ring_axes_through_nested_shapes() {
    let polygon = mixed_ring_axes_polygon();
    let multipolygon = Shape::MultiPolygon(
        crate::geometry::MultiPolygon::try_new(crate::geometry::CoordinateAxes::XYZ, vec![
            crate::geometry::MultiMember::Value(polygon.clone()),
        ])
        .expect("mixed-ring polygon has a uniform aggregate axes union"),
    );
    assert_mixed_ring_axes_error(&multipolygon);

    let collection =
        Shape::geometry_collection(vec![Shape::Point(point(8.0, 8.0)), Shape::Polygon(polygon)])
            .expect("test collection is valid");
    assert_mixed_ring_axes_error(&collection);
}

#[test]
fn writer_preserves_uniform_xyz_and_mixed_collection_axes() {
    let polygon = Shape::Polygon(Polygon::new(
        xyz_ring(
            &[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)],
            3.0,
        ),
        vec![xyz_ring(
            &[(1.0, 1.0), (1.0, 2.0), (2.0, 2.0), (2.0, 1.0), (1.0, 1.0)],
            7.0,
        )],
    ));
    let polygon_value: Value = serde_json::from_str(
        &to_geojson_string(&polygon, crate::geometry::TopologyDomain::Cartesian)
            .expect("uniform XYZ polygon serializes"),
    )
    .expect("uniform XYZ output is JSON");
    assert_eq!(polygon_value["coordinates"][0][0], json!([0.0, 0.0, 3.0]));
    assert_eq!(polygon_value["coordinates"][1][0], json!([1.0, 1.0, 7.0]));
    assert_eq!(
        parse_geojson(&polygon_value).expect("uniform XYZ output reads"),
        polygon
    );

    let xyz_point = Point::new_axes(
        3.0,
        4.0,
        crate::geometry::ZOrdinate(Some(7.0)),
        crate::geometry::MOrdinate(None),
    )
    .expect("finite XYZ test point");
    let collection =
        Shape::geometry_collection(vec![Shape::Point(point(1.0, 2.0)), Shape::Point(xyz_point)])
            .expect("mixed-axis collection is valid");
    let collection_value: Value = serde_json::from_str(
        &to_geojson_string(&collection, crate::geometry::TopologyDomain::Cartesian)
            .expect("mixed-axis collection serializes"),
    )
    .expect("mixed-axis collection output is JSON");
    assert_eq!(
        collection_value["geometries"][0]["coordinates"],
        json!([1.0, 2.0])
    );
    assert_eq!(
        collection_value["geometries"][1]["coordinates"],
        json!([3.0, 4.0, 7.0])
    );
    assert_eq!(
        parse_geojson(&collection_value).expect("mixed-axis output reads"),
        collection
    );
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn write_rejects_out_of_domain_geographic_positions() {
    // Latitude beyond ±90 and longitude beyond ±180 fail on a geographic
    // domain. The low-level serializer still accepts an explicitly selected
    // Cartesian domain for internal parity fixtures; public CRS-free writers
    // select the geographic domain instead.
    to_geojson_string(&Shape::Point(point(0.0, 91.0)), geographic_domain()).unwrap_err();
    to_geojson_string(&Shape::Point(point(181.0, 0.0)), geographic_domain()).unwrap_err();
    to_geojson_string(
        &Shape::Point(point(0.0, 91.0)),
        crate::geometry::TopologyDomain::Cartesian,
    )
    .unwrap();
}

#[test]
fn write_rejects_geojson_unrepresentable_axes_and_members() {
    let xyz_empty = Shape::typed_empty(
        crate::geometry::GeometryKind::Point,
        crate::geometry::CoordinateAxes::XYZ,
    );
    to_geojson_string(&xyz_empty, crate::geometry::TopologyDomain::Cartesian).unwrap_err();

    let measured = Shape::Point(
        Point::new_axes(
            1.0,
            2.0,
            crate::geometry::ZOrdinate(None),
            crate::geometry::MOrdinate(Some(3.0)),
        )
        .unwrap(),
    );
    to_geojson_string(&measured, crate::geometry::TopologyDomain::Cartesian).unwrap_err();

    let explicit_empty = parse_wkt("MULTIPOINT (EMPTY, (1 2))").unwrap();
    to_geojson_string(&explicit_empty, crate::geometry::TopologyDomain::Cartesian).unwrap_err();
}

#[test]
fn empty_geojson_coordinate_arrays_become_typed_empty_shapes() {
    let cases = [
        ("MultiPoint", crate::geometry::GeometryKind::MultiPoint),
        ("LineString", crate::geometry::GeometryKind::LineString),
        (
            "MultiLineString",
            crate::geometry::GeometryKind::MultiLineString,
        ),
        ("MultiPolygon", crate::geometry::GeometryKind::MultiPolygon),
    ];
    for (kind, expected) in cases {
        let value = json!({"type": kind, "coordinates": []});
        assert_eq!(
            parse_geojson(&value).unwrap(),
            Shape::typed_empty(expected, CoordinateAxes::XY)
        );
    }
}

#[test]
fn value_point_parse_does_not_allocate_a_path() {
    let value = json!({"type": "Point", "coordinates": [1.0, 2.0]});
    let (parsed, allocations) = allocation_count(|| parse_geojson(&value));

    assert_eq!(parsed.unwrap(), Shape::Point(point(1.0, 2.0)));
    assert_eq!(allocations, 0);
}

#[test]
fn nested_empty_members_are_preserved_by_both_geojson_readers() {
    let cases = [
        (
            "MultiPoint",
            serde_json::json!({"type": "MultiPoint", "coordinates": [[], [1.0, 2.0]]}),
            "MULTIPOINT (EMPTY, (1 2))",
        ),
        (
            "MultiLineString",
            serde_json::json!({
                "type": "MultiLineString",
                "coordinates": [[], [[0.0, 0.0], [1.0, 1.0]]]
            }),
            "MULTILINESTRING (EMPTY, (0 0, 1 1))",
        ),
        (
            "MultiPolygon",
            serde_json::json!({
                "type": "MultiPolygon",
                "coordinates": [[], [[[0.0, 0.0], [1.0, 0.0], [0.0, 1.0], [0.0, 0.0]]]]
            }),
            "MULTIPOLYGON (EMPTY, ((0 0, 1 0, 0 1, 0 0)))",
        ),
    ];
    for (kind, value, expected_wkt) in cases {
        let direct = parse_geojson(&value).expect("value-tree GeoJSON parses");
        assert_eq!(direct.part_count(), 2, "kind={kind}");
        assert_eq!(
            crate::io::to_wkt(&direct).unwrap(),
            expected_wkt,
            "kind={kind}"
        );

        assert_eq!(direct, parse_geojson(&value).unwrap(), "kind={kind}");
    }
}

#[test]
fn xyz_geojson_extreme_values_parse_and_round_trip() {
    let shape = Shape::Point(
        Point::new_axes(
            180.0,
            -90.0,
            crate::geometry::ZOrdinate(Some(-f64::from_bits(1))),
            crate::geometry::MOrdinate(None),
        )
        .unwrap(),
    );
    let text = to_geojson_string(&shape, crate::geometry::TopologyDomain::Cartesian)
        .expect("extreme XYZ GeoJSON writes");
    let value: Value = serde_json::from_str(&text).expect("extreme GeoJSON parses as JSON");
    assert_eq!(parse_geojson(&value).unwrap(), shape);
}

#[test]
fn read_rejects_out_of_domain_positions() {
    parse_geojson(&json!({"type": "Point", "coordinates": [0.0, 91.0]})).unwrap_err();
    parse_geojson(&json!({"type": "Point", "coordinates": [181.0, 0.0]})).unwrap_err();
    // A valid antimeridian-adjacent position still reads.
    parse_geojson(&json!({"type": "Point", "coordinates": [180.0, 90.0]})).unwrap();
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn geographic_line_is_cut_at_the_antimeridian_and_round_trips() {
    let crossing = line(&[(170.0, 0.0), (-170.0, 0.0)]);
    let cut = written(&crossing, geographic_domain());
    assert_eq!(
        cut["type"], "MultiLineString",
        "an antimeridian-crossing line cuts into a MultiLineString (RFC 7946 §3.1.9)"
    );
    // Re-serializing the already-cut geometry is a no-op: the parsed values
    // match (writer-parity is value-based, not string-based).
    let reparsed = parse_geojson(&cut).expect("cut output reads back");
    assert!(matches!(reparsed, Shape::MultiLineString(_)));
    let again = written(&reparsed, geographic_domain());
    assert_eq!(cut, again);
}

#[test]
fn explicit_cartesian_line_is_never_cut() {
    // An explicitly selected Cartesian domain stays one line. Public
    // CRS-free writers select the geographic domain instead.
    let out = written(
        &line(&[(170.0, 0.0), (-170.0, 0.0)]),
        crate::geometry::TopologyDomain::Cartesian,
    );
    assert_eq!(out["type"], "LineString");
}

#[test]
fn reader_rejects_unclosed_polygon_rings() {
    // Structural integrity: an unclosed ring is rejected, never silently
    // closed (the deliberately strict GeoJSON reader policy).
    // Four vertices (clears the minimum-length floor) but first != last,
    // so rejection is specifically the closure rule, not the count rule.
    let unclosed = json!({
        "type": "Polygon",
        "coordinates": [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0]]],
    });
    parse_geojson(&unclosed).unwrap_err();
}
