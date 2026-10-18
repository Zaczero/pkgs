#[cfg(test)]
use crate::io::geojson;
#[cfg(test)]
use crate::io::{CoordSeq, LineSeq, Ring};
use crate::io::{
    CoordinateAxes, Coordinates, GeometryKind, Point, Polygon, Result, Shape, extended_srid_code,
    require_serializable_axes,
};

mod read;
mod write;

pub(crate) use read::{classify_wkt_type_token, parse_wkt};
pub(crate) use write::{WktNumberFormat, to_wkt, to_wkt_display, to_wkt_preview, to_wkt_with_srid};

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;
    use crate::geometry::{MOrdinate, ZOrdinate};
    use crate::io::parse_geojson;

    fn wkt_number(value: f64) -> String {
        let mut out = String::new();
        WktNumberFormat::Shortest.write(&mut out, value);
        out
    }

    #[rstest::rstest]
    #[case::token_0("0")]
    #[case::token_0_0("-0.0")]
    #[case::token_1_2345678901234567("1.2345678901234567")]
    #[case::token_987654321_123456789("-987654321.123456789")]
    #[case::token_1e308("1e308")]
    #[case::token_1e_308("1e-308")]
    #[case::token_5_5e_17("5.5e-17")]
    #[case::token_2_5e_12("-2.5E+12")]
    #[case::token_3_141592653589793("3.141592653589793")]
    #[case::token_1_7976931348623157e308("1.7976931348623157e308")]
    #[case::token_2_2250738585072014e_308("2.2250738585072014e-308")]
    fn wkt_float_parse_matches_std_bits(#[case] token: &'static str) {
        let std_value = token.parse::<f64>().expect("std baseline");
        let wkt = format!("POINT ({token} {token})");
        let point = parse_wkt(&wkt).expect("wkt parses");
        let Shape::Point(parsed) = point else {
            panic!("expected point");
        };
        assert_eq!(parsed.x.to_bits(), std_value.to_bits(), "token={token}");
        assert_eq!(parsed.y.to_bits(), std_value.to_bits(), "token={token}");
    }

    #[rstest::rstest]
    #[case::point("POINT", GeometryKind::Point)]
    #[case::multipoint("MULTIPOINT", GeometryKind::MultiPoint)]
    #[case::linestring("LINESTRING", GeometryKind::LineString)]
    #[case::multilinestring("MULTILINESTRING", GeometryKind::MultiLineString)]
    #[case::polygon("POLYGON", GeometryKind::Polygon)]
    #[case::multipolygon("MULTIPOLYGON", GeometryKind::MultiPolygon)]
    #[case::geometrycollection("GEOMETRYCOLLECTION", GeometryKind::GeometryCollection)]
    fn all_typed_empty_kinds_round_trip_through_wkt_and_wkb(
        #[case] keyword: &'static str,
        #[case] kind: GeometryKind,
    ) {
        let axes = [
            ("", CoordinateAxes::XY),
            (" Z", CoordinateAxes::XYZ),
            (" M", CoordinateAxes::XYM),
            (" ZM", CoordinateAxes::XYZM),
        ];

        for (tag, expected_axes) in axes {
            let input = format!("{keyword}{tag} EMPTY");
            let parsed = parse_wkt(&input).expect("typed empty WKT parses");
            assert_eq!(
                parsed,
                Shape::typed_empty(kind, expected_axes),
                "input={input}"
            );
            let wkt = to_wkt(&parsed).expect("typed empty WKT writes");
            assert_eq!(parse_wkt(&wkt).unwrap(), parsed, "wkt={wkt}");
            let wkb = crate::io::to_wkb(&parsed, None, false).expect("typed empty WKB writes");
            assert_eq!(
                crate::io::parse_wkb(&wkb).unwrap().shape,
                parsed,
                "input={input}"
            );
        }
    }

    #[test]
    fn explicit_empty_multipoint_member_survives_wkt_and_wkb() {
        let parsed = parse_wkt("MULTIPOINT (EMPTY, (1 2))").expect("mixed member WKT parses");
        assert_eq!(parsed.part_count(), 2);
        assert_eq!(to_wkt(&parsed).unwrap(), "MULTIPOINT (EMPTY, (1 2))");

        let wkb = crate::io::to_wkb(&parsed, None, false).expect("mixed member WKB writes");
        let reparsed = crate::io::parse_wkb(&wkb)
            .expect("mixed member WKB parses")
            .shape;
        assert_eq!(reparsed.part_count(), 2);
        assert_eq!(reparsed, parsed);
    }

    #[rstest::rstest]
    #[case::multilinestring_empty_0_0_1_1("MULTILINESTRING (EMPTY, (0 0, 1 1))")]
    #[case::multipolygon_empty_0_0_1_0_0_1_0_0("MULTIPOLYGON (EMPTY, ((0 0, 1 0, 0 1, 0 0)))")]
    fn explicit_empty_line_and_polygon_members_survive_wkb(#[case] input: &'static str) {
        let shape = parse_wkt(input).expect("explicit empty multipart WKT parses");
        assert_eq!(shape.part_count(), 2, "input={input}");
        let bytes = crate::io::to_wkb(&shape, None, false).expect("WKB writes");
        let reparsed = crate::io::parse_wkb(&bytes).expect("WKB parses").shape;
        assert_eq!(reparsed, shape, "input={input}");
    }

    #[test]
    fn wkt_preview_validates_only_rings_it_reaches() {
        let xyz = |x: f64, y: f64| {
            Point::new_axes(x, y, ZOrdinate(Some(5.0)), MOrdinate(None)).expect("finite XYZ point")
        };
        let xy = |x: f64, y: f64| Point::new(x, y).expect("finite XY point");
        let shell = Ring::from_closed(
            CoordSeq::try_from_points(&[
                xyz(0.0, 0.0),
                xyz(2.0, 0.0),
                xyz(0.0, 2.0),
                xyz(0.0, 0.0),
            ])
            .unwrap(),
        )
        .unwrap();
        let late_xy_hole = Ring::from_closed(
            CoordSeq::try_from_points(&[xy(0.5, 0.5), xy(1.0, 0.5), xy(0.5, 1.0), xy(0.5, 0.5)])
                .unwrap(),
        )
        .unwrap();
        let shape = Shape::Polygon(Polygon::new(shell, vec![late_xy_hole]));

        let preview = to_wkt_preview(&shape, 1).expect("unvisited late ring is irrelevant");
        assert!(preview.starts_with("POLYGON Z"));

        let error = to_wkt_preview(&shape, 1024)
            .expect_err("full preview must visit and reject the late ring");
        assert!(matches!(error.kind(), crate::error::ErrorKind::Geometry(_)));
        let error = to_wkt(&shape).expect_err("full WKT admission rejects the late ring");
        assert!(matches!(error.kind(), crate::error::ErrorKind::Geometry(_)));
    }

    #[rstest::rstest]
    #[case::value_1_0(1.0, "1")]
    #[case::negative_value_2_0(-2.0, "-2")]
    #[case::value_0_0(0.0, "0")]
    #[case::negative_value_0_0(-0.0, "-0")]
    #[case::negative_value_122_4194(-122.4194, "-122.4194")]
    #[case::value_37_7749(37.7749, "37.7749")]
    #[case::value_1e_300(
        1e-300,
        "0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001"
    )]
    #[case::value_1e300(
        1e300,
        "1000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
    )]
    #[case::f64_from_bits_1(
        f64::from_bits(1),
        "0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005"
    )]
    #[case::f64_from_bits_0x3ff0_0000_0000_0001(
        f64::from_bits(0x3FF0_0000_0000_0001),
        "1.0000000000000002"
    )]
    #[case::f64_from_bits_0x3fd5_5555_5555_5555(
        f64::from_bits(0x3FD5_5555_5555_5555),
        "0.3333333333333333"
    )]
    #[case::f64_from_bits_0x4345_1eb8_51eb_851f(
        f64::from_bits(0x4345_1EB8_51EB_851F),
        "11889503016258110"
    )]
    #[case::f64_from_bits_0xc083_4567_89ab_cdef(
        f64::from_bits(0xC083_4567_89AB_CDEF),
        "-616.6755555555554"
    )]
    fn shortest_wkt_numbers_preserve_policy_and_round_trip(
        #[case] value: f64,
        #[case] expected: &'static str,
    ) {
        let rendered = wkt_number(value);
        assert_eq!(rendered, expected, "value={value:?}");
        let parsed = rendered.parse::<f64>().unwrap();
        assert_eq!(parsed.to_bits(), value.to_bits(), "rendered={rendered}");
    }

    #[test]
    fn shortest_wkt_xyzm_negative_min_subnormal_round_trips() {
        const MIN_SUBNORMAL: &str = "-0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005";
        let input =
            format!("POINT ZM ({MIN_SUBNORMAL} {MIN_SUBNORMAL} {MIN_SUBNORMAL} {MIN_SUBNORMAL})");
        let parsed = parse_wkt(&input).expect("negative minimum subnormal WKT parses");
        let rendered = to_wkt(&parsed).expect("negative minimum subnormal WKT writes");
        assert_eq!(rendered, input);
        assert_eq!(parse_wkt(&rendered).unwrap(), parsed);
    }

    #[test]
    fn precision_255_wkt_preserves_extreme_fixed_and_trimmed_forms() {
        const MAX_INTEGER: &str = "-179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368";
        let shape = Shape::Point(
            Point::new_axes(
                -f64::MAX,
                -f64::MAX,
                ZOrdinate(Some(-f64::MAX)),
                MOrdinate(Some(-f64::MAX)),
            )
            .unwrap(),
        );

        let fixed = to_wkt_display(&shape, WktNumberFormat::Fixed(255)).unwrap();
        let fixed_coordinates = fixed
            .strip_prefix("POINT ZM (")
            .unwrap()
            .strip_suffix(')')
            .unwrap();
        for rendered in fixed_coordinates.split(' ') {
            let (integer, fraction) = rendered.split_once('.').unwrap();
            assert_eq!(integer, MAX_INTEGER);
            assert_eq!(fraction.len(), 255);
            assert!(fraction.bytes().all(|byte| byte == b'0'));
        }
        assert_eq!(parse_wkt(&fixed).unwrap(), shape);

        let trimmed = to_wkt_display(&shape, WktNumberFormat::Trimmed(255)).unwrap();
        let trimmed_coordinates = trimmed
            .strip_prefix("POINT ZM (")
            .unwrap()
            .strip_suffix(')')
            .unwrap();
        assert_eq!(trimmed_coordinates.split(' ').count(), 4);
        assert!(
            trimmed_coordinates
                .split(' ')
                .all(|value| value == MAX_INTEGER)
        );
        assert_eq!(parse_wkt(&trimmed).unwrap(), shape);
    }

    #[test]
    fn sparse_multipoint_wkt_members_are_reserved_and_round_trip() {
        let input = format!(
            "MULTIPOINT ({})",
            std::iter::repeat_n("EMPTY", 1024)
                .chain(std::iter::once("(1 2)"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let parsed = parse_wkt(&input).expect("sparse MultiPoint WKT parses");
        let rendered = to_wkt(&parsed).expect("sparse MultiPoint WKT writes");
        assert_eq!(rendered, input);
        assert_eq!(parse_wkt(&rendered).unwrap(), parsed);
    }

    #[test]
    fn direct_geojson_writer_matches_reference_value_tree() {
        let polygon = Polygon::new(
            Ring::closed(vec![
                Point::new(0.0, 0.0).unwrap(),
                Point::new(3.0, 0.0).unwrap(),
                Point::new(3.0, 3.0).unwrap(),
                Point::new(0.0, 0.0).unwrap(),
            ])
            .unwrap(),
            vec![
                Ring::closed(vec![
                    Point::new(1.0, 1.0).unwrap(),
                    Point::new(2.0, 1.0).unwrap(),
                    Point::new(2.0, 2.0).unwrap(),
                    Point::new(1.0, 1.0).unwrap(),
                ])
                .unwrap(),
            ],
        );
        let z0 = Point::new_axes(179.9, -16.2, ZOrdinate(Some(5.0)), MOrdinate(None)).unwrap();
        let z1 = Point::new_axes(-179.8, -16.1, ZOrdinate(Some(6.0)), MOrdinate(None)).unwrap();
        let cases = vec![
            Shape::empty_point(),
            Shape::empty_polygon(),
            Shape::Point(Point::new(1.0, 2.0).unwrap()),
            Shape::Point(z0),
            Shape::MultiPoint(
                vec![Point::new(1.0, 2.0).unwrap(), Point::new(3.0, 4.0).unwrap()].into(),
            ),
            Shape::LineString(
                LineSeq::try_new(CoordSeq::try_from_points(&[z0, z1]).unwrap())
                    .expect("test line has two vertices"),
            ),
            Shape::MultiLineString(
                vec![
                    LineSeq::try_new(
                        CoordSeq::try_from_points(&[
                            Point::new(0.0, 0.0).unwrap(),
                            Point::new(1.0, 1.0).unwrap(),
                        ])
                        .unwrap(),
                    )
                    .expect("test line has two vertices"),
                    LineSeq::try_new(
                        CoordSeq::try_from_points(&[
                            Point::new(2.0, 2.0).unwrap(),
                            Point::new(3.0, 3.0).unwrap(),
                        ])
                        .unwrap(),
                    )
                    .expect("test line has two vertices"),
                ]
                .into(),
            ),
            Shape::Polygon(polygon.clone()),
            Shape::MultiPolygon(vec![polygon.clone()].into()),
            Shape::geometry_collection(vec![
                Shape::Point(Point::new(1.0, 2.0).unwrap()),
                Shape::Polygon(polygon),
            ])
            .expect("test collection is valid"),
        ];

        for shape in cases {
            if shape.has_m() || matches!(shape, Shape::Empty(_, axes) if axes != CoordinateAxes::XY)
            {
                assert!(
                    geojson::to_geojson_string(&shape, crate::geometry::TopologyDomain::Cartesian,)
                        .is_err(),
                    "unrepresentable GeoJSON axes must reject"
                );
                continue;
            }
            let direct =
                geojson::to_geojson_string(&shape, crate::geometry::TopologyDomain::Cartesian)
                    .expect("carried GeoJSON ordinates serialize");
            // Compare CONTENT, not byte order: the direct writer emits `type`
            // first (the convention) while the serde value-tree reference sorts
            // keys, so assert the parsed values are equal. The writer
            // normalizes polygon winding to the RFC 7946 right-hand rule
            // (shells CCW, holes CW), so the reference walks the same
            // oriented view.
            let oriented = shape.orient_polygons(false).unwrap();
            let value = serde_json::from_str::<serde_json::Value>(&direct).unwrap();
            assert_eq!(value, reference_geojson_value(&oriented));
            parse_geojson(&value).unwrap();
        }
    }

    #[test]
    fn geojson_reader_accepts_geometry_value_trees() {
        // Geometry-slot inputs are decoded by one Value-based reader.
        let geometry_inputs = [
            r#"{"coordinates":[1.0,2.0],"type":"Point","bbox":[0,0,1,1]}"#,
            r#"{"coordinates":[[0.0,0.0],[1.0,1.0]],"type":"LineString"}"#,
            r#"{"coordinates":[[[0.0,0.0],[2.0,0.0],[1.0,1.0],[0.0,0.0]]],"type":"Polygon"}"#,
            r#"{"coordinates":[[[[0.0,0.0],[2.0,0.0],[1.0,1.0],[0.0,0.0]]]],"type":"MultiPolygon"}"#,
            r#"{"geometries":[{"coordinates":[1.0,2.0],"type":"Point"}],"type":"GeometryCollection"}"#,
        ];
        for text in geometry_inputs {
            let value = serde_json::from_str(text).unwrap();
            parse_geojson(&value).unwrap();
        }

        // Geometry-slot decoding rejects a top-level Feature; callers extract
        // its geometry member first.
        let feature = r#"{"type":"Feature","properties":{"skip":{"deep":[1,2,3]}},"geometry":{"coordinates":[1.0,2.0],"type":"Point"}}"#;
        let value: Value = serde_json::from_str(feature).unwrap();
        let geometry = value
            .as_object()
            .unwrap()
            .get("geometry")
            .expect("Feature has geometry");
        parse_geojson(geometry).unwrap();
        parse_geojson(&value).unwrap_err();
    }

    fn reference_geojson_value(shape: &Shape) -> Value {
        match shape {
            Shape::Point(point) => json!({
                "type": "Point",
                "coordinates": reference_point(*point),
            }),
            Shape::MultiPoint(points) => json!({
                "type": "MultiPoint",
                "coordinates": reference_points(points),
            }),
            Shape::LineString(points) => json!({
                "type": "LineString",
                "coordinates": reference_points(points),
            }),
            Shape::MultiLineString(lines) => json!({
                "type": "MultiLineString",
                "coordinates": lines.iter().map(reference_points).collect::<Vec<_>>(),
            }),
            Shape::Polygon(polygon) => json!({
                "type": "Polygon",
                "coordinates": reference_polygon(polygon),
            }),
            Shape::MultiPolygon(polygons) => json!({
                "type": "MultiPolygon",
                "coordinates": polygons.iter().map(reference_polygon).collect::<Vec<_>>(),
            }),
            Shape::GeometryCollection(geometries) => json!({
                "type": "GeometryCollection",
                "geometries": geometries.iter().map(reference_geojson_value).collect::<Vec<_>>(),
            }),
            Shape::Empty(GeometryKind::GeometryCollection, _) => {
                json!({"type": "GeometryCollection", "geometries": []})
            },
            Shape::Empty(kind, _) => json!({"type": kind.geometry_type(), "coordinates": []}),
        }
    }

    fn reference_point(point: Point) -> Vec<f64> {
        point
            .z()
            .map_or_else(|| vec![point.x, point.y], |z| vec![point.x, point.y, z])
    }

    fn reference_points<C: Coordinates + ?Sized>(points: &C) -> Vec<Vec<f64>> {
        points.iter_coords().map(reference_point).collect()
    }

    fn reference_polygon(polygon: &Polygon) -> Vec<Vec<Vec<f64>>> {
        let mut rings = Vec::with_capacity(polygon.holes.len().wrapping_add(1));
        rings.push(reference_points(&polygon.shell));
        rings.extend(polygon.holes.iter().map(reference_points));
        rings
    }
}
