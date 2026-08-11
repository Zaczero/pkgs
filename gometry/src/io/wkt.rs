use crate::io::{
    CoordSeq, CoordinateAxes, Coordinates, EmptyKind, IoError, IoGeometryKind, LineSeq,
    MAX_PARSE_DEPTH, ParseFormat, Point, Polygon, Result, Ring, Shape, WktHeader,
    extended_srid_code, parse_content, require_serializable_axes,
};

mod read;
mod write;

pub(crate) use read::parse_wkt;
pub(crate) use write::{
    WktDimension, WktNumberFormat, to_wkt, to_wkt_display, to_wkt_preview, to_wkt_with_dimension,
};

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;
    use crate::geometry::{MOrdinate, ZOrdinate};
    use crate::io::{GeoJsonInput, parse_geojson, parse_geojson_text, to_geojson_string_with_z};

    fn wkt_number(value: f64) -> String {
        let mut out = String::new();
        WktNumberFormat::Shortest.write(&mut out, value);
        out
    }

    #[test]
    fn wkt_float_parse_matches_std_bits() {
        let tokens = [
            "0",
            "-0.0",
            "1.2345678901234567",
            "-987654321.123456789",
            "1e308",
            "1e-308",
            "5.5e-17",
            "-2.5E+12",
            "3.141592653589793",
            "1.7976931348623157e308",
            "2.2250738585072014e-308",
        ];
        for token in tokens {
            let std_value = token.parse::<f64>().expect("std baseline");
            let wkt = format!("POINT ({token} {token})");
            let point = parse_wkt(&wkt).expect("wkt parses");
            let Shape::Point(parsed) = point else {
                panic!("expected point");
            };
            assert_eq!(parsed.x.to_bits(), std_value.to_bits(), "token={token}");
            assert_eq!(parsed.y.to_bits(), std_value.to_bits(), "token={token}");
        }
    }

    #[test]
    fn shortest_wkt_numbers_preserve_policy_and_round_trip() {
        let cases = [
            (1.0, "1"),
            (-2.0, "-2"),
            (0.0, "0"),
            (-0.0, "-0"),
            (-122.4194, "-122.4194"),
            (37.7749, "37.7749"),
            (
                1e-300,
                "0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
            ),
            (
                1e300,
                "1000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
            ),
            (
                f64::from_bits(1),
                "0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005",
            ),
            (f64::from_bits(0x3FF0_0000_0000_0001), "1.0000000000000002"),
            (f64::from_bits(0x3FD5_5555_5555_5555), "0.3333333333333333"),
            (f64::from_bits(0x4345_1EB8_51EB_851F), "11889503016258110"),
            (f64::from_bits(0xC083_4567_89AB_CDEF), "-616.6755555555554"),
        ];

        for (value, expected) in cases {
            let rendered = wkt_number(value);
            assert_eq!(rendered, expected, "value={value:?}");
            let parsed = rendered.parse::<f64>().unwrap();
            assert_eq!(parsed.to_bits(), value.to_bits(), "rendered={rendered}");
        }
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
                LineSeq::try_new(CoordSeq::from(vec![z0, z1])).expect("test line has two vertices"),
            ),
            Shape::MultiLineString(vec![
                LineSeq::try_new(CoordSeq::from(vec![
                    Point::new(0.0, 0.0).unwrap(),
                    Point::new(1.0, 1.0).unwrap(),
                ]))
                .expect("test line has two vertices"),
                LineSeq::try_new(CoordSeq::from(vec![
                    Point::new(2.0, 2.0).unwrap(),
                    Point::new(3.0, 3.0).unwrap(),
                ]))
                .expect("test line has two vertices"),
            ]),
            Shape::Polygon(polygon.clone()),
            Shape::MultiPolygon(vec![polygon.clone()]),
            Shape::GeometryCollection(vec![
                Shape::Point(Point::new(1.0, 2.0).unwrap()),
                Shape::Polygon(polygon),
            ]),
        ];

        for shape in cases {
            for include_z in [false, true] {
                let direct = to_geojson_string_with_z(&shape, include_z);
                // Compare CONTENT, not byte order: the direct writer emits `type`
                // first (the convention) while the serde value-tree reference sorts
                // keys, so assert the parsed values are equal. The writer
                // normalizes polygon winding to the RFC 7946 right-hand rule
                // (shells CCW, holes CW), so the reference walks the same
                // oriented view.
                let oriented = shape.clone().orient_polygons(false);
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(&direct).unwrap(),
                    reference_geojson_value(&oriented, include_z),
                );
                let GeoJsonInput::Geometry(parsed) = parse_geojson_text(&direct).unwrap().input
                else {
                    panic!("writer emitted a FeatureCollection");
                };
                assert_eq!(
                    parsed,
                    parse_geojson(&serde_json::from_str(&direct).unwrap()).unwrap()
                );
            }
        }
    }

    #[test]
    fn direct_geojson_reader_matches_value_tree_reader() {
        // Geometry-slot inputs: text decoder and value-tree decoder agree.
        let geometry_inputs = [
            r#"{"coordinates":[1.0,2.0],"type":"Point","bbox":[0,0,1,1]}"#,
            r#"{"coordinates":[[0.0,0.0],[1.0,1.0]],"type":"LineString"}"#,
            r#"{"coordinates":[[[0.0,0.0],[2.0,0.0],[1.0,1.0],[0.0,0.0]]],"type":"Polygon"}"#,
            r#"{"coordinates":[[[[0.0,0.0],[2.0,0.0],[1.0,1.0],[0.0,0.0]]]],"type":"MultiPolygon"}"#,
            r#"{"geometries":[{"coordinates":[1.0,2.0],"type":"Point"}],"type":"GeometryCollection"}"#,
        ];
        for text in geometry_inputs {
            let GeoJsonInput::Geometry(parsed) = parse_geojson_text(text).unwrap().input else {
                panic!("expected scalar geometry");
            };
            let value = serde_json::from_str(text).unwrap();
            assert_eq!(parsed, parse_geojson(&value).unwrap());
        }

        // Top-level Feature: text decoder unwraps once; value-tree geometry-slot
        // decoder rejects Feature — callers extract `.geometry` first.
        let feature = r#"{"type":"Feature","properties":{"skip":{"deep":[1,2,3]}},"geometry":{"coordinates":[1.0,2.0],"type":"Point"}}"#;
        let GeoJsonInput::Geometry(parsed) = parse_geojson_text(feature).unwrap().input else {
            panic!("expected scalar geometry from Feature");
        };
        let value: Value = serde_json::from_str(feature).unwrap();
        let geometry = value
            .as_object()
            .unwrap()
            .get("geometry")
            .expect("Feature has geometry");
        assert_eq!(parsed, parse_geojson(geometry).unwrap());
        parse_geojson(&value).unwrap_err();

        let collection = r#"{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"name":"a"},"geometry":{"coordinates":[1.0,2.0],"type":"Point"}},{"type":"Feature","properties":{},"geometry":{"coordinates":[[0.0,0.0],[1.0,1.0]],"type":"LineString"}}]}"#;
        let GeoJsonInput::FeatureCollection(parsed) = parse_geojson_text(collection).unwrap().input
        else {
            panic!("expected feature collection");
        };
        let value: Value = serde_json::from_str(collection).unwrap();
        let reference: Vec<Option<Shape>> = value
            .as_object()
            .unwrap()
            .get("features")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|feature| {
                let geometry = feature
                    .as_object()
                    .unwrap()
                    .get("geometry")
                    .expect("Feature has geometry");
                parse_geojson(geometry).map(Some)
            })
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(parsed, reference);
    }

    fn reference_geojson_value(shape: &Shape, include_z: bool) -> Value {
        match shape {
            Shape::Point(point) => json!({
                "type": "Point",
                "coordinates": reference_point(*point, include_z),
            }),
            Shape::MultiPoint(points) => json!({
                "type": "MultiPoint",
                "coordinates": reference_points(points, include_z),
            }),
            Shape::LineString(points) => json!({
                "type": "LineString",
                "coordinates": reference_points(points, include_z),
            }),
            Shape::MultiLineString(lines) => json!({
                "type": "MultiLineString",
                "coordinates": lines.iter().map(|line| reference_points(line, include_z)).collect::<Vec<_>>(),
            }),
            Shape::Polygon(polygon) => json!({
                "type": "Polygon",
                "coordinates": reference_polygon(polygon, include_z),
            }),
            Shape::MultiPolygon(polygons) => json!({
                "type": "MultiPolygon",
                "coordinates": polygons.iter().map(|polygon| reference_polygon(polygon, include_z)).collect::<Vec<_>>(),
            }),
            Shape::GeometryCollection(geometries) => json!({
                "type": "GeometryCollection",
                "geometries": geometries.iter().map(|geometry| reference_geojson_value(geometry, include_z)).collect::<Vec<_>>(),
            }),
            Shape::Empty(EmptyKind::GeometryCollection, _) => {
                json!({"type": "GeometryCollection", "geometries": []})
            },
            Shape::Empty(kind, _) => json!({"type": kind.geometry_type(), "coordinates": []}),
        }
    }

    fn reference_point(point: Point, include_z: bool) -> Vec<f64> {
        if include_z {
            point
                .z()
                .map_or_else(|| vec![point.x, point.y], |z| vec![point.x, point.y, z])
        } else {
            vec![point.x, point.y]
        }
    }

    fn reference_points<C: Coordinates + ?Sized>(points: &C, include_z: bool) -> Vec<Vec<f64>> {
        points
            .iter_coords()
            .map(|point| reference_point(point, include_z))
            .collect()
    }

    fn reference_polygon(polygon: &Polygon, include_z: bool) -> Vec<Vec<Vec<f64>>> {
        let mut rings = Vec::with_capacity(polygon.holes.len() + 1);
        rings.push(reference_points(&polygon.shell, include_z));
        rings.extend(
            polygon
                .holes
                .iter()
                .map(|hole| reference_points(hole, include_z)),
        );
        rings
    }
}
