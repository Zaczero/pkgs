//! RFC 7946 conformance tests for the GeoJSON writer/reader.
//! RFC 7946 conformance: right-hand ring winding on output, the WGS84
//! lon/lat domain on read and write, and antimeridian cutting on output.

use serde_json::{Value, json};

use super::*;

fn point(x: f64, y: f64) -> Point {
    Point::new(x, y).expect("finite test coordinate")
}

fn ring(coords: &[(f64, f64)]) -> Ring {
    Ring::closed(coords.iter().map(|&(x, y)| point(x, y)).collect()).expect("closed test ring")
}

fn line(coords: &[(f64, f64)]) -> Shape {
    Shape::LineString(
        LineSeq::try_new(CoordSeq::from(
            coords.iter().map(|&(x, y)| point(x, y)).collect::<Vec<_>>(),
        ))
        .expect("test line has two vertices"),
    )
}

fn written(shape: &Shape, geographic: bool) -> Value {
    let text = to_geojson_string::<false>(shape, geographic).expect("serializes");
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
    pts.windows(2)
        .map(|w| w[0].0 * w[1].1 - w[1].0 * w[0].1)
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
    let out = written(&cw_shell, false);
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
    let out = written(&poly, false);
    assert!(ring_signed_area(&out["coordinates"][0]) > 0.0, "shell CCW");
    assert!(
        ring_signed_area(&out["coordinates"][1]) < 0.0,
        "RFC 7946 §3.1.6 requires clockwise interior rings"
    );
}

#[test]
fn multipolygon_and_collection_rings_are_right_handed() {
    let cw = ring(&[(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0), (0.0, 0.0)]);
    let multi = Shape::MultiPolygon(vec![Polygon::new(cw.clone(), vec![])]);
    let out = written(&multi, false);
    assert!(ring_signed_area(&out["coordinates"][0][0]) > 0.0);

    let collection = Shape::GeometryCollection(vec![Shape::Polygon(Polygon::new(cw, vec![]))]);
    let out = written(&collection, false);
    assert!(ring_signed_area(&out["geometries"][0]["coordinates"][0]) > 0.0);
}

#[test]
fn write_rejects_out_of_domain_geographic_positions() {
    // Latitude beyond ±90 and longitude beyond ±180 fail on a geographic
    // frame; the same coordinates serialize fine as untagged planar data.
    to_geojson_string::<false>(&Shape::Point(point(0.0, 91.0)), true).unwrap_err();
    to_geojson_string::<false>(&Shape::Point(point(181.0, 0.0)), true).unwrap_err();
    to_geojson_string::<false>(&Shape::Point(point(0.0, 91.0)), false).unwrap();
}

#[test]
fn read_rejects_out_of_domain_positions() {
    parse_geojson(&json!({"type": "Point", "coordinates": [0.0, 91.0]})).unwrap_err();
    parse_geojson(&json!({"type": "Point", "coordinates": [181.0, 0.0]})).unwrap_err();
    // A valid antimeridian-adjacent position still reads.
    parse_geojson(&json!({"type": "Point", "coordinates": [180.0, 90.0]})).unwrap();
}

#[test]
fn geographic_line_is_cut_at_the_antimeridian_and_round_trips() {
    let crossing = line(&[(170.0, 0.0), (-170.0, 0.0)]);
    let cut = written(&crossing, true);
    assert_eq!(
        cut["type"], "MultiLineString",
        "an antimeridian-crossing line cuts into a MultiLineString (RFC 7946 §3.1.9)"
    );
    // Re-serializing the already-cut geometry is a no-op: the parsed values
    // match (writer-parity is value-based, not string-based).
    let reparsed = parse_geojson(&cut).expect("cut output reads back");
    assert!(matches!(reparsed, Shape::MultiLineString(_)));
    let again = written(&reparsed, true);
    assert_eq!(cut, again);
}

#[test]
fn planar_line_is_never_cut() {
    // The identical coordinates on a CRS-free (planar) frame stay one line.
    let out = written(&line(&[(170.0, 0.0), (-170.0, 0.0)]), false);
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
