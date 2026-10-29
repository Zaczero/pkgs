//! Conversion between MVT integer paths and gometry shapes.

use super::wire;
use crate::geometry::{
    CoordSeq, LineSeq, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon, Ring, Shape,
    SharedSlice, ring_winding,
};

fn position(point: Point) -> Result<[i64; 2], String> {
    fn coordinate(value: f64) -> Result<i64, String> {
        if !value.is_finite()
            || value.fract() != 0.0
            || value < i64::MIN as f64
            || value >= -(i64::MIN as f64)
        {
            return Err("local coordinates must be exactly represented integers".into());
        }
        Ok(value as i64)
    }
    Ok([coordinate(point.x)?, coordinate(point.y)?])
}

fn line(points: impl IntoIterator<Item = Point>) -> Result<Vec<[i64; 2]>, String> {
    points.into_iter().map(position).collect()
}

fn ring(ring: &Ring, outer: bool) -> Result<Vec<[i64; 2]>, String> {
    let winding = ring_winding(ring);
    if winding.is_degenerate() {
        return Err("polygon ring has zero area".into());
    }
    let mut points = line(ring.iter())?;
    points.pop().ok_or("polygon ring has no vertices")?;
    if winding.is_ccw() != outer {
        points.reverse();
    }
    Ok(points)
}

fn polygon(polygon: &Polygon, rings: &mut Vec<Vec<[i64; 2]>>) -> Result<(), String> {
    rings.push(ring(&polygon.shell, true)?);
    for hole in polygon.holes.iter() {
        rings.push(ring(hole, false)?);
    }
    Ok(())
}

pub(super) fn encode_shape(shape: &Shape) -> Result<Option<wire::Geometry>, String> {
    if shape.has_z() || shape.has_m() {
        return Err("local MVT geometry must have XY coordinates".into());
    }
    if let Some(issue) = shape.validate().map_err(|error| error.to_string())? {
        return Err(format!("invalid local MVT geometry: {}", issue.detail));
    }
    let geometry = match shape {
        Shape::Point(point) => wire::Geometry::Points(vec![position(*point)?]),
        Shape::MultiPoint(points) => wire::Geometry::Points(line(points.points())?),
        Shape::LineString(points) => wire::Geometry::Lines(vec![line(points.iter())?]),
        Shape::MultiLineString(lines) => wire::Geometry::Lines(
            lines
                .iter()
                .map(|points| line(points.iter()))
                .collect::<Result<_, _>>()?,
        ),
        Shape::Polygon(value) => {
            let mut rings = Vec::new();
            polygon(value, &mut rings)?;
            wire::Geometry::Rings(rings)
        },
        Shape::MultiPolygon(values) => {
            let mut rings = Vec::new();
            for value in values.iter() {
                polygon(value, &mut rings)?;
            }
            wire::Geometry::Rings(rings)
        },
        Shape::GeometryCollection(_) => {
            return Err("MVT does not encode GeometryCollection".into());
        },
        Shape::Empty(..) => return Ok(None),
    };
    Ok(Some(geometry))
}

fn coordinates(points: &[[i64; 2]], closed: bool) -> Result<CoordSeq, String> {
    let mut positions = points
        .iter()
        .map(|&[x, y]| Point::new_raw_xy(x as f64, y as f64))
        .collect::<Vec<_>>();
    if closed {
        positions.push(*positions.first().ok_or("empty polygon ring")?);
    }
    CoordSeq::try_from_points(&positions).map_err(|error| error.to_string())
}

fn ring_from_positions(points: &[[i64; 2]]) -> Result<Ring, String> {
    Ring::try_from_closed(coordinates(points, true)?).map_err(|error| error.to_string())
}

pub(super) fn decode_shape(geometry: &wire::Geometry) -> Result<Shape, String> {
    let shape = match geometry {
        wire::Geometry::Points(points) => {
            let coords = coordinates(points, false)?;
            if points.len() == 1 {
                Shape::Point(coords.iter().next().ok_or("missing point")?)
            } else {
                Shape::MultiPoint(
                    MultiPoint::from_coordseq(coords).map_err(|error| error.to_string())?,
                )
            }
        },
        wire::Geometry::Lines(parts) => {
            let lines = parts
                .iter()
                .map(|part| {
                    LineSeq::try_new(coordinates(part, false)?).map_err(|error| error.to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            if lines.len() == 1 {
                Shape::LineString(lines.into_iter().next().ok_or("missing line")?)
            } else {
                Shape::MultiLineString(
                    MultiLineString::from_dense(lines).ok_or("missing line parts")?,
                )
            }
        },
        wire::Geometry::Rings(rings) => {
            let mut polygons = Vec::new();
            let mut shell = None;
            let mut holes = Vec::new();
            for positions in rings {
                let value = ring_from_positions(positions)?;
                let winding = ring_winding(&value);
                if winding.is_degenerate() {
                    return Err("polygon ring has zero area".into());
                }
                if winding.is_ccw() {
                    if let Some(shell) = shell.replace(value) {
                        polygons.push(Polygon {
                            shell,
                            holes: SharedSlice::from_vec(std::mem::take(&mut holes)),
                        });
                    }
                } else {
                    if shell.is_none() {
                        return Err("polygon hole precedes its shell".into());
                    }
                    holes.push(value);
                }
            }
            polygons.push(Polygon {
                shell: shell.ok_or("polygon has no shell")?,
                holes: SharedSlice::from_vec(holes),
            });
            if polygons.len() == 1 {
                Shape::Polygon(polygons.into_iter().next().ok_or("missing polygon")?)
            } else {
                Shape::MultiPolygon(
                    MultiPolygon::from_dense(polygons).ok_or("missing polygon parts")?,
                )
            }
        },
    };
    if let Some(issue) = shape.validate().map_err(|error| error.to_string())? {
        return Err(format!("invalid MVT geometry: {}", issue.detail));
    }
    Ok(shape)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_geometry_roundtrips_points_lines_and_holes() {
        for (source, expected_area) in [
            ("POINT (25 17)", 0.0),
            ("MULTIPOINT ((1 2),(3 4))", 0.0),
            ("LINESTRING (2 2,2 10,10 10)", 0.0),
            (
                "POLYGON ((0 0,10 0,10 10,0 10,0 0),(2 2,2 4,4 4,4 2,2 2))",
                96.0,
            ),
            (
                "MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),((8 0,10 0,10 2,8 2,8 0)))",
                20.0,
            ),
        ] {
            let shape = crate::io::parse_wkt(source).unwrap();
            let encoded = encode_shape(&shape).unwrap().unwrap();
            let (kind, words) = wire::encode_geometry(&encoded).unwrap();
            let parsed = wire::decode_geometry(kind, &words).unwrap();
            let restored = decode_shape(&parsed).unwrap();
            assert!(restored.validate().unwrap().is_none());
            assert_eq!(restored.area(), expected_area);
            assert_eq!(encode_shape(&restored).unwrap(), Some(encoded));
        }
    }

    #[test]
    fn local_geometry_rejects_fractional_coordinates_and_bad_ring_grouping() {
        let fractional = crate::io::parse_wkt("POINT (1.5 2)").unwrap();
        encode_shape(&fractional).unwrap_err();
        let first_hole = wire::Geometry::Rings(vec![vec![[0, 0], [0, 4], [4, 4], [4, 0]]]);
        decode_shape(&first_hole).unwrap_err();
        let crossing = wire::Geometry::Rings(vec![vec![[0, 0], [4, 4], [0, 4], [4, 0]]]);
        decode_shape(&crossing).unwrap_err();
        let misplaced_hole = wire::Geometry::Rings(vec![
            vec![[0, 0], [4, 0], [4, 4], [0, 4]],
            vec![[8, 0], [8, 2], [10, 2], [10, 0]],
        ]);
        decode_shape(&misplaced_hole).unwrap_err();
    }
}
