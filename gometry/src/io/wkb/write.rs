use crate::geometry::{CoordinateAxes, MultiLineString, MultiMember, MultiPoint, MultiPolygon};
use crate::io::wkb::{
    Coordinates, EWKB_SRID_FLAG, GeometryKind, IoError, LineSeq, Point, Polygon, Result, Shape,
    WKB_COUNT, WKB_HEADER_BASE, extended_srid_code, wkb_coords_len, wkb_dimension, wkb_type,
};

pub(crate) fn to_wkb(shape: &Shape, crs: Option<&str>, include_srid: bool) -> Result<Vec<u8>> {
    // Resolve the SRID once and thread it through length + header so the
    // wire-alias table (CRS84→4326) cannot diverge between the two.
    let srid = extended_srid_code(crs, include_srid, "EWKB")?;
    let preflight = preflight_wkb(shape, srid.is_some())?;
    let mut out = Vec::new();
    crate::boundary::try_reserve_vec(&mut out, preflight.length)?;
    preflight.write(&mut out, srid);
    Ok(out)
}

/// Write one row using an SRID resolved by the owning batch.
///
/// Resolving an array's uniform CRS in the row loop repeats the same parse for
/// every row. The batch path resolves once and still validates each shape's
/// axes here, preserving row-level axis errors and notes.
pub(crate) fn write_wkb_to_srid(out: &mut Vec<u8>, shape: &Shape, srid: Option<u32>) -> Result<()> {
    let preflight = preflight_wkb(shape, srid.is_some())?;
    crate::boundary::try_reserve_vec(out, preflight.length)?;
    preflight.write(out, srid);
    Ok(())
}

struct WkbPreflight<'a> {
    geometry: WkbShape<'a>,
    length: usize,
}

impl WkbPreflight<'_> {
    fn write(self, out: &mut Vec<u8>, srid: Option<u32>) {
        self.geometry.write(out, srid);
    }
}

fn preflight_wkb(shape: &Shape, srid: bool) -> Result<WkbPreflight<'_>> {
    crate::io::require_serializable_axes(shape)?;
    let geometry = WkbShape::classify(shape);
    let length = geometry.encoded_len(srid)?;
    Ok(WkbPreflight { geometry, length })
}

fn write_columns(
    out: &mut Vec<u8>,
    xs: &[f64],
    ys: &[f64],
    zs: Option<&[f64]>,
    ms: Option<&[f64]>,
) {
    let count = xs.len();
    debug_assert_eq!(ys.len(), count);
    debug_assert!(zs.is_none_or(|z| z.len() == count));
    debug_assert!(ms.is_none_or(|m| m.len() == count));
    let dimension = 2 + usize::from(zs.is_some()) + usize::from(ms.is_some());
    let byte_len = count
        .checked_mul(dimension)
        .and_then(|bytes| bytes.checked_mul(size_of::<f64>()))
        .expect("WKB coordinate byte length was preflighted");
    let expected_end = out
        .len()
        .checked_add(byte_len)
        .expect("WKB output length was preflighted");
    debug_assert!(out.capacity() - out.len() >= byte_len);
    match (zs, ms) {
        (None, None) => {
            for (&x, &y) in std::iter::zip(xs, ys) {
                out.extend_from_slice(&x.to_le_bytes());
                out.extend_from_slice(&y.to_le_bytes());
            }
        },
        (Some(zs), None) => {
            for ((&x, &y), &z) in std::iter::zip(std::iter::zip(xs, ys), zs) {
                out.extend_from_slice(&x.to_le_bytes());
                out.extend_from_slice(&y.to_le_bytes());
                out.extend_from_slice(&z.to_le_bytes());
            }
        },
        (None, Some(ms)) => {
            for ((&x, &y), &m) in std::iter::zip(std::iter::zip(xs, ys), ms) {
                out.extend_from_slice(&x.to_le_bytes());
                out.extend_from_slice(&y.to_le_bytes());
                out.extend_from_slice(&m.to_le_bytes());
            }
        },
        (Some(zs), Some(ms)) => {
            for (((&x, &y), &z), &m) in
                std::iter::zip(std::iter::zip(std::iter::zip(xs, ys), zs), ms)
            {
                out.extend_from_slice(&x.to_le_bytes());
                out.extend_from_slice(&y.to_le_bytes());
                out.extend_from_slice(&z.to_le_bytes());
                out.extend_from_slice(&m.to_le_bytes());
            }
        },
    }
    debug_assert_eq!(out.len(), expected_end);
}

/// One geometry's complete WKB classification. Size calculation and writing
/// consume this same kind/axes/body value, so nested heterogeneous dimensions
/// cannot drift between the allocation pass and the emitted bytes.
#[derive(Clone, Copy)]
struct WkbShape<'a> {
    kind: GeometryKind,
    axes: CoordinateAxes,
    body: WkbBody<'a>,
}

#[derive(Clone, Copy)]
enum WkbBody<'a> {
    Point(Point),
    MultiPoint(&'a MultiPoint),
    LineString(&'a LineSeq),
    MultiLineString(&'a MultiLineString),
    Polygon(&'a Polygon),
    MultiPolygon(&'a MultiPolygon),
    GeometryCollection(&'a [Shape]),
    EmptyPoint,
    EmptyMultiPoint,
    EmptyLineString,
    EmptyMultiLineString,
    EmptyMultiPolygon,
    EmptyPolygon,
}

impl<'a> WkbShape<'a> {
    fn classify(shape: &'a Shape) -> Self {
        match shape {
            Shape::Point(point) => Self::point(*point),
            Shape::MultiPoint(points) => Self {
                kind: GeometryKind::MultiPoint,
                axes: points.axes(),
                body: WkbBody::MultiPoint(points),
            },
            Shape::LineString(line) => Self::line(line),
            Shape::MultiLineString(lines) => Self {
                kind: GeometryKind::MultiLineString,
                axes: lines.axes(),
                body: WkbBody::MultiLineString(lines),
            },
            Shape::Polygon(polygon) => Self::polygon(polygon),
            Shape::MultiPolygon(polygons) => Self {
                kind: GeometryKind::MultiPolygon,
                axes: polygons.axes(),
                body: WkbBody::MultiPolygon(polygons),
            },
            Shape::GeometryCollection(geometries) => Self {
                kind: GeometryKind::GeometryCollection,
                axes: shape.axes(),
                body: WkbBody::GeometryCollection(geometries),
            },
            // A typed empty writes its declared axes into the header type code
            // (1001-style for `POINT Z EMPTY`) with an axes-matched body: the
            // NaN sentinel per present ordinate for a point, a zero count for
            // the counted kinds.
            Shape::Empty(kind, axes) => {
                let axes = *axes;
                match kind {
                    GeometryKind::Point => Self {
                        kind: GeometryKind::Point,
                        axes,
                        body: WkbBody::EmptyPoint,
                    },
                    GeometryKind::MultiPoint => Self {
                        kind: GeometryKind::MultiPoint,
                        axes,
                        body: WkbBody::EmptyMultiPoint,
                    },
                    GeometryKind::LineString => Self {
                        kind: GeometryKind::LineString,
                        axes,
                        body: WkbBody::EmptyLineString,
                    },
                    GeometryKind::Polygon => Self {
                        kind: GeometryKind::Polygon,
                        axes,
                        body: WkbBody::EmptyPolygon,
                    },
                    GeometryKind::MultiLineString => Self {
                        kind: GeometryKind::MultiLineString,
                        axes,
                        body: WkbBody::EmptyMultiLineString,
                    },
                    GeometryKind::MultiPolygon => Self {
                        kind: GeometryKind::MultiPolygon,
                        axes,
                        body: WkbBody::EmptyMultiPolygon,
                    },
                    GeometryKind::GeometryCollection => Self {
                        kind: GeometryKind::GeometryCollection,
                        axes,
                        body: WkbBody::GeometryCollection(&[]),
                    },
                }
            },
        }
    }

    const fn point(point: Point) -> Self {
        Self {
            kind: GeometryKind::Point,
            axes: CoordinateAxes::from_point(point),
            body: WkbBody::Point(point),
        }
    }

    fn line(line: &'a LineSeq) -> Self {
        Self {
            kind: GeometryKind::LineString,
            axes: line.axes(),
            body: WkbBody::LineString(line),
        }
    }

    fn polygon(polygon: &'a Polygon) -> Self {
        Self {
            kind: GeometryKind::Polygon,
            axes: polygon.axes(),
            body: WkbBody::Polygon(polygon),
        }
    }

    fn encoded_len(self, srid: bool) -> Result<usize> {
        let header = WKB_HEADER_BASE
            .checked_add(if srid { 4 } else { 0 })
            .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize"))?;
        header
            .checked_add(self.body.encoded_len(self.axes)?)
            .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize"))
    }

    fn write(self, out: &mut Vec<u8>, srid: Option<u32>) {
        write_wkb_header(out, self.kind, self.axes, srid);
        self.body.write(out, self.axes);
    }
}

impl WkbBody<'_> {
    fn encoded_len(self, axes: CoordinateAxes) -> Result<usize> {
        match self {
            Self::Point(_) | Self::EmptyPoint => wkb_coords_len(1, axes),
            Self::MultiPoint(points) => wkb_counted_body_len(points.member_count(), || {
                let child = WKB_HEADER_BASE
                    .checked_add(wkb_coords_len(1, axes)?)
                    .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize"))?;
                points
                    .member_count()
                    .checked_mul(child)
                    .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize"))
            }),
            Self::LineString(line) => WKB_COUNT
                .checked_add(wkb_coords_len(line.len(), axes)?)
                .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize")),
            // Admitted homogeneous multipart members share the parent's axes,
            // so each child header and body is sized against that one layout.
            Self::MultiLineString(lines) => wkb_counted_body_len(lines.member_count(), || {
                lines.members().try_fold(0_usize, |total, member| {
                    let child = match member {
                        MultiMember::Empty => WKB_HEADER_BASE.checked_add(WKB_COUNT),
                        MultiMember::Value(index) => {
                            WKB_HEADER_BASE.checked_add(WKB_COUNT).and_then(|prefix| {
                                wkb_coords_len(lines.as_slice()[index].len(), axes)
                                    .ok()
                                    .and_then(|body| prefix.checked_add(body))
                            })
                        },
                    }
                    .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize"))?;
                    total
                        .checked_add(child)
                        .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize"))
                })
            }),
            Self::Polygon(polygon) => wkb_polygon_body_len(polygon, axes),
            Self::MultiPolygon(polygons) => wkb_counted_body_len(polygons.member_count(), || {
                polygons.members().try_fold(0_usize, |total, member| {
                    let child = match member {
                        MultiMember::Empty => WKB_HEADER_BASE.checked_add(WKB_COUNT),
                        MultiMember::Value(index) => WKB_HEADER_BASE
                            .checked_add(wkb_polygon_body_len(&polygons.as_slice()[index], axes)?),
                    }
                    .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize"))?;
                    total
                        .checked_add(child)
                        .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize"))
                })
            }),
            Self::GeometryCollection(geometries) => wkb_counted_body_len(geometries.len(), || {
                geometries.iter().try_fold(0_usize, |total, geometry| {
                    total
                        .checked_add(WkbShape::classify(geometry).encoded_len(false)?)
                        .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize"))
                })
            }),
            Self::EmptyMultiPoint
            | Self::EmptyLineString
            | Self::EmptyMultiLineString
            | Self::EmptyMultiPolygon
            | Self::EmptyPolygon => Ok(WKB_COUNT),
        }
    }

    fn write(self, out: &mut Vec<u8>, axes: CoordinateAxes) {
        match self {
            Self::Point(point) => {
                write_point(out, point, axes);
            },
            // Every homogeneous-multipart member is written with the parent's
            // axes, and explicit empty members retain their count and position.
            Self::MultiPoint(points) => {
                write_u32(out, points.member_count() as u32);
                for member in points.members() {
                    write_wkb_header(out, GeometryKind::Point, axes, None);
                    match member {
                        MultiMember::Empty => {
                            for _ in 0..wkb_dimension(axes) {
                                out.extend_from_slice(&f64::NAN.to_le_bytes());
                            }
                        },
                        MultiMember::Value(index) => {
                            write_point(out, points.point_at(index), axes);
                        },
                    }
                }
            },
            Self::LineString(line) => write_wkb_sequence(out, line, axes),
            Self::MultiLineString(lines) => {
                write_u32(out, lines.member_count() as u32);
                for member in lines.members() {
                    write_wkb_header(out, GeometryKind::LineString, axes, None);
                    match member {
                        MultiMember::Empty => write_u32(out, 0),
                        MultiMember::Value(index) => {
                            write_wkb_sequence(out, &lines.as_slice()[index], axes);
                        },
                    }
                }
            },
            Self::Polygon(polygon) => write_wkb_polygon(out, polygon, axes),
            Self::MultiPolygon(polygons) => {
                write_u32(out, polygons.member_count() as u32);
                for member in polygons.members() {
                    write_wkb_header(out, GeometryKind::Polygon, axes, None);
                    match member {
                        MultiMember::Empty => write_u32(out, 0),
                        MultiMember::Value(index) => {
                            write_wkb_polygon(out, &polygons.as_slice()[index], axes);
                        },
                    }
                }
            },
            Self::GeometryCollection(geometries) => {
                write_u32(out, geometries.len() as u32);
                for geometry in geometries {
                    WkbShape::classify(geometry).write(out, None);
                }
            },
            Self::EmptyPoint => {
                // The OGC empty-point sentinel: NaN for EVERY ordinate the
                // header's axes declare (2 for XY, 3 for Z/M, 4 for ZM) —
                // symmetric with the reader's `wkb_point_is_empty`.
                for _ in 0..wkb_dimension(axes) {
                    out.extend_from_slice(&f64::NAN.to_le_bytes());
                }
            },
            Self::EmptyMultiPoint
            | Self::EmptyLineString
            | Self::EmptyMultiLineString
            | Self::EmptyMultiPolygon
            | Self::EmptyPolygon => {
                write_u32(out, 0);
            },
        }
    }
}

/// Exact serialized WKB/EWKB byte length from the same classification used by
/// the writer. `srid` means the top-level header carries an EWKB SRID.
pub(crate) fn wkb_len(shape: &Shape, srid: bool) -> Result<usize> {
    Ok(preflight_wkb(shape, srid)?.length)
}

fn wkb_counted_body_len<F>(count: usize, body: F) -> Result<usize>
where
    F: FnOnce() -> Result<usize>,
{
    checked_len(count)?;
    WKB_COUNT
        .checked_add(body()?)
        .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize"))
}

fn wkb_polygon_body_len(polygon: &Polygon, axes: CoordinateAxes) -> Result<usize> {
    let ring_count = polygon.holes.len() + 1;
    wkb_counted_body_len(ring_count, || {
        std::iter::once(&polygon.shell)
            .chain(polygon.holes.iter())
            .try_fold(0_usize, |total, ring| {
                let ring_len = WKB_COUNT
                    .checked_add(wkb_coords_len(ring.coord_count(), axes)?)
                    .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize"))?;
                total
                    .checked_add(ring_len)
                    .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize"))
            })
    })
}

fn write_wkb_header(
    out: &mut Vec<u8>,
    kind: GeometryKind,
    axes: CoordinateAxes,
    srid: Option<u32>,
) {
    out.push(1);
    // `srid` is the already-resolved integer from `extended_srid_code` —
    // never re-parse the CRS string here (wire aliases live only in that
    // single resolver).
    let mut geometry_type = wkb_type(kind, axes, srid.is_some());
    if srid.is_some() {
        geometry_type |= EWKB_SRID_FLAG;
    }
    write_u32(out, geometry_type);
    if let Some(srid) = srid {
        write_u32(out, srid);
    }
}

fn write_wkb_polygon(out: &mut Vec<u8>, polygon: &Polygon, axes: CoordinateAxes) {
    write_u32(out, (polygon.holes.len() + 1) as u32);
    write_wkb_sequence(out, &polygon.shell, axes);
    for hole in polygon.holes.iter() {
        write_wkb_sequence(out, hole, axes);
    }
}

/// Write one counted coordinate run after the WKB sizing preflight.
fn write_wkb_sequence<C: Coordinates + ?Sized>(
    out: &mut Vec<u8>,
    points: &C,
    axes: CoordinateAxes,
) {
    write_u32(out, points.coord_count() as u32);
    let (xs, ys) = points
        .xy_columns()
        .expect("WKB coordinate sequences expose columns");
    let zs = points.z_column();
    let ms = points.m_column();
    debug_assert_eq!(axes.has_z(), zs.is_some());
    debug_assert_eq!(axes.has_m(), ms.is_some());
    write_columns(out, xs, ys, zs, ms);
}

/// Write one vertex under axes already proven to match the point.
fn write_point(out: &mut Vec<u8>, point: Point, axes: CoordinateAxes) {
    out.extend_from_slice(&point.x.to_le_bytes());
    out.extend_from_slice(&point.y.to_le_bytes());
    if axes.has_z() {
        out.extend_from_slice(&point.z().expect("WKB Z axis was preflighted").to_le_bytes());
    }
    if axes.has_m() {
        out.extend_from_slice(&point.m().expect("WKB M axis was preflighted").to_le_bytes());
    }
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn checked_len(value: usize) -> Result<u32> {
    value
        .try_into()
        .map_err(|_| IoError::wkb("geometry is too large for WKB"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::{MOrdinate, ZOrdinate};
    use crate::io::parse_wkb;
    use crate::{CoordSeq, Ring};

    fn xy(x: f64, y: f64) -> Point {
        Point::new_raw_xy(x, y)
    }

    fn xyz(x: f64, y: f64, z: f64) -> Point {
        Point::new_raw_axes(x, y, ZOrdinate(Some(z)), MOrdinate(None))
    }

    fn xym(x: f64, y: f64, m: f64) -> Point {
        Point::new_raw_axes(x, y, ZOrdinate(None), MOrdinate(Some(m)))
    }

    fn line(points: &[Point]) -> LineSeq {
        LineSeq::try_new(CoordSeq::try_from_points(points).unwrap()).expect("test line is valid")
    }

    fn polygon(points: &[Point]) -> Polygon {
        Polygon::new(
            Ring::from_closed(CoordSeq::try_from_points(points).unwrap()).unwrap(),
            vec![],
        )
        .expect("test polygon is valid")
    }

    fn ring(axes: CoordinateAxes) -> Ring {
        let point = |x: f64, y: f64| {
            Point::new_axes(
                x,
                y,
                ZOrdinate(axes.has_z().then_some(1.0)),
                MOrdinate(axes.has_m().then_some(2.0)),
            )
            .expect("finite test point")
        };
        Ring::from_closed(
            CoordSeq::try_from_points(&[
                point(0.0, 0.0),
                point(2.0, 0.0),
                point(0.0, 2.0),
                point(0.0, 0.0),
            ])
            .unwrap(),
        )
        .unwrap()
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn counted_body_length_rejects_wire_count_before_body() {
        let mut called = false;
        let error = wkb_counted_body_len((u32::MAX as usize) + 1, || {
            called = true;
            Ok(0)
        })
        .expect_err("wire count must be checked before the body");
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Io(crate::io::IoError { .. })
        ));
        assert!(!called, "oversized count must not traverse its body");
    }

    #[test]
    fn mixed_ring_sizing_rejects_before_append_mutation() {
        let polygon =
            Polygon::new(ring(CoordinateAxes::XYZ), vec![ring(CoordinateAxes::XY)]).unwrap();
        let shape = Shape::MultiPolygon(
            MultiPolygon::try_new(CoordinateAxes::XYZ, vec![MultiMember::Value(polygon)])
                .expect("polygon aggregate axes match"),
        );
        let mut out = vec![7_u8, 8];
        let error = write_wkb_to_srid(&mut out, &shape, None)
            .expect_err("mixed rings must be rejected before writing");
        assert!(matches!(error.kind(), crate::error::ErrorKind::Geometry(_)));
        assert_eq!(
            out,
            [7, 8],
            "failed preflight leaves appended bytes untouched"
        );
    }

    #[test]
    fn mixed_axes_multiline_is_unrepresentable() {
        let result = MultiLineString::try_new(crate::geometry::CoordinateAxes::XY, vec![
            MultiMember::Value(line(&[xy(0.0, 0.0), xy(1.0, 1.0)])),
            MultiMember::Value(line(&[xyz(2.0, 2.0, 3.0), xyz(3.0, 3.0, 4.0)])),
        ]);
        result.unwrap_err();
    }

    #[test]
    fn mixed_axes_multipolygon_is_unrepresentable() {
        let result = MultiPolygon::try_new(crate::geometry::CoordinateAxes::XY, vec![
            MultiMember::Value(polygon(&[
                xy(0.0, 0.0),
                xy(1.0, 0.0),
                xy(1.0, 1.0),
                xy(0.0, 0.0),
            ])),
            MultiMember::Value(polygon(&[
                xyz(2.0, 2.0, 1.0),
                xyz(3.0, 2.0, 2.0),
                xyz(3.0, 3.0, 3.0),
                xyz(2.0, 2.0, 1.0),
            ])),
        ]);
        result.unwrap_err();
    }

    /// Collections keep genuinely heterogeneous members (no axis promotion),
    /// so size classification and writing must agree per nested member.
    fn assert_exact_mixed_nested_round_trip(shape: &Shape) {
        let expected_len = wkb_len(shape, false).expect("test shape has a finite WKB length");
        let bytes = to_wkb(shape, None, false).expect("mixed nested WKB writes");
        assert_eq!(bytes.len(), expected_len);
        assert_eq!(&parse_wkb(&bytes).expect("written WKB parses").shape, shape);
    }

    #[test]
    fn mixed_axes_collection_classifies_every_nested_header_once() {
        assert_exact_mixed_nested_round_trip(
            &Shape::geometry_collection(vec![
                Shape::Point(xy(0.0, 0.0)),
                Shape::Point(xym(1.0, 1.0, 7.0)),
                Shape::LineString(line(&[xyz(2.0, 2.0, 3.0), xyz(3.0, 3.0, 4.0)])),
                Shape::empty_polygon(),
            ])
            .expect("test collection is valid"),
        );
    }

    #[test]
    fn xyzm_empty_member_has_axes_typed_child_and_exact_length() {
        let shape = crate::io::parse_wkt("MULTILINESTRING ZM (EMPTY, (0 0 1 2, 1 1 3 4))")
            .expect("valid typed empty member");
        let bytes = to_wkb(&shape, None, false).expect("WKB writes");
        assert_eq!(bytes.len(), wkb_len(&shape, false).expect("length"));
        assert_eq!(u32::from_le_bytes(bytes[1..5].try_into().unwrap()), 3005);
        assert_eq!(u32::from_le_bytes(bytes[5..9].try_into().unwrap()), 2);
        assert_eq!(u32::from_le_bytes(bytes[10..14].try_into().unwrap()), 3002);
        assert_eq!(u32::from_le_bytes(bytes[14..18].try_into().unwrap()), 0);
        assert_eq!(u32::from_le_bytes(bytes[19..23].try_into().unwrap()), 3002);
        assert_eq!(u32::from_le_bytes(bytes[23..27].try_into().unwrap()), 2);
    }

    #[test]
    fn mixed_collection_child_headers_keep_their_axes() {
        let shape = Shape::geometry_collection(vec![
            Shape::Point(xy(0.0, 0.0)),
            Shape::Point(xyz(1.0, 1.0, 3.0)),
        ])
        .expect("valid heterogeneous collection");
        let bytes = to_wkb(&shape, None, false).expect("WKB writes");
        assert_eq!(u32::from_le_bytes(bytes[1..5].try_into().unwrap()), 1007);
        assert_eq!(u32::from_le_bytes(bytes[10..14].try_into().unwrap()), 1);
        assert_eq!(u32::from_le_bytes(bytes[31..35].try_into().unwrap()), 1001);
    }
}
