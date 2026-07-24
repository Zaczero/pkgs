#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;

pub(crate) fn to_wkb(shape: &Shape, crs: Option<&str>, include_srid: bool) -> Result<Vec<u8>> {
    let srid = wkb_srid(crs, include_srid)?;
    let geometry = WkbShape::classify(shape);
    // Size the buffer once from the exact serialized length, so even a
    // million-vertex geometry fills with no reallocation churn.
    let mut out = Vec::with_capacity(geometry.encoded_len(srid));
    geometry.write(&mut out, crs, include_srid)?;
    debug_assert_eq!(
        out.len(),
        geometry.encoded_len(srid),
        "WKB classification length drifted from its writer"
    );
    Ok(out)
}

pub(crate) fn write_wkb_to(
    out: &mut Vec<u8>,
    shape: &Shape,
    crs: Option<&str>,
    include_srid: bool,
) -> Result<()> {
    wkb_srid(crs, include_srid)?;
    WkbShape::classify(shape).write(out, crs, include_srid)
}

fn wkb_srid(crs: Option<&str>, include_srid: bool) -> Result<bool> {
    extended_srid_code(crs, include_srid, "EWKB").map(|code| code.is_some())
}

trait WkbOut {
    fn push_byte(&mut self, value: u8);
    fn extend_bytes(&mut self, bytes: &[u8]);

    /// Append `xs.len()` interleaved little-endian vertices in `x, y, (z), (m)`
    /// order. Every supplied ordinate column MUST have exactly `xs.len()`
    /// elements; a mismatch is an error, never silent truncation.
    fn write_columns(
        &mut self,
        xs: &[f64],
        ys: &[f64],
        zs: Option<&[f64]>,
        ms: Option<&[f64]>,
    ) -> Result<()>;
}

impl WkbOut for Vec<u8> {
    fn push_byte(&mut self, value: u8) {
        self.push(value);
    }

    fn extend_bytes(&mut self, bytes: &[u8]) {
        self.extend_from_slice(bytes);
    }

    fn write_columns(
        &mut self,
        xs: &[f64],
        ys: &[f64],
        zs: Option<&[f64]>,
        ms: Option<&[f64]>,
    ) -> Result<()> {
        let count = xs.len();
        // The unsafe fill commits `count * dimension` vertices out of spare
        // capacity, one `[u8; 8]` lane per ordinate. A column shorter than
        // `count` would leave committed bytes uninitialized once `set_len`
        // runs, so the equal-length invariant (upheld by every `CoordSeq`
        // caller) is validated HERE rather than assumed — the unsafe block's
        // soundness is then locally provable from this check alone.
        if ys.len() != count
            || zs.is_some_and(|z| z.len() != count)
            || ms.is_some_and(|m| m.len() != count)
        {
            return Err(IoError::wkb(
                "WKB coordinate columns have mismatched lengths",
            ));
        }
        let dimension = 2 + usize::from(zs.is_some()) + usize::from(ms.is_some());
        let byte_len = count * dimension * size_of::<f64>();
        let base = self.len();
        self.reserve(byte_len);
        // SAFETY: `reserve` guarantees `spare_capacity_mut().len() >= byte_len`.
        // `byte_len` is an exact multiple of 8, so `as_chunks_mut::<8>` yields
        // `byte_len / 8` whole `[u8; 8]` lanes and an empty remainder. Each arm
        // zips those lanes 1:1 with the flattened ordinate stream; the
        // length check above proves the stream has exactly `byte_len / 8`
        // ordinates, so `zip` writes every lane and `set_len` commits only
        // fully-initialized bytes. Assigning `*lane = value.to_le_bytes()`
        // stores into a fixed-size array (no per-store bounds check).
        unsafe {
            let spare = self.spare_capacity_mut();
            let bytes = std::slice::from_raw_parts_mut(spare.as_mut_ptr().cast::<u8>(), byte_len);
            let (lanes, rest) = bytes.as_chunks_mut::<8>();
            debug_assert!(rest.is_empty());
            match (zs, ms) {
                (None, None) => {
                    let stream = std::iter::zip(xs, ys).flat_map(|(&x, &y)| [x, y]);
                    for (lane, value) in std::iter::zip(lanes, stream) {
                        *lane = value.to_le_bytes();
                    }
                },
                (Some(zs), None) => {
                    let stream = std::iter::zip(std::iter::zip(xs, ys), zs)
                        .flat_map(|((&x, &y), &z)| [x, y, z]);
                    for (lane, value) in std::iter::zip(lanes, stream) {
                        *lane = value.to_le_bytes();
                    }
                },
                (None, Some(ms)) => {
                    let stream = std::iter::zip(std::iter::zip(xs, ys), ms)
                        .flat_map(|((&x, &y), &m)| [x, y, m]);
                    for (lane, value) in std::iter::zip(lanes, stream) {
                        *lane = value.to_le_bytes();
                    }
                },
                (Some(zs), Some(ms)) => {
                    let stream = std::iter::zip(std::iter::zip(std::iter::zip(xs, ys), zs), ms)
                        .flat_map(|(((&x, &y), &z), &m)| [x, y, z, m]);
                    for (lane, value) in std::iter::zip(lanes, stream) {
                        *lane = value.to_le_bytes();
                    }
                },
            }
            self.set_len(base + byte_len);
        }
        Ok(())
    }
}

/// One geometry's complete WKB classification. Size calculation and writing
/// consume this same kind/axes/body value, so nested heterogeneous dimensions
/// cannot drift between the allocation pass and the emitted bytes.
#[derive(Clone, Copy)]
struct WkbShape<'a> {
    kind: IoGeometryKind,
    axes: WkbAxes,
    body: WkbBody<'a>,
}

#[derive(Clone, Copy)]
enum WkbBody<'a> {
    Point(Point),
    MultiPoint(&'a CoordSeq),
    LineString(&'a LineSeq),
    MultiLineString(&'a [LineSeq]),
    Polygon(&'a Polygon),
    MultiPolygon(&'a [Polygon]),
    GeometryCollection(&'a [Shape]),
    EmptyPoint,
    EmptyPolygon,
}

impl<'a> WkbShape<'a> {
    fn classify(shape: &'a Shape) -> Self {
        match shape {
            Shape::Point(point) => Self::point(*point),
            Shape::MultiPoint(points) => Self {
                kind: IoGeometryKind::MultiPoint,
                axes: WkbAxes::from_coordinate_axes(points.axes()),
                body: WkbBody::MultiPoint(points),
            },
            Shape::LineString(line) => Self::line(line),
            Shape::MultiLineString(lines) => Self {
                kind: IoGeometryKind::MultiLineString,
                axes: WkbAxes::from_coordinate_axes(shape.axes()),
                body: WkbBody::MultiLineString(lines),
            },
            Shape::Polygon(polygon) => Self::polygon(polygon),
            Shape::MultiPolygon(polygons) => Self {
                kind: IoGeometryKind::MultiPolygon,
                axes: WkbAxes::from_coordinate_axes(shape.axes()),
                body: WkbBody::MultiPolygon(polygons),
            },
            Shape::GeometryCollection(geometries) => Self {
                kind: IoGeometryKind::GeometryCollection,
                axes: WkbAxes::from_coordinate_axes(shape.axes()),
                body: WkbBody::GeometryCollection(geometries),
            },
            // A typed empty writes its declared axes into the header type code
            // (1001-style for `POINT Z EMPTY`) with an axes-matched body: the
            // NaN sentinel per present ordinate for a point, a zero count for
            // the counted kinds.
            Shape::Empty(kind, axes) => {
                let axes = WkbAxes::from_coordinate_axes(*axes);
                match kind {
                    EmptyKind::Point => Self {
                        kind: IoGeometryKind::Point,
                        axes,
                        body: WkbBody::EmptyPoint,
                    },
                    EmptyKind::Polygon => Self {
                        kind: IoGeometryKind::Polygon,
                        axes,
                        body: WkbBody::EmptyPolygon,
                    },
                    EmptyKind::MultiLineString => Self {
                        kind: IoGeometryKind::MultiLineString,
                        axes,
                        body: WkbBody::MultiLineString(&[]),
                    },
                    EmptyKind::MultiPolygon => Self {
                        kind: IoGeometryKind::MultiPolygon,
                        axes,
                        body: WkbBody::MultiPolygon(&[]),
                    },
                    EmptyKind::GeometryCollection => Self {
                        kind: IoGeometryKind::GeometryCollection,
                        axes,
                        body: WkbBody::GeometryCollection(&[]),
                    },
                }
            },
        }
    }

    fn point(point: Point) -> Self {
        Self {
            kind: IoGeometryKind::Point,
            axes: WkbAxes::from_point(point),
            body: WkbBody::Point(point),
        }
    }

    fn line(line: &'a LineSeq) -> Self {
        Self {
            kind: IoGeometryKind::LineString,
            axes: WkbAxes::from_coordinate_axes(line.axes()),
            body: WkbBody::LineString(line),
        }
    }

    fn polygon(polygon: &'a Polygon) -> Self {
        Self {
            kind: IoGeometryKind::Polygon,
            axes: WkbAxes::from_polygon(polygon),
            body: WkbBody::Polygon(polygon),
        }
    }

    fn encoded_len(self, srid: bool) -> usize {
        WKB_HEADER_BASE + if srid { 4 } else { 0 } + self.body.encoded_len(self.axes)
    }

    fn write(self, out: &mut impl WkbOut, crs: Option<&str>, include_srid: bool) -> Result<()> {
        write_wkb_header(out, self.kind, self.axes, crs, include_srid);
        self.body.write(out, self.axes)
    }
}

impl WkbBody<'_> {
    fn encoded_len(self, axes: WkbAxes) -> usize {
        match self {
            Self::Point(_) | Self::EmptyPoint => wkb_coords_len(1, axes),
            Self::MultiPoint(points) => {
                WKB_COUNT + points.len() * (WKB_HEADER_BASE + wkb_coords_len(1, axes))
            },
            Self::LineString(line) => WKB_COUNT + wkb_coords_len(line.len(), axes),
            // Homogeneous multiparts promote every member to the multipart's
            // union axes, so each member header + body is sized against `axes`
            // (the parent's), NOT the member's own axes — a mixed XY/XYZ
            // multipart must not size a child at XY while the outer type is Z.
            Self::MultiLineString(lines) => {
                WKB_COUNT
                    + lines
                        .iter()
                        .map(|line| WKB_HEADER_BASE + WKB_COUNT + wkb_coords_len(line.len(), axes))
                        .sum::<usize>()
            },
            Self::Polygon(polygon) => wkb_polygon_body_len(polygon, axes),
            Self::MultiPolygon(polygons) => {
                WKB_COUNT
                    + polygons
                        .iter()
                        .map(|polygon| WKB_HEADER_BASE + wkb_polygon_body_len(polygon, axes))
                        .sum::<usize>()
            },
            Self::GeometryCollection(geometries) => {
                WKB_COUNT
                    + geometries
                        .iter()
                        .map(|geometry| WkbShape::classify(geometry).encoded_len(false))
                        .sum::<usize>()
            },
            Self::EmptyPolygon => WKB_COUNT,
        }
    }

    fn write(self, out: &mut impl WkbOut, axes: WkbAxes) -> Result<()> {
        match self {
            Self::Point(point) => {
                write_point(out, point, axes);
                Ok(())
            },
            // Every homogeneous-multipart member is written with the parent's
            // union `axes` (its header carries the same Z/M flags, its body
            // fills any absent ordinate with 0.0). A member's own narrower axes
            // never reach the wire — that is what produced standards-invalid
            // outer-Z / child-XY WKB before.
            Self::MultiPoint(points) => {
                write_u32(out, checked_len(points.len())?);
                for point in points {
                    write_wkb_header(out, IoGeometryKind::Point, axes, None, false);
                    write_point(out, point, axes);
                }
                Ok(())
            },
            Self::LineString(line) => write_wkb_sequence(out, line, axes),
            Self::MultiLineString(lines) => {
                write_u32(out, checked_len(lines.len())?);
                for line in lines {
                    write_wkb_header(out, IoGeometryKind::LineString, axes, None, false);
                    write_wkb_sequence(out, line, axes)?;
                }
                Ok(())
            },
            Self::Polygon(polygon) => write_wkb_polygon(out, polygon, axes),
            Self::MultiPolygon(polygons) => {
                write_u32(out, checked_len(polygons.len())?);
                for polygon in polygons {
                    write_wkb_header(out, IoGeometryKind::Polygon, axes, None, false);
                    write_wkb_polygon(out, polygon, axes)?;
                }
                Ok(())
            },
            Self::GeometryCollection(geometries) => {
                write_u32(out, checked_len(geometries.len())?);
                for geometry in geometries {
                    WkbShape::classify(geometry).write(out, None, false)?;
                }
                Ok(())
            },
            Self::EmptyPoint => {
                // The OGC empty-point sentinel: NaN for EVERY ordinate the
                // header's axes declare (2 for XY, 3 for Z/M, 4 for ZM) —
                // symmetric with the reader's `wkb_point_is_empty`.
                for _ in 0..wkb_dimension(axes) {
                    out.extend_bytes(&f64::NAN.to_le_bytes());
                }
                Ok(())
            },
            Self::EmptyPolygon => {
                write_u32(out, 0);
                Ok(())
            },
        }
    }
}

/// Exact serialized WKB/EWKB byte length from the same classification used by
/// the writer. `srid` means the top-level header carries an EWKB SRID.
pub(crate) fn wkb_len(shape: &Shape, srid: bool) -> usize {
    WkbShape::classify(shape).encoded_len(srid)
}

fn wkb_polygon_body_len(polygon: &Polygon, axes: WkbAxes) -> usize {
    WKB_COUNT
        + std::iter::once(&polygon.shell)
            .chain(polygon.holes.iter())
            .map(|ring| WKB_COUNT + wkb_coords_len(ring.coord_count(), axes))
            .sum::<usize>()
}

fn write_wkb_header(
    out: &mut impl WkbOut,
    kind: IoGeometryKind,
    axes: WkbAxes,
    crs: Option<&str>,
    include_srid: bool,
) {
    out.push_byte(1);
    let mut geometry_type = wkb_type(kind, axes, include_srid);
    let srid = if include_srid {
        crs.and_then(crs::parse_epsg).inspect(|_| {
            geometry_type |= EWKB_SRID_FLAG;
        })
    } else {
        None
    };
    write_u32(out, geometry_type);
    if let Some(srid) = srid {
        write_u32(out, srid);
    }
}

fn write_wkb_polygon(out: &mut impl WkbOut, polygon: &Polygon, axes: WkbAxes) -> Result<()> {
    write_u32(out, checked_len(polygon.holes.len() + 1)?);
    write_wkb_sequence(out, &polygon.shell, axes)?;
    for hole in polygon.holes.iter() {
        write_wkb_sequence(out, hole, axes)?;
    }
    Ok(())
}

/// One body writer for every counted point run (linestring bodies and polygon
/// rings): the u32 count, then the column-direct interleave when the sequence's
/// own ordinate columns exactly match the requested `axes`, else the per-point
/// fallback that fills any promoted-but-absent ordinate with 0.0.
fn write_wkb_sequence<C: Coordinates + ?Sized>(
    out: &mut impl WkbOut,
    points: &C,
    axes: WkbAxes,
) -> Result<()> {
    write_u32(out, checked_len(points.coord_count())?);
    // The column-direct fast path applies only when the sequence carries
    // exactly the requested ordinates (the scalar and uniform-multipart case).
    // `axes` is the union over the enclosing multipart, so it is always a
    // superset of a member's columns; a member NARROWER than `axes` takes the
    // per-point path and fills the missing ordinate with 0.0 (the `force_3d`
    // convention) rather than a fabricated NaN.
    if let Some((xs, ys)) = points.xy_columns() {
        let zs = points.z_column();
        let ms = points.m_column();
        if axes.z == zs.is_some() && axes.m == ms.is_some() {
            return out.write_columns(xs, ys, zs, ms);
        }
    }
    for point in points.iter_coords() {
        write_point(out, point, axes);
    }
    Ok(())
}

/// Write one vertex under `axes`. A promoted member may lack an ordinate the
/// union `axes` requires; the absent Z/M is filled with 0.0 (the `force_3d`
/// fill convention). For a scalar or uniform sequence `axes` matches the data,
/// so no fill occurs.
fn write_point(out: &mut impl WkbOut, point: Point, axes: WkbAxes) {
    out.extend_bytes(&point.x.to_le_bytes());
    out.extend_bytes(&point.y.to_le_bytes());
    if axes.z {
        out.extend_bytes(&point.z().unwrap_or(0.0).to_le_bytes());
    }
    if axes.m {
        out.extend_bytes(&point.m().unwrap_or(0.0).to_le_bytes());
    }
}

fn write_u32(out: &mut impl WkbOut, value: u32) {
    out.extend_bytes(&value.to_le_bytes());
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

    fn xy(x: f64, y: f64) -> Point {
        Point::new_unchecked_xy(x, y)
    }

    fn xyz(x: f64, y: f64, z: f64) -> Point {
        Point::new_unchecked_axes(x, y, ZOrdinate(Some(z)), MOrdinate(None))
    }

    fn xym(x: f64, y: f64, m: f64) -> Point {
        Point::new_unchecked_axes(x, y, ZOrdinate(None), MOrdinate(Some(m)))
    }

    fn line(points: &[Point]) -> LineSeq {
        LineSeq::try_new(CoordSeq::from_points(points)).expect("test line is valid")
    }

    fn polygon(points: &[Point]) -> Polygon {
        Polygon::new(
            Ring::from_trusted_closed(CoordSeq::from_points(points)),
            vec![],
        )
    }

    fn outer_wkb_type(bytes: &[u8]) -> u32 {
        assert_eq!(bytes[0], 1, "little-endian byte order");
        u32::from_le_bytes(bytes[1..5].try_into().expect("4 type bytes"))
    }

    /// A mixed-axis homogeneous multipart serializes with the union axes on the
    /// outer AND every child header, so the round trip yields the fully-promoted
    /// shape (absent Z/M filled with 0.0) — never an outer-Z / child-XY body.
    fn assert_promotes_to_union_axes(mixed: &Shape, promoted: &Shape, outer_type: u32) {
        let bytes = to_wkb(mixed, None, false).expect("mixed nested WKB writes");
        assert_eq!(
            bytes.len(),
            wkb_len(mixed, false),
            "length matches classifier"
        );
        assert_eq!(
            outer_wkb_type(&bytes),
            outer_type,
            "outer carries union axes"
        );
        // Re-parsing proves every child header carried the union Z flag: a
        // child written as an XY type/body would parse back to an XY member and
        // fail this equality against the all-Z promoted shape.
        assert_eq!(
            &parse_wkb(&bytes).expect("promoted WKB parses").shape,
            promoted,
        );
    }

    #[test]
    fn mixed_axes_multiline_promotes_members_to_union_axes() {
        assert_promotes_to_union_axes(
            &Shape::MultiLineString(vec![
                line(&[xy(0.0, 0.0), xy(1.0, 1.0)]),
                line(&[xyz(2.0, 2.0, 3.0), xyz(3.0, 3.0, 4.0)]),
            ]),
            &Shape::MultiLineString(vec![
                line(&[xyz(0.0, 0.0, 0.0), xyz(1.0, 1.0, 0.0)]),
                line(&[xyz(2.0, 2.0, 3.0), xyz(3.0, 3.0, 4.0)]),
            ]),
            1005, // WKBMultiLineStringZ
        );
    }

    #[test]
    fn mixed_axes_multipolygon_promotes_members_to_union_axes() {
        assert_promotes_to_union_axes(
            &Shape::MultiPolygon(vec![
                polygon(&[xy(0.0, 0.0), xy(1.0, 0.0), xy(1.0, 1.0), xy(0.0, 0.0)]),
                polygon(&[
                    xyz(2.0, 2.0, 1.0),
                    xyz(3.0, 2.0, 2.0),
                    xyz(3.0, 3.0, 3.0),
                    xyz(2.0, 2.0, 1.0),
                ]),
            ]),
            &Shape::MultiPolygon(vec![
                polygon(&[
                    xyz(0.0, 0.0, 0.0),
                    xyz(1.0, 0.0, 0.0),
                    xyz(1.0, 1.0, 0.0),
                    xyz(0.0, 0.0, 0.0),
                ]),
                polygon(&[
                    xyz(2.0, 2.0, 1.0),
                    xyz(3.0, 2.0, 2.0),
                    xyz(3.0, 3.0, 3.0),
                    xyz(2.0, 2.0, 1.0),
                ]),
            ]),
            1006, // WKBMultiPolygonZ
        );
    }

    /// Collections keep genuinely heterogeneous members (no axis promotion),
    /// so size classification and writing must agree per nested member.
    fn assert_exact_mixed_nested_round_trip(shape: &Shape) {
        let expected_len = wkb_len(shape, false);
        let bytes = to_wkb(shape, None, false).expect("mixed nested WKB writes");
        assert_eq!(bytes.len(), expected_len);
        assert_eq!(&parse_wkb(&bytes).expect("written WKB parses").shape, shape);
    }

    #[test]
    fn mixed_axes_collection_classifies_every_nested_header_once() {
        assert_exact_mixed_nested_round_trip(&Shape::GeometryCollection(vec![
            Shape::Point(xy(0.0, 0.0)),
            Shape::Point(xym(1.0, 1.0, 7.0)),
            Shape::LineString(line(&[xyz(2.0, 2.0, 3.0), xyz(3.0, 3.0, 4.0)])),
            Shape::empty_polygon(),
        ]));
    }

    #[test]
    fn column_writer_rejects_mismatched_lengths_before_initializing_output() {
        let mut out = vec![7_u8, 8];
        let error = out
            .write_columns(&[1.0, 2.0], &[3.0], None, None)
            .expect_err("short y column must be rejected");
        assert_eq!(
            error.to_string(),
            "invalid WKB: WKB coordinate columns have mismatched lengths"
        );
        assert_eq!(out, [7, 8], "failed write leaves existing bytes untouched");
    }
}
