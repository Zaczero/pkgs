#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;
use crate::geometry::{CoordSeqBuilder, MOrdinate, ZOrdinate, column_all_finite};

pub(crate) fn parse_wkb(value: &[u8]) -> Result<WkbGeometry> {
    parse_wkb_inner(value).map_err(|error| parse_content(ParseFormat::Wkb, error))
}

fn parse_wkb_inner(value: &[u8]) -> Result<WkbGeometry> {
    let mut reader = WkbReader { value, offset: 0 };
    let (shape, crs) = reader.read_shape()?;
    if reader.offset != value.len() {
        return Err(IoError::wkb("trailing bytes after WKB geometry"));
    }
    Ok(WkbGeometry { shape, crs })
}
fn parse_wkb_type(value: u32) -> Result<(u32, WkbAxes)> {
    let ewkb_z = value & EWKB_Z_FLAG != 0;
    let ewkb_m = value & EWKB_M_FLAG != 0;
    let mut base = value & !(EWKB_Z_FLAG | EWKB_M_FLAG | EWKB_SRID_FLAG);
    let mut iso_z = false;
    let mut iso_m = false;
    for index in [3_usize, 1, 2] {
        let offset = ISO_DIM_OFFSET[index];
        if base >= offset {
            base -= offset;
            iso_z = (index >> 1) != 0;
            iso_m = (index & 1) != 0;
            break;
        }
    }
    if (ewkb_z || ewkb_m) && (iso_z || iso_m) {
        return Err(IoError::wkb(
            "WKB geometry type mixes ISO and EWKB dimensional encodings",
        ));
    }
    Ok((base, WkbAxes {
        z: ewkb_z || iso_z,
        m: ewkb_m || iso_m,
    }))
}

/// OGC empty-point sentinel: every ordinate present in the WKB type is `NaN`.
fn wkb_point_is_empty(x: f64, y: f64, z: Option<f64>, m: Option<f64>, axes: WkbAxes) -> bool {
    if !x.is_nan() || !y.is_nan() {
        return false;
    }
    if axes.z && !z.is_some_and(f64::is_nan) {
        return false;
    }
    if axes.m && !m.is_some_and(f64::is_nan) {
        return false;
    }
    true
}

#[derive(Clone, Copy)]
enum WkbByteOrder {
    BigEndian,
    LittleEndian,
}

struct WkbReader<'a> {
    value: &'a [u8],
    offset: usize,
}

impl WkbReader<'_> {
    const fn remaining_bytes(&self) -> usize {
        self.value.len().saturating_sub(self.offset)
    }

    /// Homogeneous multi members must carry the same axes as the outer type.
    fn require_member_axes(&self, outer: WkbAxes, member: WkbAxes) -> Result<()> {
        if outer.z == member.z && outer.m == member.m {
            Ok(())
        } else {
            Err(IoError::wkb(
                "WKB multi geometry member axes must match the outer type",
            ))
        }
    }

    /// Fallible reserve for a validated decoded total (never panics on OOM).
    fn try_vec_with_capacity<T>(&self, capacity: usize) -> Result<Vec<T>> {
        let mut vec = Vec::new();
        if capacity == 0 {
            return Ok(vec);
        }
        vec.try_reserve(capacity).map_err(|_| {
            IoError::wkb(format!(
                "failed to reserve capacity for {capacity} WKB elements"
            ))
        })?;
        Ok(vec)
    }

    fn validate_coord_count(&self, count: usize, axes: WkbAxes) -> Result<()> {
        // Encoding-true bound: each vertex costs `stride` payload bytes.
        let stride = wkb_dimension(axes) * size_of::<f64>();
        let required = count
            .checked_mul(stride)
            .ok_or_else(|| IoError::wkb("WKB coordinate count is too large"))?;
        if required > self.remaining_bytes() {
            return Err(IoError::wkb("WKB coordinate count exceeds remaining input"));
        }
        Ok(())
    }

    fn validate_element_count(&self, count: usize) -> Result<()> {
        // Nested members: each costs at least a WKB header (byte order + type).
        self.validate_count_bytes(count, WKB_HEADER_BASE, "element")
    }

    /// MultiPoint members compact into a `CoordSeq` — same remaining-byte bound.
    fn validate_multipoint_member_count(&self, count: usize) -> Result<()> {
        self.validate_count_bytes(count, WKB_HEADER_BASE, "element")
    }

    fn validate_ring_count(&self, ring_count: usize) -> Result<()> {
        // Encoding-true bound: each ring starts with a 4-byte vertex count.
        // Empty / short rings fail in `read_ring` via `Ring::MIN_VERTICES_CLOSED`
        // before materializing the ring set (first ring fails immediately).
        self.validate_count_bytes(ring_count, WKB_COUNT, "ring")
    }

    /// Encoding-true bound: `count * minimum_bytes` must fit in remaining input.
    fn validate_count_bytes(&self, count: usize, minimum_bytes: usize, kind: &str) -> Result<()> {
        let required = count
            .checked_mul(minimum_bytes)
            .ok_or_else(|| IoError::wkb(format!("WKB {kind} count is too large")))?;
        if required > self.remaining_bytes() {
            return Err(IoError::wkb(format!(
                "WKB {kind} count exceeds remaining input"
            )));
        }
        Ok(())
    }
}

/// Resolve runtime axes to the const-generic decoder instantiations.
fn decode_coordseq_axes<const BIG: bool>(
    bytes: &[u8],
    axes: WkbAxes,
    count: usize,
) -> Result<CoordSeq> {
    match (axes.z, axes.m) {
        (false, false) => decode_coordseq::<BIG, false, false>(bytes, count),
        (true, false) => decode_coordseq::<BIG, true, false>(bytes, count),
        (false, true) => decode_coordseq::<BIG, false, true>(bytes, count),
        (true, true) => decode_coordseq::<BIG, true, true>(bytes, count),
    }
}

/// Decode one coordinate run into separated columns, monomorphized over
/// byte order AND axes so the hot loop carries no per-vertex branch or
/// `Option` at all — eight straight-line loop bodies, dispatched once.
fn decode_coordseq<const BIG: bool, const Z: bool, const M: bool>(
    bytes: &[u8],
    count: usize,
) -> Result<CoordSeq> {
    let decode = |array: [u8; 8]| {
        if BIG {
            f64::from_be_bytes(array)
        } else {
            f64::from_le_bytes(array)
        }
    };
    let stride = (2 + usize::from(Z) + usize::from(M)) * size_of::<f64>();
    // Validated coordinate count (caller checked remaining payload bytes):
    // fallible exact reservation — never `Vec::with_capacity` (OOM abort).
    let mut xs = Vec::new();
    xs.try_reserve(count).map_err(|_| {
        IoError::wkb(format!(
            "failed to reserve capacity for {count} WKB coordinates"
        ))
    })?;
    let mut ys = Vec::new();
    ys.try_reserve(count).map_err(|_| {
        IoError::wkb(format!(
            "failed to reserve capacity for {count} WKB coordinates"
        ))
    })?;
    let mut zs = if Z {
        let mut column = Vec::new();
        column.try_reserve(count).map_err(|_| {
            IoError::wkb(format!(
                "failed to reserve capacity for {count} WKB coordinates"
            ))
        })?;
        Some(column)
    } else {
        None
    };
    let mut ms = if M {
        let mut column = Vec::new();
        column.try_reserve(count).map_err(|_| {
            IoError::wkb(format!(
                "failed to reserve capacity for {count} WKB coordinates"
            ))
        })?;
        Some(column)
    } else {
        None
    };
    for vertex in bytes.chunks_exact(stride) {
        let (ordinates, _) = vertex.as_chunks::<{ size_of::<f64>() }>();
        xs.push(decode(ordinates[0]));
        ys.push(decode(ordinates[1]));
        if let Some(column) = &mut zs {
            column.push(decode(ordinates[2]));
        }
        if let Some(column) = &mut ms {
            column.push(decode(ordinates[2 + usize::from(Z)]));
        }
    }
    if !column_all_finite(&xs)
        || !column_all_finite(&ys)
        || zs
            .as_deref()
            .is_some_and(|column| !column_all_finite(column))
        || ms
            .as_deref()
            .is_some_and(|column| !column_all_finite(column))
    {
        return Err(GeometryErrorKind::NonFiniteCoordinate.into());
    }
    CoordSeq::try_from_columns(xs.into(), ys.into(), zs.map(Into::into), ms.map(Into::into))
}

/// A member's position inside a nested WKB container. Threaded only so a
/// nested-SRID conflict can name the offending path; the string is built
/// lazily, on error, from this parent-linked stack (no per-element alloc).
#[derive(Clone, Copy)]
struct WkbMemberPath<'a> {
    parent: Option<&'a Self>,
    container: u32,
    index: usize,
}

impl WkbMemberPath<'_> {
    fn render(&self) -> String {
        let mut segments = Vec::new();
        let mut node = Some(self);
        while let Some(current) = node {
            segments.push(format!(
                "{} member {}",
                wkb_container_name(current.container),
                current.index
            ));
            node = current.parent;
        }
        segments.reverse();
        segments.join(" > ")
    }
}

const fn wkb_container_name(code: u32) -> &'static str {
    match code {
        WKB_MULTIPOINT => "MULTIPOINT",
        WKB_MULTILINESTRING => "MULTILINESTRING",
        WKB_MULTIPOLYGON => "MULTIPOLYGON",
        WKB_GEOMETRYCOLLECTION => "GEOMETRYCOLLECTION",
        _ => "geometry",
    }
}

/// PostGIS SRID 0 is unknown/unspecified — same as a missing flag (top-level
/// [`crate::io::crs_from_srid`] rule). Normalize before nested reconciliation so
/// outer-0/child-4326, outer-4326/child-0, and sibling 0/4326 never invent a
/// conflict against the unknown sentinel.
#[inline]
const fn normalize_ewkb_srid(srid: Option<u32>) -> Option<u32> {
    match srid {
        Some(0) | None => None,
        some => some,
    }
}

/// Reconcile an element's own EWKB SRID with the SRID inherited from an
/// enclosing element. PostGIS may stamp the SRID flag on nested members, so a
/// nested SRID equal to (or absent against) the parent's is accepted; only a
/// genuine parent/child disagreement is rejected, naming both codes and the
/// member path. SRID 0 is treated as absent on both sides first.
fn resolve_nested_srid(
    inherited: Option<u32>,
    local: Option<u32>,
    path: Option<&WkbMemberPath<'_>>,
) -> Result<Option<u32>> {
    let inherited = normalize_ewkb_srid(inherited);
    let local = normalize_ewkb_srid(local);
    match (inherited, local) {
        (Some(parent), Some(child)) if parent != child => {
            let location = path.map_or_else(|| "nested geometry".to_owned(), WkbMemberPath::render);
            Err(IoError::wkb(format!(
                "nested WKB SRID {child} conflicts with enclosing SRID {parent} at {location}"
            )))
        },
        (_, Some(child)) => Ok(Some(child)),
        (inherited, None) => Ok(inherited),
    }
}

impl WkbReader<'_> {
    fn read_shape(&mut self) -> Result<(Shape, Option<SmolStr>)> {
        let (shape, srid) = self.read_geometry(None, None, 0)?;
        // SRID 0 → CRS-free; nonzero → canonical PROJ CRS (rejects unknowns).
        let crs = match srid {
            Some(code) => crate::io::crs_from_srid(code)?,
            None => None,
        };
        Ok((shape, crs))
    }

    /// Read one geometry, possibly a nested member. `inherited` is the SRID
    /// declared by an enclosing element and `path` its position; a nested SRID
    /// is reconciled against them by [`resolve_nested_srid`]. Returns the
    /// resolved SRID so it flows down to this geometry's own members.
    #[expect(
        clippy::too_many_lines,
        reason = "WKB type dispatch is one exhaustive match; splitting obscures kind coverage"
    )]
    fn read_geometry(
        &mut self,
        inherited: Option<u32>,
        path: Option<&WkbMemberPath<'_>>,
        depth: usize,
    ) -> Result<(Shape, Option<u32>)> {
        if depth >= MAX_PARSE_DEPTH {
            return Err(IoError::wkb(format!(
                "WKB geometry nesting exceeds the limit of {MAX_PARSE_DEPTH}"
            )));
        }
        let byte_order = match self.read_u8()? {
            0 => WkbByteOrder::BigEndian,
            1 => WkbByteOrder::LittleEndian,
            _ => return Err(IoError::wkb("invalid WKB byte order")),
        };
        let raw_geometry_type = self.read_u32(byte_order)?;
        let (geometry_type, axes) = parse_wkb_type(raw_geometry_type)?;
        let local_srid = if raw_geometry_type & EWKB_SRID_FLAG != 0 {
            Some(self.read_u32(byte_order)?)
        } else {
            None
        };
        let mut srid = resolve_nested_srid(inherited, local_srid, path)?;
        let shape = match geometry_type {
            WKB_POINT => self.read_point_shape(byte_order, axes)?,
            WKB_MULTIPOINT => {
                let (points, effective) =
                    self.read_nested_multipoint(byte_order, axes, srid, path, depth)?;
                srid = effective;
                Shape::MultiPoint(points)
            },
            WKB_LINESTRING => {
                let count = self.read_u32(byte_order)? as usize;
                let points = self.read_coordseq(byte_order, axes, count)?;
                Shape::LineString(LineSeq::try_new(points)?)
            },
            WKB_MULTILINESTRING => {
                let (members, effective) = self.read_nested_typed(
                    byte_order,
                    geometry_type,
                    axes,
                    srid,
                    path,
                    |shape| match shape {
                        Shape::LineString(points) => {
                            let member_axes = WkbAxes::from_coordinate_axes(points.axes());
                            if axes.z != member_axes.z || axes.m != member_axes.m {
                                return Err(IoError::wkb(
                                    "WKB multi geometry member axes must match the outer type",
                                ));
                            }
                            Ok(Some(points))
                        },
                        // Typed empty line members are not a separate EmptyKind;
                        // zero-vertex LineStrings are kept (structurally present).
                        _ => Err(IoError::wkb("MULTILINESTRING member must be LineString")),
                    },
                    depth,
                )?;
                srid = effective;
                if members.is_empty() {
                    Shape::typed_empty(EmptyKind::MultiLineString, axes.coordinate_axes())
                } else {
                    Shape::MultiLineString(members)
                }
            },
            WKB_POLYGON => self.read_polygon_shape(byte_order, axes)?,
            WKB_MULTIPOLYGON => {
                let (members, effective) = self.read_nested_typed(
                    byte_order,
                    geometry_type,
                    axes,
                    srid,
                    path,
                    |shape| match shape {
                        Shape::Polygon(polygon) => {
                            let member_axes = WkbAxes::from_polygon(&polygon);
                            if axes.z != member_axes.z || axes.m != member_axes.m {
                                return Err(IoError::wkb(
                                    "WKB multi geometry member axes must match the outer type",
                                ));
                            }
                            Ok(Some(polygon))
                        },
                        // Matching typed empty Polygon members: accept, validate
                        // axes, then drop — same normalization as the WKT path.
                        Shape::Empty(EmptyKind::Polygon, empty_axes) => {
                            let member_axes = WkbAxes::from_coordinate_axes(empty_axes);
                            if axes.z != member_axes.z || axes.m != member_axes.m {
                                return Err(IoError::wkb(
                                    "WKB multi geometry member axes must match the outer type",
                                ));
                            }
                            Ok(None)
                        },
                        _ => Err(IoError::wkb("MULTIPOLYGON member must be Polygon")),
                    },
                    depth,
                )?;
                srid = effective;
                if members.is_empty() {
                    Shape::typed_empty(EmptyKind::MultiPolygon, axes.coordinate_axes())
                } else {
                    Shape::MultiPolygon(members)
                }
            },
            WKB_GEOMETRYCOLLECTION => {
                let (members, effective) =
                    self.read_nested_shapes(byte_order, geometry_type, srid, path, depth)?;
                srid = effective;
                if members.is_empty() {
                    Shape::typed_empty(EmptyKind::GeometryCollection, axes.coordinate_axes())
                } else {
                    Shape::GeometryCollection(members)
                }
            },
            value => {
                return Err(IoError::wkb(unsupported_wkb_geometry_type_message(
                    raw_geometry_type,
                    value,
                )));
            },
        };
        Ok((shape, srid))
    }

    fn read_u8(&mut self) -> Result<u8> {
        let value = *self
            .value
            .get(self.offset)
            .ok_or_else(|| IoError::wkb("unexpected end of WKB"))?;
        self.offset += 1;
        Ok(value)
    }

    fn read_u32(&mut self, byte_order: WkbByteOrder) -> Result<u32> {
        let bytes = self.read_array::<4>()?;
        Ok(match byte_order {
            WkbByteOrder::BigEndian => u32::from_be_bytes(bytes),
            WkbByteOrder::LittleEndian => u32::from_le_bytes(bytes),
        })
    }

    fn read_f64(&mut self, byte_order: WkbByteOrder) -> Result<f64> {
        let bytes = self.read_array::<8>()?;
        Ok(match byte_order {
            WkbByteOrder::BigEndian => f64::from_be_bytes(bytes),
            WkbByteOrder::LittleEndian => f64::from_le_bytes(bytes),
        })
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        // `first_chunk` borrows `[u8; N]` directly (no `offset + N` that could
        // overflow, no intermediate copy); the success proves `offset + N <= len`
        // so the advance below cannot overflow.
        let out = *self
            .value
            .get(self.offset..)
            .and_then(<[u8]>::first_chunk::<N>)
            .ok_or_else(|| IoError::wkb("unexpected end of WKB"))?;
        self.offset += N;
        Ok(out)
    }

    /// Read a WKB point, decoding the OGC empty-point convention (every present
    /// ordinate `NaN`) to the typed empty carrying the header's axes. Raw
    /// ordinates are read first so the all-`NaN` sentinel is detected before
    /// `Point::new_axes` rejects non-finite coordinates.
    fn read_point_shape(&mut self, byte_order: WkbByteOrder, axes: WkbAxes) -> Result<Shape> {
        let x = self.read_f64(byte_order)?;
        let y = self.read_f64(byte_order)?;
        let z = axes.z.then(|| self.read_f64(byte_order)).transpose()?;
        let m = axes.m.then(|| self.read_f64(byte_order)).transpose()?;
        if wkb_point_is_empty(x, y, z, m, axes) {
            return Ok(Shape::typed_empty(EmptyKind::Point, axes.coordinate_axes()));
        }
        Ok(Shape::Point(Point::new_axes(
            x,
            y,
            ZOrdinate(z),
            MOrdinate(m),
        )?))
    }

    /// Borrow the next `len` bytes and advance the cursor (overflow-safe).
    fn read_bytes(&mut self, len: usize) -> Result<&[u8]> {
        let end = self
            .offset
            .checked_add(len)
            .filter(|&end| end <= self.value.len())
            .ok_or_else(|| IoError::wkb("unexpected end of WKB"))?;
        let bytes = &self.value[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }

    /// Read a `count`-vertex coordinate run straight into separated `CoordSeq`
    /// columns. WKB stores interleaved `x, y, (z), (m)` per vertex, so a copy
    /// to deinterleave is unavoidable, but this skips any per-vertex `Point`
    /// build and `Vec<Point>` re-gather. (A `bytemuck` whole-run cast is not
    /// used here: WKB byte buffers are rarely `f64`-aligned, so the guarded
    /// cast would fall back to this scalar decode anyway.)
    fn read_coordseq(
        &mut self,
        byte_order: WkbByteOrder,
        axes: WkbAxes,
        count: usize,
    ) -> Result<CoordSeq> {
        self.validate_coord_count(count, axes)?;
        let stride = wkb_dimension(axes) * size_of::<f64>();
        let byte_len = count
            .checked_mul(stride)
            .ok_or_else(|| IoError::wkb("WKB coordinate run is too large"))?;
        let bytes = self.read_bytes(byte_len)?;

        // Dispatch ONCE on (byte order, axes) — the loop body is fully
        // monomorphized, so the hot loop re-tests neither.
        match byte_order {
            WkbByteOrder::BigEndian => decode_coordseq_axes::<true>(bytes, axes, count),
            WkbByteOrder::LittleEndian => decode_coordseq_axes::<false>(bytes, axes, count),
        }
    }

    fn read_ring(&mut self, byte_order: WkbByteOrder, axes: WkbAxes) -> Result<CoordSeq> {
        let count = self.read_u32(byte_order)? as usize;
        // Structural rule: a polygon ring needs ≥ MIN_VERTICES_CLOSED vertices.
        // Independent of any size budget — empty rings are simply malformed.
        if count < Ring::MIN_VERTICES_CLOSED {
            return Err(IoError::wkb(format!(
                "WKB polygon ring requires at least {} coordinates, got {count}",
                Ring::MIN_VERTICES_CLOSED
            )));
        }
        self.read_coordseq(byte_order, axes, count)
    }

    /// Read a WKB polygon, decoding a zero-ring body to the typed empty
    /// polygon carrying the header's axes (the OGC empty-polygon convention)
    /// rather than erroring.
    fn read_polygon_shape(&mut self, byte_order: WkbByteOrder, axes: WkbAxes) -> Result<Shape> {
        let ring_count = self.read_u32(byte_order)? as usize;
        self.validate_ring_count(ring_count)?;
        if ring_count == 0 {
            return Ok(Shape::typed_empty(
                EmptyKind::Polygon,
                axes.coordinate_axes(),
            ));
        }
        // Rings are structurally checked (min vertices) then accepted as
        // closed storage without re-running winding/simplicity so
        // `Geometry.validate()` can still diagnose content issues.
        // Shell first, holes straight into their final Vec (no front removal).
        let shell = Ring::from_trusted_closed(self.read_ring(byte_order, axes)?);
        let mut holes = self.try_vec_with_capacity(ring_count - 1)?;
        for _ in 0..ring_count - 1 {
            holes.push(Ring::from_trusted_closed(self.read_ring(byte_order, axes)?));
        }
        Ok(Shape::Polygon(Polygon::new(shell, holes)))
    }

    /// Read a container's members, threading the effective SRID across
    /// siblings: the first member that declares an EWKB SRID under an
    /// SRID-less container ESTABLISHES it (PostGIS payloads may stamp members
    /// only), and every later member reconciles against it, so sibling
    /// disagreement rejects with the member path. The established SRID is
    /// returned so the container adopts it.
    fn read_nested_shapes(
        &mut self,
        byte_order: WkbByteOrder,
        container: u32,
        inherited: Option<u32>,
        parent: Option<&WkbMemberPath<'_>>,
        depth: usize,
    ) -> Result<(Vec<Shape>, Option<u32>)> {
        let count = self.read_u32(byte_order)? as usize;
        self.validate_element_count(count)?;
        let mut shapes = self.try_vec_with_capacity(count)?;
        let mut effective = inherited;
        for index in 0..count {
            let node = WkbMemberPath {
                parent,
                container,
                index,
            };
            let (shape, member_srid) = self.read_geometry(effective, Some(&node), depth + 1)?;
            effective = effective.or(member_srid);
            shapes.push(shape);
        }
        Ok((shapes, effective))
    }

    /// Decode a homogeneous nested container one member at a time, moving
    /// each concrete payload straight into the final typed vector. `extract`
    /// returns `Ok(Some(item))` to keep, `Ok(None)` to drop a matching typed
    /// empty (WKT-parity normalize), or `Err` for wrong kind / axes. A
    /// malformed or wrong-kind member is still fully decoded before the same
    /// container error is raised, preserving the generic reader's error
    /// precedence.
    fn read_nested_typed<T>(
        &mut self,
        byte_order: WkbByteOrder,
        container: u32,
        _outer_axes: WkbAxes,
        inherited: Option<u32>,
        parent: Option<&WkbMemberPath<'_>>,
        mut extract: impl FnMut(Shape) -> Result<Option<T>>,
        depth: usize,
    ) -> Result<(Vec<T>, Option<u32>)> {
        let count = self.read_u32(byte_order)? as usize;
        self.validate_element_count(count)?;
        let mut items = self.try_vec_with_capacity(count)?;
        let mut effective = inherited;
        let mut member_error = None;
        for index in 0..count {
            let node = WkbMemberPath {
                parent,
                container,
                index,
            };
            let (shape, member_srid) = self.read_geometry(effective, Some(&node), depth + 1)?;
            effective = effective.or(member_srid);
            match extract(shape) {
                Ok(Some(item)) => items.push(item),
                Ok(None) => {},
                Err(error) => {
                    // The old generic staging lane decoded every sibling
                    // before validating container member kinds. Retain that
                    // boundary error precedence without retaining each Shape.
                    member_error.get_or_insert(error);
                },
            }
        }
        if let Some(error) = member_error {
            return Err(error);
        }
        Ok((items, effective))
    }

    fn read_nested_multipoint(
        &mut self,
        byte_order: WkbByteOrder,
        axes: WkbAxes,
        inherited: Option<u32>,
        parent: Option<&WkbMemberPath<'_>>,
        depth: usize,
    ) -> Result<(CoordSeq, Option<u32>)> {
        let count = self.read_u32(byte_order)? as usize;
        // Compact into CoordSeq: no flat Shape charge (would falsely cap
        // valid MultiPoints at ~65_536 members under the structure budget).
        self.validate_multipoint_member_count(count)?;
        let mut builder = None;
        let mut effective = inherited;
        let mut member_error = None;
        for index in 0..count {
            let node = WkbMemberPath {
                parent,
                container: WKB_MULTIPOINT,
                index,
            };
            let (shape, member_srid) = self.read_geometry(effective, Some(&node), depth + 1)?;
            effective = effective.or(member_srid);
            match shape {
                Shape::Point(point) => {
                    let member_axes = WkbAxes::from_point(point);
                    if let Err(error) = self.require_member_axes(axes, member_axes) {
                        member_error.get_or_insert(error);
                        continue;
                    }
                    builder
                        .get_or_insert_with(|| {
                            CoordSeqBuilder::with_capacity(axes.coordinate_axes(), count)
                        })
                        .push(point);
                },
                // Matching typed empty Point members: accept, validate axes,
                // drop — same normalize as WKT `MULTIPOINT (EMPTY, (1 2))`.
                Shape::Empty(EmptyKind::Point, empty_axes) => {
                    let member_axes = WkbAxes::from_coordinate_axes(empty_axes);
                    if let Err(error) = self.require_member_axes(axes, member_axes) {
                        member_error.get_or_insert(error);
                    }
                },
                _ => {
                    member_error
                        .get_or_insert_with(|| IoError::wkb("MULTIPOINT member must be Point"));
                },
            }
        }
        if let Some(error) = member_error {
            return Err(error);
        }
        let points = match builder {
            Some(builder) => builder.finish().map_err(|_| {
                IoError::wkb("MULTIPOINT members must have matching coordinate axes")
            })?,
            None => CoordSeq::empty(axes.coordinate_axes()),
        };
        Ok((points, effective))
    }
}

fn unsupported_wkb_geometry_type_message(raw: u32, base: u32) -> String {
    let name = match base {
        8 => Some("CircularString"),
        9 => Some("CompoundCurve"),
        10 => Some("CurvePolygon"),
        11 => Some("MultiCurve"),
        12 => Some("MultiSurface"),
        13 => Some("Curve"),
        14 => Some("Surface"),
        15 => Some("PolyhedralSurface"),
        16 => Some("TIN"),
        17 => Some("Triangle"),
        _ => None,
    };
    match name {
        Some(name) if raw == base => format!("unsupported WKB geometry type {name} ({base})"),
        Some(name) => format!("unsupported WKB geometry type {name} ({base}; raw {raw})"),
        None if raw == base => format!("unsupported WKB geometry type {base}"),
        None => format!("unsupported WKB geometry type {base} (raw {raw})"),
    }
}
