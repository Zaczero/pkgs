#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;
use crate::geometry::{HasM, HasZ, MOrdinate, ZOrdinate};
use crate::py::errors::parameter_error;

/// The direct equal-chord limit calibrated to the wide-band materiality cases.
const BOX_PARALLEL_MAX_CHORD_DEGREES: f64 = 0.247 * 180.0 / std::f64::consts::PI;

pub(super) fn geometry_with_crs_epoch(
    shape: impl Into<std::sync::Arc<ShapeData>>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeometry> {
    Ok(PyGeometry::with_frame(shape, parse_crs_epoch(crs, epoch)?))
}

fn multipart_members(
    values: &Bound<'_, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
    context: &str,
    expected: &str,
    exact: impl Fn(&Shape) -> Option<Shape>,
    raw: impl Fn(&Bound<'_, PyAny>) -> PyResult<Shape>,
) -> PyResult<(Vec<Shape>, Frame)> {
    // Fallible growth (D10) via collect_py_iter — no artificial count ceiling.
    let mut row = 0_usize;
    let mut members = crate::collect_py_iter(values, |item| {
        let this_row = row;
        row += 1;
        if let Some(geometry) = exact_geometry(&item) {
            let shape = exact(geometry.shape.shape()).ok_or_else(|| {
                PyTypeError::new_err(format!(
                    "{context} expected {expected} at element {this_row}"
                ))
            })?;
            Ok(PyGeometry::with_frame(shape, geometry.frame.clone()))
        } else {
            Ok(PyGeometry::with_frame(raw(&item)?, Frame::None))
        }
    })?;
    let frame = Frame::resolve_items(
        &mut members,
        FrameAdoption {
            crs: parse_crs(crs)?,
            epoch: coordinate_epoch_option("epoch", epoch)?,
        },
        context,
    )?;
    Ok((
        members
            .into_iter()
            .map(
                |geometry| match std::sync::Arc::try_unwrap(geometry.shape) {
                    Ok(data) => data.into_shape(),
                    Err(shared) => shared.shape().clone(),
                },
            )
            .collect(),
        frame,
    ))
}

/// The canonical `LINESTRING EMPTY` — an `XY` line with no vertices.
pub(super) fn empty_line_string() -> Shape {
    Shape::LineString(LineSeq::empty(CoordinateAxes::XY))
}

/// The canonical `MULTIPOINT EMPTY` — an `XY` multipoint with no members.
pub(super) fn empty_multi_point() -> Shape {
    Shape::MultiPoint(CoordSeq::empty(CoordinateAxes::XY))
}

/// The canonical `MULTILINESTRING EMPTY` — a multilinestring with no members.
pub(super) const fn empty_multi_line_string() -> Shape {
    Shape::MultiLineString(Vec::new())
}

/// The canonical `MULTIPOLYGON EMPTY` — a multipolygon with no members.
pub(super) const fn empty_multi_polygon() -> Shape {
    Shape::MultiPolygon(Vec::new())
}

/// The canonical `GEOMETRYCOLLECTION EMPTY` — a collection with no members.
pub(super) const fn empty_geometry_collection() -> Shape {
    Shape::GeometryCollection(Vec::new())
}

/// Build a ``Point`` geometry — ``XY``, ``XYZ``, ``XYM``, or ``XYZM``.
pub(crate) fn build_point(
    x: Option<&Bound<'_, PyAny>>,
    y: Option<&Bound<'_, PyAny>>,
    z: Option<&Bound<'_, PyAny>>,
    m: Option<&Bound<'_, PyAny>>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeometry> {
    let shape = match (x, y) {
        (Some(x), Some(y)) => {
            let x = finite_coordinate_required("x", x)?;
            let y = finite_coordinate_required("y", y)?;
            let z = z.map(|z| finite_coordinate_required("z", z)).transpose()?;
            let m = m.map(|m| finite_coordinate_required("m", m)).transpose()?;
            Shape::Point(Point::new_axes(x, y, ZOrdinate(z), MOrdinate(m))?)
        },
        // No coordinates → `POINT EMPTY` (Shapely-compatible `Point()`); Z/M
        // are meaningless without X/Y.
        (None, None) if z.is_none() && m.is_none() => Shape::empty_point(),
        (None, None) => {
            return Err(InvalidGeometryError::new_err(
                "empty point takes no z/m; pass x and y for a non-empty point",
            ));
        },
        _ => {
            return Err(InvalidGeometryError::new_err(
                "point requires both x and y, or neither",
            ));
        },
    };
    geometry_with_crs_epoch(shape, crs, epoch)
}

/// Build a ``LineString`` from an ordered coordinate sequence.
pub(crate) fn build_line_string(
    coordinates: Option<&Bound<'_, PyAny>>,
    x: Option<&Bound<'_, PyAny>>,
    y: Option<&Bound<'_, PyAny>>,
    z: Option<&Bound<'_, PyAny>>,
    m: Option<&Bound<'_, PyAny>>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeometry> {
    // No coordinates → `LINESTRING EMPTY` (Shapely-compatible `LineString()`).
    if coordinates.is_none() && x.is_none() && y.is_none() && z.is_none() && m.is_none() {
        return geometry_with_crs_epoch(empty_line_string(), crs, epoch);
    }
    let vertices = coordinate_vertices(coordinates, x, y, z, m)?;
    geometry_with_crs_epoch(
        Shape::LineString(LineSeq::try_new(vertices).map_err(PyErr::from)?),
        crs,
        epoch,
    )
}

/// Resolve a line/multipoint's vertices from EITHER a coordinate sequence
/// (``[(x, y), …]`` / ``(N, 2)`` array) OR parallel ``x``/``y`` columns (with
/// optional ``z``/``m`` columns) — the columnar inverse of ``coords.x``/``.y``.
pub(super) fn coordinate_vertices(
    coordinates: Option<&Bound<'_, PyAny>>,
    x: Option<&Bound<'_, PyAny>>,
    y: Option<&Bound<'_, PyAny>>,
    z: Option<&Bound<'_, PyAny>>,
    m: Option<&Bound<'_, PyAny>>,
) -> PyResult<CoordSeq> {
    match (coordinates, x, y) {
        // Point-tuple input must round-trip through `Point` (it is AoS by
        // nature); `CoordSeq::from` gathers it into columns.
        (Some(coordinates), None, None) => {
            let points = extract_points(coordinates, z, m)?;
            if points.is_empty() {
                let axes = if z.is_some() || m.is_some() {
                    CoordinateAxes::new(HasZ(z.is_some()), HasM(m.is_some()))
                } else {
                    coordinates
                        .getattr("shape")
                        .ok()
                        .and_then(|shape| shape.extract::<Vec<usize>>().ok())
                        .and_then(|shape| match shape.as_slice() {
                            [0, 2] => Some(CoordinateAxes::XY),
                            [0, 3] => Some(CoordinateAxes::XYZ),
                            [0, 4] => Some(CoordinateAxes::XYZM),
                            _ => None,
                        })
                        .unwrap_or(CoordinateAxes::XY)
                };
                return Ok(CoordSeq::empty(axes));
            }
            Ok(points.into())
        },
        // Column input is already SoA — build the `CoordSeq` straight from the
        // `x`/`y`/`z`/`m` columns, with no `Vec<Point>` bounce.
        (None, Some(x), Some(y)) => {
            let xs = coordinate_arc_values(x.py(), x, "x")?;
            let ys = coordinate_arc_values(y.py(), y, "y")?;
            if xs.len() != ys.len() {
                return Err(InvalidGeometryError::new_err(format!(
                    "x and y must have the same length, got {} and {}",
                    xs.len(),
                    ys.len(),
                )));
            }
            let z = optional_coordinate_arc_values(x.py(), z, xs.len(), "z")?;
            let m = optional_coordinate_arc_values(x.py(), m, xs.len(), "m")?;
            Ok(CoordSeq::from_arc_columns(xs, ys, z, m)?)
        },
        (None, None, None) => Err(InvalidGeometryError::new_err(
            "provide a coordinate sequence or both x= and y= columns",
        )),
        (None, ..) => Err(InvalidGeometryError::new_err(
            "x= and y= columns must be provided together",
        )),
        (Some(_), ..) => Err(InvalidGeometryError::new_err(
            "provide either a coordinate sequence or x=/y= columns, not both",
        )),
    }
}

/// Build a ``Polygon`` from an exterior ring and optional holes.
pub(crate) fn build_polygon(
    shell: Option<&Bound<'_, PyAny>>,
    holes: Option<&Bound<'_, PyAny>>,
    x: Option<&Bound<'_, PyAny>>,
    y: Option<&Bound<'_, PyAny>>,
    z: Option<&Bound<'_, PyAny>>,
    m: Option<&Bound<'_, PyAny>>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeometry> {
    // No shell (or an empty one) yields `POLYGON EMPTY` (Shapely-compatible
    // `Polygon()`). A bare empty shell with holes is contradictory.
    let shell_coords = match (shell, x, y) {
        (Some(shell), None, None) => coordinate_vertices(Some(shell), None, None, z, m)?,
        (None, Some(x), Some(y)) => coordinate_vertices(None, Some(x), Some(y), z, m)?,
        (None, None, None) => {
            let z = optional_coordinates(z, 0, "z")?;
            let m = optional_coordinates(m, 0, "m")?;
            CoordSeq::empty(CoordinateAxes::new(HasZ(z.is_some()), HasM(m.is_some())))
        },
        (None, ..) => {
            return Err(InvalidGeometryError::new_err(
                "x= and y= columns must be provided together",
            ));
        },
        (Some(_), ..) => {
            return Err(InvalidGeometryError::new_err(
                "provide either a shell coordinate sequence or x=/y= columns, not both",
            ));
        },
    };
    if shell_coords.is_empty() {
        // `holes` must still be a (possibly empty) iterable: a wrong-kind value
        // is a TypeError, never silently ignored.
        if let Some(holes) = holes
            && holes.try_iter()?.next().is_some()
        {
            return Err(InvalidGeometryError::new_err(
                "an empty polygon cannot have holes",
            ));
        }
        return geometry_with_crs_epoch(
            Shape::typed_empty(EmptyKind::Polygon, shell_coords.axes()),
            crs,
            epoch,
        );
    }
    let shell = Ring::closed_coordseq(shell_coords)?;
    // Cross-ring axes may differ (XY shell + XYZ hole): keep per-ring storage
    // and expose the union via `coordinate_axes` / the coords view with NaN for
    // absent ordinates — same policy as `from_geojson`. Within one ring,
    // `coordinate_vertices` still rejects mixed-dim vertices.
    let holes = match holes {
        // Fallible growth (D10): `holes=itertools.repeat(...)` → MemoryError.
        Some(value) => crate::collect_py_iter(value, |item| {
            Ok(Ring::closed_coordseq(coordinate_vertices(
                Some(&item),
                None,
                None,
                None,
                None,
            )?)?)
        })?,
        None => Vec::new(),
    };
    geometry_with_crs_epoch(Shape::Polygon(Polygon::new(shell, holes)), crs, epoch)
}

/// Build a ``MultiPoint`` geometry from a coordinate sequence.
pub(crate) fn build_multi_point(
    coordinates: Option<&Bound<'_, PyAny>>,
    x: Option<&Bound<'_, PyAny>>,
    y: Option<&Bound<'_, PyAny>>,
    z: Option<&Bound<'_, PyAny>>,
    m: Option<&Bound<'_, PyAny>>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeometry> {
    // No coordinates → `MULTIPOINT EMPTY` (Shapely-compatible `MultiPoint()`).
    if coordinates.is_none() && x.is_none() && y.is_none() && z.is_none() && m.is_none() {
        return geometry_with_crs_epoch(empty_multi_point(), crs, epoch);
    }
    if x.is_some() || y.is_some() || z.is_some() || m.is_some() {
        return geometry_with_crs_epoch(
            Shape::MultiPoint(coordinate_vertices(coordinates, x, y, z, m)?),
            crs,
            epoch,
        );
    }
    let coordinates = coordinates.expect("non-empty multipart input checked above");
    let (members, frame) = multipart_members(
        coordinates,
        crs,
        epoch,
        "MultiPoint",
        "Point or coordinate",
        |shape| match shape {
            Shape::Point(point) => Some(Shape::Point(*point)),
            _ => None,
        },
        |item| Ok(Shape::Point(extract_coordinate(item)?)),
    )?;
    let points = members
        .into_iter()
        .map(|shape| match shape {
            Shape::Point(point) => point,
            _ => unreachable!("multipart member type checked above"),
        })
        .collect::<Vec<_>>();
    ensure_homogeneous_axes(&points)?;
    Ok(PyGeometry::with_frame(
        Shape::MultiPoint(CoordSeq::from_points(&points)),
        frame,
    ))
}

/// Build a ``MultiLineString`` from a sequence of line coordinate sequences.
pub(crate) fn build_multi_line_string(
    lines: Option<&Bound<'_, PyAny>>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeometry> {
    // No members → `MULTILINESTRING EMPTY` (Shapely-compatible `MultiLineString()`).
    let Some(lines) = lines else {
        return geometry_with_crs_epoch(empty_multi_line_string(), crs, epoch);
    };
    let (members, frame) = multipart_members(
        lines,
        crs,
        epoch,
        "MultiLineString",
        "LineString or coordinate sequence",
        |shape| match shape {
            Shape::LineString(line) => Some(Shape::LineString(line.clone())),
            _ => None,
        },
        |item| {
            Ok(Shape::LineString(
                LineSeq::try_new(CoordSeq::from(extract_points(item, None, None)?))
                    .map_err(PyErr::from)?,
            ))
        },
    )?;
    Ok(PyGeometry::with_frame(
        Shape::MultiLineString(
            members
                .into_iter()
                .map(|shape| match shape {
                    Shape::LineString(line) => line,
                    _ => unreachable!("multipart member type checked above"),
                })
                .collect(),
        ),
        frame,
    ))
}

/// Build a ``MultiPolygon`` from a sequence of polygons.
pub(crate) fn build_multi_polygon(
    polygons: Option<&Bound<'_, PyAny>>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeometry> {
    // No members → `MULTIPOLYGON EMPTY` (Shapely-compatible `MultiPolygon()`).
    let Some(polygons) = polygons else {
        return geometry_with_crs_epoch(empty_multi_polygon(), crs, epoch);
    };
    let (members, frame) = multipart_members(
        polygons,
        crs,
        epoch,
        "MultiPolygon",
        "Polygon or polygon coordinates",
        |shape| match shape {
            Shape::Polygon(polygon) => Some(Shape::Polygon(polygon.clone())),
            _ => None,
        },
        polygon_from_coordinates_item,
    )?;
    Ok(PyGeometry::with_frame(
        Shape::MultiPolygon(
            members
                .into_iter()
                .map(|shape| match shape {
                    Shape::Polygon(polygon) => polygon,
                    _ => unreachable!("multipart member type checked above"),
                })
                .collect(),
        ),
        frame,
    ))
}

pub(super) fn build_box_shape(
    minx: f64,
    miny: f64,
    maxx: f64,
    maxy: f64,
    wrap: Option<&str>,
    crs: Option<&Crs>,
) -> PyResult<Shape> {
    if miny > maxy {
        return Err(InvalidGeometryError::new_err(format!(
            "box miny ({miny}) must not exceed maxy ({maxy})"
        )));
    }
    match wrap {
        None => {
            if minx > maxx {
                return Err(InvalidGeometryError::new_err(format!(
                    "box minx ({minx}) must not exceed maxx ({maxx}) unless wrap='split'"
                )));
            }
            // `box` names a lat/lon rectangle, so its horizontal edges are
            // parallels rather than one long geodesic between corners. Add
            // the minimum number of equal longitude chords at the calibrated
            // angular limit.
            if let Some(crs) = crs
                && (-180.0..=180.0).contains(&minx)
                && (-180.0..=180.0).contains(&maxx)
                && (-90.0..=90.0).contains(&miny)
                && (-90.0..=90.0).contains(&maxy)
                && crs::is_geographic_crs(crs).map_err(PyErr::from)?
            {
                let segments = geographic_box_parallel_segments(minx, maxx);
                if segments > 1 {
                    return Ok(Shape::Polygon(geographic_box_polygon(
                        minx, miny, maxx, maxy, segments,
                    )?));
                }
            }
            Ok(Shape::Polygon(box_polygon(minx, miny, maxx, maxy)?))
        },
        Some("split") => {
            match crs {
                Some(crs) if crs::is_wgs84_lonlat(crs) => {},
                _ => {
                    return Err(CRSError::new_err(
                        "box wrap='split' requires crs=4326 (EPSG:4326 longitude/latitude)",
                    ));
                },
            }
            wrapped_box(minx, miny, maxx, maxy)
        },
        Some(value) => Err(parameter_error(
            crate::tokens::unknown_token_message("box wrap", value, &["split"]),
            "wrap",
        )),
    }
}

/// Number of equal longitude chords needed to approximate parallel edges.
fn geographic_box_parallel_segments(minx: f64, maxx: f64) -> usize {
    ((maxx - minx) / BOX_PARALLEL_MAX_CHORD_DEGREES)
        .ceil()
        .max(1.0) as usize
}

fn geographic_box_polygon(
    minx: f64,
    miny: f64,
    maxx: f64,
    maxy: f64,
    segments: usize,
) -> PyResult<Polygon> {
    let capacity = segments
        .checked_mul(2)
        .and_then(|value| value.checked_add(3))
        .ok_or_else(|| GeometryError::new_err("geographic box has too many vertices"))?;
    let mut ring = crate::try_vec_with_capacity(capacity)?;
    let span = maxx - minx;
    for index in 0..=segments {
        ring.push(Point::new(
            minx + span * index as f64 / segments as f64,
            miny,
        )?);
    }
    ring.push(Point::new(maxx, maxy)?);
    for index in (0..segments).rev() {
        ring.push(Point::new(
            minx + span * index as f64 / segments as f64,
            maxy,
        )?);
    }
    ring.push(Point::new(minx, miny)?);
    Ok(Polygon::new(Ring::from_trusted_closed(ring), Vec::new()))
}

/// A `LineString` array member from raw coordinates.
pub(super) fn line_string_from_data_item(item: &Bound<'_, PyAny>) -> PyResult<Shape> {
    geometry_from_data_item(item, "LineString", |item| {
        let vertices = coordinate_vertices(Some(item), None, None, None, None)?;
        Ok(Shape::LineString(
            LineSeq::try_new(vertices).map_err(PyErr::from)?,
        ))
    })
}

/// A `MultiPoint` array member from raw coordinates.
pub(super) fn multi_point_from_data_item(item: &Bound<'_, PyAny>) -> PyResult<Shape> {
    geometry_from_data_item(item, "MultiPoint", |item| {
        Ok(Shape::MultiPoint(coordinate_vertices(
            Some(item),
            None,
            None,
            None,
            None,
        )?))
    })
}

/// A `MultiLineString` array member from raw line coordinate sequences.
pub(super) fn multi_line_string_from_data_item(item: &Bound<'_, PyAny>) -> PyResult<Shape> {
    geometry_from_data_item(item, "MultiLineString", |item| {
        Ok(Shape::MultiLineString(
            extract_lines(item)?
                .into_iter()
                .map(|line| {
                    let line = CoordSeq::from(line);
                    LineSeq::try_new(line).map_err(PyErr::from)
                })
                .collect::<PyResult<_>>()?,
        ))
    })
}

/// A `MultiPolygon` array member from raw polygon coordinate sequences.
pub(super) fn multi_polygon_from_data_item(item: &Bound<'_, PyAny>) -> PyResult<Shape> {
    geometry_from_data_item(item, "MultiPolygon", |item| {
        Ok(Shape::MultiPolygon(extract_polygons(item)?))
    })
}

pub(super) fn polygon_from_data_item(item: &Bound<'_, PyAny>) -> PyResult<Shape> {
    geometry_from_data_item(item, "Polygon", polygon_from_coordinates_item)
}

fn geometry_from_data_item(
    item: &Bound<'_, PyAny>,
    expected: &'static str,
    build: impl FnOnce(&Bound<'_, PyAny>) -> PyResult<Shape>,
) -> PyResult<Shape> {
    if exact_geometry(item).is_some() {
        return Err(PyTypeError::new_err(format!(
            "{expected} array factories accept raw coordinate inputs only; use GeometryArray(values) for built geometries"
        )));
    }
    build(item)
}

fn polygon_from_coordinates_item(item: &Bound<'_, PyAny>) -> PyResult<Shape> {
    // Materialize once, then classify. Speculative `extract_points` on the
    // parent would consume the first ring of a one-shot iterator and leave
    // only holes for the ring-list path (shell silently dropped).
    let members = crate::collect_py_iter(item, Ok)?;
    polygon_from_coordinate_members(&members)
}

/// Classify a materialized top-level polygon item as a bare shell or ring list.
fn polygon_from_coordinate_members(members: &[Bound<'_, PyAny>]) -> PyResult<Shape> {
    if members.is_empty() {
        return Ok(Shape::empty_polygon());
    }
    // Bare shell coordinate sequence `[(x, y), ...]` — each member is a point.
    if let Ok(shell_points) = points_from_coordinate_members(members) {
        return Ok(Shape::Polygon(Polygon::new(
            Ring::closed(shell_points)?,
            Vec::new(),
        )));
    }
    // Ring list: first member is the shell, the rest are holes.
    Ok(Shape::Polygon(Polygon::new(
        Ring::closed(extract_points(&members[0], None, None)?)?,
        members[1..]
            .iter()
            .map(|ring| Ok(Ring::closed(extract_points(ring, None, None)?)?))
            .collect::<PyResult<Vec<_>>>()?,
    )))
}

/// Parse every top-level member as a coordinate (bare-shell form).
fn points_from_coordinate_members(members: &[Bound<'_, PyAny>]) -> PyResult<Vec<Point>> {
    let mut points = crate::try_vec_with_capacity(members.len())?;
    for member in members {
        crate::try_push(&mut points, extract_coordinate(member)?)?;
    }
    ensure_homogeneous_axes(&points)?;
    Ok(points)
}

pub(super) fn build_geometry_array(
    values: &Bound<'_, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
    item: impl Fn(&Bound<'_, PyAny>) -> PyResult<Shape>,
) -> PyResult<PyGeometryArray> {
    // Fallible growth (D10): linestrings/polygons factories and similar
    // `itertools.repeat` inputs terminate with MemoryError, never abort.
    // Stream directly into StreamingShapes — no intermediate Vec<Shape>.
    let frame = parse_crs_epoch(crs, epoch)?;
    let mut rows = crate::array::StreamingShapes::new();
    for item_value in values.try_iter()? {
        rows.try_push(item(&item_value?)?)?;
    }
    Ok(rows.finish(frame))
}

/// Build a ``GeometryCollection`` from a sequence of geometries.
pub(crate) fn build_geometry_collection(
    geometries: Option<&Bound<'_, PyAny>>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeometry> {
    // No members → `GEOMETRYCOLLECTION EMPTY` (Shapely-compatible
    // `GeometryCollection()`), carrying any explicit crs/epoch.
    let Some(geometries) = geometries else {
        return geometry_with_crs_epoch(empty_geometry_collection(), crs, epoch);
    };
    // Fallible growth (D10): `GeometryCollection(itertools.repeat(Point))`
    // must MemoryError on real fallible OOM, never hang/abort.
    // Fail-fast on the first missing member: GeometryCollection cannot carry
    // None (unlike GeometryArray rows).
    let mut row = 0_usize;
    let mut members = crate::collect_py_iter(geometries, |item| {
        let this_row = row;
        row += 1;
        if item.is_none() {
            return Err(crate::missing_geometry_items_error());
        }
        let Some(geometry) = exact_geometry(&item) else {
            return Err(PyTypeError::new_err(format!(
                "expected Geometry at element {this_row}; strings parse via from_wkt/from_wkb/from_geojson"
            )));
        };
        Ok(geometry.clone())
    })?;
    let explicit_crs = parse_crs(crs)?;
    let explicit_epoch = coordinate_epoch_option("epoch", epoch)?;
    let frame = Frame::resolve_items(
        &mut members,
        FrameAdoption {
            crs: explicit_crs,
            epoch: explicit_epoch,
        },
        "GeometryCollection",
    )?;
    let items = members
        .into_iter()
        .map(
            |geometry| match std::sync::Arc::try_unwrap(geometry.shape) {
                Ok(data) => data.into_shape(),
                Err(shared) => shared.shape().clone(),
            },
        )
        .collect::<Vec<_>>();
    Ok(PyGeometry::with_epoch(
        Shape::GeometryCollection(items),
        frame.crs_owned(),
        frame.epoch(),
    ))
}
