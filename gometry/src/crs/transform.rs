#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! The public coordinate-transform API (`transform`/`transform_coordinates`/
//! `transform_bounds`/`transform_bounds_3d`).
//!
//! These take `&Shape`/`&str`/`&[f64]` and drive the FFI pipeline engine and
//! raw-pointer helpers (which stay private in the parent `crs` module) via
//! `use super::*`; re-exported at `crs` so `crs::transform` callers resolve.

use crate::crs::{
    CoordSeq, CrsError, GeometryErrorKind, Point, ProjDirection, ProjPipeline, Shape,
    TRANSFORM_BOUNDS_MAX_DENSIFY, TransformOptions, Transformer, coordinate_identity_crs, info,
    normalize_pair, proj_error_message, with_proj_operation, with_proj_pipeline,
};
use crate::error::Result;
use crate::geometry::{LineSeq, MOrdinate, ZOrdinate};

/// One transformed 3D bounds row: (minx, miny, minz, maxx, maxy, maxz).
pub(crate) type TransformedBounds3d = (f64, f64, f64, f64, f64, f64);

pub(crate) fn transform(shape: &Shape, source: &str, target: &str) -> Result<Shape> {
    Transformer::new(source, target).transform_shape(shape)
}

pub(crate) fn transform_coordinates(
    source: &str,
    target: &str,
    x: &mut [f64],
    y: &mut [f64],
    zt: crate::ZtLanes<'_>,
    options: TransformOptions,
) -> Result<()> {
    Transformer::new_with_options(source, target, options).transform_coordinates(x, y, zt)
}

/// Shared densify gate for the bounds transforms: capped, and at least 2 when
/// the output CRS is geographic (PROJ needs edge samples to track the
/// antimeridian).
fn validate_bounds_densify(target: &str, densify: u32) -> Result<()> {
    if densify > TRANSFORM_BOUNDS_MAX_DENSIFY {
        return Err(CrsError::invalid(format!(
            "bounds densify must be <= {TRANSFORM_BOUNDS_MAX_DENSIFY}"
        )));
    }
    if densify < 2 && is_geographic_crs(target)? {
        return Err(CrsError::invalid(
            "bounds densify must be at least 2 when the output CRS is geographic".to_owned(),
        ));
    }
    Ok(())
}

trait BoundsTransform: Copy {
    fn validate(source: &str, bounds: Self) -> Result<()>;
    fn crosses_antimeridian(bounds: Self) -> bool;
    fn transform_one(transformer: &Transformer, bounds: Self, densify: u32) -> Result<Self>;
    fn transform_one_proj(transformer: &ProjPipeline, bounds: Self, densify: u32) -> Result<Self>;
    fn transform_many(transformer: &Transformer, rows: &[Self], densify: u32) -> Result<Vec<Self>>;
}

impl BoundsTransform for (f64, f64, f64, f64) {
    fn validate(source: &str, bounds: Self) -> Result<()> {
        validate_transform_bounds(source, bounds)
    }

    fn crosses_antimeridian(bounds: Self) -> bool {
        bounds.0 > bounds.2
    }

    fn transform_one(transformer: &Transformer, bounds: Self, densify: u32) -> Result<Self> {
        transformer.transform_bounds(bounds, densify)
    }

    fn transform_one_proj(transformer: &ProjPipeline, bounds: Self, densify: u32) -> Result<Self> {
        transformer.transform_bounds(bounds, densify)
    }

    fn transform_many(transformer: &Transformer, rows: &[Self], densify: u32) -> Result<Vec<Self>> {
        transformer.transform_bounds_many(rows, densify)
    }
}

impl BoundsTransform for TransformedBounds3d {
    fn validate(source: &str, bounds: Self) -> Result<()> {
        validate_transform_bounds_3d(source, bounds)
    }

    fn crosses_antimeridian(bounds: Self) -> bool {
        bounds.0 > bounds.3
    }

    fn transform_one(transformer: &Transformer, bounds: Self, densify: u32) -> Result<Self> {
        transformer.transform_bounds_3d(bounds, densify)
    }

    fn transform_one_proj(transformer: &ProjPipeline, bounds: Self, densify: u32) -> Result<Self> {
        transformer.transform_bounds_3d(bounds, densify)
    }

    fn transform_many(transformer: &Transformer, rows: &[Self], densify: u32) -> Result<Vec<Self>> {
        transformer.transform_bounds_3d_many(rows, densify)
    }
}

fn transform_bounds_row<B: BoundsTransform>(
    source: &str,
    target: &str,
    bounds: B,
    densify: u32,
    options: TransformOptions,
) -> Result<B> {
    B::validate(source, bounds)?;
    reject_bounds_epochs(&options)?;
    if coordinate_identity_crs(source, target) {
        return Ok(bounds);
    }
    validate_bounds_densify(target, densify)?;
    if B::crosses_antimeridian(bounds) {
        let (source, target) = normalize_pair(source, target)?;
        return with_proj_pipeline(&source, &target, &options, |transformer| {
            B::transform_one_proj(transformer, bounds, densify)
        });
    }
    B::transform_one(
        &Transformer::new_with_options(source, target, options),
        bounds,
        densify,
    )
}

fn transform_bounds_rows<B: BoundsTransform>(
    source: &str,
    target: &str,
    rows: &[B],
    densify: u32,
    options: TransformOptions,
) -> Result<Vec<B>> {
    for &bounds in rows {
        B::validate(source, bounds)?;
    }
    reject_bounds_epochs(&options)?;
    if coordinate_identity_crs(source, target) {
        return Ok(rows.to_vec());
    }
    validate_bounds_densify(target, densify)?;
    if rows.iter().any(|&bounds| B::crosses_antimeridian(bounds)) {
        return rows
            .iter()
            .map(|&bounds| transform_bounds_row(source, target, bounds, densify, options.clone()))
            .collect();
    }
    B::transform_many(
        &Transformer::new_with_options(source, target, options),
        rows,
        densify,
    )
}

fn reject_bounds_epochs(options: &TransformOptions) -> Result<()> {
    if options.source_epoch.is_some() || options.target_epoch.is_some() {
        return Err(CrsError::message(
            "bounds transform does not support coordinate epochs until time-aware bounds are implemented",
        ));
    }
    Ok(())
}

pub(crate) fn transform_bounds(
    source: &str,
    target: &str,
    bounds: (f64, f64, f64, f64),
    densify: u32,
    options: TransformOptions,
) -> Result<(f64, f64, f64, f64)> {
    transform_bounds_row(source, target, bounds, densify, options)
}

pub(crate) fn transform_bounds_many(
    source: &str,
    target: &str,
    rows: &[(f64, f64, f64, f64)],
    densify: u32,
    options: TransformOptions,
) -> Result<Vec<(f64, f64, f64, f64)>> {
    transform_bounds_rows(source, target, rows, densify, options)
}

pub(crate) fn transform_bounds_3d(
    source: &str,
    target: &str,
    bounds: (f64, f64, f64, f64, f64, f64),
    densify: u32,
    options: TransformOptions,
) -> Result<(f64, f64, f64, f64, f64, f64)> {
    transform_bounds_row(source, target, bounds, densify, options)
}

pub(crate) fn transform_bounds_3d_many(
    source: &str,
    target: &str,
    rows: &[(f64, f64, f64, f64, f64, f64)],
    densify: u32,
    options: TransformOptions,
) -> Result<Vec<TransformedBounds3d>> {
    transform_bounds_rows(source, target, rows, densify, options)
}

pub(crate) fn apply_operation(
    definition: &str,
    direction: ProjDirection,
    x: &mut [f64],
    y: &mut [f64],
    zt: crate::ZtLanes<'_>,
) -> Result<()> {
    let definition = definition.trim();
    if definition.is_empty() {
        return Err(CrsError::invalid(
            "operation definition is required".to_owned(),
        ));
    }
    with_proj_operation(definition, |operation| {
        transform_coordinates_with_pipeline(operation, direction, x, y, zt)
    })
}

pub(crate) fn parse_epsg(value: &str) -> Option<u32> {
    let (authority, code) = value.split_once(':')?;
    authority
        .eq_ignore_ascii_case("EPSG")
        .then(|| code.parse().ok())
        .flatten()
}

pub(crate) fn is_wgs84_lonlat(value: &str) -> bool {
    matches!(parse_epsg(value), Some(4326 | 4979))
}

pub(crate) fn validate_coordinate_slices(
    x: &[f64],
    y: &[f64],
    z: Option<&[f64]>,
    t: Option<&[f64]>,
) -> Result<()> {
    if let Some(other) = [Some(y.len()), z.map(<[f64]>::len), t.map(<[f64]>::len)]
        .into_iter()
        .flatten()
        .find(|&len| len != x.len())
    {
        return Err(GeometryErrorKind::CoordinateLength(x.len(), other).into());
    }
    if crate::geometry::column_all_finite(x)
        && crate::geometry::column_all_finite(y)
        && z.is_none_or(crate::geometry::column_all_finite)
        && t.is_none_or(crate::geometry::column_all_finite)
    {
        Ok(())
    } else {
        Err(GeometryErrorKind::NonFiniteCoordinate.into())
    }
}

pub(crate) fn validate_coordinate_lanes(
    x: &[f64],
    y: &[f64],
    zt: crate::ZtLaneRefs<'_>,
) -> Result<()> {
    match zt {
        crate::Zt::None => validate_coordinate_slices(x, y, None, None),
        crate::Zt::Z(z) => validate_coordinate_slices(x, y, Some(z), None),
        crate::Zt::T(t) => validate_coordinate_slices(x, y, None, Some(t)),
        crate::Zt::Zt { z, t } => validate_coordinate_slices(x, y, Some(z), Some(t)),
    }
}

pub(crate) fn coordinates_are_finite(
    x: &[f64],
    y: &[f64],
    z: Option<&[f64]>,
    t: Option<&[f64]>,
) -> bool {
    crate::geometry::column_all_finite(x)
        && crate::geometry::column_all_finite(y)
        && z.is_none_or(crate::geometry::column_all_finite)
        && t.is_none_or(crate::geometry::column_all_finite)
}

pub(crate) fn proj_transform_error(error: i32) -> Box<str> {
    if error == 0 {
        "projection returned a non-finite coordinate".into()
    } else {
        proj_error_message(error).into()
    }
}

pub(crate) const BOUNDS_2D_ERROR: &str =
    "bounds must be finite (minx, miny, maxx, maxy) with min <= max";
pub(crate) const BOUNDS_2D_GEO_ERROR: &str = "bounds must be finite (minx, miny, maxx, maxy) with min <= max, except antimeridian-crossing geographic bounds may use west > east";
pub(crate) const BOUNDS_3D_ERROR: &str =
    "bounds must be finite (minx, miny, minz, maxx, maxy, maxz) with min <= max";
pub(crate) const BOUNDS_3D_GEO_ERROR: &str = "bounds must be finite (minx, miny, minz, maxx, maxy, maxz) with min <= max, except antimeridian-crossing geographic bounds may use west > east";

pub(crate) fn all_finite(values: &[f64]) -> bool {
    values.iter().all(|value| value.is_finite())
}

pub(crate) fn validate_bounds(bounds: (f64, f64, f64, f64)) -> Result<()> {
    if all_finite(&<[f64; 4]>::from(bounds)) && bounds.0 <= bounds.2 && bounds.1 <= bounds.3 {
        return Ok(());
    }
    Err(CrsError::invalid(BOUNDS_2D_ERROR.to_owned()))
}

pub(crate) fn validate_transform_bounds(source: &str, bounds: (f64, f64, f64, f64)) -> Result<()> {
    if is_geographic_crs(source)? {
        if all_finite(&<[f64; 4]>::from(bounds))
            && (-180.0..=180.0).contains(&bounds.0)
            && (-180.0..=180.0).contains(&bounds.2)
            && (-90.0..=90.0).contains(&bounds.1)
            && (-90.0..=90.0).contains(&bounds.3)
            && bounds.1 <= bounds.3
        {
            return Ok(());
        }
    } else if bounds.0 <= bounds.2 {
        return validate_bounds(bounds);
    }
    Err(CrsError::invalid(BOUNDS_2D_GEO_ERROR.to_owned()))
}

pub(crate) fn validate_bounds_3d(bounds: (f64, f64, f64, f64, f64, f64)) -> Result<()> {
    if all_finite(&<[f64; 6]>::from(bounds))
        && bounds.0 <= bounds.3
        && bounds.1 <= bounds.4
        && bounds.2 <= bounds.5
    {
        return Ok(());
    }
    Err(CrsError::invalid(BOUNDS_3D_ERROR.to_owned()))
}

pub(crate) fn validate_transform_bounds_3d(
    source: &str,
    bounds: (f64, f64, f64, f64, f64, f64),
) -> Result<()> {
    if is_geographic_crs(source)? {
        if all_finite(&<[f64; 6]>::from(bounds))
            && (-180.0..=180.0).contains(&bounds.0)
            && (-180.0..=180.0).contains(&bounds.3)
            && (-90.0..=90.0).contains(&bounds.1)
            && (-90.0..=90.0).contains(&bounds.4)
            && bounds.1 <= bounds.4
            && bounds.2 <= bounds.5
        {
            return Ok(());
        }
    } else if bounds.0 <= bounds.3 {
        return validate_bounds_3d(bounds);
    }
    Err(CrsError::invalid(BOUNDS_3D_GEO_ERROR.to_owned()))
}

/// Whether `value` is geographic — including compound CRSs whose horizontal
/// component is geographic (e.g. EPSG:9707). Uses the same recursive kind
/// walk as [`CrsInfo::is_geographic`].
pub(crate) fn is_geographic_crs(value: &str) -> Result<bool> {
    Ok(info(value)?.is_geographic())
}

pub(crate) fn transform_coordinates_with_pipeline(
    transformer: &ProjPipeline,
    direction: ProjDirection,
    x: &mut [f64],
    y: &mut [f64],
    zt: crate::ZtLanes<'_>,
) -> Result<()> {
    match zt {
        crate::Zt::None => {
            validate_coordinate_lanes(x, y, crate::Zt::None)?;
            transformer.transform_lanes(x, y, None, None, direction)
        },
        crate::Zt::Z(z) => {
            validate_coordinate_lanes(x, y, crate::Zt::Z(&*z))?;
            transformer.transform_lanes(x, y, Some(z), None, direction)
        },
        crate::Zt::T(t) => {
            validate_coordinate_lanes(x, y, crate::Zt::T(&*t))?;
            transformer.transform_lanes(x, y, None, Some(t), direction)
        },
        crate::Zt::Zt { z, t } => {
            validate_coordinate_lanes(x, y, crate::Zt::Zt { z: &*z, t: &*t })?;
            transformer.transform_lanes(x, y, Some(z), Some(t), direction)
        },
    }
}

pub(crate) fn transform_proj_shapes(
    transformer: &ProjPipeline,
    shapes: &[&Shape],
    source_epoch: Option<f64>,
) -> Result<Vec<Shape>> {
    let mut scratch = TransformScratch::new();
    for shape in shapes {
        scratch.extend_shape(shape);
    }
    scratch.transform(transformer, source_epoch)?;
    let (xy_len, xyz_len) = (scratch.xy.xs.len(), scratch.xyz.xs.len());
    let mut cursor = scratch.cursor();
    let rebuilt = shapes
        .iter()
        .map(|shape| rebuild_shape_columns(shape, &mut cursor))
        .collect();
    debug_assert_eq!(cursor.xy_offset, xy_len);
    debug_assert_eq!(cursor.xyz_offset, xyz_len);
    Ok(rebuilt)
}

/// One gathered ordinate batch for the transform. Same-axes coordinates from
/// every input shape are packed contiguously so PROJ runs at most once per
/// batch and the transformed columns feed the rebuild directly — no
/// original-order scatter buffer, and no Z lane allocated for XY-only input.
struct TransformLaneBatch {
    xs: Vec<f64>,
    ys: Vec<f64>,
}

struct TransformZLaneBatch {
    xs: Vec<f64>,
    ys: Vec<f64>,
    zs: Vec<f64>,
}

/// The two gathered lane batches (XY and XYZ). Each input sequence is
/// homogeneous, so it lands wholly in one batch; the rebuild walks the shapes
/// in the same order and pulls each sequence's transformed columns straight
/// from its batch via the paired cursor offsets.
struct TransformScratch {
    xy: TransformLaneBatch,
    xyz: TransformZLaneBatch,
}

impl TransformScratch {
    const fn new() -> Self {
        Self {
            xy: TransformLaneBatch {
                xs: Vec::new(),
                ys: Vec::new(),
            },
            xyz: TransformZLaneBatch {
                xs: Vec::new(),
                ys: Vec::new(),
                zs: Vec::new(),
            },
        }
    }

    fn extend_shape(&mut self, shape: &Shape) {
        match shape {
            Shape::Point(point) => self.extend_point(point),
            Shape::MultiPoint(points) => self.extend_seq(points),
            Shape::LineString(points) => self.extend_seq(points),
            Shape::MultiLineString(lines) => {
                for line in lines {
                    self.extend_seq(line);
                }
            },
            Shape::Polygon(polygon) => {
                for ring in polygon.rings() {
                    self.extend_seq(ring);
                }
            },
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons {
                    for ring in polygon.rings() {
                        self.extend_seq(ring);
                    }
                }
            },
            Shape::GeometryCollection(geometries) => {
                for geometry in geometries {
                    self.extend_shape(geometry);
                }
            },
            Shape::Empty(..) => {},
        }
    }

    fn extend_point(&mut self, point: &Point) {
        if let Some(z) = point.z() {
            self.xyz.xs.push(point.x);
            self.xyz.ys.push(point.y);
            self.xyz.zs.push(z);
        } else {
            self.xy.xs.push(point.x);
            self.xy.ys.push(point.y);
        }
    }

    fn extend_seq(&mut self, seq: &CoordSeq) {
        if seq.is_empty() {
            return;
        }
        if let Some(zs) = seq.zs() {
            self.xyz.xs.extend_from_slice(seq.xs());
            self.xyz.ys.extend_from_slice(seq.ys());
            self.xyz.zs.extend_from_slice(zs);
        } else {
            self.xy.xs.extend_from_slice(seq.xs());
            self.xy.ys.extend_from_slice(seq.ys());
        }
    }

    fn transform(&mut self, transformer: &ProjPipeline, source_epoch: Option<f64>) -> Result<()> {
        if !self.xy.xs.is_empty() {
            let mut epoch = broadcast_epoch(transformer, source_epoch)?;
            transformer.transform_lanes(
                &mut self.xy.xs,
                &mut self.xy.ys,
                None,
                epoch.as_mut().map(<[f64; 1]>::as_mut_slice),
                ProjDirection::Forward,
            )?;
        }
        if !self.xyz.xs.is_empty() {
            let mut epoch = broadcast_epoch(transformer, source_epoch)?;
            transformer.transform_lanes(
                &mut self.xyz.xs,
                &mut self.xyz.ys,
                Some(&mut self.xyz.zs),
                epoch.as_mut().map(<[f64; 1]>::as_mut_slice),
                ProjDirection::Forward,
            )?;
        }
        Ok(())
    }

    fn cursor(&self) -> TransformCursor<'_> {
        TransformCursor {
            xy_xs: &self.xy.xs,
            xy_ys: &self.xy.ys,
            xy_offset: 0,
            xyz_xs: &self.xyz.xs,
            xyz_ys: &self.xyz.ys,
            xyz_zs: &self.xyz.zs,
            xyz_offset: 0,
        }
    }
}

/// The single-element broadcast epoch lane for a PROJ transform, or `None`
/// when the source carries no coordinate epoch (raising early when the
/// operation nonetheless requires one). PROJ treats a length-1 time array as
/// a constant broadcast across every coordinate, so a dynamic-datum transform
/// needs no per-vertex epoch buffer (see `transform_lanes`).
fn broadcast_epoch(
    transformer: &ProjPipeline,
    source_epoch: Option<f64>,
) -> Result<Option<[f64; 1]>> {
    if let Some(epoch) = source_epoch {
        Ok(Some([epoch]))
    } else {
        transformer.require_epoch_lane()?;
        Ok(None)
    }
}

/// Read cursor over the two transformed batches: each shape consumes exactly
/// the slices it contributed, in order, from the XY or XYZ batch matching its
/// axes.
struct TransformCursor<'a> {
    xy_xs: &'a [f64],
    xy_ys: &'a [f64],
    xy_offset: usize,
    xyz_xs: &'a [f64],
    xyz_ys: &'a [f64],
    xyz_zs: &'a [f64],
    xyz_offset: usize,
}

struct TransformColumns<'a> {
    xs: &'a [f64],
    ys: &'a [f64],
    zs: Option<&'a [f64]>,
}

impl<'a> TransformCursor<'a> {
    /// Take the next `len` XY coordinates from the XY batch.
    fn take_xy(&mut self, len: usize) -> TransformColumns<'a> {
        let range = self.xy_offset..self.xy_offset + len;
        self.xy_offset = range.end;
        TransformColumns {
            xs: &self.xy_xs[range.clone()],
            ys: &self.xy_ys[range],
            zs: None,
        }
    }

    /// Take the next `len` XYZ coordinates from the XYZ batch.
    fn take_xyz(&mut self, len: usize) -> TransformColumns<'a> {
        let range = self.xyz_offset..self.xyz_offset + len;
        self.xyz_offset = range.end;
        TransformColumns {
            xs: &self.xyz_xs[range.clone()],
            ys: &self.xyz_ys[range.clone()],
            zs: Some(&self.xyz_zs[range]),
        }
    }
}

fn rebuild_point_columns(point: &Point, cursor: &mut TransformCursor<'_>) -> Point {
    let cols = if point.z().is_some() {
        cursor.take_xyz(1)
    } else {
        cursor.take_xy(1)
    };
    Point::new_unchecked_axes(
        cols.xs[0],
        cols.ys[0],
        ZOrdinate(cols.zs.map(|zs| zs[0])),
        MOrdinate(point.m()),
    )
}

/// Rebuild one shape from the transformed batches — original axes are
/// preserved (2D stays 2D; 3D gets the transformed Z lane). Infallible by
/// construction.
fn rebuild_shape_columns(shape: &Shape, cursor: &mut TransformCursor<'_>) -> Shape {
    match shape {
        Shape::Point(point) => Shape::Point(rebuild_point_columns(point, cursor)),
        Shape::MultiPoint(points) => Shape::MultiPoint(rebuild_seq_columns(points, cursor)),
        Shape::LineString(points) => Shape::LineString(
            LineSeq::try_new(rebuild_seq_columns(points, cursor))
                .expect("transformed line remains empty or >=2 vertices"),
        ),
        Shape::MultiLineString(lines) => Shape::MultiLineString(
            lines
                .iter()
                .map(|line| {
                    LineSeq::try_new(rebuild_seq_columns(line, cursor))
                        .expect("transformed line remains empty or >=2 vertices")
                })
                .collect(),
        ),
        Shape::Polygon(polygon) => Shape::Polygon(rebuild_polygon_columns(polygon, cursor)),
        Shape::MultiPolygon(polygons) => Shape::MultiPolygon(
            polygons
                .iter()
                .map(|polygon| rebuild_polygon_columns(polygon, cursor))
                .collect(),
        ),
        Shape::GeometryCollection(geometries) => Shape::GeometryCollection(
            geometries
                .iter()
                .map(|geometry| rebuild_shape_columns(geometry, cursor))
                .collect(),
        ),
        Shape::Empty(..) => shape.clone(),
    }
}

fn rebuild_polygon_columns(
    polygon: &crate::geometry::Polygon,
    cursor: &mut TransformCursor<'_>,
) -> crate::geometry::Polygon {
    use crate::geometry::Ring;
    crate::geometry::Polygon {
        // CRS rebuild only rewrites coordinates, preserving ring structure.
        shell: Ring::from_trusted_closed(rebuild_seq_columns(polygon.shell.coords(), cursor)),
        holes: polygon
            .holes
            .iter()
            .map(|hole| Ring::from_trusted_closed(rebuild_seq_columns(hole.coords(), cursor)))
            .collect(),
    }
}

fn rebuild_seq_columns(seq: &CoordSeq, cursor: &mut TransformCursor<'_>) -> CoordSeq {
    let cols = if seq.zs().is_some() {
        cursor.take_xyz(seq.len())
    } else {
        cursor.take_xy(seq.len())
    };
    CoordSeq::copy_from_trusted_columns(cols.xs, cols.ys, cols.zs, seq.ms())
}
