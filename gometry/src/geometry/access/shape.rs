mod bounds;
mod ordinates;
mod owners;
mod parts;
mod points;
mod runs;
pub(crate) use ordinates::shape_axes_all_z;
pub(crate) use owners::{RunIdentity, SharedShapeOwners, SupportRunKind};
pub(crate) use points::PointSet;
pub(crate) use runs::{
    Component, CoordRun, Lineal, MetricSupportRun, Polygonal, linework_first_point,
};
#[cfg(test)]
mod tests;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::hash::Hash;
use std::ops::ControlFlow;

use crate::error::Result;
use crate::geometry::access::{bounds_from_iter, canonicalize_zero, column_minmax, line_length};
use crate::geometry::{
    Bounds, CoordSeq, CoordinateAxes, Coordinates as _, Dimension, GeometryErrorKind, GeometryKind,
    HasM, HasZ, LineSeq, MultiLineString, MultiMember, MultiPoint, MultiPolygon, Point, PointKey,
    Polygon, Shape, ShapeCountQuery, ShapeOwnerKey, VisiblePointRunKey, VisibleXYKey,
    line_length_3d,
};

impl Shape {
    pub fn area(&self) -> f64 {
        canonicalize_zero(match self {
            Self::Polygon(polygon) => polygon.area(),
            Self::MultiPolygon(polygons) => polygons.iter().map(Polygon::area).sum(),
            Self::GeometryCollection(geometries) => geometries.iter().map(Self::area).sum(),
            Self::Point(_)
            | Self::MultiPoint(_)
            | Self::LineString(_)
            | Self::MultiLineString(_)
            | Self::Empty(..) => 0.0,
        })
    }

    pub fn length(&self) -> f64 {
        canonicalize_zero(match self {
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => 0.0,
            Self::LineString(points) => line_length(points),
            Self::MultiLineString(lines) => lines.iter().map(|line| line_length(line)).sum(),
            Self::Polygon(polygon) => polygon.length(),
            Self::MultiPolygon(polygons) => polygons.iter().map(Polygon::length).sum(),
            Self::GeometryCollection(geometries) => geometries.iter().map(Self::length).sum(),
        })
    }

    /// Total 3D (Euclidean) length, folding `sqrt(dx² + dy² + dz²)` over every
    /// segment. Every vertex that participates in a segment must carry a Z
    /// ordinate; otherwise [`GeometryErrorKind::MissingZ`] is returned rather than
    /// silently treating the missing elevation as 0.
    pub fn length_3d(&self) -> Result<f64> {
        match self {
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => Ok(0.0),
            Self::LineString(points) => line_length_3d(points),
            Self::MultiLineString(lines) => lines.iter().map(line_length_3d).sum(),
            Self::Polygon(polygon) => polygon.rings().map(|ring| line_length_3d(&ring)).sum(),
            Self::MultiPolygon(polygons) => polygons
                .iter()
                .flat_map(Polygon::rings)
                .map(|ring| line_length_3d(&ring))
                .sum(),
            Self::GeometryCollection(geometries) => geometries.iter().map(Self::length_3d).sum(),
        }
        .map(canonicalize_zero)
    }
}
