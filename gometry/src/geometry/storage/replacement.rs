//! Coordinate replacement payload shared by array packed/mixed paths and the
//! `Coordinates` setter surface.

use std::cell::Cell;

use crate::error::Result;
use crate::geometry::storage::packed_columns::validate_replacement_ring;
use crate::geometry::{
    CoordSeq, CoordinateAxes, GeometryErrorKind, MColumn, MOrdinate, Point, Shape, SharedSlice,
    ZOrdinate,
};
#[cfg(test)]
use crate::test_support::boundary_probe;

#[derive(Clone)]
pub(crate) enum ReplacementAxis {
    Replace(SharedSlice<f64>),
    Carry,
    Drop,
}

#[derive(Clone)]
pub(crate) struct CoordinateReplacement {
    pub(crate) xs: Option<SharedSlice<f64>>,
    pub(crate) ys: Option<SharedSlice<f64>>,
    pub(crate) zs: ReplacementAxis,
    pub(crate) ms: ReplacementAxis,
    pub(crate) len: usize,
    pub(crate) axes: CoordinateAxes,
    pub(crate) positional: bool,
}

fn replacement_seq(
    old: &CoordSeq,
    replacement: &CoordinateReplacement,
    cursor: &Cell<usize>,
) -> Result<CoordSeq> {
    // Union-layout matrices preserve each sequence's axes; padded columns do
    // not add an ordinate to a member that lacked it.
    if replacement.positional
        && ((old.axes().has_z() && !replacement.axes.has_z())
            || (old.axes().has_m() && !replacement.axes.has_m()))
    {
        return Err(GeometryErrorKind::message(
            "coordinates must preserve each coordinate sequence axes",
        ));
    }
    let start = cursor.get();
    let end = start.wrapping_add(old.len());
    cursor.set(end);
    CoordSeq::try_from_vecs(
        replacement
            .x_column_for_seq(old, start..end)
            .as_slice()
            .to_vec(),
        replacement
            .y_column_for_seq(old, start..end)
            .as_slice()
            .to_vec(),
        replacement
            .z_column_for_seq(old, start..end)
            .map(|values| values.as_slice().to_vec()),
        replacement
            .m_column_for_seq(old, start..end)
            .map(|values| values.as_slice().to_vec()),
    )
}

fn replacement_point(
    old: &Point,
    replacement: &CoordinateReplacement,
    cursor: &Cell<usize>,
) -> Result<Point> {
    if replacement.positional
        && ((old.axes.has_z() && !replacement.axes.has_z())
            || (old.axes.has_m() && !replacement.axes.has_m()))
    {
        return Err(GeometryErrorKind::message(
            "coordinates must preserve each coordinate sequence axes",
        ));
    }
    let index = cursor.get();
    cursor.set(index.wrapping_add(1));
    Point::new_axes(
        replacement.x_at(*old, index),
        replacement.y_at(*old, index),
        ZOrdinate(replacement.z_at(*old, index)),
        MOrdinate(replacement.m_at(*old, index)),
    )
}

fn validate_shape_rings(shape: &Shape) -> Result<()> {
    match shape {
        Shape::Polygon(polygon) => {
            for ring in polygon.rings() {
                validate_replacement_ring(ring, 0..ring.len())?;
            }
        },
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons {
                for ring in polygon.rings() {
                    validate_replacement_ring(ring, 0..ring.len())?;
                }
            }
        },
        Shape::GeometryCollection(items) => {
            for item in items {
                validate_shape_rings(item)?;
            }
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Empty(..) => {},
    }
    Ok(())
}

pub(crate) fn replace_shape_coordinates(
    shape: &Shape,
    replacement: &CoordinateReplacement,
) -> Result<Shape> {
    #[cfg(test)]
    boundary_probe::record("shape replacement");
    let cursor = Cell::new(0);
    let mut replaced = shape.try_map_coordseqs(
        |seq| replacement_seq(seq, replacement, &cursor),
        |point| replacement_point(point, replacement, &cursor),
    )?;
    if cursor.get() != replacement.len {
        return Err(GeometryErrorKind::message(format!(
            "coordinates must have length {}; got length {}",
            replacement.len,
            cursor.get(),
        )));
    }
    let replaces_axis = !matches!(&replacement.zs, ReplacementAxis::Carry)
        || !matches!(&replacement.ms, ReplacementAxis::Carry);
    if replaces_axis && has_empty_axis_change(&replaced, replacement) {
        replaced = retag_empty_axes(&replaced, replacement)?;
    }
    validate_shape_rings(&replaced)?;
    #[cfg(test)]
    boundary_probe::record("shape replacement finished");
    Ok(replaced)
}

fn has_empty_axis_change(shape: &Shape, replacement: &CoordinateReplacement) -> bool {
    match shape {
        Shape::Empty(_, axes) => replacement.output_axes(*axes) != *axes,
        Shape::GeometryCollection(geometries) => geometries
            .iter()
            .any(|geometry| has_empty_axis_change(geometry, replacement)),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_) => false,
    }
}

fn retag_empty_axes(shape: &Shape, replacement: &CoordinateReplacement) -> Result<Shape> {
    match shape {
        Shape::Empty(kind, axes) => Ok(Shape::typed_empty(*kind, replacement.output_axes(*axes))),
        Shape::GeometryCollection(geometries) => Ok(Shape::GeometryCollection(
            geometries.try_map_members(|geometry| retag_empty_axes(geometry, replacement))?,
        )),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_) => Ok(shape.clone()),
    }
}

pub(crate) fn slice_replacement_for_shape(
    replacement: &CoordinateReplacement,
    start: usize,
    len: usize,
) -> CoordinateReplacement {
    let end = start.wrapping_add(len);
    let slice = |values: &SharedSlice<f64>| {
        SharedSlice::from_vec(
            values
                .get(start..end)
                .expect("replacement range is bounded by admitted coordinates")
                .to_vec(),
        )
    };
    let axis = |values: &ReplacementAxis| match values {
        ReplacementAxis::Replace(values) => ReplacementAxis::Replace(slice(values)),
        ReplacementAxis::Carry => ReplacementAxis::Carry,
        ReplacementAxis::Drop => ReplacementAxis::Drop,
    };
    CoordinateReplacement {
        xs: replacement.xs.as_ref().map(slice),
        ys: replacement.ys.as_ref().map(slice),
        zs: axis(&replacement.zs),
        ms: axis(&replacement.ms),
        len,
        axes: replacement.axes,
        positional: replacement.positional,
    }
}

impl CoordinateReplacement {
    pub(crate) const fn output_axes(&self, input: CoordinateAxes) -> CoordinateAxes {
        let has_z = match &self.zs {
            ReplacementAxis::Replace(_) => true,
            ReplacementAxis::Carry => input.has_z(),
            ReplacementAxis::Drop => false,
        };
        let has_m = match &self.ms {
            ReplacementAxis::Replace(_) => true,
            ReplacementAxis::Carry => input.has_m(),
            ReplacementAxis::Drop => false,
        };
        input.with_axes(has_z, has_m)
    }

    /// Positional matrices use the geometry's *union* axes (NaN-padded for
    /// members that lack Z/M). Each independent sequence keeps its own axes:
    /// ignore padded columns rather than fabricating or rejecting them.
    const fn ignore_union_padding_for_missing_axis(&self, member_has_axis: bool) -> bool {
        self.positional && !member_has_axis
    }

    fn axis_column_for_seq(
        &self,
        axis: &ReplacementAxis,
        member_has_axis: bool,
        carried: impl FnOnce() -> Option<SharedSlice<f64>>,
        range: std::ops::Range<usize>,
    ) -> Option<SharedSlice<f64>> {
        match axis {
            ReplacementAxis::Replace(values) => {
                if self.ignore_union_padding_for_missing_axis(member_has_axis) {
                    return None;
                }
                Some(SharedSlice::from_vec(
                    values
                        .get(range)
                        .expect("replacement range is bounded by its admitted coordinate count")
                        .to_vec(),
                ))
            },
            ReplacementAxis::Carry => carried(),
            ReplacementAxis::Drop => None,
        }
    }

    pub(crate) fn x_column_for_seq(
        &self,
        seq: &CoordSeq,
        range: std::ops::Range<usize>,
    ) -> SharedSlice<f64> {
        self.xy_column_for_seq(self.xs.as_ref(), seq.carried_xs(), range)
    }

    pub(crate) fn y_column_for_seq(
        &self,
        seq: &CoordSeq,
        range: std::ops::Range<usize>,
    ) -> SharedSlice<f64> {
        self.xy_column_for_seq(self.ys.as_ref(), seq.carried_ys(), range)
    }

    fn xy_column_for_seq(
        &self,
        replacement: Option<&SharedSlice<f64>>,
        carried: SharedSlice<f64>,
        range: std::ops::Range<usize>,
    ) -> SharedSlice<f64> {
        replacement.map_or(carried, |values| {
            SharedSlice::from_vec(
                values
                    .get(range)
                    .expect("replacement range is bounded by its admitted coordinate count")
                    .to_vec(),
            )
        })
    }

    pub(crate) fn x_at(&self, old: Point, index: usize) -> f64 {
        self.xs.as_ref().map_or(old.x, |values| {
            *values
                .get(index)
                .expect("replacement index is bounded by its admitted coordinate count")
        })
    }

    pub(crate) fn y_at(&self, old: Point, index: usize) -> f64 {
        self.ys.as_ref().map_or(old.y, |values| {
            *values
                .get(index)
                .expect("replacement index is bounded by its admitted coordinate count")
        })
    }

    pub(crate) fn z_column_for_seq(
        &self,
        seq: &CoordSeq,
        range: std::ops::Range<usize>,
    ) -> Option<SharedSlice<f64>> {
        self.axis_column_for_seq(&self.zs, seq.axes().has_z(), || seq.carried_zs(), range)
    }

    pub(crate) fn m_column_for_seq(
        &self,
        seq: &CoordSeq,
        range: std::ops::Range<usize>,
    ) -> Option<SharedSlice<f64>> {
        self.axis_column_for_seq(
            &self.ms,
            seq.axes().has_m(),
            || {
                seq.ms()
                    .map(|values| SharedSlice::from_vec(values.to_vec()))
            },
            range,
        )
    }

    fn axis_at(
        &self,
        axis: &ReplacementAxis,
        member_has_axis: bool,
        carried: Option<f64>,
        index: usize,
    ) -> Option<f64> {
        match axis {
            ReplacementAxis::Replace(values) => {
                if self.ignore_union_padding_for_missing_axis(member_has_axis) {
                    return None;
                }
                Some(
                    *values
                        .get(index)
                        .expect("replacement index is bounded by its admitted coordinate count"),
                )
            },
            ReplacementAxis::Carry => carried,
            ReplacementAxis::Drop => None,
        }
    }

    pub(crate) fn z_at(&self, old: Point, index: usize) -> Option<f64> {
        self.axis_at(&self.zs, old.z().is_some(), old.z(), index)
    }

    pub(crate) fn m_at(&self, old: Point, index: usize) -> Option<f64> {
        self.axis_at(&self.ms, old.m().is_some(), old.m(), index)
    }

    /// Build a full `CoordSeq` from this replacement applied to `old`.
    pub(crate) fn apply_to_seq(&self, old: &CoordSeq) -> Result<CoordSeq> {
        if self.positional && old.axes() != self.axes {
            return Err(GeometryErrorKind::message(
                "coordinates must preserve each coordinate sequence axes",
            ));
        }
        let xs = self.xs.clone().unwrap_or_else(|| old.carried_xs());
        let ys = self.ys.clone().unwrap_or_else(|| old.carried_ys());
        let zs = match &self.zs {
            ReplacementAxis::Replace(values) => Some(values.clone()),
            ReplacementAxis::Carry => old.carried_zs(),
            ReplacementAxis::Drop => None,
        };
        let ms = match &self.ms {
            ReplacementAxis::Replace(values) => Some(MColumn::Shared(values.clone())),
            ReplacementAxis::Carry => old.carried_ms(),
            ReplacementAxis::Drop => None,
        };
        CoordSeq::from_shared_columns(xs, ys, zs, ms)
    }
}
