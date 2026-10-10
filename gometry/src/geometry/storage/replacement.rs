//! Coordinate replacement payload shared by array packed/mixed paths and the
//! `Coordinates` setter surface.

use crate::error::Result;
use crate::geometry::{CoordSeq, CoordinateAxes, GeometryErrorKind, MColumn, Point, SharedSlice};

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
