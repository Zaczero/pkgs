//! Coordinate replacement payload shared by array packed/mixed paths and the
//! `Coordinates` setter surface.

use pyo3::prelude::*;

use crate::geometry::{CoordSeq, CoordinateAxes, MColumn, Point, SharedSlice};
use crate::py::errors::InvalidGeometryError;

#[derive(Clone)]
pub(crate) enum ReplacementAxis {
    Replace(SharedSlice<f64>),
    Carry,
}

#[derive(Clone)]
pub(crate) struct CoordinateReplacement {
    pub(crate) xs: SharedSlice<f64>,
    pub(crate) ys: SharedSlice<f64>,
    pub(crate) zs: ReplacementAxis,
    pub(crate) ms: ReplacementAxis,
    pub(crate) len: usize,
    pub(crate) axes: CoordinateAxes,
    pub(crate) positional: bool,
}

impl CoordinateReplacement {
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
    ) -> PyResult<Option<SharedSlice<f64>>> {
        match axis {
            ReplacementAxis::Replace(values) => {
                if self.ignore_union_padding_for_missing_axis(member_has_axis) {
                    return Ok(None);
                }
                if !member_has_axis {
                    return Err(InvalidGeometryError::new_err(
                        "coordinates must preserve each coordinate sequence axes",
                    ));
                }
                Ok(Some(SharedSlice::from_vec(
                    values
                        .get(range)
                        .expect("replacement range is bounded by its admitted coordinate count")
                        .to_vec(),
                )))
            },
            ReplacementAxis::Carry => Ok(carried()),
        }
    }

    pub(crate) fn z_column_for_seq(
        &self,
        seq: &CoordSeq,
        range: std::ops::Range<usize>,
    ) -> PyResult<Option<SharedSlice<f64>>> {
        self.axis_column_for_seq(&self.zs, seq.axes().has_z(), || seq.carried_zs(), range)
    }

    pub(crate) fn m_column_for_seq(
        &self,
        seq: &CoordSeq,
        range: std::ops::Range<usize>,
    ) -> PyResult<Option<SharedSlice<f64>>> {
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
    ) -> PyResult<Option<f64>> {
        match axis {
            ReplacementAxis::Replace(values) => {
                if self.ignore_union_padding_for_missing_axis(member_has_axis) {
                    return Ok(None);
                }
                if !member_has_axis {
                    return Err(InvalidGeometryError::new_err(
                        "coordinates must preserve each coordinate sequence axes",
                    ));
                }
                Ok(Some(*values.get(index).expect(
                    "replacement index is bounded by its admitted coordinate count",
                )))
            },
            ReplacementAxis::Carry => Ok(carried),
        }
    }

    pub(crate) fn z_at(&self, old: Point, index: usize) -> PyResult<Option<f64>> {
        self.axis_at(&self.zs, old.z().is_some(), old.z(), index)
    }

    pub(crate) fn m_at(&self, old: Point, index: usize) -> PyResult<Option<f64>> {
        self.axis_at(&self.ms, old.m().is_some(), old.m(), index)
    }

    /// Build a full `CoordSeq` from this replacement applied to `old`.
    pub(crate) fn apply_to_seq(&self, old: &CoordSeq) -> PyResult<CoordSeq> {
        if self.positional && old.axes() != self.axes {
            return Err(InvalidGeometryError::new_err(
                "coordinates must preserve each coordinate sequence axes",
            ));
        }
        let zs = match &self.zs {
            ReplacementAxis::Replace(values) => {
                if !old.axes().has_z() {
                    return Err(InvalidGeometryError::new_err(
                        "coordinates must preserve each coordinate sequence axes",
                    ));
                }
                Some(values.clone())
            },
            ReplacementAxis::Carry => old.carried_zs(),
        };
        let ms = match &self.ms {
            ReplacementAxis::Replace(values) => {
                if !old.axes().has_m() {
                    return Err(InvalidGeometryError::new_err(
                        "coordinates must preserve each coordinate sequence axes",
                    ));
                }
                Some(MColumn::Shared(values.clone()))
            },
            ReplacementAxis::Carry => old.carried_ms(),
        };
        CoordSeq::from_shared_columns(self.xs.clone(), self.ys.clone(), zs, ms).map_err(PyErr::from)
    }
}
