#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Coordinate replacement payload shared by array packed/mixed paths and the
//! `Coordinates` setter surface.

use std::sync::Arc;

use pyo3::prelude::*;

use crate::geometry::{CoordSeq, CoordinateAxes, Point};

#[derive(Clone)]
pub(crate) enum ReplacementAxis {
    Replace(Arc<[f64]>),
    Carry,
}

#[derive(Clone)]
pub(crate) struct CoordinateReplacement {
    pub(crate) xs: Arc<[f64]>,
    pub(crate) ys: Arc<[f64]>,
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
        carried: Option<Arc<[f64]>>,
        range: std::ops::Range<usize>,
    ) -> PyResult<Option<Arc<[f64]>>> {
        match axis {
            ReplacementAxis::Replace(values) => {
                if self.ignore_union_padding_for_missing_axis(member_has_axis) {
                    return Ok(None);
                }
                if !member_has_axis {
                    return Err(crate::py::errors::InvalidGeometryError::new_err(
                        "coordinates must preserve each coordinate sequence axes",
                    ));
                }
                Ok(Some(Arc::from(&values[range])))
            },
            ReplacementAxis::Carry => Ok(carried),
        }
    }

    pub(crate) fn z_column_for_seq(
        &self,
        seq: &CoordSeq,
        range: std::ops::Range<usize>,
    ) -> PyResult<Option<Arc<[f64]>>> {
        self.axis_column_for_seq(&self.zs, seq.axes().has_z(), seq.carried_zs(), range)
    }

    pub(crate) fn m_column_for_seq(
        &self,
        seq: &CoordSeq,
        range: std::ops::Range<usize>,
    ) -> PyResult<Option<Arc<[f64]>>> {
        self.axis_column_for_seq(&self.ms, seq.axes().has_m(), seq.carried_ms(), range)
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
                    return Err(crate::py::errors::InvalidGeometryError::new_err(
                        "coordinates must preserve each coordinate sequence axes",
                    ));
                }
                Ok(Some(values[index]))
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
            return Err(crate::py::errors::InvalidGeometryError::new_err(
                "coordinates must preserve each coordinate sequence axes",
            ));
        }
        let zs = match &self.zs {
            ReplacementAxis::Replace(values) => {
                if !old.axes().has_z() {
                    return Err(crate::py::errors::InvalidGeometryError::new_err(
                        "coordinates must preserve each coordinate sequence axes",
                    ));
                }
                Some(Arc::clone(values))
            },
            ReplacementAxis::Carry => old.carried_zs(),
        };
        let ms = match &self.ms {
            ReplacementAxis::Replace(values) => {
                if !old.axes().has_m() {
                    return Err(crate::py::errors::InvalidGeometryError::new_err(
                        "coordinates must preserve each coordinate sequence axes",
                    ));
                }
                Some(Arc::clone(values))
            },
            ReplacementAxis::Carry => old.carried_ms(),
        };
        CoordSeq::from_arc_columns(Arc::clone(&self.xs), Arc::clone(&self.ys), zs, ms)
            .map_err(PyErr::from)
    }
}
