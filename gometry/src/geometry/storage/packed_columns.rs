//! Detached packed-column execution over borrowed physical columns and a logical row selection.
//!
//! Present rows flow through `PackedRow`, which resolves admitted selectors
//! and outer CSR rows once; polygon coordinate windows are derived on demand.
//! Only Identity and Window selections expose contiguous slices; Gather never
//! rebuilds an input CSR payload here.
//! `collect_rows` carries missingness separately as `Option<PackedRow>`.
//! Output-producing kernels write logical rows once into their final coordinate
//! and offset columns.

mod geographic;

mod map;
mod replace;
mod resolve;
mod transform;
mod views;

pub(crate) use map::map_selected_points;
pub(crate) use replace::{replace_packed_columns_dispatch, validate_replacement_ring};
pub(crate) use resolve::{
    PackedColumnError, PackedColumnOutput, PackedColumnResult, assemble_packed_output,
};
pub(super) use resolve::{batch_err, resolve_packed_columns, row_err};
pub(crate) use transform::map_selected_to_crs;
pub(crate) use views::{
    LineColumns, PackedColumns, PointColumns, PointRows, PolygonColumns, SegmentedRuns, XyColumns,
};
#[cfg(test)]
mod tests;
use std::borrow::Cow;
use std::sync::Arc;

use super::admission::{CheckedRingWindow, RingWindowError};
use super::builder::PackedColumnBuilder;
#[cfg(test)]
use super::kernels as packed_column_kernels;
use super::kernels::map_masked_point_columns_to_crs;
use super::replacement::{CoordinateReplacement, ReplacementAxis};
use crate::crs::TransformFailure;
use crate::geographic::GeographicFrame;
use crate::geometry::storage::validity::MissingIter;
use crate::geometry::storage::{
    ArrayRows, GeometryArrayStorage, PackedRow, PackedStorage, RowSelectionRef, ValidityMask,
    admit_packed_total, csr_value_window, materialized_packed_lines_len,
    materialized_packed_polygons_len,
};
use crate::geometry::{
    CoordSeq, CoordSeqBuilder, CoordWindow, CoordinateAxes, CsrOffsetBuilder, CsrOffsetColumn,
    GeometryErrorKind, Point, PolygonLevel, RingLevel, storage,
};
