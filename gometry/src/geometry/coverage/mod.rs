#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Polygonal-coverage kernels: validation, boundary simplification, and
//! cleaning over a `GeometryArray` of polygons that are supposed to tile a
//! region without gaps or overlaps (parcels, admin boundaries — the
//! GEOS/PostGIS "coverage" model; distinct from the DGGS cell coverages).
//!
//! The shared substrate is the EDGE MAP: every boundary segment of every
//! row, keyed by its undirected endpoints. A segment that appears in two
//! rows is a *matched* interface (exactly shared linework — the coverage
//! contract); everything else is candidate exterior boundary, and the
//! validator checks that it genuinely stays exterior to every other row.

use std::cmp::Reverse;

pub(crate) use rstar::{AABB, RTreeObject};

use super::tessellation::minimal_positive_face_rings;
use super::*;
pub(crate) use crate::collections::{HashMap, HashMapExt, HashSet, sort_row_ids};
use crate::error::Result;

mod clean;
mod simplify;
mod validate;

pub(crate) use clean::*;
pub(crate) use simplify::{vw_area_tolerance, *};
pub(crate) use validate::*;
