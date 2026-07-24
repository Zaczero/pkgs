#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! CRS engine — libPROJ-backed transforms, introspection, catalog queries,
//! geodesy, and in-core fast paths.
//!
//! This module is the seam only: submodule declarations, the re-exported
//! public API, and the handful of shared constants. The substrate lives in
//! the children — `proj` (FFI handles + string/error utilities), `introspect`
//! (raw-pointer metadata readers), `cache` (thread-local LRU machinery),
//! `runtime` (global PROJ runtime config), `pipeline`/`transformer`/
//! `transform` (the transform engine), `in_core` (closed-form Web
//! Mercator/UTM), `catalog`/`info`/`operations`/`grids` (database queries),
//! `geodesic` (ellipsoidal math), and `types`/`options_impls`/`error` (DTOs,
//! their impls, and the error model). Children reach shared items via
//! `use super::*`.

use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{OnceLock, RwLock};

use geographiclib_rs::Geodesic;

use crate::geometry::{CoordSeq, GeometryErrorKind, Point, Shape};

mod cache;
mod catalog;
mod error;
mod geodesic;
mod grids;
mod in_core;
mod info;
mod introspect;
mod local;
mod operations;
mod options_impls;
mod pipeline;
mod proj;
mod proj_object;
mod rhumb;
mod runtime;
mod transform;
mod transformer;
mod types;

use cache::*;
pub(crate) use cache::{cache_info, lru_resolve};
pub(crate) use catalog::*;
pub(crate) use error::CrsError;
pub(crate) use geodesic::*;
pub(crate) use grids::*;
use in_core::*;
pub(crate) use info::*;
pub(crate) use introspect::Confidence;
use introspect::*;
pub(crate) use local::*;
pub(crate) use operations::*;
pub(crate) use options_impls::*;
use proj::*;
pub(crate) use rhumb::*;
pub(crate) use runtime::*;
pub(crate) use transform::*;
pub(crate) use transformer::Transformer;
pub(crate) use types::*;

const GOLDEN_RATIO: f64 = 0.618_033_988_749_894_9;
const GOLDEN_SECTION_TOLERANCE_METRES: f64 = 1e-3;
const WEB_MERCATOR_RADIUS: f64 = 6_378_137.0;
const WGS84_A: f64 = 6_378_137.0;
const WGS84_F: f64 = 1.0 / 298.257_223_563;
const UTM_K0: f64 = 0.9996;
const TRANSFORM_BOUNDS_MAX_DENSIFY: u32 = 10_000;
const DEGREE_TO_RADIAN: f64 = std::f64::consts::PI / 180.0;
