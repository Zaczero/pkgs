use super::{Productivity, rect_bounds};

mod authority;
mod budget;
mod parity;
mod productivity;
mod support;

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use support::*;

use crate::geometry::{
    Bounds, CoordSeq, LineSeq, Point, PointBatchTester, Polygon, Ring, Shape, ShapeCountQuery,
    Strictness,
};
use crate::grid::affine_source::{
    GeographicCoverageSource, SphericalGridTarget, WorkingShapeRelation,
};
use crate::grid::coverer::CellClass;
use crate::grid::s2::cell::Cell;
use crate::grid::s2::cellid::{CellId, Face};
use crate::grid::s2::coverer::{
    AffineRectClass, CandidateCertificate, CoverContext, CoverRule, CoverStats, Coverer, Covering,
    classify, classify_one, productive_state, stage_children_at,
};
use crate::grid::s2::projection::MAX_LEVEL;
use crate::grid::spherical_arc::{
    CertifiedDegreeWindows, CertifiedLongitudeDegrees, DegreeWindowResult,
};
