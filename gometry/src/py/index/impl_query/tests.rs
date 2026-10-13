use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use super::*;
use crate::array::BoundsSeed;
use crate::crs::frame;
use crate::geometry::storage::validity::{count_valid_bytes, reset_count_valid_bytes};
use crate::geometry::{CoordSeq, CsrOffsetColumn, LineSeq, PolygonLevel, RingLevel, SharedSlice};
use crate::py::index::{build_spatial_index, restore_spatial_index_v2, spatial_index};

include!("tests/support.rs");
include!("tests/domain.rs");
include!("tests/candidates.rs");
include!("tests/dwithin.rs");
include!("tests/points.rs");
