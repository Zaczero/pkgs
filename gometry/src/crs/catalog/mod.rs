#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! CRS database queries (identify, search, codes, lookup, catalog, units,
//! ellipsoids, ...) and serialization/comparison (WKT, PROJ string, PROJJSON,
//! CF, equality). Thin PROJ-database glue over the FFI wrappers in the parent.

use std::num::NonZeroUsize;

use super::CrsError;
use crate::error::Result;

/// Maximum CRS search result limit accepted by the search API.
pub(crate) const CRS_SEARCH_MAX_LIMIT: usize = 1_000;

pub(crate) fn validate_search_limit(limit: i64) -> Result<NonZeroUsize> {
    if !(1..=CRS_SEARCH_MAX_LIMIT as i64).contains(&limit) {
        return Err(CrsError::invalid(format!(
            "CRS search limit must be between 1 and {CRS_SEARCH_MAX_LIMIT}, got {limit}"
        )));
    }
    Ok(NonZeroUsize::new(limit as usize).expect("range check excludes zero"))
}

mod cf;
mod export;
mod identify;
mod lookup;
mod units;

pub(crate) use cf::*;
pub(crate) use export::*;
pub(crate) use identify::*;
pub(crate) use lookup::*;
pub(crate) use units::*;
