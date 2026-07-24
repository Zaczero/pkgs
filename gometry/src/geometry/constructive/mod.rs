#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Constructive/editing transforms on `Shape`: buffer, offset, simplify,
//! snap, segmentize, reverse, orient, normalize, clip — and (below) quantize,
//! the `set_z`/`set_m` ordinate setters, `force_2d`, and the affine family.

pub(crate) use std::sync::Arc;

use super::*;
use crate::error::Result;
mod buffer;
mod edit;
mod offset;
mod ring;
mod simplify;
mod smooth;
mod snap;
mod walk;
mod winding;

pub(crate) use offset::*;
pub(crate) use ring::*;
pub(crate) use simplify::*;
pub(crate) use snap::*;
pub(crate) use walk::*;
pub(crate) use winding::*;
#[cfg(test)]
mod tests;
