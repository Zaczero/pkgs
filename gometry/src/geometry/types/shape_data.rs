use std::fmt;
use std::sync::{Arc, OnceLock};

use crate::HeapSize;
use crate::geometry::{Bounds, Shape};

/// Frozen geometry payload and its cheap bounds cache.
///
/// Heavy working sets belong to [`PreparedPlan`], not to ordinary geometry
/// values. This keeps a geometry that is never used as a repeated operand
/// free of topology, distance, tester, and linear-index slots.
pub struct ShapeData {
    shape: Shape,
    /// Prepared location checks distinguish an unset cache from cached-empty
    /// bounds, so the geometry subsystem reads this cell without recomputing.
    pub(in crate::geometry) bounds: OnceLock<Option<Bounds>>,
}

impl ShapeData {
    pub const fn new(shape: Shape) -> Self {
        Self {
            shape,
            bounds: OnceLock::new(),
        }
    }

    pub const fn shape(&self) -> &Shape {
        &self.shape
    }

    /// Take the shape out of a uniquely-owned handle (the caches are
    /// derived state and drop with it).
    pub fn into_shape(self) -> Shape {
        self.shape
    }

    /// Planar bounds, computed once.
    pub fn bounds(&self) -> Option<Bounds> {
        *self.bounds.get_or_init(|| self.shape.bounds())
    }

    pub(crate) fn existing_bounds(&self) -> Option<Bounds> {
        self.bounds.get().copied().flatten()
    }

    pub(in crate::geometry) fn seed_bounds(&self, bounds: Option<Bounds>) {
        self.bounds.get_or_init(|| bounds);
    }
}

impl HeapSize for ShapeData {
    fn heap_bytes(&self) -> usize {
        self.shape.logical_heap_bytes()
    }
}

impl std::ops::Deref for ShapeData {
    type Target = Shape;

    fn deref(&self) -> &Shape {
        &self.shape
    }
}

impl From<Shape> for ShapeData {
    fn from(shape: Shape) -> Self {
        Self::new(shape)
    }
}

pub(crate) trait IntoSharedShape {
    fn into_shared(self) -> Arc<ShapeData>;
}

impl IntoSharedShape for Shape {
    fn into_shared(self) -> Arc<ShapeData> {
        ShapeData::new(self).into_shared()
    }
}

impl IntoSharedShape for ShapeData {
    fn into_shared(self) -> Arc<ShapeData> {
        Arc::new(self)
    }
}

impl IntoSharedShape for Arc<ShapeData> {
    fn into_shared(self) -> Arc<ShapeData> {
        self
    }
}

impl PartialEq for ShapeData {
    fn eq(&self, other: &Self) -> bool {
        self.shape == other.shape
    }
}

impl Eq for ShapeData {}

impl std::hash::Hash for ShapeData {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.shape.hash(state);
    }
}

impl fmt::Debug for ShapeData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.shape.fmt(f)
    }
}
