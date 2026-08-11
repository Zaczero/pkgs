use crate::geometry::overlay::{
    DimensionalParts, OverlayOp, Strictness, bbox_overlap_clusters_for_bounds,
};
use crate::geometry::{Bounds, CoordinateAxes, Dimension, HashMapExt as _, LineSeq, Result, Shape};
pub(crate) fn has_mixed_non_empty_topological_dimensions(inputs: &[&Shape]) -> bool {
    let dimensions = inputs.iter().fold(0_u8, |mask, input| {
        mask | DimensionalParts::non_empty_dimension_mask(input)
    });
    let known_dimensions =
        DimensionalParts::POINT_BIT | DimensionalParts::LINE_BIT | DimensionalParts::POLYGON_BIT;
    (dimensions & known_dimensions).count_ones() > 1
}

#[derive(Clone)]
pub(crate) struct OverlayWorkItem {
    pub(crate) shape: Shape,
    key: OverlayReductionKey,
    serial: usize,
    /// Cached once at construction — `shape.bounds()` is a full vertex scan
    /// and must never re-run inside sort comparators or cluster walks
    /// (sibling precedent: `areal.rs` dissolve median split).
    bounds: Option<Bounds>,
}

impl OverlayWorkItem {
    pub(crate) fn new(shape: Shape, serial: usize) -> Self {
        let bounds = shape.bounds();
        let key = OverlayReductionKey::of_with_bounds(&shape, bounds);
        Self {
            shape,
            key,
            serial,
            bounds,
        }
    }
}

impl Eq for OverlayWorkItem {}

impl PartialEq for OverlayWorkItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.serial == other.serial
    }
}

impl Ord for OverlayWorkItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.serial.cmp(&self.serial))
    }
}

impl PartialOrd for OverlayWorkItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Copy)]
pub(crate) struct OverlayReductionKey {
    dimension: Dimension,
    bbox_area: f64,
    bbox_span: f64,
    coord_count: usize,
}

impl OverlayReductionKey {
    fn of_with_bounds(shape: &Shape, bounds: Option<Bounds>) -> Self {
        let (bbox_area, bbox_span) = bounds.map_or((f64::INFINITY, f64::INFINITY), |bounds| {
            let width = bounds.maxx() - bounds.minx();
            let height = bounds.maxy() - bounds.miny();
            (width * height, width + height)
        });
        Self {
            dimension: shape.topological_dimension(),
            bbox_area,
            bbox_span,
            coord_count: shape.coord_count(),
        }
    }
}

impl Eq for OverlayReductionKey {}

impl PartialEq for OverlayReductionKey {
    fn eq(&self, other: &Self) -> bool {
        self.dimension == other.dimension
            && self.bbox_area.to_bits() == other.bbox_area.to_bits()
            && self.bbox_span.to_bits() == other.bbox_span.to_bits()
            && self.coord_count == other.coord_count
    }
}

impl Ord for OverlayReductionKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.dimension
            .cmp(&other.dimension)
            .then_with(|| self.bbox_area.total_cmp(&other.bbox_area))
            .then_with(|| self.bbox_span.total_cmp(&other.bbox_span))
            .then_with(|| self.coord_count.cmp(&other.coord_count))
    }
}

impl PartialOrd for OverlayReductionKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub(crate) fn empty_nary_overlay_shape(inputs: &[&Shape], op: OverlayOp) -> Shape {
    debug_assert!(!inputs.is_empty());
    let dimension = match op {
        OverlayOp::Intersection => inputs
            .iter()
            .map(|shape| shape.topological_dimension())
            .min()
            .expect("non-empty inputs"),
        OverlayOp::Difference => inputs[0].topological_dimension(),
        OverlayOp::Union | OverlayOp::SymmetricDifference => inputs
            .iter()
            .map(|shape| shape.topological_dimension())
            .max()
            .expect("non-empty inputs"),
    };
    empty_shape_for_dimension(dimension)
}

pub(crate) fn empty_shape_for_dimension(dimension: Dimension) -> Shape {
    match dimension {
        Dimension::Point => Shape::empty_point(),
        Dimension::Curve => Shape::LineString(LineSeq::empty(CoordinateAxes::XY)),
        Dimension::Surface => Shape::empty_polygon(),
    }
}

pub(crate) fn overlay_work_clusters(work: Vec<OverlayWorkItem>) -> Vec<Vec<OverlayWorkItem>> {
    if work.len() <= 1 {
        return vec![work];
    }
    let bounds: Vec<_> = work.iter().map(|item| item.bounds).collect();
    let Some(roots) = bbox_overlap_clusters_for_bounds(&bounds) else {
        return vec![work];
    };
    let mut slot_of: crate::collections::HashMap<usize, usize> = crate::collections::HashMap::new();
    let mut groups: Vec<Vec<OverlayWorkItem>> = Vec::new();
    for (item, root) in work.into_iter().zip(roots) {
        let slot = *slot_of.entry(root).or_insert_with(|| {
            groups.push(Vec::new());
            groups.len() - 1
        });
        groups[slot].push(item);
    }
    groups
}

pub(crate) fn symmetric_difference_cluster_balanced(
    mut work: Vec<OverlayWorkItem>,
) -> Result<Shape> {
    debug_assert!(!work.is_empty());
    if work.len() == 1 {
        return Ok(work.pop().expect("one work item").shape);
    }
    let right = split_overlay_work(&mut work);
    let left = symmetric_difference_cluster_balanced(work)?;
    let right = symmetric_difference_cluster_balanced(right)?;
    // Intermediate cluster reductions stay XY-only; Z/M is carried once at the
    // end from the ORIGINAL inputs by the caller.
    left.overlay(&right, OverlayOp::SymmetricDifference, Strictness::Lenient)
}

pub(crate) fn symmetric_difference_cluster_ordered(
    mut work: Vec<OverlayWorkItem>,
) -> Result<Shape> {
    debug_assert!(!work.is_empty());
    work.sort_unstable_by_key(|item| item.serial);
    let mut iter = work.into_iter();
    let mut accumulated = iter.next().expect("non-empty work").shape;
    for item in iter {
        accumulated = accumulated.overlay(
            &item.shape,
            OverlayOp::SymmetricDifference,
            Strictness::Lenient,
        )?;
    }
    Ok(accumulated)
}

pub(crate) fn split_overlay_work(work: &mut Vec<OverlayWorkItem>) -> Vec<OverlayWorkItem> {
    let axis = wider_bounds_axis(work);
    work.sort_unstable_by(|a, b| {
        bounds_center(a.bounds, axis)
            .total_cmp(&bounds_center(b.bounds, axis))
            .then_with(|| a.key.cmp(&b.key))
            .then_with(|| a.serial.cmp(&b.serial))
    });
    let total_weight: usize = work.iter().map(overlay_work_weight).sum();
    let mut prefix = 0_usize;
    let mut split = 1_usize;
    let mut best_delta = usize::MAX;
    for (index, item) in work.iter().enumerate().take(work.len() - 1) {
        prefix += overlay_work_weight(item);
        let delta = prefix.abs_diff(total_weight.saturating_sub(prefix));
        if delta <= best_delta {
            best_delta = delta;
            split = index + 1;
        }
    }
    work.split_off(split)
}

pub(crate) fn overlay_work_weight(item: &OverlayWorkItem) -> usize {
    item.key.coord_count.max(1)
}

pub(crate) fn wider_bounds_axis(work: &[OverlayWorkItem]) -> usize {
    let mut bounds = work.iter().filter_map(|item| item.bounds);
    let Some(mut total) = bounds.next() else {
        return 0;
    };
    for bounds in bounds {
        total.include_bounds(bounds);
    }
    usize::from((total.maxy() - total.miny()) > (total.maxx() - total.minx()))
}

pub(crate) fn bounds_center(bounds: Option<Bounds>, axis: usize) -> f64 {
    bounds.map_or(0.0, |bounds| {
        if axis == 0 {
            f64::midpoint(bounds.minx(), bounds.maxx())
        } else {
            f64::midpoint(bounds.miny(), bounds.maxy())
        }
    })
}
