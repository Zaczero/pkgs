//! Continuous planar Hausdorff distance with an absolute output-unit error bound.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::error::{Error, Result};
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::overlay::{ExactMetricSpan, ExactMetricSupport, ExactSourceInventory};
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational as Rational};
use crate::geometry::{
    CoordSeq, ExpansionBudget, LineSeq, PackedLineColumnView, Segment, Shape, XY,
};
use crate::{try_push_vec, try_reserve_vec};

struct Accuracy {
    error: Rational,
    scale_squared: Rational,
}

impl Accuracy {
    fn new(max_error: f64, scale: f64) -> Result<Self> {
        if !(max_error.is_finite() && max_error > 0.0) {
            return Err(crate::geometry::GeometryErrorKind::message(
                "max_error must be positive and finite",
            ));
        }
        if !(scale.is_finite() && scale > 0.0) {
            return Err(crate::geometry::GeometryErrorKind::message(
                "Hausdorff unit scale must be positive and finite",
            ));
        }
        Ok(Self {
            error: Rational::from_f64(max_error)?,
            scale_squared: Rational::from_f64(scale)?.try_square()?,
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct FloatBox {
    min_x: f64,
    max_x: f64,
    min_y: f64,
    max_y: f64,
}

impl FloatBox {
    fn from_point(point: &ExactPoint) -> Result<Self> {
        let (min_x, max_x) = point.x.outward_bounds()?;
        let (min_y, max_y) = point.y.outward_bounds()?;
        if !(min_x.is_finite() && max_x.is_finite() && min_y.is_finite() && max_y.is_finite()) {
            return Err(crate::geometry::GeometryErrorKind::message(
                "selected support has no finite conservative index enclosure",
            ));
        }
        Ok(Self {
            min_x,
            max_x,
            min_y,
            max_y,
        })
    }

    fn include(self, other: Self) -> Self {
        Self {
            min_x: self.min_x.min(other.min_x),
            max_x: self.max_x.max(other.max_x),
            min_y: self.min_y.min(other.min_y),
            max_y: self.max_y.max(other.max_y),
        }
    }

    fn corners(self) -> [(f64, f64); 4] {
        [
            (self.min_x, self.min_y),
            (self.min_x, self.max_y),
            (self.max_x, self.min_y),
            (self.max_x, self.max_y),
        ]
    }
}

#[derive(Debug)]
struct SupportSpan {
    source: Segment,
    t_lo: Rational,
    t_hi: Rational,
    start: ExactPoint,
    bounds: FloatBox,
}

#[derive(Debug)]
struct BoundaryEdge {
    start: ExactPoint,
    end: ExactPoint,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PrimitiveId {
    Span(usize),
    Point(usize),
}

#[derive(Clone, Copy, Debug)]
struct IndexItem {
    primitive: PrimitiveId,
    bounds: FloatBox,
}

#[derive(Clone, Copy, Debug)]
struct IndexNode {
    bounds: FloatBox,
    left: Option<usize>,
    right: Option<usize>,
    start: usize,
    end: usize,
}

#[derive(Debug, Default)]
struct SupportIndex {
    items: Vec<IndexItem>,
    order: Vec<usize>,
    nodes: Vec<IndexNode>,
    root: Option<usize>,
}

impl SupportIndex {
    fn build(spans: &[SupportSpan], points: &[ExactPoint]) -> Result<Self> {
        let item_count = spans
            .len()
            .checked_add(points.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let mut items = Vec::new();
        try_reserve_vec(&mut items, item_count)?;
        for (index, span) in spans.iter().enumerate() {
            try_push_vec(&mut items, IndexItem {
                primitive: PrimitiveId::Span(index),
                bounds: span.bounds,
            })?;
        }
        for (index, point) in points.iter().enumerate() {
            try_push_vec(&mut items, IndexItem {
                primitive: PrimitiveId::Point(index),
                bounds: FloatBox::from_point(point)?,
            })?;
        }
        let mut order = Vec::new();
        try_reserve_vec(&mut order, items.len())?;
        for index in 0..items.len() {
            try_push_vec(&mut order, index)?;
        }
        let node_capacity = items
            .len()
            .checked_mul(2)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let mut index = Self {
            items,
            order,
            nodes: Vec::new(),
            root: None,
        };
        try_reserve_vec(&mut index.nodes, node_capacity)?;
        if !index.items.is_empty() {
            index.root = Some(index.build_range(0, index.order.len())?);
        }

        Ok(index)
    }

    fn build_range(&mut self, start: usize, end: usize) -> Result<usize> {
        let bounds = self.range_bounds(start, end);
        if end.saturating_sub(start) <= 4 {
            let node = IndexNode {
                bounds,
                left: None,
                right: None,
                start,
                end,
            };
            let index = self.nodes.len();
            try_push_vec(&mut self.nodes, node)?;
            return Ok(index);
        }

        let split_x = (bounds.max_x - bounds.min_x) >= (bounds.max_y - bounds.min_y);
        self.order[start..end].sort_unstable_by(|left, right| {
            let left_bounds = self.items[*left].bounds;
            let right_bounds = self.items[*right].bounds;
            let left_key = if split_x {
                f64::midpoint(left_bounds.min_x, left_bounds.max_x)
            } else {
                f64::midpoint(left_bounds.min_y, left_bounds.max_y)
            };
            let right_key = if split_x {
                f64::midpoint(right_bounds.min_x, right_bounds.max_x)
            } else {
                f64::midpoint(right_bounds.min_y, right_bounds.max_y)
            };
            left_key.total_cmp(&right_key).then_with(|| left.cmp(right))
        });
        let middle = start + (end - start) / 2;
        let left = self.build_range(start, middle)?;
        let right = self.build_range(middle, end)?;
        let index = self.nodes.len();
        try_push_vec(&mut self.nodes, IndexNode {
            bounds,
            left: Some(left),
            right: Some(right),
            start: 0,
            end: 0,
        })?;
        Ok(index)
    }

    fn range_bounds(&self, start: usize, end: usize) -> FloatBox {
        let first = self.items[self.order[start]].bounds;
        self.order[start + 1..end]
            .iter()
            .fold(first, |bounds, index| {
                bounds.include(self.items[*index].bounds)
            })
    }

    fn nearest(&self, support: &PreparedSupport, point: &ExactPoint) -> Result<Rational> {
        let Some(root) = self.root else {
            return Err(crate::geometry::GeometryErrorKind::message(
                "hausdorff_distance requires non-empty support",
            ));
        };
        let mut best = None;
        let point_bounds = FloatBox::from_point(point)?;
        let mut best_upper = f64::INFINITY;
        self.nearest_node(
            root,
            support,
            point,
            point_bounds,
            &mut best,
            &mut best_upper,
        )?;
        best.ok_or_else(|| {
            crate::geometry::GeometryErrorKind::message(
                "hausdorff_distance requires non-empty support",
            )
            .into()
        })
    }

    fn nearest_node(
        &self,
        node_index: usize,
        support: &PreparedSupport,
        point: &ExactPoint,
        point_bounds: FloatBox,

        best: &mut Option<Rational>,
        best_upper: &mut f64,
    ) -> Result<()> {
        let node = self.nodes[node_index];
        if box_distance_lower(point_bounds, node.bounds) > *best_upper {
            return Ok(());
        }
        if let (Some(left), Some(right)) = (node.left, node.right) {
            let (first, second) = {
                let left_lower = box_distance_lower(point_bounds, self.nodes[left].bounds);
                let right_lower = box_distance_lower(point_bounds, self.nodes[right].bounds);
                if left_lower <= right_lower {
                    (left, right)
                } else {
                    (right, left)
                }
            };
            self.nearest_node(first, support, point, point_bounds, best, best_upper)?;
            self.nearest_node(second, support, point, point_bounds, best, best_upper)?;
            return Ok(());
        }

        for order_index in &self.order[node.start..node.end] {
            let item = self.items[*order_index];

            if box_distance_lower(point_bounds, item.bounds) > *best_upper {
                continue;
            }

            let distance = support.primitive_distance_squared(item.primitive, point)?;
            let replace = match best {
                None => true,
                Some(current) => distance.try_cmp(current)? == Ordering::Less,
            };
            if replace {
                *best_upper = nonnegative_upper_bound(&distance)?;
                *best = Some(distance);
            }
        }
        Ok(())
    }

    fn nominate(&self, point: &ExactPoint) -> Result<Vec<PrimitiveId>> {
        let mut nominated = Vec::new();
        try_reserve_vec(&mut nominated, 8)?;
        if let Some(root) = self.root {
            self.nominate_node(root, point, &mut nominated)?;
        }
        Ok(nominated)
    }

    fn nominate_node(
        &self,
        node_index: usize,
        point: &ExactPoint,

        nominated: &mut Vec<PrimitiveId>,
    ) -> Result<()> {
        if nominated.len() >= 8 {
            return Ok(());
        }

        let node = self.nodes[node_index];
        if let (Some(left), Some(right)) = (node.left, node.right) {
            let left_lower = aabb_lower_squared(point, self.nodes[left].bounds)?;
            let right_lower = aabb_lower_squared(point, self.nodes[right].bounds)?;
            let (first, second) = if left_lower.try_cmp(&right_lower)? != Ordering::Greater {
                (left, right)
            } else {
                (right, left)
            };
            self.nominate_node(first, point, nominated)?;
            self.nominate_node(second, point, nominated)?;
        } else {
            for order_index in &self.order[node.start..node.end] {
                if nominated.len() >= 8 {
                    break;
                }
                try_push_vec(&mut *nominated, self.items[*order_index].primitive)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct PreparedSupport {
    spans: Vec<SupportSpan>,
    points: Vec<ExactPoint>,
    faces: Vec<Vec<Vec<ExactPoint>>>,
    boundary_edges: Vec<BoundaryEdge>,
    area_bounds: Option<FloatBox>,
    index: SupportIndex,
}

impl PreparedSupport {
    fn from_shape(shape: &Shape, budget: &mut ExpansionBudget) -> Result<Self> {
        if shape.is_empty() {
            return Err(crate::geometry::GeometryErrorKind::message(
                "hausdorff_distance requires non-empty support",
            ));
        }
        if !shape.has_area_parts() {
            let mut spans = Vec::new();
            let mut points = Vec::new();
            try_reserve_vec(&mut spans, shape.segment_count())?;
            shape.try_for_each_vertex_pair(|start, end| -> crate::error::Result<()> {
                push_raw_segment(
                    Segment {
                        start: start.xy(),
                        end: end.xy(),
                    },
                    &mut spans,
                    &mut points,
                )
            })?;
            for point in shape.point_only_points()? {
                try_push_vec(&mut points, ExactPoint::from_xy(point.xy())?)?;
            }
            return Self::from_parts(spans, points, Vec::new());
        }
        let inventory = ExactSourceInventory::from_shapes(&[shape])?;
        let partition = inventory.partition(budget)?;
        let support = inventory.metric_support(&partition)?;
        Self::from_metric_support(support)
    }

    fn from_line_columns(xs: &[f64], ys: &[f64]) -> Result<Self> {
        debug_assert_eq!(xs.len(), ys.len());
        let mut spans = Vec::new();
        let mut points = Vec::new();
        try_reserve_vec(&mut spans, xs.len().saturating_sub(1))?;
        if xs.len() == 1 {
            try_push_vec(&mut points, exact_point_xy(xs[0], ys[0])?)?;
        }
        for index in 1..xs.len() {
            push_raw_segment(
                Segment {
                    start: XY::new(xs[index - 1], ys[index - 1]),
                    end: XY::new(xs[index], ys[index]),
                },
                &mut spans,
                &mut points,
            )?;
        }
        Self::from_parts(spans, points, Vec::new())
    }

    fn from_metric_support(support: ExactMetricSupport) -> Result<Self> {
        let ExactMetricSupport {
            spans: metric_spans,
            points: metric_points,
            faces: metric_faces,
        } = support;
        let mut spans = Vec::new();
        try_reserve_vec(&mut spans, metric_spans.len())?;
        for span in metric_spans {
            try_push_vec(&mut spans, support_span(span)?)?;
        }

        let mut points = Vec::new();
        try_reserve_vec(&mut points, metric_points.len())?;
        for point in metric_points {
            try_push_vec(
                &mut points,
                ExactPoint::from_homogeneous(point.homogeneous)?,
            )?;
        }

        let mut faces = Vec::new();
        try_reserve_vec(&mut faces, metric_faces.len())?;
        for face in metric_faces {
            let mut cycles = Vec::new();
            try_reserve_vec(&mut cycles, face.len())?;
            for cycle in face {
                let mut points = Vec::new();
                try_reserve_vec(&mut points, cycle.len())?;
                for point in cycle {
                    try_push_vec(
                        &mut points,
                        ExactPoint::from_homogeneous(point.homogeneous)?,
                    )?;
                }
                try_push_vec(&mut cycles, points)?;
            }
            try_push_vec(&mut faces, cycles)?;
        }

        Self::from_parts(spans, points, faces)
    }

    fn from_parts(
        spans: Vec<SupportSpan>,
        points: Vec<ExactPoint>,
        faces: Vec<Vec<Vec<ExactPoint>>>,
    ) -> Result<Self> {
        let mut boundary_edges = Vec::new();
        for face in &faces {
            for cycle in face {
                if cycle.len() < 2 {
                    continue;
                }
                for index in 0..cycle.len() {
                    try_push_vec(&mut boundary_edges, BoundaryEdge {
                        start: cycle[index].try_clone()?,
                        end: cycle[(index + 1) % cycle.len()].try_clone()?,
                    })?;
                }
            }
        }

        let mut area_bounds: Option<FloatBox> = None;
        for edge in &boundary_edges {
            let edge_bounds =
                FloatBox::from_point(&edge.start)?.include(FloatBox::from_point(&edge.end)?);
            area_bounds = Some(match area_bounds {
                None => edge_bounds,
                Some(current) => current.include(edge_bounds),
            });
        }

        if spans.is_empty() && points.is_empty() && faces.is_empty() {
            return Err(crate::geometry::GeometryErrorKind::message(
                "hausdorff_distance requires non-empty support",
            ));
        }

        Ok(Self {
            index: SupportIndex::build(&spans, &points)?,
            spans,
            points,
            faces,
            boundary_edges,
            area_bounds,
        })
    }

    fn initial_patches(&self) -> Result<Vec<SourcePatch>> {
        let mut patches = Vec::new();
        let capacity = self
            .spans
            .len()
            .checked_add(usize::from(!self.faces.is_empty()))
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        try_reserve_vec(&mut patches, capacity)?;
        for (index, span) in self.spans.iter().enumerate() {
            try_push_vec(&mut patches, SourcePatch::Span {
                span: index,
                lo: span.t_lo.try_clone()?,
                hi: span.t_hi.try_clone()?,
            })?;
        }
        if let Some(bounds) = self.area_bounds
            && !self.faces.is_empty()
        {
            try_push_vec(&mut patches, SourcePatch::Box(bounds))?;
        }
        Ok(patches)
    }

    fn contains_area(&self, point: &ExactPoint) -> Result<bool> {
        for face in &self.faces {
            let mut inside = false;
            for cycle in face {
                if point_in_cycle(point, cycle)? {
                    return Ok(true);
                }
                if cycle_crosses_ray(point, cycle)? {
                    inside = !inside;
                }
            }
            if inside {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn source_box_witness(&self, bounds: FloatBox) -> Result<Option<ExactPoint>> {
        for (x, y) in bounds.corners() {
            let point = exact_point_xy(x, y)?;
            if self.contains_area(&point)? {
                return Ok(Some(point));
            }
        }
        for edge in &self.boundary_edges {
            if let Some(point) = segment_box_intersection(edge, bounds)? {
                return Ok(Some(point));
            }
        }
        Ok(None)
    }

    fn primitive_distance_squared(
        &self,
        primitive: PrimitiveId,
        point: &ExactPoint,
    ) -> Result<Rational> {
        match primitive {
            PrimitiveId::Span(index) => point_span_distance_squared(point, &self.spans[index]),
            PrimitiveId::Point(index) => {
                let dx = (&point.x).try_subtract(&self.points[index].x)?;
                let dy = (&point.y).try_subtract(&self.points[index].y)?;
                (&(&dx).try_square()?).try_add(&(&dy).try_square()?)
            },
        }
    }

    fn distance_squared(&self, point: &ExactPoint) -> Result<Rational> {
        let result = if self.contains_area(point)? {
            Ok(Rational::zero())
        } else {
            self.index.nearest(self, point)
        };

        result
    }
}

fn push_raw_segment(
    source: Segment,
    spans: &mut Vec<SupportSpan>,
    points: &mut Vec<ExactPoint>,
) -> Result<()> {
    if source.start == source.end {
        return try_push_vec(points, ExactPoint::from_xy(source.start)?);
    }
    try_push_vec(spans, SupportSpan {
        start: ExactPoint::from_xy(source.start)?,
        t_lo: Rational::zero(),
        t_hi: Rational::one(),
        bounds: FloatBox {
            min_x: source.start.x.min(source.end.x),
            max_x: source.start.x.max(source.end.x),
            min_y: source.start.y.min(source.end.y),
            max_y: source.start.y.max(source.end.y),
        },
        source,
    })
}

fn support_span(span: ExactMetricSpan) -> Result<SupportSpan> {
    let start = ExactPoint::from_homogeneous(span.start.homogeneous)?;
    let end = ExactPoint::from_homogeneous(span.end.homogeneous)?;
    let mut t_lo = Rational::new(
        span.start_parameter[0].try_clone()?,
        span.start_parameter[1].try_clone()?,
    )?;
    let mut t_hi = Rational::new(
        span.end_parameter[0].try_clone()?,
        span.end_parameter[1].try_clone()?,
    )?;
    if t_lo.try_cmp(&t_hi)? == Ordering::Greater {
        std::mem::swap(&mut t_lo, &mut t_hi);
    }
    let bounds = FloatBox::from_point(&start)?.include(FloatBox::from_point(&end)?);
    Ok(SupportSpan {
        source: span.source,
        t_lo,
        t_hi,
        start,
        bounds,
    })
}

fn exact_point_xy(x: f64, y: f64) -> Result<ExactPoint> {
    Ok(ExactPoint::from_xy(XY::new(x, y))?)
}

fn carrier_components(source: Segment) -> Result<(Rational, Rational, Rational, Rational)> {
    let ax = Rational::from_f64(source.start.x)?;
    let ay = Rational::from_f64(source.start.y)?;
    let bx = Rational::from_f64(source.end.x)?;
    let by = Rational::from_f64(source.end.y)?;
    Ok((
        ax.try_clone()?,
        ay.try_clone()?,
        (&bx).try_subtract(&ax)?,
        (&by).try_subtract(&ay)?,
    ))
}

fn span_point(span: &SupportSpan, parameter: &Rational) -> Result<ExactPoint> {
    let (ax, ay, dx, dy) = carrier_components(span.source)?;
    let x = (&ax).try_add(&(&dx).try_product(parameter)?)?;
    let y = (&ay).try_add(&(&dy).try_product(parameter)?)?;
    Ok(ExactPoint { x, y })
}

fn span_projection_parameter(point: &ExactPoint, span: &SupportSpan) -> Result<Rational> {
    let (ax, ay, dx, dy) = carrier_components(span.source)?;
    let px = (&point.x).try_subtract(&ax)?;
    let py = (&point.y).try_subtract(&ay)?;
    let numerator = (&(&px).try_product(&dx)?).try_add(&(&py).try_product(&dy)?)?;
    let denominator = (&(&dx).try_square()?).try_add(&(&dy).try_square()?)?;
    if denominator.is_zero() {
        return Ok(Rational::zero());
    }
    (&numerator).try_quotient(&denominator)
}

fn point_span_distance_squared(point: &ExactPoint, span: &SupportSpan) -> Result<Rational> {
    let (ax, ay, dx, dy) = carrier_components(span.source)?;
    let px = (&point.x).try_subtract(&ax)?;
    let py = (&point.y).try_subtract(&ay)?;
    let dot = (&(&px).try_product(&dx)?).try_add(&(&py).try_product(&dy)?)?;
    let norm = (&(&dx).try_square()?).try_add(&(&dy).try_square()?)?;
    let low = (&span.t_lo).try_product(&norm)?;
    let high = (&span.t_hi).try_product(&norm)?;
    let parameter = if norm.is_zero() || dot.try_cmp(&low)? == Ordering::Less {
        &span.t_lo
    } else if dot.try_cmp(&high)? == Ordering::Greater {
        &span.t_hi
    } else {
        let cross = (&(&px).try_product(&dy)?).try_subtract(&(&py).try_product(&dx)?)?;
        return (&(&cross).try_square()?).try_quotient(&norm);
    };
    let projected = span_point(span, parameter)?;
    let dx = (&point.x).try_subtract(&projected.x)?;
    let dy = (&point.y).try_subtract(&projected.y)?;
    (&(&dx).try_square()?).try_add(&(&dy).try_square()?)
}

fn nonnegative_upper_bound(value: &Rational) -> Result<f64> {
    if value.try_cmp(&Rational::from_f64(f64::MAX)?)? == Ordering::Greater {
        Ok(f64::INFINITY)
    } else {
        Ok(value.outward_bounds()?.1)
    }
}

fn box_distance_lower(left: FloatBox, right: FloatBox) -> f64 {
    let gaps = [
        right.min_x - left.max_x,
        left.min_x - right.max_x,
        right.min_y - left.max_y,
        left.min_y - right.max_y,
    ];
    if gaps.iter().any(|value| value.is_nan()) {
        return 0.0;
    }
    let dx = gaps[0].max(gaps[1]).next_down().max(0.0);
    let dy = gaps[2].max(gaps[3]).next_down().max(0.0);
    let x_squared = (dx * dx).next_down().max(0.0);
    let y_squared = (dy * dy).next_down().max(0.0);
    (x_squared + y_squared).next_down().max(0.0)
}

fn aabb_lower_squared(point: &ExactPoint, bounds: FloatBox) -> Result<Rational> {
    let min_x = Rational::from_f64(bounds.min_x)?;
    let max_x = Rational::from_f64(bounds.max_x)?;
    let min_y = Rational::from_f64(bounds.min_y)?;
    let max_y = Rational::from_f64(bounds.max_y)?;
    let zero = Rational::zero();
    let dx = if point.x.try_cmp(&min_x)? == Ordering::Less {
        (&min_x).try_subtract(&point.x)?
    } else if point.x.try_cmp(&max_x)? == Ordering::Greater {
        (&point.x).try_subtract(&max_x)?
    } else {
        zero.try_clone()?
    };
    let dy = if point.y.try_cmp(&min_y)? == Ordering::Less {
        (&min_y).try_subtract(&point.y)?
    } else if point.y.try_cmp(&max_y)? == Ordering::Greater {
        (&point.y).try_subtract(&max_y)?
    } else {
        zero
    };
    (&(&dx).try_square()?).try_add(&(&dy).try_square()?)
}

fn point_in_cycle(point: &ExactPoint, cycle: &[ExactPoint]) -> Result<bool> {
    if cycle.len() < 3 {
        return Ok(false);
    }
    for index in 0..cycle.len() {
        let left = &cycle[index];
        let right = &cycle[(index + 1) % cycle.len()];
        let dx = (&right.x).try_subtract(&left.x)?;
        let dy = (&right.y).try_subtract(&left.y)?;
        let px = (&point.x).try_subtract(&left.x)?;
        let py = (&point.y).try_subtract(&left.y)?;
        let cross = (&(&dx).try_product(&py)?).try_subtract(&(&dy).try_product(&px)?)?;
        if cross.is_zero()
            && between(&point.x, &left.x, &right.x)?
            && between(&point.y, &left.y, &right.y)?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn cycle_crosses_ray(point: &ExactPoint, cycle: &[ExactPoint]) -> Result<bool> {
    if cycle.len() < 3 {
        return Ok(false);
    }
    let mut crossings = false;
    for index in 0..cycle.len() {
        let left = &cycle[index];
        let right = &cycle[(index + 1) % cycle.len()];
        let left_above = left.y.try_cmp(&point.y)? == Ordering::Greater;
        let right_above = right.y.try_cmp(&point.y)? == Ordering::Greater;
        if left_above == right_above {
            continue;
        }
        let dx = (&right.x).try_subtract(&left.x)?;
        let dy = (&right.y).try_subtract(&left.y)?;
        let px = (&point.x).try_subtract(&left.x)?;
        let py = (&point.y).try_subtract(&left.y)?;
        let cross = (&(&dx).try_product(&py)?).try_subtract(&(&dy).try_product(&px)?)?;
        let rising = right.y.try_cmp(&left.y)? == Ordering::Greater;
        if (rising && cross.sign() == ExactSign::Positive)
            || (!rising && cross.sign() == ExactSign::Negative)
        {
            crossings = !crossings;
        }
    }
    Ok(crossings)
}

fn between(value: &Rational, left: &Rational, right: &Rational) -> Result<bool> {
    Ok(
        (value.try_cmp(left)? != Ordering::Less && value.try_cmp(right)? != Ordering::Greater)
            || (value.try_cmp(left)? != Ordering::Greater
                && value.try_cmp(right)? != Ordering::Less),
    )
}

fn segment_box_intersection(edge: &BoundaryEdge, bounds: FloatBox) -> Result<Option<ExactPoint>> {
    if point_in_box(&edge.start, bounds)? {
        return Ok(Some(edge.start.try_clone()?));
    }
    if point_in_box(&edge.end, bounds)? {
        return Ok(Some(edge.end.try_clone()?));
    }
    let dx = (&edge.end.x).try_subtract(&edge.start.x)?;
    let dy = (&edge.end.y).try_subtract(&edge.start.y)?;
    for x in [bounds.min_x, bounds.max_x] {
        if dx.is_zero() {
            continue;
        }
        let x = Rational::from_f64(x)?;
        let parameter = (&(&x).try_subtract(&edge.start.x)?).try_quotient(&dx)?;
        if parameter.try_cmp(&Rational::zero())? == Ordering::Less
            || parameter.try_cmp(&Rational::one())? == Ordering::Greater
        {
            continue;
        }
        let y = (&edge.start.y).try_add(&(&dy).try_product(&parameter)?)?;
        if y.try_cmp(&Rational::from_f64(bounds.min_y)?)? != Ordering::Less
            && y.try_cmp(&Rational::from_f64(bounds.max_y)?)? != Ordering::Greater
        {
            return Ok(Some(ExactPoint { x, y }));
        }
    }
    for y in [bounds.min_y, bounds.max_y] {
        if dy.is_zero() {
            continue;
        }
        let y = Rational::from_f64(y)?;
        let parameter = (&(&y).try_subtract(&edge.start.y)?).try_quotient(&dy)?;
        if parameter.try_cmp(&Rational::zero())? == Ordering::Less
            || parameter.try_cmp(&Rational::one())? == Ordering::Greater
        {
            continue;
        }
        let x = (&edge.start.x).try_add(&(&dx).try_product(&parameter)?)?;
        if x.try_cmp(&Rational::from_f64(bounds.min_x)?)? != Ordering::Less
            && x.try_cmp(&Rational::from_f64(bounds.max_x)?)? != Ordering::Greater
        {
            return Ok(Some(ExactPoint { x, y }));
        }
    }
    Ok(None)
}

fn point_in_box(point: &ExactPoint, bounds: FloatBox) -> Result<bool> {
    let min_x = Rational::from_f64(bounds.min_x)?;
    let max_x = Rational::from_f64(bounds.max_x)?;
    let min_y = Rational::from_f64(bounds.min_y)?;
    let max_y = Rational::from_f64(bounds.max_y)?;
    Ok(point.x.try_cmp(&min_x)? != Ordering::Less
        && point.x.try_cmp(&max_x)? != Ordering::Greater
        && point.y.try_cmp(&min_y)? != Ordering::Less
        && point.y.try_cmp(&max_y)? != Ordering::Greater)
}

#[derive(Debug)]
enum SourcePatch {
    Span {
        span: usize,
        lo: Rational,
        hi: Rational,
    },
    Box(FloatBox),
}

impl SourcePatch {
    fn witness(&self, source: &PreparedSupport) -> Result<Option<ExactPoint>> {
        match self {
            Self::Span { span, lo, hi } => {
                let midpoint = lo.try_midpoint(hi)?;
                Ok(Some(span_point(&source.spans[*span], &midpoint)?))
            },
            Self::Box(bounds) => source.source_box_witness(*bounds),
        }
    }

    fn center_and_radius_squared(
        &self,
        source: &PreparedSupport,
    ) -> Result<(ExactPoint, Rational)> {
        match self {
            Self::Span { span, lo, hi } => {
                let midpoint = lo.try_midpoint(hi)?;
                let center = span_point(&source.spans[*span], &midpoint)?;
                let radius = span_radius_squared(&source.spans[*span], lo, hi)?;
                Ok((center, radius))
            },
            Self::Box(bounds) => {
                let min_x = Rational::from_f64(bounds.min_x)?;
                let max_x = Rational::from_f64(bounds.max_x)?;
                let min_y = Rational::from_f64(bounds.min_y)?;
                let max_y = Rational::from_f64(bounds.max_y)?;
                let center = ExactPoint {
                    x: min_x.try_midpoint(&max_x)?,
                    y: min_y.try_midpoint(&max_y)?,
                };
                let half_x =
                    (&(&max_x).try_subtract(&min_x)?).try_quotient(&Rational::from_f64(2.0)?)?;
                let half_y =
                    (&(&max_y).try_subtract(&min_y)?).try_quotient(&Rational::from_f64(2.0)?)?;
                let radius = (&(&half_x).try_square()?).try_add(&(&half_y).try_square()?)?;
                Ok((center, radius))
            },
        }
    }

    #[cfg(any(test, debug_assertions))]
    fn witness_is_valid(&self, source: &PreparedSupport, witness: &ExactPoint) -> Result<bool> {
        match self {
            Self::Span { span, lo, hi } => {
                let source_span = &source.spans[*span];
                let parameter = span_projection_parameter(witness, source_span)?;
                if parameter.try_cmp(lo)? == Ordering::Less
                    || parameter.try_cmp(hi)? == Ordering::Greater
                {
                    return Ok(false);
                }
                let reconstructed = span_point(source_span, &parameter)?;
                Ok(reconstructed.same_position(witness)?)
            },
            Self::Box(bounds) => {
                Ok(point_in_box(witness, *bounds)? && source.contains_area(witness)?)
            },
        }
    }

    fn corners(&self, source: &PreparedSupport) -> Result<Vec<ExactPoint>> {
        let mut corners = Vec::new();
        match self {
            Self::Span { span, lo, hi } => {
                try_reserve_vec(&mut corners, 2)?;
                try_push_vec(&mut corners, span_point(&source.spans[*span], lo)?)?;
                try_push_vec(&mut corners, span_point(&source.spans[*span], hi)?)?;
            },
            Self::Box(bounds) => {
                try_reserve_vec(&mut corners, 4)?;
                for (x, y) in bounds.corners() {
                    try_push_vec(&mut corners, exact_point_xy(x, y)?)?;
                }
            },
        }
        Ok(corners)
    }

    fn split(self, budget: &mut ExpansionBudget) -> Result<Option<Vec<Self>>> {
        match self {
            Self::Span { span, lo, hi } => {
                budget.add(2)?;
                let midpoint = lo.try_midpoint(&hi)?;
                let mut children = Vec::new();
                try_reserve_vec(&mut children, 2)?;
                try_push_vec(&mut children, Self::Span {
                    span,
                    lo: lo.try_clone()?,
                    hi: midpoint.try_clone()?,
                })?;
                try_push_vec(&mut children, Self::Span {
                    span,
                    lo: midpoint,
                    hi,
                })?;
                Ok(Some(children))
            },
            Self::Box(bounds) => {
                let mid_x = f64::midpoint(bounds.min_x, bounds.max_x);
                let mid_y = f64::midpoint(bounds.min_y, bounds.max_y);
                let split_x = bounds.min_x < mid_x && mid_x < bounds.max_x;
                let split_y = bounds.min_y < mid_y && mid_y < bounds.max_y;
                if !split_x && !split_y {
                    return Ok(None);
                }
                let count = (1 + usize::from(split_x)) * (1 + usize::from(split_y));
                budget.add(count)?;
                let mut children = Vec::new();
                try_reserve_vec(&mut children, count)?;
                let xs = if split_x {
                    [(bounds.min_x, mid_x), (mid_x, bounds.max_x)]
                } else {
                    [(bounds.min_x, bounds.max_x); 2]
                };
                let ys = if split_y {
                    [(bounds.min_y, mid_y), (mid_y, bounds.max_y)]
                } else {
                    [(bounds.min_y, bounds.max_y); 2]
                };
                for &(min_x, max_x) in &xs[..if split_x { 2 } else { 1 }] {
                    for &(min_y, max_y) in &ys[..if split_y { 2 } else { 1 }] {
                        try_push_vec(
                            &mut children,
                            Self::Box(FloatBox {
                                min_x,
                                max_x,
                                min_y,
                                max_y,
                            }),
                        )?;
                    }
                }
                Ok(Some(children))
            },
        }
    }
}

fn span_radius_squared(span: &SupportSpan, lo: &Rational, hi: &Rational) -> Result<Rational> {
    let (_, _, dx, dy) = carrier_components(span.source)?;
    let delta = (hi).try_subtract(lo)?;
    let half = (&delta).try_quotient(&Rational::from_f64(2.0)?)?;
    let direction_squared = (&(&dx).try_square()?).try_add(&(&dy).try_square()?)?;
    (&direction_squared).try_product(&(&half).try_square()?)
}

#[derive(Debug)]
struct SqrtInterval {
    lower: ExactDyadic,
    upper: Option<ExactDyadic>,
}

impl SqrtInterval {
    fn from_squared(squared: &Rational) -> Result<Self> {
        let value = squared.try_clone()?;
        let rounded = value.round_sqrt()?;
        if rounded.is_infinite() {
            return Ok(Self {
                lower: ExactDyadic::try_from_f64(f64::MAX)?,
                upper: None,
            });
        }
        let exact = ExactDyadic::try_from_f64(rounded)?;
        let exact_squared = Rational::from_dyadic(exact.try_clone()?)?;
        let exact_squared = (&(&exact_squared).try_square()?).try_add(&Rational::zero())?;
        match squared.try_cmp(&exact_squared)? {
            Ordering::Equal => Ok(Self {
                lower: exact.try_clone()?,
                upper: Some(exact),
            }),
            Ordering::Less => Ok(Self {
                lower: ExactDyadic::try_from_f64(rounded.next_down())?,
                upper: Some(exact),
            }),
            Ordering::Greater => {
                if rounded == f64::MAX {
                    return Ok(Self {
                        lower: exact,
                        upper: None,
                    });
                }
                Ok(Self {
                    lower: exact,
                    upper: Some(ExactDyadic::try_from_f64(rounded.next_up())?),
                })
            },
        }
    }

    fn refine(&mut self, squared: &Rational, error: &Rational) -> Result<()> {
        let Some(mut upper) = self.upper.take() else {
            return Ok(());
        };
        loop {
            let width =
                Rational::from_dyadic(upper.try_clone()?.try_subtract(self.lower.try_clone()?)?)?;
            if width.try_cmp(error)? != Ordering::Greater {
                self.upper = Some(upper);
                return Ok(());
            }

            let midpoint = self
                .lower
                .try_clone()?
                .try_add(upper.try_clone()?)?
                .try_product(&ExactDyadic::try_from_f64(0.5)?)?;
            let midpoint_rational = Rational::from_dyadic(midpoint.try_clone()?)?;
            let midpoint_squared = (&midpoint_rational).try_square()?;

            match squared.try_cmp(&midpoint_squared)? {
                Ordering::Equal => {
                    self.lower = midpoint.try_clone()?;
                    upper = midpoint;
                },
                Ordering::Greater => {
                    self.lower = midpoint;
                },
                Ordering::Less => {
                    upper = midpoint;
                },
            }
        }
    }

    fn upper_rational(&self) -> Result<Option<Rational>> {
        self.upper
            .as_ref()
            .map(|value| Rational::from_dyadic(value.try_clone()?))
            .transpose()
    }
}

fn min_rational(current: &mut Option<Rational>, candidate: Rational) -> Result<()> {
    let replace = match current {
        None => true,
        Some(value) => candidate.try_cmp(value)? == Ordering::Less,
    };
    if replace {
        *current = Some(candidate);
    }
    Ok(())
}

fn max_rational(current: &mut Option<Rational>, candidate: Rational) -> Result<()> {
    let replace = match current {
        None => true,
        Some(value) => candidate.try_cmp(value)? == Ordering::Greater,
    };
    if replace {
        *current = Some(candidate);
    }
    Ok(())
}

#[derive(Debug)]
struct PatchRecord {
    patch: SourcePatch,
    upper: Option<Rational>,
}

#[derive(Debug)]
struct QueueEntry {
    key: f64,
    record: PatchRecord,
}

impl PartialEq for QueueEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key.to_bits() == other.key.to_bits()
    }
}

impl Eq for QueueEntry {}

impl PartialOrd for QueueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueueEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.total_cmp(&other.key)
    }
}

#[derive(Debug)]
struct PatchQueue {
    heap: BinaryHeap<QueueEntry>,
}

impl PatchQueue {
    fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
        }
    }

    fn push(&mut self, record: PatchRecord) -> Result<()> {
        let key = match &record.upper {
            Some(value) => nonnegative_upper_bound(value)?,
            None => f64::INFINITY,
        };
        self.heap.try_reserve(1).map_err(Error::from)?;
        self.heap.push(QueueEntry { key, record });

        Ok(())
    }

    fn pop(&mut self) -> Option<PatchRecord> {
        let record = self.heap.pop().map(|entry| entry.record);
        record
    }

    fn max_upper(&self) -> Result<Option<Rational>> {
        let mut maximum = None;
        for entry in &self.heap {
            let Some(upper) = &entry.record.upper else {
                return Ok(None);
            };
            max_rational(&mut maximum, upper.try_clone()?)?;
        }
        Ok(maximum)
    }

    fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}

struct SearchContext<'a> {
    accuracy: &'a Accuracy,
    budget: ExpansionBudget,
    lower_squared: Option<Rational>,
}

impl<'a> SearchContext<'a> {
    fn new(accuracy: &'a Accuracy) -> Self {
        Self {
            accuracy,
            budget: ExpansionBudget::new("hausdorff_distance", "max_error"),
            lower_squared: None,
        }
    }

    fn update_lower(&mut self, squared: Rational) -> Result<()> {
        let replace = match &self.lower_squared {
            None => true,
            Some(current) => squared.try_cmp(current)? == Ordering::Greater,
        };
        if replace {
            self.lower_squared = Some(squared);
        }
        Ok(())
    }

    fn scaled_squared(&self, squared: &Rational) -> Result<Rational> {
        squared.try_product(&self.accuracy.scale_squared)
    }
}

fn sqrt_upper(squared: &Rational, context: &mut SearchContext<'_>) -> Result<SqrtInterval> {
    let mut interval = SqrtInterval::from_squared(squared)?;

    let result = interval.refine(squared, &context.accuracy.error);

    result?;
    Ok(interval)
}

fn sum_upper(left: &SqrtInterval, right: &SqrtInterval) -> Result<Option<Rational>> {
    let (Some(left), Some(right)) = (&left.upper, &right.upper) else {
        return Ok(None);
    };
    let sum = left.try_clone()?.try_add(right.try_clone()?)?;
    Ok(Some(Rational::from_dyadic(sum)?))
}

fn fixed_site_upper(
    patch: &SourcePatch,
    source: &PreparedSupport,
    target: &PreparedSupport,
    nominated: &[PrimitiveId],
    context: &mut SearchContext<'_>,
) -> Result<Option<Rational>> {
    if nominated.is_empty() {
        return Ok(None);
    }
    let corners = patch.corners(source)?;
    let mut best = None;
    for &primitive in nominated {
        let mut site_upper = None;
        for corner in &corners {
            let squared = target.primitive_distance_squared(primitive, corner)?;
            max_rational(&mut site_upper, squared)?;
        }
        min_rational(&mut best, site_upper.expect("fixed site has corners"))?;
    }
    let squared = context.scaled_squared(&best.expect("nonempty nominated sites"))?;
    sqrt_upper(&squared, context)?.upper_rational()
}

fn carrier_line_value(span: &SupportSpan, point: &ExactPoint) -> Result<Rational> {
    let (ax, ay, dx, dy) = carrier_components(span.source)?;
    let px = (&point.x).try_subtract(&ax)?;
    let py = (&point.y).try_subtract(&ay)?;
    (&(&dx).try_product(&py)?).try_subtract(&(&dy).try_product(&px)?)
}

fn exact_parallel(left: &SupportSpan, right: &SupportSpan) -> Result<bool> {
    let (_, _, left_dx, left_dy) = carrier_components(left.source)?;
    let (_, _, right_dx, right_dy) = carrier_components(right.source)?;
    if (left_dx.is_zero() && left_dy.is_zero()) || (right_dx.is_zero() && right_dy.is_zero()) {
        return Ok(false);
    }
    Ok((&(&left_dx).try_product(&right_dy)?)
        .try_subtract(&(&left_dy).try_product(&right_dx)?)?
        .is_zero())
}

fn projection_is_in_span(point: &ExactPoint, span: &SupportSpan) -> Result<bool> {
    let parameter = span_projection_parameter(point, span)?;
    Ok(parameter.try_cmp(&span.t_lo)? != Ordering::Less
        && parameter.try_cmp(&span.t_hi)? != Ordering::Greater)
}

fn signs_between_parallel_lines(first: &[Rational], second: &[Rational]) -> bool {
    let first_positive = first
        .iter()
        .all(|value| value.sign() != ExactSign::Negative);
    let first_negative = first
        .iter()
        .all(|value| value.sign() != ExactSign::Positive);
    let second_positive = second
        .iter()
        .all(|value| value.sign() != ExactSign::Negative);
    let second_negative = second
        .iter()
        .all(|value| value.sign() != ExactSign::Positive);
    (first_positive && second_negative) || (first_negative && second_positive)
}

fn parallel_strip_upper(
    patch: &SourcePatch,
    source: &PreparedSupport,
    target: &PreparedSupport,
    nominated: &[PrimitiveId],
    context: &mut SearchContext<'_>,
) -> Result<Option<Rational>> {
    let mut span_indices = Vec::new();
    try_reserve_vec(&mut span_indices, nominated.len())?;
    for &primitive in nominated {
        if let PrimitiveId::Span(index) = primitive {
            try_push_vec(&mut span_indices, index)?;
        }
    }
    if span_indices.len() < 2 {
        return Ok(None);
    }
    let corners = patch.corners(source)?;
    let mut best = None;
    for (left_offset, &left_index) in span_indices.iter().enumerate() {
        for &right_index in &span_indices[(left_offset + 1)..] {
            let left = &target.spans[left_index];
            let right = &target.spans[right_index];
            if !exact_parallel(left, right)? {
                continue;
            }
            let separation = carrier_line_value(left, &right.start)?;
            let mut left_values = Vec::new();
            let mut right_values = Vec::new();
            try_reserve_vec(&mut left_values, corners.len())?;
            try_reserve_vec(&mut right_values, corners.len())?;
            let mut valid = true;
            for corner in &corners {
                if !projection_is_in_span(corner, left)? || !projection_is_in_span(corner, right)? {
                    valid = false;
                    break;
                }
                let left_value = carrier_line_value(left, corner)?;
                try_push_vec(&mut right_values, (&left_value).try_subtract(&separation)?)?;
                try_push_vec(&mut left_values, left_value)?;
            }
            if !valid || !signs_between_parallel_lines(&left_values, &right_values) {
                continue;
            }
            let (_, _, dx, dy) = carrier_components(left.source)?;
            let direction_squared = (&(&dx).try_square()?).try_add(&(&dy).try_square()?)?;
            if direction_squared.is_zero() {
                continue;
            }
            let separation_squared =
                (&(&separation).try_square()?).try_quotient(&direction_squared)?;
            let half = (&separation_squared).try_quotient(&Rational::from_f64(4.0)?)?;
            let squared = context.scaled_squared(&half)?;
            let Some(upper) = sqrt_upper(&squared, context)?.upper_rational()? else {
                continue;
            };

            min_rational(&mut best, upper)?;
        }
    }
    Ok(best)
}

fn patch_upper(
    patch: &SourcePatch,
    source: &PreparedSupport,
    target: &PreparedSupport,
    context: &mut SearchContext<'_>,
) -> Result<Option<Rational>> {
    let (center, radius_squared) = patch.center_and_radius_squared(source)?;
    let center_squared = target.distance_squared(&center)?;
    let center_squared = context.scaled_squared(&center_squared)?;
    let radius_squared = context.scaled_squared(&radius_squared)?;
    let center_interval = sqrt_upper(&center_squared, context)?;
    let radius_interval = sqrt_upper(&radius_squared, context)?;
    let mut best = sum_upper(&center_interval, &radius_interval)?;

    let nominated = target.index.nominate(&center)?;
    if let Some(fixed) = fixed_site_upper(patch, source, target, &nominated, context)? {
        let replaces = match &best {
            None => true,
            Some(current) => fixed.try_cmp(current)? == Ordering::Less,
        };
        if replaces {
            best = Some(fixed);
        }
    }
    if let Some(strip) = parallel_strip_upper(patch, source, target, &nominated, context)? {
        let replaces = match &best {
            None => true,
            Some(current) => strip.try_cmp(current)? == Ordering::Less,
        };
        if replaces {
            best = Some(strip);
        }
    }
    Ok(best)
}

fn upper_is_below_lower(upper: &Rational, context: &mut SearchContext<'_>) -> Result<bool> {
    let Some(lower) = &context.lower_squared else {
        return Ok(false);
    };
    let upper_squared = (upper).try_square()?;
    Ok(upper_squared.try_cmp(lower)? != Ordering::Greater)
}

fn build_patch(
    patch: SourcePatch,
    source: &PreparedSupport,
    target: &PreparedSupport,
    context: &mut SearchContext<'_>,
) -> Result<Option<PatchRecord>> {
    let Some(witness) = patch.witness(source)? else {
        return Ok(None);
    };
    #[cfg(debug_assertions)]
    debug_assert!(patch.witness_is_valid(source, &witness)?);

    let witness_squared = target.distance_squared(&witness)?;
    let witness_squared = context.scaled_squared(&witness_squared)?;
    context.update_lower(witness_squared)?;
    let upper = patch_upper(&patch, source, target, context)?;
    if let Some(upper_value) = &upper
        && upper_is_below_lower(upper_value, context)?
    {
        return Ok(None);
    }
    Ok(Some(PatchRecord { patch, upper }))
}

fn lower_candidate(context: &mut SearchContext<'_>) -> Result<Option<f64>> {
    let Some(lower) = &context.lower_squared else {
        return Ok(None);
    };
    let lower = lower.try_clone()?;
    let value = lower.round_sqrt()?;
    if value.is_finite() {
        Ok(Some(value))
    } else {
        Ok(None)
    }
}

fn candidate_certified(
    candidate: f64,
    upper: &Rational,
    context: &mut SearchContext<'_>,
) -> Result<bool> {
    let Some(lower_squared) = &context.lower_squared else {
        return Ok(false);
    };
    if !candidate.is_finite() {
        return Ok(false);
    }
    let candidate = Rational::from_f64(candidate)?;
    let high = (&candidate).try_add(&context.accuracy.error)?;
    if upper.try_cmp(&high)? == Ordering::Greater {
        return Ok(false);
    }
    let low = (&candidate).try_subtract(&context.accuracy.error)?;
    if low.sign() != ExactSign::Positive {
        return Ok(true);
    }
    let low_squared = (&low).try_square()?;
    Ok(lower_squared.try_cmp(&low_squared)? != Ordering::Less)
}

fn queue_upper(queue: &PatchQueue, context: &mut SearchContext<'_>) -> Result<Option<Rational>> {
    let Some(lower) = context
        .lower_squared
        .as_ref()
        .map(Rational::try_clone)
        .transpose()?
    else {
        return Ok(None);
    };
    let interval = sqrt_upper(&lower, context)?;
    let Some(witness_upper) = interval.upper_rational()? else {
        return Ok(None);
    };
    if queue.is_empty() {
        return Ok(Some(witness_upper));
    }
    let Some(mut upper) = queue.max_upper()? else {
        return Ok(None);
    };
    if upper.try_cmp(&witness_upper)? == Ordering::Less {
        upper = witness_upper;
    }
    Ok(Some(upper))
}

fn run_direction(
    source: &PreparedSupport,
    target: &PreparedSupport,
    context: &mut SearchContext<'_>,
) -> Result<Rational> {
    let mut queue = PatchQueue::new();
    for patch in source.initial_patches()? {
        if let Some(record) = build_patch(patch, source, target, context)? {
            queue.push(record)?;
        }
    }
    loop {
        let candidate = lower_candidate(context)?;
        let upper = queue_upper(&queue, context)?;
        if let (Some(candidate), Some(upper)) = (candidate, upper)
            && candidate_certified(candidate, &upper, context)?
        {
            return Ok(upper);
        }
        if queue.is_empty() {
            return Err(crate::geometry::GeometryErrorKind::message(
                "hausdorff_distance lower/upper enclosure did not close",
            ));
        }
        let record = queue.pop().expect("nonempty patch queue");

        if let Some(upper) = &record.upper
            && upper_is_below_lower(upper, context)?
        {
            continue;
        }
        let Some(children) = record.patch.split(&mut context.budget)? else {
            return Err(crate::geometry::GeometryErrorKind::message(
                "hausdorff_distance patch reached a nonrepresentable subdivision",
            ));
        };

        for patch in children {
            if let Some(child) = build_patch(patch, source, target, context)? {
                queue.push(child)?;
            }
        }
    }
}

pub(crate) fn distance(left: &Shape, right: &Shape, max_error: f64, scale: f64) -> Result<f64> {
    let accuracy = Accuracy::new(max_error, scale)?;
    let mut context = SearchContext::new(&accuracy);
    if left.is_empty() || right.is_empty() {
        return Err(crate::geometry::GeometryErrorKind::message(
            "hausdorff_distance requires non-empty geometries",
        ));
    }
    let identical = left == right;
    if identical && !left.has_area_parts() {
        return Ok(0.0);
    }
    let source = PreparedSupport::from_shape(left, &mut context.budget)?;
    if identical {
        return Ok(0.0);
    }
    let target = PreparedSupport::from_shape(right, &mut context.budget)?;
    symmetric_distance(&source, &target, &mut context, || left.equals(right))
}

fn symmetric_distance(
    source: &PreparedSupport,
    target: &PreparedSupport,
    context: &mut SearchContext<'_>,
    equal_support: impl FnOnce() -> Result<bool>,
) -> Result<f64> {
    for (source, target) in [(source, target), (target, source)] {
        for span in &source.spans {
            for parameter in [&span.t_lo, &span.t_hi] {
                let point = span_point(span, parameter)?;
                let squared = target.distance_squared(&point)?;
                let squared = context.scaled_squared(&squared)?;
                context.update_lower(squared)?;
            }
        }
        for point in &source.points {
            let squared = target.distance_squared(point)?;
            let squared = context.scaled_squared(&squared)?;
            context.update_lower(squared)?;
        }
    }
    if context
        .lower_squared
        .as_ref()
        .is_some_and(Rational::is_zero)
        && equal_support()?
    {
        return Ok(0.0);
    }
    let forward = run_direction(source, target, context)?;
    let backward = run_direction(target, source, context)?;
    let upper = if forward.try_cmp(&backward)? == Ordering::Greater {
        forward
    } else {
        backward
    };
    let Some(mut candidate) = lower_candidate(context)? else {
        return Err(crate::geometry::GeometryErrorKind::message(
            "hausdorff_distance has no finite output within the requested error",
        ));
    };
    if candidate == 0.0 {
        candidate = f64::from_bits(1);
    }
    if !candidate_certified(candidate, &upper, context)? {
        return Err(crate::geometry::GeometryErrorKind::message(
            "hausdorff_distance cannot represent the requested accuracy",
        ));
    }
    Ok(candidate)
}

pub(crate) fn line_columns(
    left_xs: &[f64],
    left_ys: &[f64],
    right_xs: &[f64],
    right_ys: &[f64],
    max_error: f64,
    scale: f64,
) -> Result<f64> {
    let accuracy = Accuracy::new(max_error, scale)?;
    line_columns_with_accuracy(left_xs, left_ys, right_xs, right_ys, &accuracy)
}

fn line_columns_with_accuracy(
    left_xs: &[f64],
    left_ys: &[f64],
    right_xs: &[f64],
    right_ys: &[f64],
    accuracy: &Accuracy,
) -> Result<f64> {
    let mut context = SearchContext::new(accuracy);
    if left_xs.is_empty() || right_xs.is_empty() {
        return Err(crate::geometry::GeometryErrorKind::message(
            "hausdorff_distance requires non-empty geometries",
        ));
    }
    if left_xs == right_xs && left_ys == right_ys {
        return Ok(0.0);
    }
    let source = PreparedSupport::from_line_columns(left_xs, left_ys)?;
    let target = PreparedSupport::from_line_columns(right_xs, right_ys)?;
    symmetric_distance(&source, &target, &mut context, || {
        let line = |xs: &[f64], ys: &[f64]| -> Result<Shape> {
            let mut points = Vec::new();
            try_reserve_vec(&mut points, xs.len())?;
            for (&x, &y) in xs.iter().zip(ys) {
                try_push_vec(&mut points, XY::new(x, y))?;
            }
            Ok(Shape::LineString(LineSeq::try_new(CoordSeq::try_from_xy(
                &points,
            )?)?))
        };
        line(left_xs, left_ys)?.equals(&line(right_xs, right_ys)?)
    })
}

pub(crate) fn line_columns_batch(
    left: PackedLineColumnView<'_>,
    right: PackedLineColumnView<'_>,
    out: &mut [f64],
    max_error: f64,
    scale: f64,
) -> Result<()> {
    let accuracy = Accuracy::new(max_error, scale)?;
    debug_assert_eq!(left.logical_len(), right.logical_len());
    debug_assert_eq!(left.logical_len(), out.len());
    for (index, value) in out.iter_mut().enumerate() {
        let (left_xs, left_ys) = left.row_xy(index);
        let (right_xs, right_ys) = right.row_xy(index);
        *value = line_columns_with_accuracy(left_xs, left_ys, right_xs, right_ys, &accuracy)?;
    }
    Ok(())
}

#[derive(Debug)]
struct ExactPoint {
    x: Rational,
    y: Rational,
}

impl ExactPoint {
    fn from_xy(point: XY) -> crate::error::Result<Self> {
        Ok(Self {
            x: Rational::from_f64(point.x)?,
            y: Rational::from_f64(point.y)?,
        })
    }

    fn from_homogeneous(value: [ExactDyadic; 3]) -> crate::error::Result<Self> {
        let [x, y, w] = value;
        Ok(Self {
            x: Rational::new(x, w.try_clone()?)?,
            y: Rational::new(y, w)?,
        })
    }

    fn try_clone(&self) -> crate::error::Result<Self> {
        Ok(Self {
            x: self.x.try_clone()?,
            y: self.y.try_clone()?,
        })
    }

    #[cfg(any(test, debug_assertions))]
    fn same_position(&self, other: &Self) -> crate::error::Result<bool> {
        Ok(self.x.try_cmp(&other.x)?.is_eq() && self.y.try_cmp(&other.y)?.is_eq())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::{CoordSeq, LineSeq, Point};

    fn line(vertices: &[(f64, f64)]) -> Shape {
        let points: Vec<_> = vertices
            .iter()
            .map(|&(x, y)| Point::new_raw_xy(x, y))
            .collect();
        Shape::LineString(LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap()).unwrap())
    }

    fn polygon(vertices: &[(f64, f64)]) -> Shape {
        let points: Vec<_> = vertices.iter().map(|&(x, y)| XY::new(x, y)).collect();
        Shape::Polygon(
            crate::geometry::Polygon::new(
                crate::geometry::Ring::from_closed(CoordSeq::try_from_xy(&points).unwrap()),
                Vec::new(),
            )
            .unwrap(),
        )
    }

    #[test]
    fn queue_enclosure_keeps_unbounded_patches_and_later_witnesses() {
        let accuracy = Accuracy::new(1e-6, 1.0).unwrap();
        let mut context = SearchContext::new(&accuracy);
        let mut queue = PatchQueue::new();
        let patch = || {
            SourcePatch::Box(FloatBox {
                min_x: 0.0,
                max_x: 1.0,
                min_y: 0.0,
                max_y: 1.0,
            })
        };
        queue
            .push(PatchRecord {
                patch: patch(),
                upper: Some(Rational::one()),
            })
            .unwrap();
        context
            .update_lower(Rational::from_f64(4.0).unwrap())
            .unwrap();
        let upper = queue_upper(&queue, &mut context).unwrap().unwrap();
        assert_ne!(
            upper.try_cmp(&Rational::from_f64(2.0).unwrap()).unwrap(),
            Ordering::Less
        );
        queue
            .push(PatchRecord {
                patch: patch(),
                upper: None,
            })
            .unwrap();
        assert!(queue_upper(&queue, &mut context).unwrap().is_none());
    }

    #[test]
    fn box_radius_covers_unrepresentable_midpoint_corners() {
        let source = PreparedSupport::from_line_columns(&[0.0], &[0.0]).unwrap();
        let patch = SourcePatch::Box(FloatBox {
            min_x: 1.0,
            max_x: 1.0_f64.next_up(),
            min_y: 0.0,
            max_y: f64::from_bits(1),
        });
        let (center, radius) = patch.center_and_radius_squared(&source).unwrap();
        for corner in patch.corners(&source).unwrap() {
            let dx = corner.x.try_subtract(&center.x).unwrap();
            let dy = corner.y.try_subtract(&center.y).unwrap();
            let squared = dx
                .try_square()
                .unwrap()
                .try_add(&dy.try_square().unwrap())
                .unwrap();
            assert_ne!(squared.try_cmp(&radius).unwrap(), Ordering::Greater);
        }
    }

    #[test]
    fn floating_box_pruning_stays_below_exact_distance() {
        let target = FloatBox {
            min_x: -2.0,
            max_x: -1.0,
            min_y: 1.0,
            max_y: 2.0,
        };
        for (x, y) in [
            (0.0, 0.0),
            (0.1, -0.3),
            (f64::from_bits(1), 0.0),
            (f64::MAX, -f64::MAX),
        ] {
            let point = exact_point_xy(x, y).unwrap();
            let lower = box_distance_lower(FloatBox::from_point(&point).unwrap(), target);
            let exact = aabb_lower_squared(&point, target).unwrap();
            assert_ne!(
                Rational::from_f64(lower).unwrap().try_cmp(&exact).unwrap(),
                Ordering::Greater,
            );
        }
    }

    #[test]
    fn empty_area_patch_cannot_invent_a_source_witness() {
        let shape = polygon(&[(0.0, 0.0), (4.0, 0.0), (0.0, 3.0), (0.0, 0.0)]);
        let accuracy = Accuracy::new(1e-6, 1.0).unwrap();
        let mut context = SearchContext::new(&accuracy);
        let source = PreparedSupport::from_shape(&shape, &mut context.budget).unwrap();
        let target = PreparedSupport::from_line_columns(&[1.0], &[1.0]).unwrap();
        let patch = SourcePatch::Box(FloatBox {
            min_x: 10.0,
            max_x: 11.0,
            min_y: 10.0,
            max_y: 11.0,
        });
        assert!(
            build_patch(patch, &source, &target, &mut context)
                .unwrap()
                .is_none()
        );
        assert!(context.lower_squared.is_none());
        let span = SourcePatch::Span {
            span: 0,
            lo: Rational::zero(),
            hi: Rational::one(),
        };
        assert!(
            !span
                .witness_is_valid(&source, &exact_point_xy(10.0, 10.0).unwrap())
                .unwrap()
        );
    }

    #[test]
    fn subdivision_uses_the_shared_generated_work_budget() {
        let mut budget = ExpansionBudget::new("hausdorff_distance", "max_error");
        budget
            .add(crate::geometry::generated_budget::GENERATED_ITEM_LIMIT)
            .unwrap();
        let patch = SourcePatch::Box(FloatBox {
            min_x: 0.0,
            max_x: 1.0,
            min_y: 0.0,
            max_y: 1.0,
        });
        let error = patch.split(&mut budget).unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(
                crate::geometry::GeometryErrorKind::GeneratedOutputTooLarge { .. }
            )
        ));
    }

    #[test]
    fn parallel_strip_rejects_outside_points_with_reversed_carriers() {
        let target =
            PreparedSupport::from_line_columns(&[0.0, 4.0, 4.0, 0.0], &[0.0, 0.0, 2.0, 2.0])
                .unwrap();
        let source = PreparedSupport::from_line_columns(&[2.0], &[1.0]).unwrap();
        let accuracy = Accuracy::new(1e-6, 1.0).unwrap();
        let mut context = SearchContext::new(&accuracy);
        for (y, inside) in [(1.0, true), (3.0, false), (-1.0, false)] {
            let patch = SourcePatch::Box(FloatBox {
                min_x: 2.0,
                max_x: 2.0,
                min_y: y,
                max_y: y,
            });
            let upper = parallel_strip_upper(
                &patch,
                &source,
                &target,
                &[PrimitiveId::Span(0), PrimitiveId::Span(2)],
                &mut context,
            )
            .unwrap();
            if inside {
                assert_eq!(
                    upper.unwrap().try_cmp(&Rational::one()).unwrap(),
                    Ordering::Equal
                );
            } else {
                assert!(upper.is_none());
            }
        }
    }

    #[test]
    fn selected_span_clamps_oblique_projection() {
        let mut support = PreparedSupport::from_line_columns(&[0.0, 3.0], &[0.0, 4.0]).unwrap();
        for (lo, hi, x, y, expected) in [
            (0.0, 1.0, 1.0, 3.0, 1.0),
            (0.0, 1.0, -3.0, -4.0, 25.0),
            (0.0, 1.0, 6.0, 8.0, 25.0),
            (0.25, 0.75, 0.0, 0.0, 1.5625),
            (0.25, 0.75, 3.0, 4.0, 1.5625),
        ] {
            support.spans[0].t_lo = Rational::from_f64(lo).unwrap();
            support.spans[0].t_hi = Rational::from_f64(hi).unwrap();
            let value =
                point_span_distance_squared(&exact_point_xy(x, y).unwrap(), &support.spans[0])
                    .unwrap();
            assert_eq!(
                value
                    .try_cmp(&Rational::from_f64(expected).unwrap())
                    .unwrap(),
                Ordering::Equal
            );
        }
    }

    #[test]
    fn filled_target_membership_and_holes_remain_selected() {
        let ring = |points: &[XY]| {
            crate::geometry::Ring::from_closed(CoordSeq::try_from_xy(points).unwrap())
        };
        let shell = [
            XY::new(0.0, 0.0),
            XY::new(4.0, 0.0),
            XY::new(4.0, 4.0),
            XY::new(0.0, 4.0),
            XY::new(0.0, 0.0),
        ];
        let target = Shape::Polygon(
            crate::geometry::Polygon::new(ring(&shell), vec![
                ring(&[
                    XY::new(1.0, 1.0),
                    XY::new(3.0, 1.0),
                    XY::new(3.0, 3.0),
                    XY::new(1.0, 3.0),
                    XY::new(1.0, 1.0),
                ]),
                ring(&[
                    XY::new(6.0, 6.0),
                    XY::new(7.0, 6.0),
                    XY::new(7.0, 7.0),
                    XY::new(6.0, 7.0),
                    XY::new(6.0, 6.0),
                ]),
            ])
            .unwrap(),
        );
        let mut budget = ExpansionBudget::new("test", "support");
        let support = PreparedSupport::from_shape(&target, &mut budget).unwrap();
        assert!(
            support
                .distance_squared(&exact_point_xy(0.5, 0.5).unwrap())
                .unwrap()
                .is_zero()
        );
        assert_eq!(
            support
                .distance_squared(&exact_point_xy(2.0, 2.0).unwrap())
                .unwrap()
                .round()
                .unwrap(),
            1.0
        );
        let filled =
            Shape::Polygon(crate::geometry::Polygon::new(ring(&shell), Vec::new()).unwrap());
        assert!((distance(&filled, &target, 1e-6, 1.0).unwrap() - 1.0).abs() <= 1e-6);
    }

    #[test]
    fn allocation_failure_never_publishes_a_scalar() {
        let source = line(&[(0.0, 0.0), (2.0, 0.0)]);
        let target = Shape::Point(Point::new_raw_xy(1.0, 1.0));
        let (result, count) =
            crate::test_support::allocation_count(|| distance(&source, &target, 1e-6, 1.0));
        result.unwrap();
        assert!(count > 0);
        for allowed in 0..=count {
            match crate::test_support::fail_allocations_after(allowed, || {
                distance(&source, &target, 1e-6, 1.0)
            }) {
                Err(error) => assert!(matches!(
                    error.kind(),
                    crate::error::ErrorKind::Resource(crate::error::ResourceError::Allocation)
                )),
                Ok(value) => assert!((value - 2.0_f64.sqrt()).abs() <= 1e-6),
            }
        }
    }

    #[test]
    fn output_units_preserve_extreme_finite_distances() {
        for (coordinate, scale) in [
            (2.0_f64.powi(1023), 2.0_f64.powi(-1023)),
            (2.0_f64.powi(-1023), 2.0_f64.powi(1023)),
        ] {
            let left = Shape::Point(Point::new_raw_xy(-coordinate, 0.0));
            let right = Shape::Point(Point::new_raw_xy(coordinate, 0.0));
            assert_eq!(distance(&left, &right, 1e-9, scale).unwrap(), 2.0);
        }
    }

    #[test]
    fn filled_interior_and_parallel_plateau_obey_the_error_bound() {
        for vertices in [vec![(0.0, 0.0), (4.0, 0.0), (0.0, 3.0), (0.0, 0.0)], vec![
            (0.0, 0.0),
            (100.0, 0.0),
            (100.0, 2.0),
            (0.0, 2.0),
            (0.0, 0.0),
        ]] {
            let value = distance(&polygon(&vertices), &line(&vertices), 1e-6, 1.0).unwrap();
            assert!((value - 1.0).abs() <= 1e-6, "{value}");
        }
    }

    #[test]
    fn zero_distinguishes_equal_support_from_close_support() {
        let baseline = line(&[(0.0, 0.0), (2.0, 0.0)]);
        let segmented = line(&[(2.0, 0.0), (1.0, 0.0), (0.0, 0.0)]);
        assert_eq!(distance(&baseline, &segmented, 1e-3, 1.0).unwrap(), 0.0);
        let nearby = line(&[(0.0, 1e-9), (2.0, 1e-9)]);
        let value = distance(&baseline, &nearby, 1e-3, 1.0).unwrap();
        assert!(value > 0.0);
        assert!((value - 1e-9).abs() <= 1e-3);
        let vertices = [(0.0, 0.0), (4.0, 0.0), (0.0, 3.0), (0.0, 0.0)];
        let value = distance(&polygon(&vertices), &line(&vertices), 2.0, 1.0).unwrap();
        assert!(value > 0.0);
        assert!((value - 1.0).abs() <= 2.0);
    }

    #[test]
    fn identical_empty_support_is_still_undefined() {
        assert!(distance(&Shape::empty_point(), &Shape::empty_point(), 1e-3, 1.0).is_err());
    }

    #[test]
    fn identical_cancelled_polygon_support_is_undefined() {
        let ring = crate::geometry::Ring::from_closed(
            CoordSeq::try_from_xy(&[
                XY::new(0.0, 0.0),
                XY::new(1.0, 0.0),
                XY::new(2.0, 0.0),
                XY::new(0.0, 0.0),
            ])
            .unwrap(),
        );
        let shape =
            Shape::Polygon(crate::geometry::Polygon::new(ring.clone(), vec![ring]).unwrap());
        assert!(distance(&shape, &shape, 1e-6, 1.0).is_err());
    }

    #[test]
    fn invalid_accuracy_is_rejected_before_identity() {
        let point = Shape::Point(Point::new_raw_xy(3.0, 4.0));
        for error in [0.0, -1.0, f64::INFINITY, f64::NAN] {
            assert!(distance(&point, &point, error, 1.0).is_err());
        }
    }
}
