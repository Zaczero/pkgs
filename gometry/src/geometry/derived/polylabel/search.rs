use super::{
    AxisFrame, BVH_MIN_INDEXED_SEGMENTS, BinaryHeap, Bounds, ExpansionBudget, FacetBvh, Ordering,
    Point, PreparedLinework, PreparedSupport, Result, Shape, degenerate_polygon_center,
    framed_metric_segment_distance, label_carrier_enclosure, representative_point,
};

#[derive(Clone, Copy)]
pub(super) struct LabelCell {
    center: Point,
    half_x: f64,
    half_y: f64,
    distance: f64,
    upper: f64,
}

impl PartialEq for LabelCell {
    fn eq(&self, other: &Self) -> bool {
        self.upper.to_bits() == other.upper.to_bits()
    }
}

impl Eq for LabelCell {}

impl Ord for LabelCell {
    fn cmp(&self, other: &Self) -> Ordering {
        self.upper.total_cmp(&other.upper)
    }
}

impl PartialOrd for LabelCell {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub(super) struct PolylabelField<'a> {
    shape: &'a Shape,
    linework: PreparedLinework,
    bvh: Option<FacetBvh>,
    stack: Vec<u32>,
    frame: Option<AxisFrame>,
    upper: f64,
}

impl PolylabelField<'_> {
    pub(super) fn metric(&self, x: f64, y: f64) -> f64 {
        self.frame.map_or_else(
            || x.hypot(y),
            |frame| (x / frame.scale_x()).hypot(y / frame.scale_y()),
        )
    }

    pub(super) fn boundary_distance(&mut self, point: Point, bound: f64) -> Result<f64> {
        if let Some(frame) = self.frame {
            let mut best = f64::INFINITY;
            self.linework.for_each_segment(|segment| {
                best = best.min(framed_metric_segment_distance(frame, point, segment));
            });
            return Ok(best);
        }
        let squared = bound * bound;
        let squared = if bound > 0.0 && squared == 0.0 {
            f64::INFINITY
        } else {
            squared
        };
        Ok(match &self.bvh {
            Some(bvh) => bvh.min_point_distance_with_stack::<true>(
                &self.linework,
                point.x,
                point.y,
                &mut self.stack,
                squared,
            )?,
            None => self
                .linework
                .min_points_distance::<true>(std::iter::once((point.x, point.y)), squared)?,
        }
        .sqrt())
    }

    pub(super) fn cell(
        &mut self,
        center: Point,
        half_x: f64,
        half_y: f64,
        parent: Option<&LabelCell>,
    ) -> Result<LabelCell> {
        let (bound, sign) = parent.map_or((f64::INFINITY, None), |parent| {
            let gap = self.metric(center.x - parent.center.x, center.y - parent.center.y);
            (
                (parent.distance.abs() + gap) * (1.0 + 1e-9),
                (parent.distance.abs() > gap).then_some(parent.distance > 0.0),
            )
        });
        let distance = self.boundary_distance(center, bound)?;
        let inside = sign.unwrap_or_else(|| self.shape.contains_point(center));
        let distance = if inside { distance } else { -distance };
        Ok(LabelCell {
            center,
            half_x,
            half_y,
            distance,
            upper: (distance + self.metric(half_x, half_y)).min(self.upper),
        })
    }
}

pub(super) fn polylabel_search(
    shape: &Shape,
    bounds: Bounds,
    frame: Option<AxisFrame>,
    tolerance: f64,
) -> Result<Point> {
    let center = degenerate_polygon_center(bounds);
    // Midpoint rounding can make the two sides differ by an ulp. Cover
    // both extrema rather than assuming a perfectly centered rectangle.
    let half_x = (center.x - bounds.minx())
        .max(bounds.maxx() - center.x)
        .next_up();
    let half_y = (center.y - bounds.miny())
        .max(bounds.maxy() - center.y)
        .next_up();
    let world_x = frame.map_or(half_x, |frame| half_x / frame.scale_x());
    let world_y = frame.map_or(half_y, |frame| half_y / frame.scale_y());
    let upper = world_x.min(world_y);
    let linework = PreparedLinework::build(shape)?;
    let bvh = (frame.is_none() && linework.segment_count() >= BVH_MIN_INDEXED_SEGMENTS)
        .then(|| FacetBvh::build(&linework))
        .flatten();
    let mut field = PolylabelField {
        shape,
        linework,
        bvh,
        stack: Vec::new(),
        frame,
        upper,
    };
    let mut best = field.cell(representative_point(shape, bounds)?, 0.0, 0.0, None)?;
    let root = field.cell(center, half_x, half_y, None)?;
    if root.distance > best.distance {
        best = root;
    }
    if upper - best.distance <= tolerance {
        return Ok(best.center);
    }
    if best.distance <= 0.0 {
        let mut budget = ExpansionBudget::new("polylabel", "tolerance");
        if PreparedSupport::from_shape_with_boundary(shape, &mut budget)?
            .is_none_or(|source| !source.has_positive_area())
        {
            return Ok(center);
        }
    }
    if world_x > 2.0 * world_y || world_y > 2.0 * world_x {
        let dx = frame.map_or(upper, |frame| upper * frame.scale_x());
        let dy = frame.map_or(upper, |frame| upper * frame.scale_y());
        let ends = if world_x > world_y {
            [
                Point::new_raw_xy(bounds.minx() + dx, center.y),
                Point::new_raw_xy(bounds.maxx() - dx, center.y),
            ]
        } else {
            [
                Point::new_raw_xy(center.x, bounds.miny() + dy),
                Point::new_raw_xy(center.x, bounds.maxy() - dy),
            ]
        };
        for point in ends {
            let seed = field.cell(point, 0.0, 0.0, None)?;
            if seed.distance > best.distance {
                best = seed;
            }
        }
    }
    if field.bvh.is_none()
        && frame.is_none()
        && upper - best.distance > tolerance
        && let Some((upper, center)) =
            label_carrier_enclosure(&field.linework, best.center, tolerance)
    {
        field.upper = field.upper.min(upper);
        let seed = field.cell(center, 0.0, 0.0, None)?;
        if seed.distance > best.distance {
            best = seed;
        }
    }
    if field.upper - best.distance <= tolerance {
        return Ok(best.center);
    }
    let mut budget = ExpansionBudget::new("polylabel", "tolerance");
    let mut cells = BinaryHeap::new();
    cells.push(LabelCell {
        upper: root.upper.min(field.upper),
        ..root
    });
    budget.add(1)?;
    while let Some(cell) = cells.pop() {
        if cell.distance > best.distance {
            best = cell;
        }
        if cell.upper - best.distance <= tolerance {
            continue;
        }
        let x = frame.map_or(cell.half_x, |frame| cell.half_x / frame.scale_x());
        let y = frame.map_or(cell.half_y, |frame| cell.half_y / frame.scale_y());
        let split_x = x >= y;
        let split_both = x <= 2.0 * y && y <= 2.0 * x;
        let hx = if split_x || split_both {
            cell.half_x / 2.0
        } else {
            cell.half_x
        };
        let hy = if !split_x || split_both {
            cell.half_y / 2.0
        } else {
            cell.half_y
        };
        if hx <= 0.0 || hy <= 0.0 {
            continue;
        }
        budget.add(if split_both { 4 } else { 2 })?;
        for sx in [-1.0, 1.0] {
            let dx = if split_x || split_both { sx * hx } else { 0.0 };
            for sy in [-1.0, 1.0] {
                let dy = if !split_x || split_both { sy * hy } else { 0.0 };
                cells.push(field.cell(
                    Point::new_raw_xy(cell.center.x + dx, cell.center.y + dy),
                    hx,
                    hy,
                    Some(&cell),
                )?);
                if split_x && !split_both {
                    break;
                }
            }
            if !split_x && !split_both {
                break;
            }
        }
    }
    Ok(best.center)
}
