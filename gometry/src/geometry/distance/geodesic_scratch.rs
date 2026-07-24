use super::*;
#[derive(Default)]
pub(crate) struct GeodesicScratch {
    pub(crate) left_points: Vec<Point>,
    pub(crate) right_points: Vec<Point>,
    pub(crate) left_edges: Vec<GeodesicSegment>,
    pub(crate) right_edges: Vec<GeodesicSegment>,
    pub(crate) left_point_only: Vec<Point>,
    pub(crate) right_point_only: Vec<Point>,
    pub(crate) cap_lengths: Vec<f64>,
    pub(crate) cap_groups: Vec<CapGroup>,
    pub(crate) rows: Vec<RowProbe>,
    pub(crate) stack: Vec<u32>,
}

impl GeodesicScratch {
    const fn new() -> Self {
        Self {
            left_points: Vec::new(),
            right_points: Vec::new(),
            left_edges: Vec::new(),
            right_edges: Vec::new(),
            left_point_only: Vec::new(),
            right_point_only: Vec::new(),
            cap_lengths: Vec::new(),
            cap_groups: Vec::new(),
            rows: Vec::new(),
            stack: Vec::new(),
        }
    }

    fn clear(&mut self) {
        self.left_points.clear();
        self.right_points.clear();
        self.left_edges.clear();
        self.right_edges.clear();
        self.left_point_only.clear();
        self.right_point_only.clear();
        self.cap_lengths.clear();
        self.cap_groups.clear();
        self.rows.clear();
        self.stack.clear();
    }
}

thread_local! {
    static GEODESIC_SCRATCH: std::cell::RefCell<GeodesicScratch> =
        const { std::cell::RefCell::new(GeodesicScratch::new()) };
}

pub(crate) struct GeodesicScratchGuard {
    pub(crate) scratch: GeodesicScratch,
}

impl GeodesicScratchGuard {
    pub(crate) fn take() -> Self {
        Self {
            scratch: GEODESIC_SCRATCH.with(|scratch| std::mem::take(&mut *scratch.borrow_mut())),
        }
    }
}

impl Drop for GeodesicScratchGuard {
    fn drop(&mut self) {
        self.scratch.clear();
        GEODESIC_SCRATCH.with(|scratch| *scratch.borrow_mut() = std::mem::take(&mut self.scratch));
    }
}

pub(crate) fn collect_geodesic_segments_into(
    shape: &Shape,
    metric: &impl GeodesicMetric,
    out: &mut Vec<GeodesicSegment>,
) {
    out.clear();
    out.reserve(shape.segment_count());
    shape.for_each_vertex_pair(|start, end| out.push(metric.make_segment(start, end)));
}

pub(crate) fn collect_point_only_into(shape: &Shape, out: &mut Vec<Point>) {
    out.clear();
    match shape {
        Shape::Point(point) => out.push(*point),
        Shape::MultiPoint(points) => out.extend(points.iter()),
        Shape::GeometryCollection(geometries) => {
            for geometry in geometries {
                collect_point_only_into(geometry, out);
            }
        },
        _ => {},
    }
}
