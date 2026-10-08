use crate::error::Result;
use crate::geographic::lonlat::geographic_admission_error;
use crate::geographic::{GeographicFrame, ScalarGeographicView};
use crate::geometry::overlay::ExactSourceInventory;
use crate::geometry::relate::De9im;
use crate::geometry::{
    Bounds, Coordinates as _, ExactGeographicTopology, GeographicTopologyRoute, Loc, Point,
    PreparedPlan, Ring, Shape, storage,
};
#[cfg(test)]
use crate::geometry::{CoordSeq, Polygon, XY, predicates};

#[derive(Clone, Copy)]
pub(crate) struct GeographicPrepared<'a> {
    plan: &'a PreparedPlan,
    view: ScalarGeographicView<'a>,
    bounds_sound: bool,
    chart_route: GeographicTopologyRoute,
}

impl<'a> GeographicPrepared<'a> {
    /// The operand's plan and frame, for a caller running the pre-topology
    /// gates before admitting this pair to a relation.
    pub(crate) const fn plan(&self) -> &'a PreparedPlan {
        self.plan
    }

    pub(crate) const fn frame(&self) -> GeographicFrame {
        self.view.frame()
    }

    pub(crate) const fn view(self) -> ScalarGeographicView<'a> {
        self.view
    }

    /// Whether the operand's raw box stands in for it in this frame.
    pub(crate) const fn bounds_sound(&self) -> bool {
        self.bounds_sound
    }

    pub(crate) const fn chart_route(&self) -> GeographicTopologyRoute {
        self.chart_route
    }

    /// Admit `plan.shape()` once per frame; later pairs reuse the admission
    /// and its box fact from the plan's frame cache.
    pub(crate) fn try_new(
        plan: &'a PreparedPlan,
        frame_cache: &'a crate::geometry::FrameDependentCaches,
        frame: GeographicFrame,
    ) -> crate::error::Result<Self> {
        if let Some((bounds_sound, chart_route)) = frame_cache.geographic_admission(frame) {
            return Ok(Self {
                plan,
                view: ScalarGeographicView::previously_admitted(frame, plan.shape()),
                bounds_sound,
                chart_route,
            });
        }
        let view = ScalarGeographicView::try_new(frame, plan.shape())
            .map_err(|diagnostic| geographic_admission_error(&diagnostic))?;
        let chart_route = crate::geometry::classify_geographic_chart(view);
        let bounds_sound = plan.bounds().is_some_and(|bounds| {
            crate::geometry::raw_coordinate_bounds_are_sound_in_frame(
                Some(plan.shape()),
                bounds,
                frame,
            )
        });
        frame_cache.record_admission(frame, bounds_sound, chart_route);
        Ok(Self {
            plan,
            view,
            bounds_sound,
            chart_route,
        })
    }
}

impl GeographicPrepared<'_> {
    pub(crate) fn exact_topology(self) -> Result<std::sync::Arc<ExactGeographicTopology>> {
        self.plan.geographic_exact_topology(self.view)
    }

    /// Exact set membership of an admitted point in this prepared frame.
    pub(crate) fn point_covers(self, point: Point) -> Result<bool> {
        let Some(bounds) = self.plan.bounds() else {
            return Ok(false);
        };
        let longitude = self
            .frame()
            .longitude()
            .canonicalize(point.x)
            .expect("the query point was admitted in the prepared geographic frame");
        let point = Point::new_raw_xy(longitude.get(), point.y);
        if self.chart_route == GeographicTopologyRoute::ExactPartition {
            if point.x < bounds.minx()
                || point.x > bounds.maxx()
                || point.y < bounds.miny()
                || point.y > bounds.maxy()
            {
                return Ok(false);
            }
            return self.plan.cartesian_point_covers(point);
        }
        // Chart linework cannot leave its source latitude interval. Polygon
        // interiors may enclose a pole beyond their boundary vertices.
        if !self.plan.shape().has_area_parts()
            && (point.y < bounds.miny() || point.y > bounds.maxy())
        {
            return Ok(false);
        }
        Ok(self.exact_topology()?.locate_point(point)?.geometry != Loc::Exterior)
    }
}

/// Compute the complete Cartesian relation matrix from exact source witnesses.
pub(crate) fn graph_relate_shapes(left: &Shape, right: &Shape) -> Result<De9im> {
    graph_relate_data(
        &PreparedPlan::transient(left),
        &PreparedPlan::transient(right),
    )
}

/// The geographic relation tests read: a degree frame stands in for a CRS,
/// which is why this helper never leaves the test build.
#[cfg(test)]
fn graph_relate_shapes_geographic(left: &Shape, right: &Shape) -> De9im {
    let unit =
        crate::geographic::AngularFactor::try_new("test degrees", std::f64::consts::PI / 180.0)
            .unwrap();
    let frame = GeographicFrame::try_new(unit, unit, 0.0).unwrap();
    let left = PreparedPlan::transient(left);
    let right = PreparedPlan::transient(right);
    let left_cache = crate::geometry::FrameDependentCaches::default();
    let right_cache = crate::geometry::FrameDependentCaches::default();
    graph_relate_data_geographic_q(
        GeographicPrepared::try_new(&left, &left_cache, frame).expect("fixture admits"),
        GeographicPrepared::try_new(&right, &right_cache, frame).expect("fixture admits"),
    )
    .unwrap()
}

pub(crate) fn graph_relate_data(left: &PreparedPlan, right: &PreparedPlan) -> Result<De9im> {
    super::evaluator::relate(left, right)
}

/// The global partition relation remains the owner for geographic chart pairs
/// that require its domain-specific face topology.
fn exact_partition_relate<'a>(
    left: impl Into<storage::ShapeRow<'a>>,
    right: impl Into<storage::ShapeRow<'a>>,
) -> Result<De9im> {
    let inventory =
        ExactSourceInventory::from_rows_with_occurrences([left.into(), right.into()].into_iter())?;
    let mut budget = crate::geometry::ExpansionBudget::new("relate", "exact partition");
    let partition = inventory.partition(&mut budget)?;
    inventory.relate(&partition)
}

#[cfg(test)]
pub(crate) fn partition_relate_data(left: &PreparedPlan, right: &PreparedPlan) -> Result<De9im> {
    exact_partition_relate(left.shape(), right.shape())
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "GeographicPrepared is a Copy view of three borrows; the relate functions take the completed prepare step by value"
)]
pub(crate) fn graph_relate_data_geographic(
    left: GeographicPrepared<'_>,
    right: GeographicPrepared<'_>,
) -> Result<De9im> {
    debug_assert_eq!(left.frame(), right.frame());
    if left.chart_route().combine(right.chart_route()) == GeographicTopologyRoute::ExactPartition {
        return exact_partition_relate(left.plan().shape(), right.plan().shape());
    }
    graph_relate_data_geographic_q(left, right)
}

/// Set intersection is insensitive to repeated shared collection owners.
/// Full DE-9IM relations keep their occurrence-aware source inventory.
pub(crate) fn graph_intersects_data_geographic(
    left: &GeographicPrepared<'_>,
    right: &GeographicPrepared<'_>,
) -> Result<bool> {
    debug_assert_eq!(left.frame(), right.frame());
    if let Shape::Point(point) = left.plan().shape() {
        return right.point_covers(*point);
    }
    if let Shape::Point(point) = right.plan().shape() {
        return left.point_covers(*point);
    }
    if left.chart_route().combine(right.chart_route()) == GeographicTopologyRoute::ExactPartition {
        let shapes = [left.plan().shape(), right.plan().shape()];
        let inventory = ExactSourceInventory::from_shapes(&shapes)?;
        let mut budget =
            crate::geometry::ExpansionBudget::new("geographic intersects", "exact partition");
        let partition = inventory.partition(&mut budget)?;
        return Ok(inventory.relate(&partition)?.is_intersects());
    }
    ExactGeographicTopology::from_views_as_set(
        left.frame(),
        [left.view(), right.view()].into_iter(),
    )?
    .relate_pair()
    .map(|relation| relation.is_intersects())
}

fn graph_relate_data_geographic_q(
    left: GeographicPrepared<'_>,
    right: GeographicPrepared<'_>,
) -> Result<De9im> {
    debug_assert_eq!(left.frame(), right.frame());
    ExactGeographicTopology::from_views(left.frame(), [left.view(), right.view()].into_iter())?
        .relate_pair()
}

/// A closed ring with real extent and a nonzero exact area.
pub(crate) fn ring_has_area(ring: &Ring) -> bool {
    ring.coord_count() >= Ring::MIN_VERTICES_CLOSED
        && !crate::geometry::ring_winding(ring).is_degenerate()
}

/// Whether `shape` is exactly the box `bounds`: a hole-free polygon whose every
/// vertex sits on the box, whose every step runs along one of its sides, which
/// reaches all four corners, and whose winding encloses the box. Extra
/// collinear vertices along a side are admitted; a degenerate box, or a ring
/// whose traversals cancel, is not.
pub(crate) fn is_axis_aligned_rectangle(shape: &Shape, bounds: Bounds) -> bool {
    is_axis_aligned_rectangle_row(storage::ShapeRow::Shape(shape), bounds)
}

/// Whether a storage row is one exactly filled axis-aligned rectangle.
pub(crate) fn is_axis_aligned_rectangle_row(row: storage::ShapeRow<'_>, bounds: Bounds) -> bool {
    let Some(mut rings) = row.polygon_rings() else {
        return false;
    };
    let Some(shell) = rings.next() else {
        return false;
    };
    if rings.next().is_some() {
        return false;
    }
    is_axis_aligned_rectangle_ring(&shell, bounds)
}

#[expect(
    clippy::float_cmp,
    reason = "rectangle certification compares represented coordinates with their exact bounds"
)]
fn is_axis_aligned_rectangle_ring(
    shell: &(impl crate::geometry::Coordinates + ?Sized),
    bounds: Bounds,
) -> bool {
    if bounds.minx() == bounds.maxx() || bounds.miny() == bounds.maxy() {
        return false;
    }
    if shell.coord_count() < 5 {
        return false;
    }
    let Some((xs, ys)) = shell.xy_columns() else {
        return false;
    };
    let mut seen = [false; 4];
    for (&x, &y) in xs
        .iter()
        .zip(ys)
        .take(shell.coord_count().saturating_sub(1))
    {
        let on_vertical = x == bounds.minx() || x == bounds.maxx();
        let on_horizontal = y == bounds.miny() || y == bounds.maxy();
        if !on_vertical && !on_horizontal {
            return false;
        }
        if on_vertical && on_horizontal {
            match (x == bounds.maxx(), y == bounds.maxy()) {
                (false, false) => seen[0] = true,
                (false, true) => seen[1] = true,
                (true, false) => seen[2] = true,
                (true, true) => seen[3] = true,
            }
        }
    }
    if xs
        .array_windows::<2>()
        .zip(ys.array_windows::<2>())
        .any(|([x0, x1], [y0, y1])| x0 != x1 && y0 != y1)
    {
        return false;
    }
    seen.iter().all(|&corner| corner) && !crate::geometry::ring_winding(shell).is_degenerate()
}

#[cfg(test)]
mod tests;
