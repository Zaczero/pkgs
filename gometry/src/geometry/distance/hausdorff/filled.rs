//! Filled-set Hausdorff distance.
//!
//! The ordinary Hausdorff kernel is a continuum solver for linework.  A
//! polygon adds one more part to that continuum: its area.  The boundary
//! sweep remains the exact answer on a source boundary; this module finds the
//! only additional candidates, local maxima of the distance field in a
//! source interior.  A cell queue supplies the admissible localization and a
//! small line/point-site solver finishes the localized problem without
//! refining a flat medial-axis ridge to the last bit.

#![allow(
    clippy::float_cmp,
    clippy::large_types_passed_by_value,
    reason = "boundary certificates require exact binary64 coordinate equality, and the Copy site snapshots stay by value in the hot local solver"
)]

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use super::{HausdorffFeature, HausdorffTarget, quadratic_roots};
use crate::error::Result;
use crate::geometry::predicates::shell_is_convex;
use crate::geometry::{
    Bounds, Coordinates as _, Orientation, Point, RectContact, Segment, Shape, XY, orientation,
    point_distance, point_on_segment, point_segment_distance, ring_winding, segment_cross_point,
    segment_vs_rect,
};

const MAX_REFINEMENT_DEPTH: u8 = 48;
const GLOBAL_LOCAL_SOLVE_FEATURE_LIMIT: usize = 32;
const EXTENDED_GLOBAL_LOCAL_SOLVE_FEATURE_LIMIT: usize = 64;
const PRELOCAL_SITE_LIMIT: usize = 24;

#[derive(Clone, Copy, Debug)]
struct FilledCell {
    min_x: f64,
    max_x: f64,
    min_y: f64,
    max_y: f64,
    upper: f64,
    depth: u8,
}

impl Eq for FilledCell {}

impl PartialEq for FilledCell {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Ord for FilledCell {
    fn cmp(&self, other: &Self) -> Ordering {
        self.upper
            .total_cmp(&other.upper)
            .then_with(|| other.depth.cmp(&self.depth))
            .then_with(|| self.min_x.total_cmp(&other.min_x))
            .then_with(|| self.max_x.total_cmp(&other.max_x))
            .then_with(|| self.min_y.total_cmp(&other.min_y))
            .then_with(|| self.max_y.total_cmp(&other.max_y))
    }
}

impl PartialOrd for FilledCell {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Copy)]
struct LineSite {
    /// Unit normal and signed offset in coordinates relative to `origin`.
    a: f64,
    b: f64,
    c: f64,
    start: XY,
    end: XY,
    segment: Segment,
}

#[derive(Clone, Copy)]
enum Site {
    Point(XY),
    Line(LineSite),
}

#[derive(Clone, Copy)]
struct Linear {
    a: f64,
    b: f64,
    c: f64,
}

impl Linear {
    fn value(self, point: XY) -> f64 {
        self.a * point.x + self.b * point.y + self.c
    }
}

pub(crate) fn filled_hausdorff_distance(left: &Shape, right: &Shape) -> Result<f64> {
    let left_to_right = directed_filled_hausdorff(left, right)?;
    let right_to_left = directed_filled_hausdorff(right, left)?;
    Ok(left_to_right.max(right_to_left))
}

fn directed_filled_hausdorff(source: &Shape, target: &Shape) -> Result<f64> {
    let target_is_convex = convex_target(target);
    if target_is_convex {
        return convex_target_directed_distance(source, target);
    }
    let target = HausdorffTarget::build(target)?;
    let boundary_squared =
        super::kernel::directed_hausdorff_distance_squared_with_target_shape(source, &target)?;
    let mut best = if boundary_squared.is_normal() {
        boundary_squared.sqrt()
    } else {
        // Squared-space query results are deliberately not used as a
        // magnitude when they underflow or overflow.  The existing guarded
        // source-boundary finisher retains the same breakpoint owner.
        super::super::shape_impl::robust_directed_hausdorff_with_target(source, &target)?
    };

    if !source.has_area_parts() {
        return Ok(best);
    }
    // Distance to a closed convex target is convex. Its maximum over every
    // polygonal source component is therefore attained on that component's
    // boundary, which the continuous boundary kernel has already solved.
    if target_is_convex {
        return Ok(best);
    }

    // This is a useful exact zero certificate and, importantly, prevents a
    // source wholly covered by a filled target from refining every cell in
    // its bounding box.
    if target.has_filled_area() && source.covered_by(target_shape(&target))? {
        return Ok(0.0);
    }

    let Some(bounds) = filled_bounds(source) else {
        return Ok(best);
    };
    let search_bounds = if target.has_filled_area() {
        source_minus_target_bounds(source, target_shape(&target), bounds)?
    } else {
        bounds
    };
    let mut search = FilledSearch {
        source,
        target: &target,
        source_bounds: bounds,
        search_bounds,
        best,
        neighborhood: Vec::new(),
        bvh_stack: Vec::new(),
    };
    search.run()?;
    best = search.best;
    Ok(best)
}

fn convex_target_directed_distance(source: &Shape, target: &Shape) -> Result<f64> {
    let mut best = 0.0_f64;
    source.try_for_each_point(|point| {
        let distance = if target.area_covers_point(point) {
            0.0
        } else {
            let mut distance = f64::INFINITY;
            target.try_for_each_vertex_pair(|start, end| {
                distance = distance.min(point_segment_distance(point.xy(), Segment {
                    start: start.xy(),
                    end: end.xy(),
                })?);
                Ok::<(), crate::error::Error>(())
            })?;
            if distance == f64::INFINITY {
                target.try_for_each_point(|candidate| {
                    distance = distance.min(point_distance(point.xy(), candidate.xy()));
                    Ok::<(), crate::error::Error>(())
                })?;
            }
            distance
        };
        best = best.max(distance);
        Ok::<(), crate::error::Error>(())
    })?;
    Ok(best)
}

fn convex_target(shape: &Shape) -> bool {
    match shape {
        Shape::Point(_) => true,
        Shape::MultiPoint(points) => points.len() == 1,
        Shape::LineString(line) => line.coord_count() == 2,
        Shape::MultiLineString(lines) => {
            lines.len() == 1 && lines.first().is_some_and(|line| line.coord_count() == 2)
        },
        Shape::Polygon(polygon) => polygon.holes.is_empty() && shell_is_convex(&polygon.shell),
        _ => false,
    }
}

/// Return the target shape retained for area-membership proofs.
const fn target_shape(target: &HausdorffTarget) -> &Shape {
    target
        .filled_area_shape()
        .expect("area certificate only runs for a filled target")
}

struct FilledSearch<'a> {
    source: &'a Shape,
    target: &'a HausdorffTarget,
    source_bounds: Bounds,
    search_bounds: Bounds,
    best: f64,
    neighborhood: Vec<HausdorffFeature>,
    bvh_stack: Vec<u32>,
}

impl FilledSearch<'_> {
    fn run(&mut self) -> Result<()> {
        let cell = self.make_cell(
            self.search_bounds.minx(),
            self.search_bounds.maxx(),
            self.search_bounds.miny(),
            self.search_bounds.maxy(),
            0,
        )?;
        let mut cells = BinaryHeap::new();
        if !self.cell_may_contain_source(cell) {
            return Ok(());
        }
        // Small targets have a bounded site set. A localized source-minus-
        // target envelope is also a complete local problem, so keep all of
        // its support sites and avoid paying for a queue that only localizes
        // the already certified domain. Other large targets use the queue
        // below to localize first.
        let localized_global_solve = self.target.features.len()
            <= EXTENDED_GLOBAL_LOCAL_SOLVE_FEATURE_LIMIT
            && self.search_bounds != self.source_bounds;
        if self.target.features.len() <= GLOBAL_LOCAL_SOLVE_FEATURE_LIMIT || localized_global_solve
        {
            self.probe_cell(cell)?;
            self.solve_local(cell, localized_global_solve)?;
            return Ok(());
        }
        cells.push(cell);

        while let Some(cell) = cells.pop() {
            if cell.upper <= self.best.next_up() {
                continue;
            }
            self.probe_cell(cell)?;
            self.solve_local(cell, false)?;
            if cell.upper <= self.best.next_up() {
                continue;
            }
            if cell.depth >= MAX_REFINEMENT_DEPTH {
                // A finite floating-point box has reached a coordinate
                // fixed point.  The local solver has already consumed every
                // algebraic candidate that can live in this box.
                continue;
            }
            let mid_x = f64::midpoint(cell.min_x, cell.max_x);
            let mid_y = f64::midpoint(cell.min_y, cell.max_y);
            if mid_x == cell.min_x
                || mid_x == cell.max_x
                || mid_y == cell.min_y
                || mid_y == cell.max_y
            {
                continue;
            }
            for (min_x, max_x, min_y, max_y) in [
                (cell.min_x, mid_x, cell.min_y, mid_y),
                (mid_x, cell.max_x, cell.min_y, mid_y),
                (cell.min_x, mid_x, mid_y, cell.max_y),
                (mid_x, cell.max_x, mid_y, cell.max_y),
            ] {
                let child =
                    self.make_cell(min_x, max_x, min_y, max_y, cell.depth.saturating_add(1))?;
                if !self.cell_may_contain_source(child) {
                    continue;
                }
                if child.upper <= self.best.next_up() {
                    continue;
                }
                cells.push(child);
            }
        }
        Ok(())
    }

    fn make_cell(
        &mut self,
        min_x: f64,
        max_x: f64,
        min_y: f64,
        max_y: f64,
        depth: u8,
    ) -> Result<FilledCell> {
        if self.cell_is_filled(min_x, max_x, min_y, max_y)? {
            return Ok(FilledCell {
                min_x,
                max_x,
                min_y,
                max_y,
                upper: 0.0,
                depth,
            });
        }
        let mut upper = cell_feature_upper_bound(min_x, max_x, min_y, max_y, self.target)?;
        // Two parallel finite segment sites provide a sharper bound than
        // taking the maximum of either convex distance independently. This
        // is the certificate that makes rectangular medial-axis plateaus
        // terminate.
        // A pair can improve the current bound only when both supports are
        // close enough to the cell to participate in that improvement. The
        // BVH/box neighbourhood is conservative for that question; a
        // support omitted by it is farther than the current bound everywhere
        // in the cell, and its pair cannot improve the individual
        // feature-envelope bound already computed above.
        self.target.features_near_cell(
            min_x,
            max_x,
            min_y,
            max_y,
            next_up(upper),
            &mut self.neighborhood,
            &mut self.bvh_stack,
        )?;
        for (left_index, &left) in self.neighborhood.iter().enumerate() {
            let HausdorffFeature::Segment(left) = left else {
                continue;
            };
            for &right in self.neighborhood.iter().skip(left_index + 1) {
                let HausdorffFeature::Segment(right) = right else {
                    continue;
                };
                if let Some(bound) =
                    parallel_segment_pair_upper_bound(min_x, max_x, min_y, max_y, left, right)?
                {
                    upper = upper.min(bound);
                }
            }
        }
        let cell = FilledCell {
            min_x,
            max_x,
            min_y,
            max_y,
            upper: next_up(upper),
            depth,
        };
        Ok(cell)
    }

    /// A covered center plus no target boundary entering the cell interior
    /// certifies that the whole cell is in the target's filled area. This turns
    /// the common "large source inside a larger polygon" case into a zero-cost
    /// prune without assuming that corner probes alone characterize a holed or
    /// concave region.
    fn cell_is_filled(&mut self, min_x: f64, max_x: f64, min_y: f64, max_y: f64) -> Result<bool> {
        let Some(shape) = self.target.filled_area_shape() else {
            return Ok(false);
        };
        let center = Point::new_raw_xy(f64::midpoint(min_x, max_x), f64::midpoint(min_y, max_y));
        if !shape.area_covers_point(center) {
            return Ok(false);
        }
        let rectangle = Bounds::new_raw(min_x, min_y, max_x, max_y);
        self.target.features_near_cell(
            min_x,
            max_x,
            min_y,
            max_y,
            0.0,
            &mut self.neighborhood,
            &mut self.bvh_stack,
        )?;
        Ok(!self.neighborhood.iter().any(|feature| {
            let HausdorffFeature::Segment(segment) = *feature else {
                return false;
            };
            segment_vs_rect(segment, rectangle) == RectContact::Open
        }))
    }

    fn cell_may_contain_source(&self, cell: FilledCell) -> bool {
        if cell.max_x < self.source_bounds.minx()
            || cell.min_x > self.source_bounds.maxx()
            || cell.max_y < self.source_bounds.miny()
            || cell.min_y > self.source_bounds.maxy()
        {
            return false;
        }
        let points = cell_corners(cell);
        if points
            .iter()
            .copied()
            .chain(std::iter::once(cell_center(cell)))
            .any(|point| {
                self.source
                    .area_covers_point(Point::new_raw_xy(point.x, point.y))
            })
        {
            return true;
        }
        // If no probe is inside, an intersecting source boundary is the only
        // way an area can cross the cell. Use exact segment-cell contact
        // rather than the segment's box: a diagonal segment's box can cover a
        // large rectangle without the source touching most of it.
        let mut possible = false;
        let rectangle = Bounds::new_raw(cell.min_x, cell.min_y, cell.max_x, cell.max_y);
        self.source.for_each_segment(|segment| {
            if possible {
                return;
            }
            possible = segment_vs_rect(segment, rectangle) != RectContact::None;
        });
        possible
    }

    fn probe_cell(&mut self, cell: FilledCell) -> Result<()> {
        for point in cell_corners(cell)
            .into_iter()
            .chain(std::iter::once(cell_center(cell)))
        {
            self.consider(point)?;
        }
        Ok(())
    }

    fn consider(&mut self, point: XY) -> Result<()> {
        if !point.x.is_finite()
            || !point.y.is_finite()
            || !self
                .source
                .area_covers_point(Point::new_raw_xy(point.x, point.y))
        {
            return Ok(());
        }
        let distance = self.target.distance(point)?;
        self.best = self.best.max(distance);
        Ok(())
    }

    /// A generated candidate is useful only when its selected support site
    /// already permits an improvement. Since the full target distance is the
    /// minimum over all sites, a support distance at or below the incumbent
    /// proves that the candidate cannot improve it and avoids a cold full
    /// target reduction.
    fn consider_local_candidate(&mut self, point: XY, site: Site, origin: XY) -> Result<()> {
        if site_distance(site, point, origin)? <= self.best {
            return Ok(());
        }
        self.consider(world_point(origin, point))
    }

    fn solve_local(&mut self, cell: FilledCell, complete: bool) -> Result<()> {
        let origin = cell_center(cell);
        self.target.features_near_cell(
            cell.min_x,
            cell.max_x,
            cell.min_y,
            cell.max_y,
            cell.upper,
            &mut self.neighborhood,
            &mut self.bvh_stack,
        )?;
        let mut sites = Vec::new();
        let source_span = (self.source_bounds.maxx() - self.source_bounds.minx())
            .max(self.source_bounds.maxy() - self.source_bounds.miny());
        let cell_span = (cell.max_x - cell.min_x).max(cell.max_y - cell.min_y);
        let include_segment_endpoints =
            complete || self.target.features.len() <= 32 || cell_span <= source_span / 8.0;
        for &feature in &self.neighborhood {
            match feature {
                HausdorffFeature::Point(point) => {
                    sites.push((
                        self.target.feature_distance(origin, feature)?,
                        Site::Point(relative(origin, point)),
                    ));
                },
                HausdorffFeature::Segment(segment) => {
                    sites.push((
                        self.target.feature_distance(origin, feature)?,
                        Site::Line(line_site(origin, segment)),
                    ));
                    // Segment endpoints are point sites in the segment-site
                    // Voronoi problem. Keeping them here handles a circle
                    // tangent at a polygon corner without a separate
                    // endpoint/parabola implementation.
                    if include_segment_endpoints {
                        sites.push((
                            point_distance(origin, segment.start),
                            Site::Point(relative(origin, segment.start)),
                        ));
                        sites.push((
                            point_distance(origin, segment.end),
                            Site::Point(relative(origin, segment.end)),
                        ));
                    }
                },
            }
        }

        // A cell's local feature set is the only cubic part of the solver.
        // Features whose entire box is farther than the cell's admissible
        // upper bound cannot be a nearest support at a maximizing center.
        sites.retain(|(_, site)| match *site {
            Site::Point(point) => point_to_cell_distance(point, cell, origin) <= cell.upper,
            Site::Line(line) => line_to_cell_distance(line, cell, origin) <= cell.upper,
        });
        let mut unique_sites = Vec::with_capacity(sites.len());
        for (distance, site) in sites {
            if !unique_sites
                .iter()
                .any(|(_, candidate)| same_site(*candidate, site))
            {
                unique_sites.push((distance, site));
            }
        }
        if !complete
            && cell.depth < 3
            && self.target.features.len() > GLOBAL_LOCAL_SOLVE_FEATURE_LIMIT
            && unique_sites.len() > PRELOCAL_SITE_LIMIT
        {
            unique_sites.sort_unstable_by(|left, right| left.0.total_cmp(&right.0));
            unique_sites.truncate(PRELOCAL_SITE_LIMIT);
        }
        let sites: Vec<Site> = unique_sites.into_iter().map(|(_, site)| site).collect();

        for point in cell_corners(cell)
            .into_iter()
            .chain(std::iter::once(cell_center(cell)))
        {
            self.consider(point)?;
        }

        for i in 0..sites.len() {
            for j in (i + 1)..sites.len() {
                self.consider_pair_ridge(sites[i], sites[j], cell, origin)?;
                for k in (j + 1)..sites.len() {
                    self.consider_triple(sites[i], sites[j], sites[k], cell, origin)?;
                }
            }
        }
        Ok(())
    }

    fn consider_pair_ridge(
        &mut self,
        left: Site,
        right: Site,
        cell: FilledCell,
        origin: XY,
    ) -> Result<()> {
        let local_cell = relative_cell(cell, origin);
        let lines = match (left, right) {
            (Site::Point(left), Site::Point(right)) => vec![point_equality(left, right)],
            (Site::Line(left), Site::Line(right)) if parallel_lines(left, right) => {
                line_equality(left, right)
            },
            _ => return Ok(()),
        };
        for line in lines {
            for point in line_cell_candidates(line, local_cell) {
                self.consider_local_candidate(point, left, origin)?;
            }
            // A ridge can cross a source component while every rectangle
            // endpoint lies in a hole or outside a concavity. Source-boundary
            // intersections split the ridge into intervals; a midpoint of
            // each interval is the representative plateau witness.
            let mut points = line_cell_candidates(line, local_cell);
            self.source.for_each_segment(|segment| {
                if let Some(point) = segment_line_intersection(segment, line, origin)
                    && point_in_cell(point, local_cell)
                {
                    points.push(point);
                }
            });
            for point in source_vertices_projected_to_line(self.source, line, origin, local_cell) {
                points.push(point);
            }
            sort_line_points(&mut points, line);
            for pair in points.array_windows::<2>() {
                let midpoint = XY::new(
                    f64::midpoint(pair[0].x, pair[1].x),
                    f64::midpoint(pair[0].y, pair[1].y),
                );
                self.consider_local_candidate(midpoint, left, origin)?;
            }
        }
        Ok(())
    }

    fn consider_triple(
        &mut self,
        first: Site,
        second: Site,
        third: Site,
        cell: FilledCell,
        origin: XY,
    ) -> Result<()> {
        let local_cell = relative_cell(cell, origin);
        match (first, second, third) {
            (Site::Point(left), Site::Point(right), Site::Point(third)) => {
                let equality = point_equality(left, right);
                if let Some(point) = intersect_lines(equality, point_equality(left, third))
                    && point_in_cell(point, local_cell)
                {
                    self.consider_local_candidate(point, Site::Point(left), origin)?;
                }
            },
            (Site::Point(left), Site::Point(right), Site::Line(line))
            | (Site::Point(left), Site::Line(line), Site::Point(right))
            | (Site::Line(line), Site::Point(left), Site::Point(right)) => {
                let equality = point_equality(left, right);
                for point in point_line_roots(equality, left, line).into_iter().flatten() {
                    if point_in_cell(point, local_cell) {
                        self.consider_local_candidate(point, Site::Point(left), origin)?;
                    }
                }
            },
            (Site::Point(point), Site::Line(left), Site::Line(right))
            | (Site::Line(left), Site::Point(point), Site::Line(right))
            | (Site::Line(left), Site::Line(right), Site::Point(point)) => {
                for equality in line_equality(left, right) {
                    for point in point_line_roots(equality, point, left)
                        .into_iter()
                        .flatten()
                    {
                        if point_in_cell(point, local_cell) {
                            self.consider_local_candidate(point, Site::Line(left), origin)?;
                        }
                    }
                }
            },
            (Site::Line(left), Site::Line(right), Site::Line(third)) => {
                for equality in line_equality(left, right) {
                    for other in line_equality(third, left) {
                        if let Some(point) = intersect_lines(equality, other)
                            && point_in_cell(point, local_cell)
                        {
                            self.consider_local_candidate(point, Site::Line(left), origin)?;
                        }
                    }
                }
            },
        }
        Ok(())
    }
}

fn filled_bounds(shape: &Shape) -> Option<Bounds> {
    const fn merge(mut left: Option<Bounds>, right: Option<Bounds>) -> Option<Bounds> {
        if let Some(right) = right {
            if let Some(left_bounds) = &mut left {
                left_bounds.include_bounds(right);
            } else {
                left = Some(right);
            }
        }
        left
    }

    match shape {
        Shape::Polygon(polygon) if !ring_winding(polygon.shell.coords()).is_degenerate() => {
            Bounds::from_coords(polygon.shell.coords())
        },
        Shape::MultiPolygon(polygons) => polygons
            .iter()
            .filter(|polygon| !ring_winding(polygon.shell.coords()).is_degenerate())
            .filter_map(|polygon| Bounds::from_coords(polygon.shell.coords()))
            .fold(None, |bounds, next| merge(bounds, Some(next))),
        Shape::GeometryCollection(parts) => parts
            .iter()
            .filter_map(filled_bounds)
            .fold(None, |bounds, next| merge(bounds, Some(next))),
        _ => None,
    }
}

/// Return a conservative envelope for the unexamined interior of `source`
/// outside a filled `target`.  The source boundary has already been solved by
/// the continuous linework kernel, so a shared boundary contact that has no
/// nearby difference face need not enlarge this interior search. The extrema
/// of the remaining polygon difference occur at a source vertex outside the
/// target, a target-boundary vertex inside the source, or a source/target
/// boundary crossing. Crossings use the shared robust placement path, and the
/// resulting envelope is expanded by one representable coordinate on every
/// side before it is used as a search root.
///
/// A missing or numerically unplaceable candidate is deliberately not treated
/// as an empty difference: the full source envelope is the sound fallback.
fn source_minus_target_bounds(source: &Shape, target: &Shape, fallback: Bounds) -> Result<Bounds> {
    let source_segments = collect_segments(source)?;
    let target_segments = collect_segments(target)?;
    let mut candidates = None;
    let mut uncertain = false;

    source.for_each_point(|point| {
        if !target.area_covers_point(point) && !include_search_point(&mut candidates, point.xy()) {
            uncertain = true;
        }
    });
    target.for_each_point(|point| {
        if area_strictly_covers_point(source, point.xy())
            && !include_search_point(&mut candidates, point.xy())
        {
            uncertain = true;
        }
    });

    for &left in &source_segments {
        if uncertain {
            break;
        }
        for &right in &target_segments {
            if !segment_is_finite(left) || !segment_is_finite(right) {
                uncertain = true;
                break;
            }
            if strictly_crosses(left, right) {
                if let Some(point) = segment_cross_point(left, right) {
                    if !include_search_point(&mut candidates, point) {
                        uncertain = true;
                    }
                } else {
                    uncertain = true;
                }
                continue;
            }
            for point in [left.start, left.end, right.start, right.end] {
                if point_on_segment(point, left.start, left.end)
                    && point_on_segment(point, right.start, right.end)
                {
                    match difference_at_contact(
                        source,
                        target,
                        &source_segments,
                        &target_segments,
                        point,
                    )? {
                        Some(true) => {
                            if !include_search_point(&mut candidates, point) {
                                uncertain = true;
                            }
                        },
                        Some(false) => {},
                        None => uncertain = true,
                    }
                }
            }
        }
    }

    if uncertain {
        return Ok(fallback);
    }
    let Some(bounds) = candidates else {
        return Ok(fallback);
    };
    let expanded = Bounds::new_raw(
        bounds.minx().next_down(),
        bounds.miny().next_down(),
        bounds.maxx().next_up(),
        bounds.maxy().next_up(),
    );
    if expanded.minx().is_finite()
        && expanded.miny().is_finite()
        && expanded.maxx().is_finite()
        && expanded.maxy().is_finite()
    {
        Ok(expanded)
    } else {
        Ok(fallback)
    }
}

fn collect_segments(shape: &Shape) -> Result<Vec<Segment>> {
    let mut segments = Vec::new();
    shape.try_for_each_vertex_pair(|start, end| {
        crate::try_push_vec(&mut segments, Segment {
            start: start.xy(),
            end: end.xy(),
        })
    })?;
    Ok(segments)
}

const fn include_search_point(bounds: &mut Option<Bounds>, point: XY) -> bool {
    if !point.x.is_finite() || !point.y.is_finite() {
        return false;
    }
    if let Some(bounds) = bounds {
        bounds.include_xy(point);
    } else {
        *bounds = Some(Bounds::new_raw(point.x, point.y, point.x, point.y));
    }
    true
}

/// Whether a boundary contact has a nearby source point outside the target.
/// The incident rays partition a small neighbourhood of the contact into
/// faces; probing one point in each face distinguishes a genuine difference
/// boundary from a shared boundary that only touches.
fn difference_at_contact(
    source: &Shape,
    target: &Shape,
    source_segments: &[Segment],
    target_segments: &[Segment],
    point: XY,
) -> Result<Option<bool>> {
    let mut rays = Vec::new();
    for &segment in source_segments.iter().chain(target_segments) {
        if !point_on_segment(point, segment.start, segment.end) {
            continue;
        }
        let mut add_ray = |endpoint: XY| -> Result<()> {
            let dx = endpoint.x - point.x;
            let dy = endpoint.y - point.y;
            let length = dx.hypot(dy);
            if length == 0.0 || !length.is_finite() {
                return Ok(());
            }
            let ray = XY::new(dx / length, dy / length);
            if !rays.iter().any(|candidate| same_xy(*candidate, ray)) {
                crate::try_push_vec(&mut rays, ray)?;
            }
            Ok(())
        };
        add_ray(segment.start)?;
        add_ray(segment.end)?;
    }
    if rays.len() < 2 {
        return Ok(None);
    }

    let mut clearance = f64::INFINITY;
    for &segment in source_segments.iter().chain(target_segments) {
        if !point_on_segment(point, segment.start, segment.end) {
            clearance = clearance.min(point_segment_distance(
                Point::new_raw_xy(point.x, point.y),
                segment,
            )?);
        }
    }
    let ray_length = source_segments
        .iter()
        .chain(target_segments)
        .filter(|segment| point_on_segment(point, segment.start, segment.end))
        .map(|segment| {
            (segment.start.x - point.x)
                .hypot(segment.start.y - point.y)
                .min((segment.end.x - point.x).hypot(segment.end.y - point.y))
        })
        .filter(|length| length.is_finite() && *length > 0.0)
        .fold(f64::INFINITY, f64::min);
    let radius = 0.25
        * if clearance.is_finite() {
            clearance.min(ray_length)
        } else {
            ray_length
        };
    if !radius.is_finite() || radius <= 0.0 {
        return Ok(None);
    }

    rays.sort_unstable_by(|left, right| left.y.atan2(left.x).total_cmp(&right.y.atan2(right.x)));
    let mut face_count = 0;
    for (index, &left) in rays.iter().enumerate() {
        let right = rays[(index + 1) % rays.len()];
        let mut right_angle = right.y.atan2(right.x);
        let left_angle = left.y.atan2(left.x);
        if right_angle <= left_angle {
            right_angle += std::f64::consts::TAU;
        }
        let angle = f64::midpoint(left_angle, right_angle);
        let probe = XY::new(
            point.x + radius * angle.cos(),
            point.y + radius * angle.sin(),
        );
        if !probe.x.is_finite() || !probe.y.is_finite() {
            return Ok(None);
        }
        face_count += 1;
        if source.area_covers_point(Point::new_raw_xy(probe.x, probe.y))
            && !target.area_covers_point(Point::new_raw_xy(probe.x, probe.y))
        {
            return Ok(Some(true));
        }
    }
    if face_count > 0 {
        Ok(Some(false))
    } else {
        Ok(None)
    }
}

const fn same_xy(left: XY, right: XY) -> bool {
    left.x.to_bits() == right.x.to_bits() && left.y.to_bits() == right.y.to_bits()
}

const fn same_site(left: Site, right: Site) -> bool {
    match (left, right) {
        (Site::Point(left), Site::Point(right)) => same_xy(left, right),
        (Site::Line(left), Site::Line(right)) => {
            (same_xy(left.segment.start, right.segment.start)
                && same_xy(left.segment.end, right.segment.end))
                || (same_xy(left.segment.start, right.segment.end)
                    && same_xy(left.segment.end, right.segment.start))
        },
        _ => false,
    }
}

fn area_strictly_covers_point(shape: &Shape, point: XY) -> bool {
    if !shape.area_covers_point(Point::new_raw_xy(point.x, point.y)) {
        return false;
    }
    let mut boundary = false;
    shape.for_each_segment(|segment| {
        if !boundary && point_on_segment(point, segment.start, segment.end) {
            boundary = true;
        }
    });
    !boundary
}

const fn segment_is_finite(segment: Segment) -> bool {
    segment.start.x.is_finite()
        && segment.start.y.is_finite()
        && segment.end.x.is_finite()
        && segment.end.y.is_finite()
}

fn strictly_crosses(left: Segment, right: Segment) -> bool {
    const fn opposite(left: Orientation, right: Orientation) -> bool {
        matches!(
            (left, right),
            (Orientation::CounterClockwise, Orientation::Clockwise)
                | (Orientation::Clockwise, Orientation::CounterClockwise)
        )
    }

    opposite(
        orientation(left.start, left.end, right.start),
        orientation(left.start, left.end, right.end),
    ) && opposite(
        orientation(right.start, right.end, left.start),
        orientation(right.start, right.end, left.end),
    )
}

const fn cell_corners(cell: FilledCell) -> [XY; 4] {
    [
        XY::new(cell.min_x, cell.min_y),
        XY::new(cell.max_x, cell.min_y),
        XY::new(cell.min_x, cell.max_y),
        XY::new(cell.max_x, cell.max_y),
    ]
}

const fn cell_center(cell: FilledCell) -> XY {
    XY::new(
        f64::midpoint(cell.min_x, cell.max_x),
        f64::midpoint(cell.min_y, cell.max_y),
    )
}

fn relative_cell(cell: FilledCell, origin: XY) -> FilledCell {
    FilledCell {
        min_x: cell.min_x - origin.x,
        max_x: cell.max_x - origin.x,
        min_y: cell.min_y - origin.y,
        max_y: cell.max_y - origin.y,
        upper: cell.upper,
        depth: cell.depth,
    }
}

fn next_up(value: f64) -> f64 {
    if value.is_finite() && value > 0.0 {
        value.next_up()
    } else {
        value
    }
}

fn point_in_cell(point: XY, cell: FilledCell) -> bool {
    point.x >= cell.min_x && point.x <= cell.max_x && point.y >= cell.min_y && point.y <= cell.max_y
}

fn relative(origin: XY, point: XY) -> XY {
    XY::new(point.x - origin.x, point.y - origin.y)
}

fn world_point(origin: XY, point: XY) -> XY {
    XY::new(origin.x + point.x, origin.y + point.y)
}

fn point_to_cell_distance(point: XY, cell: FilledCell, origin: XY) -> f64 {
    let point = world_point(origin, point);
    let dx = if point.x < cell.min_x {
        cell.min_x - point.x
    } else if point.x > cell.max_x {
        point.x - cell.max_x
    } else {
        0.0
    };
    let dy = if point.y < cell.min_y {
        cell.min_y - point.y
    } else if point.y > cell.max_y {
        point.y - cell.max_y
    } else {
        0.0
    };
    dx.hypot(dy)
}

fn site_distance(site: Site, point: XY, origin: XY) -> Result<f64> {
    Ok(match site {
        Site::Point(site) => point_distance(site, point),
        Site::Line(site) => point_segment_distance(
            Point::new_raw_xy(origin.x + point.x, origin.y + point.y),
            site.segment,
        )?,
    })
}

fn line_to_cell_distance(line: LineSite, cell: FilledCell, origin: XY) -> f64 {
    // The finite segment's box is a lower bound on its distance to the cell.
    // A supporting line can cross the cell even when the segment does not,
    // but retaining that segment would only make a local cubic solve larger.
    let start = line.start;
    let end = line.end;
    let segment_min_x = start.x.min(end.x);
    let segment_max_x = start.x.max(end.x);
    let segment_min_y = start.y.min(end.y);
    let segment_max_y = start.y.max(end.y);
    let min_x = cell.min_x - origin.x;
    let max_x = cell.max_x - origin.x;
    let min_y = cell.min_y - origin.y;
    let max_y = cell.max_y - origin.y;
    let dx = if segment_max_x < min_x {
        min_x - segment_max_x
    } else if segment_min_x > max_x {
        segment_min_x - max_x
    } else {
        0.0
    };
    let dy = if segment_max_y < min_y {
        min_y - segment_max_y
    } else if segment_min_y > max_y {
        segment_min_y - max_y
    } else {
        0.0
    };
    dx.hypot(dy)
}

fn line_site(origin: XY, segment: Segment) -> LineSite {
    let dx = segment.end.x - segment.start.x;
    let dy = segment.end.y - segment.start.y;
    let scale = dx.abs().max(dy.abs());
    if scale == 0.0 || !scale.is_finite() {
        return LineSite {
            a: 0.0,
            b: 0.0,
            c: f64::INFINITY,
            start: relative(origin, segment.start),
            end: relative(origin, segment.end),
            segment,
        };
    }
    let sx = dx / scale;
    let sy = dy / scale;
    let norm = sx.hypot(sy);
    let a = -sy / norm;
    let b = sx / norm;
    let start = relative(origin, segment.start);
    let end = relative(origin, segment.end);
    LineSite {
        a,
        b,
        c: -(a * start.x + b * start.y),
        start,
        end,
        segment,
    }
}

fn parallel_lines(left: LineSite, right: LineSite) -> bool {
    if !left.a.is_finite() || !right.a.is_finite() {
        return false;
    }
    let cross = left.a * right.b - left.b * right.a;
    cross == 0.0
        || orientation(
            XY::new(0.0, 0.0),
            XY::new(left.a, left.b),
            XY::new(right.a, right.b),
        ) == Orientation::Collinear
}

fn line_equality(left: LineSite, right: LineSite) -> Vec<Linear> {
    let dot = left.a * right.a + left.b * right.b;
    let right = if dot < 0.0 {
        LineSite {
            a: -right.a,
            b: -right.b,
            c: -right.c,
            start: right.start,
            end: right.end,
            segment: right.segment,
        }
    } else {
        right
    };
    let mut lines = Vec::with_capacity(2);
    push_linear(&mut lines, Linear {
        a: left.a - right.a,
        b: left.b - right.b,
        c: left.c - right.c,
    });
    push_linear(&mut lines, Linear {
        a: left.a + right.a,
        b: left.b + right.b,
        c: left.c + right.c,
    });
    lines
}

fn push_linear(lines: &mut Vec<Linear>, line: Linear) {
    if line.a != 0.0 || line.b != 0.0 {
        lines.push(line);
    }
}

fn point_equality(left: XY, right: XY) -> Linear {
    Linear {
        a: 2.0 * (right.x - left.x),
        b: 2.0 * (right.y - left.y),
        c: left.x * left.x + left.y * left.y - right.x * right.x - right.y * right.y,
    }
}

fn intersect_lines(left: Linear, right: Linear) -> Option<XY> {
    let determinant = left.a * right.b - right.a * left.b;
    if determinant == 0.0 || !determinant.is_finite() {
        return None;
    }
    let x = (left.b * right.c - right.b * left.c) / determinant;
    let y = (right.a * left.c - left.a * right.c) / determinant;
    (x.is_finite() && y.is_finite()).then_some(XY::new(x, y))
}

fn point_line_roots(equality: Linear, point: XY, line: LineSite) -> [Option<XY>; 2] {
    let denominator = equality.a * equality.a + equality.b * equality.b;
    if denominator == 0.0 || !denominator.is_finite() {
        return [None, None];
    }
    let base = XY::new(
        -equality.a * equality.c / denominator,
        -equality.b * equality.c / denominator,
    );
    let direction = XY::new(-equality.b, equality.a);
    let (line_a, line_b, line_c) = (line.a, line.b, line.c);
    let vx = base.x - point.x;
    let vy = base.y - point.y;
    let lv = line_a * base.x + line_b * base.y + line_c;
    let ld = line_a * direction.x + line_b * direction.y;
    let qa = direction.x * direction.x + direction.y * direction.y - ld * ld;
    let qb = 2.0 * (vx * direction.x + vy * direction.y - lv * ld);
    let qc = vx * vx + vy * vy - lv * lv;
    let mut roots = [None; 2];
    for (index, parameter) in quadratic_roots(qa, qb, qc).into_iter().enumerate() {
        let Some(parameter) = parameter else {
            continue;
        };
        let point = XY::new(
            base.x + parameter * direction.x,
            base.y + parameter * direction.y,
        );
        if point.x.is_finite() && point.y.is_finite() {
            roots[index] = Some(point);
        }
    }
    roots
}

fn line_cell_candidates(line: Linear, cell: FilledCell) -> Vec<XY> {
    let mut points = Vec::new();
    if line.a != 0.0 {
        for y in [cell.min_y, cell.max_y] {
            let x = -(line.b * y + line.c) / line.a;
            let point = XY::new(x, y);
            if point_in_cell(point, cell) {
                points.push(point);
            }
        }
    }
    if line.b != 0.0 {
        for x in [cell.min_x, cell.max_x] {
            let y = -(line.a * x + line.c) / line.b;
            let point = XY::new(x, y);
            if point_in_cell(point, cell) {
                points.push(point);
            }
        }
    }
    points
}

fn segment_line_intersection(segment: Segment, line: Linear, origin: XY) -> Option<XY> {
    let start = relative(origin, segment.start);
    let end = relative(origin, segment.end);
    let direction = XY::new(end.x - start.x, end.y - start.y);
    let denominator = line.a * direction.x + line.b * direction.y;
    if denominator == 0.0 {
        return None;
    }
    let parameter = -(line.a * start.x + line.b * start.y + line.c) / denominator;
    (0.0..=1.0).contains(&parameter).then_some(XY::new(
        start.x + parameter * direction.x,
        start.y + parameter * direction.y,
    ))
}

fn source_vertices_projected_to_line(
    source: &Shape,
    line: Linear,
    origin: XY,
    cell: FilledCell,
) -> Vec<XY> {
    let denominator = line.a * line.a + line.b * line.b;
    if denominator == 0.0 {
        return Vec::new();
    }
    let mut projected = Vec::new();
    source.for_each_point(|point| {
        let point = relative(origin, point.xy());
        let amount = line.value(point) / denominator;
        let candidate = XY::new(point.x - line.a * amount, point.y - line.b * amount);
        if point_in_cell(candidate, cell) {
            projected.push(candidate);
        }
    });
    projected
}

fn sort_line_points(points: &mut Vec<XY>, line: Linear) {
    let direction = XY::new(-line.b, line.a);
    points.sort_unstable_by(|left, right| {
        (left.x * direction.x + left.y * direction.y)
            .total_cmp(&(right.x * direction.x + right.y * direction.y))
    });
    points.dedup_by(|left, right| {
        left.x.to_bits() == right.x.to_bits() && left.y.to_bits() == right.y.to_bits()
    });
}

fn cell_feature_upper_bound(
    min_x: f64,
    max_x: f64,
    min_y: f64,
    max_y: f64,
    target: &HausdorffTarget,
) -> Result<f64> {
    let corners = [
        XY::new(min_x, min_y),
        XY::new(max_x, min_y),
        XY::new(min_x, max_y),
        XY::new(max_x, max_y),
    ];
    let mut upper = f64::INFINITY;
    for &feature in &target.features {
        let mut feature_upper: f64 = 0.0;
        for corner in corners {
            feature_upper = feature_upper.max(target.feature_distance(corner, feature)?);
        }
        upper = upper.min(feature_upper);
    }
    // The distance to a closed set is 1-Lipschitz.  A center query plus the
    // cell radius is therefore another admissible upper bound.  Unlike the
    // corner envelope above, it observes that a covered center has zero
    // distance to a filled target, which closes cells straddling its boundary
    // without constructing a rounded source-minus-target domain.
    let center = XY::new(f64::midpoint(min_x, max_x), f64::midpoint(min_y, max_y));
    let radius = 0.5 * (max_x - min_x).hypot(max_y - min_y);
    upper = upper.min(next_up(target.distance(center)? + radius));
    Ok(upper)
}

fn parallel_segment_pair_upper_bound(
    min_x: f64,
    max_x: f64,
    min_y: f64,
    max_y: f64,
    left: Segment,
    right: Segment,
) -> Result<Option<f64>> {
    let corners = [
        XY::new(min_x, min_y),
        XY::new(max_x, min_y),
        XY::new(min_x, max_y),
        XY::new(max_x, max_y),
    ];
    let projection_covered = |segment: Segment| {
        let dx = segment.end.x - segment.start.x;
        let dy = segment.end.y - segment.start.y;
        let denominator = dx * dx + dy * dy;
        if denominator == 0.0 || !denominator.is_finite() {
            return false;
        }
        corners.iter().all(|&corner| {
            let parameter = ((corner.x - segment.start.x) * dx + (corner.y - segment.start.y) * dy)
                / denominator;
            (0.0..=1.0).contains(&parameter)
        })
    };
    if !projection_covered(left) || !projection_covered(right) {
        return Ok(None);
    }
    let left_line = line_site(XY::new(0.0, 0.0), left);
    let right_line = line_site(XY::new(0.0, 0.0), right);
    if !parallel_lines(left_line, right_line) {
        return Ok(None);
    }
    let dot = left_line.a * right_line.a + left_line.b * right_line.b;
    let right_line = if dot < 0.0 {
        LineSite {
            a: -right_line.a,
            b: -right_line.b,
            c: -right_line.c,
            start: right_line.start,
            end: right_line.end,
            segment: right_line.segment,
        }
    } else {
        right_line
    };
    let ridge = Linear {
        a: left_line.a,
        b: left_line.b,
        c: f64::midpoint(left_line.c, right_line.c),
    };
    let mut bound: f64 = 0.0;
    for corner in corners {
        let left_distance = point_segment_distance(Point::new_raw_xy(corner.x, corner.y), left)?;
        let right_distance = point_segment_distance(Point::new_raw_xy(corner.x, corner.y), right)?;
        bound = bound.max(left_distance.min(right_distance));
    }
    if !line_cell_candidates(ridge, FilledCell {
        min_x,
        max_x,
        min_y,
        max_y,
        upper: f64::INFINITY,
        depth: 0,
    })
    .is_empty()
    {
        bound = bound.max((left_line.c - right_line.c).abs() / 2.0);
    }
    Ok(Some(bound))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::{CoordSeq, LineSeq, Polygon, Ring};

    fn polygon(points: &[(f64, f64)], holes: &[&[(f64, f64)]]) -> Shape {
        let ring = |points: &[(f64, f64)]| {
            Ring::from_closed(
                CoordSeq::try_from_xy(
                    &points
                        .iter()
                        .map(|&(x, y)| XY::new(x, y))
                        .collect::<Vec<_>>(),
                )
                .expect("test coordinates"),
            )
        };
        let holes = holes.iter().map(|points| ring(points)).collect::<Vec<_>>();
        Shape::Polygon(Polygon::new(ring(points), holes).expect("test polygon"))
    }

    fn line(start: (f64, f64), end: (f64, f64)) -> Shape {
        Shape::LineString(
            LineSeq::segment(
                Point::new_raw_xy(start.0, start.1),
                Point::new_raw_xy(end.0, end.1),
            )
            .expect("test line"),
        )
    }

    #[test]
    fn filled_source_finds_hole_center_maximum() {
        let source = polygon(
            &[(2.0, 2.0), (8.0, 2.0), (8.0, 8.0), (2.0, 8.0), (2.0, 2.0)],
            &[],
        );
        assert!(source.has_area_parts());
        let target = polygon(
            &[(2.0, 2.0), (8.0, 2.0), (8.0, 8.0), (2.0, 8.0), (2.0, 2.0)],
            &[&[(3.0, 3.0), (7.0, 3.0), (7.0, 7.0), (3.0, 7.0), (3.0, 3.0)]],
        );
        let query = HausdorffTarget::build(&target).unwrap();
        assert!(source.area_covers_point(Point::new_raw_xy(5.0, 5.0)));
        assert_eq!(query.distance(XY::new(5.0, 5.0)).unwrap(), 2.0);
        assert_eq!(directed_filled_hausdorff(&source, &target).unwrap(), 2.0);
        assert_eq!(directed_filled_hausdorff(&target, &source).unwrap(), 0.0);
        assert_eq!(filled_hausdorff_distance(&source, &target).unwrap(), 2.0);
    }

    #[test]
    fn zero_signed_bowtie_retains_structural_filled_support() {
        let source = polygon(
            &[(0.0, 0.0), (2.0, 2.0), (0.0, 2.0), (2.0, 0.0), (0.0, 0.0)],
            &[],
        );
        assert!(source.has_area_parts());
        assert!(source.area_covers_point(Point::new_raw_xy(0.5, 1.5)));
        assert!(HausdorffTarget::build(&source).unwrap().has_filled_area());
    }

    #[test]
    fn filled_source_finds_inradius_against_its_boundary() {
        let source = polygon(
            &[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ],
            &[],
        );
        let target = line((0.0, 0.0), (10.0, 0.0));
        assert_eq!(filled_hausdorff_distance(&source, &target).unwrap(), 10.0);
        let boundary = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_xy(&[
                    XY::new(0.0, 0.0),
                    XY::new(10.0, 0.0),
                    XY::new(10.0, 10.0),
                    XY::new(0.0, 10.0),
                    XY::new(0.0, 0.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        assert_eq!(filled_hausdorff_distance(&source, &boundary).unwrap(), 5.0);
    }

    #[test]
    fn parallel_target_ridge_is_finished_without_refinement() {
        let source = polygon(
            &[
                (-1.0, -1.0),
                (1.0, -1.0),
                (1.0, 1.0),
                (-1.0, 1.0),
                (-1.0, -1.0),
            ],
            &[],
        );
        let target = Shape::geometry_collection(vec![
            line((-2.0, -1.0), (2.0, -1.0)),
            line((-2.0, 1.0), (2.0, 1.0)),
        ])
        .unwrap();
        assert_eq!(filled_hausdorff_distance(&source, &target).unwrap(), 1.0);
    }

    #[test]
    fn multiple_target_holes_choose_the_largest_empty_circle() {
        let source = polygon(
            &[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ],
            &[],
        );
        let target = polygon(
            &[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ],
            &[
                &[(1.0, 1.0), (3.0, 1.0), (3.0, 3.0), (1.0, 3.0), (1.0, 1.0)],
                &[(6.0, 6.0), (9.0, 6.0), (9.0, 9.0), (6.0, 9.0), (6.0, 6.0)],
            ],
        );
        assert_eq!(directed_filled_hausdorff(&source, &target).unwrap(), 1.5);
        let small_source = polygon(
            &[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)],
            &[],
        );
        assert_eq!(
            directed_filled_hausdorff(&small_source, &target).unwrap(),
            0.0
        );
    }

    #[test]
    fn source_inside_target_hole_measures_to_the_hole_boundary() {
        let source = polygon(
            &[(4.0, 4.0), (6.0, 4.0), (6.0, 6.0), (4.0, 6.0), (4.0, 4.0)],
            &[],
        );
        let target = polygon(
            &[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ],
            &[&[(2.0, 2.0), (8.0, 2.0), (8.0, 8.0), (2.0, 8.0), (2.0, 2.0)]],
        );
        let query = HausdorffTarget::build(&target).unwrap();
        assert_eq!(query.distance(XY::new(5.0, 5.0)).unwrap(), 3.0);
        assert_eq!(query.distance(XY::new(4.0, 4.0)).unwrap(), 2.0);
        assert!(source.area_covers_point(Point::new_raw_xy(5.0, 5.0)));
        assert_eq!(directed_filled_hausdorff(&source, &target).unwrap(), 3.0);
    }

    #[test]
    fn triangle_site_triple_is_available_to_local_solver() {
        let source = polygon(&[(0.0, 0.0), (10.0, 0.0), (0.0, 10.0), (0.0, 0.0)], &[]);
        let target = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_xy(&[
                    XY::new(0.0, 0.0),
                    XY::new(10.0, 0.0),
                    XY::new(0.0, 10.0),
                    XY::new(0.0, 0.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        let query = HausdorffTarget::build(&target).unwrap();
        let bounds = filled_bounds(&source).unwrap();
        let mut search = FilledSearch {
            source: &source,
            target: &query,
            source_bounds: bounds,
            search_bounds: bounds,
            best: 0.0,
            neighborhood: Vec::new(),
            bvh_stack: Vec::new(),
        };
        let cell = search.make_cell(0.0, 10.0, 0.0, 10.0, 0).unwrap();
        search.solve_local(cell, false).unwrap();
        assert!(search.best > 2.0, "local best={}", search.best);
        assert!(search.best < 4.0, "local best={}", search.best);
    }

    #[test]
    fn large_local_neighborhood_finishes_with_all_final_supports() {
        let source = polygon(
            &[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ],
            &[],
        );
        let mut target_parts = Vec::new();
        for index in 0..9 {
            let start = f64::from(index) * 10.0 / 9.0;
            let end = f64::from(index + 1) * 10.0 / 9.0;
            target_parts.extend([
                line((start, 0.0), (end, 0.0)),
                line((10.0, start), (10.0, end)),
                line((end, 10.0), (start, 10.0)),
                line((0.0, end), (0.0, start)),
            ]);
        }
        let target = Shape::geometry_collection(target_parts).unwrap();
        assert_eq!(filled_hausdorff_distance(&source, &target).unwrap(), 5.0);
    }
}
